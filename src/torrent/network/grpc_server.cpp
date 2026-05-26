/**
 * grpc_server.cpp — gRPC Inter-Broker Communication Server
 *
 * Implements the gRPC server for inter-broker RPC operations:
 *   - Raft RPC Service: AppendEntries, RequestVote, InstallSnapshot
 *   - Metadata Service: fetch cluster metadata, register broker
 *   - Consumer Group Service: find coordinator, join group, sync group
 *
 * The server uses a lightweight gRPC-like request/response pattern over
 * HTTP/2 (or raw TCP for the fast path). Since this project does not
 * use protobuf code generation, message serialization is done inline
 * using the project's own big-endian binary format via ProtocolSerializer.
 *
 * Wire format (custom binary, gRPC-compatible framing):
 *   [1 byte]     compression flag (0=none, 1=gzip)
 *   [4 bytes BE] message length (excluding these 5 bytes)
 *   [payload]    serialised protobuf-compatible message
 *
 * Thread safety:
 *   - Service handlers are invoked from the gRPC thread pool.
 *   - All mutable state is protected by internal locks of the subsystems
 *     they delegate to.
 *   - Server start/stop is serialised externally.
 */

#include "torrent/network/grpc_server.h"
#include "torrent/network/protocol.h"
#include "torrent/network/transport.h"
#include "torrent/network/protocol_serializer.h"
#include "torrent/common/types.h"
#include "torrent/consensus/raft_types.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <unistd.h>

using namespace std::chrono_literals;

// ============================================================================
// Forward declaration
// ============================================================================

namespace torrent::network {
class GrpcServer;
}

// ============================================================================
// Anonymous namespace — internal helpers and constants
// ============================================================================

namespace {

namespace tp = torrent::protocol;
namespace raft = torrent::raft;
using torrent::error_code;
using torrent::endpoint;
using torrent::broker_id_t;

// =========================================================================
// Constants
// =========================================================================

/// Maximum gRPC message size (16 MiB).
static constexpr size_t kMaxGrpcMessageSize = 16 * 1024 * 1024;

/// gRPC frame header size: 1 byte compression + 4 bytes length = 5 bytes.
static constexpr size_t kGrpcFrameHeaderSize = 5;

/// Listen backlog for the gRPC server socket.
static constexpr int kGrpcBacklog = 128;

/// Maximum number of concurrent RPC handlers per server.
static constexpr int kMaxConcurrentRpcs = 256;

/// Default gRPC port for inter-broker communication.
static constexpr uint16_t kDefaultGrpcPort = 9097;

/// Epoll max events per iteration.
static constexpr int kEpollMaxEvents = 64;

/// Read buffer size per-connection.
static constexpr size_t kReadBufferSize = 65536;

// =========================================================================
// RPC method identifiers (1-byte tag for multiplexing)
// =========================================================================

enum class RpcMethod : uint8_t {
    // Raft RPCs
    AppendEntries       = 0,
    RequestVote         = 1,
    InstallSnapshot     = 2,

    // Metadata service
    FetchMetadata       = 10,
    RegisterBroker      = 11,
    UpdateMetadata      = 12,

    // Consumer group service
    FindCoordinator     = 20,
    JoinGroup           = 21,
    SyncGroup           = 22,
    Heartbeat           = 23,
    LeaveGroup          = 24,
    OffsetCommit        = 25,
    OffsetFetch         = 26,

    // Health check
    HealthCheck         = 30,
};

/// Human-readable name for each RPC method.
[[nodiscard]] const char* rpc_method_name(RpcMethod m) noexcept {
    switch (m) {
    case RpcMethod::AppendEntries:    return "AppendEntries";
    case RpcMethod::RequestVote:      return "RequestVote";
    case RpcMethod::InstallSnapshot:  return "InstallSnapshot";
    case RpcMethod::FetchMetadata:    return "FetchMetadata";
    case RpcMethod::RegisterBroker:   return "RegisterBroker";
    case RpcMethod::UpdateMetadata:   return "UpdateMetadata";
    case RpcMethod::FindCoordinator:  return "FindCoordinator";
    case RpcMethod::JoinGroup:        return "JoinGroup";
    case RpcMethod::SyncGroup:        return "SyncGroup";
    case RpcMethod::Heartbeat:        return "Heartbeat";
    case RpcMethod::LeaveGroup:       return "LeaveGroup";
    case RpcMethod::OffsetCommit:     return "OffsetCommit";
    case RpcMethod::OffsetFetch:      return "OffsetFetch";
    case RpcMethod::HealthCheck:      return "HealthCheck";
    default:                          return "Unknown";
    }
}

// =========================================================================
// Logging
// =========================================================================

[[nodiscard]] std::shared_ptr<spdlog::logger> get_grpc_logger() {
    static auto logger = []() {
        auto l = spdlog::get("grpc_server");
        if (!l) {
            l = spdlog::stdout_color_mt("grpc_server");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define GRPC_LOG(level, ...) get_grpc_logger()->level("[grpc] " __VA_ARGS__)

// =========================================================================
// Frame reader/writer for gRPC framing
// =========================================================================

/// Write a big-endian uint32 to a byte buffer.
void write_uint32_be(uint8_t* p, uint32_t v) noexcept {
    p[0] = static_cast<uint8_t>((v >> 24) & 0xFF);
    p[1] = static_cast<uint8_t>((v >> 16) & 0xFF);
    p[2] = static_cast<uint8_t>((v >> 8) & 0xFF);
    p[3] = static_cast<uint8_t>(v & 0xFF);
}

/// Read a big-endian uint32 from a byte buffer.
uint32_t read_uint32_be(const uint8_t* p) noexcept {
    return (static_cast<uint32_t>(p[0]) << 24)
         | (static_cast<uint32_t>(p[1]) << 16)
         | (static_cast<uint32_t>(p[2]) << 8)
         |  static_cast<uint32_t>(p[3]);
}

/// Write a gRPC-framed message to a socket. Returns true on success.
/// The frame format is: [compression:1][length:4 BE][payload...]
bool write_grpc_frame(int fd, const std::vector<uint8_t>& payload) {
    if (payload.size() > kMaxGrpcMessageSize) {
        GRPC_LOG(error, "Message too large for gRPC frame: {} bytes",
                 payload.size());
        return false;
    }

    uint8_t header[kGrpcFrameHeaderSize];
    header[0] = 0; // no compression
    write_uint32_be(header + 1, static_cast<uint32_t>(payload.size()));

    // Write header
    for (size_t off = 0; off < kGrpcFrameHeaderSize; ) {
        ssize_t n = ::write(fd, header + off, kGrpcFrameHeaderSize - off);
        if (n <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Would block — caller should retry
                return false;
            }
            return false;
        }
        off += static_cast<size_t>(n);
    }

    // Write payload
    for (size_t off = 0; off < payload.size(); ) {
        ssize_t n = ::write(fd, payload.data() + off, payload.size() - off);
        if (n <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return false;
            }
            return false;
        }
        off += static_cast<size_t>(n);
    }

    return true;
}

/// Read a gRPC frame from a socket. Returns empty vector on error/EOF.
/// The first byte of the returned vector is the RPC method tag; the rest
/// is the payload.
std::vector<uint8_t> read_grpc_frame(int fd) {
    // Read gRPC header: [compression:1][length:4 BE]
    uint8_t header[kGrpcFrameHeaderSize];
    for (size_t off = 0; off < kGrpcFrameHeaderSize; ) {
        ssize_t n = ::read(fd, header + off, kGrpcFrameHeaderSize - off);
        if (n <= 0) {
            if (n == 0) {
                GRPC_LOG(debug, "Connection closed by peer (fd={})", fd);
            } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                GRPC_LOG(warn, "Read error on fd={}: {}", fd, std::strerror(errno));
            }
            return {};
        }
        off += static_cast<size_t>(n);
    }

    uint8_t compression = header[0];
    uint32_t length = read_uint32_be(header + 1);

    if (length == 0 || length > kMaxGrpcMessageSize) {
        GRPC_LOG(warn, "Invalid gRPC frame length: {} (max={})",
                 length, kMaxGrpcMessageSize);
        return {};
    }

    if (compression != 0) {
        GRPC_LOG(warn, "Compressed gRPC frames not supported (compression={})",
                 compression);
        return {};
    }

    // Read payload
    std::vector<uint8_t> payload(length);
    for (size_t off = 0; off < length; ) {
        ssize_t n = ::read(fd, payload.data() + off, length - off);
        if (n <= 0) {
            GRPC_LOG(warn, "Read error on fd={} during payload: {}",
                     fd, std::strerror(errno));
            return {};
        }
        off += static_cast<size_t>(n);
    }

    return payload;
}

// =========================================================================
// gRPC Connection — per-client connection state
// =========================================================================

/// Represents a single gRPC client connection. Each connection is serviced
/// by a single thread reading frames in a loop.
struct GrpcConnection {
    int fd = -1;
    endpoint peer;
    std::atomic<bool> running{true};
    std::chrono::steady_clock::time_point created_at;

    GrpcConnection(int f, endpoint p)
        : fd(f), peer(std::move(p)),
          created_at(std::chrono::steady_clock::now()) {}

    GrpcConnection(const GrpcConnection&) = delete;
    GrpcConnection& operator=(const GrpcConnection&) = delete;
    GrpcConnection(GrpcConnection&&) = default;
    GrpcConnection& operator=(GrpcConnection&&) = default;

    ~GrpcConnection() {
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }

    [[nodiscard]] double uptime_seconds() const noexcept {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration<double>(now - created_at).count();
    }
};

/// Safe close of a fd.
void safe_close(int& fd) noexcept {
    if (fd < 0) return;
    ::close(fd);
    fd = -1;
}

/// Set socket to non-blocking.
bool set_nonblocking(int fd) noexcept {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) >= 0;
}

} // anonymous namespace

// ============================================================================
// GrpcServer — Implementation
// ============================================================================

namespace torrent::network {

// ============================================================================
// PIMPL — hide implementation details
// ============================================================================

struct GrpcServer::Impl {
    // --- Configuration ---
    endpoint bind_addr;
    uint16_t port = kDefaultGrpcPort;
    int listen_fd = -1;
    int epoll_fd = -1;

    // --- Server state ---
    std::atomic<bool> running{false};
    std::atomic<bool> stopping{false};
    std::thread accept_thread;
    std::vector<std::thread> worker_threads;

    // --- Connection tracking ---
    mutable std::mutex conn_mutex;
    std::unordered_map<int, std::unique_ptr<GrpcConnection>> connections;

    // --- Raft RPC handlers (set by BrokerServer) ---
    using AeHandler = std::function<raft::AppendEntriesResponse(
        const raft::AppendEntriesRequest&)>;
    using RvHandler = std::function<raft::RequestVoteResponse(
        const raft::RequestVoteRequest&)>;
    using IsHandler = std::function<raft::InstallSnapshotResponse(
        const raft::InstallSnapshotRequest&)>;

    AeHandler append_entries_handler;
    RvHandler request_vote_handler;
    IsHandler install_snapshot_handler;

    // --- Metadata service handlers ---
    using FetchMetaHandler = std::function<tp::MetadataResponse(
        const tp::MetadataRequest&)>;
    using RegisterHandler = std::function<tp::PartitionResult(
        broker_id_t broker_id, const endpoint& addr)>;
    using UpdateMetaHandler = std::function<void(
        const tp::MetadataResponse&)>;

    FetchMetaHandler fetch_metadata_handler;
    RegisterHandler register_broker_handler;
    UpdateMetaHandler update_metadata_handler;

    // --- Consumer group handlers ---
    using CoordHandler = std::function<tp::FindCoordinatorResponse(
        const tp::FindCoordinatorRequest&)>;
    using JoinHandler = std::function<tp::JoinGroupResponse(
        const tp::JoinGroupRequest&)>;
    using SyncHandler = std::function<tp::SyncGroupResponse(
        const tp::SyncGroupRequest&)>;
    using HbHandler = std::function<tp::HeartbeatResponse(
        const tp::HeartbeatRequest&)>;
    using LeaveHandler = std::function<tp::LeaveGroupResponse(
        const tp::LeaveGroupRequest&)>;
    using OcHandler = std::function<tp::OffsetCommitResponse(
        const tp::OffsetCommitRequest&)>;
    using OfHandler = std::function<tp::OffsetFetchResponse(
        const tp::OffsetFetchRequest&)>;

    CoordHandler find_coordinator_handler;
    JoinHandler join_group_handler;
    SyncHandler sync_group_handler;
    HbHandler heartbeat_handler;
    LeaveHandler leave_group_handler;
    OcHandler offset_commit_handler;
    OfHandler offset_fetch_handler;

    // --- Metrics ---
    std::atomic<uint64_t> total_requests{0};
    std::atomic<uint64_t> total_errors{0};
    std::atomic<uint64_t> active_connections{0};
};

// ---------------------------------------------------------------------------
// Constructor / Destructor
// ---------------------------------------------------------------------------

GrpcServer::GrpcServer()
    : impl_(std::make_unique<Impl>()) {
    GRPC_LOG(info, "GrpcServer created");
}

GrpcServer::GrpcServer(const endpoint& bind_addr, uint16_t port)
    : impl_(std::make_unique<Impl>()) {
    impl_->bind_addr = bind_addr;
    impl_->port = port > 0 ? port : kDefaultGrpcPort;
    GRPC_LOG(info, "GrpcServer created for {}:{}",
             bind_addr.host, impl_->port);
}

GrpcServer::~GrpcServer() {
    shutdown();
    GRPC_LOG(info, "GrpcServer destroyed");
}

// ---------------------------------------------------------------------------
// Raft RPC handler registration
// ---------------------------------------------------------------------------

void GrpcServer::set_append_entries_handler(
    std::function<raft::AppendEntriesResponse(const raft::AppendEntriesRequest&)> handler) {
    impl_->append_entries_handler = std::move(handler);
    GRPC_LOG(info, "AppendEntries handler registered");
}

void GrpcServer::set_request_vote_handler(
    std::function<raft::RequestVoteResponse(const raft::RequestVoteRequest&)> handler) {
    impl_->request_vote_handler = std::move(handler);
    GRPC_LOG(info, "RequestVote handler registered");
}

void GrpcServer::set_install_snapshot_handler(
    std::function<raft::InstallSnapshotResponse(const raft::InstallSnapshotRequest&)> handler) {
    impl_->install_snapshot_handler = std::move(handler);
    GRPC_LOG(info, "InstallSnapshot handler registered");
}

// ---------------------------------------------------------------------------
// Metadata service handler registration
// ---------------------------------------------------------------------------

void GrpcServer::set_fetch_metadata_handler(
    std::function<tp::MetadataResponse(const tp::MetadataRequest&)> handler) {
    impl_->fetch_metadata_handler = std::move(handler);
    GRPC_LOG(info, "FetchMetadata handler registered");
}

void GrpcServer::set_register_broker_handler(
    std::function<tp::PartitionResult(broker_id_t, const endpoint&)> handler) {
    impl_->register_broker_handler = std::move(handler);
    GRPC_LOG(info, "RegisterBroker handler registered");
}

void GrpcServer::set_update_metadata_handler(
    std::function<void(const tp::MetadataResponse&)> handler) {
    impl_->update_metadata_handler = std::move(handler);
    GRPC_LOG(info, "UpdateMetadata handler registered");
}

// ---------------------------------------------------------------------------
// Consumer group handler registration
// ---------------------------------------------------------------------------

void GrpcServer::set_find_coordinator_handler(
    std::function<tp::FindCoordinatorResponse(const tp::FindCoordinatorRequest&)> handler) {
    impl_->find_coordinator_handler = std::move(handler);
    GRPC_LOG(info, "FindCoordinator handler registered");
}

void GrpcServer::set_join_group_handler(
    std::function<tp::JoinGroupResponse(const tp::JoinGroupRequest&)> handler) {
    impl_->join_group_handler = std::move(handler);
    GRPC_LOG(info, "JoinGroup handler registered");
}

void GrpcServer::set_sync_group_handler(
    std::function<tp::SyncGroupResponse(const tp::SyncGroupRequest&)> handler) {
    impl_->sync_group_handler = std::move(handler);
    GRPC_LOG(info, "SyncGroup handler registered");
}

void GrpcServer::set_heartbeat_handler(
    std::function<tp::HeartbeatResponse(const tp::HeartbeatRequest&)> handler) {
    impl_->heartbeat_handler = std::move(handler);
    GRPC_LOG(info, "Heartbeat handler registered");
}

void GrpcServer::set_leave_group_handler(
    std::function<tp::LeaveGroupResponse(const tp::LeaveGroupRequest&)> handler) {
    impl_->leave_group_handler = std::move(handler);
    GRPC_LOG(info, "LeaveGroup handler registered");
}

void GrpcServer::set_offset_commit_handler(
    std::function<tp::OffsetCommitResponse(const tp::OffsetCommitRequest&)> handler) {
    impl_->offset_commit_handler = std::move(handler);
    GRPC_LOG(info, "OffsetCommit handler registered");
}

void GrpcServer::set_offset_fetch_handler(
    std::function<tp::OffsetFetchResponse(const tp::OffsetFetchRequest&)> handler) {
    impl_->offset_fetch_handler = std::move(handler);
    GRPC_LOG(info, "OffsetFetch handler registered");
}

// ---------------------------------------------------------------------------
// start — begin accepting connections and servicing RPCs
// ---------------------------------------------------------------------------

bool GrpcServer::start() {
    if (impl_->running.load(std::memory_order_acquire)) {
        GRPC_LOG(warn, "GrpcServer already running");
        return false;
    }

    GRPC_LOG(info, "Starting gRPC server on {}:{}",
             impl_->bind_addr.host.empty() ? "*" : impl_->bind_addr.host,
             impl_->port);

    // ---- Create listening socket ----
    impl_->listen_fd = ::socket(AF_INET6, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (impl_->listen_fd < 0) {
        GRPC_LOG(error, "socket() failed: {}", std::strerror(errno));
        return false;
    }

    // Dual-stack
    int v6only = 0;
    setsockopt(impl_->listen_fd, IPPROTO_IPV6, IPV6_V6ONLY,
               &v6only, sizeof(v6only));

    int optval = 1;
    setsockopt(impl_->listen_fd, SOL_SOCKET, SO_REUSEADDR,
               &optval, sizeof(optval));

    // ---- Bind ----
    sockaddr_in6 addr6{};
    addr6.sin6_family = AF_INET6;
    addr6.sin6_port = htons(impl_->port);
    addr6.sin6_addr = in6addr_any;

    if (!impl_->bind_addr.host.empty() && impl_->bind_addr.host != "*") {
        // Resolve the specific host
        // For simplicity, try as IPv4 mapped
        inet_pton(AF_INET6, impl_->bind_addr.host.c_str(), &addr6.sin6_addr);
    }

    if (::bind(impl_->listen_fd, reinterpret_cast<const sockaddr*>(&addr6),
               sizeof(addr6)) < 0) {
        GRPC_LOG(error, "bind() failed: {}", std::strerror(errno));
        safe_close(impl_->listen_fd);
        return false;
    }

    // ---- Listen ----
    if (::listen(impl_->listen_fd, kGrpcBacklog) < 0) {
        GRPC_LOG(error, "listen() failed: {}", std::strerror(errno));
        safe_close(impl_->listen_fd);
        return false;
    }

    GRPC_LOG(info, "gRPC server listening on port {}", impl_->port);

    // ---- Epoll setup ----
    impl_->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (impl_->epoll_fd < 0) {
        GRPC_LOG(error, "epoll_create1() failed: {}", std::strerror(errno));
        safe_close(impl_->listen_fd);
        return false;
    }

    // Add listen socket to epoll
    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = impl_->listen_fd;
    if (epoll_ctl(impl_->epoll_fd, EPOLL_CTL_ADD, impl_->listen_fd, &ev) < 0) {
        GRPC_LOG(error, "epoll_ctl(add listen) failed: {}", std::strerror(errno));
        safe_close(impl_->listen_fd);
        safe_close(impl_->epoll_fd);
        return false;
    }

    impl_->running.store(true, std::memory_order_release);
    impl_->stopping.store(false, std::memory_order_release);

    // ---- Worker threads for servicing connections ----
    int num_workers = std::max(1u, std::thread::hardware_concurrency() / 2);
    impl_->worker_threads.reserve(static_cast<size_t>(num_workers));

    for (int i = 0; i < num_workers; ++i) {
        impl_->worker_threads.emplace_back([this]() {
            worker_loop();
        });
    }

    // ---- Accept thread ----
    impl_->accept_thread = std::thread([this]() {
        accept_loop();
    });

    GRPC_LOG(info, "gRPC server started with {} worker threads", num_workers);
    return true;
}

// ---------------------------------------------------------------------------
// shutdown — gracefully stop the server
// ---------------------------------------------------------------------------

void GrpcServer::shutdown() {
    if (!impl_->running.load(std::memory_order_acquire)) return;

    GRPC_LOG(info, "Shutting down gRPC server...");
    impl_->stopping.store(true, std::memory_order_release);

    // Close listen socket to unblock accept
    if (impl_->listen_fd >= 0) {
        ::shutdown(impl_->listen_fd, SHUT_RDWR);
        safe_close(impl_->listen_fd);
    }

    // Wake epoll by closing it
    if (impl_->epoll_fd >= 0) {
        safe_close(impl_->epoll_fd);
    }

    // Join threads
    if (impl_->accept_thread.joinable()) {
        impl_->accept_thread.join();
        GRPC_LOG(debug, "Accept thread joined");
    }

    for (auto& t : impl_->worker_threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    impl_->worker_threads.clear();
    GRPC_LOG(debug, "Worker threads joined");

    // Close all client connections
    {
        std::lock_guard<std::mutex> lock(impl_->conn_mutex);
        for (auto& [fd, conn] : impl_->connections) {
            conn->running.store(false, std::memory_order_release);
            ::shutdown(fd, SHUT_RDWR);
        }
        impl_->connections.clear();
    }
    impl_->active_connections.store(0, std::memory_order_relaxed);

    impl_->running.store(false, std::memory_order_release);
    GRPC_LOG(info, "gRPC server shutdown complete — total_requests={}, "
            "total_errors={}",
            impl_->total_requests.load(),
            impl_->total_errors.load());
}

// ---------------------------------------------------------------------------
// accept_loop — accept incoming gRPC connections
// ---------------------------------------------------------------------------

void GrpcServer::accept_loop() {
    GRPC_LOG(info, "Accept loop started on fd={}", impl_->listen_fd);

    while (!impl_->stopping.load(std::memory_order_acquire)) {
        sockaddr_in6 peer_sa{};
        socklen_t peer_len = sizeof(peer_sa);

        int client_fd = ::accept4(impl_->listen_fd,
                                   reinterpret_cast<sockaddr*>(&peer_sa),
                                   &peer_len,
                                   SOCK_NONBLOCK | SOCK_CLOEXEC);

        if (client_fd >= 0) {
            // Set TCP_NODELAY
            int one = 1;
            setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

            // Build peer endpoint
            endpoint peer_ep;
            char host[INET6_ADDRSTRLEN] = {};
            inet_ntop(AF_INET6, &peer_sa.sin6_addr, host, sizeof(host));
            peer_ep.host = host;
            peer_ep.port = ntohs(peer_sa.sin6_port);

            // Register in epoll
            struct epoll_event ev{};
            ev.events = EPOLLIN | EPOLLET;  // edge-triggered
            ev.data.fd = client_fd;

            if (epoll_ctl(impl_->epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
                GRPC_LOG(warn, "epoll_ctl(add client {}) failed: {}",
                         client_fd, std::strerror(errno));
                safe_close(client_fd);
                continue;
            }

            // Create connection object
            {
                std::lock_guard<std::mutex> lock(impl_->conn_mutex);
                impl_->connections[client_fd] =
                    std::make_unique<GrpcConnection>(client_fd, peer_ep);
            }
            impl_->active_connections.fetch_add(1, std::memory_order_relaxed);

            GRPC_LOG(debug, "Accepted gRPC connection from {}:{} (fd={}, total={})",
                     peer_ep.host, peer_ep.port, client_fd,
                     impl_->active_connections.load());

        } else {
            int ec = errno;
            if (ec == EAGAIN || ec == EWOULDBLOCK || ec == EINTR) {
                // No pending connections
                std::this_thread::sleep_for(1ms);
                continue;
            }
            if (ec == EBADF || ec == EINVAL) {
                // Socket closed — accept loop should exit
                break;
            }
            GRPC_LOG(warn, "Accept error: {}", std::strerror(ec));
            if (ec == EMFILE || ec == ENFILE) {
                std::this_thread::sleep_for(100ms);
            }
        }
    }

    GRPC_LOG(info, "Accept loop ended");
}

// ---------------------------------------------------------------------------
// worker_loop — epoll-based worker servicing gRPC connections
// ---------------------------------------------------------------------------

void GrpcServer::worker_loop() {
    GRPC_LOG(debug, "Worker thread started");

    std::array<struct epoll_event, kEpollMaxEvents> events;

    while (!impl_->stopping.load(std::memory_order_acquire)) {
        int nfds = epoll_wait(impl_->epoll_fd, events.data(),
                              static_cast<int>(events.size()), 100);

        if (nfds < 0) {
            if (errno == EINTR) continue;
            if (errno == EBADF) break;  // epoll fd closed
            GRPC_LOG(error, "epoll_wait error: {}", std::strerror(errno));
            break;
        }

        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;

            // Skip listen socket (handled by accept thread)
            if (fd == impl_->listen_fd) continue;

            if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                // Connection error or hangup — remove
                remove_connection(fd);
                continue;
            }

            if (events[i].events & EPOLLIN) {
                handle_client_readable(fd);
            }
        }
    }

    GRPC_LOG(debug, "Worker thread ended");
}

// ---------------------------------------------------------------------------
// handle_client_readable — read and dispatch one gRPC frame
// ---------------------------------------------------------------------------

void GrpcServer::handle_client_readable(int fd) {
    auto frame = read_grpc_frame(fd);
    if (frame.empty()) {
        // EOF or error
        remove_connection(fd);
        return;
    }

    if (frame.size() < 1) {
        GRPC_LOG(warn, "Empty gRPC frame from fd={}", fd);
        return;
    }

    // First byte is the RPC method tag
    RpcMethod method = static_cast<RpcMethod>(frame[0]);

    // Remaining bytes are the serialized request
    std::vector<uint8_t> payload(frame.begin() + 1, frame.end());

    impl_->total_requests.fetch_add(1, std::memory_order_relaxed);

    // Dispatched
    std::vector<uint8_t> response = dispatch_rpc(method, payload);

    // Prepend method tag to response
    response.insert(response.begin(), static_cast<uint8_t>(method));

    // Write response
    if (!write_grpc_frame(fd, response)) {
        GRPC_LOG(warn, "Failed to write gRPC response to fd={}", fd);
        remove_connection(fd);
    }
}

// ---------------------------------------------------------------------------
// dispatch_rpc — route to the appropriate handler
// ---------------------------------------------------------------------------

std::vector<uint8_t> GrpcServer::dispatch_rpc(
    RpcMethod method, const std::vector<uint8_t>& payload) {
    std::vector<uint8_t> response;

    try {
        switch (method) {
        // ---- Raft RPCs ----
        case RpcMethod::AppendEntries: {
            if (!impl_->append_entries_handler) {
                response = serialize_error_response(
                    error_code::unknown_server_error,
                    "AppendEntries handler not registered");
                break;
            }
            auto req = deserialize_append_entries_request(payload);
            auto res = impl_->append_entries_handler(req);
            response = serialize_append_entries_response(res);
            break;
        }
        case RpcMethod::RequestVote: {
            if (!impl_->request_vote_handler) {
                response = serialize_error_response(
                    error_code::unknown_server_error,
                    "RequestVote handler not registered");
                break;
            }
            auto req = deserialize_request_vote_request(payload);
            auto res = impl_->request_vote_handler(req);
            response = serialize_request_vote_response(res);
            break;
        }
        case RpcMethod::InstallSnapshot: {
            if (!impl_->install_snapshot_handler) {
                response = serialize_error_response(
                    error_code::unknown_server_error,
                    "InstallSnapshot handler not registered");
                break;
            }
            auto req = deserialize_install_snapshot_request(payload);
            auto res = impl_->install_snapshot_handler(req);
            response = serialize_install_snapshot_response(res);
            break;
        }

        // ---- Metadata service ----
        case RpcMethod::FetchMetadata: {
            if (!impl_->fetch_metadata_handler) {
                response = serialize_error_response(
                    error_code::unknown_server_error,
                    "FetchMetadata handler not registered");
                break;
            }
            auto req = deserialize_metadata_request(payload);
            auto res = impl_->fetch_metadata_handler(req);
            response = serialize_metadata_response(res);
            break;
        }
        case RpcMethod::RegisterBroker: {
            if (!impl_->register_broker_handler) {
                response = serialize_error_response(
                    error_code::unknown_server_error,
                    "RegisterBroker handler not registered");
                break;
            }
            // Deserialize broker_id + endpoint from payload
            auto [broker_id, addr] = deserialize_register_broker_request(payload);
            auto res = impl_->register_broker_handler(broker_id, addr);
            response = serialize_partition_result(res);
            break;
        }
        case RpcMethod::UpdateMetadata: {
            if (!impl_->update_metadata_handler) {
                response = serialize_error_response(
                    error_code::unknown_server_error,
                    "UpdateMetadata handler not registered");
                break;
            }
            auto req = deserialize_metadata_response(payload);
            impl_->update_metadata_handler(req);
            response = serialize_error_response(error_code::none, "");
            break;
        }

        // ---- Consumer group service ----
        case RpcMethod::FindCoordinator: {
            if (!impl_->find_coordinator_handler) {
                response = serialize_error_response(
                    error_code::unknown_server_error,
                    "FindCoordinator handler not registered");
                break;
            }
            auto req = deserialize_find_coordinator_request(payload);
            auto res = impl_->find_coordinator_handler(req);
            response = serialize_find_coordinator_response(res);
            break;
        }
        case RpcMethod::JoinGroup: {
            if (!impl_->join_group_handler) {
                response = serialize_error_response(
                    error_code::unknown_server_error,
                    "JoinGroup handler not registered");
                break;
            }
            auto req = deserialize_join_group_request(payload);
            auto res = impl_->join_group_handler(req);
            response = serialize_join_group_response(res);
            break;
        }
        case RpcMethod::SyncGroup: {
            if (!impl_->sync_group_handler) {
                response = serialize_error_response(
                    error_code::unknown_server_error,
                    "SyncGroup handler not registered");
                break;
            }
            auto req = deserialize_sync_group_request(payload);
            auto res = impl_->sync_group_handler(req);
            response = serialize_sync_group_response(res);
            break;
        }
        case RpcMethod::Heartbeat: {
            if (!impl_->heartbeat_handler) {
                response = serialize_error_response(
                    error_code::unknown_server_error,
                    "Heartbeat handler not registered");
                break;
            }
            auto req = deserialize_heartbeat_request(payload);
            auto res = impl_->heartbeat_handler(req);
            response = serialize_heartbeat_response(res);
            break;
        }
        case RpcMethod::LeaveGroup: {
            if (!impl_->leave_group_handler) {
                response = serialize_error_response(
                    error_code::unknown_server_error,
                    "LeaveGroup handler not registered");
                break;
            }
            auto req = deserialize_leave_group_request(payload);
            auto res = impl_->leave_group_handler(req);
            response = serialize_leave_group_response(res);
            break;
        }
        case RpcMethod::OffsetCommit: {
            if (!impl_->offset_commit_handler) {
                response = serialize_error_response(
                    error_code::unknown_server_error,
                    "OffsetCommit handler not registered");
                break;
            }
            auto req = deserialize_offset_commit_request(payload);
            auto res = impl_->offset_commit_handler(req);
            response = serialize_offset_commit_response(res);
            break;
        }
        case RpcMethod::OffsetFetch: {
            if (!impl_->offset_fetch_handler) {
                response = serialize_error_response(
                    error_code::unknown_server_error,
                    "OffsetFetch handler not registered");
                break;
            }
            auto req = deserialize_offset_fetch_request(payload);
            auto res = impl_->offset_fetch_handler(req);
            response = serialize_offset_fetch_response(res);
            break;
        }

        // ---- Health check ----
        case RpcMethod::HealthCheck: {
            response = serialize_error_response(error_code::none, "OK");
            break;
        }

        default:
            GRPC_LOG(warn, "Unknown RPC method: {}",
                     static_cast<int>(method));
            response = serialize_error_response(
                error_code::unsupported_version,
                "Unknown RPC method: " + std::to_string(static_cast<int>(method)));
            break;
        }
    } catch (const std::exception& ex) {
        GRPC_LOG(error, "Exception in {} handler: {}",
                 rpc_method_name(method), ex.what());
        impl_->total_errors.fetch_add(1, std::memory_order_relaxed);
        response = serialize_error_response(
            error_code::unknown_server_error, ex.what());
    }

    return response;
}

// ---------------------------------------------------------------------------
// remove_connection — clean up a client connection
// ---------------------------------------------------------------------------

void GrpcServer::remove_connection(int fd) {
    std::lock_guard<std::mutex> lock(impl_->conn_mutex);
    auto it = impl_->connections.find(fd);
    if (it != impl_->connections.end()) {
        GRPC_LOG(debug, "Removing gRPC connection fd={} peer={}",
                 fd, it->second->peer.to_string());
        it->second->running.store(false, std::memory_order_release);
        epoll_ctl(impl_->epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
        impl_->connections.erase(it);
        impl_->active_connections.fetch_sub(1, std::memory_order_relaxed);
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

uint64_t GrpcServer::total_requests() const {
    return impl_->total_requests.load(std::memory_order_relaxed);
}

uint64_t GrpcServer::total_errors() const {
    return impl_->total_errors.load(std::memory_order_relaxed);
}

uint64_t GrpcServer::active_connections() const {
    return impl_->active_connections.load(std::memory_order_relaxed);
}

bool GrpcServer::is_running() const {
    return impl_->running.load(std::memory_order_acquire);
}

// ============================================================================
// Internal deserialization helpers (thin wrappers around ProtocolSerializer)
// These are inline so grpc_server.cpp is self-contained without needing
// a separate protobuf-generated library.
// ============================================================================

namespace {

// --- Big-endian read/write helpers ---

void write_be_uint32(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
}

void write_be_uint64(std::vector<uint8_t>& buf, uint64_t v) {
    for (int i = 7; i >= 0; --i)
        buf.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
}

void write_be_int64(std::vector<uint8_t>& buf, int64_t v) {
    write_be_uint64(buf, static_cast<uint64_t>(v));
}

void write_bytes(std::vector<uint8_t>& buf, const std::vector<uint8_t>& v) {
    write_be_uint32(buf, static_cast<uint32_t>(v.size()));
    buf.insert(buf.end(), v.begin(), v.end());
}

uint32_t read_be_uint32(const uint8_t*& p) {
    uint32_t v = (static_cast<uint32_t>(p[0]) << 24)
               | (static_cast<uint32_t>(p[1]) << 16)
               | (static_cast<uint32_t>(p[2]) << 8)
               |  static_cast<uint32_t>(p[3]);
    p += 4;
    return v;
}

uint64_t read_be_uint64(const uint8_t*& p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
        v = (v << 8) | static_cast<uint64_t>(p[i]);
    p += 8;
    return v;
}

int64_t read_be_int64(const uint8_t*& p) {
    return static_cast<int64_t>(read_be_uint64(p));
}

std::vector<uint8_t> read_bytes(const uint8_t*& p) {
    uint32_t len = read_be_uint32(p);
    std::vector<uint8_t> result(p, p + len);
    p += len;
    return result;
}

// --- Serialization of error responses ---

std::vector<uint8_t> serialize_error_response(
    torrent::error_code ec, const std::string& msg) {
    std::vector<uint8_t> buf;
    // error_code as int16 BE
    int16_t ec_raw = static_cast<int16_t>(ec);
    buf.push_back(static_cast<uint8_t>((ec_raw >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(ec_raw & 0xFF));
    // message length + data
    write_be_uint32(buf, static_cast<uint32_t>(msg.size()));
    buf.insert(buf.end(), msg.begin(), msg.end());
    return buf;
}

// --- Raft RPC serialization/deserialization ---

raft::AppendEntriesRequest deserialize_append_entries_request(
    const std::vector<uint8_t>& payload) {
    raft::AppendEntriesRequest req;
    const uint8_t* p = payload.data();
    const uint8_t* end = p + payload.size();
    (void)end;

    req.term = static_cast<raft::RaftTerm>(read_be_uint64(p));
    req.leader_id = static_cast<raft::RaftNodeId>(static_cast<int32_t>(read_be_uint64(p)));
    req.prev_log_index = read_be_int64(p);
    req.prev_log_term = read_be_int64(p);
    req.leader_commit = read_be_int64(p);

    uint32_t entry_count = read_be_uint32(p);
    req.entries.reserve(entry_count);
    for (uint32_t i = 0; i < entry_count; ++i) {
        raft::LogEntry entry;
        entry.term = read_be_int64(p);
        entry.index = read_be_int64(p);
        uint8_t cmd_type = *p++;
        if (cmd_type == 0) {
            entry.command = raft::NoOpCommand{};
            // skip 4 bytes padding
            p += 4;
        } else if (cmd_type == 1) {
            raft::ConfigChangeCommand cc;
            cc.joint_consensus = (*p++ != 0);
            // read members
            uint32_t count = read_be_uint32(p);
            for (uint32_t j = 0; j < count; ++j)
                cc.new_members.push_back(static_cast<raft::RaftNodeId>(static_cast<int32_t>(read_be_uint64(p))));
            entry.command = cc;
        } else {
            raft::UserCommand uc;
            uc.payload = read_bytes(p);
            entry.command = uc;
        }
        req.entries.push_back(std::move(entry));
    }

    return req;
}

std::vector<uint8_t> serialize_append_entries_response(
    const raft::AppendEntriesResponse& res) {
    std::vector<uint8_t> buf;
    write_be_uint64(buf, static_cast<uint64_t>(res.term));
    buf.push_back(res.success ? 1 : 0);
    write_be_int64(buf, res.last_log_index);
    write_be_int64(buf, res.conflict_index);
    return buf;
}

raft::RequestVoteRequest deserialize_request_vote_request(
    const std::vector<uint8_t>& payload) {
    raft::RequestVoteRequest req;
    const uint8_t* p = payload.data();
    req.term = static_cast<raft::RaftTerm>(read_be_uint64(p));
    req.candidate_id = static_cast<raft::RaftNodeId>(static_cast<int32_t>(read_be_uint64(p)));
    req.last_log_index = read_be_int64(p);
    req.last_log_term = read_be_int64(p);
    req.pre_vote = (*p++ != 0);
    return req;
}

std::vector<uint8_t> serialize_request_vote_response(
    const raft::RequestVoteResponse& res) {
    std::vector<uint8_t> buf;
    write_be_uint64(buf, static_cast<uint64_t>(res.term));
    buf.push_back(res.vote_granted ? 1 : 0);
    return buf;
}

raft::InstallSnapshotRequest deserialize_install_snapshot_request(
    const std::vector<uint8_t>& payload) {
    raft::InstallSnapshotRequest req;
    const uint8_t* p = payload.data();
    req.term = static_cast<raft::RaftTerm>(read_be_uint64(p));
    req.leader_id = static_cast<raft::RaftNodeId>(static_cast<int32_t>(read_be_uint64(p)));
    req.last_included_index = read_be_int64(p);
    req.last_included_term = read_be_int64(p);
    req.offset = read_be_uint64(p);
    req.data = read_bytes(p);
    req.done = (*p++ != 0);
    return req;
}

std::vector<uint8_t> serialize_install_snapshot_response(
    const raft::InstallSnapshotResponse& res) {
    std::vector<uint8_t> buf;
    write_be_uint64(buf, static_cast<uint64_t>(res.term));
    return buf;
}

// --- Metadata service serialization ---

tp::MetadataRequest deserialize_metadata_request(
    const std::vector<uint8_t>& payload) {
    tp::MetadataRequest req;
    const uint8_t* p = payload.data();
    const uint8_t* end = p + payload.size();
    (void)end;

    uint32_t topic_count = read_be_uint32(p);
    for (uint32_t i = 0; i < topic_count; ++i) {
        uint32_t len = read_be_uint32(p);
        req.topics.emplace_back(reinterpret_cast<const char*>(p), len);
        p += len;
    }
    req.allow_auto_create = (*p++ != 0);
    req.include_cluster_authorized_operations = (*p++ != 0);
    req.include_topic_authorized_operations = (*p++ != 0);

    return req;
}

std::vector<uint8_t> serialize_metadata_response(
    const tp::MetadataResponse& res) {
    std::vector<uint8_t> buf;
    write_be_uint32(buf, static_cast<uint32_t>(res.throttle_time_ms));

    write_be_uint32(buf, static_cast<uint32_t>(res.brokers.size()));
    for (const auto& b : res.brokers) {
        write_be_uint32(buf, static_cast<uint32_t>(b.size()));
        buf.insert(buf.end(), b.begin(), b.end());
    }

    write_be_uint32(buf, static_cast<uint32_t>(res.topics.size()));
    for (const auto& t : res.topics) {
        write_be_uint32(buf, static_cast<uint32_t>(t.size()));
        buf.insert(buf.end(), t.begin(), t.end());
    }

    write_be_uint32(buf, static_cast<uint32_t>(res.partition_counts.size()));
    for (auto pc : res.partition_counts) {
        write_be_uint32(buf, static_cast<uint32_t>(pc));
    }

    int16_t ec = static_cast<int16_t>(res.error_code);
    buf.push_back(static_cast<uint8_t>((ec >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(ec & 0xFF));

    return buf;
}

tp::MetadataResponse deserialize_metadata_response(
    const std::vector<uint8_t>& payload) {
    tp::MetadataResponse res;
    const uint8_t* p = payload.data();

    res.throttle_time_ms = static_cast<int32_t>(read_be_uint32(p));

    uint32_t broker_count = read_be_uint32(p);
    for (uint32_t i = 0; i < broker_count; ++i) {
        uint32_t len = read_be_uint32(p);
        res.brokers.emplace_back(reinterpret_cast<const char*>(p), len);
        p += len;
    }

    uint32_t topic_count = read_be_uint32(p);
    for (uint32_t i = 0; i < topic_count; ++i) {
        uint32_t len = read_be_uint32(p);
        res.topics.emplace_back(reinterpret_cast<const char*>(p), len);
        p += len;
    }

    uint32_t pc_count = read_be_uint32(p);
    for (uint32_t i = 0; i < pc_count; ++i) {
        res.partition_counts.push_back(static_cast<int32_t>(read_be_uint32(p)));
    }

    uint8_t hi = *p++; uint8_t lo = *p++;
    res.error_code = static_cast<tp::ErrorCode>((static_cast<int16_t>(hi) << 8) | lo);

    return res;
}

std::pair<broker_id_t, endpoint> deserialize_register_broker_request(
    const std::vector<uint8_t>& payload) {
    const uint8_t* p = payload.data();

    broker_id_t broker_id = static_cast<broker_id_t>(static_cast<int32_t>(read_be_uint64(p)));

    endpoint addr;
    uint32_t host_len = read_be_uint32(p);
    addr.host.assign(reinterpret_cast<const char*>(p), host_len);
    p += host_len;
    addr.port = static_cast<uint16_t>(read_be_uint32(p));

    return {broker_id, addr};
}

std::vector<uint8_t> serialize_partition_result(
    const tp::PartitionResult& res) {
    std::vector<uint8_t> buf;
    write_be_uint32(buf, static_cast<uint32_t>(res.topic.size()));
    buf.insert(buf.end(), res.topic.begin(), res.topic.end());
    write_be_uint32(buf, static_cast<uint32_t>(res.partition));

    int16_t ec = static_cast<int16_t>(res.error_code);
    buf.push_back(static_cast<uint8_t>((ec >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(ec & 0xFF));

    write_be_uint32(buf, static_cast<uint32_t>(res.error_msg.size()));
    buf.insert(buf.end(), res.error_msg.begin(), res.error_msg.end());

    return buf;
}

// --- Consumer group service serialization ---

tp::FindCoordinatorRequest deserialize_find_coordinator_request(
    const std::vector<uint8_t>& payload) {
    tp::FindCoordinatorRequest req;
    const uint8_t* p = payload.data();

    uint32_t key_len = read_be_uint32(p);
    req.key.assign(reinterpret_cast<const char*>(p), key_len);
    p += key_len;
    req.type = static_cast<int8_t>(*p++);

    return req;
}

std::vector<uint8_t> serialize_find_coordinator_response(
    const tp::FindCoordinatorResponse& res) {
    std::vector<uint8_t> buf;
    write_be_uint32(buf, static_cast<uint32_t>(res.throttle_time_ms));

    int16_t ec = static_cast<int16_t>(res.error_code);
    buf.push_back(static_cast<uint8_t>((ec >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(ec & 0xFF));

    write_be_uint32(buf, static_cast<uint32_t>(res.node_id));

    write_be_uint32(buf, static_cast<uint32_t>(res.host.size()));
    buf.insert(buf.end(), res.host.begin(), res.host.end());

    write_be_uint32(buf, static_cast<uint32_t>(res.port));

    return buf;
}

tp::JoinGroupRequest deserialize_join_group_request(
    const std::vector<uint8_t>& payload) {
    tp::JoinGroupRequest req;
    const uint8_t* p = payload.data();

    uint32_t glen = read_be_uint32(p);
    req.group_id.assign(reinterpret_cast<const char*>(p), glen); p += glen;

    req.session_timeout_ms = static_cast<int32_t>(read_be_uint32(p));
    req.rebalance_timeout_ms = static_cast<int32_t>(read_be_uint32(p));

    uint32_t mlen = read_be_uint32(p);
    req.member_id.assign(reinterpret_cast<const char*>(p), mlen); p += mlen;

    uint32_t ilen = read_be_uint32(p);
    req.group_instance_id.assign(reinterpret_cast<const char*>(p), ilen); p += ilen;

    uint32_t plen = read_be_uint32(p);
    req.protocol_type.assign(reinterpret_cast<const char*>(p), plen); p += plen;

    uint32_t proto_count = read_be_uint32(p);
    for (uint32_t i = 0; i < proto_count; ++i) {
        uint32_t slen = read_be_uint32(p);
        req.protocols.emplace_back(reinterpret_cast<const char*>(p), slen);
        p += slen;
    }

    uint32_t meta_count = read_be_uint32(p);
    for (uint32_t i = 0; i < meta_count; ++i) {
        req.protocol_metadata.push_back(read_bytes(p));
    }

    return req;
}

std::vector<uint8_t> serialize_join_group_response(
    const tp::JoinGroupResponse& res) {
    std::vector<uint8_t> buf;
    write_be_uint32(buf, static_cast<uint32_t>(res.throttle_time_ms));

    int16_t ec = static_cast<int16_t>(res.error_code);
    buf.push_back(static_cast<uint8_t>((ec >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(ec & 0xFF));

    write_be_uint32(buf, static_cast<uint32_t>(res.generation_id));

    write_be_uint32(buf, static_cast<uint32_t>(res.protocol_name.size()));
    buf.insert(buf.end(), res.protocol_name.begin(), res.protocol_name.end());

    write_be_uint32(buf, static_cast<uint32_t>(res.leader_id.size()));
    buf.insert(buf.end(), res.leader_id.begin(), res.leader_id.end());

    write_be_uint32(buf, static_cast<uint32_t>(res.member_id.size()));
    buf.insert(buf.end(), res.member_id.begin(), res.member_id.end());

    write_be_uint32(buf, static_cast<uint32_t>(res.members.size()));
    for (const auto& m : res.members) {
        write_be_uint32(buf, static_cast<uint32_t>(m.size()));
        buf.insert(buf.end(), m.begin(), m.end());
    }

    return buf;
}

tp::SyncGroupRequest deserialize_sync_group_request(
    const std::vector<uint8_t>& payload) {
    tp::SyncGroupRequest req;
    const uint8_t* p = payload.data();

    uint32_t glen = read_be_uint32(p);
    req.group_id.assign(reinterpret_cast<const char*>(p), glen); p += glen;

    req.generation_id = static_cast<int32_t>(read_be_uint32(p));

    uint32_t mlen = read_be_uint32(p);
    req.member_id.assign(reinterpret_cast<const char*>(p), mlen); p += mlen;

    uint32_t ilen = read_be_uint32(p);
    req.group_instance_id.assign(reinterpret_cast<const char*>(p), ilen); p += ilen;

    uint32_t tlen = read_be_uint32(p);
    req.protocol_type.assign(reinterpret_cast<const char*>(p), tlen); p += tlen;

    uint32_t nlen = read_be_uint32(p);
    req.protocol_name.assign(reinterpret_cast<const char*>(p), nlen); p += nlen;

    uint32_t assign_count = read_be_uint32(p);
    for (uint32_t i = 0; i < assign_count; ++i) {
        req.assignments.push_back(read_bytes(p));
    }

    return req;
}

std::vector<uint8_t> serialize_sync_group_response(
    const tp::SyncGroupResponse& res) {
    std::vector<uint8_t> buf;
    write_be_uint32(buf, static_cast<uint32_t>(res.throttle_time_ms));

    int16_t ec = static_cast<int16_t>(res.error_code);
    buf.push_back(static_cast<uint8_t>((ec >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(ec & 0xFF));

    write_be_uint32(buf, static_cast<uint32_t>(res.protocol_type.size()));
    buf.insert(buf.end(), res.protocol_type.begin(), res.protocol_type.end());

    write_be_uint32(buf, static_cast<uint32_t>(res.protocol_name.size()));
    buf.insert(buf.end(), res.protocol_name.begin(), res.protocol_name.end());

    write_be_uint32(buf, static_cast<uint32_t>(res.assignment.size()));
    buf.insert(buf.end(), res.assignment.begin(), res.assignment.end());

    return buf;
}

tp::HeartbeatRequest deserialize_heartbeat_request(
    const std::vector<uint8_t>& payload) {
    tp::HeartbeatRequest req;
    const uint8_t* p = payload.data();

    uint32_t glen = read_be_uint32(p);
    req.group_id.assign(reinterpret_cast<const char*>(p), glen); p += glen;

    req.generation_id = static_cast<int32_t>(read_be_uint32(p));

    uint32_t mlen = read_be_uint32(p);
    req.member_id.assign(reinterpret_cast<const char*>(p), mlen); p += mlen;

    uint32_t ilen = read_be_uint32(p);
    req.group_instance_id.assign(reinterpret_cast<const char*>(p), ilen); p += ilen;

    return req;
}

std::vector<uint8_t> serialize_heartbeat_response(
    const tp::HeartbeatResponse& res) {
    std::vector<uint8_t> buf;
    write_be_uint32(buf, static_cast<uint32_t>(res.throttle_time_ms));

    int16_t ec = static_cast<int16_t>(res.error_code);
    buf.push_back(static_cast<uint8_t>((ec >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(ec & 0xFF));

    return buf;
}

tp::LeaveGroupRequest deserialize_leave_group_request(
    const std::vector<uint8_t>& payload) {
    tp::LeaveGroupRequest req;
    const uint8_t* p = payload.data();

    uint32_t glen = read_be_uint32(p);
    req.group_id.assign(reinterpret_cast<const char*>(p), glen); p += glen;

    uint32_t mlen = read_be_uint32(p);
    req.member_id.assign(reinterpret_cast<const char*>(p), mlen); p += mlen;

    uint32_t count = read_be_uint32(p);
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t slen = read_be_uint32(p);
        req.members.emplace_back(reinterpret_cast<const char*>(p), slen);
        p += slen;
    }

    return req;
}

std::vector<uint8_t> serialize_leave_group_response(
    const tp::LeaveGroupResponse& res) {
    std::vector<uint8_t> buf;
    write_be_uint32(buf, static_cast<uint32_t>(res.throttle_time_ms));

    int16_t ec = static_cast<int16_t>(res.error_code);
    buf.push_back(static_cast<uint8_t>((ec >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(ec & 0xFF));

    return buf;
}

tp::OffsetCommitRequest deserialize_offset_commit_request(
    const std::vector<uint8_t>& payload) {
    tp::OffsetCommitRequest req;
    const uint8_t* p = payload.data();

    uint32_t glen = read_be_uint32(p);
    req.group_id.assign(reinterpret_cast<const char*>(p), glen); p += glen;

    req.generation_id = static_cast<int32_t>(read_be_uint32(p));

    uint32_t mlen = read_be_uint32(p);
    req.member_id.assign(reinterpret_cast<const char*>(p), mlen); p += mlen;

    uint32_t ilen = read_be_uint32(p);
    req.group_instance_id.assign(reinterpret_cast<const char*>(p), ilen); p += ilen;

    req.retention_ms = read_be_int64(p);

    uint32_t count = read_be_uint32(p);
    for (uint32_t i = 0; i < count; ++i) {
        tp::TopicPartition tp;
        uint32_t tlen = read_be_uint32(p);
        tp.topic.assign(reinterpret_cast<const char*>(p), tlen); p += tlen;
        tp.partition = static_cast<tp::PartitionIndex>(static_cast<int32_t>(read_be_uint32(p)));
        req.partitions.push_back(tp);
        req.offsets.push_back(read_be_int64(p));

        uint32_t mdlen = read_be_uint32(p);
        req.metadata.emplace_back(reinterpret_cast<const char*>(p), mdlen);
        p += mdlen;
    }

    return req;
}

std::vector<uint8_t> serialize_offset_commit_response(
    const tp::OffsetCommitResponse& res) {
    std::vector<uint8_t> buf;
    write_be_uint32(buf, static_cast<uint32_t>(res.throttle_time_ms));

    write_be_uint32(buf, static_cast<uint32_t>(res.results.size()));
    for (const auto& r : res.results) {
        write_be_uint32(buf, static_cast<uint32_t>(r.topic.size()));
        buf.insert(buf.end(), r.topic.begin(), r.topic.end());

        write_be_uint32(buf, static_cast<uint32_t>(r.partition));

        int16_t ec = static_cast<int16_t>(r.error_code);
        buf.push_back(static_cast<uint8_t>((ec >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(ec & 0xFF));

        write_be_uint32(buf, static_cast<uint32_t>(r.error_msg.size()));
        buf.insert(buf.end(), r.error_msg.begin(), r.error_msg.end());
    }

    return buf;
}

tp::OffsetFetchRequest deserialize_offset_fetch_request(
    const std::vector<uint8_t>& payload) {
    tp::OffsetFetchRequest req;
    const uint8_t* p = payload.data();

    uint32_t glen = read_be_uint32(p);
    req.group_id.assign(reinterpret_cast<const char*>(p), glen); p += glen;

    uint32_t count = read_be_uint32(p);
    for (uint32_t i = 0; i < count; ++i) {
        tp::TopicPartition tp;
        uint32_t tlen = read_be_uint32(p);
        tp.topic.assign(reinterpret_cast<const char*>(p), tlen); p += tlen;
        tp.partition = static_cast<tp::PartitionIndex>(static_cast<int32_t>(read_be_uint32(p)));
        req.partitions.push_back(tp);
    }

    return req;
}

std::vector<uint8_t> serialize_offset_fetch_response(
    const tp::OffsetFetchResponse& res) {
    std::vector<uint8_t> buf;
    write_be_uint32(buf, static_cast<uint32_t>(res.throttle_time_ms));

    int16_t ec = static_cast<int16_t>(res.error_code);
    buf.push_back(static_cast<uint8_t>((ec >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(ec & 0xFF));

    write_be_uint32(buf, static_cast<uint32_t>(res.offsets.size()));
    for (const auto& po : res.offsets) {
        write_be_uint32(buf, static_cast<uint32_t>(po.topic.size()));
        buf.insert(buf.end(), po.topic.begin(), po.topic.end());

        write_be_uint32(buf, static_cast<uint32_t>(po.partition));
        write_be_int64(buf, po.offset);
        write_be_uint32(buf, static_cast<uint32_t>(po.leader_epoch));

        int16_t off_ec = static_cast<int16_t>(po.error_code);
        buf.push_back(static_cast<uint8_t>((off_ec >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(off_ec & 0xFF));
    }

    return buf;
}

} // anonymous namespace

} // namespace torrent::network
