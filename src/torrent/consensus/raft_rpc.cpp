/**
 * raft_rpc.cpp — Raft RPC Transport Layer
 *
 * Persistent-connection RPC dispatch for AppendEntries, RequestVote, and
 * InstallSnapshot with retry, exponential backoff, connection pooling.
 *
 * Wire format: [length:4 BE][corr_id:8 LE][msg_type:1][payload...]
 */

#include "torrent/consensus/raft_types.h"
#include "torrent/common/types.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <spdlog/spdlog.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace torrent::raft {

// ============================================================================
// Constants
// ============================================================================

namespace {
static constexpr auto kDefaultRpcTimeout = std::chrono::milliseconds(500);
static constexpr auto kMaxBackoff = std::chrono::seconds(5);
static constexpr auto kConnectionIdleTimeout = std::chrono::seconds(30);
static constexpr auto kHealthCheckInterval = std::chrono::seconds(5);
static constexpr int kSocketRcvBuf = 256 * 1024;
static constexpr int kSocketSndBuf = 256 * 1024;
static constexpr size_t kMaxMessageSize = 64 * 1024 * 1024;
static constexpr uint8_t kMsgAppendEntries   = 0;
static constexpr uint8_t kMsgRequestVote     = 1;
static constexpr uint8_t kMsgInstallSnapshot = 2;
static constexpr int kEpollMaxEvents = 64;
}  // namespace

// ============================================================================
// DNS resolution
// ============================================================================

static bool resolve_peer(std::string_view addr, sockaddr_in& out) {
    auto colon = addr.rfind(':');
    if (colon == std::string_view::npos) return false;
    std::string host(addr.substr(0, colon));
    std::string port_str(addr.substr(colon + 1));
    int port = std::atoi(port_str.c_str());
    if (port <= 0 || port > 65535) return false;

    struct addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    int rc = ::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res);
    if (rc != 0 || !res) return false;
    std::memcpy(&out, res->ai_addr, sizeof(sockaddr_in));
    ::freeaddrinfo(res);
    return true;
}

// ============================================================================
// Length-delimited framing
// ============================================================================

static bool write_frame(int fd, const uint8_t* data, size_t len) {
    uint8_t hdr[4] = {
        static_cast<uint8_t>((len >> 24) & 0xFF),
        static_cast<uint8_t>((len >> 16) & 0xFF),
        static_cast<uint8_t>((len >> 8) & 0xFF),
        static_cast<uint8_t>(len & 0xFF)};

    for (size_t off = 0; off < 4; ) {
        ssize_t n = ::write(fd, hdr + off, 4 - off);
        if (n <= 0) return (errno == EAGAIN || errno == EWOULDBLOCK);
        off += static_cast<size_t>(n);
    }
    for (size_t off = 0; off < len; ) {
        ssize_t n = ::write(fd, data + off, len - off);
        if (n <= 0) return (errno == EAGAIN || errno == EWOULDBLOCK);
        off += static_cast<size_t>(n);
    }
    return true;
}

static std::vector<uint8_t> read_frame(int fd) {
    uint8_t hdr[4];
    for (size_t off = 0; off < 4; ) {
        ssize_t n = ::read(fd, hdr + off, 4 - off);
        if (n <= 0) return {};
        off += static_cast<size_t>(n);
    }
    uint32_t len = (static_cast<uint32_t>(hdr[0]) << 24)
                 | (static_cast<uint32_t>(hdr[1]) << 16)
                 | (static_cast<uint32_t>(hdr[2]) << 8)
                 | static_cast<uint32_t>(hdr[3]);
    if (len == 0 || len > kMaxMessageSize) return {};

    std::vector<uint8_t> buf(len);
    for (size_t off = 0; off < len; ) {
        ssize_t n = ::read(fd, buf.data() + off, len - off);
        if (n <= 0) return {};
        off += static_cast<size_t>(n);
    }
    return buf;
}

// ============================================================================
// Binary encoding helpers
// ============================================================================

namespace {

void enc_u8(std::vector<uint8_t>& b, uint8_t v) { b.push_back(v); }
void enc_u32(std::vector<uint8_t>& b, uint32_t v) {
    for (int i = 0; i < 4; i++) b.push_back(static_cast<uint8_t>((v >> (i*8)) & 0xFF));
}
void enc_u64(std::vector<uint8_t>& b, uint64_t v) {
    for (int i = 0; i < 8; i++) b.push_back(static_cast<uint8_t>((v >> (i*8)) & 0xFF));
}
void enc_i64(std::vector<uint8_t>& b, int64_t v) { enc_u64(b, static_cast<uint64_t>(v)); }
void enc_bytes(std::vector<uint8_t>& b, const std::vector<uint8_t>& v) {
    enc_u32(b, static_cast<uint32_t>(v.size()));
    b.insert(b.end(), v.begin(), v.end());
}

uint32_t dec_u32(const uint8_t*& p) {
    uint32_t v = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8)
               | (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
    p += 4; return v;
}
uint64_t dec_u64(const uint8_t*& p) {
    uint64_t v = static_cast<uint64_t>(p[0]) | (static_cast<uint64_t>(p[1]) << 8)
               | (static_cast<uint64_t>(p[2]) << 16) | (static_cast<uint64_t>(p[3]) << 24)
               | (static_cast<uint64_t>(p[4]) << 32) | (static_cast<uint64_t>(p[5]) << 40)
               | (static_cast<uint64_t>(p[6]) << 48) | (static_cast<uint64_t>(p[7]) << 56);
    p += 8; return v;
}
int64_t dec_i64(const uint8_t*& p) { return static_cast<int64_t>(dec_u64(p)); }
uint8_t dec_u8(const uint8_t*& p) { return *p++; }
std::vector<uint8_t> dec_bytes(const uint8_t*& p) {
    uint32_t len = dec_u32(p);
    std::vector<uint8_t> v(p, p + len);
    p += len;
    return v;
}

}  // namespace

// ============================================================================
// Request/Response serialization
// ============================================================================

static std::vector<uint8_t> serialize_ae(const AppendEntriesRequest& req) {
    std::vector<uint8_t> b;
    enc_u64(b, static_cast<uint64_t>(req.term));
    enc_u64(b, static_cast<uint64_t>(req.leader_id));
    enc_i64(b, req.prev_log_index);
    enc_i64(b, req.prev_log_term);
    enc_i64(b, req.leader_commit);
    enc_u32(b, static_cast<uint32_t>(req.entries.size()));
    for (const auto& e : req.entries) {
        enc_i64(b, e.term);
        enc_i64(b, e.index);
        if (std::holds_alternative<NoOpCommand>(e.command)) {
            enc_u8(b, 0); enc_u32(b, 0);
        } else if (auto* cc = std::get_if<ConfigChangeCommand>(&e.command)) {
            enc_u8(b, 1);
            std::vector<uint8_t> sub;
            enc_u8(sub, cc->joint_consensus ? 1 : 0);
            enc_u32(sub, static_cast<uint32_t>(cc->old_members.size()));
            for (auto m : cc->old_members) enc_u64(sub, static_cast<uint64_t>(m));
            enc_u32(sub, static_cast<uint32_t>(cc->new_members.size()));
            for (auto m : cc->new_members) enc_u64(sub, static_cast<uint64_t>(m));
            enc_bytes(b, sub);
        } else if (auto* uc = std::get_if<UserCommand>(&e.command)) {
            enc_u8(b, 2); enc_bytes(b, uc->payload);
        }
    }
    return b;
}

static AppendEntriesRequest deserialize_ae(const uint8_t*& p, size_t len) {
    const uint8_t* end = p + len; (void)end;
    AppendEntriesRequest req;
    req.term           = static_cast<RaftTerm>(dec_i64(p));
    req.leader_id      = static_cast<RaftNodeId>(static_cast<int32_t>(dec_u64(p)));
    req.prev_log_index = static_cast<LogIndex>(dec_i64(p));
    req.prev_log_term  = static_cast<RaftTerm>(dec_i64(p));
    req.leader_commit  = static_cast<LogIndex>(dec_i64(p));
    uint32_t n = dec_u32(p);
    for (uint32_t i = 0; i < n; i++) {
        LogEntry e;
        e.term  = static_cast<RaftTerm>(dec_i64(p));
        e.index = static_cast<LogIndex>(dec_i64(p));
        uint8_t tag = dec_u8(p);
        uint32_t plen = dec_u32(p);
        switch (tag) {
        case 1: {
            ConfigChangeCommand cc;
            const uint8_t* pp = p;
            cc.joint_consensus = dec_u8(pp) != 0;
            uint32_t oc = dec_u32(pp), nc = dec_u32(pp);
            for (uint32_t j = 0; j < oc; j++)
                cc.old_members.push_back(static_cast<RaftNodeId>(static_cast<int32_t>(dec_u64(pp))));
            for (uint32_t j = 0; j < nc; j++)
                cc.new_members.push_back(static_cast<RaftNodeId>(static_cast<int32_t>(dec_u64(pp))));
            e.command = std::move(cc);
            break;
        }
        case 2: { UserCommand uc; uc.payload.assign(p, p+plen); e.command = std::move(uc); break; }
        default: e.command = NoOpCommand{};
        }
        p += plen;
        req.entries.push_back(std::move(e));
    }
    return req;
}

static std::vector<uint8_t> serialize_ae_resp(const AppendEntriesResponse& r) {
    std::vector<uint8_t> b;
    enc_i64(b, r.term); enc_u8(b, r.success ? 1 : 0);
    enc_i64(b, r.last_log_index); enc_i64(b, r.conflict_index);
    return b;
}

static AppendEntriesResponse deserialize_ae_resp(const uint8_t*& p, size_t len) {
    (void)len;
    AppendEntriesResponse r;
    r.term = static_cast<RaftTerm>(dec_i64(p));
    r.success = dec_u8(p) != 0;
    r.last_log_index = static_cast<LogIndex>(dec_i64(p));
    r.conflict_index = static_cast<LogIndex>(dec_i64(p));
    return r;
}

static std::vector<uint8_t> serialize_rv(const RequestVoteRequest& req) {
    std::vector<uint8_t> b;
    enc_i64(b, req.term); enc_u64(b, static_cast<uint64_t>(req.candidate_id));
    enc_i64(b, req.last_log_index); enc_i64(b, req.last_log_term);
    enc_u8(b, req.pre_vote ? 1 : 0);
    return b;
}

static RequestVoteRequest deserialize_rv(const uint8_t*& p, size_t) {
    RequestVoteRequest req;
    req.term = static_cast<RaftTerm>(dec_i64(p));
    req.candidate_id = static_cast<RaftNodeId>(static_cast<int32_t>(dec_u64(p)));
    req.last_log_index = static_cast<LogIndex>(dec_i64(p));
    req.last_log_term = static_cast<RaftTerm>(dec_i64(p));
    req.pre_vote = dec_u8(p) != 0;
    return req;
}

static std::vector<uint8_t> serialize_rv_resp(const RequestVoteResponse& r) {
    std::vector<uint8_t> b; enc_i64(b, r.term); enc_u8(b, r.vote_granted ? 1 : 0); return b;
}

static RequestVoteResponse deserialize_rv_resp(const uint8_t*& p, size_t) {
    RequestVoteResponse r;
    r.term = static_cast<RaftTerm>(dec_i64(p)); r.vote_granted = dec_u8(p) != 0; return r;
}

static std::vector<uint8_t> serialize_is(const InstallSnapshotRequest& req) {
    std::vector<uint8_t> b;
    enc_i64(b, req.term); enc_u64(b, static_cast<uint64_t>(req.leader_id));
    enc_i64(b, req.last_included_index); enc_i64(b, req.last_included_term);
    enc_u64(b, req.offset); enc_u8(b, req.done ? 1 : 0); enc_bytes(b, req.data);
    return b;
}

static InstallSnapshotRequest deserialize_is(const uint8_t*& p, size_t) {
    InstallSnapshotRequest req;
    req.term = static_cast<RaftTerm>(dec_i64(p));
    req.leader_id = static_cast<RaftNodeId>(static_cast<int32_t>(dec_u64(p)));
    req.last_included_index = static_cast<LogIndex>(dec_i64(p));
    req.last_included_term = static_cast<RaftTerm>(dec_i64(p));
    req.offset = dec_u64(p); req.done = dec_u8(p) != 0; req.data = dec_bytes(p);
    return req;
}

static std::vector<uint8_t> serialize_is_resp(const InstallSnapshotResponse& r) {
    std::vector<uint8_t> b; enc_i64(b, r.term); return b;
}

static InstallSnapshotResponse deserialize_is_resp(const uint8_t*& p, size_t) {
    InstallSnapshotResponse r; r.term = static_cast<RaftTerm>(dec_i64(p)); return r;
}

// ============================================================================
// RaftRpcResponse — union of response types
// ============================================================================

using RaftRpcResponse = std::variant<
    AppendEntriesResponse, RequestVoteResponse, InstallSnapshotResponse>;

enum class RpcType : uint8_t {
    AppendEntries = kMsgAppendEntries,
    RequestVote   = kMsgRequestVote,
    InstallSnapshot = kMsgInstallSnapshot,
};

struct PendingRequest {
    uint64_t correlation_id = 0;
    RpcType type;
    std::promise<RaftRpcResponse> promise;
    std::chrono::steady_clock::time_point deadline;
};

// ============================================================================
// RaftRpcConnection — persistent TCP connection to a peer
// ============================================================================

class RaftRpcConnection {
public:
    RaftRpcConnection(std::string peer_addr, RaftNodeId peer_id)
        : peer_addr_(std::move(peer_addr)), peer_id_(peer_id) {}
    ~RaftRpcConnection() { disconnect(); }

    RaftRpcConnection(const RaftRpcConnection&) = delete;
    RaftRpcConnection& operator=(const RaftRpcConnection&) = delete;

    bool connect() {
        std::lock_guard lock(mutex_);
        if (fd_ >= 0) return true;

        sockaddr_in addr{};
        if (!resolve_peer(peer_addr_, addr)) {
            SPDLOG_WARN("raft_rpc: cannot resolve {}", peer_addr_);
            return false;
        }

        fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd_ < 0) return false;

        int yes = 1;
        ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
        ::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &kSocketRcvBuf, sizeof(kSocketRcvBuf));
        ::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &kSocketSndBuf, sizeof(kSocketSndBuf));

        int rc = ::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        if (rc < 0 && errno != EINPROGRESS) { ::close(fd_); fd_ = -1; return false; }

        struct pollfd pfd{fd_, POLLOUT, 0};
        if (::poll(&pfd, 1, 3000) <= 0) { ::close(fd_); fd_ = -1; return false; }

        int err = 0; socklen_t elen = sizeof(err);
        ::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &elen);
        if (err != 0) { ::close(fd_); fd_ = -1; return false; }

        last_active_.store(std::chrono::steady_clock::now());
        return true;
    }

    void disconnect() {
        std::lock_guard lock(mutex_);
        if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
        for (auto& [cid, pr] : pending_) {
            try { pr.promise.set_value(default_response(pr.type)); } catch (...) {}
        }
        pending_.clear();
    }

    bool is_connected() const noexcept {
        std::lock_guard lock(mutex_); return fd_ >= 0;
    }

    auto last_active() const noexcept { return last_active_.load(); }
    int fd() const { return fd_; }

    std::future<RaftRpcResponse> send_append_entries(
        const AppendEntriesRequest& req, std::chrono::milliseconds to) {
        return send_rpc(RpcType::AppendEntries, serialize_ae(req), to);
    }

    std::future<RaftRpcResponse> send_request_vote(
        const RequestVoteRequest& req, std::chrono::milliseconds to) {
        return send_rpc(RpcType::RequestVote, serialize_rv(req), to);
    }

    std::future<RaftRpcResponse> send_install_snapshot(
        const InstallSnapshotRequest& req, std::chrono::milliseconds to) {
        return send_rpc(RpcType::InstallSnapshot, serialize_is(req), to);
    }

    bool handle_response(const std::vector<uint8_t>& frame) {
        if (frame.size() < 8) return false;
        const uint8_t* p = frame.data();
        uint64_t corr_id = dec_u64(p);

        std::lock_guard lock(mutex_);
        auto it = pending_.find(corr_id);
        if (it == pending_.end()) return false;

        RaftRpcResponse resp;
        size_t rem = frame.size() - 8;
        switch (it->second.type) {
        case RpcType::AppendEntries:   resp = deserialize_ae_resp(p, rem); break;
        case RpcType::RequestVote:     resp = deserialize_rv_resp(p, rem); break;
        case RpcType::InstallSnapshot: resp = deserialize_is_resp(p, rem); break;
        }
        try { it->second.promise.set_value(std::move(resp)); } catch (...) {}
        pending_.erase(it);
        last_active_.store(std::chrono::steady_clock::now());
        return true;
    }

    void cancel_pending() {
        std::lock_guard lock(mutex_);
        for (auto& [cid, pr] : pending_) {
            try { pr.promise.set_value(default_response(pr.type)); } catch (...) {}
        }
        pending_.clear();
    }

    bool idle_for(std::chrono::milliseconds d) const {
        return std::chrono::steady_clock::now() - last_active_.load() > d;
    }

private:
    std::future<RaftRpcResponse> send_rpc(RpcType type,
                                           std::vector<uint8_t> payload,
                                           std::chrono::milliseconds timeout) {
        auto corr_id = next_corr_id_++;

        std::vector<uint8_t> frame;
        enc_u64(frame, corr_id);
        frame.insert(frame.end(), payload.begin(), payload.end());

        std::promise<RaftRpcResponse> promise;
        auto future = promise.get_future();

        {
            std::lock_guard lock(mutex_);
            if (fd_ < 0) { promise.set_value(default_response(type)); return future; }

            PendingRequest pr{corr_id, type, std::move(promise),
                              std::chrono::steady_clock::now() + timeout};
            pending_[corr_id] = std::move(pr);

            if (!write_frame(fd_, frame.data(), frame.size())) {
                auto it = pending_.find(corr_id);
                if (it != pending_.end()) {
                    try { it->second.promise.set_value(default_response(type)); } catch (...) {}
                    pending_.erase(it);
                }
            }
        }
        last_active_.store(std::chrono::steady_clock::now());
        return future;
    }

    static RaftRpcResponse default_response(RpcType type) {
        switch (type) {
        case RpcType::AppendEntries:   return AppendEntriesResponse{};
        case RpcType::RequestVote:     return RequestVoteResponse{};
        case RpcType::InstallSnapshot: return InstallSnapshotResponse{};
        }
        return AppendEntriesResponse{};
    }

    std::string peer_addr_;
    RaftNodeId peer_id_;
    mutable std::mutex mutex_;
    int fd_ = -1;
    uint64_t next_corr_id_ = 1;
    std::unordered_map<uint64_t, PendingRequest> pending_;
    std::atomic<std::chrono::steady_clock::time_point> last_active_{std::chrono::steady_clock::now()};
};

// ============================================================================
// RaftRpcClient — high-level API with retry and connection pool
// ============================================================================

class RaftRpcClient {
public:
    RaftRpcClient(int32_t retry_count, std::chrono::milliseconds retry_backoff,
                  std::chrono::milliseconds rpc_timeout)
        : retry_count_(retry_count), retry_backoff_(retry_backoff),
          rpc_timeout_(rpc_timeout) {
        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ < 0)
            throw std::runtime_error("RaftRpcClient: epoll_create1: " + std::string(strerror(errno)));
        reader_thread_ = std::thread(&RaftRpcClient::reader_loop, this);
        health_thread_ = std::thread(&RaftRpcClient::health_loop, this);
    }

    ~RaftRpcClient() { shutdown(); }

    RaftRpcClient(const RaftRpcClient&) = delete;
    RaftRpcClient& operator=(const RaftRpcClient&) = delete;

    void shutdown() {
        bool expected = false;
        if (!running_.compare_exchange_strong(expected, true)) return;
        {
            std::lock_guard lock(conn_mutex_);
            for (auto& [id, conn] : connections_) conn->disconnect();
        }
        if (health_thread_.joinable()) { health_cv_.notify_all(); health_thread_.join(); }
        if (reader_thread_.joinable()) { ::close(epoll_fd_); reader_thread_.join(); }
    }

    AppendEntriesResponse send_append_entries(
        RaftNodeId target, const std::string& addr, const AppendEntriesRequest& req) {
        return send_with_retry<AppendEntriesResponse>(
            target, addr,
            [](RaftRpcConnection& c, const AppendEntriesRequest& r, auto to) {
                return c.send_append_entries(r, to);
            }, req,
            [](const RaftRpcResponse& v) { return std::get<AppendEntriesResponse>(v); });
    }

    RequestVoteResponse send_request_vote(
        RaftNodeId target, const std::string& addr, const RequestVoteRequest& req) {
        return send_with_retry<RequestVoteResponse>(
            target, addr,
            [](RaftRpcConnection& c, const RequestVoteRequest& r, auto to) {
                return c.send_request_vote(r, to);
            }, req,
            [](const RaftRpcResponse& v) { return std::get<RequestVoteResponse>(v); });
    }

    InstallSnapshotResponse send_install_snapshot(
        RaftNodeId target, const std::string& addr, const InstallSnapshotRequest& req) {
        return send_with_retry<InstallSnapshotResponse>(
            target, addr,
            [](RaftRpcConnection& c, const InstallSnapshotRequest& r, auto to) {
                return c.send_install_snapshot(r, to);
            }, req,
            [](const RaftRpcResponse& v) { return std::get<InstallSnapshotResponse>(v); });
    }

    void register_peer(RaftNodeId id, std::string addr) {
        std::lock_guard lock(peer_mutex_);
        peer_addrs_[id] = std::move(addr);
    }

    void unregister_peer(RaftNodeId id) {
        std::lock_guard lock(peer_mutex_); peer_addrs_.erase(id);
    }

private:
    template<typename Req, typename Resp, typename SendFn, typename UnwrapFn>
    Resp send_with_retry(RaftNodeId target, const std::string& addr,
                          SendFn send_fn, const Req& request,
                          UnwrapFn unwrap_fn) {
        int attempt = 0;
        auto backoff = retry_backoff_;

        while (attempt <= retry_count_) {
            auto conn = get_connection(target, addr);
            if (!conn) {
                attempt++; backoff = std::min(backoff * 2, kMaxBackoff);
                std::this_thread::sleep_for(backoff); continue;
            }

            auto future = send_fn(*conn, request, rpc_timeout_);
            auto status = future.wait_for(rpc_timeout_ + std::chrono::milliseconds(100));

            if (status == std::future_status::ready) {
                try { return unwrap_fn(future.get()); }
                catch (const std::exception& e) {
                    SPDLOG_WARN("raft_rpc: response error peer {}: {}", target, e.what());
                }
            } else {
                SPDLOG_WARN("raft_rpc: timeout peer {} attempt {}", target, attempt);
                conn->disconnect();
            }

            attempt++; backoff = std::min(backoff * 2, kMaxBackoff);
            if (attempt <= retry_count_) std::this_thread::sleep_for(backoff);
        }
        SPDLOG_ERROR("raft_rpc: retries exhausted for peer {}", target);
        return Resp{};
    }

    std::shared_ptr<RaftRpcConnection> get_connection(RaftNodeId peer_id,
                                                       const std::string& addr) {
        std::lock_guard lock(conn_mutex_);
        auto it = connections_.find(peer_id);
        if (it != connections_.end() && it->second->is_connected()) return it->second;

        auto conn = std::make_shared<RaftRpcConnection>(addr, peer_id);
        if (!conn->connect()) return nullptr;

        struct epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;
        ev.data.ptr = conn.get();
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, conn->fd(), &ev);

        connections_[peer_id] = conn;
        fd_to_conn_[conn->fd()] = conn;
        return conn;
    }

    void reader_loop() {
        std::array<struct epoll_event, kEpollMaxEvents> events{};
        while (!running_.load(std::memory_order_acquire)) {
            int nfds = ::epoll_wait(epoll_fd_, events.data(), kEpollMaxEvents, 100);
            if (nfds < 0) {
                if (errno == EINTR) continue;
                if (running_.load()) break;
                break;
            }
            for (int i = 0; i < nfds; i++) {
                auto* conn = static_cast<RaftRpcConnection*>(events[i].data.ptr);
                if (!conn) continue;
                if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                    conn->cancel_pending(); cleanup_connection(conn); continue;
                }
                if (events[i].events & EPOLLIN) {
                    while (true) {
                        auto frame = read_frame(conn->fd());
                        if (frame.empty()) {
                            if (errno != EAGAIN && errno != EWOULDBLOCK)
                                { conn->cancel_pending(); cleanup_connection(conn); }
                            break;
                        }
                        conn->handle_response(frame);
                    }
                }
            }
        }
    }

    void health_loop() {
        while (!running_.load(std::memory_order_acquire)) {
            {
                std::unique_lock lock(conn_mutex_);
                health_cv_.wait_for(lock, kHealthCheckInterval, [this] {
                    return running_.load(); });
                if (running_.load()) break;

                auto it = connections_.begin();
                while (it != connections_.end()) {
                    if (!it->second->is_connected()
                        || it->second->idle_for(kConnectionIdleTimeout)) {
                        it->second->disconnect();
                        fd_to_conn_.erase(it->second->fd());
                        it = connections_.erase(it);
                    } else { ++it; }
                }
            }
        }
    }

    void cleanup_connection(RaftRpcConnection* conn) {
        std::lock_guard lock(conn_mutex_);
        int fd = conn->fd(); conn->disconnect(); fd_to_conn_.erase(fd);
        for (auto it = connections_.begin(); it != connections_.end(); ++it) {
            if (it->second.get() == conn) { connections_.erase(it); break; }
        }
    }

    int32_t retry_count_;
    std::chrono::milliseconds retry_backoff_, rpc_timeout_;
    std::mutex peer_mutex_;
    std::unordered_map<RaftNodeId, std::string> peer_addrs_;
    std::mutex conn_mutex_;
    std::unordered_map<RaftNodeId, std::shared_ptr<RaftRpcConnection>> connections_;
    std::unordered_map<int, std::shared_ptr<RaftRpcConnection>> fd_to_conn_;
    int epoll_fd_ = -1;
    std::thread reader_thread_, health_thread_;
    std::condition_variable health_cv_;
    std::atomic<bool> running_{false};
};

// ============================================================================
// RaftRpcServer — accepts connections and dispatches inbound RPCs
// ============================================================================

using AppendEntriesHandler = std::function<AppendEntriesResponse(const AppendEntriesRequest&)>;
using RequestVoteHandler = std::function<RequestVoteResponse(const RequestVoteRequest&)>;
using InstallSnapshotHandler = std::function<InstallSnapshotResponse(const InstallSnapshotRequest&)>;

class RaftRpcServer {
public:
    RaftRpcServer(uint16_t port, AppendEntriesHandler ae_h,
                  RequestVoteHandler rv_h, InstallSnapshotHandler is_h)
        : port_(port), ae_(std::move(ae_h)), rv_(std::move(rv_h)), is_(std::move(is_h)) {}

    ~RaftRpcServer() { stop(); }

    RaftRpcServer(const RaftRpcServer&) = delete;
    RaftRpcServer& operator=(const RaftRpcServer&) = delete;

    bool start() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (listen_fd_ < 0) return false;

        int yes = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port_);

        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0
            || ::listen(listen_fd_, SOMAXCONN) < 0) {
            ::close(listen_fd_); listen_fd_ = -1; return false;
        }

        running_.store(true);
        accept_thread_ = std::thread(&RaftRpcServer::accept_loop, this);
        SPDLOG_INFO("raft_rpc_server: listening on :{}", port_);
        return true;
    }

    void stop() {
        bool expected = true;
        if (!running_.compare_exchange_strong(expected, false)) return;
        if (listen_fd_ >= 0) { ::close(listen_fd_); listen_fd_ = -1; }
        if (accept_thread_.joinable()) accept_thread_.join();
    }

private:
    void accept_loop() {
        while (running_.load()) {
            sockaddr_in ca{}; socklen_t al = sizeof(ca);
            int fd = ::accept4(listen_fd_, reinterpret_cast<sockaddr*>(&ca), &al,
                               SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) { ::poll(nullptr, 0, 10); continue; }
                if (!running_.load()) break;
                continue;
            }
            int yes = 1;
            ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
            std::thread([this, fd]() { handle_connection(fd); ::close(fd); }).detach();
        }
    }

    void handle_connection(int fd) {
        while (running_.load()) {
            auto frame = read_frame(fd);
            if (frame.empty()) break;
            if (frame.size() < 9) continue;

            const uint8_t* p = frame.data();
            uint64_t corr_id = dec_u64(p);
            uint8_t msg_type = dec_u8(p);  // msg_type embedded after corr_id in server protocol

            std::vector<uint8_t> resp;
            // corr_id prefix (8 bytes LE)
            for (int i = 0; i < 8; i++) resp.push_back(static_cast<uint8_t>((corr_id >> (i*8)) & 0xFF));

            std::vector<uint8_t> body;
            switch (msg_type) {
            case kMsgAppendEntries:   body = serialize_ae_resp(ae_(deserialize_ae(p, frame.size()-9))); break;
            case kMsgRequestVote:     body = serialize_rv_resp(rv_(deserialize_rv(p, frame.size()-9))); break;
            case kMsgInstallSnapshot: body = serialize_is_resp(is_(deserialize_is(p, frame.size()-9))); break;
            default: continue;
            }
            resp.insert(resp.end(), body.begin(), body.end());
            if (!write_frame(fd, resp.data(), resp.size())) break;
        }
    }

    uint16_t port_;
    int listen_fd_ = -1;
    AppendEntriesHandler ae_;
    RequestVoteHandler rv_;
    InstallSnapshotHandler is_;
    std::thread accept_thread_;
    std::atomic<bool> running_{false};
};

// ============================================================================
// Factory functions
// ============================================================================

std::unique_ptr<RaftRpcClient> make_raft_rpc_client(
    int32_t retry_count, std::chrono::milliseconds retry_backoff,
    std::chrono::milliseconds rpc_timeout) {
    return std::make_unique<RaftRpcClient>(retry_count, retry_backoff, rpc_timeout);
}

std::unique_ptr<RaftRpcServer> make_raft_rpc_server(
    uint16_t port, AppendEntriesHandler ae_h, RequestVoteHandler rv_h,
    InstallSnapshotHandler is_h) {
    return std::make_unique<RaftRpcServer>(port, std::move(ae_h), std::move(rv_h), std::move(is_h));
}

}  // namespace torrent::raft
