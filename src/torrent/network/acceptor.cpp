/**
 * acceptor.cpp — TCP Acceptor: bind, listen, non-blocking accept loop
 *
 * Implements the server-side socket acceptor for torrent-mq:
 *   - Creates a dual-stack (IPv4/IPv6) socket via socket(AF_INET6, ...)
 *   - Sets SO_REUSEADDR, SO_REUSEPORT (if available), IPV6_V6ONLY=0
 *   - Binds to the requested endpoint, calls listen()
 *   - accept_loop() runs accept4() in a loop with comprehensive error
 *     classification (retryable, fatal, resource-exhausted)
 *   - Each accepted connection is wrapped in a TcpTransport and
 *     dispatched to the caller-provided ConnectionHandler callback
 *   - Graceful stop via stop() and close()
 *
 * Thread safety: accept_loop() is designed to run on a single thread.
 * Multiple threads sharing the Acceptor must serialise externally.
 */

#include "torrent/network/acceptor.h"
#include "torrent/network/transport.h"
#include "torrent/common/types.h"

#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <cstdlib>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>

using namespace std::chrono_literals;

// ============================================================================
// Anonymous namespace — internal helpers and constants
// ============================================================================

namespace {

namespace net = torrent::network;
using torrent::endpoint;

// =========================================================================
// Compile-time feature detection
// =========================================================================

#if defined(SO_REUSEPORT)
    static constexpr bool kHasReusePort = true;
#else
    static constexpr bool kHasReusePort = false;
#endif

#if defined(TCP_DEFER_ACCEPT)
    static constexpr bool kHasTcpDeferAccept = true;
#else
    static constexpr bool kHasTcpDeferAccept = false;
#endif

#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
    static constexpr bool kHasSockNonblockCloexec = true;
#else
    static constexpr bool kHasSockNonblockCloexec = false;
#endif

#if defined(TCP_FASTOPEN)
    static constexpr bool kHasTcpFastOpen = true;
#else
    static constexpr bool kHasTcpFastOpen = false;
#endif

// =========================================================================
// Default constants
// =========================================================================

/// Default listen backlog.
static constexpr int kDefaultBacklog = 4096;

/// Maximum number of consecutive accept errors before the acceptor pauses
/// briefly to avoid a busy loop under ongoing error conditions.
static constexpr int kMaxConsecutiveErrors = 16;

/// Pause duration (microseconds) when hitting max consecutive errors.
static constexpr auto kErrorPauseUs = std::chrono::microseconds(1000);

/// Maximum accept-rate limit per second (0 = unlimited).
static constexpr int kDefaultAcceptRateLimit = 0;

// =========================================================================
// Logging helpers
// =========================================================================

[[nodiscard]] std::shared_ptr<spdlog::logger> get_acceptor_logger() {
    static auto logger = []() {
        auto l = spdlog::get("acceptor");
        if (!l) {
            l = spdlog::stdout_color_mt("acceptor");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define ACC_LOG(level, ...) get_acceptor_logger()->level("[acceptor] " __VA_ARGS__)

// =========================================================================
// Errno classifiers
// =========================================================================

/// True if the accept error is transient and should be retried immediately.
[[nodiscard]] inline bool is_accept_retryable(int ec) noexcept {
    return ec == EAGAIN || ec == EWOULDBLOCK || ec == EINTR
        || ec == ECONNABORTED;
}

/// True if the accept error indicates resource exhaustion.
[[nodiscard]] inline bool is_resource_exhausted(int ec) noexcept {
    return ec == EMFILE || ec == ENFILE || ec == ENOBUFS || ec == ENOMEM;
}

/// Human-readable errno string.
[[nodiscard]] std::string errno_str(int ec) {
    const char* s = std::strerror(ec);
    return s ? std::string(s) : std::string("errno=") + std::to_string(ec);
}

// =========================================================================
// Socket utility functions
// =========================================================================

/// Set O_NONBLOCK on a file descriptor.
[[nodiscard]] int set_nonblocking(int fd) noexcept {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/// Set FD_CLOEXEC so child processes don't inherit this fd.
[[nodiscard]] int set_cloexec(int fd) noexcept {
    int flags = fcntl(fd, F_GETFD, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
}

/// Create a non-blocking, close-on-exec socket for the given domain.
[[nodiscard]] int create_nonblocking_socket(int domain, int type) {
    if constexpr (kHasSockNonblockCloexec) {
        return ::socket(domain, type | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    }
    int fd = ::socket(domain, type, 0);
    if (fd < 0) return -1;
    if (set_nonblocking(fd) < 0 || set_cloexec(fd) < 0) {
        int ec = errno;
        ::close(fd);
        errno = ec;
        return -1;
    }
    return fd;
}

/// Safely close a file descriptor.
void safe_close(int& fd) noexcept {
    if (fd < 0) return;
    int old = fd;
    fd = -1;
    if (::close(old) < 0) {
        ACC_LOG(debug, "close({}) failed: {}", old, errno_str(errno));
    }
}

/// Resolve an endpoint to a sockaddr_storage.
/// Returns std::nullopt on failure.
[[nodiscard]] std::optional<sockaddr_storage>
resolve_endpoint(const endpoint& ep, int socktype = SOCK_STREAM) {
    sockaddr_storage sa{};
    std::memset(&sa, 0, sizeof(sa));

    auto port_str = std::to_string(ep.port);

    // Fast path: numeric
    {
        struct addrinfo hints{};
        hints.ai_family   = AF_UNSPEC;
        hints.ai_socktype = socktype;
        hints.ai_flags    = AI_NUMERICHOST | AI_NUMERICSERV;
        struct addrinfo* res = nullptr;
        if (getaddrinfo(ep.host.c_str(), port_str.c_str(), &hints, &res) == 0
            && res != nullptr) {
            std::memcpy(&sa, res->ai_addr, res->ai_addrlen);
            freeaddrinfo(res);
            return sa;
        }
        if (res) freeaddrinfo(res);
    }

    // Slow path: DNS
    {
        struct addrinfo hints{};
        hints.ai_family   = AF_UNSPEC;
        hints.ai_socktype = socktype;
        hints.ai_flags    = AI_NUMERICSERV;
        struct addrinfo* res = nullptr;
        int gai = getaddrinfo(ep.host.c_str(), port_str.c_str(), &hints, &res);
        if (gai != 0 || res == nullptr) {
            if (res) freeaddrinfo(res);
            ACC_LOG(error, "getaddrinfo({}:{}) failed: {}",
                     ep.host, ep.port, gai_strerror(gai));
            return std::nullopt;
        }
        std::memcpy(&sa, res->ai_addr, res->ai_addrlen);
        freeaddrinfo(res);
        return sa;
    }
}

/// Build an endpoint from a sockaddr_storage.
[[nodiscard]] endpoint endpoint_from_sa(const sockaddr_storage& sa) {
    endpoint ep;
    char host[INET6_ADDRSTRLEN] = {};

    if (sa.ss_family == AF_INET) {
        const auto* sin = reinterpret_cast<const sockaddr_in*>(&sa);
        inet_ntop(AF_INET, &sin->sin_addr, host, sizeof(host));
        ep.port = ntohs(sin->sin_port);
    } else if (sa.ss_family == AF_INET6) {
        const auto* sin6 = reinterpret_cast<const sockaddr_in6*>(&sa);
        inet_ntop(AF_INET6, &sin6->sin6_addr, host, sizeof(host));
        ep.port = ntohs(sin6->sin6_port);
    } else {
        host[0] = '?';
        host[1] = '\0';
    }
    ep.host = host;
    return ep;
}

/// Return the actual sockaddr length.
[[nodiscard]] socklen_t sa_len(const sockaddr_storage& sa) {
    if (sa.ss_family == AF_INET6) return sizeof(sockaddr_in6);
    return sizeof(sockaddr_in);
}

/// Set socket options common to accept sockets.
bool set_accept_socket_options(int fd, int backlog) {
    // SO_REUSEADDR — allow rapid restart without TIME_WAIT blocking the port
    int optval = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
        ACC_LOG(warn, "SO_REUSEADDR failed: {}", errno_str(errno));
    }

    // SO_REUSEPORT — allow multiple acceptors on the same port (if available)
    if constexpr (kHasReusePort) {
        if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval)) < 0) {
            ACC_LOG(warn, "SO_REUSEPORT failed: {}", errno_str(errno));
        }
    }

    // SO_KEEPALIVE — detect dead clients
    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval)) < 0) {
        ACC_LOG(warn, "SO_KEEPALIVE failed: {}", errno_str(errno));
    }

    // TCP_NODELAY — disable Nagle on accepted connections
    // (applied to the listen socket for inheritance; also set per-connection)

    // TCP_DEFER_ACCEPT — wait for data before completing accept (reduce wakeups)
    if constexpr (kHasTcpDeferAccept) {
        int defer_sec = 1;
        if (setsockopt(fd, IPPROTO_TCP, TCP_DEFER_ACCEPT,
                       &defer_sec, sizeof(defer_sec)) < 0) {
            ACC_LOG(debug, "TCP_DEFER_ACCEPT failed: {}", errno_str(errno));
        }
    }

    // TCP_FASTOPEN — accept TFO cookies from clients
    if constexpr (kHasTcpFastOpen) {
        int qlen = std::min(backlog, 256);
        if (setsockopt(fd, IPPROTO_TCP, TCP_FASTOPEN, &qlen, sizeof(qlen)) < 0) {
            ACC_LOG(debug, "TCP_FASTOPEN failed: {}", errno_str(errno));
        }
    }

    (void)backlog;
    return true;
}

} // anonymous namespace

// ============================================================================
// Acceptor implementation
// ============================================================================

namespace torrent::network {

// ---------------------------------------------------------------------------
// Construction — create socket, bind, listen
// ---------------------------------------------------------------------------

Acceptor::Acceptor(const endpoint& bind_addr, uint16_t port,
                   int backlog, bool non_blocking)
    : fd_(-1)
    , port_(port > 0 ? port : kDefaultPort)
    , backlog_(backlog > 0 ? backlog : kDefaultBacklog)
    , stopped_(false)
    , total_accepts_(0)
    , total_errors_(0)
    , consecutive_errors_(0)
{
    ACC_LOG(info, "Creating acceptor for {}:{} (backlog={}, non_blocking={})",
            bind_addr.host.empty() ? "*" : bind_addr.host,
            port_, backlog_, non_blocking);

    // Validate address: use INADDR_ANY / in6addr_any if host is empty
    endpoint effective_addr = bind_addr;
    if (effective_addr.host.empty()) {
        effective_addr.host = "::";  // IPv6 any (dual-stack)
    }

    // Resolve the bind address
    auto sa_opt = resolve_endpoint(effective_addr);
    if (!sa_opt.has_value()) {
        std::string msg = "Failed to resolve bind address: " +
                          effective_addr.to_string();
        ACC_LOG(error, "{}", msg);
        throw std::runtime_error(msg);
    }

    sockaddr_storage bind_sa = sa_opt.value();

    // Create the socket — use IPv6 for dual-stack compatibility
    int domain = bind_sa.ss_family == AF_INET ? AF_INET : AF_INET6;
    fd_ = create_nonblocking_socket(domain, SOCK_STREAM);
    if (fd_ < 0) {
        std::string msg = "socket() failed: " + errno_str(errno);
        ACC_LOG(error, "{}", msg);
        throw std::system_error(errno, std::generic_category(), msg);
    }
    ACC_LOG(debug, "Created socket fd={}", fd_);

    // Enable dual-stack: accept IPv4 clients via IPv4-mapped IPv6 addresses
    if (domain == AF_INET6) {
        int v6only = 0;
        if (setsockopt(fd_, IPPROTO_IPV6, IPV6_V6ONLY,
                       &v6only, sizeof(v6only)) < 0) {
            ACC_LOG(debug, "IPV6_V6ONLY=0 failed: {}", errno_str(errno));
        }
    }

    // Set common accept socket options
    set_accept_socket_options(fd_, backlog_);

    // Set non-blocking if requested (already non-blocking by default from
    // create_nonblocking_socket; this is a no-op in the common case)
    if (non_blocking) {
        if (set_nonblocking(fd_) < 0) {
            ACC_LOG(warn, "set_nonblocking failed (already set?): {}",
                    errno_str(errno));
        }
    }

    // Set CLOEXEC if not already set
    if constexpr (!kHasSockNonblockCloexec) {
        set_cloexec(fd_);
    }

    // Bind
    // The port in the sockaddr may differ from the requested port;
    // ensure we use the correct one.
    if (bind_sa.ss_family == AF_INET) {
        auto* sa4 = reinterpret_cast<sockaddr_in*>(&bind_sa);
        sa4->sin_port = htons(port_);
    } else {
        auto* sa6 = reinterpret_cast<sockaddr_in6*>(&bind_sa);
        sa6->sin6_port = htons(port_);
    }

    if (::bind(fd_, reinterpret_cast<const sockaddr*>(&bind_sa),
               sa_len(bind_sa)) < 0) {
        std::string msg = "bind(" + effective_addr.host + ":" +
                          std::to_string(port_) + ") failed: " +
                          errno_str(errno);
        safe_close(fd_);
        ACC_LOG(error, "{}", msg);
        throw std::system_error(errno, std::generic_category(), msg);
    }
    ACC_LOG(info, "Bound to {}:{}", effective_addr.host, port_);

    // Listen
    if (::listen(fd_, backlog_) < 0) {
        std::string msg = "listen(backlog=" + std::to_string(backlog_) +
                          ") failed: " + errno_str(errno);
        safe_close(fd_);
        ACC_LOG(error, "{}", msg);
        throw std::system_error(errno, std::generic_category(), msg);
    }

    // Retrieve the actual bound address (port may differ if port_ was 0)
    sockaddr_storage actual_sa{};
    socklen_t actual_len = sizeof(actual_sa);
    if (getsockname(fd_, reinterpret_cast<sockaddr*>(&actual_sa),
                    &actual_len) == 0) {
        bound_ = endpoint_from_sa(actual_sa);
    } else {
        bound_ = effective_addr;
        bound_.port = port_;
    }

    ACC_LOG(info, "Listening on {}:{} (fd={}, backlog={})",
            bound_.host, bound_.port, fd_, backlog_);
}

// ---------------------------------------------------------------------------
// Destruction
// ---------------------------------------------------------------------------

Acceptor::~Acceptor() {
    stop();
    close();
}

// ---------------------------------------------------------------------------
// accept_loop — main accept loop with error classification
// ---------------------------------------------------------------------------

void Acceptor::accept_loop(ConnectionHandler handler) {
    if (fd_ < 0) {
        ACC_LOG(error, "accept_loop called on closed Acceptor");
        return;
    }

    if (!handler) {
        ACC_LOG(error, "accept_loop called with null handler");
        return;
    }

    ACC_LOG(info, "Starting accept loop on fd={} ({}:{})",
            fd_, bound_.host, bound_.port);

    stopped_.store(false, std::memory_order_release);
    consecutive_errors_.store(0, std::memory_order_relaxed);

    while (!stopped_.load(std::memory_order_acquire)) {
        // Allocate a sockaddr_storage large enough for IPv6
        sockaddr_storage peer_sa{};
        socklen_t peer_len = sizeof(peer_sa);
        std::memset(&peer_sa, 0, sizeof(peer_sa));

        // Blocking accept4 — the fd is non-blocking, so if no connections
        // are pending this returns immediately with EAGAIN/EWOULDBLOCK.
        int client_fd = ::accept4(fd_,
                                   reinterpret_cast<sockaddr*>(&peer_sa),
                                   &peer_len,
                                   SOCK_NONBLOCK | SOCK_CLOEXEC);

        if (client_fd >= 0) {
            // ---- Successful accept ----
            consecutive_errors_.store(0, std::memory_order_relaxed);
            total_accepts_.fetch_add(1, std::memory_order_relaxed);

            endpoint peer_ep = endpoint_from_sa(peer_sa);
            endpoint local_ep = bound_;

            ACC_LOG(debug, "Accepted connection from {}:{} (fd={}, total={})",
                    peer_ep.host, peer_ep.port, client_fd,
                    total_accepts_.load(std::memory_order_relaxed));

            // Apply per-connection socket options
            int optval = 1;
            setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY,
                       &optval, sizeof(optval));

            // Create TcpTransport and dispatch
            try {
                auto transport = std::make_unique<TcpTransport>(
                    client_fd, peer_ep, local_ep);
                handler(std::move(transport));
            } catch (const std::exception& ex) {
                ACC_LOG(error, "Failed to create TcpTransport for fd={}: {}",
                        client_fd, ex.what());
                safe_close(client_fd);
                total_errors_.fetch_add(1, std::memory_order_relaxed);
            }

        } else {
            // ---- Accept error ----
            int ec = errno;

            if (is_accept_retryable(ec)) {
                // Transient — no connections pending, sleep briefly to avoid
                // busy-looping in a non-blocking epoll-free accept loop.
                // In production this would be driven by epoll/kqueue.
                std::this_thread::sleep_for(1ms);
                continue;
            }

            if (is_resource_exhausted(ec)) {
                // File descriptors or memory exhausted — back off briefly
                total_errors_.fetch_add(1, std::memory_order_relaxed);
                uint32_t consec = consecutive_errors_.fetch_add(
                    1, std::memory_order_relaxed) + 1;

                ACC_LOG(warn, "Accept resource exhaustion (fd={}, errno={}, "
                        "consecutive_errors={}): {}",
                        fd_, ec, consec, errno_str(ec));

                // Exponential backoff but cap it
                auto sleep_us = kErrorPauseUs * (1 << std::min(consec, 4u));
                std::this_thread::sleep_for(sleep_us);

                if (consec >= kMaxConsecutiveErrors) {
                    ACC_LOG(error, "{} consecutive accept errors; "
                            "pausing accept loop", consec);
                    std::this_thread::sleep_for(10ms);
                    consecutive_errors_.store(0, std::memory_order_relaxed);
                }
                continue;
            }

            // Fatal error — stop the accept loop
            total_errors_.fetch_add(1, std::memory_order_relaxed);
            ACC_LOG(error, "Fatal accept error on fd={}, errno={}: {}. "
                    "Stopping accept loop.",
                    fd_, ec, errno_str(ec));
            break;
        }
    }

    ACC_LOG(info, "Accept loop ended on fd={} ({}:{}) — total_accepts={}, "
            "total_errors={}",
            fd_, bound_.host, bound_.port,
            total_accepts_.load(std::memory_order_relaxed),
            total_errors_.load(std::memory_order_relaxed));
}

// ---------------------------------------------------------------------------
// stop — signal the accept loop to exit
// ---------------------------------------------------------------------------

void Acceptor::stop() {
    bool was_stopped = stopped_.exchange(true, std::memory_order_acq_rel);
    if (!was_stopped) {
        ACC_LOG(info, "Acceptor stop requested on fd={} ({}:{})",
                fd_, bound_.host, bound_.port);

        // Shutdown the listening socket to unblock any pending accept4() call.
        // This is safe: shutdown(fd, SHUT_RD) causes accept4 to return
        // immediately (typically with EINVAL, which is handled by the loop).
        if (fd_ >= 0) {
            ::shutdown(fd_, SHUT_RD);
        }
    }
}

// ---------------------------------------------------------------------------
// close — release the listen socket
// ---------------------------------------------------------------------------

void Acceptor::close() {
    if (fd_ >= 0) {
        ACC_LOG(info, "Closing acceptor fd={} ({}:{}) — total_accepts={}",
                fd_, bound_.host, bound_.port,
                total_accepts_.load(std::memory_order_relaxed));
        safe_close(fd_);
    }
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

int Acceptor::fd() const noexcept {
    return fd_;
}

endpoint Acceptor::bound_address() const {
    return bound_;
}

} // namespace torrent::network
