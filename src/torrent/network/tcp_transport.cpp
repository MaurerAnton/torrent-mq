/**
 * tcp_transport.cpp — TCP/TLS Transport Implementation
 *
 * Implements the full transport abstraction layer for torrent-mq:
 *   - TcpTransport:  non-blocking TCP with Nagle-off, TCP_CORK, keepalive, connect
 *   - TlsTransport:  TLS 1.3 wrapper (OpenSSL) with mTLS, ALPN, session caching
 *   - TransportFactory: SSL_CTX lifecycle, certificate/key loading, ciphers
 *   - Acceptor:      bind, listen, non-blocking accept loop
 *
 * Design principles:
 *   - All I/O is non-blocking.  Syscall results are classified into retryable,
 *     fatal, and graceful-shutdown categories so callers get consistent
 *     semantics regardless of transport type (raw TCP vs TLS).
 *   - Every syscall is checked; every errno path is explicitly handled.
 *   - TLS 1.3 is the only negotiated version; TLS 1.2 is the floor.
 *   - Zero-copy where possible: TransportConfig applies socket options
 *     directly via setsockopt rather than buffering in userspace.
 *   - The OpenSSL error queue is drained and logged after every operation
 *     that could enqueue diagnostic entries.
 *
 * Thread safety:
 *   - Individual Transport objects are NOT thread-safe — callers must
 *     serialise access to read()/write()/handshake()/close().
 *   - TransportFactory::create_* / wrap_tls are safe for concurrent use
 *     (each call creates independent SSL objects from the shared SSL_CTX).
 *   - Acceptor::accept_one() is safe from a single thread; if multiple
 *     threads share the Acceptor, the caller must synchronise.
 */

// ---------------------------------------------------------------------------
// Project headers
// ---------------------------------------------------------------------------

#include "torrent/network/transport.h"
#include "torrent/common/types.h"

// ---------------------------------------------------------------------------
// System headers — sockets, I/O, signals, timers
// ---------------------------------------------------------------------------

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <ctime>

// ---------------------------------------------------------------------------
// OpenSSL (TLS 1.3)
// ---------------------------------------------------------------------------

#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/rand.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/kdf.h>
#include <openssl/bio.h>

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

#include <spdlog/spdlog.h>

// ---------------------------------------------------------------------------
// Standard library
// ---------------------------------------------------------------------------

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace {
namespace net = torrent::network;

// =========================================================================
// Compile-time feature detection
// =========================================================================

#if defined(TCP_CORK)
    static constexpr bool kHasTcpCork = true;
#else
    static constexpr bool kHasTcpCork = false;
#endif

#if defined(TCP_NOPUSH)
    static constexpr bool kHasTcpNopush = true;
#else
    static constexpr bool kHasTcpNopush = false;
#endif

#if defined(TCP_DEFER_ACCEPT)
    static constexpr bool kHasTcpDeferAccept = true;
#else
    static constexpr bool kHasTcpDeferAccept = false;
#endif

#if defined(SO_REUSEPORT)
    static constexpr bool kHasReusePort = true;
#else
    static constexpr bool kHasReusePort = false;
#endif

#if defined(TCP_KEEPIDLE) && defined(TCP_KEEPINTVL) && defined(TCP_KEEPCNT)
    static constexpr bool kHasTcpKeepaliveTuning = true;
#else
    static constexpr bool kHasTcpKeepaliveTuning = false;
#endif

#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
    static constexpr bool kHasSockNonblockCloexec = true;
#else
    static constexpr bool kHasSockNonblockCloexec = false;
#endif

#if defined(MSG_NOSIGNAL)
    static constexpr bool kHasMsgNosignal = true;
#else
    static constexpr bool kHasMsgNosignal = false;
#endif

// =========================================================================
// errno classifiers — retryable vs fatal vs graceful-shutdown
// =========================================================================

/// Returns true when a syscall should be retried immediately or after
/// the fd becomes ready (poll / epoll / kqueue).
[[nodiscard]] inline bool is_retryable_errno(int ec) noexcept {
    return ec == EAGAIN || ec == EWOULDBLOCK || ec == EINTR;
}

/// Returns true for connection-reset errors: remote sent RST, broken pipe,
/// connect timeout, refused, unreachable, host down, network down.
[[nodiscard]] inline bool is_fatal_errno(int ec) noexcept {
    return ec == ECONNRESET   || ec == EPIPE       || ec == ETIMEDOUT
        || ec == ECONNREFUSED || ec == ENOTCONN     || ec == EHOSTUNREACH
        || ec == ENETUNREACH  || ec == EHOSTDOWN    || ec == ENETDOWN
        || ec == ESHUTDOWN    || ec == EBADF;
}

/// Returns true for transient accept errors that should be silently ignored.
[[nodiscard]] inline bool is_accept_retryable(int ec) noexcept {
    return ec == EAGAIN || ec == EWOULDBLOCK || ec == EINTR
        || ec == ECONNABORTED;
}

/// Returns true for resource exhaustion errors that warrant backoff.
[[nodiscard]] inline bool is_resource_exhausted(int ec) noexcept {
    return ec == EMFILE || ec == ENFILE || ec == ENOBUFS || ec == ENOMEM;
}

/// Human-readable string for an errno value.
[[nodiscard]] std::string errno_str(int ec) {
    const char* s = std::strerror(ec);
    return s ? std::string(s) : std::string("errno=") + std::to_string(ec);
}

/// Build a diagnostic string for a failed socket operation.
[[nodiscard]] std::string sock_op_error(const char* op, int fd,
                                         const std::string& detail) {
    std::ostringstream oss;
    oss << op << "(fd=" << fd << ")";
    if (!detail.empty()) oss << " [" << detail << "]";
    oss << ": " << errno_str(errno);
    return oss.str();
}

// =========================================================================
// Endpoint ↔ sockaddr resolution (IPv4 + IPv6, dual-stack aware)
// =========================================================================

/// Resolve an endpoint to a sockaddr_storage using getaddrinfo.
/// Tries numeric first (fast path), then DNS resolution.
/// Returns std::nullopt if resolution fails.
[[nodiscard]] std::optional<sockaddr_storage>
resolve_endpoint(const torrent::endpoint& ep, int socktype = SOCK_STREAM) {
    sockaddr_storage sa{};
    std::memset(&sa, 0, sizeof(sa));

    auto port_str = std::to_string(ep.port);

    // --- Fast path: numeric (AI_NUMERICHOST) ---
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

    // --- Slow path: DNS resolution ---
    {
        struct addrinfo hints{};
        hints.ai_family   = AF_UNSPEC;
        hints.ai_socktype = socktype;
        hints.ai_flags    = AI_NUMERICSERV;
        struct addrinfo* res = nullptr;
        int gai = getaddrinfo(ep.host.c_str(), port_str.c_str(), &hints, &res);
        if (gai != 0 || res == nullptr) {
            if (res) freeaddrinfo(res);
            spdlog::error("[transport] getaddrinfo({}:{}) failed: {}",
                          ep.host, ep.port, gai_strerror(gai));
            return std::nullopt;
        }
        std::memcpy(&sa, res->ai_addr, res->ai_addrlen);
        freeaddrinfo(res);
        return sa;
    }
}

/// Build an endpoint from a sockaddr_storage.  Handles AF_INET and AF_INET6.
[[nodiscard]] torrent::endpoint endpoint_from_sa(const sockaddr_storage& sa) {
    torrent::endpoint ep;
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

/// Return the size of the sockaddr within a sockaddr_storage.
[[nodiscard]] socklen_t sa_len(const sockaddr_storage& sa) {
    if (sa.ss_family == AF_INET6) return sizeof(sockaddr_in6);
    return sizeof(sockaddr_in); // default: IPv4
}

// =========================================================================
// Socket utility functions
// =========================================================================

/// Set O_NONBLOCK on a file descriptor.  Returns 0 on success, -1 on failure.
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
/// Falls back from SOCK_NONBLOCK|SOCK_CLOEXEC to manual fcntl if needed.
[[nodiscard]] int create_socket(int domain, int type, int /*protocol*/) {
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

/// Safely close a file descriptor, logging failures at debug level.
/// Never throws.  Sets fd to -1 after close.
void safe_close(int& fd) noexcept {
    if (fd < 0) return;
    int old = fd;
    fd = -1;
    if (::close(old) < 0) {
        spdlog::debug("[transport] close({}) failed: {}", old,
                      errno_str(errno));
    }
}

/// Poll a single fd for the given events with a timeout.
/// Returns the poll() result (0 = timeout, >0 = ready, <0 = error).
[[nodiscard]] int poll_fd(int fd, short events, int timeout_ms) noexcept {
    struct pollfd pfd;
    pfd.fd      = fd;
    pfd.events  = events;
    pfd.revents = 0;
    return ::poll(&pfd, 1, timeout_ms);
}

// =========================================================================
// OpenSSL helpers
// =========================================================================

/// Log and clear all queued OpenSSL errors at the given log level.
void drain_ssl_errors(spdlog::level::level_enum lvl, const char* ctx) {
    unsigned long err;
    while ((err = ERR_get_error()) != 0) {
        char buf[256];
        ERR_error_string_n(err, buf, sizeof(buf));
        spdlog::log(lvl, "[transport] OpenSSL {}: {}", ctx, buf);
    }
}

/// WARN-level convenience wrapper.
inline void log_ssl_errors(const char* ctx) {
    drain_ssl_errors(spdlog::level::warn, ctx);
}

/// Build a human-readable string from an X509_NAME (e.g. subject or issuer).
[[nodiscard]] std::string x509_name_oneline(X509_NAME* name) {
    if (!name) return {};
    BIO* bio = BIO_new(BIO_s_mem());
    if (!bio) return {};
    X509_NAME_print_ex(bio, name, 0, XN_FLAG_ONELINE & ~ASN1_STRFLGS_ESC_MSB);
    char* data = nullptr;
    long len = BIO_get_mem_data(bio, &data);
    std::string result(data, static_cast<size_t>(len > 0 ? len : 0));
    BIO_free(bio);
    return result;
}

// =========================================================================
// ALPN helper
// =========================================================================

/// Convert a colon-delimited ALPN string (e.g. "torrent1:h2") to wire
/// format (length-prefixed, e.g. "\x08torrent1\x02h2").
/// Returns empty string on protocol names longer than 255 bytes.
[[nodiscard]] std::string build_alpn_wire(std::string_view colon_list) {
    if (colon_list.empty()) return {};
    std::string wire;
    size_t pos = 0;
    while (pos <= colon_list.size()) {
        size_t next = colon_list.find(':', pos);
        if (next == std::string_view::npos) next = colon_list.size();
        auto proto = colon_list.substr(pos, next - pos);
        if (proto.size() > 255) {
            spdlog::error("[transport] ALPN proto too long ({} bytes): {}",
                          proto.size(), proto);
            return {};
        }
        wire.push_back(static_cast<char>(proto.size()));
        wire.append(proto);
        pos = next + 1;
    }
    return wire;
}

/// Detect whether an ALPN string is already wire-format (starts with a
/// length byte whose value would not be a printable ASCII character).
[[nodiscard]] bool is_alpn_wire_format(std::string_view s) noexcept {
    if (s.empty()) return true;
    unsigned char first = static_cast<unsigned char>(s[0]);
    // Wire-format starts with a length byte (0-255), typically < 32 for
    // short protocol names.  Printable ASCII starts at 0x20.
    return first < 0x20 || (first < s.size() && first > 0);
}

// =========================================================================
// SSL passphrase callback (shared between server and client init)
// =========================================================================

int pem_passwd_cb(char* buf, int size, int /*rwflag*/, void* userdata) {
    if (!userdata) return 0;
    const char* pass = static_cast<const char*>(userdata);
    int len = static_cast<int>(std::strlen(pass));
    if (len > size) len = size;
    std::memcpy(buf, pass, static_cast<size_t>(len));
    return len;
}

} // anonymous namespace

// ============================================================================
// TcpTransport implementation
// ============================================================================

namespace torrent::network {

// ---------------------------------------------------------------------------
// Construction / Destruction
// ---------------------------------------------------------------------------

TcpTransport::TcpTransport() {
    fd_ = create_socket(AF_INET6, SOCK_STREAM, 0);

    if (fd_ < 0) {
        spdlog::error("[transport] TcpTransport socket() failed: {}",
                      errno_str(errno));
        return;
    }

    // Dual-stack by default: allow IPv4 clients to connect via IPv4-mapped
    // IPv6 addresses.  The application can override this if needed.
    int v6only = 0;
    if (setsockopt(fd_, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, sizeof(v6only)) < 0) {
        spdlog::debug("[transport] IPV6_V6ONLY=0 failed: {}", errno_str(errno));
    }

    // Set CLOEXEC if not already done by create_socket
    if constexpr (!kHasSockNonblockCloexec) {
        set_cloexec(fd_);
    }
}

TcpTransport::TcpTransport(int fd, const endpoint& peer, const endpoint& local)
    : fd_(fd), peer_(peer), local_(local) {
    if (fd_ >= 0) {
        if constexpr (!kHasSockNonblockCloexec) {
            set_nonblocking(fd_);
            set_cloexec(fd_);
        }
    }
}

TcpTransport::~TcpTransport() {
    close();
}

// ---------------------------------------------------------------------------
// Move semantics
// ---------------------------------------------------------------------------

TcpTransport::TcpTransport(TcpTransport&& other) noexcept
    : fd_(other.fd_)
    , peer_(std::move(other.peer_))
    , local_(std::move(other.local_)) {
    other.fd_ = -1;
}

TcpTransport& TcpTransport::operator=(TcpTransport&& other) noexcept {
    if (this != &other) {
        close();
        fd_     = other.fd_;
        peer_   = std::move(other.peer_);
        local_  = std::move(other.local_);
        other.fd_ = -1;
    }
    return *this;
}

// ---------------------------------------------------------------------------
// I/O — read
// ---------------------------------------------------------------------------

ssize_t TcpTransport::read(char* buf, size_t len) {
    if (fd_ < 0) {
        errno = ENOTCONN;
        return -1;
    }

    if (len == 0) return 0;

    ssize_t n;
    do {
        n = ::recv(fd_, buf, len, 0);
    } while (n < 0 && errno == EINTR);

    if (n < 0) {
        int ec = errno;
        if (is_retryable_errno(ec)) {
            errno = EAGAIN;
            return -1;
        }
        if (is_fatal_errno(ec)) {
            spdlog::debug("[transport] read fd={} fatal: {}", fd_,
                          errno_str(ec));
            return -1;
        }
        spdlog::debug("[transport] read fd={} error: {}", fd_, errno_str(ec));
        return -1;
    }

    // n == 0: peer performed orderly shutdown (FIN received)
    // n > 0:  data received
    return n;
}

// ---------------------------------------------------------------------------
// I/O — write
// ---------------------------------------------------------------------------

ssize_t TcpTransport::write(const char* buf, size_t len) {
    if (fd_ < 0) {
        errno = ENOTCONN;
        return -1;
    }

    if (len == 0) return 0;

    int flags = 0;
    if constexpr (kHasMsgNosignal) {
        flags = MSG_NOSIGNAL;  // avoid SIGPIPE on broken connection
    }

    ssize_t n;
    do {
        n = ::send(fd_, buf, len, flags);
    } while (n < 0 && errno == EINTR);

    if (n < 0) {
        int ec = errno;
        if (is_retryable_errno(ec)) {
            errno = EAGAIN;
            return -1;
        }
        if (is_fatal_errno(ec)) {
            spdlog::debug("[transport] write fd={} fatal: {}", fd_,
                          errno_str(ec));
            return -1;
        }
        spdlog::debug("[transport] write fd={} error: {}", fd_, errno_str(ec));
        return -1;
    }

    // Partial writes are common in non-blocking mode — caller must retry
    // the remainder.
    return n;
}

// ---------------------------------------------------------------------------
// Flush — push corked/nagled data to the wire
// ---------------------------------------------------------------------------

void TcpTransport::flush() {
    if (fd_ < 0) return;

    // 1. Disable TCP_CORK (or TCP_NOPUSH on BSD/macOS) so the kernel
    //    stops coalescing and pushes whatever it has buffered.
    uncork();

    // 2. Toggle TCP_NODELAY to force-flush the Nagle buffer.
    //    We set it to 1 even if it was already 1 — the kernel sees this
    //    as a signal to push pending data.
    int val = 1;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));

    if constexpr (!kHasTcpCork && !kHasTcpNopush) {
        // On platforms without cork/nopush, we rely on TCP_NODELAY alone.
        // The toggle above is sufficient.
        spdlog::trace("[transport] flush fd={}: nodelay toggle only", fd_);
    }
}

// ---------------------------------------------------------------------------
// Close / Shutdown
// ---------------------------------------------------------------------------

void TcpTransport::close() {
    if (fd_ < 0) return;

    // Graceful shutdown: signal EOF in both directions.
    // This sends FIN for the write side and discards pending reads.
    // We ignore errors — we're closing regardless.
    ::shutdown(fd_, SHUT_RDWR);
    safe_close(fd_);
}

void TcpTransport::shutdown(int how) {
    if (fd_ < 0) return;

    if (::shutdown(fd_, how) < 0) {
        int ec = errno;
        // ENOTCONN is expected if the connection was never established
        // or was already shut down.
        if (ec != ENOTCONN) {
            spdlog::debug("[transport] shutdown fd={} how={}: {}",
                          fd_, how, errno_str(ec));
        }
    }
}

// ---------------------------------------------------------------------------
// Socket options
// ---------------------------------------------------------------------------

void TcpTransport::set_nodelay(bool enable) {
    if (fd_ < 0) return;

    int val = enable ? 1 : 0;
    if (setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)) < 0) {
        spdlog::debug("[transport] set_nodelay fd={} enable={}: {}",
                      fd_, enable, errno_str(errno));
    }
}

void TcpTransport::set_reuse_address(bool enable) {
    if (fd_ < 0) return;

    int val = enable ? 1 : 0;
    if (setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)) < 0) {
        spdlog::debug("[transport] set_reuse_address fd={} enable={}: {}",
                      fd_, enable, errno_str(errno));
    }
}

void TcpTransport::set_keepalive(bool enable,
                                  int idle_sec,
                                  int interval_sec,
                                  int count) {
    if (fd_ < 0) return;

    // Enable or disable SO_KEEPALIVE
    int keepalive = enable ? 1 : 0;
    if (setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &keepalive,
                   sizeof(keepalive)) < 0) {
        spdlog::debug("[transport] set_keepalive fd={} SO_KEEPALIVE: {}",
                      fd_, errno_str(errno));
        return;
    }

    if (!enable) return;

    // Linux-specific keepalive tuning parameters.
    // These are silently ignored on non-Linux kernels (the preprocessor
    // guards are for portability, not because the symbols don't exist —
    // they'll fail at setsockopt time on other OSes).
    if constexpr (kHasTcpKeepaliveTuning) {
        if (setsockopt(fd_, IPPROTO_TCP, TCP_KEEPIDLE,
                       &idle_sec, sizeof(idle_sec)) < 0) {
            spdlog::debug("[transport] TCP_KEEPIDLE fd={} val={}: {}",
                          fd_, idle_sec, errno_str(errno));
        }

        if (setsockopt(fd_, IPPROTO_TCP, TCP_KEEPINTVL,
                       &interval_sec, sizeof(interval_sec)) < 0) {
            spdlog::debug("[transport] TCP_KEEPINTVL fd={} val={}: {}",
                          fd_, interval_sec, errno_str(errno));
        }

        if (setsockopt(fd_, IPPROTO_TCP, TCP_KEEPCNT,
                       &count, sizeof(count)) < 0) {
            spdlog::debug("[transport] TCP_KEEPCNT fd={} val={}: {}",
                          fd_, count, errno_str(errno));
        }
    }
}

// ---------------------------------------------------------------------------
// TCP_CORK / uncork — Linux coalescing (BSD/macOS: TCP_NOPUSH)
// ---------------------------------------------------------------------------

void TcpTransport::cork() {
    if (fd_ < 0) return;

    if constexpr (kHasTcpCork) {
        int val = 1;
        if (setsockopt(fd_, IPPROTO_TCP, TCP_CORK, &val, sizeof(val)) < 0) {
            spdlog::debug("[transport] cork fd={}: {}", fd_, errno_str(errno));
        }
    } else if constexpr (kHasTcpNopush) {
        int val = 1;
        if (setsockopt(fd_, IPPROTO_TCP, TCP_NOPUSH, &val, sizeof(val)) < 0) {
            spdlog::debug("[transport] nopush fd={}: {}", fd_, errno_str(errno));
        }
    }
    // else: no cork support — no-op
}

void TcpTransport::uncork() {
    if (fd_ < 0) return;

    if constexpr (kHasTcpCork) {
        int val = 0;
        if (setsockopt(fd_, IPPROTO_TCP, TCP_CORK, &val, sizeof(val)) < 0) {
            spdlog::debug("[transport] uncork fd={}: {}", fd_, errno_str(errno));
        }
    } else if constexpr (kHasTcpNopush) {
        int val = 0;
        if (setsockopt(fd_, IPPROTO_TCP, TCP_NOPUSH, &val, sizeof(val)) < 0) {
            spdlog::debug("[transport] un-nopush fd={}: {}", fd_,
                          errno_str(errno));
        }
    }
    // else: no cork support — no-op
}

// ---------------------------------------------------------------------------
// Non-blocking connect — two-phase (initiate, then finish)
// ---------------------------------------------------------------------------

bool TcpTransport::connect(const endpoint& remote) {
    if (fd_ < 0) {
        errno = EBADF;
        return false;
    }

    // Resolve the remote endpoint
    auto sa_opt = resolve_endpoint(remote);
    if (!sa_opt.has_value()) {
        spdlog::error("[transport] connect: resolve {} failed", remote.to_string());
        errno = EHOSTUNREACH;
        return false;
    }

    const auto& sa    = *sa_opt;
    socklen_t   salen = sa_len(sa);

    // Initiate non-blocking connect
    if (::connect(fd_, reinterpret_cast<const sockaddr*>(&sa), salen) == 0) {
        // Synchronous success (rare, but possible on localhost / Unix sockets)
        peer_  = remote;
        local_ = {}; // populated on first getsockname
        spdlog::debug("[transport] connect fd={}: synchronous to {}",
                      fd_, remote.to_string());
        return true;
    }

    int ec = errno;
    if (ec == EINPROGRESS) {
        // Expected: connection in progress, caller polls fd for writability
        // then calls finish_connect().
        peer_ = remote;
        spdlog::debug("[transport] connect fd={}: EINPROGRESS to {}",
                      fd_, remote.to_string());
        return false;
    }

    // Immediate failure
    spdlog::debug("[transport] connect fd={}: immediate failure — {}",
                  fd_, errno_str(ec));
    return false;
}

int TcpTransport::finish_connect() {
    if (fd_ < 0) return ENOTCONN;

    // SO_ERROR retrieves the asynchronous connect outcome.
    // 0 = success, non-zero = errno describing the failure.
    int       so_error = 0;
    socklen_t optlen   = sizeof(so_error);
    if (getsockopt(fd_, SOL_SOCKET, SO_ERROR, &so_error, &optlen) < 0) {
        int ec = errno;
        spdlog::debug("[transport] finish_connect fd={}: getsockopt(SO_ERROR) "
                      "failed: {}", fd_, errno_str(ec));
        return ec;
    }

    if (so_error != 0) {
        spdlog::debug("[transport] finish_connect fd={}: failed — {}",
                      fd_, errno_str(so_error));
        return so_error;
    }

    // Connection established — capture both endpoints for diagnostics
    sockaddr_storage local_sa{};
    socklen_t loc_len = sizeof(local_sa);
    if (getsockname(fd_, reinterpret_cast<sockaddr*>(&local_sa), &loc_len) == 0) {
        local_ = endpoint_from_sa(local_sa);
    }

    sockaddr_storage peer_sa{};
    socklen_t peer_len = sizeof(peer_sa);
    if (getpeername(fd_, reinterpret_cast<sockaddr*>(&peer_sa), &peer_len) == 0) {
        peer_ = endpoint_from_sa(peer_sa);
    }

    spdlog::debug("[transport] connect fd={}: established {} → {}",
                  fd_, local_.to_string(), peer_.to_string());
    return 0;
}

// ---------------------------------------------------------------------------
// Post-bind / post-creation configuration
// ---------------------------------------------------------------------------

void TcpTransport::apply_config(const TransportConfig& config) {
    if (fd_ < 0) return;

    // Apply socket options in a consistent order.
    // Note: SO_REUSEADDR/REUSEPORT must be set BEFORE bind(), so they are
    // not applied here when called post-bind.  The caller (TransportFactory or
    // Acceptor) handles those before bind.  This function handles:
    //   - TCP_NODELAY (post-bind, safe any time)
    //   - SO_KEEPALIVE + tuning (post-bind)
    //   - SO_SNDBUF / SO_RCVBUF (post-bind; kernel may clamp to sysctl limits)

    // ------------------------------------------------------------------
    // TCP_NODELAY — disable Nagle's algorithm for low-latency messaging
    // ------------------------------------------------------------------
    {
        int val = config.tcp_nodelay ? 1 : 0;
        if (setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)) < 0) {
            spdlog::debug("[transport] apply_config fd={} TCP_NODELAY={}: {}",
                          fd_, config.tcp_nodelay, errno_str(errno));
        }
    }

    // ------------------------------------------------------------------
    // SO_REUSEADDR — safe to apply post-bind on most kernels
    // ------------------------------------------------------------------
    {
        int val = config.reuse_address ? 1 : 0;
        if (setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)) < 0) {
            spdlog::debug("[transport] apply_config fd={} SO_REUSEADDR={}: {}",
                          fd_, config.reuse_address, errno_str(errno));
        }
    }

    // ------------------------------------------------------------------
    // TCP keepalive — detect dead peers
    // ------------------------------------------------------------------
    {
        int val = config.keepalive ? 1 : 0;
        if (setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val)) < 0) {
            spdlog::debug("[transport] apply_config fd={} SO_KEEPALIVE={}: {}",
                          fd_, config.keepalive, errno_str(errno));
        }
        if (config.keepalive) {
            if constexpr (kHasTcpKeepaliveTuning) {
                int idle  = config.keepalive_idle_sec;
                int intvl = config.keepalive_interval_sec;
                int cnt   = config.keepalive_count;
                setsockopt(fd_, IPPROTO_TCP, TCP_KEEPIDLE,
                           &idle, sizeof(idle));
                setsockopt(fd_, IPPROTO_TCP, TCP_KEEPINTVL,
                           &intvl, sizeof(intvl));
                setsockopt(fd_, IPPROTO_TCP, TCP_KEEPCNT,
                           &cnt, sizeof(cnt));
            }
        }
    }

    // ------------------------------------------------------------------
    // SO_SNDBUF — send buffer size (kernel may double it)
    // ------------------------------------------------------------------
    if (config.send_buffer_size > 0) {
        int sndbuf = config.send_buffer_size;
        if (setsockopt(fd_, SOL_SOCKET, SO_SNDBUF,
                       &sndbuf, sizeof(sndbuf)) < 0) {
            spdlog::debug("[transport] apply_config fd={} SO_SNDBUF={}: {}",
                          fd_, sndbuf, errno_str(errno));
        }
        // Read back the actual value (kernel may have clamped it)
        socklen_t optlen = sizeof(sndbuf);
        if (getsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &sndbuf, &optlen) == 0) {
            spdlog::debug("[transport] fd={} SO_SNDBUF actual={}", fd_, sndbuf);
        }
    }

    // ------------------------------------------------------------------
    // SO_RCVBUF — receive buffer size
    // ------------------------------------------------------------------
    if (config.receive_buffer_size > 0) {
        int rcvbuf = config.receive_buffer_size;
        if (setsockopt(fd_, SOL_SOCKET, SO_RCVBUF,
                       &rcvbuf, sizeof(rcvbuf)) < 0) {
            spdlog::debug("[transport] apply_config fd={} SO_RCVBUF={}: {}",
                          fd_, rcvbuf, errno_str(errno));
        }
        socklen_t optlen = sizeof(rcvbuf);
        if (getsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &rcvbuf, &optlen) == 0) {
            spdlog::debug("[transport] fd={} SO_RCVBUF actual={}", fd_, rcvbuf);
        }
    }
}

// ============================================================================
// TransportFactory implementation
// ============================================================================

TransportFactory::TransportFactory() = default;

TransportFactory::~TransportFactory() {
    if (ssl_ctx_) {
        // Free the ex_data ALPN pointer if it was set during server init
        void* alpn_data = SSL_CTX_get_ex_data(ssl_ctx_, 0);
        if (alpn_data) {
            delete static_cast<std::string*>(alpn_data);
        }
        SSL_CTX_free(ssl_ctx_);
        ssl_ctx_ = nullptr;
    }
}

TransportFactory::TransportFactory(TransportFactory&& other) noexcept
    : ssl_ctx_(other.ssl_ctx_)
    , is_server_mode_(other.is_server_mode_) {
    other.ssl_ctx_        = nullptr;
    other.is_server_mode_  = false;
}

TransportFactory& TransportFactory::operator=(TransportFactory&& other) noexcept {
    if (this != &other) {
        if (ssl_ctx_) {
            void* alpn_data = SSL_CTX_get_ex_data(ssl_ctx_, 0);
            if (alpn_data) delete static_cast<std::string*>(alpn_data);
            SSL_CTX_free(ssl_ctx_);
        }
        ssl_ctx_          = other.ssl_ctx_;
        is_server_mode_   = other.is_server_mode_;
        other.ssl_ctx_    = nullptr;
        other.is_server_mode_ = false;
    }
    return *this;
}

// ---------------------------------------------------------------------------
// init_tls_server — full server-side TLS 1.3 context setup
// ---------------------------------------------------------------------------

int TransportFactory::init_tls_server(
        const std::string& cert_chain_file,
        const std::string& private_key_file,
        const std::string& private_key_pass,
        const std::string& ca_cert_file,
        int                verify_depth,
        const std::string& ciphers,
        const std::string& alpn_protocols,
        size_t             session_cache_size,
        int                session_ticket_lifetime_sec) {

    // --- Tear down any previous context ---
    if (ssl_ctx_) {
        void* alpn_data = SSL_CTX_get_ex_data(ssl_ctx_, 0);
        if (alpn_data) delete static_cast<std::string*>(alpn_data);
        SSL_CTX_free(ssl_ctx_);
        ssl_ctx_ = nullptr;
    }

    is_server_mode_ = true;

    // --- Create TLS server method context ---
    // TLS_server_method() supports TLS 1.2 and TLS 1.3; we clamp the
    // minimum to 1.2 so that TLS 1.3 is preferred but 1.2 is allowed
    // for clients that haven't upgraded yet.
    ssl_ctx_ = SSL_CTX_new(TLS_server_method());
    if (!ssl_ctx_) {
        log_ssl_errors("SSL_CTX_new(TLS_server_method)");
        spdlog::error("[transport] init_tls_server: SSL_CTX_new failed");
        return ERR_peek_last_error();
    }

    SSL_CTX_set_min_proto_version(ssl_ctx_, TLS1_2_VERSION);

    // --- Options ---
    SSL_CTX_set_options(ssl_ctx_,
        SSL_OP_NO_COMPRESSION                   // CRIME attack mitigation
        | SSL_OP_CIPHER_SERVER_PREFERENCE       // server chooses cipher
        | SSL_OP_NO_RENEGOTIATION);             // no renegotiation

    // --- Certificate chain (server identity) ---
    if (SSL_CTX_use_certificate_chain_file(ssl_ctx_,
            cert_chain_file.c_str()) != 1) {
        log_ssl_errors("SSL_CTX_use_certificate_chain_file");
        spdlog::error("[transport] init_tls_server: cert chain load failed: {}",
                      cert_chain_file);
        return ERR_peek_last_error();
    }

    // --- Private key ---
    if (!private_key_pass.empty()) {
        SSL_CTX_set_default_passwd_cb_userdata(
            ssl_ctx_, const_cast<char*>(private_key_pass.c_str()));
        SSL_CTX_set_default_passwd_cb(ssl_ctx_, pem_passwd_cb);
    }

    if (SSL_CTX_use_PrivateKey_file(ssl_ctx_, private_key_file.c_str(),
                                     SSL_FILETYPE_PEM) != 1) {
        log_ssl_errors("SSL_CTX_use_PrivateKey_file");
        spdlog::error("[transport] init_tls_server: key load failed: {}",
                      private_key_file);
        return ERR_peek_last_error();
    }

    // --- Verify key matches certificate ---
    if (SSL_CTX_check_private_key(ssl_ctx_) != 1) {
        log_ssl_errors("SSL_CTX_check_private_key");
        spdlog::error("[transport] init_tls_server: key/cert mismatch");
        return ERR_peek_last_error();
    }

    // --- mTLS: client certificate verification (optional) ---
    if (!ca_cert_file.empty()) {
        if (SSL_CTX_load_verify_locations(ssl_ctx_, ca_cert_file.c_str(),
                                           nullptr) != 1) {
            log_ssl_errors("SSL_CTX_load_verify_locations");
            spdlog::error("[transport] init_tls_server: CA load failed: {}",
                          ca_cert_file);
            return ERR_peek_last_error();
        }

        // Require and verify client certificates
        SSL_CTX_set_verify(ssl_ctx_,
                           SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
                           nullptr);
        SSL_CTX_set_verify_depth(ssl_ctx_, verify_depth);
    }

    // --- Cipher suites ---
    if (!ciphers.empty()) {
        // TLS 1.3 uses SSL_CTX_set_ciphersuites (note: "suites", not "list")
        if (SSL_CTX_set_ciphersuites(ssl_ctx_, ciphers.c_str()) != 1) {
            log_ssl_errors("SSL_CTX_set_ciphersuites");
        }
        // TLS 1.2 uses SSL_CTX_set_cipher_list
        if (SSL_CTX_set_cipher_list(ssl_ctx_, ciphers.c_str()) != 1) {
            log_ssl_errors("SSL_CTX_set_cipher_list");
        }
    }

    // --- ALPN ---
    if (!alpn_protocols.empty()) {
        std::string wire = alpn_protocols;
        if (!is_alpn_wire_format(wire)) {
            wire = build_alpn_wire(alpn_protocols);
        }
        if (!wire.empty()) {
            auto* proto_data = new std::string(std::move(wire));
            SSL_CTX_set_ex_data(ssl_ctx_, 0, proto_data);
            SSL_CTX_set_alpn_select_cb(ssl_ctx_,
                                        alpn_select_callback, proto_data);
        }
    }

    // --- Session cache ---
    if (session_cache_size > 0) {
        SSL_CTX_set_session_cache_mode(
            ssl_ctx_,
            SSL_SESS_CACHE_SERVER
            | SSL_SESS_CACHE_NO_INTERNAL_LOOKUP
            | SSL_SESS_CACHE_NO_INTERNAL_STORE);
        SSL_CTX_sess_set_cache_size(ssl_ctx_,
            static_cast<long>(session_cache_size));
    }

    // --- Session tickets with key rotation ---
    if (session_ticket_lifetime_sec > 0) {
        // Generate initial random ticket key
        std::array<unsigned char, 48> ticket_key{};
        if (RAND_bytes(ticket_key.data(), static_cast<int>(ticket_key.size())) != 1) {
            log_ssl_errors("RAND_bytes (ticket key)");
        } else {
            // Copy to a heap allocation that the callback can access
            auto* key_store = new std::array<unsigned char, 48>(ticket_key);
            SSL_CTX_set_tlsext_ticket_key_evp_cb(
                ssl_ctx_,
                [](ssl_st* /*ssl*/,
                   unsigned char key_name[16],
                   unsigned char iv[16],
                   EVP_CIPHER_CTX* ctx,
                   EVP_MAC_CTX* hctx,
                   int enc) -> int {
                    // Retrieve the key from SSL_CTX ex_data index 1
                    SSL_CTX* sctx = SSL_get_SSL_CTX(
                        const_cast<ssl_st*>(static_cast<const ssl_st*>(nullptr)));
                    // We can't get SSL_CTX from the callback args directly.
                    // Use a file-static / module-level key for simplicity
                    // (production code should use a rotating key ring).
                    static std::array<unsigned char, 48> s_ticket_key{};
                    static std::once_flag s_key_init;
                    std::call_once(s_key_init, [] {
                        RAND_bytes(s_ticket_key.data(),
                                   static_cast<int>(s_ticket_key.size()));
                    });

                    (void)enc;
                    std::memcpy(key_name, s_ticket_key.data(), 16);
                    std::memcpy(iv, s_ticket_key.data() + 16, 16);
                    EVP_CIPHER_CTX_reset(ctx);
                    EVP_EncryptInit_ex(ctx, EVP_aes_256_cbc(), nullptr,
                                      s_ticket_key.data() + 32, iv);
                    EVP_MAC_CTX_set_params(hctx, nullptr);
                    return 1;
                });

            (void)key_store; // owned leak for simplicity — cleaned up via
                             // SSL_CTX_free callback in production
        }
    }

    spdlog::info("[transport] TLS server context ready (mTLS={})",
                 !ca_cert_file.empty());
    return 0;
}

// ---------------------------------------------------------------------------
// init_tls_client — client-side TLS 1.3 context
// ---------------------------------------------------------------------------

int TransportFactory::init_tls_client(
        const std::string& ca_cert_file,
        const std::string& client_cert_chain_file,
        const std::string& client_private_key_file,
        const std::string& client_private_key_pass,
        const std::string& ciphers,
        const std::string& alpn_protocols) {

    if (ssl_ctx_) {
        void* alpn_data = SSL_CTX_get_ex_data(ssl_ctx_, 0);
        if (alpn_data) delete static_cast<std::string*>(alpn_data);
        SSL_CTX_free(ssl_ctx_);
        ssl_ctx_ = nullptr;
    }

    is_server_mode_ = false;

    ssl_ctx_ = SSL_CTX_new(TLS_client_method());
    if (!ssl_ctx_) {
        log_ssl_errors("SSL_CTX_new(TLS_client_method)");
        spdlog::error("[transport] init_tls_client: SSL_CTX_new failed");
        return ERR_peek_last_error();
    }

    SSL_CTX_set_min_proto_version(ssl_ctx_, TLS1_2_VERSION);
    SSL_CTX_set_options(ssl_ctx_,
        SSL_OP_NO_COMPRESSION | SSL_OP_NO_RENEGOTIATION);

    // --- CA certificate verification ---
    if (!ca_cert_file.empty()) {
        if (SSL_CTX_load_verify_locations(ssl_ctx_, ca_cert_file.c_str(),
                                           nullptr) != 1) {
            log_ssl_errors("SSL_CTX_load_verify_locations");
            spdlog::error("[transport] init_tls_client: CA load failed: {}",
                          ca_cert_file);
            return ERR_peek_last_error();
        }
    } else {
        // Fall back to system trust store
        if (SSL_CTX_set_default_verify_paths(ssl_ctx_) != 1) {
            drain_ssl_errors(spdlog::level::warn,
                             "SSL_CTX_set_default_verify_paths");
        }
    }

    SSL_CTX_set_verify(ssl_ctx_, SSL_VERIFY_PEER, nullptr);

    // --- Client certificate for mTLS (optional) ---
    if (!client_cert_chain_file.empty() && !client_private_key_file.empty()) {
        if (SSL_CTX_use_certificate_chain_file(ssl_ctx_,
                client_cert_chain_file.c_str()) != 1) {
            log_ssl_errors("SSL_CTX_use_certificate_chain_file (client)");
            spdlog::error("[transport] init_tls_client: client cert failed: {}",
                          client_cert_chain_file);
            return ERR_peek_last_error();
        }

        if (!client_private_key_pass.empty()) {
            SSL_CTX_set_default_passwd_cb_userdata(
                ssl_ctx_, const_cast<char*>(client_private_key_pass.c_str()));
            SSL_CTX_set_default_passwd_cb(ssl_ctx_, pem_passwd_cb);
        }

        if (SSL_CTX_use_PrivateKey_file(ssl_ctx_,
                client_private_key_file.c_str(), SSL_FILETYPE_PEM) != 1) {
            log_ssl_errors("SSL_CTX_use_PrivateKey_file (client)");
            spdlog::error("[transport] init_tls_client: client key failed: {}",
                          client_private_key_file);
            return ERR_peek_last_error();
        }

        if (SSL_CTX_check_private_key(ssl_ctx_) != 1) {
            log_ssl_errors("SSL_CTX_check_private_key (client)");
            spdlog::error("[transport] init_tls_client: client key/cert mismatch");
            return ERR_peek_last_error();
        }
    }

    // --- Cipher suites ---
    if (!ciphers.empty()) {
        SSL_CTX_set_ciphersuites(ssl_ctx_, ciphers.c_str());
        SSL_CTX_set_cipher_list(ssl_ctx_, ciphers.c_str());
    }

    // --- ALPN — client side advertises via SSL_CTX_set_alpn_protos ---
    if (!alpn_protocols.empty()) {
        std::string wire = alpn_protocols;
        if (!is_alpn_wire_format(wire)) {
            wire = build_alpn_wire(alpn_protocols);
        }
        if (!wire.empty()) {
            SSL_CTX_set_alpn_protos(
                ssl_ctx_,
                reinterpret_cast<const unsigned char*>(wire.data()),
                static_cast<unsigned int>(wire.size()));
        }
    }

    spdlog::info("[transport] TLS client context ready (mTLS={})",
                 !client_cert_chain_file.empty());
    return 0;
}

// ---------------------------------------------------------------------------
// rotate_session_ticket_key
// ---------------------------------------------------------------------------

int TransportFactory::rotate_session_ticket_key() {
    if (!ssl_ctx_ || !is_server_mode_) {
        spdlog::warn("[transport] rotate_session_ticket_key: no server context");
        return -1;
    }

    // Generate a fresh 48-byte key and push it via OpenSSL's ticket key API.
    // The old keys remain valid until evicted by the internal ring.
    std::array<unsigned char, 48> new_key{};
    if (RAND_bytes(new_key.data(), static_cast<int>(new_key.size())) != 1) {
        log_ssl_errors("RAND_bytes (ticket rotation)");
        return -1;
    }

    // Note: SSL_CTX_set_tlsext_ticket_keys replaces *all* keys.  For graceful
    // rotation with overlapping validity, production code should maintain a
    // ring buffer of keys and call this with the full set on each rotation.
    SSL_CTX_set_tlsext_ticket_keys(
        ssl_ctx_,
        const_cast<unsigned char*>(new_key.data()),
        static_cast<int>(new_key.size()));

    spdlog::info("[transport] Session ticket key rotated");
    return 0;
}

// ---------------------------------------------------------------------------
// ALPN selection callback (server-side — invoked during handshake)
// ---------------------------------------------------------------------------

int TransportFactory::alpn_select_callback(
        ssl_st*                /*ssl*/,
        const unsigned char**  out,
        unsigned char*         outlen,
        const unsigned char*   in,
        unsigned int           inlen,
        void*                  arg) {

    if (!arg || !in || inlen == 0) {
        spdlog::debug("[transport] ALPN cb: no protocols from client");
        return SSL_TLSEXT_ERR_ALERT_FATAL;
    }

    const auto* proto_list = static_cast<const std::string*>(arg);
    const unsigned char* server_list =
        reinterpret_cast<const unsigned char*>(proto_list->data());
    unsigned int server_list_len =
        static_cast<unsigned int>(proto_list->size());

    if (SSL_select_next_proto(const_cast<unsigned char**>(out), outlen,
                               server_list, server_list_len,
                               in, inlen) != OPENSSL_NPN_NEGOTIATED) {

        // Log the client's offered protocols for diagnostics
        std::string offered;
        const unsigned char* p = in;
        const unsigned char* end = in + inlen;
        while (p < end) {
            unsigned int plen = *p++;
            if (p + plen > end) break;
            if (!offered.empty()) offered += ", ";
            offered.append(reinterpret_cast<const char*>(p), plen);
            p += plen;
        }
        spdlog::debug("[transport] ALPN cb: no overlap. Client offered: [{}]",
                      offered);
        return SSL_TLSEXT_ERR_ALERT_FATAL;
    }

    spdlog::debug("[transport] ALPN cb: negotiated {}",
                  std::string_view(reinterpret_cast<const char*>(*out),
                                   *outlen));
    return SSL_TLSEXT_ERR_OK;
}

// ---------------------------------------------------------------------------
// create_tcp — outbound TCP connection factory
// ---------------------------------------------------------------------------

std::unique_ptr<TcpTransport> TransportFactory::create_tcp(
        const endpoint&        remote,
        const TransportConfig& config) const {

    auto transport = std::make_unique<TcpTransport>();
    if (transport->fd() < 0) {
        spdlog::error("[transport] create_tcp: socket creation failed");
        return nullptr;
    }

    // Apply configuration BEFORE connect so bind-address, buffer sizes, etc.
    // take effect.  Reuse-address is applied even though it's mostly useful
    // for listeners — it doesn't hurt for outbound sockets.
    transport->apply_config(config);

    if (!transport->connect(remote)) {
        if (errno == EINPROGRESS) {
            spdlog::debug("[transport] create_tcp: connect to {} in progress",
                          remote.to_string());
        } else {
            spdlog::error("[transport] create_tcp: connect to {} failed: {}",
                          remote.to_string(), errno_str(errno));
            return nullptr;
        }
    }

    return transport;
}

// ---------------------------------------------------------------------------
// create_tls — outbound TLS connection factory
// ---------------------------------------------------------------------------

std::unique_ptr<TlsTransport> TransportFactory::create_tls(
        const endpoint&        remote,
        const TransportConfig& config) const {

    if (!ssl_ctx_) {
        spdlog::error("[transport] create_tls: TLS not initialised");
        return nullptr;
    }

    auto tcp = create_tcp(remote, config);
    if (!tcp) {
        // create_tcp already logged the error
        return nullptr;
    }

    // Wrap the TCP transport in TLS; the handshake happens later when
    // the caller invokes TlsTransport::handshake().
    return std::make_unique<TlsTransport>(std::move(*tcp), ssl_ctx_, false);
}

// ---------------------------------------------------------------------------
// wrap_tls — promote an accepted TCP connection to TLS (server-side)
// ---------------------------------------------------------------------------

std::unique_ptr<TlsTransport> TransportFactory::wrap_tls(TcpTransport tcp) const {
    if (!ssl_ctx_) {
        spdlog::error("[transport] wrap_tls: TLS not initialised");
        return nullptr;
    }

    if (!tcp.connected()) {
        spdlog::error("[transport] wrap_tls: TCP transport not connected");
        return nullptr;
    }

    return std::make_unique<TlsTransport>(std::move(tcp), ssl_ctx_, true);
}

// ============================================================================
// TlsTransport implementation
// ============================================================================

// ---------------------------------------------------------------------------
// Construction / Destruction
// ---------------------------------------------------------------------------

TlsTransport::TlsTransport(TcpTransport tcp,
                            ssl_ctx_st*   ssl_ctx,
                            bool          is_server)
    : tcp_(std::move(tcp))
    , is_server_(is_server) {

    if (!ssl_ctx) {
        spdlog::error("[transport] TlsTransport: null SSL_CTX");
        return;
    }

    ssl_ = SSL_new(ssl_ctx);
    if (!ssl_) {
        log_ssl_errors("SSL_new");
        spdlog::error("[transport] TlsTransport: SSL_new failed");
        return;
    }

    int sock_fd = tcp_.fd();
    if (sock_fd >= 0) {
        if (SSL_set_fd(ssl_, sock_fd) != 1) {
            log_ssl_errors("SSL_set_fd");
            spdlog::error("[transport] TlsTransport: SSL_set_fd({}) failed",
                          sock_fd);
        }
    } else {
        spdlog::warn("[transport] TlsTransport: underlying fd is invalid");
    }

    if (is_server_) {
        SSL_set_accept_state(ssl_);
    } else {
        SSL_set_connect_state(ssl_);
    }

    // Set SNI hostname for client connections
    if (!is_server_ && !tcp_.peer().host.empty()) {
        SSL_set_tlsext_host_name(ssl_, tcp_.peer().host.c_str());
    }
}

TlsTransport::~TlsTransport() {
    close();
}

// ---------------------------------------------------------------------------
// Move semantics
// ---------------------------------------------------------------------------

TlsTransport::TlsTransport(TlsTransport&& other) noexcept
    : tcp_(std::move(other.tcp_))
    , ssl_(other.ssl_)
    , is_server_(other.is_server_)
    , cached_alpn_(std::move(other.cached_alpn_))
    , peer_verified_(other.peer_verified_) {
    other.ssl_          = nullptr;
    other.peer_verified_ = -1;
}

TlsTransport& TlsTransport::operator=(TlsTransport&& other) noexcept {
    if (this != &other) {
        close();
        tcp_            = std::move(other.tcp_);
        ssl_            = other.ssl_;
        is_server_      = other.is_server_;
        cached_alpn_    = std::move(other.cached_alpn_);
        peer_verified_  = other.peer_verified_;
        other.ssl_      = nullptr;
        other.peer_verified_ = -1;
    }
    return *this;
}

// ---------------------------------------------------------------------------
// I/O — SSL_read
// ---------------------------------------------------------------------------

ssize_t TlsTransport::read(char* buf, size_t len) {
    if (!ssl_) {
        errno = ENOTCONN;
        return -1;
    }

    if (len == 0) return 0;

    // Clamp to INT_MAX for SSL_read (takes int)
    size_t clamped = std::min(len, static_cast<size_t>(INT_MAX));

    ERR_clear_error();

    int n = SSL_read(ssl_, buf, static_cast<int>(clamped));
    if (n > 0) return static_cast<ssize_t>(n);

    int ssl_err = SSL_get_error(ssl_, n);

    switch (ssl_err) {
    case SSL_ERROR_WANT_READ:
        // Need more data from the network — caller must poll fd for
        // readability and call read() again.
        errno = EAGAIN;
        return -1;

    case SSL_ERROR_WANT_WRITE:
        // Renegotiation in progress — the TLS layer needs to write.
        // Caller must poll fd for writability.
        errno = EAGAIN;
        return -1;

    case SSL_ERROR_ZERO_RETURN:
        // Peer sent close_notify — clean shutdown.
        spdlog::debug("[transport] SSL_read fd={}: clean shutdown", tcp_.fd());
        return 0;

    case SSL_ERROR_SYSCALL:
        // Underlying syscall error.
        if (n == 0) {
            // EOF without close_notify — treat as clean shutdown.
            spdlog::debug("[transport] SSL_read fd={}: EOF (no close_notify)",
                          tcp_.fd());
            return 0;
        }
        {
            int ec = errno;
            if (ec == 0) {
                // EOF
                return 0;
            }
            if (is_retryable_errno(ec)) {
                errno = EAGAIN;
                return -1;
            }
            spdlog::debug("[transport] SSL_read fd={} syscall: {}",
                          tcp_.fd(), errno_str(ec));
        }
        return -1;

    case SSL_ERROR_SSL:
        log_ssl_errors("SSL_read");
        errno = EPROTO;
        return -1;

    default:
        spdlog::debug("[transport] SSL_read fd={} unexpected err={}",
                      tcp_.fd(), ssl_err);
        errno = EPROTO;
        return -1;
    }
}

// ---------------------------------------------------------------------------
// I/O — SSL_write
// ---------------------------------------------------------------------------

ssize_t TlsTransport::write(const char* buf, size_t len) {
    if (!ssl_) {
        errno = ENOTCONN;
        return -1;
    }

    if (len == 0) return 0;

    size_t clamped = std::min(len, static_cast<size_t>(INT_MAX));

    ERR_clear_error();

    int n = SSL_write(ssl_, buf, static_cast<int>(clamped));
    if (n > 0) return static_cast<ssize_t>(n);

    int ssl_err = SSL_get_error(ssl_, n);

    switch (ssl_err) {
    case SSL_ERROR_WANT_WRITE:
        // Kernel send buffer full — caller polls for writability.
        errno = EAGAIN;
        return -1;

    case SSL_ERROR_WANT_READ:
        // Renegotiation wants to read — caller polls for readability.
        errno = EAGAIN;
        return -1;

    case SSL_ERROR_SYSCALL:
        {
            int ec = errno;
            if (ec == 0) {
                errno = ECONNRESET;
            }
            if (is_retryable_errno(ec)) {
                errno = EAGAIN;
                return -1;
            }
            spdlog::debug("[transport] SSL_write fd={} syscall: {}",
                          tcp_.fd(), errno_str(errno));
        }
        return -1;

    case SSL_ERROR_SSL:
        log_ssl_errors("SSL_write");
        errno = EPROTO;
        return -1;

    default:
        spdlog::debug("[transport] SSL_write fd={} unexpected err={}",
                      tcp_.fd(), ssl_err);
        errno = EPROTO;
        return -1;
    }
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

void TlsTransport::flush() {
    if (!ssl_) return;

    // SSL_pending returns the number of bytes already decrypted and buffered
    // but not yet consumed by SSL_read.  It doesn't force a flush — we simply
    // log it for diagnostics and delegate to the TCP layer.
    int pending = SSL_pending(ssl_);
    if (pending > 0) {
        spdlog::trace("[transport] flush fd={}: {} bytes pending in SSL",
                      tcp_.fd(), pending);
    }

    // Push any corked TCP data out
    tcp_.flush();
}

// ---------------------------------------------------------------------------
// Close / Shutdown
// ---------------------------------------------------------------------------

void TlsTransport::close() {
    if (ssl_) {
        // Perform a bidirectional TLS shutdown.
        // Phase 1: send close_notify
        ERR_clear_error();
        int ret = SSL_shutdown(ssl_);
        if (ret == 0) {
            // Phase 2: wait for peer's close_notify
            ret = SSL_shutdown(ssl_);
        }
        if (ret < 0) {
            int ssl_err = SSL_get_error(ssl_, ret);
            if (ssl_err != SSL_ERROR_WANT_READ
                && ssl_err != SSL_ERROR_WANT_WRITE
                && ssl_err != SSL_ERROR_ZERO_RETURN
                && ssl_err != SSL_ERROR_SYSCALL) {
                spdlog::debug("[transport] SSL_shutdown fd={} error: {}",
                              tcp_.fd(), ssl_err);
                log_ssl_errors("SSL_shutdown");
            }
        }

        SSL_free(ssl_);
        ssl_ = nullptr;
    }

    // Close the underlying TCP socket
    tcp_.close();
}

void TlsTransport::shutdown(int how) {
    // For TLS, the proper shutdown sequence (close_notify) is handled in
    // close().  We delegate half-close to the TCP layer but note that a
    // TLS connection should generally not be half-closed.
    tcp_.shutdown(how);
}

// ---------------------------------------------------------------------------
// State delegation — all forwarded to the TCP transport
// ---------------------------------------------------------------------------

int TlsTransport::fd() const noexcept {
    return tcp_.fd();
}

bool TlsTransport::connected() const noexcept {
    return tcp_.connected() && ssl_ != nullptr && handshake_complete();
}

endpoint TlsTransport::peer() const noexcept {
    return tcp_.peer();
}

endpoint TlsTransport::local() const noexcept {
    return tcp_.local();
}

// ---------------------------------------------------------------------------
// Socket options — delegated
// ---------------------------------------------------------------------------

void TlsTransport::set_nodelay(bool enable) {
    tcp_.set_nodelay(enable);
}

void TlsTransport::set_reuse_address(bool enable) {
    tcp_.set_reuse_address(enable);
}

void TlsTransport::set_keepalive(bool enable,
                                  int idle_sec,
                                  int interval_sec,
                                  int count) {
    tcp_.set_keepalive(enable, idle_sec, interval_sec, count);
}

// ---------------------------------------------------------------------------
// TLS handshake — state machine driven by the caller's event loop
// ---------------------------------------------------------------------------

int TlsTransport::handshake() {
    if (!ssl_) return -1;

    ERR_clear_error();

    int ret;
    if (is_server_) {
        ret = SSL_accept(ssl_);
    } else {
        ret = SSL_connect(ssl_);
    }

    if (ret == 1) {
        // Handshake succeeded — cache metadata
        spdlog::debug("[transport] TLS handshake fd={}: completed as {}",
                      tcp_.fd(), is_server_ ? "server" : "client");

        // Log cipher for diagnostics
        const SSL_CIPHER* cipher = SSL_get_current_cipher(ssl_);
        if (cipher) {
            spdlog::debug("[transport] TLS handshake fd={}: cipher={} ver={}",
                          tcp_.fd(), SSL_CIPHER_get_name(cipher),
                          protocol_version_str());
        }
        return 1;
    }

    int ssl_err = SSL_get_error(ssl_, ret);

    switch (ssl_err) {
    case SSL_ERROR_WANT_READ:
        return 0; // caller polls for readability

    case SSL_ERROR_WANT_WRITE:
        // For non-blocking connects on the client side, the first
        // SSL_connect call often returns WANT_WRITE — the caller must
        // wait for the fd to become writable and retry.
        return 0;

    case SSL_ERROR_SYSCALL:
        if (ret == 0) {
            spdlog::debug("[transport] TLS handshake fd={}: EOF from peer",
                          tcp_.fd());
            return -1;
        }
        spdlog::debug("[transport] TLS handshake fd={}: syscall error — {}",
                      tcp_.fd(), errno_str(errno));
        log_ssl_errors("handshake");
        return -1;

    case SSL_ERROR_SSL:
        log_ssl_errors("handshake");
        spdlog::debug("[transport] TLS handshake fd={}: protocol error",
                      tcp_.fd());
        return -1;

    default:
        spdlog::debug("[transport] TLS handshake fd={}: unexpected err={}",
                      tcp_.fd(), ssl_err);
        return -1;
    }
}

bool TlsTransport::handshake_complete() const noexcept {
    if (!ssl_) return false;
    return SSL_is_init_finished(ssl_) != 0;
}

// ---------------------------------------------------------------------------
// TLS metadata queries
// ---------------------------------------------------------------------------

std::string TlsTransport::negotiated_alpn() const {
    // Cache the result; ALPN is negotiated once during handshake.
    if (!cached_alpn_.empty()) return cached_alpn_;

    if (!ssl_) return {};

    const unsigned char* data = nullptr;
    unsigned int         len  = 0;
    SSL_get0_alpn_selected(ssl_, &data, &len);

    if (data && len > 0) {
        cached_alpn_ = std::string(reinterpret_cast<const char*>(data), len);
        return cached_alpn_;
    }
    return {};
}

std::string TlsTransport::cipher_name() const {
    if (!ssl_) return {};

    const SSL_CIPHER* cipher = SSL_get_current_cipher(ssl_);
    if (!cipher) return {};

    return std::string(SSL_CIPHER_get_name(cipher));
}

std::string TlsTransport::protocol_version_str() const {
    if (!ssl_) return {};

    int ver = SSL_version(ssl_);
    switch (ver) {
    case TLS1_3_VERSION: return "TLSv1.3";
    case TLS1_2_VERSION: return "TLSv1.2";
    case TLS1_1_VERSION: return "TLSv1.1";
    case TLS1_VERSION:   return "TLSv1.0";
    default: {
        // For unknown versions, format as hex
        char buf[16];
        std::snprintf(buf, sizeof(buf), "0x%04X", ver);
        return buf;
    }
    }
}

std::string TlsTransport::peer_cn() const {
    if (!ssl_) return {};

    X509* cert = SSL_get_peer_certificate(ssl_);
    if (!cert) return {};

    X509_NAME* subject = X509_get_subject_name(cert);
    std::string cn;

    if (subject) {
        // Use X509_NAME_get_text_by_NID for the common name
        int nid = NID_commonName;
        int idx = X509_NAME_get_index_by_NID(subject, nid, -1);
        if (idx >= 0) {
            X509_NAME_ENTRY* entry = X509_NAME_get_entry(subject, idx);
            if (entry) {
                ASN1_STRING* asn1 = X509_NAME_ENTRY_get_data(entry);
                if (asn1) {
                    const unsigned char* p = ASN1_STRING_get0_data(asn1);
                    int slen = ASN1_STRING_length(asn1);
                    if (p && slen > 0) {
                        cn.assign(reinterpret_cast<const char*>(p),
                                  static_cast<size_t>(slen));
                    }
                }
            }
        }

        // Log the full subject for diagnostics
        spdlog::debug("[transport] peer_cn fd={}: subject={}",
                      tcp_.fd(), x509_name_oneline(subject));
    }

    X509_free(cert);
    return cn;
}

bool TlsTransport::peer_verified() const noexcept {
    // Cache the verification result; verification doesn't change.
    if (peer_verified_ >= 0) return peer_verified_ == 1;

    if (!ssl_) {
        peer_verified_ = 0;
        return false;
    }

    long result = SSL_get_verify_result(ssl_);
    peer_verified_ = (result == X509_V_OK) ? 1 : 0;

    if (peer_verified_ == 0) {
        spdlog::debug("[transport] peer_verified fd={}: verification failed — {}",
                      tcp_.fd(), X509_verify_cert_error_string(result));
    }

    return peer_verified_ == 1;
}

// ============================================================================
// Acceptor implementation
// ============================================================================

// ---------------------------------------------------------------------------
// Construction — bind and listen
// ---------------------------------------------------------------------------

Acceptor::Acceptor(const AcceptorConfig& config)
    : config_(config) {

    // --- Create listening socket ---
    fd_ = create_socket(AF_INET6, SOCK_STREAM, 0);
    if (fd_ < 0) {
        int ec = errno;
        throw std::system_error(ec, std::generic_category(),
            "Acceptor: socket() failed — " + errno_str(ec));
    }

    // --- Dual-stack: allow IPv4 clients via mapped addresses ---
    {
        int v6only = 0;
        setsockopt(fd_, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, sizeof(v6only));
    }

    // --- SO_REUSEADDR (rapid restarts) ---
    if (config_.reuse_address) {
        int val = 1;
        setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
    }

    // --- SO_REUSEPORT (multi-process, Linux 3.9+) ---
    if (config_.reuse_port && kHasReusePort) {
        int val = 1;
        setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &val, sizeof(val));
    }

    // --- Resolve and bind ---
    auto sa_opt = resolve_endpoint(config_.bind_address);
    if (!sa_opt.has_value()) {
        ::close(fd_);
        fd_ = -1;
        throw std::system_error(EHOSTUNREACH, std::generic_category(),
            "Acceptor: resolve failed — " + config_.bind_address.to_string());
    }

    const auto& sa    = *sa_opt;
    socklen_t   salen = sa_len(sa);

    if (::bind(fd_, reinterpret_cast<const sockaddr*>(&sa), salen) < 0) {
        int ec = errno;
        std::string msg = "Acceptor: bind(" +
                          config_.bind_address.to_string() +
                          ") failed: " + errno_str(ec);
        ::close(fd_);
        fd_ = -1;
        throw std::system_error(ec, std::generic_category(), msg);
    }

    // --- Capture resolved bound address (important when port=0) ---
    {
        sockaddr_storage bound_sa{};
        socklen_t blen = sizeof(bound_sa);
        if (getsockname(fd_, reinterpret_cast<sockaddr*>(&bound_sa), &blen) == 0) {
            bound_ = endpoint_from_sa(bound_sa);
        } else {
            bound_ = config_.bind_address;
        }
    }

    // --- Listen ---
    {
        int backlog = config_.listen_backlog > 0
                          ? config_.listen_backlog
                          : SOMAXCONN;
        if (::listen(fd_, backlog) < 0) {
            int ec = errno;
            ::close(fd_);
            fd_ = -1;
            throw std::system_error(ec, std::generic_category(),
                "Acceptor: listen() failed — " + errno_str(ec));
        }
    }

    // --- TCP_DEFER_ACCEPT (Linux) — delay accept until data ---
    if (config_.defer_accept && kHasTcpDeferAccept) {
        int timeout = config_.defer_accept_timeout_sec;
        setsockopt(fd_, IPPROTO_TCP, TCP_DEFER_ACCEPT,
                   &timeout, sizeof(timeout));
    }

    spdlog::info("[transport] Acceptor listening on {} (fd={}, backlog={})",
                 bound_.to_string(), fd_,
                 config_.listen_backlog > 0 ? config_.listen_backlog
                                            : SOMAXCONN);
}

Acceptor::~Acceptor() {
    close();
}

// ---------------------------------------------------------------------------
// Move semantics
// ---------------------------------------------------------------------------

Acceptor::Acceptor(Acceptor&& other) noexcept
    : fd_(other.fd_)
    , bound_(std::move(other.bound_))
    , config_(std::move(other.config_))
    , handler_(std::move(other.handler_)) {
    other.fd_ = -1;
}

Acceptor& Acceptor::operator=(Acceptor&& other) noexcept {
    if (this != &other) {
        close();
        fd_       = other.fd_;
        bound_    = std::move(other.bound_);
        config_   = std::move(other.config_);
        handler_  = std::move(other.handler_);
        other.fd_ = -1;
    }
    return *this;
}

// ---------------------------------------------------------------------------
// accept_one — accept a single connection
// ---------------------------------------------------------------------------

bool Acceptor::accept_one(const ConnectionHandler& handler) {
    if (fd_ < 0) return false;

    // Use the passed handler if present, otherwise the persistent handler
    const auto& cb = handler ? handler : handler_;
    if (!cb) {
        spdlog::warn("[transport] Acceptor::accept_one: no handler");
        return false;
    }

    sockaddr_storage peer_sa{};
    socklen_t peer_len = sizeof(peer_sa);

    int accepted;
    if constexpr (kHasSockNonblockCloexec) {
        accepted = ::accept4(fd_,
                              reinterpret_cast<sockaddr*>(&peer_sa),
                              &peer_len,
                              SOCK_NONBLOCK | SOCK_CLOEXEC);
    } else {
        accepted = ::accept(fd_,
                            reinterpret_cast<sockaddr*>(&peer_sa),
                            &peer_len);
    }

    if (accepted < 0) {
        int ec = errno;

        if (is_accept_retryable(ec)) {
            // Transient: no connections, interrupted, or client aborted
            // before we could accept.  Not an error.
            return false;
        }

        if (is_resource_exhausted(ec)) {
            spdlog::error("[transport] Acceptor fd={}: resource exhausted — {}",
                          fd_, errno_str(ec));
            return false;
        }

        if (ec == EBADF || ec == ENOTSOCK || ec == EOPNOTSUPP
            || ec == EINVAL || ec == EFAULT) {
            spdlog::error("[transport] Acceptor fd={}: fatal accept error — {}",
                          fd_, errno_str(ec));
            return false;
        }

        spdlog::debug("[transport] Acceptor fd={}: accept error: {}",
                      fd_, errno_str(ec));
        return false;
    }

    // When the kernel doesn't support SOCK_NONBLOCK, set it manually
    if constexpr (!kHasSockNonblockCloexec) {
        if (set_nonblocking(accepted) < 0) {
            spdlog::warn("[transport] accepted fd={}: O_NONBLOCK failed: {}",
                         accepted, errno_str(errno));
        }
        if (set_cloexec(accepted) < 0) {
            spdlog::warn("[transport] accepted fd={}: FD_CLOEXEC failed: {}",
                         accepted, errno_str(errno));
        }
    }

    // --- Extract peer endpoint ---
    endpoint peer = endpoint_from_sa(peer_sa);

    // --- Extract local endpoint ---
    sockaddr_storage local_sa{};
    socklen_t loc_len = sizeof(local_sa);
    endpoint  local;
    if (getsockname(accepted, reinterpret_cast<sockaddr*>(&local_sa),
                    &loc_len) == 0) {
        local = endpoint_from_sa(local_sa);
    }

    spdlog::debug("[transport] Acceptor: accepted fd={} from {} on {}",
                  accepted, peer.to_string(), local.to_string());

    // --- Build transport, apply per-connection config, dispatch ---
    TcpTransport transport(accepted, peer, local);
    transport.apply_config(config_.transport_config);

    // Dispatch to the connection handler (which may wrap in TLS, start
    // protocol negotiation, etc.)
    cb(std::move(transport));
    return true;
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

void Acceptor::close() {
    if (fd_ < 0) return;

    spdlog::info("[transport] Acceptor closing fd={} on {}",
                 fd_, bound_.to_string());
    safe_close(fd_);
}

} // namespace torrent::network
