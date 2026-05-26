/**
 * network_util.cpp — Network Utility Functions
 *
 * Provides common network utility functions used across the torrent-mq
 * codebase.  Includes endpoint parsing, port availability checks, local
 * interface enumeration, hostname validation, and a RAII file descriptor
 * wrapper for safe resource management.
 *
 * Functions:
 *   parse_endpoint(host:port)    — parse "host:port" string into endpoint struct
 *   is_port_available(port)      — check if a TCP port is free to bind
 *   get_local_interfaces()       — enumerate local non-loopback IPs
 *   is_valid_hostname(name)      — validate a hostname
 *   has_reachable_host(host, ms) — check TCP connectivity to host
 *   FdGuard / SocketGuard        — RAII wrappers for file descriptors and sockets
 *
 * Dependencies:
 *   POSIX sockets (<sys/socket.h>, <netdb.h>, <arpa/inet.h>)
 *   <fcntl.h>, <unistd.h> for file descriptor operations
 *   torrent/common/types.h for the endpoint type
 *
 * Thread-safety:
 *   All free-standing functions are thread-safe with no mutable shared state.
 *   FdGuard/SocketGuard instances are not thread-safe (by design, each guard
 *   should be owned by a single thread).
 */

#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace torrent {

// ============================================================================
// Anonymous namespace — helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_netutil_logger() {
    static auto logger = spdlog::get("netutil");
    if (!logger) {
        logger = spdlog::stdout_color_mt("netutil");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

inline constexpr size_t kMaxHostnameLen = 253;
inline constexpr size_t kMaxLabelLen    = 63;
inline constexpr int    kDefaultConnectTimeoutMs = 2000;

// --------------------------------------------------------------------------
// Compile-time helpers
// --------------------------------------------------------------------------

/// Check if a character is valid in a hostname label.
[[nodiscard]] constexpr bool is_hostname_char(char c) noexcept {
    return (c >= 'a' && c <= 'z') ||
           (c >= 'A' && c <= 'Z') ||
           (c >= '0' && c <= '9') ||
           c == '-' || c == '.';
}

} // anonymous namespace

// ============================================================================
// parse_endpoint — parse "host:port" string into endpoint struct
// ============================================================================

[[nodiscard]] std::optional<endpoint> parse_endpoint(std::string_view addr) {
    if (addr.empty()) return std::nullopt;

    // Handle IPv6 addresses enclosed in brackets: [::1]:9092
    if (addr.front() == '[') {
        auto close_br = addr.find(']');
        if (close_br == std::string_view::npos) return std::nullopt;

        std::string host(addr.substr(1, close_br - 1));

        if (close_br + 1 < addr.size() && addr[close_br + 1] == ':') {
            auto port_str = addr.substr(close_br + 2);
            if (port_str.empty()) return std::nullopt;

            try {
                int port_val = std::stoi(std::string(port_str));
                if (port_val < 1 || port_val > 65535) return std::nullopt;
                endpoint ep;
                ep.host = std::move(host);
                ep.port = static_cast<uint16_t>(port_val);
                return ep;
            } catch (...) {
                return std::nullopt;
            }
        }

        endpoint ep;
        ep.host = std::move(host);
        return ep;
    }

    // Standard "host:port" or just "host"
    auto colon = addr.rfind(':');
    if (colon == std::string_view::npos) {
        endpoint ep;
        ep.host = std::string(addr);
        return ep;
    }

    std::string host(addr.substr(0, colon));
    auto port_str = addr.substr(colon + 1);

    if (port_str.empty()) {
        endpoint ep;
        ep.host = std::move(host);
        return ep;
    }

    try {
        int port_val = std::stoi(std::string(port_str));
        if (port_val < 1 || port_val > 65535) return std::nullopt;
        endpoint ep;
        ep.host = std::move(host);
        ep.port = static_cast<uint16_t>(port_val);
        return ep;
    } catch (...) {
        // Couldn't parse port — treat the whole thing as a hostname
        endpoint ep;
        ep.host = std::string(addr);
        return ep;
    }
}

// ============================================================================
// is_port_available — check if a TCP port is free to bind
// ============================================================================

[[nodiscard]] bool is_port_available(uint16_t port) {
    auto logger = get_netutil_logger();

    // Create a test socket
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        logger->error("is_port_available: socket() failed: {}",
                      std::strerror(errno));
        return false;
    }

    // Enable SO_REUSEADDR so we can bind to ports in TIME_WAIT state
    int opt = 1;
    ::setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr {};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);

    int result = ::bind(sock,
                        reinterpret_cast<struct sockaddr*>(&addr),
                        sizeof(addr));

    ::close(sock);

    if (result == 0) {
        logger->trace("is_port_available: port {} is free", port);
        return true;
    }

    if (errno == EADDRINUSE) {
        logger->trace("is_port_available: port {} is in use", port);
        return false;
    }

    logger->error("is_port_available: bind() on port {} failed: {}",
                  port, std::strerror(errno));
    return false;
}

// ============================================================================
// get_local_interfaces — enumerate local non-loopback IP addresses
// ============================================================================

[[nodiscard]] std::vector<std::string> get_local_interfaces() {
    auto logger = get_netutil_logger();
    std::vector<std::string> addresses;

    struct ifaddrs* ifa = nullptr;
    if (::getifaddrs(&ifa) != 0) {
        logger->error("get_local_interfaces: getifaddrs() failed: {}",
                      std::strerror(errno));
        return addresses;
    }

    // RAII cleanup of ifaddrs linked list
    struct IfAddrsGuard {
        struct ifaddrs* ptr;
        ~IfAddrsGuard() { if (ptr) ::freeifaddrs(ptr); }
        IfAddrsGuard(const IfAddrsGuard&) = delete;
        IfAddrsGuard& operator=(const IfAddrsGuard&) = delete;
    } guard{ifa};

    for (struct ifaddrs* i = ifa; i != nullptr; i = i->ifa_next) {
        if (!i->ifa_addr) continue;

        // Skip loopback interfaces
        if (i->ifa_flags & IFF_LOOPBACK) continue;

        // Only consider UP interfaces
        if (!(i->ifa_flags & IFF_UP)) continue;

        char host[NI_MAXHOST] = {};

        if (i->ifa_addr->sa_family == AF_INET) {
            auto* addr = reinterpret_cast<struct sockaddr_in*>(i->ifa_addr);
            if (::inet_ntop(AF_INET, &addr->sin_addr, host, sizeof(host))) {
                if (host[0] != '\0') {
                    addresses.emplace_back(host);
                }
            }
        } else if (i->ifa_addr->sa_family == AF_INET6) {
            auto* addr6 = reinterpret_cast<struct sockaddr_in6*>(i->ifa_addr);
            if (::inet_ntop(AF_INET6, &addr6->sin6_addr, host, sizeof(host))) {
                if (host[0] != '\0' && host[0] != 'f') {  // skip fe80:: (link-local)
                    addresses.emplace_back(host);
                }
            }
        }
    }

    logger->debug("get_local_interfaces: found {} non-loopback interfaces",
                  addresses.size());

    // Sort for deterministic output
    std::sort(addresses.begin(), addresses.end());

    return addresses;
}

// ============================================================================
// is_valid_hostname — validate hostname per RFC 952/1123
// ============================================================================

[[nodiscard]] bool is_valid_hostname(std::string_view name) {
    if (name.empty() || name.size() > kMaxHostnameLen) return false;

    // Check for valid characters
    for (char c : name) {
        if (!is_hostname_char(c)) return false;
    }

    // Split by dots and validate each label
    size_t start = 0;
    while (start < name.size()) {
        size_t dot = name.find('.', start);
        if (dot == std::string_view::npos) dot = name.size();

        size_t label_len = dot - start;

        // Each label must be 1-63 characters
        if (label_len == 0 || label_len > kMaxLabelLen) return false;

        // Labels must not start or end with hyphen
        if (name[start] == '-' || name[dot - 1] == '-') return false;

        // Labels must not be all-numeric (but this is a soft rule — allow)
        // Check that the TLD (last label) is not all-numeric

        start = dot + 1;
    }

    // Last label (TLD) should not be all-numeric
    auto last_dot = name.rfind('.');
    std::string_view tld;
    if (last_dot != std::string_view::npos) {
        tld = name.substr(last_dot + 1);
    } else {
        tld = name;
    }

    bool all_numeric = true;
    for (char c : tld) {
        if (c < '0' || c > '9') {
            all_numeric = false;
            break;
        }
    }
    if (all_numeric && tld.size() > 0) return false;

    return true;
}

// ============================================================================
// has_reachable_host — check TCP connectivity to a host:port
// ============================================================================

[[nodiscard]] bool has_reachable_host(const std::string& host,
                                        uint16_t port,
                                        int timeout_ms = kDefaultConnectTimeoutMs) {
    auto logger = get_netutil_logger();

    // Resolve hostname
    struct addrinfo hints {};
    hints.ai_family   = AF_UNSPEC;     // IPv4 or IPv6
    hints.ai_socktype = SOCK_STREAM;

    std::string port_str = std::to_string(port);
    struct addrinfo* result = nullptr;

    int ret = ::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &result);
    if (ret != 0) {
        logger->debug("has_reachable_host: getaddrinfo('{}:{}') failed: {}",
                      host, port, ::gai_strerror(ret));
        return false;
    }

    // RAII cleanup
    struct AddrInfoGuard {
        struct addrinfo* ptr;
        ~AddrInfoGuard() { if (ptr) ::freeaddrinfo(ptr); }
        AddrInfoGuard(const AddrInfoGuard&) = delete;
        AddrInfoGuard& operator=(const AddrInfoGuard&) = delete;
    } addr_guard{result};

    // Try each address until one connects
    for (struct addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
        int sock = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock < 0) continue;

        // Set non-blocking for timeout
        int flags = ::fcntl(sock, F_GETFL, 0);
        ::fcntl(sock, F_SETFL, flags | O_NONBLOCK);

        int conn_ret = ::connect(sock, rp->ai_addr, rp->ai_addrlen);

        if (conn_ret < 0 && errno != EINPROGRESS) {
            ::close(sock);
            continue;
        }

        if (conn_ret == 0) {
            // Connected immediately
            ::close(sock);
            return true;
        }

        // Wait for connection with timeout
        fd_set fdset;
        FD_ZERO(&fdset);
        FD_SET(sock, &fdset);

        struct timeval tv;
        tv.tv_sec  = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;

        int sel_ret = ::select(sock + 1, nullptr, &fdset, nullptr, &tv);

        if (sel_ret > 0) {
            int err = 0;
            socklen_t len = sizeof(err);
            ::getsockopt(sock, SOL_SOCKET, SO_ERROR, &err, &len);

            ::close(sock);

            if (err == 0) {
                logger->debug("has_reachable_host: {}:{} is reachable",
                              host, port);
                return true;
            }
        } else {
            ::close(sock);
        }
    }

    logger->debug("has_reachable_host: {}:{} is NOT reachable", host, port);
    return false;
}

// ============================================================================
// FdGuard — RAII wrapper for file descriptors
// ============================================================================

class FdGuard {
public:
    FdGuard() noexcept : fd_(-1) {}

    explicit FdGuard(int fd) noexcept : fd_(fd) {}

    ~FdGuard() noexcept { reset(); }

    FdGuard(const FdGuard&) = delete;
    FdGuard& operator=(const FdGuard&) = delete;

    FdGuard(FdGuard&& other) noexcept
        : fd_(other.release()) {}

    FdGuard& operator=(FdGuard&& other) noexcept {
        if (this != &other) {
            reset(other.release());
        }
        return *this;
    }

    [[nodiscard]] int get() const noexcept { return fd_; }

    [[nodiscard]] bool valid() const noexcept { return fd_ >= 0; }

    [[nodiscard]] explicit operator bool() const noexcept { return valid(); }

    void reset(int fd = -1) noexcept {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = fd;
    }

    [[nodiscard]] int release() noexcept {
        int fd = fd_;
        fd_ = -1;
        return fd;
    }

    /// Set non-blocking mode.
    bool set_nonblocking() noexcept {
        if (fd_ < 0) return false;
        int flags = ::fcntl(fd_, F_GETFL, 0);
        if (flags < 0) return false;
        return ::fcntl(fd_, F_SETFL, flags | O_NONBLOCK) == 0;
    }

    /// Set close-on-exec flag.
    bool set_cloexec() noexcept {
        if (fd_ < 0) return false;
        int flags = ::fcntl(fd_, F_GETFD, 0);
        if (flags < 0) return false;
        return ::fcntl(fd_, F_SETFD, flags | FD_CLOEXEC) == 0;
    }

    /// Set TCP_NODELAY on socket (no-op on non-sockets).
    bool set_tcp_nodelay(bool enable = true) noexcept {
        if (fd_ < 0) return false;
        int val = enable ? 1 : 0;
        return ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY,
                            &val, sizeof(val)) == 0;
    }

    /// Set SO_KEEPALIVE on socket.
    bool set_keepalive(bool enable = true) noexcept {
        if (fd_ < 0) return false;
        int val = enable ? 1 : 0;
        return ::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE,
                            &val, sizeof(val)) == 0;
    }

    /// Set send buffer size.
    bool set_send_buffer(int size) noexcept {
        if (fd_ < 0) return false;
        return ::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF,
                            &size, sizeof(size)) == 0;
    }

    /// Set receive buffer size.
    bool set_recv_buffer(int size) noexcept {
        if (fd_ < 0) return false;
        return ::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF,
                            &size, sizeof(size)) == 0;
    }

private:
    int fd_ = -1;
};

// ============================================================================
// SocketGuard — RAII wrapper specialized for sockets
// ============================================================================

class SocketGuard : public FdGuard {
public:
    using FdGuard::FdGuard;

    SocketGuard() noexcept = default;

    /// Create a socket with the given domain, type, and protocol.
    static SocketGuard create(int domain, int type, int protocol) {
        int sock = ::socket(domain, type, protocol);
        if (sock < 0) {
            auto logger = get_netutil_logger();
            logger->error("SocketGuard::create: socket() failed: {}",
                          std::strerror(errno));
        }
        return SocketGuard(sock);
    }

    /// Create a TCP socket for the given address family.
    static SocketGuard tcp_socket(int family = AF_INET) {
        return create(family, SOCK_STREAM, 0);
    }

    /// Create a UDP socket for the given address family.
    static SocketGuard udp_socket(int family = AF_INET) {
        return create(family, SOCK_DGRAM, 0);
    }

    /// Set SO_REUSEADDR.
    bool set_reuseaddr(bool enable = true) noexcept {
        int fd = get();
        if (fd < 0) return false;
        int val = enable ? 1 : 0;
        return ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
                            &val, sizeof(val)) == 0;
    }

    /// Set SO_REUSEPORT (Linux 3.9+).
    bool set_reuseport(bool enable = true) noexcept {
        int fd = get();
        if (fd < 0) return false;
        int val = enable ? 1 : 0;
        return ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT,
                            &val, sizeof(val)) == 0;
    }

    /// Set SO_LINGER with timeout (0 for immediate close).
    bool set_linger(bool enable, int timeout_sec = 0) noexcept {
        int fd = get();
        if (fd < 0) return false;
        struct linger l {};
        l.l_onoff  = enable ? 1 : 0;
        l.l_linger = timeout_sec;
        return ::setsockopt(fd, SOL_SOCKET, SO_LINGER,
                            &l, sizeof(l)) == 0;
    }
};

} // namespace torrent
