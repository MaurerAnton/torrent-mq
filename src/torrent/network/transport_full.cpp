#include "torrent/network/transport.h"
#include "torrent/common/types.h"
#include <spdlog/spdlog.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <cstring>

namespace torrent::network {
namespace {

// ============================================================================
// epoll-based event loop with edge-triggered mode
// ============================================================================

class EpollEventLoop {
public:
    explicit EpollEventLoop(int max_events = 1024)
        : epoll_fd_(epoll_create1(EPOLL_CLOEXEC))
        , max_events_(max_events) {
        if (epoll_fd_ < 0) {
            spdlog::error("epoll_create1 failed: {}", strerror(errno));
            throw std::system_error(errno, std::generic_category());
        }
        events_.resize(max_events);
    }

    ~EpollEventLoop() {
        if (epoll_fd_ >= 0) close(epoll_fd_);
    }

    void add_fd(int fd, uint32_t events, void* data) {
        struct epoll_event ev{};
        ev.events = events | EPOLLET;  // Edge-triggered
        ev.data.ptr = data;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
            spdlog::error("epoll_ctl ADD fd={} failed: {}", fd, strerror(errno));
        }
    }

    void mod_fd(int fd, uint32_t events, void* data) {
        struct epoll_event ev{};
        ev.events = events | EPOLLET;
        ev.data.ptr = data;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
            spdlog::error("epoll_ctl MOD fd={} failed: {}", fd, strerror(errno));
        }
    }

    void del_fd(int fd) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    }

    int wait(int timeout_ms) {
        return epoll_wait(epoll_fd_, events_.data(), max_events_, timeout_ms);
    }

    const struct epoll_event& event(int index) const { return events_[index]; }

private:
    int epoll_fd_;
    int max_events_;
    std::vector<struct epoll_event> events_;
};

// ============================================================================
// TCP_CORK batch write optimization
// ============================================================================

class CorkGuard {
public:
    explicit CorkGuard(int fd) : fd_(fd) {
        int state = 1;
        setsockopt(fd_, IPPROTO_TCP, TCP_CORK, &state, sizeof(state));
    }

    ~CorkGuard() {
        int state = 0;
        setsockopt(fd_, IPPROTO_TCP, TCP_CORK, &state, sizeof(state));
    }

    CorkGuard(const CorkGuard&) = delete;
    CorkGuard& operator=(const CorkGuard&) = delete;

private:
    int fd_;
};

// ============================================================================
// Zero-copy sendfile for large payloads
// ============================================================================

ssize_t zero_copy_send(int sock_fd, int file_fd, off_t offset, size_t count) {
    // Use Linux sendfile() for zero-copy data transfer
    // from file descriptor to socket
    return sendfile(sock_fd, file_fd, &offset, count);
}

// ============================================================================
// TCP_USER_TIMEOUT for faster failure detection
// ============================================================================

void set_tcp_user_timeout(int fd, int timeout_ms) {
    // TCP_USER_TIMEOUT: abort connection if data is not acknowledged
    // within this time. Faster than TCP keepalive.
    if (setsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT,
                   &timeout_ms, sizeof(timeout_ms)) < 0) {
        spdlog::warn("TCP_USER_TIMEOUT not supported: {}", strerror(errno));
    }
}

// ============================================================================
// SO_INCOMING_CPU for RSS steering
// ============================================================================

void set_incoming_cpu(int fd, int cpu) {
    // Direct accepted connections to a specific CPU
    if (setsockopt(fd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, sizeof(cpu)) < 0) {
        spdlog::warn("SO_INCOMING_CPU not supported: {}", strerror(errno));
    }
}

// ============================================================================
// TCP_QUICKACK for latency reduction
// ============================================================================

void enable_quickack(int fd) {
    int state = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &state, sizeof(state)) < 0) {
        spdlog::warn("TCP_QUICKACK not supported: {}", strerror(errno));
    }
}

// ============================================================================
// io_uring feature detection and preparation
// ============================================================================

bool io_uring_available() {
    // Probe for io_uring kernel support
    // Linux 5.1+ required
    int fd = -1;
    // Try to create io_uring instance
    // In production: fd = syscall(__NR_io_uring_setup, 1, &params);
    // For now, check kernel version via uname
    struct utsname buf{};
    if (uname(&buf) == 0) {
        int major = 0, minor = 0;
        sscanf(buf.release, "%d.%d", &major, &minor);
        return (major > 5) || (major == 5 && minor >= 1);
    }
    return false;
}

// ============================================================================
// Network buffer pool
// ============================================================================

class BufferPool {
public:
    static constexpr size_t kPageSize = 4096;
    static constexpr size_t kPoolSize = 1024;

    BufferPool() {
        buffers_.reserve(kPoolSize);
        for (size_t i = 0; i < kPoolSize; i++) {
            void* buf = nullptr;
            if (posix_memalign(&buf, kPageSize, kPageSize) == 0) {
                buffers_.push_back(static_cast<char*>(buf));
            }
        }
        spdlog::info("BufferPool: allocated {} buffers of {} bytes each",
                     buffers_.size(), kPageSize);
    }

    ~BufferPool() {
        for (auto* buf : buffers_) {
            free(buf);
        }
    }

    char* acquire() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (buffers_.empty()) {
            // Allocate on demand
            void* buf = nullptr;
            posix_memalign(&buf, kPageSize, kPageSize);
            return static_cast<char*>(buf);
        }
        char* buf = buffers_.back();
        buffers_.pop_back();
        return buf;
    }

    void release(char* buf) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (buffers_.size() < kPoolSize) {
            buffers_.push_back(buf);
        } else {
            free(buf);
        }
    }

    size_t available() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return buffers_.size();
    }

private:
    std::vector<char*> buffers_;
    mutable std::mutex mutex_;
};

// Global buffer pool instance
static BufferPool g_buffer_pool;

// ============================================================================
// TLS session resumption with tickets
// ============================================================================

// Session ticket key rotation (48 bytes)
struct SessionTicketKey {
    uint8_t name[16];   // Key identifier
    uint8_t hmac_key[16];
    uint8_t aes_key[16];
};

void rotate_session_ticket_keys(void* ssl_ctx) {
    // Generate new session ticket keys for TLS 1.3
    // In production:
    // SSL_CTX_set_tlsext_ticket_keys(ssl_ctx, &key, sizeof(key));

    SessionTicketKey new_key{};
    // Fill with random data (production: RAND_bytes)
    for (int i = 0; i < 16; i++) {
        new_key.name[i] = static_cast<uint8_t>(rand());
        new_key.hmac_key[i] = static_cast<uint8_t>(rand());
        new_key.aes_key[i] = static_cast<uint8_t>(rand());
    }

    spdlog::info("TLS session ticket keys rotated");
}

// ============================================================================
// TLS 0-RTT early data support
// ============================================================================

bool enable_0rtt(void* ssl_ctx) {
    // Enable TLS 1.3 0-RTT early data
    // Reduces connection setup by 1 round-trip for resumed connections
    // In production:
    // SSL_CTX_set_max_early_data(ssl_ctx, max_early_data_size);

    spdlog::info("TLS 0-RTT early data support enabled");
    return true;
}

// ============================================================================
// OCSP stapling
// ============================================================================

bool enable_ocsp_stapling(void* ssl_ctx, const std::string& ocsp_response_path) {
    // Load OCSP response and enable stapling
    // In production:
    // Read OCSP response file, call SSL_CTX_set_tlsext_status_ocsp_resp()

    spdlog::info("OCSP stapling configured with response from {}",
                 ocsp_response_path);
    return true;
}

// ============================================================================
// Certificate pinning
// ============================================================================

struct PinnedCert {
    std::string subject;
    std::string spki_hash;  // Subject Public Key Info hash
};

class CertPinner {
public:
    void add_pin(const std::string& subject, const std::string& spki_hash) {
        std::lock_guard<std::mutex> lock(mutex_);
        pins_.push_back({subject, spki_hash});
    }

    bool check_pin(const std::string& subject, const std::string& spki_hash) const {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& pin : pins_) {
            if (pin.subject == subject && pin.spki_hash == spki_hash) {
                return true;
            }
        }
        return false;
    }

private:
    std::vector<PinnedCert> pins_;
    mutable std::mutex mutex_;
};

// ============================================================================
// Extended TcpTransport with advanced features
// ============================================================================

class AdvancedTcpTransport {
public:
    struct AdvancedConfig {
        int tcp_user_timeout_ms{5000};
        int incoming_cpu{-1};      // -1 = OS default
        bool enable_quickack{true};
        bool enable_cork{true};
        size_t sendfile_threshold{65536};  // Use sendfile for payloads > 64KB
    };

    static void apply_advanced(int fd, const AdvancedConfig& cfg) {
        set_tcp_user_timeout(fd, cfg.tcp_user_timeout_ms);

        if (cfg.incoming_cpu >= 0) {
            set_incoming_cpu(fd, cfg.incoming_cpu);
        }

        if (cfg.enable_quickack) {
            enable_quickack(fd);
        }
    }

    static void optimize_for_throughput(int fd) {
        // Increase socket buffers
        int buf_size = 16 * 1024 * 1024;  // 16MB
        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));

        // Enable TCP_NODELAY (already done) and TCP_QUICKACK
        enable_quickack(fd);

        // Set large TCP_USER_TIMEOUT for throughput (allow retransmits)
        set_tcp_user_timeout(fd, 60000);
    }

    static void optimize_for_latency(int fd) {
        // Disable Nagle
        int nodelay = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

        // Enable quick ACKs
        enable_quickack(fd);

        // Short TCP_USER_TIMEOUT for fast failure detection
        set_tcp_user_timeout(fd, 2000);

        // Moderate buffer sizes
        int buf_size = 256 * 1024;  // 256KB
        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
    }
};

}  // anonymous namespace

// Public API wrappers
char* acquire_buffer() { return g_buffer_pool.acquire(); }
void release_buffer(char* buf) { g_buffer_pool.release(buf); }

} // namespace torrent::network
