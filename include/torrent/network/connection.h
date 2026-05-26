#pragma once

/**
 * connection.h — Connection: Async I/O, Framing, SASL/TLS, Dispatch
 *
 * A Connection wraps a Transport (TCP or TLS) and implements the
 * torrent-mq wire protocol layer: 4-byte length-prefix framing,
 * SASL/TLS handshake, request dispatching via registered callbacks,
 * ordered response queuing, token-bucket throttling, idle-timeout
 * detection, graceful drain, and live per-connection metrics.
 *
 * Lifecycle:  handshaking → active → draining → closing → closed
 *
 * The Connection is driven by an external I/O event loop via
 * on_readable(), on_writable(), and on_timer().
 */

#include <cstdint>
#include <memory>
#include <mutex>
#include <atomic>
#include <chrono>
#include <string>
#include <functional>
#include <queue>
#include <condition_variable>
#include <vector>
#include <unordered_map>

// ---------------------------------------------------------------------------
// Forward declarations from sibling headers
// ---------------------------------------------------------------------------

namespace torrent {
    struct endpoint;
    struct buffer_view;
    class  shared_buffer;
    using  broker_id_t = int32_t;
    enum class error_code : int16_t;
    using  timestamp_ms_t = int64_t;
}

namespace torrent::protocol {
    struct RequestHeader;
    struct ResponseHeader;
}

namespace torrent::network {

class Transport;
class TcpTransport;
class TlsTransport;
class TransportFactory;

// ============================================================================
// ConnectionMetrics — per-connection atomic counters
// ============================================================================

struct ConnectionMetrics {
    uint64_t connection_id = 0;
    std::chrono::steady_clock::time_point created_at{};

    std::atomic<uint64_t> bytes_received{0};
    std::atomic<uint64_t> bytes_sent{0};
    std::atomic<uint64_t> requests_received{0};
    std::atomic<uint64_t> responses_sent{0};
    std::atomic<uint64_t> framing_errors{0};
    std::atomic<uint64_t> dispatch_errors{0};
    std::atomic<uint64_t> sasl_failures{0};
    std::atomic<int64_t>  avg_latency_us{0};
    std::atomic<int64_t>  peak_latency_us{0};
    std::atomic<uint32_t> inflight_requests{0};
    std::atomic<torrent::timestamp_ms_t> last_read_at_ms{0};
    std::atomic<torrent::timestamp_ms_t> last_write_at_ms{0};

    void reset_latency() noexcept {
        avg_latency_us.store(0);
        peak_latency_us.store(0);
    }

    [[nodiscard]] double uptime_seconds() const noexcept {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration<double>(now - created_at).count();
    }

    [[nodiscard]] double recv_rate_bps() const noexcept {
        double sec = uptime_seconds();
        return sec > 0.0 ? static_cast<double>(bytes_received.load()) / sec : 0.0;
    }

    [[nodiscard]] double send_rate_bps() const noexcept {
        double sec = uptime_seconds();
        return sec > 0.0 ? static_cast<double>(bytes_sent.load()) / sec : 0.0;
    }
};

// ============================================================================
// ConnectionConfig — protocol-layer per-connection tunables
// ============================================================================

struct ConnectionConfig {
    /// Maximum request payload size (bytes).  Larger requests get kMessageTooLarge.
    uint32_t max_request_size = 100 * 1024 * 1024;   // 100 MiB
    /// Internal read-buffer capacity for partial-frame reassembly.
    uint32_t read_buffer_size = 65536;                // 64 KiB
    /// Idle timeout: close if no bytes received for this duration (0 = disabled).
    std::chrono::milliseconds idle_timeout_ms{60000}; // 60 s
    /// Max requests dispatched but not yet responded to (backpressure limit).
    uint32_t max_inflight_requests = 1024;
    /// Soft read throughput limit, bytes/sec (0 = unlimited).
    uint64_t read_throttle_bps = 0;
    /// Soft write throughput limit, bytes/sec (0 = unlimited).
    uint64_t write_throttle_bps = 0;
    /// Token-bucket burst allowance in bytes.
    uint64_t throttle_burst_bytes = 65536;            // 64 KiB
    /// Require SASL before allowing any API request beyond handshake/auth/versions.
    bool require_sasl = false;
    /// SASL mechanisms accepted by the server (empty = all available).
    std::vector<std::string> sasl_mechanisms;
    /// Promote plain TCP to TLS after accept (STARTTLS-style upgrade).
    bool tls_upgrade = false;
    /// Remote broker cluster id (kNoBroker for client connections).
    torrent::broker_id_t peer_broker_id = -2;        // kNoBroker
    /// True for broker-to-broker connections.
    bool is_inter_broker = false;
};

// ============================================================================
// ConnectionState — connection lifecycle state machine
// ============================================================================

enum class ConnectionState : uint8_t {
    handshaking = 0,  ///< TLS and/or SASL in progress.
    active      = 1,  ///< Fully established; dispatching requests.
    draining    = 2,  ///< Graceful drain: finishing in-flight, rejecting new.
    closing     = 3,  ///< Transport close in progress.
    closed      = 4,  ///< Transport released; connection dead.
};

[[nodiscard]] constexpr const char* to_string(ConnectionState s) noexcept {
    switch (s) {
    case ConnectionState::handshaking: return "HANDSHAKING";
    case ConnectionState::active:      return "ACTIVE";
    case ConnectionState::draining:    return "DRAINING";
    case ConnectionState::closing:     return "CLOSING";
    case ConnectionState::closed:      return "CLOSED";
    }
    return "UNKNOWN";
}

// ============================================================================
// SaslState — fine-grained SASL sub-state within handshaking
// ============================================================================

enum class SaslState : uint8_t {
    not_started     = 0,  ///< SASL not yet begun.
    handshake_sent  = 1,  ///< Client sent SaslHandshake; awaiting response.
    mechanism_picked = 2, ///< Mechanism selected; ready for token exchange.
    authenticating  = 3,  ///< SaslAuthenticate token exchange in progress.
    authenticated   = 4,  ///< SASL succeeded.
    failed          = 5,  ///< Authentication failed; connection will close.
};

// ============================================================================
// ResponseEnvelope — a framed response queued for writing
// ============================================================================

struct ResponseEnvelope {
    int32_t correlation_id = 0;
    std::unique_ptr<torrent::shared_buffer> payload; // serialised body, no length prefix

    ResponseEnvelope() = default;
    ResponseEnvelope(int32_t corr_id, std::unique_ptr<torrent::shared_buffer> pld)
        : correlation_id(corr_id), payload(std::move(pld)) {}
};

// ============================================================================
// RequestHandler — callback for dispatched requests
// ============================================================================

/// Handler invoked when a complete request is parsed.  body is valid only
/// for the duration of the call; handlers must copy data they need beyond
/// the return.  The handler must enqueue its response via
/// Connection::enqueue_response() before returning.
using RequestHandler = std::function<void(
    const torrent::protocol::RequestHeader& header,
    torrent::buffer_view body,
    uint64_t conn_id)>;

// ============================================================================
// Connection — async protocol layer over Transport
// ============================================================================

/**
 * Owns a Transport (TCP or TLS) and drives the full protocol lifecycle:
 * handshake → dispatch loop → drain → close.
 *
 * External I/O event loop drives this object via three entry points:
 *   on_readable()  — socket is ready for read
 *   on_writable()  — socket is ready for write
 *   on_timer()     — periodic housekeeping (idle timeout, drain completion)
 */
class Connection {
public:
    /// Construct over an already-connected transport.
    /// transport_factory is non-owning; may be nullptr if TLS is never used.
    Connection(std::unique_ptr<Transport> transport,
               const ConnectionConfig& cfg,
               TransportFactory* transport_factory,
               uint64_t conn_id);

    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    Connection(Connection&&) = delete;
    Connection& operator=(Connection&&) = delete;

    // ------------------------------------------------------------------
    // Event-loop interface
    // ------------------------------------------------------------------

    /// Read available bytes, reassemble frames, dispatch complete requests.
    /// During handshaking, drives TLS and/or SASL exchange.
    void on_readable();

    /// Drain the response queue, writing as many bytes as the socket
    /// can accept without blocking.  Respects write throttling.
    void on_writable();

    /// Periodic housekeeping: idle-timeout detection, SASL timeout,
    /// drain→close transition when inflight reaches zero.
    void on_timer(torrent::timestamp_ms_t now_ms);

    // ------------------------------------------------------------------
    // Handler registration (thread-safe)
    // ------------------------------------------------------------------

    /// Register a handler for an API key.  Only one handler per key.
    void register_handler(int16_t api_key, RequestHandler handler);
    /// Remove the handler for an API key.
    void unregister_handler(int16_t api_key);

    // ------------------------------------------------------------------
    // Response delivery (thread-safe)
    // ------------------------------------------------------------------

    /// Enqueue a response for async delivery.  Payload is the serialised
    /// body (header + data) without the 4-byte length prefix.
    void enqueue_response(ResponseEnvelope envelope);

    /// Convenience: enqueue an error response for a correlation id.
    void enqueue_error_response(int32_t correlation_id,
                                torrent::error_code ec,
                                const std::string& message);

    // ------------------------------------------------------------------
    // Lifecycle control
    // ------------------------------------------------------------------

    /// Begin graceful drain: stop accepting new requests, allow in-flight
    /// to finish, then close.  Idempotent.
    void drain();

    /// Force-close immediately: release transport, discard queues.
    void close();

    // ------------------------------------------------------------------
    // Queries
    // ------------------------------------------------------------------

    [[nodiscard]] ConnectionState state() const noexcept {
        return state_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool is_active() const noexcept {
        return state_.load(std::memory_order_acquire) == ConnectionState::active;
    }
    [[nodiscard]] bool is_closed() const noexcept {
        return state_.load(std::memory_order_acquire) == ConnectionState::closed;
    }
    [[nodiscard]] uint64_t connection_id() const noexcept { return conn_id_; }
    [[nodiscard]] int fd() const noexcept;
    [[nodiscard]] torrent::endpoint peer() const noexcept;
    [[nodiscard]] torrent::endpoint local() const noexcept;
    [[nodiscard]] const ConnectionMetrics& metrics() const noexcept { return metrics_; }
    [[nodiscard]] torrent::broker_id_t peer_broker_id() const noexcept { return config_.peer_broker_id; }
    [[nodiscard]] bool is_inter_broker() const noexcept { return config_.is_inter_broker; }

    // ------------------------------------------------------------------
    // SASL state
    // ------------------------------------------------------------------

    [[nodiscard]] SaslState sasl_state() const noexcept {
        return sasl_state_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool is_authenticated() const noexcept {
        return sasl_state_.load(std::memory_order_acquire) == SaslState::authenticated;
    }
    [[nodiscard]] const std::string& sasl_mechanism() const noexcept { return sasl_mechanism_; }
    [[nodiscard]] const std::string& principal() const noexcept { return principal_; }

    // ------------------------------------------------------------------
    // Internal — called by SASL handshake logic within on_readable
    // ------------------------------------------------------------------

    void sasl_complete(const std::string& mechanism,
                       const std::string& authenticated_principal);
    void sasl_fail(const std::string& reason);
    void start_sasl_server();

private:
    // ---- I/O internals ----

    /// Feed raw bytes into the frame parser.  Returns true if at least
    /// one complete frame was dispatched.
    bool feed_parser(const char* data, size_t len);

    /// Handle a complete request frame (header already parsed).
    void dispatch_request(const torrent::protocol::RequestHeader& header,
                          const char* body, size_t body_len);

    /// Try to write as many queued responses as possible non-blocking.
    /// Returns bytes written.
    size_t flush_response_queue();

    void set_state(ConnectionState new_state);

    /// Perform one TLS handshake step; returns 1=done, 0=in-progress, -1=error.
    int tls_handshake_step();

    /// Process a SASL-related request during the handshaking phase.
    void handle_sasl_request(const torrent::protocol::RequestHeader& header,
                             const char* body, size_t body_len);

    // ---- Throttling ----

    /// Token-bucket read check: returns true if reading is allowed now.
    bool throttle_read(size_t bytes);

    /// Token-bucket write check: returns true if writing is allowed now.
    bool throttle_write(size_t bytes);

    /// Refill token buckets based on elapsed time.
    void refill_tokens();

    // ---- Timeout ----

    bool idle_timeout_exceeded(torrent::timestamp_ms_t now_ms) const;

    // ---- Data members ----

    std::unique_ptr<Transport> transport_;
    TransportFactory* transport_factory_ = nullptr;
    ConnectionConfig config_;
    uint64_t conn_id_ = 0;

    std::atomic<ConnectionState> state_{ConnectionState::handshaking};
    std::atomic<SaslState> sasl_state_{SaslState::not_started};
    std::string sasl_mechanism_;
    std::string principal_;

    // ---- Read-side buffers ----

    std::vector<char> read_buffer_;
    size_t read_buffer_pos_ = 0;
    uint32_t expected_frame_size_ = 0;  // 0 = waiting for 4-byte length prefix

    // ---- Response queue ----

    mutable std::mutex response_mutex_;
    std::queue<ResponseEnvelope> response_queue_;
    size_t response_write_offset_ = 0;
    uint8_t response_prefix_written_ = 0; // bytes of length prefix already written
    std::condition_variable response_cv_; // signals when queue becomes non-empty

    // ---- Handler registry ----

    mutable std::mutex handler_mutex_;
    std::unordered_map<int16_t, RequestHandler> handlers_;

    // ---- Token-bucket throttling ----

    std::atomic<uint64_t> read_tokens_{0};
    std::atomic<uint64_t> write_tokens_{0};
    std::atomic<torrent::timestamp_ms_t> read_token_ts_{0};
    std::atomic<torrent::timestamp_ms_t> write_token_ts_{0};

    // ---- Metrics ----

    ConnectionMetrics metrics_;
    mutable std::mutex metrics_mutex_;
};

// ============================================================================
// Inline implementations
// ============================================================================

inline Connection::Connection(std::unique_ptr<Transport> transport,
                              const ConnectionConfig& cfg,
                              TransportFactory* transport_factory,
                              uint64_t conn_id)
    : transport_(std::move(transport))
    , transport_factory_(transport_factory)
    , config_(cfg)
    , conn_id_(conn_id)
{
    read_buffer_.reserve(cfg.read_buffer_size);
    metrics_.connection_id = conn_id;
    metrics_.created_at = std::chrono::steady_clock::now();
    read_tokens_.store(cfg.throttle_burst_bytes, std::memory_order_relaxed);
    write_tokens_.store(cfg.throttle_burst_bytes, std::memory_order_relaxed);
}

inline Connection::~Connection() {
    close();
}

inline void Connection::close() {
    if (state_.load(std::memory_order_acquire) == ConnectionState::closed) return;
    set_state(ConnectionState::closing);
    if (transport_) {
        transport_->close();
        transport_.reset();
    }
    set_state(ConnectionState::closed);
}

inline int Connection::fd() const noexcept {
    return transport_ ? transport_->fd() : -1;
}

inline torrent::endpoint Connection::peer() const noexcept {
    return transport_ ? transport_->peer() : torrent::endpoint{};
}

inline torrent::endpoint Connection::local() const noexcept {
    return transport_ ? transport_->local() : torrent::endpoint{};
}

inline void Connection::set_state(ConnectionState new_state) {
    state_.store(new_state, std::memory_order_release);
}

inline void Connection::sasl_complete(const std::string& mechanism,
                                       const std::string& authenticated_principal) {
    sasl_mechanism_ = mechanism;
    principal_ = authenticated_principal;
    sasl_state_.store(SaslState::authenticated, std::memory_order_release);
    metrics_.last_read_at_ms.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count(),
        std::memory_order_relaxed);
    if (state_.load(std::memory_order_acquire) == ConnectionState::handshaking) {
        set_state(ConnectionState::active);
    }
}

inline void Connection::sasl_fail(const std::string& /*reason*/) {
    metrics_.sasl_failures.fetch_add(1, std::memory_order_relaxed);
    sasl_state_.store(SaslState::failed, std::memory_order_release);
    close();
}

inline bool Connection::idle_timeout_exceeded(torrent::timestamp_ms_t now_ms) const {
    if (config_.idle_timeout_ms.count() == 0) return false;
    auto last = metrics_.last_read_at_ms.load(std::memory_order_relaxed);
    if (last == 0) return false;
    return (now_ms - last) > config_.idle_timeout_ms.count();
}

} // namespace torrent::network
