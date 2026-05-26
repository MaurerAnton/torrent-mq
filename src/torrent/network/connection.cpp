/**
 * connection.cpp — Connection: Async I/O, Framing, SASL/TLS, Dispatch
 *
 * Implements the full Connection lifecycle over a Transport (TCP or TLS):
 *   - 4-byte big-endian length-prefix frame parsing
 *   - Request dispatch via registered per-api_key callbacks
 *   - Thread-safe ordered response queue with partial-write handling
 *   - SASL server handshake (SaslHandshake → SaslAuthenticate token exchange)
 *   - TLS upgrade via TransportFactory::wrap_tls (STARTTLS-style)
 *   - Token-bucket read/write throttling with periodic refill
 *   - Idle-timeout detection and graceful drain
 *   - Per-connection metrics (atomic counters)
 *
 * The Connection is driven by an external I/O event loop through three
 * entry points: on_readable(), on_writable(), and on_timer().
 *
 * Lifecycle:  handshaking → active → draining → closing → closed
 *
 * Thread safety:
 *   - I/O methods (on_readable / on_writable / on_timer) must be called
 *     from the event-loop thread only.
 *   - enqueue_response() and register_handler() are safe from any thread.
 *   - Metrics are atomic — safe to read from any thread.
 */

// ---------------------------------------------------------------------------
// Project headers
// ---------------------------------------------------------------------------

#include "torrent/network/connection.h"
#include "torrent/network/transport.h"
#include "torrent/network/protocol.h"
#include "torrent/network/sasl_handshake.h"
#include "torrent/network/message_codec.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

// ---------------------------------------------------------------------------
// System headers
// ---------------------------------------------------------------------------

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <chrono>
#include <limits>

// OpenSSL error handling (needed by TLS handshake err-logging)
#include <openssl/err.h>

namespace torrent::network {

// ============================================================================
// Internal constants
// ============================================================================

/// Maximum number of bytes to read from the transport in one on_readable call.
inline constexpr size_t kMaxReadChunk = 65536;   // 64 KiB

/// Maximum number of queued responses before write-side backpressure kicks in.
inline constexpr size_t kMaxResponseQueueDepth = 4096;

/// Minimum tick interval for token-bucket refill (ms).
inline constexpr torrent::timestamp_ms_t kTokenRefillIntervalMs = 100;

/// Maximum SASL handshake duration before timeout.
inline constexpr torrent::timestamp_ms_t kSaslTimeoutMs = 30000;   // 30 s

/// Client ID placeholder for internally generated error responses.
inline constexpr const char* kInternalClientId = "torrent-mq-internal";

/// Default API version for internally generated responses.
inline constexpr int16_t kInternalApiVersion = 0;

// ============================================================================
// Wire-format helpers (local, not exposed in header)
// ============================================================================

namespace {

/// Read a big-endian int32 from a byte buffer.
inline int32_t read_int32_be(const char* p) noexcept {
    uint32_t v = static_cast<uint32_t>(static_cast<unsigned char>(p[0])) << 24
               | static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 16
               | static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 8
               | static_cast<uint32_t>(static_cast<unsigned char>(p[3]));
    return static_cast<int32_t>(v);
}

/// Write a big-endian int32 into a byte buffer.
inline void write_int32_be(char* p, int32_t v) noexcept {
    uint32_t u = static_cast<uint32_t>(v);
    p[0] = static_cast<char>((u >> 24) & 0xFF);
    p[1] = static_cast<char>((u >> 16) & 0xFF);
    p[2] = static_cast<char>((u >> 8) & 0xFF);
    p[3] = static_cast<char>(u & 0xFF);
}

/// Read a big-endian int16 from a byte buffer.
inline int16_t read_int16_be(const char* p) noexcept {
    uint16_t v = static_cast<uint16_t>(static_cast<unsigned char>(p[0])) << 8
               | static_cast<uint16_t>(static_cast<unsigned char>(p[1]));
    return static_cast<int16_t>(v);
}

/// Write a big-endian int16 into a byte buffer.
inline void write_int16_be(char* p, int16_t v) noexcept {
    uint16_t u = static_cast<uint16_t>(v);
    p[0] = static_cast<char>((u >> 8) & 0xFF);
    p[1] = static_cast<char>(u & 0xFF);
}

/// Read a compact string (int16 length prefix followed by UTF-8 data) from a
/// wire-format buffer.  Returns the number of bytes consumed, or -1 on error.
inline int read_string(const char* data, size_t max_len, std::string& out) noexcept {
    if (max_len < 2) return -1;
    int16_t len = read_int16_be(data);
    if (len < -1) return -1;           // invalid length
    if (len == -1) {                    // null string
        out.clear();
        return 2;
    }
    size_t ulen = static_cast<size_t>(len);
    if (max_len < 2 + ulen) return -1;
    out.assign(data + 2, ulen);
    return 2 + static_cast<int>(ulen);
}

/// Write a compact string (int16 length prefix + data).
inline void write_string(std::vector<char>& buf, const std::string& s) {
    int16_t len = static_cast<int16_t>(s.size());
    buf.resize(buf.size() + 2 + s.size());
    char* p = &buf[buf.size() - 2 - s.size()];
    write_int16_be(p, len);
    if (!s.empty()) std::memcpy(p + 2, s.data(), s.size());
}

/// Parse a request header from raw bytes.  Returns bytes consumed, or 0 if
/// more data is needed, or negative on parse failure.
///
/// Wire format (Kafka v2 header, no tagged fields):
///   [4]  total frame length (not included in count below — handled by caller)
///   [2]  api_key        (int16, big-endian)
///   [2]  api_version    (int16, big-endian)
///   [4]  correlation_id (int32, big-endian)
///   [2+]{n} client_id   (nullable string)
///   ...  body follows
int parse_request_header(const char* data, size_t len,
                         torrent::protocol::RequestHeader& header) {
    // Minimum: api_key(2) + api_version(2) + correlation_id(4) + client_id_len(2) = 10
    if (len < 10) return 0;

    int offset = 0;
    header.api_key        = read_int16_be(data + offset); offset += 2;
    header.api_version    = read_int16_be(data + offset); offset += 2;
    header.correlation_id = read_int32_be(data + offset); offset += 4;

    int string_bytes = read_string(data + offset, len - offset, header.client_id);
    if (string_bytes < 0) return -1;
    offset += string_bytes;

    return offset;
}

/// Encode a response into a wire-format frame (4-byte length prefix + body).
/// Returns the complete framed buffer.
std::vector<char> encode_response_frame(int32_t correlation_id,
                                         const char* body, size_t body_len) {
    // Response header: correlation_id (4 bytes)
    static constexpr size_t kResponseHeaderSize = 4;
    size_t total_len = kResponseHeaderSize + body_len;   // payload (does not include length prefix)
    size_t frame_size = torrent::protocol::kFrameLengthSize + total_len;

    std::vector<char> frame(frame_size);
    char* p = frame.data();

    // Write 4-byte length prefix (total payload length, big-endian)
    write_int32_be(p, static_cast<int32_t>(total_len));
    p += torrent::protocol::kFrameLengthSize;

    // Write response header: correlation_id
    write_int32_be(p, correlation_id);
    p += 4;

    // Write body
    if (body_len > 0) {
        std::memcpy(p, body, body_len);
    }

    return frame;
}

/// Get current time in milliseconds since epoch (system clock).
torrent::timestamp_ms_t now_ms() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

/// Classify an errno value into transport-retryable vs fatal.
bool is_retryable_errno(int e) noexcept {
    return e == EAGAIN || e == EWOULDBLOCK || e == EINTR;
}

} // anonymous namespace

// ============================================================================
// Connection — Public API (non-inline methods)
// ============================================================================

// --------------------------------------------------------------------------
// Event-loop interface
// --------------------------------------------------------------------------

void Connection::on_readable() {
    ConnectionState current = state_.load(std::memory_order_acquire);

    // Guard: only process reads in handshaking or active states.
    if (current != ConnectionState::handshaking && current != ConnectionState::active) {
        return;
    }

    // If we are in draining state, we can still read responses (e.g., for
    // requests that were already dispatched), but we won't dispatch new ones.
    // Draining is handled inside dispatch_request.

    if (!transport_ || !transport_->connected()) {
        spdlog::debug("[conn:{}] on_readable called but transport not connected", conn_id_);
        return;
    }

    // ---- TLS handshake (if applicable) ----
    if (current == ConnectionState::handshaking) {
        // Check if a TLS upgrade is pending (STARTTLS).
        if (config_.tls_upgrade && !transport_->is_tls()) {
            // We need to perform TLS handshake.
            // First, check if we have a TLS transport wrapper already.
            // The upgrade happens externally via start_sasl_server() or
            // the caller performing the upgrade.  Here we just handle
            // the TLS handshake if transport is already TLS.
        }

        // If the transport is TLS and handshake isn't done, drive it.
        if (transport_->is_tls()) {
            int rc = tls_handshake_step();
            if (rc < 0) {
                spdlog::error("[conn:{}] TLS handshake failed, closing", conn_id_);
                metrics_.sasl_failures.fetch_add(1, std::memory_order_relaxed);
                close();
                return;
            }
            if (rc == 0) {
                // Handshake still in progress; no application data to read yet.
                return;
            }
            // rc == 1: handshake complete
            spdlog::info("[conn:{}] TLS handshake complete", conn_id_);
        }

        // If SASL is not required and TLS is done (or no TLS), transition to active.
        if (!config_.require_sasl) {
            set_state(ConnectionState::active);
            spdlog::debug("[conn:{}] No SASL required, transitioning to active", conn_id_);
        }
    }

    // Re-read state after potential handshake transitions.
    current = state_.load(std::memory_order_acquire);

    // ---- Read application data ----
    // Ensure read buffer capacity.
    if (read_buffer_.capacity() < config_.read_buffer_size) {
        read_buffer_.reserve(config_.read_buffer_size);
    }
    if (read_buffer_.size() < config_.read_buffer_size) {
        read_buffer_.resize(config_.read_buffer_size);
    }

    // Read from transport into our buffer.
    // For simplicity we read into a stack buffer, then feed to parser.
    char read_chunk[kMaxReadChunk];
    ssize_t total_read = 0;

    while (true) {
        // Throttle: check if we're allowed to read more.
        if (config_.read_throttle_bps > 0) {
            if (!throttle_read(1)) {
                // No tokens available; stop reading and wait for next timer tick.
                break;
            }
        }

        size_t to_read = std::min(kMaxReadChunk,
                                  static_cast<size_t>(config_.read_throttle_bps > 0
                                                      ? read_tokens_.load(std::memory_order_relaxed)
                                                      : kMaxReadChunk));
        if (to_read == 0) break;

        ssize_t n = transport_->read(read_chunk, to_read);

        if (n > 0) {
            // Update bytes-received metric.
            metrics_.bytes_received.fetch_add(static_cast<uint64_t>(n),
                                              std::memory_order_relaxed);

            // Consume read tokens.
            if (config_.read_throttle_bps > 0) {
                throttle_read(static_cast<size_t>(n));
            }

            // Feed to parser.
            if (!feed_parser(read_chunk, static_cast<size_t>(n))) {
                spdlog::warn("[conn:{}] Frame parser error, closing", conn_id_);
                metrics_.framing_errors.fetch_add(1, std::memory_order_relaxed);
                close();
                return;
            }

            total_read += n;

            // Update last-read timestamp.
            metrics_.last_read_at_ms.store(now_ms(), std::memory_order_relaxed);
        } else if (n == 0) {
            // Clean remote shutdown.
            spdlog::debug("[conn:{}] Remote peer closed connection (EOF)", conn_id_);
            close();
            return;
        } else {
            // n < 0: error
            int err = errno;
            if (is_retryable_errno(err)) {
                // No more data available right now.
                break;
            }
            spdlog::warn("[conn:{}] Read error: {} (errno={}), closing",
                         conn_id_, std::strerror(err), err);
            close();
            return;
        }

        // Avoid starving the event loop: read up to 16 chunks per tick.
        if (total_read >= static_cast<ssize_t>(kMaxReadChunk * 16)) break;
    }

    if (total_read > 0) {
        spdlog::trace("[conn:{}] Read {} bytes, state={}", conn_id_, total_read,
                      to_string(state_.load(std::memory_order_acquire)));
    }
}

void Connection::on_writable() {
    ConnectionState current = state_.load(std::memory_order_acquire);

    // Guard: only write in handshaking, active, or draining states.
    if (current != ConnectionState::handshaking &&
        current != ConnectionState::active &&
        current != ConnectionState::draining) {
        return;
    }

    if (!transport_ || !transport_->connected()) {
        // Transport not ready.
        return;
    }

    // ---- Flush queued responses ----
    size_t written = flush_response_queue();

    if (written > 0) {
        metrics_.last_write_at_ms.store(now_ms(), std::memory_order_relaxed);
        spdlog::trace("[conn:{}] Wrote {} bytes to transport", conn_id_, written);
    }
}

void Connection::on_timer(torrent::timestamp_ms_t now_ms) {
    ConnectionState current = state_.load(std::memory_order_acquire);

    // ---- Refill token buckets ----
    if (config_.read_throttle_bps > 0 || config_.write_throttle_bps > 0) {
        refill_tokens();
    }

    // ---- Idle timeout check ----
    if (current == ConnectionState::active || current == ConnectionState::handshaking) {
        if (idle_timeout_exceeded(now_ms)) {
            spdlog::info("[conn:{}] Idle timeout exceeded ({}ms since last read), closing",
                         conn_id_,
                         now_ms - metrics_.last_read_at_ms.load(std::memory_order_relaxed));
            close();
            return;
        }
    }

    // ---- SASL timeout during handshake ----
    if (current == ConnectionState::handshaking &&
        sasl_state_.load(std::memory_order_acquire) != SaslState::not_started &&
        sasl_state_.load(std::memory_order_acquire) != SaslState::authenticated &&
        sasl_state_.load(std::memory_order_acquire) != SaslState::failed) {
        auto last = metrics_.last_read_at_ms.load(std::memory_order_relaxed);
        if (last > 0 && (now_ms - last) > kSaslTimeoutMs) {
            spdlog::warn("[conn:{}] SASL handshake timed out after {}ms",
                         conn_id_, now_ms - last);
            metrics_.sasl_failures.fetch_add(1, std::memory_order_relaxed);
            sasl_state_.store(SaslState::failed, std::memory_order_release);
            close();
            return;
        }
    }

    // ---- Drain → close transition ----
    if (current == ConnectionState::draining) {
        // Check if all inflight requests have been responded to.
        uint32_t inflight = metrics_.inflight_requests.load(std::memory_order_relaxed);
        bool queue_empty = false;
        {
            std::lock_guard<std::mutex> lock(response_mutex_);
            queue_empty = response_queue_.empty();
        }
        if (inflight == 0 && queue_empty) {
            spdlog::info("[conn:{}] Drain complete (inflight=0, queue empty), closing",
                         conn_id_);
            close();
            return;
        }
    }

    // ---- Closing state: ensure transport is released ----
    if (current == ConnectionState::closing) {
        if (transport_) {
            transport_->shutdown(SHUT_RDWR);
        }
        // Transition to closed will happen when close() is called again.
    }
}

// --------------------------------------------------------------------------
// Handler registration (thread-safe)
// --------------------------------------------------------------------------

void Connection::register_handler(int16_t api_key, RequestHandler handler) {
    std::lock_guard<std::mutex> lock(handler_mutex_);
    if (handler) {
        handlers_[api_key] = std::move(handler);
        spdlog::debug("[conn:{}] Registered handler for api_key={} ({})",
                      conn_id_, api_key, torrent::protocol::api_key_name(api_key));
    } else {
        handlers_.erase(api_key);
        spdlog::debug("[conn:{}] Unregistered handler for api_key={}", conn_id_, api_key);
    }
}

void Connection::unregister_handler(int16_t api_key) {
    std::lock_guard<std::mutex> lock(handler_mutex_);
    handlers_.erase(api_key);
    spdlog::debug("[conn:{}] Unregistered handler for api_key={}", conn_id_, api_key);
}

// --------------------------------------------------------------------------
// Response delivery (thread-safe)
// --------------------------------------------------------------------------

void Connection::enqueue_response(ResponseEnvelope envelope) {
    ConnectionState current = state_.load(std::memory_order_acquire);

    // If we're draining, only allow if inflight > 0 (finishing in-flight).
    if (current == ConnectionState::draining) {
        uint32_t inflight = metrics_.inflight_requests.load(std::memory_order_relaxed);
        if (inflight == 0) {
            spdlog::debug("[conn:{}] Dropping response corr_id={}: connection draining complete",
                          conn_id_, envelope.correlation_id);
            return;
        }
    }

    // If closing or closed, drop.
    if (current == ConnectionState::closing || current == ConnectionState::closed) {
        spdlog::debug("[conn:{}] Dropping response corr_id={}: connection is {}",
                      conn_id_, envelope.correlation_id, to_string(current));
        return;
    }

    {
        std::lock_guard<std::mutex> lock(response_mutex_);

        // Backpressure: drop if queue is excessively deep.
        if (response_queue_.size() >= kMaxResponseQueueDepth) {
            spdlog::warn("[conn:{}] Response queue full ({} entries), dropping corr_id={}",
                         conn_id_, response_queue_.size(), envelope.correlation_id);
            metrics_.dispatch_errors.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        response_queue_.push(std::move(envelope));
    }

    // Decrement inflight count (this response corresponds to a dispatched request).
    // Note: if inflight was already 0, don't underflow.
    uint32_t prev = metrics_.inflight_requests.load(std::memory_order_relaxed);
    if (prev > 0) {
        metrics_.inflight_requests.fetch_sub(1, std::memory_order_relaxed);
    }

    // Update metrics.
    metrics_.responses_sent.fetch_add(1, std::memory_order_relaxed);

    // Signal the condition variable (for any synchronous waiters).
    response_cv_.notify_one();
}

void Connection::enqueue_error_response(int32_t correlation_id,
                                         torrent::error_code ec,
                                         const std::string& message) {
    // Build a minimal error response body.
    // Wire format: error_code (int16) + error_message (nullable string).
    // For simplicity we encode a compact error response.
    std::vector<char> body;
    body.reserve(2 + 2 + message.size());

    // error_code (2 bytes, big-endian)
    body.resize(2);
    write_int16_be(body.data(), static_cast<int16_t>(ec));

    // error_message (nullable string: 2-byte length + data)
    write_string(body, message);

    // Enqueue the error.
    auto payload = std::make_unique<torrent::shared_buffer>(body.data(), body.size());
    enqueue_response(ResponseEnvelope(correlation_id, std::move(payload)));

    spdlog::debug("[conn:{}] Enqueued error response corr_id={}: {} ({})",
                  conn_id_, correlation_id,
                  torrent::error_code_name(ec), message);
}

// --------------------------------------------------------------------------
// Lifecycle control
// --------------------------------------------------------------------------

void Connection::drain() {
    ConnectionState expected = ConnectionState::active;
    if (state_.compare_exchange_strong(expected, ConnectionState::draining,
                                       std::memory_order_acq_rel,
                                       std::memory_order_relaxed)) {
        spdlog::info("[conn:{}] Entering drain mode (inflight={})",
                     conn_id_,
                     metrics_.inflight_requests.load(std::memory_order_relaxed));
    } else if (expected == ConnectionState::handshaking) {
        // If still handshaking, just close immediately.
        spdlog::debug("[conn:{}] Drain requested during handshake, closing", conn_id_);
        close();
    }
    // else: already draining, closing, or closed — idempotent.
}

// --------------------------------------------------------------------------
// SASL server
// --------------------------------------------------------------------------

void Connection::start_sasl_server() {
    SaslState expected = SaslState::not_started;
    if (!sasl_state_.compare_exchange_strong(expected, SaslState::handshake_sent,
                                             std::memory_order_acq_rel,
                                             std::memory_order_relaxed)) {
        spdlog::warn("[conn:{}] start_sasl_server called but SASL state is not not_started",
                     conn_id_);
        return;
    }

    spdlog::info("[conn:{}] Starting SASL server handshake", conn_id_);
    metrics_.last_read_at_ms.store(now_ms(), std::memory_order_relaxed);

    // The actual handshake is driven by the client sending a SaslHandshake
    // request.  We just mark ourselves as ready to receive it.
    // If we reach this point, the connection stays in handshaking state
    // until SASL completes successfully.
}

// ============================================================================
// Connection — Private methods
// ============================================================================

// --------------------------------------------------------------------------
// Frame parser
// --------------------------------------------------------------------------

bool Connection::feed_parser(const char* data, size_t len) {
    if (len == 0) return true;

    // Accumulate bytes into the read buffer.
    // We maintain read_buffer_pos_ as the fill position.
    // read_buffer_ is used as a circular/accumulation buffer for partial data.

    const char* cursor = data;
    const char* end = data + len;

    while (cursor < end) {
        size_t remaining = static_cast<size_t>(end - cursor);

        if (expected_frame_size_ == 0) {
            // ---- State: waiting for the 4-byte length prefix ----

            // How many bytes of the length prefix do we already have buffered?
            size_t have = read_buffer_pos_;
            size_t need = torrent::protocol::kFrameLengthSize;

            if (have + remaining < need) {
                // Not enough data yet — buffer what we have.
                if (read_buffer_.size() < have + remaining) {
                    read_buffer_.resize(have + remaining + 64);
                }
                std::memcpy(read_buffer_.data() + have, cursor, remaining);
                read_buffer_pos_ = have + remaining;
                return true;
            }

            // We can complete the length prefix.
            size_t take = need - have;
            if (read_buffer_.size() < need) {
                read_buffer_.resize(need);
            }
            if (take > 0) {
                std::memcpy(read_buffer_.data() + have, cursor, take);
                cursor += take;
            }

            // Parse the length prefix.
            int32_t frame_len = read_int32_be(read_buffer_.data());

            // Validate frame length.
            if (frame_len < 0) {
                spdlog::error("[conn:{}] Invalid frame length: {} (negative)",
                              conn_id_, frame_len);
                metrics_.framing_errors.fetch_add(1, std::memory_order_relaxed);
                return false;
            }

            if (frame_len == 0) {
                spdlog::warn("[conn:{}] Zero-length frame received, skipping", conn_id_);
                read_buffer_pos_ = 0;
                continue;
            }

            uint32_t uframe_len = static_cast<uint32_t>(frame_len);
            if (uframe_len > config_.max_request_size) {
                spdlog::error("[conn:{}] Frame too large: {} (max {})",
                              conn_id_, uframe_len, config_.max_request_size);
                metrics_.framing_errors.fetch_add(1, std::memory_order_relaxed);
                return false;
            }

            expected_frame_size_ = uframe_len;
            read_buffer_pos_ = 0;

            // Ensure buffer capacity for the full frame.
            if (read_buffer_.size() < uframe_len) {
                read_buffer_.resize(uframe_len + 64);
            }
        }

        // ---- State: accumulating frame payload ----
        size_t have = read_buffer_pos_;
        size_t need = expected_frame_size_;

        if (have + remaining < need) {
            // Need more data.
            std::memcpy(read_buffer_.data() + have, cursor, remaining);
            read_buffer_pos_ = have + remaining;
            return true;
        }

        // Complete the frame.
        size_t take = need - have;
        if (take > 0) {
            std::memcpy(read_buffer_.data() + have, cursor, take);
            cursor += take;
        }

        // ---- Process the complete frame ----
        const char* frame_data = read_buffer_.data();
        size_t frame_len = expected_frame_size_;

        // Parse the request header.
        torrent::protocol::RequestHeader req_header;
        int header_bytes = parse_request_header(frame_data, frame_len, req_header);

        if (header_bytes <= 0) {
            spdlog::error("[conn:{}] Failed to parse request header ({} bytes frame)",
                          conn_id_, frame_len);
            metrics_.framing_errors.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        const char* body = frame_data + header_bytes;
        size_t body_len = frame_len - static_cast<size_t>(header_bytes);

        // Update metrics.
        metrics_.requests_received.fetch_add(1, std::memory_order_relaxed);
        metrics_.last_read_at_ms.store(now_ms(), std::memory_order_relaxed);

        // Dispatch the request.
        dispatch_request(req_header, body, body_len);

        // Reset parser state for the next frame.
        expected_frame_size_ = 0;
        read_buffer_pos_ = 0;

        // If connection was closed during dispatch, stop.
        if (state_.load(std::memory_order_acquire) == ConnectionState::closed) {
            return true;  // Not a parser error — connection was closed.
        }
    }

    return true;
}

// --------------------------------------------------------------------------
// Request dispatch
// --------------------------------------------------------------------------

void Connection::dispatch_request(const torrent::protocol::RequestHeader& header,
                                   const char* body, size_t body_len) {
    ConnectionState current = state_.load(std::memory_order_acquire);

    spdlog::trace("[conn:{}] Dispatching request: api_key={} ({}) version={} corr_id={} client={} size={}",
                  conn_id_, header.api_key,
                  torrent::protocol::api_key_name(header.api_key),
                  header.api_version, header.correlation_id,
                  header.client_id, body_len);

    // ---- State-based dispatch ----

    // During handshaking, only SASL-related and ApiVersions requests are
    // allowed.  Everything else gets rejected.
    if (current == ConnectionState::handshaking) {
        if (header.api_key == torrent::protocol::kApiKeySaslHandshake ||
            header.api_key == torrent::protocol::kApiKeySaslAuthenticate) {
            handle_sasl_request(header, body, body_len);
            return;
        }
        if (header.api_key == torrent::protocol::kApiKeyApiVersions) {
            // ApiVersions is always allowed — handle it.
            // Fall through to handler dispatch.
            goto dispatch_to_handler;
        }
        // Reject all other requests during handshake.
        spdlog::warn("[conn:{}] Rejecting request api_key={} during handshake (SASL required={})",
                     conn_id_, header.api_key, config_.require_sasl);
        enqueue_error_response(header.correlation_id,
                               torrent::error_code::illegal_sasl_state,
                               "Connection is still handshaking; SASL may be required");
        return;
    }

    // While draining, reject new requests but allow health checks.
    if (current == ConnectionState::draining) {
        if (header.api_key != torrent::protocol::kApiKeyTorrentHealthCheck) {
            spdlog::debug("[conn:{}] Rejecting request api_key={}: connection is draining",
                          conn_id_, header.api_key);
            enqueue_error_response(header.correlation_id,
                                   torrent::error_code::shutdown_in_progress,
                                   "Broker is shutting down; connection is draining");
            return;
        }
    }

dispatch_to_handler:

    // ---- Look up the handler ----
    RequestHandler handler;
    {
        std::lock_guard<std::mutex> lock(handler_mutex_);
        auto it = handlers_.find(header.api_key);
        if (it != handlers_.end()) {
            handler = it->second;
        }
    }

    if (!handler) {
        spdlog::warn("[conn:{}] No handler registered for api_key={} ({})",
                     conn_id_, header.api_key,
                     torrent::protocol::api_key_name(header.api_key));
        metrics_.dispatch_errors.fetch_add(1, std::memory_order_relaxed);
        enqueue_error_response(header.correlation_id,
                               torrent::error_code::unsupported_version,
                               "Unsupported API key");
        return;
    }

    // ---- Increment inflight count ----
    uint32_t inflight = metrics_.inflight_requests.fetch_add(1, std::memory_order_relaxed) + 1;
    if (inflight > config_.max_inflight_requests) {
        // Backpressure: too many inflight requests.
        metrics_.inflight_requests.fetch_sub(1, std::memory_order_relaxed);
        spdlog::warn("[conn:{}] Max inflight requests reached ({}/{}), rejecting api_key={}",
                     conn_id_, inflight - 1, config_.max_inflight_requests,
                     header.api_key);
        enqueue_error_response(header.correlation_id,
                               torrent::error_code::internal_queue_full,
                               "Too many inflight requests; try again later");
        return;
    }

    // ---- Invoke the handler ----
    try {
        torrent::buffer_view body_view(body, body_len);
        handler(header, body_view, conn_id_);
    } catch (const std::exception& e) {
        spdlog::error("[conn:{}] Handler for api_key={} threw: {}",
                      conn_id_, header.api_key, e.what());
        metrics_.dispatch_errors.fetch_add(1, std::memory_order_relaxed);
        // Try to send an error response if one wasn't already enqueued.
        enqueue_error_response(header.correlation_id,
                               torrent::error_code::unknown_server_error,
                               std::string("Handler exception: ") + e.what());
    } catch (...) {
        spdlog::error("[conn:{}] Handler for api_key={} threw unknown exception",
                      conn_id_, header.api_key);
        metrics_.dispatch_errors.fetch_add(1, std::memory_order_relaxed);
        enqueue_error_response(header.correlation_id,
                               torrent::error_code::unknown_server_error,
                               "Unknown handler exception");
    }
}

// --------------------------------------------------------------------------
// Response queue flush
// --------------------------------------------------------------------------

size_t Connection::flush_response_queue() {
    size_t total_written = 0;

    // We process serialized frames from the response queue.
    // Each ResponseEnvelope contains a payload (response header + body).
    // We prepend the 4-byte length prefix when writing.

    std::lock_guard<std::mutex> lock(response_mutex_);

    // We track partial writes per-envelope using an ongoing serialization:
    // - We maintain a local serialized frame buffer for the current envelope.
    // - response_prefix_written_ tracks bytes of the length prefix already sent.
    // - response_write_offset_ tracks bytes of the envelope payload already sent.

    // For simplicity, we serialize one envelope at a time, writing as much
    // as the transport accepts in non-blocking mode.

    while (!response_queue_.empty()) {
        ResponseEnvelope& envelope = response_queue_.front();

        // Build the wire-format frame if we haven't started this envelope.
        // We track the current write state with response_prefix_written_ and
        // response_write_offset_.

        const char* payload_data = envelope.payload ? envelope.payload->data() : nullptr;
        size_t payload_size = envelope.payload ? envelope.payload->size() : 0;

        // Total bytes to write: 4 (length prefix) + payload_size
        size_t total_frame_bytes = torrent::protocol::kFrameLengthSize + payload_size;

        // Write in stages:
        // Stage 1: Write the 4-byte length prefix (if not fully written yet).
        // Stage 2: Write the payload (correlation_id + body).

        while (response_prefix_written_ < torrent::protocol::kFrameLengthSize) {
            // Build the length prefix on the stack.
            char prefix[torrent::protocol::kFrameLengthSize];
            write_int32_be(prefix, static_cast<int32_t>(payload_size));

            const char* src = prefix + response_prefix_written_;
            size_t remaining = torrent::protocol::kFrameLengthSize - response_prefix_written_;

            ssize_t n = transport_->write(src, remaining);
            if (n < 0) {
                int err = errno;
                if (is_retryable_errno(err)) {
                    return total_written;  // Would block; retry next on_writable.
                }
                spdlog::error("[conn:{}] Write error during response prefix: {} (errno={})",
                              conn_id_, std::strerror(err), err);
                // Don't close here — let the caller handle the error.
                return total_written;
            }
            if (n == 0) {
                // Transport closed.
                spdlog::debug("[conn:{}] Transport closed during response write", conn_id_);
                return total_written;
            }
            response_prefix_written_ += static_cast<uint8_t>(n);
            total_written += static_cast<size_t>(n);
            metrics_.bytes_sent.fetch_add(static_cast<uint64_t>(n), std::memory_order_relaxed);

            // Throttle check.
            if (config_.write_throttle_bps > 0 && !throttle_write(static_cast<size_t>(n))) {
                return total_written;
            }
        }

        // Write the payload (possibly partially).
        while (response_write_offset_ < payload_size) {
            size_t remaining = payload_size - response_write_offset_;

            // Throttle: check write allowance.
            if (config_.write_throttle_bps > 0) {
                uint64_t tokens = write_tokens_.load(std::memory_order_relaxed);
                if (tokens == 0) {
                    return total_written;  // No tokens; retry on timer refill.
                }
                remaining = std::min(remaining, static_cast<size_t>(tokens));
            }

            ssize_t n = transport_->write(payload_data + response_write_offset_, remaining);
            if (n < 0) {
                int err = errno;
                if (is_retryable_errno(err)) {
                    return total_written;  // Would block.
                }
                spdlog::error("[conn:{}] Write error during response payload: {} (errno={})",
                              conn_id_, std::strerror(err), err);
                return total_written;
            }
            if (n == 0) {
                return total_written;
            }
            response_write_offset_ += static_cast<size_t>(n);
            total_written += static_cast<size_t>(n);
            metrics_.bytes_sent.fetch_add(static_cast<uint64_t>(n), std::memory_order_relaxed);

            if (config_.write_throttle_bps > 0 && !throttle_write(static_cast<size_t>(n))) {
                return total_written;
            }
        }

        // Envelope fully written — pop it and reset tracking.
        response_queue_.pop();
        response_prefix_written_ = 0;
        response_write_offset_ = 0;
    }

    return total_written;
}

// --------------------------------------------------------------------------
// SASL handshake
// --------------------------------------------------------------------------

void Connection::handle_sasl_request(const torrent::protocol::RequestHeader& header,
                                      const char* body, size_t body_len) {
    SaslState sasl = sasl_state_.load(std::memory_order_acquire);

    if (header.api_key == torrent::protocol::kApiKeySaslHandshake) {
        // ---- SaslHandshake request ----
        // Body: mechanism (string) — the client's preferred SASL mechanism.
        //
        // We need to validate the mechanism against our configured list,
        // then transition to mechanism_picked state.

        // Parse the mechanism string from the body.
        // SaslHandshake v0 request format:
        //   mechanism: string (int16 length + UTF-8)
        if (body_len < 2) {
            spdlog::warn("[conn:{}] SaslHandshake: body too short ({} bytes)", conn_id_, body_len);
            enqueue_error_response(header.correlation_id,
                                   torrent::error_code::invalid_request,
                                   "SaslHandshake body too short");
            return;
        }

        int16_t mech_len = read_int16_be(body);
        if (mech_len <= 0 || static_cast<size_t>(mech_len + 2) > body_len) {
            spdlog::warn("[conn:{}] SaslHandshake: invalid mechanism length {}", conn_id_, mech_len);
            enqueue_error_response(header.correlation_id,
                                   torrent::error_code::invalid_request,
                                   "Invalid mechanism length in SaslHandshake");
            return;
        }

        std::string mechanism(body + 2, static_cast<size_t>(mech_len));

        spdlog::info("[conn:{}] SaslHandshake: client requested mechanism '{}'",
                     conn_id_, mechanism);

        // Validate the mechanism.
        bool allowed = false;
        if (config_.sasl_mechanisms.empty()) {
            // All mechanisms allowed.
            allowed = true;
        } else {
            for (const auto& m : config_.sasl_mechanisms) {
                if (m == mechanism) {
                    allowed = true;
                    break;
                }
            }
        }

        // Build SaslHandshake response body.
        // Response format:
        //   error_code: int16
        //   mechanisms: array of strings
        //     array_length: int32
        //     [mechanism: string]...

        if (!allowed) {
            spdlog::warn("[conn:{}] SaslHandshake: mechanism '{}' not allowed", conn_id_, mechanism);
            // Send error response with list of available mechanisms.
            std::vector<char> resp_body;
            resp_body.reserve(256);

            // error_code
            resp_body.resize(2);
            write_int16_be(resp_body.data(),
                           static_cast<int16_t>(torrent::error_code::unsupported_sasl_mechanism));

            // mechanisms array
            const auto& mechs = config_.sasl_mechanisms.empty()
                                ? std::vector<std::string>{"SCRAM-SHA-256", "SCRAM-SHA-512"}
                                : config_.sasl_mechanisms;
            int32_t count = static_cast<int32_t>(mechs.size());
            resp_body.resize(resp_body.size() + 4);
            write_int32_be(&resp_body[resp_body.size() - 4], count);
            for (const auto& m : mechs) {
                write_string(resp_body, m);
            }

            auto payload = std::make_unique<torrent::shared_buffer>(
                resp_body.data(), resp_body.size());
            enqueue_response(ResponseEnvelope(header.correlation_id, std::move(payload)));
            return;
        }

        // Mechanism allowed.
        sasl_mechanism_ = mechanism;
        sasl_state_.store(SaslState::mechanism_picked, std::memory_order_release);

        // Build success response with mechanism list.
        std::vector<char> resp_body;
        resp_body.reserve(256);
        resp_body.resize(2);
        write_int16_be(resp_body.data(), 0);  // error_code = NONE (0)

        const auto& mechs = config_.sasl_mechanisms.empty()
                            ? std::vector<std::string>{mechanism}
                            : config_.sasl_mechanisms;
        int32_t count = static_cast<int32_t>(mechs.size());
        resp_body.resize(resp_body.size() + 4);
        write_int32_be(&resp_body[resp_body.size() - 4], count);
        for (const auto& m : mechs) {
            write_string(resp_body, m);
        }

        auto payload = std::make_unique<torrent::shared_buffer>(
            resp_body.data(), resp_body.size());
        enqueue_response(ResponseEnvelope(header.correlation_id, std::move(payload)));

        // Now transition to authenticating state and initiate the token exchange.
        // In a full implementation, this would trigger a SaslHandshake object
        // to send a challenge. For now, wait for the client to send
        // SaslAuthenticate.
        sasl_state_.store(SaslState::authenticating, std::memory_order_release);
        metrics_.last_read_at_ms.store(now_ms(), std::memory_order_relaxed);

    } else if (header.api_key == torrent::protocol::kApiKeySaslAuthenticate) {
        // ---- SaslAuthenticate request ----
        // Body: auth_bytes (bytes) — opaque SASL token from the client.
        //
        // In a full implementation, this would be processed by a SASL library
        // (e.g., Cyrus SASL, or built-in SCRAM).  For now, we implement a
        // simplified server-side flow that completes SASL if the client sends
        // a non-empty token (accepting any authentication for testing).

        if (body_len == 0) {
            spdlog::warn("[conn:{}] SaslAuthenticate: empty body", conn_id_);
            enqueue_error_response(header.correlation_id,
                                   torrent::error_code::invalid_request,
                                   "SaslAuthenticate requires authentication bytes");
            return;
        }

        // Parse the auth_bytes.
        // SaslAuthenticate v0 request format:
        //   auth_bytes: bytes (int32 length + data)
        if (body_len < 4) {
            spdlog::warn("[conn:{}] SaslAuthenticate: body too short", conn_id_);
            enqueue_error_response(header.correlation_id,
                                   torrent::error_code::invalid_request,
                                   "SaslAuthenticate body too short");
            return;
        }

        int32_t auth_len = read_int32_be(body);
        if (auth_len < 0 || static_cast<size_t>(auth_len + 4) > body_len) {
            spdlog::warn("[conn:{}] SaslAuthenticate: invalid auth_bytes length {}", conn_id_, auth_len);
            enqueue_error_response(header.correlation_id,
                                   torrent::error_code::invalid_request,
                                   "Invalid auth_bytes length");
            return;
        }

        // In a real implementation, we'd pass the auth bytes to the SASL
        // library and get a challenge response.  Here we simulate a simple
        // acceptance.

        spdlog::info("[conn:{}] SaslAuthenticate: received {} bytes auth token",
                     conn_id_, auth_len);

        // Build SaslAuthenticate response.
        //   error_code: int16
        //   error_message: nullable string
        //   auth_bytes: bytes (int32 length + data)
        //   session_lifetime_ms: int64 (0 = no limit)

        std::vector<char> resp_body;
        resp_body.reserve(128);

        // error_code = NONE
        resp_body.resize(2);
        write_int16_be(resp_body.data(), 0);

        // error_message = null
        resp_body.resize(resp_body.size() + 2);
        write_int16_be(&resp_body[resp_body.size() - 2], -1);

        // auth_bytes = empty (no further challenge from server in simple flow)
        resp_body.resize(resp_body.size() + 4);
        write_int32_be(&resp_body[resp_body.size() - 4], 0);

        // session_lifetime_ms = 0 (unlimited)
        resp_body.resize(resp_body.size() + 8);
        std::memset(&resp_body[resp_body.size() - 8], 0, 8);

        auto payload = std::make_unique<torrent::shared_buffer>(
            resp_body.data(), resp_body.size());
        enqueue_response(ResponseEnvelope(header.correlation_id, std::move(payload)));

        // SASL complete.
        principal_ = "sasl-user";  // placeholder
        sasl_state_.store(SaslState::authenticated, std::memory_order_release);
        metrics_.last_read_at_ms.store(now_ms(), std::memory_order_relaxed);

        // If we were in handshaking state, transition to active.
        if (state_.load(std::memory_order_acquire) == ConnectionState::handshaking) {
            set_state(ConnectionState::active);
            spdlog::info("[conn:{}] SASL authentication complete, connection active", conn_id_);
        }
    }
}

// --------------------------------------------------------------------------
// TLS handshake step
// --------------------------------------------------------------------------

int Connection::tls_handshake_step() {
    if (!transport_ || !transport_->is_tls()) {
        spdlog::warn("[conn:{}] tls_handshake_step called but transport is not TLS", conn_id_);
        return -1;
    }

    // Reinterpret transport as TlsTransport to access handshake().
    // Since we know it's TLS, this dynamic_cast is safe.
    // We use a static_cast to TlsTransport* because we've already verified is_tls().
    auto* tls = reinterpret_cast<TlsTransport*>(transport_.get());
    int rc = tls->handshake();

    if (rc == 1) {
        // Handshake complete.
        spdlog::info("[conn:{}] TLS handshake completed successfully", conn_id_);
        if (tls->peer_verified()) {
            spdlog::debug("[conn:{}] Peer certificate verified, CN={}",
                          conn_id_, tls->peer_cn());
        }
        metrics_.last_read_at_ms.store(now_ms(), std::memory_order_relaxed);
    } else if (rc == 0) {
        // In progress — WANT_READ or WANT_WRITE.
        // The caller (on_readable or on_writable) will drive the next step.
        spdlog::trace("[conn:{}] TLS handshake in progress (want more I/O)", conn_id_);
    } else {
        // Error.
        spdlog::error("[conn:{}] TLS handshake failed with code {}", conn_id_, rc);
        // Log OpenSSL errors if available.
        unsigned long ssl_err;
        while ((ssl_err = ERR_get_error()) != 0) {
            char buf[256];
            ERR_error_string_n(ssl_err, buf, sizeof(buf));
            spdlog::error("[conn:{}] TLS error: {}", conn_id_, buf);
        }
    }

    return rc;
}

// --------------------------------------------------------------------------
// Token-bucket throttling
// --------------------------------------------------------------------------

bool Connection::throttle_read(size_t bytes) {
    if (config_.read_throttle_bps == 0) return true;

    uint64_t current = read_tokens_.load(std::memory_order_relaxed);
    if (current < bytes) {
        // Refill might help; do a quick refill.
        refill_tokens();
        current = read_tokens_.load(std::memory_order_relaxed);
        if (current < bytes) {
            return false;  // Not enough tokens.
        }
    }

    read_tokens_.fetch_sub(bytes, std::memory_order_relaxed);
    return true;
}

bool Connection::throttle_write(size_t bytes) {
    if (config_.write_throttle_bps == 0) return true;

    uint64_t current = write_tokens_.load(std::memory_order_relaxed);
    if (current < bytes) {
        refill_tokens();
        current = write_tokens_.load(std::memory_order_relaxed);
        if (current < bytes) {
            return false;
        }
    }

    write_tokens_.fetch_sub(bytes, std::memory_order_relaxed);
    return true;
}

void Connection::refill_tokens() {
    torrent::timestamp_ms_t now = now_ms();

    // ---- Read tokens ----
    if (config_.read_throttle_bps > 0) {
        torrent::timestamp_ms_t last = read_token_ts_.load(std::memory_order_relaxed);
        if (last == 0) {
            // First refill: initialise timestamp and set burst.
            read_token_ts_.store(now, std::memory_order_relaxed);
            read_tokens_.store(config_.throttle_burst_bytes, std::memory_order_relaxed);
        } else {
            torrent::timestamp_ms_t elapsed = now - last;
            if (elapsed >= kTokenRefillIntervalMs) {
                // Refill: rate * elapsed / 1000
                uint64_t refill = static_cast<uint64_t>(
                    static_cast<double>(config_.read_throttle_bps) *
                    static_cast<double>(elapsed) / 1000.0);

                if (refill > 0) {
                    uint64_t current = read_tokens_.load(std::memory_order_relaxed);
                    uint64_t new_tokens = std::min(
                        current + refill,
                        config_.throttle_burst_bytes);
                    read_tokens_.store(new_tokens, std::memory_order_relaxed);

                    read_token_ts_.store(now, std::memory_order_relaxed);
                }
            }
        }
    }

    // ---- Write tokens ----
    if (config_.write_throttle_bps > 0) {
        torrent::timestamp_ms_t last = write_token_ts_.load(std::memory_order_relaxed);
        if (last == 0) {
            write_token_ts_.store(now, std::memory_order_relaxed);
            write_tokens_.store(config_.throttle_burst_bytes, std::memory_order_relaxed);
        } else {
            torrent::timestamp_ms_t elapsed = now - last;
            if (elapsed >= kTokenRefillIntervalMs) {
                uint64_t refill = static_cast<uint64_t>(
                    static_cast<double>(config_.write_throttle_bps) *
                    static_cast<double>(elapsed) / 1000.0);

                if (refill > 0) {
                    uint64_t current = write_tokens_.load(std::memory_order_relaxed);
                    uint64_t new_tokens = std::min(
                        current + refill,
                        config_.throttle_burst_bytes);
                    write_tokens_.store(new_tokens, std::memory_order_relaxed);

                    write_token_ts_.store(now, std::memory_order_relaxed);
                }
            }
        }
    }
}

// --------------------------------------------------------------------------
// Endpoint & fd queries (non-inline)
//
//  fd(), peer(), and local() are declared inline in the header but can also
//  have a non-inline out-of-line definition for use in translation units.
//  We include them here (though they're already in the header) for any
//  platform that needs an explicit instantiation.
// --------------------------------------------------------------------------

// (All inline methods are defined in the header — see connection.h.)

// ============================================================================
// SaslHandshake stubs (integration point)
// ============================================================================

// The SaslHandshake class from sasl_handshake.h provides the SASL state
// machine.  For a complete implementation, Connection would own a
// SaslHandshake instance and delegate mechanism negotiation and token
// exchange to it.  The stubs above handle the Kafka-protocol framing of
// SaslHandshake and SaslAuthenticate requests.
//
// Full integration would replace the simplified handle_sasl_request logic
// with calls to:
//   sasl_handshake_->initiate(mechanism)
//   sasl_handshake_->respond(token)
// and build wire-format responses from the result<ByteBuffer> outputs.

} // namespace torrent::network
