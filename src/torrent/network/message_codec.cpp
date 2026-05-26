/**
 * message_codec.cpp — MessageCodec: Wire-Protocol Frame Encoder/Decoder
 *
 * Implements the torrent-mq wire protocol framing layer.  All messages
 * exchanged between clients and brokers (and broker-to-broker) use a
 * consistent 4-byte big-endian length-prefix framing.
 *
 * Request frame format (Kafka-compatible v2 header):
 *   ┌──────────────────────────────────────────────────────────────────┐
 *   │  frame_size        int32 (big-endian)  — total bytes after this  │
 *   │  api_key           int16 (big-endian)  — request type             │
 *   │  api_version       int16 (big-endian)  — request version          │
 *   │  correlation_id    int32 (big-endian)  — client-assigned id       │
 *   │  client_id_len     int16 (big-endian)  — length of client_id      │
 *   │  client_id         UTF-8 string        — client identifier        │
 *   │  [tagged fields]   variable            — v2+ tagged fields (none) │
 *   │  body              bytes               — serialized request body  │
 *   └──────────────────────────────────────────────────────────────────┘
 *
 * Response frame format:
 *   ┌──────────────────────────────────────────────────────────────────┐
 *   │  frame_size        int32 (big-endian)  — total bytes after this  │
 *   │  correlation_id    int32 (big-endian)  — echoes request id        │
 *   │  body              bytes               — serialized response body │
 *   └──────────────────────────────────────────────────────────────────┘
 *
 * All multi-byte integers are encoded in big-endian (network byte order).
 * This is consistent with Kafka's wire protocol and ensures interop with
 * existing Kafka client libraries.
 *
 * Peek operations allow frame boundary detection without consuming bytes
 * from the buffer, enabling efficient event-loop integration.
 */

// ============================================================================
// Project headers
// ============================================================================

#include "torrent/network/message_codec.h"
#include "torrent/common/types.h"

// ============================================================================
// System headers
// ============================================================================

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>

namespace torrent::network {

// ============================================================================
// Anonymous namespace — wire-format encoding helpers
// ============================================================================

namespace {

/// Logger instance.
std::shared_ptr<spdlog::logger> get_codec_logger() {
    static auto logger = spdlog::get("message_codec");
    if (!logger) {
        logger = spdlog::stdout_color_mt("message_codec");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Big-endian encoding / decoding primitives
// --------------------------------------------------------------------------

/// Write a big-endian int32 into a byte buffer at position `offset`.
inline void write_int32_be(std::vector<uint8_t>& buf, size_t offset, int32_t v) {
    uint32_t u = static_cast<uint32_t>(v);
    buf[offset]     = static_cast<uint8_t>((u >> 24) & 0xFF);
    buf[offset + 1] = static_cast<uint8_t>((u >> 16) & 0xFF);
    buf[offset + 2] = static_cast<uint8_t>((u >> 8) & 0xFF);
    buf[offset + 3] = static_cast<uint8_t>(u & 0xFF);
}

/// Write a big-endian int16 into a byte buffer at position `offset`.
inline void write_int16_be(std::vector<uint8_t>& buf, size_t offset, int16_t v) {
    uint16_t u = static_cast<uint16_t>(v);
    buf[offset]     = static_cast<uint8_t>((u >> 8) & 0xFF);
    buf[offset + 1] = static_cast<uint8_t>(u & 0xFF);
}

/// Read a big-endian int32 from a byte buffer at `offset`.
inline int32_t read_int32_be(const uint8_t* data, size_t offset) {
    uint32_t v = (static_cast<uint32_t>(data[offset])     << 24)
               | (static_cast<uint32_t>(data[offset + 1]) << 16)
               | (static_cast<uint32_t>(data[offset + 2]) << 8)
               |  static_cast<uint32_t>(data[offset + 3]);
    return static_cast<int32_t>(v);
}

/// Read a big-endian int16 from a byte buffer at `offset`.
inline int16_t read_int16_be(const uint8_t* data, size_t offset) {
    uint16_t v = (static_cast<uint16_t>(data[offset])     << 8)
               |  static_cast<uint16_t>(data[offset + 1]);
    return static_cast<int16_t>(v);
}

// --------------------------------------------------------------------------
// Request header sizes
// --------------------------------------------------------------------------

/// Minimum request header size in bytes:
///   api_key(2) + api_version(2) + correlation_id(4) + client_id_len(2) = 10
inline constexpr size_t kMinRequestHeaderSize = 10;

/// Frame length prefix size: 4 bytes (int32 big-endian).
inline constexpr size_t kFrameLengthSize = 4;

/// Response header size: correlation_id (4 bytes).
inline constexpr size_t kResponseHeaderSize = 4;

/// Maximum client ID length (defensive bound).
inline constexpr int16_t kMaxClientIdLength = 512;

/// Maximum frame size (defensive bound: 100 MiB).
inline constexpr int32_t kMaxFrameSize = 100 * 1024 * 1024;

// --------------------------------------------------------------------------
// String encoding (compact string: int16 length prefix + UTF-8 data)
// --------------------------------------------------------------------------

/// Write a compact string (int16 length prefix + data) into buf at the end.
inline void append_compact_string(std::vector<uint8_t>& buf, const std::string& s) {
    int16_t len = static_cast<int16_t>(s.size());
    size_t pos = buf.size();
    buf.resize(pos + 2 + s.size());
    write_int16_be(buf, pos, len);
    if (!s.empty()) {
        std::memcpy(buf.data() + pos + 2, s.data(), s.size());
    }
}

/// Write a nullable compact string.  -1 length means null.
inline void append_nullable_string(std::vector<uint8_t>& buf, const std::string& s) {
    // In Kafka protocol, nullable strings use int16 length; -1 = null.
    // Empty string has length 0.
    int16_t len = static_cast<int16_t>(s.size());
    size_t pos = buf.size();
    buf.resize(pos + 2 + s.size());
    write_int16_be(buf, pos, len);
    if (!s.empty()) {
        std::memcpy(buf.data() + pos + 2, s.data(), s.size());
    }
}

/// Read a compact string from `data` at position `pos`.
/// Returns the number of bytes consumed, or -1 on error.
inline int read_compact_string(const uint8_t* data, size_t size,
                                size_t pos, std::string& out) {
    if (size < pos + 2) return -1;
    int16_t len = read_int16_be(data, pos);
    if (len < -1) return -1;
    if (static_cast<size_t>(len) > kMaxClientIdLength && len > 0) return -1;
    if (len == -1) {
        out.clear();
        return 2;
    }
    size_t ulen = static_cast<size_t>(len);
    if (size < pos + 2 + ulen) return -1;
    out.assign(reinterpret_cast<const char*>(data + pos + 2), ulen);
    return 2 + static_cast<int>(ulen);
}

} // anonymous namespace

// ============================================================================
// MessageCodec::encode_request
// ============================================================================

/**
 * Build a complete request frame ready for transmission over the wire.
 *
 * The returned frame includes:
 *   [4] total frame length (big-endian int32)
 *   [2] api_key (big-endian int16)
 *   [2] api_version (big-endian int16)
 *   [4] correlation_id (big-endian int32)
 *   [2 + len] client_id (compact string: length prefix + UTF-8)
 *   [N] body (opaque serialized request payload)
 *
 * The frame length includes everything after the 4-byte length prefix.
 *
 * @param api_key         Request type identifier (e.g., 0 = Produce, 1 = Fetch).
 * @param api_version     Protocol version for this request type.
 * @param correlation_id  Client-assigned correlation ID for matching response.
 * @param client_id       Client identifier (e.g., "torrent-cli/1.0").
 * @param body            Serialized request body (buffer_view over raw bytes).
 * @return                Complete wire-format frame as a byte vector.
 */
std::vector<uint8_t> MessageCodec::encode_request(
    int16_t api_key,
    int16_t api_version,
    int32_t correlation_id,
    const std::string& client_id,
    buffer_view body)
{
    // Calculate payload size (everything after the 4-byte length prefix).
    // api_key(2) + api_version(2) + correlation_id(4) + client_id(2+len) + body
    size_t payload_size = kMinRequestHeaderSize + client_id.size() + body.size;

    // Check for overflow.
    if (payload_size > static_cast<size_t>(kMaxFrameSize)) {
        auto logger = get_codec_logger();
        logger->error("encode_request: frame too large ({} bytes), max {}",
                      payload_size, kMaxFrameSize);
        return {};
    }

    size_t total_size = kFrameLengthSize + payload_size;

    std::vector<uint8_t> frame(total_size);

    // Write 4-byte frame length prefix (payload size, not including the
    // 4-byte prefix itself).
    write_int32_be(frame, 0, static_cast<int32_t>(payload_size));

    size_t offset = kFrameLengthSize;

    // Write api_key (2 bytes, big-endian).
    write_int16_be(frame, offset, api_key);
    offset += 2;

    // Write api_version (2 bytes, big-endian).
    write_int16_be(frame, offset, api_version);
    offset += 2;

    // Write correlation_id (4 bytes, big-endian).
    write_int32_be(frame, offset, correlation_id);
    offset += 4;

    // Write client_id (compact string: int16 length prefix + UTF-8 data).
    int16_t client_id_len = static_cast<int16_t>(client_id.size());
    write_int16_be(frame, offset, client_id_len);
    offset += 2;
    if (client_id_len > 0) {
        std::memcpy(frame.data() + offset, client_id.data(),
                     static_cast<size_t>(client_id_len));
        offset += static_cast<size_t>(client_id_len);
    }

    // Write body.
    if (body.size > 0 && body.data != nullptr) {
        std::memcpy(frame.data() + offset, body.data, body.size);
    }

    auto logger = get_codec_logger();
    logger->trace("encode_request: api_key={}, api_version={}, corr_id={}, "
                  "client='{}', body={}B, frame={}B",
                  api_key, api_version, correlation_id, client_id,
                  body.size, total_size);

    return frame;
}

// ============================================================================
// MessageCodec::encode_response
// ============================================================================

/**
 * Build a complete response frame.
 *
 * Response frame format:
 *   [4] frame_size        int32 (big-endian)  — total bytes after this
 *   [4] correlation_id    int32 (big-endian)  — echoes request
 *   [N] body              bytes               — serialized response body
 *
 * The response header is minimal: only the correlation_id is needed,
 * since the request type is known by the client from its pending
 * request map.
 *
 * @param correlation_id  Matches the request's correlation_id.
 * @param body            Serialized response body.
 * @return                Complete wire-format frame.
 */
std::vector<uint8_t> MessageCodec::encode_response(
    int32_t correlation_id,
    buffer_view body)
{
    // Payload: correlation_id(4) + body.
    size_t payload_size = kResponseHeaderSize + body.size;

    if (payload_size > static_cast<size_t>(kMaxFrameSize)) {
        auto logger = get_codec_logger();
        logger->error("encode_response: frame too large ({} bytes), max {}",
                      payload_size, kMaxFrameSize);
        return {};
    }

    size_t total_size = kFrameLengthSize + payload_size;
    std::vector<uint8_t> frame(total_size);

    // Write 4-byte length prefix (payload size).
    write_int32_be(frame, 0, static_cast<int32_t>(payload_size));

    // Write correlation_id (4 bytes).
    write_int32_be(frame, kFrameLengthSize, correlation_id);

    // Write body.
    if (body.size > 0 && body.data != nullptr) {
        std::memcpy(frame.data() + kFrameLengthSize + kResponseHeaderSize,
                    body.data, body.size);
    }

    auto logger = get_codec_logger();
    logger->trace("encode_response: corr_id={}, body={}B, frame={}B",
                  correlation_id, body.size, total_size);

    return frame;
}

// ============================================================================
// MessageCodec::decode_request_header
// ============================================================================

/**
 * Parse a request header from raw bytes.
 *
 * Extracts api_key, api_version, correlation_id, and client_id from
 * the wire-format header.  Returns a buffer_view pointing to the body
 * data (the remaining bytes after the header).
 *
 * Wire format:
 *   [2] api_key
 *   [2] api_version
 *   [4] correlation_id
 *   [2 + N] client_id (compact string)
 *   [...]  body (remaining bytes)
 *
 * Note: the `data` and `size` parameters should describe the payload
 * AFTER the 4-byte length prefix has been consumed.  The caller is
 * responsible for frame boundary detection via peek_frame_size().
 *
 * @param data            Pointer to start of payload (after length prefix).
 * @param size            Total bytes available in the payload.
 * @param api_key         [out] Parsed API key.
 * @param api_version     [out] Parsed API version.
 * @param correlation_id  [out] Parsed correlation ID.
 * @return                buffer_view over the body bytes.  On error, returns
 *                        a buffer_view with data=nullptr, size=0 and the
 *                        out-parameters are left unmodified.
 */
buffer_view MessageCodec::decode_request_header(
    const uint8_t* data,
    size_t size,
    int16_t& api_key,
    int16_t& api_version,
    int32_t& correlation_id)
{
    // Validate minimum size.
    if (size < kMinRequestHeaderSize) {
        auto logger = get_codec_logger();
        logger->warn("decode_request_header: insufficient data: {} bytes (need {})",
                     size, kMinRequestHeaderSize);
        return buffer_view{};
    }

    if (data == nullptr) {
        auto logger = get_codec_logger();
        logger->warn("decode_request_header: null data pointer");
        return buffer_view{};
    }

    size_t offset = 0;

    // Parse api_key (2 bytes).
    api_key = read_int16_be(data, offset);
    offset += 2;

    // Parse api_version (2 bytes).
    api_version = read_int16_be(data, offset);
    offset += 2;

    // Parse correlation_id (4 bytes).
    correlation_id = read_int32_be(data, offset);
    offset += 4;

    // Parse client_id (compact string).
    std::string client_id;
    int consumed = read_compact_string(data, size, offset, client_id);
    if (consumed < 0) {
        auto logger = get_codec_logger();
        logger->warn("decode_request_header: invalid client_id string at offset {}",
                     offset);
        return buffer_view{};
    }
    offset += static_cast<size_t>(consumed);

    // Remaining bytes are the body.
    size_t body_size = (size > offset) ? (size - offset) : 0;

    auto logger = get_codec_logger();
    logger->trace("decode_request_header: api_key={}, api_version={}, "
                  "corr_id={}, client='{}', header={}B, body={}B",
                  api_key, api_version, correlation_id, client_id,
                  offset, body_size);

    if (body_size == 0) {
        return buffer_view{};
    }

    return buffer_view(reinterpret_cast<const char*>(data + offset), body_size);
}

// ============================================================================
// MessageCodec::peek_correlation_id
// ============================================================================

/**
 * Read the correlation_id from a frame's header without consuming the
 * buffer or performing a full parse.
 *
 * The correlation_id is at offset 8 within the payload (after the 4-byte
 * length prefix, then api_key(2) + api_version(2) = 4, then corr_id(4)).
 *
 * This is useful for response dispatching: the event loop can peek the
 * correlation_id, look up the pending request, and route the response
 * without doing a full decode.
 *
 * @param data   Pointer to start of payload (after the length prefix).
 * @param size   Total bytes available.
 * @return       The correlation_id, or -1 on error / insufficient data.
 */
int32_t MessageCodec::peek_correlation_id(
    const uint8_t* data,
    size_t size)
{
    // Need at least: api_key(2) + api_version(2) + correlation_id(4) = 8 bytes.
    static constexpr size_t kCorrIdOffset = 4; // offset into payload
    static constexpr size_t kCorrIdSize   = 4;

    if (size < kCorrIdOffset + kCorrIdSize) {
        auto logger = get_codec_logger();
        logger->trace("peek_correlation_id: insufficient data: {} bytes (need {})",
                      size, kCorrIdOffset + kCorrIdSize);
        return -1;
    }

    if (data == nullptr) {
        return -1;
    }

    return read_int32_be(data, kCorrIdOffset);
}

// ============================================================================
// MessageCodec::peek_frame_size
// ============================================================================

/**
 * Read the 4-byte big-endian frame length prefix from the start of a
 * frame buffer.
 *
 * The returned value is the total payload size in bytes (everything
 * after the 4-byte prefix).  The caller uses this to determine when
 * a complete frame has been received.
 *
 * Returns -1 if there are fewer than 4 bytes available (incomplete
 * length prefix) or if the frame size exceeds kMaxFrameSize.
 *
 * @param data   Pointer to start of frame (including the 4-byte prefix).
 * @param size   Total bytes available.
 * @return       The frame payload size, or -1 on error.
 */
int32_t MessageCodec::peek_frame_size(
    const uint8_t* data,
    size_t size)
{
    if (size < kFrameLengthSize) {
        // Not enough data for the length prefix yet.
        return -1;
    }

    if (data == nullptr) {
        return -1;
    }

    int32_t frame_size = read_int32_be(data, 0);

    // Validate frame size.
    if (frame_size < 0) {
        auto logger = get_codec_logger();
        logger->warn("peek_frame_size: negative frame size {}", frame_size);
        return -1;
    }

    if (frame_size > kMaxFrameSize) {
        auto logger = get_codec_logger();
        logger->warn("peek_frame_size: frame too large: {} (max {})",
                     frame_size, kMaxFrameSize);
        return -1;
    }

    // Minimum reasonable frame: at least a request header (10 bytes) or
    // a response header (4 bytes).
    if (frame_size < 4) {
        auto logger = get_codec_logger();
        logger->warn("peek_frame_size: frame too small: {} bytes", frame_size);
        return -1;
    }

    return frame_size;
}

// ============================================================================
// Optional: quick response correlation_id peek
// ============================================================================

/**
 * Internal utility: peek the correlation_id from a response frame.
 * For responses, the correlation_id is at offset 0 of the payload
 * (right after the 4-byte length prefix).
 *
 * This is the same offset as for requests (corr_id is at payload+4 in
 * both formats), so peek_correlation_id works for both.
 */

} // namespace torrent::network
