#include "torrent/common/endian.h"
#include <cstring>
#include <bit>
#include <arpa/inet.h>

namespace torrent {

uint16_t host_to_be16(uint16_t val) noexcept {
    return htons(val);
}

uint32_t host_to_be32(uint32_t val) noexcept {
    return htonl(val);
}

uint64_t host_to_be64(uint64_t val) noexcept {
    if constexpr (std::endian::native == std::endian::big) {
        return val;
    } else {
        uint32_t high = static_cast<uint32_t>(val >> 32);
        uint32_t low = static_cast<uint32_t>(val & 0xFFFFFFFF);
        return (static_cast<uint64_t>(ntohl(low)) << 32) | static_cast<uint64_t>(ntohl(high));
    }
}

uint16_t be_to_host16(uint16_t val) noexcept {
    return ntohs(val);
}

uint32_t be_to_host32(uint32_t val) noexcept {
    return ntohl(val);
}

uint64_t be_to_host64(uint64_t val) noexcept {
    if constexpr (std::endian::native == std::endian::big) {
        return val;
    } else {
        uint32_t high = static_cast<uint32_t>(val >> 32);
        uint32_t low = static_cast<uint32_t>(val & 0xFFFFFFFF);
        return (static_cast<uint64_t>(ntohl(low)) << 32) | static_cast<uint64_t>(ntohl(high));
    }
}

uint16_t host_to_le16(uint16_t val) noexcept {
    if constexpr (std::endian::native == std::endian::little) {
        return val;
    } else {
        return __builtin_bswap16(val);
    }
}

uint32_t host_to_le32(uint32_t val) noexcept {
    if constexpr (std::endian::native == std::endian::little) {
        return val;
    } else {
        return __builtin_bswap32(val);
    }
}

uint64_t host_to_le64(uint64_t val) noexcept {
    if constexpr (std::endian::native == std::endian::little) {
        return val;
    } else {
        return __builtin_bswap64(val);
    }
}

uint16_t le_to_host16(uint16_t val) noexcept {
    if constexpr (std::endian::native == std::endian::little) {
        return val;
    } else {
        return __builtin_bswap16(val);
    }
}

uint32_t le_to_host32(uint32_t val) noexcept {
    if constexpr (std::endian::native == std::endian::little) {
        return val;
    } else {
        return __builtin_bswap32(val);
    }
}

uint64_t le_to_host64(uint64_t val) noexcept {
    if constexpr (std::endian::native == std::endian::little) {
        return val;
    } else {
        return __builtin_bswap64(val);
    }
}

// ============================================================================
// ZigZag encoding for signed integers (used in varint protocol)
// ============================================================================

uint32_t zigzag_encode_32(int32_t val) noexcept {
    return static_cast<uint32_t>((val << 1) ^ (val >> 31));
}

uint64_t zigzag_encode_64(int64_t val) noexcept {
    return static_cast<uint64_t>((val << 1) ^ (val >> 63));
}

int32_t zigzag_decode_32(uint32_t val) noexcept {
    return static_cast<int32_t>((val >> 1) ^ -(val & 1));
}

int64_t zigzag_decode_64(uint64_t val) noexcept {
    return static_cast<int64_t>((val >> 1) ^ -(val & 1));
}

// ============================================================================
// Varint encoding (unsigned, used in Kafka v2 record format)
// ============================================================================

size_t varint_encode_32(uint32_t val, uint8_t* out) noexcept {
    size_t written = 0;
    while (val >= 0x80) {
        out[written++] = static_cast<uint8_t>(val | 0x80);
        val >>= 7;
    }
    out[written++] = static_cast<uint8_t>(val);
    return written;
}

size_t varint_encode_64(uint64_t val, uint8_t* out) noexcept {
    size_t written = 0;
    while (val >= 0x80) {
        out[written++] = static_cast<uint8_t>(val | 0x80);
        val >>= 7;
    }
    out[written++] = static_cast<uint8_t>(val);
    return written;
}

size_t varint_decode_32(const uint8_t* data, size_t max_len,
                         uint32_t& out) noexcept {
    out = 0;
    size_t shift = 0;
    size_t read = 0;
    while (read < max_len) {
        uint8_t byte = data[read++];
        out |= static_cast<uint32_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) return read;
        shift += 7;
    }
    out = 0;
    return 0; // Underflow/overflow
}

size_t varint_decode_64(const uint8_t* data, size_t max_len,
                         uint64_t& out) noexcept {
    out = 0;
    size_t shift = 0;
    size_t read = 0;
    while (read < max_len) {
        uint8_t byte = data[read++];
        out |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) return read;
        shift += 7;
    }
    out = 0;
    return 0; // Underflow/overflow
}

// ============================================================================
// Signed varint (ZigZag-encoded varint)
// ============================================================================

size_t varint_encode_signed_32(int32_t val, uint8_t* out) noexcept {
    return varint_encode_32(zigzag_encode_32(val), out);
}

size_t varint_encode_signed_64(int64_t val, uint8_t* out) noexcept {
    return varint_encode_64(zigzag_encode_64(val), out);
}

size_t varint_decode_signed_32(const uint8_t* data, size_t max_len,
                                int32_t& out) noexcept {
    uint32_t uval = 0;
    size_t read = varint_decode_32(data, max_len, uval);
    if (read > 0) out = zigzag_decode_32(uval);
    return read;
}

size_t varint_decode_signed_64(const uint8_t* data, size_t max_len,
                                int64_t& out) noexcept {
    uint64_t uval = 0;
    size_t read = varint_decode_64(data, max_len, uval);
    if (read > 0) out = zigzag_decode_64(uval);
    return read;
}

// ============================================================================
// Raw byte read/write helpers
// ============================================================================

void write_int16_be(uint8_t* dst, int16_t val) noexcept {
    uint16_t net = host_to_be16(static_cast<uint16_t>(val));
    std::memcpy(dst, &net, sizeof(net));
}

void write_int32_be(uint8_t* dst, int32_t val) noexcept {
    uint32_t net = host_to_be32(static_cast<uint32_t>(val));
    std::memcpy(dst, &net, sizeof(net));
}

void write_int64_be(uint8_t* dst, int64_t val) noexcept {
    uint64_t net = host_to_be64(static_cast<uint64_t>(val));
    std::memcpy(dst, &net, sizeof(net));
}

int16_t read_int16_be(const uint8_t* src) noexcept {
    uint16_t net = 0;
    std::memcpy(&net, src, sizeof(net));
    return static_cast<int16_t>(be_to_host16(net));
}

int32_t read_int32_be(const uint8_t* src) noexcept {
    uint32_t net = 0;
    std::memcpy(&net, src, sizeof(net));
    return static_cast<int32_t>(be_to_host32(net));
}

int64_t read_int64_be(const uint8_t* src) noexcept {
    uint64_t net = 0;
    std::memcpy(&net, src, sizeof(net));
    return static_cast<int64_t>(be_to_host64(net));
}

} // namespace torrent
