#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>

namespace torrent {

// Big-endian ↔ host byte order
uint16_t host_to_be16(uint16_t val) noexcept;
uint32_t host_to_be32(uint32_t val) noexcept;
uint64_t host_to_be64(uint64_t val) noexcept;
uint16_t be_to_host16(uint16_t val) noexcept;
uint32_t be_to_host32(uint32_t val) noexcept;
uint64_t be_to_host64(uint64_t val) noexcept;

// Little-endian ↔ host byte order
uint16_t host_to_le16(uint16_t val) noexcept;
uint32_t host_to_le32(uint32_t val) noexcept;
uint64_t host_to_le64(uint64_t val) noexcept;
uint16_t le_to_host16(uint16_t val) noexcept;
uint32_t le_to_host32(uint32_t val) noexcept;
uint64_t le_to_host64(uint64_t val) noexcept;

// ZigZag encoding (signed → unsigned for varint)
uint32_t zigzag_encode_32(int32_t val) noexcept;
uint64_t zigzag_encode_64(int64_t val) noexcept;
int32_t zigzag_decode_32(uint32_t val) noexcept;
int64_t zigzag_decode_64(uint64_t val) noexcept;

// Varint encoding (unsigned)
size_t varint_encode_32(uint32_t val, uint8_t* out) noexcept;
size_t varint_encode_64(uint64_t val, uint8_t* out) noexcept;
size_t varint_decode_32(const uint8_t* data, size_t max_len, uint32_t& out) noexcept;
size_t varint_decode_64(const uint8_t* data, size_t max_len, uint64_t& out) noexcept;

// Signed varint (ZigZag + varint)
size_t varint_encode_signed_32(int32_t val, uint8_t* out) noexcept;
size_t varint_encode_signed_64(int64_t val, uint8_t* out) noexcept;
size_t varint_decode_signed_32(const uint8_t* data, size_t max_len, int32_t& out) noexcept;
size_t varint_decode_signed_64(const uint8_t* data, size_t max_len, int64_t& out) noexcept;

// Raw I/O helpers (write to/read from byte buffers)
void write_int16_be(uint8_t* dst, int16_t val) noexcept;
void write_int32_be(uint8_t* dst, int32_t val) noexcept;
void write_int64_be(uint8_t* dst, int64_t val) noexcept;
int16_t read_int16_be(const uint8_t* src) noexcept;
int32_t read_int32_be(const uint8_t* src) noexcept;
int64_t read_int64_be(const uint8_t* src) noexcept;

} // namespace torrent
