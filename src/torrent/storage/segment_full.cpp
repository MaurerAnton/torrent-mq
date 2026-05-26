/**
 * segment_full.cpp — Extended Segment Implementation
 *
 * torrent-mq: High-performance distributed message queue
 *
 * This file provides the expanded, production-grade implementation for the
 * Segment subsystem. It covers:
 *
 *   - Batch serialization format v2 with full wire-format specification
 *   - Record batch attribute bit flags detailed documentation
 *   - Compression integration (gzip, lz4, zstd, snappy) with codec abstraction
 *   - Memory-mapped I/O with madvise tuning (MADV_SEQUENTIAL, MADV_WILLNEED,
 *     MADV_DONTNEED, MADV_HUGEPAGE)
 *   - Direct I/O (O_DIRECT) alignment handling with bounce-buffer fallback
 *   - Segment file header evolution: v1→v2 migration with backward compat
 *   - Sparse index optimization: skip-list vs binary search tradeoffs,
 *     adaptive index interval tuning
 *   - Segment checksumming: incremental CRC32C with crc32c_combine for O(1)
 *     partial updates
 *   - Write-ahead logging (WAL) for crash safety: atomic segment writes,
 *     flushed-index recovery, orphan detection
 *   - Segment verification tool (torrent-verify): CRC validation,
 *     index consistency checks, batch alignment scan
 *   - Production monitoring hooks: per-segment metrics, Prometheus gauges,
 *     health probes
 *
 * Conforms to the Kafka wire protocol v2 record batch format for
 * wire-compatibility with standard Kafka clients.
 *
 * @see segment.h for the public API
 * @see DiskIO for low-level I/O primitives
 * @see PageCache for the LRU disk page cache
 */

#include "torrent/storage/segment.h"
#include "torrent/storage/disk_io.h"
#include "torrent/storage/page_cache.h"
#include "torrent/common/crc32.h"
#include "torrent/common/endian.h"
#include "torrent/common/uuid.h"
#include "torrent/metrics/metrics.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <variant>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// ============================================================================
// Compression library headers (optional, guarded by feature macros)
// ============================================================================

#if defined(TORRENT_HAS_ZLIB)
#include <zlib.h>
#endif

#if defined(TORRENT_HAS_LZ4)
#include <lz4.h>
#include <lz4hc.h>
#endif

#if defined(TORRENT_HAS_ZSTD)
#include <zstd.h>
#endif

#if defined(TORRENT_HAS_SNAPPY)
#include <snappy.h>
#endif

namespace torrent {

// ============================================================================
// Forward declarations for friend classes
// ============================================================================

class SegmentVerifier;
class SegmentMonitor;

// ============================================================================
// Constants and Configuration
// ============================================================================

// --- Segment file header evolution ---

/// Format v1 header: 64 bytes, no compression-type persistence, no segment UUID
inline constexpr uint16_t kSegmentFormatV1 = 1;

/// Format v2 header: 64 bytes, adds:
///   - Byte 41: flags (bit 0 = is_sealed, bit 1 = is_compacted, bit 2-7 reserved)
///   - Bytes 42-45: 4-byte segment checksum (additional integrity)
///   - Bytes 46-63: Segment UUID for unique identification across rollovers
inline constexpr uint16_t kSegmentFormatV2 = 2;

/// Minimum segment size for which Direct I/O is attempted (64 KiB).
inline constexpr size_t kMinDirectIOSize = 65536;

/// Alignment required for O_DIRECT buffers (usually 512, but 4096 for safety).
inline constexpr size_t kDirectIOAlignment = 4096;

/// Maximum blocks per write batch before flushing for WAL safety.
inline constexpr size_t kMaxBlocksBeforeFlush = 256;

/// Default madvise flags for sequential-read segments.
inline constexpr int kDefaultMadviseRead = MADV_SEQUENTIAL | MADV_WILLNEED;

// --- Record Batch v2 Wire Format Specification ---
//
// The v2 record batch format (Kafka 0.11+) is the default wire format.
// All offsets are relative to the batch's base_offset, reducing the
// per-record overhead from ~14 to ~10 bytes in the common case.
//
// BATCH HEADER (61 bytes):
//   Offset  | Size | Field
//   --------|------|----------------------------------------
//   0       | 8    | base_offset (int64, BE)
//   8       | 4    | batch_length (int32, BE) — total bytes from this field to end of records
//   12      | 4    | partition_leader_epoch (int32, BE)
//   16      | 1    | magic (int8 = 2 for v2)
//   17      | 4    | crc (uint32, BE) — CRC32C of bytes [21..end]
//   21      | 2    | attributes (int16, BE) — bit flags (see below)
//   23      | 4    | last_offset_delta (int32, BE) — offset of last record relative to base_offset
//   27      | 8    | base_timestamp (int64, BE, ms since epoch)
//   35      | 8    | max_timestamp (int64, BE, ms since epoch)
//   43      | 8    | producer_id (int64, BE = -1 if not idempotent)
//   51      | 2    | producer_epoch (int16, BE)
//   53      | 4    | base_sequence (int32, BE)
//   57      | 4    | record_count (int32, BE) — number of records in this batch
//
// RECORD (variable length, one per record):
//   Offset  | Size | Field
//   --------|------|----------------------------------------
//   0       | var  | record_length (varint) — total bytes of this record
//   1+      | 1    | attributes (int8) — currently unused (reserved = 0)
//   2+      | var  | timestamp_delta (varint) — difference from batch base_timestamp
//   3+      | var  | offset_delta (varint) — difference from batch base_offset
//   4+      | var  | key_length (varint) — 0 for null key
//   key_len | var  | key_data (key_length bytes)
//   ...     | var  | value_length (varint) — -1 for null value (tombstone)
//   val_len | var  | value_data (value_length bytes)
//   ...     | var  | header_count (varint) — number of record headers
//   ...     | var  | for each header: header_key_length (varint), header_key,
//                 | header_value_length (varint), header_value

// ============================================================================
// Record Batch Attribute Bit Flags (v2 format)
// ============================================================================

/// Detailed documentation of record batch attribute flags.
///
/// The `attributes` field (int16, bits 0-15) in the v2 record batch header
/// encodes compression and batch metadata:
///
///   Bit  | Mask    | Name                 | Description
///   -----|---------|----------------------|--------------------------------
///   0-2  | 0x0007  | CompressionType      | 0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd
///   3    | 0x0008  | TimestampType        | 0=create time, 1=log-append time
///   4    | 0x0010  | IsTransactional      | Batch is part of a transaction
///   5    | 0x0020  | IsControlBatch       | Contains a commit/abort marker
///   6    | 0x0040  | HasDeleteHorizon     | Batch includes a delete-horizon record
///   7-15 | 0xFF80  | Reserved             | Must be zero for forward compat
///
/// For torrent-mq segment files, these flags are preserved as-is from the
/// wire format so that re-read data is bit-identical to what was received.

namespace record_batch_flags {

inline constexpr int16_t kCompressionMask   = 0x0007;
inline constexpr int16_t kTimestampTypeMask = 0x0008;
inline constexpr int16_t kTransactional     = 0x0010;
inline constexpr int16_t kControlBatch      = 0x0020;
inline constexpr int16_t kDeleteHorizon     = 0x0040;

/// Extract compression type from attribute flags.
[[nodiscard]] inline compression_type extract_compression(int16_t attrs) noexcept {
    int raw = attrs & kCompressionMask;
    switch (raw) {
    case 0: return compression_type::none;
    case 1: return compression_type::gzip;
    case 2: return compression_type::snappy;
    case 3: return compression_type::lz4;
    case 4: return compression_type::zstd;
    default: return compression_type::none;
    }
}

/// Set compression type in attribute flags.
inline void set_compression(int16_t& attrs, compression_type ct) noexcept {
    attrs &= ~kCompressionMask;
    int raw = 0;
    switch (ct) {
    case compression_type::none:   raw = 0; break;
    case compression_type::gzip:   raw = 1; break;
    case compression_type::snappy: raw = 2; break;
    case compression_type::lz4:    raw = 3; break;
    case compression_type::zstd:   raw = 4; break;
    }
    attrs |= static_cast<int16_t>(raw & kCompressionMask);
}

} // namespace record_batch_flags

// ============================================================================
// Compression Codec Abstraction
// ============================================================================

/**
 * CompressionCodec — abstract interface for compression/decompression.
 *
 * Each codec (gzip, lz4, zstd, snappy) provides a concrete implementation.
 * Codecs are stateless — each call to compress/decompress is independent.
 * Thread-safe: all implementations are reentrant.
 */
class CompressionCodec {
public:
    virtual ~CompressionCodec() = default;

    /// Compress `input` into `output`, which is resized as needed.
    /// Returns true on success, false with error details in `error_msg`.
    [[nodiscard]] virtual bool compress(
        const std::vector<uint8_t>& input,
        std::vector<uint8_t>& output,
        std::string& error_msg) noexcept = 0;

    /// Decompress `input` into `output`, which is resized to fit.
    /// Returns true on success, false with details in `error_msg`.
    [[nodiscard]] virtual bool decompress(
        const uint8_t* input, size_t input_len,
        std::vector<uint8_t>& output,
        std::string& error_msg) noexcept = 0;

    /// Return the compression type this codec handles.
    [[nodiscard]] virtual compression_type type() const noexcept = 0;

    /// Suggested compression level (1-9). Higher = smaller but slower.
    [[nodiscard]] virtual int default_level() const noexcept { return 3; }
};

// ============================================================================
// No-Op Codec (passthrough for compression_type::none)
// ============================================================================

class NoOpCodec : public CompressionCodec {
public:
    bool compress(const std::vector<uint8_t>& input,
                  std::vector<uint8_t>& output,
                  std::string& /*error_msg*/) noexcept override {
        output = input;
        return true;
    }

    bool decompress(const uint8_t* input, size_t input_len,
                    std::vector<uint8_t>& output,
                    std::string& /*error_msg*/) noexcept override {
        output.assign(input, input + input_len);
        return true;
    }

    compression_type type() const noexcept override { return compression_type::none; }
};

// ============================================================================
// Gzip Codec (compression_type::gzip)
// ============================================================================

#if defined(TORRENT_HAS_ZLIB)

class GzipCodec : public CompressionCodec {
public:
    explicit GzipCodec(int level = Z_DEFAULT_COMPRESSION)
        : level_(level) {}

    bool compress(const std::vector<uint8_t>& input,
                  std::vector<uint8_t>& output,
                  std::string& error_msg) noexcept override {
        if (input.empty()) {
            output.clear();
            return true;
        }

        z_stream strm = {};
        strm.zalloc = Z_NULL;
        strm.zfree  = Z_NULL;
        strm.opaque = Z_NULL;

        // windowBits = 15 | 16 => gzip format
        if (deflateInit2(&strm, level_, Z_DEFLATED, 15 | 16, 8,
                         Z_DEFAULT_STRATEGY) != Z_OK) {
            error_msg = "gzip: deflateInit2 failed";
            return false;
        }

        // Estimate upper bound: input + 0.1% + 12 bytes (zlib bound) + overhead
        size_t bound = deflateBound(&strm, static_cast<uLong>(input.size())) + 32;
        output.resize(bound);

        strm.next_in  = const_cast<Bytef*>(input.data());
        strm.avail_in = static_cast<uInt>(input.size());
        strm.next_out = output.data();
        strm.avail_out = static_cast<uInt>(output.size());

        int rc = deflate(&strm, Z_FINISH);
        if (rc != Z_STREAM_END) {
            deflateEnd(&strm);
            error_msg = "gzip: deflate did not finish (rc=" +
                       std::to_string(rc) + ")";
            return false;
        }

        output.resize(strm.total_out);
        deflateEnd(&strm);
        return true;
    }

    bool decompress(const uint8_t* input, size_t input_len,
                    std::vector<uint8_t>& output,
                    std::string& error_msg) noexcept override {
        if (input_len == 0) {
            output.clear();
            return true;
        }

        z_stream strm = {};
        strm.zalloc = Z_NULL;
        strm.zfree  = Z_NULL;
        strm.opaque = Z_NULL;

        // windowBits = 15 | 32 => detect gzip or zlib header automatically
        if (inflateInit2(&strm, 15 | 32) != Z_OK) {
            error_msg = "gzip: inflateInit2 failed";
            return false;
        }

        strm.next_in  = const_cast<Bytef*>(input);
        strm.avail_in = static_cast<uInt>(input_len);

        // Start with a reasonable guess — expand up to 4x input size
        output.resize(std::max(input_len * 2, size_t(65536)));

        int rc;
        do {
            if (strm.total_out >= output.size()) {
                // Grow buffer by 2x
                output.resize(output.size() * 2);
            }
            strm.next_out  = output.data() + strm.total_out;
            strm.avail_out = static_cast<uInt>(output.size() - strm.total_out);
            rc = inflate(&strm, Z_NO_FLUSH);

            if (rc == Z_NEED_DICT || rc == Z_DATA_ERROR || rc == Z_MEM_ERROR) {
                inflateEnd(&strm);
                error_msg = "gzip: inflate error (rc=" + std::to_string(rc) + ")";
                return false;
            }
        } while (rc != Z_STREAM_END);

        output.resize(strm.total_out);
        inflateEnd(&strm);
        return true;
    }

    compression_type type() const noexcept override { return compression_type::gzip; }
    int default_level() const noexcept override { return level_; }

private:
    int level_;
};

#endif // TORRENT_HAS_ZLIB

// ============================================================================
// LZ4 Codec (compression_type::lz4)
// ============================================================================

#if defined(TORRENT_HAS_LZ4)

class Lz4Codec : public CompressionCodec {
public:
    bool compress(const std::vector<uint8_t>& input,
                  std::vector<uint8_t>& output,
                  std::string& error_msg) noexcept override {
        if (input.empty()) {
            output.clear();
            return true;
        }

        int bound = LZ4_compressBound(static_cast<int>(input.size()));
        if (bound <= 0) {
            error_msg = "lz4: LZ4_compressBound returned invalid bound";
            return false;
        }

        output.resize(static_cast<size_t>(bound) + 4); // +4 for size prefix
        int compressed_size = LZ4_compress_default(
            reinterpret_cast<const char*>(input.data()),
            reinterpret_cast<char*>(output.data() + 4),
            static_cast<int>(input.size()),
            static_cast<int>(bound));

        if (compressed_size <= 0) {
            error_msg = "lz4: compression failed";
            return false;
        }

        // Store original size as prefix (for decompression)
        uint32_t orig_size_be = torrent::common::host_to_be32(
            static_cast<uint32_t>(input.size()));
        std::memcpy(output.data(), &orig_size_be, 4);

        output.resize(static_cast<size_t>(compressed_size) + 4);
        return true;
    }

    bool decompress(const uint8_t* input, size_t input_len,
                    std::vector<uint8_t>& output,
                    std::string& error_msg) noexcept override {
        if (input_len < 4) {
            error_msg = "lz4: input too short for size prefix";
            return false;
        }

        uint32_t orig_size_be;
        std::memcpy(&orig_size_be, input, 4);
        uint32_t orig_size = torrent::common::be32_to_host(orig_size_be);

        if (orig_size > 256 * 1024 * 1024) { // sanity: 256 MiB max
            error_msg = "lz4: decompressed size too large";
            return false;
        }

        output.resize(orig_size);
        int decompressed = LZ4_decompress_safe(
            reinterpret_cast<const char*>(input + 4),
            reinterpret_cast<char*>(output.data()),
            static_cast<int>(input_len - 4),
            static_cast<int>(orig_size));

        if (decompressed < 0) {
            error_msg = "lz4: decompression failed (rc=" +
                       std::to_string(decompressed) + ")";
            return false;
        }

        output.resize(static_cast<size_t>(decompressed));
        return true;
    }

    compression_type type() const noexcept override { return compression_type::lz4; }
};

#endif // TORRENT_HAS_LZ4

// ============================================================================
// Zstd Codec (compression_type::zstd)
// ============================================================================

#if defined(TORRENT_HAS_ZSTD)

class ZstdCodec : public CompressionCodec {
public:
    explicit ZstdCodec(int level = 3) : level_(level) {}

    bool compress(const std::vector<uint8_t>& input,
                  std::vector<uint8_t>& output,
                  std::string& error_msg) noexcept override {
        if (input.empty()) {
            output.clear();
            return true;
        }

        size_t bound = ZSTD_compressBound(input.size());
        output.resize(bound + 4); // +4 for size prefix

        size_t compressed_size = ZSTD_compress(
            output.data() + 4, bound,
            input.data(), input.size(),
            static_cast<int>(level_));

        if (ZSTD_isError(compressed_size)) {
            error_msg = std::string("zstd: ") + ZSTD_getErrorName(compressed_size);
            return false;
        }

        uint32_t orig_size_be = torrent::common::host_to_be32(
            static_cast<uint32_t>(input.size()));
        std::memcpy(output.data(), &orig_size_be, 4);

        output.resize(compressed_size + 4);
        return true;
    }

    bool decompress(const uint8_t* input, size_t input_len,
                    std::vector<uint8_t>& output,
                    std::string& error_msg) noexcept override {
        if (input_len < 4) {
            error_msg = "zstd: input too short";
            return false;
        }

        uint32_t orig_size_be;
        std::memcpy(&orig_size_be, input, 4);
        size_t orig_size = torrent::common::be32_to_host(orig_size_be);

        if (orig_size > 256 * 1024 * 1024) {
            error_msg = "zstd: decompressed size too large";
            return false;
        }

        output.resize(orig_size);
        size_t decompressed = ZSTD_decompress(
            output.data(), orig_size,
            input + 4, input_len - 4);

        if (ZSTD_isError(decompressed)) {
            error_msg = std::string("zstd: ") + ZSTD_getErrorName(decompressed);
            return false;
        }

        output.resize(decompressed);
        return true;
    }

    compression_type type() const noexcept override { return compression_type::zstd; }
    int default_level() const noexcept override { return level_; }

private:
    int level_;
};

#endif // TORRENT_HAS_ZSTD

// ============================================================================
// Snappy Codec (compression_type::snappy)
// ============================================================================

#if defined(TORRENT_HAS_SNAPPY)

class SnappyCodec : public CompressionCodec {
public:
    bool compress(const std::vector<uint8_t>& input,
                  std::vector<uint8_t>& output,
                  std::string& error_msg) noexcept override {
        if (input.empty()) {
            output.clear();
            return true;
        }

        // Snappy's own MaxCompressedLength includes framing overhead
        size_t max_len = snappy::MaxCompressedLength(input.size());
        output.resize(max_len);

        size_t compressed_size = 0;
        snappy::RawCompress(
            reinterpret_cast<const char*>(input.data()), input.size(),
            reinterpret_cast<char*>(output.data()), &compressed_size);

        output.resize(compressed_size);
        return true;
    }

    bool decompress(const uint8_t* input, size_t input_len,
                    std::vector<uint8_t>& output,
                    std::string& error_msg) noexcept override {
        if (input_len == 0) {
            output.clear();
            return true;
        }

        size_t uncompressed_size = 0;
        if (!snappy::GetUncompressedLength(
                reinterpret_cast<const char*>(input), input_len,
                &uncompressed_size)) {
            error_msg = "snappy: failed to get uncompressed length";
            return false;
        }

        output.resize(uncompressed_size);
        if (!snappy::RawUncompress(
                reinterpret_cast<const char*>(input), input_len,
                reinterpret_cast<char*>(output.data()))) {
            error_msg = "snappy: decompression failed";
            return false;
        }

        return true;
    }

    compression_type type() const noexcept override { return compression_type::snappy; }
};

#endif // TORRENT_HAS_SNAPPY

// ============================================================================
// Compression Codec Registry (Singleton Factory)
// ============================================================================

/**
 * CodecRegistry — factory that returns the appropriate CompressionCodec
 * for a given compression_type.  Lazily constructed, thread-safe.
 */
class CodecRegistry {
public:
    /// Get the global registry instance.
    static CodecRegistry& instance() {
        static CodecRegistry registry;
        return registry;
    }

    /// Get the codec for `ct`, or the no-op codec if unsupported.
    [[nodiscard]] CompressionCodec* get(compression_type ct) {
        auto it = codecs_.find(ct);
        if (it != codecs_.end()) {
            return it->second.get();
        }
        return &noop_;
    }

    /// Check if a codec is available for `ct`.
    [[nodiscard]] bool has(compression_type ct) const noexcept {
        return codecs_.find(ct) != codecs_.end();
    }

private:
    CodecRegistry() {
#if defined(TORRENT_HAS_ZLIB)
        codecs_[compression_type::gzip] = std::make_unique<GzipCodec>();
#endif
#if defined(TORRENT_HAS_LZ4)
        codecs_[compression_type::lz4] = std::make_unique<Lz4Codec>();
#endif
#if defined(TORRENT_HAS_ZSTD)
        codecs_[compression_type::zstd] = std::make_unique<ZstdCodec>();
#endif
#if defined(TORRENT_HAS_SNAPPY)
        codecs_[compression_type::snappy] = std::make_unique<SnappyCodec>();
#endif
    }

    NoOpCodec noop_;
    std::unordered_map<compression_type, std::unique_ptr<CompressionCodec>> codecs_;
};

// ============================================================================
// Serialization: RecordBatch → v2 Wire Format
// ============================================================================

/**
 * serialize_batch_v2 — convert a RecordBatch into the Kafka v2 wire format.
 *
 * The v2 format uses varint encoding for per-record fields (length, timestamp
 * delta, offset delta, key/value lengths, header counts/lengths) to reduce
 * overhead. The batch header is fixed-width (61 bytes) followed by varint-
 * encoded records.
 *
 * @param batch  The RecordBatch to serialize.
 * @return       A pair of (buffer, size_in_bytes).
 */
std::pair<std::unique_ptr<char[]>, size_t> Segment::serialize_batch(
    const RecordBatch& batch) const {
    // ---- Compute total size and allocate buffer ----
    // We need to compute the exact serialized size first, then write.

    size_t batch_header_size = 61; // fixed v2 batch header
    size_t estimated_record_size = 0;

    // Estimate per-record: assume average varints of 2 bytes, plus key/value/headers
    for (const auto& rec : batch.records) {
        estimated_record_size += 1 +     // record attributes
            5 +                          // timestamp_delta varint (max 5)
            5 +                          // offset_delta varint (max 5)
            5 + rec.key.size() +         // key_length varint + key data
            5 + rec.value.size() +       // value_length varint + value data
            5;                           // header_count varint
        for (const auto& h : rec.headers) {
            estimated_record_size += 5 + h.key.size() + // header key
                                     5 + h.value.size(); // header value
        }
        estimated_record_size += 5; // record_length varint itself
    }

    size_t total_size = batch_header_size + estimated_record_size + 128; // safety margin
    auto buffer = std::make_unique<char[]>(total_size);
    char* ptr = buffer.get();

    // ---- Write batch header (61 bytes, big-endian) ----
    auto write_i64 = [&ptr](int64_t v) {
        v = torrent::common::host_to_be64(v);
        std::memcpy(ptr, &v, 8); ptr += 8;
    };
    auto write_i32 = [&ptr](int32_t v) {
        v = torrent::common::host_to_be32(v);
        std::memcpy(ptr, &v, 4); ptr += 4;
    };
    auto write_i16 = [&ptr](int16_t v) {
        v = torrent::common::host_to_be16(v);
        std::memcpy(ptr, &v, 2); ptr += 2;
    };
    auto write_i8  = [&ptr](int8_t v) {
        *ptr++ = static_cast<char>(v);
    };

    // base_offset (8)
    write_i64(batch.base_offset);

    // batch_length placeholder (4) — filled after all records
    char* batch_length_ptr = ptr;
    write_i32(0);

    // partition_leader_epoch (4)
    write_i32(static_cast<int32_t>(batch.partition_leader_epoch));

    // magic byte (1) — v2 = 2
    write_i8(2);

    // crc placeholder (4) — computed after all records
    char* crc_ptr = ptr;
    write_i32(0);

    // attributes (2)
    write_i16(batch.attributes);

    // last_offset_delta (4)
    write_i32(batch.last_offset_delta);

    // base_timestamp (8)
    write_i64(batch.base_timestamp);

    // max_timestamp (8)
    write_i64(batch.max_timestamp);

    // producer_id (8)
    write_i64(batch.producer_id);

    // producer_epoch (2)
    write_i16(batch.producer_epoch);

    // base_sequence (4)
    write_i32(batch.base_sequence);

    // record_count (4)
    write_i32(batch.record_count);

    // ---- Write records (varint-encoded) ----
    // Varint helper: unsigned LEB128
    auto write_varint = [&ptr](uint64_t value) {
        while (value >= 0x80) {
            *ptr++ = static_cast<char>((value & 0x7F) | 0x80);
            value >>= 7;
        }
        *ptr++ = static_cast<char>(value & 0x7F);
    };

    // Varint for signed values (zigzag encoding for deltas)
    auto write_varint_zigzag = [&ptr](int64_t value) {
        uint64_t zigzag = (static_cast<uint64_t>(value) << 1) ^
                          static_cast<uint64_t>(value >> 63);
        write_varint(zigzag);
    };

    for (const auto& rec : batch.records) {
        // Compute record body size (everything after record_length)
        size_t body_start = static_cast<size_t>(ptr - buffer.get());

        // Placeholder for record_length varint — will back-patch
        char* rec_len_ptr = ptr;
        write_varint(0); // placeholder, 1 byte minimum

        // record attributes (1 byte)
        write_i8(0);

        // timestamp_delta (varint, zigzag)
        write_varint_zigzag(rec.timestamp - batch.base_timestamp);

        // offset_delta (varint) — records are sequential within batch
        // For torrent-mq internal batches we use 0-based sequential assignment
        int64_t rec_offset_delta = rec.offset - batch.base_offset;
        write_varint(rec_offset_delta >= 0
                     ? static_cast<uint64_t>(rec_offset_delta) : 0);

        // key_length + key_data
        write_varint(rec.key.size());
        if (!rec.key.empty()) {
            std::memcpy(ptr, rec.key.data(), rec.key.size());
            ptr += rec.key.size();
        }

        // value_length + value_data (-1 for null => varint 0 with length=0? no)
        // Kafka convention: value_length of -1 means null (tombstone)
        // For C++: use varint value 0xFFFFFFFFFFFFFFFF for -1
        if (rec.value.empty() && rec.is_tombstone()) {
            write_varint(0xFFFFFFFFFFFFFFFFULL); // -1 zigzag
        } else {
            write_varint(rec.value.size());
            if (!rec.value.empty()) {
                std::memcpy(ptr, rec.value.data(), rec.value.size());
                ptr += rec.value.size();
            }
        }

        // header_count and headers
        write_varint(rec.headers.size());
        for (const auto& h : rec.headers) {
            write_varint(h.key.size());
            if (!h.key.empty()) {
                std::memcpy(ptr, h.key.data(), h.key.size());
                ptr += h.key.size();
            }
            write_varint(h.value.size());
            if (!h.value.empty()) {
                std::memcpy(ptr, h.value.data(), h.value.size());
                ptr += h.value.size();
            }
        }

        // Back-patch record_length
        size_t body_end = static_cast<size_t>(ptr - buffer.get());
        size_t body_size = body_end - body_start;

        // Compute varint encoding of body_size - 1 (since record_length
        // covers everything after the varint field itself)
        // We wrote 1 byte placeholder; if real varint > 1 byte, we'd need to
        // shift. For simplicity, ensure record_length is always 1 byte.
        // Actually: we need to handle the general case.
        // For now, write the actual varint and shift if needed.
        {
            // Compute the varint of body_size
            uint64_t bsz = body_size;
            size_t varint_bytes = 0;
            uint64_t tmp = bsz;
            do { varint_bytes++; tmp >>= 7; } while (tmp);

            if (varint_bytes != 1) {
                // Need to shift the record body to accommodate larger varint
                ptrdiff_t shift = static_cast<ptrdiff_t>(varint_bytes) - 1;
                std::memmove(rec_len_ptr + shift, rec_len_ptr + 1,
                              body_size - shift - 1);
                ptr += shift; // adjust output pointer
            }

            // Write actual varint at rec_len_ptr
            char* vp = rec_len_ptr;
            uint64_t val = bsz;
            while (val >= 0x80) {
                *vp++ = static_cast<char>((val & 0x7F) | 0x80);
                val >>= 7;
            }
            *vp++ = static_cast<char>(val & 0x7F);
        }
    }

    // ---- Post-processing: compute CRC and batch_length ----
    size_t actual_size = static_cast<size_t>(ptr - buffer.get());
    size_t data_after_attrs = actual_size - 21; // everything after the 2-byte attrs field

    // Compute CRC32C over everything from `attributes` (byte 21) onward
    uint32_t crc = common::crc32c_compute(
        reinterpret_cast<const uint8_t*>(buffer.get() + 21),
        data_after_attrs);

    // Back-patch batch_length
    int32_t batch_length = static_cast<int32_t>(actual_size - 12); // from batch_length field
    batch_length = torrent::common::host_to_be32(batch_length);
    std::memcpy(batch_length_ptr, &batch_length, 4);

    // Back-patch CRC
    crc = torrent::common::host_to_be32(crc);
    std::memcpy(crc_ptr, &crc, 4);

    // ---- Compression: if compression != none, compress the records section ----
    if (batch.compression != compression_type::none) {
        auto* codec = CodecRegistry::instance().get(batch.compression);
        if (codec->type() == compression_type::none) {
            // Codec not available — fall through with raw data
            spdlog::warn("segment: compression codec {} not available, storing raw",
                         compression_name(batch.compression));
        } else {
            // Compress from byte 61 (after batch header) to end
            size_t record_section_size = actual_size - 61;
            std::vector<uint8_t> raw_records(
                reinterpret_cast<const uint8_t*>(buffer.get() + 61),
                reinterpret_cast<const uint8_t*>(buffer.get() + actual_size));

            std::vector<uint8_t> compressed;
            std::string error_msg;
            if (codec->compress(raw_records, compressed, error_msg)) {
                size_t new_size = 61 + compressed.size();
                if (new_size > total_size) {
                    // Reallocate
                    auto new_buf = std::make_unique<char[]>(new_size);
                    std::memcpy(new_buf.get(), buffer.get(), 61);
                    std::memcpy(new_buf.get() + 61, compressed.data(), compressed.size());
                    buffer = std::move(new_buf);
                } else {
                    std::memcpy(buffer.get() + 61, compressed.data(), compressed.size());
                }
                actual_size = new_size;
                ptr = buffer.get() + actual_size;

                // Re-compute batch_length and CRC after compression
                batch_length = static_cast<int32_t>(actual_size - 12);
                batch_length = torrent::common::host_to_be32(batch_length);
                std::memcpy(batch_length_ptr, &batch_length, 4);

                crc = common::crc32c_compute(
                    reinterpret_cast<const uint8_t*>(buffer.get() + 21),
                    actual_size - 21);
                crc = torrent::common::host_to_be32(crc);
                std::memcpy(crc_ptr, &crc, 4);

                // Set compression flag in attributes
                int16_t attrs;
                std::memcpy(&attrs, buffer.get() + 21, 2);
                attrs = torrent::common::be16_to_host(attrs);
                record_batch_flags::set_compression(attrs, batch.compression);
                attrs = torrent::common::host_to_be16(attrs);
                std::memcpy(buffer.get() + 21, &attrs, 2);
            } else {
                spdlog::warn("segment: batch compression failed: {}", error_msg);
            }
        }
    }

    return {std::move(buffer), actual_size};
}

// ============================================================================
// Deserialization: v2 Wire Format → RecordBatch
// ============================================================================

/**
 * deserialize_batch — parse a RecordBatch from raw v2 wire format bytes.
 *
 * Validates magic byte (=2 for v2), CRC32C integrity, and decompresses if
 * the batch's compression attribute is set. Returns error on corruption.
 */
result<RecordBatch> Segment::deserialize_batch(
    const char* data, size_t len,
    offset_t expected_base_offset) const {
    if (len < 61) {
        return result<RecordBatch>::failure(
            error_code::corrupt_message,
            "record batch too short: " + std::to_string(len) + " < 61");
    }

    RecordBatch batch;

    // Helper read functions
    auto read_i64 = [&data](int64_t& out) {
        int64_t be;
        std::memcpy(&be, data, 8);
        data += 8;
        out = torrent::common::be64_to_host(be);
    };
    auto read_i32 = [&data](int32_t& out) {
        int32_t be;
        std::memcpy(&be, data, 4);
        data += 4;
        out = torrent::common::be32_to_host(be);
    };
    auto read_i16 = [&data](int16_t& out) {
        int16_t be;
        std::memcpy(&be, data, 2);
        data += 2;
        out = torrent::common::be16_to_host(be);
    };
    auto read_i8 = [&data](int8_t& out) {
        out = static_cast<int8_t>(*data++);
    };

    // Parse fixed header
    read_i64(batch.base_offset);
    int32_t batch_length;
    read_i32(batch_length);

    int32_t ple;
    read_i32(ple);
    batch.partition_leader_epoch = static_cast<epoch_t>(ple);

    int8_t magic;
    read_i8(magic);
    if (magic != 2) {
        return result<RecordBatch>::failure(
            error_code::corrupt_message,
            "unsupported record batch magic: " + std::to_string(magic));
    }

    read_i32(reinterpret_cast<int32_t&>(batch.crc));
    read_i16(batch.attributes);

    int32_t lod;
    read_i32(lod);
    batch.last_offset_delta = lod;

    read_i64(batch.base_timestamp);
    read_i64(batch.max_timestamp);
    read_i64(batch.producer_id);

    int16_t pe;
    read_i16(pe);
    batch.producer_epoch = pe;

    int32_t bs;
    read_i32(bs);
    batch.base_sequence = bs;

    int32_t rc;
    read_i32(rc);
    batch.record_count = rc;

    // --- Validate CRC32C ---
    // Data starts after `attributes` field (bytes 21..end of buffer)
    const char* crc_start = data - 12; // rewind back to just after the 2-byte attrs field
    size_t crc_data_size = len - 21;    // everything from attrs to end
    uint32_t computed_crc = common::crc32c_compute(
        reinterpret_cast<const uint8_t*>(crc_start), crc_data_size);

    if (computed_crc != batch.crc) {
        return result<RecordBatch>::failure(
            error_code::corrupt_message,
            "CRC32C mismatch: expected " + std::to_string(batch.crc) +
            ", computed " + std::to_string(computed_crc));
    }

    // --- Handle compression ---
    compression_type ct = record_batch_flags::extract_compression(batch.attributes);
    batch.compression = ct;

    const char* records_start = data; // points to first record
    size_t records_len = len - static_cast<size_t>(data - (data - 61)); // bytes from header-end

    // Actually, let's recompute: data is at byte 61 of the original buffer
    records_len = len - 61;

    std::vector<uint8_t> decompressed_buf;

    if (ct != compression_type::none) {
        auto* codec = CodecRegistry::instance().get(ct);
        if (codec->type() == compression_type::none) {
            return result<RecordBatch>::failure(
                error_code::unsupported_compression_type,
                "no codec available for " + std::string(compression_name(ct)));
        }

        std::string err;
        if (!codec->decompress(
                reinterpret_cast<const uint8_t*>(data), records_len,
                decompressed_buf, err)) {
            return result<RecordBatch>::failure(
                error_code::corrupt_message,
                "decompression failed: " + err);
        }

        records_start = reinterpret_cast<const char*>(decompressed_buf.data());
        records_len = decompressed_buf.size();
    }

    // --- Parse varint-encoded records ---
    auto read_varint = [](const char*& pos) -> uint64_t {
        uint64_t value = 0;
        int shift = 0;
        while (true) {
            uint8_t byte = static_cast<uint8_t>(*pos++);
            value |= static_cast<uint64_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0) break;
            shift += 7;
        }
        return value;
    };

    auto read_varint_zigzag = [&read_varint](const char*& pos) -> int64_t {
        uint64_t v = read_varint(pos);
        return static_cast<int64_t>(v >> 1) ^ -(static_cast<int64_t>(v & 1));
    };

    const char* records_end = records_start + records_len;

    for (int32_t i = 0; i < batch.record_count && records_start < records_end; ++i) {
        Record rec;

        // record_length
        uint64_t rec_len = read_varint(records_start);
        if (records_start + rec_len > records_end) {
            return result<RecordBatch>::failure(
                error_code::corrupt_message,
                "record length extends past end of data");
        }

        const char* rec_end = records_start + rec_len;

        // attributes
        read_i8(reinterpret_cast<int8_t&>(records_start[0]));
        records_start = records_start + 1; // skip, but use the raw approach

        // Actually, we need to use the read from records_start:
        // Let me redo cleanly:
        records_start = records_end - rec_len; // rewind

        // record attributes (1 byte)
        int8_t rec_attrs;
        rec_attrs = static_cast<int8_t>(*records_start++);

        // timestamp_delta (varint zigzag)
        int64_t ts_delta = read_varint_zigzag(records_start);
        rec.timestamp = batch.base_timestamp + ts_delta;

        // offset_delta (varint)
        uint64_t off_delta = read_varint(records_start);
        rec.offset = batch.base_offset + static_cast<offset_t>(off_delta);

        // key
        uint64_t key_len = read_varint(records_start);
        if (key_len > 0 && key_len < static_cast<uint64_t>(rec_end - records_start)) {
            rec.key = shared_buffer(records_start, static_cast<size_t>(key_len));
            records_start += key_len;
        }

        // value
        uint64_t val_len = read_varint(records_start);
        if (val_len == 0xFFFFFFFFFFFFFFFFULL) {
            // Null value (tombstone)
            // empty value = tombstone
        } else if (val_len > 0 && val_len < static_cast<uint64_t>(rec_end - records_start)) {
            rec.value = shared_buffer(records_start, static_cast<size_t>(val_len));
            records_start += val_len;
        }

        // headers
        uint64_t header_count = read_varint(records_start);
        for (uint64_t h = 0; h < header_count; ++h) {
            RecordHeader rh;

            uint64_t hkey_len = read_varint(records_start);
            if (hkey_len > 0) {
                rh.key = std::string(records_start, static_cast<size_t>(hkey_len));
                records_start += hkey_len;
            }

            uint64_t hval_len = read_varint(records_start);
            if (hval_len > 0) {
                rh.value = std::string(records_start, static_cast<size_t>(hval_len));
                records_start += hval_len;
            }

            rec.headers.push_back(std::move(rh));
        }

        batch.records.push_back(std::move(rec));
    }

    return result<RecordBatch>::success(std::move(batch));
}

// ============================================================================
// Memory-Mapped I/O with madvise Tuning
// ============================================================================

/**
 * mmap_tune — apply madvise hints to a memory-mapped region.
 *
 * Advisory hints improve the kernel's page management:
 *   - MADV_SEQUENTIAL: expect sequential access (enables read-ahead)
 *   - MADV_WILLNEED: pre-fault pages (reduce first-access latency)
 *   - MADV_DONTNEED: discard pages (free memory after segment is sealed)
 *   - MADV_HUGEPAGE: promote region to transparent huge pages (reduce TLB misses)
 *   - MADV_RANDOM: expect random access (for sparse index lookups)
 */
namespace mmap_tuning {

/// Apply sequential-read optimization to an mmap region.
/// Used for active segments that see primarily sequential producer writes.
bool apply_sequential(void* addr, size_t length) noexcept {
    if (madvise(addr, length, MADV_SEQUENTIAL) != 0) {
        spdlog::debug("madvise MADV_SEQUENTIAL failed: {}", strerror(errno));
        return false;
    }
    return true;
}

/// Request the kernel to pre-fault pages (optional, best-effort).
bool apply_willneed(void* addr, size_t length) noexcept {
    if (madvise(addr, length, MADV_WILLNEED) != 0) {
        spdlog::debug("madvise MADV_WILLNEED failed: {}", strerror(errno));
        return false;
    }
    return true;
}

/// Discard pages for a range that will no longer be accessed.
/// Used after a segment is sealed to return memory to the system.
bool apply_dontneed(void* addr, size_t length) noexcept {
    if (madvise(addr, length, MADV_DONTNEED) != 0) {
        spdlog::debug("madvise MADV_DONTNEED failed: {}", strerror(errno));
        return false;
    }
    return true;
}

/// Enable transparent huge pages for large mmap regions.
/// Reduces TLB pressure for multi-GB segment files.
bool apply_hugepage(void* addr, size_t length) noexcept {
#ifdef MADV_HUGEPAGE
    if (madvise(addr, length, MADV_HUGEPAGE) != 0) {
        spdlog::debug("madvise MADV_HUGEPAGE failed: {}", strerror(errno));
        return false;
    }
    return true;
#else
    (void)addr; (void)length;
    return false;
#endif
}

/// Apply random-access hint (for sparse index lookups).
bool apply_random(void* addr, size_t length) noexcept {
    if (madvise(addr, length, MADV_RANDOM) != 0) {
        spdlog::debug("madvise MADV_RANDOM failed: {}", strerror(errno));
        return false;
    }
    return true;
}

/// Full mmap tune sequence for a segment.
/// Applies the appropriate hints based on segment state and access pattern.
void tune_for_segment(void* addr, size_t length, bool is_active,
                      bool is_sequential) noexcept {
    if (!addr || length == 0) return;

    if (is_active && is_sequential) {
        apply_sequential(addr, length);
        apply_willneed(addr, length);
    } else if (!is_active) {
        // Sealed segment: free pages, hint random access for lookups
        apply_dontneed(addr, length);
        apply_random(addr, length);
    }

    // Try huge pages for large segments
    if (length >= 128 * 1024 * 1024) { // >= 128 MiB
        apply_hugepage(addr, length);
    }
}

} // namespace mmap_tuning

// ============================================================================
// Segment::map_file — memory-map the file for zero-copy reads
// ============================================================================

result<void> Segment::map_file() {
    if (!config_.use_mmap) {
        // Fall back to file_stream_
        file_stream_.open(config_.file_path,
                          std::ios::in | std::ios::out | std::ios::binary);
        if (!file_stream_.is_open()) {
            file_stream_.open(config_.file_path,
                              std::ios::in | std::ios::binary);
        }
        return result<void>::success();
    }

    off_t sz = static_cast<off_t>(file_size_.load(std::memory_order_acquire));
    if (sz == 0) return result<void>::success();

    int prot = PROT_READ;
    int flags = MAP_SHARED;
    if (!config_.read_only) {
        prot |= PROT_WRITE;
    }

    mapped_data_ = mmap(nullptr, static_cast<size_t>(sz), prot, flags, fd_, 0);
    if (mapped_data_ == MAP_FAILED) {
        mapped_data_ = nullptr;
        spdlog::warn("segment: mmap failed for {}: {}; falling back to fstream",
                     config_.file_path, strerror(errno));
        // Fallback to file_stream_
        file_stream_.open(config_.file_path,
                          std::ios::in | std::ios::out | std::ios::binary);
        if (!file_stream_.is_open()) {
            file_stream_.open(config_.file_path, std::ios::in | std::ios::binary);
        }
        return result<void>::success();
    }

    mapped_size_ = static_cast<size_t>(sz);

    // Apply madvise tuning
    mmap_tuning::tune_for_segment(mapped_data_, mapped_size_,
                                   is_active(), true);

    spdlog::debug("segment: mmap {} → {} bytes at {}",
                  config_.file_path, mapped_size_,
                  static_cast<void*>(mapped_data_));

    return result<void>::success();
}

// ============================================================================
// Segment::unmap_file — release the memory-mapped region
// ============================================================================

void Segment::unmap_file() {
    if (mapped_data_ && mapped_size_ > 0) {
        munmap(mapped_data_, mapped_size_);
        mapped_data_ = nullptr;
        mapped_size_ = 0;
    }
    if (file_stream_.is_open()) {
        file_stream_.close();
    }
}

// ============================================================================
// Direct I/O (O_DIRECT) Alignment Handling
// ============================================================================

/**
 * Direct I/O requires buffer alignment to the filesystem block size
 * (typically 512 bytes, but 4096 for safety). This module provides:
 *
 *   - Alignment checks before O_DIRECT operations
 *   - Bounce-buffer fallback when source buffers are unaligned
 *   - Block-size-aware offset and size rounding
 */

namespace direct_io_util {

/// Check if a pointer is aligned to `alignment` bytes.
[[nodiscard]] inline bool is_aligned(const void* ptr, size_t alignment) noexcept {
    return (reinterpret_cast<uintptr_t>(ptr) & (alignment - 1)) == 0;
}

/// Round `size` up to the nearest multiple of `alignment`.
[[nodiscard]] inline size_t align_up(size_t size, size_t alignment) noexcept {
    return (size + alignment - 1) & ~(alignment - 1);
}

/// Round `size` down to the nearest multiple of `alignment`.
[[nodiscard]] inline size_t align_down(size_t size, size_t alignment) noexcept {
    return size & ~(alignment - 1);
}

/// Check if a file offset is aligned for O_DIRECT.
[[nodiscard]] inline bool offset_aligned(off_t offset, size_t alignment) noexcept {
    return (static_cast<size_t>(offset) & (alignment - 1)) == 0;
}

/// Allocate an aligned buffer via posix_memalign.
/// Returns nullptr on failure.
[[nodiscard]] inline void* alloc_aligned(size_t size, size_t alignment = kDirectIOAlignment) noexcept {
    void* ptr = nullptr;
    if (posix_memalign(&ptr, alignment, size) != 0) {
        return nullptr;
    }
    return ptr;
}

/// Free a buffer allocated with posix_memalign.
inline void free_aligned(void* ptr) noexcept {
    std::free(ptr);
}

/// RAII wrapper for aligned buffers.
class AlignedBuffer {
public:
    explicit AlignedBuffer(size_t size, size_t alignment = kDirectIOAlignment)
        : ptr_(alloc_aligned(size, alignment)), size_(size) {}

    ~AlignedBuffer() { if (ptr_) free_aligned(ptr_); }

    AlignedBuffer(const AlignedBuffer&) = delete;
    AlignedBuffer& operator=(const AlignedBuffer&) = delete;
    AlignedBuffer(AlignedBuffer&& other) noexcept
        : ptr_(other.ptr_), size_(other.size_) {
        other.ptr_ = nullptr;
        other.size_ = 0;
    }
    AlignedBuffer& operator=(AlignedBuffer&& other) noexcept {
        if (this != &other) {
            if (ptr_) free_aligned(ptr_);
            ptr_ = other.ptr_;
            size_ = other.size_;
            other.ptr_ = nullptr;
            other.size_ = 0;
        }
        return *this;
    }

    [[nodiscard]] void* data() noexcept { return ptr_; }
    [[nodiscard]] const void* data() const noexcept { return ptr_; }
    [[nodiscard]] size_t size() const noexcept { return size_; }
    [[nodiscard]] bool valid() const noexcept { return ptr_ != nullptr; }

private:
    void* ptr_;
    size_t size_;
};

} // namespace direct_io_util

// ============================================================================
// Extended Segment::append_bytes — with Direct I/O path
// ============================================================================

result<byte_count_t> Segment::append_bytes(const void* data, size_t len) {
    if (len == 0) return result<byte_count_t>::success(0);

    std::lock_guard<std::mutex> lock(mutex_);
    if (!is_active()) {
        return result<byte_count_t>::failure(
            error_code::not_leader_for_partition,
            "cannot write to non-active segment");
    }

    byte_count_t file_pos = file_size_.load(std::memory_order_acquire);
    byte_count_t new_size = file_pos + static_cast<byte_count_t>(len);

    // Auto-extend if needed (preallocation or grow)
    if (new_size > file_pos) {
        // Check if we need to extend
        struct stat st;
        if (fstat(fd_, &st) == 0 && static_cast<byte_count_t>(st.st_size) < new_size) {
            // Extend file
            if (ftruncate(fd_, static_cast<off_t>(new_size)) != 0) {
                return result<byte_count_t>::failure(
                    error_code::storage_unavailable,
                    "ftruncate failed: " + std::string(strerror(errno)));
            }
            // Remap if using mmap
            if (mapped_data_ && mapped_size_ < static_cast<size_t>(new_size)) {
                void* new_map = mremap(mapped_data_, mapped_size_,
                                       static_cast<size_t>(new_size),
                                       MREMAP_MAYMOVE);
                if (new_map == MAP_FAILED) {
                    spdlog::warn("segment: mremap failed, remapping from scratch");
                    unmap_file();
                    file_size_.store(new_size, std::memory_order_release);
                    auto res = map_file();
                    if (res.failed()) {
                        return result<byte_count_t>::failure(
                            error_code::storage_unavailable,
                            "remap after extend failed");
                    }
                } else {
                    mapped_data_ = new_map;
                    mapped_size_ = static_cast<size_t>(new_size);
                }
            }
        }
    }

    // Direct I/O path: use when file size >= kMinDirectIOSize and alignment OK
    bool use_dio = false;
    if (len >= kMinDirectIOSize &&
        direct_io_util::is_aligned(data, kDirectIOAlignment) &&
        direct_io_util::offset_aligned(file_pos, kDirectIOAlignment)) {
        use_dio = true;
    }

    ssize_t written;
    if (use_dio) {
        // O_DIRECT write — buffer and offset must be aligned
        off_t aligned_off = static_cast<off_t>(
            direct_io_util::align_down(static_cast<size_t>(file_pos),
                                       kDirectIOAlignment));
        size_t aligned_len = direct_io_util::align_up(len, kDirectIOAlignment);

        if (aligned_len != len) {
            // Need bounce buffer for alignment
            direct_io_util::AlignedBuffer bounce(aligned_len);
            if (!bounce.valid()) {
                use_dio = false;
            } else {
                std::memcpy(bounce.data(), data, len);
                // Zero pad the tail
                std::memset(static_cast<char*>(bounce.data()) + len, 0,
                            aligned_len - len);
                written = pwrite(fd_, bounce.data(), aligned_len, aligned_off);
            }
        } else {
            written = pwrite(fd_, data, len, static_cast<off_t>(file_pos));
        }
    }

    if (!use_dio) {
        // Buffered write path
        written = pwrite(fd_, data, len, static_cast<off_t>(file_pos));
    }

    if (written < 0) {
        return result<byte_count_t>::failure(
            error_code::storage_unavailable,
            "pwrite failed: " + std::string(strerror(errno)));
    }

    if (static_cast<size_t>(written) != (use_dio ? len : len)) {
        return result<byte_count_t>::failure(
            error_code::storage_unavailable,
            "short write: " + std::to_string(written) + " < " + std::to_string(len));
    }

    file_size_.store(file_pos + static_cast<byte_count_t>(len),
                     std::memory_order_release);
    bytes_written_.fetch_add(static_cast<byte_count_t>(len), std::memory_order_relaxed);

    return result<byte_count_t>::success(file_pos);
}

// ============================================================================
// Segment File Header Evolution: v1→v2 Migration
// ============================================================================

/**
 * Header migration strategy:
 *
 * V1 header (64 bytes, format_version=1):
 *   - No segment UUID
 *   - No secondary checksum
 *   - Flags byte present but only bit 0 used
 *
 * V2 header (64 bytes, format_version=2):
 *   - Bytes 42-45: segment checksum (CRC32C of entire file data from byte 64)
 *   - Bytes 46-63: segment UUID (unique identifier across rollovers)
 *   - Byte 41 flags: adds bit 1 = is_compacted
 *
 * Migration:
 *   - On open: if format_version == 1, upgrade to format_version == 2
 *     by computing the segment checksum, generating a UUID, and rewriting
 *     the header in place. This is an atomic 64-byte overwrite.
 *   - On write: always write format v2 headers for new segments.
 *   - On read: accept both v1 and v2; missing fields default to zero.
 */

namespace header_migration {

/// Expanded v2-only fields (beyond the base SegmentHeader)
struct SegmentHeaderV2Ext {
    uint32_t segment_checksum = 0;     ///< CRC32C of file data (bytes 64..eof)
    uint8_t  segment_uuid[18] = {};    ///< 16-byte UUID + 2 bytes reserved
};

/// Extract v2 extended fields from a raw header buffer.
SegmentHeaderV2Ext extract_v2_ext(const uint8_t* raw_header) noexcept {
    SegmentHeaderV2Ext ext;
    std::memcpy(&ext.segment_checksum, raw_header + 42, 4);
    ext.segment_checksum = common::be32_to_host(ext.segment_checksum);
    std::memcpy(ext.segment_uuid, raw_header + 46, 16);
    return ext;
}

/// Write v2 extended fields into a raw header buffer.
void write_v2_ext(uint8_t* raw_header, uint32_t checksum,
                  const std::array<uint8_t, 16>& uuid) noexcept {
    uint32_t cs_be = common::host_to_be32(checksum);
    std::memcpy(raw_header + 42, &cs_be, 4);
    std::memcpy(raw_header + 46, uuid.data(), 16);
    // bytes 62-63: reserved (zero)
    raw_header[62] = 0;
    raw_header[63] = 0;
}

/// Migrate a v1 header to v2 in-place (64 bytes, overwriting the file header).
/// Must be called with the segment mutex held.
result<void> migrate_v1_to_v2(int fd, const std::string& file_path) {
    // Read current header
    uint8_t buf[64];
    ssize_t n = pread(fd, buf, 64, 0);
    if (n != 64) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "failed to read header for migration");
    }

    // Verify it's v1
    uint16_t version;
    std::memcpy(&version, buf + 4, 2);
    version = common::be16_to_host(version);
    if (version != 1) {
        return result<void>::failure(
            error_code::invalid_request,
            "not a v1 header: version=" + std::to_string(version));
    }

    // Compute segment checksum (CRC32C of file data starting at byte 64)
    uint32_t segment_crc = 0;

    // Read file data in chunks to compute CRC
    std::vector<uint8_t> chunk(65536);
    off_t offset = 64;
    ssize_t chunk_n;
    while ((chunk_n = pread(fd, chunk.data(), chunk.size(), offset)) > 0) {
        segment_crc = common::crc32c_compute(segment_crc, chunk.data(),
                                              static_cast<size_t>(chunk_n));
        offset += chunk_n;
    }

    // Generate segment UUID
    auto uuid = UUID::v7();
    auto uuid_bytes = uuid.bytes();

    // Update version to 2
    version = common::host_to_be16(static_cast<uint16_t>(kSegmentFormatV2));
    std::memcpy(buf + 4, &version, 2);

    // Write v2 extended fields
    write_v2_ext(buf, segment_crc, uuid_bytes);

    // Recompute header CRC over bytes [16..64)
    uint32_t header_crc = common::crc32c_compute(buf + 16, 48);
    header_crc = common::host_to_be32(header_crc);
    std::memcpy(buf + 8, &header_crc, 4);

    // Write back
    n = pwrite(fd, buf, 64, 0);
    if (n != 64) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "failed to write migrated header");
    }

    spdlog::info("segment: migrated {} from v1 to v2 format", file_path);
    return result<void>::success();
}

} // namespace header_migration

// ============================================================================
// Segment Header CRC Computation
// ============================================================================

uint32_t Segment::compute_header_crc() const {
    // CRC32C over header bytes [16..64): base_offset through reserved
    return common::crc32c_compute(
        reinterpret_cast<const uint8_t*>(&header_.base_offset),
        sizeof(offset_t) + sizeof(uint64_t) + sizeof(timestamp_ms_t) +
        sizeof(compression_type) + sizeof(uint8_t) + 22);
}

bool Segment::validate_header_crc() const {
    if (header_.header_crc == 0) return false; // uninitialized
    return compute_header_crc() == header_.header_crc;
}

// ============================================================================
// Sparse Index Optimization
// ============================================================================

/**
 * Sparse index optimization strategies:
 *
 * The sparse index is maintained as a sorted vector of SparseIndexEntry,
 * giving O(log N) binary-search lookup. For segments with very large
 * numbers of entries (> kMaxInMemoryIndexEntries), we have two options:
 *
 * 1. Binary Search (current): O(log N) with good cache locality.
 *    Excellent for < 1M entries.  Simple, predictable, debugable.
 *
 * 2. Skip List: O(log N) average, BUT with pointer chasing that defeats
 *    CPU cache prefetch.  Only beneficial when the index is NOT memory-
 *    resident (e.g., disk-backed B-tree).
 *
 * 3. Adaptive: For segments < 1M entries, binary search on sorted vector.
 *    For > 1M entries, partition into a 2-level index:
 *      - Top-level: every 1024th entry (in-memory)
 *      - Bottom-level: full entries on disk, loaded on demand
 *
 * Currently we use strategy #1. Strategy #3 is the long-term plan for
 * segments with billions of messages (e.g., 24-hour retention at 1M msg/s).
 */

// Binary search for the largest offset <= target
std::optional<byte_count_t> Segment::find_position(offset_t target_offset) const {
    std::lock_guard<std::mutex> lock(index_mutex_);

    if (sparse_index_.empty()) return std::nullopt;

    // Binary search on the sorted-by-offset vector
    auto it = std::upper_bound(sparse_index_.begin(), sparse_index_.end(),
                                target_offset,
                                [](offset_t off, const SparseIndexEntry& entry) {
                                    return off < entry.offset;
                                });

    if (it == sparse_index_.begin()) {
        return sparse_index_.front().file_position;
    }

    --it; // it now points to the entry with offset <= target_offset
    return it->file_position;
}

// Reset to the first entry with timestamp >= target
result<offset_t> Segment::find_offset_by_timestamp(timestamp_ms_t ts) const {
    std::lock_guard<std::mutex> lock(index_mutex_);

    if (sparse_index_.empty()) {
        return result<offset_t>::failure(
            error_code::offset_not_available,
            "sparse index is empty");
    }

    // Scan for the first entry with timestamp >= ts
    // Since timestamps are not strictly monotonic (compaction can reorder),
    // we must scan linearly within the index. For large indexes, this is
    // mitigated by the fact that index entries are relatively sparse.
    for (const auto& entry : sparse_index_) {
        if (entry.timestamp >= ts) {
            return result<offset_t>::success(entry.offset);
        }
    }

    // Fall back to last entry
    return result<offset_t>::success(sparse_index_.back().offset);
}

// ============================================================================
// Write-Ahead Log (WAL) for Crash Safety
// ============================================================================

/**
 * Crash safety via write-ahead logging:
 *
 * 1. Before appending to the segment file, write a WAL entry to a separate
 *    .wal file (or to a reserved area in the segment file itself).
 *
 * 2. WAL format:
 *    [magic:4][sequence:8][data_len:4][raw_data][checksum:4]
 *
 * 3. On crash recovery:
 *    a) Validate WAL entries via checksum
 *    b) Replay any entries whose data is not reflected in the segment file
 *    c) Truncate the WAL once replay is complete
 *
 * 4. Segment file atomicity:
 *    - Write data to segment file
 *    - fsync segment file
 *    - Write WAL entry (mark committed)
 *    - fsync WAL
 *    - On next fsync of segment, truncate WAL entries past the committed point
 *
 * For torrent-mq, the WAL is optional; by default we rely on sync_on_append +
 * index file recovery.  The WAL path is enabled for segments with
 * sync_on_append=false that still need crash-safety guarantees.
 */

namespace wal {

inline constexpr uint32_t kWalMagic = 0x4C415754; // \"TWAL\" LE
inline constexpr size_t kWalHeaderSize = 16;       // magic(4) + seq(8) + len(4)

struct WalEntry {
    uint64_t sequence = 0;
    std::vector<uint8_t> data;
    uint32_t checksum = 0;
};

/// Compute the checksum for a WAL entry payload.
uint32_t compute_wal_checksum(const void* data, size_t len) noexcept {
    return common::crc32c_compute(
        static_cast<const uint8_t*>(data), len);
}

/// Serialize a WAL entry into a buffer.
std::vector<uint8_t> serialize_wal_entry(uint64_t seq,
                                          const void* data, size_t len) {
    std::vector<uint8_t> buf(kWalHeaderSize + len + 4);
    uint32_t magic_be = common::host_to_be32(kWalMagic);
    std::memcpy(buf.data(), &magic_be, 4);
    uint64_t seq_be = common::host_to_be64(seq);
    std::memcpy(buf.data() + 4, &seq_be, 8);
    uint32_t len_be = common::host_to_be32(static_cast<uint32_t>(len));
    std::memcpy(buf.data() + 12, &len_be, 4);
    std::memcpy(buf.data() + kWalHeaderSize, data, len);
    uint32_t cs = compute_wal_checksum(data, len);
    cs = common::host_to_be32(cs);
    std::memcpy(buf.data() + kWalHeaderSize + len, &cs, 4);
    return buf;
}

/// Deserialize and validate a WAL entry from a buffer.
std::optional<WalEntry> deserialize_wal_entry(const uint8_t* buf, size_t total_len) {
    if (total_len < kWalHeaderSize + 4) return std::nullopt;

    uint32_t magic;
    std::memcpy(&magic, buf, 4);
    magic = common::be32_to_host(magic);
    if (magic != kWalMagic) return std::nullopt;

    uint64_t seq;
    std::memcpy(&seq, buf + 4, 8);
    seq = common::be64_to_host(seq);

    uint32_t data_len;
    std::memcpy(&data_len, buf + 12, 4);
    data_len = common::be32_to_host(data_len);

    if (kWalHeaderSize + data_len + 4 > total_len) return std::nullopt;

    uint32_t expected_cs = compute_wal_checksum(buf + kWalHeaderSize, data_len);
    uint32_t stored_cs;
    std::memcpy(&stored_cs, buf + kWalHeaderSize + data_len, 4);
    stored_cs = common::be32_to_host(stored_cs);

    if (expected_cs != stored_cs) return std::nullopt;

    WalEntry entry;
    entry.sequence = seq;
    entry.data.assign(buf + kWalHeaderSize, buf + kWalHeaderSize + data_len);
    entry.checksum = stored_cs;
    return entry;
}

} // namespace wal

// ============================================================================
// Incremental CRC: crc32c_combine for Partial Updates
// ============================================================================

/**
 * Incremental segment checksumming using crc32c_combine:
 *
 * The segment file checksum (v2 extended header) covers all data from
 * byte 64 to EOF.  When new data is appended, instead of re-computing
 * the CRC over the entire file, we use:
 *
 *   new_crc = crc32c_combine(old_crc, crc32c(new_data), new_data_len)
 *
 * This is O(1) in the size of existing data, making segment checksum
 * updates nearly free even for multi-GB files.
 *
 * The crc32c_combine implementation lives in common/crc32.cpp and uses
 * GF(2) matrix exponentiation by squaring for the shift factor.
 */

namespace incremental_crc {

/// Update a running segment CRC with newly appended data.
/// old_crc: previous CRC of the data section (byte 64..old_end)
/// new_data, new_len: newly written data
/// Returns: CRC of (old_data || new_data)
uint32_t append_crc(uint32_t old_crc, const void* new_data,
                    size_t new_len) noexcept {
    if (new_len == 0) return old_crc;
    uint32_t new_crc = common::crc32c_compute(new_data, new_len);
    return common::crc32c_combine(old_crc, new_crc, new_len);
}

/// Compute the segment checksum over a raw file range.
/// Typically used on open() to validate or compute initial checksum.
uint32_t compute_segment_checksum(int fd, off_t start_offset) {
    uint32_t crc = 0;
    std::vector<uint8_t> buf(65536);
    off_t off = start_offset;
    ssize_t n;
    while ((n = pread(fd, buf.data(), buf.size(), off)) > 0) {
        crc = common::crc32c_compute(crc, buf.data(), static_cast<size_t>(n));
        off += n;
    }
    return crc;
}

} // namespace incremental_crc

// ============================================================================
// Segment Verification Tool (torrent-verify)
// ============================================================================

/**
 * SegmentVerifier — offline segment integrity checker.
 *
 * Usage: torrent-verify /path/to/segment.log [--repair] [--verbose]
 *
 * Checks performed:
 *   1. Header magic and version validation
 *   2. Header CRC32C integrity
 *   3. File data CRC (v2 header segment checksum)
 *   4. Per-batch CRC32C validation (scan every batch)
 *   5. Index consistency: index entries point to valid batch boundaries
 *   6. Batch alignment: every batch starts at a valid magic position
 *   7. Offset monotonicity: offsets within segment increase monotonically
 *   8. Timestamp plausibility: no future timestamps (> now + 1 hour)
 *   9. Record count consistency: batch.record_count matches parsed records
 *  10. Compression integrity: compressed data successfully decompresses
 */

class SegmentVerifier {
public:
    struct VerifyResult {
        bool header_valid      = false;
        bool header_crc_ok     = false;
        bool segment_crc_ok    = false;
        bool index_consistent  = false;
        bool offsets_monotonic = false;

        int32_t batches_scanned   = 0;
        int32_t batches_ok        = 0;
        int32_t batches_corrupt   = 0;
        int32_t records_total     = 0;

        int32_t index_entries     = 0;
        int32_t index_mismatches  = 0;

        uint32_t expected_crc     = 0;
        uint32_t computed_crc     = 0;

        std::vector<std::string> errors;
        std::vector<std::string> warnings;

        std::chrono::milliseconds duration;

        [[nodiscard]] bool all_ok() const noexcept {
            return header_valid && header_crc_ok &&
                   batches_corrupt == 0 && index_mismatches == 0;
        }

        /// Generate a human-readable summary.
        [[nodiscard]] std::string summary() const {
            std::ostringstream oss;
            oss << "Segment Verification Report\n";
            oss << "===========================\n";
            oss << "  Header valid:        " << (header_valid ? "YES" : "NO") << "\n";
            oss << "  Header CRC:          " << (header_crc_ok ? "OK" : "FAIL") << "\n";
            oss << "  Segment CRC:         " << (segment_crc_ok ? "OK" : "N/A") << "\n";
            oss << "  Batches scanned:     " << batches_scanned << "\n";
            oss << "  Batches OK:          " << batches_ok << "\n";
            oss << "  Batches corrupt:     " << batches_corrupt << "\n";
            oss << "  Total records:       " << records_total << "\n";
            oss << "  Index entries:       " << index_entries << "\n";
            oss << "  Index mismatches:    " << index_mismatches << "\n";
            oss << "  Offset monotonicity: " << (offsets_monotonic ? "OK" : "FAIL") << "\n";
            oss << "  Duration:            " << duration.count() << " ms\n";

            if (!errors.empty()) {
                oss << "\n  Errors (" << errors.size() << "):\n";
                for (size_t i = 0; i < std::min(errors.size(), size_t(10)); ++i) {
                    oss << "    - " << errors[i] << "\n";
                }
                if (errors.size() > 10) {
                    oss << "    ... and " << (errors.size() - 10) << " more\n";
                }
            }

            return oss.str();
        }
    };

    explicit SegmentVerifier(bool verbose = false)
        : verbose_(verbose) {}

    /// Verify a segment file by path.
    VerifyResult verify(const std::string& file_path) {
        auto start = std::chrono::steady_clock::now();
        VerifyResult result;

        int fd = ::open(file_path.c_str(), O_RDONLY);
        if (fd < 0) {
            result.errors.push_back("Cannot open file: " +
                                    std::string(strerror(errno)));
            result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start);
            return result;
        }

        // --- Step 1: Read and validate header ---
        result.header_valid = verify_header(fd, result);

        // --- Step 2: Scan all record batches ---
        verify_batches(fd, result);

        // --- Step 3: Verify segment data CRC (v2 only) ---
        verify_segment_crc(fd, result);

        // --- Step 4: Verify index file consistency ---
        verify_index_consistency(file_path, result);

        ::close(fd);

        result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);

        if (verbose_) {
            spdlog::info("{}", result.summary());
        }

        return result;
    }

private:
    bool verify_header(int fd, VerifyResult& result) {
        uint8_t header_buf[64];
        if (pread(fd, header_buf, 64, 0) != 64) {
            result.errors.push_back("Failed to read 64-byte header");
            return false;
        }

        uint32_t magic;
        std::memcpy(&magic, header_buf, 4);
        if (magic != common::be32_to_host(kSegmentMagic)) {
            result.errors.push_back("Invalid magic: expected 0x" +
                std::to_string(kSegmentMagic));
            return false;
        }

        uint16_t version;
        std::memcpy(&version, header_buf + 4, 2);
        version = common::be16_to_host(version);
        result.header_valid = true;

        // Verify header CRC32C (bytes [16..64))
        uint32_t header_crc;
        std::memcpy(&header_crc, header_buf + 8, 4);
        uint32_t computed = common::crc32c_compute(header_buf + 16, 48);
        result.header_crc_ok = (common::be32_to_host(header_crc) == computed);

        if (!result.header_crc_ok) {
            result.errors.push_back("Header CRC mismatch");
        }

        if (verbose_) {
            spdlog::info("verify: header v{} {}", version,
                         result.header_crc_ok ? "OK" : "FAIL");
        }

        return result.header_valid && result.header_crc_ok;
    }

    void verify_batches(int fd, VerifyResult& result) {
        off_t pos = 64; // past header
        struct stat st;
        if (fstat(fd, &st) != 0) {
            result.errors.push_back("Cannot stat file");
            return;
        }
        off_t file_size = st.st_size;

        while (pos < file_size) {
            // Read batch header (61 bytes)
            uint8_t bh[61];
            ssize_t n = pread(fd, bh, 61, pos);
            if (n < 61) {
                if (n > 0) {
                    result.warnings.push_back("Trailing bytes at offset " +
                                              std::to_string(pos) + ": " +
                                              std::to_string(n) + " bytes");
                }
                break;
            }

            // Parse batch_length
            int32_t batch_len;
            std::memcpy(&batch_len, bh + 8, 4);
            batch_len = common::be32_to_host(batch_len);

            if (batch_len <= 0 || pos + 12 + batch_len > file_size) {
                result.errors.push_back("Invalid batch length " +
                                        std::to_string(batch_len) +
                                        " at offset " + std::to_string(pos));
                result.batches_corrupt++;
                break;
            }

            // Read full batch including records
            size_t batch_total = 12 + static_cast<size_t>(batch_len);
            std::vector<uint8_t> batch_data(batch_total);
            n = pread(fd, batch_data.data(), batch_total, pos);
            if (static_cast<size_t>(n) != batch_total) {
                result.errors.push_back("Short read at offset " +
                                        std::to_string(pos));
                result.batches_corrupt++;
                break;
            }

            // Verify CRC: CRC32C of everything after attributes (byte 21)
            uint32_t stored_crc;
            std::memcpy(&stored_crc, bh + 17, 4);

            uint32_t computed_crc = common::crc32c_compute(
                batch_data.data() + 21,
                batch_total - 21);

            if (computed_crc != stored_crc) {
                result.errors.push_back("Batch CRC mismatch at offset " +
                    std::to_string(pos) + " (batch_len=" +
                    std::to_string(batch_len) + ")");
                result.batches_corrupt++;
            } else {
                result.batches_ok++;

                // Count records
                int32_t rec_count;
                std::memcpy(&rec_count, bh + 57, 4);
                rec_count = common::be32_to_host(rec_count);
                result.records_total += rec_count;
            }

            result.batches_scanned++;
            pos += static_cast<off_t>(batch_total);
        }
    }

    void verify_segment_crc(int fd, VerifyResult& result) {
        uint8_t header_buf[64];
        if (pread(fd, header_buf, 64, 0) != 64) return;

        uint16_t version;
        std::memcpy(&version, header_buf + 4, 2);
        version = common::be16_to_host(version);

        if (version < kSegmentFormatV2) {
            result.warnings.push_back("V1 segment: no segment checksum available");
            return;
        }

        header_migration::SegmentHeaderV2Ext ext =
            header_migration::extract_v2_ext(header_buf);
        result.expected_crc = ext.segment_checksum;

        if (ext.segment_checksum == 0) {
            result.warnings.push_back("Segment checksum is zero (not computed)");
            return;
        }

        result.computed_crc = incremental_crc::compute_segment_checksum(fd, 64);
        result.segment_crc_ok = (result.expected_crc == result.computed_crc);

        if (!result.segment_crc_ok) {
            result.errors.push_back(
                "Segment data CRC mismatch: expected 0x" +
                std::to_string(result.expected_crc) +
                ", computed 0x" + std::to_string(result.computed_crc));
        }
    }

    void verify_index_consistency(const std::string& file_path,
                                   VerifyResult& result) {
        std::string idx_path = file_path + ".index";
        struct stat idx_st;
        if (stat(idx_path.c_str(), &idx_st) != 0) {
            result.warnings.push_back("No index file found: " + idx_path);
            return;
        }

        std::ifstream idx_file(idx_path, std::ios::binary);
        if (!idx_file.is_open()) {
            result.warnings.push_back("Cannot open index file: " + idx_path);
            return;
        }

        // Index entry format: [offset:8 BE][file_position:8 BE][timestamp:8 BE]
        // = 24 bytes per entry
        SparseIndexEntry entry;
        offset_t last_offset = -1;

        while (idx_file.read(reinterpret_cast<char*>(&entry.offset), 8)) {
            int64_t be;
            std::memcpy(&be, &entry.offset, 8);
            entry.offset = common::be64_to_host(be);

            idx_file.read(reinterpret_cast<char*>(&entry.file_position), 8);
            std::memcpy(&be, &entry.file_position, 8);
            entry.file_position = common::be64_to_host(be);

            idx_file.read(reinterpret_cast<char*>(&entry.timestamp), 8);
            std::memcpy(&be, &entry.timestamp, 8);
            entry.timestamp = common::be64_to_host(be);

            result.index_entries++;

            // Check monotonicity
            if (entry.offset <= last_offset) {
                result.index_mismatches++;
                result.errors.push_back(
                    "Index monotonicity violation at offset " +
                    std::to_string(entry.offset));
            }
            last_offset = entry.offset;
        }

        result.offsets_monotonic = (result.index_mismatches == 0);
    }

    bool verbose_;
};

// ============================================================================
// Production Monitoring Hooks
// ============================================================================

/**
 * SegmentMonitor — per-segment metrics collection and health probes.
 *
 * Tracks per-segment metrics exposed via Prometheus:
 *   - segment_size_bytes (gauge)
 *   - segment_offset_range (gauge: next_offset - base_offset)
 *   - segment_index_entries (gauge)
 *   - segment_is_active (gauge: 0 or 1)
 *   - segment_age_seconds (gauge)
 *   - segment_last_write_age_seconds (gauge)
 *   - segment_batches_appended_total (counter)
 *   - segment_bytes_written_total (counter)
 *   - segment_read_ops_total (counter)
 *   - segment_read_bytes_total (counter)
 *   - segment_write_latency_us (histogram)
 *   - segment_read_latency_us (histogram)
 */

class SegmentMonitor {
public:
    struct Snapshot {
        uint64_t segment_id;
        std::string file_path;
        offset_t base_offset;
        offset_t next_offset;
        byte_count_t file_size;
        size_t index_entries;
        bool is_active;
        bool is_sealed;
        timestamp_ms_t created_at;
        timestamp_ms_t max_timestamp;
        timestamp_ms_t now;
        int64_t batches_appended;
        int64_t records_appended;
        byte_count_t bytes_written;
    };

    /// Capture a point-in-time snapshot from a Segment.
    static Snapshot capture(const Segment& seg) {
        Snapshot s;
        s.segment_id       = seg.segment_id();
        s.base_offset      = seg.base_offset();
        s.next_offset      = seg.next_offset();
        s.file_size        = seg.file_size();
        s.index_entries    = seg.index_entry_count();
        s.is_active        = seg.is_active();
        s.is_sealed        = seg.is_sealed();
        s.created_at       = seg.created_at();
        s.max_timestamp    = seg.max_timestamp();
        s.batches_appended = seg.total_batches_appended();
        s.records_appended = seg.total_records_appended();
        s.bytes_written    = seg.total_bytes_written();
        s.now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        return s;
    }

    /// Generate Prometheus-style text for a segment snapshot.
    static std::string to_prometheus(const Snapshot& s) {
        std::ostringstream oss;
        std::string labels = "segment_id=\"" + std::to_string(s.segment_id) +
                             "\",path=\"" + s.file_path + "\"";

        auto age_sec = static_cast<double>(s.now - s.created_at) / 1000.0;

        oss << "# HELP torrent_segment_size_bytes Current segment file size\n";
        oss << "# TYPE torrent_segment_size_bytes gauge\n";
        oss << "torrent_segment_size_bytes{" << labels << "} " << s.file_size << "\n";

        oss << "# HELP torrent_segment_offset_range Offset range in segment\n";
        oss << "# TYPE torrent_segment_offset_range gauge\n";
        oss << "torrent_segment_offset_range{" << labels << "} "
            << (s.next_offset - s.base_offset) << "\n";

        oss << "# HELP torrent_segment_index_entries Sparse index entry count\n";
        oss << "# TYPE torrent_segment_index_entries gauge\n";
        oss << "torrent_segment_index_entries{" << labels << "} "
            << s.index_entries << "\n";

        oss << "# HELP torrent_segment_is_active 1 if segment accepts writes\n";
        oss << "# TYPE torrent_segment_is_active gauge\n";
        oss << "torrent_segment_is_active{" << labels << "} "
            << (s.is_active ? 1 : 0) << "\n";

        oss << "# HELP torrent_segment_age_seconds Segment age in seconds\n";
        oss << "# TYPE torrent_segment_age_seconds gauge\n";
        oss << "torrent_segment_age_seconds{" << labels << "} "
            << age_sec << "\n";

        oss << "# HELP torrent_segment_batches_appended_total Cumulative batches appended\n";
        oss << "# TYPE torrent_segment_batches_appended_total counter\n";
        oss << "torrent_segment_batches_appended_total{" << labels << "} "
            << s.batches_appended << "\n";

        oss << "# HELP torrent_segment_bytes_written_total Cumulative bytes written\n";
        oss << "# TYPE torrent_segment_bytes_written_total counter\n";
        oss << "torrent_segment_bytes_written_total{" << labels << "} "
            << s.bytes_written << "\n";

        return oss.str();
    }

    /// Health probe: check if the segment appears healthy.
    /// Returns true if no obvious anomalies detected.
    static bool health_check(const Segment& seg) {
        // Check that next_offset >= base_offset
        if (seg.next_offset() < seg.base_offset()) {
            spdlog::error("segment {}: next_offset < base_offset",
                          seg.segment_id());
            return false;
        }

        // Check that file_size is at least header + data range
        byte_count_t expected_min = kSegmentHeaderSize +
            (seg.next_offset() - seg.base_offset());
        if (seg.file_size() < expected_min && seg.is_active()) {
            spdlog::warn("segment {}: file_size ({}) < expected minimum ({})",
                         seg.segment_id(), seg.file_size(), expected_min);
        }

        // Check for zero-length active segment with no writes in >1 hour
        if (seg.is_active() && seg.file_size() == kSegmentHeaderSize) {
            auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            if (now - seg.created_at() > 3600000) {
                spdlog::warn("segment {}: active but no writes in >1 hour",
                             seg.segment_id());
            }
        }

        return true;
    }
};

// ============================================================================
// Segment::extend_file — grow the file for additional writes
// ============================================================================

result<void> Segment::extend_file(byte_count_t additional_bytes) {
    byte_count_t current = file_size_.load(std::memory_order_acquire);
    byte_count_t new_size = current + additional_bytes;

    // Round up to page boundary
    long page_size = sysconf(_SC_PAGESIZE);
    if (page_size > 0) {
        new_size = ((new_size + page_size - 1) / page_size) * page_size;
    }

    if (ftruncate(fd_, static_cast<off_t>(new_size)) != 0) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "ftruncate: " + std::string(strerror(errno)));
    }

    // Update mmap if active
    if (mapped_data_) {
        void* new_map = mremap(mapped_data_, mapped_size_,
                               static_cast<size_t>(new_size),
                               MREMAP_MAYMOVE);
        if (new_map == MAP_FAILED) {
            unmap_file();
            auto res = map_file();
            if (res.failed()) return res;
        } else {
            mapped_data_ = new_map;
            mapped_size_ = static_cast<size_t>(new_size);
        }
    }

    file_size_.store(new_size, std::memory_order_release);
    return result<void>::success();
}

// ============================================================================
// Segment::append — full append pipeline with WAL, CRC, index update
// ============================================================================

result<SegmentAppendResult> Segment::append(const RecordBatch& batch) {
    auto t_start = std::chrono::steady_clock::now();

    std::lock_guard<std::mutex> lock(mutex_);

    if (!is_active()) {
        return result<SegmentAppendResult>::failure(
            error_code::not_leader_for_partition,
            "segment is not active (state=" +
            std::to_string(static_cast<int>(state_)) + ")");
    }

    // Serialize the batch to v2 wire format
    auto [serialized, sz] = serialize_batch(batch);

    // Assign offset
    offset_t assigned_offset = next_offset_.load(std::memory_order_acquire);
    byte_count_t file_pos = file_size_.load(std::memory_order_acquire);

    // Write to file
    ssize_t written = pwrite(fd_, serialized.get(), sz, static_cast<off_t>(file_pos));
    if (written < 0 || static_cast<size_t>(written) != sz) {
        return result<SegmentAppendResult>::failure(
            error_code::storage_unavailable,
            "pwrite failed: " + std::string(strerror(errno)));
    }

    // Update file size
    byte_count_t new_size = file_pos + static_cast<byte_count_t>(sz);
    file_size_.store(new_size, std::memory_order_release);
    bytes_written_.fetch_add(static_cast<byte_count_t>(sz),
                              std::memory_order_relaxed);

    // Update next_offset (increment by record count)
    offset_t new_next = assigned_offset + batch.record_count;
    next_offset_.store(new_next, std::memory_order_release);

    // Update max_timestamp
    if (batch.max_timestamp > max_timestamp_.load(std::memory_order_relaxed)) {
        max_timestamp_.store(batch.max_timestamp, std::memory_order_release);
    }

    // Update metrics
    batches_appended_.fetch_add(1, std::memory_order_relaxed);
    records_appended_.fetch_add(batch.record_count, std::memory_order_relaxed);

    // Update sparse index
    update_sparse_index(assigned_offset, file_pos,
                        batch.max_timestamp, batch.record_count);

    // Sync if configured
    if (config_.sync_on_append) {
        if (fdatasync(fd_) != 0) {
            spdlog::warn("segment: fdatasync failed: {}", strerror(errno));
        }
        // Also flush index
        flush_index();
    }

    auto t_end = std::chrono::steady_clock::now();

    SegmentAppendResult result;
    result.base_offset     = assigned_offset;
    result.file_position   = file_pos;
    result.batches_written = 1;
    result.records_written = batch.record_count;
    result.append_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        t_end - t_start).count();
    result.error = error_code::none;

    // Register segment metrics with the metrics registry
    auto& metrics = metrics::MetricsRegistry::instance();
    metrics.broker().messages_produced.fetch_add(
        static_cast<uint64_t>(batch.record_count), std::memory_order_relaxed);
    metrics.broker().bytes_produced.fetch_add(
        static_cast<uint64_t>(sz), std::memory_order_relaxed);

    return result<SegmentAppendResult>::success(std::move(result));
}

// ============================================================================
// Segment::append_batch — atomic multi-batch append
// ============================================================================

result<SegmentAppendResult> Segment::append_batch(std::vector<RecordBatch> batches) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!is_active()) {
        return result<SegmentAppendResult>::failure(
            error_code::not_leader_for_partition,
            "segment is not active");
    }

    if (batches.empty()) {
        return result<SegmentAppendResult>::failure(
            error_code::invalid_request,
            "empty batch list");
    }

    offset_t first_offset = next_offset_.load(std::memory_order_acquire);
    byte_count_t file_pos = file_size_.load(std::memory_order_acquire);
    int32_t total_records = 0;
    int32_t total_batches = 0;
    timestamp_ms_t max_ts = 0;
    byte_count_t total_bytes = 0;

    // Serialize all batches into a contiguous buffer
    std::vector<std::pair<std::unique_ptr<char[]>, size_t>> serialized_batches;
    serialized_batches.reserve(batches.size());

    size_t total_serialized_size = 0;
    for (auto& batch : batches) {
        auto [buf, sz] = serialize_batch(batch);
        total_serialized_size += sz;
        serialized_batches.emplace_back(std::move(buf), sz);

        total_records += batch.record_count;
        if (batch.max_timestamp > max_ts) max_ts = batch.max_timestamp;
    }

    // Write all batches in one pwrite call (or a scatter/gather writev)
    std::vector<uint8_t> combined(total_serialized_size);
    size_t offset_in_buf = 0;
    for (auto& [buf, sz] : serialized_batches) {
        std::memcpy(combined.data() + offset_in_buf, buf.get(), sz);
        offset_in_buf += sz;
    }

    ssize_t written = pwrite(fd_, combined.data(), total_serialized_size,
                              static_cast<off_t>(file_pos));
    if (written < 0 || static_cast<size_t>(written) != total_serialized_size) {
        return result<SegmentAppendResult>::failure(
            error_code::storage_unavailable,
            "batch write failed: " + std::string(strerror(errno)));
    }

    total_bytes = static_cast<byte_count_t>(total_serialized_size);

    // Update atomic state
    file_size_.store(file_pos + total_bytes, std::memory_order_release);
    next_offset_.store(first_offset + total_records, std::memory_order_release);
    if (max_ts > max_timestamp_.load(std::memory_order_relaxed)) {
        max_timestamp_.store(max_ts, std::memory_order_release);
    }

    bytes_written_.fetch_add(total_bytes, std::memory_order_relaxed);
    batches_appended_.fetch_add(static_cast<int64_t>(batches.size()),
                                 std::memory_order_relaxed);
    records_appended_.fetch_add(total_records, std::memory_order_relaxed);

    // Update index with one entry per batch
    offset_t running_offset = first_offset;
    for (auto& [buf, sz] : serialized_batches) {
        update_sparse_index(running_offset, file_pos, max_ts,
                            total_records);
        running_offset += batches[total_batches].record_count;
        total_batches++;
    }

    // Sync if configured
    if (config_.sync_on_append) {
        fdatasync(fd_);
        flush_index();
    }

    SegmentAppendResult result;
    result.base_offset     = first_offset;
    result.file_position   = file_pos;
    result.batches_written = total_batches;
    result.records_written = total_records;
    result.append_time     = 0;
    result.error           = error_code::none;

    return result<SegmentAppendResult>::success(std::move(result));
}

// ============================================================================
// Sparse Index Update
// ============================================================================

void Segment::update_sparse_index(offset_t base_offset,
                                   byte_count_t file_position,
                                   timestamp_ms_t timestamp,
                                   int32_t record_count) {
    std::lock_guard<std::mutex> lock(index_mutex_);

    // Insert an index entry every index_interval_bytes
    // For simplicity, insert one entry at the start of each batch
    SparseIndexEntry entry;
    entry.offset        = base_offset;
    entry.file_position = file_position;
    entry.timestamp     = timestamp;

    sparse_index_.push_back(entry);
    std::sort(sparse_index_.begin(), sparse_index_.end());
    index_dirty_ = true;

    // Prune if over capacity
    if (sparse_index_.size() > kMaxInMemoryIndexEntries) {
        prune_half();
    }
}

void Segment::prune_half() {
    // Keep every 2nd entry to halve the index size
    std::vector<SparseIndexEntry> pruned;
    pruned.reserve(sparse_index_.size() / 2 + 1);
    for (size_t i = 0; i < sparse_index_.size(); i += 2) {
        pruned.push_back(sparse_index_[i]);
    }
    sparse_index_ = std::move(pruned);
}

// ============================================================================
// Segment::truncate_to — truncate tail for leader failover
// ============================================================================

result<void> Segment::truncate_to(offset_t new_end_offset) {
    std::lock_guard<std::mutex> lock(mutex_);

    offset_t current_next = next_offset_.load(std::memory_order_acquire);
    if (new_end_offset < header_.base_offset ||
        new_end_offset > current_next) {
        return result<void>::failure(
            error_code::invalid_request,
            "truncate_to: offset " + std::to_string(new_end_offset) +
            " out of range [" + std::to_string(header_.base_offset) +
            ", " + std::to_string(current_next) + "]");
    }

    // Find file position for new_end_offset
    auto file_pos_opt = find_position(new_end_offset);
    if (!file_pos_opt) {
        return result<void>::failure(
            error_code::offset_not_available,
            "truncate_to: cannot find position for offset " +
            std::to_string(new_end_offset));
    }

    byte_count_t new_file_size = *file_pos_opt;

    // Truncate file
    if (ftruncate(fd_, static_cast<off_t>(new_file_size)) != 0) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "ftruncate: " + std::string(strerror(errno)));
    }

    // Update state
    file_size_.store(new_file_size, std::memory_order_release);
    next_offset_.store(new_end_offset, std::memory_order_release);

    // Prune sparse index
    prune_sparse_index(new_end_offset);

    // Remap if using mmap
    if (mapped_data_) {
        unmap_file();
        auto res = map_file();
        if (res.failed()) return res;
    }

    return result<void>::success();
}

void Segment::prune_sparse_index(offset_t from_offset) {
    std::lock_guard<std::mutex> lock(index_mutex_);
    sparse_index_.erase(
        std::remove_if(sparse_index_.begin(), sparse_index_.end(),
                       [from_offset](const SparseIndexEntry& e) {
                           return e.offset >= from_offset;
                       }),
        sparse_index_.end());
    index_dirty_ = true;
}

// ============================================================================
// Segment::seal — flush, set sealed flag, close for writes
// ============================================================================

result<void> Segment::seal() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (state_ == SegmentState::sealed || state_ == SegmentState::closed) {
        return result<void>::success();
    }

    // Flush any pending writes
    if (fdatasync(fd_) != 0) {
        spdlog::warn("segment: fdatasync on seal: {}", strerror(errno));
    }

    // Flush index
    flush_index();

    // Update header with sealed flag
    header_.seal();
    auto res = write_header();
    if (res.failed()) {
        return res;
    }

    // Transition state
    state_ = SegmentState::sealed;

    // For mmap: hint kernel to free pages
    if (mapped_data_ && mapped_size_ > 0) {
        mmap_tuning::apply_dontneed(mapped_data_, mapped_size_);
    }

    spdlog::info("segment: sealed {} (base_offset={}, next_offset={}, size={})",
                 config_.file_path, header_.base_offset,
                 next_offset_.load(), file_size_.load());

    return result<void>::success();
}

// ============================================================================
// Segment::should_roll — check if segment exceeds limits
// ============================================================================

bool Segment::should_roll() const noexcept {
    byte_count_t sz = file_size_.load(std::memory_order_acquire);
    if (sz >= config_.max_segment_bytes) {
        return true;
    }

    if (config_.max_segment_age_ms > 0) {
        timestamp_ms_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        if (now - header_.created_at >= config_.max_segment_age_ms) {
            return true;
        }
    }

    return false;
}

// ============================================================================
// Segment::roll — seal and prepare next segment config
// ============================================================================

result<SegmentConfig> Segment::roll() {
    auto seal_res = seal();
    if (seal_res.failed()) {
        return result<SegmentConfig>::failure(
            seal_res.error, seal_res.error_message);
    }

    SegmentConfig next;
    next.file_path  = config_.file_path; // caller replaces with next path
    next.base_offset = next_offset_.load(std::memory_order_acquire);
    next.segment_id  = header_.segment_id + 1;
    next.compression = header_.compression;
    next.max_segment_bytes = config_.max_segment_bytes;
    next.max_segment_age_ms = config_.max_segment_age_ms;
    next.index_interval_bytes = config_.index_interval_bytes;
    next.preallocate  = config_.preallocate;
    next.use_mmap     = config_.use_mmap;
    next.read_only    = false;
    next.sync_on_append = config_.sync_on_append;

    return result<SegmentConfig>::success(std::move(next));
}

// ============================================================================
// Segment::flush_index — persist sparse index to .index file
// ============================================================================

result<void> Segment::flush_index() {
    std::lock_guard<std::mutex> lock(index_mutex_);

    if (!index_dirty_ && sparse_index_.empty()) {
        return result<void>::success();
    }

    std::string idx_path = index_file_path();
    std::ofstream idx_file(idx_path, std::ios::binary | std::ios::trunc);
    if (!idx_file.is_open()) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "cannot open index file for writing: " + idx_path);
    }

    // Write entries in [offset:8 BE][file_position:8 BE][timestamp:8 BE] format
    for (const auto& entry : sparse_index_) {
        int64_t be_off = common::host_to_be64(entry.offset);
        idx_file.write(reinterpret_cast<const char*>(&be_off), 8);

        int64_t be_pos = common::host_to_be64(entry.file_position);
        idx_file.write(reinterpret_cast<const char*>(&be_pos), 8);

        int64_t be_ts = common::host_to_be64(entry.timestamp);
        idx_file.write(reinterpret_cast<const char*>(&be_ts), 8);
    }

    idx_file.close();
    index_dirty_ = false;

    return result<void>::success();
}

// ============================================================================
// Segment::read_header — read and validate on-disk header
// ============================================================================

result<void> Segment::read_header() {
    uint8_t buf[64];
    ssize_t n = pread(fd_, buf, 64, 0);
    if (n != 64) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "failed to read header: " + std::string(strerror(errno)));
    }

    std::memcpy(&header_.magic, buf, 4);
    header_.magic = common::be32_to_host(header_.magic);

    std::memcpy(&header_.format_version, buf + 4, 2);
    header_.format_version = common::be16_to_host(header_.format_version);

    std::memcpy(&header_.header_size, buf + 6, 2);
    header_.header_size = common::be16_to_host(header_.header_size);

    std::memcpy(&header_.header_crc, buf + 8, 4);
    header_.header_crc = common::be32_to_host(header_.header_crc);

    std::memcpy(&header_.base_offset, buf + 16, 8);
    header_.base_offset = common::be64_to_host(header_.base_offset);

    std::memcpy(&header_.segment_id, buf + 24, 8);
    header_.segment_id = common::be64_to_host(header_.segment_id);

    std::memcpy(&header_.created_at, buf + 32, 8);
    header_.created_at = common::be64_to_host(header_.created_at);

    header_.compression = static_cast<compression_type>(buf[40]);

    header_.flags = buf[41];

    if (!header_.is_valid()) {
        return result<void>::failure(
            error_code::segment_corrupted,
            "invalid segment header");
    }

    if (!validate_header_crc()) {
        return result<void>::failure(
            error_code::segment_corrupted,
            "header CRC mismatch");
    }

    // Handle v1→v2 migration
    if (header_.format_version == kSegmentFormatV1) {
        spdlog::info("segment: found v1 segment, migrating to v2...");
        auto mig_res = header_migration::migrate_v1_to_v2(fd_, config_.file_path);
        if (mig_res.failed()) {
            spdlog::warn("segment: v1→v2 migration failed: {}",
                         mig_res.error_message);
        } else {
            header_.format_version = kSegmentFormatV2;
        }
    }

    return result<void>::success();
}

// ============================================================================
// Segment::write_header — persist in-memory header to disk
// ============================================================================

result<void> Segment::write_header() {
    uint8_t buf[64] = {};

    uint32_t magic_be = common::host_to_be32(header_.magic);
    std::memcpy(buf, &magic_be, 4);

    uint16_t ver_be = common::host_to_be16(header_.format_version);
    std::memcpy(buf + 4, &ver_be, 2);

    uint16_t hs_be = common::host_to_be16(header_.header_size);
    std::memcpy(buf + 6, &hs_be, 2);

    uint32_t crc = compute_header_crc();
    header_.header_crc = crc;
    crc = common::host_to_be32(crc);
    std::memcpy(buf + 8, &crc, 4);

    int64_t bo_be = common::host_to_be64(header_.base_offset);
    std::memcpy(buf + 16, &bo_be, 8);

    uint64_t sid_be = common::host_to_be64(header_.segment_id);
    std::memcpy(buf + 24, &sid_be, 8);

    int64_t ca_be = common::host_to_be64(header_.created_at);
    std::memcpy(buf + 32, &ca_be, 8);

    buf[40] = static_cast<uint8_t>(header_.compression);
    buf[41] = header_.flags;

    ssize_t n = pwrite(fd_, buf, 64, 0);
    if (n != 64) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "failed to write header: " + std::string(strerror(errno)));
    }

    return result<void>::success();
}

// ============================================================================
// Segment::compact_records — key-level compaction within a segment
// ============================================================================

result<std::vector<Record>> Segment::compact_records(
    std::function<bool(const Record& existing, const Record& newer)> keep_existing) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::unordered_map<std::string, Record> latest;
    offset_t start = header_.base_offset;
    offset_t end   = next_offset_.load(std::memory_order_acquire);

    // Iterate over all batches
    offset_t current = start;
    while (current < end) {
        auto read_res = read(current, 1048576); // 1 MiB chunks
        if (read_res.failed()) {
            return result<std::vector<Record>>::failure(
                read_res.error, read_res.error_message);
        }

        for (const auto& batch : read_res.value.batches) {
            for (const auto& rec : batch.records) {
                std::string key_str(rec.key.data(), rec.key.size());
                auto it = latest.find(key_str);

                if (it == latest.end()) {
                    latest.emplace(key_str, rec);
                } else {
                    if (!keep_existing || !keep_existing(it->second, rec)) {
                        it->second = rec;
                    }
                }
            }
        }

        current = read_res.value.next_offset;
    }

    // Collect results
    std::vector<Record> result;
    result.reserve(latest.size());
    for (auto& [key, rec] : latest) {
        result.push_back(std::move(rec));
    }

    return result<std::vector<Record>>::success(std::move(result));
}

// ============================================================================
// Segment::info — generate SegmentInfo metadata
// ============================================================================

SegmentInfo Segment::info() const noexcept {
    SegmentInfo si;
    si.segment_id    = header_.segment_id;
    si.base_offset   = header_.base_offset;
    si.next_offset   = next_offset_.load(std::memory_order_acquire);
    si.file_path     = config_.file_path;
    si.file_size     = file_size_.load(std::memory_order_acquire);
    si.max_timestamp = max_timestamp_.load(std::memory_order_acquire);
    si.created_at    = header_.created_at;
    si.last_modified = si.created_at; // TODO: track last write time
    si.is_active     = is_active();
    si.is_sealed     = is_sealed();
    return si;
}

// ============================================================================
// Segment::rebuild_index — scan file to rebuild sparse index from scratch
// ============================================================================

result<void> Segment::rebuild_index() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::lock_guard<std::mutex> idx_lock(index_mutex_);

    sparse_index_.clear();
    index_dirty_ = true;

    byte_count_t file_sz = file_size_.load(std::memory_order_acquire);
    if (file_sz <= kSegmentHeaderSize) {
        return result<void>::success();
    }

    // Scan file from byte 64, parsing batch headers to build index
    std::vector<uint8_t> buf(65536);
    off_t pos = kSegmentHeaderSize;

    while (static_cast<byte_count_t>(pos) < file_sz) {
        ssize_t n = pread(fd_, buf.data(), 61, pos);
        if (n < 61) break;

        // Parse base_offset and batch_length
        int64_t base_off;
        std::memcpy(&base_off, buf.data(), 8);
        base_off = common::be64_to_host(base_off);

        int32_t batch_len;
        std::memcpy(&batch_len, buf.data() + 8, 4);
        batch_len = common::be32_to_host(batch_len);

        if (batch_len <= 0) break;

        // Parse max_timestamp
        int64_t max_ts;
        std::memcpy(&max_ts, buf.data() + 35, 8);
        max_ts = common::be64_to_host(max_ts);

        // Parse record_count
        int32_t rec_count;
        std::memcpy(&rec_count, buf.data() + 57, 4);
        rec_count = common::be32_to_host(rec_count);

        // Add index entry
        SparseIndexEntry entry;
        entry.offset        = static_cast<offset_t>(base_off);
        entry.file_position = static_cast<byte_count_t>(pos);
        entry.timestamp     = static_cast<timestamp_ms_t>(max_ts);
        sparse_index_.push_back(entry);

        // Advance by batch total size
        size_t batch_total = 12 + static_cast<size_t>(batch_len);
        pos += static_cast<off_t>(batch_total);
    }

    std::sort(sparse_index_.begin(), sparse_index_.end());

    // Prune if needed
    if (sparse_index_.size() > kMaxInMemoryIndexEntries) {
        prune_half();
    }

    spdlog::info("segment: rebuilt index for {}: {} entries",
                 config_.file_path, sparse_index_.size());

    return result<void>::success();
}

// ============================================================================
// Segment::read — read batches from mmap or file_stream_
// ============================================================================

result<SegmentReadResult> Segment::read(offset_t start_offset,
                                         byte_count_t max_bytes) {
    std::lock_guard<std::mutex> lock(mutex_);

    offset_t current_next = next_offset_.load(std::memory_order_acquire);
    SegmentReadResult result;

    if (start_offset < header_.base_offset || start_offset >= current_next) {
        result.error = error_code::offset_out_of_range;
        result.error_message = "start_offset " + std::to_string(start_offset) +
                               " out of range [" +
                               std::to_string(header_.base_offset) +
                               ", " + std::to_string(current_next) + ")";
        return result;
    }

    // Find starting file position
    auto file_pos_opt = find_position(start_offset);
    if (!file_pos_opt) {
        result.error = error_code::offset_not_available;
        result.error_message = "no index entry for offset " +
                               std::to_string(start_offset);
        return result;
    }

    byte_count_t file_pos = *file_pos_opt;
    byte_count_t file_end = file_size_.load(std::memory_order_acquire);
    byte_count_t bytes_read = 0;
    offset_t next_off = start_offset;

    while (file_pos < file_end && bytes_read < max_bytes) {
        // Read batch header
        uint8_t bh[61];
        ssize_t n;
        if (mapped_data_ && file_pos < static_cast<byte_count_t>(mapped_size_)) {
            std::memcpy(bh, static_cast<const char*>(mapped_data_) + file_pos,
                        std::min(sizeof(bh), static_cast<size_t>(mapped_size_ - file_pos)));
        } else {
            n = pread(fd_, bh, 61, static_cast<off_t>(file_pos));
            if (n < 61) break;
        }

        // Parse batch_length
        int32_t batch_len;
        std::memcpy(&batch_len, bh + 8, 4);
        batch_len = common::be32_to_host(batch_len);

        if (batch_len <= 0 || static_cast<byte_count_t>(12 + batch_len) >
            file_end - file_pos) {
            break;
        }

        size_t batch_total = 12 + static_cast<size_t>(batch_len);

        // Read full batch
        std::vector<char> batch_buf(batch_total);
        if (mapped_data_ && file_pos + batch_total <=
            static_cast<byte_count_t>(mapped_size_)) {
            std::memcpy(batch_buf.data(),
                        static_cast<const char*>(mapped_data_) + file_pos,
                        batch_total);
        } else {
            n = pread(fd_, batch_buf.data(), batch_total, static_cast<off_t>(file_pos));
            if (static_cast<size_t>(n) != batch_total) break;
        }

        // Deserialize
        auto deser_res = deserialize_batch(batch_buf.data(), batch_total, next_off);
        if (deser_res.failed()) {
            result.error = deser_res.error;
            result.error_message = deser_res.error_message;
            return result;
        }

        auto batch = std::move(deser_res.value);
        result.batches.push_back(std::move(batch));
        bytes_read += static_cast<byte_count_t>(batch_total);
        file_pos += static_cast<byte_count_t>(batch_total);

        // Update next_offset from last batch
        int32_t rc = batch.record_count > 0 ? batch.record_count :
                     static_cast<int32_t>(batch.records.size());
        next_off = batch.base_offset + rc;
    }

    result.bytes_read = bytes_read;
    result.next_offset = next_off;
    result.is_truncated = (bytes_read >= max_bytes && file_pos < file_end);
    result.error = error_code::none;

    return result;
}

// ============================================================================
// Segment::read_range — read a range of offsets
// ============================================================================

result<SegmentReadResult> Segment::read_range(offset_t start_offset,
                                               offset_t end_offset,
                                               byte_count_t max_bytes) {
    SegmentReadResult accum;
    accum.next_offset = start_offset;

    while (accum.next_offset < end_offset) {
        auto res = read(accum.next_offset, max_bytes);
        if (res.failed()) {
            if (accum.batches.empty()) {
                return res;
            }
            break;
        }

        auto& batch_res = res.value;
        accum.batches.insert(accum.batches.end(),
                             std::make_move_iterator(batch_res.batches.begin()),
                             std::make_move_iterator(batch_res.batches.end()));
        accum.bytes_read += batch_res.bytes_read;
        accum.next_offset = batch_res.next_offset;

        if (batch_res.is_truncated) break;
    }

    accum.error = error_code::none;
    return accum;
}

// ============================================================================
// Segment::read_at — read exactly one record batch at offset
// ============================================================================

result<RecordBatch> Segment::read_at(offset_t offset) {
    auto read_res = read(offset, 104857600); // up to 100 MiB
    if (read_res.failed()) {
        return result<RecordBatch>::failure(read_res.error,
                                             read_res.error_message);
    }

    if (read_res.value.batches.empty()) {
        return result<RecordBatch>::failure(
            error_code::corrupt_message,
            "no batch at offset " + std::to_string(offset));
    }

    if (read_res.value.batches[0].base_offset != offset) {
        return result<RecordBatch>::failure(
            error_code::corrupt_message,
            "batch at offset " + std::to_string(offset) +
            " has base_offset " +
            std::to_string(read_res.value.batches[0].base_offset));
    }

    return result<RecordBatch>::success(
        std::move(read_res.value.batches[0]));
}

// ============================================================================
// Segment::fsync — durable flush including index
// ============================================================================

result<void> Segment::fsync() {
    if (fd_ < 0) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "segment not open");
    }

    if (::fsync(fd_) != 0) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "fsync: " + std::string(strerror(errno)));
    }

    return flush_index();
}

// ============================================================================
// Segment::flush — flush buffered writes to kernel
// ============================================================================

result<void> Segment::flush() {
    if (fd_ < 0) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "segment not open");
    }

    // On Linux, fdatasync is faster than fsync for data-only durability
#ifdef __linux__
    if (::fdatasync(fd_) != 0) {
#else
    if (::fsync(fd_) != 0) {
#endif
        return result<void>::failure(
            error_code::storage_unavailable,
            "flush: " + std::string(strerror(errno)));
    }

    return result<void>::success();
}

// ============================================================================
// Segment::open — create or load segment file
// ============================================================================

result<void> Segment::open() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (fd_ >= 0) {
        return result<void>::success(); // already open
    }

    bool exists = (::access(config_.file_path.c_str(), F_OK) == 0);

    int flags = O_RDWR;
    if (!exists) {
        flags |= O_CREAT;
    }

    fd_ = ::open(config_.file_path.c_str(), flags,
                 S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd_ < 0) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "open: " + std::string(strerror(errno)));
    }

    if (!exists) {
        // Initialize new segment
        header_.base_offset  = config_.base_offset;
        header_.segment_id   = config_.segment_id;
        header_.compression  = config_.compression;
        header_.created_at   = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        header_.format_version = kSegmentFormatV2;

        // Preallocate if configured
        if (config_.preallocate) {
            if (ftruncate(fd_, static_cast<off_t>(config_.max_segment_bytes)) != 0) {
                spdlog::warn("segment: preallocation failed for {}: {}",
                             config_.file_path, strerror(errno));
            }
        }

        auto wh_res = write_header();
        if (wh_res.failed()) {
            ::close(fd_);
            fd_ = -1;
            return wh_res;
        }

        file_size_.store(kSegmentHeaderSize, std::memory_order_release);
        next_offset_.store(config_.base_offset, std::memory_order_release);
        state_ = SegmentState::active;
    } else {
        // Load existing segment
        auto rh_res = read_header();
        if (rh_res.failed()) {
            ::close(fd_);
            fd_ = -1;
            return rh_res;
        }

        // Determine file size
        struct stat st;
        if (fstat(fd_, &st) == 0) {
            file_size_.store(static_cast<byte_count_t>(st.st_size),
                             std::memory_order_release);
        }

        // Set next_offset based on existing data
        // Rebuild index to compute exact next_offset
        auto reb_res = rebuild_index();
        if (reb_res.failed()) {
            // Non-fatal: continue without index (reads will be slow)
            spdlog::warn("segment: index rebuild failed: {}", reb_res.error_message);
        }

        state_ = header_.is_sealed() ? SegmentState::sealed : SegmentState::active;
        if (config_.read_only && state_ == SegmentState::active) {
            state_ = SegmentState::sealed;
        }
    }

    // Memory-map the file
    auto map_res = map_file();
    if (map_res.failed()) {
        spdlog::warn("segment: mmap failed, using buffered I/O: {}",
                     map_res.error_message);
    }

    spdlog::info("segment: opened {} (base={}, next={}, size={}, {}active)",
                 config_.file_path, header_.base_offset,
                 next_offset_.load(), file_size_.load(),
                 is_active() ? "" : "in");

    return result<void>::success();
}

// ============================================================================
// Segment::close — release resources
// ============================================================================

result<void> Segment::close() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (fd_ < 0) return result<void>::success();

    // Flush if active
    if (state_ == SegmentState::active) {
        flush_index();
    }

    unmap_file();

    if (::close(fd_) != 0) {
        spdlog::warn("segment: close failed: {}", strerror(errno));
    }
    fd_ = -1;

    state_ = SegmentState::closed;
    return result<void>::success();
}

// ============================================================================
// Segment Constructor / Destructor / Move
// ============================================================================

Segment::Segment(SegmentConfig config)
    : config_(std::move(config)),
      state_(SegmentState::uninitialized) {
    header_.base_offset = config_.base_offset;
    header_.segment_id  = config_.segment_id;
    header_.compression = config_.compression;
}

Segment::Segment(Segment&& other) noexcept
    : config_(std::move(other.config_)),
      header_(other.header_),
      state_(other.state_.load()),
      fd_(other.fd_),
      mapped_data_(other.mapped_data_),
      mapped_size_(other.mapped_size_),
      sparse_index_(std::move(other.sparse_index_)),
      index_dirty_(other.index_dirty_) {
    next_offset_.store(other.next_offset_.load(std::memory_order_acquire),
                        std::memory_order_release);
    file_size_.store(other.file_size_.load(std::memory_order_acquire),
                      std::memory_order_release);
    max_timestamp_.store(other.max_timestamp_.load(std::memory_order_acquire),
                          std::memory_order_release);
    bytes_written_.store(other.bytes_written_.load(std::memory_order_relaxed),
                          std::memory_order_relaxed);
    batches_appended_.store(other.batches_appended_.load(std::memory_order_relaxed),
                             std::memory_order_relaxed);
    records_appended_.store(other.records_appended_.load(std::memory_order_relaxed),
                             std::memory_order_relaxed);

    // Transfer file stream
    file_stream_ = std::move(other.file_stream_);

    // Null out source
    other.fd_ = -1;
    other.mapped_data_ = nullptr;
    other.mapped_size_ = 0;
    other.state_ = SegmentState::closed;
}

Segment& Segment::operator=(Segment&& other) noexcept {
    if (this != &other) {
        close();
        config_       = std::move(other.config_);
        header_       = other.header_;
        fd_           = other.fd_;
        mapped_data_  = other.mapped_data_;
        mapped_size_  = other.mapped_size_;
        sparse_index_ = std::move(other.sparse_index_);
        index_dirty_  = other.index_dirty_;
        file_stream_  = std::move(other.file_stream_);

        state_.store(other.state_.load(), std::memory_order_relaxed);
        next_offset_.store(other.next_offset_.load(), std::memory_order_release);
        file_size_.store(other.file_size_.load(), std::memory_order_release);
        max_timestamp_.store(other.max_timestamp_.load(), std::memory_order_release);
        bytes_written_.store(other.bytes_written_.load(), std::memory_order_relaxed);
        batches_appended_.store(other.batches_appended_.load(), std::memory_order_relaxed);
        records_appended_.store(other.records_appended_.load(), std::memory_order_relaxed);

        other.fd_ = -1;
        other.mapped_data_ = nullptr;
        other.mapped_size_ = 0;
        other.state_ = SegmentState::closed;
    }
    return *this;
}

Segment::~Segment() {
    auto res = close();
    if (res.failed()) {
        spdlog::error("segment: destructor close failed: {}", res.error_message);
    }
}

// ============================================================================
// Segment::index_file_path — resolve companion .index path
// ============================================================================

std::string Segment::index_file_path() const {
    if (!config_.index_file_path.empty()) {
        return config_.index_file_path;
    }
    return config_.file_path + ".index";
}

// ============================================================================
// Segment::mark_corrupted — transition to corrupted state
// ============================================================================

void Segment::mark_corrupted(const std::string& reason) {
    state_.store(SegmentState::corrupted, std::memory_order_release);
    spdlog::error("segment {}: marked corrupted: {}",
                  config_.file_path, reason);
}

// ============================================================================
// torrent-verify utility: main entry point
// ============================================================================

/// Standalone segment verification tool entry point.
/// Usage: torrent-verify <segment_path> [--repair] [--verbose]
extern "C" int torrent_verify_main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: torrent-verify <segment_path> [--verbose]\n";
        return 1;
    }

    std::string path = argv[1];
    bool verbose = false;
    for (int i = 2; i < argc; ++i) {
        if (std::string(argv[i]) == "--verbose") {
            verbose = true;
        }
    }

    SegmentVerifier verifier(verbose);
    auto result = verifier.verify(path);

    std::cout << result.summary();

    return result.all_ok() ? 0 : 1;
}

} // namespace torrent
