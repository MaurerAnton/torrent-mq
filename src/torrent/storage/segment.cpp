/**
 * torrent-mq — Segment Implementation
 *
 * Append-only log segment backed by an on-disk file.  Each segment stores a
 * contiguous range of logical offsets with CRC32C-protected record batches.
 * Reads use memory-mapped I/O when available with a fallback to buffered
 * fstream reads.  A sparse index maps logical offsets to file positions for
 * O(log N) lookups.
 *
 * Wire format (little-endian unless noted otherwise):
 *
 *   Segment header (64 bytes):
 *     [0..3]   Magic "TQSG" (0x47535154 LE)
 *     [4..5]   format_version (uint16_t)
 *     [6..7]   header_size (uint16_t, = 64)
 *     [8..11]  header_crc32c (covers [16..64))
 *     [12..15] reserved
 *     [16..23] base_offset (int64_t)
 *     [24..31] segment_id (uint64_t)
 *     [32..39] created_at (int64_t, ms epoch)
 *     [40]     compression_type (int8_t)
 *     [41]     flags (bit 0 = is_sealed)
 *     [42..63] reserved padding
 *
 *   RecordBatch (starts at byte 64 or after prior batch):
 *     int64   base_offset
 *     int32   batch_length (bytes following this field)
 *     int32   partition_leader_epoch
 *     int8    magic_byte (= 2)
 *     uint32  crc32c (covers from attributes to end of last record)
 *     int16   attributes
 *     int32   last_offset_delta
 *     int64   base_timestamp
 *     int64   max_timestamp
 *     int64   producer_id
 *     int16   producer_epoch
 *     int32   base_sequence
 *     int32   record_count
 *     Record[record_count] ...
 *
 *   Record:
 *     int32   record_length (excludes this field)
 *     int8    attributes
 *     int64   timestamp_delta (varint, signed zigzag)
 *     int32   offset_delta (varint, signed zigzag)
 *     int32   key_length (-1 = null)
 *     uint8[] key
 *     int32   value_length (-1 = null)
 *     uint8[] value
 *     int32   header_count
 *     Header[header_count] ...
 *
 *   Header:
 *     int32   key_length
 *     uint8[] key
 *     int32   value_length
 *     uint8[] value
 */

#include "torrent/storage/segment.h"
#include "torrent/storage/types.h"
#include "torrent/common/types.h"
#include "torrent/storage/disk_io.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstring>
#include <algorithm>
#include <cmath>
#include <cassert>
#include <ctime>
#include <unordered_map>
#include <sstream>

#include <spdlog/spdlog.h>

namespace torrent {

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_logger() {
    static auto logger = spdlog::get("segment");
    if (!logger) {
        logger = spdlog::stdout_color_mt("segment");
        logger->set_level(spdlog::level::debug);
    }
    return logger;
}

// --------------------------------------------------------------------------
// CRC32C (Castagnoli) — hardware-accelerated when SSE4.2 is available
// --------------------------------------------------------------------------

/// CRC32C polynomial: 0x1EDC6F41 (Castagnoli)
static constexpr uint32_t kCrc32cPoly = 0x82F63B78u;

/// Pre-computed CRC32C lookup table (256 entries for byte-at-a-time fallback).
static const std::array<uint32_t, 256> make_crc32c_table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc & 1) ? (crc >> 1) ^ kCrc32cPoly : (crc >> 1);
        }
        table[i] = crc;
    }
    return table;
}

static const auto kCrc32cTable = make_crc32c_table();

/// Compute CRC32C over a range of bytes.  Attempts to use SSE4.2 hardware
/// intrinsics at compile time; falls back to the lookup table otherwise.
uint32_t crc32c(uint32_t initial, const void* data, size_t len) noexcept {
    auto* p = static_cast<const uint8_t*>(data);
    uint32_t crc = initial ^ 0xFFFFFFFFu;

#ifdef __SSE4_2__
    // Process 8 bytes at a time with _mm_crc32_u64 when available.
    while (len >= 8) {
        uint64_t chunk;
        std::memcpy(&chunk, p, sizeof(chunk));
        crc = static_cast<uint32_t>(_mm_crc32_u64(crc, chunk));
        p += 8;
        len -= 8;
    }
    // Process remaining bytes with _mm_crc32_u8.
    while (len > 0) {
        crc = _mm_crc32_u8(crc, *p);
        ++p;
        --len;
    }
#else
    // Software fallback: process one byte at a time via lookup table.
    for (size_t i = 0; i < len; ++i) {
        crc = (crc >> 8) ^ kCrc32cTable[(crc & 0xFF) ^ p[i]];
    }
#endif

    return crc ^ 0xFFFFFFFFu;
}

/// Convenience: compute CRC32C with zero initial value.
uint32_t crc32c(const void* data, size_t len) noexcept {
    return crc32c(0, data, len);
}

// --------------------------------------------------------------------------
// Varint / ZigZag encoding (for record timestamp_delta, offset_delta)
// --------------------------------------------------------------------------

/// Maximum bytes needed for a 64-bit varint.
static constexpr size_t kMaxVarint64Len = 10;

/// Write an unsigned varint64 to buf; return number of bytes written.
size_t write_uvarint64(uint64_t value, uint8_t* buf) noexcept {
    size_t i = 0;
    while (value >= 0x80) {
        buf[i++] = static_cast<uint8_t>(value | 0x80);
        value >>= 7;
    }
    buf[i++] = static_cast<uint8_t>(value);
    return i;
}

/// Write a signed varint64 (zigzag-encoded) to buf; return number of bytes written.
size_t write_svarint64(int64_t value, uint8_t* buf) noexcept {
    // ZigZag encoding: map signed to unsigned.
    uint64_t zigzag = static_cast<uint64_t>((value << 1) ^ (value >> 63));
    return write_uvarint64(zigzag, buf);
}

/// Read an unsigned varint64 from buf; return {value, bytes_consumed}.
/// Returns {0,0} on buffer overflow (malformed input).
std::pair<uint64_t, size_t> read_uvarint64(const uint8_t* buf, size_t buf_len) noexcept {
    uint64_t value = 0;
    size_t shift = 0;
    size_t i = 0;
    while (i < buf_len && i < kMaxVarint64Len) {
        uint8_t byte = buf[i++];
        value |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            return {value, i};
        }
        shift += 7;
        if (shift >= 64) {
            // Too many bytes for a valid 64-bit varint.
            return {0, 0};
        }
    }
    // Buffer exhausted before last byte; malformed.
    return {0, 0};
}

/// Read a signed varint64 (zigzag-decoded) from buf.
/// Returns {value, bytes_consumed} or {0,0} on error.
std::pair<int64_t, size_t> read_svarint64(const uint8_t* buf, size_t buf_len) noexcept {
    auto [zigzag, consumed] = read_uvarint64(buf, buf_len);
    if (consumed == 0) return {0, 0};
    int64_t value = static_cast<int64_t>(zigzag >> 1);
    if (zigzag & 1) value = ~value;
    return {value, consumed};
}

/// Read an unsigned varint32.
std::pair<uint32_t, size_t> read_uvarint32(const uint8_t* buf, size_t buf_len) noexcept {
    auto [v, c] = read_uvarint64(buf, buf_len);
    return {static_cast<uint32_t>(v), c};
}

/// Read a signed varint32.
std::pair<int32_t, size_t> read_svarint32(const uint8_t* buf, size_t buf_len) noexcept {
    auto [v, c] = read_svarint64(buf, buf_len);
    return {static_cast<int32_t>(v), c};
}

// --------------------------------------------------------------------------
// Little-endian integer I/O (host ↔ LE wire format)
// --------------------------------------------------------------------------

/// Write a 64-bit little-endian integer to buf.
inline void write_le64(uint8_t* buf, uint64_t value) noexcept {
    buf[0] = static_cast<uint8_t>(value);
    buf[1] = static_cast<uint8_t>(value >> 8);
    buf[2] = static_cast<uint8_t>(value >> 16);
    buf[3] = static_cast<uint8_t>(value >> 24);
    buf[4] = static_cast<uint8_t>(value >> 32);
    buf[5] = static_cast<uint8_t>(value >> 40);
    buf[6] = static_cast<uint8_t>(value >> 48);
    buf[7] = static_cast<uint8_t>(value >> 56);
}
inline void write_le32(uint8_t* buf, uint32_t value) noexcept {
    buf[0] = static_cast<uint8_t>(value);
    buf[1] = static_cast<uint8_t>(value >> 8);
    buf[2] = static_cast<uint8_t>(value >> 16);
    buf[3] = static_cast<uint8_t>(value >> 24);
}
inline void write_le16(uint8_t* buf, uint16_t value) noexcept {
    buf[0] = static_cast<uint8_t>(value);
    buf[1] = static_cast<uint8_t>(value >> 8);
}
inline void write_le8(uint8_t* buf, uint8_t value) noexcept {
    buf[0] = value;
}

/// Read a 64-bit little-endian integer from buf.
inline uint64_t read_le64(const uint8_t* buf) noexcept {
    return static_cast<uint64_t>(buf[0])
         | (static_cast<uint64_t>(buf[1]) << 8)
         | (static_cast<uint64_t>(buf[2]) << 16)
         | (static_cast<uint64_t>(buf[3]) << 24)
         | (static_cast<uint64_t>(buf[4]) << 32)
         | (static_cast<uint64_t>(buf[5]) << 40)
         | (static_cast<uint64_t>(buf[6]) << 48)
         | (static_cast<uint64_t>(buf[7]) << 56);
}
inline uint32_t read_le32(const uint8_t* buf) noexcept {
    return static_cast<uint32_t>(buf[0])
         | (static_cast<uint32_t>(buf[1]) << 8)
         | (static_cast<uint32_t>(buf[2]) << 16)
         | (static_cast<uint32_t>(buf[3]) << 24);
}
inline uint16_t read_le16(const uint8_t* buf) noexcept {
    return static_cast<uint16_t>(buf[0])
         | (static_cast<uint16_t>(buf[1]) << 8);
}

/// Write a signed int64_t in little-endian (same bit pattern).
inline void write_le64s(uint8_t* buf, int64_t value) noexcept {
    write_le64(buf, static_cast<uint64_t>(value));
}

/// Read a signed int64_t in little-endian.
inline int64_t read_le64s(const uint8_t* buf) noexcept {
    return static_cast<int64_t>(read_le64(buf));
}
inline int32_t read_le32s(const uint8_t* buf) noexcept {
    return static_cast<int32_t>(read_le32(buf));
}

// --------------------------------------------------------------------------
// Time helpers
// --------------------------------------------------------------------------

/// Current time in milliseconds since epoch.
timestamp_ms_t now_ms() noexcept {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

/// Safe pwrite that retries on EINTR.
ssize_t safe_pwrite(int fd, const void* buf, size_t count, off_t offset) noexcept {
    const auto* bytes = static_cast<const uint8_t*>(buf);
    size_t written = 0;
    while (written < count) {
        ssize_t n = ::pwrite(fd, bytes + written, count - written,
                              static_cast<off_t>(offset) + static_cast<off_t>(written));
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) {
            // Unexpected EOF on write — disk full?
            errno = ENOSPC;
            return -1;
        }
        written += static_cast<size_t>(n);
    }
    return static_cast<ssize_t>(written);
}

/// Safe pread that retries on EINTR.
ssize_t safe_pread(int fd, void* buf, size_t count, off_t offset) noexcept {
    auto* bytes = static_cast<uint8_t*>(buf);
    size_t readb = 0;
    while (readb < count) {
        ssize_t n = ::pread(fd, bytes + readb, count - readb,
                             static_cast<off_t>(offset) + static_cast<off_t>(readb));
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) {
            // EOF reached.
            break;
        }
        readb += static_cast<size_t>(n);
    }
    return static_cast<ssize_t>(readb);
}

/// Safe write that retries on EINTR.
ssize_t safe_write(int fd, const void* buf, size_t count) noexcept {
    const auto* bytes = static_cast<const uint8_t*>(buf);
    size_t written = 0;
    while (written < count) {
        ssize_t n = ::write(fd, bytes + written, count - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) {
            errno = ENOSPC;
            return -1;
        }
        written += static_cast<size_t>(n);
    }
    return static_cast<ssize_t>(written);
}

/// Safe read that retries on EINTR.
ssize_t safe_read(int fd, void* buf, size_t count) noexcept {
    auto* bytes = static_cast<uint8_t*>(buf);
    size_t readb = 0;
    while (readb < count) {
        ssize_t n = ::read(fd, bytes + readb, count - readb);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) {
            break;
        }
        readb += static_cast<size_t>(n);
    }
    return static_cast<ssize_t>(readb);
}

/// Compute file size via fstat.
result<byte_count_t> get_file_size(int fd) {
    struct stat st;
    if (::fstat(fd, &st) < 0) {
        return result<byte_count_t>::failure(
            error_code::storage_unavailable,
            fmt::format("fstat failed: {} (errno={})", std::strerror(errno), errno));
    }
    return result<byte_count_t>::success(static_cast<byte_count_t>(st.st_size));
}

} // anonymous namespace

// ============================================================================
// File-scope constants
// ============================================================================

/// RecordBatch v2 magic byte (identifies the batch format version).
static constexpr int8_t kRecordBatchMagic = 2;

/// Minimum size of a valid RecordBatch header on disk.
static constexpr size_t kMinBatchHeaderSize = 8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4;
// base_offset(8) + length(4) + partition_leader_epoch(4) + magic(1) + crc(4) +
// attributes(2) + last_offset_delta(4) + base_timestamp(8) + max_timestamp(8) +
// producer_id(8) + producer_epoch(2) + base_sequence(4) + record_count(4) = 61

/// File size increase step when extending (64 KiB).
static constexpr byte_count_t kFileExtendStep = 65536;

/// Maximum batch size to prevent OOM during read (128 MiB).
static constexpr byte_count_t kMaxBatchBytes = 134217728;

// ============================================================================
// Construction / Destruction
// ============================================================================

Segment::Segment(SegmentConfig config)
    : config_(std::move(config))
    , state_(SegmentState::uninitialized)
{
    // Validate config.
    if (config_.file_path.empty()) {
        get_logger()->error("Segment constructed with empty file_path");
        mark_corrupted("Empty file_path in config");
        return;
    }

    if (config_.max_segment_bytes <= 0) {
        get_logger()->warn("Segment '{}' max_segment_bytes is {}; using 1 GiB default",
                           config_.file_path, config_.max_segment_bytes);
        config_.max_segment_bytes = 1073741824;
    }

    if (config_.index_interval_bytes <= 0) {
        config_.index_interval_bytes = kDefaultIndexInterval;
    }

    // Initialise the header fields from config.
    header_.base_offset  = config_.base_offset;
    header_.segment_id   = config_.segment_id;
    header_.compression  = config_.compression;
    header_.created_at   = now_ms();

    next_offset_.store(config_.base_offset, std::memory_order_release);
    max_timestamp_.store(0, std::memory_order_release);

    get_logger()->info("Segment({}) constructed: file='{}', base_offset={}, "
                       "segment_id={}, max_bytes={}, use_mmap={}, read_only={}",
                       static_cast<void*>(this),
                       config_.file_path,
                       config_.base_offset,
                       config_.segment_id,
                       config_.max_segment_bytes,
                       config_.use_mmap,
                       config_.read_only);
}

Segment::Segment(Segment&& other) noexcept
    : config_(std::move(other.config_))
    , header_(other.header_)
    , state_(other.state_)
    , next_offset_(other.next_offset_.load(std::memory_order_acquire))
    , file_size_(other.file_size_.load(std::memory_order_acquire))
    , max_timestamp_(other.max_timestamp_.load(std::memory_order_acquire))
    , fd_(other.fd_)
    , file_stream_(std::move(other.file_stream_))
    , mapped_data_(other.mapped_data_)
    , mapped_size_(other.mapped_size_)
    , sparse_index_(std::move(other.sparse_index_))
    , index_dirty_(other.index_dirty_)
    , bytes_written_(other.bytes_written_.load(std::memory_order_relaxed))
    , batches_appended_(other.batches_appended_.load(std::memory_order_relaxed))
    , records_appended_(other.records_appended_.load(std::memory_order_relaxed))
{
    // Clear the source's resources to avoid double-close.
    other.fd_          = -1;
    other.mapped_data_ = nullptr;
    other.mapped_size_ = 0;
    other.state_       = SegmentState::closed;
    other.next_offset_.store(kInvalidOffset, std::memory_order_release);
    other.file_size_.store(0, std::memory_order_release);

    get_logger()->debug("Segment moved: {} -> {}",
                        static_cast<void*>(&other), static_cast<void*>(this));
}

Segment& Segment::operator=(Segment&& other) noexcept {
    if (this != &other) {
        // Close current resources first.
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (fd_ >= 0) {
                ::close(fd_);
            }
            if (mapped_data_ && mapped_data_ != MAP_FAILED) {
                ::munmap(mapped_data_, mapped_size_);
            }
        }

        config_          = std::move(other.config_);
        header_          = other.header_;
        state_           = other.state_;
        next_offset_.store(other.next_offset_.load(std::memory_order_acquire),
                           std::memory_order_release);
        file_size_.store(other.file_size_.load(std::memory_order_acquire),
                         std::memory_order_release);
        max_timestamp_.store(other.max_timestamp_.load(std::memory_order_acquire),
                             std::memory_order_release);
        fd_              = other.fd_;
        file_stream_     = std::move(other.file_stream_);
        mapped_data_     = other.mapped_data_;
        mapped_size_     = other.mapped_size_;
        sparse_index_    = std::move(other.sparse_index_);
        index_dirty_     = other.index_dirty_;
        bytes_written_.store(other.bytes_written_.load(std::memory_order_relaxed),
                             std::memory_order_relaxed);
        batches_appended_.store(other.batches_appended_.load(std::memory_order_relaxed),
                                 std::memory_order_relaxed);
        records_appended_.store(other.records_appended_.load(std::memory_order_relaxed),
                                 std::memory_order_relaxed);

        other.fd_          = -1;
        other.mapped_data_ = nullptr;
        other.mapped_size_ = 0;
        other.state_       = SegmentState::closed;
        other.next_offset_.store(kInvalidOffset, std::memory_order_release);
        other.file_size_.store(0, std::memory_order_release);
    }
    return *this;
}

Segment::~Segment() {
    get_logger()->debug("Segment({}) destructor: state={}, fd={}",
                        static_cast<void*>(this),
                        static_cast<int>(state_.load(std::memory_order_acquire)),
                        fd_);

    // Best-effort close.  The user should have called close() explicitly;
    // if not, we attempt to flush and release resources here but cannot
    // report errors.
    if (state_ != SegmentState::closed && state_ != SegmentState::uninitialized) {
        try {
            auto result = close();
            if (result.failed()) {
                get_logger()->error("Segment({}) destructor close failed: {} ({})",
                                    static_cast<void*>(this),
                                    result.error_message,
                                    static_cast<int>(result.error));
            }
        } catch (const std::exception& e) {
            get_logger()->error("Segment({}) destructor exception: {}",
                                static_cast<void*>(this), e.what());
        } catch (...) {
            get_logger()->error("Segment({}) destructor unknown exception",
                                static_cast<void*>(this));
        }
    }
}

// ============================================================================
// Lifecycle: open / close
// ============================================================================

result<void> Segment::open() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (state_ == SegmentState::active || state_ == SegmentState::sealed) {
        get_logger()->warn("Segment({}) already open (state={}), ignoring open()",
                           static_cast<void*>(this),
                           static_cast<int>(state_.load(std::memory_order_acquire)));
        return result<void>::success();
    }

    if (state_ == SegmentState::corrupted) {
        return result<void>::failure(
            error_code::segment_corrupted,
            "Cannot open a corrupted segment; recovery is required first");
    }

    get_logger()->info("Segment({}) opening: file='{}'", static_cast<void*>(this), config_.file_path);

    // Step 1: Prepare the file (create or open existing).
    auto prepare_res = prepare_file();
    if (prepare_res.failed()) {
        mark_corrupted(prepare_res.error_message);
        return result<void>::failure(prepare_res.error, prepare_res.error_message);
    }

    // Step 2: The header is already read/written during prepare_file().
    //         Validate it again for safety.
    if (!header_.is_valid()) {
        mark_corrupted("Segment header validation failed after prepare_file");
        return result<void>::failure(
            error_code::segment_corrupted,
            "Segment header is invalid (bad magic, version, or base_offset)");
    }

    if (!validate_header_crc()) {
        mark_corrupted("Segment header CRC mismatch");
        return result<void>::failure(
            error_code::corrupt_message,
            "Header CRC32C check failed — segment may be corrupted");
    }

    // Step 3: Determine file size.
    auto sz_res = get_file_size(fd_);
    if (sz_res.failed()) {
        mark_corrupted(sz_res.error_message);
        return result<void>::failure(sz_res.error, sz_res.error_message);
    }
    file_size_.store(sz_res.value, std::memory_order_release);

    // Step 4: Memory-map the file if enabled.
    if (config_.use_mmap) {
        auto map_res = map_file();
        if (map_res.failed()) {
            get_logger()->warn("Segment({}) mmap failed: {} — falling back to fstream reads",
                               static_cast<void*>(this), map_res.error_message);
            // mmap failure is not fatal; reads fall back to file_stream_.
        }
    }

    // If mmap is not enabled or failed, open the fstream for fallback reads.
    if (!mapped_data_) {
        if (file_stream_.is_open()) {
            file_stream_.close();
        }
        // Open in binary read mode. Write path uses fd directly.
        file_stream_.open(config_.file_path,
                          std::ios::in | std::ios::binary | std::ios::ate);
        if (!file_stream_.is_open()) {
            get_logger()->error("Segment({}) failed to open fstream fallback for '{}'",
                                static_cast<void*>(this), config_.file_path);
            // Not fatal — reads will use pread.
        }
    }

    // Step 5: Load or rebuild the sparse index.
    auto load_res = load_index();
    if (load_res.failed()) {
        get_logger()->warn("Segment({}) loading index failed: {} — rebuilding",
                           static_cast<void*>(this), load_res.error_message);
        auto rebuild_res = rebuild_index();
        if (rebuild_res.failed()) {
            mark_corrupted("Failed to rebuild sparse index: " + rebuild_res.error_message);
            return result<void>::failure(rebuild_res.error, rebuild_res.error_message);
        }
    }

    // Step 6: If the sparse index is still empty but we have data (unlikely),
    //         scan the file to build it.
    if (sparse_index_.empty() && sz_res.value > kSegmentHeaderSize) {
        get_logger()->warn("Segment({}) has data but no index entries; rebuilding",
                           static_cast<void*>(this));
        auto rebuild_res = rebuild_index();
        if (rebuild_res.failed()) {
            mark_corrupted("Failed to rebuild sparse index on open: " + rebuild_res.error_message);
            return result<void>::failure(rebuild_res.error, rebuild_res.error_message);
        }
    }

    // Step 7: Determine next_offset from the file.
    //         If the file contains only the header, next_offset = base_offset.
    //         Otherwise, we scan to find the last valid batch and set next_offset
    //         past it.
    if (sz_res.value <= kSegmentHeaderSize) {
        next_offset_.store(header_.base_offset, std::memory_order_release);
    } else if (!sparse_index_.empty()) {
        // The last batch in the file — we need to read it to know its last
        // offset.  We'll scan from the last index entry to find all batches.
        byte_count_t scan_start = sparse_index_.back().file_position;
        offset_t cur_offset = sparse_index_.back().offset;

        // Read batches starting from scan_start until EOF.
        byte_count_t pos = scan_start;
        while (pos < sz_res.value) {
            if (static_cast<size_t>(pos + 12) > static_cast<size_t>(sz_res.value)) {
                // Not enough bytes for even a batch header; stop.
                break;
            }

            uint8_t batch_header[12]; // base_offset(8) + length(4)
            auto* src = static_cast<const char*>(mapped_data_);
            // We can read from memory (mapped_data_ or file).
            if (!mapped_data_) {
                ssize_t n = safe_pread(fd_, batch_header, sizeof(batch_header),
                                       static_cast<off_t>(pos));
                if (n != static_cast<ssize_t>(sizeof(batch_header))) {
                    get_logger()->warn("Segment({}) failed to read batch header at pos {}",
                                       static_cast<void*>(this), pos);
                    break;
                }
            } else {
                std::memcpy(batch_header, src + pos, sizeof(batch_header));
            }

            offset_t bo = read_le64s(batch_header);
            int32_t blen = read_le32s(batch_header + 8);

            if (blen <= 0 || blen > kMaxBatchBytes) {
                get_logger()->warn("Segment({}) invalid batch length {} at pos {}",
                                   static_cast<void*>(this), blen, pos);
                break;
            }

            // Advance past this batch.
            byte_count_t batch_total = 8 + 4 + static_cast<byte_count_t>(blen);
            pos += batch_total;
            cur_offset = bo + 1; // move past this batch
        }

        next_offset_.store(cur_offset, std::memory_order_release);
    }

    // Step 8: Set state.
    if (header_.is_sealed() || config_.read_only) {
        state_ = SegmentState::sealed;
        get_logger()->info("Segment({}) opened in sealed/read-only state", static_cast<void*>(this));
    } else {
        state_ = SegmentState::active;
        get_logger()->info("Segment({}) opened in active state", static_cast<void*>(this));
    }

    // Step 9: Update max_timestamp from the index.
    {
        std::lock_guard<std::mutex> idx_lock(index_mutex_);
        if (!sparse_index_.empty()) {
            max_timestamp_.store(sparse_index_.back().timestamp, std::memory_order_release);
        }
    }

    get_logger()->info("Segment({}) open complete: next_offset={}, file_size={}, "
                       "index_entries={}, state={}",
                       static_cast<void*>(this),
                       next_offset_.load(),
                       file_size_.load(),
                       sparse_index_.size(),
                       static_cast<int>(state_.load(std::memory_order_acquire)));

    return result<void>::success();
}

result<void> Segment::close() {
    std::lock_guard<std::mutex> lock(mutex_);

    auto current_state = state_.load(std::memory_order_acquire);

    if (current_state == SegmentState::closed) {
        get_logger()->debug("Segment({}) already closed, ignoring close()",
                            static_cast<void*>(this));
        return result<void>::success();
    }

    if (current_state == SegmentState::uninitialized) {
        get_logger()->debug("Segment({}) never opened, marking as closed",
                            static_cast<void*>(this));
        state_ = SegmentState::closed;
        return result<void>::success();
    }

    get_logger()->info("Segment({}) closing: file='{}', state={}, file_size={}",
                       static_cast<void*>(this),
                       config_.file_path,
                       static_cast<int>(current_state),
                       file_size_.load());

    result<void> final_result = result<void>::success();
    std::string error_accum;

    // Step 1: Flush dirty data to OS page cache.
    if (fd_ >= 0) {
        auto flush_res = flush();
        if (flush_res.failed()) {
            error_accum += "flush failed: " + flush_res.error_message + "; ";
            final_result = result<void>::failure(flush_res.error, error_accum);
            get_logger()->error("Segment({}) flush failed during close: {}",
                                static_cast<void*>(this), flush_res.error_message);
        }
    }

    // Step 2: fsync for full durability.
    if (fd_ >= 0) {
        auto sync_res = fsync();
        if (sync_res.failed()) {
            error_accum += "fsync failed: " + sync_res.error_message + "; ";
            if (final_result.ok()) {
                final_result = result<void>::failure(sync_res.error, error_accum);
            }
            get_logger()->error("Segment({}) fsync failed during close: {}",
                                static_cast<void*>(this), sync_res.error_message);
        }
    }

    // Step 3: Unmap memory region (safe to call when mapped_data_ is nullptr).
    unmap_file();

    // Step 4: Close the file descriptor.
    if (fd_ >= 0) {
        get_logger()->debug("Segment({}) closing fd={}", static_cast<void*>(this), fd_);
        if (::close(fd_) < 0) {
            std::string msg = fmt::format("close fd failed: {} (errno={})",
                                          std::strerror(errno), errno);
            error_accum += msg + "; ";
            if (final_result.ok()) {
                final_result = result<void>::failure(error_code::storage_unavailable, error_accum);
            }
            get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        }
        fd_ = -1;
    }

    // Step 5: Close the fallback fstream.
    if (file_stream_.is_open()) {
        file_stream_.close();
        if (file_stream_.fail()) {
            get_logger()->warn("Segment({}) fstream close reported failure",
                               static_cast<void*>(this));
        }
    }

    // Step 6: Transition state.
    state_ = SegmentState::closed;
    file_size_.store(0, std::memory_order_release);
    next_offset_.store(kInvalidOffset, std::memory_order_release);

    get_logger()->info("Segment({}) closed successfully", static_cast<void*>(this));
    return final_result;
}

// ============================================================================
// Write path: append / append_batch
// ============================================================================

result<SegmentAppendResult> Segment::append(const RecordBatch& batch) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Pre-condition check.
    if (!is_active()) {
        return result<SegmentAppendResult>::failure(
            error_code::not_leader_for_partition,
            fmt::format("Segment is not active (state={}, sealed={}, read_only={})",
                        static_cast<int>(state_.load(std::memory_order_acquire)),
                        header_.is_sealed(),
                        config_.read_only));
    }

    if (batch.records.empty()) {
        get_logger()->warn("Segment({}) append called with empty batch; returning success",
                           static_cast<void*>(this));
        SegmentAppendResult res;
        res.base_offset     = next_offset_.load(std::memory_order_acquire);
        res.file_position   = file_size_.load(std::memory_order_acquire);
        res.batches_written = 0;
        res.records_written = 0;
        res.append_time     = now_ms();
        res.error           = error_code::none;
        res.error_message   = "empty batch";
        return result<SegmentAppendResult>::success(std::move(res));
    }

    auto append_start = now_ms();
    get_logger()->debug("Segment({}) append: {} records at base_offset candidate={}",
                        static_cast<void*>(this),
                        batch.records.size(),
                        next_offset_.load(std::memory_order_acquire));

    // Step 1: Serialize the RecordBatch to wire format.
    auto [serialized, ser_len] = serialize_batch(batch);
    if (!serialized || ser_len == 0) {
        return result<SegmentAppendResult>::failure(
            error_code::invalid_record,
            "Failed to serialize RecordBatch");
    }

    // Step 2: Assign the base_offset from our monotonically-increasing counter.
    offset_t assigned_offset = next_offset_.load(std::memory_order_acquire);

    // Step 3: Write the serialized bytes to the file.
    byte_count_t current_size = file_size_.load(std::memory_order_acquire);
    auto write_pos = current_size;

    // Extend the file if needed.
    if (write_pos + static_cast<byte_count_t>(ser_len) > current_size) {
        byte_count_t needed = (write_pos + static_cast<byte_count_t>(ser_len)) - current_size;
        auto ext_res = extend_file(needed);
        if (ext_res.failed()) {
            return result<SegmentAppendResult>::failure(ext_res.error, ext_res.error_message);
        }
    }

    // Now write using pwrite at the current end-of-file position.
    // We manually handle offset assignment within the serialized data.
    // The base_offset was serialized as whatever was in the batch; we need to
    // patch it in the serialized buffer.
    {
        // Patch base_offset in the serialized buffer (first 8 bytes).
        auto* header_ptr = reinterpret_cast<uint8_t*>(serialized.get());
        write_le64s(header_ptr, assigned_offset);

        // Recompute and patch CRC since base_offset changed.
        // The CRC covers from `attributes` through end of records.
        // attributes starts at offset 8+4+4+1 = 17 in the batch header.
        size_t crc_start = 8 + 4 + 4 + 1; // skip base_offset, length, leader_epoch, magic
        size_t crc_end   = ser_len;
        if (crc_end > crc_start) {
            uint32_t new_crc = crc32c(header_ptr + crc_start, crc_end - crc_start);
            write_le32(header_ptr + crc_start, new_crc);
        }
    }

    ssize_t written = safe_pwrite(fd_, serialized.get(), ser_len,
                                   static_cast<off_t>(write_pos));
    if (written < 0 || static_cast<size_t>(written) != ser_len) {
        std::string msg = fmt::format("pwrite failed: {} (errno={}), wrote {}/{} bytes",
                                      std::strerror(errno), errno,
                                      (written >= 0 ? written : 0), ser_len);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<SegmentAppendResult>::failure(error_code::storage_unavailable, msg);
    }

    get_logger()->debug("Segment({}) wrote {} bytes at pos {} (base_offset={})",
                        static_cast<void*>(this), ser_len, write_pos, assigned_offset);

    // Step 4: Update file size.
    byte_count_t new_size = write_pos + static_cast<byte_count_t>(ser_len);
    file_size_.store(new_size, std::memory_order_release);

    // Step 5: Update next_offset.
    offset_t new_next = assigned_offset + static_cast<offset_t>(batch.records.size());
    next_offset_.store(new_next, std::memory_order_release);

    // Step 6: Update max_timestamp.
    timestamp_ms_t batch_max_ts = batch.max_timestamp > 0 ? batch.max_timestamp : batch.base_timestamp;
    if (batch_max_ts > 0) {
        timestamp_ms_t cur_max = max_timestamp_.load(std::memory_order_acquire);
        while (batch_max_ts > cur_max) {
            if (max_timestamp_.compare_exchange_weak(cur_max, batch_max_ts,
                                                      std::memory_order_release,
                                                      std::memory_order_acquire)) {
                break;
            }
        }
    }

    // Step 7: Update sparse index.
    update_sparse_index(assigned_offset, write_pos, batch_max_ts,
                        static_cast<int32_t>(batch.records.size()));

    // Step 8: Update metrics.
    bytes_written_.fetch_add(static_cast<byte_count_t>(ser_len), std::memory_order_relaxed);
    batches_appended_.fetch_add(1, std::memory_order_relaxed);
    records_appended_.fetch_add(static_cast<int64_t>(batch.records.size()),
                                 std::memory_order_relaxed);

    // Step 9: Sync if configured.
    if (config_.sync_on_append) {
        auto sync_res = flush();
        if (sync_res.failed()) {
            get_logger()->warn("Segment({}) sync_on_append flush failed: {}",
                               static_cast<void*>(this), sync_res.error_message);
            // Non-fatal: the write is still valid in the page cache.
        }
    }

    // Step 10: Build result.
    SegmentAppendResult res;
    res.base_offset     = assigned_offset;
    res.file_position   = write_pos;
    res.batches_written = 1;
    res.records_written = static_cast<int32_t>(batch.records.size());
    res.append_time     = append_start;
    res.error           = error_code::none;

    get_logger()->debug("Segment({}) append complete: base_offset={}, pos={}, "
                        "records={}, next_offset={}",
                        static_cast<void*>(this),
                        assigned_offset, write_pos,
                        batch.records.size(), new_next);

    return result<SegmentAppendResult>::success(std::move(res));
}

result<SegmentAppendResult> Segment::append_batch(std::vector<RecordBatch> batches) {
    if (batches.empty()) {
        SegmentAppendResult empty_res;
        empty_res.error         = error_code::none;
        empty_res.base_offset   = next_offset_.load(std::memory_order_acquire);
        empty_res.append_time   = now_ms();
        empty_res.error_message = "no batches to append";
        return result<SegmentAppendResult>::success(std::move(empty_res));
    }

    std::lock_guard<std::mutex> lock(mutex_);

    if (!is_active()) {
        return result<SegmentAppendResult>::failure(
            error_code::not_leader_for_partition,
            "Segment is not active for batch append");
    }

    get_logger()->debug("Segment({}) append_batch: {} batches",
                        static_cast<void*>(this), batches.size());

    auto batch_start_time = now_ms();
    offset_t first_offset = next_offset_.load(std::memory_order_acquire);
    byte_count_t first_position = file_size_.load(std::memory_order_acquire);
    int32_t total_records = 0;
    int32_t total_batches = 0;
    timestamp_ms_t batch_max_ts = 0;

    // Pre-serialize all batches to compute total size.
    struct PendingWrite {
        std::unique_ptr<char[]> data;
        size_t                   len;
        offset_t                 base_offset;
    };
    std::vector<PendingWrite> pending;
    pending.reserve(batches.size());

    byte_count_t current_next = first_position;
    offset_t current_offset = first_offset;

    for (auto& batch : batches) {
        auto [ser_data, ser_len] = serialize_batch(batch);
        if (!ser_data || ser_len == 0) {
            // Rollback: truncate to first_position.
            get_logger()->error("Segment({}) append_batch: serialization failed at batch {}; "
                                "truncating to offset {}",
                                static_cast<void*>(this), pending.size(), first_offset);
            auto trunc_res = truncate_to(first_offset);
            if (trunc_res.failed()) {
                get_logger()->error("Segment({}) truncate rollback failed: {}",
                                    static_cast<void*>(this), trunc_res.error_message);
            }
            return result<SegmentAppendResult>::failure(
                error_code::invalid_record,
                fmt::format("Failed to serialize batch {} of {}", pending.size(), batches.size()));
        }

        // Patch base_offset.
        auto* hdr = reinterpret_cast<uint8_t*>(ser_data.get());
        write_le64s(hdr, current_offset);

        // Patch CRC.
        size_t crc_start = 8 + 4 + 4 + 1;
        if (ser_len > crc_start) {
            uint32_t new_crc = crc32c(hdr + crc_start, ser_len - crc_start);
            write_le32(hdr + crc_start, new_crc);
        }

        pending.push_back({std::move(ser_data), ser_len, current_offset});

        current_offset += static_cast<offset_t>(batch.records.size());
        current_next   += static_cast<byte_count_t>(ser_len);
        total_records  += static_cast<int32_t>(batch.records.size());
        total_batches++;

        timestamp_ms_t bts = batch.max_timestamp > 0 ? batch.max_timestamp : batch.base_timestamp;
        if (bts > batch_max_ts) batch_max_ts = bts;
    }

    // Extend the file if needed.
    byte_count_t total_bytes = current_next - first_position;
    byte_count_t final_size = first_position + total_bytes;
    if (final_size > file_size_.load(std::memory_order_acquire)) {
        byte_count_t needed = final_size - file_size_.load(std::memory_order_acquire);
        auto ext_res = extend_file(needed);
        if (ext_res.failed()) {
            return result<SegmentAppendResult>::failure(ext_res.error, ext_res.error_message);
        }
    }

    // Write all serialized data sequentially.
    for (size_t i = 0; i < pending.size(); ++i) {
        auto& pw = pending[i];
        ssize_t written = safe_pwrite(fd_, pw.data.get(), pw.len,
                                       static_cast<off_t>(first_position));
        if (written < 0 || static_cast<size_t>(written) != pw.len) {
            std::string msg = fmt::format(
                "pwrite failed for batch {}: {} (errno={}), wrote {}/{} bytes",
                i, std::strerror(errno), errno,
                (written >= 0 ? written : 0), pw.len);
            get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);

            // Attempt rollback.
            auto trunc_res = truncate_to(first_offset);
            if (trunc_res.failed()) {
                get_logger()->error("Segment({}) truncate rollback after partial write failed: {}",
                                    static_cast<void*>(this), trunc_res.error_message);
            }
            return result<SegmentAppendResult>::failure(error_code::storage_unavailable, msg);
        }

        // Update sparse index for this batch.
        update_sparse_index(pw.base_offset, first_position, batch_max_ts,
                            static_cast<int32_t>(batches[i].records.size()));

        first_position += static_cast<byte_count_t>(pw.len);
    }

    // Update state.
    file_size_.store(final_size, std::memory_order_release);
    next_offset_.store(current_offset, std::memory_order_release);

    if (batch_max_ts > 0) {
        timestamp_ms_t cur_max = max_timestamp_.load(std::memory_order_acquire);
        while (batch_max_ts > cur_max) {
            if (max_timestamp_.compare_exchange_weak(cur_max, batch_max_ts,
                                                      std::memory_order_release,
                                                      std::memory_order_acquire)) {
                break;
            }
        }
    }

    bytes_written_.fetch_add(total_bytes, std::memory_order_relaxed);
    batches_appended_.fetch_add(total_batches, std::memory_order_relaxed);
    records_appended_.fetch_add(total_records, std::memory_order_relaxed);

    if (config_.sync_on_append) {
        auto sync_res = flush();
        if (sync_res.failed()) {
            get_logger()->warn("Segment({}) sync_on_append flush failed: {}",
                               static_cast<void*>(this), sync_res.error_message);
        }
    }

    SegmentAppendResult res;
    res.base_offset     = first_offset;
    res.file_position   = first_position - total_bytes;
    res.batches_written = total_batches;
    res.records_written = total_records;
    res.append_time     = batch_start_time;
    res.error           = error_code::none;

    get_logger()->info("Segment({}) append_batch complete: {} batches, {} records, "
                       "base_offset={}, size+= {}",
                       static_cast<void*>(this),
                       total_batches, total_records,
                       first_offset, total_bytes);

    return result<SegmentAppendResult>::success(std::move(res));
}

// ============================================================================
// Write path: flush / fsync
// ============================================================================

result<void> Segment::flush() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (fd_ < 0) {
        return result<void>::failure(error_code::storage_unavailable, "Segment not open (no fd)");
    }

    get_logger()->debug("Segment({}) flush (fdatasync)", static_cast<void*>(this));

    // fdatasync: flush file data (not metadata like mtime).
    if (::fdatasync(fd_) < 0) {
        std::string msg = fmt::format("fdatasync failed: {} (errno={})",
                                      std::strerror(errno), errno);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<void>::failure(error_code::storage_unavailable, msg);
    }

    get_logger()->debug("Segment({}) flush complete", static_cast<void*>(this));
    return result<void>::success();
}

result<void> Segment::fsync() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (fd_ < 0) {
        return result<void>::failure(error_code::storage_unavailable, "Segment not open (no fd)");
    }

    get_logger()->debug("Segment({}) fsync", static_cast<void*>(this));

    // Step 1: fsync the data file.
    if (::fsync(fd_) < 0) {
        std::string msg = fmt::format("fsync data file failed: {} (errno={})",
                                      std::strerror(errno), errno);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<void>::failure(error_code::storage_unavailable, msg);
    }

    // Step 2: Flush the sparse index to its companion file.
    auto flush_idx_res = flush_index();
    if (flush_idx_res.failed()) {
        get_logger()->warn("Segment({}) fsync: index flush failed: {}",
                           static_cast<void*>(this), flush_idx_res.error_message);
        // Non-fatal: index is recoverable.
    }

    get_logger()->debug("Segment({}) fsync complete", static_cast<void*>(this));
    return result<void>::success();
}

// ============================================================================
// Read path: read / read_range / read_at
// ============================================================================

result<SegmentReadResult> Segment::read(offset_t start_offset,
                                        byte_count_t max_bytes) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (state_ == SegmentState::uninitialized || state_ == SegmentState::closed) {
        return result<SegmentReadResult>::failure(
            error_code::storage_unavailable,
            "Segment is not open for reading");
    }

    if (state_ == SegmentState::corrupted) {
        return result<SegmentReadResult>::failure(
            error_code::segment_corrupted,
            "Segment is corrupted; reads are not allowed");
    }

    get_logger()->debug("Segment({}) read: start_offset={}, max_bytes={}",
                        static_cast<void*>(this), start_offset, max_bytes);

    offset_t current_next = next_offset_.load(std::memory_order_acquire);

    // Range check.
    if (start_offset < header_.base_offset) {
        return result<SegmentReadResult>::failure(
            error_code::offset_out_of_range,
            fmt::format("start_offset {} < base_offset {}",
                        start_offset, header_.base_offset));
    }

    if (start_offset >= current_next) {
        // No data available at or past this offset.
        SegmentReadResult empty_res;
        empty_res.bytes_read  = 0;
        empty_res.next_offset = current_next;
        empty_res.is_truncated = false;
        empty_res.error       = error_code::none;
        return result<SegmentReadResult>::success(std::move(empty_res));
    }

    // Step 1: Find the file position using the sparse index.
    auto pos_opt = find_position(start_offset);
    if (!pos_opt.has_value()) {
        // Fallback: scan from the beginning of data.
        get_logger()->warn("Segment({}) find_position returned nullopt for offset {}; "
                           "scanning from data start",
                           static_cast<void*>(this), start_offset);
        return result<SegmentReadResult>::failure(
            error_code::offset_out_of_range,
            fmt::format("Cannot find file position for offset {}", start_offset));
    }

    byte_count_t scan_pos = pos_opt.value();

    // Step 2: Scan forward from scan_pos, parsing RecordBatches until
    //         we reach start_offset or run out of data.
    byte_count_t file_sz = file_size_.load(std::memory_order_acquire);
    SegmentReadResult result;
    result.error       = error_code::none;
    result.next_offset = start_offset;
    offset_t current_offset = kInvalidOffset;

    const char* mapped = static_cast<const char*>(mapped_data_);
    std::vector<uint8_t> read_buf;

    while (scan_pos < file_sz) {
        // Read batch header: base_offset(8) + batch_length(4).
        if (scan_pos + 12 > file_sz) {
            break; // incomplete batch at EOF
        }

        uint8_t bh_buf[12];
        if (mapped) {
            std::memcpy(bh_buf, mapped + scan_pos, 12);
        } else {
            ssize_t n = safe_pread(fd_, bh_buf, 12, static_cast<off_t>(scan_pos));
            if (n != 12) {
                get_logger()->warn("Segment({}) failed to read batch header at pos {}",
                                   static_cast<void*>(this), scan_pos);
                break;
            }
        }

        offset_t bo = read_le64s(bh_buf);
        int32_t  blen = read_le32s(bh_buf + 8);

        if (blen <= 0 || blen > kMaxBatchBytes) {
            get_logger()->warn("Segment({}) invalid batch length {} at pos {}, offset hint={}",
                               static_cast<void*>(this), blen, scan_pos, bo);
            break;
        }

        byte_count_t batch_total = 12 + static_cast<byte_count_t>(blen);
        if (scan_pos + batch_total > file_sz) {
            get_logger()->warn("Segment({}) truncated batch at pos {} (need {} bytes, have {})",
                               static_cast<void*>(this), scan_pos, batch_total, file_sz - scan_pos);
            break;
        }

        // If this batch ends before start_offset, skip it.
        offset_t batch_end_offset = bo + 1; // approximate: we need to read record_count

        // Read the full batch into memory.
        read_buf.resize(static_cast<size_t>(batch_total));
        if (mapped) {
            std::memcpy(read_buf.data(), mapped + scan_pos, static_cast<size_t>(batch_total));
        } else {
            ssize_t n = safe_pread(fd_, read_buf.data(), static_cast<size_t>(batch_total),
                                   static_cast<off_t>(scan_pos));
            if (n != static_cast<ssize_t>(batch_total)) {
                get_logger()->warn("Segment({}) failed to read full batch at pos {}",
                                   static_cast<void*>(this), scan_pos);
                break;
            }
        }

        // Parse the batch to get actual record_count and determine batch end offset.
        auto batch_data = read_buf.data() + 12; // skip base_offset + length
        size_t batch_data_len = static_cast<size_t>(blen);

        // Extract record_count (offset 8+4+4+1+4+2+4+8+8+8+2+4 = 57 in batch_data)
        if (batch_data_len < 61) {
            get_logger()->warn("Segment({}) batch too small at pos {}", static_cast<void*>(this), scan_pos);
            break;
        }
        int32_t record_count = read_le32s(read_buf.data() + 12 + 57);
        if (record_count < 0 || record_count > 1000000) {
            get_logger()->warn("Segment({}) insane record_count {} at pos {}",
                               static_cast<void*>(this), record_count, scan_pos);
            break;
        }

        batch_end_offset = bo + record_count;

        if (batch_end_offset <= start_offset) {
            // This batch is fully before our target; skip to next.
            scan_pos += batch_total;
            current_offset = batch_end_offset;
            continue;
        }

        // This batch overlaps with our range. Deserialize it.
        auto deser = deserialize_batch(
            reinterpret_cast<const char*>(read_buf.data()),
            static_cast<size_t>(batch_total),
            bo);
        if (deser.failed()) {
            get_logger()->error("Segment({}) deserialize failed at offset {}: {}",
                                static_cast<void*>(this), bo, deser.error_message);
            result.error = deser.error;
            result.error_message = deser.error_message;
            return result<SegmentReadResult>::failure(deser.error, deser.error_message);
        }

        result.batches.push_back(std::move(deser.value));
        result.bytes_read += batch_total;
        result.next_offset = batch_end_offset;
        current_offset = batch_end_offset;

        scan_pos += batch_total;

        // Check if we've read enough.
        if (result.bytes_read >= max_bytes) {
            result.is_truncated = true;
            break;
        }
    }

    if (result.batches.empty()) {
        result.error = error_code::none;
        result.next_offset = current_next;
        get_logger()->debug("Segment({}) read returned 0 batches for offset {}",
                            static_cast<void*>(this), start_offset);
    } else {
        get_logger()->debug("Segment({}) read returned {} batches, {} bytes, next_offset={}",
                            static_cast<void*>(this),
                            result.batches.size(),
                            result.bytes_read,
                            result.next_offset);
    }

    return result<SegmentReadResult>::success(std::move(result));
}

result<SegmentReadResult> Segment::read_range(offset_t start_offset,
                                              offset_t end_offset,
                                              byte_count_t max_bytes) {
    get_logger()->debug("Segment({}) read_range: [{}, {}) max_bytes={}",
                        static_cast<void*>(this), start_offset, end_offset, max_bytes);

    if (start_offset >= end_offset) {
        SegmentReadResult empty_res;
        empty_res.error = error_code::none;
        empty_res.next_offset = start_offset;
        return result<SegmentReadResult>::success(std::move(empty_res));
    }

    SegmentReadResult accumulated;
    accumulated.error       = error_code::none;
    accumulated.next_offset = start_offset;
    accumulated.bytes_read  = 0;

    offset_t cursor = start_offset;
    while (cursor < end_offset && accumulated.bytes_read < max_bytes) {
        byte_count_t remaining_bytes = max_bytes - accumulated.bytes_read;
        byte_count_t chunk_max = std::min(remaining_bytes, static_cast<byte_count_t>(1048576)); // 1 MiB chunks

        auto read_res = read(cursor, chunk_max);
        if (read_res.failed()) {
            // Propagate the error.
            return result<SegmentReadResult>::failure(read_res.error, read_res.error_message);
        }

        auto& chunk = read_res.value;
        if (chunk.batches.empty()) {
            // No more data available.
            break;
        }

        // Move batches from chunk to accumulated.
        for (auto& b : chunk.batches) {
            accumulated.batches.push_back(std::move(b));
        }
        accumulated.bytes_read  += chunk.bytes_read;
        accumulated.next_offset  = chunk.next_offset;

        cursor = chunk.next_offset;

        if (chunk.is_truncated) {
            accumulated.is_truncated = true;
            break;
        }
    }

    if (accumulated.batches.empty() && cursor < end_offset) {
        // We didn't find anything; that's OK — just return empty.
        accumulated.next_offset = end_offset;
    }

    get_logger()->debug("Segment({}) read_range complete: {} batches, {} bytes",
                        static_cast<void*>(this),
                        accumulated.batches.size(),
                        accumulated.bytes_read);

    return result<SegmentReadResult>::success(std::move(accumulated));
}

result<RecordBatch> Segment::read_at(offset_t offset) {
    get_logger()->debug("Segment({}) read_at: offset={}", static_cast<void*>(this), offset);

    // Use read() with a small max_bytes to get just the batch at `offset`.
    auto read_res = read(offset, kMaxBatchBytes);
    if (read_res.failed()) {
        return result<RecordBatch>::failure(read_res.error, read_res.error_message);
    }

    auto& res = read_res.value;
    if (res.batches.empty()) {
        return result<RecordBatch>::failure(
            error_code::corrupt_message,
            fmt::format("No batch found at offset {}", offset));
    }

    // The first batch should be the one at `offset`.
    // Verify that it indeed starts at the requested offset.
    if (res.batches[0].base_offset != offset) {
        return result<RecordBatch>::failure(
            error_code::corrupt_message,
            fmt::format("Batch at offset {} has base_offset {}, expected {}",
                        offset, res.batches[0].base_offset, offset));
    }

    return result<RecordBatch>::success(std::move(res.batches[0]));
}

// ============================================================================
// Truncation
// ============================================================================

result<void> Segment::truncate_to(offset_t new_end_offset) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (fd_ < 0) {
        return result<void>::failure(error_code::storage_unavailable, "Segment not open (no fd)");
    }

    offset_t cur_next = next_offset_.load(std::memory_order_acquire);

    get_logger()->info("Segment({}) truncate_to: {} (current next_offset={})",
                       static_cast<void*>(this), new_end_offset, cur_next);

    // Validate range.
    if (new_end_offset < header_.base_offset) {
        return result<void>::failure(
            error_code::offset_out_of_range,
            fmt::format("new_end_offset {} < base_offset {}",
                        new_end_offset, header_.base_offset));
    }

    if (new_end_offset > cur_next) {
        return result<void>::failure(
            error_code::offset_out_of_range,
            fmt::format("new_end_offset {} > next_offset {}",
                        new_end_offset, cur_next));
    }

    if (new_end_offset == cur_next) {
        get_logger()->debug("Segment({}) truncate_to: no-op (same offset)", static_cast<void*>(this));
        return result<void>::success();
    }

    // Step 1: Find the file position for new_end_offset.
    byte_count_t new_file_size = kSegmentHeaderSize; // minimum: just the header
    if (new_end_offset > header_.base_offset) {
        auto pos_opt = find_position(new_end_offset);
        if (pos_opt.has_value()) {
            new_file_size = pos_opt.value();
        } else {
            // If we can't find via index, scan the file to find the right position.
            get_logger()->warn("Segment({}) cannot find position for offset {}; "
                               "using header-only size",
                               static_cast<void*>(this), new_end_offset);
            // Scan forward from header to find batches up to new_end_offset.
            byte_count_t scan_pos = kSegmentHeaderSize;
            byte_count_t file_sz = file_size_.load(std::memory_order_acquire);
            const char* mapped = static_cast<const char*>(mapped_data_);
            std::vector<uint8_t> bh(12);

            while (scan_pos < file_sz) {
                if (scan_pos + 12 > file_sz) break;
                if (mapped) {
                    std::memcpy(bh.data(), mapped + scan_pos, 12);
                } else {
                    ssize_t n = safe_pread(fd_, bh.data(), 12, static_cast<off_t>(scan_pos));
                    if (n != 12) break;
                }
                offset_t bo = read_le64s(bh.data());
                int32_t  blen = read_le32s(bh.data() + 8);
                if (blen <= 0 || blen > kMaxBatchBytes) break;

                byte_count_t batch_total = 12 + static_cast<byte_count_t>(blen);
                if (scan_pos + batch_total > file_sz) break;

                // Read record_count to determine batch end offset.
                if (blen >= 57) {
                    int32_t rc;
                    if (mapped) {
                        rc = read_le32s(reinterpret_cast<const uint8_t*>(mapped + scan_pos + 12 + 57));
                    } else {
                        uint8_t rc_buf[4];
                        safe_pread(fd_, rc_buf, 4, static_cast<off_t>(scan_pos + 12 + 57));
                        rc = read_le32s(rc_buf);
                    }
                    if (rc > 0 && bo + rc <= new_end_offset) {
                        // This batch is fully before the truncation point.
                        scan_pos += batch_total;
                        continue;
                    }
                }

                // Found the first batch that extends past new_end_offset.
                new_file_size = scan_pos;
                break;
            }
        }
    }

    get_logger()->info("Segment({}) truncating file from {} to {} bytes",
                       static_cast<void*>(this),
                       file_size_.load(), new_file_size);

    // Step 2: ftruncate the file.
    if (::ftruncate(fd_, static_cast<off_t>(new_file_size)) < 0) {
        std::string msg = fmt::format("ftruncate failed: {} (errno={})",
                                      std::strerror(errno), errno);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<void>::failure(error_code::storage_unavailable, msg);
    }

    // Step 3: Re-map if using mmap.
    if (mapped_data_ && mapped_data_ != MAP_FAILED) {
        unmap_file();
        auto map_res = map_file();
        if (map_res.failed()) {
            get_logger()->warn("Segment({}) re-map after truncate failed: {}",
                               static_cast<void*>(this), map_res.error_message);
        }
    }

    // Step 4: Update file_size_.
    file_size_.store(new_file_size, std::memory_order_release);

    // Step 5: Prune sparse index.
    prune_sparse_index(new_end_offset);

    // Step 6: Update next_offset.
    next_offset_.store(new_end_offset, std::memory_order_release);

    // Step 7: If we truncated to base_offset, there's no data left.
    if (new_end_offset <= header_.base_offset) {
        get_logger()->info("Segment({}) truncated to base_offset — segment is now empty",
                           static_cast<void*>(this));
    }

    get_logger()->info("Segment({}) truncate_to complete: new file_size={}, next_offset={}",
                       static_cast<void*>(this), new_file_size, new_end_offset);

    return result<void>::success();
}

// ============================================================================
// Index operations
// ============================================================================

std::optional<byte_count_t> Segment::find_position(offset_t target_offset) const {
    std::lock_guard<std::mutex> lock(index_mutex_);

    if (sparse_index_.empty()) {
        // No index entries: if there's data, it all starts at the header.
        if (file_size_.load(std::memory_order_acquire) > kSegmentHeaderSize) {
            return kSegmentHeaderSize;
        }
        return std::nullopt;
    }

    // Binary search for the largest entry with offset <= target_offset.
    auto it = std::upper_bound(sparse_index_.begin(), sparse_index_.end(), target_offset,
                                [](offset_t off, const SparseIndexEntry& entry) {
                                    return off < entry.offset;
                                });

    if (it == sparse_index_.begin()) {
        // All entries have offset > target_offset.
        // Return the start of data if target_offset >= base_offset.
        if (target_offset >= header_.base_offset) {
            return kSegmentHeaderSize;
        }
        return std::nullopt;
    }

    --it;
    return it->file_position;
}

result<offset_t> Segment::find_offset_by_timestamp(timestamp_ms_t ts) const {
    std::lock_guard<std::mutex> lock(index_mutex_);

    if (sparse_index_.empty()) {
        return result<offset_t>::failure(
            error_code::offset_out_of_range,
            "No index entries available for timestamp search");
    }

    // Binary search for first entry with timestamp >= ts.
    auto it = std::lower_bound(sparse_index_.begin(), sparse_index_.end(), ts,
                                [](const SparseIndexEntry& entry, timestamp_ms_t t) {
                                    return entry.timestamp < t;
                                });

    if (it == sparse_index_.end()) {
        // All timestamps are < ts; return the last offset.
        return result<offset_t>::success(sparse_index_.back().offset);
    }

    return result<offset_t>::success(it->offset);
}

result<void> Segment::rebuild_index() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::lock_guard<std::mutex> idx_lock(index_mutex_);

    if (fd_ < 0 && !file_stream_.is_open() && !mapped_data_) {
        return result<void>::failure(error_code::storage_unavailable,
                                     "No data source available for index rebuild");
    }

    get_logger()->info("Segment({}) rebuilding sparse index...", static_cast<void*>(this));

    sparse_index_.clear();
    sparse_index_.reserve(128);

    byte_count_t file_sz = file_size_.load(std::memory_order_acquire);
    if (file_sz <= kSegmentHeaderSize) {
        get_logger()->info("Segment({}) empty segment — index rebuild complete (0 entries)",
                           static_cast<void*>(this));
        index_dirty_ = false;
        return result<void>::success();
    }

    const char* mapped = static_cast<const char*>(mapped_data_);
    std::vector<uint8_t> buf;
    byte_count_t scan_pos = kSegmentHeaderSize;
    byte_count_t last_index_pos = 0;
    offset_t last_offset = header_.base_offset;
    timestamp_ms_t max_ts = 0;

    while (scan_pos < file_sz) {
        // Ensure we can read a batch header (at least 12 bytes).
        if (scan_pos + 12 > file_sz) break;

        // Read batch header.
        uint8_t bh[16]; // read more for record_count field
        size_t to_read = std::min(sizeof(bh), static_cast<size_t>(file_sz - scan_pos));
        if (mapped) {
            std::memcpy(bh, mapped + scan_pos, to_read);
        } else {
            ssize_t n = safe_pread(fd_, bh, to_read, static_cast<off_t>(scan_pos));
            if (n != static_cast<ssize_t>(to_read)) {
                get_logger()->warn("Segment({}) read error during index rebuild at pos {}",
                                   static_cast<void*>(this), scan_pos);
                break;
            }
        }

        offset_t bo = read_le64s(bh);
        int32_t  blen = read_le32s(bh + 8);

        if (blen <= 0 || blen > kMaxBatchBytes) {
            get_logger()->warn("Segment({}) invalid batch length {} at pos {} during rebuild",
                               static_cast<void*>(this), blen, scan_pos);
            break;
        }

        byte_count_t batch_total = 12 + static_cast<byte_count_t>(blen);
        if (scan_pos + batch_total > file_sz) {
            get_logger()->warn("Segment({}) truncated batch at pos {} during rebuild",
                               static_cast<void*>(this), scan_pos);
            break;
        }

        // Read record_count and timestamps at fixed offsets within the batch.
        // Batch layout: base_offset(8) + length(4) + leader_epoch(4) + magic(1) +
        //                crc(4) + attributes(2) + last_offset_delta(4) +
        //                base_timestamp(8) + max_timestamp(8) = 43 bytes into batch_data.
        // We need to read the batch data to get these fields.
        buf.resize(static_cast<size_t>(batch_total));
        if (mapped) {
            std::memcpy(buf.data(), mapped + scan_pos, static_cast<size_t>(batch_total));
        } else {
            ssize_t n = safe_pread(fd_, buf.data(), static_cast<size_t>(batch_total),
                                   static_cast<off_t>(scan_pos));
            if (n != static_cast<ssize_t>(batch_total)) {
                get_logger()->warn("Segment({}) failed to read full batch at pos {} during rebuild",
                                   static_cast<void*>(this), scan_pos);
                break;
            }
        }

        // Extract timestamps (offset 8+4+4+1+4+2+4=27 from batch start).
        size_t ts_off = 8 + 4 + 4 + 1 + 4 + 2 + 4; // 27 bytes
        timestamp_ms_t base_ts = read_le64s(buf.data() + ts_off);
        timestamp_ms_t batch_max_ts = read_le64s(buf.data() + ts_off + 8);

        // Extract record_count (offset 27+8+8+8+2+4=57).
        int32_t rec_count = read_le32s(buf.data() + 57);
        if (rec_count <= 0) rec_count = 1;

        timestamp_ms_t effective_ts = batch_max_ts > 0 ? batch_max_ts : base_ts;
        if (effective_ts > max_ts) max_ts = effective_ts;

        // Create an index entry at the configured interval.
        if (scan_pos - last_index_pos >= static_cast<byte_count_t>(config_.index_interval_bytes)
            || sparse_index_.empty()) {
            sparse_index_.push_back({bo, scan_pos, effective_ts});
            last_index_pos = scan_pos;
            last_offset = bo + rec_count;
        }

        scan_pos += batch_total;
    }

    // Always add a final entry for the last batch.
    if (!sparse_index_.empty() && last_offset > sparse_index_.back().offset) {
        byte_count_t final_pos = last_index_pos; // point to the last indexed batch
        // Actually, let's ensure we have good coverage.
        // For the last batch, make sure we have an entry.
        if (sparse_index_.back().offset < last_offset) {
            // Already have entries; the last one covers the final batch.
        }
    }

    // Update max_timestamp.
    if (max_ts > 0) {
        max_timestamp_.store(max_ts, std::memory_order_release);
    }

    // Limit index size.
    while (sparse_index_.size() > kMaxInMemoryIndexEntries) {
        // Remove every other entry (down-sample).
        std::vector<SparseIndexEntry> downsampled;
        downsampled.reserve(sparse_index_.size() / 2 + 1);
        for (size_t i = 0; i < sparse_index_.size(); i += 2) {
            downsampled.push_back(sparse_index_[i]);
        }
        sparse_index_ = std::move(downsampled);
        get_logger()->warn("Segment({}) index downsampled to {} entries",
                           static_cast<void*>(this), sparse_index_.size());
    }

    index_dirty_ = true;

    get_logger()->info("Segment({}) index rebuild complete: {} entries, max_ts={}",
                       static_cast<void*>(this), sparse_index_.size(), max_ts);

    // Try to flush the rebuilt index.
    auto flush_res = flush_index();
    if (flush_res.failed()) {
        get_logger()->warn("Segment({}) flushing rebuilt index failed: {}",
                           static_cast<void*>(this), flush_res.error_message);
    }

    return result<void>::success();
}

result<void> Segment::flush_index() {
    std::lock_guard<std::mutex> lock(index_mutex_);

    std::string idx_path = index_file_path();

    get_logger()->debug("Segment({}) flushing {} index entries to '{}'",
                        static_cast<void*>(this), sparse_index_.size(), idx_path);

    if (sparse_index_.empty() && !index_dirty_) {
        get_logger()->debug("Segment({}) no index entries and not dirty; skipping flush",
                            static_cast<void*>(this));
        return result<void>::success();
    }

    // Open the index file for writing (truncate).
    int idx_fd = ::open(idx_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (idx_fd < 0) {
        std::string msg = fmt::format("Cannot open index file '{}': {} (errno={})",
                                      idx_path, std::strerror(errno), errno);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<void>::failure(error_code::storage_unavailable, msg);
    }

    // Format: little-endian sequence of {offset(8), file_position(8), timestamp(8)}.
    // Each entry is 24 bytes.
    static constexpr size_t kIndexEntrySize = 24;
    size_t total_bytes = sparse_index_.size() * kIndexEntrySize;

    // Write header: magic "TQIX" + version + entry_count.
    uint8_t header[16] = {};
    header[0] = 'T';
    header[1] = 'Q';
    header[2] = 'I';
    header[3] = 'X'; // Index magic
    write_le16(header + 4, 1); // version
    write_le32(header + 6, static_cast<uint32_t>(sparse_index_.size()));
    // header[10..15] reserved

    ssize_t written = safe_write(idx_fd, header, sizeof(header));
    if (written != static_cast<ssize_t>(sizeof(header))) {
        std::string msg = fmt::format("Failed to write index header: {} (errno={})",
                                      std::strerror(errno), errno);
        ::close(idx_fd);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<void>::failure(error_code::storage_unavailable, msg);
    }

    // Write entries in batches to avoid large stack allocations.
    std::vector<uint8_t> entry_buf;
    entry_buf.reserve(4096);

    for (size_t i = 0; i < sparse_index_.size(); ++i) {
        const auto& entry = sparse_index_[i];
        size_t base = entry_buf.size();
        entry_buf.resize(base + kIndexEntrySize);
        write_le64s(entry_buf.data() + base, entry.offset);
        write_le64(entry_buf.data() + base + 8, static_cast<uint64_t>(entry.file_position));
        write_le64s(entry_buf.data() + base + 16, entry.timestamp);

        // Flush periodically.
        if (entry_buf.size() >= 4096 || i == sparse_index_.size() - 1) {
            ssize_t w = safe_write(idx_fd, entry_buf.data(), entry_buf.size());
            if (w != static_cast<ssize_t>(entry_buf.size())) {
                std::string msg = fmt::format("Failed to write index entries: {} (errno={})",
                                              std::strerror(errno), errno);
                ::close(idx_fd);
                get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
                return result<void>::failure(error_code::storage_unavailable, msg);
            }
            entry_buf.clear();
        }
    }

    // fsync the index file.
    if (::fsync(idx_fd) < 0) {
        std::string msg = fmt::format("fsync index file failed: {} (errno={})",
                                      std::strerror(errno), errno);
        ::close(idx_fd);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<void>::failure(error_code::storage_unavailable, msg);
    }

    ::close(idx_fd);
    index_dirty_ = false;

    get_logger()->debug("Segment({}) flushed {} index entries to '{}' ({} bytes)",
                        static_cast<void*>(this), sparse_index_.size(), idx_path, total_bytes);

    return result<void>::success();
}

// ============================================================================
// Rollover / Seal
// ============================================================================

result<void> Segment::seal() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (is_sealed()) {
        get_logger()->info("Segment({}) already sealed; no-op", static_cast<void*>(this));
        return result<void>::success();
    }

    get_logger()->info("Segment({}) sealing...", static_cast<void*>(this));

    // Step 1: Flush all pending writes.
    auto flush_res = fsync();
    if (flush_res.failed()) {
        get_logger()->error("Segment({}) fsync before seal failed: {}",
                            static_cast<void*>(this), flush_res.error_message);
        return flush_res;
    }

    // Step 2: Set the sealed flag in the in-memory header.
    header_.seal();

    // Step 3: Write the updated header to disk.
    auto write_res = write_header();
    if (write_res.failed()) {
        get_logger()->error("Segment({}) write_header during seal failed: {}",
                            static_cast<void*>(this), write_res.error_message);
        header_.flags &= ~0x01; // roll back sealed flag
        return write_res;
    }

    // Step 4: Sync the header write.
    if (::fsync(fd_) < 0) {
        std::string msg = fmt::format("fsync after seal header write failed: {} (errno={})",
                                      std::strerror(errno), errno);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<void>::failure(error_code::storage_unavailable, msg);
    }

    // Step 5: Flush the index.
    auto idx_res = flush_index();
    if (idx_res.failed()) {
        get_logger()->warn("Segment({}) flush_index during seal failed: {}",
                           static_cast<void*>(this), idx_res.error_message);
        // Non-fatal; index is recoverable.
    }

    // Step 6: Transition state.
    state_ = SegmentState::sealed;

    get_logger()->info("Segment({}) sealed successfully", static_cast<void*>(this));
    return result<void>::success();
}

bool Segment::should_roll() const noexcept {
    // We need to read state, file_size, and created_at safely.
    // Since these are atomics or immutable post-construction, we can
    // read them without locking.
    auto current_state = state_.load(std::memory_order_acquire);
    if (current_state != SegmentState::active) {
        return false; // can't roll a non-active segment
    }

    if (is_sealed()) {
        return false; // already sealed
    }

    byte_count_t cur_size = file_size_.load(std::memory_order_acquire);
    if (cur_size >= config_.max_segment_bytes && config_.max_segment_bytes > 0) {
        return true;
    }

    if (config_.max_segment_age_ms > 0) {
        timestamp_ms_t age = now_ms() - header_.created_at;
        if (age >= config_.max_segment_age_ms) {
            return true;
        }
    }

    return false;
}

result<SegmentConfig> Segment::roll() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!is_active()) {
        return result<SegmentConfig>::failure(
            error_code::not_leader_for_partition,
            "Cannot roll a non-active segment");
    }

    if (is_sealed()) {
        return result<SegmentConfig>::failure(
            error_code::not_leader_for_partition,
            "Segment is already sealed");
    }

    get_logger()->info("Segment({}) rolling: creating successor config", static_cast<void*>(this));

    // Step 1: Seal this segment.
    auto seal_res = seal();
    if (seal_res.failed()) {
        return result<SegmentConfig>::failure(seal_res.error,
                                               "Roll failed during seal: " + seal_res.error_message);
    }

    // Step 2: Create a config for the successor segment.
    offset_t cur_next = next_offset_.load(std::memory_order_acquire);

    SegmentConfig next_config;
    // Construct successor file path: replace the last numeric suffix or append ".1".
    // Simple strategy: append ".{next_id}" to base path.
    std::string base_path = config_.file_path;
    // Remove any existing numeric suffix in ".NNNNNNNNNN" format.
    auto dot_pos = base_path.rfind('.');
    if (dot_pos != std::string::npos) {
        std::string after_dot = base_path.substr(dot_pos + 1);
        bool all_digits = !after_dot.empty() &&
                          std::all_of(after_dot.begin(), after_dot.end(), ::isdigit);
        if (all_digits) {
            base_path = base_path.substr(0, dot_pos);
        }
    }
    next_config.file_path = fmt::format("{}.{:010d}", base_path, header_.segment_id + 1);

    // Compute index file path for successor.
    if (!config_.index_file_path.empty()) {
        std::string idx_base = config_.index_file_path;
        auto idx_dot = idx_base.rfind('.');
        if (idx_dot != std::string::npos) {
            std::string idx_after = idx_base.substr(idx_dot + 1);
            bool idx_digits = !idx_after.empty() &&
                              std::all_of(idx_after.begin(), idx_after.end(), ::isdigit);
            if (idx_digits) {
                idx_base = idx_base.substr(0, idx_dot);
            }
        }
        next_config.index_file_path = fmt::format("{}.{:010d}.index", idx_base, header_.segment_id + 1);
    }

    next_config.base_offset         = cur_next;
    next_config.segment_id          = header_.segment_id + 1;
    next_config.compression         = config_.compression;
    next_config.max_segment_bytes   = config_.max_segment_bytes;
    next_config.max_segment_age_ms  = config_.max_segment_age_ms;
    next_config.index_interval_bytes = config_.index_interval_bytes;
    next_config.preallocate         = config_.preallocate;
    next_config.use_mmap            = config_.use_mmap;
    next_config.read_only           = false;
    next_config.sync_on_append      = config_.sync_on_append;

    get_logger()->info("Segment({}) roll complete: successor will be '{}' "
                       "(base_offset={}, segment_id={})",
                       static_cast<void*>(this),
                       next_config.file_path,
                       next_config.base_offset,
                       next_config.segment_id);

    return result<SegmentConfig>::success(std::move(next_config));
}

// ============================================================================
// Compaction
// ============================================================================

result<std::vector<Record>> Segment::compact_records(
    std::function<bool(const Record& existing, const Record& newer)> keep_existing) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!keep_existing) {
        return result<std::vector<Record>>::failure(
            error_code::invalid_config,
            "combiner function is null");
    }

    get_logger()->info("Segment({}) compact_records: scanning all records", static_cast<void*>(this));

    // Step 1: Read all records from this segment.
    offset_t cur_next = next_offset_.load(std::memory_order_acquire);
    auto read_res = read_range(header_.base_offset, cur_next);
    if (read_res.failed()) {
        return result<std::vector<Record>>::failure(read_res.error,
                                                     "Failed to read records for compaction: " +
                                                     read_res.error_message);
    }

    // Step 2: Group records by key and apply the combiner.
    //         We use the serialized key bytes as the map key for comparison.
    //         We'll use a hash of the key data for the map, then a predicate for equality.
    struct KeyHash {
        size_t operator()(const std::string& k) const {
            return std::hash<std::string>{}(k);
        }
    };

    std::unordered_map<std::string, Record, KeyHash> compacted;
    size_t total_input = 0;

    for (auto& batch : read_res.value.batches) {
        for (auto& rec : batch.records) {
            total_input++;
            // Build a key string from the shared_buffer.
            std::string key_str(rec.key.data(), rec.key.size());

            auto it = compacted.find(key_str);
            if (it == compacted.end()) {
                // First occurrence of this key.
                compacted.emplace(std::move(key_str), std::move(rec));
            } else {
                // Key already exists; ask the combiner which to keep.
                if (keep_existing(it->second, rec)) {
                    // Keep existing — new record is discarded.
                } else {
                    // Replace existing with new.
                    it->second = std::move(rec);
                }
            }
        }
    }

    // Step 3: Collect the compacted records.
    std::vector<Record> result;
    result.reserve(compacted.size());
    for (auto& kv : compacted) {
        result.push_back(std::move(kv.second));
    }

    // Sort by offset to maintain approximate order.
    std::sort(result.begin(), result.end(),
              [](const Record& a, const Record& b) {
                  return a.offset < b.offset;
              });

    get_logger()->info("Segment({}) compact_records complete: {} input records -> {} compacted",
                       static_cast<void*>(this), total_input, result.size());

    return result<std::vector<Record>>::success(std::move(result));
}

// ============================================================================
// Accessor: info()
// ============================================================================

SegmentInfo Segment::info() const noexcept {
    SegmentInfo si;
    si.segment_id     = header_.segment_id;
    si.base_offset    = header_.base_offset;
    si.next_offset    = next_offset_.load(std::memory_order_acquire);
    si.file_path      = config_.file_path;
    si.file_size      = file_size_.load(std::memory_order_acquire);

    // Compute approximate index size.
    {
        std::lock_guard<std::mutex> lock(index_mutex_);
        si.index_size    = static_cast<byte_count_t>(sparse_index_.size() * 24 + 16);
        si.max_timestamp = max_timestamp_.load(std::memory_order_acquire);
    }

    si.time_index_size = 0; // not yet implemented
    si.created_at      = header_.created_at;
    si.last_modified   = now_ms();
    si.is_active       = is_active();
    si.is_sealed       = is_sealed();

    return si;
}

// ============================================================================
// Private helpers — header I/O
// ============================================================================

result<void> Segment::read_header() {
    if (fd_ < 0) {
        return result<void>::failure(error_code::storage_unavailable, "No file descriptor");
    }

    uint8_t raw_header[kSegmentHeaderSize];
    ssize_t n = safe_pread(fd_, raw_header, kSegmentHeaderSize, 0);
    if (n < 0) {
        std::string msg = fmt::format("Failed to read header: {} (errno={})",
                                      std::strerror(errno), errno);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<void>::failure(error_code::storage_unavailable, msg);
    }
    if (static_cast<size_t>(n) < kSegmentHeaderSize) {
        return result<void>::failure(
            error_code::corrupt_message,
            fmt::format("Header too short: got {} bytes, expected {}",
                        n, kSegmentHeaderSize));
    }

    // Parse the header.
    header_.magic          = read_le32(raw_header);
    header_.format_version = read_le16(raw_header + 4);
    header_.header_size    = read_le16(raw_header + 6);
    header_.header_crc     = read_le32(raw_header + 8);
    // bytes 12..15: reserved
    header_.base_offset    = read_le64s(raw_header + 16);
    header_.segment_id     = read_le64(raw_header + 24);
    header_.created_at     = read_le64s(raw_header + 32);
    header_.compression    = static_cast<compression_type>(raw_header[40]);
    header_.flags          = raw_header[41];
    // bytes 42..63: reserved

    get_logger()->debug("Segment({}) read header: magic={:08X}, version={}, "
                        "base_offset={}, segment_id={}, compression={}, sealed={}",
                        static_cast<void*>(this),
                        header_.magic,
                        header_.format_version,
                        header_.base_offset,
                        header_.segment_id,
                        static_cast<int>(header_.compression),
                        header_.is_sealed());

    // Validate magic, version, and header size.
    if (!header_.is_valid()) {
        std::string msg = fmt::format(
            "Invalid header: magic={:08X} (expected {:08X}), version={} (expected {}), "
            "header_size={} (expected {}), base_offset={}",
            header_.magic, kSegmentMagic,
            header_.format_version, kSegmentFormatVersion,
            header_.header_size, kSegmentHeaderSize,
            header_.base_offset);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<void>::failure(error_code::corrupt_message, msg);
    }

    // Validate CRC.
    if (!validate_header_crc()) {
        uint32_t expected = compute_header_crc();
        std::string msg = fmt::format(
            "Header CRC mismatch: stored={:08X}, computed={:08X}",
            header_.header_crc, expected);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<void>::failure(error_code::corrupt_message, msg);
    }

    return result<void>::success();
}

result<void> Segment::write_header() {
    if (fd_ < 0) {
        return result<void>::failure(error_code::storage_unavailable, "No file descriptor");
    }

    get_logger()->debug("Segment({}) writing header: base_offset={}, segment_id={}, sealed={}",
                        static_cast<void*>(this),
                        header_.base_offset,
                        header_.segment_id,
                        header_.is_sealed());

    // Compute CRC before writing.
    header_.header_crc = compute_header_crc();

    uint8_t raw_header[kSegmentHeaderSize] = {};
    write_le32(raw_header, header_.magic);
    write_le16(raw_header + 4, header_.format_version);
    write_le16(raw_header + 6, header_.header_size);
    write_le32(raw_header + 8, header_.header_crc);
    // bytes 12..15: reserved (already zero)
    write_le64s(raw_header + 16, header_.base_offset);
    write_le64(raw_header + 24, header_.segment_id);
    write_le64s(raw_header + 32, header_.created_at);
    raw_header[40] = static_cast<uint8_t>(header_.compression);
    raw_header[41] = header_.flags;
    // bytes 42..63: reserved (already zero)

    ssize_t n = safe_pwrite(fd_, raw_header, kSegmentHeaderSize, 0);
    if (n < 0) {
        std::string msg = fmt::format("Failed to write header: {} (errno={})",
                                      std::strerror(errno), errno);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<void>::failure(error_code::storage_unavailable, msg);
    }
    if (static_cast<size_t>(n) < kSegmentHeaderSize) {
        std::string msg = fmt::format("Short header write: {} of {} bytes",
                                      n, kSegmentHeaderSize);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<void>::failure(error_code::storage_unavailable, msg);
    }

    get_logger()->debug("Segment({}) header written successfully (crc={:08X})",
                        static_cast<void*>(this), header_.header_crc);
    return result<void>::success();
}

uint32_t Segment::compute_header_crc() const {
    // CRC covers bytes [16..64) of the header: base_offset through reserved padding.
    // We need to serialize just that portion into a temporary buffer.
    uint8_t buf[48]; // bytes 16..63 (48 bytes)
    write_le64s(buf, header_.base_offset);
    write_le64(buf + 8, header_.segment_id);
    write_le64s(buf + 16, header_.created_at);
    buf[24] = static_cast<uint8_t>(header_.compression);
    buf[25] = header_.flags;
    std::memset(buf + 26, 0, 22); // reserved padding

    return crc32c(buf, sizeof(buf));
}

bool Segment::validate_header_crc() const {
    uint32_t computed = compute_header_crc();
    return computed == header_.header_crc;
}

// ============================================================================
// Private helpers — file preparation
// ============================================================================

result<void> Segment::prepare_file() {
    bool file_exists = (::access(config_.file_path.c_str(), F_OK) == 0);

    if (file_exists) {
        get_logger()->info("Segment({}) opening existing file: '{}'",
                           static_cast<void*>(this), config_.file_path);

        // Open for read/write.
        int flags = O_RDWR;
        if (!config_.read_only) {
            // Use O_DSYNC for data integrity on critical writes.
            // Actually, O_DSYNC would impact performance; we handle sync manually.
        }

        fd_ = ::open(config_.file_path.c_str(), flags);
        if (fd_ < 0) {
            std::string msg = fmt::format("Cannot open existing file '{}': {} (errno={})",
                                          config_.file_path, std::strerror(errno), errno);
            get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
            return result<void>::failure(error_code::storage_unavailable, msg);
        }

        // Read and validate the header.
        auto hdr_res = read_header();
        if (hdr_res.failed()) {
            ::close(fd_);
            fd_ = -1;
            return hdr_res;
        }

        // Verify that the segment_id and base_offset match config if non-zero.
        if (config_.segment_id != 0 && header_.segment_id != config_.segment_id) {
            std::string msg = fmt::format(
                "Segment ID mismatch: config expects {}, file has {}",
                config_.segment_id, header_.segment_id);
            get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
            ::close(fd_);
            fd_ = -1;
            return result<void>::failure(error_code::corrupt_message, msg);
        }

        if (config_.base_offset != kInvalidOffset &&
            config_.base_offset != 0 &&
            header_.base_offset != config_.base_offset) {
            std::string msg = fmt::format(
                "Base offset mismatch: config expects {}, file has {}",
                config_.base_offset, header_.base_offset);
            get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
            ::close(fd_);
            fd_ = -1;
            return result<void>::failure(error_code::corrupt_message, msg);
        }

        // If config passed base_offset=0 (default) and file has a real offset,
        // adopt the file's value.
        if (config_.base_offset == 0 && header_.base_offset > 0) {
            config_.base_offset = header_.base_offset;
            get_logger()->info("Segment({}) adopting file's base_offset={}",
                               static_cast<void*>(this), header_.base_offset);
        }

        get_logger()->info("Segment({}) existing file opened: fd={}, base_offset={}, "
                           "sealed={}",
                           static_cast<void*>(this), fd_,
                           header_.base_offset, header_.is_sealed());

    } else {
        get_logger()->info("Segment({}) creating new file: '{}'",
                           static_cast<void*>(this), config_.file_path);

        // Create parent directories if needed.
        auto last_slash = config_.file_path.rfind('/');
        if (last_slash != std::string::npos) {
            std::string dir = config_.file_path.substr(0, last_slash);
            // Recursive mkdir with default permissions.
            std::string cmd = "mkdir -p '" + dir + "'";
            if (::system(cmd.c_str()) != 0) {
                get_logger()->warn("Segment({}) mkdir -p '{}' returned non-zero",
                                   static_cast<void*>(this), dir);
            }
        }

        int flags = O_RDWR | O_CREAT | O_EXCL;
        fd_ = ::open(config_.file_path.c_str(), flags, 0644);
        if (fd_ < 0) {
            std::string msg = fmt::format("Cannot create file '{}': {} (errno={})",
                                          config_.file_path, std::strerror(errno), errno);
            get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
            return result<void>::failure(error_code::storage_unavailable, msg);
        }

        // Set up header with current time and config values.
        header_.magic          = kSegmentMagic;
        header_.format_version = kSegmentFormatVersion;
        header_.header_size    = kSegmentHeaderSize;
        header_.base_offset    = config_.base_offset;
        header_.segment_id     = config_.segment_id;
        header_.created_at     = now_ms();
        header_.compression    = config_.compression;
        header_.flags          = 0;
        header_.header_crc     = 0;

        // Write the initial header.
        auto write_res = write_header();
        if (write_res.failed()) {
            ::close(fd_);
            fd_ = -1;
            return write_res;
        }

        // Preallocate the file if requested.
        if (config_.preallocate) {
            get_logger()->info("Segment({}) preallocating {} bytes", static_cast<void*>(this),
                               config_.max_segment_bytes);
            if (::ftruncate(fd_, static_cast<off_t>(config_.max_segment_bytes)) < 0) {
                std::string msg = fmt::format("Preallocation (ftruncate) failed: {} (errno={})",
                                              std::strerror(errno), errno);
                get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
                ::close(fd_);
                fd_ = -1;
                return result<void>::failure(error_code::storage_unavailable, msg);
            }
            // Seek back to 0 so appends go to the correct position.
            // Actually, with pwrite, position doesn't matter.
        }

        // Sync the header.
        if (::fsync(fd_) < 0) {
            std::string msg = fmt::format("fsync after file creation failed: {} (errno={})",
                                          std::strerror(errno), errno);
            get_logger()->warn("Segment({}) {}", static_cast<void*>(this), msg);
        }

        get_logger()->info("Segment({}) new file created: fd={}, base_offset={}, size={}",
                           static_cast<void*>(this), fd_,
                           header_.base_offset,
                           config_.preallocate ? config_.max_segment_bytes : kSegmentHeaderSize);
    }

    return result<void>::success();
}

// ============================================================================
// Private helpers — memory mapping
// ============================================================================

result<void> Segment::map_file() {
    if (fd_ < 0) {
        return result<void>::failure(error_code::storage_unavailable, "No file descriptor");
    }

    byte_count_t sz = file_size_.load(std::memory_order_acquire);
    if (sz <= 0) {
        get_logger()->debug("Segment({}) map_file: file size is 0, nothing to map",
                            static_cast<void*>(this));
        return result<void>::success();
    }

    get_logger()->debug("Segment({}) mmap: file_size={}", static_cast<void*>(this), sz);

    // Ensure we don't try to map more than a reasonable size.
    // On 32-bit systems, we might want to limit this.
    size_t map_size = static_cast<size_t>(sz);

    void* mapped = ::mmap(nullptr, map_size, PROT_READ | PROT_WRITE,
                          MAP_SHARED, fd_, 0);
    if (mapped == MAP_FAILED) {
        std::string msg = fmt::format("mmap failed for '{}' (size={}): {} (errno={})",
                                      config_.file_path, map_size,
                                      std::strerror(errno), errno);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        mapped_data_ = nullptr;
        mapped_size_ = 0;
        return result<void>::failure(error_code::storage_unavailable, msg);
    }

    mapped_data_ = mapped;
    mapped_size_ = map_size;

    // Advise the kernel that we'll be doing random reads.
    ::madvise(mapped, map_size, MADV_RANDOM);

    get_logger()->info("Segment({}) mmap succeeded: {} bytes at {}",
                       static_cast<void*>(this), map_size, mapped);

    return result<void>::success();
}

void Segment::unmap_file() {
    if (mapped_data_ && mapped_data_ != MAP_FAILED) {
        get_logger()->debug("Segment({}) munmap: {} bytes at {}",
                            static_cast<void*>(this), mapped_size_, mapped_data_);

        // Sync before unmapping.
        ::msync(mapped_data_, mapped_size_, MS_SYNC);

        if (::munmap(mapped_data_, mapped_size_) < 0) {
            get_logger()->error("Segment({}) munmap failed: {} (errno={})",
                                static_cast<void*>(this),
                                std::strerror(errno), errno);
        }
    }
    mapped_data_ = nullptr;
    mapped_size_ = 0;
}

result<void> Segment::extend_file(byte_count_t additional_bytes) {
    if (fd_ < 0) {
        return result<void>::failure(error_code::storage_unavailable, "No file descriptor");
    }

    byte_count_t cur_size = file_size_.load(std::memory_order_acquire);
    byte_count_t new_size = cur_size + additional_bytes;

    // Round up to kFileExtendStep for fewer truncate calls.
    new_size = ((new_size + kFileExtendStep - 1) / kFileExtendStep) * kFileExtendStep;

    get_logger()->debug("Segment({}) extending file from {} to {} bytes",
                        static_cast<void*>(this), cur_size, new_size);

    if (::ftruncate(fd_, static_cast<off_t>(new_size)) < 0) {
        std::string msg = fmt::format("ftruncate extend failed: {} (errno={})",
                                      std::strerror(errno), errno);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<void>::failure(error_code::storage_unavailable, msg);
    }

    file_size_.store(new_size, std::memory_order_release);

    // Re-map if mmap is active.
    if (mapped_data_ && mapped_data_ != MAP_FAILED) {
        // mremap would be ideal but is Linux-specific; unmount/remap instead.
        size_t old_size = mapped_size_;
        unmap_file();
        auto map_res = map_file();
        if (map_res.failed()) {
            get_logger()->warn("Segment({}) re-map after extend failed: {}",
                               static_cast<void*>(this), map_res.error_message);
            // Non-fatal: reads can fall back to pread.
        }
    }

    return result<void>::success();
}

// ============================================================================
// Private helpers — byte-level I/O
// ============================================================================

result<byte_count_t> Segment::append_bytes(const void* data, size_t len) {
    if (fd_ < 0) {
        return result<byte_count_t>::failure(error_code::storage_unavailable, "No file descriptor");
    }

    byte_count_t cur_size = file_size_.load(std::memory_order_acquire);
    ssize_t written = safe_pwrite(fd_, data, len, static_cast<off_t>(cur_size));
    if (written < 0) {
        std::string msg = fmt::format("pwrite append failed: {} (errno={})",
                                      std::strerror(errno), errno);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<byte_count_t>::failure(error_code::storage_unavailable, msg);
    }

    byte_count_t new_size = cur_size + static_cast<byte_count_t>(written);
    file_size_.store(new_size, std::memory_order_release);

    return result<byte_count_t>::success(cur_size);
}

// ============================================================================
// Private helpers — serialization / deserialization
// ============================================================================

std::pair<std::unique_ptr<char[]>, size_t> Segment::serialize_batch(
    const RecordBatch& batch) const {
    // Compute the total size needed.
    // Batch header: base_offset(8) + length(4) + leader_epoch(4) + magic(1) +
    //               crc(4) + attributes(2) + last_offset_delta(4) +
    //               base_timestamp(8) + max_timestamp(8) + producer_id(8) +
    //               producer_epoch(2) + base_sequence(4) + record_count(4) = 61
    static constexpr size_t kBatchOverhead = 8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4;

    size_t records_size = 0;
    for (const auto& rec : batch.records) {
        // Record header: length(4) + attributes(1) + ts_delta(varint max 10) +
        //               offset_delta(varint max 10) + key_len(4) + key +
        //               value_len(4) + value + header_count(4) + headers
        size_t rec_size = 4 + 1 + kMaxVarint64Len + kMaxVarint64Len
                         + 4 + rec.key.size()
                         + 4 + rec.value.size()
                         + 4;
        for (const auto& h : rec.headers) {
            rec_size += 4 + h.key.size() + 4 + h.value.size();
        }
        records_size += rec_size;
    }

    size_t total_size = kBatchOverhead + records_size;
    auto buffer = std::make_unique<char[]>(total_size);
    auto* out = reinterpret_cast<uint8_t*>(buffer.get());
    size_t pos = 0;

    // base_offset (will be patched by caller)
    write_le64s(out + pos, batch.base_offset); pos += 8;
    // length placeholder (will be patched)
    size_t length_pos = pos;
    write_le32(out + pos, 0); pos += 4;
    // partition_leader_epoch
    write_le32s(out + pos, batch.partition_leader_epoch); pos += 4;
    // magic byte
    out[pos++] = static_cast<uint8_t>(kRecordBatchMagic);
    // crc placeholder
    size_t crc_pos = pos;
    write_le32(out + pos, 0); pos += 4;
    // attributes
    write_le16(out + pos, static_cast<uint16_t>(batch.attributes)); pos += 2;
    // last_offset_delta
    write_le32s(out + pos, batch.last_offset_delta); pos += 4;
    // base_timestamp
    write_le64s(out + pos, batch.base_timestamp); pos += 8;
    // max_timestamp
    write_le64s(out + pos, batch.max_timestamp); pos += 8;
    // producer_id
    write_le64s(out + pos, batch.producer_id); pos += 8;
    // producer_epoch
    write_le16(out + pos, static_cast<uint16_t>(batch.producer_epoch)); pos += 2;
    // base_sequence
    write_le32s(out + pos, batch.base_sequence); pos += 4;
    // record_count
    int32_t rcount = static_cast<int32_t>(batch.records.size());
    write_le32(out + pos, rcount); pos += 4;

    // Records
    for (int i = 0; i < rcount; ++i) {
        const auto& rec = batch.records[i];
        size_t rec_start = pos;

        // Record length placeholder
        size_t rec_len_pos = pos;
        write_le32(out + pos, 0); pos += 4;

        // Attributes
        out[pos++] = 0;

        // Timestamp delta (zigzag-encoded varint)
        int64_t ts_delta = rec.timestamp - batch.base_timestamp;
        pos += write_svarint64(ts_delta, out + pos);

        // Offset delta (zigzag-encoded varint)
        int32_t off_delta = (batch.last_offset_delta > 0)
                          ? (i == rcount - 1 ? batch.last_offset_delta : i)
                          : i;
        pos += write_svarint64(off_delta, out + pos);

        // Key
        write_le32s(out + pos, static_cast<int32_t>(rec.key.size())); pos += 4;
        if (!rec.key.empty()) {
            std::memcpy(out + pos, rec.key.data(), rec.key.size());
            pos += rec.key.size();
        }

        // Value
        write_le32s(out + pos, static_cast<int32_t>(rec.value.size())); pos += 4;
        if (!rec.value.empty()) {
            std::memcpy(out + pos, rec.value.data(), rec.value.size());
            pos += rec.value.size();
        }

        // Headers
        int32_t hcount = static_cast<int32_t>(rec.headers.size());
        write_le32s(out + pos, hcount); pos += 4;
        for (const auto& h : rec.headers) {
            write_le32s(out + pos, static_cast<int32_t>(h.key.size())); pos += 4;
            if (!h.key.empty()) {
                std::memcpy(out + pos, h.key.data(), h.key.size());
                pos += h.key.size();
            }
            write_le32s(out + pos, static_cast<int32_t>(h.value.size())); pos += 4;
            if (!h.value.empty()) {
                std::memcpy(out + pos, h.value.data(), h.value.size());
                pos += h.value.size();
            }
        }

        // Patch record length.
        write_le32(out + rec_len_pos, static_cast<uint32_t>(pos - rec_start - 4));
    }

    // Patch batch length (bytes after the length field itself).
    write_le32(out + length_pos, static_cast<uint32_t>(pos - length_pos - 4));

    // Compute and patch CRC (covers from attributes to end).
    size_t crc_data_start = crc_pos; // crc field is just before attributes
    size_t crc_data_len = pos - crc_data_start;
    // Actually, CRC covers the bytes after the CRC field: attributes onwards.
    size_t crc_cover_start = crc_pos + 4; // skip the CRC field itself
    size_t crc_cover_len = pos - crc_cover_start;
    uint32_t batch_crc = crc32c(out + crc_cover_start, crc_cover_len);
    write_le32(out + crc_pos, batch_crc);

    get_logger()->trace("Serialized batch: {} records, {} total bytes, crc={:08X}",
                        rcount, pos, batch_crc);

    return {std::move(buffer), pos};
}

result<RecordBatch> Segment::deserialize_batch(const char* data, size_t len,
                                                offset_t expected_base_offset) const {
    if (len < kMinBatchHeaderSize) {
        return result<RecordBatch>::failure(
            error_code::corrupt_message,
            fmt::format("Batch too small: {} bytes (min {})", len, kMinBatchHeaderSize));
    }

    auto* ptr = reinterpret_cast<const uint8_t*>(data);
    size_t pos = 0;

    // base_offset
    offset_t base_offset = read_le64s(ptr + pos); pos += 8;

    // Validate base_offset against expected.
    if (expected_base_offset != kInvalidOffset && base_offset != expected_base_offset) {
        get_logger()->warn("Batch base_offset mismatch: expected {}, got {}",
                           expected_base_offset, base_offset);
    }

    // batch_length
    int32_t batch_length = read_le32s(ptr + pos); pos += 4;

    if (static_cast<size_t>(batch_length) + 12 != len) {
        return result<RecordBatch>::failure(
            error_code::corrupt_message,
            fmt::format("Batch length mismatch: header says {}, actual data {} (total {})",
                        batch_length, len - 12, len));
    }

    // partition_leader_epoch
    int32_t partition_leader_epoch = read_le32s(ptr + pos); pos += 4;

    // magic byte
    int8_t magic = static_cast<int8_t>(ptr[pos]); pos += 1;
    if (magic != kRecordBatchMagic) {
        return result<RecordBatch>::failure(
            error_code::corrupt_message,
            fmt::format("Bad batch magic: {} (expected {})", magic, kRecordBatchMagic));
    }

    // crc
    uint32_t stored_crc = read_le32(ptr + pos); pos += 4;

    // Validate CRC.
    size_t crc_cover_start = pos; // from attributes onward
    size_t crc_cover_len = len - crc_cover_start;
    uint32_t computed_crc = crc32c(ptr + crc_cover_start, crc_cover_len);
    if (stored_crc != 0 && computed_crc != stored_crc) {
        return result<RecordBatch>::failure(
            error_code::corrupt_message,
            fmt::format("Batch CRC mismatch at offset {}: stored={:08X}, computed={:08X}",
                        base_offset, stored_crc, computed_crc));
    }

    // attributes
    int16_t attributes = static_cast<int16_t>(read_le16(ptr + pos)); pos += 2;

    // last_offset_delta
    int32_t last_offset_delta = read_le32s(ptr + pos); pos += 4;

    // base_timestamp
    timestamp_ms_t base_timestamp = read_le64s(ptr + pos); pos += 8;

    // max_timestamp
    timestamp_ms_t max_timestamp = read_le64s(ptr + pos); pos += 8;

    // producer_id
    int64_t producer_id = read_le64s(ptr + pos); pos += 8;

    // producer_epoch
    int16_t producer_epoch = static_cast<int16_t>(read_le16(ptr + pos)); pos += 2;

    // base_sequence
    int32_t base_sequence = read_le32s(ptr + pos); pos += 4;

    // record_count
    int32_t record_count = read_le32s(ptr + pos); pos += 4;

    if (record_count < 0 || record_count > 100000) {
        return result<RecordBatch>::failure(
            error_code::corrupt_message,
            fmt::format("Insane record_count: {}", record_count));
    }

    // Parse records.
    std::vector<Record> records;
    records.reserve(static_cast<size_t>(record_count));

    for (int32_t i = 0; i < record_count; ++i) {
        if (pos + 4 > len) {
            return result<RecordBatch>::failure(
                error_code::corrupt_message,
                fmt::format("Truncated record {} of {} at offset {}", i, record_count, base_offset));
        }

        // Record length.
        int32_t rec_len = read_le32s(ptr + pos); pos += 4;
        if (rec_len < 0 || static_cast<size_t>(pos + rec_len) > len) {
            return result<RecordBatch>::failure(
                error_code::corrupt_message,
                fmt::format("Invalid record length {} at record {} of {} (offset {})",
                            rec_len, i, record_count, base_offset));
        }

        // Record attributes.
        if (pos >= len) break;
        uint8_t rec_attr = ptr[pos++];

        // Timestamp delta (varint).
        auto [ts_delta, ts_bytes] = read_svarint64(ptr + pos, len - pos);
        if (ts_bytes == 0) {
            return result<RecordBatch>::failure(
                error_code::corrupt_message,
                fmt::format("Cannot decode timestamp_delta at record {}", i));
        }
        pos += ts_bytes;

        // Offset delta (varint).
        auto [off_delta, off_bytes] = read_svarint32(ptr + pos, len - pos);
        if (off_bytes == 0) {
            return result<RecordBatch>::failure(
                error_code::corrupt_message,
                fmt::format("Cannot decode offset_delta at record {}", i));
        }
        pos += off_bytes;

        // Key.
        if (pos + 4 > len) break;
        int32_t key_len = read_le32s(ptr + pos); pos += 4;
        if (key_len < -1 || static_cast<size_t>(pos + key_len) > len) {
            return result<RecordBatch>::failure(
                error_code::corrupt_message,
                fmt::format("Invalid key length {} at record {}", key_len, i));
        }
        shared_buffer key;
        if (key_len > 0) {
            key = shared_buffer(reinterpret_cast<const char*>(ptr + pos),
                                static_cast<size_t>(key_len));
            pos += static_cast<size_t>(key_len);
        }

        // Value.
        if (pos + 4 > len) break;
        int32_t value_len = read_le32s(ptr + pos); pos += 4;
        if (value_len < -1 || static_cast<size_t>(pos + value_len) > len) {
            return result<RecordBatch>::failure(
                error_code::corrupt_message,
                fmt::format("Invalid value length {} at record {}", value_len, i));
        }
        shared_buffer value;
        if (value_len > 0) {
            value = shared_buffer(reinterpret_cast<const char*>(ptr + pos),
                                  static_cast<size_t>(value_len));
            pos += static_cast<size_t>(value_len);
        }

        // Headers.
        if (pos + 4 > len) break;
        int32_t header_count = read_le32s(ptr + pos); pos += 4;
        if (header_count < 0 || header_count > 1000) {
            return result<RecordBatch>::failure(
                error_code::corrupt_message,
                fmt::format("Invalid header count {} at record {}", header_count, i));
        }
        std::vector<RecordHeader> headers;
        headers.reserve(static_cast<size_t>(header_count));
        for (int32_t h = 0; h < header_count; ++h) {
            if (pos + 4 > len) break;
            int32_t hkey_len = read_le32s(ptr + pos); pos += 4;
            if (hkey_len < 0 || static_cast<size_t>(pos + hkey_len) > len) {
                return result<RecordBatch>::failure(
                    error_code::corrupt_message,
                    fmt::format("Invalid header key len at record {}, header {}", i, h));
            }
            std::string hkey(reinterpret_cast<const char*>(ptr + pos),
                             static_cast<size_t>(hkey_len));
            pos += static_cast<size_t>(hkey_len);

            if (pos + 4 > len) break;
            int32_t hval_len = read_le32s(ptr + pos); pos += 4;
            if (hval_len < 0 || static_cast<size_t>(pos + hval_len) > len) {
                return result<RecordBatch>::failure(
                    error_code::corrupt_message,
                    fmt::format("Invalid header value len at record {}, header {}", i, h));
            }
            std::string hvalue(reinterpret_cast<const char*>(ptr + pos),
                               static_cast<size_t>(hval_len));
            pos += static_cast<size_t>(hval_len);

            headers.push_back({std::move(hkey), std::move(hvalue)});
        }

        Record rec;
        rec.key          = std::move(key);
        rec.value        = std::move(value);
        rec.headers      = std::move(headers);
        rec.timestamp    = base_timestamp + ts_delta;
        rec.offset       = base_offset + off_delta;
        rec.partition_id = 0; // not stored in batch; set by caller
        rec.sequence     = base_sequence >= 0 ? base_sequence + i : -1;
        rec.producer_id  = producer_id;

        records.push_back(std::move(rec));
    }

    RecordBatch result;
    result.base_offset           = base_offset;
    result.base_timestamp        = base_timestamp;
    result.max_timestamp         = max_timestamp;
    result.last_offset_delta     = last_offset_delta;
    result.compression           = compression_type::none; // decompressed by caller if needed
    result.attributes            = attributes;
    result.crc                   = stored_crc;
    result.producer_id           = producer_id;
    result.producer_epoch        = producer_epoch;
    result.base_sequence         = base_sequence;
    result.record_count          = static_cast<int32_t>(records.size());
    result.partition_leader_epoch = partition_leader_epoch;
    result.records               = std::move(records);

    get_logger()->trace("Deserialized batch: base_offset={}, {} records, crc={:08X}",
                        base_offset, result.records.size(), stored_crc);

    return result<RecordBatch>::success(std::move(result));
}

// ============================================================================
// Private helpers — sparse index management
// ============================================================================

void Segment::update_sparse_index(offset_t base_offset, byte_count_t file_position,
                                   timestamp_ms_t timestamp, int32_t record_count) {
    std::lock_guard<std::mutex> lock(index_mutex_);

    // Determine if we should create a new index entry based on interval.
    bool should_add = sparse_index_.empty();

    if (!should_add) {
        const auto& last = sparse_index_.back();
        if (file_position - last.file_position >=
            static_cast<byte_count_t>(config_.index_interval_bytes)) {
            should_add = true;
        }
    }

    // Also add if the offset has advanced significantly.
    if (!should_add && !sparse_index_.empty()) {
        const auto& last = sparse_index_.back();
        if (base_offset - last.offset >= 100) {
            should_add = true;
        }
    }

    if (!should_add) {
        // Update the last entry's timestamp if this is more recent.
        if (!sparse_index_.empty() && timestamp > sparse_index_.back().timestamp) {
            sparse_index_.back().timestamp = timestamp;
            index_dirty_ = true;
        }
        return;
    }

    SparseIndexEntry entry;
    entry.offset        = base_offset;
    entry.file_position = file_position;
    entry.timestamp     = timestamp;

    // Insert in sorted order.
    auto it = std::lower_bound(sparse_index_.begin(), sparse_index_.end(), entry);
    sparse_index_.insert(it, entry);

    index_dirty_ = true;

    // Limit index size.
    if (sparse_index_.size() > kMaxInMemoryIndexEntries) {
        // Downsample by removing every other entry.
        std::vector<SparseIndexEntry> downsampled;
        downsampled.reserve(sparse_index_.size() / 2 + 1);
        for (size_t i = 0; i < sparse_index_.size(); i += 2) {
            downsampled.push_back(sparse_index_[i]);
        }
        sparse_index_ = std::move(downsampled);
        index_dirty_ = true;
        get_logger()->warn("Segment({}) index downsampled to {} entries",
                           static_cast<void*>(this), sparse_index_.size());
    }

    get_logger()->trace("Segment({}) index entry added: offset={}, pos={}, ts={}",
                        static_cast<void*>(this), base_offset, file_position, timestamp);
}

void Segment::prune_sparse_index(offset_t from_offset) {
    std::lock_guard<std::mutex> lock(index_mutex_);

    if (sparse_index_.empty()) return;

    // Find the first entry with offset >= from_offset.
    auto it = std::lower_bound(sparse_index_.begin(), sparse_index_.end(), from_offset,
                                [](const SparseIndexEntry& entry, offset_t off) {
                                    return entry.offset < off;
                                });

    if (it == sparse_index_.begin()) {
        // All entries are >= from_offset; clear the index.
        sparse_index_.clear();
    } else if (it != sparse_index_.end()) {
        // Erase [it, end).
        sparse_index_.erase(it, sparse_index_.end());
    }

    index_dirty_ = true;

    // Update max_timestamp.
    if (!sparse_index_.empty()) {
        max_timestamp_.store(sparse_index_.back().timestamp, std::memory_order_release);
    } else {
        max_timestamp_.store(0, std::memory_order_release);
    }

    get_logger()->debug("Segment({}) pruned index to {} entries (from_offset={})",
                        static_cast<void*>(this), sparse_index_.size(), from_offset);
}

// ============================================================================
// Private helpers — index persistence
// ============================================================================

result<void> Segment::load_index() {
    std::lock_guard<std::mutex> lock(index_mutex_);

    std::string idx_path = index_file_path();

    get_logger()->debug("Segment({}) loading index from '{}'", static_cast<void*>(this), idx_path);

    // Check if index file exists.
    if (::access(idx_path.c_str(), F_OK) != 0) {
        get_logger()->info("Segment({}) no index file found at '{}'; will rebuild",
                           static_cast<void*>(this), idx_path);
        return result<void>::failure(error_code::offset_out_of_range,
                                     "Index file not found");
    }

    // Open and read the index file.
    int idx_fd = ::open(idx_path.c_str(), O_RDONLY);
    if (idx_fd < 0) {
        std::string msg = fmt::format("Cannot open index file '{}': {} (errno={})",
                                      idx_path, std::strerror(errno), errno);
        get_logger()->error("Segment({}) {}", static_cast<void*>(this), msg);
        return result<void>::failure(error_code::storage_unavailable, msg);
    }

    // Read header (16 bytes).
    uint8_t hdr[16];
    ssize_t n = safe_read(idx_fd, hdr, sizeof(hdr));
    if (n != sizeof(hdr)) {
        ::close(idx_fd);
        return result<void>::failure(
            error_code::corrupt_message,
            fmt::format("Index file header too short: {} bytes", n));
    }

    // Validate magic.
    if (hdr[0] != 'T' || hdr[1] != 'Q' || hdr[2] != 'I' || hdr[3] != 'X') {
        ::close(idx_fd);
        return result<void>::failure(
            error_code::corrupt_message,
            fmt::format("Index file magic mismatch: {:02X}{:02X}{:02X}{:02X}",
                        hdr[0], hdr[1], hdr[2], hdr[3]));
    }

    uint16_t idx_version = read_le16(hdr + 4);
    if (idx_version != 1) {
        ::close(idx_fd);
        return result<void>::failure(
            error_code::corrupt_message,
            fmt::format("Unsupported index version: {}", idx_version));
    }

    uint32_t entry_count = read_le32(hdr + 6);

    get_logger()->debug("Segment({}) index file header: version={}, entries={}",
                        static_cast<void*>(this), idx_version, entry_count);

    if (entry_count == 0) {
        ::close(idx_fd);
        sparse_index_.clear();
        index_dirty_ = false;
        return result<void>::success();
    }

    // Read entries.
    size_t total_entry_bytes = static_cast<size_t>(entry_count) * 24;

    // Check file size.
    struct stat st;
    if (::fstat(idx_fd, &st) < 0) {
        ::close(idx_fd);
        return result<void>::failure(
            error_code::storage_unavailable,
            fmt::format("fstat index file failed: {} (errno={})",
                        std::strerror(errno), errno));
    }

    if (static_cast<size_t>(st.st_size) < 16 + total_entry_bytes) {
        ::close(idx_fd);
        return result<void>::failure(
            error_code::corrupt_message,
            fmt::format("Index file too small: {} bytes (expected >= {})",
                        st.st_size, 16 + total_entry_bytes));
    }

    std::vector<uint8_t> entry_data(total_entry_bytes);
    n = safe_read(idx_fd, entry_data.data(), total_entry_bytes);
    ::close(idx_fd);

    if (static_cast<size_t>(n) < total_entry_bytes) {
        return result<void>::failure(
            error_code::corrupt_message,
            fmt::format("Short read on index entries: {} of {} bytes", n, total_entry_bytes));
    }

    sparse_index_.clear();
    sparse_index_.reserve(entry_count);

    for (uint32_t i = 0; i < entry_count; ++i) {
        size_t off = static_cast<size_t>(i) * 24;
        SparseIndexEntry entry;
        entry.offset        = read_le64s(entry_data.data() + off);
        entry.file_position = static_cast<byte_count_t>(read_le64(entry_data.data() + off + 8));
        entry.timestamp     = read_le64s(entry_data.data() + off + 16);
        sparse_index_.push_back(entry);
    }

    index_dirty_ = false;

    get_logger()->info("Segment({}) loaded {} index entries from '{}'",
                       static_cast<void*>(this), sparse_index_.size(), idx_path);

    return result<void>::success();
}

std::string Segment::index_file_path() const {
    if (!config_.index_file_path.empty()) {
        return config_.index_file_path;
    }
    return config_.file_path + ".index";
}

// ============================================================================
// Private helpers — error handling
// ============================================================================

void Segment::mark_corrupted(const std::string& reason) {
    auto old_state = state_.load(std::memory_order_acquire);
    state_ = SegmentState::corrupted;
    get_logger()->error("Segment({}) MARKED CORRUPTED (was state={}): {}",
                        static_cast<void*>(this),
                        static_cast<int>(old_state),
                        reason);
}

} // namespace torrent
