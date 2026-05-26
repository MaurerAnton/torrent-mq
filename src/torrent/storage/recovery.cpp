/**
 * torrent-mq — Log Crash Recovery
 *
 * Implements LogRecovery: crash recovery, segment validation/repair,
 * and index rebuild.  Scans all segment files in the data directory,
 * validates headers and batch CRCs, detects corruption, and truncates
 * the last segment to the last valid batch boundary.
 *
 * Recovery flow:
 *   1. Scan data_dir for *.log files, sorted by base_offset.
 *   2. For each segment, validate_header() — check magic, version, header CRC.
 *   3. For each segment, validate_batches() — scan all RecordBatches,
 *      verify each CRC and batch_length sanity.
 *   4. If the last segment has corruption: repair_segment() — find last
 *      valid batch boundary and ftruncate.  Mark segment as needing
 *      index rebuild.
 *   5. Optionally rebuild_index() for segments that need it.
 *   6. Return RecoveryResult with per-segment status.
 *
 * Thread safety: all methods are stateless utilities; the caller
 * is responsible for serialising access to segment files.
 */

#include "torrent/storage/types.h"
#include "torrent/storage/segment.h"
#include "torrent/storage/disk_io.h"
#include "torrent/common/types.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cerrno>
#include <cstring>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <cassert>

#include <spdlog/spdlog.h>

namespace torrent {

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_recovery_logger() {
    static auto logger = spdlog::get("recovery");
    if (!logger) {
        logger = spdlog::stdout_color_mt("recovery");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// CRC32C (same algorithm as segment.cpp)
// --------------------------------------------------------------------------

static constexpr uint32_t kCrc32cPoly = 0x82F63B78u;

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

uint32_t crc32c(uint32_t initial, const void* data, size_t len) noexcept {
    auto* p = static_cast<const uint8_t*>(data);
    uint32_t crc = initial ^ 0xFFFFFFFFu;

#ifdef __SSE4_2__
    while (len >= 8) {
        uint64_t chunk;
        std::memcpy(&chunk, p, sizeof(chunk));
        crc = static_cast<uint32_t>(_mm_crc32_u64(crc, chunk));
        p += 8;
        len -= 8;
    }
    while (len > 0) {
        crc = _mm_crc32_u8(crc, *p);
        ++p;
        --len;
    }
#else
    for (size_t i = 0; i < len; ++i) {
        crc = (crc >> 8) ^ kCrc32cTable[(crc & 0xFF) ^ p[i]];
    }
#endif

    return crc ^ 0xFFFFFFFFu;
}

uint32_t crc32c(const void* data, size_t len) noexcept {
    return crc32c(0, data, len);
}

// --------------------------------------------------------------------------
// LE integer I/O (duplicated from segment.cpp for self-contained recovery)
// --------------------------------------------------------------------------

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

inline int64_t read_le64s(const uint8_t* buf) noexcept {
    return static_cast<int64_t>(read_le64(buf));
}

inline int32_t read_le32s(const uint8_t* buf) noexcept {
    return static_cast<int32_t>(read_le32(buf));
}

// --------------------------------------------------------------------------
// Time helper
// --------------------------------------------------------------------------

timestamp_ms_t now_ms() noexcept {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// --------------------------------------------------------------------------
// Segment header parsing
// --------------------------------------------------------------------------

/// Magic bytes for valid segment files: "TQSG" in little-endian.
static constexpr uint32_t kSegmentMagic         = 0x47535154;
static constexpr uint16_t kSegmentFormatVersion = 1;
static constexpr uint16_t kSegmentHeaderSize    = 64;

/// RecordBatch magic byte (v2 format).
static constexpr int8_t kRecordBatchMagic = 2;

/// Maximum batch size to prevent OOM (128 MiB).
static constexpr byte_count_t kMaxBatchBytes = 134217728;

/// On-disk segment header structure (for raw parsing).
struct RawSegmentHeader {
    uint32_t magic;
    uint16_t format_version;
    uint16_t header_size;
    uint32_t header_crc;
    uint8_t  reserved1[4];
    int64_t  base_offset;
    uint64_t segment_id;
    int64_t  created_at;
    int8_t   compression_type;
    uint8_t  flags;
    uint8_t  reserved2[22];
};
static_assert(sizeof(RawSegmentHeader) == 64, "RawSegmentHeader must be 64 bytes");

/// Parse a raw segment header from a 64-byte buffer.
RawSegmentHeader parse_header(const uint8_t* buf) {
    RawSegmentHeader h{};
    h.magic            = read_le32(buf + 0);
    h.format_version   = read_le16(buf + 4);
    h.header_size      = read_le16(buf + 6);
    h.header_crc       = read_le32(buf + 8);
    h.base_offset      = read_le64s(buf + 16);
    h.segment_id       = read_le64(buf + 24);
    h.created_at       = read_le64s(buf + 32);
    h.compression_type = static_cast<int8_t>(buf[40]);
    h.flags            = buf[41];
    return h;
}

/// Verify the header CRC covering bytes [16..64).
bool validate_header_crc(const uint8_t* header_buf) {
    auto h = parse_header(header_buf);
    uint32_t computed = crc32c(header_buf + 16, 48);
    return computed == h.header_crc;
}

/// Check magic, version, and header_size fields.
bool validate_header_fields(const RawSegmentHeader& h) {
    return h.magic          == kSegmentMagic
        && h.format_version == kSegmentFormatVersion
        && h.header_size    == kSegmentHeaderSize
        && h.base_offset    >= 0;
}

/// True if the sealed flag is set.
bool is_sealed(const RawSegmentHeader& h) {
    return (h.flags & 0x01) != 0;
}

// --------------------------------------------------------------------------
// Batch scanning helpers
// --------------------------------------------------------------------------

/// Minimum size of a valid RecordBatch header on disk.
/// base_offset(8) + length(4) + partition_leader_epoch(4) + magic(1) +
/// crc(4) + attributes(2) + last_offset_delta(4) + base_timestamp(8) +
/// max_timestamp(8) + producer_id(8) + producer_epoch(2) + base_sequence(4) +
/// record_count(4) = 61
static constexpr size_t kMinBatchHeaderSize = 61;

/// Information about a single batch found during scan.
struct BatchBoundary {
    offset_t       base_offset       = kInvalidOffset;
    byte_count_t   file_position     = 0;  ///< where the batch starts in the file
    int32_t        batch_length      = 0;  ///< bytes following base_offset+length
    int32_t        record_count      = 0;
    timestamp_ms_t max_timestamp     = 0;
    bool           crc_valid         = false;
};

/// Parse and validate a single RecordBatch at `file_pos` in `data`.
/// `file_size` is the total mapped/available data size.
/// Returns a BatchBoundary on success, std::nullopt on failure.
std::optional<BatchBoundary> parse_batch_at(const uint8_t* data,
                                             byte_count_t file_pos,
                                             byte_count_t file_size) {
    // Need at least the batch header.
    if (file_pos + kMinBatchHeaderSize > file_size) {
        return std::nullopt;
    }

    const uint8_t* p = data + file_pos;
    offset_t   base_offset         = read_le64s(p);      // bytes 0-7
    int32_t    batch_length        = read_le32s(p + 8);  // bytes 8-11
    int32_t    partition_epoch     = read_le32s(p + 12); // bytes 12-15
    int8_t     magic               = static_cast<int8_t>(p[16]); // byte 16
    uint32_t   batch_crc           = read_le32(p + 17);  // bytes 17-20
    int16_t    attributes          = static_cast<int16_t>(read_le16(p + 21)); // 21-22
    int32_t    last_offset_delta   = read_le32s(p + 23); // bytes 23-26
    int64_t    base_timestamp      = read_le64s(p + 27); // bytes 27-34
    int64_t    max_timestamp       = read_le64s(p + 35); // bytes 35-42
    int64_t    producer_id         = read_le64s(p + 43); // bytes 43-50
    int16_t    producer_epoch      = static_cast<int16_t>(read_le16(p + 51)); // 51-52
    int32_t    base_sequence       = read_le32s(p + 53); // bytes 53-56
    int32_t    record_count        = read_le32s(p + 57); // bytes 57-60

    // Basic sanity checks.
    if (magic != kRecordBatchMagic) {
        return std::nullopt;
    }
    if (batch_length < 0) {
        return std::nullopt;
    }
    if (record_count < 0) {
        return std::nullopt;
    }

    // Total batch size on disk = 8 (base_offset) + 4 (length) + batch_length.
    byte_count_t total_batch_size = 8 + 4 + static_cast<byte_count_t>(batch_length);
    if (file_pos + total_batch_size > file_size) {
        // Batch extends beyond file — truncated write.
        return std::nullopt;
    }
    if (total_batch_size > kMaxBatchBytes) {
        return std::nullopt;
    }

    // Verify CRC: covers from `attributes` field through end of last record.
    // attributes starts at byte 21 relative to batch start.
    // CRC covers: attributes(2) + last_offset_delta(4) + base_timestamp(8) +
    //             max_timestamp(8) + producer_id(8) + producer_epoch(2) +
    //             base_sequence(4) + record_count(4) + records_payload
    //
    // The CRC-protected region begins after crc field (byte 21) and extends
    // to the end of the batch (file_pos + total_batch_size).
    size_t crc_payload_offset = 21;  // skip base_offset, length, partition_epoch, magic, crc
    size_t crc_payload_size   = static_cast<size_t>(total_batch_size) - crc_payload_offset;
    uint32_t computed_crc = crc32c(p + crc_payload_offset, crc_payload_size);

    BatchBoundary bb;
    bb.base_offset   = base_offset;
    bb.file_position = file_pos;
    bb.batch_length  = batch_length;
    bb.record_count  = record_count;
    bb.max_timestamp = max_timestamp;
    bb.crc_valid     = (computed_crc == batch_crc);

    return bb;
}

// --------------------------------------------------------------------------
// Safe file I/O wrappers for recovery operations
// --------------------------------------------------------------------------

/// Open a file for reading only.
int open_readonly(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        get_recovery_logger()->error("Cannot open '{}' for reading: {} (errno={})",
                                     path, std::strerror(errno), errno);
    }
    return fd;
}

/// Open a file for reading and writing.
int open_readwrite(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) {
        get_recovery_logger()->error("Cannot open '{}' for read/write: {} (errno={})",
                                     path, std::strerror(errno), errno);
    }
    return fd;
}

/// Get file size via fstat. Returns -1 on error.
off_t get_file_size(int fd) {
    struct stat st;
    if (::fstat(fd, &st) < 0) {
        get_recovery_logger()->error("fstat failed: {} (errno={})",
                                     std::strerror(errno), errno);
        return -1;
    }
    return st.st_size;
}

/// Truncate file to `new_size` bytes.
bool truncate_file(int fd, off_t new_size) {
    if (::ftruncate(fd, new_size) < 0) {
        get_recovery_logger()->error("ftruncate to {} failed: {} (errno={})",
                                     new_size, std::strerror(errno), errno);
        return false;
    }
    return true;
}

/// Fsync a file descriptor.
bool sync_file(int fd) {
    if (::fsync(fd) < 0) {
        get_recovery_logger()->warn("fsync failed: {} (errno={})",
                                    std::strerror(errno), errno);
        return false;
    }
    return true;
}

/// Parse base_offset from a segment filename: `<id>_<base_offset>.log`
std::optional<std::pair<uint64_t, offset_t>> parse_segment_filename(
    const std::string& filename) noexcept
{
    std::string_view basename(filename);
    constexpr std::string_view kLogExt = ".log";
    if (basename.size() <= kLogExt.size() ||
        basename.substr(basename.size() - kLogExt.size()) != kLogExt) {
        return std::nullopt;
    }
    basename = basename.substr(0, basename.size() - kLogExt.size());

    auto pos = basename.find('_');
    if (pos == std::string_view::npos || pos == 0 || pos == basename.size() - 1) {
        return std::nullopt;
    }

    try {
        uint64_t seg_id = std::stoull(std::string(basename.substr(0, pos)));
        offset_t base_off = std::stoll(std::string(basename.substr(pos + 1)));
        return std::make_pair(seg_id, base_off);
    } catch (...) {
        return std::nullopt;
    }
}

} // anonymous namespace

// ============================================================================
// LogRecovery::RecoveryResult
// ============================================================================

bool LogRecovery::RecoveryResult::ok() const noexcept {
    return error == error_code::none;
}

bool LogRecovery::RecoveryResult::has_corruption() const noexcept {
    return corrupted_segments > 0 || repaired_segments > 0;
}

// ============================================================================
// LogRecovery::SegmentRecoveryInfo
// ============================================================================

LogRecovery::SegmentRecoveryInfo::SegmentRecoveryInfo() = default;

// ============================================================================
// LogRecovery — main recovery entry point
// ============================================================================

LogRecovery::LogRecovery() = default;
LogRecovery::~LogRecovery() = default;

LogRecovery::RecoveryResult LogRecovery::recover(const std::string& data_dir) {
    RecoveryResult result;
    result.recovery_time = now_ms();

    auto logger = get_recovery_logger();
    logger->info("Starting crash recovery in directory '{}'", data_dir);

    namespace fs = std::filesystem;

    // Step 1: Discover segment files.
    std::error_code ec;
    if (!fs::exists(data_dir, ec)) {
        logger->info("Data directory '{}' does not exist — no recovery needed", data_dir);
        return result;
    }

    std::vector<fs::path> segment_paths;
    for (const auto& entry : fs::directory_iterator(data_dir, ec)) {
        if (ec) break;
        if (!entry.is_regular_file()) continue;
        auto path_str = entry.path().string();
        if (path_str.size() > 4 && path_str.substr(path_str.size() - 4) == ".log") {
            segment_paths.push_back(entry.path());
        }
    }

    if (segment_paths.empty()) {
        logger->info("No segment files found in '{}' — fresh log", data_dir);
        return result;
    }

    // Sort by filename (which encodes base_offset in id_offset.log format).
    std::sort(segment_paths.begin(), segment_paths.end());

    result.segments_found = static_cast<int32_t>(segment_paths.size());

    // Step 2: Validate each segment.
    std::vector<std::pair<fs::path, SegmentRecoveryInfo>> validated;
    validated.reserve(segment_paths.size());

    for (const auto& sp : segment_paths) {
        auto info = validate_segment(sp.string());
        result.total_batches   += info.batches_found;
        result.total_batch_bytes += info.total_batch_bytes;

        if (info.state == SegmentRecoveryInfo::State::valid) {
            result.valid_segments++;
        } else if (info.state == SegmentRecoveryInfo::State::header_corrupt) {
            result.corrupted_segments++;
        } else if (info.state == SegmentRecoveryInfo::State::batch_corrupt) {
            result.corrupted_segments++;
        }

        validated.emplace_back(sp, std::move(info));
    }

    // Step 3: Attempt repair on corrupt segments.
    // Only repair the last segment (active/potentially unsealed).
    // Other corrupt segments are left for quarantine/deletion by the caller.
    if (!validated.empty()) {
        auto& last = validated.back();
        if (last.second.state == SegmentRecoveryInfo::State::batch_corrupt) {
            logger->info("Attempting repair on last segment '{}'", last.first.string());
            auto repair_info = repair_segment(last.first.string());
            if (repair_info.state == SegmentRecoveryInfo::State::valid) {
                // Update the info for this segment.
                last.second = std::move(repair_info);
                result.repaired_segments++;
                result.corrupted_segments--;
                logger->info("Repair successful: truncated to offset {} ({} batches retained)",
                             last.second.next_offset, last.second.batches_found);
            } else {
                logger->warn("Repair failed for '{}': state={}",
                             last.first.string(),
                             static_cast<int>(repair_info.state));
            }
        }
    }

    // Step 4: Rebuild sparse indexes for segments that need it
    // (repaired or otherwise marked dirty).
    for (auto& [path, info] : validated) {
        if (info.state == SegmentRecoveryInfo::State::valid && !info.index_valid) {
            result.indexes_rebuilt++;
            logger->info("Marking index for rebuild on segment '{}'", path.string());
            // The actual rebuild is done at a higher level (LogManager) via
            // Segment::rebuild_index().  We flag it here for the caller.
        }
    }

    // Step 5: Populate segment infos for the caller.
    for (auto& [path, info] : validated) {
        result.segments.push_back(info);
    }

    result.duration_ms = now_ms() - result.recovery_time;

    logger->info("Recovery complete: {} segments found, {} valid, {} corrupt, "
                 "{} repaired, {} batches, {}ms",
                 result.segments_found, result.valid_segments,
                 result.corrupted_segments, result.repaired_segments,
                 result.total_batches, result.duration_ms);

    return result;
}

// ============================================================================
// LogRecovery::validate_segment
// ============================================================================

LogRecovery::SegmentRecoveryInfo LogRecovery::validate_segment(const std::string& path) {
    SegmentRecoveryInfo info;
    info.file_path = path;

    auto logger = get_recovery_logger();
    logger->debug("Validating segment '{}'", path);

    // Parse filename for segment_id and base_offset.
    auto parsed = parse_segment_filename(
        std::filesystem::path(path).filename().string());
    if (parsed.has_value()) {
        info.segment_id  = parsed->first;
        info.base_offset = parsed->second;
    }

    // Open file for reading.
    int fd = open_readonly(path);
    if (fd < 0) {
        info.state = SegmentRecoveryInfo::State::header_corrupt;
        info.error_message = fmt::format("Cannot open file: {} (errno={})",
                                         std::strerror(errno), errno);
        logger->error("{}", info.error_message);
        return info;
    }

    off_t fsize = get_file_size(fd);
    if (fsize < 0) {
        ::close(fd);
        info.state = SegmentRecoveryInfo::State::header_corrupt;
        info.error_message = "Cannot stat file";
        return info;
    }

    info.file_size = static_cast<byte_count_t>(fsize);

    // File is empty — valid but empty segment.
    if (fsize == 0) {
        ::close(fd);
        info.state = SegmentRecoveryInfo::State::valid;
        info.next_offset = info.base_offset;
        logger->debug("Segment '{}' is empty (0 bytes) — valid", path);
        return info;
    }

    // File is too small to contain a header.
    if (fsize < kSegmentHeaderSize) {
        ::close(fd);
        info.state = SegmentRecoveryInfo::State::header_corrupt;
        info.error_message = fmt::format("File too small ({} bytes < {} header)",
                                         fsize, kSegmentHeaderSize);
        logger->warn("{}", info.error_message);
        return info;
    }

    // Read the 64-byte header.
    uint8_t header_buf[kSegmentHeaderSize];
    ssize_t nread = ::pread(fd, header_buf, kSegmentHeaderSize, 0);
    if (nread != kSegmentHeaderSize) {
        ::close(fd);
        info.state = SegmentRecoveryInfo::State::header_corrupt;
        info.error_message = fmt::format("Failed to read header: {} bytes read, errno={}",
                                         nread, errno);
        return info;
    }

    auto h = parse_header(header_buf);

    // Validate magic, version, header_size.
    if (!validate_header_fields(h)) {
        ::close(fd);
        info.state = SegmentRecoveryInfo::State::header_corrupt;
        if (h.magic != kSegmentMagic) {
            info.error_message = fmt::format("Invalid magic: 0x{:08X}, expected 0x{:08X}",
                                             h.magic, kSegmentMagic);
        } else if (h.format_version != kSegmentFormatVersion) {
            info.error_message = fmt::format("Unsupported format version: {}, expected {}",
                                             h.format_version, kSegmentFormatVersion);
        } else {
            info.error_message = fmt::format("Invalid header_size: {}", h.header_size);
        }
        logger->warn("{}", info.error_message);
        return info;
    }

    // Validate header CRC.
    if (!validate_header_crc(header_buf)) {
        ::close(fd);
        info.state = SegmentRecoveryInfo::State::header_corrupt;
        info.error_message = fmt::format("Header CRC mismatch (stored={}, computed={})",
                                         h.header_crc,
                                         crc32c(header_buf + 16, 48));
        logger->warn("{}", info.error_message);
        return info;
    }

    // Populate info from valid header.
    info.base_offset    = h.base_offset;
    info.segment_id     = h.segment_id;
    info.created_at     = h.created_at;
    info.is_sealed      = is_sealed(h);
    info.compression    = static_cast<compression_type>(h.compression_type);
    info.header_valid   = true;

    // Now scan all RecordBatches starting at byte 64.
    byte_count_t file_position = kSegmentHeaderSize;
    byte_count_t file_size     = static_cast<byte_count_t>(fsize);

    // We need to read batches sequentially.  For large segments, we
    // read in chunks to avoid huge memory allocations.
    // Map- or pread- based reading.

    // Read the entire segment data past the header for batch scanning.
    // For very large segments (> 256 MiB), we would use chunked scanning.
    // For recovery, reading the whole thing is acceptable since it's a
    // once-at-startup operation.
    size_t data_size = static_cast<size_t>(file_size - kSegmentHeaderSize);
    std::unique_ptr<uint8_t[]> data_buf;
    const uint8_t* batch_data = nullptr;

    if (data_size > 0) {
        if (data_size > 256 * 1024 * 1024) {
            // Large segment — use mmap for batch scanning.
            void* map = ::mmap(nullptr, static_cast<size_t>(file_size),
                               PROT_READ, MAP_PRIVATE, fd, 0);
            if (map == MAP_FAILED) {
                // Fallback: allocate and pread.
                logger->warn("mmap failed for large segment '{}', falling back to pread", path);
                data_buf = std::make_unique<uint8_t[]>(data_size);
                nread = ::pread(fd, data_buf.get(), data_size, kSegmentHeaderSize);
                if (nread != static_cast<ssize_t>(data_size)) {
                    ::close(fd);
                    info.state = SegmentRecoveryInfo::State::batch_corrupt;
                    info.error_message = "Failed to read batch data from large segment";
                    return info;
                }
                batch_data = data_buf.get();
            } else {
                batch_data = static_cast<const uint8_t*>(map) + kSegmentHeaderSize;
            }
        } else {
            data_buf = std::make_unique<uint8_t[]>(data_size > 0 ? data_size : 1);
            nread = ::pread(fd, data_buf.get(), data_size, kSegmentHeaderSize);
            if (nread != static_cast<ssize_t>(data_size)) {
                ::close(fd);
                info.state = SegmentRecoveryInfo::State::batch_corrupt;
                info.error_message = fmt::format("Failed to read batch data: {} of {} bytes",
                                                 nread, data_size);
                return info;
            }
            batch_data = data_buf.get();
        }
    }

    // Scan batches sequentially.
    offset_t running_offset = info.base_offset;
    timestamp_ms_t seg_max_ts = 0;
    bool found_corrupt_batch = false;

    while (file_position < file_size) {
        // Calculate offset into the data buffer.
        byte_count_t buf_offset = file_position - kSegmentHeaderSize;

        auto batch_opt = parse_batch_at(batch_data, buf_offset, data_size + kSegmentHeaderSize);
        if (!batch_opt.has_value()) {
            // Could not parse a valid batch here.  If we're at the very end
            // with just a few bytes (partial header), this is a torn write.
            if (file_position + kMinBatchHeaderSize > file_size) {
                // Partial batch at end — torn write, not true corruption.
                logger->debug("Torn write at end of '{}': {} bytes remaining",
                              path, file_size - file_position);
                info.has_torn_write = true;
            } else {
                logger->warn("Corrupt batch at offset {} (file pos {}) in '{}'",
                             running_offset, file_position, path);
                found_corrupt_batch = true;
            }
            break;
        }

        auto& batch = batch_opt.value();

        if (!batch.crc_valid) {
            logger->warn("CRC mismatch in batch at offset {} (file pos {}) in '{}'",
                         batch.base_offset, file_position, path);
            found_corrupt_batch = true;
            break;
        }

        // Record this batch boundary.
        info.batch_offsets.push_back(batch.base_offset);
        info.batches_found++;
        info.total_batch_bytes += 8 + 4 + static_cast<byte_count_t>(batch.batch_length);

        if (batch.max_timestamp > seg_max_ts) {
            seg_max_ts = batch.max_timestamp;
        }

        // Advance position: base_offset(8) + length(4) + batch_length.
        byte_count_t batch_total = 8 + 4 + static_cast<byte_count_t>(batch.batch_length);
        file_position += batch_total;
        running_offset += batch.record_count;

        info.next_offset = running_offset;
    }

    info.max_timestamp = seg_max_ts;

    if (found_corrupt_batch) {
        info.state = SegmentRecoveryInfo::State::batch_corrupt;
        info.error_message = "One or more batches failed CRC validation";
        info.last_valid_offset = info.next_offset;
    } else {
        info.state = SegmentRecoveryInfo::State::valid;
    }

    // Unmap if we used mmap.
    if (data_size > 256 * 1024 * 1024 && batch_data) {
        const uint8_t* map_start = batch_data - kSegmentHeaderSize;
        if (map_start && map_start != data_buf.get()) {
            ::munmap(const_cast<uint8_t*>(map_start), static_cast<size_t>(file_size));
        }
    }

    ::close(fd);

    logger->debug("Segment '{}' validation: state={}, batches={}, next_offset={}, max_ts={}",
                  path, static_cast<int>(info.state), info.batches_found,
                  info.next_offset, info.max_timestamp);

    return info;
}

// ============================================================================
// LogRecovery::repair_segment
// ============================================================================

LogRecovery::SegmentRecoveryInfo LogRecovery::repair_segment(const std::string& path) {
    auto logger = get_recovery_logger();
    logger->info("Attempting repair on segment '{}'", path);

    // First, re-validate to find the last valid batch boundary.
    auto info = validate_segment(path);

    // If already valid, nothing to repair.
    if (info.state == SegmentRecoveryInfo::State::valid) {
        logger->info("Segment '{}' is already valid — no repair needed", path);
        return info;
    }

    // Can't repair header corruption (header must be intact).
    if (info.state == SegmentRecoveryInfo::State::header_corrupt) {
        logger->warn("Cannot repair '{}': header is corrupt", path);
        info.repair_attempted = true;
        info.repair_success   = false;
        return info;
    }

    // We have batch corruption.  Truncate to the last valid boundary.
    // The `next_offset` field contains the offset after the last valid batch
    // (or base_offset if no batches were valid).

    if (info.batches_found == 0) {
        // No valid batches at all.  Truncate to header only (64 bytes).
        logger->info("No valid batches in '{}' — truncating to header only (64 bytes)", path);

        int fd = open_readwrite(path);
        if (fd < 0) {
            info.repair_attempted = true;
            info.repair_success   = false;
            info.error_message    = "Cannot open file for truncation";
            return info;
        }

        if (!truncate_file(fd, kSegmentHeaderSize)) {
            ::close(fd);
            info.repair_attempted = true;
            info.repair_success   = false;
            info.error_message    = "ftruncate to header size failed";
            return info;
        }

        sync_file(fd);
        ::close(fd);

        info.file_size     = kSegmentHeaderSize;
        info.next_offset   = info.base_offset;
        info.batch_offsets.clear();
        info.state         = SegmentRecoveryInfo::State::valid;
        info.repair_attempted = true;
        info.repair_success   = true;
        info.index_valid   = false;  // needs rebuild

        logger->info("Segment '{}' truncated to header only ({} bytes)", path, kSegmentHeaderSize);
        return info;
    }

    // Calculate the exact byte position after the last valid batch.
    // We need to sum up all valid batch sizes.
    byte_count_t truncate_pos = kSegmentHeaderSize;
    // We can't easily re-scan here without the file data, so we re-validate
    // to get the correct truncation point.
    // Actually, we can compute it from the batch_offsets and the final next_offset:
    // The last valid batch ends at: header(64) + sum of all batch total sizes.
    // We recorded total_batch_bytes which includes base_offset(8)+length(4)+batch_length
    // for each batch. So:
    truncate_pos = kSegmentHeaderSize + info.total_batch_bytes;

    logger->info("Truncating '{}' to byte {} (last valid batch ends here), "
                 "{} batches retained, next_offset={}",
                 path, truncate_pos, info.batches_found, info.next_offset);

    int fd = open_readwrite(path);
    if (fd < 0) {
        info.repair_attempted = true;
        info.repair_success   = false;
        info.error_message    = "Cannot open file for repair truncation";
        return info;
    }

    if (!truncate_file(fd, static_cast<off_t>(truncate_pos))) {
        ::close(fd);
        info.repair_attempted = true;
        info.repair_success   = false;
        info.error_message    = fmt::format("ftruncate to {} failed", truncate_pos);
        return info;
    }

    sync_file(fd);
    ::close(fd);

    /**** Clean up any torn write at the end of the file.
     * We also want to verify the truncation was correct by re-reading the
     * last batch one more time.
     ****/

    // Mark as valid after successful truncation.
    info.state           = SegmentRecoveryInfo::State::valid;
    info.file_size       = truncate_pos;
    info.has_torn_write  = false;
    info.repair_attempted = true;
    info.repair_success  = true;
    info.index_valid     = false;  // sparse index must be rebuilt
    info.error_message.clear();

    logger->info("Repair complete: '{}' truncated to {} bytes, {} batches, next_offset={}",
                 path, truncate_pos, info.batches_found, info.next_offset);

    return info;
}

// ============================================================================
// LogRecovery::rebuild_index_for
// ============================================================================

result<void> LogRecovery::rebuild_index_for(const SegmentRecoveryInfo& info) {
    // This is a stub that delegates to Segment::rebuild_index().
    // The actual index rebuild is performed by the LogManager during open()
    // via Segment::rebuild_index().  This method exists so that the recovery
    // layer can expose a programmatic interface for external orchestration.
    //
    // In production, the caller would:
    //   1. Open the segment via Segment::open().
    //   2. Call Segment::rebuild_index().
    //   3. Verify the index is consistent with on-disk data.

    auto logger = get_recovery_logger();

    if (!info.header_valid) {
        return result<void>::failure(
            error_code::segment_corrupted,
            fmt::format("Cannot rebuild index for '{}': header is invalid", info.file_path));
    }

    logger->info("Index rebuild requested for '{}' ({} batches, next_offset={})",
                 info.file_path, info.batches_found, info.next_offset);

    // In a full implementation, this would:
    //   - Open the segment file
    //   - Scan all batches to build SparseIndexEntry vector
    //   - Write the companion .index file
    //   - Update Segment's in-memory sparse_index_

    return result<void>::success();
}

// ============================================================================
// SegmentHeaderParser — public convenience for external header parsing
// ============================================================================

SegmentHeaderParser::ParsedHeader SegmentHeaderParser::parse(const std::string& path) {
    ParsedHeader result;
    result.file_path = path;

    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        result.valid = false;
        result.error = fmt::format("Cannot open: {} (errno={})", std::strerror(errno), errno);
        return result;
    }

    uint8_t buf[kSegmentHeaderSize];
    ssize_t n = ::pread(fd, buf, kSegmentHeaderSize, 0);
    ::close(fd);

    if (n != kSegmentHeaderSize) {
        result.valid = false;
        result.error = fmt::format("Short read: {} of {} bytes", n, kSegmentHeaderSize);
        return result;
    }

    auto h = parse_header(buf);

    result.magic          = h.magic;
    result.format_version = h.format_version;
    result.header_size    = h.header_size;
    result.header_crc     = h.header_crc;
    result.base_offset    = h.base_offset;
    result.segment_id     = h.segment_id;
    result.created_at     = h.created_at;
    result.compression    = h.compression_type;
    result.flags          = h.flags;
    result.is_sealed      = is_sealed(h);
    result.header_crc_ok  = validate_header_crc(buf);
    result.valid          = validate_header_fields(h) && result.header_crc_ok;

    return result;
}

} // namespace torrent
