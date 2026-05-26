/**
 * snapshot.cpp — LogSnapshot: Consistent Segment Snapshot for Raft
 *
 * Implements creation, installation, and validation of log snapshots
 * used by the Raft consensus layer for log compaction.  Snapshots
 * capture segment metadata and partition watermarks as a self-describing
 * binary file with CRC32C integrity protection and optional zstd
 * compression of the segment table.
 *
 * File format (version 2):
 *   - 72-byte header (magic, version, flags, partition_id, offsets, CRC)
 *   - Segment table (count-prefixed, per-segment wire format)
 *   - Trailer (body_size + body_crc32c)
 *
 * Thread safety:
 *   - This class has no internal mutable state (save snapshot_dir).
 *   - Callers serialize creation/installation externally.
 */

// ============================================================================
// Project headers
// ============================================================================

#include "torrent/storage/snapshot.h"
#include "torrent/common/types.h"

// ============================================================================
// System headers
// ============================================================================

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include <spdlog/spdlog.h>

// zstd compression (optional — graceful degradation if not available).
#if __has_include(<zstd.h>)
    #define TORRENT_HAS_ZSTD 1
    #include <zstd.h>
#else
    #define TORRENT_HAS_ZSTD 0
#endif

namespace torrent {

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace {

/// Logger instance.
std::shared_ptr<spdlog::logger> get_snapshot_logger() {
    static auto logger = spdlog::get("snapshot");
    if (!logger) {
        logger = spdlog::stdout_color_mt("snapshot");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// CRC32C (Castagnoli polynomial)
// --------------------------------------------------------------------------

static const std::array<uint32_t, 256> kCrc32cTable = []() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc & 1) ? (crc >> 1) ^ 0x82F63B78 : crc >> 1;
        }
        table[i] = crc;
    }
    return table;
}();

[[nodiscard]] uint32_t crc32c_update(uint32_t crc,
                                      const uint8_t* data,
                                      size_t len) noexcept {
    crc ^= 0xFFFFFFFF;
    for (size_t i = 0; i < len; ++i) {
        crc = kCrc32cTable[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

// --------------------------------------------------------------------------
// Binary encoding (little-endian for all snapshot fields)
// --------------------------------------------------------------------------

void write_u32_le(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
}

void write_u64_le(std::vector<uint8_t>& buf, uint64_t v) {
    for (int i = 0; i < 8; ++i)
        buf.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
}

void write_i64_le(std::vector<uint8_t>& buf, int64_t v) {
    write_u64_le(buf, static_cast<uint64_t>(v));
}

void write_i32_le(std::vector<uint8_t>& buf, int32_t v) {
    write_u32_le(buf, static_cast<uint32_t>(v));
}

[[nodiscard]] uint32_t read_u32_le(const uint8_t*& p) {
    uint32_t v = static_cast<uint32_t>(p[0])
               | (static_cast<uint32_t>(p[1]) << 8)
               | (static_cast<uint32_t>(p[2]) << 16)
               | (static_cast<uint32_t>(p[3]) << 24);
    p += 4;
    return v;
}

[[nodiscard]] uint64_t read_u64_le(const uint8_t*& p) {
    uint64_t v = static_cast<uint64_t>(p[0])
               | (static_cast<uint64_t>(p[1]) << 8)
               | (static_cast<uint64_t>(p[2]) << 16)
               | (static_cast<uint64_t>(p[3]) << 24)
               | (static_cast<uint64_t>(p[4]) << 32)
               | (static_cast<uint64_t>(p[5]) << 40)
               | (static_cast<uint64_t>(p[6]) << 48)
               | (static_cast<uint64_t>(p[7]) << 56);
    p += 8;
    return v;
}

[[nodiscard]] int64_t read_i64_le(const uint8_t*& p) {
    return static_cast<int64_t>(read_u64_le(p));
}

[[nodiscard]] int32_t read_i32_le(const uint8_t*& p) {
    return static_cast<int32_t>(read_u32_le(p));
}

} // anonymous namespace

// ============================================================================
// LogSnapshot::serialize_segments
// ============================================================================

std::vector<uint8_t> LogSnapshot::serialize_segments(
    const std::vector<SegmentInfo>& segments) const
{
    std::vector<uint8_t> buf;

    // Segment count prefix.
    write_u32_le(buf, static_cast<uint32_t>(segments.size()));

    for (const auto& seg : segments) {
        // Fixed-size fields (80 bytes).
        write_u64_le(buf, seg.segment_id);
        write_i64_le(buf, seg.base_offset);
        write_i64_le(buf, seg.next_offset);
        write_i64_le(buf, seg.file_size);
        write_i64_le(buf, seg.index_size);
        write_i64_le(buf, seg.time_index_size);
        write_i64_le(buf, seg.max_timestamp);
        write_i64_le(buf, seg.created_at);
        write_i64_le(buf, seg.last_modified);

        // Flags: bit 0 = is_active, bit 1 = is_sealed.
        uint32_t flags = 0;
        if (seg.is_active) flags |= 0x01;
        if (seg.is_sealed) flags |= 0x02;
        write_u32_le(buf, flags);

        // Variable-length file path.
        const auto& path = seg.file_path;
        write_u32_le(buf, static_cast<uint32_t>(path.size()));
        buf.insert(buf.end(), path.begin(), path.end());
    }

    return buf;
}

// ============================================================================
// LogSnapshot::deserialize_segments
// ============================================================================

std::optional<std::vector<SegmentInfo>> LogSnapshot::deserialize_segments(
    const uint8_t* data, size_t size) const
{
    if (size < 4) {
        get_snapshot_logger()->error("snapshot: segment table too small for count ({} bytes)", size);
        return std::nullopt;
    }

    const uint8_t* p = data;
    const uint8_t* end = data + size;

    uint32_t count = read_u32_le(p);
    std::vector<SegmentInfo> segments;
    segments.reserve(count);

    for (uint32_t i = 0; i < count; ++i) {
        // Fixed portion: 80 bytes minimum.
        if (static_cast<size_t>(end - p) < 80) {
            get_snapshot_logger()->error(
                "snapshot: truncated segment entry {}: need 80 bytes, have {}",
                i, static_cast<size_t>(end - p));
            return std::nullopt;
        }

        SegmentInfo seg;

        seg.segment_id      = read_u64_le(p);
        seg.base_offset     = static_cast<offset_t>(read_i64_le(p));
        seg.next_offset     = static_cast<offset_t>(read_i64_le(p));
        seg.file_size       = static_cast<byte_count_t>(read_i64_le(p));
        seg.index_size      = static_cast<byte_count_t>(read_i64_le(p));
        seg.time_index_size = static_cast<byte_count_t>(read_i64_le(p));
        seg.max_timestamp   = static_cast<timestamp_ms_t>(read_i64_le(p));
        seg.created_at      = static_cast<timestamp_ms_t>(read_i64_le(p));
        seg.last_modified   = static_cast<timestamp_ms_t>(read_i64_le(p));

        uint32_t flags = read_u32_le(p);
        seg.is_active = (flags & 0x01) != 0;
        seg.is_sealed = (flags & 0x02) != 0;

        // Read file path.
        if (static_cast<size_t>(end - p) < 4) {
            get_snapshot_logger()->error(
                "snapshot: truncated path length for segment {}", i);
            return std::nullopt;
        }
        uint32_t path_len = read_u32_le(p);
        if (path_len > 4096) { // sanity check
            get_snapshot_logger()->error(
                "snapshot: unreasonable path length {} for segment {}", path_len, i);
            return std::nullopt;
        }
        if (static_cast<size_t>(end - p) < path_len) {
            get_snapshot_logger()->error(
                "snapshot: truncated path data for segment {}", i);
            return std::nullopt;
        }
        seg.file_path.assign(reinterpret_cast<const char*>(p), path_len);
        p += path_len;

        segments.push_back(std::move(seg));
    }

    return segments;
}

// ============================================================================
// LogSnapshot::create
// ============================================================================

std::optional<std::string> LogSnapshot::create(
    partition_id_t partition_id,
    offset_t snapshot_index,
    term_t term,
    const std::vector<SegmentInfo>& segments,
    offset_t high_watermark,
    offset_t log_start_offset,
    offset_t last_stable_offset,
    timestamp_ms_t created_at,
    bool compress)
{
    namespace fs = std::filesystem;

    auto logger = get_snapshot_logger();

    // Use current time if not specified.
    if (created_at == 0) {
        created_at = now_ms();
    }

    // Ensure snapshot directory exists.
    if (!snapshot_dir.empty()) {
        std::error_code ec;
        fs::create_directories(snapshot_dir, ec);
        if (ec) {
            logger->error("Cannot create snapshot directory '{}': {}",
                          snapshot_dir, ec.message());
            return std::nullopt;
        }
    }

    // Generate filenames.
    std::string basename = make_filename(partition_id, snapshot_index, term, created_at);
    std::string tmp_path = (fs::path(snapshot_dir) / (basename + ".tmp")).string();
    std::string final_path = (fs::path(snapshot_dir) / basename).string();

    // Serialize the segment table.
    auto segment_table = serialize_segments(segments);

    // Compress if requested.
    uint32_t flags = 0;
    if (compress && !segment_table.empty()) {
#if TORRENT_HAS_ZSTD
        auto compressed = compress_zstd(segment_table.data(),
                                         segment_table.size(),
                                         kDefaultZstdCompressionLevel);
        if (!compressed.empty()) {
            segment_table = std::move(compressed);
            flags |= snapshot_flags::compressed;
            logger->debug("Compressed segment table: {} -> {} bytes",
                          segments.size() * kMinSegmentEntrySize,
                          segment_table.size());
        } else {
            logger->warn("zstd compression failed, storing uncompressed");
            compress = false;
        }
#else
        logger->warn("zstd not available; storing uncompressed segment table");
        compress = false;
#endif
    }

    // Compute body CRC before writing.
    uint64_t body_size = segment_table.size();
    uint32_t body_crc = compute_crc32c(segment_table.data(), body_size);

    // Build header (72 bytes).
    std::vector<uint8_t> header;
    header.reserve(kSnapshotHeaderSize);

    // Magic (8 bytes).
    header.insert(header.end(), kSnapshotFileMagic, kSnapshotFileMagic + 8);

    // Version, flags, partition_id (4+4+4 = 12 bytes).
    write_u32_le(header, kSnapshotFileVersion);
    write_u32_le(header, flags);
    write_i32_le(header, static_cast<int32_t>(partition_id));

    // Raft metadata (8+8 = 16 bytes).
    write_i64_le(header, snapshot_index);
    write_i64_le(header, static_cast<int64_t>(term));

    // Watermarks (8+8+8 = 24 bytes).
    write_i64_le(header, high_watermark);
    write_i64_le(header, log_start_offset);
    write_i64_le(header, last_stable_offset);

    // Created at (8 bytes).
    write_i64_le(header, created_at);

    // Placeholder for header CRC (4 bytes).
    size_t crc_offset = header.size();
    write_u32_le(header, 0);

    // Sanity check header size.
    if (header.size() != kSnapshotHeaderSize) {
        logger->error("Header size mismatch: expected {}, got {}",
                      kSnapshotHeaderSize, header.size());
        return std::nullopt;
    }

    // Compute and patch header CRC (covers all bytes before the CRC slot).
    uint32_t header_crc = compute_crc32c(header.data(), crc_offset);
    header[crc_offset]     = static_cast<uint8_t>(header_crc & 0xFF);
    header[crc_offset + 1] = static_cast<uint8_t>((header_crc >> 8) & 0xFF);
    header[crc_offset + 2] = static_cast<uint8_t>((header_crc >> 16) & 0xFF);
    header[crc_offset + 3] = static_cast<uint8_t>((header_crc >> 24) & 0xFF);

    // Assemble full file: header + segment_table + body_size + body_crc.
    std::vector<uint8_t> file_buf;
    file_buf.reserve(header.size() + segment_table.size() + 12);
    file_buf.insert(file_buf.end(), header.begin(), header.end());
    file_buf.insert(file_buf.end(), segment_table.begin(), segment_table.end());
    write_u64_le(file_buf, body_size);
    write_u32_le(file_buf, body_crc);

    // Size check.
    if (file_buf.size() > kMaxSnapshotFileSize) {
        logger->error("Snapshot too large: {} bytes (max {})",
                      file_buf.size(), kMaxSnapshotFileSize);
        return std::nullopt;
    }

    // Write atomically.
    if (!write_file_atomic(tmp_path, file_buf)) {
        logger->error("Failed to write snapshot temporary file '{}'", tmp_path);
        fs::remove(tmp_path);
        return std::nullopt;
    }

    // Atomic rename.
    std::error_code ec;
    fs::rename(tmp_path, final_path, ec);
    if (ec) {
        logger->error("Failed to rename {} -> {}: {}",
                      tmp_path, final_path, ec.message());
        fs::remove(tmp_path, ec);
        return std::nullopt;
    }

    logger->info("Created snapshot '{}': partition={}, index={}, term={}, "
                 "segments={}, size={} bytes, compressed={}",
                 final_path, partition_id, snapshot_index, term,
                 segments.size(), file_buf.size(), compress);

    return final_path;
}

// ============================================================================
// LogSnapshot::create_from_metadata
// ============================================================================

std::optional<std::string> LogSnapshot::create_from_metadata(
    const SnapshotMetadata& meta,
    bool compress)
{
    return create(
        meta.partition_id,
        meta.snapshot_index,
        meta.term,
        meta.segments,
        meta.high_watermark,
        meta.log_start_offset,
        meta.last_stable_offset,
        meta.created_at,
        compress);
}

// ============================================================================
// LogSnapshot::install
// ============================================================================

std::optional<SnapshotMetadata> LogSnapshot::install(
    const std::string& snapshot_path)
{
    auto data = read_file(snapshot_path);
    if (!data) {
        get_snapshot_logger()->error("Cannot read snapshot file '{}'", snapshot_path);
        return std::nullopt;
    }
    return install_from_buffer(*data);
}

// ============================================================================
// LogSnapshot::install_from_buffer
// ============================================================================

std::optional<SnapshotMetadata> LogSnapshot::install_from_buffer(
    const std::vector<uint8_t>& data)
{
    auto logger = get_snapshot_logger();

    // Minimum size: header (72) + segment_count (4) + body_size (8) + body_crc (4) = 88.
    if (data.size() < kSnapshotHeaderSize + 4 + 12) {
        logger->error("Snapshot buffer too small: {} bytes (min {})",
                      data.size(), kSnapshotHeaderSize + 4 + 12);
        return std::nullopt;
    }

    // Size sanity check.
    if (data.size() > kMaxSnapshotFileSize) {
        logger->error("Snapshot buffer exceeds max size: {} > {}",
                      data.size(), kMaxSnapshotFileSize);
        return std::nullopt;
    }

    const uint8_t* p = data.data();
    const uint8_t* end = data.data() + data.size();

    // --- Parse header ---

    // Magic.
    if (std::memcmp(p, kSnapshotFileMagic, 8) != 0) {
        logger->error("Bad snapshot magic: expected '{}', got '{:.8}'",
                      std::string(kSnapshotFileMagic, 8),
                      std::string(reinterpret_cast<const char*>(p),
                                  std::min(size_t(8), data.size())));
        return std::nullopt;
    }
    p += 8;

    // Version.
    uint32_t version = read_u32_le(p);
    if (version != kSnapshotFileVersion) {
        logger->error("Unsupported snapshot version {} (expected {})",
                      version, kSnapshotFileVersion);
        return std::nullopt;
    }

    // Flags.
    uint32_t flags = read_u32_le(p);
    bool is_compressed = (flags & snapshot_flags::compressed) != 0;

    // Partition ID.
    partition_id_t partition_id = static_cast<partition_id_t>(read_i32_le(p));

    // Raft metadata.
    offset_t snapshot_index = static_cast<offset_t>(read_i64_le(p));
    term_t   term           = static_cast<term_t>(read_i64_le(p));

    // Watermarks.
    offset_t high_watermark    = static_cast<offset_t>(read_i64_le(p));
    offset_t log_start_offset  = static_cast<offset_t>(read_i64_le(p));
    offset_t last_stable_offset = static_cast<offset_t>(read_i64_le(p));

    // Created at.
    timestamp_ms_t created_at = static_cast<timestamp_ms_t>(read_i64_le(p));

    // Header CRC.
    uint32_t stored_hdr_crc = read_u32_le(p);

    // Verify header CRC: from start of header to just before the CRC field.
    size_t crc_offset = static_cast<size_t>(p - data.data()) - 4;
    uint32_t computed_hdr_crc = compute_crc32c(data.data(), crc_offset);
    if (computed_hdr_crc != stored_hdr_crc) {
        logger->error("Header CRC mismatch: computed 0x{:08X}, stored 0x{:08X}",
                      computed_hdr_crc, stored_hdr_crc);
        return std::nullopt;
    }

    // --- Parse segment table ---

    // Body size trailer is at: end - 12.
    size_t trailer_offset = data.size() - 12;
    if (static_cast<size_t>(p - data.data()) > trailer_offset) {
        logger->error("Snapshot layout error: header overlaps trailer");
        return std::nullopt;
    }

    const uint8_t* trailer_ptr = data.data() + trailer_offset;
    uint64_t stored_body_size = read_u64_le(trailer_ptr);
    uint32_t stored_body_crc  = read_u32_le(trailer_ptr);

    size_t segment_table_size = data.size() - kSnapshotHeaderSize - 12;
    if (segment_table_size != stored_body_size) {
        logger->warn("Snapshot body size mismatch: computed {} vs stored {}",
                     segment_table_size, stored_body_size);
        // Trust the stored value if it's within bounds.
        if (stored_body_size <= data.size() - kSnapshotHeaderSize - 12) {
            segment_table_size = stored_body_size;
        } else {
            logger->error("Stored body size {} exceeds available data", stored_body_size);
            return std::nullopt;
        }
    }

    // Body CRC check.
    uint32_t computed_body_crc = compute_crc32c(
        data.data() + kSnapshotHeaderSize, segment_table_size);
    if (computed_body_crc != stored_body_crc) {
        logger->error("Body CRC mismatch: computed 0x{:08X}, stored 0x{:08X}",
                      computed_body_crc, stored_body_crc);
        return std::nullopt;
    }

    // --- Deserialize segment table ---

    const uint8_t* seg_data = data.data() + kSnapshotHeaderSize;

    // Decompress if needed.
    std::vector<uint8_t> decompressed_buf;
    if (is_compressed) {
#if TORRENT_HAS_ZSTD
        // Estimate uncompressed size (heuristic: 5x compressed size, capped).
        size_t estimated = std::min(segment_table_size * 5, kMaxSnapshotFileSize);
        decompressed_buf = decompress_zstd(seg_data, segment_table_size, estimated);
        if (decompressed_buf.empty()) {
            logger->error("Failed to decompress segment table");
            return std::nullopt;
        }
        seg_data = decompressed_buf.data();
        segment_table_size = decompressed_buf.size();
#else
        logger->error("Snapshot is compressed but zstd is not available");
        return std::nullopt;
#endif
    }

    auto segments = deserialize_segments(seg_data, segment_table_size);
    if (!segments) {
        return std::nullopt;
    }

    // --- Build result ---

    SnapshotMetadata meta;
    meta.partition_id       = partition_id;
    meta.snapshot_index     = snapshot_index;
    meta.term               = term;
    meta.segments           = std::move(*segments);
    meta.high_watermark     = high_watermark;
    meta.log_start_offset   = log_start_offset;
    meta.last_stable_offset = last_stable_offset;
    meta.created_at         = created_at;
    meta.crc                = stored_body_crc;  // track the CRC for reference

    logger->info("Installed snapshot: partition={}, index={}, term={}, "
                 "segments={}, size={} bytes, compressed={}",
                 partition_id, snapshot_index, term,
                 meta.segments.size(), data.size(), is_compressed);

    return meta;
}

// ============================================================================
// LogSnapshot::validate
// ============================================================================

bool LogSnapshot::validate(const std::string& snapshot_path) const {
    auto data = read_file(snapshot_path);
    if (!data) {
        get_snapshot_logger()->warn("Snapshot validation failed: cannot read '{}'", snapshot_path);
        return false;
    }

    if (data->size() > kMaxSnapshotFileSize) {
        get_snapshot_logger()->warn("Snapshot '{}' exceeds max size: {} > {}",
                                    snapshot_path, data->size(), kMaxSnapshotFileSize);
        return false;
    }

    if (data->size() < kSnapshotHeaderSize) {
        get_snapshot_logger()->warn("Snapshot '{}' too small: {} bytes", snapshot_path, data->size());
        return false;
    }

    // Validate magic.
    if (std::memcmp(data->data(), kSnapshotFileMagic, 8) != 0) {
        get_snapshot_logger()->warn("Snapshot '{}' bad magic", snapshot_path);
        return false;
    }

    // Validate version.
    const uint8_t* p = data->data() + 8;
    uint32_t version = read_u32_le(p);
    if (version != kSnapshotFileVersion) {
        get_snapshot_logger()->warn("Snapshot '{}' version {} != expected {}",
                                    snapshot_path, version, kSnapshotFileVersion);
        return false;
    }

    // Validate header CRC.
    uint32_t stored_hdr_crc = 0;
    {
        // Back up to the CRC slot (at offset 68).
        const uint8_t* crc_ptr = data->data() + 68;
        stored_hdr_crc = read_u32_le(crc_ptr);
    }
    size_t hdr_crc_coverage = kSnapshotHeaderSize - 4; // exclude the CRC field itself
    uint32_t computed_hdr_crc = compute_crc32c(data->data(), hdr_crc_coverage);
    if (computed_hdr_crc != stored_hdr_crc) {
        get_snapshot_logger()->warn("Snapshot '{}' header CRC mismatch: 0x{:08X} vs 0x{:08X}",
                                    snapshot_path, computed_hdr_crc, stored_hdr_crc);
        return false;
    }

    // Validate body CRC.
    if (data->size() >= kSnapshotHeaderSize + 12) {
        size_t body_size = data->size() - kSnapshotHeaderSize - 12;
        const uint8_t* trailer = data->data() + kSnapshotHeaderSize + body_size;

        uint64_t stored_body_size = 0;
        uint32_t stored_body_crc = 0;
        {
            const uint8_t* tp = trailer;
            stored_body_size = read_u64_le(tp);
            stored_body_crc  = read_u32_le(tp);
        }

        if (body_size != stored_body_size) {
            get_snapshot_logger()->warn("Snapshot '{}' body size mismatch: {} vs {}",
                                        snapshot_path, body_size, stored_body_size);
            return false;
        }

        uint32_t computed_body_crc = compute_crc32c(
            data->data() + kSnapshotHeaderSize, body_size);
        if (computed_body_crc != stored_body_crc) {
            get_snapshot_logger()->warn("Snapshot '{}' body CRC mismatch: 0x{:08X} vs 0x{:08X}",
                                        snapshot_path, computed_body_crc, stored_body_crc);
            return false;
        }
    }

    get_snapshot_logger()->debug("Snapshot '{}' validated successfully", snapshot_path);
    return true;
}

// ============================================================================
// LogSnapshot::validate_quick
// ============================================================================

bool LogSnapshot::validate_quick(const std::string& snapshot_path) const {
    namespace fs = std::filesystem;

    std::error_code ec;
    if (!fs::exists(snapshot_path, ec)) return false;

    auto sz = fs::file_size(snapshot_path, ec);
    if (ec) return false;
    if (sz < kSnapshotHeaderSize || sz > kMaxSnapshotFileSize) return false;

    // Read just the header.
    std::ifstream in(snapshot_path, std::ios::binary);
    if (!in) return false;

    char magic[8];
    in.read(magic, 8);
    if (!in || std::memcmp(magic, kSnapshotFileMagic, 8) != 0) return false;

    // Check version.
    char version_buf[4];
    in.read(version_buf, 4);
    if (!in) return false;

    uint32_t version = static_cast<uint32_t>(static_cast<unsigned char>(version_buf[0]))
                     | (static_cast<uint32_t>(static_cast<unsigned char>(version_buf[1])) << 8)
                     | (static_cast<uint32_t>(static_cast<unsigned char>(version_buf[2])) << 16)
                     | (static_cast<uint32_t>(static_cast<unsigned char>(version_buf[3])) << 24);

    return version == kSnapshotFileVersion;
}

// ============================================================================
// LogSnapshot::read_header
// ============================================================================

std::optional<SnapshotHeader> LogSnapshot::read_header(
    const std::string& snapshot_path) const
{
    auto data = read_file(snapshot_path);
    if (!data || data->size() < kSnapshotHeaderSize) return std::nullopt;

    const uint8_t* p = data->data();

    // Magic.
    if (std::memcmp(p, kSnapshotFileMagic, 8) != 0) return std::nullopt;
    p += 8;

    SnapshotHeader hdr;
    hdr.version           = read_u32_le(p);
    hdr.flags             = read_u32_le(p);
    hdr.partition_id      = static_cast<partition_id_t>(read_i32_le(p));
    hdr.snapshot_index    = static_cast<offset_t>(read_i64_le(p));
    hdr.term              = static_cast<term_t>(read_i64_le(p));
    hdr.high_watermark    = static_cast<offset_t>(read_i64_le(p));
    hdr.log_start_offset  = static_cast<offset_t>(read_i64_le(p));
    hdr.last_stable_offset = static_cast<offset_t>(read_i64_le(p));
    hdr.created_at        = static_cast<timestamp_ms_t>(read_i64_le(p));

    return hdr;
}

// ============================================================================
// LogSnapshot::list_snapshots
// ============================================================================

std::vector<std::string> LogSnapshot::list_snapshots() const {
    namespace fs = std::filesystem;
    std::vector<std::string> result;

    if (snapshot_dir.empty()) return result;

    std::error_code ec;
    if (!fs::exists(snapshot_dir, ec)) return result;

    // Collect .snap files.
    for (const auto& entry : fs::directory_iterator(snapshot_dir, ec)) {
        if (ec) break;
        if (entry.is_regular_file() && entry.path().extension() == ".snap") {
            result.push_back(entry.path().string());
        }
    }

    // Sort by modification time (newest first).
    std::sort(result.begin(), result.end(),
              [](const std::string& a, const std::string& b) {
                  std::error_code ea, eb;
                  auto ta = std::filesystem::last_write_time(a, ea);
                  auto tb = std::filesystem::last_write_time(b, eb);
                  if (ea || eb) return a > b;  // fallback to name sort
                  return ta > tb;
              });

    return result;
}

// ============================================================================
// LogSnapshot::find_latest
// ============================================================================

std::optional<std::string> LogSnapshot::find_latest() const {
    auto snapshots = list_snapshots();
    for (const auto& path : snapshots) {
        if (validate_quick(path)) {
            return path;
        }
    }
    return std::nullopt;
}

// ============================================================================
// LogSnapshot::prune_snapshots
// ============================================================================

size_t LogSnapshot::prune_snapshots(size_t keep_count) {
    namespace fs = std::filesystem;
    auto snapshots = list_snapshots();
    size_t removed = 0;

    for (size_t i = keep_count; i < snapshots.size(); ++i) {
        std::error_code ec;
        fs::remove(snapshots[i], ec);
        if (!ec) {
            ++removed;
        } else {
            get_snapshot_logger()->warn("Failed to delete snapshot '{}': {}",
                                        snapshots[i], ec.message());
        }
    }

    if (removed > 0) {
        get_snapshot_logger()->info("Pruned {} old snapshots", removed);
    }

    return removed;
}

// ============================================================================
// LogSnapshot::delete_snapshot
// ============================================================================

bool LogSnapshot::delete_snapshot(const std::string& snapshot_path) {
    std::error_code ec;
    std::filesystem::remove(snapshot_path, ec);
    if (ec) {
        get_snapshot_logger()->warn("Failed to delete '{}': {}", snapshot_path, ec.message());
        return false;
    }
    return true;
}

// ============================================================================
// Static helpers
// ============================================================================

uint32_t LogSnapshot::compute_crc32c(const uint8_t* data, size_t len) noexcept {
    return crc32c_update(0, data, len);
}

#if TORRENT_HAS_ZSTD

std::vector<uint8_t> LogSnapshot::compress_zstd(
    const uint8_t* data, size_t len, int level)
{
    size_t bound = ZSTD_compressBound(len);
    std::vector<uint8_t> result(bound);

    size_t compressed_size = ZSTD_compress(
        result.data(), bound, data, len, level);

    if (ZSTD_isError(compressed_size)) {
        get_snapshot_logger()->error("zstd compress error: {}",
                                     ZSTD_getErrorName(compressed_size));
        return {};
    }

    result.resize(compressed_size);
    return result;
}

std::vector<uint8_t> LogSnapshot::decompress_zstd(
    const uint8_t* data, size_t len, size_t expected_size)
{
    // First get the decompressed size.
    unsigned long long decompressed_size = ZSTD_getFrameContentSize(data, len);
    if (decompressed_size == ZSTD_CONTENTSIZE_ERROR) {
        get_snapshot_logger()->error("zstd: invalid compressed data");
        return {};
    }
    if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
        // Size not embedded in frame; use hint.
        decompressed_size = expected_size;
    }

    if (decompressed_size > kMaxSnapshotFileSize) {
        get_snapshot_logger()->error("zstd: decompressed size exceeds max");
        return {};
    }

    std::vector<uint8_t> result(static_cast<size_t>(decompressed_size));
    size_t actual = ZSTD_decompress(result.data(), result.size(), data, len);

    if (ZSTD_isError(actual)) {
        get_snapshot_logger()->error("zstd decompress error: {}",
                                     ZSTD_getErrorName(actual));
        return {};
    }

    result.resize(actual);
    return result;
}

#else  // !TORRENT_HAS_ZSTD

std::vector<uint8_t> LogSnapshot::compress_zstd(
    const uint8_t*, size_t, int)
{
    return {};
}

std::vector<uint8_t> LogSnapshot::decompress_zstd(
    const uint8_t*, size_t, size_t)
{
    return {};
}

#endif

std::string LogSnapshot::make_filename(
    partition_id_t partition_id,
    offset_t snapshot_index,
    term_t term,
    timestamp_ms_t created_at)
{
    std::ostringstream oss;
    oss << "snapshot-"
        << std::setfill('0')
        << "p" << partition_id << "-"
        << std::setw(20) << snapshot_index << "-"
        << std::setw(6) << term << "-"
        << std::setw(16) << created_at
        << ".snap";
    return oss.str();
}

timestamp_ms_t LogSnapshot::now_ms() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

bool LogSnapshot::write_file_atomic(
    const std::string& path,
    const std::vector<uint8_t>& data)
{
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        get_snapshot_logger()->error("Cannot open '{}' for writing: {}",
                                     path, std::strerror(errno));
        return false;
    }

    const uint8_t* p = data.data();
    size_t remaining = data.size();
    while (remaining > 0) {
        ssize_t n = ::write(fd, p, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            get_snapshot_logger()->error("write to '{}' failed: {}",
                                         path, std::strerror(errno));
            ::close(fd);
            return false;
        }
        p += static_cast<size_t>(n);
        remaining -= static_cast<size_t>(n);
    }

    // Fsync for durability.
    ::fsync(fd);
    ::close(fd);

    return true;
}

std::optional<std::vector<uint8_t>> LogSnapshot::read_file(
    const std::string& path)
{
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in) return std::nullopt;

    auto size = static_cast<size_t>(in.tellg());
    if (size > kMaxSnapshotFileSize) return std::nullopt;

    std::vector<uint8_t> data(size);
    in.seekg(0);
    in.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(size));
    if (!in) return std::nullopt;

    return data;
}

} // namespace torrent
