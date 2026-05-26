#pragma once

/**
 * torrent-mq — LogSnapshot: Consistent Segment Snapshot for Raft
 *
 * LogSnapshot creates, installs, and validates persistent, consistent
 * snapshots of the partition log's segment metadata and watermarks.
 * These snapshots are used by the Raft consensus layer to compact the
 * Raft log: instead of replaying all log entries from index 0, a new
 * or recovering node installs the latest snapshot and catches up only
 * the entries committed after the snapshot index.
 *
 * A snapshot does NOT contain the full segment data — it captures only
 * the metadata (segment info, watermarks, Raft metadata).  The actual
 * segment files remain on disk (or in tiered storage).  This keeps
 * snapshots small and fast to create even for multi-TB partitions.
 *
 * Snapshot File Format (version 2):
 *
 *   ┌──────────────────────────────────────────────────────────────────┐
 *   │                         HEADER (64 bytes)                         │
 *   │  magic:8             "TQSNAPSH" (no null terminator)             │
 *   │  version:4           uint32_t, currently 2                       │
 *   │  flags:4             uint32_t: bit 0=compressed, 1-31=reserved   │
 *   │  partition_id:4      int32_t                                     │
 *   │  snapshot_index:8    int64_t (Raft log index)                    │
 *   │  term:8              int64_t (Raft term)                         │
 *   │  high_watermark:8    int64_t                                     │
 *   │  log_start_offset:8  int64_t                                     │
 *   │  last_stable_offset:8 int64_t                                    │
 *   │  created_at:8        int64_t (ms since epoch)                    │
 *   │  header_crc32c:4     covers header bytes [0..60)                 │
 *   ├──────────────────────────────────────────────────────────────────┤
 *   │                    SEGMENT TABLE (variable)                       │
 *   │  segment_count:4     uint32_t                                    │
 *   │  For each segment:                                               │
 *   │    segment_id:8      uint64_t                                    │
 *   │    base_offset:8     int64_t                                     │
 *   │    next_offset:8     int64_t                                     │
 *   │    file_size:8       int64_t                                     │
 *   │    index_size:8      int64_t                                     │
 *   │    time_index_size:8  int64_t                                    │
 *   │    max_timestamp:8   int64_t                                     │
 *   │    created_at:8      int64_t                                     │
 *   │    last_modified:8   int64_t                                     │
 *   │    flags:4           uint32_t: bit 0=is_active, bit 1=is_sealed  │
 *   │    file_path_len:4   uint32_t                                    │
 *   │    file_path:N       UTF-8 bytes (no null terminator)            │
 *   ├──────────────────────────────────────────────────────────────────┤
 *   │                     BODY CRC (12 bytes)                           │
 *   │  body_size:8         uint64_t (size of segment table above)      │
 *   │  body_crc32c:4       uint32_t                                    │
 *   └──────────────────────────────────────────────────────────────────┘
 *
 * Optional compression (flags bit 0): when set, the segment table is
 * compressed with zstd before writing.  body_size reflects the compressed
 * size, and body_crc32c covers the compressed bytes.
 *
 * Thread safety:
 *   - snapshot creation is called under the LogManager's read lock.
 *   - install and validate are standalone operations that modify disk
 *     state; they should be serialized externally (by the Raft layer).
 */

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <optional>
#include <filesystem>

#include "torrent/common/types.h"
#include "torrent/storage/types.h"

namespace torrent {

// ============================================================================
// Snapshot format constants
// ============================================================================

/// Magic bytes at the start of every snapshot file: "TQSNAPSH"
inline constexpr const char kSnapshotFileMagic[9] = "TQSNAPSH";

/// Current snapshot file format version.
inline constexpr uint32_t kSnapshotFileVersion = 2;

/// Snapshot header size (fixed portion): 8+4+4+4+8+8+8+8+8+8+4 = 72 bytes
inline constexpr size_t kSnapshotHeaderSize = 72;

/// Flags for the snapshot format.
namespace snapshot_flags {
    /// Segment table is compressed with zstd.
    inline constexpr uint32_t compressed = 0x00000001;
    /// Reserved for future use.
    inline constexpr uint32_t reserved   = 0xFFFFFFFE;
}

/// Minimum segment table entry size without file_path:
/// 8+8+8+8+8+8+8+8+8+4+4 = 80 bytes
inline constexpr size_t kMinSegmentEntrySize = 80;

/// Maximum snapshot total size (1 GiB for safety).
inline constexpr size_t kMaxSnapshotFileSize = 1073741824;

/// Default zstd compression level for snapshot body.
inline constexpr int kDefaultZstdCompressionLevel = 3;

// ============================================================================
// SnapshotSegmentEntry — per-segment metadata in snapshot
// ============================================================================

/// Wire-format representation of a single segment within a snapshot.
/// Mirrors SegmentInfo but is encoded in a compact binary form.
struct SnapshotSegmentEntry {
    uint64_t       segment_id      = 0;
    offset_t       base_offset     = kInvalidOffset;
    offset_t       next_offset     = kInvalidOffset;
    byte_count_t   file_size       = 0;
    byte_count_t   index_size      = 0;
    byte_count_t   time_index_size = 0;
    timestamp_ms_t max_timestamp   = 0;
    timestamp_ms_t created_at      = 0;
    timestamp_ms_t last_modified   = 0;
    bool           is_active       = false;
    bool           is_sealed       = false;
    std::string    file_path;

    /// Convert from the runtime SegmentInfo type.
    static SnapshotSegmentEntry from_segment_info(const SegmentInfo& si) {
        SnapshotSegmentEntry e;
        e.segment_id      = si.segment_id;
        e.base_offset     = si.base_offset;
        e.next_offset     = si.next_offset;
        e.file_size       = si.file_size;
        e.index_size      = si.index_size;
        e.time_index_size = si.time_index_size;
        e.max_timestamp   = si.max_timestamp;
        e.created_at      = si.created_at;
        e.last_modified   = si.last_modified;
        e.is_active       = si.is_active;
        e.is_sealed       = si.is_sealed;
        e.file_path       = si.file_path;
        return e;
    }

    /// Convert back to SegmentInfo.
    [[nodiscard]] SegmentInfo to_segment_info() const {
        SegmentInfo si;
        si.segment_id      = segment_id;
        si.base_offset     = base_offset;
        si.next_offset     = next_offset;
        si.file_size       = file_size;
        si.index_size      = index_size;
        si.time_index_size = time_index_size;
        si.max_timestamp   = max_timestamp;
        si.created_at      = created_at;
        si.last_modified   = last_modified;
        si.is_active       = is_active;
        si.is_sealed       = is_sealed;
        si.file_path       = file_path;
        return si;
    }
};

// ============================================================================
// SnapshotHeader — in-memory representation of the snapshot header
// ============================================================================

struct SnapshotHeader {
    uint32_t       version           = kSnapshotFileVersion;
    uint32_t       flags             = 0;
    partition_id_t partition_id      = 0;
    offset_t       snapshot_index    = kInvalidOffset;
    term_t         term              = 0;
    offset_t       high_watermark    = kInvalidOffset;
    offset_t       log_start_offset  = kInvalidOffset;
    offset_t       last_stable_offset = kInvalidOffset;
    timestamp_ms_t created_at        = 0;
};

// ============================================================================
// LogSnapshot — snapshot create / install / validate
// ============================================================================

/**
 * Manages consistent snapshots of the partition log for Raft compaction.
 *
 * A LogSnapshot is a point-in-time capture of all segment metadata and
 * partition watermarks, serialized to a file on disk.  The Raft layer
 * uses these snapshots to:
 *
 *   1. Compact the Raft log by discarding entries before snapshot_index.
 *   2. Bootstrap new nodes without replaying the full Raft log.
 *   3. Recover from disk corruption by restoring to a known-good state.
 *
 * Snapshot files are self-describing: they contain a magic number,
 * version, CRC32C checksums, and all metadata needed to reconstruct
 * the partition's segment list and watermarks.
 */
class LogSnapshot {
public:
    // -- Configuration ----------------------------------------------------

    /// Directory where snapshot files are stored.
    std::string snapshot_dir;

    // -- Snapshot creation ------------------------------------------------

    /**
     * Create a new snapshot file from the given segment metadata and
     * watermarks.
     *
     * Writes to a temporary file and renames atomically on success.
     * The caller is responsible for serializing the write (typically
     * via the LogManager's shared read lock during creation).
     *
     * @param partition_id       Owning partition.
     * @param snapshot_index     Raft log index covered by this snapshot.
     * @param term               Raft term at snapshot time.
     * @param segments           Ordered list of segment metadata.
     * @param high_watermark     Current high watermark.
     * @param log_start_offset   Current log start offset.
     * @param last_stable_offset Current last stable offset.
     * @param created_at         Wall-clock creation timestamp (0 = now).
     * @param compress           If true, compress the segment table with zstd.
     * @return                   Path to the created snapshot file, or
     *                           nullopt on error.
     */
    [[nodiscard]] std::optional<std::string> create(
        partition_id_t partition_id,
        offset_t snapshot_index,
        term_t term,
        const std::vector<SegmentInfo>& segments,
        offset_t high_watermark,
        offset_t log_start_offset,
        offset_t last_stable_offset,
        timestamp_ms_t created_at = 0,
        bool compress = false);

    /**
     * Create a snapshot from SnapshotMetadata (convenience overload).
     *
     * @param meta   SnapshotMetadata as produced by LogManager::create_snapshot().
     * @param compress  If true, compress segment table.
     * @return       Path to the created file, or nullopt on error.
     */
    [[nodiscard]] std::optional<std::string> create_from_metadata(
        const SnapshotMetadata& meta,
        bool compress = false);

    // -- Snapshot installation --------------------------------------------

    /**
     * Install (restore) state from a snapshot file.
     *
     * Validates the snapshot's header CRC and body CRC.  On success,
     * returns the parsed snapshot metadata.  The caller is responsible
     * for deleting existing segments and applying the new state.
     *
     * This method does NOT delete any files — it only reads and
     * validates the snapshot.
     *
     * @param snapshot_path  Path to the snapshot file to install.
     * @return               Parsed SnapshotMetadata, or nullopt on error.
     */
    [[nodiscard]] std::optional<SnapshotMetadata> install(
        const std::string& snapshot_path);

    /**
     * Install from an in-memory buffer (for testing or RPC transfer).
     *
     * @param data  Raw snapshot file contents.
     * @return      Parsed SnapshotMetadata, or nullopt on error.
     */
    [[nodiscard]] std::optional<SnapshotMetadata> install_from_buffer(
        const std::vector<uint8_t>& data);

    // -- Snapshot validation ----------------------------------------------

    /**
     * Validate a snapshot file's integrity without loading it.
     *
     * Checks:
     *   - File exists and is readable.
     *   - Magic bytes match kSnapshotFileMagic.
     *   - Version is supported.
     *   - Header CRC32C matches.
     *   - Body CRC32C matches (if present).
     *   - File size is within kMaxSnapshotFileSize.
     *
     * @param snapshot_path  Path to the snapshot file.
     * @return               true if the snapshot is valid.
     */
    [[nodiscard]] bool validate(const std::string& snapshot_path) const;

    /**
     * Quick-validate: check only the file header (magic, version, size).
     * Does not verify CRC or body integrity.  Used for directory scanning.
     *
     * @param snapshot_path  Path to the snapshot file.
     * @return               true if the header looks valid.
     */
    [[nodiscard]] bool validate_quick(const std::string& snapshot_path) const;

    // -- Inspection -------------------------------------------------------

    /**
     * Read the header of a snapshot file without parsing the body.
     *
     * @param snapshot_path  Path to the snapshot file.
     * @return               SnapshotHeader, or nullopt on error.
     */
    [[nodiscard]] std::optional<SnapshotHeader> read_header(
        const std::string& snapshot_path) const;

    // -- Snapshot listing -------------------------------------------------

    /**
     * List all valid snapshot files in the configured snapshot directory,
     * sorted by creation time (newest first).
     *
     * @return  Ordered list of snapshot file paths.
     */
    [[nodiscard]] std::vector<std::string> list_snapshots() const;

    /**
     * Find the latest valid snapshot file.
     *
     * @return  Path to the newest valid snapshot, or nullopt if none found.
     */
    [[nodiscard]] std::optional<std::string> find_latest() const;

    // -- Cleanup ----------------------------------------------------------

    /**
     * Delete all snapshot files except the `keep_count` most recent.
     *
     * @param keep_count  Number of newest snapshots to keep.
     * @return            Number of files deleted.
     */
    size_t prune_snapshots(size_t keep_count);

    /**
     * Delete a specific snapshot file.
     *
     * @param snapshot_path  Path to delete.
     * @return               true if the file was deleted successfully.
     */
    bool delete_snapshot(const std::string& snapshot_path);

private:
    // -- Internal helpers -------------------------------------------------

    /// Serialize the segment table to a binary buffer.
    [[nodiscard]] std::vector<uint8_t> serialize_segments(
        const std::vector<SegmentInfo>& segments) const;

    /// Deserialize segment entries from a binary buffer.
    [[nodiscard]] std::optional<std::vector<SegmentInfo>> deserialize_segments(
        const uint8_t* data, size_t size) const;

    /// Compute CRC32C of a buffer range.
    [[nodiscard]] static uint32_t compute_crc32c(
        const uint8_t* data, size_t len) noexcept;

    /// Compress a buffer with zstd. Returns compressed data or empty on error.
    [[nodiscard]] static std::vector<uint8_t> compress_zstd(
        const uint8_t* data, size_t len, int level);

    /// Decompress a zstd buffer. Returns decompressed data or empty on error.
    [[nodiscard]] static std::vector<uint8_t> decompress_zstd(
        const uint8_t* data, size_t len, size_t expected_size);

    /// Generate a unique snapshot filename.
    [[nodiscard]] static std::string make_filename(
        partition_id_t partition_id,
        offset_t snapshot_index,
        term_t term,
        timestamp_ms_t created_at);

    /// Current timestamp in ms.
    [[nodiscard]] static timestamp_ms_t now_ms() noexcept;

    /// Write raw bytes to file, fsync, close. Returns success bool.
    [[nodiscard]] static bool write_file_atomic(
        const std::string& path,
        const std::vector<uint8_t>& data);

    /// Read entire file into a vector.
    [[nodiscard]] static std::optional<std::vector<uint8_t>> read_file(
        const std::string& path);
};

} // namespace torrent
