#pragma once

/**
 * torrent-mq — Segment: Append-Only Log Segment
 *
 * The Segment is the fundamental on-disk storage unit. Each partition log
 * is an ordered sequence of segments. Only the last (active) segment accepts
 * writes; older segments are read-only and eligible for compaction/archival.
 *
 * File layout (.log file):
 *   Bytes 0-3:   Magic "TQSG" (0x47535154 LE)
 *   Bytes 4-5:   Format version (uint16_t)
 *   Bytes 6-7:   Header length (uint16_t, = 64)
 *   Bytes 8-11:  Header CRC32C (covers bytes 16..63)
 *   Bytes 12-15: Reserved
 *   Bytes 16-23: Base offset (int64_t)
 *   Bytes 24-31: Segment ID (uint64_t)
 *   Bytes 32-39: Created at (int64_t, ms epoch)
 *   Byte  40:    Compression type
 *   Byte  41:    Flags (bit 0 = is_sealed)
 *   Bytes 42-63: Reserved padding
 *   Bytes 64+:   Serialized RecordBatches
 *
 * Companion .index file: sequence of SparseIndexEntry records at
 * configurable byte intervals for O(log N) offset lookup.
 */

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <memory>
#include <optional>
#include <functional>
#include <fstream>
#include <chrono>
#include <atomic>
#include <mutex>

#include "torrent/common/types.h"
#include "torrent/storage/types.h"

namespace torrent {

// ============================================================================
// Segment format constants
// ============================================================================

inline constexpr uint32_t kSegmentMagic           = 0x47535154;  // "TQSG" LE
inline constexpr uint16_t kSegmentFormatVersion   = 1;
inline constexpr uint16_t kSegmentHeaderSize      = 64;
inline constexpr byte_count_t kSegmentInitialSize = 1048576;     // 1 MiB
inline constexpr byte_count_t kDefaultIndexInterval = 4096;
inline constexpr size_t kMaxInMemoryIndexEntries  = 100000;

// ============================================================================
// SegmentHeader — on-disk header (exactly 64 bytes)
// ============================================================================

/// On-disk header prepended to every segment file. CRC32C protects
/// fields from base_offset onward for integrity verification on open.
struct SegmentHeader {
    uint32_t          magic          = kSegmentMagic;
    uint16_t          format_version = kSegmentFormatVersion;
    uint16_t          header_size    = kSegmentHeaderSize;
    uint32_t          header_crc     = 0;           // CRC32C of bytes [16..64)
    offset_t          base_offset    = kInvalidOffset;
    uint64_t          segment_id     = 0;
    timestamp_ms_t    created_at     = 0;
    compression_type  compression    = compression_type::none;
    uint8_t           flags          = 0;           // bit 0 = is_sealed
    uint8_t           reserved[22]   = {0};

    [[nodiscard]] bool is_sealed() const noexcept { return (flags & 0x01) != 0; }
    void seal() noexcept { flags |= 0x01; }

    [[nodiscard]] bool is_valid() const noexcept {
        return magic == kSegmentMagic
            && format_version == kSegmentFormatVersion
            && header_size == kSegmentHeaderSize
            && base_offset >= 0;
    }
};

static_assert(sizeof(SegmentHeader) == kSegmentHeaderSize,
              "SegmentHeader must be exactly 64 bytes");

// ============================================================================
// SparseIndexEntry — offset-to-file-position mapping
// ============================================================================

/// Maps a logical offset to a physical file position. Built at configurable
/// byte intervals for O(log N) offset lookup without full file scan.
struct SparseIndexEntry {
    offset_t       offset        = kInvalidOffset;
    byte_count_t   file_position = 0;
    timestamp_ms_t timestamp     = 0;

    [[nodiscard]] bool operator<(const SparseIndexEntry& o) const noexcept {
        return offset < o.offset;
    }
    [[nodiscard]] bool operator<(offset_t off) const noexcept {
        return offset < off;
    }
};

inline bool operator<(offset_t off, const SparseIndexEntry& entry) noexcept {
    return off < entry.offset;
}

// ============================================================================
// SegmentConfig — construction parameters
// ============================================================================

struct SegmentConfig {
    std::string      file_path;
    std::string      index_file_path;            // defaults to file_path + ".index"
    offset_t         base_offset         = 0;
    uint64_t         segment_id          = 0;
    compression_type compression         = compression_type::none;
    byte_count_t     max_segment_bytes   = 1073741824;   // 1 GiB
    timestamp_ms_t   max_segment_age_ms  = 604800000;    // 7 days
    byte_count_t     index_interval_bytes = kDefaultIndexInterval;
    bool             preallocate         = false;
    bool             use_mmap            = true;
    bool             read_only           = false;
    bool             sync_on_append      = false;
};

// ============================================================================
// SegmentAppendResult — result of appending to a segment
// ============================================================================

struct SegmentAppendResult {
    offset_t       base_offset     = kInvalidOffset;
    byte_count_t   file_position   = 0;
    int32_t        batches_written = 0;
    int32_t        records_written = 0;
    timestamp_ms_t append_time     = 0;
    error_code     error           = error_code::none;
    std::string    error_message;

    [[nodiscard]] bool ok() const noexcept { return error == error_code::none; }
};

// ============================================================================
// SegmentReadResult — result of reading from a segment
// ============================================================================

struct SegmentReadResult {
    std::vector<RecordBatch> batches;
    byte_count_t   bytes_read   = 0;
    offset_t       next_offset  = kInvalidOffset;
    bool           is_truncated = false;
    error_code     error        = error_code::none;
    std::string    error_message;

    [[nodiscard]] bool ok()    const noexcept { return error == error_code::none; }
    [[nodiscard]] bool empty() const noexcept { return batches.empty(); }
};

// ============================================================================
// SegmentState — runtime lifecycle
// ============================================================================

enum class SegmentState : uint8_t {
    uninitialized = 0,
    active        = 1,   // accepting writes
    sealed        = 2,   // reads only, no writes
    closed        = 3,   // resources released
    corrupted     = 4,   // needs recovery
};

// ============================================================================
// Segment — append-only log segment
// ============================================================================

/**
 * A single append-only log segment backed by an on-disk file.
 *
 * Each segment stores a contiguous range of logical offsets. Writes are
 * strict append-only with CRC32C protection on both the segment header
 * and individual record batches. Reads use memory-mapped I/O (when
 * SegmentConfig::use_mmap is true) for zero-copy delivery to the network
 * layer, with a fallback to buffered fstream reads.
 *
 * The sparse index maps logical offsets to file positions, enabling
 * O(log N) lookups. Index entries are created at configurable byte
 * intervals (default: every 4 KiB written) and flushed to a companion
 * .index file for crash recovery.
 *
 * Lifecycle:
 *   1. construct(SegmentConfig) — define file paths, limits, options
 *   2. open()                   — create or load + validate the segment file
 *   3. append() / append_batch()— write RecordBatches (active only)
 *   4. read() / read_range()    — retrieve batches by offset
 *   5. flush() / fsync()        — ensure durability
 *   6. seal() / roll()          — stop writes, prepare successor segment
 *   7. close()                  — release fd, munmap, free resources
 *
 * Thread safety: all write-path and read-path methods are serialized
 * via an internal mutex. Const accessors that read atomic counters
 * are lock-free. The sparse index is protected by a separate mutex
 * to allow concurrent read-only index queries during writes.
 */
class Segment {
public:
    // -- Construction / Destruction ----------------------------------------

    explicit Segment(SegmentConfig config);
    Segment(Segment&& other) noexcept;
    Segment& operator=(Segment&& other) noexcept;
    Segment(const Segment&) = delete;
    Segment& operator=(const Segment&) = delete;
    ~Segment();

    // -- Lifecycle ---------------------------------------------------------

    /// Open the segment file (create if not exists, validate header if exists).
    result<void> open();

    /// Flush pending writes, release file handles and mmap regions. Idempotent.
    result<void> close();

    [[nodiscard]] bool is_open() const noexcept {
        auto s = state_.load(std::memory_order_acquire);
        return s == SegmentState::active || s == SegmentState::sealed;
    }

    [[nodiscard]] bool is_active() const noexcept {
        auto s = state_.load(std::memory_order_acquire);
        return s == SegmentState::active && !header_.is_sealed() && !config_.read_only;
    }

    [[nodiscard]] bool is_sealed() const noexcept {
        auto s = state_.load(std::memory_order_acquire);
        return s == SegmentState::sealed || header_.is_sealed();
    }

    [[nodiscard]] SegmentState state() const noexcept { return state_.load(std::memory_order_acquire); }

    // -- Write path --------------------------------------------------------

    /// Append a single RecordBatch to the end of this segment.
    ///
    /// The segment assigns the base_offset from its monotonically
    /// increasing next_offset counter, overwriting whatever offset the
    /// caller may have set. The batch is serialized to wire format,
    /// CRC-checked, and written to the file via pwrite. The sparse index
    /// is updated with the new offset→position mapping.
    ///
    /// If sync_on_append is configured, an fsync follows the write.
    ///
    /// Returns the assigned base offset and file position on success.
    /// Requires: is_active() == true.
    result<SegmentAppendResult> append(const RecordBatch& batch);

    /// Atomically append multiple RecordBatches in a single call.
    ///
    /// Offsets are assigned sequentially starting from next_offset.
    /// The operation acquires the segment mutex once for the entire
    /// batch, ensuring no interleaving with other writers. If any
    /// batch fails to write, the partial write may be recovered via
    /// truncate_to (truncate back to the pre-append next_offset).
    /// Index entries are flushed after all batches are written.
    result<SegmentAppendResult> append_batch(std::vector<RecordBatch> batches);

    /// Flush buffered writes to the operating system page cache.
    ///
    /// After flush, data is visible to mmap-based readers but is not
    /// yet guaranteed durable across a crash. Follow with fsync() for
    /// full durability. Typically called periodically or before sealing.
    result<void> flush();

    /// Force all written data to durable storage via fsync/fdatasync.
    ///
    /// Blocks until the filesystem commits pending writes to the
    /// underlying device. Call before acknowledging produce requests
    /// when acks=all or similar durability guarantees are required.
    /// Also flushes the sparse index to its companion .index file.
    result<void> fsync();

    // -- Read path ---------------------------------------------------------

    /// Read record batches starting from `start_offset` (inclusive).
    ///
    /// Uses find_position() to locate the file position via the sparse
    /// index (O(log N)), then scans forward parsing RecordBatches from
    /// either the mmap region or file stream. Each batch's CRC is
    /// validated; a corrupt batch triggers an immediate error return.
    ///
    /// Stops when `max_bytes` has been consumed or the end of the segment
    /// (next_offset) is reached. Sets is_truncated=true if the limit was
    /// hit before exhausting available data.
    ///
    /// @param start_offset  logical offset to begin reading from
    /// @param max_bytes     maximum total bytes to read (default 1 MiB)
    result<SegmentReadResult> read(offset_t start_offset,
                                   byte_count_t max_bytes = 1048576);

    /// Read all batches in the half-open range [start_offset, end_offset).
    ///
    /// Convenience wrapper that repeatedly calls read() until end_offset
    /// is reached. All batches are accumulated into a single result.
    result<SegmentReadResult> read_range(offset_t start_offset,
                                         offset_t end_offset,
                                         byte_count_t max_bytes = 104857600);

    /// Read exactly one RecordBatch starting at the given offset.
    ///
    /// Locates the batch via the sparse index, deserializes it, and
    /// returns it. Returns an error with error_code::corrupt_message if
    /// no batch begins at the requested offset (i.e., the offset falls
    /// mid-batch or beyond the segment's valid range).
    result<RecordBatch> read_at(offset_t offset);

    // -- Truncation --------------------------------------------------------

    /// Truncate the segment so that its valid data ends at `new_end_offset`.
    ///
    /// All data at offsets >= new_end_offset is discarded. The underlying
    /// file is truncated via ftruncate(), the mmap region is re-mapped to
    /// the new size, and sparse index entries at or beyond new_end_offset
    /// are pruned. The next_offset counter is reset to new_end_offset,
    /// allowing the segment to resume writes from that point.
    ///
    /// This is typically used during leader failover to truncate
    /// uncommitted data that was not replicated to the full ISR.
    ///
    /// @param new_end_offset  must satisfy base_offset <= new_end_offset <= next_offset
    result<void> truncate_to(offset_t new_end_offset);

    // -- Index operations --------------------------------------------------

    /// Binary-search sparse index for file position of largest offset <= target.
    [[nodiscard]] std::optional<byte_count_t> find_position(offset_t target_offset) const;

    /// Find first offset with timestamp >= ts using index timestamps.
    [[nodiscard]] result<offset_t> find_offset_by_timestamp(timestamp_ms_t ts) const;

    /// Rebuild sparse index by scanning the entire segment file (crash recovery).
    result<void> rebuild_index();

    /// Flush sparse index to companion .index file on disk.
    result<void> flush_index();

    // -- Rollover / Seal ---------------------------------------------------

    /// Seal this segment so it no longer accepts writes.
    ///
    /// Flushes all pending data and index entries to disk, sets the
    /// is_sealed flag in the on-disk header, and transitions the segment
    /// state to `sealed`. After sealing, the segment is read-only and
    /// can be safely archived or compacted.
    ///
    /// Idempotent — calling seal() on an already-sealed segment is a no-op.
    result<void> seal();

    /// Check whether this segment meets the criteria for rollover.
    ///
    /// Returns true when the segment file size exceeds max_segment_bytes
    /// or the segment age exceeds max_segment_age_ms. The caller should
    /// then call roll() to seal this segment and prepare its successor.
    [[nodiscard]] bool should_roll() const noexcept;

    /// Roll the segment: seal this one and produce a config for the next.
    ///
    /// After calling roll(), this segment is sealed and a new SegmentConfig
    /// is returned with base_offset set to this segment's next_offset and
    /// segment_id = this segment's id + 1. The caller constructs a new
    /// Segment from the returned config and begins appending to it.
    result<SegmentConfig> roll();

    // -- Compaction support ------------------------------------------------

    /// Perform key-level compaction over all records in this segment.
    ///
    /// Scans all record batches, grouping records by key. For each key
    /// with multiple values, the combiner function is invoked:
    ///   keep_existing(existing_record, newer_record) → bool
    /// When it returns true the existing record is kept; when false the
    /// newer record replaces it. This enables log compaction where only
    /// the latest value per key is retained.
    ///
    /// Returns the compacted set of Record objects. The caller is
    /// responsible for writing these into a new segment.
    result<std::vector<Record>> compact_records(
        std::function<bool(const Record& existing, const Record& newer)> keep_existing);

    // -- Accessors ---------------------------------------------------------

    [[nodiscard]] SegmentInfo info() const noexcept;

    [[nodiscard]] offset_t base_offset() const noexcept { return header_.base_offset; }

    [[nodiscard]] offset_t next_offset() const noexcept {
        return next_offset_.load(std::memory_order_acquire);
    }

    [[nodiscard]] byte_count_t file_size() const noexcept {
        return file_size_.load(std::memory_order_acquire);
    }

    [[nodiscard]] size_t index_entry_count() const noexcept {
        std::lock_guard<std::mutex> lock(index_mutex_);
        return sparse_index_.size();
    }

    [[nodiscard]] timestamp_ms_t created_at() const noexcept { return header_.created_at; }

    [[nodiscard]] timestamp_ms_t max_timestamp() const noexcept {
        return max_timestamp_.load(std::memory_order_acquire);
    }

    [[nodiscard]] offset_t approximate_record_count() const noexcept {
        return next_offset_.load(std::memory_order_acquire) - header_.base_offset;
    }

    [[nodiscard]] uint64_t segment_id()   const noexcept { return header_.segment_id; }
    [[nodiscard]] compression_type compression() const noexcept { return header_.compression; }
    [[nodiscard]] int fd()                const noexcept { return fd_; }

    // -- Metrics -----------------------------------------------------------

    [[nodiscard]] byte_count_t total_bytes_written()     const noexcept { return bytes_written_.load(std::memory_order_relaxed); }
    [[nodiscard]] int64_t      total_batches_appended()  const noexcept { return batches_appended_.load(std::memory_order_relaxed); }
    [[nodiscard]] int64_t      total_records_appended()  const noexcept { return records_appended_.load(std::memory_order_relaxed); }

private:
    // -- Internal helpers --------------------------------------------------

    /// Read the 64-byte header from the file, validate magic/version/CRC.
    result<void> read_header();

    /// Write the in-memory SegmentHeader to the beginning of the file.
    /// Used during open (initial write) and seal (to set the sealed flag).
    result<void> write_header();

    /// Compute CRC32C over header bytes [16..64) (base_offset through reserved).
    uint32_t compute_header_crc() const;

    /// Return true if the header CRC matches the expected value.
    bool validate_header_crc() const;

    /// Create a new file (if one doesn't exist) with optional preallocation.
    /// If the file already exists, open it and determine file_size_.
    result<void> prepare_file();

    /// Memory-map the file for zero-copy reads. On failure, falls back to
    /// file_stream_-based reads (the mmap member remains nullptr).
    result<void> map_file();

    /// Unmap the memory region. Safe to call when mapped_data_ is nullptr.
    void unmap_file();

    /// Grow the file by at least `additional_bytes`. If mmap is active,
    /// the mapping is extended to cover the new region. This is called
    /// automatically by append_bytes when needed.
    result<void> extend_file(byte_count_t additional_bytes);

    /// Write raw bytes to the file at the current end, returning the
    /// file position where the write began. Uses pwrite with O_APPEND
    /// semantics (writes at fd_ seek position = file_size_).
    result<byte_count_t> append_bytes(const void* data, size_t len);

    /// Convert a RecordBatch to the on-disk wire format.
    /// Returns an owning buffer and its exact byte size.
    std::pair<std::unique_ptr<char[]>, size_t> serialize_batch(const RecordBatch& batch) const;

    /// Parse a RecordBatch from a raw buffer, using expected_base_offset
    /// to validate the batch's integrity. Returns error on CRC mismatch.
    result<RecordBatch> deserialize_batch(const char* data, size_t len,
                                          offset_t expected_base_offset) const;

    /// Insert or update sparse index entries after a successful write.
    /// An entry is created every index_interval_bytes. The sparse_index_
    /// vector is kept sorted by offset.
    void update_sparse_index(offset_t base_offset, byte_count_t file_position,
                             timestamp_ms_t timestamp, int32_t record_count);

    /// Remove all sparse index entries with offset >= from_offset.
    /// Used by truncate_to to keep the index consistent with file data.
    void prune_sparse_index(offset_t from_offset);

    /// Load sparse index entries from the companion .index file on disk.
    /// Called during open() to restore index state after a clean shutdown.
    result<void> load_index();

    /// Resolve the companion index file path. If config_.index_file_path is
    /// non-empty, returns it; otherwise appends ".index" to config_.file_path.
    std::string index_file_path() const;

    /// Transition the segment to corrupted state and log the reason.
    /// Subsequent operations on a corrupted segment return errors until
    /// the segment is closed and reopened (or recovered).
    void mark_corrupted(const std::string& reason);

    // -- Members -----------------------------------------------------------

    SegmentConfig                     config_;
    SegmentHeader                     header_;
    std::atomic<SegmentState>         state_            = SegmentState::uninitialized;
    std::atomic<offset_t>             next_offset_      {kInvalidOffset};
    std::atomic<byte_count_t>         file_size_        {0};
    std::atomic<timestamp_ms_t>       max_timestamp_    {0};
    int                               fd_               = -1;
    std::fstream                      file_stream_;
    void*                             mapped_data_      = nullptr;
    size_t                            mapped_size_      = 0;
    std::vector<SparseIndexEntry>     sparse_index_;
    bool                              index_dirty_      = false;
    mutable std::mutex                mutex_;
    mutable std::mutex                index_mutex_;
    std::atomic<byte_count_t>         bytes_written_    {0};
    std::atomic<int64_t>              batches_appended_ {0};
    std::atomic<int64_t>              records_appended_ {0};
};

} // namespace torrent
