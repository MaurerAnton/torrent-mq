#pragma once

/**
 * torrent-mq — LogManager: Partition Replica Log Lifecycle
 *
 * LogManager orchestrates all log segments for a single partition replica:
 * creation, append, read, rollover, compaction, retention, recovery, tiered
 * storage offload, and Raft snapshot integration.  Only the last (active)
 * segment accepts writes; sealed predecessors are read-only and eligible
 * for background maintenance (compaction, retention, tiered archival).
 *
 * Thread safety: segment_list_mutex_ (shared) serialises segment list
 * mutations; active_segment_mutex_ serialises writes/rollover; watermarks
 * are lock-free atomics.  Recovery rebuilds indexes from on-disk data during
 * open().
 *
 * Raft integration:
 *   - truncate_to(new_end_offset) — discards uncommitted data after leader
 *     change (Raft log truncation).
 *   - create_snapshot() / install_snapshot() — enable Raft snapshotting.
 *
 * Usage:
 *   Leader:   construct → open() → append()/read() → compact()/retention_check()
 *             → archive_to_tiered_storage() → close()
 *   Follower: construct → open() → truncate_to(leader_hw) → append()
 *   Recovery: construct(rebuild_indexes_on_open=true) → open() → resume
 */

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <atomic>
#include <functional>
#include <optional>

#include "torrent/common/types.h"
#include "torrent/storage/types.h"
#include "torrent/storage/segment.h"

namespace torrent {

// ============================================================================
// LogManagerConfig — construction-time parameters
// ============================================================================

/// Immutable parameters for LogManager construction.  All tuning knobs
/// governing segment lifecycle, crash recovery policy, and tiered-storage
/// integration are gathered here.  Once passed to the constructor they
/// cannot be changed — topic-level config updates must go through the
/// cluster controller and result in a new LogManager.
struct LogManagerConfig {
    /// Owning partition id.
    partition_id_t partition_id              = 0;

    /// Root directory where segment .log and .index files reside.
    /// Files are named <segment_id>.log and <segment_id>.index.
    std::string    data_directory;

    /// Topic-level configuration (compression, retention window, segment
    /// size limit, flush policy, compaction parameters, etc.).
    TopicConfig    topic_config;

    /// Maximum number of segments kept in the local hot tier.  When the
    /// count exceeds this value, the oldest sealed segments become
    /// candidates for tiered storage offload.  0 = keep everything local.
    int32_t        max_hot_segments          = 0;

    /// Minimum number of segments to retain locally even when the hot
    /// tier is "full."  Prevents aggressive offload of recently-written
    /// data that consumers may still be reading.
    int32_t        min_hot_segments          = 2;

    /// When true, open() performs a full sparse-index rebuild on every
    /// discovered segment (conservative crash-recovery path).  When false,
    /// indexes are loaded from companion .index files if present and valid.
    bool           rebuild_indexes_on_open   = false;

    /// When true, corrupt segments are renamed to <name>.corrupt instead
    /// of being deleted.  Allows operator inspection and manual recovery.
    bool           quarantine_corrupt        = true;

    /// Tiered-storage backend endpoint (S3 bucket / prefix, e.g.
    /// "s3://my-bucket/torrent/tiered/").  Empty string disables
    /// tiered storage entirely — archive_to_tiered_storage() is a no-op.
    std::string    tiered_storage_endpoint;

    /// Tiered-storage access key (leave empty for instance-role / IAM).
    std::string    tiered_storage_access_key;

    /// Tiered-storage secret key.
    std::string    tiered_storage_secret_key;

    /// Number of background threads for compaction and tiered uploads.
    /// Each worker can handle one compaction or upload task at a time.
    int32_t        background_workers        = 2;
};

// ============================================================================
// SnapshotMetadata — Raft snapshot payload
// ============================================================================

/// Minimal state required for the Raft layer to persist and restore a
/// partition replica's log via snapshot.  Contains per-segment metadata
/// and all watermarks; actual segment byte data lives in the segment
/// files on disk or in tiered storage.  A CRC32C checksum protects the
/// entire metadata payload against corruption during serialization.
struct SnapshotMetadata {
    /// Partition this snapshot belongs to.
    partition_id_t           partition_id       = 0;

    /// Monotonically increasing snapshot index in the Raft log.
    offset_t                 snapshot_index     = kInvalidOffset;

    /// Raft term at the time this snapshot was created.
    term_t                   term               = 0;

    /// Per-segment metadata, ordered by ascending base_offset.
    std::vector<SegmentInfo> segments;

    /// High watermark at snapshot time.
    offset_t                 high_watermark     = kInvalidOffset;

    /// Log-start offset (oldest offset still available) at snapshot time.
    offset_t                 log_start_offset   = kInvalidOffset;

    /// Last stable offset at snapshot time.
    offset_t                 last_stable_offset = kInvalidOffset;

    /// Wall-clock time (milliseconds since epoch) the snapshot was taken.
    timestamp_ms_t           created_at         = 0;

    /// CRC32C computed over all fields in this struct, for integrity
    /// verification when the snapshot is loaded.
    uint32_t                 crc                = 0;
};

// ============================================================================
// CompactionResult
// ============================================================================

/// Summarises what was accomplished during a single compaction cycle.
/// Returned by LogManager::compact() for use in metrics, logging,
/// and admin API responses.
struct CompactionResult {
    /// Number of sealed segments that were fully processed (compacted
    /// into new segments with only the latest value per key).
    int32_t       segments_compacted  = 0;

    /// Number of segments that were merged together (typically 2–4 old
    /// segments combined into a single new compacted segment).
    int32_t       segments_merged     = 0;

    /// Total records deleted: duplicate keys (keep latest) and
    /// tombstone records older than delete_retention_ms.
    int64_t       records_deleted     = 0;

    /// Total bytes freed from disk after old segments were deleted.
    byte_count_t  bytes_reclaimed     = 0;

    /// Wall-clock duration of the compaction run (milliseconds).
    duration_ms_t duration_ms         = 0;

    /// True when the compaction was aborted early (e.g. shutdown signal
    /// received, or the active segment changed during compaction).
    bool          aborted             = false;
};

// ============================================================================
// RetentionResult
// ============================================================================

/// Summarises what the retention policy deleted during a cleanup cycle.
/// Returned by LogManager::retention_check() and LogManager::cleanup().
struct RetentionResult {
    /// Number of segments removed because their max_timestamp fell
    /// outside the retention window (now - retention_ms).
    int32_t       segments_deleted_by_time = 0;

    /// Number of segments removed because total partition bytes
    /// exceeded retention_bytes (size-based cleanup).
    int32_t       segments_deleted_by_size = 0;

    /// Total bytes freed from local disk by this cleanup run.
    byte_count_t  bytes_deleted            = 0;

    /// The oldest offset still available after this cycle — any consumer
    /// requesting an offset below this value will get OFFSET_OUT_OF_RANGE.
    offset_t      new_log_start_offset     = kInvalidOffset;

    /// Wall-clock duration of the retention cycle (milliseconds).
    duration_ms_t duration_ms              = 0;
};

// ============================================================================
// LogManager — partition replica log orchestrator
// ============================================================================

/**
 * Manages the complete lifecycle of a partition replica's on-disk log.
 * Owns all Segment objects, enforces rollover/retention policies,
 * dispatches reads/writes to the correct segment, and coordinates
 * background maintenance (compaction, tiered offload).
 *
 * Move-only (non-copyable).  Destructor calls close() (idempotent).
 */
class LogManager {
public:
    // -- Construction / Destruction ----------------------------------------

    explicit LogManager(LogManagerConfig config);
    LogManager(LogManager&& other) noexcept;
    LogManager& operator=(LogManager&& other) noexcept;
    LogManager(const LogManager&) = delete;
    LogManager& operator=(const LogManager&) = delete;
    ~LogManager();

    // -- Lifecycle ---------------------------------------------------------

    /// Open (or create) the partition log for this replica.
    ///
    /// Scans `config_.data_directory` for existing segment *.log files,
    /// validates their headers and CRC checksums, and loads or rebuilds
    /// sparse indexes depending on `config_.rebuild_indexes_on_open`.
    /// Corrupt segments are either truncated at the last valid offset or
    /// quarantined (renamed to .corrupt) based on `config_.quarantine_corrupt`.
    ///
    /// If no segments are found, a fresh active segment is created at
    /// offset 0.  After open() returns successfully the LogManager is
    /// ready for reads and writes.
    ///
    /// Must be called exactly once before any other method (close() is
    /// always safe to call, even on an already-closed LogManager).
    ///
    /// @return  error_code::none on success; storage-layer codes
    ///          (segment_corrupted, storage_unavailable, recovery_needed)
    ///          on failure.
    result<void> open();

    /// Flush all segments, release files/mmaps/indexes. Idempotent.
    result<void> close();

    [[nodiscard]] bool is_open() const noexcept {
        return is_open_.load(std::memory_order_acquire);
    }

    // -- Write path --------------------------------------------------------

    /// Append a RecordBatch to the active segment.  Transparently rolls
    /// if this append would exceed segment_bytes.  The batch's base_offset
    /// is overwritten with the actual assigned offset.
    /// @param expected_base_offset  If >= 0, fail if assigned offset differs
    ///                              (for idempotent / transactional producers).
    result<LogAppendResult> append(const RecordBatch& batch,
                                   offset_t expected_base_offset = kInvalidOffset);

    /// Append multiple batches atomically under a single lock acquisition.
    /// If any batch fails the segment may hold a partial write — recover
    /// via truncate_to().
    result<LogAppendResult> append_batch(std::vector<RecordBatch> batches);

    // -- Read path ---------------------------------------------------------

    /// Fetch records from start_offset (inclusive).  Binary-searches the
    /// segment list, delegates to Segment::read/read_range, and returns a
    /// FetchResult populated with current watermarks.
    /// @param end_offset  Exclusive upper bound (optional).
    /// @param isolation   read_committed respects last_stable_offset.
    result<FetchResult> read(offset_t start_offset,
                             byte_count_t max_bytes = 1048576,
                             std::optional<offset_t> end_offset = std::nullopt,
                             isolation_level isolation = isolation_level::read_uncommitted);

    /// Read a single RecordBatch at the exact given offset.
    /// Returns error if no batch begins at `offset`.
    result<RecordBatch> read_at(offset_t offset);

    // -- Truncation (Raft log consistency) ---------------------------------

    /// Truncate the partition log so that its valid data ends at
    /// `new_end_offset`.
    ///
    /// All data at offsets >= new_end_offset is discarded permanently.
    /// Segments whose base_offset >= new_end_offset are closed and their
    /// files deleted from disk.  The segment that contains new_end_offset
    /// (i.e. base_offset <= new_end_offset < next_offset) is truncated
    /// in-place via Segment::truncate_to() and becomes the new active
    /// (writable) segment, accepting subsequent appends.
    ///
    /// This is the key mechanism for Raft log consistency: on leader
    /// failover, uncommitted data that was not replicated to the full ISR
    /// is truncated so the follower's log matches the new leader exactly.
    /// It is also used to recover from a failed append_batch() that left
    /// a partial write in the active segment.
    ///
    /// @param new_end_offset  Must satisfy log_start_offset <= new_end_offset
    ///                        <= log_end_offset.  Values outside this range
    ///                        return error_code::offset_out_of_range.
    result<void> truncate_to(offset_t new_end_offset);

    // -- Watermarks --------------------------------------------------------

    /// Advance high watermark (highest offset replicated to full ISR).
    /// Called by the Raft layer after ISR acknowledgement.
    void update_high_watermark(offset_t new_hw) noexcept;

    /// Advance last stable offset (highest offset with committed
    /// transactions).  Limits read_committed consumers.
    void update_last_stable_offset(offset_t new_lso) noexcept;

    [[nodiscard]] offset_t get_high_watermark() const noexcept {
        return high_watermark_.load(std::memory_order_acquire);
    }
    [[nodiscard]] offset_t get_last_stable_offset() const noexcept {
        return last_stable_offset_.load(std::memory_order_acquire);
    }
    [[nodiscard]] offset_t get_log_start_offset() const noexcept;
    [[nodiscard]] offset_t get_log_end_offset() const noexcept;

    // -- Durability --------------------------------------------------------

    /// Flush active-segment buffered writes to OS page cache.  Data becomes
    /// visible to mmap readers but is not crash-safe.  Follow with sync().
    result<void> flush();

    /// Force all written data to durable storage (fsync/fdatasync), plus
    /// flush sparse indexes.  Call before acknowledging produce requests
    /// requiring durability (e.g. acks = all).
    result<void> sync();

    // -- Segment inspection ------------------------------------------------

    /// Ordered metadata for every segment (ascending base_offset).
    [[nodiscard]] std::vector<SegmentInfo> list_segments() const;

    /// Pointer to the active (writable) segment, or nullptr.  Lifetime is
    /// tied to the segment_list_mutex_ hold duration.
    [[nodiscard]] const Segment* active_segment() const;

    [[nodiscard]] int32_t       segment_count()    const noexcept { return segment_count_.load(std::memory_order_relaxed); }
    [[nodiscard]] byte_count_t  total_disk_bytes() const noexcept { return total_disk_bytes_.load(std::memory_order_relaxed); }

    // -- Compaction --------------------------------------------------------

    /// Run a compaction cycle over eligible sealed segments.
    ///
    /// Eligibility criteria for a segment to be compacted:
    ///   - Sealed (not the active/writable segment).
    ///   - Its max_timestamp is older than topic_config.min_compaction_lag_ms
    ///     (avoids compacting data that may still be actively read).
    ///   - Not already archived to tiered storage.
    ///
    /// For each eligible segment, a new compacted segment is created that
    /// contains only the latest value per message key.  Tombstone records
    /// (empty-value messages) older than topic_config.delete_retention_ms
    /// are removed entirely, permanently deleting those keys.  The original
    /// segments are deleted from disk after the compacted replacement is
    /// sealed and flushed.
    ///
    /// Compaction respects the configured cleanup_policy: it runs only
    /// when policy is compact_only or compact_and_delete.
    ///
    /// @param keep_existing  Optional combining function: return true to
    ///                       keep the existing record, false to replace it
    ///                       with the newer record.  nullptr (default) means
    ///                       keep the latest record per key (standard log
    ///                       compaction semantics).
    /// @return  CompactionResult with detailed metrics on what was achieved.
    result<CompactionResult> compact(
        std::function<bool(const Record& existing, const Record& newer)> keep_existing = nullptr);

    // -- Retention ---------------------------------------------------------

    /// Evaluate time-based and size-based retention from TopicConfig.
    /// Deletes eligible segments oldest-first; never deletes the active
    /// segment.  Advances log_start_offset.  Called on a periodic timer.
    result<RetentionResult> retention_check();

    /// Convenience passthrough: delete old segments via retention policy.
    result<RetentionResult> cleanup() { return retention_check(); }

    // -- Tiered storage ----------------------------------------------------

    /// Result of a tiered-storage archival run.
    struct TieredArchiveResult {
        int32_t      segments_archived = 0;
        byte_count_t bytes_freed       = 0;
        error_code   error             = error_code::none;
        std::string  error_message;
    };

    /// Archive eligible cold sealed segments to S3/compatible storage.
    ///
    /// Eligibility: sealed, not active, hot segment count > max_hot_segments
    /// (keeping at least min_hot_segments local).  After upload the local
    /// file is deleted; future reads fetch from remote on demand (with a
    /// local LRU cache to avoid repeated downloads of the same byte ranges).
    ///
    /// This is typically called on a periodic background timer and is a
    /// no-op when tiered_storage_endpoint is empty.
    result<TieredArchiveResult> archive_to_tiered_storage();

    [[nodiscard]] bool tiered_storage_enabled() const noexcept {
        return !config_.tiered_storage_endpoint.empty();
    }

    // -- Raft snapshot support ---------------------------------------------

    /// Create a consistent snapshot of the current log state for Raft.
    ///
    /// Gathers SegmentInfo for every segment in order, captures the current
    /// high_watermark, log_start_offset, and last_stable_offset, and
    /// computes a CRC32C checksum over the entire metadata payload.  The
    /// Raft layer persists this snapshot to durable storage and uses it to
    /// avoid replaying the full Raft log from index 0, dramatically reducing
    /// catch-up time for new or recovering followers.
    ///
    /// The snapshot is taken under a shared lock on segment_list_mutex_,
    /// guaranteeing a consistent point-in-time view of the segment list
    /// and all watermarks.
    ///
    /// @param snapshot_index  Raft log index that this snapshot covers
    ///                        (all log entries up to this index are included).
    /// @param term            Raft term at the time the snapshot is taken.
    /// @return  SnapshotMetadata with CRC, ready for the Raft layer to persist.
    result<SnapshotMetadata> create_snapshot(offset_t snapshot_index, term_t term);

    /// Restore the partition log from a Raft snapshot.
    ///
    /// Closes all currently-open segments, deletes their local files from
    /// disk, and reconstructs the partition's segment list from the metadata
    /// described in `snapshot`.  Watermarks are set to the snapshot values.
    ///
    /// After install_snapshot() returns, the LogManager is ready for normal
    /// append() calls to catch up on Raft log entries committed after the
    /// snapshot index.  This enables fast node bootstrap without replaying
    /// the entire Raft log from index 0.
    ///
    /// @param snapshot  The SnapshotMetadata describing the state to restore.
    ///                  The CRC field is validated before any state is modified.
    result<void> install_snapshot(const SnapshotMetadata& snapshot);

    // -- Accessors ---------------------------------------------------------

    [[nodiscard]] const LogManagerConfig& config()       const noexcept { return config_; }
    [[nodiscard]] partition_id_t          partition_id() const noexcept { return config_.partition_id; }

private:
    // -- Internal helpers --------------------------------------------------

    /// Scan data_directory for *.log files, parse headers to extract
    /// base_offset, sort ascending, return ordered file paths.
    result<std::vector<std::string>> discover_segments() const;

    /// Create and open a new segment at base_offset with given segment_id.
    /// Returns the opened Segment and its SegmentInfo.
    result<std::pair<std::unique_ptr<Segment>, SegmentInfo>>
        create_segment(offset_t base_offset, uint64_t segment_id);

    /// Seal the currently-active segment, create a successor segment at
    /// the current log_end_offset, append the successor to segments_,
    /// and update active_index_ to point at the new last element.
    ///
    /// Must be called with active_segment_mutex_ held.  The old active
    /// segment is sealed (flushed + fsynced + header sealed flag set),
    /// and the new segment is opened and ready for writes before this
    /// method returns.
    result<void> roll_active_segment();

    /// Binary-search segments_ for the segment owning `offset`.
    /// Returns index or std::nullopt if not found.
    [[nodiscard]] std::optional<size_t> find_segment_index(offset_t offset) const;

    /// Close segment at index, remove its .log/.index files, erase from
    /// segments_.  Advances log_start_offset if the deleted segment was
    /// first in the list.
    result<void> delete_segment(size_t index);

    /// Rebuild sparse indexes for all segments by scanning data files
    /// from beginning to end.  Used during crash recovery.
    result<void> rebuild_all_indexes();

    /// Recompute total_disk_bytes_ after a segment-list mutation.
    void recompute_disk_bytes();

    /// Clear segment list, zero counters, release resources.  Called by
    /// close() and install_snapshot().
    void reset_state();

    // -- Members -----------------------------------------------------------

    LogManagerConfig                        config_;

    /// Ordered list of segments (ascending base_offset).  segments_[0] is
    /// the oldest, segments_.back() is the active segment.
    std::vector<std::unique_ptr<Segment>>   segments_;
    mutable std::shared_mutex               segment_list_mutex_;    ///< Serialises segment CRUD

    /// Serialises writes to the active segment and rollover.
    mutable std::mutex                      active_segment_mutex_;

    /// Watermarks — lock-free atomics for hot read path.
    std::atomic<offset_t>                   high_watermark_     {kInvalidOffset};
    std::atomic<offset_t>                   last_stable_offset_ {kInvalidOffset};
    std::atomic<offset_t>                   log_start_offset_   {kInvalidOffset};

    /// Derived counters kept consistent with segment list.
    std::atomic<int32_t>                    segment_count_      {0};
    std::atomic<byte_count_t>               total_disk_bytes_   {0};

    std::atomic<bool>                       is_open_            {false};
    uint64_t                                next_segment_id_    = 0;     ///< Monotonic segment id
    size_t                                  active_index_       = 0;     ///< Index of active segment in segments_
    std::atomic<uint64_t>                   generation_         {0};     ///< Incremented on list mutation
    std::atomic<bool>                       dirty_              {false}; ///< Unsaved writes since last sync

    /// Background thread pool for async compaction and tiered uploads.
    /// Forward-declared; full definition in the .cc implementation file.
    struct BackgroundWorker;
    std::unique_ptr<BackgroundWorker>       background_;
};

} // namespace torrent
