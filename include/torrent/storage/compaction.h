#pragma once

/**
 * torrent-mq — Log Compaction Engine
 *
 * Full log compaction pipeline for key-based deduplication of sealed
 * log segments.  The engine consists of five cooperating components:
 *
 *   Compactor          — Core key→latest-record dedup with combiners
 *   CompactionStrategy — Segment eligibility evaluation & merge planning
 *   KeyedRecordIndex   — key→offset O(1) lookup with LRU eviction
 *   CompactionExecutor — Atomic segment replacement with crash recovery
 *   SegmentMerge       — Multi-segment merge with cross-segment dedup
 *
 * Design principles:
 *   - Tombstones (null-value records) are retained until they age past
 *     delete_retention_ms, then they are permanently purged.
 *   - All file mutations are atomic: write temp, fsync, rename.
 *   - LRU eviction bounds memory usage for keyspaces that exceed RAM.
 *   - Metrics are captured at every stage for Prometheus export.
 */

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <functional>
#include <memory>
#include <unordered_map>
#include <list>
#include <optional>
#include <chrono>
#include <mutex>
#include <atomic>

#include "torrent/common/types.h"
#include "torrent/storage/types.h"

namespace torrent {

class Segment;

// ============================================================================
// CompactorConfig — tuning knobs for the compaction engine
// ============================================================================

/// Parameters controlling compaction behaviour.  Defaults mirror Kafka's
/// log compaction settings where applicable.
struct CompactorConfig {
    /// Minimum time (ms) a segment must be sealed before it can be
    /// compacted.  Prevents compacting data consumers are still reading.
    duration_ms_t min_compaction_lag_ms   = 0;

    /// Maximum time (ms) a segment may remain uncompacted.  Segments
    /// older than this are forced into the next compaction cycle
    /// regardless of dirty ratio.  0 = disabled.
    duration_ms_t max_compaction_lag_ms   = 0;

    /// Minimum ratio of duplicate keys to total keys before a segment
    /// is considered "dirty enough" to compact.  Range: [0.0, 1.0].
    /// 0.0 = compact everything; 1.0 = never compact by ratio alone.
    double       min_cleanable_dirty_ratio = 0.5;

    /// Maximum number of keys tracked in the KeyedRecordIndex LRU cache.
    /// After this many unique keys the oldest entries are evicted.
    size_t       max_key_index_entries      = 1'000'000;

    /// Maximum number of segment files that may be merged in a single
    /// merge-compaction operation (merge adjacent small segments before
    /// deduplicating).
    int32_t      max_merge_segments         = 8;

    /// During compaction, size threshold (bytes) below which adjacent
    /// sealed segments are merged instead of compacted individually.
    byte_count_t small_segment_threshold    = 16 * 1024 * 1024;  // 16 MiB

    /// Delete retention: tombstone records (empty value) are retained
    /// for at least this long after their timestamp before being purged.
    duration_ms_t delete_retention_ms       = 86'400'000;        // 1 day

    /// Maximum records packed into a single output RecordBatch.  Batches
    /// exceeding this are split into multiple batches.
    int32_t      max_records_per_batch      = 50'000;

    /// When true, the executor syncs extra aggressively (double fsync
    /// on temp file before rename).  Slower but safer on edge-filesystems.
    bool         paranoid_sync              = false;

    /// Directory for temporary compaction files.  Must be on the same
    /// filesystem as the target segment to guarantee atomic rename.
    std::string  temp_directory;

    /// Number of compaction workers (for parallel segment compaction).
    int32_t      worker_threads             = 1;
};

// ============================================================================
// CompactionMetrics — per-operation and cumulative counters
// ============================================================================

/// Detailed metrics produced by a single compaction or merge operation.
/// Suitable for feeding into Prometheus / Grafana via the metrics subsystem.
struct CompactionMetrics {
    /// Bytes read from source segment(s).
    byte_count_t   bytes_in            = 0;

    /// Bytes written to the compacted / merged output.
    byte_count_t   bytes_out           = 0;

    /// Number of records in the source(s) before compaction.
    int64_t        records_in          = 0;

    /// Number of records remaining after deduplication.
    int64_t        records_out         = 0;

    /// Number of tombstone records purged (aged past delete_retention_ms).
    int64_t        tombstones_purged   = 0;

    /// Number of duplicate keys removed.
    int64_t        duplicates_removed  = 0;

    /// Number of unique keys found during compaction.
    int64_t        unique_keys         = 0;

    /// Wall-clock duration of the operation (milliseconds).
    duration_ms_t  duration_ms         = 0;

    /// Key evictions from the LRU index during this operation.
    int64_t        lru_evictions       = 0;

    /// Error code — none on success.
    error_code     error               = error_code::none;

    /// Human-readable error description.
    std::string    error_message;

    [[nodiscard]] bool ok() const noexcept { return error == error_code::none; }

    [[nodiscard]] double compaction_ratio() const noexcept {
        if (records_in == 0) return 0.0;
        return 1.0 - (static_cast<double>(records_out) / static_cast<double>(records_in));
    }
};

// ============================================================================
// CompactionPlan — result of running the compaction strategy
// ============================================================================

/// Describes what the strategy decided: which segments to compact, which
/// to merge, and in what order.  Consumed by the CompactionExecutor.
struct CompactionPlan {
    /// Strategy: compact a single segment in isolation.
    struct SingleTask {
        uint64_t segment_id;       ///< Segment to compact.
        bool     urgent = false;   ///< True when max_compaction_lag_ms exceeded.
    };

    /// Strategy: merge several adjacent segments before compaction.
    struct MergeTask {
        std::vector<uint64_t> segment_ids;   ///< Ordered list of segments to merge.
        bool urgent = false;
    };

    /// Ordered tasks — the executor processes them sequentially (single
    /// worker) or distributes them across a thread pool.
    std::vector<SingleTask> single_tasks;
    std::vector<MergeTask>  merge_tasks;

    /// Number of segments that could not be compacted (e.g. still active,
    /// locked, or error'd).
    int32_t                 skipped_count = 0;

    [[nodiscard]] size_t total_tasks() const noexcept {
        return single_tasks.size() + merge_tasks.size();
    }

    [[nodiscard]] bool empty() const noexcept {
        return single_tasks.empty() && merge_tasks.empty();
    }
};

// ============================================================================
// KeyedRecordIndex — in-memory key→offset index with LRU eviction
// ============================================================================

/**
 * Provides O(1) duplicate-key detection during compaction.
 *
 * Internally maps `std::string key` → `Record*` (or offset).  When the
 * number of tracked entries exceeds `max_entries`, the least-recently-used
 * entry is evicted.  Evicted keys are treated as new (their duplicate
 * status is lost), which is *safe*: at worst we keep an extra copy of the
 * record.  This bounds memory while guaranteeing correctness.
 *
 * Thread-safe: all public methods acquire an internal mutex.
 */
class KeyedRecordIndex {
public:
    /// A record pointer with its associated key.  Used during merging.
    struct IndexEntry {
        std::string   key;
        offset_t      latest_offset = kInvalidOffset;
        timestamp_ms_t timestamp     = 0;
        bool           is_tombstone  = false;
    };

    explicit KeyedRecordIndex(size_t max_entries = 1'000'000);
    ~KeyedRecordIndex();

    KeyedRecordIndex(const KeyedRecordIndex&) = delete;
    KeyedRecordIndex& operator=(const KeyedRecordIndex&) = delete;
    KeyedRecordIndex(KeyedRecordIndex&&) noexcept = default;
    KeyedRecordIndex& operator=(KeyedRecordIndex&&) noexcept = default;

    // -- Lookup / insert ----------------------------------------------------

    /// Look up the latest offset for `key`.  Returns nullptr if not found.
    [[nodiscard]] const IndexEntry* find(std::string_view key) const;

    /// Insert or update the entry for `key`.  If the key already exists,
    /// the offset is updated only when `new_offset > existing.latest_offset`.
    /// Touches the LRU — the entry is moved to the front of the LRU list.
    void insert_or_update(std::string_view key, offset_t offset,
                          timestamp_ms_t timestamp, bool is_tombstone);

    /// Remove a key from the index entirely (used when tombstone is purged).
    void remove(std::string_view key);

    /// Evict the least-recently-used entry.  Returns true if an entry was
    /// evicted, false if the index was already empty.
    bool evict_lru();

    // -- Bulk operations ----------------------------------------------------

    /// Clear all entries.
    void clear();

    /// Reserve internal hash-table capacity (to avoid rehashing).
    void reserve(size_t capacity);

    // -- Accessors ----------------------------------------------------------

    [[nodiscard]] size_t size()      const noexcept;
    [[nodiscard]] size_t max_size()  const noexcept { return max_entries_; }
    [[nodiscard]] bool   empty()     const noexcept;
    [[nodiscard]] int64_t evictions() const noexcept { return evictions_.load(std::memory_order_relaxed); }

private:
    /// Internal node for the LRU list.
    struct LruNode {
        std::string key;
        offset_t    latest_offset;
        timestamp_ms_t timestamp;
        bool        is_tombstone;
    };

    using LruList   = std::list<LruNode>;
    using MapIter   = LruList::iterator;
    using HashMap   = std::unordered_map<std::string, MapIter>;

    size_t                  max_entries_;
    LruList                 lru_list_;
    HashMap                 map_;
    mutable std::mutex      mutex_;
    std::atomic<int64_t>    evictions_{0};
};

// ============================================================================
// Compactor — key-based deduplication engine
// ============================================================================

/**
 * Core compaction algorithm.
 *
 * Given a vector of RecordBatches, groups all records by key, keeps only
 * the record with the highest offset for each key, and produces new
 * compacted RecordBatches.  An optional combiner function allows
 * custom merge logic (e.g. numeric aggregation).
 *
 * Tombstones (records with empty values) are retained for eventual
 * deletion when they age past delete_retention_ms.
 *
 * Usage:
 *   Compactor compactor(cfg);
 *   auto result = compactor.compact(input_batches);
 *   // result.compacted — new, deduplicated RecordBatches
 *   // result.metrics  — per-operation statistics
 */
class Compactor {
public:
    /// Result of a single compact() call.
    struct CompactResult {
        std::vector<RecordBatch> compacted;
        CompactionMetrics        metrics;
    };

    /// Combiner signature: given an existing record and a newer candidate,
    /// return true to keep the existing record, false to replace it with
    /// the newer one.  nullptr = default: keep the record with the
    /// highest offset (latest write wins).
    using CombinerFn = std::function<bool(const Record& existing,
                                          const Record& newer)>;

    explicit Compactor(CompactorConfig config);
    ~Compactor();

    Compactor(const Compactor&) = delete;
    Compactor& operator=(const Compactor&) = delete;
    Compactor(Compactor&&) noexcept = default;
    Compactor& operator=(Compactor&&) noexcept = default;

    /// Compact a collection of RecordBatches.  The input is consumed via
    /// move — the caller should not reuse it.
    ///
    /// @param batches    Input batches to compact.
    /// @param combiner   Optional custom merge function.  When nullptr,
    ///                   the record with the highest offset is kept.
    /// @return           CompactResult with deduplicated batches and metrics.
    CompactResult compact(std::vector<RecordBatch> batches,
                          CombinerFn combiner = nullptr);

    /// Reset internal state (key index, etc.) for reuse.
    void reset();

    /// Access the underlying key index for pre-population during merges.
    [[nodiscard]] KeyedRecordIndex& index() noexcept { return index_; }
    [[nodiscard]] const KeyedRecordIndex& index() const noexcept { return index_; }

    /// Access the configuration.
    [[nodiscard]] const CompactorConfig& config() const noexcept { return config_; }

private:
    /// Build output RecordBatches from the compacted index, respecting
    /// max_records_per_batch and maintaining approximate offset ordering.
    std::vector<RecordBatch> build_output_batches(std::vector<KeyedRecordIndex::IndexEntry>& entries);

    /// Determine if a tombstone is eligible for purging based on its
    /// timestamp vs now minus delete_retention_ms.
    [[nodiscard]] bool can_purge_tombstone(timestamp_ms_t ts, timestamp_ms_t now) const noexcept;

    CompactorConfig     config_;
    KeyedRecordIndex    index_;
};

// ============================================================================
// CompactionStrategy — segment eligibility and merge planning
// ============================================================================

/**
 * Evaluates a list of SegmentInfo descriptors against the compaction
 * policy and produces a CompactionPlan.
 *
 * Eligibility:
 *   - Segment must be sealed (not active).
 *   - Segment must not be empty.
 *   - Segment age >= min_compaction_lag_ms.
 *   - (Optional) dirty ratio >= min_cleanable_dirty_ratio.
 *
 * Urgency:
 *   - If segment age >= max_compaction_lag_ms, the task is marked urgent
 *     and it bypasses the dirty-ratio gate.
 *
 * Merge planning:
 *   - Adjacent sealed segments below small_segment_threshold are bundled
 *     into a single MergeTask for efficiency.
 */
class CompactionStrategy {
public:
    /// Information about a single segment for strategy evaluation.
    struct SegmentCandidate {
        SegmentInfo   info;
        int64_t       duplicate_count = 0;   ///< Estimated number of duplicate keys.
        int64_t       total_keys      = 0;   ///< Estimated total unique-ish keys.
        timestamp_ms_t now            = 0;   ///< Current wall-clock time for age checks.
    };

    explicit CompactionStrategy(CompactorConfig config);
    ~CompactionStrategy();

    CompactionStrategy(const CompactionStrategy&) = delete;
    CompactionStrategy& operator=(const CompactionStrategy&) = delete;
    CompactionStrategy(CompactionStrategy&&) noexcept = default;
    CompactionStrategy& operator=(CompactionStrategy&&) noexcept = default;

    /// Build a compaction plan from a list of segment candidates.
    /// Candidates should be ordered by base_offset ascending.
    ///
    /// @param segments  All sealed segments to evaluate.
    /// @return          A CompactionPlan with tasks ready for execution.
    CompactionPlan plan(const std::vector<SegmentCandidate>& segments);

    /// Check whether a single segment is eligible for compaction at all.
    [[nodiscard]] bool is_eligible(const SegmentCandidate& seg) const;

    /// Check whether a segment is urgent (max_compaction_lag exceeded).
    [[nodiscard]] bool is_urgent(const SegmentCandidate& seg) const;

    /// Compute the estimated dirty ratio for a segment.
    /// dirty_ratio = duplicate_count / max(total_keys, 1)
    [[nodiscard]] static double dirty_ratio(const SegmentCandidate& seg);

    /// Compute the age of a segment in milliseconds since last modified.
    [[nodiscard]] static duration_ms_t age_ms(const SegmentCandidate& seg,
                                               timestamp_ms_t now);

    [[nodiscard]] const CompactorConfig& config() const noexcept { return config_; }

private:
    /// Group adjacent small segments into merge tasks.
    void plan_merges(const std::vector<SegmentCandidate>& segments,
                     CompactionPlan& plan);

    /// Decide which remaining eligible segments become single tasks.
    void plan_singles(const std::vector<SegmentCandidate>& segments,
                      CompactionPlan& plan);

    CompactorConfig config_;
};

// ============================================================================
// CompactionExecutor — atomic segment replacement
// ============================================================================

/**
 * Executes compaction tasks safely via atomic file replacement.
 *
 * For each segment to compact:
 *   1. Read all batches from the segment.
 *   2. Run the Compactor to deduplicate records.
 *   3. Write compacted batches to a temporary file in the same directory.
 *   4. Fsync the temp file and its metadata.
 *   5. Atomically rename the temp file over the original segment.
 *   6. Delete any leftover temp files (crash recovery).
 *
 * On restart after a crash, detect_partial_files() scans for orphaned
 * `.compact` temp files and either completes the rename or deletes them.
 */
class CompactionExecutor {
public:
    /// Result of executing a single task.
    struct TaskResult {
        uint64_t          segment_id;
        CompactionMetrics metrics;
        bool              completed = false;
        error_code        error     = error_code::none;
        std::string       error_message;
    };

    /// Result of a full plan execution.
    struct ExecuteResult {
        std::vector<TaskResult> results;
        CompactionMetrics       cumulative;
    };

    explicit CompactionExecutor(CompactorConfig config);
    ~CompactionExecutor();

    CompactionExecutor(const CompactionExecutor&) = delete;
    CompactionExecutor& operator=(const CompactionExecutor&) = delete;
    CompactionExecutor(CompactionExecutor&&) noexcept = default;
    CompactionExecutor& operator=(CompactionExecutor&&) noexcept = default;

    /// Execute a single compaction task: compact segment `segment_id`
    /// by reading from `source_path`, compacting with `combiner`, and
    /// atomically replacing the source file.
    ///
    /// @param segment     Reference to the Segment object to compact.
    /// @param combiner    Optional merge function (see Compactor::CombinerFn).
    /// @return            Metrics for the completed task.
    TaskResult execute_single(Segment& segment,
                              Compactor::CombinerFn combiner = nullptr);

    /// Execute a merge task: merge several small segments into one,
    /// deduplicating across all of them.
    ///
    /// @param segments    List of segments to merge (moved from).
    /// @param combiner    Optional merge function.
    /// @return            Metrics for the merge task.
    TaskResult execute_merge(std::vector<std::unique_ptr<Segment>> segments,
                             Compactor::CombinerFn combiner = nullptr);

    /// Execute an entire CompactionPlan, processing single and merge
    /// tasks.  Returns cumulative metrics.
    ///
    /// @param plan       The plan produced by CompactionStrategy.
    /// @param segments   The list of all segments (for looking up
    ///                   segments by ID for single tasks).
    /// @param combiner   Optional merge function.
    /// @return           ExecuteResult with per-task results + cumulative.
    ExecuteResult execute_plan(const CompactionPlan& plan,
                               const std::vector<std::unique_ptr<Segment>>& segments,
                               Compactor::CombinerFn combiner = nullptr);

    /// Scan a directory for orphaned `.compact` temp files from a
    /// previous crash.  For each found file:
    ///   - If the target segment file does not exist, rename temp→target.
    ///   - If the target exists and is newer, delete the temp.
    ///   - If both exist and the temp is newer, swap (rename target→backup,
    ///     rename temp→target).
    ///
    /// @param data_directory  Directory to scan for orphaned files.
    /// @return                Number of orphaned files resolved.
    int32_t detect_partial_files(const std::string& data_directory);

    /// Access the internal Compactor for pre-populating keys.
    [[nodiscard]] Compactor& compactor() noexcept { return compactor_; }

    /// Access the configuration.
    [[nodiscard]] const CompactorConfig& config() const noexcept { return config_; }

private:
    /// Write compacted batches to a temp file and atomically rename it
    /// over `target_path`.  Returns metrics and the temp file path.
    [[nodiscard]] result<CompactionMetrics> write_and_swap(
        const std::vector<RecordBatch>& batches,
        const std::string& target_path,
        timestamp_ms_t start_time);

    /// Generate a unique temporary file path in temp_directory (or the
    /// target file's directory if temp_directory is empty).
    [[nodiscard]] std::string temp_file_path(const std::string& target_path) const;

    /// Attempt to delete a file.  Logs a warning on failure.
    void safe_delete(const std::string& path);

    CompactorConfig  config_;
    Compactor        compactor_;
    std::mutex       executor_mutex_;
};

// ============================================================================
// SegmentMerge — merge multiple small segments into one
// ============================================================================

/**
 * Merges several sealed segments into a single larger segment.
 *
 * The merge reads all batches from the source segments in order, feeds
 * them through the Compactor for cross-segment deduplication, and writes
 * the result to a new segment file.  After the merge, the source segments
 * can be safely deleted.
 *
 * Merge is typically used before compaction when many tiny segments
 * (below the small_segment_threshold) are discovered.  Merging them
 * first amortises the compaction overhead.
 */
class SegmentMerge {
public:
    /// Result of a merge operation.
    struct MergeResult {
        /// Newly created segment (moved to caller).  nullptr on failure.
        std::unique_ptr<Segment> merged_segment;

        /// Metrics for the merge + deduplication.
        CompactionMetrics        metrics;

        /// Number of source segments that were merged.
        int32_t                  segments_merged = 0;

        [[nodiscard]] bool ok() const noexcept { return metrics.ok(); }
    };

    explicit SegmentMerge(CompactorConfig config);
    ~SegmentMerge();

    SegmentMerge(const SegmentMerge&) = delete;
    SegmentMerge& operator=(const SegmentMerge&) = delete;
    SegmentMerge(SegmentMerge&&) noexcept = default;
    SegmentMerge& operator=(SegmentMerge&&) noexcept = default;

    /// Merge a list of source segments into a single compacted segment.
    ///
    /// Reads all records from source segments, deduplicates by key
    /// (highest-offset wins by default, or custom combiner), and writes
    /// the result to a new segment file at `base_offset`.
    ///
    /// @param sources        Ordered list of source segments to merge.
    /// @param base_offset    Base offset for the new merged segment.
    /// @param output_path    File path for the new merged segment.
    /// @param combiner       Optional merge function.
    /// @return               MergeResult with the new segment + metrics.
    MergeResult merge(std::vector<Segment*> sources,
                      offset_t base_offset,
                      const std::string& output_path,
                      Compactor::CombinerFn combiner = nullptr);

    /// Access the internal Compactor for pre-populating keys.
    [[nodiscard]] Compactor& compactor() noexcept { return compactor_; }

    /// Access the configuration.
    [[nodiscard]] const CompactorConfig& config() const noexcept { return config_; }

private:
    CompactorConfig config_;
    Compactor       compactor_;
};

} // namespace torrent
