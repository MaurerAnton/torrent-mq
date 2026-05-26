#pragma once

/**
 * torrent-mq — Log Cleaner (Retention Policy Engine)
 *
 * Evaluates time-based and size-based retention policies on sealed log
 * segments.  Determines which segments should be deleted, in what
 * priority order, and never touches the active segment.
 *
 * Retention policies:
 *   Time-based:  max_timestamp < now - retention_ms
 *   Size-based:  total_bytes > retention_bytes → delete oldest first
 *
 * Priority ordering: time-expired > size-excess > empty > orphans.
 */

#include <cstdint>
#include <string>
#include <vector>

#include "torrent/common/types.h"
#include "torrent/storage/types.h"

namespace torrent {

// ============================================================================
// LogCleaner — Retention Policy Engine
// ============================================================================

/**
 * Evaluates log segment retention policies and executes deletions.
 *
 * Called periodically by RetentionManager on a configurable timer.
 * Uses a priority-based ordering to decide which segments to delete
 * first (oldest time-expired first, then size-excess oldest, then
 * empty segments, then orphaned index files).
 *
 * All methods are const with respect to the policy configuration;
 * execute() produces side effects (file deletion).
 */
class LogCleaner {
public:
    // ------------------------------------------------------------------
    // Delete reason enumeration
    // ------------------------------------------------------------------

    /// Why a segment is being deleted.
    enum class DeleteReason : uint8_t {
        none           = 0,
        time_expired   = 1,   ///< max_timestamp < now - retention_ms
        size_excess    = 2,   ///< total bytes exceeds retention_bytes
        empty_segment  = 3,   ///< segment has zero records
        orphan_index   = 4,   ///< .index/.timeindex with no matching .log
        manual         = 5,   ///< explicitly requested deletion
    };

    /// Human-readable name for a DeleteReason.
    static const char* to_string(DeleteReason reason) noexcept;

    // ------------------------------------------------------------------
    // CleanerConfig
    // ------------------------------------------------------------------

    struct CleanerConfig {
        /// Maximum age of a segment before it becomes eligible for
        /// time-based deletion (milliseconds).  -1 = infinite.
        duration_ms_t retention_ms = 604800000;   // 7 days

        /// Maximum total bytes across all segments before size-based
        /// deletion kicks in.  -1 = no limit.
        byte_count_t  retention_bytes = -1;

        /// When true, .index and .timeindex files with no matching
        /// .log file are deleted during the orphan scan.
        bool delete_orphan_indexes = true;

        /// When true, the cleaner only reports what it would delete
        /// without actually removing files.
        bool dry_run = false;

        CleanerConfig();
    };

    // ------------------------------------------------------------------
    // SegmentEligibility — describes why a segment is eligible
    // ------------------------------------------------------------------

    struct SegmentEligibility {
        SegmentInfo   info;                     ///< Segment metadata
        DeleteReason  reason = DeleteReason::none;
        timestamp_ms_t age_ms            = 0;   ///< Age since max_timestamp
        duration_ms_t retention_limit_ms = 0;   ///< From config_.retention_ms
        byte_count_t  retention_limit_bytes = 0; ///< From config_.retention_bytes
        byte_count_t  excess_bytes       = 0;   ///< Bytes over retention limit
        bool          eligible           = false;

        SegmentEligibility();
    };

    // ------------------------------------------------------------------
    // CleanerResult — outcome of a cleaning run
    // ------------------------------------------------------------------

    struct CleanerResult {
        /// All segments evaluated as eligible for deletion.
        std::vector<SegmentEligibility> eligible;

        /// Count of segments deleted by time-based policy.
        int32_t       segments_deleted_by_time = 0;

        /// Count of segments deleted by size-based policy.
        int32_t       segments_deleted_by_size = 0;

        /// Count of empty segments deleted.
        int32_t       segments_deleted_empty   = 0;

        /// Count of orphan index/timeindex files deleted.
        int32_t       orphan_files_deleted     = 0;

        /// Total bytes freed during this run.
        byte_count_t  bytes_deleted            = 0;

        /// Computed new log_start_offset after deletions.
        offset_t      new_log_start_offset     = kInvalidOffset;

        /// Number of deletion operations that failed.
        int32_t       error_count              = 0;

        /// Wall-clock duration of the cleaning run.
        duration_ms_t duration_ms              = 0;

        /// Error code (none on success, even if 0 deletions).
        error_code    error                    = error_code::none;

        /// Human-readable error description.
        std::string   error_message;

        [[nodiscard]] bool ok()          const noexcept;
        [[nodiscard]] bool deleted_any() const noexcept;
    };

    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    explicit LogCleaner(CleanerConfig config);
    ~LogCleaner();

    LogCleaner(const LogCleaner&) = delete;
    LogCleaner& operator=(const LogCleaner&) = delete;
    LogCleaner(LogCleaner&&) noexcept = default;
    LogCleaner& operator=(LogCleaner&&) noexcept = default;

    // ------------------------------------------------------------------
    // Evaluation (no side effects)
    // ------------------------------------------------------------------

    /// Evaluate time-based retention: return eligible segments that have
    /// expired (max_timestamp < now - retention_ms).  Sorted oldest-first.
    [[nodiscard]] CleanerResult evaluate_time_retention(
        const std::vector<SegmentInfo>& segments,
        timestamp_ms_t now);

    /// Evaluate size-based retention: if total_bytes > retention_bytes,
    /// return oldest sealed non-active segments until accumulated size
    /// covers the excess.  Sorted by max_timestamp ascending.
    [[nodiscard]] CleanerResult evaluate_size_retention(
        const std::vector<SegmentInfo>& segments,
        byte_count_t total_bytes,
        timestamp_ms_t now);

    /// Find all empty sealed non-active segments.  These are always
    /// safe to delete regardless of retention settings.
    [[nodiscard]] CleanerResult evaluate_empty_segments(
        const std::vector<SegmentInfo>& segments);

    /// Scan for orphan .index and .timeindex files that have no
    /// corresponding .log file.  Deletes them when config_.delete_orphan_indexes
    /// is true.
    [[nodiscard]] CleanerResult evaluate_orphans(
        const std::string& data_dir,
        const std::vector<SegmentInfo>& segments);

    // ------------------------------------------------------------------
    // Execution (deletes files)
    // ------------------------------------------------------------------

    /// Run the full cleaning pipeline: evaluate time, size, empty, and
    /// orphan policies, sort by priority, and execute deletions.
    /// @param data_dir   Directory containing segment files.
    /// @param segments   Ordered list of segment metadata.
    /// @param now        Current wall-clock time (0 = auto).
    CleanerResult execute(const std::string& data_dir,
                          const std::vector<SegmentInfo>& segments,
                          timestamp_ms_t now = 0);

    // ------------------------------------------------------------------
    // Policy queries
    // ------------------------------------------------------------------

    /// True when this segment should be deleted under either time or
    /// size retention policy.  False for active or unsealed segments.
    [[nodiscard]] bool should_delete_segment(const SegmentInfo& seg,
                                              timestamp_ms_t now) const noexcept;

    /// True when only the time-based policy triggers deletion.
    [[nodiscard]] bool should_delete_by_time(const SegmentInfo& seg,
                                              timestamp_ms_t now) const noexcept;

    /// True when only the size-based policy triggers deletion.
    /// Always returns false (size is evaluated globally, not per-segment).
    [[nodiscard]] bool should_delete_by_size(const SegmentInfo& seg) const noexcept;

    // ------------------------------------------------------------------
    // Priority computation
    // ------------------------------------------------------------------

    /// Compute a numeric priority score for an eligible segment.
    /// Higher = more urgent to delete.
    /// Scale: time-expired(80-100) > size-excess(40-60) >
    ///        empty(20) > orphan(10) > none(0).
    [[nodiscard]] int32_t compute_priority(const SegmentEligibility& eligibility) const noexcept;

    /// Sort a vector of eligible segments by descending priority.
    void sort_by_priority(std::vector<SegmentEligibility>& eligible) const noexcept;

    // ------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------

    [[nodiscard]] const CleanerConfig& config() const noexcept { return config_; }

private:
    /// Compute the new log_start_offset after deletions.
    [[nodiscard]] offset_t compute_new_log_start(
        const std::vector<SegmentInfo>& segments,
        const CleanerResult& result) const noexcept;

    CleanerConfig config_;
};

// ============================================================================
// Convenience: free function
// ============================================================================

/// Run a one-shot cleaner with the given parameters.
/// Useful for tests and admin CLI commands.
LogCleaner::CleanerResult run_cleaner(
    const std::string& data_dir,
    const std::vector<SegmentInfo>& segments,
    duration_ms_t retention_ms,
    byte_count_t retention_bytes,
    bool dry_run = false);

} // namespace torrent
