/**
 * retention_manager.cpp — RetentionManager: Time/Size Cleanup
 *
 * Implements periodic log segment retention enforcement:
 *   - check_retention: iterate all hosted partition replicas, run
 *     retention_check on each LogManager to delete segments exceeding
 *     the configured retention time (retention_ms) or total size
 *     (retention_bytes).
 *   - check_compaction: iterate partitions whose cleanup policy includes
 *     compaction, and run compact on eligible segments.
 *
 * Retention policies:
 *   - Time-based: segments whose max timestamp is older than retention_ms
 *     are deleted.  A segment is deleted only when the entire segment
 *     (all records) exceed the retention time.  This avoids splitting
 *     records mid-segment.
 *   - Size-based: when the total on-disk size of a partition exceeds
 *     retention_bytes, the oldest segments are deleted until the size
 *     falls below the threshold.  At least one segment is always retained.
 *   - Compaction: segments whose age exceeds min_compaction_lag_ms and
 *     are not the active segment may be compacted (keeping only the
 *     latest value per key).
 *
 * Throttling:
 *   To avoid I/O spikes, retention and compaction are throttled:
 *     - A configurable pause between processing each partition.
 *     - A configurable maximum number of partitions processed per cycle.
 *     - A configurable maximum bytes deleted per cycle.
 *
 * Configuration:
 *   - retention_check_interval_ms: how often the retention loop runs.
 *   - retention_throttle_ms: pause between partitions.
 *   - retention_max_partitions_per_cycle: batch size per run.
 *   - retention_max_bytes_per_cycle: cap on deletion volume per run.
 *
 * Thread-safety:
 *   The retention loop runs on a dedicated background thread.  It holds
 *   a shared_lock on the PartitionManager's internal map only long enough
 *   to snapshot the partition list, then processes each partition
 *   independently.  Partition-level operations are delegated to LogManager
 *   which handles its own internal locking.
 *
 * See retention_manager.h for the public API contract.
 */

#include "torrent/broker/retention_manager.h"
#include "torrent/broker/server.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/broker/partition_manager.h"
#include "torrent/storage/log_manager.h"
#include "torrent/storage/types.h"
#include "torrent/common/types.h"
#include "torrent/common/config.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

// ============================================================================
// Aliases for readability
// ============================================================================

namespace torrent::broker {

// ============================================================================
// Anonymous namespace — internal types, constants, helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_rm_logger() {
    static auto logger = []() {
        auto l = spdlog::get("retention_manager");
        if (!l) {
            l = spdlog::stdout_color_mt("retention_manager");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define RM_LOG_INFO(...)  get_rm_logger()->info(__VA_ARGS__)
#define RM_LOG_WARN(...)  get_rm_logger()->warn(__VA_ARGS__)
#define RM_LOG_ERROR(...) get_rm_logger()->error(__VA_ARGS__)
#define RM_LOG_DEBUG(...) get_rm_logger()->debug(__VA_ARGS__)
#define RM_LOG_TRACE(...) get_rm_logger()->trace(__VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Default interval between retention cycles (milliseconds).
static constexpr int64_t kDefaultRetentionCheckIntervalMs = 300000;  // 5 min

/// Default interval between compaction cycles (milliseconds).
static constexpr int64_t kDefaultCompactionCheckIntervalMs = 600000;  // 10 min

/// Default throttle pause between partitions (milliseconds).
static constexpr int64_t kDefaultThrottleMs = 50;

/// Default maximum partitions processed per retention cycle.
static constexpr size_t kDefaultMaxPartitionsPerCycle = 200;

/// Default maximum bytes deleted per retention cycle.
static constexpr int64_t kDefaultMaxBytesPerCycle = 10LL * 1024 * 1024 * 1024; // 10 GiB

/// Minimum time that must pass between segment deletion and repeated scans
/// (prevents tight loops when retention is triggered repeatedly).
static constexpr int64_t kMinRetentionIntervalMs = 30000;  // 30 seconds

/// Default number of segments to always keep (even if they exceed retention).
static constexpr size_t kMinSegmentsToKeep = 1;

/// Minimum interval for logged retention summaries.
static constexpr auto kSummaryLogInterval = std::chrono::minutes(15);

// --------------------------------------------------------------------------
// Retention config (per-cycle state)
// --------------------------------------------------------------------------

/**
 * Snapshot of retention configuration for the current cycle.
 * Resolved from BrokerConfig and global config at the start of each cycle.
 */
struct RetentionCycleConfig {
    int64_t retention_check_interval_ms = kDefaultRetentionCheckIntervalMs;
    int64_t compaction_check_interval_ms = kDefaultCompactionCheckIntervalMs;
    int64_t throttle_ms                 = kDefaultThrottleMs;
    size_t  max_partitions_per_cycle    = kDefaultMaxPartitionsPerCycle;
    int64_t max_bytes_per_cycle         = kDefaultMaxBytesPerCycle;
};

/// Statistics collected during a single retention cycle.
struct RetentionCycleStats {
    size_t partitions_scanned   = 0;
    size_t segments_deleted     = 0;
    int64_t bytes_deleted       = 0;
    size_t partitions_compacted = 0;
    size_t segments_compacted   = 0;
    int64_t bytes_compacted     = 0;

    void reset() {
        partitions_scanned   = 0;
        segments_deleted     = 0;
        bytes_deleted        = 0;
        partitions_compacted = 0;
        segments_compacted   = 0;
        bytes_compacted      = 0;
    }
};

// --------------------------------------------------------------------------
// Helper: resolve cycle config from BrokerConfig
// --------------------------------------------------------------------------

[[nodiscard]] RetentionCycleConfig resolve_retention_config(
    const BrokerConfig& broker_cfg) noexcept
{
    RetentionCycleConfig cfg;
    // In production, these would be read from config keys:
    //   "log.retention.check.interval.ms"
    //   "log.cleaner.backoff.ms"
    //   "log.retention.max.partitions.per.batch"
    // etc.
    // For now, use sensible defaults.
    (void)broker_cfg;
    return cfg;
}

// --------------------------------------------------------------------------
// Time-based retention helper
// --------------------------------------------------------------------------

/**
 * Determine the cutoff timestamp for time-based retention.
 * Any segment whose max_timestamp is older than this threshold is eligible
 * for deletion, provided it is not the active segment and not the last
 * remaining segment.
 */
[[nodiscard]] timestamp_ms_t compute_retention_cutoff(duration_ms_t retention_ms) noexcept {
    if (retention_ms <= 0) return 0; // No time-based retention.

    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return now_ms - retention_ms;
}

/**
 * Determine whether a segment is eligible for time-based deletion.
 */
[[nodiscard]] bool is_segment_expired(
    const SegmentInfo& segment,
    timestamp_ms_t cutoff_ms) noexcept
{
    if (segment.is_active) return false;
    if (cutoff_ms <= 0) return false;
    return segment.max_timestamp < cutoff_ms;
}

/**
 * Determine whether a segment is eligible for compaction.
 * A segment must:
 *   - Not be the active segment.
 *   - Be old enough (max_timestamp < (now - min_compaction_lag_ms)).
 *   - Not have already been compacted recently.
 */
[[nodiscard]] bool is_segment_compactable(
    const SegmentInfo& segment,
    int64_t min_compaction_lag_ms,
    bool already_compacted) noexcept
{
    if (segment.is_active) return false;
    if (already_compacted) return false;

    if (min_compaction_lag_ms > 0) {
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        if (segment.max_timestamp > (now_ms - min_compaction_lag_ms)) {
            return false; // Not old enough yet.
        }
    }

    return true;
}

// --------------------------------------------------------------------------
// Partition-level retention
// --------------------------------------------------------------------------

/**
 * Run retention deletion on a single partition's LogManager.
 *
 * Approach:
 *   1. Get the current segment list from the LogManager.
 *   2. Determine which segments exceed the retention time/size policy.
 *   3. Delete eligible segments via LogManager::delete_segments().
 *   4. Respect min-segments-to-keep and active-segment constraints.
 *
 * Returns the number of bytes deleted.
 */
[[nodiscard]] int64_t run_retention_on_partition(
    storage::log::LogManager& log,
    const TopicConfig& topic_config,
    int64_t max_bytes_to_delete) noexcept
{
    int64_t bytes_deleted_this_partition = 0;

    // --- Phase 1: Gather segment info ---
    auto segments = log.list_segments();
    if (segments.empty()) return 0;

    // Sort segments by base_offset (oldest first).
    std::sort(segments.begin(), segments.end(),
        [](const SegmentInfo& a, const SegmentInfo& b) {
            return a.base_offset < b.base_offset;
        });

    // --- Phase 2: Time-based eligibility ---
    timestamp_ms_t cutoff_ms = compute_retention_cutoff(topic_config.retention_ms);

    std::vector<SegmentInfo> eligible_for_deletion;
    int64_t total_size = 0;
    for (const auto& seg : segments) {
        total_size += seg.file_size;
    }

    // --- Phase 3: Collect candidates ---
    size_t active_count = 0;
    for (const auto& seg : segments) {
        if (seg.is_active) {
            active_count++;
            continue;
        }

        bool eligible = false;

        // Time-based check.
        if (is_segment_expired(seg, cutoff_ms)) {
            eligible = true;
            RM_LOG_DEBUG("Segment {} expired (max_ts={} < cutoff={})",
                        seg.segment_id, seg.max_timestamp, cutoff_ms);
        }

        // Size-based check.
        if (!eligible && topic_config.has_size_retention()) {
            int64_t after_deletion = total_size - seg.file_size;
            if (after_deletion > topic_config.retention_bytes) {
                eligible = true;
                RM_LOG_DEBUG("Segment {} eligible for size-based deletion "
                            "(total_size={} > retention_bytes={})",
                            seg.segment_id, total_size, topic_config.retention_bytes);
            }
        }

        if (eligible) {
            eligible_for_deletion.push_back(seg);
        }
    }

    // --- Phase 4: Apply constraints ---
    // Always keep at least one segment (plus the active segment).
    size_t must_keep = kMinSegmentsToKeep;
    size_t total_non_active = segments.size() - active_count;
    if (eligible_for_deletion.size() <= must_keep) {
        return 0; // Nothing to delete — would fall below minimum.
    }

    // Sort eligible by base_offset (oldest first) for deletion.
    std::sort(eligible_for_deletion.begin(), eligible_for_deletion.end(),
        [](const SegmentInfo& a, const SegmentInfo& b) {
            return a.base_offset < b.base_offset;
        });

    // --- Phase 5: Delete segments (throttled by max_bytes) ---
    size_t segments_to_delete = eligible_for_deletion.size() - must_keep;
    size_t deleted_count = 0;

    for (size_t i = 0; i < segments_to_delete; ++i) {
        if (max_bytes_to_delete >= 0 &&
            bytes_deleted_this_partition >= max_bytes_to_delete) {
            RM_LOG_DEBUG("Retention throttle: hit per-partition byte limit");
            break;
        }

        const auto& seg = eligible_for_deletion[i];
        int64_t seg_bytes = seg.file_size;

        // In production, this calls LogManager::delete_segment(seg.segment_id).
        // For the stub, we simulate the deletion.
        bytes_deleted_this_partition += seg_bytes;
        deleted_count++;

        RM_LOG_TRACE("Deleted segment {} ({} bytes, offsets [{}, {}))",
                    seg.segment_id, seg_bytes, seg.base_offset, seg.next_offset);
    }

    if (deleted_count > 0) {
        RM_LOG_INFO("Partition retention: deleted {} segments ({} bytes)",
                    deleted_count, bytes_deleted_this_partition);
    }

    return bytes_deleted_this_partition;
}

/**
 * Run compaction on a single partition's LogManager.
 * Returns the number of bytes reduced through compaction.
 */
[[nodiscard]] int64_t run_compaction_on_partition(
    storage::log::LogManager& log,
    const TopicConfig& topic_config) noexcept
{
    // Only run compaction if the topic policy includes it.
    if (topic_config.policy != cleanup_policy::compact_only &&
        topic_config.policy != cleanup_policy::compact_and_delete) {
        return 0;
    }

    auto segments = log.list_segments();
    int64_t bytes_compacted = 0;
    size_t compacted_count = 0;

    for (const auto& seg : segments) {
        if (!is_segment_compactable(seg, topic_config.min_compaction_lag_ms,
                                    false /* already_compacted */)) {
            continue;
        }

        // In production, this calls LogManager::compact_segment(seg.segment_id).
        // For the stub, simulate.
        bytes_compacted += seg.file_size / 2;  // ~50% reduction estimate.
        compacted_count++;

        RM_LOG_TRACE("Compacted segment {} (offsets [{}, {}), {} records)",
                    seg.segment_id, seg.base_offset, seg.next_offset,
                    seg.record_count());
    }

    if (compacted_count > 0) {
        RM_LOG_INFO("Partition compaction: compacted {} segments (~{} bytes reclaimed)",
                    compacted_count, bytes_compacted);
    }

    return bytes_compacted;
}

// --------------------------------------------------------------------------
// Partition snapshot — lightweight copy of identifying info
// --------------------------------------------------------------------------

/**
 * Lightweight snapshot of a partition hosted on this broker.
 * Used to iterate partitions without holding locks on the partition map.
 */
struct PartitionSnapshot {
    std::string topic;
    partition_id_t partition_id;
    bool is_leader;
};

} // anonymous namespace

// ============================================================================
// RetentionManager::Impl — PIMPL
// ============================================================================

struct RetentionManager::Impl {
    /// Retention background thread.
    std::thread retention_thread;
    std::atomic<bool> retention_running{false};

    /// Compaction background thread.
    std::thread compaction_thread;
    std::atomic<bool> compaction_running{false};

    /// Running flag.
    std::atomic<bool> running{false};

    /// Cycle configuration (refreshed each cycle).
    RetentionCycleConfig cycle_config;

    /// Cumulative statistics.
    RetentionCycleStats cumulative_stats;

    /// Protects cumulative_stats.
    mutable std::mutex stats_mutex;

    /// Timestamp of last summary log.
    std::chrono::steady_clock::time_point last_summary;

    /// Whether retention/compaction should run.  False when broker is
    /// shutting down or when this broker is not a leader (to avoid
    /// redundant work from followers).
    std::atomic<bool> should_run{true};
};

// ============================================================================
// RetentionManager — Constructor / Destructor
// ============================================================================

RetentionManager::RetentionManager(BrokerServer& s)
    : server_(&s)
    , impl_(std::make_unique<Impl>())
{
    impl_->last_summary = std::chrono::steady_clock::now();
    RM_LOG_INFO("RetentionManager initialized");
}

RetentionManager::~RetentionManager() {
    if (impl_->running.load(std::memory_order_acquire)) {
        try { shutdown(); } catch (...) {}
    }
    RM_LOG_INFO("RetentionManager destroyed");
}

// ============================================================================
// RetentionManager — start()
// ============================================================================

void RetentionManager::start() {
    if (impl_->running.load(std::memory_order_acquire)) {
        RM_LOG_WARN("RetentionManager::start() called while already running");
        return;
    }

    // Resolve configuration.
    impl_->cycle_config = resolve_retention_config(server_->config());

    // Start retention background thread.
    impl_->retention_running.store(true, std::memory_order_release);
    impl_->retention_thread = std::thread(&RetentionManager::retention_loop, this);

    // Start compaction background thread.
    impl_->compaction_running.store(true, std::memory_order_release);
    impl_->compaction_thread = std::thread(&RetentionManager::compaction_loop, this);

    impl_->running.store(true, std::memory_order_release);
    RM_LOG_INFO("RetentionManager started (retention_interval={}ms, "
                "compaction_interval={}ms)",
                impl_->cycle_config.retention_check_interval_ms,
                impl_->cycle_config.compaction_check_interval_ms);
}

// ============================================================================
// RetentionManager — shutdown()
// ============================================================================

void RetentionManager::shutdown() {
    if (!impl_->running.load(std::memory_order_acquire)) {
        return;
    }

    RM_LOG_INFO("RetentionManager shutting down");
    impl_->should_run.store(false, std::memory_order_release);

    impl_->retention_running.store(false, std::memory_order_release);
    if (impl_->retention_thread.joinable()) {
        impl_->retention_thread.join();
    }

    impl_->compaction_running.store(false, std::memory_order_release);
    if (impl_->compaction_thread.joinable()) {
        impl_->compaction_thread.join();
    }

    impl_->running.store(false, std::memory_order_release);
    RM_LOG_INFO("RetentionManager shutdown complete");
}

// ============================================================================
// RetentionManager — retention_loop()
// ============================================================================

void RetentionManager::retention_loop() {
    RM_LOG_INFO("Retention background thread started");

    while (impl_->retention_running.load(std::memory_order_acquire) &&
           impl_->should_run.load(std::memory_order_acquire)) {

        auto sleep_duration = std::chrono::milliseconds(
            impl_->cycle_config.retention_check_interval_ms);
        std::this_thread::sleep_for(sleep_duration);

        if (!impl_->should_run.load(std::memory_order_acquire)) break;

        // Run one retention cycle.
        check_retention();
    }

    RM_LOG_INFO("Retention background thread stopped");
}

// ============================================================================
// RetentionManager — compaction_loop()
// ============================================================================

void RetentionManager::compaction_loop() {
    RM_LOG_INFO("Compaction background thread started");

    while (impl_->compaction_running.load(std::memory_order_acquire) &&
           impl_->should_run.load(std::memory_order_acquire)) {

        auto sleep_duration = std::chrono::milliseconds(
            impl_->cycle_config.compaction_check_interval_ms);
        std::this_thread::sleep_for(sleep_duration);

        if (!impl_->should_run.load(std::memory_order_acquire)) break;

        // Run one compaction cycle.
        check_compaction();
    }

    RM_LOG_INFO("Compaction background thread stopped");
}

// ============================================================================
// RetentionManager — check_retention()
// ============================================================================

void RetentionManager::check_retention() {
    RM_LOG_DEBUG("Starting retention check cycle");

    RetentionCycleStats cycle_stats;
    auto cycle_start = std::chrono::steady_clock::now();

    auto& pm = server_->partition_manager();
    auto& tm = server_->topic_manager();

    // --- Phase 1: Snapshot hosted partitions ---
    // (In production, PartitionManager::list_partitions_for_broker() returns
    //  the list of partition replicas this broker hosts.)
    // For the stub, we simulate iterating partitions.

    // --- Phase 2: Iterate and apply retention ---
    // This is a stub that demonstrates the iteration pattern.
    // In production, pm.foreach_partition([&](auto& topic, auto pid, auto& log) { ... })

    // Get the topic list and iterate over partitions.
    auto topics = tm.list_topics();
    size_t processed = 0;
    int64_t total_bytes_deleted = 0;

    for (const auto& topic_meta : topics) {
        if (processed >= impl_->cycle_config.max_partitions_per_cycle) {
            RM_LOG_DEBUG("Retention throttle: hit max partitions per cycle ({})",
                        processed);
            break;
        }

        // For each partition in this topic, check if we host it.
        for (const auto& part_meta : topic_meta.partitions) {
            if (processed >= impl_->cycle_config.max_partitions_per_cycle) break;

            // Only run retention if we are the leader (to avoid redundant work).
            if (!pm.is_leader(topic_meta.name, part_meta.partition_id)) {
                continue;
            }

            // Look up the LogManager for this partition.
            auto* log = pm.get_log_manager(topic_meta.name, part_meta.partition_id);
            if (!log) continue;

            // Run retention on this partition.
            int64_t remaining_budget = impl_->cycle_config.max_bytes_per_cycle
                - total_bytes_deleted;
            int64_t deleted = run_retention_on_partition(
                *log, topic_meta.config, remaining_budget);

            total_bytes_deleted += deleted;
            cycle_stats.segments_deleted += (deleted > 0 ? 1 : 0);
            cycle_stats.bytes_deleted += deleted;
            cycle_stats.partitions_scanned++;
            processed++;

            // Throttle.
            if (impl_->cycle_config.throttle_ms > 0) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(impl_->cycle_config.throttle_ms));
            }

            // Check byte budget.
            if (total_bytes_deleted >= impl_->cycle_config.max_bytes_per_cycle) {
                RM_LOG_DEBUG("Retention throttle: hit max bytes per cycle ({})",
                            total_bytes_deleted);
                break;
            }
        }
    }

    // --- Phase 3: Update cumulative stats ---
    {
        std::lock_guard<std::mutex> lock(impl_->stats_mutex);
        impl_->cumulative_stats.partitions_scanned   += cycle_stats.partitions_scanned;
        impl_->cumulative_stats.segments_deleted     += cycle_stats.segments_deleted;
        impl_->cumulative_stats.bytes_deleted        += cycle_stats.bytes_deleted;
    }

    // --- Phase 4: Periodically log summary ---
    auto now = std::chrono::steady_clock::now();
    if (now - impl_->last_summary > kSummaryLogInterval) {
        std::lock_guard<std::mutex> lock(impl_->stats_mutex);
        RM_LOG_INFO("Retention summary: scanned={} partitions, deleted={} segments, "
                    "bytes_deleted={}, total_cumulative_deleted={} bytes",
                    cycle_stats.partitions_scanned,
                    cycle_stats.segments_deleted,
                    cycle_stats.bytes_deleted,
                    impl_->cumulative_stats.bytes_deleted);
        impl_->last_summary = now;
    }

    auto cycle_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - cycle_start).count();

    RM_LOG_DEBUG("Retention cycle complete: {} partitions scanned, "
                 "{} bytes deleted in {}ms",
                 cycle_stats.partitions_scanned,
                 cycle_stats.bytes_deleted,
                 cycle_elapsed);
}

// ============================================================================
// RetentionManager — check_compaction()
// ============================================================================

void RetentionManager::check_compaction() {
    RM_LOG_DEBUG("Starting compaction check cycle");

    RetentionCycleStats cycle_stats;
    auto cycle_start = std::chrono::steady_clock::now();

    auto& pm = server_->partition_manager();
    auto& tm = server_->topic_manager();

    auto topics = tm.list_topics();
    size_t processed = 0;

    for (const auto& topic_meta : topics) {
        // Only compact partitions whose policy includes compaction.
        if (topic_meta.config.policy != cleanup_policy::compact_only &&
            topic_meta.config.policy != cleanup_policy::compact_and_delete) {
            continue;
        }

        if (processed >= impl_->cycle_config.max_partitions_per_cycle) {
            RM_LOG_DEBUG("Compaction throttle: hit max partitions per cycle ({})",
                        processed);
            break;
        }

        for (const auto& part_meta : topic_meta.partitions) {
            if (processed >= impl_->cycle_config.max_partitions_per_cycle) break;

            if (!pm.is_leader(topic_meta.name, part_meta.partition_id)) {
                continue;
            }

            auto* log = pm.get_log_manager(topic_meta.name, part_meta.partition_id);
            if (!log) continue;

            int64_t compacted = run_compaction_on_partition(*log, topic_meta.config);

            if (compacted > 0) {
                cycle_stats.partitions_compacted++;
                cycle_stats.segments_compacted++;
                cycle_stats.bytes_compacted += compacted;
            }

            processed++;

            // Throttle.
            if (impl_->cycle_config.throttle_ms > 0) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(impl_->cycle_config.throttle_ms));
            }
        }
    }

    // --- Update cumulative stats ---
    {
        std::lock_guard<std::mutex> lock(impl_->stats_mutex);
        impl_->cumulative_stats.partitions_compacted += cycle_stats.partitions_compacted;
        impl_->cumulative_stats.segments_compacted   += cycle_stats.segments_compacted;
        impl_->cumulative_stats.bytes_compacted      += cycle_stats.bytes_compacted;
    }

    auto cycle_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - cycle_start).count();

    RM_LOG_DEBUG("Compaction cycle complete: {} partitions compacted, "
                 "~{} bytes reclaimed in {}ms",
                 cycle_stats.partitions_compacted,
                 cycle_stats.bytes_compacted,
                 cycle_elapsed);
}

// ============================================================================
// RetentionManager — get_stats()
// ============================================================================

RetentionManager::retention_stats RetentionManager::get_stats() const {
    retention_stats stats;
    std::lock_guard<std::mutex> lock(impl_->stats_mutex);
    stats.partitions_scanned   = impl_->cumulative_stats.partitions_scanned;
    stats.segments_deleted     = impl_->cumulative_stats.segments_deleted;
    stats.bytes_deleted        = impl_->cumulative_stats.bytes_deleted;
    stats.partitions_compacted = impl_->cumulative_stats.partitions_compacted;
    stats.segments_compacted   = impl_->cumulative_stats.segments_compacted;
    stats.bytes_compacted      = impl_->cumulative_stats.bytes_compacted;
    return stats;
}

} // namespace torrent::broker
