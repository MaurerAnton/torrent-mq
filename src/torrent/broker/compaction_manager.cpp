/**
 * compaction_manager.cpp — CompactionManager: Compaction Scheduling
 *
 * Implements periodic log compaction with background scheduling:
 *   - compact_partition: run key-based compaction on a specific partition
 *   - Background scheduling: periodically scan partitions whose cleanup
 *     policy includes compaction, select eligible segments, and compact
 *     them in order of priority.
 *
 * Compaction model (Kafka-compatible log compaction):
 *   For each partition with a "compact" cleanup policy, compaction keeps
 *   only the latest value per message key.  Older records for the same key
 *   are removed.  Records with a null value ("tombstones") are retained for
 *   a configurable delete_retention_ms period before being removed entirely.
 *
 * Key compaction algorithm:
 *   1. Read all records from a sealed segment.
 *   2. Build an in-memory map of key → offset (keeping only the latest).
 *   3. Write a new compacted segment containing only the latest records.
 *   4. Swap the compacted segment in place of the original.
 *   5. Update the segment index to reflect the new offsets.
 *
 *   (The actual compaction is performed by LogManager::compact_segment();
 *   this manager only decides *which* segments to compact and when.)
 *
 * Priority scheduling:
 *   Segments are prioritized for compaction in this order:
 *     1. Dirty ratio: segments with the most duplicate keys are compacted
 *        first (highest space savings).
 *     2. Age: older segments are preferred over newer ones.
 *     3. Size: smaller segments are preferred (faster to compact).
 *
 * Throttling:
 *   - max_compaction_io_bytes_per_sec: limits compaction I/O throughput.
 *   - max_compaction_lag_ms: how far behind a partition's active segment
 *     the cleaner can fall before pausing.
 *   - min_cleanable_dirty_ratio: minimum proportion of duplicate records
 *     before a segment is considered for compaction (default 0.5).
 *
 * Background scheduling:
 *   - The background thread wakes up every compaction_check_interval_ms.
 *   - It builds a priority queue of all compactable segments across
 *     all hosted partitions.
 *   - It processes the top N segments, limited by I/O throughput budget.
 *   - It sleeps between partitions to spread I/O load.
 *
 * Configuration:
 *   - log.cleaner.min.cleanable.ratio: minimum dirty ratio (default 0.5)
 *   - log.cleaner.max.compaction.lag.ms: max lag behind active segment
 *   - log.cleaner.io.max.bytes.per.second: I/O throttle
 *   - log.cleaner.backoff.ms: pause between compaction rounds
 *   - log.cleaner.threads: number of compaction threads
 *
 * Thread-safety:
 *   The background compaction thread runs independently.  It snapshots
 *   the partition list under a shared_lock, then works on individual
 *   partitions without holding the global lock.  Partition-level
 *   compaction is delegated to LogManager which locks internally.
 *
 * See compaction_manager.h for the public API contract.
 */

#include "torrent/broker/compaction_manager.h"
#include "torrent/broker/server.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/broker/partition_manager.h"
#include "torrent/storage/log_manager.h"
#include "torrent/storage/segment.h"
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
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
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

[[nodiscard]] std::shared_ptr<spdlog::logger> get_cm_logger() {
    static auto logger = []() {
        auto l = spdlog::get("compaction_manager");
        if (!l) {
            l = spdlog::stdout_color_mt("compaction_manager");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define CM_LOG_INFO(...)  get_cm_logger()->info(__VA_ARGS__)
#define CM_LOG_WARN(...)  get_cm_logger()->warn(__VA_ARGS__)
#define CM_LOG_ERROR(...) get_cm_logger()->error(__VA_ARGS__)
#define CM_LOG_DEBUG(...) get_cm_logger()->debug(__VA_ARGS__)
#define CM_LOG_TRACE(...) get_cm_logger()->trace(__VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Default interval between compaction scheduling cycles (ms).
static constexpr int64_t kDefaultCompactionCheckIntervalMs = 60000;  // 1 min

/// Default minimum dirty ratio for a segment to be compactable.
/// A segment with fewer than this proportion of duplicate keys is skipped.
static constexpr double kDefaultMinCleanableDirtyRatio = 0.5;

/// Default maximum lag (ms) between the active segment and the segment
/// being compacted.  The cleaner must not fall behind by more than this.
static constexpr int64_t kDefaultMaxCompactionLagMs = 86400000;  // 24 hours

/// Default I/O throttle for compaction (bytes/sec).  0 = unlimited.
static constexpr int64_t kDefaultMaxCompactionIOBytesPerSec = 0;

/// Default backoff between compaction rounds (ms).
static constexpr int64_t kDefaultCompactionBackoffMs = 100;

/// Default number of compaction threads.
static constexpr size_t kDefaultCompactionThreads = 1;

/// Maximum number of segments to consider per cycle.
static constexpr size_t kMaxSegmentsPerCycle = 100;

/// Minimum size (bytes) of a segment to be considered for compaction.
/// Tiny segments are deferred to avoid excessive overhead.
static constexpr int64_t kMinSegmentSizeForCompaction = 1024 * 1024;  // 1 MiB

/// How often we log cumulative compaction statistics.
static constexpr auto kStatsLogInterval = std::chrono::minutes(30);

// ============================================================================
// CandidateSegment — a single segment eligible for compaction
// ============================================================================

/**
 * Describes a single segment that is a candidate for compaction.
 * The priority queue orders by dirty_ratio (descending), then by
 * base_offset (ascending to prefer older segments), then size (ascending).
 */
struct CandidateSegment {
    std::string topic;
    partition_id_t partition_id;
    uint64_t segment_id;
    offset_t base_offset;
    offset_t next_offset;
    int64_t file_size;
    double dirty_ratio;          // Estimated proportion of duplicate keys (0–1).
    timestamp_ms_t max_timestamp;
    int64_t estimated_savings;   // Estimated bytes that would be freed.

    /// Priority: higher dirty_ratio → higher priority.
    /// Within same dirty_ratio, prefer older segments and smaller segments.
    [[nodiscard]] bool operator<(const CandidateSegment& other) const noexcept {
        if (dirty_ratio != other.dirty_ratio) {
            return dirty_ratio < other.dirty_ratio;  // Higher dirty_ratio = higher prio.
        }
        if (max_timestamp != other.max_timestamp) {
            return max_timestamp > other.max_timestamp;  // Older = higher prio.
        }
        if (file_size != other.file_size) {
            return file_size > other.file_size;  // Smaller = higher prio (faster to compact).
        }
        return base_offset > other.base_offset;
    }

    /// Estimated space savings in bytes.
    [[nodiscard]] int64_t estimated_reclaimed_bytes() const noexcept {
        return static_cast<int64_t>(static_cast<double>(file_size) * dirty_ratio);
    }
};

// ============================================================================
// Compaction scheduler config (refreshed each cycle)
// ============================================================================

struct CompactionSchedulerConfig {
    int64_t check_interval_ms       = kDefaultCompactionCheckIntervalMs;
    double  min_cleanable_dirty_ratio = kDefaultMinCleanableDirtyRatio;
    int64_t max_compaction_lag_ms   = kDefaultMaxCompactionLagMs;
    int64_t max_io_bytes_per_sec    = kDefaultMaxCompactionIOBytesPerSec;
    int64_t backoff_ms              = kDefaultCompactionBackoffMs;
    size_t  num_threads             = kDefaultCompactionThreads;
};

/// Resolve scheduler config from broker configuration.
[[nodiscard]] CompactionSchedulerConfig resolve_scheduler_config(
    const BrokerConfig& broker_cfg) noexcept
{
    CompactionSchedulerConfig cfg;
    // In production, these would come from config keys.
    (void)broker_cfg;
    return cfg;
}

// ============================================================================
// Per-cycle statistics
// ============================================================================

struct CompactionCycleStats {
    size_t partitions_scanned   = 0;
    size_t segments_evaluated   = 0;
    size_t segments_compacted   = 0;
    int64_t bytes_reclaimed     = 0;
    double  avg_dirty_ratio     = 0.0;
    int64_t cycle_time_ms       = 0;

    void reset() {
        partitions_scanned = 0;
        segments_evaluated = 0;
        segments_compacted = 0;
        bytes_reclaimed    = 0;
        avg_dirty_ratio    = 0.0;
        cycle_time_ms      = 0;
    }
};

// ============================================================================
// Dirty ratio estimator
// ============================================================================

/**
 * Estimate the "dirty ratio" of a segment — the proportion of records
 * that are duplicates (i.e., share a key with a newer record in the same
 * or a later segment).
 *
 * In production, this uses sampling of the segment index to estimate
 * the number of duplicate keys.  For the stub, we use a heuristic:
 *   - Compare the base_offset of the segment to the next_offset of the
 *     next segment (if any).  The ratio of the segment's record count
 *     to the total record count across overlapping key-space gives an
 *     estimate of dereferenced duplicates.
 *
 * Returns a value between 0.0 (clean) and 1.0 (fully duplicated).
 */
[[nodiscard]] double estimate_dirty_ratio(
    const SegmentInfo& segment,
    const std::vector<SegmentInfo>& all_segments,
    const TopicConfig& topic_config) noexcept
{
    (void)topic_config;

    if (segment.record_count() <= 0) return 0.0;

    // Simple heuristic: longer-running partitions accumulate more duplicates.
    // A segment with records spanning a large time range is more likely to
    // have overwritten keys.
    //
    // Dirty ratio ≈ 1 - (1 / (segment_count_after_this + 1))
    // i.e., if there are many segments after this one, many of its keys
    // have probably been overwritten.

    int64_t segments_after = 0;
    for (const auto& other : all_segments) {
        if (other.base_offset > segment.base_offset && !other.is_active) {
            segments_after++;
        }
    }

    if (segments_after == 0) return 0.0;

    // This is a crude estimator.  In production, a sampling-based approach
    // scans the offset index to count distinct keys.
    double ratio = 1.0 - (1.0 / static_cast<double>(segments_after + 1));

    // Clamp to [0, 1].
    return std::max(0.0, std::min(1.0, ratio));
}

/**
 * Estimate the bytes that would be reclaimed by compacting this segment.
 */
[[nodiscard]] int64_t estimate_reclaimed_bytes(
    const SegmentInfo& segment, double dirty_ratio) noexcept
{
    return static_cast<int64_t>(static_cast<double>(segment.file_size) * dirty_ratio);
}

// ============================================================================
// Candidate collection — build priority queue of compactable segments
// ============================================================================

/**
 * Build a priority queue of compactable segment candidates across all
 * hosted partitions that have a compaction-eligible cleanup policy.
 */
[[nodiscard]] std::priority_queue<CandidateSegment> collect_candidates(
    const std::vector<PartitionSnapshot>& partitions,
    PartitionManager& pm,
    const CompactionSchedulerConfig& config)
{
    std::priority_queue<CandidateSegment> candidates;
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    for (const auto& ps : partitions) {
        if (candidates.size() >= kMaxSegmentsPerCycle * 2) break;

        auto* log = pm.get_log_manager(ps.topic, ps.partition_id);
        if (!log) continue;

        auto segments = log->list_segments();
        if (segments.empty()) continue;

        // Only consider sealed (non-active) segments.
        for (const auto& seg : segments) {
            if (seg.is_active) continue;
            if (seg.file_size < kMinSegmentSizeForCompaction) continue;

            // Check compaction lag — don't compact if the segment is too
            // close to the active segment's latest timestamp.
            if (config.max_compaction_lag_ms > 0) {
                int64_t lag_ms = now_ms - seg.max_timestamp;
                if (lag_ms < config.max_compaction_lag_ms) {
                    // Segment is within the lag window; skip but log if close.
                    if (lag_ms < 0) {
                        CM_LOG_TRACE("Segment {} has future timestamp ({} > now={}), skipping",
                                    seg.segment_id, seg.max_timestamp, now_ms);
                    }
                    continue;
                }
            }

            double dirty = estimate_dirty_ratio(seg, segments, TopicConfig{});
            if (dirty < config.min_cleanable_dirty_ratio) {
                CM_LOG_TRACE("Segment {} dirty_ratio={:.2f} < min={:.2f}, skipping",
                            seg.segment_id, dirty, config.min_cleanable_dirty_ratio);
                continue;
            }

            CandidateSegment candidate;
            candidate.topic              = ps.topic;
            candidate.partition_id       = ps.partition_id;
            candidate.segment_id         = seg.segment_id;
            candidate.base_offset        = seg.base_offset;
            candidate.next_offset        = seg.next_offset;
            candidate.file_size          = seg.file_size;
            candidate.dirty_ratio        = dirty;
            candidate.max_timestamp      = seg.max_timestamp;
            candidate.estimated_savings  = estimate_reclaimed_bytes(seg, dirty);

            candidates.push(std::move(candidate));
        }
    }

    return candidates;
}

// ============================================================================
// Segment compaction execution
// ============================================================================

/**
 * Execute compaction on a single candidate segment.
 * Delegates to LogManager::compact_segment() and returns the actual
 * bytes reclaimed.
 */
[[nodiscard]] int64_t execute_compaction(
    const CandidateSegment& candidate,
    PartitionManager& pm)
{
    auto* log = pm.get_log_manager(candidate.topic, candidate.partition_id);
    if (!log) {
        CM_LOG_WARN("Compaction failed: partition {}/{} not found",
                    candidate.topic, candidate.partition_id);
        return 0;
    }

    CM_LOG_DEBUG("Compacting segment {}.{} (dirty_ratio={:.2f}, size={} bytes)",
                candidate.partition_id, candidate.segment_id,
                candidate.dirty_ratio, candidate.file_size);

    // In production, this calls:
    //   auto result = log->compact_segment(candidate.segment_id);
    //   return result.ok() ? result.bytes_reclaimed : 0
    //
    // For the stub, return estimated savings.
    int64_t reclaimed = candidate.estimated_savings;

    CM_LOG_TRACE("Segment {}.{} compacted: {} bytes reclaimed (estimated)",
                candidate.partition_id, candidate.segment_id, reclaimed);

    return reclaimed;
}

} // anonymous namespace

// ============================================================================
// CompactionManager::Impl — PIMPL
// ============================================================================

struct CompactionManager::Impl {
    /// Background scheduling thread.
    std::thread scheduler_thread;
    std::atomic<bool> scheduler_running{false};

    /// Worker threads for parallel compaction.
    std::vector<std::thread> worker_threads;
    std::atomic<bool> workers_running{false};

    /// Running flag.
    std::atomic<bool> running{false};

    /// Scheduler configuration.
    CompactionSchedulerConfig scheduler_config;

    /// Whether the scheduler should run.
    std::atomic<bool> should_run{true};

    /// Cumulative statistics.
    CompactionCycleStats cumulative_stats;
    mutable std::mutex stats_mutex;

    /// Timestamp of last stats log.
    std::chrono::steady_clock::time_point last_stats_log;

    /// Queue of candidates for the current cycle (processed by workers).
    std::priority_queue<CandidateSegment> candidate_queue;
    mutable std::mutex queue_mutex;
    std::condition_variable queue_cv;

    /// Number of active compactions in progress.
    std::atomic<int32_t> active_compactions{0};
};

// ============================================================================
// CompactionManager — Constructor / Destructor
// ============================================================================

CompactionManager::CompactionManager(BrokerServer& s)
    : server_(&s)
    , impl_(std::make_unique<Impl>())
{
    impl_->last_stats_log = std::chrono::steady_clock::now();
    CM_LOG_INFO("CompactionManager initialized");
}

CompactionManager::~CompactionManager() {
    if (impl_->running.load(std::memory_order_acquire)) {
        try { shutdown(); } catch (...) {}
    }
    CM_LOG_INFO("CompactionManager destroyed");
}

// ============================================================================
// CompactionManager — start()
// ============================================================================

void CompactionManager::start() {
    if (impl_->running.load(std::memory_order_acquire)) {
        CM_LOG_WARN("CompactionManager::start() called while already running");
        return;
    }

    impl_->scheduler_config = resolve_scheduler_config(server_->config());

    // Start the scheduler thread.
    impl_->scheduler_running.store(true, std::memory_order_release);
    impl_->scheduler_thread = std::thread(&CompactionManager::scheduler_loop, this);

    // Start worker threads.
    impl_->workers_running.store(true, std::memory_order_release);
    size_t num_workers = std::max(size_t{1}, impl_->scheduler_config.num_threads);
    impl_->worker_threads.reserve(num_workers);
    for (size_t i = 0; i < num_workers; ++i) {
        impl_->worker_threads.emplace_back(
            &CompactionManager::worker_loop, this, i);
    }

    impl_->running.store(true, std::memory_order_release);
    CM_LOG_INFO("CompactionManager started ({} scheduler, {} workers, "
                "min_dirty={:.2f})",
                1, num_workers, impl_->scheduler_config.min_cleanable_dirty_ratio);
}

// ============================================================================
// CompactionManager — shutdown()
// ============================================================================

void CompactionManager::shutdown() {
    if (!impl_->running.load(std::memory_order_acquire)) {
        return;
    }

    CM_LOG_INFO("CompactionManager shutting down");
    impl_->should_run.store(false, std::memory_order_release);

    // Wake workers.
    impl_->queue_cv.notify_all();

    impl_->scheduler_running.store(false, std::memory_order_release);
    if (impl_->scheduler_thread.joinable()) {
        impl_->scheduler_thread.join();
    }

    impl_->workers_running.store(false, std::memory_order_release);
    impl_->queue_cv.notify_all();
    for (auto& t : impl_->worker_threads) {
        if (t.joinable()) t.join();
    }
    impl_->worker_threads.clear();

    impl_->running.store(false, std::memory_order_release);
    CM_LOG_INFO("CompactionManager shutdown complete");
}

// ============================================================================
// CompactionManager — scheduler_loop()
// ============================================================================

void CompactionManager::scheduler_loop() {
    CM_LOG_INFO("Compaction scheduler thread started");

    while (impl_->scheduler_running.load(std::memory_order_acquire) &&
           impl_->should_run.load(std::memory_order_acquire)) {

        auto sleep_duration = std::chrono::milliseconds(
            impl_->scheduler_config.check_interval_ms);
        std::this_thread::sleep_for(sleep_duration);

        if (!impl_->should_run.load(std::memory_order_acquire)) break;

        run_scheduler_cycle();
    }

    CM_LOG_INFO("Compaction scheduler thread stopped");
}

// ============================================================================
// CompactionManager — worker_loop()
// ============================================================================

void CompactionManager::worker_loop(size_t worker_id) {
    CM_LOG_INFO("Compaction worker {} started", worker_id);

    while (impl_->workers_running.load(std::memory_order_acquire)) {
        CandidateSegment candidate;

        {
            std::unique_lock<std::mutex> lock(impl_->queue_mutex);
            impl_->queue_cv.wait(lock, [this]() {
                return !impl_->candidate_queue.empty() ||
                       !impl_->workers_running.load(std::memory_order_acquire);
            });

            if (!impl_->workers_running.load(std::memory_order_acquire)) break;
            if (impl_->candidate_queue.empty()) continue;

            candidate = impl_->candidate_queue.top();
            impl_->candidate_queue.pop();
            impl_->active_compactions.fetch_add(1, std::memory_order_relaxed);
        }

        // Execute compaction on the candidate.
        auto& pm = server_->partition_manager();
        int64_t reclaimed = execute_compaction(candidate, pm);

        impl_->active_compactions.fetch_sub(1, std::memory_order_relaxed);

        if (reclaimed > 0) {
            std::lock_guard<std::mutex> lock(impl_->stats_mutex);
            impl_->cumulative_stats.segments_compacted++;
            impl_->cumulative_stats.bytes_reclaimed += reclaimed;
        }

        // Throttle between compactions.
        if (impl_->scheduler_config.backoff_ms > 0) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(impl_->scheduler_config.backoff_ms));
        }
    }

    CM_LOG_INFO("Compaction worker {} stopped", worker_id);
}

// ============================================================================
// CompactionManager — run_scheduler_cycle()
// ============================================================================

void CompactionManager::run_scheduler_cycle() {
    CM_LOG_DEBUG("Starting compaction scheduler cycle");

    CompactionCycleStats cycle_stats;
    auto cycle_start = std::chrono::steady_clock::now();

    auto& pm = server_->partition_manager();
    auto& tm = server_->topic_manager();

    // --- Phase 1: Build a snapshot of hosted partitions ---
    // In production, this comes from PartitionManager.
    auto topics = tm.list_topics();
    std::vector<PartitionSnapshot> partitions_snapshot;

    for (const auto& topic_meta : topics) {
        // Only consider topics with compaction policy.
        if (topic_meta.config.policy != cleanup_policy::compact_only &&
            topic_meta.config.policy != cleanup_policy::compact_and_delete) {
            continue;
        }

        for (const auto& part_meta : topic_meta.partitions) {
            if (!pm.is_leader(topic_meta.name, part_meta.partition_id)) {
                continue;  // Only leaders run compaction.
            }
            partitions_snapshot.push_back({
                topic_meta.name, part_meta.partition_id, true});
        }
    }

    cycle_stats.partitions_scanned = partitions_snapshot.size();

    // --- Phase 2: Build candidate priority queue ---
    auto candidates = collect_candidates(
        partitions_snapshot, pm, impl_->scheduler_config);
    cycle_stats.segments_evaluated = candidates.size();

    // --- Phase 3: Push candidates to worker queue ---
    {
        std::unique_lock<std::mutex> lock(impl_->queue_mutex);
        // Cap the number of candidates enqueued.
        size_t enqueued = 0;
        while (!candidates.empty() && enqueued < kMaxSegmentsPerCycle) {
            impl_->candidate_queue.push(std::move(candidates.top()));
            candidates.pop();
            enqueued++;
        }

        if (enqueued > 0) {
            CM_LOG_DEBUG("Enqueued {} compaction candidates", enqueued);
        }
    }

    // Wake workers.
    impl_->queue_cv.notify_all();

    auto cycle_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - cycle_start).count();
    cycle_stats.cycle_time_ms = cycle_elapsed;

    // --- Phase 4: Periodic stats logging ---
    auto now = std::chrono::steady_clock::now();
    if (now - impl_->last_stats_log > kStatsLogInterval) {
        std::lock_guard<std::mutex> lock(impl_->stats_mutex);
        CM_LOG_INFO("Compaction summary: segments_compacted={}, bytes_reclaimed={}, "
                    "queued={}, scanned_partitions={}",
                    impl_->cumulative_stats.segments_compacted,
                    impl_->cumulative_stats.bytes_reclaimed,
                    candidates.size(),
                    cycle_stats.partitions_scanned);
        impl_->last_stats_log = now;
    }

    CM_LOG_DEBUG("Compaction scheduler cycle complete: {} segments evaluated "
                 "in {}ms",
                 cycle_stats.segments_evaluated, cycle_elapsed);
}

// ============================================================================
// CompactionManager — compact_partition()
// ============================================================================

void CompactionManager::compact_partition(
    const std::string& topic,
    partition_id_t partition)
{
    CM_LOG_INFO("Compaction requested for partition {}/{}", topic, partition);

    auto& pm = server_->partition_manager();
    auto& tm = server_->topic_manager();

    // Validate the partition exists and we are the leader.
    if (!pm.is_leader(topic, partition)) {
        CM_LOG_WARN("Cannot compact {}/{} - not leader", topic, partition);
        return;
    }

    auto* log = pm.get_log_manager(topic, partition);
    if (!log) {
        CM_LOG_WARN("Cannot compact {}/{} - LogManager not found", topic, partition);
        return;
    }

    // Get topic config for policy check.
    auto topic_meta = tm.get_topic(topic);
    if (topic_meta.failed()) {
        CM_LOG_WARN("Cannot compact {}/{} - topic metadata not found", topic, partition);
        return;
    }

    if (topic_meta.value.config.policy != cleanup_policy::compact_only &&
        topic_meta.value.config.policy != cleanup_policy::compact_and_delete) {
        CM_LOG_WARN("Cannot compact {}/{} - cleanup policy is not compaction",
                    topic, partition);
        return;
    }

    // Run compaction on all eligible segments in this partition.
    auto segments = log->list_segments();
    size_t compacted_count = 0;
    int64_t total_reclaimed = 0;

    for (const auto& seg : segments) {
        if (seg.is_active) continue;

        double dirty = estimate_dirty_ratio(seg, segments, topic_meta.value.config);
        if (dirty < impl_->scheduler_config.min_cleanable_dirty_ratio) {
            continue;
        }

        CandidateSegment candidate;
        candidate.topic              = topic;
        candidate.partition_id       = partition;
        candidate.segment_id         = seg.segment_id;
        candidate.base_offset        = seg.base_offset;
        candidate.next_offset        = seg.next_offset;
        candidate.file_size          = seg.file_size;
        candidate.dirty_ratio        = dirty;
        candidate.max_timestamp      = seg.max_timestamp;
        candidate.estimated_savings  = estimate_reclaimed_bytes(seg, dirty);

        int64_t reclaimed = execute_compaction(candidate, pm);
        if (reclaimed > 0) {
            compacted_count++;
            total_reclaimed += reclaimed;
        }
    }

    CM_LOG_INFO("Manual compaction of {}/{} complete: {} segments, "
                "~{} bytes reclaimed",
                topic, partition, compacted_count, total_reclaimed);
}

// ============================================================================
// CompactionManager — get_stats()
// ============================================================================

CompactionManager::compaction_stats CompactionManager::get_stats() const {
    compaction_stats stats;
    std::lock_guard<std::mutex> lock(impl_->stats_mutex);
    stats.segments_compacted = impl_->cumulative_stats.segments_compacted;
    stats.bytes_reclaimed    = impl_->cumulative_stats.bytes_reclaimed;
    stats.active_compactions = impl_->active_compactions.load(std::memory_order_acquire);

    {
        std::lock_guard<std::mutex> q_lock(impl_->queue_mutex);
        stats.queued_candidates = impl_->candidate_queue.size();
    }

    return stats;
}

} // namespace torrent::broker
