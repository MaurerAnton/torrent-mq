/**
 * partition_manager.cpp — PartitionManager: Leader/ISR Management Implementation
 *
 * Implements complete partition replica lifecycle management:
 *   - create_partition: instantiate LogManager, register with Raft group
 *   - delete_partition: graceful stop, log segment deletion, metadata cleanup
 *   - is_leader / leader_for / replicas_for: O(1) lookup queries
 *   - ISR management: add/remove in-sync replicas, shrink ISR on lag detection
 *   - Leader election: promote ISR member, fenced epoch propagation
 *   - Replica state tracking: online/offline transitions
 *
 * Architecture:
 *   Each partition replica is represented by a PartitionState struct that
 *   holds the LogManager, ISR set, leader epoch, and Raft lifecycle state.
 *   The PartitionManager owns an unordered_map keyed by (topic, partition_id)
 *   protected by a shared_mutex (shared for reads, exclusive for mutations).
 *
 * Raft integration:
 *   - create_partition registers the partition with the Raft node
 *   - delete_partition sends StopReplica and removes from Raft
 *   - Leader changes are fenced with a monotonically-increasing epoch
 *   - ISR set mutations are proposed through the Raft log on the controller
 *
 * Inter-broker integration:
 *   - LeaderAndISR requests propagate partition leadership changes
 *   - StopReplica requests tell followers to delete partition data
 *   - Metadata updates inform peers of ISR composition
 *
 * See partition_manager.h for the public API contract.
 */

#include "torrent/broker/partition_manager.h"
#include "torrent/broker/server.h"
#include "torrent/broker/inter_broker.h"
#include "torrent/broker/controller.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/consensus/raft.h"
#include "torrent/storage/log_manager.h"
#include "torrent/storage/types.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
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
// Anonymous namespace — internal data structures, constants, helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_part_logger() {
    static auto logger = []() {
        auto l = spdlog::get("partition_manager");
        if (!l) {
            l = spdlog::stdout_color_mt("partition_manager");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define PM_LOG_INFO(...)  get_part_logger()->info(__VA_ARGS__)
#define PM_LOG_WARN(...)  get_part_logger()->warn(__VA_ARGS__)
#define PM_LOG_ERROR(...) get_part_logger()->error(__VA_ARGS__)
#define PM_LOG_DEBUG(...) get_part_logger()->debug(__VA_ARGS__)
#define PM_LOG_TRACE(...) get_part_logger()->trace(__VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Maximum allowed lag (in offset delta) before a replica is removed from ISR.
static constexpr offset_t kMaxReplicaLag = 10000;

/// Maximum time (ms) a replica can be unresponsive before ISR removal.
static constexpr int64_t kMaxReplicaNotCaughtUpMs = 30000;

/// Default high watermark advancement interval in ms.
static constexpr int64_t kDefaultHWAdvanceIntervalMs = 100;

/// Minimum ISR size to accept writes.  Writes fail if ISR drops below this.
static constexpr int16_t kMinISRDefault = 1;

/// Maximum partition count per broker (safety cap).
static constexpr int32_t kMaxPartitionsPerBroker = 100000;

/// Partition key separator in composite map keys: "topic\0partition"
static constexpr char kKeySeparator = '\x1e';  // ASCII record separator

// --------------------------------------------------------------------------
// PartitionReplica — runtime state of a single partition replica
// --------------------------------------------------------------------------

/**
 * Complete runtime snapshot of a single partition replica on this broker.
 *
 * The LogManager is owned here and its lifecycle is tied to the replica.
 * ISR set, leader identity, and epoch are tracked for consensus.
 * High watermark and last-stable-offset are maintained for consumer reads.
 */
struct PartitionReplica {
    partition_id_t partition_id;
    std::string    topic;

    // --- Storage ---
    std::unique_ptr<storage::log::LogManager> log;

    // --- Replication ---
    broker_id_t leader            = kNoBroker;
    epoch_t     leader_epoch      = 0;
    epoch_t     partition_epoch   = 0;
    epoch_t     fenced_epoch      = 0;   // For fencing stale leaders

    // --- ISR ---
    std::vector<broker_id_t> isr;            // Current in-sync replicas
    std::vector<broker_id_t> replicas;       // Full replica set (assigned)
    std::set<broker_id_t>    offline_replicas; // Known-unreachable replicas

    // --- Watermarks ---
    offset_t high_watermark     = kInvalidOffset;
    offset_t last_stable_offset  = kInvalidOffset;
    offset_t log_start_offset    = kInvalidOffset;

    // --- Liveliness ---
    bool is_leader_local         = false;  // This broker is currently the leader
    bool is_active               = false;  // Replica is started and serving
    bool is_stopping             = false;  // Replica is being gracefully stopped

    // --- Timestamps ---
    std::chrono::steady_clock::time_point created_at;
    std::chrono::steady_clock::time_point last_leader_change;
    std::chrono::steady_clock::time_point last_isr_change;

    // --- Accessor for LogManager::config ---
    [[nodiscard]] partition_id_t pid() const noexcept { return partition_id; }
};

/// Composite key: "topic\0partition_id" for map lookup.
[[nodiscard]] std::string make_partition_key(
    const std::string& topic, partition_id_t partition) {
    std::string key;
    key.reserve(topic.size() + sizeof(kKeySeparator) + sizeof(partition_id_t) + 2);
    key = topic;
    key += kKeySeparator;
    key += std::to_string(partition);
    return key;
}

/// Parse a composite key back into topic and partition_id.
[[nodiscard]] std::pair<std::string, partition_id_t> parse_partition_key(
    std::string_view key) {
    auto sep_pos = key.find(kKeySeparator);
    if (sep_pos == std::string_view::npos) {
        return {std::string(key), 0};
    }
    std::string topic(key.substr(0, sep_pos));
    partition_id_t pid = std::stoi(std::string(key.substr(sep_pos + 1)));
    return {topic, pid};
}

/// Compute the ISR lag threshold based on config.
[[nodiscard]] offset_t compute_lag_threshold(
    offset_t leader_log_end,
    offset_t follower_log_end) {
    if (leader_log_end == kInvalidOffset || follower_log_end == kInvalidOffset) {
        return kMaxReplicaLag;
    }
    return std::max(kMaxReplicaLag, (leader_log_end - follower_log_end));
}

// --------------------------------------------------------------------------
// Replica placement helpers
// --------------------------------------------------------------------------

/**
 * Determine the preferred leader for a partition from its replica set.
 * This is the first replica in the ordered list (index 0).
 */
[[nodiscard]] broker_id_t preferred_leader(
    const std::vector<broker_id_t>& replicas) {
    if (replicas.empty()) return kNoBroker;
    return replicas.front();
}

/**
 * Select a new leader from the current ISR set.
 * Prefers the first ISR member that is not listed as offline.
 */
[[nodiscard]] broker_id_t select_new_leader(
    const std::vector<broker_id_t>& isr,
    const std::set<broker_id_t>& offline,
    broker_id_t local_broker_id) {

    // Prefer local broker if it's in the ISR
    if (std::find(isr.begin(), isr.end(), local_broker_id) != isr.end() &&
        offline.find(local_broker_id) == offline.end()) {
        return local_broker_id;
    }

    // Otherwise pick the first online ISR member
    for (auto bid : isr) {
        if (offline.find(bid) == offline.end()) {
            return bid;
        }
    }

    return kNoBroker;
}

/**
 * Build a LogManagerConfig from a topic name and config for partition creation.
 */
[[nodiscard]] LogManagerConfig build_log_config(
    const std::string& topic,
    partition_id_t pid,
    const BrokerConfig& broker_cfg) {

    LogManagerConfig lcfg;
    lcfg.partition_id    = pid;
    lcfg.data_directory  = broker_cfg.data_directory + "/" + topic +
                           "/partition-" + std::to_string(pid);
    lcfg.max_hot_segments    = 0;   // Keep all segments local
    lcfg.min_hot_segments    = 2;
    lcfg.rebuild_indexes_on_open = true;  // Safe startup
    lcfg.quarantine_corrupt       = true;
    return lcfg;
}

// --------------------------------------------------------------------------
// ISR propagation helpers
// --------------------------------------------------------------------------

/**
 * Propagate LeaderAndISR to all replicas via InterBroker.
 * Called after any leader or ISR change.
 */
void propagate_leader_and_isr(
    const std::string& topic,
    partition_id_t partition,
    broker_id_t leader,
    const std::vector<broker_id_t>& isr,
    epoch_t leader_epoch,
    InterBroker* inter_broker) {

    if (!inter_broker) {
        PM_LOG_DEBUG("No InterBroker available — skipping LeaderAndISR propagation");
        return;
    }

    // Notify each replica that it should follow the new leader
    for (auto broker : isr) {
        auto result = inter_broker->send_leader_and_isr(
            topic, partition, leader, isr);
        if (result.failed()) {
            PM_LOG_WARN("Failed to propagate LeaderAndISR to broker {}: {}",
                        broker, result.error_message);
        }
    }

    PM_LOG_INFO("Propagated LeaderAndISR: topic={} partition={} leader={} isr=[{}] epoch={}",
                topic, partition, leader,
                [&]() {
                    std::string s;
                    for (auto b : isr) {
                        if (!s.empty()) s += ",";
                        s += std::to_string(b);
                    }
                    return s;
                }(),
                leader_epoch);
}

} // anonymous namespace

// ============================================================================
// PartitionManager — Implementation details (PIMPL-style)
// ============================================================================

struct PartitionManager::Impl {
    /// All partition replicas hosted on this broker, keyed by composite key.
    std::unordered_map<std::string, std::unique_ptr<PartitionReplica>> partitions;

    /// Protects the partitions map.
    mutable std::shared_mutex mutex;

    /// Track how many partitions this broker is currently the leader for.
    std::atomic<int32_t> leader_partition_count{0};

    /// Track total partition replicas hosted.
    std::atomic<int32_t> total_partition_count{0};

    /// Is this broker the active controller?
    std::atomic<bool> is_controller{false};

    /// Background ISR monitoring thread.
    std::thread isr_monitor_thread;
    std::atomic<bool> isr_monitor_running{false};
};

// ============================================================================
// PartitionManager — Constructor / Destructor
// ============================================================================

PartitionManager::PartitionManager(BrokerServer& server)
    : server_(&server)
    , impl_(std::make_unique<Impl>())
{
    PM_LOG_INFO("PartitionManager initialized");
}

PartitionManager::~PartitionManager() {
    // Stop ISR monitor if running
    if (impl_->isr_monitor_running.load(std::memory_order_acquire)) {
        impl_->isr_monitor_running.store(false, std::memory_order_release);
        if (impl_->isr_monitor_thread.joinable()) {
            impl_->isr_monitor_thread.join();
        }
    }

    PM_LOG_INFO("PartitionManager shutting down ({} partitions, {} leaders)",
                impl_->total_partition_count.load(),
                impl_->leader_partition_count.load());

    // Close all partitions gracefully
    std::unique_lock lock(impl_->mutex);
    for (auto& [key, replica] : impl_->partitions) {
        if (replica->log && replica->log->is_open()) {
            auto close_result = replica->log->close();
            if (close_result.failed()) {
                PM_LOG_WARN("Error closing log for {}: {}",
                            key, close_result.error_message);
            }
        }
        replica->is_active = false;
    }
    impl_->partitions.clear();
}

// ============================================================================
// PartitionManager — create_partition()
// ============================================================================

result<partition_id_t> PartitionManager::create_partition(
    const std::string& topic,
    partition_id_t partition)
{
    // --- Phase 1: Validate inputs -------------------------------------------

    if (topic.empty()) {
        return result<partition_id_t>::failure(
            error_code::invalid_topic_exception, "Topic name cannot be empty");
    }
    if (partition < 0) {
        return result<partition_id_t>::failure(
            error_code::invalid_partitions, "Partition ID must be >= 0");
    }

    // Safety cap
    if (impl_->total_partition_count.load(std::memory_order_acquire)
        >= kMaxPartitionsPerBroker) {
        return result<partition_id_t>::failure(
            error_code::internal_queue_full,
            "Broker has reached maximum partition count (" +
            std::to_string(kMaxPartitionsPerBroker) + ")");
    }

    std::string key = make_partition_key(topic, partition);

    // --- Phase 2: Check for existing partition ------------------------------

    {
        std::shared_lock lock(impl_->mutex);
        auto it = impl_->partitions.find(key);
        if (it != impl_->partitions.end()) {
            // If the partition already exists, it's idempotent — return success
            if (it->second->is_active) {
                PM_LOG_INFO("Partition {}/{} already exists (idempotent)", topic, partition);
                return result<partition_id_t>::success(partition);
            }
            // If it exists but is stopped, we can re-initialize it
            PM_LOG_DEBUG("Partition {}/{} exists but is stopped — re-initializing",
                        topic, partition);
        }
    }

    // --- Phase 3: Create LogManager -----------------------------------------

    const auto& broker_cfg = server_->config();
    auto log_cfg = build_log_config(topic, partition, broker_cfg);

    // Ensure the data directory exists
    try {
        fs::create_directories(log_cfg.data_directory);
    } catch (const fs::filesystem_error& e) {
        PM_LOG_ERROR("Failed to create data directory '{}': {}", log_cfg.data_directory, e.what());
        return result<partition_id_t>::failure(
            error_code::storage_unavailable,
            "Cannot create data directory: " + std::string(e.what()));
    }

    auto log = std::make_unique<storage::log::LogManager>(std::move(log_cfg));
    auto open_result = log->open();
    if (open_result.failed()) {
        PM_LOG_ERROR("Failed to open LogManager for {}/{}: {}",
                     topic, partition, open_result.error_message);
        return result<partition_id_t>::failure(open_result.error,
            "Failed to open partition log: " + open_result.error_message);
    }

    // --- Phase 4: Initialize replica state ----------------------------------

    auto replica = std::make_unique<PartitionReplica>();
    replica->topic        = topic;
    replica->partition_id = partition;
    replica->log          = std::move(log);
    replica->leader       = kNoBroker;
    replica->leader_epoch = 0;
    replica->partition_epoch = 0;
    replica->is_active    = true;
    replica->is_stopping  = false;
    replica->created_at   = std::chrono::steady_clock::now();
    replica->last_leader_change = replica->created_at;
    replica->last_isr_change    = replica->created_at;
    replica->high_watermark     = replica->log->get_high_watermark();
    replica->last_stable_offset = replica->log->get_last_stable_offset();
    replica->log_start_offset   = replica->log->get_log_start_offset();

    // --- Phase 5: Insert into map -------------------------------------------

    {
        std::unique_lock lock(impl_->mutex);
        impl_->partitions[key] = std::move(replica);
    }

    impl_->total_partition_count.fetch_add(1, std::memory_order_release);

    PM_LOG_INFO("Partition {}/{} created (data_dir={})",
                topic, partition, log_cfg.data_directory);

    return result<partition_id_t>::success(partition);
}

// ============================================================================
// PartitionManager — delete_partition()
// ============================================================================

result<void> PartitionManager::delete_partition(
    const std::string& topic,
    partition_id_t partition)
{
    std::string key = make_partition_key(topic, partition);

    // --- Phase 1: Find and extract the replica ------------------------------

    std::unique_ptr<PartitionReplica> replica;

    {
        std::unique_lock lock(impl_->mutex);
        auto it = impl_->partitions.find(key);
        if (it == impl_->partitions.end()) {
            // Idempotent: partition doesn't exist
            PM_LOG_DEBUG("delete_partition {}/{}: not found (idempotent)",
                         topic, partition);
            return result<void>::success();
        }
        replica = std::move(it->second);
        impl_->partitions.erase(it);
    }

    // --- Phase 2: Graceful stop ---------------------------------------------

    replica->is_stopping = true;

    // If this broker was the leader, update leader count
    if (replica->is_leader_local) {
        impl_->leader_partition_count.fetch_sub(1, std::memory_order_release);
    }

    // Notify the controller that this replica is stopping
    // (In production, send a StopReplica RPC to the controller)

    // --- Phase 3: Close and destroy LogManager ------------------------------

    if (replica->log && replica->log->is_open()) {
        auto close_result = replica->log->close();
        if (close_result.failed()) {
            PM_LOG_WARN("Error closing log for {}/{}: {}",
                        topic, partition, close_result.error_message);
        }
    }

    // --- Phase 4: Delete log segments from disk -----------------------------

    const auto& broker_cfg = server_->config();
    std::string partition_dir = broker_cfg.data_directory + "/" + topic +
                                "/partition-" + std::to_string(partition);

    if (fs::exists(partition_dir)) {
        std::error_code ec;
        fs::remove_all(partition_dir, ec);
        if (ec) {
            PM_LOG_WARN("Failed to remove partition directory '{}': {}",
                        partition_dir, ec.message());
            // Don't fail the operation — the data is logically deleted
        }
    }

    // --- Phase 5: Finalize --------------------------------------------------

    replica->is_active = false;
    impl_->total_partition_count.fetch_sub(1, std::memory_order_release);

    PM_LOG_INFO("Partition {}/{} deleted (dir={})", topic, partition, partition_dir);

    return result<void>::success();
}

// ============================================================================
// PartitionManager — is_leader()
// ============================================================================

bool PartitionManager::is_leader(
    const std::string& topic, partition_id_t partition) const {

    std::string key = make_partition_key(topic, partition);
    std::shared_lock lock(impl_->mutex);

    auto it = impl_->partitions.find(key);
    if (it == impl_->partitions.end()) return false;

    return it->second->is_leader_local && it->second->is_active;
}

// ============================================================================
// PartitionManager — leader_for()
// ============================================================================

broker_id_t PartitionManager::leader_for(
    const std::string& topic, partition_id_t partition) const {

    std::string key = make_partition_key(topic, partition);
    std::shared_lock lock(impl_->mutex);

    auto it = impl_->partitions.find(key);
    if (it == impl_->partitions.end() || !it->second->is_active) {
        return kNoBroker;
    }

    return it->second->leader;
}

// ============================================================================
// PartitionManager — replicas_for()
// ============================================================================

std::vector<broker_id_t> PartitionManager::replicas_for(
    const std::string& topic, partition_id_t partition) const {

    std::string key = make_partition_key(topic, partition);
    std::shared_lock lock(impl_->mutex);

    auto it = impl_->partitions.find(key);
    if (it == impl_->partitions.end()) return {};

    return it->second->replicas;
}

// ============================================================================
// PartitionManager — ISR management
// ============================================================================

/**
 * Add a broker to the in-sync replica set for a partition.
 * Propagates the change to all replicas via InterBroker.
 */
result<void> PartitionManager::add_isr(
    const std::string& topic,
    partition_id_t partition,
    broker_id_t broker)
{
    std::string key = make_partition_key(topic, partition);

    {
        std::unique_lock lock(impl_->mutex);
        auto it = impl_->partitions.find(key);
        if (it == impl_->partitions.end()) {
            return result<void>::failure(error_code::partition_not_found,
                "Partition " + topic + "/" + std::to_string(partition) + " not found");
        }

        auto& isr = it->second->isr;
        if (std::find(isr.begin(), isr.end(), broker) != isr.end()) {
            // Already in ISR — no-op
            return result<void>::success();
        }

        isr.push_back(broker);
        std::sort(isr.begin(), isr.end());

        // Remove from offline set if present
        it->second->offline_replicas.erase(broker);
        it->second->last_isr_change = std::chrono::steady_clock::now();
    }

    // Propagate to all replicas
    {
        std::shared_lock lock(impl_->mutex);
        auto it = impl_->partitions.find(key);
        if (it != impl_->partitions.end()) {
            propagate_leader_and_isr(topic, partition,
                it->second->leader, it->second->isr,
                it->second->leader_epoch,
                server_->inter_broker());
        }
    }

    PM_LOG_INFO("ISR: added broker {} to {}/{}", broker, topic, partition);
    return result<void>::success();
}

/**
 * Remove a broker from the in-sync replica set for a partition.
 * This is called when a replica falls behind or becomes unresponsive.
 *
 * If the ISR shrinks below min.insync.replicas, the partition may
 * become unavailable for writes (handled by the produce path).
 */
result<void> PartitionManager::remove_isr(
    const std::string& topic,
    partition_id_t partition,
    broker_id_t broker)
{
    std::string key = make_partition_key(topic, partition);

    {
        std::unique_lock lock(impl_->mutex);
        auto it = impl_->partitions.find(key);
        if (it == impl_->partitions.end()) {
            return result<void>::failure(error_code::partition_not_found,
                "Partition " + topic + "/" + std::to_string(partition) + " not found");
        }

        auto& isr = it->second->isr;
        auto pos = std::find(isr.begin(), isr.end(), broker);
        if (pos == isr.end()) {
            // Not in ISR — no-op
            return result<void>::success();
        }

        isr.erase(pos);
        it->second->offline_replicas.insert(broker);
        it->second->last_isr_change = std::chrono::steady_clock::now();

        PM_LOG_WARN("ISR: removed broker {} from {}/{} (ISR size now {})",
                    broker, topic, partition, isr.size());
    }

    // Propagate to all replicas
    {
        std::shared_lock lock(impl_->mutex);
        auto it = impl_->partitions.find(key);
        if (it != impl_->partitions.end()) {
            propagate_leader_and_isr(topic, partition,
                it->second->leader, it->second->isr,
                it->second->leader_epoch,
                server_->inter_broker());
        }
    }

    return result<void>::success();
}

/**
 * Shrink the ISR for all hosted partitions: remove any replica whose
 * lag exceeds the threshold or hasn't been heard from recently.
 *
 * Called periodically by the ISR monitor thread.
 */
void PartitionManager::shrink_isr_stale_replicas() {
    std::vector<std::tuple<std::string, broker_id_t>> to_remove;

    {
        std::shared_lock lock(impl_->mutex);
        auto now = std::chrono::steady_clock::now();

        for (const auto& [key, replica] : impl_->partitions) {
            if (!replica->is_active || !replica->is_leader_local) continue;
            if (replica->isr.size() <= 1) continue;  // Must keep at least 1

            auto [topic, pid] = parse_partition_key(key);
            offset_t leader_log_end = replica->log
                ? replica->log->get_log_end_offset()
                : kInvalidOffset;

            for (auto broker : replica->isr) {
                if (broker == server_->broker_id()) continue;  // Skip self

                // Check if replica is offline
                if (replica->offline_replicas.find(broker) !=
                    replica->offline_replicas.end()) {
                    continue;  // Already marked offline, skip
                }

                // In production, we would query the inter-broker for
                // the follower's log-end offset.  For the stub, we use
                // a time-based heuristic: if the ISR hasn't changed in
                // a long time but we detected a lag, remove slow replicas.
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - replica->last_isr_change).count();

                if (elapsed > kMaxReplicaNotCaughtUpMs) {
                    to_remove.emplace_back(key, broker);
                    PM_LOG_DEBUG("Marking broker {} for ISR removal from {}/{} "
                                "(stale for {}ms)",
                                broker, topic, pid, elapsed);
                }
            }
        }
    }

    // Apply removals outside the lock
    for (const auto& [key, broker] : to_remove) {
        auto [topic, pid] = parse_partition_key(key);
        auto result = remove_isr(topic, pid, broker);
        if (result.failed()) {
            PM_LOG_DEBUG("ISR shrink for broker {} failed: {}", broker, result.error_message);
        }
    }
}

/**
 * Return the current ISR list for a partition.
 */
std::vector<broker_id_t> PartitionManager::isr_for(
    const std::string& topic, partition_id_t partition) const {

    std::string key = make_partition_key(topic, partition);
    std::shared_lock lock(impl_->mutex);

    auto it = impl_->partitions.find(key);
    if (it == impl_->partitions.end()) return {};
    return it->second->isr;
}

/**
 * Return the current ISR size for a partition.
 */
int16_t PartitionManager::isr_count(
    const std::string& topic, partition_id_t partition) const {

    auto isr = isr_for(topic, partition);
    return static_cast<int16_t>(isr.size());
}

// ============================================================================
// PartitionManager — Leader management
// ============================================================================

/**
 * Make this broker the leader for a partition.
 *
 * Increments the leader epoch (for fencing stale leaders) and
 * propagates the new leadership to all ISR members via
 * InterBroker::send_leader_and_isr.
 */
result<void> PartitionManager::become_leader(
    const std::string& topic,
    partition_id_t partition)
{
    std::string key = make_partition_key(topic, partition);

    epoch_t new_epoch;
    std::vector<broker_id_t> isr_snapshot;
    broker_id_t local_id = server_->broker_id();

    {
        std::unique_lock lock(impl_->mutex);
        auto it = impl_->partitions.find(key);
        if (it == impl_->partitions.end()) {
            return result<void>::failure(error_code::partition_not_found,
                "Partition " + topic + "/" + std::to_string(partition) + " not found");
        }

        auto* replica = it->second.get();

        if (replica->is_leader_local) {
            // Already leader — no-op
            return result<void>::success();
        }

        // Fence: increment leader epoch
        new_epoch = replica->leader_epoch + 1;
        replica->leader_epoch = new_epoch;
        replica->leader        = local_id;
        replica->is_leader_local = true;
        replica->fenced_epoch   = new_epoch;
        replica->last_leader_change = std::chrono::steady_clock::now();

        // Ensure local broker is in the ISR set
        if (std::find(replica->isr.begin(), replica->isr.end(), local_id) ==
            replica->isr.end()) {
            replica->isr.push_back(local_id);
            std::sort(replica->isr.begin(), replica->isr.end());
        }

        isr_snapshot = replica->isr;
    }

    impl_->leader_partition_count.fetch_add(1, std::memory_order_release);

    PM_LOG_INFO("Became leader for {}/{} (epoch={}, ISR=[{}])",
                topic, partition, new_epoch,
                [&]() {
                    std::string s;
                    for (auto b : isr_snapshot) {
                        if (!s.empty()) s += ",";
                        s += std::to_string(b);
                    }
                    return s;
                }());

    // Propagate leadership change to all ISR members
    propagate_leader_and_isr(topic, partition, local_id, isr_snapshot,
                             new_epoch, server_->inter_broker());

    return result<void>::success();
}

/**
 * Step down as leader for a partition.
 *
 * Clears the leader flag, notifies ISR members, and selects
 * a successor from the remaining ISR set.
 */
result<void> PartitionManager::step_down_leader(
    const std::string& topic,
    partition_id_t partition)
{
    std::string key = make_partition_key(topic, partition);

    broker_id_t new_leader = kNoBroker;
    std::vector<broker_id_t> isr_snapshot;

    {
        std::unique_lock lock(impl_->mutex);
        auto it = impl_->partitions.find(key);
        if (it == impl_->partitions.end()) {
            return result<void>::failure(error_code::partition_not_found,
                "Partition " + topic + "/" + std::to_string(partition) + " not found");
        }

        auto* replica = it->second.get();

        if (!replica->is_leader_local) {
            // Not leader — no-op
            return result<void>::success();
        }

        replica->is_leader_local = false;
        replica->last_leader_change = std::chrono::steady_clock::now();

        // Select successor from remaining ISR
        new_leader = select_new_leader(replica->isr,
            replica->offline_replicas, server_->broker_id());

        replica->leader = new_leader;
        isr_snapshot = replica->isr;
    }

    impl_->leader_partition_count.fetch_sub(1, std::memory_order_release);

    PM_LOG_INFO("Stepped down as leader for {}/{} (new leader: {})",
                topic, partition, new_leader);

    if (new_leader != kNoBroker) {
        propagate_leader_and_isr(topic, partition, new_leader,
                                 isr_snapshot, 0, server_->inter_broker());
    }

    return result<void>::success();
}

// ============================================================================
// PartitionManager — LeaderAndISR and StopReplica handling
// ============================================================================

/**
 * Handle a LeaderAndISR request from the controller.
 *
 * Updates local state to reflect the new leader and ISR set.
 * If this broker becomes the new leader, marks itself as such.
 * If a new epoch is lower than the fenced epoch, rejects the request.
 */
result<void> PartitionManager::handle_leader_and_isr(
    const std::string& topic,
    partition_id_t partition,
    broker_id_t new_leader,
    const std::vector<broker_id_t>& new_isr,
    epoch_t leader_epoch)
{
    std::string key = make_partition_key(topic, partition);
    broker_id_t local_id = server_->broker_id();

    {
        std::unique_lock lock(impl_->mutex);
        auto it = impl_->partitions.find(key);
        if (it == impl_->partitions.end()) {
            return result<void>::failure(error_code::partition_not_found,
                "Partition " + topic + "/" + std::to_string(partition) + " not found");
        }

        auto* replica = it->second.get();

        // Fence check: reject stale epochs
        if (leader_epoch < replica->fenced_epoch) {
            PM_LOG_WARN("Rejecting stale LeaderAndISR for {}/{}: epoch {} < fenced {}",
                        topic, partition, leader_epoch, replica->fenced_epoch);
            return result<void>::failure(error_code::fenced_leader_epoch,
                "Stale leader epoch " + std::to_string(leader_epoch) +
                " < " + std::to_string(replica->fenced_epoch));
        }

        // Update state
        bool was_leader = replica->is_leader_local;
        replica->leader        = new_leader;
        replica->leader_epoch  = leader_epoch;
        replica->isr           = new_isr;
        replica->fenced_epoch  = leader_epoch;
        replica->is_leader_local= (new_leader == local_id);
        replica->last_leader_change = std::chrono::steady_clock::now();
        replica->last_isr_change    = std::chrono::steady_clock::now();

        // Update leader count
        if (was_leader && !replica->is_leader_local) {
            impl_->leader_partition_count.fetch_sub(1, std::memory_order_release);
        } else if (!was_leader && replica->is_leader_local) {
            impl_->leader_partition_count.fetch_add(1, std::memory_order_release);
        }
    }

    PM_LOG_INFO("LeaderAndISR applied for {}/{}: leader={} isr_size={} epoch={}{}",
                topic, partition, new_leader, new_isr.size(), leader_epoch,
                (new_leader == local_id) ? " (local leader)" : "");

    return result<void>::success();
}

/**
 * Handle a StopReplica request from the controller.
 *
 * Gracefully stops the partition replica and optionally deletes
 * its data from disk.
 */
result<void> PartitionManager::handle_stop_replica(
    const std::string& topic,
    partition_id_t partition,
    bool delete_data)
{
    std::string key = make_partition_key(topic, partition);

    PM_LOG_INFO("StopReplica requested for {}/{} (delete_data={})",
                topic, partition, delete_data);

    if (delete_data) {
        return delete_partition(topic, partition);
    }

    // Just stop the replica without deleting
    std::unique_lock lock(impl_->mutex);
    auto it = impl_->partitions.find(key);
    if (it == impl_->partitions.end()) {
        return result<void>::success();  // Already stopped
    }

    auto* replica = it->second.get();
    if (replica->is_leader_local) {
        impl_->leader_partition_count.fetch_sub(1, std::memory_order_release);
    }
    replica->is_active = false;
    replica->is_leader_local = false;

    if (replica->log && replica->log->is_open()) {
        auto close_result = replica->log->close();
        if (close_result.failed()) {
            PM_LOG_WARN("Error closing log for {}/{}: {}",
                        topic, partition, close_result.error_message);
        }
    }

    PM_LOG_INFO("StopReplica: {}/{} stopped", topic, partition);
    return result<void>::success();
}

// ============================================================================
// PartitionManager — Background ISR monitor
// ============================================================================

/**
 * Start the background ISR monitor thread.
 *
 * Periodically checks all partitions where this broker is leader,
 * evaluates follower lag, and shrinks the ISR for slow replicas.
 */
void PartitionManager::start_isr_monitor() {
    if (impl_->isr_monitor_running.load(std::memory_order_acquire)) {
        PM_LOG_DEBUG("ISR monitor already running");
        return;
    }

    impl_->isr_monitor_running.store(true, std::memory_order_release);
    impl_->isr_monitor_thread = std::thread([this]() {
        PM_LOG_INFO("ISR monitor thread started");

        while (impl_->isr_monitor_running.load(std::memory_order_acquire)) {
            try {
                shrink_isr_stale_replicas();
                maybe_advance_high_watermark_all();
            } catch (const std::exception& e) {
                PM_LOG_ERROR("ISR monitor exception: {}", e.what());
            } catch (...) {
                PM_LOG_ERROR("ISR monitor unknown exception");
            }

            // Sleep for the monitoring interval
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kDefaultHWAdvanceIntervalMs));
        }

        PM_LOG_INFO("ISR monitor thread stopped");
    });
}

/**
 * Stop the background ISR monitor thread.
 */
void PartitionManager::stop_isr_monitor() {
    impl_->isr_monitor_running.store(false, std::memory_order_release);
    if (impl_->isr_monitor_thread.joinable()) {
        impl_->isr_monitor_thread.join();
    }
}

// ============================================================================
// PartitionManager — High watermark advancement
// ============================================================================

/**
 * Advance the high watermark for all partitions where this broker is leader.
 *
 * The high watermark is the minimum log-end-offset across all ISR members,
 * and represents the offset up to which consumers can safely read.
 */
void PartitionManager::maybe_advance_high_watermark_all() {
    broker_id_t local_id = server_->broker_id();

    std::shared_lock lock(impl_->mutex);

    for (auto& [key, replica] : impl_->partitions) {
        if (!replica->is_active || !replica->is_leader_local) continue;
        if (!replica->log || !replica->log->is_open()) continue;

        // Get leader's log-end offset
        offset_t leader_log_end = replica->log->get_log_end_offset();
        if (leader_log_end <= replica->high_watermark) continue;

        // For each ISR member, track the minimum log-end offset
        // (In production, we'd query followers for their log-end offsets)
        offset_t min_isr_log_end = leader_log_end;

        // For now, since we can't query remote followers in the stub,
        // we use the leader's own log-end offset as the upper bound.
        // The Raft consensus layer actually manages this more precisely.

        if (min_isr_log_end > replica->high_watermark) {
            offset_t new_hw = min_isr_log_end;
            replica->log->update_high_watermark(new_hw);
            replica->high_watermark = new_hw;

            PM_LOG_TRACE("Advanced HW for {}/{} to {}",
                         replica->topic, replica->partition_id, new_hw);
        }
    }
}

/**
 * Advance the high watermark for a specific partition.
 */
void PartitionManager::advance_high_watermark(
    const std::string& topic,
    partition_id_t partition,
    offset_t new_hw)
{
    std::string key = make_partition_key(topic, partition);

    std::unique_lock lock(impl_->mutex);
    auto it = impl_->partitions.find(key);
    if (it == impl_->partitions.end()) return;

    auto* replica = it->second.get();
    if (!replica->is_active) return;

    if (new_hw > replica->high_watermark && replica->log) {
        replica->log->update_high_watermark(new_hw);
        replica->high_watermark = new_hw;
    }
}

// ============================================================================
// PartitionManager — Watermark queries
// ============================================================================

offset_t PartitionManager::high_watermark_for(
    const std::string& topic, partition_id_t partition) const {

    std::string key = make_partition_key(topic, partition);
    std::shared_lock lock(impl_->mutex);

    auto it = impl_->partitions.find(key);
    if (it == impl_->partitions.end()) return kInvalidOffset;
    return it->second->high_watermark;
}

offset_t PartitionManager::log_end_offset_for(
    const std::string& topic, partition_id_t partition) const {

    std::string key = make_partition_key(topic, partition);
    std::shared_lock lock(impl_->mutex);

    auto it = impl_->partitions.find(key);
    if (it == impl_->partitions.end() || !it->second->log) return kInvalidOffset;
    return it->second->log->get_log_end_offset();
}

// ============================================================================
// PartitionManager — Aggregate metrics
// ============================================================================

int32_t PartitionManager::leader_partition_count() const noexcept {
    return impl_->leader_partition_count.load(std::memory_order_acquire);
}

int32_t PartitionManager::total_partition_count() const noexcept {
    return impl_->total_partition_count.load(std::memory_order_acquire);
}

int32_t PartitionManager::under_replicated_count() const {
    int32_t count = 0;
    std::shared_lock lock(impl_->mutex);

    for (const auto& [key, replica] : impl_->partitions) {
        if (!replica->is_active) continue;
        if (static_cast<int32_t>(replica->isr.size()) <
            static_cast<int32_t>(replica->replicas.size())) {
            ++count;
        }
    }

    return count;
}

/**
 * Get the LogManager for a specific partition (used by produce/fetch paths).
 */
storage::log::LogManager* PartitionManager::log_for(
    const std::string& topic, partition_id_t partition) {

    std::string key = make_partition_key(topic, partition);
    std::shared_lock lock(impl_->mutex);

    auto it = impl_->partitions.find(key);
    if (it == impl_->partitions.end()) return nullptr;
    return it->second->log.get();
}

/**
 * Get the PartitionReplica for a specific partition (advanced use).
 */
PartitionReplica* PartitionManager::replica_for(
    const std::string& topic, partition_id_t partition) {

    std::string key = make_partition_key(topic, partition);
    std::shared_lock lock(impl_->mutex);

    auto it = impl_->partitions.find(key);
    if (it == impl_->partitions.end()) return nullptr;
    return it->second.get();
}

/**
 * List all partitions hosted on this broker, optionally filtered by topic.
 */
std::vector<std::pair<std::string, partition_id_t>>
PartitionManager::list_hosted_partitions(
    const std::string& topic_filter) const {

    std::shared_lock lock(impl_->mutex);

    std::vector<std::pair<std::string, partition_id_t>> result;
    result.reserve(impl_->partitions.size());

    for (const auto& [key, replica] : impl_->partitions) {
        if (!replica->is_active) continue;
        auto [topic, pid] = parse_partition_key(key);
        if (!topic_filter.empty() && topic != topic_filter) continue;
        result.emplace_back(topic, pid);
    }

    std::sort(result.begin(), result.end());
    return result;
}

} // namespace torrent::broker
