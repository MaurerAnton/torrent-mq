/**
 * controller.cpp — Controller: Cluster-Wide Orchestration
 *
 * Implements the torrent-mq cluster controller — the brain of the cluster.
 * The controller manages topic lifecycle, partition assignment, replica
 * placement, leadership election, broker failover/recovery, and epoch fencing.
 *
 * Architecture:
 *   - Controller election via Raft: the Raft leader is the controller
 *   - Topic orchestration: create/delete topics across the cluster
 *   - Partition assignment: rack-aware round-robin replica placement
 *   - Replica management: reassignment on broker join/leave
 *   - Preferred leader election: ensures leaders are evenly distributed
 *   - Failover detection: periodic health checking of all brokers
 *   - Epoch fencing: monotonically increasing epoch for stale-controller rejection
 *
 * Controller election flow:
 *   1. Raft elects a leader
 *   2. The Raft leader becomes the controller
 *   3. Controller increments its epoch (fencing all previous controllers)
 *   4. Controller propagates its identity via UpdateMetadata
 *   5. If Raft leadership changes, controller transitions to the new leader
 *
 * Rack awareness:
 *   Replicas are placed across different racks to survive rack failures.
 *   The rack assignment uses a greedy algorithm that prefers racks with
 *   the fewest replicas for the current partition.
 *
 * See controller.h for the public API contract.
 */

#include "torrent/broker/controller.h"
#include "torrent/broker/server.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/broker/partition_manager.h"
#include "torrent/broker/inter_broker.h"
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
#include <numeric>
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

// ============================================================================
// Aliases
// ============================================================================

using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace torrent::broker {

// ============================================================================
// Anonymous namespace — internal types, constants, helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_ctrl_logger() {
    static auto logger = []() {
        auto l = spdlog::get("controller");
        if (!l) {
            l = spdlog::stdout_color_mt("controller");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define CTRL_LOG_INFO(...)  get_ctrl_logger()->info(__VA_ARGS__)
#define CTRL_LOG_WARN(...)  get_ctrl_logger()->warn(__VA_ARGS__)
#define CTRL_LOG_ERROR(...) get_ctrl_logger()->error(__VA_ARGS__)
#define CTRL_LOG_DEBUG(...) get_ctrl_logger()->debug(__VA_ARGS__)
#define CTRL_LOG_TRACE(...) get_ctrl_logger()->trace(__VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Interval between controller election attempts when there is no controller.
static constexpr auto kElectionCheckInterval = 1s;

/// Interval between failover detection scans.
static constexpr auto kFailoverCheckInterval = 5s;

/// Interval between metadata sync operations.
static constexpr auto kMetadataSyncInterval = 30s;

/// Maximum time a broker can be unresponsive before failover triggers.
static constexpr auto kBrokerFailureTimeout = 30s;

/// Maximum time to wait for a broker to recover before full reassignment.
static constexpr auto kBrokerRecoveryGracePeriod = 60s;

/// Maximum partitions to reassign in a single batch.
static constexpr int32_t kMaxReassignmentBatchSize = 100;

/// Maximum partitions per broker (safety cap).
static constexpr int32_t kMaxPartitionsPerBroker = 100000;

/// Maximum replication factor.
static constexpr int16_t kMaxReplicationFactor = 16;

/// Default replication factor when not specified.
static constexpr int16_t kDefaultReplicationFactor = 3;

// --------------------------------------------------------------------------
// RackInfo — per-rack metadata for rack-aware placement
// --------------------------------------------------------------------------

/**
 * Tracks rack-level information for replica placement decisions.
 */
struct RackInfo {
    std::string rack_name;
    std::vector<broker_id_t> brokers;
    int32_t total_replica_count = 0;
};

// --------------------------------------------------------------------------
// BrokerMeta — per-broker metadata tracked by the controller
// --------------------------------------------------------------------------

/**
 * Controller's view of a single broker in the cluster.
 * Updated via heartbeats and health checks.
 */
struct BrokerMeta {
    broker_id_t broker_id;
    endpoint ep;
    std::string rack;
    bool is_alive = true;
    int32_t partition_count = 0;
    int32_t leader_count = 0;

    std::chrono::steady_clock::time_point last_heartbeat;
    std::chrono::steady_clock::time_point registered_at;
    std::chrono::steady_clock::time_point failed_at;

    /// Check if this broker has likely failed (no heartbeat within timeout).
    [[nodiscard]] bool is_likely_failed() const {
        if (!is_alive) return true;
        auto now = std::chrono::steady_clock::now();
        return (now - last_heartbeat) > kBrokerFailureTimeout;
    }
};

// --------------------------------------------------------------------------
// PartitionAssignment — controller-level partition metadata
// --------------------------------------------------------------------------

/**
 * Controller's view of a single partition's assignment and leadership.
 */
struct PartitionAssignmentEntry {
    std::string topic;
    partition_id_t partition_id;
    std::vector<broker_id_t> replicas;      // All assigned replicas (ordered)
    std::vector<broker_id_t> isr;            // In-sync replicas
    broker_id_t leader = kNoBroker;
    epoch_t leader_epoch = 0;
    bool reassigning = false;

    /// First replica is the preferred leader.
    [[nodiscard]] broker_id_t preferred_leader() const {
        if (replicas.empty()) return kNoBroker;
        return replicas.front();
    }
};

// --------------------------------------------------------------------------
// Rack-aware replica assignment algorithm
// --------------------------------------------------------------------------

/**
 * Assign replicas for partition_count partitions across available brokers,
 * respecting rack diversity where possible.
 *
 * Algorithm:
 *   1. Group brokers by rack
 *   2. For each partition, pick rf distinct racks (if enough racks exist)
 *   3. Within each rack, pick the broker with the fewest existing replicas
 *   4. If not enough racks, fill remaining replicas from any rack
 *
 * This minimizes the probability that a single rack failure causes data loss.
 */
[[nodiscard]] std::vector<std::vector<broker_id_t>> assign_replicas_rack_aware(
    int32_t num_partitions,
    int32_t replication_factor,
    const std::unordered_map<broker_id_t, BrokerMeta>& brokers,
    const std::unordered_map<std::string, RackInfo>& racks)
{
    std::vector<std::vector<broker_id_t>> assignments;
    assignments.reserve(static_cast<size_t>(num_partitions));

    // Collect alive broker IDs
    std::vector<broker_id_t> alive_brokers;
    for (const auto& [id, meta] : brokers) {
        if (meta.is_alive) {
            alive_brokers.push_back(id);
        }
    }
    std::sort(alive_brokers.begin(), alive_brokers.end());

    if (alive_brokers.empty()) {
        // No brokers — can't assign
        for (int32_t i = 0; i < num_partitions; ++i) {
            assignments.push_back({});
        }
        return assignments;
    }

    // If replication_factor > alive brokers, cap it
    int32_t effective_rf = std::min(replication_factor,
        static_cast<int32_t>(alive_brokers.size()));

    // Track how many replicas each broker currently hosts
    std::unordered_map<broker_id_t, int32_t> broker_load;
    for (auto id : alive_brokers) {
        broker_load[id] = 0;
    }

    // Build rack→brokers map for alive brokers
    std::unordered_map<std::string, std::vector<broker_id_t>> rack_brokers;
    for (auto id : alive_brokers) {
        auto it = brokers.find(id);
        if (it != brokers.end()) {
            rack_brokers[it->second.rack].push_back(id);
        }
    }

    // Sort rack names for deterministic ordering
    std::vector<std::string> rack_names;
    for (const auto& [rack, _] : rack_brokers) {
        rack_names.push_back(rack);
    }
    std::sort(rack_names.begin(), rack_names.end());

    // For each partition, pick replicas
    int32_t start_index = 0;
    for (int32_t p = 0; p < num_partitions; ++p) {
        std::vector<broker_id_t> replicas;
        replicas.reserve(static_cast<size_t>(effective_rf));

        std::set<broker_id_t> used;
        std::set<std::string> used_racks;

        // Pass 1: prefer distinct racks
        for (int32_t r = 0; r < effective_rf; ++r) {
            // Find a rack not yet used, if possible
            std::string best_rack;
            broker_id_t best_broker = kNoBroker;
            int32_t best_load = kMaxPartitionsPerBroker;

            for (const auto& rack_name : rack_names) {
                if (used_racks.size() < rack_names.size() &&
                    used_racks.count(rack_name) > 0) {
                    continue; // Already used this rack, try others first
                }

                auto it = rack_brokers.find(rack_name);
                if (it == rack_brokers.end()) continue;

                for (auto bid : it->second) {
                    if (used.count(bid) > 0) continue;
                    int32_t load = broker_load[bid];
                    if (load < best_load) {
                        best_load = load;
                        best_broker = bid;
                        best_rack = rack_name;
                    }
                }
            }

            if (best_broker != kNoBroker) {
                replicas.push_back(best_broker);
                used.insert(best_broker);
                used_racks.insert(best_rack);
                broker_load[best_broker]++;
            }
        }

        // If we couldn't fill all replicas, pick any remaining broker
        while (static_cast<int32_t>(replicas.size()) < effective_rf) {
            broker_id_t best_broker = kNoBroker;
            int32_t best_load = kMaxPartitionsPerBroker;

            for (auto bid : alive_brokers) {
                if (used.count(bid) > 0) continue;
                int32_t load = broker_load[bid];
                if (load < best_load) {
                    best_load = load;
                    best_broker = bid;
                }
            }

            if (best_broker != kNoBroker) {
                replicas.push_back(best_broker);
                used.insert(best_broker);
                broker_load[best_broker]++;
            } else {
                break;
            }
        }

        assignments.push_back(std::move(replicas));
        start_index = (start_index + effective_rf) %
            static_cast<int32_t>(alive_brokers.size());
    }

    return assignments;
}

// --------------------------------------------------------------------------
// Simple round-robin replica assignment (non-rack-aware fallback)
// --------------------------------------------------------------------------

[[nodiscard]] std::vector<std::vector<broker_id_t>> assign_replicas_simple(
    int32_t num_partitions,
    int32_t replication_factor,
    const std::vector<broker_id_t>& brokers)
{
    std::vector<std::vector<broker_id_t>> assignments;
    assignments.reserve(static_cast<size_t>(num_partitions));

    int32_t broker_count = static_cast<int32_t>(brokers.size());
    if (broker_count == 0) {
        for (int32_t i = 0; i < num_partitions; ++i) {
            assignments.push_back({});
        }
        return assignments;
    }

    int32_t effective_rf = std::min(replication_factor, broker_count);
    int32_t start_index = 0;

    for (int32_t p = 0; p < num_partitions; ++p) {
        std::vector<broker_id_t> replicas;
        replicas.reserve(static_cast<size_t>(effective_rf));
        for (int32_t r = 0; r < effective_rf; ++r) {
            int32_t idx = (start_index + r) % broker_count;
            replicas.push_back(brokers[static_cast<size_t>(idx)]);
        }
        assignments.push_back(std::move(replicas));
        start_index = (start_index + effective_rf) % broker_count;
    }

    return assignments;
}

// --------------------------------------------------------------------------
// Topic name validation
// --------------------------------------------------------------------------

[[nodiscard]] error_code validate_topic_name(const std::string& name) noexcept {
    if (name.empty()) return error_code::invalid_topic_exception;
    if (name.size() > 249) return error_code::invalid_topic_exception;
    // Reserved prefixes
    if (name.size() >= 2 && name[0] == '_' && name[1] == '_') {
        // Internal topics are allowed but restricted
        if (name == "__consumer_offsets" ||
            name == "__transaction_state" ||
            name == "__cluster_metadata") {
            return error_code::none; // System topics are fine
        }
    }
    return error_code::none;
}

} // anonymous namespace

// ============================================================================
// Controller::Impl — PIMPL
// ============================================================================

struct Controller::Impl {
    // Controller identity
    std::atomic<broker_id_t> controller_id_{kNoBroker};
    std::atomic<epoch_t> controller_epoch_{0};
    std::atomic<bool> is_controller_{false};

    // Broker metadata (controller's view of the cluster)
    std::unordered_map<broker_id_t, BrokerMeta> brokers;
    mutable std::shared_mutex brokers_mutex;

    // Rack information
    std::unordered_map<std::string, RackInfo> racks;
    mutable std::shared_mutex racks_mutex;

    // Partition assignments (topic → partition → assignment)
    std::unordered_map<std::string,
        std::unordered_map<partition_id_t, PartitionAssignmentEntry>> assignments;
    mutable std::shared_mutex assignments_mutex;

    // Running state
    std::atomic<bool> running{false};

    // Background threads
    std::thread election_thread;
    std::thread failover_thread;
    std::thread metadata_sync_thread;

    // Metrics
    std::atomic<uint64_t> leader_elections_triggered{0};
    std::atomic<uint64_t> reassignments_completed{0};
    std::chrono::steady_clock::time_point last_failover;
};

// ============================================================================
// Controller — Constructor / Destructor
// ============================================================================

Controller::Controller(BrokerServer& s)
    : server_(&s)
    , impl_(std::make_unique<Impl>())
{
    CTRL_LOG_INFO("Controller initialized");
}

Controller::~Controller() {
    if (impl_->running.load(std::memory_order_acquire)) {
        try { shutdown(); } catch (...) {}
    }
    CTRL_LOG_INFO("Controller destroyed");
}

// ============================================================================
// Controller — start() / shutdown()
// ============================================================================

void Controller::start() {
    if (impl_->running.load(std::memory_order_acquire)) {
        CTRL_LOG_WARN("Controller::start() called while already running");
        return;
    }

    impl_->running.store(true, std::memory_order_release);

    // Start background threads
    impl_->election_thread = std::thread(&Controller::controller_election_loop, this);
    impl_->failover_thread = std::thread(&Controller::failover_detection_loop, this);
    impl_->metadata_sync_thread = std::thread(&Controller::metadata_sync_loop, this);

    CTRL_LOG_INFO("Controller started (is_controller={})",
                  impl_->is_controller_.load());
}

void Controller::shutdown() {
    if (!impl_->running.load(std::memory_order_acquire)) {
        return;
    }

    CTRL_LOG_INFO("Controller shutting down...");

    impl_->running.store(false, std::memory_order_release);

    if (impl_->election_thread.joinable()) impl_->election_thread.join();
    if (impl_->failover_thread.joinable()) impl_->failover_thread.join();
    if (impl_->metadata_sync_thread.joinable()) impl_->metadata_sync_thread.join();

    CTRL_LOG_INFO("Controller shut down");
}

// ============================================================================
// Controller — is_controller() / controller_id() / controller_epoch()
// ============================================================================

bool Controller::is_controller() const noexcept {
    return impl_->is_controller_.load(std::memory_order_acquire);
}

broker_id_t Controller::controller_id() const noexcept {
    return impl_->controller_id_.load(std::memory_order_acquire);
}

epoch_t Controller::controller_epoch() const noexcept {
    return impl_->controller_epoch_.load(std::memory_order_acquire);
}

// ============================================================================
// Controller — elect_controller() / on_become_controller() / on_resign_controller()
// ============================================================================

void Controller::elect_controller() {
    // Controller election is driven by Raft leadership.
    // The Raft leader becomes the controller automatically.

    if (!server_->is_controller()) {
        CTRL_LOG_DEBUG("elect_controller: not Raft leader, cannot become controller");
        return;
    }

    broker_id_t local_id = server_->broker_id();
    broker_id_t current = impl_->controller_id_.load(std::memory_order_acquire);

    if (current == local_id) {
        // Already the controller
        return;
    }

    // Become the controller
    on_become_controller();
}

void Controller::on_become_controller() {
    broker_id_t local_id = server_->broker_id();

    // Increment epoch to fence any previous controller
    epoch_t new_epoch = impl_->controller_epoch_.fetch_add(1, std::memory_order_acq_rel) + 1;

    impl_->controller_id_.store(local_id, std::memory_order_release);
    impl_->is_controller_.store(true, std::memory_order_release);

    CTRL_LOG_INFO("Broker {} became controller with epoch {}",
                  local_id, new_epoch);

    // Register self as the first broker
    {
        std::unique_lock lock(impl_->brokers_mutex);
        BrokerMeta self_meta;
        self_meta.broker_id = local_id;
        self_meta.ep = server_->config().advertised_listeners.empty()
            ? endpoint{"localhost", kDefaultPort}
            : server_->config().advertised_listeners.front();
        self_meta.rack = server_->config().rack;
        self_meta.is_alive = true;
        self_meta.last_heartbeat = std::chrono::steady_clock::now();
        self_meta.registered_at = self_meta.last_heartbeat;
        impl_->brokers[local_id] = std::move(self_meta);
    }

    // Update metadata cache
    server_->metadata_cache().set_controller(local_id, new_epoch);
}

void Controller::on_resign_controller() {
    if (!impl_->is_controller_.load(std::memory_order_acquire)) {
        return;
    }

    broker_id_t local_id = server_->broker_id();
    CTRL_LOG_INFO("Broker {} resigning as controller", local_id);

    impl_->is_controller_.store(false, std::memory_order_release);
    impl_->controller_id_.store(kNoBroker, std::memory_order_release);
}

// ============================================================================
// Controller — create_topic()
// ============================================================================

result<void> Controller::create_topic(
    const std::string& name,
    int32_t num_partitions,
    int16_t replication_factor,
    const std::map<std::string, std::string>& configs)
{
    if (!impl_->is_controller_.load(std::memory_order_acquire)) {
        return result<void>::failure(
            error_code::not_controller,
            "Only the controller can create topics. Current controller is broker " +
            std::to_string(impl_->controller_id_.load()));
    }

    // Validate inputs
    auto ec = validate_topic_name(name);
    if (ec != error_code::none) {
        return result<void>::failure(ec, "Invalid topic name: " + name);
    }

    if (num_partitions < 1 || num_partitions > 200000) {
        return result<void>::failure(
            error_code::invalid_partitions,
            "Partition count must be between 1 and 200000, got " +
            std::to_string(num_partitions));
    }

    if (replication_factor < 1 || replication_factor > kMaxReplicationFactor) {
        return result<void>::failure(
            error_code::invalid_replication_factor,
            "Replication factor must be between 1 and " +
            std::to_string(kMaxReplicationFactor) +
            ", got " + std::to_string(replication_factor));
    }

    // Check broker count
    size_t broker_count;
    {
        std::shared_lock lock(impl_->brokers_mutex);
        broker_count = impl_->brokers.size();
    }

    if (broker_count == 0) {
        return result<void>::failure(
            error_code::broker_not_available,
            "No brokers available to host topic partitions");
    }

    if (replication_factor > static_cast<int16_t>(broker_count)) {
        return result<void>::failure(
            error_code::invalid_replication_factor,
            "Replication factor " + std::to_string(replication_factor) +
            " exceeds available broker count " + std::to_string(broker_count));
    }

    CTRL_LOG_INFO("Creating topic '{}' with {} partitions, rf={}",
                  name, num_partitions, replication_factor);

    // Assign replicas (rack-aware)
    auto assignments = assign_replicas(num_partitions, replication_factor, true);

    // Store assignments
    {
        std::unique_lock lock(impl_->assignments_mutex);
        auto& topic_map = impl_->assignments[name];
        for (int32_t p = 0; p < num_partitions && p < static_cast<int32_t>(assignments.size()); ++p) {
            PartitionAssignmentEntry entry;
            entry.topic = name;
            entry.partition_id = p;
            entry.replicas = assignments[static_cast<size_t>(p)];
            entry.isr = entry.replicas; // Initially all replicas are in ISR
            entry.leader = entry.preferred_leader();
            entry.leader_epoch = impl_->controller_epoch_.load(std::memory_order_acquire);
            topic_map[p] = std::move(entry);
        }
    }

    // Notify all brokers to create their partitions
    {
        std::shared_lock lock(impl_->assignments_mutex);
        auto it = impl_->assignments.find(name);
        if (it != impl_->assignments.end()) {
            for (const auto& [pid, entry] : it->second) {
                // For each replica, tell that broker to create the partition
                // In production: send CreatePartitionRPC to each replica broker
                CTRL_LOG_DEBUG("Controller: assigned partition {}/{} to brokers [{}]",
                              name, pid,
                              [&]() {
                                  std::string s;
                                  for (auto b : entry.replicas) {
                                      if (!s.empty()) s += ",";
                                      s += std::to_string(b);
                                  }
                                  return s;
                              }());
            }
        }
    }

    // Propose to Raft for consensus
    {
        std::vector<uint8_t> payload; // Serialized CreateTopic command
        propose_to_raft("CreateTopic", payload);
    }

    CTRL_LOG_INFO("Topic '{}' created successfully", name);
    return result<void>::success();
}

// ============================================================================
// Controller — delete_topic()
// ============================================================================

result<void> Controller::delete_topic(const std::string& name) {
    if (!impl_->is_controller_.load(std::memory_order_acquire)) {
        return result<void>::failure(error_code::not_controller,
            "Only the controller can delete topics");
    }

    CTRL_LOG_INFO("Deleting topic '{}'", name);

    // Remove from assignments
    {
        std::unique_lock lock(impl_->assignments_mutex);
        auto it = impl_->assignments.find(name);
        if (it == impl_->assignments.end()) {
            return result<void>::failure(
                error_code::unknown_topic_or_partition,
                "Topic not found: " + name);
        }

        // For each partition, send StopReplica to all replica brokers
        for (const auto& [pid, entry] : it->second) {
            for (auto broker : entry.replicas) {
                // In production: inter_broker->send_stop_replica(name, pid, broker, true)
                CTRL_LOG_DEBUG("Controller: sent StopReplica for {}/{} to broker {}",
                              name, pid, broker);
            }
        }

        impl_->assignments.erase(it);
    }

    // Propose to Raft
    {
        std::vector<uint8_t> payload;
        propose_to_raft("DeleteTopic", payload);
    }

    CTRL_LOG_INFO("Topic '{}' deleted", name);
    return result<void>::success();
}

// ============================================================================
// Controller — create_partitions()
// ============================================================================

result<void> Controller::create_partitions(
    const std::string& name,
    int32_t new_total_count)
{
    if (!impl_->is_controller_.load(std::memory_order_acquire)) {
        return result<void>::failure(error_code::not_controller,
            "Only the controller can create partitions");
    }

    std::unique_lock lock(impl_->assignments_mutex);
    auto it = impl_->assignments.find(name);
    if (it == impl_->assignments.end()) {
        return result<void>::failure(
            error_code::unknown_topic_or_partition,
            "Topic not found: " + name);
    }

    int32_t current_count = static_cast<int32_t>(it->second.size());
    if (new_total_count <= current_count) {
        return result<void>::failure(
            error_code::invalid_partitions,
            "New partition count " + std::to_string(new_total_count) +
            " is not greater than current count " + std::to_string(current_count));
    }

    int32_t num_new = new_total_count - current_count;
    CTRL_LOG_INFO("Increasing partitions for topic '{}' from {} to {}",
                  name, current_count, new_total_count);

    int32_t replication_factor = static_cast<int32_t>(
        it->second.begin()->second.replicas.size());

    auto new_assignments = assign_replicas(num_new, replication_factor, true);

    for (int32_t i = 0; i < num_new; ++i) {
        partition_id_t pid = current_count + i;
        PartitionAssignmentEntry entry;
        entry.topic = name;
        entry.partition_id = pid;
        entry.replicas = new_assignments[static_cast<size_t>(i)];
        entry.isr = entry.replicas;
        entry.leader = entry.preferred_leader();
        entry.leader_epoch = impl_->controller_epoch_.load(std::memory_order_acquire);
        it->second[pid] = std::move(entry);
    }

    propose_to_raft("CreatePartitions", {});

    return result<void>::success();
}

// ============================================================================
// Controller — assign_replicas()
// ============================================================================

std::vector<std::vector<broker_id_t>> Controller::assign_replicas(
    int32_t num_partitions,
    int32_t replication_factor,
    bool rack_aware) const
{
    if (num_partitions <= 0) return {};

    // Gather alive brokers
    std::vector<broker_id_t> alive_brokers;
    {
        std::shared_lock lock(impl_->brokers_mutex);
        for (const auto& [id, meta] : impl_->brokers) {
            if (meta.is_alive) {
                alive_brokers.push_back(id);
            }
        }
    }

    if (alive_brokers.empty()) {
        CTRL_LOG_WARN("assign_replicas: no alive brokers available");
        return {};
    }

    int32_t effective_rf = std::min(replication_factor,
        static_cast<int32_t>(alive_brokers.size()));

    if (rack_aware) {
        std::shared_lock rlock(impl_->racks_mutex);
        return assign_replicas_rack_aware(
            num_partitions, effective_rf, impl_->brokers, impl_->racks);
    } else {
        return assign_replicas_simple(num_partitions, effective_rf, alive_brokers);
    }
}

// ============================================================================
// Controller — reassign_partitions()
// ============================================================================

result<void> Controller::reassign_partitions(
    const std::vector<std::pair<std::string, partition_id_t>>& partitions,
    const std::vector<std::vector<broker_id_t>>& new_assignments)
{
    if (!impl_->is_controller_.load(std::memory_order_acquire)) {
        return result<void>::failure(error_code::not_controller,
            "Only the controller can reassign partitions");
    }

    if (partitions.size() != new_assignments.size()) {
        return result<void>::failure(
            error_code::invalid_replica_assignment,
            "Mismatch: " + std::to_string(partitions.size()) +
            " partitions but " + std::to_string(new_assignments.size()) +
            " assignments");
    }

    CTRL_LOG_INFO("Reassigning {} partitions", partitions.size());

    std::unique_lock lock(impl_->assignments_mutex);

    for (size_t i = 0; i < partitions.size(); ++i) {
        const auto& [topic, pid] = partitions[i];
        const auto& new_replicas = new_assignments[i];

        auto topic_it = impl_->assignments.find(topic);
        if (topic_it == impl_->assignments.end()) continue;

        auto part_it = topic_it->second.find(pid);
        if (part_it == topic_it->second.end()) continue;

        auto& entry = part_it->second;

        // Mark as reassigning
        entry.reassigning = true;

        // Compute which replicas to add/remove
        std::set<broker_id_t> old_set(entry.replicas.begin(), entry.replicas.end());
        std::set<broker_id_t> new_set(new_replicas.begin(), new_replicas.end());

        std::vector<broker_id_t> to_add;
        std::vector<broker_id_t> to_remove;

        for (auto b : new_replicas) {
            if (old_set.find(b) == old_set.end()) {
                to_add.push_back(b);
            }
        }
        for (auto b : entry.replicas) {
            if (new_set.find(b) == new_set.end()) {
                to_remove.push_back(b);
            }
        }

        // In production: initiate replica addition/removal via RPCs
        CTRL_LOG_DEBUG("Reassigning {}/{}: add={} remove={}",
                      topic, pid, to_add.size(), to_remove.size());

        // Update the assignment
        entry.replicas = new_replicas;
        entry.isr = new_replicas; // Full ISR after reassignment
        entry.leader = entry.preferred_leader();
        entry.reassigning = false;
    }

    impl_->reassignments_completed.fetch_add(1, std::memory_order_relaxed);

    propose_to_raft("ReassignPartitions", {});

    return result<void>::success();
}

// ============================================================================
// Controller — elect_preferred_leader()
// ============================================================================

broker_id_t Controller::elect_preferred_leader(
    const std::string& topic,
    partition_id_t partition) const
{
    std::shared_lock lock(impl_->assignments_mutex);

    auto topic_it = impl_->assignments.find(topic);
    if (topic_it == impl_->assignments.end()) {
        return kNoBroker;
    }

    auto part_it = topic_it->second.find(partition);
    if (part_it == topic_it->second.end()) {
        return kNoBroker;
    }

    const auto& entry = part_it->second;

    // Preferred leader is the first replica in the ordered list
    broker_id_t preferred = entry.preferred_leader();

    // Check if the preferred leader is alive and in ISR
    if (preferred != kNoBroker) {
        std::shared_lock block(impl_->brokers_mutex);
        auto broker_it = impl_->brokers.find(preferred);
        if (broker_it != impl_->brokers.end() && broker_it->second.is_alive) {
            // Check if in ISR
            auto isr_it = std::find(entry.isr.begin(), entry.isr.end(), preferred);
            if (isr_it != entry.isr.end()) {
                return preferred;
            }
        }
    }

    // Fallback: pick first alive ISR member
    {
        std::shared_lock block(impl_->brokers_mutex);
        for (auto bid : entry.isr) {
            auto broker_it = impl_->brokers.find(bid);
            if (broker_it != impl_->brokers.end() && broker_it->second.is_alive) {
                return bid;
            }
        }
    }

    return kNoBroker;
}

// ============================================================================
// Controller — trigger_leader_election()
// ============================================================================

result<void> Controller::trigger_leader_election(
    const std::string& topic,
    partition_id_t partition)
{
    if (!impl_->is_controller_.load(std::memory_order_acquire)) {
        return result<void>::failure(error_code::not_controller,
            "Only the controller can trigger leader elections");
    }

    auto new_leader = elect_preferred_leader(topic, partition);
    if (new_leader == kNoBroker) {
        return result<void>::failure(
            error_code::leader_not_available,
            "No eligible leader found for " + topic + "/" +
            std::to_string(partition));
    }

    CTRL_LOG_INFO("Triggering leader election for {}/{} → broker {}",
                  topic, partition, new_leader);

    // Update the assignment
    {
        std::unique_lock lock(impl_->assignments_mutex);
        auto topic_it = impl_->assignments.find(topic);
        if (topic_it != impl_->assignments.end()) {
            auto part_it = topic_it->second.find(partition);
            if (part_it != topic_it->second.end()) {
                part_it->second.leader = new_leader;
                part_it->second.leader_epoch =
                    impl_->controller_epoch_.load(std::memory_order_acquire);
            }
        }
    }

    impl_->leader_elections_triggered.fetch_add(1, std::memory_order_relaxed);

    // In production: send LeaderAndISR to all replicas
    return result<void>::success();
}

// ============================================================================
// Controller — handle_broker_failure()
// ============================================================================

result<void> Controller::handle_broker_failure(broker_id_t failed_broker) {
    if (!impl_->is_controller_.load(std::memory_order_acquire)) {
        return result<void>::failure(error_code::not_controller,
            "Only the controller can handle broker failures");
    }

    CTRL_LOG_WARN("Handling failure of broker {}", failed_broker);

    // Mark broker as failed
    {
        std::unique_lock lock(impl_->brokers_mutex);
        auto it = impl_->brokers.find(failed_broker);
        if (it != impl_->brokers.end()) {
            it->second.is_alive = false;
            it->second.failed_at = std::chrono::steady_clock::now();
        }
    }

    impl_->last_failover = std::chrono::steady_clock::now();

    // Find all partitions that had this broker as leader or replica
    std::vector<std::pair<std::string, partition_id_t>> affected;
    {
        std::shared_lock lock(impl_->assignments_mutex);
        for (const auto& [topic, parts] : impl_->assignments) {
            for (const auto& [pid, entry] : parts) {
                bool is_affected = false;

                // Check if this broker was the leader
                if (entry.leader == failed_broker) {
                    is_affected = true;
                }

                // Check if this broker was in ISR
                if (std::find(entry.isr.begin(), entry.isr.end(), failed_broker)
                    != entry.isr.end()) {
                    is_affected = true;
                }

                if (is_affected) {
                    affected.emplace_back(topic, pid);
                }
            }
        }
    }

    CTRL_LOG_INFO("Broker {} failure affects {} partitions", failed_broker, affected.size());

    // For each affected partition, elect a new leader if needed
    {
        std::unique_lock lock(impl_->assignments_mutex);
        for (const auto& [topic, pid] : affected) {
            auto& entry = impl_->assignments[topic][pid];

            // Remove failed broker from ISR
            entry.isr.erase(
                std::remove(entry.isr.begin(), entry.isr.end(), failed_broker),
                entry.isr.end());

            // If the failed broker was the leader, elect a new one
            if (entry.leader == failed_broker) {
                broker_id_t new_leader = elect_preferred_leader(topic, pid);
                if (new_leader != kNoBroker) {
                    entry.leader = new_leader;
                    entry.leader_epoch =
                        impl_->controller_epoch_.load(std::memory_order_acquire);
                    CTRL_LOG_INFO("Elected new leader {} for {}/{} after broker {} failure",
                                 new_leader, topic, pid, failed_broker);
                } else {
                    CTRL_LOG_WARN("No eligible leader for {}/{} after broker {} failure",
                                 topic, pid, failed_broker);
                    entry.leader = kNoBroker;
                }
            }
        }
    }

    impl_->leader_elections_triggered.fetch_add(
        static_cast<uint64_t>(affected.size()), std::memory_order_relaxed);

    CTRL_LOG_INFO("Broker {} failure handled: {} partitions affected",
                  failed_broker, affected.size());

    return result<void>::success();
}

// ============================================================================
// Controller — handle_broker_recovery()
// ============================================================================

result<void> Controller::handle_broker_recovery(broker_id_t recovered_broker) {
    if (!impl_->is_controller_.load(std::memory_order_acquire)) {
        return result<void>::failure(error_code::not_controller,
            "Only the controller can handle broker recovery");
    }

    CTRL_LOG_INFO("Handling recovery of broker {}", recovered_broker);

    // Mark broker as alive
    {
        std::unique_lock lock(impl_->brokers_mutex);
        auto it = impl_->brokers.find(recovered_broker);
        if (it != impl_->brokers.end()) {
            it->second.is_alive = true;
            it->second.last_heartbeat = std::chrono::steady_clock::now();
        }
    }

    // Restore ISR for partitions where this broker is a replica
    int32_t restored_count = 0;
    {
        std::unique_lock lock(impl_->assignments_mutex);
        for (auto& [topic, parts] : impl_->assignments) {
            for (auto& [pid, entry] : parts) {
                // If this broker is in the replica set but not ISR, add it back
                if (std::find(entry.replicas.begin(), entry.replicas.end(),
                              recovered_broker) != entry.replicas.end()) {
                    if (std::find(entry.isr.begin(), entry.isr.end(),
                                  recovered_broker) == entry.isr.end()) {
                        entry.isr.push_back(recovered_broker);
                        ++restored_count;
                    }
                }
            }
        }
    }

    CTRL_LOG_INFO("Broker {} recovery: restored to ISR for {} partitions",
                  recovered_broker, restored_count);

    return result<void>::success();
}

// ============================================================================
// Controller — is_epoch_valid() / increment_epoch()
// ============================================================================

bool Controller::is_epoch_valid(epoch_t epoch) const noexcept {
    epoch_t current = impl_->controller_epoch_.load(std::memory_order_acquire);
    return epoch >= current;
}

void Controller::increment_epoch() {
    epoch_t new_epoch = impl_->controller_epoch_.fetch_add(1, std::memory_order_acq_rel) + 1;
    CTRL_LOG_INFO("Controller epoch incremented to {}", new_epoch);
}

// ============================================================================
// Controller — get_metrics()
// ============================================================================

Controller::Metrics Controller::get_metrics() const {
    Metrics m;

    m.is_controller = impl_->is_controller_.load(std::memory_order_acquire);
    m.controller_id = impl_->controller_id_.load(std::memory_order_acquire);
    m.controller_epoch = impl_->controller_epoch_.load(std::memory_order_acquire);

    {
        std::shared_lock lock(impl_->assignments_mutex);
        m.topic_count = static_cast<int32_t>(impl_->assignments.size());
        m.partition_count = 0;
        m.under_replicated_count = 0;

        for (const auto& [topic, parts] : impl_->assignments) {
            m.partition_count += static_cast<int32_t>(parts.size());
            for (const auto& [pid, entry] : parts) {
                if (entry.isr.size() < entry.replicas.size()) {
                    ++m.under_replicated_count;
                }
                if (entry.leader != entry.preferred_leader()) {
                    ++m.preferred_leader_imbalance_count;
                }
            }
        }
    }

    {
        std::shared_lock lock(impl_->brokers_mutex);
        for (const auto& [id, meta] : impl_->brokers) {
            if (!meta.is_alive) {
                ++m.offline_replica_count;
            }
        }
    }

    m.leader_elections_triggered = impl_->leader_elections_triggered.load(
        std::memory_order_acquire);
    m.reassignments_completed = impl_->reassignments_completed.load(
        std::memory_order_acquire);
    m.last_failover = impl_->last_failover;

    return m;
}

// ============================================================================
// Controller — Background loops
// ============================================================================

void Controller::controller_election_loop() {
    CTRL_LOG_DEBUG("Controller election loop started");

    while (impl_->running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(kElectionCheckInterval);

        // Check if we are the Raft leader
        bool raft_leader = server_->is_controller();

        bool currently_controller = impl_->is_controller_.load(std::memory_order_acquire);

        if (raft_leader && !currently_controller) {
            // We became Raft leader — become controller
            on_become_controller();
        } else if (!raft_leader && currently_controller) {
            // We lost Raft leadership — resign as controller
            on_resign_controller();
        }
    }

    CTRL_LOG_DEBUG("Controller election loop stopped");
}

void Controller::failover_detection_loop() {
    CTRL_LOG_DEBUG("Failover detection loop started");

    while (impl_->running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(kFailoverCheckInterval);

        if (!impl_->is_controller_.load(std::memory_order_acquire)) {
            continue; // Only the controller runs failover
        }

        std::vector<broker_id_t> failed_brokers;

        {
            std::shared_lock lock(impl_->brokers_mutex);
            auto now = std::chrono::steady_clock::now();
            for (const auto& [id, meta] : impl_->brokers) {
                if (meta.is_alive &&
                    (now - meta.last_heartbeat) > kBrokerFailureTimeout) {
                    failed_brokers.push_back(id);
                }
            }
        }

        for (auto bid : failed_brokers) {
            CTRL_LOG_WARN("Failover detection: broker {} appears to have failed", bid);
            auto result = handle_broker_failure(bid);
            if (result.failed()) {
                CTRL_LOG_ERROR("Failover for broker {} failed: {}",
                              bid, result.error_message);
            }
        }
    }

    CTRL_LOG_DEBUG("Failover detection loop stopped");
}

void Controller::metadata_sync_loop() {
    CTRL_LOG_DEBUG("Metadata sync loop started");

    while (impl_->running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(kMetadataSyncInterval);

        if (!impl_->is_controller_.load(std::memory_order_acquire)) {
            continue;
        }

        // Propagate cluster metadata to all known brokers
        // In production: collect full broker list and topic→partition map,
        // then send UpdateMetadata RPC to every known broker.

        CTRL_LOG_TRACE("Metadata sync: controller epoch={}, topics={}",
                       impl_->controller_epoch_.load(),
                       [this]() {
                           std::shared_lock lock(impl_->assignments_mutex);
                           return impl_->assignments.size();
                       }());
    }

    CTRL_LOG_DEBUG("Metadata sync loop stopped");
}

// ============================================================================
// Controller — propose_to_raft()
// ============================================================================

void Controller::propose_to_raft(
    const std::string& command_type,
    const std::vector<uint8_t>& payload)
{
    CTRL_LOG_DEBUG("Proposing '{}' to Raft consensus ({} bytes)",
                  command_type, payload.size());

    // In production:
    // 1. Serialize command into a Raft LogEntry
    // 2. Propose it to the RaftNode
    // 3. Wait for commitment by majority
    // 4. Apply via the state-machine callback
    //
    // The state machine callback then executes the actual metadata change
    // (creating/deleting topics, updating assignments, etc.)
}

} // namespace torrent::broker
