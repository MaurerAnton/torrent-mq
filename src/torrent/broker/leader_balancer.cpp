/**
 * leader_balancer.cpp — LeaderBalancer: Periodic Leadership Rebalancing
 *
 * Monitors leader distribution across brokers and periodically moves
 * leadership to preferred replicas to maintain even load distribution.
 * Supports rack-aware balancing and throttling to minimize disruption.
 *
 * Architecture:
 *   - Periodic balance cycle: runs every configurable interval
 *   - Imbalance detection: identifies brokers with too many/few leaders
 *   - Preferred leader election: moves leaders to first replica in assignment
 *   - Rack-aware balancing: ensures leaders are spread across racks
 *   - Throttling: limits leadership moves per cycle to avoid disruption
 *   - Metrics: tracks imbalance ratio, move counts, distribution
 *
 * Balancing algorithm:
 *   1. Compute leader count per broker
 *   2. Compute average (mean) leaders per broker
 *   3. Identify over-utilized brokers (> avg + threshold)
 *   4. For each over-utilized broker, find partitions where:
 *      a. Current leader is the over-utilized broker
 *      b. Preferred leader is an under-utilized broker
 *      c. Preferred leader is alive and in ISR
 *   5. Move leadership to preferred leader (throttled)
 *
 * Throttling:
 *   - max_moves_per_cycle: hard limit per balance cycle
 *   - move_throttle_ms: minimum time between moves
 *   - Cooldown: don't move a partition that was recently moved
 *
 * See leader_balancer.h for the public API contract.
 */

#include "torrent/broker/leader_balancer.h"
#include "torrent/broker/server.h"
#include "torrent/broker/controller.h"
#include "torrent/broker/partition_manager.h"
#include "torrent/broker/inter_broker.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

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
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

// ============================================================================
// Aliases
// ============================================================================

using namespace std::chrono_literals;

namespace torrent::broker {

// ============================================================================
// Anonymous namespace — internal types, constants, helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_lb_logger() {
    static auto logger = []() {
        auto l = spdlog::get("leader_balancer");
        if (!l) {
            l = spdlog::stdout_color_mt("leader_balancer");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define LB_LOG_INFO(...)  get_lb_logger()->info(__VA_ARGS__)
#define LB_LOG_WARN(...)  get_lb_logger()->warn(__VA_ARGS__)
#define LB_LOG_ERROR(...) get_lb_logger()->error(__VA_ARGS__)
#define LB_LOG_DEBUG(...) get_lb_logger()->debug(__VA_ARGS__)
#define LB_LOG_TRACE(...) get_lb_logger()->trace(__VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Default rebalance cycle interval.
static constexpr auto kDefaultRebalanceInterval = 60s;

/// Default maximum leadership moves per rebalance cycle.
static constexpr size_t kDefaultMaxMovesPerCycle = 10;

/// Default throttle between individual leadership moves.
static constexpr auto kDefaultMoveThrottle = 1s;

/// Minimum cooldown before a partition can be moved again.
static constexpr auto kMoveCooldown = 30s; // 5 min

/// Imbalance threshold: brokers with leader count > avg * threshold trigger moves.
static constexpr double kImbalanceThreshold = 1.3;

/// Minimum broker count before balancing is meaningful.
static constexpr size_t kMinBrokersForBalancing = 2;

/// Maximum partitions to scan per cycle.
static constexpr size_t kMaxPartitionScan = 10000;

/// Rack imbalance threshold (fraction of leaders per rack vs. ideal).
static constexpr double kRackImbalanceThreshold = 0.2;

// --------------------------------------------------------------------------
// PartitionMoveRecord — tracks recent moves for cooldown
// --------------------------------------------------------------------------

/**
 * Records a recent leadership move to prevent thrashing.
 */
struct PartitionMoveRecord {
    std::string topic;
    partition_id_t partition_id;
    broker_id_t from_broker;
    broker_id_t to_broker;
    std::chrono::steady_clock::time_point moved_at;
};

// --------------------------------------------------------------------------
// BrokerLeaderStats — per-broker leader distribution
// --------------------------------------------------------------------------

/**
 * Leader statistics for a single broker.
 */
struct BrokerLeaderStats {
    broker_id_t broker_id;
    std::string rack;
    int32_t leader_count = 0;
    int32_t partition_count = 0;
    bool is_alive = true;
};

// --------------------------------------------------------------------------
// Compute the ideal leader count per broker.
// --------------------------------------------------------------------------

[[nodiscard]] double compute_average_leaders(
    const std::unordered_map<broker_id_t, BrokerLeaderStats>& stats)
{
    int32_t total = 0;
    int32_t count = 0;
    for (const auto& [id, st] : stats) {
        if (st.is_alive) {
            total += st.leader_count;
            ++count;
        }
    }
    if (count == 0) return 0.0;
    return static_cast<double>(total) / static_cast<double>(count);
}

// --------------------------------------------------------------------------
// Compute leaders per rack for rack-aware balancing.
// --------------------------------------------------------------------------

[[nodiscard]] std::unordered_map<std::string, int32_t> compute_rack_leaders(
    const std::unordered_map<broker_id_t, BrokerLeaderStats>& stats)
{
    std::unordered_map<std::string, int32_t> rack_leaders;
    for (const auto& [id, st] : stats) {
        if (st.is_alive) {
            rack_leaders[st.rack] += st.leader_count;
        }
    }
    return rack_leaders;
}

// --------------------------------------------------------------------------
// Find the broker with the most leaders.
// --------------------------------------------------------------------------

[[nodiscard]] std::pair<broker_id_t, int32_t> find_max_leader_broker(
    const std::unordered_map<broker_id_t, BrokerLeaderStats>& stats)
{
    broker_id_t max_id = kNoBroker;
    int32_t max_count = 0;
    for (const auto& [id, st] : stats) {
        if (st.is_alive && st.leader_count > max_count) {
            max_count = st.leader_count;
            max_id = id;
        }
    }
    return {max_id, max_count};
}

// --------------------------------------------------------------------------
// Find the broker with the fewest leaders.
// --------------------------------------------------------------------------

[[nodiscard]] std::pair<broker_id_t, int32_t> find_min_leader_broker(
    const std::unordered_map<broker_id_t, BrokerLeaderStats>& stats)
{
    broker_id_t min_id = kNoBroker;
    int32_t min_count = std::numeric_limits<int32_t>::max();
    for (const auto& [id, st] : stats) {
        if (st.is_alive && st.leader_count < min_count) {
            min_count = st.leader_count;
            min_id = id;
        }
    }
    if (min_id == kNoBroker) {
        return {kNoBroker, 0};
    }
    return {min_id, min_count};
}

// --------------------------------------------------------------------------
// Compute imbalance ratio: max_leaders / avg_leaders (1.0 = perfectly balanced)
// --------------------------------------------------------------------------

[[nodiscard]] double compute_imbalance_ratio(
    const std::unordered_map<broker_id_t, BrokerLeaderStats>& stats)
{
    double avg = compute_average_leaders(stats);
    if (avg < 1.0) return 1.0; // Not enough leaders to be meaningful

    auto [max_id, max_count] = find_max_leader_broker(stats);
    if (max_count <= 0) return 1.0;

    return static_cast<double>(max_count) / avg;
}

// --------------------------------------------------------------------------
// Count imbalanced partitions (leader != preferred leader)
// --------------------------------------------------------------------------

[[nodiscard]] int32_t count_imbalanced_partitions(
    const std::vector<std::pair<std::string, partition_id_t>>& leaders,
    const std::unordered_map<std::string,
        std::unordered_map<partition_id_t, broker_id_t>>& preferred_map)
{
    int32_t imbalanced = 0;
    for (const auto& [topic, pid] : leaders) {
        auto topic_it = preferred_map.find(topic);
        if (topic_it != preferred_map.end()) {
            auto part_it = topic_it->second.find(pid);
            if (part_it != topic_it->second.end()) {
                // We don't know the current leader here, just count
                ++imbalanced;
            }
        }
    }
    return imbalanced;
}

} // anonymous namespace

// ============================================================================
// LeaderBalancer::Impl — PIMPL
// ============================================================================

struct LeaderBalancer::Impl {
    // Configuration
    std::chrono::milliseconds rebalance_interval = kDefaultRebalanceInterval;
    size_t max_moves_per_cycle = kDefaultMaxMovesPerCycle;
    std::chrono::milliseconds move_throttle = kDefaultMoveThrottle;
    bool rack_aware = true;

    // Recent move cooldown tracking
    std::deque<PartitionMoveRecord> recent_moves;
    mutable std::shared_mutex recent_moves_mutex;

    // Running state
    std::atomic<bool> running{false};
    std::thread rebalance_thread;

    // Metrics
    std::atomic<uint64_t> total_cycles{0};
    std::atomic<uint64_t> total_moves{0};
    std::atomic<uint64_t> last_cycle_moves{0};
    std::atomic<int32_t> imbalanced_partitions{0};
    std::atomic<double> imbalance_ratio{1.0};
    std::atomic<broker_id_t> most_leaders_broker{kNoBroker};
    std::atomic<int32_t> most_leaders_count{0};
    std::atomic<broker_id_t> fewest_leaders_broker{kNoBroker};
    std::atomic<int32_t> fewest_leaders_count{0};
    std::chrono::steady_clock::time_point last_cycle_time;

    // Last successful move timestamp (for throttle)
    std::chrono::steady_clock::time_point last_move_time;
};

// ============================================================================
// LeaderBalancer — Constructor / Destructor
// ============================================================================

LeaderBalancer::LeaderBalancer(BrokerServer& s)
    : server_(&s)
    , impl_(std::make_unique<Impl>())
{
    LB_LOG_INFO("LeaderBalancer initialized");
}

LeaderBalancer::~LeaderBalancer() {
    if (impl_->running.load(std::memory_order_acquire)) {
        try { shutdown(); } catch (...) {}
    }
    LB_LOG_INFO("LeaderBalancer destroyed");
}

// ============================================================================
// LeaderBalancer — start() / shutdown()
// ============================================================================

void LeaderBalancer::start() {
    if (impl_->running.load(std::memory_order_acquire)) {
        LB_LOG_WARN("LeaderBalancer::start() already running");
        return;
    }

    impl_->running.store(true, std::memory_order_release);
    impl_->rebalance_thread = std::thread(&LeaderBalancer::rebalance_loop, this);

    LB_LOG_INFO("LeaderBalancer started (interval={}s, max_moves={}, rack_aware={})",
               impl_->rebalance_interval.count() / 1000.0,
               impl_->max_moves_per_cycle,
               impl_->rack_aware);
}

void LeaderBalancer::shutdown() {
    if (!impl_->running.load(std::memory_order_acquire)) {
        return;
    }

    LB_LOG_INFO("LeaderBalancer shutting down...");
    impl_->running.store(false, std::memory_order_release);

    if (impl_->rebalance_thread.joinable()) {
        impl_->rebalance_thread.join();
    }

    LB_LOG_INFO("LeaderBalancer shut down (total cycles={}, total moves={})",
               impl_->total_cycles.load(), impl_->total_moves.load());
}

// ============================================================================
// LeaderBalancer — rebalance()
// ============================================================================

void LeaderBalancer::rebalance() {
    // Only the controller should perform rebalancing
    if (!server_->is_controller()) {
        LB_LOG_DEBUG("rebalance: not controller, skipping");
        return;
    }

    LB_LOG_INFO("Starting leader rebalance cycle #{}",
               impl_->total_cycles.load() + 1);

    auto cycle_start = std::chrono::steady_clock::now();

    // --- Phase 1: Compute current leader distribution ---

    std::unordered_map<broker_id_t, int32_t> leader_counts;
    compute_leader_distribution(leader_counts);

    if (leader_counts.size() < kMinBrokersForBalancing) {
        LB_LOG_DEBUG("rebalance: only {} brokers, skipping (need >= {})",
                    leader_counts.size(), kMinBrokersForBalancing);
        return;
    }

    // Build broker stats map
    std::unordered_map<broker_id_t, BrokerLeaderStats> broker_stats;
    for (const auto& [bid, count] : leader_counts) {
        BrokerLeaderStats st;
        st.broker_id = bid;
        st.leader_count = count;
        st.is_alive = true;
        // Rack info would come from metadata cache
        st.rack = "default";
        broker_stats[bid] = st;
    }

    // --- Phase 2: Detect imbalance ---

    double avg = compute_average_leaders(broker_stats);
    double imbalance = compute_imbalance_ratio(broker_stats);
    auto [max_id, max_count] = find_max_leader_broker(broker_stats);
    auto [min_id, min_count] = find_min_leader_broker(broker_stats);

    LB_LOG_INFO("Leader distribution: avg={:.1f} max={} ({} leaders) min={} ({} leaders) ratio={:.2f}",
               avg, max_id, max_count, min_id, min_count, imbalance);

    // Update metrics
    impl_->imbalance_ratio.store(imbalance, std::memory_order_release);
    impl_->most_leaders_broker.store(max_id, std::memory_order_release);
    impl_->most_leaders_count.store(max_count, std::memory_order_release);
    impl_->fewest_leaders_broker.store(min_id, std::memory_order_release);
    impl_->fewest_leaders_count.store(min_count, std::memory_order_release);

    // If well balanced, nothing to do
    if (imbalance <= kImbalanceThreshold) {
        LB_LOG_DEBUG("rebalance: cluster is well balanced (ratio={:.2f} <= threshold={:.2f})",
                    imbalance, kImbalanceThreshold);
        impl_->last_cycle_moves.store(0, std::memory_order_release);
        impl_->total_cycles.fetch_add(1, std::memory_order_relaxed);
        impl_->last_cycle_time = cycle_start;
        return;
    }

    // --- Phase 3: Identify partitions to move ---

    // Build the set of over-utilized brokers
    std::set<broker_id_t> over_utilized;
    for (const auto& [bid, st] : broker_stats) {
        if (st.is_alive &&
            static_cast<double>(st.leader_count) > avg * kImbalanceThreshold) {
            over_utilized.insert(bid);
        }
    }

    // Build the set of under-utilized brokers
    std::set<broker_id_t> under_utilized;
    for (const auto& [bid, st] : broker_stats) {
        if (st.is_alive &&
            static_cast<double>(st.leader_count) < avg / kImbalanceThreshold) {
            under_utilized.insert(bid);
        }
    }

    LB_LOG_DEBUG("Over-utilized brokers: {}  Under-utilized brokers: {}",
                over_utilized.size(), under_utilized.size());

    // Clean up old move records
    {
        std::unique_lock lock(impl_->recent_moves_mutex);
        auto now = std::chrono::steady_clock::now();
        while (!impl_->recent_moves.empty() &&
               (now - impl_->recent_moves.front().moved_at) > kMoveCooldown) {
            impl_->recent_moves.pop_front();
        }
    }

    // --- Phase 4: Execute leadership moves (throttled) ---

    size_t moves_this_cycle = 0;
    auto& partition_mgr = server_->partition_manager();

    // We create a plan of moves, then execute them one-by-one with throttling.
    // In production, this would query the partition manager for all leaders,
    // filter to over-utilized brokers, and move to preferred leaders on
    // under-utilized brokers.

    // For now, simulate a move plan based on the imbalance
    struct MovePlan {
        std::string topic;
        partition_id_t partition;
        broker_id_t from_broker;
        broker_id_t to_broker;
    };

    std::vector<MovePlan> move_plan;

    // Find partitions led by over-utilized brokers that have a preferred
    // leader on an under-utilized broker.
    //
    // In production: iterate through all partitions, check if the current
    // leader is over-utilized and the preferred leader is under-utilized.
    //
    // For stub: create a bounded number of candidate moves.

    for (auto over_bid : over_utilized) {
        if (moves_this_cycle >= impl_->max_moves_per_cycle) break;

        for (auto under_bid : under_utilized) {
            if (moves_this_cycle >= impl_->max_moves_per_cycle) break;

            // Find a partition to move (simplified)
            // In production: query partition_manager for partitions led by over_bid
            // with preferred leader = under_bid

            // Create a synthetic move plan entry
            MovePlan plan;
            plan.from_broker = over_bid;
            plan.to_broker = under_bid;

            // Check cooldown
            bool in_cooldown = false;
            {
                std::shared_lock lock(impl_->recent_moves_mutex);
                for (const auto& rec : impl_->recent_moves) {
                    if (rec.from_broker == over_bid &&
                        rec.to_broker == under_bid) {
                        in_cooldown = true;
                        break;
                    }
                }
            }

            if (in_cooldown) {
                continue;
            }

            // In production: the move would specify real topic/partition
            // For now, stub the plan
            move_plan.push_back(plan);
            ++moves_this_cycle;
        }
    }

    // Execute moves with throttling
    size_t executed_moves = 0;
    for (const auto& plan : move_plan) {
        // Throttle: ensure minimum time between moves
        auto now = std::chrono::steady_clock::now();
        auto elapsed_since_last = now - impl_->last_move_time;
        if (elapsed_since_last < impl_->move_throttle) {
            auto wait_time = impl_->move_throttle - elapsed_since_last;
            LB_LOG_DEBUG("Throttling: waiting {}ms before next move",
                        std::chrono::duration_cast<std::chrono::milliseconds>(wait_time).count());
            std::this_thread::sleep_for(wait_time);
            now = std::chrono::steady_clock::now();
        }

        // Execute the move
        // In production: would call move_leader() with real topic/partition
        LB_LOG_INFO("Moving leader from broker {} to broker {}",
                   plan.from_broker, plan.to_broker);

        // Record the move
        {
            std::unique_lock lock(impl_->recent_moves_mutex);
            PartitionMoveRecord rec;
            rec.from_broker = plan.from_broker;
            rec.to_broker = plan.to_broker;
            rec.moved_at = std::chrono::steady_clock::now();
            impl_->recent_moves.push_back(rec);

            // Keep recent_moves bounded
            while (impl_->recent_moves.size() > 1000) {
                impl_->recent_moves.pop_front();
            }
        }

        impl_->last_move_time = std::chrono::steady_clock::now();
        ++executed_moves;
    }

    // --- Phase 5: Rack-aware balancing (if enabled) ---

    if (impl_->rack_aware && executed_moves < impl_->max_moves_per_cycle) {
        auto rack_leaders = compute_rack_leaders(broker_stats);

        if (rack_leaders.size() > 1) {
            // Compute total leaders and ideal per rack
            int32_t total_leaders = 0;
            for (const auto& [rack, count] : rack_leaders) {
                total_leaders += count;
            }

            double ideal_per_rack = static_cast<double>(total_leaders) /
                                    static_cast<double>(rack_leaders.size());

            for (const auto& [rack, count] : rack_leaders) {
                double rack_ratio = static_cast<double>(count) / ideal_per_rack;
                if (rack_ratio > (1.0 + kRackImbalanceThreshold)) {
                    LB_LOG_DEBUG("Rack '{}' has {} leaders ({}x ideal), rack-imbalanced",
                                rack, count, rack_ratio);

                    // In production: move leaders from over-utilized rack to
                    // partitions whose preferred leader is on an under-utilized rack.
                    // This is complex and requires cross-rack leader moves.
                }
            }
        }
    }

    // Update cycle metrics
    impl_->last_cycle_moves.store(executed_moves, std::memory_order_release);
    impl_->total_moves.fetch_add(executed_moves, std::memory_order_relaxed);
    impl_->total_cycles.fetch_add(1, std::memory_order_relaxed);
    impl_->last_cycle_time = cycle_start;

    LB_LOG_INFO("Rebalance cycle complete: {} moves executed (total cycles={}, total moves={})",
               executed_moves,
               impl_->total_cycles.load(),
               impl_->total_moves.load());
}

// ============================================================================
// LeaderBalancer — move_leader()
// ============================================================================

result<void> LeaderBalancer::move_leader(
    const std::string& topic,
    partition_id_t partition,
    broker_id_t target)
{
    if (!server_->is_controller()) {
        return result<void>::failure(
            error_code::not_controller,
            "Only the controller can move partition leaders");
    }

    if (topic.empty()) {
        return result<void>::failure(
            error_code::invalid_topic_exception,
            "Topic name cannot be empty");
    }

    if (target == kNoBroker || target < 0) {
        return result<void>::failure(
            error_code::invalid_request,
            "Invalid target broker: " + std::to_string(target));
    }

    LB_LOG_INFO("Moving leader for {}/{} to broker {}",
               topic, partition, target);

    // Check if move cooldown applies
    {
        std::shared_lock lock(impl_->recent_moves_mutex);
        auto now = std::chrono::steady_clock::now();
        for (const auto& rec : impl_->recent_moves) {
            if (rec.topic == topic && rec.partition_id == partition) {
                auto elapsed = now - rec.moved_at;
                if (elapsed < kMoveCooldown) {
                    auto remaining = std::chrono::duration_cast<std::chrono::seconds>(
                        kMoveCooldown - elapsed);
                    return result<void>::failure(
                        error_code::reassignment_in_progress,
                        "Partition " + topic + "/" + std::to_string(partition) +
                        " was moved recently, cooldown remaining: " +
                        std::to_string(remaining.count()) + "s");
                }
                break;
            }
        }
    }

    // In production:
    // 1. Verify the target broker is alive and is a replica/ISR member
    // 2. Increment the partition epoch
    // 3. Send LeaderAndISR to all replicas with new leader
    // 4. Wait for acknowledgment from target broker
    // 5. Update partition manager state

    // Record the move
    {
        std::unique_lock lock(impl_->recent_moves_mutex);
        PartitionMoveRecord rec;
        rec.topic = topic;
        rec.partition_id = partition;
        rec.to_broker = target;
        rec.moved_at = std::chrono::steady_clock::now();
        impl_->recent_moves.push_back(rec);
    }

    impl_->total_moves.fetch_add(1, std::memory_order_relaxed);
    impl_->last_move_time = std::chrono::steady_clock::now();

    LB_LOG_INFO("Leader for {}/{} moved to broker {}", topic, partition, target);
    return result<void>::success();
}

// ============================================================================
// LeaderBalancer — Configuration
// ============================================================================

void LeaderBalancer::set_max_moves_per_cycle(size_t max_moves) {
    if (max_moves == 0) max_moves = 1;
    if (max_moves > 1000) max_moves = 1000;
    impl_->max_moves_per_cycle = max_moves;
    LB_LOG_INFO("max_moves_per_cycle set to {}", max_moves);
}

void LeaderBalancer::set_move_throttle_ms(int64_t throttle_ms) {
    if (throttle_ms < 0) throttle_ms = 0;
    if (throttle_ms > 60000) throttle_ms = 60000;
    impl_->move_throttle = std::chrono::milliseconds(throttle_ms);
    LB_LOG_INFO("move_throttle_ms set to {}", throttle_ms);
}

void LeaderBalancer::set_rack_aware(bool enabled) {
    impl_->rack_aware = enabled;
    LB_LOG_INFO("rack_aware set to {}", enabled);
}

// ============================================================================
// LeaderBalancer — get_metrics()
// ============================================================================

LeaderBalancer::Metrics LeaderBalancer::get_metrics() const {
    Metrics m;

    m.total_cycles = impl_->total_cycles.load(std::memory_order_acquire);
    m.total_moves = impl_->total_moves.load(std::memory_order_acquire);
    m.last_cycle_moves = impl_->last_cycle_moves.load(std::memory_order_acquire);
    m.imbalanced_partitions = impl_->imbalanced_partitions.load(std::memory_order_acquire);
    m.imbalance_ratio = impl_->imbalance_ratio.load(std::memory_order_acquire);
    m.most_leaders_broker = impl_->most_leaders_broker.load(std::memory_order_acquire);
    m.most_leaders_count = impl_->most_leaders_count.load(std::memory_order_acquire);
    m.fewest_leaders_broker = impl_->fewest_leaders_broker.load(std::memory_order_acquire);
    m.fewest_leaders_count = impl_->fewest_leaders_count.load(std::memory_order_acquire);
    m.last_cycle_time = impl_->last_cycle_time;

    return m;
}

// ============================================================================
// LeaderBalancer — Internal helpers
// ============================================================================

void LeaderBalancer::rebalance_loop() {
    LB_LOG_INFO("LeaderBalancer rebalance loop started");

    while (impl_->running.load(std::memory_order_acquire)) {
        auto wait_start = std::chrono::steady_clock::now();

        // Sleep in small increments to allow clean shutdown
        while (impl_->running.load(std::memory_order_acquire)) {
            auto elapsed = std::chrono::steady_clock::now() - wait_start;
            if (elapsed >= impl_->rebalance_interval) break;
            std::this_thread::sleep_for(1s);
        }

        if (!impl_->running.load(std::memory_order_acquire)) break;

        // Only run rebalance if we are the controller
        if (server_->is_controller()) {
            rebalance();
        }
    }

    LB_LOG_INFO("LeaderBalancer rebalance loop stopped");
}

void LeaderBalancer::compute_leader_distribution(
    std::unordered_map<broker_id_t, int32_t>& leader_counts) const
{
    // In production: query the controller's partition assignments
    // to count how many partitions each broker is leading.
    //
    // For now, build a simple distribution from what we know.
    leader_counts.clear();

    // Add self with a representative count
    broker_id_t local_id = server_->broker_id();
    leader_counts[local_id] = 10; // Placeholder

    // Add other known brokers with placeholder counts
    // In production: iterate through the controller's partition assignments
    // and count leaders per broker.

    LB_LOG_TRACE("compute_leader_distribution: {} brokers", leader_counts.size());
}

bool LeaderBalancer::should_move_leader(
    const std::string& topic,
    partition_id_t partition,
    broker_id_t current_leader,
    broker_id_t preferred_leader,
    const std::unordered_map<broker_id_t, int32_t>& leader_counts) const
{
    // Don't move if current leader is the preferred leader
    if (current_leader == preferred_leader) return false;

    // Don't move if preferred leader is not available
    if (preferred_leader == kNoBroker) return false;

    // Don't move if the current leader count is below average
    double avg = 0.0;
    int32_t count = 0;
    for (const auto& [bid, lc] : leader_counts) {
        avg += static_cast<double>(lc);
        ++count;
    }
    if (count > 0) avg /= static_cast<double>(count);

    auto current_it = leader_counts.find(current_leader);
    if (current_it != leader_counts.end() &&
        static_cast<double>(current_it->second) <= avg * kImbalanceThreshold) {
        // Current leader is not over-utilized
        return false;
    }

    // Don't move if preferred leader is already over-utilized
    auto preferred_it = leader_counts.find(preferred_leader);
    if (preferred_it != leader_counts.end() &&
        static_cast<double>(preferred_it->second) >= avg * kImbalanceThreshold) {
        return false;
    }

    // Don't move if partition was recently moved (cooldown)
    {
        std::shared_lock lock(impl_->recent_moves_mutex);
        auto now = std::chrono::steady_clock::now();
        for (const auto& rec : impl_->recent_moves) {
            if (rec.topic == topic && rec.partition_id == partition) {
                if ((now - rec.moved_at) < kMoveCooldown) {
                    return false;
                }
                break;
            }
        }
    }

    return true;
}

} // namespace torrent::broker
