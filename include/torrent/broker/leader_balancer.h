#pragma once
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <torrent/common/types.h>

namespace torrent::broker {

class LeaderBalancer {
public:
    explicit LeaderBalancer(class BrokerServer& s);
    ~LeaderBalancer();

    LeaderBalancer(const LeaderBalancer&) = delete;
    LeaderBalancer& operator=(const LeaderBalancer&) = delete;
    LeaderBalancer(LeaderBalancer&&) = delete;
    LeaderBalancer& operator=(LeaderBalancer&&) = delete;

    // ---- Lifecycle ----

    void start();
    void shutdown();

    // ---- Rebalancing ----

    /// Perform a full leader rebalance across all partitions.
    /// Moves leaders to their preferred replicas where possible,
    /// while respecting rack constraints and throttling limits.
    void rebalance();

    /// Move leadership for a specific partition to a target broker.
    result<void> move_leader(
        const std::string& topic,
        partition_id_t partition,
        broker_id_t target);

    // ---- Configuration ----

    /// Set the maximum number of leadership moves per rebalance cycle.
    void set_max_moves_per_cycle(size_t max_moves);

    /// Set the minimum time between consecutive leadership moves (throttle).
    void set_move_throttle_ms(int64_t throttle_ms);

    /// Enable or disable rack-aware balancing.
    void set_rack_aware(bool enabled);

    // ---- Metrics ----

    struct Metrics {
        uint64_t total_cycles = 0;
        uint64_t total_moves = 0;
        uint64_t last_cycle_moves = 0;
        int32_t imbalanced_partitions = 0;
        double imbalance_ratio = 0.0;
        broker_id_t most_leaders_broker = kNoBroker;
        int32_t most_leaders_count = 0;
        broker_id_t fewest_leaders_broker = kNoBroker;
        int32_t fewest_leaders_count = 0;
        std::chrono::steady_clock::time_point last_cycle_time;
    };

    [[nodiscard]] Metrics get_metrics() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    BrokerServer* server_;

    // Internal helpers
    void rebalance_loop();
    void compute_leader_distribution(
        std::unordered_map<broker_id_t, int32_t>& leader_counts) const;
    [[nodiscard]] bool should_move_leader(
        const std::string& topic,
        partition_id_t partition,
        broker_id_t current_leader,
        broker_id_t preferred_leader,
        const std::unordered_map<broker_id_t, int32_t>& leader_counts) const;
};

} // namespace torrent::broker
