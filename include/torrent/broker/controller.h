#pragma once
#include <memory>
#include <atomic>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include <chrono>
#include <torrent/common/types.h>

namespace torrent::broker {

class Controller {
public:
    explicit Controller(class BrokerServer& s);
    ~Controller();

    Controller(const Controller&) = delete;
    Controller& operator=(const Controller&) = delete;
    Controller(Controller&&) = delete;
    Controller& operator=(Controller&&) = delete;

    // ---- Lifecycle ----

    void start();
    void shutdown();

    // ---- Controller identity ----

    /// True if this broker is the currently elected controller.
    [[nodiscard]] bool is_controller() const noexcept;

    /// The broker ID of the current controller (may be remote).
    [[nodiscard]] broker_id_t controller_id() const noexcept;

    /// The current controller epoch (monotonically increasing).
    [[nodiscard]] epoch_t controller_epoch() const noexcept;

    // ---- Controller election ----

    /// Initiate or participate in a controller election via Raft.
    void elect_controller();

    /// Callback when this broker is elected controller.
    void on_become_controller();

    /// Callback when this broker steps down as controller.
    void on_resign_controller();

    // ---- Topic orchestration ----

    /// Create a topic across the cluster. Must be called on the controller.
    result<void> create_topic(
        const std::string& name,
        int32_t num_partitions,
        int16_t replication_factor,
        const std::map<std::string, std::string>& configs = {});

    /// Delete a topic across the cluster. Must be called on the controller.
    result<void> delete_topic(const std::string& name);

    /// Increase partition count for an existing topic.
    result<void> create_partitions(
        const std::string& name,
        int32_t new_total_count);

    // ---- Partition assignment ----

    /// Assign replicas for a partition set across known brokers.
    /// Returns a vector of (partition_id → list of replica broker_ids).
    [[nodiscard]] std::vector<std::vector<broker_id_t>> assign_replicas(
        int32_t num_partitions,
        int32_t replication_factor,
        bool rack_aware = true) const;

    /// Reassign replicas for an existing partition (e.g., broker failure).
    result<void> reassign_partitions(
        const std::vector<std::pair<std::string, partition_id_t>>& partitions,
        const std::vector<std::vector<broker_id_t>>& new_assignments);

    // ---- Leader election ----

    /// Elect a preferred leader for a partition from its ISR.
    [[nodiscard]] broker_id_t elect_preferred_leader(
        const std::string& topic,
        partition_id_t partition) const;

    /// Trigger leader election for a stalled partition.
    result<void> trigger_leader_election(
        const std::string& topic,
        partition_id_t partition);

    // ---- Failover ----

    /// Handle broker failure: reassign partitions, elect new leaders.
    result<void> handle_broker_failure(broker_id_t failed_broker);

    /// Handle broker recovery: reassign replicas, restore ISR.
    result<void> handle_broker_recovery(broker_id_t recovered_broker);

    // ---- Epoch fencing ----

    /// Check if a given epoch is still valid (not fenced by a newer epoch).
    [[nodiscard]] bool is_epoch_valid(epoch_t epoch) const noexcept;

    /// Increment the controller epoch to fence stale controllers.
    void increment_epoch();

    // ---- Metrics ----

    struct Metrics {
        bool is_controller = false;
        broker_id_t controller_id = kNoBroker;
        epoch_t controller_epoch = 0;
        int32_t topic_count = 0;
        int32_t partition_count = 0;
        int32_t under_replicated_count = 0;
        int32_t offline_replica_count = 0;
        int32_t preferred_leader_imbalance_count = 0;
        uint64_t leader_elections_triggered = 0;
        uint64_t reassignments_completed = 0;
        std::chrono::steady_clock::time_point last_failover;
    };

    [[nodiscard]] Metrics get_metrics() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    BrokerServer* server_;

    // Internal helpers
    void controller_election_loop();
    void failover_detection_loop();
    void metadata_sync_loop();
    void propose_to_raft(const std::string& command_type,
                         const std::vector<uint8_t>& payload);
};

} // namespace torrent::broker
