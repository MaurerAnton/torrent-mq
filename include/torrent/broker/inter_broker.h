#pragma once
#include <memory>
#include <functional>
#include <string>
#include <vector>
#include <unordered_map>
#include <optional>
#include <chrono>
#include <torrent/common/types.h>
#include "torrent/consensus/raft_types.h"

namespace torrent::raft {
    struct AppendEntriesRequest;
    struct RequestVoteRequest;
    struct InstallSnapshotRequest;
    struct AppendEntriesResponse;
    struct RequestVoteResponse;
    struct InstallSnapshotResponse;
}

namespace torrent::broker {

class InterBroker {
public:
    explicit InterBroker(class BrokerServer& s);
    ~InterBroker();

    InterBroker(const InterBroker&) = delete;
    InterBroker& operator=(const InterBroker&) = delete;
    InterBroker(InterBroker&&) = delete;
    InterBroker& operator=(InterBroker&&) = delete;

    // ---- Lifecycle ----

    void start();
    void shutdown();

    // ---- Partition leadership & ISR propagation ----

    /// Notify replicas of a leadership change (LeaderAndISR RPC).
    /// Sent by the controller to all replicas in the ISR set.
    result<void> send_leader_and_isr(
        const std::string& topic,
        partition_id_t partition,
        broker_id_t leader,
        const std::vector<broker_id_t>& isr,
        epoch_t leader_epoch = 0);

    /// Propagate cluster metadata (broker list, topic→partition map).
    /// Sent by the controller to all brokers.
    result<void> send_update_metadata(const std::vector<endpoint>& brokers);

    /// Tell a follower to stop replicating a partition.
    /// Sent by the controller when a partition is being deleted or reassigned.
    result<void> send_stop_replica(
        const std::string& topic,
        partition_id_t partition,
        broker_id_t target_broker,
        bool delete_data = true);

    /// Fetch data from a leader (replication fetch).
    /// Used by followers to catch up on replication.
    result<std::vector<uint8_t>> send_fetch_request(
        broker_id_t leader,
        const std::string& topic,
        partition_id_t partition,
        offset_t fetch_offset,
        int32_t max_bytes);

    // ---- Raft RPC relay ----

    /// Relay AppendEntries RPC to a Raft peer.
    void send_append_entries(
        raft::RaftNodeId target,
        const raft::AppendEntriesRequest& req);

    /// Relay RequestVote RPC to a Raft peer.
    void send_request_vote(
        raft::RaftNodeId target,
        const raft::RequestVoteRequest& req);

    /// Relay InstallSnapshot RPC to a Raft peer.
    void send_install_snapshot(
        raft::RaftNodeId target,
        const raft::InstallSnapshotRequest& req);

    // ---- Connection management ----

    /// Add a known peer broker endpoint for future communication.
    void upsert_peer(broker_id_t broker_id, endpoint ep);

    /// Remove a peer (decommissioned / failed).
    void remove_peer(broker_id_t broker_id);

    /// Check if a peer is currently reachable.
    [[nodiscard]] bool is_peer_alive(broker_id_t broker_id) const;

    /// Get the count of known peers.
    [[nodiscard]] size_t peer_count() const;

    // ---- Controller communication ----

    /// Forward a controller-bound request (e.g., topic create/delete from
    /// a non-controller broker).
    result<shared_buffer> forward_to_controller(
        int16_t api_key,
        buffer_view request);

    // ---- Metrics ----

    struct Metrics {
        size_t active_connections = 0;
        uint64_t messages_sent = 0;
        uint64_t messages_received = 0;
        uint64_t messages_failed = 0;
        uint64_t bytes_sent = 0;
        uint64_t bytes_received = 0;
        uint64_t reconnects = 0;
        int64_t avg_rtt_us = 0;
    };

    [[nodiscard]] Metrics get_metrics() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    BrokerServer* server_;
};

} // namespace torrent::broker
