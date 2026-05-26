#pragma once

/**
 * grpc_server.h — gRPC Inter-Broker Communication Server
 *
 * Lightweight gRPC-style server for inter-broker RPC operations:
 *   - Raft RPC Service: AppendEntries, RequestVote, InstallSnapshot
 *   - Metadata Service: fetch metadata, register broker
 *   - Consumer Group Service: find coordinator, join group, sync group
 *
 * Uses custom binary framing over TCP (no protobuf dependency):
 *   [compression:1][length:4 BE][rpc_tag:1][payload...]
 */

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "torrent/common/types.h"
#include "torrent/network/protocol.h"

// Forward declarations
namespace torrent::raft {
    struct AppendEntriesRequest;
    struct AppendEntriesResponse;
    struct RequestVoteRequest;
    struct RequestVoteResponse;
    struct InstallSnapshotRequest;
    struct InstallSnapshotResponse;
}

namespace torrent::network {

namespace tp = torrent::protocol;

// ============================================================================
// GrpcServer
// ============================================================================

class GrpcServer {
public:
    GrpcServer();
    explicit GrpcServer(const torrent::endpoint& bind_addr, uint16_t port);
    ~GrpcServer();

    GrpcServer(const GrpcServer&) = delete;
    GrpcServer& operator=(const GrpcServer&) = delete;
    GrpcServer(GrpcServer&&) = delete;
    GrpcServer& operator=(GrpcServer&&) = delete;

    // ---- Lifecycle ----

    /// Start accepting connections and servicing RPCs.
    /// Returns true on success.
    bool start();

    /// Gracefully shut down the server.
    void shutdown();

    // ---- Raft RPC handlers ----

    void set_append_entries_handler(
        std::function<raft::AppendEntriesResponse(const raft::AppendEntriesRequest&)> handler);
    void set_request_vote_handler(
        std::function<raft::RequestVoteResponse(const raft::RequestVoteRequest&)> handler);
    void set_install_snapshot_handler(
        std::function<raft::InstallSnapshotResponse(const raft::InstallSnapshotRequest&)> handler);

    // ---- Metadata service handlers ----

    void set_fetch_metadata_handler(
        std::function<tp::MetadataResponse(const tp::MetadataRequest&)> handler);
    void set_register_broker_handler(
        std::function<tp::PartitionResult(torrent::broker_id_t, const torrent::endpoint&)> handler);
    void set_update_metadata_handler(
        std::function<void(const tp::MetadataResponse&)> handler);

    // ---- Consumer group handlers ----

    void set_find_coordinator_handler(
        std::function<tp::FindCoordinatorResponse(const tp::FindCoordinatorRequest&)> handler);
    void set_join_group_handler(
        std::function<tp::JoinGroupResponse(const tp::JoinGroupRequest&)> handler);
    void set_sync_group_handler(
        std::function<tp::SyncGroupResponse(const tp::SyncGroupRequest&)> handler);
    void set_heartbeat_handler(
        std::function<tp::HeartbeatResponse(const tp::HeartbeatRequest&)> handler);
    void set_leave_group_handler(
        std::function<tp::LeaveGroupResponse(const tp::LeaveGroupRequest&)> handler);
    void set_offset_commit_handler(
        std::function<tp::OffsetCommitResponse(const tp::OffsetCommitRequest&)> handler);
    void set_offset_fetch_handler(
        std::function<tp::OffsetFetchResponse(const tp::OffsetFetchRequest&)> handler);

    // ---- Metrics ----

    [[nodiscard]] uint64_t total_requests() const;
    [[nodiscard]] uint64_t total_errors() const;
    [[nodiscard]] uint64_t active_connections() const;
    [[nodiscard]] bool is_running() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;

    // Internal RPC method identifiers
    enum class RpcMethod : uint8_t {
        AppendEntries       = 0,
        RequestVote         = 1,
        InstallSnapshot     = 2,
        FetchMetadata       = 10,
        RegisterBroker      = 11,
        UpdateMetadata      = 12,
        FindCoordinator     = 20,
        JoinGroup           = 21,
        SyncGroup           = 22,
        Heartbeat           = 23,
        LeaveGroup          = 24,
        OffsetCommit        = 25,
        OffsetFetch         = 26,
        HealthCheck         = 30,
    };

    std::vector<uint8_t> dispatch_rpc(RpcMethod method, const std::vector<uint8_t>& payload);

    // Internal methods
    void accept_loop();
    void worker_loop();
    void handle_client_readable(int fd);
    void remove_connection(int fd);
};

} // namespace torrent::network
