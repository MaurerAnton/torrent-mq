#pragma once

/**
 * raft_types.h — Core Raft Consensus Types
 *
 * Vocabulary types for the Raft consensus protocol used by torrent-mq's
 * metadata control plane.  Follows Ongaro's Raft dissertation with Pre-Vote,
 * LeaderLease for linearizable reads, chunked InstallSnapshot, and dynamic
 * membership with joint consensus.
 */

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <variant>
#include <optional>
#include <chrono>
#include <memory>
#include <functional>
#include <atomic>
#include <unordered_map>
#include <algorithm>

#include "torrent/common/types.h"

namespace torrent::raft {

// ============================================================================
// Core Identifiers
// ============================================================================

/// Identifies a Raft node; reuses broker_id_t so each broker is a single identity.
using RaftNodeId = broker_id_t;

/// Sentinel for "no leader elected".
inline constexpr RaftNodeId kNoLeader = kNoBroker;

/// Monotonically increasing term number — Raft's logical clock.
using RaftTerm = term_t;

/// 1-based index into the replicated log.  Index 0 is the virtual sentinel.
using LogIndex = offset_t;

/// Sentinel for an uninitialised or missing log index / term.
inline constexpr LogIndex kNoLogIndex = 0;
inline constexpr RaftTerm  kNoTerm     = 0;

// ============================================================================
// RaftCommand — Commands replicated in the log
// ============================================================================

/// No-op; used during leader election to commit prior-term entries and assert
/// leadership.
struct NoOpCommand {};

/// Atomically change the cluster membership.  Supports joint consensus when
/// `joint_consensus` is true (both old_members and new_members active).
struct ConfigChangeCommand final {
    std::vector<RaftNodeId> new_members;
    bool joint_consensus = false;
    std::vector<RaftNodeId> old_members;  // only valid during joint consensus
};

/// Opaque user-level cluster operation (e.g. create topic, alter partition).
/// The business-logic layer handles serialisation.
struct UserCommand final {
    std::vector<uint8_t> payload;
};

/// The set of all command types that can appear in a Raft log entry.
using RaftCommand = std::variant<NoOpCommand, ConfigChangeCommand, UserCommand>;

// ============================================================================
// LogEntry — A single record in the replicated log
// ============================================================================

/// One entry in the replicated log, tagged with the term it was created in and
/// its 1-based index.  The variant command is dispatched by the state machine
/// once committed.
struct LogEntry final {
    RaftTerm  term    = kNoTerm;
    LogIndex  index   = kNoLogIndex;
    RaftCommand command;

    [[nodiscard]] bool is_noop() const noexcept {
        return std::holds_alternative<NoOpCommand>(command);
    }
    [[nodiscard]] bool is_config_change() const noexcept {
        return std::holds_alternative<ConfigChangeCommand>(command);
    }
};

// ============================================================================
// AppendEntries — Log replication / heartbeat
// ============================================================================

/// Leader-to-follower RPC for log replication.  An empty `entries` vector is a
/// heartbeat.
struct AppendEntriesRequest final {
    RaftTerm  term        = kNoTerm;      ///< Leader's current term.
    RaftNodeId leader_id  = kNoLeader;    ///< So followers can redirect clients.
    LogIndex  prev_log_index = 0;          ///< Index just before new entries.
    RaftTerm  prev_log_term  = kNoTerm;   ///< Term at prev_log_index (consistency check).
    std::vector<LogEntry> entries;         ///< Entries to store (empty = heartbeat).
    LogIndex  leader_commit = 0;           ///< Leader's commit index.
};

/// Follower's response to AppendEntries.
struct AppendEntriesResponse final {
    RaftTerm term          = kNoTerm;   ///< Current term; leader steps down if higher.
    bool     success       = false;     ///< True if log matched at prev_log_index/term.
    LogIndex last_log_index = 0;         ///< For fast log backtracking.
    LogIndex conflict_index = 0;         ///< First index of conflicting term (optimization).
};

// ============================================================================
// RequestVote — Leader election RPCs
// ============================================================================

/// Candidate (or pre-candidate) RPC requesting a vote.  When `pre_vote` is
/// true the term is NOT incremented — this probes viability first.
struct RequestVoteRequest final {
    RaftTerm  term          = kNoTerm;
    RaftNodeId candidate_id = kNoLeader;
    LogIndex  last_log_index = 0;         ///< For the log-up-to-date check.
    RaftTerm  last_log_term  = kNoTerm;
    bool      pre_vote       = false;     ///< Pre-Vote: don't increment term.
};

/// Response to a RequestVote (or pre-vote) RPC.
struct RequestVoteResponse final {
    RaftTerm term         = kNoTerm;   ///< Higher term causes candidate to step down.
    bool     vote_granted = false;
};

// ============================================================================
// InstallSnapshot — Chunked snapshot transfer
// ============================================================================

/// Leader-to-follower RPC to install a snapshot (streamed in chunks).
/// The final chunk sets `done = true`.
struct InstallSnapshotRequest final {
    RaftTerm  term               = kNoTerm;
    RaftNodeId leader_id         = kNoLeader;
    LogIndex  last_included_index = 0;    ///< Log prefix replaced by this snapshot.
    RaftTerm  last_included_term  = kNoTerm;
    uint64_t  offset             = 0;     ///< Byte offset into this chunk.
    std::vector<uint8_t> data;            ///< Raw chunk payload.
    bool      done               = false; ///< True if final chunk.
};

/// Response to an InstallSnapshot RPC.
struct InstallSnapshotResponse final {
    RaftTerm term = kNoTerm;   ///< Higher term forces the leader to step down.
};

// ============================================================================
// RaftNodeState — Node lifecycle states
// ============================================================================

/// The four states a Raft node can be in.  PreCandidate supports the Pre-Vote
/// optimization: the node probes peers without incrementing its term, avoiding
/// unnecessary term inflation during transient partitions.
enum class RaftNodeState : uint8_t {
    Follower     = 0,  ///< Passive; responds to leader RPCs.
    Candidate    = 1,  ///< Active election; requests votes from peers.
    Leader       = 2,  ///< Handles client requests and replicates the log.
    PreCandidate = 3,  ///< Pre-vote probe; does not increment term.
};

/// Human-readable name for a RaftNodeState value.
[[nodiscard]] constexpr std::string_view to_string(RaftNodeState s) noexcept {
    switch (s) {
    case RaftNodeState::Follower:     return "Follower";
    case RaftNodeState::Candidate:    return "Candidate";
    case RaftNodeState::Leader:       return "Leader";
    case RaftNodeState::PreCandidate: return "PreCandidate";
    }
    return "Unknown";
}

// ============================================================================
// RaftConfig — Tunable protocol parameters
// ============================================================================

/// Configuration for the Raft engine with production-grade defaults for a
/// message-queue control plane (150 ms heartbeat, 150–300 ms randomised
/// election timeout).
struct RaftConfig final {
    std::chrono::milliseconds heartbeat_interval{150};
    std::chrono::milliseconds min_election_timeout{150};
    std::chrono::milliseconds max_election_timeout{300};

    size_t max_entries_per_append     = 100;                     ///< Entries per AppendEntries.
    size_t max_append_bytes           = 4 * 1024 * 1024;         ///< 4 MiB per message.
    size_t rpc_queue_capacity         = 256;                     ///< Ring buffer (must be pow2).

    bool enable_pre_vote              = true;                    ///< Use PreCandidate state.
    bool enable_leader_lease          = true;                    ///< Linearizable reads w/o RTT.
    std::chrono::milliseconds max_clock_drift{10};               ///< Lease clock drift allowance.

    int32_t  rpc_retry_count          = 3;                       ///< Retries before peer marked down.
    std::chrono::milliseconds rpc_retry_backoff{50};

    std::string log_directory         = "/var/lib/torrent/raft/log";
    std::string snapshot_directory    = "/var/lib/torrent/raft/snapshots";

    size_t snapshot_threshold_entries = 10'000;                  ///< Trigger snapshot after N entries.
    size_t snapshot_threshold_bytes   = 64 * 1024 * 1024;        ///< … or 64 MiB.
    size_t snapshot_max_concurrent_chunks = 4;
    size_t snapshot_chunk_size        = 1024 * 1024;             ///< 1 MiB per chunk.
};

// ============================================================================
// RaftMembership — Cluster topology (replicated via ConfigChangeCommand)
// ============================================================================

/// Tracks the set of nodes in the cluster.  During a joint-consensus
/// transition both `members` (old) and `joint_members` (new) are active;
/// quorum requires a majority of both sets.
struct RaftMembership final {
    std::vector<RaftNodeId> members;         ///< Current (committed) configuration.
    std::vector<RaftNodeId> joint_members;   ///< New config during joint consensus.
    LogIndex config_index = 0;               ///< Log index where config was committed.

    [[nodiscard]] bool in_joint_consensus() const noexcept {
        return !joint_members.empty();
    }

    /// Union of old + new configs (deduplicated).  Used for quorum during
    /// joint consensus; returns just `members` otherwise.
    [[nodiscard]] std::vector<RaftNodeId> quorum_set() const {
        if (!in_joint_consensus()) return members;
        std::vector<RaftNodeId> result = members;
        for (auto id : joint_members) {
            if (std::find(members.begin(), members.end(), id) == members.end())
                result.push_back(id);
        }
        return result;
    }

    /// Quorum size: floor(n/2)+1 (max of both configs during joint consensus).
    [[nodiscard]] size_t quorum_size() const noexcept {
        size_t q_old = (members.size() / 2) + 1;
        if (!in_joint_consensus()) return q_old;
        size_t q_new = (joint_members.size() / 2) + 1;
        return std::max(q_old, q_new);
    }
};

// ============================================================================
// LeaderLease — Linearizable reads without log round-trips
// ============================================================================

/// Tracks the leader's lease, extended on each successful AppendEntries
/// heartbeat majority.  While valid (accounting for clock drift), the leader
/// may serve reads locally.
struct LeaderLease final {
    std::chrono::steady_clock::time_point expiration{
        std::chrono::steady_clock::time_point::min()};
    RaftTerm lease_term = kNoTerm;

    /// True if the lease hasn't expired, optionally accounting for clock drift.
    [[nodiscard]] bool is_valid(
        std::chrono::nanoseconds drift = std::chrono::nanoseconds{0}
    ) const noexcept {
        return std::chrono::steady_clock::now() + drift < expiration;
    }

    /// Acquire or extend the lease for `duration` from now.
    void extend(std::chrono::milliseconds duration, RaftTerm term) noexcept {
        expiration = std::chrono::steady_clock::now() + duration;
        lease_term = term;
    }

    /// Immediately invalidate (e.g. on step-down).
    void revoke() noexcept {
        expiration = std::chrono::steady_clock::time_point::min();
        lease_term = kNoTerm;
    }
};

// ============================================================================
// RaftSnapshot — Compacted log prefix persisted to disk
// ============================================================================

/// Metadata for a snapshot that replaces all log entries up to
/// `last_included_index`.  Snapshots are created when log thresholds are
/// exceeded and streamed to lagging followers via InstallSnapshot RPCs.
struct RaftSnapshot final {
    std::string file_path;             ///< Path on disk to the snapshot file.
    LogIndex last_included_index = 0; ///< Last log index captured (inclusive).
    RaftTerm last_included_term  = kNoTerm;
    RaftMembership membership;        ///< Membership config as of this snapshot.
    uint64_t file_size_bytes     = 0;
    timestamp_ms_t created_at_ms = 0;

    [[nodiscard]] bool is_valid() const noexcept {
        return !file_path.empty() && last_included_index > 0;
    }
};

// ============================================================================
// RaftLog — Abstract replicated write-ahead log interface
// ============================================================================

/// Interface for the persistent Raft log.  Implementations may use on-disk
/// segments, RocksDB, or in-memory stores (testing).  Only exposes operations
/// the Raft core algorithm requires.
class RaftLog {
public:
    virtual ~RaftLog() = default;

    // -- Metadata --
    [[nodiscard]] virtual LogIndex first_index() const noexcept = 0;
    [[nodiscard]] virtual LogIndex last_index()  const noexcept = 0;
    [[nodiscard]] virtual size_t   entry_count() const noexcept = 0;

    // -- Read --
    [[nodiscard]] virtual RaftTerm term_at(LogIndex index) const noexcept = 0;

    /// Returns count of entries read into `entries` for [start, end].
    [[nodiscard]] virtual size_t get_entries(
        LogIndex start, LogIndex end,
        std::vector<LogEntry>& entries) const = 0;

    [[nodiscard]] virtual std::optional<LogEntry> entry_at(
        LogIndex index) const = 0;

    // -- Write (leader only) --
    /// Appends entries after validating prev_log_index/term.  Returns the
    /// index of the first new entry, or kNoLogIndex on failure.
    [[nodiscard]] virtual LogIndex append(
        LogIndex prev_log_index, RaftTerm prev_log_term,
        std::vector<LogEntry> entries) = 0;

    // -- Truncation --
    /// Truncate suffix: leader overwrites inconsistent follower log.
    virtual void truncate_suffix(LogIndex last_index) = 0;
    /// Truncate prefix: discards entries before `first_index` after snapshot.
    virtual void truncate_prefix(LogIndex first_index) = 0;

    // -- Snapshot --
    /// Apply a snapshot, discarding all entries <= snapshot.last_included_index.
    /// The snapshot's last_included_term remains accessible via term_at().
    virtual void apply_snapshot(const RaftSnapshot& snapshot) = 0;
};

} // namespace torrent::raft
