#pragma once

/**
 * raft.h — Full Raft Consensus Protocol Implementation
 *
 * Implements the complete Raft consensus algorithm (Ongaro 2014) with
 * production-grade extensions:
 *
 *   Pre-Vote (PreCandidate state)
 *     Probes peers for viability before incrementing the term, avoiding
 *     unnecessary term inflation during transient network partitions.
 *
 *   Leader Leases
 *     Time-bound guarantee that the leader is still recognised, enabling
 *     linearizable reads without log round-trips.  Extended on heartbeat
 *     majority acknowledgement; expires after heartbeat / 2 + clock drift.
 *
 *   Pipeline Replication
 *     Multiple in-flight AppendEntries per follower (bounded by
 *     rpc_queue_capacity), saturating the link and amortising RTT.
 *
 *   Batch Replication
 *     Entries coalesced up to max_entries_per_append / max_append_bytes
 *     per RPC, reducing per-entry framing overhead.
 *
 *   Joint Consensus Membership Changes
 *     Safe reconfiguration via intermediate joint config requiring
 *     dual-majority commit (majorities of both old and new sets).
 *
 *   Fencing via Term Numbers
 *     Higher-term RPC forces immediate step-down; a deposed leader cannot
 *     replicate because followers reject its stale term.
 *
 *   Chunked InstallSnapshot
 *     Log compaction: snapshots replace log prefix; streamed to lagging
 *     followers in configurable chunks.
 *
 *   Quorum-Based Commit
 *     Entry committed once a majority of the relevant config has acked.
 *     During joint consensus, dual-majority required.  Leader commits only
 *     entries from its own term (the commitment rule from §3.5.3).
 *
 *   Leader Step-Down on Partition
 *     Voluntary abdication when the leader cannot reach a quorum.
 *
 *   Leadership Transfer
 *     Graceful handoff: replicate to target, signal immediate election, step
 *     down.  Falls back to normal operation on failure.
 *
 * Thread Safety
 *   shared_mutex: queries use shared_lock (concurrent), mutations use
 *   exclusive_lock (serialised).  Background election and heartbeat threads
 *   acquire exclusive lock briefly per tick.
 *
 * Usage
 *   @code
 *   RaftNode node(id, cfg, std::move(log), apply_fn, ae_send, rv_send, is_send);
 *   node.start();
 *   auto res = node.propose(UserCommand{payload});
 *   @endcode
 *
 * @see raft_types.h for the vocabulary types.
 */

#include <memory>
#include <atomic>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <chrono>
#include <functional>
#include <vector>
#include <unordered_map>
#include <queue>
#include <random>

#include "torrent/common/types.h"
#include "torrent/consensus/raft_types.h"

namespace torrent::raft {

// ============================================================================
// Transport-agnostic RPC callbacks
// ============================================================================

/// Delivers AppendEntries RPC.  Caller routes async response back via
/// handle_append_entries_response().
using AppendEntriesSender = std::function<void(
    RaftNodeId target, const AppendEntriesRequest& req)>;

/// Delivers RequestVote or Pre-Vote RPC.  Response routed via
/// handle_request_vote_response().
using RequestVoteSender = std::function<void(
    RaftNodeId target, const RequestVoteRequest& req)>;

/// Delivers InstallSnapshot RPC.  Response routed via
/// handle_install_snapshot_response().
using InstallSnapshotSender = std::function<void(
    RaftNodeId target, const InstallSnapshotRequest& req)>;

/// Called for each committed entry, in strict index order.  MUST be
/// idempotent — entries may be replayed across restarts from last_applied.
using StateMachineApply = std::function<void(const LogEntry& entry)>;

// ============================================================================
// RaftNode — Complete Raft consensus engine
// ============================================================================

/**
 * Owns persistent log storage and background threads; delegates network I/O
 * and state-machine application to caller-supplied callbacks.
 *
 * State machine:
 *
 *   start() ──► Follower ──(election timeout)──► PreCandidate
 *                  ▲                                 │
 *                  │                      majority pre-votes?
 *                  │                         yes ▼        no ▼
 *                  │                      Candidate      Follower
 *                  │                           │
 *                  │                   majority votes?
 *                  │                      yes ▼
 *                  │                     Leader
 *                  │                        │
 *                  └──(higher term seen)────┘
 */
class RaftNode {
public:
    // ========================================================================
    // Construction / Destruction
    // ========================================================================

    /**
     * Construct a Raft node.  The node is NOT started — call start().
     *
     * @param node_id    Cluster-unique identifier (maps 1:1 to broker_id_t).
     * @param config     Tuning parameters: heartbeats, timeouts, snapshot
     *                   thresholds, pre-vote toggle, pipeline limits, etc.
     *                   Copied; external mutations after construction ignored.
     * @param log        Persistent Raft log implementation (ownership
     *                   transferred to RaftNode).
     * @param apply_fn   Called for each committed entry in index order.
     *                   Must be idempotent.
     * @param ae_sender  Delivers AppendEntries RPCs to peers.
     * @param rv_sender  Delivers RequestVote / Pre-Vote RPCs to peers.
     * @param is_sender  Delivers InstallSnapshot RPCs to peers.
     */
    RaftNode(RaftNodeId node_id,
             RaftConfig config,
             std::unique_ptr<RaftLog> log,
             StateMachineApply apply_fn,
             AppendEntriesSender ae_sender,
             RequestVoteSender rv_sender,
             InstallSnapshotSender is_sender);

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;
    RaftNode(RaftNode&&) = delete;
    RaftNode& operator=(RaftNode&&) = delete;

    /// If still running, calls shutdown() and joins background threads.
    ~RaftNode();

    // -- Lifecycle -----------------------------------------------------------

    /**
     * Start the Raft node.  Spawns two background threads:
     *   1. Election timer — random timeout; fires on_election_timeout().
     *      Reset on every AppendEntries from leader.  Quiesced on Leader.
     *   2. Heartbeat — active only on Leader.  Wakes every heartbeat_interval
     *      ms to send AppendEntries (heartbeat or with entries).
     *
     * Node initialises in Follower state.
     * @throws std::logic_error if already started.
     */
    void start();

    /**
     * Graceful shutdown: sets running_ = false, notifies the condition
     * variable, joins both threads.  Transitions to Follower, revokes any
     * leader lease.  Safe to call multiple times.
     */
    void shutdown();

    // -- Client API ----------------------------------------------------------

    /**
     * Propose a command for replication.
     *
     * Leader path:
     *   1. Append LogEntry at current_term_.
     *   2. Wake heartbeat thread → replicate_to_followers().
     *   3. Return assigned log index immediately (does NOT wait for commit).
     *
     * Non-leader path: returns error_code::not_leader.
     *
     * During config change (pending_config_index_ != 0), new ConfigChange
     * proposals rejected with error_code::reassignment_in_progress.
     * During leadership transfer, proposals rejected with error_code::not_leader.
     *
     * @return result<LogIndex> with assigned index, or error.
     */
    [[nodiscard]] result<LogIndex> propose(RaftCommand command);

    // -- Query (shared_lock — concurrent reads allowed) ---------------------

    [[nodiscard]] bool is_leader() const noexcept;
    [[nodiscard]] RaftNodeId current_leader() const noexcept;
    [[nodiscard]] RaftTerm get_term() const noexcept;
    [[nodiscard]] RaftNodeState get_state() const noexcept;
    [[nodiscard]] const RaftConfig& config() const noexcept { return config_; }
    [[nodiscard]] const RaftMembership& membership() const noexcept;
    [[nodiscard]] LogIndex get_commit_index() const noexcept;
    [[nodiscard]] LogIndex get_last_applied() const noexcept;

    /// True if Leader and lease unexpired (allows serving local linearizable
    /// reads without contacting followers).
    [[nodiscard]] bool lease_valid() const noexcept;

    // -- Manual Transitions --------------------------------------------------

    /// Force step-down: Leader → Follower (revokes lease, clears follower
    /// state).  No-op for other states.
    void step_down();

    /**
     * Graceful leadership transfer to a designated peer.
     *
     * Leader: stops accepting proposals, replicates outstanding entries to
     * target, signals immediate election, steps down.  Falls back to normal
     * operation if target is unreachable or not up-to-date.
     */
    void transfer_leadership(RaftNodeId target);

    // -- Inbound RPC Handlers (synchronous; callers deliver the response) ---

    /**
     * Handle AppendEntries (§3.5).
     *
     *   - Reject if req.term < current_term_.
     *   - If req.term > current_term_: become_follower(req.term).
     *   - Reset election timeout (valid leader heard from).
     *   - Log consistency: check entry at prev_log_index has term
     *     prev_log_term.  On match: truncate conflicting suffix, append
     *     entries.  On mismatch: return failure with conflict_index hint
     *     for fast leader backtracking.
     *   - Advance commit_index to min(req.leader_commit, last_new_index).
     */
    AppendEntriesResponse handle_append_entries(const AppendEntriesRequest& req);

    /**
     * Handle RequestVote (§3.6, §9.6).
     *
     * Standard vote (pre_vote=false):
     *   - Reject if req.term < current_term_.
     *   - If req.term > current_term_: become_follower(req.term).
     *   - Grant if (a) haven't voted this term or already voted for this
     *     candidate, AND (b) candidate's log is at least as up-to-date
     *     (log_is_up_to_date).
     *   - On grant: persist voted_for, reset election timer.
     *
     * Pre-Vote (pre_vote=true):
     *   - Reject if req.term < current_term_.
     *   - Grant based solely on log-up-to-date check.
     *   - Do NOT persist voted_for or update term (non-binding).
     */
    RequestVoteResponse handle_request_vote(const RequestVoteRequest& req);

    /**
     * Handle InstallSnapshot chunk (§3.7 + chunked extension).
     *
     *   - Reject if req.term < current_term_.
     *   - If req.term > current_term_: become_follower(req.term).
     *   - First chunk (offset==0): initialise snapshot_buffer_.
     *   - Subsequent chunks: append data at given offset.
     *   - Final chunk (done==true): persist snapshot, call
     *     log_->apply_snapshot(), truncate log prefix.
     */
    InstallSnapshotResponse handle_install_snapshot(
        const InstallSnapshotRequest& req);

    // -- Outbound RPC Response Handlers (called by transport layer) ---------

    /**
     * Process AppendEntries response from a follower.
     *
     * Success: update match_index to prev_log_index + entries.size(),
     *   advance next_index to match_index + 1, decrement in_flight,
     *   trigger advance_commit_index() and extend_lease().
     * Failure (log inconsistency): decrement next_index (using
     *   conflict_index hint if provided) and retry.
     * If resp.term > current_term_: become_follower(resp.term).
     */
    void handle_append_entries_response(RaftNodeId from,
                                        const AppendEntriesResponse& resp);

    /**
     * Process RequestVote response.
     *
     * Candidate: if resp.term > current_term_ → Follower.  If vote_granted,
     *   increment votes_received_; on majority → become_leader().
     * PreCandidate: if vote_granted, increment pre_votes_received_; on
     *   majority → become_candidate() (real election); on timeout → revert.
     */
    void handle_request_vote_response(RaftNodeId from,
                                      const RequestVoteResponse& resp);

    /// Process InstallSnapshot response: advance follower state on success;
    /// if resp.term > current_term_, become_follower().
    void handle_install_snapshot_response(
        RaftNodeId from, const InstallSnapshotResponse& resp);

    // -- Snapshot -----------------------------------------------------------

    /// Create snapshot at commit_index, persist metadata, truncate log prefix
    /// via log_->truncate_prefix().  Returns metadata.
    RaftSnapshot create_snapshot();

private:
    // ========================================================================
    // State Transitions (require exclusive lock on mutex_)
    // ========================================================================

    /**
     * Become Follower.  If new_term > current_term_: update and persist term,
     * clear voted_for.  Reset election deadline, revoke lease, clear
     * follower_states_, signal election thread.
     */
    void become_follower(RaftTerm new_term);

    /**
     * Become PreCandidate.  Does NOT increment term.  Sends Pre-Vote probes
     * to all peers.  On majority → become_candidate(); on timeout →
     * become_follower().  Avoids term inflation during partitions.
     */
    void become_pre_candidate();

    /**
     * Become Candidate.  Increment current_term_ (persisted), vote for self,
     * reset election deadline to now + random_election_timeout(), send
     * RequestVote to all peers.  On majority → Leader; on higher term →
     * Follower; on timeout → new election (incremented term, retry).
     */
    void become_candidate();

    /**
     * Become Leader.  Initialise follower_states_ (next_index = last+1,
     * match_index = 0 for each peer).  Append NoOp to commit prior-term
     * entries.  Send immediate heartbeat to assert authority.  Start
     * heartbeat thread, quiesce election thread.
     */
    void become_leader();

    // -- Election Subsystem ------------------------------------------------

    /// Election timer fired: → PreCandidate (if enabled) or Candidate.
    void on_election_timeout();

    /// Random timeout in [min_election_timeout, max_election_timeout] via
    /// Mersenne Twister (seeded with node_id ^ clock for de-correlation).
    std::chrono::milliseconds random_election_timeout();

    /// Broadcast RequestVote to all members. pre_vote=true → Pre-Vote probes.
    void start_vote_round(bool pre_vote);

    /// Tally votes or pre-votes; decide whether to become_leader(),
    /// become_candidate(), or revert to Follower.
    void tally_votes(bool pre_vote);

    // -- Leader Replication Subsystem --------------------------------------

    /// Send empty AppendEntries to all followers.  Extends lease on majority
    /// ack; prevents follower election timeouts.
    void send_heartbeats();

    /**
     * Replicate entries to followers.  Pipelined: multiple in-flight per
     * follower (bounded by rpc_queue_capacity).  Batched: up to
     * max_entries_per_append / max_append_bytes per RPC.  For followers
     * whose next_index <= log_->first_index(), sends InstallSnapshot.
     */
    void replicate_to_followers();

    /**
     * Build AppendEntries batch for [follower.next_index, last_index],
     * bounded by batch limits.  Respects pipeline: defers if in_flight >=
     * rpc_queue_capacity.
     */
    void send_append_entries_to(RaftNodeId target);

    /**
     * Advance commit_index.  For each N > commit_index_ through
     * last_log_index, if term_at(N) == current_term_ and a majority of the
     * relevant config (dual-majority during joint consensus) has
     * match_index >= N, set commit_index_ = N and trigger apply_committed().
     */
    void advance_commit_index();

    /// Apply entries [last_applied_+1, commit_index_] via apply_fn_.
    /// ConfigChangeCommand entries are intercepted → process_config_change().
    void apply_committed();

    /// Extend leader lease to heartbeat_interval + max_clock_drift from now.
    void extend_lease();

    // -- Snapshot / Log Compaction -----------------------------------------

    /// Send chunked InstallSnapshot to lagging follower.  On success,
    /// advance follower's next_index past snapshot.
    void send_install_snapshot_to(RaftNodeId target);

    /// Check thresholds (entry count, bytes); create snapshot if exceeded.
    void maybe_create_snapshot();

    // -- Membership / Joint Consensus --------------------------------------

    /**
     * Process committed ConfigChangeCommand.
     *
     * Not in joint consensus:
     *   joint=false → direct membership update.
     *   joint=true  → enter joint consensus (set joint_members).
     *
     * In joint consensus:
     *   Only joint=false accepted; new_members must match pending
     *   joint_members → finalize_joint_consensus().
     */
    void process_config_change(const ConfigChangeCommand& cmd);

    /// Finalize joint consensus: members = joint_members, clear joint_members.
    void finalize_joint_consensus();

    // -- Log Helpers -------------------------------------------------------

    /**
     * Raft §3.6.1 log-up-to-date check.  Remote log is at least as up-to-
     * date if its last term is higher, or same term with >= index.
     */
    bool log_is_up_to_date(LogIndex remote_last_index,
                           RaftTerm remote_last_term) const;

    /**
     * Validate prev_log_index / prev_log_term consistency.  On match:
     * truncate conflicting suffix, append entries.  On failure: set
     * conflict_index to first index of the conflicting term for fast
     * leader backtracking.
     */
    bool append_entries_to_log(const AppendEntriesRequest& req,
                               LogIndex& conflict_index);

    /// Delegates to RaftMembership::quorum_size() (handles joint consensus).
    size_t quorum_size() const;

private:
    // ========================================================================
    // Fields
    // ========================================================================

    const RaftNodeId node_id_;
    const RaftConfig config_;
    std::unique_ptr<RaftLog> log_;
    StateMachineApply apply_fn_;
    AppendEntriesSender ae_sender_;
    RequestVoteSender rv_sender_;
    InstallSnapshotSender is_sender_;

    // -- Persistent state (survives restarts) --

    RaftTerm current_term_ = kNoTerm;               ///< Monotonically non-decreasing.
    RaftNodeId voted_for_ = kNoLeader;               ///< One vote per term.

    // -- Volatile state (all roles) --

    RaftNodeState state_ = RaftNodeState::Follower;
    RaftNodeId leader_id_ = kNoLeader;               ///< For client redirection.
    LogIndex commit_index_ = 0;                      ///< Highest committed index.
    LogIndex last_applied_ = 0;                      ///< <= commit_index_.

    // -- Leader-only volatile state --

    /// Per-follower replication tracking.  Valid only when Leader.
    struct FollowerState {
        LogIndex next_index = 1;                     ///< Next entry to send.
        LogIndex match_index = 0;                    ///< Highest index replicated.
        bool is_active = true;
        size_t in_flight = 0;                        ///< Pipeline depth.
        std::optional<InstallSnapshotRequest> pending_snapshot;
        std::chrono::steady_clock::time_point last_ack;
    };
    std::unordered_map<RaftNodeId, FollowerState> follower_states_;

    // -- Election state --

    size_t votes_received_ = 0;
    size_t pre_votes_received_ = 0;
    std::chrono::steady_clock::time_point last_heartbeat_;
    std::chrono::steady_clock::time_point election_deadline_;

    // -- Membership --

    RaftMembership membership_;
    LogIndex pending_config_index_ = 0;              ///< Blocks concurrent config changes.

    // -- Leader lease --

    LeaderLease lease_;                              ///< Valid only when Leader.

    // -- Threading --

    mutable std::shared_mutex mutex_;
    std::thread election_thread_;
    std::thread heartbeat_thread_;
    std::unordered_map<RaftNodeId, int> heartbeat_failures_;
    std::condition_variable cv_;
    std::atomic<bool> running_{false};
    std::atomic<bool> transferring_{false};          ///< Suppresses proposals during handoff.

    // -- Misc --

    std::mt19937 rng_;                               ///< Seeded with node_id ^ clock.
    std::vector<uint8_t> snapshot_buffer_;            ///< Follower snapshot reception.
    InstallSnapshotRequest pending_snapshot_meta_;
};

// ============================================================================
// Inline Query Implementations
// ============================================================================

inline bool RaftNode::is_leader() const noexcept {
    std::shared_lock lock(mutex_);
    return state_ == RaftNodeState::Leader;
}

inline RaftNodeId RaftNode::current_leader() const noexcept {
    std::shared_lock lock(mutex_);
    return leader_id_;
}

inline RaftTerm RaftNode::get_term() const noexcept {
    std::shared_lock lock(mutex_);
    return current_term_;
}

inline RaftNodeState RaftNode::get_state() const noexcept {
    std::shared_lock lock(mutex_);
    return state_;
}

inline const RaftMembership& RaftNode::membership() const noexcept {
    std::shared_lock lock(mutex_);
    return membership_;
}

inline LogIndex RaftNode::get_commit_index() const noexcept {
    std::shared_lock lock(mutex_);
    return commit_index_;
}

inline LogIndex RaftNode::get_last_applied() const noexcept {
    std::shared_lock lock(mutex_);
    return last_applied_;
}

inline bool RaftNode::lease_valid() const noexcept {
    std::shared_lock lock(mutex_);
    if (state_ != RaftNodeState::Leader || !config_.enable_leader_lease)
        return false;
    return lease_.is_valid(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            config_.max_clock_drift));
}

// ============================================================================
// Inline Private Helpers
// ============================================================================

inline std::chrono::milliseconds RaftNode::random_election_timeout() {
    long long lo = config_.min_election_timeout.count();
    long long hi = config_.max_election_timeout.count();
    return std::chrono::milliseconds{
        std::uniform_int_distribution<long long>{lo, hi}(rng_)};
}

inline bool RaftNode::log_is_up_to_date(LogIndex remote_last_index,
                                         RaftTerm remote_last_term) const {
    RaftTerm local_last_term = log_->term_at(log_->last_index());
    if (remote_last_term != local_last_term)
        return remote_last_term > local_last_term;
    return remote_last_index >= log_->last_index();
}

inline size_t RaftNode::quorum_size() const {
    return membership_.quorum_size();
}

} // namespace torrent::raft
