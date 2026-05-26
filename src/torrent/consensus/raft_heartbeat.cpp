/**
 * raft_heartbeat.cpp — Leader Heartbeat & Pipeline Replication
 *
 * Implements the leader heartbeat loop and pipelined log replication:
 *
 *   Heartbeat Loop
 *     Periodic empty AppendEntries to prevent follower election timeouts
 *     and assert continued leadership.  Wakes every heartbeat_interval_ms.
 *
 *   Pipeline Replication
 *     Multiple in-flight AppendEntries per follower (bounded by
 *     rpc_queue_capacity).  Entries are sent optimistically; ordering
 *     is maintained per-follower.
 *
 *   Follower State Tracking
 *     Per-follower: next_index, match_index, in_flight count,
 *     last_ack timestamp.  Used for heartbeat response processing,
 *     commit index advancement, and tracking inactive peers.
 *
 *   Leader Lease
 *     Time-bound guarantee that the leader is still recognised.
 *     Extended on successful heartbeat majority acknowledgement.
 *     Expires after heartbeat_interval * 1.5 + clock_drift.
 *
 *   Batch Replication
 *     Coalesces entries up to max_entries_per_append / max_append_bytes
 *     per RPC to reduce per-entry framing overhead.
 *
 * All I/O is delegated to caller-provided callbacks, making this module
 * transport-agnostic and testable without network dependencies.
 *
 * References:
 *   Ongaro §3.5 (Log Replication), §3.5.3 (Commitment Rule)
 *   Raft §6.4 (Leader Leases)
 */

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "torrent/common/types.h"
#include "torrent/consensus/raft_types.h"

namespace torrent::raft {

// ============================================================================
// Internal Constants
// ============================================================================

/// Minimum heartbeat interval to prevent thrashing.
static constexpr auto kMinHeartbeatInterval = std::chrono::milliseconds{20};

/// Maximum heartbeat interval (sanity cap).
static constexpr auto kMaxHeartbeatInterval = std::chrono::seconds{5};

/// Fraction of heartbeat interval added to lease extension for jitter buffer.
static constexpr double kLeaseExtensionFactor = 1.5;

/// Number of consecutive heartbeat failures before marking a follower inactive.
static constexpr size_t kMaxHeartbeatFailures = 3;

/// Period after which we attempt to reactivate an inactive follower.
static constexpr auto kFollowerReactivationInterval = std::chrono::seconds{5};

/// Maximum number of in-flight AppendEntries per follower (pipeline depth).
static constexpr size_t kDefaultPipelineDepth = 8;

/// Maximum entries per single AppendEntries batch (hard cap).
static constexpr size_t kHardMaxEntriesPerAppend = 1000;

/// Maximum number of backtrack rounds before falling back to snapshot.
static constexpr size_t kMaxBacktrackRounds = 10;

/// Default entries per append when not specified.
static constexpr size_t kDefaultMaxEntriesPerAppend = 100;

/// Default max bytes per append.
static constexpr size_t kDefaultMaxAppendBytes = 4 * 1024 * 1024;

// ============================================================================
// Anonymous helpers
// ============================================================================

namespace {

std::shared_ptr<spdlog::logger> get_heartbeat_logger() {
    static auto logger = spdlog::get("raft_heartbeat");
    if (!logger) {
        logger = spdlog::stdout_color_mt("raft_heartbeat");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

[[nodiscard]] std::chrono::steady_clock::time_point steady_now() {
    return std::chrono::steady_clock::now();
}

/// Approximate byte size of a LogEntry for batch limit checks.
[[nodiscard]] size_t entry_byte_size(const LogEntry& entry) {
    size_t sz = sizeof(LogEntry);
    if (auto* uc = std::get_if<UserCommand>(&entry.command)) {
        sz += uc->payload.size();
    } else if (auto* cc = std::get_if<ConfigChangeCommand>(&entry.command)) {
        sz += cc->new_members.size() * sizeof(RaftNodeId);
        sz += cc->old_members.size() * sizeof(RaftNodeId);
    }
    return sz;
}

} // anonymous namespace

// ============================================================================
// FollowerTracker — Per-follower replication state
// ============================================================================

/**
 * Tracks replication state for a single follower.
 *
 * Maintains the next index to send, the highest matched index, pipeline
 * depth, and liveness status.  Used by the heartbeat/replication engine
 * to decide what to send and when to retry.
 */
struct FollowerTracker {
    RaftNodeId peer_id;
    LogIndex next_index = 1;
    LogIndex match_index = 0;
    size_t in_flight = 0;
    bool is_active = true;
    std::chrono::steady_clock::time_point last_ack = steady_now();
    size_t consecutive_failures = 0;
    size_t backtrack_rounds = 0;
    std::chrono::steady_clock::time_point inactive_since{};

    /// Reset the follower state for a new leader term.
    void reset_for_leader(LogIndex leader_last_index) {
        next_index = leader_last_index + 1;
        match_index = 0;
        in_flight = 0;
        is_active = true;
        consecutive_failures = 0;
        backtrack_rounds = 0;
        last_ack = steady_now();
        inactive_since = {};
    }

    /// Mark follower as inactive after repeated failures.
    void mark_inactive() {
        if (!is_active) return;
        is_active = false;
        inactive_since = steady_now();
        get_heartbeat_logger()->warn(
            "FollowerTracker: peer {} marked inactive after {} failures",
            peer_id, consecutive_failures);
    }

    /// Reactivate a follower (e.g., after a successful reconnect).
    void reactivate() {
        is_active = true;
        consecutive_failures = 0;
        backtrack_rounds = 0;
        last_ack = steady_now();
        get_heartbeat_logger()->info(
            "FollowerTracker: peer {} reactivated", peer_id);
    }

    /// Check if enough time has passed to attempt reactivation.
    [[nodiscard]] bool should_reactivate() const noexcept {
        if (is_active) return false;
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            steady_now() - inactive_since);
        return elapsed >= kFollowerReactivationInterval;
    }
};

// ============================================================================
// LeaderLease — Time-bound read lease
// ============================================================================

/**
 * Leader lease for serving linearizable reads without contacting followers.
 *
 * Extended on each successful heartbeat round (majority acknowledgement).
 * While the lease is valid (accounting for clock drift), the leader may
 * serve reads locally.  The lease is revoked on step-down.
 */
class LeaderLease {
public:
    /**
     * @param heartbeat_interval  Base heartbeat interval (used to compute
     *                            lease duration).
     * @param clock_drift        Maximum expected clock drift between nodes.
     */
    LeaderLease(std::chrono::milliseconds heartbeat_interval,
                std::chrono::milliseconds clock_drift)
        : heartbeat_interval_(heartbeat_interval)
        , clock_drift_(clock_drift)
        , expiration_(std::chrono::steady_clock::time_point::min())
        , lease_term_(kNoTerm)
    {}

    /// Extend or acquire the lease for the current term.
    void extend(RaftTerm current_term) {
        auto duration = std::chrono::milliseconds(
            static_cast<long long>(
                heartbeat_interval_.count() * kLeaseExtensionFactor));
        expiration_ = steady_now() + duration;
        lease_term_ = current_term;
        get_heartbeat_logger()->debug(
            "LeaderLease: extended for {}ms (term={})",
            duration.count(), current_term);
    }

    /// Revoke the lease immediately (on step-down).
    void revoke() noexcept {
        expiration_ = std::chrono::steady_clock::time_point::min();
        lease_term_ = kNoTerm;
        get_heartbeat_logger()->debug("LeaderLease: revoked");
    }

    /// True if the lease is currently valid for the given term.
    [[nodiscard]] bool is_valid(RaftTerm current_term) const noexcept {
        if (expiration_ == std::chrono::steady_clock::time_point::min())
            return false;
        if (lease_term_ != current_term) return false;

        auto now = steady_now();
        // Account for clock drift: the lease expires when now + drift
        // exceeds the expiration deadline.
        return (now + clock_drift_) < expiration_;
    }

    /// Remaining lease time (0 if expired).
    [[nodiscard]] std::chrono::milliseconds remaining() const noexcept {
        if (expiration_ == std::chrono::steady_clock::time_point::min())
            return std::chrono::milliseconds{0};
        auto now = steady_now();
        if (now >= expiration_) return std::chrono::milliseconds{0};
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            expiration_ - now);
    }

    [[nodiscard]] RaftTerm lease_term() const noexcept { return lease_term_; }
    [[nodiscard]] std::chrono::milliseconds heartbeat_interval() const noexcept {
        return heartbeat_interval_;
    }

private:
    std::chrono::milliseconds heartbeat_interval_;
    std::chrono::milliseconds clock_drift_;
    std::chrono::steady_clock::time_point expiration_;
    RaftTerm lease_term_;
};

// ============================================================================
// HeartbeatEngine — Leader heartbeat and replication manager
// ============================================================================

/**
 * Orchestrates periodic heartbeats, pipelined log replication, and
 * commit-index advancement.
 *
 * Callbacks let the caller provide log access, send RPCs, and apply
 * committed entries.  The engine is single-threaded — the caller must
 * serialise access (typically from a dedicated heartbeat thread).
 *
 * Usage:
 * @code
 *   HeartbeatEngine engine(id, config, log_provider, ae_sender, apply_fn);
 *
 *   // Called every heartbeat tick:
 *   engine.on_heartbeat_tick();
 *
 *   // Called when new entries are appended (proposal):
 *   engine.on_new_entries();
 *
 *   // Called when an AppendEntries response arrives:
 *   engine.handle_append_entries_response(from, resp);
 * @endcode
 */
class HeartbeatEngine {
public:
    // -- Callback types ----------------------------------------------------

    /// Return the leader's current term.
    using TermProvider = std::function<RaftTerm()>;

    /// Return the log's last committed index.
    using CommitIndexProvider = std::function<LogIndex()>;

    /// Return the log's last index.
    using LastIndexProvider = std::function<LogIndex()>;

    /// Return the term at a given log index.
    using TermAtFn = std::function<RaftTerm(LogIndex)>;

    /// Read a range of log entries [start, end].
    using GetEntriesFn = std::function<size_t(
        LogIndex start, LogIndex end,
        std::vector<LogEntry>& entries)>;

    /// Deliver an AppendEntries RPC to a peer.
    using AppendEntriesSender = std::function<void(
        RaftNodeId target, const AppendEntriesRequest& req)>;

    /// Deliver an InstallSnapshot RPC to a peer (for lagging followers).
    using InstallSnapshotSender = std::function<void(
        RaftNodeId target, const InstallSnapshotRequest& req)>;

    /// Called when the commit index advances (entries to apply).
    using CommitNotifier = std::function<void(LogIndex new_commit_index)>;

    /// Provide the current membership set.
    using MembershipProvider = std::function<std::vector<RaftNodeId>()>;

    /// Provide the quorum size (may change during joint consensus).
    using QuorumProvider = std::function<size_t()>;

    /// Called when a follower should be sent a snapshot.
    using SnapshotNeededFn = std::function<void(RaftNodeId target)>;

    // -- Construction -----------------------------------------------------

    /**
     * @param node_id              Leader's cluster-unique ID.
     * @param heartbeat_interval   Interval between heartbeat rounds.
     * @param max_entries_per_append Max entries per AppendEntries batch.
     * @param max_append_bytes     Max bytes per AppendEntries batch.
     * @param pipeline_depth       Max in-flight AppendEntries per follower.
     * @param enable_lease         Whether to enable leader leases.
     * @param clock_drift          Maximum expected clock drift.
     * @param term_provider        Returns current term.
     * @param last_index_provider  Returns last log index.
     * @param term_at              Returns term at a given log index.
     * @param get_entries          Reads log entries.
     * @param ae_sender            Sends AppendEntries RPCs.
     * @param is_sender            Sends InstallSnapshot RPCs.
     * @param commit_notifier      Notified when commit advances.
     * @param membership_provider  Returns current peers.
     * @param quorum_provider      Returns current quorum size.
     * @param snapshot_needed      Called when a follower needs a snapshot.
     */
    HeartbeatEngine(RaftNodeId node_id,
                    std::chrono::milliseconds heartbeat_interval,
                    size_t max_entries_per_append,
                    size_t max_append_bytes,
                    size_t pipeline_depth,
                    bool enable_lease,
                    std::chrono::milliseconds clock_drift,
                    TermProvider term_provider,
                    LastIndexProvider last_index_provider,
                    TermAtFn term_at,
                    GetEntriesFn get_entries,
                    AppendEntriesSender ae_sender,
                    InstallSnapshotSender is_sender,
                    CommitNotifier commit_notifier,
                    MembershipProvider membership_provider,
                    QuorumProvider quorum_provider,
                    SnapshotNeededFn snapshot_needed)
        : node_id_(node_id)
        , heartbeat_interval_(
              std::clamp(heartbeat_interval, kMinHeartbeatInterval,
                         kMaxHeartbeatInterval))
        , max_entries_per_append_(
              std::clamp(max_entries_per_append, size_t{1},
                         kHardMaxEntriesPerAppend))
        , max_append_bytes_(std::max(max_append_bytes, size_t{4096}))
        , pipeline_depth_(std::max(pipeline_depth, size_t{1}))
        , lease_(heartbeat_interval_, clock_drift)
        , lease_enabled_(enable_lease)
        , term_provider_(std::move(term_provider))
        , last_index_provider_(std::move(last_index_provider))
        , term_at_(std::move(term_at))
        , get_entries_(std::move(get_entries))
        , ae_sender_(std::move(ae_sender))
        , is_sender_(std::move(is_sender))
        , commit_notifier_(std::move(commit_notifier))
        , membership_provider_(std::move(membership_provider))
        , quorum_provider_(std::move(quorum_provider))
        , snapshot_needed_(std::move(snapshot_needed))
    {
        get_heartbeat_logger()->info(
            "HeartbeatEngine: node {} initialised (hb={}ms, max_ents={}, "
            "pipeline={}, lease={})",
            node_id_, heartbeat_interval_.count(), max_entries_per_append_,
            pipeline_depth_, lease_enabled_);
    }

    // -- Initialisation (called when node becomes leader) ------------------

    /// Reset all follower state for a new leader term.
    void init_for_leader() {
        followers_.clear();
        auto members = membership_provider_();
        LogIndex last_idx = last_index_provider_();

        for (auto peer : members) {
            if (peer == node_id_) continue;
            FollowerTracker ft;
            ft.peer_id = peer;
            ft.reset_for_leader(last_idx);
            followers_[peer] = std::move(ft);
        }

        get_heartbeat_logger()->info(
            "HeartbeatEngine: node {} initialised leader state "
            "({} followers, last_log={})",
            node_id_, followers_.size(), last_idx);
    }

    /// Clean up leader state on step-down.
    void clear_for_follower() {
        followers_.clear();
        lease_.revoke();
        get_heartbeat_logger()->info(
            "HeartbeatEngine: node {} cleared leader state", node_id_);
    }

    // -- Heartbeat tick ---------------------------------------------------

    /**
     * Called every heartbeat interval.  Sends empty AppendEntries
     * (heartbeats) to all followers.
     *
     * Also checks leader health: if we can't reach a quorum of followers,
     * the caller should consider stepping down.
     *
     * @return true if the leader can still reach a quorum.
     */
    [[nodiscard]] bool send_heartbeats() {
        RaftTerm current_term = term_provider_();
        LogIndex commit_idx = last_index_provider_();
        LogIndex last_idx = commit_idx;

        size_t sent = 0;
        auto members = membership_provider_();

        for (auto peer : members) {
            if (peer == node_id_) continue;

            auto it = followers_.find(peer);
            if (it == followers_.end()) {
                // New follower that joined via config change.
                FollowerTracker ft;
                ft.peer_id = peer;
                ft.reset_for_leader(last_idx);
                followers_[peer] = std::move(ft);
                it = followers_.find(peer);
            }

            if (!it->second.is_active) {
                // Skip inactive followers; try to reactivate if enough
                // time has passed.
                if (it->second.should_reactivate()) {
                    it->second.reactivate();
                } else {
                    continue;
                }
            }

            AppendEntriesRequest req;
            req.term = current_term;
            req.leader_id = node_id_;
            req.prev_log_index = it->second.next_index - 1;
            req.prev_log_term = term_at_(req.prev_log_index);
            req.leader_commit = commit_idx;
            // entries empty → heartbeat

            ae_sender_(peer, req);

            it->second.in_flight++;
            sent++;
        }

        // Single-node cluster: always quorum.
        if (members.size() <= 1) {
            if (lease_enabled_) lease_.extend(current_term);
            return true;
        }

        get_heartbeat_logger()->trace(
            "HeartbeatEngine: node {} sent {} heartbeats (term={})",
            node_id_, sent, current_term);

        return check_leader_health();
    }

    /**
     * Replicate pending entries to all followers.
     *
     * For each follower, builds a batch of entries starting from
     * next_index, respecting batch limits and pipeline depth.
     * Followers whose next_index precedes the log's first_index
     * get an InstallSnapshot instead.
     */
    void replicate_to_followers(LogIndex first_log_index) {
        RaftTerm current_term = term_provider_();
        LogIndex commit_idx = last_index_provider_();
        LogIndex last_idx = commit_idx;

        auto members = membership_provider_();
        if (members.empty()) return;

        for (auto peer : members) {
            if (peer == node_id_) continue;

            auto it = followers_.find(peer);
            if (it == followers_.end() || !it->second.is_active) continue;

            FollowerTracker& ft = it->second;

            // Respect pipeline depth: don't send more if pipe is full.
            if (ft.in_flight >= pipeline_depth_) {
                get_heartbeat_logger()->trace(
                    "HeartbeatEngine: peer {} pipeline full (in_flight={})",
                    peer, ft.in_flight);
                continue;
            }

            // No new entries to send.
            if (ft.next_index > last_idx) continue;

            // Follower has fallen behind; send snapshot.
            if (ft.next_index <= first_log_index) {
                get_heartbeat_logger()->info(
                    "HeartbeatEngine: peer {} behind log (next={}, "
                    "first_log={}) — requesting snapshot",
                    peer, ft.next_index, first_log_index);
                if (snapshot_needed_) {
                    snapshot_needed_(peer);
                }
                continue;
            }

            // Build a batch.
            std::vector<LogEntry> batch;
            LogIndex batch_end = std::min(
                ft.next_index + static_cast<LogIndex>(max_entries_per_append_) - 1,
                last_idx);

            size_t batch_bytes = 0;
            std::vector<LogEntry> entries;
            size_t got = get_entries_(ft.next_index, batch_end, entries);

            for (size_t i = 0; i < got; ++i) {
                LogEntry& e = entries[i];
                size_t esz = entry_byte_size(e);
                if (batch_bytes + esz > max_append_bytes_ && !batch.empty()) {
                    break;
                }
                batch_bytes += esz;
                batch.push_back(std::move(e));
            }

            if (batch.empty()) {
                // Shouldn't happen if next_index <= last_idx, but handle.
                continue;
            }

            AppendEntriesRequest req;
            req.term = current_term;
            req.leader_id = node_id_;
            req.prev_log_index = ft.next_index - 1;
            req.prev_log_term = term_at_(req.prev_log_index);
            req.entries = std::move(batch);
            req.leader_commit = commit_idx;

            ae_sender_(peer, req);
            ft.in_flight++;

            get_heartbeat_logger()->trace(
                "HeartbeatEngine: replicated {} entries (idx {}-{}) "
                "to peer {} ({} bytes)",
                req.entries.size(),
                req.entries.front().index,
                req.entries.back().index,
                peer, batch_bytes);
        }
    }

    // -- AppendEntries response handler -----------------------------------

    /**
     * Process an AppendEntries response from a follower.
     *
     * @param from   Peer that sent the response.
     * @param resp   The response message.
     * @param current_commit_index  Current leader commit index (to advance).
     * @return       Structure indicating actions for the caller.
     */
    struct AppendResult {
        bool leader_should_step_down = false;  // Higher term seen.
        bool commit_advanced = false;          // New entries committed.
        LogIndex new_commit_index = 0;
        bool follower_needs_snapshot = false;  // Too many backtrack rounds.
        RaftTerm higher_term = kNoTerm;
    };

    [[nodiscard]] AppendResult handle_append_entries_response(
        RaftNodeId from,
        const AppendEntriesResponse& resp)
    {
        AppendResult result;

        RaftTerm current_term = term_provider_();

        // --- Higher term: step down ---
        if (resp.term > current_term) {
            get_heartbeat_logger()->info(
                "HeartbeatEngine: node {} discovered higher term {} > {} "
                "from peer {}",
                node_id_, resp.term, current_term, from);
            result.leader_should_step_down = true;
            result.higher_term = resp.term;
            return result;
        }

        auto it = followers_.find(from);
        if (it == followers_.end()) {
            get_heartbeat_logger()->debug(
                "HeartbeatEngine: response from unknown follower {} — ignoring",
                from);
            return result;
        }

        FollowerTracker& ft = it->second;

        // Decrement in_flight (even on failure — the RPC round finished).
        if (ft.in_flight > 0) ft.in_flight--;

        if (resp.success) {
            // --- Success: update match_index, advance next_index ---
            // The follower acknowledged all entries up to its last log index.
            // We can compute the effective match index from the response.
            LogIndex new_match = resp.last_log_index;

            if (new_match > ft.match_index) {
                ft.match_index = new_match;
                ft.next_index = new_match + 1;
            }

            ft.consecutive_failures = 0;
            ft.backtrack_rounds = 0;
            ft.last_ack = steady_now();
            ft.is_active = true;

            get_heartbeat_logger()->trace(
                "HeartbeatEngine: peer {} ack'd up to idx={} "
                "(match={}, next={})",
                from, new_match, ft.match_index, ft.next_index);

            // --- Try to advance commit ---
            result.commit_advanced = try_advance_commit();
            if (result.commit_advanced) {
                result.new_commit_index = last_index_provider_();
            }

            // --- Extend lease on heartbeat success ---
            maybe_extend_lease();

        } else {
            // --- Log inconsistency: backtrack ---
            ft.consecutive_failures++;
            ft.backtrack_rounds++;

            get_heartbeat_logger()->debug(
                "HeartbeatEngine: peer {} AppendEntries failed "
                "(failures={}, backtrack={})",
                from, ft.consecutive_failures, ft.backtrack_rounds);

            // Decrement next_index for exponential backtracking, using
            // conflict_index hint if provided for faster convergence.
            if (resp.conflict_index > 0 && resp.conflict_index < ft.next_index) {
                ft.next_index = resp.conflict_index;
            } else if (ft.next_index > 1) {
                ft.next_index--;
            }

            // Too many backtrack rounds → send snapshot.
            if (ft.backtrack_rounds >= kMaxBacktrackRounds) {
                get_heartbeat_logger()->warn(
                    "HeartbeatEngine: peer {} exceeded max backtrack "
                    "rounds ({} >= {}) — triggering snapshot",
                    from, ft.backtrack_rounds, kMaxBacktrackRounds);
                result.follower_needs_snapshot = true;
                ft.backtrack_rounds = 0;
            }

            // Mark inactive after repeated failures.
            if (ft.consecutive_failures >= kMaxHeartbeatFailures) {
                ft.mark_inactive();
            }
        }

        return result;
    }

    // -- Commit advancement ------------------------------------------------

    /**
     * Attempt to advance the commit index.
     *
     * For each N > current_commit through last_index, if N's term equals
     * the current term AND a quorum of followers has match_index >= N,
     * advance the commit index.
     *
     * @return true if commit index was advanced.
     */
    [[nodiscard]] bool try_advance_commit() {
        RaftTerm current_term = term_provider_();
        LogIndex last_idx = last_index_provider_();
        size_t qsize = quorum_provider_();

        // Scan backward from the last index for efficiency; we only need
        // the highest N that satisfies the quorum condition.
        for (LogIndex n = last_idx; n > 0; --n) {
            if (term_at_(n) != current_term) continue;

            size_t count = 1; // self
            for (auto& [peer, ft] : followers_) {
                if (ft.match_index >= n) count++;
            }

            if (count >= qsize) {
                if (n > last_index_provider_()) {
                    get_heartbeat_logger()->info(
                        "HeartbeatEngine: advancing commit {} -> {} "
                        "(quorum={}/{})",
                        last_index_provider_(), n, count, qsize);
                    if (commit_notifier_) {
                        commit_notifier_(n);
                    }
                    return true;
                }
                break; // Already committed at or above n; nothing to do.
            }
        }

        return false;
    }

    // -- Lease management -------------------------------------------------

    /// Extend the leader lease if the lease feature is enabled.
    void maybe_extend_lease() {
        if (!lease_enabled_) return;

        RaftTerm current_term = term_provider_();

        // Check if a majority of followers acknowledged the last heartbeat.
        size_t acked = 1; // self
        auto now = steady_now();
        auto hb_window = heartbeat_interval_ * 2;

        for (auto& [peer, ft] : followers_) {
            if (ft.is_active && (now - ft.last_ack) < hb_window) {
                acked++;
            }
        }

        size_t qsize = quorum_provider_();
        if (acked >= qsize) {
            lease_.extend(current_term);
            get_heartbeat_logger()->trace(
                "HeartbeatEngine: lease extended ({} of {} acked)",
                acked, qsize);
        }
    }

    /// Revoke the lease (on step-down).
    void revoke_lease() { lease_.revoke(); }

    /// Check if the lease is currently valid for reads.
    [[nodiscard]] bool lease_valid() const noexcept {
        if (!lease_enabled_) return false;
        return lease_.is_valid(term_provider_());
    }

    // -- Health check -----------------------------------------------------

    /**
     * Check if the leader can reach a quorum of active followers.
     * @return true if quorum is reachable.
     */
    [[nodiscard]] bool check_leader_health() const {
        size_t active = 1; // self
        auto now = steady_now();
        auto threshold = heartbeat_interval_ * 3;

        for (auto& [peer, ft] : followers_) {
            if (ft.is_active && (now - ft.last_ack) < threshold) {
                active++;
            }
        }

        size_t qsize = quorum_provider_();
        return active >= qsize;
    }

    // -- Follower management ----------------------------------------------

    /// Attempt to reactivate followers that have been inactive long enough.
    void try_reactivate_followers() {
        for (auto& [peer, ft] : followers_) {
            if (!ft.is_active && ft.should_reactivate()) {
                ft.reactivate();
            }
        }
    }

    /// Remove a follower (e.g., after a config change removes a node).
    void remove_follower(RaftNodeId peer) {
        followers_.erase(peer);
        get_heartbeat_logger()->info(
            "HeartbeatEngine: removed follower {} ({} remaining)",
            peer, followers_.size());
    }

    /// Add or update a follower (config change adds a node).
    void add_follower(RaftNodeId peer) {
        if (peer == node_id_) return;
        if (followers_.find(peer) != followers_.end()) return;

        FollowerTracker ft;
        ft.peer_id = peer;
        ft.reset_for_leader(last_index_provider_());
        followers_[peer] = std::move(ft);

        get_heartbeat_logger()->info(
            "HeartbeatEngine: added follower {} ({} total)",
            peer, followers_.size());
    }

    // -- Queries ----------------------------------------------------------

    [[nodiscard]] const FollowerTracker* follower(RaftNodeId peer) const {
        auto it = followers_.find(peer);
        return it != followers_.end() ? &it->second : nullptr;
    }

    [[nodiscard]] size_t follower_count() const noexcept {
        return followers_.size();
    }

    [[nodiscard]] size_t active_follower_count() const noexcept {
        size_t count = 0;
        for (auto& [_, ft] : followers_) {
            if (ft.is_active) count++;
        }
        return count;
    }

    [[nodiscard]] const LeaderLease& lease() const noexcept { return lease_; }
    [[nodiscard]] std::chrono::milliseconds heartbeat_interval() const noexcept {
        return heartbeat_interval_;
    }

    /// Collect match indices for all followers (useful for commit calculation).
    [[nodiscard]] std::vector<LogIndex> match_indices() const {
        std::vector<LogIndex> result;
        result.reserve(followers_.size());
        for (auto& [_, ft] : followers_) {
            result.push_back(ft.match_index);
        }
        return result;
    }

private:
    RaftNodeId node_id_;
    std::chrono::milliseconds heartbeat_interval_;
    size_t max_entries_per_append_;
    size_t max_append_bytes_;
    size_t pipeline_depth_;

    LeaderLease lease_;
    bool lease_enabled_;

    TermProvider term_provider_;
    LastIndexProvider last_index_provider_;
    TermAtFn term_at_;
    GetEntriesFn get_entries_;
    AppendEntriesSender ae_sender_;
    InstallSnapshotSender is_sender_;
    CommitNotifier commit_notifier_;
    MembershipProvider membership_provider_;
    QuorumProvider quorum_provider_;
    SnapshotNeededFn snapshot_needed_;

    std::unordered_map<RaftNodeId, FollowerTracker> followers_;
};

// ============================================================================
// Free functions: heartbeat utilities
// ============================================================================

/**
 * Validate and clamp a heartbeat interval to safe bounds.
 */
std::chrono::milliseconds clamp_heartbeat_interval(
    std::chrono::milliseconds desired) noexcept
{
    return std::clamp(desired, kMinHeartbeatInterval, kMaxHeartbeatInterval);
}

/**
 * Compute a recommended leader lease duration from the heartbeat interval.
 * The lease is heartbeat_interval * kLeaseExtensionFactor.
 */
std::chrono::milliseconds compute_lease_duration(
    std::chrono::milliseconds heartbeat_interval) noexcept
{
    return std::chrono::milliseconds(
        static_cast<long long>(
            heartbeat_interval.count() * kLeaseExtensionFactor));
}

/**
 * Check if the leader should voluntarily step down due to partition.
 * Returns true if the leader cannot reach a quorum within the given
 * election timeout window.
 *
 * @param active_followers   Number of followers with recent acks.
 * @param quorum_size        Required quorum.
 * @param time_since_last_hb Time since the last heartbeat round started.
 * @param max_election_timeout Used as a threshold: step down after
 *                             3 × max_election_timeout without quorum.
 */
bool should_leader_step_down(
    size_t active_followers,
    size_t quorum_size,
    std::chrono::milliseconds time_since_last_hb,
    std::chrono::milliseconds max_election_timeout) noexcept
{
    size_t total_reachable = active_followers + 1; // +1 for self
    if (total_reachable >= quorum_size) return false;
    return time_since_last_hb > (max_election_timeout * 3);
}

} // namespace torrent::raft
