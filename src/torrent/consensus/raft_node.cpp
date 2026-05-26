/**
 * raft_node.cpp — Full Raft Consensus Implementation
 *
 * Implements leader election (with pre-vote), log replication (pipeline +
 * batch), heartbeats, leader leases, snapshotting, membership changes with
 * joint consensus, and commit/apply pipeline.  Production quality with
 * comprehensive error handling and term-based fencing.
 *
 * References:
 *   Ongaro, D. (2014). "Consensus: Bridging Theory and Practice"
 *   Raft §3.5 (Log Replication), §3.6 (Leader Election), §3.7 (Snapshotting)
 *   Raft §4 (Cluster Membership Changes), §9.6 (Pre-Vote)
 *
 * Thread safety: all public methods acquire the appropriate lock (shared for
 * queries, exclusive for mutations).  Background threads acquire exclusive
 * lock briefly per tick.
 *
 * See raft.h for the API contract and architectural overview.
 */

#include "torrent/consensus/raft.h"
#include "torrent/consensus/raft_types.h"
#include "torrent/common/types.h"
#include "torrent/network/transport.h"

#include <spdlog/spdlog.h>
#include <algorithm>
#include <random>
#include <thread>
#include <cmath>
#include <cstring>
#include <sstream>
#include <fstream>
#include <filesystem>
#include <numeric>
#include <iomanip>

namespace torrent::raft {

using namespace std::chrono_literals;
namespace fs = std::filesystem;

// ============================================================================
// Internal Constants
// ============================================================================

/// Minimum time between two successive election timeout firings (prevents
/// election storms when the clock is coarse or timeouts are very short).
static constexpr auto kMinElectionInterval = 10ms;

/// Fraction of heartbeat interval added to lease to account for jitter in
/// the heartbeat delivery / processing path.
static constexpr double kLeaseExtensionFactor = 1.5;

/// Maximum number of probe rounds while backtracking a follower's log before
/// giving up and sending InstallSnapshot.
static constexpr size_t kMaxBacktrackRounds = 10;

/// Upper bound on the number of committed entries to apply in one batch
/// (prevents the state machine from being starved by a single apply_committed).
static constexpr size_t kMaxApplyBatch = 1024;

/// Maximum number of entries in a single AppendEntries batch when compression
/// or framing is expensive.  Overrides config max_entries_per_append if set.
static constexpr size_t kHardMaxEntriesPerAppend = 1000;

/// Maximum time to wait for a follower to become active after being marked
/// inactive due to RPC failures.
static constexpr auto kFollowerReactivationInterval = 5s;

/// Number of consecutive heartbeat failures before a follower is considered
/// inactive.
static constexpr size_t kMaxHeartbeatFailures = 3;

/// Maximum number of in-flight snapshots per node.
static constexpr size_t kMaxConcurrentSnapshots = 2;

/// Grace period after receiving a valid AppendEntries before an election
/// timeout can fire (prevents premature elections during network jitter).
static constexpr auto kElectionGracePeriod = 5ms;

// ============================================================================
// Helper: time utilities
// ============================================================================

static std::chrono::steady_clock::time_point steady_now() {
    return std::chrono::steady_clock::now();
}

static timestamp_ms_t timestamp_now_ms() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               now.time_since_epoch())
        .count();
}

// ============================================================================
// Helper: state name for logging
// ============================================================================

static const char* state_name(RaftNodeState s) {
    switch (s) {
    case RaftNodeState::Follower:     return "Follower";
    case RaftNodeState::Candidate:    return "Candidate";
    case RaftNodeState::Leader:       return "Leader";
    case RaftNodeState::PreCandidate: return "PreCandidate";
    }
    return "Unknown";
}

// ============================================================================
// Helper: binary I/O for snapshots
// ============================================================================

static bool read_file_binary(const std::string& path,
                              std::vector<uint8_t>& data) {
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in) {
        SPDLOG_ERROR("raft: cannot open file for reading: {}", path);
        return false;
    }
    auto size = in.tellg();
    if (size < 0) {
        SPDLOG_ERROR("raft: cannot determine file size: {}", path);
        return false;
    }
    data.resize(static_cast<size_t>(size));
    in.seekg(0, std::ios::beg);
    in.read(reinterpret_cast<char*>(data.data()), size);
    if (!in) {
        SPDLOG_ERROR("raft: read failed for file: {}", path);
        return false;
    }
    return true;
}

static bool write_file_atomic(const std::string& path,
                               const std::vector<uint8_t>& data) {
    std::string tmp = path + ".tmp." + std::to_string(timestamp_now_ms());
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out) {
        SPDLOG_ERROR("raft: cannot open snapshot temp file {}", tmp);
        return false;
    }
    out.write(reinterpret_cast<const char*>(data.data()),
              static_cast<std::streamsize>(data.size()));
    out.close();
    if (!out) {
        SPDLOG_ERROR("raft: write failed for snapshot temp file {}", tmp);
        std::error_code ec;
        fs::remove(tmp, ec);
        return false;
    }
    std::error_code ec;
    fs::rename(tmp, path, ec);
    if (ec) {
        SPDLOG_ERROR("raft: atomic rename failed {} -> {}: {}",
                     tmp, path, ec.message());
        return false;
    }
    return true;
}

// ============================================================================
// Helper: ensure directory exists
// ============================================================================

static bool ensure_directory(const std::string& path) {
    std::error_code ec;
    if (fs::exists(path, ec)) return true;
    if (!fs::create_directories(path, ec)) {
        SPDLOG_ERROR("raft: cannot create directory {}: {}", path, ec.message());
        return false;
    }
    return true;
}

// ============================================================================
// Helper: compute byte size of a LogEntry (approximate)
// ============================================================================

static size_t log_entry_byte_size(const LogEntry& entry) {
    size_t size = sizeof(LogEntry);
    if (auto* uc = std::get_if<UserCommand>(&entry.command)) {
        size += uc->payload.size();
    } else if (auto* cc = std::get_if<ConfigChangeCommand>(&entry.command)) {
        size += cc->new_members.size() * sizeof(RaftNodeId);
        size += cc->old_members.size() * sizeof(RaftNodeId);
    }
    return size;
}

// ============================================================================
// Helper: validate that a node id is in the membership set
// ============================================================================

static bool is_member(const std::vector<RaftNodeId>& members,
                       RaftNodeId id) {
    return std::find(members.begin(), members.end(), id) != members.end();
}

// ============================================================================
// Constructor
// ============================================================================

RaftNode::RaftNode(RaftNodeId node_id,
                   RaftConfig config,
                   std::unique_ptr<RaftLog> log,
                   StateMachineApply apply_fn,
                   AppendEntriesSender ae_sender,
                   RequestVoteSender rv_sender,
                   InstallSnapshotSender is_sender)
    : node_id_(node_id)
    , config_(std::move(config))
    , log_(std::move(log))
    , apply_fn_(std::move(apply_fn))
    , ae_sender_(std::move(ae_sender))
    , rv_sender_(std::move(rv_sender))
    , is_sender_(std::move(is_sender))
    , rng_(static_cast<uint32_t>(
          node_id ^ std::chrono::steady_clock::now().time_since_epoch().count()))
{
    // --- Pre-condition checks ---
    if (!log_) {
        throw std::invalid_argument(
            "RaftNode: log must not be null (node_id="
            + std::to_string(node_id) + ")");
    }
    if (!apply_fn_) {
        throw std::invalid_argument(
            "RaftNode: apply_fn must not be null (node_id="
            + std::to_string(node_id) + ")");
    }
    if (!ae_sender_) {
        throw std::invalid_argument(
            "RaftNode: ae_sender must not be null (node_id="
            + std::to_string(node_id) + ")");
    }
    if (!rv_sender_) {
        throw std::invalid_argument(
            "RaftNode: rv_sender must not be null (node_id="
            + std::to_string(node_id) + ")");
    }
    if (!is_sender_) {
        throw std::invalid_argument(
            "RaftNode: is_sender must not be null (node_id="
            + std::to_string(node_id) + ")");
    }

    // --- Config validation ---
    if (config_.heartbeat_interval <= 0ms) {
        SPDLOG_WARN("raft: node {} heartbeat_interval {}ms is non-positive, "
                    "clamping to 50ms",
                    node_id_, config_.heartbeat_interval.count());
        config_.heartbeat_interval = 50ms;
    }
    if (config_.min_election_timeout <= 0ms) {
        config_.min_election_timeout = 50ms;
    }
    if (config_.max_election_timeout < config_.min_election_timeout) {
        config_.max_election_timeout = config_.min_election_timeout * 2;
    }
    if (config_.max_append_bytes == 0) {
        config_.max_append_bytes = 4 * 1024 * 1024;
    }
    if (config_.snapshot_chunk_size == 0) {
        config_.snapshot_chunk_size = 1024 * 1024;
    }

    // --- Ensure directories exist ---
    ensure_directory(config_.log_directory);
    ensure_directory(config_.snapshot_directory);

    SPDLOG_INFO("raft: node {} created (pre-vote={}, pipeline_cap={}, "
                "batch={}, lease={}, election=[{},{}]ms, hb={}ms)",
                node_id_,
                config_.enable_pre_vote,
                config_.rpc_queue_capacity,
                config_.max_entries_per_append,
                config_.enable_leader_lease,
                config_.min_election_timeout.count(),
                config_.max_election_timeout.count(),
                config_.heartbeat_interval.count());
}

// ============================================================================
// Destructor
// ============================================================================

RaftNode::~RaftNode() {
    shutdown();
}

// ============================================================================
// Lifecycle: start()
// ============================================================================

void RaftNode::start() {
    {
        std::unique_lock lock(mutex_);
        if (running_.exchange(true)) {
            throw std::logic_error(
                "RaftNode::start() called when already running (node_id="
                + std::to_string(node_id_) + ")");
        }
        state_ = RaftNodeState::Follower;
        leader_id_ = kNoLeader;
        election_deadline_ = steady_now() + random_election_timeout();
        last_heartbeat_ = steady_now();
        heartbeat_failures_.clear();
        backtrack_rounds_.clear();

        SPDLOG_INFO("raft: node {} starting as Follower (term={}, log=[{},{}])",
                    node_id_, current_term_,
                    log_->first_index(), log_->last_index());
    }

    // --- Spawn election timer thread ---
    election_thread_ = std::thread([this]() {
        SPDLOG_DEBUG("raft: node {} election thread started", node_id_);
        while (running_.load(std::memory_order_acquire)) {
            std::unique_lock lock(mutex_);

            auto now = steady_now();
            auto deadline = election_deadline_;
            RaftNodeState current_state = state_;

            if (current_state == RaftNodeState::Leader) {
                // Leader doesn't run elections; wait until step-down or
                // shutdown is signalled.
                cv_.wait(lock, [this] {
                    return !running_.load(std::memory_order_acquire)
                        || state_ != RaftNodeState::Leader;
                });
                continue;
            }

            // Sleep until deadline, or until woken by an AppendEntries
            // heartbeat (which resets the deadline).
            if (deadline > now) {
                cv_.wait_until(lock, deadline, [this, deadline] {
                    return !running_.load(std::memory_order_acquire)
                        || steady_now() >= deadline;
                });
                if (!running_.load(std::memory_order_acquire)) break;
            }

            // Re-check state after wakeup; a heartbeat may have arrived
            // while we were waiting and reset the timer.
            now = steady_now();
            if (state_ != RaftNodeState::Leader
                && state_ != RaftNodeState::Follower
                    ? now >= election_deadline_
                    : (now >= election_deadline_
                       && now - last_heartbeat_
                              >= config_.min_election_timeout + kElectionGracePeriod)) {

                SPDLOG_INFO("raft: node {} election timeout fired "
                            "(state={}, term={}, last_hb={}ms ago)",
                            node_id_, state_name(state_), current_term_,
                            std::chrono::duration_cast<std::chrono::milliseconds>(
                                now - last_heartbeat_).count());

                on_election_timeout();
            }
        }
        SPDLOG_DEBUG("raft: node {} election thread stopped", node_id_);
    });

    // --- Spawn heartbeat / replication / snapshot thread ---
    heartbeat_thread_ = std::thread([this]() {
        SPDLOG_DEBUG("raft: node {} heartbeat thread started", node_id_);
        while (running_.load(std::memory_order_acquire)) {
            {
                std::unique_lock lock(mutex_);
                if (state_ != RaftNodeState::Leader) {
                    // Wait until we become leader or shut down.
                    cv_.wait(lock, [this] {
                        return !running_.load(std::memory_order_acquire)
                            || state_ == RaftNodeState::Leader;
                    });
                    if (!running_.load(std::memory_order_acquire)) break;
                }
            }

            // Leader loop: heartbeats, replication, snapshot check.
            {
                std::unique_lock lock(mutex_);
                if (state_ != RaftNodeState::Leader) continue;

                // --- Leader health check: detect partition ---
                auto now = steady_now();
                size_t active_followers = 0;
                for (auto& [peer, fs] : follower_states_) {
                    if (fs.is_active
                        && (now - fs.last_ack) < config_.heartbeat_interval * 3) {
                        active_followers++;
                    }
                }
                // If we can't reach a quorum, consider stepping down.
                size_t q_reached = active_followers + 1; // +1 for self
                if (q_reached < quorum_size()
                    && (now - last_heartbeat_) > config_.max_election_timeout * 3) {
                    SPDLOG_WARN("raft: node {} leader partition detected: "
                                "active={} quorum={} — stepping down",
                                node_id_, q_reached, quorum_size());
                    become_follower(current_term_);
                    continue;
                }

                send_heartbeats();
                replicate_to_followers();
                maybe_create_snapshot();
                try_reactivate_followers();
            }

            // Sleep for heartbeat interval.
            auto next_wake = steady_now() + config_.heartbeat_interval;
            {
                std::unique_lock lock(mutex_);
                cv_.wait_until(lock, next_wake, [this] {
                    return !running_.load(std::memory_order_acquire);
                });
            }
        }
        SPDLOG_DEBUG("raft: node {} heartbeat thread stopped", node_id_);
    });
}

// ============================================================================
// Lifecycle: shutdown()
// ============================================================================

void RaftNode::shutdown() {
    bool was_running = running_.exchange(false);
    if (!was_running) return;

    SPDLOG_INFO("raft: node {} shutting down (state={}, term={})",
                node_id_, state_name(state_), current_term_);

    // Step down to Follower, revoke lease, and notify threads.
    {
        std::unique_lock lock(mutex_);
        if (state_ == RaftNodeState::Leader) {
            SPDLOG_INFO("raft: node {} stepping down from leader before shutdown",
                        node_id_);
            lease_.revoke();
            follower_states_.clear();
            heartbeat_failures_.clear();
            backtrack_rounds_.clear();
        }
        state_ = RaftNodeState::Follower;
        leader_id_ = kNoLeader;
        transferring_.store(false, std::memory_order_release);
    }

    cv_.notify_all();

    if (election_thread_.joinable()) {
        election_thread_.join();
    }
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }

    SPDLOG_INFO("raft: node {} shut down complete (final term={}, commit={}, "
                "applied={})",
                node_id_, current_term_, commit_index_, last_applied_);
}

// ============================================================================
// State Transitions
// ============================================================================

void RaftNode::become_follower(RaftTerm new_term) {
    auto prev_state = state_;
    auto prev_term = current_term_;

    if (new_term > current_term_) {
        SPDLOG_INFO("raft: node {} advancing term {} -> {} and becoming Follower",
                    node_id_, current_term_, new_term);
        current_term_ = new_term;
        voted_for_ = kNoLeader;
    } else if (new_term == current_term_ && state_ == RaftNodeState::Follower) {
        // Already follower in this term; just reset the election timer
        // to avoid an immediate re-timeout.  This handles duplicate
        // AppendEntries heartbeats efficiently.
        election_deadline_ = steady_now() + random_election_timeout();
        cv_.notify_all();
        return;
    }

    // Clean up leader state if we were leader.
    if (prev_state == RaftNodeState::Leader) {
        SPDLOG_INFO("raft: node {} stepping down from Leader (term {} -> {})",
                    node_id_, prev_term, current_term_);
        lease_.revoke();
        follower_states_.clear();
        heartbeat_failures_.clear();
        backtrack_rounds_.clear();
        transferring_.store(false, std::memory_order_release);
    }

    // Clean up election state.
    if (prev_state == RaftNodeState::Candidate
        || prev_state == RaftNodeState::PreCandidate) {
        SPDLOG_INFO("raft: node {} abandoning {} for Follower (term={})",
                    node_id_, state_name(prev_state), current_term_);
    }

    state_ = RaftNodeState::Follower;
    leader_id_ = kNoLeader;
    votes_received_ = 0;
    pre_votes_received_ = 0;
    election_deadline_ = steady_now() + random_election_timeout();
    last_heartbeat_ = steady_now();

    // Notify background threads.
    cv_.notify_all();
}

void RaftNode::become_pre_candidate() {
    if (state_ == RaftNodeState::PreCandidate) {
        SPDLOG_DEBUG("raft: node {} already PreCandidate — skipping", node_id_);
        return;
    }
    if (state_ == RaftNodeState::Leader) {
        SPDLOG_WARN("raft: node {} is Leader — cannot become PreCandidate",
                    node_id_);
        return;
    }
    if (membership_.members.empty()) {
        SPDLOG_WARN("raft: node {} cannot become PreCandidate — "
                    "empty membership; staying Follower", node_id_);
        return;
    }
    if (!config_.enable_pre_vote) {
        SPDLOG_DEBUG("raft: node {} pre-vote disabled, going straight to Candidate",
                     node_id_);
        become_candidate();
        return;
    }

    SPDLOG_INFO("raft: node {} becoming PreCandidate (current term={}, "
                "probing for term {})",
                node_id_, current_term_, current_term_ + 1);

    state_ = RaftNodeState::PreCandidate;
    pre_votes_received_ = 1; // Implicit self-vote.
    votes_received_ = 0;

    start_vote_round(/*pre_vote=*/true);

    // Set a deadline for the pre-vote phase.  If we don't get a majority
    // before this deadline, we revert to Follower.
    election_deadline_ = steady_now() + random_election_timeout();
    cv_.notify_all();
}

void RaftNode::become_candidate() {
    if (membership_.members.empty()) {
        SPDLOG_WARN("raft: node {} cannot become Candidate — empty membership",
                    node_id_);
        become_follower(current_term_);
        return;
    }
    if (state_ == RaftNodeState::Leader) {
        SPDLOG_WARN("raft: node {} is Leader — cannot become Candidate",
                    node_id_);
        return;
    }

    // Increment term (persisted).
    current_term_++;
    voted_for_ = node_id_;
    votes_received_ = 1; // Self-vote.
    pre_votes_received_ = 0;

    SPDLOG_INFO("raft: node {} becoming Candidate for term {} "
                "(log=[{},{}], prev_state={})",
                node_id_, current_term_,
                log_->first_index(), log_->last_index(),
                state_name(state_));

    state_ = RaftNodeState::Candidate;
    leader_id_ = kNoLeader;
    election_deadline_ = steady_now() + random_election_timeout();

    start_vote_round(/*pre_vote=*/false);
    cv_.notify_all();
}

void RaftNode::become_leader() {
    if (membership_.members.empty()) {
        SPDLOG_ERROR("raft: node {} cannot become Leader — empty membership",
                     node_id_);
        become_follower(current_term_);
        return;
    }

    SPDLOG_INFO("raft: node {} becoming Leader for term {} "
                "(commit={}, applied={}, log=[{},{}])",
                node_id_, current_term_,
                commit_index_, last_applied_,
                log_->first_index(), log_->last_index());

    state_ = RaftNodeState::Leader;
    leader_id_ = node_id_;
    transferring_.store(false, std::memory_order_release);

    // Re-initialise per-follower tracking.
    LogIndex last_idx = log_->last_index();
    follower_states_.clear();
    heartbeat_failures_.clear();
    backtrack_rounds_.clear();

    for (auto peer : membership_.quorum_set()) {
        if (peer == node_id_) continue;
        FollowerState& fs = follower_states_[peer];
        fs.next_index = last_idx + 1;
        fs.match_index = 0;
        fs.is_active = true;
        fs.in_flight = 0;
        fs.last_ack = steady_now();
    }

    // --- Append NoOp to commit prior-term entries (Raft §3.5.3) ---
    // The leader commitment rule: a leader can only commit entries from
    // its own term.  Appending a NoOp at the current term ensures that
    // entries from prior terms become committed indirectly once this
    // NoOp is replicated to a majority.
    LogEntry noop;
    noop.term = current_term_;
    noop.index = log_->last_index() + 1;
    noop.command = NoOpCommand{};
    std::vector<LogEntry> entries{std::move(noop)};

    LogIndex appended = log_->append(log_->last_index(),
                                     log_->term_at(log_->last_index()),
                                     entries);
    if (appended == kNoLogIndex) {
        SPDLOG_ERROR("raft: node {} failed to append leader NoOp — "
                     "log persistence error; stepping down", node_id_);
        become_follower(current_term_);
        return;
    }

    SPDLOG_DEBUG("raft: node {} appended leader NoOp at index {}",
                 node_id_, entries[0].index);

    // --- Extend leader lease ---
    if (config_.enable_leader_lease) {
        extend_lease();
    }

    // --- Immediate heartbeat round to assert authority ---
    send_heartbeats();

    // Wake heartbeat thread.
    cv_.notify_all();
}

// ============================================================================
// Election Subsystem
// ============================================================================

void RaftNode::on_election_timeout() {
    if (state_ == RaftNodeState::Leader) {
        SPDLOG_WARN("raft: node {} on_election_timeout called while Leader — ignoring",
                    node_id_);
        return;
    }

    if (config_.enable_pre_vote && state_ != RaftNodeState::Candidate) {
        become_pre_candidate();
    } else {
        become_candidate();
    }
}

void RaftNode::start_vote_round(bool pre_vote) {
    RaftTerm request_term = pre_vote ? current_term_ + 1 : current_term_;
    LogIndex last_idx = log_->last_index();
    RaftTerm last_term = log_->term_at(last_idx);

    RequestVoteRequest req;
    req.term = request_term;
    req.candidate_id = node_id_;
    req.last_log_index = last_idx;
    req.last_log_term = last_term;
    req.pre_vote = pre_vote;

    SPDLOG_INFO("raft: node {} broadcasting {}RequestVote "
                "(term={}, last_log={}:{}) to {} peers",
                node_id_,
                pre_vote ? "Pre-" : "",
                request_term, last_idx, last_term,
                membership_.quorum_set().size() - 1);

    size_t sent = 0;
    size_t failed = 0;

    for (auto peer : membership_.quorum_set()) {
        if (peer == node_id_) continue;
        try {
            rv_sender_(peer, req);
            sent++;
        } catch (const std::exception& e) {
            SPDLOG_WARN("raft: node {} failed to send {}RequestVote to {}: {}",
                        node_id_,
                        pre_vote ? "Pre-" : "",
                        peer, e.what());
            failed++;
        }
    }

    if (failed > 0) {
        SPDLOG_WARN("raft: node {} vote round: {}/{} RPC sends failed",
                    node_id_, failed, sent + failed);
    }
}

void RaftNode::tally_votes(bool pre_vote) {
    size_t votes = pre_vote ? pre_votes_received_ : votes_received_;
    size_t qsize = quorum_size();

    SPDLOG_TRACE("raft: node {} tally_votes: {}/{} {}votes (pre_vote={})",
                 node_id_, votes, qsize,
                 votes >= qsize ? "MAJORITY " : "", pre_vote);

    if (pre_vote) {
        if (votes >= qsize && state_ == RaftNodeState::PreCandidate) {
            SPDLOG_INFO("raft: node {} pre-vote majority ({}/{}), "
                        "advancing to Candidate for term {}",
                        node_id_, votes, qsize, current_term_ + 1);
            become_candidate();
        }
        // If pre-vote fails, the election deadline will expire and
        // the node will retry or hear from a valid leader.
    } else {
        if (votes >= qsize && state_ == RaftNodeState::Candidate) {
            SPDLOG_INFO("raft: node {} won election for term {} ({}/{} votes)",
                        node_id_, current_term_, votes, qsize);
            become_leader();
        }
    }
}

// ============================================================================
// Leader Replication Subsystem
// ============================================================================

void RaftNode::send_heartbeats() {
    if (state_ != RaftNodeState::Leader) {
        SPDLOG_TRACE("raft: node {} send_heartbeats: not leader", node_id_);
        return;
    }
    if (membership_.members.empty()) return;

    AppendEntriesRequest hb;
    hb.term = current_term_;
    hb.leader_id = node_id_;
    hb.leader_commit = commit_index_;

    size_t sent = 0;
    size_t skipped = 0;

    for (auto peer : membership_.quorum_set()) {
        if (peer == node_id_) continue;

        auto it = follower_states_.find(peer);
        if (it == follower_states_.end()) {
            // New peer from a membership change — initialise tracking.
            FollowerState& fs = follower_states_[peer];
            fs.next_index = log_->last_index() + 1;
            fs.match_index = 0;
            fs.is_active = true;
            fs.in_flight = 0;
            fs.last_ack = steady_now();
            it = follower_states_.find(peer);
        }

        FollowerState& fs = it->second;

        // Skip followers with a full pipeline to avoid head-of-line blocking
        // on heartbeat delivery.
        if (fs.in_flight >= config_.rpc_queue_capacity) {
            skipped++;
            continue;
        }

        // Build per-follower heartbeat with correct prev_log_index/term
        // so the heartbeat doubles as an implicit log probe.
        AppendEntriesRequest req = hb;
        if (fs.next_index > 1) {
            req.prev_log_index = fs.next_index - 1;
            req.prev_log_term = log_->term_at(req.prev_log_index);
        } else {
            req.prev_log_index = 0;
            req.prev_log_term = kNoTerm;
        }

        try {
            ae_sender_(peer, req);
            sent++;
        } catch (const std::exception& e) {
            SPDLOG_WARN("raft: node {} heartbeat send to {} failed: {}",
                        node_id_, peer, e.what());
            // Track failures for health monitoring.
            heartbeat_failures_[peer]++;
            if (heartbeat_failures_[peer] >= kMaxHeartbeatFailures) {
                fs.is_active = false;
                SPDLOG_WARN("raft: node {} marking follower {} as inactive "
                            "after {} consecutive heartbeat failures",
                            node_id_, peer, heartbeat_failures_[peer]);
            }
        }
    }

    if (sent > 0 || skipped > 0) {
        SPDLOG_TRACE("raft: node {} heartbeat round: {} sent, {} skipped "
                     "(term={}, commit={})",
                     node_id_, sent, skipped, current_term_, commit_index_);
    }

    last_heartbeat_ = steady_now();
}

void RaftNode::replicate_to_followers() {
    if (state_ != RaftNodeState::Leader) return;
    if (membership_.members.empty()) return;

    LogIndex last_idx = log_->last_index();
    size_t replicated = 0;

    for (auto& [peer, fs] : follower_states_) {
        // Skip inactive peers.
        if (!fs.is_active) continue;

        // Respect pipeline capacity.
        if (fs.in_flight >= config_.rpc_queue_capacity) continue;

        // Check if the follower is already caught up.
        if (fs.next_index > last_idx) continue;

        // If the follower has fallen behind the log prefix (i.e., entries
        // we need to send have been snapshotted away), use InstallSnapshot.
        if (fs.next_index > 0 && fs.next_index <= log_->first_index()) {
            send_install_snapshot_to(peer);
            continue;
        }

        // Regular log replication.
        send_append_entries_to(peer);
        replicated++;
    }

    if (replicated > 0) {
        SPDLOG_TRACE("raft: node {} replication round: {} followers triggered",
                     node_id_, replicated);
    }
}

void RaftNode::send_append_entries_to(RaftNodeId target) {
    auto it = follower_states_.find(target);
    if (it == follower_states_.end()) {
        SPDLOG_WARN("raft: node {} send_append_entries_to: unknown follower {}",
                    node_id_, target);
        return;
    }

    FollowerState& fs = it->second;

    // Check pipeline depth.
    if (fs.in_flight >= config_.rpc_queue_capacity) {
        SPDLOG_TRACE("raft: node {} follower {} pipeline full (in_flight={})",
                     node_id_, target, fs.in_flight);
        return;
    }

    // Nothing to send.
    if (fs.next_index > log_->last_index()) return;

    // Build the AppendEntries batch.
    AppendEntriesRequest req;
    req.term = current_term_;
    req.leader_id = node_id_;
    req.leader_commit = commit_index_;

    if (fs.next_index > 1) {
        req.prev_log_index = fs.next_index - 1;
        req.prev_log_term = log_->term_at(req.prev_log_index);
    } else {
        req.prev_log_index = 0;
        req.prev_log_term = kNoTerm;
    }

    // Determine batch size: at most max_entries_per_append, but also
    // limited by byte count and the number of available entries.
    size_t max_entries = std::min(config_.max_entries_per_append,
                                  kHardMaxEntriesPerAppend);
    size_t available = static_cast<size_t>(log_->last_index() - fs.next_index + 1);
    size_t to_send = std::min(max_entries, available);

    // Read entries from log.
    LogIndex end = fs.next_index + static_cast<LogIndex>(to_send) - 1;
    req.entries.reserve(to_send);
    size_t read = log_->get_entries(fs.next_index, end, req.entries);
    if (read < to_send) {
        SPDLOG_WARN("raft: node {} log read short: requested {} got {} "
                    "for follower {}",
                    node_id_, to_send, read, target);
        if (read == 0) return;
    }

    // Enforce byte limit by trimming entries from the end.
    size_t total_bytes = 0;
    size_t keep = 0;
    for (size_t i = 0; i < req.entries.size(); ++i) {
        size_t entry_bytes = log_entry_byte_size(req.entries[i]);
        if (total_bytes + entry_bytes > config_.max_append_bytes && i > 0) {
            break;
        }
        total_bytes += entry_bytes;
        keep = i + 1;
    }
    req.entries.resize(keep);

    if (req.entries.empty()) {
        // No entries fit in the byte budget — send a heartbeat instead
        // and try again on the next round.
        return;
    }

    // Mark in-flight.
    fs.in_flight++;

    SPDLOG_TRACE("raft: node {} sending {} entries to {} "
                 "(next={}, prev={}:{}, commit={}, in_flight={}, "
                 "bytes={})",
                 node_id_, req.entries.size(), target,
                 fs.next_index, req.prev_log_index, req.prev_log_term,
                 req.leader_commit, fs.in_flight, total_bytes);

    try {
        ae_sender_(target, req);
        // Reset failure counter on successful send.
        heartbeat_failures_[target] = 0;
    } catch (const std::exception& e) {
        SPDLOG_WARN("raft: node {} AppendEntries send to {} failed: {}",
                    node_id_, target, e.what());
        if (fs.in_flight > 0) fs.in_flight--;
        heartbeat_failures_[target]++;
    }
}

void RaftNode::advance_commit_index() {
    if (state_ != RaftNodeState::Leader) return;

    LogIndex last_idx = log_->last_index();
    if (last_idx <= commit_index_) return;

    // Build a sorted view of match indices (including leader's own log).
    std::vector<LogIndex> matches;
    matches.reserve(membership_.quorum_set().size());
    matches.push_back(last_idx); // Leader has all entries.

    for (auto& [peer, fs] : follower_states_) {
        if (fs.is_active) {
            matches.push_back(fs.match_index);
        }
    }

    if (matches.size() < 2) {
        // No followers — the leader alone can commit if it's a
        // single-node cluster.
        if (membership_.members.size() == 1) {
            // Single-node cluster: commit all entries.
            commit_index_ = last_idx;
            apply_committed();
        }
        return;
    }

    std::sort(matches.begin(), matches.end(), std::greater<LogIndex>());
    size_t qsize = quorum_size();

    // Walk backward from last_idx to find the highest index that a
    // majority has replicated.  Only commit entries from the current
    // term per Raft §3.5.3 (the leader commitment rule).
    bool in_joint = membership_.in_joint_consensus();

    for (LogIndex n = last_idx; n > commit_index_; --n) {
        // Quick check: does the qsize-th highest match cover n?
        if (matches.size() < qsize) break;
        if (matches[qsize - 1] < n) continue;

        // A majority of the quorum set has replicated n.
        RaftTerm entry_term = log_->term_at(n);
        if (entry_term != current_term_) {
            // Cannot commit entries from prior terms directly.
            // (They will be committed indirectly when a later entry from
            // the current term is committed.)
            continue;
        }

        // During joint consensus, entries must also have a majority in
        // the new config.
        if (in_joint) {
            size_t new_qsize = (membership_.joint_members.size() / 2) + 1;
            size_t new_matches = 0;
            if (is_member(membership_.joint_members, node_id_)) {
                new_matches++;
            }
            for (auto peer : membership_.joint_members) {
                if (peer == node_id_) continue;
                auto it = follower_states_.find(peer);
                if (it != follower_states_.end()
                    && it->second.match_index >= n) {
                    new_matches++;
                }
            }
            if (new_matches < new_qsize) {
                continue; // New config doesn't have quorum for this index.
            }
        }

        // All conditions met — commit this index.
        commit_index_ = n;
        SPDLOG_DEBUG("raft: node {} advanced commit_index to {} "
                     "(term={}, joint={}, qsize={})",
                     node_id_, commit_index_, current_term_,
                     in_joint, qsize);
        apply_committed();
        return; // One index per invocation; apply_committed() will chain.
    }
}

void RaftNode::apply_committed() {
    while (commit_index_ > last_applied_) {
        size_t batch = static_cast<size_t>(commit_index_ - last_applied_);
        batch = std::min(batch, kMaxApplyBatch);
        LogIndex apply_end = last_applied_ + static_cast<LogIndex>(batch);

        SPDLOG_DEBUG("raft: node {} applying entries [{}, {}] ({} entries)",
                     node_id_, last_applied_ + 1, apply_end, batch);

        std::vector<LogEntry> entries;
        entries.reserve(batch);
        size_t read = log_->get_entries(last_applied_ + 1, apply_end, entries);
        if (read == 0) {
            SPDLOG_ERROR("raft: node {} log read returned 0 entries for "
                         "[{}, {}] — possible log corruption",
                         node_id_, last_applied_ + 1, apply_end);
            break;
        }

        for (auto& entry : entries) {
            // Process config change entries internally before applying
            // to the state machine.
            if (entry.is_config_change()) {
                auto& cc = std::get<ConfigChangeCommand>(entry.command);
                SPDLOG_INFO("raft: node {} applying ConfigChange at index {} "
                            "(joint={}, members={}->{})",
                            node_id_, entry.index,
                            cc.joint_consensus,
                            membership_.members.size(),
                            cc.new_members.size());
                process_config_change(cc);
            }

            // Call the user's state machine apply callback.
            // This MUST be idempotent — entries may be replayed.
            if (apply_fn_) {
                try {
                    apply_fn_(entry);
                } catch (const std::exception& e) {
                    SPDLOG_ERROR("raft: node {} state machine apply failed "
                                 "for index {} (term={}): {}",
                                 node_id_, entry.index, entry.term, e.what());
                    // Continue — committed entries cannot be rolled back.
                    // A production system should crash/restart here if
                    // idempotency cannot be guaranteed.  We continue to
                    // prevent a permanent stall, but correctness may be
                    // compromised.
                }
            }

            last_applied_ = entry.index;
        }
    }
}

void RaftNode::extend_lease() {
    if (!config_.enable_leader_lease) return;
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        config_.heartbeat_interval * kLeaseExtensionFactor
        + config_.max_clock_drift);
    lease_.extend(duration, current_term_);
    SPDLOG_TRACE("raft: node {} lease extended by {}ms (term={})",
                 node_id_, duration.count(), current_term_);
}

void RaftNode::try_reactivate_followers() {
    if (state_ != RaftNodeState::Leader) return;

    auto now = steady_now();
    for (auto& [peer, fs] : follower_states_) {
        if (!fs.is_active
            && (now - fs.last_ack) > kFollowerReactivationInterval) {
            SPDLOG_INFO("raft: node {} reactivating follower {} "
                        "after {}ms of inactivity",
                        node_id_, peer,
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            now - fs.last_ack).count());
            fs.is_active = true;
            heartbeat_failures_[peer] = 0;
        }
    }
}

// ============================================================================
// Snapshot / Log Compaction
// ============================================================================

RaftSnapshot RaftNode::create_snapshot() {
    std::unique_lock lock(mutex_);

    RaftSnapshot snap;
    if (commit_index_ <= log_->first_index()) {
        SPDLOG_DEBUG("raft: node {} create_snapshot skipped: "
                     "commit={} already <= first_log={}",
                     node_id_, commit_index_, log_->first_index());
        return snap;
    }

    LogIndex snap_index = commit_index_;
    RaftTerm snap_term = log_->term_at(snap_index);

    if (snap_term == kNoTerm && snap_index > 0) {
        SPDLOG_ERROR("raft: node {} create_snapshot: cannot read term at "
                     "index {} — log may be corrupted", node_id_, snap_index);
        return snap;
    }

    SPDLOG_INFO("raft: node {} creating snapshot at index {} (term={})",
                node_id_, snap_index, snap_term);

    // Build snapshot file path.
    auto now_ms = timestamp_now_ms();
    std::ostringstream fname;
    fname << config_.snapshot_directory << "/snapshot-"
          << node_id_ << "-" << snap_index << "-" << now_ms << ".snap";
    std::string path = fname.str();

    // --- Serialize snapshot data ---
    // In a production implementation, this serializes the full state machine
    // state.  Here we write a minimal binary metadata header, suitable for
    // the caller to extend.
    //
    // Binary format:
    //   [8 bytes] last_included_index  (little-endian int64)
    //   [8 bytes] last_included_term   (little-endian int64)
    //   [8 bytes] timestamp_ms         (little-endian int64)
    //   [4 bytes] member_count
    //   [4*N bytes] member IDs
    //   [4 bytes] joint_member_count
    //   [4*M bytes] joint member IDs
    //   [variable] state machine payload (implementation-defined)

    std::vector<uint8_t> data;

    auto push_int64 = [&](int64_t v) {
        for (int i = 0; i < 8; ++i) data.push_back(static_cast<uint8_t>(v >> (i * 8)));
    };
    auto push_int32 = [&](int32_t v) {
        for (int i = 0; i < 4; ++i) data.push_back(static_cast<uint8_t>(v >> (i * 8)));
    };

    push_int64(snap_index);
    push_int64(snap_term);
    push_int64(now_ms);
    push_int32(static_cast<int32_t>(membership_.members.size()));
    for (auto id : membership_.members) push_int32(id);
    push_int32(static_cast<int32_t>(membership_.joint_members.size()));
    for (auto id : membership_.joint_members) push_int32(id);

    // State machine payload: zero-length placeholder.
    // In production, call state_machine_.serialize(data).

    if (!write_file_atomic(path, data)) {
        SPDLOG_ERROR("raft: node {} failed to write snapshot to {}",
                     node_id_, path);
        return snap;
    }

    // Fill metadata.
    snap.file_path = path;
    snap.last_included_index = snap_index;
    snap.last_included_term = snap_term;
    snap.membership = membership_;
    snap.file_size_bytes = data.size();
    snap.created_at_ms = now_ms;

    // Truncate log prefix and apply snapshot.
    log_->truncate_prefix(snap_index);
    log_->apply_snapshot(snap);

    SPDLOG_INFO("raft: node {} snapshot created: {} ({} bytes, "
                "log now [{}, {}])",
                node_id_, path, snap.file_size_bytes,
                log_->first_index(), log_->last_index());

    return snap;
}

void RaftNode::send_install_snapshot_to(RaftNodeId target) {
    auto it = follower_states_.find(target);
    if (it == follower_states_.end()) return;

    FollowerState& fs = it->second;

    // Check if we have a snapshot to send.
    if (log_->first_index() <= 1) {
        return; // Nothing snapshotted yet.
    }

    // Count active snapshot transfers to limit concurrency.
    size_t active_snapshots = 0;
    for (auto& [p, state] : follower_states_) {
        if (state.pending_snapshot.has_value()) active_snapshots++;
    }
    if (active_snapshots >= kMaxConcurrentSnapshots
        && !fs.pending_snapshot.has_value()) {
        SPDLOG_TRACE("raft: node {} deferring InstallSnapshot to {} "
                     "(max concurrent reached)",
                     node_id_, target);
        return;
    }

    // Initiate or continue transfer.
    if (!fs.pending_snapshot.has_value()) {
        InstallSnapshotRequest req;
        req.term = current_term_;
        req.leader_id = node_id_;
        req.last_included_index = log_->first_index() - 1;
        req.last_included_term = log_->term_at(req.last_included_index);
        req.offset = 0;
        req.done = false;

        // Load snapshot data from disk.  We search for the most recent
        // snapshot file covering the required index.
        // For production: maintain a snapshot registry.
        std::string snap_path;
        bool found = false;
        std::error_code ec;
        for (auto& entry : fs::directory_iterator(config_.snapshot_directory, ec)) {
            if (!entry.is_regular_file()) continue;
            std::string fname = entry.path().filename().string();
            // Expected format: snapshot-<node>-<index>-<ts>.snap
            if (fname.find("snapshot-" + std::to_string(node_id_) + "-") == 0) {
                snap_path = entry.path().string();
                found = true;
            }
        }

        if (!found) {
            SPDLOG_WARN("raft: node {} no snapshot file found for InstallSnapshot "
                        "to {} (first_log_index={})",
                        node_id_, target, log_->first_index());
            return;
        }

        if (!read_file_binary(snap_path, req.data)) {
            SPDLOG_ERROR("raft: node {} failed to read snapshot {} for {}",
                         node_id_, snap_path, target);
            return;
        }

        req.done = true; // Single-chunk for small snapshots.
        if (req.data.size() > config_.snapshot_chunk_size) {
            // Need chunking: set done=false, send first chunk.
            req.done = false;
            fs.pending_snapshot = req;
            fs.pending_snapshot->data.clear(); // Stored externally.
            snapshot_chunk_data_[target] = std::move(req.data);
            // Build the first chunk.
            req.data.assign(
                snapshot_chunk_data_[target].begin(),
                snapshot_chunk_data_[target].begin()
                    + config_.snapshot_chunk_size);
            req.offset = 0;
        }

        SPDLOG_INFO("raft: node {} sending InstallSnapshot to {} "
                    "(last_included={}:{}, total_bytes={}, chunks={})",
                    node_id_, target,
                    req.last_included_index, req.last_included_term,
                    req.done ? req.data.size()
                             : snapshot_chunk_data_[target].size(),
                    req.done ? 1
                             : (snapshot_chunk_data_[target].size()
                                + config_.snapshot_chunk_size - 1)
                               / config_.snapshot_chunk_size);
    } else {
        // Continue sending next chunk.
        auto& stored = snapshot_chunk_data_[target];
        size_t offset = fs.pending_snapshot->offset;
        size_t chunk_size = config_.snapshot_chunk_size;
        bool is_last = (offset + chunk_size >= stored.size());

        InstallSnapshotRequest req = fs.pending_snapshot.value();
        req.data.assign(stored.begin() + offset,
                        stored.begin() + std::min(offset + chunk_size, stored.size()));
        req.offset = offset;
        req.done = is_last;

        fs.pending_snapshot->offset = offset + req.data.size();
        if (is_last) {
            fs.pending_snapshot.reset();
            snapshot_chunk_data_.erase(target);
        }
    }

    // Build the actual request to send.
    InstallSnapshotRequest send_req = fs.pending_snapshot.has_value()
        ? fs.pending_snapshot.value()
        : InstallSnapshotRequest{};

    if (!fs.pending_snapshot.has_value()) {
        // Single-chunk case: build from the chunk data.
        send_req.term = current_term_;
        send_req.leader_id = node_id_;
        send_req.last_included_index = log_->first_index() - 1;
        send_req.last_included_term = log_->term_at(send_req.last_included_index);
        send_req.data = std::move(snapshot_chunk_data_[target]);
        send_req.offset = 0;
        send_req.done = true;
    } else {
        // Multi-chunk: next chunk.
        size_t offset = fs.pending_snapshot->offset;
        send_req.data.assign(
            snapshot_chunk_data_[target].begin() + offset,
            snapshot_chunk_data_[target].begin()
                + std::min(offset + config_.snapshot_chunk_size,
                           snapshot_chunk_data_[target].size()));
        send_req.offset = offset;
        send_req.done = (offset + send_req.data.size()
                         >= snapshot_chunk_data_[target].size());
    }

    try {
        is_sender_(target, send_req);
        fs.in_flight++;
        SPDLOG_DEBUG("raft: node {} sent InstallSnapshot chunk to {} "
                     "(offset={}, size={}, done={})",
                     node_id_, target,
                     send_req.offset, send_req.data.size(), send_req.done);
    } catch (const std::exception& e) {
        SPDLOG_WARN("raft: node {} InstallSnapshot send to {} failed: {}",
                    node_id_, target, e.what());
        if (fs.in_flight > 0) fs.in_flight--;
        // Clean up on failure.
        fs.pending_snapshot.reset();
        snapshot_chunk_data_.erase(target);
    }
}

void RaftNode::maybe_create_snapshot() {
    if (state_ != RaftNodeState::Leader) return;

    size_t entry_count = log_->entry_count();
    if (entry_count >= config_.snapshot_threshold_entries) {
        SPDLOG_INFO("raft: node {} snapshot triggered by entry count "
                    "({} >= {})",
                    node_id_, entry_count,
                    config_.snapshot_threshold_entries);
        create_snapshot();
    }

    // Byte-based threshold would require a total log size metric.
    // In production, add RaftLog::total_bytes() and check against
    // config_.snapshot_threshold_bytes.
}

// ============================================================================
// Membership / Joint Consensus
// ============================================================================

void RaftNode::process_config_change(const ConfigChangeCommand& cmd) {
    SPDLOG_INFO("raft: node {} processing ConfigChangeCommand "
                "(joint={}, new_members_count={}, in_joint_now={})",
                node_id_,
                cmd.joint_consensus,
                cmd.new_members.size(),
                membership_.in_joint_consensus());

    if (!membership_.in_joint_consensus()) {
        // --- Normal membership change ---
        if (!cmd.joint_consensus) {
            apply_direct_membership_change(cmd.new_members);
        } else {
            apply_joint_consensus_entry(cmd);
        }
    } else {
        // --- Already in joint consensus ---
        apply_joint_consensus_finalization(cmd);
    }

    // Clear the pending config index fence.
    pending_config_index_ = 0;

    SPDLOG_INFO("raft: node {} membership now: members={}, joint={}, "
                "in_joint={}",
                node_id_,
                membership_.members.size(),
                membership_.joint_members.size(),
                membership_.in_joint_consensus());
}

void RaftNode::apply_direct_membership_change(
    const std::vector<RaftNodeId>& new_members) {
    if (new_members.empty()) {
        SPDLOG_ERROR("raft: node {} cannot apply empty membership — ignoring",
                     node_id_);
        return;
    }

    SPDLOG_INFO("raft: node {} applying direct membership change: "
                "{} -> {} members",
                node_id_, membership_.members.size(), new_members.size());

    // Validate: ensure this node is still a member.
    if (!is_member(new_members, node_id_)) {
        SPDLOG_WARN("raft: node {} being removed from membership — "
                    "will step down if leader", node_id_);
    }

    auto old_members = membership_.members;
    membership_.members = new_members;
    membership_.joint_members.clear();

    // Update follower tracking if leader.
    if (state_ == RaftNodeState::Leader) {
        reconcile_follower_states(old_members, new_members);
    }
}

void RaftNode::apply_joint_consensus_entry(const ConfigChangeCommand& cmd) {
    if (cmd.old_members.empty()) {
        SPDLOG_ERROR("raft: node {} joint consensus requested but old_members "
                     "not provided — falling back to direct change", node_id_);
        apply_direct_membership_change(cmd.new_members);
        return;
    }

    SPDLOG_INFO("raft: node {} entering joint consensus: "
                "old={} members, new={} members",
                node_id_, cmd.old_members.size(), cmd.new_members.size());

    membership_.members = cmd.old_members;
    membership_.joint_members = cmd.new_members;

    // Add joint members to tracking.
    if (state_ == RaftNodeState::Leader) {
        for (auto peer : cmd.new_members) {
            if (peer == node_id_) continue;
            auto it = follower_states_.find(peer);
            if (it == follower_states_.end()) {
                FollowerState& fs = follower_states_[peer];
                fs.next_index = log_->last_index() + 1;
                fs.match_index = 0;
                fs.is_active = true;
                fs.in_flight = 0;
                fs.last_ack = steady_now();
                SPDLOG_INFO("raft: node {} added joint member {} to tracking",
                            node_id_, peer);
            }
        }
    }
}

void RaftNode::apply_joint_consensus_finalization(
    const ConfigChangeCommand& cmd) {
    if (cmd.joint_consensus) {
        SPDLOG_ERROR("raft: node {} nested joint consensus while already "
                     "in joint consensus — ignoring",
                     node_id_);
        return;
    }

    // Verify new_members match the joint_members we are transitioning to.
    if (cmd.new_members != membership_.joint_members) {
        SPDLOG_ERROR("raft: node {} joint consensus finalization mismatch: "
                     "got {} members, expected {} — ignoring",
                     node_id_,
                     cmd.new_members.size(),
                     membership_.joint_members.size());
        return;
    }

    SPDLOG_INFO("raft: node {} finalizing joint consensus: "
                "committing new membership of {} members",
                node_id_, membership_.joint_members.size());

    // Build the set of members being removed.
    std::vector<RaftNodeId> removed;
    for (auto id : membership_.members) {
        if (!is_member(membership_.joint_members, id)) {
            removed.push_back(id);
        }
    }

    membership_.members = membership_.joint_members;
    membership_.joint_members.clear();

    if (state_ == RaftNodeState::Leader) {
        // Remove followers that are no longer in the membership.
        for (auto peer : removed) {
            auto it = follower_states_.find(peer);
            if (it != follower_states_.end()) {
                SPDLOG_INFO("raft: node {} removing follower {} after joint "
                            "consensus finalization",
                            node_id_, peer);
                follower_states_.erase(it);
            }
            heartbeat_failures_.erase(peer);
            backtrack_rounds_.erase(peer);
            snapshot_chunk_data_.erase(peer);
        }
    }

    // If this node itself was removed, step down.
    if (!is_member(membership_.members, node_id_)) {
        SPDLOG_INFO("raft: node {} removed from membership during joint "
                    "consensus — stepping down", node_id_);
        become_follower(current_term_);
    }
}

void RaftNode::reconcile_follower_states(
    const std::vector<RaftNodeId>& old_members,
    const std::vector<RaftNodeId>& new_members) {
    // Add new followers.
    for (auto peer : new_members) {
        if (peer == node_id_) continue;
        if (follower_states_.find(peer) == follower_states_.end()) {
            FollowerState& fs = follower_states_[peer];
            fs.next_index = log_->last_index() + 1;
            fs.match_index = 0;
            fs.is_active = true;
            fs.in_flight = 0;
            fs.last_ack = steady_now();
            SPDLOG_INFO("raft: node {} added new follower {}",
                        node_id_, peer);
        }
    }

    // Remove stale followers.
    for (auto it = follower_states_.begin();
         it != follower_states_.end(); ) {
        if (!is_member(new_members, it->first)) {
            SPDLOG_INFO("raft: node {} removing stale follower {}",
                        node_id_, it->first);
            heartbeat_failures_.erase(it->first);
            backtrack_rounds_.erase(it->first);
            snapshot_chunk_data_.erase(it->first);
            it = follower_states_.erase(it);
        } else {
            ++it;
        }
    }
}

void RaftNode::finalize_joint_consensus() {
    // Legacy path — handled by apply_joint_consensus_finalization now.
    SPDLOG_INFO("raft: node {} finalizing joint consensus to {} members",
                node_id_, membership_.joint_members.size());

    membership_.members = membership_.joint_members;
    membership_.joint_members.clear();

    if (state_ == RaftNodeState::Leader) {
        for (auto it = follower_states_.begin();
             it != follower_states_.end(); ) {
            if (!is_member(membership_.members, it->first)) {
                SPDLOG_INFO("raft: node {} removing follower {} after "
                            "joint consensus",
                            node_id_, it->first);
                it = follower_states_.erase(it);
            } else {
                ++it;
            }
        }
    }
}

// ============================================================================
// Client API: propose()
// ============================================================================

result<LogIndex> RaftNode::propose(RaftCommand command) {
    std::unique_lock lock(mutex_);

    // --- Pre-condition checks ---

    if (!running_.load(std::memory_order_acquire)) {
        return result<LogIndex>::failure(
            error_code::shutdown_in_progress,
            "Raft node " + std::to_string(node_id_) + " is shutting down");
    }

    if (state_ != RaftNodeState::Leader) {
        std::string leader_hint = (leader_id_ != kNoLeader)
            ? std::to_string(leader_id_)
            : "unknown";
        return result<LogIndex>::failure(
            error_code::not_leader_for_partition,
            "Not the leader (node " + std::to_string(node_id_)
            + " is " + state_name(state_)
            + ", leader is " + leader_hint + ")");
    }

    if (!membership_.members.empty()
        && !is_member(membership_.members, node_id_)) {
        return result<LogIndex>::failure(
            error_code::not_leader_for_partition,
            "Node " + std::to_string(node_id_)
            + " is not in the current membership");
    }

    if (transferring_.load(std::memory_order_acquire)) {
        return result<LogIndex>::failure(
            error_code::not_leader_for_partition,
            "Leadership transfer in progress — proposals temporarily rejected");
    }

    if (pending_config_index_ != 0
        && std::holds_alternative<ConfigChangeCommand>(command)) {
        return result<LogIndex>::failure(
            error_code::reassignment_in_progress,
            "A configuration change is already in progress at index "
            + std::to_string(pending_config_index_));
    }

    if (membership_.members.empty()) {
        return result<LogIndex>::failure(
            error_code::broker_not_available,
            "No cluster membership configured — cannot accept proposals");
    }

    // --- Append to log ---

    LogEntry entry;
    entry.term = current_term_;
    entry.index = log_->last_index() + 1;
    entry.command = std::move(command);

    std::vector<LogEntry> entries;
    entries.push_back(std::move(entry));

    LogIndex prev_idx = log_->last_index();
    RaftTerm prev_term = (prev_idx > 0) ? log_->term_at(prev_idx) : kNoTerm;

    LogIndex appended = log_->append(prev_idx, prev_term, entries);

    if (appended == kNoLogIndex) {
        SPDLOG_ERROR("raft: node {} failed to append proposed entry "
                     "(prev={}:{}, log=[{},{}])",
                     node_id_, prev_idx, prev_term,
                     log_->first_index(), log_->last_index());
        return result<LogIndex>::failure(
            error_code::corrupt_message,
            "Failed to append entry to Raft log at index "
            + std::to_string(prev_idx + 1));
    }

    // Track config change index to fence concurrent config proposals.
    if (entries[0].is_config_change()) {
        if (pending_config_index_ != 0) {
            SPDLOG_WARN("raft: node {} config change race: pending_index={}, "
                        "new_index={}",
                        node_id_, pending_config_index_, entries[0].index);
        }
        pending_config_index_ = entries[0].index;
        SPDLOG_INFO("raft: node {} proposed ConfigChange at index {}",
                    node_id_, pending_config_index_);
    }

    LogIndex assigned_index = entries[0].index;

    // Wake the heartbeat/replication thread to push entries urgently.
    cv_.notify_all();

    SPDLOG_DEBUG("raft: node {} proposed entry at index {} (term={}, type={})",
                 node_id_, assigned_index, current_term_,
                 entries[0].is_config_change() ? "ConfigChange"
                 : entries[0].is_noop() ? "NoOp" : "UserCommand");

    return result<LogIndex>::success(assigned_index);
}

// ============================================================================
// Manual Transitions
// ============================================================================

void RaftNode::step_down() {
    std::unique_lock lock(mutex_);
    if (state_ != RaftNodeState::Leader) {
        SPDLOG_DEBUG("raft: node {} step_down called but not leader (state={})",
                     node_id_, state_name(state_));
        return;
    }
    SPDLOG_INFO("raft: node {} manual step-down from Leader (term={})",
                node_id_, current_term_);
    become_follower(current_term_);
}

void RaftNode::transfer_leadership(RaftNodeId target) {
    std::unique_lock lock(mutex_);

    if (state_ != RaftNodeState::Leader) {
        SPDLOG_WARN("raft: node {} transfer_leadership called but not leader "
                    "(state={})",
                    node_id_, state_name(state_));
        return;
    }

    if (target == node_id_) {
        SPDLOG_WARN("raft: node {} cannot transfer leadership to self",
                    node_id_);
        return;
    }

    // Validate target is a known cluster member.
    if (!is_member(membership_.quorum_set(), target)) {
        SPDLOG_WARN("raft: node {} transfer target {} not in quorum set",
                    node_id_, target);
        return;
    }

    // Mark transferring to suppress new proposals.
    transferring_.store(true, std::memory_order_release);

    SPDLOG_INFO("raft: node {} initiating leadership transfer to {}",
                node_id_, target);

    auto it = follower_states_.find(target);
    if (it == follower_states_.end()) {
        SPDLOG_WARN("raft: node {} transfer target {} not in follower_states_",
                    node_id_, target);
        transferring_.store(false, std::memory_order_release);
        return;
    }

    // Replicate all outstanding entries to the target.
    // Retry for a limited number of rounds.
    static constexpr int kTransferMaxRounds = 20;
    LogIndex last_idx = log_->last_index();

    for (int round = 0; round < kTransferMaxRounds; ++round) {
        if (!running_.load(std::memory_order_acquire)) {
            transferring_.store(false, std::memory_order_release);
            return;
        }
        if (it->second.match_index >= last_idx) {
            SPDLOG_INFO("raft: node {} transfer target {} caught up "
                        "(match={}, last={})",
                        node_id_, target, it->second.match_index, last_idx);
            break; // Target is caught up.
        }
        send_append_entries_to(target);

        // Release lock briefly so the response can be processed.
        lock.unlock();
        std::this_thread::sleep_for(config_.heartbeat_interval);
        lock.lock();

        if (state_ != RaftNodeState::Leader) {
            // We lost leadership — transfer is moot.
            SPDLOG_INFO("raft: node {} lost leadership during transfer",
                        node_id_);
            transferring_.store(false, std::memory_order_release);
            return;
        }
    }

    // Send a TimeoutNow-like signal: a RequestVote to the target so it
    // can jump-start an election immediately.
    RequestVoteRequest req;
    req.term = current_term_ + 1; // Target should use this as its term.
    req.candidate_id = target;
    req.last_log_index = log_->last_index();
    req.last_log_term = log_->term_at(log_->last_index());
    req.pre_vote = false;

    try {
        rv_sender_(target, req);
        SPDLOG_INFO("raft: node {} sent leadership transfer signal to {} "
                    "(term={})",
                    node_id_, target, req.term);
    } catch (const std::exception& e) {
        SPDLOG_WARN("raft: node {} transfer signal to {} failed: {}",
                    node_id_, target, e.what());
    }

    // Step down after a brief delay to let the target start its election.
    auto deadline = steady_now() + config_.min_election_timeout;
    cv_.wait_until(lock, deadline, [this] {
        return !running_.load(std::memory_order_acquire);
    });

    if (state_ == RaftNodeState::Leader) {
        SPDLOG_INFO("raft: node {} stepping down to complete leadership transfer",
                    node_id_);
        become_follower(current_term_);
    }

    transferring_.store(false, std::memory_order_release);
}

// ============================================================================
// Inbound RPC Handlers
// ============================================================================

AppendEntriesResponse RaftNode::handle_append_entries(
    const AppendEntriesRequest& req) {
    std::unique_lock lock(mutex_);

    AppendEntriesResponse resp;
    resp.term = current_term_;
    resp.last_log_index = log_->last_index();

    // --- 1. Reject stale requests ---
    if (req.term < current_term_) {
        SPDLOG_TRACE("raft: node {} rejecting AppendEntries from {}: "
                     "req_term={} < current_term={}",
                     node_id_, req.leader_id, req.term, current_term_);
        resp.success = false;
        resp.conflict_index = log_->last_index() + 1;
        return resp;
    }

    // --- 2. Handle higher term ---
    if (req.term > current_term_) {
        SPDLOG_INFO("raft: node {} received AppendEntries with higher term "
                    "({} > {}), stepping down to Follower",
                    node_id_, req.term, current_term_);
        become_follower(req.term);
        resp.term = current_term_;
    }

    // --- 3. Update leader tracking ---
    leader_id_ = req.leader_id;
    last_heartbeat_ = steady_now();
    election_deadline_ = steady_now() + random_election_timeout();

    // A leader should never receive AppendEntries from another node
    // with the same term.  This indicates either a stale/delayed message
    // or a split-brain scenario (shouldn't happen with proper fencing).
    if (state_ == RaftNodeState::Leader && req.term == current_term_) {
        SPDLOG_WARN("raft: node {} (Leader) received AppendEntries from {} "
                    "with same term {} — possible delayed/stale message",
                    node_id_, req.leader_id, req.term);
        resp.success = false;
        return resp;
    }

    // --- 4. Log consistency check ---
    LogIndex conflict_index = 0;
    bool log_ok = append_entries_to_log(req, conflict_index);

    if (!log_ok) {
        SPDLOG_TRACE("raft: node {} AppendEntries log mismatch "
                     "(prev={}:{}, local_last={}, conflict={})",
                     node_id_,
                     req.prev_log_index, req.prev_log_term,
                     log_->last_index(), conflict_index);
        resp.success = false;
        resp.conflict_index = conflict_index;
        return resp;
    }

    // --- 5. Advance commit index ---
    if (req.leader_commit > commit_index_) {
        LogIndex last_new = req.entries.empty()
            ? req.prev_log_index
            : req.entries.back().index;
        LogIndex old_commit = commit_index_;
        commit_index_ = std::min(req.leader_commit, last_new);

        if (commit_index_ > old_commit) {
            SPDLOG_TRACE("raft: node {} follower commit_index {} -> {} "
                         "(leader_commit={}, last_new={})",
                         node_id_, old_commit, commit_index_,
                         req.leader_commit, last_new);
        }

        // Apply committed entries.
        apply_committed();
    }

    resp.success = true;
    cv_.notify_all();
    return resp;
}

RequestVoteResponse RaftNode::handle_request_vote(
    const RequestVoteRequest& req) {
    std::unique_lock lock(mutex_);

    RequestVoteResponse resp;
    resp.term = current_term_;
    resp.vote_granted = false;

    // --- Pre-Vote handling ---
    if (req.pre_vote) {
        // Pre-votes are non-binding probes.  We grant if:
        //   1. req.term >= current_term_ (candidate is not stale)
        //   2. Candidate's log is at least as up-to-date.
        if (req.term < current_term_) {
            SPDLOG_TRACE("raft: node {} rejecting PreVote from {}: "
                         "req_term={} < current_term={}",
                         node_id_, req.candidate_id, req.term, current_term_);
            return resp;
        }
        if (!log_is_up_to_date(req.last_log_index, req.last_log_term)) {
            SPDLOG_TRACE("raft: node {} rejecting PreVote from {}: "
                         "log not up-to-date (local={}:{}, remote={}:{})",
                         node_id_, req.candidate_id,
                         log_->last_index(),
                         log_->term_at(log_->last_index()),
                         req.last_log_index, req.last_log_term);
            return resp;
        }
        resp.vote_granted = true;
        SPDLOG_DEBUG("raft: node {} granted PreVote to {} (term={})",
                     node_id_, req.candidate_id, req.term);
        return resp;
    }

    // --- Standard vote handling ---

    if (req.term < current_term_) {
        SPDLOG_TRACE("raft: node {} rejecting RequestVote from {}: "
                     "req_term={} < current_term={}",
                     node_id_, req.candidate_id, req.term, current_term_);
        return resp;
    }

    if (req.term > current_term_) {
        SPDLOG_INFO("raft: node {} received RequestVote with higher term "
                    "({} > {}), becoming follower",
                    node_id_, req.term, current_term_);
        become_follower(req.term);
        resp.term = current_term_;
    }

    // Check if already voted for someone else in this term.
    if (voted_for_ != kNoLeader && voted_for_ != req.candidate_id) {
        SPDLOG_TRACE("raft: node {} already voted for {} in term {} "
                     "(rejecting {})",
                     node_id_, voted_for_, current_term_, req.candidate_id);
        return resp;
    }

    // Log up-to-date check.
    if (!log_is_up_to_date(req.last_log_index, req.last_log_term)) {
        SPDLOG_TRACE("raft: node {} rejecting RequestVote from {}: "
                     "log not up-to-date (local={}:{}, remote={}:{})",
                     node_id_, req.candidate_id,
                     log_->last_index(),
                     log_->term_at(log_->last_index()),
                     req.last_log_index, req.last_log_term);
        return resp;
    }

    // Grant vote.
    voted_for_ = req.candidate_id;
    resp.vote_granted = true;
    election_deadline_ = steady_now() + random_election_timeout();

    SPDLOG_INFO("raft: node {} voted for {} in term {} "
                "(log=[{},{}])",
                node_id_, req.candidate_id, current_term_,
                log_->first_index(), log_->last_index());

    cv_.notify_all();
    return resp;
}

InstallSnapshotResponse RaftNode::handle_install_snapshot(
    const InstallSnapshotRequest& req) {
    std::unique_lock lock(mutex_);

    InstallSnapshotResponse resp;
    resp.term = current_term_;

    // --- 1. Reject stale requests ---
    if (req.term < current_term_) {
        SPDLOG_TRACE("raft: node {} rejecting InstallSnapshot from {}: "
                     "req_term={} < current_term={}",
                     node_id_, req.leader_id, req.term, current_term_);
        return resp;
    }

    // --- 2. Handle higher term ---
    if (req.term > current_term_) {
        SPDLOG_INFO("raft: node {} received InstallSnapshot with higher term "
                    "({} > {}), becoming follower",
                    node_id_, req.term, current_term_);
        become_follower(req.term);
        resp.term = current_term_;
    }

    // --- 3. Update leader and election timer ---
    leader_id_ = req.leader_id;
    last_heartbeat_ = steady_now();
    election_deadline_ = steady_now() + random_election_timeout();

    // --- 4. First chunk: initialize buffer ---
    if (req.offset == 0) {
        SPDLOG_INFO("raft: node {} receiving InstallSnapshot from {} "
                    "(first chunk, last_included={}:{})",
                    node_id_, req.leader_id,
                    req.last_included_index, req.last_included_term);
        pending_snapshot_meta_ = req;
        pending_snapshot_meta_.data.clear();
        snapshot_buffer_.clear();
        snapshot_buffer_.reserve(config_.snapshot_chunk_size * 4); // Pre-allocate.
    }

    // --- 5. Validate offset ---
    if (req.offset != snapshot_buffer_.size()) {
        SPDLOG_WARN("raft: node {} InstallSnapshot offset mismatch: "
                    "expected {} got {} — resetting buffer",
                    node_id_, snapshot_buffer_.size(), req.offset);
        snapshot_buffer_.clear();
        if (req.offset == 0) {
            pending_snapshot_meta_ = req;
            pending_snapshot_meta_.data.clear();
        } else {
            // Out of order; reject.
            return resp;
        }
    }

    // --- 6. Append chunk data ---
    snapshot_buffer_.insert(snapshot_buffer_.end(),
                            req.data.begin(), req.data.end());

    SPDLOG_TRACE("raft: node {} InstallSnapshot chunk at offset {}: "
                 "{} bytes (total {} so far)",
                 node_id_, req.offset, req.data.size(),
                 snapshot_buffer_.size());

    // --- 7. Final chunk: persist and apply ---
    if (req.done) {
        SPDLOG_INFO("raft: node {} received final snapshot chunk from {} "
                    "({} total bytes, last_included={})",
                    node_id_, req.leader_id,
                    snapshot_buffer_.size(),
                    pending_snapshot_meta_.last_included_index);

        // Build snapshot metadata.
        RaftSnapshot snap;
        snap.last_included_index = pending_snapshot_meta_.last_included_index;
        snap.last_included_term = pending_snapshot_meta_.last_included_term;
        snap.file_size_bytes = snapshot_buffer_.size();
        snap.created_at_ms = timestamp_now_ms();

        // Write snapshot to disk.
        std::ostringstream fname;
        fname << config_.snapshot_directory << "/snapshot-"
              << node_id_ << "-"
              << snap.last_included_index
              << "-" << snap.created_at_ms << ".snap";
        snap.file_path = fname.str();

        if (write_file_atomic(snap.file_path, snapshot_buffer_)) {
            log_->apply_snapshot(snap);

            // Advance commit/applied indices to match snapshot.
            if (commit_index_ < snap.last_included_index) {
                commit_index_ = snap.last_included_index;
            }
            if (last_applied_ < snap.last_included_index) {
                last_applied_ = snap.last_included_index;
            }

            SPDLOG_INFO("raft: node {} snapshot installed: {} "
                        "(commit={}, applied={}, first_log={})",
                        node_id_, snap.file_path,
                        commit_index_, last_applied_,
                        log_->first_index());
        } else {
            SPDLOG_ERROR("raft: node {} failed to persist received snapshot "
                         "to {} — data may be lost",
                         node_id_, snap.file_path);
        }

        // Clear reception buffers.
        snapshot_buffer_.clear();
        pending_snapshot_meta_ = InstallSnapshotRequest{};
    }

    cv_.notify_all();
    return resp;
}

// ============================================================================
// Outbound RPC Response Handlers
// ============================================================================

void RaftNode::handle_append_entries_response(
    RaftNodeId from, const AppendEntriesResponse& resp) {
    std::unique_lock lock(mutex_);

    // Guard: ignore if no longer leader.
    if (state_ != RaftNodeState::Leader) {
        SPDLOG_TRACE("raft: node {} ignoring AE response from {} — not leader "
                     "(state={})",
                     node_id_, from, state_name(state_));
        return;
    }

    // Higher term from follower: step down.
    if (resp.term > current_term_) {
        SPDLOG_INFO("raft: node {} stepping down: follower {} has higher term "
                    "({} > {})",
                    node_id_, from, resp.term, current_term_);
        become_follower(resp.term);
        return;
    }

    auto it = follower_states_.find(from);
    if (it == follower_states_.end()) {
        SPDLOG_WARN("raft: node {} AE response from unknown follower {}",
                    node_id_, from);
        return;
    }

    FollowerState& fs = it->second;

    // Decrement in-flight (clamped).
    if (fs.in_flight > 0) fs.in_flight--;

    fs.last_ack = steady_now();
    fs.is_active = true;
    heartbeat_failures_[from] = 0;

    if (resp.success) {
        // --- Successful replication ---

        // Determine the highest index the follower has now.
        // We sent entries starting at prev_log_index+1; the follower
        // confirmed they all matched.
        LogIndex last_sent = resp.last_log_index;
        if (last_sent > fs.match_index) {
            fs.match_index = last_sent;
            fs.next_index = fs.match_index + 1;

            SPDLOG_TRACE("raft: node {} follower {} match={} next={} "
                         "(in_flight={})",
                         node_id_, from, fs.match_index, fs.next_index,
                         fs.in_flight);
        }

        // Try to advance commit index with the updated match.
        advance_commit_index();

        // --- Leader lease extension ---
        // Check if a majority of followers have acked recently.
        if (config_.enable_leader_lease) {
            auto now = steady_now();
            auto lease_window = config_.heartbeat_interval * 2;
            size_t acked = 1; // Leader counts itself.
            for (auto& [peer, state] : follower_states_) {
                if (peer == node_id_) continue;
                if (state.is_active
                    && (now - state.last_ack) < lease_window) {
                    acked++;
                }
            }
            if (acked >= quorum_size()) {
                extend_lease();
            }
        }
    } else {
        // --- Log inconsistency: backtrack ---

        if (resp.conflict_index > 0 && resp.conflict_index < fs.next_index) {
            // Fast backtrack using the conflict_index hint from the
            // follower (first index of conflicting term).
            fs.next_index = resp.conflict_index;
            backtrack_rounds_[from]++;
            SPDLOG_DEBUG("raft: node {} fast-backtracked {} to next={} "
                         "(conflict_index hint, round {})",
                         node_id_, from, fs.next_index,
                         backtrack_rounds_[from]);
        } else {
            // Conservative backtrack: step back one entry.
            if (fs.next_index > 1) fs.next_index--;
            backtrack_rounds_[from]++;
            SPDLOG_DEBUG("raft: node {} backtracked {} to next={} "
                         "(round {})",
                         node_id_, from, fs.next_index,
                         backtrack_rounds_[from]);
        }

        // Clamp to log's first index.
        if (fs.next_index < log_->first_index()) {
            fs.next_index = log_->first_index();
        }

        // If we've backtracked below the log prefix, fall back to
        // InstallSnapshot.
        if (fs.next_index <= log_->first_index() && log_->first_index() > 1) {
            SPDLOG_INFO("raft: node {} follower {} next_index {} <= "
                        "first_index {} — falling back to InstallSnapshot",
                        node_id_, from, fs.next_index, log_->first_index());
            backtrack_rounds_[from] = 0;
            send_install_snapshot_to(from);
            return;
        }

        // If we've backtracked too many times, try InstallSnapshot anyway.
        if (backtrack_rounds_[from] >= kMaxBacktrackRounds) {
            SPDLOG_WARN("raft: node {} follower {} exceeded max backtrack "
                        "rounds ({}), falling back to InstallSnapshot",
                        node_id_, from, kMaxBacktrackRounds);
            backtrack_rounds_[from] = 0;
            send_install_snapshot_to(from);
            return;
        }

        // Retry with updated next_index.
        send_append_entries_to(from);
    }
}

void RaftNode::handle_request_vote_response(
    RaftNodeId from, const RequestVoteResponse& resp) {
    std::unique_lock lock(mutex_);

    // Guard: ignore if not in an election state.
    if (state_ != RaftNodeState::Candidate
        && state_ != RaftNodeState::PreCandidate) {
        SPDLOG_TRACE("raft: node {} ignoring {}Response from {} — "
                     "not in election (state={})",
                     node_id_,
                     state_ == RaftNodeState::Leader ? "Vote" : "",
                     from, state_name(state_));
        return;
    }

    bool is_pre_vote = (state_ == RaftNodeState::PreCandidate);

    // Higher term means we are stale.
    RaftTerm our_request_term = is_pre_vote
        ? current_term_ + 1
        : current_term_;
    if (resp.term > our_request_term) {
        SPDLOG_INFO("raft: node {} {} from {} with higher term {} (>{}), "
                    "stepping down",
                    node_id_,
                    is_pre_vote ? "PreVote response" : "Vote response",
                    from, resp.term, our_request_term);
        become_follower(resp.term);
        return;
    }

    // Re-check state (could have changed during lock release).
    if (is_pre_vote && state_ != RaftNodeState::PreCandidate) return;
    if (!is_pre_vote && state_ != RaftNodeState::Candidate) return;

    if (resp.vote_granted) {
        if (is_pre_vote) {
            pre_votes_received_++;
            SPDLOG_DEBUG("raft: node {} received PreVote from {} "
                         "({}/{} pre-votes for term {})",
                         node_id_, from,
                         pre_votes_received_, quorum_size(),
                         current_term_ + 1);
        } else {
            votes_received_++;
            SPDLOG_DEBUG("raft: node {} received vote from {} "
                         "({}/{} votes for term {})",
                         node_id_, from,
                         votes_received_, quorum_size(),
                         current_term_);
        }
    } else {
        SPDLOG_TRACE("raft: node {} {} from {} was NOT granted",
                     node_id_,
                     is_pre_vote ? "PreVote" : "Vote",
                     from);
    }

    tally_votes(is_pre_vote);
}

void RaftNode::handle_install_snapshot_response(
    RaftNodeId from, const InstallSnapshotResponse& resp) {
    std::unique_lock lock(mutex_);

    if (state_ != RaftNodeState::Leader) {
        SPDLOG_TRACE("raft: node {} ignoring InstallSnapshot response from {} "
                     "— not leader",
                     node_id_, from);
        return;
    }

    // Higher term: step down.
    if (resp.term > current_term_) {
        SPDLOG_INFO("raft: node {} stepping down: follower {} has higher term "
                    "({} > {}) after InstallSnapshot",
                    node_id_, from, resp.term, current_term_);
        become_follower(resp.term);
        return;
    }

    auto it = follower_states_.find(from);
    if (it == follower_states_.end()) return;

    FollowerState& fs = it->second;
    if (fs.in_flight > 0) fs.in_flight--;

    // On successful InstallSnapshot, advance the follower's tracking.
    // The snapshot's last_included_index replaces the log prefix.
    if (fs.pending_snapshot.has_value()) {
        if (fs.pending_snapshot->done) {
            LogIndex snap_idx = fs.pending_snapshot->last_included_index;
            if (snap_idx > fs.match_index) {
                fs.match_index = snap_idx;
                fs.next_index = snap_idx + 1;
            }
            fs.pending_snapshot.reset();
            snapshot_chunk_data_.erase(from);

            SPDLOG_INFO("raft: node {} follower {} snapshot installed: "
                        "match={}, next={}",
                        node_id_, from, fs.match_index, fs.next_index);

            advance_commit_index();
        } else {
            // Intermediate chunk acknowledged — send the next chunk.
            send_install_snapshot_to(from);
        }
    }
}

// ============================================================================
// Log Helpers
// ============================================================================

bool RaftNode::append_entries_to_log(const AppendEntriesRequest& req,
                                      LogIndex& conflict_index) {
    // Pure heartbeat (no entries): just verify prev_log_index exists.
    if (req.entries.empty()) {
        if (req.prev_log_index > 0) {
            if (req.prev_log_index > log_->last_index()) {
                conflict_index = log_->last_index() + 1;
                return false;
            }
            RaftTerm local_term = log_->term_at(req.prev_log_index);
            if (local_term != req.prev_log_term) {
                // Find first index of the conflicting term for fast
                // leader backtracking.
                conflict_index = req.prev_log_index;
                if (local_term != kNoTerm) {
                    while (conflict_index > log_->first_index()
                           && log_->term_at(conflict_index - 1) == local_term) {
                        conflict_index--;
                    }
                }
                return false;
            }
        }
        return true;
    }

    // --- Entries present: check prev_log consistency ---
    if (req.prev_log_index > 0) {
        if (req.prev_log_index > log_->last_index()) {
            SPDLOG_TRACE("raft: node {} prev_log_index {} > last_index {}",
                         node_id_, req.prev_log_index, log_->last_index());
            conflict_index = log_->last_index() + 1;
            return false;
        }
        RaftTerm local_term = log_->term_at(req.prev_log_index);
        if (local_term != req.prev_log_term) {
            SPDLOG_TRACE("raft: node {} prev_log_term mismatch: "
                         "local({}:{}) != req({}:{})",
                         node_id_,
                         req.prev_log_index, local_term,
                         req.prev_log_index, req.prev_log_term);
            conflict_index = req.prev_log_index;
            if (local_term != kNoTerm) {
                while (conflict_index > log_->first_index()
                       && log_->term_at(conflict_index - 1) == local_term) {
                    conflict_index--;
                }
            }
            return false;
        }
    }

    // --- Check for conflicting suffix ---
    LogIndex new_first = req.entries.front().index;
    if (new_first <= log_->last_index()) {
        // There may be conflicting entries already in the log.
        // Raft guarantees: if an entry at a given index has the same term,
        // all prior entries are identical.  Truncate the suffix and append.
        LogIndex truncate_at = new_first - 1;
        SPDLOG_TRACE("raft: node {} truncating suffix from {} (new entries "
                     "start at {})",
                     node_id_, new_first, new_first);
        log_->truncate_suffix(truncate_at);
    }

    // --- Append new entries ---
    std::vector<LogEntry> entries_copy = req.entries;
    LogIndex result = log_->append(req.prev_log_index, req.prev_log_term,
                                   entries_copy);

    if (result == kNoLogIndex) {
        SPDLOG_ERROR("raft: node {} log append failed for {} entries "
                     "(prev={}:{})",
                     node_id_, entries_copy.size(),
                     req.prev_log_index, req.prev_log_term);
        conflict_index = req.prev_log_index + 1;
        return false;
    }

    return true;
}

} // namespace torrent::raft
