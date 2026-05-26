/**
 * raft_election.cpp — Raft Leader Election with Pre-Vote
 *
 * Implements the full Raft election protocol, including:
 *   - Randomised election timeout with jitter in [150ms, 300ms]
 *   - Pre-Vote (PreCandidate) phase: probe peers without incrementing term
 *   - Candidate phase: increment term, broadcast RequestVote
 *   - Vote tallying: grant votes based on log-up-to-date check
 *   - Split-vote detection and retry with backoff
 *   - Step-down on higher-term discovery
 *
 * This is a standalone module that can be used by the RaftNode monolith or
 * composed into a custom election engine.  All callbacks are injected so the
 * caller controls the transport layer and persistent storage.
 *
 * References:
 *   Ongaro §3.6 (Leader Election), §9.6 (Pre-Vote)
 *   Raft §5.2 (Term-based leader election)
 */

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <functional>
#include <mutex>
#include <random>
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

/// Minimum interval between election timeout firings to prevent storms.
static constexpr auto kMinElectionInterval = std::chrono::milliseconds{10};

/// Default minimum election timeout.  Overridden by RaftConfig.
static constexpr auto kDefaultMinElectionTimeout = std::chrono::milliseconds{150};

/// Default maximum election timeout.  Overridden by RaftConfig.
static constexpr auto kDefaultMaxElectionTimeout = std::chrono::milliseconds{300};

/// Number of election retries before escalating to log warnings.
static constexpr size_t kElectionRetryWarnThreshold = 4;

/// Maximum consecutive election timeouts before triggering a stall alert.
static constexpr size_t kElectionStallThreshold = 10;

// ============================================================================
// Anonymous helpers
// ============================================================================

namespace {

std::shared_ptr<spdlog::logger> get_election_logger() {
    static auto logger = spdlog::get("raft_election");
    if (!logger) {
        logger = spdlog::stdout_color_mt("raft_election");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

/// Human-readable state name for logging.
[[nodiscard]] const char* state_name(RaftNodeState s) noexcept {
    switch (s) {
    case RaftNodeState::Follower:     return "Follower";
    case RaftNodeState::Candidate:    return "Candidate";
    case RaftNodeState::Leader:       return "Leader";
    case RaftNodeState::PreCandidate: return "PreCandidate";
    }
    return "Unknown";
}

/// Return a steady-clock timestamp for deadline arithmetic.
[[nodiscard]] std::chrono::steady_clock::time_point steady_now() {
    return std::chrono::steady_clock::now();
}

} // anonymous namespace

// ============================================================================
// ElectionTimer — Jittered election timeout manager
// ============================================================================

/**
 * Manages the election deadline with configurable jittered timeouts.
 *
 * Each reset picks a new random deadline in [min_timeout, max_timeout].
 * Designed to de-correlate election timers across nodes so that split votes
 * are only a transient condition.
 */
class ElectionTimer {
public:
    /**
     * Construct with timeout range and an initial seed for the PRNG.
     *
     * @param min_ms  Lower bound of the timeout window.
     * @param max_ms  Upper bound (must be >= min_ms).
     * @param seed    Seed for the Mersenne Twister PRNG.  Should be derived
     *                from node_id combined with a clock reading for
     *                de-correlation.
     */
    ElectionTimer(std::chrono::milliseconds min_ms,
                  std::chrono::milliseconds max_ms,
                  uint64_t seed)
        : min_timeout_(std::max(min_ms, kMinElectionInterval))
        , max_timeout_(std::max(max_ms, min_timeout_ + kMinElectionInterval))
        , rng_(static_cast<uint32_t>(seed))
        , deadline_(steady_now() + random_timeout())
    {
        get_election_logger()->debug(
            "ElectionTimer: [{}ms, {}ms] seed={}",
            min_timeout_.count(), max_timeout_.count(), seed);
    }

    /// Reset the timer to now + a new random timeout.  Called on every
    /// heartbeat from a valid leader or after an election completes.
    void reset() {
        deadline_ = steady_now() + random_timeout();
    }

    /// Reset with an explicit time point (used after becoming a candidate).
    void set_deadline_to(std::chrono::steady_clock::time_point t) {
        deadline_ = t;
    }

    /// True if the current time is past the deadline.
    [[nodiscard]] bool expired() const noexcept {
        return steady_now() >= deadline_;
    }

    /// Remaining time until the deadline (>= 0 if already expired).
    [[nodiscard]] std::chrono::milliseconds remaining() const noexcept {
        auto now = steady_now();
        if (now >= deadline_) return std::chrono::milliseconds{0};
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline_ - now);
    }

    /// Current deadline (for use with cv_.wait_until).
    [[nodiscard]] std::chrono::steady_clock::time_point deadline() const noexcept {
        return deadline_;
    }

    [[nodiscard]] std::chrono::milliseconds min_timeout() const noexcept {
        return min_timeout_;
    }
    [[nodiscard]] std::chrono::milliseconds max_timeout() const noexcept {
        return max_timeout_;
    }

private:
    [[nodiscard]] std::chrono::milliseconds random_timeout() {
        long long lo = min_timeout_.count();
        long long hi = max_timeout_.count();
        auto ms = std::uniform_int_distribution<long long>{lo, hi}(rng_);
        return std::chrono::milliseconds{ms};
    }

    std::chrono::milliseconds min_timeout_;
    std::chrono::milliseconds max_timeout_;
    mutable std::mt19937_64 rng_;
    std::chrono::steady_clock::time_point deadline_;
};

// ============================================================================
// VoteTracker — Tally votes during an election round
// ============================================================================

/**
 * Tracks votes granted during a standard election or pre-vote phase.
 *
 * A vote round begins with an implicit self-vote.  Each positive response
 * increments the counter; on reaching a majority (floor(n/2)+1), the round
 * is won.  Higher-term responses from peers cause the candidate to step down
 * immediately.
 */
class VoteTracker {
public:
    /**
     * Construct a vote tracker for a cluster of the given size.
     *
     * @param cluster_size  Total number of voting members.  Must be > 0.
     * @param self_vote     Whether to count the local node (true for Candidate,
     *                      false for PreCandidate although we grant implicit
     *                      self pre-vote).
     */
    VoteTracker(size_t cluster_size, bool self_vote = true)
        : cluster_size_(cluster_size)
        , votes_received_(self_vote ? 1 : 0)
        , peers_responded_(0)
        , quorum_((cluster_size / 2) + 1)
    {
        if (cluster_size_ == 0) {
            get_election_logger()->error(
                "VoteTracker: cluster_size is 0 — quorum is undefined");
            cluster_size_ = 1;
            quorum_ = 1;
        }
    }

    /// Record a vote grant from a peer.  Returns true if quorum reached.
    [[nodiscard]] bool record_grant(RaftNodeId peer) {
        if (already_voted_.find(peer) != already_voted_.end()) {
            get_election_logger()->debug(
                "VoteTracker: duplicate vote from peer {} ignored", peer);
            return quorum_reached();
        }
        already_voted_[peer] = true;
        votes_received_++;
        get_election_logger()->debug(
            "VoteTracker: peer {} granted vote ({}/{})",
            peer, votes_received_, quorum_);
        return quorum_reached();
    }

    /// Record a vote rejection.  Used to detect when all peers have responded
    /// (election round finished without a winner).
    void record_rejection(RaftNodeId peer) {
        if (already_voted_.find(peer) != already_voted_.end()) return;
        already_voted_[peer] = false;
        peers_responded_++;
    }

    /// True when at least quorum_ peers have granted votes.
    [[nodiscard]] bool quorum_reached() const noexcept {
        return votes_received_ >= quorum_;
    }

    /// True when all peers have responded (both grants and rejections).
    /// Used to detect a split-vote scenario.
    [[nodiscard]] bool all_responded() const noexcept {
        return (already_voted_.size() + 1) >= cluster_size_;
    }

    [[nodiscard]] size_t votes() const noexcept { return votes_received_; }
    [[nodiscard]] size_t quorum() const noexcept { return quorum_; }
    [[nodiscard]] size_t cluster_size() const noexcept { return cluster_size_; }

private:
    size_t cluster_size_;
    size_t votes_received_;
    size_t peers_responded_;
    size_t quorum_;
    std::unordered_map<RaftNodeId, bool> already_voted_;
};

// ============================================================================
// ElectionEngine — Full Raft election state machine
// ============================================================================

/**
 * Manages the complete election lifecycle for a single Raft node.
 *
 * Callbacks for sending RPCs and accessing persistent state are injected
 * at construction time.  The engine is purely algorithmic — it does not
 * own any threads or locks; the caller serialises access.
 *
 * Usage:
 * @code
 *   ElectionEngine engine(id, config, get_term_fn, get_log_state_fn,
 *                         get_members_fn, send_rv_fn);
 *
 *   // Called when the election timer fires:
 *   engine.on_timeout();
 *
 *   // Called when a RequestVote arrives from a peer:
 *   auto resp = engine.handle_request_vote(req);
 *
 *   // Called when a RequestVote response arrives:
 *   engine.handle_request_vote_response(from, resp);
 * @endcode
 */
class ElectionEngine {
public:
    // -- Callback types ---------------------------------------------------

    /// Provide the caller's current term.
    using TermProvider = std::function<RaftTerm()>;

    /// Provide last-log index and term for the log-up-to-date check.
    using LogStateProvider = std::function<std::pair<LogIndex, RaftTerm>()>;

    /// Provide the current membership set (union during joint consensus).
    using MembershipProvider = std::function<std::vector<RaftNodeId>()>;

    /// Deliver a RequestVote or Pre-Vote RPC to a peer.
    using RequestVoteSender = std::function<void(
        RaftNodeId target, const RequestVoteRequest& req)>;

    /// Notify the caller of a state transition.
    using StateChangeNotifier = std::function<void(
        RaftNodeState old_state, RaftNodeState new_state, RaftTerm term)>;

    // -- Construction -------------------------------------------------------

    /**
     * Construct the election engine.
     *
     * @param node_id           Cluster-unique node identifier.
     * @param config            Raft tuning parameters (uses election timeout
     *                          range and pre_vote toggle).
     * @param term_provider     Returns the current term (read from caller's
     *                          persistent store).
     * @param log_provider      Returns {last_index, last_term}.
     * @param membership_provider Returns the set of peer node IDs.
     * @param rv_sender         Sends a RequestVote RPC.
     * @param on_state_change   Optional: called on every state transition.
     */
    ElectionEngine(RaftNodeId node_id,
                   const RaftConfig& config,
                   TermProvider term_provider,
                   LogStateProvider log_provider,
                   MembershipProvider membership_provider,
                   RequestVoteSender rv_sender,
                   StateChangeNotifier on_state_change = {})
        : node_id_(node_id)
        , config_(&config)
        , term_provider_(std::move(term_provider))
        , log_provider_(std::move(log_provider))
        , membership_provider_(std::move(membership_provider))
        , rv_sender_(std::move(rv_sender))
        , on_state_change_(std::move(on_state_change))
        , timer_(config.min_election_timeout, config.max_election_timeout,
                 static_cast<uint64_t>(node_id)
                     ^ std::chrono::steady_clock::now()
                           .time_since_epoch()
                           .count())
        , state_(RaftNodeState::Follower)
        , consecutive_timeouts_(0)
    {
        get_election_logger()->info(
            "ElectionEngine: node {} initialised (pre_vote={}, "
            "timeout=[{},{}]ms)",
            node_id_, config_->enable_pre_vote,
            config_->min_election_timeout.count(),
            config_->max_election_timeout.count());
    }

    // -- Lifecycle ----------------------------------------------------------

    /// Transition to Follower state.  If `new_term` > current_term, the
    /// caller should persist the term and clear voted_for.
    /// Returns true if the caller must persist term/voted_for.
    [[nodiscard]] bool become_follower(RaftTerm new_term) {
        RaftNodeState prev = state_;
        RaftTerm prev_term = term_provider_();

        if (new_term > prev_term) {
            get_election_logger()->info(
                "ElectionEngine: node {} term {} -> {} becoming Follower",
                node_id_, prev_term, new_term);
        }

        state_ = RaftNodeState::Follower;
        vote_tracker_.reset();
        consecutive_timeouts_ = 0;
        timer_.reset();

        if (prev != RaftNodeState::Follower) {
            notify_state_change(prev, RaftNodeState::Follower,
                                std::max(new_term, prev_term));
        }

        // The caller must persist term and clear voted_for if term advanced.
        return new_term > prev_term;
    }

    /// Called when the election timer fires.  Transitions to PreCandidate
    /// or Candidate depending on config.
    void on_timeout() {
        RaftTerm current_term = term_provider_();

        if (state_ == RaftNodeState::Leader) {
            get_election_logger()->warn(
                "ElectionEngine: node {} timeout while Leader — ignoring",
                node_id_);
            return;
        }

        // Detect election stalls.
        consecutive_timeouts_++;
        if (consecutive_timeouts_ >= kElectionStallThreshold) {
            get_election_logger()->error(
                "ElectionEngine: node {} election stalled after {} timeouts "
                "(term={}, state={})",
                node_id_, consecutive_timeouts_, current_term,
                state_name(state_));
        } else if (consecutive_timeouts_ >= kElectionRetryWarnThreshold) {
            get_election_logger()->warn(
                "ElectionEngine: node {} election retry {} (term={})",
                node_id_, consecutive_timeouts_, current_term);
        }

        get_election_logger()->info(
            "ElectionEngine: node {} election timeout fired "
            "(state={}, term={})",
            node_id_, state_name(state_), current_term);

        if (config_->enable_pre_vote && state_ != RaftNodeState::Candidate) {
            become_pre_candidate();
        } else {
            become_candidate();
        }
    }

    /// Reset the election timer (called on receiving a valid heartbeat).
    void reset_timer() {
        timer_.reset();
        // Only reset timeout counter on valid leader contact, not on
        // self-initiated transitions.
        if (state_ == RaftNodeState::Follower) {
            consecutive_timeouts_ = 0;
        }
    }

    /// Called when the node becomes a Leader (won election).
    /// Resets election state; the caller handles leader initialisation.
    void on_become_leader() {
        RaftNodeState prev = state_;
        state_ = RaftNodeState::Leader;
        vote_tracker_.reset();
        consecutive_timeouts_ = 0;
        notify_state_change(prev, RaftNodeState::Leader, term_provider_());
    }

    // -- Inbound RPC handler ------------------------------------------------

    /**
     * Handle an incoming RequestVote (or Pre-Vote) from a peer.
     *
     * Standard vote (pre_vote=false):
     *   - Reject if req.term < current_term.
     *   - Grant if voted_for is unset or already this candidate, AND log is
     *     at least as up-to-date.
     *
     * Pre-Vote (pre_vote=true):
     *   - Grant based solely on log-up-to-date check.
     *   - Do not persist voted_for (non-binding).
     *
     * @return Response to send back to the candidate.
     */
    [[nodiscard]] RequestVoteResponse handle_request_vote(
        const RequestVoteRequest& req)
    {
        RaftTerm current_term = term_provider_();
        RequestVoteResponse resp;
        resp.term = current_term;
        resp.vote_granted = false;

        // --- Reject if candidate's term is stale ---
        if (req.term < current_term) {
            get_election_logger()->debug(
                "ElectionEngine: node {} rejecting {}vote from {}: "
                "stale term {} < {}",
                node_id_,
                req.pre_vote ? "pre-" : "",
                req.candidate_id,
                req.term, current_term);
            return resp;
        }

        // --- For standard RequestVote, step down if peer term is higher ---
        if (!req.pre_vote && req.term > current_term) {
            get_election_logger()->info(
                "ElectionEngine: node {} discovered higher term {} > {} "
                "from {} — becoming Follower",
                node_id_, req.term, current_term, req.candidate_id);
            become_follower(req.term);
            resp.term = req.term;
            // Fall through: we may still grant the vote after stepping down.
        }

        // --- Pre-Vote: reject if our term is higher than the candidate's
        //     probing term (they are probing for term+1, we have advanced) ---
        if (req.pre_vote && req.term < current_term + 1) {
            // Not necessarily an error; we may have already seen a higher
            // term.  Reject gracefully.
            get_election_logger()->debug(
                "ElectionEngine: node {} rejecting pre-vote from {}: "
                "we are already at term {}",
                node_id_, req.candidate_id, current_term);
            return resp;
        }

        // --- Log-up-to-date check (§3.6.1) ---
        auto [last_idx, last_term] = log_provider_();

        bool log_ok = log_is_up_to_date(
            req.last_log_index, req.last_log_term,
            last_idx, last_term);

        if (!log_ok) {
            get_election_logger()->debug(
                "ElectionEngine: node {} rejecting {}vote from {}: "
                "log not up-to-date (remote=[{},{}] local=[{},{}])",
                node_id_,
                req.pre_vote ? "pre-" : "",
                req.candidate_id,
                req.last_log_index, req.last_log_term,
                last_idx, last_term);
            return resp;
        }

        // --- Grant vote ---
        // For pre-votes, we don't persist anything — they're non-binding.
        // The caller is responsible for checking/updating voted_for for
        // standard votes.

        resp.vote_granted = true;
        resp.term = std::max(current_term, req.term);

        get_election_logger()->info(
            "ElectionEngine: node {} granted {}vote to {} "
            "(remote_term={}, local_term={})",
            node_id_,
            req.pre_vote ? "pre-" : "",
            req.candidate_id,
            req.term, current_term);

        // On granting a standard vote, reset our own election timer so
        // we don't start a competing election while this one is in-flight.
        if (!req.pre_vote) {
            timer_.reset();
            reset_timer();
        }

        return resp;
    }

    // -- Outbound RPC response handler -------------------------------------

    /**
     * Process a RequestVote response from a peer.
     *
     * Standard election: on grant, tally.  On quorum → caller becomes Leader.
     * Pre-Vote: on majority → caller becomes Candidate.
     * Higher term → step down to Follower.
     *
     * @return The action the caller should take: 'leader', 'candidate',
     *         'follower', or 'none'.
     */
    enum class Action { None, BecomeLeader, BecomeCandidate, BecomeFollower };

    struct ResponseResult {
        Action action = Action::None;
        RaftTerm peer_term = kNoTerm;
        bool vote_granted = false;
    };

    [[nodiscard]] ResponseResult handle_request_vote_response(
        RaftNodeId from,
        const RequestVoteResponse& resp,
        bool was_pre_vote)
    {
        ResponseResult result;
        result.peer_term = resp.term;
        result.vote_granted = resp.vote_granted;

        RaftTerm current_term = term_provider_();

        // --- Higher term: step down immediately ---
        if (resp.term > current_term) {
            get_election_logger()->info(
                "ElectionEngine: node {} discovered higher term {} > {} "
                "from {} — stepping down",
                node_id_, resp.term, current_term, from);
            become_follower(resp.term);
            result.action = Action::BecomeFollower;
            return result;
        }

        // Ignore responses when we're no longer in an election state.
        if (state_ != RaftNodeState::Candidate
            && state_ != RaftNodeState::PreCandidate) {
            get_election_logger()->debug(
                "ElectionEngine: node {} ignoring {}vote response from {} "
                "— not in election state ({})",
                node_id_,
                was_pre_vote ? "pre-" : "",
                from, state_name(state_));
            return result;
        }

        // --- Stale term response (shouldn't happen after the check above,
        //     but handle defensively) ---
        if (resp.term < current_term) {
            get_election_logger()->debug(
                "ElectionEngine: node {} ignoring stale {}vote response "
                "from {} (resp.term={} < current_term={})",
                node_id_,
                was_pre_vote ? "pre-" : "",
                from, resp.term, current_term);
            return result;
        }

        if (!vote_tracker_) {
            get_election_logger()->warn(
                "ElectionEngine: node {} {}vote response from {} but no "
                "active vote tracker — ignoring",
                node_id_,
                was_pre_vote ? "pre-" : "",
                from);
            return result;
        }

        if (resp.vote_granted) {
            bool won = vote_tracker_->record_grant(from);
            if (won) {
                if (was_pre_vote) {
                    get_election_logger()->info(
                        "ElectionEngine: node {} won pre-vote "
                        "({} votes / {} quorum) — advancing to Candidate",
                        node_id_,
                        vote_tracker_->votes(),
                        vote_tracker_->quorum());
                    result.action = Action::BecomeCandidate;
                } else {
                    get_election_logger()->info(
                        "ElectionEngine: node {} won election "
                        "({} votes / {} quorum) for term {}",
                        node_id_,
                        vote_tracker_->votes(),
                        vote_tracker_->quorum(),
                        current_term);
                    result.action = Action::BecomeLeader;
                }
            }
        } else {
            vote_tracker_->record_rejection(from);

            // Split vote: all peers have responded but no quorum.
            if (vote_tracker_->all_responded() && !vote_tracker_->quorum_reached()) {
                get_election_logger()->info(
                    "ElectionEngine: node {} split vote — {} votes of {} "
                    "(need {})",
                    node_id_,
                    vote_tracker_->votes(),
                    vote_tracker_->cluster_size(),
                    vote_tracker_->quorum());
                // Don't transition to Follower here — let the election
                // timeout fire again with a new random delay.  This
                // prevents tight split-vote loops.
                timer_.reset();
            }
        }

        return result;
    }

    // -- Queries ------------------------------------------------------------

    [[nodiscard]] RaftNodeState state() const noexcept { return state_; }
    [[nodiscard]] bool is_candidate() const noexcept {
        return state_ == RaftNodeState::Candidate
            || state_ == RaftNodeState::PreCandidate;
    }

    [[nodiscard]] const ElectionTimer& timer() const noexcept { return timer_; }
    [[nodiscard]] std::chrono::milliseconds randomized_timeout() const noexcept {
        // Return the full timeout range for diagnostics.
        return std::chrono::milliseconds(
            (timer_.min_timeout().count() + timer_.max_timeout().count()) / 2);
    }

    [[nodiscard]] size_t consecutive_timeouts() const noexcept {
        return consecutive_timeouts_;
    }

    /// Step down to Follower at a higher term (called when a peer has a
    /// higher term).
    void step_down(RaftTerm new_term) {
        get_election_logger()->info(
            "ElectionEngine: node {} stepping down to Follower (new_term={})",
            node_id_, new_term);
        become_follower(new_term);
    }

    /// Step down from leader (voluntary abdication).
    void step_down_from_leader(RaftTerm current_term) {
        if (state_ != RaftNodeState::Leader) return;
        get_election_logger()->info(
            "ElectionEngine: node {} voluntarily stepping down from Leader",
            node_id_);
        become_follower(current_term);
    }

private:
    // -- State transitions -------------------------------------------------

    void become_pre_candidate() {
        if (state_ == RaftNodeState::PreCandidate) {
            get_election_logger()->debug(
                "ElectionEngine: node {} already PreCandidate", node_id_);
            return;
        }
        if (state_ == RaftNodeState::Leader) {
            get_election_logger()->warn(
                "ElectionEngine: node {} is Leader — cannot become PreCandidate",
                node_id_);
            return;
        }

        auto members = membership_provider_();
        if (members.empty()) {
            get_election_logger()->warn(
                "ElectionEngine: node {} empty membership — staying Follower",
                node_id_);
            return;
        }

        RaftTerm current_term = term_provider_();
        get_election_logger()->info(
            "ElectionEngine: node {} becoming PreCandidate "
            "(probing for term {})",
            node_id_, current_term + 1);

        RaftNodeState prev = state_;
        state_ = RaftNodeState::PreCandidate;
        vote_tracker_.emplace(members.size(), /*self_vote=*/true);
        timer_.reset();

        notify_state_change(prev, RaftNodeState::PreCandidate, current_term + 1);
        start_vote_round(/*pre_vote=*/true);
    }

    void become_candidate() {
        auto members = membership_provider_();
        if (members.empty()) {
            get_election_logger()->warn(
                "ElectionEngine: node {} empty membership — staying Follower",
                node_id_);
            state_ = RaftNodeState::Follower;
            timer_.reset();
            return;
        }

        // Increment term.  The caller (RaftNode) is responsible for
        // persisting the term and voting for self.  Here we track the
        // term increment and dispatch votes.
        RaftTerm new_term = term_provider_() + 1;

        get_election_logger()->info(
            "ElectionEngine: node {} becoming Candidate for term {} "
            "({} peers, prev_state={})",
            node_id_, new_term, members.size() - 1,
            state_name(state_));

        RaftNodeState prev = state_;
        state_ = RaftNodeState::Candidate;
        vote_tracker_.emplace(members.size(), /*self_vote=*/true);
        timer_.reset();

        notify_state_change(prev, RaftNodeState::Candidate, new_term);
        start_vote_round(/*pre_vote=*/false);
    }

    // -- Vote round ---------------------------------------------------------

    void start_vote_round(bool pre_vote) {
        RaftTerm current_term = term_provider_();
        RaftTerm request_term = pre_vote ? current_term + 1 : current_term;
        auto [last_idx, last_term] = log_provider_();

        RequestVoteRequest req;
        req.term = request_term;
        req.candidate_id = node_id_;
        req.last_log_index = last_idx;
        req.last_log_term = last_term;
        req.pre_vote = pre_vote;

        auto members = membership_provider_();
        size_t sent = 0;

        get_election_logger()->info(
            "ElectionEngine: node {} broadcasting {}RequestVote "
            "(term={}, last_log=[{},{}]) to {} peers",
            node_id_,
            pre_vote ? "Pre-" : "",
            request_term, last_idx, last_term,
            members.empty() ? 0 : members.size() - 1);

        for (auto peer : members) {
            if (peer == node_id_) continue;
            req.term = request_term; // Reset in case of mutation.
            req.pre_vote = pre_vote;
            rv_sender_(peer, req);
            sent++;
        }

        if (sent == 0 && members.size() <= 1) {
            // Single-node cluster: auto-win.
            if (pre_vote) {
                get_election_logger()->info(
                    "ElectionEngine: node {} auto-winning pre-vote "
                    "(single-node cluster)",
                    node_id_);
                become_candidate();
            } else {
                get_election_logger()->info(
                    "ElectionEngine: node {} auto-winning election "
                    "(single-node cluster)",
                    node_id_);
                on_become_leader();
            }
        }
    }

    // -- Log-up-to-date check -----------------------------------------------

    /**
     * Raft §3.6.1: a candidate's log is at least as up-to-date as the
     * receiver's if its last term is higher, or same term and >= index.
     */
    [[nodiscard]] static bool log_is_up_to_date(
        LogIndex remote_last_index, RaftTerm remote_last_term,
        LogIndex local_last_index, RaftTerm local_last_term) noexcept
    {
        if (remote_last_term != local_last_term) {
            return remote_last_term > local_last_term;
        }
        return remote_last_index >= local_last_index;
    }

    // -- State change notification ------------------------------------------

    void notify_state_change(RaftNodeState old_state, RaftNodeState new_state,
                             RaftTerm term) {
        get_election_logger()->debug(
            "ElectionEngine: node {} state {} -> {} (term={})",
            node_id_, state_name(old_state), state_name(new_state), term);
        if (on_state_change_) {
            on_state_change_(old_state, new_state, term);
        }
    }

    // -- Fields -------------------------------------------------------------

    RaftNodeId node_id_;
    const RaftConfig* config_;            // Non-owning; must outlive engine.

    TermProvider term_provider_;
    LogStateProvider log_provider_;
    MembershipProvider membership_provider_;
    RequestVoteSender rv_sender_;
    StateChangeNotifier on_state_change_;

    ElectionTimer timer_;
    RaftNodeState state_;
    std::optional<VoteTracker> vote_tracker_;

    size_t consecutive_timeouts_;
};

// ============================================================================
// Convenience factory
// ============================================================================

/**
 * Create an ElectionEngine pre-configured with the standard Raft timeout
 * parameters from the given config.
 */
ElectionEngine make_election_engine(
    RaftNodeId node_id,
    const RaftConfig& config,
    ElectionEngine::TermProvider term_provider,
    ElectionEngine::LogStateProvider log_provider,
    ElectionEngine::MembershipProvider membership_provider,
    ElectionEngine::RequestVoteSender rv_sender,
    ElectionEngine::StateChangeNotifier on_state_change = {})
{
    return ElectionEngine(
        node_id, config,
        std::move(term_provider),
        std::move(log_provider),
        std::move(membership_provider),
        std::move(rv_sender),
        std::move(on_state_change));
}

// ============================================================================
// Free functions: election helper utilities
// ============================================================================

/**
 * Generate a randomised election timeout in milliseconds.
 * Uses a thread-local Mersenne Twister seeded with a hash of the node_id
 * and thread id for de-correlation.
 *
 * @param node_id  Cluster node ID (used as part of seed).
 * @param min_ms   Minimum timeout (inclusive).
 * @param max_ms   Maximum timeout (inclusive).
 * @return         Random timeout in [min_ms, max_ms].
 */
std::chrono::milliseconds randomized_election_timeout(
    RaftNodeId node_id,
    std::chrono::milliseconds min_ms,
    std::chrono::milliseconds max_ms)
{
    thread_local std::mt19937_64 rng(
        static_cast<uint64_t>(node_id)
        ^ std::hash<std::thread::id>{}(std::this_thread::get_id())
        ^ static_cast<uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count()));

    long long lo = std::max(min_ms.count(), 10LL);
    long long hi = std::max(max_ms.count(), lo + 1LL);

    auto ms = std::uniform_int_distribution<long long>{lo, hi}(rng);
    return std::chrono::milliseconds{ms};
}

/**
 * Check if a candidate's log is at least as up-to-date as the local log.
 *
 * @param remote_last_index  Candidate's last log index.
 * @param remote_last_term   Candidate's last log term.
 * @param local_last_index   Local last log index.
 * @param local_last_term    Local last log term.
 * @return true if the candidate's log is up-to-date.
 */
bool is_log_up_to_date(
    LogIndex remote_last_index, RaftTerm remote_last_term,
    LogIndex local_last_index, RaftTerm local_last_term) noexcept
{
    if (remote_last_term != local_last_term) {
        return remote_last_term > local_last_term;
    }
    return remote_last_index >= local_last_index;
}

/**
 * Compute the quorum size for a cluster of N nodes.
 * Quorum = floor(N/2) + 1.
 */
size_t quorum_size(size_t cluster_size) noexcept {
    if (cluster_size == 0) return 0;
    return (cluster_size / 2) + 1;
}

/**
 * Determine if the node should step down based on a higher term found in
 * an RPC response from a peer.  Returns true if the caller should
 * transition to Follower and update their persisted term.
 */
bool should_step_down(RaftTerm current_term, RaftTerm peer_term) noexcept {
    return peer_term > current_term;
}

} // namespace torrent::raft
