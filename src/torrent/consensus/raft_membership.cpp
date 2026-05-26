/**
 * raft_membership.cpp — Joint Consensus Membership Changes
 *
 * Implements safe dynamic cluster reconfiguration via joint consensus
 * as described in Ongaro §4:
 *
 *   Joint Consensus Protocol
 *     1. Leader proposes ConfigChange with old+new members (joint=true).
 *     2. Joint config commits via dual-majority (old AND new configs).
 *     3. Leader proposes a second ConfigChange with only new members
 *        (joint=false).
 *     4. Final config commits via single-majority (new config only).
 *
 *   Catch-Up Replication
 *     Before a new node joins, the leader replicates the log to the
 *     prospective member until it is nearly caught up.  This prevents
 *     availability loss during the joint consensus phase.
 *
 *   Add / Remove Node
 *     Add: propose joint config, wait for catch-up, finalise.
 *     Remove: propose joint config, finalise.
 *
 *   Configuration Tracking
 *     Tracks the committed config index, joint config state, and
 *     pending config changes (to prevent concurrent proposals).
 *
 * References:
 *   Ongaro §4 (Cluster Membership Changes)
 *   Raft §4.3 (Joint Consensus)
 */

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "torrent/common/types.h"
#include "torrent/consensus/raft_types.h"

namespace torrent::raft {

// ============================================================================
// Internal Constants
// ============================================================================

/// Maximum number of catch-up entries to send in a single batch.
static constexpr size_t kCatchUpBatchSize = 100;

/// Maximum time to wait for a new node to catch up before timing out.
static constexpr auto kCatchUpTimeout = std::chrono::seconds{30};

/// Number of catch-up rounds before warning about slow nodes.
static constexpr size_t kCatchUpWarnThreshold = 50;

/// Maximum number of membership changes to allow in quick succession.
static constexpr size_t kMaxRapidConfigChanges = 5;

/// Window for "rapid" config change detection.
static constexpr auto kRapidConfigWindow = std::chrono::seconds{10};

// ============================================================================
// Anonymous helpers
// ============================================================================

namespace {

std::shared_ptr<spdlog::logger> get_membership_logger() {
    static auto logger = spdlog::get("raft_membership");
    if (!logger) {
        logger = spdlog::stdout_color_mt("raft_membership");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

[[nodiscard]] bool is_member(const std::vector<RaftNodeId>& members,
                              RaftNodeId id) noexcept {
    return std::find(members.begin(), members.end(), id) != members.end();
}

/// Compute the union of two membership sets (deduplicated).
[[nodiscard]] std::vector<RaftNodeId> union_sets(
    const std::vector<RaftNodeId>& a,
    const std::vector<RaftNodeId>& b)
{
    std::vector<RaftNodeId> result = a;
    for (auto id : b) {
        if (!is_member(result, id)) {
            result.push_back(id);
        }
    }
    return result;
}

/// Compute the quorum size for a set of N nodes.
[[nodiscard]] size_t quorum_for(size_t n) noexcept {
    if (n == 0) return 0;
    return (n / 2) + 1;
}

} // anonymous namespace

// ============================================================================
// MembershipConfig — A committed configuration snapshot
// ============================================================================

/**
 * Represents a single committed membership configuration.
 *
 * During normal operation, `joint` is false and `members` is the active set.
 * During joint consensus, `joint` is true and both `old_members` and
 * `new_members` are set; quorum requires a majority of both.
 */
struct MembershipConfig {
    std::vector<RaftNodeId> members;
    std::vector<RaftNodeId> old_members;   // Only valid during joint consensus.
    bool joint = false;
    LogIndex config_index = 0;             // Log index where this config committed.

    [[nodiscard]] bool in_joint_consensus() const noexcept {
        return joint && !old_members.empty();
    }

    /// The set of all voting members (union during joint consensus).
    [[nodiscard]] std::vector<RaftNodeId> all_members() const {
        if (!in_joint_consensus()) return members;
        return union_sets(members, old_members);
    }

    /// Quorum size accounting for joint consensus dual-majority.
    [[nodiscard]] size_t quorum_size() const noexcept {
        if (!in_joint_consensus()) {
            return quorum_for(members.size());
        }
        // Dual majority: max of both quorums.
        return std::max(quorum_for(members.size()),
                        quorum_for(old_members.size()));
    }

    /// Check if a given node ID is in the configuration (either set).
    [[nodiscard]] bool contains(RaftNodeId id) const noexcept {
        return is_member(members, id)
            || (in_joint_consensus() && is_member(old_members, id));
    }

    /// Human-readable representation for logging.
    [[nodiscard]] std::string describe() const {
        std::string s = "MembershipConfig{members=[";
        for (size_t i = 0; i < members.size(); ++i) {
            if (i > 0) s += ",";
            s += std::to_string(members[i]);
        }
        s += "]";
        if (joint) {
            s += ", joint=true, old_members=[";
            for (size_t i = 0; i < old_members.size(); ++i) {
                if (i > 0) s += ",";
                s += std::to_string(old_members[i]);
            }
            s += "]";
        }
        s += ", config_index=" + std::to_string(config_index) + "}";
        return s;
    }
};

// ============================================================================
// MembershipManager — Joint consensus orchestration
// ============================================================================

/**
 * Manages Raft cluster membership with joint consensus safety.
 *
 * Proposals are converted to ConfigChangeCommand entries, committed
 * through the normal Raft log, and applied atomically via the
 * process_config_change() callback.
 *
 * The manager also orchestrates catch-up replication for new nodes:
 * before the joint config is proposed, the new node's log is brought
 * close to the leader's current state.
 *
 * Usage:
 * @code
 *   MembershipManager mgr(node_id, initial_members, propose_fn);
 *
 *   // To add a node:
 *   mgr.add_node(new_node_id);
 *
 *   // To remove a node:
 *   mgr.remove_node(stale_node_id);
 *
 *   // Called when a ConfigChangeCommand is committed:
 *   mgr.process_config_change(cmd, committed_index);
 * @endcode
 */
class MembershipManager {
public:
    // -- Callback types ----------------------------------------------------

    /// Propose a ConfigChangeCommand to the Raft log.  Returns the log
    /// index assigned, or kNoLogIndex on failure.
    using ProposeFn = std::function<LogIndex(const ConfigChangeCommand& cmd)>;

    /// Called when the committed configuration changes.
    using ConfigChangeNotifier = std::function<void(
        const MembershipConfig& old_config,
        const MembershipConfig& new_config)>;

    /// Provide the leader's current log last index (for catch-up tracking).
    using LastIndexProvider = std::function<LogIndex()>;

    /// Send entries to a node for catch-up.
    using CatchUpSender = std::function<void(
        RaftNodeId target, LogIndex from_index)>;

    // -- Construction -----------------------------------------------------

    /**
     * @param node_id            Local node ID.
     * @param initial_members    Initial cluster membership (must include self).
     * @param propose            Callback to propose a ConfigChangeCommand.
     * @param on_config_change   Optional: notified on every config transition.
     */
    MembershipManager(RaftNodeId node_id,
                      std::vector<RaftNodeId> initial_members,
                      ProposeFn propose,
                      ConfigChangeNotifier on_config_change = {})
        : node_id_(node_id)
        , propose_(std::move(propose))
        , on_config_change_(std::move(on_config_change))
        , pending_config_index_(0)
    {
        config_.members = std::move(initial_members);
        config_.joint = false;
        config_.config_index = 0;

        if (!is_member(config_.members, node_id_)) {
            get_membership_logger()->warn(
                "MembershipManager: node {} not in initial membership — adding",
                node_id_);
            config_.members.push_back(node_id_);
        }

        get_membership_logger()->info(
            "MembershipManager: node {} initialised with {} members: {}",
            node_id_, config_.members.size(), config_.describe());
    }

    // -- Configuration ----------------------------------------------------

    /// Add a node to the cluster using joint consensus.
    ///
    /// Steps:
    ///   1. Validate the request (no concurrent change, node not already
    ///      present, etc.).
    ///   2. If the new node is not up-to-date, initiate catch-up.
    ///   3. Propose joint config: old_members = current, new_members = current+new.
    ///   4. When joint config commits, propose final config: members = current+new.
    ///
    /// @return LogIndex of the proposed joint config entry, or kNoLogIndex on error.
    [[nodiscard]] LogIndex add_node(RaftNodeId new_node) {
        if (pending_config_index_ != 0) {
            get_membership_logger()->warn(
                "MembershipManager: add_node({}) rejected — config change "
                "already in progress at index {}",
                new_node, pending_config_index_);
            return kNoLogIndex;
        }

        if (config_.contains(new_node)) {
            get_membership_logger()->warn(
                "MembershipManager: add_node({}) rejected — node already "
                "in membership", new_node);
            return kNoLogIndex;
        }

        if (rate_limited()) {
            get_membership_logger()->warn(
                "MembershipManager: add_node({}) rejected — rate limited "
                "(too many config changes)", new_node);
            return kNoLogIndex;
        }

        std::vector<RaftNodeId> new_members = config_.members;
        new_members.push_back(new_node);

        // Sort for deterministic ordering across nodes.
        std::sort(new_members.begin(), new_members.end());

        get_membership_logger()->info(
            "MembershipManager: proposing add_node({}): {} -> {}",
            new_node,
            membership_to_string(config_.members),
            membership_to_string(new_members));

        LogIndex idx = propose_joint_config(config_.members, new_members);
        if (idx != kNoLogIndex) {
            pending_config_index_ = idx;
            pending_new_members_ = new_members;
            pending_operation_ = PendingOp::Add;
            record_config_change();
        }
        return idx;
    }

    /// Remove a node from the cluster using joint consensus.
    ///
    /// @return LogIndex of the proposed joint config entry, or kNoLogIndex on error.
    [[nodiscard]] LogIndex remove_node(RaftNodeId target) {
        if (pending_config_index_ != 0) {
            get_membership_logger()->warn(
                "MembershipManager: remove_node({}) rejected — config change "
                "already in progress at index {}",
                target, pending_config_index_);
            return kNoLogIndex;
        }

        if (target == node_id_) {
            get_membership_logger()->warn(
                "MembershipManager: remove_node(self) rejected — "
                "cannot remove self");
            return kNoLogIndex;
        }

        if (!config_.contains(target)) {
            get_membership_logger()->warn(
                "MembershipManager: remove_node({}) rejected — node not "
                "in membership", target);
            return kNoLogIndex;
        }

        if (rate_limited()) {
            get_membership_logger()->warn(
                "MembershipManager: remove_node({}) rejected — rate limited",
                target);
            return kNoLogIndex;
        }

        // Build the new membership sans target.
        std::vector<RaftNodeId> new_members;
        for (auto id : config_.members) {
            if (id != target) new_members.push_back(id);
        }

        get_membership_logger()->info(
            "MembershipManager: proposing remove_node({}): {} -> {}",
            target,
            membership_to_string(config_.members),
            membership_to_string(new_members));

        LogIndex idx = propose_joint_config(config_.members, new_members);
        if (idx != kNoLogIndex) {
            pending_config_index_ = idx;
            pending_new_members_ = new_members;
            pending_operation_ = PendingOp::Remove;
            record_config_change();
        }
        return idx;
    }

    /// Replace the entire membership set (e.g., for initial bootstrap).
    [[nodiscard]] LogIndex replace_membership(
        std::vector<RaftNodeId> new_members)
    {
        if (pending_config_index_ != 0) {
            get_membership_logger()->warn(
                "MembershipManager: replace_membership rejected — "
                "config change in progress");
            return kNoLogIndex;
        }

        std::sort(new_members.begin(), new_members.end());

        get_membership_logger()->info(
            "MembershipManager: proposing membership replacement: "
            "{} -> {}",
            config_.describe(),
            membership_to_string(new_members));

        LogIndex idx = propose_joint_config(config_.members, new_members);
        if (idx != kNoLogIndex) {
            pending_config_index_ = idx;
            pending_new_members_ = new_members;
            pending_operation_ = PendingOp::Replace;
            record_config_change();
        }
        return idx;
    }

    // -- Config Change Processing -----------------------------------------

    /**
     * Process a committed ConfigChangeCommand.
     *
     * Called by the state machine when a committed entry contains a
     * ConfigChangeCommand.  This transitions the membership state through
     * the joint consensus protocol.
     *
     * @param cmd              The committed command.
     * @param committed_index  The log index where it was committed.
     * @return true if the membership configuration changed.
     */
    [[nodiscard]] bool process_config_change(const ConfigChangeCommand& cmd,
                                              LogIndex committed_index)
    {
        MembershipConfig old_config = config_;

        if (cmd.joint_consensus) {
            // --- Enter joint consensus ---
            if (config_.in_joint_consensus()) {
                get_membership_logger()->error(
                    "MembershipManager: received joint config while already "
                    "in joint consensus — ignoring");
                return false;
            }

            config_.old_members = cmd.old_members;
            config_.members = cmd.new_members;
            config_.joint = true;
            config_.config_index = committed_index;

            get_membership_logger()->info(
                "MembershipManager: entered joint consensus at index {}: {}",
                committed_index, config_.describe());

            // Clear pending state; the next step is to propose the final config.
            // This is handled externally — the leader proposes the second
            // ConfigChange after the joint config commits.

        } else {
            // --- Finalise (exit joint consensus or direct apply) ---
            if (config_.in_joint_consensus()) {
                // Exiting joint consensus: new_members becomes the sole config.
                if (!pending_new_members_.empty()) {
                    config_.members = pending_new_members_;
                } else {
                    config_.members = cmd.new_members;
                }
                config_.old_members.clear();
                config_.joint = false;
                config_.config_index = committed_index;

                get_membership_logger()->info(
                    "MembershipManager: finalised joint consensus at index {}: {}",
                    committed_index, config_.describe());

                // Clear pending state.
                pending_config_index_ = 0;
                pending_new_members_.clear();
                pending_operation_ = PendingOp::None;

            } else {
                // Direct membership update (no joint consensus).
                config_.members = cmd.new_members;
                config_.joint = false;
                config_.config_index = committed_index;

                get_membership_logger()->info(
                    "MembershipManager: direct membership update at index {}: {}",
                    committed_index, config_.describe());
            }
        }

        if (on_config_change_) {
            on_config_change_(old_config, config_);
        }

        return true;
    }

    /**
     * Called when the leader should propose the second (final) config
     * after the joint config has committed.
     *
     * @return LogIndex of the proposed final config, or kNoLogIndex.
     */
    [[nodiscard]] LogIndex finalise_joint_consensus() {
        if (!config_.in_joint_consensus()) {
            get_membership_logger()->debug(
                "MembershipManager: finalise_joint_consensus called but "
                "not in joint consensus");
            return kNoLogIndex;
        }

        get_membership_logger()->info(
            "MembershipManager: proposing final config for joint consensus "
            "transition: {}",
            membership_to_string(config_.members));

        ConfigChangeCommand cmd;
        cmd.new_members = config_.members;
        cmd.joint_consensus = false;

        LogIndex idx = propose_(cmd);
        if (idx != kNoLogIndex) {
            pending_final_config_index_ = idx;
        }
        return idx;
    }

    // -- Catch-up Replication ----------------------------------------------

    /**
     * Check if a new node needs catch-up before joining.
     *
     * @param new_node      The prospective node ID.
     * @param leader_last_index  Leader's last log index.
     * @return Number of entries the new node is behind (0 = caught up).
     */
    [[nodiscard]] LogIndex catch_up_required(RaftNodeId new_node,
                                              LogIndex leader_last_index) const {
        auto it = catch_up_state_.find(new_node);
        if (it == catch_up_state_.end()) return leader_last_index;
        return leader_last_index - it->second.match_index;
    }

    /// Record progress of catch-up for a new node.
    void update_catch_up_progress(RaftNodeId node, LogIndex match_index) {
        auto& state = catch_up_state_[node];
        state.match_index = match_index;
        state.last_update = std::chrono::steady_clock::now();
        state.rounds++;

        get_membership_logger()->debug(
            "MembershipManager: catch-up progress for node {}: match={}, "
            "rounds={}",
            node, match_index, state.rounds);
    }

    /// Check if a new node is sufficiently caught up to enter joint consensus.
    /// "Sufficiently" means within a small delta of the leader.
    [[nodiscard]] bool is_caught_up(RaftNodeId node,
                                     LogIndex leader_last_index,
                                     LogIndex max_lag = 10) const {
        auto it = catch_up_state_.find(node);
        if (it == catch_up_state_.end()) return false;
        return (leader_last_index - it->second.match_index) <= max_lag;
    }

    /// Warn if catch-up is taking too long.
    [[nodiscard]] bool catch_up_stalled(RaftNodeId node) const {
        auto it = catch_up_state_.find(node);
        if (it == catch_up_state_.end()) return false;
        return it->second.rounds >= kCatchUpWarnThreshold;
    }

    /// Clean up catch-up state for a node that has fully joined.
    void clear_catch_up(RaftNodeId node) {
        catch_up_state_.erase(node);
    }

    // -- Queries ----------------------------------------------------------

    [[nodiscard]] const MembershipConfig& config() const noexcept {
        return config_;
    }

    [[nodiscard]] const std::vector<RaftNodeId>& members() const noexcept {
        return config_.members;
    }

    [[nodiscard]] bool in_joint_consensus() const noexcept {
        return config_.in_joint_consensus();
    }

    [[nodiscard]] const std::vector<RaftNodeId>& joint_old_members() const noexcept {
        return config_.old_members;
    }

    [[nodiscard]] const std::vector<RaftNodeId>& all_members() const {
        return config_.all_members();
    }

    [[nodiscard]] size_t quorum_size() const noexcept {
        return config_.quorum_size();
    }

    [[nodiscard]] bool has_pending_change() const noexcept {
        return pending_config_index_ != 0;
    }

    [[nodiscard]] LogIndex pending_config_index() const noexcept {
        return pending_config_index_;
    }

    [[nodiscard]] LogIndex config_index() const noexcept {
        return config_.config_index;
    }

    /// True if the given node is a voting member.
    [[nodiscard]] bool is_voter(RaftNodeId id) const noexcept {
        return config_.contains(id);
    }

    /// Human-readable membership description.
    [[nodiscard]] std::string describe() const {
        return config_.describe();
    }

private:
    // -- Pending operation -------------------------------------------------

    enum class PendingOp : uint8_t { None, Add, Remove, Replace };

    // -- Internal helpers --------------------------------------------------

    /// Propose a joint consensus ConfigChange.
    [[nodiscard]] LogIndex propose_joint_config(
        const std::vector<RaftNodeId>& old_set,
        const std::vector<RaftNodeId>& new_set)
    {
        ConfigChangeCommand cmd;
        cmd.new_members = new_set;
        cmd.old_members = old_set;
        cmd.joint_consensus = true;
        return propose_(cmd);
    }

    /// Serialize a membership vector to a string.
    [[nodiscard]] static std::string membership_to_string(
        const std::vector<RaftNodeId>& members)
    {
        std::string s = "[";
        for (size_t i = 0; i < members.size(); ++i) {
            if (i > 0) s += ",";
            s += std::to_string(members[i]);
        }
        s += "]";
        return s;
    }

    /// Record a config change for rate limiting.
    void record_config_change() {
        auto now = std::chrono::steady_clock::now();
        config_change_times_.push_back(now);

        // Prune old entries outside the window.
        auto cutoff = now - kRapidConfigWindow;
        config_change_times_.erase(
            std::remove_if(config_change_times_.begin(),
                           config_change_times_.end(),
                           [cutoff](auto t) { return t < cutoff; }),
            config_change_times_.end());
    }

    /// True if too many config changes have occurred recently.
    [[nodiscard]] bool rate_limited() const {
        return config_change_times_.size() >= kMaxRapidConfigChanges;
    }

    // -- Catch-up state ----------------------------------------------------

    struct CatchUpState {
        LogIndex match_index = 0;
        size_t rounds = 0;
        std::chrono::steady_clock::time_point last_update =
            std::chrono::steady_clock::now();
    };

    // -- Fields -------------------------------------------------------------

    RaftNodeId node_id_;
    MembershipConfig config_;
    ProposeFn propose_;
    ConfigChangeNotifier on_config_change_;

    LogIndex pending_config_index_ = 0;
    LogIndex pending_final_config_index_ = 0;
    std::vector<RaftNodeId> pending_new_members_;
    PendingOp pending_operation_ = PendingOp::None;

    std::unordered_map<RaftNodeId, CatchUpState> catch_up_state_;
    std::vector<std::chrono::steady_clock::time_point> config_change_times_;
};

// ============================================================================
// Free functions: membership utilities
// ============================================================================

/**
 * Validate a membership configuration for correctness:
 *   - Must contain at least 1 node.
 *   - Recommended odd number of nodes (3, 5, 7) for fault tolerance.
 *   - No duplicate IDs.
 *
 * @return true if the configuration is valid.
 */
bool validate_membership(const std::vector<RaftNodeId>& members) noexcept {
    if (members.empty()) {
        get_membership_logger()->error(
            "validate_membership: empty membership");
        return false;
    }

    // Check for duplicates by sorting a copy.
    std::vector<RaftNodeId> sorted = members;
    std::sort(sorted.begin(), sorted.end());
    auto dup = std::adjacent_find(sorted.begin(), sorted.end());
    if (dup != sorted.end()) {
        get_membership_logger()->error(
            "validate_membership: duplicate node ID {} found", *dup);
        return false;
    }

    // Warn about even-sized clusters (poor fault tolerance).
    if (members.size() % 2 == 0) {
        get_membership_logger()->warn(
            "validate_membership: even-sized cluster ({}) — consider "
            "using an odd number for better fault tolerance",
            members.size());
    }

    if (members.size() < 3) {
        get_membership_logger()->warn(
            "validate_membership: cluster size {} — cannot tolerate "
            "any failures", members.size());
    }

    return true;
}

/**
 * Compute the quorum size for a given cluster size.
 */
size_t compute_quorum(size_t cluster_size) noexcept {
    if (cluster_size == 0) return 0;
    return (cluster_size / 2) + 1;
}

/**
 * Compute the quorum size during joint consensus (dual-majority).
 */
size_t compute_joint_quorum(size_t old_size, size_t new_size) noexcept {
    return std::max(compute_quorum(old_size), compute_quorum(new_size));
}

/**
 * Check whether a given node is in the membership set.
 */
bool node_in_membership(const std::vector<RaftNodeId>& members,
                        RaftNodeId id) noexcept {
    return is_member(members, id);
}

/**
 * Build the union of two membership sets (used for quorum during joint
 * consensus).
 */
std::vector<RaftNodeId> membership_union(
    const std::vector<RaftNodeId>& a,
    const std::vector<RaftNodeId>& b)
{
    return union_sets(a, b);
}

/**
 * Generate a recommended initial cluster size message.
 * For production, odd numbers (3, 5, 7) are recommended.
 */
std::string recommend_cluster_size(size_t current_size) {
    if (current_size < 3) {
        return "Minimum recommended cluster size is 3 for fault tolerance";
    }
    if (current_size % 2 == 0) {
        return "Consider using an odd number of nodes ("
               + std::to_string(current_size + 1)
               + ") for better fault tolerance";
    }
    return "Cluster size " + std::to_string(current_size) + " is adequate";
}

} // namespace torrent::raft
