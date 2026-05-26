#include "torrent/consensus/raft.h"
#include "torrent/consensus/raft_types.h"
#include "torrent/common/types.h"
#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <unordered_set>

namespace torrent::raft {
namespace {

// ============================================================================
// Leader lease with bounded staleness for reads
// ============================================================================

class LeaderLeaseManager {
public:
    explicit LeaderLeaseManager(std::chrono::milliseconds lease_duration)
        : lease_duration_ms_(lease_duration.count())
        , lease_start_us_(0)
        , lease_valid_(false) {}

    void extend() {
        lease_start_us_.store(now_us(), std::memory_order_release);
        lease_valid_.store(true, std::memory_order_release);
    }

    void revoke() {
        lease_valid_.store(false, std::memory_order_release);
    }

    bool is_valid(int64_t clock_drift_us = 1000) const {
        if (!lease_valid_.load(std::memory_order_acquire)) return false;
        int64_t elapsed = now_us() - lease_start_us_.load(std::memory_order_acquire);
        return elapsed + clock_drift_us < (lease_duration_ms_ * 1000);
    }

    int64_t remaining_us() const {
        if (!lease_valid_.load(std::memory_order_acquire)) return 0;
        int64_t elapsed = now_us() - lease_start_us_.load(std::memory_order_acquire);
        int64_t remaining = (lease_duration_ms_ * 1000) - elapsed;
        return std::max(int64_t(0), remaining);
    }

private:
    static int64_t now_us() {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count();
    }

    int64_t lease_duration_ms_;
    std::atomic<int64_t> lease_start_us_;
    std::atomic<bool> lease_valid_;
};

// ============================================================================
// ReadIndex protocol for consistent reads from followers
// ============================================================================

struct ReadIndexRequest {
    int64_t request_id;
    std::chrono::steady_clock::time_point start_time;
};

struct ReadIndexResponse {
    int64_t request_id;
    int64_t read_index;  // The log index at which the read can be served
    bool success;
};

class ReadIndexProtocol {
public:
    ReadIndexResponse process_read_index(const ReadIndexRequest& req,
                                          bool is_leader,
                                          int64_t commit_index,
                                          LeaderLeaseManager& lease) {
        ReadIndexResponse resp;
        resp.request_id = req.request_id;

        if (is_leader) {
            // Leader: if lease is valid, return commit_index immediately
            if (lease.is_valid()) {
                resp.read_index = commit_index;
                resp.success = true;
            } else {
                // Lease expired — must contact majority before responding
                resp.read_index = -1;
                resp.success = false;
            }
        } else {
            // Follower: forward to leader or return error
            resp.read_index = -1;
            resp.success = false;
        }
        return resp;
    }

    bool confirm_leadership(int64_t& read_index,
                             const std::vector<bool>& follower_acks,
                             int majority) {
        int acks = 0;
        for (bool ack : follower_acks) {
            if (ack) acks++;
        }
        return acks + 1 >= majority;  // +1 for leader itself
    }
};

// ============================================================================
// Witness nodes (non-voting members)
// ============================================================================

struct WitnessNode {
    RaftNodeId node_id;
    bool is_witness;
    std::chrono::steady_clock::time_point last_contact;
};

class WitnessManager {
public:
    void add_witness(RaftNodeId node_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        witnesses_.push_back({node_id, true, std::chrono::steady_clock::now()});
        spdlog::info("Added witness node {}", node_id);
    }

    void remove_witness(RaftNodeId node_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        witnesses_.erase(
            std::remove_if(witnesses_.begin(), witnesses_.end(),
                           [node_id](const WitnessNode& w) {
                               return w.node_id == node_id;
                           }),
            witnesses_.end());
    }

    bool is_witness(RaftNodeId node_id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& w : witnesses_) {
            if (w.node_id == node_id) return true;
        }
        return false;
    }

    size_t witness_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return std::count_if(witnesses_.begin(), witnesses_.end(),
                              [](const WitnessNode& w) { return w.is_witness; });
    }

private:
    std::vector<WitnessNode> witnesses_;
    mutable std::mutex mutex_;
};

// ============================================================================
// Learner nodes (catch-up without voting)
// ============================================================================

struct LearnerNode {
    RaftNodeId node_id;
    int64_t next_index;
    int64_t match_index;
    bool caught_up;
    std::chrono::steady_clock::time_point start_time;
};

class LearnerManager {
public:
    void add_learner(RaftNodeId node_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        learners_[node_id] = {node_id, 1, 0, false,
                              std::chrono::steady_clock::now()};
        spdlog::info("Added learner node {}", node_id);
    }

    void promote_to_voter(RaftNodeId node_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = learners_.find(node_id);
        if (it != learners_.end()) {
            if (it->second.caught_up) {
                learners_.erase(it);
                spdlog::info("Promoted learner {} to voter", node_id);
            } else {
                spdlog::warn("Cannot promote learner {}: not caught up", node_id);
            }
        }
    }

    void update_progress(RaftNodeId node_id, int64_t match_index) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = learners_.find(node_id);
        if (it != learners_.end()) {
            it->second.match_index = match_index;
        }
    }

    bool is_caught_up(RaftNodeId node_id, int64_t leader_last_index) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = learners_.find(node_id);
        if (it == learners_.end()) return false;
        // Consider caught up within 100 entries
        return (leader_last_index - it->second.match_index) < 100;
    }

private:
    std::unordered_map<RaftNodeId, LearnerNode> learners_;
    mutable std::mutex mutex_;
};

// ============================================================================
// Rate-limited log replication per follower
// ============================================================================

class RateLimitedReplicator {
public:
    struct Config {
        int64_t max_entries_per_batch{1000};
        int64_t max_bytes_per_batch{1048576};  // 1MB
        int64_t max_bytes_per_second{104857600};  // 100MB/s
        int64_t burst_bytes{10485760};  // 10MB burst
    };

    explicit RateLimitedReplicator(const Config& cfg) : config_(cfg) {
        tokens_.store(cfg.burst_bytes, std::memory_order_relaxed);
        last_refill_us_.store(now_us(), std::memory_order_relaxed);
    }

    bool can_replicate(int64_t bytes) {
        refill_tokens();
        int64_t current = tokens_.load(std::memory_order_acquire);
        if (current < bytes) return false;

        // Try to consume tokens
        int64_t desired = current - bytes;
        while (!tokens_.compare_exchange_weak(current, desired,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)) {
            if (current < bytes) return false;
            desired = current - bytes;
        }
        return true;
    }

    int64_t max_entries_this_batch(int64_t available_entries) const {
        return std::min(available_entries, config_.max_entries_per_batch);
    }

private:
    void refill_tokens() {
        int64_t now = now_us();
        int64_t last = last_refill_us_.load(std::memory_order_acquire);
        int64_t elapsed_us = now - last;

        if (elapsed_us < 100000) return;  // Refill at most every 100ms

        int64_t refill = (config_.max_bytes_per_second * elapsed_us) / 1000000;
        if (refill > 0) {
            int64_t current = tokens_.load(std::memory_order_relaxed);
            int64_t new_val = std::min(current + refill, config_.burst_bytes);
            tokens_.store(new_val, std::memory_order_release);
            last_refill_us_.store(now, std::memory_order_release);
        }
    }

    static int64_t now_us() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    Config config_;
    std::atomic<int64_t> tokens_;
    std::atomic<int64_t> last_refill_us_;
};

// ============================================================================
// Raft metrics
// ============================================================================

struct RaftMetrics {
    std::atomic<int64_t> term_changes{0};
    std::atomic<int64_t> elections_started{0};
    std::atomic<int64_t> elections_won{0};
    std::atomic<int64_t> elections_lost{0};
    std::atomic<int64_t> entries_committed{0};
    std::atomic<int64_t> entries_applied{0};
    std::atomic<int64_t> entries_replicated_bytes{0};
    std::atomic<int64_t> append_entries_sent{0};
    std::atomic<int64_t> append_entries_failed{0};
    std::atomic<int64_t> request_vote_received{0};
    std::atomic<int64_t> snapshots_created{0};
    std::atomic<int64_t> snapshots_installed{0};
    std::atomic<int64_t> snapshot_bytes_sent{0};
    std::atomic<int64_t> heartbeat_sent{0};
    std::atomic<int64_t> leadership_transfers{0};
    int64_t leader_since_us{0};

    void record_election_start() { elections_started.fetch_add(1); }
    void record_election_won() { elections_won.fetch_add(1); }
    void record_election_lost() { elections_lost.fetch_add(1); }
    void record_term_change() { term_changes.fetch_add(1); }
    void record_commit() { entries_committed.fetch_add(1); }
    void record_apply() { entries_applied.fetch_add(1); }
    void record_replicated_bytes(int64_t bytes) {
        entries_replicated_bytes.fetch_add(bytes);
    }
    void record_append_success() { append_entries_sent.fetch_add(1); }
    void record_append_failure() { append_entries_failed.fetch_add(1); }
    void record_vote_request() { request_vote_received.fetch_add(1); }
    void record_snapshot_created() { snapshots_created.fetch_add(1); }
    void record_snapshot_installed() { snapshots_installed.fetch_add(1); }
    void record_heartbeat() { heartbeat_sent.fetch_add(1); }
    void record_leadership_transfer() { leadership_transfers.fetch_add(1); }
    void mark_leader() {
        leader_since_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    double append_success_rate() const {
        int64_t total = append_entries_sent.load() + append_entries_failed.load();
        if (total == 0) return 1.0;
        return static_cast<double>(append_entries_sent.load()) / total;
    }
};

// ============================================================================
// Raft debug endpoints
// ============================================================================

class RaftDebugInfo {
public:
    struct NodeInfo {
        RaftNodeId node_id;
        RaftNodeState state;
        int64_t term;
        int64_t commit_index;
        int64_t last_applied;
        int64_t log_size;
        bool is_leader;
        int64_t leader_id;
    };

    struct ReplicationInfo {
        RaftNodeId follower_id;
        int64_t next_index;
        int64_t match_index;
        int64_t lag;
        bool is_active;
    };

    void update_node_info(const NodeInfo& info) {
        std::lock_guard<std::mutex> lock(mutex_);
        node_info_ = info;
    }

    void update_replication(const std::vector<ReplicationInfo>& info) {
        std::lock_guard<std::mutex> lock(mutex_);
        replication_info_ = info;
    }

    NodeInfo get_node_info() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return node_info_;
    }

    std::vector<ReplicationInfo> get_replication_info() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return replication_info_;
    }

    std::string dump_json() const {
        // Build JSON debug output for admin API
        std::ostringstream oss;
        oss << "{";
        oss << "\"state\":\"" << (node_info_.is_leader ? "Leader" : "Follower") << "\",";
        oss << "\"term\":" << node_info_.term << ",";
        oss << "\"commit_index\":" << node_info_.commit_index << ",";
        oss << "\"last_applied\":" << node_info_.last_applied << ",";
        oss << "\"log_size\":" << node_info_.log_size << ",";
        oss << "\"replication\":[";
        bool first = true;
        for (const auto& r : replication_info_) {
            if (!first) oss << ",";
            first = false;
            oss << "{\"follower\":" << r.follower_id
                << ",\"lag\":" << r.lag
                << ",\"active\":" << (r.is_active ? "true" : "false") << "}";
        }
        oss << "]}";
        return oss.str();
    }

private:
    NodeInfo node_info_{};
    std::vector<ReplicationInfo> replication_info_;
    mutable std::mutex mutex_;
};

// ============================================================================
// Quorum-based reads
// ============================================================================

class QuorumReadHandler {
public:
    bool can_serve_read(int64_t leader_commit,
                         const std::vector<int64_t>& follower_match_indices,
                         int total_nodes) {
        int majority = (total_nodes / 2) + 1;

        // Count nodes that have the leader's commit index
        int up_to_date = 1;  // Leader itself
        for (int64_t match_idx : follower_match_indices) {
            if (match_idx >= leader_commit) up_to_date++;
        }

        return up_to_date >= majority;
    }
};

}  // anonymous namespace
}  // namespace torrent::raft
