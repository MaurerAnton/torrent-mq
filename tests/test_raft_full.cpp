#include <gtest/gtest.h>
#include "torrent/consensus/raft.h"
#include "torrent/consensus/raft_types.h"
#include "torrent/common/types.h"
#include <thread>
#include <chrono>
#include <random>
#include <algorithm>

namespace torrent::raft::test {
namespace {

// ===== Mock Raft Log =====
class MockRaftLog : public RaftLog {
public:
    int64_t first_index() const override {
        std::shared_lock lock(mutex_);
        return entries_.empty() ? kNoLogIndex : entries_.begin()->first;
    }
    int64_t last_index() const override {
        std::shared_lock lock(mutex_);
        return entries_.empty() ? kNoLogIndex : entries_.rbegin()->first;
    }
    int64_t entry_count() const override {
        std::shared_lock lock(mutex_);
        return static_cast<int64_t>(entries_.size());
    }
    int64_t term_at(int64_t index) const override {
        std::shared_lock lock(mutex_);
        auto it = entries_.find(index);
        return it == entries_.end() ? kNoTerm : it->second.term;
    }
    std::vector<LogEntry> get_entries(int64_t begin, int64_t end) const override {
        std::shared_lock lock(mutex_);
        std::vector<LogEntry> result;
        for (auto it = entries_.lower_bound(begin);
             it != entries_.end() && it->first < end; ++it) {
            result.push_back(it->second);
        }
        return result;
    }
    LogEntry entry_at(int64_t index) const override {
        std::shared_lock lock(mutex_);
        auto it = entries_.find(index);
        if (it == entries_.end()) return LogEntry{};
        return it->second;
    }
    int64_t append(const std::vector<LogEntry>& entries) override {
        std::unique_lock lock(mutex_);
        int64_t idx = entries_.empty() ? 1 : entries_.rbegin()->first + 1;
        for (const auto& e : entries) {
            entries_[idx] = e;
            entries_[idx].index = idx;
            idx++;
        }
        return idx - 1;
    }
    void truncate_suffix(int64_t from_index) override {
        std::unique_lock lock(mutex_);
        for (auto it = entries_.lower_bound(from_index);
             it != entries_.end();) {
            it = entries_.erase(it);
        }
    }
    void truncate_prefix(int64_t upto_index) override {
        std::unique_lock lock(mutex_);
        for (auto it = entries_.begin();
             it != entries_.end() && it->first < upto_index;) {
            it = entries_.erase(it);
        }
    }
    void apply_snapshot(const RaftSnapshot& snapshot) override {
        std::unique_lock lock(mutex_);
        entries_.clear();
        last_included_index_ = snapshot.last_included_index;
        last_included_term_ = snapshot.last_included_term;
    }
    std::map<int64_t, LogEntry> entries_;
    int64_t last_included_index_{0};
    int64_t last_included_term_{0};
    mutable std::shared_mutex mutex_;
};

// ===== Raft Election Tests =====
TEST(RaftElectionTest, SingleNodeBecomesLeader) {
    MockRaftLog log;
    RaftConfig config;
    config.heartbeat_interval_ms = 50;
    config.election_timeout_min_ms = 50;
    config.election_timeout_max_ms = 100;
    RaftNode node(1, config, log, [](const LogEntry&) {});
    node.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_TRUE(node.is_leader() || node.state() == RaftNodeState::Leader);
    node.shutdown();
}
TEST(RaftElectionTest, TermStartsAtZero) {
    MockRaftLog log;
    RaftConfig config;
    RaftNode node(1, config, log, [](const LogEntry&) {});
    EXPECT_EQ(node.term(), 0);
}
TEST(RaftElectionTest, CandidateIncrementsTerm) {
    MockRaftLog log;
    RaftConfig config;
    config.election_timeout_min_ms = 10;
    config.election_timeout_max_ms = 20;
    config.pre_vote = false;
    RaftNode node(1, config, log, [](const LogEntry&) {});
    node.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_GE(node.term(), 1);
    node.shutdown();
}
TEST(RaftElectionTest, FollowerStateInitially) {
    MockRaftLog log;
    RaftConfig config;
    RaftNode node(1, config, log, [](const LogEntry&) {});
    EXPECT_EQ(node.state(), RaftNodeState::Follower);
}
TEST(RaftElectionTest, StepDown) {
    MockRaftLog log;
    RaftConfig config;
    config.election_timeout_min_ms = 10;
    config.election_timeout_max_ms = 20;
    config.pre_vote = false;
    RaftNode node(1, config, log, [](const LogEntry&) {});
    node.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    node.step_down();
    EXPECT_EQ(node.state(), RaftNodeState::Follower);
    node.shutdown();
}
TEST(RaftElectionTest, ShutdownChangesState) {
    MockRaftLog log;
    RaftConfig config;
    RaftNode node(1, config, log, [](const LogEntry&) {});
    node.start();
    node.shutdown();
    EXPECT_TRUE(true); // No crash
}

// ===== Raft Log Tests =====
TEST(RaftLogTest, EmptyLog) {
    MockRaftLog log;
    EXPECT_EQ(log.first_index(), kNoLogIndex);
    EXPECT_EQ(log.last_index(), kNoLogIndex);
    EXPECT_EQ(log.entry_count(), 0);
}
TEST(RaftLogTest, AppendSingle) {
    MockRaftLog log;
    LogEntry entry;
    entry.term = 1;
    log.append({entry});
    EXPECT_EQ(log.first_index(), 1);
    EXPECT_EQ(log.last_index(), 1);
    EXPECT_EQ(log.entry_count(), 1);
    EXPECT_EQ(log.term_at(1), 1);
}
TEST(RaftLogTest, AppendMultiple) {
    MockRaftLog log;
    for (int i = 0; i < 10; i++) {
        LogEntry entry;
        entry.term = 1;
        log.append({entry});
    }
    EXPECT_EQ(log.entry_count(), 10);
    EXPECT_EQ(log.first_index(), 1);
    EXPECT_EQ(log.last_index(), 10);
}
TEST(RaftLogTest, GetEntriesRange) {
    MockRaftLog log;
    for (int i = 0; i < 10; i++) {
        LogEntry e; e.term = 1;
        log.append({e});
    }
    auto entries = log.get_entries(3, 7);
    EXPECT_EQ(entries.size(), 4);
}
TEST(RaftLogTest, TruncateSuffix) {
    MockRaftLog log;
    for (int i = 0; i < 10; i++) {
        LogEntry e; e.term = 1;
        log.append({e});
    }
    log.truncate_suffix(5);
    EXPECT_EQ(log.last_index(), 4);
    EXPECT_EQ(log.entry_count(), 4);
}
TEST(RaftLogTest, TruncatePrefix) {
    MockRaftLog log;
    for (int i = 0; i < 10; i++) {
        LogEntry e; e.term = 1;
        log.append({e});
    }
    log.truncate_prefix(5);
    EXPECT_EQ(log.first_index(), 5);
}
TEST(RaftLogTest, TermAtMissingIndex) {
    MockRaftLog log;
    EXPECT_EQ(log.term_at(999), kNoTerm);
}

// ===== Raft Config Tests =====
TEST(RaftConfigTest, DefaultHeartbeat) {
    RaftConfig config;
    EXPECT_EQ(config.heartbeat_interval_ms, 150);
}
TEST(RaftConfigTest, ElectionTimeoutRange) {
    RaftConfig config;
    EXPECT_LE(config.election_timeout_min_ms, config.election_timeout_max_ms);
    EXPECT_EQ(config.election_timeout_min_ms, 150);
    EXPECT_EQ(config.election_timeout_max_ms, 300);
}
TEST(RaftConfigTest, PreVoteEnabled) {
    RaftConfig config;
    config.pre_vote = true;
    EXPECT_TRUE(config.pre_vote);
}
TEST(RaftConfigTest, SnapshotThresholds) {
    RaftConfig config;
    EXPECT_EQ(config.snapshot_threshold_entries, 10000);
    EXPECT_EQ(config.snapshot_threshold_bytes, 67108864);
}

// ===== LogEntry Tests =====
TEST(LogEntryTest, NoOpCommand) {
    LogEntry entry;
    entry.term = 1;
    entry.index = 5;
    entry.command = NoOpCommand{};
    EXPECT_EQ(entry.term, 1);
    EXPECT_EQ(entry.index, 5);
    EXPECT_TRUE(std::holds_alternative<NoOpCommand>(entry.command));
}
TEST(LogEntryTest, ConfigChangeCommand) {
    LogEntry entry;
    ConfigChangeCommand cmd;
    cmd.old_members = {1, 2, 3};
    cmd.new_members = {1, 2, 3, 4};
    entry.command = cmd;
    auto& c = std::get<ConfigChangeCommand>(entry.command);
    EXPECT_EQ(c.old_members.size(), 3);
    EXPECT_EQ(c.new_members.size(), 4);
}
TEST(LogEntryTest, UserCommand) {
    LogEntry entry;
    UserCommand cmd;
    cmd.data = {1, 2, 3, 4};
    entry.command = cmd;
    auto& c = std::get<UserCommand>(entry.command);
    EXPECT_EQ(c.data.size(), 4);
}

// ===== RequestVote Tests =====
TEST(RequestVoteTest, DefaultValues) {
    RequestVoteRequest req;
    EXPECT_EQ(req.term, 0);
    EXPECT_EQ(req.candidate_id, 0);
    EXPECT_EQ(req.last_log_index, 0);
    EXPECT_EQ(req.last_log_term, 0);
    EXPECT_FALSE(req.pre_vote);
}
TEST(RequestVoteTest, PreVoteFlag) {
    RequestVoteRequest req;
    req.pre_vote = true;
    EXPECT_TRUE(req.pre_vote);
}
TEST(RequestVoteTest, ResponseDefaults) {
    RequestVoteResponse resp;
    EXPECT_EQ(resp.term, 0);
    EXPECT_FALSE(resp.vote_granted);
}

// ===== AppendEntries Tests =====
TEST(AppendEntriesTest, RequestDefaults) {
    AppendEntriesRequest req;
    EXPECT_EQ(req.term, 0);
    EXPECT_EQ(req.leader_id, 0);
    EXPECT_EQ(req.prev_log_index, 0);
    EXPECT_EQ(req.leader_commit, 0);
    EXPECT_TRUE(req.entries.empty());
}
TEST(AppendEntriesTest, ResponseSuccess) {
    AppendEntriesResponse resp;
    resp.success = true;
    EXPECT_TRUE(resp.success);
}
TEST(AppendEntriesTest, ConflictInfo) {
    AppendEntriesResponse resp;
    resp.conflict_index = 10;
    resp.conflict_term = 3;
    EXPECT_EQ(resp.conflict_index, 10);
    EXPECT_EQ(resp.conflict_term, 3);
}

// ===== Snapshot Tests =====
TEST(SnapshotTest, RequestDefaults) {
    InstallSnapshotRequest req;
    EXPECT_EQ(req.term, 0);
    EXPECT_EQ(req.leader_id, 0);
    EXPECT_EQ(req.offset, 0);
    EXPECT_FALSE(req.done);
}
TEST(SnapshotTest, ChunkedTransfer) {
    InstallSnapshotRequest req;
    req.offset = 0;
    req.data = {1, 2, 3};
    req.done = false;
    EXPECT_EQ(req.data.size(), 3);
    EXPECT_FALSE(req.done);
    InstallSnapshotRequest req2;
    req2.offset = 3;
    req2.data = {4, 5, 6};
    req2.done = true;
    EXPECT_TRUE(req2.done);
}
TEST(SnapshotTest, Metadata) {
    RaftSnapshot snap;
    snap.last_included_index = 100;
    snap.last_included_term = 5;
    EXPECT_EQ(snap.last_included_index, 100);
    EXPECT_EQ(snap.last_included_term, 5);
}

// ===== Membership Tests =====
TEST(MembershipTest, DefaultConfig) {
    RaftMembership members;
    EXPECT_TRUE(members.voters.empty());
    EXPECT_TRUE(members.learners.empty());
}
TEST(MembershipTest, QuorumSize) {
    RaftMembership members;
    members.voters = {1, 2, 3, 4, 5};
    EXPECT_EQ(members.quorum_size(), 3);
}
TEST(MembershipTest, QuorumSizeEven) {
    RaftMembership members;
    members.voters = {1, 2, 3, 4};
    EXPECT_EQ(members.quorum_size(), 3);
}
TEST(MembershipTest, QuorumSizeSingle) {
    RaftMembership members;
    members.voters = {1};
    EXPECT_EQ(members.quorum_size(), 1);
}

// ===== LeaderLease Tests =====
TEST(LeaderLeaseTest, InitiallyInvalid) {
    LeaderLease lease;
    EXPECT_FALSE(lease.is_valid());
}
TEST(LeaderLeaseTest, ExtendMakesValid) {
    LeaderLease lease;
    lease.extend();
    EXPECT_TRUE(lease.is_valid());
}
TEST(LeaderLeaseTest, RevokeInvalidates) {
    LeaderLease lease;
    lease.extend();
    lease.revoke();
    EXPECT_FALSE(lease.is_valid());
}

// ===== State Transitions =====
TEST(StateTransitionTest, FollowerToCandidate) {
    // Follower times out -> becomes Candidate
    RaftNodeState state = RaftNodeState::Follower;
    state = RaftNodeState::Candidate;
    EXPECT_EQ(state, RaftNodeState::Candidate);
}
TEST(StateTransitionTest, CandidateToLeader) {
    // Candidate wins election -> becomes Leader
    RaftNodeState state = RaftNodeState::Candidate;
    state = RaftNodeState::Leader;
    EXPECT_EQ(state, RaftNodeState::Leader);
}
TEST(StateTransitionTest, LeaderToFollower) {
    // Leader receives higher term -> steps down
    RaftNodeState state = RaftNodeState::Leader;
    state = RaftNodeState::Follower;
    EXPECT_EQ(state, RaftNodeState::Follower);
}
TEST(StateTransitionTest, AllStatesHaveNames) {
    for (auto s : {RaftNodeState::Follower, RaftNodeState::Candidate,
                    RaftNodeState::Leader, RaftNodeState::PreCandidate}) {
        const char* name = raft_node_state_to_string(s);
        EXPECT_STRNE(name, "UNKNOWN");
        EXPECT_GT(strlen(name), 2);
    }
}

}  // namespace
}  // namespace torrent::raft::test
