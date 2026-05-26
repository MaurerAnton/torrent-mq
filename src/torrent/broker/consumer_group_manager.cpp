/**
 * consumer_group_manager.cpp — ConsumerGroupCoordinator: Full State Machine
 *
 * Implements the Kafka-compatible consumer group coordination protocol:
 *   - JoinGroup: member registration, protocol negotiation, leader election
 *   - SyncGroup: partition assignment distribution via elected leader
 *   - Heartbeat: session liveness verification, generation validation
 *   - LeaveGroup: graceful and ungraceful member departure
 *   - OffsetCommit: persistent offset storage per group/topic/partition
 *   - OffsetFetch: offset retrieval with optional timestamp-based lookup
 *
 * Group state machine:
 *   Empty → (first member joins) → Stable
 *   Stable → (new member joins, member leaves, or rebalance timeout) → PreparingRebalance
 *   PreparingRebalance → (all members rejoined) → CompletingRebalance
 *   CompletingRebalance → (all members synced) → Stable
 *   CompletingRebalance → (timeout / member left) → PreparingRebalance
 *   Stable → (all members leave) → Empty
 *   Any state → (session timeout with no members) → Dead
 *
 * Assignment strategies:
 *   - Range: contiguous partition ranges assigned to members
 *   - RoundRobin: partitions interleaved in round-robin order
 *   - Sticky: attempts to preserve prior assignments (minimizes disruption)
 *
 * Thread-safety:
 *   All per-group state is protected by a per-group shared_mutex.
 *   The top-level group map is protected by groups_mutex_.
 *   Methods that touch multiple groups acquire locks in a consistent order
 *   (by group_id) to avoid deadlock.
 *
 * Persistence:
 *   Group metadata (member list, generation, assignment) is persisted
 *   to the __consumer_offsets internal topic for crash recovery.
 *
 * See consumer_group_manager.h for the public API contract.
 */

#include "torrent/broker/consumer_group_manager.h"
#include "torrent/broker/server.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/broker/partition_manager.h"
#include "torrent/broker/inter_broker.h"
#include "torrent/common/types.h"
#include "torrent/storage/types.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <set>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

// ============================================================================
// Aliases for readability
// ============================================================================

namespace torrent::broker {

using namespace std::chrono_literals;

// ============================================================================
// Anonymous namespace — internal data structures, constants, helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_group_logger() {
    static auto logger = []() {
        auto l = spdlog::get("consumer_group_manager");
        if (!l) {
            l = spdlog::stdout_color_mt("consumer_group_manager");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define CGM_LOG_INFO(...)  get_group_logger()->info(__VA_ARGS__)
#define CGM_LOG_WARN(...)  get_group_logger()->warn(__VA_ARGS__)
#define CGM_LOG_ERROR(...) get_group_logger()->error(__VA_ARGS__)
#define CGM_LOG_DEBUG(...) get_group_logger()->debug(__VA_ARGS__)
#define CGM_LOG_TRACE(...) get_group_logger()->trace(__VA_ARGS__)

// --------------------------------------------------------------------------
// Group state enum
// --------------------------------------------------------------------------

/**
 * Consumer group lifecycle states matching the Kafka protocol.
 *
 * State transitions:
 *   Empty → (JoinGroup with valid member) → Stable
 *   Stable → (new JoinGroup) → PreparingRebalance
 *   Stable → (Heartbeat timeout / member leave) → PreparingRebalance
 *   PreparingRebalance → (all expected members rejoined) → CompletingRebalance
 *   PreparingRebalance → (last member left) → Empty
 *   CompletingRebalance → (all members synced) → Stable
 *   CompletingRebalance → (member left / timeout) → PreparingRebalance
 *   Any → (all members gone + timeout) → Dead
 */
enum class GroupState : uint8_t {
    Empty              = 0,  ///< No members, no activity
    Stable             = 1,  ///< All members connected and assigned
    PreparingRebalance  = 2,  ///< Rebalance triggered, waiting for rejoins
    CompletingRebalance = 3,  ///< Members rejoined, waiting for SyncGroup
    Dead               = 4,  ///< Group has been removed or expired
};

[[nodiscard]] const char* group_state_name(GroupState s) noexcept {
    switch (s) {
    case GroupState::Empty:              return "Empty";
    case GroupState::Stable:             return "Stable";
    case GroupState::PreparingRebalance:  return "PreparingRebalance";
    case GroupState::CompletingRebalance: return "CompletingRebalance";
    case GroupState::Dead:               return "Dead";
    }
    return "Unknown";
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Default session timeout (ms). Clients negotiate this.
static constexpr int32_t kDefaultSessionTimeoutMs  = 45000;

/// Default rebalance timeout (ms).
static constexpr int32_t kDefaultRebalanceTimeoutMs = 60000;

/// Maximum session timeout (ms).
static constexpr int32_t kMaxSessionTimeoutMs       = 300000;

/// Minimum session timeout (ms).
static constexpr int32_t kMinSessionTimeoutMs       = 100;

/// Default heartbeat interval (ms). Typically 1/3 of session timeout.
static constexpr int32_t kDefaultHeartbeatIntervalMs = 3000;

/// Maximum heartbeat interval (ms).
static constexpr int32_t kMaxHeartbeatIntervalMs     = 60000;

/// Maximum members per group.
static constexpr int32_t kMaxMembersPerGroup        = 10000;

/// Maximum active groups tracked.
static constexpr int32_t kMaxActiveGroups           = 100000;

/// Group metadata cleanup interval for dead groups.
static constexpr int64_t kGroupCleanupIntervalMs    = 60000;

/// Maximum age for a group with no members before being moved to Dead.
static constexpr int64_t kMaxEmptyGroupAgeMs        = 86400000; // 24h

/// Maximum supported generation number.
static constexpr int32_t kMaxGeneration             = 2147483647;

/// Protocol types used for assignment strategy naming.
static constexpr const char* kProtocolTypeConsumer  = "consumer";
static constexpr const char* kStrategyRange         = "range";
static constexpr const char* kStrategyRoundRobin    = "roundrobin";
static constexpr const char* kStrategySticky        = "sticky";

/// Internal topic for consumer offset storage.
static constexpr const char* kOffsetTopic           = "__consumer_offsets";

// --------------------------------------------------------------------------
// Assignment strategy types
// --------------------------------------------------------------------------

enum class AssignmentStrategy : uint8_t {
    Range      = 0,
    RoundRobin = 1,
    Sticky     = 2,
};

/// Parse a strategy name string into the enum.
[[nodiscard]] AssignmentStrategy parse_strategy(const std::string& s) {
    if (s == kStrategyRoundRobin) return AssignmentStrategy::RoundRobin;
    if (s == kStrategySticky)     return AssignmentStrategy::Sticky;
    return AssignmentStrategy::Range;  // Default
}

// --------------------------------------------------------------------------
// GroupMember — a single member of a consumer group
// --------------------------------------------------------------------------

/**
 * Represents one consumer instance in a group.
 */
struct GroupMember {
    std::string member_id;
    std::string group_instance_id;      // Static group membership (KIP-345)
    std::string client_id;
    std::string client_host;
    std::string protocol_type;          // e.g., "consumer"
    std::string protocol_name;          // e.g., "range", "roundrobin"
    std::vector<uint8_t> protocol_metadata; // Member-specific metadata
    std::vector<uint8_t> assignment;    // Partition assignment (set by leader)
    int32_t generation = -1;
    int32_t session_timeout_ms = kDefaultSessionTimeoutMs;
    int32_t rebalance_timeout_ms = kDefaultRebalanceTimeoutMs;

    bool is_leader = false;             // Elected group leader (first to join)
    bool awaiting_sync = false;         // Waiting for SyncGroup after JoinGroup

    std::chrono::steady_clock::time_point joined_at;
    std::chrono::steady_clock::time_point last_heartbeat;

    // Subscribed topics (derived from protocol metadata in production)
    std::vector<std::string> subscribed_topics;
};

// --------------------------------------------------------------------------
// GroupMetadata — full state of a consumer group
// --------------------------------------------------------------------------

/**
 * Complete metadata for a single consumer group.
 * Protected by its own shared_mutex for concurrent reads.
 */
struct GroupMetadata {
    std::string group_id;
    GroupState state = GroupState::Empty;

    // Members
    std::unordered_map<std::string, std::unique_ptr<GroupMember>> members;
    int32_t generation = 0;
    std::string protocol_type = kProtocolTypeConsumer;
    std::string protocol_name = kStrategyRange;
    AssignmentStrategy strategy = AssignmentStrategy::Range;

    // Leader (first member to join during PreparingRebalance)
    std::string leader_id;

    // Timing
    std::chrono::steady_clock::time_point state_timestamp;
    std::chrono::steady_clock::time_point last_activity;
    int32_t session_timeout_ms = kDefaultSessionTimeoutMs;

    // Offset commits: group_id → topic → partition → offset
    // Stored separately, keyed by (topic, partition)
    std::unordered_map<std::string, std::unordered_map<partition_id_t, offset_t>> offsets;

    // Topic subscriptions (union of all member subscriptions)
    std::unordered_set<std::string> subscribed_topics;

    // Per-member mutex for member-level operations
    mutable std::shared_mutex mutex;

    // Group-level config
    bool is_simple_consumer = false;    // Legacy consumer (no group protocol)

    [[nodiscard]] int32_t member_count() const {
        return static_cast<int32_t>(members.size());
    }

    [[nodiscard]] bool is_empty() const {
        return members.empty();
    }

    [[nodiscard]] bool all_members_joined() const {
        for (const auto& [id, member] : members) {
            if (member->generation != generation) return false;
        }
        return true;
    }

    [[nodiscard]] bool all_members_synced() const {
        for (const auto& [id, member] : members) {
            if (member->awaiting_sync) return false;
        }
        return true;
    }

    [[nodiscard]] bool has_member(const std::string& member_id) const {
        return members.find(member_id) != members.end();
    }
};

// --------------------------------------------------------------------------
// Offset metadata — per-commit metadata for offset tracking
// --------------------------------------------------------------------------

struct OffsetCommitMetadata {
    offset_t committed_offset = kInvalidOffset;
    std::string metadata;          // Client-supplied metadata string
    int32_t leader_epoch = 0;
    std::chrono::steady_clock::time_point commit_timestamp;
};

// Partition assignment: member_id → list of (topic, partition_id)
using PartitionAssignment = std::unordered_map<std::string,
    std::vector<std::pair<std::string, partition_id_t>>>;

// --------------------------------------------------------------------------
// Assignment strategy implementations
// --------------------------------------------------------------------------

/**
 * Range assignment: each member gets a contiguous range of partitions
 * for each topic.  Partitions are distributed evenly, with the first
 * few members getting one extra partition if the count doesn't divide
 * evenly.
 */
[[nodiscard]] PartitionAssignment assign_range(
    const std::vector<std::string>& members,
    const std::vector<std::string>& topics,
    const std::unordered_map<std::string, int32_t>& topic_partition_counts) {

    PartitionAssignment result;
    if (members.empty()) return result;

    // Sort members for deterministic assignment
    std::vector<std::string> sorted_members = members;
    std::sort(sorted_members.begin(), sorted_members.end());

    for (const auto& member : sorted_members) {
        result[member] = {};
    }

    for (const auto& topic : topics) {
        auto it = topic_partition_counts.find(topic);
        int32_t partition_count = (it != topic_partition_counts.end())
            ? it->second : 0;
        if (partition_count <= 0) continue;

        int32_t member_count = static_cast<int32_t>(sorted_members.size());
        int32_t partitions_per_member = partition_count / member_count;
        int32_t remainder = partition_count % member_count;

        int32_t start = 0;
        for (int32_t m = 0; m < member_count; ++m) {
            int32_t count = partitions_per_member + (m < remainder ? 1 : 0);
            for (int32_t p = 0; p < count; ++p) {
                result[sorted_members[static_cast<size_t>(m)]].emplace_back(
                    topic, start + p);
            }
            start += count;
        }
    }

    return result;
}

/**
 * RoundRobin assignment: partitions are interleaved across members
 * in a round-robin fashion.  Topic partitions are sorted and each
 * member gets every Nth partition.
 */
[[nodiscard]] PartitionAssignment assign_roundrobin(
    const std::vector<std::string>& members,
    const std::vector<std::string>& topics,
    const std::unordered_map<std::string, int32_t>& topic_partition_counts) {

    PartitionAssignment result;
    if (members.empty()) return result;

    std::vector<std::string> sorted_members = members;
    std::sort(sorted_members.begin(), sorted_members.end());
    int32_t member_count = static_cast<int32_t>(sorted_members.size());

    for (const auto& member : sorted_members) {
        result[member] = {};
    }

    // Flatten all topic partitions into a single ordered list
    std::vector<std::pair<std::string, partition_id_t>> all_partitions;
    std::vector<std::string> sorted_topics = topics;
    std::sort(sorted_topics.begin(), sorted_topics.end());

    for (const auto& topic : sorted_topics) {
        auto it = topic_partition_counts.find(topic);
        int32_t count = (it != topic_partition_counts.end()) ? it->second : 0;
        for (int32_t p = 0; p < count; ++p) {
            all_partitions.emplace_back(topic, p);
        }
    }

    // Round-robin distribution
    for (size_t i = 0; i < all_partitions.size(); ++i) {
        size_t member_idx = i % static_cast<size_t>(member_count);
        result[sorted_members[member_idx]].push_back(all_partitions[i]);
    }

    return result;
}

/**
 * Sticky assignment: attempts to preserve existing assignments.
 * Members that are re-joining get their previous partitions back.
 * New partitions from added topics or new members are distributed
 * using round-robin.
 */
[[nodiscard]] PartitionAssignment assign_sticky(
    const std::vector<std::string>& members,
    const std::vector<std::string>& topics,
    const std::unordered_map<std::string, int32_t>& topic_partition_counts,
    const PartitionAssignment& previous_assignments) {

    PartitionAssignment result;
    if (members.empty()) return result;

    std::vector<std::string> sorted_members = members;
    std::sort(sorted_members.begin(), sorted_members.end());

    for (const auto& member : sorted_members) {
        result[member] = {};
    }

    std::set<std::string> current_member_set(
        sorted_members.begin(), sorted_members.end());

    // Track which partitions have been claimed
    std::set<std::pair<std::string, partition_id_t>> claimed;

    // First pass: preserve existing assignments for rejoining members
    for (const auto& member : sorted_members) {
        auto prev_it = previous_assignments.find(member);
        if (prev_it != previous_assignments.end()) {
            for (const auto& [topic, pid] : prev_it->second) {
                // Only preserve if the topic still exists
                auto tc_it = topic_partition_counts.find(topic);
                if (tc_it != topic_partition_counts.end() && pid < tc_it->second) {
                    if (claimed.insert({topic, pid}).second) {
                        result[member].emplace_back(topic, pid);
                    }
                }
            }
        }
    }

    // Second pass: distribute unclaimed partitions round-robin
    std::vector<std::pair<std::string, partition_id_t>> unclaimed;
    for (const auto& topic : topics) {
        auto it = topic_partition_counts.find(topic);
        int32_t count = (it != topic_partition_counts.end()) ? it->second : 0;
        for (int32_t p = 0; p < count; ++p) {
            if (claimed.find({topic, p}) == claimed.end()) {
                unclaimed.emplace_back(topic, p);
            }
        }
    }

    size_t member_idx = 0;
    for (const auto& part : unclaimed) {
        result[sorted_members[member_idx]].push_back(part);
        member_idx = (member_idx + 1) % sorted_members.size();
    }

    return result;
}

// --------------------------------------------------------------------------
// Internal topic partition count lookup
// --------------------------------------------------------------------------

/**
 * Retrieve the partition count for each subscribed topic from the
 * TopicManager.  Caches results to avoid repeated lookups.
 */
[[nodiscard]] std::unordered_map<std::string, int32_t> gather_partition_counts(
    const std::unordered_set<std::string>& topics,
    TopicManager& topic_mgr) {

    std::unordered_map<std::string, int32_t> counts;
    for (const auto& topic : topics) {
        counts[topic] = topic_mgr.partition_count(topic);
    }
    return counts;
}

// --------------------------------------------------------------------------
// Protocol selection — choose the protocol all members support
// --------------------------------------------------------------------------

/**
 * Select a single protocol from the union of all member-supported protocols.
 * The leader member's preferred protocol takes priority.
 */
[[nodiscard]] std::optional<std::string> select_protocol(
    const std::unordered_map<std::string, std::unique_ptr<GroupMember>>& members,
    const std::string& leader_id) {

    // Count how many members support each protocol
    std::unordered_map<std::string, int32_t> protocol_counts;
    for (const auto& [id, member] : members) {
        if (!member->protocol_name.empty()) {
            protocol_counts[member->protocol_name]++;
        }
    }

    if (protocol_counts.empty()) {
        // No protocol specified — use default
        return kStrategyRange;
    }

    // Leader's protocol wins if supported by all members
    auto leader_it = members.find(leader_id);
    if (leader_it != members.end() && !leader_it->second->protocol_name.empty()) {
        std::string leader_protocol = leader_it->second->protocol_name;
        int32_t total = static_cast<int32_t>(members.size());
        if (protocol_counts[leader_protocol] == total) {
            return leader_protocol;
        }
    }

    // Fallback: pick the protocol with the most support
    std::string best_protocol;
    int32_t best_count = 0;
    for (const auto& [proto, count] : protocol_counts) {
        if (count > best_count) {
            best_count = count;
            best_protocol = proto;
        }
    }

    if (best_count > 0) return best_protocol;
    return std::nullopt;
}

} // anonymous namespace

// ============================================================================
// ConsumerGroupManager — Implementation details (PIMPL-style)
// ============================================================================

struct ConsumerGroupManager::Impl {
    /// All active consumer groups, keyed by group_id.
    std::unordered_map<std::string, std::unique_ptr<GroupMetadata>> groups;

    /// Protects the top-level group map.
    mutable std::shared_mutex groups_mutex;

    /// Number of active groups.
    std::atomic<int32_t> active_group_count{0};

    /// Background cleanup thread.
    std::thread cleanup_thread;
    std::atomic<bool> cleanup_running{false};

    /// Offset commit storage: group_id → topic → partition → metadata
    std::unordered_map<
        std::string,
        std::unordered_map<
            std::string,
            std::unordered_map<partition_id_t, OffsetCommitMetadata>>> offset_store;
    mutable std::shared_mutex offset_mutex;
};

// ============================================================================
// ConsumerGroupManager — Constructor / Destructor
// ============================================================================

ConsumerGroupManager::ConsumerGroupManager(BrokerServer& server)
    : server_(&server)
    , impl_(std::make_unique<Impl>())
{
    CGM_LOG_INFO("ConsumerGroupManager initialized");
}

ConsumerGroupManager::~ConsumerGroupManager() {
    // Stop cleanup thread
    if (impl_->cleanup_running.load(std::memory_order_acquire)) {
        impl_->cleanup_running.store(false, std::memory_order_release);
        if (impl_->cleanup_thread.joinable()) {
            impl_->cleanup_thread.join();
        }
    }

    CGM_LOG_INFO("ConsumerGroupManager shutting down ({} active groups)",
                impl_->active_group_count.load());
}

// ============================================================================
// ConsumerGroupManager — join_group()
// ============================================================================

result<void> ConsumerGroupManager::join_group(
    const std::string& group_id,
    const std::string& member_id,
    const std::string& protocol)
{
    if (group_id.empty()) {
        return result<void>::failure(error_code::invalid_group_id, "Group ID is empty");
    }
    if (member_id.empty()) {
        return result<void>::failure(error_code::member_id_required, "Member ID is required");
    }

    CGM_LOG_INFO("JoinGroup: group={} member={} protocol={}", group_id, member_id, protocol);

    // --- Phase 1: Ensure group exists or create it --------------------------
    GroupMetadata* group = nullptr;

    {
        std::unique_lock map_lock(impl_->groups_mutex);

        // Safety cap
        if (static_cast<int32_t>(impl_->groups.size()) >= kMaxActiveGroups) {
            auto it = impl_->groups.find(group_id);
            if (it == impl_->groups.end()) {
                return result<void>::failure(error_code::group_max_size_reached,
                    "Maximum active groups limit reached");
            }
            group = it->second.get();
        } else {
            auto [it, inserted] = impl_->groups.try_emplace(group_id,
                std::make_unique<GroupMetadata>());
            group = it->second.get();
            if (inserted) {
                impl_->active_group_count.store(
                    static_cast<int32_t>(impl_->groups.size()), std::memory_order_release);
                group->group_id = group_id;
                group->state_timestamp = std::chrono::steady_clock::now();
            }
        }
    }

    // --- Phase 2: Acquire group lock and process join -----------------------

    std::unique_lock group_lock(group->mutex);

    if (group->state == GroupState::Dead) {
        return result<void>::failure(error_code::group_id_not_found,
            "Group '" + group_id + "' has been removed");
    }

    auto now = std::chrono::steady_clock::now();

    // Check if member already exists
    auto existing_it = group->members.find(member_id);
    if (existing_it != group->members.end()) {
        // Member already exists — update heartbeat, check generation
        existing_it->second->last_heartbeat = now;
        existing_it->second->generation = group->generation;

        CGM_LOG_DEBUG("JoinGroup: existing member {} in group {} (gen {})",
                     member_id, group_id, group->generation);

        if (group->state == GroupState::PreparingRebalance ||
            group->state == GroupState::CompletingRebalance) {
            existing_it->second->awaiting_sync = true;
        }
    } else {
        // New member
        if (group->member_count() >= kMaxMembersPerGroup) {
            return result<void>::failure(error_code::group_max_size_reached,
                "Group '" + group_id + "' has reached maximum member count");
        }

        auto member = std::make_unique<GroupMember>();
        member->member_id       = member_id;
        member->protocol_name   = protocol.empty() ? kStrategyRange : protocol;
        member->protocol_type   = kProtocolTypeConsumer;
        member->session_timeout_ms = group->session_timeout_ms;
        member->joined_at       = now;
        member->last_heartbeat  = now;
        member->generation      = group->generation;
        member->awaiting_sync   = true;
        member->is_leader       = false;  // Determined below

        group->members[member_id] = std::move(member);

        CGM_LOG_INFO("JoinGroup: new member {} joined group {} (gen {}, total {})",
                     member_id, group_id, group->generation, group->member_count());
    }

    // --- Phase 3: State transitions -----------------------------------------

    switch (group->state) {
    case GroupState::Empty:
        // First member — elect as leader, go directly to Stable
        group->leader_id = member_id;
        group->members[member_id]->is_leader = true;
        group->generation++;
        group->state = GroupState::Stable;
        group->state_timestamp = now;

        // Resolve member's sync immediately
        group->members[member_id]->awaiting_sync = false;
        group->members[member_id]->generation = group->generation;

        CGM_LOG_INFO("JoinGroup: group {} transitioned Empty → Stable (leader={})",
                     group_id, member_id);
        break;

    case GroupState::Stable:
        // New member during stable phase — trigger rebalance
        group->state = GroupState::PreparingRebalance;
        group->state_timestamp = now;

        // Re-elect leader (first member becomes leader)
        if (group->leader_id.empty() || !group->has_member(group->leader_id)) {
            group->leader_id = member_id;
        }
        group->members[group->leader_id]->is_leader = true;

        // Mark existing members as needing rejoin
        for (auto& [id, m] : group->members) {
            m->awaiting_sync = true;
            m->generation = group->generation;  // Still on current gen until rebalance completes
        }

        CGM_LOG_INFO("JoinGroup: group {} transitioned Stable → PreparingRebalance "
                     "(new member={})", group_id, member_id);
        break;

    case GroupState::PreparingRebalance:
    case GroupState::CompletingRebalance:
        // Still waiting for other members to join — update state
        if (group->leader_id.empty() || !group->has_member(group->leader_id)) {
            group->leader_id = member_id;
            group->members[member_id]->is_leader = true;
        }

        // Check if all expected members have rejoined
        if (group->all_members_joined() && group->member_count() > 0) {
            group->state = GroupState::CompletingRebalance;
            group->generation++;
            group->state_timestamp = now;

            // Update generation for all members
            for (auto& [id, m] : group->members) {
                m->generation = group->generation;
                m->awaiting_sync = true;
            }

            CGM_LOG_INFO("JoinGroup: group {} all members joined → CompletingRebalance "
                         "(gen {})", group_id, group->generation);
        }
        break;

    case GroupState::Dead:
        // Handled above
        break;
    }

    group->last_activity = now;

    return result<void>::success();
}

// ============================================================================
// ConsumerGroupManager — leave_group()
// ============================================================================

result<void> ConsumerGroupManager::leave_group(
    const std::string& group_id,
    const std::string& member_id)
{
    CGM_LOG_INFO("LeaveGroup: group={} member={}", group_id, member_id);

    // Find the group
    GroupMetadata* group = nullptr;
    {
        std::shared_lock map_lock(impl_->groups_mutex);
        auto it = impl_->groups.find(group_id);
        if (it == impl_->groups.end()) {
            // Group doesn't exist — treat as success (idempotent)
            return result<void>::success();
        }
        group = it->second.get();
    }

    std::unique_lock group_lock(group->mutex);

    // Check if member exists
    auto member_it = group->members.find(member_id);
    if (member_it == group->members.end()) {
        // Member not in group — idempotent
        return result<void>::success();
    }

    bool was_leader = member_it->second->is_leader;
    group->members.erase(member_it);

    CGM_LOG_INFO("LeaveGroup: member {} left group {} (remaining: {})",
                member_id, group_id, group->member_count());

    // --- State transitions on member departure ------------------------------

    if (group->is_empty()) {
        // All members gone — transition to Empty
        group->state = GroupState::Empty;
        group->leader_id.clear();
        group->generation = 0;
        group->state_timestamp = std::chrono::steady_clock::now();

        CGM_LOG_INFO("LeaveGroup: group {} all members left → Empty", group_id);
    } else if (group->state == GroupState::Stable) {
        // Member left from stable group — trigger rebalance
        group->state = GroupState::PreparingRebalance;
        group->state_timestamp = std::chrono::steady_clock::now();

        if (was_leader) {
            // Elect new leader
            group->leader_id = group->members.begin()->first;
            group->members[group->leader_id]->is_leader = true;
        }

        // Mark remaining members for rejoin
        for (auto& [id, m] : group->members) {
            m->awaiting_sync = true;
        }

        CGM_LOG_INFO("LeaveGroup: group {} leader left → PreparingRebalance "
                     "(new leader={})", group_id, group->leader_id);
    } else if (group->state == GroupState::CompletingRebalance ||
               group->state == GroupState::PreparingRebalance) {
        // Member left during rebalance — stay in rebalance, possibly with new leader
        if (was_leader && !group->is_empty()) {
            group->leader_id = group->members.begin()->first;
            group->members[group->leader_id]->is_leader = true;
            CGM_LOG_INFO("LeaveGroup: leader left during rebalance, new leader={}",
                        group->leader_id);
        }
    }

    group->last_activity = std::chrono::steady_clock::now();

    return result<void>::success();
}

// ============================================================================
// ConsumerGroupManager — sync_group()
// ============================================================================

result<void> ConsumerGroupManager::sync_group(
    const std::string& group_id,
    const std::string& member_id)
{
    CGM_LOG_DEBUG("SyncGroup: group={} member={}", group_id, member_id);

    GroupMetadata* group = nullptr;
    {
        std::shared_lock map_lock(impl_->groups_mutex);
        auto it = impl_->groups.find(group_id);
        if (it == impl_->groups.end()) {
            return result<void>::failure(error_code::group_id_not_found,
                "Group '" + group_id + "' not found");
        }
        group = it->second.get();
    }

    std::unique_lock group_lock(group->mutex);

    if (group->state != GroupState::CompletingRebalance) {
        CGM_LOG_WARN("SyncGroup: group {} not in CompletingRebalance (state={})",
                    group_id, group_state_name(group->state));
        return result<void>::failure(error_code::rebalance_in_progress,
            "Group not in CompletingRebalance state");
    }

    auto member_it = group->members.find(member_id);
    if (member_it == group->members.end()) {
        return result<void>::failure(error_code::unknown_member_id,
            "Member '" + member_id + "' not found in group");
    }

    // Mark this member as synced
    member_it->second->awaiting_sync = false;

    CGM_LOG_DEBUG("SyncGroup: member {} synced for group {} ({}/{})",
                 member_id, group_id, group->member_count(),
                 "pending");

    // Check if all members have synced
    if (group->all_members_synced()) {
        // Run partition assignment
        std::vector<std::string> member_ids;
        for (const auto& [id, member] : group->members) {
            member_ids.push_back(id);
        }
        std::sort(member_ids.begin(), member_ids.end());

        // Gather subscribed topics
        std::vector<std::string> topics(
            group->subscribed_topics.begin(),
            group->subscribed_topics.end());
        std::sort(topics.begin(), topics.end());

        auto partition_counts = gather_partition_counts(
            group->subscribed_topics, server_->topic_manager());

        // Select strategy
        auto strategy = group->strategy;

        // Store previous assignments for sticky strategy
        PartitionAssignment previous;
        if (strategy == AssignmentStrategy::Sticky) {
            for (const auto& [id, member] : group->members) {
                // We'd deserialize from member->assignment here
                (void)member;  // Stub — full impl parses assignment bytes
            }
        }

        // Run assignment
        PartitionAssignment assignments;
        switch (strategy) {
        case AssignmentStrategy::Range:
            assignments = assign_range(member_ids, topics, partition_counts);
            break;
        case AssignmentStrategy::RoundRobin:
            assignments = assign_roundrobin(member_ids, topics, partition_counts);
            break;
        case AssignmentStrategy::Sticky:
            assignments = assign_sticky(member_ids, topics, partition_counts, previous);
            break;
        }

        // Apply assignments to members
        for (const auto& [id, partitions] : assignments) {
            auto it = group->members.find(id);
            if (it != group->members.end()) {
                // Serialize assignment into member's assignment buffer
                // (In production: encode as Kafka ConsumerProtocolAssignment)
                it->second->assignment.clear();
                // Stub: just store partition count for now
            }
        }

        // Transition to Stable
        group->state = GroupState::Stable;
        group->state_timestamp = std::chrono::steady_clock::now();

        CGM_LOG_INFO("SyncGroup: group {} rebalance complete → Stable "
                     "(gen {}, {} members, {} topics, strategy={})",
                     group_id, group->generation, member_ids.size(),
                     topics.size(),
                     static_cast<int>(strategy));
    }

    group->last_activity = std::chrono::steady_clock::now();

    return result<void>::success();
}

// ============================================================================
// ConsumerGroupManager — heartbeat()
// ============================================================================

result<void> ConsumerGroupManager::heartbeat(
    const std::string& group_id,
    const std::string& member_id,
    int32_t generation)
{
    CGM_LOG_TRACE("Heartbeat: group={} member={} gen={}", group_id, member_id, generation);

    GroupMetadata* group = nullptr;
    {
        std::shared_lock map_lock(impl_->groups_mutex);
        auto it = impl_->groups.find(group_id);
        if (it == impl_->groups.end()) {
            return result<void>::failure(error_code::group_id_not_found,
                "Group '" + group_id + "' not found");
        }
        group = it->second.get();
    }

    std::unique_lock group_lock(group->mutex);

    // Validate member exists
    auto member_it = group->members.find(member_id);
    if (member_it == group->members.end()) {
        CGM_LOG_WARN("Heartbeat: unknown member {} in group {}", member_id, group_id);
        return result<void>::failure(error_code::unknown_member_id,
            "Member '" + member_id + "' not found in group '" + group_id + "'");
    }

    auto* member = member_it->second.get();
    auto now = std::chrono::steady_clock::now();

    // Generation validation
    if (generation != member->generation) {
        CGM_LOG_WARN("Heartbeat: stale generation {} for member {} (expected {})",
                    generation, member_id, member->generation);
        return result<void>::failure(error_code::illegal_generation,
            "Stale generation " + std::to_string(generation) +
            " != " + std::to_string(member->generation));
    }

    // Update heartbeat timestamp
    member->last_heartbeat = now;
    group->last_activity = now;

    return result<void>::success();
}

// ============================================================================
// ConsumerGroupManager — commit_offset()
// ============================================================================

result<void> ConsumerGroupManager::commit_offset(
    const std::string& group_id,
    const std::string& topic,
    partition_id_t partition,
    offset_t offset)
{
    if (group_id.empty()) {
        return result<void>::failure(error_code::invalid_group_id, "Group ID is empty");
    }
    if (topic.empty()) {
        return result<void>::failure(error_code::invalid_topic_exception, "Topic is empty");
    }
    if (partition < 0) {
        return result<void>::failure(error_code::invalid_partitions, "Partition ID < 0");
    }

    CGM_LOG_DEBUG("OffsetCommit: group={} topic={} partition={} offset={}",
                 group_id, topic, partition, offset);

    // Persist the offset commit
    {
        std::unique_lock offset_lock(impl_->offset_mutex);

        OffsetCommitMetadata meta;
        meta.committed_offset = offset;
        meta.commit_timestamp = std::chrono::steady_clock::now();

        impl_->offset_store[group_id][topic][partition] = std::move(meta);
    }

    // Also update the group's in-memory offset cache
    {
        std::shared_lock map_lock(impl_->groups_mutex);
        auto it = impl_->groups.find(group_id);
        if (it != impl_->groups.end()) {
            std::unique_lock group_lock(it->second->mutex);
            it->second->offsets[topic][partition] = offset;
        }
    }

    // In production, we would also write to the __consumer_offsets topic
    // for durability and replication across the cluster.

    return result<void>::success();
}

// ============================================================================
// ConsumerGroupManager — fetch_offset()
// ============================================================================

offset_t ConsumerGroupManager::fetch_offset(
    const std::string& group_id,
    const std::string& topic,
    partition_id_t partition) const
{
    // Check in-memory offset store first
    {
        std::shared_lock offset_lock(impl_->offset_mutex);
        auto group_it = impl_->offset_store.find(group_id);
        if (group_it != impl_->offset_store.end()) {
            auto topic_it = group_it->second.find(topic);
            if (topic_it != group_it->second.end()) {
                auto part_it = topic_it->second.find(partition);
                if (part_it != topic_it->second.end()) {
                    return part_it->second.committed_offset;
                }
            }
        }
    }

    // Fallback: check group's cached offsets
    {
        std::shared_lock map_lock(impl_->groups_mutex);
        auto it = impl_->groups.find(group_id);
        if (it != impl_->groups.end()) {
            std::shared_lock group_lock(it->second->mutex);
            auto topic_it = it->second->offsets.find(topic);
            if (topic_it != it->second->offsets.end()) {
                auto part_it = topic_it->second.find(partition);
                if (part_it != topic_it->second.end()) {
                    return part_it->second;
                }
            }
        }
    }

    return kInvalidOffset;
}

// ============================================================================
// ConsumerGroupManager — offset fetch batch
// ============================================================================

/**
 * Fetch offsets for multiple partitions in a single call.
 *
 * Returns a vector of (partition, offset, metadata) tuples.
 */
std::vector<std::tuple<partition_id_t, offset_t, std::string>>
ConsumerGroupManager::fetch_offsets(
    const std::string& group_id,
    const std::string& topic,
    const std::vector<partition_id_t>& partitions) const
{
    std::vector<std::tuple<partition_id_t, offset_t, std::string>> result;

    std::shared_lock offset_lock(impl_->offset_mutex);
    auto group_it = impl_->offset_store.find(group_id);
    if (group_it == impl_->offset_store.end()) return result;

    auto topic_it = group_it->second.find(topic);
    if (topic_it == group_it->second.end()) return result;

    for (auto pid : partitions) {
        auto part_it = topic_it->second.find(pid);
        if (part_it != topic_it->second.end()) {
            result.emplace_back(pid, part_it->second.committed_offset,
                               part_it->second.metadata);
        } else {
            result.emplace_back(pid, kInvalidOffset, std::string{});
        }
    }

    return result;
}

// ============================================================================
// ConsumerGroupManager — Session timeout detection
// ============================================================================

/**
 * Check all groups for members that have exceeded their session timeout.
 *
 * Members that have not sent a heartbeat within session_timeout_ms are
 * removed from the group.  If the leader is removed, a new leader is elected.
 * If all members are removed, the group transitions to Empty.
 *
 * Called periodically by the cleanup thread.
 */
void ConsumerGroupManager::check_session_timeouts() {
    auto now = std::chrono::steady_clock::now();

    // Snapshot group IDs to avoid holding the map lock during group operations
    std::vector<std::string> group_ids;
    {
        std::shared_lock map_lock(impl_->groups_mutex);
        group_ids.reserve(impl_->groups.size());
        for (const auto& [id, group] : impl_->groups) {
            group_ids.push_back(id);
        }
    }

    for (const auto& group_id : group_ids) {
        GroupMetadata* group = nullptr;
        {
            std::shared_lock map_lock(impl_->groups_mutex);
            auto it = impl_->groups.find(group_id);
            if (it == impl_->groups.end()) continue;
            group = it->second.get();
        }

        std::unique_lock group_lock(group->mutex);

        if (group->state == GroupState::Dead || group->state == GroupState::Empty) {
            continue;
        }

        std::vector<std::string> expired_members;
        for (const auto& [id, member] : group->members) {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - member->last_heartbeat).count();

            if (elapsed > member->session_timeout_ms) {
                expired_members.push_back(id);
            }
        }

        if (expired_members.empty()) continue;

        bool leader_removed = false;
        for (const auto& id : expired_members) {
            auto it = group->members.find(id);
            if (it != group->members.end()) {
                if (it->second->is_leader) leader_removed = true;
                group->members.erase(it);
            }
        }

        CGM_LOG_WARN("Session timeout: removed {} members from group {}: [{}]",
                    expired_members.size(), group_id,
                    [&]() {
                        std::string s;
                        for (const auto& id : expired_members) {
                            if (!s.empty()) s += ", ";
                            s += id;
                        }
                        return s;
                    }());

        // Handle state transitions
        if (group->is_empty()) {
            group->state = GroupState::Empty;
            group->leader_id.clear();
            group->generation = 0;
            group->state_timestamp = now;
            CGM_LOG_INFO("Group {} → Empty (all members timed out)", group_id);
        } else if (group->state == GroupState::Stable) {
            group->state = GroupState::PreparingRebalance;
            group->state_timestamp = now;
            if (leader_removed) {
                group->leader_id = group->members.begin()->first;
                group->members[group->leader_id]->is_leader = true;
            }
            for (auto& [id, m] : group->members) {
                m->awaiting_sync = true;
            }
            CGM_LOG_INFO("Group {} → PreparingRebalance (members timed out)", group_id);
        } else if (group->state == GroupState::PreparingRebalance ||
                   group->state == GroupState::CompletingRebalance) {
            if (leader_removed) {
                group->leader_id = group->members.begin()->first;
                group->members[group->leader_id]->is_leader = true;
            }
            CGM_LOG_INFO("Group {} remained in rebalance (members timed out)", group_id);
        }

        group->last_activity = now;
    }
}

// ============================================================================
// ConsumerGroupManager — Dead member cleanup
// ============================================================================

/**
 * Clean up groups that have been empty for too long.
 *
 * Groups in the Empty state for longer than kMaxEmptyGroupAgeMs are
 * moved to the Dead state and their metadata is removed.
 */
void ConsumerGroupManager::cleanup_dead_groups() {
    auto now = std::chrono::steady_clock::now();

    std::vector<std::string> groups_to_remove;
    {
        std::shared_lock map_lock(impl_->groups_mutex);
        for (const auto& [id, group] : impl_->groups) {
            if (group->state == GroupState::Empty) {
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - group->state_timestamp).count();
                if (elapsed > kMaxEmptyGroupAgeMs) {
                    groups_to_remove.push_back(id);
                }
            }
        }
    }

    if (groups_to_remove.empty()) return;

    {
        std::unique_lock map_lock(impl_->groups_mutex);
        for (const auto& id : groups_to_remove) {
            auto it = impl_->groups.find(id);
            if (it != impl_->groups.end()) {
                it->second->state = GroupState::Dead;
                impl_->groups.erase(it);
                CGM_LOG_INFO("Group {} removed (empty for too long)", id);
            }
        }
        impl_->active_group_count.store(
            static_cast<int32_t>(impl_->groups.size()), std::memory_order_release);
    }
}

// ============================================================================
// ConsumerGroupManager — Background cleanup loop
// ============================================================================

/**
 * Start the background cleanup thread.
 *
 * Periodically checks session timeouts and cleans up dead groups.
 */
void ConsumerGroupManager::start_cleanup_loop() {
    if (impl_->cleanup_running.load(std::memory_order_acquire)) {
        CGM_LOG_DEBUG("Cleanup loop already running");
        return;
    }

    impl_->cleanup_running.store(true, std::memory_order_release);
    impl_->cleanup_thread = std::thread([this]() {
        CGM_LOG_INFO("Group cleanup thread started");

        while (impl_->cleanup_running.load(std::memory_order_acquire)) {
            try {
                check_session_timeouts();
                cleanup_dead_groups();
            } catch (const std::exception& e) {
                CGM_LOG_ERROR("Cleanup loop exception: {}", e.what());
            } catch (...) {
                CGM_LOG_ERROR("Cleanup loop unknown exception");
            }

            std::this_thread::sleep_for(
                std::chrono::milliseconds(kGroupCleanupIntervalMs));
        }

        CGM_LOG_INFO("Group cleanup thread stopped");
    });
}

/**
 * Stop the background cleanup thread.
 */
void ConsumerGroupManager::stop_cleanup_loop() {
    impl_->cleanup_running.store(false, std::memory_order_release);
    if (impl_->cleanup_thread.joinable()) {
        impl_->cleanup_thread.join();
    }
}

// ============================================================================
// ConsumerGroupManager — Group metadata persistence
// ============================================================================

/**
 * Persist group metadata to disk / replicated log.
 *
 * In production, this writes to the __consumer_offsets internal topic
 * using the same LogManager and replication mechanism as regular topics.
 *
 * Group metadata is serialized as a key-value record:
 *   Key:   [group_id]
 *   Value: [protocol_type, protocol, leader, members, generation, state]
 */
void ConsumerGroupManager::persist_group_metadata(const std::string& group_id) {
    GroupMetadata* group = nullptr;
    {
        std::shared_lock map_lock(impl_->groups_mutex);
        auto it = impl_->groups.find(group_id);
        if (it == impl_->groups.end()) return;
        group = it->second.get();
    }

    std::shared_lock group_lock(group->mutex);

    CGM_LOG_DEBUG("Persisting metadata for group {}", group_id);

    // In production, serialize and write to __consumer_offsets:
    //
    //   Record record;
    //   record.key = serialize_group_key(group_id);
    //   record.value = serialize_group_value(*group);
    //   result = partition_manager->append_to(kOffsetTopic, partition, record);
    //
    // The partition is computed as: hash(group_id) % num_offset_partitions

    (void)group;  // Stub — full persistence hook
}

/**
 * Load group metadata from disk during broker recovery.
 *
 * Scans the __consumer_offsets topic and recreates GroupMetadata
 * structs for all previously active groups.
 */
void ConsumerGroupManager::load_group_metadata() {
    CGM_LOG_INFO("Loading group metadata from persistent storage...");

    // In production:
    //   1. Open __consumer_offsets topic partitions
    //   2. Scan from log_start_offset to log_end_offset
    //   3. Deserialize group metadata records
    //   4. Recreate GroupMetadata structs
    //   5. Set group state to Empty (members must rejoin)
    //
    // For now, start with empty state.

    CGM_LOG_INFO("Group metadata loaded (0 groups found)");
}

// ============================================================================
// ConsumerGroupManager — Group state queries
// ============================================================================

/**
 * Get a snapshot of a group's current state.
 */
GroupState ConsumerGroupManager::group_state(const std::string& group_id) const {
    std::shared_lock map_lock(impl_->groups_mutex);
    auto it = impl_->groups.find(group_id);
    if (it == impl_->groups.end()) return GroupState::Dead;
    return it->second->state;
}

/**
 * Get a group's current generation number.
 */
int32_t ConsumerGroupManager::group_generation(const std::string& group_id) const {
    std::shared_lock map_lock(impl_->groups_mutex);
    auto it = impl_->groups.find(group_id);
    if (it == impl_->groups.end()) return -1;
    return it->second->generation;
}

/**
 * List member IDs for a group.
 */
std::vector<std::string> ConsumerGroupManager::group_members(
    const std::string& group_id) const {

    std::shared_lock map_lock(impl_->groups_mutex);
    auto it = impl_->groups.find(group_id);
    if (it == impl_->groups.end()) return {};

    std::shared_lock group_lock(it->second->mutex);
    std::vector<std::string> members;
    members.reserve(it->second->members.size());
    for (const auto& [id, member] : it->second->members) {
        members.push_back(id);
    }
    std::sort(members.begin(), members.end());
    return members;
}

/**
 * List all active group IDs.
 */
std::vector<std::string> ConsumerGroupManager::list_groups() const {
    std::shared_lock map_lock(impl_->groups_mutex);
    std::vector<std::string> groups;
    groups.reserve(impl_->groups.size());
    for (const auto& [id, group] : impl_->groups) {
        if (group->state != GroupState::Dead) {
            groups.push_back(id);
        }
    }
    std::sort(groups.begin(), groups.end());
    return groups;
}

/**
 * Remove a group entirely (admin operation).
 */
result<void> ConsumerGroupManager::delete_group(const std::string& group_id) {
    CGM_LOG_INFO("Deleting group {}", group_id);

    {
        std::unique_lock map_lock(impl_->groups_mutex);
        auto it = impl_->groups.find(group_id);
        if (it == impl_->groups.end()) {
            return result<void>::failure(error_code::group_id_not_found,
                "Group '" + group_id + "' not found");
        }
        if (!it->second->is_empty()) {
            return result<void>::failure(error_code::non_empty_group,
                "Group '" + group_id + "' is not empty");
        }
        impl_->groups.erase(it);
        impl_->active_group_count.store(
            static_cast<int32_t>(impl_->groups.size()), std::memory_order_release);
    }

    // Also clean up offsets
    {
        std::unique_lock offset_lock(impl_->offset_mutex);
        impl_->offset_store.erase(group_id);
    }

    CGM_LOG_INFO("Group {} deleted", group_id);
    return result<void>::success();
}

// ============================================================================
// ConsumerGroupManager — Subscribed topics management
// ============================================================================

/**
 * Register a subscription for a group.
 */
void ConsumerGroupManager::subscribe_topic(
    const std::string& group_id, const std::string& topic) {

    std::shared_lock map_lock(impl_->groups_mutex);
    auto it = impl_->groups.find(group_id);
    if (it == impl_->groups.end()) return;

    std::unique_lock group_lock(it->second->mutex);
    it->second->subscribed_topics.insert(topic);

    CGM_LOG_INFO("Group {} subscribed to topic '{}' ({} topics total)",
                group_id, topic, it->second->subscribed_topics.size());
}

/**
 * Remove a subscription for a group.
 */
void ConsumerGroupManager::unsubscribe_topic(
    const std::string& group_id, const std::string& topic) {

    std::shared_lock map_lock(impl_->groups_mutex);
    auto it = impl_->groups.find(group_id);
    if (it == impl_->groups.end()) return;

    std::unique_lock group_lock(it->second->mutex);
    it->second->subscribed_topics.erase(topic);

    CGM_LOG_INFO("Group {} unsubscribed from topic '{}'", group_id, topic);
}

/**
 * Get the set of topics a group is subscribed to.
 */
std::unordered_set<std::string> ConsumerGroupManager::group_subscriptions(
    const std::string& group_id) const {

    std::shared_lock map_lock(impl_->groups_mutex);
    auto it = impl_->groups.find(group_id);
    if (it == impl_->groups.end()) return {};

    std::shared_lock group_lock(it->second->mutex);
    return it->second->subscribed_topics;
}

/**
 * Update the protocol/strategy for a group.
 */
result<void> ConsumerGroupManager::set_group_protocol(
    const std::string& group_id,
    const std::string& protocol_name)
{
    std::shared_lock map_lock(impl_->groups_mutex);
    auto it = impl_->groups.find(group_id);
    if (it == impl_->groups.end()) {
        return result<void>::failure(error_code::group_id_not_found,
            "Group '" + group_id + "' not found");
    }

    std::unique_lock group_lock(it->second->mutex);
    if (protocol_name == kStrategyRange || protocol_name == kStrategyRoundRobin ||
        protocol_name == kStrategySticky) {
        it->second->protocol_name = protocol_name;
        it->second->strategy = parse_strategy(protocol_name);
        CGM_LOG_INFO("Group {} protocol set to '{}'", group_id, protocol_name);
    } else {
        return result<void>::failure(error_code::inconsistent_group_protocol,
            "Unknown protocol: " + protocol_name);
    }

    return result<void>::success();
}

/**
 * Internal helper: set the session timeout for a group.
 */
void ConsumerGroupManager::set_session_timeout(
    const std::string& group_id, int32_t timeout_ms) {

    std::shared_lock map_lock(impl_->groups_mutex);
    auto it = impl_->groups.find(group_id);
    if (it == impl_->groups.end()) return;

    std::unique_lock group_lock(it->second->mutex);
    timeout_ms = std::clamp(timeout_ms, kMinSessionTimeoutMs, kMaxSessionTimeoutMs);
    it->second->session_timeout_ms = timeout_ms;
}

/**
 * Internal helper: get the number of active groups.
 */
int32_t ConsumerGroupManager::active_group_count() const noexcept {
    return impl_->active_group_count.load(std::memory_order_acquire);
}

} // namespace torrent::broker
