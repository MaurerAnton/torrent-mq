/**
 * torrent-mq — GroupHandler: Full Consumer Group Coordination Handler
 *
 * Implements all consumer-group wire-protocol APIs (Kafka-compatible):
 *   - JoinGroup  (api_key=11) — member joins, leader election, rebalance trigger
 *   - Heartbeat  (api_key=12) — session liveness, generation validation
 *   - LeaveGroup (api_key=13) — member departure, rebalance trigger
 *   - SyncGroup  (api_key=14) — leader distributes partition assignments
 *   - DescribeGroups (api_key=15) — group state introspection
 *   - ListGroups      (api_key=16) — enumerate all active groups
 *   - DeleteGroups    (api_key=42) — delete empty groups
 *
 * Group state machine:
 *   Empty → (first Join) → Stable
 *   Stable → (new member / leader leave) → PreparingRebalance
 *   PreparingRebalance → (all members Synced) → Stable
 *   PreparingRebalance → (all members left) → Empty
 *
 * Assignment strategies:
 *   - Range:      contiguous partition ranges across members
 *   - RoundRobin: interleaved partition assignment
 *   - Sticky:     attempt to preserve prior assignments
 *
 * Thread-safety: Handler methods are called from the request dispatcher.
 * Internal group state is protected by a shared_mutex. All per-group
 * operations acquire the appropriate lock. The handler is stateless beyond
 * the BrokerServer pointer; all persistent group state lives in the handler's
 * internal concurrent map.
 */

#include "torrent/client/group_handler.h"
#include "torrent/broker/server.h"
#include "torrent/broker/consumer_group_manager.h"
#include "torrent/network/protocol.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// ============================================================================
// Namespace aliases for readability
// ============================================================================

namespace tp  = torrent::protocol;

namespace torrent::client {

// ============================================================================
// Anonymous namespace — internal data structures, parsers, serializers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_group_logger() {
    static auto logger = []() {
        auto l = spdlog::get("group_handler");
        if (!l) {
            l = spdlog::stdout_color_mt("group_handler");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define GRP_LOG(level, ...) \
    get_group_logger()->level("[group] " __VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Default session timeout if none provided by client (ms).
constexpr int32_t kDefaultSessionTimeoutMs  = 45000;

/// Default rebalance timeout (ms).
constexpr int32_t kDefaultRebalanceTimeoutMs = 60000;

/// Maximum session timeout allowed (ms) — 5 minutes.
constexpr int32_t kMaxSessionTimeoutMs       = 300000;

/// Minimum session timeout allowed (ms) — 100 ms.
constexpr int32_t kMinSessionTimeoutMs       = 100;

/// Maximum members per group to prevent resource exhaustion.
constexpr int32_t kMaxMembersPerGroup        = 10000;

/// Maximum groups that can be tracked simultaneously.
constexpr int32_t kMaxActiveGroups           = 100000;

/// Default throttle time in response (ms).
constexpr int32_t kDefaultThrottleMs         = 0;

/// Maximum number of supported protocols per member.
constexpr int32_t kMaxProtocolsPerMember     = 10;

/// Maximum protocol name length.
constexpr int32_t kMaxProtocolNameLength     = 256;

/// Maximum metadata payload per member (bytes).
constexpr int32_t kMaxMemberMetadataBytes    = 65536;

/// Name for the "consumer" protocol type used as default.
constexpr const char* kDefaultProtocolType   = "consumer";

// --------------------------------------------------------------------------
// Wire protocol parser helpers (big-endian, Kafka-compatible)
// --------------------------------------------------------------------------

/// Read a big-endian int16 from raw buffer, advancing pos.
inline int16_t read_int16(const char* data, size_t size, size_t& pos) noexcept {
    if (pos + 2 > size) return 0;
    uint16_t raw;
    std::memcpy(&raw, data + pos, 2);
    pos += 2;
    return static_cast<int16_t>(__builtin_bswap16(raw));
}

/// Read a big-endian int32 from raw buffer, advancing pos.
inline int32_t read_int32(const char* data, size_t size, size_t& pos) noexcept {
    if (pos + 4 > size) return 0;
    uint32_t raw;
    std::memcpy(&raw, data + pos, 4);
    pos += 4;
    return static_cast<int32_t>(__builtin_bswap32(raw));
}

/// Read a big-endian int64 from raw buffer, advancing pos.
inline int64_t read_int64(const char* data, size_t size, size_t& pos) noexcept {
    if (pos + 8 > size) return 0;
    uint64_t raw;
    std::memcpy(&raw, data + pos, 8);
    pos += 8;
    return static_cast<int64_t>(__builtin_bswap64(raw));
}

/// Read a nullable string: int16 length (-1 = null), then utf8 bytes.
inline std::string read_nullable_string(const char* data, size_t size, size_t& pos) noexcept {
    int16_t len = read_int16(data, size, pos);
    if (len == -1) return {};
    if (len <= 0)  return {};
    if (pos + static_cast<size_t>(len) > size) {
        pos = size;
        return {};
    }
    std::string s(data + pos, static_cast<size_t>(len));
    pos += static_cast<size_t>(len);
    return s;
}

/// Read a non-nullable string: int16 length, then utf8 bytes.
inline std::string read_string(const char* data, size_t size, size_t& pos) noexcept {
    int16_t len = read_int16(data, size, pos);
    if (len <= 0) return {};
    if (pos + static_cast<size_t>(len) > size) {
        pos = size;
        return {};
    }
    std::string s(data + pos, static_cast<size_t>(len));
    pos += static_cast<size_t>(len);
    return s;
}

/// Read a byte buffer: int32 length, then bytes.
inline std::vector<uint8_t> read_bytes(const char* data, size_t size, size_t& pos) noexcept {
    int32_t len = read_int32(data, size, pos);
    if (len <= 0) return {};
    if (pos + static_cast<size_t>(len) > size) {
        pos = size;
        return {};
    }
    std::vector<uint8_t> buf(reinterpret_cast<const uint8_t*>(data + pos),
                              reinterpret_cast<const uint8_t*>(data + pos + len));
    pos += static_cast<size_t>(len);
    return buf;
}

/// Read an int8 from the buffer.
inline int8_t read_int8(const char* data, size_t size, size_t& pos) noexcept {
    if (pos + 1 > size) return 0;
    int8_t val = static_cast<int8_t>(data[pos]);
    pos += 1;
    return val;
}

// --------------------------------------------------------------------------
// Wire protocol serialization helpers (big-endian)
// --------------------------------------------------------------------------

/// Write a big-endian int16 to buffer.
inline void write_int16(std::vector<uint8_t>& buf, int16_t val) {
    uint16_t n = __builtin_bswap16(static_cast<uint16_t>(val));
    buf.insert(buf.end(), reinterpret_cast<uint8_t*>(&n),
               reinterpret_cast<uint8_t*>(&n) + 2);
}

/// Write a big-endian int32 to buffer.
inline void write_int32(std::vector<uint8_t>& buf, int32_t val) {
    uint32_t n = __builtin_bswap32(static_cast<uint32_t>(val));
    buf.insert(buf.end(), reinterpret_cast<uint8_t*>(&n),
               reinterpret_cast<uint8_t*>(&n) + 4);
}

/// Write a big-endian int64 to buffer.
inline void write_int64(std::vector<uint8_t>& buf, int64_t val) {
    uint64_t n = __builtin_bswap64(static_cast<uint64_t>(val));
    buf.insert(buf.end(), reinterpret_cast<uint8_t*>(&n),
               reinterpret_cast<uint8_t*>(&n) + 8);
}

/// Write a string with int16 length prefix.
inline void write_string(std::vector<uint8_t>& buf, const std::string& s) {
    if (s.empty()) {
        write_int16(buf, 0);
        return;
    }
    if (s.size() > 32767) {
        // Truncate to max int16 length
        write_int16(buf, 32767);
        buf.insert(buf.end(), s.begin(), s.begin() + 32767);
        return;
    }
    write_int16(buf, static_cast<int16_t>(s.size()));
    buf.insert(buf.end(), s.begin(), s.end());
}

/// Write a nullable string with int16 length prefix; -1 for null.
inline void write_nullable_string(std::vector<uint8_t>& buf, const std::string& s) {
    if (s.empty()) {
        write_int16(buf, -1);
        return;
    }
    write_string(buf, s);
}

/// Write byte buffer with int32 length prefix.
inline void write_bytes(std::vector<uint8_t>& buf, const std::vector<uint8_t>& data) {
    write_int32(buf, static_cast<int32_t>(data.size()));
    if (!data.empty()) {
        buf.insert(buf.end(), data.begin(), data.end());
    }
}

/// Write error_code as int16.
inline void write_error_code(std::vector<uint8_t>& buf, tp::ErrorCode ec) {
    write_int16(buf, static_cast<int16_t>(ec));
}

/// Write int8.
inline void write_int8(std::vector<uint8_t>& buf, int8_t val) {
    buf.push_back(static_cast<uint8_t>(val));
}

/// Convert serialized buffer to shared_buffer.
inline shared_buffer to_shared_buffer(std::vector<uint8_t>&& buf) {
    if (buf.empty()) {
        return shared_buffer(0);
    }
    shared_buffer sb(reinterpret_cast<const char*>(buf.data()), buf.size());
    return sb;
}

// --------------------------------------------------------------------------
// Group state machine: states and member records
// --------------------------------------------------------------------------

/// States that a consumer group can be in.
enum class GroupState : uint8_t {
    Empty              = 0,  ///< No active members.
    Stable             = 1,  ///< Members assigned, heartbeating.
    PreparingRebalance = 2,  ///< Rebalance triggered, waiting for (re)joins.
    Dead               = 3,  ///< Group deleted; all state released.
};

/// Human-readable group state name.
[[nodiscard]] const char* group_state_name(GroupState s) noexcept {
    switch (s) {
    case GroupState::Empty:              return "Empty";
    case GroupState::Stable:             return "Stable";
    case GroupState::PreparingRebalance: return "PreparingRebalance";
    case GroupState::Dead:               return "Dead";
    }
    return "Unknown";
}

/// Record for a single member within a consumer group.
struct MemberRecord {
    std::string member_id;          ///< Unique member identifier within this group.
    std::string group_instance_id;  ///< Optional static group instance.
    std::string client_id;          ///< Client identifier from RequestContext.
    std::string client_host;        ///< Client host from RequestContext.
    std::string protocol_type;      ///< Group protocol type (e.g. "consumer").
    std::vector<std::string> supported_protocols;           ///< Protocol names this member supports.
    std::vector<std::vector<uint8_t>> protocol_metadata;    ///< Metadata for each supported protocol.
    std::vector<uint8_t> assignment;                        ///< Current partition assignment (opaque).
    int64_t last_heartbeat_ms = 0;  ///< Timestamp of last received heartbeat.
    int32_t session_timeout_ms = 0; ///< Consumer session timeout.
    int64_t join_time_ms = 0;       ///< When the member joined (epoch ms).
    bool awaiting_sync = false;     ///< True if member has joined but not yet synced.
    bool is_leader = false;         ///< True if this member is the group leader.

    /// Returns true if this member's session has expired given current time.
    [[nodiscard]] bool session_expired(int64_t now_ms) const noexcept {
        if (session_timeout_ms <= 0) return false;
        return (now_ms - last_heartbeat_ms) > session_timeout_ms;
    }
};

/// Full state for a single consumer group.
struct GroupRecord {
    std::string group_id;           ///< Consumer group identifier.
    GroupState state = GroupState::Empty;  ///< Current state machine state.
    int32_t generation_id = 0;      ///< Monotonically increasing generation id.
    std::string protocol_type;      ///< Group protocol type (elected from first member or empty).
    std::string elected_protocol;   ///< Elected protocol name for this generation.
    std::string leader_id;          ///< Member id of the group leader.
    int64_t rebalance_timeout_ms = 0;   ///< Rebalance timeout from the triggering join.
    int64_t rebalance_deadline_ms = 0;  ///< Absolute deadline for current rebalance.

    /// All current members, keyed by member_id.
    std::unordered_map<std::string, MemberRecord> members;

    /// Members that have synced in the current generation (awaiting assignments).
    std::unordered_set<std::string> synced_members;

    /// Per-member assignments provided to the leader during SyncGroup.
    /// Keyed by member_id, contains the opaque assignment bytes each member sent.
    std::unordered_map<std::string, std::vector<uint8_t>> pending_assignments;

    /// Previous assignments from the prior generation (for sticky strategy).
    /// Keyed by member_id.
    std::unordered_map<std::string, std::vector<std::string>> previous_member_partitions;

    /// Tracks which partitions each member owned in the prior stable generation.
    /// Used for sticky assignment.
    std::unordered_map<std::string, std::vector<std::string>> sticky_assignments;

    // -- Creation / join timestamps
    int64_t created_at_ms = 0;      ///< Epoch ms when group was created.
    int64_t last_state_change_ms = 0; ///< Epoch ms of last state transition.

    /// Validate the generation id against the group's current generation.
    /// Returns the appropriate error code.
    [[nodiscard]] tp::ErrorCode validate_generation(int32_t gen) const noexcept {
        if (gen != generation_id) {
            if (gen < generation_id) {
                return tp::ErrorCode::kUnknownMemberId; // stale generation = fenced
            }
            return tp::ErrorCode::kRebalanceInProgress;
        }
        return tp::ErrorCode::kNone;
    }

    /// Remove expired members and return count removed.
    int32_t expire_members(int64_t now_ms) {
        int32_t removed = 0;
        auto it = members.begin();
        while (it != members.end()) {
            if (it->second.session_expired(now_ms)) {
                GRP_LOG(info, "Expiring member {} from group {} (session timeout)",
                        it->first, group_id);
                it = members.erase(it);
                ++removed;
            } else {
                ++it;
            }
        }
        return removed;
    }

    /// Check if any member is still pending sync.
    [[nodiscard]] bool all_members_synced() const noexcept {
        if (members.empty()) return true;
        for (const auto& [id, _] : members) {
            if (synced_members.find(id) == synced_members.end()) {
                return false;
            }
        }
        return true;
    }
};

// --------------------------------------------------------------------------
// Group State Manager — thread-safe container for all group records
// --------------------------------------------------------------------------

class GroupStateManager {
public:
    GroupStateManager() = default;
    ~GroupStateManager() = default;

    GroupStateManager(const GroupStateManager&) = delete;
    GroupStateManager& operator=(const GroupStateManager&) = delete;

    /// Get or create a group record. Returns a reference to the managed record.
    /// The caller must hold the returned lock (via RAII).
    [[nodiscard]] std::pair<GroupRecord*, std::unique_lock<std::shared_mutex>>
    get_or_create(const std::string& group_id) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        // Enforce max active groups limit
        if (groups_.size() >= kMaxActiveGroups) {
            auto it = groups_.find(group_id);
            if (it != groups_.end()) {
                return {&it->second, std::move(lock)};
            }
            return {nullptr, std::move(lock)};
        }

        auto& rec = groups_[group_id];
        if (rec.created_at_ms == 0) {
            rec.group_id = group_id;
            rec.created_at_ms = wall_clock_ms();
            rec.last_state_change_ms = rec.created_at_ms;
        }
        return {&rec, std::move(lock)};
    }

    /// Get a group record if it exists. Returns nullptr if not found.
    [[nodiscard]] std::pair<GroupRecord*, std::shared_lock<std::shared_mutex>>
    get_shared(const std::string& group_id) {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = groups_.find(group_id);
        if (it != groups_.end()) {
            return {&it->second, std::move(lock)};
        }
        return {nullptr, std::shared_lock<std::shared_mutex>{}};
    }

    /// Get a group record for mutation. Returns nullptr if not found.
    [[nodiscard]] std::pair<GroupRecord*, std::unique_lock<std::shared_mutex>>
    get(const std::string& group_id) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        auto it = groups_.find(group_id);
        if (it != groups_.end()) {
            return {&it->second, std::move(lock)};
        }
        return {nullptr, std::move(lock)};
    }

    /// List all group IDs (shared lock).
    [[nodiscard]] std::vector<std::string> list_group_ids() {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        std::vector<std::string> ids;
        ids.reserve(groups_.size());
        for (const auto& [id, _] : groups_) {
            ids.push_back(id);
        }
        return ids;
    }

    /// List groups with protocol type filter.
    [[nodiscard]] std::vector<std::pair<std::string, std::string>>
    list_groups_with_protocol() {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        std::vector<std::pair<std::string, std::string>> result;
        result.reserve(groups_.size());
        for (const auto& [id, rec] : groups_) {
            result.emplace_back(id, rec.protocol_type.empty()
                                          ? kDefaultProtocolType
                                          : rec.protocol_type);
        }
        return result;
    }

    /// Delete a group if it exists and is empty. Returns error code.
    [[nodiscard]] tp::ErrorCode delete_group(const std::string& group_id) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        auto it = groups_.find(group_id);
        if (it == groups_.end()) {
            return tp::ErrorCode::kUnknownGroupId;
        }
        if (!it->second.members.empty()) {
            return tp::ErrorCode::kUnknownServerError; // group not empty
        }
        it->second.state = GroupState::Dead;
        groups_.erase(it);
        GRP_LOG(info, "Deleted group {}", group_id);
        return tp::ErrorCode::kNone;
    }

    /// Remove expired members from all groups (called periodically).
    void expire_all(int64_t now_ms) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        for (auto& [id, rec] : groups_) {
            int32_t removed = rec.expire_members(now_ms);
            if (removed > 0 && rec.members.empty()) {
                rec.state = GroupState::Empty;
                rec.last_state_change_ms = now_ms;
                GRP_LOG(info, "Group {} transitioned to Empty after all members expired", id);
            }
        }
    }

    /// Get total number of active groups.
    [[nodiscard]] size_t active_count() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return groups_.size();
    }

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, GroupRecord> groups_;
};

/// Global singleton for group state. (Lifetime = process lifetime.)
[[nodiscard]] GroupStateManager& group_state_manager() {
    static GroupStateManager gsm;
    return gsm;
}

// --------------------------------------------------------------------------
// Utility — clock
// --------------------------------------------------------------------------

/// Wall-clock time in milliseconds since epoch.
[[nodiscard]] inline int64_t wall_clock_ms() noexcept {
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

// --------------------------------------------------------------------------
// Utility — generate unique member ID
// --------------------------------------------------------------------------

[[nodiscard]] std::string generate_member_id(const std::string& client_id,
                                              const std::string& group_id) {
    // Kafka-style: clientId-groupUuid-timestamp
    static std::atomic<int64_t> counter{0};
    auto now = wall_clock_ms();
    auto cnt = counter.fetch_add(1, std::memory_order_relaxed);
    return client_id + "-" + std::to_string(now) + "-" + std::to_string(cnt);
}

// --------------------------------------------------------------------------
// Protocol selection — elect a protocol common to all members
// --------------------------------------------------------------------------

/// Choose the first protocol that is supported by all members.
/// Returns the elected protocol name, or empty string if none is common.
[[nodiscard]] std::string elect_protocol(
    const std::unordered_map<std::string, MemberRecord>& members) {

    if (members.empty()) return {};

    // Collect intersection of protocols
    std::unordered_map<std::string, int32_t> protocol_count;
    int32_t total_members = static_cast<int32_t>(members.size());

    for (const auto& [member_id, rec] : members) {
        for (const auto& proto : rec.supported_protocols) {
            protocol_count[proto]++;
        }
    }

    // First protocol supported by all members wins
    for (const auto& [member_id, rec] : members) {
        for (const auto& proto : rec.supported_protocols) {
            if (protocol_count[proto] == total_members) {
                return proto;
            }
        }
    }

    return {};
}

// ============================================================================
// ASSIGNMENT STRATEGIES
// ============================================================================

/// How partition assignments are distributed among group members.
enum class AssignmentStrategy : uint8_t {
    Range      = 0,  ///< Contiguous partition ranges.
    RoundRobin = 1,  ///< Interleaved round-robin.
    Sticky     = 2,  ///< Preserve prior assignments where possible.
};

/// Metadata that consumers can include in their protocol metadata to declare
/// which topics/partitions they are interested in.
struct MemberSubscription {
    std::string member_id;
    std::vector<std::string> topics;
    /// For parsing subscription metadata: we store topic->partition_count.
    std::unordered_map<std::string, int32_t> topic_partition_counts;
};

/// Parse subscription metadata from a member's protocol_metadata.
/// Expected format (simple text-based for now):
///   Each line: "topic_name:partition_count"
/// For the first protocol metadata entry, parse into a subscription.
[[nodiscard]] MemberSubscription parse_subscription(
    const MemberRecord& member) {

    MemberSubscription sub;
    sub.member_id = member.member_id;

    for (const auto& meta : member.protocol_metadata) {
        if (meta.empty()) continue;

        // Try to parse as text: topic:count format
        std::string text(reinterpret_cast<const char*>(meta.data()), meta.size());

        // Simple parse: split by newlines or commas
        size_t pos = 0;
        while (pos < text.size()) {
            // Find end of this entry
            size_t end = text.find_first_of(",\n", pos);
            if (end == std::string::npos) end = text.size();

            std::string entry = text.substr(pos, end - pos);
            // Trim whitespace
            while (!entry.empty() && std::isspace(static_cast<unsigned char>(entry.front())))
                entry.erase(0, 1);
            while (!entry.empty() && std::isspace(static_cast<unsigned char>(entry.back())))
                entry.pop_back();

            if (!entry.empty()) {
                // Look for colon separator
                size_t colon = entry.find(':');
                if (colon != std::string::npos) {
                    std::string topic = entry.substr(0, colon);
                    std::string count_str = entry.substr(colon + 1);
                    int32_t count = 1;
                    try {
                        count = std::stoi(count_str);
                    } catch (...) {
                        count = 1;
                    }
                    sub.topics.push_back(topic);
                    sub.topic_partition_counts[topic] = count;
                } else {
                    // No colon: assume single partition
                    sub.topics.push_back(entry);
                    sub.topic_partition_counts[entry] = 1;
                }
            }

            pos = end + 1;
            if (end == text.size()) break;
        }
    }

    return sub;
}

// --------------------------------------------------------------------------
// Range Assignment Strategy
// --------------------------------------------------------------------------
// Divides partitions of each topic into contiguous ranges, one per member.
//
// Example: 3 members, topic "foo" with 7 partitions
//   member-0: foo[0,1,2]
//   member-1: foo[3,4]
//   member-2: foo[5,6]
//
[[nodiscard]] std::unordered_map<std::string, std::vector<std::string>>
assign_range(
    const std::vector<MemberSubscription>& subscriptions,
    const std::vector<std::string>& member_ids) {

    std::unordered_map<std::string, std::vector<std::string>> assignments;

    if (member_ids.empty() || subscriptions.empty()) return assignments;

    int32_t num_members = static_cast<int32_t>(member_ids.size());

    // Collect all unique topics from all subscriptions
    std::unordered_set<std::string> all_topics;
    for (const auto& sub : subscriptions) {
        for (const auto& topic : sub.topics) {
            all_topics.insert(topic);
        }
    }

    for (const auto& topic : all_topics) {
        // Find the maximum partition count for this topic across all members
        int32_t num_partitions = 1;
        for (const auto& sub : subscriptions) {
            auto it = sub.topic_partition_counts.find(topic);
            if (it != sub.topic_partition_counts.end() && it->second > num_partitions) {
                num_partitions = it->second;
            }
        }

        // Distribute partitions with range strategy
        int32_t partitions_per_member = num_partitions / num_members;
        int32_t remainder = num_partitions % num_members;

        int32_t start = 0;
        for (int32_t i = 0; i < num_members; ++i) {
            int32_t extra = (i < remainder) ? 1 : 0;
            int32_t count = partitions_per_member + extra;

            if (count > 0) {
                for (int32_t p = 0; p < count && (start + p) < num_partitions; ++p) {
                    assignments[member_ids[i]].push_back(
                        topic + ":" + std::to_string(start + p));
                }
            }
            start += count;
        }
    }

    return assignments;
}

// --------------------------------------------------------------------------
// RoundRobin Assignment Strategy
// --------------------------------------------------------------------------
// Assigns partitions one by one in round-robin order across members.
//
// Example: 3 members, topic "foo" with 7 partitions
//   member-0: foo[0,3,6]
//   member-1: foo[1,4]
//   member-2: foo[2,5]
//
[[nodiscard]] std::unordered_map<std::string, std::vector<std::string>>
assign_round_robin(
    const std::vector<MemberSubscription>& subscriptions,
    const std::vector<std::string>& member_ids) {

    std::unordered_map<std::string, std::vector<std::string>> assignments;

    if (member_ids.empty() || subscriptions.empty()) return assignments;

    int32_t num_members = static_cast<int32_t>(member_ids.size());

    // Collect all unique topics
    std::unordered_set<std::string> all_topics;
    for (const auto& sub : subscriptions) {
        for (const auto& topic : sub.topics) {
            all_topics.insert(topic);
        }
    }

    for (const auto& topic : all_topics) {
        int32_t num_partitions = 1;
        for (const auto& sub : subscriptions) {
            auto it = sub.topic_partition_counts.find(topic);
            if (it != sub.topic_partition_counts.end() && it->second > num_partitions) {
                num_partitions = it->second;
            }
        }

        for (int32_t p = 0; p < num_partitions; ++p) {
            int32_t member_idx = p % num_members;
            assignments[member_ids[member_idx]].push_back(
                topic + ":" + std::to_string(p));
        }
    }

    return assignments;
}

// --------------------------------------------------------------------------
// Sticky Assignment Strategy
// --------------------------------------------------------------------------
// Attempts to keep existing partition assignments stable. New partitions
// are assigned to the member with the fewest partitions. If a member leaves,
// its partitions are redistributed to remaining members.
//
[[nodiscard]] std::unordered_map<std::string, std::vector<std::string>>
assign_sticky(
    const std::vector<MemberSubscription>& subscriptions,
    const std::vector<std::string>& member_ids,
    const std::unordered_map<std::string, std::vector<std::string>>& previous_assignments) {

    std::unordered_map<std::string, std::vector<std::string>> assignments;

    if (member_ids.empty() || subscriptions.empty()) return assignments;

    // Start with empty assignments for current members
    std::unordered_set<std::string> current_member_set(member_ids.begin(), member_ids.end());

    for (const auto& mid : member_ids) {
        assignments[mid] = {};
    }

    // Collect all unique topics
    std::unordered_set<std::string> all_topics;
    for (const auto& sub : subscriptions) {
        for (const auto& topic : sub.topics) {
            all_topics.insert(topic);
        }
    }

    // Build set of all partitions
    struct PartitionKey {
        std::string topic;
        int32_t partition = 0;

        bool operator==(const PartitionKey& o) const {
            return topic == o.topic && partition == o.partition;
        }
    };
    struct PartitionKeyHash {
        size_t operator()(const PartitionKey& k) const {
            return std::hash<std::string>{}(k.topic) ^
                   (std::hash<int32_t>{}(k.partition) << 1);
        }
    };

    // 1. Try to preserve existing assignments for members that are still present
    std::unordered_set<PartitionKey, PartitionKeyHash> assigned_partitions;
    for (const auto& [prev_member, partitions] : previous_assignments) {
        if (current_member_set.find(prev_member) == current_member_set.end()) {
            // This member is gone — its partitions need redistribution
            continue;
        }
        for (const auto& part : partitions) {
            // Parse "topic:partition_index"
            size_t colon = part.find(':');
            if (colon == std::string::npos) continue;
            std::string topic = part.substr(0, colon);
            int32_t pidx = std::stoi(part.substr(colon + 1));

            if (all_topics.find(topic) != all_topics.end()) {
                assignments[prev_member].push_back(part);
                assigned_partitions.insert({topic, pidx});
            }
        }
    }

    // 2. For partitions not yet assigned, assign to member with fewest
    for (const auto& topic : all_topics) {
        int32_t num_partitions = 1;
        for (const auto& sub : subscriptions) {
            auto it = sub.topic_partition_counts.find(topic);
            if (it != sub.topic_partition_counts.end() && it->second > num_partitions) {
                num_partitions = it->second;
            }
        }

        for (int32_t p = 0; p < num_partitions; ++p) {
            if (assigned_partitions.find({topic, p}) != assigned_partitions.end()) {
                continue; // already assigned
            }

            // Find member with fewest current assignments
            const std::string* best_member = &member_ids[0];
            size_t fewest = assignments[*best_member].size();

            for (const auto& mid : member_ids) {
                if (assignments[mid].size() < fewest) {
                    fewest = assignments[mid].size();
                    best_member = &mid;
                }
            }

            assignments[*best_member].push_back(
                topic + ":" + std::to_string(p));
            assigned_partitions.insert({topic, p});
        }
    }

    return assignments;
}

// --------------------------------------------------------------------------
// Strategy dispatcher
// --------------------------------------------------------------------------

/// Apply the named strategy to produce member→partition mappings.
[[nodiscard]] std::unordered_map<std::string, std::vector<std::string>>
run_assignment_strategy(
    const std::string& strategy_name,
    const std::vector<MemberSubscription>& subscriptions,
    const std::vector<std::string>& member_ids,
    const std::unordered_map<std::string, std::vector<std::string>>& previous_assignments) {

    AssignmentStrategy strat = AssignmentStrategy::Range;

    if (strategy_name.find("roundrobin") != std::string::npos ||
        strategy_name.find("round_robin") != std::string::npos) {
        strat = AssignmentStrategy::RoundRobin;
    } else if (strategy_name.find("sticky") != std::string::npos) {
        strat = AssignmentStrategy::Sticky;
    }
    // default: Range

    switch (strat) {
    case AssignmentStrategy::Range:
        return assign_range(subscriptions, member_ids);
    case AssignmentStrategy::RoundRobin:
        return assign_round_robin(subscriptions, member_ids);
    case AssignmentStrategy::Sticky:
        return assign_sticky(subscriptions, member_ids, previous_assignments);
    default:
        return assign_range(subscriptions, member_ids);
    }
}

// ============================================================================
// RESPONSE SERIALIZATION
// ============================================================================

// --------------------------------------------------------------------------
// JoinGroupResponse serialization
// --------------------------------------------------------------------------
// Wire format:
//   INT32 throttle_time_ms
//   INT16 error_code
//   INT32 generation_id
//   STRING protocol_name
//   STRING leader_id
//   STRING member_id
//   INT32 member_count
//   per-member:
//     STRING member_id
//     NULLABLE_STRING group_instance_id
//     STRING protocol_metadata (serialised as raw string)
//
[[nodiscard]] shared_buffer serialize_join_group_response(
    const tp::JoinGroupResponse& resp,
    const std::vector<MemberRecord>& members) {

    std::vector<uint8_t> buf;
    buf.reserve(1024);

    // throttle_time_ms
    write_int32(buf, 0);

    // error_code
    write_error_code(buf, resp.error_code);

    // generation_id
    write_int32(buf, resp.generation_id);

    // protocol_name (STRING)
    write_string(buf, resp.protocol_name);

    // leader_id (STRING)
    write_string(buf, resp.leader_id);

    // member_id — assigned to this specific member
    write_string(buf, resp.member_id);

    // member array
    write_int32(buf, static_cast<int32_t>(members.size()));

    for (const auto& m : members) {
        write_string(buf, m.member_id);
        write_nullable_string(buf, m.group_instance_id);

        // Serialize protocol metadata as concatenated strings
        std::string meta_str;
        for (size_t i = 0; i < m.supported_protocols.size() && i < m.protocol_metadata.size(); ++i) {
            if (!meta_str.empty()) meta_str += ",";
            meta_str += m.supported_protocols[i] + ":";
            meta_str.append(reinterpret_cast<const char*>(m.protocol_metadata[i].data()),
                            m.protocol_metadata[i].size());
        }
        write_string(buf, meta_str);
    }

    return to_shared_buffer(std::move(buf));
}

// --------------------------------------------------------------------------
// HeartbeatResponse serialization
// --------------------------------------------------------------------------
// Wire format:
//   INT32 throttle_time_ms
//   INT16 error_code
//
[[nodiscard]] shared_buffer serialize_heartbeat_response(const tp::HeartbeatResponse& resp) {
    std::vector<uint8_t> buf;
    buf.reserve(8);

    write_int32(buf, 0); // throttle_time_ms
    write_error_code(buf, resp.error_code);

    return to_shared_buffer(std::move(buf));
}

// --------------------------------------------------------------------------
// LeaveGroupResponse serialization
// --------------------------------------------------------------------------
// Wire format:
//   INT32 throttle_time_ms
//   INT16 error_code
//
[[nodiscard]] shared_buffer serialize_leave_group_response(const tp::LeaveGroupResponse& resp) {
    std::vector<uint8_t> buf;
    buf.reserve(8);

    write_int32(buf, 0); // throttle_time_ms
    write_error_code(buf, resp.error_code);

    return to_shared_buffer(std::move(buf));
}

// --------------------------------------------------------------------------
// SyncGroupResponse serialization
// --------------------------------------------------------------------------
// Wire format:
//   INT32 throttle_time_ms
//   INT16 error_code
//   STRING protocol_type
//   STRING protocol_name
//   BYTES assignment
//
[[nodiscard]] shared_buffer serialize_sync_group_response(const tp::SyncGroupResponse& resp) {
    std::vector<uint8_t> buf;
    buf.reserve(256);

    write_int32(buf, 0); // throttle_time_ms
    write_error_code(buf, resp.error_code);
    write_string(buf, resp.protocol_type);
    write_string(buf, resp.protocol_name);
    write_bytes(buf, resp.assignment);

    return to_shared_buffer(std::move(buf));
}

// --------------------------------------------------------------------------
// DescribeGroupsResponse serialization
// --------------------------------------------------------------------------
// Wire format:
//   INT32 throttle_time_ms
//   INT32 group_count
//   per-group:
//     INT16 error_code
//     STRING group_id
//     STRING state
//     STRING protocol_type
//     STRING protocol
//     INT32 member_count
//     per-member:
//       STRING member_id
//       STRING client_id
//       STRING client_host
//       BYTES  member_metadata
//       BYTES  member_assignment
//
[[nodiscard]] shared_buffer serialize_describe_groups_response(
    const std::vector<std::tuple<tp::ErrorCode, GroupRecord*>>& groups) {

    std::vector<uint8_t> buf;
    buf.reserve(4096);

    write_int32(buf, 0); // throttle_time_ms
    write_int32(buf, static_cast<int32_t>(groups.size()));

    for (const auto& [ec, rec] : groups) {
        write_error_code(buf, ec);

        if (rec != nullptr) {
            write_string(buf, rec->group_id);
            write_string(buf, group_state_name(rec->state));
            write_string(buf, rec->protocol_type.empty() ? kDefaultProtocolType : rec->protocol_type);
            write_string(buf, rec->elected_protocol);

            write_int32(buf, static_cast<int32_t>(rec->members.size()));

            for (const auto& [mid, member] : rec->members) {
                write_string(buf, member.member_id);
                write_string(buf, member.client_id);
                write_string(buf, member.client_host);

                // member_metadata: first protocol's metadata
                if (!member.protocol_metadata.empty()) {
                    write_bytes(buf, member.protocol_metadata[0]);
                } else {
                    write_int32(buf, 0);
                }

                write_bytes(buf, member.assignment);
            }
        } else {
            write_string(buf, ""); // group_id not found
            write_string(buf, "");
            write_string(buf, "");
            write_string(buf, "");
            write_int32(buf, 0); // no members
        }
    }

    return to_shared_buffer(std::move(buf));
}

// --------------------------------------------------------------------------
// ListGroupsResponse serialization
// --------------------------------------------------------------------------
// Wire format:
//   INT32 throttle_time_ms
//   INT16 error_code
//   INT32 group_count
//   per-group:
//     STRING group_id
//     STRING protocol_type
//     STRING state
//
[[nodiscard]] shared_buffer serialize_list_groups_response(
    tp::ErrorCode ec,
    const std::vector<std::tuple<std::string, std::string, std::string>>& groups) {

    std::vector<uint8_t> buf;
    buf.reserve(1024);

    write_int32(buf, 0); // throttle_time_ms
    write_error_code(buf, ec);
    write_int32(buf, static_cast<int32_t>(groups.size()));

    for (const auto& [group_id, protocol_type, state] : groups) {
        write_string(buf, group_id);
        write_string(buf, protocol_type);
        write_string(buf, state);
    }

    return to_shared_buffer(std::move(buf));
}

// --------------------------------------------------------------------------
// DeleteGroupsResponse serialization
// --------------------------------------------------------------------------
// Wire format:
//   INT32 throttle_time_ms
//   INT32 result_count
//   per-result:
//     STRING group_id
//     INT16 error_code
//
[[nodiscard]] shared_buffer serialize_delete_groups_response(
    const std::vector<std::pair<std::string, tp::ErrorCode>>& results) {

    std::vector<uint8_t> buf;
    buf.reserve(512);

    write_int32(buf, 0); // throttle_time_ms
    write_int32(buf, static_cast<int32_t>(results.size()));

    for (const auto& [group_id, ec] : results) {
        write_string(buf, group_id);
        write_error_code(buf, ec);
    }

    return to_shared_buffer(std::move(buf));
}

// ============================================================================
// REQUEST PARSING
// ============================================================================

// --------------------------------------------------------------------------
// Parse JoinGroupRequest from wire format
// --------------------------------------------------------------------------
// Wire format (Kafka JoinGroupRequest):
//   STRING group_id
//   INT32  session_timeout_ms
//   INT32  rebalance_timeout_ms
//   STRING member_id (empty = new member)
//   NULLABLE_STRING group_instance_id
//   STRING protocol_type
//   INT32  protocol_count
//   per-protocol:
//     STRING protocol_name
//     BYTES  protocol_metadata
//
[[nodiscard]] tp::JoinGroupRequest parse_join_group_request(buffer_view body) {
    tp::JoinGroupRequest req;
    size_t pos = 0;
    const char* data = body.data;
    size_t size = body.size;

    req.group_id = read_string(data, size, pos);
    req.session_timeout_ms = read_int32(data, size, pos);
    req.rebalance_timeout_ms = read_int32(data, size, pos);
    req.member_id = read_string(data, size, pos);
    req.group_instance_id = read_nullable_string(data, size, pos);
    req.protocol_type = read_string(data, size, pos);

    int32_t protocol_count = read_int32(data, size, pos);
    for (int32_t i = 0; i < protocol_count && pos < size; ++i) {
        req.protocols.push_back(read_string(data, size, pos));
        req.protocol_metadata.push_back(read_bytes(data, size, pos));
    }

    GRP_LOG(debug, "Parsed JoinGroup: group={} member={} timeout={}ms protocols={}",
            req.group_id, req.member_id, req.session_timeout_ms, req.protocols.size());

    return req;
}

// --------------------------------------------------------------------------
// Parse HeartbeatRequest from wire format
// --------------------------------------------------------------------------
// Wire format:
//   STRING group_id
//   INT32  generation_id
//   STRING member_id
//   NULLABLE_STRING group_instance_id
//
[[nodiscard]] tp::HeartbeatRequest parse_heartbeat_request(buffer_view body) {
    tp::HeartbeatRequest req;
    size_t pos = 0;
    const char* data = body.data;
    size_t size = body.size;

    req.group_id = read_string(data, size, pos);
    req.generation_id = read_int32(data, size, pos);
    req.member_id = read_string(data, size, pos);
    req.group_instance_id = read_nullable_string(data, size, pos);

    return req;
}

// --------------------------------------------------------------------------
// Parse LeaveGroupRequest from wire format
// --------------------------------------------------------------------------
// Wire format:
//   STRING group_id
//   STRING member_id
//   INT32  member_count (members to remove)
//   per-member:
//     STRING member_id
//
[[nodiscard]] tp::LeaveGroupRequest parse_leave_group_request(buffer_view body) {
    tp::LeaveGroupRequest req;
    size_t pos = 0;
    const char* data = body.data;
    size_t size = body.size;

    req.group_id = read_string(data, size, pos);
    req.member_id = read_string(data, size, pos);

    int32_t member_count = read_int32(data, size, pos);
    for (int32_t i = 0; i < member_count && pos < size; ++i) {
        req.members.push_back(read_string(data, size, pos));
    }

    GRP_LOG(debug, "Parsed LeaveGroup: group={} member={} removing={}",
            req.group_id, req.member_id, req.members.size());

    return req;
}

// --------------------------------------------------------------------------
// Parse SyncGroupRequest from wire format
// --------------------------------------------------------------------------
// Wire format:
//   STRING group_id
//   INT32  generation_id
//   STRING member_id
//   NULLABLE_STRING group_instance_id
//   STRING protocol_type
//   STRING protocol_name
//   INT32  assignment_count
//   per-assignment:
//     STRING member_id (for whom this assignment is)
//     BYTES  assignment_data
//
[[nodiscard]] tp::SyncGroupRequest parse_sync_group_request(buffer_view body) {
    tp::SyncGroupRequest req;
    size_t pos = 0;
    const char* data = body.data;
    size_t size = body.size;

    req.group_id = read_string(data, size, pos);
    req.generation_id = read_int32(data, size, pos);
    req.member_id = read_string(data, size, pos);
    req.group_instance_id = read_nullable_string(data, size, pos);
    req.protocol_type = read_string(data, size, pos);
    req.protocol_name = read_string(data, size, pos);

    int32_t assignment_count = read_int32(data, size, pos);
    for (int32_t i = 0; i < assignment_count && pos < size; ++i) {
        // Each assignment is a member_id + bytes
        // We'll store member_id in the first entry, then bytes
        // The protocol defines assignments as vector<ByteBuffer> but we
        // encode it as: per-member fields
        read_string(data, size, pos); // skip member_id for each assignment entry
        req.assignments.push_back(read_bytes(data, size, pos));
    }

    GRP_LOG(debug, "Parsed SyncGroup: group={} gen={} member={} leader_assignments={}",
            req.group_id, req.generation_id, req.member_id, req.assignments.size());

    return req;
}

// --------------------------------------------------------------------------
// Parse DescribeGroupsRequest from wire format
// --------------------------------------------------------------------------
// Wire format:
//   INT32 group_count
//   per-group:
//     STRING group_id
//   BOOL  include_authorized_operations
//
[[nodiscard]] tp::DescribeGroupsRequest parse_describe_groups_request(buffer_view body) {
    tp::DescribeGroupsRequest req;
    size_t pos = 0;
    const char* data = body.data;
    size_t size = body.size;

    int32_t group_count = read_int32(data, size, pos);
    for (int32_t i = 0; i < group_count && pos < size; ++i) {
        req.group_ids.push_back(read_string(data, size, pos));
    }
    req.include_authorized_operations = (read_int8(data, size, pos) != 0);

    return req;
}

// --------------------------------------------------------------------------
// Parse ListGroupsRequest from wire format
// --------------------------------------------------------------------------
// Wire format:
//   INT32 states_filter_count
//   per-filter:
//     STRING state_name
//
[[nodiscard]] tp::ListGroupsRequest parse_list_groups_request(buffer_view body) {
    tp::ListGroupsRequest req;
    size_t pos = 0;
    const char* data = body.data;
    size_t size = body.size;

    int32_t filter_count = read_int32(data, size, pos);
    for (int32_t i = 0; i < filter_count && pos < size; ++i) {
        req.states_filter.push_back(read_string(data, size, pos));
    }

    return req;
}

// --------------------------------------------------------------------------
// Parse DeleteGroupsRequest from wire format
// --------------------------------------------------------------------------
// Wire format:
//   INT32 group_count
//   per-group:
//     STRING group_id
//
[[nodiscard]] tp::DeleteGroupsRequest parse_delete_groups_request(buffer_view body) {
    tp::DeleteGroupsRequest req;
    size_t pos = 0;
    const char* data = body.data;
    size_t size = body.size;

    int32_t group_count = read_int32(data, size, pos);
    for (int32_t i = 0; i < group_count && pos < size; ++i) {
        req.group_ids.push_back(read_string(data, size, pos));
    }

    return req;
}

// ============================================================================
// HANDLER IMPLEMENTATIONS
// ============================================================================

// --------------------------------------------------------------------------
// handle_join — process JoinGroup request (api_key=11)
// --------------------------------------------------------------------------
//
// State transitions:
//   Empty:
//     - First member joins → assign as leader, become Stable immediately
//       (no rebalance needed for single member)
//   Stable:
//     - Known member rejoins → return current generation info
//     - New member joins → trigger PreparingRebalance, bump generation
//   PreparingRebalance:
//     - Known member rejoins → return current state (still rebalancing)
//     - New member joins → include them in rebalance
//
shared_buffer handle_join_impl(const RequestContext& ctx, buffer_view body) {
    auto req = parse_join_group_request(body);

    // Validate group_id
    if (req.group_id.empty()) {
        tp::JoinGroupResponse resp;
        resp.error_code = tp::ErrorCode::kInvalidGroupId;
        resp.member_id = "";
        return serialize_join_group_response(resp, {});
    }

    // Validate session timeout
    if (req.session_timeout_ms < kMinSessionTimeoutMs ||
        req.session_timeout_ms > kMaxSessionTimeoutMs) {
        tp::JoinGroupResponse resp;
        resp.error_code = tp::ErrorCode::kInvalidSessionTimeout;
        resp.member_id = "";
        return serialize_join_group_response(resp, {});
    }

    // Clamp rebalance timeout
    if (req.rebalance_timeout_ms <= 0) {
        req.rebalance_timeout_ms = kDefaultRebalanceTimeoutMs;
    }
    if (req.rebalance_timeout_ms > kMaxSessionTimeoutMs) {
        req.rebalance_timeout_ms = kMaxSessionTimeoutMs;
    }

    // Default protocol type
    if (req.protocol_type.empty()) {
        req.protocol_type = kDefaultProtocolType;
    }

    // Validate protocol count
    if (req.protocols.size() > kMaxProtocolsPerMember) {
        tp::JoinGroupResponse resp;
        resp.error_code = tp::ErrorCode::kInvalidRequest;
        resp.member_id = "";
        return serialize_join_group_response(resp, {});
    }

    auto& gsm = group_state_manager();
    auto [rec, lock] = gsm.get_or_create(req.group_id);

    if (rec == nullptr) {
        tp::JoinGroupResponse resp;
        resp.error_code = tp::ErrorCode::kUnknownServerError;
        resp.member_id = "";
        return serialize_join_group_response(resp, {});
    }

    int64_t now_ms = wall_clock_ms();

    // Expire dead members before processing join
    rec->expire_members(now_ms);
    if (rec->members.empty() && rec->state == GroupState::Stable) {
        rec->state = GroupState::Empty;
    }

    // Determine member_id: use existing if provided and matches a known member
    std::string member_id = req.member_id;
    bool is_rejoin = false;

    if (!member_id.empty() && rec->members.find(member_id) != rec->members.end()) {
        is_rejoin = true;
    } else if (member_id.empty()) {
        member_id = generate_member_id(ctx.client_id, req.group_id);
    }

    // Enforce max members
    if (!is_rejoin && rec->members.size() >= static_cast<size_t>(kMaxMembersPerGroup)) {
        tp::JoinGroupResponse resp;
        resp.error_code = tp::ErrorCode::kUnknownServerError;
        resp.member_id = member_id;
        return serialize_join_group_response(resp, {});
    }

    switch (rec->state) {
    // =========================================================================
    case GroupState::Empty: {
        // First member: create group, become leader, go directly to Stable
        MemberRecord new_member;
        new_member.member_id       = member_id;
        new_member.group_instance_id = req.group_instance_id;
        new_member.client_id       = ctx.client_id;
        new_member.client_host     = ctx.client_host;
        new_member.protocol_type   = req.protocol_type;
        new_member.supported_protocols = req.protocols;
        new_member.protocol_metadata   = req.protocol_metadata;
        new_member.last_heartbeat_ms   = now_ms;
        new_member.session_timeout_ms  = req.session_timeout_ms;
        new_member.join_time_ms        = now_ms;
        new_member.awaiting_sync       = true;
        new_member.is_leader           = true;

        rec->members[member_id] = std::move(new_member);
        rec->protocol_type      = req.protocol_type;
        rec->generation_id      = 1; // Start at generation 1
        rec->leader_id          = member_id;
        rec->elected_protocol   = req.protocols.empty() ? "" : req.protocols[0];
        rec->state              = GroupState::Stable; // single member = stable immediately
        rec->synced_members.clear();
        rec->pending_assignments.clear();
        rec->last_state_change_ms = now_ms;

        GRP_LOG(info, "Group {}: first member {} joined, state=Stable gen=1 leader={}",
                req.group_id, member_id, member_id);

        tp::JoinGroupResponse resp;
        resp.error_code    = tp::ErrorCode::kNone;
        resp.generation_id = rec->generation_id;
        resp.protocol_name = rec->elected_protocol;
        resp.leader_id     = rec->leader_id;
        resp.member_id     = member_id;

        std::vector<MemberRecord> members_list;
        for (const auto& [id, m] : rec->members) {
            members_list.push_back(m);
        }

        return serialize_join_group_response(resp, members_list);
    }

    // =========================================================================
    case GroupState::Stable: {
        if (is_rejoin) {
            // Known member rejoining — update their info
            auto& existing = rec->members[member_id];
            existing.last_heartbeat_ms   = now_ms;
            existing.session_timeout_ms  = req.session_timeout_ms;
            existing.supported_protocols = req.protocols;
            existing.protocol_metadata   = req.protocol_metadata;
            existing.awaiting_sync       = false; // already has assignment from prior gen
            existing.group_instance_id   = req.group_instance_id;

            GRP_LOG(debug, "Group {}: member {} rejoined (Stable, gen={})",
                    req.group_id, member_id, rec->generation_id);

            tp::JoinGroupResponse resp;
            resp.error_code    = tp::ErrorCode::kNone;
            resp.generation_id = rec->generation_id;
            resp.protocol_name = rec->elected_protocol;
            resp.leader_id     = rec->leader_id;
            resp.member_id     = member_id;

            std::vector<MemberRecord> members_list;
            for (const auto& [id, m] : rec->members) {
                members_list.push_back(m);
            }

            return serialize_join_group_response(resp, members_list);
        }

        // New member joining a stable group → trigger rebalance
        MemberRecord new_member;
        new_member.member_id       = member_id;
        new_member.group_instance_id = req.group_instance_id;
        new_member.client_id       = ctx.client_id;
        new_member.client_host     = ctx.client_host;
        new_member.protocol_type   = req.protocol_type;
        new_member.supported_protocols = req.protocols;
        new_member.protocol_metadata   = req.protocol_metadata;
        new_member.last_heartbeat_ms   = now_ms;
        new_member.session_timeout_ms  = req.session_timeout_ms;
        new_member.join_time_ms        = now_ms;
        new_member.awaiting_sync       = true;
        new_member.is_leader           = false; // existing leader stays

        rec->members[member_id] = std::move(new_member);

        // Trigger rebalance
        rec->state                = GroupState::PreparingRebalance;
        rec->generation_id       += 1;
        rec->rebalance_timeout_ms = req.rebalance_timeout_ms;
        rec->rebalance_deadline_ms = now_ms + req.rebalance_timeout_ms;
        rec->synced_members.clear();
        rec->pending_assignments.clear();
        rec->last_state_change_ms = now_ms;

        // Re-elect protocol
        rec->elected_protocol = elect_protocol(rec->members);

        GRP_LOG(info, "Group {}: new member {} triggered rebalance, state=PreparingRebalance gen={}",
                req.group_id, member_id, rec->generation_id);

        tp::JoinGroupResponse resp;
        resp.error_code    = tp::ErrorCode::kRebalanceInProgress;
        resp.generation_id = rec->generation_id;
        resp.protocol_name = rec->elected_protocol;
        resp.leader_id     = rec->leader_id;
        resp.member_id     = member_id;

        std::vector<MemberRecord> members_list;
        for (const auto& [id, m] : rec->members) {
            members_list.push_back(m);
        }

        return serialize_join_group_response(resp, members_list);
    }

    // =========================================================================
    case GroupState::PreparingRebalance: {
        if (is_rejoin) {
            // Member rejoining during rebalance — update and return current state
            auto& existing = rec->members[member_id];
            existing.last_heartbeat_ms   = now_ms;
            existing.session_timeout_ms  = req.session_timeout_ms;
            existing.supported_protocols = req.protocols;
            existing.protocol_metadata   = req.protocol_metadata;
            existing.awaiting_sync       = true;
            existing.group_instance_id   = req.group_instance_id;

            // Refresh rebalance deadline if the new timeout is larger
            int64_t new_deadline = now_ms + req.rebalance_timeout_ms;
            if (new_deadline > rec->rebalance_deadline_ms) {
                rec->rebalance_deadline_ms = new_deadline;
                rec->rebalance_timeout_ms  = req.rebalance_timeout_ms;
            }

            GRP_LOG(debug, "Group {}: member {} rejoined during rebalance (gen={})",
                    req.group_id, member_id, rec->generation_id);

            tp::JoinGroupResponse resp;
            resp.error_code    = tp::ErrorCode::kRebalanceInProgress;
            resp.generation_id = rec->generation_id;
            resp.protocol_name = rec->elected_protocol;
            resp.leader_id     = rec->leader_id;
            resp.member_id     = member_id;

            std::vector<MemberRecord> members_list;
            for (const auto& [id, m] : rec->members) {
                members_list.push_back(m);
            }

            return serialize_join_group_response(resp, members_list);
        }

        // New member joining during rebalance — add to the rebalance
        MemberRecord new_member;
        new_member.member_id       = member_id;
        new_member.group_instance_id = req.group_instance_id;
        new_member.client_id       = ctx.client_id;
        new_member.client_host     = ctx.client_host;
        new_member.protocol_type   = req.protocol_type;
        new_member.supported_protocols = req.protocols;
        new_member.protocol_metadata   = req.protocol_metadata;
        new_member.last_heartbeat_ms   = now_ms;
        new_member.session_timeout_ms  = req.session_timeout_ms;
        new_member.join_time_ms        = now_ms;
        new_member.awaiting_sync       = true;
        new_member.is_leader           = false;

        rec->members[member_id] = std::move(new_member);

        // Re-elect protocol with new member set
        rec->elected_protocol = elect_protocol(rec->members);

        // Extend deadline if needed
        int64_t new_deadline = now_ms + req.rebalance_timeout_ms;
        if (new_deadline > rec->rebalance_deadline_ms) {
            rec->rebalance_deadline_ms = new_deadline;
            rec->rebalance_timeout_ms  = req.rebalance_timeout_ms;
        }

        GRP_LOG(info, "Group {}: new member {} joined during rebalance, gen={} total_members={}",
                req.group_id, member_id, rec->generation_id, rec->members.size());

        tp::JoinGroupResponse resp;
        resp.error_code    = tp::ErrorCode::kRebalanceInProgress;
        resp.generation_id = rec->generation_id;
        resp.protocol_name = rec->elected_protocol;
        resp.leader_id     = rec->leader_id;
        resp.member_id     = member_id;

        std::vector<MemberRecord> members_list;
        for (const auto& [id, m] : rec->members) {
            members_list.push_back(m);
        }

        return serialize_join_group_response(resp, members_list);
    }

    // =========================================================================
    case GroupState::Dead:
    default: {
        // Cannot join a dead/deleted group
        tp::JoinGroupResponse resp;
        resp.error_code    = tp::ErrorCode::kUnknownGroupId;
        resp.member_id     = member_id;
        return serialize_join_group_response(resp, {});
    }
    }
}

// --------------------------------------------------------------------------
// handle_heartbeat — process Heartbeat request (api_key=12)
// --------------------------------------------------------------------------
//
// Validates generation id against current group generation.
// Updates the member's last_heartbeat timestamp.
// Returns REBALANCE_IN_PROGRESS if the group is rebalancing.
// Returns UNKNOWN_MEMBER_ID if the member is not found.
//
shared_buffer handle_heartbeat_impl(const RequestContext& ctx, buffer_view body) {
    (void)ctx; // used implicitly via ctx.client_id/host but not in heartbeat path
    auto req = parse_heartbeat_request(body);
    int64_t now_ms = wall_clock_ms();

    auto& gsm = group_state_manager();
    auto [rec, lock] = gsm.get(req.group_id);

    tp::HeartbeatResponse resp;

    if (rec == nullptr) {
        resp.error_code = tp::ErrorCode::kUnknownGroupId;
        GRP_LOG(warn, "Heartbeat for unknown group {}", req.group_id);
        return serialize_heartbeat_response(resp);
    }

    // Check if member exists
    auto member_it = rec->members.find(req.member_id);
    if (member_it == rec->members.end()) {
        resp.error_code = tp::ErrorCode::kUnknownMemberId;
        GRP_LOG(warn, "Heartbeat from unknown member {} in group {}",
                req.member_id, req.group_id);
        return serialize_heartbeat_response(resp);
    }

    // Validate generation
    tp::ErrorCode gen_ec = rec->validate_generation(req.generation_id);
    if (gen_ec != tp::ErrorCode::kNone) {
        resp.error_code = gen_ec;
        GRP_LOG(debug, "Heartbeat generation mismatch: member={} req_gen={} cur_gen={}",
                req.member_id, req.generation_id, rec->generation_id);
        return serialize_heartbeat_response(resp);
    }

    // Update heartbeat timestamp
    member_it->second.last_heartbeat_ms = now_ms;

    // If the group is rebalancing, tell the member
    if (rec->state == GroupState::PreparingRebalance) {
        resp.error_code = tp::ErrorCode::kRebalanceInProgress;

        // Check rebalance timeout
        if (now_ms > rec->rebalance_deadline_ms && rec->rebalance_deadline_ms > 0) {
            // Rebalance timed out: expire members that haven't rejoined
            int32_t expired = rec->expire_members(now_ms);
            if (expired > 0) {
                GRP_LOG(warn, "Rebalance timeout for group {}: {} members expired",
                        req.group_id, expired);
            }
            // If all remaining have joined or no members, complete rebalance
            if (rec->members.empty()) {
                rec->state = GroupState::Empty;
                rec->last_state_change_ms = now_ms;
                resp.error_code = tp::ErrorCode::kUnknownMemberId;
            }
        }
    } else {
        resp.error_code = tp::ErrorCode::kNone;
    }

    return serialize_heartbeat_response(resp);
}

// --------------------------------------------------------------------------
// handle_leave — process LeaveGroup request (api_key=13)
// --------------------------------------------------------------------------
//
// Removes one or more members from a group.
// If the leader leaves and other members remain, triggers a rebalance.
// If all members leave, group transitions to Empty.
//
shared_buffer handle_leave_impl(const RequestContext& ctx, buffer_view body) {
    (void)ctx;
    auto req = parse_leave_group_request(body);
    int64_t now_ms = wall_clock_ms();

    auto& gsm = group_state_manager();
    auto [rec, lock] = gsm.get(req.group_id);

    tp::LeaveGroupResponse resp;

    if (rec == nullptr) {
        resp.error_code = tp::ErrorCode::kUnknownGroupId;
        GRP_LOG(warn, "LeaveGroup for unknown group {}", req.group_id);
        return serialize_leave_group_response(resp);
    }

    // Determine which members to remove
    std::vector<std::string> members_to_remove;

    if (!req.members.empty()) {
        // Remove specific members
        members_to_remove = req.members;
    } else if (!req.member_id.empty()) {
        // Remove the single requesting member
        members_to_remove.push_back(req.member_id);
    } else {
        resp.error_code = tp::ErrorCode::kInvalidRequest;
        return serialize_leave_group_response(resp);
    }

    bool leader_removed = false;
    int32_t removed_count = 0;

    for (const auto& mid : members_to_remove) {
        auto it = rec->members.find(mid);
        if (it != rec->members.end()) {
            if (it->second.is_leader) {
                leader_removed = true;
            }
            rec->synced_members.erase(mid);
            rec->pending_assignments.erase(mid);
            rec->members.erase(it);
            ++removed_count;
            GRP_LOG(info, "Group {}: member {} left", req.group_id, mid);
        }
    }

    if (removed_count == 0) {
        resp.error_code = tp::ErrorCode::kUnknownMemberId;
        return serialize_leave_group_response(resp);
    }

    rec->last_state_change_ms = now_ms;

    if (rec->members.empty()) {
        // Group is now empty
        rec->state = GroupState::Empty;
        rec->leader_id.clear();
        rec->synced_members.clear();
        rec->pending_assignments.clear();
        rec->generation_id += 1;
        GRP_LOG(info, "Group {}: all members left, state=Empty", req.group_id);
        resp.error_code = tp::ErrorCode::kNone;
    } else if (leader_removed) {
        // Leader left → elect new leader and trigger rebalance
        rec->leader_id = rec->members.begin()->first;
        rec->members[rec->leader_id].is_leader = true;
        rec->state = GroupState::PreparingRebalance;
        rec->generation_id += 1;
        rec->rebalance_timeout_ms = kDefaultRebalanceTimeoutMs;
        rec->rebalance_deadline_ms = now_ms + kDefaultRebalanceTimeoutMs;
        rec->synced_members.clear();
        rec->pending_assignments.clear();
        rec->elected_protocol = elect_protocol(rec->members);
        GRP_LOG(info, "Group {}: leader left, new leader={}, rebalancing gen={}",
                req.group_id, rec->leader_id, rec->generation_id);
        resp.error_code = tp::ErrorCode::kNone;
    } else {
        // Non-leader left: update assignments but stay stable
        // Trigger a lightweight rebalance to redistribute partitions
        rec->state = GroupState::PreparingRebalance;
        rec->generation_id += 1;
        rec->rebalance_timeout_ms = kDefaultRebalanceTimeoutMs;
        rec->rebalance_deadline_ms = now_ms + kDefaultRebalanceTimeoutMs;
        rec->synced_members.clear();
        rec->pending_assignments.clear();
        GRP_LOG(info, "Group {}: member left, rebalancing gen={}",
                req.group_id, rec->generation_id);
        resp.error_code = tp::ErrorCode::kNone;
    }

    return serialize_leave_group_response(resp);
}

// --------------------------------------------------------------------------
// handle_sync — process SyncGroup request (api_key=14)
// --------------------------------------------------------------------------
//
// Leader path:
//   1. Collect all member assignments (including from this SyncGroup call).
//   2. Parse subscriptions from member metadata.
//   3. Run the assignment strategy (Range / RoundRobin / Sticky).
//   4. Distribute assignments to all members.
//   5. Transition group to Stable if all members synced.
//
// Follower path:
//   1. Send own assignment to leader via the pending_assignments map.
//   2. Wait for leader's assignment (return empty if leader hasn't finished).
//
shared_buffer handle_sync_impl(const RequestContext& ctx, buffer_view body) {
    (void)ctx;
    auto req = parse_sync_group_request(body);
    int64_t now_ms = wall_clock_ms();

    auto& gsm = group_state_manager();
    auto [rec, lock] = gsm.get(req.group_id);

    tp::SyncGroupResponse resp;
    resp.protocol_type = req.protocol_type;
    resp.protocol_name = req.protocol_name;

    if (rec == nullptr) {
        resp.error_code = tp::ErrorCode::kUnknownGroupId;
        return serialize_sync_group_response(resp);
    }

    // Validate generation
    tp::ErrorCode gen_ec = rec->validate_generation(req.generation_id);
    if (gen_ec != tp::ErrorCode::kNone) {
        resp.error_code = gen_ec;
        return serialize_sync_group_response(resp);
    }

    // Check member exists
    auto member_it = rec->members.find(req.member_id);
    if (member_it == rec->members.end()) {
        resp.error_code = tp::ErrorCode::kUnknownMemberId;
        return serialize_sync_group_response(resp);
    }

    bool is_leader = (rec->leader_id == req.member_id);

    // Mark this member as synced
    rec->synced_members.insert(req.member_id);

    // Store this member's assignments (for leader to consume)
    if (!req.assignments.empty()) {
        // The assignments array carries per-member assignment data
        // First ByteBuffer is the member's own assignment data
        rec->pending_assignments[req.member_id] = req.assignments[0];
    }

    if (is_leader) {
        // =====================================================================
        // LEADER PATH: run assignment strategy
        // =====================================================================

        // Check if all members have synced (sent their SyncGroup requests)
        if (!rec->all_members_synced()) {
            // Not everyone is ready yet; leader returns without assignment
            // Members should retry
            resp.error_code = tp::ErrorCode::kRebalanceInProgress;
            GRP_LOG(debug, "Group {}: leader {} waiting for all members to sync ({}/{})",
                    req.group_id, req.member_id,
                    rec->synced_members.size(), rec->members.size());
            return serialize_sync_group_response(resp);
        }

        // All members have synced — run assignment strategy
        GRP_LOG(info, "Group {}: leader {} running assignment strategy for {} members",
                req.group_id, req.member_id, rec->members.size());

        // Collect subscriptions from all members
        std::vector<MemberSubscription> subscriptions;
        std::vector<std::string> member_ids;

        for (const auto& [mid, member] : rec->members) {
            member_ids.push_back(mid);
            auto sub = parse_subscription(member);
            subscriptions.push_back(std::move(sub));
        }

        // Determine strategy from elected protocol name
        std::string strategy_name = rec->elected_protocol;
        // Default to Range if no strategy hints in protocol name
        if (strategy_name.empty()) {
            strategy_name = "range";
        }

        // Run the strategy
        auto new_assignments = run_assignment_strategy(
            strategy_name, subscriptions, member_ids,
            rec->sticky_assignments);

        // Store sticky assignments for future reference
        rec->sticky_assignments = new_assignments;

        // Distribute assignments to members (encode as byte buffers)
        for (auto& [mid, member] : rec->members) {
            auto& partitions = new_assignments[mid];

            // Serialize partition list as a simple text blob
            std::string assignment_text;
            for (size_t i = 0; i < partitions.size(); ++i) {
                if (i > 0) assignment_text += ",";
                assignment_text += partitions[i];
            }

            member.assignment.assign(
                reinterpret_cast<const uint8_t*>(assignment_text.data()),
                reinterpret_cast<const uint8_t*>(assignment_text.data()) + assignment_text.size());

            member.awaiting_sync = false;
        }

        // Transition to Stable
        rec->state = GroupState::Stable;
        rec->last_state_change_ms = now_ms;
        rec->synced_members.clear();

        GRP_LOG(info, "Group {}: sync complete, {} members assigned, state=Stable gen={}",
                req.group_id, rec->members.size(), rec->generation_id);

        // Return leader's own assignment
        resp.error_code = tp::ErrorCode::kNone;
        resp.assignment = rec->members[req.member_id].assignment;
    } else {
        // =====================================================================
        // FOLLOWER PATH: wait for leader to complete assignments
        // =====================================================================

        if (rec->state == GroupState::Stable) {
            // Group is stable — leader has finished; return the member's assignment
            resp.error_code = tp::ErrorCode::kNone;
            resp.assignment = member_it->second.assignment;

            GRP_LOG(debug, "Group {}: follower {} received assignment ({} bytes)",
                    req.group_id, req.member_id, resp.assignment.size());
        } else if (rec->state == GroupState::PreparingRebalance) {
            // Still rebalancing — leader hasn't finished yet
            // Check timeout
            if (now_ms > rec->rebalance_deadline_ms && rec->rebalance_deadline_ms > 0) {
                resp.error_code = tp::ErrorCode::kRebalanceInProgress;
                GRP_LOG(warn, "Group {}: sync rebalance timeout for member {}",
                        req.group_id, req.member_id);
            } else {
                resp.error_code = tp::ErrorCode::kRebalanceInProgress;
            }
        } else {
            resp.error_code = tp::ErrorCode::kUnknownServerError;
        }
    }

    return serialize_sync_group_response(resp);
}

// --------------------------------------------------------------------------
// handle_describe — process DescribeGroups request (api_key=15)
// --------------------------------------------------------------------------
//
// Returns state, protocol, and member details for each requested group.
//
shared_buffer handle_describe_impl(const RequestContext& ctx, buffer_view body) {
    (void)ctx;
    auto req = parse_describe_groups_request(body);

    auto& gsm = group_state_manager();

    std::vector<std::tuple<tp::ErrorCode, GroupRecord*>> results;
    std::vector<std::shared_lock<std::shared_mutex>> locks;

    for (const auto& group_id : req.group_ids) {
        auto [rec, lock] = gsm.get_shared(group_id);
        if (rec != nullptr) {
            results.emplace_back(tp::ErrorCode::kNone, rec);
            locks.push_back(std::move(lock));
        } else {
            results.emplace_back(tp::ErrorCode::kUnknownGroupId, nullptr);
            locks.emplace_back(); // dummy lock
        }
    }

    return serialize_describe_groups_response(results);
}

// --------------------------------------------------------------------------
// handle_list — process ListGroups request (api_key=16)
// --------------------------------------------------------------------------
//
// Returns all active (non-Dead, non-Empty) groups.
// Optionally filters by state.
//
shared_buffer handle_list_impl(const RequestContext& ctx, buffer_view body) {
    (void)ctx;
    auto req = parse_list_groups_request(body);

    auto& gsm = group_state_manager();
    auto groups = gsm.list_groups_with_protocol();

    std::vector<std::tuple<std::string, std::string, std::string>> filtered;

    // If no filter, return all groups
    if (req.states_filter.empty()) {
        for (auto& [group_id, protocol_type] : groups) {
            auto [rec, lock] = gsm.get_shared(group_id);
            if (rec != nullptr && rec->state != GroupState::Dead) {
                filtered.emplace_back(
                    group_id,
                    protocol_type,
                    group_state_name(rec->state));
            }
        }
        return serialize_list_groups_response(tp::ErrorCode::kNone, filtered);
    }

    // Filter by state
    std::unordered_set<std::string> allowed_states(
        req.states_filter.begin(), req.states_filter.end());

    for (auto& [group_id, protocol_type] : groups) {
        auto [rec, lock] = gsm.get_shared(group_id);
        if (rec != nullptr && rec->state != GroupState::Dead) {
            const char* state_name = group_state_name(rec->state);
            if (allowed_states.find(state_name) != allowed_states.end() ||
                allowed_states.empty()) {
                filtered.emplace_back(group_id, protocol_type, state_name);
            }
        }
    }

    return serialize_list_groups_response(tp::ErrorCode::kNone, filtered);
}

// --------------------------------------------------------------------------
// handle_delete — process DeleteGroups request (api_key=42)
// --------------------------------------------------------------------------
//
// Deletes empty groups. Returns per-group error codes.
// A group cannot be deleted if it has active members.
//
shared_buffer handle_delete_impl(const RequestContext& ctx, buffer_view body) {
    (void)ctx;
    auto req = parse_delete_groups_request(body);

    auto& gsm = group_state_manager();
    std::vector<std::pair<std::string, tp::ErrorCode>> results;

    for (const auto& group_id : req.group_ids) {
        tp::ErrorCode ec = gsm.delete_group(group_id);
        results.emplace_back(group_id, ec);

        if (ec == tp::ErrorCode::kNone) {
            GRP_LOG(info, "DeleteGroups: successfully deleted group {}", group_id);
        } else {
            GRP_LOG(warn, "DeleteGroups: failed to delete group {} (error={})",
                    group_id, static_cast<int16_t>(ec));
        }
    }

    return serialize_delete_groups_response(results);
}

} // anonymous namespace

// ============================================================================
// GroupHandler Public API Implementation
// ============================================================================

GroupHandler::GroupHandler(class BrokerServer& s) : server_(&s) {}

shared_buffer GroupHandler::handle_join(const RequestContext& ctx, buffer_view body) {
    try {
        return handle_join_impl(ctx, body);
    } catch (const std::exception& e) {
        GRP_LOG(error, "Exception in handle_join: {}", e.what());
        tp::JoinGroupResponse resp;
        resp.error_code = tp::ErrorCode::kUnknownServerError;
        resp.member_id = "";
        return serialize_join_group_response(resp, {});
    }
}

shared_buffer GroupHandler::handle_sync(const RequestContext& ctx, buffer_view body) {
    try {
        return handle_sync_impl(ctx, body);
    } catch (const std::exception& e) {
        GRP_LOG(error, "Exception in handle_sync: {}", e.what());
        tp::SyncGroupResponse resp;
        resp.error_code = tp::ErrorCode::kUnknownServerError;
        return serialize_sync_group_response(resp);
    }
}

shared_buffer GroupHandler::handle_heartbeat(const RequestContext& ctx, buffer_view body) {
    try {
        return handle_heartbeat_impl(ctx, body);
    } catch (const std::exception& e) {
        GRP_LOG(error, "Exception in handle_heartbeat: {}", e.what());
        tp::HeartbeatResponse resp;
        resp.error_code = tp::ErrorCode::kUnknownServerError;
        return serialize_heartbeat_response(resp);
    }
}

shared_buffer GroupHandler::handle_leave(const RequestContext& ctx, buffer_view body) {
    try {
        return handle_leave_impl(ctx, body);
    } catch (const std::exception& e) {
        GRP_LOG(error, "Exception in handle_leave: {}", e.what());
        tp::LeaveGroupResponse resp;
        resp.error_code = tp::ErrorCode::kUnknownServerError;
        return serialize_leave_group_response(resp);
    }
}

shared_buffer GroupHandler::handle_describe(const RequestContext& ctx, buffer_view body) {
    try {
        return handle_describe_impl(ctx, body);
    } catch (const std::exception& e) {
        GRP_LOG(error, "Exception in handle_describe: {}", e.what());
        // Return empty describe response on error
        std::vector<std::tuple<tp::ErrorCode, GroupRecord*>> empty;
        return serialize_describe_groups_response(empty);
    }
}

shared_buffer GroupHandler::handle_list(const RequestContext& ctx, buffer_view body) {
    try {
        return handle_list_impl(ctx, body);
    } catch (const std::exception& e) {
        GRP_LOG(error, "Exception in handle_list: {}", e.what());
        return serialize_list_groups_response(
            tp::ErrorCode::kUnknownServerError, {});
    }
}

shared_buffer GroupHandler::handle_delete(const RequestContext& ctx, buffer_view body) {
    try {
        return handle_delete_impl(ctx, body);
    } catch (const std::exception& e) {
        GRP_LOG(error, "Exception in handle_delete: {}", e.what());
        std::vector<std::pair<std::string, tp::ErrorCode>> results;
        return serialize_delete_groups_response(results);
    }
}

} // namespace torrent::client
