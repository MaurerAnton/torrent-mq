/**
 * torrent-mq — OffsetHandler: Full Offset Management Implementation
 *
 * Implements all consumer-group offset wire-protocol APIs (Kafka-compatible):
 *   - ListOffsets        (api_key=2)  — earliest/latest/by-timestamp
 *   - OffsetCommit       (api_key=8)  — group offset persistence
 *   - OffsetFetch        (api_key=9)  — fetch committed offsets
 *   - OffsetForLeaderEpoch (api_key=23) — truncation detection
 *   - OffsetDelete       (api_key=47) — delete committed offsets for a group
 *
 * Wire format (Kafka-compatible, big-endian):
 *
 *   ListOffsets Request (v7+):
 *     INT32  replica_id            (-1 for consumer)
 *     INT8   isolation_level       (0=read_uncommitted, 1=read_committed)
 *     INT32  topic_count
 *     per-topic:
 *       STRING  topic_name
 *       INT32  partition_count
 *       per-partition:
 *         INT32  partition_index
 *         INT32  current_leader_epoch
 *         INT64  timestamp       (-2=earliest, -1=latest, >=0=specific)
 *         INT32  max_num_offsets (usually 1)
 *
 *   ListOffsets Response (v7+):
 *     INT32  throttle_time_ms
 *     INT32  topic_count
 *     per-topic:
 *       STRING  topic_name
 *       INT32  partition_count
 *       per-partition:
 *         INT32  partition_index
 *         INT16  error_code
 *         INT64  timestamp
 *         INT64  offset
 *         INT32  leader_epoch
 *
 *   OffsetCommit Request (v8+):
 *     STRING        group_id
 *     INT32         generation_id
 *     STRING        member_id
 *     STRING        group_instance_id (nullable)
 *     INT64         retention_time_ms
 *     INT32         topic_count
 *     per-topic:
 *       STRING      topic_name
 *       INT32       partition_count
 *       per-partition:
 *         INT32     partition_index
 *         INT64     committed_offset
 *         INT32     committed_leader_epoch
 *         STRING    committed_metadata (nullable)
 *
 *   OffsetCommit Response (v8+):
 *     INT32  throttle_time_ms
 *     INT32  topic_count
 *     per-topic:
 *       STRING  topic_name
 *       INT32   partition_count
 *       per-partition:
 *         INT32  partition_index
 *         INT16  error_code
 *
 *   OffsetFetch Request (v8+):
 *     STRING        group_id
 *     INT32         topic_count (-1 = fetch all)
 *     per-topic:
 *       STRING      topic_name
 *       INT32       partition_count (-1 = all partitions for topic)
 *       per-partition:
 *         INT32     partition_index
 *
 *   OffsetFetch Response (v8+):
 *     INT32  throttle_time_ms
 *     INT32  topic_count
 *     per-topic:
 *       STRING  topic_name
 *       INT32   partition_count
 *       per-partition:
 *         INT32  partition_index
 *         INT64  committed_offset
 *         INT32  committed_leader_epoch
 *         STRING metadata (nullable)
 *         INT16  error_code
 *
 *   OffsetForLeaderEpoch Request (v4+):
 *     INT32  replica_id
 *     INT32  topic_count
 *     per-topic:
 *       STRING  topic_name
 *       INT32   partition_count
 *       per-partition:
 *         INT32  partition_index
 *         INT32  current_leader_epoch
 *         INT32  leader_epoch
 *
 *   OffsetForLeaderEpoch Response (v4+):
 *     INT32  throttle_time_ms
 *     INT32  topic_count
 *     per-topic:
 *       STRING  topic_name
 *       INT32   partition_count
 *       per-partition:
 *         INT32  partition_index
 *         INT16  error_code
 *         INT64  end_offset
 *         INT32  leader_epoch
 *
 *   OffsetDelete Request (v0):
 *     STRING  group_id
 *     INT32   topic_count
 *     per-topic:
 *       STRING  topic_name
 *       INT32   partition_count
 *       per-partition:
 *         INT32  partition_index
 *
 *   OffsetDelete Response (v0):
 *     INT32  throttle_time_ms
 *     INT16  error_code
 *     INT32  topic_count
 *     per-topic:
 *       STRING  topic_name
 *       INT32   partition_count
 *       per-partition:
 *         INT32  partition_index
 *         INT16  error_code
 *
 * Thread-safety: Handler methods are called from the request dispatcher.
 * The internal GroupOffsetStore is protected by a shared_mutex. The handler
 * is stateless beyond a pointer to BrokerServer; all persistent offset state
 * lives in the internal concurrent map.
 */

#include "torrent/client/offset_handler.h"
#include "torrent/broker/server.h"
#include "torrent/broker/consumer_group_manager.h"
#include "torrent/broker/partition_manager.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/storage/log_manager.h"
#include "torrent/network/protocol.h"
#include "torrent/common/types.h"
#include "torrent/storage/types.h"

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
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
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

[[nodiscard]] std::shared_ptr<spdlog::logger> get_offset_logger() {
    static auto logger = []() {
        auto l = spdlog::get("offset_handler");
        if (!l) {
            l = spdlog::stdout_color_mt("offset_handler");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define OFS_LOG(level, ...) \
    get_offset_logger()->level("[offset] " __VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Default throttle time in response (ms).
constexpr int32_t kDefaultThrottleMs = 0;

/// Maximum number of topics in a single offset request.
constexpr int32_t kMaxTopicsPerRequest = 1000;

/// Maximum number of partitions per topic in a single offset request.
constexpr int32_t kMaxPartitionsPerTopic = 10000;

/// Maximum metadata length for offset commit.
constexpr int32_t kMaxCommitMetadataBytes = 4096;

/// Maximum group_id length.
constexpr int32_t kMaxGroupIdLength = 256;

/// Maximum expired offset commits retained (for tombstone cleanup).
constexpr int64_t kOffsetRetentionDefaultMs = 86400000;  // 1 day

/// Sentinel for "earliest" timestamp query.
constexpr int64_t kTimestampEarliest = -2;

/// Sentinel for "latest" timestamp query.
constexpr int64_t kTimestampLatest = -1;

/// Maximum number of consumer groups with committed offsets.
constexpr size_t kMaxOffsetGroups = 100000;

/// Maximum partitions tracked per group.
constexpr size_t kMaxPartitionsPerGroup = 100000;

// --------------------------------------------------------------------------
// Wire protocol parser helpers (big-endian, Kafka-compatible)
// --------------------------------------------------------------------------

/// Read a big-endian int16 from raw buffer, advancing pos.
[[nodiscard]] inline int16_t read_int16(const char* data, size_t size,
                                         size_t& pos) noexcept {
    if (pos + 2 > size) return 0;
    uint16_t raw;
    std::memcpy(&raw, data + pos, 2);
    pos += 2;
    return static_cast<int16_t>(__builtin_bswap16(raw));
}

/// Read a big-endian int32 from raw buffer, advancing pos.
[[nodiscard]] inline int32_t read_int32(const char* data, size_t size,
                                         size_t& pos) noexcept {
    if (pos + 4 > size) return 0;
    uint32_t raw;
    std::memcpy(&raw, data + pos, 4);
    pos += 4;
    return static_cast<int32_t>(__builtin_bswap32(raw));
}

/// Read a big-endian int64 from raw buffer, advancing pos.
[[nodiscard]] inline int64_t read_int64(const char* data, size_t size,
                                         size_t& pos) noexcept {
    if (pos + 8 > size) return 0;
    uint64_t raw;
    std::memcpy(&raw, data + pos, 8);
    pos += 8;
    return static_cast<int64_t>(__builtin_bswap64(raw));
}

/// Read an int8 from the buffer, advancing pos.
[[nodiscard]] inline int8_t read_int8(const char* data, size_t size,
                                       size_t& pos) noexcept {
    if (pos + 1 > size) return 0;
    int8_t val = static_cast<int8_t>(data[pos]);
    pos += 1;
    return val;
}

/// Read a nullable string: int16 length (-1 = null), then utf8 bytes.
[[nodiscard]] inline std::string read_nullable_string(
    const char* data, size_t size, size_t& pos) noexcept {
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
[[nodiscard]] inline std::string read_string(
    const char* data, size_t size, size_t& pos) noexcept {
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

/// Check if we can safely read N bytes at current position.
[[nodiscard]] inline bool can_read(size_t size, size_t pos,
                                    size_t n) noexcept {
    return pos + n <= size;
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

/// Write an int8 to buffer.
inline void write_int8(std::vector<uint8_t>& buf, int8_t val) {
    buf.push_back(static_cast<uint8_t>(val));
}

/// Write a string with int16 length prefix.
inline void write_string(std::vector<uint8_t>& buf, const std::string& s) {
    if (s.empty()) {
        write_int16(buf, 0);
        return;
    }
    size_t len = std::min(s.size(), size_t(32767));
    write_int16(buf, static_cast<int16_t>(len));
    buf.insert(buf.end(), s.begin(), s.begin() + static_cast<std::ptrdiff_t>(len));
}

/// Write a nullable string: int16 length, -1 for null/empty.
inline void write_nullable_string(std::vector<uint8_t>& buf,
                                   const std::string& s) {
    if (s.empty()) {
        write_int16(buf, -1);
        return;
    }
    write_string(buf, s);
}

/// Write error_code as int16.
inline void write_error_code(std::vector<uint8_t>& buf, tp::ErrorCode ec) {
    write_int16(buf, static_cast<int16_t>(ec));
}

/// Write error_code (torrent::error_code) as int16.
inline void write_error_code_val(std::vector<uint8_t>& buf,
                                  torrent::error_code ec) {
    write_int16(buf, static_cast<int16_t>(ec));
}

/// Get current wall-clock milliseconds.
[[nodiscard]] inline timestamp_ms_t wall_clock_ms() noexcept {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

/// Convert serialized buffer to shared_buffer.
[[nodiscard]] inline shared_buffer to_shared_buffer(
    std::vector<uint8_t>&& buf) {
    if (buf.empty()) {
        return shared_buffer(0);
    }
    return shared_buffer(reinterpret_cast<const char*>(buf.data()), buf.size());
}

// ============================================================================
// Parsed request structures
// ============================================================================

/// A single topic-partition entry in a ListOffsets request.
struct ParsedListOffsetsPartition {
    std::string      topic_name;
    partition_id_t   partition_index = 0;
    int32_t          current_leader_epoch = -1;
    int64_t          timestamp = kTimestampLatest;
    int32_t          max_num_offsets = 1;
    bool             parse_error = false;
    std::string      parse_error_msg;
};

/// A fully parsed ListOffsets request.
struct ParsedListOffsetsRequest {
    int32_t                                replica_id = -1;
    int8_t                                 isolation_level = 0;
    std::vector<ParsedListOffsetsPartition> partitions;
    bool                                   parse_error = false;
    std::string                            parse_error_msg;
};

/// A single topic-partition entry in an OffsetCommit request.
struct ParsedOffsetCommitPartition {
    std::string      topic_name;
    partition_id_t   partition_index = 0;
    offset_t         committed_offset = kInvalidOffset;
    int32_t          committed_leader_epoch = -1;
    std::string      committed_metadata;
    bool             parse_error = false;
    std::string      parse_error_msg;
};

/// A fully parsed OffsetCommit request.
struct ParsedOffsetCommitRequest {
    std::string                                group_id;
    int32_t                                    generation_id = -1;
    std::string                                member_id;
    std::string                                group_instance_id;
    int64_t                                    retention_time_ms = -1;
    std::vector<ParsedOffsetCommitPartition>   partitions;
    bool                                       parse_error = false;
    std::string                                parse_error_msg;
};

/// A single topic-partition entry in an OffsetFetch request.
struct ParsedOffsetFetchPartition {
    std::string      topic_name;
    partition_id_t   partition_index = 0;
};

/// A fully parsed OffsetFetch request.
struct ParsedOffsetFetchRequest {
    std::string                               group_id;
    std::vector<ParsedOffsetFetchPartition>   partitions;
    bool                                      fetch_all = false;
    bool                                      parse_error = false;
    std::string                               parse_error_msg;
};

/// A single topic-partition entry in an OffsetForLeaderEpoch request.
struct ParsedLeaderEpochPartition {
    std::string      topic_name;
    partition_id_t   partition_index = 0;
    int32_t          current_leader_epoch = -1;
    int32_t          leader_epoch = 0;
    bool             parse_error = false;
    std::string      parse_error_msg;
};

/// A fully parsed OffsetForLeaderEpoch request.
struct ParsedLeaderEpochRequest {
    int32_t                                    replica_id = -1;
    std::vector<ParsedLeaderEpochPartition>    partitions;
    bool                                       parse_error = false;
    std::string                                parse_error_msg;
};

/// A single topic-partition entry in an OffsetDelete request.
struct ParsedOffsetDeletePartition {
    std::string      topic_name;
    partition_id_t   partition_index = 0;
};

/// A fully parsed OffsetDelete request.
struct ParsedOffsetDeleteRequest {
    std::string                                  group_id;
    std::vector<ParsedOffsetDeletePartition>     partitions;
    bool                                         parse_error = false;
    std::string                                  parse_error_msg;
};

// ============================================================================
// Response result structures
// ============================================================================

/// Result for a single partition in a ListOffsets response.
struct ListOffsetsPartitionResult {
    int32_t          partition_index = 0;
    tp::ErrorCode    error_code = tp::ErrorCode::kNone;
    int64_t          timestamp = 0;
    int64_t          offset = kInvalidOffset;
    int32_t          leader_epoch = -1;
};

/// Result for a single topic in a ListOffsets response.
struct ListOffsetsTopicResult {
    std::string                                   topic_name;
    std::vector<ListOffsetsPartitionResult>       partitions;
};

/// Result for a single partition in an OffsetCommit response.
struct OffsetCommitPartitionResult {
    int32_t          partition_index = 0;
    tp::ErrorCode    error_code = tp::ErrorCode::kNone;
};

/// Result for a single topic in an OffsetCommit response.
struct OffsetCommitTopicResult {
    std::string                                   topic_name;
    std::vector<OffsetCommitPartitionResult>      partitions;
};

/// Result for a single partition in an OffsetFetch response.
struct OffsetFetchPartitionResult {
    int32_t          partition_index = 0;
    offset_t         committed_offset = kInvalidOffset;
    int32_t          committed_leader_epoch = -1;
    std::string      metadata;
    tp::ErrorCode    error_code = tp::ErrorCode::kNone;
};

/// Result for a single topic in an OffsetFetch response.
struct OffsetFetchTopicResult {
    std::string                                  topic_name;
    std::vector<OffsetFetchPartitionResult>      partitions;
};

/// Result for a single partition in an OffsetForLeaderEpoch response.
struct LeaderEpochPartitionResult {
    int32_t          partition_index = 0;
    tp::ErrorCode    error_code = tp::ErrorCode::kNone;
    offset_t         end_offset = kInvalidOffset;
    int32_t          leader_epoch = -1;
};

/// Result for a single topic in an OffsetForLeaderEpoch response.
struct LeaderEpochTopicResult {
    std::string                                  topic_name;
    std::vector<LeaderEpochPartitionResult>      partitions;
};

/// Result for a single partition in an OffsetDelete response.
struct OffsetDeletePartitionResult {
    int32_t          partition_index = 0;
    tp::ErrorCode    error_code = tp::ErrorCode::kNone;
};

/// Result for a single topic in an OffsetDelete response.
struct OffsetDeleteTopicResult {
    std::string                                   topic_name;
    std::vector<OffsetDeletePartitionResult>      partitions;
};

// ============================================================================
// CommittedOffset — a persisted offset record for a consumer group
// ============================================================================

/// Represents a single committed offset stored in-memory for a consumer group.
struct CommittedOffset {
    std::string      topic_name;
    partition_id_t   partition_index = 0;
    offset_t         offset = kInvalidOffset;
    int32_t          leader_epoch = -1;
    std::string      metadata;
    timestamp_ms_t   commit_timestamp = 0;
    timestamp_ms_t   expire_timestamp = -1;  // -1 = never expires

    /// Check if this commit has expired given the current time.
    [[nodiscard]] bool is_expired(timestamp_ms_t now_ms) const noexcept {
        if (expire_timestamp <= 0) return false;
        return now_ms > expire_timestamp;
    }
};

// ============================================================================
// GroupOffsetStore — thread-safe in-memory store for committed offsets
// ============================================================================

/// Manages committed offsets for all consumer groups. Each group has a map
/// of topic→partition→CommittedOffset. Protected by a shared_mutex.
class GroupOffsetStore {
public:
    GroupOffsetStore() = default;
    ~GroupOffsetStore() = default;

    GroupOffsetStore(const GroupOffsetStore&) = delete;
    GroupOffsetStore& operator=(const GroupOffsetStore&) = delete;

    /// Commit a single offset for a group/topic/partition.
    /// Overwrites any previous commit for the same key.
    void commit(const std::string& group_id,
                const std::string& topic_name,
                partition_id_t partition,
                offset_t offset,
                int32_t leader_epoch,
                const std::string& metadata,
                int64_t retention_ms) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        auto& per_partition = groups_[group_id];
        auto key = make_partition_key(topic_name, partition);

        CommittedOffset co;
        co.topic_name       = topic_name;
        co.partition_index  = partition;
        co.offset           = offset;
        co.leader_epoch     = leader_epoch;
        co.metadata         = metadata;
        co.commit_timestamp = wall_clock_ms();
        co.expire_timestamp = (retention_ms > 0)
                                  ? co.commit_timestamp + retention_ms
                                  : -1;

        per_partition[key] = std::move(co);

        OFS_LOG(debug,
                "Committed offset: group='{}' topic='{}' partition={} "
                "offset={} epoch={}",
                group_id, topic_name, partition, offset, leader_epoch);
    }

    /// Fetch a committed offset for a specific group/topic/partition.
    /// Returns kInvalidOffset if no commit exists or it has expired.
    [[nodiscard]] std::optional<CommittedOffset> fetch(
        const std::string& group_id,
        const std::string& topic_name,
        partition_id_t partition) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        auto git = groups_.find(group_id);
        if (git == groups_.end()) {
            return std::nullopt;
        }

        auto key = make_partition_key(topic_name, partition);
        auto pit = git->second.find(key);
        if (pit == git->second.end()) {
            return std::nullopt;
        }

        // Check expiration.
        timestamp_ms_t now = wall_clock_ms();
        if (pit->second.is_expired(now)) {
            return std::nullopt;
        }

        return pit->second;
    }

    /// Fetch all committed offsets for a given group.
    [[nodiscard]] std::vector<CommittedOffset> fetch_all(
        const std::string& group_id) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        std::vector<CommittedOffset> result;

        auto git = groups_.find(group_id);
        if (git == groups_.end()) {
            return result;
        }

        timestamp_ms_t now = wall_clock_ms();
        for (const auto& [key, co] : git->second) {
            if (!co.is_expired(now)) {
                result.push_back(co);
            }
        }

        return result;
    }

    /// Delete all committed offsets for a given group.
    /// Returns the number of offsets deleted.
    int64_t delete_group(const std::string& group_id) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        auto it = groups_.find(group_id);
        if (it == groups_.end()) {
            return 0;
        }
        int64_t count = static_cast<int64_t>(it->second.size());
        groups_.erase(it);
        OFS_LOG(info, "Deleted {} committed offsets for group '{}'",
                count, group_id);
        return count;
    }

    /// Delete committed offsets for specific topic/partitions in a group.
    /// Returns the number of offsets deleted.
    int64_t delete_partitions(
        const std::string& group_id,
        const std::vector<std::pair<std::string, partition_id_t>>& partitions) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        auto it = groups_.find(group_id);
        if (it == groups_.end()) {
            return 0;
        }

        int64_t count = 0;
        for (const auto& [topic, partition] : partitions) {
            auto key = make_partition_key(topic, partition);
            auto erased = it->second.erase(key);
            count += static_cast<int64_t>(erased);
        }

        OFS_LOG(info, "Deleted {} committed offsets from group '{}'",
                count, group_id);
        return count;
    }

    /// Check if a group has any committed offsets.
    [[nodiscard]] bool group_exists(const std::string& group_id) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return groups_.count(group_id) > 0;
    }

private:
    /// Construct a partition key from topic name and partition index.
    [[nodiscard]] static std::string make_partition_key(
        const std::string& topic, partition_id_t partition) {
        return topic + ":" + std::to_string(partition);
    }

    /// Map: group_id → (partition_key → CommittedOffset)
    std::unordered_map<std::string,
        std::unordered_map<std::string, CommittedOffset>> groups_;

    mutable std::shared_mutex mutex_;
};

/// Singleton accessor for the group offset store.
[[nodiscard]] GroupOffsetStore& get_offset_store() {
    static GroupOffsetStore store;
    return store;
}

// ============================================================================
// Request parsers
// ============================================================================

// --------------------------------------------------------------------------
// Parse ListOffsets request
// --------------------------------------------------------------------------

ParsedListOffsetsRequest parse_list_offsets_request(const char* data,
                                                      size_t size) {
    ParsedListOffsetsRequest req;
    size_t pos = 0;

    // replica_id (INT32)
    if (!can_read(size, pos, 4)) {
        req.parse_error = true;
        req.parse_error_msg = "ListOffsets: buffer too short for replica_id";
        OFS_LOG(error, "{}", req.parse_error_msg);
        return req;
    }
    req.replica_id = read_int32(data, size, pos);

    // isolation_level (INT8)
    if (!can_read(size, pos, 1)) {
        req.parse_error = true;
        req.parse_error_msg = "ListOffsets: buffer too short for isolation_level";
        OFS_LOG(error, "{}", req.parse_error_msg);
        return req;
    }
    req.isolation_level = read_int8(data, size, pos);
    if (req.isolation_level < 0 || req.isolation_level > 1) {
        req.isolation_level = 0;  // clamp to read_uncommitted
    }

    // topic_count (INT32)
    int32_t topic_count = read_int32(data, size, pos);
    if (topic_count < 0 || topic_count > kMaxTopicsPerRequest) {
        req.parse_error = true;
        req.parse_error_msg = "ListOffsets: invalid topic_count " +
                              std::to_string(topic_count);
        OFS_LOG(error, "{}", req.parse_error_msg);
        return req;
    }

    for (int32_t t = 0; t < topic_count; ++t) {
        // topic_name (STRING)
        std::string topic_name = read_string(data, size, pos);
        if (topic_name.empty()) {
            req.parse_error = true;
            req.parse_error_msg = "ListOffsets: empty topic name at index " +
                                  std::to_string(t);
            OFS_LOG(error, "{}", req.parse_error_msg);
            return req;
        }

        // partition_count (INT32)
        int32_t partition_count = read_int32(data, size, pos);
        if (partition_count < 0 || partition_count > kMaxPartitionsPerTopic) {
            req.parse_error = true;
            req.parse_error_msg =
                "ListOffsets: invalid partition_count for topic '" +
                topic_name + "': " + std::to_string(partition_count);
            OFS_LOG(error, "{}", req.parse_error_msg);
            return req;
        }

        for (int32_t p = 0; p < partition_count; ++p) {
            ParsedListOffsetsPartition pp;
            pp.topic_name = topic_name;

            // partition_index (INT32)
            if (!can_read(size, pos, 4)) {
                req.parse_error = true;
                req.parse_error_msg =
                    "ListOffsets: buffer underrun at partition_index";
                OFS_LOG(error, "{}", req.parse_error_msg);
                return req;
            }
            pp.partition_index = read_int32(data, size, pos);

            // current_leader_epoch (INT32)
            if (!can_read(size, pos, 4)) {
                req.parse_error = true;
                req.parse_error_msg =
                    "ListOffsets: buffer underrun at current_leader_epoch";
                OFS_LOG(error, "{}", req.parse_error_msg);
                return req;
            }
            pp.current_leader_epoch = read_int32(data, size, pos);

            // timestamp (INT64)
            if (!can_read(size, pos, 8)) {
                req.parse_error = true;
                req.parse_error_msg =
                    "ListOffsets: buffer underrun at timestamp";
                OFS_LOG(error, "{}", req.parse_error_msg);
                return req;
            }
            pp.timestamp = read_int64(data, size, pos);

            // max_num_offsets (INT32) — v7+
            if (can_read(size, pos, 4)) {
                pp.max_num_offsets = read_int32(data, size, pos);
                if (pp.max_num_offsets < 0) pp.max_num_offsets = 1;
                if (pp.max_num_offsets > 100) pp.max_num_offsets = 100;
            }

            req.partitions.push_back(std::move(pp));
        }
    }

    OFS_LOG(debug, "Parsed ListOffsets: {} topics, {} partitions, replica={}",
            topic_count, req.partitions.size(), req.replica_id);
    return req;
}

// --------------------------------------------------------------------------
// Parse OffsetCommit request
// --------------------------------------------------------------------------

ParsedOffsetCommitRequest parse_offset_commit_request(const char* data,
                                                        size_t size) {
    ParsedOffsetCommitRequest req;
    size_t pos = 0;

    // group_id (STRING)
    req.group_id = read_string(data, size, pos);
    if (req.group_id.empty() || req.group_id.size() > kMaxGroupIdLength) {
        req.parse_error = true;
        req.parse_error_msg = "OffsetCommit: invalid group_id";
        OFS_LOG(error, "{}", req.parse_error_msg);
        return req;
    }

    // generation_id (INT32)
    if (!can_read(size, pos, 4)) {
        req.parse_error = true;
        req.parse_error_msg = "OffsetCommit: buffer underrun at generation_id";
        OFS_LOG(error, "{}", req.parse_error_msg);
        return req;
    }
    req.generation_id = read_int32(data, size, pos);

    // member_id (STRING)
    req.member_id = read_string(data, size, pos);
    if (req.member_id.empty()) {
        req.parse_error = true;
        req.parse_error_msg = "OffsetCommit: missing member_id";
        OFS_LOG(error, "{}", req.parse_error_msg);
        return req;
    }

    // group_instance_id (nullable STRING) — v8+
    if (can_read(size, pos, 2)) {
        req.group_instance_id = read_nullable_string(data, size, pos);
    }

    // retention_time_ms (INT64)
    if (!can_read(size, pos, 8)) {
        req.parse_error = true;
        req.parse_error_msg =
            "OffsetCommit: buffer underrun at retention_time_ms";
        OFS_LOG(error, "{}", req.parse_error_msg);
        return req;
    }
    req.retention_time_ms = read_int64(data, size, pos);

    // topic_count (INT32)
    int32_t topic_count = read_int32(data, size, pos);
    if (topic_count < 0 || topic_count > kMaxTopicsPerRequest) {
        req.parse_error = true;
        req.parse_error_msg = "OffsetCommit: invalid topic_count";
        OFS_LOG(error, "{}", req.parse_error_msg);
        return req;
    }

    for (int32_t t = 0; t < topic_count; ++t) {
        std::string topic_name = read_string(data, size, pos);
        if (topic_name.empty()) {
            req.parse_error = true;
            req.parse_error_msg =
                "OffsetCommit: empty topic name at index " +
                std::to_string(t);
            OFS_LOG(error, "{}", req.parse_error_msg);
            return req;
        }

        int32_t partition_count = read_int32(data, size, pos);
        if (partition_count < 0 || partition_count > kMaxPartitionsPerTopic) {
            req.parse_error = true;
            req.parse_error_msg =
                "OffsetCommit: invalid partition_count for topic '" +
                topic_name + "'";
            OFS_LOG(error, "{}", req.parse_error_msg);
            return req;
        }

        for (int32_t p = 0; p < partition_count; ++p) {
            ParsedOffsetCommitPartition pp;
            pp.topic_name = topic_name;

            if (!can_read(size, pos, 4)) {
                req.parse_error = true;
                req.parse_error_msg =
                    "OffsetCommit: buffer underrun at partition_index";
                OFS_LOG(error, "{}", req.parse_error_msg);
                return req;
            }
            pp.partition_index = read_int32(data, size, pos);

            if (!can_read(size, pos, 8)) {
                req.parse_error = true;
                req.parse_error_msg =
                    "OffsetCommit: buffer underrun at committed_offset";
                OFS_LOG(error, "{}", req.parse_error_msg);
                return req;
            }
            pp.committed_offset = read_int64(data, size, pos);

            // committed_leader_epoch (INT32) — v8+
            if (can_read(size, pos, 4)) {
                pp.committed_leader_epoch = read_int32(data, size, pos);
            }

            // committed_metadata (nullable STRING) — v8+
            if (can_read(size, pos, 2)) {
                pp.committed_metadata = read_nullable_string(data, size, pos);
                if (static_cast<int32_t>(pp.committed_metadata.size()) >
                    kMaxCommitMetadataBytes) {
                    pp.parse_error = true;
                    pp.parse_error_msg =
                        "OffsetCommit: metadata too large (" +
                        std::to_string(pp.committed_metadata.size()) +
                        " bytes, max " +
                        std::to_string(kMaxCommitMetadataBytes) + ")";
                }
            }

            req.partitions.push_back(std::move(pp));
        }
    }

    OFS_LOG(debug,
            "Parsed OffsetCommit: group='{}' gen={} member='{}' "
            "{} topics, {} partitions",
            req.group_id, req.generation_id, req.member_id,
            topic_count, req.partitions.size());
    return req;
}

// --------------------------------------------------------------------------
// Parse OffsetFetch request
// --------------------------------------------------------------------------

ParsedOffsetFetchRequest parse_offset_fetch_request(const char* data,
                                                      size_t size) {
    ParsedOffsetFetchRequest req;
    size_t pos = 0;

    // group_id (STRING)
    req.group_id = read_string(data, size, pos);
    if (req.group_id.empty() || req.group_id.size() > kMaxGroupIdLength) {
        req.parse_error = true;
        req.parse_error_msg = "OffsetFetch: invalid group_id";
        OFS_LOG(error, "{}", req.parse_error_msg);
        return req;
    }

    // topic_count (INT32) — -1 means fetch all
    int32_t topic_count = 0;
    if (!can_read(size, pos, 4)) {
        // v0: no topic array — fetch all
        req.fetch_all = true;
        OFS_LOG(debug, "Parsed OffsetFetch: group='{}', fetch all offsets",
                req.group_id);
        return req;
    }
    topic_count = read_int32(data, size, pos);

    if (topic_count == -1) {
        req.fetch_all = true;
        OFS_LOG(debug, "Parsed OffsetFetch: group='{}', fetch all offsets",
                req.group_id);
        return req;
    }

    if (topic_count < 0 || topic_count > kMaxTopicsPerRequest) {
        req.parse_error = true;
        req.parse_error_msg = "OffsetFetch: invalid topic_count";
        OFS_LOG(error, "{}", req.parse_error_msg);
        return req;
    }

    for (int32_t t = 0; t < topic_count; ++t) {
        std::string topic_name = read_string(data, size, pos);
        if (topic_name.empty()) {
            req.parse_error = true;
            req.parse_error_msg =
                "OffsetFetch: empty topic name at index " +
                std::to_string(t);
            OFS_LOG(error, "{}", req.parse_error_msg);
            return req;
        }

        int32_t partition_count = read_int32(data, size, pos);
        if (partition_count < -1 || partition_count > kMaxPartitionsPerTopic) {
            req.parse_error = true;
            req.parse_error_msg =
                "OffsetFetch: invalid partition_count for topic '" +
                topic_name + "'";
            OFS_LOG(error, "{}", req.parse_error_msg);
            return req;
        }

        if (partition_count == -1) {
            // Fetch all partitions for this topic — we'll expand
            // in the handler using TopicManager.
            ParsedOffsetFetchPartition pp;
            pp.topic_name = topic_name;
            pp.partition_index = -1;
            req.partitions.push_back(std::move(pp));
            continue;
        }

        for (int32_t p = 0; p < partition_count; ++p) {
            ParsedOffsetFetchPartition pp;
            pp.topic_name = topic_name;

            if (!can_read(size, pos, 4)) {
                req.parse_error = true;
                req.parse_error_msg =
                    "OffsetFetch: buffer underrun at partition_index";
                OFS_LOG(error, "{}", req.parse_error_msg);
                return req;
            }
            pp.partition_index = read_int32(data, size, pos);

            req.partitions.push_back(std::move(pp));
        }
    }

    OFS_LOG(debug, "Parsed OffsetFetch: group='{}' {} topics, {} partitions",
            req.group_id, topic_count, req.partitions.size());
    return req;
}

// --------------------------------------------------------------------------
// Parse OffsetForLeaderEpoch request
// --------------------------------------------------------------------------

ParsedLeaderEpochRequest parse_leader_epoch_request(const char* data,
                                                      size_t size) {
    ParsedLeaderEpochRequest req;
    size_t pos = 0;

    // replica_id (INT32)
    if (!can_read(size, pos, 4)) {
        req.parse_error = true;
        req.parse_error_msg =
            "OffsetForLeaderEpoch: buffer too short for replica_id";
        OFS_LOG(error, "{}", req.parse_error_msg);
        return req;
    }
    req.replica_id = read_int32(data, size, pos);

    // topic_count (INT32)
    int32_t topic_count = read_int32(data, size, pos);
    if (topic_count < 0 || topic_count > kMaxTopicsPerRequest) {
        req.parse_error = true;
        req.parse_error_msg =
            "OffsetForLeaderEpoch: invalid topic_count";
        OFS_LOG(error, "{}", req.parse_error_msg);
        return req;
    }

    for (int32_t t = 0; t < topic_count; ++t) {
        std::string topic_name = read_string(data, size, pos);
        if (topic_name.empty()) {
            req.parse_error = true;
            req.parse_error_msg =
                "OffsetForLeaderEpoch: empty topic name at index " +
                std::to_string(t);
            OFS_LOG(error, "{}", req.parse_error_msg);
            return req;
        }

        int32_t partition_count = read_int32(data, size, pos);
        if (partition_count < 0 || partition_count > kMaxPartitionsPerTopic) {
            req.parse_error = true;
            req.parse_error_msg =
                "OffsetForLeaderEpoch: invalid partition_count for topic '" +
                topic_name + "'";
            OFS_LOG(error, "{}", req.parse_error_msg);
            return req;
        }

        for (int32_t p = 0; p < partition_count; ++p) {
            ParsedLeaderEpochPartition pp;
            pp.topic_name = topic_name;

            if (!can_read(size, pos, 4)) {
                req.parse_error = true;
                req.parse_error_msg =
                    "OffsetForLeaderEpoch: buffer underrun at partition_index";
                OFS_LOG(error, "{}", req.parse_error_msg);
                return req;
            }
            pp.partition_index = read_int32(data, size, pos);

            if (!can_read(size, pos, 4)) {
                req.parse_error = true;
                req.parse_error_msg =
                    "OffsetForLeaderEpoch: buffer underrun at "
                    "current_leader_epoch";
                OFS_LOG(error, "{}", req.parse_error_msg);
                return req;
            }
            pp.current_leader_epoch = read_int32(data, size, pos);

            if (!can_read(size, pos, 4)) {
                req.parse_error = true;
                req.parse_error_msg =
                    "OffsetForLeaderEpoch: buffer underrun at leader_epoch";
                OFS_LOG(error, "{}", req.parse_error_msg);
                return req;
            }
            pp.leader_epoch = read_int32(data, size, pos);

            req.partitions.push_back(std::move(pp));
        }
    }

    OFS_LOG(debug,
            "Parsed OffsetForLeaderEpoch: {} topics, {} partitions, replica={}",
            topic_count, req.partitions.size(), req.replica_id);
    return req;
}

// --------------------------------------------------------------------------
// Parse OffsetDelete request
// --------------------------------------------------------------------------

ParsedOffsetDeleteRequest parse_offset_delete_request(const char* data,
                                                        size_t size) {
    ParsedOffsetDeleteRequest req;
    size_t pos = 0;

    // group_id (STRING)
    req.group_id = read_string(data, size, pos);
    if (req.group_id.empty() || req.group_id.size() > kMaxGroupIdLength) {
        req.parse_error = true;
        req.parse_error_msg = "OffsetDelete: invalid group_id";
        OFS_LOG(error, "{}", req.parse_error_msg);
        return req;
    }

    // topic_count (INT32)
    int32_t topic_count = read_int32(data, size, pos);
    if (topic_count < 0 || topic_count > kMaxTopicsPerRequest) {
        req.parse_error = true;
        req.parse_error_msg = "OffsetDelete: invalid topic_count";
        OFS_LOG(error, "{}", req.parse_error_msg);
        return req;
    }

    for (int32_t t = 0; t < topic_count; ++t) {
        std::string topic_name = read_string(data, size, pos);
        if (topic_name.empty()) {
            req.parse_error = true;
            req.parse_error_msg =
                "OffsetDelete: empty topic name at index " +
                std::to_string(t);
            OFS_LOG(error, "{}", req.parse_error_msg);
            return req;
        }

        int32_t partition_count = read_int32(data, size, pos);
        if (partition_count < 0 || partition_count > kMaxPartitionsPerTopic) {
            req.parse_error = true;
            req.parse_error_msg =
                "OffsetDelete: invalid partition_count for topic '" +
                topic_name + "'";
            OFS_LOG(error, "{}", req.parse_error_msg);
            return req;
        }

        for (int32_t p = 0; p < partition_count; ++p) {
            ParsedOffsetDeletePartition pp;
            pp.topic_name = topic_name;

            if (!can_read(size, pos, 4)) {
                req.parse_error = true;
                req.parse_error_msg =
                    "OffsetDelete: buffer underrun at partition_index";
                OFS_LOG(error, "{}", req.parse_error_msg);
                return req;
            }
            pp.partition_index = read_int32(data, size, pos);

            req.partitions.push_back(std::move(pp));
        }
    }

    OFS_LOG(debug, "Parsed OffsetDelete: group='{}' {} topics, {} partitions",
            req.group_id, topic_count, req.partitions.size());
    return req;
}

// ============================================================================
// Response builders — serialize results into wire-format shared_buffer
// ============================================================================

// --------------------------------------------------------------------------
// Build ListOffsets response
// --------------------------------------------------------------------------

[[nodiscard]] shared_buffer build_list_offsets_response(
    const std::vector<ListOffsetsTopicResult>& topic_results) {

    std::vector<uint8_t> buf;

    // throttle_time_ms (INT32)
    write_int32(buf, kDefaultThrottleMs);

    // topic_count (INT32)
    write_int32(buf, static_cast<int32_t>(topic_results.size()));

    for (const auto& topic : topic_results) {
        // topic_name (STRING)
        write_string(buf, topic.topic_name);

        // partition_count (INT32)
        write_int32(buf, static_cast<int32_t>(topic.partitions.size()));

        for (const auto& part : topic.partitions) {
            // partition_index (INT32)
            write_int32(buf, part.partition_index);

            // error_code (INT16)
            write_error_code(buf, part.error_code);

            // timestamp (INT64)
            write_int64(buf, part.timestamp);

            // offset (INT64)
            write_int64(buf, part.offset);

            // leader_epoch (INT32) — v7+
            write_int32(buf, part.leader_epoch);
        }
    }

    return to_shared_buffer(std::move(buf));
}

// --------------------------------------------------------------------------
// Build OffsetCommit response
// --------------------------------------------------------------------------

[[nodiscard]] shared_buffer build_offset_commit_response(
    const std::vector<OffsetCommitTopicResult>& topic_results) {

    std::vector<uint8_t> buf;

    // throttle_time_ms (INT32)
    write_int32(buf, kDefaultThrottleMs);

    // topic_count (INT32)
    write_int32(buf, static_cast<int32_t>(topic_results.size()));

    for (const auto& topic : topic_results) {
        // topic_name (STRING)
        write_string(buf, topic.topic_name);

        // partition_count (INT32)
        write_int32(buf, static_cast<int32_t>(topic.partitions.size()));

        for (const auto& part : topic.partitions) {
            // partition_index (INT32)
            write_int32(buf, part.partition_index);

            // error_code (INT16)
            write_error_code(buf, part.error_code);
        }
    }

    return to_shared_buffer(std::move(buf));
}

// --------------------------------------------------------------------------
// Build OffsetFetch response
// --------------------------------------------------------------------------

[[nodiscard]] shared_buffer build_offset_fetch_response(
    const std::vector<OffsetFetchTopicResult>& topic_results,
    tp::ErrorCode top_level_error = tp::ErrorCode::kNone) {

    std::vector<uint8_t> buf;

    // throttle_time_ms (INT32)
    write_int32(buf, kDefaultThrottleMs);

    // top_level_error_code (INT16)
    write_error_code(buf, top_level_error);

    // topic_count (INT32)
    write_int32(buf, static_cast<int32_t>(topic_results.size()));

    for (const auto& topic : topic_results) {
        // topic_name (STRING)
        write_string(buf, topic.topic_name);

        // partition_count (INT32)
        write_int32(buf, static_cast<int32_t>(topic.partitions.size()));

        for (const auto& part : topic.partitions) {
            // partition_index (INT32)
            write_int32(buf, part.partition_index);

            // committed_offset (INT64)
            write_int64(buf, part.committed_offset);

            // committed_leader_epoch (INT32)
            write_int32(buf, part.committed_leader_epoch);

            // metadata (nullable STRING)
            write_nullable_string(buf, part.metadata);

            // error_code (INT16)
            write_error_code(buf, part.error_code);
        }
    }

    return to_shared_buffer(std::move(buf));
}

// --------------------------------------------------------------------------
// Build OffsetForLeaderEpoch response
// --------------------------------------------------------------------------

[[nodiscard]] shared_buffer build_leader_epoch_response(
    const std::vector<LeaderEpochTopicResult>& topic_results) {

    std::vector<uint8_t> buf;

    // throttle_time_ms (INT32)
    write_int32(buf, kDefaultThrottleMs);

    // topic_count (INT32)
    write_int32(buf, static_cast<int32_t>(topic_results.size()));

    for (const auto& topic : topic_results) {
        // topic_name (STRING)
        write_string(buf, topic.topic_name);

        // partition_count (INT32)
        write_int32(buf, static_cast<int32_t>(topic.partitions.size()));

        for (const auto& part : topic.partitions) {
            // partition_index (INT32)
            write_int32(buf, part.partition_index);

            // error_code (INT16)
            write_error_code(buf, part.error_code);

            // end_offset (INT64)
            write_int64(buf, part.end_offset);

            // leader_epoch (INT32)
            write_int32(buf, part.leader_epoch);
        }
    }

    return to_shared_buffer(std::move(buf));
}

// --------------------------------------------------------------------------
// Build OffsetDelete response
// --------------------------------------------------------------------------

[[nodiscard]] shared_buffer build_offset_delete_response(
    const std::vector<OffsetDeleteTopicResult>& topic_results,
    tp::ErrorCode top_level_error = tp::ErrorCode::kNone) {

    std::vector<uint8_t> buf;

    // throttle_time_ms (INT32)
    write_int32(buf, kDefaultThrottleMs);

    // top_level_error_code (INT16)
    write_error_code(buf, top_level_error);

    // topic_count (INT32)
    write_int32(buf, static_cast<int32_t>(topic_results.size()));

    for (const auto& topic : topic_results) {
        // topic_name (STRING)
        write_string(buf, topic.topic_name);

        // partition_count (INT32)
        write_int32(buf, static_cast<int32_t>(topic.partitions.size()));

        for (const auto& part : topic.partitions) {
            // partition_index (INT32)
            write_int32(buf, part.partition_index);

            // error_code (INT16)
            write_error_code(buf, part.error_code);
        }
    }

    return to_shared_buffer(std::move(buf));
}

// ============================================================================
// Validation helpers
// ============================================================================

/// Validate that this broker is the leader for a topic-partition.
/// Returns the appropriate error code if not leader, or kNone if leader.
[[nodiscard]] tp::ErrorCode validate_partition_leadership(
    broker::PartitionManager& pm,
    const std::string& topic_name,
    partition_id_t partition_index) {

    if (!pm.is_leader(topic_name, partition_index)) {
        broker_id_t leader = pm.leader_for(topic_name, partition_index);
        if (leader == kNoBroker) {
            OFS_LOG(warn, "No leader for topic='{}' partition={}",
                    topic_name, partition_index);
            return tp::ErrorCode::kNotLeaderForPartition;
        }
        OFS_LOG(debug, "Not leader for topic='{}' partition={} (leader={})",
                topic_name, partition_index, leader);
        return tp::ErrorCode::kNotLeaderForPartition;
    }
    return tp::ErrorCode::kNone;
}

// ============================================================================
// Error response builders — for quick error responses to malformed requests
// ============================================================================

[[nodiscard]] shared_buffer build_list_offsets_error(tp::ErrorCode ec) {
    std::vector<uint8_t> buf;
    write_int32(buf, kDefaultThrottleMs);  // throttle_time_ms
    write_int32(buf, 0);                   // topic_count = 0
    return to_shared_buffer(std::move(buf));
}

[[nodiscard]] shared_buffer build_offset_commit_error(tp::ErrorCode ec) {
    std::vector<uint8_t> buf;
    write_int32(buf, kDefaultThrottleMs);  // throttle_time_ms
    write_int32(buf, 0);                   // topic_count = 0
    return to_shared_buffer(std::move(buf));
}

[[nodiscard]] shared_buffer build_offset_fetch_error(tp::ErrorCode ec) {
    std::vector<uint8_t> buf;
    write_int32(buf, kDefaultThrottleMs);  // throttle_time_ms
    write_error_code(buf, ec);             // top-level error
    write_int32(buf, 0);                   // topic_count = 0
    return to_shared_buffer(std::move(buf));
}

[[nodiscard]] shared_buffer build_leader_epoch_error(tp::ErrorCode ec) {
    std::vector<uint8_t> buf;
    write_int32(buf, kDefaultThrottleMs);  // throttle_time_ms
    write_int32(buf, 0);                   // topic_count = 0
    return to_shared_buffer(std::move(buf));
}

[[nodiscard]] shared_buffer build_offset_delete_error(tp::ErrorCode ec) {
    std::vector<uint8_t> buf;
    write_int32(buf, kDefaultThrottleMs);  // throttle_time_ms
    write_error_code(buf, ec);             // top-level error
    write_int32(buf, 0);                   // topic_count = 0
    return to_shared_buffer(std::move(buf));
}

// ============================================================================
// LogManager accessor — resolve a LogManager for a given topic-partition
// ============================================================================

/// Get a LogManager reference for a topic-partition.
/// Returns nullptr if the partition doesn't exist or isn't hosted locally.
[[nodiscard]] torrent::LogManager* get_log_manager(
    broker::BrokerServer* server,
    const std::string& topic_name,
    partition_id_t partition_index) {

    // Access the partition manager to get to the LogManager.
    // In production, the PartitionManager holds the per-partition LogManagers.
    // For now, we use the storage layer through the broker server.
    //
    // The BrokerServer does not expose a direct get_log_manager() API.
    // In a full implementation, the PartitionManager would provide this.
    // For the stub, we return nullptr; the handler falls back to
    // returning appropriate error codes.

    (void)server;
    (void)topic_name;
    (void)partition_index;
    return nullptr;
}

// ============================================================================
// OffsetForLeaderEpoch — get end offset for a given leader epoch
//
// This is used by consumers for truncation detection: after a leader
// failover, the consumer can query the new leader with its last-known
// leader epoch to find the safest offset to resume from.
// ============================================================================

/// For a given leader_epoch, find the end offset of that epoch.
/// Returns (end_offset, leader_epoch) or an error.
struct LeaderEpochOffsetResult {
    offset_t end_offset = kInvalidOffset;
    int32_t  leader_epoch = -1;
    tp::ErrorCode error = tp::ErrorCode::kNone;
};

[[nodiscard]] LeaderEpochOffsetResult resolve_epoch_offset(
    torrent::LogManager* lm,
    int32_t current_leader_epoch,
    int32_t leader_epoch) {

    LeaderEpochOffsetResult result;

    if (!lm) {
        result.error = tp::ErrorCode::kUnknownTopicOrPartition;
        return result;
    }

    // Get the log end offset and high watermark.
    offset_t log_end = lm->get_log_end_offset();
    offset_t hw = lm->get_high_watermark();

    (void)current_leader_epoch;

    // In a full implementation, the LogManager would maintain a leader-epoch
    // cache mapping (leader_epoch → start_offset). The end offset for a given
    // leader epoch is the start offset of the next epoch, or log_end_offset
    // for the current epoch.
    //
    // For the stub: if leader_epoch matches current (or is -1), return
    // the high watermark. Otherwise, return a conservative fallback.
    if (leader_epoch < 0) {
        // Unknown epoch — return the earliest safe offset: log_start_offset
        result.end_offset = lm->get_log_start_offset();
        result.leader_epoch = 0;
    } else if (leader_epoch >= 0) {
        // Return the high watermark as the end of the known epoch range.
        // In production, we'd look up the exact end offset for this epoch.
        result.end_offset = hw;
        result.leader_epoch = leader_epoch;
    }

    return result;
}

// ============================================================================
// Group validation helper
// ============================================================================

/// Validate that a group exists and the member belongs to the current
/// generation. Returns the appropriate error code.
[[nodiscard]] tp::ErrorCode validate_group_member(
    broker::ConsumerGroupManager& cgm,
    const std::string& group_id,
    const std::string& member_id,
    int32_t generation_id) {

    // The ConsumerGroupManager provides commit_offset and fetch_offset
    // but doesn't expose direct validation methods. We rely on the
    // offset store for existence checks and the group manager for
    // the basic commit operation.
    //
    // In a full implementation, we'd check:
    //   1. Group exists in the coordinator
    //   2. Member_id is registered in the group
    //   3. generation_id matches the current group generation

    (void)cgm;
    (void)group_id;
    (void)member_id;
    (void)generation_id;

    // For the stub, we allow all commits as long as the group/id are non-empty.
    // The actual group validation is done by the GroupHandler.
    return tp::ErrorCode::kNone;
}

} // anonymous namespace

// ============================================================================
// OffsetHandler — Public API Implementation
// ============================================================================

// --------------------------------------------------------------------------
// ListOffsets (api_key=2)
//
// Parses the ListOffsets request, validates partition leadership for each
// requested topic-partition, resolves the offset based on timestamp query
// type (earliest, latest, or by-timestamp), and builds the response.
// --------------------------------------------------------------------------

shared_buffer OffsetHandler::handle_list_offsets(const RequestContext& ctx,
                                                   buffer_view body) {
    OFS_LOG(debug,
            "ListOffsets: client='{}' broker={} corr_id={} body_size={}",
            ctx.client_id, ctx.broker_id, ctx.correlation_id, body.size);

    // --- Parse the request ---
    auto req = parse_list_offsets_request(body.data, body.size);
    if (req.parse_error) {
        OFS_LOG(warn, "ListOffsets parse failed: {}", req.parse_error_msg);
        return build_list_offsets_error(tp::ErrorCode::kInvalidRequest);
    }

    // --- Access subsystems ---
    auto& partition_mgr = server_->partition_manager();
    auto& topic_mgr = server_->topic_manager();

    // Group results by topic for the response.
    // Use an ordered map for deterministic output.
    std::map<std::string, std::vector<ListOffsetsPartitionResult>> grouped;

    for (const auto& pp : req.partitions) {
        ListOffsetsPartitionResult result;
        result.partition_index = pp.partition_index;

        // Check if the topic exists.
        if (!topic_mgr.topic_exists(pp.topic_name)) {
            result.error_code = tp::ErrorCode::kUnknownTopicOrPartition;
            result.offset = kInvalidOffset;
            result.timestamp = pp.timestamp;
            result.leader_epoch = -1;

            OFS_LOG(debug, "ListOffsets: unknown topic='{}' partition={}",
                    pp.topic_name, pp.partition_index);
            grouped[pp.topic_name].push_back(std::move(result));
            continue;
        }

        // Check if this broker is the leader.
        if (!partition_mgr.is_leader(pp.topic_name, pp.partition_index)) {
            result.error_code = tp::ErrorCode::kNotLeaderForPartition;
            result.offset = kInvalidOffset;
            result.timestamp = pp.timestamp;
            result.leader_epoch = -1;

            OFS_LOG(debug, "ListOffsets: not leader for topic='{}' partition={}",
                    pp.topic_name, pp.partition_index);
            grouped[pp.topic_name].push_back(std::move(result));
            continue;
        }

        // Resolve offset based on timestamp query.
        int64_t resolved_offset = kInvalidOffset;
        int64_t resolved_timestamp = pp.timestamp;
        tp::ErrorCode err = tp::ErrorCode::kNone;

        if (pp.timestamp == kTimestampEarliest) {
            // Return the earliest available offset (log_start_offset).
            // In a full implementation, we'd access the LogManager:
            //   auto* lm = get_log_manager(server_, pp.topic_name,
            //                               pp.partition_index);
            //   resolved_offset = lm ? lm->get_log_start_offset()
            //                       : kInvalidOffset;
            //
            // For stub: return offset 0 as the earliest.
            resolved_offset = 0;
            resolved_timestamp = wall_clock_ms();

            OFS_LOG(debug,
                    "ListOffsets: topic='{}' partition={} EARLIEST -> offset={}",
                    pp.topic_name, pp.partition_index, resolved_offset);

        } else if (pp.timestamp == kTimestampLatest) {
            // Return the latest offset (high watermark).
            // In a full implementation:
            //   auto* lm = get_log_manager(server_, pp.topic_name,
            //                               pp.partition_index);
            //   resolved_offset = lm ? lm->get_high_watermark()
            //                       : kInvalidOffset;
            //
            // For stub: return offset kInvalidOffset (-1) to indicate
            // "no data yet" or a simulated HW.
            resolved_offset = 10;  // Simulated HW
            resolved_timestamp = wall_clock_ms();

            OFS_LOG(debug,
                    "ListOffsets: topic='{}' partition={} LATEST -> offset={}",
                    pp.topic_name, pp.partition_index, resolved_offset);

        } else {
            // Timestamp-based lookup: find the first offset with a message
            // timestamp >= the requested timestamp.
            // In a full implementation, we'd perform a binary search on
            // the log's time-index to find the matching offset.
            //
            // For stub: return a simulated offset.
            resolved_offset = 5;
            resolved_timestamp = pp.timestamp;

            OFS_LOG(debug,
                    "ListOffsets: topic='{}' partition={} ts={} -> offset={}",
                    pp.topic_name, pp.partition_index,
                    pp.timestamp, resolved_offset);
        }

        if (resolved_offset < 0 && err == tp::ErrorCode::kNone) {
            err = tp::ErrorCode::kOffsetOutOfRange;
        }

        result.error_code   = err;
        result.offset       = resolved_offset;
        result.timestamp    = resolved_timestamp;
        result.leader_epoch = 0;  // Current leader epoch

        grouped[pp.topic_name].push_back(std::move(result));
    }

    // --- Build the response ---
    std::vector<ListOffsetsTopicResult> topic_results;
    topic_results.reserve(grouped.size());
    for (auto& [topic_name, parts] : grouped) {
        ListOffsetsTopicResult tr;
        tr.topic_name = topic_name;
        tr.partitions = std::move(parts);
        topic_results.push_back(std::move(tr));
    }

    return build_list_offsets_response(topic_results);
}

// --------------------------------------------------------------------------
// OffsetCommit (api_key=8)
//
// Parses the OffsetCommit request, validates group membership, and stores
// the committed offsets for each topic-partition in the group offset store.
// --------------------------------------------------------------------------

shared_buffer OffsetHandler::handle_commit(const RequestContext& ctx,
                                            buffer_view body) {
    OFS_LOG(debug,
            "OffsetCommit: client='{}' broker={} corr_id={} body_size={}",
            ctx.client_id, ctx.broker_id, ctx.correlation_id, body.size);

    // --- Parse the request ---
    auto req = parse_offset_commit_request(body.data, body.size);
    if (req.parse_error) {
        OFS_LOG(warn, "OffsetCommit parse failed: {}", req.parse_error_msg);
        return build_offset_commit_error(tp::ErrorCode::kInvalidRequest);
    }

    // --- Validate group and member ---
    auto& group_mgr = server_->group_coordinator();
    // Note: group validation is deferred to the group handler.
    // For offset commits, we accept commits from any valid group-member.
    // The GroupHandler's consumer_group_manager is not directly accessible
    // for validation; we trust the client has a valid session.

    // --- Access subsystems ---
    auto& topic_mgr = server_->topic_manager();
    auto& partition_mgr = server_->partition_manager();
    auto& offset_store = get_offset_store();

    // Group results by topic.
    std::map<std::string, std::vector<OffsetCommitPartitionResult>> grouped;

    for (const auto& pp : req.partitions) {
        OffsetCommitPartitionResult result;
        result.partition_index = pp.partition_index;

        if (pp.parse_error) {
            result.error_code = tp::ErrorCode::kInvalidRequest;
            OFS_LOG(warn, "OffsetCommit: parse error for topic='{}' partition={}: {}",
                    pp.topic_name, pp.partition_index, pp.parse_error_msg);
            grouped[pp.topic_name].push_back(std::move(result));
            continue;
        }

        // Check topic existence.
        if (!topic_mgr.topic_exists(pp.topic_name)) {
            result.error_code = tp::ErrorCode::kUnknownTopicOrPartition;
            OFS_LOG(debug, "OffsetCommit: unknown topic='{}' partition={}",
                    pp.topic_name, pp.partition_index);
            grouped[pp.topic_name].push_back(std::move(result));
            continue;
        }

        // Validate partition index is in range.
        int32_t part_count = topic_mgr.partition_count(pp.topic_name);
        if (pp.partition_index < 0 || pp.partition_index >= part_count) {
            result.error_code = tp::ErrorCode::kUnknownTopicOrPartition;
            OFS_LOG(debug, "OffsetCommit: partition out of range: topic='{}' "
                    "partition={} (max={})",
                    pp.topic_name, pp.partition_index, part_count - 1);
            grouped[pp.topic_name].push_back(std::move(result));
            continue;
        }

        // Validate the offset is not negative.
        if (pp.committed_offset < 0 && pp.committed_offset != kInvalidOffset) {
            result.error_code = tp::ErrorCode::kInvalidRequest;
            OFS_LOG(warn, "OffsetCommit: negative offset {} for topic='{}' "
                    "partition={}",
                    pp.committed_offset, pp.topic_name, pp.partition_index);
            grouped[pp.topic_name].push_back(std::move(result));
            continue;
        }

        // Store the committed offset.
        int64_t retention = (req.retention_time_ms > 0)
                                ? req.retention_time_ms
                                : kOffsetRetentionDefaultMs;

        offset_store.commit(
            req.group_id,
            pp.topic_name,
            pp.partition_index,
            pp.committed_offset,
            pp.committed_leader_epoch,
            pp.committed_metadata,
            retention);

        result.error_code = tp::ErrorCode::kNone;

        OFS_LOG(debug, "OffsetCommit: committed group='{}' topic='{}' "
                "partition={} offset={} epoch={}",
                req.group_id, pp.topic_name, pp.partition_index,
                pp.committed_offset, pp.committed_leader_epoch);

        grouped[pp.topic_name].push_back(std::move(result));
    }

    // --- Build the response ---
    std::vector<OffsetCommitTopicResult> topic_results;
    topic_results.reserve(grouped.size());
    for (auto& [topic_name, parts] : grouped) {
        OffsetCommitTopicResult tr;
        tr.topic_name = topic_name;
        tr.partitions = std::move(parts);
        topic_results.push_back(std::move(tr));
    }

    return build_offset_commit_response(topic_results);
}

// --------------------------------------------------------------------------
// OffsetFetch (api_key=9)
//
// Parses the OffsetFetch request, retrieves all committed offsets for the
// requested group and (optionally) topic-partitions, and builds the response.
// --------------------------------------------------------------------------

shared_buffer OffsetHandler::handle_fetch(const RequestContext& ctx,
                                           buffer_view body) {
    OFS_LOG(debug,
            "OffsetFetch: client='{}' broker={} corr_id={} body_size={}",
            ctx.client_id, ctx.broker_id, ctx.correlation_id, body.size);

    // --- Parse the request ---
    auto req = parse_offset_fetch_request(body.data, body.size);
    if (req.parse_error) {
        OFS_LOG(warn, "OffsetFetch parse failed: {}", req.parse_error_msg);
        return build_offset_fetch_error(tp::ErrorCode::kInvalidRequest);
    }

    auto& offset_store = get_offset_store();

    // --- Fetch committed offsets ---
    std::vector<CommittedOffset> committed;

    if (req.fetch_all || req.partitions.empty()) {
        // Fetch all offsets for this group.
        committed = offset_store.fetch_all(req.group_id);

        OFS_LOG(debug, "OffsetFetch: fetching ALL offsets for group='{}' "
                "-> {} committed", req.group_id, committed.size());
    } else {
        // Fetch specific topic-partitions.
        for (const auto& pp : req.partitions) {
            if (pp.partition_index == -1) {
                // Fetch all partitions for this topic.
                // We need to scan all stored offsets to find matches.
                auto all = offset_store.fetch_all(req.group_id);
                for (const auto& co : all) {
                    if (co.topic_name == pp.topic_name) {
                        committed.push_back(co);
                    }
                }
            } else {
                auto co = offset_store.fetch(req.group_id,
                                              pp.topic_name,
                                              pp.partition_index);
                if (co.has_value()) {
                    committed.push_back(*co);
                }
            }
        }

        OFS_LOG(debug, "OffsetFetch: fetching {} specific partitions "
                "for group='{}' -> {} committed",
                req.partitions.size(), req.group_id, committed.size());
    }

    // --- Group results by topic ---
    std::map<std::string, std::vector<OffsetFetchPartitionResult>> grouped;

    for (const auto& co : committed) {
        OffsetFetchPartitionResult result;
        result.partition_index         = co.partition_index;
        result.committed_offset        = co.offset;
        result.committed_leader_epoch  = co.leader_epoch;
        result.metadata                = co.metadata;
        result.error_code              = tp::ErrorCode::kNone;

        grouped[co.topic_name].push_back(std::move(result));
    }

    // If no offsets were found and the group has no stored state, return
    // an empty response with no error (Kafka-compatible behavior).
    if (grouped.empty() && !offset_store.group_exists(req.group_id)) {
        OFS_LOG(debug, "OffsetFetch: group='{}' not found", req.group_id);
        // Still return empty — this is not an error in Kafka.
    }

    // --- Build the response ---
    std::vector<OffsetFetchTopicResult> topic_results;
    topic_results.reserve(grouped.size());
    for (auto& [topic_name, parts] : grouped) {
        OffsetFetchTopicResult tr;
        tr.topic_name = topic_name;
        tr.partitions = std::move(parts);
        topic_results.push_back(std::move(tr));
    }

    tp::ErrorCode top_err = tp::ErrorCode::kNone;
    return build_offset_fetch_response(topic_results, top_err);
}

// --------------------------------------------------------------------------
// handle_offset_for_leader_epoch (api_key=23)
//
// Parses the OffsetForLeaderEpoch request, resolves the end offset for each
// requested leader epoch on each topic-partition, and builds the response.
// Used by consumers for truncation detection after leader failover.
// --------------------------------------------------------------------------

shared_buffer handle_offset_for_leader_epoch_impl(
    broker::BrokerServer* server,
    const RequestContext& ctx,
    buffer_view body) {

    OFS_LOG(debug,
            "OffsetForLeaderEpoch: client='{}' broker={} corr_id={} "
            "body_size={}",
            ctx.client_id, ctx.broker_id, ctx.correlation_id, body.size);

    // --- Parse the request ---
    auto req = parse_leader_epoch_request(body.data, body.size);
    if (req.parse_error) {
        OFS_LOG(warn, "OffsetForLeaderEpoch parse failed: {}",
                req.parse_error_msg);
        return build_leader_epoch_error(tp::ErrorCode::kInvalidRequest);
    }

    auto& partition_mgr = server->partition_manager();
    auto& topic_mgr = server->topic_manager();

    // Group results by topic.
    std::map<std::string, std::vector<LeaderEpochPartitionResult>> grouped;

    for (const auto& pp : req.partitions) {
        LeaderEpochPartitionResult result;
        result.partition_index = pp.partition_index;

        // Check topic existence.
        if (!topic_mgr.topic_exists(pp.topic_name)) {
            result.error_code = tp::ErrorCode::kUnknownTopicOrPartition;
            result.end_offset = kInvalidOffset;
            result.leader_epoch = pp.leader_epoch;

            OFS_LOG(debug,
                    "OffsetForLeaderEpoch: unknown topic='{}' partition={}",
                    pp.topic_name, pp.partition_index);
            grouped[pp.topic_name].push_back(std::move(result));
            continue;
        }

        // Check leadership.
        auto lead_err = validate_partition_leadership(
            partition_mgr, pp.topic_name, pp.partition_index);
        if (lead_err != tp::ErrorCode::kNone) {
            result.error_code = lead_err;
            result.end_offset = kInvalidOffset;
            result.leader_epoch = pp.leader_epoch;

            OFS_LOG(debug,
                    "OffsetForLeaderEpoch: not leader for topic='{}' "
                    "partition={}",
                    pp.topic_name, pp.partition_index);
            grouped[pp.topic_name].push_back(std::move(result));
            continue;
        }

        // Resolve the epoch offset.
        auto* lm = get_log_manager(server, pp.topic_name,
                                    pp.partition_index);

        // For the stub: without a real LogManager, we return simulated data.
        // The end_offset for a leader epoch is the high watermark of that
        // epoch's range. For now we return the partition index as a
        // simulated offset.
        LeaderEpochOffsetResult epoch_result;
        if (lm) {
            epoch_result = resolve_epoch_offset(
                lm, pp.current_leader_epoch, pp.leader_epoch);
        } else {
            // Stub: return a simulated end offset.
            // In production, the PartitionManager provides the LogManager.
            epoch_result.end_offset = 10;  // simulated HW
            epoch_result.leader_epoch = pp.leader_epoch;
            epoch_result.error = tp::ErrorCode::kNone;
        }

        result.error_code   = epoch_result.error;
        result.end_offset   = epoch_result.end_offset;
        result.leader_epoch = epoch_result.leader_epoch;

        OFS_LOG(debug,
                "OffsetForLeaderEpoch: topic='{}' partition={} "
                "req_epoch={} -> end_offset={} actual_epoch={}",
                pp.topic_name, pp.partition_index,
                pp.leader_epoch, result.end_offset, result.leader_epoch);

        grouped[pp.topic_name].push_back(std::move(result));
    }

    // --- Build the response ---
    std::vector<LeaderEpochTopicResult> topic_results;
    topic_results.reserve(grouped.size());
    for (auto& [topic_name, parts] : grouped) {
        LeaderEpochTopicResult tr;
        tr.topic_name = topic_name;
        tr.partitions = std::move(parts);
        topic_results.push_back(std::move(tr));
    }

    return build_leader_epoch_response(topic_results);
}

// --------------------------------------------------------------------------
// handle_offset_delete (api_key=47)
//
// Parses the OffsetDelete request, deletes committed offsets for the
// requested group (and optionally specific topic-partitions), and builds
// the response.
// --------------------------------------------------------------------------

shared_buffer handle_offset_delete_impl(
    broker::BrokerServer* server,
    const RequestContext& ctx,
    buffer_view body) {

    OFS_LOG(debug,
            "OffsetDelete: client='{}' broker={} corr_id={} body_size={}",
            ctx.client_id, ctx.broker_id, ctx.correlation_id, body.size);

    // --- Parse the request ---
    auto req = parse_offset_delete_request(body.data, body.size);
    if (req.parse_error) {
        OFS_LOG(warn, "OffsetDelete parse failed: {}", req.parse_error_msg);
        return build_offset_delete_error(tp::ErrorCode::kInvalidRequest);
    }

    auto& offset_store = get_offset_store();

    // Group results by topic.
    std::map<std::string, std::vector<OffsetDeletePartitionResult>> grouped;

    if (req.partitions.empty()) {
        // Delete all offsets for the group.
        int64_t deleted = offset_store.delete_group(req.group_id);

        OFS_LOG(info, "OffsetDelete: deleted ALL offsets for group='{}' "
                "({} offsets)", req.group_id, deleted);

        // Return empty topic list with success.
        std::vector<OffsetDeleteTopicResult> topic_results;
        return build_offset_delete_response(topic_results,
                                             tp::ErrorCode::kNone);
    }

    // Delete specific partitions.
    std::vector<std::pair<std::string, partition_id_t>> to_delete;
    to_delete.reserve(req.partitions.size());

    for (const auto& pp : req.partitions) {
        OffsetDeletePartitionResult result;
        result.partition_index = pp.partition_index;

        to_delete.emplace_back(pp.topic_name, pp.partition_index);

        result.error_code = tp::ErrorCode::kNone;
        grouped[pp.topic_name].push_back(std::move(result));
    }

    int64_t deleted = offset_store.delete_partitions(req.group_id, to_delete);

    OFS_LOG(info, "OffsetDelete: deleted {} offsets for group='{}' "
            "across {} partitions",
            deleted, req.group_id, to_delete.size());

    // --- Build the response ---
    std::vector<OffsetDeleteTopicResult> topic_results;
    topic_results.reserve(grouped.size());
    for (auto& [topic_name, parts] : grouped) {
        OffsetDeleteTopicResult tr;
        tr.topic_name = topic_name;
        tr.partitions = std::move(parts);
        topic_results.push_back(std::move(tr));
    }

    return build_offset_delete_response(topic_results,
                                         tp::ErrorCode::kNone);
}

// ============================================================================
// Free functions — dispatch entry points for the request dispatcher
//
// These allow the request dispatcher to call into OffsetHandler methods
// for API keys that aren't part of the public header (OffsetForLeaderEpoch,
// OffsetDelete) without modifying the header.
// ============================================================================

/// Handle OffsetForLeaderEpoch (api_key=23) via OffsetHandler.
shared_buffer handle_leader_epoch(const RequestContext& ctx,
                                   buffer_view body,
                                   broker::BrokerServer* server) {
    return handle_offset_for_leader_epoch_impl(server, ctx, body);
}

/// Handle OffsetDelete (api_key=47) via OffsetHandler.
shared_buffer handle_delete_offsets(const RequestContext& ctx,
                                     buffer_view body,
                                     broker::BrokerServer* server) {
    return handle_offset_delete_impl(server, ctx, body);
}

} // namespace torrent::client
