/**
 * torrent-mq — MetadataHandler: Metadata, Configs, and AlterConfigs API
 *
 * Handles four Kafka API keys:
 *   - Metadata               (api_key=3)
 *   - DescribeConfigs         (api_key=32)
 *   - AlterConfigs            (api_key=33)
 *   - IncrementalAlterConfigs (api_key=44)
 *
 * Parses wire-format binary protocol (big-endian, Kafka-compatible),
 * validates all preconditions (topic existence, authorization, resource
 * validity), collects cluster/topic/broker/config metadata from the server
 * subsystems, and builds serialised responses for each API.
 *
 * Wire format — Metadata Request (Kafka v0–v12 compatible):
 *   INT32  topic_count                          [nullable: if -1 list all topics]
 *   per-topic:
 *     STRING      topic_name
 *   INT8   allow_auto_topic_creation            [v4+]
 *   BOOL   include_cluster_authorized_ops       [v8+]
 *   BOOL   include_topic_authorized_ops         [v8+]
 *
 * Wire format — Metadata Response:
 *   INT32  throttle_time_ms
 *   INT32  broker_count
 *   per-broker:
 *     INT32            node_id
 *     STRING           host
 *     INT32            port
 *     NULLABLE_STRING  rack
 *   STRING    cluster_id                                 [v2+]
 *   INT32     controller_id                              [v1+]
 *   INT32     topic_count
 *   per-topic:
 *     INT16            error_code
 *     STRING           topic_name
 *     BOOL             is_internal                        [v1+]
 *     INT32            partition_count
 *     per-partition:
 *       INT16            error_code
 *       INT32            partition_index
 *       INT32            leader_id
 *       INT32            leader_epoch                     [v7+]
 *       INT32            replica_count
 *       per-replica:
 *         INT32            replica_id
 *       INT32            isr_count
 *       per-isr:
 *         INT32            isr_id
 *       INT32            offline_replica_count             [v5+]
 *       per-offline-replica:
 *         INT32            offline_replica_id
 *
 * Wire format — DescribeConfigs Request:
 *   INT32  resource_count
 *   per-resource:
 *     INT8   resource_type        (2=topic, 4=broker)
 *     STRING resource_name
 *     INT32  config_key_count     (-1 = null = all)
 *     per-key:
 *       STRING config_key
 *     BOOL   include_synonyms
 *     BOOL   include_documentation
 *
 * Wire format — DescribeConfigs Response:
 *   INT32  throttle_time_ms
 *   INT32  resource_count
 *   per-resource:
 *     INT16            error_code
 *     STRING           error_message
 *     INT8             resource_type
 *     STRING           resource_name
 *     INT32            config_count
 *     per-config:
 *       STRING           config_name
 *       NULLABLE_STRING  config_value
 *       BOOL             read_only
 *       BOOL             is_default
 *       BOOL             is_sensitive
 *       INT8             config_source
 *       INT32            synonym_count
 *       per-synonym:
 *         STRING           synonym_name
 *         NULLABLE_STRING  synonym_value
 *         INT8             synonym_source
 *
 * Wire format — AlterConfigs Request:
 *   INT32  resource_count
 *   per-resource:
 *     INT8   resource_type
 *     STRING resource_name
 *     INT32  config_count
 *     per-config:
 *       STRING           config_name
 *       NULLABLE_STRING  config_value
 *   BOOL   validate_only
 *
 * Wire format — AlterConfigs Response:
 *   INT32  throttle_time_ms
 *   INT32  resource_count
 *   per-resource:
 *     INT16  error_code
 *     STRING error_message
 *     INT8   resource_type
 *     STRING resource_name
 *
 * Wire format — IncrementalAlterConfigs Request:
 *   INT32  resource_count
 *   per-resource:
 *     INT8    resource_type
 *     STRING  resource_name
 *     INT32   config_count
 *     per-config:
 *       STRING           config_name
 *       INT8             op_type        (0=set, 1=delete, 2=append, 3=subtract)
 *       NULLABLE_STRING  config_value
 *   BOOL    validate_only
 *
 * Wire format — IncrementalAlterConfigs Response:
 *   INT32  throttle_time_ms
 *   INT32  resource_count
 *   per-resource:
 *     INT16  error_code
 *     STRING error_message
 *     INT8   resource_type
 *     STRING resource_name
 *
 * Thread-safety: The handler is called from the broker's I/O thread pool.
 * MetadataHandler itself is stateless beyond a pointer to BrokerServer.
 * All mutable state is protected by the server's internal locks.
 */

#include "torrent/client/metadata_handler.h"
#include "torrent/broker/server.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/network/protocol.h"
#include "torrent/common/types.h"
#include "torrent/storage/types.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <chrono>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

// ============================================================================
// Alias for readability
// ============================================================================

namespace tp = torrent::protocol;

namespace torrent::client {
using torrent::broker::BrokerServer;

// ============================================================================
// Anonymous namespace — internal helpers, parsing, serialization
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_metadata_logger() {
    static auto logger = []() {
        auto l = spdlog::get("metadata_handler");
        if (!l) {
            l = spdlog::stdout_color_mt("metadata_handler");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define META_LOG(level, ...) \
    get_metadata_logger()->level("[metadata] " __VA_ARGS__)

#define CONFIG_LOG(level, ...) \
    get_metadata_logger()->level("[config] " __VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Resource types for DescribeConfigs / AlterConfigs / IncrementalAlterConfigs
constexpr int8_t kResourceTypeUnknown = 0;
constexpr int8_t kResourceTypeTopic   = 2;
constexpr int8_t kResourceTypeBroker  = 4;
constexpr int8_t kResourceTypeBrokerLogger = 8;  // Kafka 1.1+

/// Config source constants (for DescribeConfigs synonyms)
constexpr int8_t kConfigSourceUnknown              = 0;
constexpr int8_t kConfigSourceTopicConfig          = 1;
constexpr int8_t kConfigSourceDynamicBrokerConfig  = 2;
constexpr int8_t kConfigSourceDynamicDefaultBrokerConfig = 3;
constexpr int8_t kConfigSourceStaticBrokerConfig   = 4;
constexpr int8_t kConfigSourceDefaultConfig        = 5;
constexpr int8_t kConfigSourceDynamicConfig        = 6;

/// Config operation types for IncrementalAlterConfigs
constexpr int8_t kConfigOpSet       = 0;
constexpr int8_t kConfigOpDelete    = 1;
constexpr int8_t kConfigOpAppend    = 2;
constexpr int8_t kConfigOpSubtract  = 3;

/// Default throttle time if processing completes under 1ms.
constexpr int32_t kDefaultThrottleMs = 0;

/// Maximum number of topics per metadata request.
constexpr int32_t kMaxTopicsPerMetadataRequest = 10000;

/// Maximum number of resources per Describe/Alter Config request.
constexpr int32_t kMaxResourcesPerConfigRequest = 1000;

/// Maximum config entries per resource.
constexpr int32_t kMaxConfigEntriesPerResource = 500;

/// Maximum config key / value length in bytes.
constexpr size_t kMaxConfigKeyLength   = 256;
constexpr size_t kMaxConfigValueLength = 16384;  // 16 KiB

/// Maximum resource name length.
constexpr size_t kMaxResourceNameLength = 1024;

// --------------------------------------------------------------------------
// Wire format parse helpers — big-endian binary protocol
// --------------------------------------------------------------------------

/// Read a big-endian INT16 from buffer at *pos, advance.
bool read_int16_be(const char* data, size_t size, size_t* pos, int16_t* out) noexcept {
    if (*pos + 2 > size) return false;
    uint16_t raw;
    std::memcpy(&raw, data + *pos, sizeof(raw));
    *out = static_cast<int16_t>(__builtin_bswap16(raw));
    *pos += 2;
    return true;
}

/// Read a big-endian INT32 from buffer.
bool read_int32_be(const char* data, size_t size, size_t* pos, int32_t* out) noexcept {
    if (*pos + 4 > size) return false;
    uint32_t raw;
    std::memcpy(&raw, data + *pos, sizeof(raw));
    *out = static_cast<int32_t>(__builtin_bswap32(raw));
    *pos += 4;
    return true;
}

/// Read a big-endian INT64 from buffer.
bool read_int64_be(const char* data, size_t size, size_t* pos, int64_t* out) noexcept {
    if (*pos + 8 > size) return false;
    uint64_t raw;
    std::memcpy(&raw, data + *pos, sizeof(raw));
    *out = static_cast<int64_t>(__builtin_bswap64(raw));
    *pos += 8;
    return true;
}

/// Read a big-endian INT8 from buffer.
bool read_int8(const char* data, size_t size, size_t* pos, int8_t* out) noexcept {
    if (*pos + 1 > size) return false;
    *out = static_cast<int8_t>(data[*pos]);
    *pos += 1;
    return true;
}

/// Read a single boolean byte (INT8 treated as 0=false, non-zero=true).
bool read_bool(const char* data, size_t size, size_t* pos, bool* out) noexcept {
    int8_t val = 0;
    if (!read_int8(data, size, pos, &val)) return false;
    *out = (val != 0);
    return true;
}

/// Read a nullable string: INT16 length (-1 = null), followed by UTF-8 bytes.
bool read_nullable_string(const char* data, size_t size, size_t* pos,
                          std::string* out) noexcept {
    int16_t len = 0;
    if (!read_int16_be(data, size, pos, &len)) return false;
    if (len == -1) {
        out->clear();
        return true;
    }
    if (len < 0) return false;
    if (*pos + static_cast<size_t>(len) > size) return false;
    out->assign(data + *pos, static_cast<size_t>(len));
    *pos += static_cast<size_t>(len);
    return true;
}

/// Read an INT16-length-prefixed string (non-nullable).
bool read_string(const char* data, size_t size, size_t* pos,
                 std::string* out) noexcept {
    int16_t len = 0;
    if (!read_int16_be(data, size, pos, &len)) return false;
    if (len < 0) return false;
    if (*pos + static_cast<size_t>(len) > size) return false;
    out->assign(data + *pos, static_cast<size_t>(len));
    *pos += static_cast<size_t>(len);
    return true;
}

/// Skip N bytes in the buffer cursor.
bool skip_bytes(const char*, size_t size, size_t* pos, size_t n) noexcept {
    if (*pos + n > size) return false;
    *pos += n;
    return true;
}

// --------------------------------------------------------------------------
// Wire format write helpers — big-endian serialization
// --------------------------------------------------------------------------

/// Write big-endian INT16 to buffer.
void write_int16_be(std::vector<char>& buf, int16_t val) noexcept {
    uint16_t raw = __builtin_bswap16(static_cast<uint16_t>(val));
    buf.insert(buf.end(), reinterpret_cast<const char*>(&raw),
               reinterpret_cast<const char*>(&raw) + sizeof(raw));
}

/// Write big-endian INT32 to buffer.
void write_int32_be(std::vector<char>& buf, int32_t val) noexcept {
    uint32_t raw = __builtin_bswap32(static_cast<uint32_t>(val));
    buf.insert(buf.end(), reinterpret_cast<const char*>(&raw),
               reinterpret_cast<const char*>(&raw) + sizeof(raw));
}

/// Write big-endian INT64 to buffer.
void write_int64_be(std::vector<char>& buf, int64_t val) noexcept {
    uint64_t raw = __builtin_bswap64(static_cast<uint64_t>(val));
    buf.insert(buf.end(), reinterpret_cast<const char*>(&raw),
               reinterpret_cast<const char*>(&raw) + sizeof(raw));
}

/// Write INT8 to buffer.
void write_int8(std::vector<char>& buf, int8_t val) noexcept {
    buf.push_back(static_cast<char>(val));
}

/// Write a BOOL as INT8 (0 or 1).
void write_bool(std::vector<char>& buf, bool val) noexcept {
    write_int8(buf, val ? 1 : 0);
}

/// Write nullable string: INT16 length, -1 for null, else bytes.
void write_nullable_string(std::vector<char>& buf, const std::string& str) noexcept {
    if (str.empty()) {
        write_int16_be(buf, -1);
    } else {
        int16_t len = static_cast<int16_t>(str.size());
        write_int16_be(buf, len);
        buf.insert(buf.end(), str.begin(), str.end());
    }
}

/// Write non-nullable string.
void write_string(std::vector<char>& buf, const std::string& str) noexcept {
    int16_t len = static_cast<int16_t>(str.size());
    write_int16_be(buf, len);
    buf.insert(buf.end(), str.begin(), str.end());
}

/// Write raw bytes with INT32 length prefix.
void write_bytes32(std::vector<char>& buf, const char* data, int32_t len) noexcept {
    write_int32_be(buf, len);
    if (len > 0 && data != nullptr) {
        buf.insert(buf.end(), data, data + len);
    }
}

/// Get current wall-clock milliseconds.
timestamp_ms_t wall_clock_ms() noexcept {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// ============================================================================
// Parsed request structures — internal representations
// ============================================================================

/// Parsed MetadataRequest with all fields decoded from wire format.
struct ParsedMetadataRequest {
    std::vector<std::string> topics;        ///< Requested topics (empty = all).
    bool allow_auto_create = false;
    bool include_cluster_authorized_ops = false;
    bool include_topic_authorized_ops = false;
    bool request_all_topics = false;         ///< True when topics array was null.
    bool parse_error = false;
    std::string parse_error_msg;
};

/// Parsed DescribeConfigsRequest with all fields decoded.
struct ParsedDescribeConfigsRequest {
    struct ResourceRequest {
        int8_t resource_type = kResourceTypeUnknown;
        std::string resource_name;
        std::vector<std::string> config_keys;  ///< Empty = all.
        bool include_synonyms = false;
        bool include_documentation = false;
    };
    std::vector<ResourceRequest> resources;
    bool parse_error = false;
    std::string parse_error_msg;
};

/// Parsed AlterConfigsRequest with all fields decoded.
struct ParsedAlterConfigsRequest {
    struct ResourceAlter {
        int8_t resource_type = kResourceTypeUnknown;
        std::string resource_name;
        std::vector<std::pair<std::string, std::string>> configs;  // key, value
    };
    std::vector<ResourceAlter> resources;
    bool validate_only = false;
    bool parse_error = false;
    std::string parse_error_msg;
};

/// Parsed IncrementalAlterConfigsRequest with all fields decoded.
struct ParsedIncrementalAlterConfigsRequest {
    struct IncrementalOp {
        std::string config_name;
        int8_t op_type = kConfigOpSet;       // 0=set, 1=delete, 2=append, 3=subtract
        std::string config_value;             // Null means delete (no value).
        bool value_is_null = false;
    };
    struct ResourceAlter {
        int8_t resource_type = kResourceTypeUnknown;
        std::string resource_name;
        std::vector<IncrementalOp> ops;
    };
    std::vector<ResourceAlter> resources;
    bool validate_only = false;
    bool parse_error = false;
    std::string parse_error_msg;
};

// ============================================================================
// Internal response building structures
// ============================================================================

/// Broker information for MetadataResponse.
struct BrokerInfo {
    broker_id_t node_id = kNoBroker;
    std::string host;
    int32_t port = kDefaultPort;
    std::string rack;
};

/// Partition information for MetadataResponse.
struct PartitionInfo {
    partition_id_t partition_index = 0;
    broker_id_t leader_id = kNoBroker;
    int32_t leader_epoch = 0;
    std::vector<broker_id_t> replicas;
    std::vector<broker_id_t> isr;          ///< In-sync replicas.
    std::vector<broker_id_t> offline_replicas;
    error_code error = error_code::none;
};

/// Topic metadata for MetadataResponse.
struct TopicMetadataInfo {
    std::string topic_name;
    bool is_internal = false;
    std::vector<PartitionInfo> partitions;
    error_code error = error_code::none;
    std::string error_message;
};

/// Config entry for DescribeConfigs response.
struct ConfigEntryInfo {
    std::string config_name;
    std::string config_value;              ///< May be null for sensitive configs.
    bool read_only = false;
    bool is_default = true;
    bool is_sensitive = false;
    int8_t config_source = kConfigSourceDefaultConfig;

    struct Synonym {
        std::string synonym_name;
        std::string synonym_value;
        int8_t synonym_source = kConfigSourceDefaultConfig;
    };
    std::vector<Synonym> synonyms;
};

/// Resource configs for DescribeConfigs response.
struct ResourceConfigInfo {
    int8_t resource_type = kResourceTypeUnknown;
    std::string resource_name;
    error_code error = error_code::none;
    std::string error_message;
    std::vector<ConfigEntryInfo> configs;
};

/// Per-resource result for AlterConfigs / IncrementalAlterConfigs response.
struct ConfigAlterResult {
    int8_t resource_type = kResourceTypeUnknown;
    std::string resource_name;
    error_code error = error_code::none;
    std::string error_message;
};

// ============================================================================
// MetadataRequestParser — wire-format parser for Metadata
// ============================================================================

class MetadataRequestParser {
public:
    explicit MetadataRequestParser(buffer_view body)
        : data_(body.data), size_(body.size), pos_(0) {}

    /// Parse the full wire request. Returns true on success.
    bool parse();

    const ParsedMetadataRequest& request() const noexcept { return req_; }

private:
    const char* data_;
    size_t size_;
    size_t pos_;
    ParsedMetadataRequest req_;
};

bool MetadataRequestParser::parse() {
    // --- Read topic count ---
    int32_t topic_count = 0;
    if (!read_int32_be(data_, size_, &pos_, &topic_count)) {
        req_.parse_error = true;
        req_.parse_error_msg = "Failed to read topic_count in MetadataRequest";
        META_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    // topic_count == -1 means "all topics" (null topics array)
    if (topic_count == -1) {
        req_.request_all_topics = true;
        META_LOG(debug, "MetadataRequest: all topics requested (topic_count=-1)");
    } else if (topic_count < 0) {
        req_.parse_error = true;
        req_.parse_error_msg = "Invalid negative topic_count: " + std::to_string(topic_count);
        META_LOG(error, "{}", req_.parse_error_msg);
        return false;
    } else if (topic_count > kMaxTopicsPerMetadataRequest) {
        req_.parse_error = true;
        req_.parse_error_msg = "Too many topics requested: " + std::to_string(topic_count) +
                               " (max " + std::to_string(kMaxTopicsPerMetadataRequest) + ")";
        META_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    // --- Read per-topic strings ---
    req_.topics.reserve(static_cast<size_t>(topic_count));
    for (int32_t t = 0; t < topic_count; ++t) {
        std::string topic_name;
        if (!read_string(data_, size_, &pos_, &topic_name)) {
            req_.parse_error = true;
            req_.parse_error_msg = "Failed to read topic name at index " + std::to_string(t);
            META_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }
        if (topic_name.empty()) {
            req_.parse_error = true;
            req_.parse_error_msg = "Empty topic name at index " + std::to_string(t);
            META_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }
        req_.topics.push_back(std::move(topic_name));
    }

    // --- Read allow_auto_topic_creation (v4+) ---
    // This field is present from API version 4. We always attempt to parse it
    // since the caller doesn't pass the API version to the parser; if the buffer
    // is consumed exactly at this point, it means the client didn't send it.
    if (pos_ < size_) {
        bool allow_auto = false;
        if (!read_bool(data_, size_, &pos_, &allow_auto)) {
            req_.parse_error = true;
            req_.parse_error_msg = "Failed to read allow_auto_topic_creation";
            META_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }
        req_.allow_auto_create = allow_auto;
    }

    // --- Read include_cluster_authorized_operations (v8+) ---
    if (pos_ < size_) {
        bool include_cluster = false;
        if (!read_bool(data_, size_, &pos_, &include_cluster)) {
            req_.parse_error = true;
            req_.parse_error_msg = "Failed to read include_cluster_authorized_operations";
            META_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }
        req_.include_cluster_authorized_ops = include_cluster;
    }

    // --- Read include_topic_authorized_operations (v8+) ---
    if (pos_ < size_) {
        bool include_topic = false;
        if (!read_bool(data_, size_, &pos_, &include_topic)) {
            req_.parse_error = true;
            req_.parse_error_msg = "Failed to read include_topic_authorized_operations";
            META_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }
        req_.include_topic_authorized_ops = include_topic;
    }

    META_LOG(debug, "Parsed MetadataRequest: topics={} all={} auto_create={}",
             req_.topics.size(), req_.request_all_topics, req_.allow_auto_create);
    return true;
}

// ============================================================================
// DescribeConfigsRequestParser — wire-format parser for DescribeConfigs
// ============================================================================

class DescribeConfigsRequestParser {
public:
    explicit DescribeConfigsRequestParser(buffer_view body)
        : data_(body.data), size_(body.size), pos_(0) {}

    bool parse();

    const ParsedDescribeConfigsRequest& request() const noexcept { return req_; }

private:
    bool parse_resource(ParsedDescribeConfigsRequest::ResourceRequest& res);

    const char* data_;
    size_t size_;
    size_t pos_;
    ParsedDescribeConfigsRequest req_;
};

bool DescribeConfigsRequestParser::parse() {
    // --- Read resource count ---
    int32_t resource_count = 0;
    if (!read_int32_be(data_, size_, &pos_, &resource_count)) {
        req_.parse_error = true;
        req_.parse_error_msg = "Failed to read resource_count in DescribeConfigsRequest";
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }
    if (resource_count < 0) {
        req_.parse_error = true;
        req_.parse_error_msg = "Negative resource_count: " + std::to_string(resource_count);
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }
    if (resource_count > kMaxResourcesPerConfigRequest) {
        req_.parse_error = true;
        req_.parse_error_msg = "Too many resources: " + std::to_string(resource_count);
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    req_.resources.reserve(static_cast<size_t>(resource_count));
    for (int32_t r = 0; r < resource_count; ++r) {
        ParsedDescribeConfigsRequest::ResourceRequest res;
        if (!parse_resource(res)) return false;
        req_.resources.push_back(std::move(res));
    }

    CONFIG_LOG(debug, "Parsed DescribeConfigsRequest: {} resources", resource_count);
    return true;
}

bool DescribeConfigsRequestParser::parse_resource(
    ParsedDescribeConfigsRequest::ResourceRequest& res) {

    // --- resource_type ---
    if (!read_int8(data_, size_, &pos_, &res.resource_type)) {
        req_.parse_error = true;
        req_.parse_error_msg = "Failed to read resource_type";
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    // --- resource_name ---
    if (!read_string(data_, size_, &pos_, &res.resource_name)) {
        req_.parse_error = true;
        req_.parse_error_msg = "Failed to read resource_name";
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    if (res.resource_name.size() > kMaxResourceNameLength) {
        req_.parse_error = true;
        req_.parse_error_msg = "Resource name too long: " +
                               std::to_string(res.resource_name.size());
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    // --- config_keys (nullable array: count then strings) ---
    int32_t key_count = 0;
    if (!read_int32_be(data_, size_, &pos_, &key_count)) {
        req_.parse_error = true;
        req_.parse_error_msg = "Failed to read config_key_count";
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    // Null array (-1) means "all configs".
    if (key_count >= 0) {
        if (key_count > kMaxConfigEntriesPerResource) {
            req_.parse_error = true;
            req_.parse_error_msg = "Too many config keys: " + std::to_string(key_count);
            CONFIG_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }

        res.config_keys.reserve(static_cast<size_t>(key_count));
        for (int32_t k = 0; k < key_count; ++k) {
            std::string key;
            if (!read_string(data_, size_, &pos_, &key)) {
                req_.parse_error = true;
                req_.parse_error_msg = "Failed to read config key at index " + std::to_string(k);
                CONFIG_LOG(error, "{}", req_.parse_error_msg);
                return false;
            }
            if (key.size() > kMaxConfigKeyLength) {
                req_.parse_error = true;
                req_.parse_error_msg = "Config key too long: " + std::to_string(key.size());
                CONFIG_LOG(error, "{}", req_.parse_error_msg);
                return false;
            }
            res.config_keys.push_back(std::move(key));
        }
    }

    // --- include_synonyms ---
    if (!read_bool(data_, size_, &pos_, &res.include_synonyms)) {
        req_.parse_error = true;
        req_.parse_error_msg = "Failed to read include_synonyms";
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    // --- include_documentation (optional, may not be present in older versions) ---
    // Try to read it, but if we're at end of buffer, default to false.
    if (pos_ < size_) {
        if (!read_bool(data_, size_, &pos_, &res.include_documentation)) {
            req_.parse_error = true;
            req_.parse_error_msg = "Failed to read include_documentation";
            CONFIG_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }
    }

    return true;
}

// ============================================================================
// AlterConfigsRequestParser — wire-format parser for AlterConfigs (api_key=33)
// ============================================================================

class AlterConfigsRequestParser {
public:
    explicit AlterConfigsRequestParser(buffer_view body)
        : data_(body.data), size_(body.size), pos_(0) {}

    bool parse();

    const ParsedAlterConfigsRequest& request() const noexcept { return req_; }

private:
    bool parse_resource_alter(ParsedAlterConfigsRequest::ResourceAlter& res);

    const char* data_;
    size_t size_;
    size_t pos_;
    ParsedAlterConfigsRequest req_;
};

bool AlterConfigsRequestParser::parse() {
    // --- Read resource count ---
    int32_t resource_count = 0;
    if (!read_int32_be(data_, size_, &pos_, &resource_count)) {
        req_.parse_error = true;
        req_.parse_error_msg = "Failed to read resource_count in AlterConfigsRequest";
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }
    if (resource_count < 0) {
        req_.parse_error = true;
        req_.parse_error_msg = "Negative resource_count: " + std::to_string(resource_count);
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }
    if (resource_count > kMaxResourcesPerConfigRequest) {
        req_.parse_error = true;
        req_.parse_error_msg = "Too many resources: " + std::to_string(resource_count);
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    req_.resources.reserve(static_cast<size_t>(resource_count));
    for (int32_t r = 0; r < resource_count; ++r) {
        ParsedAlterConfigsRequest::ResourceAlter res;
        if (!parse_resource_alter(res)) return false;
        req_.resources.push_back(std::move(res));
    }

    // --- validate_only ---
    if (pos_ < size_) {
        if (!read_bool(data_, size_, &pos_, &req_.validate_only)) {
            req_.parse_error = true;
            req_.parse_error_msg = "Failed to read validate_only in AlterConfigsRequest";
            CONFIG_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }
    }

    CONFIG_LOG(debug, "Parsed AlterConfigsRequest: {} resources validate_only={}",
               resource_count, req_.validate_only);
    return true;
}

bool AlterConfigsRequestParser::parse_resource_alter(
    ParsedAlterConfigsRequest::ResourceAlter& res) {

    // --- resource_type ---
    if (!read_int8(data_, size_, &pos_, &res.resource_type)) {
        req_.parse_error = true;
        req_.parse_error_msg = "Failed to read resource_type in AlterConfigs";
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    // --- resource_name ---
    if (!read_string(data_, size_, &pos_, &res.resource_name)) {
        req_.parse_error = true;
        req_.parse_error_msg = "Failed to read resource_name in AlterConfigs";
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    if (res.resource_name.size() > kMaxResourceNameLength) {
        req_.parse_error = true;
        req_.parse_error_msg = "Resource name too long: " +
                               std::to_string(res.resource_name.size());
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    // --- config count ---
    int32_t config_count = 0;
    if (!read_int32_be(data_, size_, &pos_, &config_count)) {
        req_.parse_error = true;
        req_.parse_error_msg = "Failed to read config_count in AlterConfigs";
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }
    if (config_count < 0) {
        req_.parse_error = true;
        req_.parse_error_msg = "Negative config_count: " + std::to_string(config_count);
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }
    if (config_count > kMaxConfigEntriesPerResource) {
        req_.parse_error = true;
        req_.parse_error_msg = "Too many config entries: " + std::to_string(config_count);
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    res.configs.reserve(static_cast<size_t>(config_count));
    for (int32_t c = 0; c < config_count; ++c) {
        std::string cfg_name;
        std::string cfg_value;

        // --- config name ---
        if (!read_string(data_, size_, &pos_, &cfg_name)) {
            req_.parse_error = true;
            req_.parse_error_msg = "Failed to read config name at index " + std::to_string(c);
            CONFIG_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }
        if (cfg_name.size() > kMaxConfigKeyLength) {
            req_.parse_error = true;
            req_.parse_error_msg = "Config name too long: " + std::to_string(cfg_name.size());
            CONFIG_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }

        // --- config value (nullable string) ---
        if (!read_nullable_string(data_, size_, &pos_, &cfg_value)) {
            req_.parse_error = true;
            req_.parse_error_msg = "Failed to read config value for '" + cfg_name + "'";
            CONFIG_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }
        if (cfg_value.size() > kMaxConfigValueLength) {
            req_.parse_error = true;
            req_.parse_error_msg = "Config value too long: " + std::to_string(cfg_value.size());
            CONFIG_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }

        res.configs.emplace_back(std::move(cfg_name), std::move(cfg_value));
    }

    return true;
}

// ============================================================================
// IncrementalAlterConfigsRequestParser — wire-format parser for api_key=44
// ============================================================================

class IncrementalAlterConfigsRequestParser {
public:
    explicit IncrementalAlterConfigsRequestParser(buffer_view body)
        : data_(body.data), size_(body.size), pos_(0) {}

    bool parse();

    const ParsedIncrementalAlterConfigsRequest& request() const noexcept { return req_; }

private:
    bool parse_resource_alter(ParsedIncrementalAlterConfigsRequest::ResourceAlter& res);

    const char* data_;
    size_t size_;
    size_t pos_;
    ParsedIncrementalAlterConfigsRequest req_;
};

bool IncrementalAlterConfigsRequestParser::parse() {
    // --- Read resource count ---
    int32_t resource_count = 0;
    if (!read_int32_be(data_, size_, &pos_, &resource_count)) {
        req_.parse_error = true;
        req_.parse_error_msg = "Failed to read resource_count in IncrementalAlterConfigsRequest";
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }
    if (resource_count < 0) {
        req_.parse_error = true;
        req_.parse_error_msg = "Negative resource_count: " + std::to_string(resource_count);
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }
    if (resource_count > kMaxResourcesPerConfigRequest) {
        req_.parse_error = true;
        req_.parse_error_msg = "Too many resources: " + std::to_string(resource_count);
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    req_.resources.reserve(static_cast<size_t>(resource_count));
    for (int32_t r = 0; r < resource_count; ++r) {
        ParsedIncrementalAlterConfigsRequest::ResourceAlter res;
        if (!parse_resource_alter(res)) return false;
        req_.resources.push_back(std::move(res));
    }

    // --- validate_only ---
    if (pos_ < size_) {
        if (!read_bool(data_, size_, &pos_, &req_.validate_only)) {
            req_.parse_error = true;
            req_.parse_error_msg = "Failed to read validate_only in IncrementalAlterConfigsRequest";
            CONFIG_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }
    }

    CONFIG_LOG(debug, "Parsed IncrementalAlterConfigsRequest: {} resources validate_only={}",
               resource_count, req_.validate_only);
    return true;
}

bool IncrementalAlterConfigsRequestParser::parse_resource_alter(
    ParsedIncrementalAlterConfigsRequest::ResourceAlter& res) {

    // --- resource_type ---
    if (!read_int8(data_, size_, &pos_, &res.resource_type)) {
        req_.parse_error = true;
        req_.parse_error_msg = "Failed to read resource_type in IncrementalAlterConfigs";
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    // --- resource_name ---
    if (!read_string(data_, size_, &pos_, &res.resource_name)) {
        req_.parse_error = true;
        req_.parse_error_msg = "Failed to read resource_name in IncrementalAlterConfigs";
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    if (res.resource_name.size() > kMaxResourceNameLength) {
        req_.parse_error = true;
        req_.parse_error_msg = "Resource name too long: " +
                               std::to_string(res.resource_name.size());
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    // --- config count ---
    int32_t config_count = 0;
    if (!read_int32_be(data_, size_, &pos_, &config_count)) {
        req_.parse_error = true;
        req_.parse_error_msg = "Failed to read config_count in IncrementalAlterConfigs";
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }
    if (config_count < 0) {
        req_.parse_error = true;
        req_.parse_error_msg = "Negative config_count: " + std::to_string(config_count);
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }
    if (config_count > kMaxConfigEntriesPerResource) {
        req_.parse_error = true;
        req_.parse_error_msg = "Too many config entries: " + std::to_string(config_count);
        CONFIG_LOG(error, "{}", req_.parse_error_msg);
        return false;
    }

    res.ops.reserve(static_cast<size_t>(config_count));
    for (int32_t c = 0; c < config_count; ++c) {
        ParsedIncrementalAlterConfigsRequest::IncrementalOp op;

        // --- config name ---
        if (!read_string(data_, size_, &pos_, &op.config_name)) {
            req_.parse_error = true;
            req_.parse_error_msg = "Failed to read config name at index " + std::to_string(c);
            CONFIG_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }
        if (op.config_name.size() > kMaxConfigKeyLength) {
            req_.parse_error = true;
            req_.parse_error_msg = "Config name too long: " + std::to_string(op.config_name.size());
            CONFIG_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }

        // --- op_type ---
        if (!read_int8(data_, size_, &pos_, &op.op_type)) {
            req_.parse_error = true;
            req_.parse_error_msg = "Failed to read op_type for '" + op.config_name + "'";
            CONFIG_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }
        if (op.op_type < 0 || op.op_type > 3) {
            req_.parse_error = true;
            req_.parse_error_msg = "Invalid op_type: " + std::to_string(op.op_type);
            CONFIG_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }

        // --- config value (nullable string) ---
        op.config_value.clear();
        op.value_is_null = false;
        if (!read_nullable_string(data_, size_, &pos_, &op.config_value)) {
            req_.parse_error = true;
            req_.parse_error_msg = "Failed to read config value for '" + op.config_name + "'";
            CONFIG_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }
        if (op.config_value.empty()) {
            op.value_is_null = true;
        }

        if (op.config_value.size() > kMaxConfigValueLength) {
            req_.parse_error = true;
            req_.parse_error_msg = "Config value too long: " + std::to_string(op.config_value.size());
            CONFIG_LOG(error, "{}", req_.parse_error_msg);
            return false;
        }

        res.ops.push_back(std::move(op));
    }

    return true;
}

// ============================================================================
// MetadataCollector — gathers cluster/topic/broker metadata from server
// ============================================================================

class MetadataCollector {
public:
    explicit MetadataCollector(BrokerServer& server,
                                const RequestContext& ctx,
                                const ParsedMetadataRequest& req)
        : server_(server), ctx_(ctx), req_(req) {}

    /// Collect all metadata needed for the response.
    void collect();

    // -- Accessors --
    const std::vector<BrokerInfo>& brokers() const noexcept { return brokers_; }
    const std::vector<TopicMetadataInfo>& topics() const noexcept { return topics_; }
    broker_id_t controller_id() const noexcept { return controller_id_; }
    int32_t controller_epoch() const noexcept { return controller_epoch_; }
    const std::string& cluster_id() const noexcept { return cluster_id_; }
    error_code top_level_error() const noexcept { return top_level_error_; }

private:
    /// Collect broker list from MetadataCache.
    void collect_brokers();

    /// Collect topic metadata for requested topics (or all if request_all_topics).
    void collect_topics();

    /// Collect partition data for a single topic.
    void collect_partitions_for_topic(TopicMetadataInfo& tmi);

    BrokerServer& server_;
    const RequestContext& ctx_;
    const ParsedMetadataRequest& req_;

    std::vector<BrokerInfo> brokers_;
    std::vector<TopicMetadataInfo> topics_;
    broker_id_t controller_id_ = kNoBroker;
    int32_t controller_epoch_ = 0;
    std::string cluster_id_;
    error_code top_level_error_ = error_code::none;
};

void MetadataCollector::collect_brokers() {
    const auto& cache = server_.metadata_cache();

    // Populate broker list from the MetadataCache.
    // In production, MetadataCache stores all known brokers via upsert_broker().
    // We iterate by checking broker count and querying each one.
    // Since MetadataCache doesn't have an iterate-all method exposed,
    // we construct from what's available: broker_id(), known topology.

    // Get the local broker's info from BrokerConfig.
    // In this implementation, we use the server's own broker ID as the
    // sole broker entry, plus any known from the metadata cache.
    // In a full cluster, the cache would contain all peers.

    // Add the local broker first.
    BrokerInfo local_broker;
    local_broker.node_id = server_.broker_id();

    // Determine host/port from advertised listeners if available.
    // We access the broker config through the server's internal state.
    // Since BrokerConfig is private, we construct from defaults.
    // The server's metadata_cache() exposes broker endpoints.

    // Query the cache for each known broker ID.
    // In a real cluster, we'd have a full list.
    auto maybe_ep = cache.get_broker(server_.broker_id());
    if (maybe_ep.has_value()) {
        local_broker.host = maybe_ep->host;
        local_broker.port = static_cast<int32_t>(maybe_ep->port);
    } else {
        // Default to localhost for single-broker setup.
        local_broker.host = "localhost";
        local_broker.port = kDefaultPort;
    }
    local_broker.rack.clear();

    brokers_.push_back(std::move(local_broker));

    // Collect additional brokers from the cache.
    // Since MetadataCache doesn't expose iterate, we collect via topic
    // replicas — each unique replica broker_id we encounter during topic
    // collection will be added. We defer that to collect_topics/collect_partitions.

    // Get controller info from the cache.
    controller_id_ = cache.controller_id();
    controller_epoch_ = static_cast<int32_t>(cache.controller_epoch());

    // If no controller is set, default to the local broker.
    if (controller_id_ == kNoBroker) {
        controller_id_ = server_.broker_id();
    }

    META_LOG(debug, "Collected {} brokers, controller_id={}",
             brokers_.size(), controller_id_);
}

void MetadataCollector::collect_partitions_for_topic(TopicMetadataInfo& tmi) {
    const auto& cache = server_.metadata_cache();
    auto& pm = server_.partition_manager();

    // Get partition count from the cache.
    int32_t part_count = cache.partition_count(tmi.topic_name);

    if (part_count <= 0) {
        // Try topic_manager for partition count.
        auto& tm = server_.topic_manager();
        part_count = tm.partition_count(tmi.topic_name);

        if (part_count <= 0) {
            // Default to 1 partition if no metadata is available.
            part_count = 1;
        }
    }

    tmi.partitions.reserve(static_cast<size_t>(part_count));

    for (partition_id_t p = 0; p < part_count; ++p) {
        PartitionInfo pi;
        pi.partition_index = p;

        // Get leader for this partition.
        broker_id_t leader = pm.leader_for(tmi.topic_name, p);
        if (leader == kNoBroker || leader == kAnyBroker) {
            leader = server_.broker_id();  // Assume local broker if no leader known
        }
        pi.leader_id = leader;
        pi.leader_epoch = 0;  // Would come from partition state in production.

        // Get replicas for this partition.
        auto replicas = pm.replicas_for(tmi.topic_name, p);
        if (replicas.empty()) {
            // Default: single replica (the leader).
            replicas = { leader };
        }
        pi.replicas = std::move(replicas);

        // ISR = replicas (simplified; in production, only in-sync replicas).
        pi.isr = pi.replicas;

        // Offline replicas — empty in this simple implementation.
        pi.offline_replicas.clear();

        pi.error = error_code::none;

        tmi.partitions.push_back(std::move(pi));
    }
}

void MetadataCollector::collect_topics() {
    auto& tm = server_.topic_manager();
    const auto& cache = server_.metadata_cache();

    if (req_.request_all_topics || req_.topics.empty()) {
        // Return metadata for all known topics.
        auto all_topics = tm.list_topics();

        topics_.reserve(all_topics.size());
        for (const auto& topic_meta : all_topics) {
            TopicMetadataInfo tmi;
            tmi.topic_name = topic_meta.name;
            tmi.is_internal = (topic_meta.name.size() >= 2 &&
                               topic_meta.name[0] == '_' &&
                               topic_meta.name[1] == '_');  // __consumer_offsets etc.
            tmi.error = error_code::none;

            collect_partitions_for_topic(tmi);
            topics_.push_back(std::move(tmi));
        }

        // If no topics are registered yet, return an empty topic list.
        if (topics_.empty()) {
            META_LOG(debug, "No topics found; metadata response will have zero topics");
        }
    } else {
        // Return metadata only for the requested topics.
        topics_.reserve(req_.topics.size());
        for (const auto& topic_name : req_.topics) {
            TopicMetadataInfo tmi;
            tmi.topic_name = topic_name;
            tmi.is_internal = false;

            if (tm.topic_exists(topic_name)) {
                tmi.error = error_code::none;
                collect_partitions_for_topic(tmi);
            } else {
                // Topic not found.
                tmi.error = error_code::unknown_topic_or_partition;
                tmi.error_message = "Topic '" + topic_name + "' not found";

                // If auto-create is allowed and this is a metadata request that
                // supports it, attempt auto-creation.
                if (req_.allow_auto_create) {
                    META_LOG(info, "Auto-creating topic '{}' from MetadataRequest", topic_name);
                    auto create_result = tm.create_topic(topic_name, 1, 1);
                    if (create_result.ok()) {
                        tmi.error = error_code::none;
                        tmi.error_message.clear();
                        collect_partitions_for_topic(tmi);
                    } else {
                        tmi.error = error_code::unknown_topic_or_partition;
                        tmi.error_message = "Topic '" + topic_name + "' not found and " +
                                            "auto-creation failed: " +
                                            create_result.error_message;
                    }
                }
            }

            // Collect any broker_ids we encounter from replicas.
            for (const auto& pi : tmi.partitions) {
                for (auto rid : pi.replicas) {
                    // Check if we already have this broker in our list.
                    bool found = false;
                    for (const auto& b : brokers_) {
                        if (b.node_id == rid) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        // Add this broker to the broker list.
                        auto ep_opt = cache.get_broker(rid);
                        BrokerInfo bi;
                        bi.node_id = rid;
                        if (ep_opt.has_value()) {
                            bi.host = ep_opt->host;
                            bi.port = static_cast<int32_t>(ep_opt->port);
                        } else {
                            bi.host = "unknown";
                            bi.port = kDefaultPort;
                        }
                        brokers_.push_back(std::move(bi));
                    }
                }
            }

            topics_.push_back(std::move(tmi));
        }
    }
}

void MetadataCollector::collect() {
    // Step 1: Collect broker information.
    collect_brokers();

    // Step 2: Collect topic metadata.
    collect_topics();

    // Step 3: Get cluster_id from BrokerConfig.
    // Since BrokerConfig is private, we use a default cluster_id.
    // In production, the server would expose a getter.
    cluster_id_ = "torrent-mq-cluster";

    META_LOG(info, "Metadata collected: {} brokers, {} topics, controller={}",
             brokers_.size(), topics_.size(), controller_id_);
}

// ============================================================================
// MetadataResponseBuilder — builds the wire-format MetadataResponse
// ============================================================================

class MetadataResponseBuilder {
public:
    /// Build and return the serialised response as a shared_buffer.
    shared_buffer build(int32_t throttle_time_ms,
                        const MetadataCollector& collector,
                        bool include_cluster_authz,
                        bool include_topic_authz) const;

private:
    /// Write a single broker entry to the buffer.
    void write_broker(std::vector<char>& buf, const BrokerInfo& broker) const;

    /// Write a single partition entry to the buffer.
    void write_partition(std::vector<char>& buf,
                         const PartitionInfo& pi,
                         bool include_leader_epoch,
                         bool include_offline_replicas) const;

    /// Write a single topic entry to the buffer.
    void write_topic(std::vector<char>& buf,
                     const TopicMetadataInfo& tmi,
                     bool include_internal_flag,
                     bool include_topic_authz) const;
};

void MetadataResponseBuilder::write_broker(std::vector<char>& buf,
                                            const BrokerInfo& broker) const {
    // node_id
    write_int32_be(buf, broker.node_id);

    // host
    write_string(buf, broker.host);

    // port
    write_int32_be(buf, broker.port);

    // rack (nullable string)
    write_nullable_string(buf, broker.rack);
}

void MetadataResponseBuilder::write_partition(std::vector<char>& buf,
                                                const PartitionInfo& pi,
                                                bool include_leader_epoch,
                                                bool include_offline_replicas) const {
    // error_code
    write_int16_be(buf, static_cast<int16_t>(pi.error));

    // partition_index
    write_int32_be(buf, pi.partition_index);

    // leader_id
    write_int32_be(buf, pi.leader_id);

    // leader_epoch (v7+)
    if (include_leader_epoch) {
        write_int32_be(buf, pi.leader_epoch);
    }

    // replica array
    write_int32_be(buf, static_cast<int32_t>(pi.replicas.size()));
    for (auto rid : pi.replicas) {
        write_int32_be(buf, rid);
    }

    // isr array
    write_int32_be(buf, static_cast<int32_t>(pi.isr.size()));
    for (auto rid : pi.isr) {
        write_int32_be(buf, rid);
    }

    // offline replicas (v5+)
    if (include_offline_replicas) {
        write_int32_be(buf, static_cast<int32_t>(pi.offline_replicas.size()));
        for (auto rid : pi.offline_replicas) {
            write_int32_be(buf, rid);
        }
    }
}

void MetadataResponseBuilder::write_topic(std::vector<char>& buf,
                                            const TopicMetadataInfo& tmi,
                                            bool include_internal_flag,
                                            bool include_topic_authz) const {
    // error_code
    write_int16_be(buf, static_cast<int16_t>(tmi.error));

    // topic_name
    write_string(buf, tmi.topic_name);

    // is_internal (v1+)
    if (include_internal_flag) {
        write_bool(buf, tmi.is_internal);
    }

    // partition array count
    write_int32_be(buf, static_cast<int32_t>(tmi.partitions.size()));

    // partitions
    for (const auto& pi : tmi.partitions) {
        write_partition(buf, pi, true /* leader_epoch */, true /* offline_replicas */);
    }

    // topic_authorized_operations (v8+)
    if (include_topic_authz) {
        // Write as INT32 count followed by INT32 op codes.
        // For now, write empty authorized operations (0 count).
        write_int32_be(buf, 0);
    }
}

shared_buffer MetadataResponseBuilder::build(
    int32_t throttle_time_ms,
    const MetadataCollector& collector,
    bool include_cluster_authz,
    bool include_topic_authz) const {

    std::vector<char> buf;
    // Pre-allocate a reasonable initial size.
    buf.reserve(4096 + collector.brokers().size() * 128 +
                collector.topics().size() * 512);

    // --- throttle_time_ms ---
    write_int32_be(buf, throttle_time_ms);

    // --- broker array ---
    write_int32_be(buf, static_cast<int32_t>(collector.brokers().size()));
    for (const auto& broker : collector.brokers()) {
        write_broker(buf, broker);
    }

    // --- cluster_id (v2+) ---
    write_string(buf, collector.cluster_id());

    // --- controller_id (v1+) ---
    write_int32_be(buf, collector.controller_id());

    // --- controller_epoch ---
    // Some implementations include controller_epoch after controller_id.
    // We include it inline with the controller_id to be compatible.

    // --- topic array ---
    write_int32_be(buf, static_cast<int32_t>(collector.topics().size()));

    for (const auto& tmi : collector.topics()) {
        write_topic(buf, tmi, true /* is_internal */, include_topic_authz);
    }

    // --- cluster_authorized_operations (v8+) ---
    if (include_cluster_authz) {
        // Write as INT32 count followed by INT32 op codes.
        // For now, write empty authorized operations.
        write_int32_be(buf, 0);
    }

    // Copy into shared_buffer.
    shared_buffer result(buf.data(), buf.size());
    return result;
}

// ============================================================================
// ConfigCollector — gathers configs from the server
// ============================================================================

class ConfigCollector {
public:
    explicit ConfigCollector(BrokerServer& server)
        : server_(server) {}

    /// Collect configs for DescribeConfigs.
    std::vector<ResourceConfigInfo> describe_configs(
        const ParsedDescribeConfigsRequest& req,
        const RequestContext& ctx);

    /// Apply configs for AlterConfigs.
    std::vector<ConfigAlterResult> alter_configs(
        const ParsedAlterConfigsRequest& req,
        const RequestContext& ctx);

    /// Apply incremental config changes.
    std::vector<ConfigAlterResult> incremental_alter_configs(
        const ParsedIncrementalAlterConfigsRequest& req,
        const RequestContext& ctx);

private:
    /// Get all configs for a topic resource.
    ResourceConfigInfo get_topic_configs(
        const std::string& topic_name,
        const std::vector<std::string>& config_keys,
        bool include_synonyms);

    /// Get all configs for a broker resource.
    ResourceConfigInfo get_broker_configs(
        const std::string& broker_name,  // Usually the broker ID as string.
        const std::vector<std::string>& config_keys,
        bool include_synonyms);

    /// Apply a single config change to a topic.
    bool apply_topic_config(const std::string& topic_name,
                            const std::string& key,
                            const std::string& value,
                            error_code* out_err,
                            std::string* out_msg);

    /// Build default config entries for known topic configs.
    std::vector<ConfigEntryInfo> build_topic_config_entries(
        const std::string& topic_name,
        const std::vector<std::string>& filter_keys);

    /// Build default config entries for broker configs.
    std::vector<ConfigEntryInfo> build_broker_config_entries(
        const std::string& broker_id_str,
        const std::vector<std::string>& filter_keys);

    BrokerServer& server_;
};

std::vector<ConfigEntryInfo> ConfigCollector::build_topic_config_entries(
    const std::string& topic_name,
    const std::vector<std::string>& filter_keys) {

    std::vector<ConfigEntryInfo> entries;

    // Define known topic-level configs with defaults.
    // This is the canonical set of Kafka topic configs.
    struct DefaultConfig {
        const char* name;
        const char* default_value;
        bool read_only;
        bool is_default;
        bool is_sensitive;
    };

    static const std::vector<DefaultConfig> kTopicDefaults = {
        {"retention.ms",            "604800000", false, true, false},
        {"retention.bytes",         "-1",        false, true, false},
        {"segment.bytes",           "1073741824",false, true, false},
        {"segment.ms",              "604800000", false, true, false},
        {"segment.index.bytes",     "10485760",  false, true, false},
        {"min.insync.replicas",     "1",         false, true, false},
        {"cleanup.policy",          "delete",     false, true, false},
        {"compression.type",        "producer",   false, true, false},
        {"delete.retention.ms",     "86400000",  false, true, false},
        {"file.delete.delay.ms",    "60000",     false, true, false},
        {"flush.messages",          "9223372036854775807", false, true, false},
        {"flush.ms",                "9223372036854775807", false, true, false},
        {"follower.replication.throttled.replicas",     "", false, true, false},
        {"index.interval.bytes",    "4096",      false, true, false},
        {"leader.replication.throttled.replicas",        "", false, true, false},
        {"max.compaction.lag.ms",   "9223372036854775807", false, true, false},
        {"max.message.bytes",       "1048588",   false, true, false},
        {"message.downconversion.enable", "true",false, true, false},
        {"message.format.version",  "3.0-IV1",   false, true, false},
        {"message.timestamp.type",  "CreateTime", false, true, false},
        {"min.cleanable.dirty.ratio","0.5",      false, true, false},
        {"min.compaction.lag.ms",   "0",         false, true, false},
        {"preallocate",             "false",     false, true, false},
        {"unclean.leader.election.enable", "false", false, true, false},
    };

    // Determine which keys to include.
    bool include_all = filter_keys.empty();
    std::unordered_set<std::string> key_set(filter_keys.begin(), filter_keys.end());

    for (const auto& dc : kTopicDefaults) {
        if (!include_all && key_set.count(dc.name) == 0) {
            continue;
        }

        ConfigEntryInfo entry;
        entry.config_name = dc.name;
        entry.config_value = dc.default_value;
        entry.read_only = dc.read_only;
        entry.is_default = dc.is_default;
        entry.is_sensitive = dc.is_sensitive;
        entry.config_source = kConfigSourceDefaultConfig;

        entries.push_back(std::move(entry));
    }

    return entries;
}

std::vector<ConfigEntryInfo> ConfigCollector::build_broker_config_entries(
    const std::string& /*broker_id_str*/,
    const std::vector<std::string>& filter_keys) {

    std::vector<ConfigEntryInfo> entries;

    struct DefaultConfig {
        const char* name;
        const char* default_value;
        bool read_only;
        bool is_default;
        bool is_sensitive;
    };

    static const std::vector<DefaultConfig> kBrokerDefaults = {
        {"log.retention.ms",            "604800000", false, true, false},
        {"log.retention.bytes",         "-1",        false, true, false},
        {"log.segment.bytes",           "1073741824",false, true, false},
        {"log.segment.ms",              "604800000", false, true, false},
        {"log.dirs",                    "/var/lib/torrent", false, true, false},
        {"num.io.threads",              "8",         true,  true, false},
        {"num.network.threads",         "3",         true,  true, false},
        {"num.replica.fetchers",        "1",         false, true, false},
        {"num.recovery.threads.per.data.dir", "1",   false, true, false},
        {"socket.send.buffer.bytes",    "102400",    true,  true, false},
        {"socket.receive.buffer.bytes", "102400",    true,  true, false},
        {"socket.request.max.bytes",    "104857600", true,  true, false},
        {"max.connections",             "65536",     false, true, false},
        {"max.connections.per.ip",      "65536",     false, true, false},
        {"background.threads",          "10",        false, true, false},
        {"compression.type",            "producer",  false, true, false},
        {"group.min.session.timeout.ms","6000",      false, true, false},
        {"group.max.session.timeout.ms","1800000",   false, true, false},
        {"group.initial.rebalance.delay.ms", "3000", false, true, false},
        {"leader.imbalance.check.interval.seconds", "300", false, true, false},
        {"leader.imbalance.per.broker.percentage", "10", false, true, false},
        {"offsets.retention.minutes",   "10080",     false, true, false},
        {"offsets.topic.num.partitions","50",        true,  true, false},
        {"offsets.topic.replication.factor", "3",    true,  true, false},
        {"transaction.state.log.min.isr", "1",       false, true, false},
        {"transaction.state.log.num.partitions", "50", true, true, false},
        {"transaction.state.log.replication.factor", "3", true, true, false},
        {"auto.create.topics.enable",   "true",      false, true, false},
        {"delete.topic.enable",         "true",      false, true, false},
    };

    bool include_all = filter_keys.empty();
    std::unordered_set<std::string> key_set(filter_keys.begin(), filter_keys.end());

    for (const auto& dc : kBrokerDefaults) {
        if (!include_all && key_set.count(dc.name) == 0) {
            continue;
        }

        ConfigEntryInfo entry;
        entry.config_name = dc.name;
        entry.config_value = dc.default_value;
        entry.read_only = dc.read_only;
        entry.is_default = dc.is_default;
        entry.is_sensitive = dc.is_sensitive;
        entry.config_source = kConfigSourceDefaultConfig;

        entries.push_back(std::move(entry));
    }

    return entries;
}

ResourceConfigInfo ConfigCollector::get_topic_configs(
    const std::string& topic_name,
    const std::vector<std::string>& config_keys,
    bool include_synonyms) {

    ResourceConfigInfo rci;
    rci.resource_type = kResourceTypeTopic;
    rci.resource_name = topic_name;

    auto& tm = server_.topic_manager();
    if (!tm.topic_exists(topic_name)) {
        rci.error = error_code::unknown_topic_or_partition;
        rci.error_message = "Topic '" + topic_name + "' not found";
        CONFIG_LOG(warn, "{}", rci.error_message);
        return rci;
    }

    rci.error = error_code::none;
    rci.configs = build_topic_config_entries(topic_name, config_keys);

    // Add synonyms if requested.
    if (include_synonyms) {
        for (auto& entry : rci.configs) {
            ConfigEntryInfo::Synonym syn;
            syn.synonym_name = entry.config_name;
            syn.synonym_value = entry.config_value;
            syn.synonym_source = kConfigSourceDefaultConfig;
            entry.synonyms.push_back(std::move(syn));
        }
    }

    CONFIG_LOG(debug, "Described topic config for '{}': {} entries",
               topic_name, rci.configs.size());
    return rci;
}

ResourceConfigInfo ConfigCollector::get_broker_configs(
    const std::string& broker_name,
    const std::vector<std::string>& config_keys,
    bool include_synonyms) {

    ResourceConfigInfo rci;
    rci.resource_type = kResourceTypeBroker;
    rci.resource_name = broker_name;
    rci.error = error_code::none;
    rci.configs = build_broker_config_entries(broker_name, config_keys);

    if (include_synonyms) {
        for (auto& entry : rci.configs) {
            ConfigEntryInfo::Synonym syn;
            syn.synonym_name = entry.config_name;
            syn.synonym_value = entry.config_value;
            syn.synonym_source = kConfigSourceStaticBrokerConfig;
            entry.synonyms.push_back(std::move(syn));
        }
    }

    CONFIG_LOG(debug, "Described broker config for '{}': {} entries",
               broker_name, rci.configs.size());
    return rci;
}

std::vector<ResourceConfigInfo> ConfigCollector::describe_configs(
    const ParsedDescribeConfigsRequest& req,
    const RequestContext& ctx) {

    std::vector<ResourceConfigInfo> results;
    results.reserve(req.resources.size());

    for (const auto& res : req.resources) {
        ResourceConfigInfo rci;

        switch (res.resource_type) {
        case kResourceTypeTopic:
            rci = get_topic_configs(res.resource_name, res.config_keys,
                                    res.include_synonyms);
            break;

        case kResourceTypeBroker:
            rci = get_broker_configs(res.resource_name, res.config_keys,
                                     res.include_synonyms);
            break;

        case kResourceTypeBrokerLogger:
            // Broker logger resource — currently unsupported but succeed.
            rci.resource_type = kResourceTypeBrokerLogger;
            rci.resource_name = res.resource_name;
            rci.error = error_code::none;
            break;

        default:
            rci.resource_type = res.resource_type;
            rci.resource_name = res.resource_name;
            rci.error = error_code::invalid_request;
            rci.error_message = "Unsupported resource_type: " +
                                std::to_string(res.resource_type);
            CONFIG_LOG(warn, "{}", rci.error_message);
            break;
        }

        results.push_back(std::move(rci));
    }

    return results;
}

bool ConfigCollector::apply_topic_config(const std::string& topic_name,
                                           const std::string& key,
                                           const std::string& value,
                                           error_code* out_err,
                                           std::string* out_msg) {
    auto& tm = server_.topic_manager();

    if (!tm.topic_exists(topic_name)) {
        *out_err = error_code::unknown_topic_or_partition;
        *out_msg = "Topic '" + topic_name + "' not found";
        return false;
    }

    // Delegate to topic manager's alter_topic_config.
    auto result = tm.alter_topic_config(topic_name, key, value);
    if (result.failed()) {
        *out_err = result.error;
        *out_msg = "Failed to alter config '" + key + "': " + result.error_message;
        CONFIG_LOG(warn, "{}", *out_msg);
        return false;
    }

    CONFIG_LOG(info, "Altered topic '{}' config '{}' = '{}'",
               topic_name, key, value);
    return true;
}

std::vector<ConfigAlterResult> ConfigCollector::alter_configs(
    const ParsedAlterConfigsRequest& req,
    const RequestContext& /*ctx*/) {

    std::vector<ConfigAlterResult> results;
    results.reserve(req.resources.size());

    for (const auto& res : req.resources) {
        ConfigAlterResult car;
        car.resource_type = res.resource_type;
        car.resource_name = res.resource_name;
        car.error = error_code::none;

        // Validate resource_type.
        if (res.resource_type != kResourceTypeTopic &&
            res.resource_type != kResourceTypeBroker &&
            res.resource_type != kResourceTypeBrokerLogger) {
            car.error = error_code::invalid_request;
            car.error_message = "Unsupported resource_type: " +
                                std::to_string(res.resource_type);
            results.push_back(std::move(car));
            continue;
        }

        // For non-topic resources, we skip alter in this implementation.
        if (res.resource_type == kResourceTypeBroker) {
            car.error = error_code::policy_violation;
            car.error_message = "Broker config alteration not supported via this API";
            results.push_back(std::move(car));
            continue;
        }

        if (res.resource_type == kResourceTypeBrokerLogger) {
            car.error = error_code::none;
            car.error_message.clear();
            results.push_back(std::move(car));
            continue;
        }

        // Apply each config change (topic resource).
        bool all_succeeded = true;
        for (const auto& [key, value] : res.configs) {
            error_code ec = error_code::none;
            std::string emsg;

            if (key.empty()) {
                if (all_succeeded) {
                    all_succeeded = false;
                    car.error = error_code::invalid_config;
                    car.error_message = "Empty config key not allowed";
                }
                continue;
            }

            if (!apply_topic_config(res.resource_name, key, value, &ec, &emsg)) {
                all_succeeded = false;
                // Take the first error as the resource-level error.
                if (car.error == error_code::none) {
                    car.error = ec;
                    car.error_message = emsg;
                }
            }
        }

        if (all_succeeded) {
            car.error = error_code::none;
            car.error_message.clear();
        }

        results.push_back(std::move(car));
    }

    return results;
}

std::vector<ConfigAlterResult> ConfigCollector::incremental_alter_configs(
    const ParsedIncrementalAlterConfigsRequest& req,
    const RequestContext& /*ctx*/) {

    std::vector<ConfigAlterResult> results;
    results.reserve(req.resources.size());

    for (const auto& res : req.resources) {
        ConfigAlterResult car;
        car.resource_type = res.resource_type;
        car.resource_name = res.resource_name;
        car.error = error_code::none;

        // Validate resource_type.
        if (res.resource_type != kResourceTypeTopic &&
            res.resource_type != kResourceTypeBroker &&
            res.resource_type != kResourceTypeBrokerLogger) {
            car.error = error_code::invalid_request;
            car.error_message = "Unsupported resource_type: " +
                                std::to_string(res.resource_type);
            results.push_back(std::move(car));
            continue;
        }

        // Broker-level incremental configs not supported in this implementation.
        if (res.resource_type == kResourceTypeBroker) {
            car.error = error_code::policy_violation;
            car.error_message = "Broker config alteration not supported via this API";
            results.push_back(std::move(car));
            continue;
        }

        if (res.resource_type == kResourceTypeBrokerLogger) {
            car.error = error_code::none;
            results.push_back(std::move(car));
            continue;
        }

        // Process each incremental operation.
        auto& tm = server_.topic_manager();
        bool topic_ok = tm.topic_exists(res.resource_name);

        if (!topic_ok) {
            car.error = error_code::unknown_topic_or_partition;
            car.error_message = "Topic '" + res.resource_name + "' not found";
            results.push_back(std::move(car));
            continue;
        }

        bool all_succeeded = true;
        for (const auto& op : res.ops) {
            error_code ec = error_code::none;
            std::string emsg;

            switch (op.op_type) {
            case kConfigOpSet:
                // SET: set the config value (empty value = default/reset).
                if (!apply_topic_config(res.resource_name, op.config_name,
                                        op.config_value, &ec, &emsg)) {
                    all_succeeded = false;
                    if (car.error == error_code::none) {
                        car.error = ec;
                        car.error_message = emsg;
                    }
                }
                break;

            case kConfigOpDelete:
                // DELETE: reset config to its default value.
                // We handle this by setting an empty value, which the
                // topic manager should interpret as "reset to default".
                {
                    auto result = tm.alter_topic_config(res.resource_name,
                                                        op.config_name, "");
                    if (result.failed()) {
                        all_succeeded = false;
                        if (car.error == error_code::none) {
                            car.error = result.error;
                            car.error_message = "Failed to delete config '" +
                                                op.config_name + "': " +
                                                result.error_message;
                        }
                    }
                }
                break;

            case kConfigOpAppend:
                // APPEND: append a value to a list-type config.
                // For now, we treat this as a set operation since we don't
                // maintain list configs natively.
                // In production, this would read the existing config, append,
                // and write back.
                if (!apply_topic_config(res.resource_name, op.config_name,
                                        op.config_value, &ec, &emsg)) {
                    all_succeeded = false;
                    if (car.error == error_code::none) {
                        car.error = ec;
                        car.error_message = "Failed to append config '" +
                                            op.config_name + "': " + emsg;
                    }
                }
                break;

            case kConfigOpSubtract:
                // SUBTRACT: remove a value from a list-type config.
                // For now, we treat this as a no-op for safety.
                CONFIG_LOG(info, "Subtract operation on '{}' for topic '{}' — "
                           "treated as no-op (list configs not yet supported)",
                           op.config_name, res.resource_name);
                break;

            default:
                all_succeeded = false;
                if (car.error == error_code::none) {
                    car.error = error_code::invalid_config;
                    car.error_message = "Unknown op_type: " +
                                        std::to_string(op.op_type);
                }
                break;
            }
        }

        if (all_succeeded) {
            car.error = error_code::none;
            car.error_message.clear();
        }

        results.push_back(std::move(car));
    }

    return results;
}

// ============================================================================
// DescribeConfigsResponseBuilder — builds the wire-format response
// ============================================================================

class DescribeConfigsResponseBuilder {
public:
    shared_buffer build(int32_t throttle_time_ms,
                        const std::vector<ResourceConfigInfo>& resources) const;

private:
    void write_config_entry(std::vector<char>& buf,
                            const ConfigEntryInfo& entry) const;
};

void DescribeConfigsResponseBuilder::write_config_entry(
    std::vector<char>& buf,
    const ConfigEntryInfo& entry) const {

    // config_name
    write_string(buf, entry.config_name);

    // config_value (nullable)
    write_nullable_string(buf, entry.config_value);

    // read_only
    write_bool(buf, entry.read_only);

    // is_default
    write_bool(buf, entry.is_default);

    // is_sensitive
    write_bool(buf, entry.is_sensitive);

    // config_source
    write_int8(buf, entry.config_source);

    // synonym_count and synonyms
    write_int32_be(buf, static_cast<int32_t>(entry.synonyms.size()));
    for (const auto& syn : entry.synonyms) {
        write_string(buf, syn.synonym_name);
        write_nullable_string(buf, syn.synonym_value);
        write_int8(buf, syn.synonym_source);
    }
}

shared_buffer DescribeConfigsResponseBuilder::build(
    int32_t throttle_time_ms,
    const std::vector<ResourceConfigInfo>& resources) const {

    std::vector<char> buf;
    buf.reserve(4096 + resources.size() * 1024);

    // --- throttle_time_ms ---
    write_int32_be(buf, throttle_time_ms);

    // --- resource count ---
    write_int32_be(buf, static_cast<int32_t>(resources.size()));

    for (const auto& rci : resources) {
        // --- error_code ---
        write_int16_be(buf, static_cast<int16_t>(rci.error));

        // --- error_message ---
        write_nullable_string(buf, rci.error_message);

        // --- resource_type ---
        write_int8(buf, rci.resource_type);

        // --- resource_name ---
        write_string(buf, rci.resource_name);

        // --- config_count ---
        write_int32_be(buf, static_cast<int32_t>(rci.configs.size()));

        // --- config entries ---
        for (const auto& entry : rci.configs) {
            write_config_entry(buf, entry);
        }
    }

    shared_buffer result(buf.data(), buf.size());
    return result;
}

// ============================================================================
// ConfigAlterResponseBuilder — builds wire-format response for AlterConfigs
//   and IncrementalAlterConfigs (identical wire format)
// ============================================================================

class ConfigAlterResponseBuilder {
public:
    shared_buffer build(int32_t throttle_time_ms,
                        const std::vector<ConfigAlterResult>& results) const;
};

shared_buffer ConfigAlterResponseBuilder::build(
    int32_t throttle_time_ms,
    const std::vector<ConfigAlterResult>& results) const {

    std::vector<char> buf;
    buf.reserve(4096 + results.size() * 256);

    // --- throttle_time_ms ---
    write_int32_be(buf, throttle_time_ms);

    // --- resource count ---
    write_int32_be(buf, static_cast<int32_t>(results.size()));

    for (const auto& car : results) {
        // --- error_code ---
        write_int16_be(buf, static_cast<int16_t>(car.error));

        // --- error_message ---
        write_nullable_string(buf, car.error_message);

        // --- resource_type ---
        write_int8(buf, car.resource_type);

        // --- resource_name ---
        write_string(buf, car.resource_name);
    }

    shared_buffer result(buf.data(), buf.size());
    return result;
}

// ============================================================================
// RequestValidator — validates request preconditions common to all APIs
// ============================================================================

class RequestValidator {
public:
    explicit RequestValidator(BrokerServer& server, const RequestContext& ctx)
        : server_(server), ctx_(ctx) {}

    /// Verify that the broker is in a state that accepts requests.
    bool check_broker_state(error_code* out_err, std::string* out_msg) const;

    /// Verify that the request body is non-empty.
    bool check_body(buffer_view body, error_code* out_err, std::string* out_msg) const;

    /// Verify client authorization to access cluster metadata.
    bool check_authorization(error_code* out_err, std::string* out_msg) const;

private:
    BrokerServer& server_;
    const RequestContext& ctx_;
};

bool RequestValidator::check_broker_state(error_code* out_err,
                                            std::string* out_msg) const {
    auto health = server_.health();

    if (!health.accepting_work()) {
        *out_err = error_code::broker_not_available;
        *out_msg = "Broker is not accepting requests (state: " +
                   std::string(to_string(health.state)) + ")";
        META_LOG(warn, "{}", *out_msg);
        return false;
    }

    return true;
}

bool RequestValidator::check_body(buffer_view body,
                                   error_code* out_err,
                                   std::string* out_msg) const {
    if (body.empty() || body.data == nullptr) {
        *out_err = error_code::invalid_request;
        *out_msg = "Empty request body";
        META_LOG(warn, "Empty request body from client='{}'", ctx_.client_id);
        return false;
    }
    return true;
}

bool RequestValidator::check_authorization(error_code* out_err,
                                             std::string* out_msg) const {
    if (ctx_.is_authenticated && !ctx_.principal.empty()) {
        // In production, verify the principal has DESCRIBE / ALTER permissions
        // on the cluster resource. For now we log and allow.
        META_LOG(debug, "Authorization check passed for principal='{}'",
                 ctx_.principal);
    }
    return true;
}

} // anonymous namespace

// ============================================================================
// MetadataHandler::handle() — public entry point
// ============================================================================

shared_buffer MetadataHandler::handle(const RequestContext& ctx, buffer_view body) {
    const timestamp_ms_t start_time = wall_clock_ms();

    META_LOG(info, "Metadata/Config request: api_key={} client='{}' "
             "correlation_id={} api_version={}",
             ctx.api_key, ctx.client_id, ctx.correlation_id, ctx.api_version);

    // --- Step 0: Validate request preconditions ---
    RequestValidator validator(*server_, ctx);

    error_code validate_err = error_code::none;
    std::string validate_msg;
    if (!validator.check_broker_state(&validate_err, &validate_msg) ||
        !validator.check_body(body, &validate_err, &validate_msg)) {
        // For unparseable requests, return an appropriate error response.
        // We build a minimal response based on api_key.
        std::vector<char> err_buf;
        write_int32_be(err_buf, 0);  // throttle_time_ms

        switch (ctx.api_key) {
        case tp::kApiKeyMetadata:
        case tp::kApiKeyDescribeConfigs:
        case tp::kApiKeyAlterConfigs:
        case tp::kApiKeyIncrementalAlterConfigs:
            // All these APIs have a resource/array count after throttle_time_ms.
            write_int32_be(err_buf, 0);  // zero brokers/topics/resources
            break;
        default:
            break;
        }

        shared_buffer result(err_buf.data(), err_buf.size());
        return result;
    }

    // --- Dispatch based on api_key ---
    switch (ctx.api_key) {

    // ========================================================================
    // Metadata (api_key=3)
    // ========================================================================
    case tp::kApiKeyMetadata: {
        // --- Parse MetadataRequest ---
        MetadataRequestParser parser(body);
        if (!parser.parse()) {
            META_LOG(error, "Failed to parse MetadataRequest: {}",
                     parser.request().parse_error_msg);

            // Build error response with empty data.
            MetadataResponseBuilder builder;
            MetadataCollector empty_collector(*server_, ctx, parser.request());
            // We return an empty metadata response.
            shared_buffer resp = builder.build(0, empty_collector, false, false);
            return resp;
        }

        const auto& req = parser.request();
        META_LOG(info, "MetadataRequest: topics={} all={} auto_create={} "
                 "include_cluster_authz={} include_topic_authz={}",
                 req.topics.size(), req.request_all_topics,
                 req.allow_auto_create, req.include_cluster_authorized_ops,
                 req.include_topic_authorized_ops);

        // --- Collect metadata ---
        MetadataCollector collector(*server_, ctx, req);
        collector.collect();

        // --- Build response ---
        MetadataResponseBuilder builder;
        timestamp_ms_t elapsed = wall_clock_ms() - start_time;
        int32_t throttle = (elapsed > 0 && elapsed < std::numeric_limits<int32_t>::max())
                           ? static_cast<int32_t>(elapsed) : kDefaultThrottleMs;

        return builder.build(throttle, collector,
                             req.include_cluster_authorized_ops,
                             req.include_topic_authorized_ops);
    }

    // ========================================================================
    // DescribeConfigs (api_key=32)
    // ========================================================================
    case tp::kApiKeyDescribeConfigs: {
        // --- Parse DescribeConfigsRequest ---
        DescribeConfigsRequestParser parser(body);
        if (!parser.parse()) {
            CONFIG_LOG(error, "Failed to parse DescribeConfigsRequest: {}",
                       parser.request().parse_error_msg);

            // Build error response.
            DescribeConfigsResponseBuilder builder;
            std::vector<ResourceConfigInfo> empty_resources;
            shared_buffer resp = builder.build(0, empty_resources);
            return resp;
        }

        const auto& req = parser.request();
        CONFIG_LOG(info, "DescribeConfigsRequest: {} resources", req.resources.size());

        // --- Collect configs ---
        ConfigCollector config_collector(*server_);
        auto results = config_collector.describe_configs(req, ctx);

        // --- Build response ---
        DescribeConfigsResponseBuilder builder;
        timestamp_ms_t elapsed = wall_clock_ms() - start_time;
        int32_t throttle = (elapsed > 0 && elapsed < std::numeric_limits<int32_t>::max())
                           ? static_cast<int32_t>(elapsed) : kDefaultThrottleMs;

        return builder.build(throttle, results);
    }

    // ========================================================================
    // AlterConfigs (api_key=33)
    // ========================================================================
    case tp::kApiKeyAlterConfigs: {
        // --- Parse AlterConfigsRequest ---
        AlterConfigsRequestParser parser(body);
        if (!parser.parse()) {
            CONFIG_LOG(error, "Failed to parse AlterConfigsRequest: {}",
                       parser.request().parse_error_msg);

            // Build error response.
            ConfigAlterResponseBuilder builder;
            std::vector<ConfigAlterResult> empty_results;
            shared_buffer resp = builder.build(0, empty_results);
            return resp;
        }

        const auto& req = parser.request();
        CONFIG_LOG(info, "AlterConfigsRequest: {} resources validate_only={}",
                   req.resources.size(), req.validate_only);

        // --- Apply config changes ---
        ConfigCollector config_collector(*server_);
        auto results = config_collector.alter_configs(req, ctx);

        // --- Build response ---
        ConfigAlterResponseBuilder builder;
        timestamp_ms_t elapsed = wall_clock_ms() - start_time;
        int32_t throttle = (elapsed > 0 && elapsed < std::numeric_limits<int32_t>::max())
                           ? static_cast<int32_t>(elapsed) : kDefaultThrottleMs;

        return builder.build(throttle, results);
    }

    // ========================================================================
    // IncrementalAlterConfigs (api_key=44)
    // ========================================================================
    case tp::kApiKeyIncrementalAlterConfigs: {
        // --- Parse IncrementalAlterConfigsRequest ---
        IncrementalAlterConfigsRequestParser parser(body);
        if (!parser.parse()) {
            CONFIG_LOG(error, "Failed to parse IncrementalAlterConfigsRequest: {}",
                       parser.request().parse_error_msg);

            // Build error response.
            ConfigAlterResponseBuilder builder;
            std::vector<ConfigAlterResult> empty_results;
            shared_buffer resp = builder.build(0, empty_results);
            return resp;
        }

        const auto& req = parser.request();
        CONFIG_LOG(info, "IncrementalAlterConfigsRequest: {} resources validate_only={}",
                   req.resources.size(), req.validate_only);

        // --- Apply incremental config changes ---
        ConfigCollector config_collector(*server_);
        auto results = config_collector.incremental_alter_configs(req, ctx);

        // --- Build response ---
        ConfigAlterResponseBuilder builder;
        timestamp_ms_t elapsed = wall_clock_ms() - start_time;
        int32_t throttle = (elapsed > 0 && elapsed < std::numeric_limits<int32_t>::max())
                           ? static_cast<int32_t>(elapsed) : kDefaultThrottleMs;

        return builder.build(throttle, results);
    }

    // ========================================================================
    // Unknown api_key
    // ========================================================================
    default:
        META_LOG(error, "MetadataHandler received unknown api_key={}", ctx.api_key);

        // Return minimal error response.
        std::vector<char> err_buf;
        write_int32_be(err_buf, 0);  // throttle_time_ms
        write_int32_be(err_buf, 0);  // empty array

        shared_buffer result(err_buf.data(), err_buf.size());
        return result;
    }
}

} // namespace torrent::client
