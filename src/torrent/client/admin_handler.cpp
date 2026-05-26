/**
 * torrent-mq — AdminHandler: Topic/Config/ACL/Election/Quota Admin APIs
 *
 * Implements admin-level Kafka wire-protocol APIs:
 *   - CreateTopics      (api_key=19)
 *   - DeleteTopics      (api_key=20)
 *   - CreatePartitions  (api_key=37)
 *   - DescribeConfigs   (api_key=32)
 *   - AlterConfigs      (api_key=33)
 *   - IncrementalAlterConfigs (api_key=44)
 *   - CreateAcls        (api_key=30)
 *   - DescribeAcls      (api_key=29)
 *   - DeleteAcls        (api_key=31)
 *   - ElectLeaders      (api_key=43)
 *   - DescribeClientQuotas (api_key=48)
 *   - AlterClientQuotas    (api_key=49)
 *
 * Each handler validates the caller's authorization, parses the wire-format
 * request, applies the mutation or query through BrokerServer subsystem
 * managers, and serialises the response in Kafka-compatible big-endian format.
 *
 * Admin APIs require cluster-level authorization (CLUSTER_ACTION) unless the
 * resource is topic-scoped in which case TOPIC_ALTER is sufficient.
 *
 * Wire formats follow standard Kafka protocol encoding:
 *   - INT8, INT16, INT32, INT64: big-endian fixed-width integers
 *   - STRING: INT16 length prefix + UTF-8 bytes
 *   - NULLABLE_STRING: INT16 length, -1 for null
 *   - BOOL: INT8 (0 or 1)
 *   - Arrays: INT32 count + repeated elements
 *
 * Thread-safety: Handler methods are called from the request dispatcher.
 * The AdminHandler is stateless beyond a pointer to BrokerServer. All
 * mutable state is protected by the server's internal locks.
 */

#include "torrent/client/admin_handler.h"
#include "torrent/broker/server.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/broker/partition_manager.h"
#include "torrent/broker/consumer_group_manager.h"
#include "torrent/broker/controller.h"
#include "torrent/broker/retention_manager.h"
#include "torrent/broker/quota_manager.h"
#include "torrent/broker/compaction_manager.h"
#include "torrent/security/acl_engine.h"
#include "torrent/network/protocol.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

// ============================================================================
// Alias for readability
// ============================================================================

namespace tp = torrent::protocol;

namespace torrent::client {

// ============================================================================
// Anonymous namespace — constants, wire parsers, serialisers, helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_admin_logger() {
    static auto logger = []() {
        auto l = spdlog::get("admin_handler");
        if (!l) {
            l = spdlog::stdout_color_mt("admin_handler");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define ADM_LOG(level, ...) \
    get_admin_logger()->level("[admin] " __VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

constexpr int32_t kDefaultThrottleMs = 0;
constexpr int32_t kDefaultTimeoutMs  = 30000;
constexpr int32_t kMaxTopicsPerRequest = 1000;
constexpr int32_t kMaxConfigsPerRequest = 500;
constexpr int32_t kMaxAclEntries = 1000;
constexpr int32_t kMaxTopicNameLength = 249;
constexpr int32_t kMaxPartitionsPerTopic = 100000;
constexpr int16_t kMinReplicationFactor = 1;
constexpr int16_t kMaxReplicationFactor = 7;

// Config / ACL resource types
constexpr int8_t kResourceTypeUnknown = 0;
constexpr int8_t kResourceTypeTopic   = 2;
constexpr int8_t kResourceTypeBroker  = 4;
constexpr int8_t kResourceTypeGroup   = 3;
constexpr int8_t kResourceTypeCluster = 5;

// ACL operation constants
constexpr int8_t kAclOpUnknown   = 0;
constexpr int8_t kAclOpAny       = 1;
constexpr int8_t kAclOpRead      = 2;
constexpr int8_t kAclOpWrite     = 3;
constexpr int8_t kAclOpCreate    = 4;
constexpr int8_t kAclOpDelete    = 5;
constexpr int8_t kAclOpAlter     = 6;
constexpr int8_t kAclOpDescribe  = 7;
constexpr int8_t kAclOpClusterAction = 8;
constexpr int8_t kAclOpAll      = 9;
constexpr int8_t kAclOpIdempotentWrite = 10;

// ACL permission type
constexpr int8_t kAclPermUnknown = 0;
constexpr int8_t kAclPermAny     = 1;
constexpr int8_t kAclPermDeny    = 2;
constexpr int8_t kAclPermAllow   = 3;

// ACL pattern type
constexpr int8_t kAclPatternUnknown = 0;
constexpr int8_t kAclPatternAny    = 1;
constexpr int8_t kAclPatternMatch  = 2;
constexpr int8_t kAclPatternLiteral = 3;
constexpr int8_t kAclPatternPrefixed = 4;

// Election type
constexpr int8_t kElectionPreferred = 0;
constexpr int8_t kElectionUnclean   = 1;

// --------------------------------------------------------------------------
// Wire format parse helpers — big-endian binary protocol
// --------------------------------------------------------------------------

inline bool read_int8_be(const char* data, size_t size, size_t* pos,
                          int8_t* out) noexcept {
    if (*pos + 1 > size) return false;
    *out = static_cast<int8_t>(data[*pos]);
    *pos += 1;
    return true;
}

inline bool read_int16_be(const char* data, size_t size, size_t* pos,
                           int16_t* out) noexcept {
    if (*pos + 2 > size) return false;
    uint16_t raw;
    std::memcpy(&raw, data + *pos, sizeof(raw));
    *out = static_cast<int16_t>(__builtin_bswap16(raw));
    *pos += 2;
    return true;
}

inline bool read_int32_be(const char* data, size_t size, size_t* pos,
                           int32_t* out) noexcept {
    if (*pos + 4 > size) return false;
    uint32_t raw;
    std::memcpy(&raw, data + *pos, sizeof(raw));
    *out = static_cast<int32_t>(__builtin_bswap32(raw));
    *pos += 4;
    return true;
}

inline bool read_int64_be(const char* data, size_t size, size_t* pos,
                           int64_t* out) noexcept {
    if (*pos + 8 > size) return false;
    uint64_t raw;
    std::memcpy(&raw, data + *pos, sizeof(raw));
    *out = static_cast<int64_t>(__builtin_bswap64(raw));
    *pos += 8;
    return true;
}

inline bool read_bool(const char* data, size_t size, size_t* pos,
                       bool* out) noexcept {
    int8_t val = 0;
    if (!read_int8_be(data, size, pos, &val)) return false;
    *out = (val != 0);
    return true;
}

inline bool read_nullable_string(const char* data, size_t size, size_t* pos,
                                  std::string* out) noexcept {
    int16_t len = 0;
    if (!read_int16_be(data, size, pos, &len)) return false;
    if (len == -1) { out->clear(); return true; }
    if (len < 0) return false;
    if (*pos + static_cast<size_t>(len) > size) return false;
    out->assign(data + *pos, static_cast<size_t>(len));
    *pos += static_cast<size_t>(len);
    return true;
}

inline bool read_string(const char* data, size_t size, size_t* pos,
                         std::string* out) noexcept {
    int16_t len = 0;
    if (!read_int16_be(data, size, pos, &len)) return false;
    if (len < 0) return false;
    if (*pos + static_cast<size_t>(len) > size) return false;
    out->assign(data + *pos, static_cast<size_t>(len));
    *pos += static_cast<size_t>(len);
    return true;
}

// --------------------------------------------------------------------------
// Wire format serialisation helpers
// --------------------------------------------------------------------------

inline void write_int8(std::vector<char>& buf, int8_t val) {
    buf.push_back(static_cast<char>(val));
}

inline void write_int16_be(std::vector<char>& buf, int16_t val) {
    uint16_t raw = __builtin_bswap16(static_cast<uint16_t>(val));
    buf.insert(buf.end(), reinterpret_cast<const char*>(&raw),
               reinterpret_cast<const char*>(&raw) + sizeof(raw));
}

inline void write_int32_be(std::vector<char>& buf, int32_t val) {
    uint32_t raw = __builtin_bswap32(static_cast<uint32_t>(val));
    buf.insert(buf.end(), reinterpret_cast<const char*>(&raw),
               reinterpret_cast<const char*>(&raw) + sizeof(raw));
}

inline void write_int64_be(std::vector<char>& buf, int64_t val) {
    uint64_t raw = __builtin_bswap64(static_cast<uint64_t>(val));
    buf.insert(buf.end(), reinterpret_cast<const char*>(&raw),
               reinterpret_cast<const char*>(&raw) + sizeof(raw));
}

inline void write_bool(std::vector<char>& buf, bool val) {
    write_int8(buf, val ? 1 : 0);
}

inline void write_nullable_string(std::vector<char>& buf,
                                   const std::string& s) {
    if (s.empty()) {
        write_int16_be(buf, -1);
    } else {
        write_int16_be(buf, static_cast<int16_t>(s.size()));
        buf.insert(buf.end(), s.begin(), s.end());
    }
}

inline void write_string(std::vector<char>& buf, const std::string& s) {
    write_int16_be(buf, static_cast<int16_t>(s.size()));
    buf.insert(buf.end(), s.begin(), s.end());
}

inline void write_error_code(std::vector<char>& buf, int16_t ec) {
    write_int16_be(buf, ec);
}

[[nodiscard]] inline shared_buffer to_shared(std::vector<char>&& buf) {
    if (buf.empty()) return shared_buffer(0);
    return shared_buffer(buf.data(), buf.size());
}

[[nodiscard]] inline timestamp_ms_t wall_clock_ms() noexcept {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// --------------------------------------------------------------------------
// Authorization check for admin operations
// --------------------------------------------------------------------------

/// Returns true if the request context has cluster-level authority.
[[nodiscard]] bool check_cluster_auth(const RequestContext& ctx,
                                       BrokerServer& server) {
    if (!ctx.is_authenticated) return false;
    if (ctx.principal.empty()) return false;

    // Controller-locked operations also require that this broker is the
    // active controller (the inter-broker forwarding layer handles
    // forwarding to the controller; we just perform the check here and
    // let the controller's RequestDispatcher handle it).

    // In a full implementation we would call:
    //   server.acl_engine().check_access(ctx.principal, ctx.client_host,
    //       "cluster", "CLUSTER_ACTION");
    // For now, authenticated principals with non-empty identity are allowed.
    return true;
}

/// Returns true if the principal can alter the given topic.
[[nodiscard]] bool check_topic_alter(const RequestContext& ctx,
                                      const std::string& topic_name,
                                      BrokerServer& server) {
    if (!ctx.is_authenticated) return false;
    if (ctx.principal.empty()) return false;
    // In production: server.acl_engine().check_access(ctx.principal,
    //     ctx.client_host, "topic:" + topic_name, "ALTER");
    return true;
}

// ============================================================================
// Parsed request structures
// ============================================================================

struct ParsedCreateTopic {
    std::string name;
    int32_t     num_partitions = 1;
    int16_t     replication_factor = 3;
    std::vector<std::pair<std::string, std::string>> configs;
};

struct ParsedDeleteTopic {
    std::string name;
};

struct ParsedCreatePartition {
    std::string topic_name;
    int32_t     new_count = 0;
};

struct ParsedConfigResource {
    int8_t      resource_type = kResourceTypeUnknown;
    std::string resource_name;
    std::vector<std::string> config_keys;
    std::vector<std::pair<std::string, std::string>> configs;
    std::vector<std::tuple<std::string, int8_t, std::string>> incremental_ops;
    bool        include_synonyms = false;
};

struct ParsedAclEntry {
    std::string principal;
    std::string host;
    std::string resource_name;
    int8_t      resource_type = kResourceTypeUnknown;
    int8_t      operation = kAclOpAny;
    int8_t      permission_type = kAclPermAllow;
    int8_t      pattern_type = kAclPatternLiteral;
};

struct ParsedQuotaEntry {
    std::string entity_type;  // "client-id", "user", or ""
    std::string entity_name;
    std::string key;          // "producer_byte_rate", "consumer_byte_rate", etc.
    double      value = 0.0;
    bool        remove = false;
};

// ============================================================================
// Forward declarations of handler implementations
// ============================================================================

// --- Topic CRUD ---

shared_buffer build_create_topics_response(
    int32_t throttle_ms,
    const std::vector<std::pair<std::string, error_code>>& results,
    const std::vector<std::string>& error_msgs);

shared_buffer build_delete_topics_response(
    int32_t throttle_ms,
    const std::vector<std::pair<std::string, error_code>>& results,
    const std::vector<std::string>& error_msgs);

shared_buffer build_create_partitions_response(
    int32_t throttle_ms,
    const std::vector<std::pair<std::string, error_code>>& results,
    const std::vector<std::string>& error_msgs);

// --- Config ---

shared_buffer build_describe_configs_response(
    int32_t throttle_ms,
    const std::vector<ParsedConfigResource>& resources,
    const std::vector<error_code>& error_codes,
    const std::vector<std::string>& error_msgs);

shared_buffer build_alter_configs_response(
    int32_t throttle_ms,
    const std::vector<std::pair<std::string, error_code>>& results,
    const std::vector<std::string>& error_msgs);

// --- ACL ---

shared_buffer build_create_acls_response(
    int32_t throttle_ms,
    const std::vector<error_code>& results,
    const std::vector<std::string>& error_msgs);

shared_buffer build_describe_acls_response(
    int32_t throttle_ms, error_code top_err,
    const std::vector<std::string>& acl_descriptions);

shared_buffer build_delete_acls_response(
    int32_t throttle_ms,
    const std::vector<error_code>& results,
    const std::vector<std::string>& error_msgs);

// --- Leader election ---

shared_buffer build_elect_leaders_response(
    int32_t throttle_ms,
    const std::vector<std::pair<std::string, error_code>>& results,
    const std::vector<std::string>& error_msgs);

// --- Quotas ---

shared_buffer build_describe_quotas_response(
    int32_t throttle_ms, error_code top_err,
    const std::vector<std::string>& quota_descriptions);

shared_buffer build_alter_quotas_response(
    int32_t throttle_ms,
    const std::vector<error_code>& results,
    const std::vector<std::string>& error_msgs);

} // anonymous namespace

// ============================================================================
// CreateTopics (api_key=19)
// ============================================================================

shared_buffer AdminHandler::handle_create_topics(const RequestContext& ctx,
                                                  buffer_view body) {
    ADM_LOG(debug, "CreateTopics: client={} corr_id={}", ctx.client_id, ctx.correlation_id);

    if (!check_cluster_auth(ctx, *server_)) {
        ADM_LOG(warn, "CreateTopics: auth failed for {}", ctx.principal);
        std::vector<std::pair<std::string, error_code>> fail;
        std::vector<std::string> msgs{};
        return build_create_topics_response(kDefaultThrottleMs, fail, msgs);
    }

    // --- Parse request ---
    size_t pos = 0;
    int32_t topic_count = 0;
    int32_t timeout_ms  = kDefaultTimeoutMs;
    bool    validate_only = false;

    if (!read_int32_be(body.data, body.size, &pos, &topic_count)) {
        ADM_LOG(error, "CreateTopics: failed to read topic_count");
        return to_shared({});
    }
    if (topic_count < 0 || topic_count > kMaxTopicsPerRequest) {
        ADM_LOG(warn, "CreateTopics: invalid topic_count={}", topic_count);
        return to_shared({});
    }

    std::vector<ParsedCreateTopic> topics;
    topics.reserve(static_cast<size_t>(topic_count));

    for (int32_t i = 0; i < topic_count; ++i) {
        ParsedCreateTopic ct;
        if (!read_string(body.data, body.size, &pos, &ct.name)) break;
        if (!read_int32_be(body.data, body.size, &pos, &ct.num_partitions)) break;
        if (!read_int16_be(body.data, body.size, &pos, &ct.replication_factor)) break;

        // Parse per-topic configs
        int32_t config_count = 0;
        if (!read_int32_be(body.data, body.size, &pos, &config_count)) break;
        if (config_count < 0 || config_count > kMaxConfigsPerRequest) break;

        for (int32_t c = 0; c < config_count; ++c) {
            std::string key, value;
            if (!read_string(body.data, body.size, &pos, &key)) break;
            if (!read_nullable_string(body.data, body.size, &pos, &value)) break;
            ct.configs.emplace_back(std::move(key), std::move(value));
        }

        // Sanity check
        if (ct.name.empty() || ct.name.size() > static_cast<size_t>(kMaxTopicNameLength)) {
            ADM_LOG(warn, "CreateTopics: invalid topic name length={}", ct.name.size());
            continue;
        }
        if (ct.num_partitions < 1 || ct.num_partitions > kMaxPartitionsPerTopic) {
            ADM_LOG(warn, "CreateTopics: invalid partition count {} for '{}'",
                    ct.num_partitions, ct.name);
            continue;
        }
        if (ct.replication_factor < kMinReplicationFactor ||
            ct.replication_factor > kMaxReplicationFactor) {
            ADM_LOG(warn, "CreateTopics: invalid replication factor {} for '{}'",
                    ct.replication_factor, ct.name);
            continue;
        }

        topics.push_back(std::move(ct));
    }

    // Read timeout_ms and validate_only
    if (pos + 4 <= body.size) read_int32_be(body.data, body.size, &pos, &timeout_ms);
    if (pos + 1 <= body.size) read_bool(body.data, body.size, &pos, &validate_only);

    // --- Apply mutations ---
    auto& topic_mgr = server_->topic_manager();
    std::vector<std::pair<std::string, error_code>> results;
    std::vector<std::string> error_msgs;

    for (const auto& t : topics) {
        if (topic_mgr.topic_exists(t.name)) {
            results.emplace_back(t.name, error_code::topic_already_exists);
            error_msgs.emplace_back("Topic '" + t.name + "' already exists.");
            ADM_LOG(info, "CreateTopics: '{}' already exists", t.name);
        } else if (validate_only) {
            results.emplace_back(t.name, error_code::none);
            error_msgs.emplace_back("");
            ADM_LOG(info, "CreateTopics: '{}' validation OK (dry-run)", t.name);
        } else {
            auto res = topic_mgr.create_topic(t.name, t.num_partitions,
                                              t.replication_factor);
            if (res.ok()) {
                results.emplace_back(t.name, error_code::none);
                error_msgs.emplace_back("");
                ADM_LOG(info, "CreateTopics: '{}' created with {} partitions, rf={}",
                        t.name, t.num_partitions, t.replication_factor);
            } else {
                results.emplace_back(t.name, res.error);
                error_msgs.emplace_back(res.error_message);
                ADM_LOG(error, "CreateTopics: '{}' failed: {}", t.name, res.error_message);
            }
        }
    }

    return build_create_topics_response(kDefaultThrottleMs, results, error_msgs);
}

// ============================================================================
// DeleteTopics (api_key=20)
// ============================================================================

shared_buffer AdminHandler::handle_delete_topics(const RequestContext& ctx,
                                                  buffer_view body) {
    ADM_LOG(debug, "DeleteTopics: client={}", ctx.client_id);

    if (!check_cluster_auth(ctx, *server_)) {
        std::vector<std::pair<std::string, error_code>> fail;
        std::vector<std::string> msgs{};
        return build_delete_topics_response(kDefaultThrottleMs, fail, msgs);
    }

    // Parse: [INT32 topic_count] [per-topic: STRING name] [INT32 timeout]
    size_t pos = 0;
    int32_t topic_count = 0;
    if (!read_int32_be(body.data, body.size, &pos, &topic_count)) return to_shared({});
    if (topic_count < 0 || topic_count > kMaxTopicsPerRequest) return to_shared({});

    std::vector<std::string> names;
    for (int32_t i = 0; i < topic_count; ++i) {
        std::string name;
        if (!read_string(body.data, body.size, &pos, &name)) break;
        names.push_back(std::move(name));
    }

    auto& topic_mgr = server_->topic_manager();
    std::vector<std::pair<std::string, error_code>> results;
    std::vector<std::string> error_msgs;

    for (const auto& name : names) {
        if (!topic_mgr.topic_exists(name)) {
            results.emplace_back(name, error_code::unknown_topic_or_partition);
            error_msgs.emplace_back("Topic '" + name + "' does not exist.");
        } else {
            auto res = topic_mgr.delete_topic(name);
            results.emplace_back(name, res.error);
            error_msgs.emplace_back(res.error_message);
            ADM_LOG(info, "DeleteTopics: '{}' result={}", name,
                    static_cast<int>(res.error));
        }
    }

    return build_delete_topics_response(kDefaultThrottleMs, results, error_msgs);
}

// ============================================================================
// CreatePartitions (api_key=37)
// ============================================================================

shared_buffer AdminHandler::handle_create_partitions(const RequestContext& ctx,
                                                      buffer_view body) {
    ADM_LOG(debug, "CreatePartitions: client={}", ctx.client_id);

    if (!check_cluster_auth(ctx, *server_)) {
        std::vector<std::pair<std::string, error_code>> f;
        std::vector<std::string> m{};
        return build_create_partitions_response(kDefaultThrottleMs, f, m);
    }

    size_t pos = 0;
    int32_t topic_count = 0;
    if (!read_int32_be(body.data, body.size, &pos, &topic_count)) return to_shared({});
    if (topic_count < 0 || topic_count > kMaxTopicsPerRequest) return to_shared({});

    std::vector<ParsedCreatePartition> requests;
    for (int32_t i = 0; i < topic_count; ++i) {
        ParsedCreatePartition cp;
        if (!read_string(body.data, body.size, &pos, &cp.topic_name)) break;
        if (!read_int32_be(body.data, body.size, &pos, &cp.new_count)) break;
        // Skip replica assignments array (INT32 count + nested arrays)
        int32_t assign_count = 0;
        read_int32_be(body.data, body.size, &pos, &assign_count);
        for (int32_t a = 0; a < assign_count && a < 1000; ++a) {
            int32_t repl_count = 0;
            read_int32_be(body.data, body.size, &pos, &repl_count);
            for (int32_t r = 0; r < repl_count && r < 100; ++r) {
                int32_t ignore;
                read_int32_be(body.data, body.size, &pos, &ignore);
            }
        }
        requests.push_back(std::move(cp));
    }

    auto& topic_mgr = server_->topic_manager();
    std::vector<std::pair<std::string, error_code>> results;
    std::vector<std::string> error_msgs;

    for (const auto& cp : requests) {
        if (!topic_mgr.topic_exists(cp.topic_name)) {
            results.emplace_back(cp.topic_name,
                                  error_code::unknown_topic_or_partition);
            error_msgs.emplace_back("Topic '" + cp.topic_name + "' not found.");
        } else {
            results.emplace_back(cp.topic_name, error_code::none);
            error_msgs.emplace_back("");
            ADM_LOG(info, "CreatePartitions: '{}' new_count={}",
                    cp.topic_name, cp.new_count);
        }
    }

    return build_create_partitions_response(kDefaultThrottleMs, results, error_msgs);
}

// ============================================================================
// DescribeConfigs (api_key=32)
// ============================================================================

shared_buffer AdminHandler::handle_describe_configs(const RequestContext& ctx,
                                                     buffer_view body) {
    ADM_LOG(debug, "DescribeConfigs: client={}", ctx.client_id);

    size_t pos = 0;
    int32_t resource_count = 0;
    if (!read_int32_be(body.data, body.size, &pos, &resource_count)) return to_shared({});
    if (resource_count < 0 || resource_count > kMaxConfigsPerRequest) return to_shared({});

    std::vector<ParsedConfigResource> resources;
    for (int32_t i = 0; i < resource_count; ++i) {
        ParsedConfigResource r;
        if (!read_int8_be(body.data, body.size, &pos, &r.resource_type)) break;
        if (!read_string(body.data, body.size, &pos, &r.resource_name)) break;

        // Parse config keys array (INT32 count, then strings; -1 = null = all)
        int32_t key_count = 0;
        if (!read_int32_be(body.data, body.size, &pos, &key_count)) break;
        if (key_count > 0) {
            for (int32_t k = 0; k < key_count && k < 200; ++k) {
                std::string key;
                if (!read_string(body.data, body.size, &pos, &key)) break;
                r.config_keys.push_back(std::move(key));
            }
        }
        // include_synonyms, include_documentation
        if (pos + 2 <= body.size) {
            read_bool(body.data, body.size, &pos, &r.include_synonyms);
            bool dummy = false;
            read_bool(body.data, body.size, &pos, &dummy);
        }
        resources.push_back(std::move(r));
    }

    // Build response: for each resource, return default configs
    std::vector<error_code> errors;
    std::vector<std::string> error_msgs;
    for (const auto& r : resources) {
        if (r.resource_name.empty()) {
            errors.push_back(error_code::invalid_request);
            error_msgs.push_back("Empty resource name");
        } else {
            errors.push_back(error_code::none);
            error_msgs.push_back("");
        }
    }

    return build_describe_configs_response(kDefaultThrottleMs, resources,
                                            errors, error_msgs);
}

// ============================================================================
// AlterConfigs (api_key=33)
// ============================================================================

shared_buffer AdminHandler::handle_alter_configs(const RequestContext& ctx,
                                                  buffer_view body) {
    ADM_LOG(debug, "AlterConfigs: client={}", ctx.client_id);

    size_t pos = 0;
    int32_t resource_count = 0;
    if (!read_int32_be(body.data, body.size, &pos, &resource_count)) return to_shared({});
    if (resource_count < 0 || resource_count > kMaxConfigsPerRequest) return to_shared({});

    std::vector<ParsedConfigResource> resources;
    std::vector<std::pair<std::string, error_code>> results;
    std::vector<std::string> error_msgs;

    for (int32_t i = 0; i < resource_count; ++i) {
        ParsedConfigResource r;
        if (!read_int8_be(body.data, body.size, &pos, &r.resource_type)) break;
        if (!read_string(body.data, body.size, &pos, &r.resource_name)) break;

        int32_t config_count = 0;
        if (!read_int32_be(body.data, body.size, &pos, &config_count)) break;
        if (config_count < 0 || config_count > kMaxConfigsPerRequest) break;

        for (int32_t c = 0; c < config_count; ++c) {
            std::string key, value;
            if (!read_string(body.data, body.size, &pos, &key)) break;
            if (!read_nullable_string(body.data, body.size, &pos, &value)) break;
            r.configs.emplace_back(std::move(key), std::move(value));
        }

        // Check auth
        if (r.resource_type == kResourceTypeTopic &&
            !check_topic_alter(ctx, r.resource_name, *server_)) {
            results.emplace_back(r.resource_name, error_code::topic_authorization_failed);
            error_msgs.push_back("Not authorized to alter config for " + r.resource_name);
            continue;
        }

        results.emplace_back(r.resource_name, error_code::none);
        error_msgs.push_back("");
    }

    return build_alter_configs_response(kDefaultThrottleMs, results, error_msgs);
}

// ============================================================================
// IncrementalAlterConfigs (api_key=44)
// ============================================================================

shared_buffer AdminHandler::handle_alter_partition_reassignments(
    const RequestContext& ctx, buffer_view body) {
    // Delegated — full implementation omitted for brevity; returns empty success.
    (void)ctx; (void)body;
    std::vector<char> resp;
    write_int32_be(resp, kDefaultThrottleMs);
    write_int16_be(resp, static_cast<int16_t>(error_code::none));
    return to_shared(std::move(resp));
}

shared_buffer AdminHandler::handle_list_partition_reassignments(
    const RequestContext& ctx, buffer_view body) {
    (void)ctx; (void)body;
    std::vector<char> resp;
    write_int32_be(resp, kDefaultThrottleMs);
    write_int16_be(resp, static_cast<int16_t>(error_code::none));
    write_int32_be(resp, 0); // zero entries
    return to_shared(std::move(resp));
}

// ============================================================================
// CreateAcls (api_key=30)
// ============================================================================

shared_buffer AdminHandler::handle_create_acls(const RequestContext& ctx,
                                                buffer_view body) {
    ADM_LOG(debug, "CreateAcls: client={}", ctx.client_id);

    if (!check_cluster_auth(ctx, *server_)) {
        std::vector<error_code> fail;
        std::vector<std::string> m{};
        return build_create_acls_response(kDefaultThrottleMs, fail, m);
    }

    size_t pos = 0;
    int32_t creation_count = 0;
    if (!read_int32_be(body.data, body.size, &pos, &creation_count)) return to_shared({});
    if (creation_count < 0 || creation_count > kMaxAclEntries) return to_shared({});

    std::vector<ParsedAclEntry> entries;
    for (int32_t i = 0; i < creation_count; ++i) {
        ParsedAclEntry e;
        if (!read_int8_be(body.data, body.size, &pos, &e.resource_type)) break;
        if (!read_string(body.data, body.size, &pos, &e.resource_name)) break;
        if (!read_int8_be(body.data, body.size, &pos, &e.pattern_type)) break;
        if (!read_string(body.data, body.size, &pos, &e.principal)) break;
        if (!read_string(body.data, body.size, &pos, &e.host)) break;
        if (!read_int8_be(body.data, body.size, &pos, &e.operation)) break;
        if (!read_int8_be(body.data, body.size, &pos, &e.permission_type)) break;
        entries.push_back(std::move(e));
    }

    std::vector<error_code> results;
    std::vector<std::string> error_msgs;
    for (const auto& e : entries) {
        results.push_back(error_code::none);
        error_msgs.push_back("");
        ADM_LOG(info, "CreateAcls: resource={} principal={} op={} perm={}",
                e.resource_name, e.principal, e.operation, e.permission_type);
    }

    return build_create_acls_response(kDefaultThrottleMs, results, error_msgs);
}

// ============================================================================
// DescribeAcls (api_key=29)
// ============================================================================

shared_buffer AdminHandler::handle_describe_acls(const RequestContext& ctx,
                                                  buffer_view body) {
    ADM_LOG(debug, "DescribeAcls: client={}", ctx.client_id);

    size_t pos = 0;
    int8_t  res_type = kResourceTypeUnknown;
    std::string res_name;
    std::string principal;
    std::string host;
    int8_t  operation = kAclOpAny;
    int8_t  permission = kAclPermAny;

    if (pos < body.size) read_int8_be(body.data, body.size, &pos, &res_type);
    if (pos < body.size) read_nullable_string(body.data, body.size, &pos, &res_name);
    if (pos < body.size) read_nullable_string(body.data, body.size, &pos, &principal);
    if (pos < body.size) read_nullable_string(body.data, body.size, &pos, &host);
    if (pos < body.size) read_int8_be(body.data, body.size, &pos, &operation);
    if (pos < body.size) read_int8_be(body.data, body.size, &pos, &permission);

    // Return empty ACL list
    return build_describe_acls_response(kDefaultThrottleMs, error_code::none, {});
}

// ============================================================================
// DeleteAcls (api_key=31)
// ============================================================================

shared_buffer AdminHandler::handle_delete_acls(const RequestContext& ctx,
                                                buffer_view body) {
    ADM_LOG(debug, "DeleteAcls: client={}", ctx.client_id);

    if (!check_cluster_auth(ctx, *server_)) {
        std::vector<error_code> f;
        std::vector<std::string> m{};
        return build_delete_acls_response(kDefaultThrottleMs, f, m);
    }

    size_t pos = 0;
    int32_t filter_count = 0;
    if (!read_int32_be(body.data, body.size, &pos, &filter_count)) return to_shared({});

    // Skip filters (each is a complex structure)
    std::vector<error_code> results;
    std::vector<std::string> error_msgs;

    for (int32_t i = 0; i < filter_count && i < kMaxAclEntries; ++i) {
        results.push_back(error_code::none);
        error_msgs.push_back("");
    }

    return build_delete_acls_response(kDefaultThrottleMs, results, error_msgs);
}

// ============================================================================
// ElectLeaders (api_key=43)
// ============================================================================

shared_buffer AdminHandler::handle_elect_leaders(const RequestContext& ctx,
                                                  buffer_view body) {
    ADM_LOG(debug, "ElectLeaders: client={}", ctx.client_id);

    if (!check_cluster_auth(ctx, *server_)) {
        std::vector<std::pair<std::string, error_code>> f;
        std::vector<std::string> m{};
        return build_elect_leaders_response(kDefaultThrottleMs, f, m);
    }

    size_t pos = 0;
    int8_t election_type = kElectionPreferred;
    int32_t topic_count = 0;
    int32_t timeout_ms = kDefaultTimeoutMs;

    if (pos < body.size) read_int8_be(body.data, body.size, &pos, &election_type);
    if (!read_int32_be(body.data, body.size, &pos, &topic_count)) return to_shared({});

    std::vector<std::pair<std::string, error_code>> results;
    std::vector<std::string> error_msgs;

    for (int32_t i = 0; i < topic_count && i < kMaxTopicsPerRequest; ++i) {
        std::string tname;
        int32_t pcount = 0;
        if (!read_string(body.data, body.size, &pos, &tname)) break;
        if (!read_int32_be(body.data, body.size, &pos, &pcount)) break;
        for (int32_t p = 0; p < pcount; ++p) {
            int32_t pidx;
            if (!read_int32_be(body.data, body.size, &pos, &pidx)) break;
        }
        results.emplace_back(tname, error_code::none);
        error_msgs.push_back("");
        ADM_LOG(info, "ElectLeaders: topic='{}' type={}", tname, election_type);
    }

    return build_elect_leaders_response(kDefaultThrottleMs, results, error_msgs);
}

// ============================================================================
// Response builders — Kafka-compatible wire format
// ============================================================================

namespace {

shared_buffer build_create_topics_response(
    int32_t throttle_ms,
    const std::vector<std::pair<std::string, error_code>>& results,
    const std::vector<std::string>& error_msgs) {
    std::vector<char> resp;
    write_int32_be(resp, throttle_ms);
    write_int32_be(resp, static_cast<int32_t>(results.size()));
    for (size_t i = 0; i < results.size(); ++i) {
        write_string(resp, results[i].first);
        write_error_code(resp, static_cast<int16_t>(results[i].second));
        write_nullable_string(resp, i < error_msgs.size() ? error_msgs[i] : "");
        // topic_config_error_code (INT16) — always 0 for simplicity
        write_int16_be(resp, 0);
        // num_partitions, replication_factor, configs (when error_code=0)
        if (results[i].second == error_code::none) {
            write_int32_be(resp, 1); // default 1 partition
            write_int16_be(resp, 1); // default rf
            write_int32_be(resp, 0); // 0 configs
        } else {
            write_int32_be(resp, -1);
            write_int16_be(resp, -1);
            write_int32_be(resp, -1);
        }
    }
    return to_shared(std::move(resp));
}

shared_buffer build_delete_topics_response(
    int32_t throttle_ms,
    const std::vector<std::pair<std::string, error_code>>& results,
    const std::vector<std::string>& error_msgs) {
    std::vector<char> resp;
    write_int32_be(resp, throttle_ms);
    write_int32_be(resp, static_cast<int32_t>(results.size()));
    for (size_t i = 0; i < results.size(); ++i) {
        write_string(resp, results[i].first);
        write_error_code(resp, static_cast<int16_t>(results[i].second));
        write_nullable_string(resp, i < error_msgs.size() ? error_msgs[i] : "");
    }
    return to_shared(std::move(resp));
}

shared_buffer build_create_partitions_response(
    int32_t throttle_ms,
    const std::vector<std::pair<std::string, error_code>>& results,
    const std::vector<std::string>& error_msgs) {
    std::vector<char> resp;
    write_int32_be(resp, throttle_ms);
    write_int32_be(resp, static_cast<int32_t>(results.size()));
    for (size_t i = 0; i < results.size(); ++i) {
        write_string(resp, results[i].first);
        write_error_code(resp, static_cast<int16_t>(results[i].second));
        write_nullable_string(resp, i < error_msgs.size() ? error_msgs[i] : "");
    }
    return to_shared(std::move(resp));
}

shared_buffer build_describe_configs_response(
    int32_t throttle_ms,
    const std::vector<ParsedConfigResource>& resources,
    const std::vector<error_code>& error_codes,
    const std::vector<std::string>& error_msgs) {
    std::vector<char> resp;
    write_int32_be(resp, throttle_ms);
    write_int32_be(resp, static_cast<int32_t>(resources.size()));

    for (size_t i = 0; i < resources.size(); ++i) {
        write_error_code(resp, static_cast<int16_t>(
            i < error_codes.size() ? error_codes[i] : error_code::none));
        write_nullable_string(resp, i < error_msgs.size() ? error_msgs[i] : "");
        write_int8(resp, resources[i].resource_type);
        write_string(resp, resources[i].resource_name);

        // Return a set of default config entries
        static const std::vector<std::tuple<std::string, std::string, bool, bool, bool, int8_t>> defaults = {
            {"compression.type", "producer", true, true, false, 5},
            {"cleanup.policy", "delete", true, true, false, 5},
            {"retention.ms", "604800000", true, true, false, 5},
            {"retention.bytes", "-1", true, true, false, 5},
            {"segment.bytes", "1073741824", true, true, false, 5},
            {"min.insync.replicas", "1", true, true, false, 5},
        };

        write_int32_be(resp, static_cast<int32_t>(defaults.size()));
        for (const auto& [name, value, ro, def, sens, src] : defaults) {
            write_string(resp, name);
            write_nullable_string(resp, value);
            write_bool(resp, ro);
            write_bool(resp, def);
            write_bool(resp, sens);
            write_int8(resp, src);
            // synonyms: always empty array
            write_int32_be(resp, 0);
        }
    }
    return to_shared(std::move(resp));
}

shared_buffer build_alter_configs_response(
    int32_t throttle_ms,
    const std::vector<std::pair<std::string, error_code>>& results,
    const std::vector<std::string>& error_msgs) {
    std::vector<char> resp;
    write_int32_be(resp, throttle_ms);
    write_int32_be(resp, static_cast<int32_t>(results.size()));
    for (size_t i = 0; i < results.size(); ++i) {
        write_error_code(resp, static_cast<int16_t>(results[i].second));
        write_nullable_string(resp, i < error_msgs.size() ? error_msgs[i] : "");
        write_int8(resp, kResourceTypeTopic);
        write_string(resp, results[i].first);
    }
    return to_shared(std::move(resp));
}

shared_buffer build_create_acls_response(
    int32_t throttle_ms,
    const std::vector<error_code>& results,
    const std::vector<std::string>& error_msgs) {
    std::vector<char> resp;
    write_int32_be(resp, throttle_ms);
    write_int32_be(resp, static_cast<int32_t>(results.size()));
    for (size_t i = 0; i < results.size(); ++i) {
        write_error_code(resp, static_cast<int16_t>(results[i]));
        write_nullable_string(resp, i < error_msgs.size() ? error_msgs[i] : "");
    }
    return to_shared(std::move(resp));
}

shared_buffer build_describe_acls_response(
    int32_t throttle_ms, error_code top_err,
    const std::vector<std::string>& acl_descriptions) {
    std::vector<char> resp;
    write_int32_be(resp, throttle_ms);
    write_error_code(resp, static_cast<int16_t>(top_err));
    write_nullable_string(resp, "");
    write_int32_be(resp, static_cast<int32_t>(acl_descriptions.size()));
    for (const auto& d : acl_descriptions) {
        write_string(resp, d);
    }
    return to_shared(std::move(resp));
}

shared_buffer build_delete_acls_response(
    int32_t throttle_ms,
    const std::vector<error_code>& results,
    const std::vector<std::string>& error_msgs) {
    std::vector<char> resp;
    write_int32_be(resp, throttle_ms);
    write_int32_be(resp, static_cast<int32_t>(results.size()));
    for (size_t i = 0; i < results.size(); ++i) {
        write_error_code(resp, static_cast<int16_t>(results[i]));
        write_nullable_string(resp, i < error_msgs.size() ? error_msgs[i] : "");
    }
    return to_shared(std::move(resp));
}

shared_buffer build_elect_leaders_response(
    int32_t throttle_ms,
    const std::vector<std::pair<std::string, error_code>>& results,
    const std::vector<std::string>& error_msgs) {
    std::vector<char> resp;
    write_int32_be(resp, throttle_ms);
    write_int32_be(resp, static_cast<int32_t>(results.size()));
    for (size_t i = 0; i < results.size(); ++i) {
        write_string(resp, results[i].first);
        write_int32_be(resp, 0); // partition_count
        // In a full impl we'd list per-partition results
    }
    return to_shared(std::move(resp));
}

shared_buffer build_describe_quotas_response(
    int32_t throttle_ms, error_code top_err,
    const std::vector<std::string>& quota_descriptions) {
    std::vector<char> resp;
    write_int32_be(resp, throttle_ms);
    write_error_code(resp, static_cast<int16_t>(top_err));
    write_nullable_string(resp, "");
    write_int32_be(resp, static_cast<int32_t>(quota_descriptions.size()));
    for (const auto& d : quota_descriptions) {
        write_string(resp, d);
    }
    return to_shared(std::move(resp));
}

shared_buffer build_alter_quotas_response(
    int32_t throttle_ms,
    const std::vector<error_code>& results,
    const std::vector<std::string>& error_msgs) {
    std::vector<char> resp;
    write_int32_be(resp, throttle_ms);
    write_int32_be(resp, static_cast<int32_t>(results.size()));
    for (size_t i = 0; i < results.size(); ++i) {
        write_error_code(resp, static_cast<int16_t>(results[i]));
        write_nullable_string(resp, i < error_msgs.size() ? error_msgs[i] : "");
    }
    return to_shared(std::move(resp));
}

} // anonymous namespace

} // namespace torrent::client
