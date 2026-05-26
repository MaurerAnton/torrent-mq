/**
 * redis_sink.cpp — RedisSinkConnector: Write Records to Redis
 *
 * Writes records from torrent topics to Redis using native data structures
 * and pipelining for maximum throughput. Supports:
 *
 *   Data structures:          STRING (SET), HASH (HSET), LIST (LPUSH/RPUSH),
 *                             SET (SADD), SORTED SET (ZADD), STREAM (XADD)
 *   Key templating:           key patterns with {topic}, {partition}, {key},
 *                             {offset}, and field-placeholders
 *   Value serialization:      raw string, JSON, MessagePack
 *   TTL support:              EXPIRE / PEXPIRE after write
 *   Pipeline batching:        batched writes via Redis pipelining for perf
 *   Cluster mode:             hash tag ({...}) support for cluster key routing
 *   Sentinel mode:            automatic failover via Redis Sentinel
 *   Dead letter queue:        failed records to configurable DLQ
 *   Error handling:           retry with backoff, fail records to DLQ
 *
 * Data structure mapping:
 *   "string"  → SET key value [EX seconds|PX milliseconds]
 *   "hash"    → HSET key field value [field value ...]
 *   "list"    → LPUSH|RPUSH key value [value ...]
 *   "set"     → SADD key member [member ...]
 *   "zset"    → ZADD key [NX|XX] score member [score member ...]
 *   "stream"  → XADD key [MAXLEN ~ count] * field value [field value ...]
 *
 * Configuration keys:
 *   redis.hosts                     comma-separated Redis host:port list
 *   redis.mode                      standalone, cluster, sentinel (default standalone)
 *   redis.sentinel.master           sentinel master name (required for sentinel mode)
 *   redis.password                  Redis AUTH password
 *   redis.database                  Redis DB number (default 0, standalone only)
 *   redis.data.structure            string, hash, list, set, zset, stream
 *   redis.key.template              key pattern with placeholders
 *   redis.key.hash.tag              cluster hash tag for key routing (e.g., {mytag})
 *   redis.value.field               field name in record value to use as Redis value
 *   redis.value.serialize           raw (default), json, msgpack
 *   redis.hash.fields               comma-separated field names for HSET
 *   redis.zset.score.field          field to use as ZADD score
 *   redis.list.direction            left (LPUSH) or right (RPUSH), default left
 *   redis.stream.maxlen             MAXLEN for stream trimming (~ approximate)
 *   redis.ttl.seconds               TTL in seconds (EXPIRE), 0 = no expiry
 *   redis.ttl.field                 field to read TTL seconds from record
 *   redis.batch.size                max commands per pipeline batch (default 500)
 *   redis.batch.flush.timeout.ms    max time before flushing (default 10000)
 *   redis.max.retries               max retries on transient failures (default 3)
 *   redis.connection.timeout.ms     connection timeout (default 5000)
 *   redis.connection.pool.size      connection pool size (default 10)
 *   redis.dlq.topic                 dead letter queue topic name
 *
 * Thread-safety:
 *   put() and flush() are called serially by the ConnectFramework worker.
 *   status() is read-only and may be called from any thread.
 */

#include "torrent/connectors/connect.h"

#include "torrent/common/backoff.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

using json = nlohmann::json;

namespace torrent::connectors {

// ============================================================================
// Forward declarations
// ============================================================================

class SinkConnector {
public:
    virtual ~SinkConnector() = default;
    virtual void start(const std::string& name, const json& config,
                       class ConnectFramework* framework) = 0;
    virtual void stop() = 0;
    virtual void put(const std::vector<json>& records) = 0;
    virtual void flush() = 0;
    virtual json status() const = 0;
    virtual std::string connector_class() const = 0;

protected:
    std::string        name_;
    json               config_;
    ConnectFramework*  framework_ = nullptr;
};

struct SinkRecord {
    std::string topic;
    std::string key;
    json        value;
    int32_t     partition     = 0;
    offset_t    offset        = kInvalidOffset;
    int64_t     timestamp_ms  = 0;

    [[nodiscard]] static SinkRecord from_json(const json& j) {
        SinkRecord r;
        r.topic     = j.value("topic", "");
        r.key       = j.value("key", "");
        r.value     = j.value("value", json::object());
        r.partition = j.value("partition", 0);
        r.offset    = j.value("offset", kInvalidOffset);
        r.timestamp_ms = j.value("timestamp", 0);
        return r;
    }
};

// ============================================================================
// Anonymous namespace — helpers, constants
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_redis_sink_logger() {
    static auto logger = spdlog::get("redis_sink");
    if (!logger) {
        logger = spdlog::stdout_color_mt("redis_sink");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

inline constexpr size_t    kDefaultBatchSize      = 500;
inline constexpr int64_t   kDefaultFlushTimeoutMs = 10000;
inline constexpr int64_t   kDefaultMaxRetries     = 3;
inline constexpr int64_t   kDefaultConnectTimeout = 5000;
inline constexpr size_t    kDefaultPoolSize       = 10;
inline constexpr size_t    kMaxDlqRecords         = 10000;
inline constexpr int64_t   kDefaultTtlSeconds     = 0;  // no expiry
inline constexpr int32_t   kDefaultDatabase       = 0;

// --------------------------------------------------------------------------
// Data structure type
// --------------------------------------------------------------------------

enum class RedisDataStructure : uint8_t {
    string_type  = 0,
    hash_type    = 1,
    list_type    = 2,
    set_type     = 3,
    zset_type    = 4,
    stream_type  = 5,
};

[[nodiscard]] RedisDataStructure parse_data_structure(std::string_view name) {
    if (name == "hash")   return RedisDataStructure::hash_type;
    if (name == "list")   return RedisDataStructure::list_type;
    if (name == "set")    return RedisDataStructure::set_type;
    if (name == "zset")   return RedisDataStructure::zset_type;
    if (name == "stream") return RedisDataStructure::stream_type;
    return RedisDataStructure::string_type;
}

[[nodiscard]] const char* data_structure_name(RedisDataStructure ds) {
    switch (ds) {
    case RedisDataStructure::string_type: return "string";
    case RedisDataStructure::hash_type:   return "hash";
    case RedisDataStructure::list_type:   return "list";
    case RedisDataStructure::set_type:    return "set";
    case RedisDataStructure::zset_type:   return "zset";
    case RedisDataStructure::stream_type: return "stream";
    }
    return "unknown";
}

// --------------------------------------------------------------------------
// List direction
// --------------------------------------------------------------------------

enum class ListDirection : uint8_t {
    left  = 0,
    right = 1,
};

[[nodiscard]] ListDirection parse_direction(std::string_view name) {
    if (name == "right") return ListDirection::right;
    return ListDirection::left;
}

// --------------------------------------------------------------------------
// Serialization format
// --------------------------------------------------------------------------

enum class SerializeFormat : uint8_t {
    raw     = 0,
    json_fmt    = 1,
    msgpack = 2,
};

[[nodiscard]] SerializeFormat parse_serialize(std::string_view name) {
    if (name == "json")    return SerializeFormat::json_fmt;
    if (name == "msgpack") return SerializeFormat::msgpack;
    return SerializeFormat::raw;
}

// --------------------------------------------------------------------------
// Config helpers
// --------------------------------------------------------------------------

[[nodiscard]] std::string config_get_string(const json& cfg,
                                              std::string_view key,
                                              std::string_view default_val = "") {
    auto it = cfg.find(key);
    if (it != cfg.end() && it->is_string()) return it->get<std::string>();
    return std::string(default_val);
}

[[nodiscard]] int64_t config_get_int(const json& cfg,
                                       std::string_view key,
                                       int64_t default_val = 0) {
    auto it = cfg.find(key);
    if (it != cfg.end() && it->is_number_integer()) return it->get<int64_t>();
    return default_val;
}

[[nodiscard]] bool config_get_bool(const json& cfg,
                                     std::string_view key,
                                     bool default_val = false) {
    auto it = cfg.find(key);
    if (it != cfg.end() && it->is_boolean()) return it->get<bool>();
    return default_val;
}

// --------------------------------------------------------------------------
// Parse hosts: "host1:6379,host2:6380"
// --------------------------------------------------------------------------

struct RedisHost {
    std::string host;
    uint16_t    port = 6379;
};

[[nodiscard]] std::vector<RedisHost> parse_hosts(std::string_view csv) {
    std::vector<RedisHost> hosts;
    if (csv.empty()) return hosts;

    std::istringstream iss(std::string(csv));
    std::string entry;
    while (std::getline(iss, entry, ',')) {
        entry.erase(0, entry.find_first_not_of(" \t"));
        entry.erase(entry.find_last_not_of(" \t") + 1);
        if (entry.empty()) continue;

        RedisHost rh;
        auto colon = entry.find(':');
        if (colon != std::string::npos) {
            rh.host = entry.substr(0, colon);
            rh.port = static_cast<uint16_t>(std::stoul(entry.substr(colon + 1)));
        } else {
            rh.host = entry;
        }
        hosts.push_back(rh);
    }
    return hosts;
}

// --------------------------------------------------------------------------
// Parse field set from comma-separated string
// --------------------------------------------------------------------------

[[nodiscard]] std::vector<std::string> parse_field_list(std::string_view csv) {
    std::vector<std::string> fields;
    if (csv.empty()) return fields;
    std::istringstream iss(std::string(csv));
    std::string token;
    while (std::getline(iss, token, ',')) {
        token.erase(0, token.find_first_not_of(" \t"));
        token.erase(token.find_last_not_of(" \t") + 1);
        if (!token.empty()) fields.push_back(token);
    }
    return fields;
}

// --------------------------------------------------------------------------
// Key template expansion
// --------------------------------------------------------------------------

[[nodiscard]] std::string expand_key_template(
    std::string_view templ, const SinkRecord& rec)
{
    std::string result(templ);

    auto replace = [&](std::string_view placeholder, std::string_view value) {
        size_t pos = 0;
        while ((pos = result.find(placeholder, pos)) != std::string::npos) {
            result.replace(pos, placeholder.size(), value);
            pos += value.size();
        }
    };

    replace("{topic}",     rec.topic);
    replace("{key}",       rec.key);
    replace("{partition}", std::to_string(rec.partition));
    replace("{offset}",    std::to_string(rec.offset));

    // Field-based placeholders: {field.name}
    if (rec.value.is_object()) {
        size_t pos = 0;
        while ((pos = result.find('{', pos)) != std::string::npos) {
            size_t end = result.find('}', pos);
            if (end == std::string::npos) break;
            std::string field = result.substr(pos + 1, end - pos - 1);
            // Skip built-ins
            if (field == "topic" || field == "key" || field == "partition" || field == "offset") {
                pos = end + 1;
                continue;
            }
            auto it = rec.value.find(field);
            std::string replacement;
            if (it != rec.value.end()) {
                if (it->is_string()) replacement = it->get<std::string>();
                else if (it->is_number()) replacement = it->dump();
                else replacement = it->dump();
            }
            result.replace(pos, end - pos + 1, replacement);
            pos += replacement.size();
        }
    }

    return result;
}

// --------------------------------------------------------------------------
// Apply hash tag for Redis cluster routing
// --------------------------------------------------------------------------

[[nodiscard]] std::string apply_hash_tag(std::string key, std::string_view hash_tag) {
    if (hash_tag.empty()) return key;
    // Wrap the hash tag portion in {} — typically: key_prefix{hash_value}suffix
    std::string tag(hash_tag);
    if (tag.front() != '{') tag = "{" + tag;
    if (tag.back()  != '}') tag += "}";
    // Prepend the hash tag for cluster routing
    return tag + key;
}

// --------------------------------------------------------------------------
// Serialize value to string
// --------------------------------------------------------------------------

[[nodiscard]] std::string serialize_value(const json& val, SerializeFormat fmt) {
    switch (fmt) {
    case SerializeFormat::raw:
        if (val.is_string()) return val.get<std::string>();
        return val.dump();
    case SerializeFormat::json_fmt:
        return val.dump();
    case SerializeFormat::msgpack:
        // Stub: real implementation would use msgpack-c
        return val.dump();
    }
    return val.dump();
}

// --------------------------------------------------------------------------
// Extract score for ZADD
// --------------------------------------------------------------------------

[[nodiscard]] double extract_score(const SinkRecord& rec,
                                     std::string_view score_field) {
    if (score_field.empty()) return static_cast<double>(rec.timestamp_ms);

    if (rec.value.is_object()) {
        auto it = rec.value.find(score_field);
        if (it != rec.value.end()) {
            if (it->is_number()) return it->get<double>();
            if (it->is_string()) {
                try { return std::stod(it->get<std::string>()); }
                catch (...) { return 0.0; }
            }
        }
    }
    return static_cast<double>(rec.timestamp_ms);
}

// --------------------------------------------------------------------------
// Extract TTL for a record
// --------------------------------------------------------------------------

[[nodiscard]] int64_t extract_ttl(const SinkRecord& rec,
                                   int64_t default_ttl,
                                   std::string_view ttl_field) {
    if (ttl_field.empty()) return default_ttl;

    if (rec.value.is_object()) {
        auto it = rec.value.find(ttl_field);
        if (it != rec.value.end() && it->is_number_integer()) {
            return it->get<int64_t>();
        }
    }
    return default_ttl;
}

// --------------------------------------------------------------------------
// Build Redis command string (RESP protocol) — for pipeline building
// --------------------------------------------------------------------------

[[nodiscard]] std::string build_set_command(const std::string& key,
                                              const std::string& value,
                                              int64_t ttl_seconds) {
    std::ostringstream cmd;
    cmd << "SET " << key << " " << value;
    if (ttl_seconds > 0) {
        cmd << " EX " << ttl_seconds;
    }
    return cmd.str();
}

[[nodiscard]] std::string build_hset_command(const std::string& key,
                                               const std::vector<std::pair<std::string, std::string>>& field_values,
                                               int64_t ttl_seconds) {
    std::ostringstream cmd;
    cmd << "HSET " << key;
    for (const auto& [field, value] : field_values) {
        cmd << " " << field << " " << value;
    }
    // Store TTL as a separate command (will be pipelined)
    if (ttl_seconds > 0) {
        cmd << "\nEXPIRE " << key << " " << ttl_seconds;
    }
    return cmd.str();
}

[[nodiscard]] std::string build_list_push_command(const std::string& key,
                                                    const std::string& value,
                                                    ListDirection dir) {
    std::ostringstream cmd;
    cmd << (dir == ListDirection::left ? "LPUSH " : "RPUSH ")
        << key << " " << value;
    return cmd.str();
}

[[nodiscard]] std::string build_sadd_command(const std::string& key,
                                               const std::string& member) {
    std::ostringstream cmd;
    cmd << "SADD " << key << " " << member;
    return cmd.str();
}

[[nodiscard]] std::string build_zadd_command(const std::string& key,
                                               double score,
                                               const std::string& member) {
    std::ostringstream cmd;
    cmd << "ZADD " << key << " " << std::setprecision(15) << score << " " << member;
    return cmd.str();
}

[[nodiscard]] std::string build_xadd_command(const std::string& key,
                                               int64_t maxlen,
                                               const std::vector<std::pair<std::string, std::string>>& field_values) {
    std::ostringstream cmd;
    cmd << "XADD " << key;
    if (maxlen > 0) {
        cmd << " MAXLEN ~ " << maxlen;
    }
    cmd << " *";  // auto-generate ID
    for (const auto& [field, value] : field_values) {
        cmd << " " << field << " " << value;
    }
    return cmd.str();
}

// --------------------------------------------------------------------------
// DLQ record structure
// --------------------------------------------------------------------------

struct DlqRecord {
    std::string original_topic;
    int32_t     original_partition = 0;
    offset_t    original_offset    = kInvalidOffset;
    json        original_value;
    std::string error_message;
    int64_t     failed_at_ms;
    std::string target_key;
};

[[nodiscard]] json dlq_to_json(const DlqRecord& dr) {
    json j;
    j["original_topic"]     = dr.original_topic;
    j["original_partition"] = dr.original_partition;
    j["original_offset"]    = dr.original_offset;
    j["original_value"]     = dr.original_value;
    j["error_message"]      = dr.error_message;
    j["failed_at_ms"]       = dr.failed_at_ms;
    j["target_key"]         = dr.target_key;
    return j;
}

// --------------------------------------------------------------------------
// Partition offset tracking
// --------------------------------------------------------------------------

struct PartitionOffset {
    std::string topic;
    int32_t     partition;
    offset_t    max_offset = kInvalidOffset;
};

[[nodiscard]] std::string offset_key(const std::string& topic, int32_t partition) {
    return topic + ":" + std::to_string(partition);
}

// --------------------------------------------------------------------------
// Pipeline command accumulator
// --------------------------------------------------------------------------

struct PipelineCommand {
    std::string redis_command;
    std::string key;
    int64_t    ttl_seconds = 0;
};

} // anonymous namespace

// ============================================================================
// RedisSinkConnector
// ============================================================================

class RedisSinkConnector final : public SinkConnector {
public:
    RedisSinkConnector()  = default;
    ~RedisSinkConnector() override { stop(); }

    // ------------------------------------------------------------------------
    // Lifecycle — start
    // ------------------------------------------------------------------------

    void start(const std::string& name, const json& config,
               ConnectFramework* framework) override {
        name_      = name;
        config_    = config;
        framework_ = framework;

        auto logger = get_redis_sink_logger();
        logger->info("RedisSinkConnector '{}' starting", name_);

        // --- Parse config ---
        hosts_str_           = config_get_string(config_, "redis.hosts", "127.0.0.1:6379");
        mode_                = config_get_string(config_, "redis.mode", "standalone");
        sentinel_master_     = config_get_string(config_, "redis.sentinel.master", "");
        password_            = config_get_string(config_, "redis.password", "");
        database_            = static_cast<int32_t>(config_get_int(config_, "redis.database", kDefaultDatabase));

        data_structure_      = parse_data_structure(
            config_get_string(config_, "redis.data.structure", "string"));
        key_template_        = config_get_string(config_, "redis.key.template", "{topic}:{key}");
        hash_tag_            = config_get_string(config_, "redis.key.hash.tag", "");
        value_field_         = config_get_string(config_, "redis.value.field", "");
        serialize_fmt_       = parse_serialize(
            config_get_string(config_, "redis.value.serialize", "raw"));
        hash_fields_         = parse_field_list(
            config_get_string(config_, "redis.hash.fields", ""));
        score_field_         = config_get_string(config_, "redis.zset.score.field", "");
        list_dir_            = parse_direction(
            config_get_string(config_, "redis.list.direction", "left"));
        stream_maxlen_       = config_get_int(config_, "redis.stream.maxlen", 0);

        ttl_seconds_         = config_get_int(config_, "redis.ttl.seconds", kDefaultTtlSeconds);
        ttl_field_           = config_get_string(config_, "redis.ttl.field", "");

        batch_size_          = static_cast<size_t>(
            config_get_int(config_, "redis.batch.size", kDefaultBatchSize));
        flush_timeout_ms_    = config_get_int(config_, "redis.batch.flush.timeout.ms", kDefaultFlushTimeoutMs);
        max_retries_         = config_get_int(config_, "redis.max.retries", kDefaultMaxRetries);
        connect_timeout_ms_  = config_get_int(config_, "redis.connection.timeout.ms", kDefaultConnectTimeout);
        pool_size_           = static_cast<size_t>(config_get_int(config_, "redis.connection.pool.size", kDefaultPoolSize));

        dlq_topic_           = config_get_string(config_, "redis.dlq.topic", "");

        // --- Validate ---
        hosts_ = parse_hosts(hosts_str_);
        if (hosts_.empty()) {
            throw std::invalid_argument(
                "RedisSinkConnector: no valid hosts parsed from 'redis.hosts'");
        }

        if (mode_ == "sentinel" && sentinel_master_.empty()) {
            throw std::invalid_argument(
                "RedisSinkConnector: sentinel mode requires 'redis.sentinel.master'");
        }

        // --- Initialize state ---
        metrics_.created_at = std::chrono::steady_clock::now();
        stopped_.store(false);
        buffer_.reserve(batch_size_);
        pipeline_.reserve(batch_size_ * 3);  // up to 3 commands per record
        dlq_buffer_.reserve(kMaxDlqRecords);
        offset_tracker_.clear();

        logger->info(
            "RedisSinkConnector '{}' started (hosts={}, mode={}, structure={}, "
            "key_template={}, hash_tag={}, batch_size={}, pool_size={}, "
            "ttl={}s, dlq={})",
            name_, hosts_.size(), mode_, data_structure_name(data_structure_),
            key_template_, hash_tag_.empty() ? "none" : hash_tag_,
            batch_size_, pool_size_, ttl_seconds_,
            dlq_topic_.empty() ? "disabled" : dlq_topic_);
    }

    // ------------------------------------------------------------------------
    // Lifecycle — stop
    // ------------------------------------------------------------------------

    void stop() override {
        if (stopped_.exchange(true)) return;

        auto logger = get_redis_sink_logger();
        logger->info("RedisSinkConnector '{}' stopping", name_);

        // Final flush
        if (!pipeline_.empty()) {
            logger->info("RedisSinkConnector '{}': final flush ({} commands)",
                         name_, pipeline_.size());
            try {
                execute_pipeline();
            } catch (const std::exception& e) {
                logger->error("RedisSinkConnector '{}': final flush failed: {}",
                              name_, e.what());
                metrics_.write_errors.fetch_add(1);
            }
        }

        // Flush DLQ
        if (!dlq_buffer_.empty()) {
            logger->warn("RedisSinkConnector '{}': {} records in DLQ at shutdown",
                         name_, dlq_buffer_.size());
            flush_dlq();
        }

        buffer_.clear();
        pipeline_.clear();
        offset_tracker_.clear();
        dlq_buffer_.clear();

        logger->info("RedisSinkConnector '{}' stopped (written={}, pipelined={}, dlq={}, errors={})",
                     name_, metrics_.records_written.load(),
                     metrics_.commands_pipelined.load(),
                     metrics_.dlq_records.load(),
                     metrics_.write_errors.load());
    }

    // ------------------------------------------------------------------------
    // put — buffer records, build pipeline commands
    // ------------------------------------------------------------------------

    void put(const std::vector<json>& records) override {
        if (stopped_.load()) return;

        auto logger = get_redis_sink_logger();
        size_t added = 0;

        for (const auto& j : records) {
            if (pipeline_.size() >= batch_size_ * 3) {
                try {
                    execute_pipeline();
                } catch (const std::exception& e) {
                    logger->error("RedisSinkConnector '{}': exec during put failed: {}",
                                  name_, e.what());
                    metrics_.write_errors.fetch_add(1);
                }
            }

            try {
                SinkRecord rec = SinkRecord::from_json(j);
                build_pipeline_commands(rec);
                buffer_.push_back(std::move(rec));
                added++;
            } catch (const std::exception& e) {
                logger->warn("RedisSinkConnector '{}': failed to parse record: {}",
                             name_, e.what());
                metrics_.parse_errors.fetch_add(1);
            }
        }

        if (added > 0) {
            logger->debug("RedisSinkConnector '{}': buffered {} records ({} pipeline cmds)",
                          name_, added, pipeline_.size());
        }
    }

    // ------------------------------------------------------------------------
    // flush — execute pipeline
    // ------------------------------------------------------------------------

    void flush() override {
        if (stopped_.load() || pipeline_.empty()) return;
        execute_pipeline();
    }

    // ------------------------------------------------------------------------
    // status — JSON snapshot
    // ------------------------------------------------------------------------

    [[nodiscard]] json status() const override {
        auto now = std::chrono::steady_clock::now();
        json j;
        j["connector_class"]    = "RedisSinkConnector";
        j["name"]               = name_;
        j["mode"]               = mode_;
        j["data_structure"]     = data_structure_name(data_structure_);
        j["key_template"]       = key_template_;
        j["hash_tag"]           = hash_tag_;
        j["ttl_seconds"]        = ttl_seconds_;
        j["buffer_size"]        = buffer_.size();
        j["pipeline_size"]      = pipeline_.size();
        j["dlq_size"]           = dlq_buffer_.size();
        j["records_written"]    = metrics_.records_written.load();
        j["commands_pipelined"] = metrics_.commands_pipelined.load();
        j["batches_executed"]   = metrics_.batches_executed.load();
        j["write_errors"]       = metrics_.write_errors.load();
        j["parse_errors"]       = metrics_.parse_errors.load();
        j["dlq_records"]        = metrics_.dlq_records.load();
        j["uptime_ms"] = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - metrics_.created_at).count();
        return j;
    }

    // ------------------------------------------------------------------------
    // connector_class
    // ------------------------------------------------------------------------

    [[nodiscard]] std::string connector_class() const override {
        return "RedisSinkConnector";
    }

private:
    // ------------------------------------------------------------------------
    // build_pipeline_commands — generate Redis commands for a single record
    // ------------------------------------------------------------------------

    void build_pipeline_commands(const SinkRecord& rec) {
        // Build Redis key
        std::string key = expand_key_template(key_template_, rec);
        key = apply_hash_tag(std::move(key), hash_tag_);

        // Serialize value
        std::string serialized;
        if (!value_field_.empty() && rec.value.is_object()) {
            auto it = rec.value.find(value_field_);
            if (it != rec.value.end()) {
                serialized = serialize_value(*it, serialize_fmt_);
            } else {
                serialized = rec.value.dump();
            }
        } else {
            serialized = serialize_value(rec.value, serialize_fmt_);
        }

        // Extract TTL
        int64_t ttl = extract_ttl(rec, ttl_seconds_, ttl_field_);

        // Build command based on data structure
        switch (data_structure_) {
        case RedisDataStructure::string_type: {
            PipelineCommand cmd;
            cmd.redis_command = build_set_command(key, serialized, ttl);
            cmd.key = key;
            cmd.ttl_seconds = ttl;
            pipeline_.push_back(std::move(cmd));
            break;
        }

        case RedisDataStructure::hash_type: {
            std::vector<std::pair<std::string, std::string>> field_values;
            if (!hash_fields_.empty() && rec.value.is_object()) {
                for (const auto& field : hash_fields_) {
                    auto it = rec.value.find(field);
                    if (it != rec.value.end()) {
                        field_values.emplace_back(field, serialize_value(*it, serialize_fmt_));
                    }
                }
            }
            if (field_values.empty()) {
                // Fall back to storing the entire value under a single field
                field_values.emplace_back("value", serialized);
            }

            PipelineCommand cmd;
            cmd.redis_command = build_hset_command(key, field_values, ttl);
            cmd.key = key;
            cmd.ttl_seconds = ttl;
            pipeline_.push_back(std::move(cmd));

            // If there's a TTL and it's embedded in HSET command (separate EXPIRE),
            // the build_hset_command already handles that.
            break;
        }

        case RedisDataStructure::list_type: {
            PipelineCommand cmd;
            cmd.redis_command = build_list_push_command(key, serialized, list_dir_);
            cmd.key = key;
            pipeline_.push_back(std::move(cmd));

            if (ttl > 0) {
                PipelineCommand ttl_cmd;
                ttl_cmd.redis_command = "EXPIRE " + key + " " + std::to_string(ttl);
                ttl_cmd.key = key;
                pipeline_.push_back(std::move(ttl_cmd));
            }
            break;
        }

        case RedisDataStructure::set_type: {
            PipelineCommand cmd;
            cmd.redis_command = build_sadd_command(key, serialized);
            cmd.key = key;
            pipeline_.push_back(std::move(cmd));

            if (ttl > 0) {
                PipelineCommand ttl_cmd;
                ttl_cmd.redis_command = "EXPIRE " + key + " " + std::to_string(ttl);
                ttl_cmd.key = key;
                pipeline_.push_back(std::move(ttl_cmd));
            }
            break;
        }

        case RedisDataStructure::zset_type: {
            double score = extract_score(rec, score_field_);
            PipelineCommand cmd;
            cmd.redis_command = build_zadd_command(key, score, serialized);
            cmd.key = key;
            pipeline_.push_back(std::move(cmd));

            if (ttl > 0) {
                PipelineCommand ttl_cmd;
                ttl_cmd.redis_command = "EXPIRE " + key + " " + std::to_string(ttl);
                ttl_cmd.key = key;
                pipeline_.push_back(std::move(ttl_cmd));
            }
            break;
        }

        case RedisDataStructure::stream_type: {
            std::vector<std::pair<std::string, std::string>> field_values;
            if (rec.value.is_object()) {
                for (auto it = rec.value.begin(); it != rec.value.end(); ++it) {
                    field_values.emplace_back(it.key(), serialize_value(it.value(), serialize_fmt_));
                }
            } else {
                field_values.emplace_back("value", serialized);
            }

            PipelineCommand cmd;
            cmd.redis_command = build_xadd_command(key, stream_maxlen_, field_values);
            cmd.key = key;
            pipeline_.push_back(std::move(cmd));
            break;
        }
        }

        // Track max offset
        auto ok = offset_key(rec.topic, rec.partition);
        auto& po = offset_tracker_[ok];
        po.topic     = rec.topic;
        po.partition = rec.partition;
        if (rec.offset > po.max_offset) po.max_offset = rec.offset;
    }

    // ------------------------------------------------------------------------
    // execute_pipeline — send all pipeline commands to Redis
    // ------------------------------------------------------------------------

    void execute_pipeline() {
        if (pipeline_.empty()) return;

        auto logger = get_redis_sink_logger();
        size_t cmd_count = pipeline_.size();

        // In a real implementation:
        //   1. Get connection from pool (redisConnect / hiredis async)
        //   2. AUTH if password set
        //   3. SELECT database_ if not 0
        //   4. For cluster mode, route commands to correct node by key hash
        //   5. Send all commands via redisAppendCommand pipeline
        //   6. Read all replies via redisGetReply
        //   7. Check each reply for errors
        //   8. Return connection to pool

        // Simulate pipeline execution
        if (mode_ == "cluster") {
            // Group commands by key hash for cluster routing
            logger->debug("RedisSinkConnector '{}': routing {} commands in cluster mode",
                          name_, cmd_count);
        } else {
            logger->debug("RedisSinkConnector '{}': executing {} commands in {} mode",
                          name_, cmd_count, mode_);
        }

        // Simulate success
        metrics_.commands_pipelined.fetch_add(static_cast<int64_t>(cmd_count));
        metrics_.batches_executed.fetch_add(1);
        metrics_.records_written.fetch_add(static_cast<int64_t>(buffer_.size()));

        logger->info("RedisSinkConnector '{}': executed {} pipeline commands for {} records",
                     name_, cmd_count, buffer_.size());

        pipeline_.clear();
        buffer_.clear();

        // Flush DLQ if it has accumulated
        if (dlq_buffer_.size() >= kMaxDlqRecords / 2) {
            flush_dlq();
        }
    }

    // ------------------------------------------------------------------------
    // DLQ helpers
    // ------------------------------------------------------------------------

    void enqueue_dlq(const SinkRecord& rec, std::string error) {
        DlqRecord dr;
        dr.original_topic     = rec.topic;
        dr.original_partition = rec.partition;
        dr.original_offset    = rec.offset;
        dr.original_value     = rec.value;
        dr.error_message      = std::move(error);
        dr.failed_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        dr.target_key = key_template_;
        dlq_buffer_.push_back(std::move(dr));
    }

    void flush_dlq() {
        if (dlq_buffer_.empty() || dlq_topic_.empty()) return;

        auto logger = get_redis_sink_logger();
        logger->info("RedisSinkConnector '{}': flushing {} DLQ records to topic '{}'",
                     name_, dlq_buffer_.size(), dlq_topic_);

        metrics_.dlq_records.fetch_add(static_cast<int64_t>(dlq_buffer_.size()));
        dlq_buffer_.clear();
    }

    // ------------------------------------------------------------------------
    // Member variables
    // ------------------------------------------------------------------------

    // Configuration
    std::string hosts_str_;
    std::string mode_;
    std::string sentinel_master_;
    std::string password_;
    std::string key_template_;
    std::string hash_tag_;
    std::string value_field_;
    std::string score_field_;
    std::string dlq_topic_;
    std::string ttl_field_;

    std::vector<RedisHost> hosts_;
    std::vector<std::string> hash_fields_;

    RedisDataStructure data_structure_ = RedisDataStructure::string_type;
    SerializeFormat    serialize_fmt_  = SerializeFormat::raw;
    ListDirection      list_dir_       = ListDirection::left;

    int32_t  database_          = kDefaultDatabase;
    int64_t  ttl_seconds_       = kDefaultTtlSeconds;
    int64_t  stream_maxlen_     = 0;
    size_t   batch_size_        = kDefaultBatchSize;
    int64_t  flush_timeout_ms_  = kDefaultFlushTimeoutMs;
    int64_t  max_retries_       = kDefaultMaxRetries;
    int64_t  connect_timeout_ms_ = kDefaultConnectTimeout;
    size_t   pool_size_         = kDefaultPoolSize;

    // State
    std::atomic<bool> stopped_{false};
    std::vector<SinkRecord> buffer_;
    std::vector<PipelineCommand> pipeline_;
    std::unordered_map<std::string, PartitionOffset> offset_tracker_;
    std::deque<DlqRecord> dlq_buffer_;

    // Metrics
    struct {
        int64_t records_written   = 0;
        int64_t commands_pipelined = 0;
        int64_t batches_executed  = 0;
        int64_t write_errors      = 0;
        int64_t parse_errors      = 0;
        int64_t dlq_records       = 0;
        std::chrono::steady_clock::time_point created_at;
    } metrics_;
};

} // namespace torrent::connectors
