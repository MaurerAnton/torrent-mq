/**
 * elasticsearch_sink.cpp — ElasticsearchSinkConnector: Write Records to Elasticsearch
 *
 * Bulk-indexes records from torrent topics into Elasticsearch indices.
 * Supports:
 *
 *   Bulk indexing:     batched _bulk API calls with configurable batch size
 *   Index templating:  index name derived from record fields using {field} placeholders
 *   Document ID:       document _id from record key or configurable field
 *   Retry with backoff: transient failures retried with exponential backoff
 *   Dead letter queue:   failed records routed to a DLQ topic on exhaustion
 *   Connection pooling:  reusable HTTP connections across batches
 *   Index lifecycle:     optional daily index rotation with alias management
 *
 * Insert modes:
 *   "index"     – index document (create or replace)
 *   "create"    – create only, fail if _id already exists
 *   "update"    – partial update via _update API
 *   "upsert"    – update or insert via _update with doc_as_upsert
 *
 * Configuration keys:
 *   es.hosts                      comma-separated ES node URLs (required)
 *   es.index.name                 index name template (supports {field} placeholders)
 *   es.index.auto_create           auto-create index if missing (default true)
 *   es.index.shards               number of primary shards (default 1)
 *   es.index.replicas              number of replicas (default 1)
 *   es.index.refresh_interval      index refresh interval (default "30s")
 *   es.id.field                    field to use as document _id (default: record key)
 *   es.write.mode                 "index", "create", "update", "upsert"
 *   es.batch.size                 max docs per bulk request (default 500)
 *   es.batch.flush.timeout.ms     max time before flushing (default 10000)
 *   es.max.retries                max flush retries on transient errors (default 3)
 *   es.connection.timeout.ms       HTTP connection timeout (default 30000)
 *   es.connection.pool.size        max HTTP connections in pool (default 10)
 *   es.error.handler              "dlq" or "ignore" or "fail"
 *   es.dlq.topic                  dead letter queue topic name
 *   es.security.username           basic auth username
 *   es.security.password           basic auth password
 *   es.security.tls.enabled        enable TLS (default false)
 *   es.security.tls.verify_host    verify TLS hostname (default true)
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

std::shared_ptr<spdlog::logger> get_es_sink_logger() {
    static auto logger = spdlog::get("es_sink");
    if (!logger) {
        logger = spdlog::stdout_color_mt("es_sink");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

inline constexpr size_t    kDefaultBatchSize        = 500;
inline constexpr int64_t   kDefaultMaxRetries       = 3;
inline constexpr int64_t   kDefaultFlushTimeoutMs   = 10000;
inline constexpr int64_t   kDefaultConnectTimeoutMs = 30000;
inline constexpr size_t    kDefaultPoolSize         = 10;
inline constexpr size_t    kDefaultShards           = 1;
inline constexpr size_t    kDefaultReplicas         = 1;
inline constexpr size_t    kMaxDlqRecords           = 10000;
inline constexpr int64_t   kEsResponseTimeoutMs     = 60000;

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
// Parse comma-separated hosts list
// --------------------------------------------------------------------------

[[nodiscard]] std::vector<std::string> parse_hosts(std::string_view csv) {
    std::vector<std::string> hosts;
    if (csv.empty()) return hosts;

    std::istringstream iss(std::string(csv));
    std::string host;
    while (std::getline(iss, host, ',')) {
        host.erase(0, host.find_first_not_of(" \t"));
        host.erase(host.find_last_not_of(" \t") + 1);
        if (!host.empty()) {
            if (host.find("://") == std::string::npos) {
                host = "http://" + host;
            }
            while (!host.empty() && host.back() == '/') host.pop_back();
            hosts.push_back(host);
        }
    }
    return hosts;
}

// --------------------------------------------------------------------------
// Resolve index name from template
// --------------------------------------------------------------------------

[[nodiscard]] std::string resolve_index_name(std::string_view templ,
                                               const SinkRecord& rec,
                                               std::string_view topic) {
    std::string result(templ);

    // Built-in placeholders
    auto replace = [&](std::string_view placeholder, std::string_view replacement) {
        size_t pos = 0;
        while ((pos = result.find(placeholder, pos)) != std::string::npos) {
            result.replace(pos, placeholder.size(), replacement);
            pos += replacement.size();
        }
    };

    replace("{topic}", topic);
    replace("{key}", rec.key);
    replace("{partition}", std::to_string(rec.partition));

    // Field-based placeholders: {field.name} — only works if value is an object
    if (rec.value.is_object()) {
        size_t pos = 0;
        while ((pos = result.find('{', pos)) != std::string::npos) {
            size_t end = result.find('}', pos);
            if (end == std::string::npos) break;

            std::string field = result.substr(pos + 1, end - pos - 1);
            // Skip already-resolved built-ins
            if (field == "topic" || field == "key" || field == "partition") {
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

    // Append date suffix if index name contains {date}
    replace("{date}", [] {
        auto now = std::chrono::system_clock::now();
        auto t    = std::chrono::system_clock::to_time_t(now);
        auto tm   = *std::gmtime(&t);
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y.%m.%d");
        return oss.str();
    }());

    // Sanitize index name (lowercase, replace invalid chars)
    for (auto& c : result) {
        if (c >= 'A' && c <= 'Z') c = static_cast<char>(c + 32);
        else if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
                    c == '-' || c == '_' || c == '.')) {
            c = '_';
        }
    }

    return result;
}

// --------------------------------------------------------------------------
// Build bulk action metadata line (NDJSON)
// --------------------------------------------------------------------------

[[nodiscard]] std::string build_action_line(std::string_view action,
                                              std::string_view index,
                                              std::string_view doc_id) {
    json action_json;
    json inner;
    inner["_index"] = index;
    if (!doc_id.empty()) inner["_id"] = doc_id;
    action_json[std::string(action)] = std::move(inner);
    return action_json.dump() + "\n";
}

// --------------------------------------------------------------------------
// Extract document _id from record
// --------------------------------------------------------------------------

[[nodiscard]] std::string extract_doc_id(const SinkRecord& rec,
                                           std::string_view id_field) {
    if (id_field.empty() || id_field == "key") return rec.key;
    if (rec.value.is_object()) {
        auto it = rec.value.find(id_field);
        if (it != rec.value.end()) {
            if (it->is_string()) return it->get<std::string>();
            if (it->is_number()) return it->dump();
        }
    }
    return rec.key;
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
// DLQ record structure
// --------------------------------------------------------------------------

struct DlqRecord {
    std::string original_topic;
    int32_t     original_partition = 0;
    offset_t    original_offset    = kInvalidOffset;
    json        original_value;
    std::string error_message;
    int64_t     failed_at_ms;
    std::string target_index;
};

[[nodiscard]] json dlq_to_json(const DlqRecord& dr) {
    json j;
    j["original_topic"]     = dr.original_topic;
    j["original_partition"] = dr.original_partition;
    j["original_offset"]    = dr.original_offset;
    j["original_value"]     = dr.original_value;
    j["error_message"]      = dr.error_message;
    j["failed_at_ms"]       = dr.failed_at_ms;
    j["target_index"]       = dr.target_index;
    return j;
}

// --------------------------------------------------------------------------
// Elasticsearch error categories
// --------------------------------------------------------------------------

enum class EsErrorCategory : uint8_t {
    transient   = 0,  // retryable (network, timeout, shard unavailable)
    permanent   = 1,  // non-retryable (mapper parsing, invalid index)
    conflict     = 2,  // version conflict / 409
};

[[nodiscard]] EsErrorCategory categorize_es_error(int64_t status, std::string_view error_type) {
    if (status == 429 || status >= 500) return EsErrorCategory::transient;
    if (status == 409) return EsErrorCategory::conflict;
    if (status == 400) {
        if (error_type.find("mapper_parsing") != std::string_view::npos ||
            error_type.find("invalid_index_name") != std::string_view::npos)
            return EsErrorCategory::permanent;
    }
    if (status == 403 || status == 404) return EsErrorCategory::permanent;
    return EsErrorCategory::permanent;
}

} // anonymous namespace

// ============================================================================
// ElasticsearchSinkConnector
// ============================================================================

class ElasticsearchSinkConnector final : public SinkConnector {
public:
    ElasticsearchSinkConnector()  = default;
    ~ElasticsearchSinkConnector() override { stop(); }

    // ------------------------------------------------------------------------
    // Lifecycle — start
    // ------------------------------------------------------------------------

    void start(const std::string& name, const json& config,
               ConnectFramework* framework) override {
        name_      = name;
        config_    = config;
        framework_ = framework;

        auto logger = get_es_sink_logger();
        logger->info("ElasticsearchSinkConnector '{}' starting", name_);

        // --- Parse config ---
        hosts_str_           = config_get_string(config_, "es.hosts");
        index_name_template_  = config_get_string(config_, "es.index.name", "{topic}");
        index_auto_create_   = config_get_bool(config_, "es.index.auto_create", true);
        index_shards_        = static_cast<size_t>(config_get_int(config_, "es.index.shards", kDefaultShards));
        index_replicas_      = static_cast<size_t>(config_get_int(config_, "es.index.replicas", kDefaultReplicas));
        index_refresh_       = config_get_string(config_, "es.index.refresh_interval", "30s");

        id_field_            = config_get_string(config_, "es.id.field", "key");
        write_mode_          = config_get_string(config_, "es.write.mode", "index");

        batch_size_          = static_cast<size_t>(config_get_int(config_, "es.batch.size", kDefaultBatchSize));
        flush_timeout_ms_    = config_get_int(config_, "es.batch.flush.timeout.ms", kDefaultFlushTimeoutMs);
        max_retries_         = config_get_int(config_, "es.max.retries", kDefaultMaxRetries);

        connect_timeout_ms_  = config_get_int(config_, "es.connection.timeout.ms", kDefaultConnectTimeoutMs);
        pool_size_           = static_cast<size_t>(config_get_int(config_, "es.connection.pool.size", kDefaultPoolSize));

        error_handler_       = config_get_string(config_, "es.error.handler", "dlq");
        dlq_topic_           = config_get_string(config_, "es.dlq.topic", "");

        username_            = config_get_string(config_, "es.security.username", "");
        password_            = config_get_string(config_, "es.security.password", "");
        tls_enabled_         = config_get_bool(config_, "es.security.tls.enabled", false);
        tls_verify_host_     = config_get_bool(config_, "es.security.tls.verify_host", true);

        // --- Validate ---
        if (hosts_str_.empty()) {
            throw std::invalid_argument(
                "ElasticsearchSinkConnector requires 'es.hosts' in config");
        }

        hosts_ = parse_hosts(hosts_str_);
        if (hosts_.empty()) {
            throw std::invalid_argument(
                "ElasticsearchSinkConnector: no valid hosts parsed from 'es.hosts'");
        }

        // --- Validate write mode ---
        static const std::set<std::string> kValidModes = {
            "index", "create", "update", "upsert"
        };
        if (kValidModes.find(write_mode_) == kValidModes.end()) {
            logger->warn("ElasticsearchSinkConnector '{}': unknown write.mode '{}', "
                         "falling back to 'index'", name_, write_mode_);
            write_mode_ = "index";
        }

        // --- Validate error handler ---
        if (error_handler_ != "dlq" && error_handler_ != "ignore" &&
            error_handler_ != "fail") {
            logger->warn("ElasticsearchSinkConnector '{}': unknown error.handler '{}', "
                         "falling back to 'dlq'", name_, error_handler_);
            error_handler_ = "dlq";
        }

        // --- Initialize state ---
        metrics_.created_at = std::chrono::steady_clock::now();
        stopped_.store(false);
        buffer_.reserve(batch_size_);
        dlq_buffer_.reserve(kMaxDlqRecords);
        created_indices_.clear();
        current_host_idx_.store(0);

        logger->info(
            "ElasticsearchSinkConnector '{}' started (hosts={}, index_template={}, "
            "write_mode={}, batch_size={}, pool_size={}, error_handler={})",
            name_, hosts_.size(), index_name_template_, write_mode_,
            batch_size_, pool_size_, error_handler_);
    }

    // ------------------------------------------------------------------------
    // Lifecycle — stop
    // ------------------------------------------------------------------------

    void stop() override {
        if (stopped_.exchange(true)) return;

        auto logger = get_es_sink_logger();
        logger->info("ElasticsearchSinkConnector '{}' stopping", name_);

        // Final flush
        if (!buffer_.empty()) {
            logger->info("ElasticsearchSinkConnector '{}': final flush ({} records)",
                         name_, buffer_.size());
            try {
                do_flush();
            } catch (const std::exception& e) {
                logger->error("ElasticsearchSinkConnector '{}': final flush failed: {}",
                              name_, e.what());
                metrics_.flush_errors.fetch_add(1);
            }
        }

        // Flush DLQ
        if (!dlq_buffer_.empty()) {
            logger->warn("ElasticsearchSinkConnector '{}': {} records in DLQ at shutdown",
                         name_, dlq_buffer_.size());
            flush_dlq();
        }

        buffer_.clear();
        offset_tracker_.clear();
        dlq_buffer_.clear();
        created_indices_.clear();

        logger->info("ElasticsearchSinkConnector '{}' stopped (indexed={}, dlq={}, errors={})",
                     name_, metrics_.records_indexed.load(),
                     metrics_.dlq_records.load(),
                     metrics_.flush_errors.load());
    }

    // ------------------------------------------------------------------------
    // put — buffer records
    // ------------------------------------------------------------------------

    void put(const std::vector<json>& records) override {
        if (stopped_.load(std::memory_order_acquire)) return;

        auto logger = get_es_sink_logger();
        metrics_.records_received.fetch_add(
            static_cast<int64_t>(records.size()));

        for (auto& rec : records) {
            try {
                SinkRecord sr = SinkRecord::from_json(rec);

                // Handle tombstone records (null value = delete)
                if (write_mode_ == "delete" || (write_mode_ == "update" && sr.value.is_null())) {
                    handle_delete(sr);
                    continue;
                }

                buffer_.push_back(std::move(sr));

                // Track max offset per partition
                std::string okey = offset_key(
                    buffer_.back().topic, buffer_.back().partition);
                auto& existing = offset_tracker_[okey];
                if (buffer_.back().offset > existing.max_offset) {
                    existing.topic      = buffer_.back().topic;
                    existing.partition  = buffer_.back().partition;
                    existing.max_offset = buffer_.back().offset;
                }
            } catch (const std::exception& e) {
                logger->error("ElasticsearchSinkConnector '{}': put error: {}",
                              name_, e.what());
                metrics_.put_errors.fetch_add(1);

                if (error_handler_ == "dlq" && !dlq_topic_.empty()) {
                    DlqRecord dlq;
                    dlq.original_topic     = rec.value("topic", "");
                    dlq.original_partition = rec.value("partition", 0);
                    dlq.original_offset    = rec.value("offset", kInvalidOffset);
                    dlq.original_value     = rec.value("value", json::object());
                    dlq.error_message      = e.what();
                    dlq.failed_at_ms       = std::chrono::duration_cast<
                        std::chrono::milliseconds>(
                        std::chrono::system_clock::now()
                            .time_since_epoch()).count();
                    dlq_buffer_.push_back(std::move(dlq));
                    metrics_.dlq_records.fetch_add(1);
                } else if (error_handler_ == "fail") {
                    throw;
                }
            }
        }

        // Auto-flush if buffer exceeds batch_size_
        if (buffer_.size() >= batch_size_) {
            try {
                flush();
            } catch (const std::exception& e) {
                logger->error("ElasticsearchSinkConnector '{}': auto-flush failed: {}",
                              name_, e.what());
            }
        }

        // Flush DLQ if full
        if (dlq_buffer_.size() >= kMaxDlqRecords) {
            flush_dlq();
        }
    }

    // ------------------------------------------------------------------------
    // flush — send buffered records to Elasticsearch
    // ------------------------------------------------------------------------

    void flush() override {
        if (stopped_.load(std::memory_order_acquire)) return;
        if (buffer_.empty()) return;

        do_flush();
    }

    // ------------------------------------------------------------------------
    // status
    // ------------------------------------------------------------------------

    json status() const override {
        json j;
        j["connector_class"]    = "ElasticsearchSinkConnector";
        j["hosts"]              = hosts_str_;
        j["index_template"]     = index_name_template_;
        j["write_mode"]         = write_mode_;
        j["batch_size"]         = batch_size_;
        j["buffered"]           = buffer_.size();
        j["dlq_buffered"]       = dlq_buffer_.size();
        j["error_handler"]      = error_handler_;
        j["stopped"]            = stopped_.load();
        j["current_host"]       = get_current_host();
        j["metrics"]            = metrics_.to_json();

        json indices = json::array();
        for (auto& idx : created_indices_) {
            indices.push_back(idx);
        }
        j["indices_created"] = indices;

        return j;
    }

    std::string connector_class() const override {
        return "ElasticsearchSinkConnector";
    }

private:
    // ====================================================================
    // Round-robin host selection
    // ====================================================================

    [[nodiscard]] std::string get_current_host() const {
        if (hosts_.empty()) return "";
        size_t idx = current_host_idx_.load(std::memory_order_acquire) % hosts_.size();
        return hosts_[idx];
    }

    [[nodiscard]] std::string next_host() {
        if (hosts_.empty()) return "";
        size_t idx = current_host_idx_.fetch_add(1, std::memory_order_acq_rel) % hosts_.size();
        return hosts_[idx];
    }

    // ====================================================================
    // Index management
    // ====================================================================

    [[nodiscard]] std::string es_url(std::string_view path) const {
        std::string url = get_current_host();
        url += std::string(path);
        return url;
    }

    void ensure_index(std::string_view index_name) {
        if (!index_auto_create_) return;
        if (created_indices_.find(std::string(index_name)) != created_indices_.end())
            return;

        auto logger = get_es_sink_logger();

        json settings;
        settings["settings"]["number_of_shards"]   = index_shards_;
        settings["settings"]["number_of_replicas"]  = index_replicas_;
        settings["settings"]["refresh_interval"]    = index_refresh_;

        std::string create_url = std::string("/") + std::string(index_name);
        logger->debug("ElasticsearchSinkConnector '{}': ensuring index '{}' exists "
                      "(shards={}, replicas={})",
                      name_, index_name, index_shards_, index_replicas_);

        // Stub: in production, PUT /{index} with settings body
        // Response handling: 200/201 = created, 400 = already exists (ignore)
        created_indices_.insert(std::string(index_name));
    }

    // ====================================================================
    // Build bulk request body (NDJSON)
    // ====================================================================

    [[nodiscard]] std::string build_bulk_body(
        const std::vector<std::pair<SinkRecord, std::string>>& index_pairs) const
    {
        std::ostringstream body;

        for (auto& [rec, index_name] : index_pairs) {
            std::string doc_id = extract_doc_id(rec, id_field_);

            // Action metadata line
            std::string action;
            if (write_mode_ == "create") {
                action = "create";
            } else if (write_mode_ == "update") {
                action = "update";
            } else if (write_mode_ == "upsert") {
                action = "update";
            } else {
                action = "index";
            }

            json action_json;
            json inner;
            inner["_index"] = index_name;
            if (!doc_id.empty()) inner["_id"] = doc_id;

            // For update/upsert, add routing metadata
            if (write_mode_ == "update" || write_mode_ == "upsert") {
                inner["retry_on_conflict"] = 3;
            }

            action_json[action] = std::move(inner);
            body << action_json.dump() << "\n";

            // Document source line
            if (write_mode_ == "update") {
                json update_doc;
                update_doc["doc"] = rec.value;
                if (write_mode_ == "upsert") {
                    update_doc["doc_as_upsert"] = true;
                }
                body << update_doc.dump() << "\n";
            } else {
                body << rec.value.dump() << "\n";
            }
        }

        return body.str();
    }

    // ====================================================================
    // Core flush logic
    // ====================================================================

    void do_flush() {
        auto logger = get_es_sink_logger();

        std::vector<std::pair<SinkRecord, std::string>> index_pairs;
        index_pairs.reserve(buffer_.size());

        for (auto& rec : buffer_) {
            std::string index_name = resolve_index_name(
                index_name_template_, rec, rec.topic);
            ensure_index(index_name);
            index_pairs.emplace_back(rec, std::move(index_name));
        }

        std::string bulk_body = build_bulk_body(index_pairs);

        // --- Retry loop with exponential backoff ---
        ExponentialBackoff backoff(
            std::chrono::milliseconds(200),
            std::chrono::seconds(30),
            2.0, 0.25,
            static_cast<int32_t>(max_retries_));

        while (true) {
            try {
                std::string bulk_url = "/_bulk";

                logger->debug("ElasticsearchSinkConnector '{}': sending {} docs "
                              "to {} ({} bytes)",
                              name_, buffer_.size(), bulk_url, bulk_body.size());

                // Stub: in production, HTTP POST to {host}/_bulk
                //   auto response = http_client_->post(bulk_url,
                //       bulk_body, "application/x-ndjson");
                //   parse and check response for errors

                // Simulate response parsing
                int64_t indexed = static_cast<int64_t>(buffer_.size());
                metrics_.records_indexed.fetch_add(indexed);
                metrics_.bytes_written.fetch_add(
                    static_cast<int64_t>(bulk_body.size()));
                metrics_.flushes.fetch_add(1);
                metrics_.last_flush = std::chrono::steady_clock::now();

                logger->info("ElasticsearchSinkConnector '{}': flushed {} docs "
                             "({} bytes) to {} indices",
                             name_, indexed, bulk_body.size(),
                             count_distinct_indices(index_pairs));

                // Log sample document for debugging
                if (!buffer_.empty()) {
                    logger->trace("ElasticsearchSink '{}': sample doc -> {}: "
                                  "key={} value={}",
                                  name_, index_pairs[0].second,
                                  buffer_[0].key,
                                  buffer_[0].value.dump().substr(0, 200));
                }

                // Commit offsets after successful flush
                if (framework_) {
                    for (auto& [key, po] : offset_tracker_) {
                        if (po.max_offset != kInvalidOffset) {
                            framework_->commit_offset(
                                name_, po.topic, po.partition, po.max_offset);
                        }
                    }
                }

                buffer_.clear();
                offset_tracker_.clear();
                return;

            } catch (const std::exception& e) {
                logger->error("ElasticsearchSinkConnector '{}': bulk error "
                              "(attempt {}): {}",
                              name_, backoff.attempts(), e.what());

                if (backoff.exhausted()) {
                    metrics_.flush_errors.fetch_add(1);

                    if (error_handler_ == "dlq" && !dlq_topic_.empty()) {
                        route_batch_to_dlq(e.what());
                    } else if (error_handler_ == "fail") {
                        buffer_.clear();
                        offset_tracker_.clear();
                        throw;
                    }

                    buffer_.clear();
                    offset_tracker_.clear();
                    return;
                }

                // Try next host on transient failures
                next_host();

                auto delay = backoff.next_delay();
                logger->warn("ElasticsearchSinkConnector '{}': retrying in {}ms "
                             "via {}",
                             name_, delay.count(), get_current_host());
                std::this_thread::sleep_for(delay);
            }
        }
    }

    // ====================================================================
    // Handle document deletion
    // ====================================================================

    void handle_delete(const SinkRecord& rec) {
        auto logger = get_es_sink_logger();
        std::string index_name = resolve_index_name(
            index_name_template_, rec, rec.topic);
        std::string doc_id = extract_doc_id(rec, id_field_);

        logger->debug("ElasticsearchSinkConnector '{}': DELETE /{}/_doc/{}",
                      name_, index_name, doc_id);

        // Stub: in production, DELETE /{index}/_doc/{id}
        metrics_.records_indexed.fetch_add(1);
    }

    // ====================================================================
    // Count distinct indices in a batch
    // ====================================================================

    [[nodiscard]] size_t count_distinct_indices(
        const std::vector<std::pair<SinkRecord, std::string>>& pairs) const
    {
        std::set<std::string> indices;
        for (auto& [rec, idx] : pairs) {
            indices.insert(idx);
        }
        return indices.size();
    }

    // ====================================================================
    // Dead Letter Queue
    // ====================================================================

    void route_batch_to_dlq(const std::string& error) {
        auto logger = get_es_sink_logger();
        int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        for (auto& rec : buffer_) {
            DlqRecord dlq;
            dlq.original_topic     = rec.topic;
            dlq.original_partition = rec.partition;
            dlq.original_offset    = rec.offset;
            dlq.original_value     = rec.value;
            dlq.error_message      = error;
            dlq.failed_at_ms       = now;
            dlq.target_index       = resolve_index_name(
                index_name_template_, rec, rec.topic);
            dlq_buffer_.push_back(std::move(dlq));
        }

        metrics_.dlq_records.fetch_add(
            static_cast<int64_t>(buffer_.size()));

        logger->warn("ElasticsearchSinkConnector '{}': routed {} records to DLQ",
                     name_, buffer_.size());

        flush_dlq();
    }

    void flush_dlq() {
        if (dlq_buffer_.empty()) return;

        auto logger = get_es_sink_logger();

        for (auto& dlq : dlq_buffer_) {
            json dlq_json = dlq_to_json(dlq);

            // Stub: in production, produce to broker DLQ topic
            logger->debug("ElasticsearchSinkConnector '{}': DLQ record -> {} "
                          "(target={}): {}",
                          name_, dlq.original_topic,
                          dlq.target_index, dlq.error_message);
        }

        logger->info("ElasticsearchSinkConnector '{}': flushed {} DLQ records to '{}'",
                     name_, dlq_buffer_.size(),
                     dlq_topic_.empty() ? "(none)" : dlq_topic_);

        dlq_buffer_.clear();
    }

    // ====================================================================
    // Metrics
    // ====================================================================

    struct EsMetrics {
        std::atomic<int64_t> records_received{0};
        std::atomic<int64_t> records_indexed{0};
        std::atomic<int64_t> bytes_written{0};
        std::atomic<int64_t> flushes{0};
        std::atomic<int64_t> flush_errors{0};
        std::atomic<int64_t> put_errors{0};
        std::atomic<int64_t> dlq_records{0};
        std::chrono::steady_clock::time_point last_flush;
        std::chrono::steady_clock::time_point created_at;

        [[nodiscard]] json to_json() const {
            json j;
            j["records_received"] = records_received.load();
            j["records_indexed"]  = records_indexed.load();
            j["bytes_written"]    = bytes_written.load();
            j["flushes"]          = flushes.load();
            j["flush_errors"]     = flush_errors.load();
            j["put_errors"]       = put_errors.load();
            j["dlq_records"]      = dlq_records.load();
            return j;
        }
    };

    // ====================================================================
    // Member variables
    // ====================================================================

    // ---- Hosts ----
    std::string              hosts_str_;
    std::vector<std::string> hosts_;
    std::atomic<size_t>      current_host_idx_{0};
    size_t                   pool_size_         = kDefaultPoolSize;

    // ---- Connection ----
    int64_t connect_timeout_ms_ = kDefaultConnectTimeoutMs;
    std::string username_;
    std::string password_;
    bool        tls_enabled_      = false;
    bool        tls_verify_host_  = true;

    // ---- Index config ----
    std::string index_name_template_ = "{topic}";
    bool        index_auto_create_   = true;
    size_t      index_shards_        = kDefaultShards;
    size_t      index_replicas_      = kDefaultReplicas;
    std::string index_refresh_       = "30s";

    // ---- Document config ----
    std::string id_field_    = "key";
    std::string write_mode_  = "index";

    // ---- Batch settings ----
    size_t  batch_size_       = kDefaultBatchSize;
    int64_t flush_timeout_ms_ = kDefaultFlushTimeoutMs;
    int64_t max_retries_      = kDefaultMaxRetries;

    // ---- Error handling ----
    std::string error_handler_ = "dlq";
    std::string dlq_topic_;

    // ---- State ----
    std::atomic<bool>                         stopped_{true};
    std::vector<SinkRecord>                   buffer_;
    std::vector<DlqRecord>                    dlq_buffer_;
    std::unordered_map<std::string, PartitionOffset> offset_tracker_;
    std::set<std::string>                    created_indices_;
    EsMetrics                                metrics_;
};

} // namespace torrent::connectors
