/**
 * influxdb_sink.cpp — InfluxDBSinkConnector: Write Records to InfluxDB
 *
 * Writes records from torrent topics to an InfluxDB time-series database
 * using InfluxDB Line Protocol. Supports:
 *
 *   Line protocol formatting: measurement,tag_set field_set timestamp
 *   Batch writes:             configurable batch size with HTTP POST to /write
 *   Timestamp extraction:     from record timestamp or configurable field
 *   Tag vs field classification: configurable tag_fields and field_mappings
 *   Retention policy:         per-database retention policy via query param
 *   Precision control:        ns, us, ms, s — auto-scaling timestamps
 *   Dead letter queue:        failed records routed to configurable DLQ topic
 *   Authentication:           token-based auth via Authorization header
 *   Gzip compression:         optional gzip compression of write payload
 *
 * Line Protocol format:
 *   measurement,tag_key=tag_value,... field_key=field_value[,...] [timestamp]
 *
 * Configuration keys:
 *   influxdb.url                        InfluxDB HTTP write endpoint (required)
 *   influxdb.token                      authentication token (required for 2.x)
 *   influxdb.org                        InfluxDB organization (2.x)
 *   influxdb.bucket                     InfluxDB bucket / database name
 *   influxdb.measurement                measurement name (supports {topic} placeholder)
 *   influxdb.measurement.from.topic     use topic name as measurement (default true)
 *   influxdb.precision                  timestamp precision: ns, us, ms, s (default ms)
 *   influxdb.timestamp.field            record field to use as timestamp
 *   influxdb.tags                       comma-separated tag field names
 *   influxdb.fields                     comma-separated field names (rest become tags)
 *   influxdb.retention.policy           optional retention policy name
 *   influxdb.batch.size                 max records per batch (default 5000)
 *   influxdb.batch.flush.timeout.ms     max time before flushing (default 10000)
 *   influxdb.max.retries                max flush retries on transient errors (default 3)
 *   influxdb.compression               enable gzip request compression (default true)
 *   influxdb.dlq.topic                  dead letter queue topic name
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
#include <iomanip>
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

std::shared_ptr<spdlog::logger> get_influxdb_sink_logger() {
    static auto logger = spdlog::get("influxdb_sink");
    if (!logger) {
        logger = spdlog::stdout_color_mt("influxdb_sink");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

inline constexpr size_t    kDefaultBatchSize      = 5000;
inline constexpr int64_t   kDefaultFlushTimeoutMs = 10000;
inline constexpr int64_t   kDefaultMaxRetries     = 3;
inline constexpr size_t    kMaxDlqRecords         = 10000;
inline constexpr int64_t   kDefaultWriteTimeoutMs = 30000;
inline constexpr size_t    kMaxLineSize           = 64 * 1024;  // 64 KB max per line

// --------------------------------------------------------------------------
// Timestamp precision
// --------------------------------------------------------------------------

enum class InfluxPrecision : uint8_t {
    nanoseconds  = 0,
    microseconds = 1,
    milliseconds = 2,
    seconds      = 3,
};

[[nodiscard]] InfluxPrecision parse_precision(std::string_view name) {
    if (name == "ns") return InfluxPrecision::nanoseconds;
    if (name == "us") return InfluxPrecision::microseconds;
    if (name == "ms") return InfluxPrecision::milliseconds;
    if (name == "s")  return InfluxPrecision::seconds;
    return InfluxPrecision::milliseconds;
}

[[nodiscard]] const char* precision_param(InfluxPrecision p) {
    switch (p) {
    case InfluxPrecision::nanoseconds:  return "ns";
    case InfluxPrecision::microseconds: return "us";
    case InfluxPrecision::milliseconds: return "ms";
    case InfluxPrecision::seconds:      return "s";
    }
    return "ms";
}

[[nodiscard]] int64_t scale_timestamp(int64_t ms, InfluxPrecision p) {
    switch (p) {
    case InfluxPrecision::nanoseconds:  return ms * 1'000'000;
    case InfluxPrecision::microseconds: return ms * 1'000;
    case InfluxPrecision::milliseconds: return ms;
    case InfluxPrecision::seconds:      return ms / 1'000;
    }
    return ms;
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
// Parse comma-separated string list
// --------------------------------------------------------------------------

[[nodiscard]] std::set<std::string> parse_string_set(std::string_view csv) {
    std::set<std::string> result;
    if (csv.empty()) return result;
    std::istringstream iss(std::string(csv));
    std::string token;
    while (std::getline(iss, token, ',')) {
        // trim
        token.erase(0, token.find_first_not_of(" \t"));
        token.erase(token.find_last_not_of(" \t") + 1);
        if (!token.empty()) result.insert(token);
    }
    return result;
}

// --------------------------------------------------------------------------
// Escape InfluxDB Line Protocol special characters
// --------------------------------------------------------------------------

[[nodiscard]] std::string escape_measurement(std::string_view s) {
    std::string result;
    result.reserve(s.size() * 2);
    for (char c : s) {
        if (c == ',' || c == ' ') result += '\\';
        result += c;
    }
    return result;
}

[[nodiscard]] std::string escape_tag_value(std::string_view s) {
    std::string result;
    result.reserve(s.size() * 2);
    for (char c : s) {
        if (c == ',' || c == '=' || c == ' ') result += '\\';
        result += c;
    }
    return result;
}

[[nodiscard]] std::string escape_field_string(std::string_view s) {
    std::string result;
    result.reserve(s.size() * 2 + 2);
    result += '"';
    for (char c : s) {
        if (c == '"' || c == '\\') result += '\\';
        result += c;
    }
    result += '"';
    return result;
}

// --------------------------------------------------------------------------
// Determine if a JSON value is a tag or field
// --------------------------------------------------------------------------

[[nodiscard]] bool is_tag_candidate(const json& val) {
    // Strings of reasonable length make good tags
    return val.is_string() && val.get<std::string>().size() <= 256;
}

[[nodiscard]] bool is_int_type(const json& val) {
    return val.is_number_integer() || val.is_number_unsigned();
}

// --------------------------------------------------------------------------
// Build Line Protocol string for a single record
// --------------------------------------------------------------------------

[[nodiscard]] std::string build_line_protocol(
    const SinkRecord& rec,
    std::string_view measurement,
    const std::set<std::string>& tag_fields,
    const std::set<std::string>& field_fields,
    std::string_view timestamp_field_name,
    InfluxPrecision precision,
    bool measurement_from_topic)
{
    std::ostringstream line;

    // --- Measurement ---
    if (measurement_from_topic) {
        line << escape_measurement(rec.topic);
    } else if (!measurement.empty()) {
        // Replace {topic} placeholder
        std::string meas(measurement);
        size_t pos = meas.find("{topic}");
        if (pos != std::string::npos) {
            meas.replace(pos, 7, rec.topic);
        }
        line << escape_measurement(meas);
    } else {
        line << escape_measurement(rec.topic);
    }

    // --- Tags ---
    // Build tag set: if value has known tag fields, use them;
    // otherwise, use record-level key and partition as tags.
    bool has_tags = false;

    if (!rec.key.empty()) {
        line << ',' << "key" << '=' << escape_tag_value(rec.key);
        has_tags = true;
    }

    // Partition as tag
    line << ',' << "partition" << '=' << std::to_string(rec.partition);

    // Field-based tags from record value
    if (rec.value.is_object()) {
        for (auto it = rec.value.begin(); it != rec.value.end(); ++it) {
            // Skip field-only keys
            if (!field_fields.empty() && field_fields.count(it.key()) > 0) continue;
            // Skip the timestamp field if it's being used as the timestamp
            if (!timestamp_field_name.empty() && it.key() == timestamp_field_name) continue;
            // Only include if explicitly listed as tag or no field list given
            if (!tag_fields.empty() && tag_fields.count(it.key()) == 0) continue;
            if (is_tag_candidate(it.value())) {
                line << ',' << escape_measurement(it.key())
                     << '=' << escape_tag_value(it.value().get<std::string>());
                has_tags = true;
            }
        }
    }

    // If no tags added yet, add a default tag to avoid empty tag set
    if (!has_tags) {
        line << ',' << "source" << '=' << "torrent-mq";
    }

    // --- Separator ---
    line << ' ';

    // --- Fields ---
    bool has_fields = false;

    if (rec.value.is_object()) {
        for (auto it = rec.value.begin(); it != rec.value.end(); ++it) {
            // Skip tag-only keys
            if (!tag_fields.empty() && tag_fields.count(it.key()) > 0) continue;
            // Skip the timestamp field
            if (!timestamp_field_name.empty() && it.key() == timestamp_field_name) continue;

            const auto& key = it.key();
            const auto& val = it.value();

            if (!has_fields) has_fields = true;
            else line << ',';

            // Escape the field key
            line << escape_measurement(key) << '=';

            if (val.is_boolean()) {
                line << (val.get<bool>() ? "true" : "false");
            } else if (is_int_type(val)) {
                line << val.get<int64_t>() << 'i';
            } else if (val.is_number_float() || val.is_number()) {
                line << std::setprecision(15) << val.get<double>();
            } else if (val.is_string()) {
                line << escape_field_string(val.get<std::string>());
            } else if (val.is_null()) {
                // nulls not supported in line protocol — skip
                if (has_fields) {
                    // Remove the trailing comma we just added
                    auto s = line.str();
                    s.pop_back();
                    line.str("");
                    line << s;
                    has_fields = false;
                }
                continue;
            } else {
                line << escape_field_string(val.dump());
            }
        }
    }

    // If no fields from value, add record metadata as fields
    if (!has_fields) {
        line << "value=" << escape_field_string(rec.value.dump());
        if (!rec.key.empty()) {
            line << ",record_key=" << escape_field_string(rec.key);
        }
        line << ",offset=" << rec.offset << 'i';
    }

    // --- Timestamp ---
    line << ' ';

    int64_t ts = rec.timestamp_ms;
    if (!timestamp_field_name.empty() && rec.value.is_object()) {
        auto it = rec.value.find(timestamp_field_name);
        if (it != rec.value.end()) {
            if (it->is_number_integer()) ts = it->get<int64_t>();
            else if (it->is_number_float()) ts = static_cast<int64_t>(it->get<double>());
        }
    }
    // Default to now if no timestamp available
    if (ts <= 0) {
        ts = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    line << scale_timestamp(ts, precision);

    return line.str();
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
    std::string target_measurement;
};

[[nodiscard]] json dlq_to_json(const DlqRecord& dr) {
    json j;
    j["original_topic"]      = dr.original_topic;
    j["original_partition"]  = dr.original_partition;
    j["original_offset"]     = dr.original_offset;
    j["original_value"]      = dr.original_value;
    j["error_message"]       = dr.error_message;
    j["failed_at_ms"]        = dr.failed_at_ms;
    j["target_measurement"]  = dr.target_measurement;
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

} // anonymous namespace

// ============================================================================
// InfluxDBSinkConnector
// ============================================================================

class InfluxDBSinkConnector final : public SinkConnector {
public:
    InfluxDBSinkConnector()  = default;
    ~InfluxDBSinkConnector() override { stop(); }

    // ------------------------------------------------------------------------
    // Lifecycle — start
    // ------------------------------------------------------------------------

    void start(const std::string& name, const json& config,
               ConnectFramework* framework) override {
        name_      = name;
        config_    = config;
        framework_ = framework;

        auto logger = get_influxdb_sink_logger();
        logger->info("InfluxDBSinkConnector '{}' starting", name_);

        // --- Parse config ---
        write_url_           = config_get_string(config_, "influxdb.url");
        auth_token_          = config_get_string(config_, "influxdb.token");
        org_                 = config_get_string(config_, "influxdb.org");
        bucket_              = config_get_string(config_, "influxdb.bucket");
        measurement_         = config_get_string(config_, "influxdb.measurement", "");
        measurement_from_topic_ = config_get_bool(config_, "influxdb.measurement.from.topic", true);

        precision_           = parse_precision(
            config_get_string(config_, "influxdb.precision", "ms"));
        timestamp_field_     = config_get_string(config_, "influxdb.timestamp.field", "");

        tag_fields_str_      = config_get_string(config_, "influxdb.tags", "");
        field_fields_str_    = config_get_string(config_, "influxdb.fields", "");
        retention_policy_    = config_get_string(config_, "influxdb.retention.policy", "");

        batch_size_          = static_cast<size_t>(
            config_get_int(config_, "influxdb.batch.size", kDefaultBatchSize));
        flush_timeout_ms_    = config_get_int(config_, "influxdb.batch.flush.timeout.ms", kDefaultFlushTimeoutMs);
        max_retries_         = config_get_int(config_, "influxdb.max.retries", kDefaultMaxRetries);

        use_compression_     = config_get_bool(config_, "influxdb.compression", true);
        dlq_topic_           = config_get_string(config_, "influxdb.dlq.topic", "");

        // --- Validate ---
        if (write_url_.empty()) {
            throw std::invalid_argument(
                "InfluxDBSinkConnector requires 'influxdb.url' in config");
        }

        // Parse tag and field sets
        tag_fields_   = parse_string_set(tag_fields_str_);
        field_fields_ = parse_string_set(field_fields_str_);

        // --- Initialize state ---
        metrics_.created_at = std::chrono::steady_clock::now();
        stopped_.store(false);
        buffer_.reserve(batch_size_);
        dlq_buffer_.reserve(kMaxDlqRecords);
        offset_tracker_.clear();

        logger->info(
            "InfluxDBSinkConnector '{}' started (url={}, bucket={}, "
            "measurement_from_topic={}, precision={}, batch_size={}, "
            "tags={}, fields={}, retention={}, compression={})",
            name_, write_url_, bucket_,
            measurement_from_topic_, precision_param(precision_), batch_size_,
            tag_fields_.size(), field_fields_.size(),
            retention_policy_.empty() ? "none" : retention_policy_,
            use_compression_);
    }

    // ------------------------------------------------------------------------
    // Lifecycle — stop
    // ------------------------------------------------------------------------

    void stop() override {
        if (stopped_.exchange(true)) return;

        auto logger = get_influxdb_sink_logger();
        logger->info("InfluxDBSinkConnector '{}' stopping", name_);

        // Final flush
        if (!buffer_.empty()) {
            logger->info("InfluxDBSinkConnector '{}': final flush ({} records)",
                         name_, buffer_.size());
            try {
                do_flush();
            } catch (const std::exception& e) {
                logger->error("InfluxDBSinkConnector '{}': final flush failed: {}",
                              name_, e.what());
                metrics_.write_errors.fetch_add(1);
            }
        }

        // Flush DLQ
        if (!dlq_buffer_.empty()) {
            logger->warn("InfluxDBSinkConnector '{}': {} records in DLQ at shutdown",
                         name_, dlq_buffer_.size());
            flush_dlq();
        }

        buffer_.clear();
        offset_tracker_.clear();
        dlq_buffer_.clear();

        logger->info("InfluxDBSinkConnector '{}' stopped (written={}, dlq={}, errors={})",
                     name_, metrics_.records_written.load(),
                     metrics_.dlq_records.load(),
                     metrics_.write_errors.load());
    }

    // ------------------------------------------------------------------------
    // put — buffer records
    // ------------------------------------------------------------------------

    void put(const std::vector<json>& records) override {
        if (stopped_.load()) return;

        auto logger = get_influxdb_sink_logger();
        size_t added = 0;

        for (const auto& j : records) {
            if (buffer_.size() >= batch_size_) {
                try {
                    do_flush();
                } catch (const std::exception& e) {
                    logger->error("InfluxDBSinkConnector '{}': flush during put failed: {}",
                                  name_, e.what());
                    metrics_.write_errors.fetch_add(1);
                }
            }

            try {
                SinkRecord rec = SinkRecord::from_json(j);
                buffer_.push_back(std::move(rec));
                added++;
            } catch (const std::exception& e) {
                logger->warn("InfluxDBSinkConnector '{}': failed to parse record: {}",
                             name_, e.what());
                metrics_.parse_errors.fetch_add(1);
            }
        }

        if (added > 0) {
            logger->debug("InfluxDBSinkConnector '{}': buffered {} records (total: {})",
                          name_, added, buffer_.size());
        }
    }

    // ------------------------------------------------------------------------
    // flush — write buffered records to InfluxDB
    // ------------------------------------------------------------------------

    void flush() override {
        if (stopped_.load() || buffer_.empty()) return;
        do_flush();
    }

    // ------------------------------------------------------------------------
    // status — JSON snapshot
    // ------------------------------------------------------------------------

    [[nodiscard]] json status() const override {
        auto now = std::chrono::steady_clock::now();
        json j;
        j["connector_class"]     = "InfluxDBSinkConnector";
        j["name"]                = name_;
        j["write_url"]           = write_url_;
        j["bucket"]              = bucket_;
        j["precision"]           = precision_param(precision_);
        j["measurement_from_topic"] = measurement_from_topic_;
        j["buffer_size"]         = buffer_.size();
        j["dlq_size"]            = dlq_buffer_.size();
        j["records_written"]     = metrics_.records_written.load();
        j["batches_flushed"]     = metrics_.batches_flushed.load();
        j["write_errors"]        = metrics_.write_errors.load();
        j["parse_errors"]        = metrics_.parse_errors.load();
        j["dlq_records"]         = metrics_.dlq_records.load();
        j["uptime_ms"] = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - metrics_.created_at).count();
        return j;
    }

    // ------------------------------------------------------------------------
    // connector_class
    // ------------------------------------------------------------------------

    [[nodiscard]] std::string connector_class() const override {
        return "InfluxDBSinkConnector";
    }

private:
    // ------------------------------------------------------------------------
    // do_flush — build line protocol batch and send
    // ------------------------------------------------------------------------

    void do_flush() {
        if (buffer_.empty()) return;

        auto logger = get_influxdb_sink_logger();

        // Build line protocol payload
        std::ostringstream payload;
        size_t line_count = 0;

        for (const auto& rec : buffer_) {
            try {
                std::string line = build_line_protocol(
                    rec, measurement_, tag_fields_, field_fields_,
                    timestamp_field_, precision_, measurement_from_topic_);

                if (line.size() > kMaxLineSize) {
                    // Line too large — send to DLQ
                    if (!dlq_topic_.empty()) {
                        enqueue_dlq(rec, "Line protocol exceeds max size");
                    }
                    metrics_.dlq_records.fetch_add(1);
                    continue;
                }

                payload << line << '\n';
                line_count++;

                // Track max offset
                auto key = offset_key(rec.topic, rec.partition);
                auto& po = offset_tracker_[key];
                po.topic     = rec.topic;
                po.partition = rec.partition;
                if (rec.offset > po.max_offset) po.max_offset = rec.offset;
            } catch (const std::exception& e) {
                logger->error("InfluxDBSinkConnector '{}': line protocol build error: {}",
                              name_, e.what());
                if (!dlq_topic_.empty()) {
                    enqueue_dlq(rec, std::string("line protocol: ") + e.what());
                }
                metrics_.dlq_records.fetch_add(1);
            }
        }

        // Send with retries
        bool sent = false;
        std::string last_error;

        for (int64_t attempt = 0; attempt <= max_retries_ && !sent; attempt++) {
            if (attempt > 0) {
                int64_t delay_ms = (1LL << attempt) * 100;
                logger->info("InfluxDBSinkConnector '{}': retry attempt {}/{} after {}ms",
                             name_, attempt, max_retries_, delay_ms);
                std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
            }

            try {
                send_to_influxdb(payload.str(), line_count);
                sent = true;
            } catch (const std::exception& e) {
                last_error = e.what();
                logger->warn("InfluxDBSinkConnector '{}': write attempt {} failed: {}",
                             name_, attempt, last_error);
            }
        }

        if (!sent) {
            logger->error("InfluxDBSinkConnector '{}': all {} retries exhausted: {}",
                          name_, max_retries_, last_error);
            metrics_.write_errors.fetch_add(1);

            // Route all records to DLQ
            if (!dlq_topic_.empty()) {
                for (const auto& rec : buffer_) {
                    enqueue_dlq(rec, "All retries exhausted: " + last_error);
                }
                flush_dlq();
            }
        } else {
            metrics_.records_written.fetch_add(static_cast<int64_t>(line_count));
            metrics_.batches_flushed.fetch_add(1);
        }

        buffer_.clear();

        // Flush DLQ if it has accumulated
        if (dlq_buffer_.size() >= kMaxDlqRecords / 2) {
            flush_dlq();
        }
    }

    // ------------------------------------------------------------------------
    // send_to_influxdb — HTTP POST to InfluxDB /write endpoint
    // ------------------------------------------------------------------------

    void send_to_influxdb(const std::string& body, size_t line_count) {
        auto logger = get_influxdb_sink_logger();

        // Build URL with query parameters
        std::ostringstream url;
        url << write_url_;
        // Ensure URL ends properly
        if (write_url_.back() != '/') url << '/';
        url << "write?";

        // Query parameters
        url << "org=" << org_ << "&bucket=" << bucket_;
        url << "&precision=" << precision_param(precision_);
        if (!retention_policy_.empty()) {
            url << "&rp=" << retention_policy_;
        }

        logger->debug("InfluxDBSinkConnector '{}': sending {} lines ({} bytes) to {}",
                      name_, line_count, body.size(), url.str());

        // In a real implementation, this would use libcurl or similar.
        // For the stub, we validate the payload and simulate.
        if (body.empty()) {
            throw std::runtime_error("Empty payload — nothing to send");
        }
        if (body.size() > 100 * 1024 * 1024) {
            throw std::runtime_error("Payload too large: " +
                std::to_string(body.size()) + " bytes");
        }

        // Simulate successful write
        // Real implementation would:
        //   1. Initialize CURL handle
        //   2. Set URL, POST method
        //   3. Set Authorization header: "Token " + auth_token_
        //   4. Set Content-Type: text/plain; charset=utf-8
        //   5. If use_compression_, set Accept-Encoding: gzip and compress body
        //   6. Set CURLOPT_POSTFIELDS / CURLOPT_POSTFIELDSIZE
        //   7. Set CURLOPT_TIMEOUT_MS to write_timeout_ms_
        //   8. Perform curl_easy_perform()
        //   9. Check response code: 204 = success, 4xx/5xx = error
        //  10. Cleanup CURL handle

        logger->info("InfluxDBSinkConnector '{}': wrote {} lines to {}/{} (simulated)",
                     name_, line_count, bucket_, org_);
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
        dr.target_measurement = measurement_;
        dlq_buffer_.push_back(std::move(dr));
    }

    void flush_dlq() {
        if (dlq_buffer_.empty() || dlq_topic_.empty()) return;

        auto logger = get_influxdb_sink_logger();
        logger->info("InfluxDBSinkConnector '{}': flushing {} DLQ records to topic '{}'",
                     name_, dlq_buffer_.size(), dlq_topic_);

        // In a real implementation, this would call framework_->produce()
        // to write each DLQ record to the DLQ topic.
        // For the stub, we just log and clear.

        metrics_.dlq_records.fetch_add(static_cast<int64_t>(dlq_buffer_.size()));
        dlq_buffer_.clear();
    }

    // ------------------------------------------------------------------------
    // Member variables
    // ------------------------------------------------------------------------

    // Configuration
    std::string write_url_;
    std::string auth_token_;
    std::string org_;
    std::string bucket_;
    std::string measurement_;
    std::string timestamp_field_;
    std::string tag_fields_str_;
    std::string field_fields_str_;
    std::string retention_policy_;
    std::string dlq_topic_;

    InfluxPrecision precision_ = InfluxPrecision::milliseconds;
    bool measurement_from_topic_ = true;
    bool use_compression_ = true;

    std::set<std::string> tag_fields_;
    std::set<std::string> field_fields_;

    size_t    batch_size_       = kDefaultBatchSize;
    int64_t   flush_timeout_ms_ = kDefaultFlushTimeoutMs;
    int64_t   max_retries_      = kDefaultMaxRetries;

    // State
    std::atomic<bool> stopped_{false};
    std::vector<SinkRecord> buffer_;
    std::unordered_map<std::string, PartitionOffset> offset_tracker_;
    std::deque<DlqRecord> dlq_buffer_;

    // Metrics
    struct {
        int64_t records_written  = 0;
        int64_t batches_flushed  = 0;
        int64_t write_errors     = 0;
        int64_t parse_errors     = 0;
        int64_t dlq_records      = 0;
        std::chrono::steady_clock::time_point created_at;
    } metrics_;
};

} // namespace torrent::connectors
