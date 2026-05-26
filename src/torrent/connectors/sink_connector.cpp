/**
 * sink_connector.cpp — SinkConnector Base Class and Built-in Implementations
 *
 * Provides the SinkConnector abstract base class and concrete implementations
 * for common sink systems.  A SinkConnector:
 *
 *   1. Is instantiated by ConnectFramework with a name and JSON config.
 *   2. Receives records via put() from the framework's consumer loop.
 *   3. Periodically flush()es buffered records to the external system.
 *   4. Commits offsets after successful flush so that on restart no records
 *      are duplicated (at-least-once) or skipped (exactly-once via idempotent
 *      writes).
 *   5. Reports metrics for throughput, latency, and error counts.
 *
 * Built-in implementations:
 *   - JdbcSinkConnector stub: INSERT/UPDATE rows into a relational DB
 *   - S3SinkConnector stub: write records as JSON/Parquet objects to S3
 *
 * Extending:
 *   Subclass SinkConnector, implement put(), flush(), and status(), then
 *   register via REGISTER_SINK_CONNECTOR(ClassName).
 *
 * Exactly-once semantics:
 *   Sink connectors must be idempotent — re-delivering the same set of
 *   records after a crash must produce the same external state.  The
 *   framework commits offsets only after a successful flush().
 *
 * Thread-safety:
 *   put() and flush() are called serially by the framework worker thread.
 *   status() is read-only and may be called from any thread.
 *
 * Dependencies:
 *   - connect_framework.cpp (ConnectFramework for offset commit)
 *   - BrokerServer for consume APIs
 *   - backoff.h for retry logic in flush()
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
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
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
// SinkConnector — Base class
// ============================================================================

/**
 * Abstract base for all sink connectors.
 *
 * Subclasses must implement:
 *   - start(): validate config, initialise external connection
 *   - stop(): graceful shutdown, final flush
 *   - put(): buffer a batch of records
 *   - flush(): write buffered records to external system
 *   - status(): JSON status snapshot
 *   - connector_class(): return class name string
 */
class SinkConnector {
public:
    SinkConnector()  = default;
    virtual ~SinkConnector() = default;

    SinkConnector(const SinkConnector&) = delete;
    SinkConnector& operator=(const SinkConnector&) = delete;
    SinkConnector(SinkConnector&&) = delete;
    SinkConnector& operator=(SinkConnector&&) = delete;

    // ---- Lifecycle ----

    /// Initialise the connector. Called once before the first put().
    /// @param name       connector instance name
    /// @param config     JSON configuration object
    /// @param framework  pointer to the owning ConnectFramework
    virtual void start(const std::string& name, const json& config,
                       class ConnectFramework* framework) = 0;

    /// Shut down. Performs a final flush() before returning.
    virtual void stop() = 0;

    // ---- Data ingestion ----

    /// Buffer a batch of records for writing. Records are JSON objects
    /// with at minimum: {"topic": "...", "key": "...", "value": {...}}.
    /// This method should be fast (append to internal buffer).
    virtual void put(const std::vector<json>& records) = 0;

    /// Flush all buffered records to the external system. Called
    /// periodically and during stop().  Must be idempotent or at-least-once
    /// safe — offset commit happens only after successful flush.
    virtual void flush() = 0;

    // ---- Status ----

    /// Return a JSON object describing current connector state.
    virtual json status() const = 0;

    // ---- Metadata ----

    /// Return the connector class name (e.g. "JdbcSink", "S3Sink").
    virtual std::string connector_class() const = 0;

protected:
    std::string              name_;
    json                     config_;
    ConnectFramework*        framework_ = nullptr;
};

// ============================================================================
// SinkRecord — canonical sink record structure
// ============================================================================

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
// Anonymous namespace — helpers, metrics
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_sink_logger() {
    static auto logger = spdlog::get("sink_connector");
    if (!logger) {
        logger = spdlog::stdout_color_mt("sink_connector");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Sink metrics
// --------------------------------------------------------------------------

struct SinkMetrics {
    std::atomic<int64_t> records_received{0};
    std::atomic<int64_t> records_written{0};
    std::atomic<int64_t> bytes_written{0};
    std::atomic<int64_t> flushes{0};
    std::atomic<int64_t> flush_errors{0};
    std::atomic<int64_t> put_errors{0};
    std::chrono::steady_clock::time_point last_flush;
    std::chrono::steady_clock::time_point created_at;

    [[nodiscard]] json to_json() const {
        json j;
        j["records_received"] = records_received.load();
        j["records_written"]  = records_written.load();
        j["bytes_written"]    = bytes_written.load();
        j["flushes"]          = flushes.load();
        j["flush_errors"]     = flush_errors.load();
        j["put_errors"]       = put_errors.load();
        return j;
    }
};

// --------------------------------------------------------------------------
// Helper: config accessors
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
// Offset tracking: track the highest offset per partition
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
// Helper: compute bytes in a JSON value (approximate)
// --------------------------------------------------------------------------

[[nodiscard]] int64_t json_byte_size(const json& j) {
    // Fast path: string
    if (j.is_string()) return static_cast<int64_t>(j.get<std::string>().size());
    // Serialise for accurate size
    std::string s = j.dump();
    return static_cast<int64_t>(s.size());
}

} // anonymous namespace

// ============================================================================
// JdbcSinkConnector — stub that demonstrates the sink pattern
// ============================================================================

class JdbcSinkConnector final : public SinkConnector {
public:
    JdbcSinkConnector() = default;
    ~JdbcSinkConnector() override { stop(); }

    void start(const std::string& name, const json& config,
               ConnectFramework* framework) override
    {
        name_      = name;
        config_    = config;
        framework_ = framework;

        auto logger = get_sink_logger();
        logger->info("JdbcSinkConnector '{}' starting (STUB)", name_);

        // --- Validate config ---
        connection_url_  = config_get_string(config_, "connection.url");
        table_name_      = config_get_string(config_, "table.name");
        insert_mode_     = config_get_string(config_, "insert.mode", "insert");
        batch_size_      = static_cast<size_t>(
            config_get_int(config_, "batch.size", 100));
        max_retries_     = config_get_int(config_, "max.retries", 3);
        pk_mode_         = config_get_string(config_, "pk.mode", "none");
        pk_fields_       = config_get_string(config_, "pk.fields", "");

        if (connection_url_.empty()) {
            throw std::invalid_argument(
                "JdbcSinkConnector requires 'connection.url' in config");
        }
        if (table_name_.empty()) {
            throw std::invalid_argument(
                "JdbcSinkConnector requires 'table.name' in config");
        }

        // --- State ---
        metrics_.created_at = std::chrono::steady_clock::now();
        stopped_.store(false);
        buffer_.reserve(batch_size_);

        logger->info(
            "JdbcSinkConnector '{}' started (table={}, insert_mode={}) [STUB — no real JDBC]",
            name_, table_name_, insert_mode_);
    }

    void stop() override {
        if (stopped_.exchange(true)) return;

        auto logger = get_sink_logger();
        logger->info("JdbcSinkConnector '{}' stopping", name_);

        // Final flush
        if (!buffer_.empty()) {
            logger->info("JdbcSinkConnector '{}' final flush ({} records)",
                          name_, buffer_.size());
            try {
                do_flush();
            } catch (const std::exception& e) {
                logger->error("JdbcSinkConnector '{}': final flush failed: {}",
                               name_, e.what());
                metrics_.flush_errors.fetch_add(1);
            }
        }

        buffer_.clear();
        offset_tracker_.clear();

        logger->info("JdbcSinkConnector '{}' stopped", name_);
    }

    void put(const std::vector<json>& records) override {
        if (stopped_.load(std::memory_order_acquire)) return;

        auto logger = get_sink_logger();
        metrics_.records_received.fetch_add(static_cast<int64_t>(records.size()));

        for (auto& rec : records) {
            try {
                SinkRecord sr = SinkRecord::from_json(rec);
                buffer_.push_back(std::move(sr));

                // Track max offset per partition for commit
                std::string okey = offset_key(sr.topic, sr.partition);
                auto& existing = offset_tracker_[okey];
                if (sr.offset > existing.max_offset) {
                    existing.topic    = sr.topic;
                    existing.partition = sr.partition;
                    existing.max_offset = sr.offset;
                }
            } catch (const std::exception& e) {
                logger->error("JdbcSinkConnector '{}': put error: {}", name_, e.what());
                metrics_.put_errors.fetch_add(1);
            }
        }

        // Auto-flush if buffer exceeds batch_size_
        if (buffer_.size() >= batch_size_) {
            try {
                flush();
            } catch (const std::exception& e) {
                logger->error("JdbcSinkConnector '{}': auto-flush failed: {}",
                               name_, e.what());
            }
        }
    }

    void flush() override {
        if (stopped_.load(std::memory_order_acquire)) return;
        if (buffer_.empty()) return;

        do_flush();
    }

    json status() const override {
        json j;
        j["connector_class"] = "JdbcSinkConnector";
        j["connection_url"]  = connection_url_;
        j["table"]           = table_name_;
        j["insert_mode"]     = insert_mode_;
        j["batch_size"]      = batch_size_;
        j["buffered"]        = buffer_.size();
        j["stopped"]         = stopped_.load();
        j["metrics"]         = metrics_.to_json();
        return j;
    }

    std::string connector_class() const override {
        return "JdbcSinkConnector";
    }

private:
    void do_flush() {
        auto logger = get_sink_logger();

        // --- Retry loop with exponential backoff ---
        ExponentialBackoff backoff(
            std::chrono::milliseconds(100),
            std::chrono::seconds(30),
            2.0, 0.25,
            static_cast<int32_t>(max_retries_));

        while (true) {
            try {
                // Stub: in production this would:
                //   1. Open JDBC connection / use connection pool
                //   2. Build batch INSERT/UPSERT/MERGE statement
                //   3. Execute batch
                //   4. Commit transaction

                int64_t bytes = 0;
                for (auto& rec : buffer_) {
                    bytes += json_byte_size(rec.value);

                    logger->trace(
                        "JdbcSink '{}': stub write to {}: key={} value={}",
                        name_, table_name_, rec.key,
                        rec.value.dump().substr(0, 200));
                }

                // Simulate successful flush
                size_t flushed_count = buffer_.size();
                int64_t flushed_bytes = bytes;

                logger->info("JdbcSinkConnector '{}': flushed {} records ({} bytes) [STUB]",
                              name_, flushed_count, flushed_bytes);

                // Update metrics
                metrics_.records_written.fetch_add(
                    static_cast<int64_t>(flushed_count));
                metrics_.bytes_written.fetch_add(flushed_bytes);
                metrics_.flushes.fetch_add(1);
                metrics_.last_flush = std::chrono::steady_clock::now();

                // Commit offsets after successful flush
                if (framework_) {
                    for (auto& [key, po] : offset_tracker_) {
                        if (po.max_offset != kInvalidOffset) {
                            framework_->commit_offset(name_, po.topic,
                                                       po.partition,
                                                       po.max_offset);
                            logger->trace("JdbcSink '{}': offset committed "
                                           "topic={} partition={} offset={}",
                                           name_, po.topic, po.partition,
                                           po.max_offset);
                        }
                    }
                }

                buffer_.clear();
                offset_tracker_.clear();
                return;

            } catch (const std::exception& e) {
                logger->error("JdbcSinkConnector '{}': flush error (attempt {}): {}",
                               name_, backoff.attempts(), e.what());

                if (backoff.exhausted()) {
                    metrics_.flush_errors.fetch_add(1);
                    throw;  // re-throw after max retries exhausted
                }

                auto delay = backoff.next_delay();
                logger->warn("JdbcSinkConnector '{}': retrying in {}ms",
                              name_, delay.count());
                std::this_thread::sleep_for(delay);
            }
        }
    }

    std::string                          connection_url_;
    std::string                          table_name_;
    std::string                          insert_mode_    = "insert";
    size_t                               batch_size_     = 100;
    int64_t                              max_retries_    = 3;
    std::string                          pk_mode_;
    std::string                          pk_fields_;

    std::vector<SinkRecord>              buffer_;
    std::unordered_map<std::string, PartitionOffset> offset_tracker_;
    SinkMetrics                          metrics_;
    std::atomic<bool>                    stopped_{true};
};

// ============================================================================
// S3SinkConnector — stub for writing objects to S3 / object storage
// ============================================================================

class S3SinkConnector final : public SinkConnector {
public:
    S3SinkConnector() = default;
    ~S3SinkConnector() override { stop(); }

    void start(const std::string& name, const json& config,
               ConnectFramework* framework) override
    {
        name_      = name;
        config_    = config;
        framework_ = framework;

        auto logger = get_sink_logger();
        logger->info("S3SinkConnector '{}' starting (STUB)", name_);

        // --- Validate config ---
        bucket_          = config_get_string(config_, "s3.bucket.name");
        region_          = config_get_string(config_, "s3.region", "us-east-1");
        endpoint_        = config_get_string(config_, "s3.endpoint", "");
        prefix_          = config_get_string(config_, "s3.prefix", "");
        format_          = config_get_string(config_, "format.output.type", "json");
        flush_size_      = static_cast<size_t>(
            config_get_int(config_, "flush.size", 10000));
        rotate_interval_ms_ = config_get_int(config_, "rotate.interval.ms", 600000);
        compression_     = config_get_string(config_, "s3.compression.type", "none");
        part_size_       = static_cast<size_t>(
            config_get_int(config_, "s3.part.size", 25 * 1024 * 1024));

        if (bucket_.empty()) {
            throw std::invalid_argument(
                "S3SinkConnector requires 's3.bucket.name' in config");
        }

        // --- State ---
        metrics_.created_at = std::chrono::steady_clock::now();
        stopped_.store(false);
        buffer_.reserve(flush_size_);

        logger->info("S3SinkConnector '{}' started (bucket={}, format={}) [STUB — no real S3]",
                      name_, bucket_, format_);
    }

    void stop() override {
        if (stopped_.exchange(true)) return;

        auto logger = get_sink_logger();
        logger->info("S3SinkConnector '{}' stopping", name_);

        if (!buffer_.empty()) {
            logger->info("S3SinkConnector '{}' final flush ({} records)",
                          name_, buffer_.size());
            try {
                do_flush();
            } catch (const std::exception& e) {
                logger->error("S3SinkConnector '{}': final flush failed: {}",
                               name_, e.what());
                metrics_.flush_errors.fetch_add(1);
            }
        }

        buffer_.clear();
        offset_tracker_.clear();
        logger->info("S3SinkConnector '{}' stopped", name_);
    }

    void put(const std::vector<json>& records) override {
        if (stopped_.load(std::memory_order_acquire)) return;

        metrics_.records_received.fetch_add(static_cast<int64_t>(records.size()));

        for (auto& rec : records) {
            try {
                SinkRecord sr = SinkRecord::from_json(rec);
                buffer_.push_back(std::move(sr));

                std::string okey = offset_key(sr.topic, sr.partition);
                auto& existing = offset_tracker_[okey];
                if (sr.offset > existing.max_offset) {
                    existing.topic     = sr.topic;
                    existing.partition = sr.partition;
                    existing.max_offset = sr.offset;
                }
            } catch (const std::exception& e) {
                get_sink_logger()->error("S3SinkConnector '{}': put error: {}",
                                          name_, e.what());
                metrics_.put_errors.fetch_add(1);
            }
        }

        if (buffer_.size() >= flush_size_) {
            try {
                flush();
            } catch (const std::exception& e) {
                get_sink_logger()->error("S3SinkConnector '{}': auto-flush failed: {}",
                                          name_, e.what());
            }
        }
    }

    void flush() override {
        if (stopped_.load(std::memory_order_acquire)) return;
        if (buffer_.empty()) return;
        do_flush();
    }

    json status() const override {
        json j;
        j["connector_class"] = "S3SinkConnector";
        j["bucket"]          = bucket_;
        j["region"]          = region_;
        j["prefix"]          = prefix_;
        j["format"]          = format_;
        j["compression"]     = compression_;
        j["buffered"]        = buffer_.size();
        j["stopped"]         = stopped_.load();
        j["metrics"]         = metrics_.to_json();
        return j;
    }

    std::string connector_class() const override {
        return "S3SinkConnector";
    }

private:
    void do_flush() {
        auto logger = get_sink_logger();

        // --- Retry loop ---
        ExponentialBackoff backoff(
            std::chrono::milliseconds(200),
            std::chrono::seconds(60),
            2.0, 0.25, 5);

        while (true) {
            try {
                // Stub: in production this would:
                //   1. Serialise batch to JSON lines / Parquet / Avro
                //   2. Optionally compress
                //   3. Generate S3 key: prefix/YYYY/MM/DD/HH/connector-uuid.ext
                //   4. Initiate multipart upload if > part_size_
                //   5. Upload parts
                //   6. Complete multipart upload

                int64_t bytes = 0;
                for (auto& rec : buffer_) {
                    bytes += json_byte_size(rec.value);
                }

                // Build S3 key
                std::ostringstream key_oss;
                if (!prefix_.empty()) key_oss << prefix_ << "/";
                auto now = std::chrono::system_clock::now();
                auto t   = std::chrono::system_clock::to_time_t(now);
                std::tm tm;
                localtime_r(&t, &tm);
                key_oss << std::put_time(&tm, "%Y/%m/%d/%H/")
                        << name_ << "-"
                        << std::chrono::duration_cast<std::chrono::milliseconds>(
                               now.time_since_epoch()).count()
                        << "." << format_;
                if (compression_ != "none") key_oss << "." << compression_;

                std::string s3_key = key_oss.str();

                size_t flushed_count  = buffer_.size();
                int64_t flushed_bytes = bytes;

                logger->info(
                    "S3SinkConnector '{}': flushed {} records ({} bytes) → "
                    "s3://{}/{} [STUB]",
                    name_, flushed_count, flushed_bytes, bucket_, s3_key);

                metrics_.records_written.fetch_add(
                    static_cast<int64_t>(flushed_count));
                metrics_.bytes_written.fetch_add(flushed_bytes);
                metrics_.flushes.fetch_add(1);
                metrics_.last_flush = std::chrono::steady_clock::now();

                // Commit offsets
                if (framework_) {
                    for (auto& [key, po] : offset_tracker_) {
                        if (po.max_offset != kInvalidOffset) {
                            framework_->commit_offset(name_, po.topic,
                                                       po.partition,
                                                       po.max_offset);
                        }
                    }
                }

                buffer_.clear();
                offset_tracker_.clear();
                return;

            } catch (const std::exception& e) {
                logger->error("S3SinkConnector '{}': flush error (attempt {}): {}",
                               name_, backoff.attempts(), e.what());

                if (backoff.exhausted()) {
                    metrics_.flush_errors.fetch_add(1);
                    throw;
                }

                auto delay = backoff.next_delay();
                logger->warn("S3SinkConnector '{}': retrying in {}ms",
                              name_, delay.count());
                std::this_thread::sleep_for(delay);
            }
        }
    }

    std::string                          bucket_;
    std::string                          region_          = "us-east-1";
    std::string                          endpoint_;
    std::string                          prefix_;
    std::string                          format_          = "json";
    size_t                               flush_size_      = 10000;
    int64_t                              rotate_interval_ms_ = 600000;
    std::string                          compression_     = "none";
    size_t                               part_size_       = 25 * 1024 * 1024;

    std::vector<SinkRecord>              buffer_;
    std::unordered_map<std::string, PartitionOffset> offset_tracker_;
    SinkMetrics                          metrics_;
    std::atomic<bool>                    stopped_{true};
};

// ============================================================================
// Factory registration
// ============================================================================

namespace {

struct SinkConnectorRegistrar {
    static void init() {
        auto& registry = ConnectorRegistry::instance();
        registry.register_sink_factory("JdbcSinkConnector", [] {
            return std::make_unique<JdbcSinkConnector>();
        });
        registry.register_sink_factory("S3SinkConnector", [] {
            return std::make_unique<S3SinkConnector>();
        });
        get_sink_logger()->info(
            "Registered sink connectors: JdbcSinkConnector, S3SinkConnector");
    }
};

static struct SinkConnectorAutoReg {
    SinkConnectorAutoReg() { SinkConnectorRegistrar::init(); }
} s_auto_reg_sink;

} // anonymous namespace

} // namespace torrent::connectors
