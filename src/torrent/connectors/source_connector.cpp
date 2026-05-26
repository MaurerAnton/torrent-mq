/**
 * source_connector.cpp — SourceConnector Base Class and Built-in Implementations
 *
 * Provides the SourceConnector abstract base class and concrete implementations
 * for common source systems.  A SourceConnector:
 *
 *   1. Is instantiated by ConnectFramework with a name and JSON config.
 *   2. Decomposes the data source into one or more tasks (polling units).
 *   3. Polls each task for new records and produces them to torrent topics.
 *   4. Tracks per-task offsets so that on restart it resumes from the
 *      last-committed position.
 *   5. Reports metrics for throughput, latency, and error counts.
 *
 * Built-in implementations:
 *   - FileSourceConnector: tails files and produces each line as a record
 *   - JdbcSourceConnector stub: incremental queries against a relational DB
 *
 * Extending:
 *   Subclass SourceConnector, implement poll() and task management, then
 *   register the class via REGISTER_SOURCE_CONNECTOR(ClassName).
 *
 * Thread-safety:
 *   Polling is single-threaded per connector (the ConnectFramework worker
 *   thread).  Status reporting is read-only and may be called from any thread.
 *
 * Dependencies:
 *   - connect_framework.cpp (ConnectFramework for offset tracking)
 *   - BrokerServer for produce APIs
 *   - backoff.h for retry logic
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
#include <cstdio>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
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

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace torrent::connectors {

// ============================================================================
// SourceConnector — Base class
// ============================================================================

/**
 * Abstract base for all source connectors.
 *
 * Subclasses must implement:
 *   - start(): validate config, create tasks
 *   - stop(): graceful shutdown
 *   - poll(): return list of records for this cycle
 *   - status(): JSON status snapshot
 *   - connector_class(): return class name string
 */
class SourceConnector {
public:
    SourceConnector()  = default;
    virtual ~SourceConnector() = default;

    SourceConnector(const SourceConnector&) = delete;
    SourceConnector& operator=(const SourceConnector&) = delete;
    SourceConnector(SourceConnector&&) = delete;
    SourceConnector& operator=(SourceConnector&&) = delete;

    // ---- Lifecycle ----

    /// Initialise the connector. Called exactly once before the first poll().
    /// @param name       connector instance name (unique within the framework)
    /// @param config     JSON configuration object
    /// @param framework  pointer to the owning ConnectFramework
    virtual void start(const std::string& name, const json& config,
                       class ConnectFramework* framework) = 0;

    /// Shut down the connector gracefully. No more poll() calls after this.
    virtual void stop() = 0;

    // ---- Data ----

    /// Poll for new records. Called periodically by the worker thread.
    /// @return A vector of records, each a JSON object with at minimum:
    ///         {"topic": "...", "key": "...", "value": {...}, "partition": N}
    virtual std::vector<json> poll() = 0;

    // ---- Status ----

    /// Return a JSON object describing current connector state for monitoring.
    virtual json status() const = 0;

    // ---- Metadata ----

    /// Return the connector class name (e.g. "FileSource", "JdbcSource").
    virtual std::string connector_class() const = 0;

protected:
    std::string              name_;
    json                     config_;
    ConnectFramework*        framework_ = nullptr;
};

// ============================================================================
// SourceRecord — canonical source record structure
// ============================================================================

struct SourceRecord {
    std::string topic;
    std::string key;
    json        value;
    int32_t     partition = 0;
    offset_t    source_offset = kInvalidOffset;
    int64_t     timestamp_ms  = 0;

    [[nodiscard]] json to_json() const {
        json j;
        j["topic"]     = topic;
        j["key"]       = key;
        j["value"]     = value;
        j["partition"] = partition;
        if (source_offset != kInvalidOffset)
            j["source_offset"] = source_offset;
        j["timestamp"] = timestamp_ms;
        return j;
    }
};

// ============================================================================
// Anonymous namespace — helpers, metrics structs
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_source_logger() {
    static auto logger = spdlog::get("source_connector");
    if (!logger) {
        logger = spdlog::stdout_color_mt("source_connector");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Source task descriptor
// --------------------------------------------------------------------------

struct SourceTask {
    int32_t     task_id       = 0;
    std::string task_name;
    json        task_config;

    // Offset tracking
    offset_t    current_offset = 0;
    bool        offset_committed = false;

    // Metrics
    int64_t     records_produced = 0;
    int64_t     bytes_produced   = 0;
    int64_t     errors           = 0;
    std::chrono::steady_clock::time_point last_poll;
    std::chrono::steady_clock::time_point created_at;
};

// --------------------------------------------------------------------------
// Helper: current time in ms since epoch
// --------------------------------------------------------------------------

[[nodiscard]] int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
}

// --------------------------------------------------------------------------
// Helper: read a config value with default
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

} // anonymous namespace

// ============================================================================
// FileSourceConnector — tails files, produces each line as a record
// ============================================================================

class FileSourceConnector final : public SourceConnector {
public:
    FileSourceConnector() = default;
    ~FileSourceConnector() override { stop(); }

    void start(const std::string& name, const json& config,
               ConnectFramework* framework) override
    {
        name_      = name;
        config_    = config;
        framework_ = framework;

        auto logger = get_source_logger();
        logger->info("FileSourceConnector '{}' starting", name_);

        // --- Validate config ---
        if (!config_.contains("file")) {
            throw std::invalid_argument(
                "FileSourceConnector requires 'file' in config");
        }

        std::string file_path = config_["file"].get<std::string>();
        if (!fs::exists(file_path)) {
            throw std::runtime_error("Source file does not exist: " + file_path);
        }
        if (!fs::is_regular_file(file_path)) {
            throw std::runtime_error("Source path is not a regular file: " +
                                     file_path);
        }

        // --- Read config options ---
        topic_        = config_get_string(config_, "topic", name_);
        batch_size_   = static_cast<size_t>(
            config_get_int(config_, "batch_size", 1000));
        tail_mode_    = config_.value("tail", true);
        encoding_     = config_get_string(config_, "encoding", "utf-8");
        max_line_bytes_ = static_cast<size_t>(
            config_get_int(config_, "max_line_bytes", 1048576));
        poll_interval_ms_ = config_get_int(config_, "poll_interval_ms", 5000);

        // --- Create single task ---
        SourceTask task;
        task.task_id   = 0;
        task.task_name = file_path;
        task.current_offset = 0;
        task.created_at = std::chrono::steady_clock::now();

        // Restore previous offset if available
        offset_t committed = framework_->committed_offset(name_, topic_, 0);
        if (committed != kInvalidOffset) {
            task.current_offset = committed;
            logger->info("FileSourceConnector '{}' resuming from offset {}",
                          name_, committed);
        }

        tasks_.clear();
        tasks_.push_back(std::move(task));

        // --- Open file ---
        file_stream_.open(file_path, std::ios::in);
        if (!file_stream_.is_open()) {
            throw std::runtime_error("Failed to open file: " + file_path);
        }

        // Seek to last offset if resuming
        if (tasks_[0].current_offset > 0) {
            file_stream_.seekg(static_cast<std::streamoff>(
                tasks_[0].current_offset));
        }

        stopped_.store(false);
        logger->info("FileSourceConnector '{}' started (file={}, topic={})",
                      name_, file_path, topic_);
    }

    void stop() override {
        if (stopped_.exchange(true)) return;

        get_source_logger()->info("FileSourceConnector '{}' stopping", name_);

        if (file_stream_.is_open()) {
            file_stream_.close();
        }

        // Final offset commit
        if (framework_ && !tasks_.empty()) {
            framework_->commit_offset(name_, topic_, 0,
                                       tasks_[0].current_offset);
        }

        tasks_.clear();
    }

    std::vector<json> poll() override {
        if (stopped_.load(std::memory_order_acquire)) return {};

        std::vector<json> records;
        records.reserve(batch_size_);

        auto& task = tasks_[0];
        task.last_poll = std::chrono::steady_clock::now();

        // Re-open file if needed (log rotation support)
        std::string file_path = config_["file"].get<std::string>();
        if (!file_stream_.is_open()) {
            file_stream_.open(file_path, std::ios::in);
            if (tail_mode_) {
                // In tail mode, seek to end for new data
                file_stream_.seekg(0, std::ios::end);
                task.current_offset = static_cast<offset_t>(file_stream_.tellg());
            } else {
                file_stream_.seekg(static_cast<std::streamoff>(
                    task.current_offset));
            }
        }

        // Read up to batch_size_ lines
        std::string line;
        line.reserve(4096);

        for (size_t i = 0; i < batch_size_; ++i) {
            if (!std::getline(file_stream_, line)) {
                // EOF or error
                if (file_stream_.eof()) {
                    file_stream_.clear();  // clear EOF flag
                    if (tail_mode_ && fs::exists(file_path)) {
                        // Clear EOF and retry on next poll (file may have
                        // grown)
                        file_stream_.clear();
                    }
                }
                break;
            }

            // Skip empty lines unless configured
            if (line.empty() && config_.value("skip_empty_lines", true))
                continue;

            // Build record
            SourceRecord rec;
            rec.topic         = topic_;
            rec.key           = std::to_string(task.current_offset);
            rec.value         = {{"line", line},
                                 {"offset", task.current_offset},
                                 {"source_file", file_path}};
            rec.partition     = 0;
            rec.source_offset = task.current_offset;
            rec.timestamp_ms  = now_ms();

            records.push_back(rec.to_json());

            // Update offset
            task.current_offset += static_cast<offset_t>(line.size() + 1); // +1 for newline
            task.records_produced++;
            task.bytes_produced += static_cast<int64_t>(line.size());
            line.clear();
        }

        // Commit offset after batch
        if (framework_ && task.current_offset > 0) {
            framework_->commit_offset(name_, topic_, 0, task.current_offset);
            task.offset_committed = true;
        }

        return records;
    }

    json status() const override {
        json j;
        j["connector_class"] = "FileSourceConnector";
        j["file"]            = config_get_string(config_, "file");
        j["topic"]           = topic_;
        j["tail_mode"]       = tail_mode_;
        j["stopped"]         = stopped_.load();

        if (!tasks_.empty()) {
            auto& t = tasks_[0];
            j["current_offset"]     = t.current_offset;
            j["records_produced"]   = t.records_produced;
            j["bytes_produced"]     = t.bytes_produced;
            j["errors"]             = t.errors;
        }

        return j;
    }

    std::string connector_class() const override {
        return "FileSourceConnector";
    }

private:
    std::string               topic_;
    bool                      tail_mode_       = true;
    std::string               encoding_        = "utf-8";
    size_t                    batch_size_      = 1000;
    size_t                    max_line_bytes_  = 1 << 20;
    int64_t                   poll_interval_ms_ = 5000;
    std::ifstream             file_stream_;
    std::vector<SourceTask>   tasks_;
    std::atomic<bool>         stopped_{true};
};

// ============================================================================
// JdbcSourceConnector — stub for JDBC incremental queries
// ============================================================================

class JdbcSourceConnector final : public SourceConnector {
public:
    JdbcSourceConnector() = default;
    ~JdbcSourceConnector() override { stop(); }

    void start(const std::string& name, const json& config,
               ConnectFramework* framework) override
    {
        name_      = name;
        config_    = config;
        framework_ = framework;

        auto logger = get_source_logger();
        logger->info("JdbcSourceConnector '{}' starting (STUB)", name_);

        // --- Validate config ---
        connection_url_   = config_get_string(config_, "connection.url");
        table_name_       = config_get_string(config_, "table.name");
        topic_prefix_     = config_get_string(config_, "topic.prefix", name_);
        incrementing_col_ = config_get_string(config_, "incrementing.column.name", "id");
        poll_interval_ms_ = config_get_int(config_, "poll.interval.ms", 5000);
        batch_max_rows_   = config_get_int(config_, "batch.max.rows", 1000);

        if (connection_url_.empty()) {
            throw std::invalid_argument(
                "JdbcSourceConnector requires 'connection.url' in config");
        }
        if (table_name_.empty()) {
            throw std::invalid_argument(
                "JdbcSourceConnector requires 'table.name' in config");
        }

        // --- Create task for this table ---
        SourceTask task;
        task.task_id   = 0;
        task.task_name = table_name_;
        task.current_offset = 0;
        task.created_at = std::chrono::steady_clock::now();

        // Restore previous offset
        offset_t committed = framework_->committed_offset(name_, topic_prefix_, 0);
        if (committed != kInvalidOffset) {
            task.current_offset = committed;
            logger->info("JdbcSourceConnector '{}' resuming from offset {}",
                          name_, committed);
        }

        tasks_.clear();
        tasks_.push_back(std::move(task));

        stopped_.store(false);
        logger->info("JdbcSourceConnector '{}' started (table={}) [STUB — no real JDBC]",
                      name_, table_name_);
    }

    void stop() override {
        if (stopped_.exchange(true)) return;

        get_source_logger()->info("JdbcSourceConnector '{}' stopping", name_);

        if (framework_ && !tasks_.empty()) {
            framework_->commit_offset(name_, topic_prefix_, 0,
                                       tasks_[0].current_offset);
        }

        tasks_.clear();
    }

    std::vector<json> poll() override {
        // Stub implementation — in production this would:
        //   1. Open a JDBC connection (or use connection pool)
        //   2. Execute: SELECT * FROM table WHERE incrementing_col > offset
        //              ORDER BY incrementing_col ASC LIMIT batch_max_rows
        //   3. Map rows to SourceRecord
        //   4. Update offset

        if (stopped_.load(std::memory_order_acquire)) return {};

        auto& task = tasks_[0];
        task.last_poll = std::chrono::steady_clock::now();

        // Stub: produce one dummy record per poll cycle for testing
        std::vector<json> records;

        get_source_logger()->trace(
            "JdbcSourceConnector '{}': stub poll (offset={})",
            name_, task.current_offset);

        // Return empty — real implementation would fill records here
        return records;
    }

    json status() const override {
        json j;
        j["connector_class"]  = "JdbcSourceConnector";
        j["connection_url"]   = connection_url_;
        j["table"]            = table_name_;
        j["topic_prefix"]     = topic_prefix_;
        j["incrementing_col"] = incrementing_col_;
        j["stopped"]          = stopped_.load();

        if (!tasks_.empty()) {
            auto& t = tasks_[0];
            j["current_offset"]   = t.current_offset;
            j["records_produced"] = t.records_produced;
            j["errors"]           = t.errors;
        }

        return j;
    }

    std::string connector_class() const override {
        return "JdbcSourceConnector";
    }

private:
    std::string               connection_url_;
    std::string               table_name_;
    std::string               topic_prefix_;
    std::string               incrementing_col_;
    int64_t                   poll_interval_ms_ = 5000;
    int64_t                   batch_max_rows_   = 1000;
    std::vector<SourceTask>   tasks_;
    std::atomic<bool>         stopped_{true};
};

// ============================================================================
// Factory registration
// ============================================================================

namespace {

struct SourceConnectorRegistrar {
    static void init() {
        auto& registry = ConnectorRegistry::instance();
        registry.register_source_factory("FileSourceConnector", [] {
            return std::make_unique<FileSourceConnector>();
        });
        registry.register_source_factory("JdbcSourceConnector", [] {
            return std::make_unique<JdbcSourceConnector>();
        });
        get_source_logger()->info(
            "Registered source connectors: FileSourceConnector, JdbcSourceConnector");
    }
};

// Trigger registration at static initialization
static struct SourceConnectorAutoReg {
    SourceConnectorAutoReg() { SourceConnectorRegistrar::init(); }
} s_auto_reg_source;

} // anonymous namespace

} // namespace torrent::connectors
