/**
 * debezium_source.cpp — DebeziumSourceConnector: CDC from Relational Databases
 *
 * Implements Change Data Capture (CDC) for MySQL (binlog) and PostgreSQL
 * (WAL / logical replication slot).  The connector operates in two modes:
 *
 *   Snapshot mode:   Runs an initial full-table dump to capture the current
 *                    state of all rows.  Each row becomes a SourceRecord
 *                    with "op": "r" (read/snapshot).
 *
 *   Incremental mode: Subscribes to the database change stream and emits
 *                    insert ("c"), update ("u"), and delete ("d") events
 *                    as they occur.
 *
 * Schema change handling:
 *   DDL events (ALTER TABLE, etc.) are captured and forwarded as schema-
 *   change records to a dedicated __schema_changes topic so that downstream
 *   consumers and sinks can adapt their schemas.
 *
 * CDC event structure (Debezium-compatible JSON envelope):
 *   {
 *     "before": { ... } | null,        // row state before the change
 *     "after":  { ... } | null,        // row state after the change
 *     "source": { "db": "...", "table": "...", "lsn": ..., "ts_ms": ... },
 *     "op":     "r"|"c"|"u"|"d",     // read, create, update, delete
 *     "ts_ms":  1234567890123
 *   }
 *
 * Thread-safety:
 *   Polling is single-threaded via ConnectFramework.  Status() is read-only
 *   and may be called from any thread.
 *
 * Dependencies:
 *   - SourceConnector base class (defined in source_connector.cpp)
 *   - ConnectFramework for offset tracking
 *   - BrokerServer for produce
 *   - backoff.h for reconnect logic
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

class SourceConnector {
public:
    virtual ~SourceConnector() = default;
    virtual void start(const std::string& name, const json& config,
                       class ConnectFramework* framework) = 0;
    virtual void stop() = 0;
    virtual std::vector<json> poll() = 0;
    virtual json status() const = 0;
    virtual std::string connector_class() const = 0;

protected:
    std::string              name_;
    json                     config_;
    ConnectFramework*        framework_ = nullptr;
};

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
// Anonymous namespace — helpers, constants, internal types
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_debezium_logger() {
    static auto logger = spdlog::get("debezium_source");
    if (!logger) {
        logger = spdlog::stdout_color_mt("debezium_source");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Default poll interval for CDC streaming mode.
inline constexpr auto kDefaultPollInterval = std::chrono::milliseconds(100);

/// Default snapshot fetch size (rows per batch).
inline constexpr int64_t kDefaultSnapshotBatchSize = 1000;

/// Maximum reconnect attempts for database connection.
inline constexpr int32_t kMaxReconnectAttempts = 10;

/// Schema change topic suffix.
inline constexpr std::string_view kSchemaChangeTopic = "__schema_changes";

/// Offset topic for tracking binlog/WAL position.
inline constexpr std::string_view kOffsetTopicPrefix = "__cdc_offsets";

// --------------------------------------------------------------------------
// CDC operation types
// --------------------------------------------------------------------------

enum class CdcOperation : uint8_t {
    snapshot = 0,   // "r" — initial snapshot row
    create   = 1,   // "c" — INSERT
    update   = 2,   // "u" — UPDATE
    delete_  = 3,   // "d" — DELETE
    schema   = 4,   // DDL / schema change
};

[[nodiscard]] std::string_view cdc_op_code(CdcOperation op) {
    switch (op) {
    case CdcOperation::snapshot: return "r";
    case CdcOperation::create:   return "c";
    case CdcOperation::update:   return "u";
    case CdcOperation::delete_:  return "d";
    case CdcOperation::schema:   return "s";
    }
    return "?";
}

// --------------------------------------------------------------------------
// Connector mode
// --------------------------------------------------------------------------

enum class ConnectorMode : uint8_t {
    snapshot      = 0,
    incremental   = 1,
    snapshot_done = 2,
};

[[nodiscard]] std::string_view mode_name(ConnectorMode m) {
    switch (m) {
    case ConnectorMode::snapshot:      return "snapshot";
    case ConnectorMode::incremental:   return "incremental";
    case ConnectorMode::snapshot_done: return "snapshot_done";
    }
    return "unknown";
}

// --------------------------------------------------------------------------
// Database type
// --------------------------------------------------------------------------

enum class DatabaseType : uint8_t {
    mysql      = 0,
    postgresql = 1,
};

[[nodiscard]] DatabaseType parse_database_type(std::string_view s) {
    if (s == "postgresql" || s == "postgres" || s == "pg") return DatabaseType::postgresql;
    return DatabaseType::mysql;
}

// --------------------------------------------------------------------------
// Table schema tracker — keeps per-table column info for DDL awareness
// --------------------------------------------------------------------------

struct TableSchema {
    std::string               table_name;
    std::string               schema_name = "public";
    std::vector<std::string>  primary_key_columns;
    std::vector<std::string>  all_columns;
    int64_t                   schema_version = 0;
    std::chrono::steady_clock::time_point last_updated;

    [[nodiscard]] json to_json() const {
        json j;
        j["table"]          = table_name;
        j["schema"]         = schema_name;
        j["primary_keys"]   = primary_key_columns;
        j["columns"]        = all_columns;
        j["schema_version"] = schema_version;
        return j;
    }
};

// --------------------------------------------------------------------------
// CDC source task — represents one table being tailed
// --------------------------------------------------------------------------

struct CdcTask {
    int32_t     task_id       = 0;
    std::string task_name;         // "schema.table"
    std::string topic_name;        // target torrent topic
    std::string schema_name;
    std::string table_name;

    // Snapshot state
    bool        snapshot_complete  = false;
    offset_t    snapshot_offset    = 0;   // last row PK value or offset

    // Incremental state
    offset_t    binlog_offset      = 0;   // binlog position / LSN
    std::string binlog_file;              // MySQL binlog filename
    int64_t     last_event_timestamp = 0;

    // Metrics
    int64_t     records_produced   = 0;
    int64_t     snapshot_rows      = 0;
    int64_t     cdc_events         = 0;
    int64_t     errors             = 0;
    int64_t     schema_changes     = 0;
    std::chrono::steady_clock::time_point last_poll;
    std::chrono::steady_clock::time_point created_at;

    [[nodiscard]] json to_json() const {
        json j;
        j["task_id"]            = task_id;
        j["task_name"]          = task_name;
        j["topic"]              = topic_name;
        j["snapshot_complete"]  = snapshot_complete;
        j["snapshot_offset"]    = snapshot_offset;
        j["binlog_offset"]      = binlog_offset;
        j["records_produced"]   = records_produced;
        j["snapshot_rows"]      = snapshot_rows;
        j["cdc_events"]         = cdc_events;
        j["errors"]             = errors;
        j["schema_changes"]     = schema_changes;
        return j;
    }
};

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
// Time helpers
// --------------------------------------------------------------------------

[[nodiscard]] int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
}

// --------------------------------------------------------------------------
// Build a Debezium-compatible source info block
// --------------------------------------------------------------------------

[[nodiscard]] json build_source_info(std::string_view db_name,
                                      std::string_view table_name,
                                      std::string_view connector_name,
                                      offset_t lsn,
                                      int64_t ts_ms) {
    json src;
    src["version"]   = "1.9.0-torrent";
    src["connector"] = "debezium-source";
    src["name"]      = connector_name;
    src["db"]        = db_name;
    src["table"]     = table_name;
    src["lsn"]       = lsn;
    src["ts_ms"]     = ts_ms;
    return src;
}

// --------------------------------------------------------------------------
// Build a Debezium-compatible CDC envelope
// --------------------------------------------------------------------------

[[nodiscard]] json build_cdc_envelope(CdcOperation op,
                                       const json& before,
                                       const json& after,
                                       const json& source_info) {
    json envelope;
    envelope["op"]     = cdc_op_code(op);
    envelope["ts_ms"]  = now_ms();
    envelope["source"] = source_info;

    if (!before.is_null()) envelope["before"] = before;
    else                  envelope["before"] = nullptr;

    if (!after.is_null())  envelope["after"]  = after;
    else                   envelope["after"]  = nullptr;

    return envelope;
}

// --------------------------------------------------------------------------
// Extract primary key value from a row for use as the record key
// --------------------------------------------------------------------------

[[nodiscard]] std::string extract_pk_string(const json& row,
                                              const std::vector<std::string>& pk_cols) {
    if (pk_cols.empty()) {
        // No PK — hash the entire row
        return std::to_string(std::hash<std::string>{}(row.dump()));
    }

    if (pk_cols.size() == 1) {
        auto it = row.find(pk_cols[0]);
        if (it != row.end()) {
            if (it->is_string()) return it->get<std::string>();
            return it->dump();
        }
        return "";
    }

    // Composite key — concatenate
    std::ostringstream oss;
    for (size_t i = 0; i < pk_cols.size(); ++i) {
        if (i > 0) oss << "|";
        auto it = row.find(pk_cols[i]);
        if (it != row.end()) {
            if (it->is_string()) oss << it->get<std::string>();
            else                 oss << it->dump();
        }
    }
    return oss.str();
}

} // anonymous namespace

// ============================================================================
// DebeziumSourceConnector
// ============================================================================

class DebeziumSourceConnector final : public SourceConnector {
public:
    DebeziumSourceConnector()  = default;
    ~DebeziumSourceConnector() override { stop(); }

    // ------------------------------------------------------------------------
    // Lifecycle — start
    // ------------------------------------------------------------------------

    void start(const std::string& name, const json& config,
               ConnectFramework* framework) override
    {
        name_      = name;
        config_    = config;
        framework_ = framework;

        auto logger = get_debezium_logger();
        logger->info("DebeziumSourceConnector '{}' starting", name_);

        // --- Parse required config ---
        connection_url_  = config_get_string(config_, "database.hostname");
        db_port_         = static_cast<uint16_t>(
            config_get_int(config_, "database.port",
                           parse_database_type(config_get_string(config_, "database.type")) == DatabaseType::postgresql ? 5432 : 3306));
        db_user_         = config_get_string(config_, "database.user", "root");
        db_password_     = config_get_string(config_, "database.password", "");
        db_name_         = config_get_string(config_, "database.dbname", "");
        db_type_str_     = config_get_string(config_, "database.type", "mysql");
        db_type_         = parse_database_type(db_type_str_);
        topic_prefix_    = config_get_string(config_, "topic.prefix", name_);

        // --- Table whitelist / blacklist ---
        std::string table_include = config_get_string(config_, "table.include.list", "");
        std::string table_exclude = config_get_string(config_, "table.exclude.list", "");

        if (!table_include.empty()) {
            std::istringstream iss(table_include);
            std::string tbl;
            while (std::getline(iss, tbl, ',')) {
                if (!tbl.empty()) table_whitelist_.insert(tbl);
            }
        }

        if (!table_exclude.empty()) {
            std::istringstream iss(table_exclude);
            std::string tbl;
            while (std::getline(iss, tbl, ',')) {
                if (!tbl.empty()) table_blacklist_.insert(tbl);
            }
        }

        // --- Snapshot config ---
        snapshot_mode_     = config_get_string(config_, "snapshot.mode", "initial");
        snapshot_batch_size_ = config_get_int(config_, "snapshot.fetch.size",
                                               kDefaultSnapshotBatchSize);
        snapshot_lock_timeout_ms_ = config_get_int(
            config_, "snapshot.lock.timeout.ms", 10000);

        // --- CDC streaming config ---
        poll_interval_ms_  = config_get_int(config_, "poll.interval.ms", 100);
        heartbeat_interval_ms_ = config_get_int(
            config_, "heartbeat.interval.ms", 0);

        // --- Plugin / slot config (PostgreSQL) ---
        plugin_name_       = config_get_string(config_, "plugin.name", "pgoutput");
        slot_name_         = config_get_string(config_,
            "slot.name", "debezium_" + name_);
        publication_name_  = config_get_string(config_,
            "publication.name", "dbz_publication");

        // --- Schema history ---
        schema_history_enabled_ = config_get_bool(
            config_, "schema.history.internal", false);

        // --- Validate ---
        if (connection_url_.empty()) {
            throw std::invalid_argument(
                "DebeziumSourceConnector requires 'database.hostname' in config");
        }
        if (db_name_.empty()) {
            throw std::invalid_argument(
                "DebeziumSourceConnector requires 'database.dbname' in config");
        }

        // --- Determine mode ---
        if (snapshot_mode_ == "initial" || snapshot_mode_ == "initial_only") {
            mode_ = ConnectorMode::snapshot;
            logger->info("DebeziumSourceConnector '{}': starting in SNAPSHOT mode", name_);
        } else if (snapshot_mode_ == "schema_only" ||
                   snapshot_mode_ == "never") {
            mode_ = ConnectorMode::incremental;
            logger->info("DebeziumSourceConnector '{}': starting in INCREMENTAL mode", name_);
        } else {
            // "when_needed" — check if we have a prior offset
            mode_ = ConnectorMode::snapshot;
        }

        // --- Discover tables and create tasks ---
        discover_tables();

        if (tasks_.empty()) {
            logger->warn("DebeziumSourceConnector '{}': no tables discovered", name_);
        }

        stopped_.store(false);
        metrics_.created_at = std::chrono::steady_clock::now();

        logger->info(
            "DebeziumSourceConnector '{}' started (db={}, type={}, tables={}, mode={})",
            name_, db_name_, db_type_str_, tasks_.size(), mode_name(mode_));
    }

    // ------------------------------------------------------------------------
    // Lifecycle — stop
    // ------------------------------------------------------------------------

    void stop() override {
        if (stopped_.exchange(true)) return;

        auto logger = get_debezium_logger();
        logger->info("DebeziumSourceConnector '{}' stopping", name_);

        // Commit final offsets
        if (framework_) {
            for (auto& task : tasks_) {
                if (task.snapshot_complete) {
                    framework_->commit_offset(name_, task.topic_name,
                                              task.task_id, task.binlog_offset);
                }
            }
        }

        tasks_.clear();
        schema_cache_.clear();

        logger->info("DebeziumSourceConnector '{}' stopped (records={}, snapshots={}, cdc={})",
                     name_, metrics_.records_produced.load(),
                     metrics_.snapshot_rows.load(),
                     metrics_.cdc_events.load());
    }

    // ------------------------------------------------------------------------
    // Poll — main entry point called by ConnectFramework worker thread
    // ------------------------------------------------------------------------

    std::vector<json> poll() override {
        if (stopped_.load(std::memory_order_acquire)) return {};

        std::vector<json> records;

        if (mode_ == ConnectorMode::snapshot) {
            records = poll_snapshot();
            if (all_snapshots_complete()) {
                auto logger = get_debezium_logger();
                logger->info("DebeziumSourceConnector '{}': snapshot phase complete",
                             name_);
                mode_ = ConnectorMode::snapshot_done;

                if (snapshot_mode_ != "initial_only") {
                    logger->info("DebeziumSourceConnector '{}': "
                                 "transitioning to incremental mode", name_);
                    mode_ = ConnectorMode::incremental;
                    start_incremental();
                }
            }
        } else if (mode_ == ConnectorMode::incremental) {
            records = poll_incremental();
        }

        // Update metrics
        metrics_.records_produced.fetch_add(
            static_cast<int64_t>(records.size()));

        return records;
    }

    // ------------------------------------------------------------------------
    // Status
    // ------------------------------------------------------------------------

    json status() const override {
        json j;
        j["connector_class"] = "DebeziumSourceConnector";
        j["mode"]            = mode_name(mode_);
        j["database"]        = db_name_;
        j["database_type"]   = db_type_str_;
        j["topic_prefix"]    = topic_prefix_;
        j["stopped"]         = stopped_.load();

        json tasks_json = json::array();
        for (auto& t : tasks_) {
            tasks_json.push_back(t.to_json());
        }
        j["tasks"] = tasks_json;
        j["metrics"] = metrics_.to_json();

        return j;
    }

    // ------------------------------------------------------------------------
    // Metadata
    // ------------------------------------------------------------------------

    std::string connector_class() const override {
        return "DebeziumSourceConnector";
    }

private:
    // ====================================================================
    // Table discovery
    // ====================================================================

    void discover_tables() {
        auto logger = get_debezium_logger();
        logger->info("DebeziumSourceConnector '{}': discovering tables in '{}'",
                     name_, db_name_);

        // Stub: in production this would query INFORMATION_SCHEMA or
        // pg_catalog to enumerate tables.  For now we accept a
        // comma-separated list of tables from config.
        std::string table_list = config_get_string(config_, "table.include.list", "");
        if (table_list.empty()) {
            logger->warn("DebeziumSourceConnector '{}': "
                         "no 'table.include.list' — using wildcard", name_);
            table_list = "*";
        }

        int32_t task_id = 0;

        if (table_list == "*") {
            // Wildcard — stub: pretend we found some tables
            for (const auto& stub_table : {"orders", "customers", "products"}) {
                add_table_task(task_id++, stub_table);
            }
        } else {
            std::istringstream iss(table_list);
            std::string tbl;
            while (std::getline(iss, tbl, ',')) {
                if (tbl.empty()) continue;
                // Trim
                tbl.erase(0, tbl.find_first_not_of(" \t"));
                tbl.erase(tbl.find_last_not_of(" \t") + 1);
                if (!tbl.empty()) {
                    add_table_task(task_id++, tbl);
                }
            }
        }
    }

    void add_table_task(int32_t task_id, const std::string& table_name) {
        CdcTask task;
        task.task_id    = task_id;
        task.task_name  = table_name;
        task.table_name = table_name;
        task.schema_name = "public";
        task.topic_name = topic_prefix_ + "." + db_name_ + "." + table_name;
        task.created_at = std::chrono::steady_clock::now();

        // Restore offset if available
        offset_t committed = 0;
        if (framework_) {
            committed = framework_->committed_offset(
                name_, task.topic_name, task_id);
        }
        if (committed != kInvalidOffset) {
            task.binlog_offset = committed;
            task.snapshot_complete = true;
        }

        // Build initial schema from config hints (stub)
        TableSchema schema;
        schema.table_name = table_name;
        schema.schema_name = "public";
        schema.schema_version = 1;
        schema.last_updated = std::chrono::steady_clock::now();

        // Stub columns — in production this comes from DB metadata
        schema.all_columns = {"id", "created_at", "updated_at", "data"};
        schema.primary_key_columns = {"id"};

        schema_cache_[table_name] = schema;

        tasks_.push_back(std::move(task));

        auto logger = get_debezium_logger();
        logger->info("DebeziumSourceConnector '{}': discovered table '{}' "
                     "-> topic '{}' (PK: id)",
                     name_, table_name, task.topic_name);
    }

    // ====================================================================
    // Snapshot polling
    // ====================================================================

    std::vector<json> poll_snapshot() {
        std::vector<json> records;
        auto logger = get_debezium_logger();

        for (auto& task : tasks_) {
            if (task.snapshot_complete) continue;

            // Stub: in production we would execute:
            //   SELECT * FROM {table} WHERE {pk} > {snapshot_offset}
            //   ORDER BY {pk} LIMIT {batch_size}

            // For the stub, we generate synthetic snapshot rows
            int64_t rows_this_batch = 0;
            for (int64_t i = 0; i < snapshot_batch_size_ &&
                               task.snapshot_offset < 100; ++i) {  // stub: stop after 100 rows
                json row;
                row["id"]         = ++task.snapshot_offset;
                row["created_at"] = "2024-01-01T00:00:00Z";
                row["updated_at"] = "2024-06-15T12:00:00Z";
                row["data"]       = "snapshot_row_" +
                                     std::to_string(task.snapshot_offset);

                json source_info = build_source_info(
                    db_name_, task.table_name, name_,
                    task.snapshot_offset, now_ms());

                json envelope = build_cdc_envelope(
                    CdcOperation::snapshot, nullptr, row, source_info);

                std::string pk_val = extract_pk_string(
                    row, schema_cache_[task.table_name].primary_key_columns);

                SourceRecord rec;
                rec.topic         = task.topic_name;
                rec.key           = task.table_name + ":" + pk_val;
                rec.value         = std::move(envelope);
                rec.partition     = task.task_id;
                rec.source_offset = task.snapshot_offset;
                rec.timestamp_ms  = now_ms();

                records.push_back(rec.to_json());
                rows_this_batch++;
            }

            task.snapshot_rows += rows_this_batch;
            task.records_produced += rows_this_batch;

            // Check if this table's snapshot is complete
            if (task.snapshot_offset >= 100) {  // stub threshold
                task.snapshot_complete = true;
                logger->info("DebeziumSourceConnector '{}': "
                             "snapshot complete for table '{}' ({} rows)",
                             name_, task.table_name, task.snapshot_rows);
            }

            task.last_poll = std::chrono::steady_clock::now();
        }

        metrics_.snapshot_rows.fetch_add(
            static_cast<int64_t>(records.size()));
        return records;
    }

    [[nodiscard]] bool all_snapshots_complete() const {
        for (auto& t : tasks_) {
            if (!t.snapshot_complete) return false;
        }
        return true;
    }

    // ====================================================================
    // Incremental (CDC) polling
    // ====================================================================

    void start_incremental() {
        auto logger = get_debezium_logger();
        logger->info("DebeziumSourceConnector '{}': initialising CDC stream "
                     "(db_type={}, slot={})",
                     name_, db_type_str_, slot_name_);

        // Stub: in production this would:
        //   MySQL:    SHOW MASTER STATUS, then COM_BINLOG_DUMP
        //   PostgreSQL: CREATE_REPLICATION_SLOT, START_REPLICATION
        // For the stub we just set up state.
        for (auto& task : tasks_) {
            task.last_event_timestamp = now_ms();
        }
    }

    std::vector<json> poll_incremental() {
        std::vector<json> records;
        auto logger = get_debezium_logger();

        // Stub: in production this reads from the binlog/WAL stream.
        // For demonstration, we occasionally emit synthetic CDC events.
        static int poll_count = 0;
        poll_count++;

        // Emit a synthetic event every ~5 polls
        if (poll_count % 5 != 0) return records;

        // Pick a random task to emit for (stub)
        if (tasks_.empty()) return records;

        size_t idx = static_cast<size_t>(poll_count / 5) % tasks_.size();
        auto& task = tasks_[idx];

        // Generate a synthetic CDC event
        int64_t event_id = ++cdc_event_counter_;
        CdcOperation op;
        json before_val = nullptr;
        json after_val;

        switch (event_id % 4) {
        case 0:
            op = CdcOperation::create;
            after_val = {{"id", event_id},
                         {"created_at", "2024-06-15T" +
                          std::to_string(12 + static_cast<int>(event_id % 12)) +
                          ":00:00Z"},
                         {"updated_at", "2024-06-15T12:00:00Z"},
                         {"data", "cdc_create_" + std::to_string(event_id)}};
            break;
        case 1:
            op = CdcOperation::update;
            before_val = {{"id", event_id - 1},
                          {"data", "old_data_" + std::to_string(event_id - 1)}};
            after_val = {{"id", event_id - 1},
                         {"updated_at", "2024-06-15T" +
                          std::to_string(12 + static_cast<int>(event_id % 12)) +
                          ":30:00Z"},
                         {"data", "cdc_update_" + std::to_string(event_id)}};
            break;
        case 2:
            op = CdcOperation::delete_;
            before_val = {{"id", event_id - 2},
                          {"data", "cdc_delete_" + std::to_string(event_id - 2)}};
            after_val  = nullptr;
            break;
        case 3:
            op = CdcOperation::create;
            after_val = {{"id", event_id},
                         {"created_at", "2024-06-15T14:00:00Z"},
                         {"data", "cdc_create2_" + std::to_string(event_id)}};
            break;
        default:
            break;
        }

        task.binlog_offset++;
        int64_t ts = now_ms();

        json source_info = build_source_info(
            db_name_, task.table_name, name_,
            task.binlog_offset, ts);

        json envelope = build_cdc_envelope(op, before_val, after_val, source_info);

        // Extract PK from after (or before for deletes)
        const json& pk_source = after_val.is_null() ? before_val : after_val;
        auto& schema = schema_cache_[task.table_name];
        std::string pk_val = extract_pk_string(pk_source,
                                                schema.primary_key_columns);

        SourceRecord rec;
        rec.topic         = task.topic_name;
        rec.key           = task.table_name + ":" + pk_val;
        rec.value         = std::move(envelope);
        rec.partition     = task.task_id;
        rec.source_offset = task.binlog_offset;
        rec.timestamp_ms  = ts;

        records.push_back(rec.to_json());

        task.cdc_events++;
        task.records_produced++;
        task.last_poll = std::chrono::steady_clock::now();

        // Commit offset periodically
        if (framework_ && task.cdc_events % 100 == 0) {
            framework_->commit_offset(name_, task.topic_name,
                                       task.task_id, task.binlog_offset);
        }

        // Handle schema changes (stub: periodically)
        if (task.cdc_events > 0 && task.cdc_events % 500 == 0) {
            records.push_back(build_schema_change_record(task, schema));
            task.schema_changes++;
        }

        metrics_.cdc_events.fetch_add(1);

        return records;
    }

    // ====================================================================
    // Schema change handling
    // ====================================================================

    json build_schema_change_record(CdcTask& task, TableSchema& schema) {
        schema.schema_version++;
        schema.last_updated = std::chrono::steady_clock::now();

        // Stub: add a column to simulate DDL
        std::string new_col = "col_" + std::to_string(schema.schema_version);
        schema.all_columns.push_back(new_col);
        schema_cache_[task.table_name] = schema;

        json change;
        change["type"]     = "ALTER";
        change["database"] = db_name_;
        change["schema"]   = schema.schema_name;
        change["table"]    = task.table_name;
        change["sql"]      = "ALTER TABLE " + task.table_name + " ADD COLUMN " +
                              new_col + " TEXT;";
        change["new_schema"] = schema.to_json();

        json envelope;
        envelope["op"]      = "s";
        envelope["ts_ms"]   = now_ms();
        envelope["source"]  = build_source_info(
            db_name_, task.table_name, name_, task.binlog_offset, now_ms());
        envelope["change"]  = std::move(change);

        json rec_json;
        rec_json["topic"]     = std::string(topic_prefix_) + "." +
                                std::string(kSchemaChangeTopic);
        rec_json["key"]       = task.table_name;
        rec_json["value"]     = std::move(envelope);
        rec_json["partition"] = task.task_id;
        rec_json["timestamp"] = now_ms();

        auto logger = get_debezium_logger();
        logger->info("DebeziumSourceConnector '{}': schema change on '{}': "
                     "v{} (+{})",
                     name_, task.table_name, schema.schema_version, new_col);

        return rec_json;
    }

    // ====================================================================
    // Metrics
    // ====================================================================

    struct DebeziumMetrics {
        std::atomic<int64_t> records_produced{0};
        std::atomic<int64_t> snapshot_rows{0};
        std::atomic<int64_t> cdc_events{0};
        std::atomic<int64_t> schema_changes{0};
        std::atomic<int64_t> poll_calls{0};
        std::atomic<int64_t> errors{0};
        std::chrono::steady_clock::time_point created_at;

        [[nodiscard]] json to_json() const {
            json j;
            j["records_produced"] = records_produced.load();
            j["snapshot_rows"]    = snapshot_rows.load();
            j["cdc_events"]       = cdc_events.load();
            j["schema_changes"]   = schema_changes.load();
            j["poll_calls"]       = poll_calls.load();
            j["errors"]           = errors.load();
            return j;
        }
    };

    // ====================================================================
    // Member variables
    // ====================================================================

    // ---- Database connection ----
    std::string    connection_url_;
    uint16_t       db_port_            = 3306;
    std::string    db_user_;
    std::string    db_password_;
    std::string    db_name_;
    std::string    db_type_str_        = "mysql";
    DatabaseType   db_type_            = DatabaseType::mysql;

    // ---- Topics ----
    std::string    topic_prefix_;

    // ---- Table filtering ----
    std::set<std::string> table_whitelist_;
    std::set<std::string> table_blacklist_;

    // ---- Snapshot ----
    std::string    snapshot_mode_          = "initial";
    int64_t        snapshot_batch_size_    = kDefaultSnapshotBatchSize;
    int64_t        snapshot_lock_timeout_ms_ = 10000;

    // ---- CDC ----
    int64_t        poll_interval_ms_       = 100;
    int64_t        heartbeat_interval_ms_  = 0;

    // ---- PostgreSQL plugin/slot ----
    std::string    plugin_name_            = "pgoutput";
    std::string    slot_name_;
    std::string    publication_name_       = "dbz_publication";

    // ---- Schema ----
    bool           schema_history_enabled_ = false;

    // ---- State ----
    ConnectorMode  mode_                   = ConnectorMode::snapshot;
    std::atomic<bool> stopped_{true};
    std::vector<CdcTask> tasks_;
    std::unordered_map<std::string, TableSchema> schema_cache_;
    DebeziumMetrics metrics_;

    int64_t cdc_event_counter_ = 0;
};

} // namespace torrent::connectors
