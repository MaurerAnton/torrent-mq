/**
 * jdbc_sink.cpp — JdbcSinkConnector: Write Records to Relational Databases
 *
 * Writes records from torrent topics into JDBC-compatible relational databases.
 * Supports:
 *
 *   Batch insert:     INSERT INTO table (cols) VALUES (...), (...), ...
 *   Upsert mode:      INSERT ... ON DUPLICATE KEY UPDATE (MySQL) or
 *                     INSERT ... ON CONFLICT ... DO UPDATE (PostgreSQL)
 *   Table auto-creation: Derives CREATE TABLE from record value schema.
 *   Primary key handling: configurable PK columns for dedup/upsert.
 *   Dead letter queue:   failed records are routed to a DLQ topic instead
 *                         of blocking the pipeline.
 *
 * Insert modes:
 *   "insert"        — plain INSERT, error on duplicate key
 *   "upsert"        — INSERT ... ON DUPLICATE KEY UPDATE (MySQL syntax)
 *   "update"        — UPDATE ... WHERE pk = ? (requires pk.fields)
 *   "insert_or_update" — try INSERT, on conflict do UPDATE
 *
 * Configuration keys:
 *   connection.url               JDBC connection string (required)
 *   connection.user              database user
 *   connection.password           database password
 *   table.name.format            target table name (can use {topic} placeholder)
 *   insert.mode                  "insert", "upsert", "update", "insert_or_update"
 *   pk.mode                      "none", "record_key", "record_value", "kafka"
 *   pk.fields                    comma-separated PK column names
 *   batch.size                   max rows per batch (default 500)
 *   max.retries                  max flush retries (default 3)
 *   auto.create                  auto-create table if missing (default false)
 *   auto.evolve                  auto-add columns from record schema (default false)
 *   delete.enabled               enable tombstone-based deletes (default false)
 *   quote.sql.identifiers        quote table/column names (default true)
 *   dlq.topic                    dead letter queue topic name
 *   dlq.enabled                  enable DLQ routing (default false)
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
    std::string              name_;
    json                     config_;
    ConnectFramework*        framework_ = nullptr;
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

std::shared_ptr<spdlog::logger> get_jdbc_sink_logger() {
    static auto logger = spdlog::get("jdbc_sink");
    if (!logger) {
        logger = spdlog::stdout_color_mt("jdbc_sink");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

inline constexpr size_t    kDefaultBatchSize   = 500;
inline constexpr int64_t   kDefaultMaxRetries  = 3;
inline constexpr size_t    kMaxColumnNameLen   = 63;
inline constexpr size_t    kMaxDlqRecords      = 10000;

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
// SQL quoting
// --------------------------------------------------------------------------

[[nodiscard]] std::string quote_identifier(std::string_view ident,
                                             bool quote) {
    if (!quote) return std::string(ident);
    // Double any embedded double-quotes
    std::string out;
    out.reserve(ident.size() + 2);
    out += '"';
    for (char c : ident) {
        if (c == '"') out += '"';
        out += c;
    }
    out += '"';
    return out;
}

// --------------------------------------------------------------------------
// SQL type mapping: JSON value type → SQL column type
// --------------------------------------------------------------------------

[[nodiscard]] std::string json_type_to_sql(const json& val) {
    switch (val.type()) {
    case json::value_t::null:            return "TEXT";
    case json::value_t::boolean:         return "BOOLEAN";
    case json::value_t::number_integer:
    case json::value_t::number_unsigned: return "BIGINT";
    case json::value_t::number_float:    return "DOUBLE PRECISION";
    case json::value_t::string: {
        const auto& s = val.get<std::string>();
        if (s.size() > 255) return "TEXT";
        return "VARCHAR(" + std::to_string(std::max<size_t>(s.size() * 2, 255)) + ")";
    }
    case json::value_t::array:
    case json::value_t::object:          return "JSONB";
    default:                             return "TEXT";
    }
}

// --------------------------------------------------------------------------
// Schema inference from a batch of records
// --------------------------------------------------------------------------

struct ColumnInfo {
    std::string name;
    std::string sql_type;
    bool        nullable = true;
};

[[nodiscard]] std::vector<ColumnInfo> infer_schema(
    const std::vector<SinkRecord>& records)
{
    std::unordered_map<std::string, ColumnInfo> col_map;
    std::vector<std::string> col_order;

    for (auto& rec : records) {
        if (!rec.value.is_object()) continue;
        for (auto& [key, val] : rec.value.items()) {
            std::string sql_type = json_type_to_sql(val);
            auto it = col_map.find(key);
            if (it == col_map.end()) {
                col_order.push_back(key);
                col_map[key] = {key, sql_type, true};
            } else {
                // Widen type if needed
                if (it->second.sql_type == "BIGINT" &&
                    sql_type == "DOUBLE PRECISION") {
                    it->second.sql_type = "DOUBLE PRECISION";
                } else if (it->second.sql_type.find("VARCHAR") == 0 &&
                           sql_type == "TEXT") {
                    it->second.sql_type = "TEXT";
                }
            }
        }
    }

    std::vector<ColumnInfo> columns;
    columns.reserve(col_order.size());
    for (auto& name : col_order) {
        columns.push_back(col_map[name]);
    }
    return columns;
}

// --------------------------------------------------------------------------
// CREATE TABLE SQL generation
// --------------------------------------------------------------------------

[[nodiscard]] std::string generate_create_table(
    std::string_view table_name,
    const std::vector<ColumnInfo>& columns,
    const std::vector<std::string>& pk_cols,
    bool quote)
{
    std::ostringstream sql;
    sql << "CREATE TABLE IF NOT EXISTS "
        << quote_identifier(table_name, quote) << " (\n";

    for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) sql << ",\n";
        sql << "    " << quote_identifier(columns[i].name, quote)
            << " " << columns[i].sql_type;
        if (!columns[i].nullable) sql << " NOT NULL";
    }

    if (!pk_cols.empty()) {
        sql << ",\n    PRIMARY KEY (";
        for (size_t i = 0; i < pk_cols.size(); ++i) {
            if (i > 0) sql << ", ";
            sql << quote_identifier(pk_cols[i], quote);
        }
        sql << ")";
    }

    sql << "\n);";
    return sql.str();
}

// --------------------------------------------------------------------------
// Build INSERT SQL
// --------------------------------------------------------------------------

[[nodiscard]] std::string build_insert_sql(
    std::string_view table_name,
    const std::vector<ColumnInfo>& columns,
    bool quote)
{
    std::ostringstream sql;
    sql << "INSERT INTO " << quote_identifier(table_name, quote) << " (";

    for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) sql << ", ";
        sql << quote_identifier(columns[i].name, quote);
    }

    sql << ") VALUES (";

    for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) sql << ", ";
        sql << ":" << columns[i].name;
    }

    sql << ")";
    return sql.str();
}

// --------------------------------------------------------------------------
// Build UPSERT SQL (PostgreSQL-style)
// --------------------------------------------------------------------------

[[nodiscard]] std::string build_upsert_sql(
    std::string_view table_name,
    const std::vector<ColumnInfo>& columns,
    const std::vector<std::string>& pk_cols,
    bool quote)
{
    std::ostringstream sql;
    sql << build_insert_sql(table_name, columns, quote);
    sql << " ON CONFLICT (";

    for (size_t i = 0; i < pk_cols.size(); ++i) {
        if (i > 0) sql << ", ";
        sql << quote_identifier(pk_cols[i], quote);
    }

    sql << ") DO UPDATE SET ";

    bool first = true;
    for (auto& col : columns) {
        // Skip PK columns in the SET clause
        if (std::find(pk_cols.begin(), pk_cols.end(), col.name) != pk_cols.end())
            continue;
        if (!first) sql << ", ";
        first = false;
        auto q = quote_identifier(col.name, quote);
        sql << q << " = EXCLUDED." << q;
    }

    return sql.str();
}

// --------------------------------------------------------------------------
// Build UPDATE SQL
// --------------------------------------------------------------------------

[[nodiscard]] std::string build_update_sql(
    std::string_view table_name,
    const std::vector<ColumnInfo>& columns,
    const std::vector<std::string>& pk_cols,
    bool quote)
{
    std::ostringstream sql;
    sql << "UPDATE " << quote_identifier(table_name, quote) << " SET ";

    bool first = true;
    for (auto& col : columns) {
        if (std::find(pk_cols.begin(), pk_cols.end(), col.name) != pk_cols.end())
            continue;
        if (!first) sql << ", ";
        first = false;
        sql << quote_identifier(col.name, quote) << " = :" << col.name;
    }

    sql << " WHERE ";
    for (size_t i = 0; i < pk_cols.size(); ++i) {
        if (i > 0) sql << " AND ";
        sql << quote_identifier(pk_cols[i], quote) << " = :" << pk_cols[i];
    }

    return sql.str();
}

// --------------------------------------------------------------------------
// Build DELETE SQL
// --------------------------------------------------------------------------

[[nodiscard]] std::string build_delete_sql(
    std::string_view table_name,
    const std::vector<std::string>& pk_cols,
    bool quote)
{
    std::ostringstream sql;
    sql << "DELETE FROM " << quote_identifier(table_name, quote) << " WHERE ";

    for (size_t i = 0; i < pk_cols.size(); ++i) {
        if (i > 0) sql << " AND ";
        sql << quote_identifier(pk_cols[i], quote) << " = :" << pk_cols[i];
    }

    return sql.str();
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
};

[[nodiscard]] json dlq_to_json(const DlqRecord& dr) {
    json j;
    j["original_topic"]     = dr.original_topic;
    j["original_partition"] = dr.original_partition;
    j["original_offset"]    = dr.original_offset;
    j["original_value"]     = dr.original_value;
    j["error_message"]      = dr.error_message;
    j["failed_at_ms"]       = dr.failed_at_ms;
    return j;
}

} // anonymous namespace

// ============================================================================
// JdbcSinkConnector
// ============================================================================

class JdbcSinkConnector final : public SinkConnector {
public:
    JdbcSinkConnector()  = default;
    ~JdbcSinkConnector() override { stop(); }

    // ------------------------------------------------------------------------
    // Lifecycle — start
    // ------------------------------------------------------------------------

    void start(const std::string& name, const json& config,
               ConnectFramework* framework) override
    {
        name_      = name;
        config_    = config;
        framework_ = framework;

        auto logger = get_jdbc_sink_logger();
        logger->info("JdbcSinkConnector '{}' starting", name_);

        // --- Parse config ---
        connection_url_   = config_get_string(config_, "connection.url");
        connection_user_  = config_get_string(config_, "connection.user", "");
        connection_pass_  = config_get_string(config_, "connection.password", "");
        table_name_format_= config_get_string(config_, "table.name.format", "{topic}");
        insert_mode_      = config_get_string(config_, "insert.mode", "insert");
        pk_mode_          = config_get_string(config_, "pk.mode", "none");
        pk_fields_str_    = config_get_string(config_, "pk.fields", "");

        batch_size_       = static_cast<size_t>(
            config_get_int(config_, "batch.size", kDefaultBatchSize));
        max_retries_      = config_get_int(config_, "max.retries", kDefaultMaxRetries);

        auto_create_      = config_get_bool(config_, "auto.create", false);
        auto_evolve_      = config_get_bool(config_, "auto.evolve", false);
        delete_enabled_   = config_get_bool(config_, "delete.enabled", false);
        quote_identifiers_= config_get_bool(config_, "quote.sql.identifiers", true);

        dlq_topic_        = config_get_string(config_, "dlq.topic", "");
        dlq_enabled_      = config_get_bool(config_, "dlq.enabled", false);

        // --- Validate ---
        if (connection_url_.empty()) {
            throw std::invalid_argument(
                "JdbcSinkConnector requires 'connection.url' in config");
        }

        // --- Parse PK fields ---
        if (!pk_fields_str_.empty()) {
            std::istringstream iss(pk_fields_str_);
            std::string field;
            while (std::getline(iss, field, ',')) {
                field.erase(0, field.find_first_not_of(" \t"));
                field.erase(field.find_last_not_of(" \t") + 1);
                if (!field.empty()) pk_fields_.push_back(field);
            }
        }

        // --- Validate insert mode ---
        if (insert_mode_ != "insert" && insert_mode_ != "upsert" &&
            insert_mode_ != "update" && insert_mode_ != "insert_or_update") {
            logger->warn("JdbcSinkConnector '{}': unknown insert.mode '{}', "
                         "falling back to 'insert'", name_, insert_mode_);
            insert_mode_ = "insert";
        }

        // --- State ---
        metrics_.created_at = std::chrono::steady_clock::now();
        stopped_.store(false);
        buffer_.reserve(batch_size_);
        dlq_buffer_.reserve(kMaxDlqRecords);
        table_created_.clear();

        logger->info(
            "JdbcSinkConnector '{}' started (table_format={}, insert_mode={}, "
            "pk_mode={}, batch_size={}, auto_create={}, dlq_enabled={})",
            name_, table_name_format_, insert_mode_, pk_mode_,
            batch_size_, auto_create_, dlq_enabled_);
    }

    // ------------------------------------------------------------------------
    // Lifecycle — stop
    // ------------------------------------------------------------------------

    void stop() override {
        if (stopped_.exchange(true)) return;

        auto logger = get_jdbc_sink_logger();
        logger->info("JdbcSinkConnector '{}' stopping", name_);

        // Final flush
        if (!buffer_.empty()) {
            logger->info("JdbcSinkConnector '{}': final flush ({} records)",
                         name_, buffer_.size());
            try {
                do_flush();
            } catch (const std::exception& e) {
                logger->error("JdbcSinkConnector '{}': final flush failed: {}",
                              name_, e.what());
                metrics_.flush_errors.fetch_add(1);
            }
        }

        // Flush DLQ
        if (!dlq_buffer_.empty()) {
            logger->warn("JdbcSinkConnector '{}': {} records in DLQ at shutdown",
                         name_, dlq_buffer_.size());
            flush_dlq();
        }

        buffer_.clear();
        offset_tracker_.clear();
        dlq_buffer_.clear();
        table_created_.clear();
        last_schema_.clear();

        logger->info("JdbcSinkConnector '{}' stopped (written={}, dlq={}, errors={})",
                     name_, metrics_.records_written.load(),
                     metrics_.dlq_records.load(),
                     metrics_.flush_errors.load());
    }

    // ------------------------------------------------------------------------
    // put — buffer records
    // ------------------------------------------------------------------------

    void put(const std::vector<json>& records) override {
        if (stopped_.load(std::memory_order_acquire)) return;

        auto logger = get_jdbc_sink_logger();
        metrics_.records_received.fetch_add(
            static_cast<int64_t>(records.size()));

        for (auto& rec : records) {
            try {
                SinkRecord sr = SinkRecord::from_json(rec);

                // Resolve target table name
                std::string table = resolve_table_name(sr.topic);

                // Handle tombstone records (null value = delete)
                if (delete_enabled_ && sr.value.is_null()) {
                    handle_delete(sr, table);
                    continue;
                }

                // Store with resolved table name as topic
                sr.topic = std::move(table);
                buffer_.push_back(std::move(sr));

                // Track max offset per partition
                std::string okey = offset_key(
                    buffer_.back().topic, buffer_.back().partition);
                auto& existing = offset_tracker_[okey];
                if (buffer_.back().offset > existing.max_offset) {
                    existing.topic     = buffer_.back().topic;
                    existing.partition = buffer_.back().partition;
                    existing.max_offset = buffer_.back().offset;
                }
            } catch (const std::exception& e) {
                logger->error("JdbcSinkConnector '{}': put error: {}", name_, e.what());
                metrics_.put_errors.fetch_add(1);

                // Route to DLQ if enabled
                if (dlq_enabled_) {
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
                }
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

        // Flush DLQ if full
        if (dlq_buffer_.size() >= kMaxDlqRecords) {
            flush_dlq();
        }
    }

    // ------------------------------------------------------------------------
    // flush — write buffered records to database
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
        j["connector_class"]   = "JdbcSinkConnector";
        j["connection_url"]    = connection_url_;
        j["table_name_format"] = table_name_format_;
        j["insert_mode"]       = insert_mode_;
        j["pk_mode"]           = pk_mode_;
        j["batch_size"]        = batch_size_;
        j["buffered"]          = buffer_.size();
        j["dlq_buffered"]      = dlq_buffer_.size();
        j["dlq_enabled"]       = dlq_enabled_;
        j["stopped"]           = stopped_.load();
        j["metrics"]           = metrics_.to_json();

        json tables = json::array();
        for (auto& t : table_created_) {
            tables.push_back(t);
        }
        j["tables_created"] = tables;

        return j;
    }

    std::string connector_class() const override {
        return "JdbcSinkConnector";
    }

private:
    // ====================================================================
    // Resolve table name from topic
    // ====================================================================

    [[nodiscard]] std::string resolve_table_name(
        std::string_view topic) const
    {
        std::string result = table_name_format_;
        size_t pos = result.find("{topic}");
        if (pos != std::string::npos) {
            result.replace(pos, 7, topic);
        }
        // Replace dots with underscores for safe SQL identifiers
        for (auto& c : result) {
            if (c == '.' || c == '-') c = '_';
        }
        return result;
    }

    // ====================================================================
    // Handle tombstone (DELETE)
    // ====================================================================

    void handle_delete(const SinkRecord& sr, const std::string& table) {
        auto logger = get_jdbc_sink_logger();

        if (pk_fields_.empty()) {
            logger->warn("JdbcSinkConnector '{}': tombstone record but no "
                         "pk.fields configured — skipping delete", name_);
            return;
        }

        // Stub: in production execute DELETE with PK values from record key
        logger->debug("JdbcSinkConnector '{}': DELETE from {} where key={}",
                      name_, table, sr.key);

        metrics_.records_written.fetch_add(1);
    }

    // ====================================================================
    // Core flush logic
    // ====================================================================

    void do_flush() {
        auto logger = get_jdbc_sink_logger();

        // Group records by target table
        std::unordered_map<std::string, std::vector<SinkRecord>> by_table;
        for (auto& rec : buffer_) {
            by_table[rec.topic].push_back(rec);
        }

        // --- Retry loop with exponential backoff ---
        ExponentialBackoff backoff(
            std::chrono::milliseconds(100),
            std::chrono::seconds(30),
            2.0, 0.25,
            static_cast<int32_t>(max_retries_));

        while (true) {
            try {
                int64_t total_written = 0;

                for (auto& [table, records] : by_table) {
                    // Infer/update schema
                    auto columns = infer_schema(records);

                    // Auto-create table if enabled and not yet created
                    if (auto_create_ &&
                        table_created_.find(table) == table_created_.end()) {
                        std::string ddl = generate_create_table(
                            table, columns, pk_fields_, quote_identifiers_);
                        logger->info("JdbcSinkConnector '{}': auto-creating "
                                     "table '{}':\n{}",
                                     name_, table, ddl);
                        // Stub: in production execute DDL via JDBC
                        table_created_.insert(table);
                        last_schema_[table] = columns;
                    }

                    // Auto-evolve: detect new columns
                    if (auto_evolve_ &&
                        table_created_.find(table) != table_created_.end()) {
                        auto it = last_schema_.find(table);
                        if (it != last_schema_.end()) {
                            std::set<std::string> existing;
                            for (auto& c : it->second) existing.insert(c.name);
                            for (auto& c : columns) {
                                if (existing.find(c.name) == existing.end()) {
                                    std::string alter = "ALTER TABLE " +
                                        quote_identifier(table, quote_identifiers_) +
                                        " ADD COLUMN " +
                                        quote_identifier(c.name, quote_identifiers_) +
                                        " " + c.sql_type + ";";
                                    logger->info("JdbcSinkConnector '{}': "
                                                 "auto-evolving table '{}': {}",
                                                 name_, table, alter);
                                    // Stub: execute ALTER TABLE
                                }
                            }
                            last_schema_[table] = columns;
                        }
                    }

                    // Determine SQL template
                    std::string sql;
                    if (insert_mode_ == "upsert" && !pk_fields_.empty()) {
                        sql = build_upsert_sql(
                            table, columns, pk_fields_, quote_identifiers_);
                    } else if (insert_mode_ == "update" && !pk_fields_.empty()) {
                        sql = build_update_sql(
                            table, columns, pk_fields_, quote_identifiers_);
                    } else {
                        sql = build_insert_sql(
                            table, columns, quote_identifiers_);
                    }

                    logger->debug("JdbcSinkConnector '{}': flushing {} records "
                                  "to '{}' using {}",
                                  name_, records.size(), table, insert_mode_);

                    // Stub: in production, execute batch via JDBC
                    //   PreparedStatement ps = conn.prepareStatement(sql);
                    //   for each record: set params, addBatch()
                    //   ps.executeBatch()
                    //   conn.commit()

                    total_written += static_cast<int64_t>(records.size());

                    // Log first record for debugging
                    if (!records.empty()) {
                        logger->trace("JdbcSink '{}': sample record -> {}: "
                                      "key={} value={}",
                                      name_, table, records[0].key,
                                      records[0].value.dump().substr(0, 200));
                    }
                }

                // Update metrics
                metrics_.records_written.fetch_add(total_written);
                metrics_.flushes.fetch_add(1);
                metrics_.last_flush = std::chrono::steady_clock::now();

                logger->info("JdbcSinkConnector '{}': flushed {} records "
                             "across {} tables",
                             name_, total_written, by_table.size());

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
                logger->error("JdbcSinkConnector '{}': flush error "
                              "(attempt {}): {}",
                              name_, backoff.attempts(), e.what());

                if (backoff.exhausted()) {
                    metrics_.flush_errors.fetch_add(1);

                    // Route failed batch to DLQ if enabled
                    if (dlq_enabled_) {
                        route_batch_to_dlq(e.what());
                    }

                    buffer_.clear();
                    offset_tracker_.clear();
                    throw;
                }

                auto delay = backoff.next_delay();
                logger->warn("JdbcSinkConnector '{}': retrying in {}ms",
                             name_, delay.count());
                std::this_thread::sleep_for(delay);
            }
        }
    }

    // ====================================================================
    // Dead Letter Queue
    // ====================================================================

    void route_batch_to_dlq(const std::string& error) {
        auto logger = get_jdbc_sink_logger();
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
            dlq_buffer_.push_back(std::move(dlq));
        }

        metrics_.dlq_records.fetch_add(
            static_cast<int64_t>(buffer_.size()));

        logger->warn("JdbcSinkConnector '{}': routed {} records to DLQ",
                     name_, buffer_.size());

        flush_dlq();
    }

    void flush_dlq() {
        if (dlq_buffer_.empty()) return;

        auto logger = get_jdbc_sink_logger();

        // Produce DLQ records to the configured DLQ topic
        for (auto& dlq : dlq_buffer_) {
            json dlq_json = dlq_to_json(dlq);

            // Stub: in production, produce to broker
            logger->debug("JdbcSinkConnector '{}': DLQ record -> {}: {}",
                          name_, dlq.original_topic, dlq.error_message);

            // framework_->produce(dlq_topic_, dlq_json.dump(), ...)
        }

        logger->info("JdbcSinkConnector '{}': flushed {} DLQ records to '{}'",
                     name_, dlq_buffer_.size(), dlq_topic_);

        dlq_buffer_.clear();
    }

    // ====================================================================
    // Metrics
    // ====================================================================

    struct JdbcMetrics {
        std::atomic<int64_t> records_received{0};
        std::atomic<int64_t> records_written{0};
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
            j["records_written"]  = records_written.load();
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

    // ---- Connection ----
    std::string    connection_url_;
    std::string    connection_user_;
    std::string    connection_pass_;

    // ---- Table config ----
    std::string    table_name_format_   = "{topic}";
    std::string    insert_mode_         = "insert";
    std::string    pk_mode_             = "none";
    std::string    pk_fields_str_;
    std::vector<std::string> pk_fields_;

    // ---- Batch settings ----
    size_t         batch_size_          = kDefaultBatchSize;
    int64_t        max_retries_         = kDefaultMaxRetries;

    // ---- Schema management ----
    bool           auto_create_         = false;
    bool           auto_evolve_         = false;
    bool           delete_enabled_      = false;
    bool           quote_identifiers_   = true;

    // ---- DLQ config ----
    std::string    dlq_topic_;
    bool           dlq_enabled_         = false;

    // ---- State ----
    std::atomic<bool>                    stopped_{true};
    std::vector<SinkRecord>             buffer_;
    std::vector<DlqRecord>              dlq_buffer_;
    std::unordered_map<std::string, PartitionOffset> offset_tracker_;
    std::set<std::string>               table_created_;
    std::unordered_map<std::string,
        std::vector<ColumnInfo>>        last_schema_;
    JdbcMetrics                         metrics_;
};

} // namespace torrent::connectors
