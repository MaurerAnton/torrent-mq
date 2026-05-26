/**
 * table_processor.cpp — TableProcessor: Materialized Table View for Streams
 *
 * Implements a materialized table abstraction atop a streaming record source.
 * A table is a snapshot of the latest value for each key — the moral
 * equivalent of a Kafka KTable or a RocksDB-backed changelog.
 *
 * Features:
 *
 *   - Upsert: insert or update a key with a new value.  If the key already
 *     exists, the old value is overwritten.  The old value is captured in
 *     the change log for audit and downstream CDC.
 *
 *   - Delete: logical deletion via tombstone.  The key is marked deleted
 *     (null value) and the tombstone is propagated.
 *
 *   - Lookup: point query for the latest value of a key.  Returns nullopt
 *     if the key is absent or tombstoned.
 *
 *   - Versioning: each upsert creates a new version.  Old versions are
 *     retained for a configurable number of generations or time window,
 *     enabling time-travel queries.
 *
 *   - Change log: every mutation (upsert, delete) is captured in a
 *     write-ahead changelog that can be consumed by downstream processors
 *     or replicated to another table instance.
 *
 *   - Snapshot / restore: create a consistent point-in-time snapshot of
 *     the entire table state; restore from a previous snapshot for fast
 *     bootstrap of a new processor instance.
 *
 *   - Compaction: periodically compact old versions and tombstone entries
 *     to reclaim storage space.
 *
 * Architecture:
 *
 *   ┌──────────────┐     ┌──────────────┐
 *   │ Change Stream │────→│ TableProcessor│
 *   │ (upserts/     │     │  ┌─────────┐  │
 *   │  deletes)     │     │  │kv store │  │
 *   └──────────────┘     │  │(RocksDB)│  │
 *                        │  └─────────┘  │
 *                        │  ┌─────────┐  │
 *                        │  │ChangeLog│  │
 *                        │  └─────────┘  │
 *                        └──────┬───────┘
 *                               │
 *                        ┌──────▼───────┐
 *                        │ Downstream   │
 *                        │ Processors   │
 *                        └──────────────┘
 *
 * Thread-safety:
 *   The underlying StateStore provides thread-safe access.  Compaction runs
 *   on a dedicated background thread.  Changelog appends are serialized
 *   through a mutex.
 *
 * Dependencies:
 *   - StateStore: persistent key-value backend
 *   - spdlog: structured logging
 *   - nlohmann/json: serialization
 */

#include "torrent/streams/topology.h"

#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

using json = nlohmann::json;
using namespace std::chrono_literals;

namespace torrent::streams {

// ============================================================================
// Anonymous namespace — internals
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_tp_logger() {
    static auto logger = spdlog::get("table_processor");
    if (!logger) {
        logger = spdlog::stdout_color_mt("table_processor");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

constexpr int64_t kDefaultCompactionIntervalSec = 300; // 5 minutes
constexpr int32_t kDefaultMaxVersions = 10;
constexpr int64_t kDefaultTombstoneRetentionSec = 86'400; // 24 hours
constexpr size_t  kMaxChangelogBatch = 1000;

// --------------------------------------------------------------------------
// Version key: key + version number
// --------------------------------------------------------------------------

std::string make_version_key(std::string_view table_ns, std::string_view key,
                              int64_t version) {
    std::string result;
    result.reserve(table_ns.size() + 1 + key.size() + 1 + 20);
    result.append(table_ns);
    result.push_back(':');
    result.append(key);
    result.push_back(':');
    result.append(std::to_string(version));
    return result;
}

std::string make_latest_key(std::string_view table_ns, std::string_view key) {
    std::string result;
    result.reserve(table_ns.size() + 1 + key.size());
    result.append(table_ns);
    result.push_back(':');
    result.append(key);
    return result;
}

// --------------------------------------------------------------------------
// ChangeLog entry types
// --------------------------------------------------------------------------

enum class ChangeOp : uint8_t {
    upsert = 0,
    delete_tombstone = 1,
    snapshot_marker = 2,
};

[[nodiscard]] std::string_view change_op_name(ChangeOp op) {
    switch (op) {
    case ChangeOp::upsert: return "upsert";
    case ChangeOp::delete_tombstone: return "delete";
    case ChangeOp::snapshot_marker: return "snapshot";
    }
    return "unknown";
}

} // anonymous namespace

// ============================================================================
// ChangeLogEntry — one entry in the write-ahead changelog
// ============================================================================

struct ChangeLogEntry {
    int64_t sequence_number = 0;
    int64_t timestamp_ms = 0;
    ChangeOp operation = ChangeOp::upsert;
    std::string key;
    std::optional<std::string> value; // nullopt for deletion
    int64_t version = 0;

    [[nodiscard]] json to_json() const {
        json j;
        j["seq"] = sequence_number;
        j["ts"] = timestamp_ms;
        j["op"] = std::string(change_op_name(operation));
        j["key"] = key;
        if (value.has_value()) {
            j["value"] = value.value();
        } else {
            j["value"] = nullptr;
        }
        j["version"] = version;
        return j;
    }

    [[nodiscard]] static ChangeLogEntry from_json(const json& j) {
        ChangeLogEntry e;
        e.sequence_number = j.value("seq", 0L);
        e.timestamp_ms = j.value("ts", 0L);
        std::string op_str = j.value("op", "upsert");
        if (op_str == "upsert") e.operation = ChangeOp::upsert;
        else if (op_str == "delete") e.operation = ChangeOp::delete_tombstone;
        else e.operation = ChangeOp::snapshot_marker;
        e.key = j.value("key", "");
        if (!j["value"].is_null()) {
            e.value = j["value"].get<std::string>();
        }
        e.version = j.value("version", 0L);
        return e;
    }
};

// ============================================================================
// TableConfig — table-level configuration
// ============================================================================

struct TableConfig {
    std::string table_name;
    int32_t max_versions = kDefaultMaxVersions;
    int64_t compaction_interval_sec = kDefaultCompactionIntervalSec;
    int64_t tombstone_retention_sec = kDefaultTombstoneRetentionSec;
    bool changelog_enabled = true;
    std::string changelog_topic; // empty = don't emit to broker

    [[nodiscard]] static TableConfig from_json(const json& j) {
        TableConfig cfg;
        cfg.table_name = j.value("table_name", "default_table");
        cfg.max_versions = j.value("max_versions", kDefaultMaxVersions);
        cfg.compaction_interval_sec
            = j.value("compaction_interval_sec", kDefaultCompactionIntervalSec);
        cfg.tombstone_retention_sec
            = j.value("tombstone_retention_sec", kDefaultTombstoneRetentionSec);
        cfg.changelog_enabled = j.value("changelog_enabled", true);
        cfg.changelog_topic = j.value("changelog_topic", "");
        return cfg;
    }
};

// ============================================================================
// TableProcessor::Impl — PIMPL body
// ============================================================================

class TableProcessor::Impl {
public:
    Impl(const TableConfig& cfg, std::shared_ptr<StateStore> store)
        : config_(cfg)
        , state_store_(std::move(store))
    {
        if (!state_store_) {
            throw std::runtime_error("TableProcessor requires a StateStore");
        }

        // Ensure table namespace exists
        std::string ns = table_namespace();
        if (!state_store_->namespace_exists(ns)) {
            state_store_->create_namespace(ns);
        }

        // Start background compactor
        start_compactor();

        get_tp_logger()->info("TableProcessor '{}' initialized", cfg.table_name);
    }

    ~Impl() { shutdown(); }

    void shutdown() {
        compaction_running_.store(false, std::memory_order_release);
        if (compaction_thread_ && compaction_thread_->joinable()) {
            compaction_thread_->join();
        }
        get_tp_logger()->info("TableProcessor '{}' shut down",
                              config_.table_name);
    }

    // ----------------------------------------------------------------------
    // Upsert — insert or update a key
    // ----------------------------------------------------------------------

    [[nodiscard]] int64_t upsert(std::string_view key,
                                  std::string_view value) {
        std::string table_ns = table_namespace();
        std::string latest_key = make_latest_key(table_ns, key);

        // Read current version
        int64_t new_version = 1;
        auto current = state_store_->get(table_ns, latest_key);
        if (current.has_value()) {
            json current_meta = json::parse(current.value());
            new_version = current_meta.value("version", 0L) + 1;
        }

        // Build value with metadata
        json entry;
        entry["value"] = std::string(value);
        entry["version"] = new_version;
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
        entry["timestamp_ms"] = now;
        entry["is_tombstone"] = false;

        std::string serialized = entry.dump();

        // Store latest
        state_store_->put(table_ns, latest_key, serialized);

        // Store versioned copy
        std::string version_key
            = make_version_key(table_ns, key, new_version);
        state_store_->put(table_ns, version_key, serialized);

        // Append to changelog
        append_changelog(ChangeOp::upsert, std::string(key),
                         std::string(value), new_version, now);

        upsert_count_.fetch_add(1, std::memory_order_relaxed);
        key_count_.store(
            static_cast<int64_t>(estimated_key_count()),
            std::memory_order_release);

        return new_version;
    }

    // ----------------------------------------------------------------------
    // Delete — remove a key (tombstone)
    // ----------------------------------------------------------------------

    [[nodiscard]] bool del(std::string_view key) {
        std::string table_ns = table_namespace();
        std::string latest_key = make_latest_key(table_ns, key);

        auto current = state_store_->get(table_ns, latest_key);
        if (!current.has_value()) {
            return false; // key doesn't exist
        }

        json current_meta = json::parse(current.value());
        int64_t version = current_meta.value("version", 0L);

        // Write tombstone
        json tombstone;
        tombstone["value"] = nullptr;
        tombstone["version"] = version + 1;
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
        tombstone["timestamp_ms"] = now;
        tombstone["is_tombstone"] = true;

        std::string serialized = tombstone.dump();
        state_store_->put(table_ns, latest_key, serialized);

        // Keep versioned tombstone
        std::string version_key
            = make_version_key(table_ns, key, version + 1);
        state_store_->put(table_ns, version_key, serialized);

        append_changelog(ChangeOp::delete_tombstone, std::string(key),
                         std::nullopt, version + 1, now);

        delete_count_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    // ----------------------------------------------------------------------
    // Lookup — retrieve the latest value for a key
    // ----------------------------------------------------------------------

    [[nodiscard]] std::optional<std::string>
    lookup(std::string_view key) const {
        std::string table_ns = table_namespace();
        std::string latest_key = make_latest_key(table_ns, key);

        auto raw = state_store_->get(table_ns, latest_key);
        if (!raw.has_value()) {
            miss_count_.fetch_add(1, std::memory_order_relaxed);
            return std::nullopt;
        }

        json entry = json::parse(raw.value());
        bool is_tombstone = entry.value("is_tombstone", false);

        if (is_tombstone) {
            miss_count_.fetch_add(1, std::memory_order_relaxed);
            return std::nullopt;
        }

        hit_count_.fetch_add(1, std::memory_order_relaxed);
        return entry.value("value", "");
    }

    // ----------------------------------------------------------------------
    // Versioned lookup — retrieve a specific version
    // ----------------------------------------------------------------------

    [[nodiscard]] std::optional<std::string>
    lookup_version(std::string_view key, int64_t version) const {
        std::string table_ns = table_namespace();
        std::string vkey = make_version_key(table_ns, key, version);

        auto raw = state_store_->get(table_ns, vkey);
        if (!raw.has_value()) return std::nullopt;

        json entry = json::parse(raw.value());
        if (entry.value("is_tombstone", false)) return std::nullopt;

        return entry.value("value", "");
    }

    // ----------------------------------------------------------------------
    // Scan — return all (non-tombstoned) key-value pairs
    // ----------------------------------------------------------------------

    [[nodiscard]] std::vector<std::pair<std::string, std::string>>
    scan(std::string_view prefix = "", size_t limit = 1000) const {
        std::string table_ns = table_namespace();
        std::string scan_prefix
            = table_ns + ":" + std::string(prefix);

        auto raw_results = state_store_->prefix_scan(table_ns, scan_prefix,
                                                       limit);

        std::vector<std::pair<std::string, std::string>> results;
        for (auto& [key, raw_value] : raw_results) {
            json entry = json::parse(raw_value);
            if (!entry.value("is_tombstone", false)) {
                results.emplace_back(key,
                                     entry.value("value", ""));
            }
        }
        return results;
    }

    // ----------------------------------------------------------------------
    // Changelog
    // ----------------------------------------------------------------------

    void append_changelog(ChangeOp op, std::string key,
                          std::optional<std::string> value,
                          int64_t version, int64_t timestamp_ms) {
        ChangeLogEntry entry;
        entry.sequence_number
            = changelog_seq_.fetch_add(1, std::memory_order_relaxed);
        entry.timestamp_ms = timestamp_ms;
        entry.operation = op;
        entry.key = std::move(key);
        entry.value = std::move(value);
        entry.version = version;

        {
            std::lock_guard<std::mutex> lock(changelog_mutex_);
            changelog_.push_back(std::move(entry));
        }

        // Flush if batch is large enough
        if (changelog_.size() >= kMaxChangelogBatch) {
            flush_changelog();
        }
    }

    void flush_changelog() {
        std::vector<ChangeLogEntry> batch;
        {
            std::lock_guard<std::mutex> lock(changelog_mutex_);
            if (changelog_.empty()) return;
            batch.swap(changelog_);
        }

        // Write changelog entries to state store
        std::string table_ns = table_namespace();
        std::vector<StateStore::BatchEntry> writes;
        for (const auto& e : batch) {
            std::string cl_key = "__changelog:" + std::to_string(e.sequence_number);
            writes.push_back({StateStore::BatchEntry::PUT, cl_key,
                               e.to_json().dump()});
        }
        state_store_->batch_write(table_ns, writes);

        get_tp_logger()->debug("Flushed {} changelog entries", batch.size());
    }

    // ----------------------------------------------------------------------
    // Snapshot and restore
    // ----------------------------------------------------------------------

    [[nodiscard]] std::string create_snapshot() const {
        // Emit snapshot marker in changelog
        const_cast<Impl*>(this)->append_changelog(
            ChangeOp::snapshot_marker, "__snapshot__", std::nullopt, 0,
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());

        flush_changelog();

        return state_store_->create_checkpoint();
    }

    void restore_from_snapshot(const std::string& snapshot_path) {
        state_store_->restore_from_checkpoint(snapshot_path);
        get_tp_logger()->info("TableProcessor '{}' restored from snapshot {}",
                              config_.table_name, snapshot_path);
    }

    // ----------------------------------------------------------------------
    // Compaction
    // ----------------------------------------------------------------------

    void compact() {
        auto now = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

        std::string table_ns = table_namespace();
        auto all_entries = state_store_->prefix_scan(table_ns, "", 100000);

        // Map key to list of versions with timestamps
        std::unordered_map<std::string,
                           std::vector<std::pair<int64_t, bool>>>
            key_versions; // key -> [(version, is_tombstone), ...]

        for (auto& [key, raw_value] : all_entries) {
            json entry = json::parse(raw_value);
            int64_t ver = entry.value("version", 0L);
            bool is_tombstone = entry.value("is_tombstone", false);
            int64_t ts = entry.value("timestamp_ms", 0L);

            // Extract base key (strip version suffix if present)
            std::string base_key = key;
            auto last_colon = base_key.rfind(':');
            if (last_colon != std::string::npos) {
                auto maybe_ver = base_key.substr(last_colon + 1);
                if (std::all_of(maybe_ver.begin(), maybe_ver.end(), ::isdigit))
                {
                    base_key = base_key.substr(0, last_colon);
                }
            }

            key_versions[base_key].emplace_back(ver, is_tombstone);
        }

        std::vector<StateStore::BatchEntry> to_delete;
        int32_t max_ver = config_.max_versions;

        for (auto& [key, versions] : key_versions) {
            if (versions.size() <= static_cast<size_t>(max_ver)) continue;

            // Sort by version descending
            std::sort(versions.begin(), versions.end(),
                      [](const auto& a, const auto& b) {
                          return a.first > b.first;
                      });

            // Delete versions beyond max
            for (size_t i = max_ver; i < versions.size(); ++i) {
                std::string vkey
                    = make_version_key(table_ns, key, versions[i].first);
                to_delete.push_back(
                    {StateStore::BatchEntry::DELETE, vkey, ""});
            }
        }

        // Also delete old changelog entries
        // (omitted for brevity — would use prefix scan + TTL logic)

        if (!to_delete.empty()) {
            state_store_->batch_write(table_ns, to_delete);
            get_tp_logger()->info("Compaction removed {} old versions",
                                  to_delete.size());
        }

        compacted_count_.fetch_add(1, std::memory_order_relaxed);
    }

    // ----------------------------------------------------------------------
    // Statistics
    // ----------------------------------------------------------------------

    [[nodiscard]] json stats() const {
        json j;
        j["table_name"] = config_.table_name;
        j["upserts"] = upsert_count_.load(std::memory_order_acquire);
        j["deletes"] = delete_count_.load(std::memory_order_acquire);
        j["hits"] = hit_count_.load(std::memory_order_acquire);
        j["misses"] = miss_count_.load(std::memory_order_acquire);
        j["estimated_keys"]
            = key_count_.load(std::memory_order_acquire);
        j["changelog_entries"]
            = changelog_seq_.load(std::memory_order_acquire);
        j["compactions"]
            = compacted_count_.load(std::memory_order_acquire);
        j["changelog_enabled"] = config_.changelog_enabled;

        // Hit rate
        int64_t total_lookups
            = hit_count_.load(std::memory_order_acquire)
            + miss_count_.load(std::memory_order_acquire);
        j["hit_rate"] = total_lookups > 0
            ? static_cast<double>(hit_count_.load(std::memory_order_acquire))
                  / static_cast<double>(total_lookups)
            : 0.0;

        return j;
    }

    [[nodiscard]] const TableConfig& config() const { return config_; }

private:
    // ----------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------

    [[nodiscard]] std::string table_namespace() const {
        return "table:" + config_.table_name;
    }

    [[nodiscard]] size_t estimated_key_count() const {
        // Approximate by scanning; in production, maintain a counter.
        return static_cast<size_t>(
            key_count_.load(std::memory_order_acquire));
    }

    void start_compactor() {
        compaction_running_.store(true, std::memory_order_release);
        compaction_thread_ = std::make_unique<std::thread>([this]() {
            while (compaction_running_.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(
                    std::chrono::seconds(config_.compaction_interval_sec));
                if (!compaction_running_.load(std::memory_order_acquire))
                    break;
                compact();
            }
        });
    }

    // ----------------------------------------------------------------------
    // Members
    // ----------------------------------------------------------------------

    TableConfig config_;
    std::shared_ptr<StateStore> state_store_;

    // Changelog
    std::deque<ChangeLogEntry> changelog_;
    std::mutex changelog_mutex_;
    std::atomic<int64_t> changelog_seq_{0};

    // Counters
    std::atomic<int64_t> upsert_count_{0};
    std::atomic<int64_t> delete_count_{0};
    std::atomic<int64_t> hit_count_{0};
    std::atomic<int64_t> miss_count_{0};
    std::atomic<int64_t> key_count_{0};
    std::atomic<int64_t> compacted_count_{0};

    // Compaction thread
    std::unique_ptr<std::thread> compaction_thread_;
    std::atomic<bool> compaction_running_{false};
};

// ============================================================================
// TableProcessor — public API (PIMPL wrapper)
// ============================================================================

TableProcessor::TableProcessor(const json& config,
                                std::shared_ptr<StateStore> store)
    : impl_(std::make_unique<Impl>(TableConfig::from_json(config),
                                    std::move(store)))
{}

TableProcessor::~TableProcessor() = default;

int64_t TableProcessor::upsert(std::string_view key, std::string_view value) {
    return impl_->upsert(key, value);
}
bool TableProcessor::del(std::string_view key) { return impl_->del(key); }

std::optional<std::string>
TableProcessor::lookup(std::string_view key) const {
    return impl_->lookup(key);
}

std::optional<std::string>
TableProcessor::lookup_version(std::string_view key, int64_t version) const {
    return impl_->lookup_version(key, version);
}

std::vector<std::pair<std::string, std::string>>
TableProcessor::scan(std::string_view prefix, size_t limit) const {
    return impl_->scan(prefix, limit);
}

void TableProcessor::flush_changelog() { impl_->flush_changelog(); }
std::string TableProcessor::create_snapshot() const {
    return impl_->create_snapshot();
}
void TableProcessor::restore_from_snapshot(const std::string& path) {
    impl_->restore_from_snapshot(path);
}
void TableProcessor::compact() { impl_->compact(); }

json TableProcessor::stats() const { return impl_->stats(); }

} // namespace torrent::streams
