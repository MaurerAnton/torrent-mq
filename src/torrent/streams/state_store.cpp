/**
 * state_store.cpp — StateStore: Persistent Key-Value State for Stream Processors
 *
 * RocksDB-backed persistent state store for stream processing operators.
 * Stateful processors (aggregations, joins, windowing) rely on the state
 * store to persist their working state across restarts and to recover
 * from failures without data loss.
 *
 * Features:
 *
 *   - put/get/delete: atomic single-key operations
 *   - Namespace isolation: each processor gets its own logical namespace
 *     backed by a dedicated RocksDB column family
 *   - Windowed state: values with TTL (time-to-live) for automatic
 *     expiration of windowed state
 *   - Batch operations: WriteBatch for atomic multi-key puts/deletes,
 *     MultiGet for concurrent read of multiple keys
 *   - Checkpoint and restore: create point-in-time snapshots of the
 *     entire state, restore from a previous checkpoint with minimal
 *     downtime
 *   - Iterator: range scans with prefix filtering for efficient
 *     cleanup and window eviction
 *   - Metrics: key count, approximate size, hit/miss rates
 *
 * Architecture:
 *
 *   ┌───────────────┐     ┌───────────────┐
 *   │  Processor A   │────→│  namespace_a  │ (column family)
 *   │  (aggregate)   │     │  CF in RocksDB│
 *   └───────────────┘     └───────────────┘
 *   ┌───────────────┐     ┌───────────────┐
 *   │  Processor B   │────→│  namespace_b  │ (column family)
 *   │  (join)        │     │  CF in RocksDB│
 *   └───────────────┘     └───────────────┘
 *
 * Checkpoint lifecycle:
 *   1. create_checkpoint(): flush all column families, hard-link SST files
 *      into a new directory, write a manifest with CF metadata
 *   2. Restore: on recovery, open the checkpoint directory as a RocksDB
 *      instance and iterate state into the live store — or simply swap
 *      the data directory
 *
 * Thread-safety:
 *   RocksDB handles are thread-safe.  Column family creation/deletion is
 *   serialized through a global mutex.  Per-namespace operations use
 *   shared locks; namespace creation uses exclusive lock.
 *
 * Dependencies:
 *   - librocksdb: embedded key-value store
 *   - spdlog: structured logging
 *   - torrent::endpoint / torrent::result: common types
 */

#include "torrent/streams/topology.h"

#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/checkpoint.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/cache.h>
#include <rocksdb/table.h>
#include <rocksdb/merge_operator.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

using json = nlohmann::json;
namespace fs = std::filesystem;

namespace torrent::streams {

// ============================================================================
// Anonymous namespace — internals
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_ss_logger() {
    static auto logger = spdlog::get("state_store");
    if (!logger) {
        logger = spdlog::stdout_color_mt("state_store");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

constexpr std::string_view kDefaultColumnFamily = "default";
constexpr size_t kDefaultCacheSizeMB = 128;
constexpr int32_t kDefaultNumLevels = 6;
constexpr size_t kDefaultWriteBufferMB = 64;
constexpr int32_t kDefaultMaxOpenFiles = 1024;

// --------------------------------------------------------------------------
// Helper: construct a namespaced key
// --------------------------------------------------------------------------

std::string make_key(std::string_view ns, std::string_view key) {
    std::string result;
    result.reserve(ns.size() + 1 + key.size());
    result.append(ns);
    result.push_back(':');
    result.append(key);
    return result;
}

// --------------------------------------------------------------------------
// Helper: strip namespace prefix from a key
// --------------------------------------------------------------------------

std::pair<std::string_view, std::string_view>
split_key(std::string_view full_key) {
    auto pos = full_key.find(':');
    if (pos == std::string_view::npos) {
        return {full_key, std::string_view{}};
    }
    return {full_key.substr(0, pos), full_key.substr(pos + 1)};
}

// --------------------------------------------------------------------------
// TTL entry: value with expiration timestamp
// --------------------------------------------------------------------------

struct TtlEntry {
    std::string value;
    int64_t expires_at_ms = 0; // 0 means no expiration

    [[nodiscard]] bool is_expired(int64_t now_ms) const {
        return expires_at_ms > 0 && now_ms > expires_at_ms;
    }

    [[nodiscard]] std::string serialize() const {
        // Wire format: [8 bytes: expires_at_ms][4 bytes: value_len][value bytes]
        std::string out;
        out.reserve(12 + value.size());
        out.append(reinterpret_cast<const char*>(&expires_at_ms),
                   sizeof(expires_at_ms));
        int32_t vlen = static_cast<int32_t>(value.size());
        out.append(reinterpret_cast<const char*>(&vlen), sizeof(vlen));
        out.append(value);
        return out;
    }

    static TtlEntry deserialize(std::string_view raw) {
        TtlEntry entry;
        if (raw.size() < 12) return entry; // malformed
        std::memcpy(&entry.expires_at_ms, raw.data(), sizeof(int64_t));
        int32_t vlen = 0;
        std::memcpy(&vlen, raw.data() + sizeof(int64_t), sizeof(int32_t));
        if (vlen > 0 && static_cast<size_t>(vlen + 12) <= raw.size()) {
            entry.value.assign(raw.data() + 12, vlen);
        }
        return entry;
    }
};

} // anonymous namespace

// ============================================================================
// NamespaceDescriptor — metadata about one state namespace
// ============================================================================

struct NamespaceDescriptor {
    std::string name;
    rocksdb::ColumnFamilyHandle* cf_handle = nullptr;
    std::atomic<int64_t> key_count{0};
    std::atomic<int64_t> hit_count{0};
    std::atomic<int64_t> miss_count{0};

    NamespaceDescriptor(std::string n, rocksdb::ColumnFamilyHandle* h)
        : name(std::move(n)), cf_handle(h) {}

    void record_hit() {
        hit_count.fetch_add(1, std::memory_order_relaxed);
        key_count.fetch_add(1, std::memory_order_relaxed);
    }

    void record_miss() {
        miss_count.fetch_add(1, std::memory_order_relaxed);
    }

    [[nodiscard]] double hit_rate() const {
        int64_t total = hit_count.load(std::memory_order_acquire)
                      + miss_count.load(std::memory_order_acquire);
        if (total == 0) return 0.0;
        return static_cast<double>(hit_count.load(std::memory_order_acquire))
             / static_cast<double>(total);
    }

    [[nodiscard]] json stats() const {
        json j;
        j["namespace"] = name;
        j["key_count"] = key_count.load(std::memory_order_acquire);
        j["hit_count"] = hit_count.load(std::memory_order_acquire);
        j["miss_count"] = miss_count.load(std::memory_order_acquire);
        j["hit_rate"] = hit_rate();
        return j;
    }
};

// ============================================================================
// StateStore::Impl — PIMPL body
// ============================================================================

class StateStore::Impl {
public:
    // ----------------------------------------------------------------------
    // Construction / destruction
    // ----------------------------------------------------------------------

    explicit Impl(const std::string& db_path)
        : db_path_(db_path)
        , db_(nullptr)
    {
        rocksdb::Options opts = default_options();
        opts.create_if_missing = true;
        opts.create_missing_column_families = true;

        // Collect existing column families
        std::vector<std::string> cf_names;
        rocksdb::DB::ListColumnFamilies(opts, db_path_, &cf_names);

        std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
        std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;

        if (cf_names.empty()) {
            // Fresh database
            cf_descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName, opts);
        } else {
            for (const auto& name : cf_names) {
                cf_descriptors.emplace_back(name, opts);
            }
        }

        auto status = rocksdb::DB::Open(opts, db_path, cf_descriptors,
                                         &cf_handles, &db_);
        if (!status.ok()) {
            throw std::runtime_error("StateStore: failed to open RocksDB at "
                                     + db_path_ + ": "
                                     + status.ToString());
        }

        // Populate namespace descriptors from existing CFs
        for (size_t i = 0; i < cf_handles.size(); ++i) {
            std::string name = (i == 0)
                ? std::string(kDefaultColumnFamily)
                : cf_names[i];
            auto desc = std::make_shared<NamespaceDescriptor>(name,
                                                               cf_handles[i]);
            namespaces_[name] = desc;
            // Default CF is reserved; skip for external use
        }

        get_ss_logger()->info("StateStore opened at {} with {} column families",
                              db_path_, cf_handles.size());
    }

    ~Impl() {
        shutdown();
    }

    void shutdown() {
        std::unique_lock<std::shared_mutex> lock(ns_mutex_);
        for (auto& [name, desc] : namespaces_) {
            if (desc->cf_handle && db_) {
                db_->DestroyColumnFamilyHandle(desc->cf_handle);
                desc->cf_handle = nullptr;
            }
        }
        namespaces_.clear();
        if (db_) {
            delete db_;
            db_ = nullptr;
        }
        get_ss_logger()->info("StateStore shut down");
    }

    // ----------------------------------------------------------------------
    // Namespace management
    // ----------------------------------------------------------------------

    [[nodiscard]] bool namespace_exists(std::string_view ns) const {
        std::shared_lock<std::shared_mutex> lock(ns_mutex_);
        return namespaces_.find(std::string(ns)) != namespaces_.end();
    }

    void create_namespace(std::string_view ns) {
        std::unique_lock<std::shared_mutex> lock(ns_mutex_);

        std::string ns_str(ns);
        if (namespaces_.count(ns_str)) {
            return; // already exists
        }

        rocksdb::ColumnFamilyHandle* cf = nullptr;
        rocksdb::ColumnFamilyOptions cf_opts = default_cf_options();

        auto status = db_->CreateColumnFamily(cf_opts, ns_str, &cf);
        if (!status.ok()) {
            throw std::runtime_error("StateStore: failed to create namespace '"
                                     + ns_str + "': " + status.ToString());
        }

        auto desc = std::make_shared<NamespaceDescriptor>(ns_str, cf);
        namespaces_[ns_str] = desc;

        get_ss_logger()->info("Created namespace '{}'", ns_str);
    }

    void drop_namespace(std::string_view ns) {
        std::unique_lock<std::shared_mutex> lock(ns_mutex_);

        std::string ns_str(ns);
        auto it = namespaces_.find(ns_str);
        if (it == namespaces_.end()) return;

        auto status = db_->DropColumnFamily(it->second->cf_handle);
        if (!status.ok()) {
            get_ss_logger()->error("Failed to drop namespace '{}': {}",
                                    ns_str, status.ToString());
            throw std::runtime_error("StateStore: drop failed: "
                                     + status.ToString());
        }

        namespaces_.erase(it);
        get_ss_logger()->info("Dropped namespace '{}'", ns_str);
    }

    // ----------------------------------------------------------------------
    // Key-value operations
    // ----------------------------------------------------------------------

    [[nodiscard]] std::optional<std::string>
    get(std::string_view ns, std::string_view key) const {
        std::shared_lock<std::shared_mutex> lock(ns_mutex_);

        auto desc = find_namespace(ns);
        if (!desc) {
            get_ss_logger()->warn("StateStore::get: namespace '{}' not found",
                                   ns);
            return std::nullopt;
        }

        std::string full_key = make_key(ns, key);
        std::string value;
        auto status = db_->Get(rocksdb::ReadOptions(), desc->cf_handle,
                               full_key, &value);
        if (status.IsNotFound()) {
            desc->record_miss();
            return std::nullopt;
        }
        if (!status.ok()) {
            get_ss_logger()->error("StateStore::get failed for '{}/{}': {}",
                                    ns, key, status.ToString());
            desc->record_miss();
            return std::nullopt;
        }

        desc->record_hit();
        return value;
    }

    bool put(std::string_view ns, std::string_view key,
             std::string_view value) {
        std::shared_lock<std::shared_mutex> lock(ns_mutex_);

        auto desc = find_namespace(ns);
        if (!desc) return false;

        std::string full_key = make_key(ns, key);
        auto status = db_->Put(rocksdb::WriteOptions(), desc->cf_handle,
                               full_key, value);
        if (!status.ok()) {
            get_ss_logger()->error("StateStore::put failed for '{}/{}': {}",
                                    ns, key, status.ToString());
            return false;
        }

        desc->key_count.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    bool put_with_ttl(std::string_view ns, std::string_view key,
                      std::string_view value,
                      std::chrono::milliseconds ttl) {
        std::shared_lock<std::shared_mutex> lock(ns_mutex_);

        auto desc = find_namespace(ns);
        if (!desc) return false;

        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
        TtlEntry entry;
        entry.value.assign(value);
        entry.expires_at_ms = now + ttl.count();

        std::string serialized = entry.serialize();
        std::string full_key = make_key(ns, key);
        auto status = db_->Put(rocksdb::WriteOptions(), desc->cf_handle,
                               full_key, serialized);
        if (!status.ok()) {
            get_ss_logger()->error(
                "StateStore::put_with_ttl failed for '{}/{}': {}",
                ns, key, status.ToString());
            return false;
        }

        desc->key_count.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    [[nodiscard]] std::optional<std::string>
    get_with_ttl(std::string_view ns, std::string_view key) const {
        std::shared_lock<std::shared_mutex> lock(ns_mutex_);

        auto desc = find_namespace(ns);
        if (!desc) return std::nullopt;

        std::string full_key = make_key(ns, key);
        std::string raw;
        auto status = db_->Get(rocksdb::ReadOptions(), desc->cf_handle,
                               full_key, &raw);
        if (status.IsNotFound()) {
            desc->record_miss();
            return std::nullopt;
        }
        if (!status.ok()) {
            desc->record_miss();
            return std::nullopt;
        }

        TtlEntry entry = TtlEntry::deserialize(raw);
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

        if (entry.is_expired(now)) {
            // Lazy deletion: best-effort, ignore failure
            db_->Delete(rocksdb::WriteOptions(), desc->cf_handle, full_key);
            desc->record_miss();
            return std::nullopt;
        }

        desc->record_hit();
        return std::move(entry.value);
    }

    bool del(std::string_view ns, std::string_view key) {
        std::shared_lock<std::shared_mutex> lock(ns_mutex_);

        auto desc = find_namespace(ns);
        if (!desc) return false;

        std::string full_key = make_key(ns, key);
        auto status = db_->Delete(rocksdb::WriteOptions(), desc->cf_handle,
                                  full_key);
        if (!status.ok()) {
            get_ss_logger()->error("StateStore::del failed for '{}/{}': {}",
                                    ns, key, status.ToString());
            return false;
        }
        return true;
    }

    // ----------------------------------------------------------------------
    // Batch operations
    // ----------------------------------------------------------------------

    struct BatchEntry {
        enum Op { PUT, DELETE };
        Op op;
        std::string key;
        std::string value;
    };

    bool batch_write(std::string_view ns,
                     const std::vector<BatchEntry>& entries) {
        std::shared_lock<std::shared_mutex> lock(ns_mutex_);

        auto desc = find_namespace(ns);
        if (!desc) return false;

        rocksdb::WriteBatch batch;
        for (const auto& e : entries) {
            std::string full_key = make_key(ns, e.key);
            switch (e.op) {
            case BatchEntry::PUT:
                batch.Put(desc->cf_handle, full_key, e.value);
                break;
            case BatchEntry::DELETE:
                batch.Delete(desc->cf_handle, full_key);
                break;
            }
        }

        auto status = db_->Write(rocksdb::WriteOptions(), &batch);
        if (!status.ok()) {
            get_ss_logger()->error(
                "StateStore::batch_write failed for '{}': {}", ns,
                status.ToString());
            return false;
        }
        return true;
    }

    [[nodiscard]] std::vector<std::optional<std::string>>
    batch_get(std::string_view ns, const std::vector<std::string>& keys) const {
        std::shared_lock<std::shared_mutex> lock(ns_mutex_);

        auto desc = find_namespace(ns);
        if (!desc) return {};

        std::vector<rocksdb::Slice> slices;
        std::vector<std::string> full_keys;
        full_keys.reserve(keys.size());
        slices.reserve(keys.size());

        for (const auto& k : keys) {
            full_keys.push_back(make_key(ns, k));
            slices.emplace_back(full_keys.back());
        }

        std::vector<rocksdb::PinnableSlice> results(keys.size());
        std::vector<rocksdb::Status> statuses(keys.size());

        db_->MultiGet(rocksdb::ReadOptions(), desc->cf_handle,
                      static_cast<int>(slices.size()), slices.data(),
                      results.data(), statuses.data());

        std::vector<std::optional<std::string>> output;
        output.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            if (statuses[i].ok()) {
                output.push_back(results[i].ToString());
                desc->record_hit();
            } else {
                output.push_back(std::nullopt);
                desc->record_miss();
            }
        }
        return output;
    }

    // ----------------------------------------------------------------------
    // Range scan / iteration
    // ----------------------------------------------------------------------

    [[nodiscard]] std::vector<std::pair<std::string, std::string>>
    prefix_scan(std::string_view ns, std::string_view prefix,
                size_t limit = 1000) const
    {
        std::shared_lock<std::shared_mutex> lock(ns_mutex_);

        auto desc = find_namespace(ns);
        if (!desc) return {};

        std::string full_prefix = make_key(ns, prefix);
        auto it = std::unique_ptr<rocksdb::Iterator>(
            db_->NewIterator(rocksdb::ReadOptions(), desc->cf_handle));

        std::vector<std::pair<std::string, std::string>> results;
        for (it->Seek(full_prefix);
             it->Valid() && results.size() < limit;
             it->Next())
        {
            // Check that key still falls within the namespace prefix
            std::string_view full_key = it->key().ToStringView();
            if (full_key.size() < full_prefix.size()) break;
            if (full_key.substr(0, full_prefix.size()) != full_prefix) break;

            // Strip namespace prefix
            auto [ns_part, key_part] = split_key(full_key);
            results.emplace_back(std::string(key_part),
                                 it->value().ToString());
        }
        return results;
    }

    // ----------------------------------------------------------------------
    // TTL cleanup — evict expired entries
    // ----------------------------------------------------------------------

    size_t evict_expired(std::string_view ns) {
        std::shared_lock<std::shared_mutex> lock(ns_mutex_);

        auto desc = find_namespace(ns);
        if (!desc) return 0;

        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

        std::string ns_prefix = std::string(ns) + ":";
        auto it = std::unique_ptr<rocksdb::Iterator>(
            db_->NewIterator(rocksdb::ReadOptions(), desc->cf_handle));

        rocksdb::WriteBatch batch;
        size_t evicted = 0;

        for (it->Seek(ns_prefix);
             it->Valid();
             it->Next())
        {
            std::string_view full_key = it->key().ToStringView();
            if (full_key.size() < ns_prefix.size()) break;
            if (full_key.substr(0, ns_prefix.size()) != ns_prefix) break;

            TtlEntry entry = TtlEntry::deserialize(it->value().ToStringView());
            if (entry.is_expired(now)) {
                batch.Delete(desc->cf_handle, it->key());
                ++evicted;
            }
        }

        if (evicted > 0) {
            db_->Write(rocksdb::WriteOptions(), &batch);
            desc->key_count.fetch_sub(static_cast<int64_t>(evicted),
                                       std::memory_order_relaxed);
            get_ss_logger()->info("Evicted {} expired entries from '{}'",
                                   evicted, ns);
        }
        return evicted;
    }

    // ----------------------------------------------------------------------
    // Checkpoint / restore
    // ----------------------------------------------------------------------

    [[nodiscard]] std::string create_checkpoint() const {
        std::shared_lock<std::shared_mutex> lock(ns_mutex_);

        std::string ckpt_dir = db_path_ + "/checkpoints/"
                             + std::to_string(
                                 std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::system_clock::now()
                                         .time_since_epoch())
                                     .count());

        fs::create_directories(ckpt_dir);

        rocksdb::Checkpoint* ckpt = nullptr;
        auto status = rocksdb::Checkpoint::Create(db_, &ckpt);
        if (!status.ok()) {
            throw std::runtime_error("StateStore: failed to create checkpoint "
                                     "object: " + status.ToString());
        }

        std::unique_ptr<rocksdb::Checkpoint> ckpt_guard(ckpt);
        status = ckpt->CreateCheckpoint(ckpt_dir);
        if (!status.ok()) {
            throw std::runtime_error("StateStore: checkpoint failed: "
                                     + status.ToString());
        }

        get_ss_logger()->info("Created checkpoint at {}", ckpt_dir);

        // Write a small metadata file
        json meta;
        meta["namespaces"] = json::array();
        for (const auto& [name, desc] : namespaces_) {
            meta["namespaces"].push_back(name);
        }
        meta["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now()
                                     .time_since_epoch())
                                 .count();

        std::ofstream meta_file(ckpt_dir + "/checkpoint_meta.json");
        meta_file << meta.dump(2);

        return ckpt_dir;
    }

    void restore_from_checkpoint(const std::string& ckpt_dir) {
        std::unique_lock<std::shared_mutex> lock(ns_mutex_);

        if (!fs::exists(ckpt_dir)) {
            throw std::runtime_error("StateStore: checkpoint directory not "
                                     "found: " + ckpt_dir);
        }

        // Read checkpoint metadata
        std::string meta_path = ckpt_dir + "/checkpoint_meta.json";
        json meta;
        if (fs::exists(meta_path)) {
            std::ifstream in(meta_path);
            in >> meta;
        }

        // Close all current column families
        for (auto& [name, desc] : namespaces_) {
            if (desc->cf_handle) {
                db_->DestroyColumnFamilyHandle(desc->cf_handle);
                desc->cf_handle = nullptr;
            }
        }
        namespaces_.clear();

        delete db_;
        db_ = nullptr;

        // Copy checkpoint data to main DB directory
        // In a production system this would use atomic directory swap.
        // Here we copy SST files.
        fs::remove_all(db_path_);
        fs::create_directories(db_path_);
        for (const auto& entry : fs::directory_iterator(ckpt_dir)) {
            if (entry.path().filename() == "checkpoint_meta.json") continue;
            fs::copy(entry.path(), db_path_ / entry.path().filename(),
                     fs::copy_options::recursive);
        }

        // Re-open
        rocksdb::Options opts = default_options();
        std::vector<std::string> cf_names;
        rocksdb::DB::ListColumnFamilies(opts, db_path_, &cf_names);

        std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
        std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;

        for (const auto& name : cf_names) {
            cf_descriptors.emplace_back(name, opts);
        }

        auto status = rocksdb::DB::Open(opts, db_path_, cf_descriptors,
                                         &cf_handles, &db_);
        if (!status.ok()) {
            throw std::runtime_error("StateStore: restore open failed: "
                                     + status.ToString());
        }

        // Rebuild namespace descriptors
        for (size_t i = 0; i < cf_handles.size(); ++i) {
            std::string name = (i == 0)
                ? std::string(kDefaultColumnFamily)
                : cf_names[i];
            auto desc = std::make_shared<NamespaceDescriptor>(name,
                                                               cf_handles[i]);
            namespaces_[name] = desc;
        }

        get_ss_logger()->info("Restored from checkpoint at {}", ckpt_dir);
    }

    // ----------------------------------------------------------------------
    // Statistics
    // ----------------------------------------------------------------------

    [[nodiscard]] json stats() const {
        std::shared_lock<std::shared_mutex> lock(ns_mutex_);

        json j;
        j["db_path"] = db_path_;

        // Per-namespace stats
        json ns_stats = json::array();
        for (const auto& [name, desc] : namespaces_) {
            ns_stats.push_back(desc->stats());
        }
        j["namespaces"] = ns_stats;

        // RocksDB-level stats
        std::string rocksdb_stats;
        if (db_) {
            db_->GetProperty("rocksdb.stats", &rocksdb_stats);
        }
        j["rocksdb_summary"] = rocksdb_stats;

        // Approximate total size on disk
        uint64_t total_size = 0;
        for (const auto& entry : fs::recursive_directory_iterator(db_path_)) {
            if (entry.is_regular_file()) {
                total_size += entry.file_size();
            }
        }
        j["disk_bytes"] = total_size;

        return j;
    }

    [[nodiscard]] size_t namespace_count() const {
        std::shared_lock<std::shared_mutex> lock(ns_mutex_);
        return namespaces_.size();
    }

    [[nodiscard]] std::vector<std::string> list_namespaces() const {
        std::shared_lock<std::shared_mutex> lock(ns_mutex_);
        std::vector<std::string> result;
        result.reserve(namespaces_.size());
        for (const auto& [name, desc] : namespaces_) {
            result.push_back(name);
        }
        return result;
    }

private:
    // ----------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------

    [[nodiscard]] std::shared_ptr<NamespaceDescriptor>
    find_namespace(std::string_view ns) const {
        std::string ns_str(ns);
        auto it = namespaces_.find(ns_str);
        if (it == namespaces_.end()) return nullptr;
        return it->second;
    }

    [[nodiscard]] rocksdb::Options default_options() const {
        rocksdb::Options opts;
        opts.create_if_missing = true;
        opts.create_missing_column_families = true;
        opts.max_open_files = kDefaultMaxOpenFiles;
        opts.write_buffer_size = kDefaultWriteBufferMB * 1024 * 1024;

        // Block-based table
        rocksdb::BlockBasedTableOptions table_opts;
        table_opts.block_cache
            = rocksdb::NewLRUCache(kDefaultCacheSizeMB * 1024 * 1024);
        table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        table_opts.block_size = 16 * 1024;
        opts.table_factory.reset(
            rocksdb::NewBlockBasedTableFactory(table_opts));

        // Level-based compaction
        opts.num_levels = kDefaultNumLevels;
        opts.compression = rocksdb::kSnappyCompression;
        opts.bottommost_compression = rocksdb::kZSTD;

        // DB options
        opts.IncreaseParallelism(std::max<int>(1,
            static_cast<int>(std::thread::hardware_concurrency()) / 2));
        opts.max_background_jobs = 4;

        return opts;
    }

    [[nodiscard]] rocksdb::ColumnFamilyOptions default_cf_options() const {
        rocksdb::ColumnFamilyOptions opts;
        opts.write_buffer_size = kDefaultWriteBufferMB * 1024 * 1024;
        return opts;
    }

    // ----------------------------------------------------------------------
    // Data members
    // ----------------------------------------------------------------------

    std::string db_path_;
    rocksdb::DB* db_;
    std::unordered_map<std::string, std::shared_ptr<NamespaceDescriptor>>
        namespaces_;
    mutable std::shared_mutex ns_mutex_;
};

// ============================================================================
// StateStore — public API (PIMPL wrapper)
// ============================================================================

StateStore::StateStore(const std::string& db_path)
    : impl_(std::make_unique<Impl>(db_path))
{}

StateStore::~StateStore() = default;

void StateStore::shutdown() { impl_->shutdown(); }

bool StateStore::namespace_exists(std::string_view ns) const {
    return impl_->namespace_exists(ns);
}
void StateStore::create_namespace(std::string_view ns) {
    impl_->create_namespace(ns);
}
void StateStore::drop_namespace(std::string_view ns) {
    impl_->drop_namespace(ns);
}

std::optional<std::string> StateStore::get(std::string_view ns,
                                           std::string_view key) const {
    return impl_->get(ns, key);
}
bool StateStore::put(std::string_view ns, std::string_view key,
                     std::string_view value) {
    return impl_->put(ns, key, value);
}
bool StateStore::put_with_ttl(std::string_view ns, std::string_view key,
                              std::string_view value,
                              std::chrono::milliseconds ttl) {
    return impl_->put_with_ttl(ns, key, value, ttl);
}
std::optional<std::string> StateStore::get_with_ttl(std::string_view ns,
                                                     std::string_view key) const {
    return impl_->get_with_ttl(ns, key);
}
bool StateStore::del(std::string_view ns, std::string_view key) {
    return impl_->del(ns, key);
}

bool StateStore::batch_write(std::string_view ns,
                              const std::vector<BatchEntry>& entries) {
    return impl_->batch_write(ns, entries);
}
std::vector<std::optional<std::string>>
StateStore::batch_get(std::string_view ns,
                       const std::vector<std::string>& keys) const {
    return impl_->batch_get(ns, keys);
}

std::vector<std::pair<std::string, std::string>>
StateStore::prefix_scan(std::string_view ns, std::string_view prefix,
                         size_t limit) const {
    return impl_->prefix_scan(ns, prefix, limit);
}

size_t StateStore::evict_expired(std::string_view ns) {
    return impl_->evict_expired(ns);
}

std::string StateStore::create_checkpoint() const {
    return impl_->create_checkpoint();
}
void StateStore::restore_from_checkpoint(const std::string& ckpt_dir) {
    impl_->restore_from_checkpoint(ckpt_dir);
}

json StateStore::stats() const { return impl_->stats(); }
size_t StateStore::namespace_count() const { return impl_->namespace_count(); }
std::vector<std::string> StateStore::list_namespaces() const {
    return impl_->list_namespaces();
}

} // namespace torrent::streams
