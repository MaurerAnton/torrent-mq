/**
 * torrent-mq — RocksDB Metadata Store
 *
 * RocksDB-backed key-value store used for persistent metadata:
 *   - Consumer group offsets
 *   - Transaction state (producer IDs, ongoing transactions)
 *   - Topic configuration snapshots
 *   - Cluster metadata cache
 *
 * Provides a simple put/get/delete interface over RocksDB with:
 *   - WriteBatch for atomic multi-key updates
 *   - Iterator for range scans (prefix scans, offset listing)
 *   - Column family support for logical separation of data types
 *   - Automatic repair on open if corruption is detected
 *   - Statistics export for monitoring
 *
 * Thread safety: RocksDB handle is thread-safe for reads/writes.
 * Column family handles are serialized via a mutex during open/close.
 *
 * Dependencies: librocksdb (RocksDB C++ API)
 */

#include "torrent/storage/types.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/cache.h>
#include <rocksdb/rate_limiter.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// ============================================================================
// Anonymous namespace — logging and helpers
// ============================================================================

namespace torrent {
namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_rocksdb_logger() {
    static auto logger = spdlog::get("rocksdb_store");
    if (!logger) {
        logger = spdlog::stdout_color_mt("rocksdb_store");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Column family names
// --------------------------------------------------------------------------

static constexpr std::string_view kCfOffsets        = "consumer_offsets";
static constexpr std::string_view kCfTransactions    = "transactions";
static constexpr std::string_view kCfProducerIds     = "producer_ids";
static constexpr std::string_view kCfTopicConfigs    = "topic_configs";
static constexpr std::string_view kCfClusterMeta     = "cluster_metadata";
static constexpr std::string_view kCfDefault         = "default";

// --------------------------------------------------------------------------
// Default RocksDB configuration
// --------------------------------------------------------------------------

static constexpr size_t kDefaultBlockCacheSize = 128 * 1024 * 1024;  // 128 MiB
static constexpr int    kDefaultWriteBufferSize = 64 * 1024 * 1024;  // 64 MiB
static constexpr int    kDefaultMaxWriteBufferNumber = 3;
static constexpr int    kDefaultMaxBackgroundJobs = 4;
static constexpr int    kDefaultBloomBitsPerKey = 10;

// --------------------------------------------------------------------------
// Serialize offset_t for consistent byte ordering
// --------------------------------------------------------------------------

void append_uint64(std::string& out, uint64_t val) {
    // Big-endian to preserve lexicographic ordering
    for (int i = 7; i >= 0; --i) {
        out.push_back(static_cast<char>((val >> (i * 8)) & 0xFF));
    }
}

uint64_t read_uint64(std::string_view data) {
    uint64_t val = 0;
    for (size_t i = 0; i < 8 && i < data.size(); ++i) {
        val = (val << 8) | static_cast<unsigned char>(data[i]);
    }
    return val;
}

// --------------------------------------------------------------------------
// Build keys for specific data types
// --------------------------------------------------------------------------

std::string make_offset_key(const std::string& group_id,
                             topic_id_t topic_id,
                             partition_id_t partition_id) {
    // Key format: group_id (var) + '\0' + topic_id (8 bytes BE) + partition_id (4 bytes BE)
    std::string key;
    key.reserve(group_id.size() + 1 + 12);
    key.append(group_id);
    key.push_back('\0');
    // topic_id as big-endian uint64
    append_uint64(key, static_cast<uint64_t>(topic_id));
    // partition_id as big-endian uint32 (store as 4 bytes)
    uint32_t pid = static_cast<uint32_t>(partition_id);
    for (int i = 3; i >= 0; --i) {
        key.push_back(static_cast<char>((pid >> (i * 8)) & 0xFF));
    }
    return key;
}

std::string make_transaction_key(const std::string& transactional_id) {
    return std::string("txn:") + transactional_id;
}

std::string make_pid_key(producer_id_t producer_id) {
    std::string key("pid:");
    append_uint64(key, static_cast<uint64_t>(producer_id));
    return key;
}

std::string make_topic_config_key(topic_id_t topic_id) {
    std::string key("cfg:");
    append_uint64(key, static_cast<uint64_t>(topic_id));
    return key;
}

// --------------------------------------------------------------------------
// Helper: open a column family, creating if needed
// --------------------------------------------------------------------------

rocksdb::ColumnFamilyHandle* open_column_family(
    rocksdb::DB* db,
    const std::string& name,
    const rocksdb::ColumnFamilyOptions& options) {

    rocksdb::ColumnFamilyHandle* handle = nullptr;
    auto status = db->CreateColumnFamily(options, name, &handle);
    if (status.ok()) {
        get_rocksdb_logger()->info("Created column family: {}", name);
        return handle;
    }

    // Try opening existing
    status = db->CreateColumnFamily(options, name, &handle);
    if (!status.ok()) {
        get_rocksdb_logger()->error("Cannot create/open column family '{}': {}",
                                    name, status.ToString());
    }
    return handle;
}

} // anonymous namespace

// ============================================================================
// RocksDBStore — Public Interface
// ============================================================================

/**
 * RocksDBStore provides a persistent key-value store backed by RocksDB.
 *
 * Column families are used to separate different types of metadata:
 *   - "consumer_offsets": group_id + topic + partition → offset
 *   - "transactions": transactional_id → transaction state
 *   - "producer_ids": producer_id → producer epoch
 *   - "topic_configs": topic_id → TopicConfig
 *   - "cluster_metadata": arbitrary cluster-level key-value pairs
 *   - "default": fallback for any other keys
 *
 * Typical usage:
 *   RocksDBStore store;
 *   store.open("/var/lib/torrent/metadata");
 *   store.put("default", "my_key", "my_value");
 *   auto val = store.get("default", "my_key");
 *   store.close();
 */
class RocksDBStore {
public:
    /// Configuration for the RocksDB store.
    struct Config {
        std::string data_directory;         ///< Path to RocksDB data directory.
        size_t      block_cache_size  = kDefaultBlockCacheSize;
        int         write_buffer_size = kDefaultWriteBufferSize;
        int         max_write_buffer_number = kDefaultMaxWriteBufferNumber;
        int         max_background_jobs     = kDefaultMaxBackgroundJobs;
        int         bloom_bits_per_key      = kDefaultBloomBitsPerKey;
        bool        create_if_missing       = true;
        bool        error_if_exists         = false;
        bool        paranoid_checks         = false;   ///< Enable extra CRC checks at cost of perf
        int         stats_dump_period_sec   = 600;     ///< Periodic stats dump interval
    };

    RocksDBStore() = default;

    ~RocksDBStore() {
        close();
    }

    RocksDBStore(const RocksDBStore&) = delete;
    RocksDBStore& operator=(const RocksDBStore&) = delete;
    RocksDBStore(RocksDBStore&&) = delete;
    RocksDBStore& operator=(RocksDBStore&&) = delete;

    // -- Lifecycle ----------------------------------------------------------

    /**
     * Open the RocksDB database.
     *
     * Creates the data directory if it doesn't exist. Opens all known
     * column families. If the database exists but is corrupted, attempts
     * automatic repair.
     *
     * @param config  Database configuration.
     * @return        result<void> on success.
     */
    result<void> open(const Config& config) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (db_) {
            return result<void>::failure(
                error_code::duplicate_resource,
                "RocksDBStore already open; close first");
        }

        config_ = config;

        std::filesystem::create_directories(config_.data_directory);

        auto logger = get_rocksdb_logger();
        logger->info("Opening RocksDB at: {}", config_.data_directory);

        // Build DBOptions
        rocksdb::DBOptions db_options;
        db_options.create_if_missing = config_.create_if_missing;
        db_options.create_missing_column_families = true;
        db_options.error_if_exists = config_.error_if_exists;
        db_options.paranoid_checks = config_.paranoid_checks;
        db_options.max_background_jobs = config_.max_background_jobs;
        db_options.stats_dump_period_sec = static_cast<unsigned int>(config_.stats_dump_period_sec);

        // Statistics
        db_options.statistics = rocksdb::CreateDBStatistics();

        // Configure logging to use spdlog
        db_options.info_log_level = rocksdb::InfoLogLevel::INFO_LEVEL;

        // Build ColumnFamilyOptions
        rocksdb::ColumnFamilyOptions cf_options;
        cf_options.OptimizeLevelStyleCompaction();

        // Block cache
        rocksdb::BlockBasedTableOptions table_options;
        table_options.block_cache = rocksdb::NewLRUCache(config_.block_cache_size);
        table_options.filter_policy.reset(
            rocksdb::NewBloomFilterPolicy(config_.bloom_bits_per_key));
        table_options.cache_index_and_filter_blocks = true;
        table_options.pin_l0_filter_and_index_blocks_in_cache = true;
        cf_options.table_factory.reset(
            rocksdb::NewBlockBasedTableFactory(table_options));

        // Write buffer
        cf_options.write_buffer_size = static_cast<size_t>(config_.write_buffer_size);
        cf_options.max_write_buffer_number = config_.max_write_buffer_number;

        // List of all column families (default is always present)
        std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
        cf_descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName, cf_options);
        cf_descriptors.emplace_back(std::string(kCfOffsets), cf_options);
        cf_descriptors.emplace_back(std::string(kCfTransactions), cf_options);
        cf_descriptors.emplace_back(std::string(kCfProducerIds), cf_options);
        cf_descriptors.emplace_back(std::string(kCfTopicConfigs), cf_options);
        cf_descriptors.emplace_back(std::string(kCfClusterMeta), cf_options);

        // Open the database
        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        rocksdb::DB* raw_db = nullptr;
        auto status = rocksdb::DB::Open(db_options, config_.data_directory,
                                        cf_descriptors, &handles, &raw_db);

        if (!status.ok()) {
            // Try repair
            logger->warn("RocksDB open failed: {}. Attempting repair...", status.ToString());
            auto repair_status = rocksdb::RepairDB(config_.data_directory, db_options);
            if (repair_status.ok()) {
                logger->info("RocksDB repair succeeded, retrying open");
                status = rocksdb::DB::Open(db_options, config_.data_directory,
                                           cf_descriptors, &handles, &raw_db);
            } else {
                logger->error("RocksDB repair failed: {}", repair_status.ToString());
            }
        }

        if (!status.ok()) {
            return result<void>::failure(
                error_code::storage_unavailable,
                fmt::format("RocksDB open failed: {}", status.ToString()));
        }

        db_.reset(raw_db);

        // Map column family handles by name
        const std::vector<std::string_view> cf_names = {
            rocksdb::kDefaultColumnFamilyName,
            kCfOffsets, kCfTransactions, kCfProducerIds,
            kCfTopicConfigs, kCfClusterMeta
        };
        for (size_t i = 0; i < handles.size() && i < cf_names.size(); ++i) {
            column_families_[std::string(cf_names[i])] = handles[i];
        }

        is_open_ = true;
        logger->info("RocksDB opened successfully ({} column families, {} handles)",
                     cf_names.size(), handles.size());

        return result<void>::success();
    }

    /**
     * Close the database.
     *
     * Flushes all pending writes, releases column family handles,
     * and destroys the DB instance. Idempotent.
     */
    result<void> close() {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!db_) return result<void>::success();

        auto logger = get_rocksdb_logger();
        logger->info("Closing RocksDB at: {}", config_.data_directory);

        // Release column family handles
        for (auto& [name, handle] : column_families_) {
            if (handle) {
                db_->DestroyColumnFamilyHandle(handle);
            }
        }
        column_families_.clear();

        // Cancel background work and close
        auto status = db_->Close();
        db_.reset();
        is_open_ = false;

        if (!status.ok()) {
            logger->error("RocksDB close failed: {}", status.ToString());
            return result<void>::failure(
                error_code::storage_unavailable,
                fmt::format("RocksDB close failed: {}", status.ToString()));
        }

        logger->info("RocksDB closed");
        return result<void>::success();
    }

    [[nodiscard]] bool is_open() const noexcept {
        return is_open_.load(std::memory_order_acquire);
    }

    // -- Basic Operations ---------------------------------------------------

    /**
     * Store a key-value pair in the specified column family.
     *
     * @param cf_name  Column family name.
     * @param key      Byte key.
     * @param value    Byte value.
     * @return         result<void> on success.
     */
    result<void> put(const std::string& cf_name,
                     std::string_view key,
                     std::string_view value) {
        if (!is_open_) {
            return result<void>::failure(
                error_code::storage_unavailable, "RocksDBStore not open");
        }

        auto* cf = get_cf(cf_name);
        if (!cf) {
            return result<void>::failure(
                error_code::resource_not_found,
                fmt::format("Column family not found: {}", cf_name));
        }

        rocksdb::Slice k(key.data(), key.size());
        rocksdb::Slice v(value.data(), value.size());

        rocksdb::WriteOptions opts;
        opts.sync = false;  // async write for performance (use sync_put for durability)

        auto status = db_->Put(opts, cf, k, v);
        if (!status.ok()) {
            return result<void>::failure(
                error_code::storage_unavailable,
                fmt::format("RocksDB put failed: {}", status.ToString()));
        }

        return result<void>::success();
    }

    /**
     * Store with synchronous durability (fsync on write).
     */
    result<void> sync_put(const std::string& cf_name,
                          std::string_view key,
                          std::string_view value) {
        if (!is_open_) {
            return result<void>::failure(
                error_code::storage_unavailable, "RocksDBStore not open");
        }

        auto* cf = get_cf(cf_name);
        if (!cf) {
            return result<void>::failure(
                error_code::resource_not_found,
                fmt::format("Column family not found: {}", cf_name));
        }

        rocksdb::Slice k(key.data(), key.size());
        rocksdb::Slice v(value.data(), value.size());

        rocksdb::WriteOptions opts;
        opts.sync = true;

        auto status = db_->Put(opts, cf, k, v);
        if (!status.ok()) {
            return result<void>::failure(
                error_code::storage_unavailable,
                fmt::format("RocksDB sync_put failed: {}", status.ToString()));
        }

        return result<void>::success();
    }

    /**
     * Retrieve a value by key.
     *
     * @param cf_name  Column family name.
     * @param key      Byte key.
     * @return         result with optional string (nullopt if not found).
     */
    result<std::optional<std::string>> get(const std::string& cf_name,
                                            std::string_view key) {
        if (!is_open_) {
            return result<std::optional<std::string>>::failure(
                error_code::storage_unavailable, "RocksDBStore not open");
        }

        auto* cf = get_cf(cf_name);
        if (!cf) {
            return result<std::optional<std::string>>::failure(
                error_code::resource_not_found,
                fmt::format("Column family not found: {}", cf_name));
        }

        rocksdb::Slice k(key.data(), key.size());
        rocksdb::ReadOptions opts;

        std::string value;
        auto status = db_->Get(opts, cf, k, &value);

        if (status.IsNotFound()) {
            return result<std::optional<std::string>>::success(std::nullopt);
        }

        if (!status.ok()) {
            return result<std::optional<std::string>>::failure(
                error_code::storage_unavailable,
                fmt::format("RocksDB get failed: {}", status.ToString()));
        }

        return result<std::optional<std::string>>::success(std::move(value));
    }

    /**
     * Delete a key.
     *
     * @param cf_name  Column family name.
     * @param key      Byte key.
     * @return         result<void> on success (deleting non-existent key is OK).
     */
    result<void> del(const std::string& cf_name, std::string_view key) {
        if (!is_open_) {
            return result<void>::failure(
                error_code::storage_unavailable, "RocksDBStore not open");
        }

        auto* cf = get_cf(cf_name);
        if (!cf) {
            return result<void>::failure(
                error_code::resource_not_found,
                fmt::format("Column family not found: {}", cf_name));
        }

        rocksdb::Slice k(key.data(), key.size());
        rocksdb::WriteOptions opts;

        auto status = db_->Delete(opts, cf, k);
        if (!status.ok()) {
            return result<void>::failure(
                error_code::storage_unavailable,
                fmt::format("RocksDB delete failed: {}", status.ToString()));
        }

        return result<void>::success();
    }

    // -- Batch Operations ---------------------------------------------------

    /**
     * A batch of writes applied atomically.
     */
    class WriteBatch {
    public:
        WriteBatch() : batch_(std::make_unique<rocksdb::WriteBatch>()) {}

        void put(const std::string& cf_name, std::string_view key, std::string_view value) {
            // Column family is resolved at commit time; store the name for now
            entries_.push_back({Op::Put, cf_name, std::string(key), std::string(value)});
        }

        void del(const std::string& cf_name, std::string_view key) {
            entries_.push_back({Op::Delete, cf_name, std::string(key), {}});
        }

        [[nodiscard]] size_t count() const noexcept { return entries_.size(); }
        void clear() noexcept { entries_.clear(); }

    private:
        friend class RocksDBStore;

        enum class Op : uint8_t { Put, Delete };

        struct Entry {
            Op op;
            std::string cf_name;
            std::string key;
            std::string value;
        };

        std::unique_ptr<rocksdb::WriteBatch> batch_;
        std::vector<Entry> entries_;
    };

    /**
     * Commit a WriteBatch atomically.
     *
     * @param batch  The batch to commit (cleared on success).
     * @return       result<void> on success.
     */
    result<void> commit_batch(WriteBatch& batch) {
        if (!is_open_) {
            return result<void>::failure(
                error_code::storage_unavailable, "RocksDBStore not open");
        }

        rocksdb::WriteBatch wb;
        for (const auto& entry : batch.entries_) {
            auto* cf = get_cf(entry.cf_name);
            if (!cf) {
                return result<void>::failure(
                    error_code::resource_not_found,
                    fmt::format("Column family not found in batch: {}", entry.cf_name));
            }

            rocksdb::Slice k(entry.key.data(), entry.key.size());
            if (entry.op == WriteBatch::Op::Put) {
                rocksdb::Slice v(entry.value.data(), entry.value.size());
                auto status = wb.Put(cf, k, v);
                if (!status.ok()) {
                    return result<void>::failure(
                        error_code::storage_unavailable,
                        fmt::format("Batch put failed: {}", status.ToString()));
                }
            } else {
                auto status = wb.Delete(cf, k);
                if (!status.ok()) {
                    return result<void>::failure(
                        error_code::storage_unavailable,
                        fmt::format("Batch delete failed: {}", status.ToString()));
                }
            }
        }

        rocksdb::WriteOptions opts;
        opts.sync = false;

        auto status = db_->Write(opts, &wb);
        if (!status.ok()) {
            return result<void>::failure(
                error_code::storage_unavailable,
                fmt::format("Batch commit failed: {}", status.ToString()));
        }

        batch.clear();
        return result<void>::success();
    }

    // -- Iteration ----------------------------------------------------------

    /**
     * Iterator for range scans over a column family.
     */
    class Iterator {
    public:
        Iterator(rocksdb::Iterator* iter) : iter_(iter) {}

        ~Iterator() { delete iter_; }

        Iterator(const Iterator&) = delete;
        Iterator& operator=(const Iterator&) = delete;

        [[nodiscard]] bool valid() const noexcept { return iter_->Valid(); }

        void seek_to_first() { iter_->SeekToFirst(); }
        void seek_to_last()  { iter_->SeekToLast(); }
        void seek(std::string_view key) {
            iter_->Seek(rocksdb::Slice(key.data(), key.size()));
        }
        void next() { iter_->Next(); }
        void prev() { iter_->Prev(); }

        [[nodiscard]] std::string_view key() const {
            auto s = iter_->key();
            return {s.data(), s.size()};
        }

        [[nodiscard]] std::string_view value() const {
            auto s = iter_->value();
            return {s.data(), s.size()};
        }

        [[nodiscard]] rocksdb::Status status() const { return iter_->status(); }

    private:
        rocksdb::Iterator* iter_;
    };

    /**
     * Create an iterator for the specified column family.
     *
     * The caller must delete the iterator when done.
     */
    std::unique_ptr<Iterator> iterator(const std::string& cf_name) {
        if (!is_open_) return nullptr;

        auto* cf = get_cf(cf_name);
        if (!cf) return nullptr;

        rocksdb::ReadOptions opts;
        auto* raw_iter = db_->NewIterator(opts, cf);
        return std::make_unique<Iterator>(raw_iter);
    }

    // -- Consumer Offsets (convenience) -------------------------------------

    /**
     * Store a consumer group offset commit.
     */
    result<void> put_offset(const std::string& group_id,
                             topic_id_t topic_id,
                             partition_id_t partition_id,
                             offset_t offset) {
        auto key = make_offset_key(group_id, topic_id, partition_id);
        std::string value;
        append_uint64(value, static_cast<uint64_t>(offset));
        return sync_put(std::string(kCfOffsets), key, value);
    }

    /**
     * Retrieve a consumer group offset commit.
     */
    result<std::optional<offset_t>> get_offset(const std::string& group_id,
                                                 topic_id_t topic_id,
                                                 partition_id_t partition_id) {
        auto key = make_offset_key(group_id, topic_id, partition_id);
        auto res = get(std::string(kCfOffsets), key);
        if (res.failed()) {
            return result<std::optional<offset_t>>::failure(res.error, res.error_message);
        }
        if (!res.value.has_value()) {
            return result<std::optional<offset_t>>::success(std::nullopt);
        }
        return result<std::optional<offset_t>>::success(
            static_cast<offset_t>(read_uint64(res.value.value())));
    }

    /**
     * List all offsets for a consumer group (prefix scan).
     */
    result<std::vector<std::tuple<topic_id_t, partition_id_t, offset_t>>>
    list_group_offsets(const std::string& group_id) {
        std::vector<std::tuple<topic_id_t, partition_id_t, offset_t>> results;

        std::string prefix = group_id;
        prefix.push_back('\0');

        auto it = iterator(std::string(kCfOffsets));
        if (!it) {
            return result<decltype(results)>::failure(
                error_code::storage_unavailable, "Cannot create iterator");
        }

        it->seek(prefix);
        while (it->valid() && it->key().starts_with(prefix)) {
            // Parse key beyond prefix: topic_id (8 bytes) + partition_id (4 bytes)
            auto key = it->key();
            auto suffix = key.substr(prefix.size());

            if (suffix.size() >= 12) {
                topic_id_t tid = static_cast<topic_id_t>(
                    read_uint64(suffix.substr(0, 8)));
                partition_id_t pid = static_cast<partition_id_t>(
                    read_uint64({suffix.data() + 8, 4}) & 0xFFFFFFFF);
                offset_t off = static_cast<offset_t>(read_uint64(it->value()));
                results.emplace_back(tid, pid, off);
            }

            it->next();
        }

        return result<decltype(results)>::success(std::move(results));
    }

    // -- Transaction State --------------------------------------------------

    /**
     * Store transaction state.
     */
    result<void> put_transaction_state(const std::string& transactional_id,
                                        std::string_view state) {
        return put(std::string(kCfTransactions),
                   make_transaction_key(transactional_id), state);
    }

    result<std::optional<std::string>> get_transaction_state(
        const std::string& transactional_id) {
        return get(std::string(kCfTransactions),
                   make_transaction_key(transactional_id));
    }

    result<void> delete_transaction_state(const std::string& transactional_id) {
        return del(std::string(kCfTransactions),
                   make_transaction_key(transactional_id));
    }

    // -- Statistics ---------------------------------------------------------

    /**
     * Get a human-readable statistics string from RocksDB.
     */
    [[nodiscard]] std::string get_statistics() const {
        if (!db_) return "RocksDB not open";
        return db_->GetOptions().statistics->ToString();
    }

    /**
     * Get the approximate disk usage of the database.
     */
    [[nodiscard]] uint64_t approximate_size() const {
        if (!db_ || config_.data_directory.empty()) return 0;

        uint64_t total = 0;
        for (const auto& entry :
             std::filesystem::recursive_directory_iterator(config_.data_directory)) {
            if (entry.is_regular_file()) {
                total += entry.file_size();
            }
        }
        return total;
    }

private:
    // -- Internal helpers ---------------------------------------------------

    rocksdb::ColumnFamilyHandle* get_cf(const std::string& name) {
        auto it = column_families_.find(name);
        if (it != column_families_.end()) {
            return it->second;
        }
        // Try "default"
        it = column_families_.find(std::string(rocksdb::kDefaultColumnFamilyName));
        if (it != column_families_.end()) {
            return it->second;
        }
        return nullptr;
    }

    Config config_;
    std::unique_ptr<rocksdb::DB> db_;
    std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*> column_families_;
    std::atomic<bool> is_open_{false};
    mutable std::mutex mutex_;
};

} // namespace torrent
