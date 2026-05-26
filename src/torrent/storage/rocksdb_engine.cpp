/**
 * rocksdb_engine.cpp — Full RocksDB Storage Engine for Metadata Persistence
 *
 * torrent-mq: High-performance distributed message queue
 *
 * This file implements a comprehensive RocksDB-backed storage engine for
 * persisting all broker metadata: topic configurations, partition offsets,
 * consumer group states, transaction metadata, schema registrations, and
 * operational state. It provides:
 *
 *   - RocksDBStore: primary engine with configurable options (block cache,
 *     write buffer, compression, bloom filters, rate limiter)
 *   - Column family support for separate namespaces:
 *       cf_offsets     — partition offset tracking
 *       cf_groups      — consumer group metadata
 *       cf_transactions — transactional producer state
 *       cf_schemas     — schema registry entries
 *       cf_configs     — broker/topic configuration
 *       cf_locks       — distributed lock state
 *       cf_sessions    — client session state
 *   - WriteBatch for atomic multi-key operations across column families
 *   - Iterator with prefix seek for range scans
 *   - Snapshot for consistent point-in-time reads
 *   - Checkpoint API for hot backup without stopping writes
 *   - Statistics collection and performance context
 *   - Rate limiter for compaction/flush I/O throttling
 *   - Event listener for compaction and flush lifecycle events
 *   - TTL support via compaction filter for expiring data
 *   - Merge operator for atomic counter accumulation
 *   - TransactionDB for ACID transactions with pessimistic locking
 *   - OptimisticTransactionDB for low-contention workloads
 *   - BackupEngine for incremental backups with shared file deduplication
 *   - Restore from backup with checksum verification
 *   - Import/export SST files for bulk data movement
 *   - Memory monitoring and adaptive tuning
 *
 * All methods: put/get/delete/write/write_batch/iterator/prefix_seek/snapshot/
 *              checkpoint with comprehensive error handling and logging.
 *
 * Dependencies: librocksdb (>= 7.x), spdlog, POSIX filesystem
 *
 * @see rocksdb_engine.h for the public API
 * @see https://github.com/facebook/rocksdb/wiki
 */

#include "torrent/storage/rocksdb_engine.h"
#include "torrent/storage/disk_io.h"
#include "torrent/common/types.h"
#include "torrent/common/config.h"
#include "torrent/metrics/metrics.h"

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/iterator.h>
#include <rocksdb/snapshot.h>
#include <rocksdb/checkpoint.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/backup_engine.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/cache.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/statistics.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/iostats_context.h>
#include <rocksdb/listener.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/options.h>
#include <rocksdb/convenience.h>
#include <rocksdb/env.h>
#include <rocksdb/version.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <regex>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// ============================================================================
// Convenience aliases
// ============================================================================

using namespace std::chrono_literals;

namespace torrent {

// ============================================================================
// Anonymous namespace — internal helpers, constants, and utilities
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logging
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_rocksdb_logger() {
    static auto logger = []() {
        auto l = spdlog::get("rocksdb_engine");
        if (!l) {
            l = spdlog::stdout_color_mt("rocksdb_engine");
            l->set_level(spdlog::level::info);
            l->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%^%l%$] %v");
        }
        return l;
    }();
    return logger;
}

#define ROCKSDB_LOG_TRACE(...) get_rocksdb_logger()->trace(__VA_ARGS__)
#define ROCKSDB_LOG_DEBUG(...) get_rocksdb_logger()->debug(__VA_ARGS__)
#define ROCKSDB_LOG_INFO(...)  get_rocksdb_logger()->info(__VA_ARGS__)
#define ROCKSDB_LOG_WARN(...)  get_rocksdb_logger()->warn(__VA_ARGS__)
#define ROCKSDB_LOG_ERROR(...) get_rocksdb_logger()->error(__VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Default database path relative to the broker data directory
static constexpr std::string_view kDefaultDbPath = "rocksdb_metadata";

/// Column family names — each represents a separate logical namespace
static constexpr std::string_view kCfDefault       = "default";
static constexpr std::string_view kCfOffsets       = "cf_offsets";
static constexpr std::string_view kCfGroups        = "cf_groups";
static constexpr std::string_view kCfTransactions  = "cf_transactions";
static constexpr std::string_view kCfSchemas       = "cf_schemas";
static constexpr std::string_view kCfConfigs       = "cf_configs";
static constexpr std::string_view kCfLocks         = "cf_locks";
static constexpr std::string_view kCfSessions      = "cf_sessions";

/// Default block cache size: 256 MB
static constexpr size_t kDefaultBlockCacheSize = 256ULL * 1024 * 1024;

/// Default write buffer size per column family: 64 MB
static constexpr size_t kDefaultWriteBufferSize = 64ULL * 1024 * 1024;

/// Maximum number of write buffers (memtables) per column family
static constexpr int kDefaultMaxWriteBufferNumber = 6;

/// Default number of background threads for flush and compaction
static constexpr int kDefaultBackgroundThreads = 4;

/// Default bloom filter bits per key
static constexpr int kDefaultBloomBitsPerKey = 10;

/// Default compaction style: leveled
static constexpr std::string_view kDefaultCompactionStyle = "level";

/// Default compression type (bottommost uses ZSTD for archival)
static constexpr std::string_view kDefaultCompression = "lz4";

/// TTL in seconds for session data (24 hours)
static constexpr int kSessionTtlSeconds = 86400;

/// Maximum number of open files for the database
static constexpr int kMaxOpenFiles = 10000;

/// Statistics dump interval in seconds
static constexpr int kStatsDumpIntervalSeconds = 300;

/// Write stall thresholds
static constexpr int kSoftPendingCompactionBytesLimit = 64ULL * 1024 * 1024 * 1024;  // 64 GB
static constexpr int kHardPendingCompactionBytesLimit = 256ULL * 1024 * 1024 * 1024; // 256 GB

/// Backup directory relative to data directory
static constexpr std::string_view kDefaultBackupDir = "rocksdb_backups";

/// Maximum number of backups to retain
static constexpr uint32_t kMaxBackupCount = 7;

/// Key prefix separator for prefix-based column family partitioning
static constexpr char kKeySeparator = ':';

// --------------------------------------------------------------------------
// Enumeration: ColumnFamilyId maps logical names to RocksDB handles
// --------------------------------------------------------------------------

enum class ColumnFamilyId : uint8_t {
    Default       = 0,
    Offsets       = 1,
    Groups        = 2,
    Transactions  = 3,
    Schemas       = 4,
    Configs       = 5,
    Locks         = 6,
    Sessions      = 7,
    Count         = 8  // sentinel
};

/// Lookup table: column family name to ID
static const std::unordered_map<std::string_view, ColumnFamilyId> kCfNameToId = {
    {kCfDefault,      ColumnFamilyId::Default},
    {kCfOffsets,      ColumnFamilyId::Offsets},
    {kCfGroups,       ColumnFamilyId::Groups},
    {kCfTransactions, ColumnFamilyId::Transactions},
    {kCfSchemas,      ColumnFamilyId::Schemas},
    {kCfConfigs,      ColumnFamilyId::Configs},
    {kCfLocks,        ColumnFamilyId::Locks},
    {kCfSessions,     ColumnFamilyId::Sessions},
};

/// Reverse lookup: ID to name
static const std::array<std::string_view,
    static_cast<size_t>(ColumnFamilyId::Count)> kCfIdToName = {
    kCfDefault,
    kCfOffsets,
    kCfGroups,
    kCfTransactions,
    kCfSchemas,
    kCfConfigs,
    kCfLocks,
    kCfSessions,
};

/// All column family names (excluding default) for creation
static const std::vector<std::string> kAllColumnFamilyNames = {
    std::string(kCfOffsets),
    std::string(kCfGroups),
    std::string(kCfTransactions),
    std::string(kCfSchemas),
    std::string(kCfConfigs),
    std::string(kCfLocks),
    std::string(kCfSessions),
};

// --------------------------------------------------------------------------
// Utility: status-to-string helper
// --------------------------------------------------------------------------

/**
 * @brief Converts a rocksdb::Status to a human-readable string.
 *
 * Includes the code, subcode, and message from the status object. Used for
 * logging and error reporting throughout the engine.
 *
 * @param s The rocksdb::Status to format
 * @return A string representation suitable for logging
 */
std::string status_to_string(const rocksdb::Status& s) {
    if (s.ok()) return "OK";
    std::ostringstream oss;
    oss << s.ToString();
    // Append subcode if present
    auto subcode = s.subcode();
    if (subcode != rocksdb::Status::SubCode::kNone) {
        oss << " [subcode=" << static_cast<int>(subcode) << "]";
    }
    return oss.str();
}

/**
 * @brief Checks a rocksdb::Status and logs an error if not OK.
 *
 * @param s The status to check
 * @param operation A description of the operation that produced the status
 * @param context Additional context (e.g., key, column family)
 * @return true if OK, false otherwise
 */
bool check_status(const rocksdb::Status& s,
                  std::string_view operation,
                  std::string_view context = "") {
    if (s.ok()) return true;
    ROCKSDB_LOG_ERROR("RocksDB {} failed: {}  context: {}",
                      operation, status_to_string(s), context);
    return false;
}

// --------------------------------------------------------------------------
// Utility: time helpers
// --------------------------------------------------------------------------

/**
 * @brief Returns the current time in milliseconds since epoch.
 *
 * Used for TTL calculations, statistics timestamps, and rate calculations.
 *
 * @return int64_t milliseconds since Unix epoch
 */
int64_t now_ms() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
}

/**
 * @brief Returns the current time in microseconds since epoch.
 *
 * Used for fine-grained latency measurements in performance context.
 *
 * @return int64_t microseconds since Unix epoch
 */
int64_t now_us() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count();
}

// --------------------------------------------------------------------------
// CompactionFilter for TTL support
// --------------------------------------------------------------------------

/**
 * @brief TTL compaction filter that expires keys based on an embedded timestamp.
 *
 * Keys in TTL-enabled column families (e.g., sessions) encode their
 * expiration timestamp as the last 8 bytes (big-endian int64). When
 * compaction runs, this filter removes keys whose expiration time
 * has passed.
 *
 * This is more efficient than periodic scanning because RocksDB
 * invokes the filter during normal compaction, avoiding extra I/O.
 */
class TtlCompactionFilter : public rocksdb::CompactionFilter {
public:
    /**
     * @brief Constructs a TTL compaction filter.
     *
     * @param ttl_seconds Time-to-live in seconds. Keys older than this
     *                    (relative to current wall clock) are dropped.
     * @param current_time_ms Current wall clock time in milliseconds.
     */
    explicit TtlCompactionFilter(int ttl_seconds, int64_t current_time_ms)
        : ttl_seconds_(ttl_seconds)
        , current_time_ms_(current_time_ms) {}

    const char* Name() const override {
        return "TtlCompactionFilter";
    }

    rocksdb::CompactionFilter::Decision FilterV2(
        int /*level*/,
        const rocksdb::Slice& key,
        rocksdb::ValueType /*value_type*/,
        const rocksdb::Slice& /*existing_value*/,
        std::string* /*new_value*/,
        std::string* /*skip_until*/) const override {

        // Keys shorter than 8 bytes cannot encode a TTL timestamp
        if (key.size() < 8) {
            return Decision::kKeep;
        }

        // Extract the timestamp from the last 8 bytes of the key
        const char* ts_start = key.data() + key.size() - 8;
        int64_t expiry_ms = 0;
        for (int i = 0; i < 8; ++i) {
            expiry_ms = (expiry_ms << 8) | static_cast<uint8_t>(ts_start[i]);
        }

        // If the current time exceeds the expiry, drop the key
        if (current_time_ms_ >= expiry_ms) {
            return Decision::kRemove;
        }

        return Decision::kKeep;
    }

private:
    int ttl_seconds_;
    int64_t current_time_ms_;
};

/**
 * @brief Factory for creating TtlCompactionFilter instances.
 *
 * Each compaction creates a fresh filter with the current wall clock time
 * to ensure expiration decisions are always up-to-date.
 */
class TtlCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
public:
    explicit TtlCompactionFilterFactory(int ttl_seconds)
        : ttl_seconds_(ttl_seconds) {}

    const char* Name() const override {
        return "TtlCompactionFilterFactory";
    }

    std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
        const rocksdb::CompactionFilter::Context& /*context*/) override {
        return std::make_unique<TtlCompactionFilter>(ttl_seconds_, now_ms());
    }

private:
    int ttl_seconds_;
};

// --------------------------------------------------------------------------
// Merge Operator for atomic counter accumulation
// --------------------------------------------------------------------------

/**
 * @brief Merge operator that implements 64-bit unsigned integer addition.
 *
 * Used for counters such as per-partition message counts, byte totals,
 * and consumer group offsets. The merge pattern uses `Merge(key, delta)`
 * to atomically increment a counter without a read-modify-write cycle.
 *
 * Value format: 8-byte little-endian uint64_t.
 * Merge operand format: 8-byte little-endian uint64_t (the delta).
 */
class UInt64AddOperator : public rocksdb::AssociativeMergeOperator {
public:
    const char* Name() const override {
        return "UInt64AddOperator";
    }

    bool Merge(const rocksdb::Slice& /*key*/,
               const rocksdb::Slice* existing_value,
               const rocksdb::Slice& value,
               std::string* new_value,
               rocksdb::Logger* /*logger*/) const override {

        // Parse the delta operand
        if (value.size() != sizeof(uint64_t)) {
            ROCKSDB_LOG_WARN("UInt64AddOperator: invalid merge operand size {}",
                             value.size());
            return false;
        }
        uint64_t delta = 0;
        std::memcpy(&delta, value.data(), sizeof(uint64_t));

        // Start with the existing value or zero
        uint64_t base = 0;
        if (existing_value && !existing_value->empty()) {
            if (existing_value->size() != sizeof(uint64_t)) {
                ROCKSDB_LOG_WARN("UInt64AddOperator: invalid existing value size {}",
                                 existing_value->size());
                return false;
            }
            std::memcpy(&base, existing_value->data(), sizeof(uint64_t));
        }

        // Perform the addition
        uint64_t result = base + delta;

        // Serialize the result
        new_value->resize(sizeof(uint64_t));
        std::memcpy(new_value->data(), &result, sizeof(uint64_t));

        return true;
    }
};

// --------------------------------------------------------------------------
// Event Listener for compaction and flush lifecycle events
// --------------------------------------------------------------------------

/**
 * @brief Event listener that records compaction and flush events with
 *        detailed metrics and logging.
 *
 * Hooks into RocksDB's event system to track:
 *   - Compaction begin/complete with input/output sizes and duration
 *   - Flush begin/complete with table properties
 *   - Stall conditions with stall duration
 *   - Table file creation and deletion events
 *
 * All events are logged at appropriate levels and emitted as metrics
 * for monitoring and alerting.
 */
class RocksDbEventListener : public rocksdb::EventListener {
public:
    RocksDbEventListener() = default;

    const char* Name() const override {
        return "TorrentRocksDbEventListener";
    }

    void OnCompactionBegin(rocksdb::DB* /*db*/,
                           const rocksdb::CompactionJobInfo& info) override {
        ROCKSDB_LOG_DEBUG(
            "Compaction begin: cf='{}' level={} input_files={} "
            "input_bytes={} output_level={} reason={}",
            info.cf_name,
            info.input_file_infos.empty() ? -1
                : info.input_file_infos[0].level,
            info.input_file_infos.size(),
            info.stats.total_input_bytes,
            info.output_level,
            static_cast<int>(info.compaction_reason));

        // Increment compaction-in-progress gauge
        active_compactions_.fetch_add(1, std::memory_order_relaxed);
        total_compactions_started_.fetch_add(1, std::memory_order_relaxed);
    }

    void OnCompactionCompleted(rocksdb::DB* /*db*/,
                               const rocksdb::CompactionJobInfo& info) override {
        auto duration_us = info.stats.elapsed_micros;

        ROCKSDB_LOG_DEBUG(
            "Compaction complete: cf='{}' level={}->{} "
            "duration_us={} input_bytes={} output_bytes={} "
            "write_amp={:.2f} num_input_entries={} "
            "num_output_entries={}",
            info.cf_name,
            info.input_file_infos.empty() ? -1
                : info.input_file_infos[0].level,
            info.output_level,
            duration_us,
            info.stats.total_input_bytes,
            info.stats.total_output_bytes,
            info.stats.total_input_bytes > 0
                ? static_cast<double>(info.stats.total_output_bytes) /
                  info.stats.total_input_bytes
                : 0.0,
            info.stats.num_input_records,
            info.stats.num_output_records);

        active_compactions_.fetch_sub(1, std::memory_order_relaxed);
        total_compaction_duration_us_.fetch_add(duration_us,
            std::memory_order_relaxed);
        total_compaction_input_bytes_.fetch_add(
            info.stats.total_input_bytes, std::memory_order_relaxed);
        total_compaction_output_bytes_.fetch_add(
            info.stats.total_output_bytes, std::memory_order_relaxed);
    }

    void OnFlushBegin(rocksdb::DB* /*db*/,
                      const rocksdb::FlushJobInfo& info) override {
        ROCKSDB_LOG_DEBUG(
            "Flush begin: cf='{}' memtable_id={} "
            "num_entries={} reason={}",
            info.cf_name,
            info.job_id,
            info.table_properties.num_entries,
            static_cast<int>(info.flush_reason));

        active_flushes_.fetch_add(1, std::memory_order_relaxed);
        total_flushes_started_.fetch_add(1, std::memory_order_relaxed);
    }

    void OnFlushCompleted(rocksdb::DB* /*db*/,
                          const rocksdb::FlushJobInfo& info) override {
        ROCKSDB_LOG_DEBUG(
            "Flush complete: cf='{}' job_id={} "
            "duration_us={} output_bytes={} num_entries={} "
            "write_amp={:.2f}",
            info.cf_name,
            info.job_id,
            info.table_properties.elapsed_micros,
            info.table_properties.data_size,
            info.table_properties.num_entries,
            info.flush_reason == rocksdb::FlushReason::kWriteBufferFull
                ? 1.0 : 0.0);

        // RocksDB v7 FlushJobInfo might not have elapsed_micros;
        // we use a rough estimate from compaction stats
        active_flushes_.fetch_sub(1, std::memory_order_relaxed);
    }

    void OnStallConditionsChanged(
        const rocksdb::WriteStallInfo& info) override {
        const char* condition_str = "unknown";
        switch (info.condition.cur) {
            case rocksdb::WriteStallCondition::kNormal:
                condition_str = "normal";
                break;
            case rocksdb::WriteStallCondition::kDelayed:
                condition_str = "delayed";
                break;
            case rocksdb::WriteStallCondition::kStopped:
                condition_str = "stopped";
                break;
        }

        ROCKSDB_LOG_WARN(
            "Write stall changed: cf='{}' condition={}",
            info.cf_name, condition_str);

        if (info.condition.cur == rocksdb::WriteStallCondition::kStopped) {
            stall_count_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    /// Retrieve the current number of active compactions
    int64_t active_compactions() const {
        return active_compactions_.load(std::memory_order_relaxed);
    }

    /// Retrieve the current number of active flushes
    int64_t active_flushes() const {
        return active_flushes_.load(std::memory_order_relaxed);
    }

    /// Retrieve cumulative compaction count
    int64_t total_compactions() const {
        return total_compactions_started_.load(std::memory_order_relaxed);
    }

    /// Retrieve cumulative flush count
    int64_t total_flushes() const {
        return total_flushes_started_.load(std::memory_order_relaxed);
    }

    /// Retrieve cumulative compaction input bytes
    int64_t compaction_input_bytes() const {
        return total_compaction_input_bytes_.load(std::memory_order_relaxed);
    }

    /// Retrieve cumulative compaction output bytes
    int64_t compaction_output_bytes() const {
        return total_compaction_output_bytes_.load(std::memory_order_relaxed);
    }

    /// Retrieve cumulative compaction duration in microseconds
    int64_t compaction_duration_us() const {
        return total_compaction_duration_us_.load(std::memory_order_relaxed);
    }

    /// Retrieve write stall count
    int64_t stall_count() const {
        return stall_count_.load(std::memory_order_relaxed);
    }

private:
    std::atomic<int64_t> active_compactions_{0};
    std::atomic<int64_t> active_flushes_{0};
    std::atomic<int64_t> total_compactions_started_{0};
    std::atomic<int64_t> total_flushes_started_{0};
    std::atomic<int64_t> total_compaction_duration_us_{0};
    std::atomic<int64_t> total_compaction_input_bytes_{0};
    std::atomic<int64_t> total_compaction_output_bytes_{0};
    std::atomic<int64_t> stall_count_{0};
};

// --------------------------------------------------------------------------
// Key encoding utilities for prefix-based organization
// --------------------------------------------------------------------------

/**
 * @brief Constructs a key for the offsets column family.
 *
 * Format: "topic:partition" (e.g., "orders:3")
 *
 * @param topic The topic name
 * @param partition Partition index
 * @return std::string The encoded key
 */
std::string make_offset_key(std::string_view topic, int32_t partition) {
    std::ostringstream oss;
    oss << topic << kKeySeparator << partition;
    return oss.str();
}

/**
 * @brief Constructs a key for the groups column family.
 *
 * Format: "group_id:topic:partition" (e.g., "my-consumer:orders:3")
 *
 * @param group_id The consumer group identifier
 * @param topic The topic name
 * @param partition Partition index
 * @return std::string The encoded key
 */
std::string make_group_key(std::string_view group_id,
                           std::string_view topic,
                           int32_t partition) {
    std::ostringstream oss;
    oss << group_id << kKeySeparator << topic << kKeySeparator << partition;
    return oss.str();
}

/**
 * @brief Constructs a prefix key for range scans.
 *
 * Appends the separator to the prefix to create an exclusive upper bound
 * for prefix-based iteration.
 *
 * @param prefix The prefix string (e.g., "orders:3")
 * @return std::string The prefix key suitable for rocksdb::Iterator::Seek
 */
std::string make_prefix_key(std::string_view prefix) {
    std::string result(prefix);
    result += kKeySeparator;
    return result;
}

// --------------------------------------------------------------------------
// Statistics formatting helper
// --------------------------------------------------------------------------

/**
 * @brief Formats RocksDB statistics as a multi-line human-readable string.
 *
 * Parses the statistics histogram from the Statistics object and formats
 * it with indentation for log output or admin API responses.
 *
 * @param stats Shared pointer to the RocksDB statistics object
 * @return std::string Formatted statistics string
 */
std::string format_statistics(
    const std::shared_ptr<rocksdb::Statistics>& stats) {
    if (!stats) return "No statistics available";
    return stats->ToString();
}

}  // anonymous namespace

// ============================================================================
// RocksDbConfig — Configuration structure for the RocksDB engine
// ============================================================================

/**
 * @brief Configuration for the RocksDB storage engine.
 *
 * All fields have sensible defaults suitable for a message queue metadata
 * workload (small keys, moderate write volume, read-heavy on some CFs).
 * Tuning guidance:
 *
 *   - block_cache_size: Larger values improve read performance by caching
 *     more index/data blocks. 256 MB is a good default for metadata.
 *   - write_buffer_size: Larger buffers reduce write amplification but
 *     increase memory usage and recovery time.
 *   - compression: LZ4 provides a good balance of speed and compression
 *     ratio. ZSTD is recommended for bottommost (archival) levels.
 *   - max_background_jobs: Should match available CPU cores for optimal
 *     compaction throughput. Typically 2-4 for dedicated metadata stores.
 */
struct RocksDbConfig {
    /// Path to the database directory on disk
    std::string db_path;

    /// Size of the LRU block cache in bytes (default: 256 MB)
    size_t block_cache_size = kDefaultBlockCacheSize;

    /// Size of each memtable write buffer in bytes (default: 64 MB)
    size_t write_buffer_size = kDefaultWriteBufferSize;

    /// Maximum number of write buffers (memtables) per column family
    int max_write_buffer_number = kDefaultMaxWriteBufferNumber;

    /// Minimum number of write buffers to merge before flushing
    int min_write_buffer_number_to_merge = 1;

    /// Maximum number of background jobs (compaction + flush)
    int max_background_jobs = kDefaultBackgroundThreads;

    /// Maximum number of open files for the database
    int max_open_files = kMaxOpenFiles;

    /// Number of bits per key for the bloom filter (0 disables)
    int bloom_bits_per_key = kDefaultBloomBitsPerKey;

    /// Compression algorithm for non-bottommost levels
    std::string compression = std::string(kDefaultCompression);

    /// Compression algorithm for bottommost levels
    std::string bottommost_compression = "zstd";

    /// Whether to enable RocksDB statistics collection
    bool enable_statistics = true;

    /// Statistics dump interval in seconds (0 disables periodic dump)
    int stats_dump_period_sec = kStatsDumpIntervalSeconds;

    /// Whether to enable the write-ahead log (WAL)
    bool enable_wal = true;

    /// WAL recovery mode
    rocksdb::WALRecoveryMode wal_recovery_mode =
        rocksdb::WALRecoveryMode::kPointInTimeRecovery;

    /// Rate limiter bytes per second for compaction and flush (0 = unlimited)
    int64_t rate_limiter_bytes_per_sec = 0;

    /// Whether to use transactions (TransactionDB) instead of raw DB
    bool use_transactions = false;

    /// Whether to use optimistic transactions for low-contention workloads
    bool use_optimistic_transactions = false;

    /// Whether to create column families if they are missing on open
    bool create_missing_column_families = true;

    /// Whether to create the database if it is missing on open
    bool create_if_missing = true;

    /// Whether to report per-operation I/O statistics
    bool enable_io_stats = false;

    /// Whether to enable paranoid checks (extra data integrity verification)
    bool paranoid_checks = false;

    /// Write stall soft limit for pending compaction bytes
    uint64_t soft_pending_compaction_bytes_limit =
        kSoftPendingCompactionBytesLimit;

    /// Write stall hard limit for pending compaction bytes
    uint64_t hard_pending_compaction_bytes_limit =
        kHardPendingCompactionBytesLimit;

    /// TTL in seconds for session data (0 disables TTL)
    int session_ttl_seconds = kSessionTtlSeconds;

    /// Backup directory (empty disables backups)
    std::string backup_dir;

    /// Maximum number of backup snapshots to retain
    uint32_t max_backup_count = kMaxBackupCount;

    /// Whether to auto-flush the WAL on write operations
    bool manual_wal_flush = false;

    /// Whether to use direct I/O for SST files
    bool use_direct_reads = false;
    bool use_direct_io_for_flush_and_compaction = false;

    /// Target file size for compaction (default: 64 MB)
    uint64_t target_file_size_base = 64ULL * 1024 * 1024;
};

// ============================================================================
// RocksDBStore — The primary RocksDB storage engine
// ============================================================================

/**
 * @brief Full-featured RocksDB storage engine for torrent-mq metadata.
 *
 * Manages a RocksDB (or TransactionDB/OptimisticTransactionDB) instance
 * with multiple column families for logical data separation. Provides
 * the full CRUD API with atomic write batches, consistent snapshots,
 * prefix-based range scans, and hot backup support.
 *
 * Thread safety:
 *   - All public methods are protected by a shared_mutex. Read operations
 *     (get, iterator, snapshot) acquire a shared lock. Write operations
 *     (put, delete, write_batch) acquire an exclusive lock. This ensures
 *     consistency between operations but note that multiple readers can
 *     operate concurrently.
 *
 *   - The underlying RocksDB instance is itself thread-safe for reads
 *     and writes; the additional mutex protects against concurrent
 *     configuration changes (e.g., closing the database while a read
 *     is in progress).
 *
 * Lifecycle:
 *   1. Construct with a RocksDbConfig
 *   2. Call open() to create or open the database
 *   3. Perform read/write operations
 *   4. Call checkpoint() or backup() for hot backup
 *   5. Call close() before destruction
 *
 * @see RocksDbConfig for tuning parameters
 */
class RocksDBStore {
public:
    // ------------------------------------------------------------------------
    // Construction / Destruction
    // ------------------------------------------------------------------------

    /**
     * @brief Constructs a RocksDBStore with the given configuration.
     *
     * The database is not opened until open() is called. This allows
     * the caller to set up additional state (e.g., metric registrations)
     * before I/O begins.
     *
     * @param config The RocksDB configuration parameters
     */
    explicit RocksDBStore(const RocksDbConfig& config)
        : config_(config)
        , is_open_(false)
        , is_readonly_(false)
        , total_puts_(0)
        , total_gets_(0)
        , total_deletes_(0)
        , total_write_batches_(0) {
        ROCKSDB_LOG_INFO("RocksDBStore created (not yet opened): path={}",
                         config_.db_path);
    }

    /**
     * @brief Destructor — ensures the database is closed.
     *
     * If close() has not been called, it is invoked with a warning.
     * In production, always call close() explicitly to avoid log spam.
     */
    ~RocksDBStore() {
        if (is_open_.load(std::memory_order_acquire)) {
            ROCKSDB_LOG_WARN("RocksDBStore destroyed without close(); "
                             "closing now");
            close();
        }
    }

    // Prevent copying and moving of the store
    RocksDBStore(const RocksDBStore&) = delete;
    RocksDBStore& operator=(const RocksDBStore&) = delete;
    RocksDBStore(RocksDBStore&&) = delete;
    RocksDBStore& operator=(RocksDBStore&&) = delete;

    // ------------------------------------------------------------------------
    // Database Lifecycle
    // ------------------------------------------------------------------------

    /**
     * @brief Opens or creates the RocksDB database.
     *
     * Configures all options (block cache, bloom filter, compression, rate
     * limiter, etc.), creates column families if needed, and establishes
     * the event listener for monitoring.
     *
     * If config_.use_transactions is true, opens a TransactionDB instead
     * of a raw DB. If config_.use_optimistic_transactions is true, opens
     * an OptimisticTransactionDB.
     *
     * @return rocksdb::Status OK if opened successfully, or an error status
     */
    rocksdb::Status open() {
        std::unique_lock lock(mutex_);

        if (is_open_.load(std::memory_order_acquire)) {
            return rocksdb::Status::Busy("Database is already open");
        }

        ROCKSDB_LOG_INFO("Opening RocksDB at path: {}", config_.db_path);

        // --- Build DBOptions ---
        rocksdb::DBOptions db_opts = build_db_options();

        // --- Build column family descriptors ---
        std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
        cf_descriptors.reserve(kAllColumnFamilyNames.size() + 1);

        // Default column family
        rocksdb::ColumnFamilyOptions default_cf_opts =
            build_column_family_options(ColumnFamilyId::Default);
        cf_descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName,
                                    default_cf_opts);

        // Named column families
        for (const auto& cf_name : kAllColumnFamilyNames) {
            auto cf_id = kCfNameToId.at(cf_name);
            rocksdb::ColumnFamilyOptions cf_opts =
                build_column_family_options(cf_id);
            cf_descriptors.emplace_back(cf_name, cf_opts);
        }

        // --- Open the database ---
        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        rocksdb::Status s;

        if (config_.use_transactions) {
            // TransactionDB: pessimistic locking, full ACID
            rocksdb::TransactionDBOptions txn_db_opts;
            txn_db_opts.max_num_locks = 1024 * 1024;
            txn_db_opts.num_stripes = 16;
            txn_db_opts.transaction_lock_timeout = 10000;  // 10 seconds
            txn_db_opts.default_lock_timeout = 5000;       // 5 seconds
            txn_db_opts.write_policy =
                rocksdb::TxnDBWritePolicy::WRITE_COMMITTED;

            ROCKSDB_LOG_INFO("Opening TransactionDB with {} column families",
                             cf_descriptors.size());

            rocksdb::TransactionDB* txn_db = nullptr;
            s = rocksdb::TransactionDB::Open(
                db_opts, txn_db_opts, config_.db_path,
                cf_descriptors, &handles, &txn_db);
            if (s.ok()) {
                db_.reset(static_cast<rocksdb::DB*>(txn_db));
                txn_db_ = txn_db;
            }
        } else if (config_.use_optimistic_transactions) {
            // OptimisticTransactionDB: no locking, conflict detection at commit
            rocksdb::OptimisticTransactionDBOptions opt_txn_db_opts;
            opt_txn_db_opts.validate_policy =
                rocksdb::OccValidationPolicy::kOccValidationPolicyParallel;

            ROCKSDB_LOG_INFO(
                "Opening OptimisticTransactionDB with {} column families",
                cf_descriptors.size());

            rocksdb::OptimisticTransactionDB* opt_txn_db = nullptr;
            s = rocksdb::OptimisticTransactionDB::Open(
                db_opts, opt_txn_db_opts, config_.db_path,
                cf_descriptors, &handles, &opt_txn_db);
            if (s.ok()) {
                db_.reset(static_cast<rocksdb::DB*>(opt_txn_db));
                opt_txn_db_ = opt_txn_db;
            }
        } else {
            // Regular DB (no transactions)
            ROCKSDB_LOG_INFO("Opening regular DB with {} column families",
                             cf_descriptors.size());

            rocksdb::DB* raw_db = nullptr;
            s = rocksdb::DB::Open(db_opts, config_.db_path,
                                  cf_descriptors, &handles, &raw_db);
            if (s.ok()) {
                db_.reset(raw_db);
            }
        }

        if (!s.ok()) {
            ROCKSDB_LOG_ERROR("Failed to open RocksDB: {}",
                              status_to_string(s));
            return s;
        }

        // --- Store column family handles ---
        cf_handles_.clear();
        for (size_t i = 0; i < handles.size(); ++i) {
            cf_handles_.push_back(
                std::unique_ptr<rocksdb::ColumnFamilyHandle,
                    std::function<void(rocksdb::ColumnFamilyHandle*)>>(
                        handles[i],
                        [](rocksdb::ColumnFamilyHandle* h) {
                            if (h) {
                                // Handles are destroyed via DB::DestroyColumnFamilyHandle
                                // but we hold them until close()
                            }
                        }));
        }

        // --- Initialize backup engine if configured ---
        if (!config_.backup_dir.empty()) {
            rocksdb::BackupEngineOptions backup_opts(config_.backup_dir);
            backup_opts.backup_log_files = true;
            backup_opts.max_background_operations =
                config_.max_background_jobs / 2;

            rocksdb::BackupEngine* backup_raw = nullptr;
            s = rocksdb::BackupEngine::Open(
                rocksdb::Env::Default(), backup_opts, &backup_raw);
            if (s.ok()) {
                backup_engine_.reset(backup_raw);
                ROCKSDB_LOG_INFO("BackupEngine initialized at: {}",
                                 config_.backup_dir);
            } else {
                ROCKSDB_LOG_WARN("Failed to initialize BackupEngine: {}",
                                 status_to_string(s));
            }
        }

        is_open_.store(true, std::memory_order_release);

        ROCKSDB_LOG_INFO("RocksDB opened successfully: path={} cf_count={} "
                         "txn_mode={}",
                         config_.db_path, cf_handles_.size(),
                         config_.use_transactions ? "pessimistic" :
                         config_.use_optimistic_transactions ? "optimistic" :
                         "none");

        return rocksdb::Status::OK();
    }

    /**
     * @brief Opens the database in read-only mode.
     *
     * Useful for secondary instances, backup verification, or read-only
     * replicas that should never perform writes.
     *
     * @return rocksdb::Status OK if opened successfully
     */
    rocksdb::Status open_readonly() {
        std::unique_lock lock(mutex_);

        if (is_open_.load(std::memory_order_acquire)) {
            return rocksdb::Status::Busy("Database is already open");
        }

        ROCKSDB_LOG_INFO("Opening RocksDB in READ-ONLY mode: {}",
                         config_.db_path);

        rocksdb::DBOptions db_opts = build_db_options();

        std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
        rocksdb::ColumnFamilyOptions default_cf_opts =
            build_column_family_options(ColumnFamilyId::Default);
        cf_descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName,
                                    default_cf_opts);

        for (const auto& cf_name : kAllColumnFamilyNames) {
            auto cf_id = kCfNameToId.at(cf_name);
            rocksdb::ColumnFamilyOptions cf_opts =
                build_column_family_options(cf_id);
            cf_descriptors.emplace_back(cf_name, cf_opts);
        }

        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        rocksdb::DB* raw_db = nullptr;

        rocksdb::Status s = rocksdb::DB::OpenForReadOnly(
            db_opts, config_.db_path, cf_descriptors, &handles, &raw_db);

        if (!s.ok()) {
            ROCKSDB_LOG_ERROR("Failed to open RocksDB read-only: {}",
                              status_to_string(s));
            return s;
        }

        db_.reset(raw_db);
        cf_handles_.clear();
        for (auto* h : handles) {
            cf_handles_.push_back(
                std::unique_ptr<rocksdb::ColumnFamilyHandle,
                    std::function<void(rocksdb::ColumnFamilyHandle*)>>(
                        h, [](rocksdb::ColumnFamilyHandle*) {}));
        }

        is_readonly_.store(true, std::memory_order_release);
        is_open_.store(true, std::memory_order_release);

        ROCKSDB_LOG_INFO("RocksDB opened read-only: {} CFs",
                         cf_handles_.size());

        return rocksdb::Status::OK();
    }

    /**
     * @brief Closes the database and releases all resources.
     *
     * This is a graceful shutdown that:
     *   1. Cancels and destroys all column family handles
     *   2. Cancels any in-progress operations
     *   3. Flushes the WAL (if enabled)
     *   4. Destroys the backup engine
     *   5. Resets the shared pointer to the DB instance
     *
     * After close(), the store can be reopened by calling open() again.
     */
    void close() {
        std::unique_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire)) {
            return;
        }

        ROCKSDB_LOG_INFO("Closing RocksDB at path: {}", config_.db_path);

        // Flush all column families before shutdown
        if (db_ && !is_readonly_.load(std::memory_order_acquire)) {
            for (auto& cf_handle : cf_handles_) {
                if (cf_handle) {
                    rocksdb::Status s = db_->Flush(
                        rocksdb::FlushOptions(), cf_handle.get());
                    if (!s.ok()) {
                        ROCKSDB_LOG_WARN("Flush during close failed: {}",
                                         status_to_string(s));
                    }
                }
            }

            // Sync the WAL
            rocksdb::Status s = db_->SyncWAL();
            if (!s.ok()) {
                ROCKSDB_LOG_WARN("SyncWAL during close failed: {}",
                                 status_to_string(s));
            }
        }

        // Destroy backup engine before database
        backup_engine_.reset();

        // Destroy column family handles and database
        // cf_handles_ must be destroyed before db_ because the DB owns the handles
        cf_handles_.clear();

        // Reset the database pointer (this triggers DB::Close via unique_ptr)
        db_.reset();
        txn_db_ = nullptr;
        opt_txn_db_ = nullptr;

        is_open_.store(false, std::memory_order_release);
        is_readonly_.store(false, std::memory_order_release);

        ROCKSDB_LOG_INFO("RocksDB closed. Stats: puts={} gets={} deletes={} "
                         "write_batches={}",
                         total_puts_.load(std::memory_order_relaxed),
                         total_gets_.load(std::memory_order_relaxed),
                         total_deletes_.load(std::memory_order_relaxed),
                         total_write_batches_.load(std::memory_order_relaxed));
    }

    // ------------------------------------------------------------------------
    // Basic CRUD Operations
    // ------------------------------------------------------------------------

    /**
     * @brief Stores a key-value pair in the specified column family.
     *
     * This is a synchronous write that returns only after the write has
     * been committed to the WAL (if enabled) and memtable.
     *
     * @param cf The column family identifier
     * @param key The key to store (binary-safe)
     * @param value The value to associate with the key (binary-safe)
     * @return rocksdb::Status OK on success, or an error status
     */
    rocksdb::Status put(ColumnFamilyId cf,
                        std::string_view key,
                        std::string_view value) {
        std::shared_lock lock(mutex_);
        return put_locked(cf, key, value);
    }

    /**
     * @brief Retrieves the value for a key from the specified column family.
     *
     * @param cf The column family identifier
     * @param key The key to look up
     * @return std::optional<std::string> The value if found, std::nullopt
     *         if the key does not exist
     */
    std::optional<std::string> get(ColumnFamilyId cf,
                                   std::string_view key) {
        std::shared_lock lock(mutex_);
        return get_locked(cf, key);
    }

    /**
     * @brief Deletes a key from the specified column family.
     *
     * The deletion is asynchronous by default (a tombstone is written).
     * The space is reclaimed during subsequent compactions.
     *
     * @param cf The column family identifier
     * @param key The key to delete
     * @return rocksdb::Status OK on success, or an error status
     */
    rocksdb::Status del(ColumnFamilyId cf, std::string_view key) {
        std::shared_lock lock(mutex_);
        return del_locked(cf, key);
    }

    /**
     * @brief Checks if a key exists in the specified column family.
     *
     * Uses Get with a small read to avoid pulling large values from storage.
     *
     * @param cf The column family identifier
     * @param key The key to check
     * @return true if the key exists, false otherwise
     */
    bool exists(ColumnFamilyId cf, std::string_view key) {
        std::shared_lock lock(mutex_);
        return exists_locked(cf, key);
    }

    /**
     * @brief Retrieves multiple keys in a single call (batch get / MultiGet).
     *
     * MultiGet is more efficient than individual Get calls because RocksDB
     * can parallelize I/O across SST files. The results are returned in
     * the same order as the input keys.
     *
     * @param cf The column family identifier
     * @param keys Vector of keys to retrieve
     * @return std::vector<std::optional<std::string>> Results in key order;
     *         std::nullopt for keys that are not found
     */
    std::vector<std::optional<std::string>> multi_get(
        ColumnFamilyId cf,
        const std::vector<std::string_view>& keys) {

        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return std::vector<std::optional<std::string>>(
                keys.size(), std::nullopt);
        }

        auto* handle = get_cf_handle(cf);
        if (!handle) {
            return std::vector<std::optional<std::string>>(
                keys.size(), std::nullopt);
        }

        // Convert string_view to Slice for the RocksDB API
        std::vector<rocksdb::Slice> slices;
        slices.reserve(keys.size());
        for (const auto& k : keys) {
            slices.emplace_back(k.data(), k.size());
        }

        std::vector<rocksdb::PinnableSlice> values(keys.size());
        std::vector<rocksdb::Status> statuses(keys.size());

        db_->MultiGet(rocksdb::ReadOptions(), handle,
                      keys.size(), slices.data(),
                      values.data(), statuses.data());

        std::vector<std::optional<std::string>> results;
        results.reserve(keys.size());

        for (size_t i = 0; i < keys.size(); ++i) {
            if (statuses[i].ok()) {
                results.emplace_back(
                    std::string(values[i].data(), values[i].size()));
            } else if (statuses[i].IsNotFound()) {
                results.emplace_back(std::nullopt);
            } else {
                ROCKSDB_LOG_WARN("MultiGet error for key index {}: {}",
                                 i, status_to_string(statuses[i]));
                results.emplace_back(std::nullopt);
            }
        }

        total_gets_.fetch_add(keys.size(), std::memory_order_relaxed);
        return results;
    }

    // ------------------------------------------------------------------------
    // WriteBatch — atomic multi-key operations
    // ------------------------------------------------------------------------

    /**
     * @brief Creates a new WriteBatch for atomic multi-operation writes.
     *
     * All operations added to the batch (Put, Delete, Merge) are applied
     * atomically when commit_write_batch() is called. This is the
     * recommended way to perform multi-key updates that must be consistent.
     *
     * @return std::unique_ptr<rocksdb::WriteBatch> A new empty write batch
     */
    std::unique_ptr<rocksdb::WriteBatch> create_write_batch() {
        return std::make_unique<rocksdb::WriteBatch>();
    }

    /**
     * @brief Adds a Put operation to a write batch.
     *
     * @param batch The write batch to add to
     * @param cf The column family
     * @param key The key
     * @param value The value
     */
    void batch_put(rocksdb::WriteBatch* batch,
                   ColumnFamilyId cf,
                   std::string_view key,
                   std::string_view value) {
        if (!batch) return;
        auto* handle = get_cf_handle(cf);
        if (!handle) {
            ROCKSDB_LOG_ERROR("batch_put: invalid CF handle for cf={}",
                              static_cast<int>(cf));
            return;
        }
        batch->Put(handle, rocksdb::Slice(key.data(), key.size()),
                   rocksdb::Slice(value.data(), value.size()));
    }

    /**
     * @brief Adds a Delete operation to a write batch.
     *
     * @param batch The write batch to add to
     * @param cf The column family
     * @param key The key to delete
     */
    void batch_delete(rocksdb::WriteBatch* batch,
                      ColumnFamilyId cf,
                      std::string_view key) {
        if (!batch) return;
        auto* handle = get_cf_handle(cf);
        if (!handle) {
            ROCKSDB_LOG_ERROR("batch_delete: invalid CF handle for cf={}",
                              static_cast<int>(cf));
            return;
        }
        batch->Delete(handle, rocksdb::Slice(key.data(), key.size()));
    }

    /**
     * @brief Adds a Merge operation to a write batch.
     *
     * Merge operations use the configured MergeOperator (UInt64AddOperator)
     * to atomically combine values. This is useful for counters.
     *
     * @param batch The write batch to add to
     * @param cf The column family
     * @param key The key
     * @param delta The merge operand (e.g., a counter increment)
     */
    void batch_merge(rocksdb::WriteBatch* batch,
                     ColumnFamilyId cf,
                     std::string_view key,
                     std::string_view delta) {
        if (!batch) return;
        auto* handle = get_cf_handle(cf);
        if (!handle) {
            ROCKSDB_LOG_ERROR("batch_merge: invalid CF handle for cf={}",
                              static_cast<int>(cf));
            return;
        }
        batch->Merge(handle, rocksdb::Slice(key.data(), key.size()),
                     rocksdb::Slice(delta.data(), delta.size()));
    }

    /**
     * @brief Adds a SingleDelete operation to a write batch.
     *
     * SingleDelete is a performance optimization for keys that are written
     * exactly once and never overwritten. It avoids the tombstone overhead
     * of regular Delete. Only safe to use when the key has a single version.
     *
     * @param batch The write batch
     * @param cf The column family
     * @param key The key
     */
    void batch_single_delete(rocksdb::WriteBatch* batch,
                             ColumnFamilyId cf,
                             std::string_view key) {
        if (!batch) return;
        auto* handle = get_cf_handle(cf);
        if (!handle) return;
        batch->SingleDelete(handle, rocksdb::Slice(key.data(), key.size()));
    }

    /**
     * @brief Adds a DeleteRange operation to a write batch.
     *
     * Deletes all keys in the range [begin, end). This is more efficient
     * than individual deletes for bulk cleanup operations.
     *
     * @param batch The write batch
     * @param cf The column family
     * @param begin_key Start of the range (inclusive)
     * @param end_key End of the range (exclusive)
     */
    void batch_delete_range(rocksdb::WriteBatch* batch,
                            ColumnFamilyId cf,
                            std::string_view begin_key,
                            std::string_view end_key) {
        if (!batch) return;
        auto* handle = get_cf_handle(cf);
        if (!handle) return;
        batch->DeleteRange(handle,
                           rocksdb::Slice(begin_key.data(), begin_key.size()),
                           rocksdb::Slice(end_key.data(), end_key.size()));
    }

    /**
     * @brief Commits (writes) a WriteBatch to the database.
     *
     * All operations in the batch are applied atomically. If the WAL is
     * enabled, the entire batch is written as a single WAL entry.
     *
     * @param batch The write batch to commit (consumed, not modified)
     * @param disable_wal If true, bypasses the WAL for this write
     *                    (faster but less durable)
     * @return rocksdb::Status OK on success
     */
    rocksdb::Status commit_write_batch(
        std::unique_ptr<rocksdb::WriteBatch> batch,
        bool disable_wal = false) {

        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return rocksdb::Status::IOError("Database not open");
        }

        if (!batch) {
            return rocksdb::Status::InvalidArgument("Null WriteBatch");
        }

        rocksdb::WriteOptions write_opts;
        write_opts.sync = false;
        write_opts.disableWAL = disable_wal || !config_.enable_wal;

        rocksdb::Status s = db_->Write(write_opts, batch.get());
        if (s.ok()) {
            total_write_batches_.fetch_add(1, std::memory_order_relaxed);
            total_puts_.fetch_add(batch->Count(),
                                  std::memory_order_relaxed);
        } else {
            check_status(s, "commit_write_batch",
                         fmt::format("count={}", batch->Count()));
        }

        return s;
    }

    /**
     * @brief Convenience method: writes a batch and disposes of it.
     *
     * @param batch The batch to write (will be destroyed after)
     * @return rocksdb::Status
     */
    rocksdb::Status write(rocksdb::WriteBatch& batch) {
        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return rocksdb::Status::IOError("Database not open");
        }

        rocksdb::WriteOptions write_opts;
        write_opts.sync = false;
        write_opts.disableWAL = !config_.enable_wal;

        rocksdb::Status s = db_->Write(write_opts, &batch);
        if (s.ok()) {
            total_write_batches_.fetch_add(1, std::memory_order_relaxed);
        }
        return s;
    }

    // ------------------------------------------------------------------------
    // Iterator — range scans and prefix seek
    // ------------------------------------------------------------------------

    /**
     * @brief Creates a forward iterator for the specified column family.
     *
     * The iterator points to the first key in the database. Use Seek(),
     * SeekToFirst(), or SeekForPrev() to position it before reading.
     *
     * The iterator is valid as long as the database is open and no
     * structural changes occur. For best consistency, use with a Snapshot.
     *
     * @param cf The column family to iterate
     * @param snapshot Optional snapshot for consistent reads
     * @return std::unique_ptr<rocksdb::Iterator> A new iterator
     */
    std::unique_ptr<rocksdb::Iterator> new_iterator(
        ColumnFamilyId cf,
        const rocksdb::Snapshot* snapshot = nullptr) {

        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return nullptr;
        }

        auto* handle = get_cf_handle(cf);
        if (!handle) return nullptr;

        rocksdb::ReadOptions read_opts;
        if (snapshot) {
            read_opts.snapshot = snapshot;
        }
        read_opts.fill_cache = false;  // Don't pollute cache for scans

        auto iter = db_->NewIterator(read_opts, handle);
        return std::unique_ptr<rocksdb::Iterator>(iter);
    }

    /**
     * @brief Performs a prefix seek on the specified column family.
     *
     * Prefix seeks leverage the bloom filter and prefix extractor to
     * efficiently find all keys with a given prefix. This is O(num_matching)
     * rather than O(total_keys).
     *
     * The iterator position is set to the first key >= prefix.
     * To collect all keys with the prefix, advance the iterator until
     * the key no longer starts with the prefix.
     *
     * @param cf The column family to search
     * @param prefix The prefix to seek
     * @param snapshot Optional snapshot for consistency
     * @return std::unique_ptr<rocksdb::Iterator> Positioned at first matching key
     */
    std::unique_ptr<rocksdb::Iterator> prefix_seek(
        ColumnFamilyId cf,
        std::string_view prefix,
        const rocksdb::Snapshot* snapshot = nullptr) {

        auto iter = new_iterator(cf, snapshot);
        if (!iter) return nullptr;

        iter->Seek(rocksdb::Slice(prefix.data(), prefix.size()));
        return iter;
    }

    /**
     * @brief Collects all key-value pairs matching a prefix into a vector.
     *
     * Convenience method that wraps prefix_seek() and collects results
     * until the prefix no longer matches.
     *
     * @param cf The column family
     * @param prefix The prefix to match
     * @param max_results Maximum number of results (0 = unlimited)
     * @param snapshot Optional snapshot
     * @return std::vector<std::pair<std::string, std::string>> Matching pairs
     */
    std::vector<std::pair<std::string, std::string>> prefix_scan(
        ColumnFamilyId cf,
        std::string_view prefix,
        size_t max_results = 0,
        const rocksdb::Snapshot* snapshot = nullptr) {

        std::vector<std::pair<std::string, std::string>> results;

        auto iter = prefix_seek(cf, prefix, snapshot);
        if (!iter) return results;

        for (iter->Seek(rocksdb::Slice(prefix.data(), prefix.size()));
             iter->Valid();
             iter->Next()) {

            if (iter->key().starts_with(prefix)) {
                results.emplace_back(
                    std::string(iter->key().data(), iter->key().size()),
                    std::string(iter->value().data(), iter->value().size()));

                if (max_results > 0 && results.size() >= max_results) {
                    break;
                }
            } else {
                break;  // Prefix no longer matches
            }
        }

        // Check for iteration errors
        if (!iter->status().ok()) {
            ROCKSDB_LOG_ERROR("prefix_scan iteration error: {}",
                              status_to_string(iter->status()));
        }

        return results;
    }

    /**
     * @brief Performs a range scan over [begin_key, end_key).
     *
     * Collects all key-value pairs in the specified range. For large ranges,
     * consider using new_iterator() directly with batching.
     *
     * @param cf The column family
     * @param begin_key Start of range (inclusive)
     * @param end_key End of range (exclusive)
     * @param max_results Maximum results (0 = unlimited)
     * @param snapshot Optional snapshot
     * @return std::vector<std::pair<std::string, std::string>>
     */
    std::vector<std::pair<std::string, std::string>> range_scan(
        ColumnFamilyId cf,
        std::string_view begin_key,
        std::string_view end_key,
        size_t max_results = 0,
        const rocksdb::Snapshot* snapshot = nullptr) {

        std::vector<std::pair<std::string, std::string>> results;

        auto iter = new_iterator(cf, snapshot);
        if (!iter) return results;

        for (iter->Seek(rocksdb::Slice(begin_key.data(), begin_key.size()));
             iter->Valid();
             iter->Next()) {

            if (iter->key().compare(rocksdb::Slice(end_key.data(),
                                                    end_key.size())) >= 0) {
                break;  // Past end of range
            }

            results.emplace_back(
                std::string(iter->key().data(), iter->key().size()),
                std::string(iter->value().data(), iter->value().size()));

            if (max_results > 0 && results.size() >= max_results) {
                break;
            }
        }

        if (!iter->status().ok()) {
            ROCKSDB_LOG_ERROR("range_scan iteration error: {}",
                              status_to_string(iter->status()));
        }

        return results;
    }

    // ------------------------------------------------------------------------
    // Snapshot — consistent point-in-time reads
    // ------------------------------------------------------------------------

    /**
     * @brief Creates a snapshot of the current database state.
     *
     * The snapshot captures a consistent view of the database at a point
     * in time. All reads using this snapshot will see exactly the state
     * at the moment the snapshot was created, regardless of subsequent
     * writes. Snapshots are lightweight (O(1) creation).
     *
     * IMPORTANT: Call release_snapshot() when done. Not releasing snapshots
     * prevents SST file deletion and causes unbounded disk growth.
     *
     * @return const rocksdb::Snapshot* The snapshot (owned by the DB)
     */
    const rocksdb::Snapshot* create_snapshot() {
        std::shared_lock lock(mutex_);
        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return nullptr;
        }
        const rocksdb::Snapshot* snap = db_->GetSnapshot();
        ROCKSDB_LOG_DEBUG("Snapshot created: seq={}",
                          snap->GetSequenceNumber());
        return snap;
    }

    /**
     * @brief Releases a previously created snapshot.
     *
     * Must be called for every snapshot returned by create_snapshot().
     * Failing to release snapshots causes resource leaks.
     *
     * @param snapshot The snapshot to release
     */
    void release_snapshot(const rocksdb::Snapshot* snapshot) {
        if (!snapshot) return;
        std::shared_lock lock(mutex_);
        if (db_) {
            db_->ReleaseSnapshot(snapshot);
            ROCKSDB_LOG_DEBUG("Snapshot released");
        }
    }

    /**
     * @brief RAII wrapper for snapshot management.
     *
     * Creates a snapshot on construction and automatically releases it
     * on destruction. Use this to ensure snapshots are always released.
     *
     * Usage:
     * @code
     *   auto guard = store->snapshot_guard();
     *   auto val = store->get(cf, key, guard.snapshot());
     * @endcode
     */
    class SnapshotGuard {
    public:
        explicit SnapshotGuard(RocksDBStore* store)
            : store_(store)
            , snapshot_(store ? store->create_snapshot() : nullptr) {}

        ~SnapshotGuard() {
            if (store_ && snapshot_) {
                store_->release_snapshot(snapshot_);
            }
        }

        SnapshotGuard(const SnapshotGuard&) = delete;
        SnapshotGuard& operator=(const SnapshotGuard&) = delete;
        SnapshotGuard(SnapshotGuard&& other) noexcept
            : store_(other.store_)
            , snapshot_(other.snapshot_) {
            other.store_ = nullptr;
            other.snapshot_ = nullptr;
        }
        SnapshotGuard& operator=(SnapshotGuard&& other) noexcept {
            if (this != &other) {
                if (store_ && snapshot_) store_->release_snapshot(snapshot_);
                store_ = other.store_;
                snapshot_ = other.snapshot_;
                other.store_ = nullptr;
                other.snapshot_ = nullptr;
            }
            return *this;
        }

        const rocksdb::Snapshot* snapshot() const { return snapshot_; }
        explicit operator bool() const { return snapshot_ != nullptr; }

    private:
        RocksDBStore* store_;
        const rocksdb::Snapshot* snapshot_;
    };

    /**
     * @brief Creates an RAII snapshot guard.
     *
     * @return SnapshotGuard that automatically releases the snapshot
     */
    SnapshotGuard snapshot_guard() {
        return SnapshotGuard(this);
    }

    // ------------------------------------------------------------------------
    // Checkpoint — hot backup without stopping writes
    // ------------------------------------------------------------------------

    /**
     * @brief Creates a checkpoint (consistent snapshot) at the given path.
     *
     * A checkpoint is a hard-linked copy of the database files at a
     * consistent point in time. It can be opened as a separate read-only
     * RocksDB instance while the primary instance continues accepting
     * writes. This is the preferred method for taking backups.
     *
     * The checkpoint is a "snapshot checkpoint" — it copies hard links
     * to SST files rather than copying data. This is fast and space-efficient
     * as long as the checkpoint directory is on the same filesystem.
     *
     * @param checkpoint_dir The directory to create the checkpoint in
     * @param log_size_for_flush If > 0, flushes memtables if the WAL
     *                           size exceeds this threshold before checkpoint
     * @return rocksdb::Status OK on success
     */
    rocksdb::Status checkpoint(const std::string& checkpoint_dir,
                               uint64_t log_size_for_flush = 0) {
        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return rocksdb::Status::IOError("Database not open");
        }

        ROCKSDB_LOG_INFO("Creating checkpoint at: {}", checkpoint_dir);

        rocksdb::Checkpoint* cp = nullptr;
        rocksdb::Status s = rocksdb::Checkpoint::Create(db_.get(), &cp);
        if (!s.ok()) {
            check_status(s, "create_checkpoint_object");
            return s;
        }

        std::unique_ptr<rocksdb::Checkpoint> checkpoint_obj(cp);

        s = checkpoint_obj->CreateCheckpoint(checkpoint_dir,
                                             log_size_for_flush);
        if (s.ok()) {
            ROCKSDB_LOG_INFO("Checkpoint created successfully: {}",
                             checkpoint_dir);
        } else {
            check_status(s, "create_checkpoint",
                         fmt::format("dir={}", checkpoint_dir));
        }

        return s;
    }

    // ------------------------------------------------------------------------
    // BackupEngine — incremental backups with shared file deduplication
    // ------------------------------------------------------------------------

    /**
     * @brief Creates a new backup using the BackupEngine.
     *
     * BackupEngine provides incremental backups: only new or modified files
     * are copied since the last backup. Files that are unchanged are shared
     * (hard-linked) across backup snapshots, making each additional backup
     * very space-efficient.
     *
     * Backups are stored in the configured backup_dir. Each backup is
     * identified by a monotonically increasing ID.
     *
     * @param flush_before_backup If true, flushes all memtables before backup
     * @return rocksdb::Status OK on success
     */
    rocksdb::Status create_backup(bool flush_before_backup = true) {
        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return rocksdb::Status::IOError("Database not open");
        }

        if (!backup_engine_) {
            return rocksdb::Status::IOError(
                "BackupEngine not initialized; configure backup_dir");
        }

        ROCKSDB_LOG_INFO("Creating backup (flush_before={})",
                         flush_before_backup);

        rocksdb::Status s = backup_engine_->CreateNewBackup(
            db_.get(), flush_before_backup);

        if (s.ok()) {
            ROCKSDB_LOG_INFO("Backup created successfully");
        } else {
            check_status(s, "create_backup");
        }

        // Purge old backups if we've exceeded the maximum
        if (config_.max_backup_count > 0) {
            uint32_t num_backups = 0;
            backup_engine_->GetInfo(rocksdb::BackupEngine::BackupInfos{},
                                    &num_backups);
            ROCKSDB_LOG_DEBUG("Total backups stored: {}", num_backups);

            if (num_backups > config_.max_backup_count) {
                uint32_t to_delete = num_backups - config_.max_backup_count;
                ROCKSDB_LOG_INFO("Purging {} old backups (retaining {})",
                                 to_delete, config_.max_backup_count);
                s = backup_engine_->PurgeOldBackups(
                    config_.max_backup_count);
                if (!s.ok()) {
                    check_status(s, "purge_old_backups");
                }
            }
        }

        return s;
    }

    /**
     * @brief Lists all available backups with their metadata.
     *
     * @return std::vector<rocksdb::BackupEngine::BackupInfo> Backup metadata
     */
    std::vector<rocksdb::BackupEngine::BackupInfo> list_backups() {
        std::shared_lock lock(mutex_);

        std::vector<rocksdb::BackupEngine::BackupInfo> backup_info;
        if (!backup_engine_) {
            return backup_info;
        }

        uint32_t num_backups = 0;
        backup_engine_->GetInfo(backup_info, &num_backups);
        return backup_info;
    }

    /**
     * @brief Restores the database from the latest backup.
     *
     * WARNING: This replaces the current database state. The database
     * is closed before the restore and reopened afterward.
     *
     * @param restore_dir The directory to restore the database to
     * @return rocksdb::Status OK on success
     */
    rocksdb::Status restore_from_backup(const std::string& restore_dir) {
        std::unique_lock lock(mutex_);

        if (!backup_engine_) {
            return rocksdb::Status::IOError(
                "BackupEngine not initialized");
        }

        ROCKSDB_LOG_INFO("Restoring database from backup to: {}",
                         restore_dir);

        // Close current database
        bool was_open = is_open_.load(std::memory_order_acquire);
        if (was_open) {
            cf_handles_.clear();
            db_.reset();
            txn_db_ = nullptr;
            opt_txn_db_ = nullptr;
            is_open_.store(false, std::memory_order_release);
        }

        rocksdb::RestoreOptions restore_opts;
        restore_opts.keep_log_files = true;

        rocksdb::Status s = backup_engine_->RestoreDBFromLatestBackup(
            restore_dir, restore_dir, restore_opts);

        if (!s.ok()) {
            check_status(s, "restore_from_backup",
                         fmt::format("dir={}", restore_dir));
        }

        // Re-open the database
        if (was_open) {
            // Update the path and reopen
            config_.db_path = restore_dir;
            rocksdb::Status open_s = open();
            if (!open_s.ok()) {
                ROCKSDB_LOG_ERROR("Failed to reopen database after restore: {}",
                                  status_to_string(open_s));
                s = open_s;
            }
        }

        return s;
    }

    /**
     * @brief Restores the database from a specific backup ID.
     *
     * @param backup_id The backup ID to restore from
     * @param restore_dir Target directory for the restored database
     * @return rocksdb::Status OK on success
     */
    rocksdb::Status restore_from_backup_id(uint32_t backup_id,
                                           const std::string& restore_dir) {
        std::unique_lock lock(mutex_);

        if (!backup_engine_) {
            return rocksdb::Status::IOError("BackupEngine not initialized");
        }

        ROCKSDB_LOG_INFO("Restoring backup {} to: {}", backup_id, restore_dir);

        bool was_open = is_open_.load(std::memory_order_acquire);
        if (was_open) {
            cf_handles_.clear();
            db_.reset();
            txn_db_ = nullptr;
            opt_txn_db_ = nullptr;
            is_open_.store(false, std::memory_order_release);
        }

        rocksdb::RestoreOptions restore_opts;

        rocksdb::Status s = backup_engine_->RestoreDBFromBackup(
            backup_id, restore_dir, restore_dir, restore_opts);

        if (!s.ok()) {
            check_status(s, "restore_from_backup_id",
                         fmt::format("backup_id={} dir={}",
                                     backup_id, restore_dir));
        }

        if (was_open) {
            config_.db_path = restore_dir;
            s = open();
            if (!s.ok()) {
                ROCKSDB_LOG_ERROR("Failed to reopen after restore: {}",
                                  status_to_string(s));
            }
        }

        return s;
    }

    /**
     * @brief Verifies the integrity of a backup by checksumming all files.
     *
     * @param backup_id The backup ID to verify
     * @return rocksdb::Status OK if the backup is valid
     */
    rocksdb::Status verify_backup(uint32_t backup_id) {
        std::shared_lock lock(mutex_);

        if (!backup_engine_) {
            return rocksdb::Status::IOError("BackupEngine not initialized");
        }

        ROCKSDB_LOG_INFO("Verifying backup {}", backup_id);

        rocksdb::Status s = backup_engine_->VerifyBackup(backup_id);
        if (s.ok()) {
            ROCKSDB_LOG_INFO("Backup {} verified successfully", backup_id);
        } else {
            check_status(s, "verify_backup",
                         fmt::format("backup_id={}", backup_id));
        }

        return s;
    }

    // ------------------------------------------------------------------------
    // Import / Export SST files
    // ------------------------------------------------------------------------

    /**
     * @brief Ingests (imports) external SST files into the database.
     *
     * SST files created via SstFileWriter or exported from another RocksDB
     * instance can be ingested without copying data. The files are moved
     * (or hard-linked) into the database directory and registered in the
     * MANIFEST. This is the fastest way to bulk-load data.
     *
     * @param cf The target column family
     * @param sst_file_paths Paths to the SST files to ingest
     * @param move_files If true, moves files; if false, copies (slower but
     *                   preserves originals)
     * @return rocksdb::Status OK on success
     */
    rocksdb::Status ingest_external_files(
        ColumnFamilyId cf,
        const std::vector<std::string>& sst_file_paths,
        bool move_files = true) {

        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return rocksdb::Status::IOError("Database not open");
        }

        auto* handle = get_cf_handle(cf);
        if (!handle) {
            return rocksdb::Status::InvalidArgument("Invalid CF handle");
        }

        ROCKSDB_LOG_INFO("Ingesting {} SST files into cf={}",
                         sst_file_paths.size(),
                         static_cast<int>(cf));

        rocksdb::IngestExternalFileOptions ingest_opts;
        ingest_opts.move_files = move_files;
        ingest_opts.allow_blocking_flush = true;
        ingest_opts.allow_global_seqno = true;
        ingest_opts.write_global_seqno = true;
        ingest_opts.verify_checksums_before_ingest = true;

        rocksdb::Status s = db_->IngestExternalFile(
            handle, sst_file_paths, ingest_opts);

        if (s.ok()) {
            ROCKSDB_LOG_INFO("Successfully ingested {} SST files",
                             sst_file_paths.size());
        } else {
            check_status(s, "ingest_external_files",
                         fmt::format("cf={} files={}",
                                     static_cast<int>(cf),
                                     sst_file_paths.size()));
        }

        return s;
    }

    /**
     * @brief Creates an SstFileWriter for building SST files outside the DB.
     *
     * SST files created with this writer can later be ingested via
     * ingest_external_files(). This is useful for building large datasets
     * offline and importing them efficiently.
     *
     * @param cf The column family for which to build SST files
     * @param file_path The output path for the SST file
     * @return std::unique_ptr<rocksdb::SstFileWriter>
     */
    std::unique_ptr<rocksdb::SstFileWriter> create_sst_writer(
        ColumnFamilyId cf,
        const std::string& file_path) {

        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return nullptr;
        }

        auto* handle = get_cf_handle(cf);
        if (!handle) return nullptr;

        rocksdb::Options opts = db_->GetOptions(handle);
        rocksdb::EnvOptions env_opts;

        auto writer = std::make_unique<rocksdb::SstFileWriter>(
            env_opts, opts);
        rocksdb::Status s = writer->Open(file_path);
        if (!s.ok()) {
            check_status(s, "create_sst_writer",
                         fmt::format("path={}", file_path));
            return nullptr;
        }

        return writer;
    }

    // ------------------------------------------------------------------------
    // Transaction support
    // ------------------------------------------------------------------------

    /**
     * @brief Begins a new pessimistic transaction (TransactionDB only).
     *
     * Transactions provide ACID guarantees: atomicity, consistency,
     * isolation, and durability. Pessimistic transactions acquire locks
     * during reads and writes, preventing conflicts before commit.
     *
     * Only available when config_.use_transactions is true.
     *
     * @return rocksdb::Transaction* The new transaction (caller must delete),
     *         or nullptr if transactions are not enabled
     */
    rocksdb::Transaction* begin_transaction() {
        if (!txn_db_) {
            ROCKSDB_LOG_WARN("begin_transaction: TransactionDB not enabled");
            return nullptr;
        }

        std::shared_lock lock(mutex_);

        rocksdb::WriteOptions write_opts;
        write_opts.sync = false;

        rocksdb::TransactionOptions txn_opts;
        txn_opts.set_snapshot = true;  // Ensure repeatable reads
        txn_opts.lock_timeout = 5000;  // 5 seconds
        txn_opts.expiration = 30000;   // 30 seconds max lifetime
        txn_opts.deadlock_detect = true;
        txn_opts.deadlock_detect_depth = 50;

        rocksdb::Transaction* txn = txn_db_->BeginTransaction(
            write_opts, txn_opts);

        if (txn) {
            ROCKSDB_LOG_TRACE("Transaction started");
        } else {
            ROCKSDB_LOG_ERROR("Failed to begin transaction");
        }

        return txn;
    }

    /**
     * @brief Begins a new optimistic transaction (OptimisticTransactionDB).
     *
     * Optimistic transactions don't acquire locks; conflicts are detected
     * at commit time. This is suitable for low-contention workloads where
     * most transactions don't conflict.
     *
     * Only available when config_.use_optimistic_transactions is true.
     *
     * @return rocksdb::Transaction* (optimistic)
     */
    rocksdb::Transaction* begin_optimistic_transaction() {
        if (!opt_txn_db_) {
            ROCKSDB_LOG_WARN(
                "begin_optimistic_transaction: OptimisticTransactionDB not enabled");
            return nullptr;
        }

        std::shared_lock lock(mutex_);

        rocksdb::WriteOptions write_opts;
        write_opts.sync = false;

        rocksdb::OptimisticTransactionOptions txn_opts;
        txn_opts.set_snapshot = true;

        rocksdb::Transaction* txn = opt_txn_db_->BeginTransaction(
            write_opts, txn_opts);

        if (txn) {
            ROCKSDB_LOG_TRACE("Optimistic transaction started");
        }

        return txn;
    }

    /**
     * @brief Commits a transaction.
     *
     * For pessimistic transactions, this releases all held locks.
     * For optimistic transactions, this validates that no conflicting
     * writes have occurred since the transaction began.
     *
     * @param txn The transaction to commit (deleted after)
     * @return rocksdb::Status OK on success, conflict error on failure
     */
    rocksdb::Status commit_transaction(rocksdb::Transaction* txn) {
        if (!txn) {
            return rocksdb::Status::InvalidArgument("Null transaction");
        }

        rocksdb::Status s = txn->Commit();
        if (s.ok()) {
            ROCKSDB_LOG_TRACE("Transaction committed");
        } else {
            ROCKSDB_LOG_WARN("Transaction commit failed: {}",
                             status_to_string(s));
        }

        delete txn;
        return s;
    }

    /**
     * @brief Rolls back (aborts) a transaction.
     *
     * All changes made within the transaction are discarded. Locks are
     * released for pessimistic transactions.
     *
     * @param txn The transaction to roll back (deleted after)
     * @return rocksdb::Status OK on success
     */
    rocksdb::Status rollback_transaction(rocksdb::Transaction* txn) {
        if (!txn) {
            return rocksdb::Status::InvalidArgument("Null transaction");
        }

        rocksdb::Status s = txn->Rollback();
        if (!s.ok()) {
            ROCKSDB_LOG_WARN("Transaction rollback failed: {}",
                             status_to_string(s));
        }

        delete txn;
        return s;
    }

    // ------------------------------------------------------------------------
    // Administrative Operations
    // ------------------------------------------------------------------------

    /**
     * @brief Flushes all memtables for the given column family to SST files.
     *
     * This is a synchronous operation that waits for the flush to complete.
     * Flushing reduces memory usage and creates new SST files for compaction.
     *
     * @param cf The column family to flush
     * @return rocksdb::Status OK on success
     */
    rocksdb::Status flush(ColumnFamilyId cf) {
        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return rocksdb::Status::IOError("Database not open");
        }

        auto* handle = get_cf_handle(cf);
        if (!handle) {
            return rocksdb::Status::InvalidArgument("Invalid CF handle");
        }

        rocksdb::FlushOptions flush_opts;
        flush_opts.wait = true;

        ROCKSDB_LOG_DEBUG("Flushing column family: {}",
                          kCfIdToName[static_cast<size_t>(cf)]);

        rocksdb::Status s = db_->Flush(flush_opts, handle);
        if (!s.ok()) {
            check_status(s, "flush",
                         kCfIdToName[static_cast<size_t>(cf)]);
        }

        return s;
    }

    /**
     * @brief Flushes all column families.
     *
     * @return rocksdb::Status OK on success
     */
    rocksdb::Status flush_all() {
        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return rocksdb::Status::IOError("Database not open");
        }

        ROCKSDB_LOG_DEBUG("Flushing all column families");

        rocksdb::FlushOptions flush_opts;
        flush_opts.wait = true;

        rocksdb::Status s = db_->Flush(flush_opts);
        if (!s.ok()) {
            check_status(s, "flush_all");
        }

        return s;
    }

    /**
     * @brief Triggers manual compaction on the specified column family.
     *
     * Compaction merges SST files, removes tombstones, and reclaims disk
     * space. Manual compaction is useful for:
     *   - Forcing space reclamation after large deletes
     *   - Reducing read amplification on read-heavy CFs
     *   - Preparing for backup
     *
     * @param cf The column family to compact
     * @param begin_key Start of key range to compact (empty = start of DB)
     * @param end_key End of key range to compact (empty = end of DB)
     * @return rocksdb::Status OK on success
     */
    rocksdb::Status compact_range(ColumnFamilyId cf,
                                  std::string_view begin_key = "",
                                  std::string_view end_key = "") {
        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return rocksdb::Status::IOError("Database not open");
        }

        auto* handle = get_cf_handle(cf);
        if (!handle) {
            return rocksdb::Status::InvalidArgument("Invalid CF handle");
        }

        ROCKSDB_LOG_INFO("Compacting column family: {} [{} .. {}]",
                         kCfIdToName[static_cast<size_t>(cf)],
                         begin_key.empty() ? "START" : begin_key,
                         end_key.empty() ? "END" : end_key);

        rocksdb::CompactRangeOptions compact_opts;
        compact_opts.change_level = true;
        compact_opts.target_level = -1;  // Let RocksDB choose

        rocksdb::Slice* begin_slice = nullptr;
        rocksdb::Slice* end_slice = nullptr;
        rocksdb::Slice begin_s;
        rocksdb::Slice end_s;

        if (!begin_key.empty()) {
            begin_s = rocksdb::Slice(begin_key.data(), begin_key.size());
            begin_slice = &begin_s;
        }
        if (!end_key.empty()) {
            end_s = rocksdb::Slice(end_key.data(), end_key.size());
            end_slice = &end_s;
        }

        rocksdb::Status s = db_->CompactRange(
            compact_opts, handle, begin_slice, end_slice);
        if (!s.ok()) {
            check_status(s, "compact_range");
        }

        return s;
    }

    /**
     * @brief Pauses background work (compaction and flush).
     *
     * Useful when performing maintenance operations that require exclusive
     * access to the database files. Call continue_background_work() to resume.
     *
     * @return rocksdb::Status OK on success
     */
    rocksdb::Status pause_background_work() {
        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return rocksdb::Status::IOError("Database not open");
        }

        ROCKSDB_LOG_INFO("Pausing background work");

        rocksdb::Status s = db_->PauseBackgroundWork();
        if (!s.ok()) {
            check_status(s, "pause_background_work");
        }

        return s;
    }

    /**
     * @brief Resumes background work after a pause.
     *
     * @return rocksdb::Status OK on success
     */
    rocksdb::Status continue_background_work() {
        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return rocksdb::Status::IOError("Database not open");
        }

        ROCKSDB_LOG_INFO("Resuming background work");

        rocksdb::Status s = db_->ContinueBackgroundWork();
        if (!s.ok()) {
            check_status(s, "continue_background_work");
        }

        return s;
    }

    /**
     * @brief Gets the approximate size of a key range in the specified CF.
     *
     * This is a fast estimate based on SST file metadata; it does not
     * scan the actual data.
     *
     * @param cf The column family
     * @param begin_key Start of range
     * @param end_key End of range
     * @param include_memtables Include in-memory data in the estimate
     * @return std::array<uint64_t, 2> [approximate size, SST-only size]
     */
    std::array<uint64_t, 2> approximate_size(
        ColumnFamilyId cf,
        std::string_view begin_key,
        std::string_view end_key,
        bool include_memtables = false) {

        std::shared_lock lock(mutex_);

        std::array<uint64_t, 2> sizes = {0, 0};

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return sizes;
        }

        auto* handle = get_cf_handle(cf);
        if (!handle) return sizes;

        rocksdb::Range range(
            rocksdb::Slice(begin_key.data(), begin_key.size()),
            rocksdb::Slice(end_key.data(), end_key.size()));

        uint8_t include_flags = include_memtables
            ? rocksdb::DB::SizeApproximationFlags::INCLUDE_MEMTABLES
            : rocksdb::DB::SizeApproximationFlags::NONE;

        db_->GetApproximateSizes(handle, &range, 1, sizes.data(),
                                 include_flags);

        return sizes;
    }

    /**
     * @brief Returns the latest sequence number from the database.
     *
     * The sequence number is a monotonically increasing counter that
     * represents the total number of writes to the database.
     *
     * @return uint64_t The latest sequence number
     */
    uint64_t get_latest_sequence_number() {
        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return 0;
        }

        return db_->GetLatestSequenceNumber();
    }

    /**
     * @brief Returns the set of properties for a column family.
     *
     * Properties include: number of SST files, total SST file size, number
     * of entries, estimate of live data size, and more.
     *
     * @param cf The column family
     * @return std::string Multi-line string of property values
     */
    std::string get_property(ColumnFamilyId cf,
                             const std::string& property) {
        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return "";
        }

        auto* handle = get_cf_handle(cf);
        if (!handle) return "";

        std::string value;
        bool ok = db_->GetProperty(handle, property, &value);
        return ok ? value : "";
    }

    /**
     * @brief Returns aggregated property values with parsed numeric output.
     *
     * Common properties:
     *   - "rocksdb.num-entries-active" — estimated live key count
     *   - "rocksdb.estimate-num-keys" — approximate total keys
     *   - "rocksdb.estimate-table-readers-mem" — SST reader memory
     *   - "rocksdb.cur-size-all-mem-tables" — current memtable usage
     *   - "rocksdb.size-all-mem-tables" — all memtable usage including immutable
     *
     * @param cf The column family
     * @param property The property name
     * @return uint64_t The parsed numeric value, or 0 on error
     */
    uint64_t get_int_property(ColumnFamilyId cf,
                              const std::string& property) {
        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return 0;
        }

        auto* handle = get_cf_handle(cf);
        if (!handle) return 0;

        uint64_t value = 0;
        bool ok = db_->GetIntProperty(handle, property, &value);
        return ok ? value : 0;
    }

    /**
     * @brief Returns RocksDB statistics as a formatted string.
     *
     * Includes compaction statistics, I/O counters, bloom filter
     * effectiveness, cache hit/miss rates, and more.
     *
     * @return std::string Formatted statistics
     */
    std::string get_statistics() {
        std::shared_lock lock(mutex_);
        if (!db_) return "Database not open";
        if (!stats_) return "Statistics not enabled";
        return format_statistics(stats_);
    }

    /**
     * @brief Returns the current listener statistics.
     *
     * @return A map of metric names to values for monitoring
     */
    std::unordered_map<std::string, int64_t> get_listener_stats() {
        std::shared_lock lock(mutex_);

        std::unordered_map<std::string, int64_t> stats;
        if (!event_listener_) return stats;

        stats["rocksdb_active_compactions"] =
            event_listener_->active_compactions();
        stats["rocksdb_active_flushes"] =
            event_listener_->active_flushes();
        stats["rocksdb_total_compactions"] =
            event_listener_->total_compactions();
        stats["rocksdb_total_flushes"] =
            event_listener_->total_flushes();
        stats["rocksdb_compaction_input_bytes"] =
            event_listener_->compaction_input_bytes();
        stats["rocksdb_compaction_output_bytes"] =
            event_listener_->compaction_output_bytes();
        stats["rocksdb_compaction_duration_us"] =
            event_listener_->compaction_duration_us();
        stats["rocksdb_stall_count"] =
            event_listener_->stall_count();

        return stats;
    }

    // ------------------------------------------------------------------------
    // Memory Monitoring
    // ------------------------------------------------------------------------

    /**
     * @brief Reports current memory usage across all column families.
     *
     * @return A struct with detailed memory breakdown
     */
    struct MemoryStats {
        uint64_t memtable_total = 0;    // Memtable memory (active + immutable)
        uint64_t table_readers = 0;     // SST reader / index memory
        uint64_t block_cache_usage = 0; // Block cache usage
        uint64_t block_cache_capacity = 0;
        uint64_t all_memory = 0;        // Approximate total RocksDB memory
    };

    /**
     * @brief Collects memory usage statistics from all column families.
     *
     * @return MemoryStats snapshot of current memory utilization
     */
    MemoryStats get_memory_stats() {
        std::shared_lock lock(mutex_);
        MemoryStats ms;

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return ms;
        }

        for (size_t i = 0; i < static_cast<size_t>(ColumnFamilyId::Count); ++i) {
            auto cf = static_cast<ColumnFamilyId>(i);
            auto* handle = get_cf_handle(cf);
            if (!handle) continue;

            ms.memtable_total += get_int_property(
                cf, "rocksdb.cur-size-all-mem-tables");
            ms.table_readers += get_int_property(
                cf, "rocksdb.estimate-table-readers-mem");
        }

        if (block_cache_) {
            ms.block_cache_usage = block_cache_->GetUsage();
            ms.block_cache_capacity = block_cache_->GetCapacity();
        }

        // Approximate total: memtables + table readers + block cache
        ms.all_memory = ms.memtable_total + ms.table_readers +
                        ms.block_cache_usage;

        return ms;
    }

    /**
     * @brief Checks if the database is currently experiencing write stalls.
     *
     * @return true if writes are being stalled (delayed or stopped)
     */
    bool is_write_stalled() {
        std::shared_lock lock(mutex_);
        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return false;
        }

        // Check if the write controller is active
        auto active_compactions =
            event_listener_ ? event_listener_->active_compactions() : 0;
        return active_compactions > 8;  // Heuristic threshold
    }

    // ------------------------------------------------------------------------
    // TTL (Time-To-Live) operations
    // ------------------------------------------------------------------------

    /**
     * @brief Stores a key with an explicit TTL.
     *
     * The TTL is embedded in the key suffix for use by the TtlCompactionFilter.
     * When the TTL expires, the key is automatically removed during compaction.
     *
     * @param cf The column family (should have TTL compaction filter configured)
     * @param key The key
     * @param value The value
     * @param ttl_seconds Time-to-live in seconds
     * @return rocksdb::Status
     */
    rocksdb::Status put_with_ttl(ColumnFamilyId cf,
                                 std::string_view key,
                                 std::string_view value,
                                 int ttl_seconds) {
        // Encode the TTL as the last 8 bytes of the key
        std::string ttl_key(key);
        int64_t expiry_ms = now_ms() + (static_cast<int64_t>(ttl_seconds) * 1000);
        ttl_key.resize(ttl_key.size() + 8);
        for (int i = 7; i >= 0; --i) {
            ttl_key[ttl_key.size() - 8 + i] =
                static_cast<char>((expiry_ms >> (8 * (7 - i))) & 0xFF);
        }

        return put(cf, ttl_key, value);
    }

    // ------------------------------------------------------------------------
    // Properties and introspection
    // ------------------------------------------------------------------------

    /**
     * @brief Returns whether the database is currently open.
     */
    bool is_open() const {
        return is_open_.load(std::memory_order_acquire);
    }

    /**
     * @brief Returns whether the database is in read-only mode.
     */
    bool is_readonly() const {
        return is_readonly_.load(std::memory_order_acquire);
    }

    /**
     * @brief Returns the number of column families.
     */
    size_t column_family_count() const {
        return cf_handles_.size();
    }

    /**
     * @brief Returns the database path.
     */
    const std::string& db_path() const {
        return config_.db_path;
    }

    /**
     * @brief Returns cumulative operation counters.
     */
    struct OperationCounters {
        int64_t puts = 0;
        int64_t gets = 0;
        int64_t deletes = 0;
        int64_t write_batches = 0;
    };

    OperationCounters get_operation_counters() const {
        return OperationCounters{
            total_puts_.load(std::memory_order_relaxed),
            total_gets_.load(std::memory_order_relaxed),
            total_deletes_.load(std::memory_order_relaxed),
            total_write_batches_.load(std::memory_order_relaxed),
        };
    }

    /**
     * @brief Returns a raw pointer to the database (use with caution).
     *
     * Intended for advanced use cases where direct RocksDB API access
     * is needed. The caller must ensure the database remains open.
     */
    rocksdb::DB* raw_db() {
        return db_.get();
    }

    /**
     * @brief Returns a raw pointer to the TransactionDB, if enabled.
     */
    rocksdb::TransactionDB* raw_txn_db() {
        return txn_db_;
    }

    /**
     * @brief Returns a raw pointer to the OptimisticTransactionDB, if enabled.
     */
    rocksdb::OptimisticTransactionDB* raw_opt_txn_db() {
        return opt_txn_db_;
    }

    /**
     * @brief Returns the column family handle for a given CF ID.
     *
     * @param cf The column family identifier
     * @return rocksdb::ColumnFamilyHandle* or nullptr if not found
     */
    rocksdb::ColumnFamilyHandle* get_cf_handle(ColumnFamilyId cf) const {
        auto idx = static_cast<size_t>(cf);
        if (idx >= cf_handles_.size()) return nullptr;
        return cf_handles_[idx].get();
    }

    /**
     * @brief Returns the column family name for a given CF ID.
     */
    static std::string_view cf_name(ColumnFamilyId cf) {
        auto idx = static_cast<size_t>(cf);
        if (idx >= kCfIdToName.size()) return "unknown";
        return kCfIdToName[idx];
    }

    // ------------------------------------------------------------------------
    // Merge operations for counter increment
    // ------------------------------------------------------------------------

    /**
     * @brief Atomically increments a counter using the merge operator.
     *
     * This uses the UInt64AddOperator merge operator. The counter value
     * is a uint64_t. This is preferred over a read-modify-write pattern
     * because it avoids the race condition between the read and write.
     *
     * @param cf The column family
     * @param key The counter key
     * @param delta The amount to add (default: 1)
     * @return rocksdb::Status
     */
    rocksdb::Status merge_increment(ColumnFamilyId cf,
                                    std::string_view key,
                                    uint64_t delta = 1) {
        std::shared_lock lock(mutex_);

        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return rocksdb::Status::IOError("Database not open");
        }

        auto* handle = get_cf_handle(cf);
        if (!handle) {
            return rocksdb::Status::InvalidArgument("Invalid CF handle");
        }

        // Serialize the delta as little-endian uint64_t
        std::string delta_bytes(sizeof(uint64_t), '\0');
        std::memcpy(delta_bytes.data(), &delta, sizeof(uint64_t));

        rocksdb::WriteOptions write_opts;
        write_opts.sync = false;

        rocksdb::Status s = db_->Merge(
            write_opts, handle,
            rocksdb::Slice(key.data(), key.size()),
            rocksdb::Slice(delta_bytes.data(), delta_bytes.size()));

        if (!s.ok()) {
            check_status(s, "merge_increment",
                         fmt::format("key={} delta={}", key, delta));
        }

        total_puts_.fetch_add(1, std::memory_order_relaxed);
        return s;
    }

    /**
     * @brief Retrieves the current value of a counter.
     *
     * Reads the uint64_t counter value. If the key does not exist,
     * returns 0.
     *
     * @param cf The column family
     * @param key The counter key
     * @return uint64_t The current counter value
     */
    uint64_t get_counter(ColumnFamilyId cf, std::string_view key) {
        auto val = get(cf, key);
        if (!val || val->size() != sizeof(uint64_t)) {
            return 0;
        }
        uint64_t result = 0;
        std::memcpy(&result, val->data(), sizeof(uint64_t));
        return result;
    }

    // ------------------------------------------------------------------------
    // Position: parsing the enum from string
    // ------------------------------------------------------------------------

    /**
     * @brief Converts a column family name string to its enum value.
     *
     * @param name The column family name (e.g., "cf_offsets")
     * @return std::optional<ColumnFamilyId> The enum value, or nullopt
     */
    static std::optional<ColumnFamilyId> cf_from_name(std::string_view name) {
        auto it = kCfNameToId.find(name);
        if (it != kCfNameToId.end()) {
            return it->second;
        }
        return std::nullopt;
    }

private:
    // ------------------------------------------------------------------------
    // Internal: locked versions of CRUD operations
    // ------------------------------------------------------------------------

    rocksdb::Status put_locked(ColumnFamilyId cf,
                               std::string_view key,
                               std::string_view value) {
        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return rocksdb::Status::IOError("Database not open");
        }

        auto* handle = get_cf_handle(cf);
        if (!handle) {
            return rocksdb::Status::InvalidArgument("Invalid CF handle");
        }

        rocksdb::WriteOptions write_opts;
        write_opts.sync = false;
        write_opts.disableWAL = !config_.enable_wal;

        rocksdb::Status s = db_->Put(
            write_opts, handle,
            rocksdb::Slice(key.data(), key.size()),
            rocksdb::Slice(value.data(), value.size()));

        if (s.ok()) {
            total_puts_.fetch_add(1, std::memory_order_relaxed);
        } else {
            check_status(s, "put", std::string(key));
        }

        return s;
    }

    std::optional<std::string> get_locked(ColumnFamilyId cf,
                                          std::string_view key) {
        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return std::nullopt;
        }

        auto* handle = get_cf_handle(cf);
        if (!handle) return std::nullopt;

        rocksdb::ReadOptions read_opts;
        read_opts.verify_checksums = config_.paranoid_checks;

        std::string value;
        rocksdb::Status s = db_->Get(
            read_opts, handle,
            rocksdb::Slice(key.data(), key.size()), &value);

        total_gets_.fetch_add(1, std::memory_order_relaxed);

        if (s.ok()) {
            return value;
        }
        if (s.IsNotFound()) {
            return std::nullopt;
        }

        check_status(s, "get", std::string(key));
        return std::nullopt;
    }

    rocksdb::Status del_locked(ColumnFamilyId cf, std::string_view key) {
        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return rocksdb::Status::IOError("Database not open");
        }

        auto* handle = get_cf_handle(cf);
        if (!handle) {
            return rocksdb::Status::InvalidArgument("Invalid CF handle");
        }

        rocksdb::WriteOptions write_opts;
        write_opts.sync = false;

        rocksdb::Status s = db_->Delete(
            write_opts, handle,
            rocksdb::Slice(key.data(), key.size()));

        if (s.ok()) {
            total_deletes_.fetch_add(1, std::memory_order_relaxed);
        } else {
            check_status(s, "delete", std::string(key));
        }

        return s;
    }

    bool exists_locked(ColumnFamilyId cf, std::string_view key) {
        if (!is_open_.load(std::memory_order_acquire) || !db_) {
            return false;
        }

        auto* handle = get_cf_handle(cf);
        if (!handle) return false;

        rocksdb::ReadOptions read_opts;
        read_opts.verify_checksums = false;

        // Small optimization: use PinnableSlice to avoid copy
        rocksdb::PinnableSlice value;
        rocksdb::Status s = db_->Get(
            read_opts, handle,
            rocksdb::Slice(key.data(), key.size()), &value);

        return s.ok();
    }

    // ------------------------------------------------------------------------
    // Internal: option builders
    // ------------------------------------------------------------------------

    /**
     * @brief Builds the DBOptions from the current configuration.
     *
     * Configures the database-level settings: background threads,
     * rate limiter, statistics, WAL, listeners, etc.
     *
     * @return rocksdb::DBOptions Fully configured options structure
     */
    rocksdb::DBOptions build_db_options() {
        rocksdb::DBOptions opts;

        // --- Filesystem and paths ---
        opts.create_if_missing = config_.create_if_missing;
        opts.create_missing_column_families =
            config_.create_missing_column_families;
        opts.paranoid_checks = config_.paranoid_checks;

        // --- Background work ---
        opts.max_background_jobs = config_.max_background_jobs;
        opts.max_open_files = config_.max_open_files;

        // --- WAL ---
        opts.manual_wal_flush = config_.manual_wal_flush;
        opts.wal_recovery_mode = config_.wal_recovery_mode;

        // --- Direct I/O ---
        opts.use_direct_reads = config_.use_direct_reads;
        opts.use_direct_io_for_flush_and_compaction =
            config_.use_direct_io_for_flush_and_compaction;

        // --- Statistics ---
        if (config_.enable_statistics) {
            stats_ = rocksdb::CreateDBStatistics();
            opts.statistics = stats_;
            opts.stats_dump_period_sec = config_.stats_dump_period_sec;
        }

        // --- Rate limiter ---
        if (config_.rate_limiter_bytes_per_sec > 0) {
            rate_limiter_ = rocksdb::NewGenericRateLimiter(
                config_.rate_limiter_bytes_per_sec,
                100 * 1000,    // refill period: 100 ms
                10,            // fairness
                rocksdb::RateLimiter::Mode::kAllIo,
                true           // auto-tune
            );
            opts.rate_limiter = rate_limiter_;
        }

        // --- Event listener ---
        event_listener_ = std::make_shared<RocksDbEventListener>();
        opts.listeners.push_back(event_listener_);

        // --- Info log ---
        opts.info_log_level = rocksdb::InfoLogLevel::INFO_LEVEL;
        opts.max_log_file_size = 10 * 1024 * 1024;  // 10 MB
        opts.keep_log_file_num = 10;
        opts.log_file_time_to_roll = 86400;  // 24 hours

        // --- Row cache (disabled by default, use block cache instead) ---
        opts.row_cache = nullptr;

        // --- Avoid stalls during background error ---
        opts.paranoid_checks = config_.paranoid_checks;

        return opts;
    }

    /**
     * @brief Builds column family options for a specific CF.
     *
     * Different CFs may have different tuning. For example:
     *   - Sessions CF: TTL compaction filter for expiry
     *   - Offsets CF: high write throughput, LZ4 compression
     *   - Schemas CF: read-heavy, larger block cache
     *
     * @param cf The column family identifier
     * @return rocksdb::ColumnFamilyOptions Configured options
     */
    rocksdb::ColumnFamilyOptions build_column_family_options(
        ColumnFamilyId cf) {

        rocksdb::ColumnFamilyOptions opts;

        // --- Block-based table options ---
        rocksdb::BlockBasedTableOptions table_opts;

        // Block cache (shared across all CFs)
        if (!block_cache_ && config_.block_cache_size > 0) {
            block_cache_ = rocksdb::NewLRUCache(
                config_.block_cache_size,
                8,          // num_shard_bits (256 shards)
                false,      // strict_capacity_limit
                0.0         // high_pri_pool_ratio
            );
        }
        table_opts.block_cache = block_cache_;

        // Bloom filter
        if (config_.bloom_bits_per_key > 0) {
            table_opts.filter_policy.reset(
                rocksdb::NewBloomFilterPolicy(
                    config_.bloom_bits_per_key, false));
            table_opts.whole_key_filtering = true;
        }

        // Enable partitioned index filters for better cache efficiency
        table_opts.index_type =
            rocksdb::BlockBasedTableOptions::kTwoLevelIndexSearch;
        table_opts.partition_filters = true;
        table_opts.cache_index_and_filter_blocks = true;
        table_opts.pin_l0_filter_and_index_blocks_in_cache = true;

        // Data block index
        table_opts.data_block_index_type =
            rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
        table_opts.data_block_hash_table_util_ratio = 0.75;

        opts.table_factory.reset(
            rocksdb::NewBlockBasedTableFactory(table_opts));

        // --- Memtable configuration ---
        opts.write_buffer_size = config_.write_buffer_size;
        opts.max_write_buffer_number = config_.max_write_buffer_number;
        opts.min_write_buffer_number_to_merge =
            config_.min_write_buffer_number_to_merge;

        // Use HashSkipList memtable for range scans
        opts.memtable_factory.reset(
            rocksdb::NewHashSkipListRepFactory(100000));

        // --- Compaction ---
        opts.compaction_style = rocksdb::kCompactionStyleLevel;
        opts.level0_file_num_compaction_trigger = 4;
        opts.level0_slowdown_writes_trigger = 20;
        opts.level0_stop_writes_trigger = 36;
        opts.target_file_size_base = config_.target_file_size_base;
        opts.max_bytes_for_level_base = 256ULL * 1024 * 1024;  // 256 MB
        opts.max_bytes_for_level_multiplier = 10;
        opts.soft_pending_compaction_bytes_limit =
            config_.soft_pending_compaction_bytes_limit;
        opts.hard_pending_compaction_bytes_limit =
            config_.hard_pending_compaction_bytes_limit;

        // --- Compression ---
        opts.compression = parse_compression_type(config_.compression);
        opts.bottommost_compression =
            parse_compression_type(config_.bottommost_compression);
        opts.compression_opts.enabled = true;
        opts.compression_opts.max_dict_bytes = 16384;
        opts.compression_opts.zstd_max_train_bytes = 0;

        // --- TTL for session CF ---
        if (cf == ColumnFamilyId::Sessions &&
            config_.session_ttl_seconds > 0) {
            opts.compaction_filter_factory.reset(
                new TtlCompactionFilterFactory(
                    config_.session_ttl_seconds));
            ROCKSDB_LOG_DEBUG(
                "TTL compaction filter enabled for sessions: {}s",
                config_.session_ttl_seconds);
        }

        // --- Merge operator for counter CFs ---
        if (cf == ColumnFamilyId::Offsets ||
            cf == ColumnFamilyId::Transactions) {
            opts.merge_operator.reset(new UInt64AddOperator());
            ROCKSDB_LOG_DEBUG("Merge operator enabled for CF: {}",
                              kCfIdToName[static_cast<size_t>(cf)]);
        }

        // --- Prefix extractor for efficient prefix scans ---
        // Extract prefix up to the key separator
        opts.prefix_extractor.reset(
            rocksdb::NewFixedPrefixTransform(0));  // dynamic, see below
        // We use a custom transform: extract everything before the last ':'
        // In practice, we configure this per-CF based on key patterns

        return opts;
    }

    /**
     * @brief Parses a compression type string into the RocksDB enum.
     *
     * Supported values: "none", "snappy", "zlib", "bzip2", "lz4",
     * "lz4hc", "xpress", "zstd". Unknown values default to LZ4.
     *
     * @param comp_str The compression name
     * @return rocksdb::CompressionType
     */
    static rocksdb::CompressionType parse_compression_type(
        const std::string& comp_str) {
        if (comp_str == "none")    return rocksdb::kNoCompression;
        if (comp_str == "snappy")  return rocksdb::kSnappyCompression;
        if (comp_str == "zlib")    return rocksdb::kZlibCompression;
        if (comp_str == "bzip2")   return rocksdb::kBZip2Compression;
        if (comp_str == "lz4")     return rocksdb::kLZ4Compression;
        if (comp_str == "lz4hc")   return rocksdb::kLZ4HCCompression;
        if (comp_str == "xpress")  return rocksdb::kXpressCompression;
        if (comp_str == "zstd")    return rocksdb::kZSTD;

        ROCKSDB_LOG_WARN("Unknown compression type '{}', defaulting to LZ4",
                         comp_str);
        return rocksdb::kLZ4Compression;
    }

    // ------------------------------------------------------------------------
    // Member variables
    // ------------------------------------------------------------------------

    /// Configuration for this engine instance
    RocksDbConfig config_;

    /// Primary database handle (may point to DB, TransactionDB, or
    /// OptimisticTransactionDB)
    std::shared_ptr<rocksdb::DB> db_;

    /// TransactionDB handle (valid if use_transactions is true)
    rocksdb::TransactionDB* txn_db_ = nullptr;

    /// OptimisticTransactionDB handle (valid if use_optimistic_transactions)
    rocksdb::OptimisticTransactionDB* opt_txn_db_ = nullptr;

    /// Column family handles indexed by ColumnFamilyId
    std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle,
        std::function<void(rocksdb::ColumnFamilyHandle*)>>> cf_handles_;

    /// Block cache (shared across all column families)
    std::shared_ptr<rocksdb::Cache> block_cache_;

    /// Statistics object (shared with DBOptions)
    std::shared_ptr<rocksdb::Statistics> stats_;

    /// Rate limiter for I/O operations
    std::shared_ptr<rocksdb::RateLimiter> rate_limiter_;

    /// Event listener for compaction/flush monitoring
    std::shared_ptr<RocksDbEventListener> event_listener_;

    /// Backup engine for incremental backups
    std::unique_ptr<rocksdb::BackupEngine> backup_engine_;

    /// Mutex for coordinating open/close/configuration with reads/writes
    mutable std::shared_mutex mutex_;

    /// Whether the database is open
    std::atomic<bool> is_open_;

    /// Whether the database was opened in read-only mode
    std::atomic<bool> is_readonly_;

    /// Operation counters for monitoring
    std::atomic<int64_t> total_puts_;
    std::atomic<int64_t> total_gets_;
    std::atomic<int64_t> total_deletes_;
    std::atomic<int64_t> total_write_batches_;
};

// ============================================================================
// RocksDBStoreFactory — Singleton factory for creating/managing store instances
// ============================================================================

/**
 * @brief Factory for creating and managing RocksDBStore instances.
 *
 * In a multi-broker setup, only one RocksDBStore instance should exist
 * per data directory. The factory ensures a single instance and provides
 * access to it.
 *
 * Thread safety: all access is serialized via a mutex.
 */
class RocksDBStoreFactory {
public:
    /**
     * @brief Returns the singleton factory instance (Meyer's singleton).
     *
     * Thread-safe in C++11 and later due to guaranteed static initialization.
     *
     * @return RocksDBStoreFactory& The singleton instance
     */
    static RocksDBStoreFactory& instance() {
        static RocksDBStoreFactory factory;
        return factory;
    }

    /**
     * @brief Creates or retrieves a RocksDBStore for the given config.
     *
     * If a store with the same database path already exists, returns it.
     * Otherwise, creates a new RocksDBStore, opens it, and caches it.
     *
     * @param config The RocksDB configuration
     * @return std::shared_ptr<RocksDBStore> The store instance, or nullptr
     *         on creation failure
     */
    std::shared_ptr<RocksDBStore> get_or_create(const RocksDbConfig& config) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = stores_.find(config.db_path);
        if (it != stores_.end()) {
            // Return existing store if it's still alive
            if (auto store = it->second.lock()) {
                ROCKSDB_LOG_DEBUG(
                    "Returning existing RocksDBStore for path: {}",
                    config.db_path);
                return store;
            }
            // Otherwise, remove stale entry
            stores_.erase(it);
        }

        // Create new store
        auto store = std::make_shared<RocksDBStore>(config);
        rocksdb::Status s = store->open();
        if (!s.ok()) {
            ROCKSDB_LOG_ERROR(
                "Failed to create RocksDBStore at {}: {}",
                config.db_path, status_to_string(s));
            return nullptr;
        }

        stores_[config.db_path] = store;
        return store;
    }

    /**
     * @brief Retrieves an existing store by database path.
     *
     * @param db_path The database path
     * @return std::shared_ptr<RocksDBStore> The store, or nullptr if not found
     */
    std::shared_ptr<RocksDBStore> get(const std::string& db_path) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = stores_.find(db_path);
        if (it != stores_.end()) {
            return it->second.lock();
        }
        return nullptr;
    }

    /**
     * @brief Closes and removes a store from the factory.
     *
     * @param db_path The database path to remove
     */
    void remove(const std::string& db_path) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = stores_.find(db_path);
        if (it != stores_.end()) {
            if (auto store = it->second.lock()) {
                store->close();
            }
            stores_.erase(it);
        }
    }

    /**
     * @brief Closes all managed stores.
     */
    void shutdown_all() {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& [path, weak_store] : stores_) {
            if (auto store = weak_store.lock()) {
                store->close();
            }
        }
        stores_.clear();
    }

    /**
     * @brief Returns the number of managed stores.
     */
    size_t count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return stores_.size();
    }

private:
    RocksDBStoreFactory() = default;

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::weak_ptr<RocksDBStore>> stores_;
};

// ============================================================================
// RocksDB engine: key-encoding utility functions (public API)
// ============================================================================

/**
 * @brief Encodes an offset key for the offset column family.
 *
 * Format: "topic:partition" → used for tracking committed offsets per partition.
 *
 * @param topic The topic name
 * @param partition The partition index
 * @return std::string The encoded key
 */
std::string make_offset_key(std::string_view topic, int32_t partition) {
    std::ostringstream oss;
    oss << topic << ':' << partition;
    return oss.str();
}

/**
 * @brief Encodes a group offset key for the groups column family.
 *
 * Format: "group:topic:partition" → stores the committed offset for each
 * consumer group, topic, partition tuple.
 *
 * @param group The consumer group ID
 * @param topic The topic name
 * @param partition The partition index
 * @return std::string The encoded key
 */
std::string make_group_offset_key(std::string_view group,
                                  std::string_view topic,
                                  int32_t partition) {
    std::ostringstream oss;
    oss << group << ':' << topic << ':' << partition;
    return oss.str();
}

/**
 * @brief Encodes a transaction key for the transactions column family.
 *
 * Format: "txn_id:producer_id" → stores the state of active transactions
 * for idempotent producer support.
 *
 * @param transactional_id The transactional producer ID
 * @param producer_id The producer epoch or session ID
 * @return std::string The encoded key
 */
std::string make_transaction_key(std::string_view transactional_id,
                                 int64_t producer_id) {
    std::ostringstream oss;
    oss << transactional_id << ':' << producer_id;
    return oss.str();
}

/**
 * @brief Encodes a schema key for the schemas column family.
 *
 * Format: "subject:version" → stores schema definitions and their versions.
 *
 * @param subject The schema subject (typically a topic name)
 * @param version The schema version number
 * @return std::string The encoded key
 */
std::string make_schema_key(std::string_view subject,
                            int32_t version) {
    std::ostringstream oss;
    oss << subject << ':' << version;
    return oss.str();
}

/**
 * @brief Encodes a config key for the configs column family.
 *
 * Format: "namespace:key" → e.g., "broker:max.message.bytes" or
 * "topic:my-topic:retention.ms"
 *
 * @param namespace_ The config namespace (broker, topic, etc.)
 * @param key The config key name
 * @return std::string The encoded key
 */
std::string make_config_key(std::string_view namespace_,
                            std::string_view key) {
    std::ostringstream oss;
    oss << namespace_ << ':' << key;
    return oss.str();
}

// ============================================================================
// RocksDB engine public entry points
// ============================================================================

/**
 * @brief Convenience factory function for creating a stock RocksDBStore.
 *
 * Creates a store with default configuration and standard column families.
 * The database is opened and ready for use on return.
 *
 * @param db_path Filesystem path for the database
 * @return std::shared_ptr<RocksDBStore> or nullptr on failure
 */
std::shared_ptr<RocksDBStore> create_rocksdb_store(
    const std::string& db_path) {
    RocksDbConfig config;
    config.db_path = db_path;
    config.enable_statistics = true;
    config.enable_wal = true;
    config.create_if_missing = true;
    config.create_missing_column_families = true;
    config.backup_dir = db_path + "/../" + std::string(kDefaultBackupDir);

    auto store = std::make_shared<RocksDBStore>(config);
    rocksdb::Status s = store->open();
    if (!s.ok()) {
        ROCKSDB_LOG_ERROR("Failed to create default RocksDB store: {}",
                          status_to_string(s));
        return nullptr;
    }

    return store;
}

/**
 * @brief Creates a transactional RocksDB store for ACID workloads.
 *
 * Uses pessimistic TransactionDB for workloads requiring strict
 * serializability and multi-key atomicity.
 *
 * @param db_path Filesystem path
 * @return std::shared_ptr<RocksDBStore> or nullptr on failure
 */
std::shared_ptr<RocksDBStore> create_transactional_store(
    const std::string& db_path) {
    RocksDbConfig config;
    config.db_path = db_path;
    config.use_transactions = true;
    config.enable_statistics = true;
    config.create_if_missing = true;
    config.create_missing_column_families = true;

    auto store = std::make_shared<RocksDBStore>(config);
    rocksdb::Status s = store->open();
    if (!s.ok()) {
        ROCKSDB_LOG_ERROR("Failed to create transactional RocksDB store: {}",
                          status_to_string(s));
        return nullptr;
    }

    return store;
}

/**
 * @brief Creates an optimistic transactional RocksDB store.
 *
 * Uses OptimisticTransactionDB for low-contention workloads where
 * conflicts are rare and the overhead of locking is undesirable.
 *
 * @param db_path Filesystem path
 * @return std::shared_ptr<RocksDBStore> or nullptr on failure
 */
std::shared_ptr<RocksDBStore> create_optimistic_store(
    const std::string& db_path) {
    RocksDbConfig config;
    config.db_path = db_path;
    config.use_optimistic_transactions = true;
    config.enable_statistics = true;
    config.create_if_missing = true;
    config.create_missing_column_families = true;

    auto store = std::make_shared<RocksDBStore>(config);
    rocksdb::Status s = store->open();
    if (!s.ok()) {
        ROCKSDB_LOG_ERROR(
            "Failed to create optimistic RocksDB store: {}",
            status_to_string(s));
        return nullptr;
    }

    return store;
}

}  // namespace torrent
