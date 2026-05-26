#pragma once

/**
 * torrent-mq — BatchCache: LRU cache for recently-read RecordBatches
 *
 * Provides an in-memory cache keyed by (topic, partition, offset) for
 * frequently accessed RecordBatches.  Uses LRU eviction based on total
 * byte count rather than entry count, since batch sizes vary widely.
 *
 * Key responsibilities:
 *   - Store and retrieve RecordBatches by (topic_id, partition_id, base_offset)
 *   - Track total cached bytes and evict LRU entries when capacity exceeded
 *   - Track hit/miss ratio for observability
 *   - Support bulk invalidation when partition state changes (seal, truncate)
 *   - Thread-safe for concurrent reads and writes (shared_mutex)
 *
 * Integration points:
 *   - FetchHandler: checks cache before reading from disk
 *   - ProduceHandler: invalidates affected offset ranges after appends
 *   - PartitionManager: invalidates entire partition cache on reassignment
 *   - CompactionManager: invalidates after compaction merges segments
 */

#include <cstdint>
#include <list>
#include <unordered_map>
#include <shared_mutex>
#include <optional>
#include <functional>
#include <string>

#include "torrent/common/types.h"
#include "torrent/storage/types.h"

namespace torrent {

// ============================================================================
// BatchCacheKey — compound key for cache lookups
// ============================================================================

/**
 * Uniquely identifies a RecordBatch within the cache.
 *
 * The key includes (topic_id, partition_id, base_offset) because batches
 * from different partitions may share offset values, and the topic
 * namespace provides an additional dimension for multi-tenant isolation.
 */
struct BatchCacheKey {
    topic_id_t     topic_id     = 0;
    partition_id_t partition_id = 0;
    offset_t       base_offset  = kInvalidOffset;

    [[nodiscard]] bool operator==(const BatchCacheKey& other) const noexcept {
        return topic_id == other.topic_id
            && partition_id == other.partition_id
            && base_offset == other.base_offset;
    }

    [[nodiscard]] bool operator!=(const BatchCacheKey& other) const noexcept {
        return !(*this == other);
    }
};

// ============================================================================
// BatchCacheStats — runtime statistics for monitoring
// ============================================================================

/**
 * Snapshot of cache performance metrics.
 *
 * Exposed via prometheus_exporter and admin API for operational insight.
 */
struct BatchCacheStats {
    /// Total number of cache lookups (successful + failed).
    uint64_t total_lookups = 0;

    /// Number of lookups that found a cached entry (hits).
    uint64_t hits = 0;

    /// Number of lookups that missed (cache miss).
    uint64_t misses = 0;

    /// Number of cache insertions (including overwrites).
    uint64_t insertions = 0;

    /// Number of evictions due to capacity pressure.
    uint64_t evictions = 0;

    /// Number of invalidations (explicit removes, not LRU evictions).
    uint64_t invalidations = 0;

    /// Number of batch invalidations (partition/topic clears).
    uint64_t batch_invalidations = 0;

    /// Current number of cached entries.
    size_t entry_count = 0;

    /// Current total bytes of cached data.
    byte_count_t byte_count = 0;

    /// Maximum configured byte capacity.
    byte_count_t max_bytes = 0;

    /// Computed hit ratio: hits / total_lookups (0.0 if no lookups).
    [[nodiscard]] double hit_ratio() const noexcept {
        if (total_lookups == 0) return 0.0;
        return static_cast<double>(hits) / static_cast<double>(total_lookups);
    }

    /// Computed miss ratio: misses / total_lookups.
    [[nodiscard]] double miss_ratio() const noexcept {
        if (total_lookups == 0) return 0.0;
        return static_cast<double>(misses) / static_cast<double>(total_lookups);
    }

    /// Memory utilisation: byte_count / max_bytes (0.0 if max_bytes == 0).
    [[nodiscard]] double utilisation() const noexcept {
        if (max_bytes == 0) return 0.0;
        return static_cast<double>(byte_count) / static_cast<double>(max_bytes);
    }
};

// ============================================================================
// BatchCacheEntry — cache entry holding a RecordBatch with metadata
// ============================================================================

/**
 * Wraps a RecordBatch with metadata used for LRU tracking and eviction.
 */
struct BatchCacheEntry {
    /// The cached RecordBatch.
    RecordBatch batch;

    /// Byte size of this entry (approximate, from RecordBatch::approximate_size).
    byte_count_t byte_size = 0;

    /// Wall-clock time when this entry was inserted or last accessed.
    timestamp_ms_t last_access_ms = 0;

    /// Number of times this entry has been retrieved (access count).
    uint64_t access_count = 0;

    [[nodiscard]] bool empty() const noexcept {
        return batch.records.empty() && batch.record_count == 0;
    }
};

// ============================================================================
// BatchCache — LRU cache keyed by (topic, partition, offset)
// ============================================================================

/**
 * Thread-safe, byte-limited LRU cache for RecordBatches.
 *
 * Design:
 *   - Uses std::shared_mutex: shared for reads, exclusive for writes.
 *   - LRU list tracks access order; hash map provides O(1) lookup.
 *   - Eviction is byte-based: when total cached bytes exceeds max_bytes,
 *     the least recently used entries are evicted until within capacity.
 *   - Auto-invalidation: invalidate_partition() removes all entries for a
 *     partition; invalidate_range() removes entries whose offsets fall
 *     within a specified range.
 *
 * Usage:
 *   BatchCache cache(256 * 1024 * 1024);  // 256 MiB
 *   auto batch = cache.get({topic, part, offset});
 *   if (!batch) { read from disk; cache.put(key, batch); }
 */
class BatchCache {
public:
    // ----------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------

    /**
     * Construct a BatchCache with the given byte capacity.
     *
     * @param max_bytes Maximum total cached bytes before eviction.
     *                  0 = unlimited (not recommended in production).
     */
    explicit BatchCache(byte_count_t max_bytes = 268435456);  // 256 MiB default
    ~BatchCache();

    BatchCache(const BatchCache&) = delete;
    BatchCache& operator=(const BatchCache&) = delete;
    BatchCache(BatchCache&&) = delete;
    BatchCache& operator=(BatchCache&&) = delete;

    // ----------------------------------------------------------------
    // Core operations
    // ----------------------------------------------------------------

    /**
     * Retrieve a RecordBatch from the cache.
     *
     * On hit: promotes the entry to MRU, updates access_count and
     * last_access_ms, and returns a copy of the batch.
     *
     * On miss: returns std::nullopt.
     *
     * @param key The (topic_id, partition_id, base_offset) key
     * @return The cached RecordBatch if found, nullopt otherwise
     */
    [[nodiscard]] std::optional<RecordBatch> get(const BatchCacheKey& key);

    /**
     * Insert or update a RecordBatch in the cache.
     *
     * If the key already exists, the existing entry is updated
     * (byte_size is recalculated and the entry promoted to MRU).
     *
     * If the cache is over capacity after insertion, LRU entries
     * are evicted until total bytes <= max_bytes.
     *
     * @param key   The cache key
     * @param batch The RecordBatch to cache (copied)
     */
    void put(const BatchCacheKey& key, const RecordBatch& batch);

    /**
     * Check if a key exists in the cache without promoting it.
     * Does not affect LRU order or access counts.
     */
    [[nodiscard]] bool contains(const BatchCacheKey& key) const;

    /**
     * Remove a specific entry from the cache.
     *
     * @param key The key to remove
     * @return true if an entry was removed, false if not found
     */
    bool erase(const BatchCacheKey& key);

    /**
     * Remove all entries from the cache.
     */
    void clear();

    // ----------------------------------------------------------------
    // Capacity management
    // ----------------------------------------------------------------

    /// Get the current maximum byte capacity.
    [[nodiscard]] byte_count_t max_bytes() const noexcept;

    /**
     * Resize the cache capacity at runtime.
     *
     * If new_max_bytes < current byte_count, LRU entries are evicted
     * immediately until within the new limit.
     *
     * @param new_max_bytes New maximum byte capacity
     */
    void resize(byte_count_t new_max_bytes);

    /// Get the current total cached bytes.
    [[nodiscard]] byte_count_t total_bytes() const noexcept;

    /// Get the current number of cached entries.
    [[nodiscard]] size_t entry_count() const noexcept;

    /// True when the cache has no entries.
    [[nodiscard]] bool empty() const noexcept;

    // ----------------------------------------------------------------
    // Invalidation
    // ----------------------------------------------------------------

    /**
     * Invalidate all cached entries for a given partition.
     *
     * Called when a partition is reassigned, deleted, or its
     * log-start offset advances past cached data.
     *
     * @param topic_id     The topic owning the partition
     * @param partition_id The partition to invalidate
     * @return Number of entries removed
     */
    size_t invalidate_partition(topic_id_t topic_id,
                                partition_id_t partition_id);

    /**
     * Invalidate all cached entries for a given topic.
     *
     * Called on topic deletion or massive reconfiguration.
     *
     * @param topic_id The topic to invalidate
     * @return Number of entries removed
     */
    size_t invalidate_topic(topic_id_t topic_id);

    /**
     * Invalidate cached entries whose base_offset falls within
     * [start_offset, end_offset) for a given partition.
     *
     * Called after compaction merges segments, making old offsets
     * unavailable. Entries with base_offset in the range are removed.
     *
     * @param topic_id      The topic
     * @param partition_id  The partition
     * @param start_offset  Start of invalidated range (inclusive)
     * @param end_offset    End of invalidated range (exclusive)
     * @return Number of entries removed
     */
    size_t invalidate_offset_range(topic_id_t topic_id,
                                   partition_id_t partition_id,
                                   offset_t start_offset,
                                   offset_t end_offset);

    // ----------------------------------------------------------------
    // Statistics
    // ----------------------------------------------------------------

    /**
     * Get a snapshot of current cache statistics.
     * Resets hit/miss counters (sliding window) unless keep_stats=true.
     */
    [[nodiscard]] BatchCacheStats stats(bool keep_stats = false) const;

    /// Reset hit/miss counters to zero (preserves cached entries).
    void reset_stats();

    /// Get a human-readable summary string for logging.
    [[nodiscard]] std::string summary() const;

private:
    // ----------------------------------------------------------------
    // Internal data structures
    // ----------------------------------------------------------------

    /// A node in the LRU list.
    struct LruNode {
        BatchCacheKey key;
        BatchCacheEntry entry;
    };

    using LruList = std::list<LruNode>;
    using LruMap  = std::unordered_map<BatchCacheKey,
                                        typename LruList::iterator>;

    /// Evict LRU entries until total bytes <= target or cache is empty.
    /// Must be called while holding exclusive lock.
    void evict_lru(size_t target_bytes);

    /// Get the current timestamp in milliseconds since epoch.
    [[nodiscard]] static timestamp_ms_t now_ms() noexcept;

    // ----------------------------------------------------------------
    // State
    // ----------------------------------------------------------------

    LruList lru_list_;
    LruMap  lru_map_;
    mutable std::shared_mutex mutex_;

    std::atomic<byte_count_t> max_bytes_{0};
    std::atomic<byte_count_t> total_bytes_{0};

    mutable uint64_t total_lookups_ = 0;
    mutable uint64_t hits_ = 0;
    mutable uint64_t misses_ = 0;
    mutable uint64_t insertions_ = 0;
    mutable uint64_t evictions_ = 0;
    mutable uint64_t invalidations_ = 0;
    mutable uint64_t batch_invalidations_ = 0;
};

} // namespace torrent

// ============================================================================
// std::hash specialisation for BatchCacheKey
// ============================================================================

namespace std {
template<>
struct hash<torrent::BatchCacheKey> {
    size_t operator()(const torrent::BatchCacheKey& key) const noexcept {
        // Combine three 64-bit values using a standard hash combine technique.
        size_t h = hash<uint64_t>{}(key.topic_id);
        h ^= hash<int32_t>{}(key.partition_id) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= hash<int64_t>{}(key.base_offset) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};
} // namespace std
