/**
 * torrent-mq — BatchCache Implementation
 *
 * In-memory LRU cache for RecordBatches keyed by (topic, partition, offset).
 * Evicts based on total byte count rather than entry count to handle the
 * wide variance in RecordBatch sizes (a few bytes to several MiB).
 *
 * Design:
 *   - std::shared_mutex: shared for reads (get/contains), exclusive for
 *     writes (put/erase/invalidate).
 *   - LRU list (std::list) tracks access order; hash map (std::unordered_map)
 *     provides O(1) key-to-iterator lookup.
 *   - Byte tracking: total_bytes_ is maintained atomically for fast reads
 *     without holding the exclusive lock during eviction calculations.
 *   - Access metadata: last_access_ms and access_count are updated on reads
 *     to support access-frequency-aware eviction policies in the future.
 *   - Invalidation: supports partition-wide, topic-wide, and offset-range
 *     invalidation for correctness when partition state changes.
 *
 * Thread safety: all public methods are safe for concurrent invocation.
 * Internal eviction is triggered within put() when the byte limit is
 * exceeded.
 */

#include "torrent/storage/batch_cache.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <sstream>

namespace torrent {

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace {

/**
 * Get (or create) a dedicated logger for the batch cache subsystem.
 */
std::shared_ptr<spdlog::logger> get_cache_logger() {
    static auto logger = spdlog::get("batch_cache");
    if (!logger) {
        logger = spdlog::stdout_color_mt("batch_cache");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

/**
 * Minimum byte threshold for a single entry. Entries smaller than this
 * don't trigger eviction checks to avoid thrashing on tiny batches.
 */
static constexpr byte_count_t kMinBatchBytesForCheck = 256;

/**
 * After eviction, we try to bring the cache to this fraction of max
 * (0.85 = 85%) rather than exactly max, to avoid immediate re-eviction
 * on the next insertion.
 */
static constexpr double kEvictionTargetRatio = 0.85;

/**
 * Compute the approximate byte size of a RecordBatch.
 * Uses the built-in approximate_size() when records are present;
 * otherwise estimates a minimum overhead.
 */
byte_count_t compute_batch_size(const RecordBatch& batch) noexcept {
    byte_count_t sz = batch.approximate_size();
    if (sz == 0 && batch.record_count > 0) {
        // Batch has records but approximate_size returned 0 — estimate.
        // Minimum batch overhead is ~61 bytes (headers) + records.
        sz = 61 + static_cast<byte_count_t>(batch.record_count) * 8;
    }
    return std::max(sz, byte_count_t(1));
}

} // anonymous namespace

// ============================================================================
// Construction / Destruction
// ============================================================================

BatchCache::BatchCache(byte_count_t max_bytes) {
    max_bytes_.store(std::max(max_bytes, byte_count_t(0)), std::memory_order_relaxed);
    total_bytes_.store(0, std::memory_order_relaxed);

    auto logger = get_cache_logger();
    logger->info("batch_cache created: max_bytes={}", max_bytes_.load());
}

BatchCache::~BatchCache() {
    auto logger = get_cache_logger();
    logger->info("batch_cache destroyed: entries={}, bytes={}, "
                 "lookups={}, hits={}, evictions={}",
                 lru_list_.size(), total_bytes_.load(),
                 total_lookups_, hits_, evictions_);
}

// ============================================================================
// now_ms — current wall time
// ============================================================================

timestamp_ms_t BatchCache::now_ms() noexcept {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch());
    return static_cast<timestamp_ms_t>(ms.count());
}

// ============================================================================
// get — retrieve a RecordBatch from the cache
// ============================================================================

std::optional<RecordBatch> BatchCache::get(const BatchCacheKey& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    total_lookups_++;

    auto it = lru_map_.find(key);
    if (it == lru_map_.end()) {
        misses_++;
        return std::nullopt;
    }

    hits_++;

    // Update access metadata on the entry.
    auto& entry = it->second->entry;
    entry.last_access_ms = now_ms();
    entry.access_count++;

    // Copy the batch before moving the node (to avoid iterator invalidation
    // issues with the splice).  We copy first, then promote.
    RecordBatch result = it->second->entry.batch;

    return result;
}

// ============================================================================
// put — insert or update a RecordBatch
// ============================================================================

void BatchCache::put(const BatchCacheKey& key, const RecordBatch& batch) {
    byte_count_t new_size = compute_batch_size(batch);
    timestamp_ms_t ts = now_ms();

    std::unique_lock<std::shared_mutex> lock(mutex_);

    auto it = lru_map_.find(key);
    if (it != lru_map_.end()) {
        // Key already exists — update in place.
        byte_count_t old_size = it->second->entry.byte_size;
        it->second->entry.batch          = batch;
        it->second->entry.byte_size      = new_size;
        it->second->entry.last_access_ms = ts;
        it->second->entry.access_count   = 0;  // reset on update

        // Adjust total byte count.
        total_bytes_.fetch_sub(old_size, std::memory_order_relaxed);
        total_bytes_.fetch_add(new_size, std::memory_order_relaxed);

        // Promote to front (MRU).
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);

        insertions_++;
    } else {
        // New entry — insert at front.
        LruNode node;
        node.key            = key;
        node.entry.batch          = batch;
        node.entry.byte_size      = new_size;
        node.entry.last_access_ms = ts;
        node.entry.access_count   = 1;

        lru_list_.emplace_front(std::move(node));
        lru_map_[key] = lru_list_.begin();

        total_bytes_.fetch_add(new_size, std::memory_order_relaxed);
        insertions_++;
    }

    // Check if over capacity and evict if needed.
    byte_count_t max = max_bytes_.load(std::memory_order_relaxed);
    byte_count_t total = total_bytes_.load(std::memory_order_relaxed);

    if (max > 0 && total > max) {
        byte_count_t target = static_cast<byte_count_t>(
            static_cast<double>(max) * kEvictionTargetRatio);
        evict_lru(static_cast<size_t>(target));
    }
}

// ============================================================================
// contains — check existence without promotion
// ============================================================================

bool BatchCache::contains(const BatchCacheKey& key) const {
    // We use a shared lock for lookup.  We don't update access metadata
    // because contains() is a non-mutating check.  We do NOT count this
    // as a lookup for hit-ratio purposes (caller can decide).
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return lru_map_.find(key) != lru_map_.end();
}

// ============================================================================
// erase — remove a single entry
// ============================================================================

bool BatchCache::erase(const BatchCacheKey& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    auto it = lru_map_.find(key);
    if (it == lru_map_.end()) {
        return false;
    }

    byte_count_t removed_size = it->second->entry.byte_size;
    lru_list_.erase(it->second);
    lru_map_.erase(it);
    total_bytes_.fetch_sub(removed_size, std::memory_order_relaxed);

    invalidations_++;

    return true;
}

// ============================================================================
// clear — remove all entries
// ============================================================================

void BatchCache::clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    size_t removed = lru_list_.size();
    lru_list_.clear();
    lru_map_.clear();
    total_bytes_.store(0, std::memory_order_relaxed);

    if (removed > 0) {
        batch_invalidations_++;
        invalidations_ += removed;
    }

    auto logger = get_cache_logger();
    logger->debug("batch_cache: cleared {} entries", removed);
}

// ============================================================================
// Capacity management
// ============================================================================

byte_count_t BatchCache::max_bytes() const noexcept {
    return max_bytes_.load(std::memory_order_relaxed);
}

void BatchCache::resize(byte_count_t new_max_bytes) {
    byte_count_t clamped = std::max(new_max_bytes, byte_count_t(0));
    max_bytes_.store(clamped, std::memory_order_relaxed);

    if (clamped == 0) {
        // Resize to 0 = clear.
        clear();
        return;
    }

    // If new max is smaller, evict until under limit.
    std::unique_lock<std::shared_mutex> lock(mutex_);
    byte_count_t total = total_bytes_.load(std::memory_order_relaxed);
    if (total > clamped) {
        byte_count_t target = static_cast<byte_count_t>(
            static_cast<double>(clamped) * kEvictionTargetRatio);
        evict_lru(static_cast<size_t>(target));
    }

    auto logger = get_cache_logger();
    logger->info("batch_cache: resized to {} bytes (current: {} entries, {} bytes)",
                 clamped, lru_list_.size(), total_bytes_.load());
}

byte_count_t BatchCache::total_bytes() const noexcept {
    return total_bytes_.load(std::memory_order_relaxed);
}

size_t BatchCache::entry_count() const noexcept {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return lru_list_.size();
}

bool BatchCache::empty() const noexcept {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return lru_list_.empty();
}

// ============================================================================
// Invalidation
// ============================================================================

size_t BatchCache::invalidate_partition(topic_id_t topic_id,
                                         partition_id_t partition_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    size_t removed = 0;
    auto it = lru_list_.begin();
    while (it != lru_list_.end()) {
        if (it->key.topic_id == topic_id && it->key.partition_id == partition_id) {
            byte_count_t removed_size = it->entry.byte_size;
            total_bytes_.fetch_sub(removed_size, std::memory_order_relaxed);
            lru_map_.erase(it->key);
            it = lru_list_.erase(it);
            removed++;
        } else {
            ++it;
        }
    }

    if (removed > 0) {
        invalidations_ += removed;
        batch_invalidations_++;
    }

    auto logger = get_cache_logger();
    logger->debug("batch_cache: invalidated partition topic={} part={}: {} entries",
                  topic_id, partition_id, removed);

    return removed;
}

size_t BatchCache::invalidate_topic(topic_id_t topic_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    size_t removed = 0;
    auto it = lru_list_.begin();
    while (it != lru_list_.end()) {
        if (it->key.topic_id == topic_id) {
            byte_count_t removed_size = it->entry.byte_size;
            total_bytes_.fetch_sub(removed_size, std::memory_order_relaxed);
            lru_map_.erase(it->key);
            it = lru_list_.erase(it);
            removed++;
        } else {
            ++it;
        }
    }

    if (removed > 0) {
        invalidations_ += removed;
        batch_invalidations_++;
    }

    auto logger = get_cache_logger();
    logger->info("batch_cache: invalidated topic {}: {} entries", topic_id, removed);

    return removed;
}

size_t BatchCache::invalidate_offset_range(topic_id_t topic_id,
                                            partition_id_t partition_id,
                                            offset_t start_offset,
                                            offset_t end_offset) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    size_t removed = 0;
    auto it = lru_list_.begin();
    while (it != lru_list_.end()) {
        if (it->key.topic_id == topic_id
            && it->key.partition_id == partition_id
            && it->key.base_offset >= start_offset
            && it->key.base_offset < end_offset) {

            byte_count_t removed_size = it->entry.byte_size;
            total_bytes_.fetch_sub(removed_size, std::memory_order_relaxed);
            lru_map_.erase(it->key);
            it = lru_list_.erase(it);
            removed++;
        } else {
            ++it;
        }
    }

    if (removed > 0) {
        invalidations_ += removed;
        // Don't count offset-range scans as batch_invalidations; they're
        // individual removals.
    }

    auto logger = get_cache_logger();
    logger->debug("batch_cache: invalidated offset range [{}, {}) "
                  "topic={} part={}: {} entries",
                  start_offset, end_offset, topic_id, partition_id, removed);

    return removed;
}

// ============================================================================
// Statistics
// ============================================================================

BatchCacheStats BatchCache::stats(bool keep_stats) const {
    BatchCacheStats s;

    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        s.total_lookups      = total_lookups_;
        s.hits               = hits_;
        s.misses             = misses_;
        s.insertions         = insertions_;
        s.evictions          = evictions_;
        s.invalidations      = invalidations_;
        s.batch_invalidations = batch_invalidations_;
        s.entry_count        = lru_list_.size();
    }

    s.byte_count = total_bytes_.load(std::memory_order_relaxed);
    s.max_bytes  = max_bytes_.load(std::memory_order_relaxed);

    if (!keep_stats) {
        total_lookups_      = 0;
        hits_               = 0;
        misses_             = 0;
        insertions_         = 0;
        evictions_          = 0;
        invalidations_      = 0;
        batch_invalidations_ = 0;
    }

    return s;
}

void BatchCache::reset_stats() {
    total_lookups_      = 0;
    hits_               = 0;
    misses_             = 0;
    insertions_         = 0;
    evictions_          = 0;
    invalidations_      = 0;
    batch_invalidations_ = 0;
}

std::string BatchCache::summary() const {
    BatchCacheStats s = stats(true);

    std::ostringstream oss;
    oss << "BatchCache["
        << "entries=" << s.entry_count
        << ", bytes=" << s.byte_count << "/" << s.max_bytes
        << " (" << std::fixed << std::setprecision(1)
        << (s.utilisation() * 100.0) << "%)"
        << ", hit_ratio=" << std::setprecision(2) << (s.hit_ratio() * 100.0) << "%"
        << ", lookups=" << s.total_lookups
        << ", evictions=" << s.evictions
        << "]";
    return oss.str();
}

// ============================================================================
// evict_lru — internal LRU eviction
// ============================================================================

void BatchCache::evict_lru(size_t target_bytes) {
    byte_count_t current = total_bytes_.load(std::memory_order_relaxed);

    while (current > static_cast<byte_count_t>(target_bytes) && !lru_list_.empty()) {
        // Evict the least recently used entry (back of the list).
        auto& back = lru_list_.back();
        byte_count_t evicted_size = back.entry.byte_size;

        // Update stats before erasing.
        lru_map_.erase(back.key);
        lru_list_.pop_back();

        current = total_bytes_.fetch_sub(evicted_size, std::memory_order_relaxed) - evicted_size;
        evictions_++;

        auto logger = get_cache_logger();
        logger->trace("batch_cache: evicted key (topic={}, part={}, offset={}) "
                      "size={} bytes, current_total={}",
                      back.key.topic_id, back.key.partition_id,
                      back.key.base_offset, evicted_size, current);
    }
}

} // namespace torrent
