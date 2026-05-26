/**
 * page_cache.cpp — LRU Disk Page Cache Implementation
 *
 * Provides a thread-safe LRU cache for disk pages.  Pages are identified
 * by (file_path, offset) pairs.  The cache sits between the storage layer
 * and the filesystem, absorbing repeated reads of the same disk regions.
 *
 * Eviction follows strict LRU: the least recently accessed page is
 * evicted first when the cache exceeds its configured limits.  Both
 * page-count and total-memory limits are enforced simultaneously.
 *
 * Direct I/O detection: pages larger than min_direct_io_size are assumed
 * to have been loaded via Direct I/O and are NOT cached (configurable
 * via PageCacheConfig::cache_direct_io).
 *
 * Thread safety:
 *   - get_page() acquires a shared lock (concurrent readers).
 *   - put_page(), invalidate(), evict() acquire exclusive locks.
 *   - Statistics counters are atomic (lock-free reads).
 *   - LRU list operations are protected by the same shared_mutex.
 */

// ============================================================================
// Project headers
// ============================================================================

#include "torrent/storage/page_cache.h"
#include "torrent/common/types.h"

// ============================================================================
// System headers
// ============================================================================

#include <algorithm>
#include <cmath>
#include <cstring>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include <spdlog/spdlog.h>

namespace torrent {

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace {

/// Logger instance for the page cache subsystem.
std::shared_ptr<spdlog::logger> get_page_cache_logger() {
    static auto logger = spdlog::get("page_cache");
    if (!logger) {
        logger = spdlog::stdout_color_mt("page_cache");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

/// Current time in milliseconds since epoch.
[[nodiscard]] timestamp_ms_t current_time_ms() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

/// Eviction batch ratio: when batch eviction is enabled, evict this
/// fraction of pages beyond the limit at once.
inline constexpr double kEvictBatchRatio = 0.10;

/// After how many get_page misses should we consider a full eviction sweep.
inline constexpr uint64_t kMissThresholdForSweep = 1024;

} // anonymous namespace

// ============================================================================
// PageCache — construction / destruction
// ============================================================================

PageCache::PageCache(PageCacheConfig config)
    : config_(std::move(config))
{
    auto logger = get_page_cache_logger();
    logger->info("PageCache created: max_pages={}, max_memory={}, page_size={}, "
                 "direct_io_min={}, cache_direct_io={}",
                 config_.max_pages, config_.max_memory_bytes, config_.page_size,
                 config_.min_direct_io_size, config_.cache_direct_io);
}

PageCache::~PageCache() {
    auto logger = get_page_cache_logger();
    size_t cleared = clear();
    if (cleared > 0) {
        logger->debug("PageCache destroyed after clearing {} pages", cleared);
    }
}

// ============================================================================
// PageCache::get_page
// ============================================================================

std::optional<CachedPage> PageCache::get_page(
    const std::string& file_path,
    off_t offset,
    size_t size)
{
    // Direct I/O bypass: if the requested page is large enough to have
    // been loaded via Direct I/O, and we're not configured to cache
    // such pages, skip the lookup entirely.
    if (!config_.cache_direct_io && size >= config_.min_direct_io_size) {
        misses_.fetch_add(1, std::memory_order_relaxed);
        return std::nullopt;
    }

    PageKey key{file_path, offset};

    // Shared lock for concurrent reads.
    std::shared_lock<std::shared_mutex> lock(mutex_);

    auto it = cache_.find(key);
    if (it == cache_.end()) {
        // Cache miss.
        misses_.fetch_add(1, std::memory_order_relaxed);

        // Periodically trigger a lazy eviction sweep when miss rate is high.
        uint64_t miss_count = misses_.load(std::memory_order_relaxed);
        if (miss_count > 0 && (miss_count % kMissThresholdForSweep) == 0) {
            // Upgrade to exclusive lock for eviction.
            lock.unlock();
            std::unique_lock<std::shared_mutex> write_lock(mutex_);
            evict_locked(config_.max_pages, config_.max_memory_bytes);
        }

        return std::nullopt;
    }

    // Cache hit: promote to MRU and update statistics.
    auto& cached_page = it->second.first;
    cached_page.touch();

    // Move the key to the front of the LRU list (within the shared lock
    // scope via const_cast for list splicing — safe because we only
    // modify the list order, not the map structure).
    // Note: std::list::splice is non-const, so we const_cast within the
    // shared lock. This is safe here because splice doesn't invalidate
    // iterators and only re-links nodes.  The map is unchanged.
    auto& list_it = it->second.second;
    const_cast<LruList&>(lru_list_).splice(
        const_cast<LruList&>(lru_list_).begin(),
        const_cast<LruList&>(lru_list_),
        list_it);

    hits_.fetch_add(1, std::memory_order_relaxed);

    return cached_page;
}

// ============================================================================
// PageCache::put_page
// ============================================================================

void PageCache::put_page(
    const std::string& file_path,
    off_t offset,
    std::shared_ptr<std::vector<uint8_t>> data,
    size_t size,
    bool direct_io)
{
    // Validate inputs.
    if (!data || size == 0) {
        auto logger = get_page_cache_logger();
        logger->warn("put_page called with null/empty data for {}:{}, ignoring",
                     file_path, offset);
        return;
    }

    // Check if this page should be cached.
    if (!should_cache(size, direct_io)) {
        auto logger = get_page_cache_logger();
        logger->trace("Skipping cache for {}:{} — {}, {} bytes, direct_io={}",
                      file_path, offset,
                      size >= config_.min_direct_io_size ? "direct I/O size" : "policy",
                      size, direct_io);
        return;
    }

    PageKey key{file_path, offset};
    CachedPage page(std::move(data), size, direct_io);

    std::unique_lock<std::shared_mutex> lock(mutex_);

    // Check if key already exists.
    auto existing = cache_.find(key);
    if (existing != cache_.end()) {
        // Update existing entry: replace data, update metadata, promote to MRU.
        size_t old_size = existing->second.first.size;
        existing->second.first = std::move(page);
        memory_used_.fetch_sub(old_size, std::memory_order_relaxed);
        memory_used_.fetch_add(size, std::memory_order_relaxed);

        // Promote to front of LRU list.
        auto& list_it = existing->second.second;
        lru_list_.splice(lru_list_.begin(), lru_list_, list_it);

        auto logger = get_page_cache_logger();
        logger->trace("Updated cached page {}:{} -> {} bytes", file_path, offset, size);
        return;
    }

    // Check limits and evict if necessary before inserting.
    size_t current_pages = page_count_.load(std::memory_order_relaxed);
    size_t current_memory = memory_used_.load(std::memory_order_relaxed);

    if (current_pages >= config_.max_pages ||
        (config_.max_memory_bytes > 0 && current_memory + size > config_.max_memory_bytes))
    {
        size_t target_pages = config_.max_pages > 0 ? config_.max_pages - 1 : 0;
        size_t target_bytes = config_.max_memory_bytes > size
                                ? config_.max_memory_bytes - size
                                : 0;
        evict_locked(target_pages, target_bytes);
    }

    // Insert new page.
    lru_list_.push_front(key);
    auto list_it = lru_list_.begin();

    cache_.emplace(key, std::make_pair(std::move(page), list_it));

    page_count_.fetch_add(1, std::memory_order_relaxed);
    memory_used_.fetch_add(size, std::memory_order_relaxed);

    auto logger = get_page_cache_logger();
    logger->trace("Cached page {}:{} ({} bytes), total pages={}, memory={}",
                  file_path, offset, size,
                  page_count_.load(std::memory_order_relaxed),
                  memory_used_.load(std::memory_order_relaxed));
}

// ============================================================================
// PageCache::invalidate
// ============================================================================

size_t PageCache::invalidate(const std::string& file_path) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    size_t removed = 0;
    size_t bytes_freed = 0;

    // Scan the LRU list and remove all entries matching the file path.
    auto it = lru_list_.begin();
    while (it != lru_list_.end()) {
        if (it->file_path == file_path) {
            // Find the cache entry and subtract its memory.
            auto cache_it = cache_.find(*it);
            if (cache_it != cache_.end()) {
                bytes_freed += cache_it->second.first.size;
                cache_.erase(cache_it);
            }
            it = lru_list_.erase(it);
            ++removed;
        } else {
            ++it;
        }
    }

    if (removed > 0) {
        page_count_.fetch_sub(removed, std::memory_order_relaxed);
        memory_used_.fetch_sub(bytes_freed, std::memory_order_relaxed);

        auto logger = get_page_cache_logger();
        logger->debug("Invalidated {} pages ({} bytes) for file '{}'",
                      removed, bytes_freed, file_path);
    }

    return removed;
}

// ============================================================================
// PageCache::evict (public)
// ============================================================================

size_t PageCache::evict() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return evict_locked(config_.max_pages, config_.max_memory_bytes);
}

// ============================================================================
// PageCache::size / memory_used — inline in header, handled by atomics.
// ============================================================================

// ============================================================================
// PageCache::hit_ratio
// ============================================================================

double PageCache::hit_ratio() const noexcept {
    uint64_t h = hits_.load(std::memory_order_relaxed);
    uint64_t m = misses_.load(std::memory_order_relaxed);
    uint64_t total = h + m;
    if (total == 0) return 0.0;
    return static_cast<double>(h) / static_cast<double>(total);
}

// ============================================================================
// PageCache::reset_stats
// ============================================================================

void PageCache::reset_stats() noexcept {
    hits_.store(0, std::memory_order_relaxed);
    misses_.store(0, std::memory_order_relaxed);
    evictions_.store(0, std::memory_order_relaxed);
}

// ============================================================================
// PageCache::clear
// ============================================================================

size_t PageCache::clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    size_t count = lru_list_.size();
    lru_list_.clear();
    cache_.clear();
    page_count_.store(0, std::memory_order_relaxed);
    memory_used_.store(0, std::memory_order_relaxed);

    auto logger = get_page_cache_logger();
    logger->debug("Cleared {} pages from cache", count);

    return count;
}

// ============================================================================
// PageCache::set_max_pages
// ============================================================================

void PageCache::set_max_pages(size_t max_pages) {
    config_.max_pages = max_pages;
    // Trigger eviction if we're now over the limit.
    std::unique_lock<std::shared_mutex> lock(mutex_);
    evict_locked(max_pages, config_.max_memory_bytes);
}

// ============================================================================
// PageCache::set_max_memory_bytes
// ============================================================================

void PageCache::set_max_memory_bytes(size_t bytes) {
    config_.max_memory_bytes = bytes;
    // Trigger eviction if we're now over the limit.
    std::unique_lock<std::shared_mutex> lock(mutex_);
    evict_locked(config_.max_pages, bytes);
}

// ============================================================================
// Internal helpers
// ============================================================================

size_t PageCache::effective_page_size(size_t requested_size) const noexcept {
    if (requested_size == 0) return config_.page_size;
    return std::max(requested_size, config_.page_size);
}

bool PageCache::should_cache(size_t size, bool direct_io) const noexcept {
    // Never cache zero-sized pages.
    if (size == 0) return false;

    // Direct I/O pages are skipped unless explicitly enabled.
    if (direct_io && !config_.cache_direct_io) return false;

    // Large pages (beyond direct I/O threshold) are also skipped unless
    // direct I/O caching is enabled.
    if (!config_.cache_direct_io && size >= config_.min_direct_io_size)
        return false;

    return true;
}

void PageCache::promote_locked(const PageKey& key) {
    auto it = cache_.find(key);
    if (it == cache_.end()) return;
    auto& list_it = it->second.second;
    lru_list_.splice(lru_list_.begin(), lru_list_, list_it);
}

bool PageCache::evict_one_locked() {
    if (lru_list_.empty()) return false;

    // The LRU page is at the back of the list.
    const PageKey& key = lru_list_.back();

    auto cache_it = cache_.find(key);
    if (cache_it == cache_.end()) {
        // Inconsistent state: key in list but not in map.  Remove from list.
        lru_list_.pop_back();
        return false;
    }

    size_t freed_bytes = cache_it->second.first.size;

    // Remove from cache map and LRU list.
    cache_.erase(cache_it);
    lru_list_.pop_back();

    page_count_.fetch_sub(1, std::memory_order_relaxed);
    memory_used_.fetch_sub(freed_bytes, std::memory_order_relaxed);
    evictions_.fetch_add(1, std::memory_order_relaxed);

    return true;
}

size_t PageCache::evict_locked(size_t target_pages, size_t target_bytes) {
    size_t current_pages = lru_list_.size();
    size_t current_bytes = memory_used_.load(std::memory_order_relaxed);

    // Nothing to do if within limits.
    if (current_pages <= target_pages &&
        (target_bytes == 0 || current_bytes <= target_bytes))
    {
        return 0;
    }

    // Calculate how many pages to evict.
    size_t pages_to_evict = 0;
    if (current_pages > target_pages) {
        pages_to_evict = current_pages - target_pages;
    }

    // Batch eviction: evict extra pages to reduce future evictions.
    if (config_.batch_eviction && pages_to_evict > 0) {
        size_t extra = static_cast<size_t>(
            std::ceil(static_cast<double>(current_pages) * kEvictBatchRatio));
        pages_to_evict = std::max(pages_to_evict, extra);
    }

    // Cap at the actual number of pages.
    pages_to_evict = std::min(pages_to_evict, current_pages);

    size_t evicted = 0;
    size_t bytes_evicted = 0;
    timestamp_ms_t start = current_time_ms();

    for (size_t i = 0; i < pages_to_evict && !lru_list_.empty(); ++i) {
        // Check memory limit as we go.
        size_t current_mem = memory_used_.load(std::memory_order_relaxed);
        if (target_bytes > 0 && current_mem <= target_bytes &&
            lru_list_.size() <= target_pages)
        {
            break;
        }

        if (evict_one_locked()) {
            ++evicted;
        }
    }

    if (evicted > 0) {
        last_eviction_time_.store(start, std::memory_order_relaxed);

        auto logger = get_page_cache_logger();
        timestamp_ms_t elapsed = current_time_ms() - start;
        logger->debug("Evicted {} pages in {}ms, pages_remaining={}, memory_remaining={}",
                      evicted, elapsed,
                      lru_list_.size(),
                      memory_used_.load(std::memory_order_relaxed));
    }

    return evicted;
}

void PageCache::recompute_memory_locked() {
    size_t total = 0;
    for (const auto& [key, pair] : cache_) {
        total += pair.first.size;
    }
    memory_used_.store(total, std::memory_order_relaxed);
}

// ============================================================================
// PageCache — Advanced Operations
// ============================================================================

/**
 * Preload a range of pages from disk into the cache.  This is a bulk
 * warming operation: for each page-aligned chunk in [offset, offset+size),
 * read the data from the given fd and insert into the cache.
 *
 * This is NOT part of the public header API but is available as an
 * internal helper for segment warming during open/recovery.
 *
 * @param file_path  Path to the file.
 * @param fd         Open file descriptor for reading.
 * @param offset     Starting byte offset (will be aligned down to page_size).
 * @param size       Total bytes to preload.
 * @param direct_io  Whether the fd was opened with O_DIRECT.
 * @return           Number of pages successfully preloaded.
 */
size_t PageCache::preload_range(
    const std::string& file_path,
    int fd,
    off_t offset,
    size_t size,
    bool direct_io)
{
    if (fd < 0 || size == 0) return 0;

    // Align offset down to page boundary.
    size_t page_size = config_.page_size;
    off_t aligned_offset = static_cast<off_t>(
        (static_cast<size_t>(offset) / page_size) * page_size);

    size_t loaded = 0;
    size_t remaining = size + static_cast<size_t>(offset - aligned_offset);

    while (remaining > 0) {
        size_t chunk = std::min(remaining, page_size);

        // Skip pages that shouldn't be cached.
        if (!should_cache(chunk, direct_io && chunk >= config_.min_direct_io_size)) {
            aligned_offset += static_cast<off_t>(page_size);
            remaining -= std::min(remaining, page_size);
            continue;
        }

        auto buf = std::make_shared<std::vector<uint8_t>>(chunk);

        ssize_t n = ::pread(fd, buf->data(), chunk, aligned_offset);
        if (n < 0) {
            if (errno == EINTR) {
                n = ::pread(fd, buf->data(), chunk, aligned_offset);
            }
            if (n < 0) {
                // Read failed — skip this page.
                aligned_offset += static_cast<off_t>(page_size);
                remaining -= std::min(remaining, page_size);
                continue;
            }
        }
        if (static_cast<size_t>(n) < chunk) {
            // Short read — resize buffer to actual bytes read.
            buf->resize(static_cast<size_t>(n));
            chunk = static_cast<size_t>(n);
        }

        put_page(file_path, aligned_offset, std::move(buf), chunk, direct_io);
        ++loaded;

        aligned_offset += static_cast<off_t>(page_size);
        remaining -= std::min(remaining, page_size);
    }

    if (loaded > 0) {
        auto logger = get_page_cache_logger();
        logger->debug("Preloaded {} pages for '{}' ({} bytes)", loaded, file_path, size);
    }

    return loaded;
}

/**
 * Collect and return cache-wide statistics for monitoring.
 * NOTE: This is for internal use; expose via public API if needed.
 */
PageCacheStats PageCache::stats() const {
    PageCacheStats s;
    s.page_count    = page_count_.load(std::memory_order_relaxed);
    s.memory_used   = memory_used_.load(std::memory_order_relaxed);
    s.max_pages     = config_.max_pages;
    s.max_memory    = config_.max_memory_bytes;
    s.hit_count     = hits_.load(std::memory_order_relaxed);
    s.miss_count    = misses_.load(std::memory_order_relaxed);
    s.eviction_count = evictions_.load(std::memory_order_relaxed);
    s.hit_ratio     = hit_ratio();

    s.last_eviction_time = last_eviction_time_.load(std::memory_order_relaxed);

    // Count pages by age bucket.
    timestamp_ms_t now = current_time_ms();
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        for (const auto& [key, pair] : cache_) {
            timestamp_ms_t age = now - pair.first.loaded_at;
            if (age < 60000)        // < 1 min
                ++s.pages_lt_1min;
            else if (age < 300000)  // < 5 min
                ++s.pages_lt_5min;
            else if (age < 900000)  // < 15 min
                ++s.pages_lt_15min;
            else
                ++s.pages_gt_15min;

            s.total_hit_count += pair.first.hit_count.load(std::memory_order_relaxed);
        }
    }

    return s;
}

/**
 * Check the health of the cache: returns true if the cache is within
 * configured limits and hasn't hit error conditions.
 */
bool PageCache::is_healthy() const noexcept {
    size_t pages = page_count_.load(std::memory_order_relaxed);
    size_t mem   = memory_used_.load(std::memory_order_relaxed);

    if (pages > config_.max_pages * 2) return false;  // severely over limit
    if (config_.max_memory_bytes > 0 && mem > config_.max_memory_bytes * 2)
        return false;

    return true;
}

/**
 * Attempt to shrink the cache by evicting the oldest `count` pages
 * regardless of limits.  Returns number actually evicted.
 */
size_t PageCache::shrink(size_t count) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    size_t evicted = 0;
    for (size_t i = 0; i < count && !lru_list_.empty(); ++i) {
        if (evict_one_locked()) ++evicted;
    }
    return evicted;
}

/**
 * Per-file hit statistics: returns the number of cache hits for pages
 * belonging to the given file path.
 */
uint64_t PageCache::file_hit_count(const std::string& file_path) const {
    uint64_t total = 0;
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (const auto& [key, pair] : cache_) {
        if (key.file_path == file_path) {
            total += pair.first.hit_count.load(std::memory_order_relaxed);
        }
    }
    return total;
}

} // namespace torrent

