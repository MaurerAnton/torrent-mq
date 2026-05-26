#pragma once

/**
 * torrent-mq — PageCache: LRU Disk Page Cache
 *
 * An LRU-based page cache that sits between the storage layer and the
 * filesystem.  Caches frequently-accessed disk regions (pages) in memory
 * to avoid repeated disk reads.  Pages are identified by (file_path,
 * offset) pairs and are evicted under the LRU policy when the cache
 * reaches its configured capacity (max_pages or max_memory_bytes).
 *
 * Features:
 *   - LRU eviction with configurable per-page size and max total memory
 *   - Thread-safe via shared_mutex (readers concurrent, writers exclusive)
 *   - Direct I/O bypass detection: pages >= kMinDirectIOSize are not
 *     cached (they bypass the page cache by design)
 *   - Batch invalidate(path): clears all pages for a given file path
 *   - Hit/miss statistics for monitoring
 *   - Manual pre-warming via put_page()
 *
 * A page is represented as a buffer_view (non-owning) into the cache's
 * internal buffer or memory-mapped region.  When a page is evicted the
 * caller must ensure any references are released.
 */

#include <cstdint>
#include <cstddef>
#include <string>
#include <string_view>
#include <list>
#include <unordered_map>
#include <shared_mutex>
#include <optional>
#include <memory>
#include <chrono>
#include <atomic>
#include <vector>
#include <utility>

namespace torrent {

// ============================================================================
// PageKey — uniquely identifies a cached page
// ============================================================================

/// Uniquely identifies a page in the cache: a file path and the starting
/// byte offset within that file.
struct PageKey {
    std::string file_path;
    off_t       offset = 0;

    [[nodiscard]] bool operator==(const PageKey& other) const noexcept {
        return offset == other.offset && file_path == other.file_path;
    }
};

} // namespace torrent

namespace std {
template<>
struct hash<torrent::PageKey> {
    size_t operator()(const torrent::PageKey& k) const noexcept {
        size_t h1 = hash<string>{}(k.file_path);
        size_t h2 = hash<off_t>{}(k.offset);
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
    }
};
} // namespace std

namespace torrent {

// ============================================================================
// CachedPage — a single page stored in the cache
// ============================================================================

/// Represents one cached page: the raw data buffer and metadata.
struct CachedPage {
    /// Raw page data (owned by the cache).
    std::shared_ptr<std::vector<uint8_t>> data;

    /// Size in bytes of valid data in this page.
    size_t                               size = 0;

    /// Wall-clock timestamp when this page was loaded into cache.
    timestamp_ms_t                       loaded_at = 0;

    /// Wall-clock timestamp when this page was last accessed.
    std::atomic<timestamp_ms_t>          last_access{0};

    /// Number of times this page has been served from cache.
    std::atomic<uint64_t>                hit_count{0};

    /// True if this page was loaded from a Direct I/O path.
    bool                                 from_direct_io = false;

    CachedPage() = default;

    CachedPage(std::shared_ptr<std::vector<uint8_t>> d,
               size_t s,
               bool direct_io = false)
        : data(std::move(d))
        , size(s)
        , from_direct_io(direct_io)
    {
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        loaded_at = now;
        last_access.store(now, std::memory_order_relaxed);
    }

    [[nodiscard]] const uint8_t* bytes() const noexcept {
        return data ? data->data() : nullptr;
    }

    [[nodiscard]] uint8_t* mutable_bytes() noexcept {
        return data ? data->data() : nullptr;
    }

    /// Touch (update last access time and increment hit count).
    void touch() noexcept {
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        last_access.store(now, std::memory_order_relaxed);
        hit_count.fetch_add(1, std::memory_order_relaxed);
    }
};

// ============================================================================
// PageCacheConfig — tunable parameters
// ============================================================================

struct PageCacheConfig {
    /// Maximum number of pages stored concurrently.  When the cache has
    /// max_pages entries, the least-recently-used page is evicted before
    /// inserting a new one.  Default: 16384 pages.
    size_t max_pages = 16384;

    /// Maximum total memory consumed by cached pages, in bytes.  When
    /// the cumulative size of all cached data exceeds this limit, LRU
    /// eviction runs until total size is below this threshold.  This
    /// limit is checked as a soft limit; pages may transiently exceed it
    /// until the next insertion triggers eviction.  Default: 1 GiB.
    /// 0 disables memory-based eviction (only page-count eviction applies).
    size_t max_memory_bytes = 1073741824;

    /// Default page size in bytes for read-ahead and alignment.  Must be
    /// a multiple of the system page size (4096).  Default: 65536 (64 KiB).
    size_t page_size = 65536;

    /// Minimum I/O size (in bytes) that triggers Direct I/O path.  Pages
    /// loaded via Direct I/O are NOT cached because they are large and
    /// benefit more from the kernel's own page cache management (or from
    /// bypassing it entirely).  Default: 262144 (256 KiB).
    size_t min_direct_io_size = 262144;

    /// When true, pages loaded via Direct I/O are cached anyway
    /// (overriding the default bypass behavior).  Default: false.
    bool cache_direct_io = false;

    /// When true, evict pages in batches (evict 10% at a time) instead
    /// of one-by-one.  Reduces lock contention under heavy load.
    /// Default: true.
    bool batch_eviction = true;

    /// Interval between eviction sweeps in milliseconds (only when
    /// batch_eviction is enabled).  Default: 5000 (5 seconds).
    timestamp_ms_t eviction_interval_ms = 5000;
};

// ============================================================================
// PageCacheStats — monitoring snapshot
// ============================================================================

/// A point-in-time snapshot of PageCache statistics for metrics / monitoring.
struct PageCacheStats {
    size_t       page_count         = 0;
    size_t       memory_used        = 0;
    size_t       max_pages          = 0;
    size_t       max_memory         = 0;
    uint64_t     hit_count          = 0;
    uint64_t     miss_count         = 0;
    uint64_t     eviction_count     = 0;
    double       hit_ratio          = 0.0;
    timestamp_ms_t last_eviction_time = 0;
    uint64_t     total_hit_count    = 0;
    size_t       pages_lt_1min      = 0;
    size_t       pages_lt_5min      = 0;
    size_t       pages_lt_15min     = 0;
    size_t       pages_gt_15min     = 0;
};

// ============================================================================
// PageCache — LRU disk page cache
// ============================================================================

/**
 * An LRU (Least Recently Used) cache for disk pages.  Pages are identified
 * by a PageKey (file_path + offset).  The cache provides O(1) lookup and
 * O(1) eviction via a combination of hash table and doubly-linked list.
 *
 * Thread safety:
 *   - get_page() uses a shared lock (multiple concurrent readers).
 *   - put_page() and invalidate() use exclusive locks (writers serialized).
 *   - Eviction (internal) runs under exclusive lock, but is triggered
 *     opportunistically on insertions rather than in a background thread.
 *
 * Usage:
 *   PageCache cache({.max_pages=4096, .max_memory_bytes=256*1024*1024});
 *   auto page = cache.get_page("/data/segment.log", 4096, 8192);
 *   if (!page) { // cache miss, read from disk
 *       auto buf = read_from_disk(path, offset, size);
 *       cache.put_page(path, offset, std::move(buf));
 *   }
 */
class PageCache {
public:
    // -- Construction / Destruction ----------------------------------------

    explicit PageCache(PageCacheConfig config = {});
    ~PageCache();

    PageCache(const PageCache&) = delete;
    PageCache& operator=(const PageCache&) = delete;
    PageCache(PageCache&&) = delete;
    PageCache& operator=(PageCache&&) = delete;

    // -- Page access -------------------------------------------------------

    /**
     * Look up a page in the cache.
     *
     * If found, the page is promoted to MRU (most-recently-used) and its
     * access metadata is updated.  Returns the CachedPage data on hit,
     * std::nullopt on miss.
     *
     * @param file_path  Path to the disk file.
     * @param offset     Starting byte offset within the file.
     * @param size       Expected size of the page (used for direct I/O check).
     * @return           std::optional<CachedPage> — the cached page if present.
     */
    [[nodiscard]] std::optional<CachedPage> get_page(
        const std::string& file_path,
        off_t offset,
        size_t size = 0);

    /**
     * Insert (or update) a page in the cache.
     *
     * If the key already exists, the existing entry is updated and
     * promoted to MRU.  If inserting would exceed max_pages or
     * max_memory_bytes, LRU eviction is triggered.
     *
     * Pages loaded via Direct I/O are NOT cached unless
     * PageCacheConfig::cache_direct_io is true.  The `direct_io` flag
     * should be set by the caller based on the I/O path used.
     *
     * @param file_path  Path to the disk file.
     * @param offset     Starting byte offset within the file.
     * @param data       Page data (ownership transferred to cache).
     * @param size       Valid size of data in bytes.
     * @param direct_io  True if this page was loaded via Direct I/O.
     */
    void put_page(const std::string& file_path,
                  off_t offset,
                  std::shared_ptr<std::vector<uint8_t>> data,
                  size_t size,
                  bool direct_io = false);

    /**
     * Invalidate (remove) all cached pages belonging to a given file path.
     *
     * This is typically called when a segment file is deleted, compacted,
     * or rotated.  Returns the number of pages evicted.
     *
     * @param file_path  Path whose cached pages should be purged.
     * @return           Number of pages removed.
     */
    size_t invalidate(const std::string& file_path);

    // -- Eviction ----------------------------------------------------------

    /**
     * Manually trigger LRU eviction.
     *
     * Evicts pages until the cache is within its configured limits
     * (max_pages and max_memory_bytes).  Normally called automatically
     * on insertions, but can be called explicitly for maintenance.
     *
     * @return  Number of pages evicted.
     */
    size_t evict();

    // -- Inspection --------------------------------------------------------

    /// Current number of pages in the cache.
    [[nodiscard]] size_t size() const noexcept {
        return page_count_.load(std::memory_order_relaxed);
    }

    /// Total bytes currently stored in the cache.
    [[nodiscard]] size_t memory_used() const noexcept {
        return memory_used_.load(std::memory_order_relaxed);
    }

    /// Maximum configured page count.
    [[nodiscard]] size_t max_pages() const noexcept {
        return config_.max_pages;
    }

    /// Maximum configured memory bytes.
    [[nodiscard]] size_t max_memory_bytes() const noexcept {
        return config_.max_memory_bytes;
    }

    /// True if the cache is empty.
    [[nodiscard]] bool empty() const noexcept {
        return page_count_.load(std::memory_order_relaxed) == 0;
    }

    // -- Statistics --------------------------------------------------------

    /// Hit count since construction or last reset.
    [[nodiscard]] uint64_t hit_count() const noexcept {
        return hits_.load(std::memory_order_relaxed);
    }

    /// Miss count since construction or last reset.
    [[nodiscard]] uint64_t miss_count() const noexcept {
        return misses_.load(std::memory_order_relaxed);
    }

    /// Eviction count since construction or last reset.
    [[nodiscard]] uint64_t eviction_count() const noexcept {
        return evictions_.load(std::memory_order_relaxed);
    }

    /// Hit ratio as a fraction [0.0, 1.0].
    [[nodiscard]] double hit_ratio() const noexcept;

    /// Reset all statistics counters.
    void reset_stats() noexcept;

    /// Remove all entries.  Returns number of pages cleared.
    size_t clear();

    // -- Configuration -----------------------------------------------------

    /// Read-only access to the current config.
    [[nodiscard]] const PageCacheConfig& config() const noexcept {
        return config_;
    }

    /// Dynamically update max_pages.  May trigger eviction.
    void set_max_pages(size_t max_pages);

    /// Dynamically update max_memory_bytes.  May trigger eviction.
    void set_max_memory_bytes(size_t bytes);

    // -- Advanced operations -----------------------------------------------

    /// Preload a range of pages from disk into the cache (bulk warming).
    /// @param fd  Open file descriptor for reading.
    /// @return    Number of pages loaded.
    size_t preload_range(const std::string& file_path, int fd,
                         off_t offset, size_t size, bool direct_io = false);

    /// Check whether the cache is within healthy limits.
    [[nodiscard]] bool is_healthy() const noexcept;

    /// Shrink the cache by evicting at most `count` oldest pages.
    /// Returns the number of pages actually evicted.
    size_t shrink(size_t count);

    /// Return the number of cache hits for pages from a specific file.
    [[nodiscard]] uint64_t file_hit_count(const std::string& file_path) const;

    /// Collect cache-wide statistics for monitoring.
    [[nodiscard]] PageCacheStats stats() const;

private:
    // -- Internal types ----------------------------------------------------

    using LruList = std::list<PageKey>;
    using LruMap  = std::unordered_map<PageKey,
                                       std::pair<CachedPage, LruList::iterator>>;

    // -- Internal helpers --------------------------------------------------

    /**
     * Choose the page size to use for a given request.  Rounds up to
     * config_.page_size if the requested size is smaller.
     */
    [[nodiscard]] size_t effective_page_size(size_t requested_size) const noexcept;

    /**
     * Check whether a page should be cached given its size and origin.
     * Direct I/O pages are skipped unless cache_direct_io is enabled.
     */
    [[nodiscard]] bool should_cache(size_t size, bool direct_io) const noexcept;

    /**
     * Promote a page to MRU (move to front of LRU list).
     * Must be called with the appropriate lock held.
     */
    void promote_locked(const PageKey& key);

    /**
     * Evict a single LRU page.  Returns true if a page was evicted.
     * Must be called with exclusive lock held.
     */
    bool evict_one_locked();

    /**
     * Evict pages until within limits.  Returns number evicted.
     * Must be called with exclusive lock held.
     */
    size_t evict_locked(size_t target_pages, size_t target_bytes);

    /**
     * Recompute the total memory_used_ from the current cache contents.
     * Must be called with exclusive lock held.
     */
    void recompute_memory_locked();

    // -- Members -----------------------------------------------------------

    PageCacheConfig             config_;

    mutable std::shared_mutex   mutex_;
    LruList                     lru_list_;
    LruMap                      cache_;

    std::atomic<size_t>         page_count_{0};
    std::atomic<size_t>         memory_used_{0};
    std::atomic<uint64_t>       hits_{0};
    std::atomic<uint64_t>       misses_{0};
    std::atomic<uint64_t>       evictions_{0};
    std::atomic<timestamp_ms_t> last_eviction_time_{0};
};

} // namespace torrent
