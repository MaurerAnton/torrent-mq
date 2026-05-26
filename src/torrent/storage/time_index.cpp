/**
 * torrent-mq — TimeIndex Implementation
 *
 * Timestamp-to-offset sparse index for time-based log queries.
 * Each entry maps a timestamp (milliseconds since epoch) to the
 * smallest logical offset whose record has that timestamp or later.
 *
 * Entries are kept sorted by timestamp, enabling binary search for:
 *   - lookup(timestamp)       — find offset at or after a given timestamp
 *   - upper_bound / lower_bound — range queries by time
 *   - truncate_to(offset)     — prune entries whose mapped offset
 *                               is at or beyond a threshold
 *
 * Thread safety: all public methods acquire a shared mutex; readers
 * can query concurrently while writers serialise.
 */

#include "torrent/storage/time_index.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <vector>

namespace torrent {

// ============================================================================
// Anonymous namespace — file-scope helpers
// ============================================================================

namespace {

std::shared_ptr<spdlog::logger> get_logger() {
    static auto logger = spdlog::get("time_index");
    if (!logger) {
        logger = spdlog::stdout_color_mt("time_index");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

/**
 * Binary search for the largest entry with timestamp <= target.
 * Returns index into `entries`, or std::nullopt if none qualify.
 */
[[nodiscard]] std::optional<size_t> binary_search_le(
    const std::vector<TimeIndexEntry>& entries,
    timestamp_ms_t target) noexcept
{
    if (entries.empty() || target < entries.front().timestamp) {
        return std::nullopt;
    }

    auto it = std::upper_bound(
        entries.begin(), entries.end(), target,
        [](timestamp_ms_t ts, const TimeIndexEntry& e) noexcept {
            return ts < e.timestamp;
        });

    if (it == entries.begin()) {
        return std::nullopt;
    }

    return static_cast<size_t>(std::distance(entries.begin(), it) - 1);
}

/**
 * Binary search for the smallest entry with timestamp >= target.
 * Returns the index of the first qualifying entry, or entries.size()
 * if no entry has timestamp >= target.
 */
[[nodiscard]] size_t binary_search_ge(
    const std::vector<TimeIndexEntry>& entries,
    timestamp_ms_t target) noexcept
{
    auto it = std::lower_bound(
        entries.begin(), entries.end(), target,
        [](const TimeIndexEntry& e, timestamp_ms_t ts) noexcept {
            return e.timestamp < ts;
        });

    return static_cast<size_t>(std::distance(entries.begin(), it));
}

} // anonymous namespace

// ============================================================================
// TimeIndex — Construction / Destruction
// ============================================================================

TimeIndex::TimeIndex()
    : entry_count_(0)
    , max_timestamp_(0)
{
    get_logger()->debug("TimeIndex({}) constructed", static_cast<void*>(this));
}

TimeIndex::TimeIndex(TimeIndex&& other) noexcept
    : entries_(std::move(other.entries_))
    , entry_count_(other.entry_count_.load(std::memory_order_acquire))
    , max_timestamp_(other.max_timestamp_.load(std::memory_order_acquire))
{
    other.entry_count_.store(0, std::memory_order_release);
    other.max_timestamp_.store(0, std::memory_order_release);
    get_logger()->debug("TimeIndex moved: {} -> {}",
                        static_cast<void*>(&other), static_cast<void*>(this));
}

TimeIndex& TimeIndex::operator=(TimeIndex&& other) noexcept {
    if (this != &other) {
        std::unique_lock<std::shared_mutex> lock_this(mutex_);
        std::unique_lock<std::shared_mutex> lock_other(other.mutex_);
        entries_ = std::move(other.entries_);
        entry_count_.store(other.entry_count_.load(std::memory_order_acquire),
                           std::memory_order_release);
        max_timestamp_.store(other.max_timestamp_.load(std::memory_order_acquire),
                             std::memory_order_release);
        other.entry_count_.store(0, std::memory_order_release);
        other.max_timestamp_.store(0, std::memory_order_release);
    }
    return *this;
}

TimeIndex::~TimeIndex() {
    get_logger()->debug("TimeIndex({}) destroyed, {} entries",
                        static_cast<void*>(this),
                        entry_count_.load(std::memory_order_relaxed));
}

// ============================================================================
// TimeIndex — Entry management
// ============================================================================

void TimeIndex::add_entry(timestamp_ms_t timestamp, offset_t offset) {
    TimeIndexEntry entry;
    entry.timestamp = timestamp;
    entry.offset    = offset;

    std::unique_lock<std::shared_mutex> lock(mutex_);

    // Maintain sorted order by timestamp.
    // Most calls are monotonic (increasing timestamp), check back first.
    if (!entries_.empty() && timestamp < entries_.back().timestamp) {
        // Out-of-order insert — binary search for insertion point.
        auto it = std::lower_bound(
            entries_.begin(), entries_.end(), entry,
            [](const TimeIndexEntry& a, const TimeIndexEntry& b) noexcept {
                return a.timestamp < b.timestamp;
            });

        if (it != entries_.end() && it->timestamp == timestamp) {
            // Replace existing entry with the same timestamp.
            // Preserve the smaller offset (earlier position in the log).
            if (offset < it->offset) {
                get_logger()->debug("TimeIndex: updating entry at ts={}, "
                                    "offset {} -> {}",
                                    timestamp, it->offset, offset);
                it->offset = offset;
            }
        } else {
            entries_.insert(it, entry);
            entry_count_.store(entries_.size(), std::memory_order_release);
            get_logger()->debug("TimeIndex: inserted ts={}, offset={}, total={}",
                                timestamp, offset, entries_.size());
        }
    } else if (!entries_.empty() && timestamp == entries_.back().timestamp) {
        // Same timestamp as last entry; keep the smaller offset.
        if (offset < entries_.back().offset) {
            get_logger()->debug("TimeIndex: updating back entry ts={}, offset={}",
                                timestamp, offset);
            entries_.back().offset = offset;
        }
    } else {
        // Fast path: append to end (monotonic case).
        entries_.push_back(entry);
        entry_count_.store(entries_.size(), std::memory_order_release);
    }

    // Track the maximum timestamp seen (lock-free CAS loop).
    timestamp_ms_t current_max = max_timestamp_.load(std::memory_order_acquire);
    while (timestamp > current_max) {
        if (max_timestamp_.compare_exchange_weak(current_max, timestamp,
                                                  std::memory_order_release,
                                                  std::memory_order_relaxed)) {
            break;
        }
    }

    // Prune if we've accumulated too many entries to keep lookups fast.
    constexpr size_t kMaxEntries = 50000;
    if (entries_.size() > kMaxEntries) {
        prune_half();
    }
}

// ============================================================================
// TimeIndex — Lookup
// ============================================================================

std::optional<offset_t> TimeIndex::lookup(timestamp_ms_t target_timestamp) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    // Find the smallest entry with timestamp >= target_timestamp.
    // This gives the offset at or after the requested timestamp.
    auto idx = binary_search_ge(entries_, target_timestamp);
    if (idx >= entries_.size()) {
        get_logger()->debug("TimeIndex::lookup(ts={}): no entry at or after",
                            target_timestamp);
        return std::nullopt;
    }

    offset_t off = entries_[idx].offset;
    get_logger()->trace("TimeIndex::lookup(ts={}) -> offset={} (entry ts={})",
                        target_timestamp, off, entries_[idx].timestamp);
    return off;
}

std::optional<offset_t> TimeIndex::upper_bound(timestamp_ms_t target_timestamp) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    auto idx = binary_search_ge(entries_, target_timestamp);
    if (idx >= entries_.size()) {
        return std::nullopt;
    }
    return entries_[idx].offset;
}

std::optional<offset_t> TimeIndex::lower_bound(timestamp_ms_t target_timestamp) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    auto idx = binary_search_le(entries_, target_timestamp);
    if (!idx.has_value()) {
        return std::nullopt;
    }
    return entries_[*idx].offset;
}

// ============================================================================
// TimeIndex — Truncation
// ============================================================================

void TimeIndex::truncate_to(offset_t cutoff_offset) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    if (entries_.empty()) return;

    // Find the first entry whose offset is >= cutoff_offset.
    // Since entries are sorted by timestamp (not offset), we scan from
    // the beginning. In practice, monotonic append means timestamps and
    // offsets increase together, so the cut point is usually near the start.
    auto it = entries_.begin();
    while (it != entries_.end() && it->offset < cutoff_offset) {
        ++it;
    }

    if (it == entries_.end()) {
        // All entries are below cutoff — clear everything.
        get_logger()->info("TimeIndex::truncate_to(offset={}): clearing all {} entries",
                           cutoff_offset, entries_.size());
        entries_.clear();
    } else if (it != entries_.begin()) {
        size_t removed = static_cast<size_t>(std::distance(entries_.begin(), it));
        get_logger()->info("TimeIndex::truncate_to(offset={}): removing {} entries"
                           " (last removed: ts={}, offset={})",
                           cutoff_offset, removed,
                           (it - 1)->timestamp, (it - 1)->offset);
        entries_.erase(entries_.begin(), it);
    }
    // else: no entries have offset < cutoff_offset — nothing to remove.

    entry_count_.store(entries_.size(), std::memory_order_release);

    // Recompute max_timestamp from remaining entries.
    if (!entries_.empty()) {
        max_timestamp_.store(entries_.back().timestamp, std::memory_order_release);
    } else {
        max_timestamp_.store(0, std::memory_order_release);
    }
}

// ============================================================================
// TimeIndex — Accessors
// ============================================================================

size_t TimeIndex::size() const noexcept {
    return entry_count_.load(std::memory_order_acquire);
}

bool TimeIndex::empty() const noexcept {
    return entry_count_.load(std::memory_order_acquire) == 0;
}

timestamp_ms_t TimeIndex::max_timestamp() const noexcept {
    return max_timestamp_.load(std::memory_order_acquire);
}

std::optional<timestamp_ms_t> TimeIndex::first_timestamp() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (entries_.empty()) return std::nullopt;
    return entries_.front().timestamp;
}

std::optional<TimeIndexEntry> TimeIndex::entry_at(size_t idx) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (idx >= entries_.size()) return std::nullopt;
    return entries_[idx];
}

std::vector<TimeIndexEntry> TimeIndex::entries() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return entries_;
}

void TimeIndex::clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    get_logger()->debug("TimeIndex::clear(): discarding {} entries", entries_.size());
    std::vector<TimeIndexEntry>().swap(entries_); // release memory
    entry_count_.store(0, std::memory_order_release);
    max_timestamp_.store(0, std::memory_order_release);
}

// ============================================================================
// TimeIndex — Internal maintenance
// ============================================================================

void TimeIndex::prune_half() {
    size_t old_size = entries_.size();
    if (old_size <= 2) return;

    std::vector<TimeIndexEntry> pruned;
    pruned.reserve(old_size / 2 + 2);

    // Keep first entry (earliest timestamp) as anchor.
    pruned.push_back(entries_.front());

    // Keep every 2nd entry from the middle range.
    for (size_t i = 2; i < old_size - 1; i += 2) {
        pruned.push_back(entries_[i]);
    }

    // Ensure we keep the last entry (latest timestamp).
    if (pruned.back().timestamp != entries_.back().timestamp) {
        pruned.push_back(entries_.back());
    }

    size_t removed = old_size - pruned.size();
    entries_ = std::move(pruned);
    entry_count_.store(entries_.size(), std::memory_order_release);

    get_logger()->info("TimeIndex::prune_half(): removed {} entries, {} remain",
                       removed, entries_.size());
}

} // namespace torrent
