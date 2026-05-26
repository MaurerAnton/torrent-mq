/**
 * torrent-mq — OffsetIndex Implementation
 *
 * Sparse offset-to-file-position index for O(log N) lookups within a
 * segment.  Each entry maps a logical offset to a physical byte position
 * in the segment data file.
 *
 * Entries are kept sorted by offset, enabling binary search for:
 *   - lookup(offset)    — exact or nearest-smaller position
 *   - upper_bound / lower_bound — range queries
 *   - truncate_to(offset) — prune entries at and beyond a threshold
 *
 * Thread safety: all public methods acquire a shared mutex; readers
 * can query concurrently while writers serialise.
 */

#include "torrent/storage/offset_index.h"
#include "torrent/storage/segment.h"
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
    static auto logger = spdlog::get("offset_index");
    if (!logger) {
        logger = spdlog::stdout_color_mt("offset_index");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

/**
 * Binary search for the largest entry with offset <= target.
 * Returns index into `entries`, or std::nullopt if none qualify.
 *
 * Precondition: entries is sorted by offset ascending.
 */
[[nodiscard]] std::optional<size_t> binary_search_le(
    const std::vector<SparseIndexEntry>& entries,
    offset_t target) noexcept
{
    if (entries.empty() || target < entries.front().offset) {
        return std::nullopt;
    }

    // std::upper_bound gives first entry with offset > target.
    // The entry before that is the one we want.
    auto it = std::upper_bound(
        entries.begin(), entries.end(), target,
        [](offset_t off, const SparseIndexEntry& e) noexcept {
            return off < e.offset;
        });

    if (it == entries.begin()) {
        // All entries have offset > target (handled above but belt-and-suspenders).
        return std::nullopt;
    }

    return static_cast<size_t>(std::distance(entries.begin(), it) - 1);
}

/**
 * Binary search for the smallest entry with offset >= target.
 * Returns index into `entries`, or entries.size() if all are smaller.
 */
[[nodiscard]] size_t binary_search_ge(
    const std::vector<SparseIndexEntry>& entries,
    offset_t target) noexcept
{
    auto it = std::lower_bound(
        entries.begin(), entries.end(), target,
        [](const SparseIndexEntry& e, offset_t off) noexcept {
            return e.offset < off;
        });

    return static_cast<size_t>(std::distance(entries.begin(), it));
}

} // anonymous namespace

// ============================================================================
// OffsetIndex — Construction / Destruction
// ============================================================================

OffsetIndex::OffsetIndex()
    : entry_count_(0)
{
    get_logger()->debug("OffsetIndex({}) constructed", static_cast<void*>(this));
}

OffsetIndex::OffsetIndex(OffsetIndex&& other) noexcept
    : entries_(std::move(other.entries_))
    , entry_count_(other.entry_count_.load(std::memory_order_acquire))
{
    other.entry_count_.store(0, std::memory_order_release);
    get_logger()->debug("OffsetIndex moved: {} -> {}",
                        static_cast<void*>(&other), static_cast<void*>(this));
}

OffsetIndex& OffsetIndex::operator=(OffsetIndex&& other) noexcept {
    if (this != &other) {
        std::unique_lock<std::shared_mutex> lock_this(mutex_);
        std::unique_lock<std::shared_mutex> lock_other(other.mutex_);
        entries_ = std::move(other.entries_);
        entry_count_.store(other.entry_count_.load(std::memory_order_acquire),
                           std::memory_order_release);
        other.entry_count_.store(0, std::memory_order_release);
    }
    return *this;
}

OffsetIndex::~OffsetIndex() {
    get_logger()->debug("OffsetIndex({}) destroyed, {} entries",
                        static_cast<void*>(this),
                        entry_count_.load(std::memory_order_relaxed));
}

// ============================================================================
// OffsetIndex — Entry management
// ============================================================================

void OffsetIndex::add_entry(offset_t offset, byte_count_t file_position,
                            timestamp_ms_t timestamp) {
    SparseIndexEntry entry;
    entry.offset        = offset;
    entry.file_position = file_position;
    entry.timestamp     = timestamp;

    std::unique_lock<std::shared_mutex> lock(mutex_);

    // Insert maintaining sorted order by offset.
    // Most appends go to the end, so check the back first.
    if (!entries_.empty() && offset < entries_.back().offset) {
        // Out-of-order insert — find the correct insertion point.
        auto it = std::lower_bound(
            entries_.begin(), entries_.end(), entry,
            [](const SparseIndexEntry& a, const SparseIndexEntry& b) noexcept {
                return a.offset < b.offset;
            });

        // If an entry for this exact offset already exists, replace it.
        if (it != entries_.end() && it->offset == offset) {
            *it = entry;
            get_logger()->debug("OffsetIndex: replaced entry at offset={}", offset);
        } else {
            entries_.insert(it, entry);
            entry_count_.store(entries_.size(), std::memory_order_release);
            get_logger()->debug("OffsetIndex: inserted entry at offset={}, total={}",
                                offset, entries_.size());
        }
    } else if (!entries_.empty() && offset == entries_.back().offset) {
        // Replace the last entry (same offset, newer timestamp / position).
        entries_.back() = entry;
        get_logger()->debug("OffsetIndex: replaced back entry at offset={}", offset);
    } else {
        // Fast path: append to end.
        entries_.push_back(entry);
        entry_count_.store(entries_.size(), std::memory_order_release);

        // Limit in-memory entries to avoid unbounded growth.
        // Keep entries evenly distributed when pruning.
        constexpr size_t kMaxEntries = 50000;
        if (entries_.size() > kMaxEntries) {
            prune_half();
        }
    }
}

void OffsetIndex::add_entries(const std::vector<SparseIndexEntry>& new_entries) {
    if (new_entries.empty()) return;

    std::unique_lock<std::shared_mutex> lock(mutex_);

    // If new entries are all beyond our current range, append them.
    if (entries_.empty() || new_entries.front().offset > entries_.back().offset) {
        entries_.insert(entries_.end(), new_entries.begin(), new_entries.end());
    } else {
        // Merge: insert each entry maintaining sorted order.
        for (const auto& e : new_entries) {
            auto it = std::lower_bound(
                entries_.begin(), entries_.end(), e,
                [](const SparseIndexEntry& a, const SparseIndexEntry& b) noexcept {
                    return a.offset < b.offset;
                });
            if (it != entries_.end() && it->offset == e.offset) {
                *it = e;
            } else {
                entries_.insert(it, e);
            }
        }
    }

    entry_count_.store(entries_.size(), std::memory_order_release);
    get_logger()->debug("OffsetIndex: batch-added {} entries, total={}",
                        new_entries.size(), entries_.size());
}

// ============================================================================
// OffsetIndex — Lookup
// ============================================================================

std::optional<byte_count_t> OffsetIndex::lookup(offset_t target_offset) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    auto idx = binary_search_le(entries_, target_offset);
    if (!idx.has_value()) {
        get_logger()->debug("OffsetIndex::lookup({}): not found (index empty or "
                            "target before first entry)", target_offset);
        return std::nullopt;
    }

    byte_count_t pos = entries_[*idx].file_position;
    get_logger()->trace("OffsetIndex::lookup({}) -> position={}", target_offset, pos);
    return pos;
}

std::optional<byte_count_t> OffsetIndex::upper_bound(offset_t target_offset) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    auto idx = binary_search_ge(entries_, target_offset);
    if (idx >= entries_.size()) {
        return std::nullopt;
    }
    return entries_[idx].file_position;
}

std::optional<byte_count_t> OffsetIndex::lower_bound(offset_t target_offset) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    auto idx = binary_search_le(entries_, target_offset);
    if (!idx.has_value()) {
        return std::nullopt;
    }

    // If the found entry has an offset strictly less than the target and
    // there is a next entry, return the next entry's position (which is
    // the first entry >= target).
    if (entries_[*idx].offset < target_offset && *idx + 1 < entries_.size()) {
        return entries_[*idx + 1].file_position;
    }

    return entries_[*idx].file_position;
}

// ============================================================================
// OffsetIndex — Truncation
// ============================================================================

void OffsetIndex::truncate_to(offset_t cutoff_offset) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    if (entries_.empty()) return;

    // Find the first entry with offset >= cutoff_offset.
    auto it = std::lower_bound(
        entries_.begin(), entries_.end(), cutoff_offset,
        [](const SparseIndexEntry& e, offset_t off) noexcept {
            return e.offset < off;
        });

    if (it == entries_.begin()) {
        // All entries are at or above cutoff — clear everything.
        get_logger()->info("OffsetIndex::truncate_to({}): clearing all {} entries",
                           cutoff_offset, entries_.size());
        entries_.clear();
    } else if (it != entries_.end()) {
        size_t removed = static_cast<size_t>(std::distance(it, entries_.end()));
        get_logger()->info("OffsetIndex::truncate_to({}): removing {} entries "
                           "(from offset {})",
                           cutoff_offset, removed, it->offset);
        entries_.erase(it, entries_.end());
    }
    // else: all entries are below cutoff_offset — nothing to do.

    entry_count_.store(entries_.size(), std::memory_order_release);
}

// ============================================================================
// OffsetIndex — Accessors
// ============================================================================

size_t OffsetIndex::size() const noexcept {
    return entry_count_.load(std::memory_order_acquire);
}

bool OffsetIndex::empty() const noexcept {
    return entry_count_.load(std::memory_order_acquire) == 0;
}

std::optional<offset_t> OffsetIndex::first_offset() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (entries_.empty()) return std::nullopt;
    return entries_.front().offset;
}

std::optional<offset_t> OffsetIndex::last_offset() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (entries_.empty()) return std::nullopt;
    return entries_.back().offset;
}

std::optional<SparseIndexEntry> OffsetIndex::entry_at(size_t idx) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (idx >= entries_.size()) return std::nullopt;
    return entries_[idx];
}

std::vector<SparseIndexEntry> OffsetIndex::entries() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return entries_;
}

void OffsetIndex::clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    get_logger()->debug("OffsetIndex::clear(): discarding {} entries", entries_.size());
    entries_.clear();
    std::vector<SparseIndexEntry>().swap(entries_); // release memory
    entry_count_.store(0, std::memory_order_release);
}

// ============================================================================
// OffsetIndex — Internal maintenance
// ============================================================================

void OffsetIndex::prune_half() {
    // Keep every other entry to halve the index size while preserving
    // approximate O(log N) lookup performance.
    //
    // Always keep the first and last entries.
    size_t old_size = entries_.size();
    if (old_size <= 2) return;

    std::vector<SparseIndexEntry> pruned;
    pruned.reserve(old_size / 2 + 2);

    // Keep first entry.
    pruned.push_back(entries_.front());

    // Keep every 2nd entry from the middle.
    for (size_t i = 2; i < old_size - 1; i += 2) {
        pruned.push_back(entries_[i]);
    }

    // Ensure we keep the last entry.
    if (pruned.back().offset != entries_.back().offset) {
        pruned.push_back(entries_.back());
    }

    size_t removed = old_size - pruned.size();
    entries_ = std::move(pruned);
    entry_count_.store(entries_.size(), std::memory_order_release);

    get_logger()->info("OffsetIndex::prune_half(): removed {} entries, {} remain",
                       removed, entries_.size());
}

} // namespace torrent
