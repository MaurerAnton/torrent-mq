/**
 * torrent-mq — SegmentIndex Implementation
 *
 * Manages the sparse index within a single log segment. Provides both
 * offset→position and timestamp→offset lookups via dual internal
 * indices (OffsetIndex + TimeIndex).
 *
 * The SegmentIndex is the primary API that segments use for all index
 * operations. It wraps the two specialised indices and ensures they
 * stay consistent.
 *
 * Operations:
 *   - add_entry()              — insert entry into both indices
 *   - find_position()          — binary search offset→position
 *   - find_offset_by_timestamp() — binary search timestamp→offset
 *   - truncate_to()            — prune both indices
 *   - entry_count / clear      — lifecycle management
 *
 * Thread safety: all public methods acquire the internal mutex.
 * Reads use a shared_lock; writes use an exclusive_lock.
 */

#include "torrent/storage/segment_index.h"
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
    static auto logger = spdlog::get("segment_index");
    if (!logger) {
        logger = spdlog::stdout_color_mt("segment_index");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

/**
 * Comparison for SparseIndexEntry used in binary search by offset.
 */
struct offset_less {
    [[nodiscard]] bool operator()(const SparseIndexEntry& e, offset_t off) const noexcept {
        return e.offset < off;
    }
    [[nodiscard]] bool operator()(offset_t off, const SparseIndexEntry& e) const noexcept {
        return off < e.offset;
    }
};

} // anonymous namespace

// ============================================================================
// SegmentIndex — Construction / Destruction
// ============================================================================

SegmentIndex::SegmentIndex()
    : entry_count_(0)
{
    get_logger()->debug("SegmentIndex({}) constructed", static_cast<void*>(this));
}

SegmentIndex::SegmentIndex(SegmentIndex&& other) noexcept
    : entries_(std::move(other.entries_))
    , entry_count_(other.entry_count_.load(std::memory_order_acquire))
{
    other.entry_count_.store(0, std::memory_order_release);
    get_logger()->debug("SegmentIndex moved: {} -> {}",
                        static_cast<void*>(&other), static_cast<void*>(this));
}

SegmentIndex& SegmentIndex::operator=(SegmentIndex&& other) noexcept {
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

SegmentIndex::~SegmentIndex() {
    get_logger()->debug("SegmentIndex({}) destroyed, {} entries",
                        static_cast<void*>(this),
                        entry_count_.load(std::memory_order_relaxed));
}

// ============================================================================
// SegmentIndex — Entry management
// ============================================================================

void SegmentIndex::add_entry(offset_t offset, byte_count_t file_position,
                             timestamp_ms_t timestamp) {
    SparseIndexEntry entry;
    entry.offset        = offset;
    entry.file_position = file_position;
    entry.timestamp     = timestamp;

    std::unique_lock<std::shared_mutex> lock(mutex_);

    // --- Deduplication check ---
    // If an entry with the same offset already exists, skip.
    // This can happen during index rebuild or when multiple batch
    // appends fall within the same index interval.
    if (!entries_.empty()) {
        auto it = std::lower_bound(
            entries_.begin(), entries_.end(), offset,
            [](const SparseIndexEntry& e, offset_t off) noexcept {
                return e.offset < off;
            });
        if (it != entries_.end() && it->offset == offset) {
            // Update the existing entry (newer timestamp / file position).
            it->file_position = file_position;
            it->timestamp     = timestamp;
            get_logger()->trace("SegmentIndex: updated entry at offset={}", offset);
            return;
        }
    }

    // --- Insert maintaining sorted-by-offset order ---
    // Fast path: most appends go to the end.
    if (entries_.empty() || offset > entries_.back().offset) {
        entries_.push_back(entry);
    } else {
        auto it = std::lower_bound(
            entries_.begin(), entries_.end(), offset,
            [](const SparseIndexEntry& e, offset_t off) noexcept {
                return e.offset < off;
            });
        entries_.insert(it, entry);
    }

    entry_count_.store(entries_.size(), std::memory_order_release);

    // Limit in-memory entries.
    constexpr size_t kMaxEntries = 50000;
    if (entries_.size() > kMaxEntries) {
        prune_half();
    }
}

void SegmentIndex::add_entries(const std::vector<SparseIndexEntry>& new_entries) {
    if (new_entries.empty()) return;

    std::unique_lock<std::shared_mutex> lock(mutex_);

    for (const auto& e : new_entries) {
        if (entries_.empty() || e.offset > entries_.back().offset) {
            entries_.push_back(e);
        } else {
            auto it = std::lower_bound(
                entries_.begin(), entries_.end(), e.offset,
                [](const SparseIndexEntry& entry, offset_t off) noexcept {
                    return entry.offset < off;
                });
            if (it != entries_.end() && it->offset == e.offset) {
                *it = e;
            } else {
                entries_.insert(it, e);
            }
        }
    }

    entry_count_.store(entries_.size(), std::memory_order_release);
    get_logger()->debug("SegmentIndex: batch-added {} entries, total={}",
                        new_entries.size(), entries_.size());

    constexpr size_t kMaxEntries = 50000;
    if (entries_.size() > kMaxEntries) {
        prune_half();
    }
}

// ============================================================================
// SegmentIndex — Position lookup (offset → file_position)
// ============================================================================

std::optional<byte_count_t> SegmentIndex::find_position(offset_t target_offset) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    if (entries_.empty()) {
        get_logger()->debug("SegmentIndex::find_position({}): index is empty", target_offset);
        return std::nullopt;
    }

    // Find the largest entry with offset <= target_offset.
    auto it = std::upper_bound(
        entries_.begin(), entries_.end(), target_offset,
        [](offset_t off, const SparseIndexEntry& e) noexcept {
            return off < e.offset;
        });

    if (it == entries_.begin()) {
        // All entries have offset > target_offset.
        get_logger()->debug("SegmentIndex::find_position({}): all entries after target",
                            target_offset);
        return std::nullopt;
    }

    --it; // Now it points to the largest entry with offset <= target_offset.
    get_logger()->trace("SegmentIndex::find_position({}) -> position={} (entry offset={})",
                        target_offset, it->file_position, it->offset);
    return it->file_position;
}

// ============================================================================
// SegmentIndex — Timestamp lookup (timestamp → offset)
// ============================================================================

std::optional<offset_t> SegmentIndex::find_offset_by_timestamp(
    timestamp_ms_t target_timestamp) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);

    if (entries_.empty()) {
        get_logger()->debug("SegmentIndex::find_offset_by_timestamp(ts={}): "
                            "index is empty", target_timestamp);
        return std::nullopt;
    }

    // Find the first entry with timestamp >= target_timestamp.
    // Fall back to entry with max_timestamp <= target_timestamp if none.
    auto it = std::lower_bound(
        entries_.begin(), entries_.end(), target_timestamp,
        [](const SparseIndexEntry& e, timestamp_ms_t ts) noexcept {
            return e.timestamp < ts;
        });

    if (it != entries_.end()) {
        get_logger()->trace("SegmentIndex::find_offset_by_timestamp(ts={}) "
                            "-> offset={} (entry ts={})",
                            target_timestamp, it->offset, it->timestamp);
        return it->offset;
    }

    // No entry has timestamp >= target_timestamp.
    // Return the offset of the last (newest) entry.
    offset_t last_off = entries_.back().offset;
    get_logger()->debug("SegmentIndex::find_offset_by_timestamp(ts={}): "
                        "no entry with ts >= target, returning last offset={}",
                        target_timestamp, last_off);
    return last_off;
}

// ============================================================================
// SegmentIndex — Truncation
// ============================================================================

void SegmentIndex::truncate_to(offset_t cutoff_offset) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    if (entries_.empty()) return;

    auto it = std::lower_bound(
        entries_.begin(), entries_.end(), cutoff_offset,
        [](const SparseIndexEntry& e, offset_t off) noexcept {
            return e.offset < off;
        });

    if (it == entries_.begin()) {
        get_logger()->info("SegmentIndex::truncate_to({}): clearing all {} entries",
                           cutoff_offset, entries_.size());
        entries_.clear();
    } else if (it != entries_.end()) {
        size_t removed = static_cast<size_t>(std::distance(it, entries_.end()));
        get_logger()->info("SegmentIndex::truncate_to({}): removing {} entries "
                           "(from offset {})",
                           cutoff_offset, removed, it->offset);
        entries_.erase(it, entries_.end());
    }

    entry_count_.store(entries_.size(), std::memory_order_release);
}

// ============================================================================
// SegmentIndex — Accessors
// ============================================================================

size_t SegmentIndex::entry_count() const noexcept {
    return entry_count_.load(std::memory_order_acquire);
}

bool SegmentIndex::empty() const noexcept {
    return entry_count_.load(std::memory_order_acquire) == 0;
}

std::optional<SparseIndexEntry> SegmentIndex::first_entry() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (entries_.empty()) return std::nullopt;
    return entries_.front();
}

std::optional<SparseIndexEntry> SegmentIndex::last_entry() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (entries_.empty()) return std::nullopt;
    return entries_.back();
}

std::optional<SparseIndexEntry> SegmentIndex::entry_at(size_t idx) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (idx >= entries_.size()) return std::nullopt;
    return entries_[idx];
}

const std::vector<SparseIndexEntry>& SegmentIndex::entries() const noexcept {
    // NOTE: This returns a reference to internal state. Callers must
    // hold their own lock or accept that the vector may be mutated
    // concurrently. For read-only snapshots, use copy_entries().
    return entries_;
}

std::vector<SparseIndexEntry> SegmentIndex::copy_entries() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return entries_;
}

void SegmentIndex::clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    get_logger()->debug("SegmentIndex::clear(): discarding {} entries", entries_.size());
    entries_.clear();
    std::vector<SparseIndexEntry>().swap(entries_); // release memory
    entry_count_.store(0, std::memory_order_release);
}

// ============================================================================
// SegmentIndex — Internal maintenance
// ============================================================================

void SegmentIndex::prune_half() {
    size_t old_size = entries_.size();
    if (old_size <= 2) return;

    std::vector<SparseIndexEntry> pruned;
    pruned.reserve(old_size / 2 + 2);

    // Keep first entry (earliest offset).
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

    get_logger()->info("SegmentIndex::prune_half(): removed {} entries, {} remain",
                       removed, entries_.size());
}

} // namespace torrent
