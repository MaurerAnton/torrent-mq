#pragma once

/**
 * torrent-mq — SegmentIndex
 *
 * Sparse index for a single log segment.  Provides both offset→position
 * and timestamp→offset lookups via a single sorted vector of entries.
 *
 * Entries are kept sorted by offset for O(log N) binary search on both
 * offset and timestamp axes.
 *
 * Thread safety: readers use shared_lock, writers use exclusive_lock.
 */

#include <cstdint>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <vector>

#include "torrent/common/types.h"
#include "torrent/storage/segment.h"

namespace torrent {

class SegmentIndex {
public:
    SegmentIndex();
    SegmentIndex(SegmentIndex&& other) noexcept;
    SegmentIndex& operator=(SegmentIndex&& other) noexcept;
    SegmentIndex(const SegmentIndex&) = delete;
    SegmentIndex& operator=(const SegmentIndex&) = delete;
    ~SegmentIndex();

    // -- Entry management -------------------------------------------------

    /// Add an index entry mapping offset→position+timestamp.
    void add_entry(offset_t offset, byte_count_t file_position,
                   timestamp_ms_t timestamp = 0);

    /// Batch-insert entries (sorted by offset, merge with existing).
    void add_entries(const std::vector<SparseIndexEntry>& new_entries);

    // -- Lookup -----------------------------------------------------------

    /// Binary search for the file position of the largest offset <= target.
    [[nodiscard]] std::optional<byte_count_t> find_position(offset_t target_offset) const;

    /// Find the offset of the first entry with timestamp >= target.
    /// Falls back to the last entry if none exceeds the target.
    [[nodiscard]] std::optional<offset_t> find_offset_by_timestamp(
        timestamp_ms_t target_timestamp) const;

    // -- Truncation -------------------------------------------------------

    /// Remove all entries with offset >= cutoff_offset.
    void truncate_to(offset_t cutoff_offset);

    // -- Accessors --------------------------------------------------------

    [[nodiscard]] size_t entry_count() const noexcept;
    [[nodiscard]] bool empty() const noexcept;

    [[nodiscard]] std::optional<SparseIndexEntry> first_entry() const;
    [[nodiscard]] std::optional<SparseIndexEntry> last_entry() const;

    /// Get a copy of the entry at index (bounds-checked).
    [[nodiscard]] std::optional<SparseIndexEntry> entry_at(size_t idx) const;

    /// Return a const reference to internal entries.
    /// WARNING: not thread-safe unless caller holds a lock.
    [[nodiscard]] const std::vector<SparseIndexEntry>& entries() const noexcept;

    /// Return a thread-safe copy of all entries.
    [[nodiscard]] std::vector<SparseIndexEntry> copy_entries() const;

    /// Clear all entries and release memory.
    void clear();

private:
    /// Halve the index size by keeping every 2nd entry.
    void prune_half();

    std::vector<SparseIndexEntry> entries_;
    std::atomic<size_t>           entry_count_{0};
    mutable std::shared_mutex     mutex_;
};

} // namespace torrent
