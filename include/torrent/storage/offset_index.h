#pragma once

/**
 * torrent-mq — OffsetIndex
 *
 * Sparse offset-to-file-position index for O(log N) lookups within a
 * log segment.  Each entry maps a logical offset to a physical byte
 * position in the segment data file.
 *
 * Thread safety: readers use shared_lock (concurrent reads allowed),
 * writers use exclusive_lock.
 */

#include <cstdint>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <vector>

#include "torrent/common/types.h"
#include "torrent/storage/segment.h"

namespace torrent {

class OffsetIndex {
public:
    OffsetIndex();
    OffsetIndex(OffsetIndex&& other) noexcept;
    OffsetIndex& operator=(OffsetIndex&& other) noexcept;
    OffsetIndex(const OffsetIndex&) = delete;
    OffsetIndex& operator=(const OffsetIndex&) = delete;
    ~OffsetIndex();

    // -- Entry management -------------------------------------------------

    /// Insert or update an offset→position entry.
    void add_entry(offset_t offset, byte_count_t file_position,
                   timestamp_ms_t timestamp = 0);

    /// Batch-insert entries (sorted by offset, merge with existing).
    void add_entries(const std::vector<SparseIndexEntry>& new_entries);

    // -- Lookup -----------------------------------------------------------

    /// Find file position for the largest offset <= target_offset.
    [[nodiscard]] std::optional<byte_count_t> lookup(offset_t target_offset) const;

    /// Find file position for the smallest offset >= target_offset.
    [[nodiscard]] std::optional<byte_count_t> upper_bound(offset_t target_offset) const;

    /// Find file position for the largest offset <= target_offset,
    /// returning the next entry if the found entry is strictly less.
    [[nodiscard]] std::optional<byte_count_t> lower_bound(offset_t target_offset) const;

    // -- Truncation -------------------------------------------------------

    /// Remove all entries with offset >= cutoff_offset.
    void truncate_to(offset_t cutoff_offset);

    // -- Accessors --------------------------------------------------------

    [[nodiscard]] size_t size() const noexcept;
    [[nodiscard]] bool empty() const noexcept;

    [[nodiscard]] std::optional<offset_t> first_offset() const;
    [[nodiscard]] std::optional<offset_t> last_offset() const;

    /// Get a copy of the entry at index (bounds-checked).
    [[nodiscard]] std::optional<SparseIndexEntry> entry_at(size_t idx) const;

    /// Return a snapshot copy of all entries.
    [[nodiscard]] std::vector<SparseIndexEntry> entries() const;

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
