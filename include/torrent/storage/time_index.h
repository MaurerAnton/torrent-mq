#pragma once

/**
 * torrent-mq — TimeIndex
 *
 * Timestamp-to-offset sparse index for time-based log queries.
 * Each entry maps a timestamp (milliseconds since epoch) to the
 * smallest logical offset whose record batch has that timestamp.
 *
 * Entries are kept sorted by timestamp for O(log N) binary search.
 *
 * Thread safety: readers use shared_lock, writers use exclusive_lock.
 */

#include <cstdint>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <vector>

#include "torrent/common/types.h"

namespace torrent {

/**
 * A single entry in the time-based sparse index.
 * Maps a record batch's maximum timestamp to its base offset.
 */
struct TimeIndexEntry {
    timestamp_ms_t timestamp = 0;
    offset_t       offset    = kInvalidOffset;

    [[nodiscard]] bool operator<(const TimeIndexEntry& o) const noexcept {
        return timestamp < o.timestamp;
    }
    [[nodiscard]] bool operator<(timestamp_ms_t ts) const noexcept {
        return timestamp < ts;
    }
};

inline bool operator<(timestamp_ms_t ts, const TimeIndexEntry& entry) noexcept {
    return ts < entry.timestamp;
}

class TimeIndex {
public:
    TimeIndex();
    TimeIndex(TimeIndex&& other) noexcept;
    TimeIndex& operator=(TimeIndex&& other) noexcept;
    TimeIndex(const TimeIndex&) = delete;
    TimeIndex& operator=(const TimeIndex&) = delete;
    ~TimeIndex();

    // -- Entry management -------------------------------------------------

    /// Insert or update a timestamp→offset entry.
    void add_entry(timestamp_ms_t timestamp, offset_t offset);

    // -- Lookup -----------------------------------------------------------

    /// Find offset for the largest timestamp <= target_timestamp.
    [[nodiscard]] std::optional<offset_t> lookup(timestamp_ms_t target_timestamp) const;

    /// Find offset for the smallest timestamp >= target_timestamp.
    [[nodiscard]] std::optional<offset_t> upper_bound(timestamp_ms_t target_timestamp) const;

    /// Find offset for the largest timestamp <= target_timestamp.
    [[nodiscard]] std::optional<offset_t> lower_bound(timestamp_ms_t target_timestamp) const;

    // -- Truncation -------------------------------------------------------

    /// Remove all entries whose offset is >= cutoff_offset.
    void truncate_to(offset_t cutoff_offset);

    // -- Accessors --------------------------------------------------------

    [[nodiscard]] size_t size() const noexcept;
    [[nodiscard]] bool empty() const noexcept;

    /// Highest timestamp in the index.
    [[nodiscard]] timestamp_ms_t max_timestamp() const noexcept;

    [[nodiscard]] std::optional<timestamp_ms_t> first_timestamp() const;

    /// Get a copy of the entry at index (bounds-checked).
    [[nodiscard]] std::optional<TimeIndexEntry> entry_at(size_t idx) const;

    /// Return a snapshot copy of all entries.
    [[nodiscard]] std::vector<TimeIndexEntry> entries() const;

    /// Clear all entries and release memory.
    void clear();

private:
    /// Halve the index size by keeping every 2nd entry.
    void prune_half();

    std::vector<TimeIndexEntry> entries_;
    std::atomic<size_t>         entry_count_{0};
    std::atomic<timestamp_ms_t> max_timestamp_{0};
    mutable std::shared_mutex   mutex_;
};

} // namespace torrent
