/**
 * torrent-mq — I/O buffer implementation (scatter-gather, zero-copy)
 *
 * Implements the iobuf class declared in torrent/common/iobuf.h.
 * iobuf is a chain of reference-counted, immutable fragments used
 * for constructing and consuming network protocol messages without
 * unnecessary copies. Write operations append new fragments; read
 * operations trim from the front. Zero-copy slicing is supported
 * via share().
 */

#include "torrent/common/iobuf.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstring>
#include <string_view>
#include <utility>

namespace torrent {

// ============================================================================
// Constructors
// ============================================================================

/**
 * Construct from a raw pointer + size (copies the data).
 *
 * Allocates a new reference-counted buffer, copies the caller's data
 * into it, and wraps it in a single fragment.
 */
iobuf::iobuf(const char* data, size_t size) {
    if (data == nullptr || size == 0) {
        return;
    }

    // Allocate a reference-counted buffer and copy the data
    auto buf = std::make_shared<char[]>(size);
    std::memcpy(buf.get(), data, size);

    fragments_.emplace_back(std::move(buf), 0, size);
    total_size_ = size;
}

/**
 * Construct from a std::string (copies the data).
 *
 * Delegates to the raw-pointer constructor to avoid duplicating
 * the allocation and copy logic.
 */
iobuf::iobuf(const std::string& str)
    : iobuf(str.data(), str.size()) {}

/**
 * Construct from a vector<uint8_t> (copies the data via memcpy).
 *
 * The vector is consumed by value so the caller can std::move if desired.
 * A new shared_ptr buffer is allocated and the vector contents are
 * memcpy'd into it — the vector itself is not held.
 */
iobuf::iobuf(std::vector<uint8_t> data) {
    if (data.empty()) {
        return;
    }

    auto buf = std::make_shared<char[]>(data.size());
    std::memcpy(buf.get(), data.data(), data.size());

    fragments_.emplace_back(std::move(buf), 0, data.size());
    total_size_ = data.size();
}

// ============================================================================
// append() — Add data to the end of the buffer
// ============================================================================

/**
 * Append raw bytes (copies data into a new fragment).
 *
 * Allocates a fresh reference-counted buffer and appends it as a
 * new fragment. Existing fragments are never modified.
 */
void iobuf::append(const char* data, size_t size) {
    if (data == nullptr || size == 0) {
        return;
    }

    auto buf = std::make_shared<char[]>(size);
    std::memcpy(buf.get(), data, size);

    fragments_.emplace_back(std::move(buf), 0, size);
    total_size_ += size;
}

/**
 * Append a std::string (delegates to append(data, size)).
 */
void iobuf::append(const std::string& str) {
    append(str.data(), str.size());
}

/**
 * Append another iobuf (splices its fragments in without copying).
 *
 * The other buffer's fragments are moved into this one, so the
 * operation is O(number of fragments in `other`). After the call,
 * `other` is left empty.
 */
void iobuf::append(iobuf other) {
    if (other.empty()) {
        return;
    }

    total_size_ += other.total_size_;
    fragments_.insert(fragments_.end(),
                      std::make_move_iterator(other.fragments_.begin()),
                      std::make_move_iterator(other.fragments_.end()));

    other.fragments_.clear();
    other.total_size_ = 0;
}

// ============================================================================
// trim_front() — Consume bytes from the beginning
// ============================================================================

/**
 * Consume up to `n` bytes from the front of the buffer.
 *
 * Iterates through fragments, advancing offsets or removing them
 * entirely until `n` bytes have been consumed. If n >= total_size_,
 * the buffer becomes empty (equivalent to clear()).
 */
void iobuf::trim_front(size_t n) {
    if (n == 0) return;

    if (n >= total_size_) {
        clear();
        return;
    }

    size_t remaining = n;
    while (remaining > 0 && !fragments_.empty()) {
        auto& f = fragments_.front();
        if (remaining >= f.size) {
            // Consume the entire fragment
            remaining -= f.size;
            fragments_.erase(fragments_.begin());
        } else {
            // Consume part of the fragment — advance its offset
            f.offset += remaining;
            f.size -= remaining;
            remaining = 0;
        }
    }

    total_size_ -= n;
}

/**
 * Clear all fragments and reset the size to zero.
 */
void iobuf::clear() noexcept {
    fragments_.clear();
    total_size_ = 0;
}

// ============================================================================
// share() — Zero-copy slice sharing the same underlying fragments
// ============================================================================

/**
 * Create a zero-copy slice of the buffer.
 *
 * The returned iobuf shares the same reference-counted fragment data
 * as this buffer, with adjusted offsets and sizes. No data is copied.
 *
 * @param offset Byte offset into this buffer where the slice begins.
 *               Clamped to [0, size_bytes()].
 * @param size   Maximum number of bytes in the slice.
 *               Clamped so the slice does not extend past the end.
 *               SIZE_MAX means "to the end".
 * @return A new iobuf sharing the same fragment memory.
 */
iobuf iobuf::share(size_t offset, size_t size) const {
    iobuf result;

    if (offset >= total_size_) {
        return result; // empty
    }

    // Clamp size so the slice does not extend past the end
    size_t max_available = total_size_ - offset;
    if (size > max_available) {
        size = max_available;
    }

    // Find the fragment containing the start offset
    size_t skipped = 0;
    size_t frag_idx = 0;
    while (frag_idx < fragments_.size()) {
        const auto& f = fragments_[frag_idx];
        if (skipped + f.size > offset) {
            break;
        }
        skipped += f.size;
        ++frag_idx;
    }

    // Build the result from the remaining fragments
    size_t bytes_needed = size;
    size_t intra_frag_offset = offset - skipped;

    for (size_t i = frag_idx; i < fragments_.size() && bytes_needed > 0; ++i) {
        const auto& f = fragments_[i];
        size_t frag_available = f.size - intra_frag_offset;
        size_t take = std::min(bytes_needed, frag_available);

        result.fragments_.emplace_back(f.data, f.offset + intra_frag_offset, take);
        result.total_size_ += take;
        bytes_needed -= take;
        intra_frag_offset = 0; // only the first fragment may have a non-zero offset
    }

    return result;
}

// ============================================================================
// flatten() — Copy all fragments into a single contiguous buffer
// ============================================================================

/**
 * Flatten the entire buffer into a contiguous std::vector<uint8_t>.
 *
 * If the buffer contains a single fragment and its offset is 0, the
 * copy can be avoided (but we still copy here for simplicity and
 * to ensure ownership independence).
 *
 * @return A contiguous copy of all data currently in the buffer.
 */
std::vector<uint8_t> iobuf::flatten() const {
    std::vector<uint8_t> result;
    if (total_size_ == 0) {
        return result;
    }

    result.resize(total_size_);

    size_t written = 0;
    for (const auto& f : fragments_) {
        if (f.size > 0) {
            std::memcpy(result.data() + written, f.begin(), f.size);
            written += f.size;
        }
    }

    return result;
}

// ============================================================================
// copy_to() — Copy data into a pre-allocated destination buffer
// ============================================================================

/**
 * Copy buffer contents into a caller-supplied destination buffer.
 *
 * Copies at most max_size bytes. If the destination buffer is smaller
 * than total_size_, only the first max_size bytes are copied.
 *
 * @param dest     Destination buffer (must be non-null if max_size > 0).
 * @param max_size Maximum number of bytes to copy.
 * @return The number of bytes actually copied.
 */
size_t iobuf::copy_to(char* dest, size_t max_size) const {
    if (dest == nullptr || max_size == 0 || total_size_ == 0) {
        return 0;
    }

    size_t to_copy = std::min(max_size, total_size_);
    size_t copied = 0;

    for (const auto& f : fragments_) {
        if (copied >= to_copy) break;

        size_t take = std::min(f.size, to_copy - copied);
        std::memcpy(dest + copied, f.begin(), take);
        copied += take;
    }

    return copied;
}

// ============================================================================
// prefix() — String view of the first N bytes (if contiguous)
// ============================================================================

/**
 * Get a std::string_view of the first `n` bytes of the buffer.
 *
 * This is only valid (returns a non-empty view) when the first
 * fragment contains at least `n` bytes. It avoids a copy and is
 * intended for cases where the caller knows the data layout
 * (e.g. protocol headers that are prepended as a single fragment).
 *
 * @param n Number of bytes to view.
 * @return A string_view of the prefix, or an empty view if not satisfied.
 */
std::string_view iobuf::prefix(size_t n) const noexcept {
    if (n == 0 || fragments_.empty()) {
        return {};
    }

    const auto& first = fragments_.front();
    if (first.size >= n) {
        return std::string_view(first.begin(), n);
    }

    return {};
}

// ============================================================================
// reserve_fragments() / shrink_to_fit() — Capacity hints
// ============================================================================

/**
 * Reserve space in the fragment list.
 *
 * Call this before a known sequence of append() calls to avoid
 * repeated vector reallocations.
 */
void iobuf::reserve_fragments(size_t n) {
    fragments_.reserve(n);
}

/**
 * Shrink the fragment vector to fit its current size.
 *
 * Releases any excess capacity in the underlying vector.
 */
void iobuf::shrink_to_fit() {
    fragments_.shrink_to_fit();
}

} // namespace torrent
