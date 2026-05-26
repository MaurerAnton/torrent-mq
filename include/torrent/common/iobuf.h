#pragma once

#include <cstdint>
#include <vector>
#include <memory>
#include <cstring>
#include <algorithm>
#include <type_traits>
#include <span>

namespace torrent {

/**
 * Scatter-gather I/O buffer for zero-copy message construction.
 *
 * iobuf is a chain of immutable buffer fragments. Appending data
 * creates a new fragment at the end; consuming removes from the front.
 * The buffer supports zero-copy slicing, so multiple readers can
 * share fragments via reference counting.
 *
 * Key properties:
 *   - Fragments are reference-counted (shared ownership)
 *   - Write path: append() adds fragments; never modifies existing data
 *   - Read path: consume(), trim_front() remove from front
 *   - Zero-copy slice: share() creates a new iobuf sharing fragments
 *   - Linearization: flatten() creates contiguous copy when needed
 */
class iobuf {
public:
    /**
     * A single contiguous buffer fragment.
     * The actual data is reference-counted so multiple iobufs
     * can share fragments safely.
     */
    struct fragment {
        std::shared_ptr<const char[]> data;
        size_t offset = 0;  // Offset into the shared data
        size_t size = 0;    // Bytes in this fragment (from offset)

        fragment() = default;

        fragment(std::shared_ptr<const char[]> d, size_t off, size_t sz)
            : data(std::move(d)), offset(off), size(sz) {}

        [[nodiscard]] const char* begin() const noexcept {
            return data.get() + offset;
        }

        [[nodiscard]] const char* end() const noexcept {
            return begin() + size;
        }

        [[nodiscard]] bool empty() const noexcept { return size == 0; }
    };

    iobuf() = default;

    // Copy: fragments are shared (reference counted)
    iobuf(const iobuf&) = default;
    iobuf& operator=(const iobuf&) = default;

    // Move
    iobuf(iobuf&&) noexcept = default;
    iobuf& operator=(iobuf&&) noexcept = default;

    /**
     * Construct from a single contiguous buffer (copies data).
     */
    explicit iobuf(const char* data, size_t size);

    /**
     * Construct from a string (copies data).
     */
    explicit iobuf(const std::string& str);

    /**
     * Construct from a vector (copies data).
     */
    explicit iobuf(std::vector<uint8_t> data);

    /**
     * Check if the buffer is empty.
     */
    [[nodiscard]] bool empty() const noexcept {
        return total_size_ == 0;
    }

    /**
     * Get total number of bytes across all fragments.
     */
    [[nodiscard]] size_t size_bytes() const noexcept {
        return total_size_;
    }

    /**
     * Get the number of fragments.
     */
    [[nodiscard]] size_t fragment_count() const noexcept {
        return fragments_.size();
    }

    /**
     * Get a view of all fragments.
     */
    [[nodiscard]] const std::vector<fragment>& fragments() const noexcept {
        return fragments_;
    }

    /**
     * Append raw bytes to the end (copies data).
     */
    void append(const char* data, size_t size);

    /**
     * Append a string.
     */
    void append(const std::string& str);

    /**
     * Append another iobuf (shares fragments).
     * The appended iobuf's fragments are appended without copying.
     */
    void append(iobuf other);

    /**
     * Consume bytes from the front.
     * If n >= size_bytes(), the buffer becomes empty.
     *
     * @param n Number of bytes to consume
     */
    void trim_front(size_t n);

    /**
     * Consume all bytes from the front.
     */
    void clear() noexcept;

    /**
     * Create a zero-copy slice sharing the same fragments.
     * The returned iobuf is a view into the same memory.
     *
     * @param offset Byte offset to start the slice
     * @param size Number of bytes in the slice (default: to end)
     * @return A new iobuf sharing fragments with this one
     */
    [[nodiscard]] iobuf share(size_t offset = 0, size_t size = SIZE_MAX) const;

    /**
     * Flatten the buffer into a single contiguous vector.
     * Copies data if there are multiple fragments.
     *
     * @return Contiguous copy of all data in the buffer
     */
    [[nodiscard]] std::vector<uint8_t> flatten() const;

    /**
     * Copy data into a pre-allocated buffer.
     *
     * @param dest Destination buffer (must be at least size_bytes())
     * @param max_size Maximum bytes to copy
     * @return Number of bytes actually copied
     */
    size_t copy_to(char* dest, size_t max_size) const;

    /**
     * Get a string_view of the first N bytes.
     * Only valid if the first fragment is at least N bytes;
     * otherwise returns empty.
     */
    [[nodiscard]] std::string_view prefix(size_t n) const noexcept;

    /**
     * Reserve space in the fragment list (optimization).
     */
    void reserve_fragments(size_t n);

    /**
     * Shrink fragment vector to fit.
     */
    void shrink_to_fit();

private:
    std::vector<fragment> fragments_;
    size_t total_size_ = 0;
};

} // namespace torrent
