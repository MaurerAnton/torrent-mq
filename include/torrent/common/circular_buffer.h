#pragma once

#include <cstdint>
#include <vector>
#include <cassert>
#include <type_traits>
#include <optional>
#include <atomic>
#include <mutex>
#include <shared_mutex>

namespace torrent {

/**
 * Lock-free single-producer single-consumer (SPSC) circular buffer.
 *
 * Optimized for low-latency message passing between a single writer
 * and a single reader without locks. Uses atomic head/tail pointers
 * and a power-of-2 capacity for fast modulo via bitmask.
 *
 * @tparam T Element type (must be trivially copyable or moveable)
 */
template<typename T>
class spsc_queue {
public:
    static_assert(std::is_nothrow_move_constructible_v<T> ||
                  std::is_trivially_copyable_v<T>,
                  "T must be nothrow move constructible or trivially copyable");

    /**
     * Construct with a capacity that will be rounded up to next power of 2.
     */
    explicit spsc_queue(size_t capacity = 1024)
        : capacity_(next_power_of_2(capacity))
        , mask_(capacity_ - 1)
        , buffer_(capacity_) {}

    /**
     * Try to push an element. Returns false if the queue is full.
     */
    bool try_push(T item) noexcept {
        size_t head = head_.load(std::memory_order_relaxed);
        size_t next = (head + 1) & mask_;

        if (next == tail_.load(std::memory_order_acquire)) {
            return false; // Full
        }

        buffer_[head] = std::move(item);
        head_.store(next, std::memory_order_release);
        return true;
    }

    /**
     * Try to pop an element. Returns nullopt if the queue is empty.
     */
    std::optional<T> try_pop() noexcept {
        size_t tail = tail_.load(std::memory_order_relaxed);

        if (tail == head_.load(std::memory_order_acquire)) {
            return std::nullopt; // Empty
        }

        T item = std::move(buffer_[tail]);
        tail_.store((tail + 1) & mask_, std::memory_order_release);
        return item;
    }

    /**
     * Check if the queue is empty.
     */
    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) ==
               tail_.load(std::memory_order_acquire);
    }

    /**
     * Check if the queue is full.
     */
    [[nodiscard]] bool full() const noexcept {
        size_t head = head_.load(std::memory_order_relaxed);
        return ((head + 1) & mask_) == tail_.load(std::memory_order_acquire);
    }

    /**
     * Get the number of elements currently in the queue.
     */
    [[nodiscard]] size_t size() const noexcept {
        size_t head = head_.load(std::memory_order_acquire);
        size_t tail = tail_.load(std::memory_order_acquire);
        if (head >= tail) return head - tail;
        return capacity_ - tail + head;
    }

    /**
     * Get the maximum capacity (one slot is reserved for empty/full distinction).
     */
    [[nodiscard]] size_t capacity() const noexcept {
        return capacity_ - 1;
    }

    /**
     * Clear the queue (not thread-safe; caller must ensure no concurrent ops).
     */
    void clear() noexcept {
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
    }

private:
    static size_t next_power_of_2(size_t n) noexcept {
        if (n == 0) return 2;
        n--;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n |= n >> 32;
        return n + 1;
    }

    size_t capacity_;
    size_t mask_;
    std::vector<T> buffer_;
    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};
};

/**
 * Thread-safe multi-producer multi-consumer (MPMC) circular buffer.
 *
 * Uses a mutex for producer/consumer coordination. Suitable for
 * scenarios where multiple threads produce and consume.
 */
template<typename T>
class mpmc_queue {
public:
    explicit mpmc_queue(size_t capacity = 1024)
        : capacity_(std::max(size_t(1), capacity)) {}

    /**
     * Push an element. Blocks if the queue is full (with timeout).
     */
    bool push(T item) noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        if (buffer_.size() >= capacity_) {
            return false;
        }
        buffer_.push_back(std::move(item));
        return true;
    }

    /**
     * Try to pop an element. Returns nullopt if empty.
     */
    std::optional<T> try_pop() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        if (buffer_.empty()) {
            return std::nullopt;
        }
        T item = std::move(buffer_.front());
        buffer_.erase(buffer_.begin());
        return item;
    }

    /**
     * Pop all available elements into a vector.
     */
    std::vector<T> drain() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<T> result;
        result.reserve(buffer_.size());
        for (auto& item : buffer_) {
            result.push_back(std::move(item));
        }
        buffer_.clear();
        return result;
    }

    [[nodiscard]] bool empty() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return buffer_.empty();
    }

    [[nodiscard]] size_t size() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return buffer_.size();
    }

    [[nodiscard]] size_t capacity() const noexcept { return capacity_; }

    void clear() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        buffer_.clear();
    }

private:
    size_t capacity_;
    mutable std::mutex mutex_;
    std::vector<T> buffer_;
};

} // namespace torrent
