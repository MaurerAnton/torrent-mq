#pragma once

#include <cstdint>
#include <atomic>
#include <chrono>
#include <mutex>

namespace torrent {

/**
 * Token-bucket rate limiter for bandwidth throttling.
 *
 * Tokens are refilled at a constant rate (bytes per second). Each operation
 * consumes tokens equal to its byte count. If insufficient tokens are
 * available, the operation may be delayed or rejected depending on the mode.
 *
 * Thread-safe: all methods use atomic operations.
 */
class rate_limiter {
public:
    /**
     * Construct a rate limiter with the given rate and burst size.
     *
     * @param rate_bps Maximum sustained rate in bytes per second (0 = unlimited)
     * @param burst_bytes Maximum burst size in bytes (default: rate_bps / 10)
     * @param initial_tokens Starting token count (default: burst_bytes)
     */
    explicit rate_limiter(int64_t rate_bps = 0,
                          int64_t burst_bytes = 0,
                          int64_t initial_tokens = 0);

    /**
     * Try to consume tokens for an operation of the given size.
     *
     * @param bytes Number of bytes to consume
     * @return true if tokens were available and consumed, false if throttled
     */
    [[nodiscard]] bool try_consume(int64_t bytes) noexcept;

    /**
     * Consume tokens, blocking until enough are available.
     *
     * @param bytes Number of bytes to consume
     * @param timeout_ms Maximum time to wait (0 = no wait, -1 = indefinite)
     * @return Number of bytes actually consumed (may be less than requested on timeout)
     */
    int64_t consume_blocking(int64_t bytes, int64_t timeout_ms = -1) noexcept;

    /**
     * Check if an operation of the given size can proceed without blocking.
     *
     * @param bytes Number of bytes to check
     * @return true if enough tokens are available right now
     */
    [[nodiscard]] bool can_consume(int64_t bytes) const noexcept;

    /**
     * Get the current number of available tokens.
     */
    [[nodiscard]] int64_t available_tokens() const noexcept {
        return tokens_.load(std::memory_order_relaxed);
    }

    /**
     * Get the current refill rate in bytes per second.
     */
    [[nodiscard]] int64_t rate_bps() const noexcept { return rate_bps_.load(); }

    /**
     * Update the refill rate at runtime.
     *
     * @param new_rate_bps New rate in bytes per second
     */
    void set_rate(int64_t new_rate_bps) noexcept;

    /**
     * Update the burst size at runtime.
     *
     * @param new_burst_bytes New burst size (clamped to >= rate_bps / 10)
     */
    void set_burst(int64_t new_burst_bytes) noexcept;

    /**
     * Manually add tokens (e.g., for connection-level token return).
     */
    void refill(int64_t tokens) noexcept;

    /**
     * Reset the limiter to initial state.
     */
    void reset() noexcept;

    /**
     * Check if the limiter is enabled (rate > 0).
     */
    [[nodiscard]] bool is_enabled() const noexcept {
        return rate_bps_.load() > 0;
    }

    /**
     * Get the time since last token refill, used for metrics.
     */
    [[nodiscard]] int64_t ms_since_last_refill() const noexcept;

private:
    /**
     * Replenish tokens based on elapsed time.
     * Called before each consume attempt.
     */
    void replenish() noexcept;

    /**
     * Get current monotonic time in microseconds.
     */
    static int64_t now_us() noexcept;

    std::atomic<int64_t> rate_bps_{0};
    std::atomic<int64_t> burst_bytes_{0};
    std::atomic<int64_t> tokens_{0};
    std::atomic<int64_t> last_refill_us_{0};
    mutable std::mutex block_mutex_;
};

} // namespace torrent
