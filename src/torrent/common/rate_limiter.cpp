/**
 * torrent-mq — Rate Limiter (Token Bucket) Implementation
 *
 * Token-bucket rate limiter for bandwidth throttling and traffic shaping.
 * Tokens are refilled at a constant rate (bytes/second). Each consumption
 * attempt atomically checks and deducts tokens. Blocking mode waits on a
 * condition variable until enough tokens have been replenished.
 *
 * Design:
 *   - Atomic operations for the token counter and rate parameters,
 *     minimising lock contention on the fast path (try_consume).
 *   - A dedicated mutex + condition variable for blocking consumers.
 *   - Token replenishment is lazy: tokens are calculated from elapsed
 *     time at the point of each consume call, avoiding a background
 *     replenishment thread.
 *   - When rate_bps is 0, the limiter is effectively disabled and all
 *     operations succeed immediately (unlimited throughput).
 *
 * Usage:
 *   rate_limiter rl(10 * 1024 * 1024);  // 10 MB/s
 *   if (rl.try_consume(1024)) { send(packet); }
 *   rl.consume_blocking(65536, 5000);  // wait up to 5s for 64 KiB
 */

#include "torrent/common/rate_limiter.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cassert>
#include <thread>

namespace torrent {

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace {

/**
 * Get (or create) a dedicated logger for the rate limiter subsystem.
 */
std::shared_ptr<spdlog::logger> get_rate_logger() {
    static auto logger = spdlog::get("rate_limiter");
    if (!logger) {
        logger = spdlog::stdout_color_mt("rate_limiter");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

/**
 * Minimum burst size as a fraction of rate (1/10th of rate, or at least
 * 1 KiB to avoid pathological cases with very low rates).
 */
static constexpr int64_t kMinBurstFraction = 10;
static constexpr int64_t kAbsoluteMinBurst = 1024;
static constexpr int64_t kMaxTokens = INT64_MAX / 2;

/**
 * Clamp the burst value to a reasonable range.
 */
int64_t clamp_burst(int64_t rate_bps, int64_t burst_bytes) noexcept {
    if (rate_bps <= 0) return burst_bytes > 0 ? burst_bytes : kAbsoluteMinBurst;
    int64_t min_burst = std::max(rate_bps / kMinBurstFraction, kAbsoluteMinBurst);
    int64_t max_burst = std::max(rate_bps * 10, min_burst);
    return std::clamp(burst_bytes, min_burst, max_burst);
}

} // anonymous namespace

// ============================================================================
// Construction
// ============================================================================

rate_limiter::rate_limiter(int64_t rate_bps, int64_t burst_bytes, int64_t initial_tokens)
{
    int64_t rate = std::max(int64_t(0), rate_bps);

    // If burst not specified, default to rate / 10 (or 1 KiB minimum).
    if (burst_bytes <= 0) {
        burst_bytes = rate > 0 ? std::max(rate / kMinBurstFraction, kAbsoluteMinBurst)
                               : kAbsoluteMinBurst;
    }

    burst_bytes = clamp_burst(rate, burst_bytes);

    // If initial tokens not specified, start with a full bucket.
    if (initial_tokens <= 0) {
        initial_tokens = burst_bytes;
    }
    initial_tokens = std::clamp(initial_tokens, int64_t(0), burst_bytes);

    rate_bps_.store(rate, std::memory_order_relaxed);
    burst_bytes_.store(burst_bytes, std::memory_order_relaxed);
    tokens_.store(initial_tokens, std::memory_order_relaxed);
    last_refill_us_.store(now_us(), std::memory_order_relaxed);

    auto logger = get_rate_logger();
    logger->debug("rate_limiter created: rate={} B/s, burst={} B, initial_tokens={}",
                  rate, burst_bytes, initial_tokens);
}

// ============================================================================
// Token replenishment
// ============================================================================

void rate_limiter::replenish() noexcept {
    int64_t rate = rate_bps_.load(std::memory_order_relaxed);
    if (rate <= 0) {
        // Rate is zero/unlimited — keep tokens at burst level.
        // We still update last_refill_us_ so time doesn't drift indefinitely.
        last_refill_us_.store(now_us(), std::memory_order_relaxed);
        return;
    }

    int64_t burst = burst_bytes_.load(std::memory_order_relaxed);
    int64_t now = now_us();
    int64_t last = last_refill_us_.load(std::memory_order_relaxed);

    // Compute elapsed time in microseconds.
    int64_t elapsed_us = now - last;
    if (elapsed_us <= 0) return;  // Clock didn't advance or went backward.

    // Tokens = rate_bps * elapsed_us / 1_000_000
    // Use 128-bit math to avoid overflow for large elapsed times.
    __int128 new_tokens_scaled = static_cast<__int128>(rate) * static_cast<__int128>(elapsed_us);
    int64_t new_tokens = static_cast<int64_t>(new_tokens_scaled / 1000000);

    if (new_tokens <= 0) return;  // Not enough time passed for at least 1 token.

    // Add tokens atomically, capped at burst.
    int64_t current = tokens_.load(std::memory_order_relaxed);
    int64_t desired;
    do {
        desired = std::min(current + new_tokens, burst);
        if (desired <= current) break;  // Already at burst.
    } while (!tokens_.compare_exchange_weak(current, desired,
                                            std::memory_order_release,
                                            std::memory_order_relaxed));

    // Update last refill time. If another thread updated it concurrently
    // with a higher value, our update is harmless (slight under-refill).
    last_refill_us_.store(now, std::memory_order_release);
}

// ============================================================================
// now_us — monotonic clock in microseconds
// ============================================================================

int64_t rate_limiter::now_us() noexcept {
    auto now = std::chrono::steady_clock::now();
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch());
    return us.count();
}

// ============================================================================
// try_consume — non-blocking token consumption
// ============================================================================

bool rate_limiter::try_consume(int64_t bytes) noexcept {
    if (bytes <= 0) return true;  // Zero or negative bytes always "consumed".

    int64_t rate = rate_bps_.load(std::memory_order_relaxed);
    if (rate <= 0) return true;  // Rate limiting disabled.

    // Replenish tokens before checking.
    replenish();

    int64_t current = tokens_.load(std::memory_order_relaxed);
    while (current >= bytes) {
        if (tokens_.compare_exchange_weak(current, current - bytes,
                                           std::memory_order_release,
                                           std::memory_order_relaxed)) {
            return true;
        }
        // current was reloaded by compare_exchange_weak on failure.
    }
    return false;
}

// ============================================================================
// can_consume — check without deducting
// ============================================================================

bool rate_limiter::can_consume(int64_t bytes) const noexcept {
    if (bytes <= 0) return true;

    int64_t rate = rate_bps_.load(std::memory_order_relaxed);
    if (rate <= 0) return true;

    // We cannot call replenish() from a const method since it modifies
    // tokens_ and last_refill_us_. Instead, compute the theoretical
    // token count including what has accumulated since last refill.
    int64_t burst = burst_bytes_.load(std::memory_order_relaxed);
    int64_t current = tokens_.load(std::memory_order_relaxed);
    if (current >= bytes) return true;

    // Estimate accumulated tokens from elapsed time.
    int64_t now = now_us();
    int64_t last = last_refill_us_.load(std::memory_order_relaxed);
    if (now > last) {
        int64_t elapsed_us = now - last;
        int64_t accumulated = (rate * elapsed_us) / 1000000;
        current = std::min(current + accumulated, burst);
    }

    return current >= bytes;
}

// ============================================================================
// consume_blocking — wait for enough tokens
// ============================================================================

int64_t rate_limiter::consume_blocking(int64_t bytes, int64_t timeout_ms) noexcept {
    if (bytes <= 0) return 0;

    int64_t rate = rate_bps_.load(std::memory_order_relaxed);
    if (rate <= 0) {
        // Unlimited — consume immediately.
        return bytes;
    }

    // Fast path: try non-blocking consumption.
    if (try_consume(bytes)) {
        return bytes;
    }

    // Slow path: block until enough tokens are available.
    // We use a condition variable per-limiter to avoid busy-waiting.
    auto logger = get_rate_logger();
    logger->trace("consume_blocking: waiting for {} bytes (timeout={}ms)",
                  bytes, timeout_ms);

    auto start = std::chrono::steady_clock::now();

    {
        std::unique_lock<std::mutex> lock(block_mutex_);

        while (true) {
            // Check remaining time.
            if (timeout_ms > 0) {
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start);
                if (elapsed.count() >= timeout_ms) {
                    // Timeout: consume whatever is available.
                    int64_t available = tokens_.load(std::memory_order_relaxed);
                    replenish();
                    available = tokens_.load(std::memory_order_relaxed);

                    int64_t consumed = std::min(available, bytes);
                    if (consumed > 0) {
                        tokens_.fetch_sub(consumed, std::memory_order_relaxed);
                    }

                    logger->debug("consume_blocking: timeout after {}ms, "
                                  "consumed {}/{} bytes",
                                  elapsed.count(), consumed, bytes);
                    return consumed;
                }
            }

            // Try to consume.
            replenish();
            int64_t current = tokens_.load(std::memory_order_relaxed);
            if (current >= bytes) {
                if (tokens_.compare_exchange_weak(current, current - bytes,
                                                   std::memory_order_release,
                                                   std::memory_order_relaxed)) {
                    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start);
                    logger->trace("consume_blocking: got {} bytes after {}ms",
                                  bytes, elapsed.count());
                    return bytes;
                }
                // CAS failed, retry.
                continue;
            }

            // Compute wait time.
            int64_t deficit = bytes - current;
            int64_t wait_us = (deficit * 1000000) / rate;

            // If timeout_ms is specified, cap the wait time.
            if (timeout_ms > 0) {
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start);
                int64_t remaining_ms = timeout_ms - elapsed.count();
                if (remaining_ms <= 0) {
                    // Checked above, but re-check for safety.
                    continue;
                }
                int64_t remaining_us = remaining_ms * 1000;
                wait_us = std::min(wait_us, remaining_us);
            }

            // Wait at most 100ms per iteration; the caller can also
            // interrupt by calling refill(), but in this design we use
            // a timed wait so the loop checks periodically.
            wait_us = std::min(wait_us, int64_t(100000));  // 100ms max per wait.

            // Unlock and wait.  We rely on the replenish loop rather
            // than signal-based notification, since there is no dedicated
            // replenishment thread.  Alternative: use a cv.wait_for().
            // We sleep in small increments and retry.
            lock.unlock();
            std::this_thread::sleep_for(std::chrono::microseconds(wait_us));
            lock.lock();
        }
    }
}

// ============================================================================
// set_rate — dynamic rate reconfiguration
// ============================================================================

void rate_limiter::set_rate(int64_t new_rate_bps) noexcept {
    int64_t clamped = std::max(int64_t(0), new_rate_bps);
    int64_t old = rate_bps_.exchange(clamped, std::memory_order_release);

    // Adjust burst proportionally if the rate changed significantly.
    int64_t old_burst = burst_bytes_.load(std::memory_order_relaxed);
    int64_t new_burst = clamp_burst(clamped, old_burst);
    if (new_burst != old_burst) {
        burst_bytes_.store(new_burst, std::memory_order_relaxed);
    }

    // If rate went to zero (disabled), fill tokens to burst so pending
    // blocking consumers unblock immediately.
    if (clamped == 0 && old > 0) {
        tokens_.store(new_burst, std::memory_order_relaxed);
    }

    auto logger = get_rate_logger();
    logger->debug("rate_limiter: rate changed {} -> {} B/s, burst={} B",
                  old, clamped, new_burst);
}

// ============================================================================
// set_burst — dynamic burst reconfiguration
// ============================================================================

void rate_limiter::set_burst(int64_t new_burst_bytes) noexcept {
    int64_t rate = rate_bps_.load(std::memory_order_relaxed);
    int64_t clamped = clamp_burst(rate, new_burst_bytes);
    int64_t old = burst_bytes_.exchange(clamped, std::memory_order_relaxed);

    // If burst decreased, cap current tokens.
    if (clamped < old) {
        int64_t current = tokens_.load(std::memory_order_relaxed);
        while (current > clamped) {
            if (tokens_.compare_exchange_weak(current, clamped,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)) {
                break;
            }
        }
    }

    auto logger = get_rate_logger();
    logger->debug("rate_limiter: burst changed {} -> {} B", old, clamped);
}

// ============================================================================
// refill — manually add tokens (connection-level return)
// ============================================================================

void rate_limiter::refill(int64_t tokens) noexcept {
    if (tokens <= 0) return;

    int64_t burst = burst_bytes_.load(std::memory_order_relaxed);
    int64_t current = tokens_.load(std::memory_order_relaxed);
    int64_t desired;
    do {
        desired = std::min(current + tokens, burst);
    } while (!tokens_.compare_exchange_weak(current, desired,
                                             std::memory_order_release,
                                             std::memory_order_relaxed));

    auto logger = get_rate_logger();
    logger->trace("rate_limiter: refilled {} tokens, current ~{}", tokens, desired);
}

// ============================================================================
// reset — return to initial state
// ============================================================================

void rate_limiter::reset() noexcept {
    int64_t burst = burst_bytes_.load(std::memory_order_relaxed);
    tokens_.store(burst, std::memory_order_relaxed);
    last_refill_us_.store(now_us(), std::memory_order_relaxed);

    auto logger = get_rate_logger();
    logger->debug("rate_limiter: reset to burst={} tokens", burst);
}

// ============================================================================
// ms_since_last_refill — for metrics
// ============================================================================

int64_t rate_limiter::ms_since_last_refill() const noexcept {
    int64_t now = now_us();
    int64_t last = last_refill_us_.load(std::memory_order_relaxed);
    if (now <= last) return 0;
    return (now - last) / 1000;
}

} // namespace torrent
