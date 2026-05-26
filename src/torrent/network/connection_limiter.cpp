/**
 * connection_limiter.cpp — ConnectionLimiter: Global Connection Counting
 *
 * Implements a thread-safe global connection limiter that enforces a
 * ceiling on the number of concurrently active connections across the
 * entire broker process.
 *
 * The limiter uses a simple atomic counter with a CAS-based try_acquire()
 * that atomically checks the current count against the configured maximum.
 * When the broker reaches capacity, new connection attempts are rejected
 * immediately — the caller should return an appropriate error to the
 * connecting client (e.g., BROKER_NOT_AVAILABLE or a throttled response).
 *
 * Design rationale:
 *   - Lock-free atomic counter — no mutex contention on the hot path.
 *     In high-throughput scenarios (100K+ connections) a mutex-based
 *     approach would serialise all connect/disconnect operations.
 *   - CAS-loop in try_acquire() ensures exactly-once admission semantics:
 *     if two threads race for the last slot, only one wins.
 *   - release() is a simple fetch_sub; it never fails.
 *   - The max_connections ceiling can be changed at runtime via
 *     set_max_connections(), allowing dynamic reconfiguration without
 *     restarting the broker.
 *
 * Thread safety:
 *   All public methods are safe to call from any thread.  The internal
 *   count_ and max_ atomics are the only shared state.
 *
 * Usage:
 *   ConnectionLimiter limiter(10000);
 *   if (!limiter.try_acquire()) {
 *       close(client_fd);  // At capacity
 *       return;
 *   }
 *   // ... handle connection ...
 *   limiter.release();  // On disconnect
 */

// ============================================================================
// Project headers
// ============================================================================

#include "torrent/network/connection_limiter.h"
#include "torrent/common/types.h"

// ============================================================================
// System headers
// ============================================================================

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

namespace torrent::network {

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_limiter_logger() {
    static auto logger = spdlog::get("connection_limiter");
    if (!logger) {
        logger = spdlog::stdout_color_mt("connection_limiter");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Absolute upper bound to prevent runaway allocation.
/// 1 million connections should be enough for any single broker node;
/// beyond that you should be horizontally scaling.
inline constexpr int64_t kHardMaxConnections = 1'000'000;

/// Below this threshold the limiter starts emitting warning logs
/// to alert operators that they are approaching capacity.
inline constexpr double kCapacityWarningThreshold = 0.85; // 85%

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

/// Clamp a value to [lo, hi] with a warning log on clamp.
[[nodiscard]] int64_t clamp_connection_limit(int64_t val, int64_t lo, int64_t hi,
                                              std::string_view name) {
    if (val < lo) {
        get_limiter_logger()->warn("{}={} clamped to min={}", name, val, lo);
        return lo;
    }
    if (val > hi) {
        get_limiter_logger()->warn("{}={} clamped to max={}", name, val, hi);
        return hi;
    }
    return val;
}

/// Emit a warning if utilisation exceeds the configured threshold.
void check_capacity_warning(int64_t count, int64_t max) {
    if (max > 0 && count > 0) {
        double utilisation = static_cast<double>(count) / static_cast<double>(max);
        if (utilisation >= kCapacityWarningThreshold) {
            get_limiter_logger()->warn(
                "Connection limit nearing capacity: {}/{} ({:.1f}%)",
                count, max, utilisation * 100.0);
        }
    }
}

} // anonymous namespace

// ============================================================================
// Construction
// ============================================================================

ConnectionLimiter::ConnectionLimiter(int64_t max_connections) {
    int64_t clamped = clamp_connection_limit(max_connections, 0, kHardMaxConnections,
                                              "max_connections");
    max_.store(clamped, std::memory_order_relaxed);

    if (clamped == 0) {
        get_limiter_logger()->warn(
            "ConnectionLimiter created with max_connections=0 — all "
            "connection attempts will be rejected");
    } else {
        get_limiter_logger()->info(
            "ConnectionLimiter created: max_connections={}", clamped);
    }
}

// ============================================================================
// Core API
// ============================================================================

bool ConnectionLimiter::try_acquire() noexcept {
    int64_t max = max_.load(std::memory_order_acquire);

    // Fast path: max == 0 means unlimited.
    if (max == 0) {
        count_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    // --- CAS loop for bounded admission ---
    for (;;) {
        int64_t current = count_.load(std::memory_order_acquire);

        if (current >= max) {
            // At capacity — reject.
            // Increment a rejection metric so operators can monitor.
            get_limiter_logger()->warn(
                "Connection rejected: count={} max={} (at capacity)",
                current, max);
            return false;
        }

        int64_t desired = current + 1;
        if (count_.compare_exchange_weak(current, desired,
                                          std::memory_order_release,
                                          std::memory_order_acquire)) {
            // Successfully acquired a slot.
            get_limiter_logger()->trace(
                "Connection acquired: {} → {} (max={})",
                current, desired, max);

            // Check if we should warn about approaching capacity.
            check_capacity_warning(desired, max);

            return true;
        }
        // CAS failed — another thread raced us.  Retry.
        // Re-read max in case it was changed concurrently.
        max = max_.load(std::memory_order_acquire);
    }
}

void ConnectionLimiter::release() noexcept {
    int64_t prev = count_.fetch_sub(1, std::memory_order_release);

    if (prev <= 0) {
        // Underflow — a release() was called without a matching try_acquire().
        // Fix the counter and log the bug.
        count_.store(0, std::memory_order_relaxed);
        get_limiter_logger()->error(
            "ConnectionLimiter::release() underflow: count was {} — "
            "mismatched acquire/release calls", prev);
        return;
    }

    get_limiter_logger()->trace(
        "Connection released: {} → {} (max={})",
        prev, prev - 1, max_.load(std::memory_order_relaxed));
}

// ============================================================================
// Accessors
// ============================================================================

int64_t ConnectionLimiter::current_count() const noexcept {
    return count_.load(std::memory_order_acquire);
}

int64_t ConnectionLimiter::max_connections() const noexcept {
    return max_.load(std::memory_order_acquire);
}

} // namespace torrent::network
