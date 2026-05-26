/**
 * flow_control.cpp — FlowControl: Credit-Based Flow Control
 *
 * Implements a per-connection credit-based flow control mechanism
 * inspired by reactive-streams and TCP receive-window accounting.
 *
 * Each connection is assigned a maximum credit budget (default 64 KiB).
 * Senders must "consume" credit before transmitting data; the receiver
 * "replenishes" credit as it processes incoming messages.  When the
 * available credit reaches zero, the sender must stop — this is the
 * backpressure signal.
 *
 * Design:
 *   - Fully lock-free: all credit operations use std::atomic<int64_t>
 *     with relaxed / acquire-release ordering.  No mutex is needed
 *     because the credit counter is a single-scalar atomic.
 *   - consume() returns false when there is insufficient credit,
 *     allowing the caller to implement backpressure (e.g., pause the
 *     send queue or return a THROTTLING_QUOTA_EXCEEDED error).
 *   - replenish() adds credit back to the pool, up to max_credit.
 *   - set_max_credit() can shrink or grow the window at runtime.
 *     Shrinking does NOT preempt already-consumed credit — it only
 *     caps future replenishments.
 *   - All arithmetic is saturating: credit is clamped to [0, max_credit]
 *     to prevent overflow or negative-credit bugs.
 *
 * Thread safety:
 *   Every public method is safe to call from any thread without
 *   external synchronisation.  The atomic credit_ counter is the
 *   sole synchronisation point.
 *
 * Usage:
 *   FlowControl fc(65536);
 *   if (fc.consume(4096)) { send(buf, 4096); }
 *   fc.replenish(4096);  // called by the read-loop after processing
 *
 * Dependencies:
 *   - flow_control.h (this file's header)
 *   - <spdlog/spdlog.h> for diagnostic logging
 */

// ============================================================================
// Project headers
// ============================================================================

#include "torrent/network/flow_control.h"
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

std::shared_ptr<spdlog::logger> get_flow_logger() {
    static auto logger = spdlog::get("flow_control");
    if (!logger) {
        logger = spdlog::stdout_color_mt("flow_control");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Validation
// --------------------------------------------------------------------------

/// Clamp a value to [lo, hi] with a warning log on clamp.
[[nodiscard]] int64_t clamp_credit(int64_t val, int64_t lo, int64_t hi,
                                    std::string_view name) {
    if (val < lo) {
        get_flow_logger()->warn("{}={} clamped to min={}", name, val, lo);
        return lo;
    }
    if (val > hi) {
        get_flow_logger()->warn("{}={} clamped to max={}", name, val, hi);
        return hi;
    }
    return val;
}

/// Upper bound for credit to guard against overflow.
inline constexpr int64_t kMaxCreditLimit = 1LL << 40; // 1 TiB

} // anonymous namespace

// ============================================================================
// Construction
// ============================================================================

FlowControl::FlowControl(int64_t initial_credit) {
    int64_t clamped = clamp_credit(initial_credit, 0, kMaxCreditLimit,
                                    "initial_credit");
    credit_.store(clamped, std::memory_order_relaxed);
    max_credit_.store(clamped, std::memory_order_relaxed);

    get_flow_logger()->debug(
        "FlowControl created: initial_credit={} max_credit={}",
        clamped, clamped);
}

// ============================================================================
// Core API
// ============================================================================

int64_t FlowControl::available_credit() const noexcept {
    return credit_.load(std::memory_order_acquire);
}

bool FlowControl::consume(int64_t bytes) noexcept {
    if (bytes <= 0) {
        // Consuming zero or negative bytes is a no-op / programming error.
        if (bytes < 0) {
            get_flow_logger()->warn("consume({}) called with negative bytes", bytes);
        }
        return true; // Nothing to consume — always succeeds.
    }

    // --- Optimistic CAS loop ---
    // We spin until we either successfully deduct bytes or discover that
    // there is insufficient credit.  This is wait-free in the common case
    // (contention is low because each connection has its own FlowControl).

    for (;;) {
        int64_t current = credit_.load(std::memory_order_acquire);
        if (current < bytes) {
            // Not enough credit — backpressure.
            get_flow_logger()->trace(
                "consume({}) rejected: available={}", bytes, current);
            return false;
        }

        int64_t desired = current - bytes;
        if (credit_.compare_exchange_weak(current, desired,
                                           std::memory_order_release,
                                           std::memory_order_acquire)) {
            get_flow_logger()->trace(
                "consume({}) succeeded: {} → {}", bytes, current, desired);
            return true;
        }
        // CAS failed — another thread modified credit_, retry.
    }
}

void FlowControl::replenish(int64_t bytes) noexcept {
    if (bytes <= 0) {
        if (bytes < 0) {
            get_flow_logger()->warn("replenish({}) called with negative bytes", bytes);
        }
        return;
    }

    int64_t max = max_credit_.load(std::memory_order_acquire);

    // --- Saturating add loop ---
    // credit = min(max, credit + bytes)

    for (;;) {
        int64_t current = credit_.load(std::memory_order_acquire);

        // If already at or above max, nothing to do.
        if (current >= max) {
            get_flow_logger()->trace(
                "replenish({}) skipped: already at max={}", bytes, max);
            return;
        }

        int64_t desired = current + bytes;
        if (desired > max || desired < current /* overflow check */) {
            desired = max;
        }

        if (credit_.compare_exchange_weak(current, desired,
                                           std::memory_order_release,
                                           std::memory_order_acquire)) {
            get_flow_logger()->trace(
                "replenish({}) succeeded: {} → {}", bytes, current, desired);
            return;
        }
        // CAS failed — retry.
        // Re-read max in case it was changed concurrently.
        max = max_credit_.load(std::memory_order_acquire);
    }
}

void FlowControl::set_max_credit(int64_t max_credit) noexcept {
    int64_t clamped = clamp_credit(max_credit, 0, kMaxCreditLimit,
                                    "max_credit");

    int64_t old_max = max_credit_.exchange(clamped, std::memory_order_acq_rel);

    // If the new max is lower than current credit, we must cap credit.
    // This can happen if an admin reduces the per-connection buffer size
    // at runtime.
    if (clamped < old_max) {
        for (;;) {
            int64_t current = credit_.load(std::memory_order_acquire);
            if (current <= clamped) break; // Already within bounds.

            if (credit_.compare_exchange_weak(current, clamped,
                                               std::memory_order_release,
                                               std::memory_order_acquire)) {
                get_flow_logger()->info(
                    "set_max_credit({}) capped credit from {} to {}",
                    clamped, current, clamped);
                break;
            }
        }
    }

    get_flow_logger()->debug(
        "set_max_credit: old={} new={}", old_max, clamped);
}

} // namespace torrent::network
