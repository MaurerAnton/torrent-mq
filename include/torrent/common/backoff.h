#pragma once

/**
 * backoff.h — ExponentialBackoff: Configurable Retry Delay with Jitter
 *
 * Provides a reusable exponential backoff strategy for retry loops across
 * all torrent-mq subsystems: network connections, Raft RPCs, connector
 * tasks, storage I/O, and proxy requests.
 *
 * The algorithm:
 *   delay = min(max_delay, initial_delay * multiplier^attempts)
 *   final_delay = delay * (1.0 - jitter_factor * U[0,1])
 *
 * Jitter smears retry storms so that many clients retrying simultaneously
 * don't synchronise and overload the system ("thundering herd").
 *
 * Configurable parameters (all with sensible defaults):
 *   - initial_delay: first backoff duration (default 100ms)
 *   - max_delay:     upper bound on backoff (default 30s)
 *   - multiplier:    growth factor per attempt (default 2.0)
 *   - jitter_factor: fraction of delay for randomisation (default 0.25)
 *   - max_attempts:  0 = unlimited, N = give up after N attempts
 *
 * Usage:
 *   ExponentialBackoff bo(100ms, 10s);
 *   while (!success) {
 *       bo.next_delay();  // returns next delay, advances internal state
 *       if (bo.exhausted()) break;
 *       std::this_thread::sleep_for(bo.last_delay());
 *   }
 *
 * Thread-safety: NOT thread-safe. Each call site should have its own instance
 *                or protect with an external mutex.
 */

#include <chrono>
#include <cstdint>
#include <mutex>
#include <random>
#include <string>
#include <string_view>

namespace torrent {

// ============================================================================
// ExponentialBackoff
// ============================================================================

class ExponentialBackoff {
public:
    // ---- Construction ----

    /// Default backoff: 100ms → 30s, 2× multiplier, 25% jitter, unlimited attempts.
    ExponentialBackoff();

    /// Full parameter constructor.
    /// @param initial  initial delay (must be > 0)
    /// @param max      maximum delay (must be >= initial)
    /// @param mult     multiplier per attempt (must be >= 1.0)
    /// @param jitter   jitter factor in [0.0, 1.0]
    /// @param attempts max attempts (0 = unlimited)
    ExponentialBackoff(std::chrono::milliseconds initial,
                       std::chrono::milliseconds max,
                       double mult = 2.0,
                       double jitter = 0.25,
                       int32_t attempts = 0);

    // ---- Core API ----

    /// Compute the next backoff delay, advance attempt counter, and return it.
    /// After max_attempts is reached, returns max_delay and sets exhausted.
    /// @return the computed delay for this attempt.
    [[nodiscard]] std::chrono::milliseconds next_delay();

    /// Reset all internal state (attempt counter, exhausted flag) to initial.
    /// Does NOT reseed the PRNG.
    void reset();

    // ---- Accessors / Queries ----

    /// True if max_attempts has been reached (when max_attempts != 0).
    [[nodiscard]] bool exhausted() const noexcept { return exhausted_; }

    /// Number of times next_delay() has been called since last reset().
    [[nodiscard]] int32_t attempts() const noexcept { return attempts_; }

    /// The last delay returned by next_delay() (initial_delay if never called).
    [[nodiscard]] std::chrono::milliseconds last_delay() const noexcept { return last_delay_; }

    /// Human-readable description for logging.
    [[nodiscard]] std::string describe() const;

    // ---- Configurable parameter accessors ----

    [[nodiscard]] std::chrono::milliseconds initial_delay() const noexcept { return initial_; }
    [[nodiscard]] std::chrono::milliseconds max_delay() const noexcept { return max_; }
    [[nodiscard]] double multiplier() const noexcept { return multiplier_; }
    [[nodiscard]] double jitter_factor() const noexcept { return jitter_; }
    [[nodiscard]] int32_t max_attempts() const noexcept { return max_attempts_; }

    // ---- Parameter mutation (reset before new config takes effect) ----

    void set_initial_delay(std::chrono::milliseconds d);
    void set_max_delay(std::chrono::milliseconds d);
    void set_multiplier(double m);
    void set_jitter_factor(double j);
    void set_max_attempts(int32_t a);

private:
    // ---- Internal helpers ----

    /// Generate a uniform random double in [0.0, 1.0). Thread-safe via mutex.
    [[nodiscard]] double random_factor();

    /// Clamp and validate delay ranges.
    [[nodiscard]] static std::chrono::milliseconds clamp_delay(
        std::chrono::milliseconds val,
        std::chrono::milliseconds lo,
        std::chrono::milliseconds hi) noexcept;

    // ---- Configuration ----

    std::chrono::milliseconds initial_;
    std::chrono::milliseconds max_;
    double                     multiplier_    = 2.0;
    double                     jitter_        = 0.25;
    int32_t                    max_attempts_  = 0;   // 0 = unlimited

    // ---- Runtime state ----

    int32_t                    attempts_      = 0;
    bool                       exhausted_     = false;
    std::chrono::milliseconds  last_delay_;

    // ---- PRNG (thread-safe, shared across all instances) ----
    //    Protected by a static mutex so that random_factor() can be called
    //    concurrently from multiple ExponentialBackoff objects.

    static std::mt19937_64     s_rng_;
    static std::mutex          s_rng_mutex_;
    static bool                s_rng_seeded_;
};

// ============================================================================
// Free-standing convenience helpers
// ============================================================================

/// Sleep for a backoff-aware duration and advance the backoff.
/// Returns false if exhausted, true otherwise.
[[nodiscard]] bool backoff_sleep(ExponentialBackoff& backoff);

/// Create a pre-configured backoff for network retries:
///   200ms → 10s, 2×, 25% jitter, unlimited.
[[nodiscard]] ExponentialBackoff network_backoff();

/// Create a pre-configured backoff for Raft RPC retries:
///   50ms → 5s, 2×, 25% jitter, 10 attempts max.
[[nodiscard]] ExponentialBackoff raft_backoff();

/// Create a pre-configured backoff for connector task retries:
///   1s → 60s, 2×, 25% jitter, unlimited.
[[nodiscard]] ExponentialBackoff connector_backoff();

/// Create a pre-configured backoff for storage I/O retries:
///   10ms → 1s, 2×, 50% jitter, 3 attempts.
[[nodiscard]] ExponentialBackoff storage_backoff();

/// Create a pre-configured backoff for proxy/HTTP retries:
///   500ms → 30s, 1.5×, 25% jitter, 5 attempts.
[[nodiscard]] ExponentialBackoff proxy_backoff();

} // namespace torrent
