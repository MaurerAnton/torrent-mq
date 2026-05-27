/**
 * backoff.cpp — ExponentialBackoff Implementation
 *
 * Implements a reusable exponential backoff with jitter for all torrent-mq
 * retry loops.  The core algorithm:
 *
 *   base = min(max_delay, initial_delay * multiplier^attempts)
 *   jittered = base * (1.0 - jitter_factor * U[0,1])
 *
 * Jitter is multiplicative (not additive) so that even at the max delay
 * there is spread, preventing thundering-herd synchronisation.
 *
 * All time values use std::chrono::milliseconds for interoperability
 * with std::this_thread::sleep_for and standard library clocks.
 *
 * Thread-safety of the static PRNG:
 *   The Mersenne Twister engine and its seeding are protected by a
 *   static std::mutex.  random_factor() acquires this mutex, so
 *   concurrent calls from different ExponentialBackoff instances
 *   are safe.  The per-instance state (attempts_, exhausted_,
 *   last_delay_) is NOT synchronised; each instance should be used
 *   from a single thread or wrapped with external locking.
 *
 * Dependencies:
 *   - backoff.h (this file's header)
 *   - <spdlog/spdlog.h> for optional diagnostics
 */

#include "torrent/common/backoff.h"

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>

namespace torrent {

// ============================================================================
// Static PRNG state
// ============================================================================

std::mt19937_64 ExponentialBackoff::s_rng_{};
std::mutex       ExponentialBackoff::s_rng_mutex_{};
bool             ExponentialBackoff::s_rng_seeded_{false};

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_backoff_logger() {
    static auto logger = spdlog::get("backoff");
    if (!logger) {
        logger = spdlog::stdout_color_mt("backoff");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Seeding helper
// --------------------------------------------------------------------------

void ensure_rng_seeded() {
    if (ExponentialBackoff::s_rng_seeded_) return;
    std::lock_guard<std::mutex> lock(ExponentialBackoff::s_rng_mutex_);
    if (ExponentialBackoff::s_rng_seeded_) return;

    std::random_device rd;
    // Seed with multiple entropy sources
    std::seed_seq seed{
        rd(), rd(), rd(), rd(),
        static_cast<uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count()),
        static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&rd))
    };
    ExponentialBackoff::s_rng_.seed(seed);
    ExponentialBackoff::s_rng_seeded_ = true;

    get_backoff_logger()->debug("ExponentialBackoff PRNG seeded");
}

// --------------------------------------------------------------------------
// Validation helpers
// --------------------------------------------------------------------------

/// Round a positive double to [lo,hi] with a warning log on clamp.
[[nodiscard]] double clamp_double(double val, double lo, double hi,
                                   std::string_view name) {
    if (val < lo) {
        get_backoff_logger()->warn("{}={:.3f} clamped to lowest={:.3f}",
                                    name, val, lo);
        return lo;
    }
    if (val > hi) {
        get_backoff_logger()->warn("{}={:.3f} clamped to highest={:.3f}",
                                    name, val, hi);
        return hi;
    }
    return val;
}

} // anonymous namespace

// ============================================================================
// Construction
// ============================================================================

ExponentialBackoff::ExponentialBackoff()
    : ExponentialBackoff(std::chrono::milliseconds(100),
                         std::chrono::seconds(30)) {}

ExponentialBackoff::ExponentialBackoff(std::chrono::milliseconds initial,
                                        std::chrono::milliseconds max,
                                        double mult,
                                        double jitter,
                                        int32_t attempts)
    : initial_(clamp_delay(initial, std::chrono::milliseconds(1),
                           std::chrono::hours(24)))
    , max_(clamp_delay(max, initial_,
                        std::chrono::hours(24)))
    , multiplier_(clamp_double(mult, 1.0, 100.0, "multiplier"))
    , jitter_(clamp_double(jitter, 0.0, 1.0, "jitter_factor"))
    , max_attempts_(std::max(attempts, 0))
    , attempts_(0)
    , exhausted_(false)
    , last_delay_(initial_)
{
    ensure_rng_seeded();
    get_backoff_logger()->debug(
        "ExponentialBackoff created: initial={}ms max={}ms mult={:.2f} "
        "jitter={:.2f} max_attempts={}",
        initial_.count(), max_.count(), multiplier_, jitter_, max_attempts_);
}

// ============================================================================
// Core API
// ============================================================================

std::chrono::milliseconds ExponentialBackoff::next_delay() {
    // Check exhaustion
    if (exhausted_) {
        get_backoff_logger()->trace("next_delay: already exhausted, returning max_delay");
        return max_;
    }
    if (max_attempts_ > 0 && attempts_ >= max_attempts_) {
        exhausted_ = true;
        get_backoff_logger()->warn(
            "next_delay: max_attempts={} reached, marking exhausted",
            max_attempts_);
        last_delay_ = max_;
        return max_;
    }

    // --- Compute base delay ---
    // base = initial_delay * multiplier^attempts, clamped to max_delay

    int64_t initial_ms = initial_.count();
    int64_t max_ms = max_.count();

    // Use double for exponentiation to avoid overflow on large attempts
    double base_d = static_cast<double>(initial_ms) *
                    std::pow(multiplier_, static_cast<double>(attempts_));
    auto base_ms = static_cast<int64_t>(std::llround(base_d));
    base_ms = std::clamp(base_ms, initial_ms, max_ms);

    // --- Apply jitter ---
    // jittered = base * (1.0 - jitter_factor * U[0,1))
    // So at jitter=0.25, the range is [0.75*base, 1.0*base]

    double factor = 1.0 - jitter_ * random_factor();
    auto jittered_ms = static_cast<int64_t>(
        std::llround(static_cast<double>(base_ms) * factor));

    // Clamp jittered result: never below initial_delay, never above max_delay
    jittered_ms = std::clamp(jittered_ms, initial_ms, max_ms);

    // --- Advance state ---
    last_delay_ = std::chrono::milliseconds(jittered_ms);
    ++attempts_;

    // Check if this was the last allowed attempt
    if (max_attempts_ > 0 && attempts_ >= max_attempts_) {
        exhausted_ = true;
        get_backoff_logger()->debug(
            "next_delay: attempt {} of {} → {}ms (exhausted)",
            attempts_, max_attempts_, jittered_ms);
    } else {
        get_backoff_logger()->trace(
            "next_delay: attempt {} → {}ms (base={}ms, jitter={:.3f})",
            attempts_, jittered_ms, base_ms, factor);
    }

    return last_delay_;
}

void ExponentialBackoff::reset() {
    attempts_   = 0;
    exhausted_  = false;
    last_delay_ = initial_;
    get_backoff_logger()->debug("ExponentialBackoff reset to initial state");
}

// ============================================================================
// Descriptive output
// ============================================================================

std::string ExponentialBackoff::describe() const {
    std::ostringstream oss;
    oss << "ExponentialBackoff{initial=" << initial_.count()
        << "ms, max=" << max_.count()
        << "ms, mult=" << multiplier_
        << ", jitter=" << jitter_
        << ", max_attempts=";
    if (max_attempts_ == 0)
        oss << "unlimited";
    else
        oss << max_attempts_;
    oss << ", attempts=" << attempts_
        << ", exhausted=" << (exhausted_ ? "yes" : "no")
        << ", last_delay=" << last_delay_.count() << "ms}";
    return oss.str();
}

// ============================================================================
// Clamping
// ============================================================================

std::chrono::milliseconds ExponentialBackoff::clamp_delay(
    std::chrono::milliseconds val,
    std::chrono::milliseconds lo,
    std::chrono::milliseconds hi) noexcept
{
    if (val < lo) return lo;
    if (val > hi) return hi;
    return val;
}

// ============================================================================
// Parameter mutation
// ============================================================================

void ExponentialBackoff::set_initial_delay(std::chrono::milliseconds d) {
    initial_ = clamp_delay(d, std::chrono::milliseconds(1),
                           std::chrono::hours(24));
    get_backoff_logger()->debug("initial_delay set to {}ms", initial_.count());
}

void ExponentialBackoff::set_max_delay(std::chrono::milliseconds d) {
    max_ = clamp_delay(d, initial_, std::chrono::hours(24));
    get_backoff_logger()->debug("max_delay set to {}ms", max_.count());
}

void ExponentialBackoff::set_multiplier(double m) {
    multiplier_ = clamp_double(m, 1.0, 100.0, "multiplier");
    get_backoff_logger()->debug("multiplier set to {:.3f}", multiplier_);
}

void ExponentialBackoff::set_jitter_factor(double j) {
    jitter_ = clamp_double(j, 0.0, 1.0, "jitter_factor");
    get_backoff_logger()->debug("jitter_factor set to {:.3f}", jitter_);
}

void ExponentialBackoff::set_max_attempts(int32_t a) {
    max_attempts_ = std::max<int32_t>(a, 0);
    get_backoff_logger()->debug("max_attempts set to {}", max_attempts_);
}

// ============================================================================
// PRNG helper
// ============================================================================

double ExponentialBackoff::random_factor() {
    ensure_rng_seeded();
    std::lock_guard<std::mutex> lock(s_rng_mutex_);

    // Generate a uniform double in [0.0, 1.0)
    static std::uniform_real_distribution<double> dist(0.0, 1.0);
    return dist(s_rng_);
}

// ============================================================================
// Convenience helpers
// ============================================================================

bool backoff_sleep(ExponentialBackoff& backoff) {
    if (backoff.exhausted()) return false;
    auto delay = backoff.next_delay();
    std::this_thread::sleep_for(delay);
    return true;
}

ExponentialBackoff network_backoff() {
    return ExponentialBackoff(
        std::chrono::milliseconds(200),
        std::chrono::seconds(10),
        2.0, 0.25, 0);  // unlimited
}

ExponentialBackoff raft_backoff() {
    return ExponentialBackoff(
        std::chrono::milliseconds(50),
        std::chrono::seconds(5),
        2.0, 0.25, 10);
}

ExponentialBackoff connector_backoff() {
    return ExponentialBackoff(
        std::chrono::seconds(1),
        std::chrono::seconds(60),
        2.0, 0.25, 0);  // unlimited
}

ExponentialBackoff storage_backoff() {
    return ExponentialBackoff(
        std::chrono::milliseconds(10),
        std::chrono::seconds(1),
        2.0, 0.50, 3);
}

ExponentialBackoff proxy_backoff() {
    return ExponentialBackoff(
        std::chrono::milliseconds(500),
        std::chrono::seconds(30),
        1.5, 0.25, 5);
}

} // namespace torrent
