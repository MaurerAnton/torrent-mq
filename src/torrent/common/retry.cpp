/**
 * torrent-mq — RetryPolicy Implementation
 *
 * Provides a configurable retry mechanism with exponential backoff
 * for all torrent-mq subsystems that need resilience against transient
 * failures: network connections, Raft RPCs, connector tasks, storage I/O,
 * and proxy requests.
 *
 * Builds on top of ExponentialBackoff (backoff.h) to provide:
 *   - RetryPolicy: bundles backoff configuration with error classification
 *   - retry(fn, policy): execute a callable with automatic retries
 *   - is_retryable_error(): determine if an error_code warrants a retry
 *   - Pre-built policies for common use cases
 *
 * Thread safety: RetryPolicy instances are not thread-safe; each call
 * site should use its own instance. The underlying PRNG in
 * ExponentialBackoff is internally synchronised.
 */

#include "torrent/common/backoff.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

namespace torrent {

// ============================================================================
// Anonymous namespace — logger
// ============================================================================

namespace {

std::shared_ptr<spdlog::logger> get_logger() {
    static auto logger = spdlog::get("retry");
    if (!logger) {
        logger = spdlog::stdout_color_mt("retry");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

} // anonymous namespace

// ============================================================================
// RetryPolicy — configuration for retry loops
// ============================================================================

/**
 * RetryPolicy bundles an ExponentialBackoff configuration with a
 * predicate that classifies which errors are retryable.
 *
 * Usage:
 * @code
 *   RetryPolicy policy;
 *   policy.max_attempts = 5;
 *   policy.initial_backoff_ms = 200;
 *
 *   auto result = retry([&]() {
 *       return do_rpc();
 *   }, policy);
 * @endcode
 */
struct RetryPolicy {
    // ---- Retry behaviour ----

    /// Maximum number of attempts (including the first try).
    /// 0 = unlimited. Default: 5.
    int32_t max_attempts = 5;

    /// Initial backoff delay in milliseconds. Default: 100ms.
    int64_t initial_backoff_ms = 100;

    /// Maximum backoff delay in milliseconds. Default: 30'000ms (30s).
    int64_t max_backoff_ms = 30'000;

    /// Backoff multiplier per attempt. Default: 2.0.
    double multiplier = 2.0;

    /// Jitter factor (0.0–1.0) to smear retry timing.
    /// 0.0 = no jitter, 1.0 = full randomisation in [0, delay].
    /// Default: 0.25.
    double jitter = 0.25;

    // ---- Error classification ----

    /**
     * User-provided predicate: returns true if the given error_code
     * should trigger a retry. If nullptr (default), the built-in
     * is_retryable_error() is used.
     *
     * This allows callers to customise which errors are retryable
     * per call site (e.g., a produce request might retry on
     * NOT_LEADER but a fetch request might not).
     */
    std::function<bool(error_code)> is_retryable = nullptr;

    // ---- Behaviour flags ----

    /// If true, re-throw the last exception after exhausting retries.
    /// If false, return the last error result (for result<T>-based APIs).
    /// Default: true.
    bool rethrow_on_exhausted = true;

    /// Human-readable name for logging context. Default: "retry".
    std::string operation_name = "retry";

    // ---- Factory methods for common policies ----

    /// Fast retry for storage I/O (10ms → 1s, 3 attempts, 50% jitter).
    static RetryPolicy storage() {
        RetryPolicy p;
        p.max_attempts       = 3;
        p.initial_backoff_ms = 10;
        p.max_backoff_ms     = 1'000;
        p.multiplier         = 2.0;
        p.jitter             = 0.50;
        p.operation_name     = "storage-retry";
        return p;
    }

    /// Network retry (200ms → 10s, unlimited, 25% jitter).
    static RetryPolicy network() {
        RetryPolicy p;
        p.max_attempts       = 0;  // unlimited
        p.initial_backoff_ms = 200;
        p.max_backoff_ms     = 10'000;
        p.multiplier         = 2.0;
        p.jitter             = 0.25;
        p.operation_name     = "network-retry";
        return p;
    }

    /// Raft RPC retry (50ms → 5s, 10 attempts, 25% jitter).
    static RetryPolicy raft() {
        RetryPolicy p;
        p.max_attempts       = 10;
        p.initial_backoff_ms = 50;
        p.max_backoff_ms     = 5'000;
        p.multiplier         = 2.0;
        p.jitter             = 0.25;
        p.operation_name     = "raft-retry";
        return p;
    }

    /// Proxy/HTTP retry (500ms → 30s, 5 attempts, 25% jitter).
    static RetryPolicy proxy() {
        RetryPolicy p;
        p.max_attempts       = 5;
        p.initial_backoff_ms = 500;
        p.max_backoff_ms     = 30'000;
        p.multiplier         = 1.5;
        p.jitter             = 0.25;
        p.operation_name     = "proxy-retry";
        return p;
    }

    /// Connector task retry (1s → 60s, unlimited, 25% jitter).
    static RetryPolicy connector() {
        RetryPolicy p;
        p.max_attempts       = 0;  // unlimited
        p.initial_backoff_ms = 1'000;
        p.max_backoff_ms     = 60'000;
        p.multiplier         = 2.0;
        p.jitter             = 0.25;
        p.operation_name     = "connector-retry";
        return p;
    }

    /// Build the underlying ExponentialBackoff from this policy.
    [[nodiscard]] ExponentialBackoff to_backoff() const {
        return ExponentialBackoff(
            std::chrono::milliseconds(initial_backoff_ms),
            std::chrono::milliseconds(max_backoff_ms),
            multiplier,
            jitter,
            max_attempts);
    }

    /// Describe this policy for logging.
    [[nodiscard]] std::string describe() const {
        auto bo = to_backoff();
        auto desc = bo.describe();
        return "RetryPolicy[" + std::string(operation_name) + "] " + desc;
    }
};

// ============================================================================
// is_retryable_error — built-in error classification
// ============================================================================

/**
 * Determine whether an error_code represents a transient failure that
 * warrants a retry attempt.
 *
 * Retryable errors include:
 *   - Network / transport failures (connection loss, timeouts, unavailability)
 *   - Leadership changes (not leader, leader not available, stale epochs)
 *   - Coordinator transitions (not coordinator, coordinator not available)
 *   - Temporary resource exhaustion (queue full, throttled)
 *   - Rebalance / reassignment in progress
 *   - Shutdown in progress (to drain gracefully)
 *
 * Non-retryable errors include:
 *   - Data corruption, invalid configuration, authorisation failures,
 *     schema incompatibility, duplicate sequence numbers, etc.
 *
 * Callers can override this via RetryPolicy::is_retryable predicate.
 */
[[nodiscard]] bool is_retryable_error(error_code ec) noexcept {
    switch (ec) {
        // --- Network / transport ---
        case error_code::network_exception:
        case error_code::request_timed_out:
        case error_code::broker_not_available:
        case error_code::replica_not_available:
        case error_code::proxy_unavailable:
            return true;

        // --- Leadership / controller ---
        case error_code::leader_not_available:
        case error_code::not_leader_for_partition:
        case error_code::not_controller:
        case error_code::stale_controller_epoch:
        case error_code::stale_broker_epoch:
        case error_code::fenced_leader_epoch:
        case error_code::unknown_leader_epoch:
            return true;

        // --- Coordinator ---
        case error_code::coordinator_not_available:
        case error_code::not_coordinator:
        case error_code::coordinator_load_in_progress:
            return true;

        // --- Rebalance / reassignment ---
        case error_code::rebalance_in_progress:
        case error_code::reassignment_in_progress:
        case error_code::preferred_leader_not_available:
        case error_code::eligible_leaders_not_available:
            return true;

        // --- Temporary resource exhaustion ---
        case error_code::internal_queue_full:
        case error_code::throttling_quota_exceeded:
        case error_code::not_enough_replicas:
        case error_code::not_enough_replicas_after_append:
            return true;

        // --- Transient storage ---
        case error_code::storage_unavailable:
        case error_code::recovery_needed:
            return true;

        // --- Shutdown (drain mode) ---
        case error_code::shutdown_in_progress:
            return true;

        // --- Transient transaction state ---
        case error_code::concurrent_transactions:
        case error_code::transaction_coordinator_fenced:
        case error_code::unknown_server_error:
            return true;

        // --- Explicitly non-retryable ---
        default:
            return false;
    }
}

// ============================================================================
// retry() — execute a callable with configurable retry logic
// ============================================================================

namespace detail {

/**
 * Internal helper: sleep for the computed backoff delay.
 * Logs at appropriate level.
 */
void backoff_sleep_ms(ExponentialBackoff& backoff, int32_t attempt,
                      std::string_view operation) {
    auto delay = backoff.next_delay();
    get_logger()->debug("{} attempt {}: sleeping for {}ms",
                        operation, attempt, delay.count());
    std::this_thread::sleep_for(delay);
}

} // namespace detail

/**
 * Execute a callable with retry logic.
 *
 * The callable must return a result<T> where result<T> has:
 *   - .ok() / .failed()
 *   - .error (error_code)
 *   - .error_message (std::string)
 *   - .value (T)
 *
 * If the callable throws an exception, it is caught and treated as a
 * retryable error unless the policy specifies otherwise.
 *
 * @tparam F  Callable type returning result<T>
 * @param fn  The operation to retry
 * @param policy  Retry configuration
 * @return The successful result, or the last error result after exhaustion
 *
 * Example:
 * @code
 *   auto res = retry([&]() {
 *       return broker.produce(topic, msg);
 *   }, RetryPolicy::network());
 *   if (res.failed()) { ... }
 * @endcode
 */
template <typename F>
auto retry(F&& fn, const RetryPolicy& policy)
    -> decltype(std::forward<F>(fn)())
{
    using result_type = decltype(std::forward<F>(fn)());
    static_assert(std::is_copy_constructible_v<result_type>,
                  "retry() requires a copyable result type");

    ExponentialBackoff backoff = policy.to_backoff();
    int32_t attempt = 0;
    result_type last_result;

    get_logger()->debug("Starting retry loop for '{}' (max_attempts={})",
                        policy.operation_name,
                        policy.max_attempts == 0 ? -1 : policy.max_attempts);

    while (true) {
        ++attempt;

        try {
            last_result = fn();
        } catch (const std::exception& e) {
            get_logger()->warn("{} attempt {} threw exception: {}",
                               policy.operation_name, attempt, e.what());

            if (backoff.exhausted()) {
                get_logger()->error("{} exhausted after {} attempts (exception)",
                                    policy.operation_name, attempt);
                if (policy.rethrow_on_exhausted) throw;
                // Build an error result
                last_result.error = error_code::unknown_server_error;
                last_result.error_message = std::string("Exception after ") +
                    std::to_string(attempt) + " attempts: " + e.what();
                return last_result;
            }

            detail::backoff_sleep_ms(backoff, attempt, policy.operation_name);
            continue;
        }

        // Check if the operation succeeded
        if (last_result.ok()) {
            if (attempt > 1) {
                get_logger()->info("{} succeeded on attempt {}",
                                   policy.operation_name, attempt);
            }
            return last_result;
        }

        // Operation failed — decide whether to retry
        error_code ec = last_result.error;
        bool retryable = false;

        if (policy.is_retryable) {
            // Use user-provided predicate
            retryable = policy.is_retryable(ec);
        } else {
            // Use built-in classification
            retryable = is_retryable_error(ec);
        }

        if (!retryable) {
            get_logger()->warn("{} attempt {}: non-retryable error {} ({}): {}",
                               policy.operation_name, attempt,
                               static_cast<int>(ec), error_code_name(ec),
                               last_result.error_message);
            return last_result;
        }

        get_logger()->debug("{} attempt {}: retryable error {} ({}): {}",
                            policy.operation_name, attempt,
                            static_cast<int>(ec), error_code_name(ec),
                            last_result.error_message);

        if (backoff.exhausted()) {
            get_logger()->error("{} exhausted after {} attempts: last error {} ({})",
                                policy.operation_name, attempt,
                                static_cast<int>(ec), error_code_name(ec));
            return last_result;
        }

        detail::backoff_sleep_ms(backoff, attempt, policy.operation_name);
    }
}

/**
 * Overload of retry() for void-returning callables.
 * The callable must return void and throw on failure.
 * On exhaustion with rethrow_on_exhausted=true, the last exception
 * is propagated.
 */
template <typename F>
auto retry_void(F&& fn, const RetryPolicy& policy)
    -> std::enable_if_t<std::is_void_v<decltype(std::forward<F>(fn)())>, void>
{
    ExponentialBackoff backoff = policy.to_backoff();
    int32_t attempt = 0;
    std::string last_error;

    get_logger()->debug("Starting void retry loop for '{}'",
                        policy.operation_name);

    while (true) {
        ++attempt;

        try {
            fn();
            if (attempt > 1) {
                get_logger()->info("{} succeeded on attempt {}",
                                   policy.operation_name, attempt);
            }
            return;
        } catch (const std::exception& e) {
            last_error = e.what();
            get_logger()->warn("{} attempt {} threw: {}",
                               policy.operation_name, attempt, last_error);

            if (backoff.exhausted()) {
                get_logger()->error("{} exhausted after {} attempts",
                                    policy.operation_name, attempt);
                if (policy.rethrow_on_exhausted) throw;
                return;
            }
        }

        detail::backoff_sleep_ms(backoff, attempt, policy.operation_name);
    }
}

// ============================================================================
// Convenience: retry with default policies
// ============================================================================

/// Retry with network policy. Equivalent to retry(fn, RetryPolicy::network()).
template <typename F>
auto retry_network(F&& fn) -> decltype(std::forward<F>(fn)()) {
    return retry(std::forward<F>(fn), RetryPolicy::network());
}

/// Retry with storage policy.
template <typename F>
auto retry_storage(F&& fn) -> decltype(std::forward<F>(fn)()) {
    return retry(std::forward<F>(fn), RetryPolicy::storage());
}

/// Retry with Raft policy.
template <typename F>
auto retry_raft(F&& fn) -> decltype(std::forward<F>(fn)()) {
    return retry(std::forward<F>(fn), RetryPolicy::raft());
}

/// Retry with proxy policy.
template <typename F>
auto retry_proxy(F&& fn) -> decltype(std::forward<F>(fn)()) {
    return retry(std::forward<F>(fn), RetryPolicy::proxy());
}

} // namespace torrent
