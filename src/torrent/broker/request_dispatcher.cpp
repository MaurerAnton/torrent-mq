/**
 * request_dispatcher.cpp — RequestDispatcher: API Routing & Threaded Dispatch
 *
 * Routes all client-facing API requests to their registered handlers.
 * Maintains a thread pool for parallel processing, enforces request
 * throttling with backpressure, matches responses via correlation IDs,
 * negotiates API versions, and collects per-request metrics.
 *
 * Architecture:
 *   - Handler registry: maps API key (int16_t) to handler_fn
 *   - Thread pool: configurable concurrency for CPU-bound work
 *   - Backpressure: bounded queue depth rejects requests under overload
 *   - Correlation: each request tagged with a correlation_id for async matching
 *   - Version negotiation: rejects unsupported API versions with clear errors
 *   - Throttling: per-client and global rate limits via token bucket
 *   - Metrics: latency histograms, throughput counters, error rates
 *
 * Dependencies:
 *   - BrokerServer: for config (thread count, queue depth) and shutdown state
 *   - thread_pool: for worker threads (from torrent/common)
 *   - config: for rate-limit and queue-depth settings
 *
 * See request_dispatcher.h for the public API contract.
 */

#include "torrent/broker/request_dispatcher.h"
#include "torrent/broker/server.h"
#include "torrent/common/thread_pool.h"
#include "torrent/common/types.h"
#include "torrent/common/config.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

// ============================================================================
// Aliases for readability
// ============================================================================

using namespace std::chrono_literals;

namespace torrent::broker {

// ============================================================================
// Anonymous namespace — internal types, constants, helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_disp_logger() {
    static auto logger = []() {
        auto l = spdlog::get("request_dispatcher");
        if (!l) {
            l = spdlog::stdout_color_mt("request_dispatcher");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define RD_LOG_INFO(...)  get_disp_logger()->info(__VA_ARGS__)
#define RD_LOG_WARN(...)  get_disp_logger()->warn(__VA_ARGS__)
#define RD_LOG_ERROR(...) get_disp_logger()->error(__VA_ARGS__)
#define RD_LOG_DEBUG(...) get_disp_logger()->debug(__VA_ARGS__)
#define RD_LOG_TRACE(...) get_disp_logger()->trace(__VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Default maximum number of concurrent in-flight requests.
static constexpr size_t kDefaultMaxConcurrentRequests = 10000;

/// Default maximum queue depth before backpressure kicks in.
static constexpr size_t kDefaultMaxQueueDepth = 5000;

/// Soft backpressure threshold (fraction of queue depth).
static constexpr double kBackpressureThreshold = 0.75;

/// Default request timeout in milliseconds.
static constexpr int64_t kDefaultRequestTimeoutMs = 30000;

/// Maximum API version we support to advertise.
static constexpr int16_t kMaxSupportedAPIVersionMajor = 3;

/// Minimum API version we accept from clients.
static constexpr int16_t kMinSupportedAPIVersionMajor = 0;

/// Maximum number of handlers that can be registered.
static constexpr size_t kMaxHandlers = 256;

/// Correlation ID mask / range for clients vs internal.
static constexpr int32_t kClientCorrelationBase = 0;
static constexpr int32_t kInternalCorrelationBase = 1'000'000'000;

/// Maximum correlation ID (wraparound guard).
static constexpr int32_t kMaxCorrelationID = 2'147'483'647;

/// Token bucket refill interval.
static constexpr auto kTokenBucketRefillInterval = 100ms;

/// Default global rate limit (requests/sec). 0 = unlimited.
static constexpr double kDefaultGlobalRateLimit = 0.0;

/// Default per-client rate limit (requests/sec). 0 = unlimited.
static constexpr double kDefaultPerClientRateLimit = 0.0;

// --------------------------------------------------------------------------
// RequestContext — metadata attached to each in-flight request
// --------------------------------------------------------------------------

/**
 * Tracks a single in-flight request from dispatch to completion.
 * Used for timeout detection, correlation matching, and metrics.
 */
struct RequestContext {
    int32_t correlation_id = 0;
    int16_t api_key = 0;
    int16_t api_version = 0;
    int32_t client_id_hash = 0;      // Hashed client identity for per-client tracking
    buffer_view request_body;

    std::chrono::steady_clock::time_point enqueue_time;
    std::chrono::steady_clock::time_point dispatch_time;
    std::chrono::steady_clock::time_point deadline;

    std::promise<shared_buffer> response_promise;
    std::shared_future<shared_buffer> response_future;

    bool timed_out = false;
    bool completed = false;

    RequestContext()
        : response_future(response_promise.get_future().share()) {}
};

// --------------------------------------------------------------------------
// TokenBucket — rate limiter primitive
// --------------------------------------------------------------------------

/**
 * Simple token bucket for rate limiting.
 * Tokens refill at a configurable rate up to a burst size.
 */
class TokenBucket {
public:
    explicit TokenBucket(double rate_per_sec, double burst_size = 0.0)
        : rate_per_sec_(rate_per_sec)
        , burst_size_(burst_size > 0.0 ? burst_size : rate_per_sec)
        , tokens_(burst_size_)
        , last_refill_(std::chrono::steady_clock::now())
    {}

    /**
     * Attempt to consume one token. Returns true if allowed.
     */
    [[nodiscard]] bool consume() {
        if (rate_per_sec_ <= 0.0) return true; // Unlimited

        std::lock_guard<std::mutex> lock(mutex_);
        refill();
        if (tokens_ >= 1.0) {
            tokens_ -= 1.0;
            return true;
        }
        return false;
    }

    /**
     * Attempt to consume `count` tokens. Returns the number actually consumed.
     */
    [[nodiscard]] size_t consume(size_t count) {
        if (rate_per_sec_ <= 0.0) return count; // Unlimited

        std::lock_guard<std::mutex> lock(mutex_);
        refill();
        size_t allowed = static_cast<size_t>(std::min(tokens_, static_cast<double>(count)));
        tokens_ -= static_cast<double>(allowed);
        return allowed;
    }

    /// Current token count (for metrics).
    [[nodiscard]] double available() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return tokens_;
    }

private:
    void refill() {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration<double>(now - last_refill_).count();
        tokens_ = std::min(burst_size_, tokens_ + elapsed * rate_per_sec_);
        last_refill_ = now;
    }

    double rate_per_sec_;
    double burst_size_;
    double tokens_;
    std::chrono::steady_clock::time_point last_refill_;
    mutable std::mutex mutex_;
};

// --------------------------------------------------------------------------
// HandlerEntry — metadata for a registered handler
// --------------------------------------------------------------------------

/**
 * Metadata for a single registered API handler.
 */
struct HandlerEntry {
    int16_t api_key;
    int16_t min_version;
    int16_t max_version;
    RequestDispatcher::handler_fn handler;
    std::string name;               // Human-readable name for logging
    std::atomic<uint64_t> call_count{0};
    std::atomic<uint64_t> error_count{0};

    /// Check whether a requested API version is supported.
    [[nodiscard]] bool supports_version(int16_t version) const noexcept {
        return version >= min_version && version <= max_version;
    }
};

// --------------------------------------------------------------------------
// LatencyHistogram — simple rolling histogram for request latency
// --------------------------------------------------------------------------

/**
 * Approximate latency histogram using pre-defined buckets.
 * Buckets are in microseconds: 100, 500, 1000, 5000, 10000, 50000, 100000,
 * 500000, 1000000, 5000000.
 */
class LatencyHistogram {
public:
    LatencyHistogram() {
        counts_.resize(kNumBuckets, 0);
    }

    void record(std::chrono::microseconds latency) {
        ++total_count_;
        total_sum_us_ += latency.count();
        auto us = latency.count();
        for (size_t i = 0; i < kNumBuckets; ++i) {
            if (us <= bucket_bounds_[i]) {
                counts_[i].fetch_add(1, std::memory_order_relaxed);
                return;
            }
        }
        // Overflow bucket
        counts_[kNumBuckets - 1].fetch_add(1, std::memory_order_relaxed);
    }

    void record_error() {
        error_count_.fetch_add(1, std::memory_order_relaxed);
    }

    /// Snapshot for metrics export.
    struct Snapshot {
        uint64_t total_count = 0;
        uint64_t error_count = 0;
        int64_t total_sum_us = 0;
        std::vector<uint64_t> bucket_counts;
    };

    [[nodiscard]] Snapshot snapshot() const {
        Snapshot snap;
        snap.total_count = total_count_.load(std::memory_order_acquire);
        snap.error_count = error_count_.load(std::memory_order_acquire);
        snap.total_sum_us = total_sum_us_.load(std::memory_order_acquire);
        snap.bucket_counts.resize(kNumBuckets);
        for (size_t i = 0; i < kNumBuckets; ++i) {
            snap.bucket_counts[i] = counts_[i].load(std::memory_order_acquire);
        }
        return snap;
    }

private:
    static constexpr size_t kNumBuckets = 10;
    static constexpr int64_t bucket_bounds_[kNumBuckets] = {
        100, 500, 1000, 5000, 10000, 50000,
        100000, 500000, 1000000, 5000000
    };

    std::atomic<uint64_t> total_count_{0};
    std::atomic<uint64_t> error_count_{0};
    std::atomic<int64_t> total_sum_us_{0};
    std::vector<std::atomic<uint64_t>> counts_;
};

// --------------------------------------------------------------------------
// Per-API-key metrics
// --------------------------------------------------------------------------

struct ApiMetrics {
    LatencyHistogram latency;
    std::atomic<uint64_t> requests_total{0};
    std::atomic<uint64_t> requests_rejected{0};
    std::atomic<uint64_t> requests_timed_out{0};
    std::atomic<uint64_t> bytes_received{0};
    std::atomic<uint64_t> bytes_sent{0};
};

// --------------------------------------------------------------------------
// Version negotiation helper
// --------------------------------------------------------------------------

/**
 * Negotiate an API version between client request and server capability.
 * Returns the negotiated version or an error.
 */
[[nodiscard]] std::optional<int16_t> negotiate_version(
    int16_t api_key,
    int16_t requested_version,
    const std::unordered_map<int16_t, HandlerEntry>& handlers) {

    auto it = handlers.find(api_key);
    if (it == handlers.end()) {
        RD_LOG_WARN("Version negotiation: unknown API key {}", api_key);
        return std::nullopt;
    }

    const auto& entry = it->second;

    // If client version is within supported range, accept it
    if (requested_version >= entry.min_version &&
        requested_version <= entry.max_version) {
        return requested_version;
    }

    // If client version is higher than max, we can downgrade to max
    if (requested_version > entry.max_version) {
        RD_LOG_DEBUG("API key {}: client v{} > server max v{}, downgrading to v{}",
                     api_key, requested_version, entry.max_version, entry.max_version);
        return entry.max_version;
    }

    // If client version is lower than min, reject
    RD_LOG_WARN("API key {}: client v{} < server min v{}, rejecting",
               api_key, requested_version, entry.min_version);
    return std::nullopt;
}

// --------------------------------------------------------------------------
// Correlation ID generator (thread-safe)
// --------------------------------------------------------------------------

class CorrelationIDGenerator {
public:
    CorrelationIDGenerator() : next_id_(kClientCorrelationBase) {}

    [[nodiscard]] int32_t next() {
        int32_t id = next_id_.fetch_add(1, std::memory_order_relaxed);
        if (id >= kMaxCorrelationID) {
            // Wrap around, but avoid internal range
            next_id_.store(kClientCorrelationBase, std::memory_order_relaxed);
            return next();
        }
        return id;
    }

private:
    std::atomic<int32_t> next_id_;
};

} // anonymous namespace

// ============================================================================
// RequestDispatcher::Impl — PIMPL
// ============================================================================

struct RequestDispatcher::Impl {
    /// Registered handlers, keyed by API key.
    std::unordered_map<int16_t, HandlerEntry> handlers;

    /// Protects the handler map.
    mutable std::shared_mutex handlers_mutex;

    /// Thread pool for parallel request processing.
    std::unique_ptr<torrent::thread_pool> worker_pool;

    /// Correlation ID generator.
    CorrelationIDGenerator correlation_gen;

    /// In-flight request contexts, keyed by correlation ID.
    std::unordered_map<int32_t, std::shared_ptr<RequestContext>> in_flight;

    /// Protects the in_flight map.
    mutable std::shared_mutex in_flight_mutex;

    /// Global token bucket for rate limiting.
    std::unique_ptr<TokenBucket> global_bucket;

    /// Per-client token buckets, keyed by hashed client ID.
    std::unordered_map<int32_t, std::unique_ptr<TokenBucket>> client_buckets;

    /// Protects client_buckets.
    mutable std::shared_mutex client_buckets_mutex;

    /// Per-API-key metrics.
    std::unordered_map<int16_t, std::unique_ptr<ApiMetrics>> api_metrics;
    mutable std::shared_mutex metrics_mutex;

    /// Aggregate metrics.
    std::unique_ptr<ApiMetrics> aggregate_metrics;

    /// Background thread for timeout detection.
    std::thread timeout_thread;
    std::atomic<bool> timeout_running{false};

    /// Background thread for token bucket refill.
    std::thread refill_thread;
    std::atomic<bool> refill_running{false};

    /// Running flag.
    std::atomic<bool> running{false};

    /// Config-sourced limits.
    size_t max_concurrent_requests = kDefaultMaxConcurrentRequests;
    size_t max_queue_depth = kDefaultMaxQueueDepth;
    int64_t request_timeout_ms = kDefaultRequestTimeoutMs;
    double global_rate_limit = kDefaultGlobalRateLimit;
    double per_client_rate_limit = kDefaultPerClientRateLimit;
};

// ============================================================================
// RequestDispatcher — Constructor / Destructor
// ============================================================================

RequestDispatcher::RequestDispatcher(BrokerServer& s)
    : server_(&s)
    , impl_(std::make_unique<Impl>())
{
    impl_->aggregate_metrics = std::make_unique<ApiMetrics>();
    RD_LOG_INFO("RequestDispatcher initialized");
}

RequestDispatcher::~RequestDispatcher() {
    if (impl_->running.load(std::memory_order_acquire)) {
        try { shutdown(); } catch (...) {}
    }
    RD_LOG_INFO("RequestDispatcher destroyed");
}

// ============================================================================
// RequestDispatcher — start()
// ============================================================================

void RequestDispatcher::start() {
    if (impl_->running.load(std::memory_order_acquire)) {
        RD_LOG_WARN("RequestDispatcher::start() called while already running");
        return;
    }

    const auto& cfg = server_->config();

    // Resolve thread pool size from config or hardware concurrency
    size_t num_workers = cfg.num_worker_threads > 0
        ? static_cast<size_t>(cfg.num_worker_threads)
        : std::thread::hardware_concurrency();
    if (num_workers == 0) num_workers = 4;

    impl_->worker_pool = std::make_unique<torrent::thread_pool>(
        num_workers, "req-dispatch");

    // Initialize token bucket for global rate limiting
    impl_->global_bucket = std::make_unique<TokenBucket>(
        impl_->global_rate_limit, impl_->global_rate_limit * 2.0);

    // Start background threads
    impl_->timeout_running.store(true, std::memory_order_release);
    impl_->timeout_thread = std::thread(&RequestDispatcher::timeout_loop, this);

    impl_->refill_running.store(true, std::memory_order_release);
    impl_->refill_thread = std::thread(&RequestDispatcher::refill_loop, this);

    impl_->running.store(true, std::memory_order_release);
    RD_LOG_INFO("RequestDispatcher started with {} worker threads", num_workers);
}

// ============================================================================
// RequestDispatcher — shutdown()
// ============================================================================

void RequestDispatcher::shutdown() {
    if (!impl_->running.load(std::memory_order_acquire)) {
        return;
    }

    RD_LOG_INFO("RequestDispatcher shutting down...");

    // Stop background threads first
    impl_->refill_running.store(false, std::memory_order_release);
    if (impl_->refill_thread.joinable()) {
        impl_->refill_thread.join();
    }

    impl_->timeout_running.store(false, std::memory_order_release);
    if (impl_->timeout_thread.joinable()) {
        impl_->timeout_thread.join();
    }

    // Drain the worker pool (graceful)
    if (impl_->worker_pool) {
        impl_->worker_pool->shutdown();
        impl_->worker_pool.reset();
    }

    // Cancel any remaining in-flight requests
    {
        std::unique_lock lock(impl_->in_flight_mutex);
        for (auto& [corr_id, ctx] : impl_->in_flight) {
            if (!ctx->completed && !ctx->timed_out) {
                ctx->timed_out = true;
                try {
                    ctx->response_promise.set_value(shared_buffer{});
                } catch (...) {
                    // Already set — ignore
                }
            }
        }
        impl_->in_flight.clear();
    }

    impl_->running.store(false, std::memory_order_release);
    RD_LOG_INFO("RequestDispatcher shut down");
}

// ============================================================================
// RequestDispatcher — register_handler()
// ============================================================================

void RequestDispatcher::register_handler(int16_t api_key, handler_fn handler) {
    if (!handler) {
        RD_LOG_WARN("register_handler: null handler for API key {}, ignoring", api_key);
        return;
    }

    std::unique_lock lock(impl_->handlers_mutex);

    if (impl_->handlers.size() >= kMaxHandlers) {
        RD_LOG_ERROR("register_handler: maximum handler count ({}) reached", kMaxHandlers);
        return;
    }

    HandlerEntry entry;
    entry.api_key = api_key;
    entry.handler = std::move(handler);

    // Set version ranges based on well-known API keys
    switch (api_key) {
    case 0:  // Produce
        entry.name = "Produce";
        entry.min_version = 0; entry.max_version = 9;
        break;
    case 1:  // Fetch
        entry.name = "Fetch";
        entry.min_version = 0; entry.max_version = 13;
        break;
    case 2:  // ListOffsets
        entry.name = "ListOffsets";
        entry.min_version = 0; entry.max_version = 7;
        break;
    case 3:  // Metadata
        entry.name = "Metadata";
        entry.min_version = 0; entry.max_version = 12;
        break;
    case 4:  // LeaderAndISR
        entry.name = "LeaderAndISR";
        entry.min_version = 0; entry.max_version = 5;
        break;
    case 5:  // StopReplica
        entry.name = "StopReplica";
        entry.min_version = 0; entry.max_version = 3;
        break;
    case 6:  // UpdateMetadata
        entry.name = "UpdateMetadata";
        entry.min_version = 0; entry.max_version = 6;
        break;
    case 7:  // ControlledShutdown
        entry.name = "ControlledShutdown";
        entry.min_version = 0; entry.max_version = 3;
        break;
    case 8:  // OffsetCommit
        entry.name = "OffsetCommit";
        entry.min_version = 0; entry.max_version = 8;
        break;
    case 9:  // OffsetFetch
        entry.name = "OffsetFetch";
        entry.min_version = 0; entry.max_version = 7;
        break;
    case 10: // FindCoordinator
        entry.name = "FindCoordinator";
        entry.min_version = 0; entry.max_version = 4;
        break;
    case 11: // JoinGroup
        entry.name = "JoinGroup";
        entry.min_version = 0; entry.max_version = 7;
        break;
    case 12: // Heartbeat
        entry.name = "Heartbeat";
        entry.min_version = 0; entry.max_version = 4;
        break;
    case 13: // LeaveGroup
        entry.name = "LeaveGroup";
        entry.min_version = 0; entry.max_version = 4;
        break;
    case 14: // SyncGroup
        entry.name = "SyncGroup";
        entry.min_version = 0; entry.max_version = 5;
        break;
    case 15: // DescribeGroups
        entry.name = "DescribeGroups";
        entry.min_version = 0; entry.max_version = 5;
        break;
    case 16: // ListGroups
        entry.name = "ListGroups";
        entry.min_version = 0; entry.max_version = 4;
        break;
    case 17: // SaslHandshake
        entry.name = "SaslHandshake";
        entry.min_version = 0; entry.max_version = 1;
        break;
    case 18: // ApiVersions
        entry.name = "ApiVersions";
        entry.min_version = 0; entry.max_version = 3;
        break;
    case 19: // CreateTopics
        entry.name = "CreateTopics";
        entry.min_version = 0; entry.max_version = 6;
        break;
    case 20: // DeleteTopics
        entry.name = "DeleteTopics";
        entry.min_version = 0; entry.max_version = 5;
        break;
    case 21: // DeleteRecords
        entry.name = "DeleteRecords";
        entry.min_version = 0; entry.max_version = 1;
        break;
    case 22: // InitProducerId
        entry.name = "InitProducerId";
        entry.min_version = 0; entry.max_version = 3;
        break;
    case 23: // OffsetForLeaderEpoch
        entry.name = "OffsetForLeaderEpoch";
        entry.min_version = 0; entry.max_version = 3;
        break;
    case 24: // AddPartitionsToTxn
        entry.name = "AddPartitionsToTxn";
        entry.min_version = 0; entry.max_version = 2;
        break;
    case 25: // AddOffsetsToTxn
        entry.name = "AddOffsetsToTxn";
        entry.min_version = 0; entry.max_version = 2;
        break;
    case 26: // EndTxn
        entry.name = "EndTxn";
        entry.min_version = 0; entry.max_version = 2;
        break;
    case 27: // WriteTxnMarkers
        entry.name = "WriteTxnMarkers";
        entry.min_version = 0; entry.max_version = 0;
        break;
    case 28: // TxnOffsetCommit
        entry.name = "TxnOffsetCommit";
        entry.min_version = 0; entry.max_version = 2;
        break;
    case 29: // DescribeAcls
        entry.name = "DescribeAcls";
        entry.min_version = 0; entry.max_version = 2;
        break;
    case 30: // CreateAcls
        entry.name = "CreateAcls";
        entry.min_version = 0; entry.max_version = 2;
        break;
    case 31: // DeleteAcls
        entry.name = "DeleteAcls";
        entry.min_version = 0; entry.max_version = 2;
        break;
    case 32: // DescribeConfigs
        entry.name = "DescribeConfigs";
        entry.min_version = 0; entry.max_version = 3;
        break;
    case 33: // AlterConfigs
        entry.name = "AlterConfigs";
        entry.min_version = 0; entry.max_version = 1;
        break;
    case 34: // AlterReplicaLogDirs
        entry.name = "AlterReplicaLogDirs";
        entry.min_version = 0; entry.max_version = 1;
        break;
    case 35: // DescribeLogDirs
        entry.name = "DescribeLogDirs";
        entry.min_version = 0; entry.max_version = 1;
        break;
    case 36: // SaslAuthenticate
        entry.name = "SaslAuthenticate";
        entry.min_version = 0; entry.max_version = 2;
        break;
    case 37: // CreatePartitions
        entry.name = "CreatePartitions";
        entry.min_version = 0; entry.max_version = 2;
        break;
    case 38: // CreateDelegationToken
        entry.name = "CreateDelegationToken";
        entry.min_version = 0; entry.max_version = 2;
        break;
    case 39: // RenewDelegationToken
        entry.name = "RenewDelegationToken";
        entry.min_version = 0; entry.max_version = 1;
        break;
    case 40: // ExpireDelegationToken
        entry.name = "ExpireDelegationToken";
        entry.min_version = 0; entry.max_version = 1;
        break;
    case 41: // DescribeDelegationToken
        entry.name = "DescribeDelegationToken";
        entry.min_version = 0; entry.max_version = 1;
        break;
    case 42: // DeleteGroups
        entry.name = "DeleteGroups";
        entry.min_version = 0; entry.max_version = 1;
        break;
    case 43: // ElectLeaders
        entry.name = "ElectLeaders";
        entry.min_version = 0; entry.max_version = 1;
        break;
    case 44: // IncrementalAlterConfigs
        entry.name = "IncrementalAlterConfigs";
        entry.min_version = 0; entry.max_version = 0;
        break;
    case 45: // AlterPartitionReassignments
        entry.name = "AlterPartitionReassignments";
        entry.min_version = 0; entry.max_version = 0;
        break;
    case 46: // ListPartitionReassignments
        entry.name = "ListPartitionReassignments";
        entry.min_version = 0; entry.max_version = 0;
        break;
    case 47: // OffsetDelete
        entry.name = "OffsetDelete";
        entry.min_version = 0; entry.max_version = 0;
        break;
    case 48: // DescribeClientQuotas
        entry.name = "DescribeClientQuotas";
        entry.min_version = 0; entry.max_version = 0;
        break;
    case 49: // AlterClientQuotas
        entry.name = "AlterClientQuotas";
        entry.min_version = 0; entry.max_version = 0;
        break;
    default:
        entry.name = "Custom-" + std::to_string(api_key);
        entry.min_version = 0;
        entry.max_version = 0;
        break;
    }

    RD_LOG_INFO("Registered handler for API key {} ({}) versions {}-{}",
                api_key, entry.name, entry.min_version, entry.max_version);

    handlers[api_key] = std::move(entry);
}

// ============================================================================
// RequestDispatcher — dispatch() (synchronous stub)
// ============================================================================

shared_buffer RequestDispatcher::dispatch(int16_t api_key, buffer_view request) {
    if (!impl_->running.load(std::memory_order_acquire)) {
        RD_LOG_WARN("dispatch: dispatcher not running, rejecting request");
        return shared_buffer{};
    }

    // Enforce backpressure
    if (impl_->worker_pool->pending_count() > impl_->max_queue_depth) {
        RD_LOG_WARN("dispatch: queue depth {} exceeds limit {}, rejecting",
                    impl_->worker_pool->pending_count(), impl_->max_queue_depth);
        return shared_buffer{};
    }

    // Enforce global rate limit
    if (!impl_->global_bucket->consume()) {
        RD_LOG_WARN("dispatch: global rate limit exceeded, rejecting request");
        return shared_buffer{};
    }

    // Find the handler
    handler_fn handler;

    {
        std::shared_lock lock(impl_->handlers_mutex);
        auto it = impl_->handlers.find(api_key);
        if (it == impl_->handlers.end()) {
            RD_LOG_WARN("dispatch: no handler for API key {}", api_key);
            return shared_buffer{};
        }
        handler = it->second.handler;
        it->second.call_count.fetch_add(1, std::memory_order_relaxed);
    }

    // Record metrics
    {
        std::shared_lock lock(impl_->metrics_mutex);
        auto it = impl_->api_metrics.find(api_key);
        if (it != impl_->api_metrics.end()) {
            it->second->requests_total.fetch_add(1, std::memory_order_relaxed);
            it->second->bytes_received.fetch_add(
                static_cast<uint64_t>(request.size), std::memory_order_relaxed);
        }
        impl_->aggregate_metrics->requests_total.fetch_add(1, std::memory_order_relaxed);
    }

    // Execute handler
    auto start = std::chrono::steady_clock::now();
    shared_buffer response;

    try {
        response = handler(api_key, request);
    } catch (const std::exception& e) {
        RD_LOG_ERROR("dispatch: handler for API key {} threw: {}", api_key, e.what());
        // Record error
        std::shared_lock lock(impl_->metrics_mutex);
        auto it = impl_->api_metrics.find(api_key);
        if (it != impl_->api_metrics.end()) {
            it->second->error_count.fetch_add(1, std::memory_order_relaxed);
        }
        impl_->aggregate_metrics->error_count.fetch_add(1, std::memory_order_relaxed);
        return shared_buffer{};
    }

    // Record latency
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start);

    {
        std::shared_lock lock(impl_->metrics_mutex);
        auto it = impl_->api_metrics.find(api_key);
        if (it != impl_->api_metrics.end()) {
            it->second->latency.record(elapsed);
            it->second->bytes_sent.fetch_add(
                static_cast<uint64_t>(response.size()), std::memory_order_relaxed);
        }
        impl_->aggregate_metrics->latency.record(elapsed);
    }

    return response;
}

// ============================================================================
// RequestDispatcher — async_dispatch()
// ============================================================================

std::future<shared_buffer> RequestDispatcher::async_dispatch(
    int16_t api_key,
    int16_t api_version,
    buffer_view request,
    int32_t client_id_hash)
{
    if (!impl_->running.load(std::memory_order_acquire)) {
        RD_LOG_WARN("async_dispatch: dispatcher not running");
        std::promise<shared_buffer> p;
        p.set_value(shared_buffer{});
        return p.get_future();
    }

    // --- Phase 1: Throttling and backpressure ---

    size_t pending = impl_->worker_pool->pending_count();
    size_t in_flight_count = 0;
    {
        std::shared_lock lock(impl_->in_flight_mutex);
        in_flight_count = impl_->in_flight.size();
    }

    // Hard backpressure: reject if queue is full
    if (pending > impl_->max_queue_depth) {
        RD_LOG_WARN("async_dispatch: queue full ({} > {}), rejecting",
                    pending, impl_->max_queue_depth);
        std::promise<shared_buffer> p;
        p.set_value(shared_buffer{});
        return p.get_future();
    }

    // Soft backpressure: warn if approaching limit
    if (pending > impl_->max_queue_depth * kBackpressureThreshold) {
        RD_LOG_DEBUG("async_dispatch: queue near capacity ({}/{})",
                     pending, impl_->max_queue_depth);
    }

    // Reject if too many concurrent in-flight
    if (in_flight_count >= impl_->max_concurrent_requests) {
        RD_LOG_WARN("async_dispatch: too many concurrent requests ({})",
                    in_flight_count);
        std::promise<shared_buffer> p;
        p.set_value(shared_buffer{});
        return p.get_future();
    }

    // Global rate limit
    if (!impl_->global_bucket->consume()) {
        RD_LOG_WARN("async_dispatch: global rate limit exceeded");
        std::promise<shared_buffer> p;
        p.set_value(shared_buffer{});
        return p.get_future();
    }

    // Per-client rate limit
    if (impl_->per_client_rate_limit > 0.0 && client_id_hash != 0) {
        TokenBucket* client_bucket = nullptr;
        {
            std::shared_lock lock(impl_->client_buckets_mutex);
            auto it = impl_->client_buckets.find(client_id_hash);
            if (it != impl_->client_buckets.end()) {
                client_bucket = it->second.get();
            }
        }
        if (!client_bucket) {
            // Lazily create per-client bucket
            std::unique_lock lock(impl_->client_buckets_mutex);
            auto [it, inserted] = impl_->client_buckets.try_emplace(
                client_id_hash,
                std::make_unique<TokenBucket>(
                    impl_->per_client_rate_limit,
                    impl_->per_client_rate_limit * 4.0));
            client_bucket = it->second.get();
        }
        if (!client_bucket->consume()) {
            RD_LOG_DEBUG("async_dispatch: per-client rate limit exceeded for client {}",
                        client_id_hash);
            std::promise<shared_buffer> p;
            p.set_value(shared_buffer{});
            return p.get_future();
        }
    }

    // --- Phase 2: Create request context ---

    auto ctx = std::make_shared<RequestContext>();
    ctx->correlation_id = impl_->correlation_gen.next();
    ctx->api_key = api_key;
    ctx->api_version = api_version;
    ctx->client_id_hash = client_id_hash;
    ctx->request_body = request;
    ctx->enqueue_time = std::chrono::steady_clock::now();
    ctx->deadline = ctx->enqueue_time +
        std::chrono::milliseconds(impl_->request_timeout_ms);

    auto future = ctx->response_future;

    // Register in-flight
    {
        std::unique_lock lock(impl_->in_flight_mutex);
        impl_->in_flight[ctx->correlation_id] = ctx;
    }

    // --- Phase 3: Submit to thread pool ---

    auto ctx_weak = std::weak_ptr<RequestContext>(ctx);

    impl_->worker_pool->submit([this, ctx_weak]() {
        auto ctx = ctx_weak.lock();
        if (!ctx) return;

        ctx->dispatch_time = std::chrono::steady_clock::now();

        // Check if already timed out
        if (ctx->timed_out) {
            remove_in_flight(ctx->correlation_id);
            return;
        }

        // Find handler
        handler_fn handler;
        {
            std::shared_lock lock(impl_->handlers_mutex);
            auto it = impl_->handlers.find(ctx->api_key);
            if (it == impl_->handlers.end()) {
                RD_LOG_WARN("async_dispatch: no handler for API key {} (corr={})",
                           ctx->api_key, ctx->correlation_id);
                complete_with_error(ctx, shared_buffer{});
                return;
            }

            // Version negotiation
            auto negotiated = negotiate_version(
                ctx->api_key, ctx->api_version, impl_->handlers);
            if (!negotiated.has_value()) {
                RD_LOG_WARN("async_dispatch: version negotiation failed for API key {}",
                           ctx->api_key);
                complete_with_error(ctx, shared_buffer{});
                return;
            }

            handler = it->second.handler;
            it->second.call_count.fetch_add(1, std::memory_order_relaxed);
        }

        // Execute handler
        shared_buffer response;
        try {
            response = handler(ctx->api_key, ctx->request_body);
        } catch (const std::exception& e) {
            RD_LOG_ERROR("async_dispatch: handler for API key {} threw: {}",
                        ctx->api_key, e.what());
            complete_with_error(ctx, shared_buffer{});
            return;
        }

        // Complete the request
        complete_with_error(ctx, std::move(response));
    }, thread_pool::priority::normal);

    return future;
}

// ============================================================================
// RequestDispatcher — get_metrics()
// ============================================================================

RequestDispatcher::Metrics RequestDispatcher::get_metrics() const {
    Metrics m;

    {
        std::shared_lock lock(impl_->handlers_mutex);
        m.registered_handlers = impl_->handlers.size();
    }

    m.pending_requests = impl_->worker_pool
        ? impl_->worker_pool->pending_count() : 0;

    {
        std::shared_lock lock(impl_->in_flight_mutex);
        m.in_flight_requests = impl_->in_flight.size();
    }

    if (impl_->worker_pool) {
        m.idle_threads = impl_->worker_pool->idle_count();
        m.total_threads = impl_->worker_pool->thread_count();
        m.total_tasks_executed = impl_->worker_pool->total_tasks_executed();
    }

    m.global_tokens_available = impl_->global_bucket
        ? impl_->global_bucket->available() : 0.0;

    if (impl_->aggregate_metrics) {
        m.total_requests = impl_->aggregate_metrics->requests_total.load(
            std::memory_order_acquire);
        m.total_errors = impl_->aggregate_metrics->error_count.load(
            std::memory_order_acquire);
        auto snap = impl_->aggregate_metrics->latency.snapshot();
        if (snap.total_count > 0) {
            m.avg_latency_us = snap.total_sum_us /
                static_cast<int64_t>(snap.total_count);
        }
    }

    return m;
}

// ============================================================================
// RequestDispatcher — Private helpers
// ============================================================================

void RequestDispatcher::timeout_loop() {
    RD_LOG_DEBUG("RequestDispatcher timeout detection loop started");

    while (impl_->timeout_running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(500ms);

        auto now = std::chrono::steady_clock::now();
        std::vector<int32_t> expired_ids;

        {
            std::shared_lock lock(impl_->in_flight_mutex);
            for (const auto& [corr_id, ctx] : impl_->in_flight) {
                if (!ctx->completed && !ctx->timed_out &&
                    now >= ctx->deadline) {
                    expired_ids.push_back(corr_id);
                }
            }
        }

        for (auto corr_id : expired_ids) {
            std::shared_ptr<RequestContext> ctx;
            {
                std::unique_lock lock(impl_->in_flight_mutex);
                auto it = impl_->in_flight.find(corr_id);
                if (it != impl_->in_flight.end()) {
                    ctx = it->second;
                }
            }

            if (ctx && !ctx->completed && !ctx->timed_out) {
                ctx->timed_out = true;
                RD_LOG_WARN("Request timed out: corr={} api_key={}",
                           ctx->correlation_id, ctx->api_key);

                // Record timeout metric
                {
                    std::shared_lock lock(impl_->metrics_mutex);
                    auto it = impl_->api_metrics.find(ctx->api_key);
                    if (it != impl_->api_metrics.end()) {
                        it->second->requests_timed_out.fetch_add(
                            1, std::memory_order_relaxed);
                    }
                }

                try {
                    ctx->response_promise.set_value(shared_buffer{});
                } catch (...) {}

                remove_in_flight(corr_id);
            }
        }
    }

    RD_LOG_DEBUG("RequestDispatcher timeout detection loop stopped");
}

void RequestDispatcher::refill_loop() {
    RD_LOG_DEBUG("RequestDispatcher token bucket refill loop started");

    while (impl_->refill_running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(kTokenBucketRefillInterval);

        // Clean up stale client buckets (clients that haven't been used recently)
        {
            std::unique_lock lock(impl_->client_buckets_mutex);
            // Periodically prune empty buckets — in production this would be
            // LRU-based with a TTL, but for now we just keep them.
        }
    }

    RD_LOG_DEBUG("RequestDispatcher token bucket refill loop stopped");
}

void RequestDispatcher::remove_in_flight(int32_t correlation_id) {
    std::unique_lock lock(impl_->in_flight_mutex);
    impl_->in_flight.erase(correlation_id);
}

void RequestDispatcher::complete_with_error(
    std::shared_ptr<RequestContext> ctx,
    shared_buffer response)
{
    if (!ctx || ctx->completed) return;

    ctx->completed = true;

    try {
        ctx->response_promise.set_value(std::move(response));
    } catch (const std::future_error&) {
        // Promise already satisfied (e.g., timed out)
    }

    remove_in_flight(ctx->correlation_id);
}

} // namespace torrent::broker
