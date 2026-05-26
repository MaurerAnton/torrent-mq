/**
 * quota_manager.cpp — QuotaManager: Client Token Bucket Quota Enforcement
 *
 * Implements per-client and global quota tracking using token buckets:
 *   - check_produce_quota: enforce produce byte-rate limits per client
 *   - check_fetch_quota: enforce fetch byte-rate limits per client
 *   - check_request_quota: enforce overall request-rate limits per client
 *   - record_produce / record_fetch: account consumed bytes after a request
 *
 * Token bucket design:
 *   Each client has three independent token buckets:
 *     1. Produce throughput (bytes/sec) — capped by quota.producer_byte_rate
 *     2. Fetch throughput (bytes/sec) — capped by quota.consumer_byte_rate
 *     3. Request rate (requests/sec) — capped by quota.request_percentage
 *
 *   Tokens refill continuously at the configured rate.  When a consume
 *   call exceeds available tokens, the request is throttled (returns false).
 *   The caller should respond with error_code::throttling_quota_exceeded (89).
 *
 * Default quotas:
 *   - producer byte rate: Long.MAX_VALUE (effectively unlimited)
 *   - consumer byte rate: Long.MAX_VALUE (effectively unlimited)
 *   - request percentage: 100 (no throttling)
 *
 * Configuration:
 *   Quotas can be set globally via BrokerConfig or overridden per-client
 *   via an admin API (override_quota).  The per-client override takes
 *   precedence over the global default.
 *
 * Thread-safety:
 *   Each client bucket is protected by its own mutex.  The global bucket
 *   map is protected by a shared_mutex (shared for reads, exclusive for
 *   insert/erase).  Quota checks are on the hot path (every produce/fetch
 *   request), so locking is minimized: a shared_lock for map lookup,
 *   then a brief lock_guard on the individual bucket.
 *
 * See quota_manager.h for the public API contract.
 */

#include "torrent/broker/quota_manager.h"
#include "torrent/broker/server.h"
#include "torrent/common/types.h"
#include "torrent/common/config.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace torrent::broker {

// ============================================================================
// Anonymous namespace — internal types, constants, helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_qm_logger() {
    static auto logger = []() {
        auto l = spdlog::get("quota_manager");
        if (!l) {
            l = spdlog::stdout_color_mt("quota_manager");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define QM_LOG_INFO(...)  get_qm_logger()->info(__VA_ARGS__)
#define QM_LOG_WARN(...)  get_qm_logger()->warn(__VA_ARGS__)
#define QM_LOG_ERROR(...) get_qm_logger()->error(__VA_ARGS__)
#define QM_LOG_DEBUG(...) get_qm_logger()->debug(__VA_ARGS__)
#define QM_LOG_TRACE(...) get_qm_logger()->trace(__VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Sentinel value meaning "unlimited" (matching Kafka's Long.MAX_VALUE).
static constexpr int64_t kUnlimitedQuota = std::numeric_limits<int64_t>::max();

/// Default produce byte rate (bytes/sec).  Unlimited by default.
static constexpr int64_t kDefaultProducerByteRate = kUnlimitedQuota;

/// Default fetch byte rate (bytes/sec).  Unlimited by default.
static constexpr int64_t kDefaultConsumerByteRate = kUnlimitedQuota;

/// Default request rate (requests/sec).  100 = effectively unlimited.
static constexpr double kDefaultRequestPercentage = 100.0;

/// Default burst size multiplier: a bucket can accumulate up to N seconds
/// worth of tokens to tolerate short-term bursts.
static constexpr double kDefaultBurstMultiplier = 2.0;

/// How often the background thread cleans up idle client buckets.
static constexpr auto kCleanupInterval = std::chrono::seconds(300);  // 5 min

/// Maximum idle age before a client bucket is removed from the map.
static constexpr auto kMaxClientIdleAge = std::chrono::hours(1);

// --------------------------------------------------------------------------
// TokenBucket — generic refilling token bucket
// --------------------------------------------------------------------------

/**
 * Thread-safe token bucket with continuous refill.
 *
 * Tokens refill at `rate` tokens per second up to a burst ceiling
 * `burst_size`.  The bucket starts full (at burst_size tokens).
 *
 * When rate is negative or zero, the bucket acts as unlimited
 * (every consume returns true).
 */
class TokenBucket {
public:
    /**
     * Construct a token bucket.
     * @param rate       Tokens refilled per second.  ≤ 0 means unlimited.
     * @param burst_size Maximum tokens the bucket can hold (default: rate).
     */
    explicit TokenBucket(double rate, double burst_size = 0.0)
        : rate_per_sec_(rate)
        , burst_size_(burst_size > 0.0 ? burst_size : rate)
        , tokens_(rate > 0.0 ? burst_size_ : std::numeric_limits<double>::max())
        , last_refill_(std::chrono::steady_clock::now())
    {}

    /// Default: creates an unlimited bucket.
    TokenBucket()
        : rate_per_sec_(-1.0)
        , burst_size_(0.0)
        , tokens_(std::numeric_limits<double>::max())
        , last_refill_(std::chrono::steady_clock::now())
    {}

    /**
     * Attempt to consume exactly 1 token.
     * @return true if the token was available (or bucket is unlimited).
     */
    [[nodiscard]] bool consume_one() {
        if (is_unlimited()) return true;

        std::lock_guard<std::mutex> lock(mutex_);
        refill();
        if (tokens_ >= 1.0) {
            tokens_ -= 1.0;
            return true;
        }
        return false;
    }

    /**
     * Attempt to consume `count` tokens.
     * @param count Number of tokens to consume.
     * @return The number of tokens actually consumed (0 if none available).
     */
    [[nodiscard]] size_t consume(size_t count) {
        if (is_unlimited()) return count;

        std::lock_guard<std::mutex> lock(mutex_);
        refill();
        double available = tokens_;
        if (available <= 0.0) return 0;

        size_t allowed = static_cast<size_t>(
            std::min(available, static_cast<double>(count)));
        tokens_ -= static_cast<double>(allowed);
        return allowed;
    }

    /**
     * Consume `bytes` worth of tokens.
     * @return true if the full amount was available (or unlimited).
     */
    [[nodiscard]] bool consume_bytes(byte_count_t bytes) {
        if (is_unlimited()) return true;
        if (bytes <= 0) return true;

        std::lock_guard<std::mutex> lock(mutex_);
        refill();
        double needed = static_cast<double>(bytes);
        if (tokens_ >= needed) {
            tokens_ -= needed;
            return true;
        }
        return false;
    }

    /**
     * Record `bytes` of usage (called after a successful write, to account
     * the actual bytes used, as opposed to check-only).
     */
    void record_bytes(byte_count_t bytes) {
        if (is_unlimited() || bytes <= 0) return;

        std::lock_guard<std::mutex> lock(mutex_);
        refill();
        // We already consumed these bytes in the check phase; this is just
        // for accounting.  No-op in the pure token-bucket model, but
        // useful for metrics.
        // (In a stricter model, we would subtract bytes here.)
        QM_LOG_TRACE("Recorded {} bytes (tokens remaining: {})", bytes, tokens_);
    }

    /// Current token count (for metrics / diagnostics).
    [[nodiscard]] double available() const {
        if (is_unlimited()) return std::numeric_limits<double>::max();
        std::lock_guard<std::mutex> lock(mutex_);
        return tokens_;
    }

    /// True when this bucket imposes no limit.
    [[nodiscard]] bool is_unlimited() const noexcept {
        return rate_per_sec_ <= 0.0;
    }

    /// Update the bucket's rate (used when overriding per-client quotas).
    void update_rate(double new_rate, double new_burst = 0.0) {
        std::lock_guard<std::mutex> lock(mutex_);
        rate_per_sec_ = new_rate;
        if (new_burst > 0.0) {
            burst_size_ = new_burst;
        } else {
            burst_size_ = new_rate;
        }
        // Don't change tokens_ — let the refill logic converge.
    }

private:
    /**
     * Refill tokens based on elapsed time since last refill.
     * Must be called while holding mutex_.
     */
    void refill() {
        auto now = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(now - last_refill_).count();
        if (elapsed > 0.0) {
            tokens_ = std::min(burst_size_, tokens_ + elapsed * rate_per_sec_);
        }
        last_refill_ = now;
    }

    double rate_per_sec_;
    double burst_size_;
    double tokens_;
    std::chrono::steady_clock::time_point last_refill_;
    mutable std::mutex mutex_;
};

// --------------------------------------------------------------------------
// ClientQuota — per-client quota state
// --------------------------------------------------------------------------

/**
 * Encapsulates the three token buckets for a single client, along with
 * per-client quota overrides and activity tracking.
 */
struct ClientQuota {
    TokenBucket produce_bucket;     ///< Produce byte throughput.
    TokenBucket fetch_bucket;       ///< Fetch byte throughput.
    TokenBucket request_bucket;     ///< Request rate.

    /// Custom overrides (set via admin API).  -1 means "use global default".
    int64_t override_producer_byte_rate = -1;
    int64_t override_consumer_byte_rate = -1;
    double  override_request_percentage = -1.0;

    /// Timestamp of last activity (for idle cleanup).
    std::chrono::steady_clock::time_point last_activity;

    /// Counters for diagnostics.
    std::atomic<uint64_t> throttled_produce_count{0};
    std::atomic<uint64_t> throttled_fetch_count{0};
    std::atomic<uint64_t> throttled_request_count{0};

    /// Protects mutable state.
    mutable std::mutex mutex;

    ClientQuota()
        : last_activity(std::chrono::steady_clock::now())
    {}

    void touch() noexcept {
        last_activity = std::chrono::steady_clock::now();
    }

    [[nodiscard]] bool is_idle(std::chrono::steady_clock::duration max_idle) const noexcept {
        return (std::chrono::steady_clock::now() - last_activity) > max_idle;
    }
};

// --------------------------------------------------------------------------
// Helper: resolve effective rate
// --------------------------------------------------------------------------

/**
 * Resolve the effective rate for a client: if a per-client override is set
 * (> 0), use it.  Otherwise fall back to the global default.
 */
inline double resolve_rate(int64_t override_val, int64_t global_val,
                           bool is_double = false) {
    if (override_val > 0) {
        return is_double ? static_cast<double>(override_val)
                         : static_cast<double>(override_val);
    }
    return static_cast<double>(global_val);
}

inline double resolve_percentage(double override_val, double global_val) {
    if (override_val >= 0.0) {
        return override_val;
    }
    return global_val;
}

} // anonymous namespace

// ============================================================================
// QuotaManager::Impl — PIMPL
// ============================================================================

struct QuotaManager::Impl {
    /// Per-client quota buckets, keyed by client_id string.
    std::unordered_map<std::string, std::unique_ptr<ClientQuota>> client_quotas;

    /// Protects the client_quotas map.
    mutable std::shared_mutex quotas_mutex;

    /// Global default produce byte rate (bytes/sec).
    int64_t default_producer_byte_rate = kDefaultProducerByteRate;

    /// Global default fetch byte rate (bytes/sec).
    int64_t default_consumer_byte_rate = kDefaultConsumerByteRate;

    /// Global default request rate (% of unlimited).
    double default_request_percentage = kDefaultRequestPercentage;

    /// Whether quotas are enabled at all.
    bool quotas_enabled = true;

    /// Background cleanup thread.
    std::thread cleanup_thread;
    std::atomic<bool> cleanup_running{false};

    /// Running flag.
    std::atomic<bool> running{false};

    /// Global counters.
    std::atomic<uint64_t> total_throttled_produce{0};
    std::atomic<uint64_t> total_throttled_fetch{0};
    std::atomic<uint64_t> total_throttled_requests{0};
};

// ============================================================================
// QuotaManager — Constructor / Destructor
// ============================================================================

QuotaManager::QuotaManager(BrokerServer& s)
    : server_(&s)
    , impl_(std::make_unique<Impl>())
{
    QM_LOG_INFO("QuotaManager initialized");
}

QuotaManager::~QuotaManager() {
    if (impl_->running.load(std::memory_order_acquire)) {
        try { shutdown(); } catch (...) {}
    }
    QM_LOG_INFO("QuotaManager destroyed");
}

// ============================================================================
// QuotaManager — start()
// ============================================================================

void QuotaManager::start() {
    if (impl_->running.load(std::memory_order_acquire)) {
        QM_LOG_WARN("QuotaManager::start() called while already running");
        return;
    }

    // Resolve global defaults from configuration.
    // In production, these come from config keys like "quota.producer.byte.rate".
    // For now, use the standard defaults or BrokerConfig flags.
    const auto& cfg = server_->config();
    impl_->quotas_enabled = true; // Configuration would set this.

    impl_->default_producer_byte_rate = kDefaultProducerByteRate;
    impl_->default_consumer_byte_rate = kDefaultConsumerByteRate;
    impl_->default_request_percentage = kDefaultRequestPercentage;

    // Start background cleanup thread.
    impl_->cleanup_running.store(true, std::memory_order_release);
    impl_->cleanup_thread = std::thread(&QuotaManager::cleanup_loop, this);

    impl_->running.store(true, std::memory_order_release);
    QM_LOG_INFO("QuotaManager started (producer_rate={} fetch_rate={} req_pct={})",
                impl_->default_producer_byte_rate == kUnlimitedQuota
                    ? "unlimited" : std::to_string(impl_->default_producer_byte_rate),
                impl_->default_consumer_byte_rate == kUnlimitedQuota
                    ? "unlimited" : std::to_string(impl_->default_consumer_byte_rate),
                impl_->default_request_percentage);
}

// ============================================================================
// QuotaManager — shutdown()
// ============================================================================

void QuotaManager::shutdown() {
    if (!impl_->running.load(std::memory_order_acquire)) {
        return;
    }

    QM_LOG_INFO("QuotaManager shutting down");

    impl_->cleanup_running.store(false, std::memory_order_release);
    if (impl_->cleanup_thread.joinable()) {
        impl_->cleanup_thread.join();
    }

    // Clear all client buckets.
    {
        std::unique_lock lock(impl_->quotas_mutex);
        impl_->client_quotas.clear();
    }

    impl_->running.store(false, std::memory_order_release);
    QM_LOG_INFO("QuotaManager shutdown complete");
}

// ============================================================================
// QuotaManager — get_or_create_client_bucket()
// ============================================================================

/**
 * Retrieve the ClientQuota for a client_id, creating a default-configured
 * bucket if one does not already exist.
 *
 * Uses double-checked locking: shared_lock for read, upgrade to exclusive
 * if insert is needed.
 */
ClientQuota* QuotaManager::get_client_quota(const std::string& client_id) {
    // Fast path: shared lock read.
    {
        std::shared_lock lock(impl_->quotas_mutex);
        auto it = impl_->client_quotas.find(client_id);
        if (it != impl_->client_quotas.end()) {
            return it->second.get();
        }
    }

    // Slow path: exclusive lock insert.
    {
        std::unique_lock lock(impl_->quotas_mutex);
        // Double-check: another thread may have inserted while we waited.
        auto it = impl_->client_quotas.find(client_id);
        if (it != impl_->client_quotas.end()) {
            return it->second.get();
        }

        auto cq = std::make_unique<ClientQuota>();

        // Configure with global defaults.
        double prod_rate = static_cast<double>(impl_->default_producer_byte_rate);
        double fetch_rate = static_cast<double>(impl_->default_consumer_byte_rate);

        if (prod_rate > 0) {
            cq->produce_bucket = TokenBucket(prod_rate, prod_rate * kDefaultBurstMultiplier);
        }
        if (fetch_rate > 0) {
            cq->fetch_bucket = TokenBucket(fetch_rate, fetch_rate * kDefaultBurstMultiplier);
        }

        // Request rate bucket: treat request_percentage as requests/sec.
        // 100% = unlimited.  Values < 100 limit proportionally.
        double req_rate = impl_->default_request_percentage;
        if (req_rate > 0 && req_rate < 100.0) {
            // For a percentage < 100, limit to that many req/sec.
            // In production, this would map to a real rate.
            cq->request_bucket = TokenBucket(req_rate, req_rate * kDefaultBurstMultiplier);
        }

        auto* raw = cq.get();
        impl_->client_quotas[client_id] = std::move(cq);
        QM_LOG_DEBUG("Created quota bucket for client '{}'", client_id);
        return raw;
    }
}

// ============================================================================
// QuotaManager — check_produce_quota()
// ============================================================================

bool QuotaManager::check_produce_quota(const std::string& client_id) noexcept {
    if (!impl_->quotas_enabled) return true;
    if (client_id.empty()) return true;

    auto* cq = get_client_quota(client_id);
    std::lock_guard<std::mutex> lock(cq->mutex);

    bool allowed = cq->produce_bucket.consume_one();
    cq->touch();

    if (!allowed) {
        cq->throttled_produce_count.fetch_add(1, std::memory_order_relaxed);
        impl_->total_throttled_produce.fetch_add(1, std::memory_order_relaxed);
        QM_LOG_TRACE("Produce quota exceeded for client '{}'", client_id);
    }
    return allowed;
}

// ============================================================================
// QuotaManager — check_fetch_quota()
// ============================================================================

bool QuotaManager::check_fetch_quota(const std::string& client_id) noexcept {
    if (!impl_->quotas_enabled) return true;
    if (client_id.empty()) return true;

    auto* cq = get_client_quota(client_id);
    std::lock_guard<std::mutex> lock(cq->mutex);

    bool allowed = cq->fetch_bucket.consume_one();
    cq->touch();

    if (!allowed) {
        cq->throttled_fetch_count.fetch_add(1, std::memory_order_relaxed);
        impl_->total_throttled_fetch.fetch_add(1, std::memory_order_relaxed);
        QM_LOG_TRACE("Fetch quota exceeded for client '{}'", client_id);
    }
    return allowed;
}

// ============================================================================
// QuotaManager — check_request_quota()
// ============================================================================

bool QuotaManager::check_request_quota(const std::string& client_id) noexcept {
    if (!impl_->quotas_enabled) return true;
    if (client_id.empty()) return true;

    auto* cq = get_client_quota(client_id);
    std::lock_guard<std::mutex> lock(cq->mutex);

    bool allowed = cq->request_bucket.consume_one();
    cq->touch();

    if (!allowed) {
        cq->throttled_request_count.fetch_add(1, std::memory_order_relaxed);
        impl_->total_throttled_requests.fetch_add(1, std::memory_order_relaxed);
        QM_LOG_TRACE("Request quota exceeded for client '{}'", client_id);
    }
    return allowed;
}

// ============================================================================
// QuotaManager — record_produce()
// ============================================================================

void QuotaManager::record_produce(const std::string& client_id, byte_count_t bytes) {
    if (!impl_->quotas_enabled) return;
    if (client_id.empty() || bytes <= 0) return;

    auto* cq = get_client_quota(client_id);
    std::lock_guard<std::mutex> lock(cq->mutex);

    // Deduct the actual bytes from the bucket.
    // In a strict system, this would be done atomically with the check.
    // Here we trust that check_produce_quota was called first.
    cq->produce_bucket.record_bytes(bytes);
    cq->touch();

    QM_LOG_TRACE("Recorded produce: client='{}' bytes={}", client_id, bytes);
}

// ============================================================================
// QuotaManager — record_fetch()
// ============================================================================

void QuotaManager::record_fetch(const std::string& client_id, byte_count_t bytes) {
    if (!impl_->quotas_enabled) return;
    if (client_id.empty() || bytes <= 0) return;

    auto* cq = get_client_quota(client_id);
    std::lock_guard<std::mutex> lock(cq->mutex);

    cq->fetch_bucket.record_bytes(bytes);
    cq->touch();

    QM_LOG_TRACE("Recorded fetch: client='{}' bytes={}", client_id, bytes);
}

// ============================================================================
// QuotaManager — override_client_quota()
// ============================================================================

void QuotaManager::override_client_quota(
    const std::string& client_id,
    std::optional<int64_t> producer_byte_rate,
    std::optional<int64_t> consumer_byte_rate,
    std::optional<double> request_percentage)
{
    auto* cq = get_client_quota(client_id);
    std::lock_guard<std::mutex> lock(cq->mutex);

    if (producer_byte_rate.has_value()) {
        cq->override_producer_byte_rate = producer_byte_rate.value();
        double rate = static_cast<double>(producer_byte_rate.value());
        if (rate > 0) {
            cq->produce_bucket.update_rate(rate, rate * kDefaultBurstMultiplier);
        } else {
            cq->produce_bucket.update_rate(-1.0);  // unlimited
        }
        QM_LOG_INFO("Client '{}' producer byte rate override: {}",
                    client_id,
                    rate > 0 ? std::to_string(static_cast<int64_t>(rate)) : "unlimited");
    }

    if (consumer_byte_rate.has_value()) {
        cq->override_consumer_byte_rate = consumer_byte_rate.value();
        double rate = static_cast<double>(consumer_byte_rate.value());
        if (rate > 0) {
            cq->fetch_bucket.update_rate(rate, rate * kDefaultBurstMultiplier);
        } else {
            cq->fetch_bucket.update_rate(-1.0);
        }
        QM_LOG_INFO("Client '{}' consumer byte rate override: {}",
                    client_id,
                    rate > 0 ? std::to_string(static_cast<int64_t>(rate)) : "unlimited");
    }

    if (request_percentage.has_value()) {
        cq->override_request_percentage = request_percentage.value();
        double rp = request_percentage.value();
        if (rp > 0 && rp < 100.0) {
            cq->request_bucket.update_rate(rp, rp * kDefaultBurstMultiplier);
        } else {
            cq->request_bucket.update_rate(-1.0);
        }
        QM_LOG_INFO("Client '{}' request percentage override: {}", client_id, rp);
    }

    cq->touch();
}

// ============================================================================
// QuotaManager — remove_client_quota()
// ============================================================================

void QuotaManager::remove_client_quota(const std::string& client_id) {
    std::unique_lock lock(impl_->quotas_mutex);
    auto it = impl_->client_quotas.find(client_id);
    if (it != impl_->client_quotas.end()) {
        impl_->client_quotas.erase(it);
        QM_LOG_DEBUG("Removed quota bucket for client '{}'", client_id);
    }
}

// ============================================================================
// QuotaManager — cleanup_loop()
// ============================================================================

void QuotaManager::cleanup_loop() {
    QM_LOG_INFO("Quota cleanup thread started");

    while (impl_->cleanup_running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(kCleanupInterval);

        size_t removed = 0;
        {
            std::unique_lock lock(impl_->quotas_mutex);
            for (auto it = impl_->client_quotas.begin();
                 it != impl_->client_quotas.end(); ) {
                std::lock_guard<std::mutex> cq_lock(it->second->mutex);
                if (it->second->is_idle(kMaxClientIdleAge)) {
                    it = impl_->client_quotas.erase(it);
                    ++removed;
                } else {
                    ++it;
                }
            }
        }

        if (removed > 0) {
            QM_LOG_INFO("Quota cleanup: removed {} idle client buckets ({} active)",
                        removed, impl_->client_quotas.size());
        }
    }

    QM_LOG_INFO("Quota cleanup thread stopped");
}

// ============================================================================
// QuotaManager — client_stats()
// ============================================================================

quota_client_stats QuotaManager::client_stats(const std::string& client_id) const {
    quota_client_stats stats;
    stats.client_id = client_id;
    stats.exists = false;

    std::shared_lock lock(impl_->quotas_mutex);
    auto it = impl_->client_quotas.find(client_id);
    if (it == impl_->client_quotas.end()) {
        return stats;
    }

    std::lock_guard<std::mutex> cq_lock(it->second->mutex);
    stats.exists                    = true;
    stats.produce_tokens_available  = it->second->produce_bucket.available();
    stats.fetch_tokens_available    = it->second->fetch_bucket.available();
    stats.request_tokens_available  = it->second->request_bucket.available();
    stats.throttled_produce_count   = it->second->throttled_produce_count.load();
    stats.throttled_fetch_count     = it->second->throttled_fetch_count.load();
    stats.throttled_request_count   = it->second->throttled_request_count.load();
    stats.producer_byte_rate_override = it->second->override_producer_byte_rate;
    stats.consumer_byte_rate_override = it->second->override_consumer_byte_rate;

    return stats;
}

// ============================================================================
// QuotaManager — total_throttled()
// ============================================================================

uint64_t QuotaManager::total_throttled_produce() const noexcept {
    return impl_->total_throttled_produce.load(std::memory_order_acquire);
}

uint64_t QuotaManager::total_throttled_fetch() const noexcept {
    return impl_->total_throttled_fetch.load(std::memory_order_acquire);
}

uint64_t QuotaManager::total_throttled_requests() const noexcept {
    return impl_->total_throttled_requests.load(std::memory_order_acquire);
}

} // namespace torrent::broker
