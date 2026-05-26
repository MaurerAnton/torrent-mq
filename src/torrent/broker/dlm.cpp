/**
 * dlm.cpp — DistributedLockManager: Cluster-Wide Locking via Raft
 *
 * Implements distributed locking across the cluster using Raft consensus
 * for lock acquire/release proposals.  Supports:
 *   - try_lock: propose lock acquisition with configurable timeout
 *   - unlock: propose lock release by owner
 *   - lock_owner: query current lock holder (local state)
 *   - Fencing tokens: monotonically increasing per-lock token
 *   - Lock expiry: background thread cleans up stale/expired locks
 *
 * Architecture:
 *   Lock acquire/release operations are proposed as UserCommands through
 *   the Raft log.  Once committed, the state machine applies the command.
 *   Every successful lock grant assigns a fencing token — an increment-only
 *   counter — that the lock holder includes in subsequent operations.
 *
 * Lock lifecycle:
 *   try_lock(name, owner, timeout)
 *     → Propose LockAcquire command via Raft
 *     → Wait for commit (up to timeout_ms)
 *     → If committed, lock is held; return true
 *     → If timeout or contested, return false
 *
 *   unlock(name, owner)
 *     → Propose LockRelease command via Raft
 *     → Return true on success
 *
 *   lock_owner(name)
 *     → Query in-memory lock table (no Raft round-trip)
 *
 * Thread-safety:
 *   The in-memory lock table is protected by a shared_mutex.  Lock
 *   proposals are serialised by the Raft consensus layer.
 *
 * Configuration (via BrokerConfig keys):
 *   - dlm.default_timeout_ms: default proposal timeout (60000)
 *   - dlm.lock_ttl_ms: idle lock expiration (300000 = 5 min)
 *   - dlm.cleanup_interval_ms: background expiry scan interval (60000)
 *   - dlm.max_locks: max concurrently held locks (10000)
 *
 * See dlm.h for the public API contract.
 */

#include "torrent/broker/dlm.h"
#include "torrent/broker/server.h"
#include "torrent/common/types.h"
#include "torrent/common/config.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
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

[[nodiscard]] std::shared_ptr<spdlog::logger> get_dlm_logger() {
    static auto logger = []() {
        auto l = spdlog::get("dlm");
        if (!l) {
            l = spdlog::stdout_color_mt("dlm");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define DLM_LOG_INFO(...)  get_dlm_logger()->info(__VA_ARGS__)
#define DLM_LOG_WARN(...)  get_dlm_logger()->warn(__VA_ARGS__)
#define DLM_LOG_ERROR(...) get_dlm_logger()->error(__VA_ARGS__)
#define DLM_LOG_DEBUG(...) get_dlm_logger()->debug(__VA_ARGS__)
#define DLM_LOG_TRACE(...) get_dlm_logger()->trace(__VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

static constexpr int64_t kDefaultTimeoutMs = 60000;           // 60 seconds
static constexpr auto kDefaultLockTTL = std::chrono::minutes(5);
static constexpr auto kCleanupInterval = std::chrono::seconds(60);
static constexpr size_t kMaxLocks = 10000;
static constexpr double kDefaultBurstMultiplier = 2.0;

// --------------------------------------------------------------------------
// LockState — in-memory state for a single distributed lock
// --------------------------------------------------------------------------

struct LockState {
    std::string owner;              ///< Current owner.  Empty if unlocked.
    epoch_t fencing_token = 0;      ///< Current fencing token (increments on grant).
    timestamp_ms_t acquired_at = 0; ///< Wall-clock time of grant.
    timestamp_ms_t last_activity = 0; ///< Last activity timestamp.

    std::atomic<uint64_t> grant_count{0};
    std::atomic<uint64_t> conflict_count{0};

    [[nodiscard]] bool is_held() const noexcept { return !owner.empty(); }

    [[nodiscard]] epoch_t next_fencing_token() noexcept { return ++fencing_token; }

    [[nodiscard]] bool is_expired(std::chrono::milliseconds ttl) const noexcept {
        if (!is_held()) return false;
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        return (now - last_activity) > ttl.count();
    }

    void touch() noexcept {
        last_activity = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }
};

// --------------------------------------------------------------------------
// DLMConfig
// --------------------------------------------------------------------------

struct DLMConfig {
    int64_t default_timeout_ms = kDefaultTimeoutMs;
    std::chrono::milliseconds lock_ttl{kDefaultLockTTL};
    std::chrono::milliseconds cleanup_interval{kCleanupInterval};
    size_t max_locks = kMaxLocks;
};

[[nodiscard]] DLMConfig resolve_dlm_config(const BrokerConfig& cfg) noexcept {
    DLMConfig c;
    (void)cfg;  // Production: read from config keys.
    return c;
}

// --------------------------------------------------------------------------
// Lock-op enum + binary command encoding for Raft UserCommand payload
// --------------------------------------------------------------------------

enum class LockOp : uint8_t { acquire = 0, release = 1 };

/**
 * Encode a lock command into a binary buffer for the Raft log.
 * Layout: op(1) | name_len(2) | name(...) | owner_len(2) | owner(...) |
 *         timeout_ms(8)
 */
[[nodiscard]] std::vector<uint8_t> encode_cmd(
    LockOp op, const std::string& name, const std::string& owner, int64_t timeout)
{
    uint16_t nl = static_cast<uint16_t>(name.size());
    uint16_t ol = static_cast<uint16_t>(owner.size());
    size_t total = 1 + 2 + nl + 2 + ol + 8;
    std::vector<uint8_t> buf(total);
    size_t pos = 0;
    buf[pos++] = static_cast<uint8_t>(op);
    std::memcpy(&buf[pos], &nl, 2);  pos += 2;
    std::memcpy(&buf[pos], name.data(), nl);  pos += nl;
    std::memcpy(&buf[pos], &ol, 2);  pos += 2;
    std::memcpy(&buf[pos], owner.data(), ol);  pos += ol;
    std::memcpy(&buf[pos], &timeout, 8);  pos += 8;
    return buf;
}

/**
 * Decode a lock command from a Raft log payload.
 * Returns std::nullopt if the buffer is malformed.
 */
struct DecodedCmd { LockOp op; std::string name; std::string owner; int64_t timeout = 0; };

[[nodiscard]] std::optional<DecodedCmd> decode_cmd(const std::vector<uint8_t>& p) {
    if (p.size() < 1 + 2) return std::nullopt;
    DecodedCmd c;
    size_t pos = 0;
    c.op = static_cast<LockOp>(p[pos++]);
    uint16_t nl; std::memcpy(&nl, &p[pos], 2); pos += 2;
    if (pos + nl > p.size()) return std::nullopt;
    c.name.assign(reinterpret_cast<const char*>(&p[pos]), nl); pos += nl;
    if (pos + 2 > p.size()) return std::nullopt;
    uint16_t ol; std::memcpy(&ol, &p[pos], 2); pos += 2;
    if (pos + ol > p.size()) return std::nullopt;
    c.owner.assign(reinterpret_cast<const char*>(&p[pos]), ol); pos += ol;
    if (pos + 8 > p.size()) return std::nullopt;
    std::memcpy(&c.timeout, &p[pos], 8);
    return c;
}

[[nodiscard]] timestamp_ms_t now_ms() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

} // anonymous namespace

// ============================================================================
// DistributedLockManager::Impl — PIMPL
// ============================================================================

struct DistributedLockManager::Impl {
    std::unordered_map<std::string, LockState> lock_table;
    mutable std::shared_mutex table_mutex;

    DLMConfig config;

    std::thread cleanup_thread;
    std::atomic<bool> cleanup_running{false};
    std::atomic<bool> running{false};

    std::atomic<uint64_t> total_acquires{0};
    std::atomic<uint64_t> total_releases{0};
    std::atomic<uint64_t> total_conflicts{0};
    std::atomic<uint64_t> total_expirations{0};
    std::atomic<uint64_t> total_timeouts{0};
};

// ============================================================================
// DistributedLockManager — Constructor / Destructor
// ============================================================================

DistributedLockManager::DistributedLockManager(BrokerServer& s)
    : server_(&s)
    , impl_(std::make_unique<Impl>())
{
    impl_->config = resolve_dlm_config(s.config());
    DLM_LOG_INFO("DistributedLockManager initialized");
}

DistributedLockManager::~DistributedLockManager() {
    if (impl_->running.load(std::memory_order_acquire)) {
        try { shutdown(); } catch (...) {}
    }
    DLM_LOG_INFO("DistributedLockManager destroyed");
}

// ============================================================================
// DistributedLockManager — start() / shutdown()
// ============================================================================

void DistributedLockManager::start() {
    if (impl_->running.load(std::memory_order_acquire)) {
        DLM_LOG_WARN("start() called while already running");
        return;
    }

    impl_->cleanup_running.store(true, std::memory_order_release);
    impl_->cleanup_thread = std::thread(&DistributedLockManager::cleanup_loop, this);

    impl_->running.store(true, std::memory_order_release);
    DLM_LOG_INFO("DLM started (timeout={}ms, ttl={}ms, max_locks={})",
                 impl_->config.default_timeout_ms,
                 impl_->config.lock_ttl.count(), impl_->config.max_locks);
}

void DistributedLockManager::shutdown() {
    if (!impl_->running.load(std::memory_order_acquire)) return;

    DLM_LOG_INFO("DLM shutting down");

    impl_->cleanup_running.store(false, std::memory_order_release);
    if (impl_->cleanup_thread.joinable()) {
        impl_->cleanup_thread.join();
    }

    {
        std::unique_lock lock(impl_->table_mutex);
        impl_->lock_table.clear();
    }

    impl_->running.store(false, std::memory_order_release);
    DLM_LOG_INFO("DLM shutdown complete");
}

// ============================================================================
// DistributedLockManager — try_lock()
// ============================================================================

bool DistributedLockManager::try_lock(
    const std::string& lock_name,
    const std::string& owner,
    duration_ms_t timeout_ms)
{
    if (lock_name.empty() || owner.empty()) {
        DLM_LOG_WARN("try_lock: empty lock_name or owner");
        return false;
    }

    int64_t effective_timeout = (timeout_ms > 0)
        ? timeout_ms : impl_->config.default_timeout_ms;

    DLM_LOG_DEBUG("try_lock: name='{}' owner='{}' timeout={}ms",
                  lock_name, owner, effective_timeout);

    // --- Phase 1: Fast-path local check ---
    {
        std::shared_lock lock(impl_->table_mutex);
        auto it = impl_->lock_table.find(lock_name);
        if (it != impl_->lock_table.end() && it->second.is_held()) {
            if (it->second.owner == owner) {
                it->second.touch();
                DLM_LOG_TRACE("try_lock: lock '{}' re-entrant for '{}'", lock_name, owner);
                return true;
            }
            it->second.conflict_count.fetch_add(1, std::memory_order_relaxed);
            impl_->total_conflicts.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
    }

    // --- Phase 2: Capacity check ---
    {
        std::shared_lock lock(impl_->table_mutex);
        if (impl_->lock_table.size() >= impl_->config.max_locks) {
            DLM_LOG_WARN("try_lock: max_locks limit ({})", impl_->config.max_locks);
            return false;
        }
    }

    // --- Phase 3: Propose lock acquisition via Raft ---
    auto payload = encode_cmd(LockOp::acquire, lock_name, owner, effective_timeout);

    // In production: server_->raft_node()->propose(payload) → future.
    // Stub: simulate synchronous proposal.
    bool raft_ok = simulate_raft_propose(std::move(payload), effective_timeout);
    if (!raft_ok) {
        impl_->total_timeouts.fetch_add(1, std::memory_order_relaxed);
        DLM_LOG_WARN("try_lock: Raft proposal timeout for '{}'", lock_name);
        return false;
    }

    // --- Phase 4: Apply the lock grant ---
    {
        std::unique_lock lock(impl_->table_mutex);
        auto it = impl_->lock_table.find(lock_name);
        if (it == impl_->lock_table.end()) {
            LockState state;
            state.owner = owner;
            state.acquired_at = now_ms();
            state.touch();
            state.next_fencing_token();
            state.grant_count.store(1, std::memory_order_relaxed);
            impl_->lock_table[lock_name] = std::move(state);
        } else if (!it->second.is_held() || it->second.owner == owner) {
            it->second.owner = owner;
            it->second.acquired_at = now_ms();
            it->second.touch();
            it->second.next_fencing_token();
            it->second.grant_count.fetch_add(1, std::memory_order_relaxed);
        } else {
            // Contested: another owner won the race via consensus.
            it->second.conflict_count.fetch_add(1, std::memory_order_relaxed);
            impl_->total_conflicts.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
    }

    impl_->total_acquires.fetch_add(1, std::memory_order_relaxed);
    DLM_LOG_INFO("try_lock: lock '{}' granted to '{}'", lock_name, owner);
    return true;
}

// ============================================================================
// DistributedLockManager — unlock()
// ============================================================================

bool DistributedLockManager::unlock(
    const std::string& lock_name,
    const std::string& owner)
{
    if (lock_name.empty() || owner.empty()) return false;

    DLM_LOG_DEBUG("unlock: name='{}' owner='{}'", lock_name, owner);

    // --- Phase 1: Local ownership check ---
    {
        std::shared_lock lock(impl_->table_mutex);
        auto it = impl_->lock_table.find(lock_name);
        if (it == impl_->lock_table.end() || !it->second.is_held()) {
            return false;  // Already unlocked.
        }
        if (it->second.owner != owner) {
            DLM_LOG_WARN("unlock: lock '{}' held by '{}', not '{}'",
                         lock_name, it->second.owner, owner);
            return false;
        }
    }

    // --- Phase 2: Propose lock release via Raft ---
    auto payload = encode_cmd(LockOp::release, lock_name, owner,
                              impl_->config.default_timeout_ms);

    bool raft_ok = simulate_raft_propose(std::move(payload),
                                         impl_->config.default_timeout_ms);
    if (!raft_ok) {
        DLM_LOG_WARN("unlock: Raft proposal timeout for '{}'", lock_name);
        return false;
    }

    // --- Phase 3: Clear the lock ---
    {
        std::unique_lock lock(impl_->table_mutex);
        auto it = impl_->lock_table.find(lock_name);
        if (it != impl_->lock_table.end()) {
            it->second.owner.clear();
            it->second.acquired_at = 0;
        }
    }

    impl_->total_releases.fetch_add(1, std::memory_order_relaxed);
    DLM_LOG_INFO("unlock: lock '{}' released by '{}'", lock_name, owner);
    return true;
}

// ============================================================================
// DistributedLockManager — lock_owner()
// ============================================================================

std::string DistributedLockManager::lock_owner(const std::string& lock_name) const {
    if (lock_name.empty()) return {};

    std::shared_lock lock(impl_->table_mutex);
    auto it = impl_->lock_table.find(lock_name);
    if (it == impl_->lock_table.end() || !it->second.is_held()) {
        return {};
    }
    return it->second.owner;
}

// ============================================================================
// DistributedLockManager — fencing_token()
// ============================================================================

epoch_t DistributedLockManager::fencing_token(const std::string& lock_name) const {
    if (lock_name.empty()) return 0;

    std::shared_lock lock(impl_->table_mutex);
    auto it = impl_->lock_table.find(lock_name);
    if (it == impl_->lock_table.end() || !it->second.is_held()) return 0;
    return it->second.fencing_token;
}

// ============================================================================
// DistributedLockManager — validate_fencing()
// ============================================================================

bool DistributedLockManager::validate_fencing(
    const std::string& lock_name,
    const std::string& owner,
    epoch_t token) const
{
    std::shared_lock lock(impl_->table_mutex);
    auto it = impl_->lock_table.find(lock_name);
    if (it == impl_->lock_table.end()) return false;
    if (!it->second.is_held()) return false;
    if (it->second.owner != owner) return false;
    return token == it->second.fencing_token;
}

// ============================================================================
// DistributedLockManager — force_release()
// ============================================================================

bool DistributedLockManager::force_release(const std::string& lock_name) {
    if (lock_name.empty()) return false;

    DLM_LOG_WARN("force_release: admin-forced release of lock '{}'", lock_name);

    std::unique_lock lock(impl_->table_mutex);
    auto it = impl_->lock_table.find(lock_name);
    if (it == impl_->lock_table.end() || !it->second.is_held()) return false;

    DLM_LOG_WARN("Force-releasing lock '{}' (owner='{}', held_for={}ms)",
                 lock_name, it->second.owner, now_ms() - it->second.acquired_at);

    it->second.owner.clear();
    it->second.acquired_at = 0;
    impl_->total_releases.fetch_add(1, std::memory_order_relaxed);
    return true;
}

// ============================================================================
// DistributedLockManager — get_stats()
// ============================================================================

DistributedLockManager::dlm_stats DistributedLockManager::get_stats() const {
    dlm_stats s;
    s.total_acquires    = impl_->total_acquires.load(std::memory_order_acquire);
    s.total_releases    = impl_->total_releases.load(std::memory_order_acquire);
    s.total_conflicts   = impl_->total_conflicts.load(std::memory_order_acquire);
    s.total_expirations = impl_->total_expirations.load(std::memory_order_acquire);
    s.total_timeouts    = impl_->total_timeouts.load(std::memory_order_acquire);

    std::shared_lock lock(impl_->table_mutex);
    s.current_locks_held = 0;
    for (const auto& [name, state] : impl_->lock_table) {
        if (state.is_held()) s.current_locks_held++;
    }
    s.total_locks_tracked = impl_->lock_table.size();
    return s;
}

// ============================================================================
// DistributedLockManager — cleanup_loop()
// ============================================================================

void DistributedLockManager::cleanup_loop() {
    DLM_LOG_INFO("DLM cleanup thread started");

    while (impl_->cleanup_running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(impl_->config.cleanup_interval);

        if (!impl_->cleanup_running.load(std::memory_order_acquire)) break;

        size_t expired = 0;
        {
            std::unique_lock lock(impl_->table_mutex);
            for (auto it = impl_->lock_table.begin();
                 it != impl_->lock_table.end(); ) {
                if (it->second.is_held() &&
                    it->second.is_expired(impl_->config.lock_ttl)) {
                    DLM_LOG_WARN("DLM cleanup: expiring lock '{}' (owner='{}')",
                                 it->first, it->second.owner);
                    it->second.owner.clear();
                    it->second.acquired_at = 0;
                    expired++;
                    ++it;
                } else if (!it->second.is_held() &&
                           (now_ms() - it->second.last_activity) >
                            impl_->config.lock_ttl.count() * 2) {
                    it = impl_->lock_table.erase(it);
                    expired++;
                } else {
                    ++it;
                }
            }
        }

        if (expired > 0) {
            impl_->total_expirations.fetch_add(expired, std::memory_order_relaxed);
            DLM_LOG_INFO("DLM cleanup: expired {} locks ({} tracked)",
                         expired, impl_->lock_table.size());
        }
    }

    DLM_LOG_INFO("DLM cleanup thread stopped");
}

// ============================================================================
// DistributedLockManager — simulate_raft_propose()
// ============================================================================

bool DistributedLockManager::simulate_raft_propose(
    std::vector<uint8_t> payload,
    int64_t timeout_ms)
{
    // In production, this calls server_->raft_node()->propose(payload)
    // and blocks until commit or timeout.
    // For the stub, we succeed by default since we already validated
    // local state before proposing.

    (void)payload;
    if (timeout_ms < 0) return false;

    DLM_LOG_TRACE("simulate_raft_propose: {} bytes, timeout={}ms",
                  payload.size(), timeout_ms);
    return true;
}

} // namespace torrent::broker
