/**
 * connection_pool.cpp — ConnectionPool: Connection Lifecycle Management
 *
 * Manages a pool of active network connections.  Provides thread-safe
 * add/remove/get operations and graceful shutdown (close_all).
 *
 * The pool is the central registry for all connections managed by a
 * broker node.  Each connection is identified by a unique uint64_t
 * connection_id assigned at accept/connect time.
 *
 * Features:
 *   - O(log N) lookup by connection_id via std::map
 *   - Thread-safe add/remove/get operations via std::mutex
 *   - Graceful close_all() for shutdown: drains all connections before
 *     releasing them
 *   - Active connection count tracking for monitoring/metrics
 *   - Bulk operations: connection_ids() returns all IDs, size() gives count
 *   - Integration with the Connection lifecycle: connections self-remove
 *     on close via a callback (optional, configured per-connection)
 *
 * Thread safety:
 *   - All public methods acquire a mutex lock.
 *   - The pool does NOT own the I/O lifecycle of connections; it only
 *     maintains the registry.  I/O is driven by the event loop.
 *   - close_all() calls drain() then close() on each connection,
 *     releasing shared_ptr references so connections can finalize.
 */

// ============================================================================
// Project headers
// ============================================================================

#include "torrent/network/connection_pool.h"
#include "torrent/network/connection.h"
#include "torrent/common/types.h"

// ============================================================================
// System headers
// ============================================================================

#include <algorithm>
#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <spdlog/spdlog.h>

namespace torrent::network {

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace {

/// Logger instance.
std::shared_ptr<spdlog::logger> get_pool_logger() {
    static auto logger = spdlog::get("connection_pool");
    if (!logger) {
        logger = spdlog::stdout_color_mt("connection_pool");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

/// Maximum time to wait for connections to drain during close_all (ms).
inline constexpr torrent::timestamp_ms_t kDrainTimeoutMs = 5000;

/// Poll interval during drain wait (ms).
inline constexpr torrent::timestamp_ms_t kDrainPollIntervalMs = 50;

} // anonymous namespace

// ============================================================================
// ConnectionPool — construction / destruction
// ============================================================================

ConnectionPool::ConnectionPool() {
    auto logger = get_pool_logger();
    logger->debug("ConnectionPool created");
}

// The destructor automatically calls close_all().  If the pool still
// holds connections at destruction time, they are drained and closed.
ConnectionPool::~ConnectionPool() {
    close_all();
}

// ============================================================================
// ConnectionPool::add
// ============================================================================

void ConnectionPool::add(std::shared_ptr<Connection> conn) {
    if (!conn) {
        auto logger = get_pool_logger();
        logger->warn("ConnectionPool::add called with null connection, ignoring");
        return;
    }

    uint64_t conn_id = conn->connection_id();

    std::lock_guard<std::mutex> lock(mutex_);

    // Check for duplicate connection ID.
    if (connections_.find(conn_id) != connections_.end()) {
        auto logger = get_pool_logger();
        logger->warn("ConnectionPool::add: connection {} already exists, replacing",
                     conn_id);
    }

    connections_[conn_id] = std::move(conn);

    auto logger = get_pool_logger();
    logger->debug("ConnectionPool: added connection {} (total: {})",
                  conn_id, connections_.size());
}

// ============================================================================
// ConnectionPool::remove
// ============================================================================

void ConnectionPool::remove(uint64_t conn_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = connections_.find(conn_id);
    if (it == connections_.end()) {
        auto logger = get_pool_logger();
        logger->trace("ConnectionPool::remove: connection {} not found", conn_id);
        return;
    }

    // If the connection is still active, drain it before removal.
    if (it->second && it->second->is_active()) {
        it->second->drain();
    }

    connections_.erase(it);

    auto logger = get_pool_logger();
    logger->debug("ConnectionPool: removed connection {} (total: {})",
                  conn_id, connections_.size());
}

// ============================================================================
// ConnectionPool::get
// ============================================================================

std::shared_ptr<Connection> ConnectionPool::get(uint64_t conn_id) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = connections_.find(conn_id);
    if (it != connections_.end()) {
        return it->second;
    }

    return nullptr;
}

// ============================================================================
// ConnectionPool::size
// ============================================================================

size_t ConnectionPool::size() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return connections_.size();
}

// ============================================================================
// ConnectionPool::close_all
// ============================================================================

void ConnectionPool::close_all() {
    auto logger = get_pool_logger();

    // Phase 1: Drain all connections (graceful shutdown).
    // Drain means stop accepting new requests but allow in-flight
    // requests to complete.
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (connections_.empty()) {
            logger->debug("ConnectionPool::close_all: pool already empty");
            return;
        }

        size_t count = connections_.size();
        logger->info("ConnectionPool: draining {} connections", count);

        for (auto& [id, conn] : connections_) {
            if (conn && !conn->is_closed()) {
                conn->drain();
            }
        }
    }

    // Phase 2: Wait for connections to drain (with timeout).
    // Poll until all connections are closed or timeout expires.
    auto start = std::chrono::steady_clock::now();

    while (true) {
        bool all_closed = true;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto& [id, conn] : connections_) {
                if (conn && !conn->is_closed()) {
                    all_closed = false;
                    break;
                }
            }
        }

        if (all_closed) break;

        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();

        if (elapsed >= static_cast<long long>(kDrainTimeoutMs)) {
            logger->warn("ConnectionPool: drain timeout after {}ms, "
                         "forcing close on remaining connections", elapsed);
            break;
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kDrainPollIntervalMs));
    }

    // Phase 3: Force-close any remaining connections and clear the map.
    {
        std::lock_guard<std::mutex> lock(mutex_);

        for (auto& [id, conn] : connections_) {
            if (conn && !conn->is_closed()) {
                conn->close();
                logger->debug("ConnectionPool: force-closed connection {}", id);
            }
        }

        size_t closed_count = connections_.size();
        connections_.clear();

        logger->info("ConnectionPool: closed all {} connections", closed_count);
    }
}

// ============================================================================
// ConnectionPool::connection_ids
// ============================================================================

std::vector<uint64_t> ConnectionPool::connection_ids() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<uint64_t> ids;
    ids.reserve(connections_.size());

    for (const auto& [id, conn] : connections_) {
        ids.push_back(id);
    }

    return ids;
}

// ============================================================================
// Additional convenience methods
// ============================================================================

/**
 * Return a snapshot of connection metrics for all active connections.
 * Useful for monitoring / Prometheus export.
 *
 * NOTE: This is not declared in the header (internal helper).
 * If needed, add a declaration to connection_pool.h.
 */
namespace {

struct PoolMetrics {
    size_t   total_connections    = 0;
    size_t   active_connections   = 0;
    size_t   draining_connections = 0;
    size_t   handshaking          = 0;
    uint64_t total_bytes_received  = 0;
    uint64_t total_bytes_sent      = 0;
    uint64_t total_requests        = 0;
    uint64_t total_responses       = 0;
    uint64_t total_errors          = 0;
};

PoolMetrics collect_pool_metrics(
    const std::map<uint64_t, std::shared_ptr<Connection>>& connections)
{
    PoolMetrics m;
    m.total_connections = connections.size();

    for (const auto& [id, conn] : connections) {
        if (!conn) continue;

        auto state = conn->state();
        switch (state) {
        case ConnectionState::active:
            ++m.active_connections;
            break;
        case ConnectionState::draining:
            ++m.draining_connections;
            break;
        case ConnectionState::handshaking:
            ++m.handshaking;
            break;
        default:
            break;
        }

        const auto& metrics = conn->metrics();
        m.total_bytes_received += metrics.bytes_received.load(std::memory_order_relaxed);
        m.total_bytes_sent     += metrics.bytes_sent.load(std::memory_order_relaxed);
        m.total_requests        += metrics.requests_received.load(std::memory_order_relaxed);
        m.total_responses       += metrics.responses_sent.load(std::memory_order_relaxed);
        m.total_errors          += metrics.framing_errors.load(std::memory_order_relaxed)
                                 + metrics.dispatch_errors.load(std::memory_order_relaxed)
                                 + metrics.sasl_failures.load(std::memory_order_relaxed);
    }

    return m;
}

} // anonymous namespace

// ============================================================================
// Additional public convenience methods (not in header, but useful internally)
// ============================================================================

/**
 * Iterate over all connections and apply a callback.  The callback
 * receives (conn_id, conn_ptr).  If the callback returns false, iteration
 * stops early.
 *
 * The pool mutex is held during iteration; callbacks must not deadlock
 * by re-entering the pool.
 */
void ConnectionPool::for_each(
    std::function<bool(uint64_t, std::shared_ptr<Connection>)> callback) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [id, conn] : connections_) {
        if (conn && !callback(id, conn)) {
            break;
        }
    }
}

/**
 * Find a connection by the peer's endpoint (host:port).
 * Returns the first matching connection or nullptr.
 */
std::shared_ptr<Connection> ConnectionPool::find_by_endpoint(
    const endpoint& ep) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [id, conn] : connections_) {
        if (conn && conn->peer() == ep && !conn->is_closed()) {
            return conn;
        }
    }
    return nullptr;
}

/**
 * Count connections in each lifecycle state.
 */
ConnectionPool::StateCounts ConnectionPool::state_counts() const {
    StateCounts counts = {};

    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [id, conn] : connections_) {
        if (!conn) continue;
        switch (conn->state()) {
        case ConnectionState::handshaking: ++counts.handshaking; break;
        case ConnectionState::active:      ++counts.active; break;
        case ConnectionState::draining:    ++counts.draining; break;
        case ConnectionState::closing:     ++counts.closing; break;
        case ConnectionState::closed:      ++counts.closed; break;
        }
    }

    return counts;
}

/**
 * Return the connection with the highest bytes_received count.
 * Useful for identifying hot connections.
 */
std::shared_ptr<Connection> ConnectionPool::hottest_connection() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::shared_ptr<Connection> hottest;
    uint64_t max_bytes = 0;

    for (const auto& [id, conn] : connections_) {
        if (!conn || conn->is_closed()) continue;
        uint64_t bytes = conn->metrics().bytes_received.load(std::memory_order_relaxed);
        if (bytes > max_bytes) {
            max_bytes = bytes;
            hottest = conn;
        }
    }

    return hottest;
}

/**
 * Check if a connection with the given ID is in the pool and active.
 */
bool ConnectionPool::is_active(uint64_t conn_id) const {
    auto conn = get(conn_id);
    return conn && conn->is_active();
}

/**
 * Drain connections that match a predicate.  Returns the number of
 * connections that were drained.
 *
 * The predicate receives (conn_id, conn_ptr) and should return true
 * for connections that should be drained.
 */
size_t ConnectionPool::drain_if(
    std::function<bool(uint64_t, const std::shared_ptr<Connection>&)> predicate)
{
    std::vector<std::shared_ptr<Connection>> to_drain;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [id, conn] : connections_) {
            if (conn && !conn->is_closed() && predicate(id, conn)) {
                to_drain.push_back(conn);
            }
        }
    }

    size_t count = 0;
    for (auto& conn : to_drain) {
        conn->drain();
        ++count;
    }

    if (count > 0) {
        auto logger = get_pool_logger();
        logger->info("Drained {} connections matching predicate", count);
    }

    return count;
}

/**
 * Close connections that have been idle for more than `idle_ms`.
 * Returns the number closed.
 */
size_t ConnectionPool::close_idle(torrent::timestamp_ms_t idle_ms,
                                   torrent::timestamp_ms_t now_ms)
{
    if (idle_ms <= 0) return 0;

    std::vector<uint64_t> to_close;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [id, conn] : connections_) {
            if (!conn || conn->is_closed()) continue;
            auto last_read = conn->metrics().last_read_at_ms.load(
                std::memory_order_relaxed);
            if (last_read > 0 && (now_ms - last_read) > idle_ms) {
                to_close.push_back(id);
            }
        }
    }

    size_t count = 0;
    for (uint64_t id : to_close) {
        auto conn = get(id);
        if (conn && !conn->is_closed()) {
            conn->drain();
            conn->close();
            ++count;
        }
    }

    if (count > 0) {
        auto logger = get_pool_logger();
        logger->info("Closed {} idle connections (>{}ms)", count, idle_ms);
    }

    return count;
}

/**
 * Total bytes received across all connections in the pool.
 */
uint64_t ConnectionPool::total_bytes_received() const {
    uint64_t total = 0;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [id, conn] : connections_) {
        if (conn) {
            total += conn->metrics().bytes_received.load(
                std::memory_order_relaxed);
        }
    }
    return total;
}

/**
 * Total bytes sent across all connections in the pool.
 */
uint64_t ConnectionPool::total_bytes_sent() const {
    uint64_t total = 0;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [id, conn] : connections_) {
        if (conn) {
            total += conn->metrics().bytes_sent.load(
                std::memory_order_relaxed);
        }
    }
    return total;
}

} // namespace torrent::network
