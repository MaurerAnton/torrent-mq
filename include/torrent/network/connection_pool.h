#pragma once
#include <memory>
#include <string>
#include <torrent/common/types.h>

namespace torrent::network {
class Connection;

class ConnectionPool {
public:
    ConnectionPool();
    ~ConnectionPool();

    void add(std::shared_ptr<Connection> conn);
    void remove(uint64_t conn_id);
    std::shared_ptr<Connection> get(uint64_t conn_id) const;
    size_t size() const noexcept;
    void close_all();
    std::vector<uint64_t> connection_ids() const;

    // -- Extended operations -----------------------------------------------

    /// Iterate over all connections with a callback.  If callback returns
    /// false, iteration stops early.
    void for_each(std::function<bool(uint64_t, std::shared_ptr<Connection>)>) const;

    /// Find a connection by peer endpoint (host:port).
    std::shared_ptr<Connection> find_by_endpoint(const endpoint& ep) const;

    /// Per-state connection counts.
    struct StateCounts {
        size_t handshaking = 0;
        size_t active      = 0;
        size_t draining    = 0;
        size_t closing     = 0;
        size_t closed      = 0;
    };
    [[nodiscard]] StateCounts state_counts() const;

    /// Return the connection with the most bytes received.
    std::shared_ptr<Connection> hottest_connection() const;

    /// Check if a connection is active.
    [[nodiscard]] bool is_active(uint64_t conn_id) const;

    /// Drain connections matching a predicate.  Returns count drained.
    size_t drain_if(std::function<bool(uint64_t, const std::shared_ptr<Connection>&)>);

    /// Close connections idle for more than idle_ms.  Returns count closed.
    size_t close_idle(torrent::timestamp_ms_t idle_ms,
                      torrent::timestamp_ms_t now_ms);

    /// Total bytes received across all connections.
    [[nodiscard]] uint64_t total_bytes_received() const;

    /// Total bytes sent across all connections.
    [[nodiscard]] uint64_t total_bytes_sent() const;

private:
    std::map<uint64_t, std::shared_ptr<Connection>> connections_;
    mutable std::mutex mutex_;
};
}