#pragma once

/**
 * server.h — BrokerServer: Top-Level Orchestrator
 *
 * The BrokerServer wires together every subsystem inside a torrent-mq broker
 * process: storage engine, Raft consensus, network acceptor, client protocol
 * dispatcher, consumer-group coordinator, transaction coordinator, schema
 * registry, admin REST API, and Prometheus metrics.
 *
 * Lifecycle:  start()  →  running  →  shutdown()  →  stopped
 *                ↑                      ↑
 *                └── degraded ──────────┘  (non-critical subsystem failure)
 *
 * Subsystem init order:  storage → consensus → managers → network →
 *                        dispatcher + coordinators → schema → admin + metrics
 *
 * Shutdown drains in reverse order with configurable timeouts.
 *
 * Thread-safety: start()/shutdown() must be serialised. All const accessors
 * are safe from any thread after start() returns.
 */

#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <chrono>
#include <functional>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <optional>

#include "torrent/common/types.h"

// ---------------------------------------------------------------------------
// Forward declarations for sibling headers
// ---------------------------------------------------------------------------

namespace torrent {
    class config;
}

namespace torrent::network {
    class Acceptor;
}

namespace torrent::raft {
    struct RaftConfig;
}

namespace torrent::storage::log {
    class LogManager;
}

namespace torrent::broker {

class TopicManager;
class PartitionManager;
class RequestDispatcher;
class ConsumerGroupCoordinator;
class TransactionCoordinator;
class SchemaRegistry;
class MetadataCache;

// ============================================================================
// BrokerConfig — Per-broker settings resolved at process startup
// ============================================================================

/**
 * Broker-level configuration distinct from the global config singleton.
 * These values come from dedicated config keys (e.g. "broker.id",
 * "broker.rack", "advertised.listeners") and are fixed once the process
 * starts.
 */
struct BrokerConfig {
    // ---- Identity ----
    broker_id_t broker_id = kNoBroker;      ///< Unique cluster ID; must be ≥0.
    std::string rack;                        ///< Rack identifier for replica placement.

    // ---- Listeners ----
    std::vector<endpoint> advertised_listeners;  ///< Addresses advertised to clients/peers.
    std::optional<endpoint> controller_listener; ///< Dedicated control-plane endpoint (optional).

    // ---- Data directories ----
    std::string data_directory = "/var/lib/torrent";   ///< Root for all persistent state.

    // ---- Network tuning ----
    int32_t max_connections = 65536;    ///< Max concurrent client connections.
    int32_t num_io_threads = 0;         ///< Network I/O threads (0 = hardware_concurrency).
    int32_t num_worker_threads = 0;     ///< CPU-bound worker threads (0 = hardware_concurrency).

    // ---- Graceful shutdown ----
    std::chrono::milliseconds shutdown_timeout{30000};               ///< Max drain wait.
    std::chrono::milliseconds leadership_transfer_timeout{10000};    ///< Max leadership transfer wait.

    // ---- Feature flags ----
    bool enable_admin_api = true;            ///< Embedded Redpanda-compatible admin REST API.
    bool enable_metrics = true;              ///< Prometheus metrics export.
    bool enable_schema_registry = false;     ///< Avro/Protobuf/JSON Schema registry.
    bool enable_transactions = true;         ///< Exactly-once transactional produce.
    bool auto_create_topics = true;          ///< Auto-create topics on first produce.

    // ---- Cluster metadata ----
    std::string cluster_id;                  ///< Human-readable cluster name.
    std::vector<endpoint> seed_servers;      ///< Bootstrap peers for initial cluster formation.

    /// Returns true when all mandatory fields are valid.
    [[nodiscard]] bool validate() const noexcept;
};

// ============================================================================
// BrokerState — Lifecycle state machine
// ============================================================================

/**
 * Ordered lifecycle: starting → running → shutting_down → stopped.
 *
 * `degraded` is orthogonal: a running broker may be degraded when a
 * non-critical subsystem (e.g. schema registry) is unhealthy.
 *
 * Transitions:
 *   starting      → running         (all subsystems initialised)
 *   running       → degraded        (health check fails on non-critical)
 *   degraded      → running         (health check recovers)
 *   running       → shutting_down   (shutdown() called)
 *   degraded      → shutting_down   (shutdown() called while degraded)
 *   shutting_down → stopped         (all subsystems drained & closed)
 */
enum class BrokerState : uint8_t {
    starting     = 0,  ///< Initialising; not yet accepting connections.
    running      = 1,  ///< Fully operational.
    shutting_down = 2, ///< Draining connections; no new requests accepted.
    stopped      = 3,  ///< All subsystems stopped; process may exit.
    degraded     = 4,  ///< Running but a non-critical subsystem is unhealthy.
};

/// Human-readable state name.
[[nodiscard]] constexpr const char* to_string(BrokerState s) noexcept {
    switch (s) {
    case BrokerState::starting:      return "STARTING";
    case BrokerState::running:       return "RUNNING";
    case BrokerState::shutting_down: return "SHUTTING_DOWN";
    case BrokerState::stopped:       return "STOPPED";
    case BrokerState::degraded:      return "DEGRADED";
    }
    return "UNKNOWN";
}

// ============================================================================
// BrokerHealth — Aggregated health snapshot
// ============================================================================

/**
 * Point-in-time health report returned by health() and exposed via the
 * admin health-check endpoint and Prometheus gauge.
 *
 * Overall health is the conjunction of all enabled subsystem probes:
 *   - Network: listeners bound, connection limits not exhausted.
 *   - Storage: active partitions writable, sufficient disk space.
 *   - Consensus: Raft node functional; leader lease valid if leader.
 *   - Dispatcher: no internal queue overflows.
 *   - Admin / Metrics / Schema: optional subsystems reporting.
 */
struct BrokerHealth {
    bool is_healthy = false;                     ///< Conjunction of all enabled subsystem checks.
    BrokerState state = BrokerState::starting;   ///< Current lifecycle state.
    std::chrono::seconds uptime{0};              ///< Monotonic seconds since start() completed.

    // ---- Subsystem health flags ----
    bool network_healthy = false;
    bool storage_healthy = false;
    bool consensus_healthy = false;
    bool dispatcher_healthy = false;
    bool admin_healthy = false;
    bool metrics_healthy = false;
    bool schema_registry_healthy = false;

    // ---- Capacity metrics ----
    int64_t active_connections = 0;          ///< Live client connections.
    int64_t topic_count = 0;                 ///< Topics served (leader + follower).
    int64_t partition_count = 0;             ///< Partition replicas hosted.
    double disk_usage_ratio = 0.0;           ///< Data directory usage fraction (0.0–1.0).
    int64_t disk_free_bytes = 0;             ///< Free space on data volume.
    int64_t under_replicated_partitions = 0; ///< ISR < replication factor.
    int64_t active_leader_count = 0;         ///< Partitions led by this broker.

    // ---- Error / diagnostic ----
    error_code last_error = error_code::none;    ///< Most recent probe failure code.
    std::string last_error_message;              ///< Human-readable failure description.

    /// True when fully healthy and RUNNING.
    [[nodiscard]] bool fully_operational() const noexcept {
        return is_healthy && state == BrokerState::running;
    }

    /// True when accepting client work (RUNNING or DEGRADED).
    [[nodiscard]] bool accepting_work() const noexcept {
        return state == BrokerState::running || state == BrokerState::degraded;
    }
};

// ============================================================================
// BrokerServer — Top-level broker orchestrator
// ============================================================================

/**
 * Single-point lifecycle manager for a torrent-mq broker process.
 * Owns every subsystem and orchestrates startup/shutdown in dependency order.
 *
 * Usage:
 * @code
 *   BrokerConfig cfg;
 *   cfg.broker_id = 0;
 *   cfg.data_directory = "/data/torrent";
 *   cfg.seed_servers = { {"seed1", 9092}, {"seed2", 9092} };
 *
 *   BrokerServer server(cfg);
 *   server.start();
 *   // ... requests processed automatically ...
 *   server.shutdown();
 * @endcode
 */
class BrokerServer {
public:
    // ---- Construction / Destruction ----

    /// Construct from BrokerConfig.  Throws std::invalid_argument if invalid.
    explicit BrokerServer(const BrokerConfig& config);

    /// Construct by deriving BrokerConfig fields from a global torrent::config.
    explicit BrokerServer(const torrent::config& global_config);

    /// Destructor.  Calls emergency shutdown if still running.
    ~BrokerServer();

    BrokerServer(const BrokerServer&) = delete;
    BrokerServer& operator=(const BrokerServer&) = delete;
    BrokerServer(BrokerServer&&) = delete;
    BrokerServer& operator=(BrokerServer&&) = delete;

    // ---- Lifecycle ----

    /**
     * Initialise all subsystems and begin accepting client connections.
     *
     * Init order: (1) storage, (2) Raft consensus, (3) topic + partition
     * managers, (4) network acceptor, (5) request dispatcher + consumer
     * group coordinator + transaction coordinator, (6) schema registry,
     * (7) admin API + metrics exporter.
     *
     * Blocks until all subsystems report ready or a fatal error occurs.
     * @throws std::runtime_error if any critical subsystem fails.
     */
    void start();

    /**
     * Graceful shutdown with drain.
     *
     * Sequence: set state to shutting_down → drain in-flight requests
     * (up to shutdown_timeout) → transfer partition leadership → close
     * network listeners → flush and close storage → final Raft step-down →
     * stop admin/metrics/schema → set state to stopped.
     *
     * Idempotent: calling on an already-stopped server is a no-op.
     */
    void shutdown();

    // ---- State Queries ----

    /// Current lifecycle state.  Safe from any thread after construction.
    [[nodiscard]] BrokerState state() const noexcept;

    /// Aggregate health snapshot (copy of the most recent probe).
    [[nodiscard]] BrokerHealth health() const;

    /// This broker's unique cluster ID (never kNoBroker after construction).
    [[nodiscard]] broker_id_t broker_id() const noexcept;

    /// True when this broker is the active Raft leader (cluster controller).
    [[nodiscard]] bool is_controller() const noexcept;

    // ---- Metadata ----

    /// Cached cluster metadata (broker list, topic→partition map, controller
    /// epoch).  Updated asynchronously via Raft log application.
    [[nodiscard]] const MetadataCache& metadata_cache() const noexcept;

    // ---- Subsystem Accessors ----

    /// Topic CRUD; controller-only for mutations, followers proxy to leader.
    [[nodiscard]] TopicManager& topic_manager() noexcept;
    [[nodiscard]] const TopicManager& topic_manager() const noexcept;

    /// Partition leadership, ISR management, high-watermark tracking.
    [[nodiscard]] PartitionManager& partition_manager() noexcept;
    [[nodiscard]] const PartitionManager& partition_manager() const noexcept;

    /// Routes produce/fetch/metadata/consumer-group/transaction RPCs.
    [[nodiscard]] RequestDispatcher& request_dispatcher() noexcept;
    [[nodiscard]] const RequestDispatcher& request_dispatcher() const noexcept;

    /// Group membership, offset commits, rebalance protocol.
    [[nodiscard]] ConsumerGroupCoordinator& group_coordinator() noexcept;
    [[nodiscard]] const ConsumerGroupCoordinator& group_coordinator() const noexcept;

    /// Producer ID allocation, transaction state machine, fencing.
    [[nodiscard]] TransactionCoordinator& txn_coordinator() noexcept;
    [[nodiscard]] const TransactionCoordinator& txn_coordinator() const noexcept;

    /// Schema registry (Avro/Protobuf/JSON Schema).  nullptr if not enabled.
    [[nodiscard]] SchemaRegistry* schema_registry() noexcept;
    [[nodiscard]] const SchemaRegistry* schema_registry() const noexcept;

    // ---- Internal / Testing ----

    /// Total connections accepted since start().
    [[nodiscard]] uint64_t total_connections() const noexcept;

    /// Force an immediate health probe (normally periodic).
    [[nodiscard]] BrokerHealth probe_health();

    /// Block until STOPPED or timeout expires.  Returns true if stopped.
    [[nodiscard]] bool wait_for_shutdown(std::chrono::milliseconds timeout);

    /// Read-only access to broker configuration.
    [[nodiscard]] const BrokerConfig& config() const noexcept { return config_; }

private:
    // ---- Configuration ----
    BrokerConfig config_;

    // ---- Lifecycle state ----
    std::atomic<BrokerState> state_{BrokerState::starting};
    mutable std::shared_mutex health_mutex_;
    BrokerHealth cached_health_;

    // ---- Timing ----
    std::chrono::steady_clock::time_point start_time_;
    std::chrono::steady_clock::time_point stop_time_;

    // ---- Subsystem ownership ----
    std::unique_ptr<storage::log::LogManager> log_manager_;
    std::unique_ptr<class RaftNodeWrapper> raft_node_;
    std::unique_ptr<TopicManager> topic_manager_;
    std::unique_ptr<PartitionManager> partition_manager_;
    std::unique_ptr<RequestDispatcher> request_dispatcher_;
    std::unique_ptr<ConsumerGroupCoordinator> group_coordinator_;
    std::unique_ptr<TransactionCoordinator> txn_coordinator_;
    std::unique_ptr<SchemaRegistry> schema_registry_;
    std::unique_ptr<MetadataCache> metadata_cache_;
    std::unique_ptr<network::Acceptor> acceptor_;
    std::unique_ptr<class AdminServer> admin_server_;
    std::unique_ptr<class MetricsExporter> metrics_exporter_;

    // ---- Background threads ----
    std::thread health_probe_thread_;
    std::atomic<bool> health_probe_running_{false};

    // ---- Internal helpers ----
    void start_storage();
    void start_consensus();
    void start_topics_and_partitions();
    void start_network();
    void start_dispatcher_and_coordinators();
    void start_schema_registry();
    void start_admin_and_metrics();
    void start_health_probe_loop();

    void stop_health_probe_loop();
    void stop_admin_and_metrics();
    void stop_schema_registry();
    void stop_dispatcher_and_coordinators();
    void stop_network();
    void stop_topics_and_partitions();
    void stop_consensus();
    void stop_storage();

    BrokerHealth collect_health() const;
    void update_cached_health(BrokerHealth health);
};

// ============================================================================
// Free functions — convenience constructors
// ============================================================================

/// Create a BrokerServer from a JSON/YAML config file path.
[[nodiscard]] std::unique_ptr<BrokerServer> create_server_from_file(
    const std::string& config_path);

/// Create a BrokerServer from command-line arguments.
[[nodiscard]] std::unique_ptr<BrokerServer> create_server_from_args(
    int argc, char** argv);

} // namespace torrent::broker
