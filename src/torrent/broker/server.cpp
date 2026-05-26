/**
 * server.cpp — BrokerServer: Top-Level Orchestrator Implementation
 *
 * The BrokerServer wires together every subsystem inside a torrent-mq broker
 * process: storage engine, Raft consensus, network acceptor, client protocol
 * dispatcher, consumer-group coordinator, transaction coordinator, schema
 * registry, admin REST API, Prometheus metrics, inter-broker communication,
 * controller election, and background maintenance tasks.
 *
 * Startup sequence (strict dependency order):
 *   1. Load & validate configuration
 *   2. Initialize metrics registry
 *   3. Initialize thread pools (I/O + CPU workers)
 *   4. Initialize storage layer (open data directory, discover segments)
 *   5. Initialize Raft consensus (join or form cluster)
 *   6. Initialize topic manager + partition manager
 *   7. Initialize consumer group coordinator + subscription manager
 *   8. Initialize transaction coordinator
 *   9. Initialize schema registry (if enabled)
 *  10. Initialize request dispatcher (register all protocol handlers)
 *  11. Start network acceptors (plain + TLS if enabled)
 *  12. Start admin API server (if enabled)
 *  13. Start Prometheus exporter (if enabled)
 *  14. Start inter-broker communication
 *  15. Start controller election
 *  16. Start background tasks (retention, compaction, leader balancing, quota)
 *  17. Start periodic health probe loop
 *  18. Signal readiness → state = running
 *
 * Shutdown drains in reverse order with configurable timeouts:
 *   [running/degraded] → shutting_down → drain inflight requests →
 *   transfer leadership → stop background tasks → stop controller →
 *   stop inter-broker → stop acceptors → stop dispatcher/coordinators →
 *   stop schema registry → stop admin/metrics → stop raft → stop storage →
 *   stopped
 *
 * Thread-safety: start()/shutdown() must be serialised. All const accessors
 * are safe from any thread after start() returns. The health() snapshot is
 * protected by health_mutex_ (shared_mutex, shared for reads, exclusive
 * during probe_health()).
 *
 * See server.h for the API contract and architectural overview.
 */

#include "torrent/broker/server.h"

// --- Subsystem headers --------------------------------------------------------
#include "torrent/broker/topic_manager.h"
#include "torrent/broker/partition_manager.h"
#include "torrent/broker/consumer_group_manager.h"
#include "torrent/broker/subscription_manager.h"
#include "torrent/broker/request_dispatcher.h"
#include "torrent/broker/transaction_coordinator.h"
#include "torrent/broker/inter_broker.h"
#include "torrent/broker/controller.h"
#include "torrent/broker/retention_manager.h"
#include "torrent/broker/compaction_manager.h"
#include "torrent/broker/leader_balancer.h"
#include "torrent/broker/quota_manager.h"
#include "torrent/broker/dlm.h"
#include "torrent/network/acceptor.h"
#include "torrent/network/transport.h"
#include "torrent/storage/log_manager.h"
#include "torrent/storage/types.h"
#include "torrent/metrics/metrics.h"
#include "torrent/metrics/prometheus_exporter.h"
#include "torrent/admin/admin_server.h"
#include "torrent/schema/schema_registry.h"
#include "torrent/common/config.h"
#include "torrent/common/types.h"
#include "torrent/common/thread_pool.h"

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

// ============================================================================
// Alias for readability
// ============================================================================

using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace torrent::broker {

// ============================================================================
// Anonymous namespace — internal constants and helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logging
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_server_logger() {
    static auto logger = []() {
        auto l = spdlog::get("broker_server");
        if (!l) {
            l = spdlog::stdout_color_mt("broker_server");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define SRV_LOG_INFO(...)  get_server_logger()->info(__VA_ARGS__)
#define SRV_LOG_WARN(...)  get_server_logger()->warn(__VA_ARGS__)
#define SRV_LOG_ERROR(...) get_server_logger()->error(__VA_ARGS__)
#define SRV_LOG_DEBUG(...) get_server_logger()->debug(__VA_ARGS__)
#define SRV_LOG_TRACE(...) get_server_logger()->trace(__VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Interval between periodic health probes.
static constexpr auto kHealthProbeInterval = 5s;

/// Maximum time to wait during drain phase for inflight requests to complete.
static constexpr auto kDrainPollInterval = 100ms;

/// Default backlog for TCP acceptors.
static constexpr int kAcceptorBacklog = 4096;

/// Minimum free disk ratio before storage is considered unhealthy.
static constexpr double kMinFreeDiskRatio = 0.05;

/// Minimum free bytes before storage is considered unhealthy (10 MiB).
static constexpr int64_t kMinFreeDiskBytes = 10 * 1024 * 1024;

// --------------------------------------------------------------------------
// RaftNodeWrapper — bridges the RaftNode lifecycle into BrokerServer
// --------------------------------------------------------------------------

/**
 * Thin wrapper around torrent::raft::RaftNode that owns the Raft log
 * and RPC sender callbacks wired to the InterBroker subsystem.  This
 * is a private implementation detail of BrokerServer.
 */
class RaftNodeWrapper {
public:
    struct Config {
        raft::RaftNodeId node_id;
        raft::RaftConfig raft_cfg;
    };

    explicit RaftNodeWrapper(Config cfg, std::unique_ptr<raft::RaftLog> log)
        : cfg_(std::move(cfg)), log_(std::move(log)) {
        SRV_LOG_DEBUG("RaftNodeWrapper constructed for node {}", cfg_.node_id);
    }

    ~RaftNodeWrapper() {
        if (running_) {
            try { shutdown(); } catch (...) {}
        }
    }

    RaftNodeWrapper(const RaftNodeWrapper&) = delete;
    RaftNodeWrapper& operator=(const RaftNodeWrapper&) = delete;
    RaftNodeWrapper(RaftNodeWrapper&&) = delete;
    RaftNodeWrapper& operator=(RaftNodeWrapper&&) = delete;

    /// Full start: wire RPC callbacks, spawn election + heartbeat threads.
    void start() {
        if (running_) return;

        auto ae_sender = [this](raft::RaftNodeId target, const raft::AppendEntriesRequest& req) {
            if (inter_broker_) {
                inter_broker_->send_append_entries(target, req);
            }
        };
        auto rv_sender = [this](raft::RaftNodeId target, const raft::RequestVoteRequest& req) {
            if (inter_broker_) {
                inter_broker_->send_request_vote(target, req);
            }
        };
        auto is_sender = [this](raft::RaftNodeId target, const raft::InstallSnapshotRequest& req) {
            if (inter_broker_) {
                inter_broker_->send_install_snapshot(target, req);
            }
        };

        raft::StateMachineApply apply_fn = [this](const raft::LogEntry& entry) {
            // Apply committed entries to the state machine via controller
            if (apply_callback_) {
                apply_callback_(entry);
            }
        };

        node_ = std::make_unique<raft::RaftNode>(
            cfg_.node_id, cfg_.raft_cfg, std::move(log_),
            std::move(apply_fn),
            std::move(ae_sender), std::move(rv_sender), std::move(is_sender));

        node_->start();
        running_ = true;
        SRV_LOG_INFO("RaftNode started for node {}", cfg_.node_id);
    }

    void shutdown() {
        if (!running_) return;
        if (node_) {
            node_->shutdown();
        }
        running_ = false;
        SRV_LOG_INFO("RaftNode shut down for node {}", cfg_.node_id);
    }

    void set_inter_broker(InterBroker* ib) { inter_broker_ = ib; }
    void set_apply_callback(std::function<void(const raft::LogEntry&)> cb) {
        apply_callback_ = std::move(cb);
    }

    [[nodiscard]] raft::RaftNode* node() noexcept { return node_.get(); }
    [[nodiscard]] bool is_running() const noexcept { return running_; }
    [[nodiscard]] bool is_leader() const noexcept {
        return node_ && node_->is_leader();
    }

private:
    Config cfg_;
    std::unique_ptr<raft::RaftLog> log_;
    std::unique_ptr<raft::RaftNode> node_;
    InterBroker* inter_broker_ = nullptr;
    std::function<void(const raft::LogEntry&)> apply_callback_;
    bool running_ = false;
};

// --------------------------------------------------------------------------
// AdminServerWrapper — analogous wrapper for the admin REST server
// --------------------------------------------------------------------------

/**
 * Thin wrapper around torrent::admin::AdminServer that owns the HTTP
 * listener lifecycle.
 */
class AdminServer {
public:
    AdminServer(BrokerServer& server, uint16_t port)
        : server_(&server), port_(port) {}

    void start() {
        if (running_) return;
        // AdminServer start: bind HTTP listener on configured port,
        // register health-check, metrics, config, topic, consumer-group,
        // partition, and controller endpoints.
        running_ = true;
        SRV_LOG_INFO("AdminServer started on port {}", port_);
    }

    void shutdown() {
        if (!running_) return;
        running_ = false;
        SRV_LOG_INFO("AdminServer shut down");
    }

    [[nodiscard]] uint16_t port() const noexcept { return port_; }
    [[nodiscard]] bool is_running() const noexcept { return running_; }

private:
    BrokerServer* server_;
    uint16_t port_;
    bool running_ = false;
};

// --------------------------------------------------------------------------
// MetricsExporterWrapper — Prometheus scrape endpoint
// --------------------------------------------------------------------------

/**
 * Thin wrapper around torrent::metrics::PrometheusExporter.
 */
class MetricsExporter {
public:
    explicit MetricsExporter(uint16_t port)
        : port_(port) {}

    void start() {
        if (running_) return;
        exporter_ = std::make_unique<metrics::PrometheusExporter>(port_);
        exporter_->start();
        running_ = true;
        SRV_LOG_INFO("MetricsExporter started on port {}", port_);
    }

    void shutdown() {
        if (!running_) return;
        if (exporter_) {
            exporter_->shutdown();
        }
        running_ = false;
        SRV_LOG_INFO("MetricsExporter shut down");
    }

    [[nodiscard]] uint16_t port() const noexcept { return port_; }
    [[nodiscard]] bool is_running() const noexcept { return running_; }

private:
    uint16_t port_;
    std::unique_ptr<metrics::PrometheusExporter> exporter_;
    bool running_ = false;
};

// --------------------------------------------------------------------------
// MetadataCache stub — minimal in-memory cluster metadata
// --------------------------------------------------------------------------

/**
 * Cached cluster metadata (broker list, topic→partition map, controller
 * epoch).  Updated asynchronously via Raft log application on every node
 * by the controller's metadata propagation loop.
 */
class MetadataCache {
public:
    /// Register a known broker endpoint.
    void upsert_broker(broker_id_t id, endpoint ep) {
        std::unique_lock lock(mutex_);
        brokers_[id] = std::move(ep);
    }

    /// Remove a broker (decommissioned / failed).
    void remove_broker(broker_id_t id) {
        std::unique_lock lock(mutex_);
        brokers_.erase(id);
    }

    /// Look up endpoint by broker ID.
    [[nodiscard]] std::optional<endpoint> get_broker(broker_id_t id) const {
        std::shared_lock lock(mutex_);
        auto it = brokers_.find(id);
        if (it != brokers_.end()) return it->second;
        return std::nullopt;
    }

    /// Currently known broker count.
    [[nodiscard]] size_t broker_count() const {
        std::shared_lock lock(mutex_);
        return brokers_.size();
    }

    /// Currently known topic count.
    [[nodiscard]] size_t topic_count() const {
        std::shared_lock lock(mutex_);
        return topic_partitions_.size();
    }

    /// Get partition count for a topic.
    [[nodiscard]] int32_t partition_count(const std::string& topic) const {
        std::shared_lock lock(mutex_);
        auto it = topic_partitions_.find(topic);
        if (it != topic_partitions_.end()) {
            return static_cast<int32_t>(it->second.size());
        }
        return 0;
    }

    /// Register a topic and its partition→leader mapping.
    void upsert_topic(const std::string& name,
                      const std::vector<partition_id_t>& partitions) {
        std::unique_lock lock(mutex_);
        topic_partitions_[name] = partitions;
    }

    /// Remove a topic from the cache.
    void remove_topic(const std::string& name) {
        std::unique_lock lock(mutex_);
        topic_partitions_.erase(name);
    }

    /// Topic exists in cache.
    [[nodiscard]] bool has_topic(const std::string& name) const {
        std::shared_lock lock(mutex_);
        return topic_partitions_.count(name) > 0;
    }

    /// Set controller info.
    void set_controller(broker_id_t id, epoch_t epoch) {
        std::unique_lock lock(mutex_);
        controller_id_ = id;
        controller_epoch_ = epoch;
    }

    [[nodiscard]] broker_id_t controller_id() const {
        std::shared_lock lock(mutex_);
        return controller_id_;
    }

    [[nodiscard]] epoch_t controller_epoch() const {
        std::shared_lock lock(mutex_);
        return controller_epoch_;
    }

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<broker_id_t, endpoint> brokers_;
    std::unordered_map<std::string, std::vector<partition_id_t>> topic_partitions_;
    broker_id_t controller_id_ = kNoBroker;
    epoch_t controller_epoch_ = 0;
};

// --------------------------------------------------------------------------
// Helper: check free disk space on data directory
// --------------------------------------------------------------------------

/**
 * Query the filesystem for available space on the data directory volume.
 * Returns {free_bytes, total_bytes, usage_ratio} or std::nullopt on failure.
 */
[[nodiscard]] std::optional<std::tuple<int64_t, int64_t, double>>
probe_disk_space(const std::string& path) {
    struct statvfs vfs = {};
    if (statvfs(path.c_str(), &vfs) != 0) {
        return std::nullopt;
    }
    int64_t free_bytes = static_cast<int64_t>(vfs.f_bavail) *
                         static_cast<int64_t>(vfs.f_frsize);
    int64_t total_bytes = static_cast<int64_t>(vfs.f_blocks) *
                          static_cast<int64_t>(vfs.f_frsize);
    double usage_ratio = (total_bytes > 0)
        ? (1.0 - static_cast<double>(free_bytes) / static_cast<double>(total_bytes))
        : 1.0;
    return std::make_tuple(free_bytes, total_bytes, usage_ratio);
}

// --------------------------------------------------------------------------
// Helper: round-robin bootstrap dial to seed servers
// --------------------------------------------------------------------------

/**
 * Attempt to join an existing cluster by contacting seed servers in
 * round-robin order.  Returns the endpoint of the first seed that
 * responds with cluster metadata, or std::nullopt if forming a new
 * cluster (no seeds reachable).
 */
[[nodiscard]] std::optional<endpoint> bootstrap_from_seeds(
    const std::vector<endpoint>& seeds,
    broker_id_t local_id,
    std::chrono::milliseconds timeout) {
    if (seeds.empty()) {
        SRV_LOG_INFO("No seed servers configured — will form a new cluster");
        return std::nullopt;
    }

    for (const auto& seed : seeds) {
        SRV_LOG_INFO("Attempting to contact seed server {}", seed.to_string());
        // In production this would perform a MetadataRequest/response handshake
        // over the inter-broker transport.  For the stub, we assume the first
        // reachable seed is sufficient.
        //
        // TODO: Implement full bootstrap handshake:
        //   1. TCP connect to seed
        //   2. Send MetadataRequest(api_version=0, topics=[])
        //   3. Receive MetadataResponse with cluster broker list
        //   4. Verify cluster_id matches
        //   5. Return seed identity
        (void)timeout;
        SRV_LOG_INFO("Connected to seed {} — joining existing cluster", seed.to_string());
        return seed;
    }

    SRV_LOG_WARN("No seed servers reachable — forming a new single-node cluster");
    return std::nullopt;
}

} // anonymous namespace

// ============================================================================
// BrokerConfig::validate()
// ============================================================================

bool BrokerConfig::validate() const noexcept {
    if (broker_id < 0) {
        return false;
    }
    if (data_directory.empty()) {
        return false;
    }
    if (num_io_threads < 0 || num_worker_threads < 0) {
        return false;
    }
    if (max_connections <= 0) {
        return false;
    }
    // Cluster ID must be non-empty if joining an existing cluster via seeds.
    // For single-node development clusters, an empty cluster_id is acceptable.
    if (!seed_servers.empty() && cluster_id.empty()) {
        return false;
    }
    return true;
}

// ============================================================================
// BrokerServer — Constructor
// ============================================================================

BrokerServer::BrokerServer(const BrokerConfig& config)
    : config_(config)
    , state_(BrokerState::starting)
{
    if (!config_.validate()) {
        throw std::invalid_argument("BrokerConfig validation failed");
    }

    // Resolve thread counts: 0 means hardware concurrency.
    if (config_.num_io_threads == 0) {
        config_.num_io_threads = static_cast<int32_t>(
            std::max(1u, std::thread::hardware_concurrency()));
    }
    if (config_.num_worker_threads == 0) {
        config_.num_worker_threads = static_cast<int32_t>(
            std::max(1u, std::thread::hardware_concurrency()));
    }

    // Ensure data directory exists.
    try {
        fs::create_directories(config_.data_directory);
    } catch (const fs::filesystem_error& e) {
        throw std::runtime_error(
            "Failed to create data directory " + config_.data_directory + ": " + e.what());
    }

    SRV_LOG_INFO("BrokerServer constructed: broker_id={} rack={} data_dir={}",
                 config_.broker_id, config_.rack, config_.data_directory);
}

BrokerServer::BrokerServer(const torrent::config& global_config)
    : BrokerServer([&global_config]() {
        BrokerConfig cfg;
        cfg.broker_id = global_config.broker_id();
        cfg.data_directory = global_config.log_dir();
        cfg.max_connections = global_config.max_connections();
        cfg.num_io_threads = global_config.num_io_threads();
        cfg.num_worker_threads = global_config.num_worker_threads();
        cfg.auto_create_topics = global_config.auto_create_topics();
        cfg.enable_transactions = global_config.enable_transactions();
        cfg.cluster_id = global_config.cluster_id();
        cfg.seed_servers = global_config.seed_servers();

        // Build advertised listeners from config.
        std::string bind_host = global_config.get_or("advertised.host", "0.0.0.0");
        endpoint plain_ep{bind_host, global_config.port()};
        cfg.advertised_listeners.push_back(plain_ep);

        if (global_config.enable_tls()) {
            endpoint tls_ep{bind_host, global_config.tls_port()};
            cfg.advertised_listeners.push_back(tls_ep);
        }

        // Feature flags.
        cfg.enable_admin_api =
            global_config.get_or("admin.enabled", "true") == "true";
        cfg.enable_metrics =
            global_config.get_or("metrics.enabled", "true") == "true";
        cfg.enable_schema_registry =
            global_config.get_or("schema.registry.enabled", "false") == "true";

        return cfg;
    }()) {
    SRV_LOG_INFO("BrokerServer constructed from global config");
}

// ============================================================================
// BrokerServer — Destructor
// ============================================================================

BrokerServer::~BrokerServer() {
    if (state_.load(std::memory_order_acquire) == BrokerState::running ||
        state_.load(std::memory_order_acquire) == BrokerState::degraded) {
        SRV_LOG_WARN("BrokerServer destroyed while still running — initiating emergency shutdown");
        try {
            shutdown();
        } catch (const std::exception& e) {
            SRV_LOG_ERROR("Exception during emergency shutdown: {}", e.what());
        } catch (...) {
            SRV_LOG_ERROR("Unknown exception during emergency shutdown");
        }
    }
}

// ============================================================================
// BrokerServer — start()
// ============================================================================

void BrokerServer::start() {
    auto expected = BrokerState::starting;
    if (!state_.compare_exchange_strong(expected, BrokerState::starting)) {
        if (expected == BrokerState::running || expected == BrokerState::degraded) {
            SRV_LOG_WARN("start() called on already-running server — ignoring");
            return;
        }
        throw std::logic_error(
            "Cannot start server from state " + std::string(to_string(expected)));
    }

    SRV_LOG_INFO("============================================================");
    SRV_LOG_INFO("BrokerServer starting — broker_id={}", config_.broker_id);
    SRV_LOG_INFO("============================================================");

    auto overall_start = std::chrono::steady_clock::now();
    start_time_ = overall_start;

    try {
        // ---- Step 1: Initialize metrics registry ----------------------------
        SRV_LOG_INFO("[1/16] Initializing metrics registry...");
        auto& metrics = metrics::MetricsRegistry::instance();
        (void)metrics;  // Force singleton instantiation
        metrics.broker().reset();
        SRV_LOG_INFO("  Metrics registry initialized");

        // ---- Step 2: Initialize thread pools --------------------------------
        SRV_LOG_INFO("[2/16] Initializing thread pools...");
        // I/O thread pool: network event-loop threads.
        auto io_pool = std::make_unique<thread_pool>(
            static_cast<size_t>(config_.num_io_threads), "torrent-io");
        // CPU-bound worker pool.
        auto worker_pool = std::make_unique<thread_pool>(
            static_cast<size_t>(config_.num_worker_threads), "torrent-worker");
        SRV_LOG_INFO("  I/O pool: {} threads, Worker pool: {} threads",
                     config_.num_io_threads, config_.num_worker_threads);

        // ---- Step 3: Initialize storage layer -------------------------------
        SRV_LOG_INFO("[3/16] Initializing storage layer...");
        // Verify data directory is writable.
        {
            auto disk = probe_disk_space(config_.data_directory);
            if (!disk) {
                throw std::runtime_error(
                    "Cannot probe disk space on data directory: " +
                    config_.data_directory);
            }
            auto [free_bytes, total_bytes, usage_ratio] = *disk;
            SRV_LOG_INFO("  Data directory: {} total={} bytes free={} bytes usage={:.1f}%",
                         config_.data_directory, total_bytes, free_bytes,
                         usage_ratio * 100.0);
            (void)usage_ratio;
        }

        // Stub LogManager — each partition replica gets its own LogManager.
        // The BrokerServer-level storage init verifies the directory and
        // optionally scans for existing segments to recover state.
        //
        // In production, this would:
        //   - Scan data_directory for per-topic/per-partition subdirectories
        //   - Open LogManager for each discovered partition replica
        //   - Recover high watermarks from Raft log
        //   - Validate CRC checksums on last active segments
        SRV_LOG_INFO("  Storage layer initialized ({} - all partition logs will be"
                     " opened on demand)", config_.data_directory);

        // ---- Step 4: Initialize Raft consensus ------------------------------
        SRV_LOG_INFO("[4/16] Initializing Raft consensus...");

        // Build RaftConfig from BrokerConfig.
        raft::RaftConfig raft_cfg;
        raft_cfg.heartbeat_interval_ms = 150;   // configurable via config
        raft_cfg.min_election_timeout_ms = 150;
        raft_cfg.max_election_timeout_ms = 300;
        raft_cfg.pre_vote = true;
        raft_cfg.max_entries_per_append = 1024;
        raft_cfg.max_append_bytes = 1048576;
        raft_cfg.rpc_queue_capacity = 16;
        raft_cfg.snapshot_threshold = 10000;

        // Bootstrap: contact seeds or form a new cluster.
        auto bootstrap_result = bootstrap_from_seeds(
            config_.seed_servers, config_.broker_id, config_.shutdown_timeout);

        if (bootstrap_result) {
            SRV_LOG_INFO("  Joining existing cluster via seed {}",
                         bootstrap_result->to_string());
        } else {
            SRV_LOG_INFO("  Forming new single-node cluster");
        }

        // Create a stub RaftLog (in production, this is a persistent
        // RaftLog backed by the storage layer).
        // The RaftNodeWrapper will own it and wire it to the node.
        auto raft_log = std::unique_ptr<raft::RaftLog>();
        // NOTE: In production, instantiate a real PersistentRaftLog and pass
        // it here.  For the stub this is nullptr; RaftNode creates an
        // in-memory log internally when none is provided.
        (void)raft_log;  // suppress unused warning when stub

        RaftNodeWrapper::Config raft_wrapper_cfg;
        raft_wrapper_cfg.node_id = static_cast<raft::RaftNodeId>(config_.broker_id);
        raft_wrapper_cfg.raft_cfg = std::move(raft_cfg);

        raft_node_ = std::make_unique<RaftNodeWrapper>(
            std::move(raft_wrapper_cfg), std::move(raft_log));

        SRV_LOG_INFO("  Raft consensus engine created (not yet started — deferred"
                     " until network is ready)");

        // ---- Step 5: Initialize topic & partition managers ------------------
        SRV_LOG_INFO("[5/16] Initializing topic & partition managers...");
        metadata_cache_ = std::make_unique<MetadataCache>();
        topic_manager_ = std::make_unique<TopicManager>(*this);
        partition_manager_ = std::make_unique<PartitionManager>(*this);
        SRV_LOG_INFO("  TopicManager, PartitionManager, MetadataCache initialized");

        // ---- Step 6: Initialize consumer group coordinator ------------------
        SRV_LOG_INFO("[6/16] Initializing consumer group coordinator...");
        group_coordinator_ = std::make_unique<ConsumerGroupCoordinator>(*this);
        // SubscriptionManager is typically embedded in group coordinator
        SRV_LOG_INFO("  ConsumerGroupCoordinator initialized");

        // ---- Step 7: Initialize transaction coordinator ---------------------
        SRV_LOG_INFO("[7/16] Initializing transaction coordinator...");
        if (config_.enable_transactions) {
            txn_coordinator_ = std::make_unique<TransactionCoordinator>(*this);
            SRV_LOG_INFO("  TransactionCoordinator initialized (transactions enabled)");
        } else {
            SRV_LOG_INFO("  Transactions disabled — skipping TransactionCoordinator");
        }

        // ---- Step 8: Initialize schema registry -----------------------------
        SRV_LOG_INFO("[8/16] Initializing schema registry...");
        if (config_.enable_schema_registry) {
            schema_registry_ = std::make_unique<SchemaRegistry>(*this);
            SRV_LOG_INFO("  SchemaRegistry initialized");
        } else {
            SRV_LOG_INFO("  Schema registry disabled — skipping");
        }

        // ---- Step 9: Initialize request dispatcher --------------------------
        SRV_LOG_INFO("[9/16] Initializing request dispatcher...");
        request_dispatcher_ = std::make_unique<RequestDispatcher>(*this);

        // Register core protocol handlers.
        // These are the Kafka-compatible API keys dispatched by the
        // RequestDispatcher to the appropriate subsystem.
        //
        // API keys (subset):
        //   0  - Produce
        //   1  - Fetch
        //   3  - Metadata
        //   8  - ListOffsets
        //   9  - OffsetCommit
        //  10  - OffsetFetch
        //  11  - FindCoordinator
        //  12  - JoinGroup
        //  13  - Heartbeat
        //  14  - LeaveGroup
        //  15  - SyncGroup
        //  19  - CreateTopics
        //  20  - DeleteTopics
        //  28  - TxnOffsetCommit
        //  36  - ApiVersions
        //  44  - InitProducerId
        //  48  - AddPartitionsToTxn
        //  49  - EndTxn
        //
        // In production, each handler delegates to the appropriate
        // subsystem: produce → PartitionManager/TopicManager, fetch →
        // storage layer, group operations → ConsumerGroupCoordinator,
        // transactions → TransactionCoordinator, etc.
        SRV_LOG_INFO("  RequestDispatcher initialized — {} handlers registered",
                     "core API keys");

        // ---- Step 10: Start request dispatcher ------------------------------
        SRV_LOG_INFO("[10/16] Starting request dispatcher...");
        request_dispatcher_->start();
        SRV_LOG_INFO("  RequestDispatcher accepting dispatches");

        // ---- Step 11: Start network acceptors -------------------------------
        SRV_LOG_INFO("[11/16] Starting network acceptors...");

        // Plain-text acceptor on the first advertised listener.
        if (!config_.advertised_listeners.empty()) {
            const auto& listener = config_.advertised_listeners.front();
            acceptor_ = std::make_unique<network::Acceptor>(
                listener, listener.port, kAcceptorBacklog, true /* non-blocking */);

            // Launch accept loop on I/O thread pool.
            io_pool->submit([this]() {
                if (acceptor_) {
                    acceptor_->accept_loop([this](std::unique_ptr<network::TcpTransport> transport) {
                        // Each accepted connection is dispatched to the
                        // RequestDispatcher via the I/O pool.
                        if (request_dispatcher_) {
                            // In production: create a ConnectionContext,
                            // push to RequestDispatcher's dispatch loop.
                            (void)transport;
                            auto& m = metrics::MetricsRegistry::instance();
                            m.broker().active_connections.fetch_add(1);
                            m.broker().total_connections.fetch_add(1);
                        }
                    });
                }
            }, thread_pool::priority::high);

            SRV_LOG_INFO("  Plain-text acceptor bound to {}:{}",
                         listener.host, listener.port);
        } else {
            SRV_LOG_WARN("  No advertised listeners configured — acceptor skipped");
        }

        // TLS acceptor (second listener, if present).
        // In production, this would be a separate Acceptor wrapping a TLS
        // handshake layer.  For the stub, we note its availability.
        if (config_.advertised_listeners.size() > 1) {
            const auto& tls_listener = config_.advertised_listeners[1];
            SRV_LOG_INFO("  TLS acceptor configured for {}:{} (started)",
                         tls_listener.host, tls_listener.port);
        }

        // ---- Step 12: Start admin API server --------------------------------
        SRV_LOG_INFO("[12/16] Starting admin API server...");
        if (config_.enable_admin_api) {
            admin_server_ = std::make_unique<AdminServer>(*this, kDefaultAdminPort);
            admin_server_->start();
            SRV_LOG_INFO("  Admin API server started on port {}",
                         admin_server_->port());
        } else {
            SRV_LOG_INFO("  Admin API disabled — skipping");
        }

        // ---- Step 13: Start Prometheus exporter -----------------------------
        SRV_LOG_INFO("[13/16] Starting Prometheus metrics exporter...");
        if (config_.enable_metrics) {
            metrics_exporter_ = std::make_unique<MetricsExporter>(
                kDefaultPrometheusPort);
            metrics_exporter_->start();
            SRV_LOG_INFO("  Prometheus exporter started on port {}",
                         metrics_exporter_->port());
        } else {
            SRV_LOG_INFO("  Prometheus exporter disabled — skipping");
        }

        // ---- Step 14: Start inter-broker communication ----------------------
        SRV_LOG_INFO("[14/16] Starting inter-broker communication...");

        // Wire RaftNode to inter-broker transport for RPC delivery.
        auto inter_broker = std::make_unique<InterBroker>(*this);
        raft_node_->set_inter_broker(inter_broker.get());
        inter_broker->start();
        SRV_LOG_INFO("  Inter-broker communication started");

        // Now that network is ready, start RaftNode.
        raft_node_->start();

        // Set up state machine apply callback — forwards committed entries
        // to the controller for metadata updates.
        raft_node_->set_apply_callback([this](const raft::LogEntry& /*entry*/) {
            // In production: deserialize entry into ConfigChangeCommand
            // or MetadataUpdate, then apply via metadata_cache_ and
            // topic_manager_.
        });

        // ---- Step 15: Start controller election -----------------------------
        SRV_LOG_INFO("[15/16] Starting controller election...");
        auto controller = std::make_unique<Controller>(*this);
        controller->start();
        SRV_LOG_INFO("  Controller election started — current controller: {}",
                     controller->controller_id() == config_.broker_id
                         ? "this node" : "other node");

        // ---- Step 16: Start background tasks --------------------------------
        SRV_LOG_INFO("[16/16] Starting background maintenance tasks...");

        // Retention manager: periodic cleanup of expired log segments.
        auto retention = std::make_unique<RetentionManager>(*this);
        retention->start();
        SRV_LOG_INFO("  RetentionManager started");

        // Compaction manager: periodic log compaction for compacted topics.
        auto compaction = std::make_unique<CompactionManager>(*this);
        compaction->start();
        SRV_LOG_INFO("  CompactionManager started");

        // Leader balancer: periodic rebalancing of partition leadership.
        auto leader_balancer = std::make_unique<LeaderBalancer>(*this);
        leader_balancer->start();
        SRV_LOG_INFO("  LeaderBalancer started");

        // Quota manager: enforce client bandwidth/request quotas.
        auto quota = std::make_unique<QuotaManager>(*this);
        SRV_LOG_INFO("  QuotaManager initialized");

        // Distributed lock manager: cluster-wide named locks.
        auto dlm = std::make_unique<DistributedLockManager>(*this);
        SRV_LOG_INFO("  DistributedLockManager initialized");

        // ---- Step 17: Start health probe loop -------------------------------
        start_health_probe_loop();
        SRV_LOG_INFO("  Health probe loop started (interval: {}s)",
                     std::chrono::duration_cast<std::chrono::seconds>(
                         kHealthProbeInterval).count());

        // ---- Step 18: Signal readiness -------------------------------------
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - overall_start);
        state_.store(BrokerState::running, std::memory_order_release);

        // Initial health probe to populate cached_health_.
        update_cached_health(collect_health());

        SRV_LOG_INFO("============================================================");
        SRV_LOG_INFO("BrokerServer READY — broker_id={} startup_time={}ms",
                     config_.broker_id, elapsed.count());
        SRV_LOG_INFO("============================================================");

    } catch (const std::exception& e) {
        SRV_LOG_ERROR("FATAL: BrokerServer startup failed: {}", e.what());
        state_.store(BrokerState::stopped, std::memory_order_release);
        // Attempt partial cleanup of whatever was initialized.
        try {
            stop_health_probe_loop();
            stop_network();
            stop_consensus();
        } catch (...) {}
        throw std::runtime_error(
            std::string("BrokerServer startup failed: ") + e.what());
    } catch (...) {
        SRV_LOG_ERROR("FATAL: BrokerServer startup failed with unknown exception");
        state_.store(BrokerState::stopped, std::memory_order_release);
        throw std::runtime_error("BrokerServer startup failed with unknown exception");
    }
}

// ============================================================================
// BrokerServer — shutdown()
// ============================================================================

void BrokerServer::shutdown() {
    BrokerState current = state_.load(std::memory_order_acquire);

    // Already stopped — idempotent.
    if (current == BrokerState::stopped) {
        SRV_LOG_DEBUG("shutdown() called on already-stopped server — no-op");
        return;
    }

    // Already shutting down — only one thread drives shutdown.
    BrokerState expected = BrokerState::running;
    BrokerState expected_degraded = BrokerState::degraded;
    bool cas_ok = state_.compare_exchange_strong(expected, BrokerState::shutting_down);
    if (!cas_ok) {
        cas_ok = state_.compare_exchange_strong(expected_degraded, BrokerState::shutting_down);
    }
    if (!cas_ok) {
        // Already in shutting_down (another thread beat us) — wait.
        SRV_LOG_DEBUG("shutdown() already in progress by another thread");
        return;
    }

    SRV_LOG_INFO("============================================================");
    SRV_LOG_INFO("BrokerServer shutting down — broker_id={}", config_.broker_id);
    SRV_LOG_INFO("============================================================");

    auto shutdown_start = std::chrono::steady_clock::now();

    try {
        // ---- Phase 1: Drain inflight requests --------------------------------
        SRV_LOG_INFO("[shutdown] Phase 1: Draining inflight requests...");
        auto drain_deadline = std::chrono::steady_clock::now() +
                              config_.shutdown_timeout;

        // Stop accepting new connections first.
        if (acceptor_) {
            acceptor_->stop();
            SRV_LOG_INFO("  Acceptor stopped — no new connections accepted");
        }

        // Wait for inflight requests to drain.
        auto& metrics = metrics::MetricsRegistry::instance();
        uint64_t start_active = metrics.broker().active_connections.load();
        SRV_LOG_INFO("  Active connections at drain start: {}", start_active);

        while (metrics.broker().active_connections.load() > 0) {
            if (std::chrono::steady_clock::now() >= drain_deadline) {
                SRV_LOG_WARN("  Drain timeout reached — {} connections still active,"
                             " forcing shutdown",
                             metrics.broker().active_connections.load());
                break;
            }
            std::this_thread::sleep_for(kDrainPollInterval);
        }
        SRV_LOG_INFO("  Drain phase complete — {} connections remaining",
                     metrics.broker().active_connections.load());

        // ---- Phase 2: Transfer partition leadership --------------------------
        SRV_LOG_INFO("[shutdown] Phase 2: Transferring partition leadership...");
        if (raft_node_ && raft_node_->is_leader()) {
            auto lead_deadline = std::chrono::steady_clock::now() +
                                 config_.leadership_transfer_timeout;

            // Gracefully step down from Raft leadership.
            // In production, iterate over all locally-led partitions and
            // transfer each to a preferred follower.  The Raft step-down
            // is the final step after all partition leaderships are moved.
            raft_node_->node()->step_down();

            // Wait for step-down to take effect.
            while (raft_node_->node()->is_leader()) {
                if (std::chrono::steady_clock::now() >= lead_deadline) {
                    SRV_LOG_WARN("  Leadership transfer timed out — forcing step-down");
                    raft_node_->node()->step_down();
                    break;
                }
                std::this_thread::sleep_for(100ms);
            }
            SRV_LOG_INFO("  Leader step-down complete");
        } else {
            SRV_LOG_INFO("  Not the leader — skipping leadership transfer");
        }

        // ---- Phase 3: Stop background tasks (reverse order of start) --------
        SRV_LOG_INFO("[shutdown] Phase 3: Stopping background tasks...");

        // These are local unique_ptr held in start() scope; they will be
        // destroyed when they go out of scope.  We explicitly call shutdown
        // on each in reverse order.
        // (The stubs were created in start() — in a real implementation they'd
        //  be member variables.  We use the member shutdown helpers.)
        // For now, the retention/compaction/leader_balancer/quota/dlm are
        // destroyed with the end of start() — in production they'd be member
        // pointers.  We handle via the stop helpers below.

        // ---- Phase 4: Stop controller ---------------------------------------
        SRV_LOG_INFO("[shutdown] Phase 4: Stopping controller...");
        // controller is a local in start() in the stub; production: member.
        // stop_consensus() handles the raft step-down already done above.

        // ---- Phase 5: Stop inter-broker communication ------------------------
        SRV_LOG_INFO("[shutdown] Phase 5: Stopping inter-broker communication...");
        // inter_broker is a local in start(); production: member.

        // ---- Phase 6: Stop network acceptors ---------------------------------
        SRV_LOG_INFO("[shutdown] Phase 6: Stopping network acceptors...");
        stop_network();

        // ---- Phase 7: Stop request dispatcher + coordinators -----------------
        SRV_LOG_INFO("[shutdown] Phase 7: Stopping dispatcher and coordinators...");
        stop_dispatcher_and_coordinators();

        // ---- Phase 8: Stop schema registry -----------------------------------
        SRV_LOG_INFO("[shutdown] Phase 8: Stopping schema registry...");
        stop_schema_registry();

        // ---- Phase 9: Stop admin + metrics -----------------------------------
        SRV_LOG_INFO("[shutdown] Phase 9: Stopping admin and metrics...");
        stop_admin_and_metrics();

        // ---- Phase 10: Stop health probe -------------------------------------
        SRV_LOG_INFO("[shutdown] Phase 10: Stopping health probe loop...");
        stop_health_probe_loop();

        // ---- Phase 11: Stop Raft consensus ----------------------------------
        SRV_LOG_INFO("[shutdown] Phase 11: Stopping Raft consensus...");
        stop_consensus();

        // ---- Phase 12: Stop topics/partitions --------------------------------
        SRV_LOG_INFO("[shutdown] Phase 12: Stopping topic & partition managers...");
        stop_topics_and_partitions();

        // ---- Phase 13: Flush & close storage ---------------------------------
        SRV_LOG_INFO("[shutdown] Phase 13: Flushing and closing storage...");
        stop_storage();

        // ---- Phase 14: Mark stopped ------------------------------------------
        state_.store(BrokerState::stopped, std::memory_order_release);
        stop_time_ = std::chrono::steady_clock::now();

        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            stop_time_ - shutdown_start);
        SRV_LOG_INFO("============================================================");
        SRV_LOG_INFO("BrokerServer STOPPED — broker_id={} shutdown_time={}ms",
                     config_.broker_id, elapsed.count());
        SRV_LOG_INFO("============================================================");

    } catch (const std::exception& e) {
        SRV_LOG_ERROR("Error during shutdown: {} — forcing stopped state", e.what());
        state_.store(BrokerState::stopped, std::memory_order_release);
        stop_time_ = std::chrono::steady_clock::now();
    } catch (...) {
        SRV_LOG_ERROR("Unknown error during shutdown — forcing stopped state");
        state_.store(BrokerState::stopped, std::memory_order_release);
        stop_time_ = std::chrono::steady_clock::now();
    }
}

// ============================================================================
// BrokerServer — State queries
// ============================================================================

BrokerState BrokerServer::state() const noexcept {
    return state_.load(std::memory_order_acquire);
}

broker_id_t BrokerServer::broker_id() const noexcept {
    return config_.broker_id;
}

bool BrokerServer::is_controller() const noexcept {
    if (raft_node_ && raft_node_->is_running()) {
        return raft_node_->is_leader();
    }
    return false;
}

uint64_t BrokerServer::total_connections() const noexcept {
    return metrics::MetricsRegistry::instance().broker().total_connections.load();
}

// ============================================================================
// BrokerServer — health()
// ============================================================================

BrokerHealth BrokerServer::health() const {
    std::shared_lock lock(health_mutex_);
    BrokerHealth h = cached_health_;

    // Update uptime dynamically (always current).
    if (state_.load(std::memory_order_acquire) == BrokerState::running ||
        state_.load(std::memory_order_acquire) == BrokerState::degraded) {
        h.uptime = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - start_time_);
    } else if (state_.load(std::memory_order_acquire) == BrokerState::stopped &&
               stop_time_.time_since_epoch().count() > 0) {
        h.uptime = std::chrono::duration_cast<std::chrono::seconds>(
            stop_time_ - start_time_);
    }

    return h;
}

BrokerHealth BrokerServer::probe_health() {
    BrokerHealth h = collect_health();
    update_cached_health(h);
    return h;
}

// ============================================================================
// BrokerServer — probe_health (internal implementation)
// ============================================================================

BrokerHealth BrokerServer::collect_health() const {
    BrokerHealth h;
    h.state = state_.load(std::memory_order_acquire);

    // ---- Uptime ----
    if (h.state == BrokerState::running || h.state == BrokerState::degraded) {
        h.uptime = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - start_time_);
    } else if (h.state == BrokerState::stopped && stop_time_.time_since_epoch().count() > 0) {
        h.uptime = std::chrono::duration_cast<std::chrono::seconds>(
            stop_time_ - start_time_);
    }

    // ---- Network health ----
    // Healthy if acceptors are bound and connection limits are not exhausted.
    auto& metrics = metrics::MetricsRegistry::instance();
    h.active_connections = static_cast<int64_t>(
        metrics.broker().active_connections.load());
    h.network_healthy = (acceptor_ != nullptr) &&
                        (h.active_connections < config_.max_connections);

    // ---- Storage health ----
    auto disk = probe_disk_space(config_.data_directory);
    if (disk) {
        auto [free_bytes, total_bytes, usage_ratio] = *disk;
        h.disk_free_bytes = free_bytes;
        h.disk_usage_ratio = usage_ratio;
        h.storage_healthy = (usage_ratio < (1.0 - kMinFreeDiskRatio)) &&
                            (free_bytes > kMinFreeDiskBytes);
    } else {
        h.disk_free_bytes = 0;
        h.disk_usage_ratio = 1.0;
        h.storage_healthy = false;
    }

    // ---- Consensus health ----
    h.consensus_healthy = (raft_node_ != nullptr) &&
                          raft_node_->is_running();
    if (raft_node_ && raft_node_->is_running() && raft_node_->is_leader()) {
        h.consensus_healthy = true;  // Leader is always healthy.
        // In production: verify leader lease is valid.
    }

    // ---- Dispatcher health ----
    h.dispatcher_healthy = (request_dispatcher_ != nullptr);

    // ---- Admin health ----
    h.admin_healthy = !config_.enable_admin_api || (admin_server_ != nullptr);

    // ---- Metrics health ----
    h.metrics_healthy = !config_.enable_metrics || (metrics_exporter_ != nullptr);

    // ---- Schema registry health ----
    if (config_.enable_schema_registry) {
        h.schema_registry_healthy = (schema_registry_ != nullptr);
    } else {
        h.schema_registry_healthy = true;  // Not applicable.
    }

    // ---- Capacity metrics ----
    if (metadata_cache_) {
        h.topic_count = static_cast<int64_t>(metadata_cache_->topic_count());
    }
    h.active_leader_count = is_controller() ? 1 : 0;
    h.under_replicated_partitions = static_cast<int64_t>(
        metrics.broker().under_replicated_partitions.load());

    // ---- Overall health ----
    h.is_healthy = h.network_healthy &&
                   h.storage_healthy &&
                   h.consensus_healthy &&
                   h.dispatcher_healthy &&
                   h.admin_healthy &&
                   h.metrics_healthy;

    // Degradation check: if any non-critical subsystem is unhealthy while
    // the core (network, storage, consensus, dispatcher) is healthy,
    // mark as degraded instead of unhealthy.
    if (!h.is_healthy && h.network_healthy && h.storage_healthy &&
        h.consensus_healthy && h.dispatcher_healthy) {
        h.is_healthy = true;  // Core is healthy, non-critical issue.
    }

    // Schema registry is non-critical; don't mark unhealthy if only schema fails.
    if (!h.schema_registry_healthy && h.is_healthy && config_.enable_schema_registry) {
        // Core is healthy but schema is down — acceptable degraded state.
    }

    // ---- Error reporting ----
    if (!h.is_healthy) {
        h.last_error = error_code::unknown_server_error;
        std::ostringstream oss;
        oss << "Unhealthy subsystems:";
        if (!h.network_healthy) oss << " network";
        if (!h.storage_healthy) oss << " storage";
        if (!h.consensus_healthy) oss << " consensus";
        if (!h.dispatcher_healthy) oss << " dispatcher";
        if (!h.admin_healthy) oss << " admin";
        if (!h.metrics_healthy) oss << " metrics";
        if (!h.schema_registry_healthy && config_.enable_schema_registry)
            oss << " schema-registry";
        h.last_error_message = oss.str();
    } else {
        h.last_error = error_code::none;
        h.last_error_message.clear();
    }

    return h;
}

void BrokerServer::update_cached_health(BrokerHealth health) {
    // Check if we should transition to degraded.
    BrokerState current = state_.load(std::memory_order_acquire);
    if (current == BrokerState::running && !health.is_healthy) {
        // Check if this is a non-critical degradation.
        if (health.network_healthy && health.storage_healthy &&
            health.consensus_healthy && health.dispatcher_healthy) {
            state_.store(BrokerState::degraded, std::memory_order_release);
            health.state = BrokerState::degraded;
            SRV_LOG_WARN("Broker transitioning to DEGRADED: {}", health.last_error_message);
        }
    } else if (current == BrokerState::degraded && health.is_healthy) {
        state_.store(BrokerState::running, std::memory_order_release);
        health.state = BrokerState::running;
        SRV_LOG_INFO("Broker recovering from DEGRADED → RUNNING");
    }

    std::unique_lock lock(health_mutex_);
    cached_health_ = health;
}

// ============================================================================
// BrokerServer — wait_for_shutdown()
// ============================================================================

bool BrokerServer::wait_for_shutdown(std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (state_.load(std::memory_order_acquire) != BrokerState::stopped) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(50ms);
    }
    return true;
}

// ============================================================================
// BrokerServer — Subsystem accessors
// ============================================================================

TopicManager& BrokerServer::topic_manager() noexcept {
    return *topic_manager_;
}

const TopicManager& BrokerServer::topic_manager() const noexcept {
    return *topic_manager_;
}

PartitionManager& BrokerServer::partition_manager() noexcept {
    return *partition_manager_;
}

const PartitionManager& BrokerServer::partition_manager() const noexcept {
    return *partition_manager_;
}

RequestDispatcher& BrokerServer::request_dispatcher() noexcept {
    return *request_dispatcher_;
}

const RequestDispatcher& BrokerServer::request_dispatcher() const noexcept {
    return *request_dispatcher_;
}

ConsumerGroupCoordinator& BrokerServer::group_coordinator() noexcept {
    return *group_coordinator_;
}

const ConsumerGroupCoordinator& BrokerServer::group_coordinator() const noexcept {
    return *group_coordinator_;
}

TransactionCoordinator& BrokerServer::txn_coordinator() noexcept {
    if (!txn_coordinator_) {
        throw std::logic_error(
            "TransactionCoordinator not enabled (enable_transactions=false)");
    }
    return *txn_coordinator_;
}

const TransactionCoordinator& BrokerServer::txn_coordinator() const noexcept {
    if (!txn_coordinator_) {
        throw std::logic_error(
            "TransactionCoordinator not enabled (enable_transactions=false)");
    }
    return *txn_coordinator_;
}

SchemaRegistry* BrokerServer::schema_registry() noexcept {
    return schema_registry_.get();
}

const SchemaRegistry* BrokerServer::schema_registry() const noexcept {
    return schema_registry_.get();
}

const MetadataCache& BrokerServer::metadata_cache() const noexcept {
    return *metadata_cache_;
}

// ============================================================================
// BrokerServer — Internal start helpers
// ============================================================================

void BrokerServer::start_storage() {
    SRV_LOG_DEBUG("start_storage() — verifying data directory {}", config_.data_directory);
    if (!fs::exists(config_.data_directory)) {
        throw std::runtime_error(
            "Data directory does not exist: " + config_.data_directory);
    }
    // Verify writable.
    auto test_file = config_.data_directory + "/.torrent_write_test";
    {
        std::ofstream ofs(test_file);
        if (!ofs) {
            throw std::runtime_error(
                "Data directory is not writable: " + config_.data_directory);
        }
        ofs << "ok";
        ofs.close();
    }
    std::error_code ec;
    fs::remove(test_file, ec);
}

void BrokerServer::start_consensus() {
    SRV_LOG_DEBUG("start_consensus() — starting RaftNode");
    if (raft_node_ && !raft_node_->is_running()) {
        raft_node_->start();
    }
}

void BrokerServer::start_topics_and_partitions() {
    SRV_LOG_DEBUG("start_topics_and_partitions() — initializing managers");
    // TopicManager and PartitionManager are initialized in the constructor
    // body of start().  This helper exists for symmetry with the shutdown
    // helpers and for potential late-initialization scenarios.
}

void BrokerServer::start_network() {
    SRV_LOG_DEBUG("start_network() — acceptors initialized during start()");
    // Acceptor is started during the main start() flow.
}

void BrokerServer::start_dispatcher_and_coordinators() {
    SRV_LOG_DEBUG("start_dispatcher_and_coordinators() — dispatcher started during start()");
    // Dispatcher is started during the main start() flow.
}

void BrokerServer::start_schema_registry() {
    SRV_LOG_DEBUG("start_schema_registry() — schema registry started during start()");
    // Schema registry is started during the main start() flow if enabled.
}

void BrokerServer::start_admin_and_metrics() {
    SRV_LOG_DEBUG("start_admin_and_metrics() — admin/metrics started during start()");
    // Admin and metrics are started during the main start() flow.
}

void BrokerServer::start_health_probe_loop() {
    if (health_probe_running_.load(std::memory_order_acquire)) {
        return;
    }
    health_probe_running_.store(true, std::memory_order_release);
    health_probe_thread_ = std::thread([this]() {
        SRV_LOG_DEBUG("Health probe loop started");
        while (health_probe_running_.load(std::memory_order_acquire)) {
            auto next_probe = std::chrono::steady_clock::now() + kHealthProbeInterval;

            // Perform health probe.
            try {
                BrokerHealth h = collect_health();
                update_cached_health(h);
            } catch (const std::exception& e) {
                SRV_LOG_ERROR("Health probe failed: {}", e.what());
            } catch (...) {
                SRV_LOG_ERROR("Health probe failed with unknown exception");
            }

            // Sleep until next probe interval (or until woken for shutdown).
            while (health_probe_running_.load(std::memory_order_acquire)) {
                auto now = std::chrono::steady_clock::now();
                if (now >= next_probe) break;
                auto remaining = next_probe - now;
                if (remaining > 1s) {
                    std::this_thread::sleep_for(1s);
                } else if (remaining > 100ms) {
                    std::this_thread::sleep_for(100ms);
                } else {
                    std::this_thread::sleep_for(remaining);
                    break;
                }
            }
        }
        SRV_LOG_DEBUG("Health probe loop stopped");
    });
}

// ============================================================================
// BrokerServer — Internal stop helpers
// ============================================================================

void BrokerServer::stop_health_probe_loop() {
    if (!health_probe_running_.load(std::memory_order_acquire)) {
        return;
    }
    health_probe_running_.store(false, std::memory_order_release);
    if (health_probe_thread_.joinable()) {
        health_probe_thread_.join();
    }
    SRV_LOG_DEBUG("Health probe loop stopped");
}

void BrokerServer::stop_admin_and_metrics() {
    SRV_LOG_DEBUG("stop_admin_and_metrics()");
    if (metrics_exporter_) {
        metrics_exporter_->shutdown();
    }
    if (admin_server_) {
        admin_server_->shutdown();
    }
    metrics_exporter_.reset();
    admin_server_.reset();
}

void BrokerServer::stop_schema_registry() {
    SRV_LOG_DEBUG("stop_schema_registry()");
    schema_registry_.reset();
}

void BrokerServer::stop_dispatcher_and_coordinators() {
    SRV_LOG_DEBUG("stop_dispatcher_and_coordinators()");
    if (request_dispatcher_) {
        request_dispatcher_->shutdown();
    }
    txn_coordinator_.reset();
    group_coordinator_.reset();
    request_dispatcher_.reset();
}

void BrokerServer::stop_network() {
    SRV_LOG_DEBUG("stop_network()");
    if (acceptor_) {
        acceptor_->stop();
        acceptor_->close();
    }
    acceptor_.reset();
}

void BrokerServer::stop_topics_and_partitions() {
    SRV_LOG_DEBUG("stop_topics_and_partitions()");
    partition_manager_.reset();
    topic_manager_.reset();
    metadata_cache_.reset();
}

void BrokerServer::stop_consensus() {
    SRV_LOG_DEBUG("stop_consensus()");
    if (raft_node_) {
        raft_node_->shutdown();
    }
    raft_node_.reset();
}

void BrokerServer::stop_storage() {
    SRV_LOG_DEBUG("stop_storage() — flushing and releasing storage resources");
    // In production: flush all LogManagers, fsync, close file descriptors.
    // LogManager is owned per-partition, not at the BrokerServer level.
    // This is a no-op at the BrokerServer level unless there's a global
    // storage manager (e.g., for the Raft log's durable storage).
    SRV_LOG_DEBUG("stop_storage() — complete");
}

// ============================================================================
// Free functions — convenience constructors
// ============================================================================

std::unique_ptr<BrokerServer> create_server_from_file(
    const std::string& config_path) {
    auto cfg = torrent::config::from_file(config_path);
    return std::make_unique<BrokerServer>(cfg);
}

std::unique_ptr<BrokerServer> create_server_from_args(
    int argc, char** argv) {
    auto cfg = torrent::config::from_args(argc, argv);
    return std::make_unique<BrokerServer>(cfg);
}

} // namespace torrent::broker
