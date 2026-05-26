/**
 * torrent-mq — TorrentClient: High-Level C++ Client SDK
 *
 * The TorrentClient is the primary entry point for applications using
 * torrent-mq. It provides a unified API for producing, consuming, and
 * administering the cluster. Under the hood, it manages connections to
 * the cluster, metadata discovery, leader routing, and provides
 * thread-safe access to producer, consumer, and admin sub-clients.
 *
 * Architecture Overview:
 *   ┌──────────────────────────────────────────────┐
 *   │              TorrentClient                    │
 *   │  ┌──────────┐ ┌──────────┐ ┌──────────────┐  │
 *   │  │Producer  │ │Consumer  │ │ AdminClient  │  │
 *   │  │Client    │ │Client    │ │              │  │
 *   │  └────┬─────┘ └────┬─────┘ └──────┬───────┘  │
 *   │       │             │              │          │
 *   │  ┌────┴─────────────┴──────────────┴───────┐  │
 *   │  │        Connection Manager               │  │
 *   │  │  (bootstrap, metadata, leader routing)  │  │
 *   │  └────────────────────┬───────────────────┘  │
 *   │                       │                      │
 *   │  ┌────────────────────┴───────────────────┐  │
 *   │  │        Dispatch Loop / Thread Pool      │  │
 *   │  └────────────────────────────────────────┘  │
 *   └──────────────────────────────────────────────┘
 *
 * Connection Lifecycle:
 *   1. Client connects to bootstrap servers.
 *   2. Metadata request discovers all brokers and the controller.
 *   3. Connections are established to each broker (lazily or eagerly).
 *   4. Leader discovery routes produce/fetch requests to partition leaders.
 *   5. On connection failure or leader change, automatic reconnection
 *      with backoff.
 *
 * Key Features:
 *   - Unified factory for ProducerClient, ConsumerClient, AdminClient
 *   - Automatic cluster discovery and leader routing
 *   - Configurable retry, timeout, and backoff policies
 *   - Thread-safe with internal dispatch loop for I/O
 *   - Graceful shutdown with resource cleanup
 *
 * Thread Safety:
 *   All public methods of TorrentClient are thread-safe. The internal
 *   dispatch loop runs on a dedicated thread. Sub-clients (producer,
 *   consumer, admin) have their own threading models and are
 *   independently thread-safe.
 *
 * Dependencies:
 *   - ConnectionPool for broker connections
 *   - MetadataClient for cluster metadata
 *   - ProducerClient, ConsumerClient, AdminClient
 *   - SASL/TLS for authentication and encryption
 */

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

// ============================================================================
// Forward declarations (sub-client types defined in their own files)
// ============================================================================

namespace torrent::client {

class ProducerClient;
struct ProducerConfig;
struct ProducerRecord;
struct RecordMetadata;
using DeliveryCallback = std::function<void(const RecordMetadata&, const std::exception*)>;

class ConsumerClient;
struct ConsumerConfig;
struct ConsumerRecord;
class RebalanceListener;

class AdminClient;
struct AdminResult;
struct TopicConfig;
struct TopicDescription;
struct ConfigEntry;
struct ConsumerGroupSummary;
struct ConsumerGroupDescription;
struct AclBinding;
struct AclEntry;

} // namespace torrent::client

// ============================================================================
// TorrentClient Configuration
// ============================================================================

namespace torrent::client {

struct TorrentConfig {
    /// Comma-separated list of bootstrap servers ("host:port,host:port").
    std::string bootstrap_servers = "localhost:9092";

    /// Client identifier sent to the broker for logging/metrics.
    std::string client_id = "torrent-client";

    // --- Connection ---

    /// Maximum number of broker connections in the pool.
    int32_t max_connections = 10;

    /// Connection timeout for initial TCP handshake (ms).
    int32_t connection_timeout_ms = 10000;

    /// Socket timeout for read/write operations (ms).
    int32_t socket_timeout_ms = 60000;

    /// Number of I/O threads for the dispatch loop.
    int32_t io_threads = 1;

    // --- Metadata ---

    /// Interval between metadata refreshes (ms).
    int32_t metadata_refresh_interval_ms = 300000; // 5 min

    /// Maximum age of metadata before forcing a refresh (ms).
    int32_t metadata_max_age_ms = 600000; // 10 min

    // --- Retry ---

    /// Maximum number of retries for transient errors.
    int32_t max_retries = 3;

    /// Base backoff between retries (ms), doubled each attempt.
    int32_t retry_backoff_ms = 100;

    /// Maximum backoff between retries (ms).
    int32_t retry_backoff_max_ms = 10000;

    // --- Security ---

    /// Security protocol: "plaintext", "ssl", "sasl_plaintext", "sasl_ssl".
    std::string security_protocol = "plaintext";

    /// SASL mechanism: "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI".
    std::string sasl_mechanism;

    /// SASL username (if SASL is enabled).
    std::string sasl_username;

    /// SASL password (if SASL is enabled).
    std::string sasl_password;

    /// Path to CA certificate bundle for TLS verification.
    std::string ssl_ca_location;

    /// Path to client certificate for mTLS.
    std::string ssl_certificate_location;

    /// Path to client private key for mTLS.
    std::string ssl_key_location;

    /// Hostname verification policy for TLS.
    bool ssl_endpoint_identification = true;

    // --- Default Producer Config ---
    ProducerConfig producer_defaults;

    // --- Default Consumer Config ---
    ConsumerConfig consumer_defaults;
};

} // namespace torrent::client

// ============================================================================
// Cluster Metadata (Internal)
// ============================================================================

namespace torrent::client {

/// Represents the known state of the cluster.
struct ClusterMetadata {
    std::string cluster_id;
    int32_t     controller_id = -1;

    struct BrokerInfo {
        int32_t     id;
        std::string host;
        int32_t     port;
        std::string rack;   // optional rack for rack-aware assignment
    };
    std::vector<BrokerInfo> brokers;

    struct TopicInfo {
        std::string name;
        uint64_t    topic_id;
        int32_t     num_partitions;
        int16_t     replication_factor;
        bool        is_internal;

        struct PartitionInfo {
            int32_t              id;
            int32_t              leader_id;
            std::vector<int32_t> replica_ids;
            std::vector<int32_t> isr_ids;
        };
        std::vector<PartitionInfo> partitions;
    };
    std::vector<TopicInfo> topics;

    /// When this metadata snapshot was obtained (ms since epoch).
    int64_t fetch_time_ms = 0;
};

} // namespace torrent::client

// ============================================================================
// TorrentClient — Public Interface
// ============================================================================

namespace torrent::client {

class TorrentClient {
public:
    // ---- Construction -------------------------------------------------------

    /**
     * Create a TorrentClient with the given configuration.
     * The client does NOT connect automatically — call connect() to
     * establish connections to the cluster.
     */
    explicit TorrentClient(const TorrentConfig& config);
    ~TorrentClient();

    // Non-copyable, non-movable (shared ownership via shared_ptr)
    TorrentClient(const TorrentClient&) = delete;
    TorrentClient& operator=(const TorrentClient&) = delete;
    TorrentClient(TorrentClient&&) = delete;
    TorrentClient& operator=(TorrentClient&&) = delete;

    // ---- Connection Management ----------------------------------------------

    /**
     * Connect to the cluster using the configured bootstrap servers.
     * Discovers all brokers, the controller, and initial topic metadata.
     *
     * Blocks until the initial metadata is fetched or the connection
     * timeout is reached.
     *
     * @return true if connected and metadata discovered successfully.
     */
    bool connect();

    /**
     * Disconnect from the cluster gracefully. Closes all sub-clients,
     * connections, and stops the dispatch loop.
     */
    void close();

    /**
     * Check if the client is currently connected to the cluster.
     */
    [[nodiscard]] bool is_connected() const noexcept;

    // ---- Producer API -------------------------------------------------------

    /**
     * Create a new producer instance with default configuration.
     * The producer is backed by this client's connection pool.
     *
     * @return A shared pointer to a ProducerClient instance.
     */
    std::shared_ptr<ProducerClient> create_producer();

    /**
     * Create a new producer instance with custom configuration.
     *
     * @param config  Producer-specific configuration overrides.
     * @return A shared pointer to a ProducerClient instance.
     */
    std::shared_ptr<ProducerClient> create_producer(const ProducerConfig& config);

    /**
     * Convenience: send a single record using an ephemeral producer.
     *
     * @param record    The record to send.
     * @param callback  Delivery callback.
     * @return true if the record was accepted.
     */
    bool send(const ProducerRecord& record, DeliveryCallback callback = nullptr);

    /**
     * Convenience: send a simple key-value record.
     */
    bool send(const std::string& topic, const std::string& key,
              const std::string& value, DeliveryCallback callback = nullptr);

    /**
     * Convenience: send a value-only record (null key).
     */
    bool send(const std::string& topic, const std::string& value,
              DeliveryCallback callback = nullptr);

    /**
     * Flush all pending producer records across all producers.
     */
    void flush(int32_t timeout_ms = -1);

    // ---- Consumer API -------------------------------------------------------

    /**
     * Create a new consumer instance with default configuration.
     *
     * @param group_id  Consumer group identifier.
     * @return A shared pointer to a ConsumerClient instance.
     */
    std::shared_ptr<ConsumerClient> create_consumer(const std::string& group_id);

    /**
     * Create a new consumer instance with custom configuration.
     */
    std::shared_ptr<ConsumerClient> create_consumer(const ConsumerConfig& config);

    /**
     * Create a consumer, subscribe to topics, and start consuming with a
     * callback-based API. This is a simplified polling loop that runs
     * on the dispatch thread.
     *
     * @param config    Consumer configuration.
     * @param topics    Topics to subscribe to.
     * @param handler   Called for each batch of records.
     * @param listener  Optional rebalance listener.
     * @return A shared pointer to the ConsumerClient.
     */
    std::shared_ptr<ConsumerClient> subscribe_and_consume(
        const ConsumerConfig& config,
        const std::vector<std::string>& topics,
        std::function<void(const std::vector<ConsumerRecord>&)> handler,
        std::shared_ptr<RebalanceListener> listener = nullptr);

    // ---- Admin API ----------------------------------------------------------

    /**
     * Get the AdminClient associated with this TorrentClient.
     * The AdminClient is a singleton per TorrentClient instance.
     */
    std::shared_ptr<AdminClient> admin();

    /**
     * Convenience: create a topic.
     */
    AdminResult create_topic(const std::string& name,
                              int32_t num_partitions = 1,
                              int16_t replication_factor = 3,
                              const std::map<std::string, std::string>& configs = {});

    /**
     * Convenience: delete a topic.
     */
    AdminResult delete_topic(const std::string& name);

    /**
     * Convenience: list all topics.
     */
    std::vector<std::string> list_topics(bool include_internal = false);

    /**
     * Convenience: describe topic(s).
     */
    std::vector<TopicDescription> describe_topics(
        const std::vector<std::string>& topic_names = {});

    // ---- Cluster Metadata ---------------------------------------------------

    /**
     * Get the cached cluster metadata. May trigger a refresh if stale.
     */
    [[nodiscard]] ClusterMetadata cluster_metadata() const noexcept;

    /**
     * Force a metadata refresh from the cluster.
     */
    void refresh_metadata();

    /**
     * Get the endpoint (host:port) for a specific broker.
     */
    [[nodiscard]] std::optional<std::string> broker_endpoint(int32_t broker_id) const noexcept;

    /**
     * Get the leader broker ID for a specific topic-partition.
     */
    [[nodiscard]] std::optional<int32_t> leader_for(const std::string& topic,
                                                      int32_t partition) const noexcept;

    // ---- Status -------------------------------------------------------------

    /**
     * Get the number of active broker connections.
     */
    [[nodiscard]] int32_t active_connections() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace torrent::client

// ============================================================================
// Implementation
// ============================================================================

namespace torrent::client {

struct TorrentClient::Impl {
    TorrentConfig                                           config;
    std::shared_ptr<spdlog::logger>                          logger;

    // --- State ---
    std::atomic<bool>                                        connected{false};
    std::atomic<bool>                                        running{false};
    std::atomic<bool>                                        closed{false};

    // --- Cluster ---
    ClusterMetadata                                          metadata;
    mutable std::mutex                                       metadata_mutex;
    std::unique_ptr<std::thread>                             dispatch_thread;
    std::unique_ptr<std::thread>                             metadata_refresh_thread;

    // --- Sub-clients ---
    std::shared_ptr<ProducerClient>                          default_producer;
    std::shared_ptr<AdminClient>                             admin_client;
    std::vector<std::shared_ptr<ConsumerClient>>             consumers;
    std::mutex                                               consumers_mutex;

    // --- Synchronization ---
    std::mutex                                               state_mutex;
    std::condition_variable                                  cv;
    std::promise<bool>                                       connect_promise;
    std::future<bool>                                        connect_future;

    // --- Thread pool / connections ---
    // In production: ConnectionPool, I/O event loop, etc.
    int32_t                                                  active_conn_count = 0;

    explicit Impl(const TorrentConfig& cfg) : config(cfg) {
        logger = spdlog::get("torrent_client");
        if (!logger) {
            logger = spdlog::stdout_color_mt("torrent_client");
            logger->set_level(spdlog::level::info);
        }

        // Apply producer defaults that inherit from the top-level config
        if (config.producer_defaults.bootstrap_servers.empty()) {
            config.producer_defaults.bootstrap_servers = config.bootstrap_servers;
        }
        if (config.producer_defaults.client_id.empty()) {
            config.producer_defaults.client_id = config.client_id + "-producer";
        }

        // Apply consumer defaults
        if (config.consumer_defaults.bootstrap_servers.empty()) {
            config.consumer_defaults.bootstrap_servers = config.bootstrap_servers;
        }
        if (config.consumer_defaults.client_id.empty()) {
            config.consumer_defaults.client_id = config.client_id + "-consumer";
        }

        connect_future = connect_promise.get_future();
    }

    void dispatch_loop() {
        logger->info("TorrentClient: dispatch loop started");

        while (running.load(std::memory_order_acquire)) {
            // In production:
            //   - Poll for I/O events on broker connections
            //   - Dispatch incoming responses to callbacks
            //   - Handle timeouts
            //   - Manage connection health

            // Stub: just sleep briefly
            std::unique_lock<std::mutex> lock(state_mutex);
            cv.wait_for(lock, std::chrono::milliseconds(100), [this] {
                return !running.load(std::memory_order_acquire);
            });
        }

        logger->info("TorrentClient: dispatch loop exiting");
    }

    void metadata_refresh_loop() {
        logger->info("TorrentClient: metadata refresh loop started, interval={}ms",
                     config.metadata_refresh_interval_ms);

        while (running.load(std::memory_order_acquire)) {
            {
                std::unique_lock<std::mutex> lock(state_mutex);
                cv.wait_for(lock,
                            std::chrono::milliseconds(config.metadata_refresh_interval_ms),
                            [this] {
                                return !running.load(std::memory_order_acquire);
                            });
            }

            if (!running.load(std::memory_order_acquire)) break;

            logger->debug("TorrentClient: refreshing metadata");
            refresh_metadata_internal();
        }

        logger->info("TorrentClient: metadata refresh loop exiting");
    }

    void refresh_metadata_internal() {
        // In production: send Metadata RPC to any broker
        // Stub: populate simulated metadata
        std::lock_guard<std::mutex> lock(metadata_mutex);

        metadata.cluster_id = "torrent-mq-prod-01";
        metadata.controller_id = 1;

        // Simulated brokers
        metadata.brokers.clear();
        metadata.brokers.push_back({1, "broker1.local", 9092, "rack-a"});
        metadata.brokers.push_back({2, "broker2.local", 9092, "rack-b"});
        metadata.brokers.push_back({3, "broker3.local", 9092, "rack-c"});

        // Simulated topics
        metadata.topics.clear();
        {
            ClusterMetadata::TopicInfo ti;
            ti.name = "orders";
            ti.topic_id = 1001;
            ti.num_partitions = 8;
            ti.replication_factor = 3;
            for (int32_t p = 0; p < 8; ++p) {
                ClusterMetadata::TopicInfo::PartitionInfo pi;
                pi.id = p;
                pi.leader_id = (p % 3) + 1;
                pi.replica_ids = {1, 2, 3};
                pi.isr_ids = {1, 2, 3};
                ti.partitions.push_back(pi);
            }
            metadata.topics.push_back(ti);
        }
        {
            ClusterMetadata::TopicInfo ti;
            ti.name = "payments";
            ti.topic_id = 1002;
            ti.num_partitions = 6;
            ti.replication_factor = 3;
            for (int32_t p = 0; p < 6; ++p) {
                ClusterMetadata::TopicInfo::PartitionInfo pi;
                pi.id = p;
                pi.leader_id = (p % 3) + 1;
                pi.replica_ids = {1, 2, 3};
                pi.isr_ids = {1, 2, 3};
                ti.partitions.push_back(pi);
            }
            metadata.topics.push_back(ti);
        }

        metadata.fetch_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        logger->debug("TorrentClient: metadata refreshed — {} brokers, {} topics",
                      metadata.brokers.size(), metadata.topics.size());
    }

    void connect_all_brokers() {
        // In production: establish TCP connections to each broker
        active_conn_count = static_cast<int32_t>(metadata.brokers.size());
        logger->info("TorrentClient: connected to {} brokers", active_conn_count);
    }
};

// ---- Construction / Destruction ---------------------------------------------

TorrentClient::TorrentClient(const TorrentConfig& config)
    : impl_(std::make_unique<Impl>(config)) {
    impl_->logger->info("TorrentClient created: bootstrap={} client_id={}",
                        config.bootstrap_servers, config.client_id);
}

TorrentClient::~TorrentClient() {
    try {
        close();
    } catch (...) {
        // Swallow exceptions in destructor
    }
}

// ---- Connection Management --------------------------------------------------

bool TorrentClient::connect() {
    std::lock_guard<std::mutex> lock(impl_->state_mutex);

    if (impl_->connected.load(std::memory_order_acquire)) {
        impl_->logger->debug("TorrentClient::connect: already connected");
        return true;
    }

    impl_->logger->info("TorrentClient::connect: connecting to {}",
                        impl_->config.bootstrap_servers);

    // Start the dispatch loop (I/O thread)
    impl_->running.store(true, std::memory_order_release);
    impl_->dispatch_thread = std::make_unique<std::thread>(
        &Impl::dispatch_loop, impl_.get());

    // Fetch initial metadata
    impl_->refresh_metadata_internal();

    // Connect to all discovered brokers
    impl_->connect_all_brokers();

    // Start the metadata refresh loop
    impl_->metadata_refresh_thread = std::make_unique<std::thread>(
        &Impl::metadata_refresh_loop, impl_.get());

    impl_->connected.store(true, std::memory_order_release);
    impl_->logger->info("TorrentClient: connected successfully");

    return true;
}

void TorrentClient::close() {
    if (impl_->closed.exchange(true, std::memory_order_acq_rel)) {
        return; // Already closed
    }

    impl_->logger->info("TorrentClient::close: shutting down");

    // Stop dispatch loops
    impl_->running.store(false, std::memory_order_release);
    impl_->cv.notify_all();

    if (impl_->metadata_refresh_thread && impl_->metadata_refresh_thread->joinable()) {
        impl_->metadata_refresh_thread->join();
    }

    if (impl_->dispatch_thread && impl_->dispatch_thread->joinable()) {
        impl_->dispatch_thread->join();
    }

    // Close sub-clients
    {
        std::lock_guard<std::mutex> lock(impl_->consumers_mutex);
        for (auto& consumer : impl_->consumers) {
            try {
                consumer->close();
            } catch (const std::exception& e) {
                impl_->logger->error("Error closing consumer: {}", e.what());
            }
        }
        impl_->consumers.clear();
    }

    if (impl_->default_producer) {
        impl_->default_producer->close();
        impl_->default_producer.reset();
    }

    if (impl_->admin_client) {
        impl_->admin_client->close();
        impl_->admin_client.reset();
    }

    impl_->connected.store(false, std::memory_order_release);
    impl_->logger->info("TorrentClient::close: shutdown complete");
}

bool TorrentClient::is_connected() const noexcept {
    return impl_->connected.load(std::memory_order_acquire);
}

// ---- Producer API -----------------------------------------------------------

std::shared_ptr<ProducerClient> TorrentClient::create_producer() {
    return create_producer(impl_->config.producer_defaults);
}

std::shared_ptr<ProducerClient> TorrentClient::create_producer(
    const ProducerConfig& config) {
    impl_->logger->info("TorrentClient::create_producer: client_id={}", config.client_id);
    auto producer = std::make_shared<ProducerClient>(config);
    return producer;
}

bool TorrentClient::send(const ProducerRecord& record, DeliveryCallback callback) {
    if (!impl_->default_producer) {
        impl_->default_producer = create_producer();
    }
    return impl_->default_producer->send(record, std::move(callback));
}

bool TorrentClient::send(const std::string& topic, const std::string& key,
                          const std::string& value, DeliveryCallback callback) {
    if (!impl_->default_producer) {
        impl_->default_producer = create_producer();
    }
    return impl_->default_producer->send(topic, key, value, std::move(callback));
}

bool TorrentClient::send(const std::string& topic, const std::string& value,
                          DeliveryCallback callback) {
    if (!impl_->default_producer) {
        impl_->default_producer = create_producer();
    }
    return impl_->default_producer->send(topic, value, std::move(callback));
}

void TorrentClient::flush(int32_t timeout_ms) {
    if (impl_->default_producer) {
        impl_->default_producer->flush(timeout_ms);
    }
}

// ---- Consumer API -----------------------------------------------------------

std::shared_ptr<ConsumerClient> TorrentClient::create_consumer(
    const std::string& group_id) {
    ConsumerConfig config = impl_->config.consumer_defaults;
    config.group_id = group_id;
    return create_consumer(config);
}

std::shared_ptr<ConsumerClient> TorrentClient::create_consumer(
    const ConsumerConfig& config) {
    impl_->logger->info("TorrentClient::create_consumer: group={}", config.group_id);
    auto consumer = std::make_shared<ConsumerClient>(config);

    {
        std::lock_guard<std::mutex> lock(impl_->consumers_mutex);
        impl_->consumers.push_back(consumer);
    }

    return consumer;
}

std::shared_ptr<ConsumerClient> TorrentClient::subscribe_and_consume(
    const ConsumerConfig& config,
    const std::vector<std::string>& topics,
    std::function<void(const std::vector<ConsumerRecord>&)> handler,
    std::shared_ptr<RebalanceListener> listener) {
    auto consumer = create_consumer(config);
    consumer->subscribe(topics, std::move(listener));

    // Launch a background poll loop on the dispatch thread
    // In production: this would be submitted to a thread pool
    impl_->logger->info("TorrentClient::subscribe_and_consume: starting poll loop "
                        "for group={} with {} topics",
                        config.group_id, topics.size());

    // (Stub: the caller would typically run their own poll loop)

    return consumer;
}

// ---- Admin API --------------------------------------------------------------

std::shared_ptr<AdminClient> TorrentClient::admin() {
    if (!impl_->admin_client) {
        impl_->admin_client = std::make_shared<AdminClient>(
            impl_->config.bootstrap_servers,
            impl_->config.socket_timeout_ms,
            impl_->config.client_id + "-admin");
        impl_->admin_client->connect();
    }
    return impl_->admin_client;
}

AdminResult TorrentClient::create_topic(const std::string& name,
                                          int32_t num_partitions,
                                          int16_t replication_factor,
                                          const std::map<std::string, std::string>& configs) {
    TopicConfig tc;
    tc.name = name;
    tc.num_partitions = num_partitions;
    tc.replication_factor = replication_factor;
    for (const auto& [k, v] : configs) {
        ConfigEntry entry;
        entry.name = k;
        entry.value = v;
        tc.configs.push_back(entry);
    }
    return admin()->create_topic(tc);
}

AdminResult TorrentClient::delete_topic(const std::string& name) {
    return admin()->delete_topic(name);
}

std::vector<std::string> TorrentClient::list_topics(bool include_internal) {
    return admin()->list_topics(include_internal);
}

std::vector<TopicDescription> TorrentClient::describe_topics(
    const std::vector<std::string>& topic_names) {
    return admin()->describe_topics(topic_names);
}

// ---- Cluster Metadata -------------------------------------------------------

ClusterMetadata TorrentClient::cluster_metadata() const noexcept {
    std::lock_guard<std::mutex> lock(impl_->metadata_mutex);
    return impl_->metadata;
}

void TorrentClient::refresh_metadata() {
    impl_->refresh_metadata_internal();
}

std::optional<std::string> TorrentClient::broker_endpoint(
    int32_t broker_id) const noexcept {
    std::lock_guard<std::mutex> lock(impl_->metadata_mutex);

    for (const auto& broker : impl_->metadata.brokers) {
        if (broker.id == broker_id) {
            return broker.host + ":" + std::to_string(broker.port);
        }
    }
    return std::nullopt;
}

std::optional<int32_t> TorrentClient::leader_for(
    const std::string& topic, int32_t partition) const noexcept {
    std::lock_guard<std::mutex> lock(impl_->metadata_mutex);

    for (const auto& ti : impl_->metadata.topics) {
        if (ti.name == topic) {
            for (const auto& pi : ti.partitions) {
                if (pi.id == partition) {
                    return pi.leader_id;
                }
            }
        }
    }
    return std::nullopt;
}

int32_t TorrentClient::active_connections() const noexcept {
    return impl_->active_conn_count;
}

} // namespace torrent::client
