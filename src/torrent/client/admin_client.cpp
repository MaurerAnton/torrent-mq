/**
 * torrent-mq — AdminClient: Administrative Operations Implementation
 *
 * The AdminClient provides a programmatic interface for cluster
 * administration tasks. It communicates with the torrent-mq controller
 * broker via the admin RPC protocol to manage topics, partitions,
 * consumer groups, ACLs, and broker/topic configurations.
 *
 * Key Capabilities:
 *   - Topic lifecycle: create, delete, list, describe, alter config
 *   - Partition management: create additional partitions, reassign
 *   - Consumer group ops: list, describe members, delete group, reset offsets
 *   - ACL management: create, describe, delete ACL entries
 *   - Config operations: describe/alter broker and topic configurations
 *   - Cluster metadata: list brokers, describe cluster
 *
 * Communication Model:
 *   All operations are RPCs sent to the cluster controller. The client
 *   maintains a connection to the controller node (discovered via
 *   Metadata RPC or bootstrap configuration). Requests include a
 *   configurable timeout; the default is 30 seconds for most operations
 *   (longer for partition reassignment which may be async).
 *
 * Thread Safety:
 *   AdminClient is thread-safe for concurrent use. Internal state is
 *   protected by a shared mutex. All RPC calls are synchronous from the
 *   caller's perspective but may internally retry on controller failover.
 *
 * Dependencies:
 *   - Cluster connection for RPC transport
 *   - SASL credentials for authenticated clusters
 */

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include "torrent/common/types.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace torrent::client {

// ============================================================================
// Forward declarations
// ============================================================================

class BrokerConnection;  // low-level RPC transport to a broker

// ============================================================================
// Configuration Types
// ============================================================================

/// A single configuration entry (key-value pair with metadata).
struct ConfigEntry {
    std::string name;
    std::string value;
    bool        is_default  = false;
    bool        is_sensitive = false;
    bool        is_read_only = false;
    std::string source;       // "static", "dynamic", "default"
};

/// Topic configuration specification.
struct TopicConfig {
    std::string              name;
    int32_t                  num_partitions     = 1;
    int16_t                  replication_factor = 3;
    std::vector<ConfigEntry> configs;
    /// Optional manual replica assignment: [partition][broker_id...]
    std::optional<std::vector<std::vector<int32_t>>> replica_assignment;
};

/// Topic metadata returned by describe/list operations.
struct TopicDescription {
    std::string              name;
    uint64_t                 topic_id   = 0;
    int32_t                  num_partitions = 0;
    int16_t                  replication_factor = 0;
    bool                     is_internal = false;
    std::vector<ConfigEntry> configs;

    /// Per-partition details.
    struct PartitionInfo {
        int32_t              partition_id = 0;
        int32_t              leader_id    = -1;
        std::vector<int32_t> replica_ids;
        std::vector<int32_t> isr_ids;       // in-sync replicas
        int64_t              log_start_offset = 0;
        int64_t              log_end_offset   = 0;
    };
    std::vector<PartitionInfo> partitions;
};

// ============================================================================
// Consumer Group Types
// ============================================================================

/// Consumer group summary (from list operation).
struct ConsumerGroupSummary {
    std::string group_id;
    std::string protocol_type;    // "consumer"
    std::string state;            // "Stable", "PreparingRebalance", etc.
    int32_t     num_members = 0;
};

/// Detailed consumer group description.
struct ConsumerGroupDescription {
    std::string group_id;
    std::string state;
    std::string protocol_type;
    std::string protocol;         // e.g., "range", "roundrobin", "sticky"
    int32_t     generation_id = 0;

    struct Member {
        std::string member_id;
        std::string client_id;
        std::string client_host;
        std::vector<std::string> subscribed_topics;
        /// Per-partition assignment: topic -> [partition_ids]
        std::map<std::string, std::vector<int32_t>> assignment;
    };
    std::vector<Member> members;
};

// ============================================================================
// ACL Types
// ============================================================================

/// Access Control Entry.
struct AclEntry {
    std::string principal;           // "User:alice", "Group:devs"
    std::string host;                // "*" for any host, or specific IP
    std::string operation;           // "Read", "Write", "Describe", "All"
    std::string permission_type;     // "Allow" or "Deny"
    std::string resource_type;       // "Topic", "Group", "Cluster", etc.
    std::string resource_name;       // Topic name, group name, or "kafka-cluster"
    std::string pattern_type;        // "Literal", "Prefixed", "Match"
};

/// ACL binding for create/delete.
struct AclBinding {
    AclEntry entry;
    /// Optional: filter for describe/delete.
    struct Filter {
        std::string principal;
        std::string host;
        std::string operation;
        std::string permission_type;
        std::string resource_type;
        std::string resource_name;
        std::string pattern_type;
    };
};

/// Result of an admin operation.
struct AdminResult {
    bool        success      = false;
    std::string error_message;
    int16_t     error_code_value = 0;
};

// ============================================================================
// AdminClient — Implementation
// ============================================================================

class AdminClient {
public:
    // ---- Construction -------------------------------------------------------

    /**
     * Construct an AdminClient connected to the given bootstrap servers.
     *
     * @param bootstrap_servers  Comma-separated list of "host:port" pairs.
     * @param timeout_ms         Default RPC timeout (milliseconds).
     * @param client_id          Identifier for this admin client instance.
     */
    explicit AdminClient(const std::string& bootstrap_servers,
                         int32_t timeout_ms = 30000,
                         const std::string& client_id = "admin-cli");

    ~AdminClient();

    // Non-copyable, movable
    AdminClient(const AdminClient&) = delete;
    AdminClient& operator=(const AdminClient&) = delete;
    AdminClient(AdminClient&&) noexcept = default;
    AdminClient& operator=(AdminClient&&) noexcept = default;

    // ---- Connection Management ----------------------------------------------

    /// Connect to the cluster, discover the controller node.
    bool connect();

    /// Disconnect and release resources.
    void close();

    /// Check if the client is currently connected.
    [[nodiscard]] bool is_connected() const noexcept;

    // ---- Topic Operations ---------------------------------------------------

    /**
     * Create a new topic with the specified configuration.
     *
     * @param config  Topic name, partition count, replication factor, configs.
     * @param timeout_ms  Per-request timeout; 0 = use default.
     * @return Result indicating success or failure with error details.
     */
    AdminResult create_topic(const TopicConfig& config, int32_t timeout_ms = 0);

    /**
     * Delete a topic by name.
     *
     * @param topic_name  Name of the topic to delete.
     * @param timeout_ms  Per-request timeout.
     */
    AdminResult delete_topic(const std::string& topic_name, int32_t timeout_ms = 0);

    /**
     * List all topics in the cluster.
     *
     * @param include_internal  If true, include internal topics (prefixed __).
     * @param timeout_ms        Per-request timeout.
     * @return List of topic names.
     */
    std::vector<std::string> list_topics(bool include_internal = false,
                                          int32_t timeout_ms = 0);

    /**
     * Describe one or more topics, returning full metadata.
     *
     * @param topic_names  If empty, describe all topics.
     * @param timeout_ms   Per-request timeout.
     */
    std::vector<TopicDescription> describe_topics(
        const std::vector<std::string>& topic_names = {},
        int32_t timeout_ms = 0);

    /**
     * Create additional partitions for an existing topic.
     *
     * @param topic_name       Target topic.
     * @param new_total_count  New total partition count (must be > current).
     * @param timeout_ms       Per-request timeout.
     */
    AdminResult create_partitions(const std::string& topic_name,
                                   int32_t new_total_count,
                                   int32_t timeout_ms = 0);

    /**
     * Alter topic-level configuration.
     *
     * @param topic_name  Target topic.
     * @param configs     Key-value pairs to set (null value = delete config).
     * @param timeout_ms  Per-request timeout.
     */
    AdminResult alter_topic_config(const std::string& topic_name,
                                    const std::vector<ConfigEntry>& configs,
                                    int32_t timeout_ms = 0);

    // ---- Consumer Group Operations ------------------------------------------

    /**
     * List all consumer groups.
     *
     * @param timeout_ms  Per-request timeout.
     */
    std::vector<ConsumerGroupSummary> list_consumer_groups(int32_t timeout_ms = 0);

    /**
     * Describe members, offsets, and state of consumer groups.
     *
     * @param group_ids   Groups to describe; if empty, all groups.
     * @param timeout_ms  Per-request timeout.
     */
    std::vector<ConsumerGroupDescription> describe_consumer_groups(
        const std::vector<std::string>& group_ids = {},
        int32_t timeout_ms = 0);

    /**
     * Delete a consumer group.
     *
     * @param group_id    Group to delete (must be empty/stable).
     * @param timeout_ms  Per-request timeout.
     */
    AdminResult delete_consumer_group(const std::string& group_id,
                                       int32_t timeout_ms = 0);

    /**
     * Reset consumer group offsets for specified topic-partitions.
     *
     * @param group_id            Target consumer group.
     * @param topic_partitions    Map of topic -> [partition_ids] to reset.
     * @param mode                "earliest", "latest", or timestamp in ms.
     * @param timeout_ms          Per-request timeout.
     */
    AdminResult reset_consumer_group_offsets(
        const std::string& group_id,
        const std::map<std::string, std::vector<int32_t>>& topic_partitions,
        const std::string& mode = "earliest",
        int32_t timeout_ms = 0);

    // ---- ACL Operations -----------------------------------------------------

    /**
     * Create one or more ACL entries.
     */
    AdminResult create_acls(const std::vector<AclBinding>& bindings,
                             int32_t timeout_ms = 0);

    /**
     * Describe ACL entries matching the given filter.
     * An empty filter returns all ACLs.
     */
    std::vector<AclBinding> describe_acls(const AclBinding::Filter& filter = {},
                                            int32_t timeout_ms = 0);

    /**
     * Delete ACL entries matching the given filter.
     */
    AdminResult delete_acls(const AclBinding::Filter& filter,
                             int32_t timeout_ms = 0);

    // ---- Configuration Operations -------------------------------------------

    /**
     * Describe broker-level configuration.
     *
     * @param broker_id   Target broker; -1 = all brokers.
     * @param timeout_ms  Per-request timeout.
     */
    std::vector<ConfigEntry> describe_broker_config(int32_t broker_id = -1,
                                                      int32_t timeout_ms = 0);

    /**
     * Alter broker-level configuration dynamically.
     */
    AdminResult alter_broker_config(int32_t broker_id,
                                     const std::vector<ConfigEntry>& configs,
                                     int32_t timeout_ms = 0);

    // ---- Cluster Metadata ---------------------------------------------------

    /// Get the current controller broker ID.
    [[nodiscard]] int32_t controller_id() const noexcept;

    /// Get the list of broker endpoints in the cluster.
    [[nodiscard]] std::vector<std::pair<int32_t, std::string>> broker_list() const;

private:
    // ---- Internal helpers ---------------------------------------------------

    /// Discover the controller node via metadata RPC.
    bool discover_controller();

    /// Send an admin RPC to the controller, retrying on failover.
    template<typename Request, typename Response>
    Response send_admin_rpc(const Request& req, int32_t timeout_ms);

    /// Reconnect to the controller after a failover.
    bool reconnect_to_controller();

    // ---- State --------------------------------------------------------------
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// ============================================================================
// Implementation
// ============================================================================

struct AdminClient::Impl {
    std::string                        bootstrap_servers;
    std::string                        client_id;
    int32_t                            default_timeout_ms;
    int32_t                            controller_broker_id = -1;
    std::vector<std::pair<int32_t, std::string>> broker_list_cache;

    std::shared_ptr<spdlog::logger>    logger;
    std::mutex                         mutex;
    bool                               connected = false;

    // In a full implementation, this would hold the actual RPC transport.
    // For the stub, we simulate via a boolean flag and mock data.
    bool simulate = true;

    Impl(const std::string& bootstrap, int32_t timeout, const std::string& cid)
        : bootstrap_servers(bootstrap)
        , client_id(cid)
        , default_timeout_ms(timeout) {
        logger = spdlog::get("admin_client");
        if (!logger) {
            logger = spdlog::stdout_color_mt("admin_client");
            logger->set_level(spdlog::level::info);
        }
    }

    [[nodiscard]] int32_t resolve_timeout(int32_t call_timeout) const noexcept {
        return call_timeout > 0 ? call_timeout : default_timeout_ms;
    }
};

// ---- Construction / Connection ----------------------------------------------

AdminClient::AdminClient(const std::string& bootstrap_servers,
                          int32_t timeout_ms,
                          const std::string& client_id)
    : impl_(std::make_unique<Impl>(bootstrap_servers, timeout_ms, client_id)) {
    impl_->logger->info("AdminClient created: bootstrap={} timeout={}ms client_id={}",
                        bootstrap_servers, timeout_ms, client_id);
}

AdminClient::~AdminClient() {
    try {
        close();
    } catch (...) {
        // swallow exceptions in destructor
    }
}

bool AdminClient::connect() {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (impl_->connected) {
        impl_->logger->debug("AdminClient::connect: already connected");
        return true;
    }

    impl_->logger->info("AdminClient: connecting to bootstrap servers: {}",
                        impl_->bootstrap_servers);

    // Simulate cluster discovery
    if (impl_->simulate) {
        impl_->controller_broker_id = 1;
        impl_->broker_list_cache = {
            {1, "broker1:9092"},
            {2, "broker2:9092"},
            {3, "broker3:9092"},
        };
        impl_->connected = true;
        impl_->logger->info("AdminClient: connected (simulated), controller=broker:{}",
                            impl_->controller_broker_id);
        return true;
    }

    return discover_controller();
}

void AdminClient::close() {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->connected = false;
    impl_->controller_broker_id = -1;
    impl_->broker_list_cache.clear();
    impl_->logger->info("AdminClient: closed");
}

bool AdminClient::is_connected() const noexcept {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->connected;
}

// ---- Topic Operations -------------------------------------------------------

AdminResult AdminClient::create_topic(const TopicConfig& config, int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (!impl_->connected) {
        return {false, "Not connected to cluster",
                static_cast<int16_t>(error_code::broker_not_available)};
    }

    int32_t effective_timeout = impl_->resolve_timeout(timeout_ms);

    impl_->logger->info("AdminClient::create_topic: name={} partitions={} rf={}",
                        config.name, config.num_partitions, config.replication_factor);

    // --- Validation ---
    if (config.name.empty()) {
        return {false, "Topic name must not be empty"};
    }
    if (config.num_partitions < 1 || config.num_partitions > 100000) {
        return {false, "Invalid partition count: " + std::to_string(config.num_partitions)};
    }
    if (config.replication_factor < 1 || config.replication_factor > 10) {
        return {false, "Invalid replication factor: " +
                std::to_string(config.replication_factor)};
    }

    // --- Send RPC to controller ---
    // (simulated success)
    impl_->logger->info("AdminClient::create_topic: topic '{}' created successfully",
                        config.name);
    return {true, "", 0};
}

AdminResult AdminClient::delete_topic(const std::string& topic_name, int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (!impl_->connected) {
        return {false, "Not connected to cluster"};
    }

    if (topic_name.empty()) {
        return {false, "Topic name must not be empty"};
    }

    impl_->logger->info("AdminClient::delete_topic: name={}", topic_name);

    // (simulated success)
    return {true, "", 0};
}

std::vector<std::string> AdminClient::list_topics(bool include_internal,
                                                    int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (!impl_->connected) return {};

    impl_->logger->info("AdminClient::list_topics: include_internal={}", include_internal);

    // (simulated response)
    std::vector<std::string> topics = {
        "orders", "payments", "inventory", "shipments",
        "user-events", "notifications"
    };
    if (include_internal) {
        topics.insert(topics.end(), {"__consumer_offsets", "__transaction_state"});
    }

    return topics;
}

std::vector<TopicDescription> AdminClient::describe_topics(
    const std::vector<std::string>& topic_names,
    int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (!impl_->connected) return {};

    std::vector<std::string> names = topic_names;
    if (names.empty()) {
        names = {"orders", "payments", "inventory"};
    }

    impl_->logger->info("AdminClient::describe_topics: {} topic(s)", names.size());

    // (simulated response)
    std::vector<TopicDescription> result;
    for (size_t i = 0; i < names.size(); ++i) {
        TopicDescription td;
        td.name = names[i];
        td.topic_id = static_cast<uint64_t>(1000 + i);
        td.num_partitions = 8;
        td.replication_factor = 3;

        for (int32_t p = 0; p < 8; ++p) {
            TopicDescription::PartitionInfo pi;
            pi.partition_id = p;
            pi.leader_id = (p % 3) + 1;
            pi.replica_ids = {1, 2, 3};
            pi.isr_ids = {1, 2, 3};
            pi.log_start_offset = 0;
            pi.log_end_offset = 1000000;
            td.partitions.push_back(pi);
        }

        ConfigEntry retention;
        retention.name = "retention.ms";
        retention.value = "604800000";
        retention.source = "static";
        td.configs.push_back(retention);

        result.push_back(std::move(td));
    }

    return result;
}

AdminResult AdminClient::create_partitions(const std::string& topic_name,
                                             int32_t new_total_count,
                                             int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (!impl_->connected) return {false, "Not connected to cluster"};
    if (topic_name.empty()) return {false, "Topic name must not be empty"};
    if (new_total_count < 1) return {false, "New partition count must be >= 1"};

    impl_->logger->info("AdminClient::create_partitions: topic={} new_count={}",
                        topic_name, new_total_count);

    return {true, "", 0};
}

AdminResult AdminClient::alter_topic_config(const std::string& topic_name,
                                              const std::vector<ConfigEntry>& configs,
                                              int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (!impl_->connected) return {false, "Not connected to cluster"};
    if (topic_name.empty()) return {false, "Topic name must not be empty"};

    impl_->logger->info("AdminClient::alter_topic_config: topic={} configs={}",
                        topic_name, configs.size());

    for (const auto& cfg : configs) {
        impl_->logger->debug("  {} = {} (read_only={}, sensitive={})",
                             cfg.name,
                             cfg.is_sensitive ? "***" : cfg.value,
                             cfg.is_read_only, cfg.is_sensitive);
    }

    return {true, "", 0};
}

// ---- Consumer Group Operations ----------------------------------------------

std::vector<ConsumerGroupSummary> AdminClient::list_consumer_groups(int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (!impl_->connected) return {};

    impl_->logger->info("AdminClient::list_consumer_groups");

    // (simulated)
    return {
        {"order-processor", "consumer", "Stable", 3},
        {"payment-validator", "consumer", "Stable", 2},
        {"analytics-sink", "consumer", "PreparingRebalance", 4},
    };
}

std::vector<ConsumerGroupDescription> AdminClient::describe_consumer_groups(
    const std::vector<std::string>& group_ids,
    int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (!impl_->connected) return {};

    std::vector<std::string> ids = group_ids.empty()
        ? std::vector<std::string>{"order-processor", "payment-validator"}
        : group_ids;

    impl_->logger->info("AdminClient::describe_consumer_groups: {} group(s)", ids.size());

    // (simulated)
    std::vector<ConsumerGroupDescription> result;
    for (const auto& gid : ids) {
        ConsumerGroupDescription cgd;
        cgd.group_id = gid;
        cgd.state = "Stable";
        cgd.protocol_type = "consumer";
        cgd.protocol = "range";
        cgd.generation_id = 5;

        ConsumerGroupDescription::Member m;
        m.member_id = gid + "-consumer-1";
        m.client_id = gid + "-client";
        m.client_host = "/192.168.1.10";
        m.subscribed_topics = {"orders", "payments"};
        m.assignment["orders"] = {0, 1, 2, 3};
        m.assignment["payments"] = {0, 1};
        cgd.members.push_back(m);

        result.push_back(std::move(cgd));
    }

    return result;
}

AdminResult AdminClient::delete_consumer_group(const std::string& group_id,
                                                int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (!impl_->connected) return {false, "Not connected to cluster"};
    if (group_id.empty()) return {false, "Group ID must not be empty"};

    impl_->logger->info("AdminClient::delete_consumer_group: group={}", group_id);

    return {true, "", 0};
}

AdminResult AdminClient::reset_consumer_group_offsets(
    const std::string& group_id,
    const std::map<std::string, std::vector<int32_t>>& topic_partitions,
    const std::string& mode,
    int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (!impl_->connected) return {false, "Not connected to cluster"};
    if (group_id.empty()) return {false, "Group ID must not be empty"};

    impl_->logger->info("AdminClient::reset_consumer_group_offsets: group={} mode={} "
                        "topic_partitions={}",
                        group_id, mode, topic_partitions.size());

    return {true, "", 0};
}

// ---- ACL Operations ---------------------------------------------------------

AdminResult AdminClient::create_acls(const std::vector<AclBinding>& bindings,
                                      int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (!impl_->connected) return {false, "Not connected to cluster"};

    impl_->logger->info("AdminClient::create_acls: {} binding(s)", bindings.size());

    for (const auto& b : bindings) {
        impl_->logger->debug("  principal={} operation={} resource={}:{} permission={}",
                             b.entry.principal, b.entry.operation,
                             b.entry.resource_type, b.entry.resource_name,
                             b.entry.permission_type);
    }

    return {true, "", 0};
}

std::vector<AclBinding> AdminClient::describe_acls(const AclBinding::Filter& filter,
                                                      int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (!impl_->connected) return {};

    impl_->logger->info("AdminClient::describe_acls: principal={} resource={}:{}",
                        filter.principal, filter.resource_type, filter.resource_name);

    // (simulated)
    AclBinding binding;
    binding.entry.principal = "User:admin";
    binding.entry.host = "*";
    binding.entry.operation = "All";
    binding.entry.permission_type = "Allow";
    binding.entry.resource_type = "Cluster";
    binding.entry.resource_name = "kafka-cluster";
    binding.entry.pattern_type = "Literal";
    return {binding};
}

AdminResult AdminClient::delete_acls(const AclBinding::Filter& filter,
                                      int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (!impl_->connected) return {false, "Not connected to cluster"};

    impl_->logger->info("AdminClient::delete_acls: principal={} resource={}:{}",
                        filter.principal, filter.resource_type, filter.resource_name);

    return {true, "", 0};
}

// ---- Configuration Operations -----------------------------------------------

std::vector<ConfigEntry> AdminClient::describe_broker_config(int32_t broker_id,
                                                              int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (!impl_->connected) return {};

    impl_->logger->info("AdminClient::describe_broker_config: broker_id={}", broker_id);

    // (simulated)
    return {
        {"log.retention.ms", "604800000", true, false, false, "static"},
        {"log.segment.bytes", "1073741824", true, false, false, "static"},
        {"compression.type", "producer", true, false, false, "static"},
        {"max.message.bytes", "1048588", true, false, false, "static"},
        {"min.insync.replicas", "2", false, false, false, "dynamic"},
    };
}

AdminResult AdminClient::alter_broker_config(int32_t broker_id,
                                              const std::vector<ConfigEntry>& configs,
                                              int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    if (!impl_->connected) return {false, "Not connected to cluster"};

    impl_->logger->info("AdminClient::alter_broker_config: broker_id={} configs={}",
                        broker_id, configs.size());

    return {true, "", 0};
}

// ---- Cluster Metadata -------------------------------------------------------

int32_t AdminClient::controller_id() const noexcept {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->controller_broker_id;
}

std::vector<std::pair<int32_t, std::string>> AdminClient::broker_list() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->broker_list_cache;
}

// ---- Internal Helpers -------------------------------------------------------

bool AdminClient::discover_controller() {
    // In a full implementation: send Metadata RPC, extract controller_id,
    // cache broker endpoints.
    impl_->logger->info("AdminClient: discovering controller via metadata RPC...");
    return false;  // stub
}

bool AdminClient::reconnect_to_controller() {
    impl_->logger->warn("AdminClient: controller failover detected, reconnecting...");
    return discover_controller();
}

} // namespace torrent::client
