/**
 * torrent-mq — ConsumerClient: Consumer Group Member Implementation
 *
 * The ConsumerClient implements Kafka-compatible consumer group semantics:
 * subscribe to topics, poll for messages, commit offsets, and participate
 * in the group rebalance protocol. It is the primary API for consuming
 * messages from torrent-mq topics.
 *
 * Key Features:
 *   - Subscribe/unsubscribe to one or more topics with pattern support
 *   - Poll-based message retrieval with configurable timeout and batch size
 *   - Automatic partition assignment via group coordinator (range/roundrobin/
 *     sticky assignors)
 *   - Manual and automatic offset commit (sync and async)
 *   - Seek to specific offsets, timestamps, beginning, or end of partitions
 *   - Pause/resume individual partitions for flow control
 *   - Rebalance listener callback for custom partition revocation/assignment
 *
 * Group Protocol:
 *   The consumer participates in a consumer group managed by the group
 *   coordinator (one of the cluster brokers). The protocol follows the
 *   Kafka group membership state machine:
 *     JoinGroup → SyncGroup → (Stable) → Heartbeat → (Timeout → Rejoin)
 *
 * Thread Safety:
 *   ConsumerClient is thread-safe. All public methods acquire an internal
 *   mutex. The poll() method is typically called from a single dedicated
 *   polling thread, but concurrent calls with other methods are safe.
 *
 * Dependencies:
 *   - Broker connections for Fetch, OffsetCommit, Heartbeat, JoinGroup RPCs
 *   - MetadataClient for topic/partition discovery
 *   - GroupCoordinator for consumer group protocol
 */

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace torrent::client {

// ============================================================================
// Forward Declarations
// ============================================================================

class BrokerConnection;

// ============================================================================
// Consumer Record
// ============================================================================

/// A single consumed record with key, value, and metadata.
struct ConsumerRecord {
    std::string topic;
    int32_t     partition   = 0;
    int64_t     offset      = 0;
    int64_t     timestamp   = 0;       // ms since epoch
    int32_t     leader_epoch = 0;

    /// Raw key bytes (may be null/empty).
    std::vector<char> key;

    /// Raw value bytes (may be null/empty).
    std::vector<char> value;

    /// Record headers (key-value pairs).
    std::vector<std::pair<std::string, std::string>> headers;

    [[nodiscard]] bool has_key() const noexcept { return !key.empty(); }
    [[nodiscard]] bool has_value() const noexcept { return !value.empty(); }
};

// ============================================================================
// Consumer Configuration
// ============================================================================

struct ConsumerConfig {
    std::string bootstrap_servers = "localhost:9092";
    std::string group_id;
    std::string client_id = "torrent-consumer";

    /// Auto-commit interval in milliseconds. 0 disables auto-commit.
    int32_t auto_commit_interval_ms = 5000;

    /// Session timeout for group membership (ms).
    int32_t session_timeout_ms = 45000;

    /// Heartbeat interval for group membership (ms).
    int32_t heartbeat_interval_ms = 3000;

    /// Maximum time to block in poll() waiting for records (ms).
    int32_t poll_timeout_ms = 500;

    /// Maximum number of records returned per poll().
    int32_t max_poll_records = 500;

    /// Maximum bytes fetched per partition per request.
    int32_t max_partition_fetch_bytes = 1048576;  // 1 MiB

    /// Offset reset policy when no committed offset exists.
    enum class OffsetReset {
        earliest,
        latest,
        none,     // throw exception if no offset
    };
    OffsetReset auto_offset_reset = OffsetReset::latest;

    /// Partition assignment strategy.
    enum class Assignor {
        range,
        round_robin,
        sticky,
        cooperative_sticky,
    };
    Assignor partition_assignor = Assignor::range;

    /// Whether to enable auto-commit of offsets.
    bool enable_auto_commit = true;

    /// Isolation level for transactional reads.
    isolation_level read_isolation = isolation_level::read_committed;
};

// ============================================================================
// Rebalance Listener
// ============================================================================

/**
 * Callback interface invoked during group rebalance.
 * Implementations receive notifications when partitions are revoked
 * (allowing final offset commit before reassignment) and assigned.
 */
class RebalanceListener {
public:
    virtual ~RebalanceListener() = default;

    /**
     * Called when partitions are about to be revoked.
     * The consumer should commit offsets for these partitions here.
     *
     * @param partitions  Map of topic → [partition_ids] being revoked.
     */
    virtual void on_partitions_revoked(
        const std::map<std::string, std::vector<int32_t>>& partitions) {}

    /**
     * Called when partitions have been assigned to this consumer.
     *
     * @param partitions  Map of topic → [partition_ids] now assigned.
     */
    virtual void on_partitions_assigned(
        const std::map<std::string, std::vector<int32_t>>& partitions) {}

    /**
     * Called when partitions are lost unexpectedly (e.g., session timeout).
     * No opportunity to commit offsets; data may be re-consumed.
     */
    virtual void on_partitions_lost(
        const std::map<std::string, std::vector<int32_t>>& partitions) {}
};

// ============================================================================
// Offset and Topic Metadata
// ============================================================================

/// Offset and metadata for a topic-partition.
struct OffsetAndMetadata {
    int64_t     offset   = 0;
    std::string metadata;  // opaque, user-provided (max 4096 bytes)
    int32_t     leader_epoch = 0;
};

/// Topic-partition identifier.
struct TopicPartition {
    std::string topic;
    int32_t     partition = 0;

    [[nodiscard]] bool operator==(const TopicPartition& other) const noexcept {
        return partition == other.partition && topic == other.topic;
    }

    [[nodiscard]] bool operator<(const TopicPartition& other) const noexcept {
        if (topic != other.topic) return topic < other.topic;
        return partition < other.partition;
    }
};

} // namespace torrent::client

namespace std {
template<>
struct hash<torrent::client::TopicPartition> {
    size_t operator()(const torrent::client::TopicPartition& tp) const noexcept {
        return hash<string>{}(tp.topic) ^ (hash<int32_t>{}(tp.partition) << 1);
    }
};
} // namespace std

namespace torrent::client {

// ============================================================================
// Offset Commit Callback
// ============================================================================

using OffsetCommitCallback = std::function<void(
    const std::map<TopicPartition, error_code>& errors)>;

// ============================================================================
// ConsumerClient — Public Interface
// ============================================================================

class ConsumerClient {
public:
    // ---- Construction -------------------------------------------------------

    explicit ConsumerClient(const ConsumerConfig& config);
    ~ConsumerClient();

    // Non-copyable, movable
    ConsumerClient(const ConsumerClient&) = delete;
    ConsumerClient& operator=(const ConsumerClient&) = delete;
    ConsumerClient(ConsumerClient&&) noexcept = default;
    ConsumerClient& operator=(ConsumerClient&&) noexcept = default;

    // ---- Subscription Management -------------------------------------------

    /**
     * Subscribe to a set of topics. This replaces any previous subscription.
     * Triggers a group rebalance to receive partition assignments.
     *
     * @param topics    List of topic names to subscribe to.
     * @param listener  Optional rebalance callback.
     */
    void subscribe(const std::vector<std::string>& topics,
                   std::shared_ptr<RebalanceListener> listener = nullptr);

    /**
     * Subscribe to topics matching a regex pattern.
     * The pattern is matched against all topics in the cluster at join time.
     */
    void subscribe_pattern(const std::string& pattern_regex,
                           std::shared_ptr<RebalanceListener> listener = nullptr);

    /**
     * Unsubscribe from all topics. This triggers a group leave and
     * revokes all assigned partitions.
     */
    void unsubscribe();

    /**
     * Get the current topic subscription.
     */
    [[nodiscard]] std::vector<std::string> subscription() const noexcept;

    // ---- Polling ------------------------------------------------------------

    /**
     * Poll for new messages from assigned partitions.
     *
     * Blocks up to the configured poll_timeout_ms waiting for records.
     * Returns an empty vector if no records are available within the timeout.
     *
     * This method also handles:
     *   - Sending heartbeat to group coordinator
     *   - Auto-committing offsets (if enabled)
     *   - Detecting and triggering rebalance
     */
    std::vector<ConsumerRecord> poll(int32_t timeout_ms = -1);

    // ---- Offset Management --------------------------------------------------

    /**
     * Commit offsets synchronously for the given partitions.
     * If no partitions are provided, commits offsets for all assigned
     * partitions at their current position.
     */
    void commit_sync(const std::map<TopicPartition, OffsetAndMetadata>& offsets = {});

    /**
     * Commit offsets asynchronously. The callback is invoked when the
     * broker acknowledges the commit (or on failure).
     */
    void commit_async(const std::map<TopicPartition, OffsetAndMetadata>& offsets,
                      OffsetCommitCallback callback = nullptr);

    /**
     * Get the last committed offset for a partition.
     */
    [[nodiscard]] int64_t committed(const TopicPartition& tp) const noexcept;

    /**
     * Get the current position (next offset to read) for a partition.
     */
    [[nodiscard]] int64_t position(const TopicPartition& tp) const noexcept;

    // ---- Seek Operations ----------------------------------------------------

    /**
     * Seek to a specific offset in a partition.
     */
    void seek(const TopicPartition& tp, int64_t offset);

    /**
     * Seek to the beginning (earliest available offset) of a partition.
     */
    void seek_to_beginning(const TopicPartition& tp);

    /**
     * Seek to the end (latest available offset) of a partition.
     */
    void seek_to_end(const TopicPartition& tp);

    /**
     * Seek to the first offset with timestamp >= the given value.
     * Returns the actual offset.
     */
    int64_t seek_to_timestamp(const TopicPartition& tp, int64_t timestamp_ms);

    // ---- Flow Control -------------------------------------------------------

    /**
     * Pause fetching from a set of partitions. Paused partitions are not
     * retrieved by poll() but their positions are still tracked.
     */
    void pause(const std::vector<TopicPartition>& partitions);

    /**
     * Resume fetching from previously paused partitions.
     */
    void resume(const std::vector<TopicPartition>& partitions);

    /**
     * Get the set of currently paused partitions.
     */
    [[nodiscard]] std::set<TopicPartition> paused() const noexcept;

    // ---- Assignment ---------------------------------------------------------

    /**
     * Get the current partition assignment (topic → partition list).
     */
    [[nodiscard]] std::map<std::string, std::vector<int32_t>> assignment() const noexcept;

    /**
     * Manually assign partitions instead of using group management.
     * This removes the consumer from any group.
     */
    void assign(const std::vector<TopicPartition>& partitions);

    // ---- Lifecycle ----------------------------------------------------------

    /**
     * Close the consumer gracefully: leave the group, commit offsets,
     * and release network resources.
     */
    void close();

    /**
     * Wake up a blocked poll() call. Useful for shutting down the
     * consumer from another thread.
     */
    void wakeup();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// ============================================================================
// Implementation
// ============================================================================

struct ConsumerClient::Impl {
    ConsumerConfig                                      config;
    std::shared_ptr<spdlog::logger>                     logger;
    std::mutex                                          mutex;
    std::condition_variable                             cv;

    // --- Subscription state ---
    std::vector<std::string>                            subscribed_topics;
    std::optional<std::string>                          subscribed_pattern;
    std::shared_ptr<RebalanceListener>                  rebalance_listener;

    // --- Partition tracking ---
    /// Current assignment: topic → [partition_ids]
    std::map<std::string, std::vector<int32_t>>          assigned_partitions;

    /// Current read position per partition.
    std::map<TopicPartition, int64_t>                    positions;

    /// Last committed offset per partition.
    std::map<TopicPartition, OffsetAndMetadata>          committed_offsets;

    /// Paused partitions.
    std::set<TopicPartition>                             paused_partitions;

    // --- Group state ---
    std::string                                          member_id;
    int32_t                                              generation_id = -1;
    int64_t                                              last_heartbeat_ms = 0;
    int64_t                                              last_auto_commit_ms = 0;
    bool                                                 group_joined = false;

    // --- Lifecycle ---
    std::atomic<bool>                                    running{false};
    std::atomic<bool>                                    wakeup_called{false};
    std::atomic<bool>                                    closed{false};

    // --- Message buffer ---
    std::vector<ConsumerRecord>                          prefetch_buffer;
    size_t                                               prefetch_index = 0;

    // --- Simulated mode (stub) ---
    bool                                                 simulate = true;
    int64_t                                              simulated_offset = 0;

    explicit Impl(const ConsumerConfig& cfg) : config(cfg) {
        logger = spdlog::get("consumer_client");
        if (!logger) {
            logger = spdlog::stdout_color_mt("consumer_client");
            logger->set_level(spdlog::level::info);
        }
    }

    void ensure_connected() {
        // In production: establish broker connections, discover group coordinator
    }

    void join_group() {
        if (!simulate && !subscribed_topics.empty()) {
            logger->info("ConsumerClient: joining group '{}' for {} topics",
                         config.group_id, subscribed_topics.size());
            // In production: send JoinGroup RPC, wait for SyncGroup, etc.
        }
        group_joined = true;
    }

    void leave_group() {
        if (group_joined) {
            logger->info("ConsumerClient: leaving group '{}'", config.group_id);
            // In production: send LeaveGroup RPC
            group_joined = false;
        }
    }

    void send_heartbeat() {
        int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();

        if (now - last_heartbeat_ms >= config.heartbeat_interval_ms) {
            last_heartbeat_ms = now;
            // In production: send Heartbeat RPC
            logger->trace("ConsumerClient: heartbeat sent, generation={}", generation_id);
        }
    }

    void auto_commit_offsets() {
        if (!config.enable_auto_commit || config.auto_commit_interval_ms <= 0) {
            return;
        }

        int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();

        if (now - last_auto_commit_ms >= config.auto_commit_interval_ms) {
            last_auto_commit_ms = now;

            std::map<TopicPartition, OffsetAndMetadata> offsets;
            for (const auto& [tp, pos] : positions) {
                OffsetAndMetadata om;
                om.offset = pos;
                offsets[tp] = om;
            }

            if (!offsets.empty()) {
                // In production: send OffsetCommit RPC
                logger->trace("ConsumerClient: auto-committing {} offsets", offsets.size());
                for (auto& [tp, om] : offsets) {
                    committed_offsets[tp] = om;
                }
            }
        }
    }

    void reset_positions_from_committed() {
        for (const auto& [tp, om] : committed_offsets) {
            positions[tp] = om.offset;
        }
    }

    std::vector<ConsumerRecord> fetch_records(int32_t timeout_ms) {
        // In production: send Fetch RPC to partition leaders
        // For stub: return simulated records
        std::vector<ConsumerRecord> records;

        if (!simulate) return records;

        // Simulate producing some records for each assigned partition
        for (const auto& [topic, partitions] : assigned_partitions) {
            for (int32_t partition : partitions) {
                TopicPartition tp{topic, partition};

                // Skip paused partitions
                if (paused_partitions.count(tp)) continue;

                // Generate up to 10 simulated records per partition per poll
                int count = std::min(10, config.max_poll_records / static_cast<int>(assigned_partitions.size() * 3));
                for (int i = 0; i < count && static_cast<int>(records.size()) < config.max_poll_records; ++i) {
                    ConsumerRecord rec;
                    rec.topic = topic;
                    rec.partition = partition;
                    rec.offset = positions[tp]++;
                    rec.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count();

                    std::string val = "message-" + std::to_string(rec.offset) +
                                      " from " + topic + "-" + std::to_string(partition);
                    rec.value.assign(val.begin(), val.end());

                    records.push_back(std::move(rec));
                }
            }
        }

        return records;
    }
};

// ---- Construction / Destruction ---------------------------------------------

ConsumerClient::ConsumerClient(const ConsumerConfig& config)
    : impl_(std::make_unique<Impl>(config)) {
    impl_->logger->info("ConsumerClient created: group={} client_id={}",
                        config.group_id, config.client_id);
    impl_->running.store(true, std::memory_order_release);
    impl_->ensure_connected();
}

ConsumerClient::~ConsumerClient() {
    try {
        close();
    } catch (...) {
        // Swallow exceptions in destructor
    }
}

// ---- Subscription -----------------------------------------------------------

void ConsumerClient::subscribe(const std::vector<std::string>& topics,
                                std::shared_ptr<RebalanceListener> listener) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    impl_->logger->info("ConsumerClient::subscribe: {} topic(s)", topics.size());

    impl_->subscribed_topics = topics;
    impl_->subscribed_pattern.reset();
    impl_->rebalance_listener = std::move(listener);

    // In production: trigger group join
    impl_->join_group();

    // Simulate assignment
    if (impl_->simulate) {
        impl_->assigned_partitions.clear();
        for (const auto& topic : topics) {
            impl_->assigned_partitions[topic] = {0, 1, 2};
            for (int32_t p = 0; p < 3; ++p) {
                TopicPartition tp{topic, p};
                if (impl_->positions.find(tp) == impl_->positions.end()) {
                    impl_->positions[tp] = 0;
                }
            }
        }

        if (impl_->rebalance_listener) {
            impl_->rebalance_listener->on_partitions_assigned(impl_->assigned_partitions);
        }
    }
}

void ConsumerClient::subscribe_pattern(const std::string& pattern_regex,
                                        std::shared_ptr<RebalanceListener> listener) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    impl_->logger->info("ConsumerClient::subscribe_pattern: regex='{}'", pattern_regex);

    impl_->subscribed_topics = {"orders", "payments"}; // simulated match
    impl_->subscribed_pattern = pattern_regex;
    impl_->rebalance_listener = std::move(listener);

    impl_->join_group();
}

void ConsumerClient::unsubscribe() {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    impl_->logger->info("ConsumerClient::unsubscribe");
    impl_->leave_group();
    impl_->subscribed_topics.clear();
    impl_->subscribed_pattern.reset();
    impl_->assigned_partitions.clear();
}

std::vector<std::string> ConsumerClient::subscription() const noexcept {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->subscribed_topics;
}

// ---- Polling ----------------------------------------------------------------

std::vector<ConsumerRecord> ConsumerClient::poll(int32_t timeout_ms) {
    int32_t effective_timeout = timeout_ms >= 0 ? timeout_ms : impl_->config.poll_timeout_ms;

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(effective_timeout);

    while (true) {
        {
            std::lock_guard<std::mutex> lock(impl_->mutex);

            if (!impl_->running.load(std::memory_order_acquire)) {
                return {};
            }

            // Send heartbeat to keep group membership alive
            impl_->send_heartbeat();

            // Auto-commit offsets on interval
            impl_->auto_commit_offsets();

            // Fetch records from assigned partitions
            auto records = impl_->fetch_records(effective_timeout);
            if (!records.empty()) {
                impl_->logger->trace("ConsumerClient::poll: {} records fetched",
                                     records.size());
                return records;
            }
        }

        // Check timeout
        if (std::chrono::steady_clock::now() >= deadline) {
            break;
        }

        // Brief sleep to avoid busy-waiting; wakeup can interrupt this
        {
            std::unique_lock<std::mutex> lock(impl_->mutex);
            impl_->cv.wait_for(lock, std::chrono::milliseconds(10), [this] {
                return impl_->wakeup_called.load(std::memory_order_acquire) ||
                       !impl_->running.load(std::memory_order_acquire);
            });
            if (impl_->wakeup_called.exchange(false, std::memory_order_acq_rel)) {
                break;
            }
        }
    }

    return {};
}

// ---- Offset Management ------------------------------------------------------

void ConsumerClient::commit_sync(const std::map<TopicPartition, OffsetAndMetadata>& offsets) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    std::map<TopicPartition, OffsetAndMetadata> to_commit = offsets;
    if (to_commit.empty()) {
        // Commit all current positions
        for (const auto& [tp, pos] : impl_->positions) {
            OffsetAndMetadata om;
            om.offset = pos;
            to_commit[tp] = om;
        }
    }

    impl_->logger->info("ConsumerClient::commit_sync: {} offsets", to_commit.size());

    // In production: send synchronous OffsetCommit RPC
    for (auto& [tp, om] : to_commit) {
        impl_->committed_offsets[tp] = om;
        impl_->logger->debug("  committed {}[{}] → offset={}",
                             tp.topic, tp.partition, om.offset);
    }
}

void ConsumerClient::commit_async(const std::map<TopicPartition, OffsetAndMetadata>& offsets,
                                   OffsetCommitCallback callback) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    impl_->logger->info("ConsumerClient::commit_async: {} offsets", offsets.size());

    // In production: send async OffsetCommit RPC, invoke callback on response
    for (const auto& [tp, om] : offsets) {
        impl_->committed_offsets[tp] = om;
    }

    if (callback) {
        callback({}); // success
    }
}

int64_t ConsumerClient::committed(const TopicPartition& tp) const noexcept {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    auto it = impl_->committed_offsets.find(tp);
    return (it != impl_->committed_offsets.end()) ? it->second.offset : -1;
}

int64_t ConsumerClient::position(const TopicPartition& tp) const noexcept {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    auto it = impl_->positions.find(tp);
    return (it != impl_->positions.end()) ? it->second : -1;
}

// ---- Seek Operations --------------------------------------------------------

void ConsumerClient::seek(const TopicPartition& tp, int64_t offset) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    impl_->logger->info("ConsumerClient::seek: {}[{}] → offset={}",
                        tp.topic, tp.partition, offset);
    impl_->positions[tp] = offset;
}

void ConsumerClient::seek_to_beginning(const TopicPartition& tp) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    impl_->logger->info("ConsumerClient::seek_to_beginning: {}[{}]",
                        tp.topic, tp.partition);
    // In production: send OffsetForLeaderEpoch or ListOffsets RPC
    // Stub: seek to 0
    impl_->positions[tp] = 0;
}

void ConsumerClient::seek_to_end(const TopicPartition& tp) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    impl_->logger->info("ConsumerClient::seek_to_end: {}[{}]",
                        tp.topic, tp.partition);
    // In production: send ListOffsets RPC with timestamp=-1 (latest)
    // Stub: seek to a high simulated offset
    impl_->positions[tp] = 999999;
}

int64_t ConsumerClient::seek_to_timestamp(const TopicPartition& tp, int64_t timestamp_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    impl_->logger->info("ConsumerClient::seek_to_timestamp: {}[{}] timestamp={}",
                        tp.topic, tp.partition, timestamp_ms);
    // In production: send OffsetForLeaderEpoch RPC with timestamp
    // Stub: return a simulated offset
    int64_t offset = timestamp_ms / 1000;
    impl_->positions[tp] = offset;
    return offset;
}

// ---- Flow Control -----------------------------------------------------------

void ConsumerClient::pause(const std::vector<TopicPartition>& partitions) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    for (const auto& tp : partitions) {
        impl_->paused_partitions.insert(tp);
        impl_->logger->debug("ConsumerClient::pause: {}[{}]", tp.topic, tp.partition);
    }
}

void ConsumerClient::resume(const std::vector<TopicPartition>& partitions) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    for (const auto& tp : partitions) {
        impl_->paused_partitions.erase(tp);
        impl_->logger->debug("ConsumerClient::resume: {}[{}]", tp.topic, tp.partition);
    }
}

std::set<TopicPartition> ConsumerClient::paused() const noexcept {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->paused_partitions;
}

// ---- Assignment -------------------------------------------------------------

std::map<std::string, std::vector<int32_t>> ConsumerClient::assignment() const noexcept {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->assigned_partitions;
}

void ConsumerClient::assign(const std::vector<TopicPartition>& partitions) {
    std::lock_guard<std::mutex> lock(impl_->mutex);

    impl_->logger->info("ConsumerClient::assign: {} partition(s)", partitions.size());

    // Manual assignment: leave the group
    impl_->leave_group();

    impl_->assigned_partitions.clear();
    impl_->positions.clear();

    for (const auto& tp : partitions) {
        impl_->assigned_partitions[tp.topic].push_back(tp.partition);
        impl_->positions[tp] = 0;
    }
}

// ---- Lifecycle --------------------------------------------------------------

void ConsumerClient::close() {
    if (impl_->closed.exchange(true, std::memory_order_acq_rel)) {
        return; // Already closed
    }

    impl_->logger->info("ConsumerClient::close");

    impl_->running.store(false, std::memory_order_release);

    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        // Commit offsets before leaving
        commit_sync({});
        impl_->leave_group();
    }

    impl_->cv.notify_all();
}

void ConsumerClient::wakeup() {
    impl_->wakeup_called.store(true, std::memory_order_release);
    impl_->cv.notify_all();
}

} // namespace torrent::client
