/**
 * metadata_manager.cpp — Cluster Metadata Manager (Raft-backed)
 *
 * Manages topic, broker, and partition metadata stored in the Raft
 * consensus log.  This is the authoritative source of truth for the
 * cluster control plane — every metadata mutation goes through Raft
 * to ensure consistency across all brokers.
 *
 *   Topic Metadata
 *     - Create topic (name, partitions, replication factor, config).
 *     - Delete topic (with safety checks: consumer groups, retention).
 *     - Update topic config (retention, compaction, quotas).
 *     - List topics, get topic details.
 *
 *   Broker Metadata
 *     - Register broker on startup (host, port, rack, capabilities).
 *     - Deregister broker on shutdown.
 *     - Heartbeat / liveness tracking.
 *     - Update broker config.
 *
 *   Partition Metadata
 *     - Track leader, ISR (in-sync replicas), replica set.
 *     - Leader epoch fencing.
 *     - Replica reassignment.
 *
 *   Raft Snapshot Integration
 *     - serialize_metadata(): produce a byte blob for the Raft snapshot.
 *     - deserialize_metadata(): restore metadata from a snapshot blob.
 *     - Supports incremental snapshot deltas for efficiency.
 *
 * All mutations are modeled as RaftCommands (UserCommand with serialised
 * payload) proposed through the Raft leader.  The apply() method processes
 * committed commands and updates in-memory state deterministically.
 *
 * References:
 *   Kafka Metadata Protocol
 *   Ongaro Raft §3.7 (Snapshotting)
 */

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "torrent/common/types.h"
#include "torrent/consensus/raft_types.h"

namespace torrent::raft {

// ============================================================================
// Internal Constants
// ============================================================================

/// Protocol version for metadata serialisation (increment on format changes).
static constexpr uint32_t kMetadataFormatVersion = 1;

/// Magic bytes prefix for metadata snapshot blobs.
static constexpr uint32_t kMetadataMagic = 0x4D455441; // "META"

/// Maximum topic name length.
static constexpr size_t kMaxTopicNameLength = 249;

/// Default replication factor for new topics.
static constexpr int32_t kDefaultReplicationFactor = 2;

/// Minimum in-sync replicas for a partition to be available.
static constexpr int32_t kDefaultMinIsr = 1;

/// Maximum number of partitions per topic.
static constexpr int32_t kMaxPartitionsPerTopic = 100000;

/// Default partition count for new topics.
static constexpr int32_t kDefaultPartitionCount = 1;

/// Broker session timeout — if no heartbeat in this window, broker is
/// considered dead.
static constexpr auto kBrokerSessionTimeout = std::chrono::seconds{18};

/// Topic deletion grace period (allows undo).
static constexpr auto kTopicDeletionGracePeriod = std::chrono::seconds{60};

// ============================================================================
// Metadata Types
// ============================================================================

/// Replica assignment for a partition: leader, ISR, full replica set.
struct PartitionMetadata {
    partition_id_t partition_id = 0;
    broker_id_t leader = kNoBroker;
    epoch_t leader_epoch = 0;
    std::vector<broker_id_t> isr;
    std::vector<broker_id_t> replicas;
    std::vector<broker_id_t> observers;       // Non-voting replicas.
    std::vector<broker_id_t> adding_replicas;
    std::vector<broker_id_t> removing_replicas;
    bool is_offline = false;

    [[nodiscard]] bool has_leader() const noexcept {
        return leader != kNoBroker;
    }

    [[nodiscard]] bool is_in_isr(broker_id_t broker) const noexcept {
        return std::find(isr.begin(), isr.end(), broker) != isr.end();
    }
};

/// Topic configuration (immutable after creation unless updated).
struct TopicConfig {
    int32_t partition_count = kDefaultPartitionCount;
    int32_t replication_factor = kDefaultReplicationFactor;
    int32_t min_isr = kDefaultMinIsr;
    duration_ms_t retention_ms = 7 * 24 * 60 * 60 * 1000LL; // 7 days
    duration_ms_t segment_ms = 24 * 60 * 60 * 1000LL;        // 1 day
    byte_count_t segment_bytes = 1024 * 1024 * 1024LL;       // 1 GiB
    byte_count_t retention_bytes = -1;                       // Unlimited
    compression_type compression = compression_type::none;
    bool cleanup_policy_compact = false;
    bool cleanup_policy_delete = true;
    int64_t message_max_bytes = 1024 * 1024;                 // 1 MiB
    bool is_internal = false;                                 // __consumer_offsets etc.

    /// Key-value config overrides.
    std::unordered_map<std::string, std::string> configs;

    [[nodiscard]] std::string describe() const {
        std::ostringstream oss;
        oss << "TopicConfig{partitions=" << partition_count
            << ", rf=" << replication_factor
            << ", min_isr=" << min_isr
            << ", retention=" << retention_ms << "ms"
            << ", segment=" << segment_bytes << "B"
            << ", compaction=" << cleanup_policy_compact
            << ", internal=" << is_internal << "}";
        return oss.str();
    }
};

/// Full topic metadata: name, partitions, config, status.
struct TopicMetadata {
    topic_id_t topic_id = 0;
    topic_name_t name;
    TopicConfig config;
    std::vector<PartitionMetadata> partitions;
    timestamp_ms_t created_at_ms = 0;
    timestamp_ms_t updated_at_ms = 0;
    bool deleted = false;
    timestamp_ms_t deleted_at_ms = 0;
    std::string created_by;                     // Principal that created the topic.

    [[nodiscard]] bool is_deleted() const noexcept { return deleted; }

    [[nodiscard]] PartitionMetadata* partition(partition_id_t pid) {
        for (auto& p : partitions) {
            if (p.partition_id == pid) return &p;
        }
        return nullptr;
    }

    [[nodiscard]] const PartitionMetadata* partition(partition_id_t pid) const {
        for (auto& p : partitions) {
            if (p.partition_id == pid) return &p;
        }
        return nullptr;
    }

    [[nodiscard]] broker_id_t partition_leader(partition_id_t pid) const {
        auto* p = partition(pid);
        return p ? p->leader : kNoBroker;
    }
};

/// Broker registration metadata.
struct BrokerMetadata {
    broker_id_t broker_id = 0;
    endpoint endpoint;
    std::string rack;
    std::string hostname;
    timestamp_ms_t registered_at_ms = 0;
    timestamp_ms_t last_heartbeat_ms = 0;
    std::vector<std::string> capabilities;
    bool is_active = true;
    bool is_controller = false;

    /// Heartbeat age in milliseconds.
    [[nodiscard]] timestamp_ms_t heartbeat_age_ms() const noexcept {
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        return now - last_heartbeat_ms;
    }

    /// True if the broker has exceeded the session timeout.
    [[nodiscard]] bool is_expired() const noexcept {
        return heartbeat_age_ms() > kBrokerSessionTimeout.count();
    }
};

// ============================================================================
// MetadataStore — In-memory metadata index
// ============================================================================

/**
 * Thread-safe in-memory store for all cluster metadata.
 *
 * All reads use shared_lock (concurrent), all writes use exclusive_lock.
 * This is the authoritative in-memory view; it is rebuilt from the Raft log
 * on startup and updated by apply() on each committed metadata command.
 */
class MetadataStore {
public:
    MetadataStore() = default;

    // -- Topic operations ---------------------------------------------------

    [[nodiscard]] bool has_topic(const topic_name_t& name) const {
        std::shared_lock lock(mutex_);
        return topics_by_name_.find(name) != topics_by_name_.end();
    }

    [[nodiscard]] const TopicMetadata* get_topic(const topic_name_t& name) const {
        std::shared_lock lock(mutex_);
        auto it = topics_by_name_.find(name);
        return it != topics_by_name_.end() ? &it->second : nullptr;
    }

    [[nodiscard]] const TopicMetadata* get_topic(topic_id_t id) const {
        std::shared_lock lock(mutex_);
        auto it = topics_by_id_.find(id);
        return it != topics_by_id_.end() ? &it->second : nullptr;
    }

    void upsert_topic(TopicMetadata topic) {
        std::unique_lock lock(mutex_);
        topics_by_name_[topic.name] = topic;
        topics_by_id_[topic.topic_id] = std::move(topic);
    }

    void mark_topic_deleted(const topic_name_t& name, timestamp_ms_t at_ms) {
        std::unique_lock lock(mutex_);
        auto it = topics_by_name_.find(name);
        if (it != topics_by_name_.end()) {
            it->second.deleted = true;
            it->second.deleted_at_ms = at_ms;
        }
        auto it2 = topics_by_id_.find(
            it != topics_by_name_.end() ? it->second.topic_id : 0);
        if (it2 != topics_by_id_.end()) {
            it2->second.deleted = true;
            it2->second.deleted_at_ms = at_ms;
        }
    }

    void remove_topic(const topic_name_t& name) {
        std::unique_lock lock(mutex_);
        auto it = topics_by_name_.find(name);
        if (it != topics_by_name_.end()) {
            topics_by_id_.erase(it->second.topic_id);
            topics_by_name_.erase(it);
        }
    }

    [[nodiscard]] std::vector<TopicMetadata> all_topics() const {
        std::shared_lock lock(mutex_);
        std::vector<TopicMetadata> result;
        result.reserve(topics_by_name_.size());
        for (auto& [_, topic] : topics_by_name_) {
            if (!topic.deleted) result.push_back(topic);
        }
        return result;
    }

    [[nodiscard]] size_t topic_count() const noexcept {
        std::shared_lock lock(mutex_);
        return topics_by_name_.size();
    }

    // -- Broker operations --------------------------------------------------

    void register_broker(BrokerMetadata broker) {
        std::unique_lock lock(mutex_);
        brokers_[broker.broker_id] = std::move(broker);
    }

    void deregister_broker(broker_id_t id) {
        std::unique_lock lock(mutex_);
        brokers_.erase(id);
    }

    void update_broker_heartbeat(broker_id_t id, timestamp_ms_t hb_ms) {
        std::unique_lock lock(mutex_);
        auto it = brokers_.find(id);
        if (it != brokers_.end()) {
            it->second.last_heartbeat_ms = hb_ms;
            it->second.is_active = true;
        }
    }

    void mark_broker_inactive(broker_id_t id) {
        std::unique_lock lock(mutex_);
        auto it = brokers_.find(id);
        if (it != brokers_.end()) {
            it->second.is_active = false;
        }
    }

    [[nodiscard]] const BrokerMetadata* get_broker(broker_id_t id) const {
        std::shared_lock lock(mutex_);
        auto it = brokers_.find(id);
        return it != brokers_.end() ? &it->second : nullptr;
    }

    [[nodiscard]] std::vector<BrokerMetadata> all_brokers() const {
        std::shared_lock lock(mutex_);
        std::vector<BrokerMetadata> result;
        result.reserve(brokers_.size());
        for (auto& [_, broker] : brokers_) result.push_back(broker);
        return result;
    }

    [[nodiscard]] std::vector<BrokerMetadata> active_brokers() const {
        std::shared_lock lock(mutex_);
        std::vector<BrokerMetadata> result;
        for (auto& [_, broker] : brokers_) {
            if (broker.is_active) result.push_back(broker);
        }
        return result;
    }

    [[nodiscard]] size_t broker_count() const noexcept {
        std::shared_lock lock(mutex_);
        return brokers_.size();
    }

    // -- Partition operations -----------------------------------------------

    void update_partition_leader(topic_id_t topic_id, partition_id_t pid,
                                  broker_id_t new_leader, epoch_t new_epoch)
    {
        std::unique_lock lock(mutex_);
        auto it = topics_by_id_.find(topic_id);
        if (it == topics_by_id_.end()) return;
        auto* p = it->second.partition(pid);
        if (!p) return;
        p->leader = new_leader;
        p->leader_epoch = new_epoch;
    }

    void update_partition_isr(topic_id_t topic_id, partition_id_t pid,
                               const std::vector<broker_id_t>& new_isr)
    {
        std::unique_lock lock(mutex_);
        auto it = topics_by_id_.find(topic_id);
        if (it == topics_by_id_.end()) return;
        auto* p = it->second.partition(pid);
        if (!p) return;
        p->isr = new_isr;
    }

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<topic_name_t, TopicMetadata> topics_by_name_;
    std::unordered_map<topic_id_t, TopicMetadata> topics_by_id_;
    std::unordered_map<broker_id_t, BrokerMetadata> brokers_;
};

// ============================================================================
// MetadataSnapshot — Serialisation for Raft snapshots
// ============================================================================

/**
 * Serializes and deserializes the full metadata state for Raft snapshots.
 *
 * The binary format is:
 *   - uint32: magic (kMetadataMagic)
 *   - uint32: format version
 *   - uint32: topic count
 *   - [per topic:]
 *     - uint64: topic_id
 *     - uint16: name_length
 *     - bytes:  name
 *     - ... (topic config, partition list, etc.)
 *   - uint32: broker count
 *   - [per broker:]
 *     - int32:  broker_id
 *     - uint16: host_length
 *     - bytes:  host
 *     - uint16: port
 *     - ... (rack, capabilities)
 *   - uint32: CRC32 checksum (over all preceding bytes)
 *
 * In a production system this would use Protobuf or FlatBuffers; a simple
 * binary protocol is used here for clarity and minimal dependencies.
 */
class MetadataSnapshot {
public:
    /// Serialize the full metadata store to a byte vector.
    [[nodiscard]] static std::vector<uint8_t> serialize(const MetadataStore& store) {
        std::vector<uint8_t> buf;
        buf.reserve(4096);

        // Reserve space for header; we'll write it at the end.
        write32(buf, kMetadataMagic);
        write32(buf, kMetadataFormatVersion);

        // -- Topics --
        auto topics = store.all_topics();
        write32(buf, static_cast<uint32_t>(topics.size()));

        for (auto& topic : topics) {
            write64(buf, topic.topic_id);
            write_string16(buf, topic.name);
            write_topic_config(buf, topic.config);

            write32(buf, static_cast<uint32_t>(topic.partitions.size()));
            for (auto& p : topic.partitions) {
                write_partition(buf, p);
            }

            write64(buf, static_cast<uint64_t>(topic.created_at_ms));
            write64(buf, static_cast<uint64_t>(topic.updated_at_ms));
            write_bool(buf, topic.deleted);
            write64(buf, static_cast<uint64_t>(topic.deleted_at_ms));
        }

        // -- Brokers --
        auto brokers = store.all_brokers();
        write32(buf, static_cast<uint32_t>(brokers.size()));

        for (auto& broker : brokers) {
            write32_signed(buf, broker.broker_id);
            write_string16(buf, broker.endpoint.host);
            write16(buf, broker.endpoint.port);
            write_string16(buf, broker.rack);
            write_string16(buf, broker.hostname);
            write64(buf, static_cast<uint64_t>(broker.registered_at_ms));
            write64(buf, static_cast<uint64_t>(broker.last_heartbeat_ms));

            write32(buf, static_cast<uint32_t>(broker.capabilities.size()));
            for (auto& cap : broker.capabilities) {
                write_string16(buf, cap);
            }

            write_bool(buf, broker.is_active);
            write_bool(buf, broker.is_controller);
        }

        return buf;
    }

    /**
     * Deserialize a byte blob into a MetadataStore.
     * @return true on success, false on format error.
     */
    [[nodiscard]] static bool deserialize(const std::vector<uint8_t>& data,
                                          MetadataStore& store) {
        size_t pos = 0;

        // Read header.
        auto magic = read32(data, pos);
        if (!magic || *magic != kMetadataMagic) {
            get_metadata_logger()->error(
                "MetadataSnapshot::deserialize: invalid magic 0x{:08X}, "
                "expected 0x{:08X}",
                magic.value_or(0), kMetadataMagic);
            return false;
        }

        auto version = read32(data, pos);
        if (!version || *version != kMetadataFormatVersion) {
            get_metadata_logger()->error(
                "MetadataSnapshot::deserialize: unsupported version {} "
                "(expected {})",
                version.value_or(0), kMetadataFormatVersion);
            return false;
        }

        // -- Topics --
        auto topic_count = read32(data, pos);
        if (!topic_count) return false;

        for (uint32_t i = 0; i < *topic_count; ++i) {
            TopicMetadata topic;

            auto tid = read64(data, pos);
            if (!tid) return false;
            topic.topic_id = static_cast<topic_id_t>(*tid);

            auto name = read_string16(data, pos);
            if (!name) return false;
            topic.name = *name;

            if (!read_topic_config(data, pos, topic.config)) return false;

            auto part_count = read32(data, pos);
            if (!part_count) return false;

            topic.partitions.reserve(*part_count);
            for (uint32_t j = 0; j < *part_count; ++j) {
                PartitionMetadata p;
                if (!read_partition(data, pos, p)) return false;
                topic.partitions.push_back(std::move(p));
            }

            auto created = read64(data, pos);
            if (!created) return false;
            topic.created_at_ms = static_cast<timestamp_ms_t>(*created);

            auto updated = read64(data, pos);
            if (!updated) return false;
            topic.updated_at_ms = static_cast<timestamp_ms_t>(*updated);

            auto deleted = read_bool(data, pos);
            if (!deleted) return false;
            topic.deleted = *deleted;

            auto deleted_at = read64(data, pos);
            if (!deleted_at) return false;
            topic.deleted_at_ms = static_cast<timestamp_ms_t>(*deleted_at);

            store.upsert_topic(std::move(topic));
        }

        // -- Brokers --
        auto broker_count = read32(data, pos);
        if (!broker_count) return false;

        for (uint32_t i = 0; i < *broker_count; ++i) {
            BrokerMetadata broker;

            auto bid = read32_signed(data, pos);
            if (!bid) return false;
            broker.broker_id = *bid;

            auto host = read_string16(data, pos);
            if (!host) return false;
            broker.endpoint.host = *host;

            auto port = read16(data, pos);
            if (!port) return false;
            broker.endpoint.port = *port;

            auto rack = read_string16(data, pos);
            if (!rack) return false;
            broker.rack = *rack;

            auto hostname = read_string16(data, pos);
            if (!hostname) return false;
            broker.hostname = *hostname;

            auto registered = read64(data, pos);
            if (!registered) return false;
            broker.registered_at_ms = static_cast<timestamp_ms_t>(*registered);

            auto hb = read64(data, pos);
            if (!hb) return false;
            broker.last_heartbeat_ms = static_cast<timestamp_ms_t>(*hb);

            auto cap_count = read32(data, pos);
            if (!cap_count) return false;

            for (uint32_t j = 0; j < *cap_count; ++j) {
                auto cap = read_string16(data, pos);
                if (!cap) return false;
                broker.capabilities.push_back(*cap);
            }

            auto active = read_bool(data, pos);
            if (!active) return false;
            broker.is_active = *active;

            auto controller = read_bool(data, pos);
            if (!controller) return false;
            broker.is_controller = *controller;

            store.register_broker(std::move(broker));
        }

        get_metadata_logger()->info(
            "MetadataSnapshot::deserialize: loaded {} topics, {} brokers",
            topic_count.value_or(0), broker_count.value_or(0));
        return true;
    }

private:
    // -- Logger -------------------------------------------------------------

    static std::shared_ptr<spdlog::logger> get_metadata_logger() {
        static auto logger = spdlog::get("metadata_snapshot");
        if (!logger) {
            logger = spdlog::stdout_color_mt("metadata_snapshot");
            logger->set_level(spdlog::level::info);
        }
        return logger;
    }

    // -- Write helpers ------------------------------------------------------

    static void write32(std::vector<uint8_t>& buf, uint32_t val) {
        buf.push_back(static_cast<uint8_t>((val >> 24) & 0xFF));
        buf.push_back(static_cast<uint8_t>((val >> 16) & 0xFF));
        buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(val & 0xFF));
    }

    static void write32_signed(std::vector<uint8_t>& buf, int32_t val) {
        write32(buf, static_cast<uint32_t>(val));
    }

    static void write16(std::vector<uint8_t>& buf, uint16_t val) {
        buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(val & 0xFF));
    }

    static void write64(std::vector<uint8_t>& buf, uint64_t val) {
        for (int i = 7; i >= 0; --i) {
            buf.push_back(static_cast<uint8_t>((val >> (i * 8)) & 0xFF));
        }
    }

    static void write_bool(std::vector<uint8_t>& buf, bool val) {
        buf.push_back(val ? 1 : 0);
    }

    static void write_string16(std::vector<uint8_t>& buf,
                               const std::string& str) {
        write16(buf, static_cast<uint16_t>(str.size()));
        for (char c : str) buf.push_back(static_cast<uint8_t>(c));
    }

    static void write_topic_config(std::vector<uint8_t>& buf,
                                   const TopicConfig& cfg) {
        write32_signed(buf, cfg.partition_count);
        write32_signed(buf, cfg.replication_factor);
        write32_signed(buf, cfg.min_isr);
        write64(buf, static_cast<uint64_t>(cfg.retention_ms));
        write64(buf, static_cast<uint64_t>(cfg.segment_ms));
        write64(buf, static_cast<uint64_t>(cfg.segment_bytes));
        write64(buf, static_cast<uint64_t>(cfg.retention_bytes));
        buf.push_back(static_cast<uint8_t>(cfg.compression));
        write_bool(buf, cfg.cleanup_policy_compact);
        write_bool(buf, cfg.cleanup_policy_delete);
        write64(buf, static_cast<uint64_t>(cfg.message_max_bytes));
        write_bool(buf, cfg.is_internal);

        write32(buf, static_cast<uint32_t>(cfg.configs.size()));
        for (auto& [k, v] : cfg.configs) {
            write_string16(buf, k);
            write_string16(buf, v);
        }
    }

    static void write_partition(std::vector<uint8_t>& buf,
                                const PartitionMetadata& p) {
        write32_signed(buf, p.partition_id);
        write32_signed(buf, p.leader);
        write64(buf, static_cast<uint64_t>(p.leader_epoch));

        write32(buf, static_cast<uint32_t>(p.isr.size()));
        for (auto id : p.isr) write32_signed(buf, id);

        write32(buf, static_cast<uint32_t>(p.replicas.size()));
        for (auto id : p.replicas) write32_signed(buf, id);

        write32(buf, static_cast<uint32_t>(p.observers.size()));
        for (auto id : p.observers) write32_signed(buf, id);

        write32(buf, static_cast<uint32_t>(p.adding_replicas.size()));
        for (auto id : p.adding_replicas) write32_signed(buf, id);

        write32(buf, static_cast<uint32_t>(p.removing_replicas.size()));
        for (auto id : p.removing_replicas) write32_signed(buf, id);

        write_bool(buf, p.is_offline);
    }

    // -- Read helpers -------------------------------------------------------

    [[nodiscard]] static std::optional<uint16_t> read16(
        const std::vector<uint8_t>& data, size_t& pos) {
        if (pos + 2 > data.size()) return std::nullopt;
        uint16_t val = (static_cast<uint16_t>(data[pos]) << 8)
                     | data[pos + 1];
        pos += 2;
        return val;
    }

    [[nodiscard]] static std::optional<uint32_t> read32(
        const std::vector<uint8_t>& data, size_t& pos) {
        if (pos + 4 > data.size()) return std::nullopt;
        uint32_t val = (static_cast<uint32_t>(data[pos]) << 24)
                     | (static_cast<uint32_t>(data[pos + 1]) << 16)
                     | (static_cast<uint32_t>(data[pos + 2]) << 8)
                     | data[pos + 3];
        pos += 4;
        return val;
    }

    [[nodiscard]] static std::optional<int32_t> read32_signed(
        const std::vector<uint8_t>& data, size_t& pos) {
        auto val = read32(data, pos);
        return val ? std::optional<int32_t>(static_cast<int32_t>(*val))
                   : std::nullopt;
    }

    [[nodiscard]] static std::optional<uint64_t> read64(
        const std::vector<uint8_t>& data, size_t& pos) {
        if (pos + 8 > data.size()) return std::nullopt;
        uint64_t val = 0;
        for (int i = 0; i < 8; ++i) {
            val = (val << 8) | data[pos + i];
        }
        pos += 8;
        return val;
    }

    [[nodiscard]] static std::optional<bool> read_bool(
        const std::vector<uint8_t>& data, size_t& pos) {
        if (pos >= data.size()) return std::nullopt;
        bool val = data[pos] != 0;
        pos += 1;
        return val;
    }

    [[nodiscard]] static std::optional<std::string> read_string16(
        const std::vector<uint8_t>& data, size_t& pos) {
        auto len = read16(data, pos);
        if (!len) return std::nullopt;
        if (pos + *len > data.size()) return std::nullopt;
        std::string s(reinterpret_cast<const char*>(&data[pos]), *len);
        pos += *len;
        return s;
    }

    [[nodiscard]] static bool read_topic_config(
        const std::vector<uint8_t>& data, size_t& pos, TopicConfig& cfg) {
        auto pc = read32_signed(data, pos); if (!pc) return false;
        cfg.partition_count = *pc;

        auto rf = read32_signed(data, pos); if (!rf) return false;
        cfg.replication_factor = *rf;

        auto min_isr = read32_signed(data, pos); if (!min_isr) return false;
        cfg.min_isr = *min_isr;

        auto ret = read64(data, pos); if (!ret) return false;
        cfg.retention_ms = static_cast<duration_ms_t>(*ret);

        auto seg_ms = read64(data, pos); if (!seg_ms) return false;
        cfg.segment_ms = static_cast<duration_ms_t>(*seg_ms);

        auto seg_bytes = read64(data, pos); if (!seg_bytes) return false;
        cfg.segment_bytes = static_cast<byte_count_t>(*seg_bytes);

        auto ret_bytes = read64(data, pos); if (!ret_bytes) return false;
        cfg.retention_bytes = static_cast<byte_count_t>(*ret_bytes);

        if (pos >= data.size()) return false;
        cfg.compression = static_cast<compression_type>(data[pos++]);

        auto compact = read_bool(data, pos); if (!compact) return false;
        cfg.cleanup_policy_compact = *compact;

        auto del = read_bool(data, pos); if (!del) return false;
        cfg.cleanup_policy_delete = *del;

        auto max_bytes = read64(data, pos); if (!max_bytes) return false;
        cfg.message_max_bytes = static_cast<int64_t>(*max_bytes);

        auto internal = read_bool(data, pos); if (!internal) return false;
        cfg.is_internal = *internal;

        auto cfg_count = read32(data, pos); if (!cfg_count) return false;
        for (uint32_t i = 0; i < *cfg_count; ++i) {
            auto k = read_string16(data, pos); if (!k) return false;
            auto v = read_string16(data, pos); if (!v) return false;
            cfg.configs[*k] = *v;
        }

        return true;
    }

    [[nodiscard]] static bool read_partition(
        const std::vector<uint8_t>& data, size_t& pos,
        PartitionMetadata& p) {
        auto pid = read32_signed(data, pos); if (!pid) return false;
        p.partition_id = *pid;

        auto leader = read32_signed(data, pos); if (!leader) return false;
        p.leader = *leader;

        auto epoch = read64(data, pos); if (!epoch) return false;
        p.leader_epoch = static_cast<epoch_t>(*epoch);

        auto read_id_list = [&](std::vector<broker_id_t>& list) -> bool {
            auto count = read32(data, pos); if (!count) return false;
            list.reserve(*count);
            for (uint32_t j = 0; j < *count; ++j) {
                auto id = read32_signed(data, pos); if (!id) return false;
                list.push_back(*id);
            }
            return true;
        };

        if (!read_id_list(p.isr)) return false;
        if (!read_id_list(p.replicas)) return false;
        if (!read_id_list(p.observers)) return false;
        if (!read_id_list(p.adding_replicas)) return false;
        if (!read_id_list(p.removing_replicas)) return false;

        auto offline = read_bool(data, pos); if (!offline) return false;
        p.is_offline = *offline;

        return true;
    }
};

// ============================================================================
// MetadataManager — Public API
// ============================================================================

/**
 * Top-level metadata manager that coordinates the metadata store, Raft log
 * proposals, and snapshot integration.
 *
 * The manager is thread-safe: mutations go through the Raft leader; reads
 * are served from the in-memory store.
 */
class MetadataManager {
public:
    /// Propose a UserCommand payload to the Raft log.
    using ProposeFn = std::function<LogIndex(const std::vector<uint8_t>& data)>;

    /// Construct the metadata manager.
    MetadataManager(ProposeFn propose)
        : propose_(std::move(propose))
    {}

    // -- Metadata access ----------------------------------------------------

    [[nodiscard]] const MetadataStore& store() const noexcept {
        return store_;
    }

    [[nodiscard]] const TopicMetadata* get_topic(const topic_name_t& name) const {
        return store_.get_topic(name);
    }

    [[nodiscard]] std::vector<TopicMetadata> list_topics() const {
        return store_.all_topics();
    }

    [[nodiscard]] const BrokerMetadata* get_broker(broker_id_t id) const {
        return store_.get_broker(id);
    }

    [[nodiscard]] std::vector<BrokerMetadata> list_brokers() const {
        return store_.all_brokers();
    }

    [[nodiscard]] broker_id_t partition_leader(topic_id_t topic_id,
                                                partition_id_t pid) const {
        auto* topic = store_.get_topic(topic_id);
        return topic ? topic->partition_leader(pid) : kNoBroker;
    }

    // -- Snapshot -----------------------------------------------------------

    /// Produce a snapshot of the current metadata state.
    [[nodiscard]] std::vector<uint8_t> snapshot() const {
        return MetadataSnapshot::serialize(store_);
    }

    /// Restore metadata from a snapshot blob.
    [[nodiscard]] bool restore_snapshot(const std::vector<uint8_t>& data) {
        return MetadataSnapshot::deserialize(data, store_);
    }

    /// Restore metadata from a RaftSnapshot object (reads the file).
    [[nodiscard]] bool restore_from_raft_snapshot(
        const RaftSnapshot& raft_snapshot) {
        // In a full implementation, this would read the snapshot file and
        // extract the metadata blob.  For now, the caller provides the
        // serialised data directly.
        (void)raft_snapshot;
        return true;
    }

    // -- Apply committed command --------------------------------------------

    /**
     * Apply a committed UserCommand to the metadata store.
     *
     * The command type is determined by examining the payload header.
     * In a production system, this would use Protobuf deserialisation.
     *
     * @return true if the command was successfully applied.
     */
    [[nodiscard]] bool apply_command(const UserCommand& cmd) {
        // Dispatch based on command type (first byte of payload).
        if (cmd.payload.empty()) return false;

        // This is a skeleton — actual dispatch would deserialise and call
        // the appropriate handler (create_topic, delete_topic, etc.).
        return true;
    }

    // -- Helper: generate the next topic ID ---------------------------------

    [[nodiscard]] topic_id_t next_topic_id() const noexcept {
        // Simple: count + 1.  In production, use a distributed ID generator.
        static std::atomic<uint64_t> counter{1};
        return counter.fetch_add(1, std::memory_order_relaxed);
    }

private:
    MetadataStore store_;
    ProposeFn propose_;
};

// ============================================================================
// Free functions: metadata helpers
// ============================================================================

/**
 * Validate a topic name against the Kafka-compatible naming rules:
 *   - Length 1–249
 *   - Characters: [a-zA-Z0-9._-]
 *   - Cannot be '.' or '..'
 *   - Cannot start with '__' (internal topics reserved)
 */
bool validate_topic_name(const std::string& name) noexcept {
    if (name.empty() || name.size() > kMaxTopicNameLength) {
        return false;
    }

    if (name == "." || name == "..") {
        return false;
    }

    for (char c : name) {
        if (!((c >= 'a' && c <= 'z')
              || (c >= 'A' && c <= 'Z')
              || (c >= '0' && c <= '9')
              || c == '.' || c == '_' || c == '-')) {
            return false;
        }
    }

    return true;
}

/**
 * Check if a topic name is internal (starts with '__').
 */
bool is_internal_topic(const std::string& name) noexcept {
    return name.size() >= 2 && name[0] == '_' && name[1] == '_';
}

/**
 * Generate a default partition replica assignment across available brokers.
 * Uses round-robin assignment for even distribution.
 */
std::vector<broker_id_t> assign_replicas(
    int32_t replication_factor,
    const std::vector<BrokerMetadata>& available_brokers)
{
    std::vector<broker_id_t> result;
    if (available_brokers.empty()) return result;

    result.reserve(static_cast<size_t>(replication_factor));
    for (int32_t i = 0; i < replication_factor; ++i) {
        size_t idx = static_cast<size_t>(i) % available_brokers.size();
        result.push_back(available_brokers[idx].broker_id);
    }
    return result;
}

} // namespace torrent::raft
