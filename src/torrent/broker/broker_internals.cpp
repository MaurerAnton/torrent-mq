#include "torrent/broker/server.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/broker/partition_manager.h"
#include "torrent/broker/consumer_group_manager.h"
#include "torrent/broker/request_dispatcher.h"
#include "torrent/broker/controller.h"
#include "torrent/common/types.h"
#include "torrent/network/protocol.h"
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <random>

namespace torrent::broker {
namespace {

using json = nlohmann::json;

// ============================================================================
// Topic Manager Full Implementation
// ============================================================================

class TopicManagerImpl {
public:
    explicit TopicManagerImpl(BrokerServer& server) : server_(server) {}

    result<TopicMetadata> create_topic(const std::string& name,
                                        int32_t partitions,
                                        int32_t replication_factor) {
        if (name.empty()) {
            return result<TopicMetadata>::failure(error_code::invalid_topic_exception,
                                                   "Topic name cannot be empty");
        }
        if (name.size() > 249) {
            return result<TopicMetadata>::failure(error_code::invalid_topic_exception,
                                                   "Topic name exceeds 249 characters");
        }
        if (name.find('.') == 0 || name.find("__") == 0) {
            return result<TopicMetadata>::failure(error_code::invalid_topic_exception,
                                                   "Topic name cannot start with . or __");
        }
        if (partitions < 1 || partitions > 100000) {
            return result<TopicMetadata>::failure(error_code::invalid_partitions,
                                                   "Partitions must be between 1 and 100000");
        }
        if (replication_factor < 1 || replication_factor > 10) {
            return result<TopicMetadata>::failure(error_code::invalid_replication_factor,
                                                   "Replication factor must be between 1 and 10");
        }

        std::unique_lock lock(mutex_);
        if (topics_.find(name) != topics_.end()) {
            return result<TopicMetadata>::failure(error_code::topic_already_exists,
                                                   "Topic '" + name + "' already exists");
        }

        TopicMetadata meta;
        meta.name = name;
        meta.id = next_topic_id_++;
        meta.error = error_code::none;

        for (int32_t p = 0; p < partitions; p++) {
            meta.partitions.push_back(p);
        }

        topics_[name] = meta;
        spdlog::info("Topic created: {} ({} partitions, RF={})", name, partitions, replication_factor);
        return result<TopicMetadata>::success(meta);
    }

    result<TopicMetadata> delete_topic(const std::string& name) {
        std::unique_lock lock(mutex_);
        auto it = topics_.find(name);
        if (it == topics_.end()) {
            return result<TopicMetadata>::failure(error_code::unknown_topic_or_partition,
                                                   "Topic not found");
        }
        auto meta = it->second;
        topics_.erase(it);
        spdlog::info("Topic deleted: {}", name);
        return result<TopicMetadata>::success(meta);
    }

    std::optional<TopicMetadata> get_topic(const std::string& name) const {
        std::shared_lock lock(mutex_);
        auto it = topics_.find(name);
        if (it == topics_.end()) return std::nullopt;
        return it->second;
    }

    std::vector<TopicMetadata> list_topics() const {
        std::shared_lock lock(mutex_);
        std::vector<TopicMetadata> result;
        for (const auto& [name, meta] : topics_) {
            result.push_back(meta);
        }
        return result;
    }

    bool topic_exists(const std::string& name) const {
        std::shared_lock lock(mutex_);
        return topics_.find(name) != topics_.end();
    }

    int32_t partition_count(const std::string& name) const {
        auto meta = get_topic(name);
        return meta.has_value() ? static_cast<int32_t>(meta->partitions.size()) : 0;
    }

private:
    BrokerServer& server_;
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, TopicMetadata> topics_;
    topic_id_t next_topic_id_{1};
};

// ============================================================================
// Partition Manager Full Implementation
// ============================================================================

class PartitionManagerImpl {
public:
    explicit PartitionManagerImpl(BrokerServer& server) : server_(server) {}

    result<partition_id_t> create_partition(const std::string& topic,
                                              partition_id_t partition) {
        std::unique_lock lock(mutex_);
        auto key = make_key(topic, partition);
        if (partitions_.find(key) != partitions_.end()) {
            return result<partition_id_t>::failure(error_code::invalid_partitions,
                                                    "Partition already exists");
        }

        PartitionState state;
        state.leader_id = server_.broker_id();
        state.replicas = {server_.broker_id()};
        state.isr = {server_.broker_id()};
        partitions_[key] = state;

        spdlog::info("Partition created: {}/{}", topic, partition);
        return result<partition_id_t>::success(partition);
    }

    result<void> delete_partition(const std::string& topic, partition_id_t partition) {
        std::unique_lock lock(mutex_);
        auto key = make_key(topic, partition);
        if (partitions_.erase(key) == 0) {
            return result<void>::failure(error_code::unknown_topic_or_partition,
                                          "Partition not found");
        }
        return result<void>::success();
    }

    bool is_leader(const std::string& topic, partition_id_t partition) const {
        std::shared_lock lock(mutex_);
        auto it = partitions_.find(make_key(topic, partition));
        if (it == partitions_.end()) return false;
        return it->second.leader_id == server_.broker_id();
    }

    broker_id_t leader_for(const std::string& topic, partition_id_t partition) const {
        std::shared_lock lock(mutex_);
        auto it = partitions_.find(make_key(topic, partition));
        if (it == partitions_.end()) return kNoBroker;
        return it->second.leader_id;
    }

    std::vector<broker_id_t> replicas_for(const std::string& topic,
                                            partition_id_t partition) const {
        std::shared_lock lock(mutex_);
        auto it = partitions_.find(make_key(topic, partition));
        if (it == partitions_.end()) return {};
        return it->second.replicas;
    }

    void update_leader(const std::string& topic, partition_id_t partition,
                        broker_id_t new_leader) {
        std::unique_lock lock(mutex_);
        auto it = partitions_.find(make_key(topic, partition));
        if (it != partitions_.end()) {
            it->second.leader_id = new_leader;
            it->second.leader_epoch++;
        }
    }

    void shrink_isr(const std::string& topic, partition_id_t partition,
                     broker_id_t failed_broker) {
        std::unique_lock lock(mutex_);
        auto it = partitions_.find(make_key(topic, partition));
        if (it != partitions_.end()) {
            auto& isr = it->second.isr;
            isr.erase(std::remove(isr.begin(), isr.end(), failed_broker), isr.end());
        }
    }

    void expand_isr(const std::string& topic, partition_id_t partition,
                     broker_id_t recovered_broker) {
        std::unique_lock lock(mutex_);
        auto it = partitions_.find(make_key(topic, partition));
        if (it != partitions_.end()) {
            auto& isr = it->second.isr;
            if (std::find(isr.begin(), isr.end(), recovered_broker) == isr.end()) {
                isr.push_back(recovered_broker);
            }
        }
    }

private:
    static std::string make_key(const std::string& topic, partition_id_t partition) {
        return topic + ":" + std::to_string(partition);
    }

    BrokerServer& server_;
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, PartitionState> partitions_;
};

// ============================================================================
// Consumer Group Manager Full Implementation
// ============================================================================

class ConsumerGroupManagerImpl {
public:
    explicit ConsumerGroupManagerImpl(BrokerServer& server) : server_(server) {}

    result<void> join_group(const std::string& group_id,
                             const std::string& member_id,
                             const std::string& protocol) {
        std::unique_lock lock(mutex_);
        auto& group = groups_[group_id];
        group.group_id = group_id;
        group.state = group_state::STABLE;

        GroupMember member;
        member.member_id = member_id.empty() ? generate_member_id() : member_id;
        member.client_id = member_id;
        member.assignment = "{}";

        group.members[member.member_id] = member;
        group.generation++;
        group.leader_id = member.member_id;

        spdlog::info("Consumer group join: {} member={} generation={}",
                     group_id, member.member_id, group.generation);
        return result<void>::success();
    }

    result<void> leave_group(const std::string& group_id,
                              const std::string& member_id) {
        std::unique_lock lock(mutex_);
        auto it = groups_.find(group_id);
        if (it == groups_.end()) {
            return result<void>::failure(error_code::unknown_member_id, "Group not found");
        }
        it->second.members.erase(member_id);
        if (it->second.members.empty()) {
            groups_.erase(it);
        }
        spdlog::info("Consumer group leave: {} member={}", group_id, member_id);
        return result<void>::success();
    }

    result<void> heartbeat(const std::string& group_id,
                            const std::string& member_id,
                            int32_t generation) {
        std::shared_lock lock(mutex_);
        auto it = groups_.find(group_id);
        if (it == groups_.end()) {
            return result<void>::failure(error_code::unknown_member_id, "Group not found");
        }
        if (it->second.generation != generation) {
            return result<void>::failure(error_code::illegal_generation,
                                          "Generation mismatch");
        }
        return result<void>::success();
    }

    result<void> commit_offset(const std::string& group_id,
                                 const std::string& topic,
                                 partition_id_t partition,
                                 offset_t offset) {
        std::unique_lock lock(mutex_);
        auto key = group_id + ":" + topic + ":" + std::to_string(partition);
        offsets_[key] = offset;
        return result<void>::success();
    }

    offset_t fetch_offset(const std::string& group_id,
                            const std::string& topic,
                            partition_id_t partition) const {
        std::shared_lock lock(mutex_);
        auto key = group_id + ":" + topic + ":" + std::to_string(partition);
        auto it = offsets_.find(key);
        return it == offsets_.end() ? kInvalidOffset : it->second;
    }

    std::vector<ConsumerGroupState> list_groups() const {
        std::shared_lock lock(mutex_);
        std::vector<ConsumerGroupState> result;
        for (const auto& [id, group] : groups_) {
            result.push_back(group);
        }
        return result;
    }

    std::optional<ConsumerGroupState> describe_group(const std::string& group_id) const {
        std::shared_lock lock(mutex_);
        auto it = groups_.find(group_id);
        if (it == groups_.end()) return std::nullopt;
        return it->second;
    }

    result<void> delete_group(const std::string& group_id) {
        std::unique_lock lock(mutex_);
        auto it = groups_.find(group_id);
        if (it == groups_.end()) {
            return result<void>::failure(error_code::group_id_not_found, "Group not found");
        }
        if (!it->second.members.empty()) {
            return result<void>::failure(error_code::non_empty_group, "Group not empty");
        }
        groups_.erase(it);
        return result<void>::success();
    }

private:
    std::string generate_member_id() {
        static std::atomic<int64_t> counter{0};
        return "member-" + std::to_string(counter.fetch_add(1));
    }

    BrokerServer& server_;
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, ConsumerGroupState> groups_;
    std::unordered_map<std::string, offset_t> offsets_;
};

// ============================================================================
// Controller Full Implementation
// ============================================================================

class ControllerImpl {
public:
    explicit ControllerImpl(BrokerServer& server) : server_(server) {}

    void start() {
        running_.store(true);
        controller_id_.store(server_.broker_id());
        spdlog::info("Controller started on broker {}", controller_id_.load());
    }

    void shutdown() { running_.store(false); }

    bool is_controller() const noexcept {
        return controller_id_.load() == server_.broker_id();
    }

    broker_id_t controller_id() const noexcept { return controller_id_.load(); }
    epoch_t controller_epoch() const noexcept { return epoch_.load(); }

    void elect_controller() {
        // In production: use Raft leader election
        controller_id_.store(server_.broker_id());
        epoch_.fetch_add(1);
    }

    // Rack-aware partition assignment
    std::vector<broker_id_t> assign_replicas(int32_t replication_factor,
                                               const std::vector<BrokerMetadata>& brokers,
                                               const std::string& rack) {
        std::vector<broker_id_t> replicas;
        std::vector<BrokerMetadata> available = brokers;

        // First: prefer different racks
        std::vector<BrokerMetadata> different_rack;
        std::copy_if(available.begin(), available.end(),
                     std::back_inserter(different_rack),
                     [&rack](const BrokerMetadata& b) { return b.rack != rack; });

        std::vector<BrokerMetadata> same_rack;
        std::copy_if(available.begin(), available.end(),
                     std::back_inserter(same_rack),
                     [&rack](const BrokerMetadata& b) { return b.rack == rack; });

        // Alternate: different rack, same rack, different rack...
        size_t di = 0, si = 0;
        for (int32_t i = 0; i < replication_factor && replicas.size() < static_cast<size_t>(replication_factor); i++) {
            if (i % 2 == 0 && di < different_rack.size()) {
                replicas.push_back(different_rack[di++].node_id);
            } else if (si < same_rack.size()) {
                replicas.push_back(same_rack[si++].node_id);
            } else if (di < different_rack.size()) {
                replicas.push_back(different_rack[di++].node_id);
            }
        }
        return replicas;
    }

private:
    BrokerServer& server_;
    std::atomic<bool> running_{false};
    std::atomic<broker_id_t> controller_id_{kNoBroker};
    std::atomic<epoch_t> epoch_{0};
};

} // anonymous namespace
} // namespace torrent::broker
