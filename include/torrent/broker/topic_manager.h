#pragma once
#include <memory>
#include <string>
#include <vector>
#include <optional>
#include <torrent/common/types.h>

namespace torrent::broker {
class PartitionManager; class SubscriptionManager; class LogManager;
class ConsumerGroupManager; class RequestDispatcher; class InterBroker;
class Controller; class QuotaManager; class RetentionManager;
class CompactionManager; class TransactionCoordinator; class LeaderBalancer;

struct TopicConfig { std::string name; int32_t partitions=1; int32_t replication_factor=3; };
struct TopicMetadata { std::string name; topic_id_t id{}; std::vector<partition_id_t> partitions; error_code error{error_code::none}; };

class TopicManager {
public:
    explicit TopicManager(class BrokerServer& server);
    ~TopicManager();
    result<TopicMetadata> create_topic(const std::string& name, int32_t partitions, int32_t replication_factor);
    result<TopicMetadata> delete_topic(const std::string& name);
    std::optional<TopicMetadata> get_topic(const std::string& name) const;
    std::vector<TopicMetadata> list_topics() const;
    bool topic_exists(const std::string& name) const;
    int32_t partition_count(const std::string& name) const;
    result<void> alter_topic_config(const std::string& name, const std::string& key, const std::string& value);
private:
    BrokerServer* server_;
};

class PartitionManager {
public:
    explicit PartitionManager(class BrokerServer& server);
    ~PartitionManager();
    result<partition_id_t> create_partition(const std::string& topic, partition_id_t partition);
    result<void> delete_partition(const std::string& topic, partition_id_t partition);
    bool is_leader(const std::string& topic, partition_id_t partition) const;
    broker_id_t leader_for(const std::string& topic, partition_id_t partition) const;
    std::vector<broker_id_t> replicas_for(const std::string& topic, partition_id_t partition) const;
private:
    BrokerServer* server_;
};
} // namespace torrent::broker
