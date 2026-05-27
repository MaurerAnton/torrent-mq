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

struct TopicConfig {
    std::string name;
    int32_t num_partitions = 1;
    int32_t partitions = 1;
    int32_t replication_factor = 3;
    int64_t retention_ms = 604800000;      // 7 days
    int64_t segment_bytes = 1073741824;    // 1 GB
    int64_t retention_bytes = -1;          // -1 = no size limit
    int64_t max_message_bytes = 1048576;   // 1 MB
    bool has_size_retention = false;
    bool is_internal = false;
    int32_t min_insync_replicas = 1;
    bool cleanup_policy_compact = false;
    bool cleanup_policy_delete = true;
    std::string policy = "delete";
};
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

} // namespace torrent::broker
