#pragma once
#include <memory>
#include <torrent/common/types.h>

namespace torrent::broker {
class PartitionManager {
public:
    explicit PartitionManager(class BrokerServer& s);
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
