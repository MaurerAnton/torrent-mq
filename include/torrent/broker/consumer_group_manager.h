#pragma once
#include <memory>
#include <string>
#include <vector>
#include <torrent/common/types.h>

namespace torrent::broker {
class ConsumerGroupManager {
public:
    explicit ConsumerGroupManager(class BrokerServer& s) : server_(&s) {}
    result<void> join_group(const std::string& group_id, const std::string& member_id, const std::string& protocol);
    result<void> leave_group(const std::string& group_id, const std::string& member_id);
    result<void> sync_group(const std::string& group_id, const std::string& member_id);
    result<void> heartbeat(const std::string& group_id, const std::string& member_id, int32_t generation);
    result<void> commit_offset(const std::string& group_id, const std::string& topic, partition_id_t partition, offset_t offset);
    offset_t fetch_offset(const std::string& group_id, const std::string& topic, partition_id_t partition) const;
private:
    BrokerServer* server_;
};
}