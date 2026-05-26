#pragma once
#include <memory>
#include <torrent/common/types.h>

namespace torrent::broker {
class CompactionManager {
public:
    explicit CompactionManager(class BrokerServer& s) : server_(&s) {}
    void start();
    void shutdown();
    void compact_partition(const std::string& topic, partition_id_t partition);
private:
    BrokerServer* server_;
};
}