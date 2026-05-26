#pragma once
#include <memory>
#include <torrent/common/types.h>

namespace torrent::broker {
class RetentionManager {
public:
    explicit RetentionManager(class BrokerServer& s) : server_(&s) {}
    void start();
    void shutdown();
    void check_retention();
    void check_compaction();
private:
    BrokerServer* server_;
};
}