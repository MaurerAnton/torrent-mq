#pragma once
#include <memory>
#include <string>
#include <torrent/common/types.h>

namespace torrent::broker {
class DistributedLockManager {
public:
    explicit DistributedLockManager(class BrokerServer& s) : server_(&s) {}
    bool try_lock(const std::string& lock_name, const std::string& owner, duration_ms_t timeout_ms);
    bool unlock(const std::string& lock_name, const std::string& owner);
    std::string lock_owner(const std::string& lock_name) const;
private:
    BrokerServer* server_;
};
}