#pragma once
#include <memory>
#include <string>
#include <vector>
#include <torrent/common/types.h>

namespace torrent::broker {
class SubscriptionManager {
public:
    explicit SubscriptionManager(class BrokerServer& s) : server_(&s) {}
    result<void> subscribe(const std::string& group, const std::string& topic);
    result<void> unsubscribe(const std::string& group, const std::string& topic);
    std::vector<std::string> subscriptions(const std::string& group) const;
private:
    BrokerServer* server_;
};
}