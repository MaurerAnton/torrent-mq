#pragma once
#include <memory>
#include <string>
#include <vector>
#include <torrent/common/types.h>

namespace torrent::security {
class AuthManager {
public:
    explicit AuthManager(class broker::BrokerServer& s);
    result<void> authenticate(const std::string& mechanism, const std::string& credentials);
    bool is_authenticated(const std::string& principal) const;
    result<void> authorize(const std::string& principal, const std::string& resource, const std::string& operation);
    std::vector<std::string> sasl_mechanisms() const;
private:
    broker::BrokerServer* server_;
};
}