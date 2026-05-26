#pragma once
#include <memory>
#include <string>
#include <vector>
#include <torrent/common/types.h>

namespace torrent::security {
class AclEngine {
public:
    explicit AclEngine(class broker::BrokerServer& s);
    bool check_access(const std::string& principal, const std::string& host, const std::string& resource, const std::string& operation);
    void add_rule(const std::string& principal, const std::string& host, const std::string& resource, const std::string& operation, bool allow);
    void remove_rule(const std::string& principal, const std::string& host, const std::string& resource, const std::string& operation);
private:
    broker::BrokerServer* server_;
};
}