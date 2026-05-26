#pragma once
#include <memory>
#include <string>
#include <torrent/common/types.h>

namespace torrent::security {
class AuditLog {
public:
    explicit AuditLog(class broker::BrokerServer& s);
    void log_auth(const std::string& principal, const std::string& action, bool success);
    void log_admin(const std::string& principal, const std::string& operation, const std::string& resource);
    void log_security(const std::string& event, const std::string& details);
private:
    broker::BrokerServer* server_;
};
}