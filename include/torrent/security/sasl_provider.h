#pragma once
#include <memory>
#include <string>
#include <vector>
#include <torrent/common/types.h>

namespace torrent::security {
class SaslProvider {
public:
    explicit SaslProvider(class broker::BrokerServer& s);
    std::vector<std::string> enabled_mechanisms() const;
    result<void> authenticate_scram_sha256(const std::string& username, const std::string& password);
    result<void> authenticate_scram_sha512(const std::string& username, const std::string& password);
    result<void> authenticate_plain(const std::string& username, const std::string& password);
    result<void> authenticate_kerberos(const std::string& ticket);
    result<void> authenticate_oauth2(const std::string& token);
private:
    broker::BrokerServer* server_;
};
}