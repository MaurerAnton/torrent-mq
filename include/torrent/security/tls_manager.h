#pragma once
#include <memory>
#include <string>
#include <torrent/common/types.h>

namespace torrent::security {
class TlsManager {
public:
    explicit TlsManager(class broker::BrokerServer& s);
    void load_certificate(const std::string& cert_path, const std::string& key_path);
    void load_ca(const std::string& ca_path);
    bool is_enabled() const noexcept;
private:
    broker::BrokerServer* server_;
};
}