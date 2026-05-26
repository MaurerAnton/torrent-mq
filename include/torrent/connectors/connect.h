#pragma once
#include <memory>
#include <string>
#include <vector>
#include <torrent/common/types.h>

namespace torrent::connectors {
class ConnectFramework {
public:
    explicit ConnectFramework(class broker::BrokerServer& s);
    void register_source(const std::string& name, const std::string& connector_class, const std::string& config);
    void register_sink(const std::string& name, const std::string& connector_class, const std::string& config);
    void start_connector(const std::string& name);
    void stop_connector(const std::string& name);
    void restart_connector(const std::string& name);
    std::string connector_status(const std::string& name) const;
    void shutdown();
private:
    broker::BrokerServer* server_;
};
}