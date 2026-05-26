#pragma once
#include <memory>
#include <string>
#include <vector>
#include <torrent/common/types.h>

namespace torrent::streams {
class TopologyBuilder {
public:
    explicit TopologyBuilder(class broker::BrokerServer& s);
    void add_source(const std::string& name, const std::string& topic);
    void add_filter(const std::string& name, const std::string& predicate);
    void add_map(const std::string& name, const std::string& transform);
    void add_aggregate(const std::string& name, const std::string& window);
    void add_sink(const std::string& name, const std::string& topic);
    void build();
    void start();
    void shutdown();
private:
    broker::BrokerServer* server_;
};
}