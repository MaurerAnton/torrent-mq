#pragma once
#include <memory>
#include <string>
#include <torrent/common/types.h>

namespace torrent::proxy {
class RestProxy {
public:
    explicit RestProxy(class broker::BrokerServer& s, uint16_t port = 8082);
    void start();
    void shutdown();
    uint16_t port() const noexcept;
private:
    broker::BrokerServer* server_;
    uint16_t port_;
};

class WebSocketProxy {
public:
    explicit WebSocketProxy(class broker::BrokerServer& s, uint16_t port = 8083);
    void start();
    void shutdown();
private:
    broker::BrokerServer* server_;
    uint16_t port_;
};

class MqttBridge {
public:
    explicit MqttBridge(class broker::BrokerServer& s, uint16_t port = 1883);
    void start();
    void shutdown();
private:
    broker::BrokerServer* server_;
    uint16_t port_;
};
}