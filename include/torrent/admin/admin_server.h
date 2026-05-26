#pragma once
#include <memory>
#include <string>
#include <vector>
#include <torrent/common/types.h>

namespace torrent::admin {

// Forward declaration of embedded HTTP server (defined in admin_server.cpp)
class HttpServer;

class AdminServer {
public:
    explicit AdminServer(class broker::BrokerServer& s, uint16_t port = 9644);
    ~AdminServer();
    void start();
    void shutdown();
    uint16_t port() const noexcept;
private:
    class broker::BrokerServer* server_;
    uint16_t port_;
    std::unique_ptr<HttpServer> http_server_;
};
}