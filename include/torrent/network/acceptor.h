#pragma once
#include <memory>
#include <functional>
#include <torrent/common/types.h>

namespace torrent::network {
class TcpTransport;
using ConnectionHandler = std::function<void(std::unique_ptr<TcpTransport>)>;

class Acceptor {
public:
    Acceptor(const endpoint& bind_addr, uint16_t port, int backlog, bool non_blocking);
    ~Acceptor();
    void accept_loop(ConnectionHandler handler);
    void stop();
    int fd() const noexcept;
    endpoint bound_address() const;
    void close();
};
}