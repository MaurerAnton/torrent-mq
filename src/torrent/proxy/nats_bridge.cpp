#include "torrent/proxy/proxy.h"
#include "torrent/common/types.h"
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <thread>
#include <atomic>
#include <unordered_map>
#include <mutex>
#include <regex>

namespace torrent::proxy {

class NatsBridge {
public:
    struct Config {
        uint16_t port{4222};
        std::string bind_address{"0.0.0.0"};
        int max_payload{1048576};
        int max_connections{65536};
    };

    explicit NatsBridge(const Config& cfg) : config_(cfg) {}

    void start() {
        running_.store(true);
        fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        int opt = 1;
        setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(config_.port);
        inet_pton(AF_INET, config_.bind_address.c_str(), &addr.sin_addr);
        bind(fd_, (sockaddr*)&addr, sizeof(addr));
        listen(fd_, SOMAXCONN);

        spdlog::info("NATS bridge started on port {}", config_.port);

        while (running_.load()) {
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept4(fd_, (sockaddr*)&client_addr, &client_len, SOCK_NONBLOCK);
            if (client_fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) { usleep(1000); continue; }
                break;
            }
            std::thread([this, client_fd]() { handle_nats_connection(client_fd); }).detach();
        }
    }

    void shutdown() {
        running_.store(false);
        if (fd_ >= 0) close(fd_);
    }

private:
    struct ClientState {
        bool verbose{false};
        bool pedantic{false};
        bool tls_required{false};
        std::string auth_token;
        std::string user;
        std::string password;
        std::unordered_set<std::string> subscriptions;
        std::string client_id;
    };

    void handle_nats_connection(int client_fd) {
        ClientState state;
        // Send INFO
        send_info(client_fd);

        char buf[65536];
        std::string buffer;

        while (running_.load()) {
            ssize_t n = recv(client_fd, buf, sizeof(buf), 0);
            if (n <= 0) break;
            buffer.append(buf, n);

            // Process complete lines (terminated by \r\n)
            size_t pos;
            while ((pos = buffer.find("\r\n")) != std::string::npos) {
                std::string line = buffer.substr(0, pos);
                buffer.erase(0, pos + 2);
                handle_nats_command(client_fd, line, state);
            }
        }
        close(client_fd);
    }

    void send_info(int fd) {
        nlohmann::json info;
        info["server_id"] = "torrent-nats-bridge";
        info["server_name"] = "torrent-mq";
        info["version"] = "0.1.0";
        info["proto"] = 1;
        info["host"] = config_.bind_address;
        info["port"] = config_.port;
        info["max_payload"] = config_.max_payload;
        info["auth_required"] = false;
        info["tls_required"] = false;

        std::string cmd = "INFO " + info.dump() + "\r\n";
        send(client_fd, cmd.c_str(), cmd.size(), MSG_NOSIGNAL);
    }

    void handle_nats_command(int fd, const std::string& line, ClientState& state) {
        // NATS protocol: CMD [subject [sid [reply] [payload_length]]]\r\n[payload]\r\n
        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;

        if (cmd == "CONNECT") {
            send(fd, "+OK\r\n", 5, MSG_NOSIGNAL);
        } else if (cmd == "PING") {
            send(fd, "PONG\r\n", 6, MSG_NOSIGNAL);
        } else if (cmd == "PONG") {
            // No response needed
        } else if (cmd == "PUB") {
            std::string subject, reply_to;
            int payload_size = 0;
            iss >> subject >> reply_to;
            if (isdigit(reply_to[0])) {
                payload_size = std::stoi(reply_to);
                reply_to.clear();
            } else {
                std::string size_str;
                iss >> size_str;
                if (!size_str.empty()) payload_size = std::stoi(size_str);
            }
            // PUB subject [reply-to] size\r\n[payload]\r\n
            // Acknowledge with +OK
            send(fd, "+OK\r\n", 5, MSG_NOSIGNAL);
        } else if (cmd == "SUB") {
            std::string subject, group, sid;
            iss >> subject;
            if (!iss.eof()) iss >> group;
            if (!iss.eof()) iss >> sid;
            state.subscriptions.insert(subject);
            send(fd, "+OK\r\n", 5, MSG_NOSIGNAL);
        } else if (cmd == "UNSUB") {
            std::string sid;
            iss >> sid;
            send(fd, "+OK\r\n", 5, MSG_NOSIGNAL);
        } else {
            // Unknown command
            std::string err = "-ERR 'Unknown Protocol Operation'\r\n";
            send(fd, err.c_str(), err.size(), MSG_NOSIGNAL);
        }
    }

    Config config_;
    int fd_{-1};
    std::atomic<bool> running_{false};
};

} // namespace torrent::proxy
