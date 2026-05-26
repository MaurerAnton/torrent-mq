#include "torrent/proxy/proxy.h"
#include "torrent/common/types.h"
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <sstream>
#include <thread>
#include <atomic>
#include <mutex>
#include <map>

namespace torrent::proxy {

class HttpBridge {
public:
    struct Config {
        uint16_t port{8084};
        std::string bind_address{"0.0.0.0"};
        int max_connections{1000};
        int thread_count{4};
        int request_timeout_ms{30000};
    };

    explicit HttpBridge(const Config& cfg) : config_(cfg) {}

    void start() {
        running_.store(true);
        // Create listening socket
        fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        int opt = 1;
        setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(config_.port);
        inet_pton(AF_INET, config_.bind_address.c_str(), &addr.sin_addr);
        bind(fd_, (sockaddr*)&addr, sizeof(addr));
        listen(fd_, SOMAXCONN);

        spdlog::info("HTTP bridge started on port {}", config_.port);

        // Accept loop
        while (running_.load()) {
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept4(fd_, (sockaddr*)&client_addr, &client_len, SOCK_NONBLOCK);
            if (client_fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    usleep(1000);
                    continue;
                }
                break;
            }
            handle_connection(client_fd);
        }
    }

    void shutdown() {
        running_.store(false);
        if (fd_ >= 0) close(fd_);
    }

private:
    void handle_connection(int client_fd) {
        char buf[65536];
        ssize_t n = recv(client_fd, buf, sizeof(buf) - 1, 0);
        if (n <= 0) { close(client_fd); return; }
        buf[n] = '\0';

        std::string request(buf, n);
        std::string method, path, body;
        parse_http_request(request, method, path, body);

        nlohmann::json response;
        int status = 200;

        try {
            if (method == "POST" && path.find("/topics/") == 0) {
                status = handle_produce(path, body, response);
            } else if (method == "GET" && path.find("/topics/") == 0 && path.find("/messages") != std::string::npos) {
                status = handle_consume(path, response);
            } else if (method == "GET" && path == "/health") {
                response["status"] = "healthy";
                response["uptime_seconds"] = 0;
            } else if (method == "GET" && path == "/") {
                response["service"] = "torrent-mq HTTP bridge";
                response["version"] = "0.1.0";
                response["endpoints"] = {"/health", "/topics/{name}", "/topics/{name}/messages"};
            } else {
                status = 404;
                response["error"] = "Not found";
                response["path"] = path;
            }
        } catch (const std::exception& e) {
            status = 500;
            response["error"] = e.what();
        }

        std::string resp_str = build_http_response(status, response.dump());
        send(client_fd, resp_str.c_str(), resp_str.size(), MSG_NOSIGNAL);
        close(client_fd);
    }

    int handle_produce(const std::string& path, const std::string& body, nlohmann::json& resp) {
        // Extract topic name from /topics/{topic}
        std::string topic = path.substr(8);
        size_t slash = topic.find('/');
        if (slash != std::string::npos) topic = topic.substr(0, slash);

        auto msg = nlohmann::json::parse(body);

        // Build produce request
        resp["topic"] = topic;
        resp["offset"] = 0;
        resp["timestamp"] = 0;
        resp["result"] = "produced";

        if (msg.contains("key")) resp["key"] = msg["key"];
        if (msg.contains("value")) resp["value"] = msg["value"];
        if (msg.contains("headers")) resp["headers"] = msg["headers"];

        return 200;
    }

    int handle_consume(const std::string& path, nlohmann::json& resp) {
        std::string topic = path.substr(8);
        size_t slash = topic.find("/messages");
        if (slash != std::string::npos) topic = topic.substr(0, slash);

        resp["topic"] = topic;
        resp["messages"] = nlohmann::json::array();
        resp["count"] = 0;
        return 200;
    }

    static void parse_http_request(const std::string& raw, std::string& method,
                                    std::string& path, std::string& body) {
        std::istringstream stream(raw);
        std::string line;
        std::getline(stream, line);
        // Parse request line: METHOD /path HTTP/1.1
        std::istringstream req_line(line);
        req_line >> method >> path;

        // Find body (after \r\n\r\n)
        size_t body_pos = raw.find("\r\n\r\n");
        if (body_pos != std::string::npos) {
            body = raw.substr(body_pos + 4);
        }
    }

    static std::string build_http_response(int status, const std::string& json_body) {
        std::ostringstream resp;
        resp << "HTTP/1.1 " << status << " " << status_text(status) << "\r\n";
        resp << "Content-Type: application/json\r\n";
        resp << "Content-Length: " << json_body.size() << "\r\n";
        resp << "Connection: close\r\n";
        resp << "Server: torrent-mq-http-bridge\r\n";
        resp << "\r\n";
        resp << json_body;
        return resp.str();
    }

    static const char* status_text(int status) {
        switch (status) {
            case 200: return "OK";
            case 201: return "Created";
            case 400: return "Bad Request";
            case 404: return "Not Found";
            case 500: return "Internal Server Error";
            case 503: return "Service Unavailable";
            default: return "Unknown";
        }
    }

    Config config_;
    int fd_{-1};
    std::atomic<bool> running_{false};
};

} // namespace torrent::proxy
