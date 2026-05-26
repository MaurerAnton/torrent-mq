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

namespace torrent::proxy {

class AmqpBridge {
public:
    struct Config {
        uint16_t port{5672};
        std::string bind_address{"0.0.0.0"};
        int max_channels{65535};
        int max_frame_size{131072};
        int heartbeat_seconds{60};
    };

    explicit AmqpBridge(const Config& cfg) : config_(cfg) {}

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

        spdlog::info("AMQP bridge started on port {}", config_.port);

        while (running_.load()) {
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept4(fd_, (sockaddr*)&client_addr, &client_len, SOCK_NONBLOCK);
            if (client_fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) { usleep(1000); continue; }
                break;
            }
            std::thread([this, client_fd]() { handle_amqp_connection(client_fd); }).detach();
        }
    }

    void shutdown() {
        running_.store(false);
        if (fd_ >= 0) close(fd_);
    }

private:
    // AMQP 0-9-1 frame types
    enum class FrameType : uint8_t { Method = 1, Header = 2, Body = 3, Heartbeat = 8 };
    enum class MethodClass : uint16_t { Connection = 10, Channel = 20, Exchange = 40, Queue = 50, Basic = 60, Tx = 90 };

    struct AmqpFrame {
        FrameType type{FrameType::Method};
        uint16_t channel{0};
        std::vector<uint8_t> payload;
    };

    void handle_amqp_connection(int client_fd) {
        // AMQP protocol header: "AMQP\x00\x00\x09\x01"
        char header[8];
        if (recv(client_fd, header, 8, MSG_WAITALL) != 8) { close(client_fd); return; }

        if (memcmp(header, "AMQP", 4) != 0) { close(client_fd); return; }

        // Send Connection.Start
        send_connection_start(client_fd);
        // Simplified: accept Connection.StartOk, send Connection.Tune, accept TuneOk, Open, OpenOk
        // Then handle channel open, exchange declare, queue declare, bind, basic publish/consume

        char buf[65536];
        while (running_.load()) {
            ssize_t n = recv(client_fd, buf, sizeof(buf), 0);
            if (n <= 0) break;

            auto frames = parse_frames(reinterpret_cast<const uint8_t*>(buf), n);
            for (const auto& frame : frames) {
                handle_frame(client_fd, frame);
            }
        }
        close(client_fd);
    }

    void send_connection_start(int fd) {
        // Connection.Start method
        std::vector<uint8_t> payload;
        payload.push_back(0); payload.push_back(10); // class_id = 10 (Connection)
        payload.push_back(0); payload.push_back(10); // method_id = 10 (Start)
        // version-major = 0, version-minor = 9
        payload.push_back(0); payload.push_back(9);
        // server-properties (field-table, empty)
        payload.push_back(0); payload.push_back(0); payload.push_back(0); payload.push_back(0);
        // mechanisms = "PLAIN"
        std::string mechanisms = "PLAIN";
        payload.push_back(static_cast<uint8_t>(mechanisms.size()));
        payload.insert(payload.end(), mechanisms.begin(), mechanisms.end());
        // locales = "en_US"
        std::string locales = "en_US";
        payload.push_back(static_cast<uint8_t>(locales.size()));
        payload.insert(payload.end(), locales.begin(), locales.end());

        write_frame(fd, FrameType::Method, 0, payload);
    }

    void handle_frame(int fd, const AmqpFrame& frame) {
        if (frame.type == FrameType::Heartbeat) {
            write_frame(fd, FrameType::Heartbeat, 0, {});
            return;
        }
        // Simplified handling: acknowledge most methods
        if (frame.payload.size() < 4) return;
        uint16_t class_id = (frame.payload[0] << 8) | frame.payload[1];
        // Send generic acknowledgment
        std::vector<uint8_t> ack = {frame.payload[0], frame.payload[1],
                                     static_cast<uint8_t>(frame.payload[2] + 1),
                                     static_cast<uint8_t>(frame.payload[3] + 1)};
        write_frame(fd, FrameType::Method, frame.channel, ack);
    }

    std::vector<AmqpFrame> parse_frames(const uint8_t* data, size_t size) {
        std::vector<AmqpFrame> frames;
        size_t offset = 0;

        while (offset + 7 <= size) {
            AmqpFrame frame;
            frame.type = static_cast<FrameType>(data[offset]);
            frame.channel = (data[offset + 1] << 8) | data[offset + 2];
            uint32_t payload_size = (data[offset + 3] << 24) | (data[offset + 4] << 16) |
                                     (data[offset + 5] << 8) | data[offset + 6];
            uint8_t frame_end = data[offset + 7 + payload_size];

            if (offset + 8 + payload_size > size) break;
            if (frame_end != 0xCE) break; // Frame-end byte

            frame.payload.assign(data + offset + 7, data + offset + 7 + payload_size);
            frames.push_back(frame);
            offset += 8 + payload_size;
        }
        return frames;
    }

    void write_frame(int fd, FrameType type, uint16_t channel, const std::vector<uint8_t>& payload) {
        std::vector<uint8_t> frame;
        frame.push_back(static_cast<uint8_t>(type));
        frame.push_back(static_cast<uint8_t>(channel >> 8));
        frame.push_back(static_cast<uint8_t>(channel & 0xFF));
        uint32_t size = payload.size();
        frame.push_back(static_cast<uint8_t>(size >> 24));
        frame.push_back(static_cast<uint8_t>(size >> 16));
        frame.push_back(static_cast<uint8_t>(size >> 8));
        frame.push_back(static_cast<uint8_t>(size & 0xFF));
        frame.insert(frame.end(), payload.begin(), payload.end());
        frame.push_back(0xCE); // Frame-end

        send(fd, frame.data(), frame.size(), MSG_NOSIGNAL);
    }

    Config config_;
    int fd_{-1};
    std::atomic<bool> running_{false};
};

} // namespace torrent::proxy
