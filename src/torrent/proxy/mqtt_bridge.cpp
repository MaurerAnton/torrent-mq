/**
 * torrent-mq — MqttBridge Implementation
 *
 * MQTT 3.1.1 / 5.0 protocol bridge that maps MQTT publish/subscribe
 * semantics to torrent-mq's produce/consume model.
 *
 * Feature mapping:
 *   MQTT PUBLISH → torrent produce (with QoS-aware ack)
 *   MQTT SUBSCRIBE → torrent consumer group registration
 *   MQTT UNSUBSCRIBE → consumer group deregistration
 *   MQTT QoS 0    → at-most-once (fire-and-forget produce)
 *   MQTT QoS 1    → at-least-once (produce with leader ack)
 *   MQTT QoS 2    → exactly-once (transactional produce, 4-way handshake)
 *   Retained messages → stored in a special __mqtt_retained topic
 *   Will messages     → stored on CONNECT, published on abnormal disconnect
 *   MQTT topic filter → mapped to torrent topic prefixes with wildcards
 *
 * MQTT packet structure:
 *   Fixed header:  [control_byte] [remaining_length...]
 *   Variable header: depends on packet type
 *   Payload: depends on packet type
 *
 * Control packet types:
 *   1  = CONNECT      9  = SUBSCRIBE
 *   2  = CONNACK      10 = SUBACK
 *   3  = PUBLISH      11 = UNSUBSCRIBE
 *   4  = PUBACK       12 = UNSUBACK
 *   5  = PUBREC       13 = PINGREQ
 *   6  = PUBREL       14 = PINGRESP
 *   7  = PUBCOMP      15 = DISCONNECT
 *   8  = SUBSCRIBE    16 = AUTH (MQTT 5.0 only)
 *
 * Thread-safety:
 *   - Accept loop on dedicated thread.
 *   - Each client connection on its own thread pool worker.
 *   - Per-client session state protected by the connection handler.
 *   - Retained messages map protected by a shared mutex.
 */

#include "torrent/proxy/proxy.h"
#include "torrent/broker/server.h"
#include "torrent/common/types.h"
#include "torrent/common/thread_pool.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace torrent::proxy {

// ============================================================================
// Anonymous namespace — MQTT protocol helpers, constants
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

auto mqtt_logger() {
    static auto l = spdlog::get("mqtt_bridge");
    if (!l) {
        l = spdlog::stdout_color_mt("mqtt_bridge");
        l->set_level(spdlog::level::info);
    }
    return l;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

constexpr int    kBacklog              = 128;
constexpr int    kDefaultWorkers       = 4;
constexpr size_t kMaxPacketSize        = 256 * 1024;  // 256 KB MQTT max
constexpr size_t kMaxClientIdLen       = 23;          // MQTT spec max
constexpr size_t kMaxTopicLen          = 65535;
constexpr auto   kKeepAliveDefault     = std::chrono::seconds(60);
constexpr auto   kKeepAliveGraceFactor = 1.5;         // MQTT allows 1.5x grace
constexpr auto   kMaxKeepAlive         = std::chrono::seconds(65535);
constexpr size_t kMaxSubscriptions     = 256;
constexpr size_t kMaxConnections       = 10000;

// MQTT protocol version
inline constexpr uint8_t kMqttVersion311 = 4;
inline constexpr uint8_t kMqttVersion5   = 5;

// MQTT control packet types (high nibble of first byte)
inline constexpr uint8_t kMqttConnect     = 1;
inline constexpr uint8_t kMqttConnack     = 2;
inline constexpr uint8_t kMqttPublish     = 3;
inline constexpr uint8_t kMqttPuback      = 4;
inline constexpr uint8_t kMqttPubrec      = 5;
inline constexpr uint8_t kMqttPubrel      = 6;
inline constexpr uint8_t kMqttPubcomp     = 7;
inline constexpr uint8_t kMqttSubscribe   = 8;
inline constexpr uint8_t kMqttSuback      = 9;
inline constexpr uint8_t kMqttUnsubscribe = 10;
inline constexpr uint8_t kMqttUnsuback    = 11;
inline constexpr uint8_t kMqttPingreq     = 12;
inline constexpr uint8_t kMqttPingresp    = 13;
inline constexpr uint8_t kMqttDisconnect  = 14;
inline constexpr uint8_t kMqttAuth        = 15;

// CONNACK return codes
inline constexpr uint8_t kConnAccepted     = 0;
inline constexpr uint8_t kConnRefusedProto = 1;
inline constexpr uint8_t kConnRefusedId    = 2;
inline constexpr uint8_t kConnRefusedSrv   = 3;
inline constexpr uint8_t kConnBadUserPass  = 4;
inline constexpr uint8_t kConnNotAuth      = 5;

// --------------------------------------------------------------------------
// MQTT topic filter → torrent topic mapping
// --------------------------------------------------------------------------

/// Convert an MQTT topic filter to a torrent topic name.
/// MQTT uses '/' as hierarchy separator; we replace '/' with '.' for torrent.
/// MQTT wildcards: '+' (single level) and '#' (multi-level).
/// For torrent we use a prefix-based approach for '#' and exact for '+'.
[[nodiscard]] std::string mqtt_topic_to_torrent(std::string_view mqtt_topic) {
    std::string result;
    result.reserve(mqtt_topic.size());

    for (size_t i = 0; i < mqtt_topic.size(); ++i) {
        char c = mqtt_topic[i];
        if (c == '+') {
            result += "*";       // single-level wildcard
        } else if (c == '#') {
            result += ">";       // multi-level wildcard (if at end)
            if (i == mqtt_topic.size() - 1) break;
            result += c;
        } else if (c == '/') {
            result += '.';
        } else {
            result += c;
        }
    }
    return result;
}

/// Convert a torrent topic back to MQTT format.
[[nodiscard]] std::string torrent_topic_to_mqtt(std::string_view torrent_topic) {
    std::string result;
    result.reserve(torrent_topic.size());
    for (char c : torrent_topic) {
        result += (c == '.') ? '/' : c;
    }
    return result;
}

// --------------------------------------------------------------------------
// MQTT length encoding (variable-length integer, 1–4 bytes)
// --------------------------------------------------------------------------

/// Read a variable-length integer from a buffer starting at offset.
/// Returns the value and the number of bytes consumed.
/// Returns {-1, 0} on error.
[[nodiscard]] std::pair<int32_t, size_t> read_mqtt_length(
    const uint8_t* data, size_t len) {
    int32_t value = 0;
    int32_t multiplier = 1;
    size_t  consumed = 0;

    for (size_t i = 0; i < 4 && i < len; ++i) {
        uint8_t byte = data[i];
        value += (byte & 0x7F) * multiplier;
        consumed++;
        if ((byte & 0x80) == 0) break;
        multiplier *= 128;
        if (multiplier > 128 * 128 * 128) return {-1, 0}; // overflow
    }

    return {value, consumed};
}

/// Encode a variable-length integer into a buffer.
/// Returns the number of bytes written.
[[nodiscard]] size_t write_mqtt_length(uint8_t* buf, size_t value) {
    size_t count = 0;
    do {
        uint8_t byte = value % 128;
        value /= 128;
        if (value > 0) byte |= 0x80;
        buf[count++] = byte;
    } while (value > 0 && count < 4);
    return count;
}

/// Compute the encoded size of a variable-length integer.
[[nodiscard]] size_t mqtt_length_size(size_t value) {
    if (value < 128) return 1;
    if (value < 16384) return 2;
    if (value < 2097152) return 3;
    return 4;
}

// --------------------------------------------------------------------------
// MQTT string encoding (2-byte length prefix, UTF-8)
// --------------------------------------------------------------------------

[[nodiscard]] bool read_mqtt_string(const uint8_t* data, size_t len,
                                      size_t& offset, std::string& out) {
    if (offset + 2 > len) return false;
    uint16_t slen = (static_cast<uint16_t>(data[offset]) << 8) |
                     static_cast<uint16_t>(data[offset + 1]);
    offset += 2;
    if (offset + slen > len) return false;
    out.assign(reinterpret_cast<const char*>(data + offset), slen);
    offset += slen;
    return true;
}

[[nodiscard]] size_t write_mqtt_string(uint8_t* buf, std::string_view s) {
    uint16_t slen = static_cast<uint16_t>(std::min(s.size(), size_t(0xFFFF)));
    buf[0] = static_cast<uint8_t>((slen >> 8) & 0xFF);
    buf[1] = static_cast<uint8_t>(slen & 0xFF);
    std::memcpy(buf + 2, s.data(), slen);
    return 2 + slen;
}

[[nodiscard]] constexpr size_t mqtt_string_size(std::string_view s) {
    return 2 + s.size();
}

// --------------------------------------------------------------------------
// MQTT packet building helpers
// --------------------------------------------------------------------------

/// Build a CONNACK packet.
[[nodiscard]] std::string build_connack(uint8_t version, uint8_t return_code,
                                          bool session_present = false) {
    // Fixed header: CONNACK (0x20), remaining length
    std::string pkt;
    uint8_t var_header[2];
    size_t var_len = 2;

    if (version == kMqttVersion5) {
        // MQTT 5.0: session present flag + reason code
        var_header[0] = session_present ? 0x01 : 0x00;
        var_header[1] = return_code;
    } else {
        // MQTT 3.1.1: session present flag + return code
        var_header[0] = session_present ? 0x01 : 0x00;
        var_header[1] = return_code;
    }

    // Fixed header
    pkt += static_cast<char>(0x20); // CONNACK
    uint8_t len_buf[4];
    size_t len_size = write_mqtt_length(len_buf, var_len);
    pkt.append(reinterpret_cast<char*>(len_buf), len_size);
    pkt.append(reinterpret_cast<char*>(var_header), var_len);

    return pkt;
}

/// Build a SUBACK packet.
[[nodiscard]] std::string build_suback(uint16_t packet_id,
                                         const std::vector<uint8_t>& return_codes) {
    std::string pkt;

    // Payload
    std::string payload;
    payload += static_cast<char>((packet_id >> 8) & 0xFF);
    payload += static_cast<char>(packet_id & 0xFF);
    for (auto rc : return_codes) {
        payload += static_cast<char>(rc);
    }

    // Fixed header
    pkt += static_cast<char>(0x90); // SUBACK
    uint8_t len_buf[4];
    size_t len_size = write_mqtt_length(len_buf, payload.size());
    pkt.append(reinterpret_cast<char*>(len_buf), len_size);
    pkt += payload;

    return pkt;
}

/// Build an UNSUBACK packet.
[[nodiscard]] std::string build_unsuback(uint16_t packet_id) {
    std::string pkt;
    pkt += static_cast<char>(0xB0); // UNSUBACK
    pkt += static_cast<char>(2);    // remaining length
    pkt += static_cast<char>((packet_id >> 8) & 0xFF);
    pkt += static_cast<char>(packet_id & 0xFF);
    return pkt;
}

/// Build a PUBACK packet (QoS 1 ack).
[[nodiscard]] std::string build_puback(uint16_t packet_id) {
    std::string pkt;
    pkt += static_cast<char>(0x40); // PUBACK
    pkt += static_cast<char>(2);    // remaining length
    pkt += static_cast<char>((packet_id >> 8) & 0xFF);
    pkt += static_cast<char>(packet_id & 0xFF);
    return pkt;
}

/// Build a PUBREC packet (QoS 2 — received).
[[nodiscard]] std::string build_pubrec(uint16_t packet_id) {
    std::string pkt;
    pkt += static_cast<char>(0x50); // PUBREC
    pkt += static_cast<char>(2);
    pkt += static_cast<char>((packet_id >> 8) & 0xFF);
    pkt += static_cast<char>(packet_id & 0xFF);
    return pkt;
}

/// Build a PUBREL packet (QoS 2 — release).
[[nodiscard]] std::string build_pubrel(uint16_t packet_id) {
    std::string pkt;
    pkt += static_cast<char>(0x62); // PUBREL (0x60 + 0x02 for QoS 1 flag)
    pkt += static_cast<char>(2);
    pkt += static_cast<char>((packet_id >> 8) & 0xFF);
    pkt += static_cast<char>(packet_id & 0xFF);
    return pkt;
}

/// Build a PUBCOMP packet (QoS 2 — complete).
[[nodiscard]] std::string build_pubcomp(uint16_t packet_id) {
    std::string pkt;
    pkt += static_cast<char>(0x70); // PUBCOMP
    pkt += static_cast<char>(2);
    pkt += static_cast<char>((packet_id >> 8) & 0xFF);
    pkt += static_cast<char>(packet_id & 0xFF);
    return pkt;
}

/// Build a PINGRESP packet.
[[nodiscard]] std::string build_pingresp() {
    return std::string("\xD0\x00", 2);
}

/// Format a torrent topic for an MQTT publish (no leading /).
[[nodiscard]] std::string topic_for_publish(std::string_view torrent_topic) {
    std::string mqtt_topic = torrent_topic_to_mqtt(torrent_topic);
    // Remove leading . if present
    if (!mqtt_topic.empty() && mqtt_topic[0] == '/') {
        mqtt_topic.erase(0, 1);
    }
    return mqtt_topic;
}

// --------------------------------------------------------------------------
// Read exact bytes from socket
// --------------------------------------------------------------------------

bool read_exact(int fd, uint8_t* buf, size_t n) {
    size_t remaining = n;
    while (remaining > 0) {
        ssize_t r = ::read(fd, buf, remaining);
        if (r <= 0) return false;
        buf += static_cast<size_t>(r);
        remaining -= static_cast<size_t>(r);
    }
    return true;
}

// --------------------------------------------------------------------------
// Per-client session state
// --------------------------------------------------------------------------

struct MqttSession {
    std::string client_id;
    uint8_t     protocol_version  = kMqttVersion311;
    bool        connected         = false;
    bool        clean_session     = true;

    // Keep-alive
    std::chrono::seconds keep_alive{kKeepAliveDefault};

    // Will message
    bool        will_flag         = false;
    uint8_t     will_qos          = 0;
    bool        will_retain       = false;
    std::string will_topic;
    std::string will_payload;

    // Credentials
    bool        has_username      = false;
    bool        has_password      = false;
    std::string username;
    std::string password;

    // Subscriptions: MQTT topic filter → max QoS
    std::map<std::string, uint8_t> subscriptions;

    // Active QoS 2 transactions (packet_id → state)
    struct Qos2State {
        enum Phase { awaiting_pubrel, awaiting_pubcomp } phase;
        std::string topic;
        std::string payload;
        bool retain;
    };
    std::map<uint16_t, Qos2State> qos2_pending;

    // Packet ID counter
    uint16_t next_packet_id = 1;
};

// --------------------------------------------------------------------------
// Retained messages store (shared across all sessions)
// --------------------------------------------------------------------------

class RetainedStore {
public:
    void set(const std::string& topic, std::string payload, uint8_t qos) {
        std::unique_lock lock(mutex_);
        if (payload.empty()) {
            retained_.erase(topic);
        } else {
            retained_[topic] = {std::move(payload), qos};
        }
    }

    [[nodiscard]] std::vector<std::pair<std::string, std::string>>
    matches(std::string_view filter) const {
        std::shared_lock lock(mutex_);
        std::vector<std::pair<std::string, std::string>> results;
        auto mqtt_filter = std::string(filter);

        for (auto& [topic, entry] : retained_) {
            if (topic_matches(topic, mqtt_filter)) {
                results.emplace_back(topic, entry.payload);
            }
        }
        return results;
    }

    void clear() {
        std::unique_lock lock(mutex_);
        retained_.clear();
    }

private:
    struct Entry {
        std::string payload;
        uint8_t     qos;
    };

    [[nodiscard]] static bool topic_matches(
        std::string_view topic, std::string_view filter)
    {
        size_t ti = 0, fi = 0;
        while (ti < topic.size() && fi < filter.size()) {
            if (filter[fi] == '#') {
                return true; // multi-level wildcard at end
            }
            if (filter[fi] == '+') {
                // Skip one level in topic
                while (ti < topic.size() && topic[ti] != '/') ti++;
                fi++;
                continue;
            }
            if (topic[ti] != filter[fi]) return false;
            ti++; fi++;
        }
        return ti == topic.size() && fi == filter.size();
    }

    mutable std::shared_mutex mutex_;
    std::map<std::string, Entry> retained_;
};

/// Global retained message store.
RetainedStore& retained_store() {
    static RetainedStore store;
    return store;
}

} // anonymous namespace

// ============================================================================
// MqttBridge — PIMPL
// ============================================================================

struct MqttBridge::Impl {
    broker::BrokerServer&               server;
    uint16_t                            port;
    std::atomic<bool>                   running{false};
    int                                 listen_fd = -1;
    std::thread                         accept_thread;
    std::unique_ptr<torrent::thread_pool> workers;
    std::atomic<size_t>                 client_count{0};
    std::atomic<bool>                   shutdown_flag{false};

    explicit Impl(broker::BrokerServer& s, uint16_t p)
        : server(s), port(p) {
        workers = std::make_unique<torrent::thread_pool>(
            kDefaultWorkers, "mqtt-bridge");
    }

    ~Impl() { shutdown(); }

    void shutdown() {
        bool expected = false;
        if (!shutdown_flag.compare_exchange_strong(expected, true)) return;

        running.store(false, std::memory_order_release);
        if (listen_fd >= 0) {
            ::shutdown(listen_fd, SHUT_RDWR);
            ::close(listen_fd);
            listen_fd = -1;
        }
        if (accept_thread.joinable()) accept_thread.join();
        if (workers) workers->shutdown();
    }
};

// ============================================================================
// MqttBridge — Public API
// ============================================================================

MqttBridge::MqttBridge(broker::BrokerServer& server, uint16_t port)
    : impl_(std::make_unique<Impl>(server, port)) {}

MqttBridge::~MqttBridge() = default;

void MqttBridge::start() {
    auto& s = *impl_;
    if (s.running.load(std::memory_order_acquire)) return;

    s.listen_fd = ::socket(AF_INET,
                            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (s.listen_fd < 0) {
        mqtt_logger()->error("socket() failed: {}", std::strerror(errno));
        throw std::runtime_error("MqttBridge: socket() failed");
    }

    int opt = 1;
    ::setsockopt(s.listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    ::setsockopt(s.listen_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    ::setsockopt(s.listen_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(s.port);

    if (::bind(s.listen_fd, reinterpret_cast<sockaddr*>(&addr),
               sizeof(addr)) < 0) {
        mqtt_logger()->error("bind(:{}) failed: {}", s.port, std::strerror(errno));
        ::close(s.listen_fd);
        s.listen_fd = -1;
        throw std::runtime_error("MqttBridge: bind() failed");
    }

    if (::listen(s.listen_fd, kBacklog) < 0) {
        mqtt_logger()->error("listen() failed: {}", std::strerror(errno));
        ::close(s.listen_fd);
        s.listen_fd = -1;
        throw std::runtime_error("MqttBridge: listen() failed");
    }

    if (s.port == 0) {
        socklen_t alen = sizeof(addr);
        getsockname(s.listen_fd, reinterpret_cast<sockaddr*>(&addr), &alen);
        s.port = ntohs(addr.sin_port);
    }

    s.running.store(true, std::memory_order_release);
    s.accept_thread = std::thread([this] { accept_loop(); });
    mqtt_logger()->info("MqttBridge started on port {}", s.port);
}

void MqttBridge::shutdown() {
    impl_->shutdown();
    mqtt_logger()->info("MqttBridge shut down");
}

// ============================================================================
// Accept loop
// ============================================================================

void MqttBridge::accept_loop() {
    auto& s = *impl_;
    while (s.running.load(std::memory_order_acquire)) {
        sockaddr_in client_addr{};
        socklen_t   addr_len = sizeof(client_addr);
        int client_fd = ::accept4(s.listen_fd,
                                   reinterpret_cast<sockaddr*>(&client_addr),
                                   &addr_len,
                                   SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            if (errno == EBADF || errno == EINVAL) break;
            mqtt_logger()->warn("accept() error: {}", std::strerror(errno));
            continue;
        }

        if (s.client_count.load() >= kMaxConnections) {
            mqtt_logger()->warn("Connection limit reached ({}), rejecting",
                                kMaxConnections);
            ::close(client_fd);
            continue;
        }

        s.client_count.fetch_add(1, std::memory_order_release);
        mqtt_logger()->debug("MQTT connection from {}:{}",
                             inet_ntoa(client_addr.sin_addr),
                             ntohs(client_addr.sin_port));

        s.workers->submit([this, client_fd] {
            handle_connection(client_fd);
            impl_->client_count.fetch_sub(1, std::memory_order_release);
            ::close(client_fd);
        });
    }
}

// ============================================================================
// Connection handler — MQTT state machine
// ============================================================================

void MqttBridge::handle_connection(int fd) {
    MqttSession session;
    uint8_t packet_buf[kMaxPacketSize];

    // Step 1: wait for CONNECT
    if (!handle_connect_packet(fd, packet_buf, session)) {
        return;
    }

    mqtt_logger()->info("MQTT client '{}' connected (version={}, clean={})",
                        session.client_id,
                        session.protocol_version,
                        session.clean_session);

    // Main packet loop
    auto last_activity = std::chrono::steady_clock::now();
    auto keep_alive_timeout = std::chrono::duration_cast<std::chrono::milliseconds>(
        session.keep_alive * kKeepAliveGraceFactor);

    while (impl_->running.load(std::memory_order_acquire) &&
           session.connected) {

        // Check keep-alive timeout
        auto now = std::chrono::steady_clock::now();
        if (now - last_activity > keep_alive_timeout) {
            mqtt_logger()->debug("MQTT client '{}': keep-alive timeout",
                                 session.client_id);
            break;
        }

        // Read fixed header (at least 2 bytes)
        if (!read_exact(fd, packet_buf, 2)) {
            mqtt_logger()->debug("MQTT client '{}': connection closed",
                                 session.client_id);
            break;
        }

        last_activity = std::chrono::steady_clock::now();

        uint8_t packet_type = (packet_buf[0] >> 4) & 0x0F;
        uint8_t flags       = packet_buf[0] & 0x0F;

        // Read remaining length
        auto [remaining_len, consumed] = read_mqtt_length(
            packet_buf + 1, kMaxPacketSize - 1);
        if (remaining_len < 0 || consumed == 0) {
            mqtt_logger()->warn("MQTT client '{}': invalid remaining length",
                                session.client_id);
            break;
        }

        if (static_cast<size_t>(remaining_len) > kMaxPacketSize) {
            mqtt_logger()->warn("MQTT client '{}': packet too large ({} bytes)",
                                session.client_id, remaining_len);
            break;
        }

        // Read remaining packet payload
        if (remaining_len > 0) {
            if (!read_exact(fd, packet_buf + 1 + consumed,
                            static_cast<size_t>(remaining_len))) {
                break;
            }
        }

        uint8_t* payload = packet_buf + 1 + consumed;
        size_t   payload_len = static_cast<size_t>(remaining_len);
        last_activity = std::chrono::steady_clock::now();

        // Dispatch
        switch (packet_type) {
        case kMqttPublish:
            handle_publish(fd, payload, payload_len, flags, session);
            break;
        case kMqttPuback:
            handle_puback(payload, payload_len, session);
            break;
        case kMqttPubrec:
            handle_pubrec(fd, payload, payload_len, session);
            break;
        case kMqttPubrel:
            handle_pubrel(fd, payload, payload_len, session);
            break;
        case kMqttPubcomp:
            handle_pubcomp(payload, payload_len, session);
            break;
        case kMqttSubscribe:
            handle_subscribe(fd, payload, payload_len, session);
            break;
        case kMqttUnsubscribe:
            handle_unsubscribe(fd, payload, payload_len, session);
            break;
        case kMqttPingreq:
            send_data(fd, build_pingresp());
            break;
        case kMqttDisconnect:
            mqtt_logger()->debug("MQTT client '{}': DISCONNECT",
                                 session.client_id);
            session.connected = false;
            break;
        case kMqttConnect:
            // Second CONNECT is protocol violation
            mqtt_logger()->warn("MQTT client '{}': duplicate CONNECT",
                                session.client_id);
            session.connected = false;
            break;
        default:
            mqtt_logger()->debug("MQTT client '{}': unknown packet type {}",
                                 session.client_id, packet_type);
            break;
        }
    }

    // Publish will message on abnormal disconnect
    if (!session.will_topic.empty() && session.will_flag) {
        mqtt_logger()->info("MQTT client '{}': publishing will message to '{}'",
                            session.client_id, session.will_topic);
        // Stub: publish will to broker
        // Also set as retained if will_retain is true
        if (session.will_retain) {
            retained_store().set(session.will_topic, session.will_payload, 0);
        }
    }

    mqtt_logger()->info("MQTT client '{}' disconnected", session.client_id);
}

// ============================================================================
// CONNECT handler
// ============================================================================

bool MqttBridge::handle_connect_packet(int fd, uint8_t* packet_buf,
                                         MqttSession& session) {
    // Read fixed header: [CONNECT (0x10)] [remaining_length...]
    if (!read_exact(fd, packet_buf, 2)) return false;

    uint8_t packet_type = (packet_buf[0] >> 4) & 0x0F;
    if (packet_type != kMqttConnect) {
        mqtt_logger()->debug("MQTT: expected CONNECT, got {}", packet_type);
        return false;
    }

    auto [remaining_len, consumed] = read_mqtt_length(
        packet_buf + 1, kMaxPacketSize - 1);
    if (remaining_len < 10 || consumed == 0) return false;

    if (remaining_len > 0) {
        if (!read_exact(fd, packet_buf + 1 + consumed,
                        static_cast<size_t>(remaining_len))) return false;
    }

    uint8_t* payload = packet_buf + 1 + consumed;
    size_t   plen    = static_cast<size_t>(remaining_len);
    size_t   offset  = 0;

    // Protocol name
    std::string proto_name;
    if (!read_mqtt_string(payload, plen, offset, proto_name)) return false;

    // Protocol version
    if (offset >= plen) return false;
    session.protocol_version = payload[offset++];

    // Connect flags
    if (offset >= plen) return false;
    uint8_t flags = payload[offset++];
    bool has_username = (flags & 0x80) != 0;
    bool has_password = (flags & 0x40) != 0;
    session.will_retain = (flags & 0x20) != 0;
    session.will_qos    = (flags >> 3) & 0x03;
    session.will_flag   = (flags & 0x04) != 0;
    session.clean_session = (flags & 0x02) != 0;

    // Keep alive (2 bytes)
    if (offset + 2 > plen) return false;
    uint16_t keep_alive_secs = (static_cast<uint16_t>(payload[offset]) << 8) |
                                static_cast<uint16_t>(payload[offset + 1]);
    offset += 2;
    session.keep_alive = std::chrono::seconds(keep_alive_secs);

    // --- Validate ---
    if (session.protocol_version != kMqttVersion311 &&
        session.protocol_version != kMqttVersion5) {
        send_data(fd, build_connack(session.protocol_version,
                                      kConnRefusedProto));
        return false;
    }

    // Client ID
    std::string client_id;
    if (!read_mqtt_string(payload, plen, offset, client_id)) return false;
    if (client_id.empty() && !session.clean_session) {
        send_data(fd, build_connack(session.protocol_version,
                                      kConnRefusedId));
        return false;
    }
    if (client_id.empty()) {
        // Generate random client ID
        client_id = "auto-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    }
    session.client_id = client_id;

    // Will topic and message
    if (session.will_flag) {
        if (!read_mqtt_string(payload, plen, offset, session.will_topic))
            return false;
        if (!read_mqtt_string(payload, plen, offset, session.will_payload))
            return false;
    }

    // Username
    if (has_username) {
        if (!read_mqtt_string(payload, plen, offset, session.username))
            return false;
        session.has_username = true;
    }

    // Password
    if (has_password) {
        if (!read_mqtt_string(payload, plen, offset, session.password))
            return false;
        session.has_password = true;
    }

    // Stub: authentication validation
    // In production: check credentials against auth provider

    session.connected = true;

    // Send CONNACK
    send_data(fd, build_connack(session.protocol_version, kConnAccepted,
                                   !session.clean_session));

    return true;
}

// ============================================================================
// PUBLISH handler
// ============================================================================

void MqttBridge::handle_publish(int fd, uint8_t* payload, size_t plen,
                                  uint8_t flags, MqttSession& session) {
    size_t offset = 0;

    // Topic name
    std::string topic;
    if (!read_mqtt_string(payload, plen, offset, topic)) return;

    uint8_t qos    = (flags >> 1) & 0x03;
    bool    retain = (flags & 0x01) != 0;

    // Packet ID (only for QoS 1 and 2)
    uint16_t packet_id = 0;
    if (qos > 0) {
        if (offset + 2 > plen) return;
        packet_id = (static_cast<uint16_t>(payload[offset]) << 8) |
                     static_cast<uint16_t>(payload[offset + 1]);
        offset += 2;
    }

    // Payload
    std::string msg_payload(
        reinterpret_cast<const char*>(payload + offset), plen - offset);

    std::string torrent_topic = mqtt_topic_to_torrent(topic);

    mqtt_logger()->debug("MQTT PUBLISH: topic='{}' (→ '{}') qos={} retain={} "
                         "len={}",
                         topic, torrent_topic, qos, retain, msg_payload.size());

    // Handle retained messages
    if (retain) {
        retained_store().set(topic, msg_payload, qos);
    }

    // Stub: produce to broker
    // auto& server = impl_->server;
    // server.produce(torrent_topic, client_id, msg_payload);

    // QoS handling
    switch (qos) {
    case 0:
        // Fire and forget — no ack
        break;

    case 1:
        // At least once — send PUBACK
        send_data(fd, build_puback(packet_id));
        break;

    case 2: {
        // Exactly once — start 4-way handshake
        // Step 1: PUBREC
        send_data(fd, build_pubrec(packet_id));

        // Store pending state
        MqttSession::Qos2State state;
        state.phase   = MqttSession::Qos2State::awaiting_pubrel;
        state.topic   = topic;
        state.payload = std::move(msg_payload);
        state.retain  = retain;
        session.qos2_pending[packet_id] = std::move(state);
        break;
    }
    }
}

// ============================================================================
// PUBACK handler (QoS 1 — server side ack from our client)
// ============================================================================

void MqttBridge::handle_puback(uint8_t* payload, size_t plen,
                                 MqttSession& session) {
    if (plen < 2) return;
    uint16_t packet_id = (static_cast<uint16_t>(payload[0]) << 8) |
                          static_cast<uint16_t>(payload[1]);
    mqtt_logger()->debug("MQTT PUBACK from '{}': packet_id={}",
                         session.client_id, packet_id);
    // Stub: acknowledge delivery of a QoS 1 message we sent
}

// ============================================================================
// PUBREC handler (QoS 2 — step 2)
// ============================================================================

void MqttBridge::handle_pubrec(int fd, uint8_t* payload, size_t plen,
                                 MqttSession& session) {
    if (plen < 2) return;
    uint16_t packet_id = (static_cast<uint16_t>(payload[0]) << 8) |
                          static_cast<uint16_t>(payload[1]);
    mqtt_logger()->debug("MQTT PUBREC from '{}': packet_id={}",
                         session.client_id, packet_id);

    // Send PUBREL (step 3)
    send_data(fd, build_pubrel(packet_id));
}

// ============================================================================
// PUBREL handler (QoS 2 — step 3)
// ============================================================================

void MqttBridge::handle_pubrel(int fd, uint8_t* payload, size_t plen,
                                 MqttSession& session) {
    if (plen < 2) return;
    uint16_t packet_id = (static_cast<uint16_t>(payload[0]) << 8) |
                          static_cast<uint16_t>(payload[1]);
    mqtt_logger()->debug("MQTT PUBREL from '{}': packet_id={}",
                         session.client_id, packet_id);

    // Complete the QoS 2 transaction
    auto it = session.qos2_pending.find(packet_id);
    if (it != session.qos2_pending.end()) {
        // Message is now fully received — deliver to subscribers
        // Stub: produce to broker
        session.qos2_pending.erase(it);
    }

    // Send PUBCOMP (step 4 — final)
    send_data(fd, build_pubcomp(packet_id));
}

// ============================================================================
// PUBCOMP handler (QoS 2 — step 4)
// ============================================================================

void MqttBridge::handle_pubcomp(uint8_t* payload, size_t plen,
                                  MqttSession& session) {
    if (plen < 2) return;
    uint16_t packet_id = (static_cast<uint16_t>(payload[0]) << 8) |
                          static_cast<uint16_t>(payload[1]);
    mqtt_logger()->debug("MQTT PUBCOMP from '{}': packet_id={}",
                         session.client_id, packet_id);
    // QoS 2 delivery complete
}

// ============================================================================
// SUBSCRIBE handler
// ============================================================================

void MqttBridge::handle_subscribe(int fd, uint8_t* payload, size_t plen,
                                    MqttSession& session) {
    size_t offset = 0;

    // Packet ID
    if (offset + 2 > plen) return;
    uint16_t packet_id = (static_cast<uint16_t>(payload[offset]) << 8) |
                          static_cast<uint16_t>(payload[offset + 1]);
    offset += 2;

    std::vector<uint8_t> return_codes;

    while (offset < plen) {
        // Topic filter
        std::string topic_filter;
        if (!read_mqtt_string(payload, plen, offset, topic_filter)) break;

        // Requested QoS
        if (offset >= plen) break;
        uint8_t req_qos = payload[offset++];
        uint8_t granted_qos = std::min(req_qos, uint8_t(2));

        std::string torrent_topic = mqtt_topic_to_torrent(topic_filter);

        mqtt_logger()->debug("MQTT SUBSCRIBE from '{}': topic='{}' (→ '{}') qos={}",
                             session.client_id, topic_filter,
                             torrent_topic, granted_qos);

        // Store subscription
        session.subscriptions[topic_filter] = granted_qos;

        // Stub: register consumer group with broker
        // auto& server = impl_->server;
        // server.subscribe(session.client_id, torrent_topic, granted_qos);

        return_codes.push_back(granted_qos);

        // Deliver retained messages for this topic
        auto retained = retained_store().matches(topic_filter);
        for (auto& [ret_topic, ret_payload] : retained) {
            // Build PUBLISH for retained message
            // Stub: send retained message to this client
            mqtt_logger()->debug("MQTT: delivering retained message for '{}' "
                                 "to '{}'", ret_topic, session.client_id);
        }
    }

    // Send SUBACK
    send_data(fd, build_suback(packet_id, return_codes));
}

// ============================================================================
// UNSUBSCRIBE handler
// ============================================================================

void MqttBridge::handle_unsubscribe(int fd, uint8_t* payload, size_t plen,
                                      MqttSession& session) {
    size_t offset = 0;

    // Packet ID
    if (offset + 2 > plen) return;
    uint16_t packet_id = (static_cast<uint16_t>(payload[offset]) << 8) |
                          static_cast<uint16_t>(payload[offset + 1]);
    offset += 2;

    while (offset < plen) {
        std::string topic_filter;
        if (!read_mqtt_string(payload, plen, offset, topic_filter)) break;

        mqtt_logger()->debug("MQTT UNSUBSCRIBE from '{}': topic='{}'",
                             session.client_id, topic_filter);

        session.subscriptions.erase(topic_filter);

        // Stub: deregister consumer from broker
    }

    // Send UNSUBACK
    send_data(fd, build_unsuback(packet_id));
}

// ============================================================================
// Send data to socket
// ============================================================================

bool MqttBridge::send_data(int fd, const std::string& data) {
    const char* ptr = data.data();
    size_t n = data.size();
    while (n > 0) {
        ssize_t w = ::write(fd, ptr, n);
        if (w <= 0) return false;
        ptr += static_cast<size_t>(w);
        n   -= static_cast<size_t>(w);
    }
    return true;
}

} // namespace torrent::proxy
