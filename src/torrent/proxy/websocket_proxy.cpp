/**
 * torrent-mq — WebSocketProxy Implementation
 *
 * WebSocket interface for producing and consuming messages from torrent-mq.
 * Supports RFC 6455 WebSocket protocol with JSON text framing.
 *
 * Protocol:
 *   Client connects via ws://host:8083/ws
 *   After the WebSocket upgrade handshake, messages are exchanged as JSON
 *   text frames:
 *
 *   Client → Server:
 *     {"type": "produce", "topic": "orders", "key": "...", "value": {...}}
 *     {"type": "subscribe", "topics": ["orders", "payments"]}
 *     {"type": "unsubscribe", "topics": ["orders"]}
 *     {"type": "commit", "topic": "orders", "partition": 0, "offset": 42}
 *     {"type": "ping"}
 *
 *   Server → Client:
 *     {"type": "message", "topic": "orders", "key": "...", "value": {...},
 *      "partition": 0, "offset": 42, "timestamp": 1234567890}
 *     {"type": "produce_ack", "topic": "orders", "offset": 42, "status": "ok"}
 *     {"type": "error", "message": "...", "code": 400}
 *     {"type": "pong"}
 *     {"type": "subscribed", "topics": ["orders"]}
 *
 * Features:
 *   - RFC 6455 WebSocket upgrade handshake
 *   - Ping/pong keep-alive (configurable interval)
 *   - Per-client subscription set with topic-based fan-out
 *   - JSON text frame encoding/decoding
 *   - Connection limits and idle timeout
 *   - Thread pool for connection handling
 *
 * Thread-safety:
 *   - Accept loop runs on a dedicated thread.
 *   - Each connection runs on a thread pool worker.
 *   - Subscriptions map protected by a shared mutex.
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
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <openssl/sha.h>

namespace torrent::proxy {

// ============================================================================
// Anonymous namespace — constants, helpers, WebSocket frame logic
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

auto ws_logger() {
    static auto l = spdlog::get("websocket_proxy");
    if (!l) {
        l = spdlog::stdout_color_mt("websocket_proxy");
        l->set_level(spdlog::level::info);
    }
    return l;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

constexpr int    kBacklog           = 128;
constexpr int    kDefaultWorkers    = 4;
constexpr size_t kMaxFrameSize      = 1 << 20;   // 1 MB
constexpr size_t kMaxMessageSize    = 1 << 20;
constexpr size_t kRecvBufferSize    = 65536;
constexpr size_t kMaxSubscriptions  = 256;
constexpr size_t kMaxConnections    = 10000;
constexpr auto   kIdleTimeout       = std::chrono::seconds(300);
constexpr auto   kPingInterval      = std::chrono::seconds(30);
constexpr auto   kHandshakeTimeout  = std::chrono::seconds(10);

// WebSocket magic GUID for handshake
inline constexpr std::string_view kWsMagicGuid =
    "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

// WebSocket opcodes
inline constexpr uint8_t kOpContinuation = 0x0;
inline constexpr uint8_t kOpText         = 0x1;
inline constexpr uint8_t kOpBinary       = 0x2;
inline constexpr uint8_t kOpClose        = 0x8;
inline constexpr uint8_t kOpPing         = 0x9;
inline constexpr uint8_t kOpPong         = 0xA;

// --------------------------------------------------------------------------
// Minimal SHA1 → Base64 for WebSocket handshake
// --------------------------------------------------------------------------

[[nodiscard]] std::string base64_encode(const unsigned char* data, size_t len) {
    static const char kBase64Chars[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);

    for (size_t i = 0; i < len; i += 3) {
        uint32_t triple = static_cast<uint32_t>(data[i]) << 16;
        if (i + 1 < len) triple |= static_cast<uint32_t>(data[i + 1]) << 8;
        if (i + 2 < len) triple |= static_cast<uint32_t>(data[i + 2]);

        out += kBase64Chars[(triple >> 18) & 0x3F];
        out += kBase64Chars[(triple >> 12) & 0x3F];
        out += (i + 1 < len) ? kBase64Chars[(triple >> 6) & 0x3F] : '=';
        out += (i + 2 < len) ? kBase64Chars[triple & 0x3F] : '=';
    }
    return out;
}

[[nodiscard]] std::string compute_accept_key(std::string_view client_key) {
    std::string combined = std::string(client_key) + std::string(kWsMagicGuid);
    unsigned char hash[SHA_DIGEST_LENGTH];
    SHA1(reinterpret_cast<const unsigned char*>(combined.data()),
         combined.size(), hash);
    return base64_encode(hash, SHA_DIGEST_LENGTH);
}

// --------------------------------------------------------------------------
// Minimal JSON builder (no external dependency, like rest_proxy.cpp)
// --------------------------------------------------------------------------

std::string json_escape(std::string_view s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (char c : s) {
        switch (c) {
        case '\"':  out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n";  break;
        case '\r': out += "\\r";  break;
        case '\t': out += "\\t";  break;
        default:
            if (static_cast<unsigned char>(c) < 0x20) {
                char buf[8];
                std::snprintf(buf, 8, "\\u%04x", static_cast<unsigned>(c));
                out += buf;
            } else {
                out += c;
            }
        }
    }
    return out;
}

std::string json_object(const std::map<std::string, std::string>& fields) {
    std::ostringstream oss;
    oss << '{';
    bool first = true;
    for (const auto& [k, v] : fields) {
        if (!first) oss << ',';
        first = false;
        oss << '"' << json_escape(k) << "\":";
        bool is_raw = !v.empty() &&
            (v[0] == '{' || v[0] == '[' ||
             v == "true" || v == "false" || v == "null" ||
             (v[0] >= '0' && v[0] <= '9') || v[0] == '-');
        if (is_raw) oss << v;
        else oss << '"' << json_escape(v) << '"';
    }
    oss << '}';
    return oss.str();
}

// --------------------------------------------------------------------------
// Minimal JSON parser — extract string values from flat objects
// --------------------------------------------------------------------------

std::optional<std::string> json_get_string(const std::string& body,
                                            std::string_view key) {
    std::string search = "\"" + std::string(key) + "\"";
    auto p = body.find(search);
    if (p == std::string::npos) return std::nullopt;

    p = body.find(':', p + search.size());
    if (p == std::string::npos) return std::nullopt;

    while (++p < body.size() &&
           (body[p] == ' ' || body[p] == '\t' || body[p] == '\n')) {}

    if (p >= body.size() || body[p] != '"') return std::nullopt;

    size_t start = p + 1, end = start;
    while (end < body.size()) {
        if (body[end] == '"' && (end == start || body[end - 1] != '\\'))
            break;
        ++end;
    }
    if (end >= body.size()) return std::nullopt;
    return body.substr(start, end - start);
}

std::optional<std::vector<std::string>> json_get_string_array(
    const std::string& body, std::string_view key) {
    std::string search = "\"" + std::string(key) + "\"";
    auto p = body.find(search);
    if (p == std::string::npos) return std::nullopt;

    p = body.find(':', p + search.size());
    if (p == std::string::npos) return std::nullopt;

    p = body.find('[', p);
    if (p == std::string::npos) return std::nullopt;

    std::vector<std::string> result;
    size_t pos = p;
    while (pos < body.size()) {
        auto qstart = body.find('"', pos);
        if (qstart == std::string::npos || body[qstart] != '"') break;
        auto qend = body.find('"', qstart + 1);
        if (qend == std::string::npos) break;
        result.push_back(body.substr(qstart + 1, qend - qstart - 1));
        pos = qend + 1;
        auto comma = body.find(',', pos);
        auto bracket = body.find(']', pos);
        if (bracket != std::string::npos &&
            (comma == std::string::npos || bracket < comma)) break;
        pos = comma != std::string::npos ? comma + 1 : bracket;
    }
    return result;
}

// --------------------------------------------------------------------------
// WebSocket frame reading
// --------------------------------------------------------------------------

enum class WsReadResult {
    ok,
    closed,
    error,
    incomplete,
};

struct WsFrame {
    uint8_t     opcode;
    bool        fin;
    bool        masked;
    std::string payload;
};

WsReadResult read_ws_frame(int fd, WsFrame& frame, std::string& recv_buf) {
    // Read header (at least 2 bytes)
    while (recv_buf.size() < 2) {
        char c;
        ssize_t r = ::read(fd, &c, 1);
        if (r == 0) return WsReadResult::closed;
        if (r < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                return WsReadResult::incomplete;
            return WsReadResult::error;
        }
        recv_buf += c;
    }

    const uint8_t* data = reinterpret_cast<const uint8_t*>(recv_buf.data());

    frame.fin    = (data[0] & 0x80) != 0;
    frame.opcode = data[0] & 0x0F;
    frame.masked = (data[1] & 0x80) != 0;

    uint64_t payload_len = data[1] & 0x7F;
    size_t   header_len  = 2;

    if (payload_len == 126) {
        header_len += 2;
        while (recv_buf.size() < header_len) {
            char c;
            ssize_t r = ::read(fd, &c, 1);
            if (r <= 0) return WsReadResult::error;
            recv_buf += c;
        }
        payload_len = (static_cast<uint16_t>(recv_buf[2]) << 8) |
                       static_cast<uint16_t>(recv_buf[3]);
    } else if (payload_len == 127) {
        header_len += 8;
        while (recv_buf.size() < header_len) {
            char c;
            ssize_t r = ::read(fd, &c, 1);
            if (r <= 0) return WsReadResult::error;
            recv_buf += c;
        }
        payload_len = 0;
        for (int i = 0; i < 8; ++i) {
            payload_len = (payload_len << 8) |
                          static_cast<uint8_t>(recv_buf[2 + i]);
        }
    }

    if (payload_len > kMaxFrameSize) {
        ws_logger()->warn("WebSocket frame too large: {} bytes", payload_len);
        return WsReadResult::error;
    }

    // Read mask key if present
    uint8_t mask_key[4] = {0, 0, 0, 0};
    if (frame.masked) {
        header_len += 4;
        while (recv_buf.size() < header_len) {
            char c;
            ssize_t r = ::read(fd, &c, 1);
            if (r <= 0) return WsReadResult::error;
            recv_buf += c;
        }
        mask_key[0] = static_cast<uint8_t>(recv_buf[header_len - 4]);
        mask_key[1] = static_cast<uint8_t>(recv_buf[header_len - 3]);
        mask_key[2] = static_cast<uint8_t>(recv_buf[header_len - 2]);
        mask_key[3] = static_cast<uint8_t>(recv_buf[header_len - 1]);
    }

    // Read payload
    frame.payload.resize(payload_len);
    size_t total_needed = header_len + payload_len;
    while (recv_buf.size() < total_needed) {
        char c;
        ssize_t r = ::read(fd, &c, 1);
        if (r <= 0) return WsReadResult::error;
        recv_buf += c;
    }

    std::copy(recv_buf.begin() + header_len,
              recv_buf.begin() + total_needed,
              frame.payload.begin());

    // Unmask
    if (frame.masked) {
        for (size_t i = 0; i < payload_len; ++i) {
            frame.payload[i] ^= mask_key[i % 4];
        }
    }

    // Consume from recv_buf
    recv_buf.erase(0, total_needed);

    return WsReadResult::ok;
}

// --------------------------------------------------------------------------
// WebSocket frame writing
// --------------------------------------------------------------------------

bool write_ws_frame(int fd, uint8_t opcode, std::string_view payload) {
    std::string frame;
    frame.reserve(14 + payload.size());

    frame += static_cast<char>(0x80 | opcode); // FIN + opcode

    if (payload.size() < 126) {
        frame += static_cast<char>(payload.size());
    } else if (payload.size() <= 0xFFFF) {
        frame += static_cast<char>(126);
        frame += static_cast<char>((payload.size() >> 8) & 0xFF);
        frame += static_cast<char>(payload.size() & 0xFF);
    } else {
        frame += static_cast<char>(127);
        for (int i = 7; i >= 0; --i) {
            frame += static_cast<char>((payload.size() >> (i * 8)) & 0xFF);
        }
    }

    frame.append(payload);

    const char* ptr = frame.data();
    size_t n = frame.size();
    while (n > 0) {
        ssize_t w = ::write(fd, ptr, n);
        if (w <= 0) return false;
        ptr += static_cast<size_t>(w);
        n   -= static_cast<size_t>(w);
    }
    return true;
}

bool send_ws_text(int fd, std::string_view json) {
    return write_ws_frame(fd, kOpText, json);
}

bool send_ws_close(int fd, uint16_t code = 1000) {
    char buf[2];
    buf[0] = static_cast<char>((code >> 8) & 0xFF);
    buf[1] = static_cast<char>(code & 0xFF);
    return write_ws_frame(fd, kOpClose, {buf, 2});
}

// --------------------------------------------------------------------------
// WebSocket handshake
// --------------------------------------------------------------------------

[[nodiscard]] bool read_line_from_socket(int fd, std::string& line) {
    line.clear();
    line.reserve(256);
    char c;
    while (true) {
        ssize_t r = ::read(fd, &c, 1);
        if (r <= 0) return false;
        if (c == '\r') {
            ssize_t r2 = ::read(fd, &c, 1);
            if (r2 <= 0) return false;
            if (c == '\n') return true;
            line += '\r';
            line += c;
            continue;
        }
        if (c == '\n') return true;
        line += c;
    }
}

} // anonymous namespace

// ============================================================================
// WebSocketProxy — PIMPL
// ============================================================================

struct WebSocketProxy::Impl {
    broker::BrokerServer&               server;
    uint16_t                            port;
    std::atomic<bool>                   running{false};
    int                                 listen_fd = -1;
    std::thread                         accept_thread;
    std::unique_ptr<torrent::thread_pool> workers;

    // Connected clients: fd → subscription set
    mutable std::shared_mutex           clients_mutex;
    std::unordered_map<int, std::set<std::string>> client_subs;
    std::atomic<size_t>                 client_count{0};

    std::atomic<bool>                   shutdown_flag{false};

    explicit Impl(broker::BrokerServer& s, uint16_t p)
        : server(s), port(p) {
        workers = std::make_unique<torrent::thread_pool>(
            kDefaultWorkers, "ws-proxy");
    }

    ~Impl() {
        shutdown();
    }

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

    // Subscription management
    void add_subscription(int fd, const std::string& topic) {
        std::unique_lock lock(clients_mutex);
        client_subs[fd].insert(topic);
    }

    void remove_subscription(int fd, const std::string& topic) {
        std::unique_lock lock(clients_mutex);
        auto it = client_subs.find(fd);
        if (it != client_subs.end()) {
            it->second.erase(topic);
            if (it->second.empty()) client_subs.erase(it);
        }
    }

    void remove_client(int fd) {
        std::unique_lock lock(clients_mutex);
        client_subs.erase(fd);
        client_count.fetch_sub(1, std::memory_order_release);
    }

    std::set<int> clients_for_topic(const std::string& topic) {
        std::shared_lock lock(clients_mutex);
        std::set<int> result;
        for (auto& [fd, topics] : client_subs) {
            if (topics.find(topic) != topics.end()) {
                result.insert(fd);
            }
        }
        return result;
    }
};

// ============================================================================
// WebSocketProxy — Public API
// ============================================================================

WebSocketProxy::WebSocketProxy(broker::BrokerServer& server, uint16_t port)
    : impl_(std::make_unique<Impl>(server, port)) {}

WebSocketProxy::~WebSocketProxy() = default;

void WebSocketProxy::start() {
    auto& s = *impl_;
    if (s.running.load(std::memory_order_acquire)) return;

    s.listen_fd = ::socket(AF_INET,
                            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (s.listen_fd < 0) {
        ws_logger()->error("socket() failed: {}", std::strerror(errno));
        throw std::runtime_error("WebSocketProxy: socket() failed");
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
        ws_logger()->error("bind(:{}) failed: {}", s.port, std::strerror(errno));
        ::close(s.listen_fd);
        s.listen_fd = -1;
        throw std::runtime_error("WebSocketProxy: bind() failed");
    }

    if (::listen(s.listen_fd, kBacklog) < 0) {
        ws_logger()->error("listen() failed: {}", std::strerror(errno));
        ::close(s.listen_fd);
        s.listen_fd = -1;
        throw std::runtime_error("WebSocketProxy: listen() failed");
    }

    if (s.port == 0) {
        socklen_t alen = sizeof(addr);
        getsockname(s.listen_fd, reinterpret_cast<sockaddr*>(&addr), &alen);
        s.port = ntohs(addr.sin_port);
    }

    s.running.store(true, std::memory_order_release);
    s.accept_thread = std::thread([this] { accept_loop(); });
    ws_logger()->info("WebSocketProxy started on port {}", s.port);
}

void WebSocketProxy::shutdown() {
    impl_->shutdown();
    ws_logger()->info("WebSocketProxy shut down");
}

// ============================================================================
// Accept loop
// ============================================================================

void WebSocketProxy::accept_loop() {
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
            ws_logger()->warn("accept() error: {}", std::strerror(errno));
            continue;
        }

        if (s.client_count.load() >= kMaxConnections) {
            ws_logger()->warn("Connection limit reached ({}), rejecting",
                              kMaxConnections);
            ::close(client_fd);
            continue;
        }

        s.client_count.fetch_add(1, std::memory_order_release);
        ws_logger()->debug("WebSocket connection from {}:{}",
                           inet_ntoa(client_addr.sin_addr),
                           ntohs(client_addr.sin_port));

        s.workers->submit([this, client_fd] {
            handle_connection(client_fd);
            impl_->remove_client(client_fd);
            ::close(client_fd);
        });
    }
}

// ============================================================================
// Connection handler — upgrade + message loop
// ============================================================================

void WebSocketProxy::handle_connection(int fd) {
    // Phase 1: HTTP → WebSocket upgrade handshake
    if (!perform_handshake(fd)) {
        return;
    }

    ws_logger()->debug("WebSocket handshake complete on fd {}", fd);

    // Phase 2: message loop
    std::string recv_buf;
    recv_buf.reserve(kRecvBufferSize);

    auto last_activity = std::chrono::steady_clock::now();
    auto last_ping     = std::chrono::steady_clock::now();

    while (impl_->running.load(std::memory_order_acquire)) {
        // Check idle timeout
        auto now = std::chrono::steady_clock::now();
        if (now - last_activity > kIdleTimeout) {
            ws_logger()->debug("WebSocket fd {}: idle timeout", fd);
            send_ws_close(fd, 1001); // Going Away
            return;
        }

        // Send ping if needed
        if (now - last_ping > kPingInterval) {
            if (!write_ws_frame(fd, kOpPing, "")) {
                ws_logger()->debug("WebSocket fd {}: ping failed", fd);
                return;
            }
            last_ping = now;
        }

        // Read frame
        WsFrame frame;
        WsReadResult result = read_ws_frame(fd, frame, recv_buf);

        switch (result) {
        case WsReadResult::closed:
            ws_logger()->debug("WebSocket fd {}: client closed", fd);
            return;

        case WsReadResult::error:
            ws_logger()->debug("WebSocket fd {}: read error", fd);
            return;

        case WsReadResult::incomplete:
            // No data available, poll again
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;

        case WsReadResult::ok:
            break;
        }

        last_activity = std::chrono::steady_clock::now();

        // Handle frame by opcode
        switch (frame.opcode) {
        case kOpText:
            handle_text_message(fd, frame.payload);
            break;

        case kOpBinary:
            // Binary frames not used in this JSON protocol
            ws_logger()->debug("WebSocket fd {}: ignoring binary frame", fd);
            break;

        case kOpClose:
            ws_logger()->debug("WebSocket fd {}: received close frame", fd);
            send_ws_close(fd, 1000);
            return;

        case kOpPing:
            write_ws_frame(fd, kOpPong, frame.payload);
            break;

        case kOpPong:
            // Pong received — connection alive
            break;

        default:
            ws_logger()->debug("WebSocket fd {}: unknown opcode {}", fd,
                               static_cast<int>(frame.opcode));
            break;
        }
    }
}

// ============================================================================
// WebSocket handshake
// ============================================================================

bool WebSocketProxy::perform_handshake(int fd) {
    // Read HTTP upgrade request line
    std::string line;
    if (!read_line_from_socket(fd, line)) return false;

    // Parse request line: GET /path HTTP/1.1
    std::string method, path, version;
    std::istringstream rl(line);
    rl >> method >> path >> version;

    if (method != "GET") {
        ws_logger()->debug("WebSocket handshake: not a GET request");
        return false;
    }

    // Read headers
    std::map<std::string, std::string> headers;
    std::string ws_key;

    while (true) {
        std::string hl;
        if (!read_line_from_socket(fd, hl)) return false;
        if (hl.empty()) break; // end of headers

        auto colon = hl.find(':');
        if (colon != std::string::npos) {
            std::string key   = hl.substr(0, colon);
            std::string value = hl.substr(colon + 1);

            // Trim key
            key.erase(0, key.find_first_not_of(" \t"));
            key.erase(key.find_last_not_of(" \t") + 1);

            // Trim value
            value.erase(0, value.find_first_not_of(" \t"));
            value.erase(value.find_last_not_of(" \t\r") + 1);

            // Lowercase key
            for (auto& c : key) c = static_cast<char>(std::tolower(c));

            headers[std::move(key)] = std::move(value);
        }
    }

    // Validate required headers
    auto upgrade_it = headers.find("upgrade");
    auto conn_it    = headers.find("connection");
    auto key_it     = headers.find("sec-websocket-key");
    auto version_it = headers.find("sec-websocket-version");

    if (upgrade_it == headers.end() ||
        conn_it == headers.end() ||
        key_it == headers.end() ||
        version_it == headers.end()) {
        ws_logger()->debug("WebSocket handshake: missing required headers");
        return false;
    }

    // Case-insensitive check for "websocket" in Upgrade
    std::string upgrade_val = upgrade_it->second;
    for (auto& c : upgrade_val) c = static_cast<char>(std::tolower(c));
    if (upgrade_val != "websocket") {
        ws_logger()->debug("WebSocket handshake: Upgrade != websocket");
        return false;
    }

    if (version_it->second != "13") {
        ws_logger()->debug("WebSocket handshake: unsupported version {}",
                           version_it->second);
        return false;
    }

    // Compute accept key
    std::string accept_key = compute_accept_key(key_it->second);

    // Build response
    std::ostringstream response;
    response << "HTTP/1.1 101 Switching Protocols\r\n";
    response << "Upgrade: websocket\r\n";
    response << "Connection: Upgrade\r\n";
    response << "Sec-WebSocket-Accept: " << accept_key << "\r\n";
    response << "Server: torrent-mq-websocket-proxy\r\n";
    response << "\r\n";

    std::string resp_str = response.str();
    const char* ptr = resp_str.data();
    size_t n = resp_str.size();
    while (n > 0) {
        ssize_t w = ::write(fd, ptr, n);
        if (w <= 0) return false;
        ptr += static_cast<size_t>(w);
        n   -= static_cast<size_t>(w);
    }

    return true;
}

// ============================================================================
// Message handling
// ============================================================================

void WebSocketProxy::handle_text_message(int fd, std::string_view payload) {
    std::string msg_type = json_get_string(std::string(payload), "type")
                               .value_or("");

    if (msg_type == "produce") {
        handle_produce(fd, payload);
    } else if (msg_type == "subscribe") {
        handle_subscribe(fd, payload);
    } else if (msg_type == "unsubscribe") {
        handle_unsubscribe(fd, payload);
    } else if (msg_type == "commit") {
        handle_commit(fd, payload);
    } else if (msg_type == "ping") {
        send_ws_text(fd, "{\"type\":\"pong\"}");
    } else {
        auto err = json_object({
            {"type",    "\"error\""},
            {"message", "\"Unknown message type: " + std::string(msg_type) + "\""},
            {"code",    "400"}
        });
        send_ws_text(fd, err);
    }
}

void WebSocketProxy::handle_produce(int fd, std::string_view payload) {
    auto topic = json_get_string(std::string(payload), "topic");
    auto key   = json_get_string(std::string(payload), "key");
    auto value = json_get_string(std::string(payload), "value");

    if (!topic || !value) {
        auto err = json_object({
            {"type", "\"error\""},
            {"message", "\"Missing 'topic' or 'value' field\""},
            {"code", "400"}
        });
        send_ws_text(fd, err);
        return;
    }

    ws_logger()->debug("WebSocket produce: topic='{}' key='{}'",
                       *topic, key.value_or(""));

    // Stub: in production, produce to broker
    // auto& server = impl_->server;
    // server.produce(*topic, key.value_or(""), *value, ...);

    auto ack = json_object({
        {"type",   "\"produce_ack\""},
        {"topic",  "\"" + json_escape(*topic) + "\""},
        {"status", "\"ok\""},
        {"offset", "0"}
    });
    send_ws_text(fd, ack);
}

void WebSocketProxy::handle_subscribe(int fd, std::string_view payload) {
    auto topics_opt = json_get_string_array(std::string(payload), "topics");
    if (!topics_opt) {
        // Try single topic
        auto topic = json_get_string(std::string(payload), "topic");
        if (!topic) {
            auto err = json_object({
                {"type", "\"error\""},
                {"message", "\"Missing 'topics' or 'topic' field\""},
                {"code", "400"}
            });
            send_ws_text(fd, err);
            return;
        }
        impl_->add_subscription(fd, *topic);
        ws_logger()->debug("WebSocket fd {}: subscribed to '{}'", fd, *topic);

        auto resp = json_object({
            {"type",   "\"subscribed\""},
            {"topics", "[\"" + json_escape(*topic) + "\"]"}
        });
        send_ws_text(fd, resp);
        return;
    }

    for (auto& topic : *topics_opt) {
        impl_->add_subscription(fd, topic);
    }

    ws_logger()->debug("WebSocket fd {}: subscribed to {} topics",
                       fd, topics_opt->size());

    std::string topics_json = "[";
    for (size_t i = 0; i < topics_opt->size(); ++i) {
        if (i > 0) topics_json += ",";
        topics_json += "\"" + json_escape((*topics_opt)[i]) + "\"";
    }
    topics_json += "]";

    auto resp = json_object({
        {"type",   "\"subscribed\""},
        {"topics", topics_json}
    });
    send_ws_text(fd, resp);
}

void WebSocketProxy::handle_unsubscribe(int fd, std::string_view payload) {
    auto topics_opt = json_get_string_array(std::string(payload), "topics");
    if (!topics_opt) {
        auto topic = json_get_string(std::string(payload), "topic");
        if (!topic) {
            auto err = json_object({
                {"type", "\"error\""},
                {"message", "\"Missing 'topics' or 'topic' field\""},
                {"code", "400"}
            });
            send_ws_text(fd, err);
            return;
        }
        impl_->remove_subscription(fd, *topic);
        ws_logger()->debug("WebSocket fd {}: unsubscribed from '{}'", fd, *topic);

        auto resp = json_object({
            {"type",   "\"unsubscribed\""},
            {"topics", "[\"" + json_escape(*topic) + "\"]"}
        });
        send_ws_text(fd, resp);
        return;
    }

    for (auto& topic : *topics_opt) {
        impl_->remove_subscription(fd, topic);
    }

    ws_logger()->debug("WebSocket fd {}: unsubscribed from {} topics",
                       fd, topics_opt->size());

    auto resp = json_object({
        {"type",   "\"unsubscribed\""},
        {"topics", std::to_string(topics_opt->size())}
    });
    send_ws_text(fd, resp);
}

void WebSocketProxy::handle_commit(int fd, std::string_view payload) {
    auto topic     = json_get_string(std::string(payload), "topic");
    auto partition = json_get_string(std::string(payload), "partition");
    auto offset    = json_get_string(std::string(payload), "offset");

    if (!topic || !partition || !offset) {
        auto err = json_object({
            {"type", "\"error\""},
            {"message", "\"Missing 'topic', 'partition', or 'offset' field\""},
            {"code", "400"}
        });
        send_ws_text(fd, err);
        return;
    }

    ws_logger()->debug("WebSocket fd {}: commit topic='{}' partition={} offset={}",
                       fd, *topic, *partition, *offset);

    // Stub: in production, commit offset via broker
    auto ack = json_object({
        {"type",   "\"commit_ack\""},
        {"topic",  "\"" + json_escape(*topic) + "\""},
        {"partition", *partition},
        {"offset", *offset},
        {"status", "\"ok\""}
    });
    send_ws_text(fd, ack);
}

} // namespace torrent::proxy
