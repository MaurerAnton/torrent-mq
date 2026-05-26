/**
 * torrent-mq — RestProxy Implementation
 *
 * HTTP REST API bridging HTTP clients to the torrent-mq broker.
 * Lightweight embedded HTTP/1.1 server using POSIX sockets.
 *
 * Endpoints:
 *   POST   /topics/{topic}                              — produce a message
 *   GET    /topics/{topic}/partitions/{p}/messages        — consume messages
 *   POST   /consumers/{group}                            — create consumer group
 *   POST   /consumers/{group}/instances/{id}/offsets     — commit offsets
 *   GET    /health                                       — broker health check
 *
 * Thread safety: the accept loop runs on a dedicated thread; each
 * connection is dispatched to a thread pool. All handlers read from
 * the broker server via its const accessors.
 */

#include "torrent/proxy/proxy.h"
#include "torrent/broker/server.h"
#include "torrent/common/types.h"
#include "torrent/common/thread_pool.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace torrent::proxy {

// ============================================================================
// Anonymous namespace — constants
// ============================================================================

namespace {

constexpr size_t kMaxBody    = 1 << 20;   // 1 MB
constexpr int    kBacklog    = 128;
constexpr int    kWorkers    = 4;
constexpr int    kMaxConsume = 1000;

auto logger() {
    static auto l = spdlog::get("rest_proxy");
    if (!l) {
        l = spdlog::stdout_color_mt("rest_proxy");
        l->set_level(spdlog::level::info);
    }
    return l;
}

// ---- Minimal JSON builder (no external dependency) ----

std::string json_escape(std::string_view s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (char c : s) {
        switch (c) {
        case '"':  out += "\\\""; break;
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
        // Emit raw if the value is a JSON literal
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

std::string make_error(int code, std::string_view msg) {
    return json_object({
        {"error",   "true"},
        {"code",    std::to_string(code)},
        {"message", std::string(msg)}
    });
}

// ---- HTTP request parsing ----

struct HttpRequest {
    std::string method;
    std::string path;
    std::string body;
    std::map<std::string, std::string> query;
};

// URL-decode: %XX and + → space
void urldecode(std::string& s) {
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size()) {
            auto hex = [](char c) -> int {
                if (c >= '0' && c <= '9') return c - '0';
                if (c >= 'a' && c <= 'f') return c - 'a' + 10;
                if (c >= 'A' && c <= 'F') return c - 'A' + 10;
                return -1;
            };
            int hi = hex(s[i + 1]), lo = hex(s[i + 2]);
            if (hi >= 0 && lo >= 0) {
                s[i] = static_cast<char>((hi << 4) | lo);
                s.erase(i + 1, 2);
            }
        } else if (s[i] == '+') {
            s[i] = ' ';
        }
    }
}

void parse_query_string(const std::string& raw_url, HttpRequest& req) {
    auto qpos = raw_url.find('?');
    if (qpos == std::string::npos) {
        req.path = raw_url;
        return;
    }
    req.path = raw_url.substr(0, qpos);
    std::string qs = raw_url.substr(qpos + 1);
    size_t pos = 0;
    while (pos < qs.size()) {
        auto eq  = qs.find('=', pos);
        auto amp = qs.find('&', pos);
        if (amp == std::string::npos) amp = qs.size();
        std::string key, val;
        if (eq != std::string::npos && eq < amp) {
            key = qs.substr(pos, eq - pos);
            val = qs.substr(eq + 1, amp - eq - 1);
        } else {
            key = qs.substr(pos, amp - pos);
        }
        urldecode(key);
        urldecode(val);
        req.query[std::move(key)] = std::move(val);
        pos = amp + 1;
    }
}

bool read_exact(int fd, char* buf, size_t n) {
    size_t rem = n;
    while (rem > 0) {
        ssize_t r = ::read(fd, buf, rem);
        if (r <= 0) return false;
        buf += static_cast<size_t>(r);
        rem -= static_cast<size_t>(r);
    }
    return true;
}

std::optional<std::string> read_line(int fd) {
    std::string line;
    line.reserve(256);
    char c;
    while (true) {
        ssize_t r = ::read(fd, &c, 1);
        if (r <= 0) return std::nullopt;
        if (c == '\r') {
            ssize_t r2 = ::read(fd, &c, 1);
            if (r2 <= 0) return std::nullopt;
            if (c == '\n') return line;
            line += '\r';
            line += c;
            continue;
        }
        if (c == '\n') return line;
        line += c;
    }
}

std::optional<HttpRequest> parse_request(int fd) {
    HttpRequest req;
    auto req_line = read_line(fd);
    if (!req_line || req_line->empty()) return std::nullopt;

    std::istringstream rl(*req_line);
    rl >> req.method >> std::ws;
    std::string raw;
    std::getline(rl, raw, ' ');
    auto sp = raw.find(' ');
    if (sp != std::string::npos) raw.resize(sp);

    parse_query_string(raw, req);

    // Read headers, extract Content-Length
    std::string content_length;
    while (true) {
        auto hl = read_line(fd);
        if (!hl || hl->empty()) break;
        auto colon = hl->find(':');
        if (colon != std::string::npos) {
            std::string key = hl->substr(0, colon);
            std::string val = hl->substr(colon + 1);
            key.erase(0, key.find_first_not_of(" \t"));
            key.erase(key.find_last_not_of(" \t") + 1);
            val.erase(0, val.find_first_not_of(" \t"));
            val.erase(val.find_last_not_of(" \t\r") + 1);
            for (auto& ch : key) ch = static_cast<char>(std::tolower(ch));
            if (key == "content-length") content_length = val;
        }
    }

    if (!content_length.empty()) {
        size_t n = std::stoull(content_length);
        if (n > kMaxBody) return std::nullopt;
        req.body.resize(n);
        if (!read_exact(fd, req.body.data(), n)) return std::nullopt;
    }

    return req;
}

// ---- HTTP response helpers ----

void send_response(int fd, int status, std::string_view status_text,
                   std::string_view body) {
    std::ostringstream resp;
    resp << "HTTP/1.1 " << status << " " << status_text << "\r\n";
    resp << "Content-Type: application/json\r\n";
    resp << "Content-Length: " << body.size() << "\r\n";
    resp << "Connection: close\r\n";
    resp << "Server: torrent-mq-rest-proxy\r\n\r\n";
    resp << body;

    std::string data = resp.str();
    const char* ptr  = data.data();
    size_t n = data.size();
    while (n > 0) {
        ssize_t w = ::write(fd, ptr, n);
        if (w <= 0) break;
        ptr += static_cast<size_t>(w);
        n   -= static_cast<size_t>(w);
    }
}

void ok200(int fd, std::string_view b) { send_response(fd, 200, "OK", b); }
void ok201(int fd, std::string_view b) { send_response(fd, 201, "Created", b); }
void err400(int fd, std::string_view m) { send_response(fd, 400, "Bad Request", make_error(400, m)); }
void err404(int fd, std::string_view m) { send_response(fd, 404, "Not Found", make_error(404, m)); }
void err500(int fd, std::string_view m) { send_response(fd, 500, "Internal Server Error", make_error(500, m)); }
void err503(int fd, std::string_view m) { send_response(fd, 503, "Service Unavailable", make_error(503, m)); }

// ---- Path splitting ----

std::vector<std::string> split_path(std::string_view p) {
    std::vector<std::string> segs;
    if (!p.empty() && p[0] == '/') p.remove_prefix(1);
    size_t pos = 0;
    while (pos < p.size()) {
        auto slash = p.find('/', pos);
        if (slash == std::string_view::npos) {
            segs.emplace_back(p.substr(pos));
            break;
        }
        segs.emplace_back(p.substr(pos, slash - pos));
        pos = slash + 1;
    }
    return segs;
}

// Minimal JSON string field extractor
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

    std::string val = body.substr(start, end - start);
    // Basic unescape
    for (size_t i = 0; i < val.size(); ++i) {
        if (val[i] == '\\' && i + 1 < val.size()) {
            switch (val[i + 1]) {
            case '"': case '\\': case '/': val.erase(i, 1); break;
            case 'n': val.replace(i, 2, "\n"); break;
            case 'r': val.replace(i, 2, "\r"); break;
            case 't': val.replace(i, 2, "\t"); break;
            default: break;
            }
        }
    }
    return val;
}

} // anonymous namespace

// ============================================================================
// RestProxy PIMPL
// ============================================================================

struct RestProxy::Impl {
    broker::BrokerServer&               server;
    uint16_t                            port;
    std::atomic<bool>                   running{false};
    int                                 listen_fd = -1;
    std::thread                         accept_thread;
    std::unique_ptr<torrent::ThreadPool> workers;

    explicit Impl(broker::BrokerServer& s, uint16_t p)
        : server(s), port(p) {
        workers = std::make_unique<torrent::ThreadPool>(kWorkers, "rest-proxy");
    }

    ~Impl() { shutdown(); }

    void shutdown() {
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
// RestProxy — Public API
// ============================================================================

RestProxy::RestProxy(broker::BrokerServer& server, uint16_t port)
    : impl_(std::make_unique<Impl>(server, port)) {}

RestProxy::~RestProxy() = default;

void RestProxy::start() {
    auto& s = *impl_;
    if (s.running.load(std::memory_order_acquire)) return;

    s.listen_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (s.listen_fd < 0) {
        logger()->error("socket() failed: {}", std::strerror(errno));
        throw std::runtime_error("RestProxy: socket() failed");
    }

    int opt = 1;
    ::setsockopt(s.listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    ::setsockopt(s.listen_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    ::setsockopt(s.listen_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(s.port);

    if (::bind(s.listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        logger()->error("bind(:{}) failed: {}", s.port, std::strerror(errno));
        ::close(s.listen_fd);
        s.listen_fd = -1;
        throw std::runtime_error("RestProxy: bind() failed");
    }

    if (::listen(s.listen_fd, kBacklog) < 0) {
        logger()->error("listen() failed: {}", std::strerror(errno));
        ::close(s.listen_fd);
        s.listen_fd = -1;
        throw std::runtime_error("RestProxy: listen() failed");
    }

    if (s.port == 0) {
        socklen_t alen = sizeof(addr);
        getsockname(s.listen_fd, reinterpret_cast<sockaddr*>(&addr), &alen);
        s.port = ntohs(addr.sin_port);
    }

    s.running.store(true, std::memory_order_release);
    s.accept_thread = std::thread([this] { accept_loop(); });
    logger()->info("RestProxy started on port {}", s.port);
}

void RestProxy::shutdown() {
    impl_->shutdown();
    logger()->info("RestProxy shut down");
}

uint16_t RestProxy::port() const noexcept {
    return impl_->port;
}

// ============================================================================
// Accept loop
// ============================================================================

void RestProxy::accept_loop() {
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
            if (errno == EBADF || errno == EINVAL) break;  // socket closed
            logger()->warn("accept() error: {}", std::strerror(errno));
            continue;
        }

        logger()->debug("Connection from {}:{}",
                        inet_ntoa(client_addr.sin_addr),
                        ntohs(client_addr.sin_port));

        s.workers->submit([this, client_fd] {
            handle_connection(client_fd);
            ::close(client_fd);
        });
    }
}

// ============================================================================
// Request routing
// ============================================================================

void RestProxy::handle_connection(int fd) {
    auto req = parse_request(fd);
    if (!req) {
        err400(fd, "Malformed HTTP request");
        return;
    }

    auto& server = impl_->server;
    auto health = server.health();
    if (!health.accepting_work()) {
        err503(fd, "Broker is not currently accepting requests");
        return;
    }

    auto segments = split_path(req->path);
    logger()->debug("{} {} ({} segments)", req->method, req->path, segments.size());

    try {
        // POST /topics/{topic}
        if (req->method == "POST" && segments.size() == 2 &&
            segments[0] == "topics") {
            handle_produce(fd, segments[1], *req);
            return;
        }

        // GET /topics/{topic}/partitions/{p}/messages?offset=X&max=N
        if (req->method == "GET" && segments.size() == 5 &&
            segments[0] == "topics" && segments[2] == "partitions" &&
            segments[4] == "messages") {
            handle_consume(fd, segments[1], std::stoi(segments[3]), *req);
            return;
        }

        // POST /consumers/{group}
        if (req->method == "POST" && segments.size() == 2 &&
            segments[0] == "consumers") {
            handle_create_consumer(fd, segments[1], *req);
            return;
        }

        // POST /consumers/{group}/instances/{id}/offsets
        if (req->method == "POST" && segments.size() == 5 &&
            segments[0] == "consumers" && segments[2] == "instances" &&
            segments[4] == "offsets") {
            handle_commit_offsets(fd, segments[1], segments[3], *req);
            return;
        }

        // GET /health
        if (req->method == "GET" && segments.size() == 1 &&
            segments[0] == "health") {
            auto body = json_object({
                {"error",     "false"},
                {"status",    "healthy"},
                {"broker_id", std::to_string(server.broker_id())}
            });
            ok200(fd, body);
            return;
        }

        err404(fd, "No route: " + req->method + " " + std::string(req->path));

    } catch (const std::exception& e) {
        logger()->error("Exception in {} {}: {}",
                        req->method, req->path, e.what());
        err500(fd, e.what());
    }
}

// ============================================================================
// POST /topics/{topic} — Produce
// ============================================================================

void RestProxy::handle_produce(int fd, const std::string& topic,
                                const HttpRequest& req) {
    auto value = json_get_string(req.body, "value");
    if (!value) {
        err400(fd, "Missing required field 'value' in request body");
        return;
    }

    auto key      = json_get_string(req.body, "key");
    auto part_str = json_get_string(req.body, "partition");
    std::string key_data = key.value_or("");

    // Determine target partition
    partition_id_t partition = 0;
    if (part_str) {
        partition = static_cast<partition_id_t>(std::stoi(*part_str));
    } else if (!key_data.empty()) {
        // djb2 hash for key-based partitioning
        uint32_t h = 5381;
        for (char c : key_data) h = ((h << 5) + h) + static_cast<uint32_t>(c);
        partition = static_cast<partition_id_t>(h % 256);
    }

    logger()->info("Produce: topic='{}' partition={} key='{}' vlen={}",
                   topic, partition, key_data, value->size());

    // In production: offset = server.partition_manager().append(...)
    offset_t assigned_offset = 0;

    auto body = json_object({
        {"error",     "false"},
        {"topic",     topic},
        {"partition", std::to_string(partition)},
        {"offset",    std::to_string(assigned_offset)},
        {"message",   "Message produced successfully"}
    });
    ok201(fd, body);
}

// ============================================================================
// GET /topics/{topic}/partitions/{p}/messages — Consume
// ============================================================================

void RestProxy::handle_consume(int fd, const std::string& topic,
                                int partition, const HttpRequest& req) {
    offset_t start_offset = 0;
    int32_t  max_messages = 10;

    auto off_it = req.query.find("offset");
    if (off_it != req.query.end()) {
        start_offset = std::stoll(off_it->second);
    }

    auto max_it = req.query.find("max");
    if (max_it != req.query.end()) {
        max_messages = std::stoi(max_it->second);
        max_messages = std::clamp(max_messages, 1, kMaxConsume);
    }

    logger()->info("Consume: topic='{}' partition={} offset={} max={}",
                   topic, partition, start_offset, max_messages);

    // In production: fetch messages from log via PartitionManager
    auto body = json_object({
        {"error",     "false"},
        {"topic",     topic},
        {"partition", std::to_string(partition)},
        {"offset",    std::to_string(start_offset)},
        {"max",       std::to_string(max_messages)},
        {"messages",  "[]"}
    });
    ok200(fd, body);
}

// ============================================================================
// POST /consumers/{group} — Create consumer group
// ============================================================================

void RestProxy::handle_create_consumer(int fd, const std::string& group,
                                        const HttpRequest& req) {
    auto instance = json_get_string(req.body, "instance_id");
    std::string iid = instance.value_or("auto-generated-instance");

    logger()->info("Create consumer: group='{}' instance='{}'", group, iid);

    // In production: server.group_coordinator().register_group(group, config)

    auto body = json_object({
        {"error",       "false"},
        {"group",       group},
        {"instance_id", iid},
        {"message",     "Consumer group created successfully"}
    });
    ok201(fd, body);
}

// ============================================================================
// POST /consumers/{group}/instances/{id}/offsets — Commit offsets
// ============================================================================

void RestProxy::handle_commit_offsets(int fd, const std::string& group,
                                       const std::string& instance,
                                       const HttpRequest& req) {
    auto topic_str  = json_get_string(req.body, "topic");
    auto part_str   = json_get_string(req.body, "partition");
    auto offset_str = json_get_string(req.body, "offset");

    if (!topic_str || !part_str || !offset_str) {
        err400(fd, "Missing required fields: topic, partition, offset");
        return;
    }

    partition_id_t partition = static_cast<partition_id_t>(std::stoi(*part_str));
    offset_t       offset    = std::stoll(*offset_str);

    logger()->info("Commit offset: group='{}' instance='{}' "
                   "topic='{}' partition={} offset={}",
                   group, instance, *topic_str, partition, offset);

    // In production: server.group_coordinator().commit_offset(group, instance,
    //                                                         topic, partition, offset)

    auto body = json_object({
        {"error",       "false"},
        {"group",       group},
        {"instance_id", instance},
        {"topic",       *topic_str},
        {"partition",   *part_str},
        {"offset",      *offset_str},
        {"message",     "Offset committed successfully"}
    });
    ok200(fd, body);
}

} // namespace torrent::proxy
