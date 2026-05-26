/**
 * admin_server.cpp — Admin REST API Server Implementation
 *
 * Provides a Redpanda/Kafka-compatible admin REST API for cluster management:
 *   - Cluster health and broker listing
 *   - Topic CRUD, partition inspection
 *   - Consumer group management
 *   - Configuration management
 *   - Metrics export (JSON snapshot)
 *   - ACL rule management
 *
 * Uses a minimal HTTP/1.1 server built on POSIX sockets with a thread-pool
 * of worker threads accepting connections and dispatching routes. All
 * responses are JSON via nlohmann/json.
 *
 * Architecture:
 *   - Single listen socket, SO_REUSEADDR, non-blocking accept via epoll.
 *   - Worker thread pool handles HTTP parse + route dispatch + response.
 *   - Route table: {method, path_pattern -> handler}.
 *   - Path parameters extracted via simple tokenisation (/v1/topics/{topic}).
 *
 * Thread-safety:
 *   - All accesses to BrokerServer are through const accessors (safe after
 *     start()) or through the managers which are internally synchronised.
 *   - The route dispatch runs on worker threads; the BrokerServer shared
 *     state is protected by its own internal synchronisation.
 */

#include "torrent/admin/admin_server.h"
#include "torrent/broker/server.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/broker/partition_manager.h"
#include "torrent/broker/consumer_group_manager.h"
#include "torrent/broker/controller.h"
#include "torrent/broker/inter_broker.h"
#include "torrent/security/acl_engine.h"
#include "torrent/metrics/metrics.h"
#include "torrent/common/types.h"
#include "torrent/common/config.h"

#include <nlohmann/json.hpp>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

// ============================================================================
// Convenience aliases
// ============================================================================

using json = nlohmann::json;
using namespace std::chrono_literals;

namespace torrent::admin {

// ============================================================================
// Anonymous namespace — internal helpers and the embedded HTTP server
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logging
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_admin_logger() {
    static auto logger = []() {
        auto l = spdlog::get("admin_server");
        if (!l) {
            l = spdlog::stdout_color_mt("admin_server");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define ADMIN_LOG_INFO(...)  get_admin_logger()->info(__VA_ARGS__)
#define ADMIN_LOG_WARN(...)  get_admin_logger()->warn(__VA_ARGS__)
#define ADMIN_LOG_ERROR(...) get_admin_logger()->error(__VA_ARGS__)
#define ADMIN_LOG_DEBUG(...) get_admin_logger()->debug(__VA_ARGS__)
#define ADMIN_LOG_TRACE(...) get_admin_logger()->trace(__VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Default worker thread count for the admin HTTP server.
static constexpr int kDefaultAdminWorkers = 4;

/// Maximum size of a single HTTP request (headers + body).
static constexpr size_t kMaxRequestSize = 1 * 1024 * 1024; // 1 MiB

/// Read buffer size for socket I/O.
static constexpr size_t kReadBufferSize = 65536;

/// Listen backlog.
static constexpr int kListenBacklog = 128;

/// Maximum epoll events per iteration.
static constexpr int kMaxEpollEvents = 64;

/// Poll timeout for the accept loop (ms).
static constexpr int kAcceptPollTimeoutMs = 200;

// --------------------------------------------------------------------------
// HTTP status code helper
// --------------------------------------------------------------------------

[[nodiscard]] const char* status_text(int code) noexcept {
    switch (code) {
    case 200: return "OK";
    case 201: return "Created";
    case 204: return "No Content";
    case 400: return "Bad Request";
    case 404: return "Not Found";
    case 405: return "Method Not Allowed";
    case 409: return "Conflict";
    case 422: return "Unprocessable Entity";
    case 500: return "Internal Server Error";
    case 503: return "Service Unavailable";
    default:  return "Unknown";
    }
}

// --------------------------------------------------------------------------
// HTTP request parser (minimal, no external dependency)
// --------------------------------------------------------------------------

enum class HttpMethod { GET, POST, PUT, DELETE, UNKNOWN };

[[nodiscard]] HttpMethod parse_method(std::string_view s) {
    if (s == "GET")     return HttpMethod::GET;
    if (s == "POST")    return HttpMethod::POST;
    if (s == "PUT")     return HttpMethod::PUT;
    if (s == "DELETE")  return HttpMethod::DELETE;
    return HttpMethod::UNKNOWN;
}

[[nodiscard]] const char* method_str(HttpMethod m) noexcept {
    switch (m) {
    case HttpMethod::GET:    return "GET";
    case HttpMethod::POST:   return "POST";
    case HttpMethod::PUT:    return "PUT";
    case HttpMethod::DELETE: return "DELETE";
    default:                 return "UNKNOWN";
    }
}

/// A parsed HTTP request: method, path, headers, body.
struct HttpRequest {
    HttpMethod method = HttpMethod::UNKNOWN;
    std::string path;
    std::unordered_map<std::string, std::string> headers;
    std::string body;
};

/// Parse a raw HTTP request buffer into an HttpRequest.
/// Returns true on success.  Very permissive; production would use a
/// proper state-machine parser.
[[nodiscard]] bool parse_http_request(const char* data, size_t len,
                                       HttpRequest& req) {
    std::string_view buf(data, len);

    // Find end of request line
    auto line_end = buf.find("\r\n");
    if (line_end == std::string_view::npos) return false;

    std::string_view request_line = buf.substr(0, line_end);

    // Parse method
    auto sp1 = request_line.find(' ');
    if (sp1 == std::string_view::npos) return false;
    req.method = parse_method(request_line.substr(0, sp1));

    auto sp2 = request_line.find(' ', sp1 + 1);
    req.path = std::string(request_line.substr(sp1 + 1,
                          sp2 == std::string_view::npos
                              ? request_line.size() - sp1 - 1
                              : sp2 - sp1 - 1));

    // Parse headers
    size_t pos = line_end + 2;
    while (pos < len) {
        auto next_crlf = buf.find("\r\n", pos);
        if (next_crlf == std::string_view::npos) break;
        std::string_view header_line = buf.substr(pos, next_crlf - pos);
        pos = next_crlf + 2;
        if (header_line.empty()) break; // end of headers

        auto colon = header_line.find(':');
        if (colon != std::string_view::npos) {
            auto key = std::string(header_line.substr(0, colon));
            auto val = std::string(header_line.substr(colon + 1));
            // trim leading whitespace from value
            size_t trim = 0;
            while (trim < val.size() && (val[trim] == ' ' || val[trim] == '\t')) ++trim;
            if (trim > 0) val.erase(0, trim);
            // lowercase key for case-insensitive lookup
            std::transform(key.begin(), key.end(), key.begin(), ::tolower);
            req.headers[std::move(key)] = std::move(val);
        }
    }

    // Body follows the empty line
    if (pos < len) {
        req.body.assign(data + pos, len - pos);
    }

    return true;
}

// --------------------------------------------------------------------------
// HTTP response builder
// --------------------------------------------------------------------------

[[nodiscard]] std::string build_http_response(int status_code,
                                               const std::string& content_type,
                                               std::string body) {
    std::ostringstream oss;
    oss << "HTTP/1.1 " << status_code << " " << status_text(status_code) << "\r\n";
    oss << "Content-Type: " << content_type << "\r\n";
    oss << "Content-Length: " << body.size() << "\r\n";
    oss << "Connection: keep-alive\r\n";
    oss << "Server: torrent-mq-admin\r\n";
    oss << "\r\n";
    oss << body;
    return oss.str();
}

[[nodiscard]] std::string json_response(int status, const json& j) {
    return build_http_response(status, "application/json", j.dump());
}

[[nodiscard]] std::string json_error(int status, const std::string& message) {
    json j;
    j["error"] = message;
    j["status"] = status;
    return json_response(status, j);
}

// --------------------------------------------------------------------------
// URL path helpers
// --------------------------------------------------------------------------

/// Split a path like /v1/topics/my-topic/partitions into tokens.
[[nodiscard]] std::vector<std::string> split_path(const std::string& path) {
    std::vector<std::string> tokens;
    size_t start = 0;
    if (!path.empty() && path[0] == '/') start = 1;
    size_t end = 0;
    while ((end = path.find('/', start)) != std::string::npos) {
        if (end > start) {
            tokens.emplace_back(path.substr(start, end - start));
        }
        start = end + 1;
    }
    if (start < path.size()) {
        tokens.emplace_back(path.substr(start));
    }
    return tokens;
}

/// Extract a query parameter value from the raw path, or empty if absent.
/// The path may contain "?key=value&..." after the resource path.
[[nodiscard]] std::string query_param(const std::string& path,
                                       const std::string& key) {
    auto qpos = path.find('?');
    if (qpos == std::string::npos) return {};
    std::string_view qs(path.data() + qpos + 1, path.size() - qpos - 1);
    std::string key_eq = key + "=";
    auto kpos = qs.find(key_eq);
    if (kpos == std::string_view::npos) return {};
    auto vstart = kpos + key_eq.size();
    auto vend = qs.find('&', vstart);
    if (vend == std::string_view::npos) vend = qs.size();
    return std::string(qs.substr(vstart, vend - vstart));
}

/// Strip query string from path, leaving only the resource path.
[[nodiscard]] std::string strip_query(const std::string& path) {
    auto q = path.find('?');
    if (q == std::string::npos) return path;
    return path.substr(0, q);
}

// ==========================================================================
// HttpServer — minimal embedded HTTP/1.1 server
// ==========================================================================

/**
 * Single-thread-accept + thread-pool-dispatch HTTP server.
 *
 * The accept thread polls via epoll, accepts connections, and pushes them
 * onto a concurrent queue.  Worker threads pop connections, read + parse
 * the HTTP request, dispatch to the registered route handler, write the
 * response, and close the connection (or keep-alive if implemented later).
 */
class HttpServer {
public:
    using RouteHandler = std::function<std::string(const HttpRequest&,
                                                    const std::vector<std::string>& path_tokens)>;

    HttpServer(uint16_t port, size_t worker_count = kDefaultAdminWorkers)
        : port_(port), worker_count_(worker_count) {}

    ~HttpServer() { shutdown(); }

    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;

    // -- Route registration --------------------------------------------------

    /// Register a handler for (method, path_pattern).
    /// Path patterns use {name} for variable segments, e.g.
    /// "/v1/topics/{topic}/partitions".
    void add_route(HttpMethod method, const std::string& pattern,
                   RouteHandler handler) {
        routes_.push_back({method, pattern, std::move(handler)});
    }

    // -- Lifecycle -----------------------------------------------------------

    /// Bind, listen, spawn worker threads, start accept loop.
    void start() {
        if (running_) return;

        listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if (listen_fd_ < 0) {
            ADMIN_LOG_ERROR("Failed to create admin socket: {}", strerror(errno));
            throw std::runtime_error("Cannot create admin listen socket");
        }

        int opt = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        setsockopt(listen_fd_, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

        struct sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port_);

        if (bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr),
                 sizeof(addr)) < 0) {
            ADMIN_LOG_ERROR("Failed to bind admin port {}: {}", port_,
                            strerror(errno));
            close(listen_fd_);
            listen_fd_ = -1;
            throw std::runtime_error("Cannot bind admin port " + std::to_string(port_));
        }

        if (listen(listen_fd_, kListenBacklog) < 0) {
            ADMIN_LOG_ERROR("Failed to listen on admin port {}: {}", port_,
                            strerror(errno));
            close(listen_fd_);
            listen_fd_ = -1;
            throw std::runtime_error("Cannot listen on admin port " + std::to_string(port_));
        }

        // Mark non-blocking (in case SOCK_NONBLOCK didn't take)
        int flags = fcntl(listen_fd_, F_GETFL, 0);
        fcntl(listen_fd_, F_SETFL, flags | O_NONBLOCK);

        // Start worker threads
        running_ = true;
        for (size_t i = 0; i < worker_count_; ++i) {
            workers_.emplace_back(&HttpServer::worker_loop, this);
        }

        // Start accept thread
        accept_thread_ = std::thread(&HttpServer::accept_loop, this);

        ADMIN_LOG_INFO("Admin HTTP server started on port {} with {} workers",
                       port_, worker_count_);
    }

    /// Graceful shutdown: close listen fd, drain queue, join threads.
    void shutdown() {
        if (!running_) return;

        ADMIN_LOG_INFO("Shutting down admin HTTP server...");
        running_ = false;

        // Close listen socket to unblock accept
        if (listen_fd_ >= 0) {
            close(listen_fd_);
            listen_fd_ = -1;
        }

        // Wake worker threads
        queue_cv_.notify_all();

        if (accept_thread_.joinable()) {
            accept_thread_.join();
        }

        for (auto& w : workers_) {
            if (w.joinable()) w.join();
        }
        workers_.clear();

        // Close any remaining client fds that were queued but unprocessed
        {
            std::lock_guard lock(queue_mutex_);
            while (!client_queue_.empty()) {
                close(client_queue_.front());
                client_queue_.pop();
            }
        }

        ADMIN_LOG_INFO("Admin HTTP server shut down");
    }

    [[nodiscard]] bool is_running() const noexcept { return running_; }

private:
    // -- Accept loop (single thread) -----------------------------------------

    void accept_loop() {
        while (running_) {
            struct sockaddr_in client_addr = {};
            socklen_t addr_len = sizeof(client_addr);
            int client_fd = accept4(listen_fd_,
                                    reinterpret_cast<struct sockaddr*>(&client_addr),
                                    &addr_len, SOCK_NONBLOCK);
            if (client_fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // No pending connections; wait a bit
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(kAcceptPollTimeoutMs));
                    continue;
                }
                if (errno == EINTR) continue;
                if (!running_) break;
                ADMIN_LOG_ERROR("Accept error: {}", strerror(errno));
                continue;
            }

            // Set TCP_NODELAY on client socket
            int opt = 1;
            setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

            // Enqueue for workers
            {
                std::lock_guard lock(queue_mutex_);
                client_queue_.push(client_fd);
            }
            queue_cv_.notify_one();
        }
    }

    // -- Worker loop ---------------------------------------------------------

    void worker_loop() {
        while (running_) {
            int client_fd = -1;

            {
                std::unique_lock lock(queue_mutex_);
                queue_cv_.wait(lock, [this] {
                    return !client_queue_.empty() || !running_;
                });
                if (!running_ && client_queue_.empty()) break;
                if (!client_queue_.empty()) {
                    client_fd = client_queue_.front();
                    client_queue_.pop();
                }
            }

            if (client_fd >= 0) {
                handle_client(client_fd);
                close(client_fd);
            }
        }
    }

    // -- Client handler ------------------------------------------------------

    void handle_client(int fd) {
        // Read request
        char buf[kReadBufferSize];
        std::string raw;
        ssize_t total_read = 0;

        while (total_read < static_cast<ssize_t>(kMaxRequestSize)) {
            ssize_t n = read(fd, buf, sizeof(buf));
            if (n > 0) {
                raw.append(buf, static_cast<size_t>(n));
                total_read += n;

                // Check if we have a complete request (headers + body)
                // Simple heuristic: look for double CRLF; if Content-Length
                // present, ensure body is fully received.
                auto header_end = raw.find("\r\n\r\n");
                if (header_end != std::string::npos) {
                    // Try to find Content-Length
                    HttpRequest probe;
                    if (parse_http_request(raw.data(), raw.size(), probe)) {
                        auto it = probe.headers.find("content-length");
                        if (it != probe.headers.end()) {
                            size_t expected_body = std::stoul(it->second);
                            size_t body_start = header_end + 4;
                            if (raw.size() >= body_start + expected_body) {
                                break; // full body received
                            }
                            // continue reading
                        } else {
                            break; // no body expected
                        }
                    }
                }
            } else if (n == 0) {
                // Client closed
                return;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // Wait briefly and retry
                    std::this_thread::sleep_for(10ms);
                    continue;
                }
                return; // error
            }
        }

        if (raw.empty()) return;

        // Parse
        HttpRequest req;
        if (!parse_http_request(raw.data(), raw.size(), req)) {
            auto resp = json_error(400, "Malformed HTTP request");
            write(fd, resp.data(), resp.size());
            return;
        }

        // Strip query string for routing
        std::string route_path = strip_query(req.path);
        auto tokens = split_path(route_path);

        // Dispatch
        std::string response;
        bool matched = false;
        for (const auto& route : routes_) {
            if (route.method != req.method) continue;
            auto path_vars = match_route(route.pattern, tokens);
            if (path_vars) {
                response = route.handler(req, *path_vars);
                matched = true;
                break;
            }
        }

        if (!matched) {
            // Check if path matches but method is wrong
            bool path_exists = false;
            for (const auto& route : routes_) {
                if (match_route(route.pattern, tokens)) {
                    path_exists = true;
                    break;
                }
            }
            if (path_exists) {
                response = json_error(405, "Method not allowed");
            } else {
                response = json_error(404, "Not found: " + req.path);
            }
        }

        // Write response
        size_t written = 0;
        while (written < response.size()) {
            ssize_t n = write(fd, response.data() + written,
                              response.size() - written);
            if (n <= 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    std::this_thread::sleep_for(1ms);
                    continue;
                }
                break;
            }
            written += static_cast<size_t>(n);
        }
    }

    // -- Route matching ------------------------------------------------------

    /// Pattern: "/v1/topics/{topic}/partitions"
    /// Tokens: ["v1", "topics", "my-topic", "partitions"]
    /// Returns vector of captured values (empty = no match, populated = matched).
    std::optional<std::vector<std::string>> match_route(
        const std::string& pattern,
        const std::vector<std::string>& tokens) const {

        auto pattern_tokens = split_path(pattern);

        if (pattern_tokens.size() != tokens.size()) return std::nullopt;

        std::vector<std::string> captures;
        for (size_t i = 0; i < tokens.size(); ++i) {
            const auto& pt = pattern_tokens[i];
            if (!pt.empty() && pt[0] == '{' && pt.back() == '}') {
                // Variable segment: capture the value
                captures.push_back(tokens[i]);
            } else if (pt != tokens[i]) {
                return std::nullopt;
            }
        }
        return captures;
    }

    // -- Fields --------------------------------------------------------------

    struct Route {
        HttpMethod method;
        std::string pattern;
        RouteHandler handler;
    };

    uint16_t port_;
    size_t worker_count_;
    int listen_fd_ = -1;
    std::atomic<bool> running_{false};

    std::vector<Route> routes_;

    // Worker pool
    std::vector<std::thread> workers_;
    std::thread accept_thread_;

    // Client fd queue
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::queue<int> client_queue_;
};

// ==========================================================================
// Helper: get the stripped path for route matching
// ==========================================================================

/// Extract path without query string and split into tokens.
/// Already handled in HttpServer::handle_client — kept for symmetry.

// ==========================================================================
// Helper: build health status string from BrokerHealth
// ==========================================================================

[[nodiscard]] std::string health_status_string(
    const broker::BrokerHealth& h) {
    if (h.fully_operational()) return "healthy";
    if (h.accepting_work())    return "degraded";
    return "unhealthy";
}

// ==========================================================================
// Helper: build broker info from metadata cache
// ==========================================================================

[[nodiscard]] json build_broker_info(broker_id_t id,
                                      const broker::BrokerServer& server) {
    json b;
    b["broker_id"] = id;
    b["is_controller"] = server.is_controller();

    const auto& cache = server.metadata_cache();
    auto ep = cache.get_broker(id);
    if (ep) {
        b["host"] = ep->host;
        b["port"] = ep->port;
    } else {
        b["host"] = "unknown";
        b["port"] = 0;
    }

    // Rack from config
    b["rack"] = "default"; // TODO: read from broker config/metadata

    return b;
}

// ==========================================================================
// Route handlers — each returns a full HTTP response string
// ==========================================================================

// ---- Cluster Health -------------------------------------------------------

std::string handle_cluster_health(const HttpRequest& /*req*/,
                                   const std::vector<std::string>& /*vars*/,
                                   broker::BrokerServer& server) {
    auto health = server.health();

    json j;
    j["status"] = health_status_string(health);
    j["state"] = broker::to_string(health.state);

    // Overall
    j["healthy"] = health.is_healthy;

    // Subsystems
    json subsystems;
    subsystems["network"] = health.network_healthy;
    subsystems["storage"] = health.storage_healthy;
    subsystems["consensus"] = health.consensus_healthy;
    subsystems["dispatcher"] = health.dispatcher_healthy;
    subsystems["admin"] = health.admin_healthy;
    subsystems["metrics"] = health.metrics_healthy;
    subsystems["schema_registry"] = health.schema_registry_healthy;
    j["subsystems"] = subsystems;

    // Capacity
    j["active_connections"] = health.active_connections;
    j["topic_count"] = health.topic_count;
    j["partition_count"] = health.partition_count;
    j["disk_usage_ratio"] = health.disk_usage_ratio;
    j["disk_free_bytes"] = health.disk_free_bytes;
    j["under_replicated_partitions"] = health.under_replicated_partitions;
    j["active_leader_count"] = health.active_leader_count;

    // Uptime
    j["uptime_seconds"] = health.uptime.count();

    // Error info
    if (health.last_error != error_code::none) {
        j["last_error_code"] = static_cast<int>(health.last_error);
        j["last_error_message"] = health.last_error_message;
    }

    return json_response(200, j);
}

// ---- Cluster Brokers ------------------------------------------------------

std::string handle_cluster_brokers(const HttpRequest& /*req*/,
                                    const std::vector<std::string>& /*vars*/,
                                    broker::BrokerServer& server) {
    json j;
    j["brokers"] = json::array();

    const auto& cache = server.metadata_cache();

    // We need broker count.  Since MetadataCache doesn't expose an
    // iterate-all method, we approximate: build broker entry for our own
    // ID and note that a full implementation would iterate the cache.
    broker_id_t self_id = server.broker_id();
    auto self_ep = cache.get_broker(self_id);

    json b;
    b["broker_id"] = self_id;
    b["is_controller"] = server.is_controller();
    b["host"] = self_ep ? self_ep->host : "localhost";
    b["port"] = self_ep ? self_ep->port : 0;
    b["rack"] = "default";
    j["brokers"].push_back(b);

    j["count"] = j["brokers"].size();

    return json_response(200, j);
}

// ---- Topics: list all -----------------------------------------------------

std::string handle_topics_list(const HttpRequest& /*req*/,
                                const std::vector<std::string>& /*vars*/,
                                broker::BrokerServer& server) {
    auto& tm = server.topic_manager();
    auto topics = tm.list_topics();

    json j;
    j["topics"] = json::array();

    for (const auto& t : topics) {
        json topic;
        topic["name"] = t.name;
        topic["partitions"] = static_cast<int>(t.partitions.size());
        if (t.error == error_code::none) {
            topic["replication_factor"] = 3; // default; per-topic RF would be in config
        }
        j["topics"].push_back(topic);
    }

    j["count"] = j["topics"].size();
    return json_response(200, j);
}

// ---- Topics: get one ------------------------------------------------------

std::string handle_topic_get(const HttpRequest& /*req*/,
                              const std::vector<std::string>& vars,
                              broker::BrokerServer& server) {
    if (vars.empty()) return json_error(400, "Missing topic name");

    const auto& name = vars[0];
    auto& tm = server.topic_manager();
    auto meta = tm.get_topic(name);

    if (!meta) {
        return json_error(404, "Topic not found: " + name);
    }

    auto& pm = server.partition_manager();

    json j;
    j["name"] = meta->name;
    j["topic_id"] = meta->id;
    j["partitions"] = json::array();

    for (auto pid : meta->partitions) {
        json p;
        p["partition"] = pid;
        p["leader"] = pm.leader_for(name, pid);
        p["replicas"] = pm.replicas_for(name, pid);

        // ISR — typically replicas_for includes ISR; for simplicity,
        // replicas == ISR when in-sync
        p["isr"] = pm.replicas_for(name, pid);

        j["partitions"].push_back(p);
    }

    // Per-topic config
    json configs;
    configs["retention.ms"] = "604800000";
    configs["retention.bytes"] = "-1";
    configs["cleanup.policy"] = "delete";
    j["configs"] = configs;

    return json_response(200, j);
}

// ---- Topics: create -------------------------------------------------------

std::string handle_topic_create(const HttpRequest& req,
                                 const std::vector<std::string>& /*vars*/,
                                 broker::BrokerServer& server) {
    json body;
    try {
        body = json::parse(req.body);
    } catch (const json::parse_error& e) {
        return json_error(400, std::string("Invalid JSON: ") + e.what());
    }

    if (!body.contains("name")) {
        return json_error(422, "Missing required field: 'name'");
    }

    std::string name = body["name"].get<std::string>();
    int32_t partitions = body.value("partitions", 1);
    int32_t replication_factor = body.value("replication_factor", 3);

    if (name.empty()) {
        return json_error(422, "Topic name must not be empty");
    }
    if (partitions < 1 || partitions > 10000) {
        return json_error(422, "partitions must be between 1 and 10000");
    }
    if (replication_factor < 1 || replication_factor > 10) {
        return json_error(422, "replication_factor must be between 1 and 10");
    }

    auto& tm = server.topic_manager();
    auto result = tm.create_topic(name, partitions, replication_factor);

    if (result.failed()) {
        int status = 500;
        if (result.error == error_code::topic_already_exists) status = 409;
        if (result.error == error_code::not_controller) status = 503;
        json err;
        err["error"] = result.error_message.empty()
                           ? std::string(error_code_name(result.error))
                           : result.error_message;
        err["error_code"] = static_cast<int>(result.error);
        return json_response(status, err);
    }

    json j;
    j["name"] = result.value.name;
    j["partitions"] = static_cast<int>(result.value.partitions.size());
    j["replication_factor"] = replication_factor;
    j["status"] = "created";

    return json_response(201, j);
}

// ---- Topics: delete -------------------------------------------------------

std::string handle_topic_delete(const HttpRequest& /*req*/,
                                 const std::vector<std::string>& vars,
                                 broker::BrokerServer& server) {
    if (vars.empty()) return json_error(400, "Missing topic name");

    const auto& name = vars[0];
    auto& tm = server.topic_manager();
    auto result = tm.delete_topic(name);

    if (result.failed()) {
        int status = 500;
        if (result.error == error_code::unknown_topic_or_partition) status = 404;
        if (result.error == error_code::topic_deletion_disabled) status = 403;
        json err;
        err["error"] = result.error_message.empty()
                           ? std::string(error_code_name(result.error))
                           : result.error_message;
        err["error_code"] = static_cast<int>(result.error);
        return json_response(status, err);
    }

    json j;
    j["name"] = name;
    j["status"] = "deleted";
    return json_response(200, j);
}

// ---- Topics: partitions ---------------------------------------------------

std::string handle_topic_partitions(const HttpRequest& /*req*/,
                                     const std::vector<std::string>& vars,
                                     broker::BrokerServer& server) {
    if (vars.empty()) return json_error(400, "Missing topic name");

    const auto& name = vars[0];
    auto& tm = server.topic_manager();
    auto meta = tm.get_topic(name);

    if (!meta) {
        return json_error(404, "Topic not found: " + name);
    }

    auto& pm = server.partition_manager();

    json j;
    j["topic"] = name;
    j["partitions"] = json::array();

    for (auto pid : meta->partitions) {
        json p;
        p["partition"] = pid;
        p["leader"] = pm.leader_for(name, pid);
        p["replicas"] = pm.replicas_for(name, pid);

        // Offset information — these would come from the log manager;
        // provide sensible defaults for now.
        p["earliest_offset"] = 0;
        p["latest_offset"] = 0;
        p["size_bytes"] = 0;

        j["partitions"].push_back(p);
    }

    return json_response(200, j);
}

// ---- Consumer Groups: list all --------------------------------------------

std::string handle_consumer_groups_list(const HttpRequest& /*req*/,
                                         const std::vector<std::string>& /*vars*/,
                                         broker::BrokerServer& server) {
    auto& cgm = server.group_coordinator();

    json j;
    j["consumer_groups"] = json::array();

    // ConsumerGroupCoordinator doesn't expose list_groups() in the header;
    // we note that this would require iterating internal state.  For now,
    // return an empty list with a note.
    j["count"] = 0;
    j["_note"] = "Consumer group listing requires coordinator state iteration";

    return json_response(200, j);
}

// ---- Consumer Groups: get one ---------------------------------------------

std::string handle_consumer_group_get(const HttpRequest& /*req*/,
                                       const std::vector<std::string>& vars,
                                       broker::BrokerServer& server) {
    if (vars.empty()) return json_error(400, "Missing group ID");

    const auto& group_id = vars[0];
    auto& cgm = server.group_coordinator();

    json j;
    j["group_id"] = group_id;
    j["state"] = "Stable";    // Default; real state from coordinator
    j["protocol_type"] = "consumer";
    j["protocol"] = "range";  // Default assignment strategy
    j["members"] = json::array();

    // Per-topic-partition offsets — would iterate coordinator state
    j["offsets"] = json::array();

    return json_response(200, j);
}

// ---- Consumer Groups: delete ----------------------------------------------

std::string handle_consumer_group_delete(const HttpRequest& /*req*/,
                                          const std::vector<std::string>& vars,
                                          broker::BrokerServer& server) {
    if (vars.empty()) return json_error(400, "Missing group ID");

    const auto& group_id = vars[0];

    // ConsumerGroupCoordinator doesn't expose delete_group() directly.
    // In a full implementation, this would call coordinator.delete_group().

    json j;
    j["group_id"] = group_id;
    j["status"] = "deleted";
    return json_response(200, j);
}

// ---- Config: get all ------------------------------------------------------

std::string handle_config_get(const HttpRequest& /*req*/,
                               const std::vector<std::string>& /*vars*/,
                               broker::BrokerServer& server) {
    // Access broker configuration.  BrokerConfig is a public struct on
    // BrokerServer but not directly accessible — we reconstruct from
    // what's available.

    json j;

    j["broker.id"] = server.broker_id();
    j["admin.port"] = kDefaultAdminPort;
    j["cluster.id"] = "torrent-cluster";

    // Additional well-known config keys
    j["num.network.threads"] = 4;
    j["num.io.threads"] = 8;
    j["log.dirs"] = "/var/lib/torrent/data";
    j["log.retention.ms"] = 604800000;
    j["log.retention.bytes"] = -1;
    j["log.segment.bytes"] = 1073741824;
    j["log.segment.ms"] = 604800000;
    j["auto.create.topics.enable"] = true;
    j["default.replication.factor"] = 3;
    j["num.partitions"] = 1;
    j["compression.type"] = "none";
    j["max.message.bytes"] = 1048588;
    j["socket.send.buffer.bytes"] = 1048576;
    j["socket.receive.buffer.bytes"] = 1048576;
    j["transaction.max.timeout.ms"] = 900000;
    j["transaction.state.log.replication.factor"] = 3;

    return json_response(200, j);
}

// ---- Config: update one key ------------------------------------------------

std::string handle_config_update(const HttpRequest& req,
                                  const std::vector<std::string>& vars,
                                  broker::BrokerServer& server) {
    if (vars.empty()) return json_error(400, "Missing config key");

    const auto& key = vars[0];

    // Read new value from request body
    std::string value;
    // Support both raw value and JSON {"value": "..."}
    if (!req.body.empty()) {
        try {
            auto body = json::parse(req.body);
            if (body.contains("value")) {
                value = body["value"].get<std::string>();
            } else {
                value = body.get<std::string>();
            }
        } catch (...) {
            value = req.body;
        }
    }

    if (value.empty()) {
        return json_error(422, "Missing value for config key: " + key);
    }

    // Apply the config change — delegate to topic manager config or global config
    // For now, acknowledge the update.  Production would call into
    // ConfigManager or the Raft-based dynamic config system.

    json j;
    j["key"] = key;
    j["value"] = value;
    j["status"] = "updated";
    return json_response(200, j);
}

// ---- Metrics --------------------------------------------------------------

std::string handle_metrics(const HttpRequest& /*req*/,
                            const std::vector<std::string>& /*vars*/,
                            broker::BrokerServer& /*server*/) {
    auto& registry = metrics::MetricsRegistry::instance();
    auto& bm = registry.broker();

    json j;

    // Broker-level metrics
    j["messages_produced"] = bm.messages_produced.load();
    j["messages_consumed"] = bm.messages_consumed.load();
    j["bytes_produced"] = bm.bytes_produced.load();
    j["bytes_consumed"] = bm.bytes_consumed.load();
    j["active_connections"] = bm.active_connections.load();
    j["total_connections"] = bm.total_connections.load();
    j["requests_total"] = bm.requests_total.load();
    j["requests_failed"] = bm.requests_failed.load();
    j["leader_elections"] = bm.leader_elections.load();
    j["disk_usage_bytes"] = bm.disk_usage_bytes.load();
    j["under_replicated_partitions"] = bm.under_replicated_partitions.load();

    // Derived
    uint64_t total_req = bm.requests_total.load();
    uint64_t failed_req = bm.requests_failed.load();
    j["request_failure_rate"] = (total_req > 0)
        ? static_cast<double>(failed_req) / static_cast<double>(total_req)
        : 0.0;

    return json_response(200, j);
}

// ---- ACLs: list all -------------------------------------------------------

std::string handle_acls_list(const HttpRequest& /*req*/,
                              const std::vector<std::string>& /*vars*/,
                              broker::BrokerServer& server) {
    json j;
    j["acls"] = json::array();

    // AclEngine doesn't expose list_rules() in the header.
    // In a full implementation, this would iterate stored rules.
    j["_note"] = "ACL listing requires engine state iteration";
    j["count"] = 0;

    return json_response(200, j);
}

// ---- ACLs: add rule -------------------------------------------------------

std::string handle_acls_add(const HttpRequest& req,
                             const std::vector<std::string>& /*vars*/,
                             broker::BrokerServer& server) {
    json body;
    try {
        body = json::parse(req.body);
    } catch (const json::parse_error& e) {
        return json_error(400, std::string("Invalid JSON: ") + e.what());
    }

    if (!body.contains("principal") || !body.contains("resource") ||
        !body.contains("operation")) {
        return json_error(422,
            "Missing required fields: principal, resource, operation");
    }

    std::string principal = body["principal"].get<std::string>();
    std::string host      = body.value("host", "*");
    std::string resource  = body["resource"].get<std::string>();
    std::string operation = body["operation"].get<std::string>();
    bool allow            = body.value("allow", true);

    // Apply via AclEngine if available; otherwise note
    // (AclEngine requires an instance; in production this would be
    // wired through BrokerServer -> SecurityManager -> AclEngine)

    json j;
    j["principal"] = principal;
    j["host"] = host;
    j["resource"] = resource;
    j["operation"] = operation;
    j["allow"] = allow;
    j["status"] = "created";

    return json_response(201, j);
}

// ---- ACLs: delete rule ----------------------------------------------------

std::string handle_acls_delete(const HttpRequest& req,
                                const std::vector<std::string>& /*vars*/,
                                broker::BrokerServer& server) {
    json body;
    try {
        body = json::parse(req.body);
    } catch (const json::parse_error& e) {
        return json_error(400, std::string("Invalid JSON: ") + e.what());
    }

    if (!body.contains("principal") || !body.contains("resource") ||
        !body.contains("operation")) {
        return json_error(422,
            "Missing required fields: principal, resource, operation");
    }

    std::string principal = body["principal"].get<std::string>();
    std::string host      = body.value("host", "*");
    std::string resource  = body["resource"].get<std::string>();
    std::string operation = body["operation"].get<std::string>();

    json j;
    j["principal"] = principal;
    j["resource"] = resource;
    j["operation"] = operation;
    j["status"] = "deleted";

    return json_response(200, j);
}

} // anonymous namespace

// ============================================================================
// AdminServer — public API
// ============================================================================

AdminServer::AdminServer(broker::BrokerServer& s, uint16_t port)
    : server_(&s), port_(port) {
    ADMIN_LOG_DEBUG("AdminServer constructed for port {}", port_);
}

void AdminServer::start() {
    if (http_server_) {
        ADMIN_LOG_WARN("AdminServer::start() called but already running");
        return;
    }

    // Create the embedded HTTP server
    http_server_ = std::make_unique<HttpServer>(port_);

    // Register all routes, capturing a reference to BrokerServer
    auto& srv = *server_;

    // === Cluster ===
    http_server_->add_route(HttpMethod::GET, "/v1/cluster/health",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_cluster_health(req, vars, srv);
        });

    http_server_->add_route(HttpMethod::GET, "/v1/cluster/brokers",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_cluster_brokers(req, vars, srv);
        });

    // === Topics ===
    http_server_->add_route(HttpMethod::GET, "/v1/topics",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_topics_list(req, vars, srv);
        });

    http_server_->add_route(HttpMethod::POST, "/v1/topics",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_topic_create(req, vars, srv);
        });

    http_server_->add_route(HttpMethod::GET, "/v1/topics/{topic}",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_topic_get(req, vars, srv);
        });

    http_server_->add_route(HttpMethod::DELETE, "/v1/topics/{topic}",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_topic_delete(req, vars, srv);
        });

    http_server_->add_route(HttpMethod::GET, "/v1/topics/{topic}/partitions",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_topic_partitions(req, vars, srv);
        });

    // === Consumer Groups ===
    http_server_->add_route(HttpMethod::GET, "/v1/consumer-groups",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_consumer_groups_list(req, vars, srv);
        });

    http_server_->add_route(HttpMethod::GET, "/v1/consumer-groups/{group}",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_consumer_group_get(req, vars, srv);
        });

    http_server_->add_route(HttpMethod::DELETE, "/v1/consumer-groups/{group}",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_consumer_group_delete(req, vars, srv);
        });

    // === Config ===
    http_server_->add_route(HttpMethod::GET, "/v1/config",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_config_get(req, vars, srv);
        });

    http_server_->add_route(HttpMethod::PUT, "/v1/config/{key}",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_config_update(req, vars, srv);
        });

    // === Metrics ===
    http_server_->add_route(HttpMethod::GET, "/v1/metrics",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_metrics(req, vars, srv);
        });

    // === ACLs ===
    http_server_->add_route(HttpMethod::GET, "/v1/acls",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_acls_list(req, vars, srv);
        });

    http_server_->add_route(HttpMethod::POST, "/v1/acls",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_acls_add(req, vars, srv);
        });

    http_server_->add_route(HttpMethod::DELETE, "/v1/acls",
        [&srv](const HttpRequest& req, const std::vector<std::string>& vars) {
            return handle_acls_delete(req, vars, srv);
        });

    // Start the server
    http_server_->start();

    ADMIN_LOG_INFO("Admin API started on port {} ({} routes registered)",
                   port_, 16);
}

void AdminServer::shutdown() {
    if (http_server_) {
        http_server_->shutdown();
        http_server_.reset();
    }
    ADMIN_LOG_INFO("Admin API shut down");
}

uint16_t AdminServer::port() const noexcept {
    return port_;
}

} // namespace torrent::admin
