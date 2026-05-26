/**
 * prometheus_exporter.cpp — Prometheus Text Format Metrics Exporter
 *
 * Starts a tiny embedded HTTP/1.1 server on `metrics_port_` (default 9090)
 * that serves a single /metrics endpoint in the Prometheus exposition format.
 * The scrape payload includes:
 *
 *   - Built-in broker metrics (messages_produced, bytes_consumed, etc.)
 *   - Per-topic meters (messages_in, messages_out, bytes_in, bytes_out, size_bytes)
 *   - Process metrics (CPU time from /proc/self/stat, resident memory from
 *     /proc/self/status, fd count via /proc/self/fd)
 *   - JVM-mimic metrics for JMX compatibility (jvm_memory_*, jvm_gc_*,
 *     jvm_threads_*, jvm_classes_*)
 *   - Any custom gauges/counters/histograms registered via MetricsRegistry
 *
 * Thread-safety:
 *   - The accept thread + worker threads touch only local state.
 *   - scrape() reads all metrics atomically (load-relaxed for counters).
 *   - start() / shutdown() are serialised by the caller (BrokerServer).
 *
 * Dependencies: POSIX sockets (no external HTTP library).
 */

#include "torrent/metrics/prometheus_exporter.h"
#include "torrent/metrics/metrics.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

// ============================================================================
// Convenience aliases
// ============================================================================

using namespace std::chrono_literals;

namespace torrent::metrics {

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logging
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_exporter_logger() {
    static auto logger = []() {
        auto l = spdlog::get("prometheus_exporter");
        if (!l) {
            l = spdlog::stdout_color_mt("prometheus_exporter");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define EXP_LOG_INFO(...)  get_exporter_logger()->info(__VA_ARGS__)
#define EXP_LOG_WARN(...)  get_exporter_logger()->warn(__VA_ARGS__)
#define EXP_LOG_ERROR(...) get_exporter_logger()->error(__VA_ARGS__)
#define EXP_LOG_DEBUG(...) get_exporter_logger()->debug(__VA_ARGS__)
#define EXP_LOG_TRACE(...) get_exporter_logger()->trace(__VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Worker thread count for the embedded HTTP server.
static constexpr size_t kDefaultWorkers = 2;

/// Maximum size of a single HTTP request (headers + body).
static constexpr size_t kMaxRequestSize = 8 * 1024;       // 8 KiB

/// Read buffer size for socket I/O.
static constexpr size_t kReadBufferSize = 8192;

/// Listen backlog.
static constexpr int kListenBacklog = 128;

/// Poll timeout for accept loop (ms).
static constexpr int kAcceptPollTimeoutMs = 150;

/// Content-Type for Prometheus text format.
static constexpr std::string_view kPrometheusContentType =
    "text/plain; version=0.0.4; charset=utf-8";

// --------------------------------------------------------------------------
// HTTP response builders (minimal)
// --------------------------------------------------------------------------

[[nodiscard]] const char* http_status_text(int code) noexcept {
    switch (code) {
    case 200: return "OK";
    case 404: return "Not Found";
    case 405: return "Method Not Allowed";
    case 500: return "Internal Server Error";
    case 503: return "Service Unavailable";
    default:  return "Unknown";
    }
}

[[nodiscard]] std::string build_http_response(int status_code,
                                               std::string_view content_type,
                                               std::string_view body) {
    std::ostringstream oss;
    oss << "HTTP/1.1 " << status_code << " " << http_status_text(status_code) << "\r\n";
    oss << "Content-Type: " << content_type << "\r\n";
    oss << "Content-Length: " << body.size() << "\r\n";
    oss << "Connection: keep-alive\r\n";
    oss << "Server: torrent-mq-metrics\r\n";
    oss << "\r\n";
    oss << body;
    return oss.str();
}

[[nodiscard]] std::string not_found_response() {
    std::string body = "404 Not Found\n";
    return build_http_response(404, "text/plain", body);
}

[[nodiscard]] std::string method_not_allowed_response() {
    std::string body = "405 Method Not Allowed\n";
    return build_http_response(405, "text/plain", body);
}

[[nodiscard]] std::string server_error_response(const std::string& msg) {
    return build_http_response(500, "text/plain", msg);
}

// --------------------------------------------------------------------------
// Minimal HTTP request parser
// --------------------------------------------------------------------------

enum class HttpMethod { GET, POST, PUT, DELETE, UNKNOWN };

[[nodiscard]] HttpMethod parse_http_method(std::string_view s) {
    if (s == "GET")    return HttpMethod::GET;
    if (s == "POST")   return HttpMethod::POST;
    if (s == "PUT")    return HttpMethod::PUT;
    if (s == "DELETE") return HttpMethod::DELETE;
    return HttpMethod::UNKNOWN;
}

struct ParsedRequest {
    HttpMethod method = HttpMethod::UNKNOWN;
    std::string path;
};

/**
 * Parse just enough of the HTTP request to extract method + path.
 * Returns true on success.
 */
[[nodiscard]] bool parse_request(const char* data, size_t len,
                                  ParsedRequest& req) {
    std::string_view buf(data, len);
    auto line_end = buf.find("\r\n");
    if (line_end == std::string_view::npos) return false;

    std::string_view request_line = buf.substr(0, line_end);

    auto sp1 = request_line.find(' ');
    if (sp1 == std::string_view::npos) return false;
    req.method = parse_http_method(request_line.substr(0, sp1));

    auto sp2 = request_line.find(' ', sp1 + 1);
    size_t path_end = (sp2 == std::string_view::npos)
        ? request_line.size() - sp1 - 1
        : sp2 - sp1 - 1;
    req.path = std::string(request_line.substr(sp1 + 1, path_end));

    // Strip query string from path
    auto qpos = req.path.find('?');
    if (qpos != std::string::npos) {
        req.path.resize(qpos);
    }

    return true;
}

// --------------------------------------------------------------------------
// Process metrics: read /proc/self/stat for CPU time
// --------------------------------------------------------------------------

/**
 * Parse /proc/self/stat fields.
 * Fields of interest (1-indexed):
 *   14 = utime (user mode jiffies)
 *   15 = stime (kernel mode jiffies)
 *
 * Returns total CPU time in seconds (wall clock), or -1.0 on failure.
 */
[[nodiscard]] double read_process_cpu_seconds() {
    std::ifstream stat_file("/proc/self/stat");
    if (!stat_file.is_open()) return -1.0;

    std::string line;
    if (!std::getline(stat_file, line)) return -1.0;

    // Field 2 is the comm in parentheses and may contain spaces.
    // Find the closing parenthesis.
    auto close_paren = line.rfind(')');
    if (close_paren == std::string::npos) return -1.0;

    // Everything after ") " is the remaining fields
    std::string_view rest(line.data() + close_paren + 2,
                           line.size() - close_paren - 2);

    // Tokenise by space; fields are 3..N (utime is field 14 overall → index 11 in rest)
    std::vector<std::string_view> fields;
    size_t pos = 0;
    while (pos < rest.size()) {
        auto sp = rest.find(' ', pos);
        if (sp == std::string_view::npos) {
            fields.push_back(rest.substr(pos));
            break;
        }
        fields.push_back(rest.substr(pos, sp - pos));
        pos = sp + 1;
    }

    // utime = field index 11 (0-based), stime = 12
    if (fields.size() < 13) return -1.0;

    try {
        long utime = std::stol(std::string(fields[11]));
        long stime = std::stol(std::string(fields[12]));
        long clk_tck = sysconf(_SC_CLK_TCK);
        if (clk_tck <= 0) clk_tck = 100; // fallback
        return static_cast<double>(utime + stime) / static_cast<double>(clk_tck);
    } catch (...) {
        return -1.0;
    }
}

// --------------------------------------------------------------------------
// Process metrics: resident memory from /proc/self/status
// --------------------------------------------------------------------------

/**
 * Read VmRSS (resident set size) from /proc/self/status.
 * Returns bytes, or -1 on failure.
 */
[[nodiscard]] int64_t read_process_rss_bytes() {
    std::ifstream status_file("/proc/self/status");
    if (!status_file.is_open()) return -1;

    std::string line;
    while (std::getline(status_file, line)) {
        if (line.starts_with("VmRSS:")) {
            // Format: "VmRSS:    12345 kB"
            std::istringstream iss(line);
            std::string label, value_str, unit;
            iss >> label >> value_str >> unit;
            try {
                int64_t kb = std::stoll(value_str);
                return kb * 1024;
            } catch (...) {
                return -1;
            }
        }
    }
    return -1;
}

// --------------------------------------------------------------------------
// Process metrics: file descriptor count from /proc/self/fd
// --------------------------------------------------------------------------

[[nodiscard]] int64_t read_process_fd_count() {
    DIR* dir = opendir("/proc/self/fd");
    if (!dir) return -1;

    int64_t count = 0;
    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_name[0] != '.') {
            ++count;
        }
    }
    // Subtract 1 for the opendir fd itself (approximately)
    // Actually opendir opened a fd to read the directory, but closedir will
    // close it. We count the regular fds only. The "." and ".." are skipped.
    closedir(dir);
    return count;
}

// --------------------------------------------------------------------------
// Process uptime: /proc/self/stat field 22 = starttime
// We approximate uptime as (now - process_start_time)
// --------------------------------------------------------------------------

[[nodiscard]] double read_process_uptime_seconds() {
    // Use /proc/uptime for system uptime, and compare with starttime
    std::ifstream stat_file("/proc/self/stat");
    if (!stat_file) return -1.0;

    std::string line;
    if (!std::getline(stat_file, line)) return -1.0;

    auto close_paren = line.rfind(')');
    if (close_paren == std::string::npos) return -1.0;

    std::string_view rest(line.data() + close_paren + 2,
                           line.size() - close_paren - 2);
    std::vector<std::string_view> fields;
    size_t pos = 0;
    while (pos < rest.size()) {
        auto sp = rest.find(' ', pos);
        if (sp == std::string_view::npos) {
            fields.push_back(rest.substr(pos));
            break;
        }
        fields.push_back(rest.substr(pos, sp - pos));
        pos = sp + 1;
    }

    // starttime = field 19 (0-based index 19)
    if (fields.size() < 20) return -1.0;

    try {
        long long starttime = std::stoll(std::string(fields[19]));
        long clk_tck = sysconf(_SC_CLK_TCK);
        if (clk_tck <= 0) clk_tck = 100;

        // Read system uptime
        std::ifstream uptime_file("/proc/uptime");
        if (!uptime_file) return -1.0;
        double system_uptime;
        uptime_file >> system_uptime;

        double process_start = static_cast<double>(starttime) / static_cast<double>(clk_tck);
        return system_uptime - process_start;
    } catch (...) {
        return -1.0;
    }
}

// --------------------------------------------------------------------------
// JVM-mimic startup time (lazy-init, cached)
// --------------------------------------------------------------------------

[[nodiscard]] double get_jvm_start_time_seconds() {
    static double s = []() -> double {
        double uptime = read_process_uptime_seconds();
        if (uptime < 0) {
            // Fallback: use wall clock at first call
            auto now = std::chrono::system_clock::now();
            auto dur = now.time_since_epoch();
            double now_s = static_cast<double>(
                std::chrono::duration_cast<std::chrono::milliseconds>(dur).count()) / 1000.0;
            return now_s - uptime;
        }
        auto now = std::chrono::system_clock::now();
        double now_s = static_cast<double>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()).count()) / 1000.0;
        return now_s - uptime;
    }();
    return s;
}

// --------------------------------------------------------------------------
// Prometheus text format helpers
// --------------------------------------------------------------------------

/// Sanitize a metric name to conform to Prometheus naming: [a-zA-Z_:][a-zA-Z0-9_:]*
[[nodiscard]] std::string sanitize_metric_name(const std::string& raw) {
    std::string out;
    out.reserve(raw.size());
    for (size_t i = 0; i < raw.size(); ++i) {
        char c = raw[i];
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') || c == '_' || c == ':') {
            if (i == 0 && c >= '0' && c <= '9') {
                out += '_';
            }
            out += c;
        } else {
            out += '_';
        }
    }
    return out;
}

/// Escape a label value: backslash-escape \ " and newline.
[[nodiscard]] std::string escape_label_value(const std::string& raw) {
    std::string out;
    out.reserve(raw.size() + 4);
    for (char c : raw) {
        switch (c) {
        case '\\': out += "\\\\"; break;
        case '"':  out += "\\\""; break;
        case '\n': out += "\\n"; break;
        default:   out += c; break;
        }
    }
    return out;
}

/// Append HELP and TYPE lines for a metric.
void append_metric_header(std::ostringstream& oss,
                          const std::string& name,
                          const std::string& help,
                          const std::string& type) {
    oss << "# HELP " << name << " " << help << "\n";
    oss << "# TYPE " << name << " " << type << "\n";
}

/// Append a simple counter/gauge line with optional labels.
void append_metric_line(std::ostringstream& oss,
                        const std::string& name,
                        double value,
                        const std::vector<std::pair<std::string, std::string>>& labels = {}) {
    oss << name;
    if (!labels.empty()) {
        oss << "{";
        for (size_t i = 0; i < labels.size(); ++i) {
            if (i > 0) oss << ",";
            oss << labels[i].first << "=\"" << escape_label_value(labels[i].second) << "\"";
        }
        oss << "}";
    }
    oss << " " << value << "\n";
}

/// Append histogram lines (sum, count, buckets).
void append_histogram_metrics(std::ostringstream& oss,
                              const std::string& name,
                              const std::string& help,
                              const std::atomic<uint64_t>* sum,
                              const std::atomic<uint64_t>* count,
                              const std::vector<double>& buckets,
                              const std::vector<std::atomic<uint64_t>>& bucket_counts) {
    append_metric_header(oss, name, help, "histogram");

    double sum_val = static_cast<double>(sum->load(std::memory_order_relaxed));
    double count_val = static_cast<double>(count->load(std::memory_order_relaxed));

    for (size_t i = 0; i < buckets.size(); ++i) {
        double bucket_val = static_cast<double>(
            bucket_counts[i].load(std::memory_order_relaxed));
        // Use "le" label for the bucket boundary
        std::string bucket_label;
        if (std::isinf(buckets[i]) && buckets[i] > 0) {
            bucket_label = "+Inf";
        } else {
            char buf[64];
            std::snprintf(buf, sizeof(buf), "%.6g", buckets[i]);
            bucket_label = buf;
        }
        append_metric_line(oss, name + "_bucket", bucket_val,
                           {{"le", bucket_label}});
    }
    // +Inf bucket
    append_metric_line(oss, name + "_bucket", count_val, {{"le", "+Inf"}});

    append_metric_line(oss, name + "_sum", sum_val);
    append_metric_line(oss, name + "_count", count_val);
}

} // anonymous namespace

// ============================================================================
// PrometheusExporter::Impl — the actual HTTP server + scrape engine
// ============================================================================

class PrometheusExporter::Impl {
public:
    explicit Impl(uint16_t port)
        : port_(port) {}

    ~Impl() {
        shutdown();
    }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    // -- Lifecycle -----------------------------------------------------------

    void start() {
        if (running_.load(std::memory_order_acquire)) return;

        listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if (listen_fd_ < 0) {
            EXP_LOG_ERROR("Failed to create metrics socket: {}", strerror(errno));
            throw std::runtime_error("Cannot create prometheus listen socket");
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
            EXP_LOG_ERROR("Failed to bind metrics port {}: {}", port_, strerror(errno));
            close(listen_fd_);
            listen_fd_ = -1;
            throw std::runtime_error("Cannot bind prometheus port " + std::to_string(port_));
        }

        if (listen(listen_fd_, kListenBacklog) < 0) {
            EXP_LOG_ERROR("Failed to listen on metrics port {}: {}", port_, strerror(errno));
            close(listen_fd_);
            listen_fd_ = -1;
            throw std::runtime_error("Cannot listen on prometheus port " + std::to_string(port_));
        }

        // Mark non-blocking (redundant with SOCK_NONBLOCK, but safe)
        int flags = fcntl(listen_fd_, F_GETFL, 0);
        fcntl(listen_fd_, F_SETFL, flags | O_NONBLOCK);

        running_.store(true, std::memory_order_release);

        // Start worker threads
        for (size_t i = 0; i < kDefaultWorkers; ++i) {
            workers_.emplace_back(&Impl::worker_loop, this);
        }

        // Start accept thread
        accept_thread_ = std::thread(&Impl::accept_loop, this);

        EXP_LOG_INFO("Prometheus exporter started on port {}", port_);
    }

    void shutdown() {
        if (!running_.load(std::memory_order_acquire)) return;

        EXP_LOG_INFO("Shutting down Prometheus exporter...");
        running_.store(false, std::memory_order_release);

        // Close listen socket to unblock accept()
        if (listen_fd_ >= 0) {
            close(listen_fd_);
            listen_fd_ = -1;
        }

        // Wake workers
        queue_cv_.notify_all();

        if (accept_thread_.joinable()) {
            accept_thread_.join();
        }

        for (auto& w : workers_) {
            if (w.joinable()) w.join();
        }
        workers_.clear();

        // Drain and close any remaining queued connections
        {
            std::lock_guard lock(queue_mutex_);
            while (!client_queue_.empty()) {
                close(client_queue_.front());
                client_queue_.pop();
            }
        }

        EXP_LOG_INFO("Prometheus exporter shut down");
    }

    [[nodiscard]] uint16_t port() const noexcept { return port_; }
    [[nodiscard]] bool is_running() const noexcept {
        return running_.load(std::memory_order_acquire);
    }

    // -- Scrape --------------------------------------------------------------

    [[nodiscard]] std::string scrape() const {
        return build_prometheus_output();
    }

private:
    // -- Accept loop (single thread) -----------------------------------------

    void accept_loop() {
        while (running_.load(std::memory_order_acquire)) {
            struct sockaddr_in client_addr = {};
            socklen_t addr_len = sizeof(client_addr);
            int client_fd = accept4(listen_fd_,
                                    reinterpret_cast<struct sockaddr*>(&client_addr),
                                    &addr_len, SOCK_NONBLOCK);
            if (client_fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(kAcceptPollTimeoutMs));
                    continue;
                }
                if (errno == EINTR) continue;
                if (!running_.load(std::memory_order_acquire)) break;
                EXP_LOG_ERROR("Accept error: {}", strerror(errno));
                continue;
            }

            int opt = 1;
            setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

            {
                std::lock_guard lock(queue_mutex_);
                client_queue_.push(client_fd);
            }
            queue_cv_.notify_one();
        }
    }

    // -- Worker loop ---------------------------------------------------------

    void worker_loop() {
        while (running_.load(std::memory_order_acquire)) {
            int client_fd = -1;

            {
                std::unique_lock lock(queue_mutex_);
                queue_cv_.wait(lock, [this] {
                    return !client_queue_.empty() || !running_.load(std::memory_order_acquire);
                });
                if (!running_.load(std::memory_order_acquire) && client_queue_.empty()) break;
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
        char buf[kReadBufferSize];
        ssize_t total_read = 0;

        // Read request with a short timeout
        struct timeval tv;
        tv.tv_sec = 5;
        tv.tv_usec = 0;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        ssize_t n = recv(fd, buf, sizeof(buf) - 1, 0);
        if (n <= 0) {
            if (n < 0) {
                EXP_LOG_TRACE("recv error on metrics fd {}: {}", fd, strerror(errno));
            }
            return;
        }
        total_read = n;
        buf[total_read] = '\0';

        // Parse request
        ParsedRequest req;
        if (!parse_request(buf, static_cast<size_t>(total_read), req)) {
            auto resp = build_http_response(400, "text/plain", "Bad Request\n");
            send(fd, resp.data(), resp.size(), MSG_NOSIGNAL);
            return;
        }

        // Route
        std::string response;
        if (req.path == "/metrics" || req.path == "/") {
            if (req.method == HttpMethod::GET) {
                response = build_http_response(200, kPrometheusContentType,
                                                build_prometheus_output());
            } else {
                response = method_not_allowed_response();
            }
        } else {
            response = not_found_response();
        }

        send(fd, response.data(), response.size(), MSG_NOSIGNAL);
    }

    // -- Prometheus text format builder --------------------------------------

    [[nodiscard]] std::string build_prometheus_output() const {
        std::ostringstream oss;

        append_broker_metrics(oss);
        append_topic_metrics(oss);
        append_process_metrics(oss);
        append_jvm_metrics(oss);
        append_custom_metrics(oss);

        // End with a newline for clarity
        oss << "# EOF\n";

        return oss.str();
    }

    // ------------------------------------------------------------------
    // Broker metrics
    // ------------------------------------------------------------------

    void append_broker_metrics(std::ostringstream& oss) const {
        const auto& bm = get_broker_metrics();

        // messages_produced
        append_metric_header(oss, "torrent_messages_produced_total",
                             "Total number of messages produced.",
                             "counter");
        append_metric_line(oss, "torrent_messages_produced_total",
                           static_cast<double>(bm.messages_produced.load(std::memory_order_relaxed)));

        // messages_consumed
        append_metric_header(oss, "torrent_messages_consumed_total",
                             "Total number of messages consumed.",
                             "counter");
        append_metric_line(oss, "torrent_messages_consumed_total",
                           static_cast<double>(bm.messages_consumed.load(std::memory_order_relaxed)));

        // bytes_produced
        append_metric_header(oss, "torrent_bytes_produced_total",
                             "Total number of bytes produced.",
                             "counter");
        append_metric_line(oss, "torrent_bytes_produced_total",
                           static_cast<double>(bm.bytes_produced.load(std::memory_order_relaxed)));

        // bytes_consumed
        append_metric_header(oss, "torrent_bytes_consumed_total",
                             "Total number of bytes consumed.",
                             "counter");
        append_metric_line(oss, "torrent_bytes_consumed_total",
                           static_cast<double>(bm.bytes_consumed.load(std::memory_order_relaxed)));

        // active_connections
        append_metric_header(oss, "torrent_active_connections",
                             "Current number of active connections.",
                             "gauge");
        append_metric_line(oss, "torrent_active_connections",
                           static_cast<double>(bm.active_connections.load(std::memory_order_relaxed)));

        // total_connections
        append_metric_header(oss, "torrent_connections_total",
                             "Total number of connections accepted.",
                             "counter");
        append_metric_line(oss, "torrent_connections_total",
                           static_cast<double>(bm.total_connections.load(std::memory_order_relaxed)));

        // requests_total
        append_metric_header(oss, "torrent_requests_total",
                             "Total number of requests processed.",
                             "counter");
        append_metric_line(oss, "torrent_requests_total",
                           static_cast<double>(bm.requests_total.load(std::memory_order_relaxed)));

        // requests_failed
        append_metric_header(oss, "torrent_requests_failed_total",
                             "Total number of failed requests.",
                             "counter");
        append_metric_line(oss, "torrent_requests_failed_total",
                           static_cast<double>(bm.requests_failed.load(std::memory_order_relaxed)));

        // leader_elections
        append_metric_header(oss, "torrent_leader_elections_total",
                             "Total number of leader elections.",
                             "counter");
        append_metric_line(oss, "torrent_leader_elections_total",
                           static_cast<double>(bm.leader_elections.load(std::memory_order_relaxed)));

        // disk_usage_bytes
        append_metric_header(oss, "torrent_disk_usage_bytes",
                             "Current disk usage in bytes.",
                             "gauge");
        append_metric_line(oss, "torrent_disk_usage_bytes",
                           static_cast<double>(bm.disk_usage_bytes.load(std::memory_order_relaxed)));

        // under_replicated_partitions
        append_metric_header(oss, "torrent_under_replicated_partitions",
                             "Number of under-replicated partitions.",
                             "gauge");
        append_metric_line(oss, "torrent_under_replicated_partitions",
                           static_cast<double>(bm.under_replicated_partitions.load(std::memory_order_relaxed)));
    }

    // ------------------------------------------------------------------
    // Per-topic metrics
    // ------------------------------------------------------------------

    void append_topic_metrics(std::ostringstream& oss) const {
        auto topic_names = get_topic_names();
        if (topic_names.empty()) return;

        // messages_in (per topic)
        {
            append_metric_header(oss, "torrent_topic_messages_in_total",
                                 "Total number of messages produced into topic.",
                                 "counter");
            for (const auto& name : topic_names) {
                const auto* tm = find_topic_metrics(name);
                if (!tm) continue;
                append_metric_line(oss, "torrent_topic_messages_in_total",
                                   static_cast<double>(tm->messages_in.load(std::memory_order_relaxed)),
                                   {{"topic", name}});
            }
        }

        // messages_out (per topic)
        {
            append_metric_header(oss, "torrent_topic_messages_out_total",
                                 "Total number of messages consumed from topic.",
                                 "counter");
            for (const auto& name : topic_names) {
                const auto* tm = find_topic_metrics(name);
                if (!tm) continue;
                append_metric_line(oss, "torrent_topic_messages_out_total",
                                   static_cast<double>(tm->messages_out.load(std::memory_order_relaxed)),
                                   {{"topic", name}});
            }
        }

        // bytes_in (per topic)
        {
            append_metric_header(oss, "torrent_topic_bytes_in_total",
                                 "Total number of bytes produced into topic.",
                                 "counter");
            for (const auto& name : topic_names) {
                const auto* tm = find_topic_metrics(name);
                if (!tm) continue;
                append_metric_line(oss, "torrent_topic_bytes_in_total",
                                   static_cast<double>(tm->bytes_in.load(std::memory_order_relaxed)),
                                   {{"topic", name}});
            }
        }

        // bytes_out (per topic)
        {
            append_metric_header(oss, "torrent_topic_bytes_out_total",
                                 "Total number of bytes consumed from topic.",
                                 "counter");
            for (const auto& name : topic_names) {
                const auto* tm = find_topic_metrics(name);
                if (!tm) continue;
                append_metric_line(oss, "torrent_topic_bytes_out_total",
                                   static_cast<double>(tm->bytes_out.load(std::memory_order_relaxed)),
                                   {{"topic", name}});
            }
        }

        // size_bytes (per topic, gauge)
        {
            append_metric_header(oss, "torrent_topic_size_bytes",
                                 "Current size of the topic in bytes.",
                                 "gauge");
            for (const auto& name : topic_names) {
                const auto* tm = find_topic_metrics(name);
                if (!tm) continue;
                append_metric_line(oss, "torrent_topic_size_bytes",
                                   static_cast<double>(tm->size_bytes.load(std::memory_order_relaxed)),
                                   {{"topic", name}});
            }
        }
    }

    // ------------------------------------------------------------------
    // Process metrics (CPU, memory, fd count via /proc/self)
    // ------------------------------------------------------------------

    void append_process_metrics(std::ostringstream& oss) const {
        // Process CPU seconds
        {
            double cpu_s = read_process_cpu_seconds();
            if (cpu_s >= 0.0) {
                append_metric_header(oss, "torrent_process_cpu_seconds_total",
                                     "Total user and system CPU time spent in seconds.",
                                     "counter");
                append_metric_line(oss, "torrent_process_cpu_seconds_total", cpu_s);
            }
        }

        // Resident memory (RSS)
        {
            int64_t rss = read_process_rss_bytes();
            if (rss >= 0) {
                append_metric_header(oss, "torrent_process_resident_memory_bytes",
                                     "Resident memory size in bytes.",
                                     "gauge");
                append_metric_line(oss, "torrent_process_resident_memory_bytes",
                                   static_cast<double>(rss));
            }
        }

        // Open file descriptors
        {
            int64_t fds = read_process_fd_count();
            if (fds >= 0) {
                append_metric_header(oss, "torrent_process_open_fds",
                                     "Number of open file descriptors.",
                                     "gauge");
                append_metric_line(oss, "torrent_process_open_fds",
                                   static_cast<double>(fds));
            }
        }

        // Process uptime
        {
            double uptime = read_process_uptime_seconds();
            if (uptime >= 0.0) {
                append_metric_header(oss, "torrent_process_uptime_seconds",
                                     "Process uptime in seconds.",
                                     "gauge");
                append_metric_line(oss, "torrent_process_uptime_seconds", uptime);
            }
        }

        // Process start time (seconds since epoch)
        {
            double start_s = get_jvm_start_time_seconds();
            append_metric_header(oss, "torrent_process_start_time_seconds",
                                 "Start time of the process since unix epoch.",
                                 "gauge");
            append_metric_line(oss, "torrent_process_start_time_seconds", start_s);
        }

        // Max file descriptors (from getrlimit)
        {
            struct rlimit rlim;
            if (getrlimit(RLIMIT_NOFILE, &rlim) == 0) {
                append_metric_header(oss, "torrent_process_max_fds",
                                     "Maximum number of open file descriptors.",
                                     "gauge");
                append_metric_line(oss, "torrent_process_max_fds",
                                   static_cast<double>(rlim.rlim_cur));
            }
        }
    }

    // ------------------------------------------------------------------
    // JVM-mimic metrics for JMX compatibility
    // ------------------------------------------------------------------

    void append_jvm_metrics(std::ostringstream& oss) const {
        // -- JVM memory (mimicked from process RSS) --
        int64_t rss = read_process_rss_bytes();

        // jvm_memory_bytes_used{area="heap"}
        append_metric_header(oss, "jvm_memory_bytes_used",
                             "Estimated JVM heap memory used (from process RSS).",
                             "gauge");
        // Heuristic: ~75% of RSS is heap (rest is native/metaspace)
        double heap_est = (rss > 0) ? static_cast<double>(rss) * 0.75 : 0.0;
        double nonheap_est = (rss > 0) ? static_cast<double>(rss) * 0.25 : 0.0;

        append_metric_line(oss, "jvm_memory_bytes_used", heap_est,
                           {{"area", "heap"}});
        append_metric_line(oss, "jvm_memory_bytes_used", nonheap_est,
                           {{"area", "nonheap"}});

        // jvm_memory_bytes_committed
        append_metric_header(oss, "jvm_memory_bytes_committed",
                             "Estimated JVM memory committed.",
                             "gauge");
        // Use RSS as committed
        double committed = (rss > 0) ? static_cast<double>(rss) : 0.0;
        append_metric_line(oss, "jvm_memory_bytes_committed", committed,
                           {{"area", "heap"}});
        append_metric_line(oss, "jvm_memory_bytes_committed",
                           committed * 0.3, {{"area", "nonheap"}});

        // jvm_memory_bytes_max
        append_metric_header(oss, "jvm_memory_bytes_max",
                             "Estimated JVM maximum memory.",
                             "gauge");
        // No real max in native code — report a large value
        append_metric_line(oss, "jvm_memory_bytes_max", 8.0 * 1024 * 1024 * 1024,
                           {{"area", "heap"}});
        append_metric_line(oss, "jvm_memory_bytes_max", 2.0 * 1024 * 1024 * 1024,
                           {{"area", "nonheap"}});

        // jvm_memory_pool_bytes_used
        append_metric_header(oss, "jvm_memory_pool_bytes_used",
                             "Estimated JVM memory pool usage.",
                             "gauge");
        double pool_val = (rss > 0) ? static_cast<double>(rss) * 0.25 : 0.0;
        append_metric_line(oss, "jvm_memory_pool_bytes_used", pool_val,
                           {{"pool", "Code Cache"}});
        append_metric_line(oss, "jvm_memory_pool_bytes_used", pool_val,
                           {{"pool", "Metaspace"}});
        append_metric_line(oss, "jvm_memory_pool_bytes_used",
                           heap_est * 0.33, {{"pool", "Eden Space"}});
        append_metric_line(oss, "jvm_memory_pool_bytes_used",
                           heap_est * 0.33, {{"pool", "Survivor Space"}});
        append_metric_line(oss, "jvm_memory_pool_bytes_used",
                           heap_est * 0.34, {{"pool", "Tenured Gen"}});

        // -- JVM GC --
        append_metric_header(oss, "jvm_gc_collection_seconds_sum",
                             "Estimated JVM GC collection time (always 0 for native).",
                             "summary");
        append_metric_line(oss, "jvm_gc_collection_seconds_sum", 0.0,
                           {{"gc", "G1 Young Generation"}});
        append_metric_line(oss, "jvm_gc_collection_seconds_sum", 0.0,
                           {{"gc", "G1 Old Generation"}});

        append_metric_header(oss, "jvm_gc_collection_seconds_count",
                             "Estimated JVM GC collection count.",
                             "summary");
        append_metric_line(oss, "jvm_gc_collection_seconds_count", 0.0,
                           {{"gc", "G1 Young Generation"}});
        append_metric_line(oss, "jvm_gc_collection_seconds_count", 0.0,
                           {{"gc", "G1 Old Generation"}});

        // -- JVM threads --
        append_metric_header(oss, "jvm_threads_current",
                             "Current number of live threads.",
                             "gauge");
        // Use a rough estimate from /proc/self/status Threads line
        {
            int64_t thread_count = -1;
            std::ifstream status("/proc/self/status");
            if (status) {
                std::string line;
                while (std::getline(status, line)) {
                    if (line.starts_with("Threads:")) {
                        std::istringstream iss(line);
                        std::string label;
                        int64_t val;
                        iss >> label >> val;
                        thread_count = val;
                        break;
                    }
                }
            }
            if (thread_count < 0) thread_count = static_cast<int64_t>(workers_.size()) + 2;
            append_metric_line(oss, "jvm_threads_current",
                               static_cast<double>(thread_count));
        }

        append_metric_header(oss, "jvm_threads_daemon",
                             "Current number of daemon threads.",
                             "gauge");
        append_metric_line(oss, "jvm_threads_daemon", 0.0);

        append_metric_header(oss, "jvm_threads_peak",
                             "Peak number of threads since start.",
                             "gauge");
        append_metric_line(oss, "jvm_threads_peak", 0.0);

        // -- JVM classes --
        append_metric_header(oss, "jvm_classes_loaded",
                             "Estimated number of loaded classes (always 0).",
                             "gauge");
        append_metric_line(oss, "jvm_classes_loaded", 0.0);

        append_metric_header(oss, "jvm_classes_loaded_total",
                             "Total number of classes loaded (always 0).",
                             "counter");
        append_metric_line(oss, "jvm_classes_loaded_total", 0.0);

        // -- JVM buffer pools --
        append_metric_header(oss, "jvm_buffer_pool_used_bytes",
                             "Estimated JVM buffer pool used bytes.",
                             "gauge");
        append_metric_line(oss, "jvm_buffer_pool_used_bytes", 0.0,
                           {{"pool", "direct"}});
        append_metric_line(oss, "jvm_buffer_pool_used_bytes", 0.0,
                           {{"pool", "mapped"}});

        append_metric_header(oss, "jvm_buffer_pool_capacity_bytes",
                             "Estimated JVM buffer pool capacity.",
                             "gauge");
        append_metric_line(oss, "jvm_buffer_pool_capacity_bytes", 0.0,
                           {{"pool", "direct"}});
        append_metric_line(oss, "jvm_buffer_pool_capacity_bytes", 0.0,
                           {{"pool", "mapped"}});

        // -- JVM info (constant) --
        append_metric_header(oss, "jvm_info",
                             "JVM version info (mimicked).",
                             "gauge");
        append_metric_line(oss, "jvm_info", 1.0,
            {{"version", "torrent-mq-native-0.10.0"},
             {"vendor", "Nous Research"}});
    }

    // ------------------------------------------------------------------
    // Custom metrics registered via MetricsRegistry
    // ------------------------------------------------------------------

    void append_custom_metrics(std::ostringstream& oss) const {
        auto snap = get_custom_metrics_snapshot();

        // Gauges
        for (const auto& g : snap.gauges) {
            std::string name = "torrent_custom_" + sanitize_metric_name(g.name);
            std::string help = g.help.empty()
                ? "Custom gauge: " + g.name
                : g.help;
            append_metric_header(oss, name, help, "gauge");
            double val = static_cast<double>(g.value->load(std::memory_order_relaxed));
            append_metric_line(oss, name, val);
        }

        // Counters
        for (const auto& c : snap.counters) {
            std::string name = "torrent_custom_" + sanitize_metric_name(c.name) + "_total";
            std::string help = c.help.empty()
                ? "Custom counter: " + c.name
                : c.help;
            append_metric_header(oss, name, help, "counter");
            double val = static_cast<double>(c.value->load(std::memory_order_relaxed));
            append_metric_line(oss, name, val);
        }

        // Histograms
        for (const auto& h : snap.histograms) {
            std::string name = "torrent_custom_" + sanitize_metric_name(h.name);
            std::string help = h.help.empty()
                ? "Custom histogram: " + h.name
                : h.help;
            append_histogram_metrics(oss, name, help,
                                     h.sum, h.count,
                                     h.buckets, h.bucket_counts);
        }
    }

    // -- Members -------------------------------------------------------------

    uint16_t port_;
    int listen_fd_ = -1;
    std::atomic<bool> running_{false};

    std::thread accept_thread_;
    std::vector<std::thread> workers_;

    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::queue<int> client_queue_;
};

// ============================================================================
// PrometheusExporter — public API delegation
// ============================================================================

PrometheusExporter::PrometheusExporter(uint16_t port)
    : port_(port)
    , impl_(std::make_unique<Impl>(port))
{
    EXP_LOG_DEBUG("PrometheusExporter constructed for port {}", port);
}

PrometheusExporter::~PrometheusExporter() {
    // impl_'s destructor handles shutdown if still running
}

PrometheusExporter::PrometheusExporter(PrometheusExporter&& other) noexcept
    : impl_(std::move(other.impl_))
    , port_(other.port_)
{
    other.port_ = 0;
}

PrometheusExporter& PrometheusExporter::operator=(PrometheusExporter&& other) noexcept {
    if (this != &other) {
        if (impl_) {
            impl_->shutdown();
        }
        impl_ = std::move(other.impl_);
        port_ = other.port_;
        other.port_ = 0;
    }
    return *this;
}

void PrometheusExporter::start() {
    impl_->start();
}

void PrometheusExporter::shutdown() {
    impl_->shutdown();
}

uint16_t PrometheusExporter::port() const noexcept {
    return port_;
}

std::string PrometheusExporter::scrape() const {
    return impl_->scrape();
}

} // namespace torrent::metrics
