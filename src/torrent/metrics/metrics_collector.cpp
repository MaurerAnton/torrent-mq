#include "torrent/metrics/metrics.h"
#include "torrent/metrics/prometheus_exporter.h"
#include "torrent/common/types.h"
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <chrono>
#include <cmath>
#include <deque>
#include <fstream>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/statvfs.h>

namespace torrent::metrics {
namespace {

using json = nlohmann::json;

// ============================================================================
// Sliding Window Rate Calculator
// ============================================================================
class SlidingWindowRate {
public:
    explicit SlidingWindowRate(std::chrono::seconds window_size)
        : window_size_(window_size) {}

    void record(int64_t value) {
        auto now = std::chrono::steady_clock::now();
        samples_.push_back({now, value});
        total_ += value;
        prune();
    }

    double rate_per_second() const {
        prune();
        if (samples_.empty()) return 0.0;
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration<double>(now - samples_.front().timestamp);
        if (elapsed.count() < 0.001) return 0.0;
        return static_cast<double>(total_.load()) / elapsed.count();
    }

    int64_t total() const { return total_.load(); }

    void reset() {
        samples_.clear();
        total_.store(0);
    }

private:
    void prune() const {
        auto now = std::chrono::steady_clock::now();
        auto cutoff = now - window_size_;
        while (!samples_.empty() && samples_.front().timestamp < cutoff) {
            total_.fetch_sub(samples_.front().value);
            samples_.pop_front();
        }
    }

    struct Sample { std::chrono::steady_clock::time_point timestamp; int64_t value; };
    std::chrono::seconds window_size_;
    mutable std::deque<Sample> samples_;
    mutable std::atomic<int64_t> total_{0};
};

// ============================================================================
// Histogram with Percentiles
// ============================================================================
class LatencyHistogram {
public:
    void record(int64_t latency_us) {
        count_.fetch_add(1);
        sum_.fetch_add(latency_us);
        if (latency_us > max_.load()) max_.store(latency_us);
        if (latency_us < min_.load() || min_.load() == 0) min_.store(latency_us);

        // Store in bounded reservoir for percentile calculation
        std::lock_guard<std::mutex> lock(mutex_);
        reservoir_.push_back(latency_us);
        if (reservoir_.size() > kMaxReservoir) {
            reservoir_.pop_front();
        }
    }

    int64_t count() const { return count_.load(); }
    int64_t sum() const { return sum_.load(); }
    double avg() const { return count_.load() > 0 ? static_cast<double>(sum_.load()) / count_.load() : 0.0; }
    int64_t max() const { return max_.load(); }
    int64_t min() const { return min_.load(); }

    int64_t percentile(double p) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (reservoir_.empty()) return 0;
        std::vector<int64_t> sorted(reservoir_.begin(), reservoir_.end());
        std::sort(sorted.begin(), sorted.end());
        size_t idx = static_cast<size_t>(p * sorted.size());
        if (idx >= sorted.size()) idx = sorted.size() - 1;
        return sorted[idx];
    }

    int64_t p50() const { return percentile(0.50); }
    int64_t p95() const { return percentile(0.95); }
    int64_t p99() const { return percentile(0.99); }
    int64_t p999() const { return percentile(0.999); }

private:
    static constexpr size_t kMaxReservoir = 10000;
    std::atomic<int64_t> count_{0}, sum_{0}, max_{0}, min_{INT64_MAX};
    mutable std::mutex mutex_;
    mutable std::deque<int64_t> reservoir_;
};

// ============================================================================
// Broker Metrics Collector
// ============================================================================
class BrokerMetricsCollector {
public:
    BrokerMetricsCollector() : produce_rate_(std::chrono::seconds(60)),
                                fetch_rate_(std::chrono::seconds(60)),
                                request_rate_(std::chrono::seconds(60)) {}

    void record_produce(int64_t messages, int64_t bytes) {
        produce_rate_.record(messages);
        auto& m = MetricsRegistry::instance().broker();
        m.messages_produced.fetch_add(messages);
        m.bytes_produced.fetch_add(bytes);
        produce_latency_.record(bytes > 0 ? bytes / messages : 0);
    }

    void record_fetch(int64_t messages, int64_t bytes) {
        fetch_rate_.record(messages);
        auto& m = MetricsRegistry::instance().broker();
        m.messages_consumed.fetch_add(messages);
        m.bytes_consumed.fetch_add(bytes);
    }

    void record_request(int16_t api_key) {
        request_rate_.record(1);
        auto& m = MetricsRegistry::instance().broker();
        m.requests_total.fetch_add(1);
        api_request_counts_[api_key].fetch_add(1);
    }

    void record_failed_request() {
        auto& m = MetricsRegistry::instance().broker();
        m.requests_failed.fetch_add(1);
    }

    void record_connection_open() {
        auto& m = MetricsRegistry::instance().broker();
        m.active_connections.fetch_add(1);
        m.total_connections.fetch_add(1);
    }

    void record_connection_close() {
        auto& m = MetricsRegistry::instance().broker();
        m.active_connections.fetch_sub(1);
    }

    void record_election() {
        auto& m = MetricsRegistry::instance().broker();
        m.leader_elections.fetch_add(1);
    }

    void update_disk_usage(int64_t bytes) {
        auto& m = MetricsRegistry::instance().broker();
        m.disk_usage_bytes.store(bytes);
    }

    void update_under_replicated(int64_t count) {
        auto& m = MetricsRegistry::instance().broker();
        m.under_replicated_partitions.store(count);
    }

    json snapshot() const {
        json j;
        j["produce_rate_per_sec"] = produce_rate_.rate_per_second();
        j["fetch_rate_per_sec"] = fetch_rate_.rate_per_second();
        j["request_rate_per_sec"] = request_rate_.rate_per_second();
        j["produce_latency_us"] = {{"p50", produce_latency_.p50()},
                                     {"p95", produce_latency_.p95()},
                                     {"p99", produce_latency_.p99()}};
        j["api_request_counts"] = json::object();
        for (const auto& [api_key, count] : api_request_counts_) {
            j["api_request_counts"][std::to_string(api_key)] = count.load();
        }

        // Process metrics
        struct rusage usage;
        if (getrusage(RUSAGE_SELF, &usage) == 0) {
            j["process"]["cpu_user_sec"] = usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6;
            j["process"]["cpu_sys_sec"] = usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1e6;
            j["process"]["max_rss_mb"] = usage.ru_maxrss / 1024.0;
        }

        // Disk usage
        struct statvfs svfs;
        if (statvfs("/var/lib/torrent/data", &svfs) == 0) {
            j["disk"]["total_bytes"] = svfs.f_blocks * svfs.f_frsize;
            j["disk"]["available_bytes"] = svfs.f_bavail * svfs.f_frsize;
            j["disk"]["used_percent"] = 100.0 * (1.0 - static_cast<double>(svfs.f_bavail) / svfs.f_blocks);
        }

        return j;
    }

    // JMX-compatible metrics
    json jmx_metrics() const {
        json j;
        j["java.lang:type=Memory"]["HeapMemoryUsage"]["used"] = 0;
        j["java.lang:type=Memory"]["HeapMemoryUsage"]["max"] = 0;
        j["java.lang:type=GarbageCollector,name=G1"]["CollectionCount"] = 0;
        j["java.lang:type=Threading"]["ThreadCount"] = 0;
        j["java.lang:type=ClassLoading"]["LoadedClassCount"] = 0;
        return j;
    }

    // Prometheus text format
    std::string prometheus_text() const {
        std::ostringstream oss;
        auto& m = MetricsRegistry::instance().broker();

        oss << "# HELP torrent_messages_produced_total Total messages produced\n";
        oss << "# TYPE torrent_messages_produced_total counter\n";
        oss << "torrent_messages_produced_total " << m.messages_produced.load() << "\n\n";

        oss << "# HELP torrent_messages_consumed_total Total messages consumed\n";
        oss << "# TYPE torrent_messages_consumed_total counter\n";
        oss << "torrent_messages_consumed_total " << m.messages_consumed.load() << "\n\n";

        oss << "# HELP torrent_bytes_produced_total Total bytes produced\n";
        oss << "# TYPE torrent_bytes_produced_total counter\n";
        oss << "torrent_bytes_produced_total " << m.bytes_produced.load() << "\n\n";

        oss << "# HELP torrent_bytes_consumed_total Total bytes consumed\n";
        oss << "# TYPE torrent_bytes_consumed_total counter\n";
        oss << "torrent_bytes_consumed_total " << m.bytes_consumed.load() << "\n\n";

        oss << "# HELP torrent_active_connections Current active connections\n";
        oss << "# TYPE torrent_active_connections gauge\n";
        oss << "torrent_active_connections " << m.active_connections.load() << "\n\n";

        oss << "# HELP torrent_requests_total Total requests\n";
        oss << "# TYPE torrent_requests_total counter\n";
        oss << "torrent_requests_total " << m.requests_total.load() << "\n\n";

        oss << "# HELP torrent_requests_failed_total Total failed requests\n";
        oss << "# TYPE torrent_requests_failed_total counter\n";
        oss << "torrent_requests_failed_total " << m.requests_failed.load() << "\n\n";

        oss << "# HELP torrent_leader_elections_total Total leader elections\n";
        oss << "# TYPE torrent_leader_elections_total counter\n";
        oss << "torrent_leader_elections_total " << m.leader_elections.load() << "\n\n";

        oss << "# HELP torrent_disk_usage_bytes Current disk usage\n";
        oss << "# TYPE torrent_disk_usage_bytes gauge\n";
        oss << "torrent_disk_usage_bytes " << m.disk_usage_bytes.load() << "\n\n";

        oss << "# HELP torrent_under_replicated_partitions Under-replicated partitions\n";
        oss << "# TYPE torrent_under_replicated_partitions gauge\n";
        oss << "torrent_under_replicated_partitions " << m.under_replicated_partitions.load() << "\n\n";

        oss << "# HELP torrent_produce_rate_per_second Produce message rate\n";
        oss << "# TYPE torrent_produce_rate_per_second gauge\n";
        oss << "torrent_produce_rate_per_second " << produce_rate_.rate_per_second() << "\n\n";

        oss << "# HELP torrent_fetch_rate_per_second Fetch message rate\n";
        oss << "# TYPE torrent_fetch_rate_per_second gauge\n";
        oss << "torrent_fetch_rate_per_second " << fetch_rate_.rate_per_second() << "\n\n";

        oss << "# HELP torrent_produce_latency_us Produce latency histogram\n";
        oss << "# TYPE torrent_produce_latency_us summary\n";
        oss << "torrent_produce_latency_us{quantile=\"0.5\"} " << produce_latency_.p50() << "\n";
        oss << "torrent_produce_latency_us{quantile=\"0.95\"} " << produce_latency_.p95() << "\n";
        oss << "torrent_produce_latency_us{quantile=\"0.99\"} " << produce_latency_.p99() << "\n";
        oss << "torrent_produce_latency_us_count " << produce_latency_.count() << "\n";
        oss << "torrent_produce_latency_us_sum " << produce_latency_.sum() << "\n";

        return oss.str();
    }

private:
    mutable SlidingWindowRate produce_rate_, fetch_rate_, request_rate_;
    LatencyHistogram produce_latency_;
    std::map<int16_t, std::atomic<int64_t>> api_request_counts_;
};

// Global collector instance
static BrokerMetricsCollector g_collector;

} // anonymous namespace

BrokerMetricsCollector& get_collector() { return g_collector; }

} // namespace torrent::metrics
