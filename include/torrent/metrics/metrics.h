#pragma once
#include <cstdint>
#include <string>
#include <atomic>
#include <vector>
#include <torrent/common/types.h>

namespace torrent::metrics {
struct BrokerMetrics {
    std::atomic<uint64_t> messages_produced{0};
    std::atomic<uint64_t> messages_consumed{0};
    std::atomic<uint64_t> bytes_produced{0};
    std::atomic<uint64_t> bytes_consumed{0};
    std::atomic<uint64_t> active_connections{0};
    std::atomic<uint64_t> total_connections{0};
    std::atomic<uint64_t> requests_total{0};
    std::atomic<uint64_t> requests_failed{0};
    std::atomic<uint64_t> leader_elections{0};
    std::atomic<int64_t> disk_usage_bytes{0};
    std::atomic<uint64_t> under_replicated_partitions{0};
    void reset();
};

struct TopicMetrics {
    std::string topic_name;
    std::atomic<uint64_t> messages_in{0};
    std::atomic<uint64_t> messages_out{0};
    std::atomic<uint64_t> bytes_in{0};
    std::atomic<uint64_t> bytes_out{0};
    std::atomic<int64_t> size_bytes{0};
};

struct GaugeDescriptor;
struct CounterDescriptor;
struct HistogramDescriptor;

struct CustomMetricsSnapshot {
    std::vector<struct GaugeDescriptor> gauges;
    std::vector<struct CounterDescriptor> counters;
    std::vector<struct HistogramDescriptor> histograms;
};

class MetricsRegistry {
public:
    static MetricsRegistry& instance();
    BrokerMetrics& broker();
    TopicMetrics& topic(const std::string& name);
    void register_gauge(const std::string& name, std::atomic<int64_t>* value);
    void register_counter(const std::string& name, std::atomic<uint64_t>* value);
    void register_histogram(const std::string& name, std::atomic<uint64_t>* sum, std::atomic<uint64_t>* count);
private:
    MetricsRegistry() = default;
};

// Free functions for PrometheusExporter use
const BrokerMetrics& get_broker_metrics();
std::vector<std::string> get_topic_names();
const TopicMetrics* find_topic_metrics(const std::string& name);
CustomMetricsSnapshot get_custom_metrics_snapshot();

}