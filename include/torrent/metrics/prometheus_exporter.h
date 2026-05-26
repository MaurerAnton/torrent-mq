#pragma once
#include <memory>
#include <string>
#include <vector>
#include <torrent/common/types.h>

namespace torrent::metrics {

// Forward-declared for pimpl
const struct BrokerMetrics& get_broker_metrics();
std::vector<std::string> get_topic_names();
const struct TopicMetrics* find_topic_metrics(const std::string& name);
struct CustomMetricsSnapshot;
CustomMetricsSnapshot get_custom_metrics_snapshot();

class PrometheusExporter {
public:
    explicit PrometheusExporter(uint16_t port = 9090);
    ~PrometheusExporter();
    PrometheusExporter(const PrometheusExporter&) = delete;
    PrometheusExporter& operator=(const PrometheusExporter&) = delete;
    PrometheusExporter(PrometheusExporter&&) noexcept;
    PrometheusExporter& operator=(PrometheusExporter&&) noexcept;

    void start();
    void shutdown();
    uint16_t port() const noexcept;
    std::string scrape() const;
private:
    class Impl;
    std::unique_ptr<Impl> impl_;
    uint16_t port_;
};
}