/**
 * metrics.cpp — MetricsRegistry Singleton + BrokerMetrics Reset
 *
 * Implements the MetricsRegistry Meyer's singleton and the BrokerMetrics
 * reset operation.  The registry owns a single BrokerMetrics instance,
 * a thread-safe map of per-topic metrics, and optional gauge/counter/
 * histogram registrations for Prometheus export.
 *
 * Design:
 *   - MetricsRegistry::instance() uses the Meyer's singleton pattern
 *     (function-local static) which is thread-safe in C++11 and later.
 *   - BrokerMetrics::reset() atomically zeroes all counters — useful
 *     for testing and for admin-triggered metric resets.
 *   - Topic metrics are stored in a ConcurrentMap-like structure
 *     protected by a shared_mutex (read-heavy, write-rare).
 *   - Gauge/counter/histogram registration stores raw pointers to
 *     std::atomic variables that the PrometheusExporter reads at
 *     scrape time.
 *
 * Thread safety:
 *   - BrokerMetrics fields are individual std::atomic<T> — no mutex
 *     needed for read/write.
 *   - Topic metrics map uses std::shared_mutex.
 *   - Custom metrics vectors use std::mutex.
 */

// ============================================================================
// Project headers
// ============================================================================

#include "torrent/metrics/metrics.h"
#include "torrent/common/types.h"

// ============================================================================
// System headers
// ============================================================================

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace torrent::metrics {

// ============================================================================
// Anonymous namespace — registry state
// ============================================================================

namespace {

/// Per-topic metrics map: topic_name → TopicMetrics
/// Protected by a shared_mutex — reads are concurrent, writes are exclusive.
std::unordered_map<std::string, TopicMetrics> g_topic_metrics;
std::shared_mutex g_topic_mutex;

/// Custom gauge/counter/histogram registrations.
struct GaugeDescriptor {
    std::string name;
    std::atomic<int64_t>* value = nullptr;
};

struct CounterDescriptor {
    std::string name;
    std::atomic<uint64_t>* value = nullptr;
};

struct HistogramDescriptor {
    std::string name;
    std::atomic<uint64_t>* sum = nullptr;
    std::atomic<uint64_t>* count = nullptr;
};

std::vector<GaugeDescriptor>    g_gauges;
std::vector<CounterDescriptor>  g_counters;
std::vector<HistogramDescriptor> g_histograms;
std::mutex g_custom_mutex;

} // anonymous namespace

// ============================================================================
// BrokerMetrics — reset
// ============================================================================

void BrokerMetrics::reset() {
    // Atomically zero all counters.  Using store(0) on each atomic is
    // the simplest approach that doesn't introduce a gap where a reader
    // sees partial zeros mixed with old values — each field is updated
    // independently, but since they are separate atomics this is
    // acceptable for a reset operation.
    messages_produced.store(0, std::memory_order_release);
    messages_consumed.store(0, std::memory_order_release);
    bytes_produced.store(0, std::memory_order_release);
    bytes_consumed.store(0, std::memory_order_release);
    active_connections.store(0, std::memory_order_release);
    total_connections.store(0, std::memory_order_release);
    requests_total.store(0, std::memory_order_release);
    requests_failed.store(0, std::memory_order_release);
    leader_elections.store(0, std::memory_order_release);
    disk_usage_bytes.store(0, std::memory_order_release);
    under_replicated_partitions.store(0, std::memory_order_release);
}

// ============================================================================
// MetricsRegistry — singleton
// ============================================================================

MetricsRegistry& MetricsRegistry::instance() {
    // Meyer's singleton — guaranteed thread-safe by C++11 [stmt.dcl]/4.
    static MetricsRegistry registry;
    return registry;
}

BrokerMetrics& MetricsRegistry::broker() {
    // Single BrokerMetrics instance owned by the registry via function-local
    // static, avoiding static-init-order fiasco.
    static BrokerMetrics metrics;
    return metrics;
}

TopicMetrics& MetricsRegistry::topic(const std::string& name) {
    // Double-checked locking for read-heavy access pattern.
    {
        std::shared_lock lock(g_topic_mutex);
        auto it = g_topic_metrics.find(name);
        if (it != g_topic_metrics.end()) {
            return it->second;
        }
    }

    // Write path: insert a new TopicMetrics entry.
    std::unique_lock lock(g_topic_mutex);
    // Double-check: another thread may have inserted between our locks.
    auto it = g_topic_metrics.find(name);
    if (it != g_topic_metrics.end()) {
        return it->second;
    }

    TopicMetrics tm;
    tm.topic_name = name;
    auto [inserted_it, _] = g_topic_metrics.emplace(name, std::move(tm));
    return inserted_it->second;
}

void MetricsRegistry::register_gauge(const std::string& name,
                                      std::atomic<int64_t>* value) {
    std::lock_guard lock(g_custom_mutex);
    g_gauges.push_back(GaugeDescriptor{name, value});
}

void MetricsRegistry::register_counter(const std::string& name,
                                        std::atomic<uint64_t>* value) {
    std::lock_guard lock(g_custom_mutex);
    g_counters.push_back(CounterDescriptor{name, value});
}

void MetricsRegistry::register_histogram(const std::string& name,
                                          std::atomic<uint64_t>* sum,
                                          std::atomic<uint64_t>* count) {
    std::lock_guard lock(g_custom_mutex);
    g_histograms.push_back(HistogramDescriptor{name, sum, count});
}

// ============================================================================
// Free functions — for PrometheusExporter and monitoring
// ============================================================================

const BrokerMetrics& get_broker_metrics() {
    return MetricsRegistry::instance().broker();
}

std::vector<std::string> get_topic_names() {
    std::shared_lock lock(g_topic_mutex);
    std::vector<std::string> names;
    names.reserve(g_topic_metrics.size());
    for (const auto& [name, _] : g_topic_metrics) {
        names.push_back(name);
    }
    return names;
}

const TopicMetrics* find_topic_metrics(const std::string& name) {
    std::shared_lock lock(g_topic_mutex);
    auto it = g_topic_metrics.find(name);
    if (it != g_topic_metrics.end()) {
        return &it->second;
    }
    return nullptr;
}

CustomMetricsSnapshot get_custom_metrics_snapshot() {
    CustomMetricsSnapshot snap;
    {
        std::lock_guard lock(g_custom_mutex);
        snap.gauges     = g_gauges;
        snap.counters   = g_counters;
        snap.histograms = g_histograms;
    }
    return snap;
}

} // namespace torrent::metrics
