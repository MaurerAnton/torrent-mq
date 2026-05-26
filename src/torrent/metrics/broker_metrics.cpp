/**
 * broker_metrics.cpp — BrokerMetrics, TopicMetrics, and MetricsRegistry Implementation
 *
 * Implements the metrics subsystem used across the broker.  BrokerMetrics is a
 * simple POD of std::atomic counters (gauge-equivalents) that subsystems
 * increment/decrement directly.  TopicMetrics holds per-topic counters.
 * MetricsRegistry is a singleton that owns the BrokerMetrics, a map of
 * per-topic TopicMetrics, and a registry of externally-provided
 * gauges/counters/histograms registered by subsystems.
 *
 * Thread-safety:
 *   - BrokerMetrics fields are std::atomic — lock-free increments.
 *   - TopicMetrics fields likewise.
 *   - MetricsRegistry uses a shared_mutex for the gauge/counter/histogram
 *     maps and for topic map access (shared on read, exclusive on write).
 *
 * See metrics.h for the API contract.
 */

#include "torrent/metrics/metrics.h"
#include "torrent/common/types.h"

#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <string_view>
#include <memory>
#include <sstream>

#include <spdlog/spdlog.h>

// ============================================================================
// Convenience aliases
// ============================================================================

namespace torrent::metrics {

// ============================================================================
// Descriptor struct definitions (forward-declared in metrics.h)
// ============================================================================

struct GaugeDescriptor {
    std::string name;
    std::string help;
    std::atomic<int64_t>* value = nullptr;
};

struct CounterDescriptor {
    std::string name;
    std::string help;
    std::atomic<uint64_t>* value = nullptr;
};

struct HistogramDescriptor {
    std::string name;
    std::string help;
    std::atomic<uint64_t>* sum = nullptr;
    std::atomic<uint64_t>* count = nullptr;
    std::vector<double> buckets;
    std::vector<std::atomic<uint64_t>> bucket_counts;
};

// ============================================================================
// Anonymous namespace — internal implementation details
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logging
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_metrics_logger() {
    static auto logger = []() {
        auto l = spdlog::get("metrics_registry");
        if (!l) {
            l = spdlog::stdout_color_mt("metrics_registry");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define METRICS_LOG_DEBUG(...) get_metrics_logger()->debug(__VA_ARGS__)
#define METRICS_LOG_INFO(...)  get_metrics_logger()->info(__VA_ARGS__)
#define METRICS_LOG_WARN(...)  get_metrics_logger()->warn(__VA_ARGS__)

// --------------------------------------------------------------------------
// MetricsRegistryImpl — the actual singleton state
// --------------------------------------------------------------------------

/**
 * All mutable state for the MetricsRegistry singleton lives here so the
 * outer MetricsRegistry class can stay thin and the friend relationships
 * are contained.
 */
class MetricsRegistryImpl {
public:
    MetricsRegistryImpl() {
        METRICS_LOG_DEBUG("MetricsRegistryImpl constructed");
    }

    ~MetricsRegistryImpl() {
        METRICS_LOG_DEBUG("MetricsRegistryImpl destroyed");
    }

    MetricsRegistryImpl(const MetricsRegistryImpl&) = delete;
    MetricsRegistryImpl& operator=(const MetricsRegistryImpl&) = delete;

    // -- Broker metrics ------------------------------------------------------

    BrokerMetrics& broker() noexcept { return broker_metrics_; }
    const BrokerMetrics& broker() const noexcept { return broker_metrics_; }

    // -- Topic metrics -------------------------------------------------------

    /**
     * Get-or-create topic metrics for `name`.  Uses shared lock for read,
     * upgrades to exclusive lock on miss.
     */
    TopicMetrics& topic(const std::string& name) {
        // Fast path: shared lock
        {
            std::shared_lock lock(topic_mutex_);
            auto it = topic_metrics_.find(name);
            if (it != topic_metrics_.end()) {
                return it->second;
            }
        }
        // Slow path: exclusive lock to insert
        {
            std::unique_lock lock(topic_mutex_);
            auto [it, inserted] = topic_metrics_.try_emplace(name);
            if (inserted) {
                it->second.topic_name = name;
                METRICS_LOG_DEBUG("Created TopicMetrics for \"{}\"", name);
            }
            return it->second;
        }
    }

    /**
     * Return a snapshot of all topic names (for enumeration during scrape).
     */
    [[nodiscard]] std::vector<std::string> topic_names() const {
        std::shared_lock lock(topic_mutex_);
        std::vector<std::string> names;
        names.reserve(topic_metrics_.size());
        for (const auto& [name, _] : topic_metrics_) {
            names.push_back(name);
        }
        return names;
    }

    /**
     * Access a specific topic's metrics by name; returns nullptr if absent.
     */
    [[nodiscard]] const TopicMetrics* find_topic(const std::string& name) const {
        std::shared_lock lock(topic_mutex_);
        auto it = topic_metrics_.find(name);
        if (it != topic_metrics_.end()) {
            return &it->second;
        }
        return nullptr;
    }

    // -- Gauge registration --------------------------------------------------

    /**
     * Register or update a gauge metric.  Gauges represent values that can
     * go up and down (e.g., queue depth, connection count, memory usage).
     *
     * @param name  Unique metric name. Must not be empty.
     * @param value Pointer to the atomic that backs this gauge.
     * @param help  Optional help text for Prometheus exposition.
     * @return true if newly registered, false if updated.
     */
    bool register_gauge(const std::string& name, std::atomic<int64_t>* value,
                        const std::string& help = "") {
        if (name.empty()) {
            METRICS_LOG_WARN("Refusing to register gauge with empty name");
            return false;
        }
        std::unique_lock lock(gauge_mutex_);
        auto it = std::find_if(gauges_.begin(), gauges_.end(),
                               [&](const GaugeDescriptor& g) { return g.name == name; });
        if (it != gauges_.end()) {
            it->value = value;
            if (!help.empty()) it->help = help;
            METRICS_LOG_DEBUG("Updated gauge \"{}\"", name);
            return false;
        } else {
            gauges_.push_back({name, help, value});
            METRICS_LOG_DEBUG("Registered gauge \"{}\"", name);
            return true;
        }
    }

    /**
     * Unregister a previously registered gauge.  The underlying atomic
     * is not touched — the caller retains ownership.
     */
    void unregister_gauge(const std::string& name) {
        std::unique_lock lock(gauge_mutex_);
        auto it = std::remove_if(gauges_.begin(), gauges_.end(),
                                 [&](const GaugeDescriptor& g) { return g.name == name; });
        if (it != gauges_.end()) {
            METRICS_LOG_DEBUG("Unregistered gauge \"{}\"", name);
            gauges_.erase(it, gauges_.end());
        }
    }

    [[nodiscard]] size_t gauge_count() const {
        std::shared_lock lock(gauge_mutex_);
        return gauges_.size();
    }

    void register_counter(const std::string& name, std::atomic<uint64_t>* value,
                          const std::string& help = "") {
        if (name.empty()) {
            METRICS_LOG_WARN("Refusing to register counter with empty name");
            return;
        }
        std::unique_lock lock(counter_mutex_);
        auto it = std::find_if(counters_.begin(), counters_.end(),
                               [&](const CounterDescriptor& c) { return c.name == name; });
        if (it != counters_.end()) {
            it->value = value;
            if (!help.empty()) it->help = help;
            METRICS_LOG_DEBUG("Updated counter \"{}\"", name);
        } else {
            counters_.push_back({name, help, value});
            METRICS_LOG_DEBUG("Registered counter \"{}\"", name);
        }
    }

    void unregister_counter(const std::string& name) {
        std::unique_lock lock(counter_mutex_);
        auto it = std::remove_if(counters_.begin(), counters_.end(),
                                 [&](const CounterDescriptor& c) { return c.name == name; });
        if (it != counters_.end()) {
            METRICS_LOG_DEBUG("Unregistered counter \"{}\"", name);
            counters_.erase(it, counters_.end());
        }
    }

    [[nodiscard]] size_t counter_count() const {
        std::shared_lock lock(counter_mutex_);
        return counters_.size();
    }

    void register_histogram(const std::string& name,
                            std::atomic<uint64_t>* sum,
                            std::atomic<uint64_t>* count,
                            const std::string& help = "",
                            std::vector<double> buckets = {}) {
        if (name.empty()) {
            METRICS_LOG_WARN("Refusing to register histogram with empty name");
            return;
        }
        std::unique_lock lock(histogram_mutex_);
        auto it = std::find_if(histograms_.begin(), histograms_.end(),
                               [&](const HistogramDescriptor& h) { return h.name == name; });
        if (it != histograms_.end()) {
            it->sum = sum;
            it->count = count;
            if (!help.empty()) it->help = help;
            if (!buckets.empty()) {
                it->buckets = std::move(buckets);
                it->bucket_counts.resize(it->buckets.size());
            }
            METRICS_LOG_DEBUG("Updated histogram \"{}\"", name);
        } else {
            HistogramDescriptor hd;
            hd.name = name;
            hd.help = help;
            hd.sum = sum;
            hd.count = count;
            hd.buckets = std::move(buckets);
            hd.bucket_counts.resize(hd.buckets.size());
            histograms_.push_back(std::move(hd));
            METRICS_LOG_DEBUG("Registered histogram \"{}\"", name);
        }
    }

    void unregister_histogram(const std::string& name) {
        std::unique_lock lock(histogram_mutex_);
        auto it = std::remove_if(histograms_.begin(), histograms_.end(),
                                 [&](const HistogramDescriptor& h) { return h.name == name; });
        if (it != histograms_.end()) {
            METRICS_LOG_DEBUG("Unregistered histogram \"{}\"", name);
            histograms_.erase(it, histograms_.end());
        }
    }

    [[nodiscard]] size_t histogram_count() const {
        std::shared_lock lock(histogram_mutex_);
        return histograms_.size();
    }

    // -- Topic management extensions -----------------------------------------

    /**
     * Remove topic metrics for a given topic name.  Safe to call even if
     * the topic doesn't exist.
     */
    void remove_topic(const std::string& name) {
        std::unique_lock lock(topic_mutex_);
        auto it = topic_metrics_.find(name);
        if (it != topic_metrics_.end()) {
            METRICS_LOG_DEBUG("Removed TopicMetrics for \"{}\"", name);
            topic_metrics_.erase(it);
        }
    }

    /**
     * Check whether a topic has been tracked in the metrics registry.
     */
    [[nodiscard]] bool topic_exists(const std::string& name) const {
        std::shared_lock lock(topic_mutex_);
        return topic_metrics_.count(name) > 0;
    }

    [[nodiscard]] size_t topic_count() const {
        std::shared_lock lock(topic_mutex_);
        return topic_metrics_.size();
    }

    // -- Snapshot helpers (take copies under lock) ---------------------------

    [[nodiscard]] CustomMetricsSnapshot snapshot() const {
        CustomMetricsSnapshot snap;
        {
            std::shared_lock gl(gauge_mutex_);
            snap.gauges = gauges_;
        }
        {
            std::shared_lock cl(counter_mutex_);
            snap.counters = counters_;
        }
        {
            std::shared_lock hl(histogram_mutex_);
            snap.histograms = histograms_;
        }
        return snap;
    }

    // -- Debug dump ----------------------------------------------------------

    /**
     * Produce a human-readable summary of all registered metrics,
     * useful for debugging and admin diagnostics.
     */
    [[nodiscard]] std::string dump_summary() const {
        std::ostringstream oss;
        const auto& bm = broker_metrics_;

        oss << "=== BrokerMetrics ===\n";
        oss << "  messages_produced:         " << bm.messages_produced.load() << "\n";
        oss << "  messages_consumed:         " << bm.messages_consumed.load() << "\n";
        oss << "  bytes_produced:            " << bm.bytes_produced.load() << "\n";
        oss << "  bytes_consumed:            " << bm.bytes_consumed.load() << "\n";
        oss << "  active_connections:        " << bm.active_connections.load() << "\n";
        oss << "  total_connections:         " << bm.total_connections.load() << "\n";
        oss << "  requests_total:            " << bm.requests_total.load() << "\n";
        oss << "  requests_failed:           " << bm.requests_failed.load() << "\n";
        oss << "  leader_elections:          " << bm.leader_elections.load() << "\n";
        oss << "  disk_usage_bytes:          " << bm.disk_usage_bytes.load() << "\n";
        oss << "  under_replicated_partitions: " << bm.under_replicated_partitions.load() << "\n";

        {
            std::shared_lock lock(topic_mutex_);
            oss << "\n=== TopicMetrics (" << topic_metrics_.size() << " topics) ===\n";
            for (const auto& [name, tm] : topic_metrics_) {
                oss << "  [" << name << "]\n";
                oss << "    messages_in:  " << tm.messages_in.load() << "\n";
                oss << "    messages_out: " << tm.messages_out.load() << "\n";
                oss << "    bytes_in:     " << tm.bytes_in.load() << "\n";
                oss << "    bytes_out:    " << tm.bytes_out.load() << "\n";
                oss << "    size_bytes:   " << tm.size_bytes.load() << "\n";
            }
        }

        {
            std::shared_lock gl(gauge_mutex_);
            oss << "\n=== Custom Gauges (" << gauges_.size() << ") ===\n";
            for (const auto& g : gauges_) {
                oss << "  " << g.name << " = " << g.value->load() << "\n";
            }
        }

        {
            std::shared_lock cl(counter_mutex_);
            oss << "\n=== Custom Counters (" << counters_.size() << ") ===\n";
            for (const auto& c : counters_) {
                oss << "  " << c.name << " = " << c.value->load() << "\n";
            }
        }

        {
            std::shared_lock hl(histogram_mutex_);
            oss << "\n=== Custom Histograms (" << histograms_.size() << ") ===\n";
            for (const auto& h : histograms_) {
                oss << "  " << h.name << " sum=" << h.sum->load()
                    << " count=" << h.count->load() << "\n";
            }
        }

        return oss.str();
    }

private:
    // -- Built-in broker metrics ---------------------------------------------
    BrokerMetrics broker_metrics_;

    // -- Per-topic metrics ---------------------------------------------------
    mutable std::shared_mutex topic_mutex_;
    std::unordered_map<std::string, TopicMetrics> topic_metrics_;

    // -- Custom metric registries --------------------------------------------
    mutable std::shared_mutex gauge_mutex_;
    std::vector<GaugeDescriptor> gauges_;

    mutable std::shared_mutex counter_mutex_;
    std::vector<CounterDescriptor> counters_;

    mutable std::shared_mutex histogram_mutex_;
    std::vector<HistogramDescriptor> histograms_;
};

// --------------------------------------------------------------------------
// Singleton access
// --------------------------------------------------------------------------

[[nodiscard]] MetricsRegistryImpl& registry_impl() {
    static MetricsRegistryImpl impl;
    return impl;
}

} // anonymous namespace

// ============================================================================
// BrokerMetrics::reset()
// ============================================================================

void BrokerMetrics::reset() {
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
    METRICS_LOG_DEBUG("BrokerMetrics reset");
}

// ============================================================================
// MetricsRegistry — singleton + public API
// ============================================================================

MetricsRegistry& MetricsRegistry::instance() {
    static MetricsRegistry reg;
    return reg;
}

BrokerMetrics& MetricsRegistry::broker() {
    return registry_impl().broker();
}

TopicMetrics& MetricsRegistry::topic(const std::string& name) {
    return registry_impl().topic(name);
}

void MetricsRegistry::register_gauge(const std::string& name,
                                      std::atomic<int64_t>* value) {
    registry_impl().register_gauge(name, value);
}

void MetricsRegistry::register_counter(const std::string& name,
                                        std::atomic<uint64_t>* value) {
    registry_impl().register_counter(name, value);
}

void MetricsRegistry::register_histogram(const std::string& name,
                                          std::atomic<uint64_t>* sum,
                                          std::atomic<uint64_t>* count) {
    registry_impl().register_histogram(name, sum, count);
}

// ============================================================================
// Free functions for PrometheusExporter use (declared in metrics.h)
// ============================================================================

const BrokerMetrics& get_broker_metrics() {
    return registry_impl().broker();
}

std::vector<std::string> get_topic_names() {
    return registry_impl().topic_names();
}

const TopicMetrics* find_topic_metrics(const std::string& name) {
    return registry_impl().find_topic(name);
}

CustomMetricsSnapshot get_custom_metrics_snapshot() {
    return registry_impl().snapshot();
}

} // namespace torrent::metrics
