#ifndef TORRENT_COMMON_METRICS_INTERNAL_H
#define TORRENT_COMMON_METRICS_INTERNAL_H
// Internal header only
#endif
#include <atomic>
#include <chrono>
#include <cmath>
#include <deque>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <spdlog/spdlog.h>

namespace torrent::metrics {
namespace {

// ============================================================================
// Gauge: point-in-time value
// ============================================================================
template<typename T>
class Gauge {
public:
    void set(T val) { value_.store(val, std::memory_order_release); }
    T get() const { return value_.load(std::memory_order_acquire); }
    void inc(T delta = 1) { value_.fetch_add(delta); }
    void dec(T delta = 1) { value_.fetch_sub(delta); }
private:
    std::atomic<T> value_{0};
};

// ============================================================================
// Counter: monotonically increasing
// ============================================================================
class Counter {
public:
    void inc(int64_t delta = 1) { value_.fetch_add(delta, std::memory_order_release); }
    int64_t get() const { return value_.load(std::memory_order_acquire); }
    void reset() { value_.store(0); }
private:
    std::atomic<int64_t> value_{0};
};

// ============================================================================
// EWMA (Exponentially Weighted Moving Average)
// ============================================================================
class Ewma {
public:
    explicit Ewma(double alpha) : alpha_(alpha) {}
    void update(int64_t value) {
        double current = rate_.load();
        double next = alpha_ * value + (1.0 - alpha_) * current;
        rate_.store(next);
        last_update_ = std::chrono::steady_clock::now();
    }
    double rate() const {
        auto elapsed = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - last_update_).count();
        if (elapsed > 5.0 * 60.0) return 0.0;
        return rate_.load() * (1.0 / std::max(1.0, elapsed));
    }
    void tick() { update(0); }
private:
    double alpha_;
    std::atomic<double> rate_{0.0};
    std::chrono::steady_clock::time_point last_update_{std::chrono::steady_clock::now()};
};

// ============================================================================
// Meter: 1-minute, 5-minute, 15-minute EWMA rates
// ============================================================================
class Meter {
public:
    Meter() : m1_(std::exp(-5.0 / 60.0)), m5_(std::exp(-5.0 / 300.0)), m15_(std::exp(-5.0 / 900.0)) {}
    void mark(int64_t n = 1) { count_.fetch_add(n); m1_.update(n); m5_.update(n); m15_.update(n); }
    int64_t count() const { return count_.load(); }
    double m1_rate() const { return m1_.rate(); }
    double m5_rate() const { return m5_.rate(); }
    double m15_rate() const { return m15_.rate(); }
private:
    std::atomic<int64_t> count_{0};
    Ewma m1_, m5_, m15_;
};

// ============================================================================
// Reservoir Sampling for Percentiles
// ============================================================================
class UniformReservoir {
    static constexpr int kSize = 1028;
public:
    void update(int64_t value) {
        count_.fetch_add(1);
        std::lock_guard<std::mutex> lock(mutex_);
        if (size_ < kSize) { reservoir_[size_++] = value; }
        else { int r = rand() % count_.load(); if (r < kSize) reservoir_[r] = value; }
    }
    int64_t percentile(double p) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (size_ == 0) return 0;
        std::vector<int64_t> sorted(reservoir_, reservoir_ + size_);
        std::sort(sorted.begin(), sorted.end());
        return sorted[static_cast<size_t>(p * (size_ - 1))];
    }
    int64_t max() const { return percentile(1.0); }
    int64_t min() const { return percentile(0.0); }
    double mean() const { return count_.load() > 0 ? static_cast<double>(sum_.load()) / count_.load() : 0.0; }
    int64_t count() const { return count_.load(); }
    int64_t sum() const { return sum_.load(); }
private:
    std::atomic<int64_t> count_{0}, sum_{0};
    mutable std::mutex mutex_;
    int64_t reservoir_[kSize]{};
    int size_{0};
};

// ============================================================================
// Timer: meters rate + tracks distribution
// ============================================================================
class Timer {
public:
    struct Context { std::chrono::steady_clock::time_point start; Timer* timer; ~Context() { timer->update(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start).count()); } };
    Context time() { return {std::chrono::steady_clock::now(), this}; }
    void update(int64_t us) { meter_.mark(); reservoir_.update(us); }
    int64_t count() const { return meter_.count(); }
    double m1_rate() const { return meter_.m1_rate(); }
    int64_t p50() const { return reservoir_.percentile(0.50); }
    int64_t p95() const { return reservoir_.percentile(0.95); }
    int64_t p99() const { return reservoir_.percentile(0.99); }
    int64_t p999() const { return reservoir_.percentile(0.999); }
    int64_t max() const { return reservoir_.max(); }
    double mean() const { return reservoir_.mean(); }
private:
    Meter meter_;
    UniformReservoir reservoir_;
};

// ============================================================================
// Composite Metrics Registry
// ============================================================================
class MetricsRegistryImpl {
public:
    static MetricsRegistryImpl& instance() { static MetricsRegistryImpl impl; return impl; }

    void register_gauge(const std::string& name, std::atomic<int64_t>* ptr) { gauges_[name] = ptr; }
    void register_counter(const std::string& name, std::atomic<uint64_t>* ptr) { counters_[name] = ptr; }
    void register_timer(const std::string& name, Timer* timer) { timers_[name] = timer; }

    Timer* get_timer(const std::string& name) {
        auto it = timers_.find(name);
        return it != timers_.end() ? it->second : nullptr;
    }

    std::string prometheus_format() const {
        std::ostringstream oss;
        for (const auto& [name, ptr] : counters_) {
            oss << "# HELP " << name << " Counter metric\n";
            oss << "# TYPE " << name << " counter\n";
            oss << name << " " << ptr->load() << "\n";
        }
        for (const auto& [name, ptr] : gauges_) {
            oss << "# HELP " << name << " Gauge metric\n";
            oss << "# TYPE " << name << " gauge\n";
            oss << name << " " << ptr->load() << "\n";
        }
        for (const auto& [name, timer] : timers_) {
            oss << "# HELP " << name << " Timer metric\n";
            oss << "# TYPE " << name << " summary\n";
            oss << name << "{quantile=\"0.5\"} " << timer->p50() << "\n";
            oss << name << "{quantile=\"0.95\"} " << timer->p95() << "\n";
            oss << name << "{quantile=\"0.99\"} " << timer->p99() << "\n";
            oss << name << "_count " << timer->count() << "\n";
        }
        return oss.str();
    }

private:
    MetricsRegistryImpl() = default;
    std::unordered_map<std::string, std::atomic<int64_t>*> gauges_;
    std::unordered_map<std::string, std::atomic<uint64_t>*> counters_;
    std::unordered_map<std::string, Timer*> timers_;
};

} // anonymous namespace
} // namespace torrent::metrics
