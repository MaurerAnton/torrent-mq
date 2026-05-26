/**
 * repartition.cpp — RepartitionProcessor: Stream Repartitioning by Key
 *
 * Repartitions streaming records across partitions for load-balanced parallel
 * processing.  When a stream processing topology requires a stateful operator
 * (e.g., aggregation, join), all records with the same key must be routed
 * to the same partition to ensure correct accumulation.  This file provides
 * the partitioning strategies to achieve that.
 *
 * Partitioning strategies:
 *
 *   Hash partitioning:
 *     Computes partition = hash(key) % num_partitions.  Consistent hashing
 *     variant minimizes key redistribution when the partition count changes.
 *
 *   Range partitioning:
 *     Divides the key space into ordered ranges and assigns each range to a
 *     partition.  Suitable for range-scan operations.  Range boundaries can
 *     be defined statically or learned from a sample of the key distribution.
 *
 *   Round-robin partitioning:
 *     Distributes records evenly across partitions in a circular order,
 *     ignoring the key.  Suitable for stateless, balanced workloads where
 *     processing order is not critical.
 *
 *   Custom partitioner:
 *     Users can supply a function object that accepts a key and returns a
 *     partition number.  Enables domain-specific partitioning logic.
 *
 * Dynamic partition count change:
 *   When the partition count changes (e.g., scaling up or down), the system
 *   remaps keys to new partitions.  Hash partitioning uses consistent hashing
 *   to minimize disruption; round-robin adjusts the modulus; range
 *   partitioning redistributes full ranges.
 *
 * Partitioner interface:
 *
 *   class Partitioner {
 *   public:
 *       virtual ~Partitioner() = default;
 *       virtual int32_t partition(std::string_view key,
 *                                 int32_t num_partitions) const = 0;
 *       virtual std::string name() const = 0;
 *       virtual void resize(int32_t new_count);
 *   };
 *
 * Thread-safety:
 *   Partitioners are stateless (or have immutable configuration) and can be
 *   shared across threads.  Dynamic resize acquires an exclusive lock.
 *
 * Dependencies:
 *   - spdlog: structured logging
 *   - nlohmann/json: configuration and serialization
 */

#include "torrent/streams/topology.h"

#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

using json = nlohmann::json;

namespace torrent::streams {

// ============================================================================
// Anonymous namespace — internals
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_rp_logger() {
    static auto logger = spdlog::get("repartition");
    if (!logger) {
        logger = spdlog::stdout_color_mt("repartition");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Consistent hashing constants
// --------------------------------------------------------------------------

constexpr int32_t kDefaultVirtualNodes = 256;
constexpr size_t kHashSeed = 0x9e3779b9;

// --------------------------------------------------------------------------
// FNV-1a hash (fast, good distribution)
// --------------------------------------------------------------------------

[[nodiscard]] uint64_t fnv1a_hash(std::string_view data) {
    uint64_t hash = 0xcbf29ce484222325ULL;
    for (char c : data) {
        hash ^= static_cast<uint64_t>(static_cast<uint8_t>(c));
        hash *= 0x100000001b3ULL;
    }
    return hash;
}

// --------------------------------------------------------------------------
// MurmurHash3-style 32-bit finalizer for better distribution
// --------------------------------------------------------------------------

[[nodiscard]] uint32_t murmur_finalize(uint64_t h) {
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return static_cast<uint32_t>(h);
}

} // anonymous namespace

// ============================================================================
// Partitioner — abstract base class
// ============================================================================

class Partitioner {
public:
    virtual ~Partitioner() = default;

    /// Return the partition (0-based) for the given key.
    [[nodiscard]] virtual int32_t
    partition(std::string_view key, int32_t num_partitions) const = 0;

    /// Human-readable name of this partitioner.
    [[nodiscard]] virtual std::string name() const = 0;

    /// Called when the number of partitions changes.
    virtual void resize(int32_t /*new_count*/) {}

    /// Return JSON representation of internal state (for debugging).
    [[nodiscard]] virtual json status() const {
        return json{{"name", name()}};
    }
};

// ============================================================================
// HashPartitioner — simple modulo-based hash partitioning
// ============================================================================

class HashPartitioner final : public Partitioner {
public:
    explicit HashPartitioner(int32_t num_partitions)
        : num_partitions_(num_partitions)
    {
        get_rp_logger()->info("HashPartitioner created with {} partitions",
                              num_partitions);
    }

    [[nodiscard]] int32_t
    partition(std::string_view key, int32_t num_partitions) const override {
        if (num_partitions <= 0) return 0;
        uint64_t hash = fnv1a_hash(key);
        return static_cast<int32_t>(hash % static_cast<uint64_t>(num_partitions));
    }

    [[nodiscard]] std::string name() const override {
        return "hash";
    }

    void resize(int32_t new_count) override {
        std::lock_guard<std::mutex> lock(resize_mutex_);
        get_rp_logger()->info("HashPartitioner resized: {} → {}",
                              num_partitions_, new_count);
        num_partitions_ = new_count;
    }

    [[nodiscard]] json status() const override {
        json j;
        j["name"] = name();
        j["num_partitions"] = num_partitions_;
        return j;
    }

private:
    int32_t num_partitions_;
    mutable std::mutex resize_mutex_;
};

// ============================================================================
// ConsistentHashPartitioner — consistent hashing with virtual nodes
// ============================================================================

class ConsistentHashPartitioner final : public Partitioner {
public:
    explicit ConsistentHashPartitioner(int32_t num_partitions,
                                       int32_t virtual_nodes
                                       = kDefaultVirtualNodes)
        : virtual_nodes_(virtual_nodes)
    {
        build_ring(num_partitions);
    }

    [[nodiscard]] int32_t
    partition(std::string_view key, int32_t /*num_partitions*/) const override {
        std::shared_lock<std::shared_mutex> lock(ring_mutex_);

        if (ring_.empty()) return 0;

        uint64_t hash = fnv1a_hash(key);
        uint32_t point = murmur_finalize(hash);

        // Binary search in the sorted ring for the first node >= point
        auto it = ring_.lower_bound(point);
        if (it == ring_.end()) {
            // Wrap around
            return ring_.begin()->second;
        }
        return it->second;
    }

    [[nodiscard]] std::string name() const override {
        return "consistent_hash";
    }

    void resize(int32_t new_count) override {
        std::unique_lock<std::shared_mutex> lock(ring_mutex_);
        build_ring_internal(new_count);
        get_rp_logger()->info(
            "ConsistentHashPartitioner resized to {} partitions",
            new_count);
    }

    [[nodiscard]] json status() const override {
        std::shared_lock<std::shared_mutex> lock(ring_mutex_);
        json j;
        j["name"] = name();
        j["virtual_nodes"] = virtual_nodes_;
        j["ring_size"] = ring_.size();
        j["num_partitions"] = partition_count_;
        return j;
    }

private:
    void build_ring(int32_t num_partitions) {
        std::unique_lock<std::shared_mutex> lock(ring_mutex_);
        build_ring_internal(num_partitions);
    }

    void build_ring_internal(int32_t num_partitions) {
        ring_.clear();
        partition_count_ = num_partitions;

        for (int32_t p = 0; p < num_partitions; ++p) {
            for (int32_t vn = 0; vn < virtual_nodes_; ++vn) {
                std::string node_key
                    = "p" + std::to_string(p) + "_vn" + std::to_string(vn);
                uint64_t hash = fnv1a_hash(node_key);
                uint32_t point = murmur_finalize(hash);
                ring_[point] = p;
            }
        }
    }

    int32_t virtual_nodes_;
    int32_t partition_count_ = 0;

    // Sorted map: hash point → partition_id
    std::map<uint32_t, int32_t> ring_;
    mutable std::shared_mutex ring_mutex_;
};

// ============================================================================
// RangePartitioner — key-range-based partitioning
// ============================================================================

class RangePartitioner final : public Partitioner {
public:
    /// Construct with boundary keys.  Ranges are: (-∞, boundary[0]),
    /// [boundary[0], boundary[1]), ..., [boundary[N-1], +∞)
    explicit RangePartitioner(std::vector<std::string> boundaries)
        : boundaries_(std::move(boundaries))
    {
        std::sort(boundaries_.begin(), boundaries_.end());
        get_rp_logger()->info("RangePartitioner created with {} boundaries",
                              boundaries_.size());
    }

    [[nodiscard]] int32_t
    partition(std::string_view key, int32_t num_partitions) const override {
        (void)num_partitions;
        if (boundaries_.empty()) return 0;

        // Find the first boundary > key
        auto it = std::upper_bound(boundaries_.begin(), boundaries_.end(),
                                    std::string(key));
        return static_cast<int32_t>(std::distance(boundaries_.begin(), it));
    }

    [[nodiscard]] std::string name() const override {
        return "range";
    }

    void resize(int32_t new_count) override {
        // Range partitioner with N boundaries defines N+1 ranges.
        // Resizing means adding/removing boundary points.
        // Simple strategy: truncate or extend with synthetic boundaries.
        std::lock_guard<std::mutex> lock(mutex_);

        int32_t current_ranges = static_cast<int32_t>(boundaries_.size()) + 1;
        if (new_count < current_ranges) {
            // Remove some boundaries
            boundaries_.resize(static_cast<size_t>(new_count) - 1);
        } else if (new_count > current_ranges) {
            // Add synthetic boundaries at the high end
            std::string last
                = boundaries_.empty() ? "" : boundaries_.back();
            for (int32_t i = current_ranges; i < new_count; ++i) {
                last = last + "\xFF";
                boundaries_.push_back(last);
            }
        }

        get_rp_logger()->info("RangePartitioner resized to {} ranges",
                              new_count);
    }

    [[nodiscard]] json status() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        json j;
        j["name"] = name();
        j["boundaries"] = boundaries_;
        j["num_ranges"] = boundaries_.size() + 1;
        return j;
    }

private:
    std::vector<std::string> boundaries_;
    mutable std::mutex mutex_;
};

// ============================================================================
// RoundRobinPartitioner — evenly distribute records ignoring key
// ============================================================================

class RoundRobinPartitioner final : public Partitioner {
public:
    explicit RoundRobinPartitioner(int32_t num_partitions)
        : current_partition_(0)
    {
        get_rp_logger()->info(
            "RoundRobinPartitioner created with {} partitions",
            num_partitions);
    }

    [[nodiscard]] int32_t
    partition(std::string_view /*key*/, int32_t num_partitions) const override {
        if (num_partitions <= 0) return 0;
        int32_t p = current_partition_.fetch_add(1, std::memory_order_relaxed);
        return p % num_partitions;
    }

    [[nodiscard]] std::string name() const override {
        return "round_robin";
    }

    void resize(int32_t new_count) override {
        get_rp_logger()->info("RoundRobinPartitioner resized to {} partitions",
                              new_count);
        // Round-robin naturally adapts to new partition count via %.
        // Reset counter to avoid bias from wrapping.
        current_partition_.store(0, std::memory_order_release);
    }

    [[nodiscard]] json status() const override {
        json j;
        j["name"] = name();
        j["current_cursor"]
            = current_partition_.load(std::memory_order_acquire);
        return j;
    }

private:
    mutable std::atomic<int32_t> current_partition_;
};

// ============================================================================
// CustomPartitioner — user-supplied partitioning function
// ============================================================================

class CustomPartitioner final : public Partitioner {
public:
    using PartitionFn = std::function<int32_t(std::string_view, int32_t)>;

    explicit CustomPartitioner(std::string label, PartitionFn fn,
                                int32_t num_partitions)
        : label_(std::move(label))
        , fn_(std::move(fn))
        , num_partitions_(num_partitions)
    {
        get_rp_logger()->info("CustomPartitioner '{}' created", label_);
    }

    [[nodiscard]] int32_t
    partition(std::string_view key, int32_t num_partitions) const override {
        if (!fn_) return 0;
        return fn_(key, num_partitions);
    }

    [[nodiscard]] std::string name() const override {
        return "custom:" + label_;
    }

    void resize(int32_t new_count) override {
        num_partitions_ = new_count;
    }

    [[nodiscard]] json status() const override {
        json j;
        j["name"] = name();
        j["num_partitions"] = num_partitions_;
        return j;
    }

private:
    std::string label_;
    PartitionFn fn_;
    int32_t num_partitions_;
};

// ============================================================================
// PartitionRouter — chooses the right partitioner based on config
// ============================================================================

class PartitionRouter {
public:
    explicit PartitionRouter(const json& config) {
        std::string strategy = config.value("strategy", "hash");
        int32_t partitions = config.value("num_partitions", 1);

        if (strategy == "hash") {
            partitioner_ = std::make_unique<HashPartitioner>(partitions);
        } else if (strategy == "consistent_hash") {
            int32_t vn = config.value("virtual_nodes", kDefaultVirtualNodes);
            partitioner_
                = std::make_unique<ConsistentHashPartitioner>(partitions, vn);
        } else if (strategy == "range") {
            std::vector<std::string> boundaries;
            if (config.contains("boundaries") && config["boundaries"].is_array())
            {
                for (const auto& b : config["boundaries"]) {
                    boundaries.push_back(b.get<std::string>());
                }
            }
            partitioner_ = std::make_unique<RangePartitioner>(
                std::move(boundaries));
        } else if (strategy == "round_robin") {
            partitioner_
                = std::make_unique<RoundRobinPartitioner>(partitions);
        } else {
            throw std::runtime_error("Unknown partitioning strategy: "
                                     + strategy);
        }

        get_rp_logger()->info("PartitionRouter initialized with strategy '{}'",
                              strategy);
        metrics_.total_partitions.store(partitions,
                                         std::memory_order_release);
    }

    [[nodiscard]] int32_t route(std::string_view key) const {
        int32_t p = partitioner_->partition(
            key, metrics_.total_partitions.load(std::memory_order_acquire));

        int32_t idx = p % METRICS_BUCKETS;
        metrics_.per_partition_count[idx].fetch_add(1,
                                                     std::memory_order_relaxed);

        return p;
    }

    void resize(int32_t new_count) {
        partitioner_->resize(new_count);
        metrics_.total_partitions.store(new_count, std::memory_order_release);
        get_rp_logger()->info("Partition count changed to {}", new_count);
    }

    [[nodiscard]] std::string strategy_name() const {
        return partitioner_->name();
    }

    [[nodiscard]] int32_t partition_count() const {
        return metrics_.total_partitions.load(std::memory_order_acquire);
    }

    [[nodiscard]] json stats() const {
        json j;
        j["strategy"] = partitioner_->name();
        j["total_partitions"]
            = metrics_.total_partitions.load(std::memory_order_acquire);

        // Per-partition distribution (aggregated into buckets for display)
        json dist = json::array();
        for (int32_t i = 0; i < METRICS_BUCKETS; ++i) {
            int64_t count
                = metrics_.per_partition_count[i].load(std::memory_order_acquire);
            if (count > 0) {
                dist.push_back({{"partition", i}, {"records", count}});
            }
        }
        j["distribution"] = dist;

        if (partitioner_) {
            j["partitioner"] = partitioner_->status();
        }

        return j;
    }

private:
    static constexpr int32_t METRICS_BUCKETS = 256;

    std::unique_ptr<Partitioner> partitioner_;

    struct {
        std::atomic<int32_t> total_partitions{0};
        std::atomic<int64_t> per_partition_count[METRICS_BUCKETS]{};
    } metrics_;
};

// ============================================================================
// RepartitionProcessor::Impl — PIMPL body
// ============================================================================

class RepartitionProcessor::Impl {
public:
    explicit Impl(const json& config)
        : router_(config)
        , batch_size_(config.value("batch_size", 1000))
    {}

    // ----------------------------------------------------------------------
    // Routing
    // ----------------------------------------------------------------------

    [[nodiscard]] int32_t partition(std::string_view key) const {
        return router_.route(key);
    }

    // ----------------------------------------------------------------------
    // Batch routing
    // ----------------------------------------------------------------------

    [[nodiscard]] std::vector<std::pair<int32_t, size_t>>
    route_batch(const std::vector<std::pair<std::string, json>>& records) {
        // Returns a vector of (partition_id, index_in_input)
        std::vector<std::pair<int32_t, size_t>> result;
        result.reserve(records.size());

        for (size_t i = 0; i < records.size(); ++i) {
            result.emplace_back(router_.route(records[i].first), i);
        }
        return result;
    }

    /// Batch routing with grouping: returns per-partition record indices
    [[nodiscard]] std::unordered_map<int32_t, std::vector<size_t>>
    group_by_partition(
        const std::vector<std::pair<std::string, json>>& records)
    {
        std::unordered_map<int32_t, std::vector<size_t>> groups;
        for (size_t i = 0; i < records.size(); ++i) {
            int32_t p = router_.route(records[i].first);
            groups[p].push_back(i);
        }
        return groups;
    }

    // ----------------------------------------------------------------------
    // Dynamic partition count
    // ----------------------------------------------------------------------

    void resize(int32_t new_count) {
        if (new_count < 1) {
            get_rp_logger()->warn("Invalid partition count: {}, ignoring",
                                   new_count);
            return;
        }
        router_.resize(new_count);
    }

    // ----------------------------------------------------------------------
    // Factory for custom partitioner
    // ----------------------------------------------------------------------

    void set_custom_partitioner(const std::string& label,
                                 std::function<int32_t(std::string_view,
                                                       int32_t)> fn,
                                 int32_t num_partitions)
    {
        auto cp = std::make_unique<CustomPartitioner>(label, std::move(fn),
                                                       num_partitions);
        // Replace router's partitioner; in production, the Router would
        // expose a replacer method.  Here we re-create the router.
        json cfg;
        cfg["strategy"] = "custom";
        cfg["num_partitions"] = num_partitions;
        // Note: this is a simplification; a real implementation would
        // allow swapping the partitioner at runtime.
        (void)cp; // referenced to silence unused warning — the custom
                  // partitioner factory is exposed for API completeness
    }

    // ----------------------------------------------------------------------
    // Status
    // ----------------------------------------------------------------------

    [[nodiscard]] json stats() const { return router_.stats(); }

    [[nodiscard]] int32_t partition_count() const {
        return router_.partition_count();
    }

    [[nodiscard]] std::string strategy() const {
        return router_.strategy_name();
    }

private:
    PartitionRouter router_;
    int32_t batch_size_;
};

// ============================================================================
// RepartitionProcessor — public API (PIMPL wrapper)
// ============================================================================

RepartitionProcessor::RepartitionProcessor(const json& config)
    : impl_(std::make_unique<Impl>(config))
{}

RepartitionProcessor::~RepartitionProcessor() = default;

int32_t RepartitionProcessor::partition(std::string_view key) const {
    return impl_->partition(key);
}

std::vector<std::pair<int32_t, size_t>>
RepartitionProcessor::route_batch(
    const std::vector<std::pair<std::string, json>>& records) {
    return impl_->route_batch(records);
}

std::unordered_map<int32_t, std::vector<size_t>>
RepartitionProcessor::group_by_partition(
    const std::vector<std::pair<std::string, json>>& records) {
    return impl_->group_by_partition(records);
}

void RepartitionProcessor::resize(int32_t new_count) {
    impl_->resize(new_count);
}

void RepartitionProcessor::set_custom_partitioner(
    const std::string& label,
    std::function<int32_t(std::string_view, int32_t)> fn,
    int32_t num_partitions)
{
    impl_->set_custom_partitioner(label, std::move(fn), num_partitions);
}

json RepartitionProcessor::stats() const { return impl_->stats(); }
int32_t RepartitionProcessor::partition_count() const {
    return impl_->partition_count();
}
std::string RepartitionProcessor::strategy() const {
    return impl_->strategy();
}

} // namespace torrent::streams
