/**
 * bench_end_to_end.cpp — End-to-End Latency & Throughput Benchmarks
 *
 * Comprehensive end-to-end benchmark suite measuring:
 *
 *   Latency:
 *     - Single partition produce→consume latency (p50, p95, p99, p99.9)
 *     - Raft log replication latency
 *     - Leader election time
 *
 *   Throughput:
 *     - Multi-partition throughput scaling (1, 2, 4, 8, 16 partitions)
 *     - Throughput at different payload sizes
 *
 *   Consumer Groups:
 *     - Consumer group rebalance time (stop-the-world duration)
 *     - Multi-consumer group fan-out overhead
 *
 *   Storage:
 *     - Compaction overhead (CPU and I/O cost)
 *     - Tiered storage upload/download time
 *     - Snapshot creation time at different sizes
 *
 *   Recovery:
 *     - Startup/recovery time at different segment counts
 *     - Crash recovery with dirty shutdown
 *
 * Uses google/benchmark framework. All metrics exposed via custom counters.
 * Simulated infrastructure — no real network or disk I/O.
 *
 * Build: included in torrent_benchmark target (CMakeLists.txt).
 * Run:   ./torrent_benchmark --benchmark_filter=BM_EndToEnd
 *
 * NOTE: For real deployment numbers, run against an actual torrent-mq cluster
 * and replace the simulated backends with real implementations.
 */

#include <benchmark/benchmark.h>

#include "torrent/common/types.h"
#include "torrent/storage/types.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <chrono>
#include <cmath>
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

// ============================================================================
// Benchmark infrastructure — simulated broker, producer, consumer
// ============================================================================

namespace {

/// Default test directory.
const std::string kBenchDir = "/tmp/torrent_bench_e2e";

/// Payload sizes tested.
constexpr size_t kPayloadSizes[] = {128, 512, 1024, 4096, 16384};

/// Partition counts tested.
constexpr int32_t kPartitionCounts[] = {1, 2, 4, 8, 16, 32};

/// Segment counts tested for recovery.
constexpr int64_t kRecoverySegmentCounts[] = {10, 100, 500, 1000, 5000};

/// Snapshot sizes tested (bytes).
constexpr int64_t kSnapshotSizes[] = {
    1LL * 1024 * 1024,       // 1 MB
    10LL * 1024 * 1024,      // 10 MB
    100LL * 1024 * 1024,     // 100 MB
    1LL * 1024 * 1024 * 1024 // 1 GB
};

/// Maximum number of latency samples to retain for percentile calculation.
constexpr size_t kMaxLatencySamples = 100'000;

/// Default message count for throughput benchmarks.
constexpr int64_t kDefaultThroughputMessages = 500'000;

// --------------------------------------------------------------------------
// Latency histogram for percentile calculation
// --------------------------------------------------------------------------

class LatencyHistogram {
public:
    void record(int64_t us) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (samples_.size() < kMaxLatencySamples) {
            samples_.push_back(us);
        }
        count_.fetch_add(1);
    }

    [[nodiscard]] int64_t p50() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (samples_.empty()) return 0;
        return percentile(50.0);
    }

    [[nodiscard]] int64_t p95() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (samples_.empty()) return 0;
        return percentile(95.0);
    }

    [[nodiscard]] int64_t p99() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (samples_.empty()) return 0;
        return percentile(99.0);
    }

    [[nodiscard]] int64_t p999() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (samples_.empty()) return 0;
        return percentile(99.9);
    }

    [[nodiscard]] int64_t min() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (samples_.empty()) return 0;
        return *std::min_element(samples_.begin(), samples_.end());
    }

    [[nodiscard]] int64_t max() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (samples_.empty()) return 0;
        return *std::max_element(samples_.begin(), samples_.end());
    }

    [[nodiscard]] double mean() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (samples_.empty()) return 0.0;
        double sum = std::accumulate(samples_.begin(), samples_.end(), 0.0);
        return sum / static_cast<double>(samples_.size());
    }

    [[nodiscard]] int64_t count() const { return count_.load(); }

    void reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        samples_.clear();
        count_.store(0);
    }

private:
    [[nodiscard]] int64_t percentile(double pct) const {
        // Assumes mutex is held
        auto sorted = samples_;
        std::sort(sorted.begin(), sorted.end());
        double idx = (pct / 100.0) * static_cast<double>(sorted.size() - 1);
        size_t lo = static_cast<size_t>(std::floor(idx));
        size_t hi = static_cast<size_t>(std::ceil(idx));
        if (lo == hi) return sorted[lo];
        double frac = idx - static_cast<double>(lo);
        return static_cast<int64_t>(
            sorted[lo] * (1.0 - frac) + sorted[hi] * frac);
    }

    mutable std::mutex mutex_;
    std::vector<int64_t> samples_;
    std::atomic<int64_t> count_{0};
};

// --------------------------------------------------------------------------
// Generate deterministic payload
// --------------------------------------------------------------------------

[[nodiscard]] std::string make_payload(size_t size, uint64_t seed) {
    std::string s(size, 'X');
    for (size_t i = 0; i < size && i < 64; i++) {
        s[i] = static_cast<char>('a' + static_cast<int>((seed + i) % 26));
    }
    return s;
}

// --------------------------------------------------------------------------
// Simulated message store (in-memory per partition)
// --------------------------------------------------------------------------

struct SimMessage {
    std::string key;
    std::string value;
    int64_t     offset      = 0;
    int64_t     produce_us  = 0;  // timestamp when produced
    int64_t     consume_us  = 0;  // timestamp when consumed
};

class SimPartitionLog {
public:
    explicit SimPartitionLog(int32_t partition_id)
        : partition_id_(partition_id) {}

    [[nodiscard]] int64_t append(std::string key, std::string value, int64_t timestamp_us) {
        std::lock_guard<std::mutex> lock(mutex_);
        SimMessage msg;
        msg.key        = std::move(key);
        msg.value      = std::move(value);
        msg.offset     = next_offset_++;
        msg.produce_us = timestamp_us;
        messages_.push_back(std::move(msg));
        return messages_.back().offset;
    }

    [[nodiscard]] std::vector<SimMessage> consume(offset_t start_offset, int64_t max_messages) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<SimMessage> result;
        auto idx = static_cast<size_t>(start_offset);
        while (idx < messages_.size() && static_cast<int64_t>(result.size()) < max_messages) {
            auto msg = messages_[idx];
            msg.consume_us = now_us();
            result.push_back(msg);
            idx++;
        }
        return result;
    }

    [[nodiscard]] offset_t high_watermark() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return next_offset_.load() - 1;
    }

    [[nodiscard]] int64_t message_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return static_cast<int64_t>(messages_.size());
    }

    void reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        messages_.clear();
        next_offset_.store(0);
    }

    void preload(int64_t count, size_t payload_size) {
        std::lock_guard<std::mutex> lock(mutex_);
        messages_.reserve(static_cast<size_t>(count));
        for (int64_t i = 0; i < count; i++) {
            SimMessage msg;
            msg.key    = "key-" + std::to_string(i);
            msg.value  = make_payload(payload_size, static_cast<uint64_t>(i));
            msg.offset = i;
            msg.produce_us = now_us();
            messages_.push_back(std::move(msg));
        }
        next_offset_.store(count);
    }

private:
    [[nodiscard]] static int64_t now_us() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()).count();
    }

    int32_t partition_id_;
    mutable std::mutex mutex_;
    std::vector<SimMessage> messages_;
    std::atomic<offset_t> next_offset_{0};
};

// --------------------------------------------------------------------------
// Simulated broker with partitions and consumer groups
// --------------------------------------------------------------------------

class SimBroker {
public:
    explicit SimBroker(int32_t num_partitions, size_t payload_size = 512)
        : num_partitions_(num_partitions), payload_size_(payload_size) {
        for (int32_t i = 0; i < num_partitions_; i++) {
            partitions_.emplace_back(std::make_shared<SimPartitionLog>(i));
        }
    }

    [[nodiscard]] int64_t produce(int32_t partition, const std::string& key,
                                    const std::string& value, int64_t ts_us = 0) {
        if (ts_us <= 0) ts_us = now_us();
        // Simulate replication overhead with small CPU spin
        for (volatile int i = 0; i < 10; i++) {}
        return partitions_[static_cast<size_t>(partition)]->append(key, value, ts_us);
    }

    [[nodiscard]] std::vector<SimMessage> consume(int32_t partition,
                                                    offset_t start_offset,
                                                    int64_t max_messages) {
        // Simulate fetch overhead
        for (volatile int i = 0; i < 5; i++) {}
        return partitions_[static_cast<size_t>(partition)]->consume(start_offset, max_messages);
    }

    [[nodiscard]] int32_t partition_count() const { return num_partitions_; }

    [[nodiscard]] offset_t high_watermark(int32_t partition) const {
        return partitions_[static_cast<size_t>(partition)]->high_watermark();
    }

    void preload(int64_t messages_per_partition) {
        for (auto& p : partitions_) {
            p->preload(messages_per_partition, payload_size_);
        }
    }

    void reset() {
        for (auto& p : partitions_) p->reset();
    }

private:
    [[nodiscard]] static int64_t now_us() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()).count();
    }

    int32_t num_partitions_;
    size_t payload_size_;
    std::vector<std::shared_ptr<SimPartitionLog>> partitions_;
};

// --------------------------------------------------------------------------
// Simulated consumer group
// --------------------------------------------------------------------------

class SimConsumerGroup {
public:
    struct Member {
        int32_t id;
        int64_t start_offset = 0;
        int64_t messages_consumed = 0;
        LatencyHistogram latency;
    };

    explicit SimConsumerGroup(int32_t num_members)
        : num_members_(num_members) {
        for (int32_t i = 0; i < num_members; i++) {
            members_.push_back({i, 0, 0, {}});
        }
    }

    void consume_from(SimBroker& broker, int32_t partition,
                      int64_t batch_size, LatencyHistogram* global_latency = nullptr) {
        for (auto& member : members_) {
            auto msgs = broker.consume(partition, member.start_offset, batch_size);
            for (const auto& msg : msgs) {
                int64_t latency_us = now_us() - msg.produce_us;
                member.latency.record(latency_us);
                if (global_latency) global_latency->record(latency_us);
                member.start_offset = msg.offset + 1;
                member.messages_consumed++;
            }
        }
    }

    [[nodiscard]] int64_t total_consumed() const {
        int64_t total = 0;
        for (const auto& m : members_) total += m.messages_consumed;
        return total;
    }

    void reset() {
        for (auto& m : members_) {
            m.start_offset = 0;
            m.messages_consumed = 0;
            m.latency.reset();
        }
    }

private:
    [[nodiscard]] static int64_t now_us() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()).count();
    }

    int32_t num_members_;
    std::vector<Member> members_;
};

// --------------------------------------------------------------------------
// Simulated compaction (key-based deduplication)
// --------------------------------------------------------------------------

struct CompactionResult {
    int64_t records_before = 0;
    int64_t records_after  = 0;
    int64_t elapsed_us     = 0;
    int64_t bytes_before   = 0;
    int64_t bytes_after    = 0;
};

[[nodiscard]] CompactionResult simulate_compaction(
    int64_t num_records, int32_t unique_keys_pct) {
    CompactionResult result;
    result.records_before = num_records;
    result.bytes_before   = num_records * static_cast<int64_t>(kPayloadSizes[2]);

    // Simulate compaction work
    auto start = std::chrono::high_resolution_clock::now();

    // Simulate scanning and deduplication
    int64_t unique = (num_records * unique_keys_pct) / 100;
    for (volatile int64_t i = 0; i < num_records / 100; i++) {}
    // Simulate writing compacted segments
    for (volatile int64_t i = 0; i < unique / 100; i++) {}

    auto end = std::chrono::high_resolution_clock::now();
    result.elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
        end - start).count();
    result.records_after = unique;
    result.bytes_after   = unique * static_cast<int64_t>(kPayloadSizes[2]);

    return result;
}

// --------------------------------------------------------------------------
// Simulated snapshot creation
// --------------------------------------------------------------------------

struct SnapshotResult {
    int64_t snapshot_size_bytes = 0;
    int64_t elapsed_us          = 0;
    int64_t segments_snapshotted = 0;
};

[[nodiscard]] SnapshotResult simulate_snapshot(int64_t total_size_bytes) {
    SnapshotResult result;
    result.snapshot_size_bytes = total_size_bytes;
    result.segments_snapshotted = total_size_bytes / (64 * 1024 * 1024);  // 64 MB segments

    auto start = std::chrono::high_resolution_clock::now();

    // Simulate: scan segments, write snapshot file, fsync
    int64_t work_units = total_size_bytes / (1024 * 1024);  // per MB
    for (volatile int64_t i = 0; i < work_units && i < 10000; i++) {}

    auto end = std::chrono::high_resolution_clock::now();
    result.elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
        end - start).count();
    return result;
}

// --------------------------------------------------------------------------
// Simulated recovery
// --------------------------------------------------------------------------

struct RecoveryResult {
    int64_t segment_count   = 0;
    int64_t elapsed_us      = 0;
    int64_t total_records   = 0;
};

[[nodiscard]] RecoveryResult simulate_recovery(int64_t segment_count,
                                                  int64_t records_per_segment = 10000) {
    RecoveryResult result;
    result.segment_count = segment_count;
    result.total_records = segment_count * records_per_segment;

    auto start = std::chrono::high_resolution_clock::now();

    // Simulate: scan segment files, rebuild offset indexes, replay unflushed data
    for (volatile int64_t i = 0; i < segment_count && i < 100000; i++) {
        // Simulate index rebuild per segment
        for (volatile int j = 0; j < 10; j++) {}
    }

    // Simulate crash recovery (replay last segment's unflushed writes)
    if (segment_count > 0) {
        for (volatile int j = 0; j < 50; j++) {}
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
        end - start).count();
    return result;
}

// --------------------------------------------------------------------------
// Simulated leader election
// --------------------------------------------------------------------------

[[nodiscard]] int64_t simulate_leader_election(int32_t cluster_size) {
    auto start = std::chrono::high_resolution_clock::now();

    // Simulate: election timeout + voting round + leader announcement
    // Base election timeout ~150ms + voting ~50ms per node
    int64_t base_delay = 150000;  // 150ms in us
    int64_t voting     = static_cast<int64_t>(cluster_size) * 5000; // 5ms per node

    for (volatile int64_t i = 0; i < (base_delay + voting) / 100; i++) {}

    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

// --------------------------------------------------------------------------
// Simulated Raft replication
// --------------------------------------------------------------------------

[[nodiscard]] int64_t simulate_raft_replication(int32_t replica_count) {
    auto start = std::chrono::high_resolution_clock::now();

    // Simulate: append to leader + replicate to followers + await quorum acks
    int64_t delay = 100 + static_cast<int64_t>(replica_count) * 50;  // us
    for (volatile int64_t i = 0; i < delay; i++) {}

    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

// --------------------------------------------------------------------------
// Simulated tiered storage upload/download
// --------------------------------------------------------------------------

struct TieredStorageResult {
    int64_t upload_elapsed_us   = 0;
    int64_t download_elapsed_us = 0;
    int64_t data_size_bytes     = 0;
};

[[nodiscard]] TieredStorageResult simulate_tiered_storage(int64_t data_size_bytes) {
    TieredStorageResult result;
    result.data_size_bytes = data_size_bytes;

    // Upload
    auto start = std::chrono::high_resolution_clock::now();
    int64_t mb = data_size_bytes / (1024 * 1024);
    for (volatile int64_t i = 0; i < mb && i < 100000; i++) {}
    auto mid = std::chrono::high_resolution_clock::now();
    result.upload_elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
        mid - start).count();

    // Download
    for (volatile int64_t i = 0; i < mb / 2 && i < 100000; i++) {}
    auto end = std::chrono::high_resolution_clock::now();
    result.download_elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
        end - mid).count();

    return result;
}

// --------------------------------------------------------------------------
// Simulated consumer group rebalance
// --------------------------------------------------------------------------

[[nodiscard]] int64_t simulate_rebalance(int32_t num_consumers, int32_t num_partitions) {
    auto start = std::chrono::high_resolution_clock::now();

    // Simulate: coordinator receives join group requests → assignment → sync group
    int64_t work = static_cast<int64_t>(num_consumers) * num_partitions;
    for (volatile int64_t i = 0; i < work && i < 100000; i++) {}

    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

} // anonymous namespace

// ============================================================================
// Benchmark: Single Partition Latency
// ============================================================================

/// Measures produce→consume latency for a single partition.
/// Reports p50, p95, p99, p99.9 in microseconds.
static void BM_EndToEnd_SinglePartitionLatency(benchmark::State& state) {
    const int32_t num_partitions = 1;
    const int64_t messages_per_batch = 100;
    const int64_t total_messages = 50000;

    SimBroker broker(num_partitions, 512);
    SimConsumerGroup consumer(1);
    LatencyHistogram global_latency;
    std::mt19937 rng(42);
    int64_t produced = 0;
    int64_t consumed = 0;

    for (auto _ : state) {
        state.PauseTiming();
        broker.reset();
        consumer.reset();
        global_latency.reset();
        produced = 0;
        consumed = 0;
        state.ResumeTiming();

        // Produce and consume in interleaved batches
        while (produced < total_messages) {
            // Produce batch
            for (int64_t i = 0; i < messages_per_batch && produced < total_messages; i++) {
                std::string key = "key-" + std::to_string(produced);
                std::string val = make_payload(512, rng());
                broker.produce(0, key, val);
                produced++;
            }

            // Consume batch
            consumer.consume_from(broker, 0, messages_per_batch, &global_latency);
            consumed = consumer.total_consumed();
        }

        // Drain remaining
        while (consumed < produced) {
            consumer.consume_from(broker, 0, messages_per_batch, &global_latency);
            consumed = consumer.total_consumed();
        }
    }

    state.counters["p50_us"] = static_cast<double>(global_latency.p50());
    state.counters["p95_us"] = static_cast<double>(global_latency.p95());
    state.counters["p99_us"] = static_cast<double>(global_latency.p99());
    state.counters["p99.9_us"] = static_cast<double>(global_latency.p999());
    state.counters["min_us"] = static_cast<double>(global_latency.min());
    state.counters["max_us"] = static_cast<double>(global_latency.max());
    state.counters["mean_us"] = global_latency.mean();
    state.counters["messages"] = static_cast<double>(produced);
}
BENCHMARK(BM_EndToEnd_SinglePartitionLatency);

// ============================================================================
// Benchmark: Multi-Partition Throughput Scaling
// ============================================================================

/// Measures throughput as partition count scales.
/// Reports messages/sec and MB/sec.
static void BM_EndToEnd_MultiPartitionThroughput(benchmark::State& state) {
    const int32_t num_partitions = static_cast<int32_t>(state.range(0));
    const size_t payload_size = 512;
    const int64_t batch_size = 500;

    SimBroker broker(num_partitions, payload_size);
    SimConsumerGroup consumer(num_partitions);
    std::mt19937 rng(42);
    int64_t produced = 0;

    for (auto _ : state) {
        // Produce to multiple partitions round-robin
        for (int32_t p = 0; p < num_partitions; p++) {
            for (int64_t i = 0; i < batch_size; i++) {
                std::string key = "p" + std::to_string(p) + "-" + std::to_string(i);
                std::string val = make_payload(payload_size, rng());
                broker.produce(p, key, val);
                produced++;
            }
        }
    }

    int64_t total_bytes = static_cast<int64_t>(produced * (payload_size + 32));  // +key overhead
    state.counters["msgs/sec"] = benchmark::Counter(
        static_cast<double>(produced),
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
    state.counters["mb/sec"] = benchmark::Counter(
        static_cast<double>(total_bytes) / 1'000'000.0,
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
    state.counters["partitions"] = static_cast<double>(num_partitions);
}
BENCHMARK(BM_EndToEnd_MultiPartitionThroughput)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(32);

// ============================================================================
// Benchmark: Payload Size Impact on Throughput
// ============================================================================

static void BM_EndToEnd_PayloadSizeThroughput(benchmark::State& state) {
    const size_t payload_size = static_cast<size_t>(state.range(0));
    const int32_t num_partitions = 8;
    const int64_t batch_size = 500;

    SimBroker broker(num_partitions, payload_size);
    std::mt19937 rng(42);
    int64_t produced = 0;
    int32_t part = 0;

    for (auto _ : state) {
        for (int64_t i = 0; i < batch_size; i++) {
            std::string key = "key-" + std::to_string(produced);
            std::string val = make_payload(payload_size, rng());
            broker.produce(part, key, val);
            produced++;
            part = (part + 1) % num_partitions;
        }
    }

    int64_t total_bytes = static_cast<int64_t>(produced * payload_size);
    state.counters["msgs/sec"] = benchmark::Counter(
        static_cast<double>(produced),
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
    state.counters["mb/sec"] = benchmark::Counter(
        static_cast<double>(total_bytes) / 1'000'000.0,
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
    state.counters["payload_bytes"] = static_cast<double>(payload_size);
}
BENCHMARK(BM_EndToEnd_PayloadSizeThroughput)
    ->Arg(128)->Arg(512)->Arg(1024)->Arg(4096)->Arg(16384);

// ============================================================================
// Benchmark: Consumer Group Rebalance Time
// ============================================================================

/// Measures stop-the-world duration during rebalance.
/// Varies consumer count against a fixed partition count.
static void BM_EndToEnd_RebalanceTime(benchmark::State& state) {
    const int32_t num_consumers = static_cast<int32_t>(state.range(0));
    const int32_t num_partitions = 12;

    for (auto _ : state) {
        int64_t rebalance_us = simulate_rebalance(num_consumers, num_partitions);
        benchmark::DoNotOptimize(rebalance_us);
        state.SetIterationTime(static_cast<double>(rebalance_us) / 1'000'000.0);
    }

    state.counters["consumers"] = static_cast<double>(num_consumers);
    state.counters["partitions"] = static_cast<double>(num_partitions);
}
BENCHMARK(BM_EndToEnd_RebalanceTime)
    ->Arg(1)->Arg(3)->Arg(5)->Arg(10)->Arg(20)->Arg(50)
    ->UseManualTime();

// ============================================================================
// Benchmark: Compaction Overhead
// ============================================================================

/// Measures compaction CPU and IO cost at different record counts.
static void BM_EndToEnd_CompactionOverhead(benchmark::State& state) {
    const int64_t num_records = static_cast<int64_t>(state.range(0));
    const int32_t unique_pct = 30;  // 30% unique keys

    CompactionResult total;
    total.records_before = num_records;

    for (auto _ : state) {
        auto result = simulate_compaction(num_records, unique_pct);
        total.records_after = result.records_after;
        total.elapsed_us += result.elapsed_us;
        total.bytes_before = result.bytes_before;
        total.bytes_after = result.bytes_after;
    }

    state.counters["records_before"] = static_cast<double>(total.records_before);
    state.counters["records_after"]  = static_cast<double>(total.records_after);
    state.counters["unique_pct"]     = static_cast<double>(unique_pct);
    state.counters["compaction_us"]  = static_cast<double>(total.elapsed_us / state.iterations());
}
BENCHMARK(BM_EndToEnd_CompactionOverhead)
    ->Arg(10000)->Arg(100000)->Arg(500000);

// ============================================================================
// Benchmark: Snapshot Creation Time
// ============================================================================

/// Measures time to create snapshots at different total data sizes.
static void BM_EndToEnd_SnapshotTime(benchmark::State& state) {
    const int64_t total_size = static_cast<int64_t>(state.range(0));

    for (auto _ : state) {
        auto snap = simulate_snapshot(total_size);
        benchmark::DoNotOptimize(snap.elapsed_us);
        state.SetIterationTime(static_cast<double>(snap.elapsed_us) / 1'000'000.0);
    }

    state.counters["snapshot_mb"] = static_cast<double>(total_size) / 1'000'000.0;
}
BENCHMARK(BM_EndToEnd_SnapshotTime)
    ->Arg(1LL * 1024 * 1024)
    ->Arg(10LL * 1024 * 1024)
    ->Arg(100LL * 1024 * 1024)
    ->Arg(1LL * 1024 * 1024 * 1024)
    ->UseManualTime();

// ============================================================================
// Benchmark: Tiered Storage Upload/Download
// ============================================================================

/// Measures time to upload to and download from tiered storage (S3).
static void BM_EndToEnd_TieredStorageUploadDownload(benchmark::State& state) {
    const int64_t data_size = static_cast<int64_t>(state.range(0));

    for (auto _ : state) {
        auto ts = simulate_tiered_storage(data_size);
        benchmark::DoNotOptimize(ts.upload_elapsed_us);
        benchmark::DoNotOptimize(ts.download_elapsed_us);
        state.SetIterationTime(
            static_cast<double>(ts.upload_elapsed_us + ts.download_elapsed_us) / 1'000'000.0);
    }

    state.counters["data_mb"] = static_cast<double>(data_size) / 1'000'000.0;
}
BENCHMARK(BM_EndToEnd_TieredStorageUploadDownload)
    ->Arg(10LL * 1024 * 1024)
    ->Arg(64LL * 1024 * 1024)
    ->Arg(256LL * 1024 * 1024)
    ->Arg(1LL * 1024 * 1024 * 1024)
    ->UseManualTime();

// ============================================================================
// Benchmark: Raft Log Replication Latency
// ============================================================================

/// Measures Raft replication latency with varying replica counts.
static void BM_EndToEnd_RaftReplication(benchmark::State& state) {
    const int32_t replica_count = static_cast<int32_t>(state.range(0));
    int64_t total_latency_us = 0;

    for (auto _ : state) {
        int64_t lat = simulate_raft_replication(replica_count);
        total_latency_us += lat;
        benchmark::DoNotOptimize(lat);
    }

    state.counters["replicas"] = static_cast<double>(replica_count);
    state.counters["avg_latency_us"] = static_cast<double>(
        total_latency_us / state.iterations());
}
BENCHMARK(BM_EndToEnd_RaftReplication)
    ->Arg(1)->Arg(3)->Arg(5)->Arg(7);

// ============================================================================
// Benchmark: Leader Election Time
// ============================================================================

/// Measures leader election time for varying cluster sizes.
static void BM_EndToEnd_LeaderElection(benchmark::State& state) {
    const int32_t cluster_size = static_cast<int32_t>(state.range(0));
    int64_t total_latency_us = 0;

    for (auto _ : state) {
        int64_t lat = simulate_leader_election(cluster_size);
        total_latency_us += lat;
        benchmark::DoNotOptimize(lat);
        state.SetIterationTime(static_cast<double>(lat) / 1'000'000.0);
    }

    state.counters["cluster_size"] = static_cast<double>(cluster_size);
    state.counters["avg_election_ms"] = static_cast<double>(
        total_latency_us / state.iterations()) / 1000.0;
}
BENCHMARK(BM_EndToEnd_LeaderElection)
    ->Arg(3)->Arg(5)->Arg(7)->Arg(11)
    ->UseManualTime();

// ============================================================================
// Benchmark: Startup/Recovery Time
// ============================================================================

/// Measures recovery time at different segment counts.
static void BM_EndToEnd_RecoveryTime(benchmark::State& state) {
    const int64_t segment_count = static_cast<int64_t>(state.range(0));

    for (auto _ : state) {
        auto recovery = simulate_recovery(segment_count);
        benchmark::DoNotOptimize(recovery.elapsed_us);
        state.SetIterationTime(static_cast<double>(recovery.elapsed_us) / 1'000'000.0);
    }

    state.counters["segments"] = static_cast<double>(segment_count);
}
BENCHMARK(BM_EndToEnd_RecoveryTime)
    ->Arg(10)->Arg(100)->Arg(500)->Arg(1000)->Arg(5000)
    ->UseManualTime();

// ============================================================================
// Benchmark: Full Pipeline — Produce + Multi-Consumer Group
// ============================================================================

/// End-to-end pipeline: produce to N partitions, consume from M consumer groups.
static void BM_EndToEnd_FullPipeline(benchmark::State& state) {
    const int32_t num_partitions = 4;
    const int32_t num_groups = 2;
    const size_t payload_size = 512;
    const int64_t batch_size = 100;

    SimBroker broker(num_partitions, payload_size);
    broker.preload(5000);  // pre-seed with some data

    std::vector<SimConsumerGroup> groups;
    for (int32_t g = 0; g < num_groups; g++) {
        groups.emplace_back(3);  // 3 consumers per group
    }

    std::mt19937 rng(42);
    int32_t part = 0;
    int64_t produced = 0;
    int64_t consumed = 0;

    for (auto _ : state) {
        state.PauseTiming();
        // Produce a batch
        for (int64_t i = 0; i < batch_size; i++) {
            std::string key = "key-" + std::to_string(produced);
            std::string val = make_payload(payload_size, rng());
            broker.produce(part, key, val);
            produced++;
            part = (part + 1) % num_partitions;
        }
        state.ResumeTiming();

        // All groups consume
        for (int32_t p = 0; p < num_partitions; p++) {
            for (auto& group : groups) {
                group.consume_from(broker, p, 50);
            }
        }
    }

    // Aggregate stats
    int64_t total_group_consumed = 0;
    for (auto& group : groups) {
        total_group_consumed += group.total_consumed();
    }
    state.counters["partitions"] = static_cast<double>(num_partitions);
    state.counters["consumer_groups"] = static_cast<double>(num_groups);
    state.counters["msgs_produced"] = static_cast<double>(produced);
    state.counters["msgs_consumed_total"] = static_cast<double>(total_group_consumed);
}
BENCHMARK(BM_EndToEnd_FullPipeline);

// ============================================================================
// Benchmark: Multi-Consumer Group Rebalance Time
// ============================================================================

/// Measures total rebalance time across multiple consumer groups.
static void BM_EndToEnd_MultiGroupRebalance(benchmark::State& state) {
    const int32_t num_groups = static_cast<int32_t>(state.range(0));
    const int32_t partitions_per_group = 12;
    const int32_t consumers_per_group = 3;

    for (auto _ : state) {
        int64_t total_time = 0;
        for (int32_t g = 0; g < num_groups; g++) {
            total_time += simulate_rebalance(consumers_per_group, partitions_per_group);
        }
        benchmark::DoNotOptimize(total_time);
        state.SetIterationTime(static_cast<double>(total_time) / 1'000'000.0);
    }

    state.counters["groups"] = static_cast<double>(num_groups);
}
BENCHMARK(BM_EndToEnd_MultiGroupRebalance)
    ->Arg(1)->Arg(5)->Arg(10)->Arg(25)
    ->UseManualTime();

BENCHMARK_MAIN();
