/**
 * bench_producer.cpp — Producer Throughput Benchmarks
 *
 * Measures producer throughput across:
 *   - Batch sizes: 1, 10, 100, 1000, 10000
 *   - Compression types: none, lz4, zstd, snappy, gzip
 *   - Acknowledgement settings: acks=0, acks=1, acks=-1
 *   - Single partition vs multi-partition fan-out
 *
 * Uses google/benchmark framework. Each benchmark reports
 * messages/sec and bytes/sec via custom counters.
 *
 * Build: included in torrent_benchmark target (CMakeLists.txt).
 * Run:   ./torrent_benchmark --benchmark_filter=BM_Producer
 */

#include <benchmark/benchmark.h>

#include "torrent/common/types.h"
#include "torrent/storage/types.h"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// ============================================================================
// Fake producer / broker infrastructure (no real I/O or network)
// ============================================================================

namespace {

/// Simulated message payload size (bytes).
constexpr size_t kPayloadSize = 512;

/// Simulated number of partitions.
constexpr int32_t kMaxPartitions = 16;

/// Generate a deterministic payload of the given size.
std::string make_payload(size_t size, uint32_t seed) {
    std::string s(size, 'X');
    for (size_t i = 0; i < size && i < 32; i++) {
        s[i] = static_cast<char>('A' + ((seed + i) % 26));
    }
    return s;
}

/// Minimal simulated RecordBatch for benchmarking.
struct SimBatch {
    std::string data;
    torrent::compression_type compression = torrent::compression_type::none;
    int32_t record_count = 0;
};

/// Fake producer that "sends" batches to an in-memory counter.
class SimProducer {
public:
    explicit SimProducer(torrent::required_acks acks)
        : acks_(acks), total_messages_(0), total_bytes_(0) {}

    /// Simulate sending a batch — store it and increment counters.
    void send(SimBatch batch) {
        std::lock_guard<std::mutex> lock(mutex_);
        total_messages_ += batch.record_count;
        total_bytes_ += static_cast<int64_t>(batch.data.size());
        // Acks simulation: acks=0 returns immediately, acks=1/= -1
        // do a tiny bit of work to simulate replication overhead.
        if (acks_ != torrent::required_acks::none) {
            // Simulate fsync / replication delay with a small spin
            for (volatile int i = 0; i < (acks_ == torrent::required_acks::all_isr ? 100 : 20); i++) {}
        }
        batches_.push_back(std::move(batch));
    }

    [[nodiscard]] int64_t total_messages() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return total_messages_;
    }

    [[nodiscard]] int64_t total_bytes() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return total_bytes_;
    }

    void reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        total_messages_ = 0;
        total_bytes_ = 0;
        batches_.clear();
    }

private:
    torrent::required_acks acks_;
    mutable std::mutex mutex_;
    std::vector<SimBatch> batches_;
    int64_t total_messages_;
    int64_t total_bytes_;
};

/// Fake compression (approximates real cost for benchmarking).
SimBatch compress_batch(const std::string& raw,
                        torrent::compression_type ct,
                        int32_t count) {
    SimBatch b;
    b.record_count = count;
    b.compression = ct;

    switch (ct) {
    case torrent::compression_type::none:
        b.data = raw;
        break;
    case torrent::compression_type::lz4:
        // LZ4 is fast — simulate ~70% compression ratio
        b.data = raw.substr(0, raw.size() * 7 / 10);
        break;
    case torrent::compression_type::zstd:
        // Zstd is strong — simulate ~50% compression ratio
        b.data = raw.substr(0, raw.size() / 2);
        // Add a bit of CPU cost (zstd is heavier)
        for (volatile int i = 0; i < 50; i++) {}
        break;
    case torrent::compression_type::snappy:
        // Snappy is fast — simulate ~75% compression ratio
        b.data = raw.substr(0, raw.size() * 3 / 4);
        break;
    case torrent::compression_type::gzip:
        // Gzip is strongest but slowest — simulate ~40% ratio
        b.data = raw.substr(0, raw.size() * 4 / 10);
        for (volatile int i = 0; i < 200; i++) {}
        break;
    }
    return b;
}

} // anonymous namespace

// ============================================================================
// Benchmark: Batch Size Scaling
// ============================================================================

/// Run a producer benchmark with the given batch size.
static void bench_producer_batch_size(benchmark::State& state,
                                       int32_t batch_size,
                                       torrent::required_acks acks,
                                       torrent::compression_type comp) {
    SimProducer producer(acks);
    std::mt19937 rng(42);
    int64_t messages_sent = 0;
    int64_t bytes_sent = 0;

    for (auto _ : state) {
        // Build one batch
        std::string raw;
        raw.reserve(batch_size * kPayloadSize);
        for (int32_t i = 0; i < batch_size; i++) {
            raw += make_payload(kPayloadSize, rng());
        }
        SimBatch batch = compress_batch(raw, comp, batch_size);
        producer.send(std::move(batch));

        messages_sent += batch_size;
        bytes_sent += static_cast<int64_t>(raw.size());
    }

    state.counters["msgs/sec"] = benchmark::Counter(
        static_cast<double>(messages_sent),
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
    state.counters["bytes/sec"] = benchmark::Counter(
        static_cast<double>(bytes_sent),
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
    state.counters["batch_size"] = static_cast<double>(batch_size);
}

// ============================================================================
// Batch-size variants (acks=1, no compression)
// ============================================================================

static void BM_Producer_Batch_1(benchmark::State& state) {
    bench_producer_batch_size(state, 1,
        torrent::required_acks::leader,
        torrent::compression_type::none);
}
BENCHMARK(BM_Producer_Batch_1);

static void BM_Producer_Batch_10(benchmark::State& state) {
    bench_producer_batch_size(state, 10,
        torrent::required_acks::leader,
        torrent::compression_type::none);
}
BENCHMARK(BM_Producer_Batch_10);

static void BM_Producer_Batch_100(benchmark::State& state) {
    bench_producer_batch_size(state, 100,
        torrent::required_acks::leader,
        torrent::compression_type::none);
}
BENCHMARK(BM_Producer_Batch_100);

static void BM_Producer_Batch_1000(benchmark::State& state) {
    bench_producer_batch_size(state, 1000,
        torrent::required_acks::leader,
        torrent::compression_type::none);
}
BENCHMARK(BM_Producer_Batch_1000);

static void BM_Producer_Batch_10000(benchmark::State& state) {
    bench_producer_batch_size(state, 10000,
        torrent::required_acks::leader,
        torrent::compression_type::none);
}
BENCHMARK(BM_Producer_Batch_10000);

// ============================================================================
// Compression-type variants (batch=1000, acks=1)
// ============================================================================

static void BM_Producer_Compress_None(benchmark::State& state) {
    bench_producer_batch_size(state, 1000,
        torrent::required_acks::leader,
        torrent::compression_type::none);
}
BENCHMARK(BM_Producer_Compress_None);

static void BM_Producer_Compress_LZ4(benchmark::State& state) {
    bench_producer_batch_size(state, 1000,
        torrent::required_acks::leader,
        torrent::compression_type::lz4);
}
BENCHMARK(BM_Producer_Compress_LZ4);

static void BM_Producer_Compress_Zstd(benchmark::State& state) {
    bench_producer_batch_size(state, 1000,
        torrent::required_acks::leader,
        torrent::compression_type::zstd);
}
BENCHMARK(BM_Producer_Compress_Zstd);

static void BM_Producer_Compress_Snappy(benchmark::State& state) {
    bench_producer_batch_size(state, 1000,
        torrent::required_acks::leader,
        torrent::compression_type::snappy);
}
BENCHMARK(BM_Producer_Compress_Snappy);

static void BM_Producer_Compress_Gzip(benchmark::State& state) {
    bench_producer_batch_size(state, 1000,
        torrent::required_acks::leader,
        torrent::compression_type::gzip);
}
BENCHMARK(BM_Producer_Compress_Gzip);

// ============================================================================
// Acks variants (batch=1000, no compression)
// ============================================================================

static void BM_Producer_Acks_0(benchmark::State& state) {
    bench_producer_batch_size(state, 1000,
        torrent::required_acks::none,
        torrent::compression_type::none);
}
BENCHMARK(BM_Producer_Acks_0);

static void BM_Producer_Acks_1(benchmark::State& state) {
    bench_producer_batch_size(state, 1000,
        torrent::required_acks::leader,
        torrent::compression_type::none);
}
BENCHMARK(BM_Producer_Acks_1);

static void BM_Producer_Acks_Minus1(benchmark::State& state) {
    bench_producer_batch_size(state, 1000,
        torrent::required_acks::all_isr,
        torrent::compression_type::none);
}
BENCHMARK(BM_Producer_Acks_Minus1);

// ============================================================================
// Multi-partition fan-out benchmark
// ============================================================================

/// Simulate sending to N partitions in a round-robin fashion.
static void BM_Producer_MultiPartition(benchmark::State& state) {
    const int32_t num_partitions = static_cast<int32_t>(state.range(0));
    const int32_t batch_size = 1000;

    std::vector<SimProducer> producers;
    producers.reserve(static_cast<size_t>(num_partitions));
    for (int32_t i = 0; i < num_partitions; i++) {
        producers.emplace_back(torrent::required_acks::leader);
    }

    std::mt19937 rng(42);
    int32_t part = 0;
    int64_t messages_sent = 0;
    int64_t bytes_sent = 0;

    for (auto _ : state) {
        std::string raw;
        raw.reserve(batch_size * kPayloadSize);
        for (int32_t i = 0; i < batch_size; i++) {
            raw += make_payload(kPayloadSize, rng());
        }
        SimBatch batch = compress_batch(raw,
            torrent::compression_type::none, batch_size);
        producers[static_cast<size_t>(part)].send(std::move(batch));

        messages_sent += batch_size;
        bytes_sent += static_cast<int64_t>(raw.size());
        part = (part + 1) % num_partitions;
    }

    state.counters["partitions"] = static_cast<double>(num_partitions);
    state.counters["msgs/sec"] = benchmark::Counter(
        static_cast<double>(messages_sent),
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
    state.counters["bytes/sec"] = benchmark::Counter(
        static_cast<double>(bytes_sent),
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}
BENCHMARK(BM_Producer_MultiPartition)
    ->Arg(1)
    ->Arg(2)
    ->Arg(4)
    ->Arg(8)
    ->Arg(16);

// ============================================================================
// Full pipeline benchmark (batch=1000, acks=1, lz4 compression, multi-part)
// ============================================================================

static void BM_Producer_FullPipeline(benchmark::State& state) {
    const int32_t num_partitions = 8;
    const int32_t batch_size = 1000;

    std::vector<SimProducer> producers;
    for (int32_t i = 0; i < num_partitions; i++) {
        producers.emplace_back(torrent::required_acks::all_isr);
    }

    std::mt19937 rng(99);
    int32_t part = 0;

    for (auto _ : state) {
        std::string raw;
        raw.reserve(batch_size * kPayloadSize);
        for (int32_t i = 0; i < batch_size; i++) {
            raw += make_payload(kPayloadSize, rng());
        }
        SimBatch batch = compress_batch(raw,
            torrent::compression_type::lz4, batch_size);
        producers[static_cast<size_t>(part)].send(std::move(batch));
        part = (part + 1) % num_partitions;
    }

    // Aggregate
    int64_t total_msgs = 0;
    int64_t total_bytes = 0;
    for (auto& p : producers) {
        total_msgs += p.total_messages();
        total_bytes += p.total_bytes();
    }
    state.counters["total_msgs"] = static_cast<double>(total_msgs);
    state.counters["total_mb"] = static_cast<double>(total_bytes) / 1'000'000.0;
}
BENCHMARK(BM_Producer_FullPipeline);

BENCHMARK_MAIN();
