/**
 * bench_storage.cpp — Storage Engine IOPS & Latency Benchmarks
 *
 * Benchmarks the storage subsystem across:
 *   - Segment append throughput (sequential writes)
 *   - Segment read throughput (sequential and random)
 *   - Segment compaction time (key-based deduplication)
 *   - Compression type impact on write/read
 *   - mmap vs fstream I/O paths
 *   - Segment size scaling (64 KB → 1 GB)
 *
 * Uses google/benchmark. Reports IOPS and latency percentiles
 * (p50/p95/p99) via custom counters.
 *
 * NOTE: These benchmarks use real disk I/O in a temporary directory.
 * Run with --benchmark_min_time=2 for stable results.
 */

#include <benchmark/benchmark.h>

#include "torrent/common/types.h"
#include "torrent/storage/types.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// POSIX for mmap
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;

// ============================================================================
// Benchmark infrastructure — fake on-disk segment
// ============================================================================

namespace {

/// Default temp directory for storage benchmarks.
const std::string kBenchDir = "/tmp/torrent_bench_storage";

/// Record sizes tested.
constexpr size_t kSmallRecord  = 128;   // bytes
constexpr size_t kMediumRecord = 1024;
constexpr size_t kLargeRecord  = 8192;

/// Segment sizes tested (in bytes).
constexpr int64_t kSegSizes[] = {
    64LL * 1024,           // 64 KiB
    1LL * 1024 * 1024,     // 1 MiB
    16LL * 1024 * 1024,    // 16 MiB
    64LL * 1024 * 1024,    // 64 MiB
    256LL * 1024 * 1024,   // 256 MiB
};

/// Build a deterministic record payload.
std::string record_payload(size_t size, uint64_t seed) {
    std::string s(size, 'X');
    for (size_t i = 0; i < size && i < 64; i++) {
        s[i] = static_cast<char>('a' + static_cast<int>((seed + i) % 26));
    }
    return s;
}

/// Simulated compression (no real libs needed for benchmark cost model).
struct FakeCompressor {
    static std::string compress(const std::string& input,
                                 torrent::compression_type ct) {
        switch (ct) {
        case torrent::compression_type::none:
            return input;
        case torrent::compression_type::lz4:
            // Fast, ~70% ratio — truncate and add modest CPU burn
            for (volatile int i = 0; i < 20; i++) {}
            return input.substr(0, input.size() * 7 / 10);
        case torrent::compression_type::zstd:
            for (volatile int i = 0; i < 80; i++) {}
            return input.substr(0, input.size() / 2);
        case torrent::compression_type::snappy:
            for (volatile int i = 0; i < 15; i++) {}
            return input.substr(0, input.size() * 3 / 4);
        case torrent::compression_type::gzip:
            for (volatile int i = 0; i < 300; i++) {}
            return input.substr(0, input.size() * 4 / 10);
        }
        return input;
    }
};

/// Simple latency collector for percentiles.
class LatencyCollector {
public:
    void record(double us) {
        std::lock_guard<std::mutex> lock(mutex_);
        samples_.push_back(us);
    }

    [[nodiscard]] double percentile(double pct) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (samples_.empty()) return 0.0;
        auto sorted = samples_;
        std::sort(sorted.begin(), sorted.end());
        size_t idx = static_cast<size_t>(pct / 100.0 * static_cast<double>(sorted.size() - 1));
        if (idx >= sorted.size()) idx = sorted.size() - 1;
        return sorted[idx];
    }

    [[nodiscard]] size_t count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return samples_.size();
    }

    void reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        samples_.clear();
    }

private:
    mutable std::mutex mutex_;
    std::vector<double> samples_;
};

/// RAII temporary file in kBenchDir.
class TempFile {
public:
    explicit TempFile(const std::string& suffix = ".seg")
        : path_(kBenchDir + "/bench_" +
                std::to_string(counter_++) + suffix)
    {
        fs::create_directories(kBenchDir);
    }

    ~TempFile() {
        close();
        std::remove(path_.c_str());
    }

    TempFile(const TempFile&) = delete;
    TempFile& operator=(const TempFile&) = delete;

    void open_write() {
        fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_ < 0) {
            std::cerr << "Failed to open " << path_ << ": " << strerror(errno) << "\n";
            std::abort();
        }
    }

    void open_read() {
        fd_ = ::open(path_.c_str(), O_RDONLY);
        if (fd_ < 0) {
            std::cerr << "Failed to open " << path_ << ": " << strerror(errno) << "\n";
            std::abort();
        }
    }

    void close() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

    [[nodiscard]] int fd() const noexcept { return fd_; }
    [[nodiscard]] const std::string& path() const noexcept { return path_; }

private:
    std::string path_;
    int fd_ = -1;
    static std::atomic<int64_t> counter_;
};

std::atomic<int64_t> TempFile::counter_{0};

} // anonymous namespace

// ============================================================================
// Benchmark: Sequential Append Throughput
// ============================================================================

/// Write `total_bytes` of records to a temp file, measuring throughput.
static void bench_append_throughput(benchmark::State& state,
                                     size_t record_size,
                                     torrent::compression_type comp) {
    int64_t total_written = 0;
    LatencyCollector latencies;

    for (auto _ : state) {
        TempFile tf;
        tf.open_write();

        std::string raw = record_payload(record_size, 42);
        std::string compressed = FakeCompressor::compress(raw, comp);

        // Write one record
        auto start = std::chrono::high_resolution_clock::now();
        ssize_t written = ::pwrite(tf.fd(), compressed.data(),
                                    compressed.size(), total_written % (64LL * 1024 * 1024));
        auto end = std::chrono::high_resolution_clock::now();
        if (written < 0) {
            state.SkipWithError("pwrite failed");
            return;
        }

        total_written += static_cast<int64_t>(written);
        latencies.record(
            static_cast<double>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end - start).count()));
    }

    state.counters["IOPS"] = benchmark::Counter(
        static_cast<double>(state.iterations()),
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
    state.counters["MB/sec"] = benchmark::Counter(
        static_cast<double>(total_written) / 1e6,
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
    state.counters["record_bytes"] = static_cast<double>(record_size);
    state.counters["p50_us"] = latencies.percentile(50.0);
    state.counters["p95_us"] = latencies.percentile(95.0);
    state.counters["p99_us"] = latencies.percentile(99.0);
}

// ============================================================================
// Sequential Append benchmarks
// ============================================================================

static void BM_Storage_Append_Small(benchmark::State& state) {
    bench_append_throughput(state, kSmallRecord,
        torrent::compression_type::none);
}
BENCHMARK(BM_Storage_Append_Small);

static void BM_Storage_Append_Medium(benchmark::State& state) {
    bench_append_throughput(state, kMediumRecord,
        torrent::compression_type::none);
}
BENCHMARK(BM_Storage_Append_Medium);

static void BM_Storage_Append_Large(benchmark::State& state) {
    bench_append_throughput(state, kLargeRecord,
        torrent::compression_type::none);
}
BENCHMARK(BM_Storage_Append_Large);

// ============================================================================
// Benchmarks: Append with different compression types
// ============================================================================

static void BM_Storage_Append_LZ4(benchmark::State& state) {
    bench_append_throughput(state, kMediumRecord,
        torrent::compression_type::lz4);
}
BENCHMARK(BM_Storage_Append_LZ4);

static void BM_Storage_Append_Zstd(benchmark::State& state) {
    bench_append_throughput(state, kMediumRecord,
        torrent::compression_type::zstd);
}
BENCHMARK(BM_Storage_Append_Zstd);

static void BM_Storage_Append_Snappy(benchmark::State& state) {
    bench_append_throughput(state, kMediumRecord,
        torrent::compression_type::snappy);
}
BENCHMARK(BM_Storage_Append_Snappy);

static void BM_Storage_Append_Gzip(benchmark::State& state) {
    bench_append_throughput(state, kMediumRecord,
        torrent::compression_type::gzip);
}
BENCHMARK(BM_Storage_Append_Gzip);

// ============================================================================
// Benchmark: Sequential Read Throughput
// ============================================================================

/// Pre-fill a file with `num_records`, then benchmark reading them back.
static void bench_read_throughput(benchmark::State& state,
                                   size_t record_size,
                                   bool random_access) {
    const int64_t num_records = 10000;
    std::string payload = record_payload(record_size, 77);

    // Prepare the file once
    TempFile tf;
    tf.open_write();
    int64_t total_size = static_cast<int64_t>(record_size) * num_records;
    std::vector<char> buffer(total_size, 'A');
    for (int64_t i = 0; i < num_records; i++) {
        std::string p = record_payload(record_size, static_cast<uint64_t>(i));
        std::memcpy(buffer.data() + i * static_cast<int64_t>(record_size),
                    p.data(), record_size);
    }
    ::pwrite(tf.fd(), buffer.data(), static_cast<size_t>(total_size), 0);
    ::fsync(tf.fd());
    tf.close();

    // Read benchmarks
    std::mt19937 rng(12345);
    std::uniform_int_distribution<int64_t> dist(0, num_records - 1);
    std::vector<char> read_buf(record_size);
    LatencyCollector latencies;

    tf.open_read();
    int fd = tf.fd();

    for (auto _ : state) {
        int64_t offset;
        if (random_access) {
            offset = dist(rng) * static_cast<int64_t>(record_size);
        } else {
            offset = (static_cast<int64_t>(state.iterations()) % num_records) *
                     static_cast<int64_t>(record_size);
        }

        auto start = std::chrono::high_resolution_clock::now();
        ssize_t bytes = ::pread(fd, read_buf.data(), record_size, offset);
        auto end = std::chrono::high_resolution_clock::now();

        if (bytes != static_cast<ssize_t>(record_size)) {
            state.SkipWithError("pread returned short read");
            return;
        }

        latencies.record(
            static_cast<double>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end - start).count()));

        // Prevent compiler from optimizing away the read
        benchmark::DoNotOptimize(read_buf[0]);
    }

    state.counters["IOPS"] = benchmark::Counter(
        static_cast<double>(state.iterations()),
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
    state.counters["record_bytes"] = static_cast<double>(record_size);
    state.counters["access"] = static_cast<double>(random_access ? 1 : 0);
    state.counters["p50_us"] = latencies.percentile(50.0);
    state.counters["p95_us"] = latencies.percentile(95.0);
    state.counters["p99_us"] = latencies.percentile(99.0);
}

static void BM_Storage_Read_Sequential(benchmark::State& state) {
    bench_read_throughput(state, kMediumRecord, false);
}
BENCHMARK(BM_Storage_Read_Sequential);

static void BM_Storage_Read_Random(benchmark::State& state) {
    bench_read_throughput(state, kMediumRecord, true);
}
BENCHMARK(BM_Storage_Read_Random);

// ============================================================================
// Benchmark: Compaction Time
// ============================================================================

/// Simulate key-based compaction over a range of records.
static void BM_Storage_Compaction(benchmark::State& state) {
    const int64_t num_keys = state.range(0);
    const int64_t records_per_key = state.range(1);

    // Prepare: for each key, generate `records_per_key` records
    // Compaction keeps only the latest record per key.
    std::vector<std::pair<std::string, std::string>> all_records;
    all_records.reserve(static_cast<size_t>(num_keys * records_per_key));

    for (int64_t k = 0; k < num_keys; k++) {
        std::string key = "key_" + std::to_string(k);
        for (int64_t r = 0; r < records_per_key; r++) {
            all_records.emplace_back(key,
                record_payload(kMediumRecord,
                               static_cast<uint64_t>(k * records_per_key + r)));
        }
    }

    // Shuffle to simulate realistic interleaving
    std::mt19937 rng(42);
    std::shuffle(all_records.begin(), all_records.end(), rng);

    for (auto _ : state) {
        // Perform key-based deduplication
        std::map<std::string, std::string, std::less<>> compacted;
        for (const auto& [key, val] : all_records) {
            compacted[key] = val;  // latest wins
        }

        benchmark::DoNotOptimize(compacted.size());
    }

    state.counters["num_keys"] = static_cast<double>(num_keys);
    state.counters["dups_per_key"] = static_cast<double>(records_per_key);
    state.counters["total_records"] = static_cast<double>(num_keys * records_per_key);
}
BENCHMARK(BM_Storage_Compaction)
    ->Args({1000, 2})
    ->Args({1000, 10})
    ->Args({10000, 5})
    ->Args({100000, 3});

// ============================================================================
// Benchmark: mmap vs fstream
// ============================================================================

/// Write using mmap.
static void BM_Storage_Mmap_Write(benchmark::State& state) {
    const size_t chunk_size = kMediumRecord;
    TempFile tf;
    tf.open_write();
    int fd = tf.fd();

    // Pre-size the file
    int64_t file_size = static_cast<int64_t>(chunk_size) *
                        static_cast<int64_t>(state.max_iterations);
    if (::ftruncate(fd, file_size) < 0) {
        state.SkipWithError("ftruncate failed");
        return;
    }

    void* map = ::mmap(nullptr, static_cast<size_t>(file_size),
                        PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
        state.SkipWithError("mmap failed");
        return;
    }

    std::string payload = record_payload(chunk_size, 88);
    int64_t offset = 0;

    for (auto _ : state) {
        std::memcpy(static_cast<char*>(map) + offset,
                    payload.data(), chunk_size);
        offset += static_cast<int64_t>(chunk_size);
        if (offset + static_cast<int64_t>(chunk_size) > file_size) {
            offset = 0;
        }
    }

    ::msync(map, static_cast<size_t>(file_size), MS_SYNC);
    ::munmap(map, static_cast<size_t>(file_size));

    state.counters["MB_written"] = benchmark::Counter(
        static_cast<double>(static_cast<int64_t>(chunk_size) *
                            static_cast<int64_t>(state.iterations())) / 1e6,
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}
BENCHMARK(BM_Storage_Mmap_Write);

/// Write using fstream.
static void BM_Storage_Fstream_Write(benchmark::State& state) {
    const size_t chunk_size = kMediumRecord;
    TempFile tf;
    tf.open_write();
    int fd = tf.fd();
    tf.close(); // let fstream manage it

    std::ofstream ofs(tf.path(), std::ios::binary | std::ios::trunc);
    if (!ofs) {
        state.SkipWithError("Failed to open fstream");
        return;
    }

    std::string payload = record_payload(chunk_size, 88);

    for (auto _ : state) {
        ofs.write(payload.data(), static_cast<std::streamsize>(chunk_size));
        if (!ofs) {
            state.SkipWithError("fstream write failed");
            return;
        }
    }

    ofs.flush();
    ofs.close();

    state.counters["MB_written"] = benchmark::Counter(
        static_cast<double>(static_cast<int64_t>(chunk_size) *
                            static_cast<int64_t>(state.iterations())) / 1e6,
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}
BENCHMARK(BM_Storage_Fstream_Write);

// ============================================================================
// Benchmark: Segment size scaling
// ============================================================================

static void BM_Storage_SegmentSize(benchmark::State& state) {
    const int64_t seg_size = state.range(0);
    const size_t record_size = kMediumRecord;
    const int64_t num_records = seg_size / static_cast<int64_t>(record_size);

    TempFile tf;
    tf.open_write();
    int fd = tf.fd();

    // Pre-size the segment
    if (::ftruncate(fd, seg_size) < 0) {
        state.SkipWithError("ftruncate failed");
        return;
    }

    std::string payload = record_payload(record_size, 99);
    int64_t records_written = 0;

    for (auto _ : state) {
        int64_t offset = (records_written % num_records) *
                         static_cast<int64_t>(record_size);
        ::pwrite(fd, payload.data(), record_size, offset);
        records_written++;
    }

    ::fsync(fd);

    state.counters["seg_size_mb"] =
        static_cast<double>(seg_size) / (1024.0 * 1024.0);
    state.counters["records_in_seg"] = static_cast<double>(num_records);
}

/// Register segment-size benchmarks manually.
static void BM_Storage_SegmentSize_64K(benchmark::State& s) {
    BM_Storage_SegmentSize(s);
}
BENCHMARK(BM_Storage_SegmentSize_64K)->Arg(64LL * 1024);

static void BM_Storage_SegmentSize_1M(benchmark::State& s) {
    BM_Storage_SegmentSize(s);
}
BENCHMARK(BM_Storage_SegmentSize_1M)->Arg(1LL * 1024 * 1024);

static void BM_Storage_SegmentSize_16M(benchmark::State& s) {
    BM_Storage_SegmentSize(s);
}
BENCHMARK(BM_Storage_SegmentSize_16M)->Arg(16LL * 1024 * 1024);

static void BM_Storage_SegmentSize_256M(benchmark::State& s) {
    BM_Storage_SegmentSize(s);
}
BENCHMARK(BM_Storage_SegmentSize_256M)->Arg(256LL * 1024 * 1024);

BENCHMARK_MAIN();
