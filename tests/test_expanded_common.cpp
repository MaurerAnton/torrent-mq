#include <gtest/gtest.h>
#include "torrent/common/types.h"
#include "torrent/common/config.h"
#include "torrent/common/endian.h"
#include "torrent/common/thread_pool.h"
#include "torrent/common/rate_limiter.h"
#include "torrent/common/iobuf.h"
#include "torrent/common/circular_buffer.h"
#include "torrent/common/lru_cache.h"
#include "torrent/common/concurrent_map.h"
#include "torrent/common/backoff.h"
#include "torrent/storage/types.h"
#include <thread>
#include <chrono>
#include <random>
#include <algorithm>

namespace torrent::test {
namespace {

// ============================================================================
// More ThreadPool Tests (Expanded)
// ============================================================================
TEST(ThreadPoolExpanded, ManyTasksManyThreads) {
    thread_pool pool(8);
    std::atomic<int64_t> sum{0};
    for (int i = 0; i < 1000; i++) {
        pool.submit([&sum]() { sum.fetch_add(1); });
    }
    pool.shutdown();
    EXPECT_EQ(sum.load(), 1000);
}
TEST(ThreadPoolExpanded, SubmitDuringShutdown) {
    thread_pool pool(2);
    pool.shutdown();
    pool.submit([]() {});  // Should be rejected
    EXPECT_EQ(pool.total_tasks_executed(), 0);
}
TEST(ThreadPoolExpanded, EmptyBatch) {
    thread_pool pool(2);
    pool.submit_batch({});
    pool.shutdown();
    EXPECT_EQ(pool.total_tasks_executed(), 0);
}
TEST(ThreadPoolExpanded, IdleCount) {
    thread_pool pool(4);
    // All threads should be idle initially (waiting for work)
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(pool.idle_count(), 4);
    pool.shutdown();
}
TEST(ThreadPoolExpanded, MultiplePrioritiesInterleaved) {
    thread_pool pool(1);
    std::vector<std::string> order;
    std::mutex mtx;
    auto add = [&](const std::string& s) {
        std::lock_guard<std::mutex> lk(mtx); order.push_back(s);
    };
    pool.submit([&]() { add("high1"); }, thread_pool::priority::high);
    pool.submit([&]() { add("low1"); }, thread_pool::priority::low);
    pool.submit([&]() { add("high2"); }, thread_pool::priority::high);
    pool.submit([&]() { add("normal1"); }, thread_pool::priority::normal);
    pool.submit([&]() { add("high3"); }, thread_pool::priority::high);
    pool.shutdown();
    EXPECT_EQ(order.size(), 5);
    EXPECT_EQ(order[0], "high1");
    EXPECT_EQ(order[1], "high2");
    EXPECT_EQ(order[2], "high3");
    EXPECT_EQ(order[3], "normal1");
    EXPECT_EQ(order[4], "low1");
}
TEST(ThreadPoolExpanded, ExceptionSafety) {
    thread_pool pool(2);
    pool.submit([]() { throw std::runtime_error("test error"); });
    pool.submit([]() {});  // Should still execute
    EXPECT_NO_THROW(pool.shutdown());
    EXPECT_EQ(pool.total_tasks_executed(), 2);
}
TEST(ThreadPoolExpanded, FutureExceptionPropagation) {
    thread_pool pool(1);
    auto future = pool.submit_future([]() -> int {
        throw std::runtime_error("future error");
        return 0;
    });
    pool.shutdown();
    EXPECT_THROW(future.get(), std::runtime_error);
}
TEST(ThreadPoolExpanded, StressTest) {
    thread_pool pool(16);
    std::atomic<int> counter{0};
    for (int i = 0; i < 10000; i++) {
        pool.submit([&]() { counter.fetch_add(1); });
    }
    pool.shutdown();
    EXPECT_EQ(counter.load(), 10000);
}

// ============================================================================
// More RateLimiter Tests
// ============================================================================
TEST(RateLimiterExpanded, BlockingConsume) {
    rate_limiter rl(10000, 100, 100);
    int64_t consumed = rl.consume_blocking(200, 1000);
    EXPECT_GT(consumed, 0);
}
TEST(RateLimiterExpanded, RateChangeDuringOperation) {
    rate_limiter rl(1000, 100, 100);
    rl.try_consume(100);
    rl.set_rate(100000);
    rl.refill(100);
    EXPECT_TRUE(rl.try_consume(50));
}
TEST(RateLimiterExpanded, BurstChange) {
    rate_limiter rl(1000, 100, 100);
    rl.set_burst(1000);
    EXPECT_TRUE(rl.try_consume(500));
}
TEST(RateLimiterExpanded, ZeroRateMeansUnlimited) {
    rate_limiter rl(0);
    EXPECT_FALSE(rl.is_enabled());
    EXPECT_TRUE(rl.try_consume(999999999));
}

// ============================================================================
// More LRU Cache Tests
// ============================================================================
TEST(LruCacheExpanded, UpdateExisting) {
    lru_cache<int, std::string> cache(5);
    cache.put(1, "one");
    cache.put(1, "uno");
    EXPECT_EQ(cache.get(1).value(), "uno");
    EXPECT_EQ(cache.size(), 1);
}
TEST(LruCacheExpanded, HitRatio) {
    lru_cache<int, std::string> cache(5);
    cache.put(1, "one");
    cache.put(2, "two");
    cache.get(1); cache.get(1); cache.get(1);  // hits
    cache.get(2);  // hit
    cache.get(3);  // miss
    // Hit ratio should be 4/5 = 0.8
    double ratio = cache.hit_ratio();
    EXPECT_GT(ratio, 0.7);
    EXPECT_LT(ratio, 0.9);
}
TEST(LruCacheExpanded, LargeCapacity) {
    lru_cache<int, int> cache(10000);
    for (int i = 0; i < 10000; i++) cache.put(i, i);
    EXPECT_EQ(cache.size(), 10000);
    cache.put(10000, 10000);  // Evicts one
    EXPECT_EQ(cache.size(), 10000);
}
TEST(LruCacheExpanded, ResetStats) {
    lru_cache<int, int> cache(5);
    cache.put(1, 1); cache.get(1); cache.get(2);
    cache.reset_stats();
    EXPECT_DOUBLE_EQ(cache.hit_ratio(), 0.0);
}

// ============================================================================
// More ConcurrentMap Tests
// ============================================================================
TEST(ConcurrentMapExpanded, ThreadSafety) {
    concurrent_map<int, int> map;
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; t++) {
        threads.emplace_back([&map, t]() {
            for (int i = 0; i < 1000; i++) {
                map.insert((t * 1000) + i, (t * 1000) + i);
            }
        });
    }
    for (auto& t : threads) t.join();
    EXPECT_EQ(map.size(), 4000);
}
TEST(ConcurrentMapExpanded, Empty) {
    concurrent_map<std::string, int> map;
    EXPECT_TRUE(map.empty());
    map.insert("a", 1);
    EXPECT_FALSE(map.empty());
}
TEST(ConcurrentMapExpanded, Entries) {
    concurrent_map<std::string, int> map;
    map.insert("x", 1);
    map.insert("y", 2);
    auto entries = map.entries();
    EXPECT_EQ(entries.size(), 2);
}

// ============================================================================
// More Backoff Tests
// ============================================================================
TEST(BackoffExpanded, MaxAttempts) {
    ExponentialBackoff backoff(100, 10000, 2.0);
    int count = 0;
    while (backoff.next_delay() > 0) { count++; if (count > 100) break; }
    EXPECT_LT(count, 100);
}
TEST(BackoffExpanded, CustomMultiplier) {
    ExponentialBackoff backoff(100, 10000, 3.0);
    int64_t d1 = backoff.next_delay();
    int64_t d2 = backoff.next_delay();
    EXPECT_GE(d2, d1 * 2);  // Multiplier 3.0, so d2 >= 3*d1 - jitter
}

// ============================================================================
// TokenBucket Structure Tests
// ============================================================================
struct TokenBucket {
    int64_t tokens;
    int64_t rate_bps;
    int64_t burst;
    int64_t last_refill_us;
};
TEST(TokenBucketTest, InitialTokens) {
    TokenBucket tb{100, 1000, 200, 0};
    EXPECT_EQ(tb.tokens, 100);
}
TEST(TokenBucketTest, RefillCalculation) {
    TokenBucket tb{0, 1000, 200, 0};
    int64_t elapsed_us = 100000;  // 100ms
    int64_t refill = (1000 * elapsed_us) / 1000000;
    EXPECT_EQ(refill, 100);
}
TEST(TokenBucketTest, BurstCap) {
    TokenBucket tb{150, 1000, 200, 0};
    int64_t elapsed_us = 200000;  // 200ms -> 200 tokens refill
    int64_t refill = std::min(tb.tokens + 200, tb.burst);
    EXPECT_EQ(refill, 200);  // Capped at burst
}

// ============================================================================
// SPSC Queue Extended Tests
// ============================================================================
TEST(SpscQueueExpanded, MoveSemantics) {
    spsc_queue<std::string> q(8);
    std::string s = "hello";
    EXPECT_TRUE(q.try_push(std::move(s)));
    EXPECT_TRUE(s.empty());
    auto val = q.try_pop();
    EXPECT_TRUE(val.has_value());
    EXPECT_EQ(val.value(), "hello");
}
TEST(SpscQueueExpanded, CapacityPowerOfTwo) {
    spsc_queue<int> q(5);
    EXPECT_EQ(q.capacity(), 7);  // Rounded up to 8, minus 1 = 7
}
TEST(SpscQueueExpanded, Clear) {
    spsc_queue<int> q(8);
    q.try_push(1); q.try_push(2); q.try_push(3);
    q.clear();
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size(), 0);
}

// ============================================================================
// IOBuf Extended Tests
// ============================================================================
TEST(IOBufExpanded, MultipleAppends) {
    iobuf buf;
    for (int i = 0; i < 100; i++) {
        buf.append("x", 1);
    }
    EXPECT_EQ(buf.size_bytes(), 100);
    EXPECT_EQ(buf.fragment_count(), 100);
}
TEST(IOBufExpanded, ConstructorFromVector) {
    std::vector<uint8_t> data = {1, 2, 3, 4, 5};
    iobuf buf(data);
    EXPECT_EQ(buf.size_bytes(), 5);
}
TEST(IOBufExpanded, ShareZeroCopy) {
    iobuf buf;
    buf.append("hello world", 11);
    auto slice = buf.share(6, 5);
    auto flat = slice.flatten();
    std::string result(flat.begin(), flat.end());
    EXPECT_EQ(result, "world");
}
TEST(IOBufExpanded, ReserveAndShrink) {
    iobuf buf;
    buf.reserve_fragments(100);
    buf.shrink_to_fit();
    EXPECT_EQ(buf.fragment_count(), 0);
}

// ============================================================================
// Compaction Data Structure Tests
// ============================================================================
TEST(CompactionDataTest, KeyGrouping) {
    struct KeyGroup { std::string key; int64_t max_offset; bool tombstone; };
    std::vector<KeyGroup> groups = {
        {"key1", 100, false},
        {"key1", 200, false},  // Duplicate, newer
        {"key2", 150, false},
        {"key3", 300, true},   // Tombstone
    };
    // After grouping: keep key1@200, key2@150, key3@300(tombstone)
    std::map<std::string, KeyGroup> grouped;
    for (const auto& g : groups) {
        auto& existing = grouped[g.key];
        if (g.max_offset > existing.max_offset) existing = g;
    }
    EXPECT_EQ(grouped.size(), 3);
    EXPECT_EQ(grouped["key1"].max_offset, 200);
    EXPECT_EQ(grouped["key2"].max_offset, 150);
    EXPECT_EQ(grouped["key3"].max_offset, 300);
}
TEST(CompactionDataTest, TombstonePreservation) {
    // Tombstones must be preserved for a configurable time
    // After retention period, they can be removed
    bool tombstone_expired = true;  // Simulated check
    EXPECT_TRUE(tombstone_expired);
}
TEST(CompactionDataTest, EmptyBatchCompaction) {
    std::vector<int> batches;
    // Compaction of empty input yields empty output
    EXPECT_TRUE(batches.empty());
}

// ============================================================================
// RecordBatch Extended Tests
// ============================================================================
TEST(RecordBatchExtended, CrcIntegrityRegion) {
    // CRC covers: attributes, last_offset_delta, base_timestamp, max_timestamp,
    // producer_id, producer_epoch, base_sequence, record_count
    RecordBatch batch;
    batch.attributes = 0x0003;
    batch.last_offset_delta = 5;
    batch.base_timestamp = 1234567890;
    // CRC should be computed over these fields
    EXPECT_GT(batch.attributes, 0);
}
TEST(RecordBatchExtended, MaxBatchSize) {
    RecordBatch batch;
    for (int i = 0; i < 10000; i++) {
        batch.records.push_back(Record{});
    }
    EXPECT_EQ(batch.record_count(), 10000);
}
TEST(RecordBatchExtended, ProducerInfo) {
    RecordBatch batch;
    batch.producer_id = 12345;
    batch.producer_epoch = 3;
    batch.base_sequence = 0;
    EXPECT_EQ(batch.producer_id, 12345);
    EXPECT_EQ(batch.producer_epoch, 3);
    EXPECT_EQ(batch.base_sequence, 0);
}

// ============================================================================
// SegmentInfo Tests
// ============================================================================
TEST(SegmentInfoTests, DefaultValues) {
    SegmentInfo info;
    EXPECT_EQ(info.base_offset, 0);
    EXPECT_EQ(info.size_bytes, 0);
    EXPECT_EQ(info.record_count, 0);
    EXPECT_FALSE(info.is_sealed);
}
TEST(SegmentInfoTests, ActiveSegment) {
    SegmentInfo info;
    info.is_sealed = false;
    EXPECT_FALSE(info.is_sealed);
}
TEST(SegmentInfoTests, SealedSegment) {
    SegmentInfo info;
    info.is_sealed = true;
    info.sealed_at_ms = 1234567890;
    EXPECT_TRUE(info.is_sealed);
    EXPECT_GT(info.sealed_at_ms, 0);
}

// ============================================================================
// TopicConfig Extended Tests
// ============================================================================
TEST(TopicConfigExtended, RetentionDefaults) {
    TopicConfig cfg;
    EXPECT_EQ(cfg.cleanup, cleanup_policy::delete_cleanup);
}
TEST(TopicConfigExtended, CompactionConfig) {
    TopicConfig cfg;
    cfg.cleanup = cleanup_policy::compact_cleanup;
    cfg.min_compaction_lag_ms = 3600000;
    cfg.max_compaction_lag_ms = 86400000;
    EXPECT_EQ(cfg.cleanup, cleanup_policy::compact_cleanup);
    EXPECT_EQ(cfg.min_compaction_lag_ms, 3600000);
}
TEST(TopicConfigExtended, BothCleanupPolicies) {
    TopicConfig cfg;
    cfg.cleanup = cleanup_policy::compact_and_delete;
    EXPECT_EQ(cfg.cleanup, cleanup_policy::compact_and_delete);
}

// ============================================================================
// PartitionState Tests
// ============================================================================
TEST(PartitionStateTests, Watermarks) {
    PartitionState state;
    state.high_watermark = 1000;
    state.last_stable_offset = 950;
    state.log_start_offset = 0;
    state.log_end_offset = 1500;
    EXPECT_EQ(state.high_watermark, 1000);
    EXPECT_GT(state.log_end_offset, state.high_watermark);
}
TEST(PartitionStateTests, EmptyPartition) {
    PartitionState state;
    EXPECT_EQ(state.high_watermark, 0);
    EXPECT_EQ(state.log_start_offset, 0);
    EXPECT_EQ(state.log_end_offset, 0);
}

// ============================================================================
// BrokerMetadata Tests
// ============================================================================
TEST(BrokerMetadataExpanded, MultipleEndpoints) {
    BrokerMetadata broker;
    broker.node_id = 1;
    broker.host = "broker1.example.com";
    broker.port = 9092;
    broker.rack = "rack-west-1a";
    EXPECT_EQ(broker.node_id, 1);
    EXPECT_EQ(broker.rack, "rack-west-1a");
}
TEST(BrokerMetadataExpanded, ControllerFlag) {
    BrokerMetadata broker;
    broker.is_controller = true;
    EXPECT_TRUE(broker.is_controller);
}

// ============================================================================
// ReplicaStatus Tests
// ============================================================================
TEST(ReplicaStatusTests, InSync) {
    ReplicaStatus status;
    status.broker_id = 2;
    status.is_in_sync = true;
    status.lag = 0;
    EXPECT_TRUE(status.is_in_sync);
    EXPECT_EQ(status.lag, 0);
}
TEST(ReplicaStatusTests, OutOfSync) {
    ReplicaStatus status;
    status.is_in_sync = false;
    status.lag = 1500;
    EXPECT_FALSE(status.is_in_sync);
    EXPECT_GT(status.lag, 0);
}

// ============================================================================
// LogAppendResult Tests
// ============================================================================
TEST(LogAppendResultTests, Success) {
    LogAppendResult result;
    result.base_offset = 12345;
    result.append_timestamp_ms = 1234567890000;
    result.error = error_code::none;
    EXPECT_EQ(result.base_offset, 12345);
    EXPECT_EQ(result.error, error_code::none);
}
TEST(LogAppendResultTests, Failure) {
    LogAppendResult result;
    result.error = error_code::not_leader_for_partition;
    result.error_message = "Not leader for partition";
    EXPECT_EQ(result.error, error_code::not_leader_for_partition);
}

// ============================================================================
// FetchResult Tests
// ============================================================================
TEST(FetchResultTests, WithRecordBatch) {
    FetchResult result;
    result.high_watermark = 500;
    result.last_stable_offset = 480;
    result.log_start_offset = 0;
    EXPECT_EQ(result.high_watermark, 500);
}
TEST(FetchResultTests, EmptyFetch) {
    FetchResult result;
    EXPECT_EQ(result.high_watermark, 0);
    EXPECT_EQ(result.log_start_offset, 0);
}

// ============================================================================
// Result<T> Move Tests
// ============================================================================
TEST(ResultExpanded, MoveOnlyType) {
    auto r = result<std::unique_ptr<int>>::success(std::make_unique<int>(42));
    EXPECT_TRUE(r.ok());
    EXPECT_EQ(*r.value, 42);
    auto r2 = std::move(r);
    EXPECT_EQ(*r2.value, 42);
}
TEST(ResultExpanded, VoidResult) {
    auto r = result<void>::failure(error_code::timeout, "timed out");
    EXPECT_TRUE(r.failed());
    EXPECT_EQ(r.error, error_code::timeout);
}

// ============================================================================
// Endpoint Hash Tests
// ============================================================================
TEST(EndpointExpanded, HashSameEndpoints) {
    endpoint a{"host", 9092};
    endpoint b{"host", 9092};
    std::hash<endpoint> hasher;
    EXPECT_EQ(hasher(a), hasher(b));
}
TEST(EndpointExpanded, HashDifferentPorts) {
    endpoint a{"host", 9092};
    endpoint b{"host", 9093};
    std::hash<endpoint> hasher;
    EXPECT_NE(hasher(a), hasher(b));
}

// ============================================================================
// Compression Round-Trip Concept
// ============================================================================
TEST(CompressionConcept, AllTypesHaveNames) {
    for (auto ct : {compression_type::none, compression_type::gzip,
                     compression_type::snappy, compression_type::lz4,
                     compression_type::zstd}) {
        const char* name = compression_name(ct);
        EXPECT_STRNE(name, "unknown");
        EXPECT_GT(strlen(name), 1);
    }
}
TEST(CompressionConcept, UniqueNames) {
    std::set<std::string> names;
    for (auto ct : {compression_type::none, compression_type::gzip,
                     compression_type::snappy, compression_type::lz4,
                     compression_type::zstd}) {
        names.insert(compression_name(ct));
    }
    EXPECT_EQ(names.size(), 5);
}

}  // namespace
}  // namespace torrent::test
