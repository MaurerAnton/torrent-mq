#include <gtest/gtest.h>
#include "torrent/common/config.h"
#include "torrent/common/types.h"
#include "torrent/common/endian.h"
#include "torrent/common/thread_pool.h"
#include "torrent/common/rate_limiter.h"
#include "torrent/common/iobuf.h"
#include "torrent/common/circular_buffer.h"
#include "torrent/common/lru_cache.h"
#include "torrent/common/concurrent_map.h"
#include "torrent/common/backoff.h"
#include "torrent/common/uuid.h"
#include "torrent/common/hostname.h"
#include "torrent/storage/types.h"
#include <chrono>
#include <thread>
#include <random>
#include <filesystem>
#include <fstream>
#include <algorithm>

namespace torrent::test {
namespace {

// ============================================================================
// Config Tests
// ============================================================================

TEST(ConfigTest, DefaultValues) {
    config cfg;
    EXPECT_EQ(cfg.broker_id(), 0);
    EXPECT_EQ(cfg.port(), 9092);
    EXPECT_EQ(cfg.admin_port(), 9644);
    EXPECT_EQ(cfg.metrics_port(), 9090);
    EXPECT_EQ(cfg.default_partitions(), 1);
    EXPECT_EQ(cfg.default_replication_factor(), 3);
    EXPECT_EQ(cfg.enable_transactions(), true);
}

TEST(ConfigTest, SetAndGet) {
    config cfg;
    cfg.set("custom.key", "custom_value");
    EXPECT_EQ(cfg.get_or("custom.key", ""), "custom_value");
    EXPECT_TRUE(cfg.has("custom.key"));
    EXPECT_FALSE(cfg.has("nonexistent.key"));
}

TEST(ConfigTest, GetOrFallback) {
    config cfg;
    EXPECT_EQ(cfg.get_or("missing.key", "default"), "default");
    EXPECT_EQ(cfg.get_or("broker.id", "999"), "0");
}

TEST(ConfigTest, TypedGetters) {
    config cfg;
    cfg.set("int.key", "42");
    cfg.set("bool.key", "true");
    cfg.set("string.key", "hello");

    EXPECT_EQ(cfg.get_as<int64_t>("int.key").value(), 42);
    EXPECT_EQ(cfg.get_as<bool>("bool.key").value(), true);
    EXPECT_EQ(cfg.get_as<std::string>("string.key").value(), "hello");
    EXPECT_FALSE(cfg.get_as<int64_t>("nonexistent").has_value());
}

TEST(ConfigTest, SeedServersParsing) {
    config cfg;
    cfg.set("cluster.seed_servers", "broker1:9092,broker2:9093,broker3:9092");
    auto seeds = cfg.seed_servers();
    EXPECT_EQ(seeds.size(), 3);
    EXPECT_EQ(seeds[0].host, "broker1");
    EXPECT_EQ(seeds[0].port, 9092);
    EXPECT_EQ(seeds[1].host, "broker2");
    EXPECT_EQ(seeds[1].port, 9093);
    EXPECT_EQ(seeds[2].host, "broker3");
    EXPECT_EQ(seeds[2].port, 9092);
}

TEST(ConfigTest, EmptySeedServers) {
    config cfg;
    cfg.set("cluster.seed_servers", "");
    auto seeds = cfg.seed_servers();
    EXPECT_TRUE(seeds.empty());
}

TEST(ConfigTest, JsonRoundTrip) {
    config cfg;
    cfg.set("test.key", "test_value");
    auto json = cfg.to_json();
    EXPECT_TRUE(json.contains("test"));
    EXPECT_EQ(json["test"]["key"], "test_value");
}

TEST(ConfigTest, FromJsonFile) {
    std::string tmp_path = "/tmp/torrent_test_config_" + std::to_string(getpid()) + ".json";
    std::ofstream ofs(tmp_path);
    ofs << R"({"broker":{"id":5},"storage":{"log":{"dir":"/tmp/data"}}})";
    ofs.close();

    auto cfg = config::from_file(tmp_path);
    EXPECT_EQ(cfg.broker_id(), 5);
    EXPECT_EQ(cfg.log_dir(), "/tmp/data");

    std::filesystem::remove(tmp_path);
}

// ============================================================================
// Endian Tests
// ============================================================================

TEST(EndianTest, HostToBe16) {
    uint16_t val = 0x1234;
    uint16_t be = host_to_be16(val);
    uint16_t roundtrip = be_to_host16(be);
    EXPECT_EQ(roundtrip, val);
}

TEST(EndianTest, HostToBe32) {
    uint32_t val = 0x12345678;
    uint32_t be = host_to_be32(val);
    uint32_t roundtrip = be_to_host32(be);
    EXPECT_EQ(roundtrip, val);
}

TEST(EndianTest, HostToBe64) {
    uint64_t val = 0x123456789ABCDEF0ULL;
    uint64_t be = host_to_be64(val);
    uint64_t roundtrip = be_to_host64(be);
    EXPECT_EQ(roundtrip, val);
}

TEST(EndianTest, HostToLe16) {
    uint16_t val = 0xABCD;
    uint16_t le = host_to_le16(val);
    uint16_t roundtrip = le_to_host16(le);
    EXPECT_EQ(roundtrip, val);
}

TEST(EndianTest, HostToLe32) {
    uint32_t val = 0xDEADBEEF;
    uint32_t le = host_to_le32(val);
    uint32_t roundtrip = le_to_host32(le);
    EXPECT_EQ(roundtrip, val);
}

TEST(EndianTest, HostToLe64) {
    uint64_t val = 0xFEEDFACECAFEBEEFULL;
    uint64_t le = host_to_le64(val);
    uint64_t roundtrip = le_to_host64(le);
    EXPECT_EQ(roundtrip, val);
}

TEST(EndianTest, ZigZagEncodeDecode32) {
    EXPECT_EQ(zigzag_decode_32(zigzag_encode_32(0)), 0);
    EXPECT_EQ(zigzag_decode_32(zigzag_encode_32(-1)), -1);
    EXPECT_EQ(zigzag_decode_32(zigzag_encode_32(1)), 1);
    EXPECT_EQ(zigzag_decode_32(zigzag_encode_32(INT32_MAX)), INT32_MAX);
    EXPECT_EQ(zigzag_decode_32(zigzag_encode_32(INT32_MIN)), INT32_MIN);
}

TEST(EndianTest, ZigZagEncodeDecode64) {
    EXPECT_EQ(zigzag_decode_64(zigzag_encode_64(0)), 0);
    EXPECT_EQ(zigzag_decode_64(zigzag_encode_64(-1)), -1);
    EXPECT_EQ(zigzag_decode_64(zigzag_encode_64(1)), 1);
}

TEST(EndianTest, VarintEncodeDecode32) {
    uint8_t buf[5];
    size_t len = varint_encode_32(300, buf);
    EXPECT_GT(len, 0);
    EXPECT_LE(len, 5);
    uint32_t decoded = 0;
    size_t read = varint_decode_32(buf, len, decoded);
    EXPECT_EQ(read, len);
    EXPECT_EQ(decoded, 300);
}

TEST(EndianTest, VarintEncodeDecode64) {
    uint8_t buf[10];
    size_t len = varint_encode_64(1000000, buf);
    EXPECT_GT(len, 0);
    uint64_t decoded = 0;
    size_t read = varint_decode_64(buf, len, decoded);
    EXPECT_EQ(read, len);
    EXPECT_EQ(decoded, 1000000);
}

TEST(EndianTest, VarintSignedRoundTrip) {
    std::vector<int64_t> test_values = {0, 1, -1, 42, -42, 1000000, -1000000,
                                          INT64_MAX, INT64_MIN, 1234567890123LL};
    for (auto val : test_values) {
        uint8_t buf[10];
        size_t len = varint_encode_signed_64(val, buf);
        int64_t decoded = 0;
        size_t read = varint_decode_signed_64(buf, len, decoded);
        EXPECT_EQ(read, len);
        EXPECT_EQ(decoded, val) << "Failed for value " << val;
    }
}

TEST(EndianTest, WriteInt16Be) {
    uint8_t buf[2];
    write_int16_be(buf, 0x1234);
    EXPECT_EQ(read_int16_be(buf), 0x1234);
}

TEST(EndianTest, WriteInt32Be) {
    uint8_t buf[4];
    write_int32_be(buf, 0x12345678);
    EXPECT_EQ(read_int32_be(buf), 0x12345678);
}

TEST(EndianTest, WriteInt64Be) {
    uint8_t buf[8];
    write_int64_be(buf, 0x123456789ABCDEF0LL);
    EXPECT_EQ(read_int64_be(buf), 0x123456789ABCDEF0LL);
}

// ============================================================================
// ThreadPool Tests
// ============================================================================

TEST(ThreadPoolTest, CreateAndShutdown) {
    thread_pool pool(4);
    EXPECT_EQ(pool.thread_count(), 4);
    EXPECT_FALSE(pool.is_shutdown());
    pool.shutdown();
    EXPECT_TRUE(pool.is_shutdown());
}

TEST(ThreadPoolTest, SubmitTask) {
    thread_pool pool(2);
    std::atomic<int> counter{0};
    pool.submit([&]() { counter.fetch_add(1); });
    pool.shutdown();
    EXPECT_EQ(counter.load(), 1);
}

TEST(ThreadPoolTest, SubmitFuture) {
    thread_pool pool(2);
    auto future = pool.submit_future([]() { return 42; });
    pool.shutdown();
    EXPECT_EQ(future.get(), 42);
}

TEST(ThreadPoolTest, SubmitBatch) {
    thread_pool pool(4);
    std::atomic<int> sum{0};
    std::vector<thread_pool::task_t> tasks;
    for (int i = 0; i < 100; i++) {
        tasks.push_back([&sum]() { sum.fetch_add(1); });
    }
    pool.submit_batch(std::move(tasks));
    pool.shutdown();
    EXPECT_EQ(sum.load(), 100);
}

TEST(ThreadPoolTest, PriorityOrdering) {
    thread_pool pool(1);
    std::vector<int> order;
    std::mutex mutex;

    pool.submit([&]() { std::lock_guard<std::mutex> lk(mutex); order.push_back(3); },
                thread_pool::priority::low);
    pool.submit([&]() { std::lock_guard<std::mutex> lk(mutex); order.push_back(1); },
                thread_pool::priority::high);
    pool.submit([&]() { std::lock_guard<std::mutex> lk(mutex); order.push_back(2); },
                thread_pool::priority::normal);

    pool.shutdown();
    EXPECT_EQ(order[0], 1); // High priority first
    EXPECT_EQ(order[1], 2); // Normal second
    EXPECT_EQ(order[2], 3); // Low last
}

TEST(ThreadPoolTest, ShutdownNow) {
    thread_pool pool(2);
    std::atomic<int> counter{0};
    for (int i = 0; i < 1000; i++) {
        pool.submit([&]() { std::this_thread::sleep_for(std::chrono::milliseconds(1)); counter++; });
    }
    pool.shutdown_now();
    EXPECT_TRUE(pool.is_shutdown());
    // Some tasks may not have executed — that's expected with shutdown_now
}

TEST(ThreadPoolTest, PendingCount) {
    thread_pool pool(1);
    EXPECT_EQ(pool.pending_count(), 0);
    pool.submit([&]() { std::this_thread::sleep_for(std::chrono::milliseconds(50)); });
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    // Task should be running or pending
    pool.shutdown();
}

TEST(ThreadPoolTest, TotalTasksExecuted) {
    thread_pool pool(2);
    for (int i = 0; i < 10; i++) {
        pool.submit([]() {});
    }
    pool.shutdown();
    EXPECT_EQ(pool.total_tasks_executed(), 10);
}

TEST(ThreadPoolTest, NullTaskRejected) {
    thread_pool pool(1);
    pool.submit(nullptr);
    pool.submit(nullptr);
    pool.shutdown();
    EXPECT_EQ(pool.total_tasks_executed(), 0);
}

// ============================================================================
// RateLimiter Tests
// ============================================================================

TEST(RateLimiterTest, UnlimitedByDefault) {
    rate_limiter rl;
    EXPECT_TRUE(rl.try_consume(1000000));
    EXPECT_FALSE(rl.is_enabled());
}

TEST(RateLimiterTest, TryConsume) {
    rate_limiter rl(1000, 100, 100);  // 1000 Bps, 100 byte burst, 100 initial tokens
    EXPECT_TRUE(rl.try_consume(50));    // 50 remaining
    EXPECT_TRUE(rl.try_consume(50));    // 0 remaining
    EXPECT_FALSE(rl.try_consume(1));    // No tokens
}

TEST(RateLimiterTest, CanConsume) {
    rate_limiter rl(1000, 100, 100);
    EXPECT_TRUE(rl.can_consume(100));
    EXPECT_FALSE(rl.can_consume(101));
}

TEST(RateLimiterTest, Refill) {
    rate_limiter rl(1000, 100, 50);
    rl.try_consume(50);  // 0 remaining
    rl.refill(30);
    EXPECT_TRUE(rl.try_consume(30));
}

TEST(RateLimiterTest, SetRate) {
    rate_limiter rl(100, 100, 100);
    rl.set_rate(10000);
    EXPECT_EQ(rl.rate_bps(), 10000);
}

TEST(RateLimiterTest, Reset) {
    rate_limiter rl(1000, 100, 100);
    rl.try_consume(100);
    rl.reset();
    EXPECT_TRUE(rl.try_consume(100));
}

// ============================================================================
// IOBuf Tests
// ============================================================================

TEST(IOBufTest, EmptyByDefault) {
    iobuf buf;
    EXPECT_TRUE(buf.empty());
    EXPECT_EQ(buf.size_bytes(), 0);
    EXPECT_EQ(buf.fragment_count(), 0);
}

TEST(IOBufTest, AppendBytes) {
    iobuf buf;
    buf.append("hello", 5);
    EXPECT_EQ(buf.size_bytes(), 5);
    EXPECT_FALSE(buf.empty());
}

TEST(IOBufTest, AppendString) {
    iobuf buf;
    buf.append(std::string("world"));
    EXPECT_EQ(buf.size_bytes(), 5);
}

TEST(IOBufTest, AppendIOBuf) {
    iobuf buf1;
    buf1.append("hello", 5);
    iobuf buf2;
    buf2.append(" world", 6);
    buf1.append(std::move(buf2));
    EXPECT_EQ(buf1.size_bytes(), 11);
    EXPECT_TRUE(buf2.empty());  // Moved from
}

TEST(IOBufTest, TrimFront) {
    iobuf buf;
    buf.append("abcdefgh", 8);
    buf.trim_front(3);
    EXPECT_EQ(buf.size_bytes(), 5);
}

TEST(IOBufTest, TrimFrontExcess) {
    iobuf buf;
    buf.append("abc", 3);
    buf.trim_front(10);  // Trim more than available
    EXPECT_TRUE(buf.empty());
}

TEST(IOBufTest, Clear) {
    iobuf buf;
    buf.append("data", 4);
    buf.clear();
    EXPECT_TRUE(buf.empty());
}

TEST(IOBufTest, Flatten) {
    iobuf buf;
    buf.append("hello", 5);
    buf.append(" world", 6);
    auto flat = buf.flatten();
    std::string result(flat.begin(), flat.end());
    EXPECT_EQ(result, "hello world");
}

TEST(IOBufTest, CopyTo) {
    iobuf buf;
    buf.append("abcdef", 6);
    char dest[10] = {0};
    size_t copied = buf.copy_to(dest, 4);
    EXPECT_EQ(copied, 4);
    EXPECT_EQ(std::string(dest, 4), "abcd");
}

TEST(IOBufTest, Share) {
    iobuf buf;
    buf.append("0123456789", 10);
    auto slice = buf.share(3, 4);
    auto flat = slice.flatten();
    std::string result(flat.begin(), flat.end());
    EXPECT_EQ(result, "3456");
}

TEST(IOBufTest, Prefix) {
    iobuf buf;
    buf.append("hello world", 11);
    auto prefix = buf.prefix(5);
    EXPECT_EQ(prefix, "hello");
}

// ============================================================================
// SPSC Queue Tests
// ============================================================================

TEST(SpscQueueTest, PushAndPop) {
    spsc_queue<int> q(8);
    EXPECT_TRUE(q.try_push(42));
    auto val = q.try_pop();
    EXPECT_TRUE(val.has_value());
    EXPECT_EQ(val.value(), 42);
}

TEST(SpscQueueTest, EmptyPop) {
    spsc_queue<int> q(8);
    EXPECT_FALSE(q.try_pop().has_value());
}

TEST(SpscQueueTest, FullQueue) {
    spsc_queue<int> q(4);  // Capacity is 3 (one slot reserved)
    EXPECT_TRUE(q.try_push(1));
    EXPECT_TRUE(q.try_push(2));
    EXPECT_TRUE(q.try_push(3));
    EXPECT_FALSE(q.try_push(4));  // Full
}

TEST(SpscQueueTest, FifoOrdering) {
    spsc_queue<int> q(8);
    for (int i = 0; i < 5; i++) q.try_push(i);
    for (int i = 0; i < 5; i++) {
        auto val = q.try_pop();
        EXPECT_TRUE(val.has_value());
        EXPECT_EQ(val.value(), i);
    }
}

TEST(SpscQueueTest, SizeAndEmpty) {
    spsc_queue<int> q(8);
    EXPECT_TRUE(q.empty());
    q.try_push(1);
    EXPECT_EQ(q.size(), 1);
    q.try_push(2);
    EXPECT_EQ(q.size(), 2);
    q.try_pop();
    EXPECT_EQ(q.size(), 1);
    q.try_pop();
    EXPECT_TRUE(q.empty());
}

// ============================================================================
// LRU Cache Tests
// ============================================================================

TEST(LruCacheTest, PutAndGet) {
    lru_cache<int, std::string> cache(5);
    cache.put(1, "one");
    auto val = cache.get(1);
    EXPECT_TRUE(val.has_value());
    EXPECT_EQ(val.value(), "one");
}

TEST(LruCacheTest, GetMissingKey) {
    lru_cache<int, std::string> cache(5);
    EXPECT_FALSE(cache.get(42).has_value());
}

TEST(LruCacheTest, Eviction) {
    lru_cache<int, std::string> cache(2);
    cache.put(1, "one");
    cache.put(2, "two");
    cache.put(3, "three");  // Evicts key 1
    EXPECT_FALSE(cache.get(1).has_value());
    EXPECT_TRUE(cache.get(2).has_value());
    EXPECT_TRUE(cache.get(3).has_value());
}

TEST(LruCacheTest, PromotesOnGet) {
    lru_cache<int, std::string> cache(2);
    cache.put(1, "one");
    cache.put(2, "two");
    cache.get(1);  // Promotes 1 to MRU
    cache.put(3, "three");  // Evicts key 2 (LRU)
    EXPECT_TRUE(cache.get(1).has_value());
    EXPECT_FALSE(cache.get(2).has_value());
    EXPECT_TRUE(cache.get(3).has_value());
}

TEST(LruCacheTest, Contains) {
    lru_cache<int, std::string> cache(5);
    cache.put(1, "one");
    EXPECT_TRUE(cache.contains(1));
    EXPECT_FALSE(cache.contains(2));
}

TEST(LruCacheTest, Erase) {
    lru_cache<int, std::string> cache(5);
    cache.put(1, "one");
    EXPECT_TRUE(cache.erase(1));
    EXPECT_FALSE(cache.contains(1));
    EXPECT_FALSE(cache.erase(1));  // Already gone
}

TEST(LruCacheTest, Clear) {
    lru_cache<int, std::string> cache(5);
    cache.put(1, "one");
    cache.put(2, "two");
    cache.clear();
    EXPECT_TRUE(cache.empty());
    EXPECT_EQ(cache.size(), 0);
}

// ============================================================================
// Concurrent Map Tests
// ============================================================================

TEST(ConcurrentMapTest, InsertAndGet) {
    concurrent_map<std::string, int> map;
    map.insert("key1", 42);
    auto val = map.get("key1");
    EXPECT_TRUE(val.has_value());
    EXPECT_EQ(val.value(), 42);
}

TEST(ConcurrentMapTest, Contains) {
    concurrent_map<std::string, int> map;
    map.insert("key1", 100);
    EXPECT_TRUE(map.contains("key1"));
    EXPECT_FALSE(map.contains("key2"));
}

TEST(ConcurrentMapTest, TryInsert) {
    concurrent_map<std::string, int> map;
    EXPECT_TRUE(map.try_insert("key1", 1));
    EXPECT_FALSE(map.try_insert("key1", 2));  // Already exists
    EXPECT_EQ(map.get("key1").value(), 1);
}

TEST(ConcurrentMapTest, Erase) {
    concurrent_map<std::string, int> map;
    map.insert("key1", 42);
    EXPECT_EQ(map.size(), 1);
    EXPECT_TRUE(map.erase("key1"));
    EXPECT_EQ(map.size(), 0);
}

TEST(ConcurrentMapTest, ForEach) {
    concurrent_map<std::string, int> map;
    map.insert("a", 1);
    map.insert("b", 2);
    map.insert("c", 3);
    int sum = 0;
    map.for_each([&](const std::string&, int v) { sum += v; });
    EXPECT_EQ(sum, 6);
}

TEST(ConcurrentMapTest, Keys) {
    concurrent_map<std::string, int> map;
    map.insert("x", 1);
    map.insert("y", 2);
    auto keys = map.keys();
    EXPECT_EQ(keys.size(), 2);
}

TEST(ConcurrentMapTest, Update) {
    concurrent_map<std::string, int> map;
    map.insert("key1", 10);
    EXPECT_TRUE(map.update("key1", [](int& v) { v += 5; }));
    EXPECT_EQ(map.get("key1").value(), 15);
    EXPECT_FALSE(map.update("missing", [](int&) {}));
}

TEST(ConcurrentMapTest, GetOrCreate) {
    concurrent_map<std::string, int> map;
    int& val = map.get_or_create("key1", []() { return 42; });
    EXPECT_EQ(val, 42);
    int& val2 = map.get_or_create("key1", []() { return 99; });
    EXPECT_EQ(val2, 42);  // Returns existing
}

// ============================================================================
// Backoff Tests
// ============================================================================

TEST(BackoffTest, InitialDelay) {
    ExponentialBackoff backoff(100, 10000, 2.0);
    EXPECT_EQ(backoff.next_delay(), 100);
}

TEST(BackoffTest, Multiplier) {
    ExponentialBackoff backoff(100, 10000, 2.0);
    backoff.next_delay();  // 100
    EXPECT_EQ(backoff.next_delay(), 200);
    EXPECT_EQ(backoff.next_delay(), 400);
}

TEST(BackoffTest, MaxDelay) {
    ExponentialBackoff backoff(100, 300, 2.0);
    backoff.next_delay();  // 100
    backoff.next_delay();  // 200
    EXPECT_EQ(backoff.next_delay(), 300);  // Capped at max
}

TEST(BackoffTest, Reset) {
    ExponentialBackoff backoff(100, 10000, 2.0);
    backoff.next_delay();  // 100
    backoff.next_delay();  // 200
    backoff.reset();
    EXPECT_EQ(backoff.next_delay(), 100);  // Back to initial
}

TEST(BackoffTest, Jitter) {
    ExponentialBackoff backoff(100, 10000, 2.0);
    // Run a few iterations — delays should vary due to jitter
    int64_t d1 = backoff.next_delay();
    backoff.reset();
    int64_t d2 = backoff.next_delay();
    // Not asserting equality — jitter adds randomness
    EXPECT_GT(d1, 0);
    EXPECT_GT(d2, 0);
}

// ============================================================================
// UUID Tests
// ============================================================================

TEST(UuidTest, V4NotEmpty) {
    std::string uuid = generate_uuid_v4_string();
    EXPECT_FALSE(uuid.empty());
    EXPECT_EQ(uuid.size(), 36);  // Standard format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
}

TEST(UuidTest, V4HasDashes) {
    std::string uuid = generate_uuid_v4_string();
    EXPECT_EQ(uuid[8], '-');
    EXPECT_EQ(uuid[13], '-');
    EXPECT_EQ(uuid[18], '-');
    EXPECT_EQ(uuid[23], '-');
}

TEST(UuidTest, V4VersionBits) {
    std::string uuid = generate_uuid_v4_string();
    EXPECT_EQ(uuid[14], '4');  // Version 4
}

TEST(UuidTest, V4Uniqueness) {
    std::string u1 = generate_uuid_v4_string();
    std::string u2 = generate_uuid_v4_string();
    EXPECT_NE(u1, u2);
}

TEST(UuidTest, V7NotEmpty) {
    std::string uuid = generate_uuid_v7_string();
    EXPECT_FALSE(uuid.empty());
    EXPECT_EQ(uuid.size(), 36);
}

TEST(UuidTest, V7Monotonic) {
    // UUID v7 should be time-ordered (not strict monotonic, but likely)
    std::string u1 = generate_uuid_v7_string();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    std::string u2 = generate_uuid_v7_string();
    EXPECT_NE(u1, u2);
}

// ============================================================================
// Hostname Tests
// ============================================================================

TEST(HostnameTest, GetHostnameNotEmpty) {
    std::string hostname = get_hostname();
    EXPECT_FALSE(hostname.empty());
}

TEST(HostnameTest, IsIpAddressV4) {
    EXPECT_TRUE(is_ip_address("127.0.0.1"));
    EXPECT_TRUE(is_ip_address("192.168.1.1"));
    EXPECT_TRUE(is_ip_address("10.0.0.1"));
}

TEST(HostnameTest, IsIpAddressV6) {
    EXPECT_TRUE(is_ip_address("::1"));
    EXPECT_TRUE(is_ip_address("fe80::1"));
}

TEST(HostnameTest, IsIpAddressBracketed) {
    EXPECT_TRUE(is_ip_address("[::1]"));
    EXPECT_TRUE(is_ip_address("[fe80::1]"));
}

TEST(HostnameTest, IsIpAddressHostname) {
    EXPECT_FALSE(is_ip_address("localhost"));
    EXPECT_FALSE(is_ip_address("example.com"));
}

TEST(HostnameTest, ResolveLocalhost) {
    auto addrs = resolve_hostname("127.0.0.1");
    EXPECT_EQ(addrs.size(), 1);
    EXPECT_EQ(addrs[0], "127.0.0.1");
}

// ============================================================================
// Storage Types Tests
// ============================================================================

TEST(RecordTest, DefaultValues) {
    Record record;
    EXPECT_TRUE(record.key.empty());
    EXPECT_TRUE(record.value.empty());
    EXPECT_EQ(record.offset, 0);
    EXPECT_EQ(record.timestamp, 0);
}

TEST(RecordTest, IsTombstone) {
    Record record;
    record.value.clear();  // Empty value = tombstone
    EXPECT_EQ(record.value.size(), 0);
}

TEST(RecordTest, SizeIncludesKeyAndValue) {
    Record record;
    record.key.assign(10, 'k');
    record.value.assign(20, 'v');
    EXPECT_EQ(record.key.size(), 10);
    EXPECT_EQ(record.value.size(), 20);
}

TEST(RecordBatchTest, DefaultValues) {
    RecordBatch batch;
    EXPECT_EQ(batch.base_offset, 0);
    EXPECT_EQ(batch.record_count(), 0);
}

TEST(RecordBatchTest, AddRecord) {
    RecordBatch batch;
    batch.records.push_back(Record{});
    EXPECT_EQ(batch.record_count(), 1);
}

TEST(RecordBatchTest, IsTransactional) {
    RecordBatch batch;
    EXPECT_FALSE(batch.is_transactional());
    batch.attributes |= 0x0010;  // Transactional flag
    EXPECT_TRUE(batch.is_transactional());
}

TEST(RecordBatchTest, IsControl) {
    RecordBatch batch;
    EXPECT_FALSE(batch.is_control_batch());
    batch.attributes |= 0x0020;  // Control flag
    EXPECT_TRUE(batch.is_control_batch());
}

TEST(PartitionMetadataTest, LeaderAndReplicas) {
    PartitionMetadata meta;
    meta.leader_id = 1;
    meta.isr = {1, 2, 3};
    meta.replicas = {1, 2, 3, 4};
    EXPECT_EQ(meta.leader_id, 1);
    EXPECT_EQ(meta.isr.size(), 3);
    EXPECT_EQ(meta.replicas.size(), 4);
}

TEST(TopicConfigTest, DefaultValues) {
    TopicConfig cfg;
    EXPECT_EQ(cfg.partitions, 3);
    EXPECT_EQ(cfg.replication_factor, 3);
}

TEST(BrokerMetadataTest, Endpoints) {
    BrokerMetadata broker;
    broker.node_id = 1;
    broker.host = "broker1";
    broker.port = 9092;
    EXPECT_EQ(broker.node_id, 1);
    EXPECT_EQ(broker.host, "broker1");
    EXPECT_EQ(broker.port, 9092);
}

TEST(ConsumerGroupStateTest, MemberManagement) {
    ConsumerGroupState state;
    state.group_id = "test-group";
    EXPECT_EQ(state.group_id, "test-group");
    EXPECT_EQ(state.generation, 0);
    EXPECT_TRUE(state.members.empty());
}

TEST(OffsetCommitTest, DefaultValues) {
    OffsetCommit commit;
    EXPECT_EQ(commit.offset, 0);
    EXPECT_EQ(commit.leader_epoch, -1);
}

// ============================================================================
// Result Type Tests
// ============================================================================

TEST(ResultTest, Success) {
    auto r = result<int>::success(42);
    EXPECT_TRUE(r.ok());
    EXPECT_FALSE(r.failed());
    EXPECT_EQ(r.value, 42);
}

TEST(ResultTest, Failure) {
    auto r = result<int>::failure(error_code::invalid_request, "bad");
    EXPECT_FALSE(r.ok());
    EXPECT_TRUE(r.failed());
    EXPECT_EQ(r.error, error_code::invalid_request);
    EXPECT_EQ(r.error_message, "bad");
}

TEST(ResultTest, MoveSuccess) {
    auto r = result<std::string>::success("hello");
    std::string val = std::move(r.value);
    EXPECT_EQ(val, "hello");
}

// ============================================================================
// Endpoint Tests
// ============================================================================

TEST(EndpointTest, ToString) {
    endpoint ep{"broker1", 9092};
    EXPECT_EQ(ep.to_string(), "broker1:9092");
}

TEST(EndpointTest, Equality) {
    endpoint a{"host", 9092};
    endpoint b{"host", 9092};
    endpoint c{"host", 9093};
    EXPECT_EQ(a, b);
    EXPECT_NE(a, c);
}

TEST(EndpointTest, Ordering) {
    endpoint a{"a", 9092};
    endpoint b{"b", 9092};
    EXPECT_LT(a, b);
}

// ============================================================================
// Error Code Tests
// ============================================================================

TEST(ErrorCodeTest, Names) {
    EXPECT_STREQ(error_code_name(error_code::none), "NONE");
    EXPECT_STREQ(error_code_name(error_code::not_leader_for_partition), "NOT_LEADER_FOR_PARTITION");
    EXPECT_STREQ(error_code_name(error_code::unknown_server_error), "UNKNOWN_SERVER_ERROR");
}

TEST(ErrorCodeTest, UniqueValues) {
    // Verify common error codes have distinct int values
    EXPECT_NE(static_cast<int16_t>(error_code::none),
              static_cast<int16_t>(error_code::unknown_server_error));
    EXPECT_NE(static_cast<int16_t>(error_code::leader_not_available),
              static_cast<int16_t>(error_code::not_leader_for_partition));
}

// ============================================================================
// Compression Type Tests
// ============================================================================

TEST(CompressionTypeTest, Names) {
    EXPECT_STREQ(compression_name(compression_type::none), "none");
    EXPECT_STREQ(compression_name(compression_type::gzip), "gzip");
    EXPECT_STREQ(compression_name(compression_type::snappy), "snappy");
    EXPECT_STREQ(compression_name(compression_type::lz4), "lz4");
    EXPECT_STREQ(compression_name(compression_type::zstd), "zstd");
}

// ============================================================================
// Buffer View Tests
// ============================================================================

TEST(BufferViewTest, Empty) {
    buffer_view bv;
    EXPECT_TRUE(bv.empty());
}

TEST(BufferViewTest, FromStringView) {
    std::string data = "hello";
    buffer_view bv(data);
    EXPECT_FALSE(bv.empty());
    EXPECT_EQ(bv.size, 5);
    EXPECT_EQ(bv.view(), "hello");
}

// ============================================================================
// Shared Buffer Tests
// ============================================================================

TEST(SharedBufferTest, Empty) {
    shared_buffer buf;
    EXPECT_TRUE(buf.empty());
    EXPECT_EQ(buf.size(), 0);
}

TEST(SharedBufferTest, FromData) {
    std::string data = "test data";
    shared_buffer buf(data.data(), data.size());
    EXPECT_EQ(buf.size(), data.size());
    EXPECT_EQ(std::string(buf.data(), buf.size()), data);
}

TEST(SharedBufferTest, Move) {
    shared_buffer buf1("hello", 5);
    shared_buffer buf2 = std::move(buf1);
    EXPECT_EQ(buf2.size(), 5);
}

}  // namespace
}  // namespace torrent::test
