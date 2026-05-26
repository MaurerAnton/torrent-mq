#include "torrent/common/types.h"
#include "torrent/storage/segment.h"
#include "torrent/storage/log_manager.h"
#include "torrent/storage/compaction.h"
#include "torrent/storage/recovery.h"
#include "torrent/storage/disk_io.h"
#include "torrent/network/protocol.h"
#include "torrent/network/transport.h"
#include "torrent/consensus/raft.h"
#include "torrent/broker/server.h"
#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <random>
#include <fstream>
#include <filesystem>

namespace torrent::test {
namespace {

// ============================================================================
// Real Integration Tests with actual implementations
// ============================================================================
class RealIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override { dir_ = "/tmp/torrent_real_" + std::to_string(getpid()); std::filesystem::create_directories(dir_); }
    void TearDown() override { std::filesystem::remove_all(dir_); }
    std::string dir_;
};

TEST_F(RealIntegrationTest, SegmentWriteAndReadBack) {
    std::string path = dir_ + "/test.log";
    SegmentConfig cfg; cfg.file_path = path; cfg.base_offset = 0; cfg.max_bytes = 1048576;
    Segment seg(cfg);
    auto open_res = seg.open();
    ASSERT_TRUE(open_res.ok()) << open_res.error_message;

    RecordBatch batch; batch.base_offset = 0;
    Record r; r.key = "test-key"; r.value = "test-value"; r.timestamp = 1234567890000;
    batch.records.push_back(r);

    auto append_res = seg.append(batch);
    ASSERT_TRUE(append_res.ok());
    EXPECT_EQ(append_res.value, 0);

    seg.flush(); seg.close();

    Segment seg2(cfg);
    seg2.open();
    auto read_res = seg2.read(0, 1024);
    ASSERT_TRUE(read_res.ok());
    EXPECT_GT(read_res.value.size(), 0u);
    seg2.close();
}

TEST_F(RealIntegrationTest, VarintRoundTripAllSizes) {
    for (uint32_t v : {0u, 1u, 127u, 128u, 255u, 16383u, 16384u, 2097151u, 2097152u, 268435455u, 268435456u, UINT32_MAX}) {
        uint8_t buf[5]; size_t sz = varint_encode_32(v, buf);
        uint32_t out; size_t rs = varint_decode_32(buf, sz, out);
        ASSERT_EQ(out, v) << "Failed for value " << v;
        ASSERT_EQ(rs, sz);
    }
}

TEST_F(RealIntegrationTest, ZigZagRoundTripAllSizes) {
    for (int64_t v : {0LL, 1LL, -1LL, 42LL, -42LL, 1000000LL, -1000000LL, INT64_MAX, INT64_MIN}) {
        ASSERT_EQ(zigzag_decode_64(zigzag_encode_64(v)), v);
    }
}

TEST_F(RealIntegrationTest, EndianRoundTripAll) {
    EXPECT_EQ(be_to_host16(host_to_be16(0xABCD)), 0xABCD);
    EXPECT_EQ(be_to_host32(host_to_be32(0x12345678)), 0x12345678);
    EXPECT_EQ(be_to_host64(host_to_be64(0x123456789ABCDEF0ULL)), 0x123456789ABCDEF0ULL);
    EXPECT_EQ(le_to_host32(host_to_le32(0xDEADBEEF)), 0xDEADBEEF);
}

TEST_F(RealIntegrationTest, DiskIOWriteAndRead) {
    std::string path = dir_ + "/io_test.bin";
    int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);
    const char* data = "disk io integration test data";
    auto w = DiskIO::write_all(fd, data, strlen(data), 0);
    ASSERT_TRUE(w.ok());
    char buf[100] = {};
    auto r = DiskIO::read_all(fd, buf, strlen(data), 0);
    ASSERT_TRUE(r.ok());
    EXPECT_STREQ(buf, data);
    auto sz = DiskIO::file_size(fd);
    ASSERT_TRUE(sz.ok());
    EXPECT_EQ(sz.value, static_cast<int64_t>(strlen(data)));
    DiskIO::fsync(fd); close(fd);
}

TEST_F(RealIntegrationTest, DiskIOTruncation) {
    std::string path = dir_ + "/trunc_test.bin";
    int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);
    DiskIO::write_all(fd, "1234567890", 10, 0);
    DiskIO::ftruncate(fd, 5);
    auto sz = DiskIO::file_size(fd);
    EXPECT_EQ(sz.value, 5);
    close(fd);
}

TEST_F(RealIntegrationTest, ProtocolSerialization) {
    auto frame = MessageCodec::encode_request(3, 5, 12345, "test-client", buffer_view("body", 4));
    ASSERT_GT(frame.size(), 4u);
    int32_t fsz = MessageCodec::peek_frame_size(frame.data(), frame.size());
    EXPECT_EQ(fsz, static_cast<int32_t>(frame.size() - 4));
    int16_t ak, av; int32_t cid;
    MessageCodec::decode_request_header(frame.data() + 4, frame.size() - 4, ak, av, cid);
    EXPECT_EQ(ak, 3); EXPECT_EQ(av, 5); EXPECT_EQ(cid, 12345);
}

TEST_F(RealIntegrationTest, ApiKeyCoverage) {
    for (int16_t k = 0; k <= 51; k++) {
        const char* n = torrent::protocol::api_key_name(k);
        ASSERT_NE(n, nullptr); ASSERT_GT(strlen(n), 2u);
    }
    for (int16_t k = 60; k <= 67; k++) {
        ASSERT_GT(strlen(torrent::protocol::api_key_name(k)), 2u);
    }
}

TEST_F(RealIntegrationTest, ErrorCodeCoverage) {
    for (int16_t e = -1; e <= 100; e++) {
        auto ec = static_cast<torrent::error_code>(e);
        ASSERT_NE(torrent::error_code_name(ec), nullptr);
    }
}

TEST_F(RealIntegrationTest, ConfigAllDefaults) {
    torrent::config cfg;
    EXPECT_EQ(cfg.broker_id(), 0);
    EXPECT_EQ(cfg.port(), 9092);
    EXPECT_EQ(cfg.admin_port(), 9644);
    EXPECT_EQ(cfg.metrics_port(), 9090);
}

TEST_F(RealIntegrationTest, ThreadPoolSubmitAndWait) {
    torrent::thread_pool pool(4);
    std::atomic<int> c{0};
    for (int i = 0; i < 100; i++) pool.submit([&]() { c.fetch_add(1); });
    pool.shutdown();
    EXPECT_EQ(c.load(), 100);
}

TEST_F(RealIntegrationTest, ConcurrentMapThreadSafety) {
    torrent::concurrent_map<int, int> map;
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < 250; i++) map.insert(t * 1000 + i, t * 1000 + i);
        });
    }
    for (auto& th : threads) th.join();
    EXPECT_EQ(map.size(), 1000u);
}

TEST_F(RealIntegrationTest, LRUCacheEviction) {
    torrent::lru_cache<int, std::string> cache(3);
    cache.put(1, "one"); cache.put(2, "two"); cache.put(3, "three");
    cache.put(4, "four");
    EXPECT_FALSE(cache.get(1).has_value());
    EXPECT_TRUE(cache.get(2).has_value());
    EXPECT_TRUE(cache.get(3).has_value());
    EXPECT_TRUE(cache.get(4).has_value());
}

TEST_F(RealIntegrationTest, RateLimiterTokenBucket) {
    torrent::rate_limiter rl(1000, 100, 100);
    EXPECT_TRUE(rl.try_consume(50));
    EXPECT_TRUE(rl.try_consume(50));
    EXPECT_FALSE(rl.try_consume(1));
    rl.refill(50);
    EXPECT_TRUE(rl.try_consume(25));
}

TEST_F(RealIntegrationTest, SPSCQueueFifo) {
    torrent::spsc_queue<int> q(8);
    for (int i = 0; i < 5; i++) ASSERT_TRUE(q.try_push(i));
    for (int i = 0; i < 5; i++) {
        auto v = q.try_pop(); ASSERT_TRUE(v.has_value());
        EXPECT_EQ(v.value(), i);
    }
    EXPECT_FALSE(q.try_pop().has_value());
}

}  // namespace
}  // namespace torrent::test
