#include "torrent/common/types.h"
#include "torrent/common/config.h"
#include "torrent/common/endian.h"
#include "torrent/storage/types.h"
#include "torrent/network/protocol.h"
#include <gtest/gtest.h>
#include <cstring>
#include <vector>
#include <algorithm>
#include <chrono>
#include <thread>
#include <random>
#include <map>
#include <set>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <deque>
#include <optional>
#include <variant>
#include <functional>
#include <memory>

namespace torrent::test {
namespace {

// ============================================================================
// Unit tests for every public method in common/types.h
// ============================================================================
TEST(TypesFullTest, AllErrorCodesHaveNames) {
    for (int16_t i = -1; i <= 100; i++) {
        auto ec = static_cast<error_code>(i);
        const char* name = error_code_name(ec);
        EXPECT_NE(name, nullptr);
    }
}
TEST(TypesFullTest, ErrorCodeValues) {
    EXPECT_EQ(static_cast<int16_t>(error_code::none), 0);
    EXPECT_EQ(static_cast<int16_t>(error_code::unknown_server_error), -1);
    EXPECT_NE(static_cast<int16_t>(error_code::not_leader_for_partition),
              static_cast<int16_t>(error_code::leader_not_available));
}
TEST(TypesFullTest, ResultSuccessInt) {
    auto r = result<int>::success(42);
    EXPECT_TRUE(r.ok()); EXPECT_FALSE(r.failed());
    EXPECT_EQ(r.value, 42);
}
TEST(TypesFullTest, ResultFailureInt) {
    auto r = result<int>::failure(error_code::timeout, "timed out");
    EXPECT_FALSE(r.ok()); EXPECT_TRUE(r.failed());
    EXPECT_EQ(r.error, error_code::timeout);
}
TEST(TypesFullTest, ResultSuccessString) {
    auto r = result<std::string>::success("hello");
    EXPECT_EQ(r.value, "hello");
}
TEST(TypesFullTest, ResultFailureString) {
    auto r = result<std::string>::failure(error_code::invalid_request, "bad");
    EXPECT_EQ(r.error_message, "bad");
}
TEST(TypesFullTest, EndpointEquality) {
    endpoint a{"h", 9092}, b{"h", 9092}, c{"h", 9093};
    EXPECT_EQ(a, b); EXPECT_NE(a, c); EXPECT_LT(a, c);
}
TEST(TypesFullTest, EndpointToString) {
    EXPECT_EQ(endpoint{"broker1", 9092}.to_string(), "broker1:9092");
}
TEST(TypesFullTest, BufferViewEmpty) {
    buffer_view bv; EXPECT_TRUE(bv.empty());
}
TEST(TypesFullTest, BufferViewFromString) {
    std::string s = "data"; buffer_view bv(s);
    EXPECT_EQ(bv.size, 4); EXPECT_EQ(bv.view(), "data");
}
TEST(TypesFullTest, SharedBufferDefault) {
    shared_buffer sb; EXPECT_TRUE(sb.empty()); EXPECT_EQ(sb.size(), 0);
}
TEST(TypesFullTest, SharedBufferFromData) {
    shared_buffer sb("test", 4); EXPECT_EQ(sb.size(), 4);
    EXPECT_EQ(std::string(sb.data(), 4), "test");
}
TEST(TypesFullTest, SharedBufferMove) {
    shared_buffer sb1("hello", 5);
    shared_buffer sb2 = std::move(sb1);
    EXPECT_EQ(sb2.size(), 5);
}
TEST(TypesFullTest, CompressionTypeNames) {
    EXPECT_STREQ(compression_name(compression_type::none), "none");
    EXPECT_STREQ(compression_name(compression_type::gzip), "gzip");
    EXPECT_STREQ(compression_name(compression_type::snappy), "snappy");
    EXPECT_STREQ(compression_name(compression_type::lz4), "lz4");
    EXPECT_STREQ(compression_name(compression_type::zstd), "zstd");
}
TEST(TypesFullTest, ApiVersionConstants) {
    EXPECT_EQ(kApiVersionMajor, 0);
    EXPECT_EQ(kApiVersionMinor, 10);
    EXPECT_EQ(kApiVersionPatch, 0);
}
TEST(TypesFullTest, DefaultPorts) {
    EXPECT_EQ(kDefaultPort, 9092);
    EXPECT_EQ(kDefaultTlsPort, 9093);
    EXPECT_EQ(kDefaultAdminPort, 9644);
    EXPECT_EQ(kDefaultPrometheusPort, 9090);
}
TEST(TypesFullTest, SentinelValues) {
    EXPECT_EQ(kAnyBroker, -1);
    EXPECT_EQ(kNoBroker, -2);
    EXPECT_EQ(kInvalidOffset, -1);
    EXPECT_EQ(kEarliestOffset, -2);
    EXPECT_EQ(kLatestOffset, -1);
}
TEST(TypesFullTest, TypeAliases) {
    static_assert(std::is_same_v<broker_id_t, int32_t>);
    static_assert(std::is_same_v<topic_id_t, uint64_t>);
    static_assert(std::is_same_v<offset_t, int64_t>);
    static_assert(std::is_same_v<epoch_t, int64_t>);
    static_assert(std::is_same_v<term_t, int64_t>);
    static_assert(std::is_same_v<timestamp_ms_t, int64_t>);
    static_assert(std::is_same_v<producer_id_t, int64_t>);
    EXPECT_TRUE(true);
}

// ============================================================================
// Storage Types Full Tests
// ============================================================================
TEST(StorageFullTypes, RecordDefault) {
    Record r; EXPECT_TRUE(r.key.empty()); EXPECT_TRUE(r.value.empty());
}
TEST(StorageFullTypes, RecordWithData) {
    Record r; r.key = "k"; r.value = "v"; r.offset = 100; r.timestamp = 9999;
    EXPECT_EQ(r.key, "k"); EXPECT_EQ(r.value, "v");
    EXPECT_EQ(r.offset, 100); EXPECT_EQ(r.timestamp, 9999);
}
TEST(StorageFullTypes, RecordBatchDefault) {
    RecordBatch b; EXPECT_EQ(b.base_offset, 0); EXPECT_EQ(b.record_count(), 0);
}
TEST(StorageFullTypes, RecordBatchAttributesFlags) {
    RecordBatch b;
    b.attributes = 0x0001; EXPECT_EQ(b.attributes & 0x0007, 1);
    b.attributes = 0x0010; EXPECT_EQ(b.attributes & 0x0010, 0x0010);
    b.attributes = 0x0020; EXPECT_EQ(b.attributes & 0x0020, 0x0020);
}
TEST(StorageFullTypes, SegmentInfoDefaults) {
    SegmentInfo s; EXPECT_EQ(s.base_offset, 0); EXPECT_FALSE(s.is_sealed);
}
TEST(StorageFullTypes, SegmentInfoSealed) {
    SegmentInfo s; s.is_sealed = true; s.sealed_at_ms = 123;
    EXPECT_TRUE(s.is_sealed); EXPECT_EQ(s.sealed_at_ms, 123);
}
TEST(StorageFullTypes, PartitionStateWatermarks) {
    PartitionState ps;
    ps.high_watermark = 500; ps.last_stable_offset = 450;
    ps.log_start_offset = 100; ps.log_end_offset = 600;
    EXPECT_EQ(ps.high_watermark, 500); EXPECT_GT(ps.log_end_offset, ps.high_watermark);
}
TEST(StorageFullTypes, PartitionMetadataLeader) {
    PartitionMetadata pm; pm.leader_id = 3; pm.replicas = {1,2,3}; pm.isr = {1,2,3};
    EXPECT_EQ(pm.leader_id, 3); EXPECT_EQ(pm.isr.size(), 3);
}
TEST(StorageFullTypes, BrokerMetadataFields) {
    BrokerMetadata bm; bm.node_id = 5; bm.host = "h"; bm.port = 9092;
    bm.rack = "r1"; bm.is_controller = false;
    EXPECT_EQ(bm.node_id, 5); EXPECT_EQ(bm.host, "h"); EXPECT_EQ(bm.rack, "r1");
}
TEST(StorageFullTypes, TopicConfigDefaults) {
    TopicConfig tc; EXPECT_EQ(tc.partitions, 3); EXPECT_EQ(tc.replication_factor, 3);
}
TEST(StorageFullTypes, TopicConfigRetention) {
    TopicConfig tc; tc.retention_ms = 86400000; tc.retention_bytes = 1073741824;
    EXPECT_EQ(tc.retention_ms, 86400000);
}
TEST(StorageFullTypes, TopicMetadataStructure) {
    TopicMetadata tm; tm.name = "test"; tm.id = 1; tm.partitions = {0,1,2};
    EXPECT_EQ(tm.name, "test"); EXPECT_EQ(tm.partitions.size(), 3);
}
TEST(StorageFullTypes, ConsumerGroupStateMembers) {
    ConsumerGroupState cgs; cgs.group_id = "g1"; cgs.generation = 5;
    EXPECT_EQ(cgs.group_id, "g1"); EXPECT_EQ(cgs.generation, 5);
}
TEST(StorageFullTypes, GroupMemberFields) {
    GroupMember gm; gm.member_id = "m1"; gm.client_id = "c1";
    EXPECT_EQ(gm.member_id, "m1");
}
TEST(StorageFullTypes, OffsetCommitValues) {
    OffsetCommit oc; oc.offset = 999; oc.leader_epoch = 3; oc.metadata = "m";
    EXPECT_EQ(oc.offset, 999); EXPECT_EQ(oc.metadata, "m");
}
TEST(StorageFullTypes, ReplicaStatusInSync) {
    ReplicaStatus rs; rs.broker_id = 2; rs.is_in_sync = true; rs.lag = 0;
    EXPECT_TRUE(rs.is_in_sync);
}
TEST(StorageFullTypes, ReplicaStatusBehind) {
    ReplicaStatus rs; rs.is_in_sync = false; rs.lag = 5000;
    EXPECT_FALSE(rs.is_in_sync); EXPECT_GT(rs.lag, 0);
}
TEST(StorageFullTypes, LogAppendResultSuccess) {
    LogAppendResult lar; lar.base_offset = 42; lar.error = error_code::none;
    EXPECT_EQ(lar.base_offset, 42);
}
TEST(StorageFullTypes, LogAppendResultFailure) {
    LogAppendResult lar; lar.error = error_code::not_leader_for_partition;
    EXPECT_EQ(lar.error, error_code::not_leader_for_partition);
}
TEST(StorageFullTypes, FetchResultWatermarks) {
    FetchResult fr; fr.high_watermark = 100; fr.last_stable_offset = 95;
    EXPECT_EQ(fr.high_watermark, 100);
}

// ============================================================================
// Config Full Tests
// ============================================================================
TEST(ConfigFullTest, AllDefaultsSet) {
    config cfg;
    EXPECT_EQ(cfg.broker_id(), 0); EXPECT_EQ(cfg.port(), 9092);
    EXPECT_EQ(cfg.admin_port(), 9644); EXPECT_EQ(cfg.metrics_port(), 9090);
    EXPECT_EQ(cfg.default_partitions(), 1); EXPECT_EQ(cfg.default_replication_factor(), 3);
    EXPECT_EQ(cfg.num_io_threads(), 4); EXPECT_EQ(cfg.num_worker_threads(), 8);
    EXPECT_EQ(cfg.max_connections(), 65536);
}
TEST(ConfigFullTest, GetOrFallback) {
    config cfg;
    EXPECT_EQ(cfg.get_or("missing", "def"), "def");
    EXPECT_EQ(cfg.get_or("broker.id", "99"), "0");
}
TEST(ConfigFullTest, SetAndHas) {
    config cfg; cfg.set("custom", "val");
    EXPECT_TRUE(cfg.has("custom")); EXPECT_EQ(cfg.get_or("custom", ""), "val");
}
TEST(ConfigFullTest, TypedGettersAll) {
    config cfg;
    cfg.set("int.k", "42"); cfg.set("bool.k", "true"); cfg.set("str.k", "hi");
    EXPECT_EQ(cfg.get_as<int64_t>("int.k").value(), 42);
    EXPECT_EQ(cfg.get_as<bool>("bool.k").value(), true);
    EXPECT_EQ(cfg.get_as<std::string>("str.k").value(), "hi");
}
TEST(ConfigFullTest, SeedServersMultiple) {
    config cfg; cfg.set("cluster.seed_servers", "a:1,b:2,c:3");
    EXPECT_EQ(cfg.seed_servers().size(), 3);
}
TEST(ConfigFullTest, SegmentDefaults) {
    config cfg;
    EXPECT_EQ(cfg.segment_max_bytes(), 1073741824);
    EXPECT_EQ(cfg.segment_max_age_ms(), 604800000);
    EXPECT_EQ(cfg.retention_bytes(), -1);
    EXPECT_EQ(cfg.retention_ms(), 604800000);
}
TEST(ConfigFullTest, RaftDefaults) {
    config cfg;
    EXPECT_EQ(cfg.raft_heartbeat_ms(), 150);
    EXPECT_EQ(cfg.raft_election_timeout_min_ms(), 150);
    EXPECT_EQ(cfg.raft_election_timeout_max_ms(), 300);
}
TEST(ConfigFullTest, SecurityDefaults) {
    config cfg;
    EXPECT_FALSE(cfg.enable_sasl()); EXPECT_FALSE(cfg.enable_tls());
}
TEST(ConfigFullTest, TransactionDefaults) {
    config cfg;
    EXPECT_TRUE(cfg.enable_transactions());
    EXPECT_EQ(cfg.transaction_timeout_ms(), 60000);
}

// ============================================================================
// Endian Full Tests
// ============================================================================
TEST(EndianFullTest, AllConversions) {
    EXPECT_EQ(be_to_host16(host_to_be16(0x1234)), 0x1234);
    EXPECT_EQ(be_to_host32(host_to_be32(0x12345678)), 0x12345678);
    EXPECT_EQ(be_to_host64(host_to_be64(0x123456789ABCDEF0ULL)), 0x123456789ABCDEF0ULL);
    EXPECT_EQ(le_to_host16(host_to_le16(0xABCD)), 0xABCD);
    EXPECT_EQ(le_to_host32(host_to_le32(0xDEADBEEF)), 0xDEADBEEF);
}
TEST(EndianFullTest, ZigZagAll) {
    for (int32_t v : {0, 1, -1, 42, -42, INT32_MAX, INT32_MIN}) {
        EXPECT_EQ(zigzag_decode_32(zigzag_encode_32(v)), v);
    }
}
TEST(EndianFullTest, VarintBoundaries) {
    for (uint32_t v : {0u, 127u, 128u, 16383u, 16384u, 2097151u, 2097152u, UINT32_MAX}) {
        uint8_t buf[5];
        size_t sz = varint_encode_32(v, buf);
        uint32_t out; size_t read = varint_decode_32(buf, sz, out);
        EXPECT_EQ(out, v); EXPECT_EQ(read, sz);
    }
}
TEST(EndianFullTest, WriteReadInts) {
    uint8_t b2[2]; write_int16_be(b2, -100); EXPECT_EQ(read_int16_be(b2), -100);
    uint8_t b4[4]; write_int32_be(b4, -99999); EXPECT_EQ(read_int32_be(b4), -99999);
    uint8_t b8[8]; write_int64_be(b8, -9999999999LL);
    EXPECT_EQ(read_int64_be(b8), -9999999999LL);
}

// ============================================================================
// Protocol Full Tests
// ============================================================================
using namespace torrent::protocol;
TEST(ProtocolFullTest, AllApiKeyNames) {
    for (int16_t k = 0; k <= 51; k++) {
        const char* name = api_key_name(k);
        EXPECT_NE(name, nullptr); EXPECT_GT(strlen(name), 2);
    }
}
TEST(ProtocolFullTest, TorrentApiNames) {
    for (int16_t k = 60; k <= 67; k++) {
        EXPECT_GT(strlen(api_key_name(k)), 2);
    }
}
TEST(ProtocolFullTest, KafkaApiDetection) {
    EXPECT_TRUE(is_kafka_api(0)); EXPECT_TRUE(is_kafka_api(51));
    EXPECT_FALSE(is_kafka_api(60));
}
TEST(ProtocolFullTest, TorrentApiDetection) {
    EXPECT_TRUE(is_torrent_native_api(60)); EXPECT_FALSE(is_torrent_native_api(0));
}
TEST(ProtocolFullTest, FrameHelpers) {
    EXPECT_EQ(frame_total_size(100), 104);
    EXPECT_TRUE(is_valid_payload_size(100));
    EXPECT_FALSE(is_valid_payload_size(kMaxFramePayloadSize + 1));
}
TEST(ProtocolFullTest, RequestResponseHeaders) {
    RequestHeader rh; rh.api_key = 3; rh.api_version = 5; rh.correlation_id = 42;
    EXPECT_EQ(rh.api_key, 3); EXPECT_EQ(rh.correlation_id, 42);
    ResponseHeader rsh; rsh.correlation_id = 42;
    EXPECT_EQ(rsh.correlation_id, 42);
}
TEST(ProtocolFullTest, AllRequestStructsConstruct) {
    ProduceRequest pr; FetchRequest fr; MetadataRequest mr;
    JoinGroupRequest jgr; SyncGroupRequest sgr; HeartbeatRequest hbr;
    LeaveGroupRequest lgr; OffsetCommitRequest ocr;
    EXPECT_TRUE(true);
}
TEST(ProtocolFullTest, ApiVersionRange) {
    ApiVersionRange avr; avr.api_key = 0; avr.min_version = 0; avr.max_version = 9;
    EXPECT_EQ(avr.max_version, 9);
}
TEST(ProtocolFullTest, ErrorCodeEnum) {
    EXPECT_EQ(static_cast<int>(ErrorCode::NONE), 0);
    EXPECT_EQ(static_cast<int>(ErrorCode::NOT_LEADER_FOR_PARTITION), 6);
    EXPECT_EQ(static_cast<int>(ErrorCode::TOPIC_ALREADY_EXISTS), 36);
    EXPECT_EQ(static_cast<int>(ErrorCode::PRODUCER_FENCED), 90);
}
TEST(ProtocolFullTest, TopicPartitionStruct) {
    TopicPartition tp; tp.topic = "test"; tp.partition = 5;
    EXPECT_EQ(tp.topic, "test");
}
TEST(ProtocolFullTest, RecordHeaderStruct) {
    RecordHeader rh; rh.key = "k"; rh.value = "v";
    EXPECT_EQ(rh.key, "k"); EXPECT_EQ(rh.value, "v");
}

// ============================================================================
// MessageCodec Full Tests
// ============================================================================
TEST(MessageCodecFull, EncodeDecodeRequest) {
    auto frame = MessageCodec::encode_request(3, 5, 123, "client", buffer_view("body", 4));
    EXPECT_GT(frame.size(), 20);
    int32_t sz = MessageCodec::peek_frame_size(frame.data(), frame.size());
    EXPECT_EQ(sz, static_cast<int32_t>(frame.size() - 4));
    int16_t ak, av; int32_t cid;
    auto rem = MessageCodec::decode_request_header(frame.data()+4, frame.size()-4, ak, av, cid);
    EXPECT_EQ(ak, 3); EXPECT_EQ(av, 5); EXPECT_EQ(cid, 123);
}
TEST(MessageCodecFull, EncodeDecodeResponse) {
    auto frame = MessageCodec::encode_response(42, buffer_view("resp", 4));
    EXPECT_GT(frame.size(), 8);
    int32_t cid = MessageCodec::peek_correlation_id(frame.data()+4, frame.size()-4);
    EXPECT_EQ(cid, 42);
}
TEST(MessageCodecFull, PeekFrameSizeInvalid) {
    uint8_t buf[2] = {0}; EXPECT_EQ(MessageCodec::peek_frame_size(buf, 2), -1);
}

}  // namespace
}  // namespace torrent::test
