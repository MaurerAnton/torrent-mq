#include "torrent/common/types.h"
#include "torrent/common/config.h"
#include "torrent/common/endian.h"
#include "torrent/storage/types.h"
#include "torrent/network/protocol.h"
#include "torrent/network/message_codec.h"
#include "torrent/network/transport.h"
#include "torrent/consensus/raft_types.h"
#include "torrent/broker/server.h"
#include <gtest/gtest.h>
#include <cstring>
#include <vector>
#include <algorithm>
#include <chrono>
#include <thread>
#include <atomic>
#include <random>
#include <map>
#include <deque>

namespace torrent::test {
namespace {

using namespace torrent::protocol;

// ============================================================================
// MessageCodec Comprehensive Tests
// ============================================================================
class MessageCodecTest : public ::testing::Test {
protected:
    void SetUp() override { srand(static_cast<unsigned>(time(nullptr))); }
};

TEST_F(MessageCodecTest, EncodeRequestBasic) {
    auto frame = MessageCodec::encode_request(3, 4, 100, "client1", buffer_view("body", 4));
    ASSERT_GT(frame.size(), 4u);
    int32_t sz = MessageCodec::peek_frame_size(frame.data(), frame.size());
    EXPECT_EQ(sz, static_cast<int32_t>(frame.size() - 4));
}
TEST_F(MessageCodecTest, EncodeRequestEmptyBody) {
    auto frame = MessageCodec::encode_request(0, 0, 0, "", buffer_view("", 0));
    ASSERT_GT(frame.size(), 4u);
}
TEST_F(MessageCodecTest, EncodeRequestEmptyClientId) {
    auto frame = MessageCodec::encode_request(1, 1, 1, "", buffer_view("x", 1));
    ASSERT_GT(frame.size(), 4u);
}
TEST_F(MessageCodecTest, EncodeRequestMaxApiKey) {
    auto frame = MessageCodec::encode_request(67, 0, 99999, "c", buffer_view("", 0));
    ASSERT_GT(frame.size(), 4u);
}
TEST_F(MessageCodecTest, EncodeResponseBasic) {
    auto frame = MessageCodec::encode_response(42, buffer_view("response", 8));
    ASSERT_GT(frame.size(), 4u);
    int32_t cid = MessageCodec::peek_correlation_id(frame.data() + 4, 4);
    EXPECT_EQ(cid, 42);
}
TEST_F(MessageCodecTest, EncodeResponseEmptyBody) {
    auto frame = MessageCodec::encode_response(0, buffer_view("", 0));
    ASSERT_GT(frame.size(), 4u);
}
TEST_F(MessageCodecTest, DecodeRequestHeaderValid) {
    auto frame = MessageCodec::encode_request(18, 2, 7777, "test", buffer_view("hi", 2));
    int16_t ak = 0, av = 0; int32_t cid = 0;
    auto rem = MessageCodec::decode_request_header(frame.data() + 4, frame.size() - 4, ak, av, cid);
    EXPECT_EQ(ak, 18); EXPECT_EQ(av, 2); EXPECT_EQ(cid, 7777);
}
TEST_F(MessageCodecTest, PeekFrameSizeCorrect) {
    auto frame = MessageCodec::encode_request(0, 0, 0, "", buffer_view("data", 4));
    int32_t sz = MessageCodec::peek_frame_size(frame.data(), 4);
    EXPECT_EQ(sz, static_cast<int32_t>(frame.size()) - 4);
}
TEST_F(MessageCodecTest, PeekFrameSizeInsufficient) {
    uint8_t buf[2] = {0, 0};
    EXPECT_EQ(MessageCodec::peek_frame_size(buf, 2), -1);
}
TEST_F(MessageCodecTest, PeekFrameSizeNullData) {
    EXPECT_EQ(MessageCodec::peek_frame_size(nullptr, 0), -1);
}
TEST_F(MessageCodecTest, PeekCorrelationIdValid) {
    uint8_t buf[8]; write_int32_be(buf, 100); write_int32_be(buf + 4, 5555);
    EXPECT_EQ(MessageCodec::peek_correlation_id(buf + 4, 4), 5555);
}
TEST_F(MessageCodecTest, PeekCorrelationIdInsufficient) {
    uint8_t buf[2] = {0, 0};
    EXPECT_EQ(MessageCodec::peek_correlation_id(buf, 2), -1);
}
TEST_F(MessageCodecTest, FrameRoundTrip) {
    for (int16_t api : {0, 1, 3, 11, 18, 60}) {
        for (int16_t ver : {0, 1, 5, 9}) {
            for (int32_t cid : {0, 1, 99999}) {
                std::string body_data("test-body-data-" + std::to_string(api));
                auto frame = MessageCodec::encode_request(api, ver, cid, "cli", buffer_view(body_data));
                int16_t ak, av; int32_t corr;
                auto rem = MessageCodec::decode_request_header(frame.data() + 4, frame.size() - 4, ak, av, corr);
                EXPECT_EQ(ak, api) << "api=" << api << " ver=" << ver;
                EXPECT_EQ(av, ver);
                EXPECT_EQ(corr, cid);
            }
        }
    }
}

// ============================================================================
// Wire Format Property Tests
// ============================================================================
TEST(WireFormatProperty, SerializeDeserializeInt16) {
    for (int16_t v : {INT16_MIN, INT16_MIN+1, -100, -1, 0, 1, 100, INT16_MAX-1, INT16_MAX}) {
        uint8_t b[2]; write_int16_be(b, v);
        EXPECT_EQ(read_int16_be(b), v) << "int16 value: " << v;
    }
}
TEST(WireFormatProperty, SerializeDeserializeInt32) {
    for (int32_t v : {INT32_MIN, -1000000, -1, 0, 1, 1000000, INT32_MAX}) {
        uint8_t b[4]; write_int32_be(b, v);
        EXPECT_EQ(read_int32_be(b), v) << "int32 value: " << v;
    }
}
TEST(WireFormatProperty, SerializeDeserializeInt64) {
    for (int64_t v : {INT64_MIN, -1000000000000LL, -1LL, 0LL, 1LL, 1000000000000LL, INT64_MAX}) {
        uint8_t b[8]; write_int64_be(b, v);
        EXPECT_EQ(read_int64_be(b), v) << "int64 value: " << v;
    }
}
TEST(WireFormatProperty, Varint32AllPowersOfTwo) {
    for (uint32_t shift = 0; shift < 32; shift++) {
        uint32_t v = 1u << shift;
        uint8_t b[5]; size_t sz = varint_encode_32(v, b);
        uint32_t o; size_t rs = varint_decode_32(b, sz, o);
        EXPECT_EQ(o, v) << "2^" << shift;
        EXPECT_GE(sz, 1u);
        EXPECT_LE(sz, 5u);
    }
}
TEST(WireFormatProperty, Varint64AllPowersOfTwo) {
    for (uint32_t shift = 0; shift < 63; shift++) {
        uint64_t v = 1ULL << shift;
        uint8_t b[10]; size_t sz = varint_encode_64(v, b);
        uint64_t o; size_t rs = varint_decode_64(b, sz, o);
        EXPECT_EQ(o, v) << "2^" << shift;
    }
}
TEST(WireFormatProperty, ZigZag32Symmetry) {
    std::vector<int32_t> vals = {0, 1, -1, 2, -2, 42, -42, 1000, -1000, INT32_MAX, INT32_MIN};
    for (auto v : vals) EXPECT_EQ(zigzag_decode_32(zigzag_encode_32(v)), v);
}
TEST(WireFormatProperty, ZigZag64Symmetry) {
    std::vector<int64_t> vals = {0LL, 1LL, -1LL, INT64_MAX, INT64_MIN};
    for (auto v : vals) EXPECT_EQ(zigzag_decode_64(zigzag_encode_64(v)), v);
}
TEST(WireFormatProperty, VarintEncodedSize) {
    EXPECT_EQ(varint_encode_32(0, (uint8_t[5]){}), 1u);
    EXPECT_EQ(varint_encode_32(127, (uint8_t[5]){}), 1u);
    EXPECT_EQ(varint_encode_32(128, (uint8_t[5]){}), 2u);
    EXPECT_EQ(varint_encode_32(16383, (uint8_t[5]){}), 2u);
    EXPECT_EQ(varint_encode_32(16384, (uint8_t[5]){}), 3u);
    EXPECT_EQ(varint_encode_32(2097151, (uint8_t[5]){}), 3u);
    EXPECT_EQ(varint_encode_32(2097152, (uint8_t[5]){}), 4u);
    EXPECT_EQ(varint_encode_32(UINT32_MAX, (uint8_t[5]){}), 5u);
}

// ============================================================================
// API Key Completeness Tests
// ============================================================================
TEST(ApiKeyTest, AllKafkaApiKeysHaveNames) {
    for (int16_t k = 0; k <= 51; k++) {
        const char* n = api_key_name(k);
        ASSERT_NE(n, nullptr) << "Key " << k;
        ASSERT_GT(strlen(n), 2u) << "Key " << k << " name=" << n;
    }
}
TEST(ApiKeyTest, AllTorrentApiKeysHaveNames) {
    for (int16_t k = 60; k <= 67; k++) {
        const char* n = api_key_name(k);
        ASSERT_NE(n, nullptr) << "Key " << k;
        ASSERT_GT(strlen(n), 2u) << "Key " << k;
    }
}
TEST(ApiKeyTest, SpecificNames) {
    EXPECT_STREQ(api_key_name(0), "Produce");
    EXPECT_STREQ(api_key_name(1), "Fetch");
    EXPECT_STREQ(api_key_name(3), "Metadata");
    EXPECT_STREQ(api_key_name(8), "OffsetCommit");
    EXPECT_STREQ(api_key_name(11), "JoinGroup");
    EXPECT_STREQ(api_key_name(14), "SyncGroup");
    EXPECT_STREQ(api_key_name(17), "SaslHandshake");
    EXPECT_STREQ(api_key_name(18), "ApiVersions");
    EXPECT_STREQ(api_key_name(19), "CreateTopics");
    EXPECT_STREQ(api_key_name(26), "EndTxn");
    EXPECT_STREQ(api_key_name(36), "SaslAuthenticate");
    EXPECT_STREQ(api_key_name(43), "ElectLeaders");
    EXPECT_STREQ(api_key_name(60), "TorrentFetch");
    EXPECT_STREQ(api_key_name(64), "TorrentHealthCheck");
}
TEST(ApiKeyTest, KafkaApiClassification) {
    for (int16_t k = 0; k <= 51; k++) EXPECT_TRUE(is_kafka_api(k));
    for (int16_t k = 52; k <= 59; k++) EXPECT_FALSE(is_kafka_api(k));
}
TEST(ApiKeyTest, TorrentApiClassification) {
    for (int16_t k = 60; k <= 67; k++) EXPECT_TRUE(is_torrent_native_api(k));
    for (int16_t k = 0; k <= 51; k++) EXPECT_FALSE(is_torrent_native_api(k));
}
TEST(ApiKeyTest, ErrorCodeNameCoverage) {
    for (int16_t e = -1; e <= 100; e++) {
        auto ec = static_cast<torrent::error_code>(e);
        const char* name = torrent::error_code_name(ec);
        EXPECT_NE(name, nullptr) << "Error code " << e;
    }
}
TEST(ApiKeyTest, ProtocolConstants) {
    EXPECT_EQ(kMinApiVersion, 0);
    EXPECT_GT(kMaxApiVersion, 0);
    EXPECT_EQ(kDefaultApiVersion, 0);
    EXPECT_EQ(kFrameLengthSize, 4);
    EXPECT_GT(kMaxFramePayloadSize, 0);
}

// ============================================================================
// Types Combinatorial Tests
// ============================================================================
TEST(TypesCombo, AllCompressionNames) {
    std::set<std::string> names;
    for (auto ct : {compression_type::none, compression_type::gzip,
                     compression_type::snappy, compression_type::lz4,
                     compression_type::zstd}) {
        std::string name = compression_name(ct);
        EXPECT_GT(name.size(), 1u);
        names.insert(name);
    }
    EXPECT_EQ(names.size(), 5u);
}
TEST(TypesCombo, AllPortsValid) {
    EXPECT_GE(kDefaultPort, 1024); EXPECT_LE(kDefaultPort, 65535);
    EXPECT_GE(kDefaultTlsPort, 1024); EXPECT_LE(kDefaultTlsPort, 65535);
}
TEST(TypesCombo, BufferViewOperations) {
    buffer_view empty; EXPECT_TRUE(empty.empty());
    buffer_view fromStr(std::string_view("hello"));
    EXPECT_EQ(fromStr.view(), "hello");
    buffer_view fromPtr("world", 5);
    EXPECT_EQ(fromPtr.view(), "world");
}
TEST(TypesCombo, SharedBufferOperations) {
    shared_buffer sb; EXPECT_TRUE(sb.empty());
    shared_buffer sb2("data", 4); EXPECT_EQ(sb2.size(), 4u);
    shared_buffer sb3 = std::move(sb2);
    EXPECT_EQ(sb3.size(), 4u);
}
TEST(TypesCombo, EndpointOperations) {
    endpoint a{"a", 9092}, b{"b", 9092}, c{"a", 9093};
    EXPECT_LT(a, b); EXPECT_NE(a, c); EXPECT_EQ(a, a);
    EXPECT_EQ(a.to_string(), "a:9092");
}
TEST(TypesCombo, ResultTemplate) {
    auto ok = result<int>::success(42);
    EXPECT_TRUE(ok.ok()); EXPECT_EQ(ok.value, 42);
    auto err = result<int>::failure(error_code::timeout, "msg");
    EXPECT_TRUE(err.failed()); EXPECT_EQ(err.error, error_code::timeout);
}
TEST(TypesCombo, ApiVersions) {
    EXPECT_GE(kApiVersionMajor, 0);
    EXPECT_GE(kApiVersionMinor, 0);
    EXPECT_GE(kApiVersionPatch, 0);
}

// ============================================================================
// Config Extended Tests
// ============================================================================
TEST(ConfigExtended, AllAccessors) {
    config cfg;
    EXPECT_NO_THROW(cfg.broker_id());
    EXPECT_NO_THROW(cfg.port());
    EXPECT_NO_THROW(cfg.tls_port());
    EXPECT_NO_THROW(cfg.admin_port());
    EXPECT_NO_THROW(cfg.metrics_port());
    EXPECT_NO_THROW(cfg.log_dir());
    EXPECT_NO_THROW(cfg.num_io_threads());
    EXPECT_NO_THROW(cfg.num_worker_threads());
    EXPECT_NO_THROW(cfg.default_replication_factor());
    EXPECT_NO_THROW(cfg.default_partitions());
    EXPECT_NO_THROW(cfg.segment_max_bytes());
    EXPECT_NO_THROW(cfg.retention_bytes());
    EXPECT_NO_THROW(cfg.retention_ms());
    EXPECT_NO_THROW(cfg.raft_heartbeat_ms());
    EXPECT_NO_THROW(cfg.enable_sasl());
    EXPECT_NO_THROW(cfg.enable_tls());
    EXPECT_NO_THROW(cfg.enable_transactions());
    EXPECT_NO_THROW(cfg.auto_create_topics());
}
TEST(ConfigExtended, SetAndGetCycle) {
    config cfg;
    cfg.set("a", "1"); EXPECT_EQ(cfg.get_or("a", ""), "1");
    cfg.set("a", "2"); EXPECT_EQ(cfg.get_or("a", ""), "2");
    EXPECT_TRUE(cfg.has("a"));
}
TEST(ConfigExtended, TypedGetAll) {
    config cfg;
    cfg.set("int", "42"); cfg.set("bool", "1"); cfg.set("str", "hello");
    EXPECT_EQ(cfg.get_as<int64_t>("int").value(), 42);
    EXPECT_TRUE(cfg.get_as<bool>("bool").value());
    EXPECT_EQ(cfg.get_as<std::string>("str").value(), "hello");
    EXPECT_FALSE(cfg.get_as<int64_t>("missing").has_value());
}
TEST(ConfigExtended, SeedServersVarious) {
    config cfg;
    cfg.set("cluster.seed_servers", "a:1"); EXPECT_EQ(cfg.seed_servers().size(), 1u);
    cfg.set("cluster.seed_servers", ""); EXPECT_TRUE(cfg.seed_servers().empty());
    cfg.set("cluster.seed_servers", "a:1,b:2,c:3,d:4,e:5"); EXPECT_EQ(cfg.seed_servers().size(), 5u);
}
TEST(ConfigExtended, JsonOutput) {
    config cfg; cfg.set("x.y", "z");
    auto j = cfg.to_json();
    EXPECT_TRUE(j.is_object());
}
TEST(ConfigExtended, ToString) {
    config cfg;
    auto s = cfg.to_string();
    EXPECT_GT(s.size(), 0u);
}

// ============================================================================
// Raft Types Tests
// ============================================================================
TEST(RaftTypesTest, SentinelValues) {
    EXPECT_EQ(kNoLeader, -1);
    EXPECT_EQ(kNoTerm, -1);
    EXPECT_EQ(kNoLogIndex, -1);
}
TEST(RaftTypesTest, StateToString) {
    for (auto s : {RaftNodeState::Follower, RaftNodeState::Candidate,
                    RaftNodeState::Leader, RaftNodeState::PreCandidate}) {
        EXPECT_GT(strlen(raft_node_state_to_string(s)), 0u);
    }
}
TEST(RaftTypesTest, ConfigDefaults) {
    RaftConfig cfg;
    EXPECT_EQ(cfg.heartbeat_interval_ms, 150);
    EXPECT_LE(cfg.election_timeout_min_ms, cfg.election_timeout_max_ms);
}
TEST(RaftTypesTest, MembershipQuorum) {
    RaftMembership m; m.voters = {1,2,3}; EXPECT_EQ(m.quorum_size(), 2);
    m.voters = {1,2,3,4,5}; EXPECT_EQ(m.quorum_size(), 3);
    m.voters = {1}; EXPECT_EQ(m.quorum_size(), 1);
    m.voters = {1,2}; EXPECT_EQ(m.quorum_size(), 2);
}
TEST(RaftTypesTest, LeaderLease) {
    LeaderLease lease;
    EXPECT_FALSE(lease.is_valid());
    lease.extend();
    EXPECT_TRUE(lease.is_valid());
    lease.revoke();
    EXPECT_FALSE(lease.is_valid());
}
TEST(RaftTypesTest, LogEntryConstructors) {
    LogEntry e; e.term = 1; e.index = 1;
    EXPECT_EQ(e.term, 1); EXPECT_EQ(e.index, 1);
}

}  // namespace
}  // namespace torrent::test
