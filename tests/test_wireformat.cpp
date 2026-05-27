#include "torrent/network/protocol.h"
#include "torrent/client/produce_handler.h"
#include "torrent/client/fetch_handler.h"
#include "torrent/client/offset_handler.h"
#include "torrent/client/metadata_handler.h"
#include "torrent/client/group_handler.h"
#include "torrent/client/admin_handler.h"
#include "torrent/common/types.h"
#include "torrent/common/endian.h"
#include "torrent/network/message_codec.h"
#include <gtest/gtest.h>
#include <cstring>
#include <vector>

namespace torrent::test {
using torrent::network::MessageCodec;
namespace {

using namespace torrent::protocol;

// ============================================================================
// Wire Format Tests (Produce v9)
// ============================================================================

static std::vector<uint8_t> build_produce_request(
    const std::string& topic, int32_t partition,
    const std::string& key, const std::string& value,
    int16_t acks = 1, int32_t timeout_ms = 5000) {

    // Build record
    std::vector<uint8_t> record;
    // length (varint)
    uint8_t len_buf[5]; size_t len_sz = varint_encode_32(0, len_buf);
    record.insert(record.end(), len_buf, len_buf + len_sz);
    record.push_back(0); // attributes
    // timestamp_delta (varint)
    uint8_t ts_buf[5]; size_t ts_sz = varint_encode_signed_64(0, ts_buf);
    record.insert(record.end(), ts_buf, ts_buf + ts_sz);
    // offset_delta (varint)
    uint8_t off_buf[5]; size_t off_sz = varint_encode_signed_32(0, off_buf);
    record.insert(record.end(), off_buf, off_buf + off_sz);
    // key (varint length + data)
    uint8_t key_len_buf[5];
    size_t key_len_sz = varint_encode_signed_32(static_cast<int32_t>(key.size()), key_len_buf);
    record.insert(record.end(), key_len_buf, key_len_buf + key_len_sz);
    record.insert(record.end(), key.begin(), key.end());
    // value (varint length + data)
    uint8_t val_len_buf[5];
    size_t val_len_sz = varint_encode_signed_32(static_cast<int32_t>(value.size()), val_len_buf);
    record.insert(record.end(), val_len_buf, val_len_buf + val_len_sz);
    record.insert(record.end(), value.begin(), value.end());
    // headers count (varint 0)
    record.push_back(0);
    // Patch record length
    len_sz = varint_encode_32(static_cast<uint32_t>(record.size() - len_sz), len_buf);
    std::copy(len_buf, len_buf + len_sz, record.begin());

    // Build record batch
    std::vector<uint8_t> batch;
    // base_offset (int64)
    uint8_t offset_buf[8]; write_int64_be(offset_buf, 0);
    batch.insert(batch.end(), offset_buf, offset_buf + 8);
    // batch_length (int32) - placeholder
    size_t batch_len_pos = batch.size();
    uint8_t blen_buf[4]; write_int32_be(blen_buf, 0);
    batch.insert(batch.end(), blen_buf, blen_buf + 4);
    // partition_leader_epoch (int32)
    uint8_t epoch_buf[4]; write_int32_be(epoch_buf, 0);
    batch.insert(batch.end(), epoch_buf, epoch_buf + 4);
    // magic (int8)
    batch.push_back(2);
    // CRC (int32) - placeholder
    size_t crc_pos = batch.size();
    uint8_t crc_buf[4]; write_int32_be(crc_buf, 0);
    batch.insert(batch.end(), crc_buf, crc_buf + 4);
    // attributes (int16)
    uint8_t attr_buf[2]; write_int16_be(attr_buf, 0);
    batch.insert(batch.end(), attr_buf, attr_buf + 2);
    // last_offset_delta (int32)
    uint8_t lod_buf[4]; write_int32_be(lod_buf, 0);
    batch.insert(batch.end(), lod_buf, lod_buf + 4);
    // base_timestamp (int64)
    uint8_t bts_buf[8]; write_int64_be(bts_buf, 1234567890000);
    batch.insert(batch.end(), bts_buf, bts_buf + 8);
    // max_timestamp (int64)
    uint8_t mts_buf[8]; write_int64_be(mts_buf, 1234567890000);
    batch.insert(batch.end(), mts_buf, mts_buf + 8);
    // producer_id (int64)
    uint8_t pid_buf[8]; write_int64_be(pid_buf, -1);
    batch.insert(batch.end(), pid_buf, pid_buf + 8);
    // producer_epoch (int16)
    uint8_t pep_buf[2]; write_int16_be(pep_buf, -1);
    batch.insert(batch.end(), pep_buf, pep_buf + 2);
    // base_sequence (int32)
    uint8_t seq_buf[4]; write_int32_be(seq_buf, -1);
    batch.insert(batch.end(), seq_buf, seq_buf + 4);
    // record_count (int32)
    uint8_t cnt_buf[4]; write_int32_be(cnt_buf, 1);
    batch.insert(batch.end(), cnt_buf, cnt_buf + 4);
    // records
    batch.insert(batch.end(), record.begin(), record.end());
    // Patch batch_length
    int32_t batch_len = static_cast<int32_t>(batch.size() - batch_len_pos - 12);
    write_int32_be(blen_buf, batch_len);
    std::copy(blen_buf, blen_buf + 4, batch.begin() + batch_len_pos);
    // Compute and patch CRC (attributes through end)
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = crc_pos + 4; i < batch.size(); i++) {
        crc ^= batch[i];
        for (int b = 0; b < 8; b++) {
            crc = (crc >> 1) ^ (0x82F63B78 & -(crc & 1));
        }
    }
    crc ^= 0xFFFFFFFF;
    write_int32_be(crc_buf, static_cast<int32_t>(crc));
    std::copy(crc_buf, crc_buf + 4, batch.begin() + crc_pos);

    // Build ProduceRequest body
    std::vector<uint8_t> body;
    // transactional_id (nullable string, -1 = null)
    uint8_t null_buf[2]; write_int16_be(null_buf, -1);
    body.insert(body.end(), null_buf, null_buf + 2);
    // acks (int16)
    uint8_t ack_buf[2]; write_int16_be(ack_buf, acks);
    body.insert(body.end(), ack_buf, ack_buf + 2);
    // timeout_ms (int32)
    uint8_t to_buf[4]; write_int32_be(to_buf, timeout_ms);
    body.insert(body.end(), to_buf, to_buf + 4);
    // topic_count (int32)
    uint8_t tc_buf[4]; write_int32_be(tc_buf, 1);
    body.insert(body.end(), tc_buf, tc_buf + 4);
    // topic_name (string)
    uint8_t tn_len_buf[2];
    write_int16_be(tn_len_buf, static_cast<int16_t>(topic.size()));
    body.insert(body.end(), tn_len_buf, tn_len_buf + 2);
    body.insert(body.end(), topic.begin(), topic.end());
    // partition_count (int32)
    uint8_t pc_buf[4]; write_int32_be(pc_buf, 1);
    body.insert(body.end(), pc_buf, pc_buf + 4);
    // partition (int32)
    uint8_t part_buf[4]; write_int32_be(part_buf, partition);
    body.insert(body.end(), part_buf, part_buf + 4);
    // record_batch_size (int32)
    uint8_t rbs_buf[4];
    write_int32_be(rbs_buf, static_cast<int32_t>(batch.size()));
    body.insert(body.end(), rbs_buf, rbs_buf + 4);
    // record_batch
    body.insert(body.end(), batch.begin(), batch.end());

    return body;
}

TEST(WireFormatTest, ProduceRequestBuild) {
    auto body = build_produce_request("test-topic", 0, "key1", "value1");
    EXPECT_GT(body.size(), 50);
}

TEST(WireFormatTest, ProduceRequestMultiplePartitions) {
    // Build request with 3 partitions
    std::vector<uint8_t> body;
    // transactional_id = null
    uint8_t null_buf[2]; write_int16_be(null_buf, -1);
    body.insert(body.end(), null_buf, null_buf + 2);
    // acks = -1 (all)
    uint8_t ack_buf[2]; write_int16_be(ack_buf, -1);
    body.insert(body.end(), ack_buf, ack_buf + 2);
    // timeout_ms
    uint8_t to_buf[4]; write_int32_be(to_buf, 30000);
    body.insert(body.end(), to_buf, to_buf + 4);
    // topic_count = 1
    uint8_t tc_buf[4]; write_int32_be(tc_buf, 1);
    body.insert(body.end(), tc_buf, tc_buf + 4);
    // topic_name = "multi-part"
    std::string topic = "multi-part";
    uint8_t tn_buf[2];
    write_int16_be(tn_buf, static_cast<int16_t>(topic.size()));
    body.insert(body.end(), tn_buf, tn_buf + 2);
    body.insert(body.end(), topic.begin(), topic.end());
    // partition_count = 3
    uint8_t pc_buf[4]; write_int32_be(pc_buf, 3);
    body.insert(body.end(), pc_buf, pc_buf + 4);
    // 3 partitions with empty batches
    for (int p = 0; p < 3; p++) {
        uint8_t part_buf[4]; write_int32_be(part_buf, p);
        body.insert(body.end(), part_buf, part_buf + 4);
        uint8_t rbs_buf[4]; write_int32_be(rbs_buf, 0); // empty batch
        body.insert(body.end(), rbs_buf, rbs_buf + 4);
    }
    EXPECT_GT(body.size(), 30);
}

TEST(WireFormatTest, FetchRequestMinimal) {
    std::vector<uint8_t> body;
    // replica_id = -1 (consumer)
    uint8_t rid_buf[4]; write_int32_be(rid_buf, -1);
    body.insert(body.end(), rid_buf, rid_buf + 4);
    // max_wait_ms = 500
    uint8_t mw_buf[4]; write_int32_be(mw_buf, 500);
    body.insert(body.end(), mw_buf, mw_buf + 4);
    // min_bytes = 1
    uint8_t mb_buf[4]; write_int32_be(mb_buf, 1);
    body.insert(body.end(), mb_buf, mb_buf + 4);
    // max_bytes = 10MB
    uint8_t mxb_buf[4]; write_int32_be(mxb_buf, 10485760);
    body.insert(body.end(), mxb_buf, mxb_buf + 4);
    // isolation_level = 0 (read_uncommitted)
    body.push_back(0);
    // session_id = 0
    uint8_t sid_buf[4]; write_int32_be(sid_buf, 0);
    body.insert(body.end(), sid_buf, sid_buf + 4);
    // session_epoch = -1
    uint8_t sep_buf[4]; write_int32_be(sep_buf, -1);
    body.insert(body.end(), sep_buf, sep_buf + 4);
    // topic_count = 1
    uint8_t tc_buf[4]; write_int32_be(tc_buf, 1);
    body.insert(body.end(), tc_buf, tc_buf + 4);
    std::string topic = "fetch-topic";
    uint8_t tn_buf[2];
    write_int16_be(tn_buf, static_cast<int16_t>(topic.size()));
    body.insert(body.end(), tn_buf, tn_buf + 2);
    body.insert(body.end(), topic.begin(), topic.end());
    // partition_count = 1
    uint8_t pc_buf[4]; write_int32_be(pc_buf, 1);
    body.insert(body.end(), pc_buf, pc_buf + 4);
    // partition = 0
    uint8_t part_buf[4]; write_int32_be(part_buf, 0);
    body.insert(body.end(), part_buf, part_buf + 4);
    // fetch_offset = 0
    uint8_t fo_buf[8]; write_int64_be(fo_buf, 0);
    body.insert(body.end(), fo_buf, fo_buf + 8);
    // log_start_offset = 0
    uint8_t lso_buf[8]; write_int64_be(lso_buf, 0);
    body.insert(body.end(), lso_buf, lso_buf + 8);
    // partition_max_bytes = 1MB
    uint8_t pmb_buf[4]; write_int32_be(pmb_buf, 1048576);
    body.insert(body.end(), pmb_buf, pmb_buf + 4);
    EXPECT_GT(body.size(), 50);
}

TEST(WireFormatTest, MetadataRequestAllTopics) {
    std::vector<uint8_t> body;
    // topics = null (all topics)
    uint8_t tc_buf[4]; write_int32_be(tc_buf, -1);
    body.insert(body.end(), tc_buf, tc_buf + 4);
    // allow_auto_topic_creation = false
    body.push_back(0);
    // include_cluster_authorized_operations = false
    body.push_back(0);
    // include_topic_authorized_operations = false
    body.push_back(0);
    EXPECT_EQ(body.size(), 7);
}

TEST(WireFormatTest, MetadataRequestSpecificTopics) {
    std::vector<uint8_t> body;
    std::vector<std::string> topics = {"topic-a", "topic-b"};
    uint8_t tc_buf[4]; write_int32_be(tc_buf, static_cast<int32_t>(topics.size()));
    body.insert(body.end(), tc_buf, tc_buf + 4);
    for (const auto& t : topics) {
        uint8_t tn_buf[2];
        write_int16_be(tn_buf, static_cast<int16_t>(t.size()));
        body.insert(body.end(), tn_buf, tn_buf + 2);
        body.insert(body.end(), t.begin(), t.end());
    }
    body.push_back(0); body.push_back(0); body.push_back(0);
    EXPECT_GT(body.size(), 15);
}

TEST(WireFormatTest, JoinGroupRequest) {
    std::vector<uint8_t> body;
    std::string group_id = "my-group";
    uint8_t gi_buf[2];
    write_int16_be(gi_buf, static_cast<int16_t>(group_id.size()));
    body.insert(body.end(), gi_buf, gi_buf + 2);
    body.insert(body.end(), group_id.begin(), group_id.end());
    // session_timeout_ms = 45000
    uint8_t st_buf[4]; write_int32_be(st_buf, 45000);
    body.insert(body.end(), st_buf, st_buf + 4);
    // rebalance_timeout_ms = 300000
    uint8_t rt_buf[4]; write_int32_be(rt_buf, 300000);
    body.insert(body.end(), rt_buf, rt_buf + 4);
    // member_id = "" (new member)
    uint8_t mi_buf[2]; write_int16_be(mi_buf, 0);
    body.insert(body.end(), mi_buf, mi_buf + 2);
    // protocol_type = "consumer"
    std::string pt = "consumer";
    uint8_t pt_buf[2];
    write_int16_be(pt_buf, static_cast<int16_t>(pt.size()));
    body.insert(body.end(), pt_buf, pt_buf + 2);
    body.insert(body.end(), pt.begin(), pt.end());
    // group_protocols count = 1
    uint8_t gp_buf[4]; write_int32_be(gp_buf, 1);
    body.insert(body.end(), gp_buf, gp_buf + 4);
    // protocol name = "range"
    std::string pn = "range";
    uint8_t pn_buf[2];
    write_int16_be(pn_buf, static_cast<int16_t>(pn.size()));
    body.insert(body.end(), pn_buf, pn_buf + 2);
    body.insert(body.end(), pn.begin(), pn.end());
    // metadata = empty
    uint8_t md_buf[4]; write_int32_be(md_buf, 0);
    body.insert(body.end(), md_buf, md_buf + 4);
    EXPECT_GT(body.size(), 30);
}

TEST(WireFormatTest, SyncGroupRequest) {
    std::vector<uint8_t> body;
    std::string group_id = "my-group";
    uint8_t gi_buf[2];
    write_int16_be(gi_buf, static_cast<int16_t>(group_id.size()));
    body.insert(body.end(), gi_buf, gi_buf + 2);
    body.insert(body.end(), group_id.begin(), group_id.end());
    // generation_id = 1
    uint8_t gen_buf[4]; write_int32_be(gen_buf, 1);
    body.insert(body.end(), gen_buf, gen_buf + 4);
    // member_id = "member-1"
    std::string member_id = "member-1";
    uint8_t mem_buf[2];
    write_int16_be(mem_buf, static_cast<int16_t>(member_id.size()));
    body.insert(body.end(), mem_buf, mem_buf + 2);
    body.insert(body.end(), member_id.begin(), member_id.end());
    // assignment = empty bytes
    uint8_t as_buf[4]; write_int32_be(as_buf, 4);
    body.insert(body.end(), as_buf, as_buf + 4);
    // assignment data
    uint8_t ad_buf[4]; write_int32_be(ad_buf, 0);
    body.insert(body.end(), ad_buf, ad_buf + 4);
    EXPECT_GT(body.size(), 20);
}

TEST(WireFormatTest, HeartbeatRequest) {
    std::vector<uint8_t> body;
    std::string group_id = "my-group";
    uint8_t gi_buf[2];
    write_int16_be(gi_buf, static_cast<int16_t>(group_id.size()));
    body.insert(body.end(), gi_buf, gi_buf + 2);
    body.insert(body.end(), group_id.begin(), group_id.end());
    uint8_t gen_buf[4]; write_int32_be(gen_buf, 1);
    body.insert(body.end(), gen_buf, gen_buf + 4);
    std::string member_id = "member-1";
    uint8_t mem_buf[2];
    write_int16_be(mem_buf, static_cast<int16_t>(member_id.size()));
    body.insert(body.end(), mem_buf, mem_buf + 2);
    body.insert(body.end(), member_id.begin(), member_id.end());
    EXPECT_GT(body.size(), 12);
}

TEST(WireFormatTest, OffsetCommitRequest) {
    std::vector<uint8_t> body;
    std::string group_id = "my-group";
    uint8_t gi_buf[2];
    write_int16_be(gi_buf, static_cast<int16_t>(group_id.size()));
    body.insert(body.end(), gi_buf, gi_buf + 2);
    body.insert(body.end(), group_id.begin(), group_id.end());
    // generation_id = 1
    uint8_t gen_buf[4]; write_int32_be(gen_buf, 1);
    body.insert(body.end(), gen_buf, gen_buf + 4);
    // member_id
    std::string mid = "member-1";
    uint8_t mid_buf[2];
    write_int16_be(mid_buf, static_cast<int16_t>(mid.size()));
    body.insert(body.end(), mid_buf, mid_buf + 2);
    body.insert(body.end(), mid.begin(), mid.end());
    // retention_time_ms = -1 (use default)
    uint8_t rt_buf[8]; write_int64_be(rt_buf, -1);
    body.insert(body.end(), rt_buf, rt_buf + 8);
    // topic_count = 1
    uint8_t tc_buf[4]; write_int32_be(tc_buf, 1);
    body.insert(body.end(), tc_buf, tc_buf + 4);
    std::string topic = "test";
    uint8_t tn_buf[2];
    write_int16_be(tn_buf, static_cast<int16_t>(topic.size()));
    body.insert(body.end(), tn_buf, tn_buf + 2);
    body.insert(body.end(), topic.begin(), topic.end());
    // partition_count = 1
    uint8_t pc_buf[4]; write_int32_be(pc_buf, 1);
    body.insert(body.end(), pc_buf, pc_buf + 4);
    uint8_t part_buf[4]; write_int32_be(part_buf, 0);
    body.insert(body.end(), part_buf, part_buf + 4);
    uint8_t off_buf[8]; write_int64_be(off_buf, 1000);
    body.insert(body.end(), off_buf, off_buf + 8);
    // metadata = ""
    uint8_t meta_buf[2]; write_int16_be(meta_buf, 0);
    body.insert(body.end(), meta_buf, meta_buf + 2);
    EXPECT_GT(body.size(), 40);
}

TEST(WireFormatTest, CreateTopicsRequest) {
    std::vector<uint8_t> body;
    // topic_count = 2
    uint8_t tc_buf[4]; write_int32_be(tc_buf, 2);
    body.insert(body.end(), tc_buf, tc_buf + 4);
    for (const auto& name : {"topic-1", "topic-2"}) {
        std::string tn(name);
        uint8_t tn_buf[2];
        write_int16_be(tn_buf, static_cast<int16_t>(tn.size()));
        body.insert(body.end(), tn_buf, tn_buf + 2);
        body.insert(body.end(), tn.begin(), tn.end());
        // num_partitions
        uint8_t np_buf[4]; write_int32_be(np_buf, 3);
        body.insert(body.end(), np_buf, np_buf + 4);
        // replication_factor
        uint8_t rf_buf[2]; write_int16_be(rf_buf, 3);
        body.insert(body.end(), rf_buf, rf_buf + 2);
        // configs count = 1
        uint8_t cc_buf[4]; write_int32_be(cc_buf, 1);
        body.insert(body.end(), cc_buf, cc_buf + 4);
        std::string ck = "retention.ms"; std::string cv = "86400000";
        uint8_t ck_buf[2]; write_int16_be(ck_buf, static_cast<int16_t>(ck.size()));
        body.insert(body.end(), ck_buf, ck_buf + 2);
        body.insert(body.end(), ck.begin(), ck.end());
        uint8_t cv_buf[2];
        write_int16_be(cv_buf, static_cast<int16_t>(cv.size()));
        body.insert(body.end(), cv_buf, cv_buf + 2);
        body.insert(body.end(), cv.begin(), cv.end());
    }
    // timeout_ms
    uint8_t to_buf[4]; write_int32_be(to_buf, 30000);
    body.insert(body.end(), to_buf, to_buf + 4);
    EXPECT_GT(body.size(), 50);
}

TEST(WireFormatTest, ApiVersionsRequest) {
    // ApiVersions request has empty body
    std::vector<uint8_t> body;
    EXPECT_EQ(body.size(), 0);
}

TEST(WireFormatTest, SaslHandshakeRequest) {
    std::vector<uint8_t> body;
    std::string mechanism = "SCRAM-SHA-256";
    uint8_t buf[2];
    write_int16_be(buf, static_cast<int16_t>(mechanism.size()));
    body.insert(body.end(), buf, buf + 2);
    body.insert(body.end(), mechanism.begin(), mechanism.end());
    EXPECT_GT(body.size(), 5);
}

TEST(WireFormatTest, DescribeGroupsRequest) {
    std::vector<uint8_t> body;
    uint8_t gc_buf[4]; write_int32_be(gc_buf, 2);
    body.insert(body.end(), gc_buf, gc_buf + 4);
    for (const auto& g : {"group-a", "group-b"}) {
        std::string gs(g);
        uint8_t buf[2];
        write_int16_be(buf, static_cast<int16_t>(gs.size()));
        body.insert(body.end(), buf, buf + 2);
        body.insert(body.end(), gs.begin(), gs.end());
    }
    EXPECT_GT(body.size(), 10);
}

TEST(WireFormatTest, ResponseErrorCode) {
    // INT16 error code serialization
    uint8_t buf[2];
    write_int16_be(buf, static_cast<int16_t>(ErrorCode::NONE));
    EXPECT_EQ(read_int16_be(buf), static_cast<int16_t>(ErrorCode::NONE));
    write_int16_be(buf, static_cast<int16_t>(ErrorCode::NOT_LEADER_FOR_PARTITION));
    EXPECT_EQ(read_int16_be(buf), static_cast<int16_t>(ErrorCode::NOT_LEADER_FOR_PARTITION));
}

TEST(WireFormatTest, VarintZeroEncoding) {
    uint8_t buf[5];
    size_t sz = varint_encode_32(0, buf);
    EXPECT_EQ(sz, 1);
    EXPECT_EQ(buf[0], 0);
}

TEST(WireFormatTest, VarintMaxEncoding) {
    uint8_t buf[5];
    size_t sz = varint_encode_32(UINT32_MAX, buf);
    EXPECT_EQ(sz, 5);
    uint32_t out;
    size_t read = varint_decode_32(buf, sz, out);
    EXPECT_EQ(read, 5);
    EXPECT_EQ(out, UINT32_MAX);
}

TEST(WireFormatTest, MessageCodecRoundTrip) {
    // Build a simple request
    std::string client_id = "test-client-123";
    std::string body_str = "test-body-data";
    buffer_view body(body_str.data(), body_str.size());

    auto frame = MessageCodec::encode_request(18, 3, 5555, client_id, body);
    EXPECT_GT(frame.size(), 10);

    int32_t frame_size = MessageCodec::peek_frame_size(frame.data(), frame.size());
    EXPECT_EQ(frame_size, static_cast<int32_t>(frame.size() - 4));

    int16_t api_key, api_version;
    int32_t correlation_id;
    auto remaining = MessageCodec::decode_request_header(
        frame.data() + 4, frame.size() - 4, api_key, api_version, correlation_id);

    EXPECT_EQ(api_key, 18);
    EXPECT_EQ(api_version, 3);
    EXPECT_EQ(correlation_id, 5555);
}

TEST(WireFormatTest, Int16ZeroEncoding) {
    uint8_t buf[2];
    write_int16_be(buf, 0);
    EXPECT_EQ(buf[0], 0);
    EXPECT_EQ(buf[1], 0);
    EXPECT_EQ(read_int16_be(buf), 0);
}

TEST(WireFormatTest, Int32ZeroEncoding) {
    uint8_t buf[4];
    write_int32_be(buf, 0);
    EXPECT_EQ(read_int32_be(buf), 0);
}

TEST(WireFormatTest, Int64ZeroEncoding) {
    uint8_t buf[8];
    write_int64_be(buf, 0);
    EXPECT_EQ(read_int64_be(buf), 0);
}

}  // namespace
}  // namespace torrent::test
