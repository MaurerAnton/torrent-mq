/**
 * protocol_serializer.cpp — Big-Endian Binary Protocol Serializer
 *
 * Provides complete wire-format serialization for all Kafka-compatible and
 * torrent-native protocol messages.  Mirrors the parsing code found in
 * produce_handler.cpp, fetch_handler.cpp, group_handler.cpp etc.
 *
 * Wire format conventions (Kafka v2+, no tagged fields):
 *   - All integers are big-endian (network byte order)
 *   - Strings: INT16 length prefix + UTF-8 bytes (nullable uses -1)
 *   - Arrays:  INT32 length prefix + elements
 *   - Bytes:   INT32 length prefix + raw bytes
 *   - Varints:  unsigned LEB128; signed uses ZigZag encoding
 *   - CRC32C:   Castagnoli polynomial, computed over batch body
 *
 * Thread safety: All functions are stateless and reentrant.  The output
 * buffer (std::vector<char>&) is the only mutable parameter.
 */

#include "torrent/network/protocol_serializer.h"
#include "torrent/network/protocol.h"
#include "torrent/common/types.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

// ============================================================================
// Anonymous namespace — wire-type primitive helpers
// ============================================================================

namespace {

namespace tp = torrent::protocol;

// =========================================================================
// CRC32C (Castagnoli) table — used for record batch integrity
// =========================================================================

static constexpr uint32_t kCrc32cPoly = 0x82F63B78u;

static const std::array<uint32_t, 256> build_crc32c_table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc & 1) ? (crc >> 1) ^ kCrc32cPoly : (crc >> 1);
        }
        table[i] = crc;
    }
    return table;
}

static const auto kCrc32cTable = build_crc32c_table();

[[nodiscard]] uint32_t compute_crc32c(const void* data, size_t len) noexcept {
    auto* p = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        crc = (crc >> 8) ^ kCrc32cTable[(crc & 0xFF) ^ p[i]];
    }
    return crc ^ 0xFFFFFFFFu;
}

// =========================================================================
// Internal write helpers
// =========================================================================

/// Write a single byte.
inline void write_byte(std::vector<char>& buf, uint8_t val) noexcept {
    buf.push_back(static_cast<char>(val));
}

/// Write raw bytes from a pointer.  Uses memcpy for efficiency.
inline void write_raw(std::vector<char>& buf, const void* data, size_t len) noexcept {
    if (len == 0) return;
    size_t old = buf.size();
    buf.resize(old + len);
    std::memcpy(&buf[old], data, len);
}

/// Write a big-endian int8 (raw cast).
inline void write_int8_impl(std::vector<char>& buf, int8_t val) noexcept {
    write_byte(buf, static_cast<uint8_t>(val));
}

/// Write a big-endian uint16 (raw bytes, no bswap needed — manual).
inline void write_uint16_be(std::vector<char>& buf, uint16_t val) noexcept {
    write_byte(buf, static_cast<uint8_t>((val >> 8) & 0xFF));
    write_byte(buf, static_cast<uint8_t>(val & 0xFF));
}

/// Write a big-endian uint32.
inline void write_uint32_be_impl(std::vector<char>& buf, uint32_t val) noexcept {
    write_byte(buf, static_cast<uint8_t>((val >> 24) & 0xFF));
    write_byte(buf, static_cast<uint8_t>((val >> 16) & 0xFF));
    write_byte(buf, static_cast<uint8_t>((val >> 8) & 0xFF));
    write_byte(buf, static_cast<uint8_t>(val & 0xFF));
}

/// Write a big-endian uint64.
inline void write_uint64_be(std::vector<char>& buf, uint64_t val) noexcept {
    write_byte(buf, static_cast<uint8_t>((val >> 56) & 0xFF));
    write_byte(buf, static_cast<uint8_t>((val >> 48) & 0xFF));
    write_byte(buf, static_cast<uint8_t>((val >> 40) & 0xFF));
    write_byte(buf, static_cast<uint8_t>((val >> 32) & 0xFF));
    write_byte(buf, static_cast<uint8_t>((val >> 24) & 0xFF));
    write_byte(buf, static_cast<uint8_t>((val >> 16) & 0xFF));
    write_byte(buf, static_cast<uint8_t>((val >> 8) & 0xFF));
    write_byte(buf, static_cast<uint8_t>(val & 0xFF));
}

} // anonymous namespace

// ============================================================================
// Public API — Wire-type primitives
// ============================================================================

namespace torrent::network {

// --------------------------------------------------------------------------
// Integer writers
// --------------------------------------------------------------------------

void write_int8(std::vector<char>& buf, int8_t val) noexcept {
    write_int8_impl(buf, val);
}

void write_int16_be(std::vector<char>& buf, int16_t val) noexcept {
    write_uint16_be(buf, static_cast<uint16_t>(val));
}

void write_int32_be(std::vector<char>& buf, int32_t val) noexcept {
    write_uint32_be_impl(buf, static_cast<uint32_t>(val));
}

void write_int64_be(std::vector<char>& buf, int64_t val) noexcept {
    write_uint64_be(buf, static_cast<uint64_t>(val));
}

void write_uint32_be(std::vector<char>& buf, uint32_t val) noexcept {
    write_uint32_be_impl(buf, val);
}

// --------------------------------------------------------------------------
// String writers
// --------------------------------------------------------------------------

void write_string(std::vector<char>& buf, const std::string& str) noexcept {
    int16_t len = static_cast<int16_t>(str.size());
    write_int16_be(buf, len);
    if (len > 0) {
        write_raw(buf, str.data(), static_cast<size_t>(len));
    }
}

void write_string(std::vector<char>& buf, std::string_view str) noexcept {
    int16_t len = static_cast<int16_t>(str.size());
    write_int16_be(buf, len);
    if (len > 0) {
        write_raw(buf, str.data(), static_cast<size_t>(len));
    }
}

void write_nullable_string(std::vector<char>& buf, const std::string& str) noexcept {
    if (str.empty()) {
        write_int16_be(buf, -1);
    } else {
        write_string(buf, str);
    }
}

// --------------------------------------------------------------------------
// Bytes writer
// --------------------------------------------------------------------------

void write_bytes(std::vector<char>& buf, const std::vector<uint8_t>& data) noexcept {
    write_int32_be(buf, static_cast<int32_t>(data.size()));
    if (!data.empty()) {
        write_raw(buf, data.data(), data.size());
    }
}

void write_bytes(std::vector<char>& buf, const uint8_t* data, size_t len) noexcept {
    write_int32_be(buf, static_cast<int32_t>(len));
    if (len > 0 && data != nullptr) {
        write_raw(buf, data, len);
    }
}

// --------------------------------------------------------------------------
// Varint writers
// --------------------------------------------------------------------------

void write_unsigned_varint(std::vector<char>& buf, uint64_t val) noexcept {
    while (val >= 0x80) {
        write_byte(buf, static_cast<uint8_t>((val & 0x7F) | 0x80));
        val >>= 7;
    }
    write_byte(buf, static_cast<uint8_t>(val & 0x7F));
}

void write_signed_varint(std::vector<char>& buf, int64_t val) noexcept {
    // ZigZag encode: (n << 1) ^ (n >> 63)
    uint64_t uv = (static_cast<uint64_t>(val) << 1)
                ^ static_cast<uint64_t>(val >> 63);
    write_unsigned_varint(buf, uv);
}

// --------------------------------------------------------------------------
// Boolean
// --------------------------------------------------------------------------

void write_bool(std::vector<char>& buf, bool val) noexcept {
    write_int8(buf, val ? 1 : 0);
}

// --------------------------------------------------------------------------
// Error code writers
// --------------------------------------------------------------------------

void write_error_code(std::vector<char>& buf, torrent::error_code ec) noexcept {
    write_int16_be(buf, static_cast<int16_t>(ec));
}

void write_error_code(std::vector<char>& buf, tp::ErrorCode ec) noexcept {
    write_int16_be(buf, static_cast<int16_t>(ec));
}

// ============================================================================
// Request Serializers
// ============================================================================

// --- ProduceRequest (api_key=0) ---
// Wire format:
//   nullable_string  transactional_id
//   INT16            acks
//   INT32            timeout_ms
//   INT32            topic_count
//   per-topic:
//     STRING           topic_name
//     INT32            partition_count
//     per-partition:
//       INT32            partition_index
//       INT32            batch_length
//       bytes            record_batch_data

void serialize_produce_request(std::vector<char>& buf,
                               const tp::ProduceRequest& req) {
    // transactional_id (nullable)
    write_nullable_string(buf, req.transaction_id);

    // acks
    write_int16_be(buf, req.acks);

    // timeout_ms
    write_int32_be(buf, req.timeout_ms);

    // topic array — need to group batches by topic
    // For simplicity, assume batches are grouped by topic (common case)
    // If empty, write 0 topics
    if (req.batches.empty()) {
        write_int32_be(buf, 0);
        return;
    }

    // Group by topic: build a map of topic_name → vector<RecordBatch>
    // Since we don't want a map dependency here, we assume the caller
    // pre-grouped or there's one topic. For full compliance, we use
    // a single-topic simplification; real code would group.
    // Actually, let's handle proper grouping using a simple scan.

    // Collect topic names in order of first appearance
    std::vector<std::string> topics_order;
    std::vector<std::vector<const tp::RecordBatch*>> topic_batches;

    for (const auto& batch : req.batches) {
        // Find or create entry for this topic
        bool found = false;
        for (size_t i = 0; i < topics_order.size(); ++i) {
            if (topics_order[i] == batch.topic) {
                topic_batches[i].push_back(&batch);
                found = true;
                break;
            }
        }
        if (!found) {
            topics_order.push_back(batch.topic);
            topic_batches.push_back({&batch});
        }
    }

    write_int32_be(buf, static_cast<int32_t>(topics_order.size()));

    for (size_t t = 0; t < topics_order.size(); ++t) {
        // topic_name
        write_string(buf, topics_order[t]);

        // partition_count
        const auto& batches = topic_batches[t];
        write_int32_be(buf, static_cast<int32_t>(batches.size()));

        for (const auto* batch : batches) {
            // partition_index
            write_int32_be(buf, batch->partition);

            // Serialize the record batch into a temporary buffer to get length
            std::vector<char> batch_data;
            serialize_record_batch(batch_data, *batch,
                                   0,     // base_offset (will be filled by broker)
                                   -1,    // partition_leader_epoch
                                   -1,    // producer_id
                                   0,     // producer_epoch
                                   0);    // base_sequence

            // batch_length + batch_data
            write_int32_be(buf, static_cast<int32_t>(batch_data.size()));
            write_raw(buf, batch_data.data(), batch_data.size());
        }
    }
}

// --- FetchRequest (api_key=1) ---
// Wire format:
//   INT32  max_wait_ms
//   INT32  min_bytes
//   INT32  max_bytes
//   INT8   isolation_level
//   INT32  session_id
//   INT32  session_epoch
//   INT32  topic_count
//   per-topic:
//     STRING   topic_name
//     INT32    partition_count
//     per-partition:
//       INT32    partition_index
//       INT64    fetch_offset
//       INT64    log_start_offset
//       INT32    partition_max_bytes

void serialize_fetch_request(std::vector<char>& buf,
                             const tp::FetchRequest& req) {
    write_int32_be(buf, req.max_wait_ms);
    write_int32_be(buf, req.min_bytes);
    write_int32_be(buf, req.max_bytes);
    write_int8(buf, req.isolation_level);
    write_int32_be(buf, req.session_id);
    write_int32_be(buf, req.session_epoch);

    // Group by topic
    // For simplicity, write one partition per topic (the common streaming case)
    write_int32_be(buf, static_cast<int32_t>(req.partitions.size()));

    for (size_t i = 0; i < req.partitions.size(); ++i) {
        write_string(buf, req.partitions[i].topic);
        write_int32_be(buf, 1);  // one partition per topic in this grouping

        write_int32_be(buf, req.partitions[i].partition);
        write_int64_be(buf, (i < req.fetch_offsets.size())
                             ? req.fetch_offsets[i] : 0);
        write_int64_be(buf, 0);  // log_start_offset (0 for now)
        write_int32_be(buf, req.max_bytes);
    }
}

// --- ListOffsetsRequest (api_key=2) ---

void serialize_list_offsets_request(std::vector<char>& buf,
                                    const tp::ListOffsetsRequest& req) {
    write_int32_be(buf, req.replica_id);
    write_int8(buf, req.isolation_level);

    write_int32_be(buf, static_cast<int32_t>(req.partitions.size()));
    for (size_t i = 0; i < req.partitions.size(); ++i) {
        write_string(buf, req.partitions[i].topic);
        write_int32_be(buf, 1);  // one partition per topic

        write_int32_be(buf, req.partitions[i].partition);
        write_int64_be(buf, (i < req.timestamps.size())
                             ? req.timestamps[i] : -1);
    }
}

// --- MetadataRequest (api_key=3) ---

void serialize_metadata_request(std::vector<char>& buf,
                                const tp::MetadataRequest& req) {
    write_int32_be(buf, static_cast<int32_t>(req.topics.size()));
    for (const auto& topic : req.topics) {
        write_string(buf, topic);
    }

    write_bool(buf, req.allow_auto_create);
    write_bool(buf, req.include_cluster_authorized_operations);
    write_bool(buf, req.include_topic_authorized_operations);
}

// --- OffsetCommitRequest (api_key=8) ---

void serialize_offset_commit_request(std::vector<char>& buf,
                                     const tp::OffsetCommitRequest& req) {
    write_string(buf, req.group_id);
    write_int32_be(buf, req.generation_id);
    write_string(buf, req.member_id);
    write_nullable_string(buf, req.group_instance_id);
    write_int64_be(buf, req.retention_ms);

    write_int32_be(buf, static_cast<int32_t>(req.partitions.size()));
    for (size_t i = 0; i < req.partitions.size(); ++i) {
        write_string(buf, req.partitions[i].topic);
        write_int32_be(buf, req.partitions[i].partition);
        write_int64_be(buf, (i < req.offsets.size()) ? req.offsets[i] : -1);
        write_string(buf, (i < req.metadata.size()) ? req.metadata[i] : std::string{});
    }
}

// --- OffsetFetchRequest (api_key=9) ---

void serialize_offset_fetch_request(std::vector<char>& buf,
                                    const tp::OffsetFetchRequest& req) {
    write_string(buf, req.group_id);

    write_int32_be(buf, static_cast<int32_t>(req.partitions.size()));
    for (const auto& tp : req.partitions) {
        write_string(buf, tp.topic);
        write_int32_be(buf, tp.partition);
    }
}

// --- FindCoordinatorRequest (api_key=10) ---

void serialize_find_coordinator_request(std::vector<char>& buf,
                                        const tp::FindCoordinatorRequest& req) {
    write_string(buf, req.key);
    write_int8(buf, req.type);
}

// --- JoinGroupRequest (api_key=11) ---

void serialize_join_group_request(std::vector<char>& buf,
                                  const tp::JoinGroupRequest& req) {
    write_string(buf, req.group_id);
    write_int32_be(buf, req.session_timeout_ms);
    write_int32_be(buf, req.rebalance_timeout_ms);
    write_string(buf, req.member_id);
    write_nullable_string(buf, req.group_instance_id);
    write_string(buf, req.protocol_type);

    write_int32_be(buf, static_cast<int32_t>(req.protocols.size()));
    for (const auto& proto : req.protocols) {
        write_string(buf, proto);
    }

    write_int32_be(buf, static_cast<int32_t>(req.protocol_metadata.size()));
    for (const auto& meta : req.protocol_metadata) {
        write_int32_be(buf, static_cast<int32_t>(meta.size()));
        write_raw(buf, meta.data(), meta.size());
    }
}

// --- HeartbeatRequest (api_key=12) ---

void serialize_heartbeat_request(std::vector<char>& buf,
                                 const tp::HeartbeatRequest& req) {
    write_string(buf, req.group_id);
    write_int32_be(buf, req.generation_id);
    write_string(buf, req.member_id);
    write_nullable_string(buf, req.group_instance_id);
}

// --- LeaveGroupRequest (api_key=13) ---

void serialize_leave_group_request(std::vector<char>& buf,
                                   const tp::LeaveGroupRequest& req) {
    write_string(buf, req.group_id);
    write_string(buf, req.member_id);

    write_int32_be(buf, static_cast<int32_t>(req.members.size()));
    for (const auto& m : req.members) {
        write_string(buf, m);
    }
}

// --- SyncGroupRequest (api_key=14) ---

void serialize_sync_group_request(std::vector<char>& buf,
                                  const tp::SyncGroupRequest& req) {
    write_string(buf, req.group_id);
    write_int32_be(buf, req.generation_id);
    write_string(buf, req.member_id);
    write_nullable_string(buf, req.group_instance_id);
    write_string(buf, req.protocol_type);
    write_string(buf, req.protocol_name);

    write_int32_be(buf, static_cast<int32_t>(req.assignments.size()));
    for (const auto& assign : req.assignments) {
        write_int32_be(buf, static_cast<int32_t>(assign.size()));
        write_raw(buf, assign.data(), assign.size());
    }
}

// --- DescribeConfigsRequest (api_key=32) ---

void serialize_describe_configs_request(std::vector<char>& buf,
                                        const tp::DescribeConfigsRequest& req) {
    write_int32_be(buf, static_cast<int32_t>(req.resource_types.size()));

    for (size_t i = 0; i < req.resource_types.size(); ++i) {
        write_int32_be(buf, req.resource_types[i]);

        write_string(buf, (i < req.resource_names.size())
                           ? req.resource_names[i] : std::string{});

        if (i < req.config_keys.size()) {
            write_string(buf, req.config_keys[i]);
        } else {
            write_string(buf, std::string{});
        }
    }

    write_bool(buf, req.include_synonyms);
    write_bool(buf, req.include_documentation);
}

// --- AlterConfigsRequest (api_key=33) ---

void serialize_alter_configs_request(std::vector<char>& buf,
                                     const tp::AlterConfigsRequest& req) {
    write_int32_be(buf, static_cast<int32_t>(req.resource_types.size()));

    for (size_t i = 0; i < req.resource_types.size(); ++i) {
        write_int32_be(buf, req.resource_types[i]);

        write_string(buf, (i < req.resource_names.size())
                           ? req.resource_names[i] : std::string{});

        const auto& cfg = (i < req.configs.size()) ? req.configs[i]
                          : std::vector<tp::ConfigEntry>{};
        write_int32_be(buf, static_cast<int32_t>(cfg.size()));
        for (const auto& entry : cfg) {
            write_string(buf, entry.name);
            write_string(buf, entry.value);
        }
    }

    write_bool(buf, req.validate_only);
}

// ============================================================================
// Response Serializers
// ============================================================================

// --- ProduceResponse (api_key=0) ---
// Wire format:
//   INT32  throttle_time_ms
//   INT32  response_count
//   per-response:
//     STRING  topic_name
//     INT32   partition_count
//     per-partition:
//       INT32    partition_index
//       INT16    error_code
//       INT64    base_offset
//       INT64    log_append_time_ms
//       INT64    log_start_offset
//       nullable_string error_message

void serialize_produce_response(std::vector<char>& buf,
                                const tp::ProduceResponse& res) {
    write_int32_be(buf, res.throttle_time_ms);

    // Group results by topic
    std::vector<std::string> topic_order;
    std::vector<std::vector<const tp::PartitionResult*>> topic_results;

    for (const auto& pr : res.results) {
        bool found = false;
        for (size_t i = 0; i < topic_order.size(); ++i) {
            if (topic_order[i] == pr.topic) {
                topic_results[i].push_back(&pr);
                found = true;
                break;
            }
        }
        if (!found) {
            topic_order.push_back(pr.topic);
            topic_results.push_back({&pr});
        }
    }

    write_int32_be(buf, static_cast<int32_t>(topic_order.size()));

    for (size_t t = 0; t < topic_order.size(); ++t) {
        write_string(buf, topic_order[t]);

        const auto& results = topic_results[t];
        write_int32_be(buf, static_cast<int32_t>(results.size()));

        for (const auto* pr : results) {
            write_int32_be(buf, pr->partition);
            write_error_code(buf, pr->error_code);
            write_int64_be(buf, 0);  // base_offset (placeholder)
            write_int64_be(buf, 0);  // log_append_time_ms
            write_int64_be(buf, 0);  // log_start_offset
            write_nullable_string(buf, pr->error_msg);
        }
    }
}

// --- FetchResponse (api_key=1) ---

void serialize_fetch_response(std::vector<char>& buf,
                              const tp::FetchResponse& res) {
    write_int32_be(buf, res.throttle_time_ms);
    write_error_code(buf, res.error_code);
    write_int32_be(buf, res.session_id);

    write_int32_be(buf, static_cast<int32_t>(res.results.size()));
    for (size_t i = 0; i < res.results.size(); ++i) {
        write_string(buf, res.results[i].topic);
        write_int32_be(buf, res.results[i].partition);
        write_error_code(buf, res.results[i].error_code);

        // Write records for this partition
        const auto& records = (i < res.records.size()) ? res.records[i]
                              : std::vector<tp::Record>{};
        write_int32_be(buf, static_cast<int32_t>(records.size()));
        for (const auto& rec : records) {
            // Record: key_length, key, value_length, value, timestamp, headers
            write_int32_be(buf, static_cast<int32_t>(rec.key.size()));
            if (!rec.key.empty())
                write_raw(buf, rec.key.data(), rec.key.size());

            write_int32_be(buf, static_cast<int32_t>(rec.value.size()));
            if (!rec.value.empty())
                write_raw(buf, rec.value.data(), rec.value.size());

            write_int64_be(buf, rec.timestamp);

            write_int32_be(buf, static_cast<int32_t>(rec.headers.size()));
            for (const auto& hdr : rec.headers) {
                write_string(buf, hdr.key);
                write_int32_be(buf, static_cast<int32_t>(hdr.value.size()));
                if (!hdr.value.empty())
                    write_raw(buf, hdr.value.data(), hdr.value.size());
            }
        }
    }
}

// --- ListOffsetsResponse (api_key=2) ---

void serialize_list_offsets_response(std::vector<char>& buf,
                                     const tp::ListOffsetsResponse& res) {
    write_int32_be(buf, res.throttle_time_ms);

    write_int32_be(buf, static_cast<int32_t>(res.offsets.size()));
    for (const auto& po : res.offsets) {
        write_string(buf, po.topic);
        write_int32_be(buf, po.partition);
        write_int64_be(buf, po.offset);
        write_int32_be(buf, po.leader_epoch);
        write_error_code(buf, po.error_code);
    }
}

// --- MetadataResponse (api_key=3) ---

void serialize_metadata_response(std::vector<char>& buf,
                                 const tp::MetadataResponse& res) {
    write_int32_be(buf, res.throttle_time_ms);

    // Brokers
    write_int32_be(buf, static_cast<int32_t>(res.brokers.size()));
    for (const auto& b : res.brokers) {
        write_string(buf, b);
    }

    // Topics
    write_int32_be(buf, static_cast<int32_t>(res.topics.size()));
    for (size_t i = 0; i < res.topics.size(); ++i) {
        write_string(buf, res.topics[i]);
        write_int32_be(buf, (i < res.partition_counts.size())
                             ? res.partition_counts[i] : 0);
    }

    write_error_code(buf, res.error_code);
}

// --- OffsetCommitResponse (api_key=8) ---

void serialize_offset_commit_response(std::vector<char>& buf,
                                      const tp::OffsetCommitResponse& res) {
    write_int32_be(buf, res.throttle_time_ms);

    write_int32_be(buf, static_cast<int32_t>(res.results.size()));
    for (const auto& pr : res.results) {
        write_string(buf, pr.topic);
        write_int32_be(buf, pr.partition);
        write_error_code(buf, pr.error_code);
        write_nullable_string(buf, pr.error_msg);
    }
}

// --- OffsetFetchResponse (api_key=9) ---

void serialize_offset_fetch_response(std::vector<char>& buf,
                                     const tp::OffsetFetchResponse& res) {
    write_int32_be(buf, res.throttle_time_ms);
    write_error_code(buf, res.error_code);

    write_int32_be(buf, static_cast<int32_t>(res.offsets.size()));
    for (const auto& po : res.offsets) {
        write_string(buf, po.topic);
        write_int32_be(buf, po.partition);
        write_int64_be(buf, po.offset);
        write_int32_be(buf, po.leader_epoch);
        write_error_code(buf, po.error_code);
    }
}

// --- FindCoordinatorResponse (api_key=10) ---

void serialize_find_coordinator_response(std::vector<char>& buf,
                                         const tp::FindCoordinatorResponse& res) {
    write_int32_be(buf, res.throttle_time_ms);
    write_error_code(buf, res.error_code);
    write_int32_be(buf, res.node_id);
    write_string(buf, res.host);
    write_int32_be(buf, res.port);
}

// --- JoinGroupResponse (api_key=11) ---

void serialize_join_group_response(std::vector<char>& buf,
                                   const tp::JoinGroupResponse& res) {
    write_int32_be(buf, res.throttle_time_ms);
    write_error_code(buf, res.error_code);
    write_int32_be(buf, res.generation_id);
    write_string(buf, res.protocol_name);
    write_string(buf, res.leader_id);
    write_string(buf, res.member_id);

    write_int32_be(buf, static_cast<int32_t>(res.members.size()));
    for (const auto& m : res.members) {
        write_string(buf, m);
    }
}

// --- HeartbeatResponse (api_key=12) ---

void serialize_heartbeat_response(std::vector<char>& buf,
                                  const tp::HeartbeatResponse& res) {
    write_int32_be(buf, res.throttle_time_ms);
    write_error_code(buf, res.error_code);
}

// --- LeaveGroupResponse (api_key=13) ---

void serialize_leave_group_response(std::vector<char>& buf,
                                    const tp::LeaveGroupResponse& res) {
    write_int32_be(buf, res.throttle_time_ms);
    write_error_code(buf, res.error_code);
}

// --- SyncGroupResponse (api_key=14) ---

void serialize_sync_group_response(std::vector<char>& buf,
                                   const tp::SyncGroupResponse& res) {
    write_int32_be(buf, res.throttle_time_ms);
    write_error_code(buf, res.error_code);
    write_string(buf, res.protocol_type);
    write_string(buf, res.protocol_name);

    write_int32_be(buf, static_cast<int32_t>(res.assignment.size()));
    if (!res.assignment.empty()) {
        write_raw(buf, res.assignment.data(), res.assignment.size());
    }
}

// --- DescribeConfigsResponse (api_key=32) ---

void serialize_describe_configs_response(std::vector<char>& buf,
                                         const tp::DescribeConfigsResponse& res) {
    write_int32_be(buf, res.throttle_time_ms);

    write_int32_be(buf, static_cast<int32_t>(res.entries.size()));
    for (const auto& resource_entries : res.entries) {
        write_int32_be(buf, static_cast<int32_t>(resource_entries.size()));
        for (const auto& entry : resource_entries) {
            write_string(buf, entry.name);
            write_string(buf, entry.value);
        }
    }
}

// --- AlterConfigsResponse (api_key=33) ---

void serialize_alter_configs_response(std::vector<char>& buf,
                                      const tp::AlterConfigsResponse& res) {
    write_int32_be(buf, res.throttle_time_ms);

    write_int32_be(buf, static_cast<int32_t>(res.results.size()));
    for (const auto& pr : res.results) {
        write_string(buf, pr.topic);
        write_int32_be(buf, pr.partition);
        write_error_code(buf, pr.error_code);
        write_nullable_string(buf, pr.error_msg);
    }
}

// ============================================================================
// RecordBatch / Record Serializers
// ============================================================================

// --- Record batch header ---
// Wire format (v2, magic=2):
//   INT64 base_offset
//   INT32 batch_length        (computed from after CRC through record end)
//   INT32 partition_leader_epoch
//   INT8  magic               (2)
//   INT32 crc                 (CRC32C of everything after this field)
//   INT16 attributes
//   INT32 last_offset_delta
//   INT64 base_timestamp
//   INT64 max_timestamp
//   INT64 producer_id
//   INT16 producer_epoch
//   INT32 base_sequence
//   INT32 record_count

void serialize_record_batch_header(
    std::vector<char>& buf,
    int64_t base_offset,
    int32_t partition_leader_epoch,
    int8_t magic,
    int16_t attributes,
    int32_t last_offset_delta,
    int64_t base_timestamp,
    int64_t max_timestamp,
    int64_t producer_id,
    int16_t producer_epoch,
    int32_t base_sequence,
    int32_t record_count,
    uint32_t crc) noexcept {

    write_int64_be(buf, base_offset);

    // batch_length placeholder — will be patched after records are written.
    // We write 0 for now; caller patches after serializing records.
    size_t batch_length_pos = buf.size();
    write_int32_be(buf, 0);  // placeholder

    write_int32_be(buf, partition_leader_epoch);
    write_int8(buf, magic);

    // CRC
    write_int32_be(buf, static_cast<int32_t>(crc));

    write_int16_be(buf, attributes);
    write_int32_be(buf, last_offset_delta);
    write_int64_be(buf, base_timestamp);
    write_int64_be(buf, max_timestamp);
    write_int64_be(buf, producer_id);
    write_int16_be(buf, producer_epoch);
    write_int32_be(buf, base_sequence);
    write_int32_be(buf, record_count);

    // If caller wants to patch batch_length, they need to know the size
    // from the CRC field onward.  We store this for reference.
    // batch_length = total_bytes_after_batch_length_field
    (void)batch_length_pos; // caller's responsibility
}

// --- Single record ---
// Wire format (v2):
//   VARINT  length          (record body length)
//   INT8    attributes      (record attributes)
//   VARINT  timestamp_delta
//   VARINT  offset_delta
//   VARINT  key_length
//   bytes   key
//   VARINT  value_length    (-1 for null)
//   bytes   value
//   VARINT  header_count
//   per-header:
//     VARINT  key_length
//     bytes   key
//     VARINT  value_length   (-1 for null)
//     bytes   value

void serialize_record(std::vector<char>& buf,
                      const tp::Record& record,
                      int64_t timestamp_delta,
                      int32_t offset_delta) noexcept {

    // Build the record body first to compute its length
    std::vector<char> body;

    // attributes
    write_int8(body, 0);

    // timestamp_delta
    write_signed_varint(body, timestamp_delta);

    // offset_delta
    write_signed_varint(body, static_cast<int64_t>(offset_delta));

    // key_length + key
    if (record.key.empty()) {
        write_signed_varint(body, -1);
    } else {
        write_signed_varint(body, static_cast<int64_t>(record.key.size()));
        write_raw(body, record.key.data(), record.key.size());
    }

    // value_length + value
    if (record.value.empty()) {
        write_signed_varint(body, -1);
    } else {
        write_signed_varint(body, static_cast<int64_t>(record.value.size()));
        write_raw(body, record.value.data(), record.value.size());
    }

    // headers
    write_signed_varint(body, static_cast<int64_t>(record.headers.size()));
    for (const auto& hdr : record.headers) {
        write_signed_varint(body, static_cast<int64_t>(hdr.key.size()));
        if (!hdr.key.empty())
            write_raw(body, hdr.key.data(), hdr.key.size());

        if (hdr.value.empty()) {
            write_signed_varint(body, -1);
        } else {
            write_signed_varint(body, static_cast<int64_t>(hdr.value.size()));
            write_raw(body, hdr.value.data(), hdr.value.size());
        }
    }

    // Write record: varint length + body
    write_signed_varint(buf, static_cast<int64_t>(body.size()));
    write_raw(buf, body.data(), body.size());
}

// --- Full record batch ---

void serialize_record_batch(std::vector<char>& buf,
                            const tp::RecordBatch& batch,
                            int64_t base_offset,
                            int32_t partition_leader_epoch,
                            int64_t producer_id,
                            int16_t producer_epoch,
                            int32_t base_sequence) noexcept {

    if (batch.records.empty()) {
        // Empty batch — produce minimal valid batch
        serialize_record_batch_header(buf, base_offset, partition_leader_epoch,
                                       2,  // magic
                                       static_cast<int16_t>(batch.compression),
                                       0,  // last_offset_delta
                                       0,  // base_timestamp
                                       0,  // max_timestamp
                                       producer_id, producer_epoch, base_sequence,
                                       0,  // record_count
                                       0); // crc (will be 0 for empty)
        return;
    }

    // Mark position before batch header
    size_t batch_start = buf.size();

    // Compute timestamps
    int64_t base_timestamp = batch.records[0].timestamp;
    int64_t max_timestamp = base_timestamp;
    for (const auto& rec : batch.records) {
        if (rec.timestamp < base_timestamp) base_timestamp = rec.timestamp;
        if (rec.timestamp > max_timestamp) max_timestamp = rec.timestamp;
    }

    int32_t last_offset_delta = static_cast<int32_t>(batch.records.size() - 1);
    int16_t attributes = static_cast<int16_t>(batch.compression) & 0x0007;
    if (batch.is_transactional) {
        attributes |= 0x0010;  // kAttrTransactionalBit
    }

    int32_t record_count = static_cast<int32_t>(batch.records.size());

    // Write batch header with placeholder CRC and batch_length
    // We'll compute the CRC after writing all records.

    write_int64_be(buf, base_offset);

    size_t batch_length_pos = buf.size();
    write_int32_be(buf, 0);  // placeholder batch_length

    write_int32_be(buf, partition_leader_epoch);
    write_int8(buf, 2);  // magic v2

    size_t crc_pos = buf.size();
    write_int32_be(buf, 0);  // placeholder CRC

    write_int16_be(buf, attributes);
    write_int32_be(buf, last_offset_delta);
    write_int64_be(buf, base_timestamp);
    write_int64_be(buf, max_timestamp);
    write_int64_be(buf, producer_id);
    write_int16_be(buf, producer_epoch);
    write_int32_be(buf, base_sequence);
    write_int32_be(buf, record_count);

    // Write records
    for (size_t i = 0; i < batch.records.size(); ++i) {
        int64_t ts_delta = batch.records[i].timestamp - base_timestamp;
        int32_t offset_delta = static_cast<int32_t>(i);
        serialize_record(buf, batch.records[i], ts_delta, offset_delta);
    }

    size_t batch_end = buf.size();

    // Patch batch_length = total bytes after batch_length field
    // batch_length covers from after batch_length through end of records
    int32_t batch_length = static_cast<int32_t>(batch_end - (batch_length_pos + 4));
    {
        uint32_t raw = static_cast<uint32_t>(batch_length);
        buf[batch_length_pos]     = static_cast<char>((raw >> 24) & 0xFF);
        buf[batch_length_pos + 1] = static_cast<char>((raw >> 16) & 0xFF);
        buf[batch_length_pos + 2] = static_cast<char>((raw >> 8) & 0xFF);
        buf[batch_length_pos + 3] = static_cast<char>(raw & 0xFF);
    }

    // Compute CRC over everything after the CRC field
    // CRC covers: attributes .. end of records
    size_t crc_body_start = crc_pos + 4;
    size_t crc_body_len = batch_end - crc_body_start;
    uint32_t crc = compute_crc32c(&buf[crc_body_start], crc_body_len);

    // Patch CRC
    {
        uint32_t raw = crc;
        buf[crc_pos]     = static_cast<char>((raw >> 24) & 0xFF);
        buf[crc_pos + 1] = static_cast<char>((raw >> 16) & 0xFF);
        buf[crc_pos + 2] = static_cast<char>((raw >> 8) & 0xFF);
        buf[crc_pos + 3] = static_cast<char>(raw & 0xFF);
    }

    (void)batch_start;
}

// ============================================================================
// Complete Request/Response Frame Builders
// ============================================================================

std::vector<char> build_request_frame(int16_t api_key,
                                      int16_t api_version,
                                      int32_t correlation_id,
                                      const std::string& client_id,
                                      const std::vector<char>& body) {
    std::vector<char> frame;

    // Request header size:
    //   api_key(2) + api_version(2) + correlation_id(4) + client_id(string) = 8+string
    // We need to know the total payload size for the 4-byte length prefix.
    size_t header_size = 2 + 2 + 4;  // api_key, api_version, correlation_id

    // client_id as compact string
    int16_t client_id_len = static_cast<int16_t>(client_id.size());
    header_size += 2 + static_cast<size_t>(std::max(0, static_cast<int>(client_id_len)));

    size_t total_payload = header_size + body.size();
    size_t total_frame = 4 + total_payload;  // 4-byte length prefix

    frame.reserve(total_frame);

    // Write 4-byte length prefix (total payload length)
    write_int32_be(frame, static_cast<int32_t>(total_payload));

    // Write header
    write_int16_be(frame, api_key);
    write_int16_be(frame, api_version);
    write_int32_be(frame, correlation_id);
    write_string(frame, client_id);

    // Write body
    if (!body.empty()) {
        frame.insert(frame.end(), body.begin(), body.end());
    }

    return frame;
}

std::vector<char> build_response_frame(int32_t correlation_id,
                                       const std::vector<char>& body) {
    std::vector<char> frame;

    // Response header: correlation_id (4 bytes)
    size_t total_payload = 4 + body.size();
    size_t total_frame = 4 + total_payload;

    frame.reserve(total_frame);

    write_int32_be(frame, static_cast<int32_t>(total_payload));
    write_int32_be(frame, correlation_id);

    if (!body.empty()) {
        frame.insert(frame.end(), body.begin(), body.end());
    }

    return frame;
}

std::vector<char> build_error_response_frame(int32_t correlation_id,
                                             tp::ErrorCode error_code,
                                             const std::string& error_message) {
    std::vector<char> body;

    // Error response body:
    //   INT16 error_code
    //   nullable_string error_message
    write_int16_be(body, static_cast<int16_t>(error_code));
    write_nullable_string(body, error_message);

    return build_response_frame(correlation_id, body);
}

} // namespace torrent::network
