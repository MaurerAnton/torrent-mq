#pragma once

/**
 * protocol_serializer.h — Big-Endian Binary Protocol Serializer
 *
 * Serializes all Kafka-compatible and torrent-native protocol messages
 * to wire format.  All integers are big-endian.  Strings use INT16 length
 * prefixing (nullable uses -1).  Arrays use INT32 count prefixes.
 *
 * These functions mirror the parsing code in produce_handler.cpp,
 * fetch_handler.cpp, etc. — they are the write-side counterpart.
 */

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "torrent/common/types.h"
#include "torrent/network/protocol.h"

namespace torrent::network {

namespace tp = torrent::protocol;

// ============================================================================
// Wire-type primitives
// ============================================================================

/// Write big-endian INT8 to buffer.
void write_int8(std::vector<char>& buf, int8_t val) noexcept;

/// Write big-endian INT16 to buffer.
void write_int16_be(std::vector<char>& buf, int16_t val) noexcept;

/// Write big-endian INT32 to buffer.
void write_int32_be(std::vector<char>& buf, int32_t val) noexcept;

/// Write big-endian INT64 to buffer.
void write_int64_be(std::vector<char>& buf, int64_t val) noexcept;

/// Write big-endian UINT32 to buffer.
void write_uint32_be(std::vector<char>& buf, uint32_t val) noexcept;

/// Write a non-nullable string (INT16 length prefix + bytes).
void write_string(std::vector<char>& buf, const std::string& str) noexcept;
void write_string(std::vector<char>& buf, std::string_view str) noexcept;

/// Write a nullable string (INT16 length, -1 for null, else bytes).
void write_nullable_string(std::vector<char>& buf, const std::string& str) noexcept;

/// Write a length-delimited byte array (INT32 length + bytes).
void write_bytes(std::vector<char>& buf, const std::vector<uint8_t>& data) noexcept;
void write_bytes(std::vector<char>& buf, const uint8_t* data, size_t len) noexcept;

/// Write unsigned VARINT.
void write_unsigned_varint(std::vector<char>& buf, uint64_t val) noexcept;

/// Write signed VARINT (ZigZag-encoded).
void write_signed_varint(std::vector<char>& buf, int64_t val) noexcept;

/// Write a boolean as INT8 (0 or 1).
void write_bool(std::vector<char>& buf, bool val) noexcept;

/// Write error_code as INT16.
void write_error_code(std::vector<char>& buf, torrent::error_code ec) noexcept;
void write_error_code(std::vector<char>& buf, tp::ErrorCode ec) noexcept;

// ============================================================================
// Kafka API serializers (Request → wire format)
// ============================================================================

/// Serialize ProduceRequest (api_key=0) to wire format.
void serialize_produce_request(std::vector<char>& buf, const tp::ProduceRequest& req);

/// Serialize FetchRequest (api_key=1) to wire format.
void serialize_fetch_request(std::vector<char>& buf, const tp::FetchRequest& req);

/// Serialize ListOffsetsRequest (api_key=2) to wire format.
void serialize_list_offsets_request(std::vector<char>& buf,
                                    const tp::ListOffsetsRequest& req);

/// Serialize MetadataRequest (api_key=3) to wire format.
void serialize_metadata_request(std::vector<char>& buf,
                                const tp::MetadataRequest& req);

/// Serialize OffsetCommitRequest (api_key=8) to wire format.
void serialize_offset_commit_request(std::vector<char>& buf,
                                     const tp::OffsetCommitRequest& req);

/// Serialize OffsetFetchRequest (api_key=9) to wire format.
void serialize_offset_fetch_request(std::vector<char>& buf,
                                    const tp::OffsetFetchRequest& req);

/// Serialize FindCoordinatorRequest (api_key=10) to wire format.
void serialize_find_coordinator_request(std::vector<char>& buf,
                                        const tp::FindCoordinatorRequest& req);

/// Serialize JoinGroupRequest (api_key=11) to wire format.
void serialize_join_group_request(std::vector<char>& buf,
                                  const tp::JoinGroupRequest& req);

/// Serialize HeartbeatRequest (api_key=12) to wire format.
void serialize_heartbeat_request(std::vector<char>& buf,
                                 const tp::HeartbeatRequest& req);

/// Serialize LeaveGroupRequest (api_key=13) to wire format.
void serialize_leave_group_request(std::vector<char>& buf,
                                   const tp::LeaveGroupRequest& req);

/// Serialize SyncGroupRequest (api_key=14) to wire format.
void serialize_sync_group_request(std::vector<char>& buf,
                                  const tp::SyncGroupRequest& req);

/// Serialize DescribeConfigsRequest (api_key=32) to wire format.
void serialize_describe_configs_request(std::vector<char>& buf,
                                        const tp::DescribeConfigsRequest& req);

/// Serialize AlterConfigsRequest (api_key=33) to wire format.
void serialize_alter_configs_request(std::vector<char>& buf,
                                     const tp::AlterConfigsRequest& req);

// ============================================================================
// Kafka API serializers (Response → wire format)
// ============================================================================

/// Serialize ProduceResponse (api_key=0) to wire format.
void serialize_produce_response(std::vector<char>& buf,
                                const tp::ProduceResponse& res);

/// Serialize FetchResponse (api_key=1) to wire format.
void serialize_fetch_response(std::vector<char>& buf,
                              const tp::FetchResponse& res);

/// Serialize ListOffsetsResponse (api_key=2) to wire format.
void serialize_list_offsets_response(std::vector<char>& buf,
                                     const tp::ListOffsetsResponse& res);

/// Serialize MetadataResponse (api_key=3) to wire format.
void serialize_metadata_response(std::vector<char>& buf,
                                 const tp::MetadataResponse& res);

/// Serialize OffsetCommitResponse (api_key=8) to wire format.
void serialize_offset_commit_response(std::vector<char>& buf,
                                      const tp::OffsetCommitResponse& res);

/// Serialize OffsetFetchResponse (api_key=9) to wire format.
void serialize_offset_fetch_response(std::vector<char>& buf,
                                     const tp::OffsetFetchResponse& res);

/// Serialize FindCoordinatorResponse (api_key=10) to wire format.
void serialize_find_coordinator_response(std::vector<char>& buf,
                                         const tp::FindCoordinatorResponse& res);

/// Serialize JoinGroupResponse (api_key=11) to wire format.
void serialize_join_group_response(std::vector<char>& buf,
                                   const tp::JoinGroupResponse& res);

/// Serialize HeartbeatResponse (api_key=12) to wire format.
void serialize_heartbeat_response(std::vector<char>& buf,
                                  const tp::HeartbeatResponse& res);

/// Serialize LeaveGroupResponse (api_key=13) to wire format.
void serialize_leave_group_response(std::vector<char>& buf,
                                    const tp::LeaveGroupResponse& res);

/// Serialize SyncGroupResponse (api_key=14) to wire format.
void serialize_sync_group_response(std::vector<char>& buf,
                                   const tp::SyncGroupResponse& res);

/// Serialize DescribeConfigsResponse (api_key=32) to wire format.
void serialize_describe_configs_response(std::vector<char>& buf,
                                         const tp::DescribeConfigsResponse& res);

/// Serialize AlterConfigsResponse (api_key=33) to wire format.
void serialize_alter_configs_response(std::vector<char>& buf,
                                      const tp::AlterConfigsResponse& res);

// ============================================================================
// RecordBatch / Record serializers
// ============================================================================

/// Serialize a record batch header (v2, magic=2) — writes base_offset through
/// record_count but NOT the records themselves.  CRC is computed over the
/// batch body (from attributes to end of records) and written.
void serialize_record_batch_header(std::vector<char>& buf,
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
                                   uint32_t crc) noexcept;

/// Serialize a single record body (v2 format with varint lengths).
void serialize_record(std::vector<char>& buf,
                      const tp::Record& record,
                      int64_t timestamp_delta,
                      int32_t offset_delta) noexcept;

/// Serialize a full record batch including records.
void serialize_record_batch(std::vector<char>& buf,
                            const tp::RecordBatch& batch,
                            int64_t base_offset,
                            int32_t partition_leader_epoch,
                            int64_t producer_id,
                            int16_t producer_epoch,
                            int32_t base_sequence) noexcept;

// ============================================================================
// Complete request/response frame builders
// ============================================================================

/// Build a complete request frame: 4-byte length prefix + header + body.
/// Returns the full wire-format frame bytes.
std::vector<char> build_request_frame(int16_t api_key,
                                      int16_t api_version,
                                      int32_t correlation_id,
                                      const std::string& client_id,
                                      const std::vector<char>& body);

/// Build a complete response frame: 4-byte length prefix + correlation_id + body.
std::vector<char> build_response_frame(int32_t correlation_id,
                                       const std::vector<char>& body);

/// Build a simple error response frame.
std::vector<char> build_error_response_frame(int32_t correlation_id,
                                             tp::ErrorCode error_code,
                                             const std::string& error_message);

} // namespace torrent::network
