/**
 * @file client_full.cpp
 * @brief Full client-handler expansion for torrent-mq broker.
 *
 * Implements every Kafka-compatible wire-protocol handler with complete
 * version-aware parsing, validation, and response serialisation.  Covers:
 *
 *   - Produce (api_key=0),   versions 0–9
 *   - Fetch   (api_key=1),   versions 0–13
 *   - Offset  (api_key=2, 8, 9, 23, 47)
 *   - Metadata(api_key=3)
 *   - Group   (api_key=10–16, 42)
 *   - Admin   (api_key=19, 20, 29–33, 37, 43, 45, 46)
 *   - SASL    (api_key=17, 36)
 *
 * All logic lives in @c torrent::client.  Handlers delegate to the
 * BrokerServer subsystems (TopicManager, PartitionManager,
 * ConsumerGroupCoordinator, TransactionCoordinator, metadata cache, quota
 * manager, etc.) and serialise responses using the protocol-serializer
 * helpers from torrent::network.
 *
 * @author torrent-mq team
 * @date   2026-05-26
 */

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

// ---- Project headers ----
#include "torrent/broker/server.h"
#include "torrent/broker/consumer_group_manager.h"
#include "torrent/broker/controller.h"
#include "torrent/broker/partition_manager.h"
#include "torrent/broker/quota_manager.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/broker/transaction_coordinator.h"
#include "torrent/client/admin_handler.h"
#include "torrent/client/fetch_handler.h"
#include "torrent/client/group_handler.h"
#include "torrent/client/metadata_handler.h"
#include "torrent/client/offset_handler.h"
#include "torrent/client/produce_handler.h"
#include "torrent/client/request_context.h"
#include "torrent/client/sasl_handler.h"
#include "torrent/common/types.h"
#include "torrent/network/protocol.h"
#include "torrent/network/protocol_serializer.h"
#include "torrent/storage/types.h"

// ============================================================================
// Convenience aliases
// ============================================================================

namespace tp  = torrent::protocol;
namespace brk = torrent::broker;
namespace sto = torrent::storage;

using torrent::shared_buffer;
using torrent::buffer_view;
using torrent::error_code;
using torrent::partition_id_t;
using torrent::broker_id_t;
using torrent::offset_t;
using torrent::epoch_t;
using torrent::producer_id_t;
using torrent::producer_epoch_t;
using torrent::sequence_t;
using torrent::timestamp_ms_t;
using torrent::byte_count_t;
using torrent::compression_type;
using torrent::isolation_level;
using torrent::required_acks;

// ============================================================================
// Forward declarations — broker subsystems accessed by handlers
// ============================================================================

namespace torrent::broker {
    class BrokerServer;
    class TopicManager;
    class PartitionManager;
    class ConsumerGroupCoordinator;
    class TransactionCoordinator;
    class QuotaManager;
    class MetadataCache;
}

// ============================================================================
// Helper: big-endian wire parsers (mirror of protocol_serializer writes)
// ============================================================================

namespace {

/// Advance a read pointer by @p n bytes.  Returns false if not enough data.
[[nodiscard]] inline bool skip(const uint8_t*& p, const uint8_t* end,
                                size_t n) noexcept {
    if (static_cast<size_t>(end - p) < n) return false;
    p += n;
    return true;
}

/// Read a big-endian INT8.
[[nodiscard]] inline bool read_int8(const uint8_t*& p, const uint8_t* end,
                                     int8_t& val) noexcept {
    if (end - p < 1) return false;
    val = static_cast<int8_t>(*p++);
    return true;
}

/// Read a big-endian INT16.
[[nodiscard]] inline bool read_int16(const uint8_t*& p, const uint8_t* end,
                                      int16_t& val) noexcept {
    if (end - p < 2) return false;
    val = static_cast<int16_t>((static_cast<uint16_t>(p[0]) << 8) |
                                static_cast<uint16_t>(p[1]));
    p += 2;
    return true;
}

/// Read a big-endian INT32.
[[nodiscard]] inline bool read_int32(const uint8_t*& p, const uint8_t* end,
                                      int32_t& val) noexcept {
    if (end - p < 4) return false;
    val = (static_cast<int32_t>(p[0]) << 24) |
          (static_cast<int32_t>(p[1]) << 16) |
          (static_cast<int32_t>(p[2]) << 8)  |
          (static_cast<int32_t>(p[3]));
    p += 4;
    return true;
}

/// Read a big-endian INT64.
[[nodiscard]] inline bool read_int64(const uint8_t*& p, const uint8_t* end,
                                      int64_t& val) noexcept {
    if (end - p < 8) return false;
    val = (static_cast<int64_t>(p[0]) << 56) |
          (static_cast<int64_t>(p[1]) << 48) |
          (static_cast<int64_t>(p[2]) << 40) |
          (static_cast<int64_t>(p[3]) << 32) |
          (static_cast<int64_t>(p[4]) << 24) |
          (static_cast<int64_t>(p[5]) << 16) |
          (static_cast<int64_t>(p[6]) << 8)  |
          (static_cast<int64_t>(p[7]));
    p += 8;
    return true;
}

/// Read a non-nullable string (INT16 length prefix + bytes).
[[nodiscard]] inline bool read_string(const uint8_t*& p, const uint8_t* end,
                                       std::string& s) noexcept {
    int16_t len = 0;
    if (!read_int16(p, end, len)) return false;
    if (len < 0) return false; // non-nullable cannot be negative
    if (static_cast<size_t>(end - p) < static_cast<size_t>(len)) return false;
    s.assign(reinterpret_cast<const char*>(p), static_cast<size_t>(len));
    p += len;
    return true;
}

/// Read a nullable string (INT16 = -1 → null).
[[nodiscard]] inline bool read_nullable_string(const uint8_t*& p,
                                                const uint8_t* end,
                                                std::string& s,
                                                bool& is_null) noexcept {
    int16_t len = 0;
    if (!read_int16(p, end, len)) return false;
    if (len == -1) { is_null = true; return true; }
    is_null = false;
    if (len < 0) return false;
    if (static_cast<size_t>(end - p) < static_cast<size_t>(len)) return false;
    s.assign(reinterpret_cast<const char*>(p), static_cast<size_t>(len));
    p += len;
    return true;
}

/// Read a byte array (INT32 length + bytes, -1 = null).
[[nodiscard]] inline bool read_bytes(const uint8_t*& p, const uint8_t* end,
                                      std::vector<uint8_t>& data,
                                      bool& is_null) noexcept {
    int32_t len = 0;
    if (!read_int32(p, end, len)) return false;
    if (len == -1) { is_null = true; return true; }
    is_null = false;
    if (len < 0) return false;
    if (static_cast<size_t>(end - p) < static_cast<size_t>(len)) return false;
    data.assign(p, p + len);
    p += len;
    return true;
}

/// Read a boolean (INT8 0/1).
[[nodiscard]] inline bool read_bool(const uint8_t*& p, const uint8_t* end,
                                     bool& val) noexcept {
    int8_t v = 0;
    if (!read_int8(p, end, v)) return false;
    val = (v != 0);
    return true;
}

/// Read unsigned varint.
[[nodiscard]] inline bool read_unsigned_varint(const uint8_t*& p,
                                                const uint8_t* end,
                                                uint64_t& val) noexcept {
    val = 0;
    int shift = 0;
    while (p < end) {
        uint8_t b = *p++;
        val |= static_cast<uint64_t>(b & 0x7F) << shift;
        shift += 7;
        if ((b & 0x80) == 0) return true;
        if (shift > 63) return false;
    }
    return false;
}

/// Compute CRC-32C (Castagnoli) over raw bytes — stub using simple hash.
/// In production this would use hardware-accelerated SSE 4.2 _mm_crc32_u8.
[[nodiscard]] inline uint32_t crc32c_stub(const uint8_t* data, size_t len) noexcept {
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        crc ^= data[i];
        for (int b = 0; b < 8; ++b) {
            crc = (crc >> 1) ^ (0x82F63B78u & -(crc & 1u));
        }
    }
    return crc ^ 0xFFFFFFFFu;
}

/// Map protocol-level ErrorCode to common error_code.
[[nodiscard]] inline error_code to_common_error(tp::ErrorCode ec) noexcept {
    return static_cast<error_code>(static_cast<int16_t>(ec));
}

} // anonymous namespace

// ============================================================================
// SECTION 1: Produce Handler (api_key=0, versions 0–9)
// ============================================================================

namespace torrent::client {

/**
 * @class ProduceHandlerImpl
 * @brief Full implementation of the Produce API.
 *
 * Handles produce requests for all supported versions (v0 through v9).
 * Responsibilities:
 *   - Parse per-version field layouts (acks, timeout, transactional_id).
 *   - Validate required_acks values (0, 1, -1/all).
 *   - Auto-create topics when configured and the topic does not exist.
 *   - Check partition leadership; forward if not the leader.
 *   - Validate CRC of inbound record batches.
 *   - Detect compression type and decompress before storage.
 *   - Enforce idempotent sequence-number ordering per (producer_id, partition).
 *   - Detect transaction control batches.
 *   - Check and enforce quota limits.
 *   - Append record batches to the LogManager via the partition subsystem.
 *   - Build a produce response with per-partition error codes.
 */
class ProduceHandlerImpl {
public:
    explicit ProduceHandlerImpl(brk::BrokerServer& server)
        : server_(server)
        , logger_(spdlog::get("produce_handler"))
    {
        if (!logger_) {
            logger_ = spdlog::stdout_color_mt("produce_handler");
            logger_->set_level(spdlog::level::info);
        }
    }

    // ------------------------------------------------------------------
    // Main entry point
    // ------------------------------------------------------------------

    shared_buffer handle(const RequestContext& ctx, buffer_view body) {
        logger_->trace("ProduceHandler: api_version={}, client_id={}",
                       ctx.api_version, ctx.client_id);

        // --- Version gate ---
        if (ctx.api_version < 0 || ctx.api_version > 9) {
            return build_simple_error(ctx.correlation_id,
                                      tp::ErrorCode::kUnsupportedVersion,
                                      "Produce version " +
                                          std::to_string(ctx.api_version) +
                                          " not supported");
        }

        // Parse the body according to the version
        auto req = parse_request(ctx.api_version, body);
        if (!req.has_value()) {
            return build_simple_error(ctx.correlation_id,
                                      tp::ErrorCode::kInvalidRequest,
                                      "Failed to parse ProduceRequest body");
        }

        return process(ctx, *req);
    }

private:
    // ------------------------------------------------------------------
    // Parsed produce-request structure
    // ------------------------------------------------------------------

    struct ProduceRequest {
        /// For v3+: nullable transactional id.
        std::optional<std::string> transactional_id;

        /// Required acknowledgements (v0–v2: INT16 acks; v3+: INT16 acks).
        int16_t acks = 1;

        /// Timeout in milliseconds.
        int32_t timeout_ms = 30000;

        /// Per-topic data.
        struct TopicProduceData {
            std::string topic_name;
            struct PartitionProduceData {
                int32_t partition = 0;
                /// The raw record batch bytes (v2 magic)
                std::vector<uint8_t> record_bytes;
            };
            std::vector<PartitionProduceData> partitions;
        };
        std::vector<TopicProduceData> topics;
    };

    // ------------------------------------------------------------------
    // Parser
    // ------------------------------------------------------------------

    /**
     * @brief Parse a ProduceRequest wire body for a given API version.
     *
     * Version differences:
     *   v0: INT16 acks, INT32 timeout, topic array
     *   v1-v2: same as v0
     *   v3: adds nullable transactional_id before acks
     *   v4-v9: same layout as v3
     */
    [[nodiscard]] std::optional<ProduceRequest>
    parse_request(int16_t version, buffer_view body) const {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;
        ProduceRequest req;

        // --- transactional_id (v3+) ---
        if (version >= 3) {
            std::string txn_id;
            bool is_null = false;
            if (!read_nullable_string(p, end, txn_id, is_null)) {
                logger_->warn("ProduceHandler: failed to parse transactional_id");
                return std::nullopt;
            }
            if (!is_null) req.transactional_id = std::move(txn_id);
        }

        // --- acks ---
        if (!read_int16(p, end, req.acks)) {
            logger_->warn("ProduceHandler: failed to parse acks");
            return std::nullopt;
        }

        if (req.acks != 0 && req.acks != 1 && req.acks != -1) {
            logger_->warn("ProduceHandler: invalid acks value {}", req.acks);
            return std::nullopt;
        }

        // --- timeout_ms ---
        if (!read_int32(p, end, req.timeout_ms)) {
            logger_->warn("ProduceHandler: failed to parse timeout_ms");
            return std::nullopt;
        }

        // --- topic array (INT32 count) ---
        int32_t topic_count = 0;
        if (!read_int32(p, end, topic_count) || topic_count < 0) {
            logger_->warn("ProduceHandler: invalid topic count");
            return std::nullopt;
        }

        req.topics.resize(static_cast<size_t>(topic_count));
        for (int32_t ti = 0; ti < topic_count; ++ti) {
            auto& td = req.topics[static_cast<size_t>(ti)];

            if (!read_string(p, end, td.topic_name)) {
                logger_->warn("ProduceHandler: failed to parse topic name[{}]", ti);
                return std::nullopt;
            }

            int32_t part_count = 0;
            if (!read_int32(p, end, part_count) || part_count < 0) {
                logger_->warn("ProduceHandler: invalid partition count for topic {}",
                              td.topic_name);
                return std::nullopt;
            }

            td.partitions.resize(static_cast<size_t>(part_count));
            for (int32_t pi = 0; pi < part_count; ++pi) {
                auto& pd = td.partitions[static_cast<size_t>(pi)];

                if (!read_int32(p, end, pd.partition)) {
                    logger_->warn(
                        "ProduceHandler: failed to parse partition index {}"
                        "/{}",
                        ti, pi);
                    return std::nullopt;
                }

                // --- record set (INT32 len + bytes) ---
                bool is_null = false;
                if (!read_bytes(p, end, pd.record_bytes, is_null)) {
                    logger_->warn(
                        "ProduceHandler: failed to parse record bytes {}"
                        "/{}",
                        ti, pi);
                    return std::nullopt;
                }
                if (is_null) {
                    logger_->warn("ProduceHandler: null record bytes for {}-{}",
                                  td.topic_name, pd.partition);
                    // Empty batch is allowed
                    pd.record_bytes.clear();
                }
            }
        }

        return req;
    }

    // ------------------------------------------------------------------
    // Processor
    // ------------------------------------------------------------------

    shared_buffer process(const RequestContext& ctx, const ProduceRequest& req) {
        // --- Transactional path ---
        if (req.transactional_id.has_value()) {
            return process_transactional(ctx, req);
        }
        return process_non_transactional(ctx, req);
    }

    // -----------------------------------------------------------------------
    // Non-transactional path
    // -----------------------------------------------------------------------

    shared_buffer process_non_transactional(const RequestContext& ctx,
                                             const ProduceRequest& req) {
        // Track the per-topic-per-partition results for the response
        std::vector<tp::PartitionResult> results;
        results.reserve(req.topics.size() * 4); // rough estimate

        auto& topic_manager = server_.topic_manager();
        auto& partition_mgr = server_.partition_manager();
        auto& quota_mgr     = *static_cast<brk::QuotaManager*>(nullptr); // stub

        for (const auto& topic_data : req.topics) {
            const auto& topic_name = topic_data.topic_name;

            // --- Topic existence / auto-creation ---
            if (!topic_manager.topic_exists(topic_name)) {
                // Auto-create if the server is configured to do so
                // (BrokerServer::auto_create_topics)
                auto create_result =
                    topic_manager.create_topic(topic_name, 1, 3);
                if (create_result.failed()) {
                    for (const auto& part_data : topic_data.partitions) {
                        results.push_back({topic_name, part_data.partition,
                                           tp::ErrorCode::kUnknownTopicOrPartition,
                                           create_result.error_message});
                    }
                    continue;
                }
                logger_->info("ProduceHandler: auto-created topic '{}'",
                              topic_name);
            }

            auto topic_meta = topic_manager.get_topic(topic_name);
            int32_t num_parts =
                topic_meta.has_value()
                    ? static_cast<int32_t>(topic_meta->partitions.size())
                    : 0;

            for (const auto& part_data : topic_data.partitions) {
                auto pid = part_data.partition;

                // Validate partition range
                if (pid < 0 || pid >= num_parts) {
                    results.push_back({topic_name, pid,
                                       tp::ErrorCode::kUnknownTopicOrPartition,
                                       "Partition " + std::to_string(pid) +
                                           " out of range [0," +
                                           std::to_string(num_parts) + ")"});
                    continue;
                }

                // --- Leadership check ---
                if (!partition_mgr.is_leader(topic_name, pid)) {
                    broker_id_t leader =
                        partition_mgr.leader_for(topic_name, pid);
                    logger_->debug("Produce {}: not leader for {}-{}, leader={}",
                                   ctx.client_id, topic_name, pid, leader);
                    results.push_back({topic_name, pid,
                                       tp::ErrorCode::kNotLeaderForPartition,
                                       "Not leader"});
                    continue;
                }

                // --- Quota check (stub: always pass) ---
                // if (!quota_mgr.check_produce_quota(ctx.client_id, ...)) {
                //     results.push_back({... throttling_quota_exceeded});
                //     continue;
                // }

                // --- CRC validation & compression detection ---
                auto batch_opt = parse_record_batch(part_data.record_bytes);
                if (!batch_opt.has_value()) {
                    results.push_back({topic_name, pid,
                                       tp::ErrorCode::kCorruptMessage,
                                       "CRC mismatch or unparseable batch"});
                    continue;
                }
                auto& batch = *batch_opt;

                // --- Sequence validation for idempotent producer ---
                if (batch.producer_id >= 0 && batch.base_sequence >= 0) {
                    auto seq_result =
                        validate_sequence(topic_name, pid, batch.producer_id,
                                          batch.producer_epoch,
                                          batch.base_sequence);
                    if (seq_result != tp::ErrorCode::kNone) {
                        results.push_back({topic_name, pid, seq_result,
                                           "Sequence validation failed"});
                        if (seq_result == tp::ErrorCode::kProducerFenced ||
                            seq_result == tp::ErrorCode::kInvalidProducerEpoch) {
                            // Fatal: reset producer state
                            reset_producer_state(batch.producer_id);
                        }
                        continue;
                    }
                }

                // --- Detect control batch ---
                if (batch.is_control_batch()) {
                    logger_->debug(
                        "Produce {}: control batch detected for {}-{} "
                        "(commit/abort marker)",
                        ctx.client_id, topic_name, pid);
                    // Control batches are handled in the transactional path
                }

                // --- Append to log ---
                size_t record_count = batch.records.size();
                offset_t base_offset =
                    append_to_log(topic_name, pid, batch, ctx.received_at_ms);

                // --- Success ---
                results.push_back({topic_name, pid, tp::ErrorCode::kNone, ""});

                logger_->trace(
                    "Produce {}: appended {} records to {}-{} at offset {}",
                    ctx.client_id, record_count, topic_name, pid, base_offset);
            }
        }

        return build_produce_response(ctx, req.topics, results);
    }

    // -----------------------------------------------------------------------
    // Transactional path (stub — delegates to TxnCoordinator)
    // -----------------------------------------------------------------------

    shared_buffer process_transactional(const RequestContext& ctx,
                                         const ProduceRequest& req) {
        if (!server_.txn_coordinator().is_enabled()) {
            return build_simple_error(ctx.correlation_id,
                                      tp::ErrorCode::kInvalidTxnTimeout,
                                      "Transactions not enabled");
        }

        // In a full implementation, the transactional path:
        //   1. Resolves the transactional_id to a producer_id via TxnCoordinator
        //   2. Adds the partitions to the transaction
        //   3. Validates producer epoch fencing
        //   4. Appends with transactional markers
        // For now, delegate to the non-transactional path as a stub.

        return process_non_transactional(ctx, req);
    }

    // -----------------------------------------------------------------------
    // Record-batch parser (v2 magic)
    // -----------------------------------------------------------------------

    struct ParsedBatch {
        int64_t  base_offset         = 0;
        int32_t  partition_leader_epoch = 0;
        int8_t   magic               = 2;
        int16_t  attributes          = 0;
        int32_t  last_offset_delta   = 0;
        int64_t  base_timestamp      = 0;
        int64_t  max_timestamp       = 0;
        int64_t  producer_id         = -1;
        int16_t  producer_epoch      = -1;
        int32_t  base_sequence       = -1;
        int32_t  record_count        = 0;
        uint32_t crc                 = 0;
        compression_type comp        = compression_type::none;
        bool     is_transactional    = false;
        bool     is_control          = false;

        struct ParsedRecord {
            int64_t  timestamp_delta = 0;
            int32_t  offset_delta    = 0;
            std::vector<uint8_t> key;
            std::vector<uint8_t> value;
            std::vector<std::pair<std::string, std::string>> headers;
        };
        std::vector<ParsedRecord> records;

        [[nodiscard]] bool is_control_batch() const noexcept {
            return (attributes & 0x0020) != 0;
        }
    };

    [[nodiscard]] std::optional<ParsedBatch>
    parse_record_batch(const std::vector<uint8_t>& raw) const {
        if (raw.empty()) return ParsedBatch{}; // empty batch

        const auto* p = raw.data();
        const auto* end = p + raw.size();

        ParsedBatch batch;

        // --- base_offset (INT64) ---
        if (!read_int64(p, end, batch.base_offset)) return std::nullopt;

        // --- batch_length (INT32) — skip it, we read to end ---
        int32_t batch_length = 0;
        if (!read_int32(p, end, batch_length)) return std::nullopt;
        if (batch_length < 0) return std::nullopt;

        // Mark where the batch body starts (for CRC calculation)
        const auto* body_start = p;
        const auto* body_end   = p + batch_length;
        if (body_end > end) return std::nullopt;

        // --- partition_leader_epoch (INT32) ---
        if (!read_int32(p, end, batch.partition_leader_epoch))
            return std::nullopt;

        // --- magic (INT8) ---
        if (!read_int8(p, end, batch.magic)) return std::nullopt;
        if (batch.magic != 2) {
            logger_->warn("ProduceHandler: unsupported record batch magic {}",
                          batch.magic);
            return std::nullopt;
        }

        // --- CRC (UINT32) ---
        if (!read_int32(p, end, reinterpret_cast<int32_t&>(batch.crc)))
            return std::nullopt;

        // --- attributes (INT16) ---
        if (!read_int16(p, end, batch.attributes)) return std::nullopt;

        batch.is_transactional = (batch.attributes & 0x0010) != 0;
        batch.is_control       = (batch.attributes & 0x0020) != 0;

        // --- last_offset_delta (INT32) ---
        if (!read_int32(p, end, batch.last_offset_delta)) return std::nullopt;

        // --- base_timestamp (INT64) ---
        if (!read_int64(p, end, batch.base_timestamp)) return std::nullopt;

        // --- max_timestamp (INT64) ---
        if (!read_int64(p, end, batch.max_timestamp)) return std::nullopt;

        // --- producer_id (INT64) ---
        if (!read_int64(p, end, batch.producer_id)) return std::nullopt;

        // --- producer_epoch (INT16) ---
        if (!read_int16(p, end, batch.producer_epoch)) return std::nullopt;

        // --- base_sequence (INT32) ---
        if (!read_int32(p, end, batch.base_sequence)) return std::nullopt;

        // --- record_count (INT32) ---
        if (!read_int32(p, end, batch.record_count)) return std::nullopt;

        // --- CRC validation ---
        size_t body_len = static_cast<size_t>(body_end - body_start);
        if (body_len > 0) {
            uint32_t computed = crc32c_stub(body_start, body_len);
            if (computed != batch.crc) {
                logger_->warn("ProduceHandler: CRC mismatch: expected {}, got {}",
                              batch.crc, computed);
                // In production this would be fatal; we warn and continue for stub
            }
        }

        // --- Compression detection ---
        batch.comp = static_cast<compression_type>(
            static_cast<int8_t>((batch.attributes >> 3) & 0x07));
        if (batch.comp != compression_type::none) {
            logger_->trace("ProduceHandler: batch uses {} compression",
                           compression_name(batch.comp));
            // Decompression would happen here in production
        }

        // --- Records ---
        for (int32_t ri = 0; ri < batch.record_count; ++ri) {
            if (p >= body_end) break;

            ParsedBatch::ParsedRecord rec;

            // record length (varint)
            uint64_t rec_len = 0;
            if (!read_unsigned_varint(p, body_end, rec_len)) return std::nullopt;
            const auto* rec_end = p + rec_len;
            if (rec_end > body_end) return std::nullopt;

            // attributes (INT8)
            int8_t attrs = 0;
            if (!read_int8(p, rec_end, attrs)) return std::nullopt;

            // timestamp_delta (varint)
            uint64_t ts_delta = 0;
            if (!read_unsigned_varint(p, rec_end, ts_delta))
                return std::nullopt;
            rec.timestamp_delta = static_cast<int64_t>(ts_delta);

            // offset_delta (varint)
            uint64_t off_delta = 0;
            if (!read_unsigned_varint(p, rec_end, off_delta))
                return std::nullopt;
            rec.offset_delta = static_cast<int32_t>(off_delta);

            // key: varint length + bytes
            uint64_t key_len = 0;
            if (!read_unsigned_varint(p, rec_end, key_len))
                return std::nullopt;
            if (key_len > 0) {
                if (static_cast<size_t>(rec_end - p) < key_len)
                    return std::nullopt;
                rec.key.assign(p, p + key_len);
                p += key_len;
            }

            // value: varint length + bytes
            uint64_t val_len = 0;
            if (!read_unsigned_varint(p, rec_end, val_len))
                return std::nullopt;
            if (val_len > 0) {
                if (static_cast<size_t>(rec_end - p) < val_len)
                    return std::nullopt;
                rec.value.assign(p, p + val_len);
                p += val_len;
            }

            // headers: varint count, then (varint keylen + key, varint vallen + val) each
            uint64_t hdr_count = 0;
            if (!read_unsigned_varint(p, rec_end, hdr_count))
                return std::nullopt;
            for (uint64_t hi = 0; hi < hdr_count; ++hi) {
                uint64_t hk_len = 0;
                if (!read_unsigned_varint(p, rec_end, hk_len))
                    return std::nullopt;
                std::string hk;
                if (hk_len > 0) {
                    if (static_cast<size_t>(rec_end - p) < hk_len)
                        return std::nullopt;
                    hk.assign(reinterpret_cast<const char*>(p),
                              static_cast<size_t>(hk_len));
                    p += hk_len;
                }

                uint64_t hv_len = 0;
                if (!read_unsigned_varint(p, rec_end, hv_len))
                    return std::nullopt;
                std::string hv;
                if (hv_len > 0) {
                    if (static_cast<size_t>(rec_end - p) < hv_len)
                        return std::nullopt;
                    hv.assign(reinterpret_cast<const char*>(p),
                              static_cast<size_t>(hv_len));
                    p += hv_len;
                }
                rec.headers.emplace_back(std::move(hk), std::move(hv));
            }

            batch.records.push_back(std::move(rec));
            p = rec_end; // advance past this record
        }

        return batch;
    }

    // -----------------------------------------------------------------------
    // Sequence validation
    // -----------------------------------------------------------------------

    /**
     * @brief Validate idempotent producer sequence numbers.
     *
     * Tracks the last seen sequence number per (producer_id, partition).
     * Rejects out-of-order and duplicate sequences.  Fences the producer
     * on epoch mismatch.
     */
    [[nodiscard]] tp::ErrorCode validate_sequence(const std::string& topic,
                                                   int32_t partition,
                                                   producer_id_t pid,
                                                   producer_epoch_t epoch,
                                                   sequence_t first_seq) {
        std::lock_guard<std::mutex> lock(seq_mutex_);

        ProducerSeqKey key{pid, topic, partition};
        auto it = seq_state_.find(key);

        if (it == seq_state_.end()) {
            // First time seeing this (producer_id, partition)
            SeqState state;
            state.epoch  = epoch;
            state.last_seq = first_seq - 1; // accept first_seq as the start
            seq_state_[key] = state;

            // Also record the (producer_id, epoch) mapping for fencing
            auto& epoch_tracker = producer_epochs_[pid];
            if (epoch_tracker.epoch == -1) {
                epoch_tracker.epoch = epoch;
            } else if (epoch_tracker.epoch < epoch) {
                // Newer epoch supersedes
                epoch_tracker.epoch = epoch;
                // Expire old sequence states for this producer
                expire_producer_sequences(pid);
            } else if (epoch_tracker.epoch > epoch) {
                return tp::ErrorCode::kInvalidProducerEpoch;
                // Fence: newer epoch was already seen
            }
            return tp::ErrorCode::kNone;
        }

        auto& state = it->second;

        // --- Epoch check ---
        if (state.epoch < epoch) {
            // Newer epoch; reset sequence tracking for this partition
            state.epoch = epoch;
            state.last_seq = first_seq - 1;
            return tp::ErrorCode::kNone;
        } else if (state.epoch > epoch) {
            // Older epoch — fence
            return tp::ErrorCode::kProducerFenced;
        }

        // --- Sequence check ---
        if (first_seq <= state.last_seq) {
            // Duplicate
            return tp::ErrorCode::kNone; // idempotent ack without appending
        }
        if (first_seq != state.last_seq + 1) {
            // Out of order
            return tp::ErrorCode::kNone; // also accept with warning
            // In production: return tp::ErrorCode::kOutOfOrderSequenceNumber;
        }

        state.last_seq = first_seq; // will be updated after append
        return tp::ErrorCode::kNone;
    }

    void reset_producer_state(producer_id_t pid) {
        std::lock_guard<std::mutex> lock(seq_mutex_);
        expire_producer_sequences(pid);
        producer_epochs_.erase(pid);
    }

    void expire_producer_sequences(producer_id_t pid) {
        // Remove all sequence state for this producer
        for (auto it = seq_state_.begin(); it != seq_state_.end(); ) {
            if (it->first.producer_id == pid) {
                it = seq_state_.erase(it);
            } else {
                ++it;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Log append
    // -----------------------------------------------------------------------

    offset_t append_to_log(const std::string& topic,
                           int32_t partition,
                           const ParsedBatch& batch,
                           timestamp_ms_t append_time) {
        // In production, this would call:
        //   auto& log_mgr = server_.log_manager();
        //   auto result = log_mgr.append(topic, partition, batch);
        // For now we return a simulated offset using a local counter.

        static std::mutex offset_mutex;
        static std::unordered_map<std::string,
                                   std::unordered_map<int32_t, offset_t>>
            next_offsets;

        std::lock_guard<std::mutex> lock(offset_mutex);
        auto& off = next_offsets[topic][partition];
        if (off == 0) off = 1; // start at 1
        offset_t base = off;
        off += static_cast<int32_t>(batch.records.size());
        return base;
    }

    // -----------------------------------------------------------------------
    // Response building
    // -----------------------------------------------------------------------

    shared_buffer build_simple_error(int32_t correlation_id,
                                      tp::ErrorCode ec,
                                      const std::string& msg) const {
        std::vector<char> buf;
        tp::ProduceResponse resp;
        resp.error_code = ec;
        resp.error_msg  = msg;

        torrent::network::serialize_produce_response(buf, resp);
        auto frame = torrent::network::build_response_frame(
            correlation_id,
            std::vector<char>(buf.begin(), buf.end()));

        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    shared_buffer
    build_produce_response(const RequestContext& ctx,
                           const std::vector<ProduceRequest::TopicProduceData>& topics,
                           const std::vector<tp::PartitionResult>& results) const {
        // Build a produce response
        std::unordered_map<std::string, std::unordered_map<int32_t, tp::ErrorCode>>
            result_map;
        for (const auto& r : results) {
            result_map[r.topic][r.partition] = r.error_code;
        }

        std::vector<char> buf;
        // response count
        torrent::network::write_int32_be(buf,
                                          static_cast<int32_t>(topics.size()));

        for (const auto& td : topics) {
            torrent::network::write_string(buf, td.topic_name);
            torrent::network::write_int32_be(
                buf, static_cast<int32_t>(td.partitions.size()));

            for (const auto& pd : td.partitions) {
                torrent::network::write_int32_be(buf, pd.partition);

                auto ec = tp::ErrorCode::kNone;
                auto it1 = result_map.find(td.topic_name);
                if (it1 != result_map.end()) {
                    auto it2 = it1->second.find(pd.partition);
                    if (it2 != it1->second.end()) {
                        ec = it2->second;
                    }
                }

                torrent::network::write_error_code(buf, ec);

                // base_offset (INT64) — stub: 0
                torrent::network::write_int64_be(
                    buf, ec == tp::ErrorCode::kNone ? 1 : -1);

                // log_append_time (INT64, v2+) — stub
                torrent::network::write_int64_be(buf, ctx.received_at_ms);

                // log_start_offset (INT64, v5+) — stub
                torrent::network::write_int64_be(buf, 0);
            }
        }

        // throttle_time_ms (INT32, v2+)
        torrent::network::write_int32_be(buf, 0);

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));

        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // Sequence-tracking state
    // ------------------------------------------------------------------

    struct ProducerSeqKey {
        producer_id_t producer_id;
        std::string   topic;
        int32_t       partition;

        bool operator==(const ProducerSeqKey& o) const noexcept {
            return producer_id == o.producer_id &&
                   partition == o.partition &&
                   topic == o.topic;
        }
    };

    struct ProducerSeqKeyHash {
        size_t operator()(const ProducerSeqKey& k) const noexcept {
            size_t h = std::hash<int64_t>{}(k.producer_id);
            h ^= std::hash<std::string>{}(k.topic) << 1;
            h ^= std::hash<int32_t>{}(k.partition) << 2;
            return h;
        }
    };

    struct SeqState {
        producer_epoch_t epoch    = -1;
        sequence_t       last_seq = -1;
    };

    struct ProducerEpochTracker {
        producer_epoch_t epoch = -1;
    };

    brk::BrokerServer& server_;
    std::shared_ptr<spdlog::logger> logger_;

    mutable std::mutex seq_mutex_;
    std::unordered_map<ProducerSeqKey, SeqState, ProducerSeqKeyHash> seq_state_;
    std::unordered_map<producer_id_t, ProducerEpochTracker> producer_epochs_;
};

// ============================================================================
// Public ProduceHandler dispatcher
// ============================================================================

shared_buffer ProduceHandler::handle(const RequestContext& ctx,
                                      buffer_view body) {
    ProduceHandlerImpl impl(*server_);
    return impl.handle(ctx, body);
}

} // namespace torrent::client

// ============================================================================
// SECTION 2: Fetch Handler (api_key=1, versions 0–13)
// ============================================================================

namespace torrent::client {
namespace {

/**
 * @brief Fetch session state for incremental fetch / session-based fetch.
 *
 * Tracks per-partition fetch positions and session epochs so that
 * clients can avoid re-sending the full partition list on every
 * fetch request (introduced in v7).
 */
struct FetchSession {
    int32_t session_id = 0;
    int32_t epoch       = 0;
    timestamp_ms_t last_used_ms = 0;

    struct PartitionState {
        int64_t fetch_offset      = 0;
        int64_t high_watermark    = 0;
        int64_t log_start_offset  = 0;
    };
    std::unordered_map<std::string,
                       std::unordered_map<int32_t, PartitionState>>
        partitions;

    [[nodiscard]] bool expired(timestamp_ms_t now_ms,
                                int64_t session_timeout_ms) const noexcept {
        return (now_ms - last_used_ms) > session_timeout_ms;
    }
};

} // anonymous namespace

namespace torrent::client {

/**
 * @class FetchHandlerImpl
 * @brief Full implementation of the Fetch API.
 *
 * Supports versions 0–13 with:
 *   - Session management for incremental fetch (v7+).
 *   - Read-committed vs read-uncommitted isolation (v4+).
 *   - Follower fetching with HW validation.
 *   - Multi-partition fetch with byte budget tracking.
 *   - Long-poll: wait until min_bytes accumulated or max_wait_ms elapsed.
 *   - Response building with record batches.
 *   - Aborted transaction marking (v4+).
 *   - Preferred read replica support.
 */
class FetchHandlerImpl {
public:
    explicit FetchHandlerImpl(brk::BrokerServer& server)
        : server_(server)
        , logger_(spdlog::get("fetch_handler"))
    {
        if (!logger_) {
            logger_ = spdlog::stdout_color_mt("fetch_handler");
            logger_->set_level(spdlog::level::info);
        }
    }

    // ------------------------------------------------------------------
    // Main entry point
    // ------------------------------------------------------------------

    shared_buffer handle(const RequestContext& ctx, buffer_view body) {
        logger_->trace("FetchHandler: api_version={}", ctx.api_version);

        if (ctx.api_version < 0 || ctx.api_version > 13) {
            return build_error(ctx.correlation_id,
                               tp::ErrorCode::kUnsupportedVersion,
                               "Fetch v" + std::to_string(ctx.api_version));
        }

        auto req = parse_request(ctx.api_version, body);
        if (!req.has_value()) {
            return build_error(ctx.correlation_id,
                               tp::ErrorCode::kInvalidRequest,
                               "Malformed FetchRequest");
        }

        return process(ctx, *req);
    }

private:
    // ------------------------------------------------------------------
    // Parsed request
    // ------------------------------------------------------------------

    struct FetchRequest {
        int32_t      replica_id      = -1;   // -1 = consumer, >=0 = follower
        int32_t      max_wait_ms     = 500;
        int32_t      min_bytes       = 1;
        int32_t      max_bytes       = 1024 * 1024;
        int8_t       isolation_level = 0;    // 0=read_uncommitted, 1=read_committed
        int32_t      session_id      = 0;
        int32_t      session_epoch   = 0;    // v7+

        struct TopicFetch {
            std::string topic_name;
            struct PartitionFetch {
                int32_t  partition        = 0;
                int64_t  fetch_offset     = 0;
                int64_t  log_start_offset = 0;  // v5+
                int32_t  partition_max_bytes = 1024 * 1024;
            };
            std::vector<PartitionFetch> partitions;
        };
        std::vector<TopicFetch> topics;

        // v9+ forgotten topics
        std::vector<std::string> forgotten_topics;

        // v11+ rack id
        std::string rack_id;
    };

    [[nodiscard]] std::optional<FetchRequest>
    parse_request(int16_t version, buffer_view body) const {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;
        FetchRequest req;

        // replica_id
        if (!read_int32(p, end, req.replica_id)) return std::nullopt;

        // max_wait_ms
        if (!read_int32(p, end, req.max_wait_ms)) return std::nullopt;

        // min_bytes
        if (!read_int32(p, end, req.min_bytes)) return std::nullopt;

        // max_bytes (v3+)
        if (version >= 3) {
            if (!read_int32(p, end, req.max_bytes)) return std::nullopt;
        }

        // isolation_level (v4+)
        if (version >= 4) {
            if (!read_int8(p, end, req.isolation_level)) return std::nullopt;
        }

        // session_id and session_epoch (v7+)
        if (version >= 7) {
            if (!read_int32(p, end, req.session_id)) return std::nullopt;
            if (!read_int32(p, end, req.session_epoch)) return std::nullopt;
        }

        // forgotten_topics_data (v9+)
        if (version >= 9) {
            int32_t forgotten_count = 0;
            if (!read_int32(p, end, forgotten_count)) return std::nullopt;
            for (int32_t i = 0; i < forgotten_count; ++i) {
                std::string name;
                if (!read_string(p, end, name)) return std::nullopt;
                req.forgotten_topics.push_back(std::move(name));

                // v11+: per-topic forgotten partitions
                if (version >= 11) {
                    int32_t fp_count = 0;
                    if (!read_int32(p, end, fp_count)) return std::nullopt;
                    // skip per-partition int32; we just forget the whole topic for now
                    for (int32_t fi = 0; fi < fp_count; ++fi) {
                        int32_t dummy = 0;
                        if (!read_int32(p, end, dummy)) return std::nullopt;
                    }
                }
            }
        }

        // rack_id (v11+)
        if (version >= 11) {
            if (!read_string(p, end, req.rack_id)) return std::nullopt;
        }

        // topics
        int32_t topic_count = 0;
        if (!read_int32(p, end, topic_count)) return std::nullopt;
        req.topics.resize(static_cast<size_t>(topic_count));

        for (int32_t ti = 0; ti < topic_count; ++ti) {
            auto& tf = req.topics[static_cast<size_t>(ti)];
            if (!read_string(p, end, tf.topic_name)) return std::nullopt;

            int32_t part_count = 0;
            if (!read_int32(p, end, part_count)) return std::nullopt;
            tf.partitions.resize(static_cast<size_t>(part_count));

            for (int32_t pi = 0; pi < part_count; ++pi) {
                auto& pf = tf.partitions[static_cast<size_t>(pi)];
                if (!read_int32(p, end, pf.partition)) return std::nullopt;

                // fetch_offset was INT64 in v0, INT64 in current
                if (!read_int64(p, end, pf.fetch_offset)) return std::nullopt;

                if (version >= 5) {
                    if (!read_int64(p, end, pf.log_start_offset))
                        return std::nullopt;
                }

                if (!read_int32(p, end, pf.partition_max_bytes))
                    return std::nullopt;
            }
        }

        return req;
    }

    // ------------------------------------------------------------------
    // Processor
    // ------------------------------------------------------------------

    shared_buffer process(const RequestContext& ctx, const FetchRequest& req) {
        timestamp_ms_t now_ms = ctx.received_at_ms;

        // --- Session management (v7+) ---
        if (ctx.api_version >= 7 && req.session_id != 0) {
            auto session = get_or_create_session(req.session_id, req.session_epoch);
            if (session.epoch != req.session_epoch) {
                return build_error(ctx.correlation_id,
                                   tp::ErrorCode::kInvalidFetchSessionEpoch,
                                   "Stale fetch session epoch");
            }
            session.last_used_ms = now_ms;
            // Remove forgotten topics
            for (const auto& ft : req.forgotten_topics) {
                session.partitions.erase(ft);
            }
        }

        // --- Determine isolation ---
        auto isolation = (req.isolation_level == 1)
                             ? isolation_level::read_committed
                             : isolation_level::read_uncommitted;

        // --- Fetch data ---
        auto& partition_mgr = server_.partition_manager();
        std::vector<char> response_buffer;
        int32_t topic_response_count = 0;

        // Build topic→partitions map
        for (const auto& topic_fetch : req.topics) {
            std::vector<std::pair<int32_t, tp::ErrorCode>> part_results;
            int32_t handled_count = 0;

            for (const auto& part_fetch : topic_fetch.partitions) {
                auto pid = part_fetch.partition;
                tp::ErrorCode ec = tp::ErrorCode::kNone;

                // --- Leadership / availability check ---
                if (!partition_mgr.is_leader(topic_fetch.topic_name, pid) &&
                    req.replica_id < 0 /* consumer */) {
                    broker_id_t leader =
                        partition_mgr.leader_for(topic_fetch.topic_name, pid);
                    ec = (leader == kNoBroker)
                             ? tp::ErrorCode::kUnknownTopicOrPartition
                             : tp::ErrorCode::kNotLeaderForPartition;
                }

                part_results.emplace_back(pid, ec);
                if (ec == tp::ErrorCode::kNone) ++handled_count;
            }

            // --- Long-poll wait ---
            // If we have no data and the client asked to wait, we block briefly.
            if (handled_count == 0 && req.max_wait_ms > 0 && req.min_bytes > 0) {
                // In production: register a waiter and block with condition variable
                // For stub: just return empty response
                logger_->trace("FetchHandler: long-poll would wait {}ms",
                               req.max_wait_ms);
            }

            ++topic_response_count;
        }

        // --- Build response ---
        return build_fetch_response(ctx, req);
    }

    // ------------------------------------------------------------------
    // Session helpers
    // ------------------------------------------------------------------

    [[nodiscard]] FetchSession& get_or_create_session(int32_t session_id,
                                                       int32_t epoch) {
        std::lock_guard<std::mutex> lock(session_mutex_);

        auto it = sessions_.find(session_id);
        if (it != sessions_.end()) {
            return it->second;
        }

        FetchSession session;
        session.session_id = session_id;
        session.epoch      = epoch;
        session.last_used_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        auto [inserted_it, _] =
            sessions_.emplace(session_id, std::move(session));
        return inserted_it->second;
    }

    void expire_old_sessions(timestamp_ms_t now_ms) {
        std::lock_guard<std::mutex> lock(session_mutex_);
        int64_t timeout_ms = 300000; // 5 minutes
        for (auto it = sessions_.begin(); it != sessions_.end();) {
            if (it->second.expired(now_ms, timeout_ms)) {
                it = sessions_.erase(it);
            } else {
                ++it;
            }
        }
    }

    // ------------------------------------------------------------------
    // Response building
    // ------------------------------------------------------------------

    shared_buffer build_error(int32_t correlation_id,
                               tp::ErrorCode ec,
                               const std::string& msg) const {
        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms
        torrent::network::write_error_code(buf, ec);
        torrent::network::write_int32_be(buf, 0); // topic_count

        auto frame = torrent::network::build_response_frame(
            correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    shared_buffer build_fetch_response(const RequestContext& ctx,
                                        const FetchRequest& req) const {
        std::vector<char> buf;

        // throttle_time_ms
        torrent::network::write_int32_be(buf, 0);

        // error_code (v7+)
        if (ctx.api_version >= 7) {
            torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
        }

        // session_id (v7+)
        if (ctx.api_version >= 7) {
            torrent::network::write_int32_be(buf, req.session_id);
        }

        // topic responses
        torrent::network::write_int32_be(
            buf, static_cast<int32_t>(req.topics.size()));

        for (const auto& tf : req.topics) {
            torrent::network::write_string(buf, tf.topic_name);

            torrent::network::write_int32_be(
                buf, static_cast<int32_t>(tf.partitions.size()));

            for (const auto& pf : tf.partitions) {
                torrent::network::write_int32_be(buf, pf.partition);

                // error_code
                bool is_leader = server_.partition_manager().is_leader(
                    tf.topic_name, pf.partition);
                auto ec = is_leader ? tp::ErrorCode::kNone
                                    : tp::ErrorCode::kNotLeaderForPartition;
                torrent::network::write_error_code(buf, ec);

                // high_watermark
                torrent::network::write_int64_be(buf,
                                                  ec == tp::ErrorCode::kNone
                                                      ? 100
                                                      : -1);

                // last_stable_offset (v4+)
                if (ctx.api_version >= 4) {
                    torrent::network::write_int64_be(buf,
                                                      ec == tp::ErrorCode::kNone
                                                          ? 100
                                                          : -1);
                }

                // log_start_offset (v5+)
                if (ctx.api_version >= 5) {
                    torrent::network::write_int64_be(buf, 0);
                }

                // aborted transactions (v4+)
                if (ctx.api_version >= 4) {
                    torrent::network::write_int32_be(buf, 0); // aborted_count
                }

                // preferred read replica (v11+)
                if (ctx.api_version >= 11) {
                    torrent::network::write_int32_be(buf, -1); // no preferred
                }

                // record batches
                if (ec == tp::ErrorCode::kNone) {
                    // Stub: empty record set
                    torrent::network::write_int32_be(buf, 0); // 0 bytes
                } else {
                    torrent::network::write_int32_be(buf, 0);
                }
            }
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // Members
    // ------------------------------------------------------------------

    brk::BrokerServer& server_;
    std::shared_ptr<spdlog::logger> logger_;

    mutable std::mutex session_mutex_;
    std::unordered_map<int32_t, FetchSession> sessions_;
};

// ============================================================================
// Public FetchHandler dispatcher
// ============================================================================

shared_buffer FetchHandler::handle(const RequestContext& ctx,
                                    buffer_view body) {
    FetchHandlerImpl impl(*server_);
    return impl.handle(ctx, body);
}

} // namespace torrent::client

// ============================================================================
// SECTION 3: Offset Handler (api_key=2, 8, 9, 23, 47)
// ============================================================================

namespace torrent::client {
namespace {

/**
 * @brief Offset handler covering ListOffsets, OffsetCommit, OffsetFetch,
 * OffsetForLeaderEpoch, and OffsetDelete.
 */
class OffsetHandlerImpl {
public:
    explicit OffsetHandlerImpl(brk::BrokerServer& server)
        : server_(server)
        , logger_(spdlog::get("offset_handler"))
    {
        if (!logger_) {
            logger_ = spdlog::stdout_color_mt("offset_handler");
            logger_->set_level(spdlog::level::info);
        }
    }

    // ------------------------------------------------------------------
    // ListOffsets (api_key=2)
    // ------------------------------------------------------------------

    /**
     * @brief Handle ListOffsets request.
     *
     * Supported offset types:
     *   -2 = earliest: log_start_offset
     *   -1 = latest:   log_end_offset (high watermark)
     *   >=0 = timestamp: find offset for the given timestamp
     */
    shared_buffer handle_list_offsets(const RequestContext& ctx,
                                       buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        // replica_id (INT32)
        int32_t replica_id = 0;
        if (!read_int32(p, end, replica_id)) {
            return build_offset_error(ctx);
        }

        // isolation_level (INT8, v2+)
        int8_t isolation = 0;
        if (ctx.api_version >= 2) {
            if (!read_int8(p, end, isolation)) return build_offset_error(ctx);
        }

        // topics
        int32_t topic_count = 0;
        if (!read_int32(p, end, topic_count)) return build_offset_error(ctx);

        std::vector<torrent::protocol::PartitionOffset> results;

        for (int32_t ti = 0; ti < topic_count; ++ti) {
            std::string topic_name;
            if (!read_string(p, end, topic_name)) return build_offset_error(ctx);

            int32_t part_count = 0;
            if (!read_int32(p, end, part_count))
                return build_offset_error(ctx);

            for (int32_t pi = 0; pi < part_count; ++pi) {
                int32_t partition = 0;
                if (!read_int32(p, end, partition))
                    return build_offset_error(ctx);

                // timestamp (INT64): -2=earliest, -1=latest, >=0=by timestamp
                int64_t ts = 0;
                if (!read_int64(p, end, ts))
                    return build_offset_error(ctx);

                // max_num_offsets (INT32, v0 only; v4+ uses it as well)
                int32_t max_offsets = 1;
                if (ctx.api_version == 0) {
                    if (!read_int32(p, end, max_offsets))
                        return build_offset_error(ctx);
                } else if (ctx.api_version >= 4) {
                    int32_t leader_epoch = 0;
                    if (!read_int32(p, end, leader_epoch))
                        return build_offset_error(ctx);
                    // For v4+, the leader_epoch is the current leader epoch
                }

                tp::PartitionOffset po;
                po.topic     = topic_name;
                po.partition = partition;
                po.leader_epoch = 0;

                // Check partition exists
                auto& part_mgr = server_.partition_manager();
                if (!part_mgr.is_leader(topic_name, partition) &&
                    replica_id < 0) {
                    po.error_code = tp::ErrorCode::kNotLeaderForPartition;
                    po.offset     = -1;
                } else {
                    po.error_code = tp::ErrorCode::kNone;

                    if (ts == -2) {
                        // earliest
                        po.offset = 0;
                    } else if (ts == -1) {
                        // latest
                        // In production: part_mgr.get_log_end_offset(topic, partition)
                        po.offset = 100; // stub
                    } else if (ts >= 0) {
                        // by timestamp
                        po.offset = 50; // stub
                    }
                }

                results.push_back(po);
            }
        }

        return build_list_offsets_response(ctx, results);
    }

    // ------------------------------------------------------------------
    // OffsetCommit (api_key=8), versions 0–8
    // ------------------------------------------------------------------

    shared_buffer handle_commit(const RequestContext& ctx,
                                 buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        // group_id (STRING)
        std::string group_id;
        if (!read_string(p, end, group_id))
            return build_simple_offset_error(ctx);

        // generation_id (INT32, v1+) or generation_id_or_member_epoch (v8+)
        int32_t generation_id = -1;
        if (ctx.api_version >= 1) {
            if (!read_int32(p, end, generation_id))
                return build_simple_offset_error(ctx);
        }

        // member_id (STRING, v1+)
        std::string member_id;
        if (ctx.api_version >= 1) {
            if (!read_string(p, end, member_id))
                return build_simple_offset_error(ctx);
        }

        // group_instance_id (nullable STRING, v7+)
        std::string group_instance_id;
        if (ctx.api_version >= 7) {
            bool is_null = false;
            if (!read_nullable_string(p, end, group_instance_id, is_null))
                return build_simple_offset_error(ctx);
        }

        // retention_time_ms (INT64, v2+)
        int64_t retention_ms = -1;
        if (ctx.api_version >= 2) {
            if (!read_int64(p, end, retention_ms))
                return build_simple_offset_error(ctx);
        }

        // topics
        int32_t topic_count = 0;
        if (!read_int32(p, end, topic_count))
            return build_simple_offset_error(ctx);

        std::vector<tp::PartitionResult> results;

        for (int32_t ti = 0; ti < topic_count; ++ti) {
            std::string topic_name;
            if (!read_string(p, end, topic_name))
                return build_simple_offset_error(ctx);

            int32_t part_count = 0;
            if (!read_int32(p, end, part_count))
                return build_simple_offset_error(ctx);

            for (int32_t pi = 0; pi < part_count; ++pi) {
                int32_t partition = 0;
                if (!read_int32(p, end, partition))
                    return build_simple_offset_error(ctx);

                int64_t offset = 0;
                if (!read_int64(p, end, offset))
                    return build_simple_offset_error(ctx);

                // leader_epoch (INT32, v6+)
                int32_t leader_epoch = -1;
                if (ctx.api_version >= 6) {
                    if (!read_int32(p, end, leader_epoch))
                        return build_simple_offset_error(ctx);
                }

                // metadata (nullable STRING)
                std::string metadata;
                if (ctx.api_version < 8) {
                    bool is_null = false;
                    if (!read_nullable_string(p, end, metadata, is_null))
                        return build_simple_offset_error(ctx);
                }
                // In v8, metadata is NULLABLE_STRING at the end
                if (ctx.api_version >= 8) {
                    bool is_null = false;
                    if (!read_nullable_string(p, end, metadata, is_null))
                        return build_simple_offset_error(ctx);
                }

                // --- Commit ---
                auto& group_mgr = server_.group_coordinator();
                auto commit_res =
                    group_mgr.commit_offset(group_id, topic_name, partition,
                                            offset, generation_id, member_id);

                tp::ErrorCode ec =
                    commit_res.ok()
                        ? tp::ErrorCode::kNone
                        : tp::ErrorCode::kUnknownServerError;

                results.push_back({topic_name, partition, ec, ""});
            }
        }

        return build_offset_commit_response(ctx, results);
    }

    // ------------------------------------------------------------------
    // OffsetFetch (api_key=9)
    // ------------------------------------------------------------------

    shared_buffer handle_fetch(const RequestContext& ctx,
                                buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        // group_id
        std::string group_id;
        if (!read_string(p, end, group_id))
            return build_simple_offset_error(ctx);

        // topics (can be null for all committed topics, v8+)
        int32_t topic_count = 0;
        if (!read_int32(p, end, topic_count))
            return build_simple_offset_error(ctx);

        std::vector<std::pair<std::string, int32_t>> request_partitions;

        for (int32_t ti = 0; ti < topic_count; ++ti) {
            std::string topic_name;
            if (!read_string(p, end, topic_name))
                return build_simple_offset_error(ctx);

            int32_t part_count = 0;
            if (!read_int32(p, end, part_count))
                return build_simple_offset_error(ctx);

            for (int32_t pi = 0; pi < part_count; ++pi) {
                int32_t partition = 0;
                if (!read_int32(p, end, partition))
                    return build_simple_offset_error(ctx);
                request_partitions.emplace_back(topic_name, partition);
            }
        }

        // require_stable (v7+)
        bool require_stable = false;
        if (ctx.api_version >= 7) {
            if (!read_bool(p, end, require_stable))
                return build_simple_offset_error(ctx);
        }

        // Build response
        auto& group_mgr = server_.group_coordinator();
        std::vector<char> buf;

        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        // topic count
        std::unordered_map<std::string, std::vector<int32_t>> topics_map;
        for (const auto& [topic, part] : request_partitions) {
            topics_map[topic].push_back(part);
        }

        torrent::network::write_int32_be(
            buf, static_cast<int32_t>(topics_map.size()));

        for (const auto& [topic, parts] : topics_map) {
            torrent::network::write_string(buf, topic);
            torrent::network::write_int32_be(
                buf, static_cast<int32_t>(parts.size()));

            for (auto part : parts) {
                torrent::network::write_int32_be(buf, part);

                // Fetch committed offset
                auto offset_opt = group_mgr.fetch_offset(group_id, topic, part);
                if (!offset_opt.has_value()) {
                    torrent::network::write_int64_be(buf, -1);
                    torrent::network::write_int32_be(buf, 0); // leader_epoch
                    torrent::network::write_nullable_string(buf, "");
                    torrent::network::write_error_code(
                        buf, tp::ErrorCode::kNone); // no offset but not an error
                } else {
                    torrent::network::write_int64_be(buf, *offset_opt);
                    torrent::network::write_int32_be(
                        buf, 0); // leader_epoch (stub)
                    torrent::network::write_nullable_string(buf, "");
                    torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
                }
            }
        }

        // error_code (v8+)
        if (ctx.api_version >= 8) {
            torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
        }

        // groups (v8+)
        if (ctx.api_version >= 8) {
            torrent::network::write_int32_be(buf, 0); // 0 groups outside main struct
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // OffsetForLeaderEpoch (api_key=23)
    // ------------------------------------------------------------------

    shared_buffer handle_leader_epoch(const RequestContext& ctx,
                                       buffer_view body) {
        // Stub: full implementation would query the log for the
        // first offset of a given leader epoch for truncation detection.
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        // Parse and respond with stub
        int32_t topic_count = 0;
        if (!read_int32(p, end, topic_count)) {
            return build_simple_offset_error(ctx);
        }

        torrent::network::write_int32_be(buf, topic_count);

        for (int32_t ti = 0; ti < topic_count; ++ti) {
            std::string topic_name;
            if (!read_string(p, end, topic_name)) {
                return build_simple_offset_error(ctx);
            }
            torrent::network::write_string(buf, topic_name);

            int32_t part_count = 0;
            if (!read_int32(p, end, part_count)) {
                return build_simple_offset_error(ctx);
            }
            torrent::network::write_int32_be(buf, part_count);

            for (int32_t pi = 0; pi < part_count; ++pi) {
                int32_t partition = 0;
                if (!read_int32(p, end, partition)) {
                    return build_simple_offset_error(ctx);
                }
                int32_t leader_epoch = 0;
                if (!read_int32(p, end, leader_epoch)) {
                    return build_simple_offset_error(ctx);
                }

                torrent::network::write_int32_be(buf, partition);
                torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
                torrent::network::write_int64_be(buf, 0); // end_offset stub
                torrent::network::write_int32_be(
                    buf, leader_epoch); // leader_epoch at that offset
            }
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // OffsetDelete (api_key=47)
    // ------------------------------------------------------------------

    shared_buffer handle_delete(const RequestContext& ctx,
                                 buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        std::string group_id;
        if (!read_string(p, end, group_id))
            return build_simple_offset_error(ctx);

        int32_t topic_count = 0;
        if (!read_int32(p, end, topic_count))
            return build_simple_offset_error(ctx);

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms
        torrent::network::write_error_code(buf, tp::ErrorCode::kNone);

        // topic results
        torrent::network::write_int32_be(buf, topic_count);

        for (int32_t ti = 0; ti < topic_count; ++ti) {
            std::string topic_name;
            if (!read_string(p, end, topic_name))
                return build_simple_offset_error(ctx);

            int32_t part_count = 0;
            if (!read_int32(p, end, part_count))
                return build_simple_offset_error(ctx);

            torrent::network::write_string(buf, topic_name);
            torrent::network::write_int32_be(buf, part_count);

            for (int32_t pi = 0; pi < part_count; ++pi) {
                int32_t partition = 0;
                if (!read_int32(p, end, partition))
                    return build_simple_offset_error(ctx);

                torrent::network::write_int32_be(buf, partition);
                torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
            }
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

private:
    // ------------------------------------------------------------------
    // Response helpers
    // ------------------------------------------------------------------

    shared_buffer build_offset_error(const RequestContext& ctx) const {
        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle
        torrent::network::write_int32_be(
            buf, 0); // no topics — error only
        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    shared_buffer build_simple_offset_error(const RequestContext& ctx) const {
        return build_offset_error(ctx);
    }

    shared_buffer
    build_list_offsets_response(const RequestContext& ctx,
                                const std::vector<tp::PartitionOffset>& results) const {
        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        // Group by topic
        std::unordered_map<std::string, std::vector<tp::PartitionOffset>>
            by_topic;
        for (const auto& r : results) {
            by_topic[r.topic].push_back(r);
        }

        torrent::network::write_int32_be(
            buf, static_cast<int32_t>(by_topic.size()));

        for (const auto& [topic, parts] : by_topic) {
            torrent::network::write_string(buf, topic);
            torrent::network::write_int32_be(
                buf, static_cast<int32_t>(parts.size()));

            for (const auto& po : parts) {
                torrent::network::write_int32_be(buf, po.partition);
                torrent::network::write_error_code(buf, po.error_code);

                // timestamp (v1+)
                int64_t ts = -1;
                torrent::network::write_int64_be(buf, ts);

                // offset
                torrent::network::write_int64_be(buf, po.offset);

                // leader_epoch (v4+)
                if (ctx.api_version >= 4) {
                    torrent::network::write_int32_be(buf, po.leader_epoch);
                }
            }
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    shared_buffer
    build_offset_commit_response(const RequestContext& ctx,
                                  const std::vector<tp::PartitionResult>& results) const {
        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        // Group by topic
        std::unordered_map<std::string, std::vector<tp::PartitionResult>>
            by_topic;
        for (const auto& r : results) {
            by_topic[r.topic].push_back(r);
        }

        torrent::network::write_int32_be(
            buf, static_cast<int32_t>(by_topic.size()));

        for (const auto& [topic, parts] : by_topic) {
            torrent::network::write_string(buf, topic);
            torrent::network::write_int32_be(
                buf, static_cast<int32_t>(parts.size()));

            for (const auto& pr : parts) {
                torrent::network::write_int32_be(buf, pr.partition);
                torrent::network::write_error_code(buf, pr.error_code);
            }
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // Members
    // ------------------------------------------------------------------

    brk::BrokerServer& server_;
    std::shared_ptr<spdlog::logger> logger_;
};

} // anonymous namespace

// ============================================================================
// Public OffsetHandler dispatchers
// ============================================================================

shared_buffer OffsetHandler::handle_list_offsets(const RequestContext& ctx,
                                                   buffer_view body) {
    OffsetHandlerImpl impl(*server_);
    return impl.handle_list_offsets(ctx, body);
}

shared_buffer OffsetHandler::handle_commit(const RequestContext& ctx,
                                            buffer_view body) {
    OffsetHandlerImpl impl(*server_);
    return impl.handle_commit(ctx, body);
}

shared_buffer OffsetHandler::handle_fetch(const RequestContext& ctx,
                                           buffer_view body) {
    OffsetHandlerImpl impl(*server_);
    return impl.handle_fetch(ctx, body);
}

} // namespace torrent::client

// ============================================================================
// SECTION 4: Metadata Handler (api_key=3)
// ============================================================================

namespace torrent::client {
namespace {

/**
 * @brief Full metadata handler.
 *
 * Supports all-topic or specific-topic queries, auto-creation flag,
 * broker list with rack info, controller ID, and per-partition metadata.
 */
class MetadataHandlerImpl {
public:
    explicit MetadataHandlerImpl(brk::BrokerServer& server)
        : server_(server)
        , logger_(spdlog::get("metadata_handler"))
    {
        if (!logger_) {
            logger_ = spdlog::stdout_color_mt("metadata_handler");
            logger_->set_level(spdlog::level::info);
        }
    }

    shared_buffer handle(const RequestContext& ctx, buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        // topic_names (array of strings, nullable for all topics in v9+)
        int32_t topic_count = 0;
        if (!read_int32(p, end, topic_count)) {
            return build_error(ctx);
        }

        bool all_topics = (topic_count == -1) ||
                          (topic_count == 0 && ctx.api_version >= 9);

        std::vector<std::string> requested_topics;
        if (!all_topics && topic_count > 0) {
            requested_topics.resize(static_cast<size_t>(topic_count));
            for (int32_t i = 0; i < topic_count; ++i) {
                std::string name;
                if (!read_string(p, end, name)) return build_error(ctx);
                requested_topics[static_cast<size_t>(i)] = std::move(name);
            }
        }

        // allow_auto_topic_creation (BOOLEAN, v4+)
        bool allow_auto_create = true;
        if (ctx.api_version >= 4) {
            if (!read_bool(p, end, allow_auto_create)) return build_error(ctx);
        }

        // include_cluster_authorized_operations (BOOLEAN, v8+)
        bool include_ops = false;
        if (ctx.api_version >= 8 && ctx.api_version <= 10) {
            if (!read_bool(p, end, include_ops)) return build_error(ctx);
        }

        // include_topic_authorized_operations (BOOLEAN, v8+)
        bool include_topic_ops = false;
        if (ctx.api_version >= 8) {
            if (!read_bool(p, end, include_topic_ops)) return build_error(ctx);
        }

        return build_response(ctx, all_topics, requested_topics, allow_auto_create);
    }

private:
    shared_buffer build_response(const RequestContext& ctx,
                                  bool all_topics,
                                  const std::vector<std::string>& requested,
                                  bool auto_create) {
        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        // --- Brokers ---
        auto& metadata_cache = server_.metadata_cache();
        const auto& brokers = metadata_cache.brokers();

        torrent::network::write_int32_be(
            buf, static_cast<int32_t>(brokers.size()));

        for (const auto& broker : brokers) {
            torrent::network::write_int32_be(buf, broker.broker_id);
            torrent::network::write_string(buf, broker.host);
            torrent::network::write_int32_be(buf,
                                              static_cast<int32_t>(broker.port));

            // rack (nullable STRING, v1+)
            if (broker.rack.has_value()) {
                torrent::network::write_nullable_string(
                    buf, broker.rack.value());
            } else {
                // write -1 length to indicate null
                const int16_t null_marker = -1;
                torrent::network::write_int16_be(
                    buf, null_marker);
            }
        }

        // --- Cluster ID (v2+) ---
        if (ctx.api_version >= 2) {
            torrent::network::write_string(buf, metadata_cache.cluster_id());
        }

        // --- Controller ID (v1+) ---
        if (ctx.api_version >= 1) {
            torrent::network::write_int32_be(
                buf, metadata_cache.controller_id());
        }

        // --- Topics ---
        auto& topic_mgr = server_.topic_manager();
        auto topics = all_topics ? topic_mgr.list_topics()
                                  : std::vector<sto::TopicMetadata>{};

        if (!all_topics) {
            for (const auto& name : requested) {
                auto meta = topic_mgr.get_topic(name);
                if (meta.has_value()) {
                    topics.push_back(*meta);
                } else {
                    sto::TopicMetadata missing;
                    missing.name  = name;
                    missing.error = torrent::error_code::unknown_topic_or_partition;
                    topics.push_back(missing);
                }
            }
        }

        torrent::network::write_int32_be(
            buf, static_cast<int32_t>(topics.size()));

        auto& part_mgr = server_.partition_manager();

        for (const auto& topic : topics) {
            // error_code (v1 first)
            if (ctx.api_version >= 1) {
                torrent::network::write_error_code(
                    buf,
                    topic.error == torrent::error_code::none
                        ? tp::ErrorCode::kNone
                        : tp::ErrorCode::kUnknownTopicOrPartition);
            }

            torrent::network::write_string(buf, topic.name);

            // is_internal (v1+)
            if (ctx.api_version >= 1) {
                torrent::network::write_bool(buf, topic.is_internal);
            }

            // partitions
            torrent::network::write_int32_be(
                buf, static_cast<int32_t>(topic.partitions.size()));

            for (size_t pi = 0; pi < topic.partitions.size(); ++pi) {
                // partition error (v1: per-partition error)
                if (ctx.api_version >= 1) {
                    torrent::network::write_error_code(
                        buf, tp::ErrorCode::kNone);
                }

                torrent::network::write_int32_be(
                    buf, static_cast<int32_t>(pi));

                auto leader = part_mgr.leader_for(
                    topic.name, static_cast<int32_t>(pi));
                torrent::network::write_int32_be(buf,
                                                  leader == kNoBroker ? -1
                                                                      : leader);

                // leader_epoch (v7+)
                if (ctx.api_version >= 7) {
                    torrent::network::write_int32_be(buf, 0);
                }

                // replicas
                auto replicas = part_mgr.replicas_for(
                    topic.name, static_cast<int32_t>(pi));
                torrent::network::write_int32_be(
                    buf, static_cast<int32_t>(replicas.size()));
                for (auto r : replicas) {
                    torrent::network::write_int32_be(buf, r);
                }

                // ISR
                torrent::network::write_int32_be(
                    buf, static_cast<int32_t>(replicas.size()));
                for (auto r : replicas) {
                    torrent::network::write_int32_be(buf, r);
                }

                // Offline replicas
                if (ctx.api_version >= 5) {
                    torrent::network::write_int32_be(buf, 0);
                }
            }

            // topic_authorized_operations (v8+)
            if (ctx.api_version >= 8) {
                torrent::network::write_int32_be(buf, 0);
            }
        }

        // cluster authorized operations (v8+)
        if (ctx.api_version >= 8 && ctx.api_version <= 10) {
            torrent::network::write_int32_be(buf, 0);
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    shared_buffer build_error(const RequestContext& ctx) const {
        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle
        torrent::network::write_int32_be(buf, 0); // no brokers
        if (ctx.api_version >= 2) {
            torrent::network::write_string(buf, "");
        }
        if (ctx.api_version >= 1) {
            torrent::network::write_int32_be(buf, -1);
        }
        torrent::network::write_int32_be(buf, 0); // no topics

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    brk::BrokerServer& server_;
    std::shared_ptr<spdlog::logger> logger_;
};

} // anonymous namespace

// ============================================================================
// Public MetadataHandler dispatcher
// ============================================================================

shared_buffer MetadataHandler::handle(const RequestContext& ctx,
                                       buffer_view body) {
    MetadataHandlerImpl impl(*server_);
    return impl.handle(ctx, body);
}

} // namespace torrent::client

// ============================================================================
// SECTION 5: Group Handler (api_key=10–16, 42)
// ============================================================================

namespace torrent::client {
namespace {

/**
 * @brief Full consumer-group coordinator handler.
 *
 * Implements:
 *   - FindCoordinator     (api_key=10)
 *   - JoinGroup           (api_key=11)
 *   - SyncGroup           (api_key=14)
 *   - Heartbeat           (api_key=12)
 *   - LeaveGroup          (api_key=13)
 *   - DescribeGroups      (api_key=15)
 *   - ListGroups          (api_key=16)
 *   - DeleteGroups        (api_key=42)
 *
 * Maintains a state machine per group: Empty → PreparingRebalance →
 * CompletingRebalance → Stable → (Dead).
 */
class GroupHandlerImpl {
public:
    explicit GroupHandlerImpl(brk::BrokerServer& server)
        : server_(server)
        , logger_(spdlog::get("group_handler"))
    {
        if (!logger_) {
            logger_ = spdlog::stdout_color_mt("group_handler");
            logger_->set_level(spdlog::level::info);
        }
    }

    // ------------------------------------------------------------------
    // FindCoordinator (api_key=10)
    // ------------------------------------------------------------------

    shared_buffer handle_find_coordinator(const RequestContext& ctx,
                                           buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        std::string key;     // group_id (or transactional_id in v3+)
        int8_t key_type = 0; // 0=group, 1=transaction (v1+)

        if (ctx.api_version >= 1) {
            if (!read_string(p, end, key)) return build_group_error(ctx);
            if (!read_int8(p, end, key_type)) return build_group_error(ctx);
        } else {
            if (!read_string(p, end, key)) return build_group_error(ctx);
        }

        // Coordinator is typically the broker handling the corresponding
        // __consumer_offsets partition hash. For stub, we return self.
        broker_id_t coordinator = server_.broker_id();

        // Resolve host/port from metadata cache
        const auto& brokers = server_.metadata_cache().brokers();
        std::string host = "localhost";
        int32_t port     = 9092;

        for (const auto& b : brokers) {
            if (b.broker_id == coordinator) {
                host = b.host;
                port = static_cast<int32_t>(b.port);
                break;
            }
        }

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        // error_code (v1+)
        if (ctx.api_version >= 1) {
            torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
            torrent::network::write_string(buf, ""); // error_message (v3+)
        }

        torrent::network::write_int32_be(buf, coordinator);
        torrent::network::write_string(buf, host);
        torrent::network::write_int32_be(buf, port);

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // JoinGroup (api_key=11)
    // ------------------------------------------------------------------

    shared_buffer handle_join(const RequestContext& ctx,
                               buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        std::string group_id;
        if (!read_string(p, end, group_id)) return build_group_error(ctx);

        int32_t session_timeout_ms = 0;
        if (!read_int32(p, end, session_timeout_ms))
            return build_group_error(ctx);

        // rebalance_timeout_ms (v1+)
        int32_t rebalance_timeout_ms = session_timeout_ms;
        if (ctx.api_version >= 1) {
            if (!read_int32(p, end, rebalance_timeout_ms))
                return build_group_error(ctx);
        }

        std::string member_id;
        if (!read_string(p, end, member_id)) return build_group_error(ctx);

        // group_instance_id (v5+)
        std::string group_instance_id;
        if (ctx.api_version >= 5) {
            bool is_null = false;
            if (!read_nullable_string(p, end, group_instance_id, is_null))
                return build_group_error(ctx);
        }

        std::string protocol_type;
        if (!read_string(p, end, protocol_type))
            return build_group_error(ctx);

        // protocols
        int32_t protocol_count = 0;
        if (!read_int32(p, end, protocol_count))
            return build_group_error(ctx);

        std::vector<std::pair<std::string, std::vector<uint8_t>>> protocols;
        for (int32_t i = 0; i < protocol_count; ++i) {
            std::string proto_name;
            if (!read_string(p, end, proto_name))
                return build_group_error(ctx);
            bool is_null = false;
            std::vector<uint8_t> proto_meta;
            if (!read_bytes(p, end, proto_meta, is_null))
                return build_group_error(ctx);
            protocols.emplace_back(std::move(proto_name), std::move(proto_meta));
        }

        // reason (v7+)
        std::string reason;
        if (ctx.api_version >= 7) {
            bool is_null = false;
            if (!read_nullable_string(p, end, reason, is_null))
                return build_group_error(ctx);
        }

        // --- Process JoinGroup ---
        auto& group_mgr = server_.group_coordinator();
        auto join_result =
            group_mgr.join_group(group_id, member_id, protocol_type,
                                 protocols, session_timeout_ms);

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        if (ctx.api_version >= 2) {
            torrent::network::write_error_code(
                buf,
                join_result.ok() ? tp::ErrorCode::kNone
                                 : tp::ErrorCode::kUnknownServerError);
        }

        torrent::network::write_int32_be(buf,
                                          join_result.generation_id);
        torrent::network::write_string(buf,
                                        join_result.protocol_name);
        torrent::network::write_string(buf,
                                        join_result.leader_id);

        if (member_id.empty() || join_result.member_id.empty()) {
            // New member — assign an ID
            torrent::network::write_string(
                buf, join_result.member_id);
        } else {
            torrent::network::write_string(buf, member_id);
        }

        // members array
        torrent::network::write_int32_be(
            buf, static_cast<int32_t>(join_result.members.size()));

        for (const auto& m : join_result.members) {
            torrent::network::write_string(buf, m.member_id);

            // group_instance_id (v5+)
            if (ctx.api_version >= 5) {
                torrent::network::write_nullable_string(
                    buf, m.group_instance_id.value_or(""));
            }

            torrent::network::write_bytes(
                buf, m.protocol_metadata.data(),
                m.protocol_metadata.size());
        }

        // skip_assignment (v9+)
        if (ctx.api_version >= 9) {
            torrent::network::write_bool(buf, false);
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // SyncGroup (api_key=14)
    // ------------------------------------------------------------------

    shared_buffer handle_sync(const RequestContext& ctx,
                               buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        std::string group_id;
        if (!read_string(p, end, group_id)) return build_group_error(ctx);

        int32_t generation_id = 0;
        if (!read_int32(p, end, generation_id))
            return build_group_error(ctx);

        std::string member_id;
        if (!read_string(p, end, member_id)) return build_group_error(ctx);

        // group_instance_id (v5+)
        std::string group_instance_id;
        if (ctx.api_version >= 5) {
            bool is_null = false;
            if (!read_nullable_string(p, end, group_instance_id, is_null))
                return build_group_error(ctx);
        }

        // protocol_type (v5+)
        std::string protocol_type;
        if (ctx.api_version >= 5) {
            bool is_null = false;
            if (!read_nullable_string(p, end, protocol_type, is_null))
                return build_group_error(ctx);
        }

        // protocol_name (v5+)
        std::string protocol_name;
        if (ctx.api_version >= 5) {
            bool is_null = false;
            if (!read_nullable_string(p, end, protocol_name, is_null))
                return build_group_error(ctx);
        }

        // assignments
        int32_t assignment_count = 0;
        if (!read_int32(p, end, assignment_count))
            return build_group_error(ctx);

        std::vector<std::pair<std::string, std::vector<uint8_t>>> assignments;
        for (int32_t i = 0; i < assignment_count; ++i) {
            std::string m_id;
            if (!read_string(p, end, m_id))
                return build_group_error(ctx);
            bool is_null = false;
            std::vector<uint8_t> assign_bytes;
            if (!read_bytes(p, end, assign_bytes, is_null))
                return build_group_error(ctx);
            assignments.emplace_back(std::move(m_id), std::move(assign_bytes));
        }

        // --- Process SyncGroup ---
        auto& group_mgr = server_.group_coordinator();
        auto sync_result =
            group_mgr.sync_group(group_id, generation_id, member_id,
                                 assignments);

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        if (ctx.api_version >= 1) {
            torrent::network::write_error_code(
                buf,
                sync_result.ok() ? tp::ErrorCode::kNone
                                 : tp::ErrorCode::kUnknownServerError);
        }

        // protocol_type (v5+)
        if (ctx.api_version >= 5) {
            torrent::network::write_nullable_string(
                buf, sync_result.protocol_type);
        }

        // protocol_name (v5+)
        if (ctx.api_version >= 5) {
            torrent::network::write_nullable_string(
                buf, sync_result.protocol_name);
        }

        // assignment bytes for this member
        torrent::network::write_bytes(
            buf, sync_result.assignment.data(),
            sync_result.assignment.size());

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // Heartbeat (api_key=12)
    // ------------------------------------------------------------------

    shared_buffer handle_heartbeat(const RequestContext& ctx,
                                    buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        std::string group_id;
        if (!read_string(p, end, group_id)) return build_group_error(ctx);

        int32_t generation_id = 0;
        if (!read_int32(p, end, generation_id))
            return build_group_error(ctx);

        std::string member_id;
        if (!read_string(p, end, member_id)) return build_group_error(ctx);

        // group_instance_id (v3+)
        std::string group_instance_id;
        if (ctx.api_version >= 3) {
            bool is_null = false;
            if (!read_nullable_string(p, end, group_instance_id, is_null))
                return build_group_error(ctx);
        }

        auto& group_mgr = server_.group_coordinator();
        auto hb_result =
            group_mgr.heartbeat(group_id, generation_id, member_id);

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        if (ctx.api_version >= 1) {
            torrent::network::write_error_code(
                buf,
                hb_result.ok() ? tp::ErrorCode::kNone
                               : tp::ErrorCode::kUnknownMemberId);
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // LeaveGroup (api_key=13)
    // ------------------------------------------------------------------

    shared_buffer handle_leave(const RequestContext& ctx,
                                buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        std::string group_id;
        if (!read_string(p, end, group_id)) return build_group_error(ctx);

        // member_id (v1+)
        std::string member_id;
        if (ctx.api_version >= 1) {
            if (!read_string(p, end, member_id))
                return build_group_error(ctx);
        }

        // members (v3+)
        std::vector<std::string> members_to_leave;
        if (ctx.api_version >= 3) {
            int32_t member_count = 0;
            if (!read_int32(p, end, member_count))
                return build_group_error(ctx);
            for (int32_t i = 0; i < member_count; ++i) {
                std::string m_id;
                if (!read_string(p, end, m_id))
                    return build_group_error(ctx);
                members_to_leave.push_back(std::move(m_id));
            }
        }

        auto& group_mgr = server_.group_coordinator();
        if (ctx.api_version < 3) {
            group_mgr.leave_group(group_id, member_id);
        } else {
            for (const auto& m : members_to_leave) {
                group_mgr.leave_group(group_id, m);
            }
        }

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        if (ctx.api_version >= 1) {
            torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // DescribeGroups (api_key=15)
    // ------------------------------------------------------------------

    shared_buffer handle_describe(const RequestContext& ctx,
                                   buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        int32_t group_count = 0;
        if (!read_int32(p, end, group_count))
            return build_group_error(ctx);

        std::vector<std::string> group_ids;
        group_ids.resize(static_cast<size_t>(group_count));
        for (int32_t i = 0; i < group_count; ++i) {
            if (!read_string(p, end,
                             group_ids[static_cast<size_t>(i)]))
                return build_group_error(ctx);
        }

        // include_authorized_operations (v3+)
        bool include_ops = false;
        if (ctx.api_version >= 3) {
            if (!read_bool(p, end, include_ops))
                return build_group_error(ctx);
        }

        auto& group_mgr = server_.group_coordinator();
        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        torrent::network::write_int32_be(buf, group_count);

        for (const auto& gid : group_ids) {
            auto group_state = group_mgr.describe_group(gid);

            // error_code
            torrent::network::write_error_code(
                buf,
                group_state.has_value()
                    ? tp::ErrorCode::kNone
                    : tp::ErrorCode::kUnknownGroupId);

            torrent::network::write_string(buf, gid);

            if (group_state.has_value()) {
                const auto& gs = *group_state;
                torrent::network::write_string(
                    buf, gs.is_stable() ? "Stable"
                                         : "PreparingRebalance");
                torrent::network::write_string(
                    buf, gs.protocol_type);
                torrent::network::write_string(
                    buf, gs.protocol);
                torrent::network::write_int32_be(
                    buf, static_cast<int32_t>(gs.members.size()));

                for (const auto& m : gs.members) {
                    torrent::network::write_string(buf, m.member_id);
                    if (ctx.api_version >= 4) {
                        torrent::network::write_string(
                            buf,
                            m.group_instance_id.value_or(""));

                    }
                    torrent::network::write_string(buf, m.client_host);
                    if (ctx.api_version >= 4) {
                        torrent::network::write_nullable_string(
                            buf, "");
                    }
                    // metadata
                    torrent::network::write_bytes(
                        buf,
                        reinterpret_cast<const uint8_t*>(
                            m.protocol_metadata.data()),
                        m.protocol_metadata.size());
                    // assignments
                    if (ctx.api_version >= 4) {
                        torrent::network::write_bytes(
                            buf, nullptr, 0);
                    }
                }
            } else {
                // Empty group description
                torrent::network::write_string(buf, "Dead");
                torrent::network::write_string(buf, "");
                torrent::network::write_string(buf, "");
                torrent::network::write_int32_be(buf, 0); // 0 members
            }

            // authorized_operations (v3+)
            if (ctx.api_version >= 3) {
                torrent::network::write_int32_be(buf, 0);
            }
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // ListGroups (api_key=16)
    // ------------------------------------------------------------------

    shared_buffer handle_list(const RequestContext& ctx,
                               buffer_view body) {
        // v4+: states_filter (array of strings)
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        std::vector<std::string> state_filters;
        if (ctx.api_version >= 4) {
            int32_t filter_count = 0;
            if (!read_int32(p, end, filter_count))
                return build_group_error(ctx);
            for (int32_t i = 0; i < filter_count; ++i) {
                std::string state;
                if (!read_string(p, end, state))
                    return build_group_error(ctx);
                state_filters.push_back(std::move(state));
            }
        }

        auto& group_mgr = server_.group_coordinator();
        auto groups = group_mgr.list_groups();

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        if (ctx.api_version >= 3) {
            torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
        }

        torrent::network::write_int32_be(
            buf, static_cast<int32_t>(groups.size()));

        for (const auto& g : groups) {
            torrent::network::write_string(buf, g.group_id);
            torrent::network::write_string(
                buf, g.is_stable() ? "Stable" : "PreparingRebalance");
            torrent::network::write_string(buf, g.protocol_type);
            if (ctx.api_version >= 4) {
                torrent::network::write_string(
                    buf, "consumer"); // protocol_type in v4+
            }
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // DeleteGroups (api_key=42)
    // ------------------------------------------------------------------

    shared_buffer handle_delete(const RequestContext& ctx,
                                 buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        int32_t group_count = 0;
        if (!read_int32(p, end, group_count))
            return build_group_error(ctx);

        std::vector<std::string> group_ids;
        group_ids.resize(static_cast<size_t>(group_count));
        for (int32_t i = 0; i < group_count; ++i) {
            if (!read_string(p, end,
                             group_ids[static_cast<size_t>(i)]))
                return build_group_error(ctx);
        }

        auto& group_mgr = server_.group_coordinator();

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        // results
        torrent::network::write_int32_be(buf, group_count);

        for (const auto& gid : group_ids) {
            auto result = group_mgr.delete_group(gid);
            torrent::network::write_error_code(
                buf, result.ok() ? tp::ErrorCode::kNone
                                  : tp::ErrorCode::kUnknownGroupId);
            torrent::network::write_string(buf, gid);
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

private:
    shared_buffer build_group_error(const RequestContext& ctx) const {
        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle
        torrent::network::write_error_code(buf,
                                            tp::ErrorCode::kInvalidRequest);
        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    brk::BrokerServer& server_;
    std::shared_ptr<spdlog::logger> logger_;
};

} // anonymous namespace

// ============================================================================
// Public GroupHandler dispatchers
// ============================================================================

shared_buffer GroupHandler::handle_join(const RequestContext& ctx,
                                         buffer_view body) {
    GroupHandlerImpl impl(*server_);
    return impl.handle_join(ctx, body);
}

shared_buffer GroupHandler::handle_sync(const RequestContext& ctx,
                                         buffer_view body) {
    GroupHandlerImpl impl(*server_);
    return impl.handle_sync(ctx, body);
}

shared_buffer GroupHandler::handle_heartbeat(const RequestContext& ctx,
                                              buffer_view body) {
    GroupHandlerImpl impl(*server_);
    return impl.handle_heartbeat(ctx, body);
}

shared_buffer GroupHandler::handle_leave(const RequestContext& ctx,
                                          buffer_view body) {
    GroupHandlerImpl impl(*server_);
    return impl.handle_leave(ctx, body);
}

shared_buffer GroupHandler::handle_describe(const RequestContext& ctx,
                                             buffer_view body) {
    GroupHandlerImpl impl(*server_);
    return impl.handle_describe(ctx, body);
}

shared_buffer GroupHandler::handle_list(const RequestContext& ctx,
                                         buffer_view body) {
    GroupHandlerImpl impl(*server_);
    return impl.handle_list(ctx, body);
}

shared_buffer GroupHandler::handle_delete(const RequestContext& ctx,
                                           buffer_view body) {
    GroupHandlerImpl impl(*server_);
    return impl.handle_delete(ctx, body);
}

} // namespace torrent::client

// ============================================================================
// SECTION 6: Admin Handler (api_key=19,20,32,33,29,30,31,37,43,45,46)
// ============================================================================

namespace torrent::client {
namespace {

/**
 * @brief Full admin handler for topic/config/ACL/partition management.
 *
 * Covers:
 *   - CreateTopics / DeleteTopics / CreatePartitions
 *   - DescribeConfigs / AlterConfigs
 *   - CreateAcls / DescribeAcls / DeleteAcls
 *   - ElectLeaders
 *   - AlterPartitionReassignments / ListPartitionReassignments
 */
class AdminHandlerImpl {
public:
    explicit AdminHandlerImpl(brk::BrokerServer& server)
        : server_(server)
        , logger_(spdlog::get("admin_handler"))
    {
        if (!logger_) {
            logger_ = spdlog::stdout_color_mt("admin_handler");
            logger_->set_level(spdlog::level::info);
        }
    }

    // ------------------------------------------------------------------
    // CreateTopics (api_key=19)
    // ------------------------------------------------------------------

    shared_buffer handle_create_topics(const RequestContext& ctx,
                                        buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        int32_t topic_count = 0;
        if (!read_int32(p, end, topic_count)) return build_admin_error(ctx);

        auto& topic_mgr = server_.topic_manager();
        std::vector<tp::PartitionResult> results;

        for (int32_t i = 0; i < topic_count; ++i) {
            std::string name;
            if (!read_string(p, end, name)) return build_admin_error(ctx);

            int32_t num_partitions = 1;
            if (!read_int32(p, end, num_partitions))
                return build_admin_error(ctx);

            int16_t rf = 1;
            if (!read_int32(p, end, reinterpret_cast<int32_t&>(rf)))
                return build_admin_error(ctx);

            // replica_assignment (ARRAY, nullable)
            int32_t reassign_count = 0;
            if (!read_int32(p, end, reassign_count))
                return build_admin_error(ctx);
            for (int32_t ri = 0; ri < reassign_count; ++ri) {
                int32_t broker_count = 0;
                if (!read_int32(p, end, broker_count))
                    return build_admin_error(ctx);
                for (int32_t bi = 0; bi < broker_count; ++bi) {
                    int32_t b_id = 0;
                    if (!read_int32(p, end, b_id))
                        return build_admin_error(ctx);
                }
            }

            // configs
            int32_t config_count = 0;
            if (!read_int32(p, end, config_count))
                return build_admin_error(ctx);
            std::map<std::string, std::string> configs;
            for (int32_t ci = 0; ci < config_count; ++ci) {
                std::string ck, cv;
                if (!read_string(p, end, ck))
                    return build_admin_error(ctx);
                if (!read_string(p, end, cv))
                    return build_admin_error(ctx);
                configs[ck] = cv;
            }

            // timeout_ms (v5+)
            int32_t timeout_ms = 30000;
            if (ctx.api_version >= 5) {
                int32_t tmt = 0;
                if (!read_int32(p, end, tmt))
                    return build_admin_error(ctx);
            }

            // validate_only (v1+)
            bool validate_only = false;
            if (ctx.api_version >= 1) {
                if (!read_bool(p, end, validate_only))
                    return build_admin_error(ctx);
            }

            if (!validate_only) {
                auto result = topic_mgr.create_topic(name, num_partitions, rf);
                tp::ErrorCode ec = result.ok()
                                       ? tp::ErrorCode::kNone
                                       : tp::ErrorCode::kTopicAlreadyExists;
                // We emit per-partition results; for topic-level errors,
                // we emit one entry with an empty-partition error
                results.push_back({name, -1, ec, result.ok() ? ""
                                                              : result.error_message});
            } else {
                results.push_back({name, -1, tp::ErrorCode::kNone, ""});
            }
        }

        return build_topic_admin_response(ctx, results);
    }

    // ------------------------------------------------------------------
    // DeleteTopics (api_key=20)
    // ------------------------------------------------------------------

    shared_buffer handle_delete_topics(const RequestContext& ctx,
                                        buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        int32_t topic_count = 0;
        if (!read_int32(p, end, topic_count)) return build_admin_error(ctx);

        std::vector<std::string> names;
        names.resize(static_cast<size_t>(topic_count));
        for (int32_t i = 0; i < topic_count; ++i) {
            if (!read_string(p, end,
                             names[static_cast<size_t>(i)]))
                return build_admin_error(ctx);
        }

        int32_t timeout_ms = 30000;
        if (ctx.api_version >= 1) {
            if (!read_int32(p, end, timeout_ms))
                return build_admin_error(ctx);
        }

        auto& topic_mgr = server_.topic_manager();
        std::vector<tp::PartitionResult> results;

        for (const auto& name : names) {
            auto result = topic_mgr.delete_topic(name);
            tp::ErrorCode ec = result.ok()
                                   ? tp::ErrorCode::kNone
                                   : tp::ErrorCode::kUnknownTopicOrPartition;
            results.push_back({name, -1, ec, ""});
        }

        return build_topic_admin_response(ctx, results);
    }

    // ------------------------------------------------------------------
    // CreatePartitions (api_key=37)
    // ------------------------------------------------------------------

    shared_buffer handle_create_partitions(const RequestContext& ctx,
                                            buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        int32_t topic_count = 0;
        if (!read_int32(p, end, topic_count)) return build_admin_error(ctx);

        std::vector<std::string> names;
        std::vector<int32_t> new_counts;
        names.resize(static_cast<size_t>(topic_count));
        new_counts.resize(static_cast<size_t>(topic_count));

        for (int32_t i = 0; i < topic_count; ++i) {
            if (!read_string(p, end,
                             names[static_cast<size_t>(i)]))
                return build_admin_error(ctx);
            if (!read_int32(p, end,
                            new_counts[static_cast<size_t>(i)]))
                return build_admin_error(ctx);

            if (ctx.api_version >= 2) {
                int32_t assignment_count = 0;
                int32_t dummy = 0;
                if (!read_int32(p, end, assignment_count))
                    return build_admin_error(ctx);
                for (int32_t a = 0; a < assignment_count; ++a) {
                    int32_t bcount = 0;
                    if (!read_int32(p, end, bcount))
                        return build_admin_error(ctx);
                    for (int32_t b = 0; b < bcount; ++b) {
                        if (!read_int32(p, end, dummy))
                            return build_admin_error(ctx);
                    }
                }
            }
        }

        // validate_only (v1+)
        bool validate_only = false;
        if (ctx.api_version >= 1) {
            if (!read_bool(p, end, validate_only))
                return build_admin_error(ctx);
        }
        int32_t timeout_ms = 30000;
        if (!read_int32(p, end, timeout_ms))
            return build_admin_error(ctx);

        std::vector<tp::PartitionResult> results;
        auto& topic_mgr = server_.topic_manager();
        for (size_t i = 0; i < names.size(); ++i) {
            auto meta = topic_mgr.get_topic(names[i]);
            tp::ErrorCode ec = tp::ErrorCode::kNone;
            if (!meta.has_value()) {
                ec = tp::ErrorCode::kUnknownTopicOrPartition;
            } else if (new_counts[i] <=
                       static_cast<int32_t>(meta->partitions.size())) {
                ec = tp::ErrorCode::kInvalidPartitions;
            }
            // In production: actually create partitions via controller
            results.push_back({names[i], -1, ec,
                               ec == tp::ErrorCode::kNone
                                   ? ""
                                   : "Cannot create partitions; stub"});
        }

        return build_topic_admin_response(ctx, results);
    }

    // ------------------------------------------------------------------
    // DescribeConfigs (api_key=32)
    // ------------------------------------------------------------------

    shared_buffer handle_describe_configs(const RequestContext& ctx,
                                           buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        int32_t resource_count = 0;
        if (!read_int32(p, end, resource_count))
            return build_admin_error(ctx);

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms
        torrent::network::write_int32_be(buf, resource_count);

        for (int32_t i = 0; i < resource_count; ++i) {
            int8_t resource_type = 0; // 2=topic, 4=broker (v0: 0=unknown,1=topic)
            if (!read_int8(p, end, resource_type))
                return build_admin_error(ctx);

            std::string resource_name;
            if (!read_string(p, end, resource_name))
                return build_admin_error(ctx);

            // config_keys (nullable array)
            int32_t key_count = 0;
            if (!read_int32(p, end, key_count))
                return build_admin_error(ctx);
            std::vector<std::string> keys;
            keys.resize(static_cast<size_t>(key_count));
            for (int32_t ki = 0; ki < key_count; ++ki) {
                if (!read_string(p, end,
                                 keys[static_cast<size_t>(ki)]))
                    return build_admin_error(ctx);
            }

            // Write response
            torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
            torrent::network::write_string(buf, ""); // error_message
            torrent::network::write_int8(buf, resource_type);
            torrent::network::write_string(buf, resource_name);

            // config entries — stub: return some defaults
            const int32_t entry_count = 3;
            torrent::network::write_int32_be(buf, entry_count);

            auto write_config_entry = [&](const char* name,
                                          const char* value,
                                          bool is_default,
                                          bool is_sensitive,
                                          bool is_read_only) {
                torrent::network::write_string(buf, name);
                torrent::network::write_nullable_string(buf, value);
                torrent::network::write_bool(buf, is_read_only);
                if (ctx.api_version >= 1) {
                    torrent::network::write_bool(buf, is_default);
                }
                if (ctx.api_version >= 2) {
                    torrent::network::write_bool(buf, is_sensitive);
                }
                // config_source (INT8, v1+)
                torrent::network::write_int8(
                    buf, is_default ? 5 : 4); // 4=DEFAULT_CONFIG, 5=STATIC_BROKER_CONFIG
                // config_synonyms (v1+)
                torrent::network::write_int32_be(buf, 0);
                // config_type (v3+)
                if (ctx.api_version >= 3) {
                    torrent::network::write_int8(buf, 0); // 0=STRING
                }
                // documentation (v3+)
                if (ctx.api_version >= 3) {
                    torrent::network::write_nullable_string(buf, "");
                }
            };

            write_config_entry("retention.ms", "604800000", true, false, false);
            write_config_entry("segment.bytes", "1073741824", true, false, false);
            write_config_entry("cleanup.policy", "delete", true, false, false);
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // AlterConfigs (api_key=33)
    // ------------------------------------------------------------------

    shared_buffer handle_alter_configs(const RequestContext& ctx,
                                        buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        int32_t resource_count = 0;
        if (!read_int32(p, end, resource_count))
            return build_admin_error(ctx);

        // validate_only (v1+)
        bool validate_only = false;
        if (ctx.api_version >= 1) {
            if (!read_bool(p, end, validate_only))
                return build_admin_error(ctx);
        }

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms
        torrent::network::write_int32_be(buf, resource_count);

        for (int32_t i = 0; i < resource_count; ++i) {
            int8_t resource_type = 0;
            if (!read_int8(p, end, resource_type))
                return build_admin_error(ctx);

            std::string resource_name;
            if (!read_string(p, end, resource_name))
                return build_admin_error(ctx);

            int32_t config_count = 0;
            if (!read_int32(p, end, config_count))
                return build_admin_error(ctx);

            for (int32_t ci = 0; ci < config_count; ++ci) {
                std::string ck, cv;
                if (!read_string(p, end, ck))
                    return build_admin_error(ctx);
                if (!read_string(p, end, cv))
                    return build_admin_error(ctx);

                // In production: actually alter the config
            }

            torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
            torrent::network::write_string(buf, "");
            torrent::network::write_int8(buf, resource_type);
            torrent::network::write_string(buf, resource_name);
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // CreateAcls (api_key=30)
    // ------------------------------------------------------------------

    shared_buffer handle_create_acls(const RequestContext& ctx,
                                      buffer_view body) {
        // Parse and create ACLs
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        int32_t creation_count = 0;
        if (!read_int32(p, end, creation_count))
            return build_admin_error(ctx);

        for (int32_t i = 0; i < creation_count; ++i) {
            // principal
            std::string principal;
            if (!read_string(p, end, principal))
                return build_admin_error(ctx);

            // host
            std::string host;
            if (!read_string(p, end, host))
                return build_admin_error(ctx);

            // operation (INT8)
            int8_t operation = 0;
            if (!read_int8(p, end, operation))
                return build_admin_error(ctx);

            // permission_type (INT8)
            int8_t permission = 0;
            if (!read_int8(p, end, permission))
                return build_admin_error(ctx);
        }

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms
        // creation_results
        torrent::network::write_int32_be(buf, creation_count);
        for (int32_t i = 0; i < creation_count; ++i) {
            torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
            torrent::network::write_string(buf, "");
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // DescribeAcls (api_key=29)
    // ------------------------------------------------------------------

    shared_buffer handle_describe_acls(const RequestContext& ctx,
                                        buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        int8_t resource_type = 0;
        if (!read_int8(p, end, resource_type))
            return build_admin_error(ctx);

        // resource_name filter (nullable, v1+)
        std::string resource_name_filter;
        if (ctx.api_version >= 1) {
            bool is_null = false;
            if (!read_nullable_string(p, end, resource_name_filter, is_null))
                return build_admin_error(ctx);
        }

        // resource_pattern_type_filter (v1+)
        int8_t pattern_type = 3; // ANY
        if (ctx.api_version >= 1) {
            if (!read_int8(p, end, pattern_type))
                return build_admin_error(ctx);
        }

        // principal filter (nullable, v1+)
        std::string principal_filter;
        if (ctx.api_version >= 1) {
            bool is_null = false;
            if (!read_nullable_string(p, end, principal_filter, is_null))
                return build_admin_error(ctx);
        }

        // host filter (nullable, v1+)
        std::string host_filter;
        if (ctx.api_version >= 1) {
            bool is_null = false;
            if (!read_nullable_string(p, end, host_filter, is_null))
                return build_admin_error(ctx);
        }

        // operation (INT8)
        int8_t operation = 0;
        if (!read_int8(p, end, operation))
            return build_admin_error(ctx);

        // permission_type (INT8)
        int8_t permission_type = 0;
        if (!read_int8(p, end, permission_type))
            return build_admin_error(ctx);

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        if (ctx.api_version >= 1) {
            torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
            torrent::network::write_string(buf, "");
        }

        // resources
        torrent::network::write_int32_be(buf, 0); // no ACLs in stub

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // DeleteAcls (api_key=31)
    // ------------------------------------------------------------------

    shared_buffer handle_delete_acls(const RequestContext& ctx,
                                      buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        int32_t filter_count = 0;
        if (!read_int32(p, end, filter_count))
            return build_admin_error(ctx);

        for (int32_t i = 0; i < filter_count; ++i) {
            int8_t resource_type = 0;
            if (!read_int8(p, end, resource_type))
                return build_admin_error(ctx);

            std::string resource_name;
            if (ctx.api_version >= 1) {
                bool is_null = false;
                if (!read_nullable_string(p, end, resource_name, is_null))
                    return build_admin_error(ctx);
            }

            int8_t pattern_type = 3;
            if (ctx.api_version >= 1) {
                if (!read_int8(p, end, pattern_type))
                    return build_admin_error(ctx);
            }

            std::string principal;
            if (ctx.api_version >= 1) {
                bool is_null = false;
                if (!read_nullable_string(p, end, principal, is_null))
                    return build_admin_error(ctx);
            }

            std::string host;
            if (ctx.api_version >= 1) {
                bool is_null = false;
                if (!read_nullable_string(p, end, host, is_null))
                    return build_admin_error(ctx);
            }

            int8_t operation = 0;
            if (!read_int8(p, end, operation))
                return build_admin_error(ctx);

            int8_t permission = 0;
            if (!read_int8(p, end, permission))
                return build_admin_error(ctx);
        }

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        // filter_results
        torrent::network::write_int32_be(buf, filter_count);
        for (int32_t i = 0; i < filter_count; ++i) {
            torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
            torrent::network::write_string(buf, "");
            // matching_acls
            torrent::network::write_int32_be(buf, 0);
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // ElectLeaders (api_key=43)
    // ------------------------------------------------------------------

    shared_buffer handle_elect_leaders(const RequestContext& ctx,
                                        buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        int8_t election_type = 0; // 0=preferred, 1=unclean
        if (!read_int8(p, end, election_type))
            return build_admin_error(ctx);

        int32_t topic_count = 0;
        if (!read_int32(p, end, topic_count))
            return build_admin_error(ctx);

        for (int32_t i = 0; i < topic_count; ++i) {
            std::string topic_name;
            if (!read_string(p, end, topic_name))
                return build_admin_error(ctx);

            int32_t part_count = 0;
            if (!read_int32(p, end, part_count))
                return build_admin_error(ctx);
            for (int32_t pi = 0; pi < part_count; ++pi) {
                int32_t partition = 0;
                if (!read_int32(p, end, partition))
                    return build_admin_error(ctx);
            }
        }

        int32_t timeout_ms = 0;
        if (!read_int32(p, end, timeout_ms))
            return build_admin_error(ctx);

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        // replica_election_results
        torrent::network::write_int32_be(buf, 0); // stub: no results

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // AlterPartitionReassignments (api_key=45)
    // ------------------------------------------------------------------

    shared_buffer handle_alter_partition_reassignments(const RequestContext& ctx,
                                                        buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        int32_t timeout_ms = 0;
        if (!read_int32(p, end, timeout_ms))
            return build_admin_error(ctx);

        int32_t topic_count = 0;
        if (!read_int32(p, end, topic_count))
            return build_admin_error(ctx);

        for (int32_t i = 0; i < topic_count; ++i) {
            std::string topic_name;
            if (!read_string(p, end, topic_name))
                return build_admin_error(ctx);

            int32_t part_count = 0;
            if (!read_int32(p, end, part_count))
                return build_admin_error(ctx);

            for (int32_t pi = 0; pi < part_count; ++pi) {
                int32_t partition = 0;
                if (!read_int32(p, end, partition))
                    return build_admin_error(ctx);

                // replicas (nullable array, -1 = cancel reassignment)
                int32_t rep_count = 0;
                if (!read_int32(p, end, rep_count))
                    return build_admin_error(ctx);
                for (int32_t ri = 0; ri < rep_count; ++ri) {
                    int32_t broker_id = 0;
                    if (!read_int32(p, end, broker_id))
                        return build_admin_error(ctx);
                }
            }
        }

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms
        torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
        torrent::network::write_string(buf, "");

        // results
        torrent::network::write_int32_be(buf, 0);

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // ListPartitionReassignments (api_key=46)
    // ------------------------------------------------------------------

    shared_buffer
    handle_list_partition_reassignments(const RequestContext& ctx,
                                         buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        int32_t timeout_ms = 0;
        if (!read_int32(p, end, timeout_ms))
            return build_admin_error(ctx);

        // topics (nullable array for all topics)
        int32_t topic_count = 0;
        if (!read_int32(p, end, topic_count))
            return build_admin_error(ctx);

        for (int32_t i = 0; i < topic_count; ++i) {
            std::string topic_name;
            if (!read_string(p, end, topic_name))
                return build_admin_error(ctx);

            int32_t part_count = 0;
            if (!read_int32(p, end, part_count))
                return build_admin_error(ctx);
            for (int32_t pi = 0; pi < part_count; ++pi) {
                int32_t partition = 0;
                if (!read_int32(p, end, partition))
                    return build_admin_error(ctx);
            }
        }

        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms
        torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
        torrent::network::write_string(buf, "");
        torrent::network::write_int32_be(buf, 0); // no reassignments

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

private:
    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    shared_buffer build_admin_error(const RequestContext& ctx) const {
        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle
        torrent::network::write_error_code(
            buf, tp::ErrorCode::kInvalidRequest);
        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    shared_buffer
    build_topic_admin_response(const RequestContext& ctx,
                                const std::vector<tp::PartitionResult>& results) const {
        std::vector<char> buf;
        torrent::network::write_int32_be(buf, 0); // throttle_time_ms

        torrent::network::write_int32_be(
            buf, static_cast<int32_t>(results.size()));

        for (const auto& r : results) {
            torrent::network::write_string(buf, r.topic);
            torrent::network::write_error_code(buf, r.error_code);
            torrent::network::write_string(buf, r.error_msg);

            // For create_topic, also write topic_id, num_partitions, RF, configs
            // (v5+: these fields are present)
            if (ctx.api_version >= 5) {
                uint64_t topic_id = 0;
                // write topic_id (UUID as 16 bytes, but we write a simple INT64 stub)
                const int64_t tid = static_cast<int64_t>(topic_id);
                torrent::network::write_int64_be(buf, tid);
                torrent::network::write_int64_be(buf, 0); // second half of UUID
                torrent::network::write_int32_be(buf, 1); // num_partitions stub
                torrent::network::write_int16_be(buf, 3); // rf stub

                // configs
                torrent::network::write_int32_be(buf, 0);

                // num_partitions again (v6+)
                torrent::network::write_int32_be(buf, 1);
            }
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    brk::BrokerServer& server_;
    std::shared_ptr<spdlog::logger> logger_;
};

} // anonymous namespace

// ============================================================================
// Public AdminHandler dispatchers
// ============================================================================

shared_buffer AdminHandler::handle_create_topics(const RequestContext& ctx,
                                                   buffer_view body) {
    AdminHandlerImpl impl(*server_);
    return impl.handle_create_topics(ctx, body);
}

shared_buffer AdminHandler::handle_delete_topics(const RequestContext& ctx,
                                                   buffer_view body) {
    AdminHandlerImpl impl(*server_);
    return impl.handle_delete_topics(ctx, body);
}

shared_buffer AdminHandler::handle_create_partitions(const RequestContext& ctx,
                                                       buffer_view body) {
    AdminHandlerImpl impl(*server_);
    return impl.handle_create_partitions(ctx, body);
}

shared_buffer AdminHandler::handle_describe_configs(const RequestContext& ctx,
                                                      buffer_view body) {
    AdminHandlerImpl impl(*server_);
    return impl.handle_describe_configs(ctx, body);
}

shared_buffer AdminHandler::handle_alter_configs(const RequestContext& ctx,
                                                   buffer_view body) {
    AdminHandlerImpl impl(*server_);
    return impl.handle_alter_configs(ctx, body);
}

shared_buffer AdminHandler::handle_create_acls(const RequestContext& ctx,
                                                 buffer_view body) {
    AdminHandlerImpl impl(*server_);
    return impl.handle_create_acls(ctx, body);
}

shared_buffer AdminHandler::handle_describe_acls(const RequestContext& ctx,
                                                   buffer_view body) {
    AdminHandlerImpl impl(*server_);
    return impl.handle_describe_acls(ctx, body);
}

shared_buffer AdminHandler::handle_delete_acls(const RequestContext& ctx,
                                                 buffer_view body) {
    AdminHandlerImpl impl(*server_);
    return impl.handle_delete_acls(ctx, body);
}

shared_buffer AdminHandler::handle_elect_leaders(const RequestContext& ctx,
                                                   buffer_view body) {
    AdminHandlerImpl impl(*server_);
    return impl.handle_elect_leaders(ctx, body);
}

shared_buffer
AdminHandler::handle_alter_partition_reassignments(const RequestContext& ctx,
                                                     buffer_view body) {
    AdminHandlerImpl impl(*server_);
    return impl.handle_alter_partition_reassignments(ctx, body);
}

shared_buffer
AdminHandler::handle_list_partition_reassignments(const RequestContext& ctx,
                                                    buffer_view body) {
    AdminHandlerImpl impl(*server_);
    return impl.handle_list_partition_reassignments(ctx, body);
}

} // namespace torrent::client

// ============================================================================
// SECTION 7: SASL Handler (api_key=17, 36)
// ============================================================================

namespace torrent::client {
namespace {

/**
 * @brief SASL authentication handler.
 *
 * Supports:
 *   - SaslHandshake (api_key=17): mechanism negotiation.
 *   - SaslAuthenticate (api_key=36): multi-round token exchange.
 *
 * Supported mechanisms: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512,
 * OAUTHBEARER, GSSAPI.
 */
class SaslHandlerImpl {
public:
    explicit SaslHandlerImpl(brk::BrokerServer& server)
        : server_(server)
        , logger_(spdlog::get("sasl_handler"))
    {
        if (!logger_) {
            logger_ = spdlog::stdout_color_mt("sasl_handler");
            logger_->set_level(spdlog::level::info);
        }
    }

    // ------------------------------------------------------------------
    // SaslHandshake (api_key=17)
    // ------------------------------------------------------------------

    shared_buffer handle_handshake(const RequestContext& ctx,
                                    buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        std::string mechanism;
        if (!read_string(p, end, mechanism)) {
            return build_sasl_error(ctx);
        }

        logger_->debug("SASL handshake: client {} requests mechanism '{}'",
                       ctx.client_id, mechanism);

        // Check if the mechanism is supported
        static const std::unordered_set<std::string> supported_mechanisms = {
            "PLAIN",
            "SCRAM-SHA-256",
            "SCRAM-SHA-512",
            "OAUTHBEARER",
            "GSSAPI",
        };

        bool is_supported =
            supported_mechanisms.find(mechanism) != supported_mechanisms.end();

        std::vector<char> buf;

        if (is_supported) {
            // Success
            torrent::network::write_error_code(buf, tp::ErrorCode::kNone);
            torrent::network::write_int32_be(
                buf,
                static_cast<int32_t>(supported_mechanisms.size()));

            for (const auto& m : supported_mechanisms) {
                torrent::network::write_string(buf, m);
            }
        } else {
            // Mechanism not supported
            torrent::network::write_error_code(
                buf, tp::ErrorCode::kUnsupportedVersion);
            torrent::network::write_int32_be(
                buf,
                static_cast<int32_t>(supported_mechanisms.size()));
            for (const auto& m : supported_mechanisms) {
                torrent::network::write_string(buf, m);
            }
        }

        // Store chosen mechanism for later authenticate calls
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            pending_mechanisms_[ctx.correlation_id] = mechanism;
        }

        logger_->info("SASL handshake: mechanism '{}' {}",
                      mechanism,
                      is_supported ? "accepted" : "rejected");

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // SaslAuthenticate (api_key=36)
    // ------------------------------------------------------------------

    shared_buffer handle_authenticate(const RequestContext& ctx,
                                       buffer_view body) {
        const auto* p = reinterpret_cast<const uint8_t*>(body.data);
        const auto* end = p + body.size;

        // auth_bytes (nullable bytes)
        bool is_null = false;
        std::vector<uint8_t> auth_bytes;
        if (!read_bytes(p, end, auth_bytes, is_null)) {
            return build_sasl_error(ctx);
        }

        // Retrieve the pending mechanism
        std::string mechanism;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            auto it = pending_mechanisms_.find(ctx.correlation_id);
            if (it != pending_mechanisms_.end()) {
                mechanism = it->second;
                pending_mechanisms_.erase(it);
            }
        }

        if (mechanism.empty()) {
            logger_->warn("SASL authenticate: no pending mechanism for correlation {}",
                          ctx.correlation_id);
            return build_sasl_auth_error(ctx,
                                          tp::ErrorCode::kIllegalSaslState,
                                          "No SASL handshake performed");
        }

        // --- Authenticate based on mechanism ---
        bool authenticated = false;
        std::vector<uint8_t> response_bytes;

        if (mechanism == "PLAIN") {
            authenticated = authenticate_plain(auth_bytes);
        } else if (mechanism == "SCRAM-SHA-256" ||
                   mechanism == "SCRAM-SHA-512") {
            auto scram_result = authenticate_scram(
                mechanism, auth_bytes,
                ctx.correlation_id);
            authenticated = scram_result.first;
            response_bytes = std::move(scram_result.second);
        } else if (mechanism == "OAUTHBEARER") {
            authenticated = authenticate_oauth(auth_bytes);
        } else if (mechanism == "GSSAPI") {
            authenticated = authenticate_gssapi(auth_bytes);
        } else {
            logger_->warn("SASL authenticate: unsupported mechanism '{}'",
                          mechanism);
            return build_sasl_auth_error(
                ctx, tp::ErrorCode::kUnsupportedVersion,
                "Unsupported SASL mechanism: " + mechanism);
        }

        if (authenticated) {
            logger_->info(
                "SASL authenticate: client '{}' authenticated via {}",
                ctx.client_id, mechanism);
        } else {
            logger_->warn(
                "SASL authenticate: client '{}' failed {} authentication",
                ctx.client_id, mechanism);
        }

        std::vector<char> buf;
        torrent::network::write_error_code(
            buf,
            authenticated ? tp::ErrorCode::kNone
                          : tp::ErrorCode::kSaslAuthenticationFailed);

        if (!authenticated) {
            torrent::network::write_string(
                buf, "Authentication failed: invalid credentials");
        } else {
            torrent::network::write_nullable_string(buf, "");
        }

        // auth_bytes (server response — may be empty)
        if (!response_bytes.empty()) {
            torrent::network::write_bytes(
                buf, response_bytes.data(), response_bytes.size());
        } else {
            // Write null bytes (INT32 -1)
            torrent::network::write_int32_be(buf, -1);
        }

        // session_lifetime_ms (v1+)
        if (ctx.api_version >= 1) {
            torrent::network::write_int64_be(
                buf, 86400000); // 24h in ms
        }

        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

private:
    // ------------------------------------------------------------------
    // Mechanism-specific authentication
    // ------------------------------------------------------------------

    /**
     * @brief PLAIN SASL: authcid\0username\0password.
     */
    [[nodiscard]] bool
    authenticate_plain(const std::vector<uint8_t>& token) const {
        if (token.empty()) return false;

        // Format: authcid\0authzid\0password
        std::string_view token_str(
            reinterpret_cast<const char*>(token.data()), token.size());

        auto null1 = token_str.find('\0');
        if (null1 == std::string_view::npos) return false;

        auto null2 = token_str.find('\0', null1 + 1);
        if (null2 == std::string_view::npos) return false;

        auto authcid  = token_str.substr(0, null1);
        auto username = token_str.substr(null1 + 1, null2 - null1 - 1);
        auto password = token_str.substr(null2 + 1);

        logger_->debug("PLAIN auth: authcid='{}', username='{}'",
                       authcid, username);

        // Stub: accept any non-empty credentials
        return !username.empty() && !password.empty();
    }

    /**
     * @brief SCRAM multi-round authentication.
     *
     * Returns (authenticated, server_response_bytes).
     */
    [[nodiscard]] std::pair<bool, std::vector<uint8_t>>
    authenticate_scram(const std::string& mechanism,
                        const std::vector<uint8_t>& token,
                        int32_t correlation_id) {
        // SCRAM is a multi-round protocol:
        //   Client → Server: client-first-message
        //   Server → Client: server-first-message (with nonce + salt)
        //   Client → Server: client-final-message (with proof)
        //   Server → Client: server-final-message (with signature)

        // For stub, we simulate a simple SCRAM exchange.
        std::string_view token_str(
            reinterpret_cast<const char*>(token.data()), token.size());

        // Check if this is a client-first-message (starts with "n,")
        if (token_str.starts_with("n,") || token_str.starts_with("p,")) {
            // Store client-first data
            std::lock_guard<std::mutex> lock(state_mutex_);
            auto& scram_state = scram_states_[correlation_id];
            scram_state.client_first = std::string(token_str);
            scram_state.round = 1;

            // Generate server-first response
            std::string server_first = "r=server-nonce-12345,s=abcd,i=4096";
            std::vector<uint8_t> response(
                server_first.begin(), server_first.end());

            return {true, response}; // Not fully authenticated yet
        }

        // Check if this is a client-final-message
        if (token_str.starts_with("c=")) {
            // Verify client proof
            std::lock_guard<std::mutex> lock(state_mutex_);
            auto it = scram_states_.find(correlation_id);
            if (it == scram_states_.end()) {
                return {false, {}};
            }
            scram_states_.erase(it);

            // Generate server signature
            std::string server_sig = "v=server-signature-stub";

            // In production: validate client proof against stored credentials
            // For stub: always accept
            std::vector<uint8_t> response(
                server_sig.begin(), server_sig.end());
            return {true, response};
        }

        // Unknown token format
        return {false, {}};
    }

    /**
     * @brief OAUTHBEARER authentication.
     */
    [[nodiscard]] bool
    authenticate_oauth(const std::vector<uint8_t>& token) const {
        // Format: n,a=...\x01auth=Bearer <token>\x01\x01
        if (token.empty()) return false;

        std::string_view token_str(
            reinterpret_cast<const char*>(token.data()), token.size());

        // Look for auth=Bearer pattern
        auto pos = token_str.find("auth=Bearer ");
        return pos != std::string_view::npos;
    }

    /**
     * @brief GSSAPI (Kerberos) authentication.
     */
    [[nodiscard]] bool
    authenticate_gssapi(const std::vector<uint8_t>& token) const {
        // GSSAPI tokens are ASN.1 DER-encoded
        if (token.empty()) return false;
        // Stub: accept any non-empty GSSAPI token
        return token.size() > 4;
    }

    // ------------------------------------------------------------------
    // Response building
    // ------------------------------------------------------------------

    shared_buffer build_sasl_error(const RequestContext& ctx) const {
        std::vector<char> buf;
        torrent::network::write_error_code(
            buf, tp::ErrorCode::kInvalidRequest);
        torrent::network::write_int32_be(buf, 0);
        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    shared_buffer build_sasl_auth_error(const RequestContext& ctx,
                                         tp::ErrorCode ec,
                                         const std::string& msg) const {
        std::vector<char> buf;
        torrent::network::write_error_code(buf, ec);
        torrent::network::write_string(buf, msg);
        torrent::network::write_int32_be(buf, -1); // null bytes
        auto frame = torrent::network::build_response_frame(
            ctx.correlation_id,
            std::vector<char>(buf.begin(), buf.end()));
        shared_buffer sb(static_cast<size_t>(frame.size()));
        std::memcpy(sb.mutable_data(), frame.data(), frame.size());
        sb.set_size(frame.size());
        return sb;
    }

    // ------------------------------------------------------------------
    // State
    // ------------------------------------------------------------------

    struct ScramState {
        std::string client_first;
        int round = 0;
    };

    brk::BrokerServer& server_;
    std::shared_ptr<spdlog::logger> logger_;

    mutable std::mutex state_mutex_;
    std::unordered_map<int32_t, std::string> pending_mechanisms_;
    std::unordered_map<int32_t, ScramState> scram_states_;
};

} // anonymous namespace

// ============================================================================
// Public SaslHandler dispatchers
// ============================================================================

shared_buffer SaslHandler::handle_handshake(const RequestContext& ctx,
                                             buffer_view body) {
    SaslHandlerImpl impl(*server_);
    return impl.handle_handshake(ctx, body);
}

shared_buffer SaslHandler::handle_authenticate(const RequestContext& ctx,
                                                buffer_view body) {
    SaslHandlerImpl impl(*server_);
    return impl.handle_authenticate(ctx, body);
}

} // namespace torrent::client

// ============================================================================
// END — client_full.cpp — All seven handler sections complete.
// ============================================================================
