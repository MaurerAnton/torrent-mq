#pragma once

/**
 * torrent-mq — Storage Types
 *
 * Defines all storage-level data structures used by the torrent-mq
 * distributed message queue. These types bridge the wire protocol
 * (common/types.h) with the on-disk log implementation.
 *
 * Structures defined:
 *   Record           — A single message in the log
 *   RecordHeader     — Key-value metadata header on a record
 *   RecordBatch      — A compressed batch of records with integrity checks
 *   SegmentInfo      — Metadata for a single log segment on disk
 *   PartitionState   — Runtime state of a partition (watermarks, offsets)
 *   PartitionMetadata— Static metadata describing a partition's topology
 *   BrokerMetadata   — Identity and endpoints of a broker
 *   TopicConfig      — User-configurable topic parameters
 *   TopicMetadata    — Complete topic descriptor (id, name, partitions, config)
 *   ConsumerGroupState— Lifetime state of a consumer group
 *   OffsetCommit     — A single consumer offset commit
 *   ReplicaStatus    — Health snapshot of a replica for monitoring
 *   LogAppendResult  — Result returned after appending records to the log
 *   FetchResult      — Result returned after fetching records from the log
 */

#include <cstdint>
#include <string>
#include <vector>
#include <optional>
#include <chrono>
#include <variant>
#include <memory>

#include "torrent/common/types.h"

namespace torrent {

// ============================================================================
// Record — a single atomic message stored in the log
// ============================================================================

/// A key-value header attached to a record (used for routing, tracing, etc.)
struct RecordHeader {
    std::string key;      ///< Header name (e.g. "trace-id")
    std::string value;    ///< Header value
};

/// A single message record — the fundamental unit of storage in torrent-mq.
///
/// Records are written into segments inside RecordBatches.  Keys, values,
/// and headers are stored as shared_buffers for zero-copy transfer between
/// layers (network → storage → network).
struct Record {
    /// Optional message key (partitioning key).  May be empty.
    shared_buffer key;

    /// Message payload.  May be empty for tombstones / heartbeat records.
    shared_buffer value;

    /// Arbitrary key-value headers (tracing, content-type, etc.)
    std::vector<RecordHeader> headers;

    /// Producer-assigned timestamp (milliseconds since epoch).
    /// Set by the broker if the producer leaves this at 0.
    timestamp_ms_t timestamp = 0;

    /// Offset assigned by the broker at append time.
    /// kInvalidOffset until the broker commits the record.
    offset_t offset = kInvalidOffset;

    /// Partition this record belongs to.
    partition_id_t partition_id = 0;

    /// Sequence number for idempotent producers.
    sequence_t sequence = 0;

    /// Producer ID (set by broker for idempotent/transactional producers).
    producer_id_t producer_id = -1;

    // ------------------------------------------------------------------
    // Convenience accessors
    // ------------------------------------------------------------------

    /// True when key and value are both empty (control / tombstone record).
    [[nodiscard]] bool is_tombstone() const noexcept {
        return key.empty() && value.empty();
    }

    /// Total approximate size of the record in bytes (for quota tracking).
    [[nodiscard]] byte_count_t approximate_size() const noexcept {
        byte_count_t n = static_cast<byte_count_t>(key.size() + value.size());
        for (const auto& h : headers) {
            n += static_cast<byte_count_t>(h.key.size() + h.value.size());
        }
        return n;
    }
};

// ============================================================================
// RecordBatch — a compressed, CRC-protected collection of records
// ============================================================================

/// Attribute flags carried in a RecordBatch's attributes field.
enum class record_batch_attributes : int16_t {
    none               = 0,
    /// The batch uses the v2 message format (Kafka 0.11+)
    is_transactional   = 0x0010,
    /// The batch is part of a transaction and not yet committed
    is_control_batch   = 0x0020,
    /// The batch contains a control message (commit/abort marker)
    has_delete_horizon = 0x0040,
};

/// A compressed batch of records written atomically to the log.
///
/// RecordBatches are the unit of replication: a batch is either fully
/// committed across the ISR or fully discarded.  CRC-32C protects the
/// batch from header to last record on disk.
struct RecordBatch {
    /// The offset of the first record in this batch.
    offset_t base_offset = kInvalidOffset;

    /// Producer-assigned timestamp of the earliest record in the batch.
    timestamp_ms_t base_timestamp = 0;

    /// Producer-assigned timestamp of the latest record in the batch.
    timestamp_ms_t max_timestamp = 0;

    /// Offset delta from base_offset to the last record in the batch.
    int32_t last_offset_delta = 0;

    /// Compression algorithm applied to the records payload.
    compression_type compression = compression_type::none;

    /// Bitfield of record_batch_attributes.
    int16_t attributes = 0;

    /// CRC-32C checksum covering everything from `attributes` through
    /// the end of the last record in `records`.
    uint32_t crc = 0;

    /// Producer ID (for idempotent / transactional producers).
    producer_id_t producer_id = -1;

    /// Producer epoch (fencing token).
    producer_epoch_t producer_epoch = -1;

    /// Sequence number of the first record in the batch.
    sequence_t base_sequence = -1;

    /// Number of records contained in this batch.
    int32_t record_count = 0;

    /// Leader epoch at the time this batch was appended.
    epoch_t partition_leader_epoch = 0;

    /// The actual records (may be empty for control batches).
    std::vector<Record> records;

    // ------------------------------------------------------------------
    // Convenience accessors
    // ------------------------------------------------------------------

    /// True when this is a transactional batch.
    [[nodiscard]] bool is_transactional() const noexcept {
        return (attributes & static_cast<int16_t>(record_batch_attributes::is_transactional)) != 0;
    }

    /// True when this is a control batch (commit / abort marker).
    [[nodiscard]] bool is_control_batch() const noexcept {
        return (attributes & static_cast<int16_t>(record_batch_attributes::is_control_batch)) != 0;
    }

    /// Total approximate byte size of the batch (excluding CRC overhead).
    [[nodiscard]] byte_count_t approximate_size() const noexcept {
        byte_count_t n = 0;
        for (const auto& r : records) {
            n += r.approximate_size();
        }
        return n;
    }
};

// ============================================================================
// SegmentInfo — metadata for a single on-disk log segment
// ============================================================================

/// Describes a single log segment file on disk.
///
/// Each partition is stored as an ordered sequence of segments.
/// Only the last (active) segment accepts writes; older segments
/// are read-only and eligible for compaction / archival.
struct SegmentInfo {
    /// Monotonically increasing segment identifier (partition-local).
    uint64_t segment_id = 0;

    /// First offset stored in this segment (inclusive).
    offset_t base_offset = kInvalidOffset;

    /// Next offset that would be appended (exclusive upper bound).
    /// Equal to base_offset when the segment is empty.
    offset_t next_offset = kInvalidOffset;

    /// Filesystem path to the segment data file.
    std::string file_path;

    /// Current size of the segment data file on disk.
    byte_count_t file_size = 0;

    /// Current size of the offset index file on disk.
    byte_count_t index_size = 0;

    /// Current size of the time-based index file on disk.
    byte_count_t time_index_size = 0;

    /// Highest timestamp among records in this segment.
    timestamp_ms_t max_timestamp = 0;

    /// Wall-clock time when this segment was created.
    timestamp_ms_t created_at = 0;

    /// Wall-clock time when this segment was last written to.
    timestamp_ms_t last_modified = 0;

    /// True if this is the active (writable) segment.
    bool is_active = false;

    /// True if the segment has been closed and sealed (no more writes).
    bool is_sealed = false;

    // ------------------------------------------------------------------
    // Convenience
    // ------------------------------------------------------------------

    /// Number of records contained in this segment (computed from offsets).
    [[nodiscard]] int64_t record_count() const noexcept {
        return next_offset - base_offset;
    }

    /// True when the segment contains no records.
    [[nodiscard]] bool empty() const noexcept {
        return next_offset <= base_offset;
    }
};

// ============================================================================
// PartitionState — runtime state of a single partition
// ============================================================================

/// Snapshot of a partition's runtime state (watermarks, lag, segment count).
///
/// This is refreshed periodically by the partition leader and exposed
/// via metrics and the admin API.
struct PartitionState {
    /// Partition this state refers to.
    partition_id_t partition_id = 0;

    /// High watermark: the highest offset that has been replicated
    /// to the full ISR (consumers can read up to this offset).
    offset_t high_watermark = kInvalidOffset;

    /// Log-start offset: the oldest offset still available (may have
    /// been advanced by retention or compaction).
    offset_t log_start_offset = kInvalidOffset;

    /// Log-end offset: the next offset that will be assigned to an
    /// incoming record (i.e., one past the last committed record).
    offset_t log_end_offset = kInvalidOffset;

    /// Last stable offset (for transactional reads — read_committed).
    offset_t last_stable_offset = kInvalidOffset;

    /// Number of segments currently managed by this partition.
    int32_t num_segments = 0;

    /// Total on-disk size of all segments belonging to this partition.
    byte_count_t total_size = 0;

    /// Number of bytes between the log-end offset and the high watermark.
    [[nodiscard]] byte_count_t lag_bytes() const noexcept {
        // estimated; real implementation measures actual payload sizes
        return (log_end_offset - high_watermark) > 0
                   ? (log_end_offset - high_watermark)
                   : 0;
    }
};

// ============================================================================
// PartitionMetadata — static topology descriptor for a partition
// ============================================================================

/// Describes the replication topology of a single partition.
///
/// This structure is managed by the controller (Raft leader) and
/// propagated to all brokers via metadata responses.
struct PartitionMetadata {
    /// Owning topic.
    topic_id_t topic_id = 0;

    /// Partition index within the topic.
    partition_id_t partition_id = 0;

    /// Broker currently acting as the partition leader.
    broker_id_t leader = kNoBroker;

    /// Current leader epoch (fencing token).
    epoch_t leader_epoch = 0;

    /// Full replica set for this partition (ordered by preference).
    std::vector<broker_id_t> replicas;

    /// In-sync replicas (subset of `replicas` that are caught up).
    std::vector<broker_id_t> isr;

    /// Replicas known to be offline / unreachable.
    std::vector<broker_id_t> offline_replicas;

    /// Monotonically increasing partition-level epoch.
    epoch_t partition_epoch = 0;

    /// Error code (set to none when metadata is valid).
    error_code error = error_code::none;

    // ------------------------------------------------------------------
    // Convenience
    // ------------------------------------------------------------------

    /// The replication factor (total number of replicas).
    [[nodiscard]] int16_t replication_factor() const noexcept {
        return static_cast<int16_t>(replicas.size());
    }

    /// Number of in-sync replicas.
    [[nodiscard]] int16_t isr_count() const noexcept {
        return static_cast<int16_t>(isr.size());
    }

    /// True when the configured min ISR requirement is met.
    [[nodiscard]] bool is_under_min_isr(int16_t min_isr) const noexcept {
        return isr_count() < min_isr;
    }
};

// ============================================================================
// BrokerMetadata — identity and endpoints of a broker node
// ============================================================================

/// Describes a broker in the cluster.
///
/// This is advertised by each broker on startup and refreshed when
/// the configuration changes.
struct BrokerMetadata {
    /// Unique broker identifier.
    broker_id_t broker_id = kNoBroker;

    /// Hostname or IP address.
    std::string host;

    /// Primary message port (usually kDefaultPort = 9092).
    uint16_t port = kDefaultPort;

    /// Optional rack identifier for rack-aware placement.
    std::optional<std::string> rack;

    /// Whether this broker is the current cluster controller.
    bool is_controller = false;

    /// All advertised endpoints for this broker (internal, external, etc.).
    std::vector<endpoint> endpoints;

    /// Whether the broker is online and reachable.
    bool is_alive = false;

    /// Software version reported by the broker.
    std::string version;
};

// ============================================================================
// TopicConfig — user-configurable topic parameters
// ============================================================================

/// Cleanup policy for log segments.
enum class cleanup_policy : uint8_t {
    /// Delete old segments based on retention time/size.
    delete_only = 0,
    /// Compact the log by keeping only the latest value per key.
    compact_only = 1,
    /// Apply both deletion and compaction.
    compact_and_delete = 2,
};

/// Configuration for a single topic.
///
/// Defaults match Apache Kafka defaults where applicable so that
/// existing tooling and client expectations are preserved.
struct TopicConfig {
    /// Human-readable topic name.
    topic_name_t name;

    /// Number of partitions (fixed at creation time unless altered).
    int32_t num_partitions = 1;

    /// Replication factor (fixed at creation time).
    int16_t replication_factor = 1;

    /// Maximum time to retain messages (milliseconds).
    /// -1 means infinite retention.
    duration_ms_t retention_ms = 604800000;   // 7 days

    /// Maximum total bytes to retain per partition before deletion.
    /// -1 means no size-based limit.
    byte_count_t retention_bytes = -1;

    /// Maximum size of a single segment before rolling to a new one.
    byte_count_t segment_bytes = 1073741824;   // 1 GiB

    /// Maximum time before rolling an active segment (milliseconds).
    /// -1 means no time-based rolling.
    duration_ms_t segment_ms = 604800000;      // 7 days

    /// Cleanup strategy for old segments.
    cleanup_policy policy = cleanup_policy::delete_only;

    /// Default compression for the topic (overridable per-producer).
    compression_type compression = compression_type::none;

    /// Maximum size of a single message (record batch) accepted by the topic.
    byte_count_t max_message_bytes = 1048588;  // ~1 MiB

    /// Minimum number of in-sync replicas that must acknowledge a write.
    int16_t min_insync_replicas = 1;

    /// Flush (fsync) the log after this many messages.
    /// -1 means the broker decides.
    int64_t flush_messages = 9223372036854775807;  // Long.MAX_VALUE → broker decides

    /// Flush (fsync) the log after this many milliseconds.
    /// -1 means the broker decides.
    duration_ms_t flush_ms = 9223372036854775807;

    /// Pre-allocate segment files on creation (avoids fragmentation).
    bool preallocate = false;

    /// Index entry interval in bytes (controls index granularity).
    int32_t index_interval_bytes = 4096;

    /// When true, this is an internal topic managed by the cluster
    /// (e.g. __consumer_offsets, __transaction_state).
    bool is_internal = false;

    /// Whether the topic is read-only (no produce requests accepted).
    bool is_read_only = false;

    /// Apply compaction at this key-level offset frequency.
    int64_t min_compaction_lag_ms = 0;

    /// Minimum tombstones that must exist before a key is eligible
    /// for compaction-based deletion.
    int64_t delete_retention_ms = 86400000;    // 1 day

    // ------------------------------------------------------------------
    // Convenience
    // ------------------------------------------------------------------

    /// True when time-based retention is enabled (not infinite).
    [[nodiscard]] bool has_time_retention() const noexcept {
        return retention_ms > 0;
    }

    /// True when size-based retention is enabled.
    [[nodiscard]] bool has_size_retention() const noexcept {
        return retention_bytes > 0;
    }
};

// ============================================================================
// TopicMetadata — complete topic descriptor
// ============================================================================

/// Full metadata for a single topic as returned to clients.
struct TopicMetadata {
    /// Internal numeric topic identifier.
    topic_id_t topic_id = 0;

    /// Human-readable topic name.
    topic_name_t name;

    /// Per-partition metadata (topology, leader, ISR).
    std::vector<PartitionMetadata> partitions;

    /// User-specified (and broker-defaulted) configuration.
    TopicConfig config;

    /// Whether this is an internal cluster topic.
    bool is_internal = false;

    /// Per-partition or topic-wide error.
    error_code error = error_code::none;

    // ------------------------------------------------------------------
    // Convenience
    // ------------------------------------------------------------------

    /// Total number of partitions across all replicas.
    [[nodiscard]] int32_t total_replica_count() const noexcept {
        int32_t count = 0;
        for (const auto& p : partitions) {
            count += static_cast<int32_t>(p.replicas.size());
        }
        return count;
    }
};

// ============================================================================
// ConsumerGroupState — lifecycle state of a consumer group
// ============================================================================

/// The high-level protocol state of a consumer group.
enum class group_state : uint8_t {
    unknown              = 0,
    preparing_rebalance  = 1,
    completing_rebalance = 2,
    stable               = 3,
    dead                 = 4,
    empty                = 5,
};

/// A single member of a consumer group.
struct GroupMember {
    /// Unique member identifier assigned by the coordinator.
    std::string member_id;

    /// Client-provided group instance id (for static membership).
    std::optional<std::string> group_instance_id;

    /// Client host (for debugging / admin visibility).
    std::string client_host;

    /// Protocol metadata opaque to the broker.
    shared_buffer protocol_metadata;

    /// Partitions assigned to this member (by partition ID).
    std::vector<partition_id_t> assigned_partitions;
};

/// Full state snapshot of a consumer group as tracked by the coordinator.
struct ConsumerGroupState {
    /// Consumer group identifier.
    group_id_t group_id;

    /// Current lifecycle state.
    group_state state = group_state::unknown;

    /// Protocol type (e.g. "consumer", "connect").
    std::string protocol_type;

    /// Protocol name chosen by the group leader.
    std::string protocol;

    /// Member id of the group leader.
    std::string leader_id;

    /// All members currently registered in the group.
    std::vector<GroupMember> members;

    /// Generation id — incremented on every rebalance.
    int32_t generation_id = -1;

    /// Broker acting as the group coordinator.
    broker_id_t coordinator = kNoBroker;

    // ------------------------------------------------------------------
    // Convenience
    // ------------------------------------------------------------------

    /// True when the group is actively consuming.
    [[nodiscard]] bool is_stable() const noexcept {
        return state == group_state::stable;
    }

    /// True when no members are registered.
    [[nodiscard]] bool is_empty() const noexcept {
        return members.empty();
    }
};

// ============================================================================
// OffsetCommit — a single offset commit from a consumer
// ============================================================================

/// Represents a committed offset for a consumer group on a topic-partition.
///
/// Offset commits are stored in the internal __consumer_offsets topic.
struct OffsetCommit {
    /// Consumer group that owns this offset.
    group_id_t group_id;

    /// Topic this offset belongs to.
    topic_id_t topic_id = 0;

    /// Partition this offset belongs to.
    partition_id_t partition_id = 0;

    /// The committed offset (next offset the consumer will read).
    offset_t offset = kInvalidOffset;

    /// Leader epoch at the time this offset was committed.
    epoch_t leader_epoch = -1;

    /// Optional consumer-provided metadata (e.g. application state).
    std::string metadata;

    /// Timestamp of the commit (assigned by the coordinator).
    timestamp_ms_t commit_timestamp = 0;

    /// Expiration timestamp for this commit (for offset retention).
    /// -1 means no expiration.
    timestamp_ms_t expire_timestamp = -1;
};

// ============================================================================
// ReplicaStatus — health snapshot of a single replica
// ============================================================================

/// Runtime status of a replica, refreshed by the leader for ISR management
/// and monitoring / admin visibility.
struct ReplicaStatus {
    /// Broker hosting this replica.
    broker_id_t broker_id = kNoBroker;

    /// Partition this replica belongs to.
    partition_id_t partition_id = 0;

    /// True when this replica is the partition leader.
    bool is_leader = false;

    /// True when this replica is in the ISR.
    bool is_in_sync = false;

    /// Last time a fetch request was received from this replica.
    timestamp_ms_t last_fetch_time = 0;

    /// Last time this replica was fully caught up with the leader.
    timestamp_ms_t last_caught_up_time = 0;

    /// Oldest offset available on this replica.
    offset_t log_start_offset = kInvalidOffset;

    /// Next offset to be assigned on this replica.
    offset_t log_end_offset = kInvalidOffset;

    /// High watermark known by this replica.
    offset_t high_watermark = kInvalidOffset;

    /// Estimated replication lag (in number of offsets behind the leader).
    int64_t replica_lag = 0;

    /// Whether this replica is considered online.
    bool is_online = true;

    // ------------------------------------------------------------------
    // Convenience
    // ------------------------------------------------------------------

    /// True when the replica has zero lag relative to the leader.
    [[nodiscard]] bool is_caught_up() const noexcept {
        return replica_lag <= 0;
    }
};

// ============================================================================
// LogAppendResult — result of a log append operation
// ============================================================================

/// Returned by the storage layer after successfully (or unsuccessfully)
/// appending records to a partition log.
struct LogAppendResult {
    /// The offset assigned to the first record in the appended batch.
    offset_t base_offset = kInvalidOffset;

    /// Broker-assigned timestamp for when the append completed.
    timestamp_ms_t log_append_time = 0;

    /// Error code (error_code::none on success).
    error_code error = error_code::none;

    /// Human-readable error message on failure.
    std::string error_message;

    // ------------------------------------------------------------------
    // Convenience
    // ------------------------------------------------------------------

    /// True when the append succeeded.
    [[nodiscard]] bool ok() const noexcept { return error == error_code::none; }
};

// ============================================================================
// FetchResult — result of a log fetch operation
// ============================================================================

/// Returned by the storage layer after fetching records from a partition log.
struct FetchResult {
    /// The records that were fetched (may be empty if no data is available).
    /// std::nullopt means the fetch was rejected outright (e.g. offset out of range).
    std::optional<RecordBatch> batch;

    /// Current high watermark at the time of the fetch.
    /// Consumers use this to track progress; no data with offset >= this
    /// is guaranteed committed.
    offset_t high_watermark = kInvalidOffset;

    /// Oldest offset still available on the partition.
    offset_t log_start_offset = kInvalidOffset;

    /// Last stable offset (relevant for read_committed transactions).
    offset_t last_stable_offset = kInvalidOffset;

    /// Error code (error_code::none on success).
    error_code error = error_code::none;

    /// Human-readable error message on failure.
    std::string error_message;

    /// True when the result was truncated due to size limits and the
    /// client should issue another fetch to get the remaining data.
    bool is_truncated = false;

    // ------------------------------------------------------------------
    // Convenience
    // ------------------------------------------------------------------

    /// True when the fetch succeeded and returned data.
    [[nodiscard]] bool ok() const noexcept { return error == error_code::none; }

    /// True when records were returned.
    [[nodiscard]] bool has_records() const noexcept {
        return batch.has_value() && !batch->records.empty();
    }

    /// Number of records in the returned batch.
    [[nodiscard]] size_t record_count() const noexcept {
        if (!batch.has_value()) return 0;
        return batch->records.size();
    }
};

} // namespace torrent
