#pragma once

/**
 * torrent-mq — High-Performance Distributed Message Queue
 *
 * A C++20 distributed messaging platform designed for microsecond latency,
 * disk-backed persistence with exactly-once guarantees, and horizontal
 * scalability via Raft-based consensus.
 *
 * Key features:
 *   - Append-only segmented log storage backed by RocksDB or raw files
 *   - Raft consensus for metadata management and leader election
 *   - gRPC + raw TCP dual transport with TLS 1.3
 *   - Consumer groups with automatic offset management
 *   - Schema registry with Avro, Protobuf, and JSON Schema support
 *   - Stream processing engine with windowed aggregations
 *   - Connect framework for source/sink connectors
 *   - Admin REST API + CLI tooling
 *   - Prometheus metrics with JMX compatibility
 *   - SASL/SCRAM, mTLS, Kerberos, OAuth2 authentication
 *   - Tiered storage with S3 archival
 *   - Transactions (exactly-once produce)
 *   - Compaction, retention, and quotas
 */

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <optional>
#include <chrono>
#include <memory>
#include <functional>
#include <variant>
#include <span>

namespace torrent {

// ============================================================================
// Protocol version constants
// ============================================================================

inline constexpr int16_t kApiVersionMajor = 0;
inline constexpr int16_t kApiVersionMinor = 10;
inline constexpr int16_t kApiVersionPatch = 0;

inline constexpr uint16_t kDefaultPort = 9092;
inline constexpr uint16_t kDefaultTlsPort = 9093;
inline constexpr uint16_t kDefaultAdminPort = 9644;
inline constexpr uint16_t kDefaultPrometheusPort = 9090;
inline constexpr uint16_t kDefaultSchemaPort = 8081;
inline constexpr uint16_t kDefaultProxyPort = 8082;

// ============================================================================
// Fundamental types
// ============================================================================

/// Unique identifier for a broker node in the cluster
using broker_id_t = int32_t;
inline constexpr broker_id_t kAnyBroker = -1;
inline constexpr broker_id_t kNoBroker = -2;

/// Unique identifier for a topic
using topic_id_t = uint64_t;

/// Topic name (human-readable)
using topic_name_t = std::string;

/// Partition index within a topic
using partition_id_t = int32_t;
inline constexpr partition_id_t kAnyPartition = -1;

/// Offset within a partition (monotonically increasing)
using offset_t = int64_t;
inline constexpr offset_t kInvalidOffset = -1;
inline constexpr offset_t kEarliestOffset = -2;
inline constexpr offset_t kLatestOffset = -1;
inline constexpr offset_t kTimestampOffset = -3;

/// Logical epoch for fencing
using epoch_t = int64_t;

/// Raft term number
using term_t = int64_t;

/// Consumer group identifier
using group_id_t = std::string;

/// Producer epoch (for idempotent/transactional producers)
using producer_epoch_t = int16_t;

/// Producer ID (for idempotent/transactional producers)
using producer_id_t = int64_t;

/// Transactional ID
using transactional_id_t = std::string;

/// Sequence number (idempotent producer)
using sequence_t = int32_t;

/// Timestamp in milliseconds since epoch
using timestamp_ms_t = int64_t;

/// Size in bytes
using byte_count_t = int64_t;

/// Duration in milliseconds
using duration_ms_t = int64_t;

// ============================================================================
// Buffer types
// ============================================================================

/// A lightweight non-owning buffer reference
struct buffer_view {
    const char* data = nullptr;
    size_t size = 0;

    buffer_view() = default;
    buffer_view(const char* d, size_t s) : data(d), size(s) {}
    buffer_view(std::string_view sv) : data(sv.data()), size(sv.size()) {}

    [[nodiscard]] bool empty() const noexcept { return size == 0 || data == nullptr; }
    [[nodiscard]] std::string_view view() const noexcept { return {data, size}; }
};

/// Reference-counted buffer for zero-copy messaging
class shared_buffer {
public:
    shared_buffer() = default;

    explicit shared_buffer(size_t capacity)
        : data_(new char[capacity]), size_(0), capacity_(capacity) {}

    shared_buffer(const char* d, size_t s)
        : data_(new char[s]), size_(s), capacity_(s) {
        std::memcpy(data_.get(), d, s);
    }

    shared_buffer(const shared_buffer&) = delete;
    shared_buffer& operator=(const shared_buffer&) = delete;

    shared_buffer(shared_buffer&&) noexcept = default;
    shared_buffer& operator=(shared_buffer&&) noexcept = default;

    [[nodiscard]] const char* data() const noexcept { return data_.get(); }
    [[nodiscard]] char* mutable_data() noexcept { return data_.get(); }
    [[nodiscard]] size_t size() const noexcept { return size_; }
    [[nodiscard]] size_t capacity() const noexcept { return capacity_; }
    [[nodiscard]] bool empty() const noexcept { return size_ == 0; }

    void set_size(size_t s) noexcept { size_ = s; }

private:
    std::unique_ptr<char[]> data_;
    size_t size_ = 0;
    size_t capacity_ = 0;
};

// ============================================================================
// Error codes
// ============================================================================

enum class error_code : int16_t {
    none = 0,
    unknown_server_error = -1,
    offset_out_of_range = 1,
    corrupt_message = 2,
    unknown_topic_or_partition = 3,
    invalid_fetch_size = 4,
    leader_not_available = 5,
    not_leader_for_partition = 6,
    request_timed_out = 7,
    broker_not_available = 8,
    replica_not_available = 9,
    message_too_large = 10,
    stale_controller_epoch = 11,
    offset_metadata_too_large = 12,
    network_exception = 13,
    coordinator_load_in_progress = 14,
    coordinator_not_available = 15,
    not_coordinator = 16,
    invalid_topic_exception = 17,
    record_list_too_large = 18,
    not_enough_replicas = 19,
    not_enough_replicas_after_append = 20,
    invalid_required_acks = 21,
    illegal_generation = 22,
    inconsistent_group_protocol = 23,
    invalid_group_id = 24,
    unknown_member_id = 25,
    invalid_session_timeout = 26,
    rebalance_in_progress = 27,
    invalid_commit_offset_size = 28,
    topic_authorization_failed = 29,
    group_authorization_failed = 30,
    cluster_authorization_failed = 31,
    invalid_timestamp = 32,
    unsupported_sasl_mechanism = 33,
    illegal_sasl_state = 34,
    unsupported_version = 35,
    topic_already_exists = 36,
    invalid_partitions = 37,
    invalid_replication_factor = 38,
    invalid_replica_assignment = 39,
    invalid_config = 40,
    not_controller = 41,
    invalid_request = 42,
    unsupported_for_message_format = 43,
    policy_violation = 44,
    out_of_order_sequence_number = 45,
    duplicate_sequence_number = 46,
    invalid_producer_epoch = 47,
    invalid_txn_state = 48,
    invalid_producer_id_mapping = 49,
    invalid_transaction_timeout = 50,
    concurrent_transactions = 51,
    transaction_coordinator_fenced = 52,
    transactional_id_authorization_failed = 53,
    security_disabled = 54,
    operation_not_attempted = 55,
    kafka_storage_error = 56,
    log_dir_not_found = 57,
    sasl_authentication_failed = 58,
    unknown_producer_id = 59,
    reassignment_in_progress = 60,
    delegation_token_auth_disabled = 61,
    delegation_token_not_found = 62,
    delegation_token_owner_mismatch = 63,
    delegation_token_request_not_allowed = 64,
    delegation_token_authorization_failed = 65,
    delegation_token_expired = 66,
    invalid_principal_type = 67,
    non_empty_group = 68,
    group_id_not_found = 69,
    fetch_session_id_not_found = 70,
    invalid_fetch_session_epoch = 71,
    listener_not_found = 72,
    topic_deletion_disabled = 73,
    fenced_leader_epoch = 74,
    unknown_leader_epoch = 75,
    unsupported_compression_type = 76,
    stale_broker_epoch = 77,
    offset_not_available = 78,
    member_id_required = 79,
    preferred_leader_not_available = 80,
    group_max_size_reached = 81,
    fenced_instance_id = 82,
    eligible_leaders_not_available = 83,
    election_not_needed = 84,
    no_reassignment_in_progress = 85,
    group_subscribed_to_topic = 86,
    invalid_record = 87,
    unstable_offset_commit = 88,
    throttling_quota_exceeded = 89,
    producer_fenced = 90,
    resource_not_found = 91,
    duplicate_resource = 92,
    unacceptable_credential = 93,
    inconsistent_voter_set = 94,
    invalid_update_version = 95,
    feature_update_failed = 96,
    principal_deserialization_failure = 97,

    // torrent-mq specific codes (1000+)
    internal_queue_full = 1000,
    shutdown_in_progress = 1001,
    partition_not_found = 1002,
    segment_corrupted = 1003,
    recovery_needed = 1004,
    storage_unavailable = 1005,
    schema_not_found = 1006,
    schema_incompatible = 1007,
    connector_failed = 1008,
    stream_task_failed = 1009,
    proxy_unavailable = 1010,
};

[[nodiscard]] constexpr const char* error_code_name(error_code ec) noexcept {
    switch (ec) {
    case error_code::none: return "NONE";
    case error_code::unknown_server_error: return "UNKNOWN_SERVER_ERROR";
    case error_code::offset_out_of_range: return "OFFSET_OUT_OF_RANGE";
    case error_code::corrupt_message: return "CORRUPT_MESSAGE";
    case error_code::unknown_topic_or_partition: return "UNKNOWN_TOPIC_OR_PARTITION";
    case error_code::leader_not_available: return "LEADER_NOT_AVAILABLE";
    case error_code::not_leader_for_partition: return "NOT_LEADER_FOR_PARTITION";
    case error_code::request_timed_out: return "REQUEST_TIMED_OUT";
    case error_code::broker_not_available: return "BROKER_NOT_AVAILABLE";
    case error_code::not_controller: return "NOT_CONTROLLER";
    case error_code::topic_authorization_failed: return "TOPIC_AUTHORIZATION_FAILED";
    case error_code::transaction_coordinator_fenced: return "TRANSACTION_COORDINATOR_FENCED";
    default: return "UNKNOWN_ERROR_CODE";
    }
}

// ============================================================================
// Compression types
// ============================================================================

enum class compression_type : int8_t {
    none = 0,
    gzip = 1,
    snappy = 2,
    lz4 = 3,
    zstd = 4,
};

[[nodiscard]] constexpr const char* compression_name(compression_type ct) noexcept {
    switch (ct) {
    case compression_type::none: return "none";
    case compression_type::gzip: return "gzip";
    case compression_type::snappy: return "snappy";
    case compression_type::lz4: return "lz4";
    case compression_type::zstd: return "zstd";
    }
    return "unknown";
}

// ============================================================================
// Isolation level for transactions
// ============================================================================

enum class isolation_level : int8_t {
    read_uncommitted = 0,
    read_committed = 1,
};

// ============================================================================
// Required acknowledgements for produce
// ============================================================================

enum class required_acks : int16_t {
    none = 0,
    leader = 1,
    all_isr = -1,
};

// ============================================================================
// Result type
// ============================================================================

template<typename T>
struct result {
    T value;
    error_code error = error_code::none;
    std::string error_message;

    [[nodiscard]] bool ok() const noexcept { return error == error_code::none; }
    [[nodiscard]] bool failed() const noexcept { return error != error_code::none; }

    static result<T> success(T val) {
        result<T> r;
        r.value = std::move(val);
        return r;
    }

    static result<T> failure(error_code ec, std::string msg = {}) {
        result<T> r;
        r.error = ec;
        r.error_message = std::move(msg);
        return r;
    }
};

// ============================================================================
// Endpoint (host:port)
// ============================================================================

struct endpoint {
    std::string host;
    uint16_t port = kDefaultPort;

    [[nodiscard]] std::string to_string() const {
        return host + ":" + std::to_string(port);
    }

    [[nodiscard]] bool operator==(const endpoint& other) const noexcept {
        return port == other.port && host == other.host;
    }

    [[nodiscard]] bool operator!=(const endpoint& other) const noexcept {
        return !(*this == other);
    }

    [[nodiscard]] bool operator<(const endpoint& other) const noexcept {
        if (host != other.host) return host < other.host;
        return port < other.port;
    }
};

} // namespace torrent

namespace std {
template<>
struct hash<torrent::endpoint> {
    size_t operator()(const torrent::endpoint& ep) const noexcept {
        return hash<string>{}(ep.host) ^ (hash<uint16_t>{}(ep.port) << 1);
    }
};
} // namespace std
