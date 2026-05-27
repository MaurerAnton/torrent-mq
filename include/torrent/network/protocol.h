#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <memory>
#include <optional>
#include <variant>
#include <functional>
#include <unordered_map>
#include <span>

/// \file protocol.h
/// \brief Complete wire protocol for torrent-mq distributed message queue.
///
/// Defines message framing (4-byte length prefix), all Kafka-compatible and
/// torrent-native API key constants, request/response structs for every API,
/// API versioning, error codes, and utility helpers. All types live in
/// \c torrent::protocol.

namespace torrent::protocol {

// ============================================================================
// Message Framing
// ============================================================================

/// Number of bytes used for the length prefix prepended to every frame.
constexpr uint32_t kFrameLengthSize = 4;

/// Maximum allowed frame payload size in bytes (100 MiB).
constexpr uint32_t kMaxFramePayloadSize = 100 * 1024 * 1024;

/// Maximum total frame size on the wire (length prefix + payload).
constexpr uint32_t kMaxFrameSize = kFrameLengthSize + kMaxFramePayloadSize;

// ============================================================================
// API Version Constants
// ============================================================================

/// Default API version used when no version is negotiated.
constexpr int16_t kDefaultApiVersion  = 0;

/// Minimum API version recognised by the protocol stack.
constexpr int16_t kMinApiVersion      = 0;

/// Maximum API version recognised by the protocol stack.
constexpr int16_t kMaxApiVersion      = 7;

// ============================================================================
// API Key Constants — Kafka-Compatible (0–51)
// ============================================================================

constexpr int16_t kApiKeyProduce                        = 0;   ///< Produce messages to a topic-partition.
constexpr int16_t kApiKeyFetch                          = 1;   ///< Fetch messages from a topic-partition.
constexpr int16_t kApiKeyListOffsets                    = 2;   ///< List partition offsets.
constexpr int16_t kApiKeyMetadata                       = 3;   ///< Request cluster / topic metadata.
constexpr int16_t kApiKeyLeaderAndIsr                   = 4;   ///< Replica-leader ISR state (inter-broker).
constexpr int16_t kApiKeyStopReplica                    = 5;   ///< Stop a replica (inter-broker).
constexpr int16_t kApiKeyUpdateMetadata                 = 6;   ///< Push metadata updates (inter-broker).
constexpr int16_t kApiKeyControlledShutdown             = 7;   ///< Graceful broker shutdown (inter-broker).
constexpr int16_t kApiKeyOffsetCommit                   = 8;   ///< Commit consumer offsets.
constexpr int16_t kApiKeyOffsetFetch                    = 9;   ///< Fetch committed consumer offsets.
constexpr int16_t kApiKeyFindCoordinator                = 10;  ///< Locate group / transaction coordinator.
constexpr int16_t kApiKeyJoinGroup                      = 11;  ///< Join a consumer group.
constexpr int16_t kApiKeyHeartbeat                      = 12;  ///< Consumer group heartbeat.
constexpr int16_t kApiKeyLeaveGroup                     = 13;  ///< Leave a consumer group.
constexpr int16_t kApiKeySyncGroup                      = 14;  ///< Synchronise group member assignments.
constexpr int16_t kApiKeyDescribeGroups                 = 15;  ///< Describe consumer groups.
constexpr int16_t kApiKeyListGroups                     = 16;  ///< List all consumer groups.
constexpr int16_t kApiKeySaslHandshake                  = 17;  ///< SASL handshake (mechanism negotiation).
constexpr int16_t kApiKeyApiVersions                    = 18;  ///< Query supported API versions.
constexpr int16_t kApiKeyCreateTopics                   = 19;  ///< Create topics.
constexpr int16_t kApiKeyDeleteTopics                   = 20;  ///< Delete topics.
constexpr int16_t kApiKeyDeleteRecords                  = 21;  ///< Delete records up to an offset.
constexpr int16_t kApiKeyInitProducerId                 = 22;  ///< Initialise transactional producer.
constexpr int16_t kApiKeyOffsetForLeaderEpoch           = 23;  ///< Offset for a given leader epoch.
constexpr int16_t kApiKeyAddPartitionsToTxn             = 24;  ///< Add partitions to a transaction.
constexpr int16_t kApiKeyAddOffsetsToTxn                = 25;  ///< Add consumer offsets to a transaction.
constexpr int16_t kApiKeyEndTxn                         = 26;  ///< Commit or abort a transaction.
constexpr int16_t kApiKeyWriteTxnMarkers               = 27;  ///< Write transaction markers (inter-broker).
constexpr int16_t kApiKeyTxnOffsetCommit                = 28;  ///< Commit offsets inside a transaction.
constexpr int16_t kApiKeyDescribeAcls                   = 29;  ///< Describe ACL entries.
constexpr int16_t kApiKeyCreateAcls                     = 30;  ///< Create ACL entries.
constexpr int16_t kApiKeyDeleteAcls                     = 31;  ///< Delete ACL entries.
constexpr int16_t kApiKeyDescribeConfigs                = 32;  ///< Describe broker / topic configs.
constexpr int16_t kApiKeyAlterConfigs                   = 33;  ///< Alter broker / topic configs.
constexpr int16_t kApiKeyAlterReplicaLogDirs            = 34;  ///< Alter replica log directories (inter-broker).
constexpr int16_t kApiKeyDescribeLogDirs                = 35;  ///< Describe log directories.
constexpr int16_t kApiKeySaslAuthenticate               = 36;  ///< SASL authentication token exchange.
constexpr int16_t kApiKeyCreatePartitions               = 37;  ///< Increase partition count of a topic.
constexpr int16_t kApiKeyCreateDelegationToken          = 38;  ///< Create a delegation token.
constexpr int16_t kApiKeyRenewDelegationToken           = 39;  ///< Renew a delegation token.
constexpr int16_t kApiKeyExpireDelegationToken          = 40;  ///< Expire a delegation token.
constexpr int16_t kApiKeyDescribeDelegationToken        = 41;  ///< Describe delegation tokens.
constexpr int16_t kApiKeyDeleteGroups                   = 42;  ///< Delete consumer groups.
constexpr int16_t kApiKeyElectLeaders                   = 43;  ///< Trigger leader election (inter-broker).
constexpr int16_t kApiKeyIncrementalAlterConfigs        = 44;  ///< Incrementally alter configs.
constexpr int16_t kApiKeyAlterPartitionReassignments    = 45;  ///< Alter partition reassignments.
constexpr int16_t kApiKeyListPartitionReassignments     = 46;  ///< List partition reassignments.
constexpr int16_t kApiKeyOffsetDelete                   = 47;  ///< Delete consumer-group offsets.
constexpr int16_t kApiKeyDescribeClientQuotas           = 48;  ///< Describe client quotas.
constexpr int16_t kApiKeyAlterClientQuotas              = 49;  ///< Alter client quotas.
constexpr int16_t kApiKeyDescribeUserScramCredentials   = 50;  ///< Describe SCRAM credentials.
constexpr int16_t kApiKeyAlterUserScramCredentials      = 51;  ///< Alter SCRAM credentials.

// ============================================================================
// API Key Constants — Torrent-Native (60–67)
// ============================================================================

constexpr int16_t kApiKeyTorrentFetch                   = 60;  ///< Optimised fetch with zero-copy delivery path.
constexpr int16_t kApiKeyTorrentBatchProduce             = 61;  ///< Batch-produce with compression hints.
constexpr int16_t kApiKeyTorrentStreamSubscribe          = 62;  ///< Subscribe to a push-based message stream.
constexpr int16_t kApiKeyTorrentStreamUnsubscribe        = 63;  ///< Unsubscribe from a message stream.
constexpr int16_t kApiKeyTorrentHealthCheck             = 64;  ///< Fast broker-health ping (sub-ms).
constexpr int16_t kApiKeyTorrentClusterState            = 65;  ///< Full cluster metadata snapshot.
constexpr int16_t kApiKeyTorrentSchemaGet               = 66;  ///< Retrieve a registered schema.
constexpr int16_t kApiKeyTorrentSchemaSet               = 67;  ///< Register or update a schema.

// Short aliases (used by tests and Kafka-compatible handlers)
constexpr auto kProduce                        = kApiKeyProduce;
constexpr auto kFetch                          = kApiKeyFetch;
constexpr auto kListOffsets                    = kApiKeyListOffsets;
constexpr auto kMetadata                       = kApiKeyMetadata;
constexpr auto kLeaderAndIsr                   = kApiKeyLeaderAndIsr;
constexpr auto kStopReplica                    = kApiKeyStopReplica;
constexpr auto kUpdateMetadata                 = kApiKeyUpdateMetadata;
constexpr auto kControlledShutdown             = kApiKeyControlledShutdown;
constexpr auto kOffsetCommit                   = kApiKeyOffsetCommit;
constexpr auto kOffsetFetch                    = kApiKeyOffsetFetch;
constexpr auto kFindCoordinator                = kApiKeyFindCoordinator;
constexpr auto kJoinGroup                      = kApiKeyJoinGroup;
constexpr auto kHeartbeat                      = kApiKeyHeartbeat;
constexpr auto kLeaveGroup                     = kApiKeyLeaveGroup;
constexpr auto kSyncGroup                      = kApiKeySyncGroup;
constexpr auto kDescribeGroups                 = kApiKeyDescribeGroups;
constexpr auto kListGroups                     = kApiKeyListGroups;
constexpr auto kSaslHandshake                  = kApiKeySaslHandshake;
constexpr auto kApiVersions                    = kApiKeyApiVersions;
constexpr auto kCreateTopics                   = kApiKeyCreateTopics;
constexpr auto kDeleteTopics                   = kApiKeyDeleteTopics;
constexpr auto kDeleteRecords                  = kApiKeyDeleteRecords;
constexpr auto kInitProducerId                 = kApiKeyInitProducerId;
constexpr auto kOffsetForLeaderEpoch           = kApiKeyOffsetForLeaderEpoch;
constexpr auto kAddPartitionsToTxn             = kApiKeyAddPartitionsToTxn;
constexpr auto kAddOffsetsToTxn                = kApiKeyAddOffsetsToTxn;
constexpr auto kEndTxn                         = kApiKeyEndTxn;
constexpr auto kWriteTxnMarkers                = kApiKeyWriteTxnMarkers;
constexpr auto kTxnOffsetCommit                = kApiKeyTxnOffsetCommit;
constexpr auto kDescribeAcls                   = kApiKeyDescribeAcls;
constexpr auto kCreateAcls                     = kApiKeyCreateAcls;
constexpr auto kDeleteAcls                     = kApiKeyDeleteAcls;
constexpr auto kDescribeConfigs                = kApiKeyDescribeConfigs;
constexpr auto kAlterConfigs                   = kApiKeyAlterConfigs;
constexpr auto kAlterReplicaLogDirs            = kApiKeyAlterReplicaLogDirs;
constexpr auto kDescribeLogDirs                = kApiKeyDescribeLogDirs;
constexpr auto kSaslAuthenticate               = kApiKeySaslAuthenticate;
constexpr auto kCreatePartitions               = kApiKeyCreatePartitions;
constexpr auto kCreateDelegationToken          = kApiKeyCreateDelegationToken;
constexpr auto kRenewDelegationToken           = kApiKeyRenewDelegationToken;
constexpr auto kExpireDelegationToken          = kApiKeyExpireDelegationToken;
constexpr auto kDescribeDelegationToken        = kApiKeyDescribeDelegationToken;
constexpr auto kDeleteGroups                   = kApiKeyDeleteGroups;
constexpr auto kElectLeaders                   = kApiKeyElectLeaders;
constexpr auto kIncrementalAlterConfigs        = kApiKeyIncrementalAlterConfigs;
constexpr auto kAlterPartitionReassignments    = kApiKeyAlterPartitionReassignments;
constexpr auto kListPartitionReassignments     = kApiKeyListPartitionReassignments;
constexpr auto kOffsetDelete                   = kApiKeyOffsetDelete;
constexpr auto kDescribeClientQuotas           = kApiKeyDescribeClientQuotas;
constexpr auto kAlterClientQuotas              = kApiKeyAlterClientQuotas;
constexpr auto kDescribeUserScramCredentials   = kApiKeyDescribeUserScramCredentials;
constexpr auto kAlterUserScramCredentials      = kApiKeyAlterUserScramCredentials;
constexpr auto kTorrentFetch                   = kApiKeyTorrentFetch;
constexpr auto kTorrentBatchProduce            = kApiKeyTorrentBatchProduce;
constexpr auto kTorrentStreamSubscribe         = kApiKeyTorrentStreamSubscribe;
constexpr auto kTorrentStreamUnsubscribe       = kApiKeyTorrentStreamUnsubscribe;
constexpr auto kTorrentHealthCheck             = kApiKeyTorrentHealthCheck;
constexpr auto kTorrentClusterState            = kApiKeyTorrentClusterState;
constexpr auto kTorrentSchemaGet               = kApiKeyTorrentSchemaGet;
constexpr auto kTorrentSchemaSet               = kApiKeyTorrentSchemaSet;

/// Human-readable name for every API key. Returns "UNKNOWN" for unmapped keys.
[[nodiscard]] inline const char* api_key_name(int16_t api_key) noexcept {
    switch (api_key) {
        case kApiKeyProduce:                        return "Produce";
        case kApiKeyFetch:                          return "Fetch";
        case kApiKeyListOffsets:                    return "ListOffsets";
        case kApiKeyMetadata:                       return "Metadata";
        case kApiKeyLeaderAndIsr:                   return "LeaderAndIsr";
        case kApiKeyStopReplica:                    return "StopReplica";
        case kApiKeyUpdateMetadata:                 return "UpdateMetadata";
        case kApiKeyControlledShutdown:             return "ControlledShutdown";
        case kApiKeyOffsetCommit:                   return "OffsetCommit";
        case kApiKeyOffsetFetch:                    return "OffsetFetch";
        case kApiKeyFindCoordinator:                return "FindCoordinator";
        case kApiKeyJoinGroup:                      return "JoinGroup";
        case kApiKeyHeartbeat:                      return "Heartbeat";
        case kApiKeyLeaveGroup:                     return "LeaveGroup";
        case kApiKeySyncGroup:                      return "SyncGroup";
        case kApiKeyDescribeGroups:                 return "DescribeGroups";
        case kApiKeyListGroups:                     return "ListGroups";
        case kApiKeySaslHandshake:                  return "SaslHandshake";
        case kApiKeyApiVersions:                    return "ApiVersions";
        case kApiKeyCreateTopics:                   return "CreateTopics";
        case kApiKeyDeleteTopics:                   return "DeleteTopics";
        case kApiKeyDeleteRecords:                  return "DeleteRecords";
        case kApiKeyInitProducerId:                 return "InitProducerId";
        case kApiKeyOffsetForLeaderEpoch:           return "OffsetForLeaderEpoch";
        case kApiKeyAddPartitionsToTxn:             return "AddPartitionsToTxn";
        case kApiKeyAddOffsetsToTxn:                return "AddOffsetsToTxn";
        case kApiKeyEndTxn:                         return "EndTxn";
        case kApiKeyWriteTxnMarkers:               return "WriteTxnMarkers";
        case kApiKeyTxnOffsetCommit:                return "TxnOffsetCommit";
        case kApiKeyDescribeAcls:                   return "DescribeAcls";
        case kApiKeyCreateAcls:                     return "CreateAcls";
        case kApiKeyDeleteAcls:                     return "DeleteAcls";
        case kApiKeyDescribeConfigs:                return "DescribeConfigs";
        case kApiKeyAlterConfigs:                   return "AlterConfigs";
        case kApiKeyAlterReplicaLogDirs:            return "AlterReplicaLogDirs";
        case kApiKeyDescribeLogDirs:                return "DescribeLogDirs";
        case kApiKeySaslAuthenticate:               return "SaslAuthenticate";
        case kApiKeyCreatePartitions:               return "CreatePartitions";
        case kApiKeyCreateDelegationToken:          return "CreateDelegationToken";
        case kApiKeyRenewDelegationToken:           return "RenewDelegationToken";
        case kApiKeyExpireDelegationToken:          return "ExpireDelegationToken";
        case kApiKeyDescribeDelegationToken:        return "DescribeDelegationToken";
        case kApiKeyDeleteGroups:                   return "DeleteGroups";
        case kApiKeyElectLeaders:                   return "ElectLeaders";
        case kApiKeyIncrementalAlterConfigs:        return "IncrementalAlterConfigs";
        case kApiKeyAlterPartitionReassignments:    return "AlterPartitionReassignments";
        case kApiKeyListPartitionReassignments:     return "ListPartitionReassignments";
        case kApiKeyOffsetDelete:                   return "OffsetDelete";
        case kApiKeyDescribeClientQuotas:           return "DescribeClientQuotas";
        case kApiKeyAlterClientQuotas:              return "AlterClientQuotas";
        case kApiKeyDescribeUserScramCredentials:   return "DescribeUserScramCredentials";
        case kApiKeyAlterUserScramCredentials:      return "AlterUserScramCredentials";
        case kApiKeyTorrentFetch:                   return "TorrentFetch";
        case kApiKeyTorrentBatchProduce:             return "TorrentBatchProduce";
        case kApiKeyTorrentStreamSubscribe:          return "TorrentStreamSubscribe";
        case kApiKeyTorrentStreamUnsubscribe:        return "TorrentStreamUnsubscribe";
        case kApiKeyTorrentHealthCheck:             return "TorrentHealthCheck";
        case kApiKeyTorrentClusterState:            return "TorrentClusterState";
        case kApiKeyTorrentSchemaGet:               return "TorrentSchemaGet";
        case kApiKeyTorrentSchemaSet:               return "TorrentSchemaSet";
        default:                                    return "UNKNOWN";
    }
}

// ============================================================================
// Error Codes
// ============================================================================

/// Wire-protocol error codes.  Zero always means success.
enum class ErrorCode : int16_t {
    /// No error — operation succeeded.
    kNone = 0, NONE = 0,

    /// Unexpected server / system error.
    kUnknownServerError = -1, UNKNOWN_SERVER_ERROR = -1,

    /// No leader exists for this partition.
    kNotLeaderForPartition = 6, NOT_LEADER_FOR_PARTITION = 6,

    /// Message send size too large for the broker.
    kMessageTooLarge = 10, MESSAGE_TOO_LARGE = 10,

    /// The requested offset is out of range.
    kOffsetOutOfRange = 1, OFFSET_OUT_OF_RANGE = 1,

    /// Corrupt or unreadable message on disk.
    kCorruptMessage = 2, CORRUPT_MESSAGE = 2,

    /// Unknown topic or partition.
    kUnknownTopicOrPartition                = 3,

    /// Invalid fetch session epoch.
    kInvalidFetchSessionEpoch               = 5,

    /// Request requires a newer API version.
    kUnsupportedVersion = 35, UNSUPPORTED_VERSION = 35,

    /// The topic already exists.
    kTopicAlreadyExists = 36, TOPIC_ALREADY_EXISTS = 36,

    /// Invalid number of partitions requested.
    kInvalidPartitions                      = 37,

    /// Invalid replication factor.
    kInvalidReplicationFactor               = 38,

    /// Invalid replica assignment.
    kInvalidReplicaAssignment               = 39,

    /// Invalid topic configuration.
    kInvalidConfig                          = 40,

    /// Not controller — this broker is not the cluster controller.
    kNotController = 41, NOT_CONTROLLER = 41,

    /// Invalid request data.
    kInvalidRequest                         = 42,

    /// Network exception during request processing.
    kNetworkException                       = 13,

    /// The coordinator is not available.
    kCoordinatorNotAvailable                = 15,

    /// The coordinator load exceeded its capacity.
    kCoordinatorLoadInProgress              = 14,

    /// Not coordinator for this group.
    kNotCoordinator                         = 16,

    /// Invalid topic exception.
    kInvalidTopicException                  = 17,

    /// Record list too large.
    kRecordListTooLarge                     = 18,

    /// Not enough replicas in-sync.
    kNotEnoughReplicas                      = 19,

    /// Not enough replicas after append.
    kNotEnoughReplicasAfterAppend           = 20,

    /// Invalid required acks value.
    kInvalidRequiredAcks                    = 21,

    /// Illegal SASL state transition.
    kIllegalSaslState                       = 34,

    /// Unknown member id in consumer group.
    kUnknownMemberId                        = 25,

    /// Invalid session timeout.
    kInvalidSessionTimeout                  = 26,

    /// Rebalance in progress for consumer group.
    kRebalanceInProgress                    = 27,

    /// Invalid group id.
    kInvalidGroupId                         = 28,

    /// Unknown consumer group id.
    kUnknownGroupId                         = 29,

    /// SASL authentication failed.
    kSaslAuthenticationFailed = 53, SASL_AUTHENTICATION_FAILED = 53,

    /// Transactional id not found.
    kConcurrentTransactions                 = 51,

    /// Producer attempted an invalid transaction state transition.
    kInvalidProducerEpoch                   = 47,

    /// Producer fenced (another producer with same id started).
    kProducerFenced = 61, PRODUCER_FENCED = 61,

    /// Invalid producer id mapping.
    kInvalidPidMapping                      = 49,

    /// Invalid transaction timeout.
    kInvalidTxnTimeout                      = 50,

    /// Transactional id currently in use.
    kTransactionalIdAuthorizationFailed     = 52,

    /// Operation not attempted (e.g. when a subsequent error supersedes).
    kOperationNotAttempted                  = 55,

    /// Broker has fenced the leader.
    kFencedLeaderEpoch                      = 74,

    /// Policy violation.
    kPolicyViolation                        = 44,

    /// Cluster authorization failed.
    kClusterAuthorizationFailed             = 31,

    /// Delegation token not found.
    kDelegationTokenNotFound                = 70,

    /// Delegation token authorization failed.
    kDelegationTokenAuthorizationFailed     = 71,

    /// Delegation token expired.
    kDelegationTokenExpired                 = 72,

    /// Authentication is required but missing.
    kSecurityDisabled                       = 54,
};

/// Return a human-readable description for \p code.
[[nodiscard]] inline const char* error_code_name(ErrorCode code) noexcept {
    switch (code) {
        case ErrorCode::kNone:                               return "NONE";
        case ErrorCode::kUnknownServerError:                 return "UNKNOWN_SERVER_ERROR";
        case ErrorCode::kNotLeaderForPartition:              return "NOT_LEADER_FOR_PARTITION";
        case ErrorCode::kMessageTooLarge:                    return "MESSAGE_TOO_LARGE";
        case ErrorCode::kOffsetOutOfRange:                   return "OFFSET_OUT_OF_RANGE";
        case ErrorCode::kCorruptMessage:                     return "CORRUPT_MESSAGE";
        case ErrorCode::kUnknownTopicOrPartition:            return "UNKNOWN_TOPIC_OR_PARTITION";
        case ErrorCode::kInvalidFetchSessionEpoch:           return "INVALID_FETCH_SESSION_EPOCH";
        case ErrorCode::kUnsupportedVersion:                 return "UNSUPPORTED_VERSION";
        case ErrorCode::kTopicAlreadyExists:                 return "TOPIC_ALREADY_EXISTS";
        case ErrorCode::kInvalidPartitions:                  return "INVALID_PARTITIONS";
        case ErrorCode::kInvalidReplicationFactor:           return "INVALID_REPLICATION_FACTOR";
        case ErrorCode::kInvalidReplicaAssignment:           return "INVALID_REPLICA_ASSIGNMENT";
        case ErrorCode::kInvalidConfig:                      return "INVALID_CONFIG";
        case ErrorCode::kNotController:                      return "NOT_CONTROLLER";
        case ErrorCode::kInvalidRequest:                     return "INVALID_REQUEST";
        case ErrorCode::kNetworkException:                   return "NETWORK_EXCEPTION";
        case ErrorCode::kCoordinatorNotAvailable:            return "COORDINATOR_NOT_AVAILABLE";
        case ErrorCode::kCoordinatorLoadInProgress:          return "COORDINATOR_LOAD_IN_PROGRESS";
        case ErrorCode::kNotCoordinator:                     return "NOT_COORDINATOR";
        case ErrorCode::kInvalidTopicException:              return "INVALID_TOPIC_EXCEPTION";
        case ErrorCode::kRecordListTooLarge:                 return "RECORD_LIST_TOO_LARGE";
        case ErrorCode::kNotEnoughReplicas:                  return "NOT_ENOUGH_REPLICAS";
        case ErrorCode::kNotEnoughReplicasAfterAppend:       return "NOT_ENOUGH_REPLICAS_AFTER_APPEND";
        case ErrorCode::kInvalidRequiredAcks:                return "INVALID_REQUIRED_ACKS";
        case ErrorCode::kIllegalSaslState:                   return "ILLEGAL_SASL_STATE";
        case ErrorCode::kUnknownMemberId:                    return "UNKNOWN_MEMBER_ID";
        case ErrorCode::kInvalidSessionTimeout:              return "INVALID_SESSION_TIMEOUT";
        case ErrorCode::kRebalanceInProgress:                return "REBALANCE_IN_PROGRESS";
        case ErrorCode::kInvalidGroupId:                     return "INVALID_GROUP_ID";
        case ErrorCode::kUnknownGroupId:                     return "UNKNOWN_GROUP_ID";
        case ErrorCode::kSaslAuthenticationFailed:           return "SASL_AUTHENTICATION_FAILED";
        case ErrorCode::kConcurrentTransactions:             return "CONCURRENT_TRANSACTIONS";
        case ErrorCode::kInvalidProducerEpoch:               return "INVALID_PRODUCER_EPOCH";
        case ErrorCode::kProducerFenced:                     return "PRODUCER_FENCED";
        case ErrorCode::kInvalidPidMapping:                  return "INVALID_PID_MAPPING";
        case ErrorCode::kInvalidTxnTimeout:                  return "INVALID_TXN_TIMEOUT";
        case ErrorCode::kTransactionalIdAuthorizationFailed: return "TRANSACTIONAL_ID_AUTHORIZATION_FAILED";
        case ErrorCode::kOperationNotAttempted:              return "OPERATION_NOT_ATTEMPTED";
        case ErrorCode::kFencedLeaderEpoch:                  return "FENCED_LEADER_EPOCH";
        case ErrorCode::kPolicyViolation:                    return "POLICY_VIOLATION";
        case ErrorCode::kClusterAuthorizationFailed:         return "CLUSTER_AUTHORIZATION_FAILED";
        case ErrorCode::kDelegationTokenNotFound:            return "DELEGATION_TOKEN_NOT_FOUND";
        case ErrorCode::kDelegationTokenAuthorizationFailed: return "DELEGATION_TOKEN_AUTHORIZATION_FAILED";
        case ErrorCode::kDelegationTokenExpired:             return "DELEGATION_TOKEN_EXPIRED";
        case ErrorCode::kSecurityDisabled:                   return "SECURITY_DISABLED";
        default:                                             return "UNKNOWN_ERROR";
    }
}

// ============================================================================
// Primitive Types Used Across the Wire Protocol
// ============================================================================

/// Uniquely identifies a topic.
using TopicName = std::string;

/// Partition index within a topic.
using PartitionIndex = int32_t;

/// A monotonically-increasing offset within a partition.
using Offset = int64_t;

/// Consumer / transactional group identifier.
using GroupId = std::string;

/// Unique identifier assigned to a consumer group member.
using MemberId = std::string;

/// Epoch marker for leader-isolation fences.
using LeaderEpoch = int32_t;

/// Producer epoch used for transaction fencing.
using ProducerEpoch = int16_t;

/// Uniquely identifies a transactional producer.
using ProducerId = int64_t;

/// Byte buffer carried on the wire.
using ByteBuffer = std::vector<uint8_t>;

/// Configuration key-value pair.
struct ConfigEntry {
    std::string name;   ///< Configuration key.
    std::string value;  ///< Configuration value.
};

// ============================================================================
// Common Nested Types
// ============================================================================

/// Identifies a single topic-partition.
struct TopicPartition {
    TopicName       topic;      ///< Topic name.
    PartitionIndex  partition;  ///< Partition index (0-based).
};

/// A record batch header (key-value pair attached to a record batch).
struct RecordHeader {
    std::string key;    ///< Header key (UTF-8).
    ByteBuffer  value;  ///< Header value (opaque bytes).
};

/// A single message record.
struct Record {
    ByteBuffer              key;        ///< Message key (may be empty).
    ByteBuffer              value;      ///< Message payload.
    std::vector<RecordHeader> headers;  ///< Per-record headers.
    int64_t                 timestamp;  ///< Create timestamp (ms since epoch).
};

/// A batch of records destined for a single topic-partition.
struct RecordBatch {
    PartitionIndex      partition;      ///< Target partition.
    std::vector<Record> records;        ///< Records in this batch.
    int8_t              compression;    ///< Compression codec (0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd).
    bool                is_transactional; ///< True if part of a transaction.
};

/// Describes a single API version range supported by the broker.
struct ApiVersionRange {
    int16_t api_key;      ///< API key this range describes.
    int16_t min_version;  ///< Minimum supported version.
    int16_t max_version;  ///< Maximum supported version.
};

/// Topic-partition result with an error code.
struct PartitionResult {
    TopicName       topic;       ///< Topic name.
    PartitionIndex  partition;   ///< Partition index.
    ErrorCode       error_code;  ///< Per-partition error code.
    std::string     error_msg;   ///< Optional error detail.
};

/// Partition-offset pair returned by offset queries.
struct PartitionOffset {
    TopicName       topic;       ///< Topic name.
    PartitionIndex  partition;   ///< Partition index.
    Offset          offset;      ///< Offset value.
    LeaderEpoch     leader_epoch; ///< Leader epoch at this offset.
    ErrorCode       error_code;  ///< Error code for this partition.
};

// ============================================================================
// Request / Response Headers
// ============================================================================

/// Every request frame starts with this fixed-size header (v2 style).
struct RequestHeader {
    int16_t     api_key;        ///< API key identifying the request type.
    int16_t     api_version;    ///< API version for forward/backward compatibility.
    int32_t     correlation_id; ///< Client-supplied correlation id echoed in the response.
    std::string client_id;      ///< Human-readable client identifier.
};

/// Every response frame starts with this fixed-size header.
struct ResponseHeader {
    int32_t     correlation_id; ///< Correlation id from the corresponding request.
};

// ============================================================================
// Request / Response Structs — Kafka-Compatible APIs
// ============================================================================

// --- Produce (api_key=0) ---

struct ProduceRequest {
    std::string                 transaction_id;  ///< Nullable transactional id.
    int16_t                     acks;            ///< Required acknowledgement level.
    int32_t                     timeout_ms;      ///< Broker-side timeout in milliseconds.
    std::vector<RecordBatch>    batches;         ///< Record batches to produce.
};

struct ProduceResponse {
    std::vector<PartitionResult> results;  ///< Per-partition produce results.
    int32_t                      throttle_time_ms; ///< Broker throttle time in ms.
};

// --- Fetch (api_key=1) ---

struct FetchRequest {
    int32_t                         max_wait_ms;    ///< Max time to block waiting for data.
    int32_t                         min_bytes;      ///< Min bytes to accumulate before responding.
    int32_t                         max_bytes;      ///< Max bytes per partition in response.
    int8_t                          isolation_level;///< 0=read_uncommitted, 1=read_committed.
    int32_t                         session_id;     ///< Fetch session id (0 = no session).
    int32_t                         session_epoch;  ///< Fetch session epoch.
    std::vector<TopicPartition>     partitions;     ///< Partitions to fetch from.
    std::vector<Offset>             fetch_offsets;  ///< Offset per partition (1:1 with \p partitions).
};

struct FetchResponse {
    int32_t                                 throttle_time_ms; ///< Broker throttle time.
    ErrorCode                               error_code;       ///< Top-level error.
    int32_t                                 session_id;       ///< Fetch session id echo.
    std::vector<std::vector<Record>>        records;          ///< Returned record batches per partition.
    std::vector<PartitionResult>            results;          ///< Per-partition results.
};

// --- ListOffsets (api_key=2) ---

struct ListOffsetsRequest {
    int32_t                         replica_id;     ///< Replica id (-1 = consumer).
    int8_t                          isolation_level;///< Isolation level for offset queries.
    std::vector<TopicPartition>     partitions;     ///< Partitions to query.
    std::vector<int64_t>            timestamps;     ///< Timestamp per partition (-2=earliest, -1=latest).
};

struct ListOffsetsResponse {
    int32_t                             throttle_time_ms;
    std::vector<PartitionOffset>        offsets; ///< Resolved offsets.
};

// --- Metadata (api_key=3) ---

struct MetadataRequest {
    std::vector<TopicName>  topics;             ///< Topics to describe (empty = all).
    bool                    allow_auto_create;  ///< Allow auto-creation of topics.
    bool                    include_cluster_authorized_operations; ///< Include authz ops.
    bool                    include_topic_authorized_operations;   ///< Include authz ops.
};

struct MetadataResponse {
    int32_t                             throttle_time_ms;
    std::vector<std::string>            brokers;          ///< Broker node descriptors.
    std::vector<TopicName>              topics;           ///< Described topics.
    std::vector<int32_t>                partition_counts; ///< Partition count per topic.
    ErrorCode                           error_code;       ///< Top-level error.
};

// --- OffsetCommit (api_key=8) ---

struct OffsetCommitRequest {
    GroupId                         group_id;       ///< Consumer group id.
    int32_t                         generation_id;  ///< Group generation id.
    MemberId                        member_id;      ///< Member id.
    std::string                     group_instance_id; ///< Optional static group instance id.
    int64_t                         retention_ms;   ///< Offset retention period.
    std::vector<TopicPartition>     partitions;     ///< Partitions to commit.
    std::vector<Offset>             offsets;        ///< Offsets to commit.
    std::vector<std::string>        metadata;       ///< Commit metadata per partition.
};

struct OffsetCommitResponse {
    int32_t                             throttle_time_ms;
    std::vector<PartitionResult>        results; ///< Commit results.
};

// --- OffsetFetch (api_key=9) ---

struct OffsetFetchRequest {
    GroupId                         group_id;   ///< Consumer group id.
    std::vector<TopicPartition>     partitions; ///< Partitions to query (empty = all).
};

struct OffsetFetchResponse {
    int32_t                             throttle_time_ms;
    ErrorCode                           error_code;
    std::vector<PartitionOffset>        offsets; ///< Fetched offsets.
};

// --- FindCoordinator (api_key=10) ---

struct FindCoordinatorRequest {
    std::string key;    ///< Group id or transactional id.
    int8_t      type;   ///< 0=group coordinator, 1=transaction coordinator.
};

struct FindCoordinatorResponse {
    int32_t     throttle_time_ms;
    ErrorCode   error_code;
    int32_t     node_id;    ///< Broker node id of the coordinator.
    std::string host;       ///< Coordinator hostname.
    int32_t     port;       ///< Coordinator port.
};

// --- JoinGroup (api_key=11) ---

struct JoinGroupRequest {
    GroupId                     group_id;           ///< Consumer group id.
    int32_t                     session_timeout_ms; ///< Consumer session timeout.
    int32_t                     rebalance_timeout_ms;///< Max rebalance duration.
    MemberId                    member_id;          ///< Member id (empty on first join).
    std::string                 group_instance_id;  ///< Optional static group instance.
    std::string                 protocol_type;      ///< Group protocol type (e.g. "consumer").
    std::vector<std::string>    protocols;          ///< Supported group protocols.
    std::vector<ByteBuffer>     protocol_metadata;  ///< Metadata for each protocol.
};

struct JoinGroupResponse {
    int32_t     throttle_time_ms;
    ErrorCode   error_code;
    int32_t     generation_id;  ///< Group generation id.
    std::string protocol_name;  ///< Elected protocol name.
    MemberId    leader_id;      ///< Member id of the group leader.
    MemberId    member_id;      ///< Member id assigned to this member.
    std::vector<MemberId> members; ///< All members in the group.
};

// --- Heartbeat (api_key=12) ---

struct HeartbeatRequest {
    GroupId     group_id;       ///< Consumer group id.
    int32_t     generation_id;  ///< Current generation id.
    MemberId    member_id;      ///< Member id.
    std::string group_instance_id; ///< Optional static group instance.
};

struct HeartbeatResponse {
    int32_t     throttle_time_ms;
    ErrorCode   error_code;
};

// --- LeaveGroup (api_key=13) ---

struct LeaveGroupRequest {
    GroupId     group_id;   ///< Consumer group id.
    MemberId    member_id;  ///< Member id of the leaver.
    std::vector<MemberId> members; ///< Members to remove.
};

struct LeaveGroupResponse {
    int32_t     throttle_time_ms;
    ErrorCode   error_code;
};

// --- SyncGroup (api_key=14) ---

struct SyncGroupRequest {
    GroupId                     group_id;       ///< Consumer group id.
    int32_t                     generation_id;  ///< Current generation id.
    MemberId                    member_id;      ///< Member id.
    std::string                 group_instance_id; ///< Optional static group instance.
    std::string                 protocol_type;  ///< Group protocol type.
    std::string                 protocol_name;  ///< Elected protocol name.
    std::vector<ByteBuffer>     assignments;    ///< Assignment payload per member.
};

struct SyncGroupResponse {
    int32_t     throttle_time_ms;
    ErrorCode   error_code;
    std::string protocol_type;  ///< Group protocol type.
    std::string protocol_name;  ///< Elected protocol name.
    ByteBuffer  assignment;     ///< Assignment for this member.
};

// --- DescribeGroups (api_key=15) ---

struct DescribeGroupsRequest {
    std::vector<GroupId> group_ids;     ///< Groups to describe.
    bool                 include_authorized_operations; ///< Include authz ops.
};

struct DescribeGroupsResponse {
    int32_t     throttle_time_ms;
    /// A textual summary per group (compact transport).
    std::vector<std::string> summaries;
};

// --- ListGroups (api_key=16) ---

struct ListGroupsRequest {
    /// States to filter by (empty = all).
    std::vector<std::string> states_filter;
};

struct ListGroupsResponse {
    int32_t                     throttle_time_ms;
    ErrorCode                   error_code;
    std::vector<GroupId>        groups; ///< List of group ids.
};

// --- SaslHandshake (api_key=17) ---

struct SaslHandshakeRequest {
    std::string mechanism;  ///< SASL mechanism name (e.g. PLAIN, SCRAM-SHA-256).
};

struct SaslHandshakeResponse {
    ErrorCode                   error_code;
    std::vector<std::string>    enabled_mechanisms; ///< Mechanisms supported by the broker.
};

// --- ApiVersions (api_key=18) ---

struct ApiVersionsRequest {
    std::string client_software_name;   ///< Human-readable client name.
    std::string client_software_version;///< Human-readable client version.
};

struct ApiVersionsResponse {
    ErrorCode                       error_code;
    std::vector<ApiVersionRange>    api_keys; ///< Ranges for every supported API key.
    int32_t                         throttle_time_ms;
};

// --- CreateTopics (api_key=19) ---

struct CreateTopicsRequest {
    std::vector<TopicName>  topics;             ///< Topics to create.
    std::vector<int32_t>    num_partitions;     ///< Partition count per topic.
    std::vector<int16_t>    replication_factors;///< Replication factor per topic.
    std::vector<std::vector<ConfigEntry>> configs; ///< Per-topic configs.
    int32_t                 timeout_ms;         ///< Operation timeout.
};

struct CreateTopicsResponse {
    int32_t                             throttle_time_ms;
    std::vector<PartitionResult>        results; ///< Per-topic creation results.
};

// --- DeleteTopics (api_key=20) ---

struct DeleteTopicsRequest {
    std::vector<TopicName>  topics;     ///< Topics to delete.
    int32_t                 timeout_ms; ///< Operation timeout.
};

struct DeleteTopicsResponse {
    int32_t                             throttle_time_ms;
    std::vector<PartitionResult>        results;
};

// --- DeleteRecords (api_key=21) ---

struct DeleteRecordsRequest {
    std::vector<TopicPartition>     partitions;     ///< Partitions to delete from.
    std::vector<Offset>             before_offsets; ///< Delete records before these offsets.
    int32_t                         timeout_ms;
};

struct DeleteRecordsResponse {
    int32_t                             throttle_time_ms;
    std::vector<PartitionResult>        results;
};

// --- InitProducerId (api_key=22) ---

struct InitProducerIdRequest {
    std::string transaction_id;  ///< Nullable transactional id.
    int32_t     transaction_timeout_ms; ///< Transaction timeout.
    ProducerId  producer_id;     ///< Current producer id (-1 if none).
    ProducerEpoch producer_epoch;///< Current producer epoch.
};

struct InitProducerIdResponse {
    int32_t         throttle_time_ms;
    ErrorCode       error_code;
    ProducerId      producer_id;    ///< Assigned producer id.
    ProducerEpoch   producer_epoch; ///< Assigned producer epoch.
};

// --- OffsetForLeaderEpoch (api_key=23) ---

struct OffsetForLeaderEpochRequest {
    int32_t                             replica_id;
    std::vector<TopicPartition>         partitions;
    std::vector<LeaderEpoch>            leader_epochs;
};

struct OffsetForLeaderEpochResponse {
    int32_t                         throttle_time_ms;
    std::vector<PartitionOffset>    offsets;
};

// --- AddPartitionsToTxn (api_key=24) ---

struct AddPartitionsToTxnRequest {
    std::string                     transaction_id;
    ProducerId                      producer_id;
    ProducerEpoch                   producer_epoch;
    std::vector<TopicPartition>     partitions;
};

struct AddPartitionsToTxnResponse {
    int32_t                         throttle_time_ms;
    std::vector<PartitionResult>    results;
};

// --- AddOffsetsToTxn (api_key=25) ---

struct AddOffsetsToTxnRequest {
    std::string                     transaction_id;
    ProducerId                      producer_id;
    ProducerEpoch                   producer_epoch;
    GroupId                         group_id;
};

struct AddOffsetsToTxnResponse {
    int32_t     throttle_time_ms;
    ErrorCode   error_code;
};

// --- EndTxn (api_key=26) ---

struct EndTxnRequest {
    std::string     transaction_id;
    ProducerId      producer_id;
    ProducerEpoch   producer_epoch;
    bool            committed; ///< true=commit, false=abort.
};

struct EndTxnResponse {
    int32_t     throttle_time_ms;
    ErrorCode   error_code;
};

// --- WriteTxnMarkers (api_key=27) ---

struct WriteTxnMarkersRequest {
    std::vector<ProducerId>     producer_ids;
    std::vector<ProducerEpoch>  producer_epochs;
    std::vector<bool>           transaction_results; ///< per-txn result.
    std::vector<TopicPartition> partitions;
};

struct WriteTxnMarkersResponse {
    std::vector<PartitionResult> results;
};

// --- TxnOffsetCommit (api_key=28) ---

struct TxnOffsetCommitRequest {
    std::string                     transaction_id;
    GroupId                         group_id;
    ProducerId                      producer_id;
    ProducerEpoch                   producer_epoch;
    int32_t                         generation_id;
    MemberId                        member_id;
    std::vector<TopicPartition>     partitions;
    std::vector<Offset>             offsets;
};

struct TxnOffsetCommitResponse {
    int32_t                         throttle_time_ms;
    std::vector<PartitionResult>    results;
};

// --- DescribeAcls (api_key=29) ---

struct DescribeAclsRequest {
    int8_t  resource_type;  ///< Resource type filter.
    std::string resource_name_filter; ///< Resource name pattern (empty = any).
    std::string principal_filter;     ///< Principal filter (empty = any).
    std::string host_filter;          ///< Host filter (empty = any).
    int8_t  operation;      ///< Operation filter.
    int8_t  permission_type;///< Permission type filter.
};

struct DescribeAclsResponse {
    int32_t     throttle_time_ms;
    ErrorCode   error_code;
    /// Serialised ACL description entries (compact representation).
    std::vector<std::string> acl_entries;
};

// --- CreateAcls (api_key=30) ---

struct CreateAclsRequest {
    /// Serialised ACL creation entries.
    std::vector<std::string> acl_creations;
};

struct CreateAclsResponse {
    int32_t     throttle_time_ms;
    /// Per-ACL creation results.
    std::vector<PartitionResult> results;
};

// --- DeleteAcls (api_key=31) ---

struct DeleteAclsRequest {
    /// Serialised ACL filter entries.
    std::vector<std::string> acl_filters;
};

struct DeleteAclsResponse {
    int32_t     throttle_time_ms;
    std::vector<PartitionResult> results;
};

// --- DescribeConfigs (api_key=32) ---

struct DescribeConfigsRequest {
    std::vector<int32_t>    resource_types;  ///< Resource type per entry (0=unknown,2=topic,4=broker).
    std::vector<std::string> resource_names; ///< Resource name per entry.
    std::vector<std::string> config_keys;    ///< Specific config keys (empty = all).
    bool                     include_synonyms;    ///< Include config synonyms.
    bool                     include_documentation; ///< Include config docs.
};

struct DescribeConfigsResponse {
    int32_t                             throttle_time_ms;
    std::vector<std::vector<ConfigEntry>> entries; ///< Per-resource config entries.
};

// --- AlterConfigs (api_key=33) ---

struct AlterConfigsRequest {
    std::vector<int32_t>                        resource_types;
    std::vector<std::string>                    resource_names;
    std::vector<std::vector<ConfigEntry>>       configs; ///< Configs to set per resource.
    bool                                        validate_only; ///< true = dry-run.
};

struct AlterConfigsResponse {
    int32_t                         throttle_time_ms;
    std::vector<PartitionResult>    results;
};

// --- AlterReplicaLogDirs (api_key=34) ---

struct AlterReplicaLogDirsRequest {
    std::vector<std::string> log_dirs;
    std::vector<TopicPartition> partitions;
};

struct AlterReplicaLogDirsResponse {
    int32_t                         throttle_time_ms;
    std::vector<PartitionResult>    results;
};

// --- DescribeLogDirs (api_key=35) ---

struct DescribeLogDirsRequest {
    std::vector<TopicPartition> partitions; ///< Empty = all.
};

struct DescribeLogDirsResponse {
    int32_t     throttle_time_ms;
    std::vector<std::string> log_dir_entries; ///< Compact log-dir descriptions.
};

// --- SaslAuthenticate (api_key=36) ---

struct SaslAuthenticateRequest {
    ByteBuffer sasl_auth_bytes; ///< Opaque SASL authentication payload.
};

struct SaslAuthenticateResponse {
    ErrorCode   error_code;
    std::string error_message;
    ByteBuffer  sasl_auth_bytes; ///< Opaque SASL response payload.
    int32_t     session_lifetime_ms; ///< Remaining session lifetime.
};

// --- CreatePartitions (api_key=37) ---

struct CreatePartitionsRequest {
    std::vector<TopicName>  topics;
    std::vector<int32_t>    new_partition_counts; ///< New total partition count.
    std::vector<std::vector<std::vector<int32_t>>> new_assignments; ///< New replica assignments.
    int32_t                 timeout_ms;
    bool                    validate_only;
};

struct CreatePartitionsResponse {
    int32_t                         throttle_time_ms;
    std::vector<PartitionResult>    results;
};

// --- Delegation Token APIs (api_key=38–41) ---

struct CreateDelegationTokenRequest {
    std::vector<std::string> renewers;  ///< Principals allowed to renew.
    int64_t                  max_lifetime_ms; ///< Max token lifetime.
};

struct CreateDelegationTokenResponse {
    int32_t     throttle_time_ms;
    ErrorCode   error_code;
    std::string token_id;   ///< Token identifier.
    std::string hmac;       ///< HMAC of the token.
    int64_t     expiry_ms;  ///< Expiry timestamp (ms since epoch).
};

struct RenewDelegationTokenRequest {
    std::string token_id;   ///< Token to renew.
    int64_t     renew_period_ms; ///< Renewal duration.
};

struct RenewDelegationTokenResponse {
    int32_t     throttle_time_ms;
    ErrorCode   error_code;
    int64_t     expiry_ms;  ///< New expiry timestamp.
};

struct ExpireDelegationTokenRequest {
    std::string token_id;   ///< Token to expire.
    int64_t     expiry_ms;  ///< Expiry timestamp to set.
};

struct ExpireDelegationTokenResponse {
    int32_t     throttle_time_ms;
    ErrorCode   error_code;
    int64_t     expiry_ms;  ///< Effective expiry timestamp.
};

struct DescribeDelegationTokenRequest {
    std::vector<std::string> owners; ///< Filter by owner principal (empty = all).
};

struct DescribeDelegationTokenResponse {
    int32_t     throttle_time_ms;
    ErrorCode   error_code;
    std::vector<std::string> token_descriptions; ///< Compact per-token descriptions.
};

// --- DeleteGroups (api_key=42) ---

struct DeleteGroupsRequest {
    std::vector<GroupId> group_ids; ///< Groups to delete.
};

struct DeleteGroupsResponse {
    int32_t                         throttle_time_ms;
    std::vector<PartitionResult>    results;
};

// --- ElectLeaders (api_key=43) ---

struct ElectLeadersRequest {
    int8_t                          election_type; ///< 0=preferred, 1=unclean.
    std::vector<TopicPartition>    partitions;     ///< Empty = all partitions.
    int32_t                         timeout_ms;
};

struct ElectLeadersResponse {
    int32_t                         throttle_time_ms;
    std::vector<PartitionResult>    results;
};

// --- IncrementalAlterConfigs (api_key=44) ---

struct IncrementalAlterConfigsRequest {
    std::vector<int32_t>                    resource_types;
    std::vector<std::string>                resource_names;
    /// Per-resource config deltas. Each entry is name, op (0=set,1=delete,2=append,3=subtract), value.
    std::vector<std::vector<std::tuple<std::string,int8_t,std::string>>> deltas;
    bool                                    validate_only;
};

struct IncrementalAlterConfigsResponse {
    int32_t                         throttle_time_ms;
    std::vector<PartitionResult>    results;
};

// --- AlterPartitionReassignments (api_key=45) ---

struct AlterPartitionReassignmentsRequest {
    int32_t                         timeout_ms;
    std::vector<TopicPartition>     partitions;
    std::vector<std::vector<int32_t>> new_replicas; ///< New replica assignments.
};

struct AlterPartitionReassignmentsResponse {
    int32_t                         throttle_time_ms;
    ErrorCode                       error_code;
    std::vector<PartitionResult>    results;
};

// --- ListPartitionReassignments (api_key=46) ---

struct ListPartitionReassignmentsRequest {
    int32_t                         timeout_ms;
    std::vector<TopicPartition>     partitions; ///< Empty = all.
};

struct ListPartitionReassignmentsResponse {
    int32_t                         throttle_time_ms;
    ErrorCode                       error_code;
    std::vector<std::string>        reassignment_entries; ///< Compact descriptions.
};

// --- OffsetDelete (api_key=47) ---

struct OffsetDeleteRequest {
    GroupId                         group_id;
    std::vector<TopicPartition>     partitions;
};

struct OffsetDeleteResponse {
    int32_t                         throttle_time_ms;
    ErrorCode                       error_code;
    std::vector<PartitionResult>    results;
};

// --- DescribeClientQuotas (api_key=48) ---

struct DescribeClientQuotasRequest {
    std::vector<std::string> components; ///< Quota entity filters.
    bool                     strict;     ///< Strict matching flag.
};

struct DescribeClientQuotasResponse {
    int32_t                         throttle_time_ms;
    ErrorCode                       error_code;
    std::vector<std::string>        quota_entries; ///< Compact quota descriptions.
};

// --- AlterClientQuotas (api_key=49) ---

struct AlterClientQuotasRequest {
    std::vector<std::string> quota_entries; ///< Quota alterations.
    bool                     validate_only;
};

struct AlterClientQuotasResponse {
    int32_t                         throttle_time_ms;
    std::vector<PartitionResult>    results;
};

// --- DescribeUserScramCredentials (api_key=50) ---

struct DescribeUserScramCredentialsRequest {
    std::vector<std::string> users; ///< Users to describe (empty = all).
};

struct DescribeUserScramCredentialsResponse {
    int32_t                         throttle_time_ms;
    ErrorCode                       error_code;
    std::vector<std::string>        credential_entries; ///< Compact SCRAM descriptions.
};

// --- AlterUserScramCredentials (api_key=51) ---

struct AlterUserScramCredentialsRequest {
    /// SCRAM credential alterations (serialised per-user entries).
    std::vector<std::string> credential_upserts;
    std::vector<std::string> credential_deletions;
};

struct AlterUserScramCredentialsResponse {
    int32_t                         throttle_time_ms;
    std::vector<PartitionResult>    results;
};

// ============================================================================
// Request / Response Structs — Torrent-Native APIs (api_key=60–67)
// ============================================================================

// --- TorrentFetch (api_key=60) ---

/// Optimised zero-copy fetch request. Uses shared-memory hints to avoid
/// kernel-space copies on the broker response path.
struct TorrentFetchRequest {
    std::vector<TopicPartition>     partitions;          ///< Partitions to fetch from.
    std::vector<Offset>             start_offsets;       ///< Starting offset per partition.
    int32_t                         max_bytes;           ///< Max response size in bytes.
    int32_t                         max_wait_ms;         ///< Max blocking wait time.
    uint64_t                        shared_memory_key;   ///< SHM key for zero-copy delivery (0 = none).
    uint32_t                        shared_memory_size;  ///< SHM region size in bytes.
};

struct TorrentFetchResponse {
    int32_t                                 throttle_time_ms;
    ErrorCode                               error_code;
    std::vector<std::vector<Record>>        record_batches;      ///< Fetched records per partition.
    std::vector<PartitionResult>            results;             ///< Per-partition results.
    std::vector<Offset>                     high_watermarks;     ///< HWMs per partition.
    bool                                    zero_copy_used;      ///< True if SHM delivery succeeded.
};

// --- TorrentBatchProduce (api_key=61) ---

/// Batch-produce request with per-batch compression hints and optional
/// flush-before-write ordering guarantees.
struct TorrentBatchProduceRequest {
    int16_t                     acks;               ///< Required acknowledgements.
    int32_t                     timeout_ms;         ///< Operation timeout.
    std::vector<RecordBatch>    batches;            ///< Batches to produce.
    int8_t                      compression_hint;   ///< Preferred compression codec for response.
    bool                        flush_before;       ///< Flush partition log before appending.
    std::string                 transaction_id;     ///< Nullable transactional id.
};

struct TorrentBatchProduceResponse {
    int32_t                         throttle_time_ms;
    std::vector<PartitionResult>    results;        ///< Per-partition results.
    std::vector<Offset>             base_offsets;   ///< Base offset of each batch after append.
};

// --- TorrentStreamSubscribe (api_key=62) ---

/// Subscribe to a real-time, push-based message stream. The broker will
/// push records to the client over the established connection.
struct TorrentStreamSubscribeRequest {
    std::vector<TopicPartition> partitions;     ///< Partitions to subscribe to.
    std::vector<Offset>         start_offsets;  ///< Offsets to start streaming from.
    int32_t                     max_unacked;    ///< Max unacknowledged messages before backpressure.
    int32_t                     heartbeat_ms;   ///< Stream heartbeat interval.
};

struct TorrentStreamSubscribeResponse {
    int32_t     throttle_time_ms;
    ErrorCode   error_code;
    std::string stream_id; ///< Unique stream identifier for subsequent unsubscribe.
};

// --- TorrentStreamUnsubscribe (api_key=63) ---

struct TorrentStreamUnsubscribeRequest {
    std::string stream_id; ///< Stream identifier from subscribe response.
};

struct TorrentStreamUnsubscribeResponse {
    int32_t     throttle_time_ms;
    ErrorCode   error_code;
};

// --- TorrentHealthCheck (api_key=64) ---

/// Lightweight broker-health ping. Designed for sub-millisecond response
/// on the fast-path, bypassing the normal request dispatcher.
struct TorrentHealthCheckRequest {
    int64_t client_timestamp_ns; ///< Client-side monotonic timestamp for RTT calculation.
};

struct TorrentHealthCheckResponse {
    int64_t client_timestamp_ns; ///< Echo of client timestamp.
    int64_t server_timestamp_ns; ///< Server-side monotonic timestamp.
    bool    is_ready;            ///< True if broker is accepting traffic.
    int32_t active_connections;  ///< Current connection count on the broker.
};

// --- TorrentClusterState (api_key=65) ---

/// Request a full snapshot of cluster metadata, including broker topology,
/// partition leadership, ISR state, and controller epoch.
struct TorrentClusterStateRequest {
    bool include_topic_configs;     ///< Include per-topic configuration.
    bool include_consumer_groups;   ///< Include consumer group state.
    bool include_quota_state;       ///< Include client quota information.
};

struct TorrentClusterStateResponse {
    int32_t                     controller_id;      ///< Current controller broker id.
    int32_t                     controller_epoch;    ///< Controller epoch.
    std::vector<std::string>    broker_nodes;        ///< Broker endpoint descriptors.
    std::vector<std::string>    topic_metadata;      ///< Topic metadata entries.
    std::vector<std::string>    group_states;        ///< Consumer group states (if requested).
    std::vector<std::string>    quota_states;        ///< Quota states (if requested).
};

// --- TorrentSchemaGet (api_key=66) ---

/// Retrieve a registered schema by subject name and optional version.
struct TorrentSchemaGetRequest {
    std::string subject;    ///< Schema subject name.
    int32_t     version;    ///< Schema version (-1 = latest).
};

struct TorrentSchemaGetResponse {
    ErrorCode   error_code;
    int32_t     schema_id;      ///< Global schema id.
    int32_t     version;        ///< Resolved version.
    std::string schema_type;    ///< Schema format (AVRO, PROTOBUF, JSON_SCHEMA).
    std::string schema_text;    ///< Schema definition text.
};

// --- TorrentSchemaSet (api_key=67) ---

/// Register a new schema version or update an existing subject.
struct TorrentSchemaSetRequest {
    std::string subject;        ///< Schema subject name.
    std::string schema_type;    ///< Schema format.
    std::string schema_text;    ///< Schema definition text.
    bool        allow_incompatible; ///< Allow incompatible schema evolution.
};

struct TorrentSchemaSetResponse {
    ErrorCode   error_code;
    int32_t     schema_id;  ///< Assigned global schema id.
    int32_t     version;    ///< Assigned version number.
};

// ============================================================================
// Wire-Frame Transport Helpers
// ============================================================================

/// Return the minimum buffer size needed to hold a length-prefixed frame
/// whose payload is \p payload_size bytes.
[[nodiscard]] constexpr uint32_t frame_total_size(uint32_t payload_size) noexcept {
    return kFrameLengthSize + payload_size;
}

/// Validate that \p payload_size does not exceed the maximum allowed value.
/// Returns true when the payload is within bounds.
[[nodiscard]] constexpr bool is_valid_payload_size(uint32_t payload_size) noexcept {
    return payload_size <= kMaxFramePayloadSize;
}

/// Check whether \p api_key lies within the torrent-native range (60–67).
[[nodiscard]] constexpr bool is_torrent_native_api(int16_t api_key) noexcept {
    return api_key >= 60 && api_key <= 67;
}

/// Check whether \p api_key is a recognised Kafka-compatible API (0–51).
[[nodiscard]] constexpr bool is_kafka_api(int16_t api_key) noexcept {
    return api_key >= 0 && api_key <= 51;
}

} // namespace torrent::protocol
