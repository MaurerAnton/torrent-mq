# torrent-mq — High-Performance Distributed Message Queue

## Overview

torrent-mq is a C++20 distributed messaging platform designed for microsecond
latency, disk-backed persistence with exactly-once guarantees, and horizontal
scalability via Raft-based consensus. It provides Kafka API compatibility while
offering a native high-performance protocol for reduced overhead.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        torrent-mq Broker                        │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │  Admin   │  │ Metrics  │  │  Schema  │  │   CLI    │       │
│  │  REST    │  │Prometheus│  │ Registry │  │torrentctl│       │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                   Request Dispatcher                      │   │
│  │    Produce | Fetch | Offset | Metadata | Group | Admin    │   │
│  └──────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────┐   │
│  │   Topic     │  │  Partition   │  │  Consumer Group      │   │
│  │  Manager    │  │  Manager     │  │  Coordinator         │   │
│  └─────────────┘  └──────────────┘  └──────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                     Storage Engine                        │   │
│  │  Segment → LogManager → Compaction → Tiered Storage      │   │
│  │  PageCache | BatchCache | Snapshots | Encryption         │   │
│  └──────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌────────────────────────────────────┐   │
│  │  Raft Consensus │  │         Network Layer              │   │
│  │  Election       │  │  TCP | TLS 1.3 | gRPC             │   │
│  │  Replication    │  │  Flow Control | Throttle           │   │
│  │  Snapshot       │  │  Connection Pool | Limiter         │   │
│  └─────────────────┘  └────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Subsystems

### Storage Engine (17 files, ~18K lines)
- **Segment**: Append-only log segment with CRC32C validation, mmap support,
  sparse index, compression (none/gzip/snappy/lz4/zstd), atomic rollover
- **LogManager**: Partition replica lifecycle — segment management, append/read,
  compaction, retention, snapshots, tiered storage archival
- **Compaction**: Key-based deduplication with custom combiner functions,
  dirty ratio detection, segment merge, atomic replacement
- **Recovery**: Crash recovery with segment validation, header CRC check,
  batch-level CRC scanning, last-valid-batch truncation
- **Cleaner**: Time-based and size-based retention with priority scoring
- **PageCache**: LRU page cache for disk reads with mmap integration
- **BatchCache**: LRU RecordBatch cache keyed by (topic, partition, offset)
- **TieredStorage**: S3 archival/restore with compression and lifecycle policies
- **Snapshots**: Consistent point-in-time snapshots for Raft state transfer
- **Encryption**: AES-256-GCM at-rest encryption with key rotation
- **DiskIO**: Direct I/O with alignment, fallocate, EINTR retry, buffer management
- **Indexes**: OffsetIndex (binary search), TimeIndex (timestamp lookup),
  SegmentIndex (sparse index management)
- **RocksDB/FileStore**: Persistent metadata backends

### Network Layer (9 files, ~9K lines)
- **TcpTransport**: Non-blocking TCP with cork/nodelay/keepalive,
  dual-stack IPv6, SO_REUSEADDR/SO_REUSEPORT
- **TlsTransport**: TLS 1.3 via OpenSSL with mTLS, ALPN, session caching,
  cipher suite configuration
- **Acceptor**: Non-blocking accept loop with accept4, configurable backlog
- **Connection**: Async I/O, 4-byte length-prefix framing, SASL/TLS handshake,
  request dispatching, response queuing, token-bucket throttling, idle timeout
- **ConnectionPool**: Thread-safe connection lifecycle management
- **gRPC Server**: Inter-broker gRPC with Raft RPC service
- **Protocol**: 60 API keys (52 Kafka-compatible + 8 torrent-native),
  big-endian binary serialization, varint/ZigZag encoding
- **FlowControl**: Credit-based flow control with atomic operations
- **ConnectionLimiter**: Global connection count limiting

### Raft Consensus (8 files, ~9K lines)
- **RaftNode**: Full Raft protocol — leader election with pre-vote,
  log replication with pipeline/batch, snapshotting with chunked transfer,
  membership changes with joint consensus, leader leases, fencing
- **RaftLog**: Persistent append-only log with CRC per entry batch
- **RaftRpc**: Peer-to-peer RPC with connection pooling and exponential backoff
- **RaftSnapshot**: Chunked snapshot creation/installation with CRC32C
- **RaftElection**: Randomized timeout 150-300ms, pre-vote, split vote resolution
- **RaftHeartbeat**: Periodic heartbeat loop, pipeline replication, leader lease
- **RaftMembership**: Joint consensus for add/remove node operations
- **MetadataManager**: Cluster metadata in Raft — topics, brokers, partitions

### Broker Core (13 files, ~14K lines)
- **Server**: 18-step startup sequence, graceful shutdown with drain,
  health probe with degradation detection
- **TopicManager**: Topic CRUD with auto-creation, metadata caching
- **PartitionManager**: Partition lifecycle with leader/ISR/replica management
- **ConsumerGroupManager**: Full state machine (Empty/Stable/PreparingRebalance/
  CompletingRebalance/Dead), Range/RoundRobin/Sticky assignment strategies
- **RequestDispatcher**: API key routing, thread pool, backpressure,
  version negotiation, rate limiting
- **InterBroker**: Peer-to-peer communication, leader/ISR propagation,
  metadata sync, circuit breaker
- **Controller**: Cluster controller with rack-aware partition assignment,
  failover detection, epoch fencing
- **TransactionCoordinator**: Exactly-once semantics — producer ID assignment,
  transaction lifecycle, control markers
- **QuotaManager**: Per-client token bucket for produce/fetch/request rate
- **RetentionManager**: Periodic time/size retention enforcement
- **CompactionManager**: Priority-based compaction scheduling
- **LeaderBalancer**: Rack-aware leader rebalancing with throttling
- **DLM**: Distributed lock manager via Raft with fencing tokens

### Client API (7 files, ~12K lines)
- **ProduceHandler**: Kafka v2 wire format parsing, CRC validation,
  topic validation, quota enforcement, transactional produce
- **FetchHandler**: Offset-based fetch with isolation levels,
  fetch sessions, long-poll, read-committed support
- **OffsetHandler**: ListOffsets (earliest/latest/by-timestamp),
  OffsetCommit/Fetch, OffsetForLeaderEpoch, OffsetDelete
- **MetadataHandler**: Metadata, DescribeConfigs, AlterConfigs,
  IncrementalAlterConfigs
- **GroupHandler**: JoinGroup/SyncGroup/Heartbeat/LeaveGroup with
  full state machine, assignment strategies
- **AdminHandler**: CreateTopics, DeleteTopics, CreatePartitions,
  ACL management, leader election
- **SaslHandler**: SASL handshake and authentication (SCRAM-SHA-256/512,
  PLAIN, OAUTHBEARER, GSSAPI)

### Security (3 files, ~4K lines)
- **AuthManager**: Authentication orchestration with session management
- **SaslProvider**: SCRAM-SHA-256/512 with PBKDF2, PLAIN, OAUTHBEARER, GSSAPI
- **AclEngine**: Wildcard-based ACL with specificity-weighted evaluation

### Observability (3 files, ~3K lines)
- **PrometheusExporter**: /metrics endpoint with broker/topic/process metrics,
  JMX compatibility
- **MetricsRegistry**: Singleton with gauge/counter/histogram, thread-safe
- **BrokerMetrics**: 11 atomic counters for broker-level telemetry

### Stream Processing (5 files, ~4K lines)
- **StreamProcessor**: Base topology with source/processor/sink nodes
- **FilterProcessor**: 14 predicate types with compound AND/OR/NOT
- **AggregateProcessor**: Windowed aggregations (tumbling/hopping/sliding/session)
- **JoinProcessor**: Stream-stream and stream-table joins
- **TopologyBuilder**: Fluent API for topology construction

### Connectors (9 files, ~5K lines)
- **ConnectFramework**: Source/sink connector lifecycle management
- **SourceConnector/SinkConnector**: Base classes with offset tracking
- **DebeziumSource**: CDC from MySQL/PostgreSQL with snapshot/incremental modes
- **JdbcSink**: JDBC batch insert/upsert with auto-create and DLQ
- **S3Sink**: S3 object output with partitioning and compression
- **ElasticsearchSink**: Bulk indexing with retry
- **Transforms**: Cast/Drop/Extract/Mask/Router/Insert field transforms
- **OffsetBackingStore**: Persistent offset storage

### Proxies (3 files, ~3K lines)
- **RestProxy**: REST API for produce/consume with consumer groups
- **WebSocketProxy**: RFC 6455 WebSocket with publish/subscribe
- **MqttBridge**: MQTT 3.1.1/5.0 with QoS 0/1/2, retained messages, will messages

### Common Utilities (12 files, ~5K lines)
- **Config**: JSON/CLI parsing with 50+ properties, typed getters, validation
- **ThreadPool**: Priority-lane thread pool with graceful/immediate shutdown
- **RateLimiter**: Token bucket with blocking and non-blocking modes
- **IOBuf**: Zero-copy scatter-gather buffer with reference-counted fragments
- **Endian**: Big/little-endian conversion, ZigZag, varint encoding
- **UUID**: v4 (random) and v7 (time-ordered) generation
- **Backoff/Retry**: Exponential backoff with jitter, retry policies
- **SignalHandler**: SIGINT/SIGTERM graceful shutdown
- **PidFile**: PID file with flock-based locking
- **Hostname**: Hostname/FQDN/DNS resolution
- **NetworkUtil**: Endpoint parsing, port availability, local interfaces

## Protocol Compatibility

torrent-mq implements 52 Kafka-compatible API keys (0-51):

| Key | API | Description |
|-----|-----|-------------|
| 0 | Produce | Send messages to a topic |
| 1 | Fetch | Read messages from a topic |
| 2 | ListOffsets | Get earliest/latest/by-timestamp offsets |
| 3 | Metadata | Get topic/partition/broker metadata |
| 4 | LeaderAndIsr | Inter-broker leader/ISR propagation |
| 5 | StopReplica | Inter-broker stop replica |
| 6 | UpdateMetadata | Inter-broker metadata update |
| 7 | ControlledShutdown | Graceful broker shutdown |
| 8 | OffsetCommit | Commit consumer group offsets |
| 9 | OffsetFetch | Fetch committed offsets |
| 10 | FindCoordinator | Find group/transaction coordinator |
| 11 | JoinGroup | Join consumer group |
| 12 | Heartbeat | Consumer group heartbeat |
| 13 | LeaveGroup | Leave consumer group |
| 14 | SyncGroup | Sync group assignments |
| 15 | DescribeGroups | Describe consumer groups |
| 16 | ListGroups | List all consumer groups |
| 17 | SaslHandshake | SASL mechanism negotiation |
| 18 | ApiVersions | API version discovery |
| 19 | CreateTopics | Admin: create topics |
| 20 | DeleteTopics | Admin: delete topics |
| 21 | DeleteRecords | Delete records before offset |
| 22 | InitProducerId | Initialize transactional producer |
| 23 | OffsetForLeaderEpoch | Offset by leader epoch |
| 24 | AddPartitionsToTxn | Add partitions to transaction |
| 25 | AddOffsetsToTxn | Add consumer offsets to transaction |
| 26 | EndTxn | Commit or abort transaction |
| 27 | WriteTxnMarkers | Write transaction markers |
| 28 | TxnOffsetCommit | Transactional offset commit |
| 29 | DescribeAcls | Describe ACL rules |
| 30 | CreateAcls | Create ACL rules |
| 31 | DeleteAcls | Delete ACL rules |
| 32 | DescribeConfigs | Describe topic/broker configs |
| 33 | AlterConfigs | Alter topic/broker configs |
| 34 | AlterReplicaLogDirs | Alter replica log directories |
| 35 | DescribeLogDirs | Describe log directories |
| 36 | SaslAuthenticate | SASL authentication exchange |
| 37 | CreatePartitions | Increase partition count |
| 38 | CreateDelegationToken | Create delegation token |
| 39 | RenewDelegationToken | Renew delegation token |
| 40 | ExpireDelegationToken | Expire delegation token |
| 41 | DescribeDelegationToken | Describe delegation tokens |
| 42 | DeleteGroups | Delete consumer groups |
| 43 | ElectLeaders | Trigger leader election |
| 44 | IncrementalAlterConfigs | Incremental config changes |
| 45 | AlterPartitionReassignments | Alter partition reassignments |
| 46 | ListPartitionReassignments | List partition reassignments |
| 47 | OffsetDelete | Delete committed offsets |
| 48 | DescribeClientQuotas | Describe client quotas |
| 49 | AlterClientQuotas | Alter client quotas |
| 50 | DescribeUserScramCredentials | Describe SCRAM credentials |
| 51 | AlterUserScramCredentials | Alter SCRAM credentials |

Plus 8 torrent-native APIs (60-67) for optimized operations:
TorrentFetch (zero-copy), TorrentBatchProduce, TorrentStreamSubscribe/Unsubscribe,
TorrentHealthCheck, TorrentClusterState, TorrentSchemaGet/Set.

## Building

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make torrentd -j$(nproc)
```

## Configuration

See `config/torrent.yaml` for a complete configuration example.
Default configuration values are documented in `src/torrent/common/config.cpp`.

## Testing

```bash
cd build
cmake .. -DTORRENT_BUILD_TESTS=ON
make torrent_tests -j$(nproc)
./torrent_tests
```

## License

See LICENSE file.
