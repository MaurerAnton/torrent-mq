# torrent-mq Architecture

## Design Philosophy

torrent-mq is built on five core principles:

1. **Performance First** — Every component is optimized for microsecond latency
   and maximum throughput. C++20 with zero-overhead abstractions, non-blocking
   I/O, lock-free data structures where possible, and SSE4.2-accelerated CRC.

2. **Correctness** — Data must never be lost or corrupted. CRC32C validation
   at every layer, Raft consensus for metadata, exactly-once semantics for
   transactions, and crash recovery with segment-level integrity checks.

3. **Horizontal Scalability** — Partitioned topics with Raft-based replication,
   rack-aware leader placement, automatic rebalancing, and a controller that
   manages the cluster topology without any external coordination service.

4. **Kafka Compatibility** — Wire protocol compatibility with 52 Kafka API keys
   means existing Kafka clients and tools work unchanged. Migration is a simple
   bootstrap server change.

5. **Operational Simplicity** — Single binary, no ZooKeeper dependency,
   self-contained Raft consensus, automatic leader election, built-in admin
   REST API and Prometheus metrics.

## Data Flow

### Write Path (Produce)

```
Client → TCP/TLS Connection → Connection (frame parsing)
  → RequestDispatcher (api_key routing)
  → ProduceHandler (parse + validate + quota check)
  → PartitionManager (find leader)
  → LogManager::append()
    → Segment::append() — serialize + CRC + pwrite
    → Segment::sparse_index_update
    → flush/fsync (depending on acks)
  → RaftNode::propose() — replicate to followers
  → Response → Connection → Client
```

### Read Path (Fetch)

```
Client → Connection → RequestDispatcher
  → FetchHandler (parse + validate)
  → LogManager::read()
    → find_segment_index() — binary search
    → Segment::read() — sparse index lookup + pread + CRC validate
    → deserialize + return RecordBatches
  → Response (with watermarks) → Client
```

### Consensus Path

```
Leader heartbeat timer → RaftNode::send_heartbeats()
  → AppendEntries RPC → followers
  → followers update commit_index
  → leader advance_commit_index (when quorum ack'd)
  → apply_committed → state machine

Leader election:
  → timeout → become_candidate → increment term
  → RequestVote RPC → peers
  → majority vote → become_leader
```

## Storage Layout

```
/var/lib/torrent/data/
├── {topic}-{partition}/
│   ├── 00000000000000000000.log        # Segment file (base offset 0)
│   ├── 00000000000000000000.index      # Sparse offset index
│   ├── 00000000000000000000.timeindex  # Timestamp index
│   ├── 00000000000000100000.log        # Segment file (base offset 100000)
│   ├── 00000000000000100000.index
│   └── ...
├── __consumer_offsets/
│   └── ...
├── __transaction_state/
│   └── ...
└── raft/
    ├── log.0
    └── snapshot.0
```

## Threading Model

torrent-mq uses a hybrid threading model:

| Thread Pool | Purpose | Default Size |
|---|---|---|
| I/O Workers | Network accept, read, write | 4 |
| CPU Workers | Request processing, serialization | 8 |
| Raft Workers | Consensus operations | 2 per node |
| Background Workers | Compaction, retention, rebalancing | 2 |
| Health Probe | Periodic health checks | 1 |

Each connection is pinned to an I/O worker thread. Requests are dispatched
to CPU workers via a priority queue (high/normal/low). Raft operations run
on dedicated threads to avoid head-of-line blocking.

## Wire Protocol

### Kafka API Frame Format

```
+-------------------+-------------------+-------------------+
| 4 bytes: length   | 2 bytes: api_key  | 2 bytes: version  |
+-------------------+-------------------+-------------------+
| 4 bytes: corr_id  | 2 bytes: client_len | N bytes: client_id|
+-------------------+-------------------+-------------------+
| ... tag buffer / request body ...                       |
+----------------------------------------------------------+
```

### Record Batch v2 Format

```
+--------+--------+---------+------+------+------+------+------+------+
| offset | length | ldr_ep  | magic| CRC  | attr | delta| ts1  | ts2  |
| 8      | 4      | 4       | 1    | 4    | 2    | 4    | 8    | 8    |
+--------+--------+---------+------+------+------+------+------+------+
| pid    | pep    | seq     | count| ... records ...                   |
| 8      | 2      | 4       | 4    |                                   |
+--------+--------+---------+------+------+------+------+------+------+
```

## Security Architecture

```
Connection → SASL Handshake (mechanism negotiation)
  → SaslHandshake → SaslAuthenticate
    → SaslProvider (SCRAM-SHA-256/512, PLAIN, OAUTHBEARER, GSSAPI)
      → PBKDF2 key derivation (SCRAM)
      → Token validation (OAUTHBEARER)
  → Session established → principal identified

Each request:
  → AclEngine::check_access(principal, host, resource, operation)
    → Wildcard matching with specificity ordering
    → Default deny
  → AuditLog::log_* (auth, admin, security events)
```

## Failure Handling

| Failure Mode | Detection | Recovery |
|---|---|---|
| Segment corruption | CRC32C on every read | Mark corrupted, rebuild from replicas |
| Leader crash | Raft election timeout | Automatic leader election (150-300ms) |
| Follower crash | Heartbeat timeout | Remove from ISR, continue with remaining |
| Network partition | Raft heartbeat failure | Minority cannot elect, majority continues |
| Disk full | write() returns ENOSPC | Log error, reject produces, alert operator |
| Process crash | OS terminates process | Segment recovery on restart (truncate last) |
| Transaction timeout | TransactionCoordinator timer | Auto-abort expired transactions |
