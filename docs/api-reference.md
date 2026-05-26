# torrent-mq API Reference

## Client Protocol

### Produce API (api_key=0)

Send messages to a topic partition.

**Request:**
```
transactional_id: NULLABLE_STRING | acks: INT16 | timeout_ms: INT32
topic_count: INT32
[topic_name: STRING, partition_count: INT32
  [partition: INT32, record_batch: RECORD_BATCH]]
```

**Response:**
```
topic_count: INT32
[topic_name: STRING, partition_count: INT32
  [partition: INT32, error_code: INT16, base_offset: INT64,
   log_append_time_ms: INT64, log_start_offset: INT64]]
throttle_time_ms: INT32
```

**Error Codes:**
- NOT_LEADER_FOR_PARTITION (6): This broker is not the leader
- MESSAGE_TOO_LARGE (10): Message exceeds max.message.bytes
- TOPIC_AUTHORIZATION_FAILED (29): Not authorized to produce
- OUT_OF_ORDER_SEQUENCE_NUMBER (45): Idempotent sequence violation
- DUPLICATE_SEQUENCE_NUMBER (46): Duplicate idempotent sequence

### Fetch API (api_key=1)

Read messages from a topic partition.

**Request:**
```
replica_id: INT32 | max_wait_ms: INT32 | min_bytes: INT32 | max_bytes: INT32
isolation_level: INT8 | session_id: INT32 | session_epoch: INT32
topic_count: INT32
[topic_name: STRING, partition_count: INT32
  [partition: INT32, fetch_offset: INT64, log_start_offset: INT64,
   partition_max_bytes: INT32]]
```

**Response:**
```
throttle_time_ms: INT32 | error_code: INT16 | session_id: INT32
topic_count: INT32
[topic_name: STRING, partition_count: INT32
  [partition: INT32, error_code: INT16, high_watermark: INT64,
   last_stable_offset: INT64, log_start_offset: INT64,
   aborted_transactions: [...], record_batch: RECORD_BATCH]]
```

### Consumer Group Protocol

**JoinGroup (api_key=11):**
```
group_id: STRING | session_timeout_ms: INT32 | rebalance_timeout_ms: INT32
member_id: STRING | protocol_type: STRING
protocol_count: INT32 [{name: STRING, metadata: BYTES}]
```

**SyncGroup (api_key=14):**
```
group_id: STRING | generation_id: INT32 | member_id: STRING
assignment: BYTES  // Only from group leader
```

**Heartbeat (api_key=12):**
```
group_id: STRING | generation_id: INT32 | member_id: STRING
```

## Admin REST API

### GET /v1/cluster/health

Returns cluster health status.

**Response:**
```json
{
  "status": "healthy",
  "subsystems": {
    "network": true,
    "storage": true,
    "consensus": true
  },
  "capacity": {
    "connections": 42,
    "topics": 5,
    "partitions": 15,
    "disk_usage_percent": 23.5
  },
  "uptime_seconds": 3600
}
```

### POST /v1/topics

Create a new topic.

**Request:**
```json
{
  "name": "my-topic",
  "partitions": 3,
  "replication_factor": 3,
  "configs": {
    "retention.ms": "86400000",
    "compression.type": "lz4"
  }
}
```

**Response (201):**
```json
{
  "name": "my-topic",
  "partitions": 3,
  "replication_factor": 3
}
```

### GET /v1/topics/{topic}

Get topic details.

**Response:**
```json
{
  "name": "my-topic",
  "internal": false,
  "partition_count": 3,
  "replication_factor": 3,
  "partitions": [
    {
      "partition": 0,
      "leader": 1,
      "replicas": [1, 2, 3],
      "isr": [1, 2, 3],
      "earliest_offset": 0,
      "latest_offset": 15000
    }
  ],
  "configs": {
    "retention.ms": "86400000",
    "compression.type": "lz4"
  }
}
```

### GET /v1/consumer-groups

List all consumer groups.

**Response:**
```json
{
  "groups": [
    {
      "group_id": "my-consumer-group",
      "protocol_type": "consumer",
      "state": "Stable",
      "members": [
        {
          "member_id": "consumer-1-abc123",
          "client_id": "consumer-1",
          "client_host": "/192.168.1.100",
          "assignment": {"topic": [0, 1]}
        }
      ]
    }
  ]
}
```

## Metrics (Prometheus)

All metrics are exposed at `:9090/metrics` in Prometheus text format:

```
# HELP torrent_messages_produced_total Total messages produced
# TYPE torrent_messages_produced_total counter
torrent_messages_produced_total 1523400

# HELP torrent_messages_consumed_total Total messages consumed
# TYPE torrent_messages_consumed_total counter
torrent_messages_consumed_total 1523000

# HELP torrent_bytes_produced_total Total bytes produced
# TYPE torrent_bytes_produced_total counter
torrent_bytes_produced_total 1523400000

# HELP torrent_active_connections Current active connections
# TYPE torrent_active_connections gauge
torrent_active_connections 42

# HELP torrent_disk_usage_bytes Current disk usage in bytes
# TYPE torrent_disk_usage_bytes gauge
torrent_disk_usage_bytes 5368709120

# HELP torrent_leader_elections_total Total leader elections
# TYPE torrent_leader_elections_total counter
torrent_leader_elections_total 3

# HELP torrent_topic_messages_in_total Messages per topic
# TYPE torrent_topic_messages_in_total counter
torrent_topic_messages_in_total{topic="my-topic"} 500000

# HELP torrent_process_cpu_seconds_total Process CPU seconds
# TYPE torrent_process_cpu_seconds_total counter
torrent_process_cpu_seconds_total 120.5
```

## CLI Reference

### torrentctl

```
Usage: torrentctl [options] <command> [args]

Options:
  --admin-url URL    Admin API URL (default: http://localhost:9644)
  --json             Output in JSON format
  --verbose          Verbose output

Commands:
  topic create <name> [--partitions N] [--replication-factor N]
  topic list
  topic describe <name>
  topic delete <name>

  consumer list
  consumer describe <group>
  consumer delete <group>
  consumer offset <group> <topic>

  cluster health
  cluster brokers
  cluster describe

  acl list
  acl add --principal P --operation OP --resource R --allow|--deny
  acl delete --principal P --operation OP --resource R

  config list
  config get <key>
  config set <key> <value>
```

## Configuration Properties

| Property | Default | Description |
|---|---|---|
| broker.id | 0 | Unique broker ID |
| listeners.plain.port | 9092 | Plain TCP port |
| listeners.tls.port | 9093 | TLS port |
| admin.port | 9644 | Admin REST API port |
| metrics.port | 9090 | Prometheus metrics port |
| storage.log.dir | /var/lib/torrent/data | Data directory |
| storage.segment.bytes | 1073741824 | Max segment size (1GB) |
| storage.retention.ms | 604800000 | Default retention (7 days) |
| network.num_io_threads | 4 | I/O worker threads |
| network.num_worker_threads | 8 | CPU worker threads |
| network.max_connections | 65536 | Max connections |
| topic.default.partitions | 1 | Default partition count |
| topic.default.replication_factor | 3 | Default replication factor |
| raft.heartbeat_interval_ms | 150 | Raft heartbeat interval |
| raft.election_timeout_min_ms | 150 | Min election timeout |
| raft.election_timeout_max_ms | 300 | Max election timeout |
| security.sasl.enabled | false | Enable SASL auth |
| security.tls.enabled | false | Enable TLS |
| transaction.enabled | true | Enable transactions |
| transaction.timeout.ms | 60000 | Transaction timeout |
