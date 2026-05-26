# Migration Guide: Apache Kafka to torrent-mq

## Overview

torrent-mq is designed to be a drop-in replacement for Apache Kafka for most
workloads. It implements 52 Kafka-compatible API keys and uses the same wire
protocol, meaning existing Kafka clients and tools work without modification.

This guide covers migrating from Apache Kafka to torrent-mq with minimal
downtime and risk.

## Compatibility Matrix

### Fully Compatible

| Feature | Status | Notes |
|---|---|---|
| Produce API (v0-v9) | ✅ Full | All acks levels, compression, idempotent, transactional |
| Fetch API (v0-v13) | ✅ Full | Isolation levels, fetch sessions, follower fetching |
| Consumer Groups | ✅ Full | Join/Sync/Heartbeat/Leave, Range/RoundRobin/Sticky |
| Offset Management | ✅ Full | Commit, fetch, list, delete, leader epoch |
| Metadata API | ✅ Full | Topic, partition, broker discovery |
| Admin API | ✅ Full | Create/delete topics, partitions, configs, ACLs |
| SASL Authentication | ✅ Full | SCRAM-SHA-256/512, PLAIN, OAUTHBEARER |
| TLS Encryption | ✅ Full | TLS 1.3, mTLS, custom cipher suites |
| Transactions | ✅ Full | Exactly-once semantics, producer fencing |
| Log Compaction | ✅ Full | Key-based deduplication with custom combiners |
| Tiered Storage | ✅ Full | S3 archival and restore |

### Partial Compatibility

| Feature | Status | Notes |
|---|---|---|
| Connect Framework | ⚠️ Partial | Core framework compatible; custom connectors may need adaptation |
| Streams API | ⚠️ Partial | DSL not wire-compatible; use torrent-mq native stream API |
| MirrorMaker | ⚠️ Partial | Use torrent-mq KafkaMirrorConnector instead |
| KSQL | ❌ Not supported | Use torrent-mq stream processing directly |

## Pre-Migration Checklist

1. **Inventory your topics**
   ```bash
   kafka-topics.sh --bootstrap-server kafka:9092 --list
   ```

2. **Record topic configurations**
   ```bash
   kafka-configs.sh --bootstrap-server kafka:9092 --describe --all --topic my-topic
   ```

3. **Record consumer group offsets**
   ```bash
   kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group my-group --describe
   ```

4. **Verify client compatibility**
   - Check client library versions (librdkafka >= 1.8, Java client >= 2.8)
   - Verify no deprecated APIs are in use (e.g., older produce versions)

5. **Set up torrent-mq cluster**
   - Deploy 3+ brokers (matching your Kafka broker count)
   - Configure matching topic defaults (partitions, replication factor)
   - Set up monitoring (Prometheus + Grafana)

## Migration Strategies

### Strategy 1: Dual-Write with Cutover (Recommended)

Best for: Applications that can tolerate a brief write pause.

1. **Deploy torrent-mq cluster** alongside existing Kafka cluster

2. **Enable dual-write** in your applications:
   ```python
   # Python example using torrent-mq client
   from torrent_mq import TorrentProducer

   producer_kafka = KafkaProducer(bootstrap_servers='kafka:9092')
   producer_torrent = TorrentProducer(bootstrap_servers='torrent:9092')

   def send_dual(topic, value):
       producer_kafka.send(topic, value)
       producer_torrent.send(topic, value)
   ```

3. **Migrate consumers gradually**:
   - Start new consumer groups on torrent-mq
   - Let old consumer groups drain on Kafka
   - Switch traffic when lag reaches zero

4. **Validate data integrity**:
   ```bash
   # Compare message counts
   kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server kafka:9092 --topic my-topic
   torrentctl topic describe my-topic
   ```

5. **Cut over**:
   - Stop writes to Kafka
   - Verify last messages arrived on torrent-mq
   - Switch consumers to torrent-mq
   - Decommission Kafka cluster

### Strategy 2: Topic Mirroring

Best for: Large topics that cannot be dual-written.

1. **Set up KafkaMirrorConnector**:
   ```yaml
   name: kafka-mirror
   connector.class: KafkaMirrorConnector
   source.bootstrap.servers: kafka:9092
   source.group.id: mirror-group
   topic.mapping: ".*" -> "torrent.${topic}"
   ```

2. **Monitor mirror lag**:
   ```bash
   torrentctl connector status kafka-mirror
   ```

3. **When lag reaches zero, switch consumers** to torrent-mq topics

4. **Use topic prefix stripping** if topic names should match:
   ```yaml
   topic.mapping: "(.*)" -> "$1"
   source.prefix.strip: "torrent."
   ```

### Strategy 3: Blue-Green Deployment

Best for: Zero-downtime migrations.

1. **Deploy new application version** that supports both Kafka and torrent-mq
   via a feature flag

2. **Deploy torrent-mq cluster** with identical topic configuration

3. **Enable torrent-mq writes** via feature flag (keep Kafka writes)
   - Monitor for errors in both clusters
   - Verify data consistency

4. **Migrate consumers** one group at a time:
   - Move consumer group to torrent-mq
   - Verify processing continues
   - Monitor lag and throughput

5. **Disable Kafka writes** via feature flag

6. **Decommission Kafka**

## Consumer Group Offset Migration

To preserve consumer group offsets during migration:

1. **Export offsets from Kafka**:
   ```bash
   kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
     --group my-group --describe > offsets.txt
   ```

2. **Import offsets to torrent-mq**:
   ```bash
   torrentctl consumer offset my-group my-topic --offset 15000
   ```

Or programmatically:
```python
from torrent_mq import TorrentConsumer

consumer = TorrentConsumer(
    bootstrap_servers='torrent:9092',
    group_id='my-group'
)
consumer.subscribe(['my-topic'])

# Manually set offsets before starting consumption
for partition in consumer.assignment():
    kafka_offset = get_kafka_offset('my-group', 'my-topic', partition)
    consumer.seek(partition, kafka_offset)
```

## Topic Configuration Mapping

| Kafka Config | torrent-mq Config | Notes |
|---|---|---|
| `retention.ms` | `storage.retention.ms` | Identical semantics |
| `retention.bytes` | `storage.retention.bytes` | Identical semantics |
| `compression.type` | `topic.compression.type` | Same options (none, gzip, snappy, lz4, zstd) |
| `max.message.bytes` | `topic.max.message.bytes` | Identical semantics |
| `min.insync.replicas` | `topic.min.insync.replicas` | Identical semantics |
| `cleanup.policy` | `storage.cleanup.policy` | delete, compact, compact+delete |
| `segment.bytes` | `storage.segment.bytes` | Identical semantics |
| `segment.ms` | `storage.segment.max_age_ms` | Convert to milliseconds |
| `min.compaction.lag.ms` | `storage.compaction.min_compaction_lag_ms` | Identical |
| `max.compaction.lag.ms` | `storage.compaction.max_compaction_lag_ms` | Identical |
| `unclean.leader.election.enable` | `raft.unclean.leader.election` | Different default (false in torrent-mq) |

## Known Differences

### Architecture

- **No ZooKeeper**: torrent-mq uses embedded Raft consensus instead of ZooKeeper
  for cluster coordination. No external coordination service needed.

- **Controller**: torrent-mq controller is a Raft leader, elected automatically.
  No `controller.quorum.voters` configuration needed.

- **Single Binary**: torrent-mq is a single binary. No separate Connect or
  Streams processes.

### Behavior

- **Default unclean leader election**: Disabled in torrent-mq (vs enabled in Kafka).
  This prioritizes consistency over availability.

- **Compaction**: torrent-mq supports custom combiner functions during compaction,
  enabling merge semantics beyond "keep latest."

- **Transactions**: Producer IDs are 64-bit (vs 16-bit in Kafka), reducing
  collision risk in large clusters.

- **Tiered Storage**: Built-in S3 archival. No external tiered storage plugin needed.

### Configuration

- Property names use dots but follow torrent-mq conventions:
  `storage.log.dir` (not `log.dirs`), `topic.default.partitions` (not `num.partitions`)

## Rollback Procedure

If migration issues arise:

1. **Stop torrent-mq consumers**
2. **Resume Kafka consumers** from last committed offsets
3. **Redirect producers back to Kafka**
4. **Investigate and fix issues** in a staging environment
5. **Retry migration** when ready

## Monitoring During Migration

Monitor these metrics during cutover:

```
# torrent-mq
torrent_messages_produced_total
torrent_messages_consumed_total
torrent_active_connections
torrent_requests_failed_total

# Kafka
kafka_server_broker_topics_messages_in_total
kafka_server_broker_topics_bytes_in_total
kafka_consumer_group_lag
```

## Post-Migration

1. **Verify all topics** exist with correct configurations
2. **Verify consumer groups** have correct offsets
3. **Run performance tests** to validate throughput
4. **Update monitoring dashboards** to point to torrent-mq
5. **Update runbooks** and documentation
6. **Schedule Kafka decommission** after a burn-in period (recommended: 1-2 weeks)

## Support

For migration assistance, contact the torrent-mq team or open an issue at
https://github.com/torrent-mq/torrent-mq/issues.
