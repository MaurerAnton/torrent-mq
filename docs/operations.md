# torrent-mq Operations Guide

## Day-to-Day Operations

This guide covers routine operational tasks for running torrent-mq in
production. It assumes you have a working cluster deployed according to
the deployment guide.

## Table of Contents

1.  [Starting and Stopping Brokers](#starting-and-stopping-brokers)
2.  [Monitoring Health](#monitoring-health)
3.  [Adding and Removing Brokers](#adding-and-removing-brokers)
4.  [Rebalancing Partitions](#rebalancing-partitions)
5.  [Changing Configurations](#changing-configurations)
6.  [Upgrading Brokers (Rolling)](#upgrading-brokers-rolling)
7.  [Backup and Restore Procedures](#backup-and-restore-procedures)
8.  [Disaster Recovery](#disaster-recovery)
9.  [Troubleshooting Common Issues](#troubleshooting-common-issues)
10. [Log Analysis](#log-analysis)
11. [Performance Tuning Checklist](#performance-tuning-checklist)

## Starting and Stopping Brokers

### Starting a Broker

torrent-mq uses a single binary, `torrentd`. Start it with:

```bash
# Systemd (recommended)
sudo systemctl start torrentd

# Manual
torrentd --config-file /etc/torrent/config.yaml
```

The broker will:

1.  Read its configuration and determine cluster membership.
2.  Connect to seed servers and join the Raft consensus group.
3.  Recover any existing partition data from disk.
4.  Begin listening on the configured ports (default: 9092 for plain,
    9093 for TLS, 9644 for admin API, 9090 for Prometheus metrics).
5.  Participate in leader election for partitions it hosts.

### Verifying Startup

```bash
# Check process
ps aux | grep torrentd

# Check admin API
curl http://localhost:9644/v1/status

# Check logs
journalctl -u torrentd -f
```

### Stopping a Broker Gracefully

```bash
# Systemd (recommended — sends SIGTERM, waits for controlled shutdown)
sudo systemctl stop torrentd

# Manual (SIGTERM from another terminal)
kill -TERM $(cat /var/run/torrentd.pid)
```

**Graceful shutdown sequence:**

1.  Stop accepting new client connections.
2.  Drain in-flight produce requests (commit to disk).
3.  Transfer partition leadership to healthy followers.
4.  Flush all segment files and close them.
5.  Write a clean shutdown marker to disk.
6.  Exit.

**Forced shutdown** (SIGKILL) triggers crash recovery on next start.
This is safe but will take longer to restart.

### Stopping the Entire Cluster

Stop brokers one at a time. Wait for leadership transfer to complete
between each stop:

```bash
for broker in broker0 broker1 broker2; do
    ssh $broker sudo systemctl stop torrentd
    sleep 30  # Let leadership transfer complete
done
```

## Monitoring Health

### Admin API Endpoints

| Endpoint                          | Description                     |
|-----------------------------------|---------------------------------|
| `GET /v1/status`                  | Broker status overview          |
| `GET /v1/health`                  | Health check (returns 200 if healthy) |
| `GET /v1/metrics`                 | Prometheus metrics endpoint     |
| `GET /v1/brokers`                 | List all brokers in cluster     |
| `GET /v1/topics`                  | List all topics                 |
| `GET /v1/topics/{name}`           | Topic details incl. partitions  |
| `GET /v1/consumer-groups`         | List consumer groups            |
| `GET /v1/consumer-groups/{name}`  | Consumer group lag details      |

### Prometheus Metrics

Key metrics to monitor:

| Metric                                    | Description                            | Alert Threshold              |
|-------------------------------------------|----------------------------------------|------------------------------|
| `torrent_broker_state`                    | 1 = healthy, 0 = degraded              | = 0                          |
| `torrent_partition_under_replicated`      | Partitions missing replicas            | > 0 for 5 min                |
| `torrent_consumer_group_lag`              | Messages behind per group              | > 100000                     |
| `torrent_disk_free_bytes`                 | Free disk space                        | < 10 GB                      |
| `torrent_active_connections`              | Active client connections              | > 80% of max_connections     |
| `torrent_produce_latency_p99_ms`          | p99 produce latency                    | > 100 ms                     |
| `torrent_raft_commit_latency_p99_ms`      | Raft commit latency                    | > 50 ms                      |
| `torrent_leader_elections_total`          | Leader election count (rate)           | > 1 / minute                 |
| `torrent_request_queue_depth`             | Pending requests in queue              | > 10000                      |
| `torrent_isr_shrink_total`                | ISR shrink events                      | > 0 / hour                   |
| `torrent_segment_count`                   | Open segments per partition            | > 100 per partition          |
| `torrent_bytes_in_per_sec`                | Inbound bytes/sec                      | > 90% of NIC capacity        |
| `torrent_bytes_out_per_sec`               | Outbound bytes/sec                     | > 90% of NIC capacity        |

### Health Check Script

```bash
#!/bin/bash
# check_cluster_health.sh — Run via cron every 60 seconds

BROKERS="broker0:9644 broker1:9644 broker2:9644"
ALERT_EMAIL="ops@example.com"

for broker in $BROKERS; do
    status=$(curl -s -o /dev/null -w "%{http_code}" http://$broker/v1/health)
    if [ "$status" != "200" ]; then
        echo "Broker $broker unhealthy (HTTP $status)" | \
            mail -s "torrent-mq alert: $broker down" $ALERT_EMAIL
    fi
done
```

### Dashboard Recommendations

Use Grafana with Prometheus datasource. Key dashboards to create:

1.  **Cluster Overview** — Broker states, partition counts, ISR status.
2.  **Throughput** — Bytes in/out, produce/fetch rates per topic.
3.  **Latency** — p50/p95/p99 for produce, fetch, and commit.
4.  **Consumer Lag** — Lag per consumer group, trend over time.
5.  **Storage** — Disk usage, segment counts, compaction stats.
6.  **Raft** — Leader elections, commit latency, snapshot frequency.

## Adding and Removing Brokers

### Adding a New Broker

1.  Install torrent-mq on the new machine.
2.  Create `/etc/torrent/config.yaml` with:
    -   Unique `broker.id` (next available integer).
    -   Same `cluster.id` as existing cluster.
    -   `cluster.seed_servers` pointing to existing brokers.
    -   Appropriate storage paths.
3.  Start the broker:

```bash
sudo systemctl start torrentd
```

4.  The new broker joins the cluster automatically. It will not host any
    partitions initially.
5.  Verify it appears in the cluster:

```bash
curl http://any-broker:9644/v1/brokers | jq '.brokers[] | select(.id == NEW_ID)'
```

### Moving Partitions to the New Broker

Use `torrentctl` to reassign partitions:

```bash
# Generate a reassignment plan (even distribution including new broker)
torrentctl admin reassign --topics ".*" --brokers 0,1,2,3 --generate

# Review the plan, then execute
torrentctl admin reassign --topics ".*" --brokers 0,1,2,3 --execute

# Monitor progress
torrentctl admin reassign --verify
```

The reassignment happens incrementally — one partition replica at a time.
Traffic is not interrupted.

### Removing a Broker

1.  Decommission the broker gracefully:

```bash
# Move all partitions off the broker first
torrentctl admin reassign --topics ".*" \
    --brokers $(torrentctl admin brokers | grep -v BROKER_ID | wc -l) \
    --exclude-brokers BROKER_ID --execute

# Wait for reassignment to complete
torrentctl admin reassign --verify
```

2.  Once the broker hosts zero partitions, stop it:

```bash
ssh target-broker sudo systemctl stop torrentd
```

3.  Remove from seed server lists on remaining brokers.

4.  (Optional) Remove data directories from the decommissioned machine.

**Important:** Do not remove a broker without first migrating its
partitions. Data loss will occur if the broker hosted the only replica
of any partition.

## Rebalancing Partitions

### When to Rebalance

-   After adding or removing brokers.
-   When disk usage is uneven across brokers (> 20% variance).
-   When throughput per broker is uneven (> 30% variance).
-   When leadership distribution is uneven.

### Manual Rebalance

```bash
# View current distribution
torrentctl admin topics --describe

# Generate rebalance plan
torrentctl admin rebalance --topics ".*" --generate > rebalance-plan.json

# Review the plan
cat rebalance-plan.json | jq .

# Execute
torrentctl admin rebalance --execute --plan rebalance-plan.json
```

### Automatic Rebalancing

Enable automatic rebalancing in the configuration:

```yaml
broker:
  auto_rebalance:
    enabled: true
    check_interval_ms: 300000  # 5 minutes
    imbalance_threshold: 0.2   # 20% variance triggers rebalance
    max_concurrent_moves: 5    # Limit concurrent replica moves
```

### Leadership Rebalance

Leaders handle all produce requests and consume more CPU. Ensure they are
evenly distributed:

```bash
# Preferred leader election (moves leaders to preferred replicas)
torrentctl admin leader-rebalance --topics ".*"

# This is safe — leadership transfer is a brief Raft operation
```

### Partition Count Changes

Increasing partition count for a topic:

```bash
torrentctl admin alter-topic my-topic --partitions 16
```

**Note:** Decreasing partition count is not supported (Kafka
compatibility). Plan partition counts carefully.

## Changing Configurations

### Dynamic Configuration (No Restart Required)

Many settings can be changed at runtime via the admin API:

```bash
# Change retention
curl -X PUT http://localhost:9644/v1/config/retention.ms \
    -H "Content-Type: application/json" \
    -d '{"value": 259200000}'  # 3 days

# Change log segment size
curl -X PUT http://localhost:9644/v1/config/segment.bytes \
    -H "Content-Type: application/json" \
    -d '{"value": 536870912}'  # 512 MB

# List dynamic configs
curl http://localhost:9644/v1/config
```

Dynamically configurable settings:

| Config Key                     | Description                   |
|-------------------------------|-------------------------------|
| `retention.ms`                | Message retention period      |
| `retention.bytes`             | Max partition size            |
| `segment.bytes`               | Segment rollover size         |
| `segment.ms`                  | Segment rollover age          |
| `compression.type`            | Default compression           |
| `max.message.bytes`           | Max message size              |
| `flush.ms`                    | Flush interval                |
| `flush.messages`              | Messages per flush            |
| `min.insync.replicas`        | Minimum ISR size              |
| `unclean.leader.election.enable` | Allow out-of-ISR election |

### Static Configuration (Requires Restart)

Changes to these settings require a rolling restart:

-   `broker.id`
-   `listeners` (host/port)
-   `storage.log.dir`
-   `raft.*` settings
-   `network.num_io_threads`
-   `security.tls.*`

### Configuration Rollback

Config changes are logged to the audit log. To revert:

```bash
# View recent config changes
curl http://localhost:9644/v1/audit?type=config&limit=10

# Revert to previous value
curl -X PUT http://localhost:9644/v1/config/retention.ms \
    -H "Content-Type: application/json" \
    -d '{"value": PREVIOUS_VALUE}'
```

## Upgrading Brokers (Rolling)

### Pre-Upgrade Checklist

-   Verify cluster is healthy (all brokers green, no under-replicated
    partitions).
-   Review the [CHANGELOG](https://github.com/torrent-mq/torrent-mq/releases).
-   Note any breaking config changes or deprecated features.
-   Back up critical metadata (see Backup section).
-   Schedule during low-traffic window.
-   Test the upgrade in a staging environment first.

### Rolling Upgrade Procedure

1.  **Stop the first broker:**

```bash
ssh broker0 sudo systemctl stop torrentd
```

2.  **Wait for leadership failover** (partitions on broker0 will elect
    new leaders on other brokers). Monitor:

```bash
curl http://broker1:9644/v1/health | jq '.under_replicated_partitions'
# Should return to 0 within ~30 seconds
```

3.  **Upgrade the binary:**

```bash
ssh broker0
wget https://github.com/torrent-mq/torrent-mq/releases/download/vX.Y.Z/torrentd
sudo cp torrentd /usr/local/bin/torrentd
sudo chmod +x /usr/local/bin/torrentd
```

4.  **Update configuration** if needed (new required fields, changed
    defaults).

5.  **Start the broker:**

```bash
sudo systemctl start torrentd
```

6.  **Wait for broker to rejoin and stabilize:**

```bash
# Watch the broker come online
curl http://broker0:9644/v1/health
# Should show 'state: active'

# Watch partition reassignment complete
watch -n 5 "curl -s http://broker0:9644/v1/status | jq '.replicas_moving'"
# Should return to 0
```

7.  **Move to the next broker.** Repeat steps 1-6 for each remaining
    broker.

**Golden rule:** Never upgrade more than one broker at a time. Wait for
the cluster to stabilize after each broker upgrade.

### Version Compatibility

torrent-mq follows semantic versioning:

-   **Patch upgrades** (x.y.Z): Always safe, no downtime required beyond
    the rolling restart.
-   **Minor upgrades** (x.Y.z): Wire protocol compatible. May add new
    config options. Rolling upgrade is safe.
-   **Major upgrades** (X.y.z): May have breaking changes. Check the
    migration guide. Two-phase upgrade may be required.

### Automated Upgrade with torrentctl

```bash
# Check available updates
torrentctl admin version --check

# Perform rolling upgrade
torrentctl admin upgrade --version X.Y.Z \
    --brokers broker0,broker1,broker2 \
    --health-check-timeout 120
```

### Rolling Back

If an upgrade causes issues, roll back to the previous version using the
same rolling procedure:

1.  Stop one broker.
2.  Replace binary with previous version.
3.  Start broker.
4.  Verify health.
5.  Repeat for remaining brokers.

**Important:** Rolling back across a major version boundary may require
the old version to handle data written by the new version. Test
downgrade compatibility in staging before production upgrades.

## Backup and Restore Procedures

### What to Back Up

| Data                          | Path (default)                       | Criticality |
|-------------------------------|--------------------------------------|-------------|
| Raft metadata                 | `/var/lib/torrent/data/raft/`        | Critical    |
| Consumer offsets              | `__consumer_offsets` topic           | Critical    |
| Transaction state             | `__transaction_state` topic          | High        |
| Topic data (if not replicated)| `/var/lib/torrent/data/{topic}*/`    | Medium      |
| Configuration                 | `/etc/torrent/config.yaml`           | High        |
| TLS certificates              | `/etc/torrent/certs/`                | High        |
| ACL file                      | `/etc/torrent/acl.yaml`              | Medium      |
| Schema registry data          | Internal topic `__schemas`           | Medium      |

### Backup Strategy

Since torrent-mq replicates data across multiple brokers (default RF=3),
individual broker failure does not cause data loss. Backups are primarily
for:

1.  **Disaster recovery** (entire cluster loss).
2.  **Logical corruption** (accidental topic deletion).
3.  **Compliance** (long-term archival).

#### Option A: Filesystem Snapshots

```bash
# On each broker, take a snapshot of the data directory
# Assumes LVM, ZFS, or btrfs with snapshot capability

# LVM example
lvcreate -L 10G -s -n torrent-snap-$(date +%Y%m%d) /dev/vg0/torrent-data
mount /dev/vg0/torrent-snap-$(date +%Y%m%d) /mnt/torrent-backup
tar czf /backup/torrent-$(date +%Y%m%d).tar.gz -C /mnt/torrent-backup .
umount /mnt/torrent-backup
lvremove -f /dev/vg0/torrent-snap-$(date +%Y%m%d)
```

#### Option B: S3/Tiered Storage Archival

Enable tiered storage in config.yaml:

```yaml
storage:
  tiered_storage:
    enabled: true
    s3:
      bucket: torrent-backup
      region: us-east-1
    archive_after_ms: 86400000  # Archive segments older than 1 day
```

This automatically archives closed segments to S3. Older segments are
kept in S3 and can be retrieved on demand.

#### Option C: Mirroring to a Second Cluster

Use the Kafka Mirror connector to replicate topics to a backup cluster:

```bash
torrentctl connectors create --config '
{
  "name": "backup-mirror",
  "connector.class": "torrent.connectors.kafka_mirror",
  "source.cluster": "primary",
  "target.cluster": "backup",
  "topics": ".*",
  "sync.group.offsets": "true"
}'
```

### Restore Procedure

#### Restoring from Filesystem Backup

1.  Stop all brokers.
2.  Restore data directories from backup:

```bash
# On each broker
sudo systemctl stop torrentd
rm -rf /var/lib/torrent/data/*
tar xzf /backup/torrent-RESTORE_DATE.tar.gz -C /var/lib/torrent/data/
sudo systemctl start torrentd
```

3.  Start brokers one by one and verify cluster health.

#### Restoring from S3 (Tiered Storage)

1.  Start a fresh broker with tiered storage enabled.
2.  The broker will detect archived segments in S3 automatically and
    begin downloading them (lazy, on-demand).
3.  Hot data must still be present on the broker's local disk.

#### Restoring Consumer Offsets from Mirror

If you have a mirror cluster with `sync.group.offsets: true`, consumer
offsets are automatically replicated. When consumers reconnect to the
backup cluster, they will resume from their last committed offset.

## Disaster Recovery

### Scenarios and Responses

#### Scenario 1: Single Broker Failure

**Impact:** Partitions with RF=3 remain available (2/3 replicas).

**Response:**
1.  Identify failed broker: `torrentctl admin brokers`
2.  Check under-replicated partitions: `torrentctl admin topics --under-replicated`
3.  Attempt to restart the broker.
4.  If hardware failure, follow the broker removal procedure.
5.  If the broker was a leader, Raft automatically elects new leaders
    (~150-300 ms).

#### Scenario 2: Two Brokers Fail (RF=3 Cluster)

**Impact:** Partitions with leaders on surviving broker remain available.
Partitions where the surviving broker is NOT the leader become
unavailable for writes (ISR < min.insync.replicas).

**Response:**
1.  Bring at least one more broker online as quickly as possible.
2.  If urgent, lower `min.insync.replicas` to 1:

```bash
curl -X PUT http://SURVIVING_BROKER:9644/v1/config/min.insync.replicas \
    -H "Content-Type: application/json" -d '{"value": 1}'
```

3.  Restore brokers and raise `min.insync.replicas` back to 2 or 3.

#### Scenario 3: Complete Cluster Loss

**Impact:** Total outage.

**Response:**
1.  Restore from backup (see Restore section).
2.  Start brokers in order: broker0, broker1, broker2.
3.  Verify Raft consensus is established.
4.  Verify partition data is intact.
5.  Restart producers and consumers.

#### Scenario 4: Raft Quorum Loss

**Impact:** Metadata operations (topic creation, configuration changes)
fail. Data plane may continue if partition leaders are healthy.

**Response:**
1.  Identify quorum: need > N/2 brokers online.
2.  Bring offline brokers back.
3.  If quorum cannot be restored, a manual recovery may be required:

```bash
# Force a single-node recovery (LAST RESORT)
torrentd --recover-raft --raft-force-single-node
```

#### Scenario 5: Disk Full

**Impact:** Produce requests rejected with error.

**Response:**
1.  Reduce retention aggressively:

```bash
curl -X PUT http://localhost:9644/v1/config/retention.ms \
    -H "Content-Type: application/json" -d '{"value": 3600000}'  # 1 hour
```

2.  Trigger immediate log cleanup:

```bash
curl -X POST http://localhost:9644/v1/admin/cleanup
```

3.  Add disk space (expand volume, add mount).
4.  Restore original retention after disk is clear.

#### Scenario 6: Split Brain

**Impact:** Two subsets of the cluster both believe they are the
majority.

**Prevention:** torrent-mq requires majority (> N/2) for leader
election. With RF=3, at least 2 brokers must agree. Network partitions
that split 2/1 are safe (minority cannot elect). Partitions that split
1/1/1 cause no leader to be elected, which is safe.

**Response:**
1.  Fix the network partition.
2.  Brokers will automatically resolve — the side with quorum resumes
    leadership.

## Troubleshooting Common Issues

### Broker Won't Start

**Symptoms:** `torrentd` exits immediately after startup.

**Checks:**
```bash
# Check logs
journalctl -u torrentd --since "5 minutes ago"

# Common causes:
# 1. Port already in use
sudo ss -tlnp | grep 9092

# 2. Corrupt data directory
ls -la /var/lib/torrent/data/

# 3. Configuration syntax error
torrentd --config-file /etc/torrent/config.yaml --validate-only

# 4. Disk full
df -h /var/lib/torrent/data
```

**Solutions:**
- Port conflict: Change `listeners.plain.port` or kill the competing
  process.
- Corrupt data: Remove `/var/lib/torrent/data/` and restart (data loss
  if no replicas elsewhere! Only do this if the partition has other
  healthy replicas).
- Config error: Fix the YAML syntax.
- Disk full: Free space or expand volume.

### High Consumer Lag

**Symptoms:** `torrent_consumer_group_lag` metric growing.

**Checks:**
```bash
# View lag per consumer group
curl http://localhost:9644/v1/consumer-groups | jq '.[] | {group: .groupId, lag: .lag}'

# Check consumer group members
curl http://localhost:9644/v1/consumer-groups/GROUP_NAME | jq '.members'
```

**Causes and Solutions:**
| Cause                          | Solution                                    |
|--------------------------------|---------------------------------------------|
| Slow consumer processing       | Scale consumer instances; optimize code     |
| Uneven partition distribution  | Rebalance partitions; increase partition count |
| Network bottleneck             | Check consumer bandwidth; enable compression |
| Consumer crashes               | Fix consumer; check session timeout config  |
| Frequent rebalances            | Increase `session.timeout.ms`; fix flaky consumers |

### Slow Produce Latency

**Checks:**
```bash
# Check produce latency percentiles
curl http://localhost:9644/v1/metrics | grep produce_latency

# Check disk I/O
iostat -x 1

# Check for ISR shrinkage
curl http://localhost:9644/v1/topics/TOPIC_NAME | jq '.partitions[].isr'
```

**Solutions:**
-   Ensure `acks=1` if latency is more important than durability.
-   Increase I/O thread count if disk is bottlenecked.
-   Enable LZ4 or ZSTD compression to reduce disk I/O.
-   Move to faster storage (NVMe SSD).
-   Ensure `flush.ms` is not set too aggressively.
-   Check for compaction storms — schedule compaction during low-traffic
    periods.

### Frequent Leader Elections

**Symptoms:** `torrent_leader_elections_total` increasing rapidly.

**Checks:**
```bash
# Check election count
curl http://localhost:9644/v1/metrics | grep leader_elections

# Check Raft heartbeat status
curl http://localhost:9644/v1/debug/raft | jq '.peers[] | {id, last_heartbeat_ms}'
```

**Causes:**
-   Network latency between brokers.
-   GC pauses on broker JVM (not applicable — torrent-mq is C++).
-   Overloaded CPU (heartbeat processing delayed).
-   Raft election timeout too low.

**Solutions:**
```yaml
raft:
  heartbeat_interval_ms: 150   # Lower = more frequent heartbeats
  election_timeout_min_ms: 300 # Raise if elections are flapping
  election_timeout_max_ms: 600
```

### Out of Memory

**Symptoms:** Broker process killed by OOM killer, `dmesg` shows OOM
message.

**Checks:**
```bash
# Check memory usage
ps aux | grep torrentd
pmap $(pgrep torrentd) | tail -1

# Check open file descriptors
ls /proc/$(pgrep torrentd)/fd | wc -l
```

**Solutions:**
-   Increase system RAM or reduce broker memory footprint.
-   Reduce page cache size:

```yaml
storage:
  page_cache:
    max_bytes: 2147483648  # 2 GB
```

-   Reduce concurrent fetch requests:

```yaml
network:
  max_connections: 16384
  fetch:
    max_bytes: 10485760  # 10 MB
```

-   Check for consumer groups fetching from the beginning with large
    `fetch.max.bytes`.

## Log Analysis

### Broker Log Locations

```
/var/log/torrent/
├── torrentd.log            # Main broker log
├── torrentd-error.log      # Error-level logs only
├── audit.log               # Security audit log
├── raft.log                # Raft consensus log
└── connector-{name}.log    # Per-connector logs
```

### Log Levels

torrent-mq supports these log levels (increasing verbosity):

| Level   | Description                                         |
|---------|-----------------------------------------------------|
| `error` | Errors that require operator attention              |
| `warn`  | Potential issues that should be investigated        |
| `info`  | Normal operational events (startup, shutdown, etc.) |
| `debug` | Detailed operational information for troubleshooting |
| `trace` | Extremely verbose — all function calls and data     |

Change log level dynamically:

```bash
curl -X PUT http://localhost:9644/v1/admin/loglevel \
    -H "Content-Type: application/json" \
    -d '{"level": "debug", "duration_seconds": 300}'
# Automatically reverts to 'info' after 5 minutes
```

### Key Log Patterns

**Broker startup sequence:**
```
[INFO] Loading configuration from /etc/torrent/config.yaml
[INFO] Broker ID: 0, Rack: rack-1
[INFO] Starting Raft node with peers: [0,1,2]
[INFO] Raft node 0 starting as follower
[INFO] Recovering partition data...
[INFO] Recovered 12 partitions (0 corrupted segments)
[INFO] Listening on 0.0.0.0:9092 (plain), 0.0.0.0:9093 (tls)
[INFO] Broker 0 is ready
```

**Raft leader election:**
```
[INFO] Election timeout — starting election for term 5
[INFO] RequestVote sent to peers [1,2]
[INFO] Received vote from peer 1 (granted)
[INFO] Received vote from peer 2 (granted)
[INFO] Won election — becoming leader for term 5
```

**Partition under-replicated:**
```
[WARN] Partition my-topic:3 ISR shrunk from [0,1,2] to [0,1]
[WARN] Partition my-topic:3 is under-replicated (ISR: 2, RF: 3)
[INFO] Partition my-topic:3 ISR expanded to [0,1,2] — broker 2 caught up
```

**Segment corruption (auto-healing):**
```
[ERROR] CRC mismatch in segment 00000000000000100000.log at offset 1048576
[INFO] Marking segment 00000000000000100000.log as corrupted
[INFO] Fetching segment 00000000000000100000.log from replica broker 1
[INFO] Segment 00000000000000100000.log repaired successfully
```

**Disk space warnings:**
```
[WARN] Disk usage at 85% on /var/lib/torrent/data (12.3 GB free)
[ERROR] Disk usage at 95% — rejecting produce requests
```

### Log Rotation

Configure in `/etc/torrent/config.yaml`:

```yaml
logging:
  dir: /var/log/torrent
  max_file_size_mb: 100
  max_files: 10
  compress_rotated: true
```

### Shipping Logs to SIEM

```yaml
logging:
  syslog:
    enabled: true
    facility: local0
    format: rfc5424  # Structured logging for SIEM ingestion
  audit:
    syslog:
      enabled: true
      facility: local1
```

## Performance Tuning Checklist

### 1. Operating System

- [ ] Set `vm.swappiness=1` to minimize swap usage.
- [ ] Set `vm.dirty_ratio=5` and `vm.dirty_background_ratio=3` for
      smoother I/O.
- [ ] Mount data directory on XFS or ext4 with `noatime,nodiratime`.
- [ ] Set `net.core.rmem_max=134217728` and
      `net.core.wmem_max=134217728` (128 MB socket buffers).
- [ ] Enable `tcp_tw_reuse` for faster connection recycling.
- [ ] Disable transparent huge pages (THP):
      `echo never > /sys/kernel/mm/transparent_hugepage/enabled`
- [ ] Set CPU governor to `performance`.

### 2. Storage

- [ ] Use NVMe SSDs for data directories.
- [ ] Separate data directory from OS disk.
- [ ] Align partition to 4K sector boundaries.
- [ ] Use `deadline` or `mq-deadline` I/O scheduler.
- [ ] Set `storage.log.segment.bytes` to 1 GB (default is fine).
- [ ] Set `storage.page_cache.max_bytes` to ~25% of RAM.

### 3. Network

- [ ] Use dedicated network interface for inter-broker traffic.
- [ ] Enable jumbo frames (MTU 9000) if supported.
- [ ] Set `network.num_io_threads` to number of CPU cores (max 16).
- [ ] Set `network.num_worker_threads` to 2x CPU cores.
- [ ] Enable TLS session resumption for reduced handshake overhead.

### 4. Topic Configuration

- [ ] Choose partition count based on expected throughput:
    - 1-10 MB/s: 3-6 partitions
    - 10-100 MB/s: 6-24 partitions
    - 100+ MB/s: 24-100+ partitions
- [ ] Set `compression.type=lz4` for best throughput/latency balance.
- [ ] Set appropriate `retention.ms` — don't retain more than needed.
- [ ] Use keyed messages and log compaction for changelog-style topics.

### 5. Producer Tuning

- [ ] Use `acks=1` for throughput, `acks=all` for durability.
- [ ] Batch messages: `batch.size=16384` or higher.
- [ ] Set `linger.ms=5` for better batching without excessive latency.
- [ ] Use asynchronous producers with reasonable buffer limits.
- [ ] Set `max.in.flight.requests.per.connection=5` for pipelining.

### 6. Consumer Tuning

- [ ] Increase `fetch.min.bytes` to reduce small-fetch overhead.
- [ ] Set `fetch.max.wait.ms=500` for batching.
- [ ] Process messages in batches, not one at a time.
- [ ] Use multiple consumer instances in a group for parallelism.
- [ ] Set `session.timeout.ms` high enough to avoid false timeouts
      during GC or CPU spikes.

### 7. Raft Tuning

- [ ] `raft.heartbeat_interval_ms=150` (default is good).
- [ ] `raft.election_timeout_min_ms=150`, `raft.election_timeout_max_ms=300`.
- [ ] Enable `raft.pre_vote=true` to reduce unnecessary elections.
- [ ] `raft.snapshot.threshold_entries=10000` — lower for large clusters.

### Performance Test Checklist

Run these before and after any tuning change:

```bash
# 1. Measure baseline throughput
torrentctl perf test-produce --topic perf-test --messages 1000000

# 2. Check latency percentiles
curl -s http://localhost:9644/v1/metrics | grep produce_latency_p99

# 3. Verify no consumer lag
torrentctl consumer-groups lag --group PERF_TEST_GROUP

# 4. Monitor disk I/O during test
iostat -x 1 10

# 5. Verify ISR stability
watch -n 2 "torrentctl admin topics --under-replicated"
```
