# torrent-mq Deployment Guide

## System Requirements

### Minimum
- CPU: 2 cores
- RAM: 2 GB
- Disk: 10 GB (SSD recommended for production)
- OS: Linux kernel 5.4+ (x86_64 or ARM64)
- Network: 1 Gbps

### Recommended (Production)
- CPU: 8+ cores
- RAM: 16+ GB
- Disk: 100+ GB NVMe SSD
- OS: Linux kernel 6.1+ (x86_64)
- Network: 10 Gbps

## Dependencies

### Build Dependencies
- GCC 13+ or Clang 16+ (C++20 support required)
- CMake 3.20+
- Boost 1.91+ (filesystem, thread components)
- OpenSSL 3.0+
- gRPC and Protobuf (optional, for inter-broker gRPC)
- RocksDB 7.0+ (optional, for persistent metadata)

### Runtime Dependencies
- Linux kernel with AIO support
- glibc 2.35+
- libstdc++ (from GCC 13+)

## Installation

### From Source

```bash
# Clone repository
git clone https://github.com/torrent-mq/torrent-mq.git
cd torrent-mq

# Install build dependencies (Debian/Ubuntu)
sudo apt-get install -y build-essential cmake libboost-all-dev \
  libssl-dev libgrpc++-dev libprotobuf-dev librocksdb-dev

# Install build dependencies (Arch/Parabola)
sudo pacman -S --needed base-devel cmake boost openssl grpc protobuf rocksdb

# Configure and build
mkdir build && cd build
cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DTORRENT_USE_GRPC=ON \
  -DTORRENT_USE_ROCKSDB=ON \
  -DTORRENT_USE_PROMETHEUS=ON \
  -DTORRENT_BUILD_TESTS=ON \
  -DTORRENT_BUILD_CLI=ON

make -j$(nproc)

# Install
sudo make install
```

### Using Pre-built Binary

```bash
# Download latest release
wget https://github.com/torrent-mq/torrent-mq/releases/latest/download/torrentd-linux-amd64.tar.gz
tar xzf torrentd-linux-amd64.tar.gz
sudo cp torrentd /usr/local/bin/
```

## Configuration

### Single Node Setup

Create `/etc/torrent/config.yaml`:

```yaml
broker:
  id: 0

listeners:
  plain:
    port: 9092

admin:
  port: 9644

metrics:
  port: 9090

storage:
  log:
    dir: /var/lib/torrent/data

cluster:
  id: my-cluster
```

Start:

```bash
sudo mkdir -p /var/lib/torrent/data
sudo torrentd --config-file /etc/torrent/config.yaml
```

### Multi-Node Cluster Setup

**Node 1** (controller candidate):

```yaml
broker:
  id: 0
  rack: rack-1

listeners:
  plain:
    host: 10.0.1.1
    port: 9092

storage:
  log:
    dir: /data/torrent

cluster:
  id: production-cluster
  seed_servers:
    - 10.0.1.1:9092
    - 10.0.1.2:9092
    - 10.0.1.3:9092
```

**Node 2:**

```yaml
broker:
  id: 1
  rack: rack-2

listeners:
  plain:
    host: 10.0.1.2
    port: 9092

storage:
  log:
    dir: /data/torrent

cluster:
  id: production-cluster
  seed_servers:
    - 10.0.1.1:9092
    - 10.0.1.2:9092
    - 10.0.1.3:9092
```

**Node 3:**

```yaml
broker:
  id: 2
  rack: rack-3

listeners:
  plain:
    host: 10.0.1.3
    port: 9092

storage:
  log:
    dir: /data/torrent

cluster:
  id: production-cluster
  seed_servers:
    - 10.0.1.1:9092
    - 10.0.1.2:9092
    - 10.0.1.3:9092
```

Start on each node:

```bash
sudo mkdir -p /data/torrent
sudo torrentd --config-file /etc/torrent/config.yaml
```

### TLS Configuration

Generate certificates:

```bash
# Generate CA
openssl req -new -x509 -days 3650 -nodes \
  -out /etc/torrent/certs/ca.crt \
  -keyout /etc/torrent/certs/ca.key \
  -subj "/CN=torrent-mq-ca"

# Generate broker certificate
openssl req -new -nodes \
  -out /etc/torrent/certs/server.csr \
  -keyout /etc/torrent/certs/server.key \
  -subj "/CN=broker1.torrent-mq.local"

openssl x509 -req -days 365 -in /etc/torrent/certs/server.csr \
  -CA /etc/torrent/certs/ca.crt -CAkey /etc/torrent/certs/ca.key \
  -CAcreateserial -out /etc/torrent/certs/server.crt

chmod 600 /etc/torrent/certs/*.key
```

Configure TLS:

```yaml
listeners:
  tls:
    port: 9093

security:
  tls:
    enabled: true
    certificate_path: /etc/torrent/certs/server.crt
    key_path: /etc/torrent/certs/server.key
    ca_path: /etc/torrent/certs/ca.crt
    require_client_auth: false
```

### SASL/SCRAM Configuration

```yaml
security:
  sasl:
    enabled: true
    mechanisms:
      - SCRAM-SHA-256
      - SCRAM-SHA-512
```

Create users:

```bash
torrentctl config set sasl.users.admin.password "secure-password"
torrentctl config set sasl.users.admin.mechanism "SCRAM-SHA-256"
```

## Systemd Service

Create `/etc/systemd/system/torrentd.service`:

```ini
[Unit]
Description=torrent-mq Message Broker
Documentation=https://github.com/torrent-mq/torrent-mq
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=torrent
Group=torrent
ExecStart=/usr/local/bin/torrentd --config-file /etc/torrent/config.yaml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=5
LimitNOFILE=1048576
LimitNPROC=65536
LimitMEMLOCK=infinity
MemoryMax=8G
TasksMax=4096
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/torrent /data/torrent
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo useradd -r -s /bin/false torrent
sudo systemctl daemon-reload
sudo systemctl enable torrentd
sudo systemctl start torrentd
sudo systemctl status torrentd
```

## Performance Tuning

### Kernel Parameters

Add to `/etc/sysctl.d/99-torrent.conf`:

```
# Increase max file descriptors
fs.file-max = 1048576

# Increase network buffer sizes
net.core.rmem_max = 134217728
net.core.wmem_max = 134217728
net.core.rmem_default = 16777216
net.core.wmem_default = 16777216

# TCP tuning
net.ipv4.tcp_rmem = 4096 87380 134217728
net.ipv4.tcp_wmem = 4096 65536 134217728
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fastopen = 3

# Increase socket backlog
net.core.somaxconn = 65536
net.core.netdev_max_backlog = 65536

# VM tuning
vm.swappiness = 1
vm.dirty_ratio = 20
vm.dirty_background_ratio = 5
```

Apply:

```bash
sudo sysctl -p /etc/sysctl.d/99-torrent.conf
```

### Broker Tuning

```yaml
network:
  num_io_threads: 8          # Match CPU cores for I/O
  num_worker_threads: 16     # Match CPU cores for processing
  max_connections: 131072
  socket:
    send_buffer:
      bytes: 16777216
    recv_buffer:
      bytes: 16777216

storage:
  log:
    segment:
      bytes: 1073741824      # 1GB segments
      index_interval_bytes: 4096

raft:
  heartbeat_interval_ms: 150
  election_timeout_min_ms: 150
  election_timeout_max_ms: 300
```

### Disk Configuration

- Use separate disks for data and OS
- XFS or ext4 with `noatime` mount option
- Ensure write cache is enabled (with battery backup for RAID)
- Monitor disk usage and set alerts at 70%
- Use RAID 10 for production (RAID 0 for performance-only workloads)

## Monitoring

### Prometheus

Add to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'torrent-mq'
    static_configs:
      - targets:
        - broker1:9090
        - broker2:9090
        - broker3:9090
```

### Key Metrics to Monitor

| Metric | Alert Threshold |
|---|---|
| `torrent_active_connections` | > 80% of `max_connections` |
| `torrent_disk_usage_bytes` | > 80% of disk capacity |
| `torrent_under_replicated_partitions` | > 0 for more than 5 minutes |
| `torrent_leader_elections_total` | > 5 per hour |
| `torrent_requests_failed_total` | > 1% of requests |
| `process_open_fds` | > 80% of ulimit |

### Grafana Dashboard

Import the provided dashboard JSON from `monitoring/grafana-dashboard.json`.

## Backup and Recovery

### Backup Strategy

1. **Metadata backup**: Copy `raft/` directory for Raft state
2. **Data backup**: Use filesystem snapshots (LVM, ZFS) for consistent backups
3. **Tiered storage**: Configure S3 archival for automatic offsite backup

### Recovery

**Single node failure:**
```bash
# Restart the broker — it will recover from local segments
sudo systemctl restart torrentd
```

**Data directory corruption:**
```bash
# Stop broker
sudo systemctl stop torrentd

# Restore from backup or tiered storage
# Then restart
sudo systemctl start torrentd
```

**Full cluster recovery:**
1. Restore data directory on each node from backup
2. Start all brokers simultaneously
3. Verify cluster health: `torrentctl cluster health`

## Upgrading

### Rolling Upgrade (Zero Downtime)

1. Upgrade one broker at a time
2. Before stopping: verify it's not the controller
3. Stop broker, upgrade binary, restart
4. Wait for broker to rejoin cluster (check health)
5. Repeat for next broker

```bash
# Check if broker is controller
torrentctl cluster describe | grep controller

# If this broker is controller, trigger controller election first
# Then proceed with upgrade
sudo systemctl stop torrentd
sudo cp torrentd-new /usr/local/bin/torrentd
sudo systemctl start torrentd
torrentctl cluster health
```

## Troubleshooting

### Common Issues

**Broker fails to start:**
```bash
# Check logs
journalctl -u torrentd -f

# Verify config
torrentd --config-file /etc/torrent/config.yaml --validate

# Check port availability
ss -tlnp | grep 9092
```

**Cluster not forming:**
```bash
# Verify network connectivity between brokers
ping 10.0.1.2
telnet 10.0.1.2 9092

# Check seed server list
torrentctl cluster describe

# Verify cluster ID matches on all nodes
grep cluster.id /etc/torrent/config.yaml
```

**High disk usage:**
```bash
# Check retention settings
torrentctl config get storage.retention.ms
torrentctl config get storage.retention.bytes

# Check topic sizes
torrentctl topic list --with-sizes

# Manually trigger retention
torrentctl config set storage.retention.force_run true
```

**Consumer lag:**
```bash
# Check consumer group offsets
torrentctl consumer describe my-group

# Check topic latest offsets
torrentctl topic describe my-topic
```

## Security Hardening

### Network Security

- Use TLS for all client and inter-broker communication
- Place brokers in a private network (VPC)
- Use firewall rules to restrict access to broker ports
- Enable mTLS for inter-broker communication
- Use SASL/SCRAM for client authentication

### Access Control

```yaml
# Example ACL rules
security:
  acl_rules:
    - principal: "User:admin"
      operation: ALL
      resource: "*"
      permission: ALLOW

    - principal: "User:producer-app"
      operation: WRITE
      resource: "Topic:orders"
      permission: ALLOW

    - principal: "User:consumer-app"
      operation: READ
      resource: "Topic:orders"
      permission: ALLOW
      host: "10.0.2.0/24"
```

### Audit Logging

Enable audit logging for compliance:

```yaml
security:
  audit:
    enabled: true
    log_auth_events: true
    log_admin_events: true
    log_security_events: true
```

## Capacity Planning

### Throughput Guidelines

| Message Size | Messages/sec per broker | MB/sec per broker |
|---|---|---|
| 100 bytes | 500,000 | 50 |
| 1 KB | 200,000 | 200 |
| 10 KB | 50,000 | 500 |
| 100 KB | 5,000 | 500 |
| 1 MB | 500 | 500 |

### Storage Guidelines

- 1 TB disk ≈ 1 billion 1KB messages
- With replication factor 3: 1 TB disk ≈ 333 million 1KB messages
- Plan for 2x headroom for compaction and temporary files
- Monitor and set alerts at 70% disk usage

### Memory Guidelines

- Base: 512 MB for broker process
- + 1 GB per 10,000 partitions
- + Page cache: OS will use available RAM for disk cache
- Plan for total system RAM = broker RAM + OS page cache (2-4 GB)
