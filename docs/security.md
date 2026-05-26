# torrent-mq Security Guide

## Security Architecture Overview

torrent-mq implements a multi-layer security model with defense-in-depth
principles:

```
┌─────────────────────────────────────────────────────────────┐
│                       Client / Admin                        │
├─────────────────────────────────────────────────────────────┤
│  Transport Security (TLS 1.2/1.3, mTLS)                    │
├─────────────────────────────────────────────────────────────┤
│  Authentication (SASL: SCRAM, GSSAPI, OAUTHBEARER, PLAIN)  │
├─────────────────────────────────────────────────────────────┤
│  Authorization (ACL engine with wildcard matching)          │
├─────────────────────────────────────────────────────────────┤
│  Audit Logging (all auth/admin/security events)             │
├─────────────────────────────────────────────────────────────┤
│  Encryption at Rest (AES-256-GCM, per-segment keys)         │
├─────────────────────────────────────────────────────────────┤
│  Network Security (firewall rules, VPC isolation)           │
└─────────────────────────────────────────────────────────────┘
```

Every client connection passes through authentication and every request
through authorization. All security-relevant events are logged to the
audit trail.

### Security Components

| Component        | File/Module                                       | Responsibility                        |
|------------------|---------------------------------------------------|---------------------------------------|
| SASL Provider    | `src/torrent/security/sasl_provider.cpp`           | Authentication handshake              |
| Auth Manager     | `src/torrent/security/auth_manager.cpp`            | Session management, credential store  |
| ACL Engine       | `src/torrent/security/acl_engine.cpp`              | Access control lists                  |
| TLS Manager      | `src/torrent/security/tls_manager.cpp`             | Certificate management, TLS config    |
| Audit Log        | `src/torrent/security/audit_log.cpp`               | Security event logging                |
| Encryption at Rest| `src/torrent/storage/encryption.cpp`              | Segment-level AES encryption          |

## Authentication Methods

### SASL/SCRAM

SCRAM (Salted Challenge Response Authentication Mechanism) is the
recommended authentication method for torrent-mq. It provides mutual
authentication without sending passwords in cleartext.

**Supported variants:**
- SCRAM-SHA-256 (recommended)
- SCRAM-SHA-512

**Configuration:**

```yaml
security:
  sasl:
    enabled: true
    mechanisms:
      - SCRAM-SHA-256
      - SCRAM-SHA-512
    scram_iterations: 15000  # PBKDF2 iteration count
```

**User management:**

```bash
# Create a user
torrentctl acl create-user \
    --username alice \
    --password 's3cur3-p4ssw0rd' \
    --mechanism SCRAM-SHA-256

# List users
torrentctl acl list-users

# Delete a user
torrentctl acl delete-user alice

# Rotate credentials
torrentctl acl rotate-credentials alice --new-password 'n3w-p4ssw0rd'
```

**How SCRAM works in torrent-mq:**

1.  Client sends SASL handshake request with mechanism `SCRAM-SHA-256`.
2.  Server responds with `client-first-message` containing nonce.
3.  Client sends `client-final-message` with `client-proof`
    (HMAC-based).
4.  Server verifies the proof, sends `server-final-message` with
    `server-signature`.
5.  Client verifies the signature — mutual authentication complete.
6.  Session is established with the authenticated principal.

**SCRAM stored credentials format:**

```
SCRAM-SHA-256$15000:<salt>$<stored_key>$<server_key>
```

Salted with random 32-byte salt. Iterations configurable (default
15,000). The plaintext password is never stored.

### mTLS (Mutual TLS)

Mutual TLS provides certificate-based mutual authentication without SASL.

**Configuration:**

```yaml
security:
  tls:
    enabled: true
    certificate_path: /etc/torrent/certs/server.crt
    key_path: /etc/torrent/certs/server.key
    ca_path: /etc/torrent/certs/ca.crt
    require_client_auth: true  # Enables mTLS
    min_version: "1.2"
    ciphers: "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384"
```

**Certificate requirements for mTLS:**

-   Server certificate: Must have the broker's hostname in SAN
    (Subject Alternative Name) or CN.
-   Client certificate: Must be signed by the CA trusted by the server.
-   The client's certificate CN or SAN is used as the principal name for
    authorization.

**Client configuration (librdkafka):**

```properties
security.protocol=SSL
ssl.ca.location=/etc/torrent/certs/ca.crt
ssl.certificate.location=/etc/torrent/certs/client.crt
ssl.key.location=/etc/torrent/certs/client.key
ssl.key.password=keypassword
```

### Kerberos (GSSAPI)

For organizations using Kerberos/Active Directory for centralized
authentication.

**Configuration:**

```yaml
security:
  sasl:
    enabled: true
    mechanisms:
      - GSSAPI
    kerberos:
      service_name: torrent
      principal: torrent/broker1.example.com@REALM.COM
      keytab: /etc/torrent/torrent.keytab
```

**Keytab generation:**

```bash
# On the KDC/domain controller
kadmin -q "addprinc -randkey torrent/broker1.example.com@REALM.COM"
kadmin -q "ktadd -k /tmp/torrent.keytab torrent/broker1.example.com@REALM.COM"
# Copy /tmp/torrent.keytab to /etc/torrent/torrent.keytab on the broker
```

**Client configuration:**

```properties
security.protocol=SASL_PLAINTEXT  # or SASL_SSL for encryption
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=torrent
```

### OAuth2 (OAUTHBEARER)

For integrating with OAuth2/OIDC identity providers.

**Configuration:**

```yaml
security:
  sasl:
    enabled: true
    mechanisms:
      - OAUTHBEARER
    oauth2:
      jwks_url: https://auth.example.com/.well-known/jwks.json
      issuer: https://auth.example.com
      audience: torrent-mq
      token_validation:
        clock_skew_seconds: 30
```

The broker validates JWT tokens against the JWKS endpoint. Tokens must:
- Be signed with RS256, ES256, or HS256.
- Have a valid `iss` claim matching `issuer`.
- Have `aud` claim containing `audience`.
- Not be expired (`exp` claim).
- Have a `sub` claim that becomes the principal.

**Client configuration:**

```properties
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.oauthbearer.config="scope=requiredScope clientId=myclient \
  clientSecret=mysecret tokenEndpoint=https://auth.example.com/token"
```

### PLAIN (Development Only)

**WARNING:** PLAIN sends credentials in cleartext. Only use with TLS
encryption and never in production unless absolutely required for legacy
compatibility.

```yaml
security:
  sasl:
    enabled: true
    mechanisms:
      - PLAIN
    # Require TLS for PLAIN
    require_tls_for_plain: true
```

## Authorization: ACL Model

### ACL Resource Types

torrent-mq ACLs protect these resource types:

| Resource Type      | Pattern Format                    | Operations                  |
|--------------------|-----------------------------------|-----------------------------|
| Topic              | `Topic:<name>`                    | Read, Write, Describe, Alter, Delete, DescribeConfigs, AlterConfigs |
| Consumer Group     | `Group:<name>`                    | Read, Describe, Delete      |
| Transactional ID   | `TransactionalId:<id>`            | Write, Describe             |
| Cluster            | `Cluster:<name>`                  | Describe, Alter, DescribeConfigs, AlterConfigs, ClusterAction, IdempotentWrite |
| Delegation Token   | `DelegationToken:<id>`            | Describe                     |

### ACL Operations

| Operation         | Description                                      |
|-------------------|--------------------------------------------------|
| `Read`            | Consume from a topic; read consumer group offsets|
| `Write`           | Produce to a topic; commit offsets; transactions |
| `Create`          | Create topics                                    |
| `Delete`          | Delete topics                                    |
| `Alter`           | Alter topic configuration, partition count       |
| `Describe`        | Describe topics, consumer groups, cluster        |
| `DescribeConfigs` | Read topic/broker configuration                  |
| `AlterConfigs`    | Modify topic/broker configuration                |
| `ClusterAction`   | Cluster-level admin operations                   |
| `IdempotentWrite` | Idempotent produce (requires cluster-level)      |
| `All`             | All operations on the resource                   |

### ACL Rule Format

```
Principal P is [Allowed|Denied] Operation O on Resource R from Host H
```

Rules are evaluated in specificity order:
1.  Most specific principal match.
2.  Most specific host match.
3.  Most specific resource match.
4.  Most specific operation match.

`Deny` takes precedence over `Allow` when both match at the same
specificity level.

### Managing ACLs

**Add a rule via torrentctl:**

```bash
# Allow alice to read from topic 'orders'
torrentctl acl add --allow \
    --principal User:alice \
    --operation Read \
    --topic orders \
    --host '*'

# Allow all users in group 'producers' to write to any topic starting with 'prod-'
torrentctl acl add --allow \
    --principal 'User:*' \
    --group producers \
    --operation Write \
    --topic 'prod-*' \
    --resource-pattern-type prefixed \
    --host '10.0.0.*'

# Deny bob from deleting topics
torrentctl acl add --deny \
    --principal User:bob \
    --operation Delete \
    --topic '*' \
    --host '*'

# Allow admin service account all operations on all resources
torrentctl acl add --allow \
    --principal User:admin-svc \
    --operation All \
    --topic '*' \
    --group '*' \
    --host '*'
```

**List rules:**

```bash
torrentctl acl list
torrentctl acl list --principal User:alice
torrentctl acl list --topic orders
```

**Delete rules:**

```bash
torrentctl acl remove --principal User:alice --operation Read --topic orders
```

### ACL File (Static Configuration)

ACLs can also be defined in a YAML file:

```yaml
# /etc/torrent/acl.yaml
rules:
  - principal: "User:admin-svc"
    host: "*"
    resource: "Topic:*"
    operation: "All"
    permission: "Allow"

  - principal: "User:*"
    host: "10.0.0.*"
    resource: "Topic:orders"
    operation: "Read"
    permission: "Allow"

  - principal: "User:*"
    host: "*"
    resource: "Topic:*"
    operation: "Read"
    permission: "Deny"  # Default deny for reads from outside 10.0.0.*
```

### Super Users

Super users bypass all ACL checks. Configure in YAML:

```yaml
security:
  super_users:
    - User:admin
    - User:torrent-monitor
```

**Use super users sparingly** — they have unrestricted access.

### Principal Format

Principals follow the format `Type:Name`:

| Type     | Example                    | Source                   |
|----------|----------------------------|--------------------------|
| `User`   | `User:alice`               | SASL/SCRAM username      |
| `User`   | `User:CN=alice,O=Corp`     | mTLS certificate CN      |
| `Group`  | `Group:producers`          | Kerberos/LDAP group      |

Wildcards are supported:
- `User:*` matches all users.
- `User:*-svc` matches users ending in `-svc`.

### Delegation Tokens

Delegation tokens allow lightweight, time-limited authentication without
full SASL handshake. Useful for worker processes that need temporary
access.

**Create a token:**

```bash
torrentctl delegation-token create \
    --owner User:alice \
    --max-lifetime-ms 86400000 \  # 24 hours
    --renewers User:bob
```

**Use a token (client):**

```properties
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule \
  required username="alice" password="DELEGATION_TOKEN_HMAC";
```

**Token lifecycle:**
1.  Owner authenticates via SASL and requests a delegation token.
2.  Broker generates an HMAC-SHA256 token, stores the mapping.
3.  Token is shared with workers.
4.  Workers authenticate using the token (no full SASL needed).
5.  Token expires after `max_lifetime_ms` or can be revoked early.

**Revoke a token:**

```bash
torrentctl delegation-token revoke --hmac TOKEN_HMAC
```

## Encryption

### TLS Configuration

**Production-grade TLS configuration:**

```yaml
security:
  tls:
    enabled: true
    certificate_path: /etc/torrent/certs/server.crt
    key_path: /etc/torrent/certs/server.key
    ca_path: /etc/torrent/certs/ca.crt
    require_client_auth: true
    min_version: "1.2"  # or "1.3"
    ciphers: "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:\
              ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305"
    # Optional: only for TLS 1.3
    ciphersuites: "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256"
    # Session resumption for performance
    session_cache_size: 10000
    session_timeout_seconds: 300
```

### Recommended Cipher Suites

**TLS 1.2 (in order of preference):**

| Cipher Suite                                                   | Notes                          |
|----------------------------------------------------------------|--------------------------------|
| `ECDHE-ECDSA-AES256-GCM-SHA384`                                | Best performance + security    |
| `ECDHE-RSA-AES256-GCM-SHA384`                                  | RSA key compatibility          |
| `ECDHE-ECDSA-CHACHA20-POLY1305`                                | Good on CPUs without AES-NI    |
| `ECDHE-RSA-CHACHA20-POLY1305`                                  | RSA variant of ChaCha20        |

**TLS 1.3 ciphersuites:**

| Ciphersuite                       |
|-----------------------------------|
| `TLS_AES_256_GCM_SHA384`          |
| `TLS_CHACHA20_POLY1305_SHA256`    |
| `TLS_AES_128_GCM_SHA256`          |

**Avoid:** CBC-mode ciphers, RC4, 3DES, EXPORT ciphers, NULL ciphers,
anonymous Diffie-Hellman.

### Encryption at Rest

torrent-mq supports per-segment AES-256-GCM encryption for data at rest.

**Configuration:**

```yaml
storage:
  encryption:
    enabled: true
    key_file: /etc/torrent/encryption.key  # 32-byte AES-256 key
    algorithm: AES-256-GCM
    # Optional: key rotation
    key_rotation:
      enabled: true
      old_key_file: /etc/torrent/encryption.old.key
      rotation_period_ms: 2592000000  # 30 days
```

**Key generation:**

```bash
# Generate a 256-bit key
openssl rand -hex 32 > /etc/torrent/encryption.key
chmod 600 /etc/torrent/encryption.key
chown torrent:torrent /etc/torrent/encryption.key
```

**How it works:**

1.  Each segment file is encrypted with AES-256-GCM.
2.  A random 12-byte IV (nonce) is generated per segment and stored in
    the segment header.
3.  Authentication tags (16 bytes per block) are stored alongside
    encrypted data, providing integrity verification.
4.  During key rotation, new segments use the new key; old segments
    retain the old key for reads. The `old_key_file` enables reading
    legacy segments.
5.  Keys are loaded into memory at startup. The key file should be
    protected with filesystem permissions (0600).

**Performance impact:**
- AES-NI hardware acceleration on modern CPUs makes the overhead
  negligible (~5% throughput reduction).
- Without AES-NI, expect ~20-30% reduction.

### Inter-Broker Encryption

Inter-broker traffic (Raft consensus, replication) must also be
encrypted:

```yaml
security:
  inter_broker:
    protocol: tls  # plain, tls, sasl_ssl
    sasl_mechanism: SCRAM-SHA-256  # if sasl_ssl
```

All inter-broker communication goes through the same TLS layer as
client-broker communication.

## Network Security

### Firewall Rules

Minimum required ports per broker:

| Port  | Protocol | Purpose               | External Access? |
|-------|----------|-----------------------|------------------|
| 9092  | TCP      | Kafka client protocol | Yes (clients)    |
| 9093  | TCP      | Kafka client TLS      | Yes (clients)    |
| 9644  | TCP      | Admin REST API        | No (internal)    |
| 9090  | TCP      | Prometheus metrics    | No (monitoring)  |
| 9092  | TCP      | Inter-broker (if using same port) | Broker-to-broker only |

**Example iptables rules:**

```bash
# Allow client access from application subnet
iptables -A INPUT -p tcp --dport 9092 -s 10.0.1.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 9093 -s 10.0.1.0/24 -j ACCEPT

# Allow inter-broker traffic
iptables -A INPUT -p tcp --dport 9092 -s 10.0.0.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 9093 -s 10.0.0.0/24 -j ACCEPT

# Allow admin and metrics from management subnet
iptables -A INPUT -p tcp --dport 9644 -s 10.0.2.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 9090 -s 10.0.2.0/24 -j ACCEPT

# Deny all other traffic
iptables -A INPUT -p tcp -m multiport --dports 9092,9093,9644,9090 -j DROP
```

### VPC / Network Design

Recommended architecture:

```
┌─────────────────────────────────────────────────────────┐
│                     VPC (10.0.0.0/16)                    │
│                                                         │
│  ┌───────────────────┐   ┌───────────────────────────┐ │
│  │  Public Subnet     │   │  Private Subnet            │ │
│  │  (10.0.1.0/24)    │   │  (10.0.0.0/24)            │ │
│  │                    │   │                            │ │
│  │  ┌──────────────┐ │   │  ┌──────┐ ┌──────┐ ┌────┐│ │
│  │  │ Load Balancer│ │   │  │Broker│ │Broker│ │Brkr││ │
│  │  │ (for clients)│ │   │  │  0   │ │  1   │ │ 2  ││ │
│  │  └──────────────┘ │   │  └──────┘ └──────┘ └────┘│ │
│  └───────────────────┘   └───────────────────────────┘ │
│                                                         │
│  ┌───────────────────┐                                 │
│  │  Management Subnet │                                 │
│  │  (10.0.2.0/24)    │                                 │
│  │  Monitoring, Admin │                                 │
│  └───────────────────┘                                 │
└─────────────────────────────────────────────────────────┘
```

**Principles:**
- Brokers in private subnets — no direct internet access.
- Clients connect via load balancer or directly to broker IPs (within VPC).
- Admin and metrics endpoints restricted to management subnet.
- Network ACLs and security groups enforce least-privilege access.
- VPC flow logs enabled for network auditing.

## Certificate Management

### Certificate Generation

**Self-signed CA (for internal use):**

```bash
# Generate CA key and certificate
openssl req -new -x509 -days 3650 -nodes \
    -out /etc/torrent/certs/ca.crt \
    -keyout /etc/torrent/certs/ca.key \
    -subj "/CN=torrent-mq-ca"

# Generate broker key and CSR
openssl req -new -nodes \
    -out /etc/torrent/certs/broker.csr \
    -keyout /etc/torrent/certs/broker.key \
    -subj "/CN=broker0.torrent.internal" \
    -addext "subjectAltName=DNS:broker0.torrent.internal,DNS:broker0,IP:10.0.0.1"

# Sign with CA
openssl x509 -req -days 365 \
    -in /etc/torrent/certs/broker.csr \
    -out /etc/torrent/certs/broker.crt \
    -CA /etc/torrent/certs/ca.crt \
    -CAkey /etc/torrent/certs/ca.key \
    -CAcreateserial \
    -copy_extensions copy

# Generate client certificate
openssl req -new -nodes \
    -out /etc/torrent/certs/client.csr \
    -keyout /etc/torrent/certs/client.key \
    -subj "/CN=client-app"

openssl x509 -req -days 365 \
    -in /etc/torrent/certs/client.csr \
    -out /etc/torrent/certs/client.crt \
    -CA /etc/torrent/certs/ca.crt \
    -CAkey /etc/torrent/certs/ca.key
```

### Certificate Rotation

Certificates should be rotated before expiration. torrent-mq supports
hot-reloading of certificates:

```bash
# Replace certificate files
cp new-server.crt /etc/torrent/certs/server.crt
cp new-server.key /etc/torrent/certs/server.key

# Trigger reload (no downtime)
curl -X POST http://localhost:9644/v1/admin/reload-tls
```

New connections will use the new certificate. Existing connections
continue with the old certificate until they disconnect.

**Rotation procedure:**

1.  Generate new certificates 30 days before expiration.
2.  Deploy to all brokers.
3.  Trigger reload on each broker (one at a time).
4.  Verify new certificate is served:

```bash
openssl s_client -connect broker0:9093 -servername broker0 \
    2>/dev/null | openssl x509 -noout -dates
```

5.  Remove old certificates after all brokers are updated.

### Certificate Revocation

**Using CRL (Certificate Revocation List):**

```yaml
security:
  tls:
    crl_path: /etc/torrent/certs/ca.crl
    crl_refresh_interval_seconds: 3600
```

**Update CRL:**

```bash
# Generate CRL
openssl ca -gencrl -out /etc/torrent/certs/ca.crl \
    -config /etc/ssl/openssl.cnf
```

**Using OCSP (Online Certificate Status Protocol):**

```yaml
security:
  tls:
    ocsp:
      enabled: true
      responder_url: http://ocsp.example.com
      stapling: true  # OCSP stapling for reduced latency
```

## Audit Logging

### Configuration

```yaml
security:
  audit:
    enabled: true
    log_path: /var/log/torrent/audit.log
    max_file_size_mb: 100
    max_files: 30
    # Forward to syslog for SIEM
    syslog:
      enabled: true
      facility: local1
    # Event filtering
    events:
      authentication: true
      authorization: true
      admin_operations: true
      config_changes: true
      security_events: true
```

### Audit Log Format

Audit log entries are JSON for easy parsing by SIEM tools:

```json
{
  "timestamp": "2026-05-26T02:20:00.123Z",
  "event_type": "AUTHENTICATION",
  "principal": "User:alice",
  "source_ip": "10.0.1.50",
  "source_port": 54321,
  "broker_id": 0,
  "outcome": "SUCCESS",
  "details": {
    "mechanism": "SCRAM-SHA-256",
    "session_id": "abc123def456"
  }
}
```

```json
{
  "timestamp": "2026-05-26T02:20:01.456Z",
  "event_type": "AUTHORIZATION",
  "principal": "User:alice",
  "source_ip": "10.0.1.50",
  "broker_id": 0,
  "outcome": "DENIED",
  "details": {
    "resource": "Topic:financial-data",
    "operation": "Read",
    "acl_match": "default-deny"
  }
}
```

```json
{
  "timestamp": "2026-05-26T02:20:05.789Z",
  "event_type": "ADMIN_OPERATION",
  "principal": "User:admin",
  "source_ip": "10.0.2.10",
  "broker_id": 0,
  "outcome": "SUCCESS",
  "details": {
    "operation": "CREATE_TOPIC",
    "resource": "Topic:new-orders",
    "parameters": {
      "partitions": 6,
      "replication_factor": 3
    }
  }
}
```

### Event Types

| Event Type           | Description                                          |
|----------------------|------------------------------------------------------|
| `AUTHENTICATION`     | SASL/mTLS authentication attempt (success or failure)|
| `AUTHORIZATION`      | ACL check result (allow or deny)                     |
| `ADMIN_OPERATION`    | Admin API operations (create topic, alter config)    |
| `CONFIG_CHANGE`      | Dynamic configuration changes                        |
| `SECURITY_EVENT`     | Certificate reload, key rotation, token revocation   |
| `DELEGATION_TOKEN`   | Token creation, renewal, revocation                  |
| `ACL_CHANGE`         | ACL rule modifications                               |

### SIEM Integration

**Syslog forwarding:**

```yaml
security:
  audit:
    syslog:
      enabled: true
      facility: local1
      format: rfc5424
```

**Log shipping via filebeat/fluentd:**

```yaml
# filebeat config excerpt
filebeat.inputs:
  - type: log
    paths:
      - /var/log/torrent/audit.log
    json.keys_under_root: true
    json.add_error_key: true

output.elasticsearch:
  hosts: ["elasticsearch:9200"]
  index: "torrent-audit-%{+yyyy.MM.dd}"
```

**Alerting rules (example for Elasticsearch/Wazuh/Splunk):**

-   Alert on any `AUTHENTICATION` with `outcome: "FAILURE"` (more than
    5 in 1 minute = brute force attempt).
-   Alert on any `AUTHORIZATION` with `outcome: "DENIED"` for sensitive
    topics.
-   Alert on `ADMIN_OPERATION` of type `DELETE_TOPIC`.
-   Alert on `CONFIG_CHANGE` to `security.*` settings.
-   Alert on `DELEGATION_TOKEN` creation for highly-privileged
    principals.

## Security Best Practices

### Deployment Hardening

1.  **Always enable TLS** — never run plaintext in production.
2.  **Use mTLS or SASL/SCRAM** — never allow anonymous access.
3.  **Run as non-root user** — torrent-mq should run under a dedicated
    `torrent` user.
4.  **Restrict file permissions:**
    ```bash
    chown -R torrent:torrent /etc/torrent/ /var/lib/torrent/ /var/log/torrent/
    chmod 750 /etc/torrent/
    chmod 600 /etc/torrent/certs/*.key /etc/torrent/encryption.key
    ```
5.  **Enable SELinux or AppArmor** — confine the torrentd process.
6.  **Use separate network interfaces** — client traffic on one, inter-
    broker on another if possible.
7.  **Rotate credentials regularly** — SCRAM passwords every 90 days,
    certificates annually.
8.  **Monitor and alert** on security events (see SIEM Integration).
9.  **Keep software updated** — apply security patches promptly.
10. **Regular security audits** — review ACLs, audit logs, and
    configuration quarterly.

### Least Privilege ACL Design

Principle of least privilege for service accounts:

```bash
# Read-only analytics service
torrentctl acl add --allow --principal User:analytics-svc \
    --operation Read --topic '*' --host '10.0.3.*'
torrentctl acl add --allow --principal User:analytics-svc \
    --operation Describe --topic '*' --host '10.0.3.*'

# Producer service (specific topics)
torrentctl acl add --allow --principal User:order-producer \
    --operation Write --topic orders --host '10.0.1.*'
torrentctl acl add --allow --principal User:order-producer \
    --operation Describe --topic orders --host '10.0.1.*'

# Consumer service (specific group)
torrentctl acl add --allow --principal User:order-processor \
    --operation Read --topic orders --host '10.0.1.*'
torrentctl acl add --allow --principal User:order-processor \
    --operation Read --group order-processors --host '10.0.1.*'
```

### Security Monitoring Checklist

- [ ] Authentication failures exceeding threshold.
- [ ] Authorization denials for sensitive resources.
- [ ] Unexpected topic deletions or configuration changes.
- [ ] Certificate expiration within 30 days.
- [ ] ACL changes by non-admin principals.
- [ ] Delegation token creation for admin-level principals.
- [ ] Unusual traffic patterns (DDoS detection).
- [ ] Connection attempts from unexpected IP ranges.
- [ ] TLS version downgrades or weak cipher usage.
- [ ] Encryption key file access attempts.

## Compliance Considerations

### SOC 2

For SOC 2 compliance, ensure:

- **Access Control:** All access is authenticated (SASL/mTLS) and
  authorized (ACLs). No anonymous access.
- **Audit Trail:** Audit logging is enabled with tamper-proof storage.
  Logs are shipped to a SIEM and retained per policy (minimum 90 days,
  typically 1 year).
- **Encryption:** Data in transit is encrypted (TLS 1.2+). Data at rest
  is encrypted (AES-256-GCM). Key management is documented and audited.
- **Change Management:** Configuration changes are logged and reviewed.
  ACK changes require approval.
- **Monitoring:** Security events are monitored and alerted in real-time.

### HIPAA

For HIPAA compliance when handling PHI (Protected Health Information):

- **Encryption everywhere:** TLS 1.2+ for all connections, AES-256-GCM
  at rest. No exceptions.
- **Access control:** Fine-grained ACLs per topic. PHI topics require
  specific authorization. Audit all access to PHI topics.
- **Audit trail:** Comprehensive logging of all access to PHI data.
  Include the principal, resource, operation, and timestamp.
- **Integrity controls:** CRC32C on every message, GCM authentication
  tags for at-rest data. Detect and alert on corruption.
- **Business Associate Agreement (BAA):** Ensure your hosting provider
  signs a BAA if infrastructure is managed.
- **Minimum necessary:** Only grant access to the specific PHI topics
  needed for each service.
- **Data retention:** Configure `retention.ms` to delete PHI after the
  required retention period. Document the retention policy.

### PCI-DSS

For PCI-DSS compliance when handling cardholder data:

- **Requirement 3:** Encrypt cardholder data at rest (AES-256-GCM) and
  in transit (TLS 1.2+). Never store CVV or full PAN in plaintext.
- **Requirement 4:** Encrypt transmission of cardholder data across open,
  public networks. Use strong TLS cipher suites only.
- **Requirement 7:** Restrict access to cardholder data by business
  need-to-know. ACLs per topic with cardholder data.
- **Requirement 8:** Unique IDs for each person with access. All access
  authenticated via SASL or mTLS.
- **Requirement 10:** Track and monitor all access to cardholder data.
  Audit logging enabled, shipped to SIEM, reviewed daily.
- **Requirement 11:** Regularly test security systems. Include torrent-mq
  in vulnerability scans and penetration tests.

**PCI-DSS topic segregation:**

Create separate topics for cardholder data:

```bash
# Cardholder data topic with strict ACLs
torrentctl topic create payment-cards --partitions 6 --replication-factor 3

# Only authorized services can access
torrentctl acl add --allow --principal User:payment-processor \
    --operation Write --topic payment-cards --host '10.0.4.*'
torrentctl acl add --allow --principal User:payment-processor \
    --operation Read --topic payment-cards --host '10.0.4.*'

# Deny everyone else
torrentctl acl add --deny --principal 'User:*' \
    --operation All --topic payment-cards --host '*'
```

**Tokenization:** Do not send raw card numbers through torrent-mq.
Tokenize before producing to Kafka and detokenize after consuming.
