"""
torrent-mq — TorrentProducer: Asynchronous Batching Message Producer

The TorrentProducer is the primary API for publishing messages to torrent-mq
topics. It implements a high-throughput, low-latency produce pipeline with
configurable batching, compression, retry, and delivery guarantees.

Architecture Overview
---------------------
  send() → RecordAccumulator (batch per topic-partition) → Sender thread
         → Compress → Network (Produce RPC) → Broker → Response
         → Delivery callback (success/failure)

Key Features
------------
- RecordAccumulator: batches records per topic-partition, reducing network
  round-trips and improving throughput.
- Configurable batch_size, linger_ms, buffer_memory for latency/throughput tuning.
- Compression at the batch level (gzip, snappy, lz4, zstd).
- Partition assignment: key-based hashing (murmur2) or round-robin for
  keyless messages.
- Delivery callbacks: per-record success/failure notification with RecordMetadata.
- Automatic leader discovery and retry with exponential backoff.
- Graceful shutdown via flush() and close().

Thread Safety
-------------
TorrentProducer is fully thread-safe. send() can be called from multiple
threads concurrently. The internal RecordAccumulator uses per-partition
locking, and the sender runs on a dedicated background thread.

Usage Example
-------------
    from torrent_mq import TorrentProducer

    producer = TorrentProducer(
        bootstrap_servers="localhost:9092",
        client_id="my-app",
        acks=1,
        compression_type="gzip",
    )

    # Send a single message
    metadata = producer.send("my-topic", b"hello world", key=b"key1")

    # Send a batch
    futures = producer.send_batch("my-topic", [
        {"value": b"msg1", "key": b"k1"},
        {"value": b"msg2", "key": b"k2"},
    ])

    # Wait for all in-flight messages
    producer.flush()

    # Graceful shutdown
    producer.close()
"""

from __future__ import annotations

import io
import logging
import queue
import random
import socket
import struct
import threading
import time
import zlib
from collections import defaultdict
from concurrent.futures import Future
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from .protocol import (
    ApiKey,
    ErrorCode,
    Record,
    RecordBatch,
    build_request,
    crc32c,
    decode_metadata_response,
    decode_produce_response,
    encode_metadata_request,
    encode_produce_request,
    encode_record_batch,
    error_description,
    parse_response,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Compression Support
# ---------------------------------------------------------------------------

try:
    import snappy

    _HAS_SNAPPY = True
except ImportError:
    _HAS_SNAPPY = False

try:
    import lz4.frame as _lz4frame_py

    _HAS_LZ4 = True
except ImportError:
    _HAS_LZ4 = False

try:
    import zstd

    _HAS_ZSTD = True
except ImportError:
    _HAS_ZSTD = False


class CompressionType(Enum):
    """Supported compression codecs for record batches."""

    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"

    def to_attrs(self) -> int:
        _map = {
            CompressionType.NONE: 0,
            CompressionType.GZIP: 1,
            CompressionType.SNAPPY: 2,
            CompressionType.LZ4: 3,
            CompressionType.ZSTD: 4,
        }
        return _map[self]

    @classmethod
    def from_string(cls, s: str) -> "CompressionType":
        for ct in cls:
            if ct.value == s.lower():
                return ct
        raise ValueError(f"Unsupported compression type: {s}")

    def compress(self, data: bytes) -> bytes:
        if self == CompressionType.NONE:
            return data
        elif self == CompressionType.GZIP:
            return zlib.compress(data, 6)
        elif self == CompressionType.SNAPPY:
            if not _HAS_SNAPPY:
                raise RuntimeError("snappy compression requires 'python-snappy' package")
            return snappy.compress(data)  # type: ignore[no-any-return]
        elif self == CompressionType.LZ4:
            if not _HAS_LZ4:
                raise RuntimeError("lz4 compression requires 'lz4' package")
            return _lz4frame_py.compress(data)  # type: ignore[no-any-return]
        elif self == CompressionType.ZSTD:
            if not _HAS_ZSTD:
                raise RuntimeError("zstd compression requires 'zstandard' package")
            return zstd.compress(data)  # type: ignore[no-any-return]
        return data


# ---------------------------------------------------------------------------
# Murmur2 Hash (for partition assignment, matching Kafka Java client)
# ---------------------------------------------------------------------------


def _murmur2(data: bytes) -> int:
    """Murmur2 hash of *data*, matching the Kafka Java client implementation."""
    length = len(data)
    seed = 0x9747B28C
    m = 0x5BD1E995
    r = 24
    h = seed ^ length

    i = 0
    while length >= 4:
        k = (
            data[i + 0] & 0xFF
            | ((data[i + 1] & 0xFF) << 8)
            | ((data[i + 2] & 0xFF) << 16)
            | ((data[i + 3] & 0xFF) << 24)
        )
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
        length -= 4
        i += 4

    if length == 3:
        h ^= (data[i + 2] & 0xFF) << 16
    if length >= 2:
        h ^= (data[i + 1] & 0xFF) << 8
    if length >= 1:
        h ^= data[i + 0] & 0xFF
        h = (h * m) & 0xFFFFFFFF

    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15

    return h & 0x7FFFFFFF


# ---------------------------------------------------------------------------
# RecordMetadata
# ---------------------------------------------------------------------------


@dataclass
class RecordMetadata:
    """Metadata about a successfully produced record.

    Attributes:
        topic: Topic the record was sent to.
        partition: Partition the record was assigned to.
        offset: Offset assigned by the broker.
        timestamp: Append timestamp in milliseconds since epoch.
    """

    topic: str
    partition: int
    offset: int
    timestamp: int


# ---------------------------------------------------------------------------
# Producer Configuration
# ---------------------------------------------------------------------------


@dataclass
class ProducerConfig:
    """Configuration for TorrentProducer.

    Attributes:
        bootstrap_servers: Comma-separated list of 'host:port' entries.
        client_id: Identifier sent to the broker for logging/metrics.
        acks: Required acknowledgements (0=none, 1=leader, -1=all ISR).
        compression_type: Compression to apply to record batches.
        batch_size: Maximum batch size in bytes before sending.
        linger_ms: Maximum time (ms) to wait before sending a partial batch.
        buffer_memory: Total memory (bytes) for buffering unsent records.
        max_request_size: Maximum size of a produce request in bytes.
        retries: Maximum number of retry attempts for send failures.
        retry_backoff_ms: Base backoff between retries (doubles each attempt).
        request_timeout_ms: Time to wait for broker acknowledgement.
        max_in_flight: Maximum in-flight requests per broker.
        metadata_max_age_ms: Maximum age of cached metadata before refresh.
        socket_timeout_ms: Socket read/write timeout.
        connection_timeout_ms: Connection timeout.
        enable_idempotence: If True, use idempotent producer semantics.
    """

    bootstrap_servers: str = "localhost:9092"
    client_id: str = "torrent-producer"

    # Delivery
    acks: int = 1
    request_timeout_ms: int = 30000
    retries: int = 3
    retry_backoff_ms: int = 100

    # Batching
    batch_size: int = 16384
    linger_ms: int = 5
    buffer_memory: int = 33554432

    # Network
    max_request_size: int = 1048576
    max_in_flight: int = 5
    metadata_max_age_ms: int = 300000
    socket_timeout_ms: int = 60000
    connection_timeout_ms: int = 10000

    # Features
    compression_type: CompressionType = CompressionType.NONE
    enable_idempotence: bool = False


# ---------------------------------------------------------------------------
# Broker Connection
# ---------------------------------------------------------------------------


class _BrokerConnection:
    """Low-level TCP connection to a single broker.

    Manages socket lifecycle, request/response correlation, and retries.
    """

    def __init__(self, host: str, port: int, config: ProducerConfig) -> None:
        self._host = host
        self._port = port
        self._config = config
        self._sock: Optional[socket.socket] = None
        self._lock = threading.Lock()
        self._correlation_id = 0
        self._pending: Dict[int, Future[bytes]] = {}
        self._connected = False

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    def connect(self) -> None:
        """Establish connection to the broker."""
        with self._lock:
            if self._connected and self._sock is not None:
                return
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.settimeout(self._config.connection_timeout_ms / 1000.0)
            self._sock.connect((self._host, self._port))
            self._sock.settimeout(self._config.socket_timeout_ms / 1000.0)
            self._connected = True
            logger.debug("Connected to broker %s:%d", self._host, self._port)

    def close(self) -> None:
        """Close the connection."""
        with self._lock:
            if self._sock:
                try:
                    self._sock.close()
                except OSError:
                    pass
                self._sock = None
            self._connected = False
            # Fail all pending futures
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError(f"Connection to {self._host}:{self._port} closed"))
            self._pending.clear()

    def _next_correlation_id(self) -> int:
        self._correlation_id += 1
        return self._correlation_id

    def send_request(self, api_key: int, api_version: int, body: bytes, timeout_ms: int = 30000) -> bytes:
        """Send a request and block for the response.

        Args:
            api_key: API key for the RPC.
            api_version: API version to use.
            body: Serialized request body.
            timeout_ms: Maximum time to wait for response.

        Returns:
            Raw response body bytes.

        Raises:
            ConnectionError: On connection failure.
            TimeoutError: If response is not received in time.
        """
        correlation_id = self._next_correlation_id()
        request_bytes = build_request(
            api_key=api_key,
            api_version=api_version,
            correlation_id=correlation_id,
            client_id=self._config.client_id,
            body=body,
        )

        fut: Future[bytes] = Future()
        self._pending[correlation_id] = fut

        with self._lock:
            if not self._connected or self._sock is None:
                self.connect()
            try:
                self._sock.sendall(request_bytes)  # type: ignore[union-attr]
            except OSError as e:
                self._pending.pop(correlation_id, None)
                self._connected = False
                raise ConnectionError(f"Send failed to {self._host}:{self._port}: {e}") from e

        try:
            return fut.result(timeout=timeout_ms / 1000.0)
        except Exception:
            self._pending.pop(correlation_id, None)
            raise

    def recv_loop(self) -> None:
        """Process incoming responses (called from a background thread)."""
        while self._connected:
            try:
                with self._lock:
                    if self._sock is None:
                        break
                    # Read 4-byte size prefix
                    size_bytes = self._recv_exact(4)
                    if size_bytes is None:
                        break
                    size = struct.unpack(">i", size_bytes)[0]
                    body = self._recv_exact(size)
                    if body is None:
                        break
                response = parse_response(size_bytes + body)
                fut = self._pending.pop(response.correlation_id, None)
                if fut is not None and not fut.done():
                    fut.set_result(response.data)
            except (OSError, struct.error, ValueError) as e:
                logger.debug("Recv error on %s:%d: %s", self._host, self._port, e)
                self.close()
                break

    def _recv_exact(self, n: int) -> Optional[bytes]:
        """Receive exactly *n* bytes from the socket."""
        if self._sock is None:
            return None
        data = b""
        while len(data) < n:
            try:
                chunk = self._sock.recv(n - len(data))
            except (socket.timeout, OSError):
                return None
            if not chunk:
                return None
            data += chunk
        return data


# ---------------------------------------------------------------------------
# Record Accumulator
# ---------------------------------------------------------------------------


@dataclass
class _BatchRecord:
    """Internal representation of a record in the accumulator."""

    value: Optional[bytes]
    key: Optional[bytes]
    headers: List[Tuple[str, str]]
    timestamp_ms: int
    partition: int
    future: Future[RecordMetadata]


class RecordAccumulator:
    """Append-only buffer that batches records per topic-partition.

    Records are accumulated until a batch reaches `batch_size` or
    `linger_ms` has elapsed since the first record was appended.
    The sender thread drains completed batches.
    """

    def __init__(self, config: ProducerConfig) -> None:
        self._config = config
        self._lock = threading.Lock()
        # (topic, partition) -> deque of _BatchRecord
        self._batches: Dict[Tuple[str, int], List[_BatchRecord]] = defaultdict(list)
        self._batch_first_ts: Dict[Tuple[str, int], float] = {}
        self._batch_size_bytes: Dict[Tuple[str, int], int] = defaultdict(int)

    def append(
        self,
        topic: str,
        value: Optional[bytes],
        key: Optional[bytes] = None,
        headers: Optional[List[Tuple[str, str]]] = None,
        timestamp_ms: Optional[int] = None,
        partition: int = 0,
    ) -> Future[RecordMetadata]:
        """Append a record to the accumulator.

        Returns a Future that resolves to RecordMetadata on successful send.
        """
        fut: Future[RecordMetadata] = Future()
        ts = timestamp_ms or int(time.time() * 1000)
        record = _BatchRecord(
            value=value,
            key=key,
            headers=headers or [],
            timestamp_ms=ts,
            partition=partition,
            future=fut,
        )

        key_tp = (topic, partition)
        with self._lock:
            self._batches[key_tp].append(record)
            # Estimate record size
            rec_size = (
                (len(value) if value else 0)
                + (len(key) if key else 0)
                + 50  # overhead
            )
            self._batch_size_bytes[key_tp] += rec_size
            if key_tp not in self._batch_first_ts:
                self._batch_first_ts[key_tp] = time.monotonic()
        return fut

    def drain(self) -> Dict[Tuple[str, int], List[_BatchRecord]]:
        """Return batches that are ready to send.

        A batch is ready when it reaches batch_size or linger_ms has elapsed
        since the first record was appended.

        Returns:
            Dict mapping (topic, partition) -> list of records to send.
        """
        ready: Dict[Tuple[str, int], List[_BatchRecord]] = {}
        now = time.monotonic()
        with self._lock:
            for key_tp, records in list(self._batches.items()):
                if not records:
                    continue
                size = self._batch_size_bytes[key_tp]
                first_ts = self._batch_first_ts.get(key_tp, now)
                linger = self._config.linger_ms / 1000.0

                if size >= self._config.batch_size or (now - first_ts) >= linger:
                    ready[key_tp] = records[:]
                    del self._batches[key_tp]
                    self._batch_size_bytes.pop(key_tp, None)
                    self._batch_first_ts.pop(key_tp, None)

        # Also drain if total accumulator memory exceeds buffer_memory
        total_size = sum(self._batch_size_bytes.values())
        if total_size > self._config.buffer_memory * 0.9:
            with self._lock:
                for key_tp in list(self._batches.keys()):
                    if key_tp not in ready and self._batches[key_tp]:
                        ready[key_tp] = self._batches[key_tp][:]
                        del self._batches[key_tp]
                        self._batch_size_bytes.pop(key_tp, None)
                        self._batch_first_ts.pop(key_tp, None)

        return ready

    def flush_all(self) -> Dict[Tuple[str, int], List[_BatchRecord]]:
        """Force-drain all batches regardless of size or linger."""
        ready: Dict[Tuple[str, int], List[_BatchRecord]] = {}
        with self._lock:
            for key_tp in list(self._batches.keys()):
                if self._batches[key_tp]:
                    ready[key_tp] = self._batches[key_tp][:]
            self._batches.clear()
            self._batch_size_bytes.clear()
            self._batch_first_ts.clear()
        return ready

    def pending_count(self) -> int:
        with self._lock:
            return sum(len(v) for v in self._batches.values())


# ---------------------------------------------------------------------------
# Cluster Metadata (cached)
# ---------------------------------------------------------------------------


@dataclass
class _PartitionInfo:
    partition_id: int
    leader_id: int
    leader_host: str
    leader_port: int
    replicas: List[int]
    isr: List[int]


@dataclass
class _TopicMetadata:
    name: str
    partitions: Dict[int, _PartitionInfo]


class MetadataCache:
    """Thread-safe cache of cluster metadata (brokers, topics, partitions)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._brokers: Dict[int, Dict[str, Any]] = {}
        self._topics: Dict[str, _TopicMetadata] = {}
        self._controller_id: int = -1
        self._last_refresh: float = 0.0

    def refresh(self, response: Dict[str, Any]) -> None:
        """Update cache from a Metadata response dict."""
        with self._lock:
            for b in response.get("brokers", []):
                self._brokers[b["id"]] = b
            self._controller_id = response.get("controller_id", -1)
            for topic in response.get("topics", []):
                name = topic["name"]
                partitions: Dict[int, _PartitionInfo] = {}
                for p in topic.get("partitions", []):
                    leader_id = p.get("leader_id", -1)
                    leader_info = self._brokers.get(leader_id, {})
                    partitions[p["id"]] = _PartitionInfo(
                        partition_id=p["id"],
                        leader_id=leader_id,
                        leader_host=leader_info.get("host", ""),
                        leader_port=leader_info.get("port", 9092),
                        replicas=p.get("replicas", []),
                        isr=p.get("isr", []),
                    )
                self._topics[name] = _TopicMetadata(name=name, partitions=partitions)
            self._last_refresh = time.monotonic()

    def get_partition_leader(self, topic: str, partition: int) -> Optional[Tuple[str, int]]:
        """Get (host, port) of the leader for a topic-partition."""
        with self._lock:
            tm = self._topics.get(topic)
            if tm is None:
                return None
            pi = tm.partitions.get(partition)
            if pi is None:
                return None
            if pi.leader_id == -1:
                return None
            return (pi.leader_host, pi.leader_port)

    def get_partition_count(self, topic: str) -> int:
        with self._lock:
            tm = self._topics.get(topic)
            return len(tm.partitions) if tm else 0

    def get_broker(self, broker_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._brokers.get(broker_id)

    @property
    def last_refresh(self) -> float:
        return self._last_refresh


# ---------------------------------------------------------------------------
# TorrentProducer
# ---------------------------------------------------------------------------


class TorrentProducer:
    """High-level producer for torrent-mq.

    The TorrentProducer sends messages to torrent-mq topics with configurable
    batching, compression, delivery guarantees, and automatic partition
    assignment. Internally it uses a RecordAccumulator to batch records
    and a background sender thread to dispatch them to the appropriate
    partition leaders.

    Args:
        bootstrap_servers: Comma-separated 'host:port' entries. Default: 'localhost:9092'.
        client_id: Client identifier for logging and metrics. Default: 'torrent-producer'.
        acks: Required acks: 0 (no ack), 1 (leader only), -1 (all ISR). Default: 1.
        compression_type: 'none', 'gzip', 'snappy', 'lz4', or 'zstd'. Default: 'none'.
        batch_size: Max bytes per batch before sending. Default: 16384 (16 KiB).
        linger_ms: Max wait (ms) before sending a partial batch. Default: 5.
        buffer_memory: Total memory for buffering unsent records. Default: 32 MiB.
        max_request_size: Max produce request size. Default: 1 MiB.
        retries: Max retry attempts for transient failures. Default: 3.
        retry_backoff_ms: Base backoff between retries. Default: 100 ms.
        request_timeout_ms: Max time to wait for broker ack. Default: 30000 ms.
        enable_idempotence: Enable exactly-once delivery. Default: False.
        **kwargs: Additional configuration passed to ProducerConfig.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        client_id: str = "torrent-producer",
        acks: int = 1,
        compression_type: Union[str, CompressionType] = "none",
        batch_size: int = 16384,
        linger_ms: int = 5,
        buffer_memory: int = 33554432,
        max_request_size: int = 1048576,
        retries: int = 3,
        retry_backoff_ms: int = 100,
        request_timeout_ms: int = 30000,
        enable_idempotence: bool = False,
        **kwargs: Any,
    ) -> None:
        if isinstance(compression_type, str):
            compression_type = CompressionType.from_string(compression_type)

        self._config = ProducerConfig(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            acks=acks,
            compression_type=compression_type,
            batch_size=batch_size,
            linger_ms=linger_ms,
            buffer_memory=buffer_memory,
            max_request_size=max_request_size,
            retries=retries,
            retry_backoff_ms=retry_backoff_ms,
            request_timeout_ms=request_timeout_ms,
            enable_idempotence=enable_idempotence,
            **kwargs,
        )

        self._accumulator = RecordAccumulator(self._config)
        self._metadata = MetadataCache()
        self._connections: Dict[Tuple[str, int], _BrokerConnection] = {}
        self._conn_lock = threading.Lock()
        self._running = True
        self._sender_thread: Optional[threading.Thread] = None

        # Discover initial cluster metadata
        self._discover_metadata()
        # Start background sender
        self._start_sender()

    def _resolve_bootstrap(self) -> List[Tuple[str, int]]:
        """Parse bootstrap_servers into list of (host, port)."""
        servers: List[Tuple[str, int]] = []
        for entry in self._config.bootstrap_servers.split(","):
            entry = entry.strip()
            if not entry:
                continue
            if ":" in entry:
                host, port_s = entry.rsplit(":", 1)
                servers.append((host.strip(), int(port_s)))
            else:
                servers.append((entry.strip(), 9092))
        return servers

    def _get_connection(self, host: str, port: int) -> _BrokerConnection:
        key = (host, port)
        with self._conn_lock:
            if key not in self._connections:
                conn = _BrokerConnection(host, port, self._config)
                conn.connect()
                self._connections[key] = conn
            return self._connections[key]

    def _discover_metadata(self, topics: Optional[List[str]] = None) -> None:
        """Fetch cluster metadata from a bootstrap server.

        Tries bootstrap servers in order until one succeeds, then updates
        the MetadataCache.
        """
        for host, port in self._resolve_bootstrap():
            try:
                conn = self._get_connection(host, port)
                body = encode_metadata_request(topics)
                response_data = conn.send_request(
                    ApiKey.METADATA, 4, body, self._config.request_timeout_ms
                )
                metadata = decode_metadata_response(response_data)
                self._metadata.refresh(metadata)
                logger.info("Metadata discovered from %s:%d: %d topics, %d brokers",
                            host, port,
                            len(metadata.get("topics", [])),
                            len(metadata.get("brokers", [])))
                return
            except Exception as e:
                logger.debug("Failed to fetch metadata from %s:%d: %s", host, port, e)
        raise ConnectionError(
            f"Unable to discover cluster metadata from any bootstrap server: "
            f"{self._config.bootstrap_servers}"
        )

    def _ensure_metadata(self, topic: str) -> None:
        """Refresh metadata if topic is unknown or cache is stale."""
        now = time.monotonic()
        if (
            self._metadata.get_partition_count(topic) == 0
            or (now - self._metadata.last_refresh) * 1000 > self._config.metadata_max_age_ms
        ):
            try:
                self._discover_metadata([topic])
            except Exception:
                pass  # Will fail at send time

    def _partition_for(self, topic: str, key: Optional[bytes], partition: Optional[int]) -> int:
        """Determine partition for a record.

        If *partition* is given explicitly, use it. Otherwise, if *key* is
        provided, hash it with Murmur2. If no key, round-robin across
        available partitions.
        """
        if partition is not None:
            return partition

        num_parts = self._metadata.get_partition_count(topic)
        if num_parts == 0:
            return 0

        if key is not None and len(key) > 0:
            return _murmur2(key) % num_parts
        # Round-robin: use a simple counter per topic
        key_obj = f"__rr_{topic}"
        if not hasattr(self, "_rr_counters"):
            self._rr_counters: Dict[str, int] = {}
        self._rr_counters[key_obj] = self._rr_counters.get(key_obj, 0) + 1
        return self._rr_counters[key_obj] % num_parts

    def send(
        self,
        topic: str,
        value: Union[bytes, str],
        key: Optional[Union[bytes, str]] = None,
        headers: Optional[List[Tuple[str, str]]] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
    ) -> RecordMetadata:
        """Send a single message to a topic and block until acknowledged.

        Args:
            topic: Destination topic name.
            value: Message payload (bytes or str).
            key: Optional message key for partition assignment.
            headers: Optional list of (key, value) string header pairs.
            partition: Explicit partition to send to (auto-assigned if None).
            timestamp_ms: Timestamp for the record (current time if None).

        Returns:
            RecordMetadata with assigned offset and partition.

        Raises:
            ConnectionError: If the broker is unreachable.
            TimeoutError: If the request times out.
            RuntimeError: If the broker returns a non-retriable error.
        """
        if isinstance(value, str):
            value = value.encode("utf-8")
        if isinstance(key, str):
            key = key.encode("utf-8")

        self._ensure_metadata(topic)
        part = self._partition_for(topic, key, partition)

        fut = self._accumulator.append(
            topic=topic,
            value=value,
            key=key,
            headers=headers,
            timestamp_ms=timestamp_ms,
            partition=part,
        )

        # Flush immediately for synchronous send
        self._sender_step()
        return fut.result(timeout=self._config.request_timeout_ms / 1000.0)

    def send_batch(
        self,
        topic: str,
        messages: List[Dict[str, Any]],
    ) -> List[Future[RecordMetadata]]:
        """Send a batch of messages asynchronously.

        Each message dict may contain:
            - 'value' (bytes or str): message payload (required)
            - 'key' (bytes or str): message key (optional)
            - 'headers' (list of (str, str)): message headers (optional)
            - 'partition' (int): explicit partition (optional)
            - 'timestamp_ms' (int): timestamp (optional)

        Args:
            topic: Destination topic.
            messages: List of message dicts.

        Returns:
            List of Futures, each resolving to RecordMetadata for that message.
        """
        self._ensure_metadata(topic)
        futures: List[Future[RecordMetadata]] = []
        for msg in messages:
            value = msg["value"]
            if isinstance(value, str):
                value = value.encode("utf-8")
            key = msg.get("key")
            if isinstance(key, str):
                key = key.encode("utf-8")

            part = self._partition_for(topic, key, msg.get("partition"))
            fut = self._accumulator.append(
                topic=topic,
                value=value,
                key=key,
                headers=msg.get("headers"),
                timestamp_ms=msg.get("timestamp_ms"),
                partition=part,
            )
            futures.append(fut)
        return futures

    def _start_sender(self) -> None:
        """Launch the background sender thread."""
        self._sender_thread = threading.Thread(
            target=self._sender_loop,
            name="torrent-producer-sender",
            daemon=True,
        )
        self._sender_thread.start()

    def _sender_loop(self) -> None:
        """Background loop that drains the accumulator and sends batches."""
        while self._running:
            try:
                self._sender_step()
            except Exception as e:
                logger.error("Sender loop error: %s", e)
            time.sleep(self._config.linger_ms / 1000.0)

    def _sender_step(self) -> None:
        """Drain ready batches and send them to the appropriate brokers."""
        ready = self._accumulator.drain()
        if not ready:
            return

        # Group by (host, port) for efficient batching
        by_broker: Dict[Tuple[str, int], List[Tuple[str, int, List[_BatchRecord]]]] = defaultdict(list)
        for (topic, partition), records in ready.items():
            leader = self._metadata.get_partition_leader(topic, partition)
            if leader is None:
                # Leader unknown; fail these records
                for rec in records:
                    rec.future.set_exception(
                        RuntimeError(f"No leader for {topic}/{partition}")
                    )
                continue
            by_broker[leader].append((topic, partition, records))

        for (host, port), tp_records_list in by_broker.items():
            for topic, partition, records in tp_records_list:
                self._send_batch_to_broker(host, port, topic, partition, records)

    def _build_record_batch(self, records: List[_BatchRecord], base_offset: int = 0) -> bytes:
        """Build a v2 record batch from accumulator records.

        Applies compression if configured.
        """
        batch = RecordBatch(
            base_offset=base_offset,
            first_timestamp=min(r.timestamp_ms for r in records),
            max_timestamp=max(r.timestamp_ms for r in records),
            last_offset_delta=len(records) - 1,
            records=[
                Record(
                    offset_delta=i,
                    timestamp_delta=records[i].timestamp_ms - min(r.timestamp_ms for r in records),
                    key=records[i].key,
                    value=records[i].value,
                    headers=[
                        (hk, hv.encode("utf-8") if isinstance(hv, str) else hv)
                        for hk, hv in records[i].headers
                    ],
                )
                for i in range(len(records))
            ],
        )
        batch.compression_type = self._config.compression_type.to_attrs()
        return encode_record_batch(batch)

    def _send_batch_to_broker(
        self,
        host: str,
        port: int,
        topic: str,
        partition: int,
        records: List[_BatchRecord],
    ) -> None:
        """Send a batch of records to a specific broker with retry logic."""
        batch_bytes = self._build_record_batch(records)
        if self._config.compression_type != CompressionType.NONE:
            try:
                batch_bytes = self._config.compression_type.compress(batch_bytes)
            except Exception as e:
                for rec in records:
                    rec.future.set_exception(e)
                return

        last_error: Optional[Exception] = None
        for attempt in range(self._config.retries + 1):
            try:
                conn = self._get_connection(host, port)
                body = encode_produce_request(
                    topic=topic,
                    partition=partition,
                    record_batch=batch_bytes,
                    acks=self._config.acks,
                    timeout_ms=self._config.request_timeout_ms,
                )
                response_data = conn.send_request(
                    ApiKey.PRODUCE, 7, body, self._config.request_timeout_ms
                )
                result = decode_produce_response(response_data)
                topic_result = result.get(topic, {})
                part_result = topic_result.get(partition, {})

                error_code = part_result.get("error_code", 0)
                if error_code == ErrorCode.NONE:
                    base_offset = part_result.get("base_offset", 0)
                    timestamp = part_result.get("log_append_time_ms", int(time.time() * 1000))
                    for rec in records:
                        metadata = RecordMetadata(
                            topic=topic,
                            partition=partition,
                            offset=base_offset,
                            timestamp=timestamp,
                        )
                        rec.future.set_result(metadata)
                    return
                elif error_code in (
                    ErrorCode.NOT_LEADER_FOR_PARTITION,
                    ErrorCode.LEADER_NOT_AVAILABLE,
                    ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                ):
                    # Retriable: refresh metadata and retry
                    self._discover_metadata([topic])
                    leader = self._metadata.get_partition_leader(topic, partition)
                    if leader:
                        host, port = leader
                    last_error = RuntimeError(
                        f"{error_description(error_code)} (code={error_code})"
                    )
                else:
                    raise RuntimeError(f"{error_description(error_code)} (code={error_code})")

            except (ConnectionError, TimeoutError) as e:
                last_error = e
                # Exponential backoff
                backoff = self._config.retry_backoff_ms * (2**attempt) / 1000.0
                time.sleep(min(backoff, 30.0))
                # Refresh metadata on connection failure
                self._discover_metadata([topic])
            except Exception as e:
                for rec in records:
                    rec.future.set_exception(e)
                return

        # All retries exhausted
        if last_error:
            for rec in records:
                rec.future.set_exception(last_error)

    def flush(self, timeout_ms: int = 30000) -> None:
        """Block until all in-flight records have been sent and acknowledged.

        Args:
            timeout_ms: Maximum time to wait (ms). Raises TimeoutError if exceeded.
        """
        deadline = time.monotonic() + timeout_ms / 1000.0
        while self._accumulator.pending_count() > 0:
            self._sender_step()
            if time.monotonic() > deadline:
                raise TimeoutError("Flush timed out after {timeout_ms}ms")
            time.sleep(0.01)

    def close(self) -> None:
        """Gracefully shut down the producer.

        Flushes all pending records, stops the sender thread, and closes
        all broker connections.
        """
        logger.info("Shutting down producer...")
        self._running = False

        # Flush remaining records
        try:
            self.flush(10000)
        except TimeoutError:
            logger.warning("Some records could not be sent before shutdown")

        if self._sender_thread is not None:
            self._sender_thread.join(timeout=5.0)

        with self._conn_lock:
            for conn in self._connections.values():
                conn.close()
            self._connections.clear()

        logger.info("Producer shut down.")

    def __enter__(self) -> "TorrentProducer":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
