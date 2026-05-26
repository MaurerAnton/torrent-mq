"""
torrent-mq — TorrentConsumer: Consumer Group Member Implementation

The TorrentConsumer implements consumer group semantics compatible with
torrent-mq: subscribe to topics, poll for messages, commit offsets, and
participate in the group rebalance protocol.

Architecture Overview
---------------------
  subscribe() → JoinGroup → SyncGroup → Stable (poll loop)
               ↑                    ↓
               └── Heartbeat timeout / leader change ──┘

Key Features
------------
- Subscribe/unsubscribe to one or more topics.
- Poll-based message retrieval with configurable timeout and batch size.
- Automatic partition assignment via the group coordinator.
- Manual and automatic offset commit (sync and async).
- Seek to specific offsets, beginning, or end of partitions.
- Rebalance listener callbacks for partition revocation/assignment.

Group Protocol
--------------
The consumer participates in a consumer group managed by the group
coordinator (one of the cluster brokers). The protocol follows the
group membership state machine:
    1. FindCoordinator: Locate the group coordinator.
    2. JoinGroup: Register with the group, propose partition assignment protocols.
    3. SyncGroup: The leader distributes partition assignments.
    4. Heartbeat: Periodic keep-alive; timeout triggers rebalance.

Thread Safety
-------------
TorrentConsumer is thread-safe. All public methods acquire an internal
mutex. The background heartbeat and auto-commit tasks run on daemon threads.

Usage Example
-------------
    from torrent_mq import TorrentConsumer

    consumer = TorrentConsumer(
        bootstrap_servers="localhost:9092",
        group_id="my-group",
        client_id="consumer-1",
    )
    consumer.subscribe(["my-topic"])

    while True:
        records = consumer.poll(1000)
        for rec in records:
            print(f"Received: {rec.topic}/{rec.partition}@{rec.offset}: {rec.value}")
        consumer.commit()
    consumer.close()
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
import uuid
from collections import defaultdict
from concurrent.futures import Future
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from .protocol import (
    ApiKey,
    ErrorCode,
    RecordBatch,
    build_request,
    decode_assignment,
    decode_fetch_response,
    decode_find_coordinator_response,
    decode_heartbeat_response,
    decode_join_group_response,
    decode_list_offsets_response,
    decode_offset_commit_response,
    decode_sync_group_response,
    decode_record_batch,
    encode_assignment,
    encode_fetch_request,
    encode_find_coordinator_request,
    encode_heartbeat_request,
    encode_join_group_request,
    encode_leave_group_request,
    encode_list_offsets_request,
    encode_offset_commit_request,
    encode_offset_fetch_request,
    encode_subscription_metadata,
    encode_sync_group_request,
    error_description,
    parse_response,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ConsumerRecord
# ---------------------------------------------------------------------------


@dataclass
class ConsumerRecord:
    """A single consumed record with key, value, and metadata.

    Attributes:
        topic: Source topic.
        partition: Source partition.
        offset: Offset of this record in the partition.
        timestamp: Record timestamp in milliseconds since epoch.
        key: Optional record key bytes.
        value: Record value bytes.
        headers: List of (key, value) string header pairs.
        leader_epoch: Leader epoch at time of consumption (0 if unknown).
    """

    topic: str
    partition: int
    offset: int
    timestamp: int
    key: Optional[bytes]
    value: Optional[bytes]
    headers: List[Tuple[str, str]] = field(default_factory=list)
    leader_epoch: int = 0


# ---------------------------------------------------------------------------
# Consumer Configuration
# ---------------------------------------------------------------------------


class OffsetReset(Enum):
    """Offset reset policy when no committed offset is found."""

    EARLIEST = "earliest"
    LATEST = "latest"
    NONE = "none"  # Throw exception


class Assignor(Enum):
    """Partition assignment strategy for consumer groups."""

    RANGE = "range"
    ROUND_ROBIN = "roundrobin"
    STICKY = "sticky"


@dataclass
class ConsumerConfig:
    """Configuration for TorrentConsumer.

    Attributes:
        bootstrap_servers: Comma-separated 'host:port' entries.
        group_id: Consumer group identifier (required).
        client_id: Client identifier for logging/metrics.
        auto_commit_interval_ms: Interval for auto-commit (0 = disabled).
        session_timeout_ms: Session timeout for group membership.
        heartbeat_interval_ms: Heartbeat interval for group membership.
        max_poll_records: Max records returned per poll().
        max_partition_fetch_bytes: Max bytes to fetch per partition.
        poll_timeout_ms: Max time poll() blocks waiting for records.
        auto_offset_reset: Where to start when no committed offset exists.
        partition_assignor: Partition assignment strategy.
        enable_auto_commit: Whether to auto-commit offsets periodically.
        fetch_min_bytes: Minimum bytes to accumulate before fetch returns.
        fetch_max_wait_ms: Maximum time fetch waits for min_bytes to accumulate.
        socket_timeout_ms: Socket read/write timeout.
        connection_timeout_ms: Connection timeout.
        rebalance_timeout_ms: Max time allowed for rebalance.
    """

    bootstrap_servers: str = "localhost:9092"
    group_id: str = ""
    client_id: str = "torrent-consumer"

    # Group membership
    session_timeout_ms: int = 45000
    heartbeat_interval_ms: int = 3000
    rebalance_timeout_ms: int = 60000

    # Polling
    max_poll_records: int = 500
    max_partition_fetch_bytes: int = 1048576
    poll_timeout_ms: int = 500
    fetch_min_bytes: int = 1
    fetch_max_wait_ms: int = 500

    # Offset management
    auto_offset_reset: OffsetReset = OffsetReset.LATEST
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000

    # Partition assignment
    partition_assignor: Assignor = Assignor.RANGE

    # Network
    socket_timeout_ms: int = 60000
    connection_timeout_ms: int = 10000


# ---------------------------------------------------------------------------
# Broker Connection (Consumer)
# ---------------------------------------------------------------------------


class _ConsumerBroker:
    """Low-level TCP connection to a single broker for consumer operations."""

    def __init__(self, host: str, port: int, client_id: str, config: ConsumerConfig) -> None:
        self._host = host
        self._port = port
        self._client_id = client_id
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
        with self._lock:
            if self._connected and self._sock is not None:
                return
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.settimeout(self._config.connection_timeout_ms / 1000.0)
            self._sock.connect((self._host, self._port))
            self._sock.settimeout(self._config.socket_timeout_ms / 1000.0)
            self._connected = True

    def close(self) -> None:
        with self._lock:
            if self._sock:
                try:
                    self._sock.close()
                except OSError:
                    pass
                self._sock = None
            self._connected = False
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError(f"Connection closed {self._host}:{self._port}"))
            self._pending.clear()

    def _next_correlation_id(self) -> int:
        self._correlation_id += 1
        return self._correlation_id

    def send_request(self, api_key: int, api_version: int, body: bytes, timeout_ms: int = 30000) -> bytes:
        correlation_id = self._next_correlation_id()
        request_bytes = build_request(
            api_key=api_key,
            api_version=api_version,
            correlation_id=correlation_id,
            client_id=self._client_id,
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
        while self._connected:
            try:
                with self._lock:
                    if self._sock is None:
                        break
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
                logger.debug("Consumer recv error on %s:%d: %s", self._host, self._port, e)
                self.close()
                break

    def _recv_exact(self, n: int) -> Optional[bytes]:
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
# Rebalance Listener
# ---------------------------------------------------------------------------


class RebalanceListener:
    """Callback interface invoked during group rebalance.

    Override `on_partitions_revoked` and `on_partitions_assigned` to
    perform custom logic (e.g., commit offsets before reassignment).

    Usage::

        class MyListener(RebalanceListener):
            def on_partitions_revoked(self, partitions):
                print(f"Revoked: {partitions}")

            def on_partitions_assigned(self, partitions):
                print(f"Assigned: {partitions}")
    """

    def on_partitions_revoked(self, partitions: List[Dict[str, Any]]) -> None:
        """Called before partitions are revoked.

        Args:
            partitions: List of {'topic': str, 'partition': int} dicts.
        """

    def on_partitions_assigned(self, partitions: List[Dict[str, Any]]) -> None:
        """Called after new partitions are assigned.

        Args:
            partitions: List of {'topic': str, 'partition': int} dicts.
        """


# ---------------------------------------------------------------------------
# Offset Manager
# ---------------------------------------------------------------------------


class _OffsetManager:
    """Tracks committed and current positions for each assigned partition."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # (topic, partition) -> committed_offset
        self._committed: Dict[Tuple[str, int], int] = {}
        # (topic, partition) -> current_position
        self._positions: Dict[Tuple[str, int], int] = {}

    def set_committed(self, topic: str, partition: int, offset: int) -> None:
        with self._lock:
            self._committed[(topic, partition)] = offset

    def get_committed(self, topic: str, partition: int) -> Optional[int]:
        with self._lock:
            return self._committed.get((topic, partition))

    def set_position(self, topic: str, partition: int, offset: int) -> None:
        with self._lock:
            self._positions[(topic, partition)] = offset

    def get_position(self, topic: str, partition: int) -> Optional[int]:
        with self._lock:
            return self._positions.get((topic, partition))

    def advance_position(self, topic: str, partition: int, offset: int) -> None:
        with self._lock:
            key = (topic, partition)
            current = self._positions.get(key, 0)
            if offset >= current:
                self._positions[key] = offset + 1

    def all_committed_offsets(self) -> Dict[Tuple[str, int], int]:
        with self._lock:
            return dict(self._committed)

    def uncommitted_offsets(self) -> Dict[Tuple[str, int], int]:
        """Return offsets that have been consumed but not committed."""
        with self._lock:
            result: Dict[Tuple[str, int], int] = {}
            for key, pos in self._positions.items():
                committed = self._committed.get(key, -1)
                if pos > committed:
                    result[key] = pos
            return result

    def clear(self) -> None:
        with self._lock:
            self._committed.clear()
            self._positions.clear()


# ---------------------------------------------------------------------------
# TorrentConsumer
# ---------------------------------------------------------------------------


class TorrentConsumer:
    """High-level consumer for torrent-mq.

    The TorrentConsumer participates in consumer groups to read messages
    from torrent-mq topics. It handles group membership, partition
    assignment, offset management, and provides a poll-based API for
    retrieving messages.

    Args:
        bootstrap_servers: Comma-separated 'host:port' entries. Default: 'localhost:9092'.
        group_id: Consumer group identifier (required).
        client_id: Client identifier for logging and metrics.
        session_timeout_ms: Session timeout for group membership. Default: 45000.
        heartbeat_interval_ms: Heartbeat interval. Default: 3000.
        max_poll_records: Max records per poll(). Default: 500.
        enable_auto_commit: Auto-commit offsets periodically. Default: True.
        auto_commit_interval_ms: Auto-commit interval. Default: 5000.
        auto_offset_reset: 'earliest', 'latest', or 'none'. Default: 'latest'.
        partition_assignor: 'range', 'roundrobin', or 'sticky'. Default: 'range'.
        rebalance_listener: Optional RebalanceListener for custom rebalance logic.
        **kwargs: Additional configuration passed to ConsumerConfig.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "",
        client_id: str = "torrent-consumer",
        session_timeout_ms: int = 45000,
        heartbeat_interval_ms: int = 3000,
        max_poll_records: int = 500,
        enable_auto_commit: bool = True,
        auto_commit_interval_ms: int = 5000,
        auto_offset_reset: Union[str, OffsetReset] = "latest",
        partition_assignor: Union[str, Assignor] = "range",
        rebalance_listener: Optional[RebalanceListener] = None,
        **kwargs: Any,
    ) -> None:
        if isinstance(auto_offset_reset, str):
            auto_offset_reset = OffsetReset(auto_offset_reset.lower())
        if isinstance(partition_assignor, str):
            partition_assignor = Assignor(partition_assignor.lower())

        if not group_id:
            raise ValueError("group_id is required")

        self._config = ConsumerConfig(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            client_id=client_id,
            session_timeout_ms=session_timeout_ms,
            heartbeat_interval_ms=heartbeat_interval_ms,
            max_poll_records=max_poll_records,
            enable_auto_commit=enable_auto_commit,
            auto_commit_interval_ms=auto_commit_interval_ms,
            auto_offset_reset=auto_offset_reset,
            partition_assignor=partition_assignor,
            **kwargs,
        )

        self._group_id = group_id
        self._member_id = ""
        self._generation_id = -1
        self._subscribed_topics: List[str] = []
        self._assignment: List[Dict[str, Any]] = []  # [{topic, partition}]
        self._offsets = _OffsetManager()
        self._listener = rebalance_listener or RebalanceListener()

        # Coordinator connection
        self._coordinator: Optional[_ConsumerBroker] = None
        self._coordinator_lock = threading.Lock()

        # Broker connections for fetch
        self._brokers: Dict[Tuple[str, int], _ConsumerBroker] = {}
        self._broker_lock = threading.Lock()

        # Metadata cache
        self._broker_map: Dict[int, Dict[str, Any]] = {}
        self._topic_partitions: Dict[str, List[int]] = {}

        # State
        self._running = True
        self._rebalancing = threading.Event()
        self._state_lock = threading.Lock()
        self._pending_records: queue.Queue = queue.Queue()

        # Background tasks
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._auto_commit_thread: Optional[threading.Thread] = None

        # Initialise
        self._discover_coordinator()
        self._start_background_tasks()

    # -----------------------------------------------------------------------
    # Connection Management
    # -----------------------------------------------------------------------

    def _resolve_bootstrap(self) -> List[Tuple[str, int]]:
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

    def _get_broker(self, host: str, port: int) -> _ConsumerBroker:
        key = (host, port)
        with self._broker_lock:
            if key not in self._brokers:
                broker = _ConsumerBroker(host, port, self._config.client_id, self._config)
                broker.connect()
                self._brokers[key] = broker
                # Start receive thread
                t = threading.Thread(
                    target=broker.recv_loop,
                    name=f"consumer-recv-{host}:{port}",
                    daemon=True,
                )
                t.start()
            return self._brokers[key]

    def _discover_coordinator(self) -> None:
        """Find and connect to the group coordinator."""
        for host, port in self._resolve_bootstrap():
            try:
                broker = _ConsumerBroker(host, port, self._config.client_id, self._config)
                broker.connect()
                body = encode_find_coordinator_request(self._group_id)
                response_data = broker.send_request(
                    ApiKey.FIND_COORDINATOR, 1, body, self._config.connection_timeout_ms
                )
                result = decode_find_coordinator_response(response_data)
                if result["error_code"] != 0:
                    raise RuntimeError(
                        f"FindCoordinator failed: {error_description(result['error_code'])}"
                    )
                coord_host = result["host"]
                coord_port = result["port"]
                logger.info(
                    "Group coordinator for '%s' is %s:%d (node %d)",
                    self._group_id, coord_host, coord_port, result["node_id"],
                )
                with self._coordinator_lock:
                    self._coordinator = self._get_broker(coord_host, coord_port)
                    # Start receive thread for coordinator
                    t = threading.Thread(
                        target=self._coordinator.recv_loop,
                        name=f"consumer-coordinator-recv",
                        daemon=True,
                    )
                    t.start()
                broker.close()
                return
            except Exception as e:
                logger.debug("Failed to find coordinator via %s:%d: %s", host, port, e)
                broker.close()
        raise ConnectionError("Could not find group coordinator")

    def _get_coordinator(self) -> _ConsumerBroker:
        with self._coordinator_lock:
            if self._coordinator is None:
                self._discover_coordinator()
            return self._coordinator  # type: ignore[return-value]

    # -----------------------------------------------------------------------
    # Subscribe / Unsubscribe
    # -----------------------------------------------------------------------

    def subscribe(self, topics: List[str]) -> None:
        """Subscribe to one or more topics.

        This triggers a group rebalance to receive partition assignments.

        Args:
            topics: List of topic names to subscribe to.
        """
        with self._state_lock:
            self._subscribed_topics = list(topics)
        self._join_group()

    def unsubscribe(self) -> None:
        """Unsubscribe from all topics and leave the consumer group."""
        self._subscribed_topics = []
        self._leave_group()

    def assignment(self) -> List[Dict[str, str]]:
        """Return the current partition assignment.

        Returns:
            List of {'topic': str, 'partition': int} dicts.
        """
        return list(self._assignment)

    # -----------------------------------------------------------------------
    # Group Protocol
    # -----------------------------------------------------------------------

    def _join_group(self) -> None:
        """Join the consumer group and obtain partition assignments."""
        if not self._subscribed_topics:
            return

        self._rebalancing.set()
        try:
            coordinator = self._get_coordinator()

            # Build subscription metadata
            metadata = encode_subscription_metadata(self._subscribed_topics)
            protocols = [{"name": self._config.partition_assignor.value, "metadata": metadata}]

            # Step 1: JoinGroup
            body = encode_join_group_request(
                group_id=self._group_id,
                session_timeout_ms=self._config.session_timeout_ms,
                rebalance_timeout_ms=self._config.rebalance_timeout_ms,
                member_id=self._member_id,
                protocol_type="consumer",
                protocols=protocols,
            )
            response_data = coordinator.send_request(
                ApiKey.JOIN_GROUP, 2, body, self._config.session_timeout_ms
            )
            join_result = decode_join_group_response(response_data)

            if join_result["error_code"] == ErrorCode.UNKNOWN_MEMBER_ID:
                # Retry with empty member_id
                self._member_id = ""
                body = encode_join_group_request(
                    group_id=self._group_id,
                    session_timeout_ms=self._config.session_timeout_ms,
                    rebalance_timeout_ms=self._config.rebalance_timeout_ms,
                    member_id="",
                    protocol_type="consumer",
                    protocols=protocols,
                )
                response_data = coordinator.send_request(
                    ApiKey.JOIN_GROUP, 2, body, self._config.session_timeout_ms
                )
                join_result = decode_join_group_response(response_data)

            if join_result["error_code"] != 0:
                raise RuntimeError(
                    f"JoinGroup failed: {error_description(join_result['error_code'])}"
                )

            self._member_id = join_result["member_id"]
            self._generation_id = join_result["generation_id"]
            leader_id = join_result["leader_id"]
            members = join_result["members"]
            logger.info(
                "Joined group '%s' as member '%s', generation %d, leader=%s",
                self._group_id, self._member_id, self._generation_id,
                "yes" if leader_id == self._member_id else f"no ({leader_id})",
            )

            # Step 2: SyncGroup
            if leader_id == self._member_id:
                # We are the leader; compute assignments
                assignments = self._compute_assignments(members)
            else:
                assignments = []
            sync_body = encode_sync_group_request(
                group_id=self._group_id,
                generation_id=self._generation_id,
                member_id=self._member_id,
                assignments=assignments,
            )
            sync_response_data = coordinator.send_request(
                ApiKey.SYNC_GROUP, 2, sync_body, self._config.session_timeout_ms
            )
            sync_result = decode_sync_group_response(sync_response_data)

            if sync_result["error_code"] != 0:
                raise RuntimeError(
                    f"SyncGroup failed: {error_description(sync_result['error_code'])}"
                )

            # Revoke old assignment, notify listener
            old_assignment = list(self._assignment)
            if old_assignment:
                self._listener.on_partitions_revoked(old_assignment)

            # Parse new assignment
            new_assignment = decode_assignment(sync_result["assignment"])
            self._assignment = new_assignment
            self._listener.on_partitions_assigned(new_assignment)

            # Initialize offsets for newly assigned partitions
            self._init_offsets(new_assignment)

            logger.info(
                "Group '%s' sync complete: assigned %d partitions",
                self._group_id, len(new_assignment),
            )

        except Exception as e:
            logger.error("JoinGroup failed: %s", e, exc_info=True)
            raise
        finally:
            self._rebalancing.clear()

    def _compute_assignments(self, members: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Leader computes partition assignments for all group members.

        Uses the configured assignor strategy (range or round-robin).
        """
        # Collect all partitions across subscribed topics
        all_partitions: List[Dict[str, Any]] = []
        for topic in self._subscribed_topics:
            num_parts = self._metadata_partition_count(topic)
            for p in range(num_parts):
                all_partitions.append({"topic": topic, "partition": p})

        if not all_partitions:
            return []

        if self._config.partition_assignor == Assignor.ROUND_ROBIN:
            return self._assign_round_robin(members, all_partitions)
        else:
            # Default: range
            return self._assign_range(members, all_partitions)

    def _assign_range(
        self, members: List[Dict[str, Any]], partitions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Range-based assignment: each member gets contiguous partitions per topic."""
        # Group partitions by topic
        by_topic: Dict[str, List[int]] = defaultdict(list)
        for p in partitions:
            by_topic[p["topic"]].append(p["partition"])

        member_assignments: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        member_ids = [m["member_id"] for m in members]

        for topic, parts in by_topic.items():
            parts_sorted = sorted(parts)
            n = len(parts_sorted)
            m = len(member_ids)
            if m == 0:
                continue
            per_member = n // m
            remainder = n % m
            idx = 0
            for i, mid in enumerate(member_ids):
                count = per_member + (1 if i < remainder else 0)
                for j in range(count):
                    member_assignments[mid].append(
                        {"topic": topic, "partition": parts_sorted[idx]}
                    )
                    idx += 1

        result: List[Dict[str, Any]] = []
        for mid in member_ids:
            parts = member_assignments.get(mid, [])
            assignment_bytes = encode_assignment(parts)
            result.append({"member_id": mid, "assignment": assignment_bytes})
        return result

    def _assign_round_robin(
        self, members: List[Dict[str, Any]], partitions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Round-robin assignment: distribute partitions evenly across members."""
        member_ids = [m["member_id"] for m in members]
        if not member_ids:
            return []
        member_assignments: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for i, p in enumerate(partitions):
            mid = member_ids[i % len(member_ids)]
            member_assignments[mid].append(p)

        result: List[Dict[str, Any]] = []
        for mid in member_ids:
            parts = member_assignments.get(mid, [])
            assignment_bytes = encode_assignment(parts)
            result.append({"member_id": mid, "assignment": assignment_bytes})
        return result

    def _leave_group(self) -> None:
        """Gracefully leave the consumer group."""
        if not self._member_id:
            return
        try:
            coordinator = self._get_coordinator()
            body = encode_leave_group_request(self._group_id, self._member_id)
            coordinator.send_request(ApiKey.LEAVE_GROUP, 0, body)
            logger.info("Left group '%s'", self._group_id)
        except Exception as e:
            logger.warning("Error leaving group: %s", e)
        finally:
            self._member_id = ""
            self._generation_id = -1

    # -----------------------------------------------------------------------
    # Metadata
    # -----------------------------------------------------------------------

    def _metadata_partition_count(self, topic: str) -> int:
        """Get partition count for a topic. Fetches metadata if unknown."""
        with self._state_lock:
            if topic in self._topic_partitions:
                return len(self._topic_partitions[topic])
        # Fetch metadata
        for host, port in self._resolve_bootstrap():
            try:
                broker = _ConsumerBroker(host, port, self._config.client_id, self._config)
                broker.connect()
                body = encode_metadata_request([topic])
                response_data = broker.send_request(ApiKey.METADATA, 4, body)
                metadata = decode_metadata_response(response_data)
                broker.close()
                for b in metadata.get("brokers", []):
                    self._broker_map[b["id"]] = b
                for t in metadata.get("topics", []):
                    parts = [p["id"] for p in t.get("partitions", [])]
                    self._topic_partitions[t["name"]] = parts
                return len(self._topic_partitions.get(topic, []))
            except Exception as e:
                logger.debug("Metadata fetch failed via %s:%d: %s", host, port, e)
                broker.close()
        return 0

    # -----------------------------------------------------------------------
    # Offset Initialization
    # -----------------------------------------------------------------------

    def _init_offsets(self, assignment: List[Dict[str, Any]]) -> None:
        """Initialize fetch offsets for newly assigned partitions."""
        if not assignment:
            return

        # Try to fetch committed offsets first
        committed = self._fetch_committed_offsets(assignment)

        for part in assignment:
            topic = part["topic"]
            partition = part["partition"]
            key = (topic, partition)

            if key in committed:
                self._offsets.set_position(topic, partition, committed[key])
                self._offsets.set_committed(topic, partition, committed[key])
                continue

            # No committed offset; use auto_offset_reset
            offset = None
            if self._config.auto_offset_reset == OffsetReset.NONE:
                raise RuntimeError(
                    f"No committed offset for {topic}/{partition} and "
                    f"auto_offset_reset is 'none'"
                )
            offset = self._fetch_boundary_offset(
                topic,
                partition,
                -2 if self._config.auto_offset_reset == OffsetReset.EARLIEST else -1,
            )
            if offset is not None:
                self._offsets.set_position(topic, partition, offset)
            else:
                self._offsets.set_position(topic, partition, 0)

    def _fetch_committed_offsets(
        self, assignment: List[Dict[str, Any]]
    ) -> Dict[Tuple[str, int], int]:
        """Fetch committed offsets from the group coordinator."""
        by_topic: Dict[str, List[int]] = defaultdict(list)
        for p in assignment:
            by_topic[p["topic"]].append(p["partition"])

        topics = [{"topic": t, "partitions": sorted(parts)} for t, parts in by_topic.items()]
        if not topics:
            return {}

        try:
            coordinator = self._get_coordinator()
            body = encode_offset_fetch_request(self._group_id, topics)
            response_data = coordinator.send_request(
                ApiKey.OFFSET_FETCH, 2, body, 10000
            )
            # Parse OffsetFetch response
            # Response: throttle(4) topic_count(4) [topic(16+str) partition_count(4) [partition(4) offset(8) metadata(16+str) error_code(2)]]
            r = io.BytesIO(response_data)
            _throttle = struct.unpack(">i", r.read(4))[0]
            topic_count = struct.unpack(">i", r.read(4))[0]
            result: Dict[Tuple[str, int], int] = {}
            for _ in range(topic_count):
                t_len = struct.unpack(">h", r.read(2))[0]
                topic = r.read(t_len).decode("utf-8")
                part_count = struct.unpack(">i", r.read(4))[0]
                for _ in range(part_count):
                    part_id = struct.unpack(">i", r.read(4))[0]
                    offset = struct.unpack(">q", r.read(8))[0]
                    meta_len = struct.unpack(">h", r.read(2))[0]
                    _metadata = r.read(meta_len) if meta_len > 0 else b""
                    error_code = struct.unpack(">h", r.read(2))[0]
                    if error_code == 0 and offset >= 0:
                        result[(topic, part_id)] = offset
            return result
        except Exception as e:
            logger.warning("Failed to fetch committed offsets: %s", e)
            return {}

    def _fetch_boundary_offset(self, topic: str, partition: int, timestamp: int) -> Optional[int]:
        """Fetch earliest (-2) or latest (-1) offset for a partition."""
        for host, port in self._resolve_bootstrap():
            try:
                broker = _ConsumerBroker(host, port, self._config.client_id, self._config)
                broker.connect()
                body = encode_list_offsets_request(topic, partition, timestamp)
                response_data = broker.send_request(ApiKey.LIST_OFFSETS, 1, body)
                result = decode_list_offsets_response(response_data)
                broker.close()
                topic_result = result.get(topic, {})
                part_result = topic_result.get(partition, {})
                if part_result.get("error_code", 0) == 0:
                    offsets = part_result.get("offsets", [])
                    if offsets:
                        return offsets[0]
            except Exception as e:
                logger.debug("ListOffsets failed via %s:%d: %s", host, port, e)
                broker.close()
        return None

    # -----------------------------------------------------------------------
    # Poll
    # -----------------------------------------------------------------------

    def poll(self, timeout_ms: int = 500) -> List[ConsumerRecord]:
        """Fetch messages from assigned partitions.

        Blocks up to *timeout_ms* milliseconds for new records.

        Args:
            timeout_ms: Maximum time to block waiting for records.

        Returns:
            List of ConsumerRecord, possibly empty if no records available.
        """
        if self._rebalancing.is_set():
            time.sleep(0.1)
            return []

        records: List[ConsumerRecord] = []
        assignment = self._assignment

        if not assignment:
            time.sleep(min(timeout_ms / 1000.0, 0.5))
            return []

        max_records = self._config.max_poll_records

        for part_info in assignment:
            if len(records) >= max_records:
                break

            topic = part_info["topic"]
            partition = part_info["partition"]

            # Determine fetch offset
            position = self._offsets.get_position(topic, partition)
            if position is None:
                # Should not happen if assignment is valid
                continue

            # Fetch records from this partition
            try:
                fetched = self._fetch_partition(topic, partition, position, timeout_ms)
                for rec in fetched:
                    records.append(rec)
                    self._offsets.advance_position(topic, partition, rec.offset)
                    if len(records) >= max_records:
                        break
            except Exception as e:
                logger.warning(
                    "Fetch failed for %s/%d at offset %d: %s",
                    topic, partition, position, e,
                )

        return records

    def _fetch_partition(
        self,
        topic: str,
        partition: int,
        offset: int,
        timeout_ms: int,
    ) -> List[ConsumerRecord]:
        """Fetch records from a single partition.

        Returns:
            List of ConsumerRecord objects.
        """
        # Get partition leader
        leader = self._get_partition_leader(topic, partition)
        if leader is None:
            logger.warning("No leader for %s/%d", topic, partition)
            return []

        host, port = leader
        broker = self._get_broker(host, port)

        body = encode_fetch_request(
            topic=topic,
            partition=partition,
            fetch_offset=offset,
            min_bytes=self._config.fetch_min_bytes,
            max_bytes=self._config.max_partition_fetch_bytes,
            max_wait_ms=min(timeout_ms, self._config.fetch_max_wait_ms),
        )

        try:
            response_data = broker.send_request(
                ApiKey.FETCH, 9, body, max(timeout_ms, 5000)
            )
            result = decode_fetch_response(response_data)
        except Exception:
            return []

        records: List[ConsumerRecord] = []
        topics_data = result.get("topics", {})
        topic_data = topics_data.get(topic, {})
        part_data = topic_data.get(partition, {})

        if part_data.get("error_code", 0) != 0:
            return []

        batch_data = part_data.get("record_batch_data")
        if batch_data and len(batch_data) > 0:
            try:
                batch, _ = decode_record_batch(batch_data)
                for rec in batch.records:
                    # Decode headers
                    headers: List[Tuple[str, str]] = []
                    for hk, hv in rec.headers:
                        hv_str = hv.decode("utf-8", errors="replace") if hv else ""
                        headers.append((hk, hv_str))
                    records.append(
                        ConsumerRecord(
                            topic=topic,
                            partition=partition,
                            offset=batch.base_offset + rec.offset_delta,
                            timestamp=batch.first_timestamp + rec.timestamp_delta,
                            key=rec.key,
                            value=rec.value,
                            headers=headers,
                            leader_epoch=batch.partition_leader_epoch,
                        )
                    )
            except Exception as e:
                logger.debug("Failed to decode record batch: %s", e)

        return records

    def _get_partition_leader(self, topic: str, partition: int) -> Optional[Tuple[str, int]]:
        """Find the (host, port) of the leader for a topic-partition."""
        for host, port in self._resolve_bootstrap():
            try:
                broker = _ConsumerBroker(host, port, self._config.client_id, self._config)
                broker.connect()
                body = encode_metadata_request([topic])
                response_data = broker.send_request(ApiKey.METADATA, 4, body)
                metadata = decode_metadata_response(response_data)
                broker.close()

                # Update broker map
                for b in metadata.get("brokers", []):
                    self._broker_map[b["id"]] = b

                for t in metadata.get("topics", []):
                    if t["name"] != topic:
                        continue
                    for p in t.get("partitions", []):
                        if p["id"] == partition:
                            leader_id = p.get("leader_id", -1)
                            broker_info = self._broker_map.get(leader_id)
                            if broker_info:
                                return (broker_info["host"], broker_info["port"])
                return None
            except Exception as e:
                logger.debug("Metadata fetch failed via %s:%d: %s", host, port, e)
                broker.close()
        return None

    # -----------------------------------------------------------------------
    # Offset Commits
    # -----------------------------------------------------------------------

    def commit(self) -> None:
        """Synchronously commit current offsets to the group coordinator."""
        uncommitted = self._offsets.uncommitted_offsets()
        if not uncommitted:
            return

        # Group by topic
        by_topic: Dict[str, Dict[int, Dict[str, Any]]] = defaultdict(dict)
        for (topic, partition), offset in uncommitted.items():
            by_topic[topic][partition] = {"offset": offset, "metadata": ""}

        try:
            coordinator = self._get_coordinator()
            body = encode_offset_commit_request(
                group_id=self._group_id,
                generation_id=self._generation_id,
                member_id=self._member_id,
                offsets=by_topic,
            )
            response_data = coordinator.send_request(
                ApiKey.OFFSET_COMMIT, 2, body
            )
            result = decode_offset_commit_response(response_data)

            # Mark committed
            for topic, partitions in result.items():
                for part_id, error_code in partitions.items():
                    if error_code == 0:
                        offset = by_topic.get(topic, {}).get(part_id, {}).get("offset", 0)
                        self._offsets.set_committed(topic, part_id, offset)
                    else:
                        logger.warning(
                            "Offset commit failed for %s/%d: %s",
                            topic, part_id, error_description(error_code),
                        )
        except Exception as e:
            logger.error("Offset commit error: %s", e)

    def commit_async(self, callback: Optional[Callable[[Dict[Tuple[str, int], int], Optional[Exception]], None]] = None) -> None:
        """Asynchronously commit current offsets.

        Args:
            callback: Optional callback invoked on completion with
                (offsets_dict, exception_or_none).
        """
        uncommitted = dict(self._offsets.uncommitted_offsets())

        def _do_commit() -> None:
            try:
                self.commit()
                if callback:
                    callback(uncommitted, None)
            except Exception as e:
                if callback:
                    callback(uncommitted, e)

        t = threading.Thread(target=_do_commit, daemon=True)
        t.start()

    # -----------------------------------------------------------------------
    # Seek
    # -----------------------------------------------------------------------

    def seek(self, partition: int, offset: int, topic: Optional[str] = None) -> None:
        """Seek to a specific offset in the given partition.

        If *topic* is None, the first assigned topic matching the partition
        number is used.

        Args:
            partition: Partition number.
            offset: Target offset to seek to.
            topic: Optional topic name. Required if multiple topics are subscribed.
        """
        if topic is None:
            # Find the topic for this partition
            for a in self._assignment:
                if a["partition"] == partition:
                    topic = a["topic"]
                    break
            if topic is None:
                raise ValueError(f"No assigned topic for partition {partition}")

        self._offsets.set_position(topic, partition, offset)
        logger.info("Seek %s/%d -> %d", topic, partition, offset)

    def seek_to_beginning(self, partition: int, topic: Optional[str] = None) -> None:
        """Seek to the earliest offset of a partition.

        Args:
            partition: Partition number.
            topic: Optional topic name.
        """
        if topic is None:
            for a in self._assignment:
                if a["partition"] == partition:
                    topic = a["topic"]
                    break
            if topic is None:
                raise ValueError(f"No assigned topic for partition {partition}")

        offset = self._fetch_boundary_offset(topic, partition, -2)
        if offset is not None:
            self._offsets.set_position(topic, partition, offset)
            logger.info("Seek %s/%d -> beginning (%d)", topic, partition, offset)
        else:
            self._offsets.set_position(topic, partition, 0)

    def seek_to_end(self, partition: int, topic: Optional[str] = None) -> None:
        """Seek to the latest offset of a partition.

        Args:
            partition: Partition number.
            topic: Optional topic name.
        """
        if topic is None:
            for a in self._assignment:
                if a["partition"] == partition:
                    topic = a["topic"]
                    break
            if topic is None:
                raise ValueError(f"No assigned topic for partition {partition}")

        offset = self._fetch_boundary_offset(topic, partition, -1)
        if offset is not None:
            self._offsets.set_position(topic, partition, offset)
            logger.info("Seek %s/%d -> end (%d)", topic, partition, offset)

    # -----------------------------------------------------------------------
    # Background Tasks
    # -----------------------------------------------------------------------

    def _start_background_tasks(self) -> None:
        """Start heartbeat and auto-commit daemon threads."""
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"consumer-heartbeat-{self._group_id}",
            daemon=True,
        )
        self._heartbeat_thread.start()

        if self._config.enable_auto_commit:
            self._auto_commit_thread = threading.Thread(
                target=self._auto_commit_loop,
                name=f"consumer-autocommit-{self._group_id}",
                daemon=True,
            )
            self._auto_commit_thread.start()

    def _heartbeat_loop(self) -> None:
        """Periodic heartbeat to the group coordinator."""
        interval = self._config.heartbeat_interval_ms / 1000.0
        while self._running:
            time.sleep(interval)
            if not self._member_id or self._rebalancing.is_set():
                continue
            try:
                coordinator = self._get_coordinator()
                body = encode_heartbeat_request(
                    group_id=self._group_id,
                    generation_id=self._generation_id,
                    member_id=self._member_id,
                )
                response_data = coordinator.send_request(
                    ApiKey.HEARTBEAT, 0, body, self._config.session_timeout_ms
                )
                result = decode_heartbeat_response(response_data)
                if result["error_code"] != 0:
                    if result["error_code"] == ErrorCode.REBALANCE_IN_PROGRESS:
                        logger.info("Rebalance requested by coordinator")
                        self._join_group()
                    elif result["error_code"] == ErrorCode.ILLEGAL_GENERATION:
                        logger.warning("Illegal generation; rejoining group")
                        self._join_group()
                    elif result["error_code"] == ErrorCode.UNKNOWN_MEMBER_ID:
                        logger.warning("Unknown member ID; rejoining group")
                        self._member_id = ""
                        self._join_group()
            except Exception as e:
                logger.warning("Heartbeat failed: %s", e)

    def _auto_commit_loop(self) -> None:
        """Periodic auto-commit of offsets."""
        interval = self._config.auto_commit_interval_ms / 1000.0
        while self._running:
            time.sleep(interval)
            if self._rebalancing.is_set():
                continue
            try:
                self.commit()
            except Exception as e:
                logger.warning("Auto-commit failed: %s", e)

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Gracefully shut down the consumer.

        Commits current offsets, leaves the consumer group, stops background
        threads, and closes all broker connections.
        """
        logger.info("Shutting down consumer in group '%s'...", self._group_id)
        self._running = False

        # Commit offsets before leaving
        if self._config.enable_auto_commit:
            try:
                self.commit()
            except Exception as e:
                logger.warning("Final commit failed: %s", e)

        # Leave group
        self._leave_group()

        # Stop background threads
        for t in [self._heartbeat_thread, self._auto_commit_thread]:
            if t is not None and t.is_alive():
                t.join(timeout=5.0)

        # Close all broker connections
        with self._broker_lock:
            for broker in self._brokers.values():
                broker.close()
            self._brokers.clear()

        with self._coordinator_lock:
            if self._coordinator:
                self._coordinator.close()
                self._coordinator = None

        logger.info("Consumer shut down.")

    def __enter__(self) -> "TorrentConsumer":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
