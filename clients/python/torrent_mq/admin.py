"""
torrent-mq — TorrentAdmin: Administrative Client

The TorrentAdmin provides a programmatic interface for cluster
administration tasks. It communicates with the torrent-mq controller
broker via the admin RPC protocol to manage topics, partitions,
consumer groups, ACLs, and configurations.

Key Capabilities
----------------
- Topic lifecycle: create, delete, list, describe, alter config
- Partition management: create additional partitions
- Consumer group ops: list, describe members, delete group
- Config operations: describe/alter broker and topic configurations
- Cluster metadata: describe cluster, list brokers

Communication Model
-------------------
All operations are RPCs sent to the controller broker, discovered via
Metadata RPC. Requests include configurable timeouts; the default is
30 seconds for most operations.

Thread Safety
-------------
TorrentAdmin is thread-safe for concurrent use. All RPC calls are
synchronous and acquire internal locks.

Usage Example
-------------
    from torrent_mq import TorrentAdmin

    admin = TorrentAdmin(bootstrap_servers="localhost:9092")

    # Create a topic
    admin.create_topic("my-topic", partitions=3, replication_factor=3)

    # List all topics
    topics = admin.list_topics()

    # Describe a consumer group
    group_info = admin.describe_consumer_group("my-group")

    admin.close()
"""

from __future__ import annotations

import io
import logging
import socket
import struct
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union

from .protocol import (
    ApiKey,
    ErrorCode,
    BinaryReader,
    BinaryWriter,
    build_request,
    decode_create_topics_response,
    decode_delete_topics_response,
    decode_metadata_response,
    encode_create_topics_request,
    encode_delete_topics_request,
    encode_find_coordinator_request,
    encode_metadata_request,
    error_description,
    parse_response,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data Transfer Objects
# ---------------------------------------------------------------------------


@dataclass
class ConfigEntry:
    """A single configuration key-value pair with metadata.

    Attributes:
        name: Configuration key.
        value: Configuration value.
        is_default: Whether this is the default value.
        is_sensitive: Whether this is a sensitive value.
        is_read_only: Whether this config is read-only.
        source: Source of the config ('static', 'dynamic', 'default').
    """

    name: str
    value: str
    is_default: bool = False
    is_sensitive: bool = False
    is_read_only: bool = False
    source: str = ""


@dataclass
class PartitionInfo:
    """Metadata about a single partition.

    Attributes:
        partition_id: Partition number.
        leader_id: Broker ID of the current leader (-1 if none).
        replica_ids: List of broker IDs hosting replicas.
        isr_ids: List of broker IDs that are in-sync.
        log_start_offset: Earliest available offset.
        log_end_offset: Latest offset (next to be written).
    """

    partition_id: int
    leader_id: int = -1
    replica_ids: List[int] = field(default_factory=list)
    isr_ids: List[int] = field(default_factory=list)
    log_start_offset: int = 0
    log_end_offset: int = 0


@dataclass
class TopicDescription:
    """Full description of a topic.

    Attributes:
        name: Topic name.
        is_internal: Whether this is an internal topic.
        num_partitions: Number of partitions.
        replication_factor: Replication factor.
        partitions: List of per-partition details.
        configs: List of ConfigEntry objects.
    """

    name: str
    is_internal: bool = False
    num_partitions: int = 0
    replication_factor: int = 0
    partitions: List[PartitionInfo] = field(default_factory=list)
    configs: List[ConfigEntry] = field(default_factory=list)


@dataclass
class BrokerInfo:
    """Metadata about a broker in the cluster.

    Attributes:
        broker_id: Unique broker ID.
        host: Broker hostname.
        port: Broker port.
        rack: Rack identifier (may be empty).
    """

    broker_id: int
    host: str
    port: int
    rack: str = ""


@dataclass
class ClusterDescription:
    """Top-level cluster metadata.

    Attributes:
        cluster_id: Cluster identifier.
        controller_id: Broker ID of the current controller.
        brokers: List of BrokerInfo objects.
        topics: List of topic names.
    """

    cluster_id: str = ""
    controller_id: int = -1
    brokers: List[BrokerInfo] = field(default_factory=list)
    topics: List[str] = field(default_factory=list)


@dataclass
class ConsumerGroupMember:
    """A member of a consumer group.

    Attributes:
        member_id: Unique member identifier.
        client_id: Client ID.
        client_host: Client host address.
        assignment: Partition assignment dict.
    """

    member_id: str = ""
    client_id: str = ""
    client_host: str = ""
    assignment: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConsumerGroupDescription:
    """Description of a consumer group.

    Attributes:
        group_id: Consumer group identifier.
        state: Current state ('Stable', 'PreparingRebalance', etc.).
        protocol_type: Protocol type (usually 'consumer').
        protocol: Partition assignment protocol in use.
        members: List of ConsumerGroupMember.
        coordinator_id: Broker ID of the group coordinator.
    """

    group_id: str = ""
    state: str = ""
    protocol_type: str = ""
    protocol: str = ""
    members: List[ConsumerGroupMember] = field(default_factory=list)
    coordinator_id: int = -1


@dataclass
class ConsumerGroupSummary:
    """Summary of a consumer group (lighter than full description)."""

    group_id: str = ""
    protocol_type: str = ""
    state: str = ""


# ---------------------------------------------------------------------------
# Broker Connection (Admin)
# ---------------------------------------------------------------------------


class _AdminConnection:
    """Low-level TCP connection to a broker for admin operations."""

    def __init__(self, host: str, port: int, client_id: str, timeout_ms: int = 30000) -> None:
        self._host = host
        self._port = port
        self._client_id = client_id
        self._timeout_ms = timeout_ms
        self._sock: Optional[socket.socket] = None
        self._lock = threading.Lock()
        self._correlation_id = 0
        self._connected = False

    def connect(self) -> None:
        with self._lock:
            if self._connected and self._sock:
                return
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.settimeout(self._timeout_ms / 1000.0)
            self._sock.connect((self._host, self._port))
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

    def _next_correlation_id(self) -> int:
        self._correlation_id += 1
        return self._correlation_id

    def send_request(self, api_key: int, api_version: int, body: bytes, timeout_ms: Optional[int] = None) -> bytes:
        """Send a request and return the response body.

        Args:
            api_key: API key.
            api_version: API version.
            body: Serialized request body.
            timeout_ms: Override timeout for this request.

        Returns:
            Raw response body bytes.

        Raises:
            ConnectionError: On connection failure.
            TimeoutError: If response not received in time.
        """
        effective_timeout = timeout_ms or self._timeout_ms
        correlation_id = self._next_correlation_id()
        request_bytes = build_request(
            api_key=api_key,
            api_version=api_version,
            correlation_id=correlation_id,
            client_id=self._client_id,
            body=body,
        )

        with self._lock:
            if not self._connected or self._sock is None:
                self.connect()
            try:
                self._sock.sendall(request_bytes)  # type: ignore[union-attr]
            except OSError as e:
                self._connected = False
                raise ConnectionError(f"Send failed: {e}") from e

            # Read response with timeout
            self._sock.settimeout(effective_timeout / 1000.0)  # type: ignore[union-attr]
            try:
                size_bytes = self._recv_exact(4)
                if size_bytes is None:
                    raise ConnectionError("Connection closed")
                size = struct.unpack(">i", size_bytes)[0]
                body = self._recv_exact(size)
                if body is None:
                    raise ConnectionError("Connection closed")
            except socket.timeout:
                raise TimeoutError(f"Request timed out after {effective_timeout}ms")
            finally:
                self._sock.settimeout(self._timeout_ms / 1000.0)  # type: ignore[union-attr]

        response = parse_response(size_bytes + body)
        return response.data

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
# TorrentAdmin
# ---------------------------------------------------------------------------


class TorrentAdmin:
    """Administrative client for torrent-mq cluster management.

    Provides methods for creating/deleting topics, managing partitions,
    listing consumer groups, altering configurations, and inspecting
    cluster state.

    Args:
        bootstrap_servers: Comma-separated 'host:port' entries.
        client_id: Client identifier for logging.
        request_timeout_ms: Default timeout for admin requests.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        client_id: str = "torrent-admin",
        request_timeout_ms: int = 30000,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._request_timeout_ms = request_timeout_ms
        self._connections: Dict[Tuple[str, int], _AdminConnection] = {}
        self._conn_lock = threading.Lock()

        # Discover cluster metadata on init
        self._broker_map: Dict[int, Dict[str, Any]] = {}
        self._controller_id: int = -1
        self._cluster_id: str = ""
        self._topic_map: Dict[str, Any] = {}
        self._refresh_metadata()

    def _resolve_bootstrap(self) -> List[Tuple[str, int]]:
        servers: List[Tuple[str, int]] = []
        for entry in self._bootstrap_servers.split(","):
            entry = entry.strip()
            if not entry:
                continue
            if ":" in entry:
                host, port_s = entry.rsplit(":", 1)
                servers.append((host.strip(), int(port_s)))
            else:
                servers.append((entry.strip(), 9092))
        return servers

    def _get_connection(self, host: str, port: int) -> _AdminConnection:
        key = (host, port)
        with self._conn_lock:
            if key not in self._connections:
                conn = _AdminConnection(host, port, self._client_id, self._request_timeout_ms)
                conn.connect()
                self._connections[key] = conn
            return self._connections[key]

    def _get_controller_connection(self) -> _AdminConnection:
        """Get a connection to the controller broker."""
        if self._controller_id >= 0:
            broker = self._broker_map.get(self._controller_id)
            if broker:
                return self._get_connection(broker["host"], broker["port"])
        # Fall back to any bootstrap server
        for host, port in self._resolve_bootstrap():
            return self._get_connection(host, port)
        raise ConnectionError("No available broker")

    def _get_any_connection(self) -> _AdminConnection:
        """Get a connection to any available broker."""
        # Try controller first, then any broker, then bootstrap
        if self._controller_id >= 0:
            broker = self._broker_map.get(self._controller_id)
            if broker:
                try:
                    return self._get_connection(broker["host"], broker["port"])
                except Exception:
                    pass
        for broker_info in self._broker_map.values():
            try:
                return self._get_connection(broker_info["host"], broker_info["port"])
            except Exception:
                pass
        for host, port in self._resolve_bootstrap():
            return self._get_connection(host, port)
        raise ConnectionError("No available broker")

    def _refresh_metadata(self) -> None:
        """Refresh cluster metadata."""
        for host, port in self._resolve_bootstrap():
            try:
                conn = self._get_connection(host, port)
                body = encode_metadata_request(None)
                response_data = conn.send_request(ApiKey.METADATA, 4, body)
                metadata = decode_metadata_response(response_data)

                self._broker_map = {b["id"]: b for b in metadata.get("brokers", [])}
                self._controller_id = metadata.get("controller_id", -1)
                self._cluster_id = metadata.get("cluster_id", "")
                self._topic_map = {t["name"]: t for t in metadata.get("topics", [])}
                logger.info(
                    "Connected to %s:%d - %d brokers, %d topics",
                    host, port,
                    len(metadata.get("brokers", [])),
                    len(metadata.get("topics", [])),
                )
                return
            except Exception as e:
                logger.debug("Metadata refresh failed via %s:%d: %s", host, port, e)
        raise ConnectionError("Could not refresh cluster metadata")

    # -----------------------------------------------------------------------
    # Topic Management
    # -----------------------------------------------------------------------

    def create_topic(
        self,
        name: str,
        partitions: int = 1,
        replication_factor: int = 1,
        configs: Optional[Dict[str, Any]] = None,
        timeout_ms: Optional[int] = None,
    ) -> bool:
        """Create a new topic.

        Args:
            name: Topic name.
            partitions: Number of partitions. Must be >= 1.
            replication_factor: Replication factor. Must be >= 1.
            configs: Dict of configuration key-value pairs (e.g.
                {'retention.ms': '86400000', 'compression.type': 'lz4'}).
            timeout_ms: Operation timeout. Defaults to instance timeout.

        Returns:
            True if the topic was created successfully.

        Raises:
            RuntimeError: If the topic already exists or creation fails.
        """
        if partitions < 1:
            raise ValueError("partitions must be >= 1")
        if replication_factor < 1:
            raise ValueError("replication_factor must be >= 1")

        config_list: List[Dict[str, Any]] = []
        if configs:
            for k, v in configs.items():
                config_list.append({"name": k, "value": str(v)})

        topics = [
            {
                "name": name,
                "num_partitions": partitions,
                "replication_factor": replication_factor,
                "configs": config_list,
            }
        ]

        try:
            conn = self._get_controller_connection()
            body = encode_create_topics_request(topics, timeout_ms or self._request_timeout_ms)
            response_data = conn.send_request(ApiKey.CREATE_TOPICS, 2, body, timeout_ms)
            result = decode_create_topics_response(response_data)
            topic_result = result.get(name, {})
            error_code = topic_result.get("error_code", 0)

            if error_code == ErrorCode.TOPIC_ALREADY_EXISTS:
                raise RuntimeError(f"Topic '{name}' already exists")
            elif error_code == ErrorCode.NONE:
                logger.info("Created topic '%s' with %d partitions", name, partitions)
                self._refresh_metadata()
                return True
            else:
                raise RuntimeError(
                    f"Failed to create topic '{name}': {error_description(error_code)}"
                )
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"Failed to create topic '{name}': {e}") from e

    def delete_topic(self, name: str, timeout_ms: Optional[int] = None) -> bool:
        """Delete a topic.

        Args:
            name: Topic name to delete.
            timeout_ms: Operation timeout.

        Returns:
            True if the topic was deleted.

        Raises:
            RuntimeError: If deletion fails.
        """
        try:
            conn = self._get_controller_connection()
            body = encode_delete_topics_request([name], timeout_ms or self._request_timeout_ms)
            response_data = conn.send_request(ApiKey.DELETE_TOPICS, 0, body, timeout_ms)
            result = decode_delete_topics_response(response_data)
            topic_result = result.get(name, {})
            error_code = topic_result.get("error_code", 0)

            if error_code == ErrorCode.NONE:
                logger.info("Deleted topic '%s'", name)
                self._refresh_metadata()
                return True
            else:
                raise RuntimeError(
                    f"Failed to delete topic '{name}': {error_description(error_code)}"
                )
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"Failed to delete topic '{name}': {e}") from e

    def list_topics(self) -> List[str]:
        """List all topic names in the cluster.

        Returns:
            Alphabetically sorted list of topic names.
        """
        self._refresh_metadata()
        return sorted(self._topic_map.keys())

    def describe_topic(self, name: str) -> TopicDescription:
        """Get detailed metadata about a topic.

        Args:
            name: Topic name.

        Returns:
            TopicDescription with partition info and configs.
        """
        self._refresh_metadata()
        topic_data = self._topic_map.get(name)
        if topic_data is None:
            raise RuntimeError(f"Topic '{name}' not found")

        partitions = []
        for p in topic_data.get("partitions", []):
            partitions.append(
                PartitionInfo(
                    partition_id=p["id"],
                    leader_id=p.get("leader_id", -1),
                    replica_ids=p.get("replicas", []),
                    isr_ids=p.get("isr", []),
                )
            )

        configs = self.describe_configs("topic", name)

        return TopicDescription(
            name=name,
            is_internal=topic_data.get("is_internal", False),
            num_partitions=len(partitions),
            replication_factor=len(partitions[0].replica_ids) if partitions else 0,
            partitions=partitions,
            configs=configs,
        )

    def create_partitions(self, name: str, count: int, timeout_ms: Optional[int] = None) -> bool:
        """Increase the partition count of an existing topic.

        Note: Partition count can only be increased, not decreased.

        Args:
            name: Topic name.
            count: New total partition count (must be > current count).
            timeout_ms: Operation timeout.

        Returns:
            True on success.
        """
        # Encode CreatePartitions request (api_key=38)
        w = BinaryWriter()
        w.write_array(1)  # topic count
        w.write_string(name)
        w.write_int32(count)
        # assignments (null for automatic)
        w.write_array(-1)
        w.write_int32(timeout_ms or self._request_timeout_ms)
        w.write_bool(False)  # validate_only

        try:
            conn = self._get_controller_connection()
            response_data = conn.send_request(ApiKey.CREATE_PARTITIONS, 0, w.buffer(), timeout_ms)
            # Parse response: throttle(4) topic_count(4) [name(16+str) error_code(2) error_msg(16+str?)]
            r = BinaryReader(response_data)
            _throttle = r.read_int32()
            topic_count = r.read_array_length()
            for _ in range(topic_count):
                topic_name = r.read_string() or ""
                error_code = r.read_int16()
                _error_msg = r.read_string()
                if topic_name == name:
                    if error_code == ErrorCode.NONE:
                        logger.info("Increased partitions for '%s' to %d", name, count)
                        self._refresh_metadata()
                        return True
                    raise RuntimeError(
                        f"Failed to create partitions for '{name}': "
                        f"{error_description(error_code)}"
                    )
            raise RuntimeError(f"No response for topic '{name}'")
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"Failed to create partitions: {e}") from e

    # -----------------------------------------------------------------------
    # Consumer Group Management
    # -----------------------------------------------------------------------

    def list_consumer_groups(self) -> List[ConsumerGroupSummary]:
        """List all consumer groups in the cluster.

        Returns:
            List of ConsumerGroupSummary objects.
        """
        try:
            conn = self._get_any_connection()
            # ListGroups request: empty body for v0
            body = b""
            response_data = conn.send_request(ApiKey.LIST_GROUPS, 0, body)
            # Parse: error_code(2) group_count(4) [group_id(16+str) protocol_type(16+str)]
            r = BinaryReader(response_data)
            error_code = r.read_int16()
            if error_code != 0:
                raise RuntimeError(f"ListGroups failed: {error_description(error_code)}")
            count = r.read_array_length()
            groups: List[ConsumerGroupSummary] = []
            for _ in range(count):
                group_id = r.read_string() or ""
                protocol_type = r.read_string() or ""
                groups.append(
                    ConsumerGroupSummary(
                        group_id=group_id,
                        protocol_type=protocol_type,
                        state="Unknown",  # v0 doesn't return state
                    )
                )
            return groups
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"Failed to list consumer groups: {e}") from e

    def describe_consumer_group(self, group_id: str) -> ConsumerGroupDescription:
        """Get detailed metadata about a consumer group.

        Args:
            group_id: Consumer group identifier.

        Returns:
            ConsumerGroupDescription with member info.
        """
        # First find the coordinator
        coord_info = self._find_coordinator(group_id)

        try:
            conn = self._get_connection(coord_info["host"], coord_info["port"])

            # DescribeGroups request: group_count(4) [group_id(16+str)]
            w = BinaryWriter()
            w.write_array(1)
            w.write_string(group_id)
            response_data = conn.send_request(ApiKey.DESCRIBE_GROUPS, 2, w.buffer())

            r = BinaryReader(response_data)
            _throttle = r.read_int32()
            count = r.read_array_length()

            for _ in range(count):
                error_code = r.read_int16()
                gid = r.read_string() or ""
                state = r.read_string() or ""
                protocol_type = r.read_string() or ""
                protocol = r.read_string() or ""
                member_count = r.read_array_length()

                members: List[ConsumerGroupMember] = []
                for _ in range(member_count):
                    mid = r.read_string() or ""
                    cid = r.read_string() or ""
                    host = r.read_string() or ""
                    meta_bytes = r.read_bytes() or b""
                    assignment_data = r.read_bytes() or b""

                    # Parse assignment if available
                    assignment: Dict[str, Any] = {}
                    if assignment_data and len(assignment_data) > 0:
                        try:
                            ar = BinaryReader(assignment_data)
                            _version = ar.read_int16()
                            topic_count = ar.read_array_length()
                            topic_assignments: Dict[str, List[int]] = {}
                            for _ in range(topic_count):
                                tname = ar.read_string() or ""
                                part_count = ar.read_array_length()
                                parts = [ar.read_int32() for _ in range(part_count)]
                                topic_assignments[tname] = parts
                            assignment = {"topics": topic_assignments}
                        except Exception:
                            pass

                    members.append(
                        ConsumerGroupMember(
                            member_id=mid,
                            client_id=cid,
                            client_host=host,
                            assignment=assignment,
                        )
                    )

                return ConsumerGroupDescription(
                    group_id=gid,
                    state=state,
                    protocol_type=protocol_type,
                    protocol=protocol,
                    members=members,
                    coordinator_id=coord_info["node_id"],
                )

            raise RuntimeError(f"Consumer group '{group_id}' not found in response")

        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"Failed to describe consumer group '{group_id}': {e}"
            ) from e

    def delete_consumer_group(self, group_id: str) -> bool:
        """Delete a consumer group.

        Args:
            group_id: Consumer group identifier to delete.

        Returns:
            True on success.

        Raises:
            RuntimeError: If deletion fails or group is not empty.
        """
        # Find coordinator and send DeleteGroups request
        coord_info = self._find_coordinator(group_id)

        try:
            conn = self._get_connection(coord_info["host"], coord_info["port"])

            w = BinaryWriter()
            w.write_array(1)
            w.write_string(group_id)
            response_data = conn.send_request(ApiKey.DELETE_GROUPS, 0, w.buffer())

            # Parse: throttle(4) group_count(4) [group_id(16+str) error_code(2)]
            r = BinaryReader(response_data)
            _throttle = r.read_int32()
            count = r.read_array_length()
            for _ in range(count):
                gid = r.read_string() or ""
                error_code = r.read_int16()
                if gid == group_id:
                    if error_code == ErrorCode.NONE:
                        logger.info("Deleted consumer group '%s'", group_id)
                        return True
                    elif error_code == ErrorCode.NON_EMPTY_GROUP:
                        raise RuntimeError(f"Consumer group '{group_id}' is not empty")
                    else:
                        raise RuntimeError(
                            f"Failed to delete consumer group '{group_id}': "
                            f"{error_description(error_code)}"
                        )
            raise RuntimeError(f"No response for group '{group_id}'")
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"Failed to delete consumer group '{group_id}': {e}"
            ) from e

    def _find_coordinator(self, group_id: str) -> Dict[str, Any]:
        """Locate the group coordinator for a consumer group."""
        for host, port in self._resolve_bootstrap():
            try:
                conn = self._get_connection(host, port)
                body = encode_find_coordinator_request(group_id)
                response_data = conn.send_request(ApiKey.FIND_COORDINATOR, 1, body)
                r = BinaryReader(response_data)
                _throttle = r.read_int32()
                error_code = r.read_int16()
                _error_msg = r.read_string()
                node_id = r.read_int32()
                coord_host = r.read_string() or ""
                coord_port = r.read_int32()
                if error_code == 0:
                    return {"node_id": node_id, "host": coord_host, "port": coord_port}
                raise RuntimeError(f"FindCoordinator failed: {error_description(error_code)}")
            except RuntimeError:
                raise
            except Exception as e:
                logger.debug("FindCoordinator via %s:%d failed: %s", host, port, e)
        raise ConnectionError(f"Could not find coordinator for group '{group_id}'")

    # -----------------------------------------------------------------------
    # Config Management
    # -----------------------------------------------------------------------

    def describe_configs(
        self, resource_type: str, resource_name: str
    ) -> List[ConfigEntry]:
        """Describe configurations for a resource.

        Args:
            resource_type: 'broker' or 'topic'.
            resource_name: Resource name (broker ID as string, or topic name).

        Returns:
            List of ConfigEntry objects.
        """
        resource_type_id = 2 if resource_type.lower() == "broker" else 4
        # For broker, parse as int
        if resource_type.lower() == "broker":
            try:
                name_val: Any = int(resource_name)
            except ValueError:
                name_val = resource_name
        else:
            name_val = resource_name

        try:
            conn = self._get_any_connection()

            # DescribeConfigs request v0: resource_count(4) [type(1) name(16+str) config_count(4) [config_name(16+str)]]
            w = BinaryWriter()
            w.write_array(1)
            w.write_int8(resource_type_id)
            w.write_string(str(name_val))
            w.write_array(-1)  # null = all configs

            response_data = conn.send_request(ApiKey.DESCRIBE_CONFIGS, 0, w.buffer())

            # Parse response
            r = BinaryReader(response_data)
            _throttle = r.read_int32()
            res_count = r.read_array_length()
            configs: List[ConfigEntry] = []

            for _ in range(res_count):
                error_code = r.read_int16()
                _error_msg = r.read_string()
                _rtype = r.read_int8()
                _rname = r.read_string()
                entry_count = r.read_array_length()
                for _ in range(entry_count):
                    name = r.read_string() or ""
                    value = r.read_string() or ""
                    read_only = r.read_bool()
                    is_default = r.read_bool()
                    sensitive = r.read_bool()
                    configs.append(
                        ConfigEntry(
                            name=name,
                            value="[REDACTED]" if sensitive else value,
                            is_default=is_default,
                            is_sensitive=sensitive,
                            is_read_only=read_only,
                        )
                    )
            return configs
        except Exception as e:
            logger.warning("describe_configs(%s, %s) failed: %s", resource_type, resource_name, e)
            return []

    def alter_configs(
        self,
        resource_type: str,
        resource_name: str,
        configs: Dict[str, Any],
        timeout_ms: Optional[int] = None,
    ) -> bool:
        """Alter configurations for a resource.

        Args:
            resource_type: 'broker' or 'topic'.
            resource_name: Resource name.
            configs: Dict of config key-value pairs to set.
            timeout_ms: Operation timeout.

        Returns:
            True on success.
        """
        resource_type_id = 2 if resource_type.lower() == "broker" else 4

        config_list = [
            {"name": k, "value": str(v)} for k, v in configs.items()
        ]

        try:
            conn = self._get_controller_connection()

            # AlterConfigs request v0: resource_count(4) [type(1) name(16+str) config_count(4) [name(16+str) value(16+str)]]
            w = BinaryWriter()
            w.write_array(1)
            w.write_int8(resource_type_id)
            w.write_string(resource_name)
            w.write_array(len(config_list))
            for c in config_list:
                w.write_string(c["name"])
                w.write_string(c["value"])
            w.write_bool(False)  # validate_only

            response_data = conn.send_request(
                ApiKey.ALTER_CONFIGS, 0, w.buffer(), timeout_ms
            )

            # Parse: throttle(4) resource_count(4) [error_code(2) error_msg(16+str) type(1) name(16+str)]
            r = BinaryReader(response_data)
            _throttle = r.read_int32()
            res_count = r.read_array_length()
            for _ in range(res_count):
                error_code = r.read_int16()
                _error_msg = r.read_string()
                _rtype = r.read_int8()
                _rname = r.read_string()
                if error_code == ErrorCode.NONE:
                    logger.info("Configs altered for %s '%s'", resource_type, resource_name)
                    return True
                raise RuntimeError(
                    f"alter_configs failed for {resource_type} '{resource_name}': "
                    f"{error_description(error_code)}"
                )
            raise RuntimeError("No response received for alter_configs")
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"alter_configs failed: {e}") from e

    # -----------------------------------------------------------------------
    # Cluster Metadata
    # -----------------------------------------------------------------------

    def describe_cluster(self) -> ClusterDescription:
        """Get a description of the entire cluster.

        Returns:
            ClusterDescription with broker list and topic names.
        """
        self._refresh_metadata()

        brokers = [
            BrokerInfo(
                broker_id=bid,
                host=info.get("host", ""),
                port=info.get("port", 0),
                rack=info.get("rack", ""),
            )
            for bid, info in self._broker_map.items()
        ]

        return ClusterDescription(
            cluster_id=self._cluster_id,
            controller_id=self._controller_id,
            brokers=brokers,
            topics=self.list_topics(),
        )

    def list_brokers(self) -> List[BrokerInfo]:
        """List all brokers in the cluster.

        Returns:
            List of BrokerInfo objects.
        """
        self._refresh_metadata()
        return [
            BrokerInfo(
                broker_id=bid,
                host=info.get("host", ""),
                port=info.get("port", 0),
                rack=info.get("rack", ""),
            )
            for bid, info in self._broker_map.items()
        ]

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Close all broker connections."""
        with self._conn_lock:
            for conn in self._connections.values():
                conn.close()
            self._connections.clear()
        logger.info("Admin client shut down.")

    def __enter__(self) -> "TorrentAdmin":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
