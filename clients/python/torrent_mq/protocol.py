"""
torrent-mq — Low-Level Wire Protocol Implementation

This module implements the Kafka-compatible binary wire protocol used by
torrent-mq. It handles request/response framing, message encoding/decoding,
primitive type serialization, CRC32C computation, and error code mapping.

Wire Format Overview
--------------------
Each request is framed as:
    [4 bytes: message_size] [payload]

The payload consists of:
    [2 bytes: api_key]
    [2 bytes: api_version]
    [4 bytes: correlation_id]
    [2 bytes: client_id_length] [client_id: UTF-8 bytes]
    [tagged fields (if flexible version)] [request_body]

Responses mirror this structure:
    [4 bytes: message_size] [4 bytes: correlation_id] [response_body]

Primitive Type Serialization
----------------------------
- INT8/16/32/64: big-endian, two's complement
- UINT8/16/32/64: big-endian, unsigned
- VARINT: unsigned LEB128 with zigzag for signed
- STRING/NULLABLE_STRING: 2-byte length prefix (or -1 for null), followed by UTF-8 bytes
- COMPACT_STRING/NULLABLE_COMPACT_STRING: unsigned varint length, followed by UTF-8 bytes
- BYTES/NULLABLE_BYTES: 4-byte length prefix, followed by raw bytes
- COMPACT_BYTES/NULLABLE_COMPACT_BYTES: unsigned varint length, followed by raw bytes
- ARRAY: 4-byte count, followed by elements
- COMPACT_ARRAY: unsigned varint count, followed by elements
- RECORD_BATCH: v2 record batch format with CRC32C integrity

CRC32C
------
Uses the Castagnoli polynomial (0x1EDC6F41), computed via slicing-by-8
software algorithm for portability. On platforms with SSE4.2 hardware
acceleration, the internal implementation uses CPU intrinsics via the
`crc32c` package if available.
"""

from __future__ import annotations

import enum
import struct
import time
import zlib
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

# ---------------------------------------------------------------------------
# Try hardware-accelerated CRC32C; fall back to software
# ---------------------------------------------------------------------------

try:
    from crc32c import crc32c as _hw_crc32c

    _CRC32C_HW = True
except ImportError:
    _CRC32C_HW = False

# CRC32C table for slicing-by-8 (Castagnoli 0x1EDC6F41 reflected)
_CRC32C_TABLE: List[int] = []


def _build_crc32c_table() -> List[int]:
    """Build the 256-entry CRC32C lookup table."""
    table: List[int] = []
    for i in range(256):
        crc = i
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0x82F63B78
            else:
                crc >>= 1
        table.append(crc)
    return table


_CRC32C_TABLE = _build_crc32c_table()


def crc32c(data: bytes, prev_crc: int = 0) -> int:
    """Compute CRC32C (Castagnoli) checksum over *data*.

    If the optional `crc32c` package is installed, uses hardware-accelerated
    CRC32C via SSE4.2 intrinsics. Falls back to a pure-Python slicing-by-8
    implementation otherwise.

    Args:
        data: The byte string to checksum.
        prev_crc: Previous CRC value for incremental computation.

    Returns:
        32-bit unsigned CRC value.
    """
    if _CRC32C_HW:
        return _hw_crc32c(data, prev_crc)  # type: ignore[no-any-return]
    crc = prev_crc ^ 0xFFFFFFFF
    table = _CRC32C_TABLE
    # Process 8 bytes at a time where possible (slicing-by-8)
    i = 0
    n = len(data)
    while n - i >= 8:
        crc ^= (
            data[i]
            | (data[i + 1] << 8)
            | (data[i + 2] << 16)
            | (data[i + 3] << 24)
        )
        crc = (
            table[(crc >> 56) & 0xFF]
            ^ table[((crc >> 48) & 0xFF) + 0x100]
            ^ table[((crc >> 40) & 0xFF) + 0x200]
            ^ table[((crc >> 32) & 0xFF) + 0x300]
            ^ table[(data[i + 4] & 0xFF) + 0x400]
            ^ table[(data[i + 5] & 0xFF) + 0x500]
            ^ table[(data[i + 6] & 0xFF) + 0x600]
            ^ table[(data[i + 7] & 0xFF) + 0x700]
        )
        i += 8
    # Process remaining bytes
    while i < n:
        crc = table[(crc & 0xFF) ^ data[i]] ^ (crc >> 8)
        i += 1
    return crc ^ 0xFFFFFFFF


# ============================================================================
# API Key Constants
# ============================================================================


class ApiKey(enum.IntEnum):
    """Kafka / torrent-mq API keys."""

    PRODUCE = 0
    FETCH = 1
    LIST_OFFSETS = 2
    METADATA = 3
    OFFSET_COMMIT = 8
    OFFSET_FETCH = 9
    FIND_COORDINATOR = 10
    JOIN_GROUP = 11
    HEARTBEAT = 12
    LEAVE_GROUP = 13
    SYNC_GROUP = 14
    DESCRIBE_GROUPS = 15
    LIST_GROUPS = 16
    API_VERSIONS = 18
    CREATE_TOPICS = 19
    DELETE_TOPICS = 20
    DELETE_RECORDS = 21
    INIT_PRODUCER_ID = 22
    OFFSET_FOR_LEADER_EPOCH = 23
    ADD_PARTITIONS_TO_TXN = 24
    ADD_OFFSETS_TO_TXN = 25
    END_TXN = 26
    WRITE_TXN_MARKERS = 27
    TXN_OFFSET_COMMIT = 28
    DESCRIBE_ACLS = 29
    CREATE_ACLS = 30
    DELETE_ACLS = 31
    DESCRIBE_CONFIGS = 32
    ALTER_CONFIGS = 33
    ALTER_REPLICA_LOG_DIRS = 34
    DESCRIBE_LOG_DIRS = 35
    SASL_HANDSHAKE = 36
    SASL_AUTHENTICATE = 37
    CREATE_PARTITIONS = 38
    CREATE_DELEGATION_TOKEN = 39
    RENEW_DELEGATION_TOKEN = 40
    EXPIRE_DELEGATION_TOKEN = 41
    DESCRIBE_DELEGATION_TOKEN = 42
    DELETE_GROUPS = 43
    ELECT_LEADERS = 44
    INCREMENTAL_ALTER_CONFIGS = 45


# ============================================================================
# Error Codes
# ============================================================================


class ErrorCode(enum.IntEnum):
    """Kafka / torrent-mq error codes."""

    UNKNOWN_SERVER_ERROR = -1
    NONE = 0
    OFFSET_OUT_OF_RANGE = 1
    CORRUPT_MESSAGE = 2
    UNKNOWN_TOPIC_OR_PARTITION = 3
    INVALID_FETCH_SIZE = 4
    LEADER_NOT_AVAILABLE = 5
    NOT_LEADER_FOR_PARTITION = 6
    REQUEST_TIMED_OUT = 7
    BROKER_NOT_AVAILABLE = 8
    REPLICA_NOT_AVAILABLE = 9
    MESSAGE_TOO_LARGE = 10
    STALE_CONTROLLER_EPOCH = 11
    OFFSET_METADATA_TOO_LARGE = 12
    NETWORK_EXCEPTION = 13
    COORDINATOR_LOAD_IN_PROGRESS = 14
    COORDINATOR_NOT_AVAILABLE = 15
    NOT_COORDINATOR = 16
    INVALID_TOPIC_EXCEPTION = 17
    RECORD_LIST_TOO_LARGE = 18
    NOT_ENOUGH_REPLICAS = 19
    NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20
    INVALID_REQUIRED_ACKS = 21
    ILLEGAL_GENERATION = 22
    INCONSISTENT_GROUP_PROTOCOL = 23
    INVALID_GROUP_ID = 24
    UNKNOWN_MEMBER_ID = 25
    INVALID_SESSION_TIMEOUT = 26
    REBALANCE_IN_PROGRESS = 27
    INVALID_COMMIT_OFFSET_SIZE = 28
    TOPIC_AUTHORIZATION_FAILED = 29
    GROUP_AUTHORIZATION_FAILED = 30
    CLUSTER_AUTHORIZATION_FAILED = 31
    INVALID_TIMESTAMP = 32
    UNSUPPORTED_SASL_MECHANISM = 33
    ILLEGAL_SASL_STATE = 34
    UNSUPPORTED_VERSION = 35
    TOPIC_ALREADY_EXISTS = 36
    INVALID_PARTITIONS = 37
    INVALID_REPLICATION_FACTOR = 38
    INVALID_REPLICA_ASSIGNMENT = 39
    INVALID_CONFIG = 40
    NOT_CONTROLLER = 41
    INVALID_REQUEST = 42
    UNSUPPORTED_FOR_MESSAGE_FORMAT = 43
    POLICY_VIOLATION = 44
    OUT_OF_ORDER_SEQUENCE_NUMBER = 45
    DUPLICATE_SEQUENCE_NUMBER = 46
    INVALID_PRODUCER_EPOCH = 47
    INVALID_TXN_STATE = 48
    INVALID_PRODUCER_ID_MAPPING = 49
    INVALID_TRANSACTION_TIMEOUT = 50
    CONCURRENT_TRANSACTIONS = 51
    TRANSACTION_COORDINATOR_FENCED = 52
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53
    SECURITY_DISABLED = 54
    OPERATION_NOT_ATTEMPTED = 55
    KAFKA_STORAGE_ERROR = 56
    LOG_DIR_NOT_FOUND = 57
    SASL_AUTHENTICATION_FAILED = 58
    UNKNOWN_PRODUCER_ID = 59
    REASSIGNMENT_IN_PROGRESS = 60
    DELEGATION_TOKEN_AUTH_DISABLED = 61
    DELEGATION_TOKEN_NOT_FOUND = 62
    DELEGATION_TOKEN_OWNER_MISMATCH = 63
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64
    DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65
    DELEGATION_TOKEN_EXPIRED = 66
    INVALID_PRINCIPAL_TYPE = 67
    NON_EMPTY_GROUP = 68
    GROUP_ID_NOT_FOUND = 69
    FETCH_SESSION_ID_NOT_FOUND = 70
    INVALID_FETCH_SESSION_EPOCH = 71
    LISTENER_NOT_FOUND = 72
    TOPIC_DELETION_DISABLED = 73
    FENCED_LEADER_EPOCH = 74
    UNKNOWN_LEADER_EPOCH = 75
    UNSUPPORTED_COMPRESSION_TYPE = 76
    STALE_BROKER_EPOCH = 77
    OFFSET_NOT_AVAILABLE = 78
    MEMBER_ID_REQUIRED = 79
    PREFERRED_LEADER_NOT_AVAILABLE = 80
    GROUP_MAX_SIZE_REACHED = 81
    FENCED_INSTANCE_ID = 82
    ELIGIBLE_LEADERS_NOT_AVAILABLE = 83
    ELECTION_NOT_NEEDED = 84
    NO_REASSIGNMENT_IN_PROGRESS = 85
    GROUP_SUBSCRIBED_TO_TOPIC = 86
    INVALID_RECORD = 87
    UNSTABLE_OFFSET_COMMIT = 88
    UNKNOWN_ERROR = -1


# Error code descriptions for human-readable messages
_ERROR_DESCRIPTIONS: Dict[int, str] = {
    -1: "The server experienced an unexpected error when processing the request.",
    0: "No error.",
    1: "The requested offset is not within the range of offsets maintained by the server.",
    2: "The message is corrupt.",
    3: "This server does not host this topic-partition.",
    5: "There is no leader for this topic-partition.",
    6: "This server is not the leader for that topic-partition.",
    7: "The request timed out.",
    8: "The broker is not available.",
    10: "The message is larger than the maximum allowed size.",
    14: "The coordinator is loading (retry).",
    15: "The coordinator is not available.",
    16: "This is not the correct coordinator.",
    17: "The request attempted to perform an operation on an invalid topic.",
    19: "The number of in-sync replicas is lower than the minimum required.",
    22: "The generation ID provided is not the current generation.",
    25: "The member ID is not currently known.",
    26: "The requested session timeout is outside the allowed range.",
    27: "The group is rebalancing, so a rejoin is needed.",
    29: "Topic authorization failed.",
    30: "Group authorization failed.",
    36: "Topic with this name already exists.",
    37: "Number of partitions is below 1.",
    38: "Replication factor is below 1 or larger than number of brokers.",
    41: "This is not the correct controller for this cluster.",
    45: "The sequence number is out of order.",
    46: "Duplicate sequence number.",
    68: "The group is not empty.",
    69: "The group ID does not exist.",
}


def error_description(code: int) -> str:
    """Return a human-readable description for *code*."""
    return _ERROR_DESCRIPTIONS.get(code, f"Unknown error code: {code}")


# ============================================================================
# Binary Serialization Primitives
# ============================================================================


class BinaryWriter:
    """Efficient binary buffer writer for constructing wire-format messages.

    Provides methods for writing primitive types in big-endian Kafka wire
    format. Tracks the current position internally — no explicit seeking.
    """

    def __init__(self, capacity: int = 1024) -> None:
        self._buf = bytearray(capacity)
        self._pos = 0

    def _ensure(self, extra: int) -> None:
        required = self._pos + extra
        if required > len(self._buf):
            new_len = max(required, len(self._buf) * 2)
            new_buf = bytearray(new_len)
            new_buf[: self._pos] = self._buf[: self._pos]
            self._buf = new_buf

    def tell(self) -> int:
        return self._pos

    def buffer(self) -> bytes:
        return bytes(self._buf[: self._pos])

    # -- Fixed-width integers (big-endian) --

    def write_int8(self, value: int) -> None:
        self._ensure(1)
        self._buf[self._pos] = value & 0xFF
        self._pos += 1

    def write_int16(self, value: int) -> None:
        self._ensure(2)
        struct.pack_into(">h", self._buf, self._pos, value)
        self._pos += 2

    def write_int32(self, value: int) -> None:
        self._ensure(4)
        struct.pack_into(">i", self._buf, self._pos, value)
        self._pos += 4

    def write_int64(self, value: int) -> None:
        self._ensure(8)
        struct.pack_into(">q", self._buf, self._pos, value)
        self._pos += 8

    # -- Unsigned varint (LEB128) --

    def write_unsigned_varint(self, value: int) -> None:
        """Write an unsigned varint (LEB128)."""
        self._ensure(10)
        while value > 0x7F:
            self._buf[self._pos] = (value & 0x7F) | 0x80
            self._pos += 1
            value >>= 7
        self._buf[self._pos] = value & 0x7F
        self._pos += 1

    def write_varint(self, value: int) -> None:
        """Write a signed varint (zigzag + LEB128)."""
        # Zigzag encode: map signed to unsigned
        zigzag = (value << 1) ^ (value >> 63)
        self.write_unsigned_varint(zigzag)

    # -- Strings --

    def write_string(self, value: Optional[str]) -> None:
        """Write a nullable string (2-byte length prefix, -1 for null)."""
        if value is None:
            self.write_int16(-1)
            return
        data = value.encode("utf-8")
        self.write_int16(len(data))
        self._ensure(len(data))
        self._buf[self._pos : self._pos + len(data)] = data
        self._pos += len(data)

    def write_compact_string(self, value: Optional[str]) -> None:
        """Write a compact nullable string (varint length, N+1 for non-null)."""
        if value is None:
            self.write_unsigned_varint(0)
            return
        data = value.encode("utf-8")
        self.write_unsigned_varint(len(data) + 1)
        self._ensure(len(data))
        self._buf[self._pos : self._pos + len(data)] = data
        self._pos += len(data)

    # -- Bytes --

    def write_bytes(self, value: Optional[bytes]) -> None:
        """Write nullable bytes (4-byte length prefix, -1 for null)."""
        if value is None:
            self.write_int32(-1)
            return
        self.write_int32(len(value))
        self._ensure(len(value))
        self._buf[self._pos : self._pos + len(value)] = value
        self._pos += len(value)

    def write_compact_bytes(self, value: Optional[bytes]) -> None:
        """Write compact nullable bytes (varint length)."""
        if value is None:
            self.write_unsigned_varint(0)
            return
        self.write_unsigned_varint(len(value) + 1)
        self._ensure(len(value))
        self._buf[self._pos : self._pos + len(value)] = value
        self._pos += len(value)

    # -- Arrays --

    def write_array(self, count: int) -> None:
        """Write an array length prefix (4-byte signed)."""
        self.write_int32(count)

    def write_compact_array(self, count: int) -> None:
        """Write a compact array length prefix (varint, count + 1)."""
        self.write_unsigned_varint(count + 1)

    # -- Convenience --

    def write_tagged_fields(self) -> None:
        """Write an empty tagged field section (single varint zero)."""
        self.write_unsigned_varint(0)

    def write_bool(self, value: bool) -> None:
        self.write_int8(1 if value else 0)


class BinaryReader:
    """Efficient binary buffer reader for parsing wire-format messages."""

    def __init__(self, data: bytes, offset: int = 0) -> None:
        self._data = data
        self._pos = offset

    def tell(self) -> int:
        return self._pos

    def remaining(self) -> int:
        return len(self._data) - self._pos

    def _check(self, n: int) -> None:
        if self._pos + n > len(self._data):
            raise ValueError(
                f"Buffer underflow: need {n} bytes but only "
                f"{len(self._data) - self._pos} remaining at position {self._pos}"
            )

    def read_int8(self) -> int:
        self._check(1)
        val = self._data[self._pos]
        self._pos += 1
        return val

    def read_int16(self) -> int:
        self._check(2)
        val: int = struct.unpack_from(">h", self._data, self._pos)[0]
        self._pos += 2
        return val

    def read_int32(self) -> int:
        self._check(4)
        val: int = struct.unpack_from(">i", self._data, self._pos)[0]
        self._pos += 4
        return val

    def read_int64(self) -> int:
        self._check(8)
        val: int = struct.unpack_from(">q", self._data, self._pos)[0]
        self._pos += 8
        return val

    def read_unsigned_varint(self) -> int:
        """Read an unsigned varint (LEB128)."""
        value = 0
        shift = 0
        while True:
            self._check(1)
            byte = self._data[self._pos]
            self._pos += 1
            value |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                break
            shift += 7
        return value

    def read_varint(self) -> int:
        """Read a signed varint (zigzag + LEB128)."""
        zigzag = self.read_unsigned_varint()
        # Zigzag decode
        return (zigzag >> 1) ^ -(zigzag & 1)

    def read_string(self) -> Optional[str]:
        """Read a nullable string."""
        length = self.read_int16()
        if length == -1:
            return None
        self._check(length)
        s = self._data[self._pos : self._pos + length].decode("utf-8")
        self._pos += length
        return s

    def read_compact_string(self) -> Optional[str]:
        """Read a compact nullable string."""
        length = self.read_unsigned_varint() - 1
        if length < 0:
            return None
        self._check(length)
        s = self._data[self._pos : self._pos + length].decode("utf-8")
        self._pos += length
        return s

    def read_bytes(self) -> Optional[bytes]:
        """Read nullable bytes."""
        length = self.read_int32()
        if length == -1:
            return None
        self._check(length)
        b = self._data[self._pos : self._pos + length]
        self._pos += length
        return b

    def read_compact_bytes(self) -> Optional[bytes]:
        """Read compact nullable bytes."""
        length = self.read_unsigned_varint() - 1
        if length < 0:
            return None
        self._check(length)
        b = self._data[self._pos : self._pos + length]
        self._pos += length
        return b

    def read_array_length(self) -> int:
        return self.read_int32()

    def read_compact_array_length(self) -> int:
        return self.read_unsigned_varint() - 1

    def read_tagged_fields(self) -> None:
        """Read tagged fields until we hit a zero varint."""
        while True:
            tag = self.read_unsigned_varint()
            if tag == 0:
                break
            length = self.read_unsigned_varint()
            self._pos += length

    def read_bool(self) -> bool:
        return self.read_int8() != 0

    def slice(self, length: int) -> bytes:
        self._check(length)
        b = self._data[self._pos : self._pos + length]
        self._pos += length
        return b


# ============================================================================
# Record Batch (v2 / Kafka 0.11+)
# ============================================================================

RECORD_BATCH_MAGIC = 2
RECORD_BATCH_OVERHEAD = 61  # Fixed header bytes before records array
NO_PRODUCER_ID = -1
NO_PRODUCER_EPOCH = -1
NO_SEQUENCE = -1


@dataclass
class Record:
    """A single record within a record batch.

    Attributes:
        offset_delta: Relative offset within the batch.
        timestamp_delta: Milliseconds since batch `first_timestamp`.
        key: Optional record key bytes.
        value: Record value bytes.
        headers: List of (key, value) header pairs.
    """

    offset_delta: int = 0
    timestamp_delta: int = 0
    key: Optional[bytes] = None
    value: Optional[bytes] = None
    headers: List[Tuple[str, bytes]] = field(default_factory=list)


@dataclass
class RecordBatch:
    """A v2 record batch (Kafka 0.11+ magic byte).

    Encapsulates a group of records sharing the same partition leader epoch,
    producer ID, and compression scheme.

    Attributes:
        base_offset: Offset of the first record in the batch.
        partition_leader_epoch: Leader epoch at time of produce.
        magic: Always 2 for v2 batches.
        attributes: Bitfield for compression (bits 0-2) and timestamp type (bit 3).
        last_offset_delta: Offset delta of the last record.
        first_timestamp: Timestamp of the first record (ms since epoch).
        max_timestamp: Maximum timestamp in the batch.
        producer_id: Producer ID (-1 if not idempotent).
        producer_epoch: Producer epoch.
        base_sequence: Sequence number of the first record.
        records: List of Record objects.
    """

    base_offset: int = 0
    partition_leader_epoch: int = -1
    magic: int = RECORD_BATCH_MAGIC
    attributes: int = 0
    last_offset_delta: int = 0
    first_timestamp: int = 0
    max_timestamp: int = 0
    producer_id: int = NO_PRODUCER_ID
    producer_epoch: int = NO_PRODUCER_EPOCH
    base_sequence: int = NO_SEQUENCE
    records: List[Record] = field(default_factory=list)

    # Compression type constants (bits 0-2 of attributes)
    COMPRESSION_NONE = 0
    COMPRESSION_GZIP = 1
    COMPRESSION_SNAPPY = 2
    COMPRESSION_LZ4 = 3
    COMPRESSION_ZSTD = 4

    @property
    def compression_type(self) -> int:
        return self.attributes & 0x07

    @compression_type.setter
    def compression_type(self, value: int) -> None:
        self.attributes = (self.attributes & ~0x07) | (value & 0x07)

    @property
    def timestamp_type(self) -> int:
        return (self.attributes >> 3) & 0x01

    @timestamp_type.setter
    def timestamp_type(self, value: int) -> None:
        if value:
            self.attributes |= 0x08
        else:
            self.attributes &= ~0x08

    @property
    def is_transactional(self) -> bool:
        return bool(self.attributes & 0x10)

    @is_transactional.setter
    def is_transactional(self, value: bool) -> None:
        if value:
            self.attributes |= 0x10
        else:
            self.attributes &= ~0x10

    @property
    def is_control_batch(self) -> bool:
        return bool(self.attributes & 0x20)


def encode_record_batch(batch: RecordBatch) -> bytes:
    """Encode a v2 record batch to wire-format bytes with CRC32C.

    The format is:
        base_offset (8) + batch_length (4) + partition_leader_epoch (4) +
        magic (1) + crc (4) + attributes (2) + last_offset_delta (4) +
        first_timestamp (8) + max_timestamp (8) + producer_id (8) +
        producer_epoch (2) + base_sequence (4) + record_count (4) +
        [records...]

    The CRC covers everything from attributes through the end of the
    records array.
    """
    w = BinaryWriter()

    # Build records first so we know sizes
    record_bufs: List[bytes] = []
    for i, rec in enumerate(batch.records):
        rw = BinaryWriter()
        rw.write_varint(len(rec.value) if rec.value else 0)
        rw.write_varint(0)  # attributes
        rw.write_varint(rec.timestamp_delta)
        rw.write_varint(rec.offset_delta)
        rw.write_varint(len(rec.key) if rec.key else 0)
        rw.write_varint(0 if rec.key is None else len(rec.key))
        if rec.key:
            rw._ensure(len(rec.key))
            rw._buf[rw._pos : rw._pos + len(rec.key)] = rec.key
            rw._pos += len(rec.key)
        rw.write_varint(0 if rec.value is None else len(rec.value))
        if rec.value:
            rw._ensure(len(rec.value))
            rw._buf[rw._pos : rw._pos + len(rec.value)] = rec.value
            rw._pos += len(rec.value)
        rw.write_varint(len(rec.headers))
        for hk, hv in rec.headers:
            rw.write_compact_string(hk)
            rw.write_compact_bytes(hv)
        record_bufs.append(rw.buffer())

    # Fixed header (excluding base_offset and batch_length, which we write at known positions)
    # We'll compute CRC over [attributes..end_of_records]
    # Layout: base_offset(8) batch_length(4) leader_epoch(4) magic(1) crc(4) ...
    header_w = BinaryWriter()
    header_w.write_int64(batch.base_offset)
    # Placeholder for batch_length — will fill in later
    header_w.write_int32(0)
    header_w.write_int32(batch.partition_leader_epoch)
    header_w.write_int8(batch.magic)
    # Placeholder for CRC
    header_w.write_int32(0)
    header_w.write_int16(batch.attributes)
    header_w.write_int32(batch.last_offset_delta)
    header_w.write_int64(batch.first_timestamp)
    header_w.write_int64(batch.max_timestamp)
    header_w.write_int64(batch.producer_id)
    header_w.write_int16(batch.producer_epoch)
    header_w.write_int32(batch.base_sequence)
    header_w.write_int32(len(batch.records))

    # Concatenate record buffers
    records_body = b"".join(record_bufs)

    # Compute CRC over [attributes .. end of records]
    crc_body = struct.pack(">h", batch.attributes)
    crc_body += struct.pack(">i", batch.last_offset_delta)
    crc_body += struct.pack(">q", batch.first_timestamp)
    crc_body += struct.pack(">q", batch.max_timestamp)
    crc_body += struct.pack(">q", batch.producer_id)
    crc_body += struct.pack(">h", batch.producer_epoch)
    crc_body += struct.pack(">i", batch.base_sequence)
    crc_body += struct.pack(">i", len(batch.records))
    crc_body += records_body

    crc = crc32c(crc_body)

    # Patch CRC and batch_length
    full = bytearray(header_w.buffer())
    struct.pack_into(">i", full, 12, len(crc_body) - 9)  # batch_length (everything after it, i.e. magic+crc+body)
    struct.pack_into(">i", full, 17, crc)  # CRC
    result = bytes(full) + records_body

    # Patch actual batch_length in position 8-11
    batch_body_len = len(result) - 12  # everything after base_offset + batch_length header
    final = bytearray(result)
    struct.pack_into(">i", final, 8, batch_body_len)
    return bytes(final)


def decode_record_batch(data: bytes, offset: int = 0) -> Tuple[RecordBatch, int]:
    """Decode a v2 record batch from wire-format bytes.

    Args:
        data: Raw bytes containing the record batch.
        offset: Starting position in *data*.

    Returns:
        Tuple of (decoded RecordBatch, bytes_consumed).

    Raises:
        ValueError: If CRC32C verification fails or magic byte is wrong.
    """
    r = BinaryReader(data, offset)
    start = offset

    base_offset = r.read_int64()
    batch_length = r.read_int32()
    partition_leader_epoch = r.read_int32()
    magic = r.read_int8()

    if magic != RECORD_BATCH_MAGIC:
        raise ValueError(f"Unsupported record batch magic: {magic} (expected {RECORD_BATCH_MAGIC})")

    crc = r.read_int32()
    attributes = r.read_int16()
    last_offset_delta = r.read_int32()
    first_timestamp = r.read_int64()
    max_timestamp = r.read_int64()
    producer_id = r.read_int64()
    producer_epoch = r.read_int16()
    base_sequence = r.read_int32()
    record_count = r.read_int32()

    # Verify CRC
    crc_start = start + 21  # position of attributes in the stream
    crc_data = data[crc_start : start + 12 + batch_length]
    computed_crc = crc32c(crc_data)
    if computed_crc != crc:
        raise ValueError(
            f"Record batch CRC mismatch: computed {computed_crc:08x}, "
            f"expected {crc:08x}"
        )

    # Decode records
    records: List[Record] = []
    for _ in range(record_count):
        body_len = r.read_varint()
        _attrs = r.read_varint()
        timestamp_delta = r.read_varint()
        offset_delta = r.read_varint()
        key_len = r.read_varint()
        key: Optional[bytes] = r.slice(key_len) if key_len > 0 else None
        value_len = r.read_varint()
        value: Optional[bytes] = r.slice(value_len) if value_len > 0 else None
        header_count = r.read_varint()
        headers: List[Tuple[str, bytes]] = []
        for _ in range(header_count):
            hk = r.read_compact_string() or ""
            hv = r.read_compact_bytes() or b""
            headers.append((hk, hv))
        records.append(
            Record(
                offset_delta=offset_delta,
                timestamp_delta=timestamp_delta,
                key=key,
                value=value,
                headers=headers,
            )
        )

    batch = RecordBatch(
        base_offset=base_offset,
        partition_leader_epoch=partition_leader_epoch,
        magic=magic,
        attributes=attributes,
        last_offset_delta=last_offset_delta,
        first_timestamp=first_timestamp,
        max_timestamp=max_timestamp,
        producer_id=producer_id,
        producer_epoch=producer_epoch,
        base_sequence=base_sequence,
        records=records,
    )

    consumed = 12 + batch_length
    return batch, consumed


# ============================================================================
# Request / Response Framing
# ============================================================================


@dataclass
class Request:
    """A framed request ready to send over the wire.

    Attributes:
        correlation_id: Monotonically increasing ID for matching response.
        api_key: API key identifying the RPC.
        api_version: Version of the API to use.
        data: Serialized request body bytes.
    """

    correlation_id: int
    api_key: int
    api_version: int
    data: bytes


@dataclass
class Response:
    """A framed response received from the broker.

    Attributes:
        correlation_id: Matches the request's correlation ID.
        data: Raw response body bytes (may need further parsing).
    """

    correlation_id: int
    data: bytes


def build_request(
    api_key: int,
    api_version: int,
    correlation_id: int,
    client_id: str,
    body: bytes,
) -> bytes:
    """Frame a request for the wire.

    Format:
        [4: message_size] [2: api_key] [2: api_version] [4: correlation_id]
        [2: client_id_len] [client_id_bytes] [body...]

    The message_size = 2 + 2 + 4 + 2 + len(client_id) + len(body).
    """
    client_bytes = client_id.encode("utf-8")
    header_size = 2 + 2 + 4 + 2 + len(client_bytes)
    total_size = header_size + len(body)

    buf = bytearray(4 + total_size)
    struct.pack_into(">i", buf, 0, total_size)
    struct.pack_into(">h", buf, 4, api_key)
    struct.pack_into(">h", buf, 6, api_version)
    struct.pack_into(">i", buf, 8, correlation_id)
    struct.pack_into(">h", buf, 12, len(client_bytes))
    buf[14 : 14 + len(client_bytes)] = client_bytes
    buf[14 + len(client_bytes) :] = body
    return bytes(buf)


def parse_response(data: bytes) -> Response:
    """Parse a response frame from the wire.

    Format:
        [4: message_size] [4: correlation_id] [body...]
    """
    if len(data) < 8:
        raise ValueError(f"Response too short: {len(data)} bytes (need at least 8)")
    message_size = struct.unpack_from(">i", data, 0)[0]
    correlation_id = struct.unpack_from(">i", data, 4)[0]
    body = data[8 : 4 + message_size]
    return Response(correlation_id=correlation_id, data=body)


# ============================================================================
# Metadata Serde
# ============================================================================


def encode_metadata_request(topics: Optional[List[str]] = None) -> bytes:
    """Encode a Metadata request (api_key=3, version 4).

    Args:
        topics: List of topic names to query, or None for all topics.
    """
    w = BinaryWriter()
    if topics is None:
        w.write_array(-1)  # null array = all topics
    else:
        w.write_array(len(topics))
        for t in topics:
            w.write_string(t)
    w.write_bool(False)  # allow_auto_topic_creation
    return w.buffer()


def decode_metadata_response(data: bytes) -> Dict[str, Any]:
    """Decode a Metadata response (version 4).

    Returns a dictionary with keys: 'brokers', 'controller_id', 'topics'.
    Each broker is {id, host, port}. Each topic is {name, partitions: [{id, leader, replicas, isr}]}.
    """
    r = BinaryReader(data)
    _throttle = r.read_int32()
    broker_count = r.read_array_length()
    brokers: List[Dict[str, Any]] = []
    for _ in range(broker_count):
        broker_id = r.read_int32()
        host = r.read_string() or ""
        port = r.read_int32()
        r.read_string()  # rack (unused)
        brokers.append({"id": broker_id, "host": host, "port": port})
    r.read_string()  # cluster_id
    controller_id = r.read_int32()
    topic_count = r.read_array_length()
    topics: List[Dict[str, Any]] = []
    for _ in range(topic_count):
        error_code = r.read_int16()
        topic_name = r.read_string() or ""
        is_internal = r.read_bool()
        partition_count = r.read_array_length()
        partitions: List[Dict[str, Any]] = []
        for _ in range(partition_count):
            error_code = r.read_int16()
            part_id = r.read_int32()
            leader_id = r.read_int32()
            leader_epoch = r.read_int32()
            replica_count = r.read_array_length()
            replicas = [r.read_int32() for _ in range(replica_count)]
            isr_count = r.read_array_length()
            isr = [r.read_int32() for _ in range(isr_count)]
            partitions.append(
                {
                    "id": part_id,
                    "leader_id": leader_id,
                    "leader_epoch": leader_epoch,
                    "replicas": replicas,
                    "isr": isr,
                    "error_code": error_code,
                }
            )
        topics.append(
            {
                "name": topic_name,
                "is_internal": is_internal,
                "partitions": partitions,
                "error_code": error_code,
            }
        )
    return {"brokers": brokers, "controller_id": controller_id, "topics": topics}


# ============================================================================
# Produce Serde
# ============================================================================


def encode_produce_request(
    topic: str,
    partition: int,
    record_batch: bytes,
    acks: int = 1,
    timeout_ms: int = 30000,
    transactional_id: Optional[str] = None,
) -> bytes:
    """Encode a Produce request (api_key=0, version 7)."""
    w = BinaryWriter()
    w.write_string(transactional_id)
    w.write_int16(acks)
    w.write_int32(timeout_ms)
    w.write_array(1)  # topic count
    w.write_string(topic)
    w.write_array(1)  # partition count
    w.write_int32(partition)
    w.write_int32(len(record_batch))
    w._ensure(len(record_batch))
    w._buf[w._pos : w._pos + len(record_batch)] = record_batch
    w._pos += len(record_batch)
    return w.buffer()


def decode_produce_response(data: bytes) -> Dict[str, Any]:
    """Decode a Produce response.

    Returns a dict mapping topic_name -> {partition_id: {error_code, base_offset, ...}}.
    """
    r = BinaryReader(data)
    topic_count = r.read_array_length()
    result: Dict[str, Dict[int, Dict[str, Any]]] = {}
    for _ in range(topic_count):
        topic_name = r.read_string() or ""
        partition_count = r.read_array_length()
        topic_result: Dict[int, Dict[str, Any]] = {}
        for _ in range(partition_count):
            part_id = r.read_int32()
            error_code = r.read_int16()
            base_offset = r.read_int64()
            log_append_time_ms = r.read_int64()
            log_start_offset = r.read_int64()
            topic_result[part_id] = {
                "error_code": error_code,
                "base_offset": base_offset,
                "log_append_time_ms": log_append_time_ms,
                "log_start_offset": log_start_offset,
            }
        result[topic_name] = topic_result
    _throttle = r.read_int32()
    return result


# ============================================================================
# Fetch Serde
# ============================================================================


def encode_fetch_request(
    topic: str,
    partition: int,
    fetch_offset: int,
    min_bytes: int = 1,
    max_bytes: int = 1048576,
    max_wait_ms: int = 500,
    session_id: int = 0,
    session_epoch: int = 0,
) -> bytes:
    """Encode a Fetch request (api_key=1, version 9)."""
    w = BinaryWriter()
    w.write_int32(-1)  # replica_id
    w.write_int32(max_wait_ms)
    w.write_int32(min_bytes)
    w.write_int32(max_bytes)
    w.write_int8(0)  # isolation_level
    w.write_int32(session_id)
    w.write_int32(session_epoch)
    w.write_array(1)  # topic count
    w.write_string(topic)
    w.write_array(1)  # partition count
    w.write_int32(partition)
    w.write_int64(fetch_offset)  # current_leader_epoch (unused in non-idempotent)
    w.write_int64(fetch_offset)
    w.write_int64(-1)  # log_start_offset
    w.write_int32(max_bytes)
    return w.buffer()


def decode_fetch_response(data: bytes) -> Dict[str, Any]:
    """Decode a Fetch response.

    Returns a dict with topic/partition results and record batches.
    """
    r = BinaryReader(data)
    _throttle = r.read_int32()
    _error_code = r.read_int16()
    session_id = r.read_int32()
    topic_count = r.read_array_length()
    result: Dict[str, Any] = {"session_id": session_id, "topics": {}}
    for _ in range(topic_count):
        topic_name = r.read_string() or ""
        partition_count = r.read_array_length()
        partitions: Dict[int, Dict[str, Any]] = {}
        for _ in range(partition_count):
            part_id = r.read_int32()
            error_code = r.read_int16()
            high_watermark = r.read_int64()
            last_stable_offset = r.read_int64()
            log_start_offset = r.read_int64()
            _aborted_count = r.read_array_length()
            # Skip aborted transactions
            record_batch_data = None
            if error_code == 0:
                batch_len = r.read_int32()
                if batch_len > 0:
                    record_batch_data = r.slice(batch_len)
            partitions[part_id] = {
                "error_code": error_code,
                "high_watermark": high_watermark,
                "last_stable_offset": last_stable_offset,
                "log_start_offset": log_start_offset,
                "record_batch_data": record_batch_data,
            }
        result["topics"][topic_name] = partitions
    return result


# ============================================================================
# Group Protocol Serde
# ============================================================================


def encode_find_coordinator_request(group_id: str, coordinator_type: int = 0) -> bytes:
    """Encode a FindCoordinator request (api_key=10, version 1).

    coordinator_type: 0 = group, 1 = transaction.
    """
    w = BinaryWriter()
    w.write_string(group_id)
    w.write_int8(coordinator_type)
    return w.buffer()


def decode_find_coordinator_response(data: bytes) -> Dict[str, Any]:
    """Decode a FindCoordinator response."""
    r = BinaryReader(data)
    _throttle = r.read_int32()
    error_code = r.read_int16()
    _error_msg = r.read_string()
    node_id = r.read_int32()
    host = r.read_string() or ""
    port = r.read_int32()
    return {"error_code": error_code, "node_id": node_id, "host": host, "port": port}


def encode_join_group_request(
    group_id: str,
    session_timeout_ms: int,
    rebalance_timeout_ms: int,
    member_id: str,
    protocol_type: str,
    protocols: List[Dict[str, Any]],
) -> bytes:
    """Encode a JoinGroup request (api_key=11, version 2)."""
    w = BinaryWriter()
    w.write_string(group_id)
    w.write_int32(session_timeout_ms)
    w.write_int32(rebalance_timeout_ms)
    w.write_string(member_id)
    w.write_string(protocol_type)
    w.write_array(len(protocols))
    for p in protocols:
        w.write_string(p["name"])
        metadata = p.get("metadata", b"")
        if isinstance(metadata, str):
            metadata = metadata.encode("utf-8")
        w.write_bytes(metadata)
    return w.buffer()


def decode_join_group_response(data: bytes) -> Dict[str, Any]:
    """Decode a JoinGroup response."""
    r = BinaryReader(data)
    _throttle = r.read_int32()
    error_code = r.read_int16()
    generation_id = r.read_int32()
    protocol_name = r.read_string()
    leader_id = r.read_string()
    member_id = r.read_string()
    member_count = r.read_array_length()
    members: List[Dict[str, Any]] = []
    for _ in range(member_count):
        mid = r.read_string() or ""
        metadata = r.read_bytes() or b""
        members.append({"member_id": mid, "metadata": metadata})
    return {
        "error_code": error_code,
        "generation_id": generation_id,
        "protocol_name": protocol_name,
        "leader_id": leader_id,
        "member_id": member_id,
        "members": members,
    }


def encode_sync_group_request(
    group_id: str,
    generation_id: int,
    member_id: str,
    assignments: List[Dict[str, Any]],
) -> bytes:
    """Encode a SyncGroup request (api_key=14)."""
    w = BinaryWriter()
    w.write_string(group_id)
    w.write_int32(generation_id)
    w.write_string(member_id)
    w.write_array(len(assignments))
    for a in assignments:
        w.write_string(a["member_id"])
        assignment_data = a.get("assignment", b"")
        if isinstance(assignment_data, str):
            assignment_data = assignment_data.encode("utf-8")
        w.write_bytes(assignment_data)
    return w.buffer()


def decode_sync_group_response(data: bytes) -> Dict[str, Any]:
    """Decode a SyncGroup response."""
    r = BinaryReader(data)
    _throttle = r.read_int32()
    error_code = r.read_int16()
    assignment = r.read_bytes()
    return {"error_code": error_code, "assignment": assignment}


def encode_heartbeat_request(
    group_id: str,
    generation_id: int,
    member_id: str,
) -> bytes:
    """Encode a Heartbeat request (api_key=12)."""
    w = BinaryWriter()
    w.write_string(group_id)
    w.write_int32(generation_id)
    w.write_string(member_id)
    return w.buffer()


def decode_heartbeat_response(data: bytes) -> Dict[str, Any]:
    """Decode a Heartbeat response."""
    r = BinaryReader(data)
    _throttle = r.read_int32()
    error_code = r.read_int16()
    return {"error_code": error_code}


def encode_leave_group_request(group_id: str, member_id: str) -> bytes:
    """Encode a LeaveGroup request (api_key=13)."""
    w = BinaryWriter()
    w.write_string(group_id)
    w.write_string(member_id)
    return w.buffer()


def encode_offset_commit_request(
    group_id: str,
    generation_id: int,
    member_id: str,
    offsets: Dict[str, Dict[int, Dict[str, Any]]],
) -> bytes:
    """Encode an OffsetCommit request (api_key=8, version 2)."""
    w = BinaryWriter()
    w.write_string(group_id)
    w.write_int32(generation_id)
    w.write_string(member_id)
    # retention_time_ms (not supported in v2)
    w.write_array(len(offsets))
    for topic_name, partitions in offsets.items():
        w.write_string(topic_name)
        w.write_array(len(partitions))
        for part_id, info in partitions.items():
            w.write_int32(part_id)
            w.write_int64(info.get("offset", 0))
            w.write_string(info.get("metadata", ""))
    return w.buffer()


def decode_offset_commit_response(data: bytes) -> Dict[str, Any]:
    """Decode an OffsetCommit response."""
    r = BinaryReader(data)
    topic_count = r.read_array_length()
    result: Dict[str, Dict[int, int]] = {}
    for _ in range(topic_count):
        topic_name = r.read_string() or ""
        partition_count = r.read_array_length()
        topic_result: Dict[int, int] = {}
        for _ in range(partition_count):
            part_id = r.read_int32()
            error_code = r.read_int16()
            topic_result[part_id] = error_code
        result[topic_name] = topic_result
    return result


def encode_offset_fetch_request(group_id: str, topics: List[Dict[str, Any]]) -> bytes:
    """Encode an OffsetFetch request (api_key=9)."""
    w = BinaryWriter()
    w.write_string(group_id)
    w.write_array(len(topics))
    for t in topics:
        w.write_string(t["topic"])
        w.write_array(len(t["partitions"]))
        for pid in t["partitions"]:
            w.write_int32(pid)
    return w.buffer()


# ============================================================================
# Admin Serde
# ============================================================================


def encode_create_topics_request(
    topics: List[Dict[str, Any]],
    timeout_ms: int = 30000,
    validate_only: bool = False,
) -> bytes:
    """Encode a CreateTopics request (api_key=19, version 2)."""
    w = BinaryWriter()
    w.write_array(len(topics))
    for t in topics:
        w.write_string(t["name"])
        w.write_int32(t.get("num_partitions", 1))
        w.write_int16(t.get("replication_factor", 1))
        # replica_assignment
        w.write_array(-1)  # none
        configs = t.get("configs", [])
        w.write_array(len(configs))
        for c in configs:
            w.write_string(c["name"])
            w.write_string(str(c.get("value", "")))
    w.write_int32(timeout_ms)
    w.write_bool(validate_only)
    return w.buffer()


def decode_create_topics_response(data: bytes) -> Dict[str, Any]:
    """Decode a CreateTopics response."""
    r = BinaryReader(data)
    topic_count = r.read_array_length()
    result: Dict[str, Any] = {}
    for _ in range(topic_count):
        topic_name = r.read_string() or ""
        error_code = r.read_int16()
        _error_msg = r.read_string()
        result[topic_name] = {"error_code": error_code}
    return result


def encode_delete_topics_request(
    topic_names: List[str],
    timeout_ms: int = 30000,
) -> bytes:
    """Encode a DeleteTopics request (api_key=20)."""
    w = BinaryWriter()
    w.write_array(len(topic_names))
    for name in topic_names:
        w.write_string(name)
    w.write_int32(timeout_ms)
    return w.buffer()


def decode_delete_topics_response(data: bytes) -> Dict[str, Any]:
    """Decode a DeleteTopics response."""
    r = BinaryReader(data)
    topic_count = r.read_array_length()
    result: Dict[str, Any] = {}
    for _ in range(topic_count):
        topic_name = r.read_string() or ""
        error_code = r.read_int16()
        result[topic_name] = {"error_code": error_code}
    return result


def encode_list_offsets_request(
    topic: str,
    partition: int,
    timestamp: int = -1,
    max_num_offsets: int = 1,
) -> bytes:
    """Encode a ListOffsets request (api_key=2).

    timestamp: -1=latest, -2=earliest, >=0=specific ms timestamp.
    """
    w = BinaryWriter()
    w.write_int32(-1)  # replica_id
    w.write_int32(0)   # isolation_level
    w.write_array(1)
    w.write_string(topic)
    w.write_array(1)
    w.write_int32(partition)
    w.write_int64(timestamp)
    w.write_int32(max_num_offsets)
    return w.buffer()


def decode_list_offsets_response(data: bytes) -> Dict[str, Any]:
    """Decode a ListOffsets response."""
    r = BinaryReader(data)
    _throttle = r.read_int32()
    topic_count = r.read_array_length()
    result: Dict[str, Any] = {}
    for _ in range(topic_count):
        topic_name = r.read_string() or ""
        partition_count = r.read_array_length()
        topic_result: Dict[int, Any] = {}
        for _ in range(partition_count):
            part_id = r.read_int32()
            error_code = r.read_int16()
            offset_count = r.read_array_length()
            offsets = [r.read_int64() for _ in range(offset_count)]
            topic_result[part_id] = {"error_code": error_code, "offsets": offsets}
        result[topic_name] = topic_result
    return result


# ============================================================================
# Partition Assignment Serde (for consumer group protocol)
# ============================================================================


def encode_assignment(partitions: List[Dict[str, Any]]) -> bytes:
    """Encode a consumer group assignment (range/round-robin format).

    Format: version(2) + topic_count(4) + [topic(16+str) + partition_count(4) + [partition(4)]]
    """
    w = BinaryWriter()
    w.write_int16(0)  # version
    # Group by topic
    by_topic: Dict[str, List[int]] = {}
    for p in partitions:
        by_topic.setdefault(p["topic"], []).append(p["partition"])
    w.write_array(len(by_topic))
    for topic_name, parts in by_topic.items():
        w.write_string(topic_name)
        w.write_array(len(parts))
        for pid in sorted(parts):
            w.write_int32(pid)
    # user_data
    w.write_bytes(None)
    return w.buffer()


def decode_assignment(data: Optional[bytes]) -> List[Dict[str, Any]]:
    """Decode a consumer group assignment."""
    if not data:
        return []
    r = BinaryReader(data)
    _version = r.read_int16()
    topic_count = r.read_array_length()
    result: List[Dict[str, Any]] = []
    for _ in range(topic_count):
        topic_name = r.read_string() or ""
        partition_count = r.read_array_length()
        for _ in range(partition_count):
            part_id = r.read_int32()
            result.append({"topic": topic_name, "partition": part_id})
    return result


# ============================================================================
# Metadata Serialization (for consumer group join)
# ============================================================================


def encode_subscription_metadata(topics: List[str], user_data: Optional[bytes] = None) -> bytes:
    """Encode consumer subscription metadata for JoinGroup protocol.

    Format: version(2) + topic_count(4) + [topic(16+str)] + user_data(32+bytes).
    """
    w = BinaryWriter()
    w.write_int16(1)  # version
    w.write_array(len(topics))
    for t in topics:
        w.write_string(t)
    w.write_bytes(user_data or b"")
    return w.buffer()
