package org.torrentmq.client.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Checksum;

/**
 * Wire-protocol primitives for the torrent-mq / Kafka-compatible binary protocol.
 *
 * <p>Implements request/response framing, big-endian integer serialization via
 * {@link ByteBuffer}, unsigned LEB128 varints, CRC32C checksum computation, the
 * v2 record batch format (Kafka 0.11+), and API-key / error-code mappings.</p>
 */
public final class Protocol {

    private Protocol() { /* utility class — prevent instantiation */ }

    // ---------------------------------------------------------------------------
    // Magic bytes
    // ---------------------------------------------------------------------------

    /** Record batch magic value for v2 format (Kafka 0.11+). */
    public static final byte RECORD_BATCH_MAGIC = 2;

    // ---------------------------------------------------------------------------
    // API Key Constants (Kafka-compatible)
    // ---------------------------------------------------------------------------

    /** Produce messages to a topic-partition. */
    public static final short API_PRODUCE              = 0;
    /** Fetch messages from a topic-partition. */
    public static final short API_FETCH                = 1;
    /** List partition offsets. */
    public static final short API_LIST_OFFSETS         = 2;
    /** Fetch cluster metadata (brokers, topics, partitions). */
    public static final short API_METADATA             = 3;
    /** Commit consumer offsets. */
    public static final short API_OFFSET_COMMIT        = 8;
    /** Fetch committed consumer offsets. */
    public static final short API_OFFSET_FETCH         = 9;
    /** Find the group coordinator broker. */
    public static final short API_FIND_COORDINATOR     = 10;
    /** Join a consumer group. */
    public static final short API_JOIN_GROUP           = 11;
    /** Send consumer group heartbeat. */
    public static final short API_HEARTBEAT            = 12;
    /** Leave a consumer group. */
    public static final short API_LEAVE_GROUP          = 13;
    /** Sync consumer group partition assignments. */
    public static final short API_SYNC_GROUP           = 14;
    /** Describe consumer groups. */
    public static final short API_DESCRIBE_GROUPS      = 15;
    /** List consumer groups. */
    public static final short API_LIST_GROUPS          = 16;
    /** Negotiate supported API versions. */
    public static final short API_API_VERSIONS         = 18;
    /** Create topics. */
    public static final short API_CREATE_TOPICS        = 19;
    /** Delete topics. */
    public static final short API_DELETE_TOPICS        = 20;
    /** Delete records up to an offset. */
    public static final short API_DELETE_RECORDS       = 21;
    /** Describe broker/topic configs. */
    public static final short API_DESCRIBE_CONFIGS     = 32;
    /** Alter broker/topic configs. */
    public static final short API_ALTER_CONFIGS        = 33;
    /** Create additional partitions for a topic. */
    public static final short API_CREATE_PARTITIONS    = 38;
    /** Delete consumer groups. */
    public static final short API_DELETE_GROUPS        = 43;
    /** Describe cluster metadata (beyond topic metadata). */
    public static final short API_DESCRIBE_CLUSTER     = 60;

    // ---------------------------------------------------------------------------
    // Error Codes
    // ---------------------------------------------------------------------------

    /** No error — success. */
    public static final short ERR_NONE                            = 0;
    /** Unexpected server-side error. */
    public static final short ERR_UNKNOWN_SERVER_ERROR            = -1;
    /** Requested offset is outside the log range. */
    public static final short ERR_OFFSET_OUT_OF_RANGE             = 1;
    /** Message CRC does not match. */
    public static final short ERR_CORRUPT_MESSAGE                 = 2;
    /** Topic or partition does not exist. */
    public static final short ERR_UNKNOWN_TOPIC_OR_PARTITION      = 3;
    /** Invalid fetch size requested. */
    public static final short ERR_INVALID_FETCH_SIZE              = 4;
    /** Leader for this partition not yet available. */
    public static final short ERR_LEADER_NOT_AVAILABLE            = 5;
    /** This broker is not the leader for the partition. */
    public static final short ERR_NOT_LEADER_FOR_PARTITION        = 6;
    /** Request timed out. */
    public static final short ERR_REQUEST_TIMED_OUT               = 7;
    /** Broker not available. */
    public static final short ERR_BROKER_NOT_AVAILABLE             = 8;
    /** Replica not available. */
    public static final short ERR_REPLICA_NOT_AVAILABLE            = 9;
    /** Message exceeds the maximum allowed size. */
    public static final short ERR_MESSAGE_TOO_LARGE                = 10;
    /** Stale controller epoch. */
    public static final short ERR_STALE_CONTROLLER_EPOCH           = 11;
    /** Offset metadata string too large. */
    public static final short ERR_OFFSET_METADATA_TOO_LARGE        = 12;
    /** Network-level exception. */
    public static final short ERR_NETWORK_EXCEPTION                = 13;
    /** Coordinator is still loading. */
    public static final short ERR_COORDINATOR_LOAD_IN_PROGRESS     = 14;
    /** Coordinator not available. */
    public static final short ERR_COORDINATOR_NOT_AVAILABLE        = 15;
    /** This broker is not the group coordinator. */
    public static final short ERR_NOT_COORDINATOR                  = 16;
    /** Invalid topic name/configuration. */
    public static final short ERR_INVALID_TOPIC_EXCEPTION          = 17;
    /** Record batch list too large. */
    public static final short ERR_RECORD_LIST_TOO_LARGE            = 18;
    /** Not enough in-sync replicas. */
    public static final short ERR_NOT_ENOUGH_REPLICAS              = 19;
    /** Not enough ISRs after append. */
    public static final short ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20;
    /** Invalid required acks value. */
    public static final short ERR_INVALID_REQUIRED_ACKS            = 21;
    /** Illegal consumer group generation. */
    public static final short ERR_ILLEGAL_GENERATION               = 22;
    /** Inconsistent group protocol among members. */
    public static final short ERR_INCONSISTENT_GROUP_PROTOCOL      = 23;
    /** Invalid consumer group ID. */
    public static final short ERR_INVALID_GROUP_ID                 = 24;
    /** Unknown group member ID. */
    public static final short ERR_UNKNOWN_MEMBER_ID                = 25;
    /** Invalid session timeout. */
    public static final short ERR_INVALID_SESSION_TIMEOUT          = 26;
    /** Group rebalance in progress. */
    public static final short ERR_REBALANCE_IN_PROGRESS            = 27;
    /** Invalid commit offset size. */
    public static final short ERR_INVALID_COMMIT_OFFSET_SIZE       = 28;
    /** Not authorized on topic. */
    public static final short ERR_TOPIC_AUTHORIZATION_FAILED       = 29;
    /** Not authorized on group. */
    public static final short ERR_GROUP_AUTHORIZATION_FAILED       = 30;
    /** Not authorized on cluster. */
    public static final short ERR_CLUSTER_AUTHORIZATION_FAILED     = 31;
    /** Unsupported API version. */
    public static final short ERR_UNSUPPORTED_VERSION              = 35;
    /** Topic already exists. */
    public static final short ERR_TOPIC_ALREADY_EXISTS             = 36;
    /** Invalid partition count. */
    public static final short ERR_INVALID_PARTITIONS               = 37;
    /** Invalid replication factor. */
    public static final short ERR_INVALID_REPLICATION_FACTOR       = 38;
    /** Invalid replica assignment. */
    public static final short ERR_INVALID_REPLICA_ASSIGNMENT       = 39;
    /** Invalid configuration. */
    public static final short ERR_INVALID_CONFIG                   = 40;
    /** This broker is not the controller. */
    public static final short ERR_NOT_CONTROLLER                   = 41;
    /** Invalid request. */
    public static final short ERR_INVALID_REQUEST                  = 42;
    /** Policy violation. */
    public static final short ERR_POLICY_VIOLATION                 = 44;
    /** Consumer group is not empty. */
    public static final short ERR_NON_EMPTY_GROUP                  = 68;
    /** Consumer group ID not found. */
    public static final short ERR_GROUP_ID_NOT_FOUND               = 69;
    /** Topic deletion is disabled. */
    public static final short ERR_TOPIC_DELETION_DISABLED          = 73;
    /** Unsupported compression codec. */
    public static final short ERR_UNSUPPORTED_COMPRESSION_TYPE     = 76;
    /** Member ID required. */
    public static final short ERR_MEMBER_ID_REQUIRED               = 79;
    /** Consumer group max size reached. */
    public static final short ERR_GROUP_MAX_SIZE_REACHED           = 81;

    /**
     * Returns a human-readable error description for the given error code.
     *
     * @param code the wire-protocol error code
     * @return a descriptive string
     */
    public static String errorCodeDescription(short code) {
        return switch (code) {
            case ERR_NONE                            -> "no error";
            case ERR_UNKNOWN_SERVER_ERROR            -> "unknown server error";
            case ERR_OFFSET_OUT_OF_RANGE             -> "offset out of range";
            case ERR_CORRUPT_MESSAGE                 -> "corrupt message";
            case ERR_UNKNOWN_TOPIC_OR_PARTITION      -> "unknown topic or partition";
            case ERR_INVALID_FETCH_SIZE              -> "invalid fetch size";
            case ERR_LEADER_NOT_AVAILABLE            -> "leader not available";
            case ERR_NOT_LEADER_FOR_PARTITION        -> "not leader for partition";
            case ERR_REQUEST_TIMED_OUT               -> "request timed out";
            case ERR_BROKER_NOT_AVAILABLE             -> "broker not available";
            case ERR_NOT_COORDINATOR                  -> "not coordinator";
            case ERR_INVALID_TOPIC_EXCEPTION          -> "invalid topic";
            case ERR_TOPIC_ALREADY_EXISTS             -> "topic already exists";
            case ERR_NOT_CONTROLLER                   -> "not controller";
            case ERR_REBALANCE_IN_PROGRESS            -> "rebalance in progress";
            case ERR_UNKNOWN_MEMBER_ID                -> "unknown member ID";
            case ERR_ILLEGAL_GENERATION               -> "illegal generation";
            case ERR_NON_EMPTY_GROUP                  -> "group not empty";
            case ERR_GROUP_ID_NOT_FOUND               -> "group ID not found";
            case ERR_TOPIC_DELETION_DISABLED          -> "topic deletion disabled";
            case ERR_INVALID_SESSION_TIMEOUT          -> "invalid session timeout";
            case ERR_INVALID_GROUP_ID                 -> "invalid group ID";
            default -> "error code " + code;
        };
    }

    // ---------------------------------------------------------------------------
    // Compression type constants (bits 0-2 of record batch Attributes)
    // ---------------------------------------------------------------------------

    public static final int COMPRESSION_NONE   = 0;
    public static final int COMPRESSION_GZIP   = 1;
    public static final int COMPRESSION_SNAPPY = 2;
    public static final int COMPRESSION_LZ4    = 3;
    public static final int COMPRESSION_ZSTD   = 4;

    /**
     * Extracts the compression type from record batch attributes.
     *
     * @param attributes the 16-bit attributes field
     * @return compression codec constant (0-4)
     */
    public static int compressionType(short attributes) {
        return attributes & 0x07;
    }

    /**
     * Sets the compression type bits in the given attributes value.
     *
     * @param attributes existing attributes
     * @param ct         compression codec (0-4)
     * @return updated attributes
     */
    public static short setCompressionType(short attributes, int ct) {
        return (short) ((attributes & ~0x07) | (ct & 0x07));
    }

    // ---------------------------------------------------------------------------
    // Correlation ID generator (thread-safe)
    // ---------------------------------------------------------------------------

    private static final AtomicInteger CORRELATION_SEQ = new AtomicInteger(1);

    /** Allocates a fresh correlation ID for request-response matching. */
    public static int nextCorrelationId() {
        return CORRELATION_SEQ.incrementAndGet();
    }

    // ---------------------------------------------------------------------------
    // CRC32C — Castagnoli polynomial 0x1EDC6F41
    // ---------------------------------------------------------------------------

    private static final java.util.zip.CRC32C CRC32C = new java.util.zip.CRC32C();

    /**
     * Computes the CRC32C checksum of the given data using the Castagnoli polynomial.
     *
     * @param data the input bytes
     * @return unsigned 32-bit CRC value (packed in an int)
     */
    public static int crc32cChecksum(byte[] data) {
        CRC32C.reset();
        CRC32C.update(data, 0, data.length);
        return (int) CRC32C.getValue();
    }

    /**
     * Computes the CRC32C checksum of a sub-range of data.
     *
     * @param data   the byte array
     * @param offset start offset (inclusive)
     * @param length number of bytes
     * @return unsigned 32-bit CRC value
     */
    public static int crc32cChecksum(byte[] data, int offset, int length) {
        CRC32C.reset();
        CRC32C.update(data, offset, length);
        return (int) CRC32C.getValue();
    }

    // ---------------------------------------------------------------------------
    // Binary write helpers (big-endian via ByteBuffer)
    // ---------------------------------------------------------------------------

    /** Writes a 16-bit big-endian integer to the buffer at its current position. */
    public static void writeInt16(ByteBuffer buf, short value) {
        buf.putShort(value);
    }

    /** Writes a 32-bit big-endian integer to the buffer at its current position. */
    public static void writeInt32(ByteBuffer buf, int value) {
        buf.putInt(value);
    }

    /** Writes a 64-bit big-endian integer to the buffer at its current position. */
    public static void writeInt64(ByteBuffer buf, long value) {
        buf.putLong(value);
    }

    // ---------------------------------------------------------------------------
    // Binary read helpers
    // ---------------------------------------------------------------------------

    /** Reads a 16-bit big-endian integer from the buffer at its current position. */
    public static short readInt16(ByteBuffer buf) {
        return buf.getShort();
    }

    /** Reads a 32-bit big-endian integer from the buffer at its current position. */
    public static int readInt32(ByteBuffer buf) {
        return buf.getInt();
    }

    /** Reads a 64-bit big-endian integer from the buffer at its current position. */
    public static long readInt64(ByteBuffer buf) {
        return buf.getLong();
    }

    // ---------------------------------------------------------------------------
    // Unsigned LEB128 varint encoding
    // ---------------------------------------------------------------------------

    /**
     * Writes an unsigned integer as a variable-length LEB128 encoding.
     * Each byte uses the top bit as a continuation flag.
     *
     * @param buf   the destination buffer
     * @param value the unsigned value to encode
     */
    public static void writeUnsignedVarint(ByteBuffer buf, long value) {
        while (value > 0x7F) {
            buf.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buf.put((byte) (value & 0x7F));
    }

    /**
     * Reads an unsigned LEB128-encoded integer.
     *
     * @param buf the source buffer
     * @return the decoded unsigned value
     * @throws java.nio.BufferUnderflowException if the buffer ends prematurely
     */
    public static long readUnsignedVarint(ByteBuffer buf) {
        long value = 0;
        int shift = 0;
        while (true) {
            byte b = buf.get();
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
        }
        return value;
    }

    /** Writes a signed integer using zigzag + LEB128 encoding. */
    public static void writeVarint(ByteBuffer buf, long value) {
        long zigzag = (value << 1) ^ (value >> 63);
        writeUnsignedVarint(buf, zigzag);
    }

    /** Reads a signed zigzag+LEB128-encoded integer. */
    public static long readVarint(ByteBuffer buf) {
        long zigzag = readUnsignedVarint(buf);
        return (zigzag >>> 1) ^ -(zigzag & 1);
    }

    // ---------------------------------------------------------------------------
    // Nullable / compact string and bytes helpers
    // ---------------------------------------------------------------------------

    /** Writes a nullable string: length as int16, -1 for null. */
    public static void writeNullableString(ByteBuffer buf, String s) {
        if (s == null) {
            buf.putShort((short) -1);
            return;
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) bytes.length);
        buf.put(bytes);
    }

    /** Reads a nullable string. Returns null if the length was -1. */
    public static String readNullableString(ByteBuffer buf) {
        short len = buf.getShort();
        if (len == -1) return null;
        byte[] bytes = new byte[len & 0xFFFF];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Writes a non-nullable string with int16 length prefix. */
    public static void writeString(ByteBuffer buf, String s) {
        writeNullableString(buf, s); // non-nullable but same encoding
    }

    /** Reads a non-nullable string. */
    public static String readString(ByteBuffer buf) {
        return readNullableString(buf);
    }

    /** Writes a compact nullable string (varint length, 0 = null, N+1 for non-null). */
    public static void writeCompactString(ByteBuffer buf, String s) {
        if (s == null) {
            writeUnsignedVarint(buf, 0);
            return;
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeUnsignedVarint(buf, bytes.length + 1);
        buf.put(bytes);
    }

    /** Reads a compact nullable string. Returns null if length varint is 0. */
    public static String readCompactString(ByteBuffer buf) {
        long len = readUnsignedVarint(buf);
        if (len == 0) return null;
        int l = (int) (len - 1);
        byte[] bytes = new byte[l];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Writes nullable bytes with int32 length prefix, -1 for null. */
    public static void writeNullableBytes(ByteBuffer buf, byte[] b) {
        if (b == null) {
            buf.putInt(-1);
            return;
        }
        buf.putInt(b.length);
        buf.put(b);
    }

    /** Reads nullable bytes. Returns null if length was -1. */
    public static byte[] readNullableBytes(ByteBuffer buf) {
        int len = buf.getInt();
        if (len == -1) return null;
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return bytes;
    }

    /** Writes compact nullable bytes (varint length, 0 = null). */
    public static void writeCompactBytes(ByteBuffer buf, byte[] b) {
        if (b == null) {
            writeUnsignedVarint(buf, 0);
            return;
        }
        writeUnsignedVarint(buf, b.length + 1);
        buf.put(b);
    }

    /** Reads compact nullable bytes. */
    public static byte[] readCompactBytes(ByteBuffer buf) {
        long len = readUnsignedVarint(buf);
        if (len == 0) return null;
        int l = (int) (len - 1);
        byte[] bytes = new byte[l];
        buf.get(bytes);
        return bytes;
    }

    /** Writes a classic array length: int32 (can be -1 for null array). */
    public static void writeArrayLength(ByteBuffer buf, int n) {
        buf.putInt(n);
    }

    /** Reads a classic array length. Returns -1 for null array. */
    public static int readArrayLength(ByteBuffer buf) {
        return buf.getInt();
    }

    /** Writes compact array length: varint of (count + 1). */
    public static void writeCompactArrayLength(ByteBuffer buf, int n) {
        writeUnsignedVarint(buf, n + 1);
    }

    /** Reads compact array length: varint - 1. */
    public static int readCompactArrayLength(ByteBuffer buf) {
        return (int) readUnsignedVarint(buf) - 1;
    }

    /** Writes a boolean as a single byte (1 = true, 0 = false). */
    public static void writeBool(ByteBuffer buf, boolean v) {
        buf.put(v ? (byte) 1 : (byte) 0);
    }

    /** Reads a single-byte boolean. */
    public static boolean readBool(ByteBuffer buf) {
        return buf.get() != 0;
    }

    /** Writes an empty tagged-fields section (single zero varint). */
    public static void writeTaggedFields(ByteBuffer buf) {
        writeUnsignedVarint(buf, 0);
    }

    /** Skips over all tagged fields until the terminating zero varint. */
    public static void readTaggedFields(ByteBuffer buf) {
        while (true) {
            long tag = readUnsignedVarint(buf);
            if (tag == 0) break;
            long length = readUnsignedVarint(buf);
            buf.position(buf.position() + (int) length);
        }
    }

    // ---------------------------------------------------------------------------
    // Request / Response Framing
    // ---------------------------------------------------------------------------

    /**
     * Builds a wire-format Kafka-compatible request frame.
     *
     * <pre>
     * Layout:
     *   [4: message_size] [2: api_key] [2: api_version] [4: correlation_id]
     *   [2: client_id_len] [client_id_bytes] [body...]
     * </pre>
     *
     * @param apiKey        the RPC operation key
     * @param apiVersion    the API version number
     * @param correlationId unique request identifier
     * @param clientId      client identifier string
     * @param body          pre-encoded request body
     * @return the fully framed byte array ready for socket write
     */
    public static byte[] buildRequest(short apiKey, short apiVersion, int correlationId,
                                       String clientId, byte[] body) {
        byte[] clientBytes = clientId.getBytes(StandardCharsets.UTF_8);
        int headerSize = 2 + 2 + 4 + 2 + clientBytes.length;
        int totalSize  = headerSize + (body != null ? body.length : 0);

        ByteBuffer buf = ByteBuffer.allocate(4 + totalSize);
        buf.putInt(totalSize);
        buf.putShort(apiKey);
        buf.putShort(apiVersion);
        buf.putInt(correlationId);
        buf.putShort((short) clientBytes.length);
        buf.put(clientBytes);
        if (body != null) {
            buf.put(body);
        }
        return buf.array();
    }

    /**
     * Parses a wire-format response frame from a byte array.
     *
     * <pre>
     * Layout:
     *   [4: message_size] [4: correlation_id] [body...]
     * </pre>
     *
     * @param data the complete raw response bytes
     * @return a parsed response frame with body slice
     */
    public static ResponseFraming parseResponse(byte[] data) {
        if (data.length < 8) {
            throw new IllegalArgumentException("response too short: " + data.length + " bytes");
        }
        ByteBuffer buf = ByteBuffer.wrap(data);
        int size = buf.getInt();
        int correlationId = buf.getInt();
        int bodyLen = size - 4;
        byte[] body = new byte[bodyLen];
        buf.get(body);
        return new ResponseFraming(correlationId, size, body);
    }

    // ---------------------------------------------------------------------------
    // Record & RecordBatch (v2 format)
    // ---------------------------------------------------------------------------

    /**
     * A single record within a v2 record batch.
     */
    public static final class Record {
        public int offsetDelta;
        public long timestampDelta;
        public byte[] key;
        public byte[] value;
        public RecordHeader[] headers;

        public Record() {}

        public Record(int offsetDelta, long timestampDelta, byte[] key, byte[] value, RecordHeader[] headers) {
            this.offsetDelta     = offsetDelta;
            this.timestampDelta  = timestampDelta;
            this.key             = key;
            this.value           = value;
            this.headers         = headers;
        }
    }

    /** A key-value header attached to a record. */
    public record RecordHeader(String key, byte[] value) {}

    /**
     * A v2 record batch encapsulating a group of records that share the same
     * partition leader epoch, producer ID, and compression scheme.
     */
    public static final class RecordBatch {
        public long    baseOffset           = 0;
        public int     partitionLeaderEpoch = -1;
        public byte    magic                = RECORD_BATCH_MAGIC;
        public short   attributes           = 0;
        public int     lastOffsetDelta      = 0;
        public long    firstTimestamp       = 0;
        public long    maxTimestamp         = 0;
        public long    producerID           = -1;
        public short   producerEpoch        = -1;
        public int     baseSequence         = -1;
        public Record[] records;

        public int compressionType() {
            return attributes & 0x07;
        }

        public void setCompressionType(int ct) {
            attributes = (short) ((attributes & ~0x07) | (ct & 0x07));
        }
    }

    // ---------------------------------------------------------------------------
    // Record batch encoding
    // ---------------------------------------------------------------------------

    /**
     * Encodes a v2 record batch to wire-format bytes with embedded CRC32C.
     *
     * <pre>
     * Layout:
     *   base_offset (8) + batch_length (4) + partition_leader_epoch (4) +
     *   magic (1) + crc (4) + attributes (2) + last_offset_delta (4) +
     *   first_timestamp (8) + max_timestamp (8) + producer_id (8) +
     *   producer_epoch (2) + base_sequence (4) + record_count (4) +
     *   [records...]
     * </pre>
     */
    public static byte[] encodeRecordBatch(RecordBatch batch) {
        // Encode individual records first
        byte[][] recordBufs = new byte[batch.records.length][];
        for (int i = 0; i < batch.records.length; i++) {
            recordBufs[i] = encodeSingleRecord(batch.records[i]);
        }

        // Build fixed header (with placeholder for batch_length and crc)
        ByteBuffer header = ByteBuffer.allocate(256);
        header.putLong(batch.baseOffset);
        header.putInt(0);                     // placeholder: batch_length (pos 8)
        header.putInt(batch.partitionLeaderEpoch);
        header.put(batch.magic);
        header.putInt(0);                     // placeholder: crc (pos 17)
        header.putShort(batch.attributes);
        header.putInt(batch.lastOffsetDelta);
        header.putLong(batch.firstTimestamp);
        header.putLong(batch.maxTimestamp);
        header.putLong(batch.producerID);
        header.putShort(batch.producerEpoch);
        header.putInt(batch.baseSequence);
        header.putInt(batch.records.length);

        byte[] headerBytes = new byte[header.position()];
        header.flip();
        header.get(headerBytes);

        int totalRecordsLen = 0;
        for (byte[] rb : recordBufs) totalRecordsLen += rb.length;

        // Compute CRC: from attributes through end of records body
        ByteBuffer crcBody = ByteBuffer.allocate(2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + totalRecordsLen);
        crcBody.putShort(batch.attributes);
        crcBody.putInt(batch.lastOffsetDelta);
        crcBody.putLong(batch.firstTimestamp);
        crcBody.putLong(batch.maxTimestamp);
        crcBody.putLong(batch.producerID);
        crcBody.putShort(batch.producerEpoch);
        crcBody.putInt(batch.baseSequence);
        crcBody.putInt(batch.records.length);
        for (byte[] rb : recordBufs) crcBody.put(rb);
        int crc = crc32cChecksum(crcBody.array());

        // Patch batch_length and CRC into the header
        int batchBodyLen = 1 + 4 + crcBody.array().length; // magic(1) + crc(4) + crc_body
        ByteBuffer.wrap(headerBytes).putInt(8, batchBodyLen);
        ByteBuffer.wrap(headerBytes).putInt(17, crc);

        // Concatenate
        byte[] result = new byte[headerBytes.length + totalRecordsLen];
        System.arraycopy(headerBytes, 0, result, 0, headerBytes.length);
        int dst = headerBytes.length;
        for (byte[] rb : recordBufs) {
            System.arraycopy(rb, 0, result, dst, rb.length);
            dst += rb.length;
        }
        return result;
    }

    private static byte[] encodeSingleRecord(Record rec) {
        // Estimate size
        ByteBuffer buf = ByteBuffer.allocate(512);
        int bodyStart = buf.position();
        writeVarint(buf, 0);                    // placeholder body length
        writeVarint(buf, 0);                    // attributes
        writeVarint(buf, rec.timestampDelta);
        writeVarint(buf, rec.offsetDelta);

        // Key
        int keyLen = rec.key != null ? rec.key.length : 0;
        writeVarint(buf, keyLen);
        if (rec.key != null) {
            writeVarint(buf, keyLen);
            buf.put(rec.key);
        } else {
            writeVarint(buf, -1);
        }

        // Value
        int valLen = rec.value != null ? rec.value.length : 0;
        writeVarint(buf, valLen);
        if (rec.value != null) {
            buf.put(rec.value);             // value already length-prefixed by varint above
        } else {
            writeVarint(buf, -1);
        }

        // Headers
        int hdrCount = rec.headers != null ? rec.headers.length : 0;
        writeVarint(buf, hdrCount);
        if (rec.headers != null) {
            for (RecordHeader h : rec.headers) {
                writeCompactString(buf, h.key());
                writeCompactBytes(buf, h.value());
            }
        }

        // Patch body length — go back and write the actual length
        int bodyEnd = buf.position();
        int bodyLen = bodyEnd - bodyStart - 1; // minus placeholder byte (approximate)
        // Rewrite: position back, clear, rewrite
        buf.position(bodyStart);
        writeVarint(buf, bodyLen);
        buf.position(bodyEnd);

        byte[] result = new byte[buf.position()];
        buf.flip();
        buf.get(result);
        return result;
    }

    // ---------------------------------------------------------------------------
    // Record batch decoding
    // ---------------------------------------------------------------------------

    /**
     * Decodes a v2 record batch from wire-format bytes starting at the given offset.
     *
     * @param data   the raw byte array
     * @param offset the start offset within data
     * @return a populated RecordBatch (records field is always non-null)
     * @throws ProtocolException on format/CRC errors
     */
    public static RecordBatch decodeRecordBatch(byte[] data, int offset) throws ProtocolException {
        if (data.length - offset < 61) {
            throw new ProtocolException("record batch too short: " + (data.length - offset) + " bytes");
        }
        ByteBuffer buf = ByteBuffer.wrap(data);
        buf.position(offset);
        int start = offset;

        long   baseOffset     = buf.getLong();
        int    batchLength    = buf.getInt();
        int    leaderEpoch    = buf.getInt();
        byte   magic          = buf.get();
        if (magic != RECORD_BATCH_MAGIC) {
            throw new ProtocolException("unsupported record batch magic: " + magic);
        }
        int    crc            = buf.getInt();
        short  attributes     = buf.getShort();
        int    lastOffsetDelta = buf.getInt();
        long   firstTimestamp = buf.getLong();
        long   maxTimestamp   = buf.getLong();
        long   producerID     = buf.getLong();
        short  producerEpoch  = buf.getShort();
        int    baseSequence   = buf.getInt();
        int    recordCount    = buf.getInt();

        // Verify CRC
        int crcStart = start + 21;
        int crcEnd   = start + 12 + batchLength;
        if (crcEnd > data.length) {
            throw new ProtocolException("batch length exceeds data: " + crcEnd + " > " + data.length);
        }
        int computedCRC = crc32cChecksum(data, crcStart, crcEnd - crcStart);
        if (computedCRC != crc) {
            throw new ProtocolException(String.format(
                "CRC mismatch: computed %08x, expected %08x", computedCRC, crc));
        }

        // Decode records
        Record[] records = new Record[recordCount];
        for (int i = 0; i < recordCount; i++) {
            long bodyLen = readVarint(buf);
            int recordStart = buf.position();
            readVarint(buf); // attributes (skip)
            long timestampDelta = readVarint(buf);
            long offsetDelta    = readVarint(buf);

            long keyLen = readVarint(buf);
            byte[] key = null;
            if (keyLen > 0) {
                long actualKeyLen = readVarint(buf);
                if (actualKeyLen > 0) {
                    key = new byte[(int) actualKeyLen];
                    buf.get(key);
                }
            }

            long valLen = readVarint(buf);
            byte[] value = null;
            if (valLen > 0) {
                value = new byte[(int) valLen];
                buf.get(value);
            }

            long headerCount = readVarint(buf);
            RecordHeader[] headers = new RecordHeader[(int) headerCount];
            for (int j = 0; j < headerCount; j++) {
                String hk = readCompactString(buf);
                byte[] hv = readCompactBytes(buf);
                headers[j] = new RecordHeader(hk, hv);
            }

            records[i] = new Record((int) offsetDelta, timestampDelta, key, value, headers);
        }

        RecordBatch batch = new RecordBatch();
        batch.baseOffset          = baseOffset;
        batch.partitionLeaderEpoch = leaderEpoch;
        batch.magic               = magic;
        batch.attributes          = attributes;
        batch.lastOffsetDelta     = lastOffsetDelta;
        batch.firstTimestamp      = firstTimestamp;
        batch.maxTimestamp        = maxTimestamp;
        batch.producerID          = producerID;
        batch.producerEpoch       = producerEpoch;
        batch.baseSequence        = baseSequence;
        batch.records             = records;
        return batch;
    }

    /**
     * A parsed response frame containing metadata and the response body.
     */
    public static final class ResponseFraming {
        public final int correlationId;
        public final int size;
        public final byte[] body;

        public ResponseFraming(int correlationId, int size, byte[] body) {
            this.correlationId = correlationId;
            this.size          = size;
            this.body          = body;
        }
    }

    /**
     * Checked exception wrapping protocol-level errors (parse failures, CRC mismatches, etc.).
     */
    public static final class ProtocolException extends Exception {
        public ProtocolException(String message) {
            super(message);
        }

        public ProtocolException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
