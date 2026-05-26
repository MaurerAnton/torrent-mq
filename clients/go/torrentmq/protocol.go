// Package torrentmq provides a Go client SDK for torrent-mq, a Kafka-compatible
// distributed message queue. This file implements the low-level wire protocol:
// request/response framing, binary serialization primitives, CRC32C, API keys,
// error codes, and the v2 record batch format.
package torrentmq

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
)

// ---------------------------------------------------------------------------
// API Key Constants (Kafka-compatible)
// ---------------------------------------------------------------------------

// ApiKey identifies the RPC operation type in the Kafka wire protocol.
type ApiKey int16

const (
	ApiProduce              ApiKey = 0
	ApiFetch                ApiKey = 1
	ApiListOffsets          ApiKey = 2
	ApiMetadata             ApiKey = 3
	ApiOffsetCommit         ApiKey = 8
	ApiOffsetFetch          ApiKey = 9
	ApiFindCoordinator      ApiKey = 10
	ApiJoinGroup            ApiKey = 11
	ApiHeartbeat            ApiKey = 12
	ApiLeaveGroup           ApiKey = 13
	ApiSyncGroup            ApiKey = 14
	ApiDescribeGroups       ApiKey = 15
	ApiListGroups           ApiKey = 16
	ApiAPIVersions          ApiKey = 18
	ApiCreateTopics         ApiKey = 19
	ApiDeleteTopics         ApiKey = 20
	ApiDeleteRecords        ApiKey = 21
	ApiDescribeConfigs      ApiKey = 32
	ApiAlterConfigs         ApiKey = 33
	ApiCreatePartitions     ApiKey = 38
	ApiDeleteGroups         ApiKey = 43
	ApiDescribeCluster      ApiKey = 60
)

// String returns the human-readable name of the API key.
func (k ApiKey) String() string {
	switch k {
	case ApiProduce:
		return "Produce"
	case ApiFetch:
		return "Fetch"
	case ApiListOffsets:
		return "ListOffsets"
	case ApiMetadata:
		return "Metadata"
	case ApiOffsetCommit:
		return "OffsetCommit"
	case ApiOffsetFetch:
		return "OffsetFetch"
	case ApiFindCoordinator:
		return "FindCoordinator"
	case ApiJoinGroup:
		return "JoinGroup"
	case ApiHeartbeat:
		return "Heartbeat"
	case ApiLeaveGroup:
		return "LeaveGroup"
	case ApiSyncGroup:
		return "SyncGroup"
	case ApiDescribeGroups:
		return "DescribeGroups"
	case ApiListGroups:
		return "ListGroups"
	case ApiAPIVersions:
		return "ApiVersions"
	case ApiCreateTopics:
		return "CreateTopics"
	case ApiDeleteTopics:
		return "DeleteTopics"
	case ApiDeleteRecords:
		return "DeleteRecords"
	case ApiDescribeConfigs:
		return "DescribeConfigs"
	case ApiAlterConfigs:
		return "AlterConfigs"
	case ApiCreatePartitions:
		return "CreatePartitions"
	case ApiDeleteGroups:
		return "DeleteGroups"
	case ApiDescribeCluster:
		return "DescribeCluster"
	default:
		return fmt.Sprintf("Unknown(%d)", k)
	}
}

// ---------------------------------------------------------------------------
// Error Codes
// ---------------------------------------------------------------------------

// ErrorCode represents a Kafka/torrent-mq protocol error.
type ErrorCode int16

const (
	ErrNone                         ErrorCode = 0
	ErrUnknownServerError           ErrorCode = -1
	ErrOffsetOutOfRange             ErrorCode = 1
	ErrCorruptMessage               ErrorCode = 2
	ErrUnknownTopicOrPartition      ErrorCode = 3
	ErrInvalidFetchSize             ErrorCode = 4
	ErrLeaderNotAvailable           ErrorCode = 5
	ErrNotLeaderForPartition        ErrorCode = 6
	ErrRequestTimedOut              ErrorCode = 7
	ErrBrokerNotAvailable           ErrorCode = 8
	ErrReplicaNotAvailable          ErrorCode = 9
	ErrMessageTooLarge              ErrorCode = 10
	ErrStaleControllerEpoch         ErrorCode = 11
	ErrOffsetMetadataTooLarge       ErrorCode = 12
	ErrNetworkException             ErrorCode = 13
	ErrCoordinatorLoadInProgress    ErrorCode = 14
	ErrCoordinatorNotAvailable      ErrorCode = 15
	ErrNotCoordinator               ErrorCode = 16
	ErrInvalidTopicException        ErrorCode = 17
	ErrRecordListTooLarge           ErrorCode = 18
	ErrNotEnoughReplicas            ErrorCode = 19
	ErrNotEnoughReplicasAfterAppend ErrorCode = 20
	ErrInvalidRequiredAcks          ErrorCode = 21
	ErrIllegalGeneration            ErrorCode = 22
	ErrInconsistentGroupProtocol    ErrorCode = 23
	ErrInvalidGroupID               ErrorCode = 24
	ErrUnknownMemberID              ErrorCode = 25
	ErrInvalidSessionTimeout        ErrorCode = 26
	ErrRebalanceInProgress          ErrorCode = 27
	ErrInvalidCommitOffsetSize      ErrorCode = 28
	ErrTopicAuthorizationFailed     ErrorCode = 29
	ErrGroupAuthorizationFailed     ErrorCode = 30
	ErrClusterAuthorizationFailed   ErrorCode = 31
	ErrUnsupportedVersion           ErrorCode = 35
	ErrTopicAlreadyExists           ErrorCode = 36
	ErrInvalidPartitions            ErrorCode = 37
	ErrInvalidReplicationFactor     ErrorCode = 38
	ErrInvalidReplicaAssignment     ErrorCode = 39
	ErrInvalidConfig                ErrorCode = 40
	ErrNotController                ErrorCode = 41
	ErrInvalidRequest               ErrorCode = 42
	ErrPolicyViolation              ErrorCode = 44
	ErrNonEmptyGroup                ErrorCode = 68
	ErrGroupIDNotFound              ErrorCode = 69
	ErrTopicDeletionDisabled        ErrorCode = 73
	ErrUnsupportedCompressionType   ErrorCode = 76
	ErrMemberIDRequired             ErrorCode = 79
	ErrGroupMaxSizeReached          ErrorCode = 81
)

// Error returns the human-readable description for the error code.
func (e ErrorCode) Error() string {
	switch e {
	case ErrNone:
		return "no error"
	case ErrUnknownServerError:
		return "unknown server error"
	case ErrOffsetOutOfRange:
		return "offset out of range"
	case ErrCorruptMessage:
		return "corrupt message"
	case ErrUnknownTopicOrPartition:
		return "unknown topic or partition"
	case ErrLeaderNotAvailable:
		return "leader not available"
	case ErrNotLeaderForPartition:
		return "not leader for partition"
	case ErrRequestTimedOut:
		return "request timed out"
	case ErrBrokerNotAvailable:
		return "broker not available"
	case ErrNotCoordinator:
		return "not coordinator"
	case ErrInvalidTopicException:
		return "invalid topic"
	case ErrTopicAlreadyExists:
		return "topic already exists"
	case ErrNotController:
		return "not controller"
	case ErrRebalanceInProgress:
		return "rebalance in progress"
	case ErrUnknownMemberID:
		return "unknown member ID"
	case ErrIllegalGeneration:
		return "illegal generation"
	case ErrNonEmptyGroup:
		return "group not empty"
	case ErrGroupIDNotFound:
		return "group ID not found"
	case ErrTopicDeletionDisabled:
		return "topic deletion disabled"
	case ErrInvalidSessionTimeout:
		return "invalid session timeout"
	default:
		return fmt.Sprintf("error code %d", e)
	}
}

// ClientError wraps a protocol error code with contextual information.
type ClientError struct {
	Code    ErrorCode
	Message string
}

func (e *ClientError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s (code=%d): %s", e.Code.Error(), e.Code, e.Message)
	}
	return fmt.Sprintf("%s (code=%d)", e.Code.Error(), e.Code)
}

// ---------------------------------------------------------------------------
// CRC32C (Castagnoli polynomial 0x1EDC6F41)
// ---------------------------------------------------------------------------

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// crc32cChecksum computes the CRC32C checksum of data using the Castagnoli polynomial.
func crc32cChecksum(data []byte) uint32 {
	return crc32.Update(0, crc32cTable, data)
}

// ---------------------------------------------------------------------------
// Binary Writer
// ---------------------------------------------------------------------------

// binaryWriter is an internal helper for building wire-format messages.
// All integers are written in big-endian byte order.
type binaryWriter struct {
	buf []byte
	pos int
}

func newBinaryWriter(capacity int) *binaryWriter {
	return &binaryWriter{buf: make([]byte, capacity)}
}

func (w *binaryWriter) reset() {
	w.pos = 0
}

func (w *binaryWriter) bytes() []byte {
	return w.buf[:w.pos]
}

func (w *binaryWriter) ensure(n int) {
	if w.pos+n > len(w.buf) {
		newLen := (w.pos + n) * 2
		if newLen < 256 {
			newLen = 256
		}
		newBuf := make([]byte, newLen)
		copy(newBuf, w.buf[:w.pos])
		w.buf = newBuf
	}
}

func (w *binaryWriter) writeInt8(v int8) {
	w.ensure(1)
	w.buf[w.pos] = byte(v)
	w.pos++
}

func (w *binaryWriter) writeInt16(v int16) {
	w.ensure(2)
	binary.BigEndian.PutUint16(w.buf[w.pos:], uint16(v))
	w.pos += 2
}

func (w *binaryWriter) writeInt32(v int32) {
	w.ensure(4)
	binary.BigEndian.PutUint32(w.buf[w.pos:], uint32(v))
	w.pos += 4
}

func (w *binaryWriter) writeInt64(v int64) {
	w.ensure(8)
	binary.BigEndian.PutUint64(w.buf[w.pos:], uint64(v))
	w.pos += 8
}

// writeUnsignedVarint writes an unsigned LEB128-encoded integer.
func (w *binaryWriter) writeUnsignedVarint(v uint64) {
	w.ensure(10)
	for v > 0x7F {
		w.buf[w.pos] = byte(v&0x7F) | 0x80
		w.pos++
		v >>= 7
	}
	w.buf[w.pos] = byte(v & 0x7F)
	w.pos++
}

// writeVarint writes a signed zigzag+LEB128-encoded integer.
func (w *binaryWriter) writeVarint(v int64) {
	zigzag := uint64((v << 1) ^ (v >> 63))
	w.writeUnsignedVarint(zigzag)
}

// writeNullableString writes a string with a 2-byte length prefix, or -1 for nil.
func (w *binaryWriter) writeNullableString(s *string) {
	if s == nil {
		w.writeInt16(-1)
		return
	}
	data := []byte(*s)
	w.writeInt16(int16(len(data)))
	w.ensure(len(data))
	copy(w.buf[w.pos:], data)
	w.pos += len(data)
}

// writeString writes a non-nullable string with a 2-byte length prefix.
func (w *binaryWriter) writeString(s string) {
	data := []byte(s)
	w.writeInt16(int16(len(data)))
	w.ensure(len(data))
	copy(w.buf[w.pos:], data)
	w.pos += len(data)
}

// writeCompactString writes a compact nullable string (varint length, N+1 for non-null).
func (w *binaryWriter) writeCompactString(s *string) {
	if s == nil {
		w.writeUnsignedVarint(0)
		return
	}
	data := []byte(*s)
	w.writeUnsignedVarint(uint64(len(data) + 1))
	w.ensure(len(data))
	copy(w.buf[w.pos:], data)
	w.pos += len(data)
}

// writeNullableBytes writes bytes with a 4-byte length prefix, or -1 for nil.
func (w *binaryWriter) writeNullableBytes(b []byte) {
	if b == nil {
		w.writeInt32(-1)
		return
	}
	w.writeInt32(int32(len(b)))
	w.ensure(len(b))
	copy(w.buf[w.pos:], b)
	w.pos += len(b)
}

// writeCompactBytes writes compact nullable bytes (varint length).
func (w *binaryWriter) writeCompactBytes(b []byte) {
	if b == nil {
		w.writeUnsignedVarint(0)
		return
	}
	w.writeUnsignedVarint(uint64(len(b) + 1))
	w.ensure(len(b))
	copy(w.buf[w.pos:], b)
	w.pos += len(b)
}

// writeArrayLength writes a 4-byte array count (-1 for null array).
func (w *binaryWriter) writeArrayLength(n int) {
	w.writeInt32(int32(n))
}

// writeCompactArrayLength writes a varint array count (count+1).
func (w *binaryWriter) writeCompactArrayLength(n int) {
	w.writeUnsignedVarint(uint64(n + 1))
}

// writeBool writes a single byte boolean (1 = true, 0 = false).
func (w *binaryWriter) writeBool(v bool) {
	if v {
		w.writeInt8(1)
	} else {
		w.writeInt8(0)
	}
}

// writeTaggedFields writes an empty tagged fields section (a single zero varint).
func (w *binaryWriter) writeTaggedFields() {
	w.writeUnsignedVarint(0)
}

// writeRaw writes raw bytes directly.
func (w *binaryWriter) writeRaw(b []byte) {
	w.ensure(len(b))
	copy(w.buf[w.pos:], b)
	w.pos += len(b)
}

// ---------------------------------------------------------------------------
// Binary Reader
// ---------------------------------------------------------------------------

// binaryReader reads wire-format messages from a byte slice.
type binaryReader struct {
	data []byte
	pos  int
}

func newBinaryReader(data []byte) *binaryReader {
	return &binaryReader{data: data}
}

func (r *binaryReader) remaining() int {
	return len(r.data) - r.pos
}

func (r *binaryReader) check(n int) error {
	if r.pos+n > len(r.data) {
		return fmt.Errorf("buffer underflow: need %d bytes, %d remaining", n, len(r.data)-r.pos)
	}
	return nil
}

func (r *binaryReader) readInt8() (int8, error) {
	if err := r.check(1); err != nil {
		return 0, err
	}
	v := int8(r.data[r.pos])
	r.pos++
	return v, nil
}

func (r *binaryReader) readInt16() (int16, error) {
	if err := r.check(2); err != nil {
		return 0, err
	}
	v := int16(binary.BigEndian.Uint16(r.data[r.pos:]))
	r.pos += 2
	return v, nil
}

func (r *binaryReader) readInt32() (int32, error) {
	if err := r.check(4); err != nil {
		return 0, err
	}
	v := int32(binary.BigEndian.Uint32(r.data[r.pos:]))
	r.pos += 4
	return v, nil
}

func (r *binaryReader) readInt64() (int64, error) {
	if err := r.check(8); err != nil {
		return 0, err
	}
	v := int64(binary.BigEndian.Uint64(r.data[r.pos:]))
	r.pos += 8
	return v, nil
}

func (r *binaryReader) readUnsignedVarint() (uint64, error) {
	var value uint64
	var shift uint
	for {
		if err := r.check(1); err != nil {
			return 0, err
		}
		b := r.data[r.pos]
		r.pos++
		value |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return value, nil
}

func (r *binaryReader) readVarint() (int64, error) {
	zigzag, err := r.readUnsignedVarint()
	if err != nil {
		return 0, err
	}
	return int64(zigzag>>1) ^ -int64(zigzag&1), nil
}

func (r *binaryReader) readNullableString() (*string, error) {
	length, err := r.readInt16()
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	if err := r.check(int(length)); err != nil {
		return nil, err
	}
	s := string(r.data[r.pos : r.pos+int(length)])
	r.pos += int(length)
	return &s, nil
}

func (r *binaryReader) readString() (string, error) {
	length, err := r.readInt16()
	if err != nil {
		return "", err
	}
	if err := r.check(int(length)); err != nil {
		return "", err
	}
	s := string(r.data[r.pos : r.pos+int(length)])
	r.pos += int(length)
	return s, nil
}

func (r *binaryReader) readCompactString() (*string, error) {
	length, err := r.readUnsignedVarint()
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	l := int(length) - 1
	if err := r.check(l); err != nil {
		return nil, err
	}
	s := string(r.data[r.pos : r.pos+l])
	r.pos += l
	return &s, nil
}

func (r *binaryReader) readNullableBytes() ([]byte, error) {
	length, err := r.readInt32()
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	if err := r.check(int(length)); err != nil {
		return nil, err
	}
	b := make([]byte, length)
	copy(b, r.data[r.pos:r.pos+int(length)])
	r.pos += int(length)
	return b, nil
}

func (r *binaryReader) readCompactBytes() ([]byte, error) {
	length, err := r.readUnsignedVarint()
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	l := int(length) - 1
	if err := r.check(l); err != nil {
		return nil, err
	}
	b := make([]byte, l)
	copy(b, r.data[r.pos:r.pos+l])
	r.pos += l
	return b, nil
}

func (r *binaryReader) readArrayLength() (int, error) {
	length, err := r.readInt32()
	return int(length), err
}

func (r *binaryReader) readCompactArrayLength() (int, error) {
	length, err := r.readUnsignedVarint()
	return int(length) - 1, err
}

func (r *binaryReader) readBool() (bool, error) {
	v, err := r.readInt8()
	return v != 0, err
}

func (r *binaryReader) readTaggedFields() error {
	for {
		tag, err := r.readUnsignedVarint()
		if err != nil {
			return err
		}
		if tag == 0 {
			return nil
		}
		length, err := r.readUnsignedVarint()
		if err != nil {
			return err
		}
		r.pos += int(length)
	}
}

// ---------------------------------------------------------------------------
// Request / Response Framing
// ---------------------------------------------------------------------------

// RequestFraming contains the serialized bytes of a framed Kafka request.
type RequestFraming struct {
	CorrelationID int32
	Size          int32
	Raw           []byte
}

// ResponseFraming contains the parsed header of a Kafka response frame.
type ResponseFraming struct {
	CorrelationID int32
	Size          int32
	Body          []byte
}

// buildRequest frames a request for the wire.
//
// Layout:
//
//	[4: message_size] [2: api_key] [2: api_version] [4: correlation_id]
//	[2: client_id_len] [client_id_bytes] [body...]
//
// The message_size covers everything after the first 4 bytes.
func buildRequest(apiKey ApiKey, apiVersion int16, correlationID int32, clientID string, body []byte) []byte {
	clientBytes := []byte(clientID)
	headerSize := 2 + 2 + 4 + 2 + len(clientBytes)
	totalSize := headerSize + len(body)

	buf := make([]byte, 4+totalSize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalSize))
	binary.BigEndian.PutUint16(buf[4:6], uint16(apiKey))
	binary.BigEndian.PutUint16(buf[6:8], uint16(apiVersion))
	binary.BigEndian.PutUint32(buf[8:12], uint32(correlationID))
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(clientBytes)))
	copy(buf[14:14+len(clientBytes)], clientBytes)
	copy(buf[14+len(clientBytes):], body)
	return buf
}

// parseResponse parses a response frame from the wire.
//
// Layout:
//
//	[4: message_size] [4: correlation_id] [body...]
func parseResponse(data []byte) (*ResponseFraming, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("response too short: %d bytes", len(data))
	}
	size := int32(binary.BigEndian.Uint32(data[0:4]))
	correlationID := int32(binary.BigEndian.Uint32(data[4:8]))
	bodyStart := 8
	bodyEnd := bodyStart + int(size) - 4 // subtract correlation_id
	if bodyEnd > len(data) {
		bodyEnd = len(data)
	}
	body := data[bodyStart:bodyEnd]
	return &ResponseFraming{
		CorrelationID: correlationID,
		Size:          size,
		Body:          body,
	}, nil
}

// readFullResponse reads a complete response frame from a reader.
func readFullResponse(r io.Reader) (*ResponseFraming, error) {
	// Read 4-byte size prefix
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, sizeBuf); err != nil {
		return nil, fmt.Errorf("read response size: %w", err)
	}
	size := int32(binary.BigEndian.Uint32(sizeBuf))

	// Read the rest
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}
	return parseResponse(append(sizeBuf, data...))
}

// ---------------------------------------------------------------------------
// Record & RecordBatch (v2 format, Kafka 0.11+)
// ---------------------------------------------------------------------------

const recordBatchMagic = 2

// Record represents a single record within a v2 record batch.
type Record struct {
	OffsetDelta    int32
	TimestampDelta int64
	Key            []byte
	Value          []byte
	Headers        []RecordHeader
}

// RecordHeader is a key-value header attached to a record.
type RecordHeader struct {
	Key   string
	Value []byte
}

// RecordBatch is a v2 record batch (Kafka 0.11+ magic byte).
// Encapsulates a group of records sharing the same partition leader epoch,
// producer ID, and compression scheme.
type RecordBatch struct {
	BaseOffset           int64
	PartitionLeaderEpoch int32
	Magic                int8
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	Records              []Record
}

// Compression type constants encoded in bits 0-2 of Attributes.
const (
	CompressionNone   = 0
	CompressionGzip   = 1
	CompressionSnappy = 2
	CompressionLz4    = 3
	CompressionZstd   = 4
)

// CompressionType returns the compression type embedded in Attributes.
func (rb *RecordBatch) CompressionType() int {
	return int(rb.Attributes & 0x07)
}

// SetCompressionType sets the compression codec.
func (rb *RecordBatch) SetCompressionType(ct int) {
	rb.Attributes = (rb.Attributes & ^int16(0x07)) | int16(ct&0x07)
}

// encodeRecordBatch encodes a v2 record batch to wire-format bytes with CRC32C.
//
// Layout:
//
//	base_offset (8) + batch_length (4) + partition_leader_epoch (4) +
//	magic (1) + crc (4) + attributes (2) + last_offset_delta (4) +
//	first_timestamp (8) + max_timestamp (8) + producer_id (8) +
//	producer_epoch (2) + base_sequence (4) + record_count (4) +
//	[records...]
func encodeRecordBatch(batch *RecordBatch) ([]byte, error) {
	// Build records first
	var recordBufs [][]byte
	for _, rec := range batch.Records {
		w := newBinaryWriter(256)

		// Body length (varint) — we write a placeholder, then patch
		bodyStart := w.pos
		w.writeVarint(0) // placeholder
		w.writeVarint(0) // attributes
		w.writeVarint(rec.TimestampDelta)
		w.writeVarint(int64(rec.OffsetDelta))

		keyLen := len(rec.Key)
		w.writeVarint(int64(keyLen))
		if rec.Key != nil {
			w.writeVarint(int64(keyLen))
			w.writeRaw(rec.Key)
		} else {
			w.writeVarint(-1)
		}

		valLen := len(rec.Value)
		if rec.Value != nil {
			w.writeVarint(int64(valLen))
			w.writeRaw(rec.Value)
		} else {
			w.writeVarint(-1)
		}

		w.writeVarint(int64(len(rec.Headers)))
		for _, h := range rec.Headers {
			hk := h.Key
			w.writeCompactString(&hk)
			w.writeCompactBytes(h.Value)
		}

		// Patch body length
		bodyLen := int64(w.pos - bodyStart - 1) // minus the varint placeholder byte(s)
		// Rewrite body length at bodyStart
		oldPos := w.pos
		w.pos = bodyStart
		w.buf[bodyStart] = 0 // clear old varint
		// Simple: write the real varint
		w.pos = bodyStart
		w.writeVarint(bodyLen)
		w.pos = oldPos

		recordBufs = append(recordBufs, w.bytes())
	}

	// Fixed header
	w := newBinaryWriter(1024)
	w.writeInt64(batch.BaseOffset)
	w.writeInt32(0) // batch_length placeholder (position 8)
	w.writeInt32(batch.PartitionLeaderEpoch)
	w.writeInt8(batch.Magic)
	w.writeInt32(0) // crc placeholder (position 17)
	w.writeInt16(batch.Attributes)
	w.writeInt32(batch.LastOffsetDelta)
	w.writeInt64(batch.FirstTimestamp)
	w.writeInt64(batch.MaxTimestamp)
	w.writeInt64(batch.ProducerID)
	w.writeInt16(batch.ProducerEpoch)
	w.writeInt32(batch.BaseSequence)
	w.writeInt32(int32(len(batch.Records)))

	headerBytes := w.bytes()

	// Concatenate record buffers
	var recordsBody []byte
	for _, rb := range recordBufs {
		recordsBody = append(recordsBody, rb...)
	}

	// Compute CRC over [attributes .. end of records]
	crcBody := make([]byte, 2+4+8+8+8+2+4+4+len(recordsBody))
	pos := 0
	binary.BigEndian.PutUint16(crcBody[pos:], uint16(batch.Attributes))
	pos += 2
	binary.BigEndian.PutUint32(crcBody[pos:], uint32(batch.LastOffsetDelta))
	pos += 4
	binary.BigEndian.PutUint64(crcBody[pos:], uint64(batch.FirstTimestamp))
	pos += 8
	binary.BigEndian.PutUint64(crcBody[pos:], uint64(batch.MaxTimestamp))
	pos += 8
	binary.BigEndian.PutUint64(crcBody[pos:], uint64(batch.ProducerID))
	pos += 8
	binary.BigEndian.PutUint16(crcBody[pos:], uint16(batch.ProducerEpoch))
	pos += 2
	binary.BigEndian.PutUint32(crcBody[pos:], uint32(batch.BaseSequence))
	pos += 4
	binary.BigEndian.PutUint32(crcBody[pos:], uint32(len(batch.Records)))
	pos += 4
	copy(crcBody[pos:], recordsBody)

	crc := crc32cChecksum(crcBody)

	// Patch batch_length and CRC in header
	fullHeader := make([]byte, len(headerBytes))
	copy(fullHeader, headerBytes)
	// batch_length = everything after it (magic+crc+body minus base_offset+batch_length = everything from position 12)
	batchBodyLen := 1 + 4 + len(crcBody) // magic(1) + crc(4) + crc_body
	binary.BigEndian.PutUint32(fullHeader[8:12], uint32(batchBodyLen))
	binary.BigEndian.PutUint32(fullHeader[17:21], crc)

	result := make([]byte, 0, len(fullHeader)+len(recordsBody))
	result = append(result, fullHeader...)
	result = append(result, recordsBody...)

	return result, nil
}

// decodeRecordBatch decodes a v2 record batch from wire-format bytes.
// Returns the decoded RecordBatch and the number of bytes consumed.
func decodeRecordBatch(data []byte, offset int) (*RecordBatch, int, error) {
	if len(data) < 61 {
		return nil, 0, fmt.Errorf("record batch too short: %d bytes", len(data))
	}

	r := newBinaryReader(data)
	r.pos = offset
	start := offset

	baseOffset, err := r.readInt64()
	if err != nil {
		return nil, 0, err
	}
	batchLength, err := r.readInt32()
	if err != nil {
		return nil, 0, err
	}
	leaderEpoch, err := r.readInt32()
	if err != nil {
		return nil, 0, err
	}
	magic, err := r.readInt8()
	if err != nil {
		return nil, 0, err
	}
	if magic != recordBatchMagic {
		return nil, 0, fmt.Errorf("unsupported record batch magic: %d", magic)
	}
	crc, err := r.readInt32()
	if err != nil {
		return nil, 0, err
	}
	attributes, err := r.readInt16()
	if err != nil {
		return nil, 0, err
	}
	lastOffsetDelta, err := r.readInt32()
	if err != nil {
		return nil, 0, err
	}
	firstTimestamp, err := r.readInt64()
	if err != nil {
		return nil, 0, err
	}
	maxTimestamp, err := r.readInt64()
	if err != nil {
		return nil, 0, err
	}
	producerID, err := r.readInt64()
	if err != nil {
		return nil, 0, err
	}
	producerEpoch, err := r.readInt16()
	if err != nil {
		return nil, 0, err
	}
	baseSequence, err := r.readInt32()
	if err != nil {
		return nil, 0, err
	}
	recordCount, err := r.readInt32()
	if err != nil {
		return nil, 0, err
	}

	// Verify CRC
	crcStart := start + 21 // position of attributes
	crcEnd := start + 12 + int(batchLength)
	if crcEnd > len(data) {
		return nil, 0, fmt.Errorf("batch length exceeds data: %d > %d", crcEnd, len(data))
	}
	computedCRC := crc32cChecksum(data[crcStart:crcEnd])
	if computedCRC != uint32(crc) {
		return nil, 0, fmt.Errorf("CRC mismatch: computed %08x, expected %08x", computedCRC, uint32(crc))
	}

	// Decode records
	records := make([]Record, 0, recordCount)
	for i := int32(0); i < recordCount; i++ {
		bodyLen, err := r.readVarint()
		if err != nil {
			return nil, 0, err
		}
		recordStart := r.pos
		_, err = r.readVarint() // attributes (unused)
		if err != nil {
			return nil, 0, err
		}
		timestampDelta, err := r.readVarint()
		if err != nil {
			return nil, 0, err
		}
		offsetDelta, err := r.readVarint()
		if err != nil {
			return nil, 0, err
		}
		keyLen, err := r.readVarint()
		if err != nil {
			return nil, 0, err
		}
		var key []byte
		if keyLen > 0 {
			actualKeyLen, err := r.readVarint()
			if err != nil {
				return nil, 0, err
			}
			if actualKeyLen > 0 {
				if err := r.check(int(actualKeyLen)); err != nil {
					return nil, 0, err
				}
				key = make([]byte, actualKeyLen)
				copy(key, r.data[r.pos:r.pos+int(actualKeyLen)])
				r.pos += int(actualKeyLen)
			}
		}
		valLen, err := r.readVarint()
		if err != nil {
			return nil, 0, err
		}
		var value []byte
		if valLen > 0 {
			if err := r.check(int(valLen)); err != nil {
				return nil, 0, err
			}
			value = make([]byte, valLen)
			copy(value, r.data[r.pos:r.pos+int(valLen)])
			r.pos += int(valLen)
		}
		headerCount, err := r.readVarint()
		if err != nil {
			return nil, 0, err
		}
		headers := make([]RecordHeader, 0, headerCount)
		for j := int64(0); j < headerCount; j++ {
			hk, err := r.readCompactString()
			if err != nil {
				return nil, 0, err
			}
			hv, err := r.readCompactBytes()
			if err != nil {
				return nil, 0, err
			}
			hkStr := ""
			if hk != nil {
				hkStr = *hk
			}
			headers = append(headers, RecordHeader{Key: hkStr, Value: hv})
		}
		// Consume remaining bytes in record body if any
		r.pos = recordStart + int(bodyLen)

		records = append(records, Record{
			OffsetDelta:    int32(offsetDelta),
			TimestampDelta: timestampDelta,
			Key:            key,
			Value:          value,
			Headers:        headers,
		})
	}

	batch := &RecordBatch{
		BaseOffset:           baseOffset,
		PartitionLeaderEpoch: leaderEpoch,
		Magic:                magic,
		Attributes:           attributes,
		LastOffsetDelta:      lastOffsetDelta,
		FirstTimestamp:       firstTimestamp,
		MaxTimestamp:         maxTimestamp,
		ProducerID:           producerID,
		ProducerEpoch:        producerEpoch,
		BaseSequence:         baseSequence,
		Records:              records,
	}

	consumed := 12 + int(batchLength)
	return batch, consumed, nil
}

// ---------------------------------------------------------------------------
// Metadata Encoding / Decoding
// ---------------------------------------------------------------------------

// BrokerInfo holds metadata about a single broker.
type BrokerInfo struct {
	ID   int32
	Host string
	Port int32
}

// TopicMetadata holds metadata about a topic.
type TopicMetadata struct {
	Name       string
	ErrorCode  ErrorCode
	Partitions []PartitionInfo
	IsInternal bool
}

// PartitionInfo holds metadata about a single partition.
type PartitionInfo struct {
	ID        int32
	LeaderID  int32
	ErrorCode ErrorCode
	Replicas  []int32
	ISR       []int32
}

// ClusterMetadata holds the full cluster metadata.
type ClusterMetadata struct {
	Brokers      []BrokerInfo
	ControllerID int32
	ClusterID    string
	Topics       []TopicMetadata
}

// encodeMetadataRequest encodes a Metadata request (api_key=3, version 4).
// Pass nil for topics to request metadata for all topics.
func encodeMetadataRequest(topics []string) []byte {
	w := newBinaryWriter(256)
	if topics == nil {
		w.writeArrayLength(-1)
	} else {
		w.writeArrayLength(len(topics))
		for _, t := range topics {
			w.writeString(t)
		}
	}
	w.writeBool(false) // allow_auto_topic_creation
	return w.bytes()
}

// decodeMetadataResponse decodes a Metadata response (version 4+).
func decodeMetadataResponse(data []byte) (*ClusterMetadata, error) {
	r := newBinaryReader(data)
	_, err := r.readInt32() // throttle_time_ms
	if err != nil {
		return nil, err
	}

	brokerCount, err := r.readArrayLength()
	if err != nil {
		return nil, err
	}
	brokers := make([]BrokerInfo, 0, brokerCount)
	for i := 0; i < brokerCount; i++ {
		brokerID, err := r.readInt32()
		if err != nil {
			return nil, err
		}
		host, err := r.readNullableString()
		if err != nil {
			return nil, err
		}
		port, err := r.readInt32()
		if err != nil {
			return nil, err
		}
		_, err = r.readNullableString() // rack
		if err != nil {
			return nil, err
		}
		hostStr := ""
		if host != nil {
			hostStr = *host
		}
		brokers = append(brokers, BrokerInfo{
			ID:   brokerID,
			Host: hostStr,
			Port: port,
		})
	}

	clusterID, err := r.readNullableString()
	if err != nil {
		return nil, err
	}
	controllerID, err := r.readInt32()
	if err != nil {
		return nil, err
	}

	topicCount, err := r.readArrayLength()
	if err != nil {
		return nil, err
	}
	topics := make([]TopicMetadata, 0, topicCount)
	for i := 0; i < topicCount; i++ {
		errorCode, err := r.readInt16()
		if err != nil {
			return nil, err
		}
		name, err := r.readString()
		if err != nil {
			return nil, err
		}
		isInternal, err := r.readBool()
		if err != nil {
			return nil, err
		}
		partCount, err := r.readArrayLength()
		if err != nil {
			return nil, err
		}
		partitions := make([]PartitionInfo, 0, partCount)
		for j := 0; j < partCount; j++ {
			partError, err := r.readInt16()
			if err != nil {
				return nil, err
			}
			partID, err := r.readInt32()
			if err != nil {
				return nil, err
			}
			leaderID, err := r.readInt32()
			if err != nil {
				return nil, err
			}
			// replicas
			replicaCount, err := r.readArrayLength()
			if err != nil {
				return nil, err
			}
			replicas := make([]int32, replicaCount)
			for k := 0; k < replicaCount; k++ {
				replicas[k], err = r.readInt32()
				if err != nil {
					return nil, err
				}
			}
			// isr
			isrCount, err := r.readArrayLength()
			if err != nil {
				return nil, err
			}
			isr := make([]int32, isrCount)
			for k := 0; k < isrCount; k++ {
				isr[k], err = r.readInt32()
				if err != nil {
					return nil, err
				}
			}
			partitions = append(partitions, PartitionInfo{
				ID:        partID,
				LeaderID:  leaderID,
				ErrorCode: ErrorCode(partError),
				Replicas:  replicas,
				ISR:       isr,
			})
		}
		topics = append(topics, TopicMetadata{
			Name:       name,
			ErrorCode:  ErrorCode(errorCode),
			Partitions: partitions,
			IsInternal: isInternal,
		})
	}

	cid := ""
	if clusterID != nil {
		cid = *clusterID
	}
	return &ClusterMetadata{
		Brokers:      brokers,
		ControllerID: controllerID,
		ClusterID:    cid,
		Topics:       topics,
	}, nil
}

// ---------------------------------------------------------------------------
// Custom error for the stdlib errors.Is chain
// ---------------------------------------------------------------------------

// ErrTimeout is returned when an operation times out.
var ErrTimeout = errors.New("operation timed out")

// ErrClosed is returned when operating on a closed client.
var ErrClosed = errors.New("client is closed")

// ErrConnectionFailed is returned when connecting to a broker fails.
var ErrConnectionFailed = errors.New("connection failed")

// correlationIDGen is a global correlation ID generator.
type correlationIDGen struct {
	mu sync.Mutex
	id int32
}

func (g *correlationIDGen) next() int32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.id++
	return g.id
}
