package torrentmq

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// RecordMetadata
// ---------------------------------------------------------------------------

// RecordMetadata contains metadata about a successfully produced record.
type RecordMetadata struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp int64
}

// ---------------------------------------------------------------------------
// ProducerMessage
// ---------------------------------------------------------------------------

// ProducerMessage represents a message to be sent to a topic.
type ProducerMessage struct {
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// ---------------------------------------------------------------------------
// Producer Configuration
// ---------------------------------------------------------------------------

// ProducerConfig configures the producer's behavior.
type ProducerConfig struct {
	// ClientID is the identifier sent to the broker for logging and metrics.
	ClientID string

	// Acks specifies the required acknowledgments: 0, 1, or -1 (all ISR).
	Acks int16

	// CompressionType selects the compression codec: "none", "gzip", "snappy", "lz4", "zstd".
	CompressionType string

	// BatchSize is the maximum batch size in bytes before sending.
	BatchSize int

	// LingerMs is the maximum time in milliseconds to wait before sending a partial batch.
	LingerMs int

	// MaxRetries is the maximum number of retry attempts for send failures.
	MaxRetries int

	// RetryBackoffMs is the base backoff in milliseconds between retries.
	RetryBackoffMs int

	// RequestTimeoutMs is the maximum time to wait for a broker response.
	RequestTimeoutMs int

	// MaxInFlight is the maximum number of in-flight requests per broker.
	MaxInFlight int

	// BufferMemory is the total memory in bytes for buffering unsent records.
	BufferMemory int

	// SocketTimeoutMs is the socket read/write timeout.
	SocketTimeoutMs int

	// ConnectionTimeoutMs is the TCP connection timeout.
	ConnectionTimeoutMs int
}

// DefaultProducerConfig returns a ProducerConfig with sensible defaults.
func DefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		ClientID:           "torrent-producer",
		Acks:               1,
		CompressionType:    "none",
		BatchSize:          16384,
		LingerMs:           5,
		MaxRetries:         3,
		RetryBackoffMs:     100,
		RequestTimeoutMs:   30000,
		MaxInFlight:        5,
		BufferMemory:       33554432,
		SocketTimeoutMs:    60000,
		ConnectionTimeoutMs: 10000,
	}
}

// compressionTypeToInt maps string names to compression attribute constants.
func compressionTypeToInt(ct string) int {
	switch ct {
	case "gzip":
		return CompressionGzip
	case "snappy":
		return CompressionSnappy
	case "lz4":
		return CompressionLz4
	case "zstd":
		return CompressionZstd
	default:
		return CompressionNone
	}
}

// ---------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------

// Producer is a high-throughput, asynchronous message producer.
// It accumulates records into batches per topic-partition and sends them
// to the broker via a background sender goroutine.
type Producer struct {
	config   *ProducerConfig
	brokers  []string
	clientID string

	// Correlation ID generator
	corrGen correlationIDGen

	// Connection to a broker (simple round-robin for now)
	conn   net.Conn
	connMu sync.Mutex

	// Metadata cache
	metadata   *ClusterMetadata
	metadataMu sync.RWMutex

	// Accumulator: records batched per topic-partition
	accumulator map[string]*topicAccumulator // keyed by topic
	accMu       sync.Mutex

	// Sender control
	senderCtx    context.Context
	senderCancel context.CancelFunc
	senderWg     sync.WaitGroup

	// Pending requests
	pending     map[int32]chan *ResponseFraming
	pendingMu   sync.Mutex
	pendingWg   sync.WaitGroup
	recvRunning atomic.Bool

	// Closed flag
	closed atomic.Bool
}

// NewProducer creates a new Producer connected to the given brokers.
// It auto-discovers cluster metadata and starts a background sender goroutine.
func NewProducer(brokers []string, config *ProducerConfig) (*Producer, error) {
	if config == nil {
		config = DefaultProducerConfig()
	}
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Producer{
		config:      config,
		brokers:     brokers,
		clientID:    config.ClientID,
		accumulator: make(map[string]*topicAccumulator),
		pending:     make(map[int32]chan *ResponseFraming),
		senderCtx:   ctx,
		senderCancel: cancel,
	}

	// Connect to first available broker and discover metadata
	if err := p.connect(); err != nil {
		cancel()
		return nil, fmt.Errorf("producer: connect: %w", err)
	}

	// Start background receiver
	p.recvRunning.Store(true)
	p.pendingWg.Add(1)
	go p.recvLoop()

	// Start sender goroutine
	p.senderWg.Add(1)
	go p.senderLoop()

	return p, nil
}

// connect establishes a TCP connection to a broker and refreshes metadata.
func (p *Producer) connect() error {
	p.connMu.Lock()
	defer p.connMu.Unlock()

	if p.conn != nil {
		p.conn.Close()
	}

	var lastErr error
	for _, addr := range p.brokers {
		d := net.Dialer{Timeout: time.Duration(p.config.ConnectionTimeoutMs) * time.Millisecond}
		conn, err := d.DialContext(p.senderCtx, "tcp", addr)
		if err != nil {
			lastErr = err
			continue
		}
		p.conn = conn
		lastErr = nil
		break
	}
	if lastErr != nil {
		return fmt.Errorf("%w: %v", ErrConnectionFailed, lastErr)
	}

	// Fetch metadata
	return p.refreshMetadata()
}

// refreshMetadata sends a Metadata request and updates the cache.
func (p *Producer) refreshMetadata() error {
	body := encodeMetadataRequest(nil)
	resp, err := p.sendRequest(ApiMetadata, 4, body)
	if err != nil {
		return err
	}
	meta, err := decodeMetadataResponse(resp.Body)
	if err != nil {
		return fmt.Errorf("decode metadata response: %w", err)
	}
	p.metadataMu.Lock()
	p.metadata = meta
	p.metadataMu.Unlock()
	return nil
}

// ensureConnection checks the connection and reconnects if needed.
func (p *Producer) ensureConnection() error {
	p.connMu.Lock()
	if p.conn != nil {
		// Quick health check
		p.connMu.Unlock()
		return nil
	}
	p.connMu.Unlock()
	return p.connect()
}

// sendRequest sends a framed request and waits for the response.
func (p *Producer) sendRequest(apiKey ApiKey, apiVersion int16, body []byte) (*ResponseFraming, error) {
	correlationID := p.corrGen.next()
	requestBytes := buildRequest(apiKey, apiVersion, correlationID, p.clientID, body)

	respCh := make(chan *ResponseFraming, 1)
	p.pendingMu.Lock()
	p.pending[correlationID] = respCh
	p.pendingMu.Unlock()

	if err := p.ensureConnection(); err != nil {
		p.pendingMu.Lock()
		delete(p.pending, correlationID)
		p.pendingMu.Unlock()
		return nil, err
	}

	p.connMu.Lock()
	conn := p.conn
	p.connMu.Unlock()

	// Set write deadline
	_ = conn.SetWriteDeadline(time.Now().Add(time.Duration(p.config.RequestTimeoutMs) * time.Millisecond))
	_, err := conn.Write(requestBytes)
	if err != nil {
		p.connMu.Lock()
		p.conn.Close()
		p.conn = nil
		p.connMu.Unlock()
		p.pendingMu.Lock()
		delete(p.pending, correlationID)
		p.pendingMu.Unlock()
		return nil, fmt.Errorf("write request: %w", err)
	}

	// Wait for response
	select {
	case resp := <-respCh:
		if resp == nil {
			return nil, ErrConnectionFailed
		}
		return resp, nil
	case <-time.After(time.Duration(p.config.RequestTimeoutMs) * time.Millisecond):
		p.pendingMu.Lock()
		delete(p.pending, correlationID)
		p.pendingMu.Unlock()
		return nil, ErrTimeout
	case <-p.senderCtx.Done():
		return nil, ErrClosed
	}
}

// recvLoop reads responses from the broker connection and dispatches them.
func (p *Producer) recvLoop() {
	defer p.pendingWg.Done()

	for p.recvRunning.Load() {
		p.connMu.Lock()
		conn := p.conn
		p.connMu.Unlock()

		if conn == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		_ = conn.SetReadDeadline(time.Now().Add(time.Duration(p.config.SocketTimeoutMs) * time.Millisecond))
		resp, err := readFullResponse(conn)
		if err != nil {
			if errors.Is(err, io.EOF) || isNetTimeout(err) {
				// Connection closed or timeout — reconnect
				p.connMu.Lock()
				if p.conn == conn {
					p.conn.Close()
					p.conn = nil
				}
				p.connMu.Unlock()
				time.Sleep(100 * time.Millisecond)
				continue
			}
			// Other errors — keep trying
			time.Sleep(50 * time.Millisecond)
			continue
		}

		p.pendingMu.Lock()
		ch, ok := p.pending[resp.CorrelationID]
		if ok {
			delete(p.pending, resp.CorrelationID)
		}
		p.pendingMu.Unlock()

		if ok && ch != nil {
			select {
			case ch <- resp:
			default:
			}
		}
	}
}

// senderLoop drains the accumulator and sends batches to the broker.
func (p *Producer) senderLoop() {
	defer p.senderWg.Done()

	ticker := time.NewTicker(time.Duration(p.config.LingerMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-p.senderCtx.Done():
			return
		case <-ticker.C:
			p.flushBatches()
		}
	}
}

// flushBatches drains all ready batches from the accumulator and sends them.
func (p *Producer) flushBatches() {
	p.accMu.Lock()
	// Collect all batches
	type batchEntry struct {
		topic     string
		partition int32
		records   []accumulatorRecord
	}
	var batches []batchEntry
	for topic, acc := range p.accumulator {
		for part, records := range acc.drainReady(p.config.BatchSize) {
			if len(records) > 0 {
				batches = append(batches, batchEntry{
					topic:     topic,
					partition: int32(part),
					records:   records,
				})
			}
		}
	}
	p.accMu.Unlock()

	for _, batch := range batches {
		if len(batch.records) == 0 {
			continue
		}
		p.sendBatch(batch.topic, batch.partition, batch.records)
	}
}

// sendBatch sends a single batch of records to the broker.
func (p *Producer) sendBatch(topic string, partition int32, records []accumulatorRecord) {
	// Build record batch
	nowMs := time.Now().UnixMilli()
	batch := &RecordBatch{
		PartitionLeaderEpoch: -1,
		Magic:                recordBatchMagic,
		Attributes:           0,
		LastOffsetDelta:      int32(len(records) - 1),
		FirstTimestamp:       nowMs,
		MaxTimestamp:         nowMs,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		BaseSequence:         -1,
	}

	ct := compressionTypeToInt(p.config.CompressionType)
	batch.SetCompressionType(ct)

	for i, rec := range records {
		batch.Records = append(batch.Records, Record{
			OffsetDelta:    int32(i),
			TimestampDelta: 0,
			Key:            rec.key,
			Value:          rec.value,
			Headers:        convertHeaders(rec.headers),
		})
	}

	recordData, err := encodeRecordBatch(batch)
	if err != nil {
		// Fail all futures in this batch
		for _, rec := range records {
			if rec.future != nil {
				rec.future <- nil
				close(rec.future)
			}
		}
		return
	}

	// Encode produce request
	body := encodeProduceRequest(topic, partition, recordData, p.config.Acks, p.config.RequestTimeoutMs)
	resp, err := p.sendRequestWithRetry(ApiProduce, 4, body, p.config.MaxRetries)
	if err != nil {
		for _, rec := range records {
			if rec.future != nil {
				rec.future <- nil
				close(rec.future)
			}
		}
		return
	}

	// Parse response
	baseOffset, errCode, err := decodeProduceResponse(resp.Body, topic, partition)
	if err != nil || errCode != 0 {
		for _, rec := range records {
			if rec.future != nil {
				rec.future <- nil
				close(rec.future)
			}
		}
		return
	}

	// Notify futures
	for i, rec := range records {
		if rec.future != nil {
			meta := &RecordMetadata{
				Topic:     topic,
				Partition: partition,
				Offset:    baseOffset + int64(i),
				Timestamp: nowMs,
			}
			rec.future <- meta
			close(rec.future)
		}
	}
}

// sendRequestWithRetry sends a request and retries on transient failures.
func (p *Producer) sendRequestWithRetry(apiKey ApiKey, version int16, body []byte, maxRetries int) (*ResponseFraming, error) {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(p.config.RetryBackoffMs*(1<<(attempt-1))) * time.Millisecond
			time.Sleep(backoff)
			// Refresh metadata on retry
			_ = p.refreshMetadata()
		}
		resp, err := p.sendRequest(apiKey, version, body)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if errors.Is(err, ErrClosed) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("request failed after %d retries: %w", maxRetries+1, lastErr)
}

// convertHeaders converts a map[string]string to []RecordHeader.
func convertHeaders(h map[string]string) []RecordHeader {
	if len(h) == 0 {
		return nil
	}
	out := make([]RecordHeader, 0, len(h))
	for k, v := range h {
		out = append(out, RecordHeader{Key: k, Value: []byte(v)})
	}
	return out
}

// ---------------------------------------------------------------------------
// Accumulator
// ---------------------------------------------------------------------------

type accumulatorRecord struct {
	key     []byte
	value   []byte
	headers map[string]string
	size    int
	addedAt time.Time
	future  chan *RecordMetadata
}

type topicAccumulator struct {
	mu        sync.Mutex
	batches   map[int][]accumulatorRecord // partition -> records
	sizeBytes int
	lingerCh  map[int]time.Time
}

func (ta *topicAccumulator) append(partition int, rec accumulatorRecord) {
	ta.mu.Lock()
	defer ta.mu.Unlock()
	if ta.batches == nil {
		ta.batches = make(map[int][]accumulatorRecord)
		ta.lingerCh = make(map[int]time.Time)
	}
	ta.batches[partition] = append(ta.batches[partition], rec)
	ta.sizeBytes += rec.size
	if _, ok := ta.lingerCh[partition]; !ok {
		ta.lingerCh[partition] = time.Now()
	}
}

// drainReady returns batches that are ready (full or lingered long enough).
func (ta *topicAccumulator) drainReady(batchSize int) map[int][]accumulatorRecord {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	result := make(map[int][]accumulatorRecord)
	for part, records := range ta.batches {
		if len(records) == 0 {
			continue
		}
		partSize := 0
		for _, r := range records {
			partSize += r.size
		}
		// Drain if batch is full or linger has elapsed
		lingerStart, ok := ta.lingerCh[part]
		shouldDrain := partSize >= batchSize || (ok && time.Since(lingerStart) >= 5*time.Millisecond)
		if shouldDrain {
			result[part] = records
			delete(ta.batches, part)
			delete(ta.lingerCh, part)
			ta.sizeBytes -= partSize
		}
	}
	return result
}

// drainAll returns all accumulated records, used during flush.
func (ta *topicAccumulator) drainAll() map[int][]accumulatorRecord {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	result := ta.batches
	ta.batches = make(map[int][]accumulatorRecord)
	ta.lingerCh = make(map[int]time.Time)
	ta.sizeBytes = 0
	return result
}

// getOrCreateAccumulator returns the topic-specific accumulator, creating it if needed.
func (p *Producer) getOrCreateAccumulator(topic string) *topicAccumulator {
	p.accMu.Lock()
	defer p.accMu.Unlock()
	if acc, ok := p.accumulator[topic]; ok {
		return acc
	}
	acc := &topicAccumulator{
		batches:  make(map[int][]accumulatorRecord),
		lingerCh: make(map[int]time.Time),
	}
	p.accumulator[topic] = acc
	return acc
}

// ---------------------------------------------------------------------------
// Partition Routing
// ---------------------------------------------------------------------------

// partitionFor returns the target partition for a message.
// If a key is provided, uses murmur2 hash; otherwise round-robin.
func (p *Producer) partitionFor(topic string, key []byte) int32 {
	if key == nil || len(key) == 0 {
		// Round-robin via atomic counter
		return int32(time.Now().UnixNano() & 0x7FFFFFFF)
	}
	hash := murmur2(key)

	p.metadataMu.RLock()
	defer p.metadataMu.RUnlock()

	var numPartitions int32 = 1
	if p.metadata != nil {
		for _, t := range p.metadata.Topics {
			if t.Name == topic {
				numPartitions = int32(len(t.Partitions))
				break
			}
		}
	}
	if numPartitions <= 0 {
		numPartitions = 1
	}
	return int32(hash % uint32(numPartitions))
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// Send sends a single message asynchronously.
// Returns a channel that will receive the RecordMetadata or nil on failure.
func (p *Producer) Send(topic string, key, value []byte, headers map[string]string) (<-chan *RecordMetadata, error) {
	return p.sendInternal(topic, key, value, headers)
}

// SendBatch sends multiple messages asynchronously.
// Returns a slice of channels, one per message.
func (p *Producer) SendBatch(topic string, messages []ProducerMessage) ([]<-chan *RecordMetadata, error) {
	channels := make([]<-chan *RecordMetadata, len(messages))
	for i, msg := range messages {
		ch, err := p.sendInternal(topic, msg.Key, msg.Value, msg.Headers)
		if err != nil {
			return nil, err
		}
		channels[i] = ch
	}
	return channels, nil
}

// sendInternal is the common send path.
func (p *Producer) sendInternal(topic string, key, value []byte, headers map[string]string) (<-chan *RecordMetadata, error) {
	if p.closed.Load() {
		return nil, ErrClosed
	}

	partition := p.partitionFor(topic, key)
	recSize := len(key) + len(value)
	for _, v := range headers {
		recSize += len(v)
	}
	recSize += 50 // overhead

	future := make(chan *RecordMetadata, 1)
	rec := accumulatorRecord{
		key:     key,
		value:   value,
		headers: headers,
		size:    recSize,
		addedAt: time.Now(),
		future:  future,
	}

	acc := p.getOrCreateAccumulator(topic)
	acc.append(int(partition), rec)

	return future, nil
}

// Flush waits for all accumulated messages to be sent.
func (p *Producer) Flush() error {
	if p.closed.Load() {
		return ErrClosed
	}

	p.accMu.Lock()
	allBatches := make(map[string]map[int][]accumulatorRecord)
	for topic, acc := range p.accumulator {
		allBatches[topic] = acc.drainAll()
	}
	p.accMu.Unlock()

	// Send all collected batches
	for topic, partBatches := range allBatches {
		for part, records := range partBatches {
			if len(records) > 0 {
				p.sendBatch(topic, int32(part), records)
			}
		}
	}
	return nil
}

// Close shuts down the producer gracefully.
// It flushes all pending messages and closes all connections.
func (p *Producer) Close() error {
	if p.closed.Swap(true) {
		return nil
	}

	// Flush remaining records
	_ = p.Flush()

	// Stop sender
	p.senderCancel()
	p.senderWg.Wait()

	// Stop receiver
	p.recvRunning.Store(false)
	p.pendingWg.Wait()

	// Close connection
	p.connMu.Lock()
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
	p.connMu.Unlock()

	// Fail remaining pending requests
	p.pendingMu.Lock()
	for _, ch := range p.pending {
		close(ch)
	}
	p.pending = nil
	p.pendingMu.Unlock()

	return nil
}

// ---------------------------------------------------------------------------
// Murmur2 Hash (matching Kafka Java client)
// ---------------------------------------------------------------------------

func murmur2(data []byte) uint32 {
	seed := uint32(0x9747B28C)
	m := uint32(0x5BD1E995)
	r := uint32(24)
	h := seed ^ uint32(len(data))

	length := len(data)
	i := 0
	for length >= 4 {
		k := uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
		length -= 4
		i += 4
	}

	switch length {
	case 3:
		h ^= uint32(data[i+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[i+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[i])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

// ---------------------------------------------------------------------------
// Produce Request/Response Encoding/Decoding
// ---------------------------------------------------------------------------

func encodeProduceRequest(topic string, partition int32, recordData []byte, acks int16, timeoutMs int) []byte {
	w := newBinaryWriter(256)
	// Transactional ID (nullable string)
	w.writeNullableString(nil)
	// Acks
	w.writeInt16(acks)
	// Timeout
	w.writeInt32(int32(timeoutMs))
	// Topic array
	w.writeArrayLength(1)
	w.writeString(topic)
	// Partition array
	w.writeArrayLength(1)
	w.writeInt32(partition)
	w.writeNullableBytes(recordData)
	w.writeTaggedFields()
	return w.bytes()
}

func decodeProduceResponse(data []byte, topic string, partition int32) (int64, int, error) {
	r := newBinaryReader(data)
	// throttle_time_ms
	_, err := r.readInt32()
	if err != nil {
		return 0, 0, err
	}
	// Topic array
	topicCount, err := r.readArrayLength()
	if err != nil {
		return 0, 0, err
	}
	for i := 0; i < topicCount; i++ {
		respTopic, err := r.readString()
		if err != nil {
			return 0, 0, err
		}
		partCount, err := r.readArrayLength()
		if err != nil {
			return 0, 0, err
		}
		for j := 0; j < partCount; j++ {
			partID, err := r.readInt32()
			if err != nil {
				return 0, 0, err
			}
			errCode, err := r.readInt16()
			if err != nil {
				return 0, 0, err
			}
			baseOffset, err := r.readInt64()
			if err != nil {
				return 0, 0, err
			}
			// consume rest
			_, _ = r.readInt64() // log_append_time
			_, _ = r.readInt64() // log_start_offset
			if respTopic == topic && partID == partition {
				return baseOffset, int(errCode), nil
			}
		}
	}
	return 0, 0, fmt.Errorf("partition %d not found in produce response for topic %s", partition, topic)
}

// ---------------------------------------------------------------------------
// Compression helpers
// ---------------------------------------------------------------------------

// compress applies compression to data based on the compression type.
func compress(data []byte, compressionType int) ([]byte, error) {
	switch compressionType {
	case CompressionNone:
		return data, nil
	case CompressionGzip:
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		if _, err := w.Write(data); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case CompressionSnappy:
		// Use a simple stub — in production, use klauspost/compress/snappy
		return data, nil
	case CompressionLz4:
		// Use a simple stub — in production, use pierrec/lz4
		return data, nil
	case CompressionZstd:
		// Use a simple stub — in production, use klauspost/compress/zstd
		return data, nil
	default:
		return data, nil
	}
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

func isNetTimeout(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}

// Ensure math is imported (used in murmur2, offset computations)
var _ = math.MaxInt32
