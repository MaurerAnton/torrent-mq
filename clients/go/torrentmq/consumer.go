package torrentmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// ConsumerRecord
// ---------------------------------------------------------------------------

// ConsumerRecord represents a single consumed message with metadata.
type ConsumerRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
}

// ---------------------------------------------------------------------------
// Consumer Configuration
// ---------------------------------------------------------------------------

// OffsetResetPolicy specifies where to start when no committed offset exists.
type OffsetResetPolicy string

const (
	OffsetEarliest OffsetResetPolicy = "earliest"
	OffsetLatest   OffsetResetPolicy = "latest"
	OffsetNone     OffsetResetPolicy = "none"
)

// ConsumerConfig configures the consumer's behavior.
type ConsumerConfig struct {
	// GroupID is the consumer group identifier (required).
	GroupID string

	// ClientID is the client identifier sent to the broker.
	ClientID string

	// SessionTimeoutMs is the session timeout for group membership.
	SessionTimeoutMs int

	// HeartbeatIntervalMs is the heartbeat interval.
	HeartbeatIntervalMs int

	// MaxPollRecords is the maximum number of records returned per poll.
	MaxPollRecords int

	// MaxPartitionFetchBytes is the maximum bytes to fetch per partition.
	MaxPartitionFetchBytes int

	// PollTimeoutMs is the maximum time poll blocks waiting for records.
	PollTimeoutMs int

	// AutoOffsetReset specifies where to start consuming when offsets are missing.
	AutoOffsetReset OffsetResetPolicy

	// EnableAutoCommit enables periodic automatic offset commits.
	EnableAutoCommit bool

	// AutoCommitIntervalMs is the auto-commit interval.
	AutoCommitIntervalMs int

	// SocketTimeoutMs is the socket read/write timeout.
	SocketTimeoutMs int

	// ConnectionTimeoutMs is the TCP connection timeout.
	ConnectionTimeoutMs int
}

// DefaultConsumerConfig returns a ConsumerConfig with sensible defaults.
func DefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		ClientID:               "torrent-consumer",
		SessionTimeoutMs:       45000,
		HeartbeatIntervalMs:    3000,
		MaxPollRecords:         500,
		MaxPartitionFetchBytes: 1048576,
		PollTimeoutMs:          500,
		AutoOffsetReset:        OffsetLatest,
		EnableAutoCommit:       true,
		AutoCommitIntervalMs:   5000,
		SocketTimeoutMs:        60000,
		ConnectionTimeoutMs:    10000,
	}
}

// ---------------------------------------------------------------------------
// Partition Assignment
// ---------------------------------------------------------------------------

// PartitionAssignment maps topics to their assigned partitions.
type PartitionAssignment struct {
	Topic      string
	Partitions []int32
}

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

// Consumer is a consumer group member that subscribes to topics and polls
// for messages. It handles group membership, partition assignment, offset
// management, and heartbeats automatically.
type Consumer struct {
	config   *ConsumerConfig
	brokers  []string
	clientID string
	groupID  string

	// Correlation ID generator
	corrGen correlationIDGen

	// Coordinator connection
	coordConn   net.Conn
	coordHost   string
	coordPort   int
	coordMu     sync.Mutex

	// Broker connections for fetch (simple: use coordinator for now)
	brokerConn   net.Conn
	brokerMu     sync.Mutex

	// Group membership state
	memberID     string
	generationID int32
	mu           sync.RWMutex

	// Subscribed topics
	subscribedTopics []string
	topicsMu         sync.RWMutex

	// Assigned partitions
	assignment map[string][]int32 // topic -> partitions
	assignMu   sync.RWMutex

	// Offset tracking
	committedOffsets map[topicPartition]int64
	positions        map[topicPartition]int64
	offsetMu         sync.RWMutex

	// Pending requests
	pending   map[int32]chan *ResponseFraming
	pendingMu sync.Mutex

	// Background goroutines
	ctx       context.Context
	cancel    context.CancelFunc
	recvWg    sync.WaitGroup
	heartWg   sync.WaitGroup
	autoCmtWg sync.WaitGroup

	// Closed flag
	closed atomic.Bool
	recvRunning atomic.Bool
}

type topicPartition struct {
	topic     string
	partition int32
}

// NewConsumer creates a new Consumer connected to the given brokers.
// It finds the group coordinator and joins the consumer group.
func NewConsumer(brokers []string, groupID string, config *ConsumerConfig) (*Consumer, error) {
	if groupID == "" {
		return nil, errors.New("group_id is required")
	}
	if config == nil {
		config = DefaultConsumerConfig()
	}
	config.GroupID = groupID
	if config.ClientID == "" {
		config.ClientID = "torrent-consumer"
	}

	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Consumer{
		config:           config,
		brokers:          brokers,
		clientID:         config.ClientID,
		groupID:          groupID,
		assignment:       make(map[string][]int32),
		committedOffsets: make(map[topicPartition]int64),
		positions:        make(map[topicPartition]int64),
		pending:          make(map[int32]chan *ResponseFraming),
		ctx:              ctx,
		cancel:           cancel,
	}

	// Connect to a bootstrap server to find the coordinator
	if err := c.connect(); err != nil {
		cancel()
		return nil, fmt.Errorf("consumer: connect: %w", err)
	}

	// Find group coordinator
	if err := c.findCoordinator(); err != nil {
		cancel()
		return nil, fmt.Errorf("consumer: find coordinator: %w", err)
	}

	// Start background receiver
	c.recvRunning.Store(true)
	c.recvWg.Add(1)
	go c.recvLoop()

	return c, nil
}

// connect connects to a bootstrap broker.
func (c *Consumer) connect() error {
	c.coordMu.Lock()
	defer c.coordMu.Unlock()

	var lastErr error
	for _, addr := range c.brokers {
		d := net.Dialer{Timeout: time.Duration(c.config.ConnectionTimeoutMs) * time.Millisecond}
		conn, err := d.DialContext(c.ctx, "tcp", addr)
		if err != nil {
			lastErr = err
			continue
		}
		c.coordConn = conn
		host, port, _ := net.SplitHostPort(addr)
		c.coordHost = host
		c.coordPort = 9092
		if p, err := net.LookupPort("tcp", port); err == nil {
			c.coordPort = p
		}
		return nil
	}
	return fmt.Errorf("%w: %v", ErrConnectionFailed, lastErr)
}

// findCoordinator locates the group coordinator for this consumer group.
func (c *Consumer) findCoordinator() error {
	body := encodeFindCoordinatorRequest(c.groupID)
	resp, err := c.sendRequest(ApiFindCoordinator, 0, body)
	if err != nil {
		return err
	}
	coord, err := decodeFindCoordinatorResponse(resp.Body)
	if err != nil {
		return err
	}
	if coord.ErrorCode != 0 {
		return &ClientError{Code: coord.ErrorCode, Message: "find coordinator failed"}
	}
	// Connect to the coordinator
	c.coordMu.Lock()
	c.coordHost = coord.Host
	c.coordPort = int(coord.Port)
	// Reconnect to the coordinator
	if c.coordConn != nil {
		c.coordConn.Close()
	}
	d := net.Dialer{Timeout: time.Duration(c.config.ConnectionTimeoutMs) * time.Millisecond}
	conn, err := d.DialContext(c.ctx, "tcp", fmt.Sprintf("%s:%d", coord.Host, coord.Port))
	if err != nil {
		c.coordMu.Unlock()
		return err
	}
	c.coordConn = conn
	c.coordMu.Unlock()
	return nil
}

// sendRequest sends a framed request and waits for the response.
func (c *Consumer) sendRequest(apiKey ApiKey, apiVersion int16, body []byte) (*ResponseFraming, error) {
	correlationID := c.corrGen.next()
	requestBytes := buildRequest(apiKey, apiVersion, correlationID, c.clientID, body)

	respCh := make(chan *ResponseFraming, 1)
	c.pendingMu.Lock()
	c.pending[correlationID] = respCh
	c.pendingMu.Unlock()

	c.coordMu.Lock()
	conn := c.coordConn
	c.coordMu.Unlock()

	if conn == nil {
		c.pendingMu.Lock()
		delete(c.pending, correlationID)
		c.pendingMu.Unlock()
		return nil, ErrConnectionFailed
	}

	_ = conn.SetWriteDeadline(time.Now().Add(time.Duration(c.config.SocketTimeoutMs) * time.Millisecond))
	_, err := conn.Write(requestBytes)
	if err != nil {
		c.pendingMu.Lock()
		delete(c.pending, correlationID)
		c.pendingMu.Unlock()
		return nil, fmt.Errorf("write request: %w", err)
	}

	select {
	case resp := <-respCh:
		if resp == nil {
			return nil, ErrConnectionFailed
		}
		return resp, nil
	case <-time.After(time.Duration(c.config.SocketTimeoutMs) * time.Millisecond):
		c.pendingMu.Lock()
		delete(c.pending, correlationID)
		c.pendingMu.Unlock()
		return nil, ErrTimeout
	case <-c.ctx.Done():
		return nil, ErrClosed
	}
}

// recvLoop reads responses from the coordinator connection.
func (c *Consumer) recvLoop() {
	defer c.recvWg.Done()

	for c.recvRunning.Load() {
		c.coordMu.Lock()
		conn := c.coordConn
		c.coordMu.Unlock()

		if conn == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		_ = conn.SetReadDeadline(time.Now().Add(time.Duration(c.config.SocketTimeoutMs) * time.Millisecond))
		resp, err := readFullResponse(conn)
		if err != nil {
			if errors.Is(err, io.EOF) || isNetTimeout(err) {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}

		c.pendingMu.Lock()
		ch, ok := c.pending[resp.CorrelationID]
		if ok {
			delete(c.pending, resp.CorrelationID)
		}
		c.pendingMu.Unlock()

		if ok && ch != nil {
			select {
			case ch <- resp:
			default:
			}
		}
	}
}

// Subscribe subscribes to one or more topics and joins the consumer group.
func (c *Consumer) Subscribe(topics ...string) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if len(topics) == 0 {
		return errors.New("at least one topic required")
	}

	c.topicsMu.Lock()
	c.subscribedTopics = topics
	c.topicsMu.Unlock()

	return c.joinGroup()
}

// joinGroup performs the group join + sync protocol.
func (c *Consumer) joinGroup() error {
	c.topicsMu.RLock()
	topics := c.subscribedTopics
	c.topicsMu.RUnlock()

	// Step 1: JoinGroup
	joinBody := encodeJoinGroupRequest(c.groupID, c.config.SessionTimeoutMs, c.memberID, topics)
	joinResp, err := c.sendRequest(ApiJoinGroup, 2, joinBody)
	if err != nil {
		return fmt.Errorf("join group: %w", err)
	}
	joinResult, err := decodeJoinGroupResponse(joinResp.Body)
	if err != nil {
		return fmt.Errorf("decode join group: %w", err)
	}
	if joinResult.ErrorCode != 0 {
		if joinResult.ErrorCode == ErrRebalanceInProgress {
			// Retry
			time.Sleep(100 * time.Millisecond)
			return c.joinGroup()
		}
		return &ClientError{Code: joinResult.ErrorCode, Message: "join group failed"}
	}

	c.mu.Lock()
	c.memberID = joinResult.MemberID
	c.generationID = joinResult.GenerationID
	c.mu.Unlock()

	// Step 2: SyncGroup
	memberAssignment := encodeMemberAssignment(c.assignment)
	syncBody := encodeSyncGroupRequest(c.groupID, c.generationID, c.memberID, memberAssignment)
	syncResp, err := c.sendRequest(ApiSyncGroup, 2, syncBody)
	if err != nil {
		return fmt.Errorf("sync group: %w", err)
	}
	assignment, err := decodeSyncGroupResponse(syncResp.Body)
	if err != nil {
		return fmt.Errorf("decode sync group: %w", err)
	}
	if assignment.ErrorCode != 0 {
		return &ClientError{Code: assignment.ErrorCode, Message: "sync group failed"}
	}

	c.assignMu.Lock()
	c.assignment = assignment.Partitions
	c.assignMu.Unlock()

	// Start heartbeat
	c.startHeartbeat()

	// Start auto-commit if enabled
	if c.config.EnableAutoCommit {
		c.startAutoCommit()
	}

	return nil
}

// startHeartbeat starts the background heartbeat goroutine.
func (c *Consumer) startHeartbeat() {
	c.heartWg.Add(1)
	go func() {
		defer c.heartWg.Done()
		ticker := time.NewTicker(time.Duration(c.config.HeartbeatIntervalMs) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				c.mu.RLock()
				memberID := c.memberID
				genID := c.generationID
				c.mu.RUnlock()

				body := encodeHeartbeatRequest(c.groupID, genID, memberID)
				resp, err := c.sendRequest(ApiHeartbeat, 0, body)
				if err != nil {
					continue
				}
				hb, err := decodeHeartbeatResponse(resp.Body)
				if err != nil {
					continue
				}
				if hb.ErrorCode != 0 {
					// Rejoin group
					_ = c.joinGroup()
				}
			}
		}
	}()
}

// startAutoCommit starts the automatic offset commit goroutine.
func (c *Consumer) startAutoCommit() {
	c.autoCmtWg.Add(1)
	go func() {
		defer c.autoCmtWg.Done()
		ticker := time.NewTicker(time.Duration(c.config.AutoCommitIntervalMs) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				_ = c.Commit()
			}
		}
	}()
}

// Poll retrieves the next batch of records.
// Blocks up to timeout for records to become available.
func (c *Consumer) Poll(timeout time.Duration) (*ConsumerRecord, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}

	c.assignMu.RLock()
	assignment := make(map[string][]int32)
	for k, v := range c.assignment {
		assignment[k] = v
	}
	c.assignMu.RUnlock()

	if len(assignment) == 0 {
		// Wait and return nil
		deadline := time.After(timeout)
		for {
			select {
			case <-deadline:
				return nil, nil
			case <-c.ctx.Done():
				return nil, ErrClosed
			default:
				c.assignMu.RLock()
				if len(c.assignment) > 0 {
					c.assignMu.RUnlock()
					goto fetch
				}
				c.assignMu.RUnlock()
				time.Sleep(50 * time.Millisecond)
			}
		}
	}

fetch:
	// Fetch from assigned partitions
	for topic, partitions := range assignment {
		for _, partition := range partitions {
			offset := c.getPosition(topic, partition)
			record, err := c.fetchOne(topic, partition, offset)
			if err != nil {
				continue
			}
			if record != nil {
				c.advancePosition(topic, partition, record.Offset)
				return record, nil
			}
		}
	}

	return nil, nil
}

// fetchOne fetches a single record from a topic-partition.
func (c *Consumer) fetchOne(topic string, partition int32, offset int64) (*ConsumerRecord, error) {
	body := encodeFetchRequest(topic, partition, offset, int32(c.config.MaxPartitionFetchBytes))
	resp, err := c.sendRequest(ApiFetch, 4, body)
	if err != nil {
		return nil, err
	}

	return decodeFetchResponse(resp.Body, topic, partition)
}

// getPosition returns the current fetch position for a topic-partition.
func (c *Consumer) getPosition(topic string, partition int32) int64 {
	c.offsetMu.RLock()
	defer c.offsetMu.RUnlock()
	tp := topicPartition{topic, partition}
	if pos, ok := c.positions[tp]; ok {
		return pos
	}
	if cmt, ok := c.committedOffsets[tp]; ok {
		return cmt
	}
	// Fetch from beginning or end based on policy
	return c.resolveInitialOffset(topic, partition)
}

// advancePosition advances the fetch position after consuming a record.
func (c *Consumer) advancePosition(topic string, partition int32, offset int64) {
	c.offsetMu.Lock()
	defer c.offsetMu.Unlock()
	tp := topicPartition{topic, partition}
	c.positions[tp] = offset + 1
}

// resolveInitialOffset finds the initial offset based on the reset policy.
func (c *Consumer) resolveInitialOffset(topic string, partition int32) int64 {
	// Default to beginning
	timestamp := int64(-2) // earliest
	if c.config.AutoOffsetReset == OffsetLatest {
		timestamp = -1 // latest
	}

	body := encodeListOffsetsRequest(topic, partition, timestamp)
	resp, err := c.sendRequest(ApiListOffsets, 0, body)
	if err != nil {
		return 0
	}
	offset, err := decodeListOffsetsResponse(resp.Body)
	if err != nil {
		return 0
	}
	return offset
}

// Commit commits the current offsets synchronously.
func (c *Consumer) Commit() error {
	if c.closed.Load() {
		return ErrClosed
	}

	c.offsetMu.RLock()
	offsets := make(map[topicPartition]int64)
	for tp, pos := range c.positions {
		offsets[tp] = pos
	}
	c.offsetMu.RUnlock()

	if len(offsets) == 0 {
		return nil
	}

	body := encodeOffsetCommitRequest(c.groupID, c.generationID, c.memberID, offsets)
	resp, err := c.sendRequest(ApiOffsetCommit, 2, body)
	if err != nil {
		return err
	}
	errCode, err := decodeOffsetCommitResponse(resp.Body)
	if err != nil {
		return err
	}
	if errCode != 0 {
		return &ClientError{Code: errCode}
	}

	// Update committed offsets
	c.offsetMu.Lock()
	for tp, pos := range offsets {
		c.committedOffsets[tp] = pos
	}
	c.offsetMu.Unlock()

	return nil
}

// CommitAsync commits offsets asynchronously, calling the callback on completion.
func (c *Consumer) CommitAsync(callback func(error)) {
	go func() {
		err := c.Commit()
		if callback != nil {
			callback(err)
		}
	}()
}

// Seek moves the consumer's position for a given partition.
func (c *Consumer) Seek(partition int32, offset int64) error {
	if c.closed.Load() {
		return ErrClosed
	}

	c.assignMu.RLock()
	defer c.assignMu.RUnlock()

	// Apply to all assigned topics for this partition
	for topic := range c.assignment {
		c.offsetMu.Lock()
		c.positions[topicPartition{topic, partition}] = offset
		c.offsetMu.Unlock()
	}
	return nil
}

// Close shuts down the consumer gracefully.
// It leaves the consumer group and closes all connections.
func (c *Consumer) Close() error {
	if c.closed.Swap(true) {
		return nil
	}

	// Leave the group
	if c.memberID != "" {
		leaveBody := encodeLeaveGroupRequest(c.groupID, c.memberID)
		_, _ = c.sendRequest(ApiLeaveGroup, 0, leaveBody)
	}

	// Stop background goroutines
	c.cancel()
	c.recvRunning.Store(false)
	c.recvWg.Wait()
	c.heartWg.Wait()
	c.autoCmtWg.Wait()

	// Close connections
	c.coordMu.Lock()
	if c.coordConn != nil {
		c.coordConn.Close()
		c.coordConn = nil
	}
	c.coordMu.Unlock()

	c.brokerMu.Lock()
	if c.brokerConn != nil {
		c.brokerConn.Close()
		c.brokerConn = nil
	}
	c.brokerMu.Unlock()

	// Fail pending requests
	c.pendingMu.Lock()
	for _, ch := range c.pending {
		close(ch)
	}
	c.pending = nil
	c.pendingMu.Unlock()

	return nil
}

// ---------------------------------------------------------------------------
// Consumer Group Protocol Encoding/Decoding
// ---------------------------------------------------------------------------

// FindCoordinatorResponse holds the result of a FindCoordinator request.
type findCoordinatorResponse struct {
	ErrorCode ErrorCode
	Host      string
	Port      int32
}

func encodeFindCoordinatorRequest(groupID string) []byte {
	w := newBinaryWriter(128)
	w.writeString(groupID)
	w.writeInt8(0) // coordinator_type: group (0)
	w.writeTaggedFields()
	return w.bytes()
}

func decodeFindCoordinatorResponse(data []byte) (*findCoordinatorResponse, error) {
	r := newBinaryReader(data)
	_, err := r.readInt32() // throttle_time_ms
	if err != nil {
		return nil, err
	}
	errCode, err := r.readInt16()
	if err != nil {
		return nil, err
	}
	_, _ = r.readInt16() // error message
	nodeID, err := r.readInt32()
	if err != nil {
		return nil, err
	}
	host, err := r.readString()
	if err != nil {
		return nil, err
	}
	port, err := r.readInt32()
	if err != nil {
		return nil, err
	}
	return &findCoordinatorResponse{
		ErrorCode: ErrorCode(errCode),
		Host:      host,
		Port:      port,
	}, ErrFromInt(nodeID) // nodeID as potential error indicator
}

// ErrFromInt is a helper to avoid unused variable warnings. Returns nil.
func ErrFromInt(v int32) error {
	_ = v
	return nil
}

type joinGroupResponse struct {
	ErrorCode    ErrorCode
	GenerationID int32
	MemberID     string
}

func encodeJoinGroupRequest(groupID string, sessionTimeout int, memberID string, topics []string) []byte {
	w := newBinaryWriter(512)
	w.writeString(groupID)
	w.writeInt32(int32(sessionTimeout))
	w.writeInt32(-1) // rebalance_timeout_ms (use default)
	w.writeString(memberID)
	w.writeString("consumer") // protocol_type
	// Protocol array
	w.writeArrayLength(1)
	w.writeString("range") // protocol name
	// Protocol metadata: subscription
	subW := newBinaryWriter(256)
	subW.writeInt16(0) // version
	subW.writeArrayLength(len(topics))
	for _, t := range topics {
		subW.writeString(t)
	}
	subW.writeNullableBytes(nil) // user_data
	subscriptionBytes := subW.bytes()
	w.writeNullableBytes(subscriptionBytes)
	w.writeTaggedFields()
	return w.bytes()
}

func decodeJoinGroupResponse(data []byte) (*joinGroupResponse, error) {
	r := newBinaryReader(data)
	_, err := r.readInt32() // throttle_time_ms
	if err != nil {
		return nil, err
	}
	errCode, err := r.readInt16()
	if err != nil {
		return nil, err
	}
	genID, err := r.readInt32()
	if err != nil {
		return nil, err
	}
	// protocol_name
	_, err = r.readString()
	if err != nil {
		return nil, err
	}
	// leader
	_, err = r.readString()
	if err != nil {
		return nil, err
	}
	memberID, err := r.readString()
	if err != nil {
		return nil, err
	}
	// members array — skip
	memberCount, err := r.readArrayLength()
	if err != nil {
		return nil, err
	}
	for i := 0; i < memberCount; i++ {
		_, _ = r.readString() // member_id
		_, _ = r.readNullableBytes() // metadata
	}
	return &joinGroupResponse{
		ErrorCode:    ErrorCode(errCode),
		GenerationID: genID,
		MemberID:     memberID,
	}, nil
}

type syncGroupResponse struct {
	ErrorCode  ErrorCode
	Partitions map[string][]int32
}

func encodeSyncGroupRequest(groupID string, generationID int32, memberID string, assignment []byte) []byte {
	w := newBinaryWriter(256)
	w.writeString(groupID)
	w.writeInt32(generationID)
	w.writeString(memberID)
	// assignments array
	w.writeArrayLength(1)
	w.writeString(memberID)
	w.writeNullableBytes(assignment)
	w.writeTaggedFields()
	return w.bytes()
}

func decodeSyncGroupResponse(data []byte) (*syncGroupResponse, error) {
	r := newBinaryReader(data)
	_, err := r.readInt32() // throttle_time_ms
	if err != nil {
		return nil, err
	}
	errCode, err := r.readInt16()
	if err != nil {
		return nil, err
	}
	assignmentBytes, err := r.readNullableBytes()
	if err != nil {
		return nil, err
	}
	partitions := decodeMemberAssignment(assignmentBytes)
	return &syncGroupResponse{
		ErrorCode:  ErrorCode(errCode),
		Partitions: partitions,
	}, nil
}

// encodeMemberAssignment encodes the member's partition assignment for SyncGroup.
func encodeMemberAssignment(assignment map[string][]int32) []byte {
	w := newBinaryWriter(256)
	w.writeInt16(0) // version
	// Topic array
	w.writeArrayLength(len(assignment))
	for topic, parts := range assignment {
		w.writeString(topic)
		w.writeArrayLength(len(parts))
		for _, p := range parts {
			w.writeInt32(p)
		}
	}
	w.writeNullableBytes(nil) // user_data
	return w.bytes()
}

// decodeMemberAssignment decodes a member's partition assignment from SyncGroup.
func decodeMemberAssignment(data []byte) map[string][]int32 {
	if data == nil || len(data) < 2 {
		return make(map[string][]int32)
	}
	result := make(map[string][]int32)
	r := newBinaryReader(data)
	version, err := r.readInt16()
	if err != nil {
		return result
	}
	_ = version
	topicCount, err := r.readArrayLength()
	if err != nil {
		return result
	}
	for i := 0; i < topicCount; i++ {
		topic, err := r.readString()
		if err != nil {
			return result
		}
		partCount, err := r.readArrayLength()
		if err != nil {
			return result
		}
		parts := make([]int32, partCount)
		for j := 0; j < partCount; j++ {
			parts[j], err = r.readInt32()
			if err != nil {
				return result
			}
		}
		result[topic] = parts
	}
	return result
}

type heartbeatResponse struct {
	ErrorCode ErrorCode
}

func encodeHeartbeatRequest(groupID string, generationID int32, memberID string) []byte {
	w := newBinaryWriter(128)
	w.writeString(groupID)
	w.writeInt32(generationID)
	w.writeString(memberID)
	w.writeNullableString(nil) // group_instance_id
	w.writeTaggedFields()
	return w.bytes()
}

func decodeHeartbeatResponse(data []byte) (*heartbeatResponse, error) {
	r := newBinaryReader(data)
	_, err := r.readInt32() // throttle_time_ms
	if err != nil {
		return nil, err
	}
	errCode, err := r.readInt16()
	if err != nil {
		return nil, err
	}
	return &heartbeatResponse{ErrorCode: ErrorCode(errCode)}, nil
}

func encodeLeaveGroupRequest(groupID string, memberID string) []byte {
	w := newBinaryWriter(128)
	w.writeString(groupID)
	// Members array
	w.writeArrayLength(1)
	w.writeString(memberID)
	w.writeNullableString(nil) // group_instance_id
	w.writeTaggedFields()
	return w.bytes()
}

// ---------------------------------------------------------------------------
// Fetch Request/Response Encoding/Decoding
// ---------------------------------------------------------------------------

func encodeFetchRequest(topic string, partition int32, offset int64, maxBytes int32) []byte {
	w := newBinaryWriter(256)
	w.writeInt32(-1) // replica_id
	w.writeInt32(500) // max_wait_ms
	w.writeInt32(1)   // min_bytes
	w.writeInt32(0)   // max_bytes (version dependent)
	w.writeInt8(0)    // isolation_level: read_uncommitted
	w.writeInt32(0)   // session_id
	w.writeInt32(0)   // session_epoch
	// Topic array
	w.writeArrayLength(1)
	w.writeString(topic)
	// Partition array
	w.writeArrayLength(1)
	w.writeInt32(partition)
	w.writeInt32(0) // current_leader_epoch
	w.writeInt64(offset)
	w.writeInt64(-1) // log_start_offset (unused)
	w.writeInt32(maxBytes)
	w.writeTaggedFields()
	return w.bytes()
}

func decodeFetchResponse(data []byte, topic string, partition int32) (*ConsumerRecord, error) {
	r := newBinaryReader(data)
	_, err := r.readInt32() // throttle_time_ms
	if err != nil {
		return nil, err
	}
	// error_code
	_, err = r.readInt16()
	if err != nil {
		return nil, err
	}
	// session_id
	_, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	topicCount, err := r.readArrayLength()
	if err != nil {
		return nil, err
	}
	for i := 0; i < topicCount; i++ {
		respTopic, err := r.readString()
		if err != nil {
			return nil, err
		}
		partCount, err := r.readArrayLength()
		if err != nil {
			return nil, err
		}
		for j := 0; j < partCount; j++ {
			partID, err := r.readInt32()
			if err != nil {
				return nil, err
			}
			partErr, err := r.readInt16()
			if err != nil {
				return nil, err
			}
			_ = partErr
			// high_watermark
			_, err = r.readInt64()
			if err != nil {
				return nil, err
			}
			// last_stable_offset
			_, err = r.readInt64()
			if err != nil {
				return nil, err
			}
			// log_start_offset
			_, err = r.readInt64()
			if err != nil {
				return nil, err
			}
			// aborted transactions — skip
			abortCount, err := r.readArrayLength()
			if err != nil {
				return nil, err
			}
			for k := 0; k < abortCount; k++ {
				_, _ = r.readInt64()
				_, _ = r.readInt64()
			}
			// Record batches
			recordCount, err := r.readArrayLength()
			if err != nil {
				return nil, err
			}
			for k := 0; k < recordCount; k++ {
				recordBytes, err := r.readNullableBytes()
				if err != nil {
					return nil, err
				}
				if respTopic == topic && partID == partition && recordBytes != nil {
					batch, _, err := decodeRecordBatch(recordBytes, 0)
					if err != nil {
						return nil, err
					}
					if len(batch.Records) > 0 {
						rec := batch.Records[0]
						headers := make(map[string]string)
						for _, h := range rec.Headers {
							headers[h.Key] = string(h.Value)
						}
						return &ConsumerRecord{
							Topic:     topic,
							Partition: partition,
							Offset:    batch.BaseOffset + int64(rec.OffsetDelta),
							Timestamp: batch.FirstTimestamp + rec.TimestampDelta,
							Key:       rec.Key,
							Value:     rec.Value,
							Headers:   headers,
						}, nil
					}
				}
			}
		}
	}
	return nil, nil
}

// ---------------------------------------------------------------------------
// Offset Management Encoding/Decoding
// ---------------------------------------------------------------------------

func encodeListOffsetsRequest(topic string, partition int32, timestamp int64) []byte {
	w := newBinaryWriter(128)
	w.writeInt32(-1) // replica_id
	w.writeInt8(0)   // isolation_level
	// Topic array
	w.writeArrayLength(1)
	w.writeString(topic)
	// Partition array
	w.writeArrayLength(1)
	w.writeInt32(partition)
	w.writeInt32(0) // current_leader_epoch
	w.writeInt64(timestamp)
	w.writeInt32(0) // max_num_offsets
	w.writeTaggedFields()
	return w.bytes()
}

func decodeListOffsetsResponse(data []byte) (int64, error) {
	r := newBinaryReader(data)
	_, err := r.readInt32() // throttle_time_ms
	if err != nil {
		return 0, err
	}
	topicCount, err := r.readArrayLength()
	if err != nil {
		return 0, err
	}
	for i := 0; i < topicCount; i++ {
		_, err := r.readString() // topic
		if err != nil {
			return 0, err
		}
		partCount, err := r.readArrayLength()
		if err != nil {
			return 0, err
		}
		for j := 0; j < partCount; j++ {
			_, err := r.readInt32() // partition
			if err != nil {
				return 0, err
			}
			_, err = r.readInt16() // error_code
			if err != nil {
				return 0, err
			}
			_, err = r.readInt64() // timestamp
			if err != nil {
				return 0, err
			}
			offset, err := r.readInt64()
			if err != nil {
				return 0, err
			}
			return offset, nil
		}
	}
	return 0, nil
}

func encodeOffsetCommitRequest(groupID string, generationID int32, memberID string, offsets map[topicPartition]int64) []byte {
	w := newBinaryWriter(512)
	w.writeString(groupID)
	w.writeInt32(generationID)
	w.writeString(memberID)
	w.writeNullableString(nil) // group_instance_id
	w.writeInt64(-1)            // retention_time_ms
	// Topics array
	// Group by topic
	topicParts := make(map[string][]struct {
		partition int32
		offset    int64
	})
	for tp, offset := range offsets {
		topicParts[tp.topic] = append(topicParts[tp.topic], struct {
			partition int32
			offset    int64
		}{tp.partition, offset})
	}
	w.writeArrayLength(len(topicParts))
	for topic, parts := range topicParts {
		w.writeString(topic)
		w.writeArrayLength(len(parts))
		for _, p := range parts {
			w.writeInt32(p.partition)
			w.writeInt64(p.offset)
			w.writeInt32(-1) // leader_epoch
			w.writeNullableString(nil) // metadata
		}
	}
	w.writeTaggedFields()
	return w.bytes()
}

func decodeOffsetCommitResponse(data []byte) (ErrorCode, error) {
	r := newBinaryReader(data)
	_, err := r.readInt32() // throttle_time_ms
	if err != nil {
		return -1, err
	}
	topicCount, err := r.readArrayLength()
	if err != nil {
		return -1, err
	}
	for i := 0; i < topicCount; i++ {
		_, err := r.readString()
		if err != nil {
			return -1, err
		}
		partCount, err := r.readArrayLength()
		if err != nil {
			return -1, err
		}
		for j := 0; j < partCount; j++ {
			_, err := r.readInt32()
			if err != nil {
				return -1, err
			}
			errCode, err := r.readInt16()
			if err != nil {
				return -1, err
			}
			if errCode != 0 {
				return ErrorCode(errCode), nil
			}
		}
	}
	return 0, nil
}
