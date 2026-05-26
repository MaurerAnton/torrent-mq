package torrentmq

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Data Transfer Objects
// ---------------------------------------------------------------------------

// ConfigEntry represents a single configuration key-value pair.
type ConfigEntry struct {
	Name       string
	Value      string
	IsDefault  bool
	IsSensitive bool
	IsReadOnly bool
	Source     string
}

// PartitionDetail holds detailed partition information.
type PartitionDetail struct {
	PartitionID    int32
	LeaderID       int32
	ReplicaIDs     []int32
	ISRIDs         []int32
	LogStartOffset int64
	LogEndOffset   int64
}

// TopicDescription contains full details about a topic.
type TopicDescription struct {
	Name              string
	IsInternal        bool
	NumPartitions     int32
	ReplicationFactor int16
	Partitions        []PartitionDetail
	Configs           []ConfigEntry
}

// BrokerDetail holds information about a broker.
type BrokerDetail struct {
	BrokerID int32
	Host     string
	Port     int32
	Rack     string
}

// ClusterDescription holds top-level cluster metadata.
type ClusterDescription struct {
	ClusterID    string
	ControllerID int32
	Brokers      []BrokerDetail
	Topics       []string
}

// ConsumerGroupMember describes a member of a consumer group.
type ConsumerGroupMember struct {
	MemberID   string
	ClientID   string
	ClientHost string
}

// ConsumerGroupDescription describes a consumer group.
type ConsumerGroupDescription struct {
	GroupID       string
	State         string
	ProtocolType  string
	Protocol      string
	Members       []ConsumerGroupMember
	CoordinatorID int32
}

// ConsumerGroupSummary is a lightweight summary of a consumer group.
type ConsumerGroupSummary struct {
	GroupID      string
	ProtocolType string
	State        string
}

// ---------------------------------------------------------------------------
// AdminClient
// ---------------------------------------------------------------------------

// AdminClient provides administrative operations for the torrent-mq cluster.
// It communicates with the controller broker via admin RPCs to manage topics,
// consumer groups, and configurations.
type AdminClient struct {
	brokers         []string
	clientID        string
	requestTimeoutMs int

	// Connection management
	conn    net.Conn
	connMu  sync.Mutex

	// Correlation ID generator
	corrGen correlationIDGen

	// Pending requests
	pending   map[int32]chan *ResponseFraming
	pendingMu sync.Mutex

	// Metadata cache
	metadata   *ClusterMetadata
	metadataMu sync.RWMutex

	// Closed flag
	closed atomic.Bool
	recvRunning atomic.Bool
}

// NewAdminClient creates a new AdminClient connected to the given brokers.
func NewAdminClient(brokers []string) (*AdminClient, error) {
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}

	a := &AdminClient{
		brokers:          brokers,
		clientID:         "torrent-admin",
		requestTimeoutMs: 30000,
		pending:          make(map[int32]chan *ResponseFraming),
	}

	if err := a.connect(); err != nil {
		return nil, fmt.Errorf("admin: connect: %w", err)
	}

	// Fetch initial metadata
	if err := a.refreshMetadata(); err != nil {
		// Non-fatal
		_ = err
	}

	// Start receiver
	a.recvRunning.Store(true)
	go a.recvLoop()

	return a, nil
}

// connect establishes a connection to a bootstrap broker.
func (a *AdminClient) connect() error {
	a.connMu.Lock()
	defer a.connMu.Unlock()

	if a.conn != nil {
		a.conn.Close()
	}

	var lastErr error
	for _, addr := range a.brokers {
		d := net.Dialer{Timeout: time.Duration(a.requestTimeoutMs) * time.Millisecond}
		conn, err := d.Dial("tcp", addr)
		if err != nil {
			lastErr = err
			continue
		}
		a.conn = conn
		return nil
	}
	return fmt.Errorf("%w: %v", ErrConnectionFailed, lastErr)
}

// refreshMetadata fetches current cluster metadata.
func (a *AdminClient) refreshMetadata() error {
	body := encodeMetadataRequest(nil)
	resp, err := a.sendRequest(ApiMetadata, 4, body)
	if err != nil {
		return err
	}
	meta, err := decodeMetadataResponse(resp.Body)
	if err != nil {
		return fmt.Errorf("decode metadata: %w", err)
	}
	a.metadataMu.Lock()
	a.metadata = meta
	a.metadataMu.Unlock()
	return nil
}

// sendRequest sends a framed request and blocks for the response.
func (a *AdminClient) sendRequest(apiKey ApiKey, version int16, body []byte) (*ResponseFraming, error) {
	correlationID := a.corrGen.next()
	requestBytes := buildRequest(apiKey, version, correlationID, a.clientID, body)

	respCh := make(chan *ResponseFraming, 1)
	a.pendingMu.Lock()
	a.pending[correlationID] = respCh
	a.pendingMu.Unlock()

	a.connMu.Lock()
	conn := a.conn
	a.connMu.Unlock()

	if conn == nil {
		a.pendingMu.Lock()
		delete(a.pending, correlationID)
		a.pendingMu.Unlock()
		return nil, ErrConnectionFailed
	}

	_ = conn.SetWriteDeadline(time.Now().Add(time.Duration(a.requestTimeoutMs) * time.Millisecond))
	_, err := conn.Write(requestBytes)
	if err != nil {
		a.pendingMu.Lock()
		delete(a.pending, correlationID)
		a.pendingMu.Unlock()
		return nil, fmt.Errorf("write request: %w", err)
	}

	select {
	case resp := <-respCh:
		if resp == nil {
			return nil, ErrConnectionFailed
		}
		return resp, nil
	case <-time.After(time.Duration(a.requestTimeoutMs) * time.Millisecond):
		a.pendingMu.Lock()
		delete(a.pending, correlationID)
		a.pendingMu.Unlock()
		return nil, ErrTimeout
	}
}

// recvLoop reads responses from the broker connection.
func (a *AdminClient) recvLoop() {
	for a.recvRunning.Load() {
		a.connMu.Lock()
		conn := a.conn
		a.connMu.Unlock()

		if conn == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		_ = conn.SetReadDeadline(time.Now().Add(time.Duration(a.requestTimeoutMs) * time.Millisecond))
		resp, err := readFullResponse(conn)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		a.pendingMu.Lock()
		ch, ok := a.pending[resp.CorrelationID]
		if ok {
			delete(a.pending, resp.CorrelationID)
		}
		a.pendingMu.Unlock()

		if ok && ch != nil {
			select {
			case ch <- resp:
			default:
			}
		}
	}
}

// Close closes the AdminClient and its connections.
func (a *AdminClient) Close() error {
	if a.closed.Swap(true) {
		return nil
	}

	a.recvRunning.Store(false)

	a.connMu.Lock()
	if a.conn != nil {
		a.conn.Close()
		a.conn = nil
	}
	a.connMu.Unlock()

	a.pendingMu.Lock()
	for _, ch := range a.pending {
		close(ch)
	}
	a.pending = nil
	a.pendingMu.Unlock()

	return nil
}

// ---------------------------------------------------------------------------
// Topic Management
// ---------------------------------------------------------------------------

// CreateTopic creates a new topic with the specified configuration.
func (a *AdminClient) CreateTopic(name string, numPartitions int32, replicationFactor int16, configs map[string]string) error {
	if a.closed.Load() {
		return ErrClosed
	}
	if numPartitions < 1 {
		return fmt.Errorf("num_partitions must be >= 1, got %d", numPartitions)
	}
	if replicationFactor < 1 {
		return fmt.Errorf("replication_factor must be >= 1, got %d", replicationFactor)
	}

	body := encodeCreateTopicsRequest([]createTopicSpec{{
		Name:              name,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
		Configs:           configs,
	}}, a.requestTimeoutMs)

	resp, err := a.sendRequest(ApiCreateTopics, 2, body)
	if err != nil {
		return err
	}

	results, err := decodeCreateTopicsResponse(resp.Body)
	if err != nil {
		return err
	}

	if result, ok := results[name]; ok {
		if result.ErrorCode != 0 {
			return &ClientError{Code: result.ErrorCode, Message: result.ErrorMessage}
		}
		return nil
	}

	return fmt.Errorf("topic %s not found in create response", name)
}

// DeleteTopic deletes a topic by name.
func (a *AdminClient) DeleteTopic(name string) error {
	if a.closed.Load() {
		return ErrClosed
	}

	body := encodeDeleteTopicsRequest([]string{name}, a.requestTimeoutMs)
	resp, err := a.sendRequest(ApiDeleteTopics, 1, body)
	if err != nil {
		return err
	}

	results, err := decodeDeleteTopicsResponse(resp.Body)
	if err != nil {
		return err
	}

	if result, ok := results[name]; ok {
		if result.ErrorCode != 0 {
			return &ClientError{Code: result.ErrorCode, Message: result.ErrorMessage}
		}
		return nil
	}

	return fmt.Errorf("topic %s not found in delete response", name)
}

// ListTopics returns the names of all topics in the cluster.
func (a *AdminClient) ListTopics() ([]string, error) {
	if a.closed.Load() {
		return nil, ErrClosed
	}

	if err := a.refreshMetadata(); err != nil {
		return nil, err
	}

	a.metadataMu.RLock()
	defer a.metadataMu.RUnlock()

	topics := make([]string, 0, len(a.metadata.Topics))
	for _, t := range a.metadata.Topics {
		topics = append(topics, t.Name)
	}
	return topics, nil
}

// DescribeTopic returns detailed information about a specific topic.
func (a *AdminClient) DescribeTopic(name string) (*TopicDescription, error) {
	if a.closed.Load() {
		return nil, ErrClosed
	}

	if err := a.refreshMetadata(); err != nil {
		return nil, err
	}

	a.metadataMu.RLock()
	defer a.metadataMu.RUnlock()

	for _, t := range a.metadata.Topics {
		if t.Name == name {
			desc := &TopicDescription{
				Name:          t.Name,
				IsInternal:    t.IsInternal,
				NumPartitions: int32(len(t.Partitions)),
			}

			// Determine replication factor from first partition
			if len(t.Partitions) > 0 {
				desc.ReplicationFactor = int16(len(t.Partitions[0].Replicas))
			}

			for _, p := range t.Partitions {
				desc.Partitions = append(desc.Partitions, PartitionDetail{
					PartitionID: p.ID,
					LeaderID:    p.LeaderID,
					ReplicaIDs:  p.Replicas,
					ISRIDs:      p.ISR,
				})
			}
			return desc, nil
		}
	}

	return nil, &ClientError{Code: ErrUnknownTopicOrPartition, Message: fmt.Sprintf("topic %s not found", name)}
}

// ---------------------------------------------------------------------------
// Consumer Group Management
// ---------------------------------------------------------------------------

// ListConsumerGroups returns a list of all consumer groups.
func (a *AdminClient) ListConsumerGroups() ([]ConsumerGroupSummary, error) {
	if a.closed.Load() {
		return nil, ErrClosed
	}

	body := encodeListGroupsRequest()
	resp, err := a.sendRequest(ApiListGroups, 0, body)
	if err != nil {
		return nil, err
	}

	return decodeListGroupsResponse(resp.Body)
}

// DescribeConsumerGroup returns detailed information about a consumer group.
func (a *AdminClient) DescribeConsumerGroup(groupID string) (*ConsumerGroupDescription, error) {
	if a.closed.Load() {
		return nil, ErrClosed
	}

	body := encodeDescribeGroupsRequest([]string{groupID})
	resp, err := a.sendRequest(ApiDescribeGroups, 0, body)
	if err != nil {
		return nil, err
	}

	results, err := decodeDescribeGroupsResponse(resp.Body)
	if err != nil {
		return nil, err
	}

	if desc, ok := results[groupID]; ok {
		return desc, nil
	}
	return nil, &ClientError{Code: ErrGroupIDNotFound, Message: fmt.Sprintf("group %s not found", groupID)}
}

// DeleteConsumerGroup deletes a consumer group.
func (a *AdminClient) DeleteConsumerGroup(groupID string) error {
	if a.closed.Load() {
		return ErrClosed
	}

	body := encodeDeleteGroupsRequest([]string{groupID})
	resp, err := a.sendRequest(ApiDeleteGroups, 0, body)
	if err != nil {
		return err
	}

	results, err := decodeDeleteGroupsResponse(resp.Body)
	if err != nil {
		return err
	}

	if ec, ok := results[groupID]; ok {
		if ec != 0 {
			return &ClientError{Code: ec}
		}
		return nil
	}
	return nil
}

// ---------------------------------------------------------------------------
// Cluster Information
// ---------------------------------------------------------------------------

// DescribeCluster returns information about the cluster.
func (a *AdminClient) DescribeCluster() (*ClusterDescription, error) {
	if a.closed.Load() {
		return nil, ErrClosed
	}

	if err := a.refreshMetadata(); err != nil {
		return nil, err
	}

	a.metadataMu.RLock()
	defer a.metadataMu.RUnlock()

	desc := &ClusterDescription{
		ClusterID:    a.metadata.ClusterID,
		ControllerID: a.metadata.ControllerID,
	}

	for _, b := range a.metadata.Brokers {
		desc.Brokers = append(desc.Brokers, BrokerDetail{
			BrokerID: b.ID,
			Host:     b.Host,
			Port:     b.Port,
		})
	}

	for _, t := range a.metadata.Topics {
		desc.Topics = append(desc.Topics, t.Name)
	}

	return desc, nil
}

// ---------------------------------------------------------------------------
// Admin RPC Encoding/Decoding
// ---------------------------------------------------------------------------

type createTopicSpec struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
	Configs           map[string]string
}

type createTopicResult struct {
	ErrorCode    ErrorCode
	ErrorMessage string
}

func encodeCreateTopicsRequest(topics []createTopicSpec, timeoutMs int) []byte {
	w := newBinaryWriter(512)
	w.writeArrayLength(len(topics))
	for _, t := range topics {
		w.writeString(t.Name)
		w.writeInt32(t.NumPartitions)
		w.writeInt16(t.ReplicationFactor)
		// replica_assignment — null
		w.writeArrayLength(-1)
		// configs
		w.writeArrayLength(len(t.Configs))
		for k, v := range t.Configs {
			w.writeString(k)
			w.writeNullableString(&v)
		}
		w.writeTaggedFields()
	}
	w.writeInt32(int32(timeoutMs))
	w.writeBool(false) // validate_only
	w.writeTaggedFields()
	return w.bytes()
}

func decodeCreateTopicsResponse(data []byte) (map[string]createTopicResult, error) {
	r := newBinaryReader(data)
	_, err := r.readInt32() // throttle_time_ms
	if err != nil {
		return nil, err
	}
	topicCount, err := r.readArrayLength()
	if err != nil {
		return nil, err
	}
	results := make(map[string]createTopicResult, topicCount)
	for i := 0; i < topicCount; i++ {
		name, err := r.readString()
		if err != nil {
			return nil, err
		}
		ec, err := r.readInt16()
		if err != nil {
			return nil, err
		}
		msg, err := r.readNullableString()
		if err != nil {
			return nil, err
		}
		errMsg := ""
		if msg != nil {
			errMsg = *msg
		}
		results[name] = createTopicResult{
			ErrorCode:    ErrorCode(ec),
			ErrorMessage: errMsg,
		}
		// Consume remaining fields (num_partitions, replication_factor, configs)
		_, _ = r.readInt32()
		_, _ = r.readInt16()
		cfgCount, _ := r.readArrayLength()
		for j := 0; j < cfgCount; j++ {
			_, _ = r.readString()
			_, _ = r.readNullableString()
			_, _ = r.readBool() // is_sensitive
			_, _ = r.readBool() // is_read_only
			_, _ = r.readInt8() // source
		}
	}
	return results, nil
}

func encodeDeleteTopicsRequest(topics []string, timeoutMs int) []byte {
	w := newBinaryWriter(256)
	w.writeArrayLength(len(topics))
	for _, t := range topics {
		w.writeString(t)
	}
	w.writeInt32(int32(timeoutMs))
	w.writeTaggedFields()
	return w.bytes()
}

func decodeDeleteTopicsResponse(data []byte) (map[string]createTopicResult, error) {
	r := newBinaryReader(data)
	_, err := r.readInt32() // throttle_time_ms
	if err != nil {
		return nil, err
	}
	topicCount, err := r.readArrayLength()
	if err != nil {
		return nil, err
	}
	results := make(map[string]createTopicResult, topicCount)
	for i := 0; i < topicCount; i++ {
		name, err := r.readString()
		if err != nil {
			return nil, err
		}
		ec, err := r.readInt16()
		if err != nil {
			return nil, err
		}
		msg, err := r.readNullableString()
		if err != nil {
			return nil, err
		}
		errMsg := ""
		if msg != nil {
			errMsg = *msg
		}
		results[name] = createTopicResult{
			ErrorCode:    ErrorCode(ec),
			ErrorMessage: errMsg,
		}
	}
	return results, nil
}

func encodeListGroupsRequest() []byte {
	w := newBinaryWriter(64)
	w.writeTaggedFields()
	return w.bytes()
}

func decodeListGroupsResponse(data []byte) ([]ConsumerGroupSummary, error) {
	r := newBinaryReader(data)
	_, err := r.readInt32() // throttle_time_ms
	if err != nil {
		return nil, err
	}
	ec, err := r.readInt16()
	if err != nil {
		return nil, err
	}
	if ec != 0 {
		return nil, &ClientError{Code: ErrorCode(ec)}
	}
	groupCount, err := r.readArrayLength()
	if err != nil {
		return nil, err
	}
	groups := make([]ConsumerGroupSummary, 0, groupCount)
	for i := 0; i < groupCount; i++ {
		gid, err := r.readString()
		if err != nil {
			return nil, err
		}
		pt, err := r.readString()
		if err != nil {
			return nil, err
		}
		state, err := r.readString()
		if err != nil {
			return nil, err
		}
		groups = append(groups, ConsumerGroupSummary{
			GroupID:      gid,
			ProtocolType: pt,
			State:        state,
		})
	}
	return groups, nil
}

func encodeDescribeGroupsRequest(groupIDs []string) []byte {
	w := newBinaryWriter(128)
	w.writeArrayLength(len(groupIDs))
	for _, gid := range groupIDs {
		w.writeString(gid)
	}
	w.writeTaggedFields()
	return w.bytes()
}

func decodeDescribeGroupsResponse(data []byte) (map[string]*ConsumerGroupDescription, error) {
	r := newBinaryReader(data)
	_, err := r.readInt32() // throttle_time_ms
	if err != nil {
		return nil, err
	}
	groupCount, err := r.readArrayLength()
	if err != nil {
		return nil, err
	}
	results := make(map[string]*ConsumerGroupDescription, groupCount)
	for i := 0; i < groupCount; i++ {
		ec, err := r.readInt16()
		if err != nil {
			return nil, err
		}
		gid, err := r.readString()
		if err != nil {
			return nil, err
		}
		state, err := r.readString()
		if err != nil {
			return nil, err
		}
		pt, err := r.readString()
		if err != nil {
			return nil, err
		}
		protocol, err := r.readString()
		if err != nil {
			return nil, err
		}
		memberCount, err := r.readArrayLength()
		if err != nil {
			return nil, err
		}
		members := make([]ConsumerGroupMember, 0, memberCount)
		for j := 0; j < memberCount; j++ {
			memberID, err := r.readString()
			if err != nil {
				return nil, err
			}
			clientID, err := r.readString()
			if err != nil {
				return nil, err
			}
			clientHost, err := r.readString()
			if err != nil {
				return nil, err
			}
			// member_metadata — skip
			metaBytes, _ := r.readNullableBytes()
			_ = metaBytes
			// member_assignment — skip
			assignBytes, _ := r.readNullableBytes()
			_ = assignBytes
			members = append(members, ConsumerGroupMember{
				MemberID:   memberID,
				ClientID:   clientID,
				ClientHost: clientHost,
			})
		}
		desc := &ConsumerGroupDescription{
			GroupID:      gid,
			State:        state,
			ProtocolType: pt,
			Protocol:     protocol,
			Members:      members,
		}
		if ec != 0 {
			desc.State = fmt.Sprintf("error(%d)", ec)
		}
		results[gid] = desc
	}
	return results, nil
}

func encodeDeleteGroupsRequest(groupIDs []string) []byte {
	w := newBinaryWriter(128)
	w.writeArrayLength(len(groupIDs))
	for _, gid := range groupIDs {
		w.writeString(gid)
	}
	w.writeTaggedFields()
	return w.bytes()
}

func decodeDeleteGroupsResponse(data []byte) (map[string]ErrorCode, error) {
	r := newBinaryReader(data)
	_, err := r.readInt32() // throttle_time_ms
	if err != nil {
		return nil, err
	}
	groupCount, err := r.readArrayLength()
	if err != nil {
		return nil, err
	}
	results := make(map[string]ErrorCode, groupCount)
	for i := 0; i < groupCount; i++ {
		gid, err := r.readString()
		if err != nil {
			return nil, err
		}
		ec, err := r.readInt16()
		if err != nil {
			return nil, err
		}
		results[gid] = ErrorCode(ec)
	}
	return results, nil
}
