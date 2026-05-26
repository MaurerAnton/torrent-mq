package org.torrentmq.client;

import org.torrentmq.client.network.NetworkClient;
import org.torrentmq.client.protocol.Protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Admin client for torrent-mq cluster management operations.
 *
 * <p>Provides methods to create/delete/list/describe topics, manage consumer
 * groups, and inspect cluster metadata. All operations are synchronous and
 * communicate with the controller broker via the Kafka-compatible wire protocol.</p>
 *
 * <h3>Usage example</h3>
 * <pre>{@code
 * TorrentAdmin admin = new TorrentAdmin("localhost:9092");
 * admin.createTopic("my-topic", 3, (short) 2, Map.of("retention.ms", "86400000"));
 * List<String> topics = admin.listTopics();
 * admin.close();
 * }</pre>
 */
public final class TorrentAdmin implements AutoCloseable {

    // -----------------------------------------------------------------------
    // Data transfer objects
    // -----------------------------------------------------------------------

    /** A configuration key-value pair with metadata. */
    public record ConfigEntry(String name, String value, boolean isDefault,
                               boolean isSensitive, boolean isReadOnly, String source) {}

    /** Detailed partition information. */
    public record PartitionDetail(int partitionId, int leaderId, List<Integer> replicaIds,
                                   List<Integer> isrIds, long logStartOffset, long logEndOffset) {}

    /** Full topic description. */
    public record TopicDescription(String name, boolean internal, int numPartitions,
                                    short replicationFactor, List<PartitionDetail> partitions,
                                    List<ConfigEntry> configs) {}

    /** Broker information. */
    public record BrokerDetail(int brokerId, String host, int port, String rack) {}

    /** Cluster-level metadata. */
    public record ClusterDescription(String clusterId, int controllerId,
                                      List<BrokerDetail> brokers, List<String> topics) {}

    /** Consumer group member. */
    public record ConsumerGroupMember(String memberId, String clientId, String clientHost) {}

    /** Consumer group description. */
    public record ConsumerGroupDescription(String groupId, String state, String protocolType,
                                            String protocol, List<ConsumerGroupMember> members,
                                            int coordinatorId) {}

    /** Lightweight consumer group summary. */
    public record ConsumerGroupSummary(String groupId, String protocolType, String state) {}

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------

    private final String bootstrapHost;
    private final int    bootstrapPort;
    private final String clientId;
    private final int    requestTimeoutMs;

    private final NetworkClient networkClient;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Metadata cache
    private volatile ClusterMetadata clusterMetadata;
    private final Object metadataLock = new Object();

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    /**
     * Creates an AdminClient connected to the given bootstrap servers.
     *
     * @param bootstrapServers comma-separated host:port addresses
     * @throws IOException if connection fails
     */
    public TorrentAdmin(String bootstrapServers) throws IOException {
        this(bootstrapServers, null);
    }

    /**
     * Creates an AdminClient with custom settings.
     *
     * @param bootstrapServers comma-separated host:port addresses
     * @param config           optional admin configuration
     * @throws IOException if connection fails
     */
    public TorrentAdmin(String bootstrapServers, AdminConfig config) throws IOException {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            bootstrapServers = "localhost:9092";
        }
        if (config == null) config = new AdminConfig();

        this.clientId         = config.clientId;
        this.requestTimeoutMs = config.requestTimeoutMs;

        String[] parts = bootstrapServers.split(",");
        String firstAddr = parts[0].trim();
        int colon = firstAddr.lastIndexOf(':');
        if (colon > 0) {
            bootstrapHost = firstAddr.substring(0, colon);
            bootstrapPort = Integer.parseInt(firstAddr.substring(colon + 1));
        } else {
            bootstrapHost = firstAddr;
            bootstrapPort = 9092;
        }

        NetworkClient.Config netConfig = NetworkClient.Config.builder()
            .connectTimeoutMs(config.connectionTimeoutMs)
            .socketTimeoutMs(config.socketTimeoutMs)
            .build();
        this.networkClient = new NetworkClient(this.clientId, netConfig);

        networkClient.connect(bootstrapHost, bootstrapPort)
            .get(config.connectionTimeoutMs, TimeUnit.MILLISECONDS);

        // Initial metadata fetch (non-fatal)
        try { refreshMetadata(); } catch (Exception ignored) {}
    }

    // -----------------------------------------------------------------------
    // Admin configuration
    // -----------------------------------------------------------------------

    public static final class AdminConfig {
        public String clientId         = "torrent-admin";
        public int    requestTimeoutMs = 30_000;
        public int    connectionTimeoutMs = 10_000;
        public int    socketTimeoutMs     = 60_000;
    }

    // -----------------------------------------------------------------------
    // Topic management
    // -----------------------------------------------------------------------

    /**
     * Creates a new topic.
     *
     * @param name              topic name
     * @param numPartitions     number of partitions (must be &gt;= 1)
     * @param replicationFactor replication factor (must be &gt;= 1)
     * @param configs           optional topic-level configs (e.g. retention.ms)
     * @throws IOException on failure
     */
    public void createTopic(String name, int numPartitions, short replicationFactor,
                             Map<String, String> configs) throws IOException {
        checkOpen();
        if (numPartitions < 1) {
            throw new IllegalArgumentException("numPartitions must be >= 1, got " + numPartitions);
        }
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("replicationFactor must be >= 1, got " + replicationFactor);
        }

        byte[] body = encodeCreateTopicsRequest(
            List.of(new CreateTopicSpec(name, numPartitions, replicationFactor, configs)),
            requestTimeoutMs);

        Protocol.ResponseFraming resp = sendRequest(Protocol.API_CREATE_TOPICS, (short) 2, body);

        Map<String, TopicResult> results = decodeCreateTopicsResponse(resp.body);
        TopicResult result = results.get(name);
        if (result == null) {
            throw new IOException("Topic " + name + " not found in create response");
        }
        if (result.errorCode != Protocol.ERR_NONE) {
            throw new IOException("Create topic failed: " + Protocol.errorCodeDescription(result.errorCode)
                + (result.errorMessage != null ? " - " + result.errorMessage : ""));
        }
    }

    /**
     * Deletes a topic.
     *
     * @param name topic name
     * @throws IOException on failure
     */
    public void deleteTopic(String name) throws IOException {
        checkOpen();

        byte[] body = encodeDeleteTopicsRequest(List.of(name), requestTimeoutMs);
        Protocol.ResponseFraming resp = sendRequest(Protocol.API_DELETE_TOPICS, (short) 1, body);

        Map<String, TopicResult> results = decodeDeleteTopicsResponse(resp.body);
        TopicResult result = results.get(name);
        if (result == null) {
            throw new IOException("Topic " + name + " not found in delete response");
        }
        if (result.errorCode != Protocol.ERR_NONE) {
            throw new IOException("Delete topic failed: " + Protocol.errorCodeDescription(result.errorCode)
                + (result.errorMessage != null ? " - " + result.errorMessage : ""));
        }
    }

    /**
     * Lists all topics in the cluster.
     *
     * @return list of topic names
     * @throws IOException on failure
     */
    public List<String> listTopics() throws IOException {
        checkOpen();
        refreshMetadata();

        ClusterMetadata meta = clusterMetadata;
        if (meta == null) return Collections.emptyList();

        return new ArrayList<>(meta.topics.keySet());
    }

    /**
     * Returns detailed information about a topic.
     *
     * @param name topic name
     * @return topic description
     * @throws IOException if topic not found or metadata fetch fails
     */
    public TopicDescription describeTopic(String name) throws IOException {
        checkOpen();
        refreshMetadata();

        ClusterMetadata meta = clusterMetadata;
        if (meta == null) {
            throw new IOException("No metadata available");
        }

        ClusterMetadata.TopicMeta tm = meta.topics.get(name);
        if (tm == null) {
            throw new IOException("Topic not found: " + name);
        }

        List<PartitionDetail> parts = new ArrayList<>();
        short rf = 1;
        if (!tm.partitions.isEmpty()) {
            rf = (short) tm.partitions.get(0).replicas.size();
            for (ClusterMetadata.PartitionMeta pm : tm.partitions) {
                parts.add(new PartitionDetail(pm.id, pm.leaderId, pm.replicas, pm.isr, 0, 0));
            }
        }

        return new TopicDescription(name, tm.internal, tm.partitions.size(), rf, parts,
                                     Collections.emptyList());
    }

    // -----------------------------------------------------------------------
    // Consumer group management
    // -----------------------------------------------------------------------

    /**
     * Lists all consumer groups in the cluster.
     *
     * @return list of consumer group summaries
     * @throws IOException on failure
     */
    public List<ConsumerGroupSummary> listConsumerGroups() throws IOException {
        checkOpen();

        byte[] body = encodeListGroupsRequest();
        Protocol.ResponseFraming resp = sendRequest(Protocol.API_LIST_GROUPS, (short) 0, body);

        return decodeListGroupsResponse(resp.body);
    }

    /**
     * Returns detailed information about a consumer group.
     *
     * @param groupId group identifier
     * @return group description
     * @throws IOException if group not found
     */
    public ConsumerGroupDescription describeConsumerGroup(String groupId) throws IOException {
        checkOpen();

        byte[] body = encodeDescribeGroupsRequest(List.of(groupId));
        Protocol.ResponseFraming resp = sendRequest(Protocol.API_DESCRIBE_GROUPS, (short) 0, body);

        Map<String, ConsumerGroupDescription> results = decodeDescribeGroupsResponse(resp.body);
        ConsumerGroupDescription desc = results.get(groupId);
        if (desc == null) {
            throw new IOException("Consumer group not found: " + groupId);
        }
        return desc;
    }

    /**
     * Deletes a consumer group.
     *
     * @param groupId group identifier
     * @throws IOException on failure
     */
    public void deleteConsumerGroup(String groupId) throws IOException {
        checkOpen();

        byte[] body = encodeDeleteGroupsRequest(List.of(groupId));
        Protocol.ResponseFraming resp = sendRequest(Protocol.API_DELETE_GROUPS, (short) 0, body);

        Map<String, Short> results = decodeDeleteGroupsResponse(resp.body);
        Short errorCode = results.get(groupId);
        if (errorCode == null) {
            return; // idempotent
        }
        if (errorCode != Protocol.ERR_NONE) {
            throw new IOException("Delete group failed: " + Protocol.errorCodeDescription(errorCode));
        }
    }

    // -----------------------------------------------------------------------
    // Cluster information
    // -----------------------------------------------------------------------

    /**
     * Returns cluster-level metadata.
     *
     * @return cluster description
     * @throws IOException on failure
     */
    public ClusterDescription describeCluster() throws IOException {
        checkOpen();
        refreshMetadata();

        ClusterMetadata meta = clusterMetadata;
        if (meta == null) {
            throw new IOException("No metadata available");
        }

        List<BrokerDetail> brokers = new ArrayList<>();
        for (ClusterMetadata.BrokerMeta bm : meta.brokers.values()) {
            brokers.add(new BrokerDetail(bm.id, bm.host, bm.port, bm.rack));
        }

        return new ClusterDescription(meta.clusterId, meta.controllerId, brokers,
                                       new ArrayList<>(meta.topics.keySet()));
    }

    // -----------------------------------------------------------------------
    // Close
    // -----------------------------------------------------------------------

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        networkClient.close();
    }

    private void checkOpen() {
        if (closed.get()) throw new IllegalStateException("Admin client is closed");
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private Protocol.ResponseFraming sendRequest(short apiKey, short version, byte[] body)
        throws IOException {
        int corrId = Protocol.nextCorrelationId();
        byte[] request = Protocol.buildRequest(apiKey, version, corrId, clientId, body);

        CompletableFuture<Protocol.ResponseFraming> future =
            networkClient.send(bootstrapHost, bootstrapPort, corrId, request);

        try {
            return future.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IOException("Request failed: " + apiKey, e);
        }
    }

    private void refreshMetadata() throws IOException {
        byte[] body = encodeMetadataRequest(null);
        Protocol.ResponseFraming resp = sendRequest(Protocol.API_METADATA, (short) 4, body);

        ClusterMetadata meta = decodeMetadataResponse(resp.body);
        synchronized (metadataLock) {
            clusterMetadata = meta;
        }
    }

    // -----------------------------------------------------------------------
    // Metadata model (shared shape with TorrentProducer)
    // -----------------------------------------------------------------------

    static final class ClusterMetadata {
        final Map<String, TopicMeta> topics    = new HashMap<>();
        final Map<Integer, BrokerMeta> brokers = new HashMap<>();
        int controllerId = -1;
        String clusterId = "";

        record TopicMeta(String name, boolean internal, List<PartitionMeta> partitions) {}
        record PartitionMeta(int id, int leaderId, List<Integer> replicas, List<Integer> isr) {}
        record BrokerMeta(int id, String host, int port, String rack) {}
    }

    private record TopicResult(short errorCode, String errorMessage) {}
    private record CreateTopicSpec(String name, int numPartitions, short replicationFactor,
                                    Map<String, String> configs) {}

    // -- Metadata request/response --

    static byte[] encodeMetadataRequest(List<String> topics) {
        ByteBuffer buf = ByteBuffer.allocate(256);
        if (topics == null) {
            buf.putInt(-1);
        } else {
            buf.putInt(topics.size());
            for (String t : topics) Protocol.writeString(buf, t);
        }
        buf.put((byte) 0);
        byte[] out = new byte[buf.position()];
        buf.flip(); buf.get(out);
        return out;
    }

    static ClusterMetadata decodeMetadataResponse(byte[] body) {
        ByteBuffer buf = ByteBuffer.wrap(body);
        ClusterMetadata meta = new ClusterMetadata();

        int brokerCount = buf.getInt();
        for (int i = 0; i < brokerCount; i++) {
            int id   = buf.getInt();
            String host = Protocol.readString(buf);
            int port   = buf.getInt();
            String rack = Protocol.readNullableString(buf);
            meta.brokers.put(id, new ClusterMetadata.BrokerMeta(id, host, port, rack));
        }

        meta.clusterId    = Protocol.readNullableString(buf);
        meta.controllerId = buf.getInt();

        int topicCount = buf.getInt();
        for (int i = 0; i < topicCount; i++) {
            buf.getShort(); // error_code
            String name = Protocol.readString(buf);
            boolean internal = buf.get() != 0;

            int partCount = buf.getInt();
            List<ClusterMetadata.PartitionMeta> parts = new ArrayList<>(partCount);
            for (int j = 0; j < partCount; j++) {
                buf.getShort(); // partition error
                int pId    = buf.getInt();
                int leader = buf.getInt();
                int repCount = buf.getInt();
                List<Integer> replicas = new ArrayList<>(repCount);
                for (int k = 0; k < repCount; k++) replicas.add(buf.getInt());
                int isrCount = buf.getInt();
                List<Integer> isr = new ArrayList<>(isrCount);
                for (int k = 0; k < isrCount; k++) isr.add(buf.getInt());
                parts.add(new ClusterMetadata.PartitionMeta(pId, leader, replicas, isr));
            }
            meta.topics.put(name, new ClusterMetadata.TopicMeta(name, internal, parts));
        }
        return meta;
    }

    // -- CreateTopics --

    static byte[] encodeCreateTopicsRequest(List<CreateTopicSpec> specs, int timeoutMs) {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        buf.putInt(specs.size());
        for (CreateTopicSpec s : specs) {
            Protocol.writeString(buf, s.name);
            buf.putInt(s.numPartitions);
            buf.putShort(s.replicationFactor);

            // replica assignment: empty
            buf.putInt(0);

            // configs
            Map<String, String> cfgs = s.configs != null ? s.configs : Collections.emptyMap();
            buf.putInt(cfgs.size());
            for (var e : cfgs.entrySet()) {
                Protocol.writeString(buf, e.getKey());
                Protocol.writeNullableString(buf, e.getValue());
            }

            buf.putInt(timeoutMs);
        }
        buf.put((byte) 0); // validate_only = false
        byte[] out = new byte[buf.position()];
        buf.flip(); buf.get(out);
        return out;
    }

    static Map<String, TopicResult> decodeCreateTopicsResponse(byte[] body) {
        ByteBuffer buf = ByteBuffer.wrap(body);
        Map<String, TopicResult> results = new HashMap<>();

        int count = buf.getInt();
        for (int i = 0; i < count; i++) {
            String name = Protocol.readString(buf);
            short err   = buf.getShort();
            String msg  = Protocol.readNullableString(buf);
            results.put(name, new TopicResult(err, msg));
        }
        return results;
    }

    // -- DeleteTopics --

    static byte[] encodeDeleteTopicsRequest(List<String> topics, int timeoutMs) {
        ByteBuffer buf = ByteBuffer.allocate(512);
        buf.putInt(topics.size());
        for (String t : topics) Protocol.writeString(buf, t);
        buf.putInt(timeoutMs);
        byte[] out = new byte[buf.position()];
        buf.flip(); buf.get(out);
        return out;
    }

    static Map<String, TopicResult> decodeDeleteTopicsResponse(byte[] body) {
        // Same format as create topics response in many Kafka versions
        return decodeCreateTopicsResponse(body);
    }

    // -- ListGroups --

    static byte[] encodeListGroupsRequest() {
        return new byte[0]; // empty body for v0
    }

    static List<ConsumerGroupSummary> decodeListGroupsResponse(byte[] body) {
        ByteBuffer buf = ByteBuffer.wrap(body);
        short err = buf.getShort();
        int count = buf.getInt();
        List<ConsumerGroupSummary> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String groupId   = Protocol.readString(buf);
            String protoType = Protocol.readString(buf);
            String state     = Protocol.readString(buf);
            result.add(new ConsumerGroupSummary(groupId, protoType, state));
        }
        return result;
    }

    // -- DescribeGroups --

    static byte[] encodeDescribeGroupsRequest(List<String> groupIds) {
        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(groupIds.size());
        for (String g : groupIds) Protocol.writeString(buf, g);
        byte[] out = new byte[buf.position()];
        buf.flip(); buf.get(out);
        return out;
    }

    static Map<String, ConsumerGroupDescription> decodeDescribeGroupsResponse(byte[] body) {
        ByteBuffer buf = ByteBuffer.wrap(body);
        int count = buf.getInt();
        Map<String, ConsumerGroupDescription> results = new HashMap<>();
        for (int i = 0; i < count; i++) {
            short err = buf.getShort();
            String groupId     = Protocol.readString(buf);
            String state       = Protocol.readString(buf);
            String protoType   = Protocol.readString(buf);
            String protocol    = Protocol.readString(buf);
            int memberCount    = buf.getInt();
            List<ConsumerGroupMember> members = new ArrayList<>(memberCount);
            for (int j = 0; j < memberCount; j++) {
                String mId    = Protocol.readString(buf);
                String cId    = Protocol.readString(buf);
                String cHost  = Protocol.readString(buf);
                Protocol.readNullableBytes(buf); // member metadata
                Protocol.readNullableBytes(buf); // member assignment
                members.add(new ConsumerGroupMember(mId, cId, cHost));
            }
            results.put(groupId, new ConsumerGroupDescription(
                groupId, state, protoType, protocol, members, -1));
        }
        return results;
    }

    // -- DeleteGroups --

    static byte[] encodeDeleteGroupsRequest(List<String> groupIds) {
        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(groupIds.size());
        for (String g : groupIds) Protocol.writeString(buf, g);
        byte[] out = new byte[buf.position()];
        buf.flip(); buf.get(out);
        return out;
    }

    static Map<String, Short> decodeDeleteGroupsResponse(byte[] body) {
        ByteBuffer buf = ByteBuffer.wrap(body);
        int count = buf.getInt();
        Map<String, Short> results = new HashMap<>();
        for (int i = 0; i < count; i++) {
            String groupId = Protocol.readString(buf);
            short err      = buf.getShort();
            results.put(groupId, err);
        }
        return results;
    }
}
