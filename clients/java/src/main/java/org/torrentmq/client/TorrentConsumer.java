package org.torrentmq.client;

import org.torrentmq.client.network.NetworkClient;
import org.torrentmq.client.protocol.Protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A consumer that subscribes to topics, participates in a consumer group for
 * partition assignment, polls records, and manages offset commits.
 *
 * <h3>Usage example</h3>
 * <pre>{@code
 * TorrentConsumer consumer = new TorrentConsumer("localhost:9092", "my-group", new TorrentConsumer.Config());
 * consumer.subscribe(List.of("my-topic"));
 * while (running) {
 *     for (ConsumerRecord r : consumer.poll(500)) {
 *         System.out.println(new String(r.value));
 *     }
 * }
 * consumer.close();
 * }</pre>
 */
public final class TorrentConsumer implements AutoCloseable {

    // -----------------------------------------------------------------------
    // Public data types
    // -----------------------------------------------------------------------

    /** A single consumed record with full metadata. */
    public static final class ConsumerRecord {
        public final String topic;
        public final int    partition;
        public final long   offset;
        public final long   timestamp;
        public final byte[] key;
        public final byte[] value;
        public final Map<String, String> headers;

        public ConsumerRecord(String topic, int partition, long offset, long timestamp,
                              byte[] key, byte[] value, Map<String, String> headers) {
            this.topic     = topic;
            this.partition = partition;
            this.offset    = offset;
            this.timestamp = timestamp;
            this.key       = key;
            this.value     = value;
            this.headers   = headers != null ? Collections.unmodifiableMap(headers) : Collections.emptyMap();
        }

        @Override
        public String toString() {
            return "ConsumerRecord{topic=" + topic + ", partition=" + partition
                + ", offset=" + offset + "}";
        }
    }

    /** Offset reset policy when no committed offset exists. */
    public enum OffsetResetPolicy {
        EARLIEST, LATEST, NONE
    }

    // -----------------------------------------------------------------------
    // Consumer configuration
    // -----------------------------------------------------------------------

    public static final class Config {
        /** Consumer group identifier (required). */
        public String groupId = null;

        /** Client identifier sent to the broker. */
        public String clientId = "torrent-consumer";

        /** Session timeout in milliseconds for group membership. */
        public int sessionTimeoutMs = 45_000;

        /** Heartbeat interval in milliseconds. */
        public int heartbeatIntervalMs = 3_000;

        /** Maximum records returned per poll. */
        public int maxPollRecords = 500;

        /** Maximum bytes fetched per partition per request. */
        public int maxPartitionFetchBytes = 1_048_576; // 1 MiB

        /** Maximum time in milliseconds poll() blocks. */
        public int pollTimeoutMs = 500;

        /** Where to start when no committed offset exists. */
        public OffsetResetPolicy autoOffsetReset = OffsetResetPolicy.LATEST;

        /** Enable periodic automatic offset commits. */
        public boolean enableAutoCommit = true;

        /** Auto-commit interval in milliseconds. */
        public int autoCommitIntervalMs = 5_000;

        /** Socket read/write timeout. */
        public int socketTimeoutMs = 60_000;

        /** Connection timeout. */
        public int connectionTimeoutMs = 10_000;

        /** Bootstrap servers. */
        public String bootstrapServers = "localhost:9092";

        public Config() {}

        public Config(Config other) {
            this.groupId               = other.groupId;
            this.clientId              = other.clientId;
            this.sessionTimeoutMs      = other.sessionTimeoutMs;
            this.heartbeatIntervalMs   = other.heartbeatIntervalMs;
            this.maxPollRecords        = other.maxPollRecords;
            this.maxPartitionFetchBytes = other.maxPartitionFetchBytes;
            this.pollTimeoutMs         = other.pollTimeoutMs;
            this.autoOffsetReset       = other.autoOffsetReset;
            this.enableAutoCommit      = other.enableAutoCommit;
            this.autoCommitIntervalMs  = other.autoCommitIntervalMs;
            this.socketTimeoutMs       = other.socketTimeoutMs;
            this.connectionTimeoutMs   = other.connectionTimeoutMs;
            this.bootstrapServers      = other.bootstrapServers;
        }
    }

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------

    private final Config config;
    private final NetworkClient networkClient;
    private final String coordinatorHost;
    private final int    coordinatorPort;
    private final String groupId;

    // Group membership state
    private String memberId     = "";
    private int    generationId = -1;

    // Assigned partitions: topic -> list of partition IDs
    private final Map<String, List<Integer>> assignment = new ConcurrentHashMap<>();

    // Offset tracking
    private final Map<TopicPartition, Long> positions   = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Long> committed   = new ConcurrentHashMap<>();

    // Fetched records queue
    private final BlockingDeque<ConsumerRecord> recordQueue = new LinkedBlockingDeque<>();

    // Lifecycle
    private final AtomicBoolean closed       = new AtomicBoolean(false);
    private final AtomicBoolean subscribed   = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> heartbeatFuture;
    private ScheduledFuture<?> autoCommitFuture;

    // Correlation ID
    private final AtomicLong correlationSeq = new AtomicLong(0);

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    /**
     * Creates a consumer and connects to the bootstrap server.
     *
     * @param bootstrapServers comma-separated host:port addresses
     * @param groupId          consumer group identifier
     * @param config           consumer configuration
     * @throws IOException if connection fails
     */
    public TorrentConsumer(String bootstrapServers, String groupId, Config config) throws IOException {
        this.config  = (config != null) ? new Config(config) : new Config();
        if (bootstrapServers != null && !bootstrapServers.isBlank()) {
            this.config.bootstrapServers = bootstrapServers;
        }
        this.groupId = Objects.requireNonNull(groupId, "groupId is required");
        this.config.groupId = groupId;
        this.config.clientId = this.config.clientId != null ? this.config.clientId : "torrent-consumer";

        // Parse bootstrap address
        String[] parts = this.config.bootstrapServers.split(",");
        String firstAddr = parts[0].trim();
        int colon = firstAddr.lastIndexOf(':');
        if (colon > 0) {
            coordinatorHost = firstAddr.substring(0, colon);
            coordinatorPort = Integer.parseInt(firstAddr.substring(colon + 1));
        } else {
            coordinatorHost = firstAddr;
            coordinatorPort = 9092;
        }

        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "torrentmq-consumer-bg");
            t.setDaemon(true);
            return t;
        });

        NetworkClient.Config netConfig = NetworkClient.Config.builder()
            .connectTimeoutMs(this.config.connectionTimeoutMs)
            .socketTimeoutMs(this.config.socketTimeoutMs)
            .build();
        this.networkClient = new NetworkClient(this.config.clientId, netConfig);

        // Connect
        networkClient.connect(coordinatorHost, coordinatorPort)
            .get(this.config.connectionTimeoutMs, TimeUnit.MILLISECONDS);
    }

    // -----------------------------------------------------------------------
    // Subscribe & group protocol
    // -----------------------------------------------------------------------

    /**
     * Subscribes to the given topics and joins the consumer group.
     *
     * @param topics list of topic names to subscribe to
     * @throws IOException if the group join protocol fails
     */
    public void subscribe(List<String> topics) throws IOException {
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("At least one topic is required");
        }
        if (closed.get()) throw new IllegalStateException("Consumer is closed");

        // Find coordinator (may already be on the coordinator node)
        findCoordinator();

        // Join group
        joinGroup(topics);

        subscribed.set(true);

        // Start heartbeat
        startHeartbeat();

        // Start auto-commit if enabled
        if (config.enableAutoCommit) {
            startAutoCommit();
        }
    }

    /**
     * Finds the group coordinator and reconnects if necessary.
     */
    private void findCoordinator() throws IOException {
        byte[] body = encodeFindCoordinatorRequest(groupId);
        int corrId = Protocol.nextCorrelationId();
        byte[] request = Protocol.buildRequest(Protocol.API_FIND_COORDINATOR, (short) 0,
                                                corrId, config.clientId, body);

        CompletableFuture<Protocol.ResponseFraming> future =
            networkClient.send(coordinatorHost, coordinatorPort, corrId, request);

        try {
            Protocol.ResponseFraming resp = future.get(config.sessionTimeoutMs, TimeUnit.MILLISECONDS);
            CoordinatorInfo info = decodeFindCoordinatorResponse(resp.body);
            if (info.errorCode != Protocol.ERR_NONE) {
                throw new IOException("Find coordinator failed: " + Protocol.errorCodeDescription(info.errorCode));
            }
            // In this simple client we assume the bootstrap server IS the coordinator
        } catch (Exception e) {
            throw new IOException("Find coordinator failed", e);
        }
    }

    /**
     * Performs the JoinGroup + SyncGroup protocol.
     */
    private void joinGroup(List<String> topics) throws IOException {
        // Step 1: JoinGroup
        byte[] joinBody = encodeJoinGroupRequest(groupId, config.sessionTimeoutMs, memberId, topics);
        int corrId1 = Protocol.nextCorrelationId();
        byte[] joinReq = Protocol.buildRequest(Protocol.API_JOIN_GROUP, (short) 2,
                                                corrId1, config.clientId, joinBody);

        Protocol.ResponseFraming joinResp;
        try {
            joinResp = networkClient.send(coordinatorHost, coordinatorPort, corrId1, joinReq)
                .get(config.sessionTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IOException("Join group failed", e);
        }

        JoinGroupResult joinResult = decodeJoinGroupResponse(joinResp.body);
        if (joinResult.errorCode != Protocol.ERR_NONE) {
            if (joinResult.errorCode == Protocol.ERR_REBALANCE_IN_PROGRESS) {
                // Retry after delay
                try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                joinGroup(topics);
                return;
            }
            throw new IOException("Join group error: " + Protocol.errorCodeDescription(joinResult.errorCode));
        }

        this.memberId     = joinResult.memberId;
        this.generationId = joinResult.generationId;

        // Step 2: SyncGroup — send empty assignment (consumer protocol)
        byte[] emptyAssignment = encodeMemberAssignment(Collections.emptyMap());
        byte[] syncBody = encodeSyncGroupRequest(groupId, generationId, memberId, emptyAssignment);
        int corrId2 = Protocol.nextCorrelationId();
        byte[] syncReq = Protocol.buildRequest(Protocol.API_SYNC_GROUP, (short) 2,
                                                corrId2, config.clientId, syncBody);

        Protocol.ResponseFraming syncResp;
        try {
            syncResp = networkClient.send(coordinatorHost, coordinatorPort, corrId2, syncReq)
                .get(config.sessionTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IOException("Sync group failed", e);
        }

        SyncGroupResult syncResult = decodeSyncGroupResponse(syncResp.body);
        if (syncResult.errorCode != Protocol.ERR_NONE) {
            throw new IOException("Sync group error: " + Protocol.errorCodeDescription(syncResult.errorCode));
        }

        // Update assignment
        assignment.clear();
        assignment.putAll(syncResult.assignment);

        // Initialize positions
        for (var entry : syncResult.assignment.entrySet()) {
            for (int part : entry.getValue()) {
                TopicPartition tp = new TopicPartition(entry.getKey(), part);
                positions.putIfAbsent(tp, -1L); // -1 means "fetch latest"
            }
        }
    }

    // -----------------------------------------------------------------------
    // Heartbeat
    // -----------------------------------------------------------------------

    private void startHeartbeat() {
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(false);
        }
        heartbeatFuture = scheduler.scheduleAtFixedRate(() -> {
            try {
                sendHeartbeat();
            } catch (Exception e) {
                // Rejoin on heartbeat failure
                try { joinGroup(new ArrayList<>(assignment.keySet())); } catch (Exception ignored) {}
            }
        }, config.heartbeatIntervalMs, config.heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeat() throws Exception {
        byte[] body = encodeHeartbeatRequest(groupId, generationId, memberId);
        int corrId = Protocol.nextCorrelationId();
        byte[] request = Protocol.buildRequest(Protocol.API_HEARTBEAT, (short) 0,
                                                corrId, config.clientId, body);

        Protocol.ResponseFraming resp = networkClient
            .send(coordinatorHost, coordinatorPort, corrId, request)
            .get(config.socketTimeoutMs, TimeUnit.MILLISECONDS);

        short errCode = decodeHeartbeatResponse(resp.body);
        if (errCode != Protocol.ERR_NONE) {
            // Trigger rejoin
            try { joinGroup(new ArrayList<>(assignment.keySet())); } catch (Exception ignored) {}
        }
    }

    // -----------------------------------------------------------------------
    // Auto-commit
    // -----------------------------------------------------------------------

    private void startAutoCommit() {
        if (autoCommitFuture != null) {
            autoCommitFuture.cancel(false);
        }
        autoCommitFuture = scheduler.scheduleAtFixedRate(() -> {
            try {
                commitSync();
            } catch (Exception ignored) { /* best-effort */ }
        }, config.autoCommitIntervalMs, config.autoCommitIntervalMs, TimeUnit.MILLISECONDS);
    }

    // -----------------------------------------------------------------------
    // Poll
    // -----------------------------------------------------------------------

    /**
     * Polls for records from the subscribed topics.
     *
     * @param timeoutMs maximum time to block in milliseconds
     * @return list of consumer records (may be empty)
     */
    public List<ConsumerRecord> poll(long timeoutMs) {
        if (closed.get()) return Collections.emptyList();
        if (!subscribed.get()) return Collections.emptyList();

        // Fetch from all assigned partitions
        for (var entry : assignment.entrySet()) {
            String topic = entry.getKey();
            for (int partition : entry.getValue()) {
                try {
                    fetchRecords(topic, partition);
                } catch (Exception e) {
                    // Skip this partition on error
                }
            }
        }

        // Drain the queue
        List<ConsumerRecord> result = new ArrayList<>();
        try {
            ConsumerRecord first = recordQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
            if (first != null) {
                result.add(first);
                recordQueue.drainTo(result, config.maxPollRecords - 1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return result;
    }

    private void fetchRecords(String topic, int partition) throws Exception {
        TopicPartition tp = new TopicPartition(topic, partition);
        long fetchOffset = positions.getOrDefault(tp, -1L);
        if (fetchOffset < 0) {
            // Resolve offset
            fetchOffset = resolveOffset(topic, partition);
            positions.put(tp, fetchOffset);
        }

        byte[] body = encodeFetchRequest(topic, partition, fetchOffset,
                                          config.maxPartitionFetchBytes);
        int corrId = Protocol.nextCorrelationId();
        byte[] request = Protocol.buildRequest(Protocol.API_FETCH, (short) 4,
                                                corrId, config.clientId, body);

        Protocol.ResponseFraming resp = networkClient
            .send(coordinatorHost, coordinatorPort, corrId, request)
            .get(config.socketTimeoutMs, TimeUnit.MILLISECONDS);

        List<ConsumerRecord> fetched = decodeFetchResponse(resp.body, topic, partition);
        for (ConsumerRecord r : fetched) {
            recordQueue.add(r);
        }
        if (!fetched.isEmpty()) {
            positions.put(tp, fetched.get(fetched.size() - 1).offset + 1);
        }
    }

    private long resolveOffset(String topic, int partition) throws Exception {
        long timestamp = switch (config.autoOffsetReset) {
            case EARLIEST -> -2L;
            case LATEST   -> -1L;
            case NONE     -> -1L;
        };

        byte[] body = encodeListOffsetsRequest(topic, partition, timestamp);
        int corrId = Protocol.nextCorrelationId();
        byte[] request = Protocol.buildRequest(Protocol.API_LIST_OFFSETS, (short) 2,
                                                corrId, config.clientId, body);

        Protocol.ResponseFraming resp = networkClient
            .send(coordinatorHost, coordinatorPort, corrId, request)
            .get(config.socketTimeoutMs, TimeUnit.MILLISECONDS);

        return decodeListOffsetsResponse(resp.body, topic, partition);
    }

    // -----------------------------------------------------------------------
    // Seek
    // -----------------------------------------------------------------------

    /**
     * Seeks to a specific offset for a topic-partition.
     *
     * @param topic     the topic name
     * @param partition the partition ID
     * @param offset    the offset to seek to
     */
    public void seek(String topic, int partition, long offset) {
        positions.put(new TopicPartition(topic, partition), offset);
    }

    /**
     * Seeks to the beginning of a partition (earliest available offset).
     */
    public void seekToBeginning(String topic, int partition) throws Exception {
        long offset = resolveOffsetWithTimestamp(topic, partition, -2L);
        positions.put(new TopicPartition(topic, partition), offset);
    }

    /**
     * Seeks to the end of a partition (latest available offset).
     */
    public void seekToEnd(String topic, int partition) throws Exception {
        long offset = resolveOffsetWithTimestamp(topic, partition, -1L);
        positions.put(new TopicPartition(topic, partition), offset);
    }

    private long resolveOffsetWithTimestamp(String topic, int partition, long timestamp) throws Exception {
        byte[] body = encodeListOffsetsRequest(topic, partition, timestamp);
        int corrId = Protocol.nextCorrelationId();
        byte[] request = Protocol.buildRequest(Protocol.API_LIST_OFFSETS, (short) 2,
                                                corrId, config.clientId, body);
        Protocol.ResponseFraming resp = networkClient
            .send(coordinatorHost, coordinatorPort, corrId, request)
            .get(config.socketTimeoutMs, TimeUnit.MILLISECONDS);
        return decodeListOffsetsResponse(resp.body, topic, partition);
    }

    // -----------------------------------------------------------------------
    // Offset commit
    // -----------------------------------------------------------------------

    /**
     * Synchronously commits current positions.
     *
     * @throws IOException on commit failure
     */
    public void commitSync() throws IOException {
        commitSync(Duration.ofMillis(config.socketTimeoutMs));
    }

    /**
     * Synchronously commits current positions with a timeout.
     */
    public void commitSync(Duration timeout) throws IOException {
        try {
            doCommit(timeout);
        } catch (Exception e) {
            throw new IOException("Commit failed", e);
        }
    }

    /**
     * Asynchronously commits current positions.
     *
     * @param callback invoked with success/failure result
     */
    public void commitAsync(CommitCallback callback) {
        scheduler.submit(() -> {
            try {
                doCommit(Duration.ofMillis(config.socketTimeoutMs));
                if (callback != null) callback.onComplete(null);
            } catch (Exception e) {
                if (callback != null) callback.onComplete(e);
            }
        });
    }

    /** Functional callback interface for async commits. */
    @FunctionalInterface
    public interface CommitCallback {
        void onComplete(Exception error);
    }

    private void doCommit(Duration timeout) throws Exception {
        Map<TopicPartition, Long> offsets = new HashMap<>(positions);
        if (offsets.isEmpty()) return;

        byte[] body = encodeOffsetCommitRequest(groupId, generationId, memberId, offsets);
        int corrId = Protocol.nextCorrelationId();
        byte[] request = Protocol.buildRequest(Protocol.API_OFFSET_COMMIT, (short) 2,
                                                corrId, config.clientId, body);

        Protocol.ResponseFraming resp = networkClient
            .send(coordinatorHost, coordinatorPort, corrId, request)
            .get(timeout.toMillis(), TimeUnit.MILLISECONDS);

        // Update committed map on success
        committed.putAll(offsets);
    }

    // -----------------------------------------------------------------------
    // Close
    // -----------------------------------------------------------------------

    /**
     * Leaves the consumer group and shuts down the consumer.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;

        if (heartbeatFuture != null) heartbeatFuture.cancel(false);
        if (autoCommitFuture != null) autoCommitFuture.cancel(false);

        // Final commit
        if (config.enableAutoCommit) {
            try { commitSync(Duration.ofSeconds(5)); } catch (Exception ignored) {}
        }

        // Leave group
        try {
            byte[] body = encodeLeaveGroupRequest(groupId, memberId);
            int corrId = Protocol.nextCorrelationId();
            byte[] request = Protocol.buildRequest(Protocol.API_LEAVE_GROUP, (short) 0,
                                                    corrId, config.clientId, body);
            networkClient.send(coordinatorHost, coordinatorPort, corrId, request)
                .get(5, TimeUnit.SECONDS);
        } catch (Exception ignored) {}

        scheduler.shutdown();
        try { scheduler.awaitTermination(5, TimeUnit.SECONDS); } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        networkClient.close();
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private record TopicPartition(String topic, int partition) {}

    private record CoordinatorInfo(short errorCode, String host, int port) {}
    private record JoinGroupResult(short errorCode, int generationId, String memberId,
                                    String protocolName, String leaderId) {}
    private record SyncGroupResult(short errorCode, Map<String, List<Integer>> assignment) {}

    // -- Encoding helpers --

    static byte[] encodeFindCoordinatorRequest(String groupId) {
        ByteBuffer buf = ByteBuffer.allocate(256);
        Protocol.writeString(buf, groupId);
        byte[] out = new byte[buf.position()];
        buf.flip(); buf.get(out);
        return out;
    }

    static CoordinatorInfo decodeFindCoordinatorResponse(byte[] body) {
        ByteBuffer buf = ByteBuffer.wrap(body);
        short err = buf.getShort();
        // Skip coordinator id, host, port (simplified)
        return new CoordinatorInfo(err, "", 0);
    }

    static byte[] encodeJoinGroupRequest(String groupId, int sessionTimeoutMs,
                                          String memberId, List<String> topics) {
        ByteBuffer buf = ByteBuffer.allocate(512);
        Protocol.writeString(buf, groupId);
        buf.putInt(sessionTimeoutMs);
        buf.putInt(sessionTimeoutMs); // rebalance_timeout_ms
        Protocol.writeString(buf, memberId);
        Protocol.writeString(buf, "consumer"); // protocol type

        // One protocol: "range"
        buf.putInt(1);
        Protocol.writeString(buf, "range");
        // Protocol metadata: list of subscribed topics
        ByteBuffer meta = ByteBuffer.allocate(256);
        meta.putInt(topics.size());
        for (String t : topics) Protocol.writeString(meta, t);
        byte[] metaBytes = new byte[meta.position()];
        meta.flip(); meta.get(metaBytes);
        Protocol.writeNullableBytes(buf, metaBytes);

        byte[] out = new byte[buf.position()];
        buf.flip(); buf.get(out);
        return out;
    }

    static JoinGroupResult decodeJoinGroupResponse(byte[] body) {
        ByteBuffer buf = ByteBuffer.wrap(body);
        short err         = buf.getShort();
        int genId         = buf.getInt();
        String protocol   = Protocol.readString(buf);
        String leader     = Protocol.readString(buf);
        String member     = Protocol.readString(buf);

        // skip member list
        int memberCount = buf.getInt();
        for (int i = 0; i < memberCount; i++) {
            Protocol.readString(buf); // member_id
            Protocol.readNullableBytes(buf); // metadata
        }

        return new JoinGroupResult(err, genId, member, protocol, leader);
    }

    static byte[] encodeSyncGroupRequest(String groupId, int generationId,
                                          String memberId, byte[] assignment) {
        ByteBuffer buf = ByteBuffer.allocate(256 + assignment.length);
        Protocol.writeString(buf, groupId);
        buf.putInt(generationId);
        Protocol.writeString(buf, memberId);
        Protocol.writeNullableBytes(buf, assignment);
        byte[] out = new byte[buf.position()];
        buf.flip(); buf.get(out);
        return out;
    }

    static SyncGroupResult decodeSyncGroupResponse(byte[] body) {
        ByteBuffer buf = ByteBuffer.wrap(body);
        short err = buf.getShort();
        byte[] assignmentBytes = Protocol.readNullableBytes(buf);
        Map<String, List<Integer>> assignment = Collections.emptyMap();
        if (assignmentBytes != null && assignmentBytes.length > 0) {
            assignment = decodeMemberAssignment(ByteBuffer.wrap(assignmentBytes));
        }
        return new SyncGroupResult(err, assignment);
    }

    static byte[] encodeHeartbeatRequest(String groupId, int generationId, String memberId) {
        ByteBuffer buf = ByteBuffer.allocate(256);
        Protocol.writeString(buf, groupId);
        buf.putInt(generationId);
        Protocol.writeString(buf, memberId);
        byte[] out = new byte[buf.position()];
        buf.flip(); buf.get(out);
        return out;
    }

    static short decodeHeartbeatResponse(byte[] body) {
        return ByteBuffer.wrap(body).getShort();
    }

    static byte[] encodeLeaveGroupRequest(String groupId, String memberId) {
        ByteBuffer buf = ByteBuffer.allocate(256);
        Protocol.writeString(buf, groupId);
        Protocol.writeString(buf, memberId);
        byte[] out = new byte[buf.position()];
        buf.flip(); buf.get(out);
        return out;
    }

    /**
     * Encodes a member assignment for the SyncGroup protocol.
     * Format: [topic_count] [topic: string] [partition_count] [partition: int32] ... ]
     */
    static byte[] encodeMemberAssignment(Map<String, List<Integer>> assignment) {
        ByteBuffer buf = ByteBuffer.allocate(512);
        buf.putInt(assignment.size());
        for (var entry : assignment.entrySet()) {
            Protocol.writeString(buf, entry.getKey());
            buf.putInt(entry.getValue().size());
            for (int p : entry.getValue()) buf.putInt(p);
        }
        byte[] out = new byte[buf.position()];
        buf.flip(); buf.get(out);
        return out;
    }

    /** Decodes the member assignment from SyncGroup response bytes. */
    static Map<String, List<Integer>> decodeMemberAssignment(ByteBuffer buf) {
        Map<String, List<Integer>> result = new HashMap<>();
        int topicCount = buf.getInt();
        for (int i = 0; i < topicCount; i++) {
            String topic = Protocol.readString(buf);
            int partCount = buf.getInt();
            List<Integer> parts = new ArrayList<>(partCount);
            for (int j = 0; j < partCount; j++) parts.add(buf.getInt());
            result.put(topic, parts);
        }
        return result;
    }

    // -- Fetch encoding --

    static byte[] encodeFetchRequest(String topic, int partition, long fetchOffset,
                                      int maxBytes) {
        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(-1); // replica_id
        buf.putInt(0);  // max_wait_ms
        buf.putInt(0);  // min_bytes

        buf.putInt(1);  // topic count
        Protocol.writeString(buf, topic);

        buf.putInt(1);  // partition count
        buf.putInt(partition);
        buf.putLong(fetchOffset);
        buf.putLong(0); // log_start_offset
        buf.putInt(maxBytes);

        byte[] out = new byte[buf.position()];
        buf.flip(); buf.get(out);
        return out;
    }

    static List<ConsumerRecord> decodeFetchResponse(byte[] body, String topic, int partition)
        throws Protocol.ProtocolException {
        ByteBuffer buf = ByteBuffer.wrap(body);

        buf.getInt(); // throttle_time_ms
        short errTop = buf.getShort();
        int sessionId = buf.getInt();

        List<ConsumerRecord> result = new ArrayList<>();
        int topicCount = buf.getInt();
        for (int i = 0; i < topicCount; i++) {
            String t = Protocol.readString(buf);
            int partCount = buf.getInt();
            for (int j = 0; j < partCount; j++) {
                int p = buf.getInt();
                short pErr = buf.getShort();
                long highWatermark = buf.getLong();
                long lastStableOffset = buf.getLong();
                long logStartOffset = buf.getLong();
                // Aborted transactions
                int abortedCount = buf.getInt();
                for (int k = 0; k < abortedCount; k++) {
                    buf.getLong(); buf.getLong(); // skip
                }

                byte[] recordSet = Protocol.readNullableBytes(buf);
                if (recordSet != null && recordSet.length > 0 && t.equals(topic) && p == partition) {
                    Protocol.RecordBatch batch = Protocol.decodeRecordBatch(recordSet, 0);
                    for (Protocol.Record rec : batch.records) {
                        Map<String, String> headers = new HashMap<>();
                        if (rec.headers != null) {
                            for (Protocol.RecordHeader h : rec.headers) {
                                headers.put(h.key(), h.value() != null
                                    ? new String(h.value(), StandardCharsets.UTF_8) : null);
                            }
                        }
                        result.add(new ConsumerRecord(
                            t, p,
                            batch.baseOffset + rec.offsetDelta,
                            batch.firstTimestamp + rec.timestampDelta,
                            rec.key, rec.value, headers));
                    }
                }
            }
        }
        return result;
    }

    // -- ListOffsets --

    static byte[] encodeListOffsetsRequest(String topic, int partition, long timestamp) {
        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(-1); // replica_id

        buf.putInt(1); // topic count
        Protocol.writeString(buf, topic);

        buf.putInt(1); // partition count
        buf.putInt(partition);
        buf.putLong(timestamp);

        byte[] out = new byte[buf.position()];
        buf.flip(); buf.get(out);
        return out;
    }

    static long decodeListOffsetsResponse(byte[] body, String topic, int partition)
        throws Protocol.ProtocolException {
        ByteBuffer buf = ByteBuffer.wrap(body);
        buf.getInt(); // throttle_time_ms

        int topicCount = buf.getInt();
        for (int i = 0; i < topicCount; i++) {
            String t = Protocol.readString(buf);
            int partCount = buf.getInt();
            for (int j = 0; j < partCount; j++) {
                int p = buf.getInt();
                short err = buf.getShort();
                long offset = buf.getLong();
                if (t.equals(topic) && p == partition) {
                    if (err != Protocol.ERR_NONE) {
                        throw new Protocol.ProtocolException("ListOffsets error: " + err);
                    }
                    return offset;
                }
            }
        }
        throw new Protocol.ProtocolException("Partition not found in ListOffsets response");
    }

    // -- OffsetCommit --

    static byte[] encodeOffsetCommitRequest(String groupId, int generationId,
                                             String memberId,
                                             Map<TopicPartition, Long> offsets) {
        ByteBuffer buf = ByteBuffer.allocate(512);
        Protocol.writeString(buf, groupId);
        buf.putInt(generationId);
        Protocol.writeString(buf, memberId);

        // Group offsets by topic
        Map<String, Map<Integer, Long>> byTopic = new HashMap<>();
        for (var e : offsets.entrySet()) {
            byTopic.computeIfAbsent(e.getKey().topic, k -> new HashMap<>())
                    .put(e.getKey().partition, e.getValue());
        }

        buf.putInt(byTopic.size());
        for (var tEntry : byTopic.entrySet()) {
            Protocol.writeString(buf, tEntry.getKey());
            buf.putInt(tEntry.getValue().size());
            for (var pEntry : tEntry.getValue().entrySet()) {
                buf.putInt(pEntry.getKey());
                buf.putLong(pEntry.getValue());
                Protocol.writeNullableString(buf, ""); // metadata
            }
        }

        byte[] out = new byte[buf.position()];
        buf.flip(); buf.get(out);
        return out;
    }

    // Simple Duration class for this file (avoids importing java.time in case of module restrictions)
    private record Duration(long millis) {
        static Duration ofMillis(long ms) { return new Duration(ms); }
        static Duration ofSeconds(long s) { return new Duration(s * 1000); }
        long toMillis() { return millis; }
    }
}
