package org.torrentmq.client;

import org.torrentmq.client.network.NetworkClient;
import org.torrentmq.client.protocol.Protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An asynchronous, high-throughput message producer for torrent-mq.
 *
 * <p>Records sent via {@link #send(String, byte[], byte[], Map)} are accumulated
 * into per-partition batches by an internal {@link RecordAccumulator}. A
 * background sender thread drains ready batches and transmits them to the
 * broker using the Kafka-compatible wire protocol.</p>
 *
 * <h3>Usage example</h3>
 * <pre>{@code
 * TorrentProducer producer = new TorrentProducer("localhost:9092", new TorrentProducer.Config());
 * Future<RecordMetadata> future = producer.send("my-topic", "key".getBytes(), "value".getBytes());
 * RecordMetadata meta = future.get();
 * producer.close();
 * }</pre>
 */
public final class TorrentProducer implements AutoCloseable {

    // -----------------------------------------------------------------------
    // Public data types
    // -----------------------------------------------------------------------

    /** Metadata returned after a record has been successfully produced. */
    public static final class RecordMetadata {
        public final String topic;
        public final int    partition;
        public final long   offset;
        public final long   timestamp;

        public RecordMetadata(String topic, int partition, long offset, long timestamp) {
            this.topic     = topic;
            this.partition = partition;
            this.offset    = offset;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "RecordMetadata{topic=" + topic + ", partition=" + partition
                + ", offset=" + offset + ", timestamp=" + timestamp + "}";
        }
    }

    // -----------------------------------------------------------------------
    // Producer configuration
    // -----------------------------------------------------------------------

    /**
     * Producer configuration with sensible defaults matching Kafka producer conventions.
     */
    public static final class Config {
        /** Comma-separated list of bootstrap broker addresses (host:port). */
        public String bootstrapServers = "localhost:9092";

        /**
         * Required acknowledgments:
         * <ul>
         *   <li>0 — fire-and-forget (no ack)</li>
         *   <li>1 — leader acknowledge only</li>
         *   <li>-1 (or "all") — all in-sync replicas</li>
         * </ul>
         */
        public short acks = 1;

        /** Compression codec: "none", "gzip", "snappy", "lz4", "zstd". */
        public String compressionType = "none";

        /** Maximum batch size in bytes before a batch is sent. */
        public int batchSize = 16384;

        /** Maximum time in milliseconds to wait before sending a partial batch. */
        public int lingerMs = 5;

        /** Maximum number of retry attempts for transient failures. */
        public int maxRetries = 3;

        /** Base backoff in milliseconds between retries (exponential). */
        public int retryBackoffMs = 100;

        /** Maximum time to wait for a broker response, in milliseconds. */
        public int requestTimeoutMs = 30_000;

        /** Maximum in-flight requests per broker connection. */
        public int maxInFlight = 5;

        /** Total buffer memory in bytes for unsent record batches. */
        public long bufferMemory = 33_554_432L; // 32 MiB

        /** Socket read/write timeout in milliseconds. */
        public int socketTimeoutMs = 60_000;

        /** TCP connection timeout in milliseconds. */
        public int connectionTimeoutMs = 10_000;

        /** Client identifier sent to the broker. */
        public String clientId = "torrent-producer";

        /** Default constructor. */
        public Config() {}

        /** Copy constructor. */
        public Config(Config other) {
            this.bootstrapServers    = other.bootstrapServers;
            this.acks                = other.acks;
            this.compressionType     = other.compressionType;
            this.batchSize           = other.batchSize;
            this.lingerMs            = other.lingerMs;
            this.maxRetries          = other.maxRetries;
            this.retryBackoffMs      = other.retryBackoffMs;
            this.requestTimeoutMs    = other.requestTimeoutMs;
            this.maxInFlight         = other.maxInFlight;
            this.bufferMemory        = other.bufferMemory;
            this.socketTimeoutMs     = other.socketTimeoutMs;
            this.connectionTimeoutMs = other.connectionTimeoutMs;
            this.clientId            = other.clientId;
        }
    }

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------

    private final Config config;
    private final NetworkClient networkClient;
    private final String bootstrapHost;
    private final int    bootstrapPort;

    // Metadata
    private volatile ClusterMetadata clusterMetadata;
    private final Object metadataLock = new Object();

    // Record accumulator
    private final RecordAccumulator accumulator;

    // Thread pool
    private final ScheduledExecutorService scheduler;
    private final ExecutorService senderExecutor;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // In-flight state
    private final AtomicLong bufferMemoryUsed = new AtomicLong(0);

    // Correlation ID counter
    private final AtomicLong correlationSeq = new AtomicLong(0);

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    /**
     * Creates a producer connected to the given bootstrap server.
     *
     * @param bootstrapServers comma-separated list of host:port addresses
     * @param config           producer configuration
     * @throws IOException if unable to connect or fetch metadata
     */
    public TorrentProducer(String bootstrapServers, Config config) throws IOException {
        this.config = (config != null) ? new Config(config) : new Config();
        if (bootstrapServers != null && !bootstrapServers.isBlank()) {
            this.config.bootstrapServers = bootstrapServers;
        }

        // Parse first bootstrap address
        String[] parts = this.config.bootstrapServers.split(",");
        String firstAddr = parts[0].trim();
        int colon = firstAddr.lastIndexOf(':');
        if (colon > 0) {
            this.bootstrapHost = firstAddr.substring(0, colon);
            this.bootstrapPort = Integer.parseInt(firstAddr.substring(colon + 1));
        } else {
            this.bootstrapHost = firstAddr;
            this.bootstrapPort = 9092;
        }

        this.scheduler      = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "torrentmq-producer-scheduler");
            t.setDaemon(true);
            return t;
        });
        this.senderExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "torrentmq-producer-sender");
            t.setDaemon(true);
            return t;
        });

        // Build network client
        NetworkClient.Config netConfig = NetworkClient.Config.builder()
            .connectTimeoutMs(this.config.connectionTimeoutMs)
            .socketTimeoutMs(this.config.socketTimeoutMs)
            .build();
        this.networkClient = new NetworkClient(this.config.clientId, netConfig);

        // Connect and fetch metadata
        networkClient.connect(bootstrapHost, bootstrapPort)
            .get(this.config.connectionTimeoutMs, TimeUnit.MILLISECONDS);
        refreshMetadata();

        // Start accumulator and sender
        this.accumulator = new RecordAccumulator(
            this.config.batchSize, this.config.lingerMs, this.config.bufferMemory);
        senderExecutor.submit(new SenderLoop());
    }

    // -----------------------------------------------------------------------
    // Public API: send
    // -----------------------------------------------------------------------

    /**
     * Asynchronously sends a record to the given topic.
     *
     * @param topic the destination topic
     * @param key   optional message key (may be null)
     * @param value message payload
     * @return a Future that completes with RecordMetadata on success
     */
    public CompletableFuture<RecordMetadata> send(String topic, byte[] key, byte[] value) {
        return send(topic, key, value, null);
    }

    /**
     * Asynchronously sends a record with headers.
     *
     * @param topic   the destination topic
     * @param key     optional message key (may be null)
     * @param value   message payload
     * @param headers optional headers (may be null)
     * @return a Future that completes with RecordMetadata on success
     */
    public CompletableFuture<RecordMetadata> send(String topic, byte[] key, byte[] value,
                                                   Map<String, String> headers) {
        if (closed.get()) {
            CompletableFuture<RecordMetadata> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalStateException("Producer is closed"));
            return f;
        }
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(value, "value must not be null");

        int partition = computePartition(topic, key);
        return accumulator.append(topic, partition, key, value, headers);
    }

    /**
     * Convenience method: send with topic and value only (null key, no headers).
     *
     * @param topic the destination topic
     * @param value message payload
     * @return a Future that completes with RecordMetadata on success
     */
    public CompletableFuture<RecordMetadata> send(String topic, byte[] value) {
        return send(topic, null, value, null);
    }

    /**
     * Blocks until all accumulated records have been sent and acknowledged.
     */
    public void flush() {
        accumulator.flushAll();
        // Wait for accumulator to drain
        while (accumulator.totalPending() > 0) {
            try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
    }

    /**
     * Gracefully shuts down the producer, flushing pending records first.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;

        flush();

        scheduler.shutdown();
        senderExecutor.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            senderExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        networkClient.close();
    }

    // -----------------------------------------------------------------------
    // Partition assignment — Murmur2 hash
    // -----------------------------------------------------------------------

    private int computePartition(String topic, byte[] key) {
        ClusterMetadata meta = clusterMetadata;
        int numPartitions = 1;
        if (meta != null) {
            ClusterMetadata.TopicMeta tm = meta.topics.get(topic);
            if (tm != null && !tm.partitions.isEmpty()) {
                numPartitions = tm.partitions.size();
            }
        }
        if (key == null) {
            // Round-robin if no key
            return (int) (Math.abs(correlationSeq.incrementAndGet()) % numPartitions);
        }
        return murmur2(key) % numPartitions;
    }

    /** Kafka-compatible Murmur2 hash for partition assignment. */
    static int murmur2(byte[] data) {
        int length = data.length;
        int seed   = 0x9747b28c;
        int m      = 0x5bd1e995;
        int r      = 24;

        int h = seed ^ length;
        int i = 0;
        while (length >= 4) {
            int k = (data[i] & 0xFF)
                  | ((data[i + 1] & 0xFF) << 8)
                  | ((data[i + 2] & 0xFF) << 16)
                  | ((data[i + 3] & 0xFF) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
            i   += 4;
            length -= 4;
        }
        switch (length) {
            case 3: h ^= (data[i + 2] & 0xFF) << 16;
            case 2: h ^= (data[i + 1] & 0xFF) << 8;
            case 1: h ^= (data[i] & 0xFF);
                    h *= m;
        }
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h & 0x7FFFFFFF;
    }

    // -----------------------------------------------------------------------
    // Metadata
    // -----------------------------------------------------------------------

    private void refreshMetadata() throws IOException {
        byte[] body = encodeMetadataRequest(null);
        int corrId = Protocol.nextCorrelationId();
        byte[] request = Protocol.buildRequest(Protocol.API_METADATA, (short) 4, corrId,
                                                config.clientId, body);

        CompletableFuture<Protocol.ResponseFraming> future =
            networkClient.send(bootstrapHost, bootstrapPort, corrId, request);

        try {
            Protocol.ResponseFraming resp = future.get(config.requestTimeoutMs, TimeUnit.MILLISECONDS);
            ClusterMetadata meta = decodeMetadataResponse(resp.body);
            synchronized (metadataLock) {
                clusterMetadata = meta;
            }
        } catch (Exception e) {
            throw new IOException("Failed to fetch metadata", e);
        }
    }

    // =======================================================================
    // Record Accumulator
    // =======================================================================

    /**
     * Internal per-partition batching buffer. Records are appended until a
     * batch reaches {@code batchSize} bytes or {@code lingerMs} elapses, at
     * which point the batch is marked ready for the sender thread.
     */
    static final class RecordAccumulator {
        private final int batchSize;
        private final int lingerMs;
        private final long maxBufferMemory;

        // topic -> (partition -> deque of batches)
        private final Map<String, Map<Integer, Deque<ProducerBatch>>> batches;
        private final AtomicLong bufferUsed = new AtomicLong(0);

        RecordAccumulator(int batchSize, int lingerMs, long maxBufferMemory) {
            this.batchSize       = batchSize;
            this.lingerMs        = lingerMs;
            this.maxBufferMemory = maxBufferMemory;
            this.batches         = new ConcurrentHashMap<>();
        }

        /** Appends a record, returns a future for the eventual response. */
        CompletableFuture<TorrentProducer.RecordMetadata> append(
            String topic, int partition, byte[] key, byte[] value, Map<String, String> headers) {

            ProducerBatch batch = getOrCreateBatch(topic, partition);
            long timestamp = System.currentTimeMillis();
            CompletableFuture<TorrentProducer.RecordMetadata> future = new CompletableFuture<>();

            synchronized (batch) {
                batch.records.add(new EnqueuedRecord(key, value, headers, timestamp, future));
                batch.estimatedBytes += estimatedRecordSize(key, value, headers);
                bufferUsed.addAndGet(batch.estimatedBytes);

                if (batch.estimatedBytes >= batchSize) {
                    batch.ready = true;
                }
            }
            return future;
        }

        private ProducerBatch getOrCreateBatch(String topic, int partition) {
            Map<Integer, Deque<ProducerBatch>> parts = batches.computeIfAbsent(topic, k -> new ConcurrentHashMap<>());
            Deque<ProducerBatch> dq = parts.computeIfAbsent(partition, k -> new ArrayDeque<>());
            synchronized (dq) {
                ProducerBatch last = dq.peekLast();
                if (last != null && !last.ready) {
                    return last;
                }
                ProducerBatch batch = new ProducerBatch(topic, partition);
                dq.addLast(batch);
                return batch;
            }
        }

        /**
         * Drains all ready batches (and linger-expired batches) across all partitions.
         * Called by the sender thread.
         */
        List<ProducerBatch> drainReady() {
            List<ProducerBatch> ready = new ArrayList<>();
            long now = System.currentTimeMillis();
            for (Map<Integer, Deque<ProducerBatch>> parts : batches.values()) {
                for (Deque<ProducerBatch> dq : parts.values()) {
                    synchronized (dq) {
                        Iterator<ProducerBatch> it = dq.iterator();
                        while (it.hasNext()) {
                            ProducerBatch batch = it.next();
                            if (batch.ready || (now - batch.createdAt >= lingerMs)) {
                                batch.ready = true;
                                it.remove();
                                ready.add(batch);
                            }
                        }
                    }
                }
            }
            return ready;
        }

        /** Force all batches to be ready and drain them. */
        void flushAll() {
            for (Map<Integer, Deque<ProducerBatch>> parts : batches.values()) {
                for (Deque<ProducerBatch> dq : parts.values()) {
                    synchronized (dq) {
                        for (ProducerBatch batch : dq) {
                            batch.ready = true;
                        }
                    }
                }
            }
        }

        /** Total number of batches still in the accumulator. */
        int totalPending() {
            int count = 0;
            for (Map<Integer, Deque<ProducerBatch>> parts : batches.values()) {
                for (Deque<ProducerBatch> dq : parts.values()) {
                    synchronized (dq) { count += dq.size(); }
                }
            }
            return count;
        }

        private int estimatedRecordSize(byte[] key, byte[] value, Map<String, String> headers) {
            int size = 20; // fixed overhead
            if (key != null)   size += key.length;
            if (value != null) size += value.length;
            if (headers != null) {
                for (var e : headers.entrySet()) {
                    size += e.getKey().length() + (e.getValue() != null ? e.getValue().length() : 0) + 4;
                }
            }
            return size;
        }
    }

    static final class ProducerBatch {
        final String topic;
        final int    partition;
        final long   createdAt;
        final List<EnqueuedRecord> records = new ArrayList<>();
        boolean ready = false;
        int estimatedBytes = 0;

        ProducerBatch(String topic, int partition) {
            this.topic     = topic;
            this.partition = partition;
            this.createdAt = System.currentTimeMillis();
        }
    }

    record EnqueuedRecord(byte[] key, byte[] value, Map<String, String> headers,
                          long timestamp, CompletableFuture<TorrentProducer.RecordMetadata> future) {}

    // =======================================================================
    // Sender loop
    // =======================================================================

    private final class SenderLoop implements Runnable {
        @Override
        public void run() {
            while (!closed.get()) {
                try {
                    List<ProducerBatch> readyBatches = accumulator.drainReady();
                    if (readyBatches.isEmpty()) {
                        Thread.sleep(config.lingerMs);
                        continue;
                    }

                    for (ProducerBatch batch : readyBatches) {
                        sendBatch(batch);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    // Log and continue
                    try { Thread.sleep(100); } catch (InterruptedException ignored) { break; }
                }
            }
        }

        private void sendBatch(ProducerBatch batch) {
            int retries = config.maxRetries;
            Exception lastException = null;

            for (int attempt = 0; attempt <= retries; attempt++) {
                if (attempt > 0) {
                    try {
                        long backoff = config.retryBackoffMs * (1L << (attempt - 1));
                        Thread.sleep(backoff);
                        refreshMetadata();
                    } catch (Exception e) {
                        lastException = e;
                    }
                }

                try {
                    sendBatchOnce(batch);
                    return; // success
                } catch (Exception e) {
                    lastException = e;
                }
            }

            // All retries exhausted — fail all futures
            Exception finalEx = lastException;
            for (EnqueuedRecord rec : batch.records) {
                rec.future.completeExceptionally(
                    new IOException("Failed after " + retries + " retries", finalEx));
            }
        }

        private void sendBatchOnce(ProducerBatch batch) throws Exception {
            // Build protocol record batch
            long nowMs = System.currentTimeMillis();
            Protocol.RecordBatch recordBatch = new Protocol.RecordBatch();
            recordBatch.baseOffset          = 0;
            recordBatch.partitionLeaderEpoch = -1;
            recordBatch.magic                = Protocol.RECORD_BATCH_MAGIC;
            recordBatch.attributes           = 0;
            recordBatch.lastOffsetDelta      = batch.records.size() - 1;
            recordBatch.firstTimestamp       = nowMs;
            recordBatch.maxTimestamp         = nowMs;
            recordBatch.producerID           = -1;
            recordBatch.producerEpoch        = -1;
            recordBatch.baseSequence         = -1;

            int ct = compressionTypeToInt(config.compressionType);
            recordBatch.setCompressionType(ct);

            Protocol.Record[] records = new Protocol.Record[batch.records.size()];
            for (int i = 0; i < batch.records.size(); i++) {
                EnqueuedRecord er = batch.records.get(i);
                Protocol.RecordHeader[] hdrs = null;
                if (er.headers != null && !er.headers.isEmpty()) {
                    hdrs = new Protocol.RecordHeader[er.headers.size()];
                    int j = 0;
                    for (var e : er.headers.entrySet()) {
                        hdrs[j++] = new Protocol.RecordHeader(e.getKey(),
                            e.getValue() != null ? e.getValue().getBytes(StandardCharsets.UTF_8) : null);
                    }
                }
                records[i] = new Protocol.Record(i, 0, er.key, er.value, hdrs);
            }
            recordBatch.records = records;

            byte[] recordData = Protocol.encodeRecordBatch(recordBatch);

            // Encode produce request body
            byte[] body = encodeProduceRequest(batch.topic, batch.partition, recordData,
                                                config.acks, config.requestTimeoutMs);

            int corrId = Protocol.nextCorrelationId();
            byte[] request = Protocol.buildRequest(Protocol.API_PRODUCE, (short) 4, corrId,
                                                    config.clientId, body);

            CompletableFuture<Protocol.ResponseFraming> netFuture =
                networkClient.send(bootstrapHost, bootstrapPort, corrId, request);

            Protocol.ResponseFraming resp = netFuture.get(config.requestTimeoutMs, TimeUnit.MILLISECONDS);

            // Parse response — get base offset and error code
            DecodedProduceResult result = decodeProduceResponse(resp.body, batch.topic, batch.partition);

            if (result.errorCode != Protocol.ERR_NONE) {
                throw new IOException("Produce error: " + Protocol.errorCodeDescription(result.errorCode));
            }

            // Notify all futures
            for (int i = 0; i < batch.records.size(); i++) {
                TorrentProducer.RecordMetadata meta = new TorrentProducer.RecordMetadata(
                    batch.topic, batch.partition, result.baseOffset + i, nowMs);
                batch.records.get(i).future.complete(meta);
            }
        }
    }

    private static int compressionTypeToInt(String ct) {
        return switch (ct != null ? ct.toLowerCase() : "none") {
            case "gzip"   -> Protocol.COMPRESSION_GZIP;
            case "snappy" -> Protocol.COMPRESSION_SNAPPY;
            case "lz4"    -> Protocol.COMPRESSION_LZ4;
            case "zstd"   -> Protocol.COMPRESSION_ZSTD;
            default       -> Protocol.COMPRESSION_NONE;
        };
    }

    // -----------------------------------------------------------------------
    // Cluster metadata model
    // -----------------------------------------------------------------------

    static final class ClusterMetadata {
        final Map<String, TopicMeta> topics     = new HashMap<>();
        final Map<Integer, BrokerMeta> brokers  = new HashMap<>();
        int  controllerId = -1;
        String clusterId  = "";

        record TopicMeta(String name, boolean internal, List<PartitionMeta> partitions) {}
        record PartitionMeta(int id, int leaderId, List<Integer> replicas, List<Integer> isr) {}
        record BrokerMeta(int id, String host, int port, String rack) {}
    }

    // -----------------------------------------------------------------------
    // Request/Response encoding helpers
    // -----------------------------------------------------------------------

    /** Encodes a Metadata request body (null topics = all topics). */
    static byte[] encodeMetadataRequest(List<String> topics) {
        ByteBuffer buf = ByteBuffer.allocate(256);
        if (topics == null) {
            buf.putInt(-1);
        } else {
            buf.putInt(topics.size());
            for (String t : topics) Protocol.writeString(buf, t);
        }
        buf.put((byte) 0); // allowAutoTopicCreation = false
        // No tagged fields for v4
        byte[] out = new byte[buf.position()];
        buf.flip();
        buf.get(out);
        return out;
    }

    /** Parses a Metadata response body into ClusterMetadata. */
    static ClusterMetadata decodeMetadataResponse(byte[] body) throws Protocol.ProtocolException {
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
            short errorCode = buf.getShort();
            String name = Protocol.readString(buf);
            boolean internal = buf.get() != 0;

            int partCount = buf.getInt();
            List<ClusterMetadata.PartitionMeta> parts = new ArrayList<>(partCount);
            for (int j = 0; j < partCount; j++) {
                short pErr = buf.getShort();
                int pId   = buf.getInt();
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

    /** Encodes a Produce request body. */
    static byte[] encodeProduceRequest(String topic, int partition, byte[] recordData,
                                        short acks, int timeoutMs) {
        ByteBuffer buf = ByteBuffer.allocate(256 + recordData.length);
        // transactional_id = null
        Protocol.writeNullableString(buf, null);
        buf.putShort(acks);
        buf.putInt(timeoutMs);

        // topic array
        buf.putInt(1); // one topic
        Protocol.writeString(buf, topic);

        // partition array
        buf.putInt(1); // one partition
        buf.putInt(partition);
        Protocol.writeNullableBytes(buf, recordData);

        byte[] out = new byte[buf.position()];
        buf.flip();
        buf.get(out);
        return out;
    }

    private record DecodedProduceResult(long baseOffset, short errorCode) {}

    /** Simplified produce response decoder. */
    static DecodedProduceResult decodeProduceResponse(byte[] body, String topic, int partition)
        throws Protocol.ProtocolException {
        ByteBuffer buf = ByteBuffer.wrap(body);

        int topicCount = buf.getInt();
        for (int i = 0; i < topicCount; i++) {
            String t = Protocol.readString(buf);
            int partCount = buf.getInt();
            for (int j = 0; j < partCount; j++) {
                int p = buf.getInt();
                short err = buf.getShort();
                long baseOffset = buf.getLong();
                long logAppendTime = buf.getLong();
                long logStartOffset = buf.getLong();

                if (t.equals(topic) && p == partition) {
                    return new DecodedProduceResult(baseOffset, err);
                }
            }
        }
        throw new Protocol.ProtocolException("Topic/partition not found in produce response");
    }
}
