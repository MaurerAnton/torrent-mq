package org.torrentmq.client.network;

import org.torrentmq.client.protocol.Protocol;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Non-blocking TCP network client for torrent-mq brokers.
 *
 * <p>Manages a pool of persistent {@link SocketChannel} connections to broker
 * nodes, multiplexed through a single {@link Selector}-based event loop.
 * Supports request/response correlation via correlation IDs, automatic
 * reconnection with backoff, and optional TLS encryption via {@link SSLEngine}.</p>
 *
 * <p>Thread safety: all public methods may be called from any thread.
 * Internally, state transitions are guarded by the channel's lock.</p>
 */
public final class NetworkClient implements AutoCloseable {

    // -----------------------------------------------------------------------
    // Configuration
    // -----------------------------------------------------------------------

    /**
     * Immutable configuration for the network client.
     */
    public static final class Config {
        /** I/O poll timeout in milliseconds. */
        public final long pollTimeoutMs;
        /** Socket connect timeout in milliseconds. */
        public final int connectTimeoutMs;
        /** Socket read timeout in milliseconds. */
        public final int socketTimeoutMs;
        /** Maximum number of reconnection attempts (-1 = infinite). */
        public final int maxReconnectAttempts;
        /** Base backoff between reconnection attempts in milliseconds. */
        public final long reconnectBackoffMs;
        /** Whether to enable TLS. */
        public final boolean useTls;
        /** SSL context for TLS connections (required when useTls is true). */
        public final SSLContext sslContext;
        /** Maximum bytes per in-flight response buffer. */
        public final int maxResponseSize;

        private Config(Builder builder) {
            this.pollTimeoutMs        = builder.pollTimeoutMs;
            this.connectTimeoutMs     = builder.connectTimeoutMs;
            this.socketTimeoutMs      = builder.socketTimeoutMs;
            this.maxReconnectAttempts = builder.maxReconnectAttempts;
            this.reconnectBackoffMs   = builder.reconnectBackoffMs;
            this.useTls               = builder.useTls;
            this.sslContext           = builder.sslContext;
            this.maxResponseSize      = builder.maxResponseSize;
        }

        public static Builder builder() { return new Builder(); }

        public static final class Builder {
            private long pollTimeoutMs        = 100;
            private int  connectTimeoutMs     = 10_000;
            private int  socketTimeoutMs      = 60_000;
            private int  maxReconnectAttempts = 10;
            private long reconnectBackoffMs   = 200;
            private boolean useTls            = false;
            private SSLContext sslContext;
            private int maxResponseSize       = 64 * 1024 * 1024; // 64 MiB

            public Builder pollTimeoutMs(long v)          { pollTimeoutMs = v; return this; }
            public Builder connectTimeoutMs(int v)         { connectTimeoutMs = v; return this; }
            public Builder socketTimeoutMs(int v)          { socketTimeoutMs = v; return this; }
            public Builder maxReconnectAttempts(int v)     { maxReconnectAttempts = v; return this; }
            public Builder reconnectBackoffMs(long v)      { reconnectBackoffMs = v; return this; }
            public Builder useTls(boolean v)               { useTls = v; return this; }
            public Builder sslContext(SSLContext v)         { sslContext = v; return this; }
            public Builder maxResponseSize(int v)           { maxResponseSize = v; return this; }

            public Config build() {
                if (useTls && sslContext == null) {
                    throw new IllegalArgumentException("sslContext is required when useTls is enabled");
                }
                return new Config(this);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Internal connection state
    // -----------------------------------------------------------------------

    private static final class BrokerConnection {
        final InetSocketAddress address;
        volatile SocketChannel channel;
        SSLEngine sslEngine;
        ByteBuffer sslNetIn;
        ByteBuffer sslNetOut;
        ByteBuffer sslAppIn;

        // Pending write buffer
        final ByteBuffer writeBuf;
        // Read buffer for accumulating incoming data
        ByteBuffer readBuf;

        // In-flight requests keyed by correlation ID
        final Map<Integer, CompletableFuture<Protocol.ResponseFraming>> inflight;

        // Reconnection state
        final AtomicBoolean connected = new AtomicBoolean(false);
        int  reconnectAttempt = 0;
        long nextReconnectAt  = 0;

        BrokerConnection(InetSocketAddress address, int maxResponseSize) {
            this.address   = address;
            this.inflight  = new ConcurrentHashMap<>();
            this.writeBuf  = ByteBuffer.allocateDirect(256 * 1024);
            this.readBuf   = ByteBuffer.allocateDirect(8); // start small, resize on first frame
            this.writeBuf.limit(0); // nothing to write initially
        }
    }

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------

    private final Config config;
    private final Selector selector;
    private final Map<InetSocketAddress, BrokerConnection> connections;
    private final Thread eventLoopThread;
    private final AtomicBoolean running = new AtomicBoolean(true);

    // Write queue: tuples of (address, bytes) to be flushed at next poll
    private final ConcurrentLinkedQueue<WriteRequest> writeQueue = new ConcurrentLinkedQueue<>();

    private record WriteRequest(InetSocketAddress address, byte[] data,
                                 CompletableFuture<Void> future) {}

    /** Client identifier sent in request headers. */
    private final String clientId;

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Creates a new NetworkClient and starts the background I/O event loop.
     *
     * @param clientId client identifier string
     * @param config   network configuration
     * @throws IOException if the selector cannot be opened
     */
    public NetworkClient(String clientId, Config config) throws IOException {
        this.clientId    = Objects.requireNonNull(clientId);
        this.config      = Objects.requireNonNull(config);
        this.selector    = Selector.open();
        this.connections = new ConcurrentHashMap<>();
        this.eventLoopThread = new Thread(this::eventLoop, "torrentmq-nio-eventloop");
        this.eventLoopThread.setDaemon(true);
        this.eventLoopThread.start();
    }

    /**
     * Ensures a connection to the given broker address exists.
     * If not yet connected, triggers an asynchronous connect.
     *
     * @param host broker hostname
     * @param port broker port
     * @return a future that completes when the connection is ready (or fails)
     */
    public CompletableFuture<Void> connect(String host, int port) {
        InetSocketAddress addr = new InetSocketAddress(host, port);
        BrokerConnection conn = connections.computeIfAbsent(addr,
            a -> new BrokerConnection(a, config.maxResponseSize));
        if (conn.connected.get()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        // Initiate connect asynchronously
        try {
            SocketChannel sc = SocketChannel.open();
            sc.configureBlocking(false);
            sc.setOption(StandardSocketOptions.TCP_NODELAY, true);
            sc.connect(addr);
            conn.channel = sc;
            // Register with selector for CONNECT interest
            sc.register(selector, SelectionKey.OP_CONNECT, conn);
            // We'll complete the future once the connection is established in eventLoop
            conn.inflight.put(-addr.hashCode(), future.thenApply(v -> null)); // marker
        } catch (IOException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    /**
     * Sends a wire-format request to a broker and returns a future for the response.
     *
     * @param host          broker hostname
     * @param port          broker port
     * @param correlationId unique correlation identifier
     * @param requestBytes  fully framed request bytes (from Protocol.buildRequest)
     * @return a future that completes with the parsed response
     */
    public CompletableFuture<Protocol.ResponseFraming> send(String host, int port,
                                                             int correlationId, byte[] requestBytes) {
        InetSocketAddress addr = new InetSocketAddress(host, port);
        CompletableFuture<Protocol.ResponseFraming> future = new CompletableFuture<>();

        BrokerConnection conn = connections.computeIfAbsent(addr,
            a -> new BrokerConnection(a, config.maxResponseSize));

        conn.inflight.put(correlationId, future);

        // Queue the write
        writeQueue.add(new WriteRequest(addr, requestBytes, null));
        // Wake up the selector to process the write immediately
        selector.wakeup();
        return future;
    }

    /**
     * Disconnects from a specific broker, completing any pending futures exceptionally.
     *
     * @param host broker hostname
     * @param port broker port
     */
    public void disconnect(String host, int port) {
        InetSocketAddress addr = new InetSocketAddress(host, port);
        BrokerConnection conn = connections.remove(addr);
        if (conn != null) {
            closeConnection(conn);
        }
    }

    /**
     * Gracefully shuts down the event loop and closes all connections.
     */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) return;
        selector.wakeup();
        try {
            eventLoopThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (BrokerConnection conn : connections.values()) {
            closeConnection(conn);
        }
        connections.clear();
        try {
            selector.close();
        } catch (IOException ignored) { /* best-effort */ }
    }

    /**
     * Returns the number of active broker connections.
     */
    public int connectionCount() {
        return (int) connections.values().stream().filter(c -> c.connected.get()).count();
    }

    // -----------------------------------------------------------------------
    // Event loop
    // -----------------------------------------------------------------------

    private void eventLoop() {
        while (running.get()) {
            try {
                // Process any queued writes before blocking on select
                drainWriteQueue();

                int ready = selector.select(config.pollTimeoutMs);
                if (ready == 0) {
                    // Timeout — check for reconnection candidates
                    attemptReconnects();
                    continue;
                }

                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();

                    if (!key.isValid()) continue;

                    BrokerConnection conn = (BrokerConnection) key.attachment();
                    if (conn == null) {
                        key.cancel();
                        continue;
                    }

                    try {
                        if (key.isConnectable())         handleConnect(key, conn);
                        if (key.isReadable())            handleRead(key, conn);
                        if (key.isValid() && key.isWritable()) handleWrite(key, conn);
                    } catch (Exception e) {
                        // Connection broken — schedule reconnect
                        handleDisconnect(conn, e);
                    }
                }
            } catch (IOException e) {
                // Selector-level error — log and continue
                if (running.get()) {
                    try { Thread.sleep(100); } catch (InterruptedException ignored) {}
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Connection lifecycle
    // -----------------------------------------------------------------------

    private void handleConnect(SelectionKey key, BrokerConnection conn) {
        try {
            SocketChannel sc = conn.channel;
            if (sc == null) { key.cancel(); return; }

            if (sc.finishConnect()) {
                // Remove the connect-marker future
                conn.inflight.remove(-conn.address.hashCode());

                if (config.useTls) {
                    initTls(conn);
                }

                conn.connected.set(true);
                conn.reconnectAttempt = 0;

                // Register for reads
                key.interestOps(SelectionKey.OP_READ);
            } else {
                // Still connecting — keep OP_CONNECT
            }
        } catch (IOException e) {
            handleDisconnect(conn, e);
        }
    }

    private void initTls(BrokerConnection conn) throws SSLException {
        SSLEngine engine = config.sslContext.createSSLEngine(
            conn.address.getHostString(), conn.address.getPort());
        engine.setUseClientMode(true);
        conn.sslEngine = engine;
        conn.sslNetIn  = ByteBuffer.allocateDirect(engine.getSession().getPacketBufferSize());
        conn.sslNetOut = ByteBuffer.allocateDirect(engine.getSession().getPacketBufferSize());
        conn.sslAppIn  = ByteBuffer.allocateDirect(engine.getSession().getApplicationBufferSize());

        // Initiate TLS handshake
        engine.beginHandshake();
    }

    private void handleDisconnect(BrokerConnection conn, Exception cause) {
        conn.connected.set(false);
        try {
            if (conn.channel != null) {
                conn.channel.close();
            }
        } catch (IOException ignored) {}
        conn.channel = null;
        conn.sslEngine = null;
        conn.writeBuf.clear();
        conn.writeBuf.limit(0);

        // Fail all pending futures
        IOException err = new IOException("Connection to " + conn.address + " lost", cause);
        for (CompletableFuture<Protocol.ResponseFraming> f : conn.inflight.values()) {
            f.completeExceptionally(err);
        }
        conn.inflight.clear();

        // Schedule reconnect
        if (config.maxReconnectAttempts < 0 || conn.reconnectAttempt < config.maxReconnectAttempts) {
            conn.nextReconnectAt = System.currentTimeMillis()
                + config.reconnectBackoffMs * (1L << Math.min(conn.reconnectAttempt, 10));
            conn.reconnectAttempt++;
        }
    }

    private void attemptReconnects() {
        long now = System.currentTimeMillis();
        for (BrokerConnection conn : connections.values()) {
            if (conn.connected.get()) continue;
            if (conn.nextReconnectAt > 0 && now < conn.nextReconnectAt) continue;
            if (config.maxReconnectAttempts >= 0
                && conn.reconnectAttempt >= config.maxReconnectAttempts) continue;

            try {
                SocketChannel sc = SocketChannel.open();
                sc.configureBlocking(false);
                sc.setOption(StandardSocketOptions.TCP_NODELAY, true);
                sc.connect(conn.address);
                conn.channel = sc;
                conn.writeBuf.clear();
                conn.writeBuf.limit(0);
                sc.register(selector, SelectionKey.OP_CONNECT, conn);
            } catch (IOException e) {
                conn.reconnectAttempt++;
                conn.nextReconnectAt = now + config.reconnectBackoffMs
                    * (1L << Math.min(conn.reconnectAttempt, 10));
            }
        }
    }

    private void closeConnection(BrokerConnection conn) {
        conn.connected.set(false);
        IOException err = new IOException("Connection closed");
        for (CompletableFuture<Protocol.ResponseFraming> f : conn.inflight.values()) {
            f.completeExceptionally(err);
        }
        conn.inflight.clear();
        try {
            if (conn.channel != null) {
                conn.channel.close();
            }
        } catch (IOException ignored) {}
        conn.channel = null;
        conn.sslEngine = null;
    }

    // -----------------------------------------------------------------------
    // I/O operations
    // -----------------------------------------------------------------------

    private void drainWriteQueue() {
        WriteRequest req;
        while ((req = writeQueue.poll()) != null) {
            BrokerConnection conn = connections.get(req.address);
            if (conn == null || !conn.connected.get()) {
                if (req.future != null) {
                    req.future.completeExceptionally(
                        new IOException("No connection to " + req.address));
                }
                continue;
            }
            // Append to the connection's write buffer
            synchronized (conn.writeBuf) {
                int needed = conn.writeBuf.remaining() + req.data.length;
                if (needed > conn.writeBuf.capacity()) {
                    ByteBuffer larger = ByteBuffer.allocateDirect(needed * 2);
                    conn.writeBuf.flip();
                    larger.put(conn.writeBuf);
                    conn.writeBuf = larger;
                } else {
                    conn.writeBuf.compact();
                }
                conn.writeBuf.put(req.data);
                conn.writeBuf.flip();
            }
            // Enable write interest
            SelectionKey key = conn.channel != null ? conn.channel.keyFor(selector) : null;
            if (key != null && key.isValid()) {
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            }
        }
    }

    private void handleWrite(SelectionKey key, BrokerConnection conn) throws IOException {
        if (config.useTls) {
            handleTlsWrite(key, conn);
            return;
        }

        SocketChannel sc = conn.channel;
        synchronized (conn.writeBuf) {
            if (conn.writeBuf.hasRemaining()) {
                sc.write(conn.writeBuf);
            }
            if (!conn.writeBuf.hasRemaining()) {
                // All data written — clear write interest
                conn.writeBuf.compact().flip();
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            }
        }
    }

    private void handleRead(SelectionKey key, BrokerConnection conn) throws IOException {
        if (config.useTls) {
            handleTlsRead(key, conn);
            return;
        }

        SocketChannel sc = conn.channel;

        // Expand read buffer if full
        if (!conn.readBuf.hasRemaining()) {
            ByteBuffer larger = ByteBuffer.allocateDirect(conn.readBuf.capacity() * 2);
            conn.readBuf.flip();
            larger.put(conn.readBuf);
            conn.readBuf = larger;
        }

        int bytesRead = sc.read(conn.readBuf);
        if (bytesRead == -1) {
            throw new IOException("End of stream");
        }
        if (bytesRead == 0) return;

        // Try to parse complete frames
        conn.readBuf.flip();
        while (parseFrames(conn)) {
            // parseFrames advances the buffer position for each parsed frame
        }
        conn.readBuf.compact();
    }

    private boolean parseFrames(BrokerConnection conn) {
        ByteBuffer buf = conn.readBuf;
        if (buf.remaining() < 4) return false;

        int startPos = buf.position();
        int frameSize = buf.getInt(startPos); // peek
        if (frameSize < 0 || frameSize > config.maxResponseSize) {
            // Corrupt — discard
            buf.position(buf.limit());
            return false;
        }
        if (buf.remaining() < 4 + frameSize) return false;

        // Read the full frame
        byte[] frame = new byte[4 + frameSize];
        buf.get(frame);

        try {
            Protocol.ResponseFraming resp = Protocol.parseResponse(frame);
            CompletableFuture<Protocol.ResponseFraming> f = conn.inflight.remove(resp.correlationId);
            if (f != null) {
                f.complete(resp);
            }
        } catch (Exception e) {
            // Malformed response — discard
        }
        return buf.remaining() >= 4;
    }

    // -----------------------------------------------------------------------
    // TLS I/O
    // -----------------------------------------------------------------------

    private void handleTlsWrite(SelectionKey key, BrokerConnection conn) throws IOException {
        SSLEngine engine = conn.sslEngine;
        if (engine == null) return;

        SocketChannel sc = conn.channel;

        // Feed application data into the SSLEngine if we have pending writes
        synchronized (conn.writeBuf) {
            if (conn.writeBuf.hasRemaining()) {
                ByteBuffer temp = conn.writeBuf.duplicate();
                SSLEngineResult result = engine.wrap(temp, conn.sslNetOut);
                conn.writeBuf.position(conn.writeBuf.position() + temp.position() - conn.writeBuf.position());
            }
        }

        // Flush encrypted data to socket
        conn.sslNetOut.flip();
        if (conn.sslNetOut.hasRemaining()) {
            sc.write(conn.sslNetOut);
        }
        conn.sslNetOut.compact();

        // Check if handshake is still in progress
        if (engine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
            conn.sslNetOut.clear();
            SSLEngineResult result = engine.wrap(ByteBuffer.allocate(0), conn.sslNetOut);
            conn.sslNetOut.flip();
            if (conn.sslNetOut.hasRemaining()) {
                sc.write(conn.sslNetOut);
            }
            conn.sslNetOut.compact();
        }

        // Clear write interest if nothing left to write
        synchronized (conn.writeBuf) {
            if (!conn.writeBuf.hasRemaining() && conn.sslNetOut.position() == 0) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            }
        }
    }

    private void handleTlsRead(SelectionKey key, BrokerConnection conn) throws IOException {
        SSLEngine engine = conn.sslEngine;
        if (engine == null) return;

        SocketChannel sc = conn.channel;

        // Read encrypted data from socket
        int bytesRead = sc.read(conn.sslNetIn);
        if (bytesRead == -1) {
            throw new IOException("End of stream");
        }
        if (bytesRead == 0) return;

        conn.sslNetIn.flip();

        // Unwrap
        while (conn.sslNetIn.hasRemaining()) {
            conn.sslAppIn.clear();
            SSLEngineResult result = engine.unwrap(conn.sslNetIn, conn.sslAppIn);

            conn.sslAppIn.flip();
            if (conn.sslAppIn.hasRemaining()) {
                // Append decrypted data to read buffer
                if (conn.readBuf.remaining() < conn.sslAppIn.remaining()) {
                    conn.readBuf.flip();
                    ByteBuffer larger = ByteBuffer.allocateDirect(
                        conn.readBuf.remaining() + conn.sslAppIn.remaining() + 4096);
                    larger.put(conn.readBuf);
                    conn.readBuf = larger;
                } else {
                    conn.readBuf.compact();
                }
                conn.readBuf.put(conn.sslAppIn);
                conn.readBuf.flip();
            }

            switch (result.getStatus()) {
                case BUFFER_UNDERFLOW:
                    // Need more data — keep what's left in sslNetIn
                    break;
                case BUFFER_OVERFLOW:
                    conn.sslAppIn = ByteBuffer.allocateDirect(
                        engine.getSession().getApplicationBufferSize() * 2);
                    break;
                default:
                    break;
            }

            if (result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) break;
        }
        conn.sslNetIn.compact();

        // Handshake continuation
        if (engine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_TASK) {
            Runnable task;
            while ((task = engine.getDelegatedTask()) != null) {
                task.run();
            }
        }
        if (engine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        }

        // Parse frames from the read buffer
        while (parseFrames(conn)) {}
    }
}
