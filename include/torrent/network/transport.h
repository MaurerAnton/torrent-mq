#pragma once

/// \\file transport.h
/// \\brief Transport abstraction layer for torrent-mq — TCP and TLS 1.3 connections.
///
/// All types live in \\c torrent::network.

#include <cstdint>
#include <string>
#include <string_view>
#include <memory>
#include <functional>
#include <system_error>
#include <chrono>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

// ---------------------------------------------------------------------------
// OpenSSL forward declarations (no headers required for the API surface)
// ---------------------------------------------------------------------------

struct ssl_ctx_st;     // SSL_CTX
struct ssl_st;         // SSL
struct ssl_session_st; // SSL_SESSION

namespace torrent {

// Re-declared from common/types.h for header self-sufficiency.
struct endpoint;
struct buffer_view;
class shared_buffer;

} // namespace torrent

namespace torrent::network {

// ============================================================================
// Constants
// ============================================================================

/// Default I/O buffer size for socket operations.
inline constexpr size_t kDefaultBufferSize = 65536;

/// Default connect timeout (ms).
inline constexpr int kDefaultConnectTimeoutMs = 5000;

/// Default idle timeout before connection is torn down (ms).
inline constexpr int kDefaultIdleTimeoutMs = 60000;

/// Default ALPN protocol advertisement (wire-format: length-prefixed "torrent1").
inline constexpr const char* kDefaultAlpnProtocols = "\x08torrent1";

/// Default TLS 1.3 cipher suite list (OpenSSL colon-delimited).
/// Prioritises PFS and hardware-accelerated AEAD ciphers.
inline constexpr const char* kDefaultTlsCiphers =
    "TLS_AES_256_GCM_SHA384:"
    "TLS_CHACHA20_POLY1305_SHA256:"
    "TLS_AES_128_GCM_SHA256";

/// Default server-side session cache capacity.
inline constexpr size_t kDefaultSessionCacheSize = 2048;

/// Default session ticket lifetime (seconds) before rotation.
inline constexpr int kDefaultSessionTicketLifetimeSec = 300;

// ============================================================================
// TransportConfig
// ============================================================================

/// Per-connection transport settings applied at creation time.
struct TransportConfig {
    /// Local bind address.  Empty host = INADDR_ANY / in6addr_any.
    endpoint bind_address{};

    /// SO_SNDBUF size (0 = OS default).
    int send_buffer_size = 0;

    /// SO_RCVBUF size (0 = OS default).
    int receive_buffer_size = 0;

    /// Enable TCP_NODELAY (disable Nagle).  Default on for low-latency.
    bool tcp_nodelay = true;

    /// Enable SO_REUSEADDR.
    bool reuse_address = false;

    /// Enable TCP keepalive probes.
    bool keepalive = false;

    /// TCP_KEEPIDLE seconds before first probe.
    int keepalive_idle_sec = 7200;

    /// TCP_KEEPINTVL seconds between probes.
    int keepalive_interval_sec = 75;

    /// TCP_KEEPCNT — probes before declaring dead.
    int keepalive_count = 9;

    /// Connect timeout (0 = block indefinitely / OS default).
    std::chrono::milliseconds connect_timeout{0};

    /// Idle timeout after which the connection is closed (0 = disabled).
    std::chrono::milliseconds idle_timeout{0};
};

// ============================================================================
// Transport — abstract base
// ============================================================================

/// Abstract bi-directional stream between two peers.
///
/// Implementations handle the underlying I/O model but expose a uniform
/// interface to the protocol layer.
class Transport {
public:
    Transport() = default;
    virtual ~Transport() = default;

    Transport(const Transport&) = delete;
    Transport& operator=(const Transport&) = delete;
    Transport(Transport&&) noexcept = default;
    Transport& operator=(Transport&&) noexcept = default;

    // --- I/O ----------------------------------------------------------------

    /// Read up to \\c len bytes into \\c buf.  Returns bytes read, 0 on
    /// clean remote shutdown, or negative on error.
    [[nodiscard]] virtual ssize_t read(char* buf, size_t len) = 0;

    /// Write \\c len bytes from \\c buf.  Returns bytes written or negative
    /// on error.  Callers must handle partial writes for non-blocking transports.
    [[nodiscard]] virtual ssize_t write(const char* buf, size_t len) = 0;

    /// Flush any buffered writes to the kernel / network.
    virtual void flush() = 0;

    /// Close the connection.  No further I/O permitted.
    virtual void close() = 0;

    /// Shutdown one direction: SHUT_RD, SHUT_WR, or SHUT_RDWR.
    virtual void shutdown(int how) = 0;

    // --- State -------------------------------------------------------------

    /// Native file descriptor, or -1 if not connected.
    [[nodiscard]] virtual int fd() const noexcept = 0;

    /// \\c true if the connection is established and alive.
    [[nodiscard]] virtual bool connected() const noexcept = 0;

    /// Remote endpoint, or empty endpoint if unknown.
    [[nodiscard]] virtual endpoint peer() const noexcept = 0;

    /// Local endpoint, or empty endpoint if unknown.
    [[nodiscard]] virtual endpoint local() const noexcept = 0;

    /// \\c true if this transport uses TLS.
    [[nodiscard]] virtual bool is_tls() const noexcept { return false; }

    // --- Socket options ----------------------------------------------------

    /// Apply / remove TCP_NODELAY.
    virtual void set_nodelay(bool enable) = 0;

    /// Apply / remove SO_REUSEADDR.
    virtual void set_reuse_address(bool enable) = 0;

    /// Configure kernel TCP keepalive.
    virtual void set_keepalive(
        bool enable,
        int idle_sec = 7200,
        int interval_sec = 75,
        int count = 9) = 0;
};

// ============================================================================
// TcpTransport — raw TCP, Nagle off, TCP_CORK, keepalive
// ============================================================================

/// Raw TCP transport tuned for low-latency messaging.
///
/// Owns the socket fd; closes it on destruction.  Supports non-blocking
/// connects via \\c connect() / \\c finish_connect().
class TcpTransport final : public Transport {
public:
    /// Construct an unconnected transport.
    TcpTransport();

    /// Adopt an already-connected socket (used by Acceptor).
    explicit TcpTransport(int fd, const endpoint& peer, const endpoint& local);

    ~TcpTransport() override;

    TcpTransport(const TcpTransport&) = delete;
    TcpTransport& operator=(const TcpTransport&) = delete;
    TcpTransport(TcpTransport&& other) noexcept;
    TcpTransport& operator=(TcpTransport&& other) noexcept;

    // --- I/O ----------------------------------------------------------------

    [[nodiscard]] ssize_t read(char* buf, size_t len) override;
    [[nodiscard]] ssize_t write(const char* buf, size_t len) override;
    void flush() override;
    void close() override;
    void shutdown(int how) override;

    // --- State --------------------------------------------------------------

    [[nodiscard]] int fd() const noexcept override { return fd_; }
    [[nodiscard]] bool connected() const noexcept override { return fd_ >= 0; }
    [[nodiscard]] endpoint peer() const noexcept override { return peer_; }
    [[nodiscard]] endpoint local() const noexcept override { return local_; }

    // --- Socket options -----------------------------------------------------

    void set_nodelay(bool enable) override;
    void set_reuse_address(bool enable) override;
    void set_keepalive(bool enable, int idle_sec, int interval_sec, int count) override;

    // --- TCP_CORK (Linux) ---------------------------------------------------

    /// Enable TCP_CORK — coalesce subsequent writes.
    void cork();

    /// Disable TCP_CORK and flush corked data.
    void uncork();

    // --- Connect ------------------------------------------------------------

    /// Initiate a non-blocking connect to \\c remote.
    /// Returns \\c true if completed synchronously; on EINPROGRESS the caller
    /// must poll fd and call \\c finish_connect() when writable.
    [[nodiscard]] bool connect(const endpoint& remote);

    /// Complete a non-blocking connect.  Returns 0 on success, errno on failure.
    [[nodiscard]] int finish_connect();

    /// Apply TransportConfig options to this socket.
    void apply_config(const TransportConfig& config);

private:
    int fd_ = -1;
    endpoint peer_{};
    endpoint local_{};
};

// ============================================================================
// TlsTransport — TLS 1.3 (OpenSSL) with mTLS, ALPN, session caching
// ============================================================================

/// TLS transport wrapping an existing TcpTransport.
///
/// Features: TLS 1.3, mTLS, ALPN negotiation, server-side session caching,
/// session tickets with key rotation, custom cipher suites.
///
/// Call \\c handshake() after the underlying TCP connection is established.
class TlsTransport final : public Transport {
public:
    /// Wrap an already-connected TCP transport.
    /// \\param tcp       Ownership transferred.
    /// \\param ssl_ctx   OpenSSL context (owned by TransportFactory).
    /// \\param is_server Perform server-side handshake.
    TlsTransport(TcpTransport tcp, ssl_ctx_st* ssl_ctx, bool is_server);
    ~TlsTransport() override;

    TlsTransport(const TlsTransport&) = delete;
    TlsTransport& operator=(const TlsTransport&) = delete;
    TlsTransport(TlsTransport&& other) noexcept;
    TlsTransport& operator=(TlsTransport&& other) noexcept;

    // --- I/O ----------------------------------------------------------------

    [[nodiscard]] ssize_t read(char* buf, size_t len) override;
    [[nodiscard]] ssize_t write(const char* buf, size_t len) override;
    void flush() override;
    void close() override;
    void shutdown(int how) override;

    // --- State --------------------------------------------------------------

    [[nodiscard]] int fd() const noexcept override;
    [[nodiscard]] bool connected() const noexcept override;
    [[nodiscard]] endpoint peer() const noexcept override;
    [[nodiscard]] endpoint local() const noexcept override;
    [[nodiscard]] bool is_tls() const noexcept override { return true; }

    // --- Socket options (delegated to underlying TcpTransport) --------------

    void set_nodelay(bool enable) override;
    void set_reuse_address(bool enable) override;
    void set_keepalive(bool enable, int idle_sec, int interval_sec, int count) override;

    // --- TLS handshake ------------------------------------------------------

    /// Perform the TLS handshake.
    /// Returns 1 on success, 0 if more I/O is needed (WANT_READ/WANT_WRITE),
    /// or negative on failure.
    [[nodiscard]] int handshake();

    /// \\c true if the TLS handshake has completed.
    [[nodiscard]] bool handshake_complete() const noexcept;

    // --- TLS metadata -------------------------------------------------------

    /// Negotiated ALPN protocol, or empty string.
    [[nodiscard]] std::string negotiated_alpn() const;

    /// Negotiated cipher suite name (e.g. "TLS_AES_256_GCM_SHA384").
    [[nodiscard]] std::string cipher_name() const;

    /// TLS protocol version string (e.g. "TLSv1.3").
    [[nodiscard]] std::string protocol_version_str() const;

    /// Peer certificate common name (CN), or empty.
    [[nodiscard]] std::string peer_cn() const;

    /// \\c true if peer presented a valid certificate.
    [[nodiscard]] bool peer_verified() const noexcept;

    /// Underlying TCP transport (non-owning).
    [[nodiscard]] TcpTransport& tcp() noexcept { return tcp_; }
    [[nodiscard]] const TcpTransport& tcp() const noexcept { return tcp_; }

    /// Raw SSL object (advanced use).
    [[nodiscard]] ssl_st* ssl() noexcept { return ssl_; }

private:
    TcpTransport tcp_;
    ssl_st* ssl_ = nullptr;
    bool is_server_ = false;
    mutable std::string cached_alpn_;
    mutable int peer_verified_ = -1; // -1 = unset, 0 = fail, 1 = success
};

// ============================================================================
// TransportFactory — owns SSL_CTX, creates & wraps transports
// ============================================================================

/// Central factory for transports and TLS context management.
///
/// Initialise once with \\c init_tls_server() or \\c init_tls_client(),
/// then create outbound transports or wrap accepted connections.
/// The factory must outlive all TLS transports it creates.
class TransportFactory {
public:
    TransportFactory();
    ~TransportFactory();

    TransportFactory(const TransportFactory&) = delete;
    TransportFactory& operator=(const TransportFactory&) = delete;
    TransportFactory(TransportFactory&&) noexcept;
    TransportFactory& operator=(TransportFactory&&) noexcept;

    // --- TLS context initialisation ----------------------------------------

    /// Initialise as a TLS server.
    ///
    /// \\param cert_chain_file       PEM certificate chain.
    /// \\param private_key_file      PEM private key.
    /// \\param private_key_pass      Optional key passphrase.
    /// \\param ca_cert_file          CA bundle for mTLS (empty = no client verify).
    /// \\param verify_depth          Max certificate chain depth.
    /// \\param ciphers               Colon-delimited cipher list.
    /// \\param alpn_protocols        ALPN wire-format protocol list.
    /// \\param session_cache_size    Session cache entries (0 = disable).
    /// \\param session_ticket_lifetime_sec  Ticket lifetime (0 = disable tickets).
    ///
    /// Returns 0 on success, or an OpenSSL error code.
    [[nodiscard]] int init_tls_server(
        const std::string& cert_chain_file,
        const std::string& private_key_file,
        const std::string& private_key_pass = {},
        const std::string& ca_cert_file = {},
        int verify_depth = 4,
        const std::string& ciphers = kDefaultTlsCiphers,
        const std::string& alpn_protocols = kDefaultAlpnProtocols,
        size_t session_cache_size = kDefaultSessionCacheSize,
        int session_ticket_lifetime_sec = kDefaultSessionTicketLifetimeSec);

    /// Initialise as a TLS client.
    ///
    /// \\param ca_cert_file             CA bundle (empty = system trust store).
    /// \\param client_cert_chain_file   Client cert for mTLS (optional).
    /// \\param client_private_key_file  Client key for mTLS (optional).
    /// \\param client_private_key_pass  Key passphrase (optional).
    /// \\param ciphers                  Cipher list.
    /// \\param alpn_protocols           ALPN wire-format protocol list.
    ///
    /// Returns 0 on success, or an OpenSSL error code.
    [[nodiscard]] int init_tls_client(
        const std::string& ca_cert_file = {},
        const std::string& client_cert_chain_file = {},
        const std::string& client_private_key_file = {},
        const std::string& client_private_key_pass = {},
        const std::string& ciphers = kDefaultTlsCiphers,
        const std::string& alpn_protocols = kDefaultAlpnProtocols);

    /// Rotate the TLS session ticket encryption key (server-side only).
    /// Generates a new random key; old keys remain valid for graceful transition.
    /// Returns 0 on success.
    [[nodiscard]] int rotate_session_ticket_key();

    // --- Transport creation ------------------------------------------------

    /// Create a TcpTransport and connect to \\c remote.
    [[nodiscard]] std::unique_ptr<TcpTransport> create_tcp(
        const endpoint& remote,
        const TransportConfig& config = {}) const;

    /// Create a TlsTransport (TLS client) and connect to \\c remote.
    /// Requires prior \\c init_tls_client().
    [[nodiscard]] std::unique_ptr<TlsTransport> create_tls(
        const endpoint& remote,
        const TransportConfig& config = {}) const;

    /// Wrap an accepted TcpTransport into a TlsTransport (server-side).
    /// Requires prior \\c init_tls_server().
    [[nodiscard]] std::unique_ptr<TlsTransport> wrap_tls(
        TcpTransport tcp) const;

    // --- Accessors ---------------------------------------------------------

    /// \\c true if the TLS context has been initialised.
    [[nodiscard]] bool tls_initialised() const noexcept { return ssl_ctx_ != nullptr; }

    /// \\c true if configured for server mode.
    [[nodiscard]] bool is_server_mode() const noexcept { return is_server_mode_; }

    /// Raw SSL_CTX (advanced use).
    [[nodiscard]] ssl_ctx_st* ssl_ctx() noexcept { return ssl_ctx_; }

private:
    ssl_ctx_st* ssl_ctx_ = nullptr;
    bool is_server_mode_ = false;

    /// OpenSSL ALPN selection callback (server side).
    static int alpn_select_callback(
        ssl_st* ssl,
        const unsigned char** out,
        unsigned char* outlen,
        const unsigned char* in,
        unsigned int inlen,
        void* arg);
};

// ============================================================================
// AcceptorConfig
// ============================================================================

/// Server-side acceptor configuration.
struct AcceptorConfig {
    /// Address and port to bind.
    endpoint bind_address{};

    /// Listen backlog (0 = SOMAXCONN).
    int listen_backlog = 0;

    /// Non-blocking accept mode.
    bool non_blocking = true;

    /// SO_REUSEADDR for rapid restarts.
    bool reuse_address = true;

    /// SO_REUSEPORT for multi-process load balancing.
    bool reuse_port = false;

    /// Transport options applied to every accepted connection.
    TransportConfig transport_config{};

    /// TCP_DEFER_ACCEPT — delay accept until data arrives (Linux).
    bool defer_accept = false;

    /// Defer-accept timeout in seconds.
    int defer_accept_timeout_sec = 1;
};

// ============================================================================
// ConnectionHandler
// ============================================================================

/// Callback for each accepted connection.
/// The handler receives a connected TcpTransport; it may promote it to
/// TlsTransport via TransportFactory::wrap_tls().
using ConnectionHandler = std::function<void(TcpTransport transport)>;

// ============================================================================
// Acceptor
// ============================================================================

/// Server-side TCP connection acceptor.
///
/// Binds, listens, and delivers accepted connections via a callback.
/// Supports both blocking and non-blocking mode (AcceptorConfig::non_blocking).
class Acceptor final {
public:
    /// Bind and listen per \\c config.  Throws std::system_error on failure.
    explicit Acceptor(const AcceptorConfig& config);

    ~Acceptor();

    Acceptor(const Acceptor&) = delete;
    Acceptor& operator=(const Acceptor&) = delete;
    Acceptor(Acceptor&& other) noexcept;
    Acceptor& operator=(Acceptor&& other) noexcept;

    // --- Acceptance --------------------------------------------------------

    /// Accept one connection and dispatch to \\c handler.
    /// Blocking: blocks until a connection arrives.
    /// Non-blocking: returns immediately; caller polls fd and retries on EWOULDBLOCK.
    /// Returns \\c true if a connection was accepted and dispatched.
    [[nodiscard]] bool accept_one(const ConnectionHandler& handler);

    /// Set a persistent connection handler invoked by accept_one().
    void set_handler(ConnectionHandler handler) { handler_ = std::move(handler); }

    // --- State -------------------------------------------------------------

    /// Listening fd — poll with epoll / kqueue / select.
    [[nodiscard]] int fd() const noexcept { return fd_; }

    /// \\c true if bound and listening.
    [[nodiscard]] bool listening() const noexcept { return fd_ >= 0; }

    /// The bound local endpoint.
    [[nodiscard]] endpoint bound_address() const noexcept { return bound_; }

    /// Close the listening socket.
    void close();

private:
    int fd_ = -1;
    endpoint bound_{};
    AcceptorConfig config_{};
    ConnectionHandler handler_;
};

} // namespace torrent::network
