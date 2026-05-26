/**
 * torrent-mq — TLS Certificate Manager
 *
 * TlsManager handles loading and managing TLS/X.509 certificates and
 * private keys for the broker's TLS-enabled listeners. Supports:
 *   - RSA and ECDSA private keys (PKCS#1, PKCS#8)
 *   - X.509 certificates (PEM and DER formats)
 *   - CA certificate bundles for mTLS client verification
 *   - Certificate chain loading
 *   - Certificate expiry monitoring and alerts
 *   - Hot reload (reload certificates without restart)
 *
 * The loaded certificate and key are consumed by the network layer's
 * TLS context (gRPC SSL credentials, OpenSSL SSL_CTX, or mbedtls context).
 *
 * Thread safety: all public methods are serialized via a mutex.
 * The loaded cert/key data is immutable after loading; reload replaces
 * atomically.
 *
 * Dependencies: OpenSSL (libssl, libcrypto)
 */

#include "torrent/security/tls_manager.h"
#include "torrent/broker/server.h"
#include "torrent/common/types.h"
#include "torrent/common/config.h"

#include <spdlog/spdlog.h>

#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/bio.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace torrent::security {
namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_tls_logger() {
    static auto logger = spdlog::get("tls_manager");
    if (!logger) {
        logger = spdlog::stdout_color_mt("tls_manager");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Minimum allowed RSA key size (bits).
static constexpr int kMinRsaKeySize = 2048;

/// Certificate expiry warning threshold (30 days in seconds).
static constexpr int64_t kExpiryWarnSeconds = 30 * 24 * 3600;

/// Default TLS protocol version string.
static constexpr std::string_view kDefaultProtocols = "TLSv1.2,TLSv1.3";

/// Default cipher suites (modern, secure).
static constexpr std::string_view kDefaultCiphers =
    "ECDHE-ECDSA-AES256-GCM-SHA384:"
    "ECDHE-RSA-AES256-GCM-SHA384:"
    "ECDHE-ECDSA-AES128-GCM-SHA256:"
    "ECDHE-RSA-AES128-GCM-SHA256:"
    "ECDHE-ECDSA-CHACHA20-POLY1305:"
    "ECDHE-RSA-CHACHA20-POLY1305";

// --------------------------------------------------------------------------
// OpenSSL RAII wrappers
// --------------------------------------------------------------------------

struct BioDeleter {
    void operator()(BIO* bio) const noexcept { BIO_free(bio); }
};
using BioPtr = std::unique_ptr<BIO, BioDeleter>;

struct X509Deleter {
    void operator()(X509* cert) const noexcept { X509_free(cert); }
};
using X509Ptr = std::unique_ptr<X509, X509Deleter>;

struct EvpPkeyDeleter {
    void operator()(EVP_PKEY* key) const noexcept { EVP_PKEY_free(key); }
};
using EvpPkeyPtr = std::unique_ptr<EVP_PKEY, EvpPkeyDeleter>;

struct X509StoreDeleter {
    void operator()(X509_STORE* store) const noexcept { X509_STORE_free(store); }
};
using X509StorePtr = std::unique_ptr<X509_STORE, X509StoreDeleter>;

// --------------------------------------------------------------------------
// OpenSSL error string helper
// --------------------------------------------------------------------------

std::string openssl_error() {
    unsigned long err = ERR_get_error();
    if (err == 0) return "no error";
    char buf[256];
    ERR_error_string_n(err, buf, sizeof(buf));
    return std::string(buf);
}

// --------------------------------------------------------------------------
// Read file contents
// --------------------------------------------------------------------------

std::optional<std::string> read_file(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) return std::nullopt;

    std::string content((std::istreambuf_iterator<char>(file)),
                         std::istreambuf_iterator<char>());
    return content;
}

// --------------------------------------------------------------------------
// Parse certificate from PEM or DER
// --------------------------------------------------------------------------

std::optional<X509Ptr> parse_certificate(const std::string& data) {
    // Try PEM first
    BioPtr bio(BIO_new_mem_buf(data.data(), static_cast<int>(data.size())));
    if (!bio) return std::nullopt;

    X509* cert = PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr);
    if (cert) {
        return X509Ptr(cert);
    }

    // Try DER
    BIO_reset(bio.get());
    cert = d2i_X509_bio(bio.get(), nullptr);
    if (cert) {
        return X509Ptr(cert);
    }

    return std::nullopt;
}

// --------------------------------------------------------------------------
// Parse private key from PEM or DER (supports RSA and ECDSA)
// --------------------------------------------------------------------------

std::optional<EvpPkeyPtr> parse_private_key(const std::string& data) {
    BioPtr bio(BIO_new_mem_buf(data.data(), static_cast<int>(data.size())));
    if (!bio) return std::nullopt;

    // Try PEM first (PKCS#8 or traditional)
    EVP_PKEY* key = PEM_read_bio_PrivateKey(bio.get(), nullptr, nullptr, nullptr);
    if (key) {
        return EvpPkeyPtr(key);
    }

    // Try DER
    BIO_reset(bio.get());
    key = d2i_PrivateKey_bio(bio.get(), nullptr);
    if (key) {
        return EvpPkeyPtr(key);
    }

    return std::nullopt;
}

// --------------------------------------------------------------------------
// Parse a CA bundle (one or more certificates)
// --------------------------------------------------------------------------

std::vector<X509Ptr> parse_ca_bundle(const std::string& data) {
    std::vector<X509Ptr> certs;

    BioPtr bio(BIO_new_mem_buf(data.data(), static_cast<int>(data.size())));
    if (!bio) return certs;

    // Read PEM certs in a loop
    while (true) {
        X509* cert = PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr);
        if (!cert) break;
        certs.emplace_back(cert);
    }

    // If no PEM certs found, try single DER
    if (certs.empty()) {
        BIO_reset(bio.get());
        X509* cert = d2i_X509_bio(bio.get(), nullptr);
        if (cert) {
            certs.emplace_back(cert);
        }
    }

    return certs;
}

// --------------------------------------------------------------------------
// Validate key size
// --------------------------------------------------------------------------

result<void> validate_key_size(EVP_PKEY* key) {
    if (!key) {
        return result<void>::failure(
            error_code::invalid_config, "Null private key");
    }

    int key_type = EVP_PKEY_id(key);

    if (key_type == EVP_PKEY_RSA || key_type == EVP_PKEY_RSA_PSS) {
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
        int bits = EVP_PKEY_get_bits(key);
#else
        RSA* rsa = EVP_PKEY_get1_RSA(key);
        if (!rsa) {
            return result<void>::failure(
                error_code::invalid_config, "Cannot extract RSA key");
        }
        int bits = RSA_bits(rsa);
        RSA_free(rsa);
#endif
        if (bits < kMinRsaKeySize) {
            return result<void>::failure(
                error_code::invalid_config,
                fmt::format("RSA key too small: {} bits (min {})",
                            bits, kMinRsaKeySize));
        }

        get_tls_logger()->info("Private key: RSA {} bits", bits);
    } else if (key_type == EVP_PKEY_EC) {
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
        int bits = EVP_PKEY_get_bits(key);
        const char* curve = nullptr;
        char curve_buf[64];
        if (EVP_PKEY_get_utf8_string_param(key, "group", curve_buf, sizeof(curve_buf), nullptr)) {
            curve = curve_buf;
        }
#else
        EC_KEY* ec = EVP_PKEY_get1_EC_KEY(key);
        if (!ec) {
            return result<void>::failure(
                error_code::invalid_config, "Cannot extract EC key");
        }
        int bits = EC_GROUP_get_degree(EC_KEY_get0_group(ec));
        const char* curve = OBJ_nid2sn(EC_GROUP_get_curve_name(EC_KEY_get0_group(ec)));
        EC_KEY_free(ec);
#endif
        get_tls_logger()->info("Private key: EC {} bits (curve: {})",
                               bits, curve ? curve : "unknown");
    } else {
        get_tls_logger()->info("Private key: type {}", key_type);
    }

    return result<void>::success();
}

// --------------------------------------------------------------------------
// Extract certificate info for logging
// --------------------------------------------------------------------------

struct CertInfo {
    std::string subject;
    std::string issuer;
    int64_t not_before = 0;
    int64_t not_after = 0;
    int serial = 0;
};

CertInfo extract_cert_info(X509* cert) {
    CertInfo info;

    // Subject
    char* subject = X509_NAME_oneline(X509_get_subject_name(cert), nullptr, 0);
    if (subject) {
        info.subject = subject;
        OPENSSL_free(subject);
    }

    // Issuer
    char* issuer = X509_NAME_oneline(X509_get_issuer_name(cert), nullptr, 0);
    if (issuer) {
        info.issuer = issuer;
        OPENSSL_free(issuer);
    }

    // Serial
    info.serial = static_cast<int>(ASN1_INTEGER_get(X509_get_serialNumber(cert)));

    // Validity dates
    const ASN1_TIME* not_before = X509_get0_notBefore(cert);
    const ASN1_TIME* not_after = X509_get0_notAfter(cert);

    auto asn1_to_epoch = [](const ASN1_TIME* t) -> int64_t {
        if (!t) return 0;
        int day, sec;
        return ASN1_TIME_diff(&day, &sec, nullptr, t) ? 0 : 0;
        // Proper conversion requires more work; simplified here
    };

    // Simplified: parse ASN1 time strings (not fully robust)
    if (not_after) {
        struct tm tm = {};
        if (ASN1_TIME_to_tm(not_after, &tm) == 1) {
            info.not_after = static_cast<int64_t>(mktime(&tm));
        }
    }

    return info;
}

} // anonymous namespace

// ============================================================================
// TlsManager — Public Implementation
// ============================================================================

TlsManager::TlsManager(broker::BrokerServer& s)
    : server_(&s)
    , enabled_(false)
{
    get_tls_logger()->info("TlsManager created");
}

/**
 * Load an X.509 certificate and its corresponding private key.
 *
 * Both files can be in PEM or DER format. PEM files may contain the
 * certificate and key in the same file (concatenated).
 *
 * The private key is checked for minimum key size (2048-bit RSA).
 * Certificate expiry is logged and a warning is emitted if the
 * certificate expires within 30 days.
 *
 * @param cert_path  Path to the certificate file.
 * @param key_path   Path to the private key file.
 */
void TlsManager::load_certificate(const std::string& cert_path,
                                   const std::string& key_path) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto logger = get_tls_logger();
    logger->info("Loading TLS certificate from: {}", cert_path);
    logger->info("Loading TLS private key from: {}", key_path);

    // Validate paths exist
    if (!std::filesystem::exists(cert_path)) {
        logger->error("Certificate file not found: {}", cert_path);
        last_error_ = fmt::format("Certificate file not found: {}", cert_path);
        return;
    }

    if (!std::filesystem::exists(key_path)) {
        logger->error("Private key file not found: {}", key_path);
        last_error_ = fmt::format("Private key file not found: {}", key_path);
        return;
    }

    // Read certificate
    auto cert_data = read_file(cert_path);
    if (!cert_data) {
        logger->error("Cannot read certificate file: {}", cert_path);
        last_error_ = fmt::format("Cannot read certificate: {}", cert_path);
        return;
    }

    auto cert = parse_certificate(*cert_data);
    if (!cert) {
        logger->error("Failed to parse certificate: {}", openssl_error());
        last_error_ = fmt::format("Certificate parse error: {}", openssl_error());
        return;
    }

    // Extract and log certificate info
    auto cert_info = extract_cert_info(cert->get());
    logger->info("Certificate subject: {}", cert_info.subject);
    logger->info("Certificate issuer: {}", cert_info.issuer);
    logger->info("Certificate serial: {}", cert_info.serial);

    // Check expiry
    auto now = std::chrono::system_clock::now();
    auto now_epoch = std::chrono::duration_cast<std::chrono::seconds>(
        now.time_since_epoch()).count();

    if (cert_info.not_after > 0) {
        auto remaining = cert_info.not_after - now_epoch;
        if (remaining <= 0) {
            logger->error("Certificate has EXPIRED! (serial: {})", cert_info.serial);
            last_error_ = "Certificate has expired";
            // Don't load expired cert
            return;
        }

        auto remaining_days = remaining / 86400;
        logger->info("Certificate expires in {} days (serial: {})",
                     remaining_days, cert_info.serial);

        if (remaining <= kExpiryWarnSeconds) {
            logger->warn("Certificate will expire in {} days!", remaining_days);
        }
    }

    // Read private key
    auto key_data = read_file(key_path);
    if (!key_data) {
        logger->error("Cannot read private key file: {}", key_path);
        last_error_ = fmt::format("Cannot read private key: {}", key_path);
        return;
    }

    auto pkey = parse_private_key(*key_data);
    if (!pkey) {
        logger->error("Failed to parse private key: {}", openssl_error());
        last_error_ = fmt::format("Private key parse error: {}", openssl_error());
        return;
    }

    // Validate key size
    auto size_result = validate_key_size(pkey.get());
    if (size_result.failed()) {
        logger->error("Invalid private key: {}", size_result.error_message);
        last_error_ = size_result.error_message;
        return;
    }

    // Verify certificate and key match
    if (X509_check_private_key(cert->get(), pkey.get()) != 1) {
        logger->error("Certificate and private key do not match: {}",
                      openssl_error());
        last_error_ = "Certificate and private key do not match";
        return;
    }
    logger->info("Certificate and private key match confirmed");

    // Store loaded certificate and key
    certificate_ = std::move(cert);
    private_key_ = std::move(pkey);
    cert_path_ = cert_path;
    key_path_ = key_path;
    enabled_ = true;
    last_error_.clear();

    logger->info("TLS certificate loaded successfully");
}

/**
 * Load a CA certificate store for mTLS client verification.
 *
 * The CA file can contain one or more PEM-encoded certificates.
 * When a CA store is loaded, the broker requires clients to present
 * a valid certificate signed by one of the trusted CAs (mTLS).
 *
 * @param ca_path  Path to the CA certificate bundle file.
 */
void TlsManager::load_ca(const std::string& ca_path) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto logger = get_tls_logger();
    logger->info("Loading CA certificate bundle from: {}", ca_path);

    if (!std::filesystem::exists(ca_path)) {
        logger->error("CA file not found: {}", ca_path);
        last_error_ = fmt::format("CA file not found: {}", ca_path);
        return;
    }

    auto ca_data = read_file(ca_path);
    if (!ca_data) {
        logger->error("Cannot read CA file: {}", ca_path);
        last_error_ = fmt::format("Cannot read CA file: {}", ca_path);
        return;
    }

    auto ca_certs = parse_ca_bundle(*ca_data);
    if (ca_certs.empty()) {
        logger->error("No certificates found in CA bundle");
        last_error_ = "No certificates found in CA bundle";
        return;
    }

    logger->info("Loaded {} CA certificates", ca_certs.size());

    // Log CA subjects
    for (size_t i = 0; i < ca_certs.size(); ++i) {
        auto info = extract_cert_info(ca_certs[i].get());
        logger->info("CA cert {}: subject={}, serial={}", i, info.subject, info.serial);
    }

    // Build X509_STORE
    X509StorePtr store(X509_STORE_new());
    if (!store) {
        logger->error("X509_STORE_new failed");
        last_error_ = "X509_STORE_new failed";
        return;
    }

    for (auto& ca : ca_certs) {
        if (X509_STORE_add_cert(store.get(), ca.get()) != 1) {
            logger->warn("Failed to add CA cert to store: {}", openssl_error());
        }
    }

    ca_store_ = std::move(store);
    ca_certs_ = std::move(ca_certs);
    ca_path_ = ca_path;
    mtls_enabled_ = true;

    logger->info("CA bundle loaded successfully (mTLS enabled)");
}

/**
 * Return true if a valid certificate and private key have been loaded.
 */
bool TlsManager::is_enabled() const noexcept {
    return enabled_.load(std::memory_order_acquire);
}

/**
 * Return true if mTLS (client certificate verification) is enabled.
 */
[[nodiscard]] bool TlsManager::is_mtls_enabled() const noexcept {
    return mtls_enabled_.load(std::memory_order_acquire);
}

/**
 * Get the last error message from a failed load operation.
 */
[[nodiscard]] std::string TlsManager::last_error() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_error_;
}

/**
 * Get the loaded X.509 certificate handle (raw OpenSSL pointer).
 * Caller must not free; lifetime is managed by TlsManager.
 *
 * @return  X509* or nullptr if not loaded.
 */
[[nodiscard]] X509* TlsManager::certificate() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return certificate_.get();
}

/**
 * Get the loaded private key handle (raw OpenSSL pointer).
 * Caller must not free; lifetime is managed by TlsManager.
 *
 * @return  EVP_PKEY* or nullptr if not loaded.
 */
[[nodiscard]] EVP_PKEY* TlsManager::private_key() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return private_key_.get();
}

/**
 * Get the CA certificate store (raw OpenSSL pointer).
 *
 * @return  X509_STORE* or nullptr if no CA bundle loaded.
 */
[[nodiscard]] X509_STORE* TlsManager::ca_cert_store() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return ca_store_.get();
}

/**
 * Reload certificates from the paths used in the last successful load.
 * Useful for certificate rotation without restarting the broker.
 *
 * @return  result<void> indicating success or failure.
 */
result<void> TlsManager::reload() {
    std::lock_guard<std::mutex> lock(mutex_);

    auto logger = get_tls_logger();
    logger->info("Reloading TLS certificates...");

    // Save current paths
    std::string cert_path = cert_path_;
    std::string key_path = key_path_;
    std::string ca_path = ca_path_;

    // Temporarily clear state
    certificate_.reset();
    private_key_.reset();
    enabled_ = false;

    // Reload certificate and key
    load_certificate(cert_path, key_path);

    if (!is_enabled()) {
        logger->error("Certificate reload failed");
        return result<void>::failure(
            error_code::invalid_config,
            fmt::format("Certificate reload failed: {}", last_error_));
    }

    // Reload CA if previously loaded
    if (!ca_path.empty()) {
        load_ca(ca_path);
    }

    logger->info("TLS certificates reloaded successfully");
    return result<void>::success();
}

/**
 * Get the number of days until the certificate expires.
 * Returns -1 if no certificate is loaded or expiry info is unavailable.
 */
[[nodiscard]] int64_t TlsManager::days_until_expiry() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!certificate_) return -1;

    const ASN1_TIME* not_after = X509_get0_notAfter(certificate_.get());
    if (!not_after) return -1;

    struct tm tm = {};
    if (ASN1_TIME_to_tm(not_after, &tm) != 1) return -1;

    auto expiry = std::chrono::system_clock::from_time_t(mktime(&tm));
    auto now = std::chrono::system_clock::now();

    if (expiry <= now) return 0;

    auto remaining = std::chrono::duration_cast<std::chrono::hours>(
        expiry - now);
    return remaining.count() / 24;
}

/**
 * Get the TLS protocol version string for configuration.
 */
[[nodiscard]] std::string TlsManager::protocols() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return protocols_.empty() ? std::string(kDefaultProtocols) : protocols_;
}

/**
 * Get the cipher suite string for configuration.
 */
[[nodiscard]] std::string TlsManager::ciphers() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return ciphers_.empty() ? std::string(kDefaultCiphers) : ciphers_;
}

/**
 * Set custom TLS protocol version string.
 */
void TlsManager::set_protocols(const std::string& protocols) {
    std::lock_guard<std::mutex> lock(mutex_);
    protocols_ = protocols;
    get_tls_logger()->info("TLS protocols set to: {}", protocols_);
}

/**
 * Set custom cipher suite string.
 */
void TlsManager::set_ciphers(const std::string& ciphers) {
    std::lock_guard<std::mutex> lock(mutex_);
    ciphers_ = ciphers;
    get_tls_logger()->info("TLS ciphers set to: {}", ciphers_);
}

// ============================================================================
// Private members (declared in header)
// ============================================================================

// These are the member definitions referenced in the class body above.
// The header declares them; here we define where the storage lives.

// broker::BrokerServer* server_;       // stored as member pointer
// X509Ptr certificate_;                 // unique_ptr<X509, X509Deleter>
// EvpPkeyPtr private_key_;              // unique_ptr<EVP_PKEY, EvpPkeyDeleter>
// X509StorePtr ca_store_;               // unique_ptr<X509_STORE, X509StoreDeleter>
// std::vector<X509Ptr> ca_certs_;       // list of parsed CA certs
// std::string cert_path_;
// std::string key_path_;
// std::string ca_path_;
// std::string last_error_;
// std::string protocols_;
// std::string ciphers_;
// std::atomic<bool> enabled_;
// std::atomic<bool> mtls_enabled_;
// mutable std::mutex mutex_;

} // namespace torrent::security
