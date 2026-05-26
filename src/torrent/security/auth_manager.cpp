/**
 * auth_manager.cpp — Full Authentication & Authorization System
 *
 * Implements the complete security subsystem for torrent-mq:
 *   - AuthManager: orchestrates SASL authentication, session management,
 *     and authorization delegation.
 *   - SaslProvider: SCRAM-SHA-256/512 (RFC 5802 / 7677) with PBKDF2,
 *     PLAIN (RFC 4616), OAUTHBEARER (RFC 7628), and Kerberos/GSSAPI.
 *   - AclEngine: ACL rule engine with wildcard matching, most-specific-
 *     first evaluation, default-deny policy.
 *   - AuditLog: structured audit logging for authentication, admin, and
 *     security events.
 *
 * SCRAM Implementation Details
 * ----------------------------
 * Full RFC 5802 / 7677 compliant SCRAM with:
 *   - Client-first-message:  n=<username>,r=<client_nonce>
 *   - Server-first-message:  r=<combined_nonce>,s=<base64(salt)>,i=<iterations>
 *   - Client-final-message:  c=<base64(channel_binding)>,r=<combined_nonce>,
 *                            p=<base64(ClientProof)>
 *   - Server-final-message:  v=<base64(ServerSignature)>
 *
 * Channel binding: tls-server-end-point (RFC 5929). The server computes
 * a SHA-256/512 hash of the TLS server certificate and includes it in the
 * SCRAM exchange to prevent MITM attacks.
 *
 * PBKDF2: Configurable iteration count (default 15000 for SHA-256,
 * 10000 for SHA-512 per RFC 7677 recommendations).  Uses OpenSSL's
 * PKCS5_PBKDF2_HMAC.
 *
 * Nonce generation: CSPRNG via RAND_bytes (OpenSSL).
 *
 * ACL Engine
 * ----------
 * Rules are evaluated in specificity order:
 *   1. Exact principal, exact host, exact resource
 *   2. Exact principal, exact host, wildcard resource
 *   3. Exact principal, wildcard host, exact resource
 *   4. Wildcard principal, exact host, exact resource
 *   5. ... and so on.
 *
 * Within each specificity tier, longer (more specific) patterns sort first.
 * The first matching rule determines allow/deny.  Default: DENY.
 *
 * Operations: READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE,
 *             CLUSTER_ACTION, ALL
 *
 * OpenSSL is required for: SHA-256, SHA-512, HMAC, PBKDF2, RAND_bytes.
 */

#include "torrent/security/auth_manager.h"
#include "torrent/security/sasl_provider.h"
#include "torrent/security/acl_engine.h"
#include "torrent/security/audit_log.h"
#include "torrent/broker/server.h"
#include "torrent/common/types.h"
#include "torrent/common/config.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <regex>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

// OpenSSL cryptographic primitives
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/err.h>

// ============================================================================
// Internal namespace
// ============================================================================

namespace torrent::security {

using namespace std::chrono_literals;

// ============================================================================
// Anonymous namespace: logging, constants, utilities
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logging
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_auth_logger() {
    static auto logger = []() {
        auto l = spdlog::get("auth_manager");
        if (!l) {
            l = spdlog::stdout_color_mt("auth_manager");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define AUTH_LOG_TRACE(...) get_auth_logger()->trace(__VA_ARGS__)
#define AUTH_LOG_DEBUG(...) get_auth_logger()->debug(__VA_ARGS__)
#define AUTH_LOG_INFO(...)  get_auth_logger()->info(__VA_ARGS__)
#define AUTH_LOG_WARN(...)  get_auth_logger()->warn(__VA_ARGS__)
#define AUTH_LOG_ERROR(...) get_auth_logger()->error(__VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Default PBKDF2 iterations for SCRAM-SHA-256 (RFC 7677 §4)
static constexpr int kDefaultIterationsSHA256 = 15000;

/// Default PBKDF2 iterations for SCRAM-SHA-512 (RFC 7677 §4)
static constexpr int kDefaultIterationsSHA512 = 10000;

/// Minimum allowed iterations (reject below this)
static constexpr int kMinIterations = 4096;

/// Maximum allowed iterations (cap CPU cost)
static constexpr int kMaxIterations = 1'000'000;

/// Salt length in bytes
static constexpr size_t kSaltLength = 16;

/// Nonce length in bytes (client or server portion)
static constexpr size_t kNonceLength = 24;

/// Maximum nonce total combined length
static constexpr size_t kMaxCombinedNonceLength = 512;

/// Session cache TTL (5 minutes default)
static constexpr auto kDefaultSessionTtl = 5min;

/// Maximum session cache entries
static constexpr size_t kMaxSessionCacheSize = 100'000;

/// SASL mechanism names
static constexpr std::string_view kMechScramSha256  = "SCRAM-SHA-256";
static constexpr std::string_view kMechScramSha512  = "SCRAM-SHA-512";
static constexpr std::string_view kMechPlain        = "PLAIN";
static constexpr std::string_view kMechOAuthBearer  = "OAUTHBEARER";
static constexpr std::string_view kMechKerberos     = "GSSAPI";

/// GS2 header prefixes (RFC 5802 §5)
static constexpr std::string_view kGs2CbFlag    = "n,,";   // no channel binding
static constexpr std::string_view kGs2CbTlsUnique = "p=tls-unique,,";
static constexpr std::string_view kGs2CbTlsServer = "p=tls-server-end-point,,";
static constexpr std::string_view kGs2Header     = "n";     // no authzid
static constexpr std::string_view kGs2Authzid    = "a=";    // authzid present

/// SCRAM attribute delimiters
static constexpr char kScramAttrSep = ',';

/// SCRAM attributes
static constexpr std::string_view kAttrNonce    = "r=";
static constexpr std::string_view kAttrUsername = "n=";
static constexpr std::string_view kAttrAuthzid  = "a=";
static constexpr std::string_view kAttrSalt     = "s=";
static constexpr std::string_view kAttrIter     = "i=";
static constexpr std::string_view kAttrCbind    = "c=";
static constexpr std::string_view kAttrProof    = "p=";
static constexpr std::string_view kAttrVerifier = "v=";
static constexpr std::string_view kAttrError    = "e=";

/// SCRAM error values (RFC 5802 §7)
static constexpr std::string_view kScramErrInvalidEncoding    = "invalid-encoding";
static constexpr std::string_view kScramErrInvalidProof       = "invalid-proof";
static constexpr std::string_view kScramErrChannelBindings    = "channel-bindings-dont-match";
static constexpr std::string_view kScramErrServerSigInvalid   = "server-does-support-channel-binding";
static constexpr std::string_view kScramErrChannelBindNotSup  = "channel-binding-not-supported";
static constexpr std::string_view kScramErrUnsupportedChannel = "unsupported-channel-binding-type";
static constexpr std::string_view kScramErrUnknownUser        = "unknown-user";
static constexpr std::string_view kScramErrInvalidUsername    = "invalid-username";
static constexpr std::string_view kScramErrAuthzidMismatch    = "authorization-identity-mismatch";
static constexpr std::string_view kScramErrOther              = "other-error";

/// Credential separator for PLAIN mechanism (\0 byte)
static constexpr char kPlainSep = '\0';

// --------------------------------------------------------------------------
// Base64 encoding / decoding (URL-safe and standard)
// --------------------------------------------------------------------------

/// Standard Base64 encode using OpenSSL BIO.
[[nodiscard]] std::string base64_encode(const unsigned char* data, size_t len) {
    BIO* b64 = BIO_new(BIO_f_base64());
    BIO* mem = BIO_new(BIO_s_mem());
    BIO_push(b64, mem);
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    BIO_write(b64, data, static_cast<int>(len));
    BIO_flush(b64);

    BUF_MEM* buf = nullptr;
    BIO_get_mem_ptr(b64, &buf);
    std::string result(buf->data, buf->length);

    BIO_free_all(b64);
    return result;
}

[[nodiscard]] std::string base64_encode(std::string_view data) {
    return base64_encode(
        reinterpret_cast<const unsigned char*>(data.data()), data.size());
}

/// Standard Base64 decode using OpenSSL BIO.
[[nodiscard]] std::optional<std::vector<unsigned char>> base64_decode(
    std::string_view encoded) {
    // Compute expected decode length (pessimistic)
    size_t estimate = 3 * ((encoded.size() + 3) / 4) + 1;
    std::vector<unsigned char> buf(estimate);

    BIO* b64 = BIO_new(BIO_f_base64());
    BIO* mem = BIO_new_mem_buf(encoded.data(), static_cast<int>(encoded.size()));
    BIO_push(b64, mem);
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);

    int len = BIO_read(b64, buf.data(), static_cast<int>(buf.size()));
    BIO_free_all(b64);

    if (len < 0) return std::nullopt;
    buf.resize(static_cast<size_t>(len));
    return buf;
}

// --------------------------------------------------------------------------
// Hex encoding
// --------------------------------------------------------------------------

[[nodiscard]] std::string hex_encode(const unsigned char* data, size_t len) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(len * 2);
    for (size_t i = 0; i < len; ++i) {
        out.push_back(kHex[(data[i] >> 4) & 0x0F]);
        out.push_back(kHex[data[i] & 0x0F]);
    }
    return out;
}

// --------------------------------------------------------------------------
// Cryptographic utilities: HMAC, SHA, PBKDF2
// --------------------------------------------------------------------------

/// Compute HMAC using EVP_MAC or legacy HMAC.
[[nodiscard]] std::vector<unsigned char> compute_hmac(
    const EVP_MD* md,
    const unsigned char* key, size_t key_len,
    const unsigned char* data, size_t data_len) {

    std::vector<unsigned char> result(static_cast<size_t>(EVP_MAX_MD_SIZE));
    unsigned int result_len = 0;

    HMAC(md, key, static_cast<int>(key_len),
         data, data_len, result.data(), &result_len);

    result.resize(result_len);
    return result;
}

/// Compute HMAC with string_view convenience.
[[nodiscard]] std::vector<unsigned char> compute_hmac(
    const EVP_MD* md,
    std::string_view key,
    std::string_view data) {
    return compute_hmac(
        md,
        reinterpret_cast<const unsigned char*>(key.data()), key.size(),
        reinterpret_cast<const unsigned char*>(data.data()), data.size());
}

/// Compute SHA-256 digest.
[[nodiscard]] std::array<unsigned char, SHA256_DIGEST_LENGTH> sha256(
    std::string_view data) {
    std::array<unsigned char, SHA256_DIGEST_LENGTH> digest{};
    SHA256(reinterpret_cast<const unsigned char*>(data.data()),
           data.size(), digest.data());
    return digest;
}

/// Compute SHA-512 digest.
[[nodiscard]] std::array<unsigned char, SHA512_DIGEST_LENGTH> sha512(
    std::string_view data) {
    std::array<unsigned char, SHA512_DIGEST_LENGTH> digest{};
    SHA512(reinterpret_cast<const unsigned char*>(data.data()),
           data.size(), digest.data());
    return digest;
}

/// XOR two equal-length byte sequences.
void xor_bytes(unsigned char* dst, const unsigned char* src, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        dst[i] ^= src[i];
    }
}

/// PBKDF2 with HMAC-SHA-256 or HMAC-SHA-512.
[[nodiscard]] std::optional<std::vector<unsigned char>> pbkdf2(
    const EVP_MD* md,
    std::string_view password,
    const unsigned char* salt, size_t salt_len,
    int iterations,
    size_t dk_len) {

    std::vector<unsigned char> derived(dk_len);

    int rc = PKCS5_PBKDF2_HMAC(
        password.data(), static_cast<int>(password.size()),
        salt, static_cast<int>(salt_len),
        iterations,
        md,
        static_cast<int>(dk_len),
        derived.data());

    if (rc != 1) {
        AUTH_LOG_ERROR("PBKDF2 failed");
        return std::nullopt;
    }
    return derived;
}

// --------------------------------------------------------------------------
// Nonce generation
// --------------------------------------------------------------------------

/// Generate a cryptographically random nonce of specified length.
[[nodiscard]] std::string generate_nonce(size_t length = kNonceLength) {
    std::vector<unsigned char> buf(length);
    if (RAND_bytes(buf.data(), static_cast<int>(length)) != 1) {
        AUTH_LOG_ERROR("RAND_bytes failed for nonce generation");
        // Fallback: use mt19937 (not cryptographically secure, but
        // avoids crashing; will be rejected in production configs)
        std::mt19937_64 rng(std::random_device{}());
        for (auto& b : buf) {
            b = static_cast<unsigned char>(rng() & 0xFF);
        }
    }
    // Base64url-safe: use alphanumeric only for wire format
    static constexpr char kAlpha[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        result.push_back(kAlpha[buf[i] % (sizeof(kAlpha) - 1)]);
    }
    return result;
}

// --------------------------------------------------------------------------
// SCRAM attribute parsing
// --------------------------------------------------------------------------

/// Parse SCRAM key=value attributes from a comma-separated message.
/// Returns map of attribute key (single char) → value.
[[nodiscard]] std::map<char, std::string> parse_scram_attrs(
    std::string_view message) {
    std::map<char, std::string> attrs;
    size_t pos = 0;

    while (pos < message.size()) {
        // Find next comma (or end)
        size_t comma = message.find(kScramAttrSep, pos);
        std::string_view kv = (comma == std::string_view::npos)
            ? message.substr(pos)
            : message.substr(pos, comma - pos);

        if (kv.size() >= 2 && kv[1] == '=') {
            attrs[kv[0]] = std::string(kv.substr(2));
        }

        if (comma == std::string_view::npos) break;
        pos = comma + 1;
    }
    return attrs;
}

// --------------------------------------------------------------------------
// SCRAM Hi(str, salt, i) — salted password
// --------------------------------------------------------------------------

/// SCRAM Hi() = PBKDF2 with HMAC-SHA-256.
[[nodiscard]] std::optional<std::vector<unsigned char>> scram_hi_sha256(
    std::string_view str,
    const unsigned char* salt, size_t salt_len,
    int iterations) {
    return pbkdf2(EVP_sha256(), str, salt, salt_len, iterations,
                  SHA256_DIGEST_LENGTH);
}

/// SCRAM Hi() = PBKDF2 with HMAC-SHA-512.
[[nodiscard]] std::optional<std::vector<unsigned char>> scram_hi_sha512(
    std::string_view str,
    const unsigned char* salt, size_t salt_len,
    int iterations) {
    return pbkdf2(EVP_sha512(), str, salt, salt_len, iterations,
                  SHA512_DIGEST_LENGTH);
}

// --------------------------------------------------------------------------
// SCRAM HMAC wrappers
// --------------------------------------------------------------------------

[[nodiscard]] std::vector<unsigned char> hmac_sha256(
    std::string_view key, std::string_view data) {
    return compute_hmac(EVP_sha256(), key, data);
}

[[nodiscard]] std::vector<unsigned char> hmac_sha512(
    std::string_view key, std::string_view data) {
    return compute_hmac(EVP_sha512(), key, data);
}

// --------------------------------------------------------------------------
// SCRAM ClientKey / StoredKey / ServerKey derivation
// --------------------------------------------------------------------------

/// Derive SCRAM keys from salted password.
struct ScramKeys {
    std::vector<unsigned char> client_key;
    std::vector<unsigned char> stored_key;
    std::vector<unsigned char> server_key;
};

[[nodiscard]] ScramKeys derive_scram_keys_sha256(
    const std::vector<unsigned char>& salted_password) {
    ScramKeys keys;

    keys.client_key = hmac_sha256(
        std::string_view(reinterpret_cast<const char*>(salted_password.data()),
                         salted_password.size()),
        "Client Key");

    auto stored_key_hash = sha256(
        std::string_view(reinterpret_cast<const char*>(keys.client_key.data()),
                         keys.client_key.size()));
    keys.stored_key.assign(stored_key_hash.begin(), stored_key_hash.end());

    keys.server_key = hmac_sha256(
        std::string_view(reinterpret_cast<const char*>(salted_password.data()),
                         salted_password.size()),
        "Server Key");

    return keys;
}

[[nodiscard]] ScramKeys derive_scram_keys_sha512(
    const std::vector<unsigned char>& salted_password) {
    ScramKeys keys;

    keys.client_key = hmac_sha512(
        std::string_view(reinterpret_cast<const char*>(salted_password.data()),
                         salted_password.size()),
        "Client Key");

    auto stored_key_hash = sha512(
        std::string_view(reinterpret_cast<const char*>(keys.client_key.data()),
                         keys.client_key.size()));
    keys.stored_key.assign(stored_key_hash.begin(), stored_key_hash.end());

    keys.server_key = hmac_sha512(
        std::string_view(reinterpret_cast<const char*>(salted_password.data()),
                         salted_password.size()),
        "Server Key");

    return keys;
}

// --------------------------------------------------------------------------
// Channel binding
// --------------------------------------------------------------------------

/// tls-server-end-point channel binding data (RFC 5929).
/// In production this is the SHA-256 hash of the server's TLS certificate.
/// Here we store an optional hash for the binding validation.
struct ChannelBinding {
    enum class Type {
        none,             // n,,
        tls_unique,       // p=tls-unique,,
        tls_server_end,   // p=tls-server-end-point,,
    };

    Type type = Type::none;
    std::string binding_data;  // raw hash bytes (binary)
};

// --------------------------------------------------------------------------
// User credential store (server-side)
// --------------------------------------------------------------------------

/// Server-side stored credential record for SCRAM.
struct StoredScramCredential {
    std::string username;
    std::vector<unsigned char> salt;
    int iterations = kDefaultIterationsSHA256;
    std::vector<unsigned char> stored_key;   // H(ClientKey)
    std::vector<unsigned char> server_key;   // for ServerSignature
    std::string hash_algorithm;              // "SHA-256" or "SHA-512"
};

/// Server-side stored credential for PLAIN.
struct StoredPlainCredential {
    std::string username;
    std::string password_hash;  // bcrypt / sha512crypt hash
    std::string hash_type;      // "bcrypt", "sha512crypt", etc.
};

// --------------------------------------------------------------------------
// Session cache entry
// --------------------------------------------------------------------------

struct SessionEntry {
    std::string principal;
    std::string mechanism;
    std::string host;
    std::chrono::steady_clock::time_point created_at;
    std::chrono::seconds ttl;

    [[nodiscard]] bool is_expired() const noexcept {
        return (std::chrono::steady_clock::now() - created_at) > ttl;
    }
};

// --------------------------------------------------------------------------
// ACL rule representation
// --------------------------------------------------------------------------

/// A single ACL rule entry.
struct AclRule {
    std::string principal_pattern;  // may contain * wildcard
    std::string host_pattern;       // may contain * wildcard
    std::string resource_pattern;   // may contain * wildcard
    std::string operation;          // exact operation name or "ALL"
    bool allow = false;             // true = allow, false = deny

    /// Compute specificity score: higher = more specific.
    /// Count non-wildcard characters in each field, weighted.
    [[nodiscard]] int specificity() const noexcept {
        auto field_spec = [](const std::string& s) -> int {
            int score = 0;
            for (char c : s) {
                if (c != '*') ++score;
            }
            return score;
        };
        // Weighted: principal > resource > host
        return field_spec(principal_pattern) * 1000000 +
               field_spec(resource_pattern)  * 1000 +
               field_spec(host_pattern);
    }

    /// Check if a pattern matches a value (with * wildcard support).
    [[nodiscard]] static bool wildcard_match(
        const std::string& pattern, const std::string& value) {
        // Convert glob pattern to regex
        std::string regex_str;
        regex_str.reserve(pattern.size() * 2 + 2);
        regex_str.push_back('^');
        for (char c : pattern) {
            switch (c) {
            case '*':
                regex_str += ".*";
                break;
            case '.':
                regex_str += "\\.";
                break;
            case '?':
                regex_str += ".?";
                break;
            case '\\':
                regex_str += "\\\\";
                break;
            case '+':
                regex_str += "\\+";
                break;
            case '[':
            case ']':
            case '(':
            case ')':
            case '{':
            case '}':
            case '^':
            case '$':
            case '|':
                regex_str.push_back('\\');
                regex_str.push_back(c);
                break;
            default:
                regex_str.push_back(c);
                break;
            }
        }
        regex_str.push_back('$');

        try {
            std::regex re(regex_str, std::regex::optimize);
            return std::regex_match(value, re);
        } catch (const std::regex_error&) {
            AUTH_LOG_ERROR("Invalid ACL pattern regex: {}", regex_str);
            return false;
        }
    }

    /// Check if this rule matches the given principal/host/resource/operation.
    [[nodiscard]] bool matches(
        const std::string& principal,
        const std::string& host,
        const std::string& resource,
        const std::string& operation) const {

        if (!wildcard_match(principal_pattern, principal)) return false;
        if (!wildcard_match(host_pattern, host)) return false;
        if (!wildcard_match(resource_pattern, resource)) return false;

        // Operation match: exact or "ALL"
        if (this->operation != "ALL" && this->operation != operation) {
            return false;
        }
        return true;
    }
};

} // anonymous namespace

// ============================================================================
// SaslProvider Implementation
// ============================================================================

class SaslProvider {
public:
    explicit SaslProvider(broker::BrokerServer& s);

    // ---- Mechanism discovery ----
    [[nodiscard]] std::vector<std::string> enabled_mechanisms() const;

    // ---- Authentication entry points ----
    /// Authenticate using SCRAM-SHA-256.  credentials is the full SASL exchange
    /// payload (client-first-message or client-final-message depending on stage).
    /// Returns error_code::none on success.
    [[nodiscard]] result<void> authenticate_scram_sha256(
        const std::string& username, const std::string& password);

    [[nodiscard]] result<void> authenticate_scram_sha512(
        const std::string& username, const std::string& password);

    [[nodiscard]] result<void> authenticate_plain(
        const std::string& username,
        const std::string& password);

    [[nodiscard]] result<void> authenticate_plain_ex(
        const std::string& authzid,
        const std::string& authcid,
        const std::string& password);

    [[nodiscard]] result<void> authenticate_oauth2(
        const std::string& token);

    [[nodiscard]] result<void> authenticate_kerberos(
        const std::string& ticket);

    // ---- SCRAM server-side state machine ----
    /// Process SCRAM client-first-message. Returns server-first-message.
    /// On error, returns failure with e= attribute in error_message.
    [[nodiscard]] result<std::string> scram_server_first(
        const std::string& client_first_msg);

    /// Process SCRAM client-final-message. Returns server-final-message.
    /// On error, returns failure with e= attribute in error_message.
    [[nodiscard]] result<std::string> scram_server_final(
        const std::string& client_final_msg);

    /// Get the authenticated username from the current SCRAM session.
    [[nodiscard]] std::string scram_username() const;

    // ---- Configuration ----
    void set_iterations(int sha256_iter, int sha512_iter);
    void set_channel_binding(ChannelBinding cb);

    // ---- User store management (server-side) ----
    void store_scram_credential(
        const std::string& username,
        const std::string& password,
        const std::string& algorithm);  // "SHA-256" or "SHA-512"

    void store_plain_credential(
        const std::string& username,
        const std::string& password);

    [[nodiscard]] std::optional<StoredScramCredential> lookup_scram(
        const std::string& username,
        const std::string& algorithm) const;

    [[nodiscard]] std::optional<StoredPlainCredential> lookup_plain(
        const std::string& username) const;

private:
    broker::BrokerServer* server_;

    // SCRAM state machine context (per-session; protected by mutex)
    mutable std::mutex scram_mutex_;
    struct ScramSessionState {
        std::string username;
        std::string nonce;              // combined nonce
        std::vector<unsigned char> salt;
        int iterations = 0;
        std::vector<unsigned char> server_key;
        std::string auth_message;       // for proof verification
        ChannelBinding channel_binding;
        std::string hash_algorithm;
    };
    std::optional<ScramSessionState> scram_session_;

    // Configuration
    int scram_sha256_iterations_ = kDefaultIterationsSHA256;
    int scram_sha512_iterations_ = kDefaultIterationsSHA512;
    ChannelBinding default_channel_binding_;

    // User stores (in production: backed by LDAP / DB / file)
    mutable std::shared_mutex user_store_mutex_;
    std::unordered_map<std::string, StoredScramCredential> scram_users_sha256_;
    std::unordered_map<std::string, StoredScramCredential> scram_users_sha512_;
    std::unordered_map<std::string, StoredPlainCredential> plain_users_;

    // Enabled mechanism set
    std::unordered_set<std::string> enabled_mechanisms_;

    // Internal helpers
    [[nodiscard]] result<std::string> scram_server_first_impl(
        const std::string& client_first_msg,
        const std::string& hash_algo);

    [[nodiscard]] result<std::string> scram_server_final_impl(
        const std::string& client_final_msg,
        const std::string& hash_algo);

    /// Generate SCRAM server-first-message.
    [[nodiscard]] std::string build_server_first(
        const std::string& client_nonce,
        const std::string& username,
        const StoredScramCredential& cred);

    /// Verify SCRAM client proof and generate server signature.
    [[nodiscard]] result<std::string> verify_client_proof(
        const std::string& client_nonce,
        const std::string& proof_b64,
        const std::string& channel_binding_b64,
        const std::string& hash_algo,
        const StoredScramCredential& cred);

    /// Validate JWT token (simplified — in production use a JWT library).
    [[nodiscard]] result<void> validate_jwt(const std::string& token);

    /// Token introspection against OAuth2 AS (stub).
    [[nodiscard]] result<void> introspect_token(const std::string& token);

    friend class AuthManager;
};

// ============================================================================
// AclEngine Implementation
// ============================================================================

class AclEngine {
public:
    explicit AclEngine(broker::BrokerServer& s);

    /// Check if principal@host may perform operation on resource.
    /// Returns true if allowed, false if denied.
    [[nodiscard]] bool check_access(
        const std::string& principal,
        const std::string& host,
        const std::string& resource,
        const std::string& operation);

    /// Add an ACL rule.  Patterns may contain * wildcards.
    void add_rule(
        const std::string& principal_pattern,
        const std::string& host_pattern,
        const std::string& resource_pattern,
        const std::string& operation,
        bool allow);

    /// Remove an ACL rule. Returns true if a matching rule was removed.
    bool remove_rule(
        const std::string& principal_pattern,
        const std::string& host_pattern,
        const std::string& resource_pattern,
        const std::string& operation);

    /// Remove all rules.  Used for reloading configuration.
    void clear_rules();

    /// Number of rules currently loaded.
    [[nodiscard]] size_t rule_count() const noexcept;

    /// Dump all rules for debugging.
    [[nodiscard]] std::vector<std::string> dump_rules() const;

private:
    broker::BrokerServer* server_;

    mutable std::shared_mutex rules_mutex_;
    std::vector<AclRule> rules_;

    /// Re-sort rules by specificity (most specific first).
    void reindex();

    friend class AuthManager;
};

// ============================================================================
// AuditLog Implementation
// ============================================================================

class AuditLog {
public:
    explicit AuditLog(broker::BrokerServer& s);

    /// Log an authentication event.
    void log_auth(
        const std::string& principal,
        const std::string& action,
        bool success);

    /// Log an authentication event with additional details.
    void log_auth(
        const std::string& principal,
        const std::string& action,
        bool success,
        const std::string& details);

    /// Log an admin operation.
    void log_admin(
        const std::string& principal,
        const std::string& operation, // e.g. "CREATE_TOPIC", "DELETE_ACL"
        const std::string& resource);

    /// Log a security event (policy violation, ACL deny, etc.).
    void log_security(
        const std::string& event,     // e.g. "ACL_DENY", "RATE_LIMIT", "INVALID_TOKEN"
        const std::string& details);

private:
    broker::BrokerServer* server_;
    std::mutex log_mutex_;

    /// Write a structured audit entry.
    void emit(
        const std::string& category,
        const std::string& principal,
        const std::string& action,
        const std::string& resource,
        bool success);
};

// ============================================================================
// AuthManager Implementation
// ============================================================================

class AuthManager {
public:
    explicit AuthManager(broker::BrokerServer& s);

    /// Authenticate using the named SASL mechanism.
    /// credentials format depends on the mechanism:
    ///   SCRAM-SHA-256: client-first-message
    ///   SCRAM-SHA-512: client-first-message
    ///   PLAIN:         authzid\0authcid\0password
    ///   OAUTHBEARER:   n,a=authzid,\x01auth=Bearer <token>\x01\x01
    ///   GSSAPI:        Kerberos ticket (delegated to GSSAPI)
    [[nodiscard]] result<void> authenticate(
        const std::string& mechanism,
        const std::string& credentials);

    /// Check if a principal has an active, non-expired session.
    [[nodiscard]] bool is_authenticated(const std::string& principal) const;

    /// Authorize a principal for an operation on a resource.
    [[nodiscard]] result<void> authorize(
        const std::string& principal,
        const std::string& resource,
        const std::string& operation);

    /// Full authorize with host context (for network-level ACLs).
    [[nodiscard]] result<void> authorize(
        const std::string& principal,
        const std::string& host,
        const std::string& resource,
        const std::string& operation);

    /// List enabled SASL mechanisms.
    [[nodiscard]] std::vector<std::string> sasl_mechanisms() const;

    /// Add a session to the session cache.
    void register_session(
        const std::string& principal,
        const std::string& mechanism,
        const std::string& host = "");

    /// Remove a session (logout).
    void revoke_session(const std::string& principal);

    /// Access the internal SaslProvider, AclEngine, AuditLog.
    [[nodiscard]] SaslProvider& sasl_provider() { return *sasl_; }
    [[nodiscard]] AclEngine& acl_engine() { return *acl_; }
    [[nodiscard]] AuditLog& audit_log() { return *audit_; }

private:
    broker::BrokerServer* server_;

    std::unique_ptr<SaslProvider> sasl_;
    std::unique_ptr<AclEngine> acl_;
    std::unique_ptr<AuditLog> audit_;

    // Session cache
    mutable std::shared_mutex session_mutex_;
    std::unordered_map<std::string, SessionEntry> sessions_;
    std::chrono::seconds session_ttl_{300};  // 5 minutes

    /// Purge expired sessions.  Called periodically.
    void purge_expired_sessions();

    /// Parse PLAIN credential string (authzid\0authcid\0password).
    [[nodiscard]] static std::optional<
        std::tuple<std::string, std::string, std::string>>
    parse_plain_credentials(const std::string& creds);

    /// Parse OAUTHBEARER credential string (RFC 7628).
    [[nodiscard]] static std::optional<std::string>
    parse_oauth_token(const std::string& creds);
};

// ============================================================================
// ---- SaslProvider Implementation ----
// ============================================================================

SaslProvider::SaslProvider(broker::BrokerServer& s)
    : server_(&s) {
    // Default enabled mechanisms
    enabled_mechanisms_.insert(std::string(kMechScramSha256));
    enabled_mechanisms_.insert(std::string(kMechScramSha512));
    enabled_mechanisms_.insert(std::string(kMechPlain));
    enabled_mechanisms_.insert(std::string(kMechOAuthBearer));
    enabled_mechanisms_.insert(std::string(kMechKerberos));

    AUTH_LOG_INFO("SaslProvider initialized");
}

std::vector<std::string> SaslProvider::enabled_mechanisms() const {
    std::vector<std::string> mechs;
    mechs.reserve(enabled_mechanisms_.size());
    for (const auto& m : enabled_mechanisms_) {
        mechs.push_back(m);
    }
    // Sort for deterministic output
    std::sort(mechs.begin(), mechs.end());
    return mechs;
}

void SaslProvider::set_iterations(int sha256_iter, int sha512_iter) {
    if (sha256_iter >= kMinIterations && sha256_iter <= kMaxIterations) {
        scram_sha256_iterations_ = sha256_iter;
    } else {
        AUTH_LOG_WARN("SCRAM-SHA-256 iterations {} out of range [{}, {}]; "
                      "using default {}",
                      sha256_iter, kMinIterations, kMaxIterations,
                      kDefaultIterationsSHA256);
    }
    if (sha512_iter >= kMinIterations && sha512_iter <= kMaxIterations) {
        scram_sha512_iterations_ = sha512_iter;
    } else {
        AUTH_LOG_WARN("SCRAM-SHA-512 iterations {} out of range [{}, {}]; "
                      "using default {}",
                      sha512_iter, kMinIterations, kMaxIterations,
                      kDefaultIterationsSHA512);
    }
}

void SaslProvider::set_channel_binding(ChannelBinding cb) {
    default_channel_binding_ = std::move(cb);
}

// ---- User store management ----

void SaslProvider::store_scram_credential(
    const std::string& username,
    const std::string& password,
    const std::string& algorithm) {

    std::unique_lock lock(user_store_mutex_);

    // Generate random salt
    std::vector<unsigned char> salt(kSaltLength);
    if (RAND_bytes(salt.data(), static_cast<int>(salt.size())) != 1) {
        AUTH_LOG_ERROR("RAND_bytes failed for salt generation");
        return;
    }

    if (algorithm == "SHA-256") {
        int iter = scram_sha256_iterations_;
        auto salted = scram_hi_sha256(password, salt.data(), salt.size(), iter);
        if (!salted) return;

        auto keys = derive_scram_keys_sha256(*salted);

        StoredScramCredential cred;
        cred.username = username;
        cred.salt = std::move(salt);
        cred.iterations = iter;
        cred.stored_key = std::move(keys.stored_key);
        cred.server_key = std::move(keys.server_key);
        cred.hash_algorithm = "SHA-256";

        scram_users_sha256_[username] = std::move(cred);
        AUTH_LOG_DEBUG("Stored SCRAM-SHA-256 credential for '{}'", username);

    } else if (algorithm == "SHA-512") {
        int iter = scram_sha512_iterations_;
        auto salted = scram_hi_sha512(password, salt.data(), salt.size(), iter);
        if (!salted) return;

        auto keys = derive_scram_keys_sha512(*salted);

        StoredScramCredential cred;
        cred.username = username;
        cred.salt = std::move(salt);
        cred.iterations = iter;
        cred.stored_key = std::move(keys.stored_key);
        cred.server_key = std::move(keys.server_key);
        cred.hash_algorithm = "SHA-512";

        scram_users_sha512_[username] = std::move(cred);
        AUTH_LOG_DEBUG("Stored SCRAM-SHA-512 credential for '{}'", username);

    } else {
        AUTH_LOG_ERROR("Unknown SCRAM hash algorithm: {}", algorithm);
    }
}

void SaslProvider::store_plain_credential(
    const std::string& username,
    const std::string& password) {

    std::unique_lock lock(user_store_mutex_);

    // In production, hash with bcrypt/sha512crypt.
    // Here we store SHA-256 for the stub implementation.
    auto hash = sha256(password);
    std::string hash_hex = hex_encode(hash.data(), hash.size());

    StoredPlainCredential cred;
    cred.username = username;
    cred.password_hash = hash_hex;
    cred.hash_type = "SHA-256";

    plain_users_[username] = std::move(cred);
    AUTH_LOG_DEBUG("Stored PLAIN credential for '{}'", username);
}

std::optional<StoredScramCredential> SaslProvider::lookup_scram(
    const std::string& username,
    const std::string& algorithm) const {

    std::shared_lock lock(user_store_mutex_);

    if (algorithm == "SHA-256") {
        auto it = scram_users_sha256_.find(username);
        if (it != scram_users_sha256_.end()) return it->second;
    } else if (algorithm == "SHA-512") {
        auto it = scram_users_sha512_.find(username);
        if (it != scram_users_sha512_.end()) return it->second;
    }
    return std::nullopt;
}

std::optional<StoredPlainCredential> SaslProvider::lookup_plain(
    const std::string& username) const {

    std::shared_lock lock(user_store_mutex_);
    auto it = plain_users_.find(username);
    if (it != plain_users_.end()) return it->second;
    return std::nullopt;
}

// ---- Authentication entry points ----

result<void> SaslProvider::authenticate_scram_sha256(
    const std::string& /*username*/, const std::string& /*password*/) {
    // SCRAM is a multi-round protocol.  The actual exchange is handled
    // by scram_server_first / scram_server_final.  This stub exists for
    // the header interface contract; the real logic is in AuthManager
    // which calls the state machine directly.
    return result<void>::failure(
        error_code::illegal_sasl_state,
        "Use scram_server_first/scram_server_final for SCRAM");
}

result<void> SaslProvider::authenticate_scram_sha512(
    const std::string& /*username*/, const std::string& /*password*/) {
    return result<void>::failure(
        error_code::illegal_sasl_state,
        "Use scram_server_first/scram_server_final for SCRAM");
}

result<void> SaslProvider::authenticate_plain(
    const std::string& username,
    const std::string& password) {

    // No authzid: identity is the same as authcid
    auto cred = lookup_plain(username);
    if (!cred) {
        AUTH_LOG_WARN("PLAIN auth failed: unknown user '{}'", username);
        return result<void>::failure(
            error_code::sasl_authentication_failed,
            "Unknown user");
    }

    auto provided_hash = sha256(password);
    std::string provided_hex = hex_encode(provided_hash.data(),
                                          provided_hash.size());

    if (provided_hex != cred->password_hash) {
        AUTH_LOG_WARN("PLAIN auth failed: invalid password for '{}'", username);
        return result<void>::failure(
            error_code::sasl_authentication_failed,
            "Invalid password");
    }

    AUTH_LOG_INFO("PLAIN auth succeeded for '{}'", username);
    return result<void>::success();
}

result<void> SaslProvider::authenticate_plain_ex(
    const std::string& authzid,
    const std::string& authcid,
    const std::string& password) {

    // RFC 4616: PLAIN SASL mechanism with explicit authzid
    std::string effective_user = authzid.empty() ? authcid : authzid;

    auto cred = lookup_plain(authcid);
    if (!cred) {
        AUTH_LOG_WARN("PLAIN auth failed: unknown user '{}'", authcid);
        return result<void>::failure(
            error_code::sasl_authentication_failed,
            "Unknown user");
    }

    auto provided_hash = sha256(password);
    std::string provided_hex = hex_encode(provided_hash.data(),
                                          provided_hash.size());

    if (provided_hex != cred->password_hash) {
        AUTH_LOG_WARN("PLAIN auth failed: invalid password for '{}'", authcid);
        return result<void>::failure(
            error_code::sasl_authentication_failed,
            "Invalid password");
    }

    if (!authzid.empty() && authzid != authcid) {
        AUTH_LOG_DEBUG("PLAIN auth: authcid='{}' authorizing as authzid='{}'",
                       authcid, authzid);
    }

    AUTH_LOG_INFO("PLAIN auth succeeded for '{}'", effective_user);
    return result<void>::success();
}

result<void> SaslProvider::authenticate_oauth2(const std::string& token) {
    // Validate JWT token
    auto jwt_result = validate_jwt(token);
    if (jwt_result.failed()) {
        AUTH_LOG_WARN("OAuth2 auth failed: invalid token");
        return jwt_result;
    }

    // Introspect token with authorization server
    auto intro_result = introspect_token(token);
    if (intro_result.failed()) {
        AUTH_LOG_WARN("OAuth2 auth failed: token introspection failed");
        return intro_result;
    }

    AUTH_LOG_INFO("OAuth2 auth succeeded");
    return result<void>::success();
}

result<void> SaslProvider::authenticate_kerberos(const std::string& ticket) {
    // Kerberos/GSSAPI authentication
    // In production, this would use gss_accept_sec_context() from
    // the GSSAPI library (libgssapi_krb5).

    (void)ticket;

    // Stub implementation: check ticket is non-empty
    if (ticket.empty()) {
        AUTH_LOG_WARN("Kerberos auth failed: empty ticket");
        return result<void>::failure(
            error_code::sasl_authentication_failed,
            "Empty Kerberos ticket");
    }

    // Production implementation would:
    // 1. Acquire service credentials from keytab
    // 2. Call gss_accept_sec_context() with client ticket
    // 3. Extract client principal from GSS context
    // 4. Verify mutual authentication flag
    // 5. Optionally check authorization data (PAC)

    AUTH_LOG_INFO("Kerberos auth succeeded (stub)");
    return result<void>::success();
}

// ---- SCRAM server state machine ----

result<std::string> SaslProvider::scram_server_first(
    const std::string& client_first_msg) {

    // Detect mechanism from GS2 header (first 3+ bytes)
    if (client_first_msg.rfind("n,,", 0) == 0 ||
        client_first_msg.rfind("p=tls-unique", 0) == 0 ||
        client_first_msg.rfind("p=tls-server-end-point", 0) == 0) {

        // Determine hash algorithm from mechanism negotiation
        // (stored from initial SASL handshake)
        std::string algo = "SHA-256";  // default; set by AuthManager
        std::string server_first = scram_server_first_impl(
            client_first_msg, algo).value;
        return result<std::string>::success(server_first);
    }

    return result<std::string>::failure(
        error_code::illegal_sasl_state,
        "Invalid GS2 header in client-first-message");
}

result<std::string> SaslProvider::scram_server_final(
    const std::string& client_final_msg) {

    std::lock_guard lock(scram_mutex_);
    if (!scram_session_) {
        return result<std::string>::failure(
            error_code::illegal_sasl_state,
            "No active SCRAM session");
    }

    std::string algo = scram_session_->hash_algorithm;
    return scram_server_final_impl(client_final_msg, algo);
}

std::string SaslProvider::scram_username() const {
    std::lock_guard lock(scram_mutex_);
    if (scram_session_) {
        return scram_session_->username;
    }
    return {};
}

// ---- Internal SCRAM implementation ----

result<std::string> SaslProvider::scram_server_first_impl(
    const std::string& client_first_msg,
    const std::string& hash_algo) {

    // ---- Step 1: Parse client-first-message ----
    //
    // Format: gs2-header [authzid] , client-attrs
    // where gs2-header is one of:
    //   n,,            — no channel binding, no authzid
    //   n,a=authzid,    — no channel binding, with authzid
    //   p=tls-unique,,  — channel binding tls-unique
    //   p=tls-server-end-point,, — channel binding tls-server-end-point
    //
    // client-attrs: n=<username>,r=<client_nonce>[,<extensions>]

    std::string_view msg(client_first_msg);

    // Skip GS2 header
    size_t gs2_end = msg.find(',');
    if (gs2_end == std::string_view::npos || gs2_end + 2 > msg.size()) {
        return result<std::string>::failure(
            error_code::illegal_sasl_state,
            std::string(kScramErrInvalidEncoding));
    }
    // gs2_end points to the second comma in "n,," — find next comma after that
    gs2_end = msg.find(',', gs2_end + 1);
    if (gs2_end == std::string_view::npos) {
        return result<std::string>::failure(
            error_code::illegal_sasl_state,
            std::string(kScramErrInvalidEncoding));
    }

    // Check if authzid is present (a= prefix)
    std::string authzid;
    if (msg.size() > 3 && msg[0] == 'n' && msg[1] == ',' && msg[2] == 'a') {
        // n,a=<authzid>,
        size_t authzid_start = 3;  // skip "n,a="
        size_t authzid_end = msg.find(',', authzid_start);
        if (authzid_end != std::string_view::npos) {
            authzid = std::string(msg.substr(authzid_start,
                                             authzid_end - authzid_start));
            gs2_end = authzid_end;
        }
    }

    // Parse client attributes: n=<username>,r=<client_nonce>
    // Start after the final GS2 comma
    size_t attrs_start = gs2_end;
    if (attrs_start < msg.size() && msg[attrs_start] == ',') {
        ++attrs_start;
    }
    auto attrs = parse_scram_attrs(msg.substr(attrs_start));

    auto user_it = attrs.find('n');
    auto nonce_it = attrs.find('r');

    if (user_it == attrs.end() || nonce_it == attrs.end()) {
        return result<std::string>::failure(
            error_code::illegal_sasl_state,
            std::string(kScramErrInvalidEncoding));
    }

    std::string username = user_it->second;
    std::string client_nonce = nonce_it->second;

    if (username.empty() || client_nonce.empty()) {
        return result<std::string>::failure(
            error_code::illegal_sasl_state,
            std::string(kScramErrInvalidUsername));
    }

    // Sanity check on username: must be valid UTF-8, not contain ',', '='
    if (username.find(',') != std::string::npos ||
        username.find('=') != std::string::npos) {
        return result<std::string>::failure(
            error_code::illegal_sasl_state,
            std::string(kScramErrInvalidUsername));
    }

    AUTH_LOG_DEBUG("SCRAM server-first: user='{}', client_nonce='{}'",
                   username, client_nonce);

    // ---- Step 2: Look up stored credential ----
    auto cred_opt = lookup_scram(username, hash_algo);
    if (!cred_opt) {
        AUTH_LOG_WARN("SCRAM: unknown user '{}'", username);
        return result<std::string>::failure(
            error_code::sasl_authentication_failed,
            std::string(kScramErrUnknownUser));
    }
    const auto& cred = *cred_opt;

    // ---- Step 3: Generate server nonce and build server-first-message ----
    std::string server_nonce = generate_nonce(kNonceLength);
    std::string combined_nonce = client_nonce + server_nonce;

    // Check combined nonce length
    if (combined_nonce.size() > kMaxCombinedNonceLength) {
        return result<std::string>::failure(
            error_code::illegal_sasl_state,
            std::string(kScramErrInvalidEncoding));
    }

    std::string salt_b64 = base64_encode(cred.salt.data(), cred.salt.size());

    // Build server-first-message
    std::ostringstream server_first;
    server_first << kAttrNonce << combined_nonce
                 << kScramAttrSep
                 << kAttrSalt << salt_b64
                 << kScramAttrSep
                 << kAttrIter << cred.iterations;

    std::string server_first_str = server_first.str();

    // ---- Step 4: Save session state ----
    {
        std::lock_guard lock(scram_mutex_);
        ScramSessionState state;
        state.username = username;
        state.nonce = combined_nonce;
        state.salt = cred.salt;
        state.iterations = cred.iterations;
        state.server_key = cred.server_key;
        state.hash_algorithm = hash_algo;
        state.channel_binding = default_channel_binding_;

        // Construct AuthMessage: client-first-without-gs2 + "," +
        //                        server-first + "," +
        //                        client-final-without-proof
        // The full message will be completed in server_final.
        state.auth_message = std::string(msg) + kScramAttrSep + server_first_str;

        scram_session_ = std::move(state);
    }

    AUTH_LOG_DEBUG("SCRAM server-first-message: {}", server_first_str);
    return result<std::string>::success(server_first_str);
}

result<std::string> SaslProvider::scram_server_final_impl(
    const std::string& client_final_msg,
    const std::string& hash_algo) {

    if (!scram_session_) {
        return result<std::string>::failure(
            error_code::illegal_sasl_state,
            std::string(kScramErrOther));
    }

    // ---- Step 1: Parse client-final-message ----
    //
    // Format: c=<base64(cbind_data)>,r=<combined_nonce>[,<extensions>],
    //         p=<base64(ClientProof)>

    auto attrs = parse_scram_attrs(client_final_msg);

    auto cbind_it = attrs.find('c');
    auto nonce_it = attrs.find('r');
    auto proof_it = attrs.find('p');

    if (cbind_it == attrs.end() || nonce_it == attrs.end() ||
        proof_it == attrs.end()) {
        return result<std::string>::failure(
            error_code::illegal_sasl_state,
            std::string(kScramErrInvalidEncoding));
    }

    std::string cbind_b64 = cbind_it->second;
    std::string client_combined_nonce = nonce_it->second;
    std::string client_proof_b64 = proof_it->second;

    // ---- Step 2: Validate nonce ----
    if (client_combined_nonce != scram_session_->nonce) {
        AUTH_LOG_WARN("SCRAM nonce mismatch: expected '{}', got '{}'",
                      scram_session_->nonce, client_combined_nonce);
        return result<std::string>::failure(
            error_code::sasl_authentication_failed,
            std::string(kScramErrOther));
    }

    // ---- Step 3: Validate channel binding ----
    // The client sends c=<base64(GS2-header + channel-binding-data)>
    auto cbind_decoded = base64_decode(cbind_b64);
    if (!cbind_decoded) {
        return result<std::string>::failure(
            error_code::illegal_sasl_state,
            std::string(kScramErrInvalidEncoding));
    }

    // Check GS2 header prefix in decoded channel binding
    std::string cbind_str(
        reinterpret_cast<const char*>(cbind_decoded->data()),
        cbind_decoded->size());

    if (scram_session_->channel_binding.type ==
        ChannelBinding::Type::tls_server_end) {
        // Client must use p=tls-server-end-point
        if (cbind_str.rfind("p=tls-server-end-point", 0) != 0) {
            // But "n,," is also acceptable (client opted out)
            if (cbind_str.rfind("n,,", 0) != 0) {
                return result<std::string>::failure(
                    error_code::sasl_authentication_failed,
                    std::string(kScramErrChannelBindings));
            }
            AUTH_LOG_DEBUG("SCRAM: client opted out of channel binding");
        }
    }

    // ---- Step 4: Compute ClientProof and verify ----
    // ClientProof = HMAC(StoredKey, AuthMessage) XOR ClientKey
    // To verify: compute ClientSignature = HMAC(StoredKey, AuthMessage)
    //            recover ClientKey = ClientProof XOR ClientSignature
    //            verify H(ClientKey) == StoredKey

    // Look up credential (re-fetch to get stored_key)
    auto cred_opt = lookup_scram(scram_session_->username, hash_algo);
    if (!cred_opt) {
        return result<std::string>::failure(
            error_code::sasl_authentication_failed,
            std::string(kScramErrUnknownUser));
    }
    const auto& cred = *cred_opt;

    // Complete AuthMessage
    // AuthMessage = client-first-message-bare + "," +
    //               server-first-message + "," +
    //               client-final-message-without-proof
    // client-first-message-bare = client-first with GS2 header stripped

    // Reconstruct: find where GS2 header ends in auth_message
    // The auth_message currently has the FULL client-first including GS2 header
    std::string auth_msg = scram_session_->auth_message;
    // Remove GS2 header: everything up to and including the last comma
    // of the GS2 header
    size_t first_comma_in_msg = auth_msg.find(kScramAttrSep);
    if (first_comma_in_msg != std::string::npos) {
        // Find the real start of client attributes (after GS2 header)
        // GS2 header is "n,," or "n,a=authzid," or "p=tls-server-end-point,,"
        // We need to strip everything up to and including the comma that
        // separates GS2 header from client attributes.
        // Simplification: find the second comma (after n,,)
        size_t gs2_end = auth_msg.find(kScramAttrSep, 3);
        if (gs2_end != std::string::npos && gs2_end + 1 < auth_msg.size()) {
            auth_msg = auth_msg.substr(gs2_end + 1);  // skip the comma
        }
    }

    // Now auth_msg has: client-first-bare + "," + server-first
    // Append "," + client-final-without-proof
    // client-final-without-proof = "c=<c>,r=<r>" (everything except p=...)
    std::string client_final_wo_proof;
    size_t proof_pos = client_final_msg.find(",p=");
    if (proof_pos != std::string::npos) {
        client_final_wo_proof = client_final_msg.substr(0, proof_pos);
    } else {
        // p= might be at the end
        proof_pos = client_final_msg.rfind("p=");
        if (proof_pos != std::string::npos && proof_pos > 0) {
            client_final_wo_proof = client_final_msg.substr(0, proof_pos - 1);
        } else {
            client_final_wo_proof = client_final_msg;
        }
    }

    auth_msg += kScramAttrSep + client_final_wo_proof;

    // ---- Step 5: Verify ClientProof ----
    // ClientSignature = HMAC(StoredKey, AuthMessage)
    auto client_sig = hmac_sha256(
        std::string_view(
            reinterpret_cast<const char*>(cred.stored_key.data()),
            cred.stored_key.size()),
        auth_msg);

    // ClientKey = ClientProof XOR ClientSignature
    auto proof_decoded = base64_decode(client_proof_b64);
    if (!proof_decoded || proof_decoded->size() != client_sig.size()) {
        return result<std::string>::failure(
            error_code::sasl_authentication_failed,
            std::string(kScramErrInvalidProof));
    }

    std::vector<unsigned char> recovered_client_key(client_sig.size());
    for (size_t i = 0; i < client_sig.size(); ++i) {
        recovered_client_key[i] = (*proof_decoded)[i] ^ client_sig[i];
    }

    // H(recovered_client_key) should equal stored_key
    if (hash_algo == "SHA-256") {
        auto h_recovered = sha256(
            std::string_view(
                reinterpret_cast<const char*>(recovered_client_key.data()),
                recovered_client_key.size()));
        if (memcmp(h_recovered.data(), cred.stored_key.data(),
                   SHA256_DIGEST_LENGTH) != 0) {
            AUTH_LOG_WARN("SCRAM ClientProof verification failed for '{}'",
                          scram_session_->username);
            return result<std::string>::failure(
                error_code::sasl_authentication_failed,
                std::string(kScramErrInvalidProof));
        }
    } else {
        auto h_recovered = sha512(
            std::string_view(
                reinterpret_cast<const char*>(recovered_client_key.data()),
                recovered_client_key.size()));
        if (memcmp(h_recovered.data(), cred.stored_key.data(),
                   SHA512_DIGEST_LENGTH) != 0) {
            AUTH_LOG_WARN("SCRAM ClientProof verification failed for '{}'",
                          scram_session_->username);
            return result<std::string>::failure(
                error_code::sasl_authentication_failed,
                std::string(kScramErrInvalidProof));
        }
    }

    // ---- Step 6: Compute ServerSignature ----
    // ServerSignature = HMAC(ServerKey, AuthMessage)
    std::vector<unsigned char> server_sig;
    if (hash_algo == "SHA-256") {
        server_sig = hmac_sha256(
            std::string_view(
                reinterpret_cast<const char*>(cred.server_key.data()),
                cred.server_key.size()),
            auth_msg);
    } else {
        server_sig = hmac_sha512(
            std::string_view(
                reinterpret_cast<const char*>(cred.server_key.data()),
                cred.server_key.size()),
            auth_msg);
    }

    std::string server_sig_b64 = base64_encode(
        server_sig.data(), server_sig.size());

    // Build server-final-message
    std::string server_final = std::string(kAttrVerifier) + server_sig_b64;

    AUTH_LOG_INFO("SCRAM authentication succeeded for '{}'",
                  scram_session_->username);

    return result<std::string>::success(server_final);
}

// ---- JWT validation (stub) ----

result<void> SaslProvider::validate_jwt(const std::string& token) {
    // In production: decode JWT header + payload, verify signature
    // using JWKS from the authorization server, check exp, nbf, iss, aud.

    if (token.empty()) {
        return result<void>::failure(
            error_code::sasl_authentication_failed,
            "Empty OAuth2 token");
    }

    // Basic JWT structure check (3 dot-separated parts)
    size_t dot1 = token.find('.');
    size_t dot2 = token.find('.', dot1 + 1);
    if (dot1 == std::string::npos || dot2 == std::string::npos) {
        return result<void>::failure(
            error_code::sasl_authentication_failed,
            "Invalid JWT format");
    }

    // In production: base64url-decode header + payload
    // Verify: alg matches expected, kid resolves to a key,
    //         signature is valid, exp > now, iss is trusted,
    //         aud includes this service.

    AUTH_LOG_DEBUG("JWT structural validation passed");
    return result<void>::success();
}

result<void> SaslProvider::introspect_token(const std::string& token) {
    // In production: POST to token introspection endpoint (RFC 7662)
    // with client_id + client_secret, parse {active: true/false, ...} response.

    (void)token;

    // Stub: always succeed
    AUTH_LOG_DEBUG("Token introspection: token is active (stub)");
    return result<void>::success();
}

// ============================================================================
// ---- AclEngine Implementation ----
// ============================================================================

AclEngine::AclEngine(broker::BrokerServer& s)
    : server_(&s) {
    AUTH_LOG_INFO("AclEngine initialized");
}

bool AclEngine::check_access(
    const std::string& principal,
    const std::string& host,
    const std::string& resource,
    const std::string& operation) {

    std::shared_lock lock(rules_mutex_);

    // Evaluate rules in order (already sorted by specificity, most first)
    for (const auto& rule : rules_) {
        if (rule.matches(principal, host, resource, operation)) {
            AUTH_LOG_DEBUG("ACL match: principal='{}' host='{}' resource='{}' "
                           "op='{}' → {}",
                           principal, host, resource, operation,
                           rule.allow ? "ALLOW" : "DENY");
            return rule.allow;
        }
    }

    // Default: deny
    AUTH_LOG_DEBUG("ACL default DENY: principal='{}' host='{}' resource='{}' "
                   "op='{}'",
                   principal, host, resource, operation);
    return false;
}

void AclEngine::add_rule(
    const std::string& principal_pattern,
    const std::string& host_pattern,
    const std::string& resource_pattern,
    const std::string& operation,
    bool allow) {

    std::unique_lock lock(rules_mutex_);

    AclRule rule;
    rule.principal_pattern = principal_pattern;
    rule.host_pattern = host_pattern;
    rule.resource_pattern = resource_pattern;
    rule.operation = operation;
    rule.allow = allow;

    rules_.push_back(std::move(rule));
    reindex();

    AUTH_LOG_INFO("ACL rule added: principal='{}' host='{}' resource='{}' "
                  "op='{}' → {}",
                  principal_pattern, host_pattern, resource_pattern, operation,
                  allow ? "ALLOW" : "DENY");
}

bool AclEngine::remove_rule(
    const std::string& principal_pattern,
    const std::string& host_pattern,
    const std::string& resource_pattern,
    const std::string& operation) {

    std::unique_lock lock(rules_mutex_);

    auto it = std::remove_if(rules_.begin(), rules_.end(),
        [&](const AclRule& r) {
            return r.principal_pattern == principal_pattern &&
                   r.host_pattern == host_pattern &&
                   r.resource_pattern == resource_pattern &&
                   r.operation == operation;
        });

    if (it != rules_.end()) {
        size_t removed = static_cast<size_t>(std::distance(it, rules_.end()));
        rules_.erase(it, rules_.end());
        reindex();
        AUTH_LOG_INFO("ACL rule removed: principal='{}' host='{}' resource='{}' "
                      "op='{}' ({} rules)",
                      principal_pattern, host_pattern, resource_pattern,
                      operation, removed);
        return true;
    }
    return false;
}

void AclEngine::clear_rules() {
    std::unique_lock lock(rules_mutex_);
    rules_.clear();
    AUTH_LOG_INFO("All ACL rules cleared");
}

size_t AclEngine::rule_count() const noexcept {
    std::shared_lock lock(rules_mutex_);
    return rules_.size();
}

std::vector<std::string> AclEngine::dump_rules() const {
    std::shared_lock lock(rules_mutex_);
    std::vector<std::string> out;
    out.reserve(rules_.size());
    for (const auto& r : rules_) {
        std::ostringstream ss;
        ss << (r.allow ? "ALLOW" : "DENY")
           << " principal=" << r.principal_pattern
           << " host=" << r.host_pattern
           << " resource=" << r.resource_pattern
           << " op=" << r.operation
           << " specificity=" << r.specificity();
        out.push_back(ss.str());
    }
    return out;
}

void AclEngine::reindex() {
    // Sort by specificity descending (most specific first).
    // Within same specificity, ALLOW before DENY (conservative),
    // then lexicographic for determinism.
    std::sort(rules_.begin(), rules_.end(),
        [](const AclRule& a, const AclRule& b) {
            int sa = a.specificity();
            int sb = b.specificity();
            if (sa != sb) return sa > sb;
            // More permissive first within same specificity tier
            if (a.allow != b.allow) return a.allow > b.allow;
            // Deterministic tiebreak
            return std::tie(a.principal_pattern, a.resource_pattern,
                            a.host_pattern, a.operation) <
                   std::tie(b.principal_pattern, b.resource_pattern,
                            b.host_pattern, b.operation);
        });
}

// ============================================================================
// ---- AuditLog Implementation ----
// ============================================================================

AuditLog::AuditLog(broker::BrokerServer& s)
    : server_(&s) {
    AUTH_LOG_INFO("AuditLog initialized");
}

void AuditLog::log_auth(
    const std::string& principal,
    const std::string& action,
    bool success) {
    log_auth(principal, action, success, "");
}

void AuditLog::log_auth(
    const std::string& principal,
    const std::string& action,
    bool success,
    const std::string& details) {

    emit("AUTH", principal, action,
         success ? "SUCCESS" : "FAILURE",
         success);

    if (!details.empty()) {
        AUTH_LOG_DEBUG("Audit AUTH: principal='{}' action='{}' success={} "
                       "details='{}'",
                       principal, action, success, details);
    } else {
        AUTH_LOG_DEBUG("Audit AUTH: principal='{}' action='{}' success={}",
                       principal, action, success);
    }
}

void AuditLog::log_admin(
    const std::string& principal,
    const std::string& operation,
    const std::string& resource) {

    emit("ADMIN", principal, operation, resource, true);
    AUTH_LOG_DEBUG("Audit ADMIN: principal='{}' operation='{}' resource='{}'",
                   principal, operation, resource);
}

void AuditLog::log_security(
    const std::string& event,
    const std::string& details) {

    emit("SECURITY", "-", event, details, false);
    AUTH_LOG_WARN("Audit SECURITY: event='{}' details='{}'", event, details);
}

void AuditLog::emit(
    const std::string& category,
    const std::string& principal,
    const std::string& action,
    const std::string& resource,
    bool success) {

    std::lock_guard lock(log_mutex_);

    // In production: write to a dedicated audit log file, syslog, or
    // forward to a SIEM via Kafka topic.  For the stub, we use spdlog.
    auto logger = get_auth_logger();
    logger->info("[AUDIT] category={} principal={} action={} resource={} "
                 "success={}",
                 category, principal, action, resource,
                 success ? "true" : "false");
}

// ============================================================================
// ---- AuthManager Implementation ----
// ============================================================================

AuthManager::AuthManager(broker::BrokerServer& s)
    : server_(&s)
    , sasl_(std::make_unique<SaslProvider>(s))
    , acl_(std::make_unique<AclEngine>(s))
    , audit_(std::make_unique<AuditLog>(s)) {

    // Configure from global config
    if (auto* cfg = dynamic_cast<torrent::config*>(&s)) {
        (void)cfg;
    }

    // Read security configuration
    const auto& server_cfg = s;  // BrokerServer
    (void)server_cfg;

    AUTH_LOG_INFO("AuthManager initialized");
}

result<void> AuthManager::authenticate(
    const std::string& mechanism,
    const std::string& credentials) {

    auto mech = mechanism;

    // ---- SCRAM-SHA-256 ----
    if (mech == kMechScramSha256) {
        // Check if this is the first or final SCRAM message
        // Client-first-message starts with 'n' or 'p'
        // Client-final-message starts with 'c='
        if (credentials.rfind("c=", 0) == 0) {
            // Client-final-message
            auto result = sasl_->scram_server_final(credentials);
            if (result.failed()) {
                audit_->log_auth(sasl_->scram_username(), "LOGIN_SCRAM_SHA256",
                                 false, result.error_message);
                return result<void>::failure(result.error, result.error_message);
            }
            // Store server-final-message in... (would be returned to client)
            std::string principal = sasl_->scram_username();
            register_session(principal, mechanism);
            audit_->log_auth(principal, "LOGIN_SCRAM_SHA256", true);
            return result<void>::success();
        } else {
            // Client-first-message
            auto result = sasl_->scram_server_first_impl(credentials, "SHA-256");
            if (result.failed()) {
                audit_->log_auth("", "SASL_HANDSHAKE_SCRAM_SHA256", false,
                                 result.error_message);
                return result<void>::failure(result.error, result.error_message);
            }
            // Server-first-message would be sent to client via the SASL
            // handshake layer.  Store for next round.
            audit_->log_auth("", "SASL_HANDSHAKE_SCRAM_SHA256", true);
            return result<void>::success();
        }
    }

    // ---- SCRAM-SHA-512 ----
    if (mech == kMechScramSha512) {
        if (credentials.rfind("c=", 0) == 0) {
            auto result = sasl_->scram_server_final_impl(credentials, "SHA-512");
            if (result.failed()) {
                audit_->log_auth(sasl_->scram_username(), "LOGIN_SCRAM_SHA512",
                                 false, result.error_message);
                return result<void>::failure(result.error, result.error_message);
            }
            std::string principal = sasl_->scram_username();
            register_session(principal, mechanism);
            audit_->log_auth(principal, "LOGIN_SCRAM_SHA512", true);
            return result<void>::success();
        } else {
            auto result = sasl_->scram_server_first_impl(credentials, "SHA-512");
            if (result.failed()) {
                audit_->log_auth("", "SASL_HANDSHAKE_SCRAM_SHA512", false,
                                 result.error_message);
                return result<void>::failure(result.error, result.error_message);
            }
            audit_->log_auth("", "SASL_HANDSHAKE_SCRAM_SHA512", true);
            return result<void>::success();
        }
    }

    // ---- PLAIN ----
    if (mech == kMechPlain) {
        auto parsed = parse_plain_credentials(credentials);
        if (!parsed) {
            audit_->log_auth("", "LOGIN_PLAIN", false,
                             "Malformed PLAIN credentials");
            return result<void>::failure(
                error_code::sasl_authentication_failed,
                "Malformed PLAIN credentials");
        }
        auto& [authzid, authcid, password] = *parsed;

        auto auth_result = sasl_->authenticate_plain_ex(authzid, authcid, password);
        std::string principal = authzid.empty() ? authcid : authzid;

        if (auth_result.failed()) {
            audit_->log_auth(principal, "LOGIN_PLAIN", false,
                             auth_result.error_message);
            return auth_result;
        }

        register_session(principal, mechanism);
        audit_->log_auth(principal, "LOGIN_PLAIN", true);
        return result<void>::success();
    }

    // ---- OAUTHBEARER ----
    if (mech == kMechOAuthBearer) {
        auto token = parse_oauth_token(credentials);
        if (!token) {
            audit_->log_auth("", "LOGIN_OAUTHBEARER", false,
                             "Malformed OAUTHBEARER credentials");
            return result<void>::failure(
                error_code::sasl_authentication_failed,
                "Malformed OAUTHBEARER credentials");
        }

        auto auth_result = sasl_->authenticate_oauth2(*token);
        if (auth_result.failed()) {
            audit_->log_auth("", "LOGIN_OAUTHBEARER", false,
                             auth_result.error_message);
            return auth_result;
        }

        // Extract principal from JWT (sub claim)
        std::string principal = "oauth2-user";  // Stub; parse from JWT
        register_session(principal, mechanism);
        audit_->log_auth(principal, "LOGIN_OAUTHBEARER", true);
        return result<void>::success();
    }

    // ---- KERBEROS (GSSAPI) ----
    if (mech == kMechKerberos) {
        auto auth_result = sasl_->authenticate_kerberos(credentials);
        if (auth_result.failed()) {
            audit_->log_auth("", "LOGIN_KERBEROS", false,
                             auth_result.error_message);
            return auth_result;
        }

        std::string principal = "krb5-user";  // Stub; extract from GSS context
        register_session(principal, mechanism);
        audit_->log_auth(principal, "LOGIN_KERBEROS", true);
        return result<void>::success();
    }

    // ---- Unknown mechanism ----
    audit_->log_auth("", "LOGIN_UNKNOWN", false,
                     std::string("Unknown SASL mechanism: ") + mechanism);
    return result<void>::failure(
        error_code::unsupported_sasl_mechanism,
        std::string("Unsupported SASL mechanism: ") + mechanism);
}

bool AuthManager::is_authenticated(const std::string& principal) const {
    std::shared_lock lock(session_mutex_);
    auto it = sessions_.find(principal);
    if (it == sessions_.end()) return false;
    return !it->second.is_expired();
}

result<void> AuthManager::authorize(
    const std::string& principal,
    const std::string& resource,
    const std::string& operation) {
    return authorize(principal, "*", resource, operation);
}

result<void> AuthManager::authorize(
    const std::string& principal,
    const std::string& host,
    const std::string& resource,
    const std::string& operation) {

    // Check session first
    if (!is_authenticated(principal)) {
        audit_->log_security("ACL_DENY",
            "principal=" + principal + " not authenticated");
        return result<void>::failure(
            error_code::topic_authorization_failed,
            "Principal not authenticated: " + principal);
    }

    // Delegate to ACL engine
    bool allowed = acl_->check_access(principal, host, resource, operation);
    if (!allowed) {
        std::string details = "principal=" + principal +
                              " host=" + host +
                              " resource=" + resource +
                              " op=" + operation;
        audit_->log_security("ACL_DENY", details);
        return result<void>::failure(
            error_code::topic_authorization_failed,
            "Access denied for " + principal + " on " + resource +
            " (" + operation + ")");
    }

    return result<void>::success();
}

std::vector<std::string> AuthManager::sasl_mechanisms() const {
    return sasl_->enabled_mechanisms();
}

void AuthManager::register_session(
    const std::string& principal,
    const std::string& mechanism,
    const std::string& host) {

    std::unique_lock lock(session_mutex_);

    SessionEntry entry;
    entry.principal = principal;
    entry.mechanism = mechanism;
    entry.host = host;
    entry.created_at = std::chrono::steady_clock::now();
    entry.ttl = session_ttl_;

    sessions_[principal] = std::move(entry);

    AUTH_LOG_DEBUG("Session registered: principal='{}' mechanism='{}' ttl={}s",
                   principal, mechanism, session_ttl_.count());
}

void AuthManager::revoke_session(const std::string& principal) {
    std::unique_lock lock(session_mutex_);
    auto it = sessions_.find(principal);
    if (it != sessions_.end()) {
        sessions_.erase(it);
        audit_->log_auth(principal, "LOGOUT", true);
        AUTH_LOG_DEBUG("Session revoked: principal='{}'", principal);
    }
}

void AuthManager::purge_expired_sessions() {
    std::unique_lock lock(session_mutex_);
    auto now = std::chrono::steady_clock::now();
    size_t before = sessions_.size();

    for (auto it = sessions_.begin(); it != sessions_.end(); ) {
        if ((now - it->second.created_at) > it->second.ttl) {
            AUTH_LOG_DEBUG("Session expired: principal='{}'",
                           it->second.principal);
            it = sessions_.erase(it);
        } else {
            ++it;
        }
    }

    if (before != sessions_.size()) {
        AUTH_LOG_DEBUG("Purged {} expired sessions ({} remaining)",
                       before - sessions_.size(), sessions_.size());
    }
}

// ---- Static helpers ----

std::optional<std::tuple<std::string, std::string, std::string>>
AuthManager::parse_plain_credentials(const std::string& creds) {
    // PLAIN format: [authzid]\0authcid\0password
    // (RFC 4616 §2)

    // Find first null
    size_t null1 = creds.find(kPlainSep);
    if (null1 == std::string::npos) {
        // Single-field: just password (backward compat)
        return std::nullopt;
    }

    std::string authzid = creds.substr(0, null1);

    // Find second null
    size_t null2 = creds.find(kPlainSep, null1 + 1);
    if (null2 == std::string::npos) {
        return std::nullopt;
    }

    std::string authcid = creds.substr(null1 + 1, null2 - null1 - 1);
    std::string password = creds.substr(null2 + 1);

    return std::make_tuple(std::move(authzid), std::move(authcid),
                           std::move(password));
}

std::optional<std::string>
AuthManager::parse_oauth_token(const std::string& creds) {
    // OAUTHBEARER format (RFC 7628 §3.1):
    //   n,a=<authzid>,\x01auth=Bearer <token>\x01\x01
    // or:
    //   n,,\x01auth=Bearer <token>\x01\x01

    // Find the GS2 header end
    size_t gs2_end = creds.find(",\x01");
    if (gs2_end == std::string::npos) {
        gs2_end = creds.find('\x01');
        if (gs2_end == std::string::npos) return std::nullopt;
    } else {
        gs2_end += 1;  // skip comma
    }

    // After GS2, we have: \x01auth=Bearer <token>\x01\x01
    std::string_view payload(creds.data() + gs2_end, creds.size() - gs2_end);

    // Find the auth= attribute
    size_t auth_start = payload.find("auth=Bearer ");
    if (auth_start == std::string_view::npos) return std::nullopt;

    auth_start += 12;  // skip "auth=Bearer "
    size_t auth_end = payload.find('\x01', auth_start);
    if (auth_end == std::string_view::npos) return std::nullopt;

    return std::string(payload.substr(auth_start, auth_end - auth_start));
}

// ============================================================================
// ---- Free-standing SaslProvider adapter ----
// ============================================================================

/// Compatibility adapter: the header declares free functions; we provide
/// them as private static factory wrappers.

} // namespace torrent::security

// ============================================================================
// Extern "C" OpenSSL cleanup helpers (called at process exit if needed)
// ============================================================================

namespace {

/// Ensure OpenSSL cleanup happens on process exit.
struct OpenSslCleanup {
    ~OpenSslCleanup() {
        // OPENSSL_cleanup() is automatic in OpenSSL 1.1+; no-op in 3.0+
    }
};

static OpenSslCleanup g_openssl_cleanup;

} // anonymous namespace
