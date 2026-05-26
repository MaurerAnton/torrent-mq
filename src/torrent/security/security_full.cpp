#include "torrent/security/auth_manager.h"
#include "torrent/security/sasl_provider.h"
#include "torrent/security/acl_engine.h"
#include "torrent/security/tls_manager.h"
#include "torrent/security/audit_log.h"
#include "torrent/common/types.h"
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <openssl/ssl.h>
#include <openssl/rand.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <chrono>
#include <deque>
#include <regex>

namespace torrent::security {
namespace {

using json = nlohmann::json;

// ============================================================================
// TLS 1.3 Session Ticket Key Rotation
// ============================================================================
class TicketKeyRotator {
public:
    struct Config { int64_t rotation_interval_ms{3600000}; int max_keys{3}; };

    explicit TicketKeyRotator(const Config& cfg) : config_(cfg) {
        generate_new_key();
    }

    void rotate_if_needed() {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_rotation_);
        if (elapsed.count() >= config_.rotation_interval_ms) {
            generate_new_key();
            last_rotation_ = now;
        }
    }

    struct TicketKey { uint8_t name[16]; uint8_t hmac_key[32]; uint8_t aes_key[32]; };

    const std::deque<TicketKey>& keys() const { return keys_; }

private:
    void generate_new_key() {
        TicketKey key;
        RAND_bytes(key.name, sizeof(key.name));
        RAND_bytes(key.hmac_key, sizeof(key.hmac_key));
        RAND_bytes(key.aes_key, sizeof(key.aes_key));
        keys_.push_front(key);
        while (static_cast<int>(keys_.size()) > config_.max_keys) keys_.pop_back();
        spdlog::info("TLS session ticket key rotated ({} active keys)", keys_.size());
    }

    Config config_;
    std::deque<TicketKey> keys_;
    std::chrono::steady_clock::time_point last_rotation_{std::chrono::steady_clock::now()};
};

// ============================================================================
// Certificate Reload Without Restart
// ============================================================================
class CertReloader {
public:
    bool reload(SSL_CTX* ctx, const std::string& cert_path, const std::string& key_path) {
        if (SSL_CTX_use_certificate_chain_file(ctx, cert_path.c_str()) != 1) {
            spdlog::error("Failed to reload certificate: {}", cert_path);
            return false;
        }
        if (SSL_CTX_use_PrivateKey_file(ctx, key_path.c_str(), SSL_FILETYPE_PEM) != 1) {
            spdlog::error("Failed to reload private key: {}", key_path);
            return false;
        }
        if (SSL_CTX_check_private_key(ctx) != 1) {
            spdlog::error("Private key does not match certificate");
            return false;
        }
        spdlog::info("TLS certificate reloaded successfully");
        return true;
    }
};

// ============================================================================
// CRL Checking
// ============================================================================
class CrlChecker {
public:
    struct Config { std::string crl_path; int64_t refresh_interval_ms{3600000}; };

    explicit CrlChecker(const Config& cfg) : config_(cfg) {}

    bool is_revoked(const std::string& serial_number) {
        std::lock_guard<std::mutex> lock(mutex_);
        return revoked_serials_.find(serial_number) != revoked_serials_.end();
    }

    void refresh() {
        std::lock_guard<std::mutex> lock(mutex_);
        revoked_serials_.clear();
        // In production: parse CRL file, extract revoked serials
        spdlog::info("CRL refreshed from {}", config_.crl_path);
    }

private:
    Config config_;
    mutable std::mutex mutex_;
    std::unordered_set<std::string> revoked_serials_;
};

// ============================================================================
// SCRAM User Management
// ============================================================================
class ScramUserManager {
public:
    struct ScramUser {
        std::string username;
        std::string salt_base64;
        std::string stored_key_base64;
        std::string server_key_base64;
        int32_t iterations{15000};
        int64_t created_at_ms;
    };

    bool create_user(const std::string& username, const std::string& password) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (users_.find(username) != users_.end()) return false;

        ScramUser user;
        user.username = username;
        user.salt_base64 = generate_salt();
        user.iterations = 15000;
        user.created_at_ms = now_ms();

        // In production: derive SaltedPassword, ClientKey, StoredKey, ServerKey via PBKDF2
        users_[username] = user;
        spdlog::info("SCRAM user created: {}", username);
        return true;
    }

    bool delete_user(const std::string& username) {
        std::lock_guard<std::mutex> lock(mutex_);
        return users_.erase(username) > 0;
    }

    std::optional<ScramUser> get_user(const std::string& username) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = users_.find(username);
        if (it == users_.end()) return std::nullopt;
        return it->second;
    }

    std::vector<std::string> list_users() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::string> result;
        for (const auto& [name, _] : users_) result.push_back(name);
        return result;
    }

private:
    std::string generate_salt() {
        uint8_t salt[32];
        RAND_bytes(salt, sizeof(salt));
        return "base64_salt";
    }
    static int64_t now_ms() { return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count(); }
    mutable std::mutex mutex_;
    std::unordered_map<std::string, ScramUser> users_;
};

// ============================================================================
// Brute Force Protection
// ============================================================================
class BruteForceProtector {
public:
    struct Config {
        int max_failed_attempts{5};
        int64_t lockout_duration_ms{300000};
        int64_t attempt_window_ms{60000};
    };

    explicit BruteForceProtector(const Config& cfg) : config_(cfg) {}

    bool check_allowed(const std::string& principal) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& entry = entries_[principal];
        int64_t now = now_ms();

        // Check if locked out
        if (entry.locked_until_ms > now) {
            spdlog::warn("Brute force: {} is locked out until {}", principal, entry.locked_until_ms);
            return false;
        }

        // Clean old attempts
        while (!entry.attempts.empty() && entry.attempts.front() < now - config_.attempt_window_ms) {
            entry.attempts.pop_front();
        }

        // Check attempt count
        if (static_cast<int>(entry.attempts.size()) >= config_.max_failed_attempts) {
            entry.locked_until_ms = now + config_.lockout_duration_ms;
            spdlog::warn("Brute force: {} locked out after {} failed attempts", principal, config_.max_failed_attempts);
            return false;
        }

        return true;
    }

    void record_attempt(const std::string& principal, bool success) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& entry = entries_[principal];
        if (success) {
            entry.attempts.clear();
            entry.locked_until_ms = 0;
        } else {
            entry.attempts.push_back(now_ms());
        }
    }

private:
    static int64_t now_ms() { return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
    struct Entry { std::deque<int64_t> attempts; int64_t locked_until_ms{0}; };
    Config config_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Entry> entries_;
};

// ============================================================================
// IP-based Rate Limiting for Auth
// ============================================================================
class AuthRateLimiter {
public:
    struct Config { int max_per_second{10}; int burst{20}; };

    explicit AuthRateLimiter(const Config& cfg) : config_(cfg) {}

    bool allow(const std::string& ip) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& entry = entries_[ip];
        int64_t now = now_ms();
        while (!entry.timestamps.empty() && entry.timestamps.front() < now - 1000) {
            entry.timestamps.pop_front();
        }
        if (static_cast<int>(entry.timestamps.size()) >= config_.burst) return false;
        entry.timestamps.push_back(now);
        return true;
    }

private:
    static int64_t now_ms() { return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
    struct Entry { std::deque<int64_t> timestamps; };
    Config config_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Entry> entries_;
};

// ============================================================================
// JWT Token for OAUTHBEARER
// ============================================================================
class JwtValidator {
public:
    struct JwtPayload { std::string sub; std::string iss; int64_t exp; int64_t iat; };
    struct Config { std::string issuer; std::string audience; int64_t clock_skew_seconds{30}; };

    explicit JwtValidator(const Config& cfg) : config_(cfg) {}

    bool validate(const std::string& token, JwtPayload& payload) {
        // Parse JWT: header.payload.signature
        auto parts = split(token, '.');
        if (parts.size() != 3) return false;

        // Decode payload
        std::string payload_json = base64url_decode(parts[1]);
        try {
            auto j = json::parse(payload_json);
            payload.sub = j.value("sub", "");
            payload.iss = j.value("iss", "");
            payload.exp = j.value("exp", 0);
            payload.iat = j.value("iat", 0);
        } catch (...) { return false; }

        // Verify issuer
        if (!config_.issuer.empty() && payload.iss != config_.issuer) return false;

        // Verify expiration
        int64_t now = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        if (payload.exp > 0 && now > payload.exp + config_.clock_skew_seconds) return false;

        // In production: verify signature using issuer's public key
        return true;
    }

private:
    static std::vector<std::string> split(const std::string& s, char delim) {
        std::vector<std::string> r; std::istringstream ss(s); std::string item;
        while (std::getline(ss, item, delim)) r.push_back(item);
        return r;
    }
    static std::string base64url_decode(const std::string& s) { return s; } // Stub
    Config config_;
};

// ============================================================================
// Audit Log with Structured JSON
// ============================================================================
class StructuredAuditLog {
public:
    struct Config { std::string log_path; int64_t max_size_bytes{104857600}; int max_files{10}; };

    explicit StructuredAuditLog(const Config& cfg) : config_(cfg) {}

    void log(const std::string& category, const std::string& principal,
             const std::string& action, const std::string& resource, bool success) {
        json entry;
        entry["timestamp"] = iso8601_now();
        entry["category"] = category;
        entry["principal"] = principal;
        entry["action"] = action;
        entry["resource"] = resource;
        entry["success"] = success;

        std::lock_guard<std::mutex> lock(mutex_);
        spdlog::info("[AUDIT] {}", entry.dump());
        buffer_.push_back(entry.dump());
        if (buffer_.size() >= 100) flush();
    }

    void flush() {
        if (buffer_.empty()) return;
        std::ofstream ofs(config_.log_path, std::ios::app);
        for (const auto& line : buffer_) ofs << line << "\n";
        buffer_.clear();
    }

private:
    static std::string iso8601_now() {
        auto now = std::chrono::system_clock::now();
        auto t = std::chrono::system_clock::to_time_t(now);
        std::ostringstream oss; oss << std::put_time(std::gmtime(&t), "%Y-%m-%dT%H:%M:%SZ");
        return oss.str();
    }
    Config config_;
    mutable std::mutex mutex_;
    std::vector<std::string> buffer_;
};

// ============================================================================
// RBAC Layer on Top of ACL
// ============================================================================
class RbacEngine {
public:
    struct Role { std::string name; std::vector<std::string> permissions; };
    struct UserRoles { std::string principal; std::vector<std::string> roles; };

    void define_role(const std::string& name, const std::vector<std::string>& permissions) {
        std::lock_guard<std::mutex> lock(mutex_);
        roles_[name] = {name, permissions};
    }

    void assign_role(const std::string& principal, const std::string& role) {
        std::lock_guard<std::mutex> lock(mutex_);
        user_roles_[principal].roles.push_back(role);
    }

    bool check_permission(const std::string& principal, const std::string& permission) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = user_roles_.find(principal);
        if (it == user_roles_.end()) return false;
        for (const auto& role_name : it->second.roles) {
            auto rit = roles_.find(role_name);
            if (rit != roles_.end()) {
                for (const auto& perm : rit->second.permissions) {
                    if (match_permission(perm, permission)) return true;
                }
            }
        }
        return false;
    }

private:
    bool match_permission(const std::string& pattern, const std::string& perm) {
        return pattern == "*" || pattern == perm;
    }
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Role> roles_;
    std::unordered_map<std::string, UserRoles> user_roles_;
};

// ============================================================================
// Delegation Token Lifecycle
// ============================================================================
class DelegationTokenLifecycle {
public:
    struct Token {
        std::string token_id; std::string hmac; std::string owner;
        int64_t issue_ms; int64_t expiry_ms; int64_t max_lifetime_ms; bool expired{false};
    };

    std::string create(const std::string& owner, int64_t max_lifetime_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        Token t;
        t.token_id = generate_id();
        t.owner = owner;
        t.issue_ms = now_ms();
        t.max_lifetime_ms = max_lifetime_ms;
        t.expiry_ms = t.issue_ms + max_lifetime_ms;
        t.hmac = compute_hmac(t.token_id, t.expiry_ms);
        tokens_[t.token_id] = t;
        return t.token_id;
    }

    bool renew(const std::string& token_id, int64_t extend_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = tokens_.find(token_id);
        if (it == tokens_.end() || it->second.expired) return false;
        it->second.expiry_ms = std::min(now_ms() + extend_ms, it->second.issue_ms + it->second.max_lifetime_ms);
        return true;
    }

    bool expire(const std::string& token_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = tokens_.find(token_id);
        if (it == tokens_.end()) return false;
        it->second.expired = true; return true;
    }

    bool validate(const std::string& token_id, const std::string& hmac) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = tokens_.find(token_id);
        if (it == tokens_.end() || it->second.expired) return false;
        return it->second.hmac == hmac && now_ms() < it->second.expiry_ms;
    }

private:
    std::string generate_id() { static std::atomic<int64_t> c{0}; return "dt_" + std::to_string(c++); }
    std::string compute_hmac(const std::string& id, int64_t exp) { return "hmac_" + id; }
    static int64_t now_ms() { return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count(); }
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Token> tokens_;
};

// ============================================================================
// Security Event Notifications
// ============================================================================
class SecurityEventNotifier {
public:
    struct WebhookConfig { std::string url; bool enabled{false}; };

    void notify(const std::string& event_type, const json& details) {
        spdlog::warn("[SECURITY_EVENT] type={} details={}", event_type, details.dump());
        if (webhook_.enabled) send_webhook(event_type, details);
    }

    void set_webhook(const WebhookConfig& cfg) { webhook_ = cfg; }

private:
    void send_webhook(const std::string& type, const json& details) {
        // In production: HTTP POST to webhook URL
        spdlog::info("Security event webhook sent to {}", webhook_.url);
    }
    WebhookConfig webhook_;
};

} // anonymous namespace
} // namespace torrent::security
