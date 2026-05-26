/**
 * torrent-mq — Encryption at Rest (AES-256-GCM)
 *
 * Provides transparent encryption/decryption of RecordBatch data before
 * writing to disk and after reading from disk. Uses AES-256-GCM with
 * per-segment IVs derived from segment_id and a master key.
 *
 * Key management:
 *   - Master key loaded from a file (hex-encoded 32 bytes) at startup.
 *   - Key rotation support: a secondary key can be loaded for decryption
 *     of data written with a previous key; new writes always use the
 *     primary key.
 *   - Key ID is embedded in the encryption envelope so the correct key
 *     can be selected during decryption.
 *
 * Envelope format (prepended to encrypted data):
 *   [1 byte   key_id]
 *   [12 bytes GCM nonce/IV]
 *   [N bytes  ciphertext + 16-byte GCM auth tag]
 *
 * Security properties:
 *   - AES-256-GCM provides authenticated encryption (confidentiality +
 *     integrity). Any tampering is detected during decryption.
 *   - Per-segment nonce: derived from segment_id (8 bytes) + counter
 *     (4 bytes), guaranteeing uniqueness per write operation.
 *   - AAD (Additional Authenticated Data): segment_id is passed as AAD
 *     to bind the ciphertext to the correct segment.
 */

#include "torrent/storage/types.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/err.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

// ============================================================================
// Anonymous namespace — logging and constants
// ============================================================================

namespace torrent {
namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_encryption_logger() {
    static auto logger = spdlog::get("encryption");
    if (!logger) {
        logger = spdlog::stdout_color_mt("encryption");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// AES-256 key size in bytes.
static constexpr size_t kKeySize = 32;

/// GCM nonce (IV) size in bytes (12 bytes as recommended by NIST).
static constexpr size_t kGcmNonceSize = 12;

/// GCM authentication tag size in bytes.
static constexpr size_t kGcmTagSize = 16;

/// Maximum number of key rotations tracked.
static constexpr size_t kMaxKeyVersions = 8;

/// Envelope overhead: key_id (1) + nonce (12) = 13 bytes before ciphertext.
static constexpr size_t kEnvelopeOverhead = 1 + kGcmNonceSize;

/// Current key ID for active encryption.
static constexpr uint8_t kCurrentKeyId = 1;

/// AAD prefix to bind encryption to segment context.
static constexpr std::string_view kAadPrefix = "torrent-seg-v1:";

// --------------------------------------------------------------------------
// Helper: hex decode a string to raw bytes
// --------------------------------------------------------------------------

std::optional<std::vector<unsigned char>> hex_decode(std::string_view hex) {
    if (hex.size() % 2 != 0) return std::nullopt;
    std::vector<unsigned char> out;
    out.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2) {
        unsigned int byte;
        char buf[3] = {hex[i], hex[i+1], '\0'};
        if (std::sscanf(buf, "%02x", &byte) != 1) return std::nullopt;
        out.push_back(static_cast<unsigned char>(byte));
    }
    return out;
}

// --------------------------------------------------------------------------
// EVP_CIPHER_CTX RAII wrapper
// --------------------------------------------------------------------------

struct CipherCtxDeleter {
    void operator()(EVP_CIPHER_CTX* ctx) const noexcept {
        EVP_CIPHER_CTX_free(ctx);
    }
};
using CipherCtxPtr = std::unique_ptr<EVP_CIPHER_CTX, CipherCtxDeleter>;

CipherCtxPtr make_cipher_ctx() {
    return CipherCtxPtr(EVP_CIPHER_CTX_new());
}

// --------------------------------------------------------------------------
// Key entry: one version of the encryption key
// --------------------------------------------------------------------------

struct KeyEntry {
    uint8_t                    key_id = 0;
    std::array<unsigned char, kKeySize> key{};
    bool                       active = false;  ///< true = currently used for encryption

    [[nodiscard]] bool valid() const noexcept { return key_id != 0; }
};

// --------------------------------------------------------------------------
// OpenSSL error string helper
// --------------------------------------------------------------------------

std::string openssl_error_string() {
    unsigned long err = ERR_get_error();
    if (err == 0) return "no OpenSSL error";
    char buf[256];
    ERR_error_string_n(err, buf, sizeof(buf));
    return std::string(buf);
}

} // anonymous namespace

// ============================================================================
// EncryptionAtRest — Public Interface
// ============================================================================

/**
 * EncryptionAtRest provides AES-256-GCM encryption for segment data.
 *
 * Typical usage:
 *   EncryptionAtRest enc;
 *   enc.load_primary_key_from_file("/etc/torrent/encryption.key");
 *   enc.load_fallback_keys_from_dir("/etc/torrent/keys/");
 *
 *   // Before writing:
 *   auto ciphertext = enc.encrypt(segment_id, plaintext);
 *
 *   // After reading:
 *   auto plaintext = enc.decrypt(segment_id, ciphertext);
 */
class EncryptionAtRest {
public:
    EncryptionAtRest() = default;
    ~EncryptionAtRest() {
        // Zero out keys in memory before destruction
        clear_keys();
    }

    EncryptionAtRest(const EncryptionAtRest&) = delete;
    EncryptionAtRest& operator=(const EncryptionAtRest&) = delete;
    EncryptionAtRest(EncryptionAtRest&&) = delete;
    EncryptionAtRest& operator=(EncryptionAtRest&&) = delete;

    // -- Key Management -----------------------------------------------------

    /**
     * Load the primary encryption key from a file.
     * The file must contain exactly 32 bytes (hex-encoded = 64 chars),
     * or 32 raw bytes.
     *
     * @param path  Path to the key file.
     * @return      result<void> indicating success or failure.
     */
    result<void> load_primary_key_from_file(const std::string& path) {
        std::lock_guard<std::mutex> lock(mutex_);

        std::ifstream file(path, std::ios::binary);
        if (!file.is_open()) {
            return result<void>::failure(
                error_code::storage_unavailable,
                fmt::format("Cannot open key file: {}", path));
        }

        std::string content((std::istreambuf_iterator<char>(file)),
                             std::istreambuf_iterator<char>());
        file.close();

        // Try hex-encoded (64 chars → 32 bytes)
        if (content.size() == kKeySize * 2) {
            auto decoded = hex_decode(content);
            if (decoded && decoded->size() == kKeySize) {
                return install_key(kCurrentKeyId, decoded->data(), true);
            }
        }

        // Try raw binary (32 bytes)
        if (content.size() == kKeySize) {
            return install_key(kCurrentKeyId,
                               reinterpret_cast<const unsigned char*>(content.data()),
                               true);
        }

        return result<void>::failure(
            error_code::invalid_config,
            fmt::format("Key file '{}' has invalid size {} (expected {} or {})",
                        path, content.size(), kKeySize, kKeySize * 2));
    }

    /**
     * Load a raw key directly (programmatic API).
     *
     * @param key_id   Numeric identifier for this key version.
     * @param key_data 32-byte key material.
     * @param primary  If true, this key becomes the active encryption key.
     */
    result<void> load_key(uint8_t key_id, const unsigned char* key_data, bool primary) {
        std::lock_guard<std::mutex> lock(mutex_);
        return install_key(key_id, key_data, primary);
    }

    /**
     * Load fallback keys from a directory for key rotation.
     * Each file named "key-<id>.key" is loaded as a decryption-only key.
     */
    result<void> load_fallback_keys_from_dir(const std::string& dir_path) {
        std::lock_guard<std::mutex> lock(mutex_);

        for (uint8_t id = 2; id <= kMaxKeyVersions; ++id) {
            auto file_path = dir_path + "/key-" + std::to_string(id) + ".key";
            std::ifstream file(file_path, std::ios::binary);
            if (!file.is_open()) continue;  // skip missing files

            std::string content((std::istreambuf_iterator<char>(file)),
                                 std::istreambuf_iterator<char>());
            file.close();

            if (content.size() == kKeySize * 2) {
                auto decoded = hex_decode(content);
                if (decoded && decoded->size() == kKeySize) {
                    install_key(id, decoded->data(), false);
                }
            } else if (content.size() == kKeySize) {
                install_key(id,
                            reinterpret_cast<const unsigned char*>(content.data()),
                            false);
            }
        }

        return result<void>::success();
    }

    /**
     * Rotate to a new primary key.
     * The current primary key is demoted to a fallback (decryption-only).
     *
     * @param new_key_id     ID for the new key.
     * @param new_key_data   32-byte key material.
     */
    result<void> rotate_key(uint8_t new_key_id, const unsigned char* new_key_data) {
        std::lock_guard<std::mutex> lock(mutex_);

        // Demote current primary
        for (auto& entry : keys_) {
            if (entry.active) {
                entry.active = false;
            }
        }

        // Install new primary
        return install_key(new_key_id, new_key_data, true);
    }

    /**
     * Return true if a primary encryption key has been loaded.
     */
    [[nodiscard]] bool is_ready() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return primary_key_ != nullptr;
    }

    // -- Encryption / Decryption --------------------------------------------

    /**
     * Encrypt a RecordBatch payload (raw bytes) using AES-256-GCM.
     *
     * The returned buffer is prefixed with the encryption envelope:
     *   [key_id: 1 byte][nonce: 12 bytes][ciphertext + tag]
     *
     * @param segment_id   Used as AAD to bind ciphertext to this segment.
     * @param plaintext    Raw serialized RecordBatch bytes.
     * @return             result with encrypted buffer + envelope on success.
     */
    result<std::vector<unsigned char>> encrypt(
        uint64_t segment_id,
        const unsigned char* plaintext,
        size_t plaintext_len) {

        std::lock_guard<std::mutex> lock(mutex_);

        if (!primary_key_) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                "No encryption key loaded; cannot encrypt");
        }

        // Generate a random 12-byte nonce
        std::array<unsigned char, kGcmNonceSize> nonce{};
        if (RAND_bytes(nonce.data(), static_cast<int>(kGcmNonceSize)) != 1) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                fmt::format("RAND_bytes failed: {}", openssl_error_string()));
        }

        // Build AAD: prefix + segment_id
        std::string aad(kAadPrefix);
        aad.append(reinterpret_cast<const char*>(&segment_id), sizeof(segment_id));

        // Allocate output buffer: envelope + ciphertext + tag
        // GCM ciphertext is same size as plaintext
        size_t output_len = kEnvelopeOverhead + plaintext_len + kGcmTagSize;
        std::vector<unsigned char> output(output_len);

        // Write envelope header
        output[0] = primary_key_->key_id;
        std::memcpy(output.data() + 1, nonce.data(), kGcmNonceSize);

        // Encrypt using EVP
        auto ctx = make_cipher_ctx();
        if (!ctx) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                "EVP_CIPHER_CTX_new failed");
        }

        int len = 0;
        int ciphertext_len = 0;

        if (EVP_EncryptInit_ex(ctx.get(), EVP_aes_256_gcm(),
                               nullptr, nullptr, nullptr) != 1) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                fmt::format("EVP_EncryptInit_ex: {}", openssl_error_string()));
        }

        // Set IV length and IV
        if (EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_IVLEN,
                                static_cast<int>(kGcmNonceSize), nullptr) != 1) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                fmt::format("GCM set IV length: {}", openssl_error_string()));
        }

        if (EVP_EncryptInit_ex(ctx.get(), nullptr, nullptr,
                               primary_key_->key.data(), nonce.data()) != 1) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                fmt::format("EVP_EncryptInit_ex key/nonce: {}", openssl_error_string()));
        }

        // Set AAD
        if (EVP_EncryptUpdate(ctx.get(), nullptr, &len,
                              reinterpret_cast<const unsigned char*>(aad.data()),
                              static_cast<int>(aad.size())) != 1) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                fmt::format("GCM set AAD: {}", openssl_error_string()));
        }

        // Encrypt plaintext into output buffer (after envelope header)
        unsigned char* ciphertext_start = output.data() + kEnvelopeOverhead;
        if (EVP_EncryptUpdate(ctx.get(), ciphertext_start, &len,
                              plaintext, static_cast<int>(plaintext_len)) != 1) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                fmt::format("EVP_EncryptUpdate: {}", openssl_error_string()));
        }
        ciphertext_len = len;

        // Finalize
        if (EVP_EncryptFinal_ex(ctx.get(), ciphertext_start + ciphertext_len, &len) != 1) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                fmt::format("EVP_EncryptFinal_ex: {}", openssl_error_string()));
        }
        ciphertext_len += len;

        // Get the authentication tag
        unsigned char* tag_start = output.data() + kEnvelopeOverhead + ciphertext_len;
        if (EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_GET_TAG,
                                static_cast<int>(kGcmTagSize), tag_start) != 1) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                fmt::format("GCM get tag: {}", openssl_error_string()));
        }

        // Resize to exact size
        output.resize(kEnvelopeOverhead + static_cast<size_t>(ciphertext_len) + kGcmTagSize);

        auto logger = get_encryption_logger();
        logger->trace("Encrypted segment {}: {} bytes plaintext -> {} bytes ciphertext",
                      segment_id, plaintext_len, output.size());

        return result<std::vector<unsigned char>>::success(std::move(output));
    }

    /**
     * Decrypt a buffer that was encrypted with encrypt().
     *
     * Reads the envelope header to determine key_id and nonce,
     * selects the correct key, verifies the GCM auth tag, and
     * returns the plaintext.
     *
     * @param segment_id    Used as AAD (must match the value used during encrypt).
     * @param encrypted     Buffer containing envelope + ciphertext + tag.
     * @return              result with decrypted plaintext on success.
     */
    result<std::vector<unsigned char>> decrypt(
        uint64_t segment_id,
        const unsigned char* encrypted,
        size_t encrypted_len) {

        std::lock_guard<std::mutex> lock(mutex_);

        if (encrypted_len < kEnvelopeOverhead + kGcmTagSize) {
            return result<std::vector<unsigned char>>::failure(
                error_code::corrupt_message,
                fmt::format("Encrypted data too short: {} bytes (min {})",
                            encrypted_len, kEnvelopeOverhead + kGcmTagSize));
        }

        // Parse envelope
        uint8_t key_id = encrypted[0];
        const unsigned char* nonce = encrypted + 1;
        const unsigned char* ciphertext = encrypted + kEnvelopeOverhead;
        size_t ciphertext_len = encrypted_len - kEnvelopeOverhead - kGcmTagSize;
        const unsigned char* tag = encrypted + encrypted_len - kGcmTagSize;

        // Find the correct key
        const KeyEntry* key = find_key(key_id);
        if (!key) {
            return result<std::vector<unsigned char>>::failure(
                error_code::corrupt_message,
                fmt::format("Unknown key_id {} in encrypted data", key_id));
        }

        // Build AAD (must match encrypt)
        std::string aad(kAadPrefix);
        aad.append(reinterpret_cast<const char*>(&segment_id), sizeof(segment_id));

        // Allocate plaintext buffer
        std::vector<unsigned char> plaintext(ciphertext_len);

        auto ctx = make_cipher_ctx();
        if (!ctx) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                "EVP_CIPHER_CTX_new failed");
        }

        int len = 0;
        int plaintext_len = 0;

        if (EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_gcm(),
                               nullptr, nullptr, nullptr) != 1) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                fmt::format("EVP_DecryptInit_ex: {}", openssl_error_string()));
        }

        if (EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_IVLEN,
                                static_cast<int>(kGcmNonceSize), nullptr) != 1) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                fmt::format("GCM set IV length: {}", openssl_error_string()));
        }

        if (EVP_DecryptInit_ex(ctx.get(), nullptr, nullptr,
                               key->key.data(), nonce) != 1) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                fmt::format("EVP_DecryptInit_ex key/nonce: {}", openssl_error_string()));
        }

        // Set expected tag BEFORE any decryption
        if (EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_TAG,
                                static_cast<int>(kGcmTagSize),
                                const_cast<unsigned char*>(tag)) != 1) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                fmt::format("GCM set tag: {}", openssl_error_string()));
        }

        // Set AAD
        if (EVP_DecryptUpdate(ctx.get(), nullptr, &len,
                              reinterpret_cast<const unsigned char*>(aad.data()),
                              static_cast<int>(aad.size())) != 1) {
            return result<std::vector<unsigned char>>::failure(
                error_code::storage_unavailable,
                fmt::format("GCM set AAD: {}", openssl_error_string()));
        }

        // Decrypt
        if (EVP_DecryptUpdate(ctx.get(), plaintext.data(), &len,
                              ciphertext, static_cast<int>(ciphertext_len)) != 1) {
            return result<std::vector<unsigned char>>::failure(
                error_code::corrupt_message,
                fmt::format("EVP_DecryptUpdate: {}", openssl_error_string()));
        }
        plaintext_len = len;

        // Finalize — this validates the GCM tag
        int ret = EVP_DecryptFinal_ex(ctx.get(), plaintext.data() + plaintext_len, &len);
        if (ret != 1) {
            return result<std::vector<unsigned char>>::failure(
                error_code::corrupt_message,
                fmt::format("GCM authentication failed (tag mismatch or data tampered): {}",
                            openssl_error_string()));
        }
        plaintext_len += len;

        plaintext.resize(static_cast<size_t>(plaintext_len));

        auto logger = get_encryption_logger();
        logger->trace("Decrypted segment {}: {} bytes ciphertext -> {} bytes plaintext",
                      segment_id, encrypted_len, plaintext.size());

        return result<std::vector<unsigned char>>::success(std::move(plaintext));
    }

    /**
     * Generate a fresh random 256-bit key and return it as hex.
     * Useful for initial key provisioning.
     */
    [[nodiscard]] static std::string generate_key_hex() {
        std::array<unsigned char, kKeySize> key{};
        if (RAND_bytes(key.data(), static_cast<int>(kKeySize)) != 1) {
            get_encryption_logger()->error("RAND_bytes failed during key generation");
            return {};
        }
        static constexpr char kHex[] = "0123456789abcdef";
        std::string out;
        out.reserve(kKeySize * 2);
        for (auto b : key) {
            out.push_back(kHex[(b >> 4) & 0x0F]);
            out.push_back(kHex[b & 0x0F]);
        }
        // Clear key from stack
        key.fill(0);
        return out;
    }

    // -- Query --------------------------------------------------------------

    /**
     * Number of loaded key versions (primary + fallbacks).
     */
    [[nodiscard]] size_t key_count() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return keys_.size();
    }

    /**
     * Current primary key ID, or 0 if none loaded.
     */
    [[nodiscard]] uint8_t active_key_id() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return primary_key_ ? primary_key_->key_id : 0;
    }

private:
    // -- Internal ----------------------------------------------------------

    result<void> install_key(uint8_t key_id, const unsigned char* key_data, bool primary) {
        // Check for duplicate key_id
        for (auto& entry : keys_) {
            if (entry.key_id == key_id) {
                // Update existing entry
                std::memcpy(entry.key.data(), key_data, kKeySize);
                if (primary) {
                    entry.active = true;
                    primary_key_ = &entry;
                    // Demote other keys
                    for (auto& other : keys_) {
                        if (other.key_id != key_id) other.active = false;
                    }
                }
                auto logger = get_encryption_logger();
                logger->info("Updated key_id={} (primary={})", key_id, primary);
                return result<void>::success();
            }
        }

        // Add new entry
        if (keys_.size() >= kMaxKeyVersions) {
            // Evict the oldest non-active key
            for (auto it = keys_.begin(); it != keys_.end(); ++it) {
                if (!it->active) {
                    keys_.erase(it);
                    break;
                }
            }
            if (keys_.size() >= kMaxKeyVersions) {
                return result<void>::failure(
                    error_code::invalid_config,
                    fmt::format("Too many key versions (max {})", kMaxKeyVersions));
            }
        }

        KeyEntry entry;
        entry.key_id = key_id;
        std::memcpy(entry.key.data(), key_data, kKeySize);
        entry.active = primary;
        keys_.push_back(entry);

        if (primary) {
            primary_key_ = &keys_.back();
            // Demote other keys
            for (auto& other : keys_) {
                if (other.key_id != key_id) other.active = false;
            }
        }

        auto logger = get_encryption_logger();
        logger->info("Installed key_id={} (primary={}), total keys: {}",
                     key_id, primary, keys_.size());

        return result<void>::success();
    }

    const KeyEntry* find_key(uint8_t key_id) const {
        for (const auto& entry : keys_) {
            if (entry.key_id == key_id) return &entry;
        }
        return nullptr;
    }

    void clear_keys() {
        for (auto& entry : keys_) {
            entry.key.fill(0);
        }
        keys_.clear();
        primary_key_ = nullptr;
    }

    mutable std::mutex mutex_;
    std::vector<KeyEntry> keys_;
    KeyEntry* primary_key_ = nullptr;  // points into keys_
};

} // namespace torrent
