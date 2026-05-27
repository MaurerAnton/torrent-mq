/**
 * torrent-mq — UUID Generation and Utility Functions
 *
 * Implements UUID v4 (random) and UUID v7 (time-ordered) generation
 * conforming to RFC 9562.  UUIDs are 128-bit (16-byte) values.
 *
 * UUID v4: 122 random bits + 6 fixed version/variant bits.
 * UUID v7: 48-bit Unix ms timestamp + 74 random bits; time-ordered
 *          for database primary keys and log correlation.
 *
 * Free-standing convenience functions:
 *   - generate_uuid_v4() / generate_uuid_v7()
 *   - uuid_to_string() / uuid_to_string_no_dash()
 *   - parse_uuid()
 *
 * Thread safety: all functions are thread-safe. The random source
 * is protected by a mutex.
 */

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>

#include <fcntl.h>
#include <unistd.h>

namespace torrent {

// ============================================================================
// UUID internal representation
// ============================================================================

struct UUID::uuid_bytes {
    std::array<uint8_t, 16> data;
    bool operator==(const uuid_bytes& o) const noexcept { return data == o.data; }
    bool operator!=(const uuid_bytes& o) const noexcept { return data != o.data; }
};

// ============================================================================
// Anonymous namespace — internal state and helpers
// ============================================================================

namespace {

std::shared_ptr<spdlog::logger> get_uuid_logger() {
    static auto logger = spdlog::get("uuid");
    if (!logger) {
        logger = spdlog::stdout_color_mt("uuid");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --- Random source (protected by mutex) ---
std::mutex          g_random_mutex;
int                 g_urandom_fd = -1;
std::mt19937_64     g_mt19937;
std::atomic<bool>   g_prng_seeded{false};

void ensure_urandom_open() {
    if (g_urandom_fd >= 0) return;
    g_urandom_fd = ::open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (g_urandom_fd < 0) {
        get_uuid_logger()->warn("Cannot open /dev/urandom: {} (errno={}); "
                                "falling back to PRNG", std::strerror(errno), errno);
    }
}

void seed_prng() {
    if (g_prng_seeded.load(std::memory_order_acquire)) return;
    std::lock_guard<std::mutex> lock(g_random_mutex);
    if (g_prng_seeded.load(std::memory_order_acquire)) return;

    ensure_urandom_open();
    uint64_t seed = 0;
    if (g_urandom_fd >= 0) {
        ssize_t n = ::read(g_urandom_fd, &seed, sizeof(seed));
        if (n != static_cast<ssize_t>(sizeof(seed))) {
            seed = static_cast<uint64_t>(
                std::chrono::steady_clock::now().time_since_epoch().count());
            seed ^= std::hash<std::thread::id>{}(std::this_thread::get_id());
            seed ^= reinterpret_cast<uintptr_t>(&seed);
        }
    } else {
        seed = static_cast<uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count());
        seed ^= std::hash<std::thread::id>{}(std::this_thread::get_id());
    }
    g_mt19937.seed(seed);
    g_prng_seeded.store(true, std::memory_order_release);
    get_uuid_logger()->debug("PRNG seeded from /dev/urandom");
}

/**
 * Fill `buf` with `count` cryptographically-secure random bytes.
 * Uses /dev/urandom when available; falls back to mt19937_64 PRNG.
 */
void random_bytes(void* buf, size_t count) {
    seed_prng();
    std::lock_guard<std::mutex> lock(g_random_mutex);

    if (g_urandom_fd >= 0) {
        auto* ptr = static_cast<uint8_t*>(buf);
        size_t remaining = count;
        while (remaining > 0) {
            ssize_t n = ::read(g_urandom_fd, ptr, remaining);
            if (n < 0) {
                if (errno == EINTR) continue;
                get_uuid_logger()->error("read(/dev/urandom) failed; falling back to PRNG");
                goto fallback_prng;
            }
            ptr += static_cast<size_t>(n);
            remaining -= static_cast<size_t>(n);
        }
        return;
    }

fallback_prng:
    {
        auto* words = reinterpret_cast<uint64_t*>(buf);
        size_t n_words = count / 8;
        size_t rem = count % 8;
        for (size_t i = 0; i < n_words; ++i) words[i] = g_mt19937();
        if (rem > 0) {
            uint64_t last = g_mt19937();
            std::memcpy(reinterpret_cast<uint8_t*>(buf) + n_words * 8, &last, rem);
        }
    }
}

// --- Hex helpers for parsing ---
[[nodiscard]] int hex_to_nibble(char c) noexcept {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

[[nodiscard]] int hex_to_byte(const char* hex) noexcept {
    int hi = hex_to_nibble(hex[0]), lo = hex_to_nibble(hex[1]);
    return (hi < 0 || lo < 0) ? -1 : ((hi << 4) | lo);
}

// --- Formatting ---
std::string format_dashed(const std::array<uint8_t, 16>& b) {
    char buf[37];
    std::snprintf(buf, sizeof(buf),
        "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        b[0],b[1],b[2],b[3],b[4],b[5],b[6],b[7],
        b[8],b[9],b[10],b[11],b[12],b[13],b[14],b[15]);
    return {buf, 36};
}

std::string format_plain(const std::array<uint8_t, 16>& b) {
    char buf[33];
    std::snprintf(buf, sizeof(buf),
        "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
        b[0],b[1],b[2],b[3],b[4],b[5],b[6],b[7],
        b[8],b[9],b[10],b[11],b[12],b[13],b[14],b[15]);
    return {buf, 32};
}

} // anonymous namespace

// ============================================================================
// UUID — Construction
// ============================================================================

UUID::UUID() { std::memset(&bytes_, 0, sizeof(bytes_)); }

UUID::UUID(std::array<uint8_t, 16> raw) { bytes_.data = raw; }

UUID::UUID(const UUID& other) { bytes_.data = other.bytes_.data; }

UUID& UUID::operator=(const UUID& other) {
    if (this != &other) bytes_.data = other.bytes_.data;
    return *this;
}

bool UUID::operator==(const UUID& other) const noexcept { return bytes_ == other.bytes_; }
bool UUID::operator!=(const UUID& other) const noexcept { return bytes_ != other.bytes_; }

bool UUID::operator<(const UUID& other) const noexcept {
    return std::lexicographical_compare(
        bytes_.data.begin(), bytes_.data.end(),
        other.bytes_.data.begin(), other.bytes_.data.end());
}

// ============================================================================
// UUID v4 — Random (RFC 9562 §5.4)
// ============================================================================

UUID UUID::v4() {
    uuid_bytes bytes;
    random_bytes(bytes.data.data(), 16);

    // Version: 4 MSB of byte 6 → 0b0100
    bytes.data[6] = static_cast<uint8_t>((bytes.data[6] & 0x0F) | 0x40);
    // Variant: 2 MSB of byte 8 → 0b10
    bytes.data[8] = static_cast<uint8_t>((bytes.data[8] & 0x3F) | 0x80);

    get_uuid_logger()->trace("UUID::v4() generated");
    return UUID(bytes.data);
}

// ============================================================================
// UUID v7 — Time-ordered with millisecond precision (RFC 9562 §5.7)
// ============================================================================

UUID UUID::v7() {
    uuid_bytes bytes;

    // 48-bit Unix timestamp (ms), big-endian in bytes [0..5]
    using namespace std::chrono;
    int64_t ms = duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
    if (ms < 0) ms = 0;

    uint64_t ts = static_cast<uint64_t>(ms);
    bytes.data[0] = static_cast<uint8_t>((ts >> 40) & 0xFF);
    bytes.data[1] = static_cast<uint8_t>((ts >> 32) & 0xFF);
    bytes.data[2] = static_cast<uint8_t>((ts >> 24) & 0xFF);
    bytes.data[3] = static_cast<uint8_t>((ts >> 16) & 0xFF);
    bytes.data[4] = static_cast<uint8_t>((ts >>  8) & 0xFF);
    bytes.data[5] = static_cast<uint8_t>( ts        & 0xFF);

    // 74 random bits for bytes [6..15]
    uint8_t rand[10];
    random_bytes(rand, sizeof(rand));

    bytes.data[6]  = static_cast<uint8_t>((rand[0] & 0x0F) | 0x70);  // version 7
    bytes.data[7]  = rand[1];
    bytes.data[8]  = static_cast<uint8_t>((rand[2] & 0x3F) | 0x80);  // variant
    bytes.data[9]  = rand[3];
    bytes.data[10] = rand[4];
    bytes.data[11] = rand[5];
    bytes.data[12] = rand[6];
    bytes.data[13] = rand[7];
    bytes.data[14] = rand[8];
    bytes.data[15] = rand[9];

    get_uuid_logger()->trace("UUID::v7() generated (ts={})", ms);
    return UUID(bytes.data);
}

// ============================================================================
// UUID — Formatting
// ============================================================================

std::string UUID::to_string() const                 { return format_dashed(bytes_.data); }
std::string UUID::to_string_no_dashes() const       { return format_plain(bytes_.data); }
std::string UUID::format(const UUID& uuid)          { return format_dashed(uuid.bytes_.data); }
std::string UUID::format_no_dashes(const UUID& uuid){ return format_plain(uuid.bytes_.data); }

// ============================================================================
// UUID — Parsing
// ============================================================================

std::optional<UUID> UUID::from_string(const std::string& str) {
    // Strip dashes to get pure hex.
    std::string cleaned;
    cleaned.reserve(32);
    for (char c : str) if (c != '-') cleaned.push_back(c);

    if (cleaned.size() != 32) {
        get_uuid_logger()->warn("UUID::from_string: invalid length {} (need 32 hex)",
                                cleaned.size());
        return std::nullopt;
    }

    uuid_bytes bytes;
    for (size_t i = 0; i < 16; ++i) {
        int v = hex_to_byte(&cleaned[i * 2]);
        if (v < 0) {
            get_uuid_logger()->warn("UUID::from_string: invalid hex at pos {}",
                                    i * 2);
            return std::nullopt;
        }
        bytes.data[i] = static_cast<uint8_t>(v);
    }
    return UUID(bytes.data);
}

std::optional<UUID> UUID::from_string_dashed(const std::string& str) {
    if (str.size() != 36) {
        get_uuid_logger()->warn("UUID::from_string_dashed: invalid length {} (need 36)",
                                str.size());
        return std::nullopt;
    }
    // Validate dash positions: 8, 13, 18, 23
    if (str[8]!='-' || str[13]!='-' || str[18]!='-' || str[23]!='-') {
        get_uuid_logger()->warn("UUID::from_string_dashed: dashes at wrong positions");
        return std::nullopt;
    }
    return from_string(str);
}

// ============================================================================
// UUID — Accessors
// ============================================================================

std::array<uint8_t, 16> UUID::bytes() const noexcept { return bytes_.data; }

uint8_t UUID::version() const noexcept {
    return (bytes_.data[6] >> 4) & 0x0F;
}

bool UUID::is_v4()   const noexcept { return version() == 4; }
bool UUID::is_v7()   const noexcept { return version() == 7; }

bool UUID::is_null() const noexcept {
    for (auto b : bytes_.data) if (b != 0) return false;
    return true;
}

// ============================================================================
// Free-standing convenience functions
// ============================================================================

UUID generate_uuid_v4() {
    return UUID::v4();
}

UUID generate_uuid_v7() {
    return UUID::v7();
}

std::string uuid_to_string(const UUID& id) {
    return id.to_string();
}

std::string uuid_to_string_no_dash(const UUID& id) {
    return id.to_string_no_dashes();
}

std::optional<UUID> parse_uuid(const std::string& str) {
    return UUID::from_string(str);
}

} // namespace torrent
