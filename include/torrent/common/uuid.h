#pragma once

/**
 * torrent-mq — UUID
 *
 * Implements UUID v4 (random) and v7 (time-ordered) generation
 * conforming to RFC 9562.  UUIDs are 128-bit (16-byte) values.
 *
 *   - v4: 122 random bits + 6 fixed version/variant bits.
 *   - v7: 48-bit Unix ms timestamp + 74 random bits.
 *
 * Formatting supports dashed (36-char) and plain (32-char) strings.
 * Parsing accepts both formats.
 *
 * Thread safety: all static factory methods are thread-safe.
 */

#include <array>
#include <cstdint>
#include <optional>
#include <string>

namespace torrent {

class UUID {
public:
    // -- Construction -----------------------------------------------------

    /// Construct a null UUID (all zeros).
    UUID();

    /// Construct from raw 16-byte array.
    explicit UUID(std::array<uint8_t, 16> raw);

    UUID(const UUID& other);
    UUID& operator=(const UUID& other);
    UUID(UUID&&) noexcept = default;
    UUID& operator=(UUID&&) noexcept = default;

    // -- Comparison -------------------------------------------------------

    [[nodiscard]] bool operator==(const UUID& other) const noexcept;
    [[nodiscard]] bool operator!=(const UUID& other) const noexcept;

    /// Lexicographic byte comparison. v7 UUIDs sort chronologically.
    [[nodiscard]] bool operator<(const UUID& other) const noexcept;

    // -- Generation (static factories) ------------------------------------

    /// Generate a random UUID v4 (RFC 9562 §5.4).
    [[nodiscard]] static UUID v4();

    /// Generate a time-ordered UUID v7 (RFC 9562 §5.7).
    [[nodiscard]] static UUID v7();

    // -- Formatting -------------------------------------------------------

    /// "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" (36 chars, dashed).
    [[nodiscard]] std::string to_string() const;

    /// "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" (32 chars, no dashes).
    [[nodiscard]] std::string to_string_no_dashes() const;

    /// Static convenience: format a UUID to dashed string.
    [[nodiscard]] static std::string format(const UUID& uuid);

    /// Static convenience: format a UUID to plain hex string.
    [[nodiscard]] static std::string format_no_dashes(const UUID& uuid);

    // -- Parsing ----------------------------------------------------------

    /// Parse from a string (accepts dashed and plain formats).
    [[nodiscard]] static std::optional<UUID> from_string(const std::string& str);

    /// Parse from a strictly dashed format: 36 chars, dashes at 8/13/18/23.
    [[nodiscard]] static std::optional<UUID> from_string_dashed(const std::string& str);

    // -- Accessors --------------------------------------------------------

    /// Raw 16-byte representation.
    [[nodiscard]] std::array<uint8_t, 16> bytes() const noexcept;

    /// UUID version (4 or 7 for generated UUIDs, or the parsed value).
    [[nodiscard]] uint8_t version() const noexcept;

    [[nodiscard]] bool is_v4()   const noexcept;
    [[nodiscard]] bool is_v7()   const noexcept;

    /// True when all 16 bytes are zero.
    [[nodiscard]] bool is_null() const noexcept;

private:
    // 16-byte storage (big-endian per RFC 9562).
    struct uuid_bytes {
        std::array<uint8_t, 16> data;
        bool operator==(const uuid_bytes& o) const noexcept { return data == o.data; }
        bool operator!=(const uuid_bytes& o) const noexcept { return data != o.data; }
    };

    uuid_bytes bytes_;
};

} // namespace torrent
