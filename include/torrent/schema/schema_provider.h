#pragma once

/**
 * torrent-mq — Schema Provider Declarations
 *
 * Each provider is a struct with static methods for:
 *   - validate()  — parse and validate schema syntax
 *   - check_backward() / check_forward() / check_full() — compatibility
 *   - normalize() — canonical form
 *   - fingerprint() — identity hash
 *
 * Implementations live in:
 *   avro_provider.cpp, protobuf_provider.cpp, json_schema_provider.cpp
 */

#include <string>

namespace torrent::schema {

// ============================================================================
// AvroSchemaProvider
// ============================================================================

struct AvroSchemaProvider {
    static bool validate(const std::string& schema_text, std::string& error_msg);
    static bool check_backward(const std::string& old_schema,
                               const std::string& new_schema,
                               std::string& error_msg);
    static bool check_forward(const std::string& old_schema,
                              const std::string& new_schema,
                              std::string& error_msg);
    static bool check_full(const std::string& old_schema,
                           const std::string& new_schema,
                           std::string& error_msg);
    static std::string normalize(const std::string& schema_text);
    static std::string fingerprint(const std::string& schema_text);
};

// ============================================================================
// ProtobufSchemaProvider
// ============================================================================

struct ProtobufSchemaProvider {
    static bool validate(const std::string& schema_text, std::string& error_msg);
    static bool check_backward(const std::string& old_schema,
                               const std::string& new_schema,
                               std::string& error_msg);
    static bool check_forward(const std::string& old_schema,
                              const std::string& new_schema,
                              std::string& error_msg);
    static bool check_full(const std::string& old_schema,
                           const std::string& new_schema,
                           std::string& error_msg);
    static std::string normalize(const std::string& schema_text);
    static std::string fingerprint(const std::string& schema_text);
};

// ============================================================================
// JsonSchemaProvider
// ============================================================================

struct JsonSchemaProvider {
    static bool validate(const std::string& schema_text, std::string& error_msg);
    static bool check_backward(const std::string& old_schema,
                               const std::string& new_schema,
                               std::string& error_msg);
    static bool check_forward(const std::string& old_schema,
                              const std::string& new_schema,
                              std::string& error_msg);
    static bool check_full(const std::string& old_schema,
                           const std::string& new_schema,
                           std::string& error_msg);
    static std::string normalize(const std::string& schema_text);
    static std::string fingerprint(const std::string& schema_text);
};

// ============================================================================
// CompatibilityChecker — global accessors
// ============================================================================

class CompatibilityChecker;

/// Get the global compatibility checker instance (lazy singleton).
CompatibilityChecker& get_compatibility_checker();

/// Set the global compatibility mode by string name.
/// "NONE", "BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD",
/// "FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE"
void set_compatibility_mode(const std::string& mode_str);

/// Set the global compatibility mode by enum.
enum class CompatibilityMode : uint8_t {
    none                = 0,
    backward            = 1,
    backward_transitive = 2,
    forward             = 3,
    forward_transitive  = 4,
    full                = 5,
    full_transitive     = 6,
};

void set_compatibility_mode(CompatibilityMode mode);
CompatibilityMode get_compatibility_mode();

} // namespace torrent::schema
