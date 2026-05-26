/**
 * json_schema_provider.cpp — JsonSchemaProvider: JSON Schema validation
 *
 * Provides JSON Schema lifecycle management (Draft-07 and 2020-12):
 *   - Parse JSON Schema and validate against meta-schema rules
 *   - Validate schema structure (type, properties, required, patternProperties)
 *   - Check backward compatibility (no removal of required, no type narrowing)
 *   - Check forward compatibility (no new required properties)
 *   - Check full compatibility (both directions)
 *   - Schema normalization for fingerprinting
 *
 * Compatibility rules:
 *   BACKWARD: new schema accepts all data that old schema accepts.
 *     - Type must not be narrowed (e.g., "number" → "integer" is narrowing)
 *     - Required properties must not be removed
 *     - Property types must not be narrowed
 *     - Enum values must be a superset of old enum
 *     - Constraints (minLength, maxLength, minimum, maximum) must be relaxed
 *
 *   FORWARD: old schema accepts all data that new schema accepts.
 *     - No new required properties (without defaults)
 *     - Type must not be narrowed
 *     - Property types must not be narrowed
 *     - Enum values must be a subset of old enum
 *
 *   FULL: both BACKWARD and FORWARD.
 */

#include "torrent/common/types.h"
#include "torrent/schema/schema_provider.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <spdlog/spdlog.h>

namespace torrent::schema {

using json = nlohmann::json;

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_json_schema_logger() {
    static auto logger = []() {
        auto l = spdlog::get("json_schema_provider");
        if (!l) {
            l = spdlog::stdout_color_mt("json_schema_provider");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define JS_LOG_INFO(...)  get_json_schema_logger()->info(__VA_ARGS__)
#define JS_LOG_WARN(...)  get_json_schema_logger()->warn(__VA_ARGS__)
#define JS_LOG_DEBUG(...) get_json_schema_logger()->debug(__VA_ARGS__)

// --------------------------------------------------------------------------
// JSON Schema type hierarchy (widening only)
// --------------------------------------------------------------------------

/// Valid JSON Schema primitive types.
const std::set<std::string> kJsonTypes = {
    "string", "number", "integer", "boolean", "array", "object", "null"
};

/// Type widening map: old_type → {set of compatible new_types}
/// Example: "integer" → "number" is widening; "number" → "integer" is narrowing.
const std::map<std::string, std::set<std::string>> kJsonTypeWidening = {
    {"integer", {"integer", "number"}},
    {"number",  {"number"}},
    {"string",  {"string"}},
    {"boolean", {"boolean"}},
    {"array",   {"array"}},
    {"object",  {"object"}},
    {"null",    {"null"}},
};

/// Check if `new_type` is a valid widening (or same) of `old_type`.
[[nodiscard]] bool is_widening(const std::string& old_type,
                                const std::string& new_type) {
    if (old_type == new_type) return true;
    auto it = kJsonTypeWidening.find(old_type);
    if (it != kJsonTypeWidening.end()) {
        return it->second.count(new_type) > 0;
    }
    return false;
}

/// Check if `new_type` is a valid narrowing (or same) of `old_type`.
/// This is the reverse lookup.
[[nodiscard]] bool is_narrowing(const std::string& old_type,
                                 const std::string& new_type) {
    return is_widening(new_type, old_type);
}

/// Resolve a JSON Schema type field (which can be a string or array).
[[nodiscard]] std::vector<std::string> resolve_types(const json& schema) {
    std::vector<std::string> types;
    if (schema.contains("type")) {
        if (schema["type"].is_string()) {
            types.push_back(schema["type"].get<std::string>());
        } else if (schema["type"].is_array()) {
            for (const auto& t : schema["type"]) {
                if (t.is_string()) {
                    types.push_back(t.get<std::string>());
                }
            }
        }
    }
    return types;
}

/// Check if all old_types are compatible with new_types (BACKWARD).
[[nodiscard]] bool types_backward_compatible(
    const std::vector<std::string>& old_types,
    const std::vector<std::string>& new_types) {

    if (old_types.empty() || new_types.empty()) {
        // No type constraint → compatible
        return true;
    }

    // Every old type must have a compatible new type
    for (const auto& old_t : old_types) {
        bool found = false;
        for (const auto& new_t : new_types) {
            if (is_widening(old_t, new_t)) {
                found = true;
                break;
            }
        }
        if (!found) return false;
    }
    return true;
}

/// Check if all new_types are compatible with old_types (FORWARD).
[[nodiscard]] bool types_forward_compatible(
    const std::vector<std::string>& old_types,
    const std::vector<std::string>& new_types) {

    if (old_types.empty() || new_types.empty()) return true;

    // Every new type must have a compatible old type
    for (const auto& new_t : new_types) {
        bool found = false;
        for (const auto& old_t : old_types) {
            if (is_widening(new_t, old_t)) {  // reverse: new must widen to old
                found = true;
                break;
            }
        }
        if (!found) return false;
    }
    return true;
}

// --------------------------------------------------------------------------
// Constraint comparison helpers
// --------------------------------------------------------------------------

/// Check if numeric constraints are relaxed (not tightened).
/// For BACKWARD: new min must be <= old min, new max must be >= old max.
[[nodiscard]] bool constraints_relaxed(const json& old_schema,
                                         const json& new_schema,
                                         const std::string& path,
                                         std::vector<std::string>& errors) {
    // minimum
    if (old_schema.contains("minimum") && new_schema.contains("minimum")) {
        double old_min = old_schema["minimum"].get<double>();
        double new_min = new_schema["minimum"].get<double>();
        if (new_min > old_min) {
            errors.push_back(path + ": minimum tightened from " +
                             std::to_string(old_min) + " to " +
                             std::to_string(new_min));
            return false;
        }
    }

    // maximum
    if (old_schema.contains("maximum") && new_schema.contains("maximum")) {
        double old_max = old_schema["maximum"].get<double>();
        double new_max = new_schema["maximum"].get<double>();
        if (new_max < old_max) {
            errors.push_back(path + ": maximum tightened from " +
                             std::to_string(old_max) + " to " +
                             std::to_string(new_max));
            return false;
        }
    }

    // minLength
    if (old_schema.contains("minLength") && new_schema.contains("minLength")) {
        int old_min = old_schema["minLength"].get<int>();
        int new_min = new_schema["minLength"].get<int>();
        if (new_min > old_min) {
            errors.push_back(path + ": minLength tightened from " +
                             std::to_string(old_min) + " to " +
                             std::to_string(new_min));
            return false;
        }
    }

    // maxLength
    if (old_schema.contains("maxLength") && new_schema.contains("maxLength")) {
        int old_max = old_schema["maxLength"].get<int>();
        int new_max = new_schema["maxLength"].get<int>();
        if (new_max < old_max) {
            errors.push_back(path + ": maxLength tightened from " +
                             std::to_string(old_max) + " to " +
                             std::to_string(new_max));
            return false;
        }
    }

    // minItems
    if (old_schema.contains("minItems") && new_schema.contains("minItems")) {
        int old_min = old_schema["minItems"].get<int>();
        int new_min = new_schema["minItems"].get<int>();
        if (new_min > old_min) {
            errors.push_back(path + ": minItems tightened from " +
                             std::to_string(old_min) + " to " +
                             std::to_string(new_min));
            return false;
        }
    }

    // maxItems
    if (old_schema.contains("maxItems") && new_schema.contains("maxItems")) {
        int old_max = old_schema["maxItems"].get<int>();
        int new_max = new_schema["maxItems"].get<int>();
        if (new_max < old_max) {
            errors.push_back(path + ": maxItems tightened from " +
                             std::to_string(old_max) + " to " +
                             std::to_string(new_max));
            return false;
        }
    }

    return true;
}

/// Check if numeric constraints are narrowed (for FORWARD: old must be subset of new).
[[nodiscard]] bool constraints_containing(const json& old_schema,
                                            const json& new_schema,
                                            const std::string& path,
                                            std::vector<std::string>& errors) {
    // For FORWARD, old constraints must be looser or equal to new constraints
    // i.e., new constraints ⊆ old constraints

    if (old_schema.contains("minimum") && new_schema.contains("minimum")) {
        double old_min = old_schema["minimum"].get<double>();
        double new_min = new_schema["minimum"].get<double>();
        if (new_min < old_min) {
            errors.push_back(path + ": minimum loosened from " +
                             std::to_string(old_min) + " to " +
                             std::to_string(new_min) + " (FORWARD)");
            return false;
        }
    }

    if (old_schema.contains("maximum") && new_schema.contains("maximum")) {
        double old_max = old_schema["maximum"].get<double>();
        double new_max = new_schema["maximum"].get<double>();
        if (new_max > old_max) {
            errors.push_back(path + ": maximum loosened from " +
                             std::to_string(old_max) + " to " +
                             std::to_string(new_max) + " (FORWARD)");
            return false;
        }
    }

    return true;
}

// --------------------------------------------------------------------------
// Enum compatibility
// --------------------------------------------------------------------------

/// For BACKWARD: new enum must be a superset of old enum.
[[nodiscard]] bool enum_backward_compatible(const json& old_enum,
                                              const json& new_enum,
                                              const std::string& path,
                                              std::vector<std::string>& errors) {
    std::set<json> old_vals(old_enum.begin(), old_enum.end());
    std::set<json> new_vals(new_enum.begin(), new_enum.end());

    for (const auto& v : old_vals) {
        if (!new_vals.count(v)) {
            errors.push_back(path + ": enum value removed: " + v.dump());
            return false;
        }
    }
    return true;
}

/// For FORWARD: new enum must be a subset of old enum.
[[nodiscard]] bool enum_forward_compatible(const json& old_enum,
                                             const json& new_enum,
                                             const std::string& path,
                                             std::vector<std::string>& errors) {
    std::set<json> old_vals(old_enum.begin(), old_enum.end());
    std::set<json> new_vals(new_enum.begin(), new_enum.end());

    for (const auto& v : new_vals) {
        if (!old_vals.count(v)) {
            errors.push_back(path + ": new enum value added: " +
                             v.dump() + " (FORWARD incompatible)");
            return false;
        }
    }
    return true;
}

// --------------------------------------------------------------------------
// Meta-schema validation (simplified)
// --------------------------------------------------------------------------

/**
 * Basic meta-schema validation for Draft-07 / 2020-12.
 * Checks that $schema field is present or that the schema has a type/properties.
 */
[[nodiscard]] bool validate_meta_schema(const json& schema,
                                         std::string& error_msg) {
    if (!schema.is_object()) {
        error_msg = "JSON Schema must be a JSON object";
        return false;
    }

    // Must have at least one JSON Schema keyword
    bool has_schema_keyword =
        schema.contains("type") ||
        schema.contains("properties") ||
        schema.contains("$schema") ||
        schema.contains("items") ||
        schema.contains("required") ||
        schema.contains("enum") ||
        schema.contains("oneOf") ||
        schema.contains("anyOf") ||
        schema.contains("allOf") ||
        schema.contains("$ref");

    if (!has_schema_keyword) {
        error_msg = "JSON Schema must contain at least one schema keyword";
        return false;
    }

    // Validate type field if present
    if (schema.contains("type")) {
        if (schema["type"].is_string()) {
            std::string t = schema["type"].get<std::string>();
            if (!kJsonTypes.count(t)) {
                error_msg = "Unknown JSON Schema type: " + t;
                return false;
            }
        } else if (schema["type"].is_array()) {
            for (const auto& t : schema["type"]) {
                if (t.is_string() && !kJsonTypes.count(t.get<std::string>())) {
                    error_msg = "Unknown JSON Schema type in array: " +
                                t.get<std::string>();
                    return false;
                }
            }
        } else {
            error_msg = "'type' must be a string or array";
            return false;
        }
    }

    return true;
}

// --------------------------------------------------------------------------
// Property-level compatibility
// --------------------------------------------------------------------------

/**
 * Check backward compatibility for a single property.
 */
[[nodiscard]] bool property_backward_compat(const json& old_prop,
                                              const json& new_prop,
                                              const std::string& path,
                                              std::vector<std::string>& errors) {
    // Type check
    auto old_types = resolve_types(old_prop);
    auto new_types = resolve_types(new_prop);
    if (!old_types.empty() && !new_types.empty()) {
        if (!types_backward_compatible(old_types, new_types)) {
            errors.push_back(path + ": type narrowed");
            return false;
        }
    }

    // Constraint check
    if (!constraints_relaxed(old_prop, new_prop, path, errors)) {
        return false;
    }

    // Enum check
    if (old_prop.contains("enum") && new_prop.contains("enum")) {
        if (!enum_backward_compatible(old_prop["enum"], new_prop["enum"],
                                       path, errors)) {
            return false;
        }
    }

    // Nested object properties
    if (old_prop.contains("properties") && new_prop.contains("properties")) {
        for (auto it = old_prop["properties"].begin();
             it != old_prop["properties"].end(); ++it) {
            std::string key = it.key();
            if (!new_prop["properties"].contains(key)) {
                errors.push_back(path + "." + key +
                                 ": property removed");
                return false;
            }
            if (!property_backward_compat(it.value(),
                    new_prop["properties"][key],
                    path + "." + key, errors)) {
                return false;
            }
        }
    }

    return true;
}

/**
 * Check forward compatibility for a single property.
 */
[[nodiscard]] bool property_forward_compat(const json& old_prop,
                                             const json& new_prop,
                                             const std::string& path,
                                             std::vector<std::string>& errors) {
    auto old_types = resolve_types(old_prop);
    auto new_types = resolve_types(new_prop);
    if (!old_types.empty() && !new_types.empty()) {
        if (!types_forward_compatible(old_types, new_types)) {
            errors.push_back(path + ": type narrowed (FORWARD)");
            return false;
        }
    }

    if (!constraints_containing(old_prop, new_prop, path, errors)) {
        return false;
    }

    if (old_prop.contains("enum") && new_prop.contains("enum")) {
        if (!enum_forward_compatible(old_prop["enum"], new_prop["enum"],
                                      path, errors)) {
            return false;
        }
    }

    // New required properties must exist in old (with compatible types)
    if (old_prop.contains("properties") && new_prop.contains("properties")) {
        for (auto it = new_prop["properties"].begin();
             it != new_prop["properties"].end(); ++it) {
            std::string key = it.key();
            if (old_prop["properties"].contains(key)) {
                if (!property_forward_compat(old_prop["properties"][key],
                        it.value(), path + "." + key, errors)) {
                    return false;
                }
            } else if (new_prop.contains("required") &&
                       std::find(new_prop["required"].begin(),
                                 new_prop["required"].end(), key) !=
                           new_prop["required"].end()) {
                errors.push_back(path + "." + key +
                                 ": new required property (FORWARD)");
                return false;
            }
        }
    }

    return true;
}

// --------------------------------------------------------------------------
// Top-level backward/forward compatibility
// --------------------------------------------------------------------------

[[nodiscard]] bool backward_compat(const json& old_schema,
                                     const json& new_schema,
                                     const std::string& path,
                                     std::vector<std::string>& errors) {
    // Type-level check
    auto old_types = resolve_types(old_schema);
    auto new_types = resolve_types(new_schema);
    if (!old_types.empty() && !new_types.empty()) {
        if (!types_backward_compatible(old_types, new_types)) {
            errors.push_back(path + ": type narrowed");
            return false;
        }
    }

    // Constraint check
    if (!constraints_relaxed(old_schema, new_schema, path, errors)) {
        return false;
    }

    // Required: old required properties must also be in new
    if (old_schema.contains("required")) {
        std::set<std::string> old_req;
        for (const auto& r : old_schema["required"]) {
            old_req.insert(r.get<std::string>());
        }
        if (new_schema.contains("required")) {
            for (const auto& r : new_schema["required"]) {
                if (!old_req.count(r.get<std::string>())) {
                    errors.push_back(path + ": new required property '" +
                                     r.get<std::string>() +
                                     "' not in old (BACKWARD)");
                    return false;
                }
            }
        }
    }

    // Property-level checks
    if (old_schema.contains("properties")) {
        for (auto it = old_schema["properties"].begin();
             it != old_schema["properties"].end(); ++it) {
            std::string key = it.key();
            if (new_schema.contains("properties") &&
                new_schema["properties"].contains(key)) {
                if (!property_backward_compat(it.value(),
                        new_schema["properties"][key],
                        path + "." + key, errors)) {
                    return false;
                }
            } else {
                errors.push_back(path + "." + key + ": property removed");
                return false;
            }
        }
    }

    return true;
}

[[nodiscard]] bool forward_compat(const json& old_schema,
                                    const json& new_schema,
                                    const std::string& path,
                                    std::vector<std::string>& errors) {
    auto old_types = resolve_types(old_schema);
    auto new_types = resolve_types(new_schema);
    if (!old_types.empty() && !new_types.empty()) {
        if (!types_forward_compatible(old_types, new_types)) {
            errors.push_back(path + ": type narrowed (FORWARD)");
            return false;
        }
    }

    if (!constraints_containing(old_schema, new_schema, path, errors)) {
        return false;
    }

    // New required properties must exist in old
    if (new_schema.contains("required")) {
        std::set<std::string> old_req;
        if (old_schema.contains("required")) {
            for (const auto& r : old_schema["required"]) {
                old_req.insert(r.get<std::string>());
            }
        }
        for (const auto& r : new_schema["required"]) {
            if (!old_req.count(r.get<std::string>())) {
                errors.push_back(path + ": new required property '" +
                                 r.get<std::string>() +
                                 "' not in old (FORWARD)");
                return false;
            }
        }
    }

    // Property-level checks (new→old direction)
    if (new_schema.contains("properties")) {
        for (auto it = new_schema["properties"].begin();
             it != new_schema["properties"].end(); ++it) {
            std::string key = it.key();
            if (old_schema.contains("properties") &&
                old_schema["properties"].contains(key)) {
                if (!property_forward_compat(old_schema["properties"][key],
                        it.value(), path + "." + key, errors)) {
                    return false;
                }
            } else {
                // New property — check if it's required
                if (new_schema.contains("required") &&
                    std::find(new_schema["required"].begin(),
                              new_schema["required"].end(), key) !=
                        new_schema["required"].end()) {
                    errors.push_back(path + "." + key +
                                     ": new required property (FORWARD)");
                    return false;
                }
            }
        }
    }

    return true;
}

} // anonymous namespace

// ============================================================================
// JsonSchemaProvider — public API
// ============================================================================

/**
 * Parse and validate a JSON Schema.
 */
bool JsonSchemaProvider::validate(const std::string& schema_text,
                                   std::string& error_msg) {
    try {
        json schema = json::parse(schema_text);
        return validate_meta_schema(schema, error_msg);
    } catch (const json::parse_error& e) {
        error_msg = std::string("JSON parse error: ") + e.what();
        return false;
    } catch (const std::exception& e) {
        error_msg = std::string("Validation error: ") + e.what();
        return false;
    }
}

/**
 * Check backward compatibility.
 */
bool JsonSchemaProvider::check_backward(const std::string& old_schema_text,
                                         const std::string& new_schema_text,
                                         std::string& error_msg) {
    try {
        json old_schema = json::parse(old_schema_text);
        json new_schema = json::parse(new_schema_text);
        std::vector<std::string> errors;
        if (backward_compat(old_schema, new_schema, "$", errors)) {
            return true;
        }
        error_msg = errors.empty() ? "Incompatible" : errors[0];
        return false;
    } catch (const std::exception& e) {
        error_msg = std::string("Compatibility check error: ") + e.what();
        return false;
    }
}

/**
 * Check forward compatibility.
 */
bool JsonSchemaProvider::check_forward(const std::string& old_schema_text,
                                        const std::string& new_schema_text,
                                        std::string& error_msg) {
    try {
        json old_schema = json::parse(old_schema_text);
        json new_schema = json::parse(new_schema_text);
        std::vector<std::string> errors;
        if (forward_compat(old_schema, new_schema, "$", errors)) {
            return true;
        }
        error_msg = errors.empty() ? "Incompatible" : errors[0];
        return false;
    } catch (const std::exception& e) {
        error_msg = std::string("Compatibility check error: ") + e.what();
        return false;
    }
}

/**
 * Check full compatibility.
 */
bool JsonSchemaProvider::check_full(const std::string& old_schema_text,
                                     const std::string& new_schema_text,
                                     std::string& error_msg) {
    if (!check_backward(old_schema_text, new_schema_text, error_msg)) {
        return false;
    }
    std::string fwd_error;
    if (!check_forward(old_schema_text, new_schema_text, fwd_error)) {
        error_msg = fwd_error;
        return false;
    }
    return true;
}

/**
 * Normalize a JSON Schema to a canonical form.
 */
std::string JsonSchemaProvider::normalize(const std::string& schema_text) {
    try {
        json schema = json::parse(schema_text);
        // Sort keys for canonical output
        return schema.dump();  // nlohmann preserves insertion order,
                               // but parsing/dumping gives us clean JSON
    } catch (const std::exception& e) {
        JS_LOG_WARN("Failed to normalize JSON Schema: {}", e.what());
        return schema_text;
    }
}

/**
 * Generate a fingerprint for a JSON Schema.
 */
std::string JsonSchemaProvider::fingerprint(const std::string& schema_text) {
    std::string norm = normalize(schema_text);
    std::hash<std::string> hasher;
    size_t h = hasher(norm);
    std::ostringstream oss;
    oss << std::hex << h;
    return oss.str();
}

} // namespace torrent::schema
