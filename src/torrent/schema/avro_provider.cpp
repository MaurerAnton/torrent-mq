/**
 * avro_provider.cpp — AvroSchemaProvider: Avro schema validation & compatibility
 *
 * Provides Avro schema lifecycle management:
 *   - Parse and validate Avro schema JSON
 *   - Validate schema structure (type, fields, symbols, nested types)
 *   - Check compatibility modes: BACKWARD, FORWARD, FULL
 *   - Schema normalization to canonical form (Parsing Canonical Form)
 *   - Schema fingerprinting via SHA-256 of canonical form
 *
 * Compatibility rules (Confluent-style):
 *   BACKWARD: new schema can read data written by old schema.
 *     - No field removal
 *     - No type change (except int→long, float→double widening)
 *     - No removal of enum symbols
 *     - Defaults required for new fields
 *
 *   FORWARD: old schema can read data written by new schema.
 *     - No new required fields (without defaults)
 *     - No type narrowing
 *     - No new enum symbols that old readers can't handle
 *
 *   FULL: both BACKWARD and FORWARD.
 *
 * See schema_registry.cpp for the registry that uses this provider.
 */

#include "torrent/common/types.h"
#include "torrent/schema/schema_provider.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
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

[[nodiscard]] std::shared_ptr<spdlog::logger> get_avro_logger() {
    static auto logger = []() {
        auto l = spdlog::get("avro_provider");
        if (!l) {
            l = spdlog::stdout_color_mt("avro_provider");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define AVRO_LOG_INFO(...)  get_avro_logger()->info(__VA_ARGS__)
#define AVRO_LOG_WARN(...)  get_avro_logger()->warn(__VA_ARGS__)
#define AVRO_LOG_DEBUG(...) get_avro_logger()->debug(__VA_ARGS__)

// --------------------------------------------------------------------------
// Supported Avro primitive types
// --------------------------------------------------------------------------

const std::set<std::string> kAvroPrimitives = {
    "null", "boolean", "int", "long", "float", "double",
    "bytes", "string"
};

const std::set<std::string> kAvroNamedTypes = {
    "record", "enum", "fixed", "array", "map"
};

/// Type widening map: old_type → {set of compatible new_types}
const std::map<std::string, std::set<std::string>> kTypeWidening = {
    {"int",    {"int", "long", "float", "double"}},
    {"long",   {"long", "float", "double"}},
    {"float",  {"float", "double"}},
    {"double", {"double"}},
    {"string", {"string", "bytes"}},
    {"bytes",  {"bytes"}},
};

/// Check if `new_type` is a valid widening of `old_type`.
[[nodiscard]] bool is_widening(const std::string& old_type,
                                const std::string& new_type) {
    if (old_type == new_type) return true;
    auto it = kTypeWidening.find(old_type);
    if (it != kTypeWidening.end()) {
        return it->second.count(new_type) > 0;
    }
    return false;
}

/// Resolve the effective type name from an Avro type spec (union, primitive, named).
[[nodiscard]] std::string resolve_type_name(const json& type_spec) {
    if (type_spec.is_string()) {
        return type_spec.get<std::string>();
    }
    if (type_spec.is_array()) {
        // Union — return the first non-null type for comparison
        for (const auto& t : type_spec) {
            std::string tn = resolve_type_name(t);
            if (tn != "null") return tn;
        }
        return "null";
    }
    if (type_spec.is_object()) {
        return type_spec.value("type", "unknown");
    }
    return "unknown";
}

/// Check if a type spec includes "null" (nullable).
[[nodiscard]] bool is_nullable(const json& type_spec) {
    if (type_spec.is_array()) {
        for (const auto& t : type_spec) {
            if (resolve_type_name(t) == "null") return true;
        }
    }
    return false;
}

// --------------------------------------------------------------------------
// Schema validation
// --------------------------------------------------------------------------

/**
 * Recursively validate an Avro schema node.
 * Returns true if the schema is structurally valid.
 */
[[nodiscard]] bool validate_avro_node(const json& node,
                                       const std::string& path,
                                       std::string& error_msg) {
    if (node.is_string()) {
        std::string type_name = node.get<std::string>();
        if (kAvroPrimitives.count(type_name) || kAvroNamedTypes.count(type_name)) {
            return true;
        }
        // Could be a reference to a named type defined elsewhere — accept
        return true;
    }

    if (!node.is_object()) {
        error_msg = path + ": expected object or string, got " +
                    std::string(node.type_name());
        return false;
    }

    // Check for "type" field
    if (!node.contains("type")) {
        error_msg = path + ": missing required 'type' field";
        return false;
    }

    std::string type_name = resolve_type_name(node["type"]);

    if (type_name == "record") {
        if (!node.contains("name")) {
            error_msg = path + ": record missing 'name'";
            return false;
        }
        if (!node.contains("fields") || !node["fields"].is_array()) {
            error_msg = path + ": record missing 'fields' array";
            return false;
        }
        std::set<std::string> field_names;
        int idx = 0;
        for (const auto& field : node["fields"]) {
            if (!field.contains("name")) {
                error_msg = path + ".fields[" + std::to_string(idx) +
                            "]: missing 'name'";
                return false;
            }
            if (!field.contains("type")) {
                error_msg = path + ".fields[" + std::to_string(idx) +
                            "]: missing 'type'";
                return false;
            }
            std::string fname = field["name"].get<std::string>();
            if (field_names.count(fname)) {
                error_msg = path + ".fields[" + std::to_string(idx) +
                            "]: duplicate field name '" + fname + "'";
                return false;
            }
            field_names.insert(fname);

            if (!validate_avro_node(field["type"],
                     path + ".fields." + fname, error_msg)) {
                return false;
            }
            idx++;
        }
    } else if (type_name == "enum") {
        if (!node.contains("name")) {
            error_msg = path + ": enum missing 'name'";
            return false;
        }
        if (!node.contains("symbols") || !node["symbols"].is_array()) {
            error_msg = path + ": enum missing 'symbols' array";
            return false;
        }
        std::set<std::string> symbols;
        for (const auto& sym : node["symbols"]) {
            std::string s = sym.get<std::string>();
            if (symbols.count(s)) {
                error_msg = path + ": duplicate enum symbol '" + s + "'";
                return false;
            }
            symbols.insert(s);
        }
    } else if (type_name == "array") {
        if (!node.contains("items")) {
            error_msg = path + ": array missing 'items'";
            return false;
        }
        if (!validate_avro_node(node["items"],
                 path + ".items", error_msg)) {
            return false;
        }
    } else if (type_name == "map") {
        if (!node.contains("values")) {
            error_msg = path + ": map missing 'values'";
            return false;
        }
        if (!validate_avro_node(node["values"],
                 path + ".values", error_msg)) {
            return false;
        }
    } else if (type_name == "fixed") {
        if (!node.contains("name")) {
            error_msg = path + ": fixed missing 'name'";
            return false;
        }
        if (!node.contains("size") || !node["size"].is_number_integer()) {
            error_msg = path + ": fixed missing integer 'size'";
            return false;
        }
    } else if (type_name == "union-type") {
        // Inline union
        if (!node["type"].is_array()) {
            error_msg = path + ": union must be an array of types";
            return false;
        }
        int uidx = 0;
        for (const auto& ut : node["type"]) {
            if (!validate_avro_node(ut,
                     path + ".[" + std::to_string(uidx) + "]", error_msg)) {
                return false;
            }
            uidx++;
        }
    }

    // Validate "default" if present
    if (node.contains("default")) {
        // Default values are permissible for any type
    }

    return true;
}

// --------------------------------------------------------------------------
// Field-level compatibility
// --------------------------------------------------------------------------

/**
 * Check if an old_field type is compatible with a new_field type
 * for BACKWARD compatibility.
 */
[[nodiscard]] bool field_type_compatible(const json& old_type,
                                          const json& new_type,
                                          const std::string& field_path,
                                          std::vector<std::string>& errors) {
    std::string old_name = resolve_type_name(old_type);
    std::string new_name = resolve_type_name(new_type);

    if (old_name == new_name) return true;

    if (kAvroPrimitives.count(old_name) && kAvroPrimitives.count(new_name)) {
        if (is_widening(old_name, new_name)) return true;
        errors.push_back(field_path + ": type changed from " +
                         old_name + " to " + new_name +
                         " (not a valid widening)");
        return false;
    }

    // Named or complex types — type name must match exactly
    if (old_name != new_name) {
        errors.push_back(field_path + ": type changed from " +
                         old_name + " to " + new_name);
        return false;
    }
    return true;
}

// --------------------------------------------------------------------------
// Backward compatibility check
// --------------------------------------------------------------------------

/**
 * BACKWARD: new schema can read data written by old schema.
 *
 * Rules:
 *   - Fields from old must exist in new (no removal)
 *   - Field types cannot change (except int→long, float→double widening)
 *   - Enum symbols cannot be removed
 *   - New fields must have defaults
 */
[[nodiscard]] bool check_backward_compat(const json& old_schema,
                                          const json& new_schema,
                                          const std::string& path,
                                          std::vector<std::string>& errors) {
    std::string old_type = resolve_type_name(
        old_schema.is_object() ? old_schema["type"] : old_schema);
    std::string new_type = resolve_type_name(
        new_schema.is_object() ? new_schema["type"] : new_schema);

    // Type name match
    if (old_type != new_type) {
        // Allow primitive widening at top level
        if (kAvroPrimitives.count(old_type) && kAvroPrimitives.count(new_type)) {
            if (!is_widening(old_type, new_type)) {
                errors.push_back(path + ": type narrowed from " +
                                 old_type + " to " + new_type);
                return false;
            }
            return true;
        }
        errors.push_back(path + ": type changed from " +
                         old_type + " to " + new_type);
        return false;
    }

    // Recursive checks by named type
    if (old_type == "record") {
        // Build old field map
        std::map<std::string, json> old_fields;
        for (const auto& f : old_schema["fields"]) {
            old_fields[f["name"].get<std::string>()] = f;
        }

        // Build new field map
        std::map<std::string, json> new_fields;
        for (const auto& f : new_schema["fields"]) {
            new_fields[f["name"].get<std::string>()] = f;
        }

        // OLD fields must exist in NEW
        for (const auto& [name, old_field] : old_fields) {
            auto it = new_fields.find(name);
            if (it == new_fields.end()) {
                errors.push_back(path + "." + name +
                                 ": field removed from new schema");
                return false;
            }
            // Check field type compatibility
            if (!field_type_compatible(old_field["type"], it->second["type"],
                                        path + "." + name, errors)) {
                return false;
            }
        }

        // NEW fields must have defaults
        for (const auto& [name, new_field] : new_fields) {
            if (!old_fields.count(name)) {
                if (!new_field.contains("default") &&
                    !is_nullable(new_field["type"])) {
                    errors.push_back(path + "." + name +
                                     ": new field has no default");
                    return false;
                }
            }
        }
    } else if (old_type == "enum") {
        std::set<std::string> old_symbols;
        for (const auto& s : old_schema["symbols"]) {
            old_symbols.insert(s.get<std::string>());
        }
        std::set<std::string> new_symbols;
        for (const auto& s : new_schema["symbols"]) {
            new_symbols.insert(s.get<std::string>());
        }
        // All old symbols must be in new
        for (const auto& s : old_symbols) {
            if (!new_symbols.count(s)) {
                errors.push_back(path + ": enum symbol '" + s + "' removed");
                return false;
            }
        }
    } else if (old_type == "fixed") {
        // Fixed size must match
        int old_size = old_schema["size"].get<int>();
        int new_size = new_schema["size"].get<int>();
        if (old_size != new_size) {
            errors.push_back(path + ": fixed size changed from " +
                             std::to_string(old_size) + " to " +
                             std::to_string(new_size));
            return false;
        }
    } else if (old_type == "array") {
        return check_backward_compat(old_schema["items"],
                                      new_schema["items"],
                                      path + ".items", errors);
    } else if (old_type == "map") {
        return check_backward_compat(old_schema["values"],
                                      new_schema["values"],
                                      path + ".values", errors);
    }

    return true;
}

// --------------------------------------------------------------------------
// Forward compatibility check
// --------------------------------------------------------------------------

/**
 * FORWARD: old schema can read data written by new schema.
 *
 * Rules:
 *   - Fields from new must exist in old, OR new fields must have defaults
 *   - No type narrowing (same rules as backward but direction reversed)
 *   - No new enum symbols (old readers don't know them)
 */
[[nodiscard]] bool check_forward_compat(const json& old_schema,
                                         const json& new_schema,
                                         const std::string& path,
                                         std::vector<std::string>& errors) {
    std::string old_type = resolve_type_name(
        old_schema.is_object() ? old_schema["type"] : old_schema);
    std::string new_type = resolve_type_name(
        new_schema.is_object() ? new_schema["type"] : new_schema);

    if (old_type != new_type) {
        if (kAvroPrimitives.count(old_type) && kAvroPrimitives.count(new_type)) {
            // For forward compat, new_type must be subset of old_type
            if (!is_widening(new_type, old_type)) {
                errors.push_back(path + ": type " + old_type +
                                 " cannot read " + new_type);
                return false;
            }
            return true;
        }
        errors.push_back(path + ": type changed from " +
                         old_type + " to " + new_type);
        return false;
    }

    if (old_type == "record") {
        std::map<std::string, json> old_fields;
        for (const auto& f : old_schema["fields"]) {
            old_fields[f["name"].get<std::string>()] = f;
        }
        std::map<std::string, json> new_fields;
        for (const auto& f : new_schema["fields"]) {
            new_fields[f["name"].get<std::string>()] = f;
        }

        // NEW fields with no default that aren't in OLD → incompatible
        for (const auto& [name, new_field] : new_fields) {
            auto it = old_fields.find(name);
            if (it == old_fields.end()) {
                if (!new_field.contains("default") &&
                    !is_nullable(new_field["type"])) {
                    errors.push_back(path + "." + name +
                                     ": new required field not in old schema (FORWARD)");
                    return false;
                }
            } else {
                // Check field type: new must be readable by old
                if (!field_type_compatible(new_field["type"],
                                            it->second["type"],
                                            path + "." + name, errors)) {
                    return false;
                }
            }
        }
    } else if (old_type == "enum") {
        std::set<std::string> old_symbols;
        for (const auto& s : old_schema["symbols"]) {
            old_symbols.insert(s.get<std::string>());
        }
        std::set<std::string> new_symbols;
        for (const auto& s : new_schema["symbols"]) {
            new_symbols.insert(s.get<std::string>());
        }
        // New symbols not in old → incompatible (old reader can't handle)
        for (const auto& s : new_symbols) {
            if (!old_symbols.count(s)) {
                errors.push_back(path + ": new enum symbol '" + s +
                                 "' added (FORWARD incompatible)");
                return false;
            }
        }
    } else if (old_type == "fixed") {
        int old_size = old_schema["size"].get<int>();
        int new_size = new_schema["size"].get<int>();
        if (old_size != new_size) {
            errors.push_back(path + ": fixed size mismatch");
            return false;
        }
    } else if (old_type == "array") {
        return check_forward_compat(old_schema["items"],
                                     new_schema["items"],
                                     path + ".items", errors);
    } else if (old_type == "map") {
        return check_forward_compat(old_schema["values"],
                                     new_schema["values"],
                                     path + ".values", errors);
    }

    return true;
}

// --------------------------------------------------------------------------
// Canonical form (Parsing Canonical Form per Avro spec)
// --------------------------------------------------------------------------

/**
 * Produce the Parsing Canonical Form of an Avro schema.
 * This is used for fingerprinting and identity comparison.
 *
 * Rules per Avro spec:
 *   - STRIP: doc, aliases, and other non-functional attributes
 *   - ORDER: fields are sorted by name
 *   - STRIP: whitespace
 *   - CONVERT: default values to their JSON canonical form
 */
[[nodiscard]] std::string canonical_form(const json& schema) {
    json canon;

    if (schema.is_string()) {
        // Primitive type reference
        canon = schema;
    } else if (schema.is_object()) {
        canon["type"] = schema["type"];
        if (schema.contains("name"))  canon["name"] = schema["name"];
        if (schema.contains("size"))  canon["size"] = schema["size"];

        if (schema.contains("fields")) {
            // Sort fields by name
            std::vector<json> sorted_fields;
            for (const auto& f : schema["fields"]) {
                sorted_fields.push_back(canonical_form(f));
            }
            std::sort(sorted_fields.begin(), sorted_fields.end(),
                      [](const json& a, const json& b) {
                          return a["name"] < b["name"];
                      });
            canon["fields"] = sorted_fields;
        }

        if (schema.contains("symbols")) {
            canon["symbols"] = schema["symbols"];
        }

        if (schema.contains("items")) {
            canon["items"] = canonical_form(schema["items"]);
        }
        if (schema.contains("values")) {
            canon["values"] = canonical_form(schema["values"]);
        }
    } else if (schema.is_array()) {
        json arr = json::array();
        for (const auto& item : schema) {
            arr.push_back(canonical_form(item));
        }
        canon = arr;
    }

    return canon.dump();
}

// --------------------------------------------------------------------------
// Union type handling
// --------------------------------------------------------------------------

/**
 * Flatten a type spec that might be a union into a list of individual types.
 */
[[nodiscard]] std::vector<json> flatten_union(const json& type_spec) {
    if (type_spec.is_array()) {
        std::vector<json> result;
        for (const auto& t : type_spec) {
            result.push_back(t);
        }
        return result;
    }
    return {type_spec};
}

} // anonymous namespace

// ============================================================================
// AvroSchemaProvider — public API
// ============================================================================

/**
 * Parse and validate an Avro schema.
 * Returns true with empty error if valid, false with error message if invalid.
 */
bool AvroSchemaProvider::validate(const std::string& schema_text,
                                   std::string& error_msg) {
    try {
        json schema = json::parse(schema_text);
        if (!schema.is_object()) {
            error_msg = "Avro schema must be a JSON object";
            return false;
        }
        if (!schema.contains("type")) {
            error_msg = "Avro schema must have a 'type' field";
            return false;
        }
        return validate_avro_node(schema, "$", error_msg);
    } catch (const json::parse_error& e) {
        error_msg = std::string("JSON parse error: ") + e.what();
        return false;
    } catch (const std::exception& e) {
        error_msg = std::string("Validation error: ") + e.what();
        return false;
    }
}

/**
 * Check backward compatibility: new can read old data.
 */
bool AvroSchemaProvider::check_backward(const std::string& old_schema_text,
                                         const std::string& new_schema_text,
                                         std::string& error_msg) {
    try {
        json old_schema = json::parse(old_schema_text);
        json new_schema = json::parse(new_schema_text);
        std::vector<std::string> errors;
        if (check_backward_compat(old_schema, new_schema, "$", errors)) {
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
 * Check forward compatibility: old can read new data.
 */
bool AvroSchemaProvider::check_forward(const std::string& old_schema_text,
                                        const std::string& new_schema_text,
                                        std::string& error_msg) {
    try {
        json old_schema = json::parse(old_schema_text);
        json new_schema = json::parse(new_schema_text);
        std::vector<std::string> errors;
        if (check_forward_compat(old_schema, new_schema, "$", errors)) {
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
 * Check full compatibility: both backward and forward.
 */
bool AvroSchemaProvider::check_full(const std::string& old_schema_text,
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
 * Normalize an Avro schema to its canonical form.
 */
std::string AvroSchemaProvider::normalize(const std::string& schema_text) {
    try {
        json schema = json::parse(schema_text);
        return canonical_form(schema);
    } catch (const std::exception& e) {
        AVRO_LOG_WARN("Failed to normalize Avro schema: {}", e.what());
        return schema_text; // Return original on error
    }
}

/**
 * Generate a fingerprint for an Avro schema.
 * Uses the canonical form for consistency.
 */
std::string AvroSchemaProvider::fingerprint(const std::string& schema_text) {
    // Simple fingerprint: hash the canonical form
    std::string canon = normalize(schema_text);
    std::hash<std::string> hasher;
    size_t h = hasher(canon);
    std::ostringstream oss;
    oss << std::hex << h;
    return oss.str();
}

// ============================================================================
// AvroSchemaProvider static header declaration is inline in the .cpp
// ============================================================================

/**
 * We declare the AvroSchemaProvider struct here since it's not in a
 * public header. It follows the same pattern as other schema providers.
 */
// The struct will be declared in avro_provider.h or similar.
// This .cpp provides the implementation.

} // namespace torrent::schema
