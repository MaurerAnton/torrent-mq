/**
 * schema_registry.cpp — Schema Registry: CRUD with Avro/Protobuf/JSON Schema
 *
 * The SchemaRegistry provides a Confluent-compatible schema management API:
 *
 *   - register_schema(subject, schema, format)
 *       Registers a new schema under a subject.  Returns the schema ID.
 *       Validates compatibility against the latest version (if any).
 *       Auto-increments version and global ID.
 *
 *   - get_schema(subject, version)
 *       Retrieves the schema text for a given subject+version pair.
 *
 *   - get_schema_by_id(schema_id)
 *       Global lookup by unique schema ID.
 *
 *   - list_versions(subject)
 *       Returns all version numbers for a subject (excluding soft-deleted).
 *
 *   - check_compatibility(subject, new_schema)
 *       Tests whether a new schema is compatible with the latest version
 *       for that subject.  Compatibility modes: BACKWARD (default),
 *       FORWARD, FULL, NONE.
 *
 *   - enable_persistence(db_path)
 *       Optionally enables RocksDB-backed persistence.  When enabled,
 *       schema registrations are durably written and can survive restarts.
 *       (Stub implementation provided; real RocksDB integration is a TODO.)
 *
 * Storage:
 *   - In-memory: std::unordered_map<subject, std::vector<SchemaEntry>>
 *     plus a global ID → entry map.
 *   - Each SchemaEntry stores: schema text, format, version, global ID,
 *     and a soft-delete flag.
 *
 * Thread-safety: all public methods are protected by a shared_mutex
 * (shared for reads, exclusive for writes).
 *
 * See schema_registry.h for the API contract.
 */

#include "torrent/schema/schema_registry.h"
#include "torrent/broker/server.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

// ============================================================================
// Convenience aliases
// ============================================================================

using json = nlohmann::json;

namespace torrent::schema {

// ============================================================================
// Anonymous namespace — internal helpers and types
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logging
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_schema_logger() {
    static auto logger = []() {
        auto l = spdlog::get("schema_registry");
        if (!l) {
            l = spdlog::stdout_color_mt("schema_registry");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define SCHEMA_LOG_INFO(...)  get_schema_logger()->info(__VA_ARGS__)
#define SCHEMA_LOG_WARN(...)  get_schema_logger()->warn(__VA_ARGS__)
#define SCHEMA_LOG_ERROR(...) get_schema_logger()->error(__VA_ARGS__)
#define SCHEMA_LOG_DEBUG(...) get_schema_logger()->debug(__VA_ARGS__)
#define SCHEMA_LOG_TRACE(...) get_schema_logger()->trace(__VA_ARGS__)

// --------------------------------------------------------------------------
// Schema format enumeration
// --------------------------------------------------------------------------

enum class SchemaFormat : uint8_t {
    avro       = 0,
    protobuf   = 1,
    json_schema = 2,
    unknown    = 255,
};

[[nodiscard]] SchemaFormat parse_format(std::string_view fmt) {
    if (fmt == "AVRO" || fmt == "avro")       return SchemaFormat::avro;
    if (fmt == "PROTOBUF" || fmt == "protobuf") return SchemaFormat::protobuf;
    if (fmt == "JSON" || fmt == "json" ||
        fmt == "JSON_SCHEMA" || fmt == "json_schema") return SchemaFormat::json_schema;
    return SchemaFormat::unknown;
}

[[nodiscard]] const char* format_name(SchemaFormat f) noexcept {
    switch (f) {
    case SchemaFormat::avro:       return "AVRO";
    case SchemaFormat::protobuf:   return "PROTOBUF";
    case SchemaFormat::json_schema: return "JSON";
    default:                       return "UNKNOWN";
    }
}

// --------------------------------------------------------------------------
// Compatibility level
// --------------------------------------------------------------------------

enum class CompatibilityLevel : uint8_t {
    none     = 0,  ///< No compatibility checks.
    backward = 1,  ///< New schema can read data written by previous schema.
    forward  = 2,  ///< Previous schema can read data written by new schema.
    full     = 3,  ///< Both backward and forward.
};

/// Default compatibility level for all subjects.
static CompatibilityLevel g_default_compat = CompatibilityLevel::backward;

// --------------------------------------------------------------------------
// SchemaEntry — a single schema version stored in the registry
// --------------------------------------------------------------------------

struct SchemaEntry {
    int32_t schema_id = -1;          ///< Globally unique ID.
    int32_t version = -1;             ///< Per-subject version (1-based).
    std::string subject;              ///< Subject (topic name or logical name).
    std::string schema_text;          ///< The raw schema definition.
    SchemaFormat format = SchemaFormat::unknown;
    bool deleted = false;             ///< Soft-delete flag.
    int64_t created_at_ms = 0;       ///< Epoch ms at registration.

    /// Serialize to JSON for RocksDB persistence.
    [[nodiscard]] json to_json() const {
        json j;
        j["schema_id"] = schema_id;
        j["version"] = version;
        j["subject"] = subject;
        j["schema"] = schema_text;
        j["format"] = format_name(format);
        j["deleted"] = deleted;
        j["created_at_ms"] = created_at_ms;
        return j;
    }

    /// Deserialize from JSON.
    static SchemaEntry from_json(const json& j) {
        SchemaEntry e;
        e.schema_id   = j.value("schema_id", -1);
        e.version     = j.value("version", -1);
        e.subject     = j.value("subject", "");
        e.schema_text = j.value("schema", "");
        e.format      = parse_format(j.value("format", ""));
        e.deleted     = j.value("deleted", false);
        e.created_at_ms = j.value("created_at_ms", int64_t{0});
        return e;
    }
};

// --------------------------------------------------------------------------
// JSON Schema validation & compatibility helpers
// --------------------------------------------------------------------------

/**
 * Validate a JSON Schema string by attempting to parse it as JSON and
 * checking for the presence of basic structure.
 *
 * Returns true if the schema looks structurally valid.
 */
[[nodiscard]] bool validate_json_schema_syntax(const std::string& schema_text) {
    try {
        auto j = json::parse(schema_text);
        // A JSON Schema must be a JSON object
        if (!j.is_object()) return false;
        // It should have at least a "type" or "$schema" field, or be a valid
        // JSON Schema draft.  We are permissive here; just check it's an object.
        return true;
    } catch (const json::parse_error& e) {
        SCHEMA_LOG_DEBUG("JSON Schema parse error: {}", e.what());
        return false;
    }
}

/**
 * Minimal JSON Schema compatibility check (BACKWARD mode):
 *   - The new schema must accept all data that the old schema accepts.
 *   - Implementation: we compare the "type" field and "properties" keys.
 *     If the old schema has required properties that the new schema
 *     lacks, or if the type is narrowed, it's incompatible.
 *
 * This is a simplified heuristic; a full implementation would use a
 * proper JSON Schema validator library.
 */
[[nodiscard]] bool check_json_schema_compatibility(const std::string& old_schema_text,
                                                    const std::string& new_schema_text,
                                                    CompatibilityLevel level) {
    if (level == CompatibilityLevel::none) return true;

    try {
        json old_schema = json::parse(old_schema_text);
        json new_schema = json::parse(new_schema_text);

        // BACKWARD: new can read old data
        if (level == CompatibilityLevel::backward || level == CompatibilityLevel::full) {
            // Type must not be narrowed
            std::string old_type = old_schema.value("type", "");
            std::string new_type = new_schema.value("type", "");
            if (!old_type.empty() && !new_type.empty() && old_type != new_type) {
                // Type changed → incompatible unless widening
                // (e.g., "integer" → "number" is OK, "string" → "integer" is not)
                if (old_type == "integer" && new_type == "number") {
                    // Widening: OK
                } else if (old_type == "number" && new_type == "integer") {
                    // Narrowing: NOT OK
                    SCHEMA_LOG_DEBUG("Type narrowed from {} to {}", old_type, new_type);
                    return false;
                } else if (old_type != new_type) {
                    SCHEMA_LOG_DEBUG("Type changed from {} to {}", old_type, new_type);
                    return false;
                }
            }

            // New required fields must not be added (old data won't have them)
            if (old_schema.contains("required") && new_schema.contains("required")) {
                auto old_req = old_schema["required"];
                auto new_req = new_schema["required"];
                for (const auto& r : new_req) {
                    bool found = false;
                    for (const auto& o : old_req) {
                        if (r == o) { found = true; break; }
                    }
                    if (!found) {
                        SCHEMA_LOG_DEBUG("New required field '{}' not in old schema", r.get<std::string>());
                        return false;
                    }
                }
            }

            // Old properties must exist in new schema (with compatible types)
            if (old_schema.contains("properties") && new_schema.contains("properties")) {
                auto old_props = old_schema["properties"];
                auto new_props = new_schema["properties"];
                for (auto it = old_props.begin(); it != old_props.end(); ++it) {
                    if (!new_props.contains(it.key())) {
                        // Property removed → incompatible (BACKWARD)
                        SCHEMA_LOG_DEBUG("Property '{}' removed", it.key());
                        return false;
                    }
                    // Check type compatibility of property
                    std::string old_prop_type = it.value().value("type", "");
                    std::string new_prop_type = new_props[it.key()].value("type", "");
                    if (!old_prop_type.empty() && !new_prop_type.empty() &&
                        old_prop_type != new_prop_type) {
                        if (!(old_prop_type == "integer" && new_prop_type == "number")) {
                            SCHEMA_LOG_DEBUG("Property '{}' type changed from {} to {}",
                                             it.key(), old_prop_type, new_prop_type);
                            return false;
                        }
                    }
                }
            }
        }

        // FORWARD: old can read new data
        if (level == CompatibilityLevel::forward || level == CompatibilityLevel::full) {
            if (old_schema.contains("properties") && new_schema.contains("properties")) {
                auto old_props = old_schema["properties"];
                auto new_props = new_schema["properties"];
                for (auto it = new_props.begin(); it != new_props.end(); ++it) {
                    if (!old_props.contains(it.key())) {
                        // New property added → old can't read, but for FORWARD
                        // this is only a problem if the property is required
                        if (new_schema.contains("required")) {
                            for (const auto& r : new_schema["required"]) {
                                if (r == it.key()) {
                                    SCHEMA_LOG_DEBUG("New required property '{}' not in old schema (FORWARD)", it.key());
                                    return false;
                                }
                            }
                        }
                    }
                }
            }
        }

        return true;
    } catch (const json::parse_error& e) {
        SCHEMA_LOG_WARN("JSON Schema parse error during compatibility check: {}", e.what());
        return false;
    } catch (const std::exception& e) {
        SCHEMA_LOG_WARN("Error during compatibility check: {}", e.what());
        return false;
    }
}

// --------------------------------------------------------------------------
// Avro schema stub validation
// --------------------------------------------------------------------------

/**
 * Minimal Avro schema validation: must be valid JSON with a "type" field.
 * A full implementation would use libavro or avro-cpp.
 */
[[nodiscard]] bool validate_avro_schema(const std::string& schema_text) {
    try {
        auto j = json::parse(schema_text);
        if (!j.is_object()) return false;
        // Avro schema must have a "type" field
        if (!j.contains("type")) return false;
        return true;
    } catch (const json::parse_error&) {
        return false;
    }
}

/**
 * Stub Avro compatibility check.  Always returns true (NONE compat).
 * TODO: Implement FULL/TRANSITIVE Avro schema evolution rules using libavro.
 */
[[nodiscard]] bool check_avro_compatibility(const std::string& /*old_text*/,
                                             const std::string& /*new_text*/,
                                             CompatibilityLevel /*level*/) {
    // Stub: Avro compatibility requires schema fingerprinting and
    // canonical form comparison — not yet implemented.
    return true;
}

// --------------------------------------------------------------------------
// Protobuf schema stub validation
// --------------------------------------------------------------------------

/**
 * Minimal Protobuf schema validation: check that it looks like a .proto file.
 * A full implementation would use libprotobuf/protoc to parse.
 */
[[nodiscard]] bool validate_protobuf_schema(const std::string& schema_text) {
    // A .proto file must contain at least one "message" keyword
    // (simple heuristic — production would invoke protoc).
    return schema_text.find("message ") != std::string::npos ||
           schema_text.find("syntax ") != std::string::npos;
}

/**
 * Stub Protobuf compatibility check.  Always returns true.
 * TODO: Implement using protobuf descriptor comparison.
 */
[[nodiscard]] bool check_protobuf_compatibility(const std::string& /*old_text*/,
                                                 const std::string& /*new_text*/,
                                                 CompatibilityLevel /*level*/) {
    return true;
}

// --------------------------------------------------------------------------
// Format-specific validate + compatibility dispatchers
// --------------------------------------------------------------------------

[[nodiscard]] bool validate_schema(SchemaFormat fmt, const std::string& schema_text) {
    switch (fmt) {
    case SchemaFormat::json_schema: return validate_json_schema_syntax(schema_text);
    case SchemaFormat::avro:       return validate_avro_schema(schema_text);
    case SchemaFormat::protobuf:   return validate_protobuf_schema(schema_text);
    default:                       return false;
    }
}

[[nodiscard]] bool is_compatible(SchemaFormat fmt,
                                  const std::string& old_text,
                                  const std::string& new_text,
                                  CompatibilityLevel level) {
    switch (fmt) {
    case SchemaFormat::json_schema:
        return check_json_schema_compatibility(old_text, new_text, level);
    case SchemaFormat::avro:
        return check_avro_compatibility(old_text, new_text, level);
    case SchemaFormat::protobuf:
        return check_protobuf_compatibility(old_text, new_text, level);
    default:
        return false;
    }
}

// --------------------------------------------------------------------------
// Timestamp helper
// --------------------------------------------------------------------------

[[nodiscard]] int64_t epoch_ms_now() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
}

} // anonymous namespace

// ============================================================================
// SchemaRegistry::Impl — in-memory storage + optional RocksDB persistence
// ============================================================================

class SchemaRegistry::Impl {
public:
    explicit Impl(broker::BrokerServer& server)
        : server_(&server)
    {
        SCHEMA_LOG_DEBUG("SchemaRegistry::Impl constructed");
    }

    ~Impl() {
        SCHEMA_LOG_DEBUG("SchemaRegistry::Impl destroyed");
        if (persistence_enabled_) {
            flush_to_disk();
        }
    }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    // -- Persistence ---------------------------------------------------------

    void enable_persistence(const std::string& db_path) {
        std::unique_lock lock(mutex_);
        db_path_ = db_path;
        persistence_enabled_ = true;
        SCHEMA_LOG_INFO("Schema registry persistence enabled at {}", db_path);
        // Load existing schemas from disk if available
        load_from_disk();
    }

    [[nodiscard]] bool is_persistence_enabled() const noexcept {
        std::shared_lock lock(mutex_);
        return persistence_enabled_;
    }

    // -- register_schema -----------------------------------------------------

    result<int32_t> register_schema(const std::string& subject,
                                     const std::string& schema_text,
                                     const std::string& format_str) {
        SchemaFormat fmt = parse_format(format_str);
        if (fmt == SchemaFormat::unknown) {
            return result<int32_t>::failure(
                error_code::invalid_request,
                "Unknown schema format: " + format_str +
                ". Supported: AVRO, PROTOBUF, JSON");
        }

        if (!validate_schema(fmt, schema_text)) {
            return result<int32_t>::failure(
                error_code::invalid_request,
                std::string("Invalid ") + format_name(fmt) + " schema syntax");
        }

        std::unique_lock lock(mutex_);

        // Check if an identical schema already exists for this subject
        auto& versions = subjects_[subject];
        for (const auto& entry : versions) {
            if (!entry.deleted && entry.schema_text == schema_text && entry.format == fmt) {
                SCHEMA_LOG_DEBUG("Identical schema already registered for subject \"{}\" (id={})",
                                 subject, entry.schema_id);
                return result<int32_t>::success(entry.schema_id);
            }
        }

        // Check compatibility with latest version
        int32_t latest_version = versions.empty() ? 0 : versions.back().version;
        if (latest_version > 0) {
            const auto& latest = versions.back();
            if (!latest.deleted) {
                if (!is_compatible(fmt, latest.schema_text, schema_text, g_default_compat)) {
                    return result<int32_t>::failure(
                        error_code::schema_incompatible,
                        "New schema is not compatible with the latest version ("
                        + std::to_string(latest_version) + ") for subject \"" + subject + "\"");
                }
            }
        }

        // Create the schema entry
        SchemaEntry entry;
        entry.schema_id      = next_schema_id_++;
        entry.version        = latest_version + 1;
        entry.subject        = subject;
        entry.schema_text    = schema_text;
        entry.format         = fmt;
        entry.deleted        = false;
        entry.created_at_ms  = epoch_ms_now();

        versions.push_back(entry);
        id_index_[entry.schema_id] = &versions.back();

        if (persistence_enabled_) {
            persist_entry(entry);
        }

        SCHEMA_LOG_INFO("Registered schema id={} for subject \"{}\" version={} format={}",
                        entry.schema_id, subject, entry.version, format_name(fmt));

        return result<int32_t>::success(entry.schema_id);
    }

    // -- get_schema (by subject + version) -----------------------------------

    [[nodiscard]] std::optional<std::string> get_schema(const std::string& subject,
                                                         int32_t version) const {
        std::shared_lock lock(mutex_);

        auto it = subjects_.find(subject);
        if (it == subjects_.end()) return std::nullopt;

        for (const auto& entry : it->second) {
            if (entry.version == version && !entry.deleted) {
                return entry.schema_text;
            }
        }
        return std::nullopt;
    }

    // -- get_schema_by_id ----------------------------------------------------

    [[nodiscard]] std::optional<std::string> get_schema_by_id(int32_t schema_id) const {
        std::shared_lock lock(mutex_);

        auto it = id_index_.find(schema_id);
        if (it == id_index_.end()) return std::nullopt;
        if (it->second->deleted) return std::nullopt;
        return it->second->schema_text;
    }

    // -- list_versions -------------------------------------------------------

    [[nodiscard]] std::vector<int32_t> list_versions(const std::string& subject) const {
        std::shared_lock lock(mutex_);

        std::vector<int32_t> result;
        auto it = subjects_.find(subject);
        if (it == subjects_.end()) return result;

        result.reserve(it->second.size());
        for (const auto& entry : it->second) {
            if (!entry.deleted) {
                result.push_back(entry.version);
            }
        }
        return result;
    }

    // -- check_compatibility -------------------------------------------------

    [[nodiscard]] bool check_compatibility(const std::string& subject,
                                            const std::string& new_schema_text) const {
        std::shared_lock lock(mutex_);

        auto it = subjects_.find(subject);
        if (it == subjects_.end() || it->second.empty()) {
            // No existing schemas → always compatible
            return true;
        }

        // Find latest non-deleted entry
        const SchemaEntry* latest = nullptr;
        for (auto rit = it->second.rbegin(); rit != it->second.rend(); ++rit) {
            if (!rit->deleted) {
                latest = &*rit;
                break;
            }
        }
        if (!latest) return true; // All deleted → compatible

        // We don't know the format of the new schema — try to detect
        // For validation, we check against each format
        SchemaFormat new_fmt = parse_format("JSON"); // default guess
        // Try auto-detection
        try {
            auto j = json::parse(new_schema_text);
            if (j.is_object()) {
                if (j.contains("type") && !j.contains("fields")) {
                    // Could be JSON Schema or Avro; default to JSON Schema
                    new_fmt = SchemaFormat::json_schema;
                }
            }
        } catch (...) {
            // Not JSON — might be protobuf
            if (new_schema_text.find("message ") != std::string::npos ||
                new_schema_text.find("syntax ") != std::string::npos) {
                new_fmt = SchemaFormat::protobuf;
            }
        }

        // If formats differ, we can't check compatibility
        if (new_fmt != latest->format) {
            SCHEMA_LOG_DEBUG("Format mismatch: existing={} new={}",
                             format_name(latest->format), format_name(new_fmt));
            return false;
        }

        return is_compatible(latest->format, latest->schema_text,
                             new_schema_text, g_default_compat);
    }

    // -- Soft delete (internal use, not exposed in public API yet) -----------

    void soft_delete(const std::string& subject, int32_t version) {
        std::unique_lock lock(mutex_);

        auto it = subjects_.find(subject);
        if (it == subjects_.end()) return;

        for (auto& entry : it->second) {
            if (entry.version == version && !entry.deleted) {
                entry.deleted = true;
                SCHEMA_LOG_INFO("Soft-deleted schema subject=\"{}\" version={} id={}",
                                subject, version, entry.schema_id);
                if (persistence_enabled_) {
                    persist_entry(entry);
                }
                return;
            }
        }
    }

    // -- Get full entry (for admin/debug) ------------------------------------

    [[nodiscard]] std::optional<SchemaEntry> get_entry(const std::string& subject,
                                                        int32_t version) const {
        std::shared_lock lock(mutex_);
        auto it = subjects_.find(subject);
        if (it == subjects_.end()) return std::nullopt;
        for (const auto& entry : it->second) {
            if (entry.version == version) return entry;
        }
        return std::nullopt;
    }

    // -- Subject count -------------------------------------------------------

    [[nodiscard]] size_t subject_count() const {
        std::shared_lock lock(mutex_);
        return subjects_.size();
    }

private:
    // -- Persistence helpers -------------------------------------------------

    void persist_entry(const SchemaEntry& entry) {
        if (!persistence_enabled_ || db_path_.empty()) return;

        // Write entry as a JSON file: <db_path>/<subject>/<version>.json
        std::string dir = db_path_ + "/" + entry.subject;
        // Create directory if needed (best effort)
        std::string mkdir_cmd = "mkdir -p \"" + dir + "\" 2>/dev/null";
        (void)std::system(mkdir_cmd.c_str());

        std::string filename = dir + "/" + std::to_string(entry.version) + ".json";
        std::ofstream ofs(filename);
        if (ofs.is_open()) {
            ofs << entry.to_json().dump(2);
            SCHEMA_LOG_TRACE("Persisted schema entry to {}", filename);
        } else {
            SCHEMA_LOG_WARN("Failed to persist schema entry to {}", filename);
        }
    }

    void load_from_disk() {
        if (!persistence_enabled_ || db_path_.empty()) return;

        SCHEMA_LOG_INFO("Loading schemas from disk at {}", db_path_);
        // Walk db_path_ for subject directories
        std::string ls_cmd = "find \"" + db_path_ + "\" -name '*.json' -type f 2>/dev/null";
        FILE* pipe = popen(ls_cmd.c_str(), "r");
        if (!pipe) return;

        char line[4096];
        int loaded = 0;
        while (fgets(line, sizeof(line), pipe)) {
            std::string filepath(line);
            // Trim trailing newline
            if (!filepath.empty() && filepath.back() == '\n') filepath.pop_back();

            std::ifstream ifs(filepath);
            if (!ifs.is_open()) continue;

            try {
                json j;
                ifs >> j;
                SchemaEntry entry = SchemaEntry::from_json(j);

                if (entry.schema_id >= 0 && !entry.subject.empty()) {
                    // Restore ID counter
                    if (entry.schema_id >= next_schema_id_) {
                        next_schema_id_ = entry.schema_id + 1;
                    }

                    auto& versions = subjects_[entry.subject];
                    // Avoid duplicates on reload
                    bool dup = false;
                    for (const auto& v : versions) {
                        if (v.version == entry.version) { dup = true; break; }
                    }
                    if (!dup) {
                        versions.push_back(entry);
                        // Sort by version
                        std::sort(versions.begin(), versions.end(),
                                  [](const SchemaEntry& a, const SchemaEntry& b) {
                                      return a.version < b.version;
                                  });
                        id_index_[entry.schema_id] = &versions.back();
                        ++loaded;
                    }
                }
            } catch (const std::exception& e) {
                SCHEMA_LOG_WARN("Failed to load schema from {}: {}", filepath, e.what());
            }
        }
        pclose(pipe);
        SCHEMA_LOG_INFO("Loaded {} schema entries from disk", loaded);
    }

    void flush_to_disk() {
        if (!persistence_enabled_ || db_path_.empty()) return;

        SCHEMA_LOG_INFO("Flushing schema registry to disk...");
        for (const auto& [subject, versions] : subjects_) {
            for (const auto& entry : versions) {
                persist_entry(entry);
            }
        }
    }

    // -- Members -------------------------------------------------------------

    broker::BrokerServer* server_;

    mutable std::shared_mutex mutex_;

    /// Subject → ordered list of SchemaEntry (by version).
    std::unordered_map<std::string, std::vector<SchemaEntry>> subjects_;

    /// Global schema ID → SchemaEntry pointer (into subjects_).
    std::unordered_map<int32_t, const SchemaEntry*> id_index_;

    /// Auto-incrementing global schema ID counter.
    int32_t next_schema_id_ = 1;

    /// Persistence state.
    bool persistence_enabled_ = false;
    std::string db_path_;
};

// ============================================================================
// SchemaRegistry — public API delegation
// ============================================================================

SchemaRegistry::SchemaRegistry(broker::BrokerServer& s)
    : impl_(std::make_unique<Impl>(s))
    , server_(&s)
{
    SCHEMA_LOG_DEBUG("SchemaRegistry constructed");
}

SchemaRegistry::~SchemaRegistry() = default;

SchemaRegistry::SchemaRegistry(SchemaRegistry&& other) noexcept
    : impl_(std::move(other.impl_))
    , server_(other.server_)
{
    other.server_ = nullptr;
}

SchemaRegistry& SchemaRegistry::operator=(SchemaRegistry&& other) noexcept {
    if (this != &other) {
        impl_ = std::move(other.impl_);
        server_ = other.server_;
        other.server_ = nullptr;
    }
    return *this;
}

result<int32_t> SchemaRegistry::register_schema(const std::string& subject,
                                                  const std::string& schema,
                                                  const std::string& format) {
    return impl_->register_schema(subject, schema, format);
}

std::optional<std::string> SchemaRegistry::get_schema(const std::string& subject,
                                                       int32_t version) const {
    return impl_->get_schema(subject, version);
}

std::optional<std::string> SchemaRegistry::get_schema_by_id(int32_t schema_id) const {
    return impl_->get_schema_by_id(schema_id);
}

std::vector<int32_t> SchemaRegistry::list_versions(const std::string& subject) const {
    return impl_->list_versions(subject);
}

bool SchemaRegistry::check_compatibility(const std::string& subject,
                                          const std::string& new_schema) const {
    return impl_->check_compatibility(subject, new_schema);
}

void SchemaRegistry::enable_persistence(const std::string& db_path) {
    impl_->enable_persistence(db_path);
}

bool SchemaRegistry::is_persistence_enabled() const noexcept {
    return impl_->is_persistence_enabled();
}

} // namespace torrent::schema
