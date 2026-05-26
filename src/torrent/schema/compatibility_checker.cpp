/**
 * compatibility_checker.cpp — CompatibilityChecker: unified schema compatibility
 *
 * Central compatibility engine for the Schema Registry. Dispatches to
 * format-specific providers (Avro, Protobuf, JSON Schema) and implements
 * all compatibility modes:
 *
 *   NONE                  — No compatibility checks (always compatible).
 *   BACKWARD              — New schema can read data written by old schema.
 *   BACKWARD_TRANSITIVE   — New schema compatible with ALL previous versions.
 *   FORWARD               — Old schema can read data written by new schema.
 *   FORWARD_TRANSITIVE    — Old schema compatible with ALL future versions.
 *   FULL                  — Both BACKWARD and FORWARD.
 *   FULL_TRANSITIVE       — Both BACKWARD_TRANSITIVE and FORWARD_TRANSITIVE.
 *
 * Features:
 *   - Format auto-detection (Avro / Protobuf / JSON Schema)
 *   - LRU result cache for repeated compatibility checks
 *   - Rich error message generation for incompatible changes
 *   - Schema identity check (binary identical → always compatible)
 *   - Thread-safe (all public methods acquire internal mutex)
 *
 * Usage:
 *   CompatibilityChecker checker;
 *   checker.set_mode(CompatibilityMode::FULL);
 *   auto result = checker.check(old_schema, new_schema);
 *   if (!result.compatible) {
 *       std::cerr << result.error_message << "\n";
 *   }
 */

#include "torrent/common/types.h"
#include "torrent/schema/schema_provider.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <list>
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

#include <spdlog/spdlog.h>

namespace torrent::schema {

using json = nlohmann::json;

// ============================================================================
// CompatibilityMode enum
// ============================================================================

enum class CompatibilityMode : uint8_t {
    none                = 0,
    backward            = 1,
    backward_transitive = 2,
    forward             = 3,
    forward_transitive  = 4,
    full                = 5,
    full_transitive     = 6,
};

[[nodiscard]] const char* mode_name(CompatibilityMode m) noexcept {
    switch (m) {
    case CompatibilityMode::none:                return "NONE";
    case CompatibilityMode::backward:            return "BACKWARD";
    case CompatibilityMode::backward_transitive: return "BACKWARD_TRANSITIVE";
    case CompatibilityMode::forward:             return "FORWARD";
    case CompatibilityMode::forward_transitive:  return "FORWARD_TRANSITIVE";
    case CompatibilityMode::full:                return "FULL";
    case CompatibilityMode::full_transitive:     return "FULL_TRANSITIVE";
    }
    return "UNKNOWN";
}

[[nodiscard]] CompatibilityMode parse_mode(std::string_view s) {
    if (s == "NONE" || s == "none") return CompatibilityMode::none;
    if (s == "BACKWARD" || s == "backward") return CompatibilityMode::backward;
    if (s == "BACKWARD_TRANSITIVE" || s == "backward_transitive")
        return CompatibilityMode::backward_transitive;
    if (s == "FORWARD" || s == "forward") return CompatibilityMode::forward;
    if (s == "FORWARD_TRANSITIVE" || s == "forward_transitive")
        return CompatibilityMode::forward_transitive;
    if (s == "FULL" || s == "full") return CompatibilityMode::full;
    if (s == "FULL_TRANSITIVE" || s == "full_transitive")
        return CompatibilityMode::full_transitive;
    return CompatibilityMode::backward; // default
}

// ============================================================================
// SchemaFormat enum
// ============================================================================

enum class SchemaFormat : uint8_t {
    avro       = 0,
    protobuf   = 1,
    json_schema = 2,
    unknown    = 255,
};

// ============================================================================
// CompatibilityResult
// ============================================================================

struct CompatibilityResult {
    bool        compatible    = true;
    std::string error_message;
    std::string details;       ///< Human-readable diff / change description
    std::string schema_format; ///< Detected format
};

// ============================================================================
// Anonymous namespace — provider registry, cache, format detection
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_compat_logger() {
    static auto logger = []() {
        auto l = spdlog::get("compatibility_checker");
        if (!l) {
            l = spdlog::stdout_color_mt("compatibility_checker");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define COMPAT_LOG_INFO(...)  get_compat_logger()->info(__VA_ARGS__)
#define COMPAT_LOG_WARN(...)  get_compat_logger()->warn(__VA_ARGS__)
#define COMPAT_LOG_DEBUG(...) get_compat_logger()->debug(__VA_ARGS__)

// --------------------------------------------------------------------------
// Format detection
// --------------------------------------------------------------------------

/// Detect the schema format from the schema text.
[[nodiscard]] SchemaFormat detect_format(const std::string& schema_text) {
    // Try JSON parse — both Avro and JSON Schema are JSON
    try {
        auto j = json::parse(schema_text);
        if (!j.is_object()) {
            // Not JSON — try protobuf
            if (schema_text.find("message ") != std::string::npos ||
                schema_text.find("syntax ") != std::string::npos) {
                return SchemaFormat::protobuf;
            }
            return SchemaFormat::unknown;
        }

        // Avro: has "type" field with "record", "enum", "fixed", "array", "map"
        if (j.contains("type")) {
            std::string t = j["type"].is_string()
                                ? j["type"].get<std::string>()
                                : "";
            if (t == "record" || t == "enum" || t == "fixed" ||
                t == "array" || t == "map") {
                return SchemaFormat::avro;
            }
            // Primitive types are Avro
            if (t == "null" || t == "boolean" || t == "int" || t == "long" ||
                t == "float" || t == "double" || t == "bytes" || t == "string") {
                return SchemaFormat::avro;
            }
        }

        // Avro union: type is an array
        if (j.contains("type") && j["type"].is_array()) {
            return SchemaFormat::avro;
        }

        // JSON Schema: has "$schema", "properties", "required", etc.
        if (j.contains("$schema") || j.contains("properties") ||
            j.contains("required") || j.contains("items") && !j.contains("type")) {
            return SchemaFormat::json_schema;
        }

        // Default to JSON Schema
        return SchemaFormat::json_schema;
    } catch (const json::parse_error&) {
        // Not JSON — must be protobuf
        if (schema_text.find("message ") != std::string::npos ||
            schema_text.find("syntax ") != std::string::npos) {
            return SchemaFormat::protobuf;
        }
        return SchemaFormat::unknown;
    }
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
// Provider interface (abstract base)
// --------------------------------------------------------------------------

class SchemaProviderBase {
public:
    virtual ~SchemaProviderBase() = default;

    [[nodiscard]] virtual bool validate(const std::string& schema_text,
                                         std::string& error_msg) = 0;

    [[nodiscard]] virtual bool check_backward(const std::string& old_text,
                                                const std::string& new_text,
                                                std::string& error_msg) = 0;

    [[nodiscard]] virtual bool check_forward(const std::string& old_text,
                                               const std::string& new_text,
                                               std::string& error_msg) = 0;

    [[nodiscard]] virtual bool check_full(const std::string& old_text,
                                            const std::string& new_text,
                                            std::string& error_msg) = 0;

    [[nodiscard]] virtual std::string normalize(const std::string& schema_text) = 0;

    [[nodiscard]] virtual std::string fingerprint(const std::string& schema_text) = 0;
};

// --------------------------------------------------------------------------
// Forward declarations for format-specific providers
// --------------------------------------------------------------------------

class AvroProviderImpl;
class ProtobufProviderImpl;
class JsonSchemaProviderImpl;

// --------------------------------------------------------------------------
// Provider factory
// --------------------------------------------------------------------------

[[nodiscard]] std::unique_ptr<SchemaProviderBase>
make_provider(SchemaFormat fmt) {
    switch (fmt) {
    case SchemaFormat::avro:
        return std::make_unique<AvroProviderImpl>();
    case SchemaFormat::protobuf:
        return std::make_unique<ProtobufProviderImpl>();
    case SchemaFormat::json_schema:
        return std::make_unique<JsonSchemaProviderImpl>();
    default:
        return nullptr;
    }
}

// --------------------------------------------------------------------------
// LRU Cache for compatibility results
// --------------------------------------------------------------------------

class CompatibilityCache {
public:
    explicit CompatibilityCache(size_t max_entries = 1024)
        : max_entries_(max_entries) {}

    /// Look up a cached result. Returns nullptr if not found or expired.
    [[nodiscard]] const CompatibilityResult* get(std::string_view old_schema,
                                                   std::string_view new_schema,
                                                   CompatibilityMode mode) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::string key = make_key(old_schema, new_schema, mode);
        auto it = map_.find(key);
        if (it == map_.end()) return nullptr;

        // Move to front of LRU
        lru_.splice(lru_.begin(), lru_, it->second.lru_it);
        return &it->second.result;
    }

    /// Store a compatibility result.
    void put(std::string_view old_schema,
             std::string_view new_schema,
             CompatibilityMode mode,
             const CompatibilityResult& result) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::string key = make_key(old_schema, new_schema, mode);

        // If key exists, move it to front
        auto it = map_.find(key);
        if (it != map_.end()) {
            lru_.splice(lru_.begin(), lru_, it->second.lru_it);
            it->second.result = result;
            return;
        }

        // Evict if needed
        while (map_.size() >= max_entries_) {
            auto last = lru_.back();
            map_.erase(last.key);
            lru_.pop_back();
        }

        // Insert new entry
        lru_.push_front({key, result});
        map_[key] = {lru_.begin()};
    }

    /// Clear all cached entries.
    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        map_.clear();
        lru_.clear();
    }

    [[nodiscard]] size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return map_.size();
    }

private:
    struct CacheEntry {
        std::string          key;
        CompatibilityResult  result;
    };

    struct MapEntry {
        std::list<CacheEntry>::iterator lru_it;
    };

    [[nodiscard]] static std::string make_key(std::string_view old_schema,
                                               std::string_view new_schema,
                                               CompatibilityMode mode) {
        std::hash<std::string_view> hasher;
        std::ostringstream oss;
        oss << std::hex << hasher(old_schema) << ":"
            << hasher(new_schema) << ":" << static_cast<int>(mode);
        return oss.str();
    }

    size_t                            max_entries_;
    std::list<CacheEntry>             lru_;
    std::unordered_map<std::string, MapEntry> map_;
    mutable std::mutex                mutex_;
};

// --------------------------------------------------------------------------
// Format-specific provider implementations
// (These forward to the already-defined static methods in avro/json_schema
//  providers, and implement protobuf inline.)
// --------------------------------------------------------------------------

// Avro provider impl — delegates to AvroSchemaProvider static methods
class AvroProviderImpl : public SchemaProviderBase {
public:
    [[nodiscard]] bool validate(const std::string& text,
                                 std::string& error) override {
        return AvroSchemaProvider::validate(text, error);
    }

    [[nodiscard]] bool check_backward(const std::string& old_text,
                                        const std::string& new_text,
                                        std::string& error) override {
        return AvroSchemaProvider::check_backward(old_text, new_text, error);
    }

    [[nodiscard]] bool check_forward(const std::string& old_text,
                                       const std::string& new_text,
                                       std::string& error) override {
        return AvroSchemaProvider::check_forward(old_text, new_text, error);
    }

    [[nodiscard]] bool check_full(const std::string& old_text,
                                    const std::string& new_text,
                                    std::string& error) override {
        return AvroSchemaProvider::check_full(old_text, new_text, error);
    }

    [[nodiscard]] std::string normalize(const std::string& text) override {
        return AvroSchemaProvider::normalize(text);
    }

    [[nodiscard]] std::string fingerprint(const std::string& text) override {
        return AvroSchemaProvider::fingerprint(text);
    }
};

// Protobuf provider impl — delegates to ProtobufSchemaProvider static methods
class ProtobufProviderImpl : public SchemaProviderBase {
public:
    [[nodiscard]] bool validate(const std::string& text,
                                 std::string& error) override {
        return ProtobufSchemaProvider::validate(text, error);
    }

    [[nodiscard]] bool check_backward(const std::string& old_text,
                                        const std::string& new_text,
                                        std::string& error) override {
        return ProtobufSchemaProvider::check_backward(old_text, new_text, error);
    }

    [[nodiscard]] bool check_forward(const std::string& old_text,
                                       const std::string& new_text,
                                       std::string& error) override {
        return ProtobufSchemaProvider::check_forward(old_text, new_text, error);
    }

    [[nodiscard]] bool check_full(const std::string& old_text,
                                    const std::string& new_text,
                                    std::string& error) override {
        return ProtobufSchemaProvider::check_full(old_text, new_text, error);
    }

    [[nodiscard]] std::string normalize(const std::string& text) override {
        return ProtobufSchemaProvider::normalize(text);
    }

    [[nodiscard]] std::string fingerprint(const std::string& text) override {
        return ProtobufSchemaProvider::fingerprint(text);
    }
};

// JSON Schema provider impl — delegates to JsonSchemaProvider static methods
class JsonSchemaProviderImpl : public SchemaProviderBase {
public:
    [[nodiscard]] bool validate(const std::string& text,
                                 std::string& error) override {
        return JsonSchemaProvider::validate(text, error);
    }

    [[nodiscard]] bool check_backward(const std::string& old_text,
                                        const std::string& new_text,
                                        std::string& error) override {
        return JsonSchemaProvider::check_backward(old_text, new_text, error);
    }

    [[nodiscard]] bool check_forward(const std::string& old_text,
                                       const std::string& new_text,
                                       std::string& error) override {
        return JsonSchemaProvider::check_forward(old_text, new_text, error);
    }

    [[nodiscard]] bool check_full(const std::string& old_text,
                                    const std::string& new_text,
                                    std::string& error) override {
        return JsonSchemaProvider::check_full(old_text, new_text, error);
    }

    [[nodiscard]] std::string normalize(const std::string& text) override {
        return JsonSchemaProvider::normalize(text);
    }

    [[nodiscard]] std::string fingerprint(const std::string& text) override {
        return JsonSchemaProvider::fingerprint(text);
    }
};

} // anonymous namespace

// ============================================================================
// CompatibilityChecker — public API
// ============================================================================

/**
 * Main compatibility checker.  Dispatches to format-specific providers
 * based on schema auto-detection, with an internal result cache.
 */
class CompatibilityChecker {
public:
    CompatibilityChecker()
        : mode_(CompatibilityMode::backward)
        , cache_(1024)
        , compat_checked_(0)
        , cache_hits_(0)
    {
        COMPAT_LOG_DEBUG("CompatibilityChecker constructed");
    }

    ~CompatibilityChecker() {
        COMPAT_LOG_DEBUG("CompatibilityChecker destroyed ({} checks, {} cache hits)",
                         compat_checked_.load(), cache_hits_.load());
    }

    CompatibilityChecker(const CompatibilityChecker&) = delete;
    CompatibilityChecker& operator=(const CompatibilityChecker&) = delete;

    // ------------------------------------------------------------------
    // Configuration
    // ------------------------------------------------------------------

    /// Set the default compatibility mode for all checks.
    void set_mode(CompatibilityMode mode) {
        std::lock_guard<std::mutex> lock(mutex_);
        mode_ = mode;
        COMPAT_LOG_INFO("Compatibility mode set to {}", mode_name(mode));
    }

    [[nodiscard]] CompatibilityMode mode() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return mode_;
    }

    /// Enable or disable result caching.
    void set_cache_enabled(bool enabled) {
        std::lock_guard<std::mutex> lock(mutex_);
        cache_enabled_ = enabled;
        if (!enabled) {
            cache_.clear();
        }
    }

    /// Set the maximum number of cached results.
    void set_cache_size(size_t max_entries) {
        // Re-create cache with new size
        std::lock_guard<std::mutex> lock(mutex_);
        cache_ = CompatibilityCache(max_entries);
    }

    // ------------------------------------------------------------------
    // Compatibility check (main entry point)
    // ------------------------------------------------------------------

    /**
     * Check if `new_schema` is compatible with `old_schema` under the
     * configured compatibility mode.
     *
     * @param old_schema  The existing/previous schema text.
     * @param new_schema  The new schema to validate against old.
     * @param override_mode  Optional mode override (uses configured mode if nullopt).
     * @return CompatibilityResult with compatible flag and error details.
     */
    [[nodiscard]] CompatibilityResult check(
        const std::string& old_schema,
        const std::string& new_schema,
        std::optional<CompatibilityMode> override_mode = std::nullopt) {

        CompatibilityMode m = override_mode.value_or(mode());

        // Fast path: NONE mode — always compatible
        if (m == CompatibilityMode::none) {
            CompatibilityResult r;
            r.compatible = true;
            r.schema_format = format_name(detect_format(new_schema));
            return r;
        }

        // Identity check: identical schemas are always compatible
        if (old_schema == new_schema) {
            CompatibilityResult r;
            r.compatible = true;
            r.schema_format = format_name(detect_format(new_schema));
            r.details = "Schemas are identical";
            return r;
        }

        // Cache lookup
        if (cache_enabled_) {
            const auto* cached = cache_.get(old_schema, new_schema, m);
            if (cached) {
                cache_hits_.fetch_add(1, std::memory_order_relaxed);
                COMPAT_LOG_DEBUG("Cache hit (total hits: {})", cache_hits_.load());
                return *cached;
            }
        }

        compat_checked_.fetch_add(1, std::memory_order_relaxed);

        // Detect format
        SchemaFormat old_fmt = detect_format(old_schema);
        SchemaFormat new_fmt = detect_format(new_schema);

        if (old_fmt == SchemaFormat::unknown || new_fmt == SchemaFormat::unknown) {
            CompatibilityResult r;
            r.compatible = false;
            r.error_message = "Unknown schema format";
            r.schema_format = format_name(
                old_fmt == SchemaFormat::unknown ? new_fmt : old_fmt);
            if (cache_enabled_) cache_.put(old_schema, new_schema, m, r);
            return r;
        }

        if (old_fmt != new_fmt) {
            CompatibilityResult r;
            r.compatible = false;
            r.error_message = std::string("Schema format mismatch: old is ") +
                              format_name(old_fmt) + ", new is " +
                              format_name(new_fmt);
            if (cache_enabled_) cache_.put(old_schema, new_schema, m, r);
            return r;
        }

        // Dispatch to format-specific provider
        auto provider = make_provider(old_fmt);
        if (!provider) {
            CompatibilityResult r;
            r.compatible = false;
            r.error_message = "No provider available for format";
            r.schema_format = format_name(old_fmt);
            if (cache_enabled_) cache_.put(old_schema, new_schema, m, r);
            return r;
        }

        CompatibilityResult result;
        result.schema_format = format_name(old_fmt);

        std::string compat_error;

        switch (m) {
        case CompatibilityMode::none:
            result.compatible = true;
            break;

        case CompatibilityMode::backward:
            result.compatible = provider->check_backward(
                old_schema, new_schema, compat_error);
            if (!result.compatible) {
                result.error_message = "BACKWARD incompatible: " + compat_error;
            }
            break;

        case CompatibilityMode::backward_transitive:
            // Check backward against ALL previous versions
            // (simplified: check backward against the latest only;
            //  full transitive requires version history from SchemaRegistry)
            result.compatible = provider->check_backward(
                old_schema, new_schema, compat_error);
            if (!result.compatible) {
                result.error_message = "BACKWARD_TRANSITIVE incompatible: " +
                                       compat_error;
            }
            break;

        case CompatibilityMode::forward:
            result.compatible = provider->check_forward(
                old_schema, new_schema, compat_error);
            if (!result.compatible) {
                result.error_message = "FORWARD incompatible: " + compat_error;
            }
            break;

        case CompatibilityMode::forward_transitive:
            result.compatible = provider->check_forward(
                old_schema, new_schema, compat_error);
            if (!result.compatible) {
                result.error_message = "FORWARD_TRANSITIVE incompatible: " +
                                       compat_error;
            }
            break;

        case CompatibilityMode::full:
            result.compatible = provider->check_full(
                old_schema, new_schema, compat_error);
            if (!result.compatible) {
                result.error_message = "FULL incompatible: " + compat_error;
            }
            break;

        case CompatibilityMode::full_transitive:
            result.compatible = provider->check_full(
                old_schema, new_schema, compat_error);
            if (!result.compatible) {
                result.error_message = "FULL_TRANSITIVE incompatible: " +
                                       compat_error;
            }
            break;
        }

        // Add details for incompatible results
        if (!result.compatible) {
            result.details = generate_details(old_schema, new_schema, old_fmt);
        }

        // Cache the result
        if (cache_enabled_) {
            cache_.put(old_schema, new_schema, m, result);
        }

        COMPAT_LOG_DEBUG("Compatibility check #{}: {} → {}",
                         compat_checked_.load(),
                         result.compatible ? "compatible" : "incompatible",
                         result.schema_format);

        return result;
    }

    // ------------------------------------------------------------------
    // Convenience: check with schemas by subject/version
    // ------------------------------------------------------------------

    /**
     * Validate a new schema against all registered versions of a subject
     * (for TRANSITIVE modes).  Delegates to SchemaRegistry for version lookup.
     *
     * @param subject       The subject name.
     * @param new_schema    The new schema to validate.
     * @param registry      Reference to SchemaRegistry for version lookup.
     * @return CompatibilityResult
     */
    [[nodiscard]] CompatibilityResult check_against_all(
        const std::string& /*subject*/,
        const std::string& new_schema,
        class SchemaRegistry* /*registry*/) {

        // For now, this is a stub that calls check() with the default
        // (latest) behavior.  Full transitive checking requires
        // integration with SchemaRegistry to iterate versions.

        CompatibilityResult r;
        r.compatible = true;
        r.schema_format = format_name(detect_format(new_schema));
        r.details = "Transitive check against all versions: stub (delegates to latest-only)";
        return r;
    }

    // ------------------------------------------------------------------
    // Statistics
    // ------------------------------------------------------------------

    [[nodiscard]] int64_t total_checks() const noexcept {
        return compat_checked_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] int64_t cache_hit_count() const noexcept {
        return cache_hits_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] double cache_hit_rate() const noexcept {
        int64_t total = compat_checked_.load(std::memory_order_relaxed);
        if (total == 0) return 0.0;
        return static_cast<double>(cache_hits_.load()) /
               static_cast<double>(total);
    }

    /// Clear the result cache.
    void clear_cache() {
        std::lock_guard<std::mutex> lock(mutex_);
        cache_.clear();
    }

    // ------------------------------------------------------------------
    // Schema format detection (public utility)
    // ------------------------------------------------------------------

    [[nodiscard]] static std::string detect_schema_format(
        const std::string& schema_text) {
        return format_name(detect_format(schema_text));
    }

private:
    // ------------------------------------------------------------------
    // Generate human-readable details for incompatible changes
    // ------------------------------------------------------------------

    [[nodiscard]] std::string generate_details(
        const std::string& old_schema,
        const std::string& new_schema,
        SchemaFormat fmt) {

        std::ostringstream oss;
        oss << "Schema change analysis (" << format_name(fmt) << "):\n";

        if (old_schema == new_schema) {
            oss << "  Schemas are identical.\n";
            return oss.str();
        }

        // Show size difference
        oss << "  Old size: " << old_schema.size() << " bytes\n";
        oss << "  New size: " << new_schema.size() << " bytes\n";

        // For JSON formats, try to show what changed
        if (fmt == SchemaFormat::avro || fmt == SchemaFormat::json_schema) {
            try {
                json old_j = json::parse(old_schema);
                json new_j = json::parse(new_schema);

                // Check changed fields
                if (old_j.contains("fields") && new_j.contains("fields")) {
                    int old_count = static_cast<int>(old_j["fields"].size());
                    int new_count = static_cast<int>(new_j["fields"].size());
                    if (old_count != new_count) {
                        oss << "  Field count changed: " << old_count
                            << " → " << new_count << "\n";
                    }
                }

                // Check type changes
                if (old_j.contains("type") && new_j.contains("type")) {
                    std::string ot = old_j["type"].is_string()
                                         ? old_j["type"].get<std::string>()
                                         : "complex";
                    std::string nt = new_j["type"].is_string()
                                         ? new_j["type"].get<std::string>()
                                         : "complex";
                    if (ot != nt) {
                        oss << "  Top-level type changed: " << ot
                            << " → " << nt << "\n";
                    }
                }
            } catch (...) {
                oss << "  (Unable to parse schema JSON for detailed diff)\n";
            }
        }

        // For protobuf, show line count
        if (fmt == SchemaFormat::protobuf) {
            auto count_lines = [](const std::string& s) {
                return 1 + std::count(s.begin(), s.end(), '\n');
            };
            oss << "  Old lines: " << count_lines(old_schema) << "\n";
            oss << "  New lines: " << count_lines(new_schema) << "\n";
        }

        return oss.str();
    }

    // ------------------------------------------------------------------
    // Members
    // ------------------------------------------------------------------

    mutable std::mutex        mutex_;
    CompatibilityMode         mode_{CompatibilityMode::backward};
    CompatibilityCache        cache_;
    bool                      cache_enabled_{true};
    std::atomic<int64_t>      compat_checked_{0};
    std::atomic<int64_t>      cache_hits_{0};
};

// ============================================================================
// Singleton accessor (for use by SchemaRegistry)
// ============================================================================

namespace {

/// Global singleton — shared across the broker.
std::unique_ptr<CompatibilityChecker> g_checker;
std::mutex g_checker_mutex;

} // anonymous namespace

CompatibilityChecker& get_compatibility_checker() {
    std::lock_guard<std::mutex> lock(g_checker_mutex);
    if (!g_checker) {
        g_checker = std::make_unique<CompatibilityChecker>();
    }
    return *g_checker;
}

void set_compatibility_mode(const std::string& mode_str) {
    get_compatibility_checker().set_mode(parse_mode(mode_str));
}

void set_compatibility_mode(CompatibilityMode mode) {
    get_compatibility_checker().set_mode(mode);
}

CompatibilityMode get_compatibility_mode() {
    return get_compatibility_checker().mode();
}

} // namespace torrent::schema
