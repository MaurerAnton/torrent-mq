/**
 * transforms.cpp — Connect Transform Functions
 *
 * Provides a library of single-message transform (SMT) functions for the
 * torrent-mq connect framework.  Transforms are applied to records as they
 * flow through source and sink connectors, allowing field-level manipulation
 * without writing custom connector code.
 *
 * Supported transforms:
 *
 *   Cast:               convert field types (string→int, int→string, etc.)
 *   Drop:               remove specified fields from records
 *   Extract:            extract nested fields using dot-notation paths
 *   Mask:               mask sensitive fields (credit cards, SSNs, emails)
 *   TimestampConverter: convert between timestamp formats (epoch, ISO8601, etc.)
 *   RegexRouter:        route records to different topics based on regex match
 *   InsertField:        add static fields or metadata fields to records
 *
 * Each transform is a function object that takes a record and configuration
 * and returns a transformed record (or std::nullopt to drop the record).
 *
 * Transform chaining:
 *   Multiple transforms can be chained together in the connector config:
 *     "transforms": "Cast,ExtractField,MaskField"
 *   Each transform has its own sub-config:
 *     "transforms.Cast.casts": "age:int32,score:float64"
 *
 * Thread-safety:
 *   Transform functions are stateless (all config parsed at construction) and
 *   safe for concurrent use from multiple connector tasks.
 */

#include "torrent/connectors/connect.h"

#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <functional>
#include <iomanip>
#include <map>
#include <memory>
#include <optional>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

using json = nlohmann::json;

namespace torrent::connectors {

// ============================================================================
// Anonymous namespace — helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_transforms_logger() {
    static auto logger = spdlog::get("transforms");
    if (!logger) {
        logger = spdlog::stdout_color_mt("transforms");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Split string by delimiter
// --------------------------------------------------------------------------

[[nodiscard]] std::vector<std::string> split_string(std::string_view str,
                                                      char delim) {
    std::vector<std::string> parts;
    std::istringstream iss(std::string(str));
    std::string token;
    while (std::getline(iss, token, delim)) {
        token.erase(0, token.find_first_not_of(" \t"));
        token.erase(token.find_last_not_of(" \t") + 1);
        if (!token.empty()) parts.push_back(token);
    }
    return parts;
}

// --------------------------------------------------------------------------
// Get nested field by dot-notation path (e.g., "address.city")
// --------------------------------------------------------------------------

[[nodiscard]] json* get_nested(json& root, std::string_view path) {
    auto parts = split_string(path, '.');
    json* current = &root;
    for (auto& part : parts) {
        if (!current->is_object()) return nullptr;
        auto it = current->find(part);
        if (it == current->end()) return nullptr;
        current = &(*it);
    }
    return current;
}

[[nodiscard]] const json* get_nested(const json& root, std::string_view path) {
    auto parts = split_string(path, '.');
    const json* current = &root;
    for (auto& part : parts) {
        if (!current->is_object()) return nullptr;
        auto it = current->find(part);
        if (it == current->end()) return nullptr;
        current = &(*it);
    }
    return current;
}

// --------------------------------------------------------------------------
// Set nested field, creating intermediate objects as needed
// --------------------------------------------------------------------------

void set_nested(json& root, std::string_view path, json value) {
    auto parts = split_string(path, '.');
    if (parts.empty()) return;

    json* current = &root;
    for (size_t i = 0; i < parts.size() - 1; ++i) {
        if (!current->is_object()) *current = json::object();
        auto it = current->find(parts[i]);
        if (it == current->end() || !it->is_object()) {
            (*current)[parts[i]] = json::object();
        }
        current = &(*current)[parts[i]];
    }

    (*current)[parts.back()] = std::move(value);
}

// --------------------------------------------------------------------------
// Delete nested field
// --------------------------------------------------------------------------

bool delete_nested(json& root, std::string_view path) {
    auto parts = split_string(path, '.');
    if (parts.empty()) return false;

    json* current = &root;
    for (size_t i = 0; i < parts.size() - 1; ++i) {
        if (!current->is_object()) return false;
        auto it = current->find(parts[i]);
        if (it == current->end() || !it->is_object()) return false;
        current = &(*it);
    }

    if (!current->is_object()) return false;
    return current->erase(parts.back()) > 0;
}

} // anonymous namespace

// ============================================================================
// Transform base class
// ============================================================================

class Transform {
public:
    virtual ~Transform() = default;

    /// Apply the transform to a record.  Returns std::nullopt to drop the record.
    /// The input record is expected to have "key", "value", "topic" fields.
    [[nodiscard]] virtual std::optional<json> apply(json record) = 0;

    /// Return the transform name for logging and status.
    [[nodiscard]] virtual std::string name() const = 0;
};

// ============================================================================
// CastTransform: Convert field types
// ============================================================================

class CastTransform final : public Transform {
public:
    explicit CastTransform(const json& config) {
        auto logger = get_transforms_logger();

        std::string casts_spec;
        auto it = config.find("casts");
        if (it != config.end() && it->is_string()) {
            casts_spec = it->get<std::string>();
        } else {
            it = config.find("Cast.casts");
            if (it != config.end() && it->is_string()) {
                casts_spec = it->get<std::string>();
            }
        }

        // Parse "field:type,field:type,..." format
        for (auto& spec : split_string(casts_spec, ',')) {
            auto colon = spec.find(':');
            if (colon == std::string::npos) continue;

            std::string field = spec.substr(0, colon);
            std::string type  = spec.substr(colon + 1);

            // Trim
            field.erase(0, field.find_first_not_of(" \t"));
            field.erase(field.find_last_not_of(" \t") + 1);
            type.erase(0, type.find_first_not_of(" \t"));
            type.erase(type.find_last_not_of(" \t") + 1);

            // Normalize type
            for (auto& c : type) c = static_cast<char>(std::tolower(c));

            CastTarget target = CastTarget::string_val;
            if (type == "int8" || type == "byte")  target = CastTarget::int8;
            else if (type == "int16" || type == "short") target = CastTarget::int16;
            else if (type == "int32" || type == "int" || type == "integer") target = CastTarget::int32;
            else if (type == "int64" || type == "long") target = CastTarget::int64;
            else if (type == "float32" || type == "float") target = CastTarget::float32;
            else if (type == "float64" || type == "double") target = CastTarget::float64;
            else if (type == "boolean" || type == "bool") target = CastTarget::boolean_val;
            else if (type == "string" || type == "str") target = CastTarget::string_val;

            casts_.emplace_back(field, target);
            logger->debug("CastTransform: {} -> {}", field, type);
        }

        logger->info("CastTransform initialized with {} casts", casts_.size());
    }

    [[nodiscard]] std::optional<json> apply(json record) override {
        if (!record.contains("value") || !record["value"].is_object())
            return record;

        auto& val = record["value"];

        for (auto& [field, target] : casts_) {
            json* jfield = get_nested(val, field);
            if (!jfield) continue;

            try {
                switch (target) {
                case CastTarget::int8:
                    *jfield = static_cast<int8_t>(jfield->get<int64_t>());
                    break;
                case CastTarget::int16:
                    *jfield = static_cast<int16_t>(jfield->get<int64_t>());
                    break;
                case CastTarget::int32:
                    *jfield = static_cast<int32_t>(jfield->get<int64_t>());
                    break;
                case CastTarget::int64:
                    if (jfield->is_string()) {
                        *jfield = std::stoll(jfield->get<std::string>());
                    } else {
                        *jfield = jfield->get<int64_t>();
                    }
                    break;
                case CastTarget::float32:
                    if (jfield->is_string()) {
                        *jfield = std::stof(jfield->get<std::string>());
                    } else {
                        *jfield = static_cast<float>(jfield->get<double>());
                    }
                    break;
                case CastTarget::float64:
                    if (jfield->is_string()) {
                        *jfield = std::stod(jfield->get<std::string>());
                    } else {
                        *jfield = jfield->get<double>();
                    }
                    break;
                case CastTarget::boolean_val:
                    if (jfield->is_string()) {
                        std::string s = jfield->get<std::string>();
                        for (auto& c : s) c = static_cast<char>(std::tolower(c));
                        *jfield = (s == "true" || s == "1" || s == "yes");
                    } else {
                        *jfield = jfield->get<bool>();
                    }
                    break;
                case CastTarget::string_val:
                    *jfield = jfield->dump();
                    break;
                }
            } catch (const std::exception& e) {
                auto logger = get_transforms_logger();
                logger->warn("CastTransform: failed to cast '{}' to target type: {}",
                             field, e.what());
            }
        }

        return record;
    }

    [[nodiscard]] std::string name() const override { return "Cast"; }

private:
    enum class CastTarget : uint8_t {
        int8, int16, int32, int64,
        float32, float64,
        boolean_val,
        string_val,
    };

    std::vector<std::pair<std::string, CastTarget>> casts_;
};

// ============================================================================
// DropTransform: Remove specified fields
// ============================================================================

class DropTransform final : public Transform {
public:
    explicit DropTransform(const json& config) {
        auto logger = get_transforms_logger();

        std::string fields_spec;
        auto it = config.find("fields");
        if (it != config.end() && it->is_string()) {
            fields_spec = it->get<std::string>();
        } else {
            it = config.find("Drop.fields");
            if (it != config.end() && it->is_string()) {
                fields_spec = it->get<std::string>();
            }
        }

        for (auto& field : split_string(fields_spec, ',')) {
            fields_to_drop_.insert(field);
            logger->debug("DropTransform: will drop field '{}'", field);
        }

        logger->info("DropTransform initialized, dropping {} fields",
                     fields_to_drop_.size());
    }

    [[nodiscard]] std::optional<json> apply(json record) override {
        if (!record.contains("value") || !record["value"].is_object())
            return record;

        auto& val = record["value"];
        for (auto& field : fields_to_drop_) {
            delete_nested(val, field);
        }

        return record;
    }

    [[nodiscard]] std::string name() const override { return "Drop"; }

private:
    std::set<std::string> fields_to_drop_;
};

// ============================================================================
// ExtractTransform: Extract nested fields to top level
// ============================================================================

class ExtractTransform final : public Transform {
public:
    explicit ExtractTransform(const json& config) {
        auto logger = get_transforms_logger();

        // Parse "nested.path:new_name,nested.path2:new_name2"
        std::string extract_spec;
        auto it = config.find("extracts");
        if (it != config.end() && it->is_string()) {
            extract_spec = it->get<std::string>();
        } else {
            it = config.find("Extract.extracts");
            if (it != config.end() && it->is_string()) {
                extract_spec = it->get<std::string>();
            }
        }

        for (auto& spec : split_string(extract_spec, ',')) {
            auto colon = spec.find(':');
            std::string source_path;
            std::string target_name;

            if (colon != std::string::npos) {
                source_path = spec.substr(0, colon);
                target_name = spec.substr(colon + 1);

                source_path.erase(0, source_path.find_first_not_of(" \t"));
                source_path.erase(source_path.find_last_not_of(" \t") + 1);
                target_name.erase(0, target_name.find_first_not_of(" \t"));
                target_name.erase(target_name.find_last_not_of(" \t") + 1);
            } else {
                // Use last path component as target name
                source_path = spec;
                auto parts = split_string(spec, '.');
                target_name = parts.empty() ? spec : parts.back();
            }

            extractions_.emplace_back(source_path, target_name);
            logger->debug("ExtractTransform: '{}' -> '{}'",
                          source_path, target_name);
        }

        logger->info("ExtractTransform initialized with {} extractions",
                     extractions_.size());
    }

    [[nodiscard]] std::optional<json> apply(json record) override {
        if (!record.contains("value") || !record["value"].is_object())
            return record;

        auto& val = record["value"];

        for (auto& [source_path, target_name] : extractions_) {
            const json* source = get_nested(val, source_path);
            if (source) {
                set_nested(val, target_name, *source);
            }
        }

        return record;
    }

    [[nodiscard]] std::string name() const override { return "Extract"; }

private:
    std::vector<std::pair<std::string, std::string>> extractions_;
};

// ============================================================================
// MaskTransform: Mask sensitive field values
// ============================================================================

class MaskTransform final : public Transform {
public:
    explicit MaskTransform(const json& config) {
        auto logger = get_transforms_logger();

        std::string fields_spec;
        auto it = config.find("fields");
        if (it != config.end() && it->is_string()) {
            fields_spec = it->get<std::string>();
        } else {
            it = config.find("Mask.fields");
            if (it != config.end() && it->is_string()) {
                fields_spec = it->get<std::string>();
            }
        }

        for (auto& spec : split_string(fields_spec, ',')) {
            auto colon = spec.find(':');
            std::string field;
            std::string mask_type = "all";

            if (colon != std::string::npos) {
                field     = spec.substr(0, colon);
                mask_type = spec.substr(colon + 1);

                field.erase(0, field.find_first_not_of(" \t"));
                field.erase(field.find_last_not_of(" \t") + 1);
                mask_type.erase(0, mask_type.find_first_not_of(" \t"));
                mask_type.erase(mask_type.find_last_not_of(" \t") + 1);
            } else {
                field = spec;
                field.erase(0, field.find_first_not_of(" \t"));
                field.erase(field.find_last_not_of(" \t") + 1);
            }

            MaskStyle style = MaskStyle::all;
            if (mask_type == "last4") style = MaskStyle::last4;
            else if (mask_type == "first4") style = MaskStyle::first4;
            else if (mask_type == "email") style = MaskStyle::email;
            else if (mask_type == "credit_card") style = MaskStyle::credit_card;
            else if (mask_type == "ssn") style = MaskStyle::ssn;

            masks_.emplace_back(field, style);
            logger->debug("MaskTransform: mask '{}' with style '{}'",
                          field, mask_type);
        }

        logger->info("MaskTransform initialized with {} masks", masks_.size());
    }

    [[nodiscard]] std::optional<json> apply(json record) override {
        if (!record.contains("value") || !record["value"].is_object())
            return record;

        auto& val = record["value"];

        for (auto& [field, style] : masks_) {
            json* jfield = get_nested(val, field);
            if (!jfield || !jfield->is_string()) continue;

            std::string original = jfield->get<std::string>();
            std::string masked   = mask_string(original, style);
            *jfield = masked;
        }

        return record;
    }

    [[nodiscard]] std::string name() const override { return "Mask"; }

private:
    enum class MaskStyle : uint8_t {
        all,
        last4,
        first4,
        email,
        credit_card,
        ssn,
    };

    [[nodiscard]] static std::string mask_string(std::string_view input,
                                                   MaskStyle style) {
        switch (style) {
        case MaskStyle::all:
            return std::string(input.size(), '*');

        case MaskStyle::last4: {
            if (input.size() <= 4) return std::string(input.size(), '*');
            std::string result(input.size() - 4, '*');
            result += std::string(input.substr(input.size() - 4));
            return result;
        }

        case MaskStyle::first4: {
            if (input.size() <= 4) return std::string(input.size(), '*');
            std::string result(input.substr(0, 4));
            result += std::string(input.size() - 4, '*');
            return result;
        }

        case MaskStyle::email: {
            std::string s(input);
            auto at = s.find('@');
            if (at == std::string::npos) return std::string(s.size(), '*');
            // Show first char + *** + domain
            return s[0] + std::string("***") + s.substr(at);
        }

        case MaskStyle::credit_card: {
            // Show last 4 digits, mask rest
            if (input.size() <= 4) return std::string(input.size(), '*');
            std::string result(input.size() - 4, '*');
            result += std::string(input.substr(input.size() - 4));
            return result;
        }

        case MaskStyle::ssn: {
            // Show last 4 digits in XXX-XX-1234 format
            std::string digits;
            for (char c : input) {
                if (c >= '0' && c <= '9') digits += c;
            }
            if (digits.size() < 4) return std::string(input.size(), '*');
            return "***-**-" + digits.substr(digits.size() - 4);
        }
        }

        return std::string(input.size(), '*');
    }

    std::vector<std::pair<std::string, MaskStyle>> masks_;
};

// ============================================================================
// TimestampConverter: Convert timestamp formats
// ============================================================================

class TimestampConverterTransform final : public Transform {
public:
    explicit TimestampConverterTransform(const json& config) {
        auto logger = get_transforms_logger();

        target_field_ = config.value("field",
            config.value("TimestampConverter.field", ""));

        target_type_ = config.value("target.type",
            config.value("TimestampConverter.target.type", "string"));

        target_format_ = config.value("target.format",
            config.value("TimestampConverter.target.format", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"));

        // Normalize target type
        for (auto& c : target_type_) c = static_cast<char>(std::tolower(c));

        logger->info("TimestampConverter initialized: field='{}', target_type='{}'",
                     target_field_, target_type_);
    }

    [[nodiscard]] std::optional<json> apply(json record) override {
        if (target_field_.empty() || !record.contains("value"))
            return record;

        auto& val = record["value"];
        json* jfield = get_nested(val, target_field_);
        if (!jfield) return record;

        try {
            int64_t epoch_ms = 0;

            if (jfield->is_number()) {
                // Assume epoch in milliseconds
                double d = jfield->get<double>();
                if (d > 1e12) {
                    // Likely already milliseconds
                    epoch_ms = static_cast<int64_t>(d);
                } else if (d > 1e9) {
                    // Likely seconds
                    epoch_ms = static_cast<int64_t>(d * 1000);
                } else {
                    epoch_ms = static_cast<int64_t>(d * 1000);
                }
            } else if (jfield->is_string()) {
                std::string s = jfield->get<std::string>();
                // Try parsing ISO 8601 or other formats
                std::tm tm = {};
                std::istringstream iss(s);

                // Try ISO 8601: "2024-01-15T10:30:00Z"
                if (s.find('T') != std::string::npos) {
                    iss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
                    if (!iss.fail()) {
                        auto tp = std::chrono::system_clock::from_time_t(
                            std::mktime(&tm));
                        epoch_ms = std::chrono::duration_cast<
                            std::chrono::milliseconds>(
                            tp.time_since_epoch()).count();
                    }
                } else {
                    // Try "2024-01-15"
                    iss >> std::get_time(&tm, "%Y-%m-%d");
                    if (!iss.fail()) {
                        auto tp = std::chrono::system_clock::from_time_t(
                            std::mktime(&tm));
                        epoch_ms = std::chrono::duration_cast<
                            std::chrono::milliseconds>(
                            tp.time_since_epoch()).count();
                    }
                }
            }

            if (epoch_ms <= 0) return record;

            // Convert to target type
            if (target_type_ == "unix_ms" || target_type_ == "epoch_ms") {
                *jfield = epoch_ms;
            } else if (target_type_ == "unix" || target_type_ == "epoch") {
                *jfield = epoch_ms / 1000;
            } else if (target_type_ == "string" || target_type_ == "iso8601") {
                auto tp = std::chrono::system_clock::time_point(
                    std::chrono::milliseconds(epoch_ms));
                auto t  = std::chrono::system_clock::to_time_t(tp);
                auto tm = *std::gmtime(&t);

                std::ostringstream oss;
                oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S");

                // Add milliseconds
                int64_t ms = epoch_ms % 1000;
                oss << '.' << std::setw(3) << std::setfill('0') << ms << 'Z';

                *jfield = oss.str();
            }
        } catch (const std::exception& e) {
            auto logger = get_transforms_logger();
            logger->warn("TimestampConverter: failed to convert '{}': {}",
                         target_field_, e.what());
        }

        return record;
    }

    [[nodiscard]] std::string name() const override { return "TimestampConverter"; }

private:
    std::string target_field_;
    std::string target_type_;
    std::string target_format_;
};

// ============================================================================
// RegexRouter: Route records based on regex match on a field
// ============================================================================

class RegexRouterTransform final : public Transform {
public:
    explicit RegexRouterTransform(const json& config) {
        auto logger = get_transforms_logger();

        source_field_ = config.value("field",
            config.value("RegexRouter.field", "topic"));

        // Parse routing rules: "regex:target_topic,regex2:target_topic2"
        std::string rules_spec = config.value("routes",
            config.value("RegexRouter.routes", ""));

        for (auto& spec : split_string(rules_spec, ',')) {
            auto colon = spec.find(':');
            if (colon == std::string::npos) continue;

            std::string pattern = spec.substr(0, colon);
            std::string target  = spec.substr(colon + 1);

            pattern.erase(0, pattern.find_first_not_of(" \t"));
            pattern.erase(pattern.find_last_not_of(" \t") + 1);
            target.erase(0, target.find_first_not_of(" \t"));
            target.erase(target.find_last_not_of(" \t") + 1);

            try {
                routes_.emplace_back(
                    std::regex(pattern, std::regex::ECMAScript | std::regex::optimize),
                    target);
                logger->debug("RegexRouter: '{}' -> '{}'", pattern, target);
            } catch (const std::regex_error& e) {
                logger->error("RegexRouter: invalid regex '{}': {}", pattern, e.what());
            }
        }

        // Fallback topic
        fallback_topic_ = config.value("fallback",
            config.value("RegexRouter.fallback", ""));

        logger->info("RegexRouter initialized: {} routes, source_field='{}'",
                     routes_.size(), source_field_);
    }

    [[nodiscard]] std::optional<json> apply(json record) override {
        if (routes_.empty()) return record;

        // Get the value to match against
        std::string match_value;
        if (source_field_ == "topic") {
            match_value = record.value("topic", "");
        } else if (source_field_ == "key") {
            match_value = record.value("key", "");
        } else if (record.contains("value") && record["value"].is_object()) {
            json* jfield = get_nested(record["value"], source_field_);
            if (jfield && jfield->is_string()) {
                match_value = jfield->get<std::string>();
            }
        }

        // Try each route
        for (auto& [regex, target] : routes_) {
            if (std::regex_match(match_value, regex)) {
                auto logger = get_transforms_logger();
                logger->debug("RegexRouter: '{}' matched, routing to '{}'",
                              match_value, target);
                record["topic"] = target;
                return record;
            }
        }

        // Apply fallback if no match
        if (!fallback_topic_.empty()) {
            auto logger = get_transforms_logger();
            logger->debug("RegexRouter: no match for '{}', using fallback '{}'",
                          match_value, fallback_topic_);
            record["topic"] = fallback_topic_;
        }

        return record;
    }

    [[nodiscard]] std::string name() const override { return "RegexRouter"; }

private:
    std::string source_field_;
    std::vector<std::pair<std::regex, std::string>> routes_;
    std::string fallback_topic_;
};

// ============================================================================
// InsertFieldTransform: Add static or metadata fields
// ============================================================================

class InsertFieldTransform final : public Transform {
public:
    explicit InsertFieldTransform(const json& config) {
        auto logger = get_transforms_logger();

        // Parse "field.name:static_value,field2.name:static_value2"
        std::string fields_spec = config.value("fields",
            config.value("InsertField.fields", ""));

        for (auto& spec : split_string(fields_spec, ',')) {
            auto colon = spec.find(':');
            if (colon == std::string::npos) continue;

            std::string field = spec.substr(0, colon);
            std::string value = spec.substr(colon + 1);

            field.erase(0, field.find_first_not_of(" \t"));
            field.erase(field.find_last_not_of(" \t") + 1);
            value.erase(0, value.find_first_not_of(" \t"));

            // Check for metadata placeholders
            if (value == "${topic}" || value == "$topic") {
                static_fields_.emplace_back(field, StaticValue::topic);
            } else if (value == "${partition}" || value == "$partition") {
                static_fields_.emplace_back(field, StaticValue::partition);
            } else if (value == "${offset}" || value == "$offset") {
                static_fields_.emplace_back(field, StaticValue::offset);
            } else if (value == "${timestamp}" || value == "$timestamp") {
                static_fields_.emplace_back(field, StaticValue::timestamp);
            } else if (value == "${key}" || value == "$key") {
                static_fields_.emplace_back(field, StaticValue::key);
            } else {
                static_fields_.emplace_back(field, StaticValue::literal);
                literal_values_[field] = value;
            }

            logger->debug("InsertField: '{}' = '{}'", field, value);
        }

        logger->info("InsertField initialized with {} fields",
                     static_fields_.size());
    }

    [[nodiscard]] std::optional<json> apply(json record) override {
        auto& val = record["value"];
        if (!val.is_object()) val = json::object();

        for (auto& [field, kind] : static_fields_) {
            switch (kind) {
            case StaticValue::topic:
                set_nested(val, field, record.value("topic", ""));
                break;
            case StaticValue::partition:
                set_nested(val, field, record.value("partition", 0));
                break;
            case StaticValue::offset:
                set_nested(val, field, record.value("offset", kInvalidOffset));
                break;
            case StaticValue::timestamp:
                set_nested(val, field, record.value("timestamp", 0));
                break;
            case StaticValue::key:
                set_nested(val, field, record.value("key", ""));
                break;
            case StaticValue::literal: {
                auto lit = literal_values_.find(field);
                if (lit != literal_values_.end()) {
                    set_nested(val, field, lit->second);
                }
                break;
            }
            }
        }

        return record;
    }

    [[nodiscard]] std::string name() const override { return "InsertField"; }

private:
    enum class StaticValue : uint8_t {
        topic,
        partition,
        offset,
        timestamp,
        key,
        literal,
    };

    std::vector<std::pair<std::string, StaticValue>> static_fields_;
    std::unordered_map<std::string, std::string>    literal_values_;
};

// ============================================================================
// Transform registry and factory
// ============================================================================

namespace {

using TransformFactory = std::function<std::unique_ptr<Transform>(const json&)>;

class TransformRegistry {
public:
    static TransformRegistry& instance() {
        static TransformRegistry reg;
        return reg;
    }

    void register_transform(const std::string& name, TransformFactory factory) {
        std::lock_guard lock(mutex_);
        factories_[name] = std::move(factory);
    }

    [[nodiscard]] std::unique_ptr<Transform> create(
        const std::string& name, const json& config) const
    {
        std::lock_guard lock(mutex_);
        auto it = factories_.find(name);
        if (it == factories_.end()) return nullptr;
        return it->second(config);
    }

    [[nodiscard]] std::vector<std::string> registered_transforms() const {
        std::lock_guard lock(mutex_);
        std::vector<std::string> names;
        for (auto& [name, _] : factories_) names.push_back(name);
        return names;
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, TransformFactory> factories_;
};

// Auto-register all transforms at static init time
struct AutoRegister {
    AutoRegister() {
        auto& reg = TransformRegistry::instance();
        reg.register_transform("Cast", [](const json& cfg) {
            return std::make_unique<CastTransform>(cfg);
        });
        reg.register_transform("Drop", [](const json& cfg) {
            return std::make_unique<DropTransform>(cfg);
        });
        reg.register_transform("Extract", [](const json& cfg) {
            return std::make_unique<ExtractTransform>(cfg);
        });
        reg.register_transform("Mask", [](const json& cfg) {
            return std::make_unique<MaskTransform>(cfg);
        });
        reg.register_transform("TimestampConverter", [](const json& cfg) {
            return std::make_unique<TimestampConverterTransform>(cfg);
        });
        reg.register_transform("RegexRouter", [](const json& cfg) {
            return std::make_unique<RegexRouterTransform>(cfg);
        });
        reg.register_transform("InsertField", [](const json& cfg) {
            return std::make_unique<InsertFieldTransform>(cfg);
        });
    }
};

static AutoRegister s_auto_register;

} // anonymous namespace

// ============================================================================
// Public API: build transform chain from config
// ============================================================================

[[nodiscard]] std::vector<std::unique_ptr<Transform>> build_transform_chain(
    const json& config)
{
    std::vector<std::unique_ptr<Transform>> chain;
    auto logger = get_transforms_logger();

    std::string chain_spec = config.value("transforms", "");
    if (chain_spec.empty()) {
        logger->debug("No transforms configured");
        return chain;
    }

    auto& registry = TransformRegistry::instance();

    for (auto& name : split_string(chain_spec, ',')) {
        auto tx = registry.create(name, config);
        if (tx) {
            logger->info("Added transform '{}' to chain", name);
            chain.push_back(std::move(tx));
        } else {
            logger->warn("Unknown transform '{}', skipping", name);
        }
    }

    logger->info("Built transform chain with {} transforms", chain.size());
    return chain;
}

// ============================================================================
// Apply a transform chain to a single record
// ============================================================================

[[nodiscard]] std::optional<json> apply_transform_chain(
    std::vector<std::unique_ptr<Transform>>& chain,
    json record)
{
    for (auto& tx : chain) {
        auto result = tx->apply(std::move(record));
        if (!result.has_value()) {
            // Record was dropped
            auto logger = get_transforms_logger();
            logger->debug("Transform '{}' dropped the record", tx->name());
            return std::nullopt;
        }
        record = std::move(result.value());
    }

    return record;
}

// ============================================================================
// Apply a transform chain to a batch of records
// ============================================================================

[[nodiscard]] std::vector<json> apply_transform_chain_batch(
    std::vector<std::unique_ptr<Transform>>& chain,
    const std::vector<json>& records)
{
    std::vector<json> results;
    results.reserve(records.size());

    auto logger = get_transforms_logger();
    size_t dropped = 0;

    for (auto& rec : records) {
        auto result = apply_transform_chain(chain, rec);
        if (result.has_value()) {
            results.push_back(std::move(result.value()));
        } else {
            ++dropped;
        }
    }

    if (dropped > 0) {
        logger->debug("Transform chain dropped {} of {} records",
                      dropped, records.size());
    }

    return results;
}

} // namespace torrent::connectors
