#include "torrent/schema/schema_registry.h"
#include "torrent/common/types.h"
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <cstring>
#include <regex>
#include <unordered_map>
#include <mutex>

namespace torrent::schema {

namespace {

// ============================================================================
// Avro binary encoding/decoding (simplified — production would use libavro)
// ============================================================================

class AvroSerde {
public:
    // Serialize a JSON value to Avro binary format
    static std::vector<uint8_t> serialize(const nlohmann::json& schema,
                                           const nlohmann::json& value) {
        std::vector<uint8_t> result;
        serialize_value(schema, value, result);
        return result;
    }

    // Deserialize Avro binary to JSON
    static nlohmann::json deserialize(const nlohmann::json& schema,
                                       const uint8_t* data, size_t size) {
        size_t offset = 0;
        return deserialize_value(schema, data, size, offset);
    }

private:
    static void serialize_value(const nlohmann::json& schema,
                                 const nlohmann::json& value,
                                 std::vector<uint8_t>& out) {
        std::string type = schema.value("type", "string");

        if (type == "null") {
            return; // null encodes as zero bytes
        }
        if (type == "boolean") {
            out.push_back(value.get<bool>() ? 1 : 0);
        } else if (type == "int" || type == "long") {
            int64_t v = value.get<int64_t>();
            encode_zigzag_varint(v, out);
        } else if (type == "float") {
            float v = value.get<float>();
            encode_float(v, out);
        } else if (type == "double") {
            double v = value.get<double>();
            encode_double(v, out);
        } else if (type == "string" || type == "bytes") {
            std::string s = value.get<std::string>();
            encode_bytes(reinterpret_cast<const uint8_t*>(s.data()), s.size(), out);
        } else if (type == "record") {
            for (const auto& field : schema["fields"]) {
                serialize_value(field["type"], value[field["name"]], out);
            }
        } else if (type == "array") {
            encode_long(value.size(), out);
            for (const auto& item : value) {
                serialize_value(schema["items"], item, out);
            }
            out.push_back(0); // End marker
        } else if (type == "map") {
            std::vector<std::pair<std::string, nlohmann::json>> entries;
            for (auto it = value.begin(); it != value.end(); ++it) {
                entries.emplace_back(it.key(), it.value());
            }
            encode_long(entries.size(), out);
            for (const auto& [k, v] : entries) {
                encode_bytes(reinterpret_cast<const uint8_t*>(k.data()), k.size(), out);
                serialize_value(schema["values"], v, out);
            }
            out.push_back(0); // End marker
        } else if (type == "enum") {
            int idx = 0;
            for (const auto& sym : schema["symbols"]) {
                if (sym == value) { break; }
                idx++;
            }
            encode_long(idx, out);
        } else if (type == "fixed") {
            std::string s = value.get<std::string>();
            out.insert(out.end(), s.begin(), s.end());
        }
    }

    static nlohmann::json deserialize_value(const nlohmann::json& schema,
                                              const uint8_t* data, size_t size,
                                              size_t& offset) {
        std::string type = schema.value("type", "string");
        if (offset >= size) return nlohmann::json();

        if (type == "null") return nullptr;
        if (type == "boolean") {
            return data[offset++] != 0;
        }
        if (type == "int") {
            return static_cast<int32_t>(decode_zigzag_varint(data, size, offset));
        }
        if (type == "long") {
            return decode_zigzag_varint(data, size, offset);
        }
        if (type == "float") {
            float v = decode_float(data, offset);
            offset += 4;
            return v;
        }
        if (type == "double") {
            double v = decode_double(data, offset);
            offset += 8;
            return v;
        }
        if (type == "string") {
            size_t len = decode_long(data, size, offset);
            std::string s(reinterpret_cast<const char*>(data + offset), len);
            offset += len;
            return s;
        }
        if (type == "bytes") {
            size_t len = decode_long(data, size, offset);
            std::vector<uint8_t> bytes(data + offset, data + offset + len);
            offset += len;
            return bytes;
        }
        if (type == "record") {
            nlohmann::json result;
            for (const auto& field : schema["fields"]) {
                result[field["name"]] = deserialize_value(
                    field["type"], data, size, offset);
            }
            return result;
        }

        // Unknown type — return null
        return nullptr;
    }

    static void encode_zigzag_varint(int64_t val, std::vector<uint8_t>& out) {
        uint64_t zigzag = (static_cast<uint64_t>(val) << 1) ^
                          static_cast<uint64_t>(val >> 63);
        while (zigzag >= 0x80) {
            out.push_back(static_cast<uint8_t>(zigzag | 0x80));
            zigzag >>= 7;
        }
        out.push_back(static_cast<uint8_t>(zigzag));
    }

    static int64_t decode_zigzag_varint(const uint8_t* data, size_t size,
                                         size_t& offset) {
        uint64_t result = 0;
        int shift = 0;
        while (offset < size) {
            uint8_t byte = data[offset++];
            result |= static_cast<uint64_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0) break;
            shift += 7;
        }
        return static_cast<int64_t>((result >> 1) ^ -(result & 1));
    }

    static size_t decode_long(const uint8_t* data, size_t size, size_t& offset) {
        size_t result = 0;
        int shift = 0;
        while (offset < size) {
            uint8_t byte = data[offset++];
            result |= static_cast<size_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0) return result;
            shift += 7;
        }
        return result;
    }

    static void encode_long(size_t val, std::vector<uint8_t>& out) {
        while (val >= 0x80) {
            out.push_back(static_cast<uint8_t>(val | 0x80));
            val >>= 7;
        }
        out.push_back(static_cast<uint8_t>(val));
    }

    static void encode_bytes(const uint8_t* data, size_t len,
                              std::vector<uint8_t>& out) {
        encode_long(len, out);
        out.insert(out.end(), data, data + len);
    }

    static void encode_float(float val, std::vector<uint8_t>& out) {
        uint32_t bits = 0;
        std::memcpy(&bits, &val, sizeof(bits));
        out.push_back(static_cast<uint8_t>(bits));
        out.push_back(static_cast<uint8_t>(bits >> 8));
        out.push_back(static_cast<uint8_t>(bits >> 16));
        out.push_back(static_cast<uint8_t>(bits >> 24));
    }

    static float decode_float(const uint8_t* data, size_t offset) {
        uint32_t bits = static_cast<uint32_t>(data[offset]) |
                        (static_cast<uint32_t>(data[offset + 1]) << 8) |
                        (static_cast<uint32_t>(data[offset + 2]) << 16) |
                        (static_cast<uint32_t>(data[offset + 3]) << 24);
        float val = 0.0f;
        std::memcpy(&val, &bits, sizeof(val));
        return val;
    }

    static void encode_double(double val, std::vector<uint8_t>& out) {
        uint64_t bits = 0;
        std::memcpy(&bits, &val, sizeof(bits));
        for (int i = 0; i < 8; i++) {
            out.push_back(static_cast<uint8_t>(bits >> (i * 8)));
        }
    }

    static double decode_double(const uint8_t* data, size_t offset) {
        uint64_t bits = 0;
        for (int i = 0; i < 8; i++) {
            bits |= static_cast<uint64_t>(data[offset + i]) << (i * 8);
        }
        double val = 0.0;
        std::memcpy(&val, &bits, sizeof(val));
        return val;
    }
};

// ============================================================================
// Protobuf serialization (stub)
// ============================================================================

class ProtobufSerde {
public:
    static std::vector<uint8_t> serialize(const nlohmann::json& /*schema*/,
                                           const nlohmann::json& value) {
        // Production would use protoc-generated code or dynamic reflection
        // For now, store as JSON with a type prefix
        std::string json = value.dump();
        std::vector<uint8_t> result;
        result.reserve(json.size() + 4);

        // 4-byte magic header
        result.push_back('P');
        result.push_back('B');
        result.push_back('U');
        result.push_back('F');

        result.insert(result.end(), json.begin(), json.end());
        return result;
    }

    static nlohmann::json deserialize(const nlohmann::json& /*schema*/,
                                       const uint8_t* data, size_t size) {
        if (size < 4 || data[0] != 'P' || data[1] != 'B' ||
            data[2] != 'U' || data[3] != 'F') {
            return nullptr;
        }
        std::string json(reinterpret_cast<const char*>(data + 4), size - 4);
        return nlohmann::json::parse(json, nullptr, false);
    }
};

// ============================================================================
// JSON Schema validation
// ============================================================================

class JsonSchemaValidator {
public:
    static bool validate(const nlohmann::json& schema,
                          const nlohmann::json& value,
                          std::string& error_msg) {
        return validate_node(schema, value, "$", error_msg);
    }

private:
    static bool validate_node(const nlohmann::json& schema,
                               const nlohmann::json& value,
                               const std::string& path,
                               std::string& error_msg) {
        // Type validation
        if (schema.contains("type")) {
            std::string type = schema["type"];
            if (type == "string" && !value.is_string()) {
                error_msg = path + ": expected string, got " +
                            std::string(value.type_name());
                return false;
            }
            if (type == "number" && !value.is_number()) {
                error_msg = path + ": expected number";
                return false;
            }
            if (type == "integer" && !value.is_number_integer()) {
                error_msg = path + ": expected integer";
                return false;
            }
            if (type == "boolean" && !value.is_boolean()) {
                error_msg = path + ": expected boolean";
                return false;
            }
            if (type == "array" && !value.is_array()) {
                error_msg = path + ": expected array";
                return false;
            }
            if (type == "object" && !value.is_object()) {
                error_msg = path + ": expected object";
                return false;
            }
            if (type == "null" && !value.is_null()) {
                error_msg = path + ": expected null";
                return false;
            }
        }

        // String constraints
        if (schema.contains("minLength") && value.is_string()) {
            if (value.get<std::string>().size() < schema["minLength"].get<size_t>()) {
                error_msg = path + ": string too short";
                return false;
            }
        }
        if (schema.contains("maxLength") && value.is_string()) {
            if (value.get<std::string>().size() > schema["maxLength"].get<size_t>()) {
                error_msg = path + ": string too long";
                return false;
            }
        }
        if (schema.contains("pattern") && value.is_string()) {
            std::regex pattern(schema["pattern"].get<std::string>());
            if (!std::regex_match(value.get<std::string>(), pattern)) {
                error_msg = path + ": pattern mismatch";
                return false;
            }
        }

        // Numeric constraints
        if (schema.contains("minimum") && value.is_number()) {
            if (value.get<double>() < schema["minimum"].get<double>()) {
                error_msg = path + ": value below minimum";
                return false;
            }
        }
        if (schema.contains("maximum") && value.is_number()) {
            if (value.get<double>() > schema["maximum"].get<double>()) {
                error_msg = path + ": value above maximum";
                return false;
            }
        }

        // Enum validation
        if (schema.contains("enum")) {
            bool found = false;
            for (const auto& ev : schema["enum"]) {
                if (ev == value) { found = true; break; }
            }
            if (!found) {
                error_msg = path + ": value not in enum";
                return false;
            }
        }

        // Object properties
        if (schema.contains("properties") && value.is_object()) {
            for (auto it = schema["properties"].begin();
                 it != schema["properties"].end(); ++it) {
                std::string key = it.key();
                if (value.contains(key)) {
                    if (!validate_node(it.value(), value[key],
                                       path + "." + key, error_msg)) {
                        return false;
                    }
                } else if (schema.contains("required") &&
                           std::find(schema["required"].begin(),
                                     schema["required"].end(), key) !=
                               schema["required"].end()) {
                    error_msg = path + ": missing required property '" + key + "'";
                    return false;
                }
            }
        }

        // Array items
        if (schema.contains("items") && value.is_array()) {
            int idx = 0;
            for (const auto& item : value) {
                if (!validate_node(schema["items"], item,
                                   path + "[" + std::to_string(idx) + "]",
                                   error_msg)) {
                    return false;
                }
                idx++;
            }
        }

        return true;
    }
};

} // anonymous namespace

// ============================================================================
// Public API
// ============================================================================

std::vector<uint8_t> serialize_avro(const std::string& schema_json,
                                     const nlohmann::json& value) {
    try {
        auto schema = nlohmann::json::parse(schema_json);
        return AvroSerde::serialize(schema, value);
    } catch (const std::exception& e) {
        spdlog::error("Avro serialization failed: {}", e.what());
        return {};
    }
}

nlohmann::json deserialize_avro(const std::string& schema_json,
                                  const uint8_t* data, size_t size) {
    try {
        auto schema = nlohmann::json::parse(schema_json);
        return AvroSerde::deserialize(schema, data, size);
    } catch (const std::exception& e) {
        spdlog::error("Avro deserialization failed: {}", e.what());
        return nullptr;
    }
}

std::vector<uint8_t> serialize_protobuf(const std::string& /*schema_json*/,
                                          const nlohmann::json& value) {
    return ProtobufSerde::serialize({}, value);
}

nlohmann::json deserialize_protobuf(const std::string& /*schema_json*/,
                                      const uint8_t* data, size_t size) {
    return ProtobufSerde::deserialize({}, data, size);
}

bool validate_json_schema(const std::string& schema_json,
                           const nlohmann::json& value,
                           std::string& error_msg) {
    try {
        auto schema = nlohmann::json::parse(schema_json);
        return JsonSchemaValidator::validate(schema, value, error_msg);
    } catch (const std::exception& e) {
        error_msg = std::string("Schema parse error: ") + e.what();
        return false;
    }
}

std::string detect_format(const std::string& schema_json) {
    try {
        auto schema = nlohmann::json::parse(schema_json);
        if (schema.contains("type") &&
            (schema["type"] == "record" || schema["type"] == "enum" ||
             schema["type"] == "fixed" || schema["type"] == "array" ||
             schema["type"] == "map")) {
            return "AVRO";
        }
        if (schema.contains("properties") || schema.contains("$schema")) {
            return "JSON";
        }
    } catch (...) {}
    return "UNKNOWN";
}

} // namespace torrent::schema
