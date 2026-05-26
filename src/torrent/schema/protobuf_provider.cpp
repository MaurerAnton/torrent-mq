/**
 * protobuf_provider.cpp — ProtobufSchemaProvider: Protobuf schema validation
 *
 * Provides Protobuf (proto3) schema lifecycle management:
 *   - Parse proto3 schema text (message/enum definitions)
 *   - Validate field numbers (1 to 536870911, excluding reserved range)
 *   - Validate field types (scalar, message, enum)
 *   - Check backward compatibility (no field number reuse, no type change)
 *   - Reserved field handling (check for reserved numbers/names)
 *   - Oneof, map, repeated field handling
 *
 * Compatibility rules (Protobuf wire format):
 *   BACKWARD: new schema can read serialized data from old schema.
 *     - No field number reuse with different types
 *     - No removal of required fields (proto3 has no required, but checks)
 *     - No type change on existing field numbers
 *
 *   FORWARD: old schema can read serialized data from new schema.
 *     - New fields must not reuse old field numbers
 *     - New enums: old readers use default (zero value) — allowed
 *     - New fields: old readers ignore unknown fields — allowed
 *
 *   FULL: both backward and forward.
 *
 * Note: This is a text-based proto3 parser; it does not invoke protoc.
 * It handles a subset sufficient for schema registry validation.
 */

#include "torrent/common/types.h"
#include "torrent/schema/schema_provider.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
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

// ============================================================================
// Anonymous namespace — internal types and helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_protobuf_logger() {
    static auto logger = []() {
        auto l = spdlog::get("protobuf_provider");
        if (!l) {
            l = spdlog::stdout_color_mt("protobuf_provider");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define PB_LOG_INFO(...)  get_protobuf_logger()->info(__VA_ARGS__)
#define PB_LOG_WARN(...)  get_protobuf_logger()->warn(__VA_ARGS__)
#define PB_LOG_DEBUG(...) get_protobuf_logger()->debug(__VA_ARGS__)

// --------------------------------------------------------------------------
// Protobuf wire types and field number constraints
// --------------------------------------------------------------------------

/// Valid protobuf field numbers: 1 to 536870911, excluding 19000-19999.
constexpr int32_t kMaxFieldNumber     = 536870911;
constexpr int32_t kReservedRangeStart = 19000;
constexpr int32_t kReservedRangeEnd   = 19999;

/// Protobuf scalar types.
const std::set<std::string> kProtoScalars = {
    "double", "float", "int32", "int64", "uint32", "uint64",
    "sint32", "sint64", "fixed32", "fixed64", "sfixed32", "sfixed64",
    "bool", "string", "bytes"
};

/// Protobuf wire type map (used for compatibility checks).
enum class WireType : int32_t {
    varint        = 0,
    fixed64       = 1,
    length_delimited = 2,
    fixed32       = 5,
};

[[nodiscard]] WireType wire_type_for(const std::string& scalar) {
    if (scalar == "int32" || scalar == "int64" || scalar == "uint32" ||
        scalar == "uint64" || scalar == "sint32" || scalar == "sint64" ||
        scalar == "bool") {
        return WireType::varint;
    }
    if (scalar == "fixed64" || scalar == "sfixed64" || scalar == "double") {
        return WireType::fixed64;
    }
    if (scalar == "fixed32" || scalar == "sfixed32" || scalar == "float") {
        return WireType::fixed32;
    }
    return WireType::length_delimited; // string, bytes, messages, repeated
}

// --------------------------------------------------------------------------
// Parsed protobuf field
// --------------------------------------------------------------------------

struct ProtoField {
    std::string name;
    int32_t     number    = 0;
    std::string type;       // scalar type or message/enum name
    bool        is_repeated = false;
    bool        is_map      = false;
    std::string map_key_type;
    std::string map_value_type;
    std::string oneof_group; // empty if not in a oneof
};

// --------------------------------------------------------------------------
// Parsed protobuf enum value
// --------------------------------------------------------------------------

struct ProtoEnumValue {
    std::string name;
    int32_t     number = 0;
};

// --------------------------------------------------------------------------
// Parsed protobuf message/enum
// --------------------------------------------------------------------------

struct ProtoMessage {
    std::string name;
    std::vector<ProtoField> fields;
    std::vector<ProtoMessage> nested_messages;
    std::vector<ProtoEnumValue> nested_enums;
    std::set<int32_t> reserved_numbers;
    std::set<std::string> reserved_names;
};

struct ProtoEnum {
    std::string name;
    std::vector<ProtoEnumValue> values;
    std::set<int32_t> reserved_numbers;
    std::set<std::string> reserved_names;
};

// --------------------------------------------------------------------------
// Proto3 parser (recursive descent)
// --------------------------------------------------------------------------

class Proto3Parser {
public:
    explicit Proto3Parser(std::string_view source)
        : src_(source), pos_(0) {}

    /// Parse a complete .proto file, returning top-level messages.
    [[nodiscard]] std::vector<ProtoMessage> parse() {
        std::vector<ProtoMessage> messages;
        while (pos_ < src_.size()) {
            skip_whitespace_and_comments();
            if (pos_ >= src_.size()) break;

            if (match_keyword("syntax") || match_keyword("package") ||
                match_keyword("import") || match_keyword("option")) {
                skip_to_semicolon();
                continue;
            }

            if (match_keyword("message")) {
                messages.push_back(parse_message());
            } else if (match_keyword("enum")) {
                // Top-level enums stored separately
                parse_enum();
            } else {
                pos_++; // Skip unrecognized
            }
        }
        return messages;
    }

private:
    std::string_view src_;
    size_t pos_;

    void skip_whitespace_and_comments() {
        while (pos_ < src_.size()) {
            char c = src_[pos_];
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                pos_++;
            } else if (c == '/' && pos_ + 1 < src_.size()) {
                if (src_[pos_ + 1] == '/') {
                    // Line comment
                    while (pos_ < src_.size() && src_[pos_] != '\n') pos_++;
                } else if (src_[pos_ + 1] == '*') {
                    // Block comment
                    pos_ += 2;
                    while (pos_ + 1 < src_.size() &&
                           !(src_[pos_] == '*' && src_[pos_ + 1] == '/')) {
                        pos_++;
                    }
                    if (pos_ + 1 < src_.size()) pos_ += 2;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }

    bool match_keyword(std::string_view kw) {
        size_t saved = pos_;
        skip_whitespace_and_comments();
        if (src_.substr(pos_, kw.size()) == kw) {
            size_t after = pos_ + kw.size();
            if (after >= src_.size() || !std::isalnum(static_cast<unsigned char>(src_[after]))) {
                pos_ = after;
                return true;
            }
        }
        pos_ = saved;
        return false;
    }

    void skip_to_semicolon() {
        while (pos_ < src_.size() && src_[pos_] != ';') pos_++;
        if (pos_ < src_.size()) pos_++; // skip ;
    }

    std::string parse_identifier() {
        skip_whitespace_and_comments();
        std::string id;
        while (pos_ < src_.size() &&
               (std::isalnum(static_cast<unsigned char>(src_[pos_])) ||
                src_[pos_] == '_' || src_[pos_] == '.')) {
            id += src_[pos_++];
        }
        return id;
    }

    int32_t parse_integer() {
        skip_whitespace_and_comments();
        int32_t val = 0;
        bool neg = false;
        if (pos_ < src_.size() && src_[pos_] == '-') {
            neg = true;
            pos_++;
        }
        while (pos_ < src_.size() && std::isdigit(static_cast<unsigned char>(src_[pos_]))) {
            val = val * 10 + (src_[pos_++] - '0');
        }
        return neg ? -val : val;
    }

    ProtoMessage parse_message() {
        ProtoMessage msg;
        msg.name = parse_identifier();
        skip_whitespace_and_comments();

        if (pos_ < src_.size() && src_[pos_] == '{') {
            pos_++;
            while (pos_ < src_.size() && src_[pos_] != '}') {
                skip_whitespace_and_comments();
                if (pos_ >= src_.size() || src_[pos_] == '}') break;

                if (match_keyword("message")) {
                    msg.nested_messages.push_back(parse_message());
                } else if (match_keyword("enum")) {
                    auto e = parse_enum();
                    msg.nested_enums = e.values;
                } else if (match_keyword("reserved")) {
                    parse_reserved(msg.reserved_numbers, msg.reserved_names);
                } else if (match_keyword("oneof")) {
                    parse_oneof(msg);
                } else if (match_keyword("option")) {
                    skip_to_semicolon();
                } else if (match_keyword("map<")) {
                    msg.fields.push_back(parse_map_field());
                } else {
                    // Try to parse as a regular field
                    auto field = parse_field();
                    if (!field.name.empty()) {
                        msg.fields.push_back(std::move(field));
                    } else {
                        pos_++; // Skip unrecognized
                    }
                }
            }
            if (pos_ < src_.size() && src_[pos_] == '}') pos_++;
        }
        return msg;
    }

    ProtoEnum parse_enum() {
        ProtoEnum e;
        e.name = parse_identifier();
        skip_whitespace_and_comments();

        if (pos_ < src_.size() && src_[pos_] == '{') {
            pos_++;
            while (pos_ < src_.size() && src_[pos_] != '}') {
                skip_whitespace_and_comments();
                if (pos_ >= src_.size() || src_[pos_] == '}') break;

                if (match_keyword("reserved")) {
                    parse_reserved(e.reserved_numbers, e.reserved_names);
                } else if (match_keyword("option")) {
                    skip_to_semicolon();
                } else {
                    ProtoEnumValue val;
                    val.name = parse_identifier();
                    skip_whitespace_and_comments();
                    if (pos_ < src_.size() && src_[pos_] == '=') {
                        pos_++;
                        val.number = parse_integer();
                    }
                    skip_to_semicolon();
                    if (!val.name.empty()) {
                        e.values.push_back(val);
                    }
                }
            }
            if (pos_ < src_.size() && src_[pos_] == '}') pos_++;
        }
        return e;
    }

    void parse_reserved(std::set<int32_t>& numbers, std::set<std::string>& names) {
        skip_whitespace_and_comments();
        while (pos_ < src_.size() && src_[pos_] != ';') {
            skip_whitespace_and_comments();
            if (std::isdigit(static_cast<unsigned char>(src_[pos_]))) {
                int32_t num = parse_integer();
                numbers.insert(num);
                // Check for range (e.g., 10 to 20)
                skip_whitespace_and_comments();
                if (pos_ + 1 < src_.size() &&
                    src_[pos_] == 't' && src_[pos_ + 1] == 'o') {
                    pos_ += 2;
                    int32_t end = parse_integer();
                    for (int32_t i = num + 1; i <= end; i++) {
                        numbers.insert(i);
                    }
                }
            } else if (src_[pos_] == '"' || src_[pos_] == '\'') {
                char quote = src_[pos_++];
                std::string name;
                while (pos_ < src_.size() && src_[pos_] != quote) {
                    name += src_[pos_++];
                }
                if (pos_ < src_.size()) pos_++; // skip closing quote
                names.insert(name);
            } else {
                pos_++;
            }
            skip_whitespace_and_comments();
            if (pos_ < src_.size() && src_[pos_] == ',') pos_++;
        }
        skip_to_semicolon();
    }

    void parse_oneof(ProtoMessage& msg) {
        std::string oneof_name = parse_identifier();
        skip_whitespace_and_comments();
        if (pos_ < src_.size() && src_[pos_] == '{') {
            pos_++;
            while (pos_ < src_.size() && src_[pos_] != '}') {
                skip_whitespace_and_comments();
                if (pos_ >= src_.size() || src_[pos_] == '}') break;
                auto field = parse_field();
                if (!field.name.empty()) {
                    field.oneof_group = oneof_name;
                    msg.fields.push_back(std::move(field));
                } else {
                    pos_++;
                }
            }
            if (pos_ < src_.size() && src_[pos_] == '}') pos_++;
        }
    }

    ProtoField parse_field() {
        ProtoField f;
        f.type = parse_identifier();

        if (f.type.empty() || f.type == "}" || f.type == "reserved" ||
            f.type == "option") {
            return f;
        }

        if (f.type == "repeated") {
            f.is_repeated = true;
            f.type = parse_identifier();
        } else if (f.type == "optional") {
            f.type = parse_identifier();
        }

        f.name = parse_identifier();
        skip_whitespace_and_comments();

        if (pos_ < src_.size() && src_[pos_] == '=') {
            pos_++;
            f.number = parse_integer();
        }

        skip_to_semicolon();
        return f;
    }

    ProtoField parse_map_field() {
        ProtoField f;
        f.is_map = true;
        f.map_key_type = parse_identifier(); // key type
        skip_whitespace_and_comments();
        if (pos_ < src_.size() && src_[pos_] == ',') pos_++;
        f.map_value_type = parse_identifier(); // value type
        skip_whitespace_and_comments();
        if (pos_ < src_.size() && src_[pos_] == '>') pos_++;
        f.name = parse_identifier();
        skip_whitespace_and_comments();
        if (pos_ < src_.size() && src_[pos_] == '=') {
            pos_++;
            f.number = parse_integer();
        }
        skip_to_semicolon();
        return f;
    }
};

// --------------------------------------------------------------------------
// Validation
// --------------------------------------------------------------------------

[[nodiscard]] bool is_valid_field_number(int32_t n) {
    if (n < 1 || n > kMaxFieldNumber) return false;
    if (n >= kReservedRangeStart && n <= kReservedRangeEnd) return false;
    return true;
}

[[nodiscard]] bool validate_field(const ProtoField& f, std::string& error) {
    if (!is_valid_field_number(f.number)) {
        error = "Field '" + f.name + "': invalid field number " +
                std::to_string(f.number);
        return false;
    }
    if (f.type.empty() && !f.is_map) {
        error = "Field '" + f.name + "': missing type";
        return false;
    }
    return true;
}

[[nodiscard]] bool validate_message(const ProtoMessage& msg,
                                      std::string& error) {
    std::map<int32_t, std::string> field_numbers;
    std::set<std::string> field_names;

    for (const auto& f : msg.fields) {
        if (!validate_field(f, error)) return false;

        // Check field number uniqueness (except across oneof groups)
        auto it = field_numbers.find(f.number);
        if (it != field_numbers.end() && f.oneof_group.empty()) {
            error = "Message '" + msg.name + "': duplicate field number " +
                    std::to_string(f.number) + " (" + f.name + " and " +
                    it->second + ")";
            return false;
        }
        field_numbers[f.number] = f.name;

        // Check field name uniqueness
        if (field_names.count(f.name)) {
            error = "Message '" + msg.name + "': duplicate field name '" +
                    f.name + "'";
            return false;
        }
        field_names.insert(f.name);

        // Check reserved
        if (msg.reserved_numbers.count(f.number)) {
            error = "Message '" + msg.name + "': field " + f.name +
                    " uses reserved number " + std::to_string(f.number);
            return false;
        }
        if (msg.reserved_names.count(f.name)) {
            error = "Message '" + msg.name + "': field name '" + f.name +
                    "' is reserved";
            return false;
        }
    }

    // Validate nested messages
    for (const auto& nm : msg.nested_messages) {
        if (!validate_message(nm, error)) return false;
    }

    return true;
}

// --------------------------------------------------------------------------
// Build field index from parsed protobuf
// --------------------------------------------------------------------------

/// Flatten all messages into a name→message map.
void build_message_index(const std::vector<ProtoMessage>& messages,
                          std::map<std::string, ProtoMessage>& index) {
    for (const auto& msg : messages) {
        index[msg.name] = msg;
        build_message_index(msg.nested_messages, index);
    }
}

// --------------------------------------------------------------------------
// Compatibility checks
// --------------------------------------------------------------------------

/// Check if two wire types are compatible.
[[nodiscard]] bool is_wire_compatible(const std::string& old_type,
                                       const std::string& new_type) {
    if (old_type == new_type) return true;
    WireType owt = kProtoScalars.count(old_type)
                       ? wire_type_for(old_type)
                       : WireType::length_delimited;
    WireType nwt = kProtoScalars.count(new_type)
                       ? wire_type_for(new_type)
                       : WireType::length_delimited;
    if (owt != nwt) return false;
    // Varint types: int32→int64, uint32→uint64 are safe; signed/unsigned cross is not
    if (owt == WireType::varint) {
        // Allow widening within same signedness family
        bool old_unsigned = (old_type == "uint32" || old_type == "uint64" || old_type == "bool");
        bool new_unsigned = (new_type == "uint32" || new_type == "uint64" || new_type == "bool");
        if (old_unsigned != new_unsigned) return false;
        if (old_type == "bool" && new_type != "bool") return false;
        return true;
    }
    // Fixed types: width must match
    if (owt == WireType::fixed32 || owt == WireType::fixed64) {
        return owt == nwt; // Same wire type width
    }
    // Length-delimited: string/bytes/messages — wire compatible
    return true;
}

} // anonymous namespace

// ============================================================================
// ProtobufSchemaProvider — public API
// ============================================================================

/**
 * Parse and validate a proto3 schema.
 */
bool ProtobufSchemaProvider::validate(const std::string& schema_text,
                                       std::string& error_msg) {
    if (schema_text.find("message ") == std::string::npos &&
        schema_text.find("syntax ") == std::string::npos &&
        schema_text.find("enum ") == std::string::npos) {
        error_msg = "Protobuf schema must contain at least one message, enum, or syntax declaration";
        return false;
    }

    try {
        Proto3Parser parser(schema_text);
        auto messages = parser.parse();

        if (messages.empty()) {
            error_msg = "No messages found in protobuf schema";
            return false;
        }

        for (const auto& msg : messages) {
            if (!validate_message(msg, error_msg)) {
                return false;
            }
        }
        return true;
    } catch (const std::exception& e) {
        error_msg = std::string("Protobuf parse error: ") + e.what();
        return false;
    }
}

/**
 * Check backward compatibility between two proto3 schemas.
 */
bool ProtobufSchemaProvider::check_backward(const std::string& old_schema_text,
                                              const std::string& new_schema_text,
                                              std::string& error_msg) {
    try {
        Proto3Parser old_parser(old_schema_text);
        Proto3Parser new_parser(new_schema_text);

        auto old_msgs = old_parser.parse();
        auto new_msgs = new_parser.parse();

        std::map<std::string, ProtoMessage> old_index, new_index;
        build_message_index(old_msgs, old_index);
        build_message_index(new_msgs, new_index);

        for (const auto& [name, old_msg] : old_index) {
            auto it = new_index.find(name);
            if (it == new_index.end()) {
                error_msg = "Message '" + name + "' removed (BACKWARD incompatible)";
                return false;
            }
            const auto& new_msg = it->second;

            // Build field index by number
            std::map<int32_t, ProtoField> old_by_num, new_by_num;
            for (const auto& f : old_msg.fields) old_by_num[f.number] = f;
            for (const auto& f : new_msg.fields) new_by_num[f.number] = f;

            for (const auto& [num, old_f] : old_by_num) {
                auto nf_it = new_by_num.find(num);
                if (nf_it == new_by_num.end()) {
                    if (!new_msg.reserved_numbers.count(num)) {
                        error_msg = "Message '" + name +
                                    "': field " + std::to_string(num) +
                                    " (" + old_f.name +
                                    ") removed without reservation";
                        return false;
                    }
                    continue; // Reserved — OK
                }
                const auto& new_f = nf_it->second;
                if (!is_wire_compatible(old_f.type, new_f.type)) {
                    error_msg = "Message '" + name +
                                "': field " + std::to_string(num) +
                                " type changed from " + old_f.type +
                                " to " + new_f.type;
                    return false;
                }
            }
        }
        return true;
    } catch (const std::exception& e) {
        error_msg = std::string("Compatibility check error: ") + e.what();
        return false;
    }
}

/**
 * Check forward compatibility between two proto3 schemas.
 */
bool ProtobufSchemaProvider::check_forward(const std::string& old_schema_text,
                                             const std::string& new_schema_text,
                                             std::string& error_msg) {
    try {
        Proto3Parser old_parser(old_schema_text);
        Proto3Parser new_parser(new_schema_text);

        auto old_msgs = old_parser.parse();
        auto new_msgs = new_parser.parse();

        std::map<std::string, ProtoMessage> old_index, new_index;
        build_message_index(old_msgs, old_index);
        build_message_index(new_msgs, new_index);

        for (const auto& [name, new_msg] : new_index) {
            auto it = old_index.find(name);
            if (it == old_index.end()) {
                // New message — old readers don't know it but it's OK
                // as long as the message is referenced somewhere
                continue;
            }
            const auto& old_msg = it->second;

            std::map<int32_t, ProtoField> old_by_num, new_by_num;
            for (const auto& f : old_msg.fields) old_by_num[f.number] = f;
            for (const auto& f : new_msg.fields) new_by_num[f.number] = f;

            for (const auto& [num, new_f] : new_by_num) {
                auto of_it = old_by_num.find(num);
                if (of_it == old_by_num.end()) {
                    // New field — old readers ignore unknown fields, OK
                    continue;
                }
                if (!is_wire_compatible(of_it->second.type, new_f.type)) {
                    error_msg = "Message '" + name +
                                "': field " + std::to_string(num) +
                                " type changed (FORWARD incompatible)";
                    return false;
                }
            }
        }
        return true;
    } catch (const std::exception& e) {
        error_msg = std::string("Compatibility check error: ") + e.what();
        return false;
    }
}

/**
 * Check full compatibility.
 */
bool ProtobufSchemaProvider::check_full(const std::string& old_schema_text,
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
 * Normalize a protobuf schema (strip comments, normalize whitespace).
 */
std::string ProtobufSchemaProvider::normalize(const std::string& schema_text) {
    // Simple normalization: remove all comments and compress whitespace
    std::string result;
    result.reserve(schema_text.size());
    bool in_line_comment = false;
    bool in_block_comment = false;

    for (size_t i = 0; i < schema_text.size(); i++) {
        char c = schema_text[i];
        if (in_line_comment) {
            if (c == '\n') in_line_comment = false;
            continue;
        }
        if (in_block_comment) {
            if (c == '*' && i + 1 < schema_text.size() && schema_text[i + 1] == '/') {
                in_block_comment = false;
                i++;
            }
            continue;
        }
        if (c == '/' && i + 1 < schema_text.size()) {
            if (schema_text[i + 1] == '/') {
                in_line_comment = true;
                i++;
                continue;
            }
            if (schema_text[i + 1] == '*') {
                in_block_comment = true;
                i++;
                continue;
            }
        }
        // Compress whitespace
        if (std::isspace(static_cast<unsigned char>(c))) {
            if (!result.empty() && result.back() != ' ') {
                result += ' ';
            }
            continue;
        }
        result += c;
    }

    // Trim trailing whitespace
    while (!result.empty() && result.back() == ' ') result.pop_back();
    return result;
}

/**
 * Generate a fingerprint for a protobuf schema.
 */
std::string ProtobufSchemaProvider::fingerprint(const std::string& schema_text) {
    std::string norm = normalize(schema_text);
    std::hash<std::string> hasher;
    size_t h = hasher(norm);
    std::ostringstream oss;
    oss << std::hex << h;
    return oss.str();
}

} // namespace torrent::schema
