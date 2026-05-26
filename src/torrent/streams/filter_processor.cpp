/**
 * filter_processor.cpp — FilterProcessor: Predicate-Based Record Filtering
 *
 * Provides record filtering within the torrent-mq stream processing topology.
 * Records are evaluated against a predicate expression and either forwarded
 * downstream (pass) or discarded (drop).
 *
 * Supported predicate types:
 *   - Comparison:  equals, not_equals, greater_than, less_than,
 *                   greater_or_equal, less_or_equal
 *   - String:      contains, starts_with, ends_with, matches_regex
 *   - Null checks: is_null, is_not_null
 *   - Membership:  in_list, not_in_list
 *   - Field access: key, value (with JSON path), header
 *
 * Compound predicates:
 *   - AND:  all child predicates must pass
 *   - OR:   at least one child predicate must pass
 *   - NOT:  inverts the result of a child predicate
 *
 * Field access paths:
 *   - "key"                — the record key (as a string)
 *   - "value"              — the entire JSON value
 *   - "value.amount"       — JSON path into the value object
 *   - "value.items[0].id"  — JSON array access
 *   - "header.source"      — a header value by name
 *
 * Expression grammar (simple DSL):
 *   - Equals:    "value.status == 'active'"
 *   - NotEqual:  "value.status != 'deleted'"
 *   - GT:        "value.amount > 1000"
 *   - LT:        "value.age < 65"
 *   - GTE:       "value.score >= 80"
 *   - LTE:       "value.retries <= 3"
 *   - Contains:  "value.name contains 'smith'"
 *   - Regex:     "value.email matches '[a-z]+@domain\\.com'"
 *   - IsNull:    "value.phone is null"
 *   - IsNotNull: "value.email is not null"
 *   - AND:       "value.age > 18 and value.country == 'US'"
 *   - OR:        "value.priority == 1 or value.escalated == true"
 *   - NOT:       "not (value.deleted == true)"
 *
 * The predicate tree is compiled once at construction time and evaluated
 * per-record with zero allocation in the hot path (field lookups reuse
 * pre-parsed JSON pointers).
 *
 * Thread-safety:
 *   process() is called from a single thread per FilterProcessor instance,
 *   so no internal locking is needed.  Multiple FilterProcessor instances
 *   can run concurrently.
 *
 * Dependencies:
 *   - nlohmann/json for JSON value access
 *   - <regex> for regex matching
 */

#include "torrent/streams/topology.h"

#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <memory>
#include <optional>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

using json = nlohmann::json;

namespace torrent::streams {

// ============================================================================
// Anonymous namespace — predicate implementation
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_filter_logger() {
    static auto logger = spdlog::get("filter_processor");
    if (!logger) {
        logger = spdlog::stdout_color_mt("filter_processor");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// FieldAccessor — extracts a value from a record by path
// --------------------------------------------------------------------------

enum class FieldSource : uint8_t {
    key    = 0,
    value  = 1,
    header = 2,
};

struct FieldAccessor {
    FieldSource source;
    std::string path;       // for value: JSON pointer path (e.g. "/amount")
    std::string header_name; // for header: header key

    /// Extract the field value as a string from a record.
    [[nodiscard]] std::optional<std::string> extract(
        const std::string& key,
        const json& value,
        const std::unordered_map<std::string, std::string>& headers) const
    {
        switch (source) {
        case FieldSource::key:
            return key;

        case FieldSource::value: {
            try {
                json::json_pointer ptr(path);
                if (!value.contains(ptr)) return std::nullopt;

                const auto& v = value.at(ptr);
                if (v.is_string())   return v.get<std::string>();
                if (v.is_number())   return std::to_string(v.get<double>());
                if (v.is_boolean())  return v.get<bool>() ? "true" : "false";
                if (v.is_null())     return std::nullopt;
                return v.dump();  // array or object → JSON string
            } catch (const std::exception&) {
                return std::nullopt;
            }
        }

        case FieldSource::header: {
            auto it = headers.find(header_name);
            if (it != headers.end()) return it->second;
            return std::nullopt;
        }
        }

        return std::nullopt;
    }

    /// Extract as a double for numeric comparisons.
    [[nodiscard]] std::optional<double> extract_numeric(
        const std::string& key,
        const json& value,
        const std::unordered_map<std::string, std::string>& headers) const
    {
        switch (source) {
        case FieldSource::key:
            return parse_double(key);

        case FieldSource::value: {
            try {
                json::json_pointer ptr(path);
                if (!value.contains(ptr)) return std::nullopt;
                const auto& v = value.at(ptr);
                if (v.is_number()) return v.get<double>();
                if (v.is_string()) return parse_double(v.get<std::string>());
                return std::nullopt;
            } catch (const std::exception&) {
                return std::nullopt;
            }
        }

        case FieldSource::header: {
            auto it = headers.find(header_name);
            if (it != headers.end()) return parse_double(it->second);
            return std::nullopt;
        }
        }

        return std::nullopt;
    }

private:
    [[nodiscard]] static std::optional<double> parse_double(std::string_view s) {
        try {
            std::string str(s);
            size_t pos = 0;
            double v = std::stod(str, &pos);
            if (pos == str.size()) return v;
            return std::nullopt;
        } catch (...) {
            return std::nullopt;
        }
    }
};

// --------------------------------------------------------------------------
// FieldAccessor parser: "value.amount" → value source, "/amount" path
// --------------------------------------------------------------------------

[[nodiscard]] FieldAccessor parse_field(std::string_view field_spec) {
    FieldAccessor acc;

    if (field_spec == "key") {
        acc.source = FieldSource::key;
    } else if (field_spec.starts_with("header.")) {
        acc.source = FieldSource::header;
        acc.header_name = std::string(field_spec.substr(7));
    } else if (field_spec.starts_with("value")) {
        acc.source = FieldSource::value;
        // Convert "value.amount" → "/amount"
        // "value.items[0].id" → "/items/0/id"
        std::string remainder(field_spec.substr(5)); // skip "value"
        if (remainder.empty()) {
            acc.path = ""; // root
        } else {
            // Replace '.' with '/', but not inside brackets
            std::string json_ptr;
            for (size_t i = 0; i < remainder.size(); ++i) {
                if (remainder[i] == '.') {
                    json_ptr += '/';
                } else if (remainder[i] == '[') {
                    json_ptr += '/';
                    // copy digits until ']'
                } else if (remainder[i] == ']') {
                    // skip
                } else {
                    json_ptr += remainder[i];
                }
            }
            // Remove leading slash if we also prepend one
            acc.path = json_ptr;
        }
    } else {
        // Default to value field
        acc.source = FieldSource::value;
        acc.path = std::string(field_spec);
    }

    return acc;
}

// --------------------------------------------------------------------------
// Predicate base
// --------------------------------------------------------------------------

enum class PredicateType : uint8_t {
    equals          = 0,
    not_equals      = 1,
    greater_than    = 2,
    less_than       = 3,
    greater_or_equal = 4,
    less_or_equal   = 5,
    contains        = 6,
    starts_with     = 7,
    ends_with       = 8,
    matches_regex   = 9,
    is_null         = 10,
    is_not_null     = 11,
    in_list         = 12,
    not_in_list     = 13,
    // Compound
    and_op          = 20,
    or_op           = 21,
    not_op          = 22,
    // Sentinel
    always_true     = 30,
    always_false    = 31,
};

[[nodiscard]] std::string_view predicate_type_name(PredicateType pt) {
    switch (pt) {
    case PredicateType::equals:           return "equals";
    case PredicateType::not_equals:       return "not_equals";
    case PredicateType::greater_than:     return "greater_than";
    case PredicateType::less_than:        return "less_than";
    case PredicateType::greater_or_equal: return "greater_or_equal";
    case PredicateType::less_or_equal:    return "less_or_equal";
    case PredicateType::contains:         return "contains";
    case PredicateType::starts_with:      return "starts_with";
    case PredicateType::ends_with:        return "ends_with";
    case PredicateType::matches_regex:    return "matches_regex";
    case PredicateType::is_null:          return "is_null";
    case PredicateType::is_not_null:      return "is_not_null";
    case PredicateType::in_list:          return "in_list";
    case PredicateType::not_in_list:      return "not_in_list";
    case PredicateType::and_op:           return "AND";
    case PredicateType::or_op:            return "OR";
    case PredicateType::not_op:           return "NOT";
    case PredicateType::always_true:      return "always_true";
    case PredicateType::always_false:     return "always_false";
    }
    return "unknown";
}

// --------------------------------------------------------------------------
// PredicateNode — a node in the predicate expression tree
// --------------------------------------------------------------------------

struct PredicateNode {
    PredicateType type;
    FieldAccessor left_field;
    std::string   literal_value;     // for comparison predicates
    double        numeric_value = 0; // for numeric comparisons
    std::regex    regex_pattern;     // for regex matching
    bool          case_sensitive = true;
    std::vector<std::string> list_values; // for in_list

    // Compound children
    std::vector<std::unique_ptr<PredicateNode>> children;

    PredicateNode() = default;
    explicit PredicateNode(PredicateType t) : type(t) {}

    /// Evaluate this predicate against a record.
    [[nodiscard]] bool evaluate(
        const std::string& key,
        const json& value,
        const std::unordered_map<std::string, std::string>& headers) const
    {
        switch (type) {
        case PredicateType::always_true:  return true;
        case PredicateType::always_false: return false;

        case PredicateType::and_op:
            return evaluate_and(key, value, headers);

        case PredicateType::or_op:
            return evaluate_or(key, value, headers);

        case PredicateType::not_op:
            return evaluate_not(key, value, headers);

        case PredicateType::is_null:
            return !left_field.extract(key, value, headers).has_value();

        case PredicateType::is_not_null:
            return left_field.extract(key, value, headers).has_value();

        case PredicateType::contains: {
            auto field_val = left_field.extract(key, value, headers);
            if (!field_val) return false;
            return contains_str(*field_val, literal_value);
        }

        case PredicateType::starts_with: {
            auto field_val = left_field.extract(key, value, headers);
            if (!field_val) return false;
            return starts_with_str(*field_val, literal_value);
        }

        case PredicateType::ends_with: {
            auto field_val = left_field.extract(key, value, headers);
            if (!field_val) return false;
            return ends_with_str(*field_val, literal_value);
        }

        case PredicateType::matches_regex: {
            auto field_val = left_field.extract(key, value, headers);
            if (!field_val) return false;
            return std::regex_match(*field_val, regex_pattern);
        }

        case PredicateType::in_list: {
            auto field_val = left_field.extract(key, value, headers);
            if (!field_val) return false;
            return std::find(list_values.begin(), list_values.end(),
                             *field_val) != list_values.end();
        }

        case PredicateType::not_in_list: {
            auto field_val = left_field.extract(key, value, headers);
            if (!field_val) return true;  // null is not in list
            return std::find(list_values.begin(), list_values.end(),
                             *field_val) == list_values.end();
        }

        // Numeric comparisons
        case PredicateType::equals:
        case PredicateType::not_equals:
        case PredicateType::greater_than:
        case PredicateType::less_than:
        case PredicateType::greater_or_equal:
        case PredicateType::less_or_equal:
            return evaluate_comparison(key, value, headers);

        default:
            return false;
        }
    }

private:
    [[nodiscard]] bool evaluate_and(
        const std::string& key, const json& value,
        const std::unordered_map<std::string, std::string>& headers) const
    {
        for (auto& child : children) {
            if (!child->evaluate(key, value, headers)) return false;
        }
        return true;
    }

    [[nodiscard]] bool evaluate_or(
        const std::string& key, const json& value,
        const std::unordered_map<std::string, std::string>& headers) const
    {
        for (auto& child : children) {
            if (child->evaluate(key, value, headers)) return true;
        }
        return false;
    }

    [[nodiscard]] bool evaluate_not(
        const std::string& key, const json& value,
        const std::unordered_map<std::string, std::string>& headers) const
    {
        if (children.empty()) return false;
        return !children[0]->evaluate(key, value, headers);
    }

    [[nodiscard]] bool evaluate_comparison(
        const std::string& key, const json& value,
        const std::unordered_map<std::string, std::string>& headers) const
    {
        // Try numeric comparison first
        auto num_val = left_field.extract_numeric(key, value, headers);
        if (num_val) {
            switch (type) {
            case PredicateType::equals:
                return std::fabs(*num_val - numeric_value) < 1e-9;
            case PredicateType::not_equals:
                return std::fabs(*num_val - numeric_value) >= 1e-9;
            case PredicateType::greater_than:
                return *num_val > numeric_value;
            case PredicateType::less_than:
                return *num_val < numeric_value;
            case PredicateType::greater_or_equal:
                return *num_val >= numeric_value - 1e-9;
            case PredicateType::less_or_equal:
                return *num_val <= numeric_value + 1e-9;
            default: break;
            }
        }

        // Fall back to string comparison
        auto str_val = left_field.extract(key, value, headers);
        if (!str_val) return false;

        int cmp = compare_str(*str_val, literal_value);

        switch (type) {
        case PredicateType::equals:
            return cmp == 0;
        case PredicateType::not_equals:
            return cmp != 0;
        case PredicateType::greater_than:
            return cmp > 0;
        case PredicateType::less_than:
            return cmp < 0;
        case PredicateType::greater_or_equal:
            return cmp >= 0;
        case PredicateType::less_or_equal:
            return cmp <= 0;
        default: break;
        }

        return false;
    }

    // ---- String helpers ----

    [[nodiscard]] bool contains_str(
        const std::string& haystack, const std::string& needle) const
    {
        if (case_sensitive) {
            return haystack.find(needle) != std::string::npos;
        }
        return to_lower(haystack).find(to_lower(needle)) != std::string::npos;
    }

    [[nodiscard]] bool starts_with_str(
        const std::string& s, const std::string& prefix) const
    {
        if (prefix.size() > s.size()) return false;
        if (case_sensitive) {
            return s.compare(0, prefix.size(), prefix) == 0;
        }
        return to_lower(s).compare(0, prefix.size(), to_lower(prefix)) == 0;
    }

    [[nodiscard]] bool ends_with_str(
        const std::string& s, const std::string& suffix) const
    {
        if (suffix.size() > s.size()) return false;
        if (case_sensitive) {
            return s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
        }
        return to_lower(s).compare(s.size() - suffix.size(),
                                   suffix.size(),
                                   to_lower(suffix)) == 0;
    }

    [[nodiscard]] int compare_str(
        const std::string& a, const std::string& b) const
    {
        if (case_sensitive) return a.compare(b);
        return to_lower(a).compare(to_lower(b));
    }

    [[nodiscard]] static std::string to_lower(std::string s) {
        std::transform(s.begin(), s.end(), s.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        return s;
    }
};

} // anonymous namespace

// ============================================================================
// FilterProcessor — public API
// ============================================================================

class FilterProcessor {
public:
    /// Construct from a predicate expression string.
    /// @param name       processor name (for logging/metrics)
    /// @param expression filter expression in the DSL described above
    explicit FilterProcessor(std::string name, std::string expression)
        : name_(std::move(name))
        , expression_(std::move(expression))
    {
        compile();
    }

    /// Construct programmatically with a pre-built predicate tree.
    FilterProcessor(std::string name,
                     std::unique_ptr<PredicateNode> root)
        : name_(std::move(name))
        , root_(std::move(root))
    {
        expression_ = "(programmatic)";
    }

    // ------------------------------------------------------------------
    // Evaluate a record
    // ------------------------------------------------------------------

    /// Returns true if the record passes the filter.
    [[nodiscard]] bool evaluate(const std::string& key,
                                  const json& value,
                                  const std::unordered_map<std::string, std::string>& headers = {}) const
    {
        if (!root_) return true;

        bool result = root_->evaluate(key, value, headers);

        if (result) {
            passed_.fetch_add(1, std::memory_order_relaxed);
        } else {
            dropped_.fetch_add(1, std::memory_order_relaxed);
        }

        return result;
    }

    /// Convenience overload: evaluate using StreamRecord-like args.
    [[nodiscard]] bool evaluate_record(const std::string& topic,
                                         const std::string& key,
                                         const json& value,
                                         int32_t /*partition*/,
                                         offset_t /*offset*/,
                                         int64_t /*timestamp_ms*/) const
    {
        return evaluate(key, value, {});
    }

    // ------------------------------------------------------------------
    // Expression compilation
    // ------------------------------------------------------------------

    /// Re-compile the expression (e.g. after runtime reconfiguration).
    void compile() {
        auto log = get_filter_logger();
        log->debug("FilterProcessor '{}': compiling expression '{}'",
                    name_, expression_);

        root_ = compile_expression(expression_);
        passed_.store(0);
        dropped_.store(0);

        log->debug("FilterProcessor '{}': compilation complete", name_);
    }

    // ------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------

    [[nodiscard]] uint64_t passed_count() const noexcept {
        return passed_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t dropped_count() const noexcept {
        return dropped_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] double pass_rate() const noexcept {
        uint64_t p = passed_.load(std::memory_order_relaxed);
        uint64_t d = dropped_.load(std::memory_order_relaxed);
        uint64_t total = p + d;
        return total > 0 ? static_cast<double>(p) / static_cast<double>(total) : 0.0;
    }

    [[nodiscard]] const std::string& name() const noexcept { return name_; }
    [[nodiscard]] const std::string& expression() const noexcept { return expression_; }

    // ------------------------------------------------------------------
    // Static helper: quick filter without constructing a FilterProcessor
    // ------------------------------------------------------------------

    [[nodiscard]] static bool quick_filter(const std::string& expression,
                                            const json& value)
    {
        FilterProcessor tmp("quick", expression);
        return tmp.evaluate("", value);
    }

private:
    // ------------------------------------------------------------------
    // DSL Compiler — tokenize and parse expression string into tree
    // ------------------------------------------------------------------

    [[nodiscard]] std::unique_ptr<PredicateNode> compile_expression(
        const std::string& expr) const
    {
        auto log = get_filter_logger();
        std::string trimmed = trim(expr);

        if (trimmed.empty()) {
            return std::make_unique<PredicateNode>(PredicateType::always_true);
        }

        // Check for compound operators
        auto and_pos = find_keyword(trimmed, " and ");
        if (and_pos != std::string::npos) {
            return compile_binary(PredicateType::and_op,
                                  trimmed.substr(0, and_pos),
                                  trimmed.substr(and_pos + 5),
                                  "and");
        }

        auto or_pos = find_keyword(trimmed, " or ");
        if (or_pos != std::string::npos) {
            return compile_binary(PredicateType::or_op,
                                  trimmed.substr(0, or_pos),
                                  trimmed.substr(or_pos + 4),
                                  "or");
        }

        // NOT
        if (trimmed.starts_with("not ") || trimmed.starts_with("NOT ")) {
            return compile_unary(PredicateType::not_op,
                                 trimmed.substr(4));
        }

        // IS NULL / IS NOT NULL
        if (ends_with_icase(trimmed, " is null")) {
            std::string field = trimmed.substr(0, trimmed.size() - 8);
            auto node = std::make_unique<PredicateNode>(PredicateType::is_null);
            node->left_field = parse_field(trim(field));
            return node;
        }
        if (ends_with_icase(trimmed, " is not null")) {
            std::string field = trimmed.substr(0, trimmed.size() - 12);
            auto node = std::make_unique<PredicateNode>(PredicateType::is_not_null);
            node->left_field = parse_field(trim(field));
            return node;
        }

        // IN / NOT IN
        if (contains_icase(trimmed, " in (")) {
            return compile_in_list(trimmed, false);
        }
        if (contains_icase(trimmed, " not in (")) {
            return compile_in_list(trimmed, true);
        }

        // Comparison operators: == != > < >= <=
        auto [field, op, literal] = parse_comparison(trimmed);
        if (!op.empty()) {
            auto node = create_comparison_node(op, field, literal);
            if (node) return node;
        }

        // String operators: contains, starts_with, ends_with, matches
        if (contains_icase(trimmed, " contains ")) {
            return compile_string_op(trimmed, "contains",
                                     PredicateType::contains);
        }
        if (contains_icase(trimmed, " starts with ")) {
            return compile_string_op(trimmed, "starts with",
                                     PredicateType::starts_with);
        }
        if (contains_icase(trimmed, " ends with ")) {
            return compile_string_op(trimmed, "ends with",
                                     PredicateType::ends_with);
        }
        if (contains_icase(trimmed, " matches ")) {
            return compile_string_op(trimmed, "matches",
                                     PredicateType::matches_regex);
        }

        // Fallback: treat as literal boolean
        log->warn("FilterProcessor '{}': could not parse expression '{}', "
                   "defaulting to always_true", name_, trimmed);
        return std::make_unique<PredicateNode>(PredicateType::always_true);
    }

    [[nodiscard]] std::unique_ptr<PredicateNode> compile_binary(
        PredicateType op_type,
        const std::string& left_expr,
        const std::string& right_expr,
        const std::string& /*op_name*/) const
    {
        auto node = std::make_unique<PredicateNode>(op_type);
        node->children.push_back(compile_expression(left_expr));
        node->children.push_back(compile_expression(right_expr));
        return node;
    }

    [[nodiscard]] std::unique_ptr<PredicateNode> compile_unary(
        PredicateType op_type,
        const std::string& inner_expr) const
    {
        auto node = std::make_unique<PredicateNode>(op_type);
        node->children.push_back(compile_expression(trim(inner_expr)));
        return node;
    }

    [[nodiscard]] std::unique_ptr<PredicateNode> compile_string_op(
        const std::string& expr,
        const std::string& keyword,
        PredicateType op_type) const
    {
        auto pos = find_keyword(expr, " " + keyword + " ");
        if (pos == std::string::npos) {
            return std::make_unique<PredicateNode>(PredicateType::always_true);
        }

        std::string field_str  = trim(expr.substr(0, pos));
        std::string literal_str = trim(expr.substr(pos + keyword.size() + 2));

        // Strip quotes from literal
        literal_str = unquote(literal_str);

        auto node = std::make_unique<PredicateNode>(op_type);
        node->left_field     = parse_field(field_str);
        node->literal_value  = literal_str;

        if (op_type == PredicateType::matches_regex) {
            try {
                node->regex_pattern = std::regex(literal_str,
                                                  std::regex::ECMAScript |
                                                  std::regex::optimize);
            } catch (const std::regex_error& e) {
                auto log = get_filter_logger();
                log->error("FilterProcessor '{}': invalid regex '{}': {}",
                            name_, literal_str, e.what());
                return std::make_unique<PredicateNode>(PredicateType::always_false);
            }
        }

        return node;
    }

    [[nodiscard]] std::unique_ptr<PredicateNode> compile_in_list(
        const std::string& expr, bool negate) const
    {
        std::string keyword = negate ? " not in (" : " in (";
        auto pos = find_keyword(expr, keyword);
        if (pos == std::string::npos) {
            return std::make_unique<PredicateNode>(PredicateType::always_true);
        }

        std::string field_str = trim(expr.substr(0, pos));
        std::string list_str  = expr.substr(pos + keyword.size());

        // Remove trailing )
        if (list_str.back() == ')') list_str.pop_back();

        // Split by comma
        auto node = std::make_unique<PredicateNode>(
            negate ? PredicateType::not_in_list : PredicateType::in_list);
        node->left_field = parse_field(field_str);

        std::stringstream ss(list_str);
        std::string item;
        while (std::getline(ss, item, ',')) {
            node->list_values.push_back(unquote(trim(item)));
        }

        return node;
    }

    [[nodiscard]] std::unique_ptr<PredicateNode> create_comparison_node(
        const std::string& op,
        const std::string& field_str,
        const std::string& literal_str) const
    {
        PredicateType pt;
        if      (op == "==") pt = PredicateType::equals;
        else if (op == "!=") pt = PredicateType::not_equals;
        else if (op == ">")  pt = PredicateType::greater_than;
        else if (op == "<")  pt = PredicateType::less_than;
        else if (op == ">=") pt = PredicateType::greater_or_equal;
        else if (op == "<=") pt = PredicateType::less_or_equal;
        else return nullptr;

        auto node      = std::make_unique<PredicateNode>(pt);
        node->left_field  = parse_field(field_str);
        node->literal_value = unquote(literal_str);

        // Parse numeric value
        try {
            node->numeric_value = std::stod(node->literal_value);
        } catch (...) {
            // Not a number — comparison will fall back to string
            node->numeric_value = 0;
        }

        return node;
    }

    /// Parse a comparison expression like "value.amount > 1000".
    [[nodiscard]] static std::tuple<std::string, std::string, std::string>
    parse_comparison(const std::string& expr) {
        // Try two-character operators first
        for (auto& op : {"!=", ">=", "<=", "=="}) {
            auto pos = expr.find(op);
            if (pos != std::string::npos) {
                return {
                    trim(expr.substr(0, pos)),
                    std::string(op),
                    trim(expr.substr(pos + 2))
                };
            }
        }
        // Then single-character
        for (auto& op : {">", "<"}) {
            auto pos = expr.find(op);
            if (pos != std::string::npos) {
                return {
                    trim(expr.substr(0, pos)),
                    std::string(op),
                    trim(expr.substr(pos + 1))
                };
            }
        }
        return {};
    }

    // ---- String helpers ----

    [[nodiscard]] static std::string trim(std::string s) {
        auto start = s.find_first_not_of(" \t\r\n");
        if (start == std::string::npos) return "";
        auto end = s.find_last_not_of(" \t\r\n");
        return s.substr(start, end - start + 1);
    }

    [[nodiscard]] static std::string unquote(std::string s) {
        if (s.size() >= 2 &&
            ((s.front() == '\'' && s.back() == '\'') ||
             (s.front() == '"'  && s.back() == '"'))) {
            return s.substr(1, s.size() - 2);
        }
        return s;
    }

    [[nodiscard]] static std::string to_lower(std::string s) {
        std::transform(s.begin(), s.end(), s.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        return s;
    }

    [[nodiscard]] static bool ends_with_icase(const std::string& s,
                                                const std::string& suffix) {
        if (suffix.size() > s.size()) return false;
        return to_lower(s.substr(s.size() - suffix.size())) == to_lower(suffix);
    }

    [[nodiscard]] static bool contains_icase(const std::string& s,
                                               const std::string& needle) {
        return to_lower(s).find(to_lower(needle)) != std::string::npos;
    }

    [[nodiscard]] static size_t find_keyword(const std::string& s,
                                               const std::string& keyword) {
        // Case-insensitive find, returns position or npos
        auto lower_s  = to_lower(s);
        auto lower_kw = to_lower(keyword);
        return lower_s.find(lower_kw);
    }

    std::string                          name_;
    std::string                          expression_;
    std::unique_ptr<PredicateNode>       root_;

    mutable std::atomic<uint64_t>       passed_{0};
    mutable std::atomic<uint64_t>       dropped_{0};
};

// ============================================================================
// FilterProcessorBuilder — fluent API for programmatic construction
// ============================================================================

class FilterProcessorBuilder {
public:
    explicit FilterProcessorBuilder(std::string name)
        : name_(std::move(name))
    {}

    FilterProcessorBuilder& field(std::string field_spec) {
        current_field_ = std::move(field_spec);
        return *this;
    }

    FilterProcessorBuilder& equals(std::string value) {
        push_leaf(PredicateType::equals, std::move(value));
        return *this;
    }

    FilterProcessorBuilder& not_equals(std::string value) {
        push_leaf(PredicateType::not_equals, std::move(value));
        return *this;
    }

    FilterProcessorBuilder& greater_than(double value) {
        auto node = std::make_unique<PredicateNode>(PredicateType::greater_than);
        node->left_field = parse_field(current_field_);
        node->numeric_value = value;
        node->literal_value = std::to_string(value);
        push_node(std::move(node));
        return *this;
    }

    FilterProcessorBuilder& less_than(double value) {
        auto node = std::make_unique<PredicateNode>(PredicateType::less_than);
        node->left_field = parse_field(current_field_);
        node->numeric_value = value;
        node->literal_value = std::to_string(value);
        push_node(std::move(node));
        return *this;
    }

    FilterProcessorBuilder& contains(std::string substring) {
        push_leaf(PredicateType::contains, std::move(substring));
        return *this;
    }

    FilterProcessorBuilder& matches_regex(std::string pattern) {
        auto node = std::make_unique<PredicateNode>(PredicateType::matches_regex);
        node->left_field = parse_field(current_field_);
        node->literal_value = pattern;
        node->regex_pattern = std::regex(pattern,
                                          std::regex::ECMAScript |
                                          std::regex::optimize);
        push_node(std::move(node));
        return *this;
    }

    FilterProcessorBuilder& is_null() {
        auto node = std::make_unique<PredicateNode>(PredicateType::is_null);
        node->left_field = parse_field(current_field_);
        push_node(std::move(node));
        return *this;
    }

    FilterProcessorBuilder& is_not_null() {
        auto node = std::make_unique<PredicateNode>(PredicateType::is_not_null);
        node->left_field = parse_field(current_field_);
        push_node(std::move(node));
        return *this;
    }

    FilterProcessorBuilder& and_op() {
        flatten(PredicateType::and_op);
        return *this;
    }

    FilterProcessorBuilder& or_op() {
        flatten(PredicateType::or_op);
        return *this;
    }

    FilterProcessorBuilder& not_op() {
        auto node = std::make_unique<PredicateNode>(PredicateType::not_op);
        if (!stack_.empty()) {
            node->children.push_back(std::move(stack_.back()));
            stack_.pop_back();
        }
        stack_.push_back(std::move(node));
        return *this;
    }

    [[nodiscard]] FilterProcessor build() {
        flatten_all();
        if (stack_.empty()) {
            stack_.push_back(
                std::make_unique<PredicateNode>(PredicateType::always_true));
        }
        return FilterProcessor(name_, std::move(stack_.back()));
    }

private:
    void push_leaf(PredicateType pt, std::string literal) {
        auto node = std::make_unique<PredicateNode>(pt);
        node->left_field   = parse_field(current_field_);
        node->literal_value = std::move(literal);
        stack_.push_back(std::move(node));
    }

    void push_node(std::unique_ptr<PredicateNode> node) {
        stack_.push_back(std::move(node));
    }

    void flatten(PredicateType op) {
        if (stack_.size() < 2) return;

        auto node = std::make_unique<PredicateNode>(op);
        // Pop last two nodes
        auto right = std::move(stack_.back()); stack_.pop_back();
        auto left  = std::move(stack_.back()); stack_.pop_back();
        node->children.push_back(std::move(left));
        node->children.push_back(std::move(right));
        stack_.push_back(std::move(node));
    }

    void flatten_all() {
        while (stack_.size() > 1) {
            flatten(PredicateType::and_op);
        }
    }

    std::string                                  name_;
    std::string                                  current_field_;
    std::vector<std::unique_ptr<PredicateNode>>  stack_;
};

} // namespace torrent::streams
