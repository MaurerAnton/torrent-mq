/**
 * torrent-mq — Configuration system implementation
 *
 * Provides a complete configuration system with 50+ default values,
 * JSON file loading with nested key flattening, CLI argument parsing,
 * typed getters (int, int64_t, bool, string, double), seed_servers
 * parsing with IPv6 support, input validation, and serialization
 * (to_json, to_string).
 */

#include "torrent/common/config.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace torrent {

// ============================================================================
// Private helpers (anonymous namespace)
// ============================================================================

namespace {

// ---- String utilities ----

/**
 * Trim leading and trailing whitespace from a string.
 */
[[nodiscard]] std::string trim(const std::string& s) {
    auto start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return {};
    auto end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

/**
 * Convert a string to lowercase.
 */
[[nodiscard]] std::string to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}

/**
 * Split a string by delimiter and return a vector of substrings.
 * Empty tokens are skipped.
 */
[[nodiscard]] std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> tokens;
    std::istringstream iss(s);
    std::string token;
    while (std::getline(iss, token, delim)) {
        token = trim(token);
        if (!token.empty()) {
            tokens.push_back(std::move(token));
        }
    }
    return tokens;
}

/**
 * Check whether a string represents a valid integer.
 */
[[nodiscard]] bool is_integer_string(const std::string& s) noexcept {
    if (s.empty()) return false;
    size_t start = (s[0] == '-' || s[0] == '+') ? 1 : 0;
    if (start >= s.size()) return false;
    for (size_t i = start; i < s.size(); ++i) {
        if (!std::isdigit(static_cast<unsigned char>(s[i]))) return false;
    }
    return true;
}

/**
 * Check whether a string represents a valid floating-point number.
 */
[[nodiscard]] bool is_float_string(const std::string& s) noexcept {
    if (s.empty()) return false;
    bool has_dot = false;
    bool has_digit = false;
    size_t start = (s[0] == '-' || s[0] == '+') ? 1 : 0;
    for (size_t i = start; i < s.size(); ++i) {
        char c = s[i];
        if (c == '.') {
            if (has_dot) return false;
            has_dot = true;
        } else if (std::isdigit(static_cast<unsigned char>(c))) {
            has_digit = true;
        } else {
            return false;
        }
    }
    return has_digit;
}

// ---- JSON value conversion ----

/**
 * Convert a JSON value to its string representation for flat storage.
 *
 * Scalars are converted to their natural string form.
 * Arrays and objects are serialized as compact JSON.
 */
[[nodiscard]] std::string json_value_to_string(const nlohmann::json& j) {
    switch (j.type()) {
    case nlohmann::json::value_t::string:
        return j.get<std::string>();
    case nlohmann::json::value_t::boolean:
        return j.get<bool>() ? "true" : "false";
    case nlohmann::json::value_t::number_integer:
        return std::to_string(j.get<int64_t>());
    case nlohmann::json::value_t::number_unsigned:
        return std::to_string(j.get<uint64_t>());
    case nlohmann::json::value_t::number_float:
        return std::to_string(j.get<double>());
    case nlohmann::json::value_t::null:
        return "null";
    default:
        // Arrays, objects, binary: serialize as compact JSON string
        return j.dump();
    }
}

/**
 * Parse a string value back into a JSON value for to_json() reconstruction.
 *
 * Tries in order: boolean, null, integer, float, then falls back to string.
 * This heuristic matches the most natural representation.
 */
[[nodiscard]] nlohmann::json string_to_json_value(const std::string& s) {
    if (s.empty()) {
        return nlohmann::json("");
    }

    // Boolean
    if (s == "true")  return nlohmann::json(true);
    if (s == "false") return nlohmann::json(false);

    // Null
    if (s == "null")  return nlohmann::json(nullptr);

    // Integer
    if (is_integer_string(s)) {
        try {
            int64_t v = std::stoll(s);
            // Use int if it fits, otherwise keep as int64
            if (v >= static_cast<int64_t>(std::numeric_limits<int>::min()) &&
                v <= static_cast<int64_t>(std::numeric_limits<int>::max())) {
                return nlohmann::json(static_cast<int>(v));
            }
            return nlohmann::json(v);
        } catch (...) {
            // Fall through
        }
    }

    // Float
    if (is_float_string(s)) {
        try {
            return nlohmann::json(std::stod(s));
        } catch (...) {
            // Fall through
        }
    }

    // Default: string
    return nlohmann::json(s);
}

/**
 * Insert a value into a nested JSON object given a dot-separated key path.
 * Creates intermediate objects as needed.
 */
void json_set_nested(nlohmann::json& root, const std::string& key,
                     const nlohmann::json& value) {
    auto parts = split(key, '.');
    if (parts.empty()) return;

    nlohmann::json* current = &root;
    for (size_t i = 0; i < parts.size() - 1; ++i) {
        const auto& part = parts[i];
        if (!current->contains(part) || !(*current)[part].is_object()) {
            (*current)[part] = nlohmann::json::object();
        }
        current = &(*current)[part];
    }
    (*current)[parts.back()] = value;
}

} // anonymous namespace

// ============================================================================
// config::set_defaults()
//
// Initializes the complete set of 50+ default configuration properties.
// These defaults mirror Apache Kafka's defaults where applicable and define
// sensible production values for torrent-mq.
// ============================================================================

void config::set_defaults() {
    // ── Broker identity ──────────────────────────────────────────────────
    // Unique identifier for this broker within the cluster.
    // Must be a non-negative integer. 0 is the default for single-node.
    props_["broker.id"] = "0";

    // ── Listeners ────────────────────────────────────────────────────────
    // Plaintext TCP port for client and inter-broker communication.
    props_["listeners.plain.port"]    = "9092";
    // TLS-encrypted port. Only active when security.tls.enabled=true.
    props_["listeners.tls.port"]      = "9093";

    // ── Admin & Metrics ──────────────────────────────────────────────────
    // REST API admin server. Disable in production if unused.
    props_["admin.port"]              = "9644";
    props_["admin.enabled"]           = "true";
    // Prometheus metrics HTTP endpoint.
    props_["metrics.port"]            = "9090";
    props_["metrics.enabled"]         = "true";

    // ── Storage engine ───────────────────────────────────────────────────
    // Root directory for all persistent data (segments, indexes, metadata).
    props_["storage.log.dir"]               = "/var/lib/torrent/data";
    // Maximum size of a single log segment before rollover (1 GiB).
    props_["storage.segment.bytes"]          = "1073741824";
    // Maximum age of a segment before it becomes eligible for rollover (7 days).
    props_["storage.segment.max_age_ms"]     = "604800000";
    // Global retention size limit. -1 means unlimited.
    props_["storage.retention.bytes"]        = "-1";
    // Global retention time limit in milliseconds (7 days).
    props_["storage.retention.ms"]           = "604800000";
    // Compression type for on-disk segments: none, gzip, snappy, lz4, zstd.
    props_["storage.compression.type"]       = "none";

    // ── Network threading & I/O ──────────────────────────────────────────
    // Number of threads dedicated to non-blocking network I/O.
    props_["network.num_io_threads"]              = "4";
    // Number of worker threads for request processing.
    props_["network.num_worker_threads"]           = "8";
    // Hard limit on concurrent TCP connections across all listeners.
    props_["network.max_connections"]              = "65536";
    // Kernel socket send buffer size hint in bytes (1 MiB).
    props_["network.socket.send_buffer.bytes"]     = "1048576";
    // Kernel socket receive buffer size hint in bytes (1 MiB).
    props_["network.socket.recv_buffer.bytes"]     = "1048576";
    // Maximum allowed size of a single request in bytes (100 MiB).
    props_["network.max_request_size"]             = "104857600";

    // ── Topic defaults ───────────────────────────────────────────────────
    // Default replication factor for auto-created topics.
    props_["topic.default.replication_factor"] = "3";
    // Default number of partitions for auto-created topics.
    props_["topic.default.partitions"]         = "1";
    // Allow clients to produce to non-existent topics (auto-creation).
    props_["topic.auto_create"]                = "true";
    // Default compression type for new topics.
    props_["topic.compression.type"]           = "none";
    // Maximum size of a single message (record batch) in bytes (~1 MiB).
    props_["topic.max.message.bytes"]          = "1048588";

    // ── Consumer defaults ────────────────────────────────────────────────
    // Maximum bytes to return per fetch response.
    props_["consumer.fetch.max_bytes"]   = "57671680";
    // Minimum bytes to accumulate before responding to a fetch request.
    props_["consumer.fetch.min_bytes"]   = "1";
    // Maximum time to wait for min_bytes to be satisfied (ms).
    props_["consumer.fetch.max_wait_ms"] = "500";

    // ── Raft consensus ───────────────────────────────────────────────────
    // Interval between leader heartbeats (ms).
    props_["raft.heartbeat_interval_ms"]       = "150";
    // Minimum randomized election timeout (ms).
    props_["raft.election_timeout_min_ms"]     = "150";
    // Maximum randomized election timeout (ms).
    props_["raft.election_timeout_max_ms"]     = "300";
    // Number of log entries before triggering a snapshot.
    props_["raft.snapshot_threshold_entries"]  = "10000";
    // Total log byte size before triggering a snapshot (~64 MiB).
    props_["raft.snapshot_threshold_bytes"]    = "67108864";

    // ── Security ─────────────────────────────────────────────────────────
    // Enable SASL/SCRAM authentication layer.
    props_["security.sasl.enabled"]              = "false";
    // Enable TLS 1.3 transport encryption.
    props_["security.tls.enabled"]               = "false";
    // Path to the server certificate PEM file.
    props_["security.tls.certificate_path"]      = "";
    // Path to the server private key PEM file.
    props_["security.tls.key_path"]              = "";
    // Path to the CA certificate bundle for client verification.
    props_["security.tls.ca_path"]               = "";
    // Require clients to present a valid certificate (mTLS).
    props_["security.tls.require_client_auth"]   = "false";

    // ── Transactions ─────────────────────────────────────────────────────
    // Enable exactly-once transactional produce support.
    props_["transaction.enabled"]          = "true";
    // Maximum time a transaction can remain open before abort (ms).
    props_["transaction.timeout.ms"]       = "60000";
    // Time before an idle transactional ID is expired (7 days).
    props_["transaction.id.expiration.ms"] = "604800000";

    // ── Cluster ──────────────────────────────────────────────────────────
    // Logical name for this cluster. Used for fencing and metadata isolation.
    props_["cluster.id"]           = "torrent-cluster";
    // Comma-separated list of seed broker endpoints (host:port).
    // Used for initial cluster discovery when joining.
    props_["cluster.seed_servers"] = "";

    // ── Schema Registry ──────────────────────────────────────────────────
    // Enable the embedded schema registry service.
    props_["schema.registry.enabled"] = "false";

    // ── Proxy bridges ────────────────────────────────────────────────────
    // REST proxy for HTTP-based produce/fetch.
    props_["proxy.rest.enabled"]      = "false";
    // WebSocket proxy for browser-based clients.
    props_["proxy.websocket.enabled"] = "false";
    // MQTT protocol bridge.
    props_["proxy.mqtt.enabled"]      = "false";

    // ── Quotas ───────────────────────────────────────────────────────────
    // Default produce bandwidth limit in bytes/sec. -1 = unlimited.
    props_["quota.default.produce.bps"]    = "-1";
    // Default fetch bandwidth limit in bytes/sec. -1 = unlimited.
    props_["quota.default.fetch.bps"]      = "-1";
    // Default request rate limit in requests/sec. -1 = unlimited.
    props_["quota.default.request.rate"]   = "-1";

    spdlog::debug("config: {} default properties initialized", props_.size());
}

// ============================================================================
// config::parse_property()
//
// Recursively flattens a nested JSON structure into dot-separated keys
// stored in the flat property map. Arrays are serialized as compact JSON
// strings so they can be round-tripped.
// ============================================================================

void config::parse_property(const std::string& key,
                            const nlohmann::json& value,
                            const std::string& prefix) {
    std::string full_key = prefix.empty() ? key : prefix + "." + key;

    if (value.is_object()) {
        // Recurse into nested objects to flatten hierarchy
        for (auto it = value.begin(); it != value.end(); ++it) {
            parse_property(it.key(), it.value(), full_key);
        }
    } else if (value.is_array()) {
        // Serialize arrays as compact JSON string for lossless storage
        props_[full_key] = value.dump();
    } else {
        // Scalar value: convert to string representation
        props_[full_key] = json_value_to_string(value);
    }
}

// ============================================================================
// config::from_file()
//
// Loads configuration from a JSON file. The root element must be a JSON
// object. Nested objects are recursively flattened into dot-separated keys.
// Existing defaults are preserved for any keys not present in the file.
//
// Performs validation on critical fields (broker.id, listeners.plain.port,
// storage.log.dir) and logs warnings for invalid values, falling back to
// safe defaults.
// ============================================================================

config config::from_file(const std::filesystem::path& path) {
    config cfg;

    // Guard: file must exist and be readable
    if (!std::filesystem::exists(path)) {
        spdlog::warn("config: file not found '{}', using defaults only", path.string());
        return cfg;
    }

    std::ifstream ifs(path);
    if (!ifs.is_open()) {
        spdlog::error("config: cannot open file '{}', using defaults only", path.string());
        return cfg;
    }

    nlohmann::json root;
    try {
        ifs >> root;
    } catch (const nlohmann::json::parse_error& e) {
        spdlog::error("config: JSON parse error in '{}' at byte {}: {}",
                      path.string(), e.byte, e.what());
        return cfg;
    }

    if (!root.is_object()) {
        spdlog::error("config: root element in '{}' is not a JSON object (type: {})",
                      path.string(),
                      root.type_name());
        return cfg;
    }

    // Flatten all top-level keys recursively into the property map.
    // This overwrites defaults for keys present in the file.
    size_t overrides = 0;
    for (auto it = root.begin(); it != root.end(); ++it) {
        const auto& key = it.key();
        if (cfg.props_.find(key) != cfg.props_.end() ||
            it.value().is_object()) {
            // Count top-level overrides for logging
            if (!it.value().is_object()) ++overrides;
        }
        cfg.parse_property(it.key(), it.value());
    }

    spdlog::info("config: loaded {} total properties from '{}' ({} top-level overrides)",
                 cfg.props_.size(), path.string(), overrides);

    // ── Validate critical fields ─────────────────────────────────────────

    // broker.id must be a non-negative integer
    auto bid = cfg.get("broker.id");
    if (!bid.has_value() || !is_integer_string(bid.value())) {
        spdlog::warn("config: 'broker.id' missing or invalid, defaulting to 0");
        cfg.props_["broker.id"] = "0";
    } else {
        try {
            int id_val = std::stoi(bid.value());
            if (id_val < 0) {
                spdlog::warn("config: 'broker.id' {} is negative, defaulting to 0", id_val);
                cfg.props_["broker.id"] = "0";
            }
        } catch (...) {
            cfg.props_["broker.id"] = "0";
        }
    }

    // listeners.plain.port must be a valid port number (1-65535)
    auto plain_port = cfg.get_or("listeners.plain.port", "9092");
    try {
        int p = std::stoi(plain_port);
        if (p < 1 || p > 65535) {
            spdlog::warn("config: invalid listeners.plain.port={}, defaulting to 9092", p);
            cfg.props_["listeners.plain.port"] = "9092";
        }
    } catch (...) {
        spdlog::warn("config: unparseable listeners.plain.port='{}', defaulting to 9092",
                     plain_port);
        cfg.props_["listeners.plain.port"] = "9092";
    }

    // listeners.tls.port validation
    auto tls_port = cfg.get_or("listeners.tls.port", "9093");
    try {
        int p = std::stoi(tls_port);
        if (p < 1 || p > 65535) {
            spdlog::warn("config: invalid listeners.tls.port={}, defaulting to 9093", p);
            cfg.props_["listeners.tls.port"] = "9093";
        }
    } catch (...) {
        cfg.props_["listeners.tls.port"] = "9093";
    }

    // admin.port validation
    auto admin_port = cfg.get_or("admin.port", "9644");
    try {
        int p = std::stoi(admin_port);
        if (p < 1 || p > 65535) {
            spdlog::warn("config: invalid admin.port={}, defaulting to 9644", p);
            cfg.props_["admin.port"] = "9644";
        }
    } catch (...) {
        cfg.props_["admin.port"] = "9644";
    }

    // metrics.port validation
    auto metrics_port = cfg.get_or("metrics.port", "9090");
    try {
        int p = std::stoi(metrics_port);
        if (p < 1 || p > 65535) {
            spdlog::warn("config: invalid metrics.port={}, defaulting to 9090", p);
            cfg.props_["metrics.port"] = "9090";
        }
    } catch (...) {
        cfg.props_["metrics.port"] = "9090";
    }

    // storage.log.dir must be non-empty
    auto log_dir = cfg.get_or("storage.log.dir", "");
    if (log_dir.empty()) {
        spdlog::warn("config: 'storage.log.dir' is empty, defaulting to /var/lib/torrent/data");
        cfg.props_["storage.log.dir"] = "/var/lib/torrent/data";
    }

    // Validate TLS consistency: if TLS is enabled, certificate and key paths must be set
    if (cfg.get_or("security.tls.enabled", "false") == "true") {
        auto cert_path = cfg.get_or("security.tls.certificate_path", "");
        auto key_path  = cfg.get_or("security.tls.key_path", "");
        if (cert_path.empty()) {
            spdlog::warn("config: TLS enabled but certificate_path is empty");
        }
        if (key_path.empty()) {
            spdlog::warn("config: TLS enabled but key_path is empty");
        }
        if (!cert_path.empty() && !std::filesystem::exists(cert_path)) {
            spdlog::warn("config: TLS certificate file not found: '{}'", cert_path);
        }
        if (!key_path.empty() && !std::filesystem::exists(key_path)) {
            spdlog::warn("config: TLS key file not found: '{}'", key_path);
        }
    }

    // Validate raft election timeouts: min <= max
    try {
        int64_t min_to = std::stoll(cfg.get_or("raft.election_timeout_min_ms", "150"));
        int64_t max_to = std::stoll(cfg.get_or("raft.election_timeout_max_ms", "300"));
        if (min_to > max_to) {
            spdlog::warn("config: raft.election_timeout_min_ms ({}) > "
                         "raft.election_timeout_max_ms ({}), swapping",
                         min_to, max_to);
            cfg.props_["raft.election_timeout_min_ms"] = std::to_string(max_to);
            cfg.props_["raft.election_timeout_max_ms"] = std::to_string(min_to);
        }
    } catch (...) {
        // Non-numeric values handled elsewhere
    }

    return cfg;
}

// ============================================================================
// config::from_args()
//
// Parses command-line arguments to override configuration values.
//
// Supported formats:
//   --key=value       Direct assignment
//   --key value       Space-separated assignment (value consumed if not a flag)
//   --key             Boolean flag (sets key=true)
//   --no-key          Boolean flag (sets key=false)
//   -k value          Short-option shorthand
//
// Keys use dot-separated paths (e.g., --broker.id=1).
// ============================================================================

config config::from_args(int argc, char** argv) {
    config cfg;

    if (argc < 2) {
        spdlog::debug("config: no CLI arguments to parse");
        return cfg;
    }

    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);

        // ── Long option: --key=value ─────────────────────────────────────
        if (arg.starts_with("--")) {
            std::string stripped = arg.substr(2);
            auto eq_pos = stripped.find('=');
            if (eq_pos != std::string::npos) {
                std::string key = stripped.substr(0, eq_pos);
                std::string value = stripped.substr(eq_pos + 1);
                if (!key.empty()) {
                    cfg.set(key, value);
                    spdlog::debug("config: CLI --{}={}", key, value);
                }
            }
            // ── Long option: --key value or --key (flag) ─────────────────
            else {
                std::string key = stripped;
                if (key.empty()) continue;

                // Check if it's a --no-xxx negation flag
                if (key.starts_with("no-") && key.size() > 3) {
                    std::string real_key = key.substr(3);
                    cfg.set(real_key, "false");
                    spdlog::debug("config: CLI --{} -> {}=false", key, real_key);
                    continue;
                }

                // Peek at next argument — if it doesn't look like a flag,
                // treat it as the value
                if (i + 1 < argc) {
                    std::string next_arg(argv[i + 1]);
                    if (!next_arg.starts_with("-")) {
                        cfg.set(key, next_arg);
                        spdlog::debug("config: CLI --{} {}", key, next_arg);
                        ++i; // consume the value
                        continue;
                    }
                }

                // No value follows: treat as a boolean flag set to true
                cfg.set(key, "true");
                spdlog::debug("config: CLI --{} (flag -> true)", key);
            }
        }
        // ── Short option: -k value ───────────────────────────────────────
        else if (arg.starts_with("-") && arg.size() == 2 &&
                 arg[1] != '-') {
            char flag = arg[1];
            if (i + 1 < argc) {
                std::string next_arg(argv[i + 1]);
                if (!next_arg.starts_with("-")) {
                    std::string key(1, flag);
                    cfg.set(key, next_arg);
                    spdlog::debug("config: CLI -{} {}", flag, next_arg);
                    ++i; // consume the value
                }
            }
        }
        // ── Positional arguments are ignored silently ────────────────────
    }

    spdlog::info("config: parsed {} CLI argument overrides", cfg.props_.size());
    return cfg;
}

// ============================================================================
// config::get()
// ============================================================================

std::optional<std::string> config::get(const std::string& key) const {
    auto it = props_.find(key);
    if (it != props_.end()) {
        return it->second;
    }
    return std::nullopt;
}

// ============================================================================
// config::has()
// ============================================================================

bool config::has(const std::string& key) const noexcept {
    return props_.find(key) != props_.end();
}

// ============================================================================
// config::get_or()
// ============================================================================

std::string config::get_or(const std::string& key,
                           const std::string& default_val) const {
    auto it = props_.find(key);
    if (it != props_.end()) {
        return it->second;
    }
    return default_val;
}

// ============================================================================
// config::set()
// ============================================================================

void config::set(const std::string& key, const std::string& value) {
    props_[key] = value;
}

// ============================================================================
// config::get_as<T>() — Explicit template specializations
//
// Each specialization converts the stored string value to the requested type
// with proper error handling. Failed conversions return std::nullopt and
// log a warning through spdlog.
// ============================================================================

// ── int ─────────────────────────────────────────────────────────────────

template<>
std::optional<int> config::get_as<int>(const std::string& key) const {
    auto val = get(key);
    if (!val.has_value()) return std::nullopt;
    try {
        return std::stoi(val.value());
    } catch (const std::invalid_argument&) {
        spdlog::warn("config: cannot convert key '{}' value '{}' to int", key, val.value());
        return std::nullopt;
    } catch (const std::out_of_range&) {
        spdlog::warn("config: key '{}' value '{}' out of range for int", key, val.value());
        return std::nullopt;
    }
}

// ── int64_t ─────────────────────────────────────────────────────────────

template<>
std::optional<int64_t> config::get_as<int64_t>(const std::string& key) const {
    auto val = get(key);
    if (!val.has_value()) return std::nullopt;
    try {
        return std::stoll(val.value());
    } catch (const std::invalid_argument&) {
        spdlog::warn("config: cannot convert key '{}' value '{}' to int64_t",
                     key, val.value());
        return std::nullopt;
    } catch (const std::out_of_range&) {
        spdlog::warn("config: key '{}' value '{}' out of range for int64_t",
                     key, val.value());
        return std::nullopt;
    }
}

// ── bool ────────────────────────────────────────────────────────────────

template<>
std::optional<bool> config::get_as<bool>(const std::string& key) const {
    auto val = get(key);
    if (!val.has_value()) return std::nullopt;

    const auto& s = to_lower(val.value());
    // Truthy values: true, 1, yes, on, enabled
    if (s == "true" || s == "1" || s == "yes" || s == "on" || s == "enabled") {
        return true;
    }
    // Falsy values: false, 0, no, off, disabled, "" (empty)
    if (s == "false" || s == "0" || s == "no" || s == "off" ||
        s == "disabled" || s.empty()) {
        return false;
    }
    spdlog::warn("config: cannot convert key '{}' value '{}' to bool", key, s);
    return std::nullopt;
}

// ── std::string ─────────────────────────────────────────────────────────

template<>
std::optional<std::string> config::get_as<std::string>(const std::string& key) const {
    // Pass-through: all values are already stored as strings
    return get(key);
}

// ── double ──────────────────────────────────────────────────────────────

template<>
std::optional<double> config::get_as<double>(const std::string& key) const {
    auto val = get(key);
    if (!val.has_value()) return std::nullopt;
    try {
        return std::stod(val.value());
    } catch (const std::invalid_argument&) {
        spdlog::warn("config: cannot convert key '{}' value '{}' to double",
                     key, val.value());
        return std::nullopt;
    } catch (const std::out_of_range&) {
        spdlog::warn("config: key '{}' value '{}' out of range for double",
                     key, val.value());
        return std::nullopt;
    }
}

// ============================================================================
// config::seed_servers()
//
// Parses the cluster.seed_servers property (comma-separated list of
// host:port endpoints) into a vector of endpoint structs.
//
// Format examples:
//   "broker1:9092,broker2:9092,broker3:9092"
//   "10.0.0.1:9092,10.0.0.2:9092"
//   "[::1]:9092,[fe80::1]:9092"           (IPv6)
//   "single-broker"                        (defaults to port 9092)
// ============================================================================

std::vector<endpoint> config::seed_servers() const {
    std::vector<endpoint> servers;

    auto seed_str = get_or("cluster.seed_servers", "");
    if (seed_str.empty()) {
        spdlog::debug("config: no seed servers configured");
        return servers;
    }

    auto entries = split(seed_str, ',');
    servers.reserve(entries.size());

    for (const auto& entry : entries) {
        auto trimmed_entry = trim(entry);
        if (trimmed_entry.empty()) continue;

        // Find the last colon to separate host from port
        // (supports IPv6 addresses like [::1]:9092)
        auto colon_pos = trimmed_entry.rfind(':');
        if (colon_pos == std::string::npos || colon_pos == 0) {
            // No port specified — use the default port
            endpoint ep;
            ep.host = trimmed_entry;
            ep.port = kDefaultPort;
            if (!ep.host.empty()) {
                servers.push_back(std::move(ep));
                spdlog::debug("config: seed server '{}' (default port {})",
                              ep.host, kDefaultPort);
            }
        } else {
            std::string host = trim(trimmed_entry.substr(0, colon_pos));
            std::string port_str = trim(trimmed_entry.substr(colon_pos + 1));

            // Strip brackets from IPv6 addresses like [::1]:9092
            if (!host.empty() && host.front() == '[' && host.back() == ']') {
                host = host.substr(1, host.size() - 2);
            }

            if (host.empty()) {
                spdlog::warn("config: empty host in seed_servers entry '{}', skipping",
                             trimmed_entry);
                continue;
            }

            endpoint ep;
            ep.host = host;

            try {
                int p = std::stoi(port_str);
                if (p < 1 || p > 65535) {
                    spdlog::warn("config: invalid port {} in seed_servers entry '{}', "
                                 "defaulting to {}",
                                 p, trimmed_entry, kDefaultPort);
                    ep.port = kDefaultPort;
                } else {
                    ep.port = static_cast<uint16_t>(p);
                }
            } catch (const std::invalid_argument&) {
                spdlog::warn("config: cannot parse port '{}' in seed_servers entry '{}', "
                             "defaulting to {}",
                             port_str, trimmed_entry, kDefaultPort);
                ep.port = kDefaultPort;
            } catch (const std::out_of_range&) {
                spdlog::warn("config: port out of range in seed_servers entry '{}', "
                             "defaulting to {}",
                             trimmed_entry, kDefaultPort);
                ep.port = kDefaultPort;
            }

            servers.push_back(std::move(ep));
            spdlog::debug("config: seed server '{}:{}'", servers.back().host, servers.back().port);
        }
    }

    spdlog::info("config: parsed {} seed servers", servers.size());
    return servers;
}

// ============================================================================
// config::to_json()
//
// Rebuilds a nested JSON object from the flat dot-separated property map.
// String values are heuristically converted back to their natural JSON types
// (boolean, integer, float) via string_to_json_value().
// ============================================================================

nlohmann::json config::to_json() const {
    nlohmann::json root = nlohmann::json::object();

    for (const auto& [key, value] : props_) {
        json_set_nested(root, key, string_to_json_value(value));
    }

    return root;
}

// ============================================================================
// config::to_string()
//
// Produces a human-readable pretty-printed table of all configuration
// properties, sorted alphabetically by key. Includes a header with the
// total property count and aligned name=value pairs for easy scanning.
// ============================================================================

std::string config::to_string() const {
    // Collect and sort keys for deterministic, scannable output
    std::vector<std::string> keys;
    keys.reserve(props_.size());
    for (const auto& [key, _] : props_) {
        keys.push_back(key);
    }
    std::sort(keys.begin(), keys.end());

    // Compute maximum key length for column alignment
    size_t max_key_len = 0;
    for (const auto& k : keys) {
        if (k.size() > max_key_len) {
            max_key_len = k.size();
        }
    }

    constexpr size_t kLineWidth = 80;
    std::ostringstream oss;

    // Header
    oss << std::string(kLineWidth, '=') << '\n';
    oss << "  torrent-mq Configuration\n";
    oss << "  Total properties: " << props_.size() << '\n';
    oss << std::string(kLineWidth, '-') << '\n';

    // Property entries: "  key         = value"
    for (const auto& key : keys) {
        oss << "  " << key;
        // Pad for alignment: at least 2 spaces, plus difference in length
        size_t padding = (max_key_len > key.size())
                             ? (max_key_len - key.size() + 2)
                             : 2;
        oss << std::string(padding, ' ');
        oss << "= " << props_.at(key) << '\n';
    }

    oss << std::string(kLineWidth, '=') << '\n';
    return oss.str();
}

} // namespace torrent
