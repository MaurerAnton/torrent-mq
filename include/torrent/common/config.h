#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>

namespace torrent {

/**
 * Configuration system for torrent-mq.
 *
 * Configuration can be loaded from YAML/JSON files, environment variables,
 * or set programmatically. Property names use dot-separated paths
 * (e.g., "broker.id", "storage.log.dir").
 */
class config {
public:
    using property_map = std::unordered_map<std::string, std::string>;

    config() { set_defaults(); }

    /**
     * Load configuration from a JSON file.
     */
    static config from_file(const std::filesystem::path& path);

    /**
     * Load configuration from command-line arguments.
     */
    static config from_args(int argc, char** argv);

    /**
     * Get a property value by key.
     */
    [[nodiscard]] std::optional<std::string> get(const std::string& key) const;

    /**
     * Get a property value as typed T.
     */
    template<typename T>
    [[nodiscard]] std::optional<T> get_as(const std::string& key) const;

    /**
     * Get a property value with a default fallback.
     */
    [[nodiscard]] std::string get_or(const std::string& key,
                                     const std::string& default_val) const;

    /**
     * Set a property value.
     */
    void set(const std::string& key, const std::string& value);

    /**
     * Check if a property exists.
     */
    [[nodiscard]] bool has(const std::string& key) const noexcept;

    /**
     * Get all properties.
     */
    [[nodiscard]] const property_map& properties() const noexcept { return props_; }

    // ---- Typed accessors for common properties ----

    [[nodiscard]] int32_t broker_id() const {
        return std::stoi(get_or("broker.id", "0"));
    }

    [[nodiscard]] uint16_t port() const {
        return static_cast<uint16_t>(std::stoi(get_or("listeners.plain.port", "9092")));
    }

    [[nodiscard]] uint16_t tls_port() const {
        return static_cast<uint16_t>(std::stoi(get_or("listeners.tls.port", "9093")));
    }

    [[nodiscard]] uint16_t admin_port() const {
        return static_cast<uint16_t>(std::stoi(get_or("admin.port", "9644")));
    }

    [[nodiscard]] uint16_t metrics_port() const {
        return static_cast<uint16_t>(std::stoi(get_or("metrics.port", "9090")));
    }

    [[nodiscard]] std::string log_dir() const {
        return get_or("storage.log.dir", "/var/lib/torrent/data");
    }

    [[nodiscard]] int32_t num_io_threads() const {
        return std::stoi(get_or("network.num_io_threads", "4"));
    }

    [[nodiscard]] int32_t num_worker_threads() const {
        return std::stoi(get_or("network.num_worker_threads", "8"));
    }

    [[nodiscard]] int32_t default_replication_factor() const {
        return std::stoi(get_or("topic.default.replication_factor", "3"));
    }

    [[nodiscard]] int32_t default_partitions() const {
        return std::stoi(get_or("topic.default.partitions", "1"));
    }

    [[nodiscard]] int64_t segment_max_bytes() const {
        return std::stoll(get_or("storage.segment.bytes", "1073741824"));
    }

    [[nodiscard]] int64_t segment_max_age_ms() const {
        return std::stoll(get_or("storage.segment.max_age_ms", "604800000"));
    }

    [[nodiscard]] int64_t retention_bytes() const {
        return std::stoll(get_or("storage.retention.bytes", "-1"));
    }

    [[nodiscard]] int64_t retention_ms() const {
        return std::stoll(get_or("storage.retention.ms", "604800000"));
    }

    [[nodiscard]] int64_t raft_heartbeat_ms() const {
        return std::stoll(get_or("raft.heartbeat_interval_ms", "150"));
    }

    [[nodiscard]] int64_t raft_election_timeout_min_ms() const {
        return std::stoll(get_or("raft.election_timeout_min_ms", "150"));
    }

    [[nodiscard]] int64_t raft_election_timeout_max_ms() const {
        return std::stoll(get_or("raft.election_timeout_max_ms", "300"));
    }

    [[nodiscard]] bool enable_sasl() const {
        return get_or("security.sasl.enabled", "false") == "true";
    }

    [[nodiscard]] bool enable_tls() const {
        return get_or("security.tls.enabled", "false") == "true";
    }

    [[nodiscard]] std::string tls_cert_path() const {
        return get_or("security.tls.certificate_path", "");
    }

    [[nodiscard]] std::string tls_key_path() const {
        return get_or("security.tls.key_path", "");
    }

    [[nodiscard]] std::string tls_ca_path() const {
        return get_or("security.tls.ca_path", "");
    }

    [[nodiscard]] std::vector<endpoint> seed_servers() const;

    [[nodiscard]] std::string cluster_id() const {
        return get_or("cluster.id", "torrent-cluster");
    }

    [[nodiscard]] bool auto_create_topics() const {
        return get_or("topic.auto_create", "true") == "true";
    }

    [[nodiscard]] int32_t max_connections() const {
        return std::stoi(get_or("network.max_connections", "65536"));
    }

    [[nodiscard]] int64_t socket_send_buffer_bytes() const {
        return std::stoll(get_or("network.socket.send_buffer.bytes", "1048576"));
    }

    [[nodiscard]] int64_t socket_recv_buffer_bytes() const {
        return std::stoll(get_or("network.socket.recv_buffer.bytes", "1048576"));
    }

    [[nodiscard]] int64_t max_request_size() const {
        return std::stoll(get_or("network.max_request_size", "104857600"));
    }

    [[nodiscard]] int64_t fetch_max_bytes() const {
        return std::stoll(get_or("consumer.fetch.max_bytes", "57671680"));
    }

    [[nodiscard]] int32_t fetch_min_bytes() const {
        return std::stoi(get_or("consumer.fetch.min_bytes", "1"));
    }

    [[nodiscard]] int32_t fetch_max_wait_ms() const {
        return std::stoi(get_or("consumer.fetch.max_wait_ms", "500"));
    }

    [[nodiscard]] int64_t message_max_bytes() const {
        return std::stoll(get_or("topic.max.message.bytes", "1048588"));
    }

    [[nodiscard]] compression_type default_compression() const {
        auto v = get_or("topic.compression.type", "none");
        if (v == "gzip") return compression_type::gzip;
        if (v == "snappy") return compression_type::snappy;
        if (v == "lz4") return compression_type::lz4;
        if (v == "zstd") return compression_type::zstd;
        return compression_type::none;
    }

    [[nodiscard]] bool enable_transactions() const {
        return get_or("transaction.enabled", "true") == "true";
    }

    [[nodiscard]] int64_t transaction_timeout_ms() const {
        return std::stoll(get_or("transaction.timeout.ms", "60000"));
    }

    [[nodiscard]] int64_t transactional_id_expiration_ms() const {
        return std::stoll(get_or("transaction.id.expiration.ms", "604800000"));
    }

    /**
     * Dump current configuration as JSON.
     */
    [[nodiscard]] nlohmann::json to_json() const;

    /**
     * Dump current configuration as human-readable string.
     */
    [[nodiscard]] std::string to_string() const;

private:
    void set_defaults();
    void parse_property(const std::string& key, const nlohmann::json& value,
                        const std::string& prefix = "");

    property_map props_;
};

} // namespace torrent
