#include "torrent/network/protocol.h"
#include "torrent/network/message_codec.h"
#include "torrent/network/transport.h"
#include "torrent/network/connection.h"
#include "torrent/common/types.h"
#include "torrent/common/endian.h"
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <algorithm>
#include <random>
#include <sstream>

namespace torrent::network {
namespace {

// ============================================================================
// Connection Lifecycle Management
// ============================================================================

class ConnectionLifecycleManager {
public:
    struct ConnInfo {
        uint64_t id;
        endpoint peer;
        std::string client_id;
        std::string principal;
        ConnectionState state{ConnectionState::handshaking};
        int64_t created_at_ms;
        int64_t last_activity_ms;
        int64_t bytes_received{0};
        int64_t bytes_sent{0};
        int32_t inflight_requests{0};
        bool is_inter_broker{false};
    };

    void register_connection(const ConnInfo& info) {
        std::lock_guard<std::mutex> lock(mutex_);
        connections_[info.id] = info;
        active_count_.fetch_add(1);
    }

    void unregister_connection(uint64_t id) {
        std::lock_guard<std::mutex> lock(mutex_);
        connections_.erase(id);
        active_count_.fetch_sub(1);
    }

    void update_activity(uint64_t id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = connections_.find(id);
        if (it != connections_.end()) {
            it->second.last_activity_ms = now_ms();
        }
    }

    std::vector<uint64_t> idle_connections(int64_t idle_threshold_ms) const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<uint64_t> idle;
        int64_t now = now_ms();
        for (const auto& [id, info] : connections_) {
            if (now - info.last_activity_ms > idle_threshold_ms) {
                idle.push_back(id);
            }
        }
        return idle;
    }

    size_t count_by_state(ConnectionState state) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return std::count_if(connections_.begin(), connections_.end(),
                              [state](const auto& p) {
                                  return p.second.state == state;
                              });
    }

    int64_t active_count() const { return active_count_.load(); }

private:
    static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, ConnInfo> connections_;
    std::atomic<int64_t> active_count_{0};
};

// ============================================================================
// Request Batching for Throughput Optimization
// ============================================================================

class RequestBatcher {
public:
    struct BatchConfig {
        int max_batch_size{100};
        int max_batch_bytes{1048576};
        int max_linger_ms{5};
    };

    explicit RequestBatcher(const BatchConfig& cfg) : config_(cfg) {}

    void enqueue(int16_t api_key, buffer_view request, int32_t correlation_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.push_back({api_key, correlation_id,
                            std::vector<uint8_t>(request.data,
                                                  request.data + request.size)});
        total_bytes_ += request.size;

        if (should_flush()) {
            flush_pending();
        }
    }

    std::vector<std::pair<int32_t, std::vector<uint8_t>>> flush() {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::pair<int32_t, std::vector<uint8_t>>> result;
        for (auto& item : pending_) {
            result.emplace_back(item.correlation_id, std::move(item.data));
        }
        pending_.clear();
        total_bytes_ = 0;
        return result;
    }

private:
    bool should_flush() const {
        return static_cast<int>(pending_.size()) >= config_.max_batch_size ||
               total_bytes_ >= config_.max_batch_bytes;
    }

    struct PendingRequest {
        int16_t api_key;
        int32_t correlation_id;
        std::vector<uint8_t> data;
    };

    BatchConfig config_;
    std::vector<PendingRequest> pending_;
    int total_bytes_{0};
    mutable std::mutex mutex_;
};

// ============================================================================
// Response Compressor
// ============================================================================

class ResponseCompressor {
public:
    static std::vector<uint8_t> compress(buffer_view data, compression_type type) {
        switch (type) {
        case compression_type::none:
            return std::vector<uint8_t>(data.data, data.data + data.size);
        case compression_type::lz4:
            return compress_lz4(data);
        case compression_type::zstd:
            return compress_zstd(data);
        case compression_type::snappy:
            return compress_snappy(data);
        case compression_type::gzip:
            return compress_gzip(data);
        default:
            return std::vector<uint8_t>(data.data, data.data + data.size);
        }
    }

private:
    static std::vector<uint8_t> compress_lz4(buffer_view data) {
        // In production: LZ4_compress_default
        std::vector<uint8_t> result(data.data, data.data + data.size);
        return result;
    }

    static std::vector<uint8_t> compress_zstd(buffer_view data) {
        // In production: ZSTD_compress
        std::vector<uint8_t> result(data.data, data.data + data.size);
        return result;
    }

    static std::vector<uint8_t> compress_snappy(buffer_view data) {
        // In production: snappy::Compress
        std::vector<uint8_t> result(data.data, data.data + data.size);
        return result;
    }

    static std::vector<uint8_t> compress_gzip(buffer_view data) {
        // In production: deflate
        std::vector<uint8_t> result(data.data, data.data + data.size);
        return result;
    }
};

// ============================================================================
// Connection Pool with Health Checking
// ============================================================================

class ConnectionHealthChecker {
public:
    struct HealthConfig {
        int64_t check_interval_ms{30000};
        int64_t connect_timeout_ms{5000};
        int max_failures{3};
        int64_t cooldown_ms{60000};
    };

    struct BrokerHealth {
        endpoint addr;
        bool healthy{true};
        int consecutive_failures{0};
        int64_t last_check_ms{0};
        int64_t last_failure_ms{0};
        int64_t cooldown_until_ms{0};
    };

    explicit ConnectionHealthChecker(const HealthConfig& cfg) : config_(cfg) {}

    void mark_success(const endpoint& addr) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& health = health_map_[addr];
        health.healthy = true;
        health.consecutive_failures = 0;
        health.last_check_ms = now_ms();
    }

    void mark_failure(const endpoint& addr) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& health = health_map_[addr];
        health.last_check_ms = now_ms();
        health.last_failure_ms = now_ms();
        health.consecutive_failures++;

        if (health.consecutive_failures >= config_.max_failures) {
            health.healthy = false;
            health.cooldown_until_ms = now_ms() + config_.cooldown_ms;
            spdlog::warn("Broker {} marked unhealthy after {} consecutive failures",
                        addr.to_string(), health.consecutive_failures);
        }
    }

    bool is_healthy(const endpoint& addr) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = health_map_.find(addr);
        if (it == health_map_.end()) return true;

        if (!it->second.healthy && now_ms() > it->second.cooldown_until_ms) {
            return true; // Cooldown expired, try again
        }
        return it->second.healthy;
    }

private:
    static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    HealthConfig config_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, BrokerHealth> health_map_;
};

// ============================================================================
// Message Interceptor Pipeline
// ============================================================================

class MessageInterceptor {
public:
    using InterceptFn = std::function<buffer_view(buffer_view)>;

    void add_interceptor(int16_t api_key, InterceptFn fn) {
        std::lock_guard<std::mutex> lock(mutex_);
        interceptors_[api_key].push_back(fn);
    }

    buffer_view intercept(int16_t api_key, buffer_view data) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = interceptors_.find(api_key);
        if (it == interceptors_.end()) return data;

        buffer_view current = data;
        for (auto& fn : it->second) {
            current = fn(current);
        }
        return current;
    }

private:
    std::unordered_map<int16_t, std::vector<InterceptFn>> interceptors_;
    mutable std::mutex mutex_;
};

// ============================================================================
// API Version Compatibility Matrix
// ============================================================================

struct ApiVersionInfo {
    int16_t api_key;
    const char* name;
    int16_t min_version;
    int16_t max_version;
    bool is_kafka_compat;
};

static const ApiVersionInfo kApiVersionMatrix[] = {
    {0, "Produce", 0, 9, true},
    {1, "Fetch", 0, 13, true},
    {2, "ListOffsets", 0, 7, true},
    {3, "Metadata", 0, 12, true},
    {4, "LeaderAndIsr", 0, 5, true},
    {5, "StopReplica", 0, 3, true},
    {6, "UpdateMetadata", 0, 7, true},
    {7, "ControlledShutdown", 0, 3, true},
    {8, "OffsetCommit", 0, 8, true},
    {9, "OffsetFetch", 0, 8, true},
    {10, "FindCoordinator", 0, 4, true},
    {11, "JoinGroup", 0, 9, true},
    {12, "Heartbeat", 0, 4, true},
    {13, "LeaveGroup", 0, 5, true},
    {14, "SyncGroup", 0, 5, true},
    {15, "DescribeGroups", 0, 5, true},
    {16, "ListGroups", 0, 4, true},
    {17, "SaslHandshake", 0, 1, true},
    {18, "ApiVersions", 0, 3, true},
    {19, "CreateTopics", 0, 7, true},
    {20, "DeleteTopics", 0, 6, true},
    {21, "DeleteRecords", 0, 2, true},
    {22, "InitProducerId", 0, 4, true},
    {23, "OffsetForLeaderEpoch", 0, 4, true},
    {24, "AddPartitionsToTxn", 0, 3, true},
    {25, "AddOffsetsToTxn", 0, 1, true},
    {26, "EndTxn", 0, 3, true},
    {27, "WriteTxnMarkers", 0, 1, true},
    {28, "TxnOffsetCommit", 0, 3, true},
    {29, "DescribeAcls", 0, 2, true},
    {30, "CreateAcls", 0, 2, true},
    {31, "DeleteAcls", 0, 2, true},
    {32, "DescribeConfigs", 0, 4, true},
    {33, "AlterConfigs", 0, 2, true},
    {36, "SaslAuthenticate", 0, 2, true},
    {37, "CreatePartitions", 0, 3, true},
    {42, "DeleteGroups", 0, 2, true},
    {43, "ElectLeaders", 0, 2, true},
    {44, "IncrementalAlterConfigs", 0, 2, true},
    {47, "OffsetDelete", 0, 0, true},
    {60, "TorrentFetch", 0, 0, false},
    {61, "TorrentBatchProduce", 0, 0, false},
    {62, "TorrentStreamSubscribe", 0, 0, false},
    {63, "TorrentStreamUnsubscribe", 0, 0, false},
    {64, "TorrentHealthCheck", 0, 0, false},
    {65, "TorrentClusterState", 0, 0, false},
    {66, "TorrentSchemaGet", 0, 0, false},
    {67, "TorrentSchemaSet", 0, 0, false},
};

constexpr size_t kApiVersionCount = sizeof(kApiVersionMatrix) / sizeof(kApiVersionMatrix[0]);

class ApiVersionManager {
public:
    bool is_version_supported(int16_t api_key, int16_t version) const {
        for (size_t i = 0; i < kApiVersionCount; i++) {
            if (kApiVersionMatrix[i].api_key == api_key) {
                return version >= kApiVersionMatrix[i].min_version &&
                       version <= kApiVersionMatrix[i].max_version;
            }
        }
        return false;
    }

    std::vector<ApiVersionRange> supported_versions() const {
        std::vector<ApiVersionRange> result;
        for (size_t i = 0; i < kApiVersionCount; i++) {
            ApiVersionRange range;
            range.api_key = kApiVersionMatrix[i].api_key;
            range.min_version = kApiVersionMatrix[i].min_version;
            range.max_version = kApiVersionMatrix[i].max_version;
            result.push_back(range);
        }
        return result;
    }
};

} // anonymous namespace
} // namespace torrent::network
