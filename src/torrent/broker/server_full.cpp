#include "torrent/broker/server.h"
#include "torrent/common/config.h"
#include "torrent/common/types.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/broker/partition_manager.h"
#include "torrent/broker/consumer_group_manager.h"
#include "torrent/broker/request_dispatcher.h"
#include "torrent/broker/controller.h"
#include "torrent/broker/quota_manager.h"
#include "torrent/broker/leader_balancer.h"
#include "torrent/security/auth_manager.h"
#include "torrent/security/acl_engine.h"
#include <spdlog/spdlog.h>
#include <csignal>
#include <atomic>
#include <thread>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <deque>
#include <algorithm>

namespace torrent::broker {
namespace {

// ============================================================================
// Dynamic configuration reload (SIGHUP)
// ============================================================================

class DynamicConfigManager {
public:
    explicit DynamicConfigManager(config& cfg) : config_(cfg) {}

    void reload_from_file(const std::string& path) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto new_cfg = config::from_file(path);
        apply_dynamic_changes(new_cfg);
        spdlog::info("Configuration reloaded from {}", path);
    }

    bool is_dynamic_property(const std::string& key) {
        static const std::unordered_set<std::string> dynamic_props = {
            "network.max_connections",
            "network.num_worker_threads",
            "topic.auto_create",
            "quota.default.produce.bps",
            "quota.default.fetch.bps",
            "quota.default.request.rate",
            "raft.heartbeat_interval_ms",
            "consumer.fetch.max_bytes",
            "consumer.fetch.max_wait_ms",
        };
        return dynamic_props.find(key) != dynamic_props.end();
    }

private:
    void apply_dynamic_changes(const config& new_cfg) {
        for (const auto& [key, value] : new_cfg.properties()) {
            if (is_dynamic_property(key)) {
                config_.set(key, value);
                spdlog::info("Applied dynamic config change: {} = {}", key, value);
            } else {
                spdlog::warn("Config key '{}' requires restart to change", key);
            }
        }
    }

    config& config_;
    std::mutex mutex_;
};

// ============================================================================
// Graceful leadership transfer before shutdown
// ============================================================================

class ShutdownCoordinator {
public:
    struct ShutdownConfig {
        std::chrono::milliseconds drain_timeout{30000};
        std::chrono::milliseconds leadership_transfer_timeout{10000};
        std::chrono::milliseconds shutdown_timeout{60000};
        bool transfer_leadership{true};
    };

    explicit ShutdownCoordinator(const ShutdownConfig& cfg) : config_(cfg) {}

    enum class Phase {
        PREPARE,
        TRANSFER_LEADERSHIP,
        DRAIN_REQUESTS,
        STOP_ACCEPTORS,
        FLUSH_STORAGE,
        STOP_CONSENSUS,
        COMPLETE
    };

    void start_shutdown() {
        phase_.store(Phase::PREPARE);
        spdlog::info("Shutdown phase: PREPARE");
    }

    void advance_to(Phase phase) {
        phase_.store(phase);
        spdlog::info("Shutdown phase: {}", phase_name(phase));
    }

    Phase current_phase() const { return phase_.load(); }

    bool should_transfer_leadership() const { return config_.transfer_leadership; }

    std::chrono::milliseconds drain_timeout() const { return config_.drain_timeout; }
    std::chrono::milliseconds leadership_timeout() const {
        return config_.leadership_transfer_timeout;
    }

private:
    static const char* phase_name(Phase p) {
        switch (p) {
        case Phase::PREPARE: return "PREPARE";
        case Phase::TRANSFER_LEADERSHIP: return "TRANSFER_LEADERSHIP";
        case Phase::DRAIN_REQUESTS: return "DRAIN_REQUESTS";
        case Phase::STOP_ACCEPTORS: return "STOP_ACCEPTORS";
        case Phase::FLUSH_STORAGE: return "FLUSH_STORAGE";
        case Phase::STOP_CONSENSUS: return "STOP_CONSENSUS";
        case Phase::COMPLETE: return "COMPLETE";
        default: return "UNKNOWN";
        }
    }

    ShutdownConfig config_;
    std::atomic<Phase> phase_{Phase::PREPARE};
};

// ============================================================================
// Partition reassignment coordinator
// ============================================================================

struct ReassignmentTask {
    std::string topic;
    partition_id_t partition;
    std::vector<broker_id_t> target_replicas;
    ReassignmentState state;
    std::chrono::steady_clock::time_point started_at;
};

enum class ReassignmentState {
    PENDING,
    IN_PROGRESS,
    COMPLETED,
    FAILED,
    CANCELLED
};

class ReassignmentCoordinator {
public:
    void add_reassignment(const std::string& topic, partition_id_t partition,
                           const std::vector<broker_id_t>& targets) {
        std::lock_guard<std::mutex> lock(mutex_);
        ReassignmentTask task{topic, partition, targets,
                              ReassignmentState::PENDING,
                              std::chrono::steady_clock::now()};
        tasks_.push_back(task);
        spdlog::info("Reassignment queued: {}/{} -> {}", topic, partition,
                     fmt::join(targets, ","));
    }

    std::vector<ReassignmentTask> pending_tasks() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<ReassignmentTask> result;
        for (const auto& t : tasks_) {
            if (t.state == ReassignmentState::PENDING) {
                result.push_back(t);
            }
        }
        return result;
    }

    void mark_in_progress(const std::string& topic, partition_id_t partition) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& t : tasks_) {
            if (t.topic == topic && t.partition == partition) {
                t.state = ReassignmentState::IN_PROGRESS;
                break;
            }
        }
    }

    void mark_completed(const std::string& topic, partition_id_t partition) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& t : tasks_) {
            if (t.topic == topic && t.partition == partition) {
                t.state = ReassignmentState::COMPLETED;
                break;
            }
        }
    }

private:
    std::vector<ReassignmentTask> tasks_;
    mutable std::mutex mutex_;
};

// ============================================================================
// Delegation token management
// ============================================================================

struct DelegationToken {
    std::string token_id;
    std::string hmac;
    std::string owner;
    std::string renewers;
    int64_t issue_timestamp_ms;
    int64_t max_lifetime_ms;
    int64_t expiry_timestamp_ms;
    bool expired;
};

class DelegationTokenManager {
public:
    std::string create_token(const std::string& owner,
                              const std::string& renewers,
                              int64_t max_lifetime_ms) {
        std::lock_guard<std::mutex> lock(mutex_);

        DelegationToken token;
        token.token_id = generate_token_id();
        token.owner = owner;
        token.renewers = renewers;
        token.issue_timestamp_ms = now_ms();
        token.max_lifetime_ms = max_lifetime_ms;
        token.expiry_timestamp_ms = token.issue_timestamp_ms + max_lifetime_ms;
        token.expired = false;
        token.hmac = compute_hmac(token.token_id, token.expiry_timestamp_ms);

        tokens_[token.token_id] = token;
        spdlog::info("Delegation token created: {} for {}", token.token_id, owner);
        return token.token_id;
    }

    bool renew_token(const std::string& token_id, int64_t renew_period_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = tokens_.find(token_id);
        if (it == tokens_.end() || it->second.expired) return false;

        int64_t new_expiry = std::min(
            it->second.issue_timestamp_ms + it->second.max_lifetime_ms,
            now_ms() + renew_period_ms);

        if (new_expiry <= now_ms()) return false;
        it->second.expiry_timestamp_ms = new_expiry;
        return true;
    }

    bool expire_token(const std::string& token_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = tokens_.find(token_id);
        if (it == tokens_.end()) return false;
        it->second.expired = true;
        return true;
    }

    void cleanup_expired() {
        std::lock_guard<std::mutex> lock(mutex_);
        int64_t now = now_ms();
        for (auto it = tokens_.begin(); it != tokens_.end();) {
            if (it->second.expired || it->second.expiry_timestamp_ms < now) {
                it = tokens_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    std::string generate_token_id() {
        static std::atomic<int64_t> counter{0};
        return "dt_" + std::to_string(counter.fetch_add(1));
    }

    std::string compute_hmac(const std::string& token_id, int64_t expiry) {
        return "hmac_" + token_id + "_" + std::to_string(expiry);
    }

    static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    std::unordered_map<std::string, DelegationToken> tokens_;
    mutable std::mutex mutex_;
};

// ============================================================================
// SCRAM credential management
// ============================================================================

struct ScramCredential {
    std::string username;
    std::string salt;
    std::string stored_key;
    std::string server_key;
    int32_t iterations;
};

class ScramCredentialManager {
public:
    void upsert_user(const std::string& username,
                      const std::string& salt,
                      const std::string& stored_key,
                      const std::string& server_key,
                      int32_t iterations) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        ScramCredential cred{username, salt, stored_key, server_key, iterations};
        credentials_[username] = cred;
    }

    std::optional<ScramCredential> get_user(const std::string& username) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = credentials_.find(username);
        if (it == credentials_.end()) return std::nullopt;
        return it->second;
    }

    std::vector<std::string> list_users() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        std::vector<std::string> users;
        for (const auto& [user, _] : credentials_) {
            users.push_back(user);
        }
        return users;
    }

    bool delete_user(const std::string& username) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        return credentials_.erase(username) > 0;
    }

private:
    std::unordered_map<std::string, ScramCredential> credentials_;
    mutable std::shared_mutex mutex_;
};

// ============================================================================
// Connection throttling per IP
// ============================================================================

class ConnectionThrottler {
public:
    struct Config {
        int max_connections_per_ip{100};
        int max_connection_rate_per_sec{50};
        int throttle_window_sec{10};
    };

    explicit ConnectionThrottler(const Config& cfg) : config_(cfg) {}

    bool allow_connection(const std::string& ip) {
        std::lock_guard<std::mutex> lock(mutex_);
        cleanup_stale();

        auto& entry = entries_[ip];
        entry.request_times.push_back(now_ms());

        // Rate limiting
        int64_t window_start = now_ms() - (config_.throttle_window_sec * 1000);
        while (!entry.request_times.empty() &&
               entry.request_times.front() < window_start) {
            entry.request_times.pop_front();
        }

        if (static_cast<int>(entry.request_times.size()) >
            config_.max_connection_rate_per_sec * config_.throttle_window_sec) {
            spdlog::warn("Connection rate limit exceeded for {}", ip);
            return false;
        }

        // Connection count limiting
        if (entry.active_connections >= config_.max_connections_per_ip) {
            spdlog::warn("Max connections per IP exceeded for {}", ip);
            return false;
        }

        entry.active_connections++;
        return true;
    }

    void release_connection(const std::string& ip) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(ip);
        if (it != entries_.end() && it->second.active_connections > 0) {
            it->second.active_connections--;
        }
    }

private:
    struct IpEntry {
        int active_connections{0};
        std::deque<int64_t> request_times;
    };

    void cleanup_stale() {
        int64_t threshold = now_ms() - 60000;  // 1 minute
        for (auto it = entries_.begin(); it != entries_.end();) {
            if (it->second.active_connections == 0 &&
                (it->second.request_times.empty() ||
                 it->second.request_times.back() < threshold)) {
                it = entries_.erase(it);
            } else {
                ++it;
            }
        }
    }

    static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    Config config_;
    std::unordered_map<std::string, IpEntry> entries_;
    mutable std::mutex mutex_;
};

// ============================================================================
// Request prioritization
// ============================================================================

enum class RequestPriority {
    ADMIN = 0,
    PRODUCE = 1,
    FETCH = 2,
    OFFSET = 3,
    METADATA = 4,
    GROUP = 5,
    OTHER = 6
};

class RequestPrioritizer {
public:
    RequestPriority classify(int16_t api_key) {
        switch (api_key) {
        case 19: case 20: case 29: case 30: case 31:
        case 32: case 33: case 37: case 42: case 43: case 44:
            return RequestPriority::ADMIN;
        case 0:
            return RequestPriority::PRODUCE;
        case 1:
            return RequestPriority::FETCH;
        case 2: case 8: case 9: case 47:
            return RequestPriority::OFFSET;
        case 3: case 18:
            return RequestPriority::METADATA;
        case 11: case 12: case 13: case 14: case 15: case 16:
            return RequestPriority::GROUP;
        default:
            return RequestPriority::OTHER;
        }
    }

    int weight(RequestPriority prio) {
        switch (prio) {
        case RequestPriority::ADMIN: return 1;
        case RequestPriority::PRODUCE: return 2;
        case RequestPriority::FETCH: return 3;
        case RequestPriority::OFFSET: return 4;
        case RequestPriority::METADATA: return 5;
        case RequestPriority::GROUP: return 6;
        case RequestPriority::OTHER: return 7;
        }
        return 8;
    }
};

// ============================================================================
// Request logging for audit
// ============================================================================

class RequestAuditLogger {
public:
    struct AuditEntry {
        int64_t timestamp_ms;
        std::string client_ip;
        std::string principal;
        int16_t api_key;
        int32_t correlation_id;
        int64_t processing_time_us;
        error_code result;
    };

    void log_request(const AuditEntry& entry) {
        std::lock_guard<std::mutex> lock(mutex_);
        recent_entries_.push_back(entry);
        if (recent_entries_.size() > max_entries_) {
            recent_entries_.pop_front();
        }

        spdlog::info("[AUDIT] client={} principal={} api_key={} corr_id={} "
                     "time_us={} result={}",
                     entry.client_ip, entry.principal, entry.api_key,
                     entry.correlation_id, entry.processing_time_us,
                     static_cast<int16_t>(entry.result));
    }

    std::vector<AuditEntry> recent_entries(int limit = 100) const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<AuditEntry> result;
        auto it = recent_entries_.rbegin();
        for (int i = 0; i < limit && it != recent_entries_.rend(); i++, it++) {
            result.push_back(*it);
        }
        return result;
    }

private:
    static constexpr size_t max_entries_ = 10000;
    std::deque<AuditEntry> recent_entries_;
    mutable std::mutex mutex_;
};

}  // anonymous namespace
}  // namespace torrent::broker
