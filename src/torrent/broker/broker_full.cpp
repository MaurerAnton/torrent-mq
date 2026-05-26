#include "torrent/broker/server.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/broker/partition_manager.h"
#include "torrent/broker/consumer_group_manager.h"
#include "torrent/broker/request_dispatcher.h"
#include "torrent/broker/inter_broker.h"
#include "torrent/broker/controller.h"
#include "torrent/broker/transaction_coordinator.h"
#include "torrent/broker/quota_manager.h"
#include "torrent/broker/leader_balancer.h"
#include "torrent/common/types.h"
#include "torrent/common/config.h"
#include "torrent/common/thread_pool.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <thread>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <random>

namespace torrent::broker {
namespace {

// ============================================================================
// Full Server Lifecycle (18-phase startup)
// ============================================================================
class ServerLifecycle {
public:
    enum class Phase {
        UNINITIALIZED, LOADING_CONFIG, INIT_THREADS, INIT_STORAGE, INIT_METRICS,
        INIT_RAFT, INIT_TOPICS, INIT_PARTITIONS, INIT_GROUPS, INIT_TXN,
        INIT_SCHEMA, INIT_DISPATCHER, REGISTER_HANDLERS, START_ACCEPTORS,
        START_ADMIN, START_METRICS, START_INTER_BROKER, START_CONTROLLER,
        START_BACKGROUND, RUNNING, SHUTTING_DOWN, STOPPED
    };

    void transition(Phase from, Phase to) {
        spdlog::info("Server phase: {} -> {}", phase_name(from), phase_name(to));
        current_.store(to);
    }
    Phase current() const { return current_.load(); }

private:
    static const char* phase_name(Phase p) {
        switch(p) {
        case Phase::UNINITIALIZED: return "UNINITIALIZED";
        case Phase::LOADING_CONFIG: return "LOADING_CONFIG";
        case Phase::INIT_THREADS: return "INIT_THREADS";
        case Phase::INIT_STORAGE: return "INIT_STORAGE";
        case Phase::INIT_METRICS: return "INIT_METRICS";
        case Phase::INIT_RAFT: return "INIT_RAFT";
        case Phase::INIT_TOPICS: return "INIT_TOPICS";
        case Phase::INIT_PARTITIONS: return "INIT_PARTITIONS";
        case Phase::INIT_GROUPS: return "INIT_GROUPS";
        case Phase::INIT_TXN: return "INIT_TXN";
        case Phase::INIT_SCHEMA: return "INIT_SCHEMA";
        case Phase::INIT_DISPATCHER: return "INIT_DISPATCHER";
        case Phase::REGISTER_HANDLERS: return "REGISTER_HANDLERS";
        case Phase::START_ACCEPTORS: return "START_ACCEPTORS";
        case Phase::START_ADMIN: return "START_ADMIN";
        case Phase::START_METRICS: return "START_METRICS";
        case Phase::START_INTER_BROKER: return "START_INTER_BROKER";
        case Phase::START_CONTROLLER: return "START_CONTROLLER";
        case Phase::START_BACKGROUND: return "START_BACKGROUND";
        case Phase::RUNNING: return "RUNNING";
        case Phase::SHUTTING_DOWN: return "SHUTTING_DOWN";
        case Phase::STOPPED: return "STOPPED";
        default: return "UNKNOWN";
        }
    }
    std::atomic<Phase> current_{Phase::UNINITIALIZED};
};

// ============================================================================
// Topic Manager: Topic name validation
// ============================================================================
class TopicValidator {
public:
    static result<void> validate_name(const std::string& name) {
        if (name.empty()) return result<void>::failure(error_code::invalid_topic_exception, "Topic name cannot be empty");
        if (name.size() > 249) return result<void>::failure(error_code::invalid_topic_exception, "Topic name exceeds 249 characters");
        if (name == "." || name == "..") return result<void>::failure(error_code::invalid_topic_exception, "Invalid topic name");
        for (char c : name) {
            if (c < 32 || c > 126) return result<void>::failure(error_code::invalid_topic_exception, "Invalid character in topic name");
        }
        return result<void>::success();
    }
    static result<void> validate_partitions(int32_t partitions) {
        if (partitions < 1) return result<void>::failure(error_code::invalid_partitions, "Partitions must be >= 1");
        if (partitions > 100000) return result<void>::failure(error_code::invalid_partitions, "Partition count exceeds maximum");
        return result<void>::success();
    }
    static result<void> validate_replication(int32_t rf, int32_t broker_count) {
        if (rf < 1) return result<void>::failure(error_code::invalid_replication_factor, "RF must be >= 1");
        if (rf > broker_count) return result<void>::failure(error_code::invalid_replication_factor, "RF exceeds broker count");
        if (rf > 10) return result<void>::failure(error_code::invalid_replication_factor, "RF exceeds maximum of 10");
        return result<void>::success();
    }
};

// ============================================================================
// Partition Manager: ISR management
// ============================================================================
class IsrManager {
public:
    void shrink_isr(std::vector<broker_id_t>& isr, broker_id_t failed) {
        auto it = std::find(isr.begin(), isr.end(), failed);
        if (it != isr.end()) {
            isr.erase(it);
            spdlog::warn("ISR shrunk: removed broker {}", failed);
        }
    }
    void expand_isr(std::vector<broker_id_t>& isr, broker_id_t recovered) {
        if (std::find(isr.begin(), isr.end(), recovered) == isr.end()) {
            isr.push_back(recovered);
            spdlog::info("ISR expanded: added broker {}", recovered);
        }
    }
    bool is_under_replicated(const std::vector<broker_id_t>& replicas, const std::vector<broker_id_t>& isr) {
        return isr.size() < replicas.size();
    }
};

// ============================================================================
// Consumer Group: State machine
// ============================================================================
class GroupStateMachine {
public:
    enum State { EMPTY, STABLE, PREPARING_REBALANCE, COMPLETING_REBALANCE, DEAD };
    static const char* state_name(State s) {
        switch(s) {
        case EMPTY: return "Empty"; case STABLE: return "Stable";
        case PREPARING_REBALANCE: return "PreparingRebalance";
        case COMPLETING_REBALANCE: return "CompletingRebalance";
        case DEAD: return "Dead"; default: return "Unknown";
        }
    }
    static State transition(State current, const std::string& event) {
        if (event == "join" && current == EMPTY) return STABLE;
        if (event == "join" && current == STABLE) return PREPARING_REBALANCE;
        if (event == "sync" && current == COMPLETING_REBALANCE) return STABLE;
        if (event == "leave_last" && current == STABLE) return EMPTY;
        if (event == "leave" && current == STABLE) return PREPARING_REBALANCE;
        if (event == "timeout") return DEAD;
        return current;
    }
};

// ============================================================================
// Transaction Coordinator: State machine
// ============================================================================
enum class TxnState { ONGOING, PREPARE_COMMIT, PREPARE_ABORT, COMPLETE_COMMIT, COMPLETE_ABORT, EMPTY };
static const char* txn_state_name(TxnState s) {
    switch(s) {
    case TxnState::ONGOING: return "Ongoing";
    case TxnState::PREPARE_COMMIT: return "PrepareCommit";
    case TxnState::PREPARE_ABORT: return "PrepareAbort";
    case TxnState::COMPLETE_COMMIT: return "CompleteCommit";
    case TxnState::COMPLETE_ABORT: return "CompleteAbort";
    case TxnState::EMPTY: return "Empty";
    default: return "Unknown";
    }
}

// ============================================================================
// Controller: Rack-aware assignment
// ============================================================================
class RackAwareAssigner {
public:
    struct Broker { broker_id_t id; std::string rack; };
    std::vector<broker_id_t> assign(int32_t rf, const std::vector<Broker>& brokers) {
        std::vector<broker_id_t> result;
        std::unordered_map<std::string, int> rack_used;
        for (int32_t i = 0; i < rf && result.size() < static_cast<size_t>(rf); i++) {
            broker_id_t best = -1;
            for (const auto& b : brokers) {
                if (std::find(result.begin(), result.end(), b.id) != result.end()) continue;
                if (best == -1 || rack_used[b.rack] < rack_used[brokers[best].rack]) best = b.id;
            }
            if (best >= 0) { result.push_back(best); rack_used[brokers[best].rack]++; }
        }
        return result;
    }
};

// ============================================================================
// Request Dispatcher: Version negotiation
// ============================================================================
class VersionNegotiator {
public:
    struct ApiInfo { int16_t api_key; int16_t min_ver; int16_t max_ver; };
    std::optional<int16_t> negotiate(const std::vector<ApiInfo>& apis, int16_t api_key, int16_t client_ver) {
        for (const auto& api : apis) {
            if (api.api_key == api_key) {
                if (client_ver >= api.min_ver && client_ver <= api.max_ver) return client_ver;
                if (client_ver < api.min_ver) return api.min_ver;
                return api.max_ver; // Downgrade to max supported
            }
        }
        return std::nullopt; // Unknown API
    }
};

// ============================================================================
// Inter-broker: Circuit breaker
// ============================================================================
class CircuitBreaker {
public:
    enum State { CLOSED, OPEN, HALF_OPEN };
    State state() const { return state_.load(); }
    void record_success() { failures_.store(0); state_.store(State::CLOSED); }
    void record_failure() {
        if (failures_.fetch_add(1) >= 5) {
            state_.store(State::OPEN);
            open_time_ = std::chrono::steady_clock::now();
        }
    }
    bool allow_request() {
        if (state_.load() == State::CLOSED) return true;
        if (state_.load() == State::OPEN) {
            auto elapsed = std::chrono::steady_clock::now() - open_time_;
            if (elapsed > std::chrono::seconds(30)) {
                state_.store(State::HALF_OPEN);
                return true;
            }
            return false;
        }
        return true; // HALF_OPEN: allow one probe
    }
private:
    std::atomic<State> state_{State::CLOSED};
    std::atomic<int> failures_{0};
    std::chrono::steady_clock::time_point open_time_;
};

// ============================================================================
// Background Task Scheduler
// ============================================================================
class BackgroundScheduler {
public:
    struct Task { std::string name; std::function<void()> fn; std::chrono::milliseconds interval; };
    void add_task(Task t) { tasks_.push_back(t); }
    void start() {
        running_.store(true);
        for (auto& t : tasks_) {
            threads_.emplace_back([this, t]() {
                while (running_.load()) {
                    std::this_thread::sleep_for(t.interval);
                    if (running_.load()) { try { t.fn(); } catch (const std::exception& e) { spdlog::error("Background task {} failed: {}", t.name, e.what()); } }
                }
            });
        }
    }
    void shutdown() { running_.store(false); for (auto& t : threads_) if (t.joinable()) t.join(); }
private:
    std::vector<Task> tasks_;
    std::vector<std::thread> threads_;
    std::atomic<bool> running_{false};
};

} // anonymous namespace
} // namespace torrent::broker
