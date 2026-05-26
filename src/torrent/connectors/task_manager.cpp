/**
 * task_manager.cpp — TaskManager: Connector Task Lifecycle Management
 *
 * Manages the entire lifecycle of connector tasks within the ConnectFramework.
 * Each connector (source or sink) is decomposed into one or more tasks — the
 * smallest schedulable unit of work.  TaskManager provides:
 *
 *   - Task lifecycle: unassigned → assigned → running → paused → failed → stopped
 *   - Task state machine with strict transition rules
 *   - Task-to-worker assignment with configurable concurrency
 *   - Dynamic rebalancing of tasks across the worker pool
 *   - Per-task health monitoring with liveness probes
 *   - Graceful start, reconfigure, and stop of individual tasks
 *   - Rich metrics: throughput, latency, error rate, offset lag
 *   - Status reporting as JSON for the REST admin API
 *
 * Architecture:
 *
 *   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
 *   │  Connector   │────→│ TaskManager  │────→│ Worker Pool │
 *   │  (source/    │     │  ┌─────────┐ │     │  ┌───────┐  │
 *   │   sink)      │     │  │Task     │ │     │  │Worker │  │
 *   │              │     │  │state    │ │     │  │thread │  │
 *   └─────────────┘     │  │machine  │ │     │  └───────┘  │
 *                       │  └─────────┘ │     └─────────────┘
 *                       └─────────────┘
 *
 * Task state machine:
 *
 *         ┌──────────┐
 *         │unassigned│ ← new task created, no worker assigned
 *         └────┬─────┘
 *              │ assign()
 *         ┌────▼─────┐
 *         │  paused   │ ← assigned to worker but not running
 *         └────┬─────┘
 *              │ resume()
 *         ┌────▼─────┐     ┌──────────┐
 *         │  running  │────→│  failed   │ ← error threshold exceeded
 *         └────┬─────┘     └──────────┘
 *              │ pause()
 *         ┌────▼─────┐
 *         │  paused   │
 *         └────┬─────┘
 *              │ stop()
 *         ┌────▼─────┐
 *         │  stopped  │ ← terminal state
 *         └──────────┘
 *
 * Thread-safety:
 *   The tasks_ map and each Task's state are protected by a shared_mutex.
 *   Status queries (read-only) take a shared lock; mutations take an
 *   exclusive lock.  Worker callbacks execute under per-task mutexes.
 *
 * Dependencies:
 *   - ConnectFramework: parent orchestrator
 *   - BrokerServer: for produce/consume operations within tasks
 *   - spdlog: structured logging
 *   - backoff.h: exponential backoff for retry of failed tasks
 */

#include "torrent/connectors/connect.h"

#include "torrent/broker/server.h"
#include "torrent/common/backoff.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <random>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

using json = nlohmann::json;
using namespace std::chrono_literals;

namespace torrent::connectors {

// ============================================================================
// Anonymous namespace — internals
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_tm_logger() {
    static auto logger = spdlog::get("task_manager");
    if (!logger) {
        logger = spdlog::stdout_color_mt("task_manager");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Task state enumeration
// --------------------------------------------------------------------------

enum class TaskState : uint8_t {
    unassigned = 0,
    paused     = 1,
    running    = 2,
    failed     = 3,
    stopped    = 4,
};

[[nodiscard]] std::string_view task_state_name(TaskState s) {
    switch (s) {
    case TaskState::unassigned: return "unassigned";
    case TaskState::paused:     return "paused";
    case TaskState::running:    return "running";
    case TaskState::failed:     return "failed";
    case TaskState::stopped:    return "stopped";
    }
    return "unknown";
}

// --------------------------------------------------------------------------
// Valid state transitions
// --------------------------------------------------------------------------

[[nodiscard]] bool is_valid_transition(TaskState from, TaskState to) {
    switch (from) {
    case TaskState::unassigned:
        return to == TaskState::paused || to == TaskState::stopped;
    case TaskState::paused:
        return to == TaskState::running || to == TaskState::stopped
            || to == TaskState::failed;
    case TaskState::running:
        return to == TaskState::paused || to == TaskState::failed
            || to == TaskState::stopped;
    case TaskState::failed:
        return to == TaskState::paused || to == TaskState::stopped;
    case TaskState::stopped:
        return false; // terminal state
    }
    return false;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

constexpr int32_t kDefaultMaxFailures = 5;
constexpr std::chrono::seconds kDefaultHealthInterval{30};
constexpr std::chrono::seconds kDefaultLivenessTimeout{10};
constexpr std::chrono::seconds kDefaultRebalanceInterval{60};
constexpr int32_t kDefaultWorkerThreads = 4;

} // anonymous namespace

// ============================================================================
// TaskMetrics — per-task operational counters
// ============================================================================

struct TaskMetrics {
    std::atomic<int64_t> records_processed{0};
    std::atomic<int64_t> records_failed{0};
    std::atomic<int64_t> bytes_processed{0};
    std::atomic<int64_t> cumulative_latency_us{0};
    std::atomic<int64_t> current_offset{0};
    std::atomic<int64_t> committed_offset{0};
    std::atomic<int64_t> last_heartbeat_ms{0};
    std::atomic<int32_t> consecutive_failures{0};
    std::atomic<int32_t> total_restarts{0};
    std::chrono::steady_clock::time_point started_at;
    std::chrono::steady_clock::time_point last_active;

    void reset_counters() {
        records_processed.store(0, std::memory_order_release);
        records_failed.store(0, std::memory_order_release);
        bytes_processed.store(0, std::memory_order_release);
        cumulative_latency_us.store(0, std::memory_order_release);
        consecutive_failures.store(0, std::memory_order_release);
    }
};

// ============================================================================
// Task — unit of work managed by TaskManager
// ============================================================================

struct Task {
    std::string id;
    std::string connector_name;
    std::string task_type; // "source" or "sink"
    int32_t partition_id = 0;

    TaskState state = TaskState::unassigned;
    std::mutex state_mutex;

    json config;
    TaskMetrics metrics;

    int32_t assigned_worker = -1;
    int32_t max_failures = kDefaultMaxFailures;

    std::unique_ptr<Backoff> retry_backoff;

    // Work function provided by the connector
    std::function<void(Task&)> work_fn;
    std::function<bool(Task&)> health_check_fn;

    std::atomic<bool> stop_requested{false};
    std::atomic<bool> pause_requested{false};

    Task(std::string tid, std::string cname, std::string ttype, int32_t pid,
         json cfg)
        : id(std::move(tid))
        , connector_name(std::move(cname))
        , task_type(std::move(ttype))
        , partition_id(pid)
        , config(std::move(cfg))
        , retry_backoff(std::make_unique<Backoff>(
              std::chrono::milliseconds(100), std::chrono::seconds(30), 2.0))
    {
        metrics.started_at = std::chrono::steady_clock::now();
        metrics.last_active = metrics.started_at;
        metrics.last_heartbeat_ms.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count(),
            std::memory_order_release);
    }

    [[nodiscard]] bool transition(TaskState to) {
        std::lock_guard<std::mutex> lock(state_mutex);
        TaskState current = state;
        if (!is_valid_transition(current, to)) {
            return false;
        }
        state = to;
        if (to == TaskState::running) {
            stop_requested.store(false, std::memory_order_release);
            pause_requested.store(false, std::memory_order_release);
        }
        return true;
    }

    [[nodiscard]] TaskState current_state() const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(state_mutex));
        return state;
    }

    void heartbeat() {
        metrics.last_heartbeat_ms.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count(),
            std::memory_order_release);
        metrics.last_active = std::chrono::steady_clock::now();
    }

    void record_success(int64_t records, int64_t bytes, int64_t latency_us) {
        metrics.records_processed.fetch_add(records, std::memory_order_relaxed);
        metrics.bytes_processed.fetch_add(bytes, std::memory_order_relaxed);
        metrics.cumulative_latency_us.fetch_add(latency_us,
                                                std::memory_order_relaxed);
        metrics.consecutive_failures.store(0, std::memory_order_release);
        heartbeat();
    }

    void record_failure() {
        metrics.records_failed.fetch_add(1, std::memory_order_relaxed);
        metrics.consecutive_failures.fetch_add(1, std::memory_order_acq_rel);
    }

    [[nodiscard]] double avg_latency_us() const {
        int64_t total = metrics.records_processed.load(std::memory_order_acquire);
        if (total == 0) return 0.0;
        return static_cast<double>(
                   metrics.cumulative_latency_us.load(std::memory_order_acquire))
             / static_cast<double>(total);
    }

    [[nodiscard]] int64_t offset_lag() const {
        return metrics.current_offset.load(std::memory_order_acquire)
             - metrics.committed_offset.load(std::memory_order_acquire);
    }

    [[nodiscard]] json status() const {
        json j;
        j["id"] = id;
        j["connector"] = connector_name;
        j["type"] = task_type;
        j["partition"] = partition_id;
        j["state"] = task_state_name(current_state());
        j["worker"] = assigned_worker;
        j["records_processed"]
            = metrics.records_processed.load(std::memory_order_acquire);
        j["records_failed"]
            = metrics.records_failed.load(std::memory_order_acquire);
        j["bytes_processed"]
            = metrics.bytes_processed.load(std::memory_order_acquire);
        j["avg_latency_us"] = avg_latency_us();
        j["offset_lag"] = offset_lag();
        j["consecutive_failures"]
            = metrics.consecutive_failures.load(std::memory_order_acquire);
        j["total_restarts"]
            = metrics.total_restarts.load(std::memory_order_acquire);
        j["last_heartbeat_ms"]
            = metrics.last_heartbeat_ms.load(std::memory_order_acquire);
        return j;
    }
};

// ============================================================================
// Worker — execution thread for tasks
// ============================================================================

struct Worker {
    int32_t id = -1;
    std::unique_ptr<std::thread> thread;
    std::atomic<bool> running{false};
    std::atomic<bool> shutdown_requested{false};

    // Tasks currently assigned to this worker
    std::vector<std::shared_ptr<Task>> assigned_tasks;
    std::mutex tasks_mutex;

    // Condition variable for work scheduling
    std::condition_variable work_cv;
    std::mutex cv_mutex;
    std::atomic<int32_t> pending_work{0};

    void notify() {
        pending_work.store(1, std::memory_order_release);
        work_cv.notify_one();
    }

    [[nodiscard]] json status() const {
        json j;
        j["worker_id"] = id;
        j["running"] = running.load(std::memory_order_acquire);
        j["assigned_task_count"] = assigned_tasks.size();
        return j;
    }
};

// ============================================================================
// TaskManager — implementation
// ============================================================================

class TaskManager::Impl {
public:
    explicit Impl(broker::BrokerServer& server)
        : server_(&server)
        , next_task_id_(0)
        , next_worker_id_(0)
    {}

    ~Impl() { shutdown(); }

    // ----------------------------------------------------------------------
    // Lifecycle
    // ----------------------------------------------------------------------

    void start(int32_t worker_count = kDefaultWorkerThreads) {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        if (running_.load(std::memory_order_acquire)) {
            return; // already started
        }

        get_tm_logger()->info("TaskManager starting with {} workers",
                              worker_count);

        for (int32_t i = 0; i < worker_count; ++i) {
            auto w = std::make_shared<Worker>();
            w->id = next_worker_id_.fetch_add(1, std::memory_order_relaxed);
            w->running.store(true, std::memory_order_release);
            w->thread = std::make_unique<std::thread>(&Impl::worker_loop, this,
                                                       w);
            workers_.push_back(w);
        }

        // Start health monitor thread
        health_monitor_running_.store(true, std::memory_order_release);
        health_thread_ = std::make_unique<std::thread>(
            &Impl::health_monitor_loop, this);

        // Start rebalance timer thread
        rebalance_running_.store(true, std::memory_order_release);
        rebalance_thread_ = std::make_unique<std::thread>(
            &Impl::rebalance_loop, this);

        running_.store(true, std::memory_order_release);
        get_tm_logger()->info("TaskManager started successfully");
    }

    void shutdown() {
        if (!running_.load(std::memory_order_acquire)) {
            return;
        }

        get_tm_logger()->info("TaskManager shutting down");

        // Signal health monitor and rebalance to stop
        health_monitor_running_.store(false, std::memory_order_release);
        rebalance_running_.store(false, std::memory_order_release);

        // Stop all tasks
        {
            std::shared_lock<std::shared_mutex> lock(rw_mutex_);
            for (auto& [tid, task] : tasks_) {
                task->stop_requested.store(true, std::memory_order_release);
            }
        }

        // Stop all workers
        for (auto& w : workers_) {
            w->shutdown_requested.store(true, std::memory_order_release);
            w->notify();
            if (w->thread && w->thread->joinable()) {
                w->thread->join();
            }
            w->running.store(false, std::memory_order_release);
        }

        // Join helper threads
        if (health_thread_ && health_thread_->joinable()) {
            health_thread_->join();
        }
        if (rebalance_thread_ && rebalance_thread_->joinable()) {
            rebalance_thread_->join();
        }

        workers_.clear();

        {
            std::unique_lock<std::shared_mutex> lock(rw_mutex_);
            tasks_.clear();
        }

        running_.store(false, std::memory_order_release);
        get_tm_logger()->info("TaskManager shutdown complete");
    }

    // ----------------------------------------------------------------------
    // Task registration
    // ----------------------------------------------------------------------

    std::string register_task(const std::string& connector_name,
                              const std::string& task_type,
                              int32_t partition_id,
                              const json& config,
                              std::function<void(Task&)> work_fn,
                              std::function<bool(Task&)> health_fn)
    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);

        int64_t tid = next_task_id_.fetch_add(1, std::memory_order_relaxed);
        std::string task_id = connector_name + "-" + std::to_string(partition_id)
                            + "-" + std::to_string(tid);

        auto task = std::make_shared<Task>(task_id, connector_name, task_type,
                                           partition_id, config);
        task->work_fn = std::move(work_fn);
        task->health_check_fn = std::move(health_fn);

        tasks_[task_id] = task;
        task_ids_order_.push_back(task_id);

        get_tm_logger()->info("Registered task: {} (connector={}, partition={})",
                              task_id, connector_name, partition_id);

        return task_id;
    }

    // ----------------------------------------------------------------------
    // Task lifecycle operations
    // ----------------------------------------------------------------------

    bool start_task(const std::string& task_id) {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);

        auto it = tasks_.find(task_id);
        if (it == tasks_.end()) {
            get_tm_logger()->warn("start_task: task {} not found", task_id);
            return false;
        }

        auto& task = it->second;

        // Assign to least-loaded worker
        int32_t worker_id = assign_to_worker(task);
        if (worker_id < 0) {
            get_tm_logger()->error("start_task: no available worker for {}",
                                    task_id);
            return false;
        }

        if (!task->transition(TaskState::paused)) {
            get_tm_logger()->warn("start_task: invalid transition for {} "
                                  "from {}", task_id,
                                  task_state_name(task->current_state()));
            return false;
        }

        // Resume immediately to put into running state
        if (!task->transition(TaskState::running)) {
            return false;
        }

        // Notify worker
        for (auto& w : workers_) {
            if (w->id == worker_id) {
                w->notify();
                break;
            }
        }

        get_tm_logger()->info("Started task {} on worker {}", task_id,
                              worker_id);
        return true;
    }

    bool pause_task(const std::string& task_id) {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);

        auto it = tasks_.find(task_id);
        if (it == tasks_.end()) return false;

        auto& task = it->second;
        task->pause_requested.store(true, std::memory_order_release);

        if (!task->transition(TaskState::paused)) {
            get_tm_logger()->warn("pause_task: invalid transition for {}",
                                  task_id);
            return false;
        }

        get_tm_logger()->info("Paused task {}", task_id);
        return true;
    }

    bool resume_task(const std::string& task_id) {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);

        auto it = tasks_.find(task_id);
        if (it == tasks_.end()) return false;

        auto& task = it->second;
        if (task->current_state() != TaskState::paused) {
            get_tm_logger()->warn("resume_task: task {} not paused", task_id);
            return false;
        }

        if (!task->transition(TaskState::running)) return false;

        // Notify the assigned worker
        for (auto& w : workers_) {
            if (w->id == task->assigned_worker) {
                w->notify();
                break;
            }
        }

        get_tm_logger()->info("Resumed task {}", task_id);
        return true;
    }

    bool stop_task(const std::string& task_id) {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);

        auto it = tasks_.find(task_id);
        if (it == tasks_.end()) return false;

        auto& task = it->second;

        // Request the task to stop
        task->stop_requested.store(true, std::memory_order_release);

        if (!task->transition(TaskState::stopped)) {
            get_tm_logger()->warn("stop_task: invalid transition for {}",
                                  task_id);
            return false;
        }

        // Remove from worker's assigned list
        for (auto& w : workers_) {
            std::lock_guard<std::mutex> wlock(w->tasks_mutex);
            auto& v = w->assigned_tasks;
            v.erase(std::remove_if(v.begin(), v.end(),
                                   [&](const auto& t) {
                                       return t->id == task_id;
                                   }),
                    v.end());
        }

        get_tm_logger()->info("Stopped task {}", task_id);
        return true;
    }

    bool restart_task(const std::string& task_id) {
        stop_task(task_id);

        // Reset failure counters
        {
            std::shared_lock<std::shared_mutex> lock(rw_mutex_);
            auto it = tasks_.find(task_id);
            if (it != tasks_.end()) {
                it->second->metrics.reset_counters();
                it->second->metrics.total_restarts.fetch_add(
                    1, std::memory_order_relaxed);
                it->second->retry_backoff->reset();
            }
        }

        return start_task(task_id);
    }

    bool reconfigure_task(const std::string& task_id, const json& new_config) {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);

        auto it = tasks_.find(task_id);
        if (it == tasks_.end()) return false;

        auto& task = it->second;

        // Pause, apply config, resume
        task->pause_requested.store(true, std::memory_order_release);
        task->transition(TaskState::paused);

        task->config = new_config;
        get_tm_logger()->info("Reconfigured task {} with new config", task_id);

        task->transition(TaskState::running);
        for (auto& w : workers_) {
            if (w->id == task->assigned_worker) {
                w->notify();
                break;
            }
        }

        return true;
    }

    // ----------------------------------------------------------------------
    // Task assignment and rebalancing
    // ----------------------------------------------------------------------

    int32_t assign_to_worker(std::shared_ptr<Task> task) {
        // Find the worker with the fewest assigned tasks
        int32_t best_worker = -1;
        size_t min_tasks = SIZE_MAX;

        for (auto& w : workers_) {
            std::lock_guard<std::mutex> lock(w->tasks_mutex);
            if (w->assigned_tasks.size() < min_tasks) {
                min_tasks = w->assigned_tasks.size();
                best_worker = w->id;
            }
        }

        if (best_worker >= 0) {
            task->assigned_worker = best_worker;
            for (auto& w : workers_) {
                if (w->id == best_worker) {
                    std::lock_guard<std::mutex> lock(w->tasks_mutex);
                    w->assigned_tasks.push_back(task);
                    break;
                }
            }
        }

        return best_worker;
    }

    void rebalance() {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);

        if (workers_.empty()) return;

        // Collect all running/paused tasks
        std::vector<std::shared_ptr<Task>> movable_tasks;
        for (auto& [tid, task] : tasks_) {
            TaskState s = task->current_state();
            if (s == TaskState::running || s == TaskState::paused) {
                movable_tasks.push_back(task);
            }
        }

        if (movable_tasks.empty()) return;

        get_tm_logger()->info("Rebalancing {} tasks across {} workers",
                              movable_tasks.size(), workers_.size());

        // Clear all worker assignments
        for (auto& w : workers_) {
            std::lock_guard<std::mutex> wlock(w->tasks_mutex);
            w->assigned_tasks.clear();
        }

        // Reassign tasks round-robin
        int32_t wi = 0;
        for (auto& task : movable_tasks) {
            int32_t target = workers_[wi % workers_.size()]->id;
            task->assigned_worker = target;
            for (auto& w : workers_) {
                if (w->id == target) {
                    std::lock_guard<std::mutex> wlock(w->tasks_mutex);
                    w->assigned_tasks.push_back(task);
                    break;
                }
            }
            ++wi;
        }

        get_tm_logger()->info("Rebalance complete");
    }

    // ----------------------------------------------------------------------
    // Health monitoring
    // ----------------------------------------------------------------------

    void check_task_health(std::shared_ptr<Task> task) {
        auto now = std::chrono::steady_clock::now();
        auto since_active = now - task->metrics.last_active;

        // Check liveness: if a task hasn't heartbeated within the timeout
        if (since_active > kDefaultLivenessTimeout
            && task->current_state() == TaskState::running) {
            get_tm_logger()->warn("Task {} appears hung (no heartbeat for {}s)",
                                  task->id,
                                  std::chrono::duration_cast<std::chrono::seconds>(
                                      since_active)
                                      .count());

            // Run explicit health check if available
            if (task->health_check_fn) {
                bool healthy = task->health_check_fn(*task);
                if (!healthy) {
                    get_tm_logger()->error("Task {} failed health check",
                                            task->id);
                    handle_task_failure(task);
                    return;
                }
            }
        }

        // Check consecutive failure threshold
        int32_t failures = task->metrics.consecutive_failures.load(
            std::memory_order_acquire);
        if (failures >= task->max_failures) {
            get_tm_logger()->error(
                "Task {} exceeded failure threshold ({}/{})",
                task->id, failures, task->max_failures);
            handle_task_failure(task);
        }
    }

    void handle_task_failure(std::shared_ptr<Task> task) {
        task->transition(TaskState::failed);
        get_tm_logger()->warn("Task {} marked as failed", task->id);

        // Attempt automatic restart with backoff
        auto delay = task->retry_backoff->next_delay();
        get_tm_logger()->info("Scheduling restart for task {} in {}ms",
                              task->id,
                              std::chrono::duration_cast<std::chrono::milliseconds>(
                                  delay)
                                  .count());

        // In a real implementation, schedule via timer.  Here we do it
        // synchronously for simplicity after the backoff delay.
        std::this_thread::sleep_for(delay);

        if (task->current_state() == TaskState::failed
            && !task->stop_requested.load(std::memory_order_acquire)) {
            task->metrics.reset_counters();
            task->metrics.total_restarts.fetch_add(1,
                                                    std::memory_order_relaxed);
            task->transition(TaskState::paused);
            task->transition(TaskState::running);

            for (auto& w : workers_) {
                if (w->id == task->assigned_worker) {
                    w->notify();
                    break;
                }
            }
            get_tm_logger()->info("Auto-restarted task {}", task->id);
        }
    }

    // ----------------------------------------------------------------------
    // Status and metrics
    // ----------------------------------------------------------------------

    [[nodiscard]] json task_status(const std::string& task_id) const {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);

        auto it = tasks_.find(task_id);
        if (it == tasks_.end()) {
            return json{{"error", "task not found"}};
        }

        return it->second->status();
    }

    [[nodiscard]] json all_tasks_status() const {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);

        json result = json::array();
        for (const auto& [tid, task] : tasks_) {
            result.push_back(task->status());
        }
        return result;
    }

    [[nodiscard]] json worker_status() const {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);

        json result = json::array();
        for (const auto& w : workers_) {
            result.push_back(w->status());
        }
        return result;
    }

    [[nodiscard]] json summary() const {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);

        json j;
        j["total_tasks"] = tasks_.size();
        j["total_workers"] = workers_.size();
        j["running"] = running_.load(std::memory_order_acquire);

        int32_t running_count = 0, paused_count = 0, failed_count = 0,
                stopped_count = 0, unassigned_count = 0;
        int64_t total_records = 0, total_failures = 0;

        for (const auto& [tid, task] : tasks_) {
            switch (task->current_state()) {
            case TaskState::running:    ++running_count;    break;
            case TaskState::paused:     ++paused_count;     break;
            case TaskState::failed:     ++failed_count;     break;
            case TaskState::stopped:    ++stopped_count;    break;
            case TaskState::unassigned: ++unassigned_count; break;
            }
            total_records
                += task->metrics.records_processed.load(std::memory_order_acquire);
            total_failures
                += task->metrics.records_failed.load(std::memory_order_acquire);
        }

        j["by_state"]["running"] = running_count;
        j["by_state"]["paused"] = paused_count;
        j["by_state"]["failed"] = failed_count;
        j["by_state"]["stopped"] = stopped_count;
        j["by_state"]["unassigned"] = unassigned_count;
        j["total_records_processed"] = total_records;
        j["total_records_failed"] = total_failures;

        return j;
    }

private:
    // ======================================================================
    // Worker thread loop
    // ======================================================================

    void worker_loop(std::shared_ptr<Worker> worker) {
        get_tm_logger()->info("Worker {} started", worker->id);

        while (!worker->shutdown_requested.load(std::memory_order_acquire)) {
            // Wait for work
            {
                std::unique_lock<std::mutex> cv_lock(worker->cv_mutex);
                worker->work_cv.wait_for(cv_lock, 100ms, [&] {
                    return worker->pending_work.load(std::memory_order_acquire)
                           > 0
                        || worker->shutdown_requested.load(
                            std::memory_order_acquire);
                });
            }

            if (worker->shutdown_requested.load(std::memory_order_acquire)) {
                break;
            }

            worker->pending_work.store(0, std::memory_order_release);

            // Process each assigned task that is in the running state
            std::vector<std::shared_ptr<Task>> local_tasks;
            {
                std::lock_guard<std::mutex> lock(worker->tasks_mutex);
                local_tasks = worker->assigned_tasks;
            }

            for (auto& task : local_tasks) {
                // Skip tasks that are not running
                if (task->current_state() != TaskState::running) continue;
                if (task->stop_requested.load(std::memory_order_acquire))
                    continue;
                if (task->pause_requested.load(std::memory_order_acquire))
                    continue;

                try {
                    task->work_fn(*task);
                    task->heartbeat();
                } catch (const std::exception& ex) {
                    get_tm_logger()->error("Task {} threw exception: {}",
                                            task->id, ex.what());
                    task->record_failure();
                } catch (...) {
                    get_tm_logger()->error("Task {} threw unknown exception",
                                            task->id);
                    task->record_failure();
                }
            }
        }

        get_tm_logger()->info("Worker {} stopped", worker->id);
    }

    // ======================================================================
    // Health monitor loop
    // ======================================================================

    void health_monitor_loop() {
        get_tm_logger()->info("Health monitor started");

        while (health_monitor_running_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(kDefaultHealthInterval);

            std::shared_lock<std::shared_mutex> lock(rw_mutex_);
            for (auto& [tid, task] : tasks_) {
                check_task_health(task);
            }
        }

        get_tm_logger()->info("Health monitor stopped");
    }

    // ======================================================================
    // Rebalance loop
    // ======================================================================

    void rebalance_loop() {
        get_tm_logger()->info("Rebalance monitor started");

        while (rebalance_running_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(kDefaultRebalanceInterval);
            rebalance();
        }

        get_tm_logger()->info("Rebalance monitor stopped");
    }

    // ======================================================================
    // Member variables
    // ======================================================================

    broker::BrokerServer* server_;
    std::atomic<bool> running_{false};

    // Task registry
    std::unordered_map<std::string, std::shared_ptr<Task>> tasks_;
    std::vector<std::string> task_ids_order_;
    std::atomic<int64_t> next_task_id_{0};
    mutable std::shared_mutex rw_mutex_;

    // Worker pool
    std::vector<std::shared_ptr<Worker>> workers_;
    std::atomic<int32_t> next_worker_id_{0};

    // Health monitor
    std::unique_ptr<std::thread> health_thread_;
    std::atomic<bool> health_monitor_running_{false};

    // Rebalance
    std::unique_ptr<std::thread> rebalance_thread_;
    std::atomic<bool> rebalance_running_{false};
};

// ============================================================================
// TaskManager — public API (PIMPL wrapper)
// ============================================================================

TaskManager::TaskManager(broker::BrokerServer& server)
    : impl_(std::make_unique<Impl>(server))
{}

TaskManager::~TaskManager() = default;

void TaskManager::start(int32_t worker_count) { impl_->start(worker_count); }
void TaskManager::shutdown() { impl_->shutdown(); }

std::string TaskManager::register_task(
    const std::string& connector_name,
    const std::string& task_type,
    int32_t partition_id,
    const json& config,
    std::function<void(Task&)> work_fn,
    std::function<bool(Task&)> health_fn)
{
    return impl_->register_task(connector_name, task_type, partition_id,
                                config, std::move(work_fn),
                                std::move(health_fn));
}

bool TaskManager::start_task(const std::string& id) { return impl_->start_task(id); }
bool TaskManager::pause_task(const std::string& id) { return impl_->pause_task(id); }
bool TaskManager::resume_task(const std::string& id) { return impl_->resume_task(id); }
bool TaskManager::stop_task(const std::string& id) { return impl_->stop_task(id); }
bool TaskManager::restart_task(const std::string& id) { return impl_->restart_task(id); }
bool TaskManager::reconfigure_task(const std::string& id, const json& cfg) {
    return impl_->reconfigure_task(id, cfg);
}
void TaskManager::rebalance() { impl_->rebalance(); }

json TaskManager::task_status(const std::string& id) const {
    return impl_->task_status(id);
}
json TaskManager::all_tasks_status() const { return impl_->all_tasks_status(); }
json TaskManager::worker_status() const { return impl_->worker_status(); }
json TaskManager::summary() const { return impl_->summary(); }

} // namespace torrent::connectors
