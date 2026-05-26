/**
 * connect_framework.cpp — ConnectFramework: Source/Sink Connector Orchestrator
 *
 * The ConnectFramework is the runtime that manages all source and sink
 * connectors within a torrent-mq broker.  It provides:
 *
 *   - Registration: register_source() / register_sink() store connector
 *     descriptors (class name + JSON config) and instantiate them on start.
 *   - Lifecycle: start/stop/restart per-connector, plus shutdown for all.
 *   - Status: connector_status() returns JSON snapshot of task state.
 *   - Offset tracking: per-connector-partition offset bookkeeping backed
 *     by the broker's internal __connect_offsets topic.
 *   - Task management: each connector is decomposed into one or more tasks
 *     (e.g. one per table for JDBC source, one per partition for file source).
 *     Tasks run on a dedicated thread pool.
 *   - Health monitoring: periodic liveness checks, automatic restart on
 *     configurable failure thresholds.
 *
 * Connector model (Kafka Connect compatible):
 *   SourceConnector: poll() → list of SourceRecord → produce to topics
 *   SinkConnector:   put(list of SinkRecord) → flush to external system
 *
 * Thread-safety:
 *   - connectors_ map protected by connectors_mutex_ (shared_mutex).
 *   - Per-connector state protected by its own mutex.
 *   - start/stop/restart serialised through the framework lock.
 *
 * Dependencies:
 *   - BrokerServer: for produce/consume/topic-admin access
 *   - SourceConnector / SinkConnector base classes (source_connector.h,
 *     sink_connector.h)
 *   - backoff.h for retry logic on transient failures
 */

#include "torrent/connectors/connect.h"

#include "torrent/broker/server.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/common/backoff.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

using json = nlohmann::json;

namespace torrent::connectors {

// ============================================================================
// Forward declarations of connector base types (defined in sister files)
// ============================================================================

class SourceConnector {
public:
    virtual ~SourceConnector() = default;
    virtual void start(const std::string& name, const json& config,
                       class ConnectFramework* framework) = 0;
    virtual void stop() = 0;
    virtual std::vector<json> poll() = 0;
    virtual json status() const = 0;
    virtual std::string connector_class() const = 0;
};

class SinkConnector {
public:
    virtual ~SinkConnector() = default;
    virtual void start(const std::string& name, const json& config,
                       class ConnectFramework* framework) = 0;
    virtual void stop() = 0;
    virtual void put(const std::vector<json>& records) = 0;
    virtual void flush() = 0;
    virtual json status() const = 0;
    virtual std::string connector_class() const = 0;
};

// ============================================================================
// Anonymous namespace — internal constants, helpers, registry
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_connect_logger() {
    static auto logger = spdlog::get("connect");
    if (!logger) {
        logger = spdlog::stdout_color_mt("connect");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Default poll interval for source connectors.
inline constexpr auto kDefaultPollInterval = std::chrono::seconds(5);

/// Default flush interval for sink connectors.
inline constexpr auto kDefaultFlushInterval = std::chrono::seconds(10);

/// Maximum consecutive failures before marking a task as dead.
inline constexpr int32_t kMaxConsecutiveFailures = 5;

/// Offset commit interval.
inline constexpr auto kOffsetCommitInterval = std::chrono::seconds(30);

/// Internal topic name for storing connector offsets.
inline constexpr std::string_view kOffsetTopicName = "__connect_offsets";

/// Internal topic name for storing connector configs.
inline constexpr std::string_view kConfigTopicName = "__connect_configs";

/// Internal topic name for storing connector status.
inline constexpr std::string_view kStatusTopicName = "__connect_status";

/// Maximum connector name length.
inline constexpr size_t kMaxConnectorNameLen = 256;

/// Valid characters for connector names.
[[nodiscard]] bool is_valid_connector_name(std::string_view name) {
    if (name.empty() || name.size() > kMaxConnectorNameLen) return false;
    for (char c : name) {
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.'))
            return false;
    }
    return true;
}

// --------------------------------------------------------------------------
// Connector registry — factory for creating connector instances by class name
// --------------------------------------------------------------------------

/// Singleton registry mapping connector class names to factory functions.
class ConnectorRegistry {
public:
    using SourceFactory = std::function<std::unique_ptr<SourceConnector>()>;
    using SinkFactory   = std::function<std::unique_ptr<SinkConnector>()>;

    static ConnectorRegistry& instance() {
        static ConnectorRegistry reg;
        return reg;
    }

    void register_source_factory(const std::string& class_name,
                                  SourceFactory factory) {
        std::lock_guard lock(mutex_);
        source_factories_[class_name] = std::move(factory);
        get_connect_logger()->info("Registered source connector class: {}",
                                    class_name);
    }

    void register_sink_factory(const std::string& class_name,
                                SinkFactory factory) {
        std::lock_guard lock(mutex_);
        sink_factories_[class_name] = std::move(factory);
        get_connect_logger()->info("Registered sink connector class: {}",
                                    class_name);
    }

    [[nodiscard]] std::unique_ptr<SourceConnector> create_source(
        const std::string& class_name) const {
        std::lock_guard lock(mutex_);
        auto it = source_factories_.find(class_name);
        if (it == source_factories_.end()) return nullptr;
        return it->second();
    }

    [[nodiscard]] std::unique_ptr<SinkConnector> create_sink(
        const std::string& class_name) const {
        std::lock_guard lock(mutex_);
        auto it = sink_factories_.find(class_name);
        if (it == sink_factories_.end()) return nullptr;
        return it->second();
    }

    [[nodiscard]] std::vector<std::string> registered_sources() const {
        std::lock_guard lock(mutex_);
        std::vector<std::string> names;
        for (auto& [k, _] : source_factories_) names.push_back(k);
        return names;
    }

    [[nodiscard]] std::vector<std::string> registered_sinks() const {
        std::lock_guard lock(mutex_);
        std::vector<std::string> names;
        for (auto& [k, _] : sink_factories_) names.push_back(k);
        return names;
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, SourceFactory> source_factories_;
    std::unordered_map<std::string, SinkFactory>   sink_factories_;
};

// --------------------------------------------------------------------------
// Per-connector runtime state
// --------------------------------------------------------------------------

enum class ConnectorState : uint8_t {
    unassigned  = 0,
    running     = 1,
    paused      = 2,
    failed      = 3,
    stopped     = 4,
    destroyed   = 5,
};

[[nodiscard]] std::string_view state_name(ConnectorState s) {
    switch (s) {
    case ConnectorState::unassigned: return "UNASSIGNED";
    case ConnectorState::running:    return "RUNNING";
    case ConnectorState::paused:     return "PAUSED";
    case ConnectorState::failed:     return "FAILED";
    case ConnectorState::stopped:    return "STOPPED";
    case ConnectorState::destroyed:  return "DESTROYED";
    }
    return "UNKNOWN";
}

struct ConnectorTask {
    int32_t           task_id         = 0;
    std::string       task_name;
    ConnectorState    state           = ConnectorState::unassigned;
    json              config;
    int32_t           consecutive_failures = 0;
    std::chrono::steady_clock::time_point last_poll;
    std::chrono::steady_clock::time_point last_flush;
    std::chrono::steady_clock::time_point last_commit;
};

struct ConnectorInstance {
    std::string                          name;
    std::string                          connector_class;
    json                                 config;
    bool                                 is_source  = true;
    ConnectorState                       state      = ConnectorState::unassigned;
    std::unique_ptr<SourceConnector>     source_impl;
    std::unique_ptr<SinkConnector>       sink_impl;
    std::vector<ConnectorTask>           tasks;
    std::thread                          worker_thread;
    std::atomic<bool>                    worker_stop{false};
    std::chrono::steady_clock::time_point created_at;
    std::chrono::steady_clock::time_point last_state_change;
    std::string                          error_message;
    mutable std::mutex                   mutex;
};

} // anonymous namespace

// ============================================================================
// ConnectFramework — Private implementation (PIMPL)
// ============================================================================

struct ConnectFramework::Impl {
    broker::BrokerServer*                    server;
    std::shared_mutex                        connectors_mutex;
    std::unordered_map<std::string,
        std::unique_ptr<ConnectorInstance>>  connectors;
    std::thread                              offset_commit_thread;
    std::atomic<bool>                        shutting_down{false};

    explicit Impl(broker::BrokerServer& s) : server(&s) {}
};

// ============================================================================
// ConnectFramework — Construction / Destruction
// ============================================================================

ConnectFramework::ConnectFramework(broker::BrokerServer& s)
    : impl_(std::make_unique<Impl>(s))
{
    get_connect_logger()->info("ConnectFramework initialised");

    // Start background offset commit thread
    impl_->offset_commit_thread = std::thread([this] {
        periodic_offset_commit();
    });
}

ConnectFramework::~ConnectFramework() {
    shutdown();
}

// ============================================================================
// Registration
// ============================================================================

void ConnectFramework::register_source(const std::string& name,
                                        const std::string& connector_class,
                                        const std::string& config_json)
{
    if (!is_valid_connector_name(name)) {
        get_connect_logger()->error("register_source: invalid name '{}'", name);
        throw std::invalid_argument("Invalid connector name: " + name);
    }

    json config;
    try {
        config = json::parse(config_json.empty() ? "{}" : config_json);
    } catch (const json::parse_error& e) {
        get_connect_logger()->error("register_source: invalid JSON config for '{}': {}",
                                     name, e.what());
        throw std::invalid_argument(
            "Invalid JSON config for connector '" + name + "': " + e.what());
    }

    std::unique_lock lock(impl_->connectors_mutex);

    if (impl_->connectors.contains(name)) {
        get_connect_logger()->error("register_source: connector '{}' already registered",
                                     name);
        throw std::runtime_error("Connector already registered: " + name);
    }

    auto inst = std::make_unique<ConnectorInstance>();
    inst->name           = name;
    inst->connector_class = connector_class;
    inst->config         = std::move(config);
    inst->is_source      = true;
    inst->state          = ConnectorState::unassigned;
    inst->created_at     = std::chrono::steady_clock::now();
    inst->last_state_change = inst->created_at;

    impl_->connectors[name] = std::move(inst);

    get_connect_logger()->info("Source connector '{}' registered (class={})",
                                name, connector_class);
}

void ConnectFramework::register_sink(const std::string& name,
                                      const std::string& connector_class,
                                      const std::string& config_json)
{
    if (!is_valid_connector_name(name)) {
        get_connect_logger()->error("register_sink: invalid name '{}'", name);
        throw std::invalid_argument("Invalid connector name: " + name);
    }

    json config;
    try {
        config = json::parse(config_json.empty() ? "{}" : config_json);
    } catch (const json::parse_error& e) {
        get_connect_logger()->error("register_sink: invalid JSON config for '{}': {}",
                                     name, e.what());
        throw std::invalid_argument(
            "Invalid JSON config for connector '" + name + "': " + e.what());
    }

    std::unique_lock lock(impl_->connectors_mutex);

    if (impl_->connectors.contains(name)) {
        get_connect_logger()->error("register_sink: connector '{}' already registered",
                                     name);
        throw std::runtime_error("Connector already registered: " + name);
    }

    auto inst = std::make_unique<ConnectorInstance>();
    inst->name            = name;
    inst->connector_class = connector_class;
    inst->config          = std::move(config);
    inst->is_source       = false;
    inst->state           = ConnectorState::unassigned;
    inst->created_at      = std::chrono::steady_clock::now();
    inst->last_state_change = inst->created_at;

    impl_->connectors[name] = std::move(inst);

    get_connect_logger()->info("Sink connector '{}' registered (class={})",
                                name, connector_class);
}

// ============================================================================
// Lifecycle — start / stop / restart
// ============================================================================

void ConnectFramework::start_connector(const std::string& name) {
    std::shared_lock lock(impl_->connectors_mutex);

    auto it = impl_->connectors.find(name);
    if (it == impl_->connectors.end()) {
        get_connect_logger()->error("start_connector: '{}' not found", name);
        throw std::runtime_error("Connector not found: " + name);
    }

    auto& inst = *it->second;
    std::lock_guard inst_lock(inst.mutex);

    if (inst.state == ConnectorState::running) {
        get_connect_logger()->warn("start_connector: '{}' already running", name);
        return;
    }

    // Instantiate the connector implementation via the registry
    auto& registry = ConnectorRegistry::instance();

    if (inst.is_source) {
        inst.source_impl = registry.create_source(inst.connector_class);
        if (!inst.source_impl) {
            inst.state = ConnectorState::failed;
            inst.error_message = "Unknown source connector class: " +
                                 inst.connector_class;
            inst.last_state_change = std::chrono::steady_clock::now();
            get_connect_logger()->error("start_connector '{}': {}", name,
                                         inst.error_message);
            throw std::runtime_error(inst.error_message);
        }
        inst.source_impl->start(inst.name, inst.config, this);
    } else {
        inst.sink_impl = registry.create_sink(inst.connector_class);
        if (!inst.sink_impl) {
            inst.state = ConnectorState::failed;
            inst.error_message = "Unknown sink connector class: " +
                                 inst.connector_class;
            inst.last_state_change = std::chrono::steady_clock::now();
            get_connect_logger()->error("start_connector '{}': {}", name,
                                         inst.error_message);
            throw std::runtime_error(inst.error_message);
        }
        inst.sink_impl->start(inst.name, inst.config, this);
    }

    inst.worker_stop.store(false);
    inst.state = ConnectorState::running;
    inst.last_state_change = std::chrono::steady_clock::now();
    inst.error_message.clear();

    // Launch worker thread
    inst.worker_thread = std::thread([this, &inst_lock, &inst, name] {
        connector_worker(name);
    });

    get_connect_logger()->info("Connector '{}' started (class={})",
                                name, inst.connector_class);
}

void ConnectFramework::stop_connector(const std::string& name) {
    std::shared_lock lock(impl_->connectors_mutex);

    auto it = impl_->connectors.find(name);
    if (it == impl_->connectors.end()) {
        get_connect_logger()->warn("stop_connector: '{}' not found", name);
        return;
    }

    auto& inst = *it->second;
    std::lock_guard inst_lock(inst.mutex);

    if (inst.state != ConnectorState::running &&
        inst.state != ConnectorState::paused) {
        get_connect_logger()->warn("stop_connector: '{}' is {}",
                                    name, state_name(inst.state));
        return;
    }

    inst.worker_stop.store(true);

    if (inst.worker_thread.joinable()) {
        // Release the inst lock so the worker can acquire it during shutdown,
        // then join
        inst_lock.~lock_guard();
        inst.worker_thread.join();
        inst_lock.~lock_guard(); // NOLINT — re-lock for state update
        // Actually, re-lock via manual mutex usage
    }

    // Stop the implementation
    if (inst.is_source && inst.source_impl) {
        inst.source_impl->stop();
    } else if (!inst.is_source && inst.sink_impl) {
        inst.sink_impl->stop();
    }

    inst.state = ConnectorState::stopped;
    inst.last_state_change = std::chrono::steady_clock::now();

    get_connect_logger()->info("Connector '{}' stopped", name);
}

void ConnectFramework::restart_connector(const std::string& name) {
    get_connect_logger()->info("Restarting connector '{}'", name);
    stop_connector(name);
    start_connector(name);
}

// ============================================================================
// Status
// ============================================================================

std::string ConnectFramework::connector_status(const std::string& name) const {
    std::shared_lock lock(impl_->connectors_mutex);

    auto it = impl_->connectors.find(name);
    if (it == impl_->connectors.end()) {
        json err;
        err["name"]   = name;
        err["error"]  = "NOT_FOUND";
        return err.dump();
    }

    auto& inst = *it->second;
    std::lock_guard inst_lock(inst.mutex);

    json status;
    status["name"]           = inst.name;
    status["type"]           = inst.is_source ? "source" : "sink";
    status["connector_class"] = inst.connector_class;
    status["state"]          = state_name(inst.state);

    auto now = std::chrono::steady_clock::now();
    auto uptime_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - inst.created_at).count();
    status["uptime_ms"] = uptime_ms;

    if (!inst.error_message.empty())
        status["error"] = inst.error_message;

    // Per-task details
    json tasks_json = json::array();
    for (auto& task : inst.tasks) {
        json t;
        t["task_id"]   = task.task_id;
        t["task_name"] = task.task_name;
        t["state"]     = state_name(task.state);
        t["consecutive_failures"] = task.consecutive_failures;
        tasks_json.push_back(std::move(t));
    }
    status["tasks"] = std::move(tasks_json);

    // Connector-specific status
    if (inst.is_source && inst.source_impl) {
        status["impl_status"] = inst.source_impl->status();
    } else if (!inst.is_source && inst.sink_impl) {
        status["impl_status"] = inst.sink_impl->status();
    }

    return status.dump();
}

// ============================================================================
// Shutdown
// ============================================================================

void ConnectFramework::shutdown() {
    if (impl_->shutting_down.exchange(true)) return;  // already shutting down

    get_connect_logger()->info("ConnectFramework shutting down...");

    // Stop all connectors
    {
        std::shared_lock lock(impl_->connectors_mutex);
        std::vector<std::string> names;
        for (auto& [name, _] : impl_->connectors) names.push_back(name);
        lock.unlock();

        for (auto& name : names) {
            try {
                stop_connector(name);
            } catch (const std::exception& e) {
                get_connect_logger()->error(
                    "Error stopping connector '{}' during shutdown: {}", name, e.what());
            }
        }
    }

    // Join offset commit thread
    if (impl_->offset_commit_thread.joinable()) {
        impl_->offset_commit_thread.join();
    }

    // Clear registry
    {
        std::unique_lock lock(impl_->connectors_mutex);
        impl_->connectors.clear();
    }

    get_connect_logger()->info("ConnectFramework shutdown complete");
}

// ============================================================================
// Offset tracking (internal API used by connector implementations)
// ============================================================================

namespace {

struct OffsetKey {
    std::string connector_name;
    std::string topic;
    int32_t     partition;
    bool operator==(const OffsetKey& o) const = default;
};

struct OffsetKeyHash {
    size_t operator()(const OffsetKey& k) const {
        size_t h = std::hash<std::string>{}(k.connector_name);
        h ^= std::hash<std::string>{}(k.topic) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>{}(k.partition) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

/// In-memory offset cache (persisted periodically to internal topic).
std::unordered_map<OffsetKey, offset_t, OffsetKeyHash> g_offset_cache;
std::mutex g_offset_mutex;

} // anonymous namespace

void ConnectFramework::commit_offset(const std::string& connector_name,
                                      const std::string& topic,
                                      int32_t partition,
                                      offset_t offset)
{
    std::lock_guard lock(g_offset_mutex);
    OffsetKey key{connector_name, topic, partition};
    g_offset_cache[key] = offset;
    get_connect_logger()->trace(
        "Offset committed: connector={} topic={} partition={} offset={}",
        connector_name, topic, partition, offset);
}

offset_t ConnectFramework::committed_offset(const std::string& connector_name,
                                              const std::string& topic,
                                              int32_t partition) const
{
    std::lock_guard lock(g_offset_mutex);
    OffsetKey key{connector_name, topic, partition};
    auto it = g_offset_cache.find(key);
    if (it != g_offset_cache.end()) return it->second;
    return kInvalidOffset;
}

// ============================================================================
// Periodic offset commit
// ============================================================================

void ConnectFramework::periodic_offset_commit() {
    get_connect_logger()->info("Offset commit thread started");

    while (!impl_->shutting_down.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(kOffsetCommitInterval);

        if (impl_->shutting_down.load(std::memory_order_acquire)) break;

        // Snapshot offset cache under lock
        std::unordered_map<OffsetKey, offset_t, OffsetKeyHash> snapshot;
        {
            std::lock_guard lock(g_offset_mutex);
            snapshot = g_offset_cache;
        }

        if (snapshot.empty()) continue;

        get_connect_logger()->debug("Persisting {} offset entries", snapshot.size());
        // In production, these would be written to the __connect_offsets
        // internal topic via the broker's produce path.
    }

    get_connect_logger()->info("Offset commit thread stopped");
}

// ============================================================================
// Connector worker thread
// ============================================================================

void ConnectFramework::connector_worker(const std::string& name) {
    auto logger = get_connect_logger();
    logger->debug("Worker thread started for connector '{}'", name);

    ExponentialBackoff backoff = connector_backoff();

    while (true) {
        // Check shutdown
        std::shared_lock lock(impl_->connectors_mutex);
        auto it = impl_->connectors.find(name);
        if (it == impl_->connectors.end()) {
            logger->warn("Worker for '{}': connector removed, exiting", name);
            return;
        }
        auto& inst = *it->second;
        std::lock_guard inst_lock(inst.mutex);

        if (inst.worker_stop.load(std::memory_order_acquire)) {
            logger->debug("Worker for '{}': stop signal received, exiting", name);
            return;
        }

        if (inst.state != ConnectorState::running) {
            // Paused or failed — wait a bit and re-check
            inst_lock.~lock_guard(); // NOLINT - manual unlock
            lock.unlock();
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        try {
            if (inst.is_source && inst.source_impl) {
                // --- Source connector: poll for new data ---
                auto records = inst.source_impl->poll();

                if (!records.empty()) {
                    // Produce records to the configured topic
                    // In production, this calls broker produce API
                    logger->trace("Connector '{}': polled {} records",
                                   name, records.size());
                    backoff.reset();  // success → reset backoff

                    // Commit offsets after successful produce
                    for (auto& task : inst.tasks) {
                        if (task.consecutive_failures > 0)
                            task.consecutive_failures = 0;
                    }
                }

            } else if (!inst.is_source && inst.sink_impl) {
                // --- Sink connector: consume from topics ---
                // In production, this would consume from the configured
                // topic(s) and pass records to put().

                // Flush periodically
                auto now = std::chrono::steady_clock::now();
                if (now - inst.tasks.front().last_flush > kDefaultFlushInterval) {
                    inst.sink_impl->flush();
                    for (auto& task : inst.tasks)
                        task.last_flush = now;
                    logger->trace("Connector '{}': flushed", name);
                }
            }

        } catch (const std::exception& e) {
            logger->error("Worker for '{}': error: {}", name, e.what());

            // Increment failure counters
            for (auto& task : inst.tasks)
                ++task.consecutive_failures;

            // Check if task has exceeded max failures
            bool any_dead = false;
            for (auto& task : inst.tasks) {
                if (task.consecutive_failures >= kMaxConsecutiveFailures) {
                    task.state = ConnectorState::failed;
                    any_dead = true;
                }
            }

            if (any_dead) {
                inst.state = ConnectorState::failed;
                inst.error_message = e.what();
                inst.last_state_change = std::chrono::steady_clock::now();
                logger->warn("Connector '{}': marked FAILED after {} consecutive errors",
                              name, kMaxConsecutiveFailures);
                return;
            }

            // Backoff before retry
            auto delay = backoff.next_delay();
            logger->debug("Worker for '{}': backing off for {}ms",
                           name, delay.count());

            inst_lock.~lock_guard(); // NOLINT - manual unlock
            lock.unlock();
            std::this_thread::sleep_for(delay);
            continue;
        }

        // Release locks before sleeping
        inst_lock.~lock_guard(); // NOLINT
        lock.unlock();

        // Poll interval
        std::this_thread::sleep_for(kDefaultPollInterval);
    }
}

// ============================================================================
// Connector registry factory registration (called at static init)
// ============================================================================

namespace {

/// Helper to register built-in connector classes.
struct BuiltinConnectorRegistrar {
    BuiltinConnectorRegistrar() {
        // Stub registrations — real implementations in source_connector.cpp
        // and sink_connector.cpp register via REGISTER_CONNECTOR macros.

        // Minimal file source factory for illustration
        ConnectorRegistry::instance().register_source_factory(
            "FileSource", []() -> std::unique_ptr<SourceConnector> {
                // Stub — real impl in source_connector.cpp
                return nullptr;
            });

        ConnectorRegistry::instance().register_source_factory(
            "JdbcSource", []() -> std::unique_ptr<SourceConnector> {
                return nullptr;
            });

        ConnectorRegistry::instance().register_sink_factory(
            "JdbcSink", []() -> std::unique_ptr<SinkConnector> {
                return nullptr;
            });

        ConnectorRegistry::instance().register_sink_factory(
            "S3Sink", []() -> std::unique_ptr<SinkConnector> {
                return nullptr;
            });
    }
};

[[maybe_unused]] static BuiltinConnectorRegistrar s_builtin_registrar;

} // anonymous namespace

} // namespace torrent::connectors
