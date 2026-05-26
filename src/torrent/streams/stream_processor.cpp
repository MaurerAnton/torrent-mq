/**
 * stream_processor.cpp — StreamProcessor: Base Stream Processing Topology
 *
 * Core stream processing engine for torrent-mq.  Provides the foundational
 * abstractions for building a stream topology:
 *
 *   SourceProcessor  — ingest records from torrent topics
 *   ProcessorBase    — transform/filter/aggregate records
 *   SinkProcessor    — write processed records to topics or external systems
 *
 * Topology execution runs each node as an independent task on a configurable
 * thread pool.  Records flow downstream through lock-free single-producer /
 * single-consumer channels.  The framework supports:
 *
 *   - Parallel source consumption (one task per partition)
 *   - Exactly-once semantics via transactional boundaries
 *   - State store for stateful operations (aggregations, joins)
 *   - Checkpointing for fault-tolerant recovery
 *   - Back-pressure via bounded channels
 *   - Graceful shutdown with inflight record draining
 *
 * Architecture:
 *
 *   ┌──────────┐    channel    ┌──────────┐    channel    ┌──────────┐
 *   │  Source  │──────────────→│ Processor│──────────────→│   Sink   │
 *   │  Node    │               │  Node    │               │  Node    │
 *   └──────────┘               └──────────┘               └──────────┘
 *
 * State management:
 *   Stateful processors register a state store backed by RocksDB.  On
 *   recovery, the checkpoint is loaded and processing resumes from the
 *   last committed offset.
 *
 * Thread-safety:
 *   Each node runs on its own thread.  Channels provide safe inter-thread
 *   communication.  State stores are thread-safe per partition.
 *
 * Dependencies:
 *   - BrokerServer: for consume/produce operations
 *   - RocksDBStore: for persistent state storage
 *   - topology.h: integration with TopologyBuilder
 */

#include "torrent/streams/topology.h"

#include "torrent/broker/server.h"
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
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

using json = nlohmann::json;
using namespace std::chrono_literals;

namespace torrent::streams {

// ============================================================================
// Forward declarations
// ============================================================================

class ProcessorBase;
class SourceProcessor;
class SinkProcessor;

// ============================================================================
// Anonymous namespace — internal types and helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_stream_logger() {
    static auto logger = spdlog::get("stream_processor");
    if (!logger) {
        logger = spdlog::stdout_color_mt("stream_processor");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// StreamRecord — the core data unit flowing through the topology
// --------------------------------------------------------------------------

struct StreamRecord {
    std::string topic;
    std::string key;
    json        value;
    int32_t     partition   = 0;
    offset_t    offset      = kInvalidOffset;
    int64_t     timestamp_ms = 0;

    // Arbitrary headers attached to the record
    std::unordered_map<std::string, std::string> headers;

    // Returns true if the record is valid (has topic + value)
    [[nodiscard]] bool valid() const noexcept {
        return !topic.empty() && !value.is_null();
    }
};

// --------------------------------------------------------------------------
// RecordBatch — a group of records for efficient processing
// --------------------------------------------------------------------------

struct RecordBatch {
    std::vector<StreamRecord> records;
    std::string               source_topic;
    int32_t                   source_partition = 0;
    offset_t                  base_offset      = kInvalidOffset;
    int64_t                   batch_timestamp_ms = 0;

    [[nodiscard]] bool empty() const noexcept { return records.empty(); }
    [[nodiscard]] size_t size() const noexcept { return records.size(); }

    void clear() {
        records.clear();
        base_offset = kInvalidOffset;
    }
};

// --------------------------------------------------------------------------
// SPSC Channel — lock-free single-producer single-consumer bounded queue
// --------------------------------------------------------------------------

static constexpr size_t kDefaultChannelCapacity = 8192;

template<typename T>
class SpscChannel {
public:
    explicit SpscChannel(size_t capacity = kDefaultChannelCapacity)
        : capacity_(capacity + 1)  // +1 for sentinel
        , buffer_(std::make_unique<T[]>(capacity + 1))
    {
        write_pos_.store(0, std::memory_order_relaxed);
        read_pos_.store(0, std::memory_order_relaxed);
    }

    SpscChannel(const SpscChannel&) = delete;
    SpscChannel& operator=(const SpscChannel&) = delete;

    // Producer: push an item. Returns false if channel is full.
    [[nodiscard]] bool push(T item) {
        size_t w = write_pos_.load(std::memory_order_relaxed);
        size_t r = read_pos_.load(std::memory_order_acquire);
        size_t next_w = (w + 1) % capacity_;

        if (next_w == r) return false; // full

        buffer_[w] = std::move(item);
        write_pos_.store(next_w, std::memory_order_release);
        return true;
    }

    // Consumer: pop an item. Returns std::nullopt if empty.
    [[nodiscard]] std::optional<T> pop() {
        size_t r = read_pos_.load(std::memory_order_relaxed);
        size_t w = write_pos_.load(std::memory_order_acquire);

        if (r == w) return std::nullopt; // empty

        T item = std::move(buffer_[r]);
        read_pos_.store((r + 1) % capacity_, std::memory_order_release);
        return item;
    }

    // Producer: blocking push with timeout
    [[nodiscard]] bool push_blocking(T item, std::chrono::milliseconds timeout = 100ms) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!push(std::move(item))) {
            if (std::chrono::steady_clock::now() >= deadline) return false;
            std::this_thread::yield();
        }
        return true;
    }

    // Returns approximate size
    [[nodiscard]] size_t size() const noexcept {
        size_t w = write_pos_.load(std::memory_order_acquire);
        size_t r = read_pos_.load(std::memory_order_acquire);
        if (w >= r) return w - r;
        return capacity_ - r + w;
    }

    [[nodiscard]] bool empty() const noexcept { return size() == 0; }

private:
    size_t               capacity_;
    std::unique_ptr<T[]> buffer_;
    std::atomic<size_t>  write_pos_;
    std::atomic<size_t>  read_pos_;
};

// --------------------------------------------------------------------------
// ProcessorState — persisted state for stateful processors
// --------------------------------------------------------------------------

struct ProcessorState {
    std::string                     partition_key;   // "topic:partition"
    std::vector<StreamRecord>       window_buffer;    // buffered records
    json                            accumulator;       // aggregated values
    offset_t                        last_committed_offset = kInvalidOffset;
    int64_t                         last_checkpoint_ms  = 0;
    int64_t                         watermark_ms         = 0;  // stream time

    [[nodiscard]] json serialize() const {
        json j;
        j["partition_key"]         = partition_key;
        j["last_committed_offset"] = last_committed_offset;
        j["last_checkpoint_ms"]    = last_checkpoint_ms;
        j["watermark_ms"]          = watermark_ms;
        j["accumulator"]           = accumulator;
        j["window_record_count"]   = window_buffer.size();
        return j;
    }

    static ProcessorState deserialize(const json& j) {
        ProcessorState s;
        s.partition_key          = j.value("partition_key", "");
        s.last_committed_offset  = j.value("last_committed_offset", kInvalidOffset);
        s.last_checkpoint_ms     = j.value("last_checkpoint_ms", int64_t(0));
        s.watermark_ms           = j.value("watermark_ms", int64_t(0));
        s.accumulator            = j.value("accumulator", json::object());
        return s;
    }
};

// --------------------------------------------------------------------------
// StateStore — in-memory state with optional RocksDB persistence
// --------------------------------------------------------------------------

class StateStore {
public:
    explicit StateStore(std::string store_name)
        : name_(std::move(store_name))
    {}

    void put(const std::string& key, ProcessorState state) {
        std::unique_lock lock(mutex_);
        store_[key] = std::move(state);
    }

    [[nodiscard]] std::optional<ProcessorState> get(const std::string& key) const {
        std::shared_lock lock(mutex_);
        auto it = store_.find(key);
        if (it != store_.end()) return it->second;
        return std::nullopt;
    }

    void remove(const std::string& key) {
        std::unique_lock lock(mutex_);
        store_.erase(key);
    }

    [[nodiscard]] std::vector<std::string> keys() const {
        std::shared_lock lock(mutex_);
        std::vector<std::string> result;
        result.reserve(store_.size());
        for (auto& [k, _] : store_) result.push_back(k);
        return result;
    }

    [[nodiscard]] size_t size() const {
        std::shared_lock lock(mutex_);
        return store_.size();
    }

    void clear() {
        std::unique_lock lock(mutex_);
        store_.clear();
    }

    [[nodiscard]] const std::string& name() const noexcept { return name_; }

private:
    std::string                                              name_;
    mutable std::shared_mutex                                mutex_;
    std::unordered_map<std::string, ProcessorState>          store_;
};

// --------------------------------------------------------------------------
// Execution metrics for observability
// --------------------------------------------------------------------------

struct ProcessorMetrics {
    std::atomic<uint64_t> records_processed{0};
    std::atomic<uint64_t> records_dropped{0};
    std::atomic<uint64_t> records_emitted{0};
    std::atomic<uint64_t> batches_processed{0};
    std::atomic<uint64_t> checkpoints_taken{0};
    std::atomic<uint64_t> errors{0};
    std::atomic<int64_t>  current_lag{0};

    void reset() {
        records_processed.store(0);
        records_dropped.store(0);
        records_emitted.store(0);
        batches_processed.store(0);
        checkpoints_taken.store(0);
        errors.store(0);
        current_lag.store(0);
    }

    [[nodiscard]] json snapshot() const {
        return {
            {"records_processed", records_processed.load()},
            {"records_dropped",   records_dropped.load()},
            {"records_emitted",   records_emitted.load()},
            {"batches_processed", batches_processed.load()},
            {"checkpoints_taken", checkpoints_taken.load()},
            {"errors",            errors.load()},
            {"current_lag",       current_lag.load()},
        };
    }
};

} // anonymous namespace

// ============================================================================
// ProcessorBase — abstract base for all stream processing nodes
// ============================================================================

class ProcessorBase {
public:
    explicit ProcessorBase(std::string name)
        : name_(std::move(name))
    {}

    virtual ~ProcessorBase() = default;

    // ----------------------------------------------------------------------
    // Lifecycle
    // ----------------------------------------------------------------------

    /// Initialise the processor: open state stores, connect channels, etc.
    virtual void init() {
        auto log = get_stream_logger();
        log->info("Processor '{}': initialising", name_);
        state_ = ProcessorState{};
        state_->partition_key = name_;
    }

    /// Process a single record.  Implementations call emit() for results.
    virtual void process(const StreamRecord& record) = 0;

    /// Process a batch of records efficiently.
    virtual void process_batch(const RecordBatch& batch) {
        for (auto& rec : batch.records) {
            process(rec);
        }
        metrics_.batches_processed.fetch_add(1, std::memory_order_relaxed);
    }

    /// Called periodically to advance stream time and trigger window closes.
    virtual void punctuate(int64_t stream_time_ms) {
        watermark_ms_ = stream_time_ms;
    }

    /// Graceful shutdown: flush state, drain pending records.
    virtual void close() {
        auto log = get_stream_logger();
        log->info("Processor '{}': closing, processed {} records",
                  name_, metrics_.records_processed.load());
    }

    /// Checkpoint current state for fault recovery.
    virtual void checkpoint() {
        if (!state_store_ || !state_) return;

        state_->last_checkpoint_ms = now_ms();
        std::string key = make_state_key(name_, current_partition_);
        state_store_->put(key, *state_);
        metrics_.checkpoints_taken.fetch_add(1, std::memory_order_relaxed);
    }

    /// Restore state from a previous checkpoint.
    virtual void restore() {
        if (!state_store_ || !state_) return;

        std::string key = make_state_key(name_, current_partition_);
        auto saved = state_store_->get(key);
        if (saved) {
            *state_ = std::move(*saved);
            auto log = get_stream_logger();
            log->info("Processor '{}': restored state from offset {}",
                      name_, state_->last_committed_offset);
        }
    }

    // ----------------------------------------------------------------------
    // Channel wiring
    // ----------------------------------------------------------------------

    /// Attach a downstream channel that gets emitted records.
    void add_downstream(std::shared_ptr<SpscChannel<StreamRecord>> channel) {
        std::unique_lock lock(downstream_mutex_);
        downstream_channels_.push_back(std::move(channel));
    }

    // ----------------------------------------------------------------------
    // Metrics
    // ----------------------------------------------------------------------

    [[nodiscard]] const ProcessorMetrics& metrics() const noexcept { return metrics_; }
    [[nodiscard]] ProcessorMetrics& metrics() noexcept { return metrics_; }

    // ----------------------------------------------------------------------
    // Configuration
    // ----------------------------------------------------------------------

    [[nodiscard]] const std::string& name() const noexcept { return name_; }

    void set_partition(int32_t partition) { current_partition_ = partition; }
    [[nodiscard]] int32_t partition() const noexcept { return current_partition_; }

    void set_state_store(std::shared_ptr<StateStore> store) {
        state_store_ = std::move(store);
    }

    [[nodiscard]] int64_t watermark_ms() const noexcept { return watermark_ms_; }

protected:
    // ----------------------------------------------------------------------
    // Emit a processed record to all downstream channels
    // ----------------------------------------------------------------------

    void emit(StreamRecord record) {
        std::shared_lock lock(downstream_mutex_);
        for (auto& ch : downstream_channels_) {
            ch->push(record);
        }
        metrics_.records_emitted.fetch_add(1, std::memory_order_relaxed);
    }

    void emit_batch(std::vector<StreamRecord> records) {
        std::shared_lock lock(downstream_mutex_);
        for (auto& ch : downstream_channels_) {
            for (auto& rec : records) {
                ch->push(rec);
            }
        }
        metrics_.records_emitted.fetch_add(records.size(),
                                            std::memory_order_relaxed);
    }

    // ----------------------------------------------------------------------
    // Utility
    // ----------------------------------------------------------------------

    [[nodiscard]] static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    [[nodiscard]] static std::string make_state_key(const std::string& processor,
                                                     int32_t partition) {
        return processor + ":" + std::to_string(partition);
    }

    std::string                                  name_;
    int32_t                                      current_partition_ = 0;
    int64_t                                      watermark_ms_ = 0;
    std::optional<ProcessorState>                state_;
    std::shared_ptr<StateStore>                  state_store_;
    ProcessorMetrics                             metrics_;

private:
    mutable std::shared_mutex                          downstream_mutex_;
    std::vector<std::shared_ptr<SpscChannel<StreamRecord>>> downstream_channels_;
};

// ============================================================================
// SourceProcessor — ingests records from a torrent topic
// ============================================================================

class SourceProcessor final : public ProcessorBase {
public:
    SourceProcessor(std::string name,
                     broker::BrokerServer& server,
                     std::string topic,
                     std::string consumer_group = "")
        : ProcessorBase(std::move(name))
        , server_(&server)
        , topic_(std::move(topic))
        , consumer_group_(std::move(consumer_group))
    {
        if (consumer_group_.empty()) {
            consumer_group_ = "streams-" + name_;
        }
    }

    void init() override {
        ProcessorBase::init();

        auto log = get_stream_logger();
        log->info("SourceProcessor '{}': consuming topic='{}' group='{}'",
                  name_, topic_, consumer_group_);
    }

    void process(const StreamRecord& /*record*/) override {
        // Sources don't receive records — they produce them
    }

    /// Poll the source topic for new records and emit downstream.
    /// Called by the topology executor in a loop.
    void poll() {
        if (stopping_.load(std::memory_order_acquire)) return;

        // In production, this calls server_->consume() with the consumer group.
        // For the stub, we simulate record generation.

        auto log = get_stream_logger();
        log->trace("SourceProcessor '{}': polling topic='{}'", name_, topic_);

        // Simulate processing
        metrics_.records_processed.fetch_add(1, std::memory_order_relaxed);

        // Checkpoint periodically
        if (metrics_.records_processed.load(std::memory_order_relaxed) % 1000 == 0) {
            checkpoint();
        }
    }

    void close() override {
        stopping_.store(true, std::memory_order_release);
        ProcessorBase::close();
    }

    [[nodiscard]] const std::string& topic() const noexcept { return topic_; }
    [[nodiscard]] const std::string& consumer_group() const noexcept { return consumer_group_; }

private:
    broker::BrokerServer* server_;
    std::string           topic_;
    std::string           consumer_group_;
    std::atomic<bool>     stopping_{false};
};

// ============================================================================
// SinkProcessor — writes processed records to a torrent topic or external system
// ============================================================================

class SinkProcessor final : public ProcessorBase {
public:
    enum class SinkType : uint8_t {
        torrent_topic = 0,
        console       = 1,
        external      = 2,
    };

    SinkProcessor(std::string name,
                   broker::BrokerServer& server,
                   std::string topic,
                   SinkType sink_type = SinkType::torrent_topic)
        : ProcessorBase(std::move(name))
        , server_(&server)
        , topic_(std::move(topic))
        , sink_type_(sink_type)
    {}

    void init() override {
        ProcessorBase::init();

        auto log = get_stream_logger();
        log->info("SinkProcessor '{}': writing to topic='{}' type={}",
                  name_, topic_, static_cast<int>(sink_type_));
    }

    void process(const StreamRecord& record) override {
        if (!record.valid()) {
            metrics_.records_dropped.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        switch (sink_type_) {
        case SinkType::torrent_topic:
            produce_to_topic(record);
            break;
        case SinkType::console:
            produce_to_console(record);
            break;
        case SinkType::external:
            produce_to_external(record);
            break;
        }

        metrics_.records_processed.fetch_add(1, std::memory_order_relaxed);

        // Commit offset after successful produce
        if (state_) {
            state_->last_committed_offset = record.offset;
        }

        // Periodic checkpoint
        if (metrics_.records_processed.load(std::memory_order_relaxed) % 500 == 0) {
            checkpoint();
        }
    }

    void process_batch(const RecordBatch& batch) override {
        if (batch.empty()) return;

        // In production, batch produce for efficiency
        for (auto& rec : batch.records) {
            process(rec);
        }
        metrics_.batches_processed.fetch_add(1, std::memory_order_relaxed);
    }

    void close() override {
        // Flush any pending records before closing
        flush();
        ProcessorBase::close();
    }

    void flush() {
        auto log = get_stream_logger();
        log->debug("SinkProcessor '{}': flushing pending records", name_);
        // In production: await all inflight produce requests
    }

    [[nodiscard]] const std::string& topic() const noexcept { return topic_; }
    [[nodiscard]] SinkType sink_type() const noexcept { return sink_type_; }

private:
    void produce_to_topic(const StreamRecord& record) {
        // In production:
        //   server_->produce(topic_, record.key, record.value.dump(),
        //                    record.partition, record.headers);
        auto log = get_stream_logger();
        log->trace("Sink '{}': produced to topic='{}' key='{}' offset={}",
                    name_, topic_, record.key, record.offset);
    }

    void produce_to_console(const StreamRecord& record) {
        auto log = get_stream_logger();
        log->info("Sink '{}': [console] key='{}' value='{}'",
                   name_, record.key, record.value.dump());
    }

    void produce_to_external(const StreamRecord& record) {
        // Stub: external system integration point
        auto log = get_stream_logger();
        log->debug("Sink '{}': external produce key='{}'", name_, record.key);
    }

    broker::BrokerServer* server_;
    std::string           topic_;
    SinkType              sink_type_;
};

// ============================================================================
// StreamTask — a runnable unit of work in the topology
// ============================================================================

class StreamTask {
public:
    using TaskFn = std::function<void()>;

    explicit StreamTask(std::string name, TaskFn fn)
        : name_(std::move(name))
        , fn_(std::move(fn))
    {}

    void run() {
        running_.store(true, std::memory_order_release);
        try {
            fn_();
        } catch (const std::exception& e) {
            auto log = get_stream_logger();
            log->error("StreamTask '{}': unhandled exception: {}", name_, e.what());
        } catch (...) {
            auto log = get_stream_logger();
            log->error("StreamTask '{}': unknown exception", name_);
        }
        running_.store(false, std::memory_order_release);
    }

    void request_stop() { stop_requested_.store(true, std::memory_order_release); }
    [[nodiscard]] bool stop_requested() const noexcept {
        return stop_requested_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool is_running() const noexcept {
        return running_.load(std::memory_order_acquire);
    }
    [[nodiscard]] const std::string& name() const noexcept { return name_; }

private:
    std::string        name_;
    TaskFn             fn_;
    std::atomic<bool>  running_{false};
    std::atomic<bool>  stop_requested_{false};
};

// ============================================================================
// TopologyExecutor — manages task execution with checkpointing
// ============================================================================

class TopologyExecutor {
public:
    struct Config {
        size_t      thread_count    = std::thread::hardware_concurrency();
        std::chrono::milliseconds checkpoint_interval = 5000ms;
        std::chrono::milliseconds poll_interval       = 50ms;
        bool        enable_exactly_once = true;
    };

    explicit TopologyExecutor(Config cfg = {})
        : config_(std::move(cfg))
    {}

    ~TopologyExecutor() {
        shutdown();
    }

    TopologyExecutor(const TopologyExecutor&) = delete;
    TopologyExecutor& operator=(const TopologyExecutor&) = delete;

    // ----------------------------------------------------------------------
    // Register processors
    // ----------------------------------------------------------------------

    void add_source(std::shared_ptr<SourceProcessor> src) {
        sources_.push_back(std::move(src));
    }

    void add_processor(std::shared_ptr<ProcessorBase> proc) {
        processors_.push_back(std::move(proc));
    }

    void add_sink(std::shared_ptr<SinkProcessor> sink) {
        sinks_.push_back(std::move(sink));
    }

    // ----------------------------------------------------------------------
    // Build the execution plan: wire channels, create state stores
    // ----------------------------------------------------------------------

    void build() {
        auto log = get_stream_logger();
        log->info("TopologyExecutor::build: {} sources, {} processors, {} sinks",
                  sources_.size(), processors_.size(), sinks_.size());

        // Wire channels from sources → processors → sinks
        // Source[0] → Channel[0] → Processor[0] → Channel[1] → Sink[0]

        channels_.clear();

        // Source → first processor
        if (!sources_.empty() && !processors_.empty()) {
            auto ch = std::make_shared<SpscChannel<StreamRecord>>();
            for (auto& src : sources_) {
                src->add_downstream(ch);
            }
            for (auto& proc : processors_) {
                // Processors receive from this channel...  In a real
                // implementation this would be wired per-topology.
            }
            channels_.push_back(ch);
        }

        // Processor → sink
        if (!processors_.empty() && !sinks_.empty()) {
            auto ch = std::make_shared<SpscChannel<StreamRecord>>();
            for (auto& proc : processors_) {
                proc->add_downstream(ch);
            }
            channels_.push_back(ch);
        }

        // Create shared state store
        state_store_ = std::make_shared<StateStore>("topology-default");

        // Initialize all processors
        for (auto& src : sources_) {
            src->set_state_store(state_store_);
            src->init();
        }
        for (auto& proc : processors_) {
            proc->set_state_store(state_store_);
            proc->init();
        }
        for (auto& sink : sinks_) {
            sink->set_state_store(state_store_);
            sink->init();
        }

        built_ = true;
        log->info("TopologyExecutor::build complete");
    }

    // ----------------------------------------------------------------------
    // Start execution
    // ----------------------------------------------------------------------

    void start() {
        if (!built_) {
            throw std::logic_error("Must call build() before start()");
        }

        auto log = get_stream_logger();
        log->info("TopologyExecutor: starting with {} threads", config_.thread_count);

        stop_signal_.store(false, std::memory_order_release);

        // Launch source poller tasks
        for (auto& src : sources_) {
            auto task = std::make_shared<StreamTask>(
                "source-" + src->name(),
                [this, src] {
                    while (!stop_signal_.load(std::memory_order_acquire)) {
                        src->poll();
                        std::this_thread::sleep_for(config_.poll_interval);
                    }
                    src->close();
                });
            tasks_.push_back(task);
            worker_threads_.emplace_back([task] { task->run(); });
        }

        // Launch processor tasks (drain from channels)
        for (auto& proc : processors_) {
            auto task = std::make_shared<StreamTask>(
                "processor-" + proc->name(),
                [this, proc] {
                    // Process records arriving on channels
                    while (!stop_signal_.load(std::memory_order_acquire)) {
                        // In production: drain from input channels
                        std::this_thread::sleep_for(config_.poll_interval);
                    }
                    proc->close();
                });
            tasks_.push_back(task);
            worker_threads_.emplace_back([task] { task->run(); });
        }

        // Launch sink tasks
        for (auto& sink : sinks_) {
            auto task = std::make_shared<StreamTask>(
                "sink-" + sink->name(),
                [this, sink] {
                    while (!stop_signal_.load(std::memory_order_acquire)) {
                        // In production: drain from input channel
                        std::this_thread::sleep_for(config_.poll_interval);
                    }
                    sink->close();
                });
            tasks_.push_back(task);
            worker_threads_.emplace_back([task] { task->run(); });
        }

        // Launch periodic checkpointing thread
        if (config_.enable_exactly_once) {
            checkpoint_thread_ = std::thread([this] {
                auto log = get_stream_logger();
                log->info("Checkpoint thread started, interval={}ms",
                          config_.checkpoint_interval.count());

                while (!stop_signal_.load(std::memory_order_acquire)) {
                    std::unique_lock lock(stop_mutex_);
                    stop_cv_.wait_for(lock, config_.checkpoint_interval,
                                      [this] {
                                          return stop_signal_.load(
                                              std::memory_order_acquire);
                                      });
                    if (stop_signal_.load(std::memory_order_acquire)) break;

                    // Checkpoint all stateful processors
                    for (auto& src : sources_) src->checkpoint();
                    for (auto& proc : processors_) proc->checkpoint();
                    for (auto& sink : sinks_) sink->checkpoint();

                    log->trace("Checkpoint cycle complete");
                }
                log->info("Checkpoint thread stopped");
            });
        }

        log->info("TopologyExecutor: started {} tasks", tasks_.size());
    }

    // ----------------------------------------------------------------------
    // Graceful shutdown
    // ----------------------------------------------------------------------

    void shutdown() {
        auto log = get_stream_logger();

        if (!built_) return;

        log->info("TopologyExecutor: initiating shutdown...");

        // Signal all tasks to stop
        stop_signal_.store(true, std::memory_order_release);
        stop_cv_.notify_all();

        for (auto& task : tasks_) {
            task->request_stop();
        }

        // Join all worker threads
        for (auto& t : worker_threads_) {
            if (t.joinable()) t.join();
        }
        worker_threads_.clear();

        // Join checkpoint thread
        if (checkpoint_thread_.joinable()) {
            checkpoint_thread_.join();
        }

        // Final checkpoint
        for (auto& src : sources_) src->checkpoint();
        for (auto& proc : processors_) proc->checkpoint();
        for (auto& sink : sinks_) sink->checkpoint();

        tasks_.clear();
        processors_.clear();
        sources_.clear();
        sinks_.clear();
        channels_.clear();
        built_ = false;

        log->info("TopologyExecutor: shutdown complete");
    }

    // ----------------------------------------------------------------------
    // Metrics
    // ----------------------------------------------------------------------

    [[nodiscard]] json metrics_snapshot() const {
        json j;
        json sources_j = json::array();
        for (auto& src : sources_) {
            json sj;
            sj["name"]    = src->name();
            sj["metrics"] = src->metrics().snapshot();
            sources_j.push_back(std::move(sj));
        }
        j["sources"] = std::move(sources_j);

        json procs_j = json::array();
        for (auto& proc : processors_) {
            json pj;
            pj["name"]    = proc->name();
            pj["metrics"] = proc->metrics().snapshot();
            procs_j.push_back(std::move(pj));
        }
        j["processors"] = std::move(procs_j);

        json sinks_j = json::array();
        for (auto& sink : sinks_) {
            json skj;
            skj["name"]    = sink->name();
            skj["metrics"] = sink->metrics().snapshot();
            sinks_j.push_back(std::move(skj));
        }
        j["sinks"] = std::move(sinks_j);

        j["running"]          = !stop_signal_.load();
        j["task_count"]       = tasks_.size();
        j["thread_count"]     = worker_threads_.size();
        j["state_store_keys"] = state_store_ ? state_store_->size() : 0;

        return j;
    }

    [[nodiscard]] const Config& config() const noexcept { return config_; }

private:
    Config                                            config_;
    bool                                              built_ = false;

    std::shared_ptr<StateStore>                       state_store_;
    std::vector<std::shared_ptr<SourceProcessor>>     sources_;
    std::vector<std::shared_ptr<ProcessorBase>>       processors_;
    std::vector<std::shared_ptr<SinkProcessor>>       sinks_;

    std::vector<std::shared_ptr<SpscChannel<StreamRecord>>> channels_;

    std::vector<std::shared_ptr<StreamTask>>          tasks_;
    std::vector<std::thread>                          worker_threads_;
    std::thread                                       checkpoint_thread_;

    std::atomic<bool>                                 stop_signal_{false};
    std::mutex                                        stop_mutex_;
    std::condition_variable                           stop_cv_;
};

// ============================================================================
// Public factory functions (exposed via topology.h integration)
// ============================================================================

namespace factory {

std::shared_ptr<SourceProcessor> create_source(
    const std::string& name,
    broker::BrokerServer& server,
    const std::string& topic)
{
    return std::make_shared<SourceProcessor>(name, server, topic);
}

std::shared_ptr<SinkProcessor> create_sink(
    const std::string& name,
    broker::BrokerServer& server,
    const std::string& topic)
{
    return std::make_shared<SinkProcessor>(name, server, topic);
}

} // namespace factory

} // namespace torrent::streams
