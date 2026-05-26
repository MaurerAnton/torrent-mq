/**
 * grouped_processor.cpp — GroupedProcessor: Per-Key Grouping in Stream Topology
 *
 * Groups records by key (from record key, value field, or header) before
 * downstream processing. Per-key state isolation, per-key windowing, TTL-based
 * key eviction, LRU enforcement, and repartition triggers on key space changes.
 */

#include "torrent/streams/topology.h"

#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <set>
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

namespace torrent::streams {

// ============================================================================
// Anonymous namespace — internal types and helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_grouped_logger() {
    static auto logger = spdlog::get("grouped_processor");
    if (!logger) {
        logger = spdlog::stdout_color_mt("grouped_processor");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// KeySource — where to extract the key from
// --------------------------------------------------------------------------

enum class KeySource : uint8_t {
    record_key   = 0,  // use the record's key field
    value_field  = 1,  // extract from a named field in the JSON value
    header       = 2,  // extract from a record header
    custom       = 3,  // use a user-supplied function
};

[[nodiscard]] std::string_view key_source_name(KeySource ks) {
    switch (ks) {
    case KeySource::record_key:  return "record_key";
    case KeySource::value_field: return "value_field";
    case KeySource::header:      return "header";
    case KeySource::custom:      return "custom";
    }
    return "unknown";
}

[[nodiscard]] KeySource parse_key_source(std::string_view s) {
    if (s == "value_field" || s == "value") return KeySource::value_field;
    if (s == "header")                      return KeySource::header;
    if (s == "custom")                      return KeySource::custom;
    return KeySource::record_key;
}

// --------------------------------------------------------------------------
// GroupedConfig — configuration for a grouped processor
// --------------------------------------------------------------------------

struct GroupedConfig {
    KeySource   key_source          = KeySource::record_key;
    std::string key_field_name;          // field name for value_field / header modes
    int64_t     key_ttl_ms          = 300'000; // evict after 5 min idle
    int64_t     max_keys             = 100'000; // max distinct keys before eviction
    int64_t     max_buffer_per_key   = 10'000;  // max buffered records per key
    int64_t     eviction_check_ms    = 60'000;  // check for eviction every N ms
    int64_t     window_size_ms       = 60'000;  // default window size per key
    int64_t     grace_period_ms      = 5'000;   // late arrival tolerance
    bool        flush_on_eviction    = true;     // emit buffered records on eviction
    bool        repartition_on_change = false;   // trigger repartition on key changes
    int64_t     repartition_threshold = 50;      // % change to trigger repartition

    [[nodiscard]] static GroupedConfig from_json(const json& j) {
        GroupedConfig cfg;
        cfg.key_source      = parse_key_source(
            j.value("key_source", "record_key"));
        cfg.key_field_name  = j.value("key_field", "");
        cfg.key_ttl_ms      = j.value("key_ttl_ms", int64_t(300'000));
        cfg.max_keys        = j.value("max_keys", int64_t(100'000));
        cfg.max_buffer_per_key = j.value("max_buffer_per_key",
                                          int64_t(10'000));
        cfg.eviction_check_ms  = j.value("eviction_check_ms",
                                          int64_t(60'000));
        cfg.window_size_ms  = j.value("window_size_ms", int64_t(60'000));
        cfg.grace_period_ms = j.value("grace_period_ms", int64_t(5'000));
        cfg.flush_on_eviction = j.value("flush_on_eviction", true);
        cfg.repartition_on_change = j.value("repartition_on_change", false);
        cfg.repartition_threshold = j.value("repartition_threshold",
                                             int64_t(50));
        return cfg;
    }

    [[nodiscard]] json to_json() const {
        json j;
        j["key_source"]      = std::string(key_source_name(key_source));
        j["key_field"]       = key_field_name;
        j["key_ttl_ms"]      = key_ttl_ms;
        j["max_keys"]        = max_keys;
        j["max_buffer_per_key"] = max_buffer_per_key;
        j["eviction_check_ms"]  = eviction_check_ms;
        j["window_size_ms"]  = window_size_ms;
        j["grace_period_ms"] = grace_period_ms;
        j["flush_on_eviction"] = flush_on_eviction;
        j["repartition_on_change"] = repartition_on_change;
        j["repartition_threshold"] = repartition_threshold;
        return j;
    }
};

// --------------------------------------------------------------------------
// GroupedRecord — a record annotated with grouping metadata
// --------------------------------------------------------------------------

struct GroupedRecord {
    std::string topic;
    std::string group_key;
    json        value;
    int32_t     partition    = 0;
    offset_t    offset       = kInvalidOffset;
    int64_t     timestamp_ms = 0;
    int64_t     arrival_ms   = 0;  // when we received it
    std::unordered_map<std::string, std::string> headers;

    [[nodiscard]] bool valid() const noexcept {
        return !group_key.empty() && !value.is_null();
    }
};

// --------------------------------------------------------------------------
// PerKeyState — isolated state for a single grouping key
// --------------------------------------------------------------------------

struct PerKeyState {
    std::string              key;
    std::deque<GroupedRecord> buffer;          // pending records
    json                     accumulator;       // ongoing aggregation
    int64_t                  last_access_ms  = 0;
    int64_t                  created_at_ms   = 0;
    int64_t                  record_count    = 0;
    int64_t                  emitted_count   = 0;
    int64_t                  watermark_ms    = 0;
    offset_t                 last_offset     = kInvalidOffset;
    bool                     eviction_pending = false;

    mutable std::shared_mutex mtx;

    explicit PerKeyState(std::string k)
        : key(std::move(k))
    {
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        last_access_ms = now;
        created_at_ms  = now;
    }

    /// Add a record to the buffer
    bool add_record(GroupedRecord rec, int64_t max_buffer) {
        std::unique_lock lock(mtx);
        if (static_cast<int64_t>(buffer.size()) >= max_buffer) {
            return false; // buffer full
        }
        if (rec.timestamp_ms > watermark_ms) {
            watermark_ms = rec.timestamp_ms;
        }
        buffer.push_back(std::move(rec));
        record_count++;
        last_access_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        return true;
    }

    /// Drain all buffered records
    [[nodiscard]] std::vector<GroupedRecord> drain() {
        std::unique_lock lock(mtx);
        std::vector<GroupedRecord> result;
        result.reserve(buffer.size());
        while (!buffer.empty()) {
            result.push_back(std::move(buffer.front()));
            buffer.pop_front();
        }
        return result;
    }

    /// Split buffer into windows based on timestamp
    [[nodiscard]] std::vector<std::vector<GroupedRecord>>
    split_by_windows(int64_t window_size_ms) {
        std::unique_lock lock(mtx);
        std::vector<std::vector<GroupedRecord>> windows;

        if (buffer.empty()) return windows;

        // Sort buffer by timestamp for deterministic window assignment
        std::sort(buffer.begin(), buffer.end(),
            [](const GroupedRecord& a, const GroupedRecord& b) {
                return a.timestamp_ms < b.timestamp_ms;
            });

        int64_t window_start = (buffer.front().timestamp_ms / window_size_ms)
                             * window_size_ms;
        int64_t window_end   = window_start + window_size_ms;
        std::vector<GroupedRecord> current_window;

        for (auto& rec : buffer) {
            if (rec.timestamp_ms >= window_end) {
                if (!current_window.empty()) {
                    windows.push_back(std::move(current_window));
                    current_window.clear();
                }
                window_start = (rec.timestamp_ms / window_size_ms)
                             * window_size_ms;
                window_end = window_start + window_size_ms;
            }
            current_window.push_back(std::move(rec));
        }

        if (!current_window.empty()) {
            windows.push_back(std::move(current_window));
        }

        buffer.clear();
        return windows;
    }

    /// Check if this key is stale
    [[nodiscard]] bool is_stale(int64_t ttl_ms) const {
        std::shared_lock lock(mtx);
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        return (now - last_access_ms) > ttl_ms;
    }

    [[nodiscard]] json status() const {
        std::shared_lock lock(mtx);
        return {
            {"key",              key},
            {"buffered",         buffer.size()},
            {"record_count",     record_count},
            {"emitted_count",    emitted_count},
            {"watermark_ms",     watermark_ms},
            {"last_access_ms",   last_access_ms},
            {"created_at_ms",    created_at_ms},
            {"eviction_pending", eviction_pending},
        };
    }
};

// --------------------------------------------------------------------------
// KeyExtractor — extracts the grouping key from a record
// --------------------------------------------------------------------------

class KeyExtractor {
public:
    KeyExtractor(KeySource source, std::string field_name)
        : source_(source)
        , field_name_(std::move(field_name))
    {}

    /// Extract the grouping key from a GroupedRecord.
    [[nodiscard]] std::string extract(const GroupedRecord& rec) const {
        switch (source_) {
        case KeySource::record_key:
            return rec.group_key;

        case KeySource::value_field: {
            if (field_name_.empty()) return rec.group_key;
            auto it = rec.value.find(field_name_);
            if (it != rec.value.end()) {
                if (it->is_string()) return it->get<std::string>();
                return it->dump();
            }
            return rec.group_key; // fallback
        }

        case KeySource::header: {
            if (field_name_.empty()) return rec.group_key;
            auto it = rec.headers.find(field_name_);
            if (it != rec.headers.end()) return it->second;
            return rec.group_key; // fallback
        }

        case KeySource::custom:
            // Custom extraction is handled by the user-supplied function
            return rec.group_key;
        }
        return rec.group_key;
    }

    [[nodiscard]] KeySource source() const noexcept { return source_; }
    [[nodiscard]] const std::string& field_name() const noexcept {
        return field_name_;
    }

private:
    KeySource   source_;
    std::string field_name_;
};

// --------------------------------------------------------------------------
// Time helpers
// --------------------------------------------------------------------------

[[nodiscard]] int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// --------------------------------------------------------------------------
// Null-key sentinel for records with no key
// --------------------------------------------------------------------------

inline constexpr std::string_view kNullKey = "__null_group__";

} // anonymous namespace

// ============================================================================
// GroupedProcessor — groups records by key before forwarding downstream
// ============================================================================

class GroupedProcessor {
public:
    /// Callback invoked when a window of grouped records is ready.
    /// Receives the group key and a vector of records in that window.
    using WindowCallback = std::function<void(
        const std::string& key,
        std::vector<GroupedRecord> records)>;

    /// Callback invoked when a key is evicted.
    using EvictionCallback = std::function<void(
        const std::string& key,
        std::vector<GroupedRecord> flushed_records)>;

    /// Custom key extraction function (for KeySource::custom).
    using CustomExtractor = std::function<std::string(const GroupedRecord&)>;

    explicit GroupedProcessor(std::string name,
                               GroupedConfig cfg = {})
        : name_(std::move(name))
        , config_(std::move(cfg))
        , key_extractor_(config_.key_source, config_.key_field_name)
    {}

    ~GroupedProcessor() { stop(); }

    GroupedProcessor(const GroupedProcessor&) = delete;
    GroupedProcessor& operator=(const GroupedProcessor&) = delete;

    // ----------------------------------------------------------------------
    // Lifecycle
    // ----------------------------------------------------------------------

    /// Start the processor.  Begins the background eviction thread.
    void start(WindowCallback window_cb = {},
               EvictionCallback eviction_cb = {})
    {
        if (running_.exchange(true)) return;

        window_callback_   = std::move(window_cb);
        eviction_callback_ = std::move(eviction_cb);

        get_grouped_logger()->info(
            "GroupedProcessor '{}': starting, key_source={} ttl={}ms max_keys={}",
            name_, key_source_name(config_.key_source),
            config_.key_ttl_ms, config_.max_keys);

        if (config_.key_ttl_ms > 0) {
            eviction_thread_ = std::thread([this] { eviction_loop(); });
        }
    }

    /// Stop the processor.  Flushes all buffered records.
    void stop() {
        if (!running_.exchange(false)) return;

        eviction_cv_.notify_all();

        if (eviction_thread_.joinable()) {
            eviction_thread_.join();
        }

        // Flush remaining records
        flush_all();

        get_grouped_logger()->info(
            "GroupedProcessor '{}': stopped, keys={} records={}",
            name_, key_count(), total_records_.load());
    }

    // ----------------------------------------------------------------------
    // Record ingestion
    // ----------------------------------------------------------------------

    /// Process a single record.  Routes it to the correct per-key state.
    void process(GroupedRecord record) {
        std::string group_key = extract_key(record);
        if (group_key.empty()) {
            group_key = std::string(kNullKey);
        }
        record.group_key = group_key;

        // Get or create per-key state
        auto state = get_or_create_state(group_key);
        if (!state) {
            // Too many keys — must evict first
            get_grouped_logger()->warn(
                "GroupedProcessor '{}': key limit reached ({}), skipping record",
                name_, config_.max_keys);
            dropped_records_.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        bool added = state->add_record(std::move(record),
                                       config_.max_buffer_per_key);
        if (added) {
            total_records_.fetch_add(1, std::memory_order_relaxed);
        } else {
            // Buffer full for this key — flush it
            auto drained = state->drain();
            if (window_callback_ && !drained.empty()) {
                window_callback_(group_key, std::move(drained));
            }
            // Retry with the fresh record
            state->add_record(std::move(record),
                              config_.max_buffer_per_key);
            total_records_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    /// Process a batch of records.
    void process_batch(std::vector<GroupedRecord> batch) {
        for (auto& rec : batch) {
            process(std::move(rec));
        }
        batches_processed_.fetch_add(1, std::memory_order_relaxed);
    }

    /// Called periodically to close windows (driven by punctuator).
    void punctuate(int64_t watermark_ms) {
        last_watermark_.store(watermark_ms, std::memory_order_release);

        std::vector<std::string> keys_to_windowing;

        {
            std::shared_lock lock(keys_mtx_);
            keys_to_windowing.reserve(key_states_.size());
            for (auto& [k, st] : key_states_) {
                keys_to_windowing.push_back(k);
            }
        }

        for (auto& key : keys_to_windowing) {
            auto state = get_state(key);
            if (!state) continue;

            // Close windows for this key where watermark >= window_end
            if (state->watermark_ms + config_.grace_period_ms < watermark_ms) {
                auto windows = state->split_by_windows(
                    config_.window_size_ms);
                for (auto& window : windows) {
                    if (window_callback_ && !window.empty()) {
                        window_callback_(key, std::move(window));
                    }
                }
            }
        }
    }

    // ----------------------------------------------------------------------
    // Key eviction
    // ----------------------------------------------------------------------

    /// Evict a specific key, optionally flushing its buffer.
    void evict_key(const std::string& key) {
        std::unique_lock lock(keys_mtx_);
        auto it = key_states_.find(key);
        if (it == key_states_.end()) return;

        auto state = it->second;
        state->eviction_pending = true;

        if (config_.flush_on_eviction) {
            auto drained = state->drain();
            key_states_.erase(it);
            lock.unlock();

            if (eviction_callback_ && !drained.empty()) {
                eviction_callback_(key, std::move(drained));
            }
        } else {
            key_states_.erase(it);
        }
    }

    /// Evict all stale keys.
    [[nodiscard]] size_t evict_stale() {
        std::vector<std::string> stale;
        int64_t ttl = config_.key_ttl_ms;

        {
            std::shared_lock lock(keys_mtx_);
            for (auto& [k, st] : key_states_) {
                if (st->is_stale(ttl)) {
                    stale.push_back(k);
                }
            }
        }

        for (auto& key : stale) {
            evict_key(key);
            evicted_keys_.fetch_add(1, std::memory_order_relaxed);
        }

        if (!stale.empty()) {
            get_grouped_logger()->info(
                "GroupedProcessor '{}': evicted {} stale keys",
                name_, stale.size());
        }

        return stale.size();
    }

    /// Force eviction of the least recently used keys to stay under max_keys.
    void evict_lru_if_needed() {
        std::unique_lock lock(keys_mtx_);
        int64_t current = static_cast<int64_t>(key_states_.size());

        if (current <= config_.max_keys) return;

        // Collect keys sorted by last access
        std::vector<std::pair<std::string, int64_t>> lru;
        lru.reserve(key_states_.size());
        for (auto& [k, st] : key_states_) {
            lru.emplace_back(k, st->last_access_ms);
        }

        std::sort(lru.begin(), lru.end(),
            [](auto& a, auto& b) { return a.second < b.second; });

        int64_t to_evict = current - config_.max_keys;
        for (int64_t i = 0; i < to_evict && i < static_cast<int64_t>(lru.size()); ++i) {
            auto it = key_states_.find(lru[i].first);
            if (it != key_states_.end()) {
                if (config_.flush_on_eviction) {
                    auto drained = it->second->drain();
                    key_states_.erase(it);
                    if (eviction_callback_ && !drained.empty()) {
                        eviction_callback_(lru[i].first, std::move(drained));
                    }
                } else {
                    key_states_.erase(it);
                }
                evicted_keys_.fetch_add(1, std::memory_order_relaxed);
            }
        }

        key_space_changed_.store(true, std::memory_order_release);
    }

    // ----------------------------------------------------------------------
    // Repartition
    // ----------------------------------------------------------------------

    /// Check if repartition is needed based on key space changes.
    [[nodiscard]] bool needs_repartition() const {
        return config_.repartition_on_change &&
               key_space_changed_.load(std::memory_order_acquire);
    }

    /// Mark that repartition has been handled.
    void repartition_handled() {
        key_space_changed_.store(false, std::memory_order_release);
    }

    // ----------------------------------------------------------------------
    // Query
    // ----------------------------------------------------------------------

    [[nodiscard]] size_t key_count() const {
        std::shared_lock lock(keys_mtx_);
        return key_states_.size();
    }

    [[nodiscard]] std::vector<std::string> active_keys() const {
        std::shared_lock lock(keys_mtx_);
        std::vector<std::string> keys;
        keys.reserve(key_states_.size());
        for (auto& [k, _] : key_states_) keys.push_back(k);
        return keys;
    }

    [[nodiscard]] bool is_running() const noexcept { return running_.load(); }
    [[nodiscard]] const std::string& name() const noexcept { return name_; }
    [[nodiscard]] int64_t total_records() const noexcept {
        return total_records_.load();
    }
    [[nodiscard]] int64_t evicted_count() const noexcept {
        return evicted_keys_.load();
    }
    [[nodiscard]] int64_t dropped_count() const noexcept {
        return dropped_records_.load();
    }

    /// Get statistics for a specific key.
    [[nodiscard]] std::optional<json> key_status(const std::string& key) const {
        auto state = get_state(key);
        if (!state) return std::nullopt;
        return state->status();
    }

    [[nodiscard]] json status() const {
        json j;
        j["processor"] = name_;
        j["config"]    = config_.to_json();
        j["running"]   = running_.load();
        {
            std::shared_lock lock(keys_mtx_);
            j["key_count"] = key_states_.size();
        }
        j["total_records"]    = total_records_.load();
        j["batches_processed"] = batches_processed_.load();
        j["evicted_keys"]     = evicted_keys_.load();
        j["dropped_records"]  = dropped_records_.load();
        j["last_watermark"]   = last_watermark_.load();

        // Sample of keys
        json sample = json::array();
        {
            std::shared_lock lock(keys_mtx_);
            int count = 0;
            for (auto& [k, st] : key_states_) {
                if (count++ >= 20) break;
                sample.push_back(st->status());
            }
        }
        j["key_sample"] = sample;

        return j;
    }

    // ----------------------------------------------------------------------
    // Custom extractor (for KeySource::custom)
    // ----------------------------------------------------------------------

    void set_custom_extractor(CustomExtractor extractor) {
        std::unique_lock lock(extractor_mtx_);
        custom_extractor_ = std::move(extractor);
    }

private:
    // ----------------------------------------------------------------------
    // Internal helpers
    // ----------------------------------------------------------------------

    [[nodiscard]] std::string extract_key(const GroupedRecord& rec) const {
        // Try custom extractor first
        {
            std::shared_lock lock(extractor_mtx_);
            if (custom_extractor_ &&
                config_.key_source == KeySource::custom) {
                return custom_extractor_(rec);
            }
        }
        return key_extractor_.extract(rec);
    }

    [[nodiscard]] std::shared_ptr<PerKeyState> get_state(
        const std::string& key)
    {
        std::shared_lock lock(keys_mtx_);
        auto it = key_states_.find(key);
        if (it != key_states_.end()) return it->second;
        return nullptr;
    }

    [[nodiscard]] std::shared_ptr<PerKeyState> get_state(
        const std::string& key) const
    {
        std::shared_lock lock(keys_mtx_);
        auto it = key_states_.find(key);
        if (it != key_states_.end()) return it->second;
        return nullptr;
    }

    [[nodiscard]] std::shared_ptr<PerKeyState> get_or_create_state(
        const std::string& key)
    {
        {
            std::shared_lock lock(keys_mtx_);
            auto it = key_states_.find(key);
            if (it != key_states_.end()) return it->second;
        }

        // Check key limit before creating
        {
            std::shared_lock lock(keys_mtx_);
            if (static_cast<int64_t>(key_states_.size()) >= config_.max_keys) {
                return nullptr;
            }
        }

        auto state = std::make_shared<PerKeyState>(key);

        {
            std::unique_lock lock(keys_mtx_);
            // Double-check after acquiring write lock
            if (static_cast<int64_t>(key_states_.size()) >= config_.max_keys) {
                return nullptr;
            }
            key_states_.emplace(key, state);
        }

        key_space_changed_.store(true, std::memory_order_release);
        return state;
    }

    void flush_all() {
        std::vector<std::pair<std::string, std::shared_ptr<PerKeyState>>> all;

        {
            std::unique_lock lock(keys_mtx_);
            all.reserve(key_states_.size());
            for (auto& [k, st] : key_states_) {
                all.emplace_back(k, st);
            }
            key_states_.clear();
        }

        for (auto& [key, state] : all) {
            auto drained = state->drain();
            if (!drained.empty()) {
                if (eviction_callback_) {
                    eviction_callback_(key, std::move(drained));
                } else if (window_callback_) {
                    window_callback_(key, std::move(drained));
                }
            }
        }
    }

    void eviction_loop() {
        auto logger = get_grouped_logger();

        while (running_.load(std::memory_order_acquire)) {
            {
                std::unique_lock lock(eviction_mtx_);
                eviction_cv_.wait_for(lock,
                    std::chrono::milliseconds(config_.eviction_check_ms),
                    [this] { return !running_.load(std::memory_order_acquire); });
            }

            if (!running_.load(std::memory_order_acquire)) break;

            // Check for stale keys
            evict_stale();

            // Enforce key limit
            evict_lru_if_needed();
        }
    }

    // ----------------------------------------------------------------------
    // Members
    // ----------------------------------------------------------------------

    std::string      name_;
    GroupedConfig    config_;
    KeyExtractor     key_extractor_;

    // Per-key state
    mutable std::shared_mutex keys_mtx_;
    std::unordered_map<std::string, std::shared_ptr<PerKeyState>> key_states_;

    // Callbacks
    WindowCallback    window_callback_;
    EvictionCallback  eviction_callback_;
    mutable std::shared_mutex extractor_mtx_;
    CustomExtractor   custom_extractor_;

    // Runtime
    std::atomic<bool>  running_{false};
    std::atomic<int64_t> total_records_{0};
    std::atomic<int64_t> batches_processed_{0};
    std::atomic<int64_t> evicted_keys_{0};
    std::atomic<int64_t> dropped_records_{0};
    std::atomic<int64_t> last_watermark_{0};
    std::atomic<bool>  key_space_changed_{false};

    // Eviction thread
    std::thread             eviction_thread_;
    std::mutex              eviction_mtx_;
    std::condition_variable eviction_cv_;
};

} // namespace torrent::streams
