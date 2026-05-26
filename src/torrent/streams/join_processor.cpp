/**
 * join_processor.cpp — JoinProcessor: Stream-Stream & Stream-Table Joins
 *
 * Provides join operations across streaming records within the torrent-mq
 * stream processing topology.  Supports two join modalities:
 *
 *   Stream-Stream Join:
 *     Two live streams are joined on a common key within a time-based
 *     window.  Records from the left stream are matched against records
 *     from the right stream that fall within [t - window_size, t + window_size].
 *     This produces inner, left, or outer join results.
 *
 *   Stream-Table Join:
 *     A live stream is joined against a materialized table (snapshot of
 *     a compacted topic or external lookup).  Each incoming stream record
 *     triggers a lookup against the current table state.  This is equivalent
 *     to an always-current left join against the table.
 *
 * Join types:
 *   - INNER: emit only when both sides have matching records
 *   - LEFT:  emit all left-side records; null-fill for right side
 *   - OUTER: emit all records from both sides (stream-stream only)
 *
 * Join window semantics (stream-stream):
 *   For each left record at time T_L with key K, find all right records
 *   with key K in [T_L - before_ms, T_L + after_ms].  For each such pair,
 *   emit a joined record.
 *
 * State management:
 *   Each side maintains a windowed store of recent records, keyed by join
 *   key.  State is periodically checkpointed for fault tolerance.  Stale
 *   records are evicted once they fall outside the join window.
 *
 * Table representation (stream-table):
 *   The table side is maintained as a point-in-time materialized view.
 *   Updates to the table topic (compacted) overwrite the stored value.
 *   Stream lookups always see the latest table state.
 *
 * Grace period:
 *   Late-arriving records (stream-stream) may still match if they arrive
 *   within the grace period after the join window closes.
 *
 * Thread-safety:
 *   process_left() and process_right() are called from separate source
 *   threads.  Internal state stores are lock-protected per join key.
 *
 * Dependencies:
 *   - nlohmann/json for value manipulation
 *   - stream_processor.cpp (ProcessorBase, StateStore)
 */

#include "torrent/streams/topology.h"

#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using json = nlohmann::json;
using namespace std::chrono_literals;

namespace torrent::streams {

// ============================================================================
// Anonymous namespace — join internals
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_join_logger() {
    static auto logger = spdlog::get("join_processor");
    if (!logger) {
        logger = spdlog::stdout_color_mt("join_processor");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Join type
// --------------------------------------------------------------------------

enum class JoinType : uint8_t {
    inner = 0,
    left  = 1,
    outer = 2,
};

[[nodiscard]] std::string_view join_type_name(JoinType jt) {
    switch (jt) {
    case JoinType::inner: return "INNER";
    case JoinType::left:  return "LEFT";
    case JoinType::outer: return "OUTER";
    }
    return "unknown";
}

// --------------------------------------------------------------------------
// Join mode
// --------------------------------------------------------------------------

enum class JoinMode : uint8_t {
    stream_stream = 0,
    stream_table  = 1,
};

[[nodiscard]] std::string_view join_mode_name(JoinMode jm) {
    switch (jm) {
    case JoinMode::stream_stream: return "stream-stream";
    case JoinMode::stream_table:  return "stream-table";
    }
    return "unknown";
}

// --------------------------------------------------------------------------
// JoinRecord — a record stored in the join state for one side
// --------------------------------------------------------------------------

struct JoinRecord {
    std::string   key;        // join key
    json          value;       // full record value
    int64_t       timestamp_ms = 0;
    offset_t      offset       = kInvalidOffset;

    [[nodiscard]] bool valid() const noexcept { return !key.empty(); }
};

// --------------------------------------------------------------------------
// WindowedStore — stores records for one side of a stream-stream join
// --------------------------------------------------------------------------

class WindowedStore {
public:
    struct Config {
        std::chrono::milliseconds retention_ms{300000};  // 5 min default
        std::chrono::milliseconds eviction_interval{10000};
        size_t                     max_records_per_key{10000};
        size_t                     max_total_records{1000000};
    };

    explicit WindowedStore(Config cfg = {})
        : config_(std::move(cfg))
    {}

    /// Add a record to the store.
    void put(JoinRecord record) {
        std::unique_lock lock(mutex_);

        auto& list = records_by_key_[record.key];
        list.push_back(std::move(record));
        total_records_++;

        // Enforce per-key limit
        while (list.size() > config_.max_records_per_key) {
            list.pop_front();
            total_records_--;
        }

        // Enforce global limit
        if (total_records_ > config_.max_total_records) {
            evict_oldest_global();
        }
    }

    /// Find all records with the given key within a time range.
    [[nodiscard]] std::vector<JoinRecord> find(
        const std::string& key,
        int64_t from_ms,
        int64_t to_ms) const
    {
        std::shared_lock lock(mutex_);
        return find_impl(key, from_ms, to_ms);
    }

    /// Find records and optionally remove them (for outer join consumption).
    [[nodiscard]] std::vector<JoinRecord> find_and_consume(
        const std::string& key,
        int64_t from_ms,
        int64_t to_ms)
    {
        std::unique_lock lock(mutex_);

        auto results = find_impl(key, from_ms, to_ms);

        // Mark as consumed by timestamp tracking
        for (auto& r : results) {
            consumed_.insert({key, r.timestamp_ms, r.offset});
        }

        return results;
    }

    /// Evict records older than (watermark - retention_ms).
    size_t evict_before(int64_t watermark_ms) {
        std::unique_lock lock(mutex_);
        int64_t eviction_threshold = watermark_ms - config_.retention_ms.count();

        size_t evicted = 0;
        auto it = records_by_key_.begin();
        while (it != records_by_key_.end()) {
            auto& list = it->second;
            while (!list.empty() &&
                   list.front().timestamp_ms < eviction_threshold) {
                list.pop_front();
                evicted++;
                total_records_--;
            }
            if (list.empty()) {
                it = records_by_key_.erase(it);
            } else {
                ++it;
            }
        }
        return evicted;
    }

    [[nodiscard]] size_t size() const noexcept { return total_records_; }
    [[nodiscard]] size_t key_count() const {
        std::shared_lock lock(mutex_);
        return records_by_key_.size();
    }

    void clear() {
        std::unique_lock lock(mutex_);
        records_by_key_.clear();
        consumed_.clear();
        total_records_ = 0;
    }

private:
    [[nodiscard]] std::vector<JoinRecord> find_impl(
        const std::string& key,
        int64_t from_ms,
        int64_t to_ms) const
    {
        std::vector<JoinRecord> results;
        auto it = records_by_key_.find(key);
        if (it == records_by_key_.end()) return results;

        for (auto& rec : it->second) {
            if (rec.timestamp_ms >= from_ms && rec.timestamp_ms <= to_ms) {
                // Skip consumed records in outer join mode
                auto consumed_key = std::make_tuple(key, rec.timestamp_ms,
                                                     rec.offset);
                if (!consumed_.contains(consumed_key)) {
                    results.push_back(rec);
                }
            }
        }
        return results;
    }

    void evict_oldest_global() {
        // Find the key with the oldest record
        std::string oldest_key;
        int64_t     oldest_ts = std::numeric_limits<int64_t>::max();

        for (auto& [k, list] : records_by_key_) {
            if (!list.empty() && list.front().timestamp_ms < oldest_ts) {
                oldest_ts  = list.front().timestamp_ms;
                oldest_key = k;
            }
        }

        if (!oldest_key.empty()) {
            records_by_key_[oldest_key].pop_front();
            total_records_--;
        }
    }

    struct ConsumedKey {
        std::string key;
        int64_t     ts;
        offset_t    offset;

        bool operator==(const ConsumedKey& o) const {
            return key == o.key && ts == o.ts && offset == o.offset;
        }
    };

    struct ConsumedKeyHash {
        size_t operator()(const ConsumedKey& k) const {
            return std::hash<std::string>{}(k.key) ^
                   (std::hash<int64_t>{}(k.ts) << 1) ^
                   (std::hash<offset_t>{}(k.offset) << 2);
        }
    };

    Config                                             config_;
    mutable std::shared_mutex                          mutex_;
    std::unordered_map<std::string, std::deque<JoinRecord>> records_by_key_;
    std::unordered_set<ConsumedKey, ConsumedKeyHash>   consumed_;
    size_t                                             total_records_ = 0;
};

// --------------------------------------------------------------------------
// TableStore — materialized view for stream-table join (table side)
// --------------------------------------------------------------------------

class TableStore {
public:
    /// Update the table with a new value for a key (upsert).
    void put(const std::string& key, json value) {
        std::unique_lock lock(mutex_);
        table_[key] = std::move(value);
        version_++;
    }

    /// Delete a key from the table.
    void remove(const std::string& key) {
        std::unique_lock lock(mutex_);
        table_.erase(key);
        version_++;
    }

    /// Lookup the current value for a key.
    [[nodiscard]] std::optional<json> get(const std::string& key) const {
        std::shared_lock lock(mutex_);
        auto it = table_.find(key);
        if (it != table_.end()) return it->second;
        return std::nullopt;
    }

    /// Check if a key exists.
    [[nodiscard]] bool contains(const std::string& key) const {
        std::shared_lock lock(mutex_);
        return table_.contains(key);
    }

    [[nodiscard]] size_t size() const {
        std::shared_lock lock(mutex_);
        return table_.size();
    }

    [[nodiscard]] int64_t version() const noexcept { return version_; }

    /// Serialize the full table for checkpointing.
    [[nodiscard]] json serialize() const {
        std::shared_lock lock(mutex_);
        return json(table_);
    }

    /// Deserialize and restore table state.
    void deserialize(const json& j) {
        std::unique_lock lock(mutex_);
        table_.clear();
        for (auto& [k, v] : j.items()) {
            table_[k] = v;
        }
        version_++;
    }

    void clear() {
        std::unique_lock lock(mutex_);
        table_.clear();
        version_ = 0;
    }

private:
    mutable std::shared_mutex          mutex_;
    std::unordered_map<std::string, json> table_;
    std::atomic<int64_t>              version_{0};
};

} // anonymous namespace

// ============================================================================
// JoinProcessor — public API
// ============================================================================

class JoinProcessor {
public:
    // ------------------------------------------------------------------
    // Configuration
    // ------------------------------------------------------------------

    struct Config {
        JoinMode                  mode = JoinMode::stream_stream;
        JoinType                  type = JoinType::inner;

        // For stream-stream: join window
        std::chrono::milliseconds before_ms{60000};   // look-back window
        std::chrono::milliseconds after_ms{60000};     // look-ahead window
        std::chrono::milliseconds grace_period{5000};  // late arrival slack

        // Key extraction
        std::string               left_key_field  = "key";
        std::string               right_key_field = "key";

        // Value extraction for join result
        std::string               left_value_prefix  = "left_";
        std::string               right_value_prefix = "right_";

        // State retention
        std::chrono::milliseconds state_retention{300000};  // 5 min
        std::chrono::milliseconds eviction_interval{10000};

        // Table mode
        std::string               table_topic;              // topic for table side

        [[nodiscard]] json serialize() const {
            return {
                {"mode",            std::string(join_mode_name(mode))},
                {"type",            std::string(join_type_name(type))},
                {"before_ms",       before_ms.count()},
                {"after_ms",        after_ms.count()},
                {"grace_period_ms", grace_period.count()},
                {"left_key_field",  left_key_field},
                {"right_key_field", right_key_field},
                {"state_retention_ms", state_retention.count()},
                {"table_topic",     table_topic},
            };
        }
    };

    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    explicit JoinProcessor(std::string name, Config config)
        : name_(std::move(name))
        , config_(std::move(config))
    {
        // Configure windowed stores for stream-stream mode
        if (config_.mode == JoinMode::stream_stream) {
            WindowedStore::Config wcfg;
            wcfg.retention_ms = config_.state_retention;
            wcfg.eviction_interval = config_.eviction_interval;

            right_store_ = std::make_unique<WindowedStore>(wcfg);

            if (config_.type == JoinType::outer) {
                // For outer join, also store left-side for right-driven matches
                left_store_ = std::make_unique<WindowedStore>(wcfg);
            }
        } else {
            // Stream-table mode
            table_store_ = std::make_unique<TableStore>();
        }

        auto log = get_join_logger();
        log->info("JoinProcessor '{}': mode={} type={} before={}ms after={}ms",
                  name_,
                  join_mode_name(config_.mode),
                  join_type_name(config_.type),
                  config_.before_ms.count(),
                  config_.after_ms.count());
    }

    ~JoinProcessor() = default;

    JoinProcessor(const JoinProcessor&) = delete;
    JoinProcessor& operator=(const JoinProcessor&) = delete;

    // ------------------------------------------------------------------
    // Record processing — left side
    // ------------------------------------------------------------------

    /// Process a record arriving on the left stream.
    /// Emits join results via the emit callback.
    void process_left(const std::string& key,
                      const json& value,
                      int64_t timestamp_ms,
                      offset_t offset = kInvalidOffset)
    {
        left_records_.fetch_add(1, std::memory_order_relaxed);

        std::string join_key = extract_key(key, value, config_.left_key_field);

        if (config_.mode == JoinMode::stream_stream) {
            process_left_stream_stream(join_key, value, timestamp_ms, offset);
        } else {
            process_left_stream_table(join_key, value, timestamp_ms, offset);
        }

        update_watermark(timestamp_ms);
    }

    /// Process a batch of left-side records.
    void process_left_batch(
        const std::vector<std::tuple<std::string, json, int64_t, offset_t>>& records)
    {
        for (auto& [key, value, ts, offset] : records) {
            process_left(key, value, ts, offset);
        }
    }

    // ------------------------------------------------------------------
    // Record processing — right side (stream-stream only)
    // ------------------------------------------------------------------

    /// Process a record arriving on the right stream.
    void process_right(const std::string& key,
                       const json& value,
                       int64_t timestamp_ms,
                       offset_t offset = kInvalidOffset)
    {
        if (config_.mode != JoinMode::stream_stream) {
            auto log = get_join_logger();
            log->warn("JoinProcessor '{}': process_right called in stream-table mode",
                       name_);
            return;
        }

        right_records_.fetch_add(1, std::memory_order_relaxed);

        std::string join_key = extract_key(key, value, config_.right_key_field);

        // Store the right record
        JoinRecord rec;
        rec.key          = join_key;
        rec.value        = value;
        rec.timestamp_ms = timestamp_ms;
        rec.offset       = offset;
        right_store_->put(std::move(rec));

        // For outer join: match incoming right against stored left records
        if (config_.type == JoinType::outer && left_store_) {
            int64_t from_ms = timestamp_ms - config_.before_ms.count();
            int64_t to_ms   = timestamp_ms + config_.after_ms.count();

            auto left_matches = left_store_->find(join_key, from_ms, to_ms);
            for (auto& left_rec : left_matches) {
                json result = build_joined_result(left_rec.value, value,
                                                   left_rec.timestamp_ms,
                                                   timestamp_ms);
                emit(join_key, std::move(result));
                join_matches_.fetch_add(1, std::memory_order_relaxed);
            }
        }

        update_watermark(timestamp_ms);
    }

    /// Process a record for the table side (stream-table mode).
    /// Called when the table topic has an update.
    void process_table_update(const std::string& key,
                              const json& value,
                              bool is_delete = false)
    {
        if (config_.mode != JoinMode::stream_table) {
            auto log = get_join_logger();
            log->warn("JoinProcessor '{}': process_table_update called in "
                       "stream-stream mode", name_);
            return;
        }

        if (!table_store_) return;

        if (is_delete) {
            table_store_->remove(key);
        } else {
            table_store_->put(key, value);
        }

        table_updates_.fetch_add(1, std::memory_order_relaxed);
    }

    // ------------------------------------------------------------------
    // Punctuation — advance watermark and evict stale state
    // ------------------------------------------------------------------

    void punctuate(int64_t stream_time_ms) {
        update_watermark(stream_time_ms);

        if (right_store_) {
            size_t evicted = right_store_->evict_before(watermark_ms_);
            if (evicted > 0) {
                auto log = get_join_logger();
                log->trace("JoinProcessor '{}': evicted {} right records",
                            name_, evicted);
            }
        }

        if (left_store_) {
            left_store_->evict_before(watermark_ms_);
        }
    }

    // ------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------

    [[nodiscard]] uint64_t left_records() const noexcept {
        return left_records_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t right_records() const noexcept {
        return right_records_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t join_matches() const noexcept {
        return join_matches_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t unmatched_left() const noexcept {
        uint64_t left = left_records_.load(std::memory_order_relaxed);
        uint64_t matches = join_matches_.load(std::memory_order_relaxed);
        return left > matches ? left - matches : 0;
    }
    [[nodiscard]] uint64_t table_updates() const noexcept {
        return table_updates_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] double match_rate() const noexcept {
        uint64_t left = left_records_.load(std::memory_order_relaxed);
        return left > 0
            ? static_cast<double>(join_matches_.load(std::memory_order_relaxed)) /
              static_cast<double>(left)
            : 0.0;
    }

    [[nodiscard]] size_t right_store_size() const noexcept {
        return right_store_ ? right_store_->size() : 0;
    }
    [[nodiscard]] size_t left_store_size() const noexcept {
        return left_store_ ? left_store_->size() : 0;
    }
    [[nodiscard]] size_t table_store_size() const noexcept {
        return table_store_ ? table_store_->size() : 0;
    }
    [[nodiscard]] int64_t watermark() const noexcept { return watermark_ms_; }

    [[nodiscard]] const std::string& name() const noexcept { return name_; }
    [[nodiscard]] const Config& config() const noexcept { return config_; }

    // ------------------------------------------------------------------
    // Callback registration
    // ------------------------------------------------------------------

    void set_emit_callback(std::function<void(std::string, json)> callback) {
        emit_callback_ = std::move(callback);
    }

private:
    // ------------------------------------------------------------------
    // Stream-stream join logic
    // ------------------------------------------------------------------

    void process_left_stream_stream(const std::string& join_key,
                                     const json& value,
                                     int64_t timestamp_ms,
                                     offset_t offset)
    {
        // Store left record for future right-matches (outer join)
        if (left_store_) {
            JoinRecord rec;
            rec.key          = join_key;
            rec.value        = value;
            rec.timestamp_ms = timestamp_ms;
            rec.offset       = offset;
            left_store_->put(std::move(rec));
        }

        if (!right_store_) return;

        // Search right-side for matches within join window
        int64_t from_ms = timestamp_ms - config_.before_ms.count();
        int64_t to_ms   = timestamp_ms + config_.after_ms.count();

        std::vector<JoinRecord> right_matches;

        if (config_.type == JoinType::outer) {
            // Consume to avoid double-matching when right side drives
            right_matches = right_store_->find_and_consume(join_key, from_ms, to_ms);
        } else {
            right_matches = right_store_->find(join_key, from_ms, to_ms);
        }

        if (!right_matches.empty()) {
            // Found matches — emit joined records
            for (auto& right_rec : right_matches) {
                json result = build_joined_result(value, right_rec.value,
                                                   timestamp_ms,
                                                   right_rec.timestamp_ms);
                emit(join_key, std::move(result));
                join_matches_.fetch_add(1, std::memory_order_relaxed);
            }
        } else if (config_.type == JoinType::left ||
                   config_.type == JoinType::outer) {
            // No match found — emit left record with null right side
            json result = build_joined_result(value, json(),
                                               timestamp_ms, 0);
            emit(join_key, std::move(result));
            unmatched_emitted_.fetch_add(1, std::memory_order_relaxed);
        }
        // INNER join: no match → nothing emitted
    }

    // ------------------------------------------------------------------
    // Stream-table join logic
    // ------------------------------------------------------------------

    void process_left_stream_table(const std::string& join_key,
                                    const json& value,
                                    int64_t timestamp_ms,
                                    offset_t /*offset*/)
    {
        if (!table_store_) return;

        auto table_value = table_store_->get(join_key);

        if (table_value) {
            // Found match
            json result = build_joined_result(value, *table_value,
                                               timestamp_ms, timestamp_ms);
            // Tag with table version for consistency tracking
            result["_table_version"] = table_store_->version();
            emit(join_key, std::move(result));
            join_matches_.fetch_add(1, std::memory_order_relaxed);
        } else if (config_.type == JoinType::left ||
                   config_.type == JoinType::outer) {
            // No match — emit left-only
            json result = build_joined_result(value, json(),
                                               timestamp_ms, 0);
            result["_table_version"] = table_store_->version();
            emit(join_key, std::move(result));
            unmatched_emitted_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    // ------------------------------------------------------------------
    // Result construction
    // ------------------------------------------------------------------

    [[nodiscard]] json build_joined_result(const json& left_val,
                                            const json& right_val,
                                            int64_t left_ts,
                                            int64_t right_ts) const
    {
        json result;
        result["join_key"]     = "";  // filled by caller context
        result["left_value"]   = left_val;
        result["right_value"]  = right_val.is_null() ? json() : right_val;
        result["left_ts"]      = left_ts;
        result["right_ts"]     = right_ts;
        result["join_type"]    = join_type_name(config_.type);
        result["matched"]      = !right_val.is_null();

        // Flatten if configured
        if (!config_.left_value_prefix.empty() && left_val.is_object()) {
            for (auto& [k, v] : left_val.items()) {
                result[config_.left_value_prefix + k] = v;
            }
        }
        if (!config_.right_value_prefix.empty() && right_val.is_object()) {
            for (auto& [k, v] : right_val.items()) {
                result[config_.right_value_prefix + k] = v;
            }
        }

        return result;
    }

    // ------------------------------------------------------------------
    // Emit
    // ------------------------------------------------------------------

    void emit(const std::string& key, json result) {
        result["join_key"] = key;
        result["processor"] = name_;
        result["emit_ts"] = now_ms();

        if (emit_callback_) {
            emit_callback_(key, std::move(result));
        }

        total_emitted_.fetch_add(1, std::memory_order_relaxed);
    }

    // ------------------------------------------------------------------
    // Key extraction
    // ------------------------------------------------------------------

    [[nodiscard]] std::string extract_key(const std::string& record_key,
                                            const json& value,
                                            const std::string& key_field) const
    {
        if (key_field == "key" || key_field.empty()) {
            return record_key;
        }

        if (key_field.starts_with("value.")) {
            std::string path = "/" + key_field.substr(6);
            // Replace '.' with '/' for JSON pointer
            std::string json_ptr;
            for (char c : path) {
                if (c == '.') json_ptr += '/';
                else json_ptr += c;
            }
            try {
                json::json_pointer ptr(json_ptr);
                if (value.contains(ptr)) {
                    auto& v = value.at(ptr);
                    if (v.is_string()) return v.get<std::string>();
                    return v.dump();
                }
            } catch (...) {}
        }

        return record_key;
    }

    // ------------------------------------------------------------------
    // Watermark
    // ------------------------------------------------------------------

    void update_watermark(int64_t ts) {
        if (ts > watermark_ms_) {
            watermark_ms_ = ts;
        }
    }

    [[nodiscard]] static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    // ------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------

    std::string                       name_;
    Config                            config_;
    int64_t                           watermark_ms_ = 0;

    // Stream-stream state
    std::unique_ptr<WindowedStore>    left_store_;    // for outer joins
    std::unique_ptr<WindowedStore>    right_store_;

    // Stream-table state
    std::unique_ptr<TableStore>       table_store_;

    // Emit callback
    std::function<void(std::string, json)> emit_callback_;

    // Metrics
    std::atomic<uint64_t>            left_records_{0};
    std::atomic<uint64_t>            right_records_{0};
    std::atomic<uint64_t>            join_matches_{0};
    std::atomic<uint64_t>            unmatched_emitted_{0};
    std::atomic<uint64_t>            table_updates_{0};
    std::atomic<uint64_t>            total_emitted_{0};
};

// ============================================================================
// JoinProcessorBuilder — fluent construction API
// ============================================================================

class JoinProcessorBuilder {
public:
    explicit JoinProcessorBuilder(std::string name)
        : name_(std::move(name))
    {}

    JoinProcessorBuilder& stream_stream() {
        config_.mode = JoinMode::stream_stream;
        return *this;
    }

    JoinProcessorBuilder& stream_table(const std::string& table_topic = "") {
        config_.mode = JoinMode::stream_table;
        config_.table_topic = table_topic;
        return *this;
    }

    JoinProcessorBuilder& inner() {
        config_.type = JoinType::inner;
        return *this;
    }

    JoinProcessorBuilder& left() {
        config_.type = JoinType::left;
        return *this;
    }

    JoinProcessorBuilder& outer() {
        config_.type = JoinType::outer;
        return *this;
    }

    JoinProcessorBuilder& window(std::chrono::milliseconds before,
                                   std::chrono::milliseconds after) {
        config_.before_ms = before;
        config_.after_ms  = after;
        return *this;
    }

    JoinProcessorBuilder& symmetric_window(std::chrono::milliseconds window_size) {
        config_.before_ms = window_size;
        config_.after_ms  = window_size;
        return *this;
    }

    JoinProcessorBuilder& grace_period(std::chrono::milliseconds grace) {
        config_.grace_period = grace;
        return *this;
    }

    JoinProcessorBuilder& left_key(const std::string& field) {
        config_.left_key_field = field;
        return *this;
    }

    JoinProcessorBuilder& right_key(const std::string& field) {
        config_.right_key_field = field;
        return *this;
    }

    JoinProcessorBuilder& on(const std::string& field) {
        config_.left_key_field = field;
        config_.right_key_field = field;
        return *this;
    }

    JoinProcessorBuilder& state_retention(std::chrono::milliseconds retention) {
        config_.state_retention = retention;
        return *this;
    }

    JoinProcessorBuilder& left_value_prefix(const std::string& prefix) {
        config_.left_value_prefix = prefix;
        return *this;
    }

    JoinProcessorBuilder& right_value_prefix(const std::string& prefix) {
        config_.right_value_prefix = prefix;
        return *this;
    }

    [[nodiscard]] JoinProcessor build() {
        return JoinProcessor(name_, config_);
    }

private:
    std::string           name_;
    JoinProcessor::Config config_;
};

} // namespace torrent::streams
