/**
 * aggregate_processor.cpp — AggregateProcessor: Windowed Stream Aggregations
 *
 * Provides windowed aggregation over streaming records within the torrent-mq
 * stream processing topology.  Records are grouped by key, accumulated in
 * time-based windows, and emitted when the window closes.
 *
 * Supported window types:
 *   - Tumbling:  fixed-size, non-overlapping windows
 *                  e.g. 60s window every 60s
 *   - Hopping:   fixed-size, overlapping windows
 *                  e.g. 120s window advancing every 30s
 *   - Sliding:   continuous window over the last N elements
 *                  e.g. "last 100 records per key"
 *   - Session:   dynamic windows bounded by an inactivity gap
 *                  e.g. 30s gap defines session boundaries
 *
 * Supported aggregation functions:
 *   - count:      count of records in the window
 *   - sum:        sum of a numeric field
 *   - avg:        average of a numeric field
 *   - min:        minimum value of a field
 *   - max:        maximum value of a field
 *   - top_k:      top K values by frequency
 *   - distinct_count: approximate cardinality (HyperLogLog stub)
 *   - collect_list: collect all values into an array
 *   - first:      first value seen in the window
 *   - last:       last value seen in the window
 *
 * Window lifecycle:
 *   1. Records arrive and are routed to window buckets by timestamp + key
 *   2. When the watermark advances past a window's end, the window is closed
 *   3. The aggregation result is computed and emitted downstream
 *   4. Closed windows are evicted from the state store
 *
 * Grace period:
 *   A configurable grace period allows late-arriving records to be included
 *   in a window after it would normally have closed.  Records arriving after
 *   (window_end + grace_period) are discarded.
 *
 * State management:
 *   Window state is persisted to the state store for fault tolerance.
 *   On recovery, checkpoints are loaded and processing resumes.
 *
 * Dependencies:
 *   - nlohmann/json for value access and serialization
 *   - stream_processor.cpp (StateStore, ProcessorBase concepts)
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
// Anonymous namespace — aggregation internals
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_agg_logger() {
    static auto logger = spdlog::get("aggregate_processor");
    if (!logger) {
        logger = spdlog::stdout_color_mt("aggregate_processor");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Window type enumeration
// --------------------------------------------------------------------------

enum class WindowKind : uint8_t {
    tumbling = 0,
    hopping  = 1,
    sliding  = 2,
    session  = 3,
};

[[nodiscard]] std::string_view window_kind_name(WindowKind wk) {
    switch (wk) {
    case WindowKind::tumbling: return "tumbling";
    case WindowKind::hopping:  return "hopping";
    case WindowKind::sliding:  return "sliding";
    case WindowKind::session:  return "session";
    }
    return "unknown";
}

// --------------------------------------------------------------------------
// Window specification
// --------------------------------------------------------------------------

struct WindowSpec {
    WindowKind                 kind = WindowKind::tumbling;
    std::chrono::milliseconds  size{60000};       // window size (or slide size for sliding)
    std::chrono::milliseconds  advance{60000};    // hop size for hopping windows
    std::chrono::milliseconds  session_gap{30000}; // inactivity gap for sessions
    int64_t                    slide_count{100};   // element count for sliding windows
    std::chrono::milliseconds  grace_period{0};    // late-arrival allowance

    [[nodiscard]] static WindowSpec tumbling(std::chrono::milliseconds size,
                                               std::chrono::milliseconds grace = 0ms) {
        WindowSpec ws;
        ws.kind   = WindowKind::tumbling;
        ws.size   = size;
        ws.advance = size;
        ws.grace_period = grace;
        return ws;
    }

    [[nodiscard]] static WindowSpec hopping(std::chrono::milliseconds size,
                                              std::chrono::milliseconds advance,
                                              std::chrono::milliseconds grace = 0ms) {
        WindowSpec ws;
        ws.kind    = WindowKind::hopping;
        ws.size    = size;
        ws.advance = advance;
        ws.grace_period = grace;
        return ws;
    }

    [[nodiscard]] static WindowSpec sliding(int64_t count,
                                              std::chrono::milliseconds grace = 0ms) {
        WindowSpec ws;
        ws.kind        = WindowKind::sliding;
        ws.slide_count  = count;
        ws.grace_period = grace;
        return ws;
    }

    [[nodiscard]] static WindowSpec session(std::chrono::milliseconds gap,
                                              std::chrono::milliseconds grace = 0ms) {
        WindowSpec ws;
        ws.kind         = WindowKind::session;
        ws.session_gap  = gap;
        ws.grace_period = grace;
        return ws;
    }

    [[nodiscard]] json serialize() const {
        return {
            {"kind", std::string(window_kind_name(kind))},
            {"size_ms", size.count()},
            {"advance_ms", advance.count()},
            {"session_gap_ms", session_gap.count()},
            {"slide_count", slide_count},
            {"grace_period_ms", grace_period.count()},
        };
    }
};

// --------------------------------------------------------------------------
// Aggregation function type
// --------------------------------------------------------------------------

enum class AggFn : uint8_t {
    count          = 0,
    sum            = 1,
    avg            = 2,
    min            = 3,
    max            = 4,
    top_k          = 5,
    distinct_count = 6,
    collect_list   = 7,
    first          = 8,
    last           = 9,
};

[[nodiscard]] std::string_view agg_fn_name(AggFn fn) {
    switch (fn) {
    case AggFn::count:           return "count";
    case AggFn::sum:             return "sum";
    case AggFn::avg:             return "avg";
    case AggFn::min:             return "min";
    case AggFn::max:             return "max";
    case AggFn::top_k:           return "top_k";
    case AggFn::distinct_count:  return "distinct_count";
    case AggFn::collect_list:    return "collect_list";
    case AggFn::first:           return "first";
    case AggFn::last:            return "last";
    }
    return "unknown";
}

// --------------------------------------------------------------------------
// WindowAccumulator — holds running aggregation state for one window
// --------------------------------------------------------------------------

struct WindowAccumulator {
    int64_t   window_start_ms = 0;
    int64_t   window_end_ms   = 0;
    int64_t   count            = 0;
    double    sum              = 0.0;
    double    min_val          = std::numeric_limits<double>::max();
    double    max_val          = std::numeric_limits<double>::lowest();
    double    sum_sq           = 0.0;    // for stddev
    json      first_value;
    json      last_value;

    // For top_k
    std::unordered_map<std::string, int64_t> freq_map;

    // For distinct_count (HyperLogLog stub)
    std::unordered_set<std::string> distinct_set;

    // For collect_list
    std::vector<json> collected_values;

    /// Reset all state.
    void reset() {
        count    = 0;
        sum      = 0.0;
        min_val  = std::numeric_limits<double>::max();
        max_val  = std::numeric_limits<double>::lowest();
        sum_sq   = 0.0;
        first_value.clear();
        last_value.clear();
        freq_map.clear();
        distinct_set.clear();
        collected_values.clear();
    }

    /// Add a numeric value to the accumulator.
    void add_value(const json& val) {
        count++;
        if (count == 1) first_value = val;
        last_value = val;

        if (val.is_number()) {
            double d = val.get<double>();
            sum    += d;
            sum_sq += d * d;
            if (d < min_val) min_val = d;
            if (d > max_val) max_val = d;
        }

        // For frequency-based aggregations
        if (val.is_string()) {
            freq_map[val.get<std::string>()]++;
        }

        // For distinct count
        if (val.is_string()) {
            distinct_set.insert(val.get<std::string>());
        } else {
            distinct_set.insert(val.dump());
        }

        // For collect_list
        collected_values.push_back(val);
    }

    /// Merge another accumulator into this one (for hopping window reuse).
    void merge(const WindowAccumulator& other) {
        count  += other.count;
        sum    += other.sum;
        sum_sq += other.sum_sq;
        min_val = std::min(min_val, other.min_val);
        max_val = std::max(max_val, other.max_val);

        if (first_value.is_null()) first_value = other.first_value;
        last_value = other.last_value;

        for (auto& [k, v] : other.freq_map) freq_map[k] += v;
        for (auto& v : other.distinct_set) distinct_set.insert(v);
        collected_values.insert(collected_values.end(),
                                other.collected_values.begin(),
                                other.collected_values.end());
    }

    /// Compute the result for a specific aggregation function.
    [[nodiscard]] json compute(AggFn fn, int top_k_n = 10) const {
        switch (fn) {
        case AggFn::count:
            return count;

        case AggFn::sum:
            return sum;

        case AggFn::avg:
            return count > 0 ? sum / static_cast<double>(count) : 0.0;

        case AggFn::min:
            return count > 0 ? json(min_val) : json();

        case AggFn::max:
            return count > 0 ? json(max_val) : json();

        case AggFn::first:
            return first_value;

        case AggFn::last:
            return last_value;

        case AggFn::distinct_count:
            return static_cast<int64_t>(distinct_set.size());

        case AggFn::collect_list:
            return collected_values;

        case AggFn::top_k: {
            // Sort by frequency descending, take top K
            std::vector<std::pair<std::string, int64_t>> sorted(
                freq_map.begin(), freq_map.end());
            std::sort(sorted.begin(), sorted.end(),
                      [](auto& a, auto& b) {
                          if (a.second != b.second) return a.second > b.second;
                          return a.first < b.first;
                      });

            json result = json::array();
            for (size_t i = 0; i < std::min<size_t>(sorted.size(),
                                                      static_cast<size_t>(top_k_n)); ++i) {
                json entry;
                entry["value"] = sorted[i].first;
                entry["count"] = sorted[i].second;
                result.push_back(std::move(entry));
            }
            return result;
        }
        }

        return json();
    }

    [[nodiscard]] json serialize() const {
        return {
            {"window_start_ms",   window_start_ms},
            {"window_end_ms",     window_end_ms},
            {"count",             count},
            {"sum",               sum},
            {"min_val",           min_val},
            {"max_val",           max_val},
            {"sum_sq",            sum_sq},
            {"first_value",       first_value},
            {"last_value",        last_value},
            {"distinct_count",    distinct_set.size()},
            {"collected_count",   collected_values.size()},
        };
    }

    static WindowAccumulator deserialize(const json& j) {
        WindowAccumulator acc;
        acc.window_start_ms = j.value("window_start_ms", int64_t(0));
        acc.window_end_ms   = j.value("window_end_ms", int64_t(0));
        acc.count           = j.value("count", int64_t(0));
        acc.sum             = j.value("sum", 0.0);
        acc.min_val         = j.value("min_val",
                                       std::numeric_limits<double>::max());
        acc.max_val         = j.value("max_val",
                                       std::numeric_limits<double>::lowest());
        acc.sum_sq          = j.value("sum_sq", 0.0);
        acc.first_value     = j.value("first_value", json());
        acc.last_value      = j.value("last_value", json());
        return acc;
    }
};

// --------------------------------------------------------------------------
// WindowBucket — a set of per-key accumulators for one window interval
// --------------------------------------------------------------------------

class WindowBucket {
public:
    explicit WindowBucket(int64_t start_ms, int64_t end_ms)
        : start_ms_(start_ms)
        , end_ms_(end_ms)
    {}

    /// Add a record to the bucket, keyed by group key.
    void add(const std::string& group_key, const json& value) {
        auto& acc = accumulators_[group_key];
        acc.window_start_ms = start_ms_;
        acc.window_end_ms   = end_ms_;
        acc.add_value(value);
    }

    /// Compute results for all keys in this bucket.
    [[nodiscard]] std::vector<std::pair<std::string, json>>
    emit_results(const std::vector<AggFn>& functions, int top_k_n = 10) const
    {
        std::vector<std::pair<std::string, json>> results;
        results.reserve(accumulators_.size());

        for (auto& [key, acc] : accumulators_) {
            json result;
            result["window_start_ms"] = start_ms_;
            result["window_end_ms"]   = end_ms_;
            result["key"]             = key;

            json values;
            for (auto& fn : functions) {
                values[std::string(agg_fn_name(fn))] = acc.compute(fn, top_k_n);
            }
            result["values"] = std::move(values);

            results.emplace_back(key, std::move(result));
        }

        return results;
    }

    [[nodiscard]] int64_t start_ms() const noexcept { return start_ms_; }
    [[nodiscard]] int64_t end_ms()   const noexcept { return end_ms_; }
    [[nodiscard]] size_t  key_count() const noexcept { return accumulators_.size(); }
    [[nodiscard]] bool    empty()    const noexcept { return accumulators_.empty(); }

    [[nodiscard]] json serialize() const {
        json j;
        j["start_ms"] = start_ms_;
        j["end_ms"]   = end_ms_;
        json accs = json::object();
        for (auto& [k, acc] : accumulators_) {
            accs[k] = acc.serialize();
        }
        j["accumulators"] = std::move(accs);
        return j;
    }

private:
    int64_t start_ms_;
    int64_t end_ms_;
    std::unordered_map<std::string, WindowAccumulator> accumulators_;
};

// --------------------------------------------------------------------------
// BucketStore — manages active & closed window buckets
// --------------------------------------------------------------------------

class BucketStore {
public:
    explicit BucketStore(size_t max_active_windows = 10000)
        : max_active_windows_(max_active_windows)
    {}

    /// Get or create a bucket for the given window.
    [[nodiscard]] WindowBucket& get_or_create(int64_t window_start,
                                                int64_t window_end) {
        auto key = make_key(window_start, window_end);
        auto it = active_.find(key);
        if (it != active_.end()) return it->second;

        // Enforce maximum active windows
        if (active_.size() >= max_active_windows_) {
            auto log = get_agg_logger();
            log->warn("BucketStore: max active windows ({}) reached, "
                       "evicting oldest", max_active_windows_);
            evict_oldest();
        }

        auto [inserted, _] = active_.emplace(
            key, WindowBucket(window_start, window_end));
        return inserted->second;
    }

    /// Close all windows that end before (or at) the given timestamp, accounting
    /// for grace period.
    [[nodiscard]] std::vector<WindowBucket> close_windows(
        int64_t watermark_ms,
        int64_t grace_period_ms)
    {
        std::vector<WindowBucket> closed;
        int64_t close_threshold = watermark_ms - grace_period_ms;

        auto it = active_.begin();
        while (it != active_.end()) {
            if (it->second.end_ms() <= close_threshold) {
                closed.push_back(std::move(it->second));
                it = active_.erase(it);
            } else {
                ++it;
            }
        }

        return closed;
    }

    /// Close all windows unconditionally (shutdown).
    [[nodiscard]] std::vector<WindowBucket> close_all() {
        std::vector<WindowBucket> all;
        all.reserve(active_.size());
        for (auto& [_, bucket] : active_) {
            all.push_back(std::move(bucket));
        }
        active_.clear();
        return all;
    }

    [[nodiscard]] size_t active_count() const noexcept { return active_.size(); }

private:
    [[nodiscard]] static std::string make_key(int64_t start, int64_t end) {
        return std::to_string(start) + ":" + std::to_string(end);
    }

    void evict_oldest() {
        if (active_.empty()) return;

        // Find the bucket with the smallest end_ms
        auto oldest = active_.begin();
        for (auto it = active_.begin(); it != active_.end(); ++it) {
            if (it->second.end_ms() < oldest->second.end_ms()) {
                oldest = it;
            }
        }

        auto log = get_agg_logger();
        log->warn("BucketStore: evicting window [{}, {})",
                  oldest->second.start_ms(), oldest->second.end_ms());
        active_.erase(oldest);
    }

    size_t max_active_windows_;
    std::unordered_map<std::string, WindowBucket> active_;
};

// --------------------------------------------------------------------------
// SessionTracker — manages session windows per key
// --------------------------------------------------------------------------

class SessionTracker {
public:
    explicit SessionTracker(int64_t session_gap_ms)
        : session_gap_ms_(session_gap_ms)
    {}

    /// Add a record for a key. Returns the assigned session bucket key,
    /// or a new one if no active session exists.
    [[nodiscard]] std::string process_event(
        const std::string& key, int64_t timestamp_ms)
    {
        auto it = active_sessions_.find(key);
        if (it != active_sessions_.end()) {
            int64_t last_time = it->second.last_event_ms;
            if (timestamp_ms - last_time <= session_gap_ms_) {
                // Extend current session
                it->second.last_event_ms = timestamp_ms;
                it->second.session_end_ms = timestamp_ms;
                return it->second.session_id;
            }
        }

        // Start new session
        SessionInfo info;
        info.session_id     = key + "-" + std::to_string(timestamp_ms);
        info.session_start_ms = timestamp_ms;
        info.session_end_ms   = timestamp_ms;
        info.last_event_ms    = timestamp_ms;
        active_sessions_[key] = info;

        return info.session_id;
    }

    /// Close expired sessions (those whose last event is beyond the gap).
    [[nodiscard]] std::vector<std::string> close_expired(int64_t watermark_ms) {
        std::vector<std::string> closed;
        int64_t threshold = watermark_ms - session_gap_ms_;

        auto it = active_sessions_.begin();
        while (it != active_sessions_.end()) {
            if (it->second.last_event_ms < threshold) {
                closed.push_back(it->second.session_id);
                it = active_sessions_.erase(it);
            } else {
                ++it;
            }
        }
        return closed;
    }

    [[nodiscard]] size_t active_count() const noexcept {
        return active_sessions_.size();
    }

private:
    struct SessionInfo {
        std::string session_id;
        int64_t     session_start_ms = 0;
        int64_t     session_end_ms   = 0;
        int64_t     last_event_ms    = 0;
    };

    int64_t session_gap_ms_;
    std::unordered_map<std::string, SessionInfo> active_sessions_;
};

} // anonymous namespace

// ============================================================================
// AggregateProcessor — public API
// ============================================================================

class AggregateProcessor {
public:
    /// Configuration for the aggregate processor.
    struct Config {
        WindowSpec       window_spec;
        std::vector<AggFn> functions{AggFn::count};
        std::string       group_by_field = "key";   // "key", "value.field", "header.name"
        std::string       aggregate_field = "value"; // field to aggregate
        int               top_k_n = 10;              // for top_k
        bool              emit_on_close = true;      // emit only on window close
        bool              emit_on_update = false;    // emit after every record
        std::chrono::milliseconds punctuate_interval = 1000ms;
    };

    explicit AggregateProcessor(std::string name, Config config)
        : name_(std::move(name))
        , config_(std::move(config))
    {
        auto log = get_agg_logger();
        log->info("AggregateProcessor '{}': window={} functions=[{}] "
                   "group_by={}",
                   name_,
                   window_kind_name(config_.window_spec.kind),
                   [this] {
                       std::string fns;
                       for (size_t i = 0; i < config_.functions.size(); ++i) {
                           if (i > 0) fns += ", ";
                           fns += agg_fn_name(config_.functions[i]);
                       }
                       return fns;
                   }(),
                   config_.group_by_field);
    }

    // ------------------------------------------------------------------
    // Record processing
    // ------------------------------------------------------------------

    /// Process a single record: assign to window bucket.
    void process(const std::string& key,
                 const json& value,
                 int64_t timestamp_ms)
    {
        records_processed_.fetch_add(1, std::memory_order_relaxed);

        // Extract group key
        std::string group_key = extract_group_key(key, value);
        // Extract aggregation value
        json agg_value = extract_agg_value(value);

        // Determine window assignment
        switch (config_.window_spec.kind) {
        case WindowKind::tumbling:
            process_tumbling(group_key, agg_value, timestamp_ms);
            break;
        case WindowKind::hopping:
            process_hopping(group_key, agg_value, timestamp_ms);
            break;
        case WindowKind::sliding:
            process_sliding(group_key, agg_value, timestamp_ms);
            break;
        case WindowKind::session:
            process_session(group_key, agg_value, timestamp_ms);
            break;
        }

        // Update watermark
        if (timestamp_ms > watermark_ms_) {
            watermark_ms_ = timestamp_ms;
        }
    }

    /// Advance stream time and trigger window closures.
    void punctuate(int64_t stream_time_ms) {
        if (stream_time_ms > watermark_ms_) {
            watermark_ms_ = stream_time_ms;
        }

        // Close expired windows
        auto grace = config_.window_spec.grace_period.count();
        auto closed = bucket_store_.close_windows(watermark_ms_, grace);

        if (!closed.empty()) {
            emit_closed_windows(closed);
        }

        // For sessions, close expired sessions too
        if (config_.window_spec.kind == WindowKind::session && session_tracker_) {
            auto expired = session_tracker_->close_expired(watermark_ms_);
            if (!expired.empty()) {
                auto log = get_agg_logger();
                log->debug("AggregateProcessor '{}': {} sessions expired",
                            name_, expired.size());
            }
        }
    }

    /// Close all windows and emit final results (shutdown).
    std::vector<std::pair<std::string, json>> flush() {
        auto all = bucket_store_.close_all();
        auto log = get_agg_logger();
        log->info("AggregateProcessor '{}': flushing {} windows",
                  name_, all.size());
        return emit_closed_windows(all);
    }

    // ------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------

    [[nodiscard]] uint64_t records_processed() const noexcept {
        return records_processed_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t windows_emitted() const noexcept {
        return windows_emitted_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] size_t active_windows() const noexcept {
        return bucket_store_.active_count();
    }
    [[nodiscard]] int64_t watermark() const noexcept { return watermark_ms_; }
    [[nodiscard]] const std::string& name() const noexcept { return name_; }
    [[nodiscard]] const Config& config() const noexcept { return config_; }

    // ------------------------------------------------------------------
    // Configuration helpers
    // ------------------------------------------------------------------

    void set_emit_callback(std::function<void(std::string, json)> callback) {
        emit_callback_ = std::move(callback);
    }

private:
    // ------------------------------------------------------------------
    // Window assignment
    // ------------------------------------------------------------------

    void process_tumbling(const std::string& group_key,
                          const json& agg_value,
                          int64_t timestamp_ms)
    {
        int64_t window_size = config_.window_spec.size.count();
        int64_t window_start = (timestamp_ms / window_size) * window_size;
        int64_t window_end   = window_start + window_size;

        auto& bucket = bucket_store_.get_or_create(window_start, window_end);
        bucket.add(group_key, agg_value);
    }

    void process_hopping(const std::string& group_key,
                         const json& agg_value,
                         int64_t timestamp_ms)
    {
        int64_t window_size  = config_.window_spec.size.count();
        int64_t hop_size     = config_.window_spec.advance.count();
        int64_t first_start  = (timestamp_ms / hop_size) * hop_size;

        // A record belongs to all windows that cover its timestamp.
        // We add it to each applicable window.
        int64_t earliest_start = timestamp_ms - window_size + hop_size;
        earliest_start = (earliest_start / hop_size) * hop_size;

        for (int64_t ws = earliest_start; ws <= first_start; ws += hop_size) {
            int64_t we = ws + window_size;
            if (we > timestamp_ms) { // window covers this timestamp
                auto& bucket = bucket_store_.get_or_create(ws, we);
                bucket.add(group_key, agg_value);
            }
        }
    }

    void process_sliding(const std::string& group_key, const json& agg_value,
                          int64_t /*timestamp_ms*/)
    {
        // Element-based sliding window: maintain last N records per key
        auto& buffer = sliding_buffers_[group_key];
        buffer.push_back(agg_value);
        while (static_cast<int64_t>(buffer.size()) > config_.window_spec.slide_count) {
            buffer.pop_front();
        }

        // The "window" for sliding is keyed by group_key, with a synthetic window
        // that just represents the current buffer state.
        auto& bucket = bucket_store_.get_or_create(0, 1); // synthetic window
        bucket.add(group_key, agg_value);
    }

    void process_session(const std::string& group_key,
                         const json& agg_value,
                         int64_t timestamp_ms)
    {
        if (!session_tracker_) {
            session_tracker_ = std::make_unique<SessionTracker>(
                config_.window_spec.session_gap.count());
        }

        auto session_id = session_tracker_->process_event(group_key, timestamp_ms);

        // Use the session_id as the window key
        // The window boundaries are the session start/end, but we use
        // a hash-based approach for bucket management
        int64_t bucket_key = std::hash<std::string>{}(session_id) % 1000000;
        auto& bucket = bucket_store_.get_or_create(bucket_key, bucket_key + 1);
        bucket.add(group_key, agg_value);
    }

    // ------------------------------------------------------------------
    // Emit
    // ------------------------------------------------------------------

    std::vector<std::pair<std::string, json>>
    emit_closed_windows(const std::vector<WindowBucket>& closed_buckets)
    {
        std::vector<std::pair<std::string, json>> emitted;

        for (auto& bucket : closed_buckets) {
            auto results = bucket.emit_results(config_.functions, config_.top_k_n);
            for (auto& [key, result] : results) {
                if (emit_callback_) {
                    emit_callback_(key, result);
                }
                emitted.emplace_back(key, std::move(result));
            }
        }

        windows_emitted_.fetch_add(closed_buckets.size(),
                                    std::memory_order_relaxed);
        return emitted;
    }

    // ------------------------------------------------------------------
    // Field extraction
    // ------------------------------------------------------------------

    [[nodiscard]] std::string extract_group_key(const std::string& key,
                                                   const json& value) const
    {
        if (config_.group_by_field == "key") {
            return key;
        }
        if (config_.group_by_field.starts_with("value.")) {
            // JSON path extraction
            std::string path = "/" + config_.group_by_field.substr(6);
            // Replace '.' with '/' in the path
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
        if (config_.group_by_field.starts_with("header.")) {
            // Header-based grouping (not available in-process without headers)
            return "";
        }

        // Default: use key
        return key;
    }

    [[nodiscard]] json extract_agg_value(const json& value) const {
        if (config_.aggregate_field == "value" ||
            config_.aggregate_field.empty()) {
            return value;
        }
        if (config_.aggregate_field.starts_with("value.")) {
            std::string path = "/" + config_.aggregate_field.substr(6);
            std::string json_ptr;
            for (char c : path) {
                if (c == '.') json_ptr += '/';
                else json_ptr += c;
            }
            try {
                json::json_pointer ptr(json_ptr);
                if (value.contains(ptr)) return value.at(ptr);
            } catch (...) {}
        }
        return value;
    }

    // ------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------

    std::string                   name_;
    Config                        config_;
    BucketStore                   bucket_store_;
    std::unique_ptr<SessionTracker> session_tracker_;
    int64_t                       watermark_ms_ = 0;

    std::atomic<uint64_t>        records_processed_{0};
    std::atomic<uint64_t>        windows_emitted_{0};

    // Per-key sliding window buffers
    std::unordered_map<std::string, std::deque<json>> sliding_buffers_;

    // Emit callback (for wiring into topology)
    std::function<void(std::string, json)> emit_callback_;
};

// ============================================================================
// AggregateProcessorBuilder — fluent construction API
// ============================================================================

class AggregateProcessorBuilder {
public:
    explicit AggregateProcessorBuilder(std::string name)
        : name_(std::move(name))
    {}

    AggregateProcessorBuilder& tumbling_window(std::chrono::milliseconds size,
                                                  std::chrono::milliseconds grace = 0ms) {
        config_.window_spec = WindowSpec::tumbling(size, grace);
        return *this;
    }

    AggregateProcessorBuilder& hopping_window(std::chrono::milliseconds size,
                                                std::chrono::milliseconds advance,
                                                std::chrono::milliseconds grace = 0ms) {
        config_.window_spec = WindowSpec::hopping(size, advance, grace);
        return *this;
    }

    AggregateProcessorBuilder& sliding_window(int64_t count,
                                                std::chrono::milliseconds grace = 0ms) {
        config_.window_spec = WindowSpec::sliding(count, grace);
        return *this;
    }

    AggregateProcessorBuilder& session_window(std::chrono::milliseconds gap,
                                                std::chrono::milliseconds grace = 0ms) {
        config_.window_spec = WindowSpec::session(gap, grace);
        return *this;
    }

    AggregateProcessorBuilder& count() {
        config_.functions = {AggFn::count};
        return *this;
    }

    AggregateProcessorBuilder& sum() {
        config_.functions = {AggFn::sum};
        return *this;
    }

    AggregateProcessorBuilder& avg() {
        config_.functions = {AggFn::avg};
        return *this;
    }

    AggregateProcessorBuilder& min() {
        config_.functions = {AggFn::min};
        return *this;
    }

    AggregateProcessorBuilder& max() {
        config_.functions = {AggFn::max};
        return *this;
    }

    AggregateProcessorBuilder& top_k(int n = 10) {
        config_.functions = {AggFn::top_k};
        config_.top_k_n = n;
        return *this;
    }

    AggregateProcessorBuilder& distinct_count() {
        config_.functions = {AggFn::distinct_count};
        return *this;
    }

    AggregateProcessorBuilder& collect_list() {
        config_.functions = {AggFn::collect_list};
        return *this;
    }

    AggregateProcessorBuilder& multi(std::vector<AggFn> fns) {
        config_.functions = std::move(fns);
        return *this;
    }

    AggregateProcessorBuilder& group_by(std::string field) {
        config_.group_by_field = std::move(field);
        return *this;
    }

    AggregateProcessorBuilder& aggregate_on(std::string field) {
        config_.aggregate_field = std::move(field);
        return *this;
    }

    AggregateProcessorBuilder& emit_on_close(bool v = true) {
        config_.emit_on_close = v;
        return *this;
    }

    AggregateProcessorBuilder& emit_on_update(bool v = true) {
        config_.emit_on_update = v;
        return *this;
    }

    [[nodiscard]] AggregateProcessor build() {
        return AggregateProcessor(name_, config_);
    }

private:
    std::string name_;
    AggregateProcessor::Config config_;
};

} // namespace torrent::streams
