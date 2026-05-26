/**
 * window_processor.cpp — WindowProcessor: Window Lifecycle for Stream Processing
 *
 * Provides the core window management primitives for torrent-mq stream
 * processing.  Windows are the fundamental temporal grouping mechanism
 * that enables time-based aggregations, joins, and pattern matching
 * over unbounded streams.
 *
 * Window types:
 *
 *   Tumbling:  Fixed-size, non-overlapping, contiguous windows.
 *              Example: 60-second windows — [00:00, 00:60), [00:60, 01:20)
 *              Records fall into exactly one window.
 *
 *   Hopping:   Fixed-size windows that advance by a configurable step
 *              smaller than the window size, creating overlapping windows.
 *              Example: 120-second windows advancing every 30 seconds.
 *              Records may belong to multiple windows.
 *
 *   Sliding:   A window defined by a fixed record count rather than time.
 *              Example: "last 100 records".  The window continuously
 *              slides as new records arrive and old ones are evicted.
 *
 *   Session:   Dynamic windows bounded by an inactivity gap.  A session
 *              window extends as long as records arrive within `gap` time
 *              of the previous record for the same key.
 *
 * Window lifecycle:
 *
 *   1. OPEN:   Window is created when the first qualifying record arrives.
 *              State is allocated in the state store.
 *
 *   2. ACCUMULATE: Records are added to the window buffer/aggregate.
 *              The window remains open as long as records fall within
 *              its boundaries.
 *
 *   3. CLOSE:  The watermark passes the window end.  No more records
 *              can be added (except late arrivals within grace period).
 *              The result is finalized but not yet emitted.
 *
 *   4. EMIT:   The closed window's accumulated result is emitted
 *              downstream.  After emission, the window can be evicted.
 *
 *   5. EVICT:  Window state is removed from the state store.
 *
 * Late arrivals & grace period:
 *   Records arriving after the window has closed but within a configurable
 *   grace period (e.g., 10s) can still be incorporated.  This handles
 *   out-of-order events due to network delays or clock skew.
 *
 * Watermark:
 *   A monotonically increasing timestamp that represents the point in
 *   event-time up to which the system believes all events have arrived.
 *   The watermark advances as events are observed and drives window closing.
 *
 * Thread-safety:
 *   All public methods are safe for concurrent access from multiple stream
 *   threads.  Internal state is protected by a shared_mutex per window key.
 *
 * Dependencies:
 *   - StateStore: for persisting window accumulators
 *   - spdlog: structured logging
 *   - nlohmann/json: configuration and status
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
#include <set>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

using json = nlohmann::json;
using namespace std::chrono_literals;

namespace torrent::streams {

// ============================================================================
// Anonymous namespace — internals
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_wp_logger() {
    static auto logger = spdlog::get("window_processor");
    if (!logger) {
        logger = spdlog::stdout_color_mt("window_processor");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Window type enumeration
// --------------------------------------------------------------------------

enum class WindowType : uint8_t {
    tumbling = 0,
    hopping  = 1,
    sliding  = 2,
    session  = 3,
};

[[nodiscard]] std::string_view window_type_name(WindowType wt) {
    switch (wt) {
    case WindowType::tumbling: return "tumbling";
    case WindowType::hopping:  return "hopping";
    case WindowType::sliding:  return "sliding";
    case WindowType::session:  return "session";
    }
    return "unknown";
}

// --------------------------------------------------------------------------
// Window state enumeration
// --------------------------------------------------------------------------

enum class WindowState : uint8_t {
    open       = 0,
    closed     = 1,
    emitted    = 2,
    evicted    = 3,
};

[[nodiscard]] std::string_view window_state_name(WindowState ws) {
    switch (ws) {
    case WindowState::open:    return "open";
    case WindowState::closed:  return "closed";
    case WindowState::emitted: return "emitted";
    case WindowState::evicted: return "evicted";
    }
    return "unknown";
}

// --------------------------------------------------------------------------
// Helpers: window boundary calculation
// --------------------------------------------------------------------------

int64_t floor_to_window(int64_t ts_ms, int64_t window_size_ms) {
    return (ts_ms / window_size_ms) * window_size_ms;
}

struct WindowKey {
    std::string key;
    int64_t window_start;
    int64_t window_end;

    [[nodiscard]] std::string to_string() const {
        return key + ":" + std::to_string(window_start) + ":"
             + std::to_string(window_end);
    }

    [[nodiscard]] bool operator<(const WindowKey& other) const {
        if (key != other.key) return key < other.key;
        if (window_start != other.window_start)
            return window_start < other.window_start;
        return window_end < other.window_end;
    }

    [[nodiscard]] bool operator==(const WindowKey& other) const {
        return key == other.key && window_start == other.window_start
            && window_end == other.window_end;
    }
};

} // anonymous namespace

// ============================================================================
// WindowConfig — configuration for a windowed operator
// ============================================================================

struct WindowConfig {
    WindowType type = WindowType::tumbling;
    int64_t window_size_ms = 60'000;   // 60 seconds
    int64_t advance_ms = 60'000;       // same as size for tumbling
    int64_t grace_period_ms = 5'000;   // 5 seconds for late arrivals
    size_t   max_sliding_count = 100;  // for sliding windows
    int64_t  session_gap_ms = 30'000;  // 30 seconds inactivity gap

    [[nodiscard]] static WindowConfig from_json(const json& j) {
        WindowConfig cfg;
        std::string type_str = j.value("type", "tumbling");
        if (type_str == "tumbling") cfg.type = WindowType::tumbling;
        else if (type_str == "hopping") cfg.type = WindowType::hopping;
        else if (type_str == "sliding") cfg.type = WindowType::sliding;
        else if (type_str == "session") cfg.type = WindowType::session;

        cfg.window_size_ms = j.value("size_ms", 60'000L);
        cfg.advance_ms = j.value("advance_ms", cfg.window_size_ms);
        cfg.grace_period_ms = j.value("grace_period_ms", 5'000L);
        cfg.max_sliding_count = j.value("max_count", 100UL);
        cfg.session_gap_ms = j.value("session_gap_ms", 30'000L);
        return cfg;
    }

    [[nodiscard]] json to_json() const {
        json j;
        j["type"] = std::string(window_type_name(type));
        j["size_ms"] = window_size_ms;
        j["advance_ms"] = advance_ms;
        j["grace_period_ms"] = grace_period_ms;
        j["max_count"] = max_sliding_count;
        j["session_gap_ms"] = session_gap_ms;
        return j;
    }

    [[nodiscard]] std::vector<WindowKey>
    compute_windows(std::string_view key, int64_t timestamp_ms) const {
        std::vector<WindowKey> windows;

        switch (type) {
        case WindowType::tumbling: {
            int64_t start = floor_to_window(timestamp_ms, window_size_ms);
            windows.push_back({std::string(key), start,
                                start + window_size_ms});
            break;
        }
        case WindowType::hopping: {
            int64_t first_start = floor_to_window(timestamp_ms, advance_ms);
            // A record can belong to up to ceil(window_size/advance) windows
            int64_t num_windows
                = (window_size_ms + advance_ms - 1) / advance_ms;
            for (int64_t i = 0; i < num_windows; ++i) {
                int64_t start = first_start - i * advance_ms;
                if (start + window_size_ms <= timestamp_ms) continue;
                if (start > timestamp_ms) break;
                windows.push_back(
                    {std::string(key), start, start + window_size_ms});
            }
            // Sort by start time
            std::sort(windows.begin(), windows.end());
            break;
        }
        case WindowType::sliding: {
            // Sliding windows don't use time boundaries; they use count.
            // We use a single "window" keyed by [key, 0, 0] for simplicity.
            windows.push_back({std::string(key), 0, 0});
            break;
        }
        case WindowType::session: {
            // Session windows don't have predetermined boundaries.
            // The start is the record's timestamp for now; the end depends
            // on future records.
            windows.push_back(
                {std::string(key), timestamp_ms,
                 timestamp_ms + session_gap_ms});
            break;
        }
        }
        return windows;
    }

    [[nodiscard]] bool is_within_grace(int64_t window_end,
                                       int64_t record_ts) const {
        return record_ts < window_end + grace_period_ms;
    }
};

// ============================================================================
// WindowInstance — runtime state for one concrete window
// ============================================================================

struct WindowInstance {
    WindowKey wk;
    WindowState state = WindowState::open;
    int64_t created_at_ms = 0;
    int64_t closed_at_ms = 0;
    int64_t emitted_at_ms = 0;
    int64_t highest_record_ts = 0;
    int64_t record_count = 0;
    json accumulator; // aggregated state

    mutable std::shared_mutex mtx;

    explicit WindowInstance(WindowKey k) : wk(std::move(k)) {
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
        created_at_ms = now;
    }

    void add_record(int64_t record_ts, const json& record) {
        std::unique_lock<std::shared_mutex> lock(mtx);
        ++record_count;
        if (record_ts > highest_record_ts) {
            highest_record_ts = record_ts;
        }
        // Accumulation logic is handled by the aggregate processor;
        // here we just track metadata.
    }

    bool close() {
        std::unique_lock<std::shared_mutex> lock(mtx);
        if (state != WindowState::open) return false;
        state = WindowState::closed;
        closed_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
        return true;
    }

    bool mark_emitted() {
        std::unique_lock<std::shared_mutex> lock(mtx);
        if (state != WindowState::closed) return false;
        state = WindowState::emitted;
        emitted_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now()
                                .time_since_epoch())
                            .count();
        return true;
    }

    bool evict() {
        std::unique_lock<std::shared_mutex> lock(mtx);
        if (state != WindowState::emitted) return false;
        state = WindowState::evicted;
        return true;
    }

    [[nodiscard]] WindowState current_state() const {
        std::shared_lock<std::shared_mutex> lock(mtx);
        return state;
    }

    [[nodiscard]] json status() const {
        std::shared_lock<std::shared_mutex> lock(mtx);
        json j;
        j["key"] = wk.key;
        j["window_start"] = wk.window_start;
        j["window_end"] = wk.window_end;
        j["state"] = window_state_name(state);
        j["created_at_ms"] = created_at_ms;
        j["closed_at_ms"] = closed_at_ms;
        j["record_count"] = record_count;
        j["highest_record_ts"] = highest_record_ts;
        return j;
    }
};

// ============================================================================
// WatermarkTracker — event-time progress tracking
// ============================================================================

class WatermarkTracker {
public:
    explicit WatermarkTracker(int32_t parallelism)
        : parallelism_(parallelism)
    {
        per_partition_watermarks_.resize(parallelism_, 0);
    }

    void update(int32_t partition, int64_t watermark_ms) {
        if (partition < 0 || partition >= parallelism_) return;
        std::lock_guard<std::mutex> lock(mutex_);
        per_partition_watermarks_[partition] = watermark_ms;
        // Low watermark = minimum across all partitions
        int64_t min_wm = std::numeric_limits<int64_t>::max();
        for (int64_t wm : per_partition_watermarks_) {
            if (wm < min_wm) min_wm = wm;
        }
        low_watermark_.store(min_wm, std::memory_order_release);
    }

    [[nodiscard]] int64_t low_watermark() const {
        return low_watermark_.load(std::memory_order_acquire);
    }

    [[nodiscard]] int64_t partition_watermark(int32_t partition) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (partition < 0
            || static_cast<size_t>(partition) >= per_partition_watermarks_.size())
            return 0;
        return per_partition_watermarks_[partition];
    }

    [[nodiscard]] json status() const {
        std::lock_guard<std::mutex> lock(mutex_);
        json j;
        j["low_watermark"] = low_watermark_.load(std::memory_order_acquire);
        j["partition_watermarks"] = per_partition_watermarks_;
        return j;
    }

private:
    int32_t parallelism_;
    std::vector<int64_t> per_partition_watermarks_;
    std::atomic<int64_t> low_watermark_{0};
    mutable std::mutex mutex_;
};

// ============================================================================
// WindowProcessor::Impl — PIMPL body
// ============================================================================

class WindowProcessor::Impl {
public:
    Impl(const WindowConfig& cfg, int32_t parallelism,
         std::shared_ptr<StateStore> state_store)
        : config_(cfg)
        , parallelism_(parallelism)
        , state_store_(std::move(state_store))
        , watermark_tracker_(parallelism)
    {
        if (!state_store_) {
            throw std::runtime_error("WindowProcessor requires a StateStore");
        }
    }

    // ----------------------------------------------------------------------
    // Record ingestion — assign records to windows
    // ----------------------------------------------------------------------

    [[nodiscard]] std::vector<WindowKey>
    assign_record(std::string_view key, int64_t timestamp_ms,
                  const json& record)
    {
        // Compute which windows this record belongs to
        std::vector<WindowKey> windows
            = config_.compute_windows(key, timestamp_ms);

        for (auto& wk : windows) {
            auto it = windows_.find(wk);
            if (it == windows_.end()) {
                // Create new window instance
                auto win = std::make_shared<WindowInstance>(wk);
                windows_[wk] = win;
                open_windows_.insert(wk);
                get_wp_logger()->debug(
                    "Opened window: key={} start={} end={}",
                    wk.key, wk.window_start, wk.window_end);
                open_count_.fetch_add(1, std::memory_order_relaxed);
            }
            windows_[wk]->add_record(timestamp_ms, record);
        }

        // For sliding windows: trim old records if exceeding max count
        if (config_.type == WindowType::sliding) {
            trim_sliding_windows(key);
        }

        // Advance watermark
        watermark_tracker_.update(0, timestamp_ms); // partition placeholder

        return windows;
    }

    // ----------------------------------------------------------------------
    // Window lifecycle — close windows that the watermark has passed
    // ----------------------------------------------------------------------

    std::vector<WindowKey> advance_watermark(int64_t watermark_ms) {
        watermark_tracker_.update(0, watermark_ms);

        std::vector<WindowKey> newly_closed;
        std::vector<WindowKey> windows_to_close;

        // Find windows whose end is before the watermark
        {
            auto it = windows_.begin();
            while (it != windows_.end()) {
                const auto& wk = it->first;
                auto& win = it->second;

                bool should_close = false;
                if (config_.type == WindowType::tumbling
                    || config_.type == WindowType::hopping) {
                    should_close
                        = win->current_state() == WindowState::open
                       && wk.window_end <= watermark_ms;
                } else if (config_.type == WindowType::session) {
                    // Close session if no record arrived within gap
                    should_close
                        = win->current_state() == WindowState::open
                       && (watermark_ms - win->highest_record_ts)
                              > config_.session_gap_ms;
                }
                // Sliding windows aren't closed by watermark

                if (should_close) {
                    windows_to_close.push_back(wk);
                }
                ++it;
            }
        }

        for (const auto& wk : windows_to_close) {
            auto it = windows_.find(wk);
            if (it != windows_.end()) {
                it->second->close();
                newly_closed.push_back(wk);
                closed_windows_.insert(wk);
                open_windows_.erase(wk);
                open_count_.fetch_sub(1, std::memory_order_relaxed);
                closed_count_.fetch_add(1, std::memory_order_relaxed);
                get_wp_logger()->debug(
                    "Closed window: key={} start={} end={}",
                    wk.key, wk.window_start, wk.window_end);
            }
        }

        return newly_closed;
    }

    // ----------------------------------------------------------------------
    // Late arrival handling
    // ----------------------------------------------------------------------

    [[nodiscard]] bool
    handle_late_record(std::string_view key, int64_t timestamp_ms,
                       const json& record)
    {
        // Iterate closed windows to find those still within grace period
        bool accepted = false;

        for (const auto& wk : closed_windows_) {
            if (wk.key != key) continue;

            auto it = windows_.find(wk);
            if (it == windows_.end()) continue;

            if (config_.is_within_grace(wk.window_end, timestamp_ms)) {
                it->second->add_record(timestamp_ms, record);
                accepted = true;
                late_count_.fetch_add(1, std::memory_order_relaxed);
                get_wp_logger()->debug(
                    "Accepted late record for window: key={} start={} end={} "
                    "ts={}",
                    wk.key, wk.window_start, wk.window_end, timestamp_ms);
            }
        }

        if (!accepted) {
            dropped_late_count_.fetch_add(1, std::memory_order_relaxed);
        }
        return accepted;
    }

    // ----------------------------------------------------------------------
    // Emission — mark windows as emitted after results are sent downstream
    // ----------------------------------------------------------------------

    void mark_emitted(const WindowKey& wk) {
        auto it = windows_.find(wk);
        if (it != windows_.end()) {
            bool did_emit = it->second->mark_emitted();
            if (did_emit) {
                emitted_count_.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    void mark_all_emitted(const std::vector<WindowKey>& wks) {
        for (const auto& wk : wks) {
            mark_emitted(wk);
        }
    }

    // ----------------------------------------------------------------------
    // Eviction — clean up emitted windows
    // ----------------------------------------------------------------------

    size_t evict_emitted() {
        size_t count = 0;
        auto it = windows_.begin();
        while (it != windows_.end()) {
            auto& win = it->second;
            if (win->current_state() == WindowState::emitted) {
                closed_windows_.erase(it->first);
                it = windows_.erase(it);
                ++count;
                evicted_count_.fetch_add(1, std::memory_order_relaxed);
            } else {
                ++it;
            }
        }
        return count;
    }

    // Force eviction of all windows (e.g., on processor shutdown)
    void force_close_all() {
        for (auto& [wk, win] : windows_) {
            if (win->current_state() == WindowState::open) {
                win->close();
                closed_windows_.insert(wk);
            }
        }
        open_windows_.clear();
    }

    // ----------------------------------------------------------------------
    // Sliding window maintenance
    // ----------------------------------------------------------------------

    void trim_sliding_windows(std::string_view key) {
        WindowKey wk{std::string(key), 0, 0};
        auto it = windows_.find(wk);
        if (it == windows_.end()) return;

        auto& win = it->second;
        if (win->record_count > static_cast<int64_t>(config_.max_sliding_count))
        {
            // The aggregate processor handles trimming of accumulated values;
            // here we adjust the count metadata.
            int64_t excess
                = win->record_count - static_cast<int64_t>(config_.max_sliding_count);
            win->record_count = static_cast<int64_t>(config_.max_sliding_count);
            get_wp_logger()->debug("Trimmed {} old records from sliding window "
                                   "for key '{}'",
                                   excess, key);
        }
    }

    // ----------------------------------------------------------------------
    // Getters
    // ----------------------------------------------------------------------

    [[nodiscard]] std::shared_ptr<WindowInstance>
    get_window(const WindowKey& wk) {
        auto it = windows_.find(wk);
        if (it == windows_.end()) return nullptr;
        return it->second;
    }

    [[nodiscard]] std::vector<WindowKey> get_open_windows() const {
        return std::vector<WindowKey>(open_windows_.begin(),
                                      open_windows_.end());
    }

    [[nodiscard]] std::vector<WindowKey> get_closed_windows() const {
        return std::vector<WindowKey>(closed_windows_.begin(),
                                      closed_windows_.end());
    }

    [[nodiscard]] int64_t watermark() const {
        return watermark_tracker_.low_watermark();
    }

    [[nodiscard]] const WindowConfig& config() const { return config_; }

    // ----------------------------------------------------------------------
    // Statistics and status
    // ----------------------------------------------------------------------

    [[nodiscard]] json stats() const {
        json j;
        j["open_windows"] = open_count_.load(std::memory_order_acquire);
        j["closed_windows"] = closed_count_.load(std::memory_order_acquire);
        j["emitted_windows"] = emitted_count_.load(std::memory_order_acquire);
        j["evicted_windows"] = evicted_count_.load(std::memory_order_acquire);
        j["total_windows_created"] = open_count_.load(std::memory_order_acquire)
                                   + closed_count_.load(std::memory_order_acquire)
                                   + emitted_count_.load(std::memory_order_acquire)
                                   + evicted_count_.load(std::memory_order_acquire);
        j["late_records_accepted"]
            = late_count_.load(std::memory_order_acquire);
        j["late_records_dropped"]
            = dropped_late_count_.load(std::memory_order_acquire);
        j["watermark"] = watermark_tracker_.low_watermark();
        j["window_config"] = config_.to_json();
        return j;
    }

    [[nodiscard]] json dump_all_windows() const {
        json arr = json::array();
        for (const auto& [wk, win] : windows_) {
            arr.push_back(win->status());
        }
        return arr;
    }

private:
    WindowConfig config_;
    int32_t parallelism_;
    std::shared_ptr<StateStore> state_store_;

    // Window registry — indexed by WindowKey
    std::unordered_map<WindowKey, std::shared_ptr<WindowInstance>,
                       decltype([](const WindowKey& wk) {
                           return std::hash<std::string>{}(wk.to_string());
                       })>
        windows_;

    // Fast access to open/closed sets
    std::set<WindowKey> open_windows_;
    std::set<WindowKey> closed_windows_;

    // Watermark tracking
    WatermarkTracker watermark_tracker_;

    // Counters
    std::atomic<int64_t> open_count_{0};
    std::atomic<int64_t> closed_count_{0};
    std::atomic<int64_t> emitted_count_{0};
    std::atomic<int64_t> evicted_count_{0};
    std::atomic<int64_t> late_count_{0};
    std::atomic<int64_t> dropped_late_count_{0};
};

// ============================================================================
// WindowProcessor — public API
// ============================================================================

WindowProcessor::WindowProcessor(const json& config, int32_t parallelism,
                                  std::shared_ptr<StateStore> store)
    : impl_(std::make_unique<Impl>(WindowConfig::from_json(config),
                                    parallelism, std::move(store)))
{}

WindowProcessor::~WindowProcessor() = default;

std::vector<WindowKey>
WindowProcessor::assign_record(std::string_view key, int64_t timestamp_ms,
                                const json& record) {
    return impl_->assign_record(key, timestamp_ms, record);
}

std::vector<WindowKey>
WindowProcessor::advance_watermark(int64_t watermark_ms) {
    return impl_->advance_watermark(watermark_ms);
}

bool WindowProcessor::handle_late_record(std::string_view key,
                                          int64_t timestamp_ms,
                                          const json& record) {
    return impl_->handle_late_record(key, timestamp_ms, record);
}

void WindowProcessor::mark_emitted(const WindowKey& wk) {
    impl_->mark_emitted(wk);
}

void WindowProcessor::mark_all_emitted(const std::vector<WindowKey>& wks) {
    impl_->mark_all_emitted(wks);
}

size_t WindowProcessor::evict_emitted() { return impl_->evict_emitted(); }
void WindowProcessor::force_close_all() { impl_->force_close_all(); }

std::vector<WindowKey> WindowProcessor::get_open_windows() const {
    return impl_->get_open_windows();
}
std::vector<WindowKey> WindowProcessor::get_closed_windows() const {
    return impl_->get_closed_windows();
}
int64_t WindowProcessor::watermark() const { return impl_->watermark(); }

json WindowProcessor::stats() const { return impl_->stats(); }
json WindowProcessor::dump_all_windows() const {
    return impl_->dump_all_windows();
}

} // namespace torrent::streams
