/**
 * punctuator.cpp — PunctuatorProcessor: Time-Based Triggers in Stream Topology
 *
 * Wall-clock, watermark, and event-time punctuators. Fires periodic callbacks
 * for window close, session timeout, and checkpoint triggers. Grace period
 * for late arrivals. Dedicated timer thread; thread-safe public API.
 */

#include "torrent/streams/topology.h"

#include "torrent/common/backoff.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <condition_variable>
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
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
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

std::shared_ptr<spdlog::logger> get_punct_logger() {
    static auto logger = spdlog::get("punctuator");
    if (!logger) {
        logger = spdlog::stdout_color_mt("punctuator");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Punctuator mode enumeration
// --------------------------------------------------------------------------

enum class PunctuatorMode : uint8_t {
    wall_clock   = 0,  // fire based on real time
    watermark    = 1,  // fire based on stream watermark
    event_time   = 2,  // fire based on record event-time
    hybrid       = 3,  // fire on wall-clock AND watermark
};

[[nodiscard]] std::string_view punctuator_mode_name(PunctuatorMode m) {
    switch (m) {
    case PunctuatorMode::wall_clock: return "wall_clock";
    case PunctuatorMode::watermark:  return "watermark";
    case PunctuatorMode::event_time: return "event_time";
    case PunctuatorMode::hybrid:     return "hybrid";
    }
    return "unknown";
}

[[nodiscard]] PunctuatorMode parse_punctuator_mode(std::string_view s) {
    if (s == "wall_clock" || s == "wall-clock") return PunctuatorMode::wall_clock;
    if (s == "watermark")                       return PunctuatorMode::watermark;
    if (s == "event_time" || s == "event-time") return PunctuatorMode::event_time;
    if (s == "hybrid")                          return PunctuatorMode::hybrid;
    return PunctuatorMode::wall_clock;
}

// --------------------------------------------------------------------------
// PunctuatorConfig — configuration for a punctuator node
// --------------------------------------------------------------------------

struct PunctuatorConfig {
    PunctuatorMode mode           = PunctuatorMode::wall_clock;
    int64_t        interval_ms    = 30'000;     // fire every N ms
    int64_t        watermark_step_ms = 10'000;  // watermark advance per fire
    int64_t        grace_period_ms   = 5'000;   // late arrival tolerance
    int64_t        initial_delay_ms  = 0;       // delay before first fire
    bool           fire_on_idle      = true;    // keep firing when no records
    int64_t        max_idle_fires    = 10;       // max consecutive idle fires
    int64_t        session_timeout_ms = 300'000; // session inactivity timeout

    [[nodiscard]] static PunctuatorConfig from_json(const json& j) {
        PunctuatorConfig cfg;
        cfg.mode           = parse_punctuator_mode(
            j.value("mode", "wall_clock"));
        cfg.interval_ms    = j.value("interval_ms", int64_t(30'000));
        cfg.watermark_step_ms = j.value("watermark_step_ms", int64_t(10'000));
        cfg.grace_period_ms   = j.value("grace_period_ms", int64_t(5'000));
        cfg.initial_delay_ms  = j.value("initial_delay_ms", int64_t(0));
        cfg.fire_on_idle      = j.value("fire_on_idle", true);
        cfg.max_idle_fires    = j.value("max_idle_fires", int64_t(10));
        cfg.session_timeout_ms = j.value("session_timeout_ms",
                                          int64_t(300'000));
        return cfg;
    }

    [[nodiscard]] json to_json() const {
        json j;
        j["mode"]          = std::string(punctuator_mode_name(mode));
        j["interval_ms"]   = interval_ms;
        j["watermark_step_ms"] = watermark_step_ms;
        j["grace_period_ms"]   = grace_period_ms;
        j["initial_delay_ms"]  = initial_delay_ms;
        j["fire_on_idle"]      = fire_on_idle;
        j["max_idle_fires"]    = max_idle_fires;
        j["session_timeout_ms"] = session_timeout_ms;
        return j;
    }
};

// --------------------------------------------------------------------------
// PunctuationEvent — delivered to the punctuate callback
// --------------------------------------------------------------------------

struct PunctuationEvent {
    int64_t wall_clock_ms   = 0;  // current wall-clock time
    int64_t watermark_ms    = 0;  // current stream watermark
    int64_t event_time_ms   = 0;  // highest observed event time
    int64_t fire_count      = 0;  // total fires since start
    int64_t records_since_last = 0; // records processed since last fire
    bool    is_idle         = false; // true if no records since last fire
    bool    is_initial      = false; // true for first fire

    [[nodiscard]] json to_json() const {
        return {
            {"wall_clock_ms",   wall_clock_ms},
            {"watermark_ms",    watermark_ms},
            {"event_time_ms",   event_time_ms},
            {"fire_count",      fire_count},
            {"records_since_last", records_since_last},
            {"is_idle",         is_idle},
            {"is_initial",      is_initial},
        };
    }
};

// --------------------------------------------------------------------------
// PunctuateCallback — user-supplied function invoked on each fire
// --------------------------------------------------------------------------

using PunctuateCallback = std::function<void(const PunctuationEvent&)>;

// --------------------------------------------------------------------------
// SessionTracker — tracks session activity for session timeout detection
// --------------------------------------------------------------------------

class SessionTracker {
public:
    explicit SessionTracker(std::string key)
        : key_(std::move(key))
        , last_activity_ms_(now_ms())
        , session_start_ms_(now_ms())
    {}

    void touch() {
        std::lock_guard lock(mtx_);
        last_activity_ms_ = now_ms();
        ++activity_count_;
    }

    [[nodiscard]] bool is_expired(int64_t timeout_ms) const {
        std::lock_guard lock(mtx_);
        return (now_ms() - last_activity_ms_) > timeout_ms;
    }

    [[nodiscard]] int64_t idle_ms() const {
        std::lock_guard lock(mtx_);
        return now_ms() - last_activity_ms_;
    }

    [[nodiscard]] int64_t session_age_ms() const {
        std::lock_guard lock(mtx_);
        return now_ms() - session_start_ms_;
    }

    [[nodiscard]] const std::string& key() const noexcept { return key_; }

    [[nodiscard]] json status() const {
        std::lock_guard lock(mtx_);
        return {
            {"key",              key_},
            {"last_activity_ms", last_activity_ms_},
            {"session_start_ms", session_start_ms_},
            {"activity_count",   activity_count_},
            {"idle_ms",          now_ms() - last_activity_ms_},
        };
    }

private:
    static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    std::string         key_;
    mutable std::mutex  mtx_;
    int64_t             last_activity_ms_  = 0;
    int64_t             session_start_ms_ = 0;
    uint64_t            activity_count_    = 0;
};

// --------------------------------------------------------------------------
// PunctuatorMetrics — runtime counters
// --------------------------------------------------------------------------

struct PunctuatorMetrics {
    std::atomic<uint64_t> total_fires{0};
    std::atomic<uint64_t> idle_fires{0};
    std::atomic<uint64_t> skipped_fires{0};
    std::atomic<uint64_t> sessions_expired{0};
    std::atomic<uint64_t> late_records_dropped{0};
    std::atomic<int64_t>  last_watermark_ms{0};
    std::atomic<int64_t>  last_event_time_ms{0};

    void reset() {
        total_fires.store(0);
        idle_fires.store(0);
        skipped_fires.store(0);
        sessions_expired.store(0);
        late_records_dropped.store(0);
        last_watermark_ms.store(0);
        last_event_time_ms.store(0);
    }

    [[nodiscard]] json snapshot() const {
        return {
            {"total_fires",          total_fires.load()},
            {"idle_fires",           idle_fires.load()},
            {"skipped_fires",        skipped_fires.load()},
            {"sessions_expired",     sessions_expired.load()},
            {"late_records_dropped", late_records_dropped.load()},
            {"last_watermark_ms",    last_watermark_ms.load()},
            {"last_event_time_ms",   last_event_time_ms.load()},
        };
    }
};

// --------------------------------------------------------------------------
// Time helpers
// --------------------------------------------------------------------------

[[nodiscard]] int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

} // anonymous namespace

// ============================================================================
// PunctuatorProcessor — a time-based trigger node in the stream topology
// ============================================================================

class PunctuatorProcessor {
public:
    explicit PunctuatorProcessor(std::string name, PunctuatorConfig cfg)
        : name_(std::move(name))
        , config_(std::move(cfg))
    {}

    ~PunctuatorProcessor() { stop(); }

    PunctuatorProcessor(const PunctuatorProcessor&) = delete;
    PunctuatorProcessor& operator=(const PunctuatorProcessor&) = delete;

    // ----------------------------------------------------------------------
    // Lifecycle
    // ----------------------------------------------------------------------

    /// Start the punctuator.  Begins firing on a background timer thread.
    void start(PunctuateCallback callback) {
        if (running_.exchange(true)) {
            get_punct_logger()->warn(
                "Punctuator '{}': already running", name_);
            return;
        }

        callback_    = std::move(callback);
        fire_count_  = 0;
        start_time_ms_ = now_ms();

        get_punct_logger()->info(
            "Punctuator '{}': starting, mode={} interval={}ms",
            name_, punctuator_mode_name(config_.mode), config_.interval_ms);

        timer_thread_ = std::thread([this] { run_loop(); });
    }

    /// Gracefully stop the punctuator.
    void stop() {
        if (!running_.exchange(false)) return;

        {
            std::lock_guard lock(loop_mtx_);
        }
        loop_cv_.notify_all();

        if (timer_thread_.joinable()) {
            timer_thread_.join();
        }

        get_punct_logger()->info(
            "Punctuator '{}': stopped, total fires={}",
            name_, metrics_.total_fires.load());
    }

    // ----------------------------------------------------------------------
    // Record notification — called when records flow through
    // ----------------------------------------------------------------------

    /// Notify the punctuator that records have been processed.
    /// Advances the event-time watermark accordingly.
    void notify_record(int64_t event_time_ms) {
        records_since_last_fire_.fetch_add(1, std::memory_order_relaxed);

        // Track highest event-time seen
        int64_t current = metrics_.last_event_time_ms.load(
            std::memory_order_relaxed);
        while (event_time_ms > current) {
            if (metrics_.last_event_time_ms.compare_exchange_weak(
                    current, event_time_ms, std::memory_order_release,
                    std::memory_order_relaxed))
                break;
        }
    }

    /// Explicitly advance the stream watermark.
    void advance_watermark(int64_t watermark_ms) {
        int64_t current = metrics_.last_watermark_ms.load(
            std::memory_order_relaxed);
        while (watermark_ms > current) {
            if (metrics_.last_watermark_ms.compare_exchange_weak(
                    current, watermark_ms, std::memory_order_release,
                    std::memory_order_relaxed))
                break;
        }

        // Wake up watermark-based punctuator
        loop_cv_.notify_one();
    }

    // ----------------------------------------------------------------------
    // Session management
    // ----------------------------------------------------------------------

    /// Register or touch a session by key.
    void touch_session(const std::string& session_key) {
        std::unique_lock lock(sessions_mtx_);
        auto it = sessions_.find(session_key);
        if (it == sessions_.end()) {
            sessions_.emplace(session_key,
                std::make_shared<SessionTracker>(session_key));
        } else {
            it->second->touch();
        }
    }

    /// Remove a session (e.g., after emission).
    void remove_session(const std::string& session_key) {
        std::unique_lock lock(sessions_mtx_);
        sessions_.erase(session_key);
    }

    /// Find and optionally expire idle sessions.
    [[nodiscard]] std::vector<std::string> expire_sessions() {
        std::vector<std::string> expired;
        int64_t timeout = config_.session_timeout_ms;

        {
            std::shared_lock lock(sessions_mtx_);
            for (auto& [key, tracker] : sessions_) {
                if (tracker->is_expired(timeout)) {
                    expired.push_back(key);
                }
            }
        }

        if (!expired.empty()) {
            std::unique_lock lock(sessions_mtx_);
            for (auto& key : expired) {
                sessions_.erase(key);
            }
            metrics_.sessions_expired.fetch_add(expired.size(),
                std::memory_order_relaxed);
            get_punct_logger()->debug(
                "Punctuator '{}': expired {} sessions", name_, expired.size());
        }

        return expired;
    }

    // ----------------------------------------------------------------------
    // Late-arrival handling
    // ----------------------------------------------------------------------

    /// Check if a record is within the grace period for a given window end.
    [[nodiscard]] bool is_within_grace(int64_t window_end,
                                        int64_t event_time_ms) const {
        return event_time_ms <= window_end + config_.grace_period_ms;
    }

    /// Determine if a record should be dropped as too late.
    [[nodiscard]] bool is_late(int64_t watermark, int64_t event_time_ms) const {
        if (watermark == 0) return false;
        bool late = event_time_ms + config_.grace_period_ms < watermark;
        if (late) {
            metrics_.late_records_dropped.fetch_add(1,
                std::memory_order_relaxed);
        }
        return late;
    }

    // ----------------------------------------------------------------------
    // Query
    // ----------------------------------------------------------------------

    [[nodiscard]] bool is_running() const noexcept { return running_.load(); }
    [[nodiscard]] int64_t fire_count() const noexcept { return fire_count_; }
    [[nodiscard]] int64_t session_count() const {
        std::shared_lock lock(sessions_mtx_);
        return static_cast<int64_t>(sessions_.size());
    }
    [[nodiscard]] const std::string& name() const noexcept { return name_; }
    [[nodiscard]] const PunctuatorMetrics& metrics() const noexcept {
        return metrics_;
    }

    [[nodiscard]] json status() const {
        json j;
        j["name"]       = name_;
        j["mode"]       = punctuator_mode_name(config_.mode);
        j["interval_ms"] = config_.interval_ms;
        j["running"]    = running_.load();
        j["fire_count"] = fire_count_;
        j["metrics"]    = metrics_.snapshot();

        {
            std::shared_lock lock(sessions_mtx_);
            j["active_sessions"] = sessions_.size();
            json session_arr = json::array();
            int count = 0;
            for (auto& [key, tracker] : sessions_) {
                if (count++ >= 50) break; // limit output
                session_arr.push_back(tracker->status());
            }
            j["session_sample"] = session_arr;
        }

        return j;
    }

private:
    // ----------------------------------------------------------------------
    // Main timer loop
    // ----------------------------------------------------------------------

    void run_loop() {
        auto logger = get_punct_logger();

        // Initial delay
        if (config_.initial_delay_ms > 0) {
            std::unique_lock lock(loop_mtx_);
            loop_cv_.wait_for(lock,
                std::chrono::milliseconds(config_.initial_delay_ms));
        }

        while (running_.load(std::memory_order_acquire)) {
            int64_t now = now_ms();

            // Determine if we should fire
            bool should_fire = evaluate_fire_condition(now);

            if (should_fire) {
                fire(now);
            }

            // Wait for next interval
            std::unique_lock lock(loop_mtx_);
            auto wait_time = std::chrono::milliseconds(config_.interval_ms);

            if (config_.mode == PunctuatorMode::watermark ||
                config_.mode == PunctuatorMode::hybrid) {
                // For watermark mode, wake up more frequently to check progress
                wait_time = std::min(wait_time, 100ms);
            }

            loop_cv_.wait_for(lock, wait_time,
                [this] { return !running_.load(std::memory_order_acquire); });
        }
    }

    [[nodiscard]] bool evaluate_fire_condition(int64_t wall_ms) {
        switch (config_.mode) {
        case PunctuatorMode::wall_clock: {
            // Fire every interval_ms regardless
            return true;
        }

        case PunctuatorMode::watermark: {
            // Fire only if watermark has advanced since last fire
            int64_t current_wm = metrics_.last_watermark_ms.load(
                std::memory_order_acquire);
            int64_t fired_wm   = last_fired_watermark_;
            return (current_wm - fired_wm) >= config_.watermark_step_ms;
        }

        case PunctuatorMode::event_time: {
            // Fire based on record event-time progress
            int64_t current_et = metrics_.last_event_time_ms.load(
                std::memory_order_acquire);
            int64_t fired_et   = last_fired_event_time_;
            return (current_et - fired_et) >= config_.watermark_step_ms;
        }

        case PunctuatorMode::hybrid: {
            // Fire on wall clock AND check watermark
            return true; // wall-clock side always ready; fire() checks watermark
        }
        }
        return true;
    }

    void fire(int64_t wall_ms) {
        int64_t wm     = metrics_.last_watermark_ms.load(
            std::memory_order_acquire);
        int64_t et     = metrics_.last_event_time_ms.load(
            std::memory_order_acquire);
        int64_t recs   = records_since_last_fire_.exchange(
            0, std::memory_order_relaxed);
        bool is_idle   = (recs == 0);

        // Check idle fire limit
        if (is_idle) {
            idle_fire_count_++;
            if (!config_.fire_on_idle ||
                idle_fire_count_ > config_.max_idle_fires) {
                metrics_.skipped_fires.fetch_add(1,
                    std::memory_order_relaxed);
                return;
            }
        } else {
            idle_fire_count_ = 0;
        }

        ++fire_count_;
        bool is_initial = (fire_count_ == 1);

        PunctuationEvent event;
        event.wall_clock_ms     = wall_ms;
        event.watermark_ms      = wm;
        event.event_time_ms     = et;
        event.fire_count        = fire_count_;
        event.records_since_last = recs;
        event.is_idle           = is_idle;
        event.is_initial        = is_initial;

        // Update last-fired tracking
        last_fired_watermark_ = wm;
        last_fired_event_time_ = et;

        // Expire idle sessions
        expire_sessions();

        // Update metrics
        metrics_.total_fires.fetch_add(1, std::memory_order_relaxed);
        if (is_idle) {
            metrics_.idle_fires.fetch_add(1, std::memory_order_relaxed);
        }

        // Invoke the user callback
        if (callback_) {
            try {
                callback_(event);
            } catch (const std::exception& e) {
                get_punct_logger()->error(
                    "Punctuator '{}': callback error: {}", name_, e.what());
            } catch (...) {
                get_punct_logger()->error(
                    "Punctuator '{}': unknown callback error", name_);
            }
        }

        get_punct_logger()->trace(
            "Punctuator '{}': fired #{} wall={} wm={} et={} recs={} idle={}",
            name_, fire_count_, wall_ms, wm, et, recs, is_idle);
    }

    // ----------------------------------------------------------------------
    // Members
    // ----------------------------------------------------------------------

    std::string          name_;
    PunctuatorConfig     config_;
    PunctuateCallback    callback_;

    std::atomic<bool>    running_{false};
    int64_t              fire_count_ = 0;
    int64_t              start_time_ms_ = 0;
    int64_t              idle_fire_count_ = 0;
    std::atomic<int64_t> records_since_last_fire_{0};

    // Watermark / event-time tracking for condition evaluation
    int64_t              last_fired_watermark_ = 0;
    int64_t              last_fired_event_time_ = 0;

    // Session tracking
    mutable std::shared_mutex sessions_mtx_;
    std::unordered_map<std::string, std::shared_ptr<SessionTracker>> sessions_;

    // Timer thread
    std::thread              timer_thread_;
    std::mutex               loop_mtx_;
    std::condition_variable  loop_cv_;

    // Metrics
    PunctuatorMetrics metrics_;
};

} // namespace torrent::streams
