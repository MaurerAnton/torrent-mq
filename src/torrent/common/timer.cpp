/**
 * torrent-mq — Hierarchical Timing Wheel Implementation
 *
 * Provides high-resolution timer management for the torrent-mq broker and
 * client libraries. The timing wheel is an efficient data structure for
 * scheduling and firing large numbers of timeouts (O(1) insert, O(1)
 * per-tick amortized) compared to a priority heap (O(log n)).
 *
 * Architecture:
 *   - Hierarchical design with 4 levels (milliseconds, seconds, minutes,
 *     hours) to cover a range from 1 ms up to ~24 days.
 *   - Each level has a configurable number of slots (defaults: 256, 64,
 *     64, 64).
 *   - When a wheel level wraps around, timers cascade down to the next
 *     finer-grained level.
 *   - Timers are identified by a monotonically-increasing timer_id;
 *     cancellation is O(1) via a hash map index.
 *
 * Use Cases:
 *   - Raft election timeouts (randomized 150-300 ms)
 *   - Raft heartbeat intervals (50-100 ms)
 *   - Connection idle timeouts (5-60 seconds)
 *   - Session timeouts (consumer group, 10-60 seconds)
 *   - Producer request timeouts (configurable, typical 30 seconds)
 *   - Retry backoff scheduling
 *
 * Thread Safety:
 *   TimerWheel is NOT internally synchronized. The owning component must
 *   call add_timer()/cancel_timer()/tick() from a single thread (typically
 *   the I/O event loop). Timer callbacks are invoked synchronously during
 *   tick().
 *
 * References:
 *   - Varghese & Lauck, "Hashed and Hierarchical Timing Wheels" (1987)
 *   - Linux kernel timer wheel (kernel/time/timer.c)
 */

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace torrent::common {

// ============================================================================
// Constants
// ============================================================================

/// Default number of slots per wheel level.
constexpr size_t kDefaultMsSlots   = 256;   // 1 ms per slot, 0-255 ms range
constexpr size_t kDefaultSecSlots  = 64;    // 256 ms per slot (wraps from ms wheel)
constexpr size_t kDefaultMinSlots  = 64;    // ~16.4 sec per slot
constexpr size_t kDefaultHourSlots = 64;    // ~17.5 min per slot

/// Granularity of each wheel level in milliseconds.
constexpr int64_t kMsGranularity   = 1;
constexpr int64_t kSecGranularity  = 256;    // kDefaultMsSlots * kMsGranularity
constexpr int64_t kMinGranularity  = 16384;  // kDefaultSecSlots * kSecGranularity
constexpr int64_t kHourGranularity = 1048576; // ~17.5 min per slot

/// Sentinel value for invalid/empty timer IDs.
constexpr uint64_t kInvalidTimerId = 0;

// ============================================================================
// Timer Entry
// ============================================================================

/**
 * A single timer within the wheel. Stored in a doubly-linked list hanging
 * off each wheel slot. We use a simple intrusive list to avoid per-timer
 * heap allocations for list nodes.
 */
struct TimerEntry {
    uint64_t                     id;           // Unique timer identifier
    int64_t                      expires_ms;   // Absolute expiration time (ms from epoch)
    int64_t                      interval_ms;  // 0 = one-shot, >0 = repeating
    std::function<void()>        callback;     // Function to invoke on expiry
    TimerEntry*                  next = nullptr;
    TimerEntry*                  prev = nullptr;
    size_t                       wheel_level;  // Which wheel level this timer is in
    size_t                       slot_index;   // Which slot within the level

    TimerEntry() = default;

    TimerEntry(uint64_t tid, int64_t exp, int64_t interval,
               std::function<void()> cb)
        : id(tid), expires_ms(exp), interval_ms(interval),
          callback(std::move(cb)) {}
};

// ============================================================================
// Wheel Level
// ============================================================================

/**
 * A single level of the hierarchical timing wheel. Each slot holds a
 * circular doubly-linked list of TimerEntry objects.
 */
class WheelLevel {
public:
    WheelLevel(size_t num_slots, int64_t granularity_ms, int64_t range_ms)
        : slots_(num_slots)
        , granularity_ms_(granularity_ms)
        , range_ms_(range_ms)
        , num_slots_(num_slots)
        , current_slot_(0) {}

    /// Clear all slots (does not delete TimerEntries — caller owns memory).
    void clear() noexcept {
        for (auto& head : slots_) {
            head = nullptr;
        }
        current_slot_ = 0;
    }

    /// Insert a timer entry at the appropriate slot.
    void insert(TimerEntry* entry) {
        size_t slot = slot_for_expiry(entry->expires_ms);
        entry->wheel_level = 0;  // Will be set by caller
        entry->slot_index = slot;
        add_to_slot(slot, entry);
    }

    /// Advance the wheel by one tick. Returns list of expired entries (may
    /// be nullptr if none expired).
    TimerEntry* tick(int64_t now_ms) {
        TimerEntry* expired = nullptr;

        // Collect all entries in the current slot
        size_t slot = current_slot_;
        if (slots_[slot] != nullptr) {
            // Detach the entire list from this slot
            expired = slots_[slot];
            slots_[slot] = nullptr;

            // Fix up the prev pointer of the new head
            if (expired != nullptr) {
                // Walk to find the tail and break the circular link
                TimerEntry* tail = expired;
                while (tail->next != expired && tail->next != nullptr) {
                    tail = tail->next;
                }
                if (tail->next == expired) {
                    tail->next = nullptr;
                }
                if (expired->prev != nullptr) {
                    expired->prev = nullptr;
                }
            }
        }

        // Advance the slot pointer (circular)
        current_slot_ = (current_slot_ + 1) % num_slots_;

        return expired;
    }

    /// Compute the slot index for a given expiry time.
    [[nodiscard]] size_t slot_for_expiry(int64_t expires_ms) const noexcept {
        int64_t offset = (expires_ms / granularity_ms_) % num_slots_;
        return static_cast<size_t>(offset >= 0 ? offset : offset + num_slots_);
    }

    [[nodiscard]] size_t current_slot() const noexcept { return current_slot_; }
    [[nodiscard]] int64_t granularity_ms() const noexcept { return granularity_ms_; }
    [[nodiscard]] size_t num_slots() const noexcept { return num_slots_; }

private:
    void add_to_slot(size_t slot_idx, TimerEntry* entry) {
        TimerEntry*& head = slots_[slot_idx];

        entry->prev = nullptr;
        entry->next = nullptr;

        if (head == nullptr) {
            // First entry in this slot — circular list of one
            head = entry;
            entry->next = entry;
            entry->prev = entry;
        } else {
            // Insert at the end of the circular list
            TimerEntry* tail = head->prev;
            entry->next = head;
            entry->prev = tail;
            tail->next = entry;
            head->prev = entry;
        }
    }

    std::vector<TimerEntry*> slots_;
    int64_t granularity_ms_;
    int64_t range_ms_;
    size_t num_slots_;
    size_t current_slot_;
};

// ============================================================================
// TimerWheel — Public Interface
// ============================================================================

struct TimerWheel::Impl {
    // --- Wheel levels ---
    // Level 0: ms wheel — 1 ms/slot, 0..255 ms (range: 256 ms)
    // Level 1: sec wheel — 256 ms/slot, 0..~16 sec (range: 16.384 sec)
    // Level 2: min wheel — ~16.4 sec/slot, 0..~17 min (range: ~1048 sec)
    // Level 3: hour wheel — ~17.5 min/slot, 0..~18.6 hours
    WheelLevel ms_wheel;
    WheelLevel sec_wheel;
    WheelLevel min_wheel;
    WheelLevel hour_wheel;

    // --- Timer registry ---
    std::unordered_map<uint64_t, std::unique_ptr<TimerEntry>> registry;

    // --- State ---
    uint64_t next_id = 1;        // Monotonically increasing timer ID
    int64_t  last_tick_ms = 0;   // Last wall-clock time tick() was called
    int64_t  current_time_ms = 0; // Current virtual time of the wheel

    // --- Logger ---
    std::shared_ptr<spdlog::logger> logger;

    Impl()
        : ms_wheel(kDefaultMsSlots, kMsGranularity, kDefaultMsSlots * kMsGranularity)
        , sec_wheel(kDefaultSecSlots, kSecGranularity, kDefaultSecSlots * kSecGranularity)
        , min_wheel(kDefaultMinSlots, kMinGranularity, kDefaultMinSlots * kMinGranularity)
        , hour_wheel(kDefaultHourSlots, kHourGranularity, kDefaultHourSlots * kHourGranularity) {
        logger = spdlog::get("timer_wheel");
        if (!logger) {
            logger = spdlog::stdout_color_mt("timer_wheel");
            logger->set_level(spdlog::level::info);
        }
    }

    /// Choose the appropriate wheel level for a delay.
    [[nodiscard]] WheelLevel* wheel_for_delay(int64_t delay_ms) noexcept {
        if (delay_ms < kDefaultMsSlots * kMsGranularity) {
            return &ms_wheel;
        }
        if (delay_ms < kDefaultSecSlots * kSecGranularity) {
            return &sec_wheel;
        }
        if (delay_ms < kDefaultMinSlots * kMinGranularity) {
            return &min_wheel;
        }
        return &hour_wheel;
    }
};

// ============================================================================
// Construction / Destruction
// ============================================================================

TimerWheel::TimerWheel()
    : impl_(std::make_unique<Impl>()) {}

TimerWheel::~TimerWheel() = default;

// ============================================================================
// Timer Management
// ============================================================================

uint64_t TimerWheel::add_timer(int64_t delay_ms, std::function<void()> callback) {
    return add_timer(delay_ms, 0, std::move(callback));
}

uint64_t TimerWheel::add_timer(int64_t delay_ms, int64_t interval_ms,
                                std::function<void()> callback) {
    if (delay_ms <= 0) {
        impl_->logger->warn("add_timer: delay_ms={} must be positive, clamping to 1",
                            delay_ms);
        delay_ms = 1;
    }
    if (!callback) {
        impl_->logger->error("add_timer: null callback provided");
        return kInvalidTimerId;
    }

    uint64_t id = impl_->next_id++;
    int64_t expires_ms = impl_->current_time_ms + delay_ms;

    auto entry = std::make_unique<TimerEntry>(id, expires_ms, interval_ms,
                                               std::move(callback));

    WheelLevel* wheel = impl_->wheel_for_delay(delay_ms);
    entry->wheel_level = 0; // We'll fix this: 0=ms, 1=sec, 2=min, 3=hour
    if (wheel == &impl_->ms_wheel)   entry->wheel_level = 0;
    else if (wheel == &impl_->sec_wheel)  entry->wheel_level = 1;
    else if (wheel == &impl_->min_wheel)  entry->wheel_level = 2;
    else                                 entry->wheel_level = 3;

    wheel->insert(entry.get());
    entry->slot_index = wheel->slot_for_expiry(expires_ms);

    impl_->registry[id] = std::move(entry);

    impl_->logger->trace("add_timer: id={} delay={}ms expires={} level={} slot={}",
                         id, delay_ms, expires_ms,
                         impl_->registry[id]->wheel_level,
                         impl_->registry[id]->slot_index);

    return id;
}

bool TimerWheel::cancel_timer(uint64_t timer_id) {
    if (timer_id == kInvalidTimerId) return false;

    auto it = impl_->registry.find(timer_id);
    if (it == impl_->registry.end()) {
        impl_->logger->trace("cancel_timer: id={} not found", timer_id);
        return false;
    }

    TimerEntry* entry = it->second.get();

    // Remove from the circular doubly-linked list in whichever slot it
    // currently resides. We need to find which wheel level and slot.
    WheelLevel* wheel = nullptr;
    switch (entry->wheel_level) {
    case 0: wheel = &impl_->ms_wheel; break;
    case 1: wheel = &impl_->sec_wheel; break;
    case 2: wheel = &impl_->min_wheel; break;
    case 3: wheel = &impl_->hour_wheel; break;
    default:
        impl_->logger->error("cancel_timer: id={} has invalid wheel_level={}",
                             timer_id, entry->wheel_level);
        impl_->registry.erase(it);
        return false;
    }

    // The entry is in a circular doubly-linked list. We need to remove it.
    // Since we don't have direct access to the slot's head pointer from
    // the WheelLevel, we use the entry's own prev/next pointers.

    if (entry->next == entry) {
        // Only entry in the list — we can't reset the slot head directly
        // from here. We need to walk all slots to find it... or just
        // mark it as cancelled and let the tick() call skip it.
        // Simpler: mark as cancelled by nulling the callback.
        entry->callback = nullptr;
        impl_->logger->trace("cancel_timer: id={} marked as cancelled (sole entry in slot)",
                             timer_id);
    } else if (entry->next != nullptr && entry->prev != nullptr) {
        // Remove from the list
        entry->prev->next = entry->next;
        entry->next->prev = entry->prev;
        entry->next = nullptr;
        entry->prev = nullptr;
    }

    impl_->registry.erase(it);
    impl_->logger->trace("cancel_timer: id={} cancelled", timer_id);
    return true;
}

// ============================================================================
// Time Advancement
// ============================================================================

size_t TimerWheel::tick() {
    return tick(0);
}

size_t TimerWheel::tick(int64_t now_ms) {
    // If now_ms is 0, use real wall-clock time
    if (now_ms == 0) {
        auto now = std::chrono::steady_clock::now();
        now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();
    }

    // Determine how many ticks to advance
    if (impl_->last_tick_ms == 0) {
        // First call — just set the time, don't fire anything
        impl_->last_tick_ms = now_ms;
        impl_->current_time_ms = now_ms;
        impl_->logger->debug("tick: initial time set to {}ms", now_ms);
        return 0;
    }

    int64_t elapsed = now_ms - impl_->last_tick_ms;
    if (elapsed <= 0) {
        // No time has passed (or clock went backwards — handle gracefully)
        return 0;
    }

    impl_->logger->trace("tick: advancing by {}ms ({} to {})",
                         elapsed, impl_->last_tick_ms, now_ms);

    size_t fired_count = 0;

    // Advance one millisecond at a time. For large gaps this could be
    // optimized by cascading entire slots, but for correctness and
    // simplicity we tick per-ms. In production, tick() should be called
    // frequently (every 1-10 ms from the event loop).
    for (int64_t t = impl_->last_tick_ms; t < now_ms; ++t) {
        impl_->current_time_ms = t;

        // Cascade: when a coarser wheel wraps, move its entries down
        cascade_if_needed();

        // Tick the ms wheel and fire expired entries
        TimerEntry* expired = impl_->ms_wheel.tick(t);
        fired_count += fire_expired(expired, t);
    }

    impl_->last_tick_ms = now_ms;
    impl_->current_time_ms = now_ms;

    return fired_count;
}

// ============================================================================
// Internal: Cascade
// ============================================================================

void TimerWheel::cascade_if_needed() {
    // When the ms wheel wraps (slot 0 is about to be ticked),
    // cascade one slot from sec wheel into ms wheel.
    if (impl_->ms_wheel.current_slot() == 0) {
        TimerEntry* from_sec = impl_->sec_wheel.tick(impl_->current_time_ms);
        cascade_entries(from_sec, &impl_->ms_wheel);

        // When sec wheel wraps, cascade from min wheel
        if (impl_->sec_wheel.current_slot() == 0) {
            TimerEntry* from_min = impl_->min_wheel.tick(impl_->current_time_ms);
            cascade_entries(from_min, &impl_->sec_wheel);

            // When min wheel wraps, cascade from hour wheel
            if (impl_->min_wheel.current_slot() == 0) {
                TimerEntry* from_hour = impl_->hour_wheel.tick(impl_->current_time_ms);
                cascade_entries(from_hour, &impl_->min_wheel);
            }
        }
    }
}

void TimerWheel::cascade_entries(TimerEntry* head, WheelLevel* target) {
    if (head == nullptr) return;

    // Walk the linked list and re-insert each entry into the target wheel
    TimerEntry* current = head;
    TimerEntry* first = head;
    do {
        TimerEntry* next = current->next;
        current->next = nullptr;
        current->prev = nullptr;

        // Re-insert into target wheel
        target->insert(current);
        current->slot_index = target->slot_for_expiry(current->expires_ms);

        // Determine new wheel_level
        if (target == &impl_->ms_wheel)   current->wheel_level = 0;
        else if (target == &impl_->sec_wheel)  current->wheel_level = 1;
        else if (target == &impl_->min_wheel)  current->wheel_level = 2;
        else                                 current->wheel_level = 3;

        current = next;
    } while (current != nullptr && current != first);
}

// ============================================================================
// Internal: Fire Expired Callbacks
// ============================================================================

size_t TimerWheel::fire_expired(TimerEntry* head, int64_t now_ms) {
    if (head == nullptr) return 0;

    size_t count = 0;
    TimerEntry* current = head;

    while (current != nullptr) {
        TimerEntry* next = current->next;
        current->next = nullptr;
        current->prev = nullptr;

        // Only fire if the entry hasn't been cancelled (callback nulled)
        if (current->callback) {
            impl_->logger->trace("fire_expired: id={} at {}ms",
                                 current->id, now_ms);
            try {
                current->callback();
                ++count;
            } catch (const std::exception& e) {
                impl_->logger->error("timer callback id={} threw: {}",
                                     current->id, e.what());
            } catch (...) {
                impl_->logger->error("timer callback id={} threw unknown exception",
                                     current->id);
            }
        }

        // Handle repeating timer
        if (current->interval_ms > 0 && current->callback) {
            // Re-schedule the repeating timer
            current->expires_ms = now_ms + current->interval_ms;
            WheelLevel* wheel = impl_->wheel_for_delay(current->interval_ms);

            if (wheel == &impl_->ms_wheel)   current->wheel_level = 0;
            else if (wheel == &impl_->sec_wheel)  current->wheel_level = 1;
            else if (wheel == &impl_->min_wheel)  current->wheel_level = 2;
            else                                 current->wheel_level = 3;

            wheel->insert(current);
            current->slot_index = wheel->slot_for_expiry(current->expires_ms);
        } else {
            // One-shot or cancelled: remove from registry
            auto it = impl_->registry.find(current->id);
            if (it != impl_->registry.end()) {
                impl_->registry.erase(it);
            }
        }

        current = next;
    }

    return count;
}

// ============================================================================
// Query Methods
// ============================================================================

size_t TimerWheel::pending_count() const noexcept {
    return impl_->registry.size();
}

bool TimerWheel::has_pending() const noexcept {
    return !impl_->registry.empty();
}

int64_t TimerWheel::current_time_ms() const noexcept {
    return impl_->current_time_ms;
}

// ============================================================================
// High-Resolution Timer (Monotonic Clock)
// ============================================================================

int64_t get_monotonic_us() noexcept {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count();
}

int64_t get_monotonic_ms() noexcept {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
}

int64_t get_wall_clock_ms() noexcept {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
}

} // namespace torrent::common
