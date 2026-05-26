/**
 * torrent-mq — Thread pool implementation
 *
 * Fixed-size thread pool with three priority lanes (high/normal/low),
 * graceful and immediate shutdown, exception-safe worker loop,
 * and per-thread naming for debugging.
 *
 * Thread safety: all public methods use a shared mutex. Workers
 * block on a condition variable when idle. Shutdown drains or
 * discards pending work then joins all threads.
 */

#include "torrent/common/thread_pool.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <thread>
#include <utility>

#ifdef __linux__
#include <pthread.h>
#endif

namespace torrent {

// ============================================================================
// Private helpers (anonymous namespace)
// ============================================================================

namespace {

/**
 * Check whether all three priority queues are empty.
 * Must be called while holding mutex_.
 */
[[nodiscard]] bool all_queues_empty(
    const std::queue<thread_pool::task_t> (&queues)[3]) noexcept {
    return queues[0].empty() && queues[1].empty() && queues[2].empty();
}

/**
 * Dequeue the highest-priority task available.
 * Checks high first, then normal, then low.
 * Must be called while holding mutex_ and after verifying at least
 * one queue is non-empty.
 */
thread_pool::task_t dequeue_highest_priority(
    std::queue<thread_pool::task_t> (&queues)[3]) {
    // Check from highest to lowest priority
    for (int i = static_cast<int>(thread_pool::priority::high);
         i >= static_cast<int>(thread_pool::priority::low); --i) {
        if (!queues[i].empty()) {
            auto task = std::move(queues[i].front());
            queues[i].pop();
            return task;
        }
    }
    // Should never reach here if queues are known to be non-empty
    assert(false && "dequeue_highest_priority called with all queues empty");
    return {};
}

/**
 * Compute the total number of tasks across all three priority queues.
 * Must be called while holding mutex_.
 */
[[nodiscard]] size_t total_pending(
    const std::queue<thread_pool::task_t> (&queues)[3]) noexcept {
    return queues[0].size() + queues[1].size() + queues[2].size();
}

/**
 * Clear all three priority queues, discarding all pending tasks.
 * Must be called while holding mutex_.
 */
void clear_all_queues(std::queue<thread_pool::task_t> (&queues)[3]) noexcept {
    for (auto& q : queues) {
        while (!q.empty()) {
            q.pop();
        }
    }
}

/**
 * Set the name of the calling thread for debugging purposes.
 * On Linux uses pthread_setname_np with truncation to 15 chars.
 * On other platforms this is a no-op.
 */
void set_thread_name(const char* name) noexcept {
#ifdef __linux__
    // Linux limits thread names to 15 characters plus null terminator
    char truncated[16];
    std::strncpy(truncated, name, sizeof(truncated) - 1);
    truncated[sizeof(truncated) - 1] = '\0';
    pthread_setname_np(pthread_self(), truncated);
#else
    (void)name;
#endif
}

} // anonymous namespace

// ============================================================================
// Construction / Destruction
// ============================================================================

thread_pool::thread_pool(size_t num_threads, const std::string& name_prefix)
    : name_prefix_(name_prefix) {
    // Default to hardware concurrency if zero or overly large
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
    }
    if (num_threads == 0) {
        // Fallback if hardware_concurrency returns 0 (unlikely but possible)
        num_threads = 4;
    }

    // Guard against unreasonable values
    constexpr size_t kMaxThreads = 1024;
    if (num_threads > kMaxThreads) {
        spdlog::warn("thread_pool: requested {} threads exceeds maximum {}, clamping",
                     num_threads, kMaxThreads);
        num_threads = kMaxThreads;
    }

    spdlog::info("thread_pool: creating pool with {} worker threads (prefix: '{}')",
                 num_threads, name_prefix_);

    threads_.reserve(num_threads);

    // Spawn worker threads. Each thread runs worker_loop with its index.
    // Threads start immediately and block on the condition variable until
    // tasks are submitted or shutdown is initiated.
    for (size_t i = 0; i < num_threads; ++i) {
        try {
            threads_.emplace_back(&thread_pool::worker_loop, this, i);

            // Name the thread for debugging.
            // Format: "name_prefix-N" where N is the thread index.
            char thread_name[128];
            std::snprintf(thread_name, sizeof(thread_name), "%s-%zu",
                          name_prefix_.c_str(), i);
            set_thread_name(thread_name);
        } catch (const std::system_error& e) {
            spdlog::error("thread_pool: failed to spawn worker thread {}: {}",
                          i, e.what());
            // If we couldn't spawn a thread, shut down those we've already
            // created and rethrow.
            shutdown_ = true;
            drain_ = false;
            cv_.notify_all();
            for (auto& t : threads_) {
                if (t.joinable()) {
                    t.join();
                }
            }
            throw;
        }
    }

    // Update idle count now that all threads are waiting for work
    idle_count_ = num_threads;
}

thread_pool::~thread_pool() {
    // Attempt graceful shutdown. If an exception is already in flight
    // (stack unwinding), we don't want to throw from the destructor,
    // so we catch and log.
    try {
        shutdown();
    } catch (const std::exception& e) {
        spdlog::error("thread_pool: exception in destructor during shutdown: {}", e.what());
    } catch (...) {
        spdlog::error("thread_pool: unknown exception in destructor during shutdown");
    }
}

// ============================================================================
// Task submission
// ============================================================================

void thread_pool::submit(task_t fn, priority prio) {
    if (!fn) {
        spdlog::warn("thread_pool::submit: null task submitted, ignoring");
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);

        // Reject submissions after shutdown has been initiated.
        // This prevents tasks from being enqueued into a pool that is
        // in the process of stopping, which would cause the thread to
        // potentially block forever in join() during shutdown().
        if (shutdown_.load(std::memory_order_acquire)) {
            spdlog::warn("thread_pool::submit: rejecting task, pool is shutting down");
            return;
        }

        auto idx = static_cast<size_t>(prio);
        queues_[idx].emplace(std::move(fn));
    }

    // Notify exactly one worker that a new task is available.
    // We notify outside the lock to avoid the "hurry up and wait"
    // situation where the woken thread immediately blocks on the mutex.
    cv_.notify_one();
}

void thread_pool::submit_batch(std::vector<task_t> tasks, priority prio) {
    if (tasks.empty()) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (shutdown_.load(std::memory_order_acquire)) {
            spdlog::warn("thread_pool::submit_batch: rejecting {} tasks, pool is shutting down",
                         tasks.size());
            return;
        }

        auto idx = static_cast<size_t>(prio);
        auto& queue = queues_[idx];

        for (auto& task : tasks) {
            if (task) {
                queue.emplace(std::move(task));
            }
        }
    }

    // Wake workers. For large batches, we could notify more than one
    // worker. A conservative approach is to notify up to the batch size
    // (bounded by thread count), so idle threads can start processing
    // in parallel without unnecessary wake-ups.
    size_t wake_count = std::min(tasks.size(), threads_.size());
    for (size_t i = 0; i < wake_count; ++i) {
        cv_.notify_one();
    }
}

// ============================================================================
// Queue inspection
// ============================================================================

bool thread_pool::has_pending() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return !all_queues_empty(queues_);
}

size_t thread_pool::pending_count() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return total_pending(queues_);
}

// ============================================================================
// Worker loop
// ============================================================================

void thread_pool::worker_loop(size_t thread_index) {
    spdlog::debug("thread_pool: worker {} (thread {}) started",
                  thread_index,
                  std::hash<std::thread::id>{}(std::this_thread::get_id()));

    while (true) {
        task_t task;

        // --- Wait for work or shutdown signal ---
        {
            std::unique_lock<std::mutex> lock(mutex_);

            // Increment idle counter while we wait.
            // Decrement it again once we acquire a task (or exit).
            ++idle_count_;

            cv_.wait(lock, [this] {
                // Wake up if there is work to do or if shutdown was requested.
                return !all_queues_empty(queues_) || shutdown_.load(std::memory_order_acquire);
            });

            --idle_count_;

            // --- Check exit conditions ---
            //
            // We exit the worker loop in two scenarios:
            // 1. shutdown_now() was called: shutdown_ is true, drain_ is false.
            //    We discard any remaining tasks and exit immediately.
            // 2. shutdown() was called and all queues are drained:
            //    shutdown_ is true, drain_ is true, and queues are empty.
            if (shutdown_.load(std::memory_order_acquire)) {
                if (!drain_.load(std::memory_order_acquire) || all_queues_empty(queues_)) {
                    spdlog::debug("thread_pool: worker {} exiting ({} mode, {} pending tasks)",
                                  thread_index,
                                  drain_.load() ? "drain" : "immediate",
                                  total_pending(queues_));
                    return;
                }
            }

            // At this point we are guaranteed to have at least one task
            // in one of the queues, because:
            //   - If shutdown_ is false, the cv predicate ensures queues are non-empty.
            //   - If shutdown_ is true and drain_ is true, we only reach here if
            //     queues are non-empty (otherwise we would have returned above).
            task = dequeue_highest_priority(queues_);
        }

        // --- Execute the task with full exception safety ---
        //
        // Any exception thrown by a task must not escape the worker loop,
        // as that would terminate the thread and reduce pool capacity.
        // We catch and log all exceptions here.
        if (task) {
            try {
                task();
            } catch (const std::exception& e) {
                spdlog::error("thread_pool: worker {} caught std::exception in task: {}",
                              thread_index, e.what());
            } catch (...) {
                spdlog::error("thread_pool: worker {} caught unknown exception in task",
                              thread_index);
            }

            // Increment the execution counter regardless of whether the
            // task completed successfully or threw an exception.
            tasks_executed_.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

// ============================================================================
// Shutdown
// ============================================================================

void thread_pool::shutdown() {
    // Idempotent: if already shut down, just return.
    if (shutdown_.load(std::memory_order_acquire)) {
        return;
    }

    spdlog::info("thread_pool: initiating graceful shutdown, {} pending tasks",
                 pending_count());

    // Set the drain flag so workers know to finish remaining tasks
    // before exiting. Set shutdown so they stop waiting for new work
    // once the queues are empty.
    drain_.store(true, std::memory_order_release);
    shutdown_.store(true, std::memory_order_release);

    // Wake all workers. Each will drain remaining tasks, then exit
    // when it sees shutdown_=true, drain_=true, and empty queues.
    cv_.notify_all();

    // Join all threads. Each thread will finish its current task
    // and any remaining tasks in the queues.
    for (size_t i = 0; i < threads_.size(); ++i) {
        if (threads_[i].joinable()) {
            spdlog::debug("thread_pool: joining worker thread {}", i);
            threads_[i].join();
        }
    }

    spdlog::info("thread_pool: graceful shutdown complete, {} total tasks executed",
                 tasks_executed_.load());

    // Sanity check: all queues should be empty after drain.
    // Log a warning if any tasks were abandoned (shouldn't happen).
    size_t remaining = pending_count();
    if (remaining > 0) {
        spdlog::warn("thread_pool: {} tasks remaining after drain (unexpected)", remaining);
    }
}

void thread_pool::shutdown_now() {
    // Idempotent: if already shut down, just return.
    if (shutdown_.load(std::memory_order_acquire)) {
        return;
    }

    spdlog::info("thread_pool: initiating immediate shutdown, discarding {} pending tasks",
                 pending_count());

    // Discard all pending tasks. drain_ remains false so workers know
    // to exit as soon as they check the shutdown flag, even if queues
    // still had tasks (which they won't, since we clear them below).
    {
        std::lock_guard<std::mutex> lock(mutex_);
        clear_all_queues(queues_);
    }

    shutdown_.store(true, std::memory_order_release);

    // Wake all workers. They will see shutdown_=true and drain_=false,
    // so they exit immediately without processing any tasks.
    cv_.notify_all();

    // Join all threads.
    for (size_t i = 0; i < threads_.size(); ++i) {
        if (threads_[i].joinable()) {
            spdlog::debug("thread_pool: joining worker thread {} (immediate)", i);
            threads_[i].join();
        }
    }

    spdlog::info("thread_pool: immediate shutdown complete, {} total tasks executed",
                 tasks_executed_.load());
}

} // namespace torrent
