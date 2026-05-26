#pragma once

#include <cstdint>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <atomic>
#include <future>
#include <memory>
#include <string>

namespace torrent {

/**
 * A fixed-size thread pool for CPU-bound work.
 *
 * Threads are pre-spawned at construction and run until shutdown().
 * Tasks are enqueued and picked up by idle threads with FIFO ordering.
 * Supports priority lanes (high/normal/low) and named threads for debugging.
 *
 * Thread safety: all public methods are safe to call from any thread.
 */
class thread_pool {
public:
    /**
     * Task priority levels.
     * High: latency-critical operations (client responses, heartbeats)
     * Normal: standard processing (request handling, log appends)
     * Low: background work (compaction, cleanup, metrics aggregation)
     */
    enum class priority : uint8_t {
        low = 0,
        normal = 1,
        high = 2,
        _count = 3
    };

    /**
     * A single task submitted to the pool.
     */
    using task_t = std::function<void()>;

    /**
     * Construct a thread pool with the given number of threads.
     *
     * @param num_threads Number of worker threads (default: hardware concurrency)
     * @param name_prefix Thread name prefix for debugging (default: "torrent-worker")
     */
    explicit thread_pool(size_t num_threads = 0,
                         const std::string& name_prefix = "torrent-worker");

    /**
     * Destructor calls shutdown() and joins all threads.
     */
    ~thread_pool();

    // Non-copyable
    thread_pool(const thread_pool&) = delete;
    thread_pool& operator=(const thread_pool&) = delete;

    // Movable
    thread_pool(thread_pool&&) noexcept = default;
    thread_pool& operator=(thread_pool&&) noexcept = default;

    /**
     * Submit a task to the pool.
     * Returns immediately; the task executes when a thread becomes available.
     *
     * @param fn The task to execute
     * @param prio Priority level (default: normal)
     */
    void submit(task_t fn, priority prio = priority::normal);

    /**
     * Submit a task and get a future for its result.
     *
     * @param fn The task returning T
     * @param prio Priority level
     * @return std::future<T> that resolves when the task completes
     */
    template<typename F>
    auto submit_future(F&& fn, priority prio = priority::normal)
        -> std::future<decltype(fn())> {
        using result_type = decltype(fn());
        auto promise = std::make_shared<std::promise<result_type>>();
        auto future = promise->get_future();

        submit([p = std::move(promise), f = std::forward<F>(fn)]() mutable {
            try {
                p->set_value(f());
            } catch (...) {
                p->set_exception(std::current_exception());
            }
        }, prio);

        return future;
    }

    /**
     * Submit a batch of tasks. More efficient than N individual submit() calls
     * because it acquires the mutex once.
     */
    void submit_batch(std::vector<task_t> tasks, priority prio = priority::normal);

    /**
     * Check if there are pending tasks in the queue.
     */
    [[nodiscard]] bool has_pending() const noexcept;

    /**
     * Get the number of pending tasks across all priority lanes.
     */
    [[nodiscard]] size_t pending_count() const noexcept;

    /**
     * Get the number of worker threads.
     */
    [[nodiscard]] size_t thread_count() const noexcept { return threads_.size(); }

    /**
     * Get the number of idle threads (waiting for work).
     */
    [[nodiscard]] size_t idle_count() const noexcept { return idle_count_.load(); }

    /**
     * Get the total number of tasks executed since creation.
     */
    [[nodiscard]] uint64_t total_tasks_executed() const noexcept {
        return tasks_executed_.load();
    }

    /**
     * Graceful shutdown: drain remaining tasks, then stop all threads.
     * Blocks until all threads have exited.
     */
    void shutdown();

    /**
     * Immediate shutdown: discard pending tasks, stop threads.
     * Tasks currently executing are allowed to finish.
     * Blocks until all threads have exited.
     */
    void shutdown_now();

    /**
     * Check if the pool has been shut down.
     */
    [[nodiscard]] bool is_shutdown() const noexcept { return shutdown_.load(); }

private:
    /**
     * Worker thread loop.
     * Waits for tasks on the condition variable, executes them, repeats.
     * Exits when shutdown_ is true and the queue is empty.
     */
    void worker_loop(size_t thread_index);

    std::vector<std::thread> threads_;
    std::queue<task_t> queues_[static_cast<size_t>(priority::_count)];
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> shutdown_{false};
    std::atomic<bool> drain_{false};
    std::atomic<size_t> idle_count_{0};
    std::atomic<uint64_t> tasks_executed_{0};
    std::string name_prefix_;
};

} // namespace torrent
