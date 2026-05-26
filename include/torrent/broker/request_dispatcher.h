#pragma once
#include <memory>
#include <functional>
#include <future>
#include <string>
#include <vector>
#include <cstdint>
#include <torrent/common/types.h>

namespace torrent::broker {

class RequestDispatcher {
public:
    explicit RequestDispatcher(class BrokerServer& s);
    ~RequestDispatcher();

    RequestDispatcher(const RequestDispatcher&) = delete;
    RequestDispatcher& operator=(const RequestDispatcher&) = delete;
    RequestDispatcher(RequestDispatcher&&) = delete;
    RequestDispatcher& operator=(RequestDispatcher&&) = delete;

    // ---- Lifecycle ----

    void start();
    void shutdown();

    // ---- Handler registration ----

    /// Handler signature: takes api_key and raw request buffer, returns response.
    using handler_fn = std::function<shared_buffer(int16_t api_key, buffer_view request)>;

    /// Register a handler for the given API key.
    void register_handler(int16_t api_key, handler_fn handler);

    // ---- Request dispatch ----

    /// Synchronous dispatch: blocks until handler returns.
    /// Used for simple handlers or testing.
    shared_buffer dispatch(int16_t api_key, buffer_view request);

    /// Asynchronous dispatch: returns a future that resolves when the handler
    /// completes.  Uses the thread pool for parallel execution.  Enforces
    /// backpressure, version negotiation, and rate limiting.
    /// @param client_id_hash Hashed client identity for per-client rate limiting (0 = anonymous).
    [[nodiscard]] std::future<shared_buffer> async_dispatch(
        int16_t api_key,
        int16_t api_version,
        buffer_view request,
        int32_t client_id_hash = 0);

    // ---- Metrics ----

    struct Metrics {
        size_t registered_handlers = 0;
        size_t pending_requests = 0;
        size_t in_flight_requests = 0;
        size_t idle_threads = 0;
        size_t total_threads = 0;
        uint64_t total_tasks_executed = 0;
        uint64_t total_requests = 0;
        uint64_t total_errors = 0;
        int64_t avg_latency_us = 0;
        double global_tokens_available = 0.0;
    };

    /// Snapshot of current dispatcher metrics.
    [[nodiscard]] Metrics get_metrics() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    BrokerServer* server_;

    // Background threads
    void timeout_loop();
    void refill_loop();

    // Internal helpers
    void remove_in_flight(int32_t correlation_id);
    void complete_with_error(
        std::shared_ptr<struct RequestContext> ctx,
        shared_buffer response);
};

} // namespace torrent::broker
