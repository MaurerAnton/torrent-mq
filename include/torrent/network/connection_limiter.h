#pragma once
#include <cstdint>
#include <atomic>
#include <torrent/common/types.h>

namespace torrent::network {
class ConnectionLimiter {
public:
    explicit ConnectionLimiter(int64_t max_connections = 65536);
    bool try_acquire() noexcept;
    void release() noexcept;
    int64_t current_count() const noexcept;
    int64_t max_connections() const noexcept;
private:
    std::atomic<int64_t> count_{0};
    std::atomic<int64_t> max_{0};
};
}