#pragma once
#include <cstdint>
#include <atomic>
#include <torrent/common/types.h>

namespace torrent::network {
class Throttle {
public:
    explicit Throttle(int64_t rate_bps = 0, int64_t burst_bytes = 65536);
    bool allow(int64_t bytes) noexcept;
    void set_rate(int64_t rate_bps) noexcept;
    int64_t rate_bps() const noexcept;
private:
    int64_t rate_bps_;
    int64_t burst_bytes_;
    std::atomic<int64_t> tokens_{0};
};
}