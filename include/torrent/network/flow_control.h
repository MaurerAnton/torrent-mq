#pragma once
#include <cstdint>
#include <atomic>
#include <torrent/common/types.h>

namespace torrent::network {
class FlowControl {
public:
    explicit FlowControl(int64_t initial_credit = 65536);
    int64_t available_credit() const noexcept;
    bool consume(int64_t bytes) noexcept;
    void replenish(int64_t bytes) noexcept;
    void set_max_credit(int64_t max_credit) noexcept;
private:
    std::atomic<int64_t> credit_{0};
    std::atomic<int64_t> max_credit_{0};
};
}