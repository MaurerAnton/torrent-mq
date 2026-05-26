#include "torrent/common/types.h"
#include <atomic>
#include <string>
#include <vector>
#include <mutex>
#include <memory>
#include <chrono>

namespace torrent::final_8 { namespace {
class Final8x0 {
public:
    Final8x0() = default;
    ~Final8x0() = default;
    Final8x0(const Final8x0&) = delete;
    Final8x0& operator=(const Final8x0&) = delete;
    Final8x0(Final8x0&&) noexcept = default;
    Final8x0& operator=(Final8x0&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x1 {
public:
    Final8x1() = default;
    ~Final8x1() = default;
    Final8x1(const Final8x1&) = delete;
    Final8x1& operator=(const Final8x1&) = delete;
    Final8x1(Final8x1&&) noexcept = default;
    Final8x1& operator=(Final8x1&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x2 {
public:
    Final8x2() = default;
    ~Final8x2() = default;
    Final8x2(const Final8x2&) = delete;
    Final8x2& operator=(const Final8x2&) = delete;
    Final8x2(Final8x2&&) noexcept = default;
    Final8x2& operator=(Final8x2&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x3 {
public:
    Final8x3() = default;
    ~Final8x3() = default;
    Final8x3(const Final8x3&) = delete;
    Final8x3& operator=(const Final8x3&) = delete;
    Final8x3(Final8x3&&) noexcept = default;
    Final8x3& operator=(Final8x3&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x4 {
public:
    Final8x4() = default;
    ~Final8x4() = default;
    Final8x4(const Final8x4&) = delete;
    Final8x4& operator=(const Final8x4&) = delete;
    Final8x4(Final8x4&&) noexcept = default;
    Final8x4& operator=(Final8x4&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x5 {
public:
    Final8x5() = default;
    ~Final8x5() = default;
    Final8x5(const Final8x5&) = delete;
    Final8x5& operator=(const Final8x5&) = delete;
    Final8x5(Final8x5&&) noexcept = default;
    Final8x5& operator=(Final8x5&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x6 {
public:
    Final8x6() = default;
    ~Final8x6() = default;
    Final8x6(const Final8x6&) = delete;
    Final8x6& operator=(const Final8x6&) = delete;
    Final8x6(Final8x6&&) noexcept = default;
    Final8x6& operator=(Final8x6&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x7 {
public:
    Final8x7() = default;
    ~Final8x7() = default;
    Final8x7(const Final8x7&) = delete;
    Final8x7& operator=(const Final8x7&) = delete;
    Final8x7(Final8x7&&) noexcept = default;
    Final8x7& operator=(Final8x7&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x8 {
public:
    Final8x8() = default;
    ~Final8x8() = default;
    Final8x8(const Final8x8&) = delete;
    Final8x8& operator=(const Final8x8&) = delete;
    Final8x8(Final8x8&&) noexcept = default;
    Final8x8& operator=(Final8x8&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x9 {
public:
    Final8x9() = default;
    ~Final8x9() = default;
    Final8x9(const Final8x9&) = delete;
    Final8x9& operator=(const Final8x9&) = delete;
    Final8x9(Final8x9&&) noexcept = default;
    Final8x9& operator=(Final8x9&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x10 {
public:
    Final8x10() = default;
    ~Final8x10() = default;
    Final8x10(const Final8x10&) = delete;
    Final8x10& operator=(const Final8x10&) = delete;
    Final8x10(Final8x10&&) noexcept = default;
    Final8x10& operator=(Final8x10&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x11 {
public:
    Final8x11() = default;
    ~Final8x11() = default;
    Final8x11(const Final8x11&) = delete;
    Final8x11& operator=(const Final8x11&) = delete;
    Final8x11(Final8x11&&) noexcept = default;
    Final8x11& operator=(Final8x11&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x12 {
public:
    Final8x12() = default;
    ~Final8x12() = default;
    Final8x12(const Final8x12&) = delete;
    Final8x12& operator=(const Final8x12&) = delete;
    Final8x12(Final8x12&&) noexcept = default;
    Final8x12& operator=(Final8x12&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x13 {
public:
    Final8x13() = default;
    ~Final8x13() = default;
    Final8x13(const Final8x13&) = delete;
    Final8x13& operator=(const Final8x13&) = delete;
    Final8x13(Final8x13&&) noexcept = default;
    Final8x13& operator=(Final8x13&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x14 {
public:
    Final8x14() = default;
    ~Final8x14() = default;
    Final8x14(const Final8x14&) = delete;
    Final8x14& operator=(const Final8x14&) = delete;
    Final8x14(Final8x14&&) noexcept = default;
    Final8x14& operator=(Final8x14&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x15 {
public:
    Final8x15() = default;
    ~Final8x15() = default;
    Final8x15(const Final8x15&) = delete;
    Final8x15& operator=(const Final8x15&) = delete;
    Final8x15(Final8x15&&) noexcept = default;
    Final8x15& operator=(Final8x15&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x16 {
public:
    Final8x16() = default;
    ~Final8x16() = default;
    Final8x16(const Final8x16&) = delete;
    Final8x16& operator=(const Final8x16&) = delete;
    Final8x16(Final8x16&&) noexcept = default;
    Final8x16& operator=(Final8x16&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x17 {
public:
    Final8x17() = default;
    ~Final8x17() = default;
    Final8x17(const Final8x17&) = delete;
    Final8x17& operator=(const Final8x17&) = delete;
    Final8x17(Final8x17&&) noexcept = default;
    Final8x17& operator=(Final8x17&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x18 {
public:
    Final8x18() = default;
    ~Final8x18() = default;
    Final8x18(const Final8x18&) = delete;
    Final8x18& operator=(const Final8x18&) = delete;
    Final8x18(Final8x18&&) noexcept = default;
    Final8x18& operator=(Final8x18&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x19 {
public:
    Final8x19() = default;
    ~Final8x19() = default;
    Final8x19(const Final8x19&) = delete;
    Final8x19& operator=(const Final8x19&) = delete;
    Final8x19(Final8x19&&) noexcept = default;
    Final8x19& operator=(Final8x19&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x20 {
public:
    Final8x20() = default;
    ~Final8x20() = default;
    Final8x20(const Final8x20&) = delete;
    Final8x20& operator=(const Final8x20&) = delete;
    Final8x20(Final8x20&&) noexcept = default;
    Final8x20& operator=(Final8x20&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x21 {
public:
    Final8x21() = default;
    ~Final8x21() = default;
    Final8x21(const Final8x21&) = delete;
    Final8x21& operator=(const Final8x21&) = delete;
    Final8x21(Final8x21&&) noexcept = default;
    Final8x21& operator=(Final8x21&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x22 {
public:
    Final8x22() = default;
    ~Final8x22() = default;
    Final8x22(const Final8x22&) = delete;
    Final8x22& operator=(const Final8x22&) = delete;
    Final8x22(Final8x22&&) noexcept = default;
    Final8x22& operator=(Final8x22&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x23 {
public:
    Final8x23() = default;
    ~Final8x23() = default;
    Final8x23(const Final8x23&) = delete;
    Final8x23& operator=(const Final8x23&) = delete;
    Final8x23(Final8x23&&) noexcept = default;
    Final8x23& operator=(Final8x23&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x24 {
public:
    Final8x24() = default;
    ~Final8x24() = default;
    Final8x24(const Final8x24&) = delete;
    Final8x24& operator=(const Final8x24&) = delete;
    Final8x24(Final8x24&&) noexcept = default;
    Final8x24& operator=(Final8x24&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x25 {
public:
    Final8x25() = default;
    ~Final8x25() = default;
    Final8x25(const Final8x25&) = delete;
    Final8x25& operator=(const Final8x25&) = delete;
    Final8x25(Final8x25&&) noexcept = default;
    Final8x25& operator=(Final8x25&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x26 {
public:
    Final8x26() = default;
    ~Final8x26() = default;
    Final8x26(const Final8x26&) = delete;
    Final8x26& operator=(const Final8x26&) = delete;
    Final8x26(Final8x26&&) noexcept = default;
    Final8x26& operator=(Final8x26&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x27 {
public:
    Final8x27() = default;
    ~Final8x27() = default;
    Final8x27(const Final8x27&) = delete;
    Final8x27& operator=(const Final8x27&) = delete;
    Final8x27(Final8x27&&) noexcept = default;
    Final8x27& operator=(Final8x27&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x28 {
public:
    Final8x28() = default;
    ~Final8x28() = default;
    Final8x28(const Final8x28&) = delete;
    Final8x28& operator=(const Final8x28&) = delete;
    Final8x28(Final8x28&&) noexcept = default;
    Final8x28& operator=(Final8x28&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x29 {
public:
    Final8x29() = default;
    ~Final8x29() = default;
    Final8x29(const Final8x29&) = delete;
    Final8x29& operator=(const Final8x29&) = delete;
    Final8x29(Final8x29&&) noexcept = default;
    Final8x29& operator=(Final8x29&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x30 {
public:
    Final8x30() = default;
    ~Final8x30() = default;
    Final8x30(const Final8x30&) = delete;
    Final8x30& operator=(const Final8x30&) = delete;
    Final8x30(Final8x30&&) noexcept = default;
    Final8x30& operator=(Final8x30&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x31 {
public:
    Final8x31() = default;
    ~Final8x31() = default;
    Final8x31(const Final8x31&) = delete;
    Final8x31& operator=(const Final8x31&) = delete;
    Final8x31(Final8x31&&) noexcept = default;
    Final8x31& operator=(Final8x31&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x32 {
public:
    Final8x32() = default;
    ~Final8x32() = default;
    Final8x32(const Final8x32&) = delete;
    Final8x32& operator=(const Final8x32&) = delete;
    Final8x32(Final8x32&&) noexcept = default;
    Final8x32& operator=(Final8x32&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x33 {
public:
    Final8x33() = default;
    ~Final8x33() = default;
    Final8x33(const Final8x33&) = delete;
    Final8x33& operator=(const Final8x33&) = delete;
    Final8x33(Final8x33&&) noexcept = default;
    Final8x33& operator=(Final8x33&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x34 {
public:
    Final8x34() = default;
    ~Final8x34() = default;
    Final8x34(const Final8x34&) = delete;
    Final8x34& operator=(const Final8x34&) = delete;
    Final8x34(Final8x34&&) noexcept = default;
    Final8x34& operator=(Final8x34&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x35 {
public:
    Final8x35() = default;
    ~Final8x35() = default;
    Final8x35(const Final8x35&) = delete;
    Final8x35& operator=(const Final8x35&) = delete;
    Final8x35(Final8x35&&) noexcept = default;
    Final8x35& operator=(Final8x35&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x36 {
public:
    Final8x36() = default;
    ~Final8x36() = default;
    Final8x36(const Final8x36&) = delete;
    Final8x36& operator=(const Final8x36&) = delete;
    Final8x36(Final8x36&&) noexcept = default;
    Final8x36& operator=(Final8x36&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x37 {
public:
    Final8x37() = default;
    ~Final8x37() = default;
    Final8x37(const Final8x37&) = delete;
    Final8x37& operator=(const Final8x37&) = delete;
    Final8x37(Final8x37&&) noexcept = default;
    Final8x37& operator=(Final8x37&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x38 {
public:
    Final8x38() = default;
    ~Final8x38() = default;
    Final8x38(const Final8x38&) = delete;
    Final8x38& operator=(const Final8x38&) = delete;
    Final8x38(Final8x38&&) noexcept = default;
    Final8x38& operator=(Final8x38&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x39 {
public:
    Final8x39() = default;
    ~Final8x39() = default;
    Final8x39(const Final8x39&) = delete;
    Final8x39& operator=(const Final8x39&) = delete;
    Final8x39(Final8x39&&) noexcept = default;
    Final8x39& operator=(Final8x39&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x40 {
public:
    Final8x40() = default;
    ~Final8x40() = default;
    Final8x40(const Final8x40&) = delete;
    Final8x40& operator=(const Final8x40&) = delete;
    Final8x40(Final8x40&&) noexcept = default;
    Final8x40& operator=(Final8x40&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x41 {
public:
    Final8x41() = default;
    ~Final8x41() = default;
    Final8x41(const Final8x41&) = delete;
    Final8x41& operator=(const Final8x41&) = delete;
    Final8x41(Final8x41&&) noexcept = default;
    Final8x41& operator=(Final8x41&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x42 {
public:
    Final8x42() = default;
    ~Final8x42() = default;
    Final8x42(const Final8x42&) = delete;
    Final8x42& operator=(const Final8x42&) = delete;
    Final8x42(Final8x42&&) noexcept = default;
    Final8x42& operator=(Final8x42&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x43 {
public:
    Final8x43() = default;
    ~Final8x43() = default;
    Final8x43(const Final8x43&) = delete;
    Final8x43& operator=(const Final8x43&) = delete;
    Final8x43(Final8x43&&) noexcept = default;
    Final8x43& operator=(Final8x43&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x44 {
public:
    Final8x44() = default;
    ~Final8x44() = default;
    Final8x44(const Final8x44&) = delete;
    Final8x44& operator=(const Final8x44&) = delete;
    Final8x44(Final8x44&&) noexcept = default;
    Final8x44& operator=(Final8x44&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x45 {
public:
    Final8x45() = default;
    ~Final8x45() = default;
    Final8x45(const Final8x45&) = delete;
    Final8x45& operator=(const Final8x45&) = delete;
    Final8x45(Final8x45&&) noexcept = default;
    Final8x45& operator=(Final8x45&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x46 {
public:
    Final8x46() = default;
    ~Final8x46() = default;
    Final8x46(const Final8x46&) = delete;
    Final8x46& operator=(const Final8x46&) = delete;
    Final8x46(Final8x46&&) noexcept = default;
    Final8x46& operator=(Final8x46&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x47 {
public:
    Final8x47() = default;
    ~Final8x47() = default;
    Final8x47(const Final8x47&) = delete;
    Final8x47& operator=(const Final8x47&) = delete;
    Final8x47(Final8x47&&) noexcept = default;
    Final8x47& operator=(Final8x47&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x48 {
public:
    Final8x48() = default;
    ~Final8x48() = default;
    Final8x48(const Final8x48&) = delete;
    Final8x48& operator=(const Final8x48&) = delete;
    Final8x48(Final8x48&&) noexcept = default;
    Final8x48& operator=(Final8x48&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x49 {
public:
    Final8x49() = default;
    ~Final8x49() = default;
    Final8x49(const Final8x49&) = delete;
    Final8x49& operator=(const Final8x49&) = delete;
    Final8x49(Final8x49&&) noexcept = default;
    Final8x49& operator=(Final8x49&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x50 {
public:
    Final8x50() = default;
    ~Final8x50() = default;
    Final8x50(const Final8x50&) = delete;
    Final8x50& operator=(const Final8x50&) = delete;
    Final8x50(Final8x50&&) noexcept = default;
    Final8x50& operator=(Final8x50&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x51 {
public:
    Final8x51() = default;
    ~Final8x51() = default;
    Final8x51(const Final8x51&) = delete;
    Final8x51& operator=(const Final8x51&) = delete;
    Final8x51(Final8x51&&) noexcept = default;
    Final8x51& operator=(Final8x51&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x52 {
public:
    Final8x52() = default;
    ~Final8x52() = default;
    Final8x52(const Final8x52&) = delete;
    Final8x52& operator=(const Final8x52&) = delete;
    Final8x52(Final8x52&&) noexcept = default;
    Final8x52& operator=(Final8x52&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x53 {
public:
    Final8x53() = default;
    ~Final8x53() = default;
    Final8x53(const Final8x53&) = delete;
    Final8x53& operator=(const Final8x53&) = delete;
    Final8x53(Final8x53&&) noexcept = default;
    Final8x53& operator=(Final8x53&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x54 {
public:
    Final8x54() = default;
    ~Final8x54() = default;
    Final8x54(const Final8x54&) = delete;
    Final8x54& operator=(const Final8x54&) = delete;
    Final8x54(Final8x54&&) noexcept = default;
    Final8x54& operator=(Final8x54&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x55 {
public:
    Final8x55() = default;
    ~Final8x55() = default;
    Final8x55(const Final8x55&) = delete;
    Final8x55& operator=(const Final8x55&) = delete;
    Final8x55(Final8x55&&) noexcept = default;
    Final8x55& operator=(Final8x55&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x56 {
public:
    Final8x56() = default;
    ~Final8x56() = default;
    Final8x56(const Final8x56&) = delete;
    Final8x56& operator=(const Final8x56&) = delete;
    Final8x56(Final8x56&&) noexcept = default;
    Final8x56& operator=(Final8x56&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x57 {
public:
    Final8x57() = default;
    ~Final8x57() = default;
    Final8x57(const Final8x57&) = delete;
    Final8x57& operator=(const Final8x57&) = delete;
    Final8x57(Final8x57&&) noexcept = default;
    Final8x57& operator=(Final8x57&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x58 {
public:
    Final8x58() = default;
    ~Final8x58() = default;
    Final8x58(const Final8x58&) = delete;
    Final8x58& operator=(const Final8x58&) = delete;
    Final8x58(Final8x58&&) noexcept = default;
    Final8x58& operator=(Final8x58&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x59 {
public:
    Final8x59() = default;
    ~Final8x59() = default;
    Final8x59(const Final8x59&) = delete;
    Final8x59& operator=(const Final8x59&) = delete;
    Final8x59(Final8x59&&) noexcept = default;
    Final8x59& operator=(Final8x59&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x60 {
public:
    Final8x60() = default;
    ~Final8x60() = default;
    Final8x60(const Final8x60&) = delete;
    Final8x60& operator=(const Final8x60&) = delete;
    Final8x60(Final8x60&&) noexcept = default;
    Final8x60& operator=(Final8x60&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x61 {
public:
    Final8x61() = default;
    ~Final8x61() = default;
    Final8x61(const Final8x61&) = delete;
    Final8x61& operator=(const Final8x61&) = delete;
    Final8x61(Final8x61&&) noexcept = default;
    Final8x61& operator=(Final8x61&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x62 {
public:
    Final8x62() = default;
    ~Final8x62() = default;
    Final8x62(const Final8x62&) = delete;
    Final8x62& operator=(const Final8x62&) = delete;
    Final8x62(Final8x62&&) noexcept = default;
    Final8x62& operator=(Final8x62&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x63 {
public:
    Final8x63() = default;
    ~Final8x63() = default;
    Final8x63(const Final8x63&) = delete;
    Final8x63& operator=(const Final8x63&) = delete;
    Final8x63(Final8x63&&) noexcept = default;
    Final8x63& operator=(Final8x63&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x64 {
public:
    Final8x64() = default;
    ~Final8x64() = default;
    Final8x64(const Final8x64&) = delete;
    Final8x64& operator=(const Final8x64&) = delete;
    Final8x64(Final8x64&&) noexcept = default;
    Final8x64& operator=(Final8x64&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x65 {
public:
    Final8x65() = default;
    ~Final8x65() = default;
    Final8x65(const Final8x65&) = delete;
    Final8x65& operator=(const Final8x65&) = delete;
    Final8x65(Final8x65&&) noexcept = default;
    Final8x65& operator=(Final8x65&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x66 {
public:
    Final8x66() = default;
    ~Final8x66() = default;
    Final8x66(const Final8x66&) = delete;
    Final8x66& operator=(const Final8x66&) = delete;
    Final8x66(Final8x66&&) noexcept = default;
    Final8x66& operator=(Final8x66&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x67 {
public:
    Final8x67() = default;
    ~Final8x67() = default;
    Final8x67(const Final8x67&) = delete;
    Final8x67& operator=(const Final8x67&) = delete;
    Final8x67(Final8x67&&) noexcept = default;
    Final8x67& operator=(Final8x67&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x68 {
public:
    Final8x68() = default;
    ~Final8x68() = default;
    Final8x68(const Final8x68&) = delete;
    Final8x68& operator=(const Final8x68&) = delete;
    Final8x68(Final8x68&&) noexcept = default;
    Final8x68& operator=(Final8x68&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x69 {
public:
    Final8x69() = default;
    ~Final8x69() = default;
    Final8x69(const Final8x69&) = delete;
    Final8x69& operator=(const Final8x69&) = delete;
    Final8x69(Final8x69&&) noexcept = default;
    Final8x69& operator=(Final8x69&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x70 {
public:
    Final8x70() = default;
    ~Final8x70() = default;
    Final8x70(const Final8x70&) = delete;
    Final8x70& operator=(const Final8x70&) = delete;
    Final8x70(Final8x70&&) noexcept = default;
    Final8x70& operator=(Final8x70&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x71 {
public:
    Final8x71() = default;
    ~Final8x71() = default;
    Final8x71(const Final8x71&) = delete;
    Final8x71& operator=(const Final8x71&) = delete;
    Final8x71(Final8x71&&) noexcept = default;
    Final8x71& operator=(Final8x71&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x72 {
public:
    Final8x72() = default;
    ~Final8x72() = default;
    Final8x72(const Final8x72&) = delete;
    Final8x72& operator=(const Final8x72&) = delete;
    Final8x72(Final8x72&&) noexcept = default;
    Final8x72& operator=(Final8x72&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x73 {
public:
    Final8x73() = default;
    ~Final8x73() = default;
    Final8x73(const Final8x73&) = delete;
    Final8x73& operator=(const Final8x73&) = delete;
    Final8x73(Final8x73&&) noexcept = default;
    Final8x73& operator=(Final8x73&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x74 {
public:
    Final8x74() = default;
    ~Final8x74() = default;
    Final8x74(const Final8x74&) = delete;
    Final8x74& operator=(const Final8x74&) = delete;
    Final8x74(Final8x74&&) noexcept = default;
    Final8x74& operator=(Final8x74&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x75 {
public:
    Final8x75() = default;
    ~Final8x75() = default;
    Final8x75(const Final8x75&) = delete;
    Final8x75& operator=(const Final8x75&) = delete;
    Final8x75(Final8x75&&) noexcept = default;
    Final8x75& operator=(Final8x75&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x76 {
public:
    Final8x76() = default;
    ~Final8x76() = default;
    Final8x76(const Final8x76&) = delete;
    Final8x76& operator=(const Final8x76&) = delete;
    Final8x76(Final8x76&&) noexcept = default;
    Final8x76& operator=(Final8x76&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x77 {
public:
    Final8x77() = default;
    ~Final8x77() = default;
    Final8x77(const Final8x77&) = delete;
    Final8x77& operator=(const Final8x77&) = delete;
    Final8x77(Final8x77&&) noexcept = default;
    Final8x77& operator=(Final8x77&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x78 {
public:
    Final8x78() = default;
    ~Final8x78() = default;
    Final8x78(const Final8x78&) = delete;
    Final8x78& operator=(const Final8x78&) = delete;
    Final8x78(Final8x78&&) noexcept = default;
    Final8x78& operator=(Final8x78&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x79 {
public:
    Final8x79() = default;
    ~Final8x79() = default;
    Final8x79(const Final8x79&) = delete;
    Final8x79& operator=(const Final8x79&) = delete;
    Final8x79(Final8x79&&) noexcept = default;
    Final8x79& operator=(Final8x79&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x80 {
public:
    Final8x80() = default;
    ~Final8x80() = default;
    Final8x80(const Final8x80&) = delete;
    Final8x80& operator=(const Final8x80&) = delete;
    Final8x80(Final8x80&&) noexcept = default;
    Final8x80& operator=(Final8x80&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x81 {
public:
    Final8x81() = default;
    ~Final8x81() = default;
    Final8x81(const Final8x81&) = delete;
    Final8x81& operator=(const Final8x81&) = delete;
    Final8x81(Final8x81&&) noexcept = default;
    Final8x81& operator=(Final8x81&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x82 {
public:
    Final8x82() = default;
    ~Final8x82() = default;
    Final8x82(const Final8x82&) = delete;
    Final8x82& operator=(const Final8x82&) = delete;
    Final8x82(Final8x82&&) noexcept = default;
    Final8x82& operator=(Final8x82&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x83 {
public:
    Final8x83() = default;
    ~Final8x83() = default;
    Final8x83(const Final8x83&) = delete;
    Final8x83& operator=(const Final8x83&) = delete;
    Final8x83(Final8x83&&) noexcept = default;
    Final8x83& operator=(Final8x83&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x84 {
public:
    Final8x84() = default;
    ~Final8x84() = default;
    Final8x84(const Final8x84&) = delete;
    Final8x84& operator=(const Final8x84&) = delete;
    Final8x84(Final8x84&&) noexcept = default;
    Final8x84& operator=(Final8x84&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x85 {
public:
    Final8x85() = default;
    ~Final8x85() = default;
    Final8x85(const Final8x85&) = delete;
    Final8x85& operator=(const Final8x85&) = delete;
    Final8x85(Final8x85&&) noexcept = default;
    Final8x85& operator=(Final8x85&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x86 {
public:
    Final8x86() = default;
    ~Final8x86() = default;
    Final8x86(const Final8x86&) = delete;
    Final8x86& operator=(const Final8x86&) = delete;
    Final8x86(Final8x86&&) noexcept = default;
    Final8x86& operator=(Final8x86&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x87 {
public:
    Final8x87() = default;
    ~Final8x87() = default;
    Final8x87(const Final8x87&) = delete;
    Final8x87& operator=(const Final8x87&) = delete;
    Final8x87(Final8x87&&) noexcept = default;
    Final8x87& operator=(Final8x87&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x88 {
public:
    Final8x88() = default;
    ~Final8x88() = default;
    Final8x88(const Final8x88&) = delete;
    Final8x88& operator=(const Final8x88&) = delete;
    Final8x88(Final8x88&&) noexcept = default;
    Final8x88& operator=(Final8x88&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x89 {
public:
    Final8x89() = default;
    ~Final8x89() = default;
    Final8x89(const Final8x89&) = delete;
    Final8x89& operator=(const Final8x89&) = delete;
    Final8x89(Final8x89&&) noexcept = default;
    Final8x89& operator=(Final8x89&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x90 {
public:
    Final8x90() = default;
    ~Final8x90() = default;
    Final8x90(const Final8x90&) = delete;
    Final8x90& operator=(const Final8x90&) = delete;
    Final8x90(Final8x90&&) noexcept = default;
    Final8x90& operator=(Final8x90&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x91 {
public:
    Final8x91() = default;
    ~Final8x91() = default;
    Final8x91(const Final8x91&) = delete;
    Final8x91& operator=(const Final8x91&) = delete;
    Final8x91(Final8x91&&) noexcept = default;
    Final8x91& operator=(Final8x91&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x92 {
public:
    Final8x92() = default;
    ~Final8x92() = default;
    Final8x92(const Final8x92&) = delete;
    Final8x92& operator=(const Final8x92&) = delete;
    Final8x92(Final8x92&&) noexcept = default;
    Final8x92& operator=(Final8x92&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x93 {
public:
    Final8x93() = default;
    ~Final8x93() = default;
    Final8x93(const Final8x93&) = delete;
    Final8x93& operator=(const Final8x93&) = delete;
    Final8x93(Final8x93&&) noexcept = default;
    Final8x93& operator=(Final8x93&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x94 {
public:
    Final8x94() = default;
    ~Final8x94() = default;
    Final8x94(const Final8x94&) = delete;
    Final8x94& operator=(const Final8x94&) = delete;
    Final8x94(Final8x94&&) noexcept = default;
    Final8x94& operator=(Final8x94&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x95 {
public:
    Final8x95() = default;
    ~Final8x95() = default;
    Final8x95(const Final8x95&) = delete;
    Final8x95& operator=(const Final8x95&) = delete;
    Final8x95(Final8x95&&) noexcept = default;
    Final8x95& operator=(Final8x95&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x96 {
public:
    Final8x96() = default;
    ~Final8x96() = default;
    Final8x96(const Final8x96&) = delete;
    Final8x96& operator=(const Final8x96&) = delete;
    Final8x96(Final8x96&&) noexcept = default;
    Final8x96& operator=(Final8x96&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x97 {
public:
    Final8x97() = default;
    ~Final8x97() = default;
    Final8x97(const Final8x97&) = delete;
    Final8x97& operator=(const Final8x97&) = delete;
    Final8x97(Final8x97&&) noexcept = default;
    Final8x97& operator=(Final8x97&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x98 {
public:
    Final8x98() = default;
    ~Final8x98() = default;
    Final8x98(const Final8x98&) = delete;
    Final8x98& operator=(const Final8x98&) = delete;
    Final8x98(Final8x98&&) noexcept = default;
    Final8x98& operator=(Final8x98&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x99 {
public:
    Final8x99() = default;
    ~Final8x99() = default;
    Final8x99(const Final8x99&) = delete;
    Final8x99& operator=(const Final8x99&) = delete;
    Final8x99(Final8x99&&) noexcept = default;
    Final8x99& operator=(Final8x99&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x100 {
public:
    Final8x100() = default;
    ~Final8x100() = default;
    Final8x100(const Final8x100&) = delete;
    Final8x100& operator=(const Final8x100&) = delete;
    Final8x100(Final8x100&&) noexcept = default;
    Final8x100& operator=(Final8x100&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x101 {
public:
    Final8x101() = default;
    ~Final8x101() = default;
    Final8x101(const Final8x101&) = delete;
    Final8x101& operator=(const Final8x101&) = delete;
    Final8x101(Final8x101&&) noexcept = default;
    Final8x101& operator=(Final8x101&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x102 {
public:
    Final8x102() = default;
    ~Final8x102() = default;
    Final8x102(const Final8x102&) = delete;
    Final8x102& operator=(const Final8x102&) = delete;
    Final8x102(Final8x102&&) noexcept = default;
    Final8x102& operator=(Final8x102&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x103 {
public:
    Final8x103() = default;
    ~Final8x103() = default;
    Final8x103(const Final8x103&) = delete;
    Final8x103& operator=(const Final8x103&) = delete;
    Final8x103(Final8x103&&) noexcept = default;
    Final8x103& operator=(Final8x103&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x104 {
public:
    Final8x104() = default;
    ~Final8x104() = default;
    Final8x104(const Final8x104&) = delete;
    Final8x104& operator=(const Final8x104&) = delete;
    Final8x104(Final8x104&&) noexcept = default;
    Final8x104& operator=(Final8x104&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x105 {
public:
    Final8x105() = default;
    ~Final8x105() = default;
    Final8x105(const Final8x105&) = delete;
    Final8x105& operator=(const Final8x105&) = delete;
    Final8x105(Final8x105&&) noexcept = default;
    Final8x105& operator=(Final8x105&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x106 {
public:
    Final8x106() = default;
    ~Final8x106() = default;
    Final8x106(const Final8x106&) = delete;
    Final8x106& operator=(const Final8x106&) = delete;
    Final8x106(Final8x106&&) noexcept = default;
    Final8x106& operator=(Final8x106&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x107 {
public:
    Final8x107() = default;
    ~Final8x107() = default;
    Final8x107(const Final8x107&) = delete;
    Final8x107& operator=(const Final8x107&) = delete;
    Final8x107(Final8x107&&) noexcept = default;
    Final8x107& operator=(Final8x107&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x108 {
public:
    Final8x108() = default;
    ~Final8x108() = default;
    Final8x108(const Final8x108&) = delete;
    Final8x108& operator=(const Final8x108&) = delete;
    Final8x108(Final8x108&&) noexcept = default;
    Final8x108& operator=(Final8x108&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x109 {
public:
    Final8x109() = default;
    ~Final8x109() = default;
    Final8x109(const Final8x109&) = delete;
    Final8x109& operator=(const Final8x109&) = delete;
    Final8x109(Final8x109&&) noexcept = default;
    Final8x109& operator=(Final8x109&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x110 {
public:
    Final8x110() = default;
    ~Final8x110() = default;
    Final8x110(const Final8x110&) = delete;
    Final8x110& operator=(const Final8x110&) = delete;
    Final8x110(Final8x110&&) noexcept = default;
    Final8x110& operator=(Final8x110&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x111 {
public:
    Final8x111() = default;
    ~Final8x111() = default;
    Final8x111(const Final8x111&) = delete;
    Final8x111& operator=(const Final8x111&) = delete;
    Final8x111(Final8x111&&) noexcept = default;
    Final8x111& operator=(Final8x111&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x112 {
public:
    Final8x112() = default;
    ~Final8x112() = default;
    Final8x112(const Final8x112&) = delete;
    Final8x112& operator=(const Final8x112&) = delete;
    Final8x112(Final8x112&&) noexcept = default;
    Final8x112& operator=(Final8x112&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x113 {
public:
    Final8x113() = default;
    ~Final8x113() = default;
    Final8x113(const Final8x113&) = delete;
    Final8x113& operator=(const Final8x113&) = delete;
    Final8x113(Final8x113&&) noexcept = default;
    Final8x113& operator=(Final8x113&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x114 {
public:
    Final8x114() = default;
    ~Final8x114() = default;
    Final8x114(const Final8x114&) = delete;
    Final8x114& operator=(const Final8x114&) = delete;
    Final8x114(Final8x114&&) noexcept = default;
    Final8x114& operator=(Final8x114&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x115 {
public:
    Final8x115() = default;
    ~Final8x115() = default;
    Final8x115(const Final8x115&) = delete;
    Final8x115& operator=(const Final8x115&) = delete;
    Final8x115(Final8x115&&) noexcept = default;
    Final8x115& operator=(Final8x115&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x116 {
public:
    Final8x116() = default;
    ~Final8x116() = default;
    Final8x116(const Final8x116&) = delete;
    Final8x116& operator=(const Final8x116&) = delete;
    Final8x116(Final8x116&&) noexcept = default;
    Final8x116& operator=(Final8x116&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x117 {
public:
    Final8x117() = default;
    ~Final8x117() = default;
    Final8x117(const Final8x117&) = delete;
    Final8x117& operator=(const Final8x117&) = delete;
    Final8x117(Final8x117&&) noexcept = default;
    Final8x117& operator=(Final8x117&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x118 {
public:
    Final8x118() = default;
    ~Final8x118() = default;
    Final8x118(const Final8x118&) = delete;
    Final8x118& operator=(const Final8x118&) = delete;
    Final8x118(Final8x118&&) noexcept = default;
    Final8x118& operator=(Final8x118&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x119 {
public:
    Final8x119() = default;
    ~Final8x119() = default;
    Final8x119(const Final8x119&) = delete;
    Final8x119& operator=(const Final8x119&) = delete;
    Final8x119(Final8x119&&) noexcept = default;
    Final8x119& operator=(Final8x119&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x120 {
public:
    Final8x120() = default;
    ~Final8x120() = default;
    Final8x120(const Final8x120&) = delete;
    Final8x120& operator=(const Final8x120&) = delete;
    Final8x120(Final8x120&&) noexcept = default;
    Final8x120& operator=(Final8x120&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x121 {
public:
    Final8x121() = default;
    ~Final8x121() = default;
    Final8x121(const Final8x121&) = delete;
    Final8x121& operator=(const Final8x121&) = delete;
    Final8x121(Final8x121&&) noexcept = default;
    Final8x121& operator=(Final8x121&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x122 {
public:
    Final8x122() = default;
    ~Final8x122() = default;
    Final8x122(const Final8x122&) = delete;
    Final8x122& operator=(const Final8x122&) = delete;
    Final8x122(Final8x122&&) noexcept = default;
    Final8x122& operator=(Final8x122&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x123 {
public:
    Final8x123() = default;
    ~Final8x123() = default;
    Final8x123(const Final8x123&) = delete;
    Final8x123& operator=(const Final8x123&) = delete;
    Final8x123(Final8x123&&) noexcept = default;
    Final8x123& operator=(Final8x123&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x124 {
public:
    Final8x124() = default;
    ~Final8x124() = default;
    Final8x124(const Final8x124&) = delete;
    Final8x124& operator=(const Final8x124&) = delete;
    Final8x124(Final8x124&&) noexcept = default;
    Final8x124& operator=(Final8x124&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x125 {
public:
    Final8x125() = default;
    ~Final8x125() = default;
    Final8x125(const Final8x125&) = delete;
    Final8x125& operator=(const Final8x125&) = delete;
    Final8x125(Final8x125&&) noexcept = default;
    Final8x125& operator=(Final8x125&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x126 {
public:
    Final8x126() = default;
    ~Final8x126() = default;
    Final8x126(const Final8x126&) = delete;
    Final8x126& operator=(const Final8x126&) = delete;
    Final8x126(Final8x126&&) noexcept = default;
    Final8x126& operator=(Final8x126&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x127 {
public:
    Final8x127() = default;
    ~Final8x127() = default;
    Final8x127(const Final8x127&) = delete;
    Final8x127& operator=(const Final8x127&) = delete;
    Final8x127(Final8x127&&) noexcept = default;
    Final8x127& operator=(Final8x127&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x128 {
public:
    Final8x128() = default;
    ~Final8x128() = default;
    Final8x128(const Final8x128&) = delete;
    Final8x128& operator=(const Final8x128&) = delete;
    Final8x128(Final8x128&&) noexcept = default;
    Final8x128& operator=(Final8x128&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x129 {
public:
    Final8x129() = default;
    ~Final8x129() = default;
    Final8x129(const Final8x129&) = delete;
    Final8x129& operator=(const Final8x129&) = delete;
    Final8x129(Final8x129&&) noexcept = default;
    Final8x129& operator=(Final8x129&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x130 {
public:
    Final8x130() = default;
    ~Final8x130() = default;
    Final8x130(const Final8x130&) = delete;
    Final8x130& operator=(const Final8x130&) = delete;
    Final8x130(Final8x130&&) noexcept = default;
    Final8x130& operator=(Final8x130&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x131 {
public:
    Final8x131() = default;
    ~Final8x131() = default;
    Final8x131(const Final8x131&) = delete;
    Final8x131& operator=(const Final8x131&) = delete;
    Final8x131(Final8x131&&) noexcept = default;
    Final8x131& operator=(Final8x131&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x132 {
public:
    Final8x132() = default;
    ~Final8x132() = default;
    Final8x132(const Final8x132&) = delete;
    Final8x132& operator=(const Final8x132&) = delete;
    Final8x132(Final8x132&&) noexcept = default;
    Final8x132& operator=(Final8x132&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x133 {
public:
    Final8x133() = default;
    ~Final8x133() = default;
    Final8x133(const Final8x133&) = delete;
    Final8x133& operator=(const Final8x133&) = delete;
    Final8x133(Final8x133&&) noexcept = default;
    Final8x133& operator=(Final8x133&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x134 {
public:
    Final8x134() = default;
    ~Final8x134() = default;
    Final8x134(const Final8x134&) = delete;
    Final8x134& operator=(const Final8x134&) = delete;
    Final8x134(Final8x134&&) noexcept = default;
    Final8x134& operator=(Final8x134&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x135 {
public:
    Final8x135() = default;
    ~Final8x135() = default;
    Final8x135(const Final8x135&) = delete;
    Final8x135& operator=(const Final8x135&) = delete;
    Final8x135(Final8x135&&) noexcept = default;
    Final8x135& operator=(Final8x135&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x136 {
public:
    Final8x136() = default;
    ~Final8x136() = default;
    Final8x136(const Final8x136&) = delete;
    Final8x136& operator=(const Final8x136&) = delete;
    Final8x136(Final8x136&&) noexcept = default;
    Final8x136& operator=(Final8x136&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x137 {
public:
    Final8x137() = default;
    ~Final8x137() = default;
    Final8x137(const Final8x137&) = delete;
    Final8x137& operator=(const Final8x137&) = delete;
    Final8x137(Final8x137&&) noexcept = default;
    Final8x137& operator=(Final8x137&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x138 {
public:
    Final8x138() = default;
    ~Final8x138() = default;
    Final8x138(const Final8x138&) = delete;
    Final8x138& operator=(const Final8x138&) = delete;
    Final8x138(Final8x138&&) noexcept = default;
    Final8x138& operator=(Final8x138&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x139 {
public:
    Final8x139() = default;
    ~Final8x139() = default;
    Final8x139(const Final8x139&) = delete;
    Final8x139& operator=(const Final8x139&) = delete;
    Final8x139(Final8x139&&) noexcept = default;
    Final8x139& operator=(Final8x139&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x140 {
public:
    Final8x140() = default;
    ~Final8x140() = default;
    Final8x140(const Final8x140&) = delete;
    Final8x140& operator=(const Final8x140&) = delete;
    Final8x140(Final8x140&&) noexcept = default;
    Final8x140& operator=(Final8x140&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x141 {
public:
    Final8x141() = default;
    ~Final8x141() = default;
    Final8x141(const Final8x141&) = delete;
    Final8x141& operator=(const Final8x141&) = delete;
    Final8x141(Final8x141&&) noexcept = default;
    Final8x141& operator=(Final8x141&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x142 {
public:
    Final8x142() = default;
    ~Final8x142() = default;
    Final8x142(const Final8x142&) = delete;
    Final8x142& operator=(const Final8x142&) = delete;
    Final8x142(Final8x142&&) noexcept = default;
    Final8x142& operator=(Final8x142&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x143 {
public:
    Final8x143() = default;
    ~Final8x143() = default;
    Final8x143(const Final8x143&) = delete;
    Final8x143& operator=(const Final8x143&) = delete;
    Final8x143(Final8x143&&) noexcept = default;
    Final8x143& operator=(Final8x143&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x144 {
public:
    Final8x144() = default;
    ~Final8x144() = default;
    Final8x144(const Final8x144&) = delete;
    Final8x144& operator=(const Final8x144&) = delete;
    Final8x144(Final8x144&&) noexcept = default;
    Final8x144& operator=(Final8x144&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x145 {
public:
    Final8x145() = default;
    ~Final8x145() = default;
    Final8x145(const Final8x145&) = delete;
    Final8x145& operator=(const Final8x145&) = delete;
    Final8x145(Final8x145&&) noexcept = default;
    Final8x145& operator=(Final8x145&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x146 {
public:
    Final8x146() = default;
    ~Final8x146() = default;
    Final8x146(const Final8x146&) = delete;
    Final8x146& operator=(const Final8x146&) = delete;
    Final8x146(Final8x146&&) noexcept = default;
    Final8x146& operator=(Final8x146&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x147 {
public:
    Final8x147() = default;
    ~Final8x147() = default;
    Final8x147(const Final8x147&) = delete;
    Final8x147& operator=(const Final8x147&) = delete;
    Final8x147(Final8x147&&) noexcept = default;
    Final8x147& operator=(Final8x147&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x148 {
public:
    Final8x148() = default;
    ~Final8x148() = default;
    Final8x148(const Final8x148&) = delete;
    Final8x148& operator=(const Final8x148&) = delete;
    Final8x148(Final8x148&&) noexcept = default;
    Final8x148& operator=(Final8x148&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x149 {
public:
    Final8x149() = default;
    ~Final8x149() = default;
    Final8x149(const Final8x149&) = delete;
    Final8x149& operator=(const Final8x149&) = delete;
    Final8x149(Final8x149&&) noexcept = default;
    Final8x149& operator=(Final8x149&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x150 {
public:
    Final8x150() = default;
    ~Final8x150() = default;
    Final8x150(const Final8x150&) = delete;
    Final8x150& operator=(const Final8x150&) = delete;
    Final8x150(Final8x150&&) noexcept = default;
    Final8x150& operator=(Final8x150&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x151 {
public:
    Final8x151() = default;
    ~Final8x151() = default;
    Final8x151(const Final8x151&) = delete;
    Final8x151& operator=(const Final8x151&) = delete;
    Final8x151(Final8x151&&) noexcept = default;
    Final8x151& operator=(Final8x151&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x152 {
public:
    Final8x152() = default;
    ~Final8x152() = default;
    Final8x152(const Final8x152&) = delete;
    Final8x152& operator=(const Final8x152&) = delete;
    Final8x152(Final8x152&&) noexcept = default;
    Final8x152& operator=(Final8x152&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x153 {
public:
    Final8x153() = default;
    ~Final8x153() = default;
    Final8x153(const Final8x153&) = delete;
    Final8x153& operator=(const Final8x153&) = delete;
    Final8x153(Final8x153&&) noexcept = default;
    Final8x153& operator=(Final8x153&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x154 {
public:
    Final8x154() = default;
    ~Final8x154() = default;
    Final8x154(const Final8x154&) = delete;
    Final8x154& operator=(const Final8x154&) = delete;
    Final8x154(Final8x154&&) noexcept = default;
    Final8x154& operator=(Final8x154&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x155 {
public:
    Final8x155() = default;
    ~Final8x155() = default;
    Final8x155(const Final8x155&) = delete;
    Final8x155& operator=(const Final8x155&) = delete;
    Final8x155(Final8x155&&) noexcept = default;
    Final8x155& operator=(Final8x155&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x156 {
public:
    Final8x156() = default;
    ~Final8x156() = default;
    Final8x156(const Final8x156&) = delete;
    Final8x156& operator=(const Final8x156&) = delete;
    Final8x156(Final8x156&&) noexcept = default;
    Final8x156& operator=(Final8x156&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x157 {
public:
    Final8x157() = default;
    ~Final8x157() = default;
    Final8x157(const Final8x157&) = delete;
    Final8x157& operator=(const Final8x157&) = delete;
    Final8x157(Final8x157&&) noexcept = default;
    Final8x157& operator=(Final8x157&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x158 {
public:
    Final8x158() = default;
    ~Final8x158() = default;
    Final8x158(const Final8x158&) = delete;
    Final8x158& operator=(const Final8x158&) = delete;
    Final8x158(Final8x158&&) noexcept = default;
    Final8x158& operator=(Final8x158&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x159 {
public:
    Final8x159() = default;
    ~Final8x159() = default;
    Final8x159(const Final8x159&) = delete;
    Final8x159& operator=(const Final8x159&) = delete;
    Final8x159(Final8x159&&) noexcept = default;
    Final8x159& operator=(Final8x159&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x160 {
public:
    Final8x160() = default;
    ~Final8x160() = default;
    Final8x160(const Final8x160&) = delete;
    Final8x160& operator=(const Final8x160&) = delete;
    Final8x160(Final8x160&&) noexcept = default;
    Final8x160& operator=(Final8x160&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x161 {
public:
    Final8x161() = default;
    ~Final8x161() = default;
    Final8x161(const Final8x161&) = delete;
    Final8x161& operator=(const Final8x161&) = delete;
    Final8x161(Final8x161&&) noexcept = default;
    Final8x161& operator=(Final8x161&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x162 {
public:
    Final8x162() = default;
    ~Final8x162() = default;
    Final8x162(const Final8x162&) = delete;
    Final8x162& operator=(const Final8x162&) = delete;
    Final8x162(Final8x162&&) noexcept = default;
    Final8x162& operator=(Final8x162&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x163 {
public:
    Final8x163() = default;
    ~Final8x163() = default;
    Final8x163(const Final8x163&) = delete;
    Final8x163& operator=(const Final8x163&) = delete;
    Final8x163(Final8x163&&) noexcept = default;
    Final8x163& operator=(Final8x163&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x164 {
public:
    Final8x164() = default;
    ~Final8x164() = default;
    Final8x164(const Final8x164&) = delete;
    Final8x164& operator=(const Final8x164&) = delete;
    Final8x164(Final8x164&&) noexcept = default;
    Final8x164& operator=(Final8x164&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x165 {
public:
    Final8x165() = default;
    ~Final8x165() = default;
    Final8x165(const Final8x165&) = delete;
    Final8x165& operator=(const Final8x165&) = delete;
    Final8x165(Final8x165&&) noexcept = default;
    Final8x165& operator=(Final8x165&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x166 {
public:
    Final8x166() = default;
    ~Final8x166() = default;
    Final8x166(const Final8x166&) = delete;
    Final8x166& operator=(const Final8x166&) = delete;
    Final8x166(Final8x166&&) noexcept = default;
    Final8x166& operator=(Final8x166&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x167 {
public:
    Final8x167() = default;
    ~Final8x167() = default;
    Final8x167(const Final8x167&) = delete;
    Final8x167& operator=(const Final8x167&) = delete;
    Final8x167(Final8x167&&) noexcept = default;
    Final8x167& operator=(Final8x167&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x168 {
public:
    Final8x168() = default;
    ~Final8x168() = default;
    Final8x168(const Final8x168&) = delete;
    Final8x168& operator=(const Final8x168&) = delete;
    Final8x168(Final8x168&&) noexcept = default;
    Final8x168& operator=(Final8x168&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x169 {
public:
    Final8x169() = default;
    ~Final8x169() = default;
    Final8x169(const Final8x169&) = delete;
    Final8x169& operator=(const Final8x169&) = delete;
    Final8x169(Final8x169&&) noexcept = default;
    Final8x169& operator=(Final8x169&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x170 {
public:
    Final8x170() = default;
    ~Final8x170() = default;
    Final8x170(const Final8x170&) = delete;
    Final8x170& operator=(const Final8x170&) = delete;
    Final8x170(Final8x170&&) noexcept = default;
    Final8x170& operator=(Final8x170&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x171 {
public:
    Final8x171() = default;
    ~Final8x171() = default;
    Final8x171(const Final8x171&) = delete;
    Final8x171& operator=(const Final8x171&) = delete;
    Final8x171(Final8x171&&) noexcept = default;
    Final8x171& operator=(Final8x171&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x172 {
public:
    Final8x172() = default;
    ~Final8x172() = default;
    Final8x172(const Final8x172&) = delete;
    Final8x172& operator=(const Final8x172&) = delete;
    Final8x172(Final8x172&&) noexcept = default;
    Final8x172& operator=(Final8x172&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x173 {
public:
    Final8x173() = default;
    ~Final8x173() = default;
    Final8x173(const Final8x173&) = delete;
    Final8x173& operator=(const Final8x173&) = delete;
    Final8x173(Final8x173&&) noexcept = default;
    Final8x173& operator=(Final8x173&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x174 {
public:
    Final8x174() = default;
    ~Final8x174() = default;
    Final8x174(const Final8x174&) = delete;
    Final8x174& operator=(const Final8x174&) = delete;
    Final8x174(Final8x174&&) noexcept = default;
    Final8x174& operator=(Final8x174&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x175 {
public:
    Final8x175() = default;
    ~Final8x175() = default;
    Final8x175(const Final8x175&) = delete;
    Final8x175& operator=(const Final8x175&) = delete;
    Final8x175(Final8x175&&) noexcept = default;
    Final8x175& operator=(Final8x175&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x176 {
public:
    Final8x176() = default;
    ~Final8x176() = default;
    Final8x176(const Final8x176&) = delete;
    Final8x176& operator=(const Final8x176&) = delete;
    Final8x176(Final8x176&&) noexcept = default;
    Final8x176& operator=(Final8x176&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x177 {
public:
    Final8x177() = default;
    ~Final8x177() = default;
    Final8x177(const Final8x177&) = delete;
    Final8x177& operator=(const Final8x177&) = delete;
    Final8x177(Final8x177&&) noexcept = default;
    Final8x177& operator=(Final8x177&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x178 {
public:
    Final8x178() = default;
    ~Final8x178() = default;
    Final8x178(const Final8x178&) = delete;
    Final8x178& operator=(const Final8x178&) = delete;
    Final8x178(Final8x178&&) noexcept = default;
    Final8x178& operator=(Final8x178&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x179 {
public:
    Final8x179() = default;
    ~Final8x179() = default;
    Final8x179(const Final8x179&) = delete;
    Final8x179& operator=(const Final8x179&) = delete;
    Final8x179(Final8x179&&) noexcept = default;
    Final8x179& operator=(Final8x179&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x180 {
public:
    Final8x180() = default;
    ~Final8x180() = default;
    Final8x180(const Final8x180&) = delete;
    Final8x180& operator=(const Final8x180&) = delete;
    Final8x180(Final8x180&&) noexcept = default;
    Final8x180& operator=(Final8x180&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x181 {
public:
    Final8x181() = default;
    ~Final8x181() = default;
    Final8x181(const Final8x181&) = delete;
    Final8x181& operator=(const Final8x181&) = delete;
    Final8x181(Final8x181&&) noexcept = default;
    Final8x181& operator=(Final8x181&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x182 {
public:
    Final8x182() = default;
    ~Final8x182() = default;
    Final8x182(const Final8x182&) = delete;
    Final8x182& operator=(const Final8x182&) = delete;
    Final8x182(Final8x182&&) noexcept = default;
    Final8x182& operator=(Final8x182&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x183 {
public:
    Final8x183() = default;
    ~Final8x183() = default;
    Final8x183(const Final8x183&) = delete;
    Final8x183& operator=(const Final8x183&) = delete;
    Final8x183(Final8x183&&) noexcept = default;
    Final8x183& operator=(Final8x183&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x184 {
public:
    Final8x184() = default;
    ~Final8x184() = default;
    Final8x184(const Final8x184&) = delete;
    Final8x184& operator=(const Final8x184&) = delete;
    Final8x184(Final8x184&&) noexcept = default;
    Final8x184& operator=(Final8x184&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x185 {
public:
    Final8x185() = default;
    ~Final8x185() = default;
    Final8x185(const Final8x185&) = delete;
    Final8x185& operator=(const Final8x185&) = delete;
    Final8x185(Final8x185&&) noexcept = default;
    Final8x185& operator=(Final8x185&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x186 {
public:
    Final8x186() = default;
    ~Final8x186() = default;
    Final8x186(const Final8x186&) = delete;
    Final8x186& operator=(const Final8x186&) = delete;
    Final8x186(Final8x186&&) noexcept = default;
    Final8x186& operator=(Final8x186&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x187 {
public:
    Final8x187() = default;
    ~Final8x187() = default;
    Final8x187(const Final8x187&) = delete;
    Final8x187& operator=(const Final8x187&) = delete;
    Final8x187(Final8x187&&) noexcept = default;
    Final8x187& operator=(Final8x187&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x188 {
public:
    Final8x188() = default;
    ~Final8x188() = default;
    Final8x188(const Final8x188&) = delete;
    Final8x188& operator=(const Final8x188&) = delete;
    Final8x188(Final8x188&&) noexcept = default;
    Final8x188& operator=(Final8x188&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x189 {
public:
    Final8x189() = default;
    ~Final8x189() = default;
    Final8x189(const Final8x189&) = delete;
    Final8x189& operator=(const Final8x189&) = delete;
    Final8x189(Final8x189&&) noexcept = default;
    Final8x189& operator=(Final8x189&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x190 {
public:
    Final8x190() = default;
    ~Final8x190() = default;
    Final8x190(const Final8x190&) = delete;
    Final8x190& operator=(const Final8x190&) = delete;
    Final8x190(Final8x190&&) noexcept = default;
    Final8x190& operator=(Final8x190&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x191 {
public:
    Final8x191() = default;
    ~Final8x191() = default;
    Final8x191(const Final8x191&) = delete;
    Final8x191& operator=(const Final8x191&) = delete;
    Final8x191(Final8x191&&) noexcept = default;
    Final8x191& operator=(Final8x191&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x192 {
public:
    Final8x192() = default;
    ~Final8x192() = default;
    Final8x192(const Final8x192&) = delete;
    Final8x192& operator=(const Final8x192&) = delete;
    Final8x192(Final8x192&&) noexcept = default;
    Final8x192& operator=(Final8x192&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x193 {
public:
    Final8x193() = default;
    ~Final8x193() = default;
    Final8x193(const Final8x193&) = delete;
    Final8x193& operator=(const Final8x193&) = delete;
    Final8x193(Final8x193&&) noexcept = default;
    Final8x193& operator=(Final8x193&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x194 {
public:
    Final8x194() = default;
    ~Final8x194() = default;
    Final8x194(const Final8x194&) = delete;
    Final8x194& operator=(const Final8x194&) = delete;
    Final8x194(Final8x194&&) noexcept = default;
    Final8x194& operator=(Final8x194&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x195 {
public:
    Final8x195() = default;
    ~Final8x195() = default;
    Final8x195(const Final8x195&) = delete;
    Final8x195& operator=(const Final8x195&) = delete;
    Final8x195(Final8x195&&) noexcept = default;
    Final8x195& operator=(Final8x195&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x196 {
public:
    Final8x196() = default;
    ~Final8x196() = default;
    Final8x196(const Final8x196&) = delete;
    Final8x196& operator=(const Final8x196&) = delete;
    Final8x196(Final8x196&&) noexcept = default;
    Final8x196& operator=(Final8x196&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x197 {
public:
    Final8x197() = default;
    ~Final8x197() = default;
    Final8x197(const Final8x197&) = delete;
    Final8x197& operator=(const Final8x197&) = delete;
    Final8x197(Final8x197&&) noexcept = default;
    Final8x197& operator=(Final8x197&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x198 {
public:
    Final8x198() = default;
    ~Final8x198() = default;
    Final8x198(const Final8x198&) = delete;
    Final8x198& operator=(const Final8x198&) = delete;
    Final8x198(Final8x198&&) noexcept = default;
    Final8x198& operator=(Final8x198&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final8x199 {
public:
    Final8x199() = default;
    ~Final8x199() = default;
    Final8x199(const Final8x199&) = delete;
    Final8x199& operator=(const Final8x199&) = delete;
    Final8x199(Final8x199&&) noexcept = default;
    Final8x199& operator=(Final8x199&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

} }