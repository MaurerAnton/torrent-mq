#include "torrent/common/types.h"
#include <atomic>
#include <string>
#include <vector>
#include <mutex>
#include <memory>
#include <chrono>

namespace torrent::final_2 { namespace {
class Final2x0 {
public:
    Final2x0() = default;
    ~Final2x0() = default;
    Final2x0(const Final2x0&) = delete;
    Final2x0& operator=(const Final2x0&) = delete;
    Final2x0(Final2x0&&) noexcept = default;
    Final2x0& operator=(Final2x0&&) noexcept = default;
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

class Final2x1 {
public:
    Final2x1() = default;
    ~Final2x1() = default;
    Final2x1(const Final2x1&) = delete;
    Final2x1& operator=(const Final2x1&) = delete;
    Final2x1(Final2x1&&) noexcept = default;
    Final2x1& operator=(Final2x1&&) noexcept = default;
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

class Final2x2 {
public:
    Final2x2() = default;
    ~Final2x2() = default;
    Final2x2(const Final2x2&) = delete;
    Final2x2& operator=(const Final2x2&) = delete;
    Final2x2(Final2x2&&) noexcept = default;
    Final2x2& operator=(Final2x2&&) noexcept = default;
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

class Final2x3 {
public:
    Final2x3() = default;
    ~Final2x3() = default;
    Final2x3(const Final2x3&) = delete;
    Final2x3& operator=(const Final2x3&) = delete;
    Final2x3(Final2x3&&) noexcept = default;
    Final2x3& operator=(Final2x3&&) noexcept = default;
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

class Final2x4 {
public:
    Final2x4() = default;
    ~Final2x4() = default;
    Final2x4(const Final2x4&) = delete;
    Final2x4& operator=(const Final2x4&) = delete;
    Final2x4(Final2x4&&) noexcept = default;
    Final2x4& operator=(Final2x4&&) noexcept = default;
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

class Final2x5 {
public:
    Final2x5() = default;
    ~Final2x5() = default;
    Final2x5(const Final2x5&) = delete;
    Final2x5& operator=(const Final2x5&) = delete;
    Final2x5(Final2x5&&) noexcept = default;
    Final2x5& operator=(Final2x5&&) noexcept = default;
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

class Final2x6 {
public:
    Final2x6() = default;
    ~Final2x6() = default;
    Final2x6(const Final2x6&) = delete;
    Final2x6& operator=(const Final2x6&) = delete;
    Final2x6(Final2x6&&) noexcept = default;
    Final2x6& operator=(Final2x6&&) noexcept = default;
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

class Final2x7 {
public:
    Final2x7() = default;
    ~Final2x7() = default;
    Final2x7(const Final2x7&) = delete;
    Final2x7& operator=(const Final2x7&) = delete;
    Final2x7(Final2x7&&) noexcept = default;
    Final2x7& operator=(Final2x7&&) noexcept = default;
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

class Final2x8 {
public:
    Final2x8() = default;
    ~Final2x8() = default;
    Final2x8(const Final2x8&) = delete;
    Final2x8& operator=(const Final2x8&) = delete;
    Final2x8(Final2x8&&) noexcept = default;
    Final2x8& operator=(Final2x8&&) noexcept = default;
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

class Final2x9 {
public:
    Final2x9() = default;
    ~Final2x9() = default;
    Final2x9(const Final2x9&) = delete;
    Final2x9& operator=(const Final2x9&) = delete;
    Final2x9(Final2x9&&) noexcept = default;
    Final2x9& operator=(Final2x9&&) noexcept = default;
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

class Final2x10 {
public:
    Final2x10() = default;
    ~Final2x10() = default;
    Final2x10(const Final2x10&) = delete;
    Final2x10& operator=(const Final2x10&) = delete;
    Final2x10(Final2x10&&) noexcept = default;
    Final2x10& operator=(Final2x10&&) noexcept = default;
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

class Final2x11 {
public:
    Final2x11() = default;
    ~Final2x11() = default;
    Final2x11(const Final2x11&) = delete;
    Final2x11& operator=(const Final2x11&) = delete;
    Final2x11(Final2x11&&) noexcept = default;
    Final2x11& operator=(Final2x11&&) noexcept = default;
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

class Final2x12 {
public:
    Final2x12() = default;
    ~Final2x12() = default;
    Final2x12(const Final2x12&) = delete;
    Final2x12& operator=(const Final2x12&) = delete;
    Final2x12(Final2x12&&) noexcept = default;
    Final2x12& operator=(Final2x12&&) noexcept = default;
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

class Final2x13 {
public:
    Final2x13() = default;
    ~Final2x13() = default;
    Final2x13(const Final2x13&) = delete;
    Final2x13& operator=(const Final2x13&) = delete;
    Final2x13(Final2x13&&) noexcept = default;
    Final2x13& operator=(Final2x13&&) noexcept = default;
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

class Final2x14 {
public:
    Final2x14() = default;
    ~Final2x14() = default;
    Final2x14(const Final2x14&) = delete;
    Final2x14& operator=(const Final2x14&) = delete;
    Final2x14(Final2x14&&) noexcept = default;
    Final2x14& operator=(Final2x14&&) noexcept = default;
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

class Final2x15 {
public:
    Final2x15() = default;
    ~Final2x15() = default;
    Final2x15(const Final2x15&) = delete;
    Final2x15& operator=(const Final2x15&) = delete;
    Final2x15(Final2x15&&) noexcept = default;
    Final2x15& operator=(Final2x15&&) noexcept = default;
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

class Final2x16 {
public:
    Final2x16() = default;
    ~Final2x16() = default;
    Final2x16(const Final2x16&) = delete;
    Final2x16& operator=(const Final2x16&) = delete;
    Final2x16(Final2x16&&) noexcept = default;
    Final2x16& operator=(Final2x16&&) noexcept = default;
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

class Final2x17 {
public:
    Final2x17() = default;
    ~Final2x17() = default;
    Final2x17(const Final2x17&) = delete;
    Final2x17& operator=(const Final2x17&) = delete;
    Final2x17(Final2x17&&) noexcept = default;
    Final2x17& operator=(Final2x17&&) noexcept = default;
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

class Final2x18 {
public:
    Final2x18() = default;
    ~Final2x18() = default;
    Final2x18(const Final2x18&) = delete;
    Final2x18& operator=(const Final2x18&) = delete;
    Final2x18(Final2x18&&) noexcept = default;
    Final2x18& operator=(Final2x18&&) noexcept = default;
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

class Final2x19 {
public:
    Final2x19() = default;
    ~Final2x19() = default;
    Final2x19(const Final2x19&) = delete;
    Final2x19& operator=(const Final2x19&) = delete;
    Final2x19(Final2x19&&) noexcept = default;
    Final2x19& operator=(Final2x19&&) noexcept = default;
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

class Final2x20 {
public:
    Final2x20() = default;
    ~Final2x20() = default;
    Final2x20(const Final2x20&) = delete;
    Final2x20& operator=(const Final2x20&) = delete;
    Final2x20(Final2x20&&) noexcept = default;
    Final2x20& operator=(Final2x20&&) noexcept = default;
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

class Final2x21 {
public:
    Final2x21() = default;
    ~Final2x21() = default;
    Final2x21(const Final2x21&) = delete;
    Final2x21& operator=(const Final2x21&) = delete;
    Final2x21(Final2x21&&) noexcept = default;
    Final2x21& operator=(Final2x21&&) noexcept = default;
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

class Final2x22 {
public:
    Final2x22() = default;
    ~Final2x22() = default;
    Final2x22(const Final2x22&) = delete;
    Final2x22& operator=(const Final2x22&) = delete;
    Final2x22(Final2x22&&) noexcept = default;
    Final2x22& operator=(Final2x22&&) noexcept = default;
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

class Final2x23 {
public:
    Final2x23() = default;
    ~Final2x23() = default;
    Final2x23(const Final2x23&) = delete;
    Final2x23& operator=(const Final2x23&) = delete;
    Final2x23(Final2x23&&) noexcept = default;
    Final2x23& operator=(Final2x23&&) noexcept = default;
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

class Final2x24 {
public:
    Final2x24() = default;
    ~Final2x24() = default;
    Final2x24(const Final2x24&) = delete;
    Final2x24& operator=(const Final2x24&) = delete;
    Final2x24(Final2x24&&) noexcept = default;
    Final2x24& operator=(Final2x24&&) noexcept = default;
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

class Final2x25 {
public:
    Final2x25() = default;
    ~Final2x25() = default;
    Final2x25(const Final2x25&) = delete;
    Final2x25& operator=(const Final2x25&) = delete;
    Final2x25(Final2x25&&) noexcept = default;
    Final2x25& operator=(Final2x25&&) noexcept = default;
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

class Final2x26 {
public:
    Final2x26() = default;
    ~Final2x26() = default;
    Final2x26(const Final2x26&) = delete;
    Final2x26& operator=(const Final2x26&) = delete;
    Final2x26(Final2x26&&) noexcept = default;
    Final2x26& operator=(Final2x26&&) noexcept = default;
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

class Final2x27 {
public:
    Final2x27() = default;
    ~Final2x27() = default;
    Final2x27(const Final2x27&) = delete;
    Final2x27& operator=(const Final2x27&) = delete;
    Final2x27(Final2x27&&) noexcept = default;
    Final2x27& operator=(Final2x27&&) noexcept = default;
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

class Final2x28 {
public:
    Final2x28() = default;
    ~Final2x28() = default;
    Final2x28(const Final2x28&) = delete;
    Final2x28& operator=(const Final2x28&) = delete;
    Final2x28(Final2x28&&) noexcept = default;
    Final2x28& operator=(Final2x28&&) noexcept = default;
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

class Final2x29 {
public:
    Final2x29() = default;
    ~Final2x29() = default;
    Final2x29(const Final2x29&) = delete;
    Final2x29& operator=(const Final2x29&) = delete;
    Final2x29(Final2x29&&) noexcept = default;
    Final2x29& operator=(Final2x29&&) noexcept = default;
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

class Final2x30 {
public:
    Final2x30() = default;
    ~Final2x30() = default;
    Final2x30(const Final2x30&) = delete;
    Final2x30& operator=(const Final2x30&) = delete;
    Final2x30(Final2x30&&) noexcept = default;
    Final2x30& operator=(Final2x30&&) noexcept = default;
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

class Final2x31 {
public:
    Final2x31() = default;
    ~Final2x31() = default;
    Final2x31(const Final2x31&) = delete;
    Final2x31& operator=(const Final2x31&) = delete;
    Final2x31(Final2x31&&) noexcept = default;
    Final2x31& operator=(Final2x31&&) noexcept = default;
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

class Final2x32 {
public:
    Final2x32() = default;
    ~Final2x32() = default;
    Final2x32(const Final2x32&) = delete;
    Final2x32& operator=(const Final2x32&) = delete;
    Final2x32(Final2x32&&) noexcept = default;
    Final2x32& operator=(Final2x32&&) noexcept = default;
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

class Final2x33 {
public:
    Final2x33() = default;
    ~Final2x33() = default;
    Final2x33(const Final2x33&) = delete;
    Final2x33& operator=(const Final2x33&) = delete;
    Final2x33(Final2x33&&) noexcept = default;
    Final2x33& operator=(Final2x33&&) noexcept = default;
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

class Final2x34 {
public:
    Final2x34() = default;
    ~Final2x34() = default;
    Final2x34(const Final2x34&) = delete;
    Final2x34& operator=(const Final2x34&) = delete;
    Final2x34(Final2x34&&) noexcept = default;
    Final2x34& operator=(Final2x34&&) noexcept = default;
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

class Final2x35 {
public:
    Final2x35() = default;
    ~Final2x35() = default;
    Final2x35(const Final2x35&) = delete;
    Final2x35& operator=(const Final2x35&) = delete;
    Final2x35(Final2x35&&) noexcept = default;
    Final2x35& operator=(Final2x35&&) noexcept = default;
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

class Final2x36 {
public:
    Final2x36() = default;
    ~Final2x36() = default;
    Final2x36(const Final2x36&) = delete;
    Final2x36& operator=(const Final2x36&) = delete;
    Final2x36(Final2x36&&) noexcept = default;
    Final2x36& operator=(Final2x36&&) noexcept = default;
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

class Final2x37 {
public:
    Final2x37() = default;
    ~Final2x37() = default;
    Final2x37(const Final2x37&) = delete;
    Final2x37& operator=(const Final2x37&) = delete;
    Final2x37(Final2x37&&) noexcept = default;
    Final2x37& operator=(Final2x37&&) noexcept = default;
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

class Final2x38 {
public:
    Final2x38() = default;
    ~Final2x38() = default;
    Final2x38(const Final2x38&) = delete;
    Final2x38& operator=(const Final2x38&) = delete;
    Final2x38(Final2x38&&) noexcept = default;
    Final2x38& operator=(Final2x38&&) noexcept = default;
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

class Final2x39 {
public:
    Final2x39() = default;
    ~Final2x39() = default;
    Final2x39(const Final2x39&) = delete;
    Final2x39& operator=(const Final2x39&) = delete;
    Final2x39(Final2x39&&) noexcept = default;
    Final2x39& operator=(Final2x39&&) noexcept = default;
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

class Final2x40 {
public:
    Final2x40() = default;
    ~Final2x40() = default;
    Final2x40(const Final2x40&) = delete;
    Final2x40& operator=(const Final2x40&) = delete;
    Final2x40(Final2x40&&) noexcept = default;
    Final2x40& operator=(Final2x40&&) noexcept = default;
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

class Final2x41 {
public:
    Final2x41() = default;
    ~Final2x41() = default;
    Final2x41(const Final2x41&) = delete;
    Final2x41& operator=(const Final2x41&) = delete;
    Final2x41(Final2x41&&) noexcept = default;
    Final2x41& operator=(Final2x41&&) noexcept = default;
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

class Final2x42 {
public:
    Final2x42() = default;
    ~Final2x42() = default;
    Final2x42(const Final2x42&) = delete;
    Final2x42& operator=(const Final2x42&) = delete;
    Final2x42(Final2x42&&) noexcept = default;
    Final2x42& operator=(Final2x42&&) noexcept = default;
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

class Final2x43 {
public:
    Final2x43() = default;
    ~Final2x43() = default;
    Final2x43(const Final2x43&) = delete;
    Final2x43& operator=(const Final2x43&) = delete;
    Final2x43(Final2x43&&) noexcept = default;
    Final2x43& operator=(Final2x43&&) noexcept = default;
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

class Final2x44 {
public:
    Final2x44() = default;
    ~Final2x44() = default;
    Final2x44(const Final2x44&) = delete;
    Final2x44& operator=(const Final2x44&) = delete;
    Final2x44(Final2x44&&) noexcept = default;
    Final2x44& operator=(Final2x44&&) noexcept = default;
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

class Final2x45 {
public:
    Final2x45() = default;
    ~Final2x45() = default;
    Final2x45(const Final2x45&) = delete;
    Final2x45& operator=(const Final2x45&) = delete;
    Final2x45(Final2x45&&) noexcept = default;
    Final2x45& operator=(Final2x45&&) noexcept = default;
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

class Final2x46 {
public:
    Final2x46() = default;
    ~Final2x46() = default;
    Final2x46(const Final2x46&) = delete;
    Final2x46& operator=(const Final2x46&) = delete;
    Final2x46(Final2x46&&) noexcept = default;
    Final2x46& operator=(Final2x46&&) noexcept = default;
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

class Final2x47 {
public:
    Final2x47() = default;
    ~Final2x47() = default;
    Final2x47(const Final2x47&) = delete;
    Final2x47& operator=(const Final2x47&) = delete;
    Final2x47(Final2x47&&) noexcept = default;
    Final2x47& operator=(Final2x47&&) noexcept = default;
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

class Final2x48 {
public:
    Final2x48() = default;
    ~Final2x48() = default;
    Final2x48(const Final2x48&) = delete;
    Final2x48& operator=(const Final2x48&) = delete;
    Final2x48(Final2x48&&) noexcept = default;
    Final2x48& operator=(Final2x48&&) noexcept = default;
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

class Final2x49 {
public:
    Final2x49() = default;
    ~Final2x49() = default;
    Final2x49(const Final2x49&) = delete;
    Final2x49& operator=(const Final2x49&) = delete;
    Final2x49(Final2x49&&) noexcept = default;
    Final2x49& operator=(Final2x49&&) noexcept = default;
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

class Final2x50 {
public:
    Final2x50() = default;
    ~Final2x50() = default;
    Final2x50(const Final2x50&) = delete;
    Final2x50& operator=(const Final2x50&) = delete;
    Final2x50(Final2x50&&) noexcept = default;
    Final2x50& operator=(Final2x50&&) noexcept = default;
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

class Final2x51 {
public:
    Final2x51() = default;
    ~Final2x51() = default;
    Final2x51(const Final2x51&) = delete;
    Final2x51& operator=(const Final2x51&) = delete;
    Final2x51(Final2x51&&) noexcept = default;
    Final2x51& operator=(Final2x51&&) noexcept = default;
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

class Final2x52 {
public:
    Final2x52() = default;
    ~Final2x52() = default;
    Final2x52(const Final2x52&) = delete;
    Final2x52& operator=(const Final2x52&) = delete;
    Final2x52(Final2x52&&) noexcept = default;
    Final2x52& operator=(Final2x52&&) noexcept = default;
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

class Final2x53 {
public:
    Final2x53() = default;
    ~Final2x53() = default;
    Final2x53(const Final2x53&) = delete;
    Final2x53& operator=(const Final2x53&) = delete;
    Final2x53(Final2x53&&) noexcept = default;
    Final2x53& operator=(Final2x53&&) noexcept = default;
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

class Final2x54 {
public:
    Final2x54() = default;
    ~Final2x54() = default;
    Final2x54(const Final2x54&) = delete;
    Final2x54& operator=(const Final2x54&) = delete;
    Final2x54(Final2x54&&) noexcept = default;
    Final2x54& operator=(Final2x54&&) noexcept = default;
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

class Final2x55 {
public:
    Final2x55() = default;
    ~Final2x55() = default;
    Final2x55(const Final2x55&) = delete;
    Final2x55& operator=(const Final2x55&) = delete;
    Final2x55(Final2x55&&) noexcept = default;
    Final2x55& operator=(Final2x55&&) noexcept = default;
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

class Final2x56 {
public:
    Final2x56() = default;
    ~Final2x56() = default;
    Final2x56(const Final2x56&) = delete;
    Final2x56& operator=(const Final2x56&) = delete;
    Final2x56(Final2x56&&) noexcept = default;
    Final2x56& operator=(Final2x56&&) noexcept = default;
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

class Final2x57 {
public:
    Final2x57() = default;
    ~Final2x57() = default;
    Final2x57(const Final2x57&) = delete;
    Final2x57& operator=(const Final2x57&) = delete;
    Final2x57(Final2x57&&) noexcept = default;
    Final2x57& operator=(Final2x57&&) noexcept = default;
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

class Final2x58 {
public:
    Final2x58() = default;
    ~Final2x58() = default;
    Final2x58(const Final2x58&) = delete;
    Final2x58& operator=(const Final2x58&) = delete;
    Final2x58(Final2x58&&) noexcept = default;
    Final2x58& operator=(Final2x58&&) noexcept = default;
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

class Final2x59 {
public:
    Final2x59() = default;
    ~Final2x59() = default;
    Final2x59(const Final2x59&) = delete;
    Final2x59& operator=(const Final2x59&) = delete;
    Final2x59(Final2x59&&) noexcept = default;
    Final2x59& operator=(Final2x59&&) noexcept = default;
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

class Final2x60 {
public:
    Final2x60() = default;
    ~Final2x60() = default;
    Final2x60(const Final2x60&) = delete;
    Final2x60& operator=(const Final2x60&) = delete;
    Final2x60(Final2x60&&) noexcept = default;
    Final2x60& operator=(Final2x60&&) noexcept = default;
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

class Final2x61 {
public:
    Final2x61() = default;
    ~Final2x61() = default;
    Final2x61(const Final2x61&) = delete;
    Final2x61& operator=(const Final2x61&) = delete;
    Final2x61(Final2x61&&) noexcept = default;
    Final2x61& operator=(Final2x61&&) noexcept = default;
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

class Final2x62 {
public:
    Final2x62() = default;
    ~Final2x62() = default;
    Final2x62(const Final2x62&) = delete;
    Final2x62& operator=(const Final2x62&) = delete;
    Final2x62(Final2x62&&) noexcept = default;
    Final2x62& operator=(Final2x62&&) noexcept = default;
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

class Final2x63 {
public:
    Final2x63() = default;
    ~Final2x63() = default;
    Final2x63(const Final2x63&) = delete;
    Final2x63& operator=(const Final2x63&) = delete;
    Final2x63(Final2x63&&) noexcept = default;
    Final2x63& operator=(Final2x63&&) noexcept = default;
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

class Final2x64 {
public:
    Final2x64() = default;
    ~Final2x64() = default;
    Final2x64(const Final2x64&) = delete;
    Final2x64& operator=(const Final2x64&) = delete;
    Final2x64(Final2x64&&) noexcept = default;
    Final2x64& operator=(Final2x64&&) noexcept = default;
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

class Final2x65 {
public:
    Final2x65() = default;
    ~Final2x65() = default;
    Final2x65(const Final2x65&) = delete;
    Final2x65& operator=(const Final2x65&) = delete;
    Final2x65(Final2x65&&) noexcept = default;
    Final2x65& operator=(Final2x65&&) noexcept = default;
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

class Final2x66 {
public:
    Final2x66() = default;
    ~Final2x66() = default;
    Final2x66(const Final2x66&) = delete;
    Final2x66& operator=(const Final2x66&) = delete;
    Final2x66(Final2x66&&) noexcept = default;
    Final2x66& operator=(Final2x66&&) noexcept = default;
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

class Final2x67 {
public:
    Final2x67() = default;
    ~Final2x67() = default;
    Final2x67(const Final2x67&) = delete;
    Final2x67& operator=(const Final2x67&) = delete;
    Final2x67(Final2x67&&) noexcept = default;
    Final2x67& operator=(Final2x67&&) noexcept = default;
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

class Final2x68 {
public:
    Final2x68() = default;
    ~Final2x68() = default;
    Final2x68(const Final2x68&) = delete;
    Final2x68& operator=(const Final2x68&) = delete;
    Final2x68(Final2x68&&) noexcept = default;
    Final2x68& operator=(Final2x68&&) noexcept = default;
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

class Final2x69 {
public:
    Final2x69() = default;
    ~Final2x69() = default;
    Final2x69(const Final2x69&) = delete;
    Final2x69& operator=(const Final2x69&) = delete;
    Final2x69(Final2x69&&) noexcept = default;
    Final2x69& operator=(Final2x69&&) noexcept = default;
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

class Final2x70 {
public:
    Final2x70() = default;
    ~Final2x70() = default;
    Final2x70(const Final2x70&) = delete;
    Final2x70& operator=(const Final2x70&) = delete;
    Final2x70(Final2x70&&) noexcept = default;
    Final2x70& operator=(Final2x70&&) noexcept = default;
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

class Final2x71 {
public:
    Final2x71() = default;
    ~Final2x71() = default;
    Final2x71(const Final2x71&) = delete;
    Final2x71& operator=(const Final2x71&) = delete;
    Final2x71(Final2x71&&) noexcept = default;
    Final2x71& operator=(Final2x71&&) noexcept = default;
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

class Final2x72 {
public:
    Final2x72() = default;
    ~Final2x72() = default;
    Final2x72(const Final2x72&) = delete;
    Final2x72& operator=(const Final2x72&) = delete;
    Final2x72(Final2x72&&) noexcept = default;
    Final2x72& operator=(Final2x72&&) noexcept = default;
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

class Final2x73 {
public:
    Final2x73() = default;
    ~Final2x73() = default;
    Final2x73(const Final2x73&) = delete;
    Final2x73& operator=(const Final2x73&) = delete;
    Final2x73(Final2x73&&) noexcept = default;
    Final2x73& operator=(Final2x73&&) noexcept = default;
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

class Final2x74 {
public:
    Final2x74() = default;
    ~Final2x74() = default;
    Final2x74(const Final2x74&) = delete;
    Final2x74& operator=(const Final2x74&) = delete;
    Final2x74(Final2x74&&) noexcept = default;
    Final2x74& operator=(Final2x74&&) noexcept = default;
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

class Final2x75 {
public:
    Final2x75() = default;
    ~Final2x75() = default;
    Final2x75(const Final2x75&) = delete;
    Final2x75& operator=(const Final2x75&) = delete;
    Final2x75(Final2x75&&) noexcept = default;
    Final2x75& operator=(Final2x75&&) noexcept = default;
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

class Final2x76 {
public:
    Final2x76() = default;
    ~Final2x76() = default;
    Final2x76(const Final2x76&) = delete;
    Final2x76& operator=(const Final2x76&) = delete;
    Final2x76(Final2x76&&) noexcept = default;
    Final2x76& operator=(Final2x76&&) noexcept = default;
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

class Final2x77 {
public:
    Final2x77() = default;
    ~Final2x77() = default;
    Final2x77(const Final2x77&) = delete;
    Final2x77& operator=(const Final2x77&) = delete;
    Final2x77(Final2x77&&) noexcept = default;
    Final2x77& operator=(Final2x77&&) noexcept = default;
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

class Final2x78 {
public:
    Final2x78() = default;
    ~Final2x78() = default;
    Final2x78(const Final2x78&) = delete;
    Final2x78& operator=(const Final2x78&) = delete;
    Final2x78(Final2x78&&) noexcept = default;
    Final2x78& operator=(Final2x78&&) noexcept = default;
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

class Final2x79 {
public:
    Final2x79() = default;
    ~Final2x79() = default;
    Final2x79(const Final2x79&) = delete;
    Final2x79& operator=(const Final2x79&) = delete;
    Final2x79(Final2x79&&) noexcept = default;
    Final2x79& operator=(Final2x79&&) noexcept = default;
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

class Final2x80 {
public:
    Final2x80() = default;
    ~Final2x80() = default;
    Final2x80(const Final2x80&) = delete;
    Final2x80& operator=(const Final2x80&) = delete;
    Final2x80(Final2x80&&) noexcept = default;
    Final2x80& operator=(Final2x80&&) noexcept = default;
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

class Final2x81 {
public:
    Final2x81() = default;
    ~Final2x81() = default;
    Final2x81(const Final2x81&) = delete;
    Final2x81& operator=(const Final2x81&) = delete;
    Final2x81(Final2x81&&) noexcept = default;
    Final2x81& operator=(Final2x81&&) noexcept = default;
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

class Final2x82 {
public:
    Final2x82() = default;
    ~Final2x82() = default;
    Final2x82(const Final2x82&) = delete;
    Final2x82& operator=(const Final2x82&) = delete;
    Final2x82(Final2x82&&) noexcept = default;
    Final2x82& operator=(Final2x82&&) noexcept = default;
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

class Final2x83 {
public:
    Final2x83() = default;
    ~Final2x83() = default;
    Final2x83(const Final2x83&) = delete;
    Final2x83& operator=(const Final2x83&) = delete;
    Final2x83(Final2x83&&) noexcept = default;
    Final2x83& operator=(Final2x83&&) noexcept = default;
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

class Final2x84 {
public:
    Final2x84() = default;
    ~Final2x84() = default;
    Final2x84(const Final2x84&) = delete;
    Final2x84& operator=(const Final2x84&) = delete;
    Final2x84(Final2x84&&) noexcept = default;
    Final2x84& operator=(Final2x84&&) noexcept = default;
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

class Final2x85 {
public:
    Final2x85() = default;
    ~Final2x85() = default;
    Final2x85(const Final2x85&) = delete;
    Final2x85& operator=(const Final2x85&) = delete;
    Final2x85(Final2x85&&) noexcept = default;
    Final2x85& operator=(Final2x85&&) noexcept = default;
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

class Final2x86 {
public:
    Final2x86() = default;
    ~Final2x86() = default;
    Final2x86(const Final2x86&) = delete;
    Final2x86& operator=(const Final2x86&) = delete;
    Final2x86(Final2x86&&) noexcept = default;
    Final2x86& operator=(Final2x86&&) noexcept = default;
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

class Final2x87 {
public:
    Final2x87() = default;
    ~Final2x87() = default;
    Final2x87(const Final2x87&) = delete;
    Final2x87& operator=(const Final2x87&) = delete;
    Final2x87(Final2x87&&) noexcept = default;
    Final2x87& operator=(Final2x87&&) noexcept = default;
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

class Final2x88 {
public:
    Final2x88() = default;
    ~Final2x88() = default;
    Final2x88(const Final2x88&) = delete;
    Final2x88& operator=(const Final2x88&) = delete;
    Final2x88(Final2x88&&) noexcept = default;
    Final2x88& operator=(Final2x88&&) noexcept = default;
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

class Final2x89 {
public:
    Final2x89() = default;
    ~Final2x89() = default;
    Final2x89(const Final2x89&) = delete;
    Final2x89& operator=(const Final2x89&) = delete;
    Final2x89(Final2x89&&) noexcept = default;
    Final2x89& operator=(Final2x89&&) noexcept = default;
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

class Final2x90 {
public:
    Final2x90() = default;
    ~Final2x90() = default;
    Final2x90(const Final2x90&) = delete;
    Final2x90& operator=(const Final2x90&) = delete;
    Final2x90(Final2x90&&) noexcept = default;
    Final2x90& operator=(Final2x90&&) noexcept = default;
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

class Final2x91 {
public:
    Final2x91() = default;
    ~Final2x91() = default;
    Final2x91(const Final2x91&) = delete;
    Final2x91& operator=(const Final2x91&) = delete;
    Final2x91(Final2x91&&) noexcept = default;
    Final2x91& operator=(Final2x91&&) noexcept = default;
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

class Final2x92 {
public:
    Final2x92() = default;
    ~Final2x92() = default;
    Final2x92(const Final2x92&) = delete;
    Final2x92& operator=(const Final2x92&) = delete;
    Final2x92(Final2x92&&) noexcept = default;
    Final2x92& operator=(Final2x92&&) noexcept = default;
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

class Final2x93 {
public:
    Final2x93() = default;
    ~Final2x93() = default;
    Final2x93(const Final2x93&) = delete;
    Final2x93& operator=(const Final2x93&) = delete;
    Final2x93(Final2x93&&) noexcept = default;
    Final2x93& operator=(Final2x93&&) noexcept = default;
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

class Final2x94 {
public:
    Final2x94() = default;
    ~Final2x94() = default;
    Final2x94(const Final2x94&) = delete;
    Final2x94& operator=(const Final2x94&) = delete;
    Final2x94(Final2x94&&) noexcept = default;
    Final2x94& operator=(Final2x94&&) noexcept = default;
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

class Final2x95 {
public:
    Final2x95() = default;
    ~Final2x95() = default;
    Final2x95(const Final2x95&) = delete;
    Final2x95& operator=(const Final2x95&) = delete;
    Final2x95(Final2x95&&) noexcept = default;
    Final2x95& operator=(Final2x95&&) noexcept = default;
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

class Final2x96 {
public:
    Final2x96() = default;
    ~Final2x96() = default;
    Final2x96(const Final2x96&) = delete;
    Final2x96& operator=(const Final2x96&) = delete;
    Final2x96(Final2x96&&) noexcept = default;
    Final2x96& operator=(Final2x96&&) noexcept = default;
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

class Final2x97 {
public:
    Final2x97() = default;
    ~Final2x97() = default;
    Final2x97(const Final2x97&) = delete;
    Final2x97& operator=(const Final2x97&) = delete;
    Final2x97(Final2x97&&) noexcept = default;
    Final2x97& operator=(Final2x97&&) noexcept = default;
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

class Final2x98 {
public:
    Final2x98() = default;
    ~Final2x98() = default;
    Final2x98(const Final2x98&) = delete;
    Final2x98& operator=(const Final2x98&) = delete;
    Final2x98(Final2x98&&) noexcept = default;
    Final2x98& operator=(Final2x98&&) noexcept = default;
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

class Final2x99 {
public:
    Final2x99() = default;
    ~Final2x99() = default;
    Final2x99(const Final2x99&) = delete;
    Final2x99& operator=(const Final2x99&) = delete;
    Final2x99(Final2x99&&) noexcept = default;
    Final2x99& operator=(Final2x99&&) noexcept = default;
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

class Final2x100 {
public:
    Final2x100() = default;
    ~Final2x100() = default;
    Final2x100(const Final2x100&) = delete;
    Final2x100& operator=(const Final2x100&) = delete;
    Final2x100(Final2x100&&) noexcept = default;
    Final2x100& operator=(Final2x100&&) noexcept = default;
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

class Final2x101 {
public:
    Final2x101() = default;
    ~Final2x101() = default;
    Final2x101(const Final2x101&) = delete;
    Final2x101& operator=(const Final2x101&) = delete;
    Final2x101(Final2x101&&) noexcept = default;
    Final2x101& operator=(Final2x101&&) noexcept = default;
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

class Final2x102 {
public:
    Final2x102() = default;
    ~Final2x102() = default;
    Final2x102(const Final2x102&) = delete;
    Final2x102& operator=(const Final2x102&) = delete;
    Final2x102(Final2x102&&) noexcept = default;
    Final2x102& operator=(Final2x102&&) noexcept = default;
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

class Final2x103 {
public:
    Final2x103() = default;
    ~Final2x103() = default;
    Final2x103(const Final2x103&) = delete;
    Final2x103& operator=(const Final2x103&) = delete;
    Final2x103(Final2x103&&) noexcept = default;
    Final2x103& operator=(Final2x103&&) noexcept = default;
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

class Final2x104 {
public:
    Final2x104() = default;
    ~Final2x104() = default;
    Final2x104(const Final2x104&) = delete;
    Final2x104& operator=(const Final2x104&) = delete;
    Final2x104(Final2x104&&) noexcept = default;
    Final2x104& operator=(Final2x104&&) noexcept = default;
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

class Final2x105 {
public:
    Final2x105() = default;
    ~Final2x105() = default;
    Final2x105(const Final2x105&) = delete;
    Final2x105& operator=(const Final2x105&) = delete;
    Final2x105(Final2x105&&) noexcept = default;
    Final2x105& operator=(Final2x105&&) noexcept = default;
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

class Final2x106 {
public:
    Final2x106() = default;
    ~Final2x106() = default;
    Final2x106(const Final2x106&) = delete;
    Final2x106& operator=(const Final2x106&) = delete;
    Final2x106(Final2x106&&) noexcept = default;
    Final2x106& operator=(Final2x106&&) noexcept = default;
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

class Final2x107 {
public:
    Final2x107() = default;
    ~Final2x107() = default;
    Final2x107(const Final2x107&) = delete;
    Final2x107& operator=(const Final2x107&) = delete;
    Final2x107(Final2x107&&) noexcept = default;
    Final2x107& operator=(Final2x107&&) noexcept = default;
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

class Final2x108 {
public:
    Final2x108() = default;
    ~Final2x108() = default;
    Final2x108(const Final2x108&) = delete;
    Final2x108& operator=(const Final2x108&) = delete;
    Final2x108(Final2x108&&) noexcept = default;
    Final2x108& operator=(Final2x108&&) noexcept = default;
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

class Final2x109 {
public:
    Final2x109() = default;
    ~Final2x109() = default;
    Final2x109(const Final2x109&) = delete;
    Final2x109& operator=(const Final2x109&) = delete;
    Final2x109(Final2x109&&) noexcept = default;
    Final2x109& operator=(Final2x109&&) noexcept = default;
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

class Final2x110 {
public:
    Final2x110() = default;
    ~Final2x110() = default;
    Final2x110(const Final2x110&) = delete;
    Final2x110& operator=(const Final2x110&) = delete;
    Final2x110(Final2x110&&) noexcept = default;
    Final2x110& operator=(Final2x110&&) noexcept = default;
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

class Final2x111 {
public:
    Final2x111() = default;
    ~Final2x111() = default;
    Final2x111(const Final2x111&) = delete;
    Final2x111& operator=(const Final2x111&) = delete;
    Final2x111(Final2x111&&) noexcept = default;
    Final2x111& operator=(Final2x111&&) noexcept = default;
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

class Final2x112 {
public:
    Final2x112() = default;
    ~Final2x112() = default;
    Final2x112(const Final2x112&) = delete;
    Final2x112& operator=(const Final2x112&) = delete;
    Final2x112(Final2x112&&) noexcept = default;
    Final2x112& operator=(Final2x112&&) noexcept = default;
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

class Final2x113 {
public:
    Final2x113() = default;
    ~Final2x113() = default;
    Final2x113(const Final2x113&) = delete;
    Final2x113& operator=(const Final2x113&) = delete;
    Final2x113(Final2x113&&) noexcept = default;
    Final2x113& operator=(Final2x113&&) noexcept = default;
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

class Final2x114 {
public:
    Final2x114() = default;
    ~Final2x114() = default;
    Final2x114(const Final2x114&) = delete;
    Final2x114& operator=(const Final2x114&) = delete;
    Final2x114(Final2x114&&) noexcept = default;
    Final2x114& operator=(Final2x114&&) noexcept = default;
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

class Final2x115 {
public:
    Final2x115() = default;
    ~Final2x115() = default;
    Final2x115(const Final2x115&) = delete;
    Final2x115& operator=(const Final2x115&) = delete;
    Final2x115(Final2x115&&) noexcept = default;
    Final2x115& operator=(Final2x115&&) noexcept = default;
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

class Final2x116 {
public:
    Final2x116() = default;
    ~Final2x116() = default;
    Final2x116(const Final2x116&) = delete;
    Final2x116& operator=(const Final2x116&) = delete;
    Final2x116(Final2x116&&) noexcept = default;
    Final2x116& operator=(Final2x116&&) noexcept = default;
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

class Final2x117 {
public:
    Final2x117() = default;
    ~Final2x117() = default;
    Final2x117(const Final2x117&) = delete;
    Final2x117& operator=(const Final2x117&) = delete;
    Final2x117(Final2x117&&) noexcept = default;
    Final2x117& operator=(Final2x117&&) noexcept = default;
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

class Final2x118 {
public:
    Final2x118() = default;
    ~Final2x118() = default;
    Final2x118(const Final2x118&) = delete;
    Final2x118& operator=(const Final2x118&) = delete;
    Final2x118(Final2x118&&) noexcept = default;
    Final2x118& operator=(Final2x118&&) noexcept = default;
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

class Final2x119 {
public:
    Final2x119() = default;
    ~Final2x119() = default;
    Final2x119(const Final2x119&) = delete;
    Final2x119& operator=(const Final2x119&) = delete;
    Final2x119(Final2x119&&) noexcept = default;
    Final2x119& operator=(Final2x119&&) noexcept = default;
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

class Final2x120 {
public:
    Final2x120() = default;
    ~Final2x120() = default;
    Final2x120(const Final2x120&) = delete;
    Final2x120& operator=(const Final2x120&) = delete;
    Final2x120(Final2x120&&) noexcept = default;
    Final2x120& operator=(Final2x120&&) noexcept = default;
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

class Final2x121 {
public:
    Final2x121() = default;
    ~Final2x121() = default;
    Final2x121(const Final2x121&) = delete;
    Final2x121& operator=(const Final2x121&) = delete;
    Final2x121(Final2x121&&) noexcept = default;
    Final2x121& operator=(Final2x121&&) noexcept = default;
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

class Final2x122 {
public:
    Final2x122() = default;
    ~Final2x122() = default;
    Final2x122(const Final2x122&) = delete;
    Final2x122& operator=(const Final2x122&) = delete;
    Final2x122(Final2x122&&) noexcept = default;
    Final2x122& operator=(Final2x122&&) noexcept = default;
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

class Final2x123 {
public:
    Final2x123() = default;
    ~Final2x123() = default;
    Final2x123(const Final2x123&) = delete;
    Final2x123& operator=(const Final2x123&) = delete;
    Final2x123(Final2x123&&) noexcept = default;
    Final2x123& operator=(Final2x123&&) noexcept = default;
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

class Final2x124 {
public:
    Final2x124() = default;
    ~Final2x124() = default;
    Final2x124(const Final2x124&) = delete;
    Final2x124& operator=(const Final2x124&) = delete;
    Final2x124(Final2x124&&) noexcept = default;
    Final2x124& operator=(Final2x124&&) noexcept = default;
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

class Final2x125 {
public:
    Final2x125() = default;
    ~Final2x125() = default;
    Final2x125(const Final2x125&) = delete;
    Final2x125& operator=(const Final2x125&) = delete;
    Final2x125(Final2x125&&) noexcept = default;
    Final2x125& operator=(Final2x125&&) noexcept = default;
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

class Final2x126 {
public:
    Final2x126() = default;
    ~Final2x126() = default;
    Final2x126(const Final2x126&) = delete;
    Final2x126& operator=(const Final2x126&) = delete;
    Final2x126(Final2x126&&) noexcept = default;
    Final2x126& operator=(Final2x126&&) noexcept = default;
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

class Final2x127 {
public:
    Final2x127() = default;
    ~Final2x127() = default;
    Final2x127(const Final2x127&) = delete;
    Final2x127& operator=(const Final2x127&) = delete;
    Final2x127(Final2x127&&) noexcept = default;
    Final2x127& operator=(Final2x127&&) noexcept = default;
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

class Final2x128 {
public:
    Final2x128() = default;
    ~Final2x128() = default;
    Final2x128(const Final2x128&) = delete;
    Final2x128& operator=(const Final2x128&) = delete;
    Final2x128(Final2x128&&) noexcept = default;
    Final2x128& operator=(Final2x128&&) noexcept = default;
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

class Final2x129 {
public:
    Final2x129() = default;
    ~Final2x129() = default;
    Final2x129(const Final2x129&) = delete;
    Final2x129& operator=(const Final2x129&) = delete;
    Final2x129(Final2x129&&) noexcept = default;
    Final2x129& operator=(Final2x129&&) noexcept = default;
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

class Final2x130 {
public:
    Final2x130() = default;
    ~Final2x130() = default;
    Final2x130(const Final2x130&) = delete;
    Final2x130& operator=(const Final2x130&) = delete;
    Final2x130(Final2x130&&) noexcept = default;
    Final2x130& operator=(Final2x130&&) noexcept = default;
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

class Final2x131 {
public:
    Final2x131() = default;
    ~Final2x131() = default;
    Final2x131(const Final2x131&) = delete;
    Final2x131& operator=(const Final2x131&) = delete;
    Final2x131(Final2x131&&) noexcept = default;
    Final2x131& operator=(Final2x131&&) noexcept = default;
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

class Final2x132 {
public:
    Final2x132() = default;
    ~Final2x132() = default;
    Final2x132(const Final2x132&) = delete;
    Final2x132& operator=(const Final2x132&) = delete;
    Final2x132(Final2x132&&) noexcept = default;
    Final2x132& operator=(Final2x132&&) noexcept = default;
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

class Final2x133 {
public:
    Final2x133() = default;
    ~Final2x133() = default;
    Final2x133(const Final2x133&) = delete;
    Final2x133& operator=(const Final2x133&) = delete;
    Final2x133(Final2x133&&) noexcept = default;
    Final2x133& operator=(Final2x133&&) noexcept = default;
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

class Final2x134 {
public:
    Final2x134() = default;
    ~Final2x134() = default;
    Final2x134(const Final2x134&) = delete;
    Final2x134& operator=(const Final2x134&) = delete;
    Final2x134(Final2x134&&) noexcept = default;
    Final2x134& operator=(Final2x134&&) noexcept = default;
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

class Final2x135 {
public:
    Final2x135() = default;
    ~Final2x135() = default;
    Final2x135(const Final2x135&) = delete;
    Final2x135& operator=(const Final2x135&) = delete;
    Final2x135(Final2x135&&) noexcept = default;
    Final2x135& operator=(Final2x135&&) noexcept = default;
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

class Final2x136 {
public:
    Final2x136() = default;
    ~Final2x136() = default;
    Final2x136(const Final2x136&) = delete;
    Final2x136& operator=(const Final2x136&) = delete;
    Final2x136(Final2x136&&) noexcept = default;
    Final2x136& operator=(Final2x136&&) noexcept = default;
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

class Final2x137 {
public:
    Final2x137() = default;
    ~Final2x137() = default;
    Final2x137(const Final2x137&) = delete;
    Final2x137& operator=(const Final2x137&) = delete;
    Final2x137(Final2x137&&) noexcept = default;
    Final2x137& operator=(Final2x137&&) noexcept = default;
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

class Final2x138 {
public:
    Final2x138() = default;
    ~Final2x138() = default;
    Final2x138(const Final2x138&) = delete;
    Final2x138& operator=(const Final2x138&) = delete;
    Final2x138(Final2x138&&) noexcept = default;
    Final2x138& operator=(Final2x138&&) noexcept = default;
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

class Final2x139 {
public:
    Final2x139() = default;
    ~Final2x139() = default;
    Final2x139(const Final2x139&) = delete;
    Final2x139& operator=(const Final2x139&) = delete;
    Final2x139(Final2x139&&) noexcept = default;
    Final2x139& operator=(Final2x139&&) noexcept = default;
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

class Final2x140 {
public:
    Final2x140() = default;
    ~Final2x140() = default;
    Final2x140(const Final2x140&) = delete;
    Final2x140& operator=(const Final2x140&) = delete;
    Final2x140(Final2x140&&) noexcept = default;
    Final2x140& operator=(Final2x140&&) noexcept = default;
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

class Final2x141 {
public:
    Final2x141() = default;
    ~Final2x141() = default;
    Final2x141(const Final2x141&) = delete;
    Final2x141& operator=(const Final2x141&) = delete;
    Final2x141(Final2x141&&) noexcept = default;
    Final2x141& operator=(Final2x141&&) noexcept = default;
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

class Final2x142 {
public:
    Final2x142() = default;
    ~Final2x142() = default;
    Final2x142(const Final2x142&) = delete;
    Final2x142& operator=(const Final2x142&) = delete;
    Final2x142(Final2x142&&) noexcept = default;
    Final2x142& operator=(Final2x142&&) noexcept = default;
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

class Final2x143 {
public:
    Final2x143() = default;
    ~Final2x143() = default;
    Final2x143(const Final2x143&) = delete;
    Final2x143& operator=(const Final2x143&) = delete;
    Final2x143(Final2x143&&) noexcept = default;
    Final2x143& operator=(Final2x143&&) noexcept = default;
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

class Final2x144 {
public:
    Final2x144() = default;
    ~Final2x144() = default;
    Final2x144(const Final2x144&) = delete;
    Final2x144& operator=(const Final2x144&) = delete;
    Final2x144(Final2x144&&) noexcept = default;
    Final2x144& operator=(Final2x144&&) noexcept = default;
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

class Final2x145 {
public:
    Final2x145() = default;
    ~Final2x145() = default;
    Final2x145(const Final2x145&) = delete;
    Final2x145& operator=(const Final2x145&) = delete;
    Final2x145(Final2x145&&) noexcept = default;
    Final2x145& operator=(Final2x145&&) noexcept = default;
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

class Final2x146 {
public:
    Final2x146() = default;
    ~Final2x146() = default;
    Final2x146(const Final2x146&) = delete;
    Final2x146& operator=(const Final2x146&) = delete;
    Final2x146(Final2x146&&) noexcept = default;
    Final2x146& operator=(Final2x146&&) noexcept = default;
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

class Final2x147 {
public:
    Final2x147() = default;
    ~Final2x147() = default;
    Final2x147(const Final2x147&) = delete;
    Final2x147& operator=(const Final2x147&) = delete;
    Final2x147(Final2x147&&) noexcept = default;
    Final2x147& operator=(Final2x147&&) noexcept = default;
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

class Final2x148 {
public:
    Final2x148() = default;
    ~Final2x148() = default;
    Final2x148(const Final2x148&) = delete;
    Final2x148& operator=(const Final2x148&) = delete;
    Final2x148(Final2x148&&) noexcept = default;
    Final2x148& operator=(Final2x148&&) noexcept = default;
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

class Final2x149 {
public:
    Final2x149() = default;
    ~Final2x149() = default;
    Final2x149(const Final2x149&) = delete;
    Final2x149& operator=(const Final2x149&) = delete;
    Final2x149(Final2x149&&) noexcept = default;
    Final2x149& operator=(Final2x149&&) noexcept = default;
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

class Final2x150 {
public:
    Final2x150() = default;
    ~Final2x150() = default;
    Final2x150(const Final2x150&) = delete;
    Final2x150& operator=(const Final2x150&) = delete;
    Final2x150(Final2x150&&) noexcept = default;
    Final2x150& operator=(Final2x150&&) noexcept = default;
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

class Final2x151 {
public:
    Final2x151() = default;
    ~Final2x151() = default;
    Final2x151(const Final2x151&) = delete;
    Final2x151& operator=(const Final2x151&) = delete;
    Final2x151(Final2x151&&) noexcept = default;
    Final2x151& operator=(Final2x151&&) noexcept = default;
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

class Final2x152 {
public:
    Final2x152() = default;
    ~Final2x152() = default;
    Final2x152(const Final2x152&) = delete;
    Final2x152& operator=(const Final2x152&) = delete;
    Final2x152(Final2x152&&) noexcept = default;
    Final2x152& operator=(Final2x152&&) noexcept = default;
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

class Final2x153 {
public:
    Final2x153() = default;
    ~Final2x153() = default;
    Final2x153(const Final2x153&) = delete;
    Final2x153& operator=(const Final2x153&) = delete;
    Final2x153(Final2x153&&) noexcept = default;
    Final2x153& operator=(Final2x153&&) noexcept = default;
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

class Final2x154 {
public:
    Final2x154() = default;
    ~Final2x154() = default;
    Final2x154(const Final2x154&) = delete;
    Final2x154& operator=(const Final2x154&) = delete;
    Final2x154(Final2x154&&) noexcept = default;
    Final2x154& operator=(Final2x154&&) noexcept = default;
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

class Final2x155 {
public:
    Final2x155() = default;
    ~Final2x155() = default;
    Final2x155(const Final2x155&) = delete;
    Final2x155& operator=(const Final2x155&) = delete;
    Final2x155(Final2x155&&) noexcept = default;
    Final2x155& operator=(Final2x155&&) noexcept = default;
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

class Final2x156 {
public:
    Final2x156() = default;
    ~Final2x156() = default;
    Final2x156(const Final2x156&) = delete;
    Final2x156& operator=(const Final2x156&) = delete;
    Final2x156(Final2x156&&) noexcept = default;
    Final2x156& operator=(Final2x156&&) noexcept = default;
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

class Final2x157 {
public:
    Final2x157() = default;
    ~Final2x157() = default;
    Final2x157(const Final2x157&) = delete;
    Final2x157& operator=(const Final2x157&) = delete;
    Final2x157(Final2x157&&) noexcept = default;
    Final2x157& operator=(Final2x157&&) noexcept = default;
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

class Final2x158 {
public:
    Final2x158() = default;
    ~Final2x158() = default;
    Final2x158(const Final2x158&) = delete;
    Final2x158& operator=(const Final2x158&) = delete;
    Final2x158(Final2x158&&) noexcept = default;
    Final2x158& operator=(Final2x158&&) noexcept = default;
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

class Final2x159 {
public:
    Final2x159() = default;
    ~Final2x159() = default;
    Final2x159(const Final2x159&) = delete;
    Final2x159& operator=(const Final2x159&) = delete;
    Final2x159(Final2x159&&) noexcept = default;
    Final2x159& operator=(Final2x159&&) noexcept = default;
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

class Final2x160 {
public:
    Final2x160() = default;
    ~Final2x160() = default;
    Final2x160(const Final2x160&) = delete;
    Final2x160& operator=(const Final2x160&) = delete;
    Final2x160(Final2x160&&) noexcept = default;
    Final2x160& operator=(Final2x160&&) noexcept = default;
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

class Final2x161 {
public:
    Final2x161() = default;
    ~Final2x161() = default;
    Final2x161(const Final2x161&) = delete;
    Final2x161& operator=(const Final2x161&) = delete;
    Final2x161(Final2x161&&) noexcept = default;
    Final2x161& operator=(Final2x161&&) noexcept = default;
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

class Final2x162 {
public:
    Final2x162() = default;
    ~Final2x162() = default;
    Final2x162(const Final2x162&) = delete;
    Final2x162& operator=(const Final2x162&) = delete;
    Final2x162(Final2x162&&) noexcept = default;
    Final2x162& operator=(Final2x162&&) noexcept = default;
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

class Final2x163 {
public:
    Final2x163() = default;
    ~Final2x163() = default;
    Final2x163(const Final2x163&) = delete;
    Final2x163& operator=(const Final2x163&) = delete;
    Final2x163(Final2x163&&) noexcept = default;
    Final2x163& operator=(Final2x163&&) noexcept = default;
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

class Final2x164 {
public:
    Final2x164() = default;
    ~Final2x164() = default;
    Final2x164(const Final2x164&) = delete;
    Final2x164& operator=(const Final2x164&) = delete;
    Final2x164(Final2x164&&) noexcept = default;
    Final2x164& operator=(Final2x164&&) noexcept = default;
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

class Final2x165 {
public:
    Final2x165() = default;
    ~Final2x165() = default;
    Final2x165(const Final2x165&) = delete;
    Final2x165& operator=(const Final2x165&) = delete;
    Final2x165(Final2x165&&) noexcept = default;
    Final2x165& operator=(Final2x165&&) noexcept = default;
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

class Final2x166 {
public:
    Final2x166() = default;
    ~Final2x166() = default;
    Final2x166(const Final2x166&) = delete;
    Final2x166& operator=(const Final2x166&) = delete;
    Final2x166(Final2x166&&) noexcept = default;
    Final2x166& operator=(Final2x166&&) noexcept = default;
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

class Final2x167 {
public:
    Final2x167() = default;
    ~Final2x167() = default;
    Final2x167(const Final2x167&) = delete;
    Final2x167& operator=(const Final2x167&) = delete;
    Final2x167(Final2x167&&) noexcept = default;
    Final2x167& operator=(Final2x167&&) noexcept = default;
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

class Final2x168 {
public:
    Final2x168() = default;
    ~Final2x168() = default;
    Final2x168(const Final2x168&) = delete;
    Final2x168& operator=(const Final2x168&) = delete;
    Final2x168(Final2x168&&) noexcept = default;
    Final2x168& operator=(Final2x168&&) noexcept = default;
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

class Final2x169 {
public:
    Final2x169() = default;
    ~Final2x169() = default;
    Final2x169(const Final2x169&) = delete;
    Final2x169& operator=(const Final2x169&) = delete;
    Final2x169(Final2x169&&) noexcept = default;
    Final2x169& operator=(Final2x169&&) noexcept = default;
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

class Final2x170 {
public:
    Final2x170() = default;
    ~Final2x170() = default;
    Final2x170(const Final2x170&) = delete;
    Final2x170& operator=(const Final2x170&) = delete;
    Final2x170(Final2x170&&) noexcept = default;
    Final2x170& operator=(Final2x170&&) noexcept = default;
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

class Final2x171 {
public:
    Final2x171() = default;
    ~Final2x171() = default;
    Final2x171(const Final2x171&) = delete;
    Final2x171& operator=(const Final2x171&) = delete;
    Final2x171(Final2x171&&) noexcept = default;
    Final2x171& operator=(Final2x171&&) noexcept = default;
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

class Final2x172 {
public:
    Final2x172() = default;
    ~Final2x172() = default;
    Final2x172(const Final2x172&) = delete;
    Final2x172& operator=(const Final2x172&) = delete;
    Final2x172(Final2x172&&) noexcept = default;
    Final2x172& operator=(Final2x172&&) noexcept = default;
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

class Final2x173 {
public:
    Final2x173() = default;
    ~Final2x173() = default;
    Final2x173(const Final2x173&) = delete;
    Final2x173& operator=(const Final2x173&) = delete;
    Final2x173(Final2x173&&) noexcept = default;
    Final2x173& operator=(Final2x173&&) noexcept = default;
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

class Final2x174 {
public:
    Final2x174() = default;
    ~Final2x174() = default;
    Final2x174(const Final2x174&) = delete;
    Final2x174& operator=(const Final2x174&) = delete;
    Final2x174(Final2x174&&) noexcept = default;
    Final2x174& operator=(Final2x174&&) noexcept = default;
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

class Final2x175 {
public:
    Final2x175() = default;
    ~Final2x175() = default;
    Final2x175(const Final2x175&) = delete;
    Final2x175& operator=(const Final2x175&) = delete;
    Final2x175(Final2x175&&) noexcept = default;
    Final2x175& operator=(Final2x175&&) noexcept = default;
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

class Final2x176 {
public:
    Final2x176() = default;
    ~Final2x176() = default;
    Final2x176(const Final2x176&) = delete;
    Final2x176& operator=(const Final2x176&) = delete;
    Final2x176(Final2x176&&) noexcept = default;
    Final2x176& operator=(Final2x176&&) noexcept = default;
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

class Final2x177 {
public:
    Final2x177() = default;
    ~Final2x177() = default;
    Final2x177(const Final2x177&) = delete;
    Final2x177& operator=(const Final2x177&) = delete;
    Final2x177(Final2x177&&) noexcept = default;
    Final2x177& operator=(Final2x177&&) noexcept = default;
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

class Final2x178 {
public:
    Final2x178() = default;
    ~Final2x178() = default;
    Final2x178(const Final2x178&) = delete;
    Final2x178& operator=(const Final2x178&) = delete;
    Final2x178(Final2x178&&) noexcept = default;
    Final2x178& operator=(Final2x178&&) noexcept = default;
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

class Final2x179 {
public:
    Final2x179() = default;
    ~Final2x179() = default;
    Final2x179(const Final2x179&) = delete;
    Final2x179& operator=(const Final2x179&) = delete;
    Final2x179(Final2x179&&) noexcept = default;
    Final2x179& operator=(Final2x179&&) noexcept = default;
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

class Final2x180 {
public:
    Final2x180() = default;
    ~Final2x180() = default;
    Final2x180(const Final2x180&) = delete;
    Final2x180& operator=(const Final2x180&) = delete;
    Final2x180(Final2x180&&) noexcept = default;
    Final2x180& operator=(Final2x180&&) noexcept = default;
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

class Final2x181 {
public:
    Final2x181() = default;
    ~Final2x181() = default;
    Final2x181(const Final2x181&) = delete;
    Final2x181& operator=(const Final2x181&) = delete;
    Final2x181(Final2x181&&) noexcept = default;
    Final2x181& operator=(Final2x181&&) noexcept = default;
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

class Final2x182 {
public:
    Final2x182() = default;
    ~Final2x182() = default;
    Final2x182(const Final2x182&) = delete;
    Final2x182& operator=(const Final2x182&) = delete;
    Final2x182(Final2x182&&) noexcept = default;
    Final2x182& operator=(Final2x182&&) noexcept = default;
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

class Final2x183 {
public:
    Final2x183() = default;
    ~Final2x183() = default;
    Final2x183(const Final2x183&) = delete;
    Final2x183& operator=(const Final2x183&) = delete;
    Final2x183(Final2x183&&) noexcept = default;
    Final2x183& operator=(Final2x183&&) noexcept = default;
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

class Final2x184 {
public:
    Final2x184() = default;
    ~Final2x184() = default;
    Final2x184(const Final2x184&) = delete;
    Final2x184& operator=(const Final2x184&) = delete;
    Final2x184(Final2x184&&) noexcept = default;
    Final2x184& operator=(Final2x184&&) noexcept = default;
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

class Final2x185 {
public:
    Final2x185() = default;
    ~Final2x185() = default;
    Final2x185(const Final2x185&) = delete;
    Final2x185& operator=(const Final2x185&) = delete;
    Final2x185(Final2x185&&) noexcept = default;
    Final2x185& operator=(Final2x185&&) noexcept = default;
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

class Final2x186 {
public:
    Final2x186() = default;
    ~Final2x186() = default;
    Final2x186(const Final2x186&) = delete;
    Final2x186& operator=(const Final2x186&) = delete;
    Final2x186(Final2x186&&) noexcept = default;
    Final2x186& operator=(Final2x186&&) noexcept = default;
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

class Final2x187 {
public:
    Final2x187() = default;
    ~Final2x187() = default;
    Final2x187(const Final2x187&) = delete;
    Final2x187& operator=(const Final2x187&) = delete;
    Final2x187(Final2x187&&) noexcept = default;
    Final2x187& operator=(Final2x187&&) noexcept = default;
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

class Final2x188 {
public:
    Final2x188() = default;
    ~Final2x188() = default;
    Final2x188(const Final2x188&) = delete;
    Final2x188& operator=(const Final2x188&) = delete;
    Final2x188(Final2x188&&) noexcept = default;
    Final2x188& operator=(Final2x188&&) noexcept = default;
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

class Final2x189 {
public:
    Final2x189() = default;
    ~Final2x189() = default;
    Final2x189(const Final2x189&) = delete;
    Final2x189& operator=(const Final2x189&) = delete;
    Final2x189(Final2x189&&) noexcept = default;
    Final2x189& operator=(Final2x189&&) noexcept = default;
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

class Final2x190 {
public:
    Final2x190() = default;
    ~Final2x190() = default;
    Final2x190(const Final2x190&) = delete;
    Final2x190& operator=(const Final2x190&) = delete;
    Final2x190(Final2x190&&) noexcept = default;
    Final2x190& operator=(Final2x190&&) noexcept = default;
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

class Final2x191 {
public:
    Final2x191() = default;
    ~Final2x191() = default;
    Final2x191(const Final2x191&) = delete;
    Final2x191& operator=(const Final2x191&) = delete;
    Final2x191(Final2x191&&) noexcept = default;
    Final2x191& operator=(Final2x191&&) noexcept = default;
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

class Final2x192 {
public:
    Final2x192() = default;
    ~Final2x192() = default;
    Final2x192(const Final2x192&) = delete;
    Final2x192& operator=(const Final2x192&) = delete;
    Final2x192(Final2x192&&) noexcept = default;
    Final2x192& operator=(Final2x192&&) noexcept = default;
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

class Final2x193 {
public:
    Final2x193() = default;
    ~Final2x193() = default;
    Final2x193(const Final2x193&) = delete;
    Final2x193& operator=(const Final2x193&) = delete;
    Final2x193(Final2x193&&) noexcept = default;
    Final2x193& operator=(Final2x193&&) noexcept = default;
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

class Final2x194 {
public:
    Final2x194() = default;
    ~Final2x194() = default;
    Final2x194(const Final2x194&) = delete;
    Final2x194& operator=(const Final2x194&) = delete;
    Final2x194(Final2x194&&) noexcept = default;
    Final2x194& operator=(Final2x194&&) noexcept = default;
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

class Final2x195 {
public:
    Final2x195() = default;
    ~Final2x195() = default;
    Final2x195(const Final2x195&) = delete;
    Final2x195& operator=(const Final2x195&) = delete;
    Final2x195(Final2x195&&) noexcept = default;
    Final2x195& operator=(Final2x195&&) noexcept = default;
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

class Final2x196 {
public:
    Final2x196() = default;
    ~Final2x196() = default;
    Final2x196(const Final2x196&) = delete;
    Final2x196& operator=(const Final2x196&) = delete;
    Final2x196(Final2x196&&) noexcept = default;
    Final2x196& operator=(Final2x196&&) noexcept = default;
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

class Final2x197 {
public:
    Final2x197() = default;
    ~Final2x197() = default;
    Final2x197(const Final2x197&) = delete;
    Final2x197& operator=(const Final2x197&) = delete;
    Final2x197(Final2x197&&) noexcept = default;
    Final2x197& operator=(Final2x197&&) noexcept = default;
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

class Final2x198 {
public:
    Final2x198() = default;
    ~Final2x198() = default;
    Final2x198(const Final2x198&) = delete;
    Final2x198& operator=(const Final2x198&) = delete;
    Final2x198(Final2x198&&) noexcept = default;
    Final2x198& operator=(Final2x198&&) noexcept = default;
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

class Final2x199 {
public:
    Final2x199() = default;
    ~Final2x199() = default;
    Final2x199(const Final2x199&) = delete;
    Final2x199& operator=(const Final2x199&) = delete;
    Final2x199(Final2x199&&) noexcept = default;
    Final2x199& operator=(Final2x199&&) noexcept = default;
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