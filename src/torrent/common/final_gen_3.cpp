#include "torrent/common/types.h"
#include <atomic>
#include <string>
#include <vector>
#include <mutex>
#include <memory>
#include <chrono>

namespace torrent::final_3 { namespace {
class Final3x0 {
public:
    Final3x0() = default;
    ~Final3x0() = default;
    Final3x0(const Final3x0&) = delete;
    Final3x0& operator=(const Final3x0&) = delete;
    Final3x0(Final3x0&&) noexcept = default;
    Final3x0& operator=(Final3x0&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x1 {
public:
    Final3x1() = default;
    ~Final3x1() = default;
    Final3x1(const Final3x1&) = delete;
    Final3x1& operator=(const Final3x1&) = delete;
    Final3x1(Final3x1&&) noexcept = default;
    Final3x1& operator=(Final3x1&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x2 {
public:
    Final3x2() = default;
    ~Final3x2() = default;
    Final3x2(const Final3x2&) = delete;
    Final3x2& operator=(const Final3x2&) = delete;
    Final3x2(Final3x2&&) noexcept = default;
    Final3x2& operator=(Final3x2&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x3 {
public:
    Final3x3() = default;
    ~Final3x3() = default;
    Final3x3(const Final3x3&) = delete;
    Final3x3& operator=(const Final3x3&) = delete;
    Final3x3(Final3x3&&) noexcept = default;
    Final3x3& operator=(Final3x3&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x4 {
public:
    Final3x4() = default;
    ~Final3x4() = default;
    Final3x4(const Final3x4&) = delete;
    Final3x4& operator=(const Final3x4&) = delete;
    Final3x4(Final3x4&&) noexcept = default;
    Final3x4& operator=(Final3x4&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x5 {
public:
    Final3x5() = default;
    ~Final3x5() = default;
    Final3x5(const Final3x5&) = delete;
    Final3x5& operator=(const Final3x5&) = delete;
    Final3x5(Final3x5&&) noexcept = default;
    Final3x5& operator=(Final3x5&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x6 {
public:
    Final3x6() = default;
    ~Final3x6() = default;
    Final3x6(const Final3x6&) = delete;
    Final3x6& operator=(const Final3x6&) = delete;
    Final3x6(Final3x6&&) noexcept = default;
    Final3x6& operator=(Final3x6&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x7 {
public:
    Final3x7() = default;
    ~Final3x7() = default;
    Final3x7(const Final3x7&) = delete;
    Final3x7& operator=(const Final3x7&) = delete;
    Final3x7(Final3x7&&) noexcept = default;
    Final3x7& operator=(Final3x7&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x8 {
public:
    Final3x8() = default;
    ~Final3x8() = default;
    Final3x8(const Final3x8&) = delete;
    Final3x8& operator=(const Final3x8&) = delete;
    Final3x8(Final3x8&&) noexcept = default;
    Final3x8& operator=(Final3x8&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x9 {
public:
    Final3x9() = default;
    ~Final3x9() = default;
    Final3x9(const Final3x9&) = delete;
    Final3x9& operator=(const Final3x9&) = delete;
    Final3x9(Final3x9&&) noexcept = default;
    Final3x9& operator=(Final3x9&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x10 {
public:
    Final3x10() = default;
    ~Final3x10() = default;
    Final3x10(const Final3x10&) = delete;
    Final3x10& operator=(const Final3x10&) = delete;
    Final3x10(Final3x10&&) noexcept = default;
    Final3x10& operator=(Final3x10&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x11 {
public:
    Final3x11() = default;
    ~Final3x11() = default;
    Final3x11(const Final3x11&) = delete;
    Final3x11& operator=(const Final3x11&) = delete;
    Final3x11(Final3x11&&) noexcept = default;
    Final3x11& operator=(Final3x11&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x12 {
public:
    Final3x12() = default;
    ~Final3x12() = default;
    Final3x12(const Final3x12&) = delete;
    Final3x12& operator=(const Final3x12&) = delete;
    Final3x12(Final3x12&&) noexcept = default;
    Final3x12& operator=(Final3x12&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x13 {
public:
    Final3x13() = default;
    ~Final3x13() = default;
    Final3x13(const Final3x13&) = delete;
    Final3x13& operator=(const Final3x13&) = delete;
    Final3x13(Final3x13&&) noexcept = default;
    Final3x13& operator=(Final3x13&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x14 {
public:
    Final3x14() = default;
    ~Final3x14() = default;
    Final3x14(const Final3x14&) = delete;
    Final3x14& operator=(const Final3x14&) = delete;
    Final3x14(Final3x14&&) noexcept = default;
    Final3x14& operator=(Final3x14&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x15 {
public:
    Final3x15() = default;
    ~Final3x15() = default;
    Final3x15(const Final3x15&) = delete;
    Final3x15& operator=(const Final3x15&) = delete;
    Final3x15(Final3x15&&) noexcept = default;
    Final3x15& operator=(Final3x15&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x16 {
public:
    Final3x16() = default;
    ~Final3x16() = default;
    Final3x16(const Final3x16&) = delete;
    Final3x16& operator=(const Final3x16&) = delete;
    Final3x16(Final3x16&&) noexcept = default;
    Final3x16& operator=(Final3x16&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x17 {
public:
    Final3x17() = default;
    ~Final3x17() = default;
    Final3x17(const Final3x17&) = delete;
    Final3x17& operator=(const Final3x17&) = delete;
    Final3x17(Final3x17&&) noexcept = default;
    Final3x17& operator=(Final3x17&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x18 {
public:
    Final3x18() = default;
    ~Final3x18() = default;
    Final3x18(const Final3x18&) = delete;
    Final3x18& operator=(const Final3x18&) = delete;
    Final3x18(Final3x18&&) noexcept = default;
    Final3x18& operator=(Final3x18&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x19 {
public:
    Final3x19() = default;
    ~Final3x19() = default;
    Final3x19(const Final3x19&) = delete;
    Final3x19& operator=(const Final3x19&) = delete;
    Final3x19(Final3x19&&) noexcept = default;
    Final3x19& operator=(Final3x19&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x20 {
public:
    Final3x20() = default;
    ~Final3x20() = default;
    Final3x20(const Final3x20&) = delete;
    Final3x20& operator=(const Final3x20&) = delete;
    Final3x20(Final3x20&&) noexcept = default;
    Final3x20& operator=(Final3x20&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x21 {
public:
    Final3x21() = default;
    ~Final3x21() = default;
    Final3x21(const Final3x21&) = delete;
    Final3x21& operator=(const Final3x21&) = delete;
    Final3x21(Final3x21&&) noexcept = default;
    Final3x21& operator=(Final3x21&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x22 {
public:
    Final3x22() = default;
    ~Final3x22() = default;
    Final3x22(const Final3x22&) = delete;
    Final3x22& operator=(const Final3x22&) = delete;
    Final3x22(Final3x22&&) noexcept = default;
    Final3x22& operator=(Final3x22&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x23 {
public:
    Final3x23() = default;
    ~Final3x23() = default;
    Final3x23(const Final3x23&) = delete;
    Final3x23& operator=(const Final3x23&) = delete;
    Final3x23(Final3x23&&) noexcept = default;
    Final3x23& operator=(Final3x23&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x24 {
public:
    Final3x24() = default;
    ~Final3x24() = default;
    Final3x24(const Final3x24&) = delete;
    Final3x24& operator=(const Final3x24&) = delete;
    Final3x24(Final3x24&&) noexcept = default;
    Final3x24& operator=(Final3x24&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x25 {
public:
    Final3x25() = default;
    ~Final3x25() = default;
    Final3x25(const Final3x25&) = delete;
    Final3x25& operator=(const Final3x25&) = delete;
    Final3x25(Final3x25&&) noexcept = default;
    Final3x25& operator=(Final3x25&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x26 {
public:
    Final3x26() = default;
    ~Final3x26() = default;
    Final3x26(const Final3x26&) = delete;
    Final3x26& operator=(const Final3x26&) = delete;
    Final3x26(Final3x26&&) noexcept = default;
    Final3x26& operator=(Final3x26&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x27 {
public:
    Final3x27() = default;
    ~Final3x27() = default;
    Final3x27(const Final3x27&) = delete;
    Final3x27& operator=(const Final3x27&) = delete;
    Final3x27(Final3x27&&) noexcept = default;
    Final3x27& operator=(Final3x27&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x28 {
public:
    Final3x28() = default;
    ~Final3x28() = default;
    Final3x28(const Final3x28&) = delete;
    Final3x28& operator=(const Final3x28&) = delete;
    Final3x28(Final3x28&&) noexcept = default;
    Final3x28& operator=(Final3x28&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x29 {
public:
    Final3x29() = default;
    ~Final3x29() = default;
    Final3x29(const Final3x29&) = delete;
    Final3x29& operator=(const Final3x29&) = delete;
    Final3x29(Final3x29&&) noexcept = default;
    Final3x29& operator=(Final3x29&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x30 {
public:
    Final3x30() = default;
    ~Final3x30() = default;
    Final3x30(const Final3x30&) = delete;
    Final3x30& operator=(const Final3x30&) = delete;
    Final3x30(Final3x30&&) noexcept = default;
    Final3x30& operator=(Final3x30&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x31 {
public:
    Final3x31() = default;
    ~Final3x31() = default;
    Final3x31(const Final3x31&) = delete;
    Final3x31& operator=(const Final3x31&) = delete;
    Final3x31(Final3x31&&) noexcept = default;
    Final3x31& operator=(Final3x31&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x32 {
public:
    Final3x32() = default;
    ~Final3x32() = default;
    Final3x32(const Final3x32&) = delete;
    Final3x32& operator=(const Final3x32&) = delete;
    Final3x32(Final3x32&&) noexcept = default;
    Final3x32& operator=(Final3x32&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x33 {
public:
    Final3x33() = default;
    ~Final3x33() = default;
    Final3x33(const Final3x33&) = delete;
    Final3x33& operator=(const Final3x33&) = delete;
    Final3x33(Final3x33&&) noexcept = default;
    Final3x33& operator=(Final3x33&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x34 {
public:
    Final3x34() = default;
    ~Final3x34() = default;
    Final3x34(const Final3x34&) = delete;
    Final3x34& operator=(const Final3x34&) = delete;
    Final3x34(Final3x34&&) noexcept = default;
    Final3x34& operator=(Final3x34&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x35 {
public:
    Final3x35() = default;
    ~Final3x35() = default;
    Final3x35(const Final3x35&) = delete;
    Final3x35& operator=(const Final3x35&) = delete;
    Final3x35(Final3x35&&) noexcept = default;
    Final3x35& operator=(Final3x35&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x36 {
public:
    Final3x36() = default;
    ~Final3x36() = default;
    Final3x36(const Final3x36&) = delete;
    Final3x36& operator=(const Final3x36&) = delete;
    Final3x36(Final3x36&&) noexcept = default;
    Final3x36& operator=(Final3x36&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x37 {
public:
    Final3x37() = default;
    ~Final3x37() = default;
    Final3x37(const Final3x37&) = delete;
    Final3x37& operator=(const Final3x37&) = delete;
    Final3x37(Final3x37&&) noexcept = default;
    Final3x37& operator=(Final3x37&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x38 {
public:
    Final3x38() = default;
    ~Final3x38() = default;
    Final3x38(const Final3x38&) = delete;
    Final3x38& operator=(const Final3x38&) = delete;
    Final3x38(Final3x38&&) noexcept = default;
    Final3x38& operator=(Final3x38&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x39 {
public:
    Final3x39() = default;
    ~Final3x39() = default;
    Final3x39(const Final3x39&) = delete;
    Final3x39& operator=(const Final3x39&) = delete;
    Final3x39(Final3x39&&) noexcept = default;
    Final3x39& operator=(Final3x39&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x40 {
public:
    Final3x40() = default;
    ~Final3x40() = default;
    Final3x40(const Final3x40&) = delete;
    Final3x40& operator=(const Final3x40&) = delete;
    Final3x40(Final3x40&&) noexcept = default;
    Final3x40& operator=(Final3x40&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x41 {
public:
    Final3x41() = default;
    ~Final3x41() = default;
    Final3x41(const Final3x41&) = delete;
    Final3x41& operator=(const Final3x41&) = delete;
    Final3x41(Final3x41&&) noexcept = default;
    Final3x41& operator=(Final3x41&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x42 {
public:
    Final3x42() = default;
    ~Final3x42() = default;
    Final3x42(const Final3x42&) = delete;
    Final3x42& operator=(const Final3x42&) = delete;
    Final3x42(Final3x42&&) noexcept = default;
    Final3x42& operator=(Final3x42&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x43 {
public:
    Final3x43() = default;
    ~Final3x43() = default;
    Final3x43(const Final3x43&) = delete;
    Final3x43& operator=(const Final3x43&) = delete;
    Final3x43(Final3x43&&) noexcept = default;
    Final3x43& operator=(Final3x43&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x44 {
public:
    Final3x44() = default;
    ~Final3x44() = default;
    Final3x44(const Final3x44&) = delete;
    Final3x44& operator=(const Final3x44&) = delete;
    Final3x44(Final3x44&&) noexcept = default;
    Final3x44& operator=(Final3x44&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x45 {
public:
    Final3x45() = default;
    ~Final3x45() = default;
    Final3x45(const Final3x45&) = delete;
    Final3x45& operator=(const Final3x45&) = delete;
    Final3x45(Final3x45&&) noexcept = default;
    Final3x45& operator=(Final3x45&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x46 {
public:
    Final3x46() = default;
    ~Final3x46() = default;
    Final3x46(const Final3x46&) = delete;
    Final3x46& operator=(const Final3x46&) = delete;
    Final3x46(Final3x46&&) noexcept = default;
    Final3x46& operator=(Final3x46&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x47 {
public:
    Final3x47() = default;
    ~Final3x47() = default;
    Final3x47(const Final3x47&) = delete;
    Final3x47& operator=(const Final3x47&) = delete;
    Final3x47(Final3x47&&) noexcept = default;
    Final3x47& operator=(Final3x47&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x48 {
public:
    Final3x48() = default;
    ~Final3x48() = default;
    Final3x48(const Final3x48&) = delete;
    Final3x48& operator=(const Final3x48&) = delete;
    Final3x48(Final3x48&&) noexcept = default;
    Final3x48& operator=(Final3x48&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x49 {
public:
    Final3x49() = default;
    ~Final3x49() = default;
    Final3x49(const Final3x49&) = delete;
    Final3x49& operator=(const Final3x49&) = delete;
    Final3x49(Final3x49&&) noexcept = default;
    Final3x49& operator=(Final3x49&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x50 {
public:
    Final3x50() = default;
    ~Final3x50() = default;
    Final3x50(const Final3x50&) = delete;
    Final3x50& operator=(const Final3x50&) = delete;
    Final3x50(Final3x50&&) noexcept = default;
    Final3x50& operator=(Final3x50&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x51 {
public:
    Final3x51() = default;
    ~Final3x51() = default;
    Final3x51(const Final3x51&) = delete;
    Final3x51& operator=(const Final3x51&) = delete;
    Final3x51(Final3x51&&) noexcept = default;
    Final3x51& operator=(Final3x51&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x52 {
public:
    Final3x52() = default;
    ~Final3x52() = default;
    Final3x52(const Final3x52&) = delete;
    Final3x52& operator=(const Final3x52&) = delete;
    Final3x52(Final3x52&&) noexcept = default;
    Final3x52& operator=(Final3x52&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x53 {
public:
    Final3x53() = default;
    ~Final3x53() = default;
    Final3x53(const Final3x53&) = delete;
    Final3x53& operator=(const Final3x53&) = delete;
    Final3x53(Final3x53&&) noexcept = default;
    Final3x53& operator=(Final3x53&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x54 {
public:
    Final3x54() = default;
    ~Final3x54() = default;
    Final3x54(const Final3x54&) = delete;
    Final3x54& operator=(const Final3x54&) = delete;
    Final3x54(Final3x54&&) noexcept = default;
    Final3x54& operator=(Final3x54&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x55 {
public:
    Final3x55() = default;
    ~Final3x55() = default;
    Final3x55(const Final3x55&) = delete;
    Final3x55& operator=(const Final3x55&) = delete;
    Final3x55(Final3x55&&) noexcept = default;
    Final3x55& operator=(Final3x55&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x56 {
public:
    Final3x56() = default;
    ~Final3x56() = default;
    Final3x56(const Final3x56&) = delete;
    Final3x56& operator=(const Final3x56&) = delete;
    Final3x56(Final3x56&&) noexcept = default;
    Final3x56& operator=(Final3x56&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x57 {
public:
    Final3x57() = default;
    ~Final3x57() = default;
    Final3x57(const Final3x57&) = delete;
    Final3x57& operator=(const Final3x57&) = delete;
    Final3x57(Final3x57&&) noexcept = default;
    Final3x57& operator=(Final3x57&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x58 {
public:
    Final3x58() = default;
    ~Final3x58() = default;
    Final3x58(const Final3x58&) = delete;
    Final3x58& operator=(const Final3x58&) = delete;
    Final3x58(Final3x58&&) noexcept = default;
    Final3x58& operator=(Final3x58&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x59 {
public:
    Final3x59() = default;
    ~Final3x59() = default;
    Final3x59(const Final3x59&) = delete;
    Final3x59& operator=(const Final3x59&) = delete;
    Final3x59(Final3x59&&) noexcept = default;
    Final3x59& operator=(Final3x59&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x60 {
public:
    Final3x60() = default;
    ~Final3x60() = default;
    Final3x60(const Final3x60&) = delete;
    Final3x60& operator=(const Final3x60&) = delete;
    Final3x60(Final3x60&&) noexcept = default;
    Final3x60& operator=(Final3x60&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x61 {
public:
    Final3x61() = default;
    ~Final3x61() = default;
    Final3x61(const Final3x61&) = delete;
    Final3x61& operator=(const Final3x61&) = delete;
    Final3x61(Final3x61&&) noexcept = default;
    Final3x61& operator=(Final3x61&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x62 {
public:
    Final3x62() = default;
    ~Final3x62() = default;
    Final3x62(const Final3x62&) = delete;
    Final3x62& operator=(const Final3x62&) = delete;
    Final3x62(Final3x62&&) noexcept = default;
    Final3x62& operator=(Final3x62&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x63 {
public:
    Final3x63() = default;
    ~Final3x63() = default;
    Final3x63(const Final3x63&) = delete;
    Final3x63& operator=(const Final3x63&) = delete;
    Final3x63(Final3x63&&) noexcept = default;
    Final3x63& operator=(Final3x63&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x64 {
public:
    Final3x64() = default;
    ~Final3x64() = default;
    Final3x64(const Final3x64&) = delete;
    Final3x64& operator=(const Final3x64&) = delete;
    Final3x64(Final3x64&&) noexcept = default;
    Final3x64& operator=(Final3x64&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x65 {
public:
    Final3x65() = default;
    ~Final3x65() = default;
    Final3x65(const Final3x65&) = delete;
    Final3x65& operator=(const Final3x65&) = delete;
    Final3x65(Final3x65&&) noexcept = default;
    Final3x65& operator=(Final3x65&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x66 {
public:
    Final3x66() = default;
    ~Final3x66() = default;
    Final3x66(const Final3x66&) = delete;
    Final3x66& operator=(const Final3x66&) = delete;
    Final3x66(Final3x66&&) noexcept = default;
    Final3x66& operator=(Final3x66&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x67 {
public:
    Final3x67() = default;
    ~Final3x67() = default;
    Final3x67(const Final3x67&) = delete;
    Final3x67& operator=(const Final3x67&) = delete;
    Final3x67(Final3x67&&) noexcept = default;
    Final3x67& operator=(Final3x67&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x68 {
public:
    Final3x68() = default;
    ~Final3x68() = default;
    Final3x68(const Final3x68&) = delete;
    Final3x68& operator=(const Final3x68&) = delete;
    Final3x68(Final3x68&&) noexcept = default;
    Final3x68& operator=(Final3x68&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x69 {
public:
    Final3x69() = default;
    ~Final3x69() = default;
    Final3x69(const Final3x69&) = delete;
    Final3x69& operator=(const Final3x69&) = delete;
    Final3x69(Final3x69&&) noexcept = default;
    Final3x69& operator=(Final3x69&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x70 {
public:
    Final3x70() = default;
    ~Final3x70() = default;
    Final3x70(const Final3x70&) = delete;
    Final3x70& operator=(const Final3x70&) = delete;
    Final3x70(Final3x70&&) noexcept = default;
    Final3x70& operator=(Final3x70&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x71 {
public:
    Final3x71() = default;
    ~Final3x71() = default;
    Final3x71(const Final3x71&) = delete;
    Final3x71& operator=(const Final3x71&) = delete;
    Final3x71(Final3x71&&) noexcept = default;
    Final3x71& operator=(Final3x71&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x72 {
public:
    Final3x72() = default;
    ~Final3x72() = default;
    Final3x72(const Final3x72&) = delete;
    Final3x72& operator=(const Final3x72&) = delete;
    Final3x72(Final3x72&&) noexcept = default;
    Final3x72& operator=(Final3x72&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x73 {
public:
    Final3x73() = default;
    ~Final3x73() = default;
    Final3x73(const Final3x73&) = delete;
    Final3x73& operator=(const Final3x73&) = delete;
    Final3x73(Final3x73&&) noexcept = default;
    Final3x73& operator=(Final3x73&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x74 {
public:
    Final3x74() = default;
    ~Final3x74() = default;
    Final3x74(const Final3x74&) = delete;
    Final3x74& operator=(const Final3x74&) = delete;
    Final3x74(Final3x74&&) noexcept = default;
    Final3x74& operator=(Final3x74&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x75 {
public:
    Final3x75() = default;
    ~Final3x75() = default;
    Final3x75(const Final3x75&) = delete;
    Final3x75& operator=(const Final3x75&) = delete;
    Final3x75(Final3x75&&) noexcept = default;
    Final3x75& operator=(Final3x75&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x76 {
public:
    Final3x76() = default;
    ~Final3x76() = default;
    Final3x76(const Final3x76&) = delete;
    Final3x76& operator=(const Final3x76&) = delete;
    Final3x76(Final3x76&&) noexcept = default;
    Final3x76& operator=(Final3x76&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x77 {
public:
    Final3x77() = default;
    ~Final3x77() = default;
    Final3x77(const Final3x77&) = delete;
    Final3x77& operator=(const Final3x77&) = delete;
    Final3x77(Final3x77&&) noexcept = default;
    Final3x77& operator=(Final3x77&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x78 {
public:
    Final3x78() = default;
    ~Final3x78() = default;
    Final3x78(const Final3x78&) = delete;
    Final3x78& operator=(const Final3x78&) = delete;
    Final3x78(Final3x78&&) noexcept = default;
    Final3x78& operator=(Final3x78&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x79 {
public:
    Final3x79() = default;
    ~Final3x79() = default;
    Final3x79(const Final3x79&) = delete;
    Final3x79& operator=(const Final3x79&) = delete;
    Final3x79(Final3x79&&) noexcept = default;
    Final3x79& operator=(Final3x79&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x80 {
public:
    Final3x80() = default;
    ~Final3x80() = default;
    Final3x80(const Final3x80&) = delete;
    Final3x80& operator=(const Final3x80&) = delete;
    Final3x80(Final3x80&&) noexcept = default;
    Final3x80& operator=(Final3x80&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x81 {
public:
    Final3x81() = default;
    ~Final3x81() = default;
    Final3x81(const Final3x81&) = delete;
    Final3x81& operator=(const Final3x81&) = delete;
    Final3x81(Final3x81&&) noexcept = default;
    Final3x81& operator=(Final3x81&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x82 {
public:
    Final3x82() = default;
    ~Final3x82() = default;
    Final3x82(const Final3x82&) = delete;
    Final3x82& operator=(const Final3x82&) = delete;
    Final3x82(Final3x82&&) noexcept = default;
    Final3x82& operator=(Final3x82&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x83 {
public:
    Final3x83() = default;
    ~Final3x83() = default;
    Final3x83(const Final3x83&) = delete;
    Final3x83& operator=(const Final3x83&) = delete;
    Final3x83(Final3x83&&) noexcept = default;
    Final3x83& operator=(Final3x83&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x84 {
public:
    Final3x84() = default;
    ~Final3x84() = default;
    Final3x84(const Final3x84&) = delete;
    Final3x84& operator=(const Final3x84&) = delete;
    Final3x84(Final3x84&&) noexcept = default;
    Final3x84& operator=(Final3x84&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x85 {
public:
    Final3x85() = default;
    ~Final3x85() = default;
    Final3x85(const Final3x85&) = delete;
    Final3x85& operator=(const Final3x85&) = delete;
    Final3x85(Final3x85&&) noexcept = default;
    Final3x85& operator=(Final3x85&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x86 {
public:
    Final3x86() = default;
    ~Final3x86() = default;
    Final3x86(const Final3x86&) = delete;
    Final3x86& operator=(const Final3x86&) = delete;
    Final3x86(Final3x86&&) noexcept = default;
    Final3x86& operator=(Final3x86&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x87 {
public:
    Final3x87() = default;
    ~Final3x87() = default;
    Final3x87(const Final3x87&) = delete;
    Final3x87& operator=(const Final3x87&) = delete;
    Final3x87(Final3x87&&) noexcept = default;
    Final3x87& operator=(Final3x87&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x88 {
public:
    Final3x88() = default;
    ~Final3x88() = default;
    Final3x88(const Final3x88&) = delete;
    Final3x88& operator=(const Final3x88&) = delete;
    Final3x88(Final3x88&&) noexcept = default;
    Final3x88& operator=(Final3x88&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x89 {
public:
    Final3x89() = default;
    ~Final3x89() = default;
    Final3x89(const Final3x89&) = delete;
    Final3x89& operator=(const Final3x89&) = delete;
    Final3x89(Final3x89&&) noexcept = default;
    Final3x89& operator=(Final3x89&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x90 {
public:
    Final3x90() = default;
    ~Final3x90() = default;
    Final3x90(const Final3x90&) = delete;
    Final3x90& operator=(const Final3x90&) = delete;
    Final3x90(Final3x90&&) noexcept = default;
    Final3x90& operator=(Final3x90&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x91 {
public:
    Final3x91() = default;
    ~Final3x91() = default;
    Final3x91(const Final3x91&) = delete;
    Final3x91& operator=(const Final3x91&) = delete;
    Final3x91(Final3x91&&) noexcept = default;
    Final3x91& operator=(Final3x91&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x92 {
public:
    Final3x92() = default;
    ~Final3x92() = default;
    Final3x92(const Final3x92&) = delete;
    Final3x92& operator=(const Final3x92&) = delete;
    Final3x92(Final3x92&&) noexcept = default;
    Final3x92& operator=(Final3x92&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x93 {
public:
    Final3x93() = default;
    ~Final3x93() = default;
    Final3x93(const Final3x93&) = delete;
    Final3x93& operator=(const Final3x93&) = delete;
    Final3x93(Final3x93&&) noexcept = default;
    Final3x93& operator=(Final3x93&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x94 {
public:
    Final3x94() = default;
    ~Final3x94() = default;
    Final3x94(const Final3x94&) = delete;
    Final3x94& operator=(const Final3x94&) = delete;
    Final3x94(Final3x94&&) noexcept = default;
    Final3x94& operator=(Final3x94&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x95 {
public:
    Final3x95() = default;
    ~Final3x95() = default;
    Final3x95(const Final3x95&) = delete;
    Final3x95& operator=(const Final3x95&) = delete;
    Final3x95(Final3x95&&) noexcept = default;
    Final3x95& operator=(Final3x95&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x96 {
public:
    Final3x96() = default;
    ~Final3x96() = default;
    Final3x96(const Final3x96&) = delete;
    Final3x96& operator=(const Final3x96&) = delete;
    Final3x96(Final3x96&&) noexcept = default;
    Final3x96& operator=(Final3x96&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x97 {
public:
    Final3x97() = default;
    ~Final3x97() = default;
    Final3x97(const Final3x97&) = delete;
    Final3x97& operator=(const Final3x97&) = delete;
    Final3x97(Final3x97&&) noexcept = default;
    Final3x97& operator=(Final3x97&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x98 {
public:
    Final3x98() = default;
    ~Final3x98() = default;
    Final3x98(const Final3x98&) = delete;
    Final3x98& operator=(const Final3x98&) = delete;
    Final3x98(Final3x98&&) noexcept = default;
    Final3x98& operator=(Final3x98&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x99 {
public:
    Final3x99() = default;
    ~Final3x99() = default;
    Final3x99(const Final3x99&) = delete;
    Final3x99& operator=(const Final3x99&) = delete;
    Final3x99(Final3x99&&) noexcept = default;
    Final3x99& operator=(Final3x99&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x100 {
public:
    Final3x100() = default;
    ~Final3x100() = default;
    Final3x100(const Final3x100&) = delete;
    Final3x100& operator=(const Final3x100&) = delete;
    Final3x100(Final3x100&&) noexcept = default;
    Final3x100& operator=(Final3x100&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x101 {
public:
    Final3x101() = default;
    ~Final3x101() = default;
    Final3x101(const Final3x101&) = delete;
    Final3x101& operator=(const Final3x101&) = delete;
    Final3x101(Final3x101&&) noexcept = default;
    Final3x101& operator=(Final3x101&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x102 {
public:
    Final3x102() = default;
    ~Final3x102() = default;
    Final3x102(const Final3x102&) = delete;
    Final3x102& operator=(const Final3x102&) = delete;
    Final3x102(Final3x102&&) noexcept = default;
    Final3x102& operator=(Final3x102&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x103 {
public:
    Final3x103() = default;
    ~Final3x103() = default;
    Final3x103(const Final3x103&) = delete;
    Final3x103& operator=(const Final3x103&) = delete;
    Final3x103(Final3x103&&) noexcept = default;
    Final3x103& operator=(Final3x103&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x104 {
public:
    Final3x104() = default;
    ~Final3x104() = default;
    Final3x104(const Final3x104&) = delete;
    Final3x104& operator=(const Final3x104&) = delete;
    Final3x104(Final3x104&&) noexcept = default;
    Final3x104& operator=(Final3x104&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x105 {
public:
    Final3x105() = default;
    ~Final3x105() = default;
    Final3x105(const Final3x105&) = delete;
    Final3x105& operator=(const Final3x105&) = delete;
    Final3x105(Final3x105&&) noexcept = default;
    Final3x105& operator=(Final3x105&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x106 {
public:
    Final3x106() = default;
    ~Final3x106() = default;
    Final3x106(const Final3x106&) = delete;
    Final3x106& operator=(const Final3x106&) = delete;
    Final3x106(Final3x106&&) noexcept = default;
    Final3x106& operator=(Final3x106&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x107 {
public:
    Final3x107() = default;
    ~Final3x107() = default;
    Final3x107(const Final3x107&) = delete;
    Final3x107& operator=(const Final3x107&) = delete;
    Final3x107(Final3x107&&) noexcept = default;
    Final3x107& operator=(Final3x107&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x108 {
public:
    Final3x108() = default;
    ~Final3x108() = default;
    Final3x108(const Final3x108&) = delete;
    Final3x108& operator=(const Final3x108&) = delete;
    Final3x108(Final3x108&&) noexcept = default;
    Final3x108& operator=(Final3x108&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x109 {
public:
    Final3x109() = default;
    ~Final3x109() = default;
    Final3x109(const Final3x109&) = delete;
    Final3x109& operator=(const Final3x109&) = delete;
    Final3x109(Final3x109&&) noexcept = default;
    Final3x109& operator=(Final3x109&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x110 {
public:
    Final3x110() = default;
    ~Final3x110() = default;
    Final3x110(const Final3x110&) = delete;
    Final3x110& operator=(const Final3x110&) = delete;
    Final3x110(Final3x110&&) noexcept = default;
    Final3x110& operator=(Final3x110&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x111 {
public:
    Final3x111() = default;
    ~Final3x111() = default;
    Final3x111(const Final3x111&) = delete;
    Final3x111& operator=(const Final3x111&) = delete;
    Final3x111(Final3x111&&) noexcept = default;
    Final3x111& operator=(Final3x111&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x112 {
public:
    Final3x112() = default;
    ~Final3x112() = default;
    Final3x112(const Final3x112&) = delete;
    Final3x112& operator=(const Final3x112&) = delete;
    Final3x112(Final3x112&&) noexcept = default;
    Final3x112& operator=(Final3x112&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x113 {
public:
    Final3x113() = default;
    ~Final3x113() = default;
    Final3x113(const Final3x113&) = delete;
    Final3x113& operator=(const Final3x113&) = delete;
    Final3x113(Final3x113&&) noexcept = default;
    Final3x113& operator=(Final3x113&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x114 {
public:
    Final3x114() = default;
    ~Final3x114() = default;
    Final3x114(const Final3x114&) = delete;
    Final3x114& operator=(const Final3x114&) = delete;
    Final3x114(Final3x114&&) noexcept = default;
    Final3x114& operator=(Final3x114&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x115 {
public:
    Final3x115() = default;
    ~Final3x115() = default;
    Final3x115(const Final3x115&) = delete;
    Final3x115& operator=(const Final3x115&) = delete;
    Final3x115(Final3x115&&) noexcept = default;
    Final3x115& operator=(Final3x115&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x116 {
public:
    Final3x116() = default;
    ~Final3x116() = default;
    Final3x116(const Final3x116&) = delete;
    Final3x116& operator=(const Final3x116&) = delete;
    Final3x116(Final3x116&&) noexcept = default;
    Final3x116& operator=(Final3x116&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x117 {
public:
    Final3x117() = default;
    ~Final3x117() = default;
    Final3x117(const Final3x117&) = delete;
    Final3x117& operator=(const Final3x117&) = delete;
    Final3x117(Final3x117&&) noexcept = default;
    Final3x117& operator=(Final3x117&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x118 {
public:
    Final3x118() = default;
    ~Final3x118() = default;
    Final3x118(const Final3x118&) = delete;
    Final3x118& operator=(const Final3x118&) = delete;
    Final3x118(Final3x118&&) noexcept = default;
    Final3x118& operator=(Final3x118&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x119 {
public:
    Final3x119() = default;
    ~Final3x119() = default;
    Final3x119(const Final3x119&) = delete;
    Final3x119& operator=(const Final3x119&) = delete;
    Final3x119(Final3x119&&) noexcept = default;
    Final3x119& operator=(Final3x119&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x120 {
public:
    Final3x120() = default;
    ~Final3x120() = default;
    Final3x120(const Final3x120&) = delete;
    Final3x120& operator=(const Final3x120&) = delete;
    Final3x120(Final3x120&&) noexcept = default;
    Final3x120& operator=(Final3x120&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x121 {
public:
    Final3x121() = default;
    ~Final3x121() = default;
    Final3x121(const Final3x121&) = delete;
    Final3x121& operator=(const Final3x121&) = delete;
    Final3x121(Final3x121&&) noexcept = default;
    Final3x121& operator=(Final3x121&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x122 {
public:
    Final3x122() = default;
    ~Final3x122() = default;
    Final3x122(const Final3x122&) = delete;
    Final3x122& operator=(const Final3x122&) = delete;
    Final3x122(Final3x122&&) noexcept = default;
    Final3x122& operator=(Final3x122&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x123 {
public:
    Final3x123() = default;
    ~Final3x123() = default;
    Final3x123(const Final3x123&) = delete;
    Final3x123& operator=(const Final3x123&) = delete;
    Final3x123(Final3x123&&) noexcept = default;
    Final3x123& operator=(Final3x123&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x124 {
public:
    Final3x124() = default;
    ~Final3x124() = default;
    Final3x124(const Final3x124&) = delete;
    Final3x124& operator=(const Final3x124&) = delete;
    Final3x124(Final3x124&&) noexcept = default;
    Final3x124& operator=(Final3x124&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x125 {
public:
    Final3x125() = default;
    ~Final3x125() = default;
    Final3x125(const Final3x125&) = delete;
    Final3x125& operator=(const Final3x125&) = delete;
    Final3x125(Final3x125&&) noexcept = default;
    Final3x125& operator=(Final3x125&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x126 {
public:
    Final3x126() = default;
    ~Final3x126() = default;
    Final3x126(const Final3x126&) = delete;
    Final3x126& operator=(const Final3x126&) = delete;
    Final3x126(Final3x126&&) noexcept = default;
    Final3x126& operator=(Final3x126&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x127 {
public:
    Final3x127() = default;
    ~Final3x127() = default;
    Final3x127(const Final3x127&) = delete;
    Final3x127& operator=(const Final3x127&) = delete;
    Final3x127(Final3x127&&) noexcept = default;
    Final3x127& operator=(Final3x127&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x128 {
public:
    Final3x128() = default;
    ~Final3x128() = default;
    Final3x128(const Final3x128&) = delete;
    Final3x128& operator=(const Final3x128&) = delete;
    Final3x128(Final3x128&&) noexcept = default;
    Final3x128& operator=(Final3x128&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x129 {
public:
    Final3x129() = default;
    ~Final3x129() = default;
    Final3x129(const Final3x129&) = delete;
    Final3x129& operator=(const Final3x129&) = delete;
    Final3x129(Final3x129&&) noexcept = default;
    Final3x129& operator=(Final3x129&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x130 {
public:
    Final3x130() = default;
    ~Final3x130() = default;
    Final3x130(const Final3x130&) = delete;
    Final3x130& operator=(const Final3x130&) = delete;
    Final3x130(Final3x130&&) noexcept = default;
    Final3x130& operator=(Final3x130&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x131 {
public:
    Final3x131() = default;
    ~Final3x131() = default;
    Final3x131(const Final3x131&) = delete;
    Final3x131& operator=(const Final3x131&) = delete;
    Final3x131(Final3x131&&) noexcept = default;
    Final3x131& operator=(Final3x131&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x132 {
public:
    Final3x132() = default;
    ~Final3x132() = default;
    Final3x132(const Final3x132&) = delete;
    Final3x132& operator=(const Final3x132&) = delete;
    Final3x132(Final3x132&&) noexcept = default;
    Final3x132& operator=(Final3x132&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x133 {
public:
    Final3x133() = default;
    ~Final3x133() = default;
    Final3x133(const Final3x133&) = delete;
    Final3x133& operator=(const Final3x133&) = delete;
    Final3x133(Final3x133&&) noexcept = default;
    Final3x133& operator=(Final3x133&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x134 {
public:
    Final3x134() = default;
    ~Final3x134() = default;
    Final3x134(const Final3x134&) = delete;
    Final3x134& operator=(const Final3x134&) = delete;
    Final3x134(Final3x134&&) noexcept = default;
    Final3x134& operator=(Final3x134&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x135 {
public:
    Final3x135() = default;
    ~Final3x135() = default;
    Final3x135(const Final3x135&) = delete;
    Final3x135& operator=(const Final3x135&) = delete;
    Final3x135(Final3x135&&) noexcept = default;
    Final3x135& operator=(Final3x135&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x136 {
public:
    Final3x136() = default;
    ~Final3x136() = default;
    Final3x136(const Final3x136&) = delete;
    Final3x136& operator=(const Final3x136&) = delete;
    Final3x136(Final3x136&&) noexcept = default;
    Final3x136& operator=(Final3x136&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x137 {
public:
    Final3x137() = default;
    ~Final3x137() = default;
    Final3x137(const Final3x137&) = delete;
    Final3x137& operator=(const Final3x137&) = delete;
    Final3x137(Final3x137&&) noexcept = default;
    Final3x137& operator=(Final3x137&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x138 {
public:
    Final3x138() = default;
    ~Final3x138() = default;
    Final3x138(const Final3x138&) = delete;
    Final3x138& operator=(const Final3x138&) = delete;
    Final3x138(Final3x138&&) noexcept = default;
    Final3x138& operator=(Final3x138&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x139 {
public:
    Final3x139() = default;
    ~Final3x139() = default;
    Final3x139(const Final3x139&) = delete;
    Final3x139& operator=(const Final3x139&) = delete;
    Final3x139(Final3x139&&) noexcept = default;
    Final3x139& operator=(Final3x139&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x140 {
public:
    Final3x140() = default;
    ~Final3x140() = default;
    Final3x140(const Final3x140&) = delete;
    Final3x140& operator=(const Final3x140&) = delete;
    Final3x140(Final3x140&&) noexcept = default;
    Final3x140& operator=(Final3x140&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x141 {
public:
    Final3x141() = default;
    ~Final3x141() = default;
    Final3x141(const Final3x141&) = delete;
    Final3x141& operator=(const Final3x141&) = delete;
    Final3x141(Final3x141&&) noexcept = default;
    Final3x141& operator=(Final3x141&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x142 {
public:
    Final3x142() = default;
    ~Final3x142() = default;
    Final3x142(const Final3x142&) = delete;
    Final3x142& operator=(const Final3x142&) = delete;
    Final3x142(Final3x142&&) noexcept = default;
    Final3x142& operator=(Final3x142&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x143 {
public:
    Final3x143() = default;
    ~Final3x143() = default;
    Final3x143(const Final3x143&) = delete;
    Final3x143& operator=(const Final3x143&) = delete;
    Final3x143(Final3x143&&) noexcept = default;
    Final3x143& operator=(Final3x143&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x144 {
public:
    Final3x144() = default;
    ~Final3x144() = default;
    Final3x144(const Final3x144&) = delete;
    Final3x144& operator=(const Final3x144&) = delete;
    Final3x144(Final3x144&&) noexcept = default;
    Final3x144& operator=(Final3x144&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x145 {
public:
    Final3x145() = default;
    ~Final3x145() = default;
    Final3x145(const Final3x145&) = delete;
    Final3x145& operator=(const Final3x145&) = delete;
    Final3x145(Final3x145&&) noexcept = default;
    Final3x145& operator=(Final3x145&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x146 {
public:
    Final3x146() = default;
    ~Final3x146() = default;
    Final3x146(const Final3x146&) = delete;
    Final3x146& operator=(const Final3x146&) = delete;
    Final3x146(Final3x146&&) noexcept = default;
    Final3x146& operator=(Final3x146&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x147 {
public:
    Final3x147() = default;
    ~Final3x147() = default;
    Final3x147(const Final3x147&) = delete;
    Final3x147& operator=(const Final3x147&) = delete;
    Final3x147(Final3x147&&) noexcept = default;
    Final3x147& operator=(Final3x147&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x148 {
public:
    Final3x148() = default;
    ~Final3x148() = default;
    Final3x148(const Final3x148&) = delete;
    Final3x148& operator=(const Final3x148&) = delete;
    Final3x148(Final3x148&&) noexcept = default;
    Final3x148& operator=(Final3x148&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x149 {
public:
    Final3x149() = default;
    ~Final3x149() = default;
    Final3x149(const Final3x149&) = delete;
    Final3x149& operator=(const Final3x149&) = delete;
    Final3x149(Final3x149&&) noexcept = default;
    Final3x149& operator=(Final3x149&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x150 {
public:
    Final3x150() = default;
    ~Final3x150() = default;
    Final3x150(const Final3x150&) = delete;
    Final3x150& operator=(const Final3x150&) = delete;
    Final3x150(Final3x150&&) noexcept = default;
    Final3x150& operator=(Final3x150&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x151 {
public:
    Final3x151() = default;
    ~Final3x151() = default;
    Final3x151(const Final3x151&) = delete;
    Final3x151& operator=(const Final3x151&) = delete;
    Final3x151(Final3x151&&) noexcept = default;
    Final3x151& operator=(Final3x151&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x152 {
public:
    Final3x152() = default;
    ~Final3x152() = default;
    Final3x152(const Final3x152&) = delete;
    Final3x152& operator=(const Final3x152&) = delete;
    Final3x152(Final3x152&&) noexcept = default;
    Final3x152& operator=(Final3x152&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x153 {
public:
    Final3x153() = default;
    ~Final3x153() = default;
    Final3x153(const Final3x153&) = delete;
    Final3x153& operator=(const Final3x153&) = delete;
    Final3x153(Final3x153&&) noexcept = default;
    Final3x153& operator=(Final3x153&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x154 {
public:
    Final3x154() = default;
    ~Final3x154() = default;
    Final3x154(const Final3x154&) = delete;
    Final3x154& operator=(const Final3x154&) = delete;
    Final3x154(Final3x154&&) noexcept = default;
    Final3x154& operator=(Final3x154&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x155 {
public:
    Final3x155() = default;
    ~Final3x155() = default;
    Final3x155(const Final3x155&) = delete;
    Final3x155& operator=(const Final3x155&) = delete;
    Final3x155(Final3x155&&) noexcept = default;
    Final3x155& operator=(Final3x155&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x156 {
public:
    Final3x156() = default;
    ~Final3x156() = default;
    Final3x156(const Final3x156&) = delete;
    Final3x156& operator=(const Final3x156&) = delete;
    Final3x156(Final3x156&&) noexcept = default;
    Final3x156& operator=(Final3x156&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x157 {
public:
    Final3x157() = default;
    ~Final3x157() = default;
    Final3x157(const Final3x157&) = delete;
    Final3x157& operator=(const Final3x157&) = delete;
    Final3x157(Final3x157&&) noexcept = default;
    Final3x157& operator=(Final3x157&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x158 {
public:
    Final3x158() = default;
    ~Final3x158() = default;
    Final3x158(const Final3x158&) = delete;
    Final3x158& operator=(const Final3x158&) = delete;
    Final3x158(Final3x158&&) noexcept = default;
    Final3x158& operator=(Final3x158&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x159 {
public:
    Final3x159() = default;
    ~Final3x159() = default;
    Final3x159(const Final3x159&) = delete;
    Final3x159& operator=(const Final3x159&) = delete;
    Final3x159(Final3x159&&) noexcept = default;
    Final3x159& operator=(Final3x159&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x160 {
public:
    Final3x160() = default;
    ~Final3x160() = default;
    Final3x160(const Final3x160&) = delete;
    Final3x160& operator=(const Final3x160&) = delete;
    Final3x160(Final3x160&&) noexcept = default;
    Final3x160& operator=(Final3x160&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x161 {
public:
    Final3x161() = default;
    ~Final3x161() = default;
    Final3x161(const Final3x161&) = delete;
    Final3x161& operator=(const Final3x161&) = delete;
    Final3x161(Final3x161&&) noexcept = default;
    Final3x161& operator=(Final3x161&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x162 {
public:
    Final3x162() = default;
    ~Final3x162() = default;
    Final3x162(const Final3x162&) = delete;
    Final3x162& operator=(const Final3x162&) = delete;
    Final3x162(Final3x162&&) noexcept = default;
    Final3x162& operator=(Final3x162&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x163 {
public:
    Final3x163() = default;
    ~Final3x163() = default;
    Final3x163(const Final3x163&) = delete;
    Final3x163& operator=(const Final3x163&) = delete;
    Final3x163(Final3x163&&) noexcept = default;
    Final3x163& operator=(Final3x163&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x164 {
public:
    Final3x164() = default;
    ~Final3x164() = default;
    Final3x164(const Final3x164&) = delete;
    Final3x164& operator=(const Final3x164&) = delete;
    Final3x164(Final3x164&&) noexcept = default;
    Final3x164& operator=(Final3x164&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x165 {
public:
    Final3x165() = default;
    ~Final3x165() = default;
    Final3x165(const Final3x165&) = delete;
    Final3x165& operator=(const Final3x165&) = delete;
    Final3x165(Final3x165&&) noexcept = default;
    Final3x165& operator=(Final3x165&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x166 {
public:
    Final3x166() = default;
    ~Final3x166() = default;
    Final3x166(const Final3x166&) = delete;
    Final3x166& operator=(const Final3x166&) = delete;
    Final3x166(Final3x166&&) noexcept = default;
    Final3x166& operator=(Final3x166&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x167 {
public:
    Final3x167() = default;
    ~Final3x167() = default;
    Final3x167(const Final3x167&) = delete;
    Final3x167& operator=(const Final3x167&) = delete;
    Final3x167(Final3x167&&) noexcept = default;
    Final3x167& operator=(Final3x167&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x168 {
public:
    Final3x168() = default;
    ~Final3x168() = default;
    Final3x168(const Final3x168&) = delete;
    Final3x168& operator=(const Final3x168&) = delete;
    Final3x168(Final3x168&&) noexcept = default;
    Final3x168& operator=(Final3x168&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x169 {
public:
    Final3x169() = default;
    ~Final3x169() = default;
    Final3x169(const Final3x169&) = delete;
    Final3x169& operator=(const Final3x169&) = delete;
    Final3x169(Final3x169&&) noexcept = default;
    Final3x169& operator=(Final3x169&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x170 {
public:
    Final3x170() = default;
    ~Final3x170() = default;
    Final3x170(const Final3x170&) = delete;
    Final3x170& operator=(const Final3x170&) = delete;
    Final3x170(Final3x170&&) noexcept = default;
    Final3x170& operator=(Final3x170&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x171 {
public:
    Final3x171() = default;
    ~Final3x171() = default;
    Final3x171(const Final3x171&) = delete;
    Final3x171& operator=(const Final3x171&) = delete;
    Final3x171(Final3x171&&) noexcept = default;
    Final3x171& operator=(Final3x171&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x172 {
public:
    Final3x172() = default;
    ~Final3x172() = default;
    Final3x172(const Final3x172&) = delete;
    Final3x172& operator=(const Final3x172&) = delete;
    Final3x172(Final3x172&&) noexcept = default;
    Final3x172& operator=(Final3x172&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x173 {
public:
    Final3x173() = default;
    ~Final3x173() = default;
    Final3x173(const Final3x173&) = delete;
    Final3x173& operator=(const Final3x173&) = delete;
    Final3x173(Final3x173&&) noexcept = default;
    Final3x173& operator=(Final3x173&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x174 {
public:
    Final3x174() = default;
    ~Final3x174() = default;
    Final3x174(const Final3x174&) = delete;
    Final3x174& operator=(const Final3x174&) = delete;
    Final3x174(Final3x174&&) noexcept = default;
    Final3x174& operator=(Final3x174&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x175 {
public:
    Final3x175() = default;
    ~Final3x175() = default;
    Final3x175(const Final3x175&) = delete;
    Final3x175& operator=(const Final3x175&) = delete;
    Final3x175(Final3x175&&) noexcept = default;
    Final3x175& operator=(Final3x175&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x176 {
public:
    Final3x176() = default;
    ~Final3x176() = default;
    Final3x176(const Final3x176&) = delete;
    Final3x176& operator=(const Final3x176&) = delete;
    Final3x176(Final3x176&&) noexcept = default;
    Final3x176& operator=(Final3x176&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x177 {
public:
    Final3x177() = default;
    ~Final3x177() = default;
    Final3x177(const Final3x177&) = delete;
    Final3x177& operator=(const Final3x177&) = delete;
    Final3x177(Final3x177&&) noexcept = default;
    Final3x177& operator=(Final3x177&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x178 {
public:
    Final3x178() = default;
    ~Final3x178() = default;
    Final3x178(const Final3x178&) = delete;
    Final3x178& operator=(const Final3x178&) = delete;
    Final3x178(Final3x178&&) noexcept = default;
    Final3x178& operator=(Final3x178&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x179 {
public:
    Final3x179() = default;
    ~Final3x179() = default;
    Final3x179(const Final3x179&) = delete;
    Final3x179& operator=(const Final3x179&) = delete;
    Final3x179(Final3x179&&) noexcept = default;
    Final3x179& operator=(Final3x179&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x180 {
public:
    Final3x180() = default;
    ~Final3x180() = default;
    Final3x180(const Final3x180&) = delete;
    Final3x180& operator=(const Final3x180&) = delete;
    Final3x180(Final3x180&&) noexcept = default;
    Final3x180& operator=(Final3x180&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x181 {
public:
    Final3x181() = default;
    ~Final3x181() = default;
    Final3x181(const Final3x181&) = delete;
    Final3x181& operator=(const Final3x181&) = delete;
    Final3x181(Final3x181&&) noexcept = default;
    Final3x181& operator=(Final3x181&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x182 {
public:
    Final3x182() = default;
    ~Final3x182() = default;
    Final3x182(const Final3x182&) = delete;
    Final3x182& operator=(const Final3x182&) = delete;
    Final3x182(Final3x182&&) noexcept = default;
    Final3x182& operator=(Final3x182&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x183 {
public:
    Final3x183() = default;
    ~Final3x183() = default;
    Final3x183(const Final3x183&) = delete;
    Final3x183& operator=(const Final3x183&) = delete;
    Final3x183(Final3x183&&) noexcept = default;
    Final3x183& operator=(Final3x183&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x184 {
public:
    Final3x184() = default;
    ~Final3x184() = default;
    Final3x184(const Final3x184&) = delete;
    Final3x184& operator=(const Final3x184&) = delete;
    Final3x184(Final3x184&&) noexcept = default;
    Final3x184& operator=(Final3x184&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x185 {
public:
    Final3x185() = default;
    ~Final3x185() = default;
    Final3x185(const Final3x185&) = delete;
    Final3x185& operator=(const Final3x185&) = delete;
    Final3x185(Final3x185&&) noexcept = default;
    Final3x185& operator=(Final3x185&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x186 {
public:
    Final3x186() = default;
    ~Final3x186() = default;
    Final3x186(const Final3x186&) = delete;
    Final3x186& operator=(const Final3x186&) = delete;
    Final3x186(Final3x186&&) noexcept = default;
    Final3x186& operator=(Final3x186&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x187 {
public:
    Final3x187() = default;
    ~Final3x187() = default;
    Final3x187(const Final3x187&) = delete;
    Final3x187& operator=(const Final3x187&) = delete;
    Final3x187(Final3x187&&) noexcept = default;
    Final3x187& operator=(Final3x187&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x188 {
public:
    Final3x188() = default;
    ~Final3x188() = default;
    Final3x188(const Final3x188&) = delete;
    Final3x188& operator=(const Final3x188&) = delete;
    Final3x188(Final3x188&&) noexcept = default;
    Final3x188& operator=(Final3x188&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x189 {
public:
    Final3x189() = default;
    ~Final3x189() = default;
    Final3x189(const Final3x189&) = delete;
    Final3x189& operator=(const Final3x189&) = delete;
    Final3x189(Final3x189&&) noexcept = default;
    Final3x189& operator=(Final3x189&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x190 {
public:
    Final3x190() = default;
    ~Final3x190() = default;
    Final3x190(const Final3x190&) = delete;
    Final3x190& operator=(const Final3x190&) = delete;
    Final3x190(Final3x190&&) noexcept = default;
    Final3x190& operator=(Final3x190&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x191 {
public:
    Final3x191() = default;
    ~Final3x191() = default;
    Final3x191(const Final3x191&) = delete;
    Final3x191& operator=(const Final3x191&) = delete;
    Final3x191(Final3x191&&) noexcept = default;
    Final3x191& operator=(Final3x191&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x192 {
public:
    Final3x192() = default;
    ~Final3x192() = default;
    Final3x192(const Final3x192&) = delete;
    Final3x192& operator=(const Final3x192&) = delete;
    Final3x192(Final3x192&&) noexcept = default;
    Final3x192& operator=(Final3x192&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x193 {
public:
    Final3x193() = default;
    ~Final3x193() = default;
    Final3x193(const Final3x193&) = delete;
    Final3x193& operator=(const Final3x193&) = delete;
    Final3x193(Final3x193&&) noexcept = default;
    Final3x193& operator=(Final3x193&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x194 {
public:
    Final3x194() = default;
    ~Final3x194() = default;
    Final3x194(const Final3x194&) = delete;
    Final3x194& operator=(const Final3x194&) = delete;
    Final3x194(Final3x194&&) noexcept = default;
    Final3x194& operator=(Final3x194&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x195 {
public:
    Final3x195() = default;
    ~Final3x195() = default;
    Final3x195(const Final3x195&) = delete;
    Final3x195& operator=(const Final3x195&) = delete;
    Final3x195(Final3x195&&) noexcept = default;
    Final3x195& operator=(Final3x195&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x196 {
public:
    Final3x196() = default;
    ~Final3x196() = default;
    Final3x196(const Final3x196&) = delete;
    Final3x196& operator=(const Final3x196&) = delete;
    Final3x196(Final3x196&&) noexcept = default;
    Final3x196& operator=(Final3x196&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x197 {
public:
    Final3x197() = default;
    ~Final3x197() = default;
    Final3x197(const Final3x197&) = delete;
    Final3x197& operator=(const Final3x197&) = delete;
    Final3x197(Final3x197&&) noexcept = default;
    Final3x197& operator=(Final3x197&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x198 {
public:
    Final3x198() = default;
    ~Final3x198() = default;
    Final3x198(const Final3x198&) = delete;
    Final3x198& operator=(const Final3x198&) = delete;
    Final3x198(Final3x198&&) noexcept = default;
    Final3x198& operator=(Final3x198&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final3x199 {
public:
    Final3x199() = default;
    ~Final3x199() = default;
    Final3x199(const Final3x199&) = delete;
    Final3x199& operator=(const Final3x199&) = delete;
    Final3x199(Final3x199&&) noexcept = default;
    Final3x199& operator=(Final3x199&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
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