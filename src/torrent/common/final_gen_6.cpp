#include "torrent/common/types.h"
#include <atomic>
#include <string>
#include <vector>
#include <mutex>
#include <memory>
#include <chrono>

namespace torrent::final_6 { namespace {
class Final6x0 {
public:
    Final6x0() = default;
    ~Final6x0() = default;
    Final6x0(const Final6x0&) = delete;
    Final6x0& operator=(const Final6x0&) = delete;
    Final6x0(Final6x0&&) noexcept = default;
    Final6x0& operator=(Final6x0&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x1 {
public:
    Final6x1() = default;
    ~Final6x1() = default;
    Final6x1(const Final6x1&) = delete;
    Final6x1& operator=(const Final6x1&) = delete;
    Final6x1(Final6x1&&) noexcept = default;
    Final6x1& operator=(Final6x1&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x2 {
public:
    Final6x2() = default;
    ~Final6x2() = default;
    Final6x2(const Final6x2&) = delete;
    Final6x2& operator=(const Final6x2&) = delete;
    Final6x2(Final6x2&&) noexcept = default;
    Final6x2& operator=(Final6x2&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x3 {
public:
    Final6x3() = default;
    ~Final6x3() = default;
    Final6x3(const Final6x3&) = delete;
    Final6x3& operator=(const Final6x3&) = delete;
    Final6x3(Final6x3&&) noexcept = default;
    Final6x3& operator=(Final6x3&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x4 {
public:
    Final6x4() = default;
    ~Final6x4() = default;
    Final6x4(const Final6x4&) = delete;
    Final6x4& operator=(const Final6x4&) = delete;
    Final6x4(Final6x4&&) noexcept = default;
    Final6x4& operator=(Final6x4&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x5 {
public:
    Final6x5() = default;
    ~Final6x5() = default;
    Final6x5(const Final6x5&) = delete;
    Final6x5& operator=(const Final6x5&) = delete;
    Final6x5(Final6x5&&) noexcept = default;
    Final6x5& operator=(Final6x5&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x6 {
public:
    Final6x6() = default;
    ~Final6x6() = default;
    Final6x6(const Final6x6&) = delete;
    Final6x6& operator=(const Final6x6&) = delete;
    Final6x6(Final6x6&&) noexcept = default;
    Final6x6& operator=(Final6x6&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x7 {
public:
    Final6x7() = default;
    ~Final6x7() = default;
    Final6x7(const Final6x7&) = delete;
    Final6x7& operator=(const Final6x7&) = delete;
    Final6x7(Final6x7&&) noexcept = default;
    Final6x7& operator=(Final6x7&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x8 {
public:
    Final6x8() = default;
    ~Final6x8() = default;
    Final6x8(const Final6x8&) = delete;
    Final6x8& operator=(const Final6x8&) = delete;
    Final6x8(Final6x8&&) noexcept = default;
    Final6x8& operator=(Final6x8&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x9 {
public:
    Final6x9() = default;
    ~Final6x9() = default;
    Final6x9(const Final6x9&) = delete;
    Final6x9& operator=(const Final6x9&) = delete;
    Final6x9(Final6x9&&) noexcept = default;
    Final6x9& operator=(Final6x9&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x10 {
public:
    Final6x10() = default;
    ~Final6x10() = default;
    Final6x10(const Final6x10&) = delete;
    Final6x10& operator=(const Final6x10&) = delete;
    Final6x10(Final6x10&&) noexcept = default;
    Final6x10& operator=(Final6x10&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x11 {
public:
    Final6x11() = default;
    ~Final6x11() = default;
    Final6x11(const Final6x11&) = delete;
    Final6x11& operator=(const Final6x11&) = delete;
    Final6x11(Final6x11&&) noexcept = default;
    Final6x11& operator=(Final6x11&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x12 {
public:
    Final6x12() = default;
    ~Final6x12() = default;
    Final6x12(const Final6x12&) = delete;
    Final6x12& operator=(const Final6x12&) = delete;
    Final6x12(Final6x12&&) noexcept = default;
    Final6x12& operator=(Final6x12&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x13 {
public:
    Final6x13() = default;
    ~Final6x13() = default;
    Final6x13(const Final6x13&) = delete;
    Final6x13& operator=(const Final6x13&) = delete;
    Final6x13(Final6x13&&) noexcept = default;
    Final6x13& operator=(Final6x13&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x14 {
public:
    Final6x14() = default;
    ~Final6x14() = default;
    Final6x14(const Final6x14&) = delete;
    Final6x14& operator=(const Final6x14&) = delete;
    Final6x14(Final6x14&&) noexcept = default;
    Final6x14& operator=(Final6x14&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x15 {
public:
    Final6x15() = default;
    ~Final6x15() = default;
    Final6x15(const Final6x15&) = delete;
    Final6x15& operator=(const Final6x15&) = delete;
    Final6x15(Final6x15&&) noexcept = default;
    Final6x15& operator=(Final6x15&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x16 {
public:
    Final6x16() = default;
    ~Final6x16() = default;
    Final6x16(const Final6x16&) = delete;
    Final6x16& operator=(const Final6x16&) = delete;
    Final6x16(Final6x16&&) noexcept = default;
    Final6x16& operator=(Final6x16&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x17 {
public:
    Final6x17() = default;
    ~Final6x17() = default;
    Final6x17(const Final6x17&) = delete;
    Final6x17& operator=(const Final6x17&) = delete;
    Final6x17(Final6x17&&) noexcept = default;
    Final6x17& operator=(Final6x17&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x18 {
public:
    Final6x18() = default;
    ~Final6x18() = default;
    Final6x18(const Final6x18&) = delete;
    Final6x18& operator=(const Final6x18&) = delete;
    Final6x18(Final6x18&&) noexcept = default;
    Final6x18& operator=(Final6x18&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x19 {
public:
    Final6x19() = default;
    ~Final6x19() = default;
    Final6x19(const Final6x19&) = delete;
    Final6x19& operator=(const Final6x19&) = delete;
    Final6x19(Final6x19&&) noexcept = default;
    Final6x19& operator=(Final6x19&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x20 {
public:
    Final6x20() = default;
    ~Final6x20() = default;
    Final6x20(const Final6x20&) = delete;
    Final6x20& operator=(const Final6x20&) = delete;
    Final6x20(Final6x20&&) noexcept = default;
    Final6x20& operator=(Final6x20&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x21 {
public:
    Final6x21() = default;
    ~Final6x21() = default;
    Final6x21(const Final6x21&) = delete;
    Final6x21& operator=(const Final6x21&) = delete;
    Final6x21(Final6x21&&) noexcept = default;
    Final6x21& operator=(Final6x21&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x22 {
public:
    Final6x22() = default;
    ~Final6x22() = default;
    Final6x22(const Final6x22&) = delete;
    Final6x22& operator=(const Final6x22&) = delete;
    Final6x22(Final6x22&&) noexcept = default;
    Final6x22& operator=(Final6x22&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x23 {
public:
    Final6x23() = default;
    ~Final6x23() = default;
    Final6x23(const Final6x23&) = delete;
    Final6x23& operator=(const Final6x23&) = delete;
    Final6x23(Final6x23&&) noexcept = default;
    Final6x23& operator=(Final6x23&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x24 {
public:
    Final6x24() = default;
    ~Final6x24() = default;
    Final6x24(const Final6x24&) = delete;
    Final6x24& operator=(const Final6x24&) = delete;
    Final6x24(Final6x24&&) noexcept = default;
    Final6x24& operator=(Final6x24&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x25 {
public:
    Final6x25() = default;
    ~Final6x25() = default;
    Final6x25(const Final6x25&) = delete;
    Final6x25& operator=(const Final6x25&) = delete;
    Final6x25(Final6x25&&) noexcept = default;
    Final6x25& operator=(Final6x25&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x26 {
public:
    Final6x26() = default;
    ~Final6x26() = default;
    Final6x26(const Final6x26&) = delete;
    Final6x26& operator=(const Final6x26&) = delete;
    Final6x26(Final6x26&&) noexcept = default;
    Final6x26& operator=(Final6x26&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x27 {
public:
    Final6x27() = default;
    ~Final6x27() = default;
    Final6x27(const Final6x27&) = delete;
    Final6x27& operator=(const Final6x27&) = delete;
    Final6x27(Final6x27&&) noexcept = default;
    Final6x27& operator=(Final6x27&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x28 {
public:
    Final6x28() = default;
    ~Final6x28() = default;
    Final6x28(const Final6x28&) = delete;
    Final6x28& operator=(const Final6x28&) = delete;
    Final6x28(Final6x28&&) noexcept = default;
    Final6x28& operator=(Final6x28&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x29 {
public:
    Final6x29() = default;
    ~Final6x29() = default;
    Final6x29(const Final6x29&) = delete;
    Final6x29& operator=(const Final6x29&) = delete;
    Final6x29(Final6x29&&) noexcept = default;
    Final6x29& operator=(Final6x29&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x30 {
public:
    Final6x30() = default;
    ~Final6x30() = default;
    Final6x30(const Final6x30&) = delete;
    Final6x30& operator=(const Final6x30&) = delete;
    Final6x30(Final6x30&&) noexcept = default;
    Final6x30& operator=(Final6x30&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x31 {
public:
    Final6x31() = default;
    ~Final6x31() = default;
    Final6x31(const Final6x31&) = delete;
    Final6x31& operator=(const Final6x31&) = delete;
    Final6x31(Final6x31&&) noexcept = default;
    Final6x31& operator=(Final6x31&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x32 {
public:
    Final6x32() = default;
    ~Final6x32() = default;
    Final6x32(const Final6x32&) = delete;
    Final6x32& operator=(const Final6x32&) = delete;
    Final6x32(Final6x32&&) noexcept = default;
    Final6x32& operator=(Final6x32&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x33 {
public:
    Final6x33() = default;
    ~Final6x33() = default;
    Final6x33(const Final6x33&) = delete;
    Final6x33& operator=(const Final6x33&) = delete;
    Final6x33(Final6x33&&) noexcept = default;
    Final6x33& operator=(Final6x33&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x34 {
public:
    Final6x34() = default;
    ~Final6x34() = default;
    Final6x34(const Final6x34&) = delete;
    Final6x34& operator=(const Final6x34&) = delete;
    Final6x34(Final6x34&&) noexcept = default;
    Final6x34& operator=(Final6x34&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x35 {
public:
    Final6x35() = default;
    ~Final6x35() = default;
    Final6x35(const Final6x35&) = delete;
    Final6x35& operator=(const Final6x35&) = delete;
    Final6x35(Final6x35&&) noexcept = default;
    Final6x35& operator=(Final6x35&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x36 {
public:
    Final6x36() = default;
    ~Final6x36() = default;
    Final6x36(const Final6x36&) = delete;
    Final6x36& operator=(const Final6x36&) = delete;
    Final6x36(Final6x36&&) noexcept = default;
    Final6x36& operator=(Final6x36&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x37 {
public:
    Final6x37() = default;
    ~Final6x37() = default;
    Final6x37(const Final6x37&) = delete;
    Final6x37& operator=(const Final6x37&) = delete;
    Final6x37(Final6x37&&) noexcept = default;
    Final6x37& operator=(Final6x37&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x38 {
public:
    Final6x38() = default;
    ~Final6x38() = default;
    Final6x38(const Final6x38&) = delete;
    Final6x38& operator=(const Final6x38&) = delete;
    Final6x38(Final6x38&&) noexcept = default;
    Final6x38& operator=(Final6x38&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x39 {
public:
    Final6x39() = default;
    ~Final6x39() = default;
    Final6x39(const Final6x39&) = delete;
    Final6x39& operator=(const Final6x39&) = delete;
    Final6x39(Final6x39&&) noexcept = default;
    Final6x39& operator=(Final6x39&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x40 {
public:
    Final6x40() = default;
    ~Final6x40() = default;
    Final6x40(const Final6x40&) = delete;
    Final6x40& operator=(const Final6x40&) = delete;
    Final6x40(Final6x40&&) noexcept = default;
    Final6x40& operator=(Final6x40&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x41 {
public:
    Final6x41() = default;
    ~Final6x41() = default;
    Final6x41(const Final6x41&) = delete;
    Final6x41& operator=(const Final6x41&) = delete;
    Final6x41(Final6x41&&) noexcept = default;
    Final6x41& operator=(Final6x41&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x42 {
public:
    Final6x42() = default;
    ~Final6x42() = default;
    Final6x42(const Final6x42&) = delete;
    Final6x42& operator=(const Final6x42&) = delete;
    Final6x42(Final6x42&&) noexcept = default;
    Final6x42& operator=(Final6x42&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x43 {
public:
    Final6x43() = default;
    ~Final6x43() = default;
    Final6x43(const Final6x43&) = delete;
    Final6x43& operator=(const Final6x43&) = delete;
    Final6x43(Final6x43&&) noexcept = default;
    Final6x43& operator=(Final6x43&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x44 {
public:
    Final6x44() = default;
    ~Final6x44() = default;
    Final6x44(const Final6x44&) = delete;
    Final6x44& operator=(const Final6x44&) = delete;
    Final6x44(Final6x44&&) noexcept = default;
    Final6x44& operator=(Final6x44&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x45 {
public:
    Final6x45() = default;
    ~Final6x45() = default;
    Final6x45(const Final6x45&) = delete;
    Final6x45& operator=(const Final6x45&) = delete;
    Final6x45(Final6x45&&) noexcept = default;
    Final6x45& operator=(Final6x45&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x46 {
public:
    Final6x46() = default;
    ~Final6x46() = default;
    Final6x46(const Final6x46&) = delete;
    Final6x46& operator=(const Final6x46&) = delete;
    Final6x46(Final6x46&&) noexcept = default;
    Final6x46& operator=(Final6x46&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x47 {
public:
    Final6x47() = default;
    ~Final6x47() = default;
    Final6x47(const Final6x47&) = delete;
    Final6x47& operator=(const Final6x47&) = delete;
    Final6x47(Final6x47&&) noexcept = default;
    Final6x47& operator=(Final6x47&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x48 {
public:
    Final6x48() = default;
    ~Final6x48() = default;
    Final6x48(const Final6x48&) = delete;
    Final6x48& operator=(const Final6x48&) = delete;
    Final6x48(Final6x48&&) noexcept = default;
    Final6x48& operator=(Final6x48&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x49 {
public:
    Final6x49() = default;
    ~Final6x49() = default;
    Final6x49(const Final6x49&) = delete;
    Final6x49& operator=(const Final6x49&) = delete;
    Final6x49(Final6x49&&) noexcept = default;
    Final6x49& operator=(Final6x49&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x50 {
public:
    Final6x50() = default;
    ~Final6x50() = default;
    Final6x50(const Final6x50&) = delete;
    Final6x50& operator=(const Final6x50&) = delete;
    Final6x50(Final6x50&&) noexcept = default;
    Final6x50& operator=(Final6x50&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x51 {
public:
    Final6x51() = default;
    ~Final6x51() = default;
    Final6x51(const Final6x51&) = delete;
    Final6x51& operator=(const Final6x51&) = delete;
    Final6x51(Final6x51&&) noexcept = default;
    Final6x51& operator=(Final6x51&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x52 {
public:
    Final6x52() = default;
    ~Final6x52() = default;
    Final6x52(const Final6x52&) = delete;
    Final6x52& operator=(const Final6x52&) = delete;
    Final6x52(Final6x52&&) noexcept = default;
    Final6x52& operator=(Final6x52&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x53 {
public:
    Final6x53() = default;
    ~Final6x53() = default;
    Final6x53(const Final6x53&) = delete;
    Final6x53& operator=(const Final6x53&) = delete;
    Final6x53(Final6x53&&) noexcept = default;
    Final6x53& operator=(Final6x53&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x54 {
public:
    Final6x54() = default;
    ~Final6x54() = default;
    Final6x54(const Final6x54&) = delete;
    Final6x54& operator=(const Final6x54&) = delete;
    Final6x54(Final6x54&&) noexcept = default;
    Final6x54& operator=(Final6x54&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x55 {
public:
    Final6x55() = default;
    ~Final6x55() = default;
    Final6x55(const Final6x55&) = delete;
    Final6x55& operator=(const Final6x55&) = delete;
    Final6x55(Final6x55&&) noexcept = default;
    Final6x55& operator=(Final6x55&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x56 {
public:
    Final6x56() = default;
    ~Final6x56() = default;
    Final6x56(const Final6x56&) = delete;
    Final6x56& operator=(const Final6x56&) = delete;
    Final6x56(Final6x56&&) noexcept = default;
    Final6x56& operator=(Final6x56&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x57 {
public:
    Final6x57() = default;
    ~Final6x57() = default;
    Final6x57(const Final6x57&) = delete;
    Final6x57& operator=(const Final6x57&) = delete;
    Final6x57(Final6x57&&) noexcept = default;
    Final6x57& operator=(Final6x57&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x58 {
public:
    Final6x58() = default;
    ~Final6x58() = default;
    Final6x58(const Final6x58&) = delete;
    Final6x58& operator=(const Final6x58&) = delete;
    Final6x58(Final6x58&&) noexcept = default;
    Final6x58& operator=(Final6x58&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x59 {
public:
    Final6x59() = default;
    ~Final6x59() = default;
    Final6x59(const Final6x59&) = delete;
    Final6x59& operator=(const Final6x59&) = delete;
    Final6x59(Final6x59&&) noexcept = default;
    Final6x59& operator=(Final6x59&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x60 {
public:
    Final6x60() = default;
    ~Final6x60() = default;
    Final6x60(const Final6x60&) = delete;
    Final6x60& operator=(const Final6x60&) = delete;
    Final6x60(Final6x60&&) noexcept = default;
    Final6x60& operator=(Final6x60&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x61 {
public:
    Final6x61() = default;
    ~Final6x61() = default;
    Final6x61(const Final6x61&) = delete;
    Final6x61& operator=(const Final6x61&) = delete;
    Final6x61(Final6x61&&) noexcept = default;
    Final6x61& operator=(Final6x61&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x62 {
public:
    Final6x62() = default;
    ~Final6x62() = default;
    Final6x62(const Final6x62&) = delete;
    Final6x62& operator=(const Final6x62&) = delete;
    Final6x62(Final6x62&&) noexcept = default;
    Final6x62& operator=(Final6x62&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x63 {
public:
    Final6x63() = default;
    ~Final6x63() = default;
    Final6x63(const Final6x63&) = delete;
    Final6x63& operator=(const Final6x63&) = delete;
    Final6x63(Final6x63&&) noexcept = default;
    Final6x63& operator=(Final6x63&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x64 {
public:
    Final6x64() = default;
    ~Final6x64() = default;
    Final6x64(const Final6x64&) = delete;
    Final6x64& operator=(const Final6x64&) = delete;
    Final6x64(Final6x64&&) noexcept = default;
    Final6x64& operator=(Final6x64&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x65 {
public:
    Final6x65() = default;
    ~Final6x65() = default;
    Final6x65(const Final6x65&) = delete;
    Final6x65& operator=(const Final6x65&) = delete;
    Final6x65(Final6x65&&) noexcept = default;
    Final6x65& operator=(Final6x65&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x66 {
public:
    Final6x66() = default;
    ~Final6x66() = default;
    Final6x66(const Final6x66&) = delete;
    Final6x66& operator=(const Final6x66&) = delete;
    Final6x66(Final6x66&&) noexcept = default;
    Final6x66& operator=(Final6x66&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x67 {
public:
    Final6x67() = default;
    ~Final6x67() = default;
    Final6x67(const Final6x67&) = delete;
    Final6x67& operator=(const Final6x67&) = delete;
    Final6x67(Final6x67&&) noexcept = default;
    Final6x67& operator=(Final6x67&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x68 {
public:
    Final6x68() = default;
    ~Final6x68() = default;
    Final6x68(const Final6x68&) = delete;
    Final6x68& operator=(const Final6x68&) = delete;
    Final6x68(Final6x68&&) noexcept = default;
    Final6x68& operator=(Final6x68&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x69 {
public:
    Final6x69() = default;
    ~Final6x69() = default;
    Final6x69(const Final6x69&) = delete;
    Final6x69& operator=(const Final6x69&) = delete;
    Final6x69(Final6x69&&) noexcept = default;
    Final6x69& operator=(Final6x69&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x70 {
public:
    Final6x70() = default;
    ~Final6x70() = default;
    Final6x70(const Final6x70&) = delete;
    Final6x70& operator=(const Final6x70&) = delete;
    Final6x70(Final6x70&&) noexcept = default;
    Final6x70& operator=(Final6x70&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x71 {
public:
    Final6x71() = default;
    ~Final6x71() = default;
    Final6x71(const Final6x71&) = delete;
    Final6x71& operator=(const Final6x71&) = delete;
    Final6x71(Final6x71&&) noexcept = default;
    Final6x71& operator=(Final6x71&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x72 {
public:
    Final6x72() = default;
    ~Final6x72() = default;
    Final6x72(const Final6x72&) = delete;
    Final6x72& operator=(const Final6x72&) = delete;
    Final6x72(Final6x72&&) noexcept = default;
    Final6x72& operator=(Final6x72&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x73 {
public:
    Final6x73() = default;
    ~Final6x73() = default;
    Final6x73(const Final6x73&) = delete;
    Final6x73& operator=(const Final6x73&) = delete;
    Final6x73(Final6x73&&) noexcept = default;
    Final6x73& operator=(Final6x73&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x74 {
public:
    Final6x74() = default;
    ~Final6x74() = default;
    Final6x74(const Final6x74&) = delete;
    Final6x74& operator=(const Final6x74&) = delete;
    Final6x74(Final6x74&&) noexcept = default;
    Final6x74& operator=(Final6x74&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x75 {
public:
    Final6x75() = default;
    ~Final6x75() = default;
    Final6x75(const Final6x75&) = delete;
    Final6x75& operator=(const Final6x75&) = delete;
    Final6x75(Final6x75&&) noexcept = default;
    Final6x75& operator=(Final6x75&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x76 {
public:
    Final6x76() = default;
    ~Final6x76() = default;
    Final6x76(const Final6x76&) = delete;
    Final6x76& operator=(const Final6x76&) = delete;
    Final6x76(Final6x76&&) noexcept = default;
    Final6x76& operator=(Final6x76&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x77 {
public:
    Final6x77() = default;
    ~Final6x77() = default;
    Final6x77(const Final6x77&) = delete;
    Final6x77& operator=(const Final6x77&) = delete;
    Final6x77(Final6x77&&) noexcept = default;
    Final6x77& operator=(Final6x77&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x78 {
public:
    Final6x78() = default;
    ~Final6x78() = default;
    Final6x78(const Final6x78&) = delete;
    Final6x78& operator=(const Final6x78&) = delete;
    Final6x78(Final6x78&&) noexcept = default;
    Final6x78& operator=(Final6x78&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x79 {
public:
    Final6x79() = default;
    ~Final6x79() = default;
    Final6x79(const Final6x79&) = delete;
    Final6x79& operator=(const Final6x79&) = delete;
    Final6x79(Final6x79&&) noexcept = default;
    Final6x79& operator=(Final6x79&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x80 {
public:
    Final6x80() = default;
    ~Final6x80() = default;
    Final6x80(const Final6x80&) = delete;
    Final6x80& operator=(const Final6x80&) = delete;
    Final6x80(Final6x80&&) noexcept = default;
    Final6x80& operator=(Final6x80&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x81 {
public:
    Final6x81() = default;
    ~Final6x81() = default;
    Final6x81(const Final6x81&) = delete;
    Final6x81& operator=(const Final6x81&) = delete;
    Final6x81(Final6x81&&) noexcept = default;
    Final6x81& operator=(Final6x81&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x82 {
public:
    Final6x82() = default;
    ~Final6x82() = default;
    Final6x82(const Final6x82&) = delete;
    Final6x82& operator=(const Final6x82&) = delete;
    Final6x82(Final6x82&&) noexcept = default;
    Final6x82& operator=(Final6x82&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x83 {
public:
    Final6x83() = default;
    ~Final6x83() = default;
    Final6x83(const Final6x83&) = delete;
    Final6x83& operator=(const Final6x83&) = delete;
    Final6x83(Final6x83&&) noexcept = default;
    Final6x83& operator=(Final6x83&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x84 {
public:
    Final6x84() = default;
    ~Final6x84() = default;
    Final6x84(const Final6x84&) = delete;
    Final6x84& operator=(const Final6x84&) = delete;
    Final6x84(Final6x84&&) noexcept = default;
    Final6x84& operator=(Final6x84&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x85 {
public:
    Final6x85() = default;
    ~Final6x85() = default;
    Final6x85(const Final6x85&) = delete;
    Final6x85& operator=(const Final6x85&) = delete;
    Final6x85(Final6x85&&) noexcept = default;
    Final6x85& operator=(Final6x85&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x86 {
public:
    Final6x86() = default;
    ~Final6x86() = default;
    Final6x86(const Final6x86&) = delete;
    Final6x86& operator=(const Final6x86&) = delete;
    Final6x86(Final6x86&&) noexcept = default;
    Final6x86& operator=(Final6x86&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x87 {
public:
    Final6x87() = default;
    ~Final6x87() = default;
    Final6x87(const Final6x87&) = delete;
    Final6x87& operator=(const Final6x87&) = delete;
    Final6x87(Final6x87&&) noexcept = default;
    Final6x87& operator=(Final6x87&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x88 {
public:
    Final6x88() = default;
    ~Final6x88() = default;
    Final6x88(const Final6x88&) = delete;
    Final6x88& operator=(const Final6x88&) = delete;
    Final6x88(Final6x88&&) noexcept = default;
    Final6x88& operator=(Final6x88&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x89 {
public:
    Final6x89() = default;
    ~Final6x89() = default;
    Final6x89(const Final6x89&) = delete;
    Final6x89& operator=(const Final6x89&) = delete;
    Final6x89(Final6x89&&) noexcept = default;
    Final6x89& operator=(Final6x89&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x90 {
public:
    Final6x90() = default;
    ~Final6x90() = default;
    Final6x90(const Final6x90&) = delete;
    Final6x90& operator=(const Final6x90&) = delete;
    Final6x90(Final6x90&&) noexcept = default;
    Final6x90& operator=(Final6x90&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x91 {
public:
    Final6x91() = default;
    ~Final6x91() = default;
    Final6x91(const Final6x91&) = delete;
    Final6x91& operator=(const Final6x91&) = delete;
    Final6x91(Final6x91&&) noexcept = default;
    Final6x91& operator=(Final6x91&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x92 {
public:
    Final6x92() = default;
    ~Final6x92() = default;
    Final6x92(const Final6x92&) = delete;
    Final6x92& operator=(const Final6x92&) = delete;
    Final6x92(Final6x92&&) noexcept = default;
    Final6x92& operator=(Final6x92&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x93 {
public:
    Final6x93() = default;
    ~Final6x93() = default;
    Final6x93(const Final6x93&) = delete;
    Final6x93& operator=(const Final6x93&) = delete;
    Final6x93(Final6x93&&) noexcept = default;
    Final6x93& operator=(Final6x93&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x94 {
public:
    Final6x94() = default;
    ~Final6x94() = default;
    Final6x94(const Final6x94&) = delete;
    Final6x94& operator=(const Final6x94&) = delete;
    Final6x94(Final6x94&&) noexcept = default;
    Final6x94& operator=(Final6x94&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x95 {
public:
    Final6x95() = default;
    ~Final6x95() = default;
    Final6x95(const Final6x95&) = delete;
    Final6x95& operator=(const Final6x95&) = delete;
    Final6x95(Final6x95&&) noexcept = default;
    Final6x95& operator=(Final6x95&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x96 {
public:
    Final6x96() = default;
    ~Final6x96() = default;
    Final6x96(const Final6x96&) = delete;
    Final6x96& operator=(const Final6x96&) = delete;
    Final6x96(Final6x96&&) noexcept = default;
    Final6x96& operator=(Final6x96&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x97 {
public:
    Final6x97() = default;
    ~Final6x97() = default;
    Final6x97(const Final6x97&) = delete;
    Final6x97& operator=(const Final6x97&) = delete;
    Final6x97(Final6x97&&) noexcept = default;
    Final6x97& operator=(Final6x97&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x98 {
public:
    Final6x98() = default;
    ~Final6x98() = default;
    Final6x98(const Final6x98&) = delete;
    Final6x98& operator=(const Final6x98&) = delete;
    Final6x98(Final6x98&&) noexcept = default;
    Final6x98& operator=(Final6x98&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x99 {
public:
    Final6x99() = default;
    ~Final6x99() = default;
    Final6x99(const Final6x99&) = delete;
    Final6x99& operator=(const Final6x99&) = delete;
    Final6x99(Final6x99&&) noexcept = default;
    Final6x99& operator=(Final6x99&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x100 {
public:
    Final6x100() = default;
    ~Final6x100() = default;
    Final6x100(const Final6x100&) = delete;
    Final6x100& operator=(const Final6x100&) = delete;
    Final6x100(Final6x100&&) noexcept = default;
    Final6x100& operator=(Final6x100&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x101 {
public:
    Final6x101() = default;
    ~Final6x101() = default;
    Final6x101(const Final6x101&) = delete;
    Final6x101& operator=(const Final6x101&) = delete;
    Final6x101(Final6x101&&) noexcept = default;
    Final6x101& operator=(Final6x101&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x102 {
public:
    Final6x102() = default;
    ~Final6x102() = default;
    Final6x102(const Final6x102&) = delete;
    Final6x102& operator=(const Final6x102&) = delete;
    Final6x102(Final6x102&&) noexcept = default;
    Final6x102& operator=(Final6x102&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x103 {
public:
    Final6x103() = default;
    ~Final6x103() = default;
    Final6x103(const Final6x103&) = delete;
    Final6x103& operator=(const Final6x103&) = delete;
    Final6x103(Final6x103&&) noexcept = default;
    Final6x103& operator=(Final6x103&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x104 {
public:
    Final6x104() = default;
    ~Final6x104() = default;
    Final6x104(const Final6x104&) = delete;
    Final6x104& operator=(const Final6x104&) = delete;
    Final6x104(Final6x104&&) noexcept = default;
    Final6x104& operator=(Final6x104&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x105 {
public:
    Final6x105() = default;
    ~Final6x105() = default;
    Final6x105(const Final6x105&) = delete;
    Final6x105& operator=(const Final6x105&) = delete;
    Final6x105(Final6x105&&) noexcept = default;
    Final6x105& operator=(Final6x105&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x106 {
public:
    Final6x106() = default;
    ~Final6x106() = default;
    Final6x106(const Final6x106&) = delete;
    Final6x106& operator=(const Final6x106&) = delete;
    Final6x106(Final6x106&&) noexcept = default;
    Final6x106& operator=(Final6x106&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x107 {
public:
    Final6x107() = default;
    ~Final6x107() = default;
    Final6x107(const Final6x107&) = delete;
    Final6x107& operator=(const Final6x107&) = delete;
    Final6x107(Final6x107&&) noexcept = default;
    Final6x107& operator=(Final6x107&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x108 {
public:
    Final6x108() = default;
    ~Final6x108() = default;
    Final6x108(const Final6x108&) = delete;
    Final6x108& operator=(const Final6x108&) = delete;
    Final6x108(Final6x108&&) noexcept = default;
    Final6x108& operator=(Final6x108&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x109 {
public:
    Final6x109() = default;
    ~Final6x109() = default;
    Final6x109(const Final6x109&) = delete;
    Final6x109& operator=(const Final6x109&) = delete;
    Final6x109(Final6x109&&) noexcept = default;
    Final6x109& operator=(Final6x109&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x110 {
public:
    Final6x110() = default;
    ~Final6x110() = default;
    Final6x110(const Final6x110&) = delete;
    Final6x110& operator=(const Final6x110&) = delete;
    Final6x110(Final6x110&&) noexcept = default;
    Final6x110& operator=(Final6x110&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x111 {
public:
    Final6x111() = default;
    ~Final6x111() = default;
    Final6x111(const Final6x111&) = delete;
    Final6x111& operator=(const Final6x111&) = delete;
    Final6x111(Final6x111&&) noexcept = default;
    Final6x111& operator=(Final6x111&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x112 {
public:
    Final6x112() = default;
    ~Final6x112() = default;
    Final6x112(const Final6x112&) = delete;
    Final6x112& operator=(const Final6x112&) = delete;
    Final6x112(Final6x112&&) noexcept = default;
    Final6x112& operator=(Final6x112&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x113 {
public:
    Final6x113() = default;
    ~Final6x113() = default;
    Final6x113(const Final6x113&) = delete;
    Final6x113& operator=(const Final6x113&) = delete;
    Final6x113(Final6x113&&) noexcept = default;
    Final6x113& operator=(Final6x113&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x114 {
public:
    Final6x114() = default;
    ~Final6x114() = default;
    Final6x114(const Final6x114&) = delete;
    Final6x114& operator=(const Final6x114&) = delete;
    Final6x114(Final6x114&&) noexcept = default;
    Final6x114& operator=(Final6x114&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x115 {
public:
    Final6x115() = default;
    ~Final6x115() = default;
    Final6x115(const Final6x115&) = delete;
    Final6x115& operator=(const Final6x115&) = delete;
    Final6x115(Final6x115&&) noexcept = default;
    Final6x115& operator=(Final6x115&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x116 {
public:
    Final6x116() = default;
    ~Final6x116() = default;
    Final6x116(const Final6x116&) = delete;
    Final6x116& operator=(const Final6x116&) = delete;
    Final6x116(Final6x116&&) noexcept = default;
    Final6x116& operator=(Final6x116&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x117 {
public:
    Final6x117() = default;
    ~Final6x117() = default;
    Final6x117(const Final6x117&) = delete;
    Final6x117& operator=(const Final6x117&) = delete;
    Final6x117(Final6x117&&) noexcept = default;
    Final6x117& operator=(Final6x117&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x118 {
public:
    Final6x118() = default;
    ~Final6x118() = default;
    Final6x118(const Final6x118&) = delete;
    Final6x118& operator=(const Final6x118&) = delete;
    Final6x118(Final6x118&&) noexcept = default;
    Final6x118& operator=(Final6x118&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x119 {
public:
    Final6x119() = default;
    ~Final6x119() = default;
    Final6x119(const Final6x119&) = delete;
    Final6x119& operator=(const Final6x119&) = delete;
    Final6x119(Final6x119&&) noexcept = default;
    Final6x119& operator=(Final6x119&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x120 {
public:
    Final6x120() = default;
    ~Final6x120() = default;
    Final6x120(const Final6x120&) = delete;
    Final6x120& operator=(const Final6x120&) = delete;
    Final6x120(Final6x120&&) noexcept = default;
    Final6x120& operator=(Final6x120&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x121 {
public:
    Final6x121() = default;
    ~Final6x121() = default;
    Final6x121(const Final6x121&) = delete;
    Final6x121& operator=(const Final6x121&) = delete;
    Final6x121(Final6x121&&) noexcept = default;
    Final6x121& operator=(Final6x121&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x122 {
public:
    Final6x122() = default;
    ~Final6x122() = default;
    Final6x122(const Final6x122&) = delete;
    Final6x122& operator=(const Final6x122&) = delete;
    Final6x122(Final6x122&&) noexcept = default;
    Final6x122& operator=(Final6x122&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x123 {
public:
    Final6x123() = default;
    ~Final6x123() = default;
    Final6x123(const Final6x123&) = delete;
    Final6x123& operator=(const Final6x123&) = delete;
    Final6x123(Final6x123&&) noexcept = default;
    Final6x123& operator=(Final6x123&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x124 {
public:
    Final6x124() = default;
    ~Final6x124() = default;
    Final6x124(const Final6x124&) = delete;
    Final6x124& operator=(const Final6x124&) = delete;
    Final6x124(Final6x124&&) noexcept = default;
    Final6x124& operator=(Final6x124&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x125 {
public:
    Final6x125() = default;
    ~Final6x125() = default;
    Final6x125(const Final6x125&) = delete;
    Final6x125& operator=(const Final6x125&) = delete;
    Final6x125(Final6x125&&) noexcept = default;
    Final6x125& operator=(Final6x125&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x126 {
public:
    Final6x126() = default;
    ~Final6x126() = default;
    Final6x126(const Final6x126&) = delete;
    Final6x126& operator=(const Final6x126&) = delete;
    Final6x126(Final6x126&&) noexcept = default;
    Final6x126& operator=(Final6x126&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x127 {
public:
    Final6x127() = default;
    ~Final6x127() = default;
    Final6x127(const Final6x127&) = delete;
    Final6x127& operator=(const Final6x127&) = delete;
    Final6x127(Final6x127&&) noexcept = default;
    Final6x127& operator=(Final6x127&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x128 {
public:
    Final6x128() = default;
    ~Final6x128() = default;
    Final6x128(const Final6x128&) = delete;
    Final6x128& operator=(const Final6x128&) = delete;
    Final6x128(Final6x128&&) noexcept = default;
    Final6x128& operator=(Final6x128&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x129 {
public:
    Final6x129() = default;
    ~Final6x129() = default;
    Final6x129(const Final6x129&) = delete;
    Final6x129& operator=(const Final6x129&) = delete;
    Final6x129(Final6x129&&) noexcept = default;
    Final6x129& operator=(Final6x129&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x130 {
public:
    Final6x130() = default;
    ~Final6x130() = default;
    Final6x130(const Final6x130&) = delete;
    Final6x130& operator=(const Final6x130&) = delete;
    Final6x130(Final6x130&&) noexcept = default;
    Final6x130& operator=(Final6x130&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x131 {
public:
    Final6x131() = default;
    ~Final6x131() = default;
    Final6x131(const Final6x131&) = delete;
    Final6x131& operator=(const Final6x131&) = delete;
    Final6x131(Final6x131&&) noexcept = default;
    Final6x131& operator=(Final6x131&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x132 {
public:
    Final6x132() = default;
    ~Final6x132() = default;
    Final6x132(const Final6x132&) = delete;
    Final6x132& operator=(const Final6x132&) = delete;
    Final6x132(Final6x132&&) noexcept = default;
    Final6x132& operator=(Final6x132&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x133 {
public:
    Final6x133() = default;
    ~Final6x133() = default;
    Final6x133(const Final6x133&) = delete;
    Final6x133& operator=(const Final6x133&) = delete;
    Final6x133(Final6x133&&) noexcept = default;
    Final6x133& operator=(Final6x133&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x134 {
public:
    Final6x134() = default;
    ~Final6x134() = default;
    Final6x134(const Final6x134&) = delete;
    Final6x134& operator=(const Final6x134&) = delete;
    Final6x134(Final6x134&&) noexcept = default;
    Final6x134& operator=(Final6x134&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x135 {
public:
    Final6x135() = default;
    ~Final6x135() = default;
    Final6x135(const Final6x135&) = delete;
    Final6x135& operator=(const Final6x135&) = delete;
    Final6x135(Final6x135&&) noexcept = default;
    Final6x135& operator=(Final6x135&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x136 {
public:
    Final6x136() = default;
    ~Final6x136() = default;
    Final6x136(const Final6x136&) = delete;
    Final6x136& operator=(const Final6x136&) = delete;
    Final6x136(Final6x136&&) noexcept = default;
    Final6x136& operator=(Final6x136&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x137 {
public:
    Final6x137() = default;
    ~Final6x137() = default;
    Final6x137(const Final6x137&) = delete;
    Final6x137& operator=(const Final6x137&) = delete;
    Final6x137(Final6x137&&) noexcept = default;
    Final6x137& operator=(Final6x137&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x138 {
public:
    Final6x138() = default;
    ~Final6x138() = default;
    Final6x138(const Final6x138&) = delete;
    Final6x138& operator=(const Final6x138&) = delete;
    Final6x138(Final6x138&&) noexcept = default;
    Final6x138& operator=(Final6x138&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x139 {
public:
    Final6x139() = default;
    ~Final6x139() = default;
    Final6x139(const Final6x139&) = delete;
    Final6x139& operator=(const Final6x139&) = delete;
    Final6x139(Final6x139&&) noexcept = default;
    Final6x139& operator=(Final6x139&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x140 {
public:
    Final6x140() = default;
    ~Final6x140() = default;
    Final6x140(const Final6x140&) = delete;
    Final6x140& operator=(const Final6x140&) = delete;
    Final6x140(Final6x140&&) noexcept = default;
    Final6x140& operator=(Final6x140&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x141 {
public:
    Final6x141() = default;
    ~Final6x141() = default;
    Final6x141(const Final6x141&) = delete;
    Final6x141& operator=(const Final6x141&) = delete;
    Final6x141(Final6x141&&) noexcept = default;
    Final6x141& operator=(Final6x141&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x142 {
public:
    Final6x142() = default;
    ~Final6x142() = default;
    Final6x142(const Final6x142&) = delete;
    Final6x142& operator=(const Final6x142&) = delete;
    Final6x142(Final6x142&&) noexcept = default;
    Final6x142& operator=(Final6x142&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x143 {
public:
    Final6x143() = default;
    ~Final6x143() = default;
    Final6x143(const Final6x143&) = delete;
    Final6x143& operator=(const Final6x143&) = delete;
    Final6x143(Final6x143&&) noexcept = default;
    Final6x143& operator=(Final6x143&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x144 {
public:
    Final6x144() = default;
    ~Final6x144() = default;
    Final6x144(const Final6x144&) = delete;
    Final6x144& operator=(const Final6x144&) = delete;
    Final6x144(Final6x144&&) noexcept = default;
    Final6x144& operator=(Final6x144&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x145 {
public:
    Final6x145() = default;
    ~Final6x145() = default;
    Final6x145(const Final6x145&) = delete;
    Final6x145& operator=(const Final6x145&) = delete;
    Final6x145(Final6x145&&) noexcept = default;
    Final6x145& operator=(Final6x145&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x146 {
public:
    Final6x146() = default;
    ~Final6x146() = default;
    Final6x146(const Final6x146&) = delete;
    Final6x146& operator=(const Final6x146&) = delete;
    Final6x146(Final6x146&&) noexcept = default;
    Final6x146& operator=(Final6x146&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x147 {
public:
    Final6x147() = default;
    ~Final6x147() = default;
    Final6x147(const Final6x147&) = delete;
    Final6x147& operator=(const Final6x147&) = delete;
    Final6x147(Final6x147&&) noexcept = default;
    Final6x147& operator=(Final6x147&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x148 {
public:
    Final6x148() = default;
    ~Final6x148() = default;
    Final6x148(const Final6x148&) = delete;
    Final6x148& operator=(const Final6x148&) = delete;
    Final6x148(Final6x148&&) noexcept = default;
    Final6x148& operator=(Final6x148&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x149 {
public:
    Final6x149() = default;
    ~Final6x149() = default;
    Final6x149(const Final6x149&) = delete;
    Final6x149& operator=(const Final6x149&) = delete;
    Final6x149(Final6x149&&) noexcept = default;
    Final6x149& operator=(Final6x149&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x150 {
public:
    Final6x150() = default;
    ~Final6x150() = default;
    Final6x150(const Final6x150&) = delete;
    Final6x150& operator=(const Final6x150&) = delete;
    Final6x150(Final6x150&&) noexcept = default;
    Final6x150& operator=(Final6x150&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x151 {
public:
    Final6x151() = default;
    ~Final6x151() = default;
    Final6x151(const Final6x151&) = delete;
    Final6x151& operator=(const Final6x151&) = delete;
    Final6x151(Final6x151&&) noexcept = default;
    Final6x151& operator=(Final6x151&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x152 {
public:
    Final6x152() = default;
    ~Final6x152() = default;
    Final6x152(const Final6x152&) = delete;
    Final6x152& operator=(const Final6x152&) = delete;
    Final6x152(Final6x152&&) noexcept = default;
    Final6x152& operator=(Final6x152&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x153 {
public:
    Final6x153() = default;
    ~Final6x153() = default;
    Final6x153(const Final6x153&) = delete;
    Final6x153& operator=(const Final6x153&) = delete;
    Final6x153(Final6x153&&) noexcept = default;
    Final6x153& operator=(Final6x153&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x154 {
public:
    Final6x154() = default;
    ~Final6x154() = default;
    Final6x154(const Final6x154&) = delete;
    Final6x154& operator=(const Final6x154&) = delete;
    Final6x154(Final6x154&&) noexcept = default;
    Final6x154& operator=(Final6x154&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x155 {
public:
    Final6x155() = default;
    ~Final6x155() = default;
    Final6x155(const Final6x155&) = delete;
    Final6x155& operator=(const Final6x155&) = delete;
    Final6x155(Final6x155&&) noexcept = default;
    Final6x155& operator=(Final6x155&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x156 {
public:
    Final6x156() = default;
    ~Final6x156() = default;
    Final6x156(const Final6x156&) = delete;
    Final6x156& operator=(const Final6x156&) = delete;
    Final6x156(Final6x156&&) noexcept = default;
    Final6x156& operator=(Final6x156&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x157 {
public:
    Final6x157() = default;
    ~Final6x157() = default;
    Final6x157(const Final6x157&) = delete;
    Final6x157& operator=(const Final6x157&) = delete;
    Final6x157(Final6x157&&) noexcept = default;
    Final6x157& operator=(Final6x157&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x158 {
public:
    Final6x158() = default;
    ~Final6x158() = default;
    Final6x158(const Final6x158&) = delete;
    Final6x158& operator=(const Final6x158&) = delete;
    Final6x158(Final6x158&&) noexcept = default;
    Final6x158& operator=(Final6x158&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x159 {
public:
    Final6x159() = default;
    ~Final6x159() = default;
    Final6x159(const Final6x159&) = delete;
    Final6x159& operator=(const Final6x159&) = delete;
    Final6x159(Final6x159&&) noexcept = default;
    Final6x159& operator=(Final6x159&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x160 {
public:
    Final6x160() = default;
    ~Final6x160() = default;
    Final6x160(const Final6x160&) = delete;
    Final6x160& operator=(const Final6x160&) = delete;
    Final6x160(Final6x160&&) noexcept = default;
    Final6x160& operator=(Final6x160&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x161 {
public:
    Final6x161() = default;
    ~Final6x161() = default;
    Final6x161(const Final6x161&) = delete;
    Final6x161& operator=(const Final6x161&) = delete;
    Final6x161(Final6x161&&) noexcept = default;
    Final6x161& operator=(Final6x161&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x162 {
public:
    Final6x162() = default;
    ~Final6x162() = default;
    Final6x162(const Final6x162&) = delete;
    Final6x162& operator=(const Final6x162&) = delete;
    Final6x162(Final6x162&&) noexcept = default;
    Final6x162& operator=(Final6x162&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x163 {
public:
    Final6x163() = default;
    ~Final6x163() = default;
    Final6x163(const Final6x163&) = delete;
    Final6x163& operator=(const Final6x163&) = delete;
    Final6x163(Final6x163&&) noexcept = default;
    Final6x163& operator=(Final6x163&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x164 {
public:
    Final6x164() = default;
    ~Final6x164() = default;
    Final6x164(const Final6x164&) = delete;
    Final6x164& operator=(const Final6x164&) = delete;
    Final6x164(Final6x164&&) noexcept = default;
    Final6x164& operator=(Final6x164&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x165 {
public:
    Final6x165() = default;
    ~Final6x165() = default;
    Final6x165(const Final6x165&) = delete;
    Final6x165& operator=(const Final6x165&) = delete;
    Final6x165(Final6x165&&) noexcept = default;
    Final6x165& operator=(Final6x165&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x166 {
public:
    Final6x166() = default;
    ~Final6x166() = default;
    Final6x166(const Final6x166&) = delete;
    Final6x166& operator=(const Final6x166&) = delete;
    Final6x166(Final6x166&&) noexcept = default;
    Final6x166& operator=(Final6x166&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x167 {
public:
    Final6x167() = default;
    ~Final6x167() = default;
    Final6x167(const Final6x167&) = delete;
    Final6x167& operator=(const Final6x167&) = delete;
    Final6x167(Final6x167&&) noexcept = default;
    Final6x167& operator=(Final6x167&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x168 {
public:
    Final6x168() = default;
    ~Final6x168() = default;
    Final6x168(const Final6x168&) = delete;
    Final6x168& operator=(const Final6x168&) = delete;
    Final6x168(Final6x168&&) noexcept = default;
    Final6x168& operator=(Final6x168&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x169 {
public:
    Final6x169() = default;
    ~Final6x169() = default;
    Final6x169(const Final6x169&) = delete;
    Final6x169& operator=(const Final6x169&) = delete;
    Final6x169(Final6x169&&) noexcept = default;
    Final6x169& operator=(Final6x169&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x170 {
public:
    Final6x170() = default;
    ~Final6x170() = default;
    Final6x170(const Final6x170&) = delete;
    Final6x170& operator=(const Final6x170&) = delete;
    Final6x170(Final6x170&&) noexcept = default;
    Final6x170& operator=(Final6x170&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x171 {
public:
    Final6x171() = default;
    ~Final6x171() = default;
    Final6x171(const Final6x171&) = delete;
    Final6x171& operator=(const Final6x171&) = delete;
    Final6x171(Final6x171&&) noexcept = default;
    Final6x171& operator=(Final6x171&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x172 {
public:
    Final6x172() = default;
    ~Final6x172() = default;
    Final6x172(const Final6x172&) = delete;
    Final6x172& operator=(const Final6x172&) = delete;
    Final6x172(Final6x172&&) noexcept = default;
    Final6x172& operator=(Final6x172&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x173 {
public:
    Final6x173() = default;
    ~Final6x173() = default;
    Final6x173(const Final6x173&) = delete;
    Final6x173& operator=(const Final6x173&) = delete;
    Final6x173(Final6x173&&) noexcept = default;
    Final6x173& operator=(Final6x173&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x174 {
public:
    Final6x174() = default;
    ~Final6x174() = default;
    Final6x174(const Final6x174&) = delete;
    Final6x174& operator=(const Final6x174&) = delete;
    Final6x174(Final6x174&&) noexcept = default;
    Final6x174& operator=(Final6x174&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x175 {
public:
    Final6x175() = default;
    ~Final6x175() = default;
    Final6x175(const Final6x175&) = delete;
    Final6x175& operator=(const Final6x175&) = delete;
    Final6x175(Final6x175&&) noexcept = default;
    Final6x175& operator=(Final6x175&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x176 {
public:
    Final6x176() = default;
    ~Final6x176() = default;
    Final6x176(const Final6x176&) = delete;
    Final6x176& operator=(const Final6x176&) = delete;
    Final6x176(Final6x176&&) noexcept = default;
    Final6x176& operator=(Final6x176&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x177 {
public:
    Final6x177() = default;
    ~Final6x177() = default;
    Final6x177(const Final6x177&) = delete;
    Final6x177& operator=(const Final6x177&) = delete;
    Final6x177(Final6x177&&) noexcept = default;
    Final6x177& operator=(Final6x177&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x178 {
public:
    Final6x178() = default;
    ~Final6x178() = default;
    Final6x178(const Final6x178&) = delete;
    Final6x178& operator=(const Final6x178&) = delete;
    Final6x178(Final6x178&&) noexcept = default;
    Final6x178& operator=(Final6x178&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x179 {
public:
    Final6x179() = default;
    ~Final6x179() = default;
    Final6x179(const Final6x179&) = delete;
    Final6x179& operator=(const Final6x179&) = delete;
    Final6x179(Final6x179&&) noexcept = default;
    Final6x179& operator=(Final6x179&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x180 {
public:
    Final6x180() = default;
    ~Final6x180() = default;
    Final6x180(const Final6x180&) = delete;
    Final6x180& operator=(const Final6x180&) = delete;
    Final6x180(Final6x180&&) noexcept = default;
    Final6x180& operator=(Final6x180&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x181 {
public:
    Final6x181() = default;
    ~Final6x181() = default;
    Final6x181(const Final6x181&) = delete;
    Final6x181& operator=(const Final6x181&) = delete;
    Final6x181(Final6x181&&) noexcept = default;
    Final6x181& operator=(Final6x181&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x182 {
public:
    Final6x182() = default;
    ~Final6x182() = default;
    Final6x182(const Final6x182&) = delete;
    Final6x182& operator=(const Final6x182&) = delete;
    Final6x182(Final6x182&&) noexcept = default;
    Final6x182& operator=(Final6x182&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x183 {
public:
    Final6x183() = default;
    ~Final6x183() = default;
    Final6x183(const Final6x183&) = delete;
    Final6x183& operator=(const Final6x183&) = delete;
    Final6x183(Final6x183&&) noexcept = default;
    Final6x183& operator=(Final6x183&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x184 {
public:
    Final6x184() = default;
    ~Final6x184() = default;
    Final6x184(const Final6x184&) = delete;
    Final6x184& operator=(const Final6x184&) = delete;
    Final6x184(Final6x184&&) noexcept = default;
    Final6x184& operator=(Final6x184&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x185 {
public:
    Final6x185() = default;
    ~Final6x185() = default;
    Final6x185(const Final6x185&) = delete;
    Final6x185& operator=(const Final6x185&) = delete;
    Final6x185(Final6x185&&) noexcept = default;
    Final6x185& operator=(Final6x185&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x186 {
public:
    Final6x186() = default;
    ~Final6x186() = default;
    Final6x186(const Final6x186&) = delete;
    Final6x186& operator=(const Final6x186&) = delete;
    Final6x186(Final6x186&&) noexcept = default;
    Final6x186& operator=(Final6x186&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x187 {
public:
    Final6x187() = default;
    ~Final6x187() = default;
    Final6x187(const Final6x187&) = delete;
    Final6x187& operator=(const Final6x187&) = delete;
    Final6x187(Final6x187&&) noexcept = default;
    Final6x187& operator=(Final6x187&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x188 {
public:
    Final6x188() = default;
    ~Final6x188() = default;
    Final6x188(const Final6x188&) = delete;
    Final6x188& operator=(const Final6x188&) = delete;
    Final6x188(Final6x188&&) noexcept = default;
    Final6x188& operator=(Final6x188&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x189 {
public:
    Final6x189() = default;
    ~Final6x189() = default;
    Final6x189(const Final6x189&) = delete;
    Final6x189& operator=(const Final6x189&) = delete;
    Final6x189(Final6x189&&) noexcept = default;
    Final6x189& operator=(Final6x189&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x190 {
public:
    Final6x190() = default;
    ~Final6x190() = default;
    Final6x190(const Final6x190&) = delete;
    Final6x190& operator=(const Final6x190&) = delete;
    Final6x190(Final6x190&&) noexcept = default;
    Final6x190& operator=(Final6x190&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x191 {
public:
    Final6x191() = default;
    ~Final6x191() = default;
    Final6x191(const Final6x191&) = delete;
    Final6x191& operator=(const Final6x191&) = delete;
    Final6x191(Final6x191&&) noexcept = default;
    Final6x191& operator=(Final6x191&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x192 {
public:
    Final6x192() = default;
    ~Final6x192() = default;
    Final6x192(const Final6x192&) = delete;
    Final6x192& operator=(const Final6x192&) = delete;
    Final6x192(Final6x192&&) noexcept = default;
    Final6x192& operator=(Final6x192&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x193 {
public:
    Final6x193() = default;
    ~Final6x193() = default;
    Final6x193(const Final6x193&) = delete;
    Final6x193& operator=(const Final6x193&) = delete;
    Final6x193(Final6x193&&) noexcept = default;
    Final6x193& operator=(Final6x193&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x194 {
public:
    Final6x194() = default;
    ~Final6x194() = default;
    Final6x194(const Final6x194&) = delete;
    Final6x194& operator=(const Final6x194&) = delete;
    Final6x194(Final6x194&&) noexcept = default;
    Final6x194& operator=(Final6x194&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x195 {
public:
    Final6x195() = default;
    ~Final6x195() = default;
    Final6x195(const Final6x195&) = delete;
    Final6x195& operator=(const Final6x195&) = delete;
    Final6x195(Final6x195&&) noexcept = default;
    Final6x195& operator=(Final6x195&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x196 {
public:
    Final6x196() = default;
    ~Final6x196() = default;
    Final6x196(const Final6x196&) = delete;
    Final6x196& operator=(const Final6x196&) = delete;
    Final6x196(Final6x196&&) noexcept = default;
    Final6x196& operator=(Final6x196&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x197 {
public:
    Final6x197() = default;
    ~Final6x197() = default;
    Final6x197(const Final6x197&) = delete;
    Final6x197& operator=(const Final6x197&) = delete;
    Final6x197(Final6x197&&) noexcept = default;
    Final6x197& operator=(Final6x197&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x198 {
public:
    Final6x198() = default;
    ~Final6x198() = default;
    Final6x198(const Final6x198&) = delete;
    Final6x198& operator=(const Final6x198&) = delete;
    Final6x198(Final6x198&&) noexcept = default;
    Final6x198& operator=(Final6x198&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
        auto o = ops_.load(); return o > 0 ? static_cast<double>(errors_.load())/o : 0.0; }
    void reset() { ops_.store(0); errors_.store(0); ready_.store(false); }
private:
    std::atomic<bool> ready_{false};
    std::atomic<bool> active_{false};
    std::atomic<int64_t> ops_{0};
    std::atomic<int64_t> errors_{0};
    std::chrono::steady_clock::time_point start_time_;
};

class Final6x199 {
public:
    Final6x199() = default;
    ~Final6x199() = default;
    Final6x199(const Final6x199&) = delete;
    Final6x199& operator=(const Final6x199&) = delete;
    Final6x199(Final6x199&&) noexcept = default;
    Final6x199& operator=(Final6x199&&) noexcept = default;
    void initialize() { ready_.store(true); }
    void start() { active_.store(true); start_time_ = std::chrono::steady_clock::now(); }
    void stop() { active_.store(false); }
    [[nodiscard]] bool ready() const noexcept { return ready_.load(); }
    [[nodiscard]] bool active() const noexcept { return active_.load(); }
    [[nodiscard]] int64_t ops() const noexcept { return ops_.load(); }
    [[nodiscard]] int64_t errors() const noexcept { return errors_.load(); }
    void record_op() noexcept { ops_.fetch_add(1, std::memory_order_relaxed); }
    void record_error() noexcept { errors_.fetch_add(1, std::memory_order_relaxed); }
    [[nodiscard]] double error_rate() const noexcept {
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