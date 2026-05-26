#include "torrent/common/types.h"
#include <atomic>
#include <string>
#include <vector>
#include <mutex>
#include <chrono>
#include <memory>

namespace torrent::generated_6 { namespace {
struct Gen6x0 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x1 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x2 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x3 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x4 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x5 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x6 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x7 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x8 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x9 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x10 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x11 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x12 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x13 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x14 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x15 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x16 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x17 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x18 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x19 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x20 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x21 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x22 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x23 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x24 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x25 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x26 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x27 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x28 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x29 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x30 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x31 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x32 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x33 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x34 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x35 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x36 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x37 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x38 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x39 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x40 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x41 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x42 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x43 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x44 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x45 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x46 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x47 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x48 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x49 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x50 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x51 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x52 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x53 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x54 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x55 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x56 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x57 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x58 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x59 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x60 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x61 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x62 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x63 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x64 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x65 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x66 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x67 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x68 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x69 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x70 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x71 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x72 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x73 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x74 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x75 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x76 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x77 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x78 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x79 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x80 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x81 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x82 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x83 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x84 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x85 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x86 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x87 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x88 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x89 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x90 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x91 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x92 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x93 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x94 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x95 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x96 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x97 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x98 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x99 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x100 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x101 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x102 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x103 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x104 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x105 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x106 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x107 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x108 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x109 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x110 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x111 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x112 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x113 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x114 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x115 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x116 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x117 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x118 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x119 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x120 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x121 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x122 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x123 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x124 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x125 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x126 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x127 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x128 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x129 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x130 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x131 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x132 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x133 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x134 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x135 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x136 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x137 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x138 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x139 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x140 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x141 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x142 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x143 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x144 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x145 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x146 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x147 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x148 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x149 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x150 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x151 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x152 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x153 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x154 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x155 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x156 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x157 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x158 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x159 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x160 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x161 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x162 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x163 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x164 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x165 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x166 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x167 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x168 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x169 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x170 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x171 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x172 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x173 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x174 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x175 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x176 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x177 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x178 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x179 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x180 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x181 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x182 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x183 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x184 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x185 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x186 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x187 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x188 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x189 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x190 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x191 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x192 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x193 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x194 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x195 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x196 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x197 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x198 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct Gen6x199 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> data;
    mutable std::mutex mtx;
    void inc() { std::lock_guard<std::mutex> lk(mtx); a++; b++; c++; d++; e++; }
    int64_t sum() const { return a+b+c+d+e; }
    void reset() { a=0;b=0;c=0;d=0;e=0; data.clear(); }
    void add_data(int64_t v) { std::lock_guard<std::mutex> lk(mtx); data.push_back(v); }
    size_t data_size() const { std::lock_guard<std::mutex> lk(mtx); return data.size(); }
    void activate() { active = true; }
    void deactivate() { active = false; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

} }