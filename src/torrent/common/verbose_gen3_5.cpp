#include "torrent/common/types.h"
#include <atomic>
#include <string>
#include <vector>
#include <mutex>
#include <memory>

namespace torrent::gen_5 { namespace {
struct G5x0 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x1 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x2 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x3 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x4 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x5 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x6 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x7 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x8 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x9 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x10 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x11 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x12 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x13 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x14 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x15 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x16 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x17 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x18 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x19 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x20 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x21 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x22 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x23 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x24 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x25 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x26 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x27 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x28 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x29 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x30 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x31 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x32 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x33 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x34 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x35 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x36 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x37 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x38 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x39 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x40 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x41 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x42 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x43 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x44 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x45 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x46 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x47 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x48 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x49 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x50 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x51 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x52 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x53 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x54 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x55 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x56 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x57 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x58 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x59 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x60 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x61 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x62 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x63 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x64 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x65 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x66 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x67 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x68 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x69 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x70 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x71 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x72 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x73 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x74 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x75 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x76 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x77 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x78 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x79 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x80 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x81 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x82 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x83 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x84 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x85 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x86 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x87 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x88 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x89 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x90 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x91 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x92 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x93 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x94 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x95 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x96 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x97 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x98 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x99 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x100 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x101 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x102 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x103 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x104 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x105 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x106 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x107 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x108 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x109 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x110 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x111 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x112 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x113 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x114 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x115 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x116 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x117 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x118 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x119 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x120 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x121 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x122 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x123 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x124 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x125 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x126 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x127 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x128 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x129 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x130 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x131 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x132 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x133 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x134 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x135 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x136 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x137 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x138 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x139 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x140 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x141 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x142 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x143 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x144 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x145 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x146 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x147 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x148 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x149 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x150 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x151 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x152 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x153 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x154 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x155 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x156 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x157 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x158 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x159 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x160 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x161 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x162 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x163 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x164 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x165 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x166 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x167 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x168 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x169 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x170 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x171 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x172 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x173 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x174 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x175 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x176 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x177 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x178 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x179 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x180 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x181 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x182 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x183 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x184 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x185 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x186 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x187 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x188 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x189 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x190 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x191 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x192 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x193 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x194 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x195 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x196 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x197 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x198 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

struct G5x199 {
    std::atomic<int64_t> a{0},b{0},c{0},d{0},e{0},f{0};
    std::string name;
    bool active{false};
    std::vector<int64_t> items;
    mutable std::mutex mtx;
    void inc_all() { std::lock_guard<std::mutex> lk(mtx); a++;b++;c++;d++;e++;f++; }
    int64_t total() const { return a+b+c+d+e+f; }
    void reset_all() { a=0;b=0;c=0;d=0;e=0;f=0; items.clear(); }
    void push(int64_t v) { std::lock_guard<std::mutex> lk(mtx); items.push_back(v); }
    size_t size() const { std::lock_guard<std::mutex> lk(mtx); return items.size(); }
    void activate() { active = true; }
    [[nodiscard]] bool is_active() const noexcept { return active; }
};

} }