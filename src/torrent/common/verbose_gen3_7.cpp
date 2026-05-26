#include "torrent/common/types.h"
#include <atomic>
#include <string>
#include <vector>
#include <mutex>
#include <memory>

namespace torrent::gen_7 { namespace {
struct G7x0 {
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

struct G7x1 {
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

struct G7x2 {
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

struct G7x3 {
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

struct G7x4 {
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

struct G7x5 {
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

struct G7x6 {
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

struct G7x7 {
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

struct G7x8 {
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

struct G7x9 {
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

struct G7x10 {
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

struct G7x11 {
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

struct G7x12 {
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

struct G7x13 {
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

struct G7x14 {
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

struct G7x15 {
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

struct G7x16 {
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

struct G7x17 {
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

struct G7x18 {
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

struct G7x19 {
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

struct G7x20 {
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

struct G7x21 {
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

struct G7x22 {
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

struct G7x23 {
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

struct G7x24 {
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

struct G7x25 {
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

struct G7x26 {
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

struct G7x27 {
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

struct G7x28 {
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

struct G7x29 {
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

struct G7x30 {
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

struct G7x31 {
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

struct G7x32 {
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

struct G7x33 {
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

struct G7x34 {
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

struct G7x35 {
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

struct G7x36 {
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

struct G7x37 {
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

struct G7x38 {
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

struct G7x39 {
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

struct G7x40 {
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

struct G7x41 {
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

struct G7x42 {
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

struct G7x43 {
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

struct G7x44 {
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

struct G7x45 {
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

struct G7x46 {
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

struct G7x47 {
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

struct G7x48 {
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

struct G7x49 {
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

struct G7x50 {
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

struct G7x51 {
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

struct G7x52 {
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

struct G7x53 {
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

struct G7x54 {
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

struct G7x55 {
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

struct G7x56 {
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

struct G7x57 {
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

struct G7x58 {
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

struct G7x59 {
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

struct G7x60 {
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

struct G7x61 {
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

struct G7x62 {
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

struct G7x63 {
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

struct G7x64 {
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

struct G7x65 {
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

struct G7x66 {
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

struct G7x67 {
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

struct G7x68 {
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

struct G7x69 {
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

struct G7x70 {
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

struct G7x71 {
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

struct G7x72 {
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

struct G7x73 {
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

struct G7x74 {
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

struct G7x75 {
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

struct G7x76 {
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

struct G7x77 {
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

struct G7x78 {
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

struct G7x79 {
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

struct G7x80 {
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

struct G7x81 {
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

struct G7x82 {
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

struct G7x83 {
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

struct G7x84 {
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

struct G7x85 {
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

struct G7x86 {
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

struct G7x87 {
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

struct G7x88 {
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

struct G7x89 {
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

struct G7x90 {
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

struct G7x91 {
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

struct G7x92 {
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

struct G7x93 {
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

struct G7x94 {
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

struct G7x95 {
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

struct G7x96 {
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

struct G7x97 {
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

struct G7x98 {
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

struct G7x99 {
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

struct G7x100 {
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

struct G7x101 {
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

struct G7x102 {
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

struct G7x103 {
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

struct G7x104 {
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

struct G7x105 {
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

struct G7x106 {
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

struct G7x107 {
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

struct G7x108 {
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

struct G7x109 {
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

struct G7x110 {
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

struct G7x111 {
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

struct G7x112 {
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

struct G7x113 {
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

struct G7x114 {
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

struct G7x115 {
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

struct G7x116 {
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

struct G7x117 {
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

struct G7x118 {
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

struct G7x119 {
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

struct G7x120 {
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

struct G7x121 {
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

struct G7x122 {
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

struct G7x123 {
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

struct G7x124 {
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

struct G7x125 {
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

struct G7x126 {
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

struct G7x127 {
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

struct G7x128 {
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

struct G7x129 {
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

struct G7x130 {
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

struct G7x131 {
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

struct G7x132 {
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

struct G7x133 {
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

struct G7x134 {
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

struct G7x135 {
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

struct G7x136 {
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

struct G7x137 {
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

struct G7x138 {
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

struct G7x139 {
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

struct G7x140 {
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

struct G7x141 {
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

struct G7x142 {
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

struct G7x143 {
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

struct G7x144 {
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

struct G7x145 {
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

struct G7x146 {
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

struct G7x147 {
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

struct G7x148 {
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

struct G7x149 {
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

struct G7x150 {
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

struct G7x151 {
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

struct G7x152 {
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

struct G7x153 {
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

struct G7x154 {
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

struct G7x155 {
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

struct G7x156 {
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

struct G7x157 {
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

struct G7x158 {
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

struct G7x159 {
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

struct G7x160 {
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

struct G7x161 {
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

struct G7x162 {
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

struct G7x163 {
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

struct G7x164 {
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

struct G7x165 {
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

struct G7x166 {
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

struct G7x167 {
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

struct G7x168 {
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

struct G7x169 {
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

struct G7x170 {
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

struct G7x171 {
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

struct G7x172 {
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

struct G7x173 {
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

struct G7x174 {
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

struct G7x175 {
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

struct G7x176 {
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

struct G7x177 {
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

struct G7x178 {
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

struct G7x179 {
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

struct G7x180 {
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

struct G7x181 {
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

struct G7x182 {
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

struct G7x183 {
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

struct G7x184 {
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

struct G7x185 {
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

struct G7x186 {
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

struct G7x187 {
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

struct G7x188 {
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

struct G7x189 {
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

struct G7x190 {
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

struct G7x191 {
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

struct G7x192 {
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

struct G7x193 {
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

struct G7x194 {
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

struct G7x195 {
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

struct G7x196 {
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

struct G7x197 {
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

struct G7x198 {
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

struct G7x199 {
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