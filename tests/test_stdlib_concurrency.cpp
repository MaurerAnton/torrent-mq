#include <gtest/gtest.h>
#include <algorithm>
#include <thread>
#include <chrono>
#include <random>
#include <map>
#include <set>
#include <deque>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <condition_variable>

namespace torrent::test {
namespace {

// ============================================================================
// Concurrent Data Structure Tests
// ============================================================================
TEST(ConcurrencyTest, AtomicIncrement) {
    std::atomic<int> counter{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 10; t++) {
        threads.emplace_back([&]() {
            for (int i = 0; i < 1000; i++) counter.fetch_add(1);
        });
    }
    for (auto& t : threads) t.join();
    EXPECT_EQ(counter.load(), 10000);
}
TEST(ConcurrencyTest, AtomicCAS) {
    std::atomic<int> val{0};
    int expected = 0;
    EXPECT_TRUE(val.compare_exchange_strong(expected, 1));
    EXPECT_EQ(val.load(), 1);
    expected = 0;
    EXPECT_FALSE(val.compare_exchange_strong(expected, 2));
}
TEST(ConcurrencyTest, MutexExclusion) {
    std::mutex mtx;
    int counter = 0;
    std::vector<std::thread> threads;
    for (int t = 0; t < 5; t++) {
        threads.emplace_back([&]() {
            for (int i = 0; i < 1000; i++) {
                std::lock_guard<std::mutex> lk(mtx);
                counter++;
            }
        });
    }
    for (auto& t : threads) t.join();
    EXPECT_EQ(counter, 5000);
}
TEST(ConcurrencyTest, SharedMutexReads) {
    std::shared_mutex smtx;
    std::atomic<int> reads{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 10; t++) {
        threads.emplace_back([&]() {
            for (int i = 0; i < 100; i++) {
                std::shared_lock lk(smtx);
                reads.fetch_add(1);
            }
        });
    }
    for (auto& t : threads) t.join();
    EXPECT_EQ(reads.load(), 1000);
}
TEST(ConcurrencyTest, ConditionVariable) {
    std::mutex mtx;
    std::condition_variable cv;
    bool ready = false;
    int value = 0;

    std::thread producer([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        {
            std::lock_guard<std::mutex> lk(mtx);
            ready = true;
            value = 42;
        }
        cv.notify_one();
    });

    {
        std::unique_lock<std::mutex> lk(mtx);
        cv.wait(lk, [&]() { return ready; });
        EXPECT_EQ(value, 42);
    }
    producer.join();
}
TEST(ConcurrencyTest, ProducerConsumer) {
    std::mutex mtx;
    std::condition_variable cv;
    std::deque<int> queue;
    const int total_items = 100;
    std::atomic<int> consumed{0};

    std::thread producer([&]() {
        for (int i = 0; i < total_items; i++) {
            {
                std::lock_guard<std::mutex> lk(mtx);
                queue.push_back(i);
            }
            cv.notify_one();
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });

    std::thread consumer([&]() {
        while (consumed.load() < total_items) {
            std::unique_lock<std::mutex> lk(mtx);
            cv.wait_for(lk, std::chrono::milliseconds(100), [&]() {
                return !queue.empty();
            });
            while (!queue.empty()) {
                queue.pop_front();
                consumed.fetch_add(1);
            }
        }
    });

    producer.join();
    consumer.join();
    EXPECT_EQ(consumed.load(), total_items);
}

// ============================================================================
// Binary Search Tests
// ============================================================================
TEST(AlgorithmTest, LowerBound) {
    std::vector<int> v = {1, 3, 5, 7, 9, 11};
    auto it = std::lower_bound(v.begin(), v.end(), 6);
    EXPECT_EQ(*it, 7);
    EXPECT_EQ(std::distance(v.begin(), it), 3);
}
TEST(AlgorithmTest, UpperBound) {
    std::vector<int> v = {1, 3, 5, 7, 9, 11};
    auto it = std::upper_bound(v.begin(), v.end(), 7);
    EXPECT_EQ(*it, 9);
}
TEST(AlgorithmTest, BinarySearchFound) {
    std::vector<int> v = {1, 3, 5, 7, 9, 11};
    EXPECT_TRUE(std::binary_search(v.begin(), v.end(), 7));
}
TEST(AlgorithmTest, BinarySearchNotFound) {
    std::vector<int> v = {1, 3, 5, 7, 9, 11};
    EXPECT_FALSE(std::binary_search(v.begin(), v.end(), 6));
}
TEST(AlgorithmTest, SortAndUnique) {
    std::vector<int> v = {3, 1, 4, 1, 5, 9, 2, 6, 5, 3};
    std::sort(v.begin(), v.end());
    auto last = std::unique(v.begin(), v.end());
    v.erase(last, v.end());
    EXPECT_EQ(v.size(), 7);
}
TEST(AlgorithmTest, Partition) {
    std::vector<int> v = {1, 2, 3, 4, 5, 6, 7, 8};
    auto it = std::partition(v.begin(), v.end(), [](int x) { return x % 2 == 0; });
    for (auto p = v.begin(); p != it; ++p) EXPECT_EQ(*p % 2, 0);
    for (auto p = it; p != v.end(); ++p) EXPECT_EQ(*p % 2, 1);
}
TEST(AlgorithmTest, HeapOperations) {
    std::vector<int> v = {3, 1, 4, 1, 5, 9};
    std::make_heap(v.begin(), v.end());
    EXPECT_EQ(v.front(), 9);
    std::pop_heap(v.begin(), v.end());
    v.pop_back();
    EXPECT_EQ(v.front(), 5);
}
TEST(AlgorithmTest, NthElement) {
    std::vector<int> v = {5, 2, 8, 1, 9, 3, 7, 4, 6};
    std::nth_element(v.begin(), v.begin() + 4, v.end());
    int median = v[4];
    int less = 0, greater = 0;
    for (int x : v) {
        if (x < median) less++;
        if (x > median) greater++;
    }
    EXPECT_EQ(less, 4);
    EXPECT_EQ(greater, 4);
}

// ============================================================================
// Hashing Tests
// ============================================================================
TEST(HashTest, StringHash) {
    std::hash<std::string> hasher;
    auto h1 = hasher("hello");
    auto h2 = hasher("hello");
    auto h3 = hasher("world");
    EXPECT_EQ(h1, h2);
    EXPECT_NE(h1, h3);
}
TEST(HashTest, IntHash) {
    std::hash<int> hasher;
    EXPECT_EQ(hasher(42), hasher(42));
    EXPECT_NE(hasher(42), hasher(43));
}
TEST(HashTest, MapInsert) {
    std::unordered_map<std::string, int> map;
    map["one"] = 1;
    map["two"] = 2;
    EXPECT_EQ(map.size(), 2);
    EXPECT_EQ(map["one"], 1);
}
TEST(HashTest, SetInsert) {
    std::unordered_set<int> set;
    set.insert(1); set.insert(2); set.insert(3);
    EXPECT_EQ(set.size(), 3);
    EXPECT_TRUE(set.find(2) != set.end());
    EXPECT_FALSE(set.find(4) != set.end());
}

// ============================================================================
// Optional and Variant Tests
// ============================================================================
TEST(StdlibTest, OptionalHasValue) {
    std::optional<int> opt = 42;
    EXPECT_TRUE(opt.has_value());
    EXPECT_EQ(opt.value(), 42);
    EXPECT_EQ(*opt, 42);
}
TEST(StdlibTest, OptionalNullopt) {
    std::optional<int> opt;
    EXPECT_FALSE(opt.has_value());
    EXPECT_EQ(opt.value_or(99), 99);
}
TEST(StdlibTest, VariantInt) {
    std::variant<int, std::string, double> v = 42;
    EXPECT_EQ(std::get<int>(v), 42);
    EXPECT_TRUE(std::holds_alternative<int>(v));
}
TEST(StdlibTest, VariantString) {
    std::variant<int, std::string, double> v = std::string("hello");
    EXPECT_EQ(std::get<std::string>(v), "hello");
}

// ============================================================================
// Chrono Tests
// ============================================================================
TEST(ChronoTest, SteadyClock) {
    auto t1 = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    auto t2 = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1);
    EXPECT_GE(diff.count(), 5);
}
TEST(ChronoTest, SystemClock) {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    EXPECT_GT(time_t_now, 1700000000);
}
TEST(ChronoTest, DurationCast) {
    std::chrono::milliseconds ms(1500);
    auto sec = std::chrono::duration_cast<std::chrono::seconds>(ms);
    EXPECT_EQ(sec.count(), 1);
}

// ============================================================================
// Memory Tests
// ============================================================================
TEST(MemoryTest, UniquePtr) {
    auto ptr = std::make_unique<int>(42);
    EXPECT_EQ(*ptr, 42);
    *ptr = 100;
    EXPECT_EQ(*ptr, 100);
}
TEST(MemoryTest, SharedPtr) {
    auto ptr = std::make_shared<int>(42);
    EXPECT_EQ(ptr.use_count(), 1);
    auto ptr2 = ptr;
    EXPECT_EQ(ptr.use_count(), 2);
    EXPECT_EQ(*ptr2, 42);
}
TEST(MemoryTest, WeakPtr) {
    auto shared = std::make_shared<int>(42);
    std::weak_ptr<int> weak = shared;
    EXPECT_FALSE(weak.expired());
    shared.reset();
    EXPECT_TRUE(weak.expired());
}
TEST(MemoryTest, MakeUniqueArray) {
    auto arr = std::make_unique<int[]>(10);
    for (int i = 0; i < 10; i++) arr[i] = i * i;
    EXPECT_EQ(arr[5], 25);
}

// ============================================================================
// String Tests
// ============================================================================
TEST(StringTest, Find) {
    std::string s = "hello world";
    EXPECT_EQ(s.find("world"), 6);
    EXPECT_EQ(s.find("xyz"), std::string::npos);
}
TEST(StringTest, Substr) {
    std::string s = "hello world";
    EXPECT_EQ(s.substr(0, 5), "hello");
    EXPECT_EQ(s.substr(6), "world");
}
TEST(StringTest, Replace) {
    std::string s = "hello world";
    s.replace(0, 5, "hi");
    EXPECT_EQ(s, "hi world");
}
TEST(StringTest, ToString) {
    EXPECT_EQ(std::to_string(42), "42");
    EXPECT_EQ(std::to_string(3.14).substr(0, 4), "3.14");
}

// ============================================================================
// Vector Tests
// ============================================================================
TEST(VectorTest, Reserve) {
    std::vector<int> v;
    v.reserve(100);
    EXPECT_GE(v.capacity(), 100);
    EXPECT_EQ(v.size(), 0);
}
TEST(VectorTest, EmplaceBack) {
    std::vector<std::pair<int, std::string>> v;
    v.emplace_back(1, "one");
    v.emplace_back(2, "two");
    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(v[0].second, "one");
}
TEST(VectorTest, EraseRemove) {
    std::vector<int> v = {1, 2, 3, 2, 4, 2, 5};
    v.erase(std::remove(v.begin(), v.end(), 2), v.end());
    EXPECT_EQ(v.size(), 4);
}
TEST(VectorTest, Insert) {
    std::vector<int> v = {1, 5};
    v.insert(v.begin() + 1, {2, 3, 4});
    EXPECT_EQ(v.size(), 5);
    EXPECT_EQ(v[1], 2);
}

// ============================================================================
// Random Tests
// ============================================================================
TEST(RandomTest, MersenneTwister) {
    std::mt19937 gen(42);
    std::uniform_int_distribution<int> dist(1, 100);
    int sum = 0;
    for (int i = 0; i < 1000; i++) sum += dist(gen);
    double avg = static_cast<double>(sum) / 1000.0;
    EXPECT_GT(avg, 45.0);
    EXPECT_LT(avg, 55.0);
}
TEST(RandomTest, UniformReal) {
    std::mt19937 gen(12345);
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    for (int i = 0; i < 100; i++) {
        double val = dist(gen);
        EXPECT_GE(val, 0.0);
        EXPECT_LE(val, 1.0);
    }
}

// ============================================================================
// Map and Set Tests
// ============================================================================
TEST(MapTest, OrderedMap) {
    std::map<std::string, int> map;
    map["c"] = 3; map["a"] = 1; map["b"] = 2;
    EXPECT_EQ(map.begin()->first, "a");
    EXPECT_EQ(map.rbegin()->first, "c");
}
TEST(MapTest, LowerBound) {
    std::map<int, std::string> map;
    map[10] = "ten"; map[20] = "twenty"; map[30] = "thirty";
    auto it = map.lower_bound(15);
    EXPECT_EQ(it->first, 20);
}
TEST(SetTest, OrderedSet) {
    std::set<int> s = {3, 1, 4, 1, 5};
    EXPECT_EQ(s.size(), 4);
    EXPECT_EQ(*s.begin(), 1);
}

}  // namespace
}  // namespace torrent::test
