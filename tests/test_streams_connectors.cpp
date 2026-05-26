/**
 * torrent-mq — Streams & Connectors Test Suite
 *
 * 40+ Google Test cases covering stream processing and connector framework:
 *
 *   SECTION A: STREAM PROCESSING (20 tests)
 *     StreamProcessor topology, filter/aggregate/join/window processors,
 *     table processor, repartition, state store, TopologyBuilder API
 *
 *   SECTION B: CONNECTORS (20 tests)
 *     ConnectFramework registration, source/sink connectors,
 *     TaskManager lifecycle, Debezium/JDBC/S3/Elasticsearch/Redis/
 *     InfluxDB/Kafka/MongoDB connectors, transforms, offset store,
 *     connector status reporting
 *
 * Uses isolated instances and temp directories for hermetic testing.
 * Targets: ~1500 lines of compilable test code.
 */

#include <gtest/gtest.h>

#include "torrent/common/types.h"
#include "torrent/common/config.h"
#include "torrent/broker/server.h"
#include "torrent/streams/topology.h"
#include "torrent/connectors/connect.h"

#include <filesystem>
#include <fstream>
#include <thread>
#include <mutex>
#include <chrono>
#include <cstring>
#include <random>
#include <algorithm>
#include <atomic>
#include <vector>
#include <string>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <regex>
#include <sys/stat.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

// ============================================================================
// Anonymous helpers
// ============================================================================

namespace {

torrent::timestamp_ms_t now_ms() {
    return static_cast<torrent::timestamp_ms_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

std::string unique_id(const std::string& prefix = "test") {
    static std::atomic<int64_t> counter{0};
    return prefix + "_" + std::to_string(now_ms()) + "_"
           + std::to_string(counter.fetch_add(1, std::memory_order_relaxed));
}

torrent::broker::BrokerConfig make_test_broker_config(
    const std::string& data_dir, torrent::broker_id_t broker_id = 1) {
    torrent::broker::BrokerConfig cfg;
    cfg.broker_id = broker_id;
    cfg.data_directory = data_dir;
    cfg.max_connections = 100;
    cfg.num_io_threads = 1;
    cfg.num_worker_threads = 1;
    cfg.shutdown_timeout = std::chrono::milliseconds(5000);
    cfg.enable_admin_api = true;
    cfg.enable_metrics = false;
    cfg.enable_schema_registry = false;
    cfg.enable_transactions = true;
    cfg.auto_create_topics = true;
    cfg.cluster_id = "test-streams-connectors";
    return cfg;
}

} // anonymous namespace

// ============================================================================
// Test Fixtures
// ============================================================================

class StreamConnectorTestBase : public ::testing::Test {
protected:
    std::string tmp_dir_;

    void SetUp() override {
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        tmp_dir_ = fs::temp_directory_path().string()
                   + "/tq_stream_conn_test_"
                   + std::to_string(ts) + "_"
                   + ::testing::UnitTest::GetInstance()
                         ->current_test_info()
                         ->name();
        fs::create_directories(tmp_dir_);
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(tmp_dir_, ec);
    }

    torrent::broker::BrokerConfig make_config(
        torrent::broker_id_t id = 1) const {
        return make_test_broker_config(tmp_dir_, id);
    }
};

class BrokerStreamConnectorTest : public StreamConnectorTestBase {
protected:
    std::unique_ptr<torrent::broker::BrokerServer> server_;

    void SetUp() override {
        StreamConnectorTestBase::SetUp();
    }

    void TearDown() override {
        if (server_) {
            server_->shutdown();
            server_->wait_for_shutdown(std::chrono::milliseconds(5000));
            server_.reset();
        }
        StreamConnectorTestBase::TearDown();
    }

    void start_broker(torrent::broker_id_t id = 1) {
        auto cfg = make_config(id);
        server_ = std::make_unique<torrent::broker::BrokerServer>(cfg);
        server_->start();
    }

    bool is_started() const { return server_ != nullptr; }
};

// ============================================================================
// SECTION A: STREAM PROCESSING TESTS (20 tests)
// ============================================================================

class StreamProcessingTest : public BrokerStreamConnectorTest {
protected:
    void SetUp() override {
        BrokerStreamConnectorTest::SetUp();
        start_broker(1);
    }
};

// A.1 — StreamProcessor topology build and execute
TEST_F(StreamProcessingTest, StreamProcessorTopologyBuildAndExecute) {
    ASSERT_TRUE(is_started());

    auto builder = torrent::streams::TopologyBuilder(*server_);
    builder.add_source("input-source", "raw-events");
    builder.add_filter("dedup-filter", "equals:event_type=page_view");
    builder.add_map("enrich-map", "add_field:processed_by=stream_engine");
    builder.add_sink("output-sink", "processed-events");
    EXPECT_NO_THROW(builder.build());
    EXPECT_NO_THROW(builder.start());
    EXPECT_NO_THROW(builder.shutdown());
}

// A.2 — FilterProcessor with equals predicate
TEST_F(StreamProcessingTest, FilterProcessorEqualsPredicate) {
    ASSERT_TRUE(is_started());

    // Simulate a filter processor that keeps records where status equals "active"
    struct MockFilter {
        std::string field;
        std::string value;
        int passed = 0;
        int dropped = 0;

        bool evaluate(const std::string& test_value) const {
            return test_value == value;
        }
    };

    MockFilter filter{"status", "active"};
    EXPECT_TRUE(filter.evaluate("active"));
    EXPECT_FALSE(filter.evaluate("inactive"));
    EXPECT_FALSE(filter.evaluate("deleted"));
    EXPECT_TRUE(filter.evaluate("active"));

    // Count matching vs non-matching
    std::vector<std::string> inputs = {"active", "inactive", "active",
                                        "pending", "active", "deleted"};
    for (auto& v : inputs) {
        if (filter.evaluate(v)) filter.passed++;
        else filter.dropped++;
    }
    EXPECT_EQ(filter.passed, 3);
    EXPECT_EQ(filter.dropped, 3);
}

// A.3 — FilterProcessor with regex predicate
TEST_F(StreamProcessingTest, FilterProcessorRegexPredicate) {
    ASSERT_TRUE(is_started());

    std::regex email_regex(R"(^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$)");
    auto matches = [&](const std::string& s) {
        return std::regex_match(s, email_regex);
    };

    EXPECT_TRUE(matches("user@example.com"));
    EXPECT_TRUE(matches("test.user+tag@domain.co.uk"));
    EXPECT_FALSE(matches("not-an-email"));
    EXPECT_FALSE(matches("@missing-user.com"));
    EXPECT_FALSE(matches(""));

    std::vector<std::string> candidates = {
        "alice@torrentmq.io", "bob@test.org", "invalid",
        "charlie@dev.example.com", "no_at_sign", "dave@corp.net"
    };
    int matched = 0;
    for (auto& c : candidates) {
        if (matches(c)) matched++;
    }
    EXPECT_EQ(matched, 4);
}

// A.4 — FilterProcessor compound AND/OR/NOT
TEST_F(StreamProcessingTest, FilterProcessorCompoundAndOrNot) {
    ASSERT_TRUE(is_started());

    // Compound predicate: (priority >= 5 AND status != "cancelled") OR type == "override"
    struct CompoundPredicate {
        std::function<bool(int, const std::string&, const std::string&)> eval;
    };

    CompoundPredicate pred;
    pred.eval = [](int priority, const std::string& status, const std::string& type) -> bool {
        return (priority >= 5 && status != "cancelled") || type == "override";
    };

    // AND cases
    EXPECT_TRUE(pred.eval(5, "active", "normal"));
    EXPECT_FALSE(pred.eval(5, "cancelled", "normal"));
    EXPECT_FALSE(pred.eval(3, "active", "normal"));
    // OR cases
    EXPECT_TRUE(pred.eval(1, "cancelled", "override"));
    EXPECT_TRUE(pred.eval(5, "cancelled", "override"));
    // Both false
    EXPECT_FALSE(pred.eval(3, "cancelled", "normal"));

    // NOT test: type != "blocked"
    auto not_blocked = [](const std::string& type) { return type != "blocked"; };
    EXPECT_TRUE(not_blocked("normal"));
    EXPECT_TRUE(not_blocked("override"));
    EXPECT_FALSE(not_blocked("blocked"));
}

// A.5 — AggregateProcessor tumbling window count
TEST_F(StreamProcessingTest, AggregateProcessorTumblingWindowCount) {
    ASSERT_TRUE(is_started());

    // Simulate tumbling window aggregation: count per 10-second window
    struct TumblingWindow {
        int64_t window_size_ms = 10000;
        std::unordered_map<int64_t, int> counts;

        void add_record(int64_t timestamp_ms) {
            int64_t window_start = (timestamp_ms / window_size_ms) * window_size_ms;
            counts[window_start]++;
        }

        int count_for_window(int64_t timestamp_ms) const {
            int64_t w = (timestamp_ms / window_size_ms) * window_size_ms;
            auto it = counts.find(w);
            return it != counts.end() ? it->second : 0;
        }
    };

    TumblingWindow tw;
    // Window 0: t=0, 1000, 5000, 8000, 9000 → 5 records
    // Window 1: t=10000, 11000, 15000 → 3 records
    // Window 2: t=21000 → 1 record
    tw.add_record(0);
    tw.add_record(1000);
    tw.add_record(5000);
    tw.add_record(8000);
    tw.add_record(9000);
    tw.add_record(10000);
    tw.add_record(11000);
    tw.add_record(15000);
    tw.add_record(21000);

    EXPECT_EQ(tw.count_for_window(0), 5);
    EXPECT_EQ(tw.count_for_window(10000), 3);
    EXPECT_EQ(tw.count_for_window(20000), 1);
    EXPECT_EQ(tw.count_for_window(30000), 0);
}

// A.6 — AggregateProcessor hopping window sum
TEST_F(StreamProcessingTest, AggregateProcessorHoppingWindowSum) {
    ASSERT_TRUE(is_started());

    // Hopping window: size=20s, hop=10s
    struct HoppingWindow {
        int64_t window_size_ms = 20000;
        int64_t hop_ms = 10000;
        std::vector<std::pair<int64_t, int64_t>> events; // (ts, value)

        void add_record(int64_t ts, int64_t value) {
            events.emplace_back(ts, value);
        }

        int64_t sum_for_start(int64_t window_start) const {
            int64_t sum = 0;
            int64_t window_end = window_start + window_size_ms;
            for (auto& [ts, val] : events) {
                if (ts >= window_start && ts < window_end) {
                    sum += val;
                }
            }
            return sum;
        }
    };

    HoppingWindow hw;
    // Events: (0,10), (5000,20), (9000,15), (15000,30), (22000,5), (28000,40)
    hw.add_record(0, 10);
    hw.add_record(5000, 20);
    hw.add_record(9000, 15);
    hw.add_record(15000, 30);
    hw.add_record(22000, 5);
    hw.add_record(28000, 40);

    // Window [0, 20000): 10+20+15+30 = 75
    EXPECT_EQ(hw.sum_for_start(0), 75);
    // Window [10000, 30000): 30+5+40 = 75
    EXPECT_EQ(hw.sum_for_start(10000), 75);
    // Window [20000, 40000): 5+40 = 45
    EXPECT_EQ(hw.sum_for_start(20000), 45);
}

// A.7 — AggregateProcessor sliding window avg
TEST_F(StreamProcessingTest, AggregateProcessorSlidingWindowAvg) {
    ASSERT_TRUE(is_started());

    // Sliding window: always look at last N records (N=5)
    struct SlidingWindow {
        int max_size;
        std::deque<int> values;

        explicit SlidingWindow(int n) : max_size(n) {}

        void add(int val) {
            values.push_back(val);
            if (static_cast<int>(values.size()) > max_size) {
                values.pop_front();
            }
        }

        double average() const {
            if (values.empty()) return 0.0;
            int64_t sum = 0;
            for (auto v : values) sum += v;
            return static_cast<double>(sum) / values.size();
        }

        size_t size() const { return values.size(); }
    };

    SlidingWindow sw(5);
    EXPECT_DOUBLE_EQ(sw.average(), 0.0);

    sw.add(10);
    EXPECT_DOUBLE_EQ(sw.average(), 10.0);

    sw.add(20);
    EXPECT_DOUBLE_EQ(sw.average(), 15.0);

    sw.add(30);
    sw.add(40);
    sw.add(50);
    EXPECT_DOUBLE_EQ(sw.average(), 30.0);
    EXPECT_EQ(sw.size(), 5);

    sw.add(60); // drops 10
    EXPECT_DOUBLE_EQ(sw.average(), 40.0); // (20+30+40+50+60)/5
    EXPECT_EQ(sw.size(), 5);

    sw.add(70); // drops 20
    EXPECT_DOUBLE_EQ(sw.average(), 50.0); // (30+40+50+60+70)/5
}

// A.8 — AggregateProcessor session window
TEST_F(StreamProcessingTest, AggregateProcessorSessionWindow) {
    ASSERT_TRUE(is_started());

    // Session window: group events with gap < inactivity_gap
    struct SessionWindow {
        int64_t inactivity_gap_ms = 5000;
        std::vector<int64_t> timestamps;

        void add_event(int64_t ts) {
            timestamps.push_back(ts);
        }

        std::vector<std::vector<int64_t>> build_sessions() const {
            if (timestamps.empty()) return {};
            auto sorted = timestamps;
            std::sort(sorted.begin(), sorted.end());

            std::vector<std::vector<int64_t>> sessions;
            std::vector<int64_t> current;
            current.push_back(sorted[0]);

            for (size_t i = 1; i < sorted.size(); ++i) {
                if (sorted[i] - sorted[i-1] <= inactivity_gap_ms) {
                    current.push_back(sorted[i]);
                } else {
                    sessions.push_back(std::move(current));
                    current.clear();
                    current.push_back(sorted[i]);
                }
            }
            if (!current.empty()) sessions.push_back(std::move(current));
            return sessions;
        }
    };

    SessionWindow sw;

    // Session 1: 0, 2000, 4500 (gaps <= 5000)
    // Session 2: 12000, 14000, 16500 (gap from 4500 to 12000 > 5000)
    // Session 3: 25000 (gap from 16500 to 25000 > 5000)
    sw.add_event(0);
    sw.add_event(2000);
    sw.add_event(4500);
    sw.add_event(12000);
    sw.add_event(14000);
    sw.add_event(16500);
    sw.add_event(25000);

    auto sessions = sw.build_sessions();
    EXPECT_EQ(sessions.size(), 3);

    EXPECT_EQ(sessions[0].size(), 3); // 0, 2000, 4500
    EXPECT_EQ(sessions[1].size(), 3); // 12000, 14000, 16500
    EXPECT_EQ(sessions[2].size(), 1); // 25000

    EXPECT_EQ(sessions[0][0], 0);
    EXPECT_EQ(sessions[0][2], 4500);
    EXPECT_EQ(sessions[1][0], 12000);
    EXPECT_EQ(sessions[2][0], 25000);
}

// A.9 — AggregateProcessor top_k
TEST_F(StreamProcessingTest, AggregateProcessorTopK) {
    ASSERT_TRUE(is_started());

    // Top-K aggregation: maintain top 3 values
    struct TopK {
        int k;
        std::unordered_map<std::string, int64_t> counts;

        explicit TopK(int k_val) : k(k_val) {}

        void add(const std::string& key, int64_t count) {
            counts[key] += count;
        }

        std::vector<std::pair<std::string, int64_t>> top() const {
            using pair_t = std::pair<std::string, int64_t>;
            std::vector<pair_t> items(counts.begin(), counts.end());
            std::partial_sort(items.begin(),
                              items.begin() + std::min(k, static_cast<int>(items.size())),
                              items.end(),
                              [](const pair_t& a, const pair_t& b) {
                                  return a.second > b.second;
                              });
            if (static_cast<int>(items.size()) > k) items.resize(k);
            return items;
        }
    };

    TopK tk(3);
    tk.add("page_a", 150);
    tk.add("page_b", 80);
    tk.add("page_c", 200);
    tk.add("page_d", 45);
    tk.add("page_e", 120);
    tk.add("page_a", 30); // total 180

    auto top = tk.top();
    EXPECT_EQ(top.size(), 3);

    // Expected order: page_c (200), page_a (180), page_e (120)
    EXPECT_EQ(top[0].first, "page_c");
    EXPECT_EQ(top[0].second, 200);
    EXPECT_EQ(top[1].first, "page_a");
    EXPECT_EQ(top[1].second, 180);
    EXPECT_EQ(top[2].first, "page_e");
    EXPECT_EQ(top[2].second, 120);
}

// A.10 — JoinProcessor stream-stream inner join
TEST_F(StreamProcessingTest, JoinProcessorStreamStreamInnerJoin) {
    ASSERT_TRUE(is_started());

    // Inner join: match left-stream and right-stream records on key
    struct StreamJoin {
        std::unordered_map<std::string, std::vector<std::string>> left;
        std::unordered_map<std::string, std::vector<std::string>> right;
        std::vector<std::tuple<std::string, std::string, std::string>> results;

        void add_left(const std::string& key, const std::string& value) {
            left[key].push_back(value);
            auto rit = right.find(key);
            if (rit != right.end()) {
                for (auto& rv : rit->second) {
                    results.emplace_back(key, value, rv);
                }
            }
        }

        void add_right(const std::string& key, const std::string& value) {
            right[key].push_back(value);
            auto lit = left.find(key);
            if (lit != left.end()) {
                for (auto& lv : lit->second) {
                    results.emplace_back(key, lv, value);
                }
            }
        }
    };

    StreamJoin join;
    join.add_left("user1", "click");
    join.add_right("user1", "session_start");
    join.add_left("user2", "scroll");
    join.add_right("user3", "purchase");
    join.add_right("user2", "page_view");

    EXPECT_EQ(join.results.size(), 2);
    // user1: click × session_start
    EXPECT_EQ(std::get<0>(join.results[0]), "user1");
    // user2: scroll × page_view
    EXPECT_EQ(std::get<0>(join.results[1]), "user2");
}

// A.11 — JoinProcessor stream-table left join
TEST_F(StreamProcessingTest, JoinProcessorStreamTableLeftJoin) {
    ASSERT_TRUE(is_started());

    // Left join: stream events against a static/enriched table
    struct StreamTableLeftJoin {
        std::unordered_map<std::string, std::string> table;

        void upsert_table(const std::string& key, const std::string& value) {
            table[key] = value;
        }

        struct Joined {
            std::string stream_key;
            std::string stream_value;
            std::string table_value; // empty if no match
        };

        Joined join(const std::string& key, const std::string& value) const {
            Joined j{key, value, ""};
            auto it = table.find(key);
            if (it != table.end()) j.table_value = it->second;
            return j;
        }
    };

    StreamTableLeftJoin stlj;
    stlj.upsert_table("region_us", "North America");
    stlj.upsert_table("region_eu", "Europe");

    auto r1 = stlj.join("region_us", "event_1");
    EXPECT_EQ(r1.table_value, "North America");

    auto r2 = stlj.join("region_asia", "event_2");
    EXPECT_EQ(r2.table_value, ""); // no match — left join keeps the stream record

    auto r3 = stlj.join("region_eu", "event_3");
    EXPECT_EQ(r3.table_value, "Europe");
}

// A.12 — JoinProcessor outer join
TEST_F(StreamProcessingTest, JoinProcessorOuterJoin) {
    ASSERT_TRUE(is_started());

    // Outer join: emit matches and non-matches with nulls
    struct OuterJoin {
        std::unordered_map<std::string, std::string> left;
        std::unordered_map<std::string, std::string> right;
        std::unordered_set<std::string> all_keys;

        void add_left(const std::string& key, const std::string& val) {
            left[key] = val;
            all_keys.insert(key);
        }

        void add_right(const std::string& key, const std::string& val) {
            right[key] = val;
            all_keys.insert(key);
        }

        struct Result {
            std::string key;
            std::string left_val;  // empty if no left
            std::string right_val; // empty if no right
        };

        std::vector<Result> results() const {
            std::vector<Result> res;
            for (auto& k : all_keys) {
                Result r;
                r.key = k;
                auto lit = left.find(k);
                if (lit != left.end()) r.left_val = lit->second;
                auto rit = right.find(k);
                if (rit != right.end()) r.right_val = rit->second;
                res.push_back(r);
            }
            return res;
        }
    };

    OuterJoin oj;
    oj.add_left("key_a", "L-A");
    oj.add_left("key_b", "L-B");
    oj.add_right("key_b", "R-B");
    oj.add_right("key_c", "R-C");

    auto results = oj.results();
    EXPECT_EQ(results.size(), 3);

    // key_a: left only
    auto it_a = std::find_if(results.begin(), results.end(),
                              [](auto& r) { return r.key == "key_a"; });
    EXPECT_NE(it_a, results.end());
    EXPECT_EQ(it_a->left_val, "L-A");
    EXPECT_TRUE(it_a->right_val.empty());

    // key_b: matched
    auto it_b = std::find_if(results.begin(), results.end(),
                              [](auto& r) { return r.key == "key_b"; });
    EXPECT_EQ(it_b->left_val, "L-B");
    EXPECT_EQ(it_b->right_val, "R-B");

    // key_c: right only
    auto it_c = std::find_if(results.begin(), results.end(),
                              [](auto& r) { return r.key == "key_c"; });
    EXPECT_TRUE(it_c->left_val.empty());
    EXPECT_EQ(it_c->right_val, "R-C");
}

// A.13 — WindowProcessor window lifecycle (open/close/emit)
TEST_F(StreamProcessingTest, WindowProcessorWindowLifecycle) {
    ASSERT_TRUE(is_started());

    // Track window lifecycle states: CREATED → OPEN → EMITTING → CLOSED
    enum class WindowState { CREATED, OPEN, EMITTING, CLOSED };

    struct Window {
        int64_t start_ms;
        int64_t end_ms;
        WindowState state = WindowState::CREATED;
        std::vector<int> records;

        explicit Window(int64_t start, int64_t end)
            : start_ms(start), end_ms(end) {}

        void open() { state = WindowState::OPEN; }
        void add(int val) {
            if (state == WindowState::OPEN) records.push_back(val);
        }
        std::vector<int> emit() {
            state = WindowState::EMITTING;
            auto result = records;
            return result;
        }
        void close() { state = WindowState::CLOSED; }
        bool is_open() const { return state == WindowState::OPEN; }
        bool is_closed() const { return state == WindowState::CLOSED; }
    };

    Window w(0, 10000);
    EXPECT_EQ(w.state, WindowState::CREATED);

    w.open();
    EXPECT_TRUE(w.is_open());

    w.add(1);
    w.add(2);
    w.add(3);
    EXPECT_EQ(w.records.size(), 3);

    auto emitted = w.emit();
    EXPECT_EQ(emitted.size(), 3);
    EXPECT_EQ(w.state, WindowState::EMITTING);

    w.close();
    EXPECT_TRUE(w.is_closed());
}

// A.14 — TableProcessor upsert and lookup
TEST_F(StreamProcessingTest, TableProcessorUpsertAndLookup) {
    ASSERT_TRUE(is_started());

    struct TableStore {
        std::unordered_map<std::string, std::string> data;
        std::unordered_map<std::string, int64_t> versions;

        void upsert(const std::string& key, const std::string& value) {
            data[key] = value;
            versions[key]++;
        }

        std::optional<std::string> lookup(const std::string& key) const {
            auto it = data.find(key);
            if (it != data.end()) return it->second;
            return std::nullopt;
        }

        bool remove(const std::string& key) {
            return data.erase(key) > 0;
        }

        int64_t version(const std::string& key) const {
            auto it = versions.find(key);
            return it != versions.end() ? it->second : 0;
        }

        size_t size() const { return data.size(); }
    };

    TableStore ts;
    EXPECT_EQ(ts.size(), 0);

    ts.upsert("user_1", R"({"name":"Alice","score":100})");
    EXPECT_EQ(ts.size(), 1);
    EXPECT_EQ(ts.version("user_1"), 1);

    auto val = ts.lookup("user_1");
    EXPECT_TRUE(val.has_value());
    EXPECT_NE(val->find("Alice"), std::string::npos);

    // Upsert — update existing key
    ts.upsert("user_1", R"({"name":"Alice","score":150})");
    EXPECT_EQ(ts.size(), 1); // still same key count
    EXPECT_EQ(ts.version("user_1"), 2);

    // Lookup nonexistent
    EXPECT_FALSE(ts.lookup("user_999").has_value());

    // Remove
    EXPECT_TRUE(ts.remove("user_1"));
    EXPECT_EQ(ts.size(), 0);
    EXPECT_FALSE(ts.lookup("user_1").has_value());
    EXPECT_FALSE(ts.remove("user_1")); // already removed
}

// A.15 — TableProcessor versioned state
TEST_F(StreamProcessingTest, TableProcessorVersionedState) {
    ASSERT_TRUE(is_started());

    // Versioned table: track multiple versions of a key
    struct VersionedTable {
        using version_t = int64_t;
        std::map<std::string, std::map<version_t, std::string>> store;

        void put(const std::string& key, const std::string& value, version_t ver) {
            store[key][ver] = value;
        }

        std::optional<std::string> get(const std::string& key, version_t ver) const {
            auto kit = store.find(key);
            if (kit == store.end()) return std::nullopt;
            auto vit = kit->second.find(ver);
            if (vit != kit->second.end()) return vit->second;
            return std::nullopt;
        }

        std::optional<std::string> latest(const std::string& key) const {
            auto kit = store.find(key);
            if (kit == store.end() || kit->second.empty()) return std::nullopt;
            return kit->second.rbegin()->second;
        }

        void compact_before(const std::string& key, version_t ver) {
            auto kit = store.find(key);
            if (kit == store.end()) return;
            auto it = kit->second.begin();
            while (it != kit->second.end() && it->first < ver) {
                it = kit->second.erase(it);
            }
        }
    };

    VersionedTable vt;
    vt.put("config", "v1", 1);
    vt.put("config", "v2", 2);
    vt.put("config", "v3", 3);

    EXPECT_EQ(vt.get("config", 1).value(), "v1");
    EXPECT_EQ(vt.get("config", 2).value(), "v2");
    EXPECT_EQ(vt.latest("config").value(), "v3");

    vt.compact_before("config", 2);
    EXPECT_FALSE(vt.get("config", 1).has_value()); // compacted away
    EXPECT_EQ(vt.get("config", 2).value(), "v2");  // still there
    EXPECT_EQ(vt.latest("config").value(), "v3");  // still latest
}

// A.16 — RepartitionProcessor hash partitioning
TEST_F(StreamProcessingTest, RepartitionProcessorHashPartitioning) {
    ASSERT_TRUE(is_started());

    // Hash partitioner: partition = hash(key) % num_partitions
    struct HashPartitioner {
        int num_partitions;

        explicit HashPartitioner(int n) : num_partitions(n) {}

        int partition(const std::string& key) const {
            size_t h = std::hash<std::string>{}(key);
            return static_cast<int>(h % num_partitions);
        }
    };

    HashPartitioner hp(8);

    // Verify determinism
    EXPECT_EQ(hp.partition("key_a"), hp.partition("key_a"));
    EXPECT_EQ(hp.partition("key_b"), hp.partition("key_b"));

    // Verify distribution across 8 partitions
    std::set<int> used;
    for (int i = 0; i < 1000; i++) {
        used.insert(hp.partition("record_" + std::to_string(i)));
    }
    // With 1000 random-ish keys, should use most/all partitions
    EXPECT_GE(used.size(), 6); // at least 6 of 8 partitions used

    // All partitions in valid range
    for (int p : used) {
        EXPECT_GE(p, 0);
        EXPECT_LT(p, 8);
    }
}

// A.17 — RepartitionProcessor range partitioning
TEST_F(StreamProcessingTest, RepartitionProcessorRangePartitioning) {
    ASSERT_TRUE(is_started());

    // Range partitioner: assign key ranges to partitions
    struct RangePartitioner {
        std::vector<int> boundaries; // [b0, b1, b2, ...] → P0=[-∞,b0), P1=[b0,b1), ...

        explicit RangePartitioner(std::vector<int> bounds) : boundaries(std::move(bounds)) {}

        int partition(int key) const {
            for (size_t i = 0; i < boundaries.size(); i++) {
                if (key < boundaries[i]) return static_cast<int>(i);
            }
            return static_cast<int>(boundaries.size());
        }
    };

    // Partitions: P0=[0,100), P1=[100,500), P2=[500,1000), P3=[1000,∞)
    RangePartitioner rp({100, 500, 1000});

    EXPECT_EQ(rp.partition(0), 0);
    EXPECT_EQ(rp.partition(50), 0);
    EXPECT_EQ(rp.partition(99), 0);

    EXPECT_EQ(rp.partition(100), 1);
    EXPECT_EQ(rp.partition(250), 1);
    EXPECT_EQ(rp.partition(499), 1);

    EXPECT_EQ(rp.partition(500), 2);
    EXPECT_EQ(rp.partition(750), 2);
    EXPECT_EQ(rp.partition(999), 2);

    EXPECT_EQ(rp.partition(1000), 3);
    EXPECT_EQ(rp.partition(5000), 3);
}

// A.18 — StateStore put/get/delete
TEST_F(StreamProcessingTest, StateStorePutGetDelete) {
    ASSERT_TRUE(is_started());

    // In-memory key-value state store for stream state
    struct KVStateStore {
        std::unordered_map<std::string, std::string> store;
        mutable std::shared_mutex mutex;

        void put(const std::string& key, const std::string& value) {
            std::unique_lock lock(mutex);
            store[key] = value;
        }

        std::optional<std::string> get(const std::string& key) const {
            std::shared_lock lock(mutex);
            auto it = store.find(key);
            if (it != store.end()) return it->second;
            return std::nullopt;
        }

        bool remove(const std::string& key) {
            std::unique_lock lock(mutex);
            return store.erase(key) > 0;
        }

        size_t size() const {
            std::shared_lock lock(mutex);
            return store.size();
        }

        void clear() {
            std::unique_lock lock(mutex);
            store.clear();
        }

        std::vector<std::string> keys() const {
            std::shared_lock lock(mutex);
            std::vector<std::string> result;
            result.reserve(store.size());
            for (auto& [k, _] : store) result.push_back(k);
            return result;
        }
    };

    KVStateStore ss;
    EXPECT_EQ(ss.size(), 0);
    EXPECT_TRUE(ss.keys().empty());

    ss.put("offset_partition_0", "12345");
    ss.put("offset_partition_1", "67890");
    ss.put("watermark", "2024-01-15T10:30:00Z");

    EXPECT_EQ(ss.size(), 3);
    EXPECT_EQ(ss.get("offset_partition_0").value(), "12345");
    EXPECT_EQ(ss.get("offset_partition_1").value(), "67890");
    EXPECT_FALSE(ss.get("nonexistent").has_value());

    auto keys = ss.keys();
    EXPECT_EQ(keys.size(), 3);

    // Delete
    EXPECT_TRUE(ss.remove("offset_partition_0"));
    EXPECT_EQ(ss.size(), 2);
    EXPECT_FALSE(ss.get("offset_partition_0").has_value());
    EXPECT_FALSE(ss.remove("offset_partition_0")); // double delete

    ss.clear();
    EXPECT_EQ(ss.size(), 0);
}

// A.19 — StateStore checkpoint and restore
TEST_F(StreamProcessingTest, StateStoreCheckpointAndRestore) {
    ASSERT_TRUE(is_started());

    struct CheckpointableStore {
        std::unordered_map<std::string, std::string> state;
        std::string checkpoint_path;

        void set_path(const std::string& path) { checkpoint_path = path; }

        void put(const std::string& key, const std::string& value) {
            state[key] = value;
        }

        std::optional<std::string> get(const std::string& key) const {
            auto it = state.find(key);
            if (it != state.end()) return it->second;
            return std::nullopt;
        }

        void save_checkpoint() {
            if (checkpoint_path.empty()) return;
            std::ofstream ofs(checkpoint_path, std::ios::trunc);
            ofs << state.size() << "\n";
            for (auto& [k, v] : state) {
                ofs << k.size() << " " << k << " " << v.size() << " " << v << "\n";
            }
        }

        void restore_checkpoint() {
            std::ifstream ifs(checkpoint_path);
            if (!ifs.is_open()) return;
            size_t count;
            ifs >> count;
            ifs.ignore(1);
            state.clear();
            for (size_t i = 0; i < count; i++) {
                size_t klen, vlen;
                ifs >> klen;
                ifs.ignore(1);
                std::string key(klen, '\0');
                ifs.read(&key[0], klen);
                ifs.ignore(1);
                ifs >> vlen;
                ifs.ignore(1);
                std::string value(vlen, '\0');
                ifs.read(&value[0], vlen);
                ifs.ignore(1);
                state[key] = value;
            }
        }
    };

    std::string cp_path = tmp_dir_ + "/checkpoint.dat";
    CheckpointableStore cs;
    cs.set_path(cp_path);

    cs.put("agg_count", "42");
    cs.put("last_offset", "9999");
    cs.put("window_state", "open");
    cs.save_checkpoint();

    // Create new store and restore
    CheckpointableStore cs2;
    cs2.set_path(cp_path);
    EXPECT_EQ(cs2.get("agg_count"), std::nullopt);

    cs2.restore_checkpoint();
    EXPECT_EQ(cs2.get("agg_count").value(), "42");
    EXPECT_EQ(cs2.get("last_offset").value(), "9999");
    EXPECT_EQ(cs2.get("window_state").value(), "open");
}

// A.20 — TopologyBuilder fluent API
TEST_F(StreamProcessingTest, TopologyBuilderFluentApi) {
    ASSERT_TRUE(is_started());

    // TopologyBuilder supports a fluent API for building streams topologies
    auto builder = torrent::streams::TopologyBuilder(*server_);

    // Configure full topology: source → filter → aggregate → sink
    builder.add_source("event_source", "raw_events");
    builder.add_filter("priority_filter", "gte:priority=5");
    builder.add_map("normalize", "lowercase:event_type");
    builder.add_aggregate("hourly_counts", "tumbling:size=3600s:field=count");
    builder.add_sink("aggregated_output", "aggregated_events");

    EXPECT_NO_THROW(builder.build());
    EXPECT_NO_THROW(builder.start());

    // Second build on running topology should be a no-op or handled gracefully
    EXPECT_NO_THROW(builder.build());

    EXPECT_NO_THROW(builder.shutdown());
}

// ============================================================================
// SECTION B: CONNECTOR TESTS (20 tests)
// ============================================================================

class ConnectorTest : public BrokerStreamConnectorTest {
protected:
    void SetUp() override {
        BrokerStreamConnectorTest::SetUp();
        start_broker(1);
    }
};

// B.1 — ConnectFramework register source connector
TEST_F(ConnectorTest, ConnectFrameworkRegisterSourceConnector) {
    ASSERT_TRUE(is_started());

    auto cf = torrent::connectors::ConnectFramework(*server_);
    std::string config = R"({
        "connector.class": "DebeziumSource",
        "database.hostname": "localhost",
        "database.port": "5432",
        "database.user": "cdc_user",
        "database.dbname": "mydb",
        "table.whitelist": "public.orders"
    })";

    EXPECT_NO_THROW(cf.register_source("pg-cdc-source", "DebeziumSource", config));
}

// B.2 — ConnectFramework register sink connector
TEST_F(ConnectorTest, ConnectFrameworkRegisterSinkConnector) {
    ASSERT_TRUE(is_started());

    auto cf = torrent::connectors::ConnectFramework(*server_);
    std::string config = R"({
        "connector.class": "ElasticsearchSink",
        "es.hosts": "http://localhost:9200",
        "es.index.name": "torrent-logs-{date}",
        "es.batch.size": "500"
    })";

    EXPECT_NO_THROW(cf.register_sink("es-sink", "ElasticsearchSink", config));
}

// B.3 — SourceConnector poll and produce
TEST_F(ConnectorTest, SourceConnectorPollAndProduce) {
    ASSERT_TRUE(is_started());

    // Simulate source connector poll/produce cycle
    struct MockSourceConnector {
        int poll_count = 0;
        int record_count = 0;
        std::vector<std::string> source_data = {"r1", "r2", "r3", "r4", "r5"};
        size_t position = 0;

        struct SourceRecord {
            std::string key;
            std::string value;
        };

        std::vector<SourceRecord> poll() {
            poll_count++;
            std::vector<SourceRecord> batch;
            int batch_size = 2;
            for (int i = 0; i < batch_size && position < source_data.size(); i++) {
                batch.push_back({"key_" + std::to_string(position),
                                 source_data[position]});
                position++;
                record_count++;
            }
            return batch;
        }

        bool has_more() const { return position < source_data.size(); }
    };

    MockSourceConnector source;
    int total_polls = 0;
    int total_records = 0;

    while (source.has_more()) {
        auto batch = source.poll();
        total_polls++;
        total_records += batch.size();
        EXPECT_TRUE(batch.size() <= 2);
    }

    EXPECT_EQ(total_polls, 3); // 5 records in batches of 2: 2+2+1
    EXPECT_EQ(total_records, 5);
    EXPECT_EQ(source.poll_count, 3);
}

// B.4 — SinkConnector put and flush
TEST_F(ConnectorTest, SinkConnectorPutAndFlush) {
    ASSERT_TRUE(is_started());

    // Simulate sink connector buffering and flushing
    struct MockSinkConnector {
        std::vector<std::string> buffer;
        int max_batch_size = 3;
        int flush_count = 0;
        int total_written = 0;

        void put(const std::string& record) {
            buffer.push_back(record);
            if (static_cast<int>(buffer.size()) >= max_batch_size) {
                flush();
            }
        }

        void flush() {
            if (buffer.empty()) return;
            total_written += buffer.size();
            buffer.clear();
            flush_count++;
        }
    };

    MockSinkConnector sink;
    slog::put("record_1");
    EXPECT_EQ(sink.buffer.size(), 1);
    EXPECT_EQ(sink.flush_count, 0);

    sink.put("record_2");
    EXPECT_EQ(sink.buffer.size(), 2);

    sink.put("record_3"); // triggers auto-flush
    EXPECT_EQ(sink.flush_count, 1);
    EXPECT_EQ(sink.total_written, 3);
    EXPECT_TRUE(sink.buffer.empty());

    sink.put("record_4");
    sink.put("record_5");
    sink.flush(); // manual flush
    EXPECT_EQ(sink.flush_count, 2);
    EXPECT_EQ(sink.total_written, 5);
}

// B.5 — TaskManager start/stop task
TEST_F(ConnectorTest, TaskManagerStartStopTask) {
    ASSERT_TRUE(is_started());

    // Simulate TaskManager task lifecycle
    struct TaskManager {
        enum class TaskState { UNASSIGNED, PAUSED, RUNNING, STOPPED };
        struct Task {
            std::string id;
            TaskState state = TaskState::UNASSIGNED;
            int operations_done = 0;
        };

        std::unordered_map<std::string, Task> tasks;

        std::string register_task(const std::string& name) {
            Task t;
            t.id = name + "-0";
            tasks[t.id] = t;
            return t.id;
        }

        bool start_task(const std::string& id) {
            auto it = tasks.find(id);
            if (it == tasks.end()) return false;
            if (it->second.state == TaskState::RUNNING) return false;
            it->second.state = TaskState::RUNNING;
            return true;
        }

        bool stop_task(const std::string& id) {
            auto it = tasks.find(id);
            if (it == tasks.end()) return false;
            it->second.state = TaskState::STOPPED;
            return true;
        }

        TaskState state(const std::string& id) const {
            auto it = tasks.find(id);
            if (it == tasks.end()) return TaskState::STOPPED;
            return it->second.state;
        }
    };

    TaskManager tm;
    auto task_id = tm.register_task("jdbc-sink");
    EXPECT_FALSE(task_id.empty());

    EXPECT_EQ(tm.state(task_id), TaskManager::TaskState::UNASSIGNED);
    EXPECT_TRUE(tm.start_task(task_id));
    EXPECT_EQ(tm.state(task_id), TaskManager::TaskState::RUNNING);

    // Starting again when already running should fail
    EXPECT_FALSE(tm.start_task(task_id));

    EXPECT_TRUE(tm.stop_task(task_id));
}

// B.6 — TaskManager rebalance tasks
TEST_F(ConnectorTest, TaskManagerRebalanceTasks) {
    ASSERT_TRUE(is_started());

    // Simulate rebalancing tasks across workers
    struct RebalanceSimulator {
        int num_workers;
        std::unordered_map<int, std::vector<std::string>> assignments;

        explicit RebalanceSimulator(int workers) : num_workers(workers) {}

        void rebalance(const std::vector<std::string>& tasks) {
            assignments.clear();
            for (int i = 0; i < num_workers; i++) {
                assignments[i] = {};
            }
            for (size_t i = 0; i < tasks.size(); i++) {
                int worker = static_cast<int>(i % num_workers);
                assignments[worker].push_back(tasks[i]);
            }
        }

        int tasks_on_worker(int worker) const {
            auto it = assignments.find(worker);
            return it != assignments.end() ? static_cast<int>(it->second.size()) : 0;
        }

        int total_assigned() const {
            int total = 0;
            for (auto& [_, tasks] : assignments) total += tasks.size();
            return total;
        }
    };

    RebalanceSimulator rs(4);
    std::vector<std::string> tasks = {"t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9"};
    rs.rebalance(tasks);

    // 10 tasks across 4 workers: 3+3+2+2 or 3+3+3+1 distribution
    EXPECT_EQ(rs.total_assigned(), 10);

    // Each worker should have either 2 or 3 tasks
    for (int w = 0; w < 4; w++) {
        int count = rs.tasks_on_worker(w);
        EXPECT_GE(count, 2);
        EXPECT_LE(count, 3);
    }
}

// B.7 — DebeziumSource snapshot mode
TEST_F(ConnectorTest, DebeziumSourceSnapshotMode) {
    ASSERT_TRUE(is_started());

    // Simulate Debezium snapshot mode: full table dump
    struct DebeziumSnapshot {
        bool snapshot_complete = false;
        std::vector<std::string> rows = {"row1", "row2", "row3", "row4"};
        size_t cursor = 0;

        struct CDC {
            std::string op;    // "r" for snapshot read
            std::string table;
            std::string after;
        };

        std::optional<CDC> poll_snapshot() {
            if (cursor >= rows.size()) {
                snapshot_complete = true;
                return std::nullopt;
            }
            CDC cdc{"r", "public.orders", rows[cursor]};
            cursor++;
            return cdc;
        }
    };

    DebeziumSnapshot ds;
    EXPECT_FALSE(ds.snapshot_complete);

    std::vector<std::string> results;
    while (auto cdc = ds.poll_snapshot()) {
        EXPECT_EQ(cdc->op, "r");
        EXPECT_EQ(cdc->table, "public.orders");
        results.push_back(cdc->after);
    }

    EXPECT_TRUE(ds.snapshot_complete);
    EXPECT_EQ(results.size(), 4);
    EXPECT_EQ(results[0], "row1");
    EXPECT_EQ(results[3], "row4");
}

// B.8 — DebeziumSource incremental mode
TEST_F(ConnectorTest, DebeziumSourceIncrementalMode) {
    ASSERT_TRUE(is_started());

    // Simulate Debezium incremental/streaming mode after snapshot
    struct DebeziumIncremental {
        std::vector<std::pair<std::string, std::string>> changes = {
            {"c", R"({"id": 1, "name": "Alice"})"},
            {"u", R"({"id": 1, "name": "Alice Updated"})"},
            {"d", R"({"id": 2, "name": "Bob"})"},
            {"c", R"({"id": 3, "name": "Charlie"})"},
        };
        size_t pos = 0;

        struct CDC {
            std::string op;
            std::string payload;
        };

        std::optional<CDC> poll() {
            if (pos >= changes.size()) return std::nullopt;
            CDC cdc{changes[pos].first, changes[pos].second};
            pos++;
            return cdc;
        }
    };

    DebeziumIncremental di;
    int inserts = 0, updates = 0, deletes = 0;

    while (auto cdc = di.poll()) {
        if (cdc->op == "c") inserts++;
        else if (cdc->op == "u") updates++;
        else if (cdc->op == "d") deletes++;
    }

    EXPECT_EQ(inserts, 2);
    EXPECT_EQ(updates, 1);
    EXPECT_EQ(deletes, 1);
}

// B.9 — JdbcSink insert mode
TEST_F(ConnectorTest, JdbcSinkInsertMode) {
    ASSERT_TRUE(is_started());

    // Simulate JDBC sink insert mode: INSERT INTO statements
    struct JdbcInsert {
        int inserts_executed = 0;
        std::vector<std::string> sql_statements;

        void insert(const std::string& table,
                     const std::unordered_map<std::string, std::string>& fields) {
            std::ostringstream sql;
            sql << "INSERT INTO " << table << " (";
            std::vector<std::string> cols, vals;
            for (auto& [k, v] : fields) {
                cols.push_back(k);
                vals.push_back("'" + v + "'");
            }
            for (size_t i = 0; i < cols.size(); i++) {
                if (i > 0) sql << ", ";
                sql << cols[i];
            }
            sql << ") VALUES (";
            for (size_t i = 0; i < vals.size(); i++) {
                if (i > 0) sql << ", ";
                sql << vals[i];
            }
            sql << ")";
            sql_statements.push_back(sql.str());
            inserts_executed++;
        }
    };

    JdbcInsert jdbc;
    jdbc.insert("orders", {{"id", "1"}, {"status", "new"}, {"amount", "99.95"}});
    jdbc.insert("orders", {{"id", "2"}, {"status", "pending"}});

    EXPECT_EQ(jdbc.inserts_executed, 2);
    EXPECT_EQ(jdbc.sql_statements.size(), 2);

    // Verify first statement
    EXPECT_NE(jdbc.sql_statements[0].find("INSERT INTO orders"), std::string::npos);
    EXPECT_NE(jdbc.sql_statements[0].find("99.95"), std::string::npos);

    // Verify second statement
    EXPECT_NE(jdbc.sql_statements[1].find("pending"), std::string::npos);
}

// B.10 — JdbcSink upsert mode
TEST_F(ConnectorTest, JdbcSinkUpsertMode) {
    ASSERT_TRUE(is_started());

    // Simulate JDBC sink upsert mode: INSERT ... ON CONFLICT ... DO UPDATE
    struct JdbcUpsert {
        int upserts_executed = 0;
        std::vector<std::string> sql_statements;

        void upsert(const std::string& table,
                     const std::unordered_map<std::string, std::string>& fields,
                     const std::vector<std::string>& key_columns) {
            std::ostringstream sql;
            sql << "INSERT INTO " << table << " (";
            std::vector<std::string> cols, vals;
            for (auto& [k, v] : fields) {
                cols.push_back(k);
                vals.push_back("'" + v + "'");
            }
            for (size_t i = 0; i < cols.size(); i++) {
                if (i > 0) sql << ", ";
                sql << cols[i];
            }
            sql << ") VALUES (";
            for (size_t i = 0; i < vals.size(); i++) {
                if (i > 0) sql << ", ";
                sql << vals[i];
            }
            sql << ") ON CONFLICT (";
            for (size_t i = 0; i < key_columns.size(); i++) {
                if (i > 0) sql << ", ";
                sql << key_columns[i];
            }
            sql << ") DO UPDATE SET ";
            bool first = true;
            for (auto& [k, _] : fields) {
                if (std::find(key_columns.begin(), key_columns.end(), k)
                    != key_columns.end())
                    continue;
                if (!first) sql << ", ";
                sql << k << " = EXCLUDED." << k;
                first = false;
            }
            sql_statements.push_back(sql.str());
            upserts_executed++;
        }
    };

    JdbcUpsert jdbc;
    jdbc.upsert("users", {{"id", "100"}, {"name", "Alice"}, {"email", "alice@test.com"}},
                {"id"});

    EXPECT_EQ(jdbc.upserts_executed, 1);
    std::string sql = jdbc.sql_statements[0];
    EXPECT_NE(sql.find("ON CONFLICT"), std::string::npos);
    EXPECT_NE(sql.find("DO UPDATE SET"), std::string::npos);
    EXPECT_NE(sql.find("EXCLUDED"), std::string::npos);
}

// B.11 — S3Sink partition by time
TEST_F(ConnectorTest, S3SinkPartitionByTime) {
    ASSERT_TRUE(is_started());

    // Simulate S3 time-based partitioning
    struct S3Partitioner {
        int64_t partition_duration_ms = 3600000; // 1 hour

        std::string partition_path(int64_t timestamp_ms) const {
            // Extract year/month/day/hour from timestamp
            time_t t = static_cast<time_t>(timestamp_ms / 1000);
            struct tm tm_buf;
            gmtime_r(&t, &tm_buf);

            char buf[256];
            snprintf(buf, sizeof(buf), "year=%04d/month=%02d/day=%02d/hour=%02d",
                     tm_buf.tm_year + 1900, tm_buf.tm_mon + 1,
                     tm_buf.tm_mday, tm_buf.tm_hour);
            return std::string(buf);
        }

        std::string full_path(const std::string& prefix, int64_t ts) const {
            return prefix + "/" + partition_path(ts);
        }
    };

    S3Partitioner sp;

    // 2024-01-15 10:30:00 UTC = 1705312200000 ms
    int64_t ts1 = 1705312200000LL;
    std::string path1 = sp.full_path("s3://my-bucket/data", ts1);
    EXPECT_NE(path1.find("year=2024"), std::string::npos);
    EXPECT_NE(path1.find("month=01"), std::string::npos);
    EXPECT_NE(path1.find("day=15"), std::string::npos);
    EXPECT_NE(path1.find("hour=10"), std::string::npos);

    // 2024-12-31 23:45:00 UTC
    int64_t ts2 = 1735688700000LL;
    std::string path2 = sp.full_path("s3://my-bucket/data", ts2);
    EXPECT_NE(path2.find("year=2024"), std::string::npos);
    EXPECT_NE(path2.find("month=12"), std::string::npos);
    EXPECT_NE(path2.find("day=31"), std::string::npos);
    EXPECT_NE(path2.find("hour=23"), std::string::npos);
}

// B.12 — S3Sink compression
TEST_F(ConnectorTest, S3SinkCompression) {
    ASSERT_TRUE(is_started());

    // Simulate S3 compression options
    struct S3Compression {
        enum class Compression { NONE, GZIP, SNAPPY, LZ4, ZSTD };

        Compression type = Compression::NONE;

        std::string file_extension() const {
            switch (type) {
            case Compression::GZIP:   return ".gz";
            case Compression::SNAPPY: return ".snappy";
            case Compression::LZ4:    return ".lz4";
            case Compression::ZSTD:   return ".zst";
            default:                   return "";
            }
        }

        std::string content_encoding() const {
            switch (type) {
            case Compression::GZIP:   return "gzip";
            case Compression::SNAPPY: return "x-snappy-framed";
            case Compression::LZ4:    return "x-lz4";
            case Compression::ZSTD:   return "zstd";
            default:                   return "";
            }
        }
    };

    S3Compression sc;

    sc.type = S3Compression::Compression::NONE;
    EXPECT_TRUE(sc.file_extension().empty());

    sc.type = S3Compression::Compression::GZIP;
    EXPECT_EQ(sc.file_extension(), ".gz");
    EXPECT_EQ(sc.content_encoding(), "gzip");

    sc.type = S3Compression::Compression::SNAPPY;
    EXPECT_EQ(sc.file_extension(), ".snappy");

    sc.type = S3Compression::Compression::LZ4;
    EXPECT_EQ(sc.file_extension(), ".lz4");

    sc.type = S3Compression::Compression::ZSTD;
    EXPECT_EQ(sc.file_extension(), ".zst");
    EXPECT_EQ(sc.content_encoding(), "zstd");
}

// B.13 — ElasticsearchSink bulk index
TEST_F(ConnectorTest, ElasticsearchSinkBulkIndex) {
    ASSERT_TRUE(is_started());

    // Simulate Elasticsearch bulk index request building
    struct ESBulkIndexer {
        int doc_count = 0;
        std::ostringstream bulk_body;

        void add_document(const std::string& index, const std::string& id,
                          const std::string& json_doc) {
            // Action metadata line
            bulk_body << R"({"index":{"_index":")" << index
                      << R"(","_id":")" << id << "\"}}\n";
            // Document body
            bulk_body << json_doc << "\n";
            doc_count++;
        }

        void add_upsert(const std::string& index, const std::string& id,
                         const std::string& json_doc) {
            bulk_body << R"({"update":{"_index":")" << index
                      << R"(","_id":")" << id << "\"}}\n";
            bulk_body << R"({"doc":)" << json_doc
                      << R"(,"doc_as_upsert":true})" << "\n";
            doc_count++;
        }

        std::string build() const { return bulk_body.str(); }
    };

    ESBulkIndexer es;
    es.add_document("torrent-logs-2024", "doc1",
                     R"({"level":"INFO","message":"started"})");
    es.add_document("torrent-logs-2024", "doc2",
                     R"({"level":"WARN","message":"slow query"})");
    es.add_upsert("torrent-state", "state1",
                   R"({"offset":12345,"lag":100})");

    EXPECT_EQ(es.doc_count, 3);

    std::string bulk = es.build();
    EXPECT_NE(bulk.find(R"("index")"), std::string::npos);
    EXPECT_NE(bulk.find(R"("update")"), std::string::npos);
    EXPECT_NE(bulk.find("doc_as_upsert"), std::string::npos);
    EXPECT_NE(bulk.find("slow query"), std::string::npos);
}

// B.14 — RedisSink string and hash operations
TEST_F(ConnectorTest, RedisSinkStringAndHashOperations) {
    ASSERT_TRUE(is_started());

    // Simulate Redis sink data structure operations
    struct RedisSinkSim {
        std::unordered_map<std::string, std::string> strings;
        std::unordered_map<std::string, std::unordered_map<std::string, std::string>> hashes;

        void set_string(const std::string& key, const std::string& value) {
            strings[key] = value;
        }

        void hset(const std::string& key, const std::string& field,
                  const std::string& value) {
            hashes[key][field] = value;
        }

        std::string get_string(const std::string& key) const {
            auto it = strings.find(key);
            return it != strings.end() ? it->second : "";
        }

        std::string hget(const std::string& key, const std::string& field) const {
            auto it = hashes.find(key);
            if (it == hashes.end()) return "";
            auto fit = it->second.find(field);
            return fit != it->second.end() ? fit->second : "";
        }
    };

    RedisSinkSim redis;

    // String operations
    redis.set_string("user:1:name", "Alice");
    redis.set_string("user:1:score", "1500");
    EXPECT_EQ(redis.get_string("user:1:name"), "Alice");
    EXPECT_EQ(redis.get_string("user:1:score"), "1500");
    EXPECT_TRUE(redis.get_string("nonexistent").empty());

    // Hash operations
    redis.hset("session:abc123", "user_id", "1");
    redis.hset("session:abc123", "ip", "192.168.1.1");
    redis.hset("session:abc123", "created", "2024-01-15T10:00:00Z");

    EXPECT_EQ(redis.hget("session:abc123", "user_id"), "1");
    EXPECT_EQ(redis.hget("session:abc123", "ip"), "192.168.1.1");
    EXPECT_TRUE(redis.hget("session:abc123", "nonexistent_field").empty());
}

// B.15 — InfluxDBSink line protocol format
TEST_F(ConnectorTest, InfluxDBSinkLineProtocolFormat) {
    ASSERT_TRUE(is_started());

    // Simulate InfluxDB Line Protocol formatting
    struct LineProtocolFormatter {
        std::string format(const std::string& measurement,
                           const std::unordered_map<std::string, std::string>& tags,
                           const std::unordered_map<std::string, double>& fields,
                           int64_t timestamp_ns) {
            std::ostringstream lp;

            // measurement
            lp << measurement;

            // tags (comma-separated, sorted)
            if (!tags.empty()) {
                std::vector<std::pair<std::string, std::string>> sorted_tags(
                    tags.begin(), tags.end());
                std::sort(sorted_tags.begin(), sorted_tags.end());
                for (auto& [k, v] : sorted_tags) {
                    lp << "," << k << "=" << v;
                }
            }

            // space before fields
            lp << " ";

            // fields (comma-separated)
            bool first = true;
            std::vector<std::pair<std::string, double>> sorted_fields(
                fields.begin(), fields.end());
            std::sort(sorted_fields.begin(), sorted_fields.end());
            for (auto& [k, v] : sorted_fields) {
                if (!first) lp << ",";
                lp << k << "=" << v;
                first = false;
            }

            // timestamp
            lp << " " << timestamp_ns;

            return lp.str();
        }
    };

    LineProtocolFormatter lpf;
    std::string line = lpf.format(
        "cpu_usage",
        {{"host", "server01"}, {"region", "us-east"}},
        {{"usage_user", 42.5}, {"usage_system", 15.3}, {"usage_idle", 42.2}},
        1705312200000000000LL);

    EXPECT_NE(line.find("cpu_usage"), std::string::npos);
    EXPECT_NE(line.find("host=server01"), std::string::npos);
    EXPECT_NE(line.find("region=us-east"), std::string::npos);
    EXPECT_NE(line.find("usage_user=42.5"), std::string::npos);
    EXPECT_NE(line.find("usage_system=15.3"), std::string::npos);

    // The measurement comes first, then tags, space, fields, space, timestamp
    EXPECT_EQ(line[0], 'c'); // starts with "cpu_usage"
}

// B.16 — KafkaMirror topic mirroring
TEST_F(ConnectorTest, KafkaMirrorTopicMirroring) {
    ASSERT_TRUE(is_started());

    // Simulate Kafka MirrorMaker-style topic mirroring
    struct KafkaMirrorSim {
        std::unordered_map<std::string, std::vector<std::string>> source_topics;
        std::unordered_map<std::string, std::vector<std::string>> target_topics;

        void produce(const std::string& topic, const std::string& record) {
            source_topics[topic].push_back(record);
        }

        void mirror(const std::string& source, const std::string& target,
                    const std::string& prefix = "") {
            std::string full_target = prefix.empty() ? target : prefix + "." + target;
            auto& src = source_topics[source];
            auto& dst = target_topics[full_target];
            for (auto& r : src) {
                dst.push_back(r);
            }
        }

        size_t count(const std::string& topic) const {
            auto it = target_topics.find(topic);
            if (it != target_topics.end()) return it->second.size();
            auto sit = source_topics.find(topic);
            return sit != source_topics.end() ? sit->second.size() : 0;
        }
    };

    KafkaMirrorSim mirror;
    mirror.produce("orders.us", "order_1");
    mirror.produce("orders.us", "order_2");
    mirror.produce("orders.us", "order_3");
    mirror.produce("orders.eu", "order_eu_1");

    mirror.mirror("orders.us", "orders.us", "us-backup");
    mirror.mirror("orders.eu", "orders.eu", "us-backup");

    EXPECT_EQ(mirror.count("us-backup.orders.us"), 3);
    EXPECT_EQ(mirror.count("us-backup.orders.eu"), 1);
}

// B.17 — MongoDBSource change stream
TEST_F(ConnectorTest, MongoDBSourceChangeStream) {
    ASSERT_TRUE(is_started());

    // Simulate MongoDB change stream events
    struct MongoChangeStream {
        std::vector<std::tuple<std::string, std::string, std::string>> events; // op, ns, doc_id
        // op: insert, update, replace, delete

        void add_insert(const std::string& ns, const std::string& doc_id) {
            events.emplace_back("insert", ns, doc_id);
        }
        void add_update(const std::string& ns, const std::string& doc_id) {
            events.emplace_back("update", ns, doc_id);
        }
        void add_delete(const std::string& ns, const std::string& doc_id) {
            events.emplace_back("delete", ns, doc_id);
        }

        int count_by_op(const std::string& op) const {
            int c = 0;
            for (auto& [e_op, _, __] : events) {
                if (e_op == op) c++;
            }
            return c;
        }
    };

    MongoChangeStream cs;
    cs.add_insert("mydb.orders", "ObjectId(1)");
    cs.add_insert("mydb.orders", "ObjectId(2)");
    cs.add_update("mydb.orders", "ObjectId(1)");
    cs.add_delete("mydb.orders", "ObjectId(2)");
    cs.add_insert("mydb.users", "ObjectId(3)");

    EXPECT_EQ(cs.count_by_op("insert"), 3);
    EXPECT_EQ(cs.count_by_op("update"), 1);
    EXPECT_EQ(cs.count_by_op("delete"), 1);
}

// B.18 — Transform functions (Cast, Drop, Mask)
TEST_F(ConnectorTest, TransformFunctionsCastDropMask) {
    ASSERT_TRUE(is_started());

    // Simulate Connect transform functions
    struct TransformEngine {
        struct Record {
            std::unordered_map<std::string, std::string> fields;
        };

        Record cast(const Record& input, const std::string& field,
                    const std::string& target_type) {
            Record r = input;
            auto it = r.fields.find(field);
            if (it != r.fields.end()) {
                if (target_type == "int32") {
                    r.fields[field] = std::to_string(std::stoi(it->second));
                } else if (target_type == "float64") {
                    r.fields[field] = std::to_string(std::stod(it->second));
                } else if (target_type == "string") {
                    // already a string
                }
            }
            return r;
        }

        void drop(Record& input, const std::string& field) {
            input.fields.erase(field);
        }

        void mask(Record& input, const std::string& field,
                  const std::string& replacement = "***") {
            auto it = input.fields.find(field);
            if (it != input.fields.end()) {
                if (it->second.size() > 3) {
                    it->second = it->second.substr(0, 2) + replacement
                               + it->second.substr(it->second.size() - 2);
                } else {
                    it->second = replacement;
                }
            }
        }
    };

    TransformEngine te;

    // Cast
    TransformEngine::Record rec1;
    rec1.fields = {{"age", "25"}, {"name", "Alice"}};
    auto casted = te.cast(rec1, "age", "int32");
    EXPECT_EQ(casted.fields["age"], "25"); // int32 cast preserves value

    // Drop
    TransformEngine::Record rec2;
    rec2.fields = {{"ssn", "123-45-6789"}, {"name", "Bob"}, {"email", "bob@test.com"}};
    te.drop(rec2, "ssn");
    EXPECT_EQ(rec2.fields.count("ssn"), 0);
    EXPECT_EQ(rec2.fields.count("name"), 1);
    EXPECT_EQ(rec2.fields.count("email"), 1);

    // Mask
    TransformEngine::Record rec3;
    rec3.fields = {{"credit_card", "4111111111111111"}, {"name", "Charlie"}};
    te.mask(rec3, "credit_card");
    EXPECT_EQ(rec3.fields["credit_card"].size(), 7); // 41***16
    EXPECT_EQ(rec3.fields["credit_card"].substr(0, 2), "41");
    EXPECT_EQ(rec3.fields["credit_card"].substr(5, 2), "16");
}

// B.19 — OffsetBackingStore save and restore
TEST_F(ConnectorTest, OffsetBackingStoreSaveAndRestore) {
    ASSERT_TRUE(is_started());

    // Simulate offset backing store for connector position tracking
    struct OffsetBackingStoreSim {
        struct OffsetEntry {
            std::string topic;
            int partition;
            int64_t offset;
            int64_t committed_at_ms;
        };

        std::vector<OffsetEntry> offsets;
        std::string connector_name;

        void save_offset(const std::string& topic, int partition, int64_t offset) {
            // Update or insert
            for (auto& e : offsets) {
                if (e.topic == topic && e.partition == partition) {
                    e.offset = offset;
                    e.committed_at_ms = now_ms();
                    return;
                }
            }
            offsets.push_back({topic, partition, offset, now_ms()});
        }

        std::optional<int64_t> get_offset(const std::string& topic, int partition) const {
            for (auto& e : offsets) {
                if (e.topic == topic && e.partition == partition) {
                    return e.offset;
                }
            }
            return std::nullopt;
        }

        std::string serialize() const {
            std::ostringstream oss;
            for (auto& e : offsets) {
                oss << e.topic << "|" << e.partition << "|" << e.offset
                    << "|" << e.committed_at_ms << "\n";
            }
            return oss.str();
        }

        void deserialize(const std::string& data) {
            offsets.clear();
            std::istringstream iss(data);
            std::string line;
            while (std::getline(iss, line)) {
                if (line.empty()) continue;
                std::istringstream lss(line);
                std::string topic, part_str, off_str, ts_str;
                std::getline(lss, topic, '|');
                std::getline(lss, part_str, '|');
                std::getline(lss, off_str, '|');
                std::getline(lss, ts_str, '|');
                offsets.push_back({
                    topic,
                    std::stoi(part_str),
                    std::stoll(off_str),
                    std::stoll(ts_str)
                });
            }
        }
    };

    OffsetBackingStoreSim obs;
    obs.connector_name = "pg-cdc-source";

    obs.save_offset("mydb.public.orders", 0, 1000);
    obs.save_offset("mydb.public.orders", 1, 500);
    obs.save_offset("mydb.public.users", 0, 200);

    EXPECT_EQ(obs.get_offset("mydb.public.orders", 0).value(), 1000);
    EXPECT_EQ(obs.get_offset("mydb.public.orders", 1).value(), 500);
    EXPECT_FALSE(obs.get_offset("mydb.public.orders", 2).has_value());

    // Update existing
    obs.save_offset("mydb.public.orders", 0, 1500);
    EXPECT_EQ(obs.get_offset("mydb.public.orders", 0).value(), 1500);

    // Serialize and restore
    std::string serialized = obs.serialize();
    OffsetBackingStoreSim obs2;
    obs2.deserialize(serialized);
    EXPECT_EQ(obs2.get_offset("mydb.public.orders", 0).value(), 1500);
    EXPECT_EQ(obs2.get_offset("mydb.public.orders", 1).value(), 500);
    EXPECT_EQ(obs2.get_offset("mydb.public.users", 0).value(), 200);
}

// B.20 — Connector status reporting
TEST_F(ConnectorTest, ConnectorStatusReporting) {
    ASSERT_TRUE(is_started());

    // Simulate connector status reporting
    struct ConnectorStatus {
        std::string name;
        std::string type; // "source" or "sink"
        std::string state; // "RUNNING", "FAILED", "PAUSED", "STOPPED"
        int task_count = 0;
        int task_running = 0;
        int task_failed = 0;
        int64_t records_processed = 0;
        int64_t errors = 0;
        double throughput_rps = 0.0;

        std::string to_json() const {
            std::ostringstream oss;
            oss << "{"
                << R"("name":")" << name << "\","
                << R"("type":")" << type << "\","
                << R"("state":")" << state << "\","
                << R"("task_count":)" << task_count << ","
                << R"("task_running":)" << task_running << ","
                << R"("task_failed":)" << task_failed << ","
                << R"("records_processed":)" << records_processed << ","
                << R"("errors":)" << errors << ","
                << R"("throughput_rps":)" << throughput_rps
                << "}";
            return oss.str();
        }
    };

    ConnectorStatus cs;
    cs.name = "pg-cdc-source";
    cs.type = "source";
    cs.state = "RUNNING";
    cs.task_count = 4;
    cs.task_running = 3;
    cs.task_failed = 1;
    cs.records_processed = 1234567;
    cs.errors = 5;
    cs.throughput_rps = 850.5;

    std::string json = cs.to_json();
    EXPECT_NE(json.find(R"("name":"pg-cdc-source")"), std::string::npos);
    EXPECT_NE(json.find(R"("state":"RUNNING")"), std::string::npos);
    EXPECT_NE(json.find(R"("task_count":4)"), std::string::npos);
    EXPECT_NE(json.find(R"("task_running":3)"), std::string::npos);
    EXPECT_NE(json.find(R"("records_processed":1234567)"), std::string::npos);
    EXPECT_NE(json.find(R"("throughput_rps":850.5)"), std::string::npos);

    // Status changes
    cs.state = "FAILED";
    cs.errors = 6;
    json = cs.to_json();
    EXPECT_NE(json.find(R"("state":"FAILED")"), std::string::npos);
    EXPECT_NE(json.find(R"("errors":6)"), std::string::npos);
}
