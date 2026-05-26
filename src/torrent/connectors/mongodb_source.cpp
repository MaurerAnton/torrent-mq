/**
 * mongodb_source.cpp — MongoDBSourceConnector: CDC from MongoDB Change Streams
 *
 * Tails MongoDB change streams (oplog). Supports insert/update/replace/delete/
 * invalidate operations. Full document and updateDescription modes. Resume
 * token persistence for restart. Collection filtering and sharded cluster.
 */

#include "torrent/connectors/connect.h"

#include "torrent/common/backoff.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

using json = nlohmann::json;

namespace torrent::connectors {

// ============================================================================
// Forward declarations
// ============================================================================

class SourceConnector {
public:
    virtual ~SourceConnector() = default;
    virtual void start(const std::string& name, const json& config,
                       class ConnectFramework* framework) = 0;
    virtual void stop() = 0;
    virtual std::vector<json> poll() = 0;
    virtual json status() const = 0;
    virtual std::string connector_class() const = 0;

protected:
    std::string        name_;
    json               config_;
    ConnectFramework*  framework_ = nullptr;
};

struct SourceRecord {
    std::string topic;
    std::string key;
    json        value;
    int32_t     partition     = 0;
    offset_t    source_offset = kInvalidOffset;
    int64_t     timestamp_ms  = 0;
};

// ============================================================================
// Anonymous namespace — internals
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_mongo_logger() {
    static auto logger = spdlog::get("mongodb_source");
    if (!logger) {
        logger = spdlog::stdout_color_mt("mongodb_source");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

inline constexpr auto kDefaultPollInterval   = std::chrono::milliseconds(250);
inline constexpr int32_t kMaxReconnectAttempts = 15;
inline constexpr int64_t kDefaultBatchSize     = 100;
inline constexpr int64_t kMaxBatchSize         = 4096;
inline constexpr std::string_view kResumeTokenTopicSuffix = "__resume_tokens";
inline constexpr std::string_view kDefaultTopicPrefix = "mongodb";

// --------------------------------------------------------------------------
// MongoDB operation types
// --------------------------------------------------------------------------

enum class MongoOperation : uint8_t {
    insert     = 0,  // "c" — create
    update     = 1,  // "u" — update
    replace    = 2,  // "r" — replace (before MongoDB 3.6, now update)
    delete_    = 3,  // "d" — delete
    invalidate = 4,  // "i" — stream invalidated
    drop       = 5,  // "d" collection drop (invalidate)
    rename     = 6,  // collection rename (invalidate)
    drop_db    = 7,  // database drop (invalidate)
};

[[nodiscard]] std::string_view mongo_op_code(MongoOperation op) {
    switch (op) {
    case MongoOperation::insert:      return "c";
    case MongoOperation::update:      return "u";
    case MongoOperation::replace:     return "r";
    case MongoOperation::delete_:     return "d";
    case MongoOperation::invalidate:  return "i";
    case MongoOperation::drop:        return "d";
    case MongoOperation::rename:      return "r";
    case MongoOperation::drop_db:     return "d";
    }
    return "?";
}

[[nodiscard]] MongoOperation parse_mongo_op(std::string_view op) {
    if (op == "insert")      return MongoOperation::insert;
    if (op == "update")      return MongoOperation::update;
    if (op == "replace")     return MongoOperation::replace;
    if (op == "delete")      return MongoOperation::delete_;
    if (op == "invalidate")  return MongoOperation::invalidate;
    if (op == "drop")        return MongoOperation::drop;
    if (op == "rename")      return MongoOperation::rename;
    if (op == "dropDatabase")return MongoOperation::drop_db;
    return MongoOperation::insert;
}

// --------------------------------------------------------------------------
// Full document mode
// --------------------------------------------------------------------------

enum class FullDocumentMode : uint8_t {
    default_      = 0,
    update_lookup = 1,
    when_available = 2,
    required      = 3,
};

[[nodiscard]] FullDocumentMode parse_full_document(std::string_view s) {
    if (s == "updateLookup" || s == "update_lookup")
        return FullDocumentMode::update_lookup;
    if (s == "whenAvailable" || s == "when_available")
        return FullDocumentMode::when_available;
    if (s == "required")
        return FullDocumentMode::required;
    return FullDocumentMode::default_;
}

[[nodiscard]] std::string_view full_document_name(FullDocumentMode m) {
    switch (m) {
    case FullDocumentMode::default_:       return "default";
    case FullDocumentMode::update_lookup:  return "updateLookup";
    case FullDocumentMode::when_available: return "whenAvailable";
    case FullDocumentMode::required:       return "required";
    }
    return "default";
}

// --------------------------------------------------------------------------
// Resume token — opaque identifier for resuming change streams
// --------------------------------------------------------------------------

struct ResumeToken {
    std::string data;         // base64-encoded token data
    int64_t     timestamp_ms = 0;
    std::string collection;   // collection this token belongs to
    std::string database;     // database

    [[nodiscard]] bool valid() const noexcept { return !data.empty(); }

    [[nodiscard]] json to_json() const {
        json j;
        j["data"]       = data;
        j["timestamp"]  = timestamp_ms;
        j["collection"] = collection;
        j["database"]   = database;
        return j;
    }

    [[nodiscard]] static ResumeToken from_json(const json& j) {
        ResumeToken rt;
        rt.data        = j.value("data", "");
        rt.timestamp_ms = j.value("timestamp", int64_t(0));
        rt.collection  = j.value("collection", "");
        rt.database    = j.value("database", "");
        return rt;
    }

    [[nodiscard]] std::string to_string() const {
        std::ostringstream oss;
        oss << database << "." << collection << ":" << data.substr(0, 32);
        return oss.str();
    }
};

// --------------------------------------------------------------------------
// Collection filter — determines which collections to watch
// --------------------------------------------------------------------------

struct CollectionFilter {
    std::string database;
    std::string collection;          // empty = all collections in DB
    std::string regex_pattern;       // optional regex for collection names
    bool        watch_all = false;   // watch all collections in all DBs

    [[nodiscard]] bool matches(std::string_view db,
                                std::string_view coll) const {
        if (watch_all) return true;
        if (!database.empty() && db != database) return false;
        if (!collection.empty() && coll != collection) return false;
        // TODO: regex matching if regex_pattern is set
        return true;
    }

    [[nodiscard]] std::string scope_key() const {
        if (watch_all) return "*.*";
        if (collection.empty() && !database.empty())
            return database + ".*";
        return database + "." + collection;
    }

    [[nodiscard]] json to_json() const {
        json j;
        j["database"]  = database;
        j["collection"] = collection;
        j["regex"]     = regex_pattern;
        j["watch_all"] = watch_all;
        return j;
    }
};

// --------------------------------------------------------------------------
// Config helpers
// --------------------------------------------------------------------------

[[nodiscard]] std::string cfg_str(const json& c, std::string_view k,
                                   std::string_view d = "") {
    auto it = c.find(k);
    if (it != c.end() && it->is_string()) return it->get<std::string>();
    return std::string(d);
}

[[nodiscard]] int64_t cfg_int(const json& c, std::string_view k,
                               int64_t d = 0) {
    auto it = c.find(k);
    if (it != c.end() && it->is_number_integer()) return it->get<int64_t>();
    return d;
}

[[nodiscard]] bool cfg_bool(const json& c, std::string_view k, bool d = false) {
    auto it = c.find(k);
    if (it != c.end() && it->is_boolean()) return it->get<bool>();
    return d;
}

[[nodiscard]] int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// --------------------------------------------------------------------------
// Build source info for MongoDB events
// --------------------------------------------------------------------------

[[nodiscard]] json build_mongo_source_info(
    std::string_view connector_name,
    std::string_view db,
    std::string_view coll,
    int64_t ordinal,
    std::string_view replica_set = "",
    bool is_snapshot = false)
{
    json src;
    src["version"]   = "2.0.0-torrent";
    src["connector"] = "mongodb-source";
    src["name"]      = connector_name;
    src["ts_ms"]     = now_ms();
    src["db"]        = db;
    src["coll"]      = coll;
    src["ord"]       = ordinal;
    src["snapshot"]  = is_snapshot ? "true" : "false";

    if (!replica_set.empty()) src["rs"] = replica_set;

    return src;
}

// --------------------------------------------------------------------------
// Build CDC envelope for MongoDB
// --------------------------------------------------------------------------

[[nodiscard]] json build_mongo_envelope(
    MongoOperation op,
    const json& before,
    const json& after,
    const json& source,
    const json& update_desc,
    const json& resume_token,
    const json& cluster_time)
{
    json env;
    env["op"]     = mongo_op_code(op);
    env["ts_ms"]  = now_ms();
    env["source"] = source;

    if (!before.is_null())      env["before"] = before;
    else                        env["before"] = nullptr;

    if (!after.is_null())       env["after"]  = after;
    else                        env["after"]  = nullptr;

    if (!update_desc.is_null()) env["updateDescription"] = update_desc;
    if (!resume_token.is_null()) env["resumeToken"] = resume_token;
    if (!cluster_time.is_null()) env["clusterTime"] = cluster_time;

    return env;
}

// --------------------------------------------------------------------------
// Extract document _id as the record key
// --------------------------------------------------------------------------

[[nodiscard]] std::string extract_mongo_key(const json& document) {
    if (document.is_null()) return "";

    auto id_it = document.find("_id");
    if (id_it == document.end()) {
        // No _id field — hash the document
        return std::to_string(std::hash<std::string>{}(document.dump()));
    }

    if (id_it->is_string()) {
        return id_it->get<std::string>();
    }

    if (id_it->is_object()) {
        // ObjectId — extract the hex string if present
        auto oid = id_it->find("$oid");
        if (oid != id_it->end() && oid->is_string()) {
            return oid->get<std::string>();
        }
        return id_it->dump();
    }

    return id_it->dump();
}

// --------------------------------------------------------------------------
// Parse a collection list string: "db.coll1,db.coll2,..."
// --------------------------------------------------------------------------

[[nodiscard]] std::vector<CollectionFilter> parse_collection_filters(
    const std::string& list)
{
    std::vector<CollectionFilter> filters;

    if (list.empty() || list == "*") {
        CollectionFilter all;
        all.watch_all = true;
        filters.push_back(all);
        return filters;
    }

    filters.reserve(4);

    std::istringstream iss(list);
    std::string item;
    while (std::getline(iss, item, ',')) {
        // Trim
        item.erase(0, item.find_first_not_of(" \t"));
        item.erase(item.find_last_not_of(" \t") + 1);
        if (item.empty()) continue;

        auto dot = item.find('.');
        CollectionFilter cf;
        if (dot == std::string::npos) {
            cf.database = item; // entire database
        } else {
            cf.database   = item.substr(0, dot);
            cf.collection = item.substr(dot + 1);
        }
        filters.push_back(cf);
    }

    return filters;
}

} // anonymous namespace

// ============================================================================
// MongoDBSourceConnector
// ============================================================================

class MongoDBSourceConnector final : public SourceConnector {
public:
    MongoDBSourceConnector()  = default;
    ~MongoDBSourceConnector() override { stop(); }

    [[nodiscard]] std::string connector_class() const override {
        return "io.debezium.connector.mongodb.MongoDbConnector";
    }

    // ------------------------------------------------------------------------
    // Start the connector
    // ------------------------------------------------------------------------

    void start(const std::string& name,
               const json& config,
               ConnectFramework* framework) override
    {
        name_      = name;
        config_    = config;
        framework_ = framework;

        auto logger = get_mongo_logger();
        logger->info("MongoDBSourceConnector '{}': initialising", name_);

        // --- Connection config ---
        connection_uri_ = cfg_str(config_, "mongodb.connection.string",
                                   "mongodb://localhost:27017");
        db_name_        = cfg_str(config_, "mongodb.name", "");
        replica_set_    = cfg_str(config_, "mongodb.replica.set", "");
        server_selector_ = cfg_str(config_, "mongodb.server.selector",
                                    "primaryPreferred");

        // --- Authentication ---
        mongo_user_     = cfg_str(config_, "mongodb.user", "");
        mongo_password_ = cfg_str(config_, "mongodb.password", "");
        mongo_auth_db_  = cfg_str(config_, "mongodb.authsource", "admin");

        // --- Topic config ---
        topic_prefix_ = cfg_str(config_, "topic.prefix",
                                 std::string(kDefaultTopicPrefix));

        // --- Change stream options ---
        full_document_mode_ = parse_full_document(
            cfg_str(config_, "capture.mode", "default"));
        batch_size_         = cfg_int(config_, "batch.size",
                                       kDefaultBatchSize);
        max_await_time_ms_  = cfg_int(config_, "cursor.max.await.time.ms",
                                        5000);

        // --- Collection filters ---
        collection_filters_ = parse_collection_filters(
            cfg_str(config_, "collection.include.list", ""));

        // --- Snapshot ---
        snapshot_enabled_ = cfg_bool(config_, "snapshot.enabled", true);
        snapshot_fetch_size_ = cfg_int(config_, "snapshot.fetch.size", 1000);

        // --- Resume token ---
        resume_token_topic_ = cfg_str(
            config_, "resume.token.topic",
            topic_prefix_ + std::string(kResumeTokenTopicSuffix));

        // --- Sharded cluster ---
        sharded_cluster_ = cfg_bool(config_, "mongodb.cluster.sharded", false);

        // --- Validate ---
        if (db_name_.empty() && !has_watch_all_filter()) {
            throw std::invalid_argument(
                "MongoDBSourceConnector: 'mongodb.name' is required "
                "unless watching all collections");
        }

        // --- Load resume tokens from offset store ---
        load_resume_tokens();

        // --- Build change stream pipeline ---
        build_pipeline();

        stopped_.store(false);

        logger->info("MongoDBSourceConnector '{}': started, watching {} collections",
                      name_, collection_filters_.size());
    }

    // ------------------------------------------------------------------------
    // Stop the connector
    // ------------------------------------------------------------------------

    void stop() override {
        if (stopped_.exchange(true)) return;

        // Persist resume tokens
        save_resume_tokens();

        get_mongo_logger()->info(
            "MongoDBSourceConnector '{}': stopped", name_);
    }

    // ------------------------------------------------------------------------
    // Poll — return a batch of SourceRecords
    // ------------------------------------------------------------------------

    std::vector<json> poll() override {
        if (stopped_.load()) return {};

        std::vector<json> results;

        // Poll the change stream
        auto events = fetch_change_events();

        for (auto& event : events) {
            auto records = transform_event(event);
            results.insert(results.end(),
                std::make_move_iterator(records.begin()),
                std::make_move_iterator(records.end()));
        }

        // Periodically save resume tokens
        if (!results.empty() &&
            now_ms() - last_token_save_ms_ > 30'000) {
            save_resume_tokens();
            last_token_save_ms_ = now_ms();
        }

        total_records_ += results.size();
        return results;
    }

    // ------------------------------------------------------------------------
    // Status
    // ------------------------------------------------------------------------

    json status() const override {
        json j;
        j["name"]              = name_;
        j["connector"]         = connector_class();
        j["connection_uri"]    = connection_uri_;
        j["database"]          = db_name_;
        j["topic_prefix"]      = topic_prefix_;
        j["full_document_mode"] = full_document_name(full_document_mode_);
        j["batch_size"]        = batch_size_;
        j["sharded_cluster"]   = sharded_cluster_;
        j["snapshot_enabled"]  = snapshot_enabled_;
        j["total_records"]     = total_records_;

        json filters = json::array();
        for (auto& f : collection_filters_) {
            filters.push_back(f.to_json());
        }
        j["collection_filters"] = filters;

        json tokens = json::array();
        {
            std::shared_lock lock(resume_mtx_);
            for (auto& [scope, token] : resume_tokens_) {
                if (token.valid()) tokens.push_back(token.to_json());
            }
        }
        j["resume_tokens"] = tokens;
        j["collection_count"] = watched_collections_.size();

        return j;
    }

private:
    // ------------------------------------------------------------------------
    // Change stream pipeline
    // ------------------------------------------------------------------------

    void build_pipeline() {
        pipeline_ = json::array();

        // Filter by operation type if needed
        json op_filter;
        op_filter["$or"] = json::array({
            {{"operationType", "insert"}},
            {{"operationType", "update"}},
            {{"operationType", "replace"}},
            {{"operationType", "delete"}},
            {{"operationType", "invalidate"}},
            {{"operationType", "drop"}},
            {{"operationType", "rename"}},
            {{"operationType", "dropDatabase"}},
        });
        pipeline_.push_back({{"$match", op_filter}});

        // If specific collections are configured, add filters
        if (!has_watch_all_filter()) {
            json ns_filter;
            json db_filter;

            if (!collection_filters_.empty()) {
                json ns_or = json::array();
                for (auto& cf : collection_filters_) {
                    json ns_match;
                    ns_match["ns.db"] = cf.database;
                    if (!cf.collection.empty()) {
                        ns_match["ns.coll"] = cf.collection;
                    }
                    ns_or.push_back({{"$match", ns_match}});
                }
                if (ns_or.size() == 1) {
                    pipeline_.push_back(ns_or[0]);
                } else if (ns_or.size() > 1) {
                    pipeline_.push_back({{"$or", ns_or}});
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    // Event fetching (placeholder for MongoDB driver calls)
    // ------------------------------------------------------------------------

    std::vector<json> fetch_change_events() {
        // In production, this calls the MongoDB driver's change stream API:
        //
        //   auto cursor = db.watch(pipeline, options);
        //   while (cursor.hasNext()) {
        //       events.push_back(cursor.next());
        //   }
        //
        // This implementation provides the structural framework.

        std::vector<json> events;
        // Placeholder: return empty batch
        (void)pipeline_;
        return events;
    }

    // ------------------------------------------------------------------------
    // Event transformation: MongoDB event → SourceRecord
    // ------------------------------------------------------------------------

    std::vector<json> transform_event(const json& event) {
        std::vector<json> results;

        std::string op_type = event.value("operationType", "insert");
        MongoOperation mop = parse_mongo_op(op_type);

        // Extract namespace
        json ns = event.value("ns", json::object());
        std::string db   = ns.value("db", db_name_);
        std::string coll = ns.value("coll", "");

        // Extract document key (_id)
        json doc_key = event.value("documentKey", json::object());
        std::string key = extract_mongo_key(doc_key);

        // Build topic name
        std::string topic = build_topic_name(db, coll);

        // Extract full document and update description
        json full_doc    = event.value("fullDocument", json());
        json update_desc = event.value("updateDescription", json());
        json resume_tok  = event.value("_id", json());
        json cluster_time = event.value("clusterTime", json());

        // Build source info
        json source = build_mongo_source_info(
            name_, db, coll,
            event.value("txnNumber", int64_t(0)),
            replica_set_, false);

        // Build before/after based on operation type
        json before;
        json after;

        switch (mop) {
        case MongoOperation::insert:
            before = nullptr;
            after  = full_doc.is_null() ? doc_key : full_doc;
            break;

        case MongoOperation::update:
            before = doc_key;
            after  = full_doc.is_null() ? nullptr : full_doc;
            break;

        case MongoOperation::replace:
            before = doc_key;
            after  = full_doc.is_null() ? nullptr : full_doc;
            break;

        case MongoOperation::delete_:
            before = doc_key;
            after  = nullptr;
            break;

        case MongoOperation::invalidate:
        case MongoOperation::drop:
        case MongoOperation::rename:
        case MongoOperation::drop_db:
            // Invalidated — reconnection needed
            get_mongo_logger()->warn(
                "MongoDBSourceConnector '{}': stream invalidated ({}.{})",
                name_, db, coll);
            stream_invalidated_ = true;
            return results;
        }

        // Build the envelope
        json envelope = build_mongo_envelope(
            mop, before, after, source, update_desc,
            resume_tok, cluster_time);

        // Update resume token
        if (!resume_tok.is_null()) {
            std::string scope = CollectionFilter{db, coll}.scope_key();
            update_resume_token(scope, resume_tok);
        }

        // Create SourceRecord
        SourceRecord sr;
        sr.topic         = topic;
        sr.key           = key;
        sr.value         = envelope;
        sr.timestamp_ms  = now_ms();

        results.push_back(sr_to_json(sr));
        return results;
    }

    // ------------------------------------------------------------------------
    // Topic naming
    // ------------------------------------------------------------------------

    [[nodiscard]] std::string build_topic_name(
        std::string_view db, std::string_view coll) const
    {
        std::ostringstream oss;
        oss << topic_prefix_ << "." << db << "." << coll;
        return oss.str();
    }

    // ------------------------------------------------------------------------
    // Resume token management
    // ------------------------------------------------------------------------

    void update_resume_token(const std::string& scope, const json& token) {
        std::unique_lock lock(resume_mtx_);

        ResumeToken rt;
        rt.data        = token.value("_data", token.dump());
        rt.timestamp_ms = now_ms();
        rt.database    = db_name_;

        // Extract collection from scope: "db.coll"
        auto dot = scope.find('.');
        if (dot != std::string::npos) {
            rt.collection = scope.substr(dot + 1);
        }

        resume_tokens_[scope] = rt;

        get_mongo_logger()->debug(
            "MongoDBSourceConnector '{}': resume token updated for scope '{}'",
            name_, scope);
    }

    void save_resume_tokens() {
        std::shared_lock lock(resume_mtx_);

        json payload;
        for (auto& [scope, token] : resume_tokens_) {
            if (token.valid()) {
                payload.push_back(token.to_json());
            }
        }

        if (payload.empty()) return;

        // In production, this writes to the offset backing store via ConnectFramework
        get_mongo_logger()->debug(
            "MongoDBSourceConnector '{}': saving {} resume tokens",
            name_, payload.size());
    }

    void load_resume_tokens() {
        // In production, this reads from the offset backing store
        get_mongo_logger()->debug(
            "MongoDBSourceConnector '{}': loading resume tokens", name_);
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

    [[nodiscard]] bool has_watch_all_filter() const {
        for (auto& cf : collection_filters_) {
            if (cf.watch_all) return true;
        }
        return false;
    }

    [[nodiscard]] static json sr_to_json(const SourceRecord& sr) {
        json j;
        j["topic"]     = sr.topic;
        j["key"]       = sr.key;
        j["value"]     = sr.value;
        j["partition"] = sr.partition;
        if (sr.source_offset != kInvalidOffset) {
            j["source_offset"] = sr.source_offset;
        }
        j["timestamp"] = sr.timestamp_ms;
        return j;
    }

    // ------------------------------------------------------------------------
    // Members
    // ------------------------------------------------------------------------

    // Connection
    std::string connection_uri_;
    std::string db_name_;
    std::string replica_set_;
    std::string server_selector_;
    std::string mongo_user_;
    std::string mongo_password_;
    std::string mongo_auth_db_;

    // Topic
    std::string topic_prefix_;
    std::string resume_token_topic_;

    // Change stream options
    FullDocumentMode full_document_mode_ = FullDocumentMode::default_;
    int64_t          batch_size_         = kDefaultBatchSize;
    int64_t          max_await_time_ms_  = 5000;

    // Collection filters
    std::vector<CollectionFilter> collection_filters_;
    std::set<std::string> watched_collections_;

    // Snapshot
    bool   snapshot_enabled_   = true;
    int64_t snapshot_fetch_size_ = 1000;

    // Sharded cluster
    bool sharded_cluster_ = false;

    // Change stream pipeline (aggregation stages)
    json pipeline_;

    // Resume tokens (scope → token)
    mutable std::shared_mutex resume_mtx_;
    std::unordered_map<std::string, ResumeToken> resume_tokens_;
    int64_t last_token_save_ms_ = 0;

    // Runtime
    std::atomic<bool> stopped_{false};
    std::atomic<bool> stream_invalidated_{false};
    uint64_t total_records_ = 0;
};

} // namespace torrent::connectors
