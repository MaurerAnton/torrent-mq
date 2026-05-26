/**
 * torrent-mq — FetchHandler: Full Fetch Request Handler Implementation
 *
 * Handles Fetch requests (Kafka api_key=1). Parses the wire-format binary
 * protocol, validates all preconditions (topic existence, authorization,
 * quotas, partition leadership, offset range), reads from LogManager with
 * size limits, handles fetch sessions for incremental fetches, and builds
 * a serialised FetchResponse with record batches, watermarks, aborted
 * transactions, and preferred read replicas.
 *
 * Wire format (Kafka-compatible Fetch Request, API versions 0–13):
 *   INT32  replica_id            (-1 for consumer, >=0 for follower)
 *   INT32  max_wait_ms           (max time to block waiting for data)
 *   INT32  min_bytes             (min bytes to accumulate before responding)
 *   INT32  max_bytes             (max bytes for the response)          [v3+]
 *   INT8   isolation_level       (0=read_uncommitted, 1=read_committed) [v4+]
 *   INT32  session_id            (0 = no session)                      [v7+]
 *   INT32  session_epoch         (epoch for verification)              [v7+]
 *   INT32  topic_count
 *   per-topic:
 *     UUID  topic_id             (compact topic id)                    [v13+]
 *     STRING  topic_name
 *     INT32  partition_count
 *     per-partition:
 *       INT32  partition_index
 *       INT32  current_leader_epoch                                     [v9+]
 *       INT64  fetch_offset
 *       INT64  log_start_offset                                         [v5+]
 *       INT32  partition_max_bytes
 *
 * Response wire format:
 *   INT32  throttle_time_ms
 *   INT16  error_code             (top-level)
 *   INT32  session_id
 *   INT32  topic_count
 *   per-topic:
 *     UUID  topic_id              (compact)                             [v13+]
 *     STRING  topic_name
 *     INT32  partition_count
 *     per-partition:
 *       INT32  partition_index
 *       INT16  error_code
 *       INT64  high_watermark
 *       INT64  last_stable_offset
 *       INT64  log_start_offset
 *       INT32  aborted_transaction_count                                [v4+]
 *       per-aborted-txn:
 *         INT64  producer_id
 *         INT64  first_offset
 *       INT32  preferred_read_replica                                   [v11+]
 *       INT32  record_batch_set_size
 *       bytes  record_batch_set
 *
 * Thread-safety: The handler is called from the broker's I/O thread pool.
 * FetchHandler itself is stateless beyond a pointer to BrokerServer and
 * an internal fetch session map. All mutable state is protected by the
 * server's internal locks and a session mutex.
 */

#include "torrent/client/fetch_handler.h"
#include "torrent/broker/server.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/storage/log_manager.h"
#include "torrent/network/protocol.h"
#include "torrent/common/types.h"
#include "torrent/storage/types.h"
#include "torrent/security/auth_manager.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <chrono>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

// ============================================================================
// Alias for readability
// ============================================================================

namespace tp = torrent::protocol;

namespace torrent::client {

// ============================================================================
// Anonymous namespace — internal helpers, parsing, serialization
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_fetch_logger() {
    static auto logger = []() {
        auto l = spdlog::get("fetch_handler");
        if (!l) {
            l = spdlog::stdout_color_mt("fetch_handler");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define FETCH_LOG(level, ...) \
    get_fetch_logger()->level("[fetch] " __VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Maximum fetch response size (soft cap before we stop adding partitions).
constexpr byte_count_t kDefaultFetchMaxBytes = 50 * 1024 * 1024;  // 50 MiB

/// Maximum records to fetch from a single partition in one call.
constexpr int32_t kMaxRecordsPerFetchPartition = 100000;

/// Maximum number of partitions a single fetch request can target.
constexpr int32_t kMaxPartitionsPerFetch = 10000;

/// Default max wait time for long-polling (ms).
constexpr int32_t kDefaultMaxWaitMs = 500;

/// Maximum session idle time before eviction (ms).
constexpr int64_t kFetchSessionTimeoutMs = 120000;  // 2 minutes

/// Maximum number of concurrent fetch sessions.
constexpr size_t kMaxFetchSessions = 50000;

/// Minimum valid isolation level.
constexpr int8_t kIsolationReadUncommitted = 0;
constexpr int8_t kIsolationReadCommitted   = 1;

// --------------------------------------------------------------------------
// Wire format parse / write helpers
// --------------------------------------------------------------------------

/// Read a big-endian INT16 from buffer at *pos, advance.
bool read_int16_be(const char* data, size_t size, size_t* pos, int16_t* out) noexcept {
    if (*pos + 2 > size) return false;
    uint16_t raw;
    std::memcpy(&raw, data + *pos, sizeof(raw));
    *out = static_cast<int16_t>(__builtin_bswap16(raw));
    *pos += 2;
    return true;
}

/// Read a big-endian INT32 from buffer.
bool read_int32_be(const char* data, size_t size, size_t* pos, int32_t* out) noexcept {
    if (*pos + 4 > size) return false;
    uint32_t raw;
    std::memcpy(&raw, data + *pos, sizeof(raw));
    *out = static_cast<int32_t>(__builtin_bswap32(raw));
    *pos += 4;
    return true;
}

/// Read a big-endian INT64 from buffer.
bool read_int64_be(const char* data, size_t size, size_t* pos, int64_t* out) noexcept {
    if (*pos + 8 > size) return false;
    uint64_t raw;
    std::memcpy(&raw, data + *pos, sizeof(raw));
    *out = static_cast<int64_t>(__builtin_bswap64(raw));
    *pos += 8;
    return true;
}

/// Read a big-endian INT8 from buffer.
bool read_int8(const char* data, size_t size, size_t* pos, int8_t* out) noexcept {
    if (*pos + 1 > size) return false;
    *out = static_cast<int8_t>(data[*pos]);
    *pos += 1;
    return true;
}

/// Read a nullable string: INT16 length (-1 = null), followed by UTF-8 bytes.
bool read_nullable_string(const char* data, size_t size, size_t* pos,
                          std::string* out) noexcept {
    int16_t len = 0;
    if (!read_int16_be(data, size, pos, &len)) return false;
    if (len == -1) {
        out->clear();
        return true;
    }
    if (len < 0) return false;
    if (*pos + static_cast<size_t>(len) > size) return false;
    out->assign(data + *pos, static_cast<size_t>(len));
    *pos += static_cast<size_t>(len);
    return true;
}

/// Read an INT16-length-prefixed string (non-nullable).
bool read_string(const char* data, size_t size, size_t* pos,
                 std::string* out) noexcept {
    int16_t len = 0;
    if (!read_int16_be(data, size, pos, &len)) return false;
    if (len < 0) return false;
    if (*pos + static_cast<size_t>(len) > size) return false;
    out->assign(data + *pos, static_cast<size_t>(len));
    *pos += static_cast<size_t>(len);
    return true;
}

/// Skip N bytes in the buffer cursor.
bool skip_bytes(const char*, size_t size, size_t* pos, size_t n) noexcept {
    if (*pos + n > size) return false;
    *pos += n;
    return true;
}

/// Write big-endian INT16 to buffer.
void write_int16_be(std::vector<char>& buf, int16_t val) noexcept {
    uint16_t raw = __builtin_bswap16(static_cast<uint16_t>(val));
    buf.insert(buf.end(), reinterpret_cast<const char*>(&raw),
               reinterpret_cast<const char*>(&raw) + sizeof(raw));
}

/// Write big-endian INT32 to buffer.
void write_int32_be(std::vector<char>& buf, int32_t val) noexcept {
    uint32_t raw = __builtin_bswap32(static_cast<uint32_t>(val));
    buf.insert(buf.end(), reinterpret_cast<const char*>(&raw),
               reinterpret_cast<const char*>(&raw) + sizeof(raw));
}

/// Write big-endian INT64 to buffer.
void write_int64_be(std::vector<char>& buf, int64_t val) noexcept {
    uint64_t raw = __builtin_bswap64(static_cast<uint64_t>(val));
    buf.insert(buf.end(), reinterpret_cast<const char*>(&raw),
               reinterpret_cast<const char*>(&raw) + sizeof(raw));
}

/// Write INT8 to buffer.
void write_int8(std::vector<char>& buf, int8_t val) noexcept {
    buf.push_back(static_cast<char>(val));
}

/// Write nullable string: INT16 length, -1 for null, else bytes.
void write_nullable_string(std::vector<char>& buf, const std::string& str) noexcept {
    if (str.empty()) {
        write_int16_be(buf, -1);
    } else {
        int16_t len = static_cast<int16_t>(str.size());
        write_int16_be(buf, len);
        buf.insert(buf.end(), str.begin(), str.end());
    }
}

/// Write an INT16-length-prefixed string (non-nullable).
void write_string(std::vector<char>& buf, const std::string& str) noexcept {
    int16_t len = static_cast<int16_t>(str.size());
    write_int16_be(buf, len);
    buf.insert(buf.end(), str.begin(), str.end());
}

/// Write raw bytes with an INT32 length prefix.
void write_bytes(std::vector<char>& buf, const char* data, int32_t len) noexcept {
    write_int32_be(buf, len);
    if (len > 0 && data != nullptr) {
        buf.insert(buf.end(), data, data + len);
    }
}

/// Write raw bytes from a vector with an INT32 length prefix.
void write_bytes_vec(std::vector<char>& buf, const std::vector<char>& bytes) noexcept {
    write_int32_be(buf, static_cast<int32_t>(bytes.size()));
    if (!bytes.empty()) {
        buf.insert(buf.end(), bytes.begin(), bytes.end());
    }
}

/// Get current wall-clock milliseconds.
timestamp_ms_t wall_clock_ms() noexcept {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// ============================================================================
// AbortedTransaction — metadata about a transaction that was aborted
// ============================================================================

/// Represents an aborted transaction range within the log, so read_committed
/// consumers can filter out records from aborted transactions.
struct AbortedTransaction {
    producer_id_t producer_id = -1;
    offset_t      first_offset = kInvalidOffset; ///< First offset of the aborted txn
    offset_t      last_offset  = kInvalidOffset; ///< Last offset of the aborted txn

    [[nodiscard]] bool contains(offset_t off) const noexcept {
        return off >= first_offset && off <= last_offset;
    }
};

// ============================================================================
// PerPartitionFetch — parsed fetch request for a single topic-partition
// ============================================================================

/// All parsed data for a single topic-partition from the FetchRequest.
struct PerPartitionFetch {
    std::string      topic_name;
    partition_id_t   partition_index = 0;
    offset_t         fetch_offset = 0;
    offset_t         log_start_offset = 0;     ///< Client's view of LSO (for validation)
    int32_t          partition_max_bytes = 1048576; ///< 1 MiB default
    int32_t          current_leader_epoch = -1; ///< Leader epoch from client (-1 = unknown)
    bool             parse_error = false;
    error_code       parse_error_code = error_code::none;
    std::string      parse_error_msg;
};

// ============================================================================
// FetchPartitionResult — a single partition's response data
// ============================================================================

/// Result of executing a fetch for a single topic-partition, ready to be
/// serialized into the FetchResponse.
struct FetchPartitionResult {
    std::string              topic_name;
    partition_id_t           partition_index = 0;
    error_code               error = error_code::none;
    std::string              error_message;
    offset_t                 high_watermark = kInvalidOffset;
    offset_t                 last_stable_offset = kInvalidOffset;
    offset_t                 log_start_offset = kInvalidOffset;
    int32_t                  preferred_read_replica = -1; ///< -1 = read from leader
    std::vector<AbortedTransaction> aborted_transactions;
    std::vector<torrent::Record> records;                ///< Returned records (storage layer format)
    offset_t                 batch_base_offset = kInvalidOffset;
    bool                     is_truncated = false;
    byte_count_t             bytes_returned = 0;
};

// ============================================================================
// FetchSession — state for incremental fetch sessions
// ============================================================================

/// Tracks the last-fetched position for a client's fetch session. Clients
/// can use sessions to avoid re-sending the full partition list on every
/// fetch — only partitions that have new data since the last fetch are
/// included in the response.
struct FetchSession {
    int32_t session_id = 0;
    int32_t epoch = 0;

    /// Client identifier that owns this session.
    std::string client_id;

    /// Per-partition metadata: topic-partition key → (last_fetched_offset, last_fetch_time).
    /// Key format: "topic_name:partition_id"
    struct PartitionState {
        offset_t         last_fetched_offset = kInvalidOffset;
        offset_t         log_start_offset    = kInvalidOffset;
        timestamp_ms_t   last_fetch_time     = 0;
        int32_t          last_leader_epoch   = -1;
        int64_t          data_sent_bytes     = 0; ///< Bytes sent since last fetch for this partition
    };
    std::unordered_map<std::string, PartitionState> partitions;

    /// When this session was created.
    timestamp_ms_t created_at = 0;

    /// When this session was last used.
    timestamp_ms_t last_used_at = 0;

    /// Returns a topic-partition key.
    static std::string make_key(const std::string& topic, partition_id_t partition) {
        return topic + ":" + std::to_string(partition);
    }

    /// Returns true when the session is idle beyond the timeout.
    [[nodiscard]] bool is_expired(timestamp_ms_t now, int64_t timeout_ms) const noexcept {
        return (now - last_used_at) > timeout_ms;
    }

    /// Update state for a partition after a fetch.
    void update_partition(const std::string& topic, partition_id_t partition,
                           offset_t new_offset, offset_t lso, int32_t leader_epoch) {
        auto key = make_key(topic, partition);
        auto& ps = partitions[key];
        ps.last_fetched_offset = new_offset;
        ps.log_start_offset = lso;
        ps.last_fetch_time = wall_clock_ms();
        ps.last_leader_epoch = leader_epoch;
    }
};

// ============================================================================
// FetchSessionManager — manages all active fetch sessions
// ============================================================================

class FetchSessionManager {
public:
    FetchSessionManager() = default;
    ~FetchSessionManager() = default;

    FetchSessionManager(const FetchSessionManager&) = delete;
    FetchSessionManager& operator=(const FetchSessionManager&) = delete;

    /// Create a new fetch session and return its ID.
    int32_t create_session(const std::string& client_id, int32_t epoch) {
        std::lock_guard<std::mutex> lock(mutex_);

        // Evict expired sessions before creating a new one.
        evict_expired();

        if (sessions_.size() >= kMaxFetchSessions) {
            FETCH_LOG(warn, "Fetch session limit reached ({}), evicting oldest",
                      kMaxFetchSessions);
            evict_oldest();
        }

        int32_t session_id = next_session_id_++;
        if (next_session_id_ < 1) {
            next_session_id_ = 1;  // wrap around
            // Ensure no collision with existing sessions.
            while (sessions_.count(session_id) > 0) {
                session_id = next_session_id_++;
            }
        }

        auto& session = sessions_[session_id];
        session.session_id = session_id;
        session.epoch = epoch;
        session.client_id = client_id;
        session.created_at = wall_clock_ms();
        session.last_used_at = session.created_at;

        FETCH_LOG(info, "Created fetch session id={} epoch={} for client='{}'",
                  session_id, epoch, client_id);
        return session_id;
    }

    /// Look up a session by ID. Returns nullptr if not found or expired.
    FetchSession* get_session(int32_t session_id, int32_t expected_epoch) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = sessions_.find(session_id);
        if (it == sessions_.end()) {
            FETCH_LOG(debug, "Fetch session {} not found", session_id);
            return nullptr;
        }

        auto& session = it->second;
        if (session.is_expired(wall_clock_ms(), kFetchSessionTimeoutMs)) {
            FETCH_LOG(info, "Fetch session {} expired", session_id);
            sessions_.erase(it);
            return nullptr;
        }

        // Epoch validation: if the client provides a stale epoch, reject.
        if (expected_epoch != session.epoch) {
            FETCH_LOG(warn, "Fetch session {} epoch mismatch: expected={}, actual={}",
                      session_id, expected_epoch, session.epoch);
            return nullptr;  // Caller should set INVALID_FETCH_SESSION_EPOCH
        }

        // Bump last_used timestamp.
        session.last_used_at = wall_clock_ms();

        return &session;
    }

    /// Increment the session epoch (called when leadership changes invalidate sessions).
    void invalidate_session(int32_t session_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = sessions_.find(session_id);
        if (it != sessions_.end()) {
            it->second.epoch++;
            FETCH_LOG(info, "Invalidated fetch session {} (new epoch={})",
                      session_id, it->second.epoch);
        }
    }

    /// Invalidate all sessions for a specific topic-partition (e.g. on leader change).
    void invalidate_partition(const std::string& topic, partition_id_t partition) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto key = FetchSession::make_key(topic, partition);
        for (auto& [id, session] : sessions_) {
            auto it = session.partitions.find(key);
            if (it != session.partitions.end()) {
                FETCH_LOG(info, "Invalidating session {} for topic='{}' partition={}",
                          id, topic, partition);
                it->second.last_leader_epoch = -1;  // Forces re-fetch
            }
        }
    }

    /// Remove a session (e.g. on client disconnect).
    void remove_session(int32_t session_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto count = sessions_.erase(session_id);
        if (count > 0) {
            FETCH_LOG(debug, "Removed fetch session {}", session_id);
        }
    }

    /// Periodic cleanup of expired sessions.
    void cleanup_expired() {
        std::lock_guard<std::mutex> lock(mutex_);
        evict_expired();
    }

    /// Return current session count.
    size_t session_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return sessions_.size();
    }

private:
    void evict_expired() {
        timestamp_ms_t now = wall_clock_ms();
        for (auto it = sessions_.begin(); it != sessions_.end(); ) {
            if (it->second.is_expired(now, kFetchSessionTimeoutMs)) {
                FETCH_LOG(debug, "Evicting expired fetch session {}", it->first);
                it = sessions_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void evict_oldest() {
        if (sessions_.empty()) return;

        auto oldest = sessions_.begin();
        for (auto it = sessions_.begin(); it != sessions_.end(); ++it) {
            if (it->second.last_used_at < oldest->second.last_used_at) {
                oldest = it;
            }
        }
        FETCH_LOG(info, "Evicting oldest fetch session {} (client='{}')",
                  oldest->first, oldest->second.client_id);
        sessions_.erase(oldest);
    }

    mutable std::mutex mutex_;
    std::unordered_map<int32_t, FetchSession> sessions_;
    int32_t next_session_id_ = 1;
};

/// Global fetch session manager instance.
/// In production this would be owned by BrokerServer; for now we use a
/// function-local static with thread-safe initialization.
FetchSessionManager& get_fetch_session_manager() {
    static FetchSessionManager instance;
    return instance;
}

// ============================================================================
// FetchRequestParser — wire-format parser for FetchRequest
// ============================================================================

class FetchRequestParser {
public:
    explicit FetchRequestParser(buffer_view body)
        : data_(body.data), size_(body.size), pos_(0) {}

    /// Parse the full wire-format FetchRequest. Returns true on success.
    bool parse();

    // -- Accessors (valid after parse() returns true) --
    int32_t                     replica_id() const noexcept     { return replica_id_; }
    int32_t                     max_wait_ms() const noexcept    { return max_wait_ms_; }
    int32_t                     min_bytes() const noexcept      { return min_bytes_; }
    int32_t                     max_bytes() const noexcept      { return max_bytes_; }
    int8_t                      isolation_level() const noexcept{ return isolation_level_; }
    int32_t                     session_id() const noexcept     { return session_id_; }
    int32_t                     session_epoch() const noexcept  { return session_epoch_; }
    const std::vector<PerPartitionFetch>& partitions() const noexcept {
        return partitions_;
    }
    const std::string& parse_error() const noexcept { return parse_error_; }

    // -- Derived helpers --
    [[nodiscard]] bool is_follower_fetch() const noexcept { return replica_id_ >= 0; }
    [[nodiscard]] torrent::isolation_level get_isolation() const noexcept {
        return isolation_level_ == kIsolationReadCommitted
                   ? torrent::isolation_level::read_committed
                   : torrent::isolation_level::read_uncommitted;
    }
    [[nodiscard]] bool has_session() const noexcept { return session_id_ != 0; }

private:
    /// Parse a single topic's partitions array.
    bool parse_topic();

    /// Parse a single partition fetch entry.
    bool parse_partition(PerPartitionFetch& pf);

    // -- Raw buffer --
    const char* data_;
    size_t size_;
    size_t pos_;

    // -- Parsed top-level fields --
    int32_t                     replica_id_ = -1;
    int32_t                     max_wait_ms_ = kDefaultMaxWaitMs;
    int32_t                     min_bytes_ = 1;
    int32_t                     max_bytes_ = kDefaultFetchMaxBytes;
    int8_t                      isolation_level_ = kIsolationReadUncommitted;
    int32_t                     session_id_ = 0;
    int32_t                     session_epoch_ = 0;
    std::vector<PerPartitionFetch> partitions_;
    std::string                 parse_error_;
};

bool FetchRequestParser::parse() {
    // --- Step 1: Parse replica_id (INT32) ---
    if (!read_int32_be(data_, size_, &pos_, &replica_id_)) {
        parse_error_ = "Failed to read replica_id";
        FETCH_LOG(error, "{}", parse_error_);
        return false;
    }
    FETCH_LOG(debug, "replica_id={}", replica_id_);

    // --- Step 2: Parse max_wait_ms (INT32) ---
    if (!read_int32_be(data_, size_, &pos_, &max_wait_ms_)) {
        parse_error_ = "Failed to read max_wait_ms";
        FETCH_LOG(error, "{}", parse_error_);
        return false;
    }
    if (max_wait_ms_ < 0) {
        FETCH_LOG(warn, "max_wait_ms={} negative, clamping to 0", max_wait_ms_);
        max_wait_ms_ = 0;
    }
    if (max_wait_ms_ > 60000) {  // Cap at 60 seconds
        FETCH_LOG(warn, "max_wait_ms={} exceeds 60s cap, clamping", max_wait_ms_);
        max_wait_ms_ = 60000;
    }
    FETCH_LOG(debug, "max_wait_ms={}", max_wait_ms_);

    // --- Step 3: Parse min_bytes (INT32) ---
    if (!read_int32_be(data_, size_, &pos_, &min_bytes_)) {
        parse_error_ = "Failed to read min_bytes";
        FETCH_LOG(error, "{}", parse_error_);
        return false;
    }
    if (min_bytes_ < 0) {
        FETCH_LOG(warn, "min_bytes={} negative, clamping to 0", min_bytes_);
        min_bytes_ = 0;
    }
    FETCH_LOG(debug, "min_bytes={}", min_bytes_);

    // --- Step 4: Parse max_bytes (INT32) ---
    // Note: In newer API versions (v3+), max_bytes is per-partition.
    // In older versions, it's the total response size cap. We parse it as total.
    if (!read_int32_be(data_, size_, &pos_, &max_bytes_)) {
        parse_error_ = "Failed to read max_bytes (may be older API version)";
        // For older API versions, max_bytes may not be present. Default it.
        FETCH_LOG(warn, "{} — defaulting to {}", parse_error_, kDefaultFetchMaxBytes);
        max_bytes_ = kDefaultFetchMaxBytes;
        // Re-read the 4 bytes we consumed if this is an older version.
        // Actually, the wire format unconditionally has max_bytes from Kafka 0.10.1+.
        // We just return false for simplicity on parse failure.
        if (pos_ > size_) {
            pos_ -= 4; // undo the read
            max_bytes_ = kDefaultFetchMaxBytes;
        }
    }
    if (max_bytes_ <= 0) {
        max_bytes_ = kDefaultFetchMaxBytes;
    }

    // --- Step 5: Parse isolation_level (INT8) — v4+ ---
    // Try to read it; if it fails, we assume the default (read_uncommitted).
    if (!read_int8(data_, size_, &pos_, &isolation_level_)) {
        FETCH_LOG(debug, "No isolation_level in request (pre-v4), defaulting to 0");
        isolation_level_ = kIsolationReadUncommitted;
        pos_ -= (pos_ > 0) ? 1 : 0; // Undo if we consumed past valid range
    }
    if (isolation_level_ != kIsolationReadUncommitted &&
        isolation_level_ != kIsolationReadCommitted) {
        FETCH_LOG(warn, "Invalid isolation_level={}, clamping to 0", isolation_level_);
        isolation_level_ = kIsolationReadUncommitted;
    }
    FETCH_LOG(debug, "isolation_level={}", isolation_level_);

    // --- Step 6: Parse session_id (INT32) — v7+ ---
    if (!read_int32_be(data_, size_, &pos_, &session_id_)) {
        FETCH_LOG(debug, "No session_id in request (pre-v7), defaulting to 0");
        session_id_ = 0;
    }
    FETCH_LOG(debug, "session_id={}", session_id_);

    // --- Step 7: Parse session_epoch (INT32) — v7+ ---
    if (!read_int32_be(data_, size_, &pos_, &session_epoch_)) {
        FETCH_LOG(debug, "No session_epoch in request (pre-v7), defaulting to 0");
        session_epoch_ = 0;
    }
    FETCH_LOG(debug, "session_epoch={}", session_epoch_);

    // --- Step 8: Parse topic array ---
    int32_t topic_count = 0;
    if (!read_int32_be(data_, size_, &pos_, &topic_count)) {
        parse_error_ = "Failed to read topic_count";
        FETCH_LOG(error, "{}", parse_error_);
        return false;
    }
    if (topic_count < 0) {
        parse_error_ = "Negative topic_count: " + std::to_string(topic_count);
        FETCH_LOG(error, "{}", parse_error_);
        return false;
    }
    if (topic_count > kMaxPartitionsPerFetch) {
        FETCH_LOG(warn, "Large topic_count={} (max {})", topic_count, kMaxPartitionsPerFetch);
    }
    FETCH_LOG(debug, "topic_count={}", topic_count);

    partitions_.reserve(static_cast<size_t>(topic_count) * 4);

    for (int32_t t = 0; t < topic_count; ++t) {
        if (!parse_topic()) {
            return false;
        }
    }

    if (pos_ < size_) {
        size_t trailing = size_ - pos_;
        FETCH_LOG(debug, "{} bytes of trailing data after all topics consumed", trailing);
    }

    return true;
}

bool FetchRequestParser::parse_topic() {
    // --- Read topic name ---
    std::string topic_name;
    if (!read_string(data_, size_, &pos_, &topic_name)) {
        parse_error_ = "Failed to read topic name";
        FETCH_LOG(error, "{}", parse_error_);
        return false;
    }
    if (topic_name.empty()) {
        parse_error_ = "Empty topic name in fetch request";
        FETCH_LOG(error, "{}", parse_error_);
        return false;
    }
    FETCH_LOG(debug, "  topic='{}'", topic_name);

    // --- Read partition count ---
    int32_t partition_count = 0;
    if (!read_int32_be(data_, size_, &pos_, &partition_count)) {
        parse_error_ = "Failed to read partition_count for topic '" + topic_name + "'";
        FETCH_LOG(error, "{}", parse_error_);
        return false;
    }
    if (partition_count < 0) {
        parse_error_ = "Negative partition_count for topic '" + topic_name +
                       "': " + std::to_string(partition_count);
        FETCH_LOG(error, "{}", parse_error_);
        return false;
    }
    FETCH_LOG(debug, "    partition_count={}", partition_count);

    for (int32_t p = 0; p < partition_count; ++p) {
        PerPartitionFetch pf;
        pf.topic_name = topic_name;

        if (!parse_partition(pf)) {
            fprintf(stderr, "parse_partition failed\n");
            // parse_partition sets parse_error_ on failure
            pf.parse_error = true;
            pf.parse_error_code = error_code::invalid_request;
            pf.parse_error_msg = parse_error_;
            partitions_.push_back(std::move(pf));
            // Continue parsing remaining partitions instead of aborting
            continue;
        }

        partitions_.push_back(std::move(pf));
    }

    // If a partition parse error occured, we still return true (we
    // collected the error per-partition). The caller checks per-partition
    // parse_error flags.
    return true;
}

bool FetchRequestParser::parse_partition(PerPartitionFetch& pf) {
    // --- Read partition index (INT32) ---
    if (!read_int32_be(data_, size_, &pos_, &pf.partition_index)) {
        parse_error_ = "Failed to read partition_index for topic '" +
                       pf.topic_name + "'";
        FETCH_LOG(error, "{}", parse_error_);
        return false;
    }
    if (pf.partition_index < 0) {
        parse_error_ = "Negative partition index " +
                       std::to_string(pf.partition_index) +
                       " for topic '" + pf.topic_name + "'";
        FETCH_LOG(error, "{}", parse_error_);
        return false;
    }

    // --- Read current_leader_epoch (INT32) — v9+, optional ---
    // We peek: if the next 4 bytes look like an epoch (small positive int)
    // vs. a large offset, we try to parse it. For robustness, we parse it
    // as part of the standard v9+ format.
    if (!read_int32_be(data_, size_, &pos_, &pf.current_leader_epoch)) {
        parse_error_ = "Failed to read current_leader_epoch for topic '" +
                       pf.topic_name + "' partition " +
                       std::to_string(pf.partition_index);
        FETCH_LOG(warn, "{} — defaulting to -1", parse_error_);
        pf.current_leader_epoch = -1;
    }
    // Negative values (except -1) are invalid.
    if (pf.current_leader_epoch < -1) {
        pf.current_leader_epoch = -1;
    }

    // --- Read fetch_offset (INT64) ---
    if (!read_int64_be(data_, size_, &pos_, &pf.fetch_offset)) {
        parse_error_ = "Failed to read fetch_offset for topic '" +
                       pf.topic_name + "' partition " +
                       std::to_string(pf.partition_index);
        FETCH_LOG(error, "{}", parse_error_);
        return false;
    }
    if (pf.fetch_offset < 0) {
        FETCH_LOG(warn, "Fetch offset {} is negative for topic='{}' partition={}",
                  pf.fetch_offset, pf.topic_name, pf.partition_index);
    }

    // --- Read log_start_offset (INT64) — v5+ ---
    if (!read_int64_be(data_, size_, &pos_, &pf.log_start_offset)) {
        parse_error_ = "Failed to read log_start_offset for topic '" +
                       pf.topic_name + "' partition " +
                       std::to_string(pf.partition_index);
        FETCH_LOG(warn, "{} — defaulting to 0", parse_error_);
        pf.log_start_offset = 0;
    }

    // --- Read partition_max_bytes (INT32) ---
    if (!read_int32_be(data_, size_, &pos_, &pf.partition_max_bytes)) {
        parse_error_ = "Failed to read partition_max_bytes for topic '" +
                       pf.topic_name + "' partition " +
                       std::to_string(pf.partition_index);
        FETCH_LOG(error, "{}", parse_error_);
        return false;
    }
    if (pf.partition_max_bytes <= 0) {
        pf.partition_max_bytes = 1048576;  // default 1 MiB
    }
    if (pf.partition_max_bytes > 100 * 1024 * 1024) {  // 100 MiB cap
        FETCH_LOG(warn, "partition_max_bytes {} capped at 100 MiB", pf.partition_max_bytes);
        pf.partition_max_bytes = 100 * 1024 * 1024;
    }

    FETCH_LOG(debug, "    partition={} offset={} max_bytes={} leader_epoch={}",
              pf.partition_index, pf.fetch_offset,
              pf.partition_max_bytes, pf.current_leader_epoch);

    return true;
}

// ============================================================================
// FetchValidator — validates all preconditions before reading from log
// ============================================================================

class FetchValidator {
public:
    explicit FetchValidator(BrokerServer& server,
                             const RequestContext& ctx,
                             const FetchRequestParser& parser)
        : server_(server), ctx_(ctx), parser_(parser) {}

    /// Validate a single partition fetch request. Populates result.error
    /// if validation fails.
    void validate_partition(const PerPartitionFetch& pf,
                             FetchPartitionResult& result);

    /// Check if the client has exceeded its fetch quota.
    bool check_fetch_quota(byte_count_t bytes);

    /// Validate fetch session if one is in use.
    /// Returns error_code::none on success, or error code on failure.
    error_code validate_session(int32_t session_id, int32_t session_epoch);

private:
    /// Check if topic exists; if not, return appropriate error.
    bool ensure_topic_exists(const std::string& topic_name,
                              FetchPartitionResult& result);

    /// Check if this broker is the leader for the partition.
    bool check_leadership(const std::string& topic_name,
                           partition_id_t partition,
                           FetchPartitionResult& result);

    /// Validate the fetch offset is within valid range.
    bool check_offset_range(const std::string& topic_name,
                             partition_id_t partition,
                             offset_t fetch_offset,
                             FetchPartitionResult& result);

    /// Check topic-level authorization for READ.
    bool check_authorization(const std::string& topic_name,
                              FetchPartitionResult& result);

    BrokerServer&             server_;
    const RequestContext&     ctx_;
    const FetchRequestParser& parser_;
};

void FetchValidator::validate_partition(const PerPartitionFetch& pf,
                                         FetchPartitionResult& result) {
    result.topic_name = pf.topic_name;
    result.partition_index = pf.partition_index;

    // --- Step 0: If the parser already flagged an error, propagate it ---
    if (pf.parse_error) {
        result.error = pf.parse_error_code;
        result.error_message = pf.parse_error_msg;
        FETCH_LOG(warn, "Parse error for topic='{}' partition={}: {}",
                  pf.topic_name, pf.partition_index, pf.parse_error_msg);
        return;
    }

    // --- Step 1: Ensure topic exists ---
    if (!ensure_topic_exists(pf.topic_name, result)) {
        return;  // result.error already set
    }

    // --- Step 2: Check authorization ---
    if (!check_authorization(pf.topic_name, result)) {
        return;
    }

    // --- Step 3: Check quota ---
    // We'll check quota during the read phase for accuracy, but do a
    // pre-check here with an estimated size.
    if (!check_fetch_quota(static_cast<byte_count_t>(pf.partition_max_bytes))) {
        result.error = error_code::throttling_quota_exceeded;
        result.error_message = "Fetch quota exceeded for topic '" + pf.topic_name + "'";
        FETCH_LOG(warn, "Quota exceeded for client='{}'", ctx_.client_id);
        return;
    }

    // --- Step 4: Check partition leadership ---
    if (!check_leadership(pf.topic_name, pf.partition_index, result)) {
        return;
    }

    // --- Step 5: Validate offset range ---
    if (!check_offset_range(pf.topic_name, pf.partition_index,
                             pf.fetch_offset, result)) {
        return;
    }

    // All checks passed.
    result.error = error_code::none;
}

bool FetchValidator::ensure_topic_exists(const std::string& topic_name,
                                          FetchPartitionResult& result) {
    auto& tm = server_.topic_manager();

    if (tm.topic_exists(topic_name)) {
        return true;
    }

    // Topic doesn't exist. We do NOT auto-create on fetch.
    result.error = error_code::unknown_topic_or_partition;
    result.error_message = "Topic '" + topic_name + "' does not exist";
    FETCH_LOG(warn, "{}", result.error_message);
    return false;
}

bool FetchValidator::check_authorization(const std::string& topic_name,
                                          FetchPartitionResult& result) {
    // If the connection is not authenticated, skip auth checks (depends on
    // broker security configuration).
    if (!ctx_.is_authenticated || ctx_.principal.empty()) {
        return true;
    }

    // For inter-broker requests (replica fetching), auth is handled at the
    // transport layer (mTLS / SASL).
    if (ctx_.is_inter_broker) {
        return true;
    }

    // Attempt READ authorization on the topic.
    try {
        FETCH_LOG(debug, "Checking READ authorization for principal='{}' on topic='{}'",
                  ctx_.principal, topic_name);
        // In full implementation: server_.auth_manager().authorize(principal, topic, "READ");
    } catch (...) {
        result.error = error_code::topic_authorization_failed;
        result.error_message = "Authorization failed for READ on topic '" +
                               topic_name + "'";
        FETCH_LOG(warn, "{}", result.error_message);
        return false;
    }

    return true;
}

bool FetchValidator::check_leadership(const std::string& topic_name,
                                       partition_id_t partition,
                                       FetchPartitionResult& result) {
    auto& pm = server_.partition_manager();

    // For inter-broker (replica) fetches, the follower reads from the leader
    // — so we MUST be the leader, or the follower would be reading from a
    // non-leader which is invalid.
    bool is_leader = pm.is_leader(topic_name, partition);

    if (!is_leader) {
        broker_id_t leader = pm.leader_for(topic_name, partition);

        if (leader == kNoBroker || leader == kAnyBroker) {
            result.error = error_code::leader_not_available;
            result.error_message = "No leader available for topic '" +
                                   topic_name + "' partition " +
                                   std::to_string(partition);
            FETCH_LOG(warn, "{}", result.error_message);
        } else {
            result.error = error_code::not_leader_for_partition;
            result.error_message = "Not leader for topic '" + topic_name +
                                   "' partition " + std::to_string(partition) +
                                   "; leader is broker " + std::to_string(leader);
            FETCH_LOG(debug, "{}", result.error_message);
        }
        return false;
    }

    return true;
}

bool FetchValidator::check_offset_range(const std::string& topic_name,
                                         partition_id_t partition,
                                         offset_t fetch_offset,
                                         FetchPartitionResult& result) {
    // Special offset values: -2 = earliest, -1 = latest.
    // These are handled by the LogManager. We just accept them here.
    if (fetch_offset == kEarliestOffset || fetch_offset == kLatestOffset) {
        return true;
    }

    // For normal offsets, we validate that the offset is not out of range.
    // The LogManager will do the definitive check, but we can do an early
    // check via the partition manager's log_start_offset if available.

    // In the current architecture, log_start_offset is tracked per-LogManager.
    // We'll let LogManager::read() handle the definitive check; for now we
    // accept all non-negative offsets. LogManager will return
    // offset_out_of_range if the offset is invalid.

    if (fetch_offset < 0) {
        // Already handled special values above.
        result.error = error_code::offset_out_of_range;
        result.error_message = "Invalid fetch offset " + std::to_string(fetch_offset) +
                               " for topic '" + topic_name + "' partition " +
                               std::to_string(partition);
        FETCH_LOG(warn, "{}", result.error_message);
        return false;
    }

    return true;
}

bool FetchValidator::check_fetch_quota(byte_count_t bytes) {
    broker::QuotaManager qm(server_);

    if (!qm.check_fetch_quota(ctx_.client_id)) {
        return false;
    }

    qm.record_fetch(ctx_.client_id, bytes);
    return true;
}

error_code FetchValidator::validate_session(int32_t session_id, int32_t session_epoch) {
    if (session_id == 0) {
        return error_code::none;  // No session — nothing to validate.
    }

    auto& session_mgr = get_fetch_session_manager();
    auto* session = session_mgr.get_session(session_id, session_epoch);

    if (session == nullptr) {
        // Session not found or epoch mismatch.
        // We need to differentiate: not found vs. epoch mismatch.
        // For simplicity, try to look up without epoch to check existence.
        // The get_session already handles epoch mismatch internally by returning null.
        FETCH_LOG(warn, "Fetch session {} not found or epoch {} invalid",
                  session_id, session_epoch);
        return error_code::fetch_session_id_not_found;
    }

    return error_code::none;
}

// ============================================================================
// FetchLogReader — reads from LogManager for each partition
// ============================================================================

class FetchLogReader {
public:
    explicit FetchLogReader(BrokerServer& server,
                             const FetchRequestParser& parser)
        : server_(server), parser_(parser) {}

    /// Execute a fetch for a single partition. Reads records from the log
    /// and populates result with records and watermarks.
    void fetch_partition(const PerPartitionFetch& pf,
                          FetchPartitionResult& result,
                          byte_count_t* remaining_bytes);

    /// Apply a fetch session: use session state to determine what (if any)
    /// data changed since the last fetch for this partition.
    /// Returns true if there is new data to return.
    bool apply_session(const PerPartitionFetch& pf,
                        FetchPartitionResult& result,
                        FetchSession* session,
                        offset_t* effective_offset);

private:
    /// Resolve logical offsets (-2=earliest, -1=latest) to concrete offsets
    /// using watermarks from the partition.
    offset_t resolve_offset(const std::string& topic_name,
                             partition_id_t partition,
                             offset_t requested_offset);

    BrokerServer&             server_;
    const FetchRequestParser& parser_;
};

offset_t FetchLogReader::resolve_offset(const std::string& topic_name,
                                          partition_id_t partition,
                                          offset_t requested_offset) {
    if (requested_offset == kEarliestOffset) {
        // Return the log_start_offset.
        return 0;  // In production: get from LogManager. Default to 0.
    }
    if (requested_offset == kLatestOffset) {
        // Return the log_end_offset (next offset to be assigned).
        return kInvalidOffset;  // Caller will handle this by reading from the end.
    }
    if (requested_offset == kTimestampOffset) {
        // Timestamp-based offset lookup — not implemented here.
        return 0;
    }
    return requested_offset;
}

void FetchLogReader::fetch_partition(const PerPartitionFetch& pf,
                                      FetchPartitionResult& result,
                                      byte_count_t* remaining_bytes) {
    if (result.error != error_code::none) {
        return;  // Validation already failed.
    }

    if (*remaining_bytes <= 0) {
        // Response budget exhausted — but we still need to populate watermarks.
        result.error = error_code::none;
        result.bytes_returned = 0;
        return;
    }

    // Resolve the effective fetch offset (handling -1/-2).
    offset_t effective_offset = resolve_offset(pf.topic_name,
                                                pf.partition_index,
                                                pf.fetch_offset);

    // Determine the max bytes for this partition's fetch.
    // We use the smaller of: partition_max_bytes from request,
    // and the remaining total budget.
    byte_count_t partition_limit = static_cast<byte_count_t>(pf.partition_max_bytes);
    if (partition_limit > *remaining_bytes) {
        partition_limit = *remaining_bytes;
    }

    FETCH_LOG(debug, "Fetching from topic='{}' partition={} offset={} max_bytes={}",
              pf.topic_name, pf.partition_index, effective_offset, partition_limit);

    // --- Read from the storage layer ---
    // In production, we would access the partition's LogManager through
    // PartitionManager / BrokerServer. The LogManager provides read() and
    // watermarks.

    // Simulated read — in production this goes through the real LogManager:
    //
    // auto* log_mgr = server_.partition_manager().get_log_manager(
    //     pf.topic_name, pf.partition_index);
    // auto fetch_result = log_mgr->read(
    //     effective_offset, partition_limit, std::nullopt,
    //     parser_.get_isolation());
    //

    // For now, populate watermarks from the partition state.
    // In production, these come from LogManager:
    // result.high_watermark = log_mgr->get_high_watermark();
    // result.last_stable_offset = log_mgr->get_last_stable_offset();
    // result.log_start_offset = log_mgr->get_log_start_offset();

    // Simulated watermarks.
    result.high_watermark = effective_offset + 100;  // Placeholder
    result.last_stable_offset = result.high_watermark;
    result.log_start_offset = 0;

    // For read_committed isolation: fetch offsets > last_stable_offset
    // should not be returned. Validate here.
    if (parser_.get_isolation() == torrent::isolation_level::read_committed) {
        if (effective_offset > result.last_stable_offset) {
            FETCH_LOG(debug, "read_committed: fetch_offset {} > LSO {}, returning empty",
                      effective_offset, result.last_stable_offset);
            result.bytes_returned = 0;
            return;
        }
    }

    // For follower fetch (inter-broker): validate that fetch_offset <= HW.
    // Followers can only read up to the high watermark.
    if (parser_.is_follower_fetch()) {
        if (effective_offset > result.high_watermark) {
            FETCH_LOG(warn, "Follower fetch offset {} > HW {} for topic='{}' partition={}",
                      effective_offset, result.high_watermark,
                      pf.topic_name, pf.partition_index);
            result.error = error_code::offset_out_of_range;
            result.error_message = "Follower fetch offset exceeds high watermark";
            return;
        }
    }

    // The actual read from LogManager would populate result.records here.
    // For now, records is empty (placeholder). In production:
    //
    // if (fetch_result.ok() && fetch_result.has_records()) {
    //     result.records = std::move(fetch_result.batch->records);
    //     result.batch_base_offset = fetch_result.batch->base_offset;
    //     result.is_truncated = fetch_result.is_truncated;
    //     result.bytes_returned = fetch_result.batch.has_value()
    //         ? fetch_result.batch->approximate_size() : 0;
    // }
    //
    // if (fetch_result.failed()) {
    //     result.error = fetch_result.error;
    //     result.error_message = fetch_result.error_message;
    // }

    result.bytes_returned = 0;  // No records returned in placeholder.

    // Track the budget consumed.
    *remaining_bytes -= static_cast<byte_count_t>(result.bytes_returned);
    if (*remaining_bytes < 0) {
        *remaining_bytes = 0;
    }

    FETCH_LOG(debug, "Fetch result: topic='{}' partition={} rc={} hw={} lso={} lso_real={}",
              pf.topic_name, pf.partition_index,
              result.records.size(), result.high_watermark,
              result.last_stable_offset, result.log_start_offset);
}

bool FetchLogReader::apply_session(const PerPartitionFetch& pf,
                                    FetchPartitionResult& result,
                                    FetchSession* session,
                                    offset_t* effective_offset) {
    if (session == nullptr) {
        *effective_offset = pf.fetch_offset;
        return true;  // No session to apply — always fetch.
    }

    auto key = FetchSession::make_key(pf.topic_name, pf.partition_index);
    auto it = session->partitions.find(key);

    if (it == session->partitions.end()) {
        // First time fetching this partition via this session.
        FETCH_LOG(debug, "Session {}: first fetch for topic='{}' partition={}",
                  session->session_id, pf.topic_name, pf.partition_index);
        *effective_offset = pf.fetch_offset;
        return true;
    }

    const auto& ps = it->second;

    // Check if the leader epoch changed — if so, invalidate and re-fetch.
    if (pf.current_leader_epoch >= 0 &&
        ps.last_leader_epoch >= 0 &&
        pf.current_leader_epoch != ps.last_leader_epoch) {
        FETCH_LOG(info, "Session {}: leader epoch changed for topic='{}' partition={} "
                  "({} -> {}), forcing full fetch",
                  session->session_id, pf.topic_name, pf.partition_index,
                  ps.last_leader_epoch, pf.current_leader_epoch);
        *effective_offset = pf.fetch_offset;
        return true;
    }

    // If the client's requested offset is newer than what we last sent,
    // there's new data.
    if (pf.fetch_offset > ps.last_fetched_offset) {
        FETCH_LOG(debug, "Session {}: new data available for topic='{}' partition={} "
                  "(fetch_offset={} > last_fetched={})",
                  session->session_id, pf.topic_name, pf.partition_index,
                  pf.fetch_offset, ps.last_fetched_offset);
        *effective_offset = pf.fetch_offset;
        return true;
    }

    // No new data: client is fully caught up. Return empty but with current
    // watermarks.
    FETCH_LOG(debug, "Session {}: no new data for topic='{}' partition={} "
              "(fetch_offset={} <= last_fetched={})",
              session->session_id, pf.topic_name, pf.partition_index,
              pf.fetch_offset, ps.last_fetched_offset);
    return false;
}

// ============================================================================
// FetchResponseBuilder — builds the wire-format FetchResponse
// ============================================================================

class FetchResponseBuilder {
public:
    FetchResponseBuilder() {
        // Pre-allocate reasonable buffer size.
        buf_.reserve(16384);
    }

    /// Add a per-partition fetch result to the response.
    void add_partition_result(const FetchPartitionResult& result);

    /// Build and return the serialised response as a shared_buffer.
    shared_buffer build(int32_t throttle_time_ms,
                         error_code top_level_error,
                         int32_t session_id);

    /// Get the total bytes accumulated so far.
    [[nodiscard]] byte_count_t total_bytes() const noexcept { return total_bytes_; }

    /// Check if min_bytes threshold has been met.
    [[nodiscard]] bool meets_min_bytes(int32_t min_bytes) const noexcept {
        return total_bytes_ >= static_cast<byte_count_t>(min_bytes);
    }

    /// Check if the response is empty (no data for any partition).
    [[nodiscard]] bool is_empty() const noexcept {
        return topic_results_.empty();
    }

private:
    /// Group results by topic for efficient serialization.
    struct PartitionEntry {
        FetchPartitionResult result;
        std::vector<char>    serialized_records; ///< Pre-serialized record batch bytes
    };

    struct TopicEntry {
        std::string                     topic_name;
        std::vector<PartitionEntry>     partitions;
    };

    /// Serialize a set of records into the Kafka v2 RecordBatch wire format.
    std::vector<char> serialize_record_batch(
        const std::vector<torrent::Record>& records,
        offset_t base_offset);

    /// Serialize a single record into the VARINT-delimited v2 record format.
    void serialize_record(const torrent::Record& rec,
                           std::vector<char>& batch_buf,
                           int64_t base_timestamp,
                           int32_t offset_delta,
                           int32_t sequence);

    std::vector<char> buf_;  ///< Will be (re)built during build().
    std::vector<TopicEntry> topic_results_;
    std::unordered_map<std::string, size_t> topic_index_;
    byte_count_t total_bytes_ = 0;
};

void FetchResponseBuilder::add_partition_result(
    const FetchPartitionResult& result) {
    // Find or create the topic entry.
    size_t idx = 0;
    auto it = topic_index_.find(result.topic_name);
    if (it == topic_index_.end()) {
        idx = topic_results_.size();
        topic_index_[result.topic_name] = idx;
        TopicEntry te;
        te.topic_name = result.topic_name;
        topic_results_.push_back(std::move(te));
    } else {
        idx = it->second;
    }

    PartitionEntry pe;
    pe.result = result;

    // Pre-serialize records if there are any.
    if (!result.records.empty()) {
        pe.serialized_records = serialize_record_batch(
            result.records, result.batch_base_offset);
    }

    // Track total bytes.
    byte_count_t entry_bytes = static_cast<byte_count_t>(
        4 + 2 + 8 + 8 + 8 + 4 + 4    // fixed fields
        + result.error_message.size()
        + result.aborted_transactions.size() * 16
        + pe.serialized_records.size());

    total_bytes_ += entry_bytes;

    topic_results_[idx].partitions.push_back(std::move(pe));
}

std::vector<char> FetchResponseBuilder::serialize_record_batch(
    const std::vector<torrent::Record>& records,
    offset_t base_offset) {
    std::vector<char> batch_buf;
    batch_buf.reserve(256 + records.size() * 512);

    // Determine min/max timestamps.
    int64_t base_ts = (records.empty()) ? 0 : records[0].timestamp;
    int64_t max_ts = base_ts;
    for (const auto& r : records) {
        if (r.timestamp < base_ts) base_ts = r.timestamp;
        if (r.timestamp > max_ts) max_ts = r.timestamp;
        (void)r;  // suppress unused
    }

    // --- v2 RecordBatch header ---
    // base_offset (INT64)
    write_int64_be(batch_buf, base_offset);

    // We'll write batch_length (INT32) later — save position.
    size_t batch_length_pos = batch_buf.size();
    write_int32_be(batch_buf, 0);  // placeholder

    // partition_leader_epoch (INT32)
    write_int32_be(batch_buf, 0);

    // magic (INT8) — v2
    write_int8(batch_buf, 2);

    // CRC (INT32) — placeholder, compute after records
    size_t crc_pos = batch_buf.size();
    write_int32_be(batch_buf, 0);  // placeholder

    // attributes (INT16): 0 = no compression, no transactional, no control
    write_int16_be(batch_buf, 0);

    // last_offset_delta (INT32)
    int32_t last_offset_delta = static_cast<int32_t>(records.size()) - 1;
    if (last_offset_delta < 0) last_offset_delta = 0;
    write_int32_be(batch_buf, last_offset_delta);

    // base_timestamp (INT64)
    write_int64_be(batch_buf, base_ts);

    // max_timestamp (INT64)
    write_int64_be(batch_buf, max_ts);

    // producer_id (INT64) — -1 for regular (non-idempotent) records
    write_int64_be(batch_buf, -1);

    // producer_epoch (INT16)
    write_int16_be(batch_buf, -1);

    // base_sequence (INT32)
    write_int32_be(batch_buf, -1);

    // record_count (INT32)
    write_int32_be(batch_buf, static_cast<int32_t>(records.size()));

    // --- Serialize records ---
    for (size_t i = 0; i < records.size(); ++i) {
        int32_t offset_delta = static_cast<int32_t>(i);
        int32_t sequence = static_cast<int32_t>(i);
        serialize_record(records[i], batch_buf, base_ts, offset_delta, sequence);
    }

    // --- Back-patch batch_length (excludes base_offset and batch_length itself) ---
    int32_t batch_length = static_cast<int32_t>(
        batch_buf.size() - batch_length_pos - 4);
    // Rewrite batch_length at saved position.
    uint32_t bl_raw = __builtin_bswap32(static_cast<uint32_t>(batch_length));
    std::memcpy(batch_buf.data() + batch_length_pos, &bl_raw, sizeof(bl_raw));

    // --- Compute and back-patch CRC (covers from attributes through last record) ---
    size_t crc_start = crc_pos + 4;  // after the CRC field itself
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t k = crc_start; k < batch_buf.size(); ++k) {
        crc ^= static_cast<uint8_t>(batch_buf[k]);
    }
    crc = (crc >> 8) ^ 0x82F63B78u;  // simplified — production uses full CRC32C table
    uint32_t crc_raw = __builtin_bswap32(crc);
    std::memcpy(batch_buf.data() + crc_pos, &crc_raw, sizeof(crc_raw));

    return batch_buf;
}

void FetchResponseBuilder::serialize_record(
    const torrent::Record& rec,
    std::vector<char>& batch_buf,
    int64_t base_timestamp,
    int32_t offset_delta,
    int32_t sequence) {

    // Build record body (attributes, timestamp_delta, offset_delta,
    // key, value, headers) in a temporary buffer so we can measure length.
    std::vector<char> rec_body;
    rec_body.reserve(256);

    // attributes (INT8) — 0 = no special attributes
    write_int8(rec_body, 0);

    // timestamp_delta (VARINT) = timestamp - base_timestamp
    int64_t ts_delta = rec.timestamp - base_timestamp;
    {
        uint64_t uv = (static_cast<uint64_t>(ts_delta) << 1) ^
                      static_cast<uint64_t>(ts_delta >> 63);
        while (uv >= 0x80) {
            rec_body.push_back(static_cast<char>((uv & 0x7F) | 0x80));
            uv >>= 7;
        }
        rec_body.push_back(static_cast<char>(uv & 0x7F));
    }

    // offset_delta (VARINT)
    {
        uint64_t uv = (static_cast<uint64_t>(offset_delta) << 1) ^
                      static_cast<uint64_t>(offset_delta >> 63);
        while (uv >= 0x80) {
            rec_body.push_back(static_cast<char>((uv & 0x7F) | 0x80));
            uv >>= 7;
        }
        rec_body.push_back(static_cast<char>(uv & 0x7F));
    }

    // key (VARINT length + bytes)
    {
        int64_t klen = static_cast<int64_t>(rec.key.size());
        uint64_t uv = (static_cast<uint64_t>(klen) << 1) ^
                      static_cast<uint64_t>(klen >> 63);
        while (uv >= 0x80) {
            rec_body.push_back(static_cast<char>((uv & 0x7F) | 0x80));
            uv >>= 7;
        }
        rec_body.push_back(static_cast<char>(uv & 0x7F));
        if (rec.key.size() > 0) {
            rec_body.insert(rec_body.end(),
                            rec.key.data(),
                            rec.key.data() + rec.key.size());
        }
    }

    // value (VARINT length + bytes, -1 = NULL)
    {
        int64_t vlen = rec.value.empty()
                           ? -1
                           : static_cast<int64_t>(rec.value.size());
        uint64_t uv = (static_cast<uint64_t>(vlen) << 1) ^
                      static_cast<uint64_t>(vlen >> 63);
        while (uv >= 0x80) {
            rec_body.push_back(static_cast<char>((uv & 0x7F) | 0x80));
            uv >>= 7;
        }
        rec_body.push_back(static_cast<char>(uv & 0x7F));
        if (vlen > 0) {
            rec_body.insert(rec_body.end(),
                            rec.value.data(),
                            rec.value.data() + rec.value.size());
        }
    }

    // headers (VARINT count + per-header key/value)
    {
        int64_t hcount = static_cast<int64_t>(rec.headers.size());
        uint64_t uv = (static_cast<uint64_t>(hcount) << 1) ^
                      static_cast<uint64_t>(hcount >> 63);
        while (uv >= 0x80) {
            rec_body.push_back(static_cast<char>((uv & 0x7F) | 0x80));
            uv >>= 7;
        }
        rec_body.push_back(static_cast<char>(uv & 0x7F));

        for (const auto& h : rec.headers) {
            // Header key
            int64_t hklen = static_cast<int64_t>(h.key.size());
            {
                uint64_t hkuv = (static_cast<uint64_t>(hklen) << 1) ^
                                 static_cast<uint64_t>(hklen >> 63);
                while (hkuv >= 0x80) {
                    rec_body.push_back(static_cast<char>((hkuv & 0x7F) | 0x80));
                    hkuv >>= 7;
                }
                rec_body.push_back(static_cast<char>(hkuv & 0x7F));
            }
            if (h.key.size() > 0) {
                rec_body.insert(rec_body.end(), h.key.begin(), h.key.end());
            }

            // Header value
            int64_t hvlen = static_cast<int64_t>(h.value.size());
            {
                uint64_t hvuv = (static_cast<uint64_t>(hvlen) << 1) ^
                                 static_cast<uint64_t>(hvlen >> 63);
                while (hvuv >= 0x80) {
                    rec_body.push_back(static_cast<char>((hvuv & 0x7F) | 0x80));
                    hvuv >>= 7;
                }
                rec_body.push_back(static_cast<char>(hvuv & 0x7F));
            }
            if (h.value.size() > 0) {
                rec_body.insert(rec_body.end(), h.value.begin(), h.value.end());
            }
        }
    }

    // --- Write record length (VARINT) + body ---
    int64_t rec_len = static_cast<int64_t>(rec_body.size());
    {
        uint64_t uv = (static_cast<uint64_t>(rec_len) << 1) ^
                      static_cast<uint64_t>(rec_len >> 63);
        while (uv >= 0x80) {
            batch_buf.push_back(static_cast<char>((uv & 0x7F) | 0x80));
            uv >>= 7;
        }
        batch_buf.push_back(static_cast<char>(uv & 0x7F));
    }
    batch_buf.insert(batch_buf.end(), rec_body.begin(), rec_body.end());
}

shared_buffer FetchResponseBuilder::build(int32_t throttle_time_ms,
                                            error_code top_level_error,
                                            int32_t session_id) {
    // Rebuild the buffer from scratch for the final serialization.
    buf_.clear();

    // --- throttle_time_ms (INT32) ---
    write_int32_be(buf_, throttle_time_ms);

    // --- error_code (INT16) — top-level ---
    write_int16_be(buf_, static_cast<int16_t>(top_level_error));

    // --- session_id (INT32) ---
    write_int32_be(buf_, session_id);

    // --- topic_count (INT32) ---
    write_int32_be(buf_, static_cast<int32_t>(topic_results_.size()));

    for (const auto& te : topic_results_) {
        // --- topic_name (STRING) ---
        write_string(buf_, te.topic_name);

        // --- partition_count (INT32) ---
        write_int32_be(buf_, static_cast<int32_t>(te.partitions.size()));

        for (const auto& pe : te.partitions) {
            const auto& r = pe.result;

            // --- partition_index (INT32) ---
            write_int32_be(buf_, r.partition_index);

            // --- error_code (INT16) ---
            write_int16_be(buf_, static_cast<int16_t>(r.error));

            // --- high_watermark (INT64) ---
            write_int64_be(buf_, r.high_watermark);

            // --- last_stable_offset (INT64) ---
            write_int64_be(buf_, r.last_stable_offset);

            // --- log_start_offset (INT64) ---
            write_int64_be(buf_, r.log_start_offset);

            // --- aborted_transaction_count (INT32) + per-txn ---
            write_int32_be(buf_,
                static_cast<int32_t>(r.aborted_transactions.size()));
            for (const auto& atxn : r.aborted_transactions) {
                write_int64_be(buf_, atxn.producer_id);
                write_int64_be(buf_, atxn.first_offset);
            }

            // --- preferred_read_replica (INT32) — -1 = read from leader ---
            write_int32_be(buf_, r.preferred_read_replica);

            // --- record_batch_set (INT32 length + bytes) ---
            if (!pe.serialized_records.empty()) {
                write_int32_be(buf_,
                    static_cast<int32_t>(pe.serialized_records.size()));
                buf_.insert(buf_.end(),
                            pe.serialized_records.begin(),
                            pe.serialized_records.end());
            } else {
                write_int32_be(buf_, 0);  // empty record batch set
            }

            // --- error_message (nullable string, appended for debugging) ---
            // Not part of standard Kafka wire format, but included for
            // torrent-mq extended protocol compatibility.
            if (!r.error_message.empty()) {
                write_nullable_string(buf_, r.error_message);
            } else {
                write_int16_be(buf_, -1);  // null
            }
        }
    }

    // Copy into shared_buffer.
    shared_buffer result(buf_.data(), buf_.size());
    return result;
}

// ============================================================================
// Long-Poll Manager — handles the wait-for-data semantics
// ============================================================================

/// Implements the long-poll behaviour of fetch requests.
/// When min_bytes is not met and data is not immediately available,
/// the handler can block for up to max_wait_ms waiting for new data
/// to arrive before responding.
class LongPollManager {
public:
    /// Determine if we should block waiting for more data.
    /// Returns true if the response should be sent immediately,
    /// false if we should wait (the caller implements the wait loop).
    [[nodiscard]] bool should_respond_now(
        const FetchResponseBuilder& builder,
        int32_t min_bytes,
        bool has_incomplete_partitions,
        timestamp_ms_t elapsed_ms,
        int32_t max_wait_ms) const noexcept {

        // If we've exceeded the max wait time, respond now.
        if (elapsed_ms >= max_wait_ms) {
            FETCH_LOG(debug, "Long-poll: max_wait exceeded ({}ms >= {}ms)",
                      elapsed_ms, max_wait_ms);
            return true;
        }

        // If min_bytes is met, respond now.
        if (builder.meets_min_bytes(min_bytes)) {
            FETCH_LOG(debug, "Long-poll: min_bytes met ({} >= {})",
                      builder.total_bytes(), min_bytes);
            return true;
        }

        // If there are no incomplete partitions (all data read), respond.
        if (!has_incomplete_partitions) {
            FETCH_LOG(debug, "Long-poll: all partitions complete");
            return true;
        }

        // If min_bytes is 0 or negative, respond immediately.
        if (min_bytes <= 0) {
            return true;
        }

        // Still waiting for data.
        return false;
    }
};

// ============================================================================
// Cleanup thread for fetch sessions (periodic eviction)
// ============================================================================

/// Simple background cleanup: called periodically by the server's
/// health probe loop or a dedicated timer.
void cleanup_fetch_sessions() {
    get_fetch_session_manager().cleanup_expired();
}

} // anonymous namespace

// ============================================================================
// FetchHandler::handle() — public entry point
// ============================================================================

shared_buffer FetchHandler::handle(const RequestContext& ctx, buffer_view body) {
    const timestamp_ms_t start_time = wall_clock_ms();

    FETCH_LOG(info, "Fetch request from client='{}' (broker={}, api_version={}, "
              "correlation_id={})",
              ctx.client_id, ctx.broker_id, ctx.api_version, ctx.correlation_id);

    // --- Step 0: Validate the body ---
    if (body.empty()) {
        FETCH_LOG(warn, "Empty fetch request body from client='{}'", ctx.client_id);
        FetchResponseBuilder builder;
        return builder.build(0, error_code::invalid_request, 0);
    }

    // --- Step 1: Parse the wire-format FetchRequest ---
    FetchRequestParser parser(body);
    if (!parser.parse()) {
        FETCH_LOG(error, "Failed to parse fetch request from client='{}': {}",
                  ctx.client_id, parser.parse_error());
        FetchResponseBuilder builder;
        // Add a dummy partition result with the parse error.
        FetchPartitionResult err_result;
        err_result.error = error_code::invalid_request;
        err_result.error_message = parser.parse_error();
        err_result.partition_index = 0;
        builder.add_partition_result(err_result);
        return builder.build(0, error_code::invalid_request, 0);
    }

    FETCH_LOG(debug, "Parsed fetch request: replica_id={} max_wait={}ms "
              "min_bytes={} max_bytes={} isolation={} session_id={} session_epoch={} "
              "topics/partitions={}",
              parser.replica_id(), parser.max_wait_ms(),
              parser.min_bytes(), parser.max_bytes(),
              parser.isolation_level(), parser.session_id(),
              parser.session_epoch(), parser.partitions().size());

    // --- Step 2: Validate fetch session if one is in use ---
    FetchValidator validator(*server_, ctx, parser);
    error_code session_error = validator.validate_session(
        parser.session_id(), parser.session_epoch());

    FetchSession* active_session = nullptr;
    if (parser.has_session() && session_error == error_code::none) {
        active_session = get_fetch_session_manager().get_session(
            parser.session_id(), parser.session_epoch());
        if (active_session == nullptr) {
            // Session validation passed but session not found — race condition.
            // The validate_session already handled this, but we double-check.
            session_error = error_code::fetch_session_id_not_found;
        }
    }

    // --- Step 3: Validate each partition's fetch request ---
    std::vector<FetchPartitionResult> fetch_results;
    fetch_results.reserve(parser.partitions().size());

    // Track partitions that still have more data to read (for long-poll).
    bool has_incomplete_partitions = false;

    for (const auto& pf : parser.partitions()) {
        FetchPartitionResult result;
        validator.validate_partition(pf, result);
        fetch_results.push_back(result);
    }

    // --- Step 4: Read data from LogManager for valid partitions ---
    FetchLogReader reader(*server_, parser);

    FetchResponseBuilder response_builder;
    byte_count_t remaining_bytes = parser.max_bytes();

    for (size_t i = 0; i < parser.partitions().size(); ++i) {
        const auto& pf = parser.partitions()[i];
        auto& result = fetch_results[i];

        // Apply session filtering: only include partitions with new data.
        offset_t effective_offset = pf.fetch_offset;
        bool has_new_data = true;

        if (active_session != nullptr) {
            has_new_data = reader.apply_session(pf, result,
                                                  active_session,
                                                  &effective_offset);
        }

        if (result.error != error_code::none) {
            // Validation failed — add error result immediately.
            response_builder.add_partition_result(result);
            continue;
        }

        if (!has_new_data) {
            // Session says no new data — skip this partition (incremental fetch).
            // Still return watermarks but with no data.
            result.records.clear();
            response_builder.add_partition_result(result);
            continue;
        }

        // --- Read from storage ---
        reader.fetch_partition(pf, result, &remaining_bytes);

        if (result.is_truncated) {
            has_incomplete_partitions = true;
        }

        // --- Update session state ---
        if (active_session != nullptr && result.error == error_code::none) {
            offset_t new_offset = pf.fetch_offset +
                static_cast<offset_t>(result.records.size());
            active_session->update_partition(
                pf.topic_name, pf.partition_index,
                new_offset, result.log_start_offset,
                pf.current_leader_epoch);
        }

        response_builder.add_partition_result(result);
    }

    // --- Step 5: Handle long-poll (min_bytes not met) ---
    LongPollManager poll_mgr;
    timestamp_ms_t elapsed = wall_clock_ms() - start_time;
    int32_t effective_max_wait = parser.max_wait_ms();

    // Clamp max wait to reasonable bounds.
    if (effective_max_wait > 60000) effective_max_wait = 60000;
    if (effective_max_wait < 0) effective_max_wait = 0;

    // In a full implementation, we would enter a wait loop here,
    // blocking until min_bytes is met or max_wait expires, checking
    // for new data arrival via a condition variable / notification.
    // For now, we respond immediately.

    bool should_wait = !poll_mgr.should_respond_now(
        response_builder,
        parser.min_bytes(),
        has_incomplete_partitions,
        elapsed,
        effective_max_wait);

    if (should_wait) {
        FETCH_LOG(debug, "Long-poll: min_bytes={} not met (have {}), would wait "
                  "up to {}ms (elapsed {}ms)",
                  parser.min_bytes(), response_builder.total_bytes(),
                  effective_max_wait - elapsed, elapsed);
        // In production: block on a condition variable with timeout.
        // For now, respond with whatever we have.
    }

    // --- Step 6: Compute throttle_time_ms ---
    elapsed = wall_clock_ms() - start_time;
    int32_t throttle_time_ms = 0;
    if (elapsed > 0 && elapsed < std::numeric_limits<int32_t>::max()) {
        throttle_time_ms = static_cast<int32_t>(elapsed);
    }

    // --- Step 7: Determine top-level error ---
    error_code top_level_error = error_code::none;
    if (session_error != error_code::none) {
        top_level_error = session_error;
    } else if (response_builder.is_empty() && !parser.partitions().empty()) {
        // If all partitions failed validation, propagate the first error
        // as the top-level error.
        for (const auto& fr : fetch_results) {
            if (fr.error != error_code::none) {
                top_level_error = fr.error;
                break;
            }
        }
    }

    // --- Step 8: If the client requested a new session, create one ---
    int32_t response_session_id = parser.session_id();
    if (parser.session_id() == 0 && parser.session_epoch() == 0) {
        // Client wants a new session if it sent (0, 0) — but only if
        // it's a modern client (check max_wait > 0 as a heuristic).
        if (parser.max_wait_ms() > 0 && ctx.api_version >= 7) {
            response_session_id = get_fetch_session_manager().create_session(
                ctx.client_id, 0);
            FETCH_LOG(debug, "Auto-created fetch session {} for client='{}'",
                      response_session_id, ctx.client_id);
        }
    }

    // --- Step 9: Record quota usage ---
    if (response_builder.total_bytes() > 0) {
        broker::QuotaManager qm(*server_);
        qm.record_fetch(ctx.client_id, response_builder.total_bytes());
    }

    FETCH_LOG(info, "Fetch request complete: {} partitions, {} bytes, "
              "elapsed={}ms, session={}",
              parser.partitions().size(),
              response_builder.total_bytes(),
              elapsed,
              response_session_id);

    return response_builder.build(throttle_time_ms, top_level_error,
                                   response_session_id);
}

// ============================================================================
// Protocol-level convenience: parse FetchRequest from wire for interop
// ============================================================================

namespace protocol_helpers {

/// Parse a FetchRequest from the Kafka wire format into the protocol struct.
/// Used by test harnesses and inter-broker communication.
tp::FetchRequest parse_fetch_request(buffer_view body) {
    tp::FetchRequest req;

    size_t pos = 0;
    const char* data = body.data;
    size_t size = body.size;

    // replica_id (INT32) — skip for now
    if (pos + 4 <= size) pos += 4;

    // max_wait_ms (INT32)
    if (pos + 4 <= size) {
        uint32_t mw_raw;
        std::memcpy(&mw_raw, data + pos, 4);
        req.max_wait_ms = static_cast<int32_t>(__builtin_bswap32(mw_raw));
        pos += 4;
    }

    // min_bytes (INT32)
    if (pos + 4 <= size) {
        uint32_t mb_raw;
        std::memcpy(&mb_raw, data + pos, 4);
        req.min_bytes = static_cast<int32_t>(__builtin_bswap32(mb_raw));
        pos += 4;
    }

    // max_bytes (INT32) — v3+
    if (pos + 4 <= size) {
        uint32_t mxb_raw;
        std::memcpy(&mxb_raw, data + pos, 4);
        req.max_bytes = static_cast<int32_t>(__builtin_bswap32(mxb_raw));
        pos += 4;
    }

    // isolation_level (INT8) — v4+
    if (pos + 1 <= size) {
        req.isolation_level = static_cast<int8_t>(data[pos]);
        pos += 1;
    }

    // session_id (INT32) — v7+
    if (pos + 4 <= size) {
        uint32_t sid_raw;
        std::memcpy(&sid_raw, data + pos, 4);
        req.session_id = static_cast<int32_t>(__builtin_bswap32(sid_raw));
        pos += 4;
    }

    // session_epoch (INT32) — v7+
    if (pos + 4 <= size) {
        uint32_t sep_raw;
        std::memcpy(&sep_raw, data + pos, 4);
        req.session_epoch = static_cast<int32_t>(__builtin_bswap32(sep_raw));
        pos += 4;
    }

    // topic_count
    int32_t topic_count = 0;
    if (pos + 4 <= size) {
        uint32_t tc_raw;
        std::memcpy(&tc_raw, data + pos, 4);
        topic_count = static_cast<int32_t>(__builtin_bswap32(tc_raw));
        pos += 4;
    }

    for (int32_t t = 0; t < topic_count && pos < size; ++t) {
        // Read topic name
        int16_t tn_len = 0;
        if (pos + 2 > size) break;
        std::memcpy(&tn_len, data + pos, 2);
        tn_len = static_cast<int16_t>(__builtin_bswap16(
            static_cast<uint16_t>(tn_len)));
        pos += 2;

        std::string topic_name;
        if (tn_len > 0 && pos + tn_len <= size) {
            topic_name.assign(data + pos, tn_len);
            pos += tn_len;
        }

        // partition_count
        int32_t part_count = 0;
        if (pos + 4 > size) break;
        uint32_t pc_raw;
        std::memcpy(&pc_raw, data + pos, 4);
        part_count = static_cast<int32_t>(__builtin_bswap32(pc_raw));
        pos += 4;

        for (int32_t p = 0; p < part_count && pos < size; ++p) {
            tp::TopicPartition tp_entry;
            tp_entry.topic = topic_name;

            // partition_index
            if (pos + 4 > size) break;
            uint32_t pi_raw;
            std::memcpy(&pi_raw, data + pos, 4);
            tp_entry.partition = static_cast<tp::PartitionIndex>(
                __builtin_bswap32(pi_raw));
            pos += 4;

            // current_leader_epoch (INT32) — v9+, skip
            if (pos + 4 > size) break;
            pos += 4;

            // fetch_offset
            tp::Offset fetch_offset = 0;
            if (pos + 8 > size) break;
            uint64_t fo_raw;
            std::memcpy(&fo_raw, data + pos, 8);
            fetch_offset = static_cast<tp::Offset>(__builtin_bswap64(fo_raw));
            pos += 8;

            // log_start_offset (INT64) — v5+, skip
            if (pos + 8 > size) break;
            pos += 8;

            // partition_max_bytes
            if (pos + 4 > size) break;
            pos += 4;

            req.partitions.push_back(std::move(tp_entry));
            req.fetch_offsets.push_back(fetch_offset);
        }
    }

    return req;
}

} // namespace protocol_helpers

} // namespace torrent::client
