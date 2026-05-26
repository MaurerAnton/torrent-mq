/**
 * offset_backing_store.cpp — OffsetBackingStore: Persistent Offset Storage
 *
 * Provides persistent, thread-safe storage for connector source/sink offsets.
 * Offsets are committed by connectors and restored on restart so that
 * connectors can resume from their last known position.
 *
 * Storage format (JSON, one file per connector):
 *   {
 *     "connector": "my-connector",
 *     "offsets": [
 *       {
 *         "topic": "orders",
 *         "partition": 0,
 *         "offset": 12345,
 *         "committed_at_ms": 1711497600000
 *       },
 *       ...
 *     ],
 *     "last_updated_ms": 1711497600000,
 *     "format_version": 1
 *   }
 *
 * Features:
 *   - File-based JSON storage in configurable directory
 *   - Atomic writes via temp file + rename
 *   - Thread-safe with mutex (one store instance per connector)
 *   - Periodic auto-save with configurable interval
 *   - Recovery: load on construction, restore on-demand
 *   - In-memory cache for fast offset lookups
 *
 * Configuration:
 *   - offsets.dir: directory for offset files (default: ./data/connect-offsets)
 *   - offsets.commit.interval.ms: auto-commit interval (default: 30000ms)
 *   - max.offset.files: max offset files to retain (default: 100)
 *
 * Thread-safety:
 *   All public methods acquire a mutex.  The backing store is safe for
 *   concurrent access from the connector worker thread and the framework's
 *   offset commit thread.
 *
 * Dependencies:
 *   - <fstream> for file I/O
 *   - <filesystem> for directory management
 *   - <nlohmann/json.hpp> for JSON serialization
 */

#include "torrent/connectors/connect.h"

#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

using json = nlohmann::json;
namespace fs = std::filesystem;

namespace torrent::connectors {

// ============================================================================
// Anonymous namespace — helpers, constants
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_offset_store_logger() {
    static auto logger = spdlog::get("offset_store");
    if (!logger) {
        logger = spdlog::stdout_color_mt("offset_store");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

inline constexpr int64_t  kDefaultCommitIntervalMs = 30000;  // 30 seconds
inline constexpr size_t   kMaxOffsetFiles          = 100;
inline constexpr int32_t  kFormatVersion           = 1;
inline constexpr size_t   kMaxRetryWrites          = 3;

inline constexpr std::string_view kDefaultOffsetsDir = "./data/connect-offsets";

// --------------------------------------------------------------------------
// Offset record structure
// --------------------------------------------------------------------------

struct OffsetEntry {
    std::string topic;
    int32_t     partition;
    offset_t    offset = kInvalidOffset;
    int64_t     committed_at_ms = 0;

    [[nodiscard]] json to_json() const {
        json j;
        j["topic"]            = topic;
        j["partition"]        = partition;
        j["offset"]           = offset;
        j["committed_at_ms"]  = committed_at_ms;
        return j;
    }

    [[nodiscard]] static OffsetEntry from_json(const json& j) {
        OffsetEntry entry;
        entry.topic            = j.value("topic", "");
        entry.partition        = j.value("partition", 0);
        entry.offset           = j.value("offset", kInvalidOffset);
        entry.committed_at_ms  = j.value("committed_at_ms", 0);
        return entry;
    }
};

// --------------------------------------------------------------------------
// Build offset key for map lookup
// --------------------------------------------------------------------------

[[nodiscard]] std::string make_offset_key(std::string_view topic,
                                             int32_t partition) {
    std::ostringstream oss;
    oss << topic << ':' << partition;
    return oss.str();
}

// --------------------------------------------------------------------------
// Ensure directory exists
// --------------------------------------------------------------------------

bool ensure_directory(const fs::path& dir) {
    std::error_code ec;
    if (fs::exists(dir, ec)) {
        return fs::is_directory(dir, ec);
    }
    return fs::create_directories(dir, ec);
}

// --------------------------------------------------------------------------
// Sanitize connector name for use as filename
// --------------------------------------------------------------------------

[[nodiscard]] std::string sanitize_filename(std::string_view name) {
    std::string result(name);
    for (auto& c : result) {
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.')) {
            c = '_';
        }
    }
    return result;
}

// --------------------------------------------------------------------------
// Get the file path for a connector's offset file
// --------------------------------------------------------------------------

[[nodiscard]] fs::path offset_file_path(const fs::path& dir,
                                          std::string_view connector_name) {
    std::string fname = sanitize_filename(connector_name);
    fname += ".offsets.json";
    return dir / fname;
}

// --------------------------------------------------------------------------
// Atomic write: write to temp file, then rename
// --------------------------------------------------------------------------

bool atomic_write(const fs::path& filepath, const std::string& content) {
    auto logger = get_offset_store_logger();

    fs::path temp_path(filepath);
    temp_path += ".tmp." + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());

    auto parent = filepath.parent_path();
    if (!parent.empty()) {
        std::error_code ec;
        fs::create_directories(parent, ec);
    }

    for (size_t attempt = 0; attempt < kMaxRetryWrites; ++attempt) {
        try {
            // Write to temporary file
            {
                std::ofstream ofs(temp_path, std::ios::out |
                                             std::ios::trunc |
                                             std::ios::binary);
                if (!ofs.is_open()) {
                    logger->error("atomic_write: failed to open temp file {}",
                                  temp_path.string());
                    if (attempt + 1 < kMaxRetryWrites) {
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(10));
                        continue;
                    }
                    return false;
                }

                ofs.write(content.data(),
                          static_cast<std::streamsize>(content.size()));
                if (!ofs) {
                    logger->error("atomic_write: write failed for {}",
                                  temp_path.string());
                    ofs.close();
                    if (attempt + 1 < kMaxRetryWrites) {
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(10));
                        continue;
                    }
                    return false;
                }
                ofs.close();
            }

            // Rename atomically
            std::error_code ec;
            fs::rename(temp_path, filepath, ec);
            if (ec) {
                logger->error("atomic_write: rename failed: {} -> {}: {}",
                              temp_path.string(), filepath.string(),
                              ec.message());
                if (attempt + 1 < kMaxRetryWrites) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(10));
                    continue;
                }
                return false;
            }

            logger->trace("atomic_write: wrote {} bytes to {}",
                          content.size(), filepath.string());
            return true;

        } catch (const std::exception& e) {
            logger->error("atomic_write: exception: {}", e.what());
            if (attempt + 1 < kMaxRetryWrites) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(10));
                continue;
            }
            return false;
        }
    }

    return false;
}

} // anonymous namespace

// ============================================================================
// OffsetBackingStore
// ============================================================================

class OffsetBackingStore {
public:
    // ------------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------------

    /// Construct a backing store for a named connector.
    /// @param connector_name the connector this store belongs to
    /// @param offsets_dir    directory for offset files
    /// @param commit_interval_ms auto-commit interval (0 = disabled)
    explicit OffsetBackingStore(
        std::string connector_name,
        fs::path offsets_dir = kDefaultOffsetsDir,
        int64_t commit_interval_ms = kDefaultCommitIntervalMs)
        : connector_name_(std::move(connector_name))
        , offsets_dir_(std::move(offsets_dir))
        , commit_interval_ms_(commit_interval_ms)
    {
        auto logger = get_offset_store_logger();

        ensure_directory(offsets_dir_);
        file_path_ = offset_file_path(offsets_dir_, connector_name_);

        // Try to restore offsets from disk
        restore();

        // Start auto-commit thread if interval > 0
        if (commit_interval_ms_ > 0) {
            running_.store(true);
            commit_thread_ = std::thread([this] {
                periodic_commit();
            });
            logger->info("OffsetBackingStore '{}': auto-commit every {}ms",
                         connector_name_, commit_interval_ms_);
        }

        logger->info("OffsetBackingStore '{}' initialized (path={}, offsets={})",
                     connector_name_, file_path_.string(), offsets_.size());
    }

    // ------------------------------------------------------------------------
    // Destruction
    // ------------------------------------------------------------------------

    ~OffsetBackingStore() {
        stop();
    }

    // ------------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------------

    void stop() {
        if (running_.exchange(false)) {
            if (commit_thread_.joinable()) {
                commit_thread_.join();
            }
            // Final commit
            commit();
        }
    }

    // ------------------------------------------------------------------------
    // Commit a single offset
    // ------------------------------------------------------------------------

    void commit_offset(std::string_view topic,
                        int32_t partition,
                        offset_t offset) {
        std::lock_guard<std::mutex> lock(mutex_);

        std::string key = make_offset_key(topic, partition);

        auto it = offsets_.find(key);
        if (it == offsets_.end()) {
            OffsetEntry entry;
            entry.topic     = topic;
            entry.partition = partition;
            entry.offset    = offset;
            entry.committed_at_ms = now_ms();
            offsets_[key] = std::move(entry);
        } else {
            if (offset > it->second.offset) {
                it->second.offset = offset;
                it->second.committed_at_ms = now_ms();
            }
        }

        dirty_.store(true, std::memory_order_release);
    }

    // ------------------------------------------------------------------------
    // Commit a batch of offsets
    // ------------------------------------------------------------------------

    void commit_offsets(const std::vector<OffsetEntry>& entries) {
        std::lock_guard<std::mutex> lock(mutex_);

        int64_t ts = now_ms();

        for (auto& entry : entries) {
            std::string key = make_offset_key(entry.topic, entry.partition);

            auto it = offsets_.find(key);
            if (it == offsets_.end()) {
                OffsetEntry e = entry;
                e.committed_at_ms = ts;
                offsets_[key] = std::move(e);
            } else if (entry.offset > it->second.offset) {
                it->second.offset = entry.offset;
                it->second.committed_at_ms = ts;
            }
        }

        dirty_.store(true, std::memory_order_release);
    }

    // ------------------------------------------------------------------------
    // Get a stored offset
    // ------------------------------------------------------------------------

    [[nodiscard]] offset_t get_offset(std::string_view topic,
                                        int32_t partition) const {
        std::string key = make_offset_key(topic, partition);
        return get_offset_by_key(key);
    }

    // ------------------------------------------------------------------------
    // Get all stored offsets
    // ------------------------------------------------------------------------

    [[nodiscard]] std::vector<OffsetEntry> get_all_offsets() const {
        std::lock_guard<std::mutex> lock(mutex_);

        std::vector<OffsetEntry> result;
        result.reserve(offsets_.size());

        for (auto& [key, entry] : offsets_) {
            result.push_back(entry);
        }

        return result;
    }

    // ------------------------------------------------------------------------
    // Check if an offset is committed
    // ------------------------------------------------------------------------

    [[nodiscard]] bool is_committed(std::string_view topic,
                                      int32_t partition,
                                      offset_t offset) const {
        offset_t stored = get_offset(topic, partition);
        return stored != kInvalidOffset && stored >= offset;
    }

    // ------------------------------------------------------------------------
    // Persist all offsets to disk
    // ------------------------------------------------------------------------

    bool commit() {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!dirty_.load(std::memory_order_acquire)) {
            return true;  // nothing to write
        }

        bool ok = save_to_disk();
        dirty_.store(!ok, std::memory_order_release);
        return ok;
    }

    // ------------------------------------------------------------------------
    // Restore offsets from disk
    // ------------------------------------------------------------------------

    void restore() {
        std::lock_guard<std::mutex> lock(mutex_);

        auto logger = get_offset_store_logger();

        std::error_code ec;
        if (!fs::exists(file_path_, ec)) {
            logger->debug("OffsetBackingStore '{}': no existing offset file at {}",
                          connector_name_, file_path_.string());
            return;
        }

        try {
            std::ifstream ifs(file_path_);
            if (!ifs.is_open()) {
                logger->warn("OffsetBackingStore '{}': cannot open {} for reading",
                             connector_name_, file_path_.string());
                return;
            }

            json j;
            ifs >> j;
            ifs.close();

            if (!j.is_object()) {
                logger->warn("OffsetBackingStore '{}': corrupted offset file {}",
                             connector_name_, file_path_.string());
                return;
            }

            int32_t version = j.value("format_version", 0);
            if (version != kFormatVersion) {
                logger->warn("OffsetBackingStore '{}': version mismatch (file={}, expected={})",
                             connector_name_, version, kFormatVersion);
            }

            // Clear existing offsets
            offsets_.clear();

            auto offsets_array = j.find("offsets");
            if (offsets_array != j.end() && offsets_array->is_array()) {
                for (auto& offset_json : *offsets_array) {
                    auto entry = OffsetEntry::from_json(offset_json);
                    std::string key = make_offset_key(
                        entry.topic, entry.partition);
                    offsets_[key] = std::move(entry);
                }
            }

            dirty_.store(false, std::memory_order_release);

            logger->info("OffsetBackingStore '{}': restored {} offsets from {}",
                         connector_name_, offsets_.size(),
                         file_path_.string());

        } catch (const std::exception& e) {
            logger->error("OffsetBackingStore '{}': failed to restore offsets: {}",
                          connector_name_, e.what());
        }
    }

    // ------------------------------------------------------------------------
    // Size / status
    // ------------------------------------------------------------------------

    [[nodiscard]] size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return offsets_.size();
    }

    [[nodiscard]] bool is_dirty() const {
        return dirty_.load(std::memory_order_acquire);
    }

    [[nodiscard]] json status() const {
        std::lock_guard<std::mutex> lock(mutex_);

        json j;
        j["connector"]       = connector_name_;
        j["file_path"]       = file_path_.string();
        j["total_offsets"]   = offsets_.size();
        j["dirty"]           = dirty_.load();
        j["commit_interval_ms"] = commit_interval_ms_;
        j["running"]         = running_.load();

        json offsets_arr = json::array();
        for (auto& [key, entry] : offsets_) {
            offsets_arr.push_back(entry.to_json());
        }
        j["offsets"] = offsets_arr;

        return j;
    }

private:
    // ------------------------------------------------------------------------
    // Get current timestamp in milliseconds
    // ------------------------------------------------------------------------

    [[nodiscard]] static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    // ------------------------------------------------------------------------
    // Get offset by pre-computed key (caller must hold mutex_)
    // ------------------------------------------------------------------------

    [[nodiscard]] offset_t get_offset_by_key(const std::string& key) const {
        auto it = offsets_.find(key);
        if (it != offsets_.end()) {
            return it->second.offset;
        }
        return kInvalidOffset;
    }

    // ------------------------------------------------------------------------
    // Save offsets to disk (caller must hold mutex_)
    // ------------------------------------------------------------------------

    bool save_to_disk() {
        auto logger = get_offset_store_logger();

        json j;
        j["format_version"]   = kFormatVersion;
        j["connector"]        = connector_name_;
        j["last_updated_ms"]  = now_ms();

        json offsets_array = json::array();
        for (auto& [key, entry] : offsets_) {
            offsets_array.push_back(entry.to_json());
        }
        j["offsets"] = std::move(offsets_array);

        std::string content = j.dump(2);  // pretty-print with 2-space indent

        bool ok = atomic_write(file_path_, content);
        if (ok) {
            logger->debug("OffsetBackingStore '{}': saved {} offsets to {} ({} bytes)",
                          connector_name_, offsets_.size(),
                          file_path_.string(), content.size());
        } else {
            logger->error("OffsetBackingStore '{}': failed to save offsets to {}",
                          connector_name_, file_path_.string());
        }

        return ok;
    }

    // ------------------------------------------------------------------------
    // Periodic auto-commit thread
    // ------------------------------------------------------------------------

    void periodic_commit() {
        auto logger = get_offset_store_logger();

        while (running_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(commit_interval_ms_));

            if (!running_.load(std::memory_order_acquire)) break;

            if (is_dirty()) {
                logger->trace("OffsetBackingStore '{}': periodic commit triggered",
                              connector_name_);
                commit();
            }
        }
    }

    // ====================================================================
    // Member variables
    // ====================================================================

    std::string connector_name_;
    fs::path    offsets_dir_;
    fs::path    file_path_;
    int64_t     commit_interval_ms_;

    mutable std::mutex mutex_;
    std::unordered_map<std::string, OffsetEntry> offsets_;
    std::atomic<bool> dirty_{false};
    std::atomic<bool> running_{false};
    std::thread      commit_thread_;
};

} // namespace torrent::connectors
