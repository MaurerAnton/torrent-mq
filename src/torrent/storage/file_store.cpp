/**
 * torrent-mq — File-Backed Key-Value Store
 *
 * A simple append-only on-disk key-value store used as a fallback when
 * RocksDB is not available (embedded deployments, minimal builds, or
 * systems without the RocksDB library).
 *
 * Design:
 *   - Append-only write log: all puts and deletes are appended to a
 *     write-ahead log file. On restart, the log is replayed to rebuild
 *     the in-memory index.
 *   - In-memory index: a std::unordered_map mapping keys to file offsets
 *     and sizes, enabling O(1) lookups.
 *   - Periodic compaction: when the log file exceeds a threshold, entries
 *     are rewritten to a new file, compacting out deleted/tombstoned keys.
 *   - Crash safety: writes are atomic at the record level (each write is
 *     a complete, CRC-checked record). On crash, the last partial entry
 *     is discarded during replay.
 *
 * File format:
 *   Each record in the log file:
 *     [4 bytes: CRC32 of (key_len + value_len + key + value)]
 *     [2 bytes: key_len (uint16_t)]
 *     [4 bytes: value_len (uint32_t)]  — 0xFFFFFFFF = tombstone (deleted)
 *     [key_len bytes: key]
 *     [value_len bytes: value]         — absent if tombstone
 *
 * Thread safety: all public methods are serialized via a mutex.
 * Reads can be concurrent with a shared lock when no compaction is running.
 */

#include "torrent/storage/types.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// ============================================================================
// Anonymous namespace — internal helpers and constants
// ============================================================================

namespace torrent {
namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_file_store_logger() {
    static auto logger = spdlog::get("file_store");
    if (!logger) {
        logger = spdlog::stdout_color_mt("file_store");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Magic bytes prepended to the log file for identification.
static constexpr uint32_t kFileStoreMagic = 0x54514B56;  // "TQKV" LE

/// Format version.
static constexpr uint16_t kFileStoreVersion = 1;

/// Header size in bytes: magic(4) + version(2) = 6.
static constexpr size_t kHeaderSize = 6;

/// Record overhead: crc32(4) + key_len(2) + value_len(4) = 10 bytes.
static constexpr size_t kRecordOverhead = 10;

/// Sentinel value_len for tombstone (deleted key).
static constexpr uint32_t kTombstoneValueLen = 0xFFFFFFFF;

/// Default compaction threshold: compact when file exceeds this size.
static constexpr size_t kDefaultCompactThreshold = 64 * 1024 * 1024;  // 64 MiB

/// Default maximum record size.
static constexpr size_t kMaxRecordSize = 10 * 1024 * 1024;  // 10 MiB

// --------------------------------------------------------------------------
// CRC32 (simple table-driven, compatible with common CRC-32 implementations)
// --------------------------------------------------------------------------

static constexpr uint32_t kCrc32Table[256] = {
    0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA, 0x076DC419, 0x706AF48F,
    0xE963A535, 0x9E6495A3, 0x0EDB8832, 0x79DCB8A4, 0xE0D5E91E, 0x97D2D988,
    0x09B64C2B, 0x7EB17CBD, 0xE7B82D07, 0x90BF1D91, 0x1DB71064, 0x6AB020F2,
    0xF3B97148, 0x84BE41DE, 0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7,
    0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC, 0x14015C4F, 0x63066CD9,
    0xFA0F3D63, 0x8D080DF5, 0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172,
    0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B, 0x35B5A8FA, 0x42B2986C,
    0xDBBBC9D6, 0xACBCF940, 0x32D86CE3, 0x45DF5C75, 0xDCD60DCF, 0xABD13D59,
    0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116, 0x21B4F4B5, 0x56B3C423,
    0xCFBA9599, 0xB8BDA50F, 0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924,
    0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D, 0x76DC4190, 0x01DB7106,
    0x98D220BC, 0xEFD5102A, 0x71B18589, 0x06B6B51F, 0x9FBFE4A5, 0xE8B8D433,
    0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818, 0x7F6A0DBB, 0x086D3D2D,
    0x91646C97, 0xE6635C01, 0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E,
    0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457, 0x65B0D9C6, 0x12B7E950,
    0x8BBEB8EA, 0xFCB9887C, 0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65,
    0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2, 0x4ADFA541, 0x3DD895D7,
    0xA4D1C46D, 0xD3D6F4FB, 0x4369E96A, 0x346ED9FC, 0xAD678846, 0xDA60B8D0,
    0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7CC9, 0x5005713C, 0x270241AA,
    0xBE0B1010, 0xC90C2086, 0x5768B525, 0x206F85B3, 0xB966D409, 0xCE61E49F,
    0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4, 0x59B33D17, 0x2EB40D81,
    0xB7BD5C3B, 0xC0BA6CAD, 0xEDB88320, 0x9ABFB3B6, 0x03B6E20C, 0x74B1D29A,
    0xEAD54739, 0x9DD277AF, 0x04DB2615, 0x73DC1683, 0xE3630B12, 0x94643B84,
    0x0D6D6A3E, 0x7A6A5AA8, 0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1,
    0xF00F9344, 0x8708A3D2, 0x1E01F268, 0x6906C2FE, 0xF762575D, 0x806567CB,
    0x196C3671, 0x6E6B06E7, 0xFED41B76, 0x89D32BE0, 0x10DA7A5A, 0x67DD4ACC,
    0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5, 0xD6D6A3E8, 0xA1D1937E,
    0x38D8C2C4, 0x4FDFF252, 0xD1BB67F1, 0xA6BC5767, 0x3FB506DD, 0x48B2364B,
    0xD80D2BDA, 0xAF0A1B4C, 0x36034AF6, 0x41047A60, 0xDF60EFC3, 0xA867DF55,
    0x316E8EEF, 0x4669BE79, 0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236,
    0xCC0C7795, 0xBB0B4703, 0x220216B9, 0x5505262F, 0xC5BA3BBE, 0xB2BD0B28,
    0x2BB45A92, 0x5CB30A04, 0xC2D7FFA7, 0xB5D0CF31, 0x2CD99E8B, 0x5BDEAE1D,
    0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A, 0x9C0906A9, 0xEB0E363F,
    0x72076785, 0x05005713, 0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38,
    0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB7, 0x0BDBDF21, 0x86D3D2D4, 0xF1D4E242,
    0x68DDB3F8, 0x1FDA836E, 0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777,
    0x88085AE6, 0xFF0F6A70, 0x66063BCA, 0x11010B5C, 0x8F659EFF, 0xF862AE69,
    0x616BFFD3, 0x166CCF45, 0xA00AE278, 0xD70DD2EE, 0x4E048354, 0x3903B3C2,
    0xA7672661, 0xD06016F7, 0x4969474D, 0x3E6E77DB, 0xAED16A4A, 0xD9D65ADC,
    0x40DF0B66, 0x37D83BF0, 0xA9BCAE53, 0xDEBB9EC5, 0x47B2CF7F, 0x30B5FFE9,
    0xBDBDF21C, 0xCABAC28A, 0x53B39330, 0x24B4A3A6, 0xBAD03605, 0xCDD70693,
    0x54DE5729, 0x23D967BF, 0xB3667A2E, 0xC4614AB8, 0x5D681B02, 0x2A6F2B94,
    0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B, 0x2D02EF8D
};

uint32_t crc32(const void* data, size_t len, uint32_t crc = 0xFFFFFFFF) {
    const auto* p = static_cast<const uint8_t*>(data);
    crc ^= 0xFFFFFFFF;
    for (size_t i = 0; i < len; ++i) {
        crc = kCrc32Table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

// --------------------------------------------------------------------------
// Record on-disk layout
// --------------------------------------------------------------------------

#pragma pack(push, 1)
struct RecordHeader {
    uint32_t crc = 0;
    uint16_t key_len = 0;
    uint32_t value_len = 0;  // kTombstoneValueLen = deleted
};
#pragma pack(pop)

static_assert(sizeof(RecordHeader) == kRecordOverhead,
              "RecordHeader size mismatch");

// --------------------------------------------------------------------------
// Index entry: maps a key to its location in the log file
// --------------------------------------------------------------------------

struct IndexEntry {
    uint64_t file_offset = 0;   ///< Byte offset of the RecordHeader in the log file.
    uint32_t value_len  = 0;   ///< Length of value data (0 for tombstone).
    bool     is_tombstone = false;
};

} // anonymous namespace

// ============================================================================
// FileStore — Public Interface
// ============================================================================

/**
 * FileStore is a simple append-only file-backed key-value store.
 *
 * Use when RocksDB is not available. Provides put/get/delete operations
 * with O(1) lookups via an in-memory index loaded from the append-only log.
 *
 * Typical usage:
 *   FileStore store;
 *   store.open("/var/lib/torrent/kv_store");
 *   store.put("my_key", "my_value");
 *   auto val = store.get("my_key");
 *   store.close();
 */
class FileStore {
public:
    struct Config {
        std::string data_directory;   ///< Directory for the log file.
        std::string file_name = "file_store.log";
        size_t      compact_threshold = kDefaultCompactThreshold;  ///< Trigger compaction.
        bool        sync_on_write = true;   ///< fsync after each write.
    };

    FileStore() = default;

    ~FileStore() {
        close();
    }

    FileStore(const FileStore&) = delete;
    FileStore& operator=(const FileStore&) = delete;
    FileStore(FileStore&&) = delete;
    FileStore& operator=(FileStore&&) = delete;

    // -- Lifecycle ----------------------------------------------------------

    /**
     * Open the file store. Creates the data directory and log file if
     * they don't exist. Replays the log to rebuild the in-memory index.
     */
    result<void> open(const Config& config) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (is_open_) {
            return result<void>::failure(
                error_code::duplicate_resource,
                "FileStore already open");
        }

        config_ = config;
        std::filesystem::create_directories(config_.data_directory);

        file_path_ = config_.data_directory + "/" + config_.file_name;

        auto logger = get_file_store_logger();
        logger->info("Opening FileStore: {}", file_path_);

        // Open the log file (create if not exists)
        bool file_exists = std::filesystem::exists(file_path_);

        file_.open(file_path_, std::ios::binary | std::ios::in | std::ios::out);
        if (!file_.is_open()) {
            // File doesn't exist — create it
            file_.open(file_path_, std::ios::binary | std::ios::out);
            if (!file_.is_open()) {
                return result<void>::failure(
                    error_code::storage_unavailable,
                    fmt::format("Cannot create file: {}", file_path_));
            }
            // Write header
            write_header();
            file_.flush();
            file_.close();

            // Reopen in read-write mode
            file_.open(file_path_, std::ios::binary | std::ios::in | std::ios::out);
            if (!file_.is_open()) {
                return result<void>::failure(
                    error_code::storage_unavailable,
                    fmt::format("Cannot reopen file: {}", file_path_));
            }
            file_exists = false;
        }

        if (!file_exists) {
            is_open_ = true;
            return result<void>::success();
        }

        // Validate header
        auto header_res = validate_header();
        if (header_res.failed()) {
            file_.close();
            return result<void>::failure(header_res.error, header_res.error_message);
        }

        // Replay log to rebuild index
        auto replay_res = replay_log();
        if (replay_res.failed()) {
            file_.close();
            return result<void>::failure(replay_res.error, replay_res.error_message);
        }

        is_open_ = true;
        logger->info("FileStore opened: {} keys in index", index_.size());

        return result<void>::success();
    }

    /**
     * Close the file store. Flushes pending writes.
     */
    result<void> close() {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!is_open_) return result<void>::success();

        auto logger = get_file_store_logger();
        logger->info("Closing FileStore: {} ({} keys)", file_path_, index_.size());

        flush();
        file_.close();
        is_open_ = false;

        return result<void>::success();
    }

    [[nodiscard]] bool is_open() const noexcept {
        return is_open_.load(std::memory_order_acquire);
    }

    // -- Operations ---------------------------------------------------------

    /**
     * Store a key-value pair. If the key already exists, its value is
     * overwritten (newer record takes precedence in the index after replay).
     */
    result<void> put(std::string_view key, std::string_view value) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!is_open_) {
            return result<void>::failure(
                error_code::storage_unavailable, "FileStore not open");
        }

        if (key.empty()) {
            return result<void>::failure(
                error_code::invalid_record, "Key cannot be empty");
        }

        if (key.size() > std::numeric_limits<uint16_t>::max()) {
            return result<void>::failure(
                error_code::message_too_large,
                fmt::format("Key too long: {} bytes (max {})",
                            key.size(), std::numeric_limits<uint16_t>::max()));
        }

        if (value.size() > std::numeric_limits<uint32_t>::max() - 1) {
            return result<void>::failure(
                error_code::message_too_large,
                "Value too long");
        }

        return append_record(key, value, false);
    }

    /**
     * Retrieve a value by key.
     *
     * @return  result with optional string (nullopt if not found).
     */
    result<std::optional<std::string>> get(std::string_view key) {
        std::shared_lock<std::shared_mutex> lock(index_mutex_);

        if (!is_open_) {
            return result<std::optional<std::string>>::failure(
                error_code::storage_unavailable, "FileStore not open");
        }

        auto it = index_.find(std::string(key));
        if (it == index_.end() || it->second.is_tombstone) {
            return result<std::optional<std::string>>::success(std::nullopt);
        }

        // Read value from file
        return result<std::optional<std::string>>::success(
            read_value(it->second));
    }

    /**
     * Delete a key (writes a tombstone record).
     * Deleting a non-existent key is a no-op.
     */
    result<void> del(std::string_view key) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!is_open_) {
            return result<void>::failure(
                error_code::storage_unavailable, "FileStore not open");
        }

        if (index_.find(std::string(key)) == index_.end()) {
            return result<void>::success();  // idempotent
        }

        return append_record(key, {}, true);
    }

    /**
     * Check if a key exists (and is not a tombstone).
     */
    [[nodiscard]] bool exists(std::string_view key) const {
        std::shared_lock<std::shared_mutex> lock(index_mutex_);
        auto it = index_.find(std::string(key));
        return it != index_.end() && !it->second.is_tombstone;
    }

    /**
     * Number of live keys in the store.
     */
    [[nodiscard]] size_t size() const noexcept {
        std::shared_lock<std::shared_mutex> lock(index_mutex_);
        return index_.size();
    }

    /**
     * Return all keys (not tombstones).
     */
    [[nodiscard]] std::vector<std::string> list_keys() const {
        std::shared_lock<std::shared_mutex> lock(index_mutex_);
        std::vector<std::string> keys;
        keys.reserve(index_.size());
        for (const auto& [key, entry] : index_) {
            if (!entry.is_tombstone) {
                keys.push_back(key);
            }
        }
        return keys;
    }

    /**
     * Trigger compaction: rewrite live entries to a new file, removing
     * tombstones and old versions of overwritten keys. Reduces file size.
     *
     * Compaction is automatically triggered when the log file exceeds
     * `compact_threshold`, but can also be called manually.
     */
    result<void> compact() {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!is_open_) {
            return result<void>::failure(
                error_code::storage_unavailable, "FileStore not open");
        }

        auto logger = get_file_store_logger();
        logger->info("Compacting FileStore: {} ({} keys)", file_path_, index_.size());

        std::string tmp_path = file_path_ + ".compact";

        // Take snapshot of current index
        std::unordered_map<std::string, IndexEntry> snapshot;
        {
            std::shared_lock<std::shared_mutex> idx_lock(index_mutex_);
            snapshot = index_;
        }

        // Create compacted file
        std::ofstream compact(tmp_path, std::ios::binary | std::ios::trunc);
        if (!compact.is_open()) {
            return result<void>::failure(
                error_code::storage_unavailable,
                fmt::format("Cannot create compact file: {}", tmp_path));
        }

        // Write header
        compact.write(reinterpret_cast<const char*>(&kFileStoreMagic),
                      sizeof(kFileStoreMagic));
        compact.write(reinterpret_cast<const char*>(&kFileStoreVersion),
                      sizeof(kFileStoreVersion));

        // Helper lambda to write a record
        uint64_t offset = kHeaderSize;
        auto write_record = [&](std::string_view k, std::string_view v, bool tombstone) {
            RecordHeader hdr;
            hdr.key_len = static_cast<uint16_t>(k.size());
            hdr.value_len = tombstone ? kTombstoneValueLen
                                      : static_cast<uint32_t>(v.size());

            // Compute CRC: over key_len + value_len + key + value
            uint32_t crc = 0xFFFFFFFF;
            crc = crc32(&hdr.key_len, sizeof(hdr.key_len), crc);
            crc = crc32(&hdr.value_len, sizeof(hdr.value_len), crc);
            crc = crc32(k.data(), k.size(), crc);
            if (!tombstone) {
                crc = crc32(v.data(), v.size(), crc);
            }
            hdr.crc = crc ^ 0xFFFFFFFF;

            compact.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
            compact.write(k.data(), static_cast<std::streamsize>(k.size()));
            if (!tombstone) {
                compact.write(v.data(), static_cast<std::streamsize>(v.size()));
            }

            offset += sizeof(hdr) + k.size() + (tombstone ? 0 : v.size());
        };

        // Write all live entries
        int rewritten = 0;
        for (const auto& [key, entry] : snapshot) {
            if (!entry.is_tombstone) {
                auto val = read_value(entry);
                if (val) {
                    write_record(key, *val, false);
                    ++rewritten;
                }
            }
        }

        compact.flush();
        compact.close();

        // Close current file, replace with compacted
        file_.close();
        std::filesystem::remove(file_path_);
        std::filesystem::rename(tmp_path, file_path_);

        // Reopen
        file_.open(file_path_, std::ios::binary | std::ios::in | std::ios::out);
        if (!file_.is_open()) {
            return result<void>::failure(
                error_code::storage_unavailable,
                fmt::format("Cannot reopen after compaction: {}", file_path_));
        }

        // Rebuild index from compacted file
        index_.clear();
        auto replay_res = replay_log();
        if (replay_res.failed()) {
            return replay_res;
        }

        logger->info("Compaction complete: {} keys rewritten", rewritten);
        return result<void>::success();
    }

    /**
     * Check if compaction should be triggered based on file size.
     */
    [[nodiscard]] bool should_compact() const noexcept {
        return std::filesystem::exists(file_path_) &&
               std::filesystem::file_size(file_path_) > config_.compact_threshold;
    }

private:
    // -- Internal -----------------------------------------------------------

    void write_header() {
        file_.seekp(0, std::ios::beg);
        file_.write(reinterpret_cast<const char*>(&kFileStoreMagic),
                    sizeof(kFileStoreMagic));
        file_.write(reinterpret_cast<const char*>(&kFileStoreVersion),
                    sizeof(kFileStoreVersion));
    }

    result<void> validate_header() {
        file_.seekg(0, std::ios::beg);
        uint32_t magic = 0;
        uint16_t version = 0;
        file_.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        file_.read(reinterpret_cast<char*>(&version), sizeof(version));

        if (magic != kFileStoreMagic) {
            return result<void>::failure(
                error_code::corrupt_message,
                fmt::format("Invalid magic: 0x{:08X} (expected 0x{:08X})",
                            magic, kFileStoreMagic));
        }

        if (version != kFileStoreVersion) {
            return result<void>::failure(
                error_code::corrupt_message,
                fmt::format("Unsupported version: {} (expected {})",
                            version, kFileStoreVersion));
        }

        return result<void>::success();
    }

    result<void> replay_log() {
        index_.clear();

        file_.seekg(0, std::ios::end);
        auto file_size = static_cast<uint64_t>(file_.tellg());
        file_.seekg(kHeaderSize, std::ios::beg);

        uint64_t offset = kHeaderSize;
        int records_replayed = 0;
        int tombstones = 0;
        int crc_errors = 0;

        while (offset < file_size) {
            RecordHeader hdr;
            file_.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
            file_.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));

            if (file_.gcount() != static_cast<std::streamsize>(sizeof(hdr))) {
                // Partial header at end of file — truncate
                get_file_store_logger()->warn(
                    "Partial record header at offset {}; truncating", offset);
                truncate_file(offset);
                break;
            }

            // Validate sizes
            size_t record_size = sizeof(hdr) + hdr.key_len +
                (hdr.value_len == kTombstoneValueLen ? 0 : hdr.value_len);

            if (offset + record_size > file_size) {
                get_file_store_logger()->warn(
                    "Record at offset {} exceeds file size; truncating", offset);
                truncate_file(offset);
                break;
            }

            // Read key and value
            std::string key(hdr.key_len, '\0');
            file_.read(key.data(), hdr.key_len);

            std::string value;
            bool is_tombstone = (hdr.value_len == kTombstoneValueLen);
            if (!is_tombstone) {
                value.resize(hdr.value_len);
                file_.read(value.data(), hdr.value_len);
            }

            // Verify CRC
            uint32_t computed_crc = 0xFFFFFFFF;
            computed_crc = crc32(&hdr.key_len, sizeof(hdr.key_len), computed_crc);
            computed_crc = crc32(&hdr.value_len, sizeof(hdr.value_len), computed_crc);
            computed_crc = crc32(key.data(), key.size(), computed_crc);
            if (!is_tombstone) {
                computed_crc = crc32(value.data(), value.size(), computed_crc);
            }

            if (computed_crc != hdr.crc) {
                ++crc_errors;
                get_file_store_logger()->warn(
                    "CRC mismatch at offset {}; truncating", offset);
                truncate_file(offset);
                break;
            }

            // Update index
            IndexEntry entry;
            entry.file_offset = offset;
            entry.value_len = is_tombstone ? 0 : hdr.value_len;
            entry.is_tombstone = is_tombstone;

            if (is_tombstone) {
                index_.erase(key);
                ++tombstones;
            } else {
                index_[key] = entry;
            }

            ++records_replayed;
            offset += record_size;
        }

        auto logger = get_file_store_logger();
        logger->info("Log replay complete: {} records, {} tombstones, {} CRC errors, {} active keys",
                     records_replayed, tombstones, crc_errors, index_.size());

        return result<void>::success();
    }

    result<void> append_record(std::string_view key, std::string_view value,
                                bool is_tombstone) {
        file_.seekp(0, std::ios::end);
        uint64_t offset = static_cast<uint64_t>(file_.tellp());

        RecordHeader hdr;
        hdr.key_len = static_cast<uint16_t>(key.size());
        hdr.value_len = is_tombstone ? kTombstoneValueLen
                                     : static_cast<uint32_t>(value.size());

        // Compute CRC
        uint32_t crc = 0xFFFFFFFF;
        crc = crc32(&hdr.key_len, sizeof(hdr.key_len), crc);
        crc = crc32(&hdr.value_len, sizeof(hdr.value_len), crc);
        crc = crc32(key.data(), key.size(), crc);
        if (!is_tombstone) {
            crc = crc32(value.data(), value.size(), crc);
        }
        hdr.crc = crc ^ 0xFFFFFFFF;

        // Write record
        file_.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
        file_.write(key.data(), static_cast<std::streamsize>(key.size()));
        if (!is_tombstone) {
            file_.write(value.data(), static_cast<std::streamsize>(value.size()));
        }

        if (file_.fail()) {
            return result<void>::failure(
                error_code::storage_unavailable,
                "Write to log file failed");
        }

        if (config_.sync_on_write) {
            flush();
        }

        // Update index
        {
            std::unique_lock<std::shared_mutex> idx_lock(index_mutex_);
            IndexEntry entry;
            entry.file_offset = offset;
            entry.value_len = is_tombstone ? 0 : hdr.value_len;
            entry.is_tombstone = is_tombstone;

            if (is_tombstone) {
                index_.erase(std::string(key));
            } else {
                index_[std::string(key)] = entry;
            }
        }

        return result<void>::success();
    }

    std::optional<std::string> read_value(const IndexEntry& entry) const {
        if (entry.is_tombstone) return std::nullopt;

        file_.seekg(static_cast<std::streamoff>(entry.file_offset + sizeof(RecordHeader) + entry.value_len),
                     std::ios::beg);
        // Actually need to read at correct offset
        // First read the header to get key_len, then skip key, then read value
        file_.seekg(static_cast<std::streamoff>(entry.file_offset), std::ios::beg);

        RecordHeader hdr;
        file_.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
        file_.seekg(static_cast<std::streamoff>(hdr.key_len), std::ios::cur);

        std::string value(hdr.value_len, '\0');
        file_.read(value.data(), hdr.value_len);

        return value;
    }

    void truncate_file(uint64_t new_size) {
        file_.flush();
        file_.close();

        std::filesystem::resize_file(file_path_, new_size);

        file_.open(file_path_, std::ios::binary | std::ios::in | std::ios::out);
    }

    void flush() {
        if (file_.is_open()) {
            file_.flush();
        }
    }

    Config                    config_;
    std::string               file_path_;
    mutable std::fstream      file_;
    std::atomic<bool>         is_open_{false};
    mutable std::mutex        mutex_;         ///< Serializes file writes
    mutable std::shared_mutex index_mutex_;   ///< Protects index_ (shared for reads)
    std::unordered_map<std::string, IndexEntry> index_;
};

} // namespace torrent
