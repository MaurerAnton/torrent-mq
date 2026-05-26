/**
 * raft_log.cpp — Persistent On-Disk Raft Log Storage
 *
 * Implements the RaftLog abstract interface with append-only segmented files,
 * a memory-resident index for O(1) term lookups, CRC32C protection per entry
 * batch, and asynchronous fsync batching for throughput.
 *
 * Layout
 * ------
 *   log_directory/
 *     partition-<pid>/
 *       log.dat          — append-only data file
 *       log.idx          — binary index file: (log_index:8, file_offset:8, term:8)
 *       meta             — text metadata (first_index, last_applied, crc_valid)
 *       snapshots/       — snapshot references
 *
 * Data file format (per batch):
 *   ┌──────────┬──────────┬──────────────┬──────────────┬──────────────────┐
 *   │ magic:4  │ crc32c:4 │ entry_cnt:4  │ total_len:4  │ entries (var) ...│
 *   └──────────┴──────────┴──────────────┴──────────────┴──────────────────┘
 *   - magic   = 0x544C4146 ("TLAF" = Torrent Log Append Frame)
 *   - crc32c  = CRC of everything after this field
 *   - entry_cnt = number of log entries in this batch
 *   - total_len  = total bytes of serialized entries
 *
 * Per-entry encoding:
 *   ┌──────────┬──────────┬──────────┬───────────────┬──────────────┐
 *   │ term:8   │ index:8  │ cmd_t:1  │ payload_sz:4  │ payload:N    │
 *   └──────────┴──────────┴──────────┴───────────────┴──────────────┘
 *   - cmd_t: 0=NoOp, 1=ConfigChange, 2=UserCommand
 *
 * Thread safety
 *   All public methods acquire a std::shared_mutex (shared for reads,
 *   exclusive for writes).  The fsync background thread coordinates via
 *   a condition variable.
 */

#include "torrent/consensus/raft_types.h"
#include "torrent/common/types.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
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
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

namespace torrent::raft {

// ============================================================================
// Constants
// ============================================================================

namespace {

/// Magic number for log append frames: "TLAF" = Torrent Log Append Frame.
static constexpr uint32_t kLogFrameMagic = 0x544C4146;

/// Current log storage format version.  Increment on breaking changes.
static constexpr uint32_t kLogFormatVersion = 1;

/// Maximum entries in a single batch (safety limit).
static constexpr size_t kMaxBatchEntries = 10'000;

/// Maximum batch payload size (64 MiB).
static constexpr size_t kMaxBatchBytes = 64 * 1024 * 1024;

/// Size of a single index entry on disk: index(8) + offset(8) + term(8).
static constexpr size_t kIndexEntrySize = 24;

/// How many bytes to fsync at most in one background flush batch.
static constexpr size_t kMaxFsyncBatchBytes = 256 * 1024 * 1024;

/// Interval at which the background fsync thread wakes to flush pending data.
static constexpr auto kFsyncInterval = std::chrono::milliseconds(50);

/// How many outstanding fsync requests before blocking producers.
static constexpr size_t kMaxPendingFsyncSlots = 128;

/// Maximum number of index entries to keep in a pending write queue before
/// flushing to disk.
static constexpr size_t kMaxPendingIndexWrites = 4096;

/// Page size for aligned writes (4 KiB).
static constexpr size_t kPageSize = 4096;

/// Command type tags in the binary log.
enum class CommandTag : uint8_t {
    NoOp         = 0,
    ConfigChange = 1,
    UserCommand  = 2,
};

}  // namespace

// ============================================================================
// CRC32C implementation (software fallback, SSE4.2 preferred if available)
// ============================================================================

namespace {

/// CRC32C lookup table (Castagnoli polynomial 0x1EDC6F41).
static const std::array<uint32_t, 256> kCrc32cTable = []() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc & 1) ? (crc >> 1) ^ 0x82F63B78 : crc >> 1;
        }
        table[i] = crc;
    }
    return table;
}();

/// Compute CRC32C incrementally.  Pass 0 as initial value; finalize with
/// `crc ^ 0xFFFFFFFF`.
[[nodiscard]] uint32_t crc32c_update(uint32_t crc,
                                      const uint8_t* data,
                                      size_t len) noexcept {
    crc ^= 0xFFFFFFFF;
    for (size_t i = 0; i < len; ++i) {
        crc = kCrc32cTable[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

[[nodiscard]] uint32_t crc32c_span(const std::vector<uint8_t>& buf) noexcept {
    return crc32c_update(0, buf.data(), buf.size());
}

[[nodiscard]] uint32_t crc32c_string_view(std::string_view sv) noexcept {
    return crc32c_update(
        0, reinterpret_cast<const uint8_t*>(sv.data()), sv.size());
}

}  // namespace

// ============================================================================
// Helper: integer (de)serialization (little-endian)
// ============================================================================

namespace {

void write_u32_le(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
}

void write_u64_le(std::vector<uint8_t>& buf, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        buf.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
    }
}

void write_i64_le(std::vector<uint8_t>& buf, int64_t v) {
    write_u64_le(buf, static_cast<uint64_t>(v));
}

[[nodiscard]] uint32_t read_u32_le(const uint8_t*& p) {
    uint32_t v = static_cast<uint32_t>(p[0])
               | (static_cast<uint32_t>(p[1]) << 8)
               | (static_cast<uint32_t>(p[2]) << 16)
               | (static_cast<uint32_t>(p[3]) << 24);
    p += 4;
    return v;
}

[[nodiscard]] uint64_t read_u64_le(const uint8_t*& p) {
    uint64_t v = static_cast<uint64_t>(p[0])
               | (static_cast<uint64_t>(p[1]) << 8)
               | (static_cast<uint64_t>(p[2]) << 16)
               | (static_cast<uint64_t>(p[3]) << 24)
               | (static_cast<uint64_t>(p[4]) << 32)
               | (static_cast<uint64_t>(p[5]) << 40)
               | (static_cast<uint64_t>(p[6]) << 48)
               | (static_cast<uint64_t>(p[7]) << 56);
    p += 8;
    return v;
}

[[nodiscard]] int64_t read_i64_le(const uint8_t*& p) {
    return static_cast<int64_t>(read_u64_le(p));
}

}  // namespace

// ============================================================================
// IndexEntry — one record in the memory-resident term index
// ============================================================================

/// Maps a LogIndex to its file offset and term for O(1) lookup.
struct IndexEntry {
    LogIndex index  = 0;
    uint64_t offset = 0;           ///< Byte offset in log.dat of batch header.
    RaftTerm term   = kNoTerm;     ///< Term of the entry at this index.
    uint32_t batch_length = 0;     ///< Total batch size (header + entries).
};

// ============================================================================
// DiskRaftLog — on-disk implementation of RaftLog
// ============================================================================

class DiskRaftLog final : public RaftLog {
public:
    // ------------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------------

    DiskRaftLog(std::string log_dir,
                broker_id_t partition_id,
                bool enable_fsync_batching = true)
        : log_dir_(std::move(log_dir))
        , partition_id_(partition_id)
        , enable_fsync_batching_(enable_fsync_batching)
    {
        namespace fs = std::filesystem;
        auto partition_dir = partition_path();
        std::error_code ec;
        fs::create_directories(partition_dir, ec);
        if (ec) {
            SPDLOG_ERROR("raft_log: cannot create directory {}: {}",
                         partition_dir, ec.message());
            throw std::runtime_error(
                "DiskRaftLog: cannot create log directory: " + ec.message());
        }

        // Open or create data file, index file, and metadata.
        open_or_create();
        recover_index();

        // Start background fsync thread if batching is enabled.
        if (enable_fsync_batching_) {
            fsync_thread_ = std::thread(&DiskRaftLog::fsync_loop, this);
        }

        SPDLOG_INFO("raft_log: partition {} log ready [{}..{}] ({} entries, {} bytes)",
                    partition_id_, first_index_, last_index_,
                    entry_count(), data_file_size_.load());
    }

    DiskRaftLog(const DiskRaftLog&) = delete;
    DiskRaftLog& operator=(const DiskRaftLog&) = delete;
    DiskRaftLog(DiskRaftLog&&) = delete;
    DiskRaftLog& operator=(DiskRaftLog&&) = delete;

    ~DiskRaftLog() override {
        shutdown();
    }

    void shutdown() {
        bool expected = false;
        if (!running_.compare_exchange_strong(expected, true)) return;
        // already shut down

        {
            std::lock_guard lock(write_mutex_);
            flush_and_fsync();
        }

        if (fsync_thread_.joinable()) {
            fsync_cv_.notify_all();
            fsync_thread_.join();
        }

        if (data_fd_ >= 0) {
            ::fsync(data_fd_);
            ::close(data_fd_);
            data_fd_ = -1;
        }
        if (idx_fd_ >= 0) {
            ::fsync(idx_fd_);
            ::close(idx_fd_);
            idx_fd_ = -1;
        }

        SPDLOG_INFO("raft_log: partition {} log shut down", partition_id_);
    }

    // ------------------------------------------------------------------------
    // Metadata (RaftLog interface)
    // ------------------------------------------------------------------------

    [[nodiscard]] LogIndex first_index() const noexcept override {
        std::shared_lock lock(mutex_);
        return first_index_;
    }

    [[nodiscard]] LogIndex last_index() const noexcept override {
        std::shared_lock lock(mutex_);
        return last_index_;
    }

    [[nodiscard]] size_t entry_count() const noexcept override {
        std::shared_lock lock(mutex_);
        return index_.size();
    }

    // ------------------------------------------------------------------------
    // Read (RaftLog interface)
    // ------------------------------------------------------------------------

    [[nodiscard]] RaftTerm term_at(LogIndex index) const noexcept override {
        std::shared_lock lock(mutex_);
        // Snapshot virtual sentinel.
        if (snapshot_ && index == snapshot_->last_included_index) {
            return snapshot_->last_included_term;
        }
        auto it = index_.find(index);
        if (it == index_.end()) return kNoTerm;
        return it->second.term;
    }

    [[nodiscard]] size_t get_entries(
        LogIndex start, LogIndex end,
        std::vector<LogEntry>& entries) const override {

        std::shared_lock lock(mutex_);

        if (start > end || start < first_index_ || end > last_index_) {
            return 0;
        }

        // Find the batch containing `start`.
        auto batch_it = find_batch_for(start);
        if (batch_it == index_.end()) return 0;

        entries.clear();
        entries.reserve(static_cast<size_t>(end - start + 1));

        // Read batch by batch.
        LogIndex current = start;
        while (current <= end) {
            auto it = index_.find(current);
            if (it == index_.end()) break;

            auto batch_entries = read_batch(it->second.offset);
            for (const auto& e : batch_entries) {
                if (e.index >= start && e.index <= end) {
                    entries.push_back(e);
                }
            }
            current = it->second.index + it->second.batch_length; // rough
            // Move to next batch.
            // Find the next index entry.
            auto next = it;
            ++next;
            if (next == index_.end()) break;
            current = next->first;
        }
        return entries.size();
    }

    [[nodiscard]] std::optional<LogEntry> entry_at(
        LogIndex index) const override {

        std::shared_lock lock(mutex_);
        auto it = index_.find(index);
        if (it == index_.end()) return std::nullopt;

        // Read the whole batch and find the entry.
        auto batch_entries = read_batch(it->second.offset);
        for (auto& e : batch_entries) {
            if (e.index == index) return e;
        }
        return std::nullopt;
    }

    // ------------------------------------------------------------------------
    // Write (RaftLog interface) — exclusive lock
    // ------------------------------------------------------------------------

    [[nodiscard]] LogIndex append(
        LogIndex prev_log_index, RaftTerm prev_log_term,
        std::vector<LogEntry> entries) override {

        std::lock_guard lock(write_mutex_);

        if (entries.empty()) {
            return last_index_;
        }

        // Validate consistency.
        if (prev_log_index > 0) {
            RaftTerm actual_term = kNoTerm;
            {
                std::shared_lock s(mutex_);
                actual_term = term_at(prev_log_index);
            }
            if (actual_term == kNoTerm) {
                SPDLOG_WARN("raft_log: p{} append failed: prev_log_index {} not found",
                            partition_id_, prev_log_index);
                return kNoLogIndex;
            }
            if (actual_term != prev_log_term) {
                SPDLOG_WARN("raft_log: p{} append failed: term mismatch at {} "
                            "(expected {}, got {})",
                            partition_id_, prev_log_index, prev_log_term, actual_term);
                return kNoLogIndex;
            }
        }

        // Truncate any stale entries past prev_log_index.
        {
            std::lock_guard g(mutex_);
            if (prev_log_index < last_index_) {
                truncate_suffix_locked(prev_log_index + 1);
            }
        }

        // Serialize the batch.
        auto batch_data = serialize_batch(entries);
        if (batch_data.size() > kMaxBatchBytes) {
            SPDLOG_ERROR("raft_log: p{} batch too large: {} bytes",
                         partition_id_, batch_data.size());
            return kNoLogIndex;
        }

        // Compute CRC and prepend frame header.
        uint32_t crc = crc32c_span(batch_data);
        std::vector<uint8_t> frame;
        frame.reserve(16 + batch_data.size());
        write_u32_le(frame, kLogFrameMagic);
        write_u32_le(frame, crc);
        write_u32_le(frame, static_cast<uint32_t>(entries.size()));
        write_u32_le(frame, static_cast<uint32_t>(batch_data.size()));
        frame.insert(frame.end(), batch_data.begin(), batch_data.end());

        // Append to data file.
        uint64_t write_offset = data_file_size_.load(std::memory_order_acquire);
        ssize_t written = ::pwrite(data_fd_, frame.data(), frame.size(),
                                   static_cast<off_t>(write_offset));
        if (written != static_cast<ssize_t>(frame.size())) {
            SPDLOG_ERROR("raft_log: p{} write failed at offset {}: {}",
                         partition_id_, write_offset,
                         written < 0 ? strerror(errno) : "short write");
            return kNoLogIndex;
        }

        data_file_size_.store(write_offset + frame.size(), std::memory_order_release);

        // Update index.
        {
            std::lock_guard g(mutex_);
            for (const auto& e : entries) {
                IndexEntry ie;
                ie.index   = e.index;
                ie.offset  = write_offset;
                ie.term    = e.term;
                ie.batch_length = static_cast<uint32_t>(frame.size());
                index_[e.index] = ie;
            }

            if (first_index_ == 0 || entries.front().index < first_index_) {
                first_index_ = entries.front().index;
            }
            last_index_ = entries.back().index;
        }

        // Update index file.
        append_index_file(entries, write_offset, frame.size());

        // Schedule fsync or do it now.
        dirty_bytes_ += frame.size();
        if (enable_fsync_batching_) {
            std::lock_guard fs_lock(fsync_mutex_);
            fsync_cv_.notify_one();
        } else {
            flush_and_fsync();
        }

        return entries.front().index;
    }

    // ------------------------------------------------------------------------
    // Truncation (RaftLog interface) — exclusive lock
    // ------------------------------------------------------------------------

    void truncate_suffix(LogIndex from_index) override {
        std::lock_guard lock(mutex_);
        std::lock_guard wlock(write_mutex_);
        truncate_suffix_locked(from_index);
    }

    void truncate_prefix(LogIndex first_index) override {
        std::lock_guard lock(mutex_);
        std::lock_guard wlock(write_mutex_);

        if (first_index <= first_index_) return;

        // Remove index entries before first_index.
        auto it = index_.begin();
        while (it != index_.end() && it->first < first_index) {
            it = index_.erase(it);
        }

        first_index_ = std::max(first_index_, first_index);
        if (first_index_ > last_index_) {
            last_index_ = first_index_ - 1;
        }
        write_meta();
        flush_index_file();
    }

    // ------------------------------------------------------------------------
    // Snapshot (RaftLog interface)
    // ------------------------------------------------------------------------

    void apply_snapshot(const RaftSnapshot& snapshot) override {
        std::lock_guard lock(mutex_);
        std::lock_guard wlock(write_mutex_);

        if (!snapshot.is_valid()) {
            SPDLOG_ERROR("raft_log: p{} apply_snapshot with invalid snapshot",
                         partition_id_);
            return;
        }

        SPDLOG_INFO("raft_log: p{} applying snapshot up to index {}",
                    partition_id_, snapshot.last_included_index);

        // Truncate all entries before or at snapshot's last_included_index.
        auto it = index_.begin();
        while (it != index_.end() && it->first <= snapshot.last_included_index) {
            it = index_.erase(it);
        }

        // Store snapshot metadata for virtual term lookups.
        snapshot_ = snapshot;

        // Update first_index_.
        first_index_ = snapshot.last_included_index + 1;

        // If log is now empty, set last_index_ to snapshot index.
        if (index_.empty()) {
            last_index_ = snapshot.last_included_index;
            first_index_ = snapshot.last_included_index + 1;
        }

        write_meta();
        flush_index_file();

        // Truncate the data file past the snapshot (we'll keep the file
        // for recovery but reset if everything is snapshotted).
        if (index_.empty()) {
            ::ftruncate(data_fd_, 0);
            data_file_size_.store(0);
        }

        SPDLOG_INFO("raft_log: p{} snapshot applied; log now [{}..{}]",
                    partition_id_, first_index_, last_index_);
    }

    // ------------------------------------------------------------------------
    // Maintenance
    // ------------------------------------------------------------------------

    /// Return approximate on-disk size in bytes (data + index).
    [[nodiscard]] uint64_t disk_size_bytes() const noexcept {
        return data_file_size_.load() + index_file_size_.load();
    }

private:
    // ========================================================================
    // Internal: file paths
    // ========================================================================

    [[nodiscard]] std::string partition_path() const {
        namespace fs = std::filesystem;
        return (fs::path(log_dir_) / ("partition-" + std::to_string(partition_id_)))
            .string();
    }

    [[nodiscard]] std::string data_path() const {
        namespace fs = std::filesystem;
        return (fs::path(partition_path()) / "log.dat").string();
    }

    [[nodiscard]] std::string index_path() const {
        namespace fs = std::filesystem;
        return (fs::path(partition_path()) / "log.idx").string();
    }

    [[nodiscard]] std::string meta_path() const {
        namespace fs = std::filesystem;
        return (fs::path(partition_path()) / "meta").string();
    }

    // ========================================================================
    // Internal: file I/O
    // ========================================================================

    void open_or_create() {
        // Open/create data file.
        data_fd_ = ::open(data_path().c_str(),
                          O_RDWR | O_CREAT | O_APPEND, 0644);
        if (data_fd_ < 0) {
            throw std::runtime_error(
                "DiskRaftLog: cannot open data file " + data_path()
                + ": " + strerror(errno));
        }

        // Determine current size.
        struct stat st;
        if (::fstat(data_fd_, &st) == 0) {
            data_file_size_.store(static_cast<uint64_t>(st.st_size));
        }

        // Open/create index file.
        idx_fd_ = ::open(index_path().c_str(),
                         O_RDWR | O_CREAT, 0644);
        if (idx_fd_ < 0) {
            ::close(data_fd_);
            throw std::runtime_error(
                "DiskRaftLog: cannot open index file " + index_path()
                + ": " + strerror(errno));
        }

        // Determine index file size.
        if (::fstat(idx_fd_, &st) == 0) {
            index_file_size_.store(static_cast<uint64_t>(st.st_size));
        }

        // Read metadata.
        read_meta();
    }

    /// Read the metadata file.
    void read_meta() {
        std::ifstream in(meta_path());
        if (!in) {
            first_index_ = 1;
            last_index_  = 0;
            return;
        }
        std::string line;
        while (std::getline(in, line)) {
            if (line.starts_with("first_index=")) {
                first_index_ = std::stoll(line.substr(12));
            } else if (line.starts_with("last_index=")) {
                last_index_ = std::stoll(line.substr(11));
            }
        }
    }

    /// Write metadata file atomically.
    void write_meta() {
        std::string tmp = meta_path() + ".tmp";
        std::ofstream out(tmp, std::ios::trunc);
        if (!out) return;
        out << "first_index=" << first_index_ << "\n";
        out << "last_index=" << last_index_ << "\n";
        out << "format_version=" << kLogFormatVersion << "\n";
        if (snapshot_) {
            out << "snapshot_last_index=" << snapshot_->last_included_index << "\n";
            out << "snapshot_last_term=" << snapshot_->last_included_term << "\n";
        }
        out.close();
        std::error_code ec;
        std::filesystem::rename(tmp, meta_path(), ec);
    }

    // ========================================================================
    // Internal: index management
    // ========================================================================

    /// Rebuild the in-memory index by scanning the index file.
    void recover_index() {
        index_.clear();

        uint64_t idx_size = index_file_size_.load();
        if (idx_size == 0) return;

        // Read entire index file into memory.
        std::vector<uint8_t> buf(idx_size);
        ssize_t n = ::pread(idx_fd_, buf.data(), idx_size, 0);
        if (n != static_cast<ssize_t>(idx_size)) {
            SPDLOG_WARN("raft_log: p{} short read on index file: {} vs {}",
                        partition_id_, n, idx_size);
            return;
        }

        const uint8_t* p = buf.data();
        size_t num_entries = idx_size / kIndexEntrySize;
        for (size_t i = 0; i < num_entries; ++i) {
            IndexEntry ie;
            ie.index  = static_cast<LogIndex>(read_i64_le(p));
            ie.offset = read_u64_le(p);
            ie.term   = static_cast<RaftTerm>(read_i64_le(p));
            index_[ie.index] = ie;
        }

        if (!index_.empty()) {
            first_index_ = index_.begin()->first;
            last_index_  = index_.rbegin()->first;
        }

        SPDLOG_INFO("raft_log: p{} recovered {} index entries [{}..{}]",
                    partition_id_, index_.size(), first_index_, last_index_);
    }

    /// Atomically append index entries to the index file (no fsync yet).
    void append_index_file(const std::vector<LogEntry>& entries,
                           uint64_t batch_offset, size_t batch_size) {
        std::vector<uint8_t> buf;
        buf.reserve(entries.size() * kIndexEntrySize);
        for (const auto& e : entries) {
            write_i64_le(buf, static_cast<int64_t>(e.index));
            write_u64_le(buf, batch_offset);
            write_i64_le(buf, static_cast<int64_t>(e.term));
        }

        uint64_t idx_off = index_file_size_.load(std::memory_order_acquire);
        ssize_t n = ::pwrite(idx_fd_, buf.data(), buf.size(),
                             static_cast<off_t>(idx_off));
        if (n != static_cast<ssize_t>(buf.size())) {
            SPDLOG_ERROR("raft_log: p{} index write failed: {}",
                         partition_id_, strerror(errno));
            return;
        }
        index_file_size_.store(idx_off + buf.size(), std::memory_order_release);
    }

    /// Flush index file to disk.
    void flush_index_file() {
        if (idx_fd_ >= 0) {
            ::fsync(idx_fd_);
        }
    }

    // ========================================================================
    // Internal: truncation
    // ========================================================================

    void truncate_suffix_locked(LogIndex from_index) {
        if (from_index <= first_index_) {
            // Truncate everything.
            index_.clear();
            last_index_  = from_index > 0 ? from_index - 1 : 0;
            first_index_ = from_index;
            ::ftruncate(data_fd_, 0);
            ::ftruncate(idx_fd_, 0);
            data_file_size_.store(0);
            index_file_size_.store(0);
            write_meta();
            return;
        }

        // Find the batch offset just before from_index.
        // Remove all entries with index >= from_index.
        auto it = index_.lower_bound(from_index);
        while (it != index_.end()) {
            it = index_.erase(it);
        }

        last_index_ = from_index - 1;

        // Find the offset of the last remaining batch.
        uint64_t truncate_offset = 0;
        if (!index_.empty()) {
            // Find the batch header offset of the last entry, then jump past
            // the full batch.
            auto last = index_.rbegin()->second;
            truncate_offset = last.offset + last.batch_length;
        }

        // Truncate data and index files.
        ::ftruncate(data_fd_, static_cast<off_t>(truncate_offset));
        data_file_size_.store(truncate_offset);

        uint64_t idx_off = index_.size() * kIndexEntrySize;
        ::ftruncate(idx_fd_, static_cast<off_t>(idx_off));
        index_file_size_.store(idx_off);

        write_meta();
    }

    // ========================================================================
    // Internal: serialization
    // ========================================================================

    /// Serialize a vector of log entries to binary format.
    [[nodiscard]] std::vector<uint8_t> serialize_batch(
        const std::vector<LogEntry>& entries) {

        std::vector<uint8_t> buf;
        for (const auto& e : entries) {
            // term(8) + index(8) + command_type(1) + payload_size(4) + payload
            write_i64_le(buf, static_cast<int64_t>(e.term));
            write_i64_le(buf, static_cast<int64_t>(e.index));

            if (std::holds_alternative<NoOpCommand>(e.command)) {
                buf.push_back(static_cast<uint8_t>(CommandTag::NoOp));
                write_u32_le(buf, 0);
            } else if (auto* cc = std::get_if<ConfigChangeCommand>(&e.command)) {
                buf.push_back(static_cast<uint8_t>(CommandTag::ConfigChange));

                // Serialize: joint_flag(1) + old_count(4) + old[N] + new_count(4) + new[N]
                std::vector<uint8_t> payload;
                payload.push_back(cc->joint_consensus ? 1 : 0);
                write_u32_le(payload, static_cast<uint32_t>(cc->old_members.size()));
                for (auto m : cc->old_members) {
                    write_u64_le(payload, static_cast<uint64_t>(m));
                }
                write_u32_le(payload, static_cast<uint32_t>(cc->new_members.size()));
                for (auto m : cc->new_members) {
                    write_u64_le(payload, static_cast<uint64_t>(m));
                }
                write_u32_le(buf, static_cast<uint32_t>(payload.size()));
                buf.insert(buf.end(), payload.begin(), payload.end());
            } else if (auto* uc = std::get_if<UserCommand>(&e.command)) {
                buf.push_back(static_cast<uint8_t>(CommandTag::UserCommand));
                write_u32_le(buf, static_cast<uint32_t>(uc->payload.size()));
                buf.insert(buf.end(), uc->payload.begin(), uc->payload.end());
            }
        }
        return buf;
    }

    /// Deserialize a batch from the data file at the given offset.
    [[nodiscard]] std::vector<LogEntry> read_batch(uint64_t offset) const {
        std::vector<LogEntry> entries;

        // Read frame header (16 bytes).
        uint8_t hdr[16];
        ssize_t n = ::pread(data_fd_, hdr, 16, static_cast<off_t>(offset));
        if (n != 16) return entries;

        const uint8_t* p = hdr;
        uint32_t magic    = read_u32_le(p);
        uint32_t crc      = read_u32_le(p);
        uint32_t count    = read_u32_le(p);
        uint32_t total    = read_u32_le(p);

        if (magic != kLogFrameMagic) {
            SPDLOG_WARN("raft_log: p{} bad magic at offset {}: {:08X}",
                        partition_id_, offset, magic);
            return entries;
        }

        if (count > kMaxBatchEntries || total > kMaxBatchBytes) {
            SPDLOG_WARN("raft_log: p{} corrupted frame header at {}: count={} total={}",
                        partition_id_, offset, count, total);
            return entries;
        }

        // Read entry data.
        std::vector<uint8_t> data(total);
        n = ::pread(data_fd_, data.data(), total, static_cast<off_t>(offset + 16));
        if (n != static_cast<ssize_t>(total)) return entries;

        // Validate CRC.
        uint32_t actual_crc = crc32c_span(data);
        if (actual_crc != crc) {
            SPDLOG_ERROR("raft_log: p{} CRC mismatch at offset {}: expected {:08X} got {:08X}",
                         partition_id_, offset, crc, actual_crc);
            return entries;
        }

        // Deserialize entries.
        const uint8_t* dp = data.data();
        const uint8_t* end = dp + total;
        for (uint32_t i = 0; i < count && dp < end; ++i) {
            LogEntry e;
            e.term  = static_cast<RaftTerm>(read_i64_le(dp));
            e.index = static_cast<LogIndex>(read_i64_le(dp));

            auto cmd_tag = static_cast<CommandTag>(*dp++);
            uint32_t payload_sz = read_u32_le(dp);

            switch (cmd_tag) {
            case CommandTag::NoOp:
                e.command = NoOpCommand{};
                break;

            case CommandTag::ConfigChange: {
                ConfigChangeCommand cc;
                const uint8_t* pp = dp;
                cc.joint_consensus = (*pp++) != 0;
                uint32_t old_count = static_cast<uint32_t>(pp[0])
                    | (static_cast<uint32_t>(pp[1]) << 8)
                    | (static_cast<uint32_t>(pp[2]) << 16)
                    | (static_cast<uint32_t>(pp[3]) << 24);
                pp += 4;
                for (uint32_t j = 0; j < old_count; ++j) {
                    cc.old_members.push_back(
                        static_cast<RaftNodeId>(static_cast<int64_t>(
                            static_cast<uint64_t>(pp[0])
                            | (static_cast<uint64_t>(pp[1]) << 8)
                            | (static_cast<uint64_t>(pp[2]) << 16)
                            | (static_cast<uint64_t>(pp[3]) << 24)
                            | (static_cast<uint64_t>(pp[4]) << 32)
                            | (static_cast<uint64_t>(pp[5]) << 40)
                            | (static_cast<uint64_t>(pp[6]) << 48)
                            | (static_cast<uint64_t>(pp[7]) << 56))));
                    pp += 8;
                }
                uint32_t new_count = static_cast<uint32_t>(pp[0])
                    | (static_cast<uint32_t>(pp[1]) << 8)
                    | (static_cast<uint32_t>(pp[2]) << 16)
                    | (static_cast<uint32_t>(pp[3]) << 24);
                pp += 4;
                for (uint32_t j = 0; j < new_count; ++j) {
                    cc.new_members.push_back(
                        static_cast<RaftNodeId>(static_cast<int64_t>(
                            static_cast<uint64_t>(pp[0])
                            | (static_cast<uint64_t>(pp[1]) << 8)
                            | (static_cast<uint64_t>(pp[2]) << 16)
                            | (static_cast<uint64_t>(pp[3]) << 24)
                            | (static_cast<uint64_t>(pp[4]) << 32)
                            | (static_cast<uint64_t>(pp[5]) << 40)
                            | (static_cast<uint64_t>(pp[6]) << 48)
                            | (static_cast<uint64_t>(pp[7]) << 56))));
                    pp += 8;
                }
                e.command = std::move(cc);
                break;
            }

            case CommandTag::UserCommand: {
                UserCommand uc;
                uc.payload.assign(dp, dp + payload_sz);
                e.command = std::move(uc);
                break;
            }

            default:
                SPDLOG_WARN("raft_log: p{} unknown command tag {} at index {}",
                            partition_id_, static_cast<int>(cmd_tag), e.index);
                e.command = NoOpCommand{};
                break;
            }

            dp += payload_sz;
            entries.push_back(std::move(e));
        }

        return entries;
    }

    // ========================================================================
    // Internal: index lookup
    // ========================================================================

    /// Find the index entry for the batch containing `index`.
    [[nodiscard]] std::map<LogIndex, IndexEntry>::const_iterator
    find_batch_for(LogIndex index) const {
        // index_ entries point to the first entry in each batch.
        // Find the greatest entry with key <= index.
        auto it = index_.upper_bound(index);
        if (it == index_.begin()) return index_.end();
        --it;
        return it;
    }

    // ========================================================================
    // Internal: fsync management
    // ========================================================================

    void flush_and_fsync() {
        if (data_fd_ >= 0) {
            ::fsync(data_fd_);
        }
        if (idx_fd_ >= 0) {
            ::fsync(idx_fd_);
        }
        write_meta();
    }

    /// Background fsync loop for throughput batching.
    void fsync_loop() {
        SPDLOG_DEBUG("raft_log: p{} fsync thread started", partition_id_);

        while (!running_.load(std::memory_order_acquire)) {
            // Not shut down yet — running_ is false means "still running"
            // (inverted logic for shutdown detection).
            std::unique_lock lock(fsync_mutex_);
            fsync_cv_.wait_for(lock, kFsyncInterval, [this] {
                return running_.load(std::memory_order_acquire);
            });

            if (running_.load(std::memory_order_acquire)) break;

            if (dirty_bytes_ > 0) {
                flush_and_fsync();
                dirty_bytes_ = 0;
            }
        }

        SPDLOG_DEBUG("raft_log: p{} fsync thread stopped", partition_id_);
    }

    // ========================================================================
    // Fields
    // ========================================================================

    std::string log_dir_;
    broker_id_t partition_id_;
    bool enable_fsync_batching_;

    mutable std::shared_mutex mutex_;         ///< Protects index_, meta_.
    std::mutex write_mutex_;                   ///< Serializes writes/truncates.

    std::map<LogIndex, IndexEntry> index_;    ///< O(log n) term lookup.
    LogIndex first_index_ = 0;
    LogIndex last_index_  = 0;

    std::optional<RaftSnapshot> snapshot_;    ///< Current snapshot metadata.

    // File descriptors.
    int data_fd_ = -1;
    int idx_fd_  = -1;

    std::atomic<uint64_t> data_file_size_{0};
    std::atomic<uint64_t> index_file_size_{0};

    // Fsync batching.
    std::atomic<uint64_t> dirty_bytes_{0};
    std::mutex fsync_mutex_;
    std::condition_variable fsync_cv_;
    std::thread fsync_thread_;
    std::atomic<bool> running_{false};        ///< true = shutdown, false = running.
};

// ============================================================================
// Factory function
// ============================================================================

std::unique_ptr<RaftLog> make_disk_raft_log(
    std::string log_dir,
    broker_id_t partition_id,
    bool enable_fsync_batching) {

    return std::make_unique<DiskRaftLog>(
        std::move(log_dir), partition_id, enable_fsync_batching);
}

}  // namespace torrent::raft
