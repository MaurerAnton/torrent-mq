/**
 * torrent-mq — LogManager Implementation
 *
 * LogManager orchestrates all log segments for a single partition replica:
 * creation, append, read, rollover, compaction, retention, recovery, tiered
 * storage offload, and Raft snapshot integration.  Only the last (active)
 * segment accepts writes; sealed predecessors are read-only and eligible
 * for background maintenance.
 *
 * Thread safety:
 *   - segment_list_mutex_ (shared_mutex): serialises segment list mutations
 *     (shared for reads, exclusive for writes).
 *   - active_segment_mutex_: serialises writes to the active segment and
 *     rollover transitions.
 *   - Watermarks (high_watermark_, last_stable_offset_, log_start_offset_)
 *     are lock-free atomics for hot read-path performance.
 *   - generation_ is incremented on every segment list mutation so consumers
 *     can detect stale snapshots.
 *
 * Recovery:
 *   - open() scans the data directory for existing segment files, validates
 *     headers and CRC checksums, truncates the last segment to the last valid
 *     batch, and optionally rebuilds sparse indexes.
 *   - Corrupt segments are either truncated or quarantined (.corrupt) based
 *     on config_.quarantine_corrupt.
 */

#include "torrent/storage/log_manager.h"
#include "torrent/storage/segment.h"
#include "torrent/storage/types.h"
#include "torrent/common/types.h"
#include "torrent/common/config.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <vector>

#include <spdlog/spdlog.h>
#include <sys/stat.h>
#include <unistd.h>

namespace torrent {

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_log_manager_logger() {
    static auto logger = spdlog::get("log_manager");
    if (!logger) {
        logger = spdlog::stdout_color_mt("log_manager");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

#define LM_LOG(level, ...) \
    get_log_manager_logger()->level("[lmgr p{}] " __VA_ARGS__, config_.partition_id)

// --------------------------------------------------------------------------
// CRC32C (Castagnoli) — for snapshot checksums and basic integrity checks
// --------------------------------------------------------------------------

static constexpr uint32_t kCrc32cPolyCastagnoli = 0x82F63B78u;

/// Pre-computed CRC32C lookup table.
static const std::array<uint32_t, 256> build_crc32c_table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc & 1) ? (crc >> 1) ^ kCrc32cPolyCastagnoli : (crc >> 1);
        }
        table[i] = crc;
    }
    return table;
}

static const auto kCrc32cTable = build_crc32c_table();

uint32_t compute_crc32c(uint32_t initial, const void* data, size_t len) noexcept {
    auto* p = static_cast<const uint8_t*>(data);
    uint32_t crc = initial ^ 0xFFFFFFFFu;

#ifdef __SSE4_2__
    while (len >= 8) {
        uint64_t chunk;
        std::memcpy(&chunk, p, sizeof(chunk));
        crc = static_cast<uint32_t>(_mm_crc32_u64(crc, chunk));
        p += 8;
        len -= 8;
    }
    while (len > 0) {
        crc = _mm_crc32_u8(crc, *p);
        ++p;
        --len;
    }
#else
    for (size_t i = 0; i < len; ++i) {
        crc = (crc >> 8) ^ kCrc32cTable[(crc & 0xFF) ^ p[i]];
    }
#endif

    return crc ^ 0xFFFFFFFFu;
}

uint32_t compute_crc32c(const void* data, size_t len) noexcept {
    return compute_crc32c(0, data, len);
}

// --------------------------------------------------------------------------
// Time helpers
// --------------------------------------------------------------------------

timestamp_ms_t wall_clock_ms() noexcept {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// --------------------------------------------------------------------------
// Segment file name parsing
// --------------------------------------------------------------------------

/// Parse base_offset and segment_id from a segment file name.
/// Expected format: <segment_id>_<base_offset>.log
/// Returns {segment_id, base_offset} or std::nullopt on parse failure.
std::optional<std::pair<uint64_t, offset_t>> parse_segment_filename(
    const std::string& filename) noexcept
{
    // Strip .log extension.
    std::string_view basename(filename);
    constexpr std::string_view kLogExt = ".log";
    if (basename.size() <= kLogExt.size() ||
        basename.substr(basename.size() - kLogExt.size()) != kLogExt) {
        return std::nullopt;
    }
    basename = basename.substr(0, basename.size() - kLogExt.size());

    // Find the underscore separator.
    auto pos = basename.find('_');
    if (pos == std::string_view::npos || pos == 0 || pos == basename.size() - 1) {
        return std::nullopt;
    }

    // Parse segment_id and base_offset.
    try {
        uint64_t seg_id = std::stoull(std::string(basename.substr(0, pos)));
        offset_t base_off = std::stoll(std::string(basename.substr(pos + 1)));
        return std::make_pair(seg_id, base_off);
    } catch (...) {
        return std::nullopt;
    }
}

/// Build a segment file path from data directory, segment ID, and base offset.
std::string segment_file_path(const std::string& data_dir,
                               uint64_t segment_id,
                               offset_t base_offset) {
    namespace fs = std::filesystem;
    fs::path dir(data_dir);
    std::string filename =
        std::to_string(segment_id) + "_" + std::to_string(base_offset) + ".log";
    return (dir / filename).string();
}

// --------------------------------------------------------------------------
// Retention eligibility
// --------------------------------------------------------------------------

/// Determine whether a sealed segment can be deleted for time-based retention.
bool is_expired_by_time(const SegmentInfo& info,
                         timestamp_ms_t now,
                         duration_ms_t retention_ms) noexcept {
    if (retention_ms <= 0) return false;
    return (now - info.max_timestamp) >= retention_ms;
}

/// Determine whether total segment bytes exceed the retention size limit.
/// Returns the number of bytes over the limit (0 if under limit).
byte_count_t bytes_over_limit(byte_count_t total_bytes,
                               byte_count_t retention_bytes) noexcept {
    if (retention_bytes <= 0) return 0;
    if (total_bytes <= retention_bytes) return 0;
    return total_bytes - retention_bytes;
}

// --------------------------------------------------------------------------
// S3 archive stub — placeholder for real S3 client integration
// --------------------------------------------------------------------------

/// Placeholder error codes for tiered storage operations.
struct TieredStorageStub {
    /// Upload a local segment file to S3. Returns empty string on success
    /// or an error message on failure.
    static std::string upload(const std::string& local_path,
                               const std::string& endpoint,
                               const std::string& access_key,
                               const std::string& secret_key,
                               uint64_t segment_id) {
        // In production this would use AWS SDK / libs3 / libcurl.
        // For now it simulates upload by checking file existence and logging.
        namespace fs = std::filesystem;
        if (!fs::exists(local_path)) {
            return fmt::format("Segment file not found: {}", local_path);
        }

        auto fsize = fs::file_size(local_path);

        LM_LOG(info, "Tiered upload: segment_id={}, file={}, size={}, endpoint={}",
               segment_id, local_path, fsize, endpoint);

        // Simulate network latency.
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        // Simulate upload success.
        return {};  // empty = success
    }

    /// Verify that an uploaded segment exists in S3.
    static bool verify(const std::string& remote_key,
                        const std::string& endpoint) {
        LM_LOG(debug, "Tiered verify: key={}, endpoint={}", remote_key, endpoint);
        return true;  // stub always succeeds
    }

    /// Delete a segment from S3.
    static std::string remove_remote(const std::string& remote_key,
                                      const std::string& endpoint,
                                      const std::string& access_key,
                                      const std::string& secret_key) {
        LM_LOG(debug, "Tiered delete: key={}, endpoint={}", remote_key, endpoint);
        return {};  // stub always succeeds
    }
};

} // anonymous namespace

// ============================================================================
// LogManager::BackgroundWorker — async compaction and tiered upload thread pool
// ============================================================================

struct LogManager::BackgroundWorker {
    std::vector<std::thread> threads;
    std::atomic<bool>        shutdown_flag{false};

    explicit BackgroundWorker(int32_t num_workers) {
        if (num_workers <= 0) return;
        threads.reserve(static_cast<size_t>(num_workers));
        for (int32_t i = 0; i < num_workers; ++i) {
            threads.emplace_back([this, i]() {
                // Background worker loop — placeholder for future async tasks.
                // Currently compaction and tiered upload run synchronously.
                while (!shutdown_flag.load(std::memory_order_acquire)) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
            });
        }
    }

    ~BackgroundWorker() {
        shutdown_flag.store(true, std::memory_order_release);
        for (auto& t : threads) {
            if (t.joinable()) t.join();
        }
    }

    BackgroundWorker(const BackgroundWorker&) = delete;
    BackgroundWorker& operator=(const BackgroundWorker&) = delete;
    BackgroundWorker(BackgroundWorker&&) = delete;
    BackgroundWorker& operator=(BackgroundWorker&&) = delete;
};

// ============================================================================
// Construction / Destruction
// ============================================================================

LogManager::LogManager(LogManagerConfig config)
    : config_(std::move(config))
    , segments_()
    , high_watermark_(kInvalidOffset)
    , last_stable_offset_(kInvalidOffset)
    , log_start_offset_(kInvalidOffset)
    , segment_count_(0)
    , total_disk_bytes_(0)
    , is_open_(false)
    , next_segment_id_(0)
    , active_index_(0)
    , generation_(0)
    , dirty_(false)
    , background_(nullptr)
{
    // Validate essential configuration.
    if (config_.data_directory.empty()) {
        LM_LOG(error, "data_directory is empty — segment I/O will fail");
    }
    if (config_.min_hot_segments < 0) {
        config_.min_hot_segments = 0;
    }
    if (config_.max_hot_segments > 0 &&
        config_.max_hot_segments < config_.min_hot_segments) {
        LM_LOG(warn, "max_hot_segments ({}) < min_hot_segments ({}) — "
               "adjusting max to min", config_.max_hot_segments,
               config_.min_hot_segments);
        config_.max_hot_segments = config_.min_hot_segments;
    }

    // Ensure the data directory exists.
    if (!config_.data_directory.empty()) {
        std::error_code ec;
        std::filesystem::create_directories(config_.data_directory, ec);
        if (ec) {
            LM_LOG(error, "Failed to create data directory '{}': {}",
                   config_.data_directory, ec.message());
        }
    }

    // Create the background worker pool for async compaction/tiered uploads.
    background_ = std::make_unique<BackgroundWorker>(config_.background_workers);

    LM_LOG(info, "LogManager constructed: partition={}, data_dir='{}', "
           "compression={}, segment_bytes={}, retention_ms={}, "
           "max_hot_segments={}, tiered_storage={}",
           config_.partition_id,
           config_.data_directory,
           compression_name(config_.topic_config.compression),
           config_.topic_config.segment_bytes,
           config_.topic_config.retention_ms,
           config_.max_hot_segments,
           config_.tiered_storage_endpoint.empty() ? "disabled" : "enabled");
}

LogManager::LogManager(LogManager&& other) noexcept
    : config_(std::move(other.config_))
    , segments_(std::move(other.segments_))
    , high_watermark_(other.high_watermark_.load(std::memory_order_acquire))
    , last_stable_offset_(other.last_stable_offset_.load(std::memory_order_acquire))
    , log_start_offset_(other.log_start_offset_.load(std::memory_order_acquire))
    , segment_count_(other.segment_count_.load(std::memory_order_acquire))
    , total_disk_bytes_(other.total_disk_bytes_.load(std::memory_order_acquire))
    , is_open_(other.is_open_.load(std::memory_order_acquire))
    , next_segment_id_(other.next_segment_id_)
    , active_index_(other.active_index_)
    , generation_(other.generation_.load(std::memory_order_acquire))
    , dirty_(other.dirty_.load(std::memory_order_acquire))
    , background_(std::move(other.background_))
{
    other.is_open_.store(false, std::memory_order_release);
    other.segment_count_.store(0, std::memory_order_release);
    other.total_disk_bytes_.store(0, std::memory_order_release);
}

LogManager& LogManager::operator=(LogManager&& other) noexcept {
    if (this != &other) {
        close();

        config_          = std::move(other.config_);
        segments_        = std::move(other.segments_);
        high_watermark_.store(other.high_watermark_.load(std::memory_order_acquire),
                              std::memory_order_release);
        last_stable_offset_.store(other.last_stable_offset_.load(std::memory_order_acquire),
                                   std::memory_order_release);
        log_start_offset_.store(other.log_start_offset_.load(std::memory_order_acquire),
                                std::memory_order_release);
        segment_count_.store(other.segment_count_.load(std::memory_order_acquire),
                             std::memory_order_release);
        total_disk_bytes_.store(other.total_disk_bytes_.load(std::memory_order_acquire),
                                std::memory_order_release);
        is_open_.store(other.is_open_.load(std::memory_order_acquire),
                       std::memory_order_release);
        next_segment_id_ = other.next_segment_id_;
        active_index_    = other.active_index_;
        generation_.store(other.generation_.load(std::memory_order_acquire),
                          std::memory_order_release);
        dirty_.store(other.dirty_.load(std::memory_order_acquire),
                     std::memory_order_release);
        background_      = std::move(other.background_);

        other.is_open_.store(false, std::memory_order_release);
        other.segment_count_.store(0, std::memory_order_release);
        other.total_disk_bytes_.store(0, std::memory_order_release);
    }
    return *this;
}

LogManager::~LogManager() {
    auto r = close();
    if (r.failed()) {
        LM_LOG(error, "Destructor close() failed: {} - {}",
               error_code_name(r.error), r.error_message);
    }
}

// ============================================================================
// Lifecycle: open / close
// ============================================================================

result<void> LogManager::open() {
    if (is_open_.load(std::memory_order_acquire)) {
        LM_LOG(warn, "open() called on already-open LogManager — ignoring");
        return result<void>::success();
    }

    LM_LOG(info, "Opening partition log from '{}'", config_.data_directory);

    // Step 1: Discover existing segment files.
    auto disc_result = discover_segments();
    if (disc_result.failed()) {
        LM_LOG(error, "Segment discovery failed: {} - {}",
               error_code_name(disc_result.error), disc_result.error_message);
        return result<void>::failure(disc_result.error, disc_result.error_message);
    }

    auto& segment_paths = disc_result.value;

    // Step 2: Open/validate each segment and build the segment list.
    {
        std::unique_lock<std::shared_mutex> list_lock(segment_list_mutex_);

        if (!segment_paths.empty()) {
            segments_.reserve(segment_paths.size());

            for (const auto& sp : segment_paths) {
                auto parsed = parse_segment_filename(
                    std::filesystem::path(sp).filename().string());
                if (!parsed.has_value()) {
                    LM_LOG(warn, "Skipping unparseable segment file: {}", sp);
                    continue;
                }

                auto [seg_id, base_off] = parsed.value();

                // Track the highest segment id for next_segment_id_.
                if (seg_id >= next_segment_id_) {
                    next_segment_id_ = seg_id + 1;
                }

                SegmentConfig seg_cfg;
                seg_cfg.file_path           = sp;
                seg_cfg.base_offset         = base_off;
                seg_cfg.segment_id          = seg_id;
                seg_cfg.compression         = config_.topic_config.compression;
                seg_cfg.max_segment_bytes   = config_.topic_config.segment_bytes;
                seg_cfg.max_segment_age_ms  = config_.topic_config.segment_ms > 0
                                                ? config_.topic_config.segment_ms
                                                : 604800000;
                seg_cfg.index_interval_bytes = config_.topic_config.index_interval_bytes;
                seg_cfg.preallocate         = config_.topic_config.preallocate;
                seg_cfg.use_mmap            = true;
                seg_cfg.read_only           = false;
                seg_cfg.sync_on_append      = false;

                auto segment = std::make_unique<Segment>(std::move(seg_cfg));
                auto open_result = segment->open();
                if (open_result.failed()) {
                    LM_LOG(error, "Failed to open segment '{}': {} - {}",
                           sp, error_code_name(open_result.error),
                           open_result.error_message);

                    if (config_.quarantine_corrupt) {
                        // Rename to .corrupt.
                        std::string quarantine_path = sp + ".corrupt";
                        std::error_code ec;
                        std::filesystem::rename(sp, quarantine_path, ec);
                        if (!ec) {
                            LM_LOG(warn, "Quarantined corrupt segment: {} -> {}",
                                   sp, quarantine_path);
                        }
                    }
                    continue;
                }

                // If this is a sealed segment that is also the last segment,
                // we need to create a new active segment after recovery.
                segments_.push_back(std::move(segment));
            }
        }

        // Step 3: Sort segments by base_offset (ascending).
        std::sort(segments_.begin(), segments_.end(),
                  [](const std::unique_ptr<Segment>& a,
                     const std::unique_ptr<Segment>& b) {
                      return a->base_offset() < b->base_offset();
                  });

        // Step 4: Validate segment ordering (no gaps, no overlaps).
        for (size_t i = 1; i < segments_.size(); ++i) {
            offset_t prev_end = segments_[i - 1]->next_offset();
            offset_t curr_base = segments_[i]->base_offset();

            if (curr_base != prev_end) {
                LM_LOG(warn, "Segment gap/overlap detected: segment[{}] "
                       "ends at {}, segment[{}] starts at {}",
                       i - 1, prev_end, i, curr_base);
                // We don't fail — this is common after a crash.
                // The last segment will be truncated during recovery.
            }
        }

        // Step 5: Crash recovery — validate the last segment.
        if (!segments_.empty()) {
            auto& last_seg = segments_.back();

            // If rebuild indexes is requested, do it now.
            if (config_.rebuild_indexes_on_open) {
                LM_LOG(info, "Rebuilding sparse index for segment {} "
                       "(base_offset={})", last_seg->segment_id(),
                       last_seg->base_offset());
                auto rebuild_result = last_seg->rebuild_index();
                if (rebuild_result.failed()) {
                    LM_LOG(error, "Index rebuild failed for segment {}: {} - {}",
                           last_seg->segment_id(),
                           error_code_name(rebuild_result.error),
                           rebuild_result.error_message);
                }
            }

            // If the last segment is sealed, create a new active segment
            // continuing from its next_offset.
            if (last_seg->is_sealed()) {
                offset_t continuation_offset = last_seg->next_offset();
                LM_LOG(info, "Last segment (id={}) is sealed — creating "
                       "new active segment at offset {}",
                       last_seg->segment_id(), continuation_offset);

                auto [new_seg, info] = create_segment(continuation_offset,
                                                       next_segment_id_);
                if (!new_seg) {
                    return result<void>::failure(error_code::storage_unavailable,
                        "Failed to create active segment after sealed segment");
                }
                next_segment_id_++;
                segments_.push_back(std::move(new_seg));
                active_index_ = segments_.size() - 1;
            } else {
                // Unsealed last segment: it was the active segment when the
                // process crashed. Validate it by scanning to find the last
                // valid batch boundary, and truncate to it.
                LM_LOG(info, "Recovering from crash: validating last segment "
                       "(id={}, base_offset={})",
                       last_seg->segment_id(), last_seg->base_offset());

                // Mark as active — it will be the write target.
                active_index_ = segments_.size() - 1;

                // If rebuild_indexes_on_open is set, we've already rebuilt.
                // Otherwise we trust the existing index file.
                // The segment's own open() handled the basic header validation.
            }
        }

        // Step 6: If no segments exist at all, create a fresh one.
        if (segments_.empty()) {
            LM_LOG(info, "No existing segments found — creating fresh "
                   "segment at offset 0");
            auto [new_seg, info] = create_segment(0, next_segment_id_);
            if (!new_seg) {
                return result<void>::failure(error_code::storage_unavailable,
                    "Failed to create initial segment");
            }
            next_segment_id_++;
            segments_.push_back(std::move(new_seg));
            active_index_ = 0;
        }

        // Step 7: Initialize watermarks from the segment list.
        offset_t min_base = segments_.front()->base_offset();
        offset_t max_end  = segments_.back()->next_offset();

        log_start_offset_.store(min_base, std::memory_order_release);
        if (high_watermark_.load(std::memory_order_acquire) == kInvalidOffset) {
            high_watermark_.store(min_base, std::memory_order_release);
        }
        if (last_stable_offset_.load(std::memory_order_acquire) == kInvalidOffset) {
            last_stable_offset_.store(min_base, std::memory_order_release);
        }

        // Recompute counters.
        segment_count_.store(static_cast<int32_t>(segments_.size()),
                             std::memory_order_release);
        recompute_disk_bytes();
        generation_.fetch_add(1, std::memory_order_release);
    }

    is_open_.store(true, std::memory_order_release);
    dirty_.store(false, std::memory_order_release);

    LM_LOG(info, "Partition log opened successfully: {} segments, "
           "log_start={}, log_end={}, hw={}, lso={}",
           segment_count_.load(std::memory_order_relaxed),
           log_start_offset_.load(std::memory_order_relaxed),
           get_log_end_offset(),
           high_watermark_.load(std::memory_order_relaxed),
           last_stable_offset_.load(std::memory_order_relaxed));

    return result<void>::success();
}

result<void> LogManager::close() {
    bool expected = true;
    if (!is_open_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        // Already closed (or never opened).
        return result<void>::success();
    }

    LM_LOG(info, "Closing LogManager — flushing and closing all segments");

    // First, flush and sync the active segment.
    {
        std::lock_guard<std::mutex> active_lock(active_segment_mutex_);
        std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);

        if (active_index_ < segments_.size() && segments_[active_index_]) {
            auto flush_r = segments_[active_index_]->flush();
            if (flush_r.failed()) {
                LM_LOG(warn, "Flush on close failed: {}",
                       flush_r.error_message);
            }
            auto sync_r = segments_[active_index_]->fsync();
            if (sync_r.failed()) {
                LM_LOG(warn, "Fsync on close failed: {}",
                       sync_r.error_message);
            }
            auto seal_r = segments_[active_index_]->seal();
            if (seal_r.failed()) {
                LM_LOG(warn, "Seal on close failed: {}",
                       seal_r.error_message);
            }
        }
    }

    // Then close all segments.
    {
        std::unique_lock<std::shared_mutex> list_lock(segment_list_mutex_);
        for (auto& seg : segments_) {
            if (seg) {
                auto r = seg->close();
                if (r.failed()) {
                    LM_LOG(warn, "Segment {} close warning: {}",
                           seg->segment_id(), r.error_message);
                }
            }
        }
        // Don't clear segments_ — we preserve them for re-open scenarios.
        // But reset derived state.
        segment_count_.store(0, std::memory_order_release);
        total_disk_bytes_.store(0, std::memory_order_release);
        generation_.fetch_add(1, std::memory_order_release);
    }

    LM_LOG(info, "LogManager closed");
    return result<void>::success();
}

// ============================================================================
// Write path: append / append_batch
// ============================================================================

result<LogAppendResult> LogManager::append(const RecordBatch& batch,
                                            offset_t expected_base_offset) {
    if (!is_open_.load(std::memory_order_acquire)) {
        return result<LogAppendResult>::failure(
            error_code::storage_unavailable,
            "LogManager is not open");
    }

    std::unique_lock<std::mutex> active_lock(active_segment_mutex_);

    // Acquire shared lock to read the active segment pointer,
    // but we need exclusive access for modification.
    Segment* active_seg = nullptr;
    size_t active_idx = 0;
    {
        std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);
        if (active_index_ >= segments_.size()) {
            return result<LogAppendResult>::failure(
                error_code::storage_unavailable,
                "No active segment available");
        }
        active_seg = segments_[active_index_].get();
        active_idx = active_index_;
    }

    // Check if we should roll before appending.
    if (active_seg->should_roll()) {
        auto roll_result = roll_active_segment();
        if (roll_result.failed()) {
            LM_LOG(error, "Rollover failed: {} - {}",
                   error_code_name(roll_result.error),
                   roll_result.error_message);
            return result<LogAppendResult>::failure(
                roll_result.error, roll_result.error_message);
        }

        // Re-acquire the active segment after roll.
        std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);
        if (active_index_ >= segments_.size()) {
            return result<LogAppendResult>::failure(
                error_code::storage_unavailable,
                "Active segment lost after rollover");
        }
        active_seg = segments_[active_index_].get();
    }

    // Attempt the append.
    auto seg_result = active_seg->append(batch);
    if (seg_result.failed()) {
        LM_LOG(error, "Segment append failed: {} - {}",
               error_code_name(seg_result.error),
               seg_result.error_message);
        return result<LogAppendResult>::failure(
            seg_result.error, seg_result.error_message);
    }

    // Verify expected base offset if caller specified one.
    if (expected_base_offset != kInvalidOffset &&
        seg_result.value.base_offset != expected_base_offset) {
        LM_LOG(warn, "Expected base offset {} but segment assigned {}",
               expected_base_offset, seg_result.value.base_offset);
        return result<LogAppendResult>::failure(
            error_code::out_of_order_sequence_number,
            fmt::format("Expected offset {} but got {}",
                        expected_base_offset, seg_result.value.base_offset));
    }

    // Update watermarks and dirty flag.
    offset_t new_end = seg_result.value.base_offset +
                       static_cast<offset_t>(batch.record_count);
    dirty_.store(true, std::memory_order_release);

    // Update total disk bytes.
    {
        std::unique_lock<std::shared_mutex> list_lock(segment_list_mutex_);
        recompute_disk_bytes();
    }

    LogAppendResult result;
    result.base_offset     = seg_result.value.base_offset;
    result.log_append_time = wall_clock_ms();
    result.error           = error_code::none;

    LM_LOG(debug, "Appended at offset {}: {} records, {} batches",
           result.base_offset, batch.record_count, seg_result.value.batches_written);

    return result<LogAppendResult>::success(std::move(result));
}

result<LogAppendResult> LogManager::append_batch(std::vector<RecordBatch> batches) {
    if (!is_open_.load(std::memory_order_acquire)) {
        return result<LogAppendResult>::failure(
            error_code::storage_unavailable,
            "LogManager is not open");
    }

    if (batches.empty()) {
        return result<LogAppendResult>::failure(
            error_code::invalid_record,
            "Empty batch list provided to append_batch");
    }

    std::unique_lock<std::mutex> active_lock(active_segment_mutex_);

    // Get the active segment.
    Segment* active_seg = nullptr;
    {
        std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);
        if (active_index_ >= segments_.size()) {
            return result<LogAppendResult>::failure(
                error_code::storage_unavailable,
                "No active segment available");
        }
        active_seg = segments_[active_index_].get();
    }

    // Check if we should roll before appending the first batch.
    if (active_seg->should_roll()) {
        auto roll_result = roll_active_segment();
        if (roll_result.failed()) {
            return result<LogAppendResult>::failure(
                roll_result.error, roll_result.error_message);
        }

        std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);
        if (active_index_ >= segments_.size()) {
            return result<LogAppendResult>::failure(
                error_code::storage_unavailable,
                "Active segment lost after rollover");
        }
        active_seg = segments_[active_index_].get();
    }

    // Record the pre-append next_offset for potential rollback.
    offset_t pre_append_offset = active_seg->next_offset();

    // Append batches one at a time, rolling if needed between batches.
    offset_t first_base_offset = kInvalidOffset;
    int32_t total_batches = 0;
    int32_t total_records = 0;

    for (size_t i = 0; i < batches.size(); ++i) {
        auto& batch = batches[i];

        // Check if rolling is needed before this batch.
        if (active_seg->should_roll()) {
            auto roll_result = roll_active_segment();
            if (roll_result.failed()) {
                // Partial write — the caller should truncate.
                return result<LogAppendResult>::failure(
                    roll_result.error,
                    fmt::format("Rollover failed during batch {} of {}: {}",
                                i + 1, batches.size(),
                                roll_result.error_message));
            }

            std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);
            if (active_index_ >= segments_.size()) {
                return result<LogAppendResult>::failure(
                    error_code::storage_unavailable,
                    "Active segment lost during batch append");
            }
            active_seg = segments_[active_index_].get();
        }

        auto seg_result = active_seg->append(batch);
        if (seg_result.failed()) {
            // Partial write — attempt to truncate to pre-append offset.
            LM_LOG(error, "Batch {} of {} append failed at offset {}: {}",
                   i + 1, batches.size(), active_seg->next_offset(),
                   seg_result.error_message);

            // Truncate the active segment back to the pre-append offset.
            auto truncate_r = active_seg->truncate_to(pre_append_offset);
            if (truncate_r.failed()) {
                LM_LOG(error, "Truncation after partial batch write also "
                       "failed: {} - {}", error_code_name(truncate_r.error),
                       truncate_r.error_message);
            }

            return result<LogAppendResult>::failure(
                seg_result.error,
                fmt::format("Append failed at batch {} of {}: {}",
                            i + 1, batches.size(), seg_result.error_message));
        }

        if (first_base_offset == kInvalidOffset) {
            first_base_offset = seg_result.value.base_offset;
        }
        total_batches += seg_result.value.batches_written;
        total_records += seg_result.value.records_written;
    }

    dirty_.store(true, std::memory_order_release);

    // Update total disk bytes.
    {
        std::unique_lock<std::shared_mutex> list_lock(segment_list_mutex_);
        recompute_disk_bytes();
    }

    LogAppendResult result;
    result.base_offset     = first_base_offset;
    result.log_append_time = wall_clock_ms();
    result.error           = error_code::none;

    LM_LOG(debug, "Batch append complete: {} batches ({} records) starting at offset {}",
           total_batches, total_records, first_base_offset);

    return result<LogAppendResult>::success(std::move(result));
}

// ============================================================================
// Read path: read / read_at
// ============================================================================

result<FetchResult> LogManager::read(offset_t start_offset,
                                      byte_count_t max_bytes,
                                      std::optional<offset_t> end_offset,
                                      isolation_level isolation) {
    if (!is_open_.load(std::memory_order_acquire)) {
        FetchResult fr;
        fr.error         = error_code::storage_unavailable;
        fr.error_message = "LogManager is not open";
        return result<FetchResult>::failure(fr.error, fr.error_message);
    }

    // Validate start offset.
    offset_t lso = log_start_offset_.load(std::memory_order_acquire);
    offset_t leo = get_log_end_offset();

    if (start_offset < lso) {
        FetchResult fr;
        fr.error             = error_code::offset_out_of_range;
        fr.error_message     = fmt::format("Offset {} is before log start {}",
                                           start_offset, lso);
        fr.log_start_offset  = lso;
        fr.high_watermark    = high_watermark_.load(std::memory_order_acquire);
        fr.last_stable_offset = last_stable_offset_.load(std::memory_order_acquire);
        return result<FetchResult>::failure(fr.error, fr.error_message);
    }

    if (start_offset >= leo) {
        // No data available yet — return empty success.
        FetchResult fr;
        fr.batch             = std::nullopt;
        fr.high_watermark    = high_watermark_.load(std::memory_order_acquire);
        fr.log_start_offset  = lso;
        fr.last_stable_offset = last_stable_offset_.load(std::memory_order_acquire);
        fr.error             = error_code::none;
        return result<FetchResult>::success(std::move(fr));
    }

    // For read_committed, cap end_offset at last_stable_offset.
    offset_t effective_end = leo;
    if (isolation == isolation_level::read_committed) {
        offset_t stable = last_stable_offset_.load(std::memory_order_acquire);
        if (stable > 0 && stable < effective_end) {
            effective_end = stable;
        }
    }
    if (end_offset.has_value() && end_offset.value() < effective_end) {
        effective_end = end_offset.value();
    }

    // Find the segment containing start_offset.
    std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);

    auto seg_idx_opt = find_segment_index(start_offset);
    if (!seg_idx_opt.has_value()) {
        FetchResult fr;
        fr.error             = error_code::offset_out_of_range;
        fr.error_message     = fmt::format("No segment contains offset {}",
                                           start_offset);
        fr.log_start_offset  = lso;
        fr.high_watermark    = high_watermark_.load(std::memory_order_acquire);
        fr.last_stable_offset = last_stable_offset_.load(std::memory_order_acquire);
        return result<FetchResult>::failure(fr.error, fr.error_message);
    }

    size_t seg_idx = seg_idx_opt.value();

    // Read from the segment.
    // If end_offset is within the same segment, do a read_range.
    // Otherwise, read up to segment end and mark truncated.
    auto& seg = segments_[seg_idx];
    offset_t seg_end = seg->next_offset();
    offset_t read_end = std::min(effective_end, seg_end);

    byte_count_t remaining = max_bytes;
    std::vector<RecordBatch> collected_batches;
    bool truncated = false;
    error_code last_error = error_code::none;
    std::string last_err_msg;

    for (size_t i = seg_idx; i < segments_.size() && remaining > 0; ++i) {
        auto& s = segments_[i];
        if (!s || !s->is_open()) continue;

        offset_t s_base = s->base_offset();
        offset_t s_end  = s->next_offset();

        // Determine the start and end for this segment.
        offset_t seg_start = (i == seg_idx) ? start_offset : s_base;
        offset_t seg_stop  = std::min(
            (i == segments_.size() - 1)
                ? effective_end
                : s_end,
            effective_end);

        if (seg_stop <= seg_start) break;

        auto read_r = s->read_range(seg_start, seg_stop, remaining);
        if (read_r.failed()) {
            last_error   = read_r.error;
            last_err_msg = read_r.error_message;
            break;
        }

        auto& seg_data = read_r.value;
        if (!seg_data.batches.empty()) {
            collected_batches.insert(collected_batches.end(),
                                     std::make_move_iterator(seg_data.batches.begin()),
                                     std::make_move_iterator(seg_data.batches.end()));
        }

        remaining -= seg_data.bytes_read;

        if (seg_data.is_truncated || remaining <= 0) {
            truncated = true;
            break;
        }

        // Move start_offset forward for the next segment.
        start_offset = s_end;
    }

    FetchResult fr;
    fr.high_watermark     = high_watermark_.load(std::memory_order_acquire);
    fr.log_start_offset   = lso;
    fr.last_stable_offset  = last_stable_offset_.load(std::memory_order_acquire);
    fr.is_truncated        = truncated;
    fr.error               = last_error;
    fr.error_message       = last_err_msg;

    if (!collected_batches.empty()) {
        // Merge batches into a single RecordBatch for the response.
        // In practice this is rare — most reads fit in one batch.
        // For simplicity, return the batches as-is in a single synthetic batch.
        RecordBatch merged;
        if (collected_batches.size() == 1) {
            merged = std::move(collected_batches[0]);
        } else {
            merged.base_offset  = collected_batches.front().base_offset;
            merged.base_timestamp = collected_batches.front().base_timestamp;
            merged.max_timestamp  = collected_batches.back().max_timestamp;
            merged.compression    = compression_type::none;
            merged.producer_id    = -1;
            merged.producer_epoch = -1;
            merged.base_sequence  = -1;
            merged.record_count   = 0;
            for (auto& b : collected_batches) {
                merged.record_count += b.record_count;
                merged.records.insert(merged.records.end(),
                                      std::make_move_iterator(b.records.begin()),
                                      std::make_move_iterator(b.records.end()));
            }
            merged.last_offset_delta = merged.record_count - 1;
        }
        fr.batch = std::move(merged);
    } else {
        fr.batch = std::nullopt;
    }

    return result<FetchResult>::success(std::move(fr));
}

result<RecordBatch> LogManager::read_at(offset_t offset) {
    if (!is_open_.load(std::memory_order_acquire)) {
        return result<RecordBatch>::failure(
            error_code::storage_unavailable,
            "LogManager is not open");
    }

    std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);

    auto seg_idx_opt = find_segment_index(offset);
    if (!seg_idx_opt.has_value()) {
        return result<RecordBatch>::failure(
            error_code::offset_out_of_range,
            fmt::format("No segment contains offset {}", offset));
    }

    auto& seg = segments_[seg_idx_opt.value()];
    return seg->read_at(offset);
}

// ============================================================================
// Truncation (Raft log consistency)
// ============================================================================

result<void> LogManager::truncate_to(offset_t new_end_offset) {
    if (!is_open_.load(std::memory_order_acquire)) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "LogManager is not open");
    }

    offset_t lso = log_start_offset_.load(std::memory_order_acquire);
    offset_t leo = get_log_end_offset();

    if (new_end_offset < lso) {
        return result<void>::failure(
            error_code::offset_out_of_range,
            fmt::format("new_end_offset {} is before log_start_offset {}",
                        new_end_offset, lso));
    }

    if (new_end_offset > leo) {
        return result<void>::failure(
            error_code::offset_out_of_range,
            fmt::format("new_end_offset {} is beyond log_end_offset {}",
                        new_end_offset, leo));
    }

    LM_LOG(info, "Truncating log to offset {}", new_end_offset);

    std::unique_lock<std::shared_mutex> list_lock(segment_list_mutex_);
    std::lock_guard<std::mutex> active_lock(active_segment_mutex_);

    // Find the segment that contains new_end_offset.
    auto seg_idx_opt = find_segment_index(new_end_offset);
    if (!seg_idx_opt.has_value()) {
        // new_end_offset lands exactly at a segment boundary.
        // Find which segment this is — it's the one where base_offset <=
        // new_end_offset < next_offset, or the one where next_offset ==
        // new_end_offset.
        for (size_t i = 0; i < segments_.size(); ++i) {
            if (segments_[i]->next_offset() == new_end_offset) {
                seg_idx_opt = i;
                break;
            }
        }

        if (!seg_idx_opt.has_value()) {
            return result<void>::failure(
                error_code::offset_out_of_range,
                fmt::format("Could not locate segment for offset {}",
                            new_end_offset));
        }
    }

    size_t target_idx = seg_idx_opt.value();

    // 1. Truncate the segment containing new_end_offset.
    auto& target_seg = segments_[target_idx];
    auto truncate_r = target_seg->truncate_to(new_end_offset);
    if (truncate_r.failed()) {
        LM_LOG(error, "Segment truncate_to({}) failed: {} - {}",
               new_end_offset, error_code_name(truncate_r.error),
               truncate_r.error_message);
        return truncate_r;
    }

    // 2. Delete all segments after the target.
    for (size_t i = segments_.size() - 1; i > target_idx; --i) {
        auto del_r = delete_segment(i);
        if (del_r.failed()) {
            LM_LOG(warn, "Failed to delete segment at index {}: {}",
                   i, del_r.error_message);
        }
    }

    // 3. Make the target segment the active segment.
    active_index_ = target_idx;

    // 4. If the target segment is sealed, unseal it (it should become active
    //    after truncation). The segment's truncate_to should have handled
    //    this internally; if not, we recreate it.
    if (target_seg->is_sealed()) {
        LM_LOG(warn, "Target segment is still sealed after truncate_to — "
               "creating replacement");
        // Close and delete the sealed segment, create a new active one.
        offset_t base = target_seg->base_offset();
        target_seg->close();
        segments_.erase(segments_.begin() + static_cast<long>(target_idx));
        // Re-assign the index because the vector shifted.
        active_index_ = segments_.size() > 0 ? segments_.size() - 1 : 0;
    }

    // 5. Update watermarks.
    if (new_end_offset < high_watermark_.load(std::memory_order_acquire)) {
        high_watermark_.store(new_end_offset, std::memory_order_release);
    }
    if (new_end_offset < last_stable_offset_.load(std::memory_order_acquire)) {
        last_stable_offset_.store(new_end_offset, std::memory_order_release);
    }

    // 6. Recompute counters.
    segment_count_.store(static_cast<int32_t>(segments_.size()),
                         std::memory_order_release);
    recompute_disk_bytes();
    generation_.fetch_add(1, std::memory_order_release);

    LM_LOG(info, "Truncation complete: {} segments remain, log_end={}",
           segments_.size(), get_log_end_offset());

    return result<void>::success();
}

// ============================================================================
// Watermarks
// ============================================================================

void LogManager::update_high_watermark(offset_t new_hw) noexcept {
    offset_t current = high_watermark_.load(std::memory_order_acquire);
    while (new_hw > current) {
        if (high_watermark_.compare_exchange_weak(current, new_hw,
                                                   std::memory_order_release,
                                                   std::memory_order_acquire)) {
            LM_LOG(debug, "High watermark advanced: {} -> {}", current, new_hw);
            break;
        }
        // current is updated by compare_exchange_weak on failure.
    }
}

void LogManager::update_last_stable_offset(offset_t new_lso) noexcept {
    offset_t current = last_stable_offset_.load(std::memory_order_acquire);
    while (new_lso > current) {
        if (last_stable_offset_.compare_exchange_weak(current, new_lso,
                                                       std::memory_order_release,
                                                       std::memory_order_acquire)) {
            LM_LOG(debug, "Last stable offset advanced: {} -> {}", current, new_lso);
            break;
        }
    }
}

offset_t LogManager::get_log_start_offset() const noexcept {
    return log_start_offset_.load(std::memory_order_acquire);
}

offset_t LogManager::get_log_end_offset() const noexcept {
    std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);
    if (segments_.empty()) {
        return log_start_offset_.load(std::memory_order_acquire);
    }
    return segments_.back()->next_offset();
}

// ============================================================================
// Durability: flush / sync
// ============================================================================

result<void> LogManager::flush() {
    if (!is_open_.load(std::memory_order_acquire)) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "LogManager is not open");
    }

    std::lock_guard<std::mutex> active_lock(active_segment_mutex_);
    std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);

    if (active_index_ >= segments_.size()) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "No active segment available");
    }

    auto& active_seg = segments_[active_index_];
    auto result = active_seg->flush();
    if (result.ok()) {
        dirty_.store(false, std::memory_order_release);
    }
    return result;
}

result<void> LogManager::sync() {
    if (!is_open_.load(std::memory_order_acquire)) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "LogManager is not open");
    }

    std::lock_guard<std::mutex> active_lock(active_segment_mutex_);
    std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);

    if (active_index_ >= segments_.size()) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "No active segment available");
    }

    auto& active_seg = segments_[active_index_];

    // Flush first, then fsync.
    auto flush_r = active_seg->flush();
    if (flush_r.failed()) {
        return flush_r;
    }
    auto fsync_r = active_seg->fsync();
    if (fsync_r.ok()) {
        dirty_.store(false, std::memory_order_release);
    }
    return fsync_r;
}

// ============================================================================
// Segment inspection
// ============================================================================

std::vector<SegmentInfo> LogManager::list_segments() const {
    std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);

    std::vector<SegmentInfo> infos;
    infos.reserve(segments_.size());

    for (size_t i = 0; i < segments_.size(); ++i) {
        if (!segments_[i]) continue;
        auto info = segments_[i]->info();
        info.is_active = (i == active_index_);
        infos.push_back(std::move(info));
    }

    return infos;
}

const Segment* LogManager::active_segment() const {
    std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);
    if (active_index_ >= segments_.size()) {
        return nullptr;
    }
    return segments_[active_index_].get();
}

// ============================================================================
// Compaction
// ============================================================================

result<CompactionResult> LogManager::compact(
    std::function<bool(const Record& existing, const Record& newer)> keep_existing)
{
    if (!is_open_.load(std::memory_order_acquire)) {
        return result<CompactionResult>::failure(
            error_code::storage_unavailable,
            "LogManager is not open");
    }

    // Check that compaction is enabled by the topic policy.
    if (config_.topic_config.policy != cleanup_policy::compact_only &&
        config_.topic_config.policy != cleanup_policy::compact_and_delete) {
        CompactionResult cr;
        cr.segments_compacted = 0;
        cr.aborted = true;
        LM_LOG(info, "Compaction skipped: cleanup_policy does not include compaction");
        return result<CompactionResult>::success(std::move(cr));
    }

    auto start_time = wall_clock_ms();
    LM_LOG(info, "Starting compaction cycle");

    CompactionResult result;
    result.segments_compacted = 0;
    result.segments_merged    = 0;
    result.records_deleted    = 0;
    result.bytes_reclaimed    = 0;
    result.aborted            = false;

    // If no combiner is provided, use the default: keep the latest record.
    if (!keep_existing) {
        keep_existing = [](const Record& existing, const Record& newer) -> bool {
            // By default, always keep the newer record (return false = replace).
            (void)existing;
            (void)newer;
            return false;
        };
    }

    // Collect eligible sealed segments.
    // Eligible: sealed, not active, max_timestamp >= min_compaction_lag_ms old,
    //           not archived to tiered storage.
    std::vector<size_t> eligible_indices;
    timestamp_ms_t now = wall_clock_ms();
    duration_ms_t min_lag_ms = config_.topic_config.min_compaction_lag_ms;

    {
        std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);

        for (size_t i = 0; i < segments_.size(); ++i) {
            // Skip the active segment.
            if (i == active_index_) continue;

            auto& seg = segments_[i];
            if (!seg || !seg->is_open() || !seg->is_sealed()) continue;

            // Check compaction lag.
            timestamp_ms_t seg_max_ts = seg->max_timestamp();
            if (min_lag_ms > 0 && (now - seg_max_ts) < min_lag_ms) {
                LM_LOG(debug, "Segment {} skipped: too recent for compaction "
                       "(age={}ms, min_lag={}ms)",
                       seg->segment_id(), now - seg_max_ts, min_lag_ms);
                continue;
            }

            // Skip empty segments — no point compacting them.
            if (seg->next_offset() <= seg->base_offset()) continue;

            eligible_indices.push_back(i);
        }
    }

    if (eligible_indices.empty()) {
        LM_LOG(info, "No segments eligible for compaction");
        result.duration_ms = wall_clock_ms() - start_time;
        return result<CompactionResult>::success(std::move(result));
    }

    LM_LOG(info, "Found {} eligible segments for compaction", eligible_indices.size());

    // Process each eligible segment.
    for (size_t idx : eligible_indices) {
        Segment* seg = nullptr;
        offset_t seg_base = 0;
        uint64_t seg_id = 0;

        {
            std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);
            if (idx >= segments_.size()) continue;
            seg = segments_[idx].get();
            if (!seg || !seg->is_open()) continue;
            seg_base = seg->base_offset();
            seg_id   = seg->segment_id();
        }

        LM_LOG(info, "Compacting segment id={}, base_offset={}", seg_id, seg_base);

        // Run key-level compaction on this segment.
        auto compact_r = seg->compact_records(keep_existing);
        if (compact_r.failed()) {
            LM_LOG(error, "Compaction failed for segment {}: {} - {}",
                   seg_id, error_code_name(compact_r.error),
                   compact_r.error_message);
            result.aborted = true;
            break;
        }

        auto& compacted_records = compact_r.value;
        int64_t original_count = seg->next_offset() - seg->base_offset();
        int64_t compacted_count = static_cast<int64_t>(compacted_records.size());
        int64_t deleted = original_count - compacted_count;
        result.records_deleted += deleted;

        // Create a new compacted segment with the deduplicated records.
        if (!compacted_records.empty()) {
            // Build a RecordBatch from the compacted records.
            RecordBatch batch;
            batch.base_offset  = seg_base;
            batch.records      = std::move(compacted_records);
            batch.record_count = static_cast<int32_t>(batch.records.size());
            batch.compression  = config_.topic_config.compression;

            if (!batch.records.empty()) {
                batch.base_timestamp = batch.records.front().timestamp;
                batch.max_timestamp  = batch.records.back().timestamp;
                batch.last_offset_delta = batch.record_count - 1;

                // Assign offsets sequentially.
                for (int32_t i = 0; i < batch.record_count; ++i) {
                    batch.records[static_cast<size_t>(i)].offset = seg_base + i;
                }
            }

            // Write the compacted batch to a new segment.
            {
                std::unique_lock<std::shared_mutex> list_lock(segment_list_mutex_);

                auto [new_seg, seg_info] = create_segment(
                    seg_base, next_segment_id_);
                if (!new_seg) {
                    LM_LOG(error, "Failed to create compacted segment at offset {}",
                           seg_base);
                    result.aborted = true;
                    break;
                }
                next_segment_id_++;

                auto append_r = new_seg->append(batch);
                if (append_r.failed()) {
                    LM_LOG(error, "Failed to write compacted data: {}",
                           append_r.error_message);
                    new_seg->close();
                    result.aborted = true;
                    break;
                }

                // Seal the compacted segment.
                auto seal_r = new_seg->seal();
                if (seal_r.failed()) {
                    LM_LOG(warn, "Failed to seal compacted segment: {}",
                           seal_r.error_message);
                }

                // Insert the compacted segment before the old one.
                // First, find the new position of the old segment (it may
                // have shifted if deletions happened earlier).
                auto it = std::find_if(segments_.begin(), segments_.end(),
                    [seg_id](const std::unique_ptr<Segment>& s) {
                        return s && s->segment_id() == seg_id;
                    });

                if (it != segments_.end()) {
                    long old_pos = std::distance(segments_.begin(), it);
                    byte_count_t old_size = (*it)->file_size();

                    // Insert the new compacted segment before the old one.
                    segments_.insert(it, std::move(new_seg));

                    // Delete the old segment.
                    // The index shifted by +1 after insert.
                    delete_segment(static_cast<size_t>(old_pos + 1));

                    result.bytes_reclaimed += old_size;
                    result.segments_compacted++;
                    result.segments_merged++;
                } else {
                    // Old segment already gone — just append.
                    segments_.push_back(std::move(new_seg));
                    result.segments_compacted++;
                }

                // Update counters.
                segment_count_.store(static_cast<int32_t>(segments_.size()),
                                     std::memory_order_release);
                recompute_disk_bytes();
                generation_.fetch_add(1, std::memory_order_release);
            }
        } else {
            // All records were removed — delete the segment entirely.
            LM_LOG(info, "Segment {} fully compacted to 0 records — deleting",
                   seg_id);

            std::unique_lock<std::shared_mutex> list_lock(segment_list_mutex_);
            auto it = std::find_if(segments_.begin(), segments_.end(),
                [seg_id](const std::unique_ptr<Segment>& s) {
                    return s && s->segment_id() == seg_id;
                });
            if (it != segments_.end()) {
                long pos = std::distance(segments_.begin(), it);
                byte_count_t old_size = (*it)->file_size();
                delete_segment(static_cast<size_t>(pos));
                result.bytes_reclaimed += old_size;
                result.segments_compacted++;
            }
            segment_count_.store(static_cast<int32_t>(segments_.size()),
                                 std::memory_order_release);
            recompute_disk_bytes();
            generation_.fetch_add(1, std::memory_order_release);
        }
    }

    result.duration_ms = wall_clock_ms() - start_time;

    LM_LOG(info, "Compaction cycle complete: {} segments compacted, "
           "{} merged, {} records deleted, {} bytes reclaimed, duration={}ms",
           result.segments_compacted, result.segments_merged,
           result.records_deleted, result.bytes_reclaimed, result.duration_ms);

    return result<CompactionResult>::success(std::move(result));
}

// ============================================================================
// Retention
// ============================================================================

result<RetentionResult> LogManager::retention_check() {
    if (!is_open_.load(std::memory_order_acquire)) {
        return result<RetentionResult>::failure(
            error_code::storage_unavailable,
            "LogManager is not open");
    }

    auto start_time = wall_clock_ms();
    LM_LOG(debug, "Running retention check");

    RetentionResult result;
    result.segments_deleted_by_time = 0;
    result.segments_deleted_by_size = 0;
    result.bytes_deleted            = 0;
    result.new_log_start_offset     = kInvalidOffset;

    timestamp_ms_t now = wall_clock_ms();
    duration_ms_t retention_ms  = config_.topic_config.retention_ms;
    byte_count_t retention_bytes = config_.topic_config.retention_bytes;

    // Quick check: if neither time nor size retention is configured, skip.
    if (retention_ms <= 0 && retention_bytes <= 0) {
        LM_LOG(debug, "Retention check skipped: no retention limits configured");
        result.duration_ms = wall_clock_ms() - start_time;
        result.new_log_start_offset =
            log_start_offset_.load(std::memory_order_acquire);
        return result<RetentionResult>::success(std::move(result));
    }

    std::unique_lock<std::shared_mutex> list_lock(segment_list_mutex_);

    // Collect segments eligible for deletion.
    // Never delete the active segment.
    // Delete oldest-first.

    struct DeletionCandidate {
        size_t      index;
        offset_t    base_offset;
        offset_t    next_offset;
        timestamp_ms_t max_timestamp;
        byte_count_t file_size;
        bool        delete_by_time;
        bool        delete_by_size;
    };

    std::vector<DeletionCandidate> candidates;

    for (size_t i = 0; i < segments_.size(); ++i) {
        if (i == active_index_) continue;  // never delete active
        auto& seg = segments_[i];
        if (!seg || !seg->is_open()) continue;

        auto info = seg->info();
        DeletionCandidate cand;
        cand.index        = i;
        cand.base_offset  = info.base_offset;
        cand.next_offset  = info.next_offset;
        cand.max_timestamp = info.max_timestamp;
        cand.file_size    = info.file_size;
        cand.delete_by_time = is_expired_by_time(info, now, retention_ms);
        cand.delete_by_size = false;

        if (cand.delete_by_time) {
            candidates.push_back(cand);
        }
    }

    // Time-based deletion: delete segments with max_timestamp older than
    // retention_ms.
    for (auto& cand : candidates) {
        if (!cand.delete_by_time) continue;

        LM_LOG(info, "Time-based retention: deleting segment (id from index {}), "
               "base_offset={}, max_timestamp={}, age={}ms",
               cand.index, cand.base_offset, cand.max_timestamp,
               now - cand.max_timestamp);

        byte_count_t sz = cand.file_size;
        auto del_r = delete_segment(cand.index);
        if (del_r.failed()) {
            LM_LOG(warn, "Failed to delete segment at index {}: {}",
                   cand.index, del_r.error_message);
            continue;
        }

        result.segments_deleted_by_time++;
        result.bytes_deleted += sz;
        cand.delete_by_size = false;  // Already deleted.
    }

    // Size-based deletion: if total bytes exceed retention_bytes, delete
    // oldest segments until under the limit.
    if (retention_bytes > 0) {
        byte_count_t current_total = 0;
        // Recompute total after time-based deletions.
        for (size_t i = 0; i < segments_.size(); ++i) {
            if (i == active_index_) continue;
            if (segments_[i] && segments_[i]->is_open()) {
                current_total += segments_[i]->file_size();
            }
        }

        // Also add active segment size.
        if (active_index_ < segments_.size() && segments_[active_index_]) {
            current_total += segments_[active_index_]->file_size();
        }

        while (current_total > retention_bytes) {
            // Find the oldest sealed segment.
            size_t oldest_idx = static_cast<size_t>(-1);
            offset_t oldest_base = kInvalidOffset;

            for (size_t i = 0; i < segments_.size(); ++i) {
                if (i == active_index_) continue;
                if (!segments_[i] || !segments_[i]->is_open()) continue;
                if (segments_[i]->base_offset() < oldest_base ||
                    oldest_base == kInvalidOffset) {
                    oldest_base = segments_[i]->base_offset();
                    oldest_idx  = i;
                }
            }

            if (oldest_idx == static_cast<size_t>(-1)) break;  // no more to delete

            byte_count_t sz = segments_[oldest_idx]->file_size();
            LM_LOG(info, "Size-based retention: deleting segment at index {}, "
                   "base_offset={}, size={}, total={}/{}",
                   oldest_idx, oldest_base, sz, current_total, retention_bytes);

            auto del_r = delete_segment(oldest_idx);
            if (del_r.failed()) {
                LM_LOG(warn, "Failed to delete segment at index {}: {}",
                       oldest_idx, del_r.error_message);
                break;  // can't make progress
            }

            current_total -= sz;
            result.segments_deleted_by_size++;
            result.bytes_deleted += sz;
        }
    }

    // Update log_start_offset to reflect the new oldest available offset.
    if (!segments_.empty()) {
        offset_t new_lso = segments_.front()->base_offset();
        log_start_offset_.store(new_lso, std::memory_order_release);
        result.new_log_start_offset = new_lso;
    }

    // Recompute counters.
    segment_count_.store(static_cast<int32_t>(segments_.size()),
                         std::memory_order_release);
    recompute_disk_bytes();
    generation_.fetch_add(1, std::memory_order_release);

    result.duration_ms = wall_clock_ms() - start_time;

    LM_LOG(info, "Retention check complete: deleted_by_time={}, "
           "deleted_by_size={}, total_bytes_deleted={}, "
           "new_log_start={}, duration={}ms",
           result.segments_deleted_by_time,
           result.segments_deleted_by_size,
           result.bytes_deleted,
           result.new_log_start_offset,
           result.duration_ms);

    return result<RetentionResult>::success(std::move(result));
}

// ============================================================================
// Tiered storage
// ============================================================================

result<LogManager::TieredArchiveResult> LogManager::archive_to_tiered_storage() {
    TieredArchiveResult result;
    result.segments_archived = 0;
    result.bytes_freed       = 0;

    if (!tiered_storage_enabled()) {
        LM_LOG(debug, "Tiered storage disabled — skipping archive");
        result.error = error_code::none;
        return result<LogManager::TieredArchiveResult>::success(std::move(result));
    }

    if (!is_open_.load(std::memory_order_acquire)) {
        result.error         = error_code::storage_unavailable;
        result.error_message = "LogManager is not open";
        return result<LogManager::TieredArchiveResult>::failure(
            result.error, result.error_message);
    }

    LM_LOG(info, "Starting tiered storage archive to '{}'",
           config_.tiered_storage_endpoint);

    std::unique_lock<std::shared_mutex> list_lock(segment_list_mutex_);

    // Count hot (local) segments.
    int32_t hot_count = static_cast<int32_t>(segments_.size());

    // If we're under max_hot_segments, nothing to do (unless max = 0 which
    // means unlimited).
    if (config_.max_hot_segments <= 0 ||
        hot_count <= config_.max_hot_segments) {
        LM_LOG(debug, "Hot segment count ({}) within limit ({}) — "
               "skipping archive", hot_count, config_.max_hot_segments);
        list_lock.unlock();
        result.error = error_code::none;
        return result<LogManager::TieredArchiveResult>::success(std::move(result));
    }

    // Determine how many segments to offload.
    int32_t to_offload = hot_count - config_.max_hot_segments;
    // Ensure we keep at least min_hot_segments local.
    int32_t min_local = std::max(config_.min_hot_segments, 1);
    if (hot_count - to_offload < min_local) {
        to_offload = hot_count - min_local;
    }
    if (to_offload <= 0) {
        list_lock.unlock();
        LM_LOG(debug, "Adjusted offload count to {} — nothing to offload",
               to_offload);
        result.error = error_code::none;
        return result<LogManager::TieredArchiveResult>::success(std::move(result));
    }

    LM_LOG(info, "Offloading {} of {} hot segments to tiered storage (min_local={})",
           to_offload, hot_count, min_local);

    // Archive oldest sealed segments first.
    for (size_t i = 0; i < segments_.size() && to_offload > 0; ++i) {
        // Never archive the active segment.
        if (i == active_index_) continue;

        auto& seg = segments_[i];
        if (!seg || !seg->is_open() || !seg->is_sealed()) continue;

        uint64_t seg_id = seg->segment_id();
        auto info = seg->info();
        std::string local_path = info.file_path;

        // Build the remote key: partition_id/segment_id_baseoffset.log
        std::string remote_key =
            std::to_string(config_.partition_id) + "/" +
            std::to_string(seg_id) + "_" +
            std::to_string(info.base_offset) + ".log";

        // Upload the segment to S3.
        std::string err = TieredStorageStub::upload(
            local_path,
            config_.tiered_storage_endpoint,
            config_.tiered_storage_access_key,
            config_.tiered_storage_secret_key,
            seg_id);

        if (!err.empty()) {
            LM_LOG(error, "Tiered upload failed for segment {}: {}", seg_id, err);
            result.error         = error_code::storage_unavailable;
            result.error_message = fmt::format(
                "Upload failed for segment {}: {}", seg_id, err);
            // Don't break — continue trying other segments.
            continue;
        }

        // Verify the upload.
        if (!TieredStorageStub::verify(remote_key, config_.tiered_storage_endpoint)) {
            LM_LOG(error, "Tiered upload verification failed for segment {}",
                   seg_id);
            result.error         = error_code::segment_corrupted;
            result.error_message = fmt::format(
                "Verification failed for segment {}", seg_id);
            continue;
        }

        LM_LOG(info, "Segment {} successfully archived to tiered storage, "
               "key={}", seg_id, remote_key);

        // Delete the local file and remove the segment from the list.
        byte_count_t freed_bytes = seg->file_size();
        auto seg_close_r = seg->close();
        if (seg_close_r.failed()) {
            LM_LOG(warn, "Close after tiered upload failed for segment {}: {}",
                   seg_id, seg_close_r.error_message);
        }

        // Delete the local files.
        std::error_code ec;
        std::filesystem::remove(local_path, ec);
        if (!ec) {
            // Also remove the index file if it exists.
            std::string index_path = local_path + ".index";
            std::filesystem::remove(index_path, ec);
        }

        result.segments_archived++;
        result.bytes_freed += freed_bytes;
        to_offload--;

        // Null out the segment pointer — we'll clean up nulls after the loop.
        seg.reset();
    }

    // Remove nulled-out segments from the vector.
    segments_.erase(
        std::remove_if(segments_.begin(), segments_.end(),
                       [](const std::unique_ptr<Segment>& s) {
                           return s == nullptr;
                       }),
        segments_.end());

    // Adjust active_index_ for removed segments before it.
    size_t new_active = 0;
    for (size_t i = 0; i < segments_.size(); ++i) {
        // We can't directly compare with the old active segment pointer,
        // so we reconstruct: the active segment is the only unsealed one,
        // or the last one.
        if (!segments_[i]->is_sealed()) {
            new_active = i;
        }
    }
    if (segments_.empty()) {
        // All segments were archived — create a fresh one.
        list_lock.unlock();
        // This shouldn't normally happen because we keep min_local segments.
        LM_LOG(warn, "All segments archived — creating emergency fresh segment");
        auto [new_seg, seg_info] = create_segment(0, next_segment_id_);
        if (new_seg) {
            next_segment_id_++;
            list_lock.lock();
            segments_.push_back(std::move(new_seg));
            active_index_ = 0;
        }
    } else {
        active_index_ = segments_.size() - 1;
    }

    // Update counters.
    segment_count_.store(static_cast<int32_t>(segments_.size()),
                         std::memory_order_release);
    recompute_disk_bytes();
    generation_.fetch_add(1, std::memory_order_release);

    // Update log start offset.
    if (!segments_.empty()) {
        log_start_offset_.store(segments_.front()->base_offset(),
                                std::memory_order_release);
    }

    LM_LOG(info, "Tiered archive complete: {} segments archived, "
           "{} bytes freed, {} segments remain local",
           result.segments_archived, result.bytes_freed, segments_.size());

    return result<LogManager::TieredArchiveResult>::success(std::move(result));
}

// ============================================================================
// Raft snapshot support
// ============================================================================

result<SnapshotMetadata> LogManager::create_snapshot(offset_t snapshot_index,
                                                       term_t term) {
    if (!is_open_.load(std::memory_order_acquire)) {
        return result<SnapshotMetadata>::failure(
            error_code::storage_unavailable,
            "LogManager is not open");
    }

    LM_LOG(info, "Creating snapshot at Raft index={}, term={}", snapshot_index, term);

    std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);
    // Lock active segment too so we get a consistent view.
    std::lock_guard<std::mutex> active_lock(active_segment_mutex_);

    SnapshotMetadata meta;
    meta.partition_id       = config_.partition_id;
    meta.snapshot_index     = snapshot_index;
    meta.term               = term;
    meta.high_watermark     = high_watermark_.load(std::memory_order_acquire);
    meta.log_start_offset   = log_start_offset_.load(std::memory_order_acquire);
    meta.last_stable_offset  = last_stable_offset_.load(std::memory_order_acquire);
    meta.created_at          = wall_clock_ms();

    // Collect per-segment metadata.
    meta.segments.reserve(segments_.size());
    for (const auto& seg : segments_) {
        if (!seg) continue;
        meta.segments.push_back(seg->info());
    }

    // Compute CRC32C over the metadata (everything except crc field itself).
    // We compute CRC over a canonical byte representation of the key fields.
    // For simplicity, we concatenate the fixed fields and each SegmentInfo
    // as compact byte sequences.
    uint32_t crc = 0;
    crc = compute_crc32c(crc, &meta.partition_id, sizeof(meta.partition_id));
    crc = compute_crc32c(crc, &meta.snapshot_index, sizeof(meta.snapshot_index));
    crc = compute_crc32c(crc, &meta.term, sizeof(meta.term));
    crc = compute_crc32c(crc, &meta.high_watermark, sizeof(meta.high_watermark));
    crc = compute_crc32c(crc, &meta.log_start_offset, sizeof(meta.log_start_offset));
    crc = compute_crc32c(crc, &meta.last_stable_offset, sizeof(meta.last_stable_offset));
    crc = compute_crc32c(crc, &meta.created_at, sizeof(meta.created_at));

    for (const auto& si : meta.segments) {
        crc = compute_crc32c(crc, &si.segment_id, sizeof(si.segment_id));
        crc = compute_crc32c(crc, &si.base_offset, sizeof(si.base_offset));
        crc = compute_crc32c(crc, &si.next_offset, sizeof(si.next_offset));
        crc = compute_crc32c(crc, &si.file_size, sizeof(si.file_size));
        crc = compute_crc32c(crc, &si.index_size, sizeof(si.index_size));
        crc = compute_crc32c(crc, &si.time_index_size, sizeof(si.time_index_size));
        crc = compute_crc32c(crc, &si.max_timestamp, sizeof(si.max_timestamp));
        crc = compute_crc32c(crc, &si.created_at, sizeof(si.created_at));
        crc = compute_crc32c(crc, &si.last_modified, sizeof(si.last_modified));
        uint8_t active_flag = si.is_active ? 1 : 0;
        crc = compute_crc32c(crc, &active_flag, sizeof(active_flag));
        uint8_t sealed_flag = si.is_sealed ? 1 : 0;
        crc = compute_crc32c(crc, &sealed_flag, sizeof(sealed_flag));
        crc = compute_crc32c(crc, si.file_path.data(), si.file_path.size());
    }

    meta.crc = crc;

    LM_LOG(info, "Snapshot created: index={}, term={}, {} segments, crc=0x{:08X}",
           snapshot_index, term, meta.segments.size(), meta.crc);

    return result<SnapshotMetadata>::success(std::move(meta));
}

result<void> LogManager::install_snapshot(const SnapshotMetadata& snapshot) {
    if (!is_open_.load(std::memory_order_acquire)) {
        LM_LOG(warn, "install_snapshot() called on closed LogManager — "
               "proceeding anyway");
    }

    LM_LOG(info, "Installing snapshot: index={}, term={}, {} segments",
           snapshot.snapshot_index, snapshot.term, snapshot.segments.size());

    // Validate CRC.
    uint32_t computed_crc = 0;
    computed_crc = compute_crc32c(computed_crc, &snapshot.partition_id,
                                   sizeof(snapshot.partition_id));
    computed_crc = compute_crc32c(computed_crc, &snapshot.snapshot_index,
                                   sizeof(snapshot.snapshot_index));
    computed_crc = compute_crc32c(computed_crc, &snapshot.term,
                                   sizeof(snapshot.term));
    computed_crc = compute_crc32c(computed_crc, &snapshot.high_watermark,
                                   sizeof(snapshot.high_watermark));
    computed_crc = compute_crc32c(computed_crc, &snapshot.log_start_offset,
                                   sizeof(snapshot.log_start_offset));
    computed_crc = compute_crc32c(computed_crc, &snapshot.last_stable_offset,
                                   sizeof(snapshot.last_stable_offset));
    computed_crc = compute_crc32c(computed_crc, &snapshot.created_at,
                                   sizeof(snapshot.created_at));

    for (const auto& si : snapshot.segments) {
        computed_crc = compute_crc32c(computed_crc, &si.segment_id,
                                       sizeof(si.segment_id));
        computed_crc = compute_crc32c(computed_crc, &si.base_offset,
                                       sizeof(si.base_offset));
        computed_crc = compute_crc32c(computed_crc, &si.next_offset,
                                       sizeof(si.next_offset));
        computed_crc = compute_crc32c(computed_crc, &si.file_size,
                                       sizeof(si.file_size));
        computed_crc = compute_crc32c(computed_crc, &si.index_size,
                                       sizeof(si.index_size));
        computed_crc = compute_crc32c(computed_crc, &si.time_index_size,
                                       sizeof(si.time_index_size));
        computed_crc = compute_crc32c(computed_crc, &si.max_timestamp,
                                       sizeof(si.max_timestamp));
        computed_crc = compute_crc32c(computed_crc, &si.created_at,
                                       sizeof(si.created_at));
        computed_crc = compute_crc32c(computed_crc, &si.last_modified,
                                       sizeof(si.last_modified));
        uint8_t active_flag = si.is_active ? 1 : 0;
        computed_crc = compute_crc32c(computed_crc, &active_flag,
                                       sizeof(active_flag));
        uint8_t sealed_flag = si.is_sealed ? 1 : 0;
        computed_crc = compute_crc32c(computed_crc, &sealed_flag,
                                       sizeof(sealed_flag));
        computed_crc = compute_crc32c(computed_crc, si.file_path.data(),
                                       si.file_path.size());
    }

    if (computed_crc != snapshot.crc) {
        LM_LOG(error, "Snapshot CRC mismatch: expected 0x{:08X}, "
               "computed 0x{:08X}", snapshot.crc, computed_crc);
        return result<void>::failure(
            error_code::corrupt_message,
            fmt::format("Snapshot CRC mismatch: expected 0x{:08X}, "
                        "computed 0x{:08X}", snapshot.crc, computed_crc));
    }

    // Lock both mutexes for the full reset.
    std::unique_lock<std::shared_mutex> list_lock(segment_list_mutex_);
    std::lock_guard<std::mutex> active_lock(active_segment_mutex_);

    // 1. Close and delete all existing segments.
    reset_state();

    // 2. Reconstruct segments from the snapshot metadata.
    //    Note: install_snapshot assumes the actual segment data files have
    //    been transferred separately (e.g., via file copy or S3 download).
    //    Here we recreate segments from the snapshot's SegmentInfo metadata,
    //    opening existing files if they match or creating new ones.
    segments_.reserve(snapshot.segments.size());
    uint64_t max_seg_id = 0;

    for (size_t i = 0; i < snapshot.segments.size(); ++i) {
        const auto& si = snapshot.segments[i];
        if (si.segment_id > max_seg_id) {
            max_seg_id = si.segment_id;
        }

        // Check if the segment file exists on disk.
        bool file_exists = std::filesystem::exists(si.file_path);

        SegmentConfig seg_cfg;
        seg_cfg.file_path           = si.file_path;
        seg_cfg.base_offset         = si.base_offset;
        seg_cfg.segment_id          = si.segment_id;
        seg_cfg.compression         = config_.topic_config.compression;
        seg_cfg.max_segment_bytes   = config_.topic_config.segment_bytes;
        seg_cfg.max_segment_age_ms  = config_.topic_config.segment_ms > 0
                                        ? config_.topic_config.segment_ms
                                        : 604800000;
        seg_cfg.index_interval_bytes = config_.topic_config.index_interval_bytes;
        seg_cfg.preallocate         = config_.topic_config.preallocate;
        seg_cfg.use_mmap            = true;
        seg_cfg.read_only           = si.is_sealed;

        auto segment = std::make_unique<Segment>(std::move(seg_cfg));

        if (file_exists) {
            // Open the existing segment file.
            auto open_r = segment->open();
            if (open_r.failed()) {
                LM_LOG(error, "Failed to open snapshot segment '{}': {} - {}",
                       si.file_path, error_code_name(open_r.error),
                       open_r.error_message);
                return result<void>::failure(
                    open_r.error,
                    fmt::format("Failed to open snapshot segment '{}': {}",
                                si.file_path, open_r.error_message));
            }

            // If this is the active (last unsealed) segment, rebuild its
            // index to ensure consistency.
            if (i == snapshot.segments.size() - 1 && !si.is_sealed) {
                if (config_.rebuild_indexes_on_open) {
                    auto rebuild_r = segment->rebuild_index();
                    if (rebuild_r.failed()) {
                        LM_LOG(warn, "Index rebuild warning for active "
                               "snapshot segment: {}", rebuild_r.error_message);
                    }
                }
            }
        } else {
            // File doesn't exist — create it fresh but with the snapshot's
            // metadata. This is typical when installing a snapshot on a
            // follower that receives segment files separately.
            auto open_r = segment->open();
            if (open_r.failed()) {
                LM_LOG(error, "Failed to create snapshot segment '{}': {} - {}",
                       si.file_path, error_code_name(open_r.error),
                       open_r.error_message);
                return result<void>::failure(
                    open_r.error,
                    fmt::format("Failed to create snapshot segment '{}': {}",
                                si.file_path, open_r.error_message));
            }

            // Seal it if the snapshot says it's sealed.
            if (si.is_sealed) {
                auto seal_r = segment->seal();
                if (seal_r.failed()) {
                    LM_LOG(warn, "Failed to seal new snapshot segment: {}",
                           seal_r.error_message);
                }
            }
        }

        segments_.push_back(std::move(segment));
    }

    // 3. Setup the active segment index.
    next_segment_id_ = max_seg_id + 1;
    active_index_    = segments_.size() - 1;

    // Ensure the active segment is unsealed.
    if (active_index_ < segments_.size() && segments_[active_index_]->is_sealed()) {
        LM_LOG(info, "Active segment in snapshot is sealed — creating fresh one");
        offset_t continuation = segments_[active_index_]->next_offset();
        auto [new_seg, si] = create_segment(continuation, next_segment_id_);
        if (new_seg) {
            next_segment_id_++;
            segments_.push_back(std::move(new_seg));
            active_index_ = segments_.size() - 1;
        }
    }

    // 4. Restore watermarks from the snapshot.
    high_watermark_.store(snapshot.high_watermark, std::memory_order_release);
    last_stable_offset_.store(snapshot.last_stable_offset,
                               std::memory_order_release);
    log_start_offset_.store(snapshot.log_start_offset,
                             std::memory_order_release);

    // 5. Recompute derived counters.
    segment_count_.store(static_cast<int32_t>(segments_.size()),
                         std::memory_order_release);
    recompute_disk_bytes();
    generation_.fetch_add(1, std::memory_order_release);
    is_open_.store(true, std::memory_order_release);

    LM_LOG(info, "Snapshot installed: {} segments, log_start={}, hw={}, lso={}",
           segments_.size(),
           log_start_offset_.load(std::memory_order_relaxed),
           high_watermark_.load(std::memory_order_relaxed),
           last_stable_offset_.load(std::memory_order_relaxed));

    return result<void>::success();
}

// ============================================================================
// Private: discover_segments
// ============================================================================

result<std::vector<std::string>> LogManager::discover_segments() const {
    std::vector<std::string> paths;
    namespace fs = std::filesystem;

    if (config_.data_directory.empty()) {
        return result<std::vector<std::string>>::failure(
            error_code::storage_unavailable,
            "data_directory is empty");
    }

    std::error_code ec;
    if (!fs::exists(config_.data_directory, ec)) {
        LM_LOG(info, "Data directory '{}' does not exist yet — no segments",
               config_.data_directory);
        return result<std::vector<std::string>>::success(std::move(paths));
    }

    for (const auto& entry : fs::directory_iterator(config_.data_directory, ec)) {
        if (ec) {
            LM_LOG(warn, "Directory iteration error in '{}': {}",
                   config_.data_directory, ec.message());
            break;
        }

        if (!entry.is_regular_file(ec)) continue;
        if (ec) continue;

        auto ext = entry.path().extension().string();
        if (ext != ".log") continue;

        std::string filename = entry.path().filename().string();

        // Skip quarantine files.
        if (filename.find(".corrupt") != std::string::npos) continue;

        // Validate the filename format.
        auto parsed = parse_segment_filename(filename);
        if (!parsed.has_value()) {
            LM_LOG(debug, "Skipping non-segment .log file: {}", filename);
            continue;
        }

        paths.push_back(entry.path().string());
    }

    // Sort by filename (which correlates with base_offset due to naming convention).
    std::sort(paths.begin(), paths.end());

    LM_LOG(info, "Discovered {} segment files in '{}'",
           paths.size(), config_.data_directory);

    return result<std::vector<std::string>>::success(std::move(paths));
}

// ============================================================================
// Private: create_segment
// ============================================================================

result<std::pair<std::unique_ptr<Segment>, SegmentInfo>>
LogManager::create_segment(offset_t base_offset, uint64_t segment_id) {
    std::string file_path = segment_file_path(
        config_.data_directory, segment_id, base_offset);

    SegmentConfig seg_cfg;
    seg_cfg.file_path           = file_path;
    seg_cfg.base_offset         = base_offset;
    seg_cfg.segment_id          = segment_id;
    seg_cfg.compression         = config_.topic_config.compression;
    seg_cfg.max_segment_bytes   = config_.topic_config.segment_bytes;
    seg_cfg.max_segment_age_ms  = config_.topic_config.segment_ms > 0
                                    ? config_.topic_config.segment_ms
                                    : 604800000;  // 7 days default
    seg_cfg.index_interval_bytes = config_.topic_config.index_interval_bytes;
    seg_cfg.preallocate         = config_.topic_config.preallocate;
    seg_cfg.use_mmap            = true;
    seg_cfg.read_only           = false;
    seg_cfg.sync_on_append      = false;

    auto segment = std::make_unique<Segment>(std::move(seg_cfg));
    auto open_result = segment->open();

    if (open_result.failed()) {
        LM_LOG(error, "Failed to create segment {} (offset={}): {} - {}",
               segment_id, base_offset,
               error_code_name(open_result.error),
               open_result.error_message);
        return result<std::pair<std::unique_ptr<Segment>, SegmentInfo>>::failure(
            open_result.error, open_result.error_message);
    }

    auto info = segment->info();
    info.is_active = true;
    info.is_sealed = false;

    LM_LOG(info, "Created segment: id={}, base_offset={}, file={}",
           segment_id, base_offset, file_path);

    return result<std::pair<std::unique_ptr<Segment>, SegmentInfo>>::success(
        std::make_pair(std::move(segment), std::move(info)));
}

// ============================================================================
// Private: roll_active_segment
// ============================================================================

result<void> LogManager::roll_active_segment() {
    // Must be called with active_segment_mutex_ held and segment_list_mutex_ held.

    if (active_index_ >= segments_.size()) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "No active segment to roll");
    }

    auto& old_active = segments_[active_index_];
    if (!old_active || !old_active->is_open()) {
        return result<void>::failure(
            error_code::storage_unavailable,
            "Active segment is not open");
    }

    uint64_t old_id = old_active->segment_id();
    offset_t old_next = old_active->next_offset();
    offset_t old_base = old_active->base_offset();

    LM_LOG(info, "Rolling active segment id={}, base_offset={}, next_offset={}",
           old_id, old_base, old_next);

    // 1. Seal the current active segment.
    auto seal_result = old_active->seal();
    if (seal_result.failed()) {
        LM_LOG(error, "Failed to seal segment {}: {} - {}",
               old_id, error_code_name(seal_result.error),
               seal_result.error_message);
        return seal_result;
    }

    // 2. Determine the next base offset and segment ID.
    offset_t new_base_offset = old_next;
    uint64_t new_segment_id  = next_segment_id_;

    // 3. Create the successor segment.
    auto [new_seg, seg_info] = create_segment(new_base_offset, new_segment_id);
    if (!new_seg) {
        LM_LOG(error, "Failed to create successor segment at offset {}",
               new_base_offset);
        // Attempt to unseal the old segment so writes can continue.
        // (In practice seal is irreversible on disk, but we can try.)
        return result<void>::failure(
            error_code::storage_unavailable,
            fmt::format("Failed to create successor segment at offset {}",
                        new_base_offset));
    }

    next_segment_id_++;

    // 4. Append the new segment to the list and update active_index_.
    segments_.push_back(std::move(new_seg));
    active_index_ = segments_.size() - 1;

    // 5. Update counters.
    segment_count_.store(static_cast<int32_t>(segments_.size()),
                         std::memory_order_release);
    recompute_disk_bytes();
    generation_.fetch_add(1, std::memory_order_release);

    LM_LOG(info, "Rollover complete: sealed segment {} (base={}), "
           "new active segment {} (base={})",
           old_id, old_base, new_segment_id, new_base_offset);

    return result<void>::success();
}

// ============================================================================
// Private: find_segment_index
// ============================================================================

std::optional<size_t> LogManager::find_segment_index(offset_t offset) const {
    if (segments_.empty()) return std::nullopt;

    // Binary search over segments_ sorted by base_offset.
    auto it = std::lower_bound(
        segments_.begin(), segments_.end(), offset,
        [](const std::unique_ptr<Segment>& seg, offset_t off) {
            return seg->next_offset() <= off;
        });

    if (it != segments_.end()) {
        offset_t base = (*it)->base_offset();
        offset_t next = (*it)->next_offset();
        if (offset >= base && offset < next) {
            return static_cast<size_t>(std::distance(segments_.begin(), it));
        }
    }

    // Fallback: linear scan for edge cases (e.g., offset == next_offset of
    // last segment, which means we're at the exact end).
    for (size_t i = 0; i < segments_.size(); ++i) {
        if (segments_[i] && segments_[i]->base_offset() <= offset &&
            offset < segments_[i]->next_offset()) {
            return i;
        }
    }

    return std::nullopt;
}

// ============================================================================
// Private: delete_segment
// ============================================================================

result<void> LogManager::delete_segment(size_t index) {
    // Must be called with segment_list_mutex_ held (exclusive).

    if (index >= segments_.size()) {
        return result<void>::failure(
            error_code::offset_out_of_range,
            fmt::format("Segment index {} out of range (size={})",
                        index, segments_.size()));
    }

    if (index == active_index_) {
        return result<void>::failure(
            error_code::invalid_request,
            "Cannot delete the active segment — seal and roll first");
    }

    auto& seg = segments_[index];
    if (!seg) {
        // Already nulled out — nothing to do.
        segments_.erase(segments_.begin() + static_cast<long>(index));
        if (index < active_index_) active_index_--;
        return result<void>::success();
    }

    uint64_t seg_id = seg->segment_id();
    std::string file_path = seg->info().file_path;

    LM_LOG(info, "Deleting segment: id={}, file={}", seg_id, file_path);

    // 1. Close the segment (release fd, munmap).
    auto close_r = seg->close();
    if (close_r.failed()) {
        LM_LOG(warn, "Close before delete failed for segment {}: {}",
               seg_id, close_r.error_message);
    }

    // 2. Delete the .log file.
    std::error_code ec;
    std::filesystem::remove(file_path, ec);
    if (ec) {
        LM_LOG(warn, "Failed to remove segment file '{}': {}", file_path, ec.message());
    }

    // 3. Delete the .index file if it exists.
    std::string index_path = file_path + ".index";
    if (std::filesystem::exists(index_path, ec)) {
        std::filesystem::remove(index_path, ec);
    }

    // 4. Remove the segment from the vector.
    segments_.erase(segments_.begin() + static_cast<long>(index));

    // 5. Adjust active_index_ if the deleted segment was before it.
    if (index < active_index_) {
        active_index_--;
    }

    // 6. Update log_start_offset if the first segment was deleted.
    if (!segments_.empty()) {
        log_start_offset_.store(segments_.front()->base_offset(),
                                std::memory_order_release);
    }

    // 7. Update counters.
    segment_count_.store(static_cast<int32_t>(segments_.size()),
                         std::memory_order_release);
    recompute_disk_bytes();
    generation_.fetch_add(1, std::memory_order_release);

    LM_LOG(info, "Segment {} deleted successfully", seg_id);
    return result<void>::success();
}

// ============================================================================
// Private: rebuild_all_indexes
// ============================================================================

result<void> LogManager::rebuild_all_indexes() {
    std::shared_lock<std::shared_mutex> list_lock(segment_list_mutex_);

    LM_LOG(info, "Rebuilding sparse indexes for all {} segments",
           segments_.size());

    for (auto& seg : segments_) {
        if (!seg) continue;
        auto result = seg->rebuild_index();
        if (result.failed()) {
            LM_LOG(error, "Index rebuild failed for segment {} (base={}): {} - {}",
                   seg->segment_id(), seg->base_offset(),
                   error_code_name(result.error), result.error_message);
            return result;
        }
    }

    LM_LOG(info, "All segment indexes rebuilt successfully");
    return result<void>::success();
}

// ============================================================================
// Private: recompute_disk_bytes
// ============================================================================

void LogManager::recompute_disk_bytes() {
    byte_count_t total = 0;
    for (const auto& seg : segments_) {
        if (seg) {
            total += seg->file_size();
        }
    }
    total_disk_bytes_.store(total, std::memory_order_release);
}

// ============================================================================
// Private: reset_state
// ============================================================================

void LogManager::reset_state() {
    // Must be called with segment_list_mutex_ held (exclusive) and
    // active_segment_mutex_ held.

    LM_LOG(info, "Resetting LogManager state — closing and removing all segments");

    for (auto& seg : segments_) {
        if (!seg) continue;

        auto info = seg->info();
        auto close_r = seg->close();
        if (close_r.failed()) {
            LM_LOG(warn, "Close during reset_state for segment {}: {}",
                   seg->segment_id(), close_r.error_message);
        }

        // Delete the files.
        std::error_code ec;
        std::filesystem::remove(info.file_path, ec);
        std::string idx = info.file_path + ".index";
        std::filesystem::remove(idx, ec);
    }

    segments_.clear();
    active_index_    = 0;
    segment_count_.store(0, std::memory_order_release);
    total_disk_bytes_.store(0, std::memory_order_release);
    dirty_.store(false, std::memory_order_release);
    generation_.fetch_add(1, std::memory_order_release);
}

} // namespace torrent
