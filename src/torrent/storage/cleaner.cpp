/**
 * torrent-mq — Log Cleaner (Retention Policy Engine)
 *
 * Implements LogCleaner: evaluates time-based and size-based retention
 * policies on sealed log segments.  Determines which segments should be
 * deleted, in what priority order, and never touches the active segment.
 *
 * Retention policies (from TopicConfig):
 *   Time-based:  Delete segments whose max_timestamp < (now - retention_ms).
 *                Inactive/empty partitions also expire after retention_ms.
 *   Size-based:  Delete oldest sealed segments first until total partition
 *                bytes ≤ retention_bytes.
 *
 * Priority ordering for deletion:
 *   1. Time-expired segments (oldest first).
 *   2. Size-excess segments (oldest first).
 *   3. Empty segments (zero records, sealed, oldest first).
 *   4. Orphaned .index/.timeindex files with no matching .log file.
 *
 * The cleaner is invoked periodically by the broker's RetentionManager
 * (broker/retention_manager.h) on a configurable timer interval.
 */

#include "torrent/storage/types.h"
#include "torrent/storage/cleaner.h"
#include "torrent/common/types.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <set>
#include <cmath>

#include <spdlog/spdlog.h>
#include <unistd.h>
#include <sys/stat.h>

namespace torrent {

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_cleaner_logger() {
    static auto logger = spdlog::get("cleaner");
    if (!logger) {
        logger = spdlog::stdout_color_mt("cleaner");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Time helpers
// --------------------------------------------------------------------------

timestamp_ms_t wall_clock_ms() noexcept {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// --------------------------------------------------------------------------
// File-system helpers
// --------------------------------------------------------------------------

/// Delete a file from disk.  Returns true on success, false on error.
/// Logs a warning on failure but does not throw.
bool delete_file(const std::string& path) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
        get_cleaner_logger()->warn("Failed to delete '{}': {}", path, ec.message());
        return false;
    }
    return true;
}

/// Get file size in bytes.  Returns -1 on error.
byte_count_t file_size_bytes(const std::string& path) {
    std::error_code ec;
    auto sz = std::filesystem::file_size(path, ec);
    if (ec) return -1;
    return static_cast<byte_count_t>(sz);
}

/// Check if a file exists and is a regular file.
bool file_exists(const std::string& path) {
    std::error_code ec;
    return std::filesystem::is_regular_file(path, ec);
}

} // anonymous namespace

// ============================================================================
// LogCleaner::CleanerConfig
// ============================================================================

LogCleaner::CleanerConfig::CleanerConfig() = default;

// ============================================================================
// LogCleaner::SegmentEligibility
// ============================================================================

LogCleaner::SegmentEligibility::SegmentEligibility() = default;

// ============================================================================
// LogCleaner::CleanerResult
// ============================================================================

bool LogCleaner::CleanerResult::ok() const noexcept {
    return error == error_code::none;
}

bool LogCleaner::CleanerResult::deleted_any() const noexcept {
    return segments_deleted_by_time > 0
        || segments_deleted_by_size > 0
        || segments_deleted_empty > 0
        || orphan_files_deleted > 0;
}

// ============================================================================
// LogCleaner — construction / destruction
// ============================================================================

LogCleaner::LogCleaner(CleanerConfig config)
    : config_(std::move(config))
{
    auto logger = get_cleaner_logger();
    logger->info("LogCleaner initialised: retention_ms={}, retention_bytes={}, "
                 "delete_orphans={}, dry_run={}",
                 config_.retention_ms, config_.retention_bytes,
                 config_.delete_orphan_indexes, config_.dry_run);
}

LogCleaner::~LogCleaner() = default;

// ============================================================================
// LogCleaner::evaluate_time_retention
// ============================================================================

LogCleaner::CleanerResult LogCleaner::evaluate_time_retention(
    const std::vector<SegmentInfo>& segments,
    timestamp_ms_t now) {

    CleanerResult result;
    if (config_.retention_ms <= 0) {
        // Infinite time retention — never delete by time.
        return result;
    }

    auto logger = get_cleaner_logger();

    for (const auto& seg : segments) {
        // Never delete the active segment.
        if (seg.is_active) continue;

        // Never delete segments that haven't been sealed.
        if (!seg.is_sealed) continue;

        // Check time retention.
        if (!should_delete_segment(seg, now)) continue;

        // Build eligibility record.
        SegmentEligibility eligibility;
        eligibility.info            = seg;
        eligibility.reason         = DeleteReason::time_expired;
        eligibility.age_ms          = now - seg.max_timestamp;
        eligibility.retention_limit_ms = config_.retention_ms;
        eligibility.eligible         = true;

        result.eligible.push_back(std::move(eligibility));
    }

    // Sort by age descending (oldest first).
    std::sort(result.eligible.begin(), result.eligible.end(),
              [](const SegmentEligibility& a, const SegmentEligibility& b) {
                  return a.age_ms > b.age_ms;
              });

    logger->debug("Time retention: {} segments eligible for deletion",
                  result.eligible.size());

    return result;
}

// ============================================================================
// LogCleaner::evaluate_size_retention
// ============================================================================

LogCleaner::CleanerResult LogCleaner::evaluate_size_retention(
    const std::vector<SegmentInfo>& segments,
    byte_count_t total_bytes,
    timestamp_ms_t now) {

    CleanerResult result;
    if (config_.retention_bytes <= 0) {
        // No size-based limit.
        return result;
    }

    if (total_bytes <= config_.retention_bytes) {
        // Under the size limit.
        return result;
    }

    auto logger = get_cleaner_logger();
    byte_count_t excess = total_bytes - config_.retention_bytes;
    byte_count_t accumulated = 0;

    // Collect sealed, non-active segments sorted by age (oldest first).
    std::vector<SegmentInfo> candidates;
    for (const auto& seg : segments) {
        if (seg.is_active) continue;
        if (!seg.is_sealed) continue;
        // Skip if already time-expired (avoid double-counting).
        if (should_delete_segment(seg, now)) continue;
        candidates.push_back(seg);
    }

    std::sort(candidates.begin(), candidates.end(),
              [](const SegmentInfo& a, const SegmentInfo& b) {
                  return a.max_timestamp < b.max_timestamp;
              });

    for (const auto& seg : candidates) {
        if (accumulated >= excess) break;

        SegmentEligibility eligibility;
        eligibility.info               = seg;
        eligibility.reason             = DeleteReason::size_excess;
        eligibility.retention_limit_bytes = config_.retention_bytes;
        eligibility.excess_bytes       = excess;
        eligibility.eligible            = true;

        result.eligible.push_back(std::move(eligibility));
        accumulated += seg.file_size;
    }

    logger->debug("Size retention: {} segments eligible (excess={} bytes, accumulated={})",
                  result.eligible.size(), excess, accumulated);

    return result;
}

// ============================================================================
// LogCleaner::evaluate_empty_segments
// ============================================================================

LogCleaner::CleanerResult LogCleaner::evaluate_empty_segments(
    const std::vector<SegmentInfo>& segments) {

    CleanerResult result;

    for (const auto& seg : segments) {
        if (seg.is_active) continue;
        if (!seg.empty()) continue;

        SegmentEligibility eligibility;
        eligibility.info    = seg;
        eligibility.reason  = DeleteReason::empty_segment;
        eligibility.eligible = true;

        result.eligible.push_back(std::move(eligibility));
    }

    auto logger = get_cleaner_logger();
    logger->debug("Empty segments: {} eligible for deletion",
                  result.eligible.size());

    return result;
}

// ============================================================================
// LogCleaner::evaluate_orphans
// ============================================================================

LogCleaner::CleanerResult LogCleaner::evaluate_orphans(
    const std::string& data_dir,
    const std::vector<SegmentInfo>& segments) {

    CleanerResult result;
    if (!config_.delete_orphan_indexes) return result;

    namespace fs = std::filesystem;
    auto logger = get_cleaner_logger();

    // Build a set of known .log file basenames.
    std::set<std::string> known_bases;
    for (const auto& seg : segments) {
        fs::path p(seg.file_path);
        std::string stem = p.stem().string(); // removes .log
        known_bases.insert(stem);
    }

    // Scan for orphan .index and .timeindex files.
    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(data_dir, ec)) {
        if (ec) break;
        if (!entry.is_regular_file()) continue;

        std::string fname = entry.path().filename().string();
        std::string ext;

        // Detect .index or .timeindex orphans.
        if (fname.size() > 6 && fname.substr(fname.size() - 6) == ".index") {
            ext = ".index";
        } else if (fname.size() > 10 && fname.substr(fname.size() - 10) == ".timeindex") {
            ext = ".timeindex";
        } else if (fname.size() > 8 && fname.substr(fname.size() - 8) == ".corrupt") {
            ext = ".corrupt";
        } else {
            continue;
        }

        std::string stem = fname.substr(0, fname.size() - ext.size());
        if (known_bases.count(stem) == 0) {
            // Orphan found.
            SegmentEligibility eligibility;
            eligibility.info.file_path = entry.path().string();
            eligibility.info.file_size = file_size_bytes(entry.path().string());
            eligibility.reason         = DeleteReason::orphan_index;
            eligibility.eligible       = true;

            result.eligible.push_back(std::move(eligibility));
            logger->debug("Orphan file: {}", entry.path().string());
        }
    }

    result.orphan_files_deleted = static_cast<int32_t>(result.eligible.size());
    logger->debug("Orphan scan: {} orphan files found", result.eligible.size());

    return result;
}

// ============================================================================
// LogCleaner::should_delete_segment
// ============================================================================

bool LogCleaner::should_delete_segment(const SegmentInfo& seg,
                                        timestamp_ms_t now) const noexcept {
    // Guard: never delete active or unsealed segments.
    if (seg.is_active || !seg.is_sealed) {
        return false;
    }

    return should_delete_by_time(seg, now) || should_delete_by_size(seg);
}

bool LogCleaner::should_delete_by_time(const SegmentInfo& seg,
                                        timestamp_ms_t now) const noexcept {
    if (config_.retention_ms <= 0) return false;

    timestamp_ms_t cutoff = now - config_.retention_ms;
    return seg.max_timestamp > 0 && seg.max_timestamp < cutoff;
}

bool LogCleaner::should_delete_by_size(const SegmentInfo& /*seg*/) const noexcept {
    // Size-based deletion is evaluated globally (total across all segments),
    // not per-segment.  This method returns false always; the global check
    // is done in evaluate_size_retention().
    return false;
}

// ============================================================================
// LogCleaner::compute_priority
// ============================================================================

int32_t LogCleaner::compute_priority(const SegmentEligibility& eligibility) const noexcept {
    // Higher priority number = more urgent to delete.
    // Priority scale:
    //   100  — time-expired, very old
    //   80   — time-expired, moderately old
    //   60   — size-excess, large segment
    //   40   — size-excess, small segment
    //   20   — empty segment
    //   10   — orphan index file
    //   0    — not eligible

    switch (eligibility.reason) {
    case DeleteReason::time_expired: {
        // Older segments get higher priority.
        // Scale: 80 + up to 20 based on how far past retention.
        if (config_.retention_ms > 0) {
            int64_t overage = eligibility.age_ms - config_.retention_ms;
            if (overage > 0) {
                // Cap bonus at 20.
                int64_t bonus = std::min(overage / (config_.retention_ms / 20 + 1), int64_t(20));
                return 80 + static_cast<int32_t>(bonus);
            }
        }
        return 80;
    }

    case DeleteReason::size_excess: {
        // Larger segments get higher priority (free more space with one delete).
        if (config_.retention_bytes > 0 && eligibility.info.file_size > 0) {
            // Scale 40-60 based on fraction of retention_bytes.
            double fraction = static_cast<double>(eligibility.info.file_size)
                            / static_cast<double>(config_.retention_bytes);
            int32_t bonus = static_cast<int32_t>(std::min(fraction * 20.0, 20.0));
            return 40 + bonus;
        }
        return 40;
    }

    case DeleteReason::empty_segment:
        return 20;

    case DeleteReason::orphan_index:
        return 10;

    default:
        return 0;
    }
}

// ============================================================================
// LogCleaner::sort_by_priority
// ============================================================================

void LogCleaner::sort_by_priority(std::vector<SegmentEligibility>& eligible) const noexcept {
    std::sort(eligible.begin(), eligible.end(),
              [this](const SegmentEligibility& a, const SegmentEligibility& b) {
                  int32_t pa = compute_priority(a);
                  int32_t pb = compute_priority(b);
                  if (pa != pb) return pa > pb;  // higher priority first

                  // Tie-break: older first.
                  if (a.age_ms != b.age_ms) return a.age_ms > b.age_ms;

                  // Tie-break: larger file first (free more space).
                  return a.info.file_size > b.info.file_size;
              });
}

// ============================================================================
// LogCleaner::execute
// ============================================================================

LogCleaner::CleanerResult LogCleaner::execute(
    const std::string& data_dir,
    const std::vector<SegmentInfo>& segments,
    timestamp_ms_t now) {

    auto logger = get_cleaner_logger();
    auto start_time = wall_clock_ms();

    CleanerResult result;

    if (now == 0) {
        now = wall_clock_ms();
    }

    // Compute total bytes across all segments.
    byte_count_t total_bytes = 0;
    for (const auto& seg : segments) {
        total_bytes += seg.file_size;
    }

    logger->info("Running cleaner on '{}': {} segments, {} total bytes, retention_ms={}, "
                 "retention_bytes={}, now={}",
                 data_dir, segments.size(), total_bytes,
                 config_.retention_ms, config_.retention_bytes, now);

    // Phase 1: Time-based evaluation.
    auto time_result = evaluate_time_retention(segments, now);
    result.eligible.insert(result.eligible.end(),
                           std::make_move_iterator(time_result.eligible.begin()),
                           std::make_move_iterator(time_result.eligible.end()));

    // Phase 2: Size-based evaluation.
    auto size_result = evaluate_size_retention(segments, total_bytes, now);
    result.eligible.insert(result.eligible.end(),
                           std::make_move_iterator(size_result.eligible.begin()),
                           std::make_move_iterator(size_result.eligible.end()));

    // Phase 3: Empty segments.
    auto empty_result = evaluate_empty_segments(segments);
    result.eligible.insert(result.eligible.end(),
                           std::make_move_iterator(empty_result.eligible.begin()),
                           std::make_move_iterator(empty_result.eligible.end()));

    // Phase 4: Orphan files.
    auto orphan_result = evaluate_orphans(data_dir, segments);
    result.eligible.insert(result.eligible.end(),
                           std::make_move_iterator(orphan_result.eligible.begin()),
                           std::make_move_iterator(orphan_result.eligible.end()));

    // Sort all eligible segments by priority.
    sort_by_priority(result.eligible);

    // Phase 5: Execute deletions.
    for (auto& eligibility : result.eligible) {
        if (config_.dry_run) {
            logger->info("DRY-RUN: would delete '{}' (reason={}, size={})",
                         eligibility.info.file_path,
                         to_string(eligibility.reason),
                         eligibility.info.file_size);

            switch (eligibility.reason) {
            case DeleteReason::time_expired:
                result.segments_deleted_by_time++;
                break;
            case DeleteReason::size_excess:
                result.segments_deleted_by_size++;
                break;
            case DeleteReason::empty_segment:
                result.segments_deleted_empty++;
                break;
            case DeleteReason::orphan_index:
                result.orphan_files_deleted++;
                break;
            default:
                break;
            }
            result.bytes_deleted += eligibility.info.file_size;
            continue;
        }

        // Actual deletion.
        bool success = delete_file(eligibility.info.file_path);

        // Also delete companion .index and .timeindex files.
        std::string base = eligibility.info.file_path;
        if (base.size() > 4 && base.substr(base.size() - 4) == ".log") {
            std::string index_path     = base + ".index";
            std::string timeindex_path = base + ".timeindex";
            if (file_exists(index_path))     delete_file(index_path);
            if (file_exists(timeindex_path)) delete_file(timeindex_path);
        }

        if (success) {
            logger->info("Deleted '{}' (reason={}, size={})",
                         eligibility.info.file_path,
                         to_string(eligibility.reason),
                         eligibility.info.file_size);

            switch (eligibility.reason) {
            case DeleteReason::time_expired:
                result.segments_deleted_by_time++;
                break;
            case DeleteReason::size_excess:
                result.segments_deleted_by_size++;
                break;
            case DeleteReason::empty_segment:
                result.segments_deleted_empty++;
                break;
            case DeleteReason::orphan_index:
                result.orphan_files_deleted++;
                break;
            default:
                break;
            }
            result.bytes_deleted += eligibility.info.file_size;
        } else {
            logger->error("Failed to delete '{}'", eligibility.info.file_path);
            result.error_count++;
        }
    }

    // Compute new log start offset.
    result.new_log_start_offset = compute_new_log_start(segments, result);

    result.duration_ms = wall_clock_ms() - start_time;

    logger->info("Cleaner finished: {} deleted ({} time, {} size, {} empty, {} orphans), "
                 "{} bytes freed, new_log_start={}, {}ms",
                 result.segments_deleted_by_time + result.segments_deleted_by_size
                    + result.segments_deleted_empty + result.orphan_files_deleted,
                 result.segments_deleted_by_time,
                 result.segments_deleted_by_size,
                 result.segments_deleted_empty,
                 result.orphan_files_deleted,
                 result.bytes_deleted,
                 result.new_log_start_offset,
                 result.duration_ms);

    if (result.error_count > 0) {
        result.error = error_code::storage_unavailable;
        result.error_message = fmt::format("{} deletion(s) failed", result.error_count);
    }

    return result;
}

// ============================================================================
// LogCleaner::compute_new_log_start
// ============================================================================

offset_t LogCleaner::compute_new_log_start(
    const std::vector<SegmentInfo>& segments,
    const CleanerResult& result) const noexcept {

    // Build a set of deleted file paths.
    std::set<std::string> deleted;
    for (const auto& eligibility : result.eligible) {
        deleted.insert(eligibility.info.file_path);
    }

    // Find the first segment not being deleted.
    for (const auto& seg : segments) {
        if (deleted.count(seg.file_path) == 0) {
            return seg.base_offset;
        }
    }

    // All segments deleted or none remain — return the highest base_offset
    // we know about, or kInvalidOffset.
    if (!segments.empty()) {
        return segments.back().base_offset;
    }
    return kInvalidOffset;
}

// ============================================================================
// LogCleaner::to_string
// ============================================================================

const char* LogCleaner::to_string(DeleteReason reason) noexcept {
    switch (reason) {
    case DeleteReason::time_expired: return "time_expired";
    case DeleteReason::size_excess:  return "size_excess";
    case DeleteReason::empty_segment: return "empty_segment";
    case DeleteReason::orphan_index:  return "orphan_index";
    case DeleteReason::manual:        return "manual";
    case DeleteReason::none:          return "none";
    default:                          return "unknown";
    }
}

// ============================================================================
// Convenience: free function
// ============================================================================

LogCleaner::CleanerResult run_cleaner(
    const std::string& data_dir,
    const std::vector<SegmentInfo>& segments,
    duration_ms_t retention_ms,
    byte_count_t retention_bytes,
    bool dry_run) {

    LogCleaner::CleanerConfig cfg;
    cfg.retention_ms    = retention_ms;
    cfg.retention_bytes = retention_bytes;
    cfg.dry_run         = dry_run;

    LogCleaner cleaner(std::move(cfg));
    return cleaner.execute(data_dir, segments, wall_clock_ms());
}

} // namespace torrent
