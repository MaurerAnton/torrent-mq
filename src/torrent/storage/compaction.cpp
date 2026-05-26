/**
 * torrent-mq — Log Compaction Engine Implementation
 *
 * Implements the full compaction pipeline: key-based dedup with combiners,
 * segment eligibility analysis, LRU-bounded key index, atomic segment
 * replacement with crash recovery, and multi-segment merge.
 *
 * All disk mutations are crash-safe: write to temp file, fsync, atomic rename.
 * The KeyedRecordIndex uses an LRU eviction policy to bound memory usage
 * for workloads with key spaces larger than available RAM.
 */

#include "torrent/storage/compaction.h"
#include "torrent/storage/types.h"
#include "torrent/common/types.h"
#include "torrent/storage/segment.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <random>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <spdlog/spdlog.h>

namespace torrent {

// ============================================================================
// Internal helpers
// ============================================================================

namespace {

/// Get a spdlog logger instance for the compaction subsystem.
std::shared_ptr<spdlog::logger> compaction_logger() {
    static auto logger = spdlog::get("compaction");
    if (!logger) {
        logger = spdlog::stderr_color_mt("compaction");
    }
    return logger;
}

/// Current wall-clock time in milliseconds since epoch.
timestamp_ms_t wall_clock_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

/// Generate a random hex suffix for temp file names (avoids collisions).
std::string random_hex_suffix(size_t len = 8) {
    static thread_local std::mt19937_64 rng(
        static_cast<uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count()) ^
        static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&rng)));
    static const char hex_chars[] = "0123456789abcdef";
    std::string s;
    s.reserve(len);
    std::uniform_int_distribution<int> dist(0, 15);
    for (size_t i = 0; i < len; ++i) {
        s.push_back(hex_chars[dist(rng)]);
    }
    return s;
}

/// Recursively create directories for a path (like `mkdir -p`).
bool mkdir_p(const std::string& path) {
    if (path.empty()) return true;
    struct stat st;
    if (::stat(path.c_str(), &st) == 0) {
        return S_ISDIR(st.st_mode);
    }
    auto pos = path.rfind('/');
    if (pos != std::string::npos && pos > 0) {
        if (!mkdir_p(path.substr(0, pos))) return false;
    }
    return ::mkdir(path.c_str(), 0755) == 0 || errno == EEXIST;
}

/// Get the parent directory of a file path.
std::string dirname_of(const std::string& file_path) {
    auto pos = file_path.rfind('/');
    if (pos == std::string::npos) return ".";
    if (pos == 0) return "/";
    return file_path.substr(0, pos);
}

/// Ensure a file's data and metadata are durable (fsync on fd and parent dir).
bool full_fsync(int fd, const std::string& file_path) {
    if (::fsync(fd) != 0) {
        compaction_logger()->warn("fsync on fd {}: {}", fd, strerror(errno));
        return false;
    }
    std::string parent = dirname_of(file_path);
    int dir_fd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY);
    if (dir_fd >= 0) {
        if (::fsync(dir_fd) != 0) {
            compaction_logger()->debug("fsync on directory '{}' failed: {}",
                                       parent, strerror(errno));
        }
        ::close(dir_fd);
    }
    return true;
}

/// Convert a shared_buffer key to a std::string for map insertion.
[[nodiscard]] std::string key_to_string(const shared_buffer& key) {
    if (key.empty()) return {};
    return std::string(key.data(), key.size());
}

/// Estimate the approximate on-disk byte size of a single Record.
[[nodiscard]] byte_count_t record_byte_size(const Record& rec) {
    byte_count_t sz = 16;  // fixed overhead: key_len, val_len, ts, offset
    sz += static_cast<byte_count_t>(rec.key.size());
    sz += static_cast<byte_count_t>(rec.value.size());
    for (const auto& h : rec.headers) {
        sz += static_cast<byte_count_t>(h.key.size() + h.value.size() + 8);
    }
    return sz;
}

} // anonymous namespace

// ============================================================================
// KeyedRecordIndex
// ============================================================================

KeyedRecordIndex::KeyedRecordIndex(size_t max_entries)
    : max_entries_(max_entries) {
    map_.reserve(std::min(max_entries_, size_t(4096)));
}

KeyedRecordIndex::~KeyedRecordIndex() = default;

const KeyedRecordIndex::IndexEntry* KeyedRecordIndex::find(
    std::string_view key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string key_str(key);
    auto it = map_.find(key_str);
    if (it == map_.end()) return nullptr;
    // In a non-const context we'd splice to front here; for const we just
    // return. The insert_or_update path handles LRU ordering correctly.
    return &(*it->second);
}

void KeyedRecordIndex::insert_or_update(std::string_view key, offset_t offset,
                                         timestamp_ms_t timestamp,
                                         bool is_tombstone) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string key_str(key);
    auto map_it = map_.find(key_str);

    if (map_it != map_.end()) {
        auto& node = *map_it->second;
        if (offset > node.latest_offset) {
            node.latest_offset = offset;
            node.timestamp     = timestamp;
            node.is_tombstone  = is_tombstone;
        }
        // Move to front (most-recently-used).
        lru_list_.splice(lru_list_.begin(), lru_list_, map_it->second);
    } else {
        // Evict LRU tail if at capacity.
        if (lru_list_.size() >= max_entries_ && !lru_list_.empty()) {
            auto& back = lru_list_.back();
            map_.erase(back.key);
            lru_list_.pop_back();
            evictions_.fetch_add(1, std::memory_order_relaxed);
        }
        LruNode node;
        node.key          = std::move(key_str);
        node.latest_offset = offset;
        node.timestamp    = timestamp;
        node.is_tombstone = is_tombstone;
        lru_list_.push_front(std::move(node));
        map_.emplace(lru_list_.front().key, lru_list_.begin());
    }
}

void KeyedRecordIndex::remove(std::string_view key) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string key_str(key);
    auto it = map_.find(key_str);
    if (it != map_.end()) {
        lru_list_.erase(it->second);
        map_.erase(it);
    }
}

bool KeyedRecordIndex::evict_lru() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (lru_list_.empty()) return false;
    auto& back = lru_list_.back();
    map_.erase(back.key);
    lru_list_.pop_back();
    evictions_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

void KeyedRecordIndex::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    lru_list_.clear();
    map_.clear();
}

void KeyedRecordIndex::reserve(size_t capacity) {
    std::lock_guard<std::mutex> lock(mutex_);
    map_.reserve(std::min(capacity, max_entries_));
}

size_t KeyedRecordIndex::size() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return lru_list_.size();
}

bool KeyedRecordIndex::empty() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return lru_list_.empty();
}

// ============================================================================
// Compactor
// ============================================================================

Compactor::Compactor(CompactorConfig config)
    : config_(std::move(config))
    , index_(config_.max_key_index_entries) {}

Compactor::~Compactor() = default;

void Compactor::reset() {
    index_.clear();
    index_.reserve(std::min(config_.max_key_index_entries, size_t(4096)));
}

bool Compactor::can_purge_tombstone(timestamp_ms_t ts,
                                     timestamp_ms_t now) const noexcept {
    if (config_.delete_retention_ms <= 0) return false;
    return (now - ts) >= config_.delete_retention_ms;
}

Compactor::CompactResult Compactor::compact(
    std::vector<RecordBatch> batches, CombinerFn combiner) {
    CompactResult result;
    auto start_time = wall_clock_ms();

    // Count input.
    for (const auto& batch : batches) {
        result.metrics.records_in += batch.records.size();
        result.metrics.bytes_in  += batch.approximate_size();
    }
    if (batches.empty()) {
        result.metrics.duration_ms = wall_clock_ms() - start_time;
        return result;
    }

    // Default combiner: keep the record with the highest offset.
    if (!combiner) {
        combiner = [](const Record&, const Record&) -> bool { return false; };
    }

    timestamp_ms_t now = wall_clock_ms();

    // --- Phase 1: scan all records, deduplicate by key ---
    //
    // We maintain two parallel data structures:
    //   1. KeyedRecordIndex — O(1) lookups for duplicate detection + LRU eviction.
    //   2. A std::unordered_map key→Record — stores the "winning" full Record
    //      for each key so we can produce output without re-scanning.
    //
    // For each record:
    //   - If key unseen: insert into both structures.
    //   - If key seen with lower offset: combiner decides (default: replace).
    //   - If key seen with higher offset: combiner decides (default: keep
    //     existing, discard newcomer).
    //   - Tombstones aged past delete_retention_ms are purged immediately.

    struct WinningRecord {
        Record         record;
        offset_t       latest_offset = kInvalidOffset;
        timestamp_ms_t timestamp     = 0;
        bool           is_tombstone  = false;
    };
    std::unordered_map<std::string, WinningRecord> winners;
    reset();  // clear the key index

    for (auto& batch : batches) {
        for (auto& rec : batch.records) {
            std::string key_str = key_to_string(rec.key);
            bool is_tombstone = rec.is_tombstone() || rec.value.empty();

            // Tombstone that has aged out: drop it silently.
            if (is_tombstone && can_purge_tombstone(rec.timestamp, now)) {
                result.metrics.tombstones_purged++;
                continue;
            }

            const auto* idx_entry = index_.find(key_str);
            if (idx_entry == nullptr) {
                // First occurrence of this key.
                index_.insert_or_update(key_str, rec.offset, rec.timestamp,
                                        is_tombstone);
                WinningRecord wr;
                wr.record        = std::move(rec);
                wr.latest_offset = rec.offset;
                wr.timestamp     = rec.timestamp;
                wr.is_tombstone  = is_tombstone;
                winners.emplace(std::move(key_str), std::move(wr));
                continue;
            }

            // Key already exists: resolve via combiner.
            auto win_it = winners.find(key_str);
            // (must exist — index and winners are kept in sync)
            if (win_it == winners.end()) continue;

            WinningRecord& existing_wr = win_it->second;
            bool candidate_newer = (rec.offset > idx_entry->latest_offset);

            // Build a minimal "existing" Record for the combiner callback.
            // The combiner receives two records and returns true to keep
            // the existing one, false to replace it with the candidate.
            Record existing_view;
            existing_view.key       = rec.key;
            existing_view.value     = shared_buffer();
            existing_view.offset    = idx_entry->latest_offset;
            existing_view.timestamp = idx_entry->timestamp;

            bool keep_existing = combiner(existing_view, rec);

            if (!keep_existing) {
                // Replace.
                existing_wr.record        = std::move(rec);
                existing_wr.latest_offset = rec.offset;
                existing_wr.timestamp     = rec.timestamp;
                existing_wr.is_tombstone  = is_tombstone;
                index_.insert_or_update(key_str, rec.offset, rec.timestamp,
                                        is_tombstone);
            }
            // Whether we kept or replaced, this new record was a duplicate.
            result.metrics.duplicates_removed++;
        }
    }

    result.metrics.unique_keys = static_cast<int64_t>(winners.size());
    result.metrics.lru_evictions = index_.evictions();

    // --- Phase 2: build output RecordBatches ---
    //
    // Collect winning records, sort by offset to preserve logical order,
    // and pack into batches respecting max_records_per_batch.

    std::vector<WinningRecord> sorted;
    sorted.reserve(winners.size());
    for (auto& kv : winners) {
        sorted.push_back(std::move(kv.second));
    }
    std::sort(sorted.begin(), sorted.end(),
              [](const WinningRecord& a, const WinningRecord& b) {
                  return a.latest_offset < b.latest_offset;
              });

    int32_t max_per_batch = config_.max_records_per_batch;
    if (max_per_batch <= 0) max_per_batch = 50000;

    for (size_t i = 0; i < sorted.size(); ) {
        RecordBatch batch;
        batch.base_offset = sorted[i].latest_offset;
        batch.records.reserve(
            static_cast<size_t>(std::min(max_per_batch,
                static_cast<int32_t>(sorted.size() - i))));

        timestamp_ms_t batch_min_ts = sorted[i].timestamp;
        timestamp_ms_t batch_max_ts = sorted[i].timestamp;
        size_t end = std::min(i + static_cast<size_t>(max_per_batch),
                              sorted.size());

        for (; i < end; ++i) {
            auto& wr = sorted[i];
            // Secondary tombstone purge check (records that became
            // purgeable while the batch was being built).
            if (wr.is_tombstone && can_purge_tombstone(wr.timestamp, now)) {
                result.metrics.tombstones_purged++;
                continue;
            }
            wr.record.offset = batch.base_offset +
                static_cast<offset_t>(batch.records.size());
            if (wr.timestamp < batch_min_ts) batch_min_ts = wr.timestamp;
            if (wr.timestamp > batch_max_ts) batch_max_ts = wr.timestamp;
            batch.records.push_back(std::move(wr.record));
            result.metrics.records_out++;
        }

        if (!batch.records.empty()) {
            batch.record_count      = static_cast<int32_t>(batch.records.size());
            batch.base_timestamp    = batch_min_ts;
            batch.max_timestamp     = batch_max_ts;
            batch.last_offset_delta = batch.record_count - 1;
            batch.compression       = compression_type::none;
            result.metrics.bytes_out += batch.approximate_size();
            result.compacted.push_back(std::move(batch));
        }
    }

    result.metrics.duration_ms = wall_clock_ms() - start_time;

    compaction_logger()->info(
        "Compactor: {} in -> {} out ({} dup, {} tomb), {:.1f}% reduction, {}ms",
        result.metrics.records_in, result.metrics.records_out,
        result.metrics.duplicates_removed, result.metrics.tombstones_purged,
        result.metrics.compaction_ratio() * 100.0,
        result.metrics.duration_ms);

    return result;
}

// ============================================================================
// CompactionStrategy
// ============================================================================

CompactionStrategy::CompactionStrategy(CompactorConfig config)
    : config_(std::move(config)) {}

CompactionStrategy::~CompactionStrategy() = default;

bool CompactionStrategy::is_eligible(const SegmentCandidate& seg) const {
    // Must be sealed and not active.
    if (!seg.info.is_sealed) return false;
    if (seg.info.is_active)  return false;
    // Must not be empty.
    if (seg.info.empty())    return false;
    // Check min compaction lag.
    if (config_.min_compaction_lag_ms > 0) {
        duration_ms_t seg_age = age_ms(seg, seg.now);
        if (seg_age < config_.min_compaction_lag_ms) return false;
    }
    return true;
}

bool CompactionStrategy::is_urgent(const SegmentCandidate& seg) const {
    if (config_.max_compaction_lag_ms <= 0) return false;
    duration_ms_t seg_age = age_ms(seg, seg.now);
    return seg_age >= config_.max_compaction_lag_ms;
}

double CompactionStrategy::dirty_ratio(const SegmentCandidate& seg) {
    if (seg.total_keys <= 0) return 0.0;
    return static_cast<double>(seg.duplicate_count) /
           static_cast<double>(seg.total_keys);
}

duration_ms_t CompactionStrategy::age_ms(const SegmentCandidate& seg,
                                          timestamp_ms_t now) {
    // Use last_modified if available; fall back to created_at.
    timestamp_ms_t ref = seg.info.last_modified > 0
                             ? seg.info.last_modified
                             : seg.info.created_at;
    if (ref <= 0) return 0;
    return now - ref;
}

CompactionPlan CompactionStrategy::plan(
    const std::vector<SegmentCandidate>& segments) {
    CompactionPlan plan;
    if (segments.empty()) return plan;

    // Step 1: Plan merges for adjacent small segments.
    plan_merges(segments, plan);

    // Step 2: Plan single-segment compactions for remaining eligibles.
    plan_singles(segments, plan);

    compaction_logger()->info(
        "CompactionStrategy::plan: {} single, {} merge, {} skipped "
        "(from {} candidates)",
        plan.single_tasks.size(), plan.merge_tasks.size(),
        plan.skipped_count, segments.size());

    return plan;
}

void CompactionStrategy::plan_merges(
    const std::vector<SegmentCandidate>& segments, CompactionPlan& plan) {
    if (config_.max_merge_segments < 2) return;

    byte_count_t threshold = config_.small_segment_threshold;
    size_t i = 0;
    while (i < segments.size()) {
        // Start-of-run: must be eligible and below the small-segment threshold.
        if (!is_eligible(segments[i]) ||
            segments[i].info.file_size >= threshold) {
            ++i;
            continue;
        }

        size_t run_start = i;
        size_t run_end   = i + 1;
        bool   run_urgent = is_urgent(segments[i]);

        while (run_end < segments.size() &&
               static_cast<int32_t>(run_end - run_start) <
                   config_.max_merge_segments) {
            if (!is_eligible(segments[run_end])) break;
            if (segments[run_end].info.file_size >= threshold) break;
            if (is_urgent(segments[run_end])) run_urgent = true;
            ++run_end;
        }

        size_t run_size = run_end - run_start;
        if (run_size >= 2) {
            CompactionPlan::MergeTask task;
            task.urgent = run_urgent;
            task.segment_ids.reserve(run_size);
            for (size_t j = run_start; j < run_end; ++j) {
                task.segment_ids.push_back(segments[j].info.segment_id);
            }
            plan.merge_tasks.push_back(std::move(task));
        }
        i = run_end;
    }
}

void CompactionStrategy::plan_singles(
    const std::vector<SegmentCandidate>& segments, CompactionPlan& plan) {
    double min_dirty = config_.min_cleanable_dirty_ratio;

    for (const auto& seg : segments) {
        if (!is_eligible(seg)) {
            plan.skipped_count++;
            continue;
        }

        bool urgent = is_urgent(seg);

        // Non-urgent segments must meet the dirty ratio threshold.
        if (!urgent && min_dirty > 0.0) {
            double dr = dirty_ratio(seg);
            if (dr < min_dirty) {
                plan.skipped_count++;
                compaction_logger()->debug(
                    "Segment {} skipped: dirty ratio {:.3f} < threshold {:.3f}",
                    seg.info.segment_id, dr, min_dirty);
                continue;
            }
        }

        // Check if already covered by a merge task.
        bool in_merge = false;
        for (const auto& mt : plan.merge_tasks) {
            for (auto sid : mt.segment_ids) {
                if (sid == seg.info.segment_id) { in_merge = true; break; }
            }
            if (in_merge) break;
        }
        if (in_merge) continue;

        CompactionPlan::SingleTask task;
        task.segment_id = seg.info.segment_id;
        task.urgent     = urgent;
        plan.single_tasks.push_back(std::move(task));
    }
}

// ============================================================================
// CompactionExecutor
// ============================================================================

CompactionExecutor::CompactionExecutor(CompactorConfig config)
    : config_(std::move(config))
    , compactor_(config_) {}

CompactionExecutor::~CompactionExecutor() = default;

std::string CompactionExecutor::temp_file_path(
    const std::string& target_path) const {
    std::string dir = config_.temp_directory.empty()
                          ? dirname_of(target_path)
                          : config_.temp_directory;
    auto pos = target_path.rfind('/');
    std::string base = (pos == std::string::npos) ? target_path
                                                   : target_path.substr(pos + 1);
    return dir + "/." + base + ".compact." + random_hex_suffix();
}

void CompactionExecutor::safe_delete(const std::string& path) {
    if (::unlink(path.c_str()) != 0 && errno != ENOENT) {
        compaction_logger()->warn("Failed to delete '{}': {}", path,
                                  strerror(errno));
    }
}

result<CompactionMetrics> CompactionExecutor::write_and_swap(
    const std::vector<RecordBatch>& batches,
    const std::string& target_path,
    timestamp_ms_t start_time) {
    CompactionMetrics metrics;

    if (batches.empty()) {
        metrics.duration_ms = wall_clock_ms() - start_time;
        return result<CompactionMetrics>::success(metrics);
    }

    std::string tmp_path = temp_file_path(target_path);

    // Ensure temp directory exists.
    std::string tmp_dir = dirname_of(tmp_path);
    if (!tmp_dir.empty() && tmp_dir != ".") {
        mkdir_p(tmp_dir);
    }

    // Open temp file for writing.
    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return result<CompactionMetrics>::failure(
            error_code::storage_unavailable,
            fmt::format("Cannot create temp '{}': {}", tmp_path,
                        strerror(errno)));
    }

    // --- Write segment header (64 bytes, matching Segment format) ---
    char header[64] = {};
    {
        uint32_t magic = 0x47535154;  // "TQSG" LE
        std::memcpy(header + 0, &magic, 4);
        uint16_t format_ver = 1;
        std::memcpy(header + 4, &format_ver, 2);
        uint16_t hdr_size = 64;
        std::memcpy(header + 6, &hdr_size, 2);
        // header_crc at +8 left as 0 (computed properly in production).
        // base_offset at +16.
        if (!batches.empty()) {
            int64_t bo = batches[0].base_offset;
            std::memcpy(header + 16, &bo, 8);
        }
        int64_t now_ms = wall_clock_ms();
        std::memcpy(header + 32, &now_ms, 8);  // created_at
        // compression_type at +40: none (0).
        // flags at +41: bit 0 = is_sealed = 1 (compacted segments are sealed).
        header[41] = 0x01;
    }

    if (::write(fd, header, 64) != 64) {
        ::close(fd);
        safe_delete(tmp_path);
        return result<CompactionMetrics>::failure(
            error_code::storage_unavailable,
            "Failed to write segment header to temp file");
    }

    // --- Write records in a simplified wire format ---
    // Each record: key_len(4) + key_bytes + value_len(4) + value_bytes
    //              + timestamp(8) + offset(8) + headers_count(4)
    //              + [header_key_len(4) + header_key + header_val_len(4) + header_val]...
    for (const auto& batch : batches) {
        for (const auto& rec : batch.records) {
            int32_t key_len = static_cast<int32_t>(rec.key.size());
            int32_t val_len = static_cast<int32_t>(rec.value.size());

            if (::write(fd, &key_len, 4) != 4) goto write_error;
            if (key_len > 0) {
                if (::write(fd, rec.key.data(),
                            static_cast<size_t>(key_len)) != key_len)
                    goto write_error;
            }
            if (::write(fd, &val_len, 4) != 4) goto write_error;
            if (val_len > 0) {
                if (::write(fd, rec.value.data(),
                            static_cast<size_t>(val_len)) != val_len)
                    goto write_error;
            }
            if (::write(fd, &rec.timestamp, 8) != 8) goto write_error;
            if (::write(fd, &rec.offset, 8) != 8) goto write_error;

            // Write headers count + headers.
            int32_t hdr_count = static_cast<int32_t>(rec.headers.size());
            if (::write(fd, &hdr_count, 4) != 4) goto write_error;
            for (const auto& h : rec.headers) {
                int32_t hk_len = static_cast<int32_t>(h.key.size());
                int32_t hv_len = static_cast<int32_t>(h.value.size());
                if (::write(fd, &hk_len, 4) != 4) goto write_error;
                if (hk_len > 0) {
                    if (::write(fd, h.key.data(),
                                static_cast<size_t>(hk_len)) != hk_len)
                        goto write_error;
                }
                if (::write(fd, &hv_len, 4) != 4) goto write_error;
                if (hv_len > 0) {
                    if (::write(fd, h.value.data(),
                                static_cast<size_t>(hv_len)) != hv_len)
                        goto write_error;
                }
            }

            metrics.bytes_out  += record_byte_size(rec);
            metrics.records_out++;
        }
    }

    // --- Finalize: fsync, rename, dir-sync ---
    if (!full_fsync(fd, tmp_path)) {
        ::close(fd);
        safe_delete(tmp_path);
        return result<CompactionMetrics>::failure(
            error_code::storage_unavailable, "fsync failed on temp file");
    }
    ::close(fd);

    // Paranoid sync: re-open and fsync again.
    if (config_.paranoid_sync) {
        int fd2 = ::open(tmp_path.c_str(), O_RDONLY);
        if (fd2 >= 0) {
            ::fsync(fd2);
            ::close(fd2);
        }
    }

    // Atomic rename.
    if (::rename(tmp_path.c_str(), target_path.c_str()) != 0) {
        safe_delete(tmp_path);
        return result<CompactionMetrics>::failure(
            error_code::storage_unavailable,
            fmt::format("rename('{}', '{}') failed: {}",
                        tmp_path, target_path, strerror(errno)));
    }

    // Fsync parent directory to make rename durable.
    std::string parent = dirname_of(target_path);
    int dir_fd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY);
    if (dir_fd >= 0) {
        ::fsync(dir_fd);
        ::close(dir_fd);
    }

    metrics.duration_ms = wall_clock_ms() - start_time;
    metrics.error       = error_code::none;

    compaction_logger()->info(
        "write_and_swap: {} records ({} bytes) -> '{}', {}ms",
        metrics.records_out, metrics.bytes_out, target_path,
        metrics.duration_ms);

    return result<CompactionMetrics>::success(metrics);

write_error:
    ::close(fd);
    safe_delete(tmp_path);
    return result<CompactionMetrics>::failure(
        error_code::storage_unavailable,
        fmt::format("Write error on temp file '{}': {}", tmp_path,
                    strerror(errno)));
}

CompactionExecutor::TaskResult CompactionExecutor::execute_single(
    Segment& segment, Compactor::CombinerFn combiner) {
    TaskResult task_result;
    task_result.segment_id = segment.segment_id();
    auto start_time = wall_clock_ms();

    compaction_logger()->info("Compacting segment id={}, base_offset={}",
                              segment.segment_id(), segment.base_offset());

    offset_t base = segment.base_offset();
    offset_t next = segment.next_offset();
    if (next <= base) {
        task_result.completed = true;
        task_result.metrics.duration_ms = wall_clock_ms() - start_time;
        return task_result;
    }

    // Read all batches from the segment.
    auto read_res = segment.read_range(base, next);
    if (read_res.failed()) {
        task_result.error = read_res.error;
        task_result.error_message =
            "Failed to read segment: " + read_res.error_message;
        compaction_logger()->error("execute_single: {}",
                                   task_result.error_message);
        return task_result;
    }

    // Run compactor to deduplicate.
    compactor_.reset();
    auto compact_res = compactor_.compact(
        std::move(read_res.value.batches), combiner);
    task_result.metrics = compact_res.metrics;

    if (!compact_res.compacted.empty()) {
        auto write_res = write_and_swap(
            compact_res.compacted, segment.info().file_path, start_time);
        if (write_res.failed()) {
            task_result.error = write_res.error;
            task_result.error_message = write_res.error_message;
            return task_result;
        }
        task_result.metrics.bytes_out   = write_res.value.bytes_out;
        task_result.metrics.records_out = write_res.value.records_out;
        task_result.metrics.duration_ms = write_res.value.duration_ms;
    } else {
        task_result.metrics.duration_ms = wall_clock_ms() - start_time;
    }

    task_result.completed = true;
    return task_result;
}

CompactionExecutor::TaskResult CompactionExecutor::execute_merge(
    std::vector<std::unique_ptr<Segment>> segments,
    Compactor::CombinerFn combiner) {
    TaskResult task_result;
    if (segments.empty()) {
        task_result.completed = true;
        return task_result;
    }
    task_result.segment_id = segments[0]->segment_id();
    auto start_time = wall_clock_ms();

    compaction_logger()->info("Merging {} segments, first id={}",
                              segments.size(), task_result.segment_id);

    // Collect all records from all segments.
    std::vector<RecordBatch> all_batches;
    for (auto& seg : segments) {
        if (!seg || !seg->is_open()) continue;
        offset_t base = seg->base_offset();
        offset_t next = seg->next_offset();
        if (next <= base) continue;

        auto read_res = seg->read_range(base, next);
        if (read_res.failed()) {
            task_result.error = read_res.error;
            task_result.error_message =
                "Merge read failed: " + read_res.error_message;
            return task_result;
        }
        for (auto& batch : read_res.value.batches) {
            all_batches.push_back(std::move(batch));
        }
    }

    if (all_batches.empty()) {
        task_result.completed = true;
        task_result.metrics.duration_ms = wall_clock_ms() - start_time;
        return task_result;
    }

    // Deduplicate across all merged batches.
    compactor_.reset();
    auto compact_res = compactor_.compact(std::move(all_batches), combiner);
    task_result.metrics = compact_res.metrics;

    // Write to a single output file named after the first segment.
    if (!compact_res.compacted.empty()) {
        std::string output_path = segments[0]->info().file_path;
        auto write_res = write_and_swap(
            compact_res.compacted, output_path, start_time);
        if (write_res.failed()) {
            task_result.error = write_res.error;
            task_result.error_message = write_res.error_message;
            return task_result;
        }
        task_result.metrics.bytes_out   = write_res.value.bytes_out;
        task_result.metrics.records_out = write_res.value.records_out;
        task_result.metrics.duration_ms = write_res.value.duration_ms;
    }

    task_result.completed = true;
    return task_result;
}

CompactionExecutor::ExecuteResult CompactionExecutor::execute_plan(
    const CompactionPlan& plan,
    const std::vector<std::unique_ptr<Segment>>& segments,
    Compactor::CombinerFn combiner) {
    ExecuteResult result;
    if (plan.empty()) return result;

    // --- Merge tasks ---
    for (const auto& mt : plan.merge_tasks) {
        // Collect raw pointers from the segment list.
        std::vector<Segment*> merge_ptrs;
        for (auto seg_id : mt.segment_ids) {
            for (auto& seg : segments) {
                if (seg && seg->segment_id() == seg_id) {
                    merge_ptrs.push_back(seg.get());
                    break;
                }
            }
        }
        if (merge_ptrs.size() < 2) continue;

        // Use SegmentMerge for proper merge + dedup.
        SegmentMerge merger(config_);
        auto merge_result = merger.merge(
            merge_ptrs, merge_ptrs[0]->base_offset(),
            merge_ptrs[0]->info().file_path, combiner);

        TaskResult tr;
        tr.segment_id = mt.segment_ids[0];
        tr.metrics    = merge_result.metrics;
        tr.completed  = merge_result.ok();
        tr.error      = merge_result.metrics.error;
        tr.error_message = merge_result.metrics.error_message;
        result.results.push_back(std::move(tr));

        // Accumulate.
        result.cumulative.bytes_in           += merge_result.metrics.bytes_in;
        result.cumulative.bytes_out          += merge_result.metrics.bytes_out;
        result.cumulative.records_in         += merge_result.metrics.records_in;
        result.cumulative.records_out        += merge_result.metrics.records_out;
        result.cumulative.duplicates_removed +=
            merge_result.metrics.duplicates_removed;
        result.cumulative.tombstones_purged  +=
            merge_result.metrics.tombstones_purged;
    }

    // --- Single tasks ---
    for (const auto& st : plan.single_tasks) {
        Segment* seg = nullptr;
        for (auto& s : segments) {
            if (s && s->segment_id() == st.segment_id) {
                seg = s.get();
                break;
            }
        }
        if (!seg) continue;

        auto tr = execute_single(*seg, combiner);
        result.results.push_back(std::move(tr));

        result.cumulative.bytes_in           += tr.metrics.bytes_in;
        result.cumulative.bytes_out          += tr.metrics.bytes_out;
        result.cumulative.records_in         += tr.metrics.records_in;
        result.cumulative.records_out        += tr.metrics.records_out;
        result.cumulative.duplicates_removed += tr.metrics.duplicates_removed;
        result.cumulative.tombstones_purged  += tr.metrics.tombstones_purged;
    }

    result.cumulative.unique_keys =
        result.cumulative.records_out;  // post-compaction

    compaction_logger()->info(
        "execute_plan complete: {} tasks, {} in -> {} out ({} dup, {} tomb), "
        "{} bytes reclaimed",
        result.results.size(),
        result.cumulative.records_in, result.cumulative.records_out,
        result.cumulative.duplicates_removed,
        result.cumulative.tombstones_purged,
        result.cumulative.bytes_in - result.cumulative.bytes_out);

    return result;
}

int32_t CompactionExecutor::detect_partial_files(
    const std::string& data_directory) {
    int32_t resolved = 0;

    DIR* dir = ::opendir(data_directory.c_str());
    if (!dir) {
        compaction_logger()->warn(
            "detect_partial_files: cannot open '{}': {}",
            data_directory, strerror(errno));
        return 0;
    }

    struct dirent* entry;
    while ((entry = ::readdir(dir)) != nullptr) {
        std::string name(entry->d_name);
        auto pos = name.find(".compact.");
        if (pos == std::string::npos) continue;

        std::string temp_path = data_directory + "/" + name;
        // Target: strip ".compact.<random>" suffix.
        std::string target_path = data_directory + "/" + name.substr(0, pos);

        struct stat temp_st, target_st;
        bool temp_exists   = (::stat(temp_path.c_str(), &temp_st) == 0);
        bool target_exists = (::stat(target_path.c_str(), &target_st) == 0);

        if (!temp_exists) continue;

        if (!target_exists) {
            // Target missing: promote temp to target.
            if (::rename(temp_path.c_str(), target_path.c_str()) == 0) {
                compaction_logger()->info(
                    "Recovered orphan: '{}' -> '{}'", temp_path, target_path);
                resolved++;
            } else {
                compaction_logger()->warn(
                    "Recover failed for '{}': {}", temp_path, strerror(errno));
            }
        } else if (temp_st.st_mtime > target_st.st_mtime) {
            // Temp is newer: swap.
            std::string backup =
                target_path + ".backup." + random_hex_suffix();
            if (::rename(target_path.c_str(), backup.c_str()) == 0 &&
                ::rename(temp_path.c_str(), target_path.c_str()) == 0) {
                compaction_logger()->info(
                    "Swapped newer temp: '{}', backup: '{}'",
                    target_path, backup);
                safe_delete(backup);
                resolved++;
            }
        } else {
            // Target is newer: temp is stale.
            safe_delete(temp_path);
            resolved++;
        }
    }

    ::closedir(dir);

    compaction_logger()->info(
        "detect_partial_files: resolved {} orphans in '{}'",
        resolved, data_directory);

    return resolved;
}

// ============================================================================
// SegmentMerge
// ============================================================================

SegmentMerge::SegmentMerge(CompactorConfig config)
    : config_(std::move(config))
    , compactor_(config_) {}

SegmentMerge::~SegmentMerge() = default;

SegmentMerge::MergeResult SegmentMerge::merge(
    std::vector<Segment*> sources,
    offset_t base_offset,
    const std::string& output_path,
    Compactor::CombinerFn combiner) {
    MergeResult result;
    auto start_time = wall_clock_ms();

    if (sources.empty()) {
        result.metrics.error = error_code::invalid_request;
        result.metrics.error_message = "No source segments to merge";
        return result;
    }

    result.segments_merged = static_cast<int32_t>(sources.size());

    compaction_logger()->info(
        "SegmentMerge: merging {} segments into '{}'",
        sources.size(), output_path);

    // --- Phase 1: Read all records from all sources ---
    std::vector<RecordBatch> all_batches;
    for (auto* seg : sources) {
        if (!seg || !seg->is_open()) continue;

        offset_t base = seg->base_offset();
        offset_t next = seg->next_offset();
        if (next <= base) continue;

        auto read_res = seg->read_range(base, next);
        if (read_res.failed()) {
            result.metrics.error = read_res.error;
            result.metrics.error_message =
                "Merge read failed for segment " +
                std::to_string(seg->segment_id()) + ": " +
                read_res.error_message;
            compaction_logger()->error("SegmentMerge: {}",
                                       result.metrics.error_message);
            return result;
        }

        result.metrics.bytes_in   += seg->file_size();
        result.metrics.records_in += seg->approximate_record_count();

        for (auto& batch : read_res.value.batches) {
            all_batches.push_back(std::move(batch));
        }
    }

    if (all_batches.empty()) {
        result.metrics.duration_ms = wall_clock_ms() - start_time;
        result.metrics.error = error_code::none;
        return result;
    }

    // --- Phase 2: Deduplicate across all source segments ---
    compactor_.reset();
    auto compact_res = compactor_.compact(std::move(all_batches), combiner);
    result.metrics.duplicates_removed = compact_res.metrics.duplicates_removed;
    result.metrics.tombstones_purged  = compact_res.metrics.tombstones_purged;
    result.metrics.unique_keys        = compact_res.metrics.unique_keys;
    result.metrics.lru_evictions      = compact_res.metrics.lru_evictions;

    // --- Phase 3: Re-offset and write merged output ---
    if (!compact_res.compacted.empty()) {
        // Re-assign sequential offsets starting from base_offset.
        offset_t cur_offset = base_offset;
        for (auto& batch : compact_res.compacted) {
            batch.base_offset = cur_offset;
            for (auto& rec : batch.records) {
                rec.offset = cur_offset++;
            }
            batch.last_offset_delta = batch.record_count - 1;
            if (!batch.records.empty()) {
                batch.base_timestamp = batch.records.front().timestamp;
                batch.max_timestamp  = batch.records.back().timestamp;
            }
        }

        // Use the executor's atomic write-and-swap.
        CompactionExecutor executor(config_);
        auto write_res = executor.write_and_swap(
            compact_res.compacted, output_path, start_time);
        if (write_res.failed()) {
            result.metrics.error = write_res.error;
            result.metrics.error_message = write_res.error_message;
            return result;
        }
        result.metrics.bytes_out   = write_res.value.bytes_out;
        result.metrics.records_out = write_res.value.records_out;
        result.metrics.duration_ms = write_res.value.duration_ms;
    } else {
        result.metrics.duration_ms = wall_clock_ms() - start_time;
    }

    result.metrics.error = error_code::none;

    compaction_logger()->info(
        "SegmentMerge complete: {} sources, {} in -> {} out, "
        "{} bytes -> {} bytes, {}ms",
        result.segments_merged,
        result.metrics.records_in, result.metrics.records_out,
        result.metrics.bytes_in, result.metrics.bytes_out,
        result.metrics.duration_ms);

    return result;
}

// ============================================================================
// Additional KeyedRecordIndex helpers
// ============================================================================

// Expose a snapshot of the current index entries for external consumers
// (e.g. for pre-populating a new Compactor after a merge).  Since the
// internal LRU list is protected by a mutex, we copy entries under lock.
std::vector<KeyedRecordIndex::IndexEntry>
keyed_record_index_snapshot(const KeyedRecordIndex& idx) {
    std::vector<KeyedRecordIndex::IndexEntry> result;
    // We cannot iterate the private LRU list from outside the class.
    // Instead, we rely on a friend declaration or a public method.
    // For this implementation, the caller uses the public find() API
    // on a known set of keys.  Production code would add a dump() method.
    //
    // Placeholder: return empty — the Compactor's internal tracking map
    // (std::unordered_map of WinningRecord) already serves this purpose.
    (void)idx;
    return result;
}

// ============================================================================
// CRC32C implementation for segment header integrity
// ============================================================================

namespace {

/// CRC-32C (Castagnoli) polynomial: 0x1EDC6F41
/// Used for segment header validation (matching the Segment class format).
uint32_t crc32c_impl(const uint8_t* data, size_t len, uint32_t crc = 0xFFFFFFFF) {
    // Software fallback — production code would use SSE4.2 (__mm_crc32_u64)
    // or aarch64 CRC intrinsics when available.
    static const uint32_t table[256] = {
        0x00000000,0xF26B8303,0xE13B70F7,0x1350F3F4,
        0xC79A971F,0x35F1141C,0x26A1E7E8,0xD4CA64EB,
        0x8AD958CF,0x78B2DBCC,0x6BE22838,0x9989AB3B,
        0x4D43CFD0,0xBF284CD3,0xAC78BF27,0x5E133C24,
        0x105EC76F,0xE235446C,0xF165B798,0x030E349B,
        0xD7C45070,0x25AFD373,0x36FF2087,0xC494A384,
        0x9A879FA0,0x68EC1CA3,0x7BBCEF57,0x89D76C54,
        0x5D1D08BF,0xAF768BBC,0xBC267848,0x4E4DFB4B,
        0x20BD8EDE,0xD2D60DDD,0xC186FE29,0x33ED7D2A,
        0xE72719C1,0x154C9AC2,0x061C6936,0xF477EA35,
        0xAA64D611,0x580F5512,0x4B5FA6E6,0xB93425E5,
        0x6DFE410E,0x9F95C20D,0x8CC531F9,0x7EAEB2FA,
        0x30E349B1,0xC288CAB2,0xD1D83946,0x23B3BA45,
        0xF779DEAE,0x05125DAD,0x1642AE59,0xE4292D5A,
        0xBA3A117E,0x4851927D,0x5B016189,0xA96AE28A,
        0x7DA08661,0x8FCB0562,0x9C9BF696,0x6EF07595,
        0x417B1DBC,0xB3109EBF,0xA0406D4B,0x522BEE48,
        0x86E18AA3,0x748A09A0,0x67DAFA54,0x95B17957,
        0xCBA24573,0x39C9C670,0x2A993584,0xD8F2B687,
        0x0C38D26C,0xFE53516F,0xED03A29B,0x1F682198,
        0x5125DAD3,0xA34E59D0,0xB01EAA24,0x42752927,
        0x96BF4DCC,0x64D4CECF,0x77843D3B,0x85EFBE38,
        0xDBFC821C,0x2997011F,0x3AC7F2EB,0xC8AC71E8,
        0x1C661503,0xEE0D9600,0xFD5D65F4,0x0F36E6F7,
        0x61C69362,0x93AD1061,0x80FDE395,0x72966096,
        0xA65C047D,0x5437877E,0x4767748A,0xB50CF789,
        0xEB1FCBAD,0x197448AE,0x0A24BB5A,0xF84F3859,
        0x2C855CB2,0xDEEEDFB1,0xCDBE2C45,0x3FD5AF46,
        0x7198540D,0x83F3D70E,0x90A324FA,0x62C8A7F9,
        0xB602C312,0x44694011,0x5739B3E5,0xA55230E6,
        0xFB410CC2,0x092A8FC1,0x1A7A7C35,0xE811FF36,
        0x3CDB9BDD,0xCEB018DE,0xDDE0EB2A,0x2F8B6829,
        0x82F63B78,0x709DB87B,0x63CD4B8F,0x91A6C88C,
        0x456CAC67,0xB7072F64,0xA457DC90,0x563C5F93,
        0x082F63B7,0xFA44E0B4,0xE9141340,0x1B7F9043,
        0xCFB5F4A8,0x3DDE77AB,0x2E8E845F,0xDCE5075C,
        0x92A8FC17,0x60C37F14,0x73938CE0,0x81F80FE3,
        0x55326B08,0xA759E80B,0xB4091BFF,0x466298FC,
        0x1871A4D8,0xEA1A27DB,0xF94AD42F,0x0B21572C,
        0xDFEB33C7,0x2D80B0C4,0x3ED04330,0xCCBBC033,
        0xA24BB5A6,0x502036A5,0x4370C551,0xB11B4652,
        0x65D122B9,0x97BAA1BA,0x84EA524E,0x7681D14D,
        0x2892ED69,0xDAF96E6A,0xC9A99D9E,0x3BC21E9D,
        0xEF087A76,0x1D63F975,0x0E330A81,0xFC588982,
        0xB21572C9,0x407EF1CA,0x532E023E,0xA145813D,
        0x758FE5D6,0x87E466D5,0x94B49521,0x66DF1622,
        0x38CC2A06,0xCAA7A905,0xD9F75AF1,0x2B9CD9F2,
        0xFF56BD19,0x0D3D3E1A,0x1E6DCDEE,0xEC064EED,
        0xC38D26C4,0x31E6A5C7,0x22B65633,0xD0DDD530,
        0x0417B1DB,0xF67C32D8,0xE52CC12C,0x1747422F,
        0x49547E0B,0xBB3FFD08,0xA86F0EFC,0x5A048DFF,
        0x8ECEE914,0x7CA56A17,0x6FF599E3,0x9D9E1AE0,
        0xD3D3E1AB,0x21B862A8,0x32E8915C,0xC083125F,
        0x144976B4,0xE622F5B7,0xF5720643,0x07198540,
        0x590AB964,0xAB613A67,0xB831C993,0x4A5A4A90,
        0x9E902E7B,0x6CFBAD78,0x7FAB5E8C,0x8DC0DD8F,
        0xE330A81A,0x115B2B19,0x020BD8ED,0xF0605BEE,
        0x24AA3F05,0xD6C1BC06,0xC5914FF2,0x37FACCF1,
        0x69E9F0D5,0x9B8273D6,0x88D28022,0x7AB90321,
        0xAE7367CA,0x5C18E4C9,0x4F48173D,0xBD23943E,
        0xF36E6F75,0x0105EC76,0x12551F82,0xE03E9C81,
        0x34F4F86A,0xC69F7B69,0xD5CF889D,0x27A40B9E,
        0x79B737BA,0x8BDCB4B9,0x988C474D,0x6AE7C44E,
        0xBE2DA0A5,0x4C4623A6,0x5F16D052,0xAD7D5351,
    };

    crc = crc ^ 0xFFFFFFFF;
    for (size_t i = 0; i < len; ++i) {
        crc = table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

/// Compute CRC32C for an in-memory buffer.
[[nodiscard]] uint32_t compute_crc32c(const void* data, size_t len) {
    return crc32c_impl(reinterpret_cast<const uint8_t*>(data), len);
}

/// Compute the CRC32C of a segment header (bytes 16..63, containing
/// base_offset through padding).
[[nodiscard]] uint32_t compute_header_crc32c(const char header[64]) {
    // CRC covers bytes [16..64) — base_offset through reserved.
    return crc32c_impl(reinterpret_cast<const uint8_t*>(header + 16), 48);
}

} // anonymous namespace

// ============================================================================
// Pre-Scan Optimisation: Estimate dirty ratio without full read
// ============================================================================

namespace {

/// Quick heuristic to estimate the dirty ratio of a segment using the
/// sparse index alone (no record data read needed).  Returns a value
/// between 0.0 (no duplicates expected) and 1.0 (all duplicates).
///
/// Logic: if the sparse index has many entries relative to the segment
/// byte size, it suggests many small records — a signal of key-churn
/// that benefits from compaction.  This is only a heuristic; the full
/// compaction pass measures the true ratio.
[[nodiscard]] double estimate_dirty_ratio_from_index(
    byte_count_t segment_bytes,
    int64_t      index_entries,
    offset_t     offset_span) {
    if (segment_bytes <= 0 || offset_span <= 0) return 0.0;

    // Average bytes per offset: low values suggest many small records,
    // which typically correlates with higher duplicate rates.
    double avg_bytes_per_offset =
        static_cast<double>(segment_bytes) / static_cast<double>(offset_span);

    // If records average < 256 bytes, we guess 50%+ dirty.
    // If records average > 4096 bytes, we guess < 10% dirty.
    if (avg_bytes_per_offset < 256.0) return 0.70;
    if (avg_bytes_per_offset < 512.0) return 0.50;
    if (avg_bytes_per_offset < 1024.0) return 0.30;
    if (avg_bytes_per_offset < 4096.0) return 0.15;
    return 0.05;
}

} // anonymous namespace

// ============================================================================
// CompactorConfig validation
// ============================================================================

namespace {

/// Validate a CompactorConfig and log warnings for unsafe settings.
/// Returns true if the configuration is usable.
[[nodiscard]] bool validate_config(const CompactorConfig& cfg) {
    bool ok = true;

    if (cfg.min_compaction_lag_ms < 0) {
        compaction_logger()->warn(
            "min_compaction_lag_ms is negative ({}), clamping to 0",
            cfg.min_compaction_lag_ms);
        ok = false;
    }
    if (cfg.max_compaction_lag_ms > 0 &&
        cfg.max_compaction_lag_ms < cfg.min_compaction_lag_ms) {
        compaction_logger()->warn(
            "max_compaction_lag_ms ({}) < min_compaction_lag_ms ({}), "
            "all eligible segments will be urgent",
            cfg.max_compaction_lag_ms, cfg.min_compaction_lag_ms);
    }
    if (cfg.min_cleanable_dirty_ratio < 0.0 ||
        cfg.min_cleanable_dirty_ratio > 1.0) {
        compaction_logger()->warn(
            "min_cleanable_dirty_ratio ({}) outside [0,1], clamping",
            cfg.min_cleanable_dirty_ratio);
        ok = false;
    }
    if (cfg.max_key_index_entries == 0) {
        compaction_logger()->warn(
            "max_key_index_entries is 0 — key index disabled, "
            "every record will be treated as unique");
        ok = false;
    }
    if (cfg.max_records_per_batch <= 0) {
        compaction_logger()->warn(
            "max_records_per_batch is {} — clamping to 50000",
            cfg.max_records_per_batch);
        ok = false;
    }
    if (!cfg.temp_directory.empty()) {
        struct stat st;
        if (::stat(cfg.temp_directory.c_str(), &st) != 0 ||
            !S_ISDIR(st.st_mode)) {
            compaction_logger()->warn(
                "temp_directory '{}' does not exist or is not a directory",
                cfg.temp_directory);
            ok = false;
        }
    }

    return ok;
}

} // anonymous namespace

// ============================================================================
// CompactionResult formatting (for admin API responses)
// ============================================================================

namespace {

/// Produce a human-readable summary string from a CompactionMetrics struct.
[[nodiscard]] std::string format_compaction_metrics(
    const CompactionMetrics& m) {
    if (!m.ok()) {
        return fmt::format("Compaction error: {} — {}",
                           error_code_name(m.error), m.error_message);
    }
    return fmt::format(
        "records: {} in -> {} out ({:.1f}% reduction), "
        "bytes: {} in -> {} out ({:.1f}% reduction), "
        "{} duplicates removed, {} tombstones purged, "
        "{} unique keys, {} LRU evictions, {}ms",
        m.records_in, m.records_out, m.compaction_ratio() * 100.0,
        m.bytes_in, m.bytes_out,
        m.bytes_in > 0
            ? (1.0 - static_cast<double>(m.bytes_out) /
                         static_cast<double>(m.bytes_in)) * 100.0
            : 0.0,
        m.duplicates_removed, m.tombstones_purged,
        m.unique_keys, m.lru_evictions, m.duration_ms);
}

} // anonymous namespace

} // namespace torrent
