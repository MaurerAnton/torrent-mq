#pragma once

/**
 * torrent-mq — Log Crash Recovery
 *
 * Scans segment files after a crash, validates headers and batch CRCs,
 * detects and repairs corruption, and prepares segments for index rebuild.
 *
 * Recovery flow:
 *   1. Scan data_dir for *.log files, sorted by base_offset.
 *   2. validate_segment() — check header magic/version/CRC, scan all batches.
 *   3. If last segment is corrupt, repair_segment() — truncate to last valid batch.
 *   4. Flag segments needing sparse-index rebuild.
 *   5. Return RecoveryResult with per-segment status.
 */

#include <cstdint>
#include <string>
#include <vector>

#include "torrent/common/types.h"
#include "torrent/storage/types.h"

namespace torrent {

// ============================================================================
// LogRecovery — Crash Recovery Engine
// ============================================================================

/**
 * Crash recovery: validates all log segments, repairs the last segment
 * if it has a torn write, and produces a RecoveryResult describing the
 * state of every segment found.
 *
 * Usage (typically from LogManager::open()):
 *   LogRecovery recovery;
 *   auto result = recovery.recover(data_dir);
 *   if (result.has_corruption()) {
 *       // handle: quarantine, truncate, operator alert
 *   }
 *   // For each recovered segment, rebuild sparse index if needed.
 */
class LogRecovery {
public:
    // ------------------------------------------------------------------
    // RecoveryResult — summary of a recovery run
    // ------------------------------------------------------------------

    struct RecoveryResult {
        /// Per-segment recovery information.
        struct SegmentRecoveryInfo {
            /// Recovery status for a single segment.
            enum class State : uint8_t {
                valid          = 0,  ///< Header and all batches passed validation
                header_corrupt = 1,  ///< Magic/version/CRC mismatch in header
                batch_corrupt  = 2,  ///< Header OK, one or more batches failed CRC
            };

            State           state            = State::valid;
            std::string     file_path;
            uint64_t        segment_id       = 0;
            offset_t        base_offset      = kInvalidOffset;
            offset_t        next_offset      = kInvalidOffset;
            timestamp_ms_t  created_at       = 0;
            timestamp_ms_t  max_timestamp    = 0;
            byte_count_t    file_size        = 0;
            compression_type compression     = compression_type::none;
            bool            is_sealed        = false;
            bool            header_valid     = false;
            bool            index_valid      = true;   ///< Set false when index needs rebuild
            bool            has_torn_write   = false;  ///< Partial batch at end of file
            bool            repair_attempted = false;
            bool            repair_success   = false;
            int32_t         batches_found    = 0;
            byte_count_t    total_batch_bytes = 0;
            offset_t        last_valid_offset = kInvalidOffset; ///< Offset before corruption

            /// Batch base_offsets found during scanning (for index rebuild).
            std::vector<offset_t> batch_offsets;

            /// Human-readable error string (empty if valid).
            std::string     error_message;

            SegmentRecoveryInfo();
        };

        /// All segments discovered, in base_offset order.
        std::vector<SegmentRecoveryInfo> segments;

        /// Total segment files discovered.
        int32_t       segments_found      = 0;

        /// Number of fully valid segments.
        int32_t       valid_segments      = 0;

        /// Number of corrupt segments (header or batch).
        int32_t       corrupted_segments  = 0;

        /// Number of segments successfully repaired.
        int32_t       repaired_segments   = 0;

        /// Total batch count across all segments.
        int32_t       total_batches       = 0;

        /// Total batch payload bytes across all segments.
        byte_count_t  total_batch_bytes   = 0;

        /// Number of segments flagged for index rebuild.
        int32_t       indexes_rebuilt     = 0;

        /// Wall-clock time the recovery began (ms since epoch).
        timestamp_ms_t recovery_time       = 0;

        /// Wall-clock duration of the recovery run.
        duration_ms_t duration_ms          = 0;

        /// Error code (none on success).
        /// Even with corruption, error may be none — check has_corruption().
        error_code    error               = error_code::none;

        /// Human-readable error description.
        std::string   error_message;

        [[nodiscard]] bool ok()              const noexcept;
        [[nodiscard]] bool has_corruption()  const noexcept;
    };

    // ------------------------------------------------------------------
    // SegmentHeaderParser — lightweight header-only parser
    // ------------------------------------------------------------------

    /**
     * Parses only the segment header without scanning batches.
     * Useful for quick inspection and directory listing without
     * the overhead of full validation.
     */
    struct SegmentHeaderParser {
        struct ParsedHeader {
            std::string     file_path;
            uint32_t        magic          = 0;
            uint16_t        format_version = 0;
            uint16_t        header_size    = 0;
            uint32_t        header_crc     = 0;
            offset_t        base_offset    = kInvalidOffset;
            uint64_t        segment_id     = 0;
            timestamp_ms_t  created_at     = 0;
            int8_t          compression    = 0;
            uint8_t         flags          = 0;
            bool            is_sealed      = false;
            bool            header_crc_ok  = false;
            bool            valid          = false;
            std::string     error;
        };

        /// Parse only the 64-byte header from a segment file.
        static ParsedHeader parse(const std::string& path);
    };

    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    LogRecovery();
    ~LogRecovery();

    LogRecovery(const LogRecovery&) = delete;
    LogRecovery& operator=(const LogRecovery&) = delete;
    LogRecovery(LogRecovery&&) noexcept = default;
    LogRecovery& operator=(LogRecovery&&) noexcept = default;

    // ------------------------------------------------------------------
    // Recovery operations
    // ------------------------------------------------------------------

    /// Run full crash recovery on `data_dir`.
    /// Scans all *.log files, validates headers and batch CRCs, repairs
    /// the last segment if needed, and returns per-segment status.
    RecoveryResult recover(const std::string& data_dir);

    /// Validate a single segment file: check header, scan all batches,
    /// verify CRCs.  Returns a SegmentRecoveryInfo.
    RecoveryResult::SegmentRecoveryInfo validate_segment(const std::string& path);

    /// Attempt to repair a corrupted segment by truncating to the last
    /// valid batch boundary.  Only works on segments with a valid header.
    /// Returns updated SegmentRecoveryInfo with repair_attempted/success flags.
    RecoveryResult::SegmentRecoveryInfo repair_segment(const std::string& path);

    /// Rebuild the sparse index for a segment described by `info`.
    /// Delegates to Segment::rebuild_index() at the LogManager level.
    /// This is a stub that validates the segment is in a rebuildable state.
    result<void> rebuild_index_for(const RecoveryResult::SegmentRecoveryInfo& info);
};

} // namespace torrent
