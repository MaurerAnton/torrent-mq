/**
 * raft_snapshot.cpp — Snapshot Creation, Installation, and Chunked Transfer
 *
 * Implements Raft log compaction via state-machine snapshots.  Snapshots
 * replace the log prefix up to last_included_index, keeping only the
 * latest state-machine image plus Raft metadata on disk.
 *
 * File Format (version 1)
 * -----------------------
 *   ┌──────────────────────────────────────────────────────────────────────┐
 *   │                            HEADER (fixed)                            │
 *   │  magic:8        = "RAFTSSHOT"                                       │
 *   │  version:4      = 1                                                 │
 *   │  flags:4        = reserved                                          │
 *   │  last_included_index:8                                              │
 *   │  last_included_term:8                                               │
 *   │  membership_count:4                                                 │
 *   │  joint_membership_count:4                                           │
 *   │  created_at_ms:8                                                    │
 *   │  header_crc32c:4  (covers all preceding header fields)              │
 *   ├──────────────────────────────────────────────────────────────────────┤
 *   │  membership entries: membership_count * 4 bytes (int32_t node ids)  │
 *   │  joint entries:     joint_membership_count * 4 bytes                │
 *   ├──────────────────────────────────────────────────────────────────────┤
 *   │                            BODY (variable)                           │
 *   │  body_size:8                                                        │
 *   │  body_crc32c:4                                                      │
 *   │  state_machine_data: body_size bytes                                │
 *   └──────────────────────────────────────────────────────────────────────┘
 *
 * Chunked Transfer
 * ----------------
 *   InstallSnapshotRequest carries offset + data.  The receiver assembles
 *   chunks into a temporary buffer.  On the final chunk (done=true) the
 *   complete snapshot is validated and applied.
 *
 *   The sender slices the snapshot file into chunks of configurable size
 *   (default 1 MiB) and sends them sequentially.  Progress is tracked
 *   per follower to support resumption and concurrent transfers.
 *
 * Thread Safety
 *   Snapshot creation is serialized under the RaftNode's exclusive lock.
 *   Transfer reads are lock-free (read-only memory-mapped or streaming).
 */

#include "torrent/consensus/raft_types.h"
#include "torrent/common/types.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace torrent::raft {

// ============================================================================
// Constants
// ============================================================================

namespace {

/// File magic: "RAFTSSHOT" — 9 bytes.
static const char kSnapshotMagic[9] = "RAFTSSHOT";

/// Current snapshot format version.
static constexpr uint32_t kSnapshotVersion = 1;

/// Header flags (reserved for future use).
static constexpr uint32_t kFlagNone          = 0x00000000;
static constexpr uint32_t kFlagCompressed    = 0x00000001;  // reserved.

/// Fixed header size (before membership lists): 8+4+4+8+8+4+4+8+4 = 52 bytes.
static constexpr size_t kSnapshotHeaderFixedSize = 52;

/// Maximum snapshot file size (1 GiB).  Larger snapshots are rejected.
static constexpr size_t kMaxSnapshotSize = 1024 * 1024 * 1024;

/// Default chunk size for snapshot transfer (1 MiB).
static constexpr size_t kDefaultChunkSize = 1024 * 1024;

/// Maximum number of concurrent snapshot transfers (per node).
static constexpr size_t kMaxConcurrentTransfers = 4;

}  // namespace

// ============================================================================
// CRC32C (same polynomial as raft_log.cpp; duplicated for self-containedness)
// ============================================================================

namespace {

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

}  // namespace

// ============================================================================
// Binary encoding helpers
// ============================================================================

namespace {

void write_u32_le(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
}

void write_u64_le(std::vector<uint8_t>& buf, uint64_t v) {
    for (int i = 0; i < 8; ++i)
        buf.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
}

void write_i64_le(std::vector<uint8_t>& buf, int64_t v) {
    write_u64_le(buf, static_cast<uint64_t>(v));
}

void write_i32_le(std::vector<uint8_t>& buf, int32_t v) {
    write_u32_le(buf, static_cast<uint32_t>(v));
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

[[nodiscard]] int32_t read_i32_le(const uint8_t*& p) {
    return static_cast<int32_t>(read_u32_le(p));
}

}  // namespace

// ============================================================================
// SnapshotSerializer — writes snapshot files
// ============================================================================

/// Serializes Raft metadata + state machine data into the snapshot binary
/// format.  The output is written to an on-disk file atomically.
class SnapshotSerializer {
public:
    /// Build a snapshot from metadata + raw state-machine state.
    /// @param snapshot_dir  Directory to store the snapshot file.
    /// @param last_included_index  Last log index covered by this snapshot.
    /// @param last_included_term   Term of the last included entry.
    /// @param membership           Cluster membership at snapshot time.
    /// @param state_data           Serialized state machine data.
    /// @param created_at_ms        Creation timestamp (ms since epoch).
    /// @return                    RaftSnapshot metadata on success, or nullopt.
    [[nodiscard]] static std::optional<RaftSnapshot> create(
        const std::string& snapshot_dir,
        LogIndex last_included_index,
        RaftTerm last_included_term,
        const RaftMembership& membership,
        const std::vector<uint8_t>& state_data,
        timestamp_ms_t created_at_ms) {

        namespace fs = std::filesystem;

        // Ensure directory exists.
        std::error_code ec;
        fs::create_directories(snapshot_dir, ec);
        if (ec) {
            SPDLOG_ERROR("raft_snapshot: cannot create snapshot dir {}: {}",
                         snapshot_dir, ec.message());
            return std::nullopt;
        }

        // Generate unique filename.
        std::string filename = fmt::format(
            "snapshot-{:020}-{:020}-{:016}.snap",
            last_included_index, last_included_term, created_at_ms);

        std::string tmp_path = (fs::path(snapshot_dir) / (filename + ".tmp")).string();
        std::string final_path = (fs::path(snapshot_dir) / filename).string();

        // Serialize to buffer.
        auto serialized = serialize(last_included_index, last_included_term,
                                    membership, state_data, created_at_ms);

        if (serialized.size() > kMaxSnapshotSize) {
            SPDLOG_ERROR("raft_snapshot: snapshot too large: {} bytes (max {})",
                         serialized.size(), kMaxSnapshotSize);
            return std::nullopt;
        }

        // Write to temp file, then rename atomically.
        std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
        if (!out) {
            SPDLOG_ERROR("raft_snapshot: cannot open tmp file {}", tmp_path);
            return std::nullopt;
        }

        out.write(reinterpret_cast<const char*>(serialized.data()),
                  static_cast<std::streamsize>(serialized.size()));
        out.close();

        if (!out) {
            SPDLOG_ERROR("raft_snapshot: write failed for {}", tmp_path);
            fs::remove(tmp_path, ec);
            return std::nullopt;
        }

        // Fsync.
        int fd = ::open(tmp_path.c_str(), O_RDONLY);
        if (fd >= 0) {
            ::fsync(fd);
            ::close(fd);
        }

        // Atomic rename.
        fs::rename(tmp_path, final_path, ec);
        if (ec) {
            SPDLOG_ERROR("raft_snapshot: rename failed {} -> {}: {}",
                         tmp_path, final_path, ec.message());
            fs::remove(tmp_path, ec);
            return std::nullopt;
        }

        // Determine file size.
        uint64_t file_size = fs::file_size(final_path, ec);
        if (ec) file_size = serialized.size();

        RaftSnapshot snap;
        snap.file_path          = final_path;
        snap.last_included_index = last_included_index;
        snap.last_included_term  = last_included_term;
        snap.membership          = membership;
        snap.file_size_bytes     = file_size;
        snap.created_at_ms       = created_at_ms;

        SPDLOG_INFO("raft_snapshot: created {} ({:.2f} MiB, up to index {})",
                    final_path,
                    static_cast<double>(file_size) / (1024 * 1024),
                    last_included_index);

        return snap;
    }

    /// Serialize to an in-memory buffer (for testing or direct use).
    [[nodiscard]] static std::vector<uint8_t> serialize(
        LogIndex last_included_index,
        RaftTerm last_included_term,
        const RaftMembership& membership,
        const std::vector<uint8_t>& state_data,
        timestamp_ms_t created_at_ms) {

        std::vector<uint8_t> buf;

        // --- Header (fixed part) ---
        // magic: 8 bytes.
        buf.insert(buf.end(), kSnapshotMagic, kSnapshotMagic + 8);

        // version:4, flags:4.
        write_u32_le(buf, kSnapshotVersion);
        write_u32_le(buf, kFlagNone);

        // last_included_index:8, last_included_term:8.
        write_i64_le(buf, static_cast<int64_t>(last_included_index));
        write_i64_le(buf, static_cast<int64_t>(last_included_term));

        // membership_count:4, joint_membership_count:4.
        write_u32_le(buf, static_cast<uint32_t>(membership.members.size()));
        write_u32_le(buf, static_cast<uint32_t>(membership.joint_members.size()));

        // created_at_ms:8.
        write_i64_le(buf, static_cast<int64_t>(created_at_ms));

        // placeholder for header_crc32c:4 (fill after we know the value).
        size_t crc_slot = buf.size();
        write_u32_le(buf, 0);  // placeholder.

        // --- Membership lists ---
        for (auto id : membership.members) {
            write_i32_le(buf, id);
        }
        for (auto id : membership.joint_members) {
            write_i32_le(buf, id);
        }

        // --- Compute and patch header CRC ---
        uint32_t hdr_crc = crc32c_update(0, buf.data(), buf.size());
        // Patch at crc_slot (little-endian).
        buf[crc_slot]     = static_cast<uint8_t>(hdr_crc & 0xFF);
        buf[crc_slot + 1] = static_cast<uint8_t>((hdr_crc >> 8) & 0xFF);
        buf[crc_slot + 2] = static_cast<uint8_t>((hdr_crc >> 16) & 0xFF);
        buf[crc_slot + 3] = static_cast<uint8_t>((hdr_crc >> 24) & 0xFF);

        // --- Body ---
        // body_size:8.
        write_u64_le(buf, static_cast<uint64_t>(state_data.size()));

        // body_crc32c:4.
        uint32_t body_crc = crc32c_span(state_data);
        write_u32_le(buf, body_crc);

        // state_machine_data.
        buf.insert(buf.end(), state_data.begin(), state_data.end());

        return buf;
    }

private:
    SnapshotSerializer() = default;
};

// ============================================================================
// SnapshotDeserializer — reads and validates snapshot files
// ============================================================================

/// Reads a snapshot file from disk, validates header CRC and body CRC, and
/// returns the parsed metadata + state machine data.
class SnapshotDeserializer {
public:
    /// Parsed snapshot contents.
    struct ParsedSnapshot {
        LogIndex last_included_index = 0;
        RaftTerm last_included_term  = kNoTerm;
        RaftMembership membership;
        std::vector<uint8_t> state_data;
        timestamp_ms_t created_at_ms = 0;
        bool valid = false;
    };

    /// Parse a snapshot from a file path.
    [[nodiscard]] static ParsedSnapshot parse(const std::string& path) {
        // Read entire file.
        std::ifstream in(path, std::ios::binary | std::ios::ate);
        if (!in) {
            SPDLOG_ERROR("raft_snapshot: cannot open snapshot {}", path);
            return {};
        }

        auto file_size = static_cast<size_t>(in.tellg());
        if (file_size < kSnapshotHeaderFixedSize) {
            SPDLOG_ERROR("raft_snapshot: {} too small ({} bytes)", path, file_size);
            return {};
        }

        std::vector<uint8_t> buf(file_size);
        in.seekg(0);
        in.read(reinterpret_cast<char*>(buf.data()),
                static_cast<std::streamsize>(file_size));
        if (!in) {
            SPDLOG_ERROR("raft_snapshot: read failed for {}", path);
            return {};
        }

        return parse(buf);
    }

    /// Parse from an in-memory buffer (e.g., assembled from chunks).
    [[nodiscard]] static ParsedSnapshot parse(const std::vector<uint8_t>& buf) {
        ParsedSnapshot result;

        if (buf.size() < kSnapshotHeaderFixedSize) {
            SPDLOG_ERROR("raft_snapshot: buffer too small ({} bytes)", buf.size());
            return result;
        }

        const uint8_t* p = buf.data();
        const uint8_t* end = buf.data() + buf.size();

        // Magic.
        if (std::memcmp(p, kSnapshotMagic, 8) != 0) {
            SPDLOG_ERROR("raft_snapshot: bad magic in snapshot");
            return result;
        }
        p += 8;

        // Version.
        uint32_t version = read_u32_le(p);
        if (version != kSnapshotVersion) {
            SPDLOG_ERROR("raft_snapshot: unsupported version {} (expected {})",
                         version, kSnapshotVersion);
            return result;
        }

        // Flags (skip).
        uint32_t flags = read_u32_le(p);

        // last_included_index, last_included_term.
        result.last_included_index = static_cast<LogIndex>(read_i64_le(p));
        result.last_included_term  = static_cast<RaftTerm>(read_i64_le(p));

        // Membership counts.
        uint32_t member_count       = read_u32_le(p);
        uint32_t joint_member_count = read_u32_le(p);

        // created_at_ms.
        result.created_at_ms = static_cast<timestamp_ms_t>(read_i64_le(p));

        // header_crc32c.
        uint32_t stored_hdr_crc = read_u32_le(p);

        // Verify header CRC (over all bytes up to but not including the CRC field itself).
        // The CRC covers bytes [0..crc_offset).
        size_t crc_offset = static_cast<size_t>(p - buf.data()) - 4;  // back up to CRC slot.
        uint32_t computed_hdr_crc = crc32c_update(0, buf.data(), crc_offset);
        if (computed_hdr_crc != stored_hdr_crc) {
            SPDLOG_ERROR("raft_snapshot: header CRC mismatch (expected {:08X}, got {:08X})",
                         stored_hdr_crc, computed_hdr_crc);
            return result;
        }

        // Read membership lists.
        size_t membership_list_size =
            (static_cast<size_t>(member_count) + joint_member_count) * 4;
        if (static_cast<size_t>(end - p) < membership_list_size) {
            SPDLOG_ERROR("raft_snapshot: truncated membership list");
            return result;
        }

        result.membership.members.reserve(member_count);
        for (uint32_t i = 0; i < member_count; ++i) {
            result.membership.members.push_back(read_i32_le(p));
        }

        result.membership.joint_members.reserve(joint_member_count);
        for (uint32_t i = 0; i < joint_member_count; ++i) {
            result.membership.joint_members.push_back(read_i32_le(p));
        }
        result.membership.config_index = result.last_included_index;

        // Body.
        if (static_cast<size_t>(end - p) < 12) {
            SPDLOG_ERROR("raft_snapshot: truncated body header");
            return result;
        }

        uint64_t body_size = read_u64_le(p);
        uint32_t stored_body_crc = read_u32_le(p);

        if (static_cast<size_t>(end - p) < body_size) {
            SPDLOG_ERROR("raft_snapshot: truncated body (expected {} bytes, have {})",
                         body_size, static_cast<size_t>(end - p));
            return result;
        }

        result.state_data.assign(p, p + body_size);
        p += body_size;

        // Verify body CRC.
        uint32_t computed_body_crc = crc32c_span(result.state_data);
        if (computed_body_crc != stored_body_crc) {
            SPDLOG_ERROR("raft_snapshot: body CRC mismatch (expected {:08X}, got {:08X})",
                         stored_body_crc, computed_body_crc);
            return result;
        }

        if (flags & kFlagCompressed) {
            SPDLOG_WARN("raft_snapshot: compressed flag set but decompression not yet"
                        " implemented; treating as uncompressed");
        }

        result.valid = true;
        return result;
    }
};

// ============================================================================
// SnapshotTransfer — chunked snapshot transfer manager
// ============================================================================

/// Manages chunked snapshot transfers for multiple peers.
/// Tracks progress per transfer and supports concurrent streaming.
class SnapshotTransferManager {
public:
    /// Progress state for a single transfer.
    struct TransferProgress {
        RaftNodeId peer_id;
        std::string snapshot_path;
        uint64_t total_size = 0;
        uint64_t bytes_sent = 0;
        uint64_t chunks_sent = 0;
        uint64_t chunks_total = 0;
        std::chrono::steady_clock::time_point started_at;
        std::chrono::steady_clock::time_point last_chunk_at;
        bool completed = false;
        bool failed    = false;
        std::string error;
    };

    explicit SnapshotTransferManager(size_t chunk_size = kDefaultChunkSize,
                                      size_t max_concurrent = kMaxConcurrentTransfers)
        : chunk_size_(chunk_size)
        , max_concurrent_(max_concurrent)
    {}

    SnapshotTransferManager(const SnapshotTransferManager&) = delete;
    SnapshotTransferManager& operator=(const SnapshotTransferManager&) = delete;
    SnapshotTransferManager(SnapshotTransferManager&&) = default;
    SnapshotTransferManager& operator=(SnapshotTransferManager&&) = default;

    // -- Transfer lifecycle --------------------------------------------------

    /// Start a new snapshot transfer to a peer.  Returns true if transfer
    /// was successfully started.
    [[nodiscard]] bool start_transfer(RaftNodeId peer_id,
                                       const RaftSnapshot& snapshot) {
        std::lock_guard lock(mutex_);

        // Check concurrent transfer limit.
        size_t active = 0;
        for (const auto& [pid, tp] : transfers_) {
            if (!tp.completed && !tp.failed) active++;
        }
        if (active >= max_concurrent_) {
            SPDLOG_WARN("raft_snapshot: max concurrent transfers ({}) reached",
                        max_concurrent_);
            return false;
        }

        // Check if there's already an active transfer for this peer.
        auto it = transfers_.find(peer_id);
        if (it != transfers_.end() && !it->second.completed && !it->second.failed) {
            SPDLOG_WARN("raft_snapshot: transfer already active for peer {}",
                        peer_id);
            return false;
        }

        TransferProgress tp;
        tp.peer_id        = peer_id;
        tp.snapshot_path  = snapshot.file_path;
        tp.total_size     = snapshot.file_size_bytes;
        tp.bytes_sent     = 0;
        tp.chunks_sent    = 0;
        tp.chunks_total   = (snapshot.file_size_bytes + chunk_size_ - 1)
                            / chunk_size_;
        tp.started_at     = std::chrono::steady_clock::now();
        tp.last_chunk_at  = tp.started_at;
        tp.completed      = false;
        tp.failed         = false;

        transfers_[peer_id] = tp;

        // Memory-map snapshot file for efficient chunk access.
        load_snapshot_file(snapshot.file_path, peer_id);

        SPDLOG_INFO("raft_snapshot: started transfer to peer {} ({} chunks, {} bytes)",
                    peer_id, tp.chunks_total, tp.total_size);
        return true;
    }

    /// Build the next chunk for a peer.  Returns an InstallSnapshotRequest
    /// with the chunk data, or nullopt if no more chunks or error.
    [[nodiscard]] std::optional<InstallSnapshotRequest> next_chunk(
        RaftNodeId peer_id,
        RaftTerm term,
        RaftNodeId leader_id,
        LogIndex last_included_index,
        RaftTerm last_included_term) {

        std::lock_guard lock(mutex_);

        auto it = transfers_.find(peer_id);
        if (it == transfers_.end()) {
            SPDLOG_WARN("raft_snapshot: no transfer for peer {}", peer_id);
            return std::nullopt;
        }

        auto& tp = it->second;
        if (tp.completed || tp.failed) return std::nullopt;

        // Get file data.
        auto& file_data = snapshot_files_[peer_id];
        if (file_data.empty()) {
            tp.failed = true;
            tp.error = "snapshot file not loaded";
            return std::nullopt;
        }

        uint64_t offset   = tp.bytes_sent;
        uint64_t remaining = tp.total_size - offset;
        uint64_t chunk_len = std::min(remaining, chunk_size_);

        bool is_last = (offset + chunk_len >= tp.total_size);

        InstallSnapshotRequest req;
        req.term                = term;
        req.leader_id           = leader_id;
        req.last_included_index = last_included_index;
        req.last_included_term  = last_included_term;
        req.offset              = offset;
        req.done                = is_last;

        req.data.assign(
            file_data.begin() + static_cast<ptrdiff_t>(offset),
            file_data.begin() + static_cast<ptrdiff_t>(offset + chunk_len));

        tp.bytes_sent += chunk_len;
        tp.chunks_sent++;
        tp.last_chunk_at = std::chrono::steady_clock::now();

        if (is_last) {
            tp.completed = true;
            SPDLOG_INFO("raft_snapshot: transfer to peer {} complete ({} chunks)",
                        peer_id, tp.chunks_sent);
        }

        return req;
    }

    /// Get progress for a peer.
    [[nodiscard]] std::optional<TransferProgress> get_progress(
        RaftNodeId peer_id) const {
        std::lock_guard lock(mutex_);
        auto it = transfers_.find(peer_id);
        if (it == transfers_.end()) return std::nullopt;
        return it->second;
    }

    /// Mark a transfer as failed.
    void mark_failed(RaftNodeId peer_id, std::string error) {
        std::lock_guard lock(mutex_);
        auto it = transfers_.find(peer_id);
        if (it != transfers_.end()) {
            it->second.failed = true;
            it->second.error = std::move(error);
        }
    }

    /// Clean up completed or failed transfers.
    void cleanup(RaftNodeId peer_id) {
        std::lock_guard lock(mutex_);
        transfers_.erase(peer_id);
        snapshot_files_.erase(peer_id);
    }

    /// Clean up all transfers.
    void cleanup_all() {
        std::lock_guard lock(mutex_);
        transfers_.clear();
        snapshot_files_.clear();
    }

    /// Get number of active transfers.
    [[nodiscard]] size_t active_count() const {
        std::lock_guard lock(mutex_);
        size_t count = 0;
        for (const auto& [pid, tp] : transfers_) {
            if (!tp.completed && !tp.failed) count++;
        }
        return count;
    }

    /// Resend the last chunk (e.g., after a timeout — same offset).
    [[nodiscard]] std::optional<InstallSnapshotRequest> resend_last_chunk(
        RaftNodeId peer_id,
        RaftTerm term,
        RaftNodeId leader_id,
        LogIndex last_included_index,
        RaftTerm last_included_term) {

        std::lock_guard lock(mutex_);

        auto it = transfers_.find(peer_id);
        if (it == transfers_.end() || it->second.completed || it->second.failed) {
            return std::nullopt;
        }

        auto& tp = it->second;
        auto& file_data = snapshot_files_[peer_id];
        if (file_data.empty()) return std::nullopt;

        // Re-send from the same offset (bytes_sent was already advanced).
        // For retry, we go back one chunk.
        uint64_t offset = (tp.chunks_sent > 0)
            ? tp.bytes_sent - std::min(chunk_size_, tp.bytes_sent)
            : 0;

        uint64_t remaining = tp.total_size - offset;
        uint64_t chunk_len = std::min(remaining, chunk_size_);
        bool is_last = (offset + chunk_len >= tp.total_size);

        InstallSnapshotRequest req;
        req.term                = term;
        req.leader_id           = leader_id;
        req.last_included_index = last_included_index;
        req.last_included_term  = last_included_term;
        req.offset              = offset;
        req.done                = is_last;

        req.data.assign(
            file_data.begin() + static_cast<ptrdiff_t>(offset),
            file_data.begin() + static_cast<ptrdiff_t>(offset + chunk_len));

        return req;
    }

private:
    /// Load snapshot file into memory for chunked serving.
    void load_snapshot_file(const std::string& path, RaftNodeId peer_id) {
        std::ifstream in(path, std::ios::binary | std::ios::ate);
        if (!in) {
            SPDLOG_ERROR("raft_snapshot: cannot open snapshot file {}", path);
            transfers_[peer_id].failed = true;
            transfers_[peer_id].error  = "cannot open file: " + path;
            return;
        }

        auto size = static_cast<size_t>(in.tellg());
        in.seekg(0);

        auto& buf = snapshot_files_[peer_id];
        buf.resize(size);
        in.read(reinterpret_cast<char*>(buf.data()),
                static_cast<std::streamsize>(size));

        if (!in) {
            SPDLOG_ERROR("raft_snapshot: failed to read snapshot file {}", path);
            transfers_[peer_id].failed = true;
            transfers_[peer_id].error  = "failed to read file: " + path;
            buf.clear();
            return;
        }
    }

    // -- Fields ---------------------------------------------------------------

    size_t chunk_size_;
    size_t max_concurrent_;

    mutable std::mutex mutex_;
    std::unordered_map<RaftNodeId, TransferProgress> transfers_;
    std::unordered_map<RaftNodeId, std::vector<uint8_t>> snapshot_files_;
};

// ============================================================================
// SnapshotAssembler — follower-side chunk assembly and validation
// ============================================================================

/// Receives InstallSnapshot chunks, assembles them in memory, and on the
/// final chunk validates the complete snapshot and persists it to disk.
class SnapshotAssembler {
public:
    /// State of the assembly process.
    enum class State {
        Idle,           ///< No transfer in progress.
        Receiving,      ///< Receiving chunks.
        Complete,       ///< Final chunk received, validated.
        Failed,          ///< Validation or I/O failure.
    };

    explicit SnapshotAssembler(std::string snapshot_dir)
        : snapshot_dir_(std::move(snapshot_dir))
    {}

    /// Start receiving a new snapshot.  Call on the first chunk (offset==0).
    /// Returns false if a transfer is already in progress.
    [[nodiscard]] bool begin_snapshot(LogIndex last_included_index,
                                       RaftTerm last_included_term) {
        std::lock_guard lock(mutex_);

        if (state_ == State::Receiving) {
            SPDLOG_WARN("raft_snapshot_asm: already receiving a snapshot; "
                        "aborting previous");
            reset();
        }

        state_ = State::Receiving;
        expected_last_index_ = last_included_index;
        expected_last_term_  = last_included_term;
        buffer_.clear();
        bytes_received_ = 0;

        SPDLOG_DEBUG("raft_snapshot_asm: starting snapshot receive (up to index {})",
                     last_included_index);
        return true;
    }

    /// Feed a chunk.  Returns true if the chunk was accepted.
    /// Call is_complete() after this to check for the final chunk.
    [[nodiscard]] bool receive_chunk(const InstallSnapshotRequest& req) {
        std::lock_guard lock(mutex_);

        if (state_ != State::Receiving) {
            SPDLOG_WARN("raft_snapshot_asm: received chunk while not receiving");
            return false;
        }

        // Check that offset matches expected.
        if (req.offset != bytes_received_) {
            SPDLOG_ERROR("raft_snapshot_asm: offset mismatch: expected {} got {}",
                         bytes_received_, req.offset);
            state_ = State::Failed;
            error_msg_ = "offset mismatch";
            return false;
        }

        // Check that metadata is consistent.
        if (req.last_included_index != expected_last_index_
            || req.last_included_term != expected_last_term_) {
            SPDLOG_ERROR("raft_snapshot_asm: metadata mismatch: "
                         "({},{}) vs expected ({},{})",
                         req.last_included_index, req.last_included_term,
                         expected_last_index_, expected_last_term_);
            state_ = State::Failed;
            error_msg_ = "metadata mismatch";
            return false;
        }

        // Append data.
        buffer_.insert(buffer_.end(), req.data.begin(), req.data.end());
        bytes_received_ += req.data.size();

        if (req.done) {
            // Final chunk: validate.
            bool ok = validate_and_persist();
            if (ok) {
                state_ = State::Complete;
                SPDLOG_INFO("raft_snapshot_asm: snapshot received and validated "
                            "({} bytes)", buffer_.size());
            } else {
                state_ = State::Failed;
            }
        }

        return true;
    }

    /// Check if the snapshot has been completely received and validated.
    [[nodiscard]] bool is_complete() const {
        std::lock_guard lock(mutex_);
        return state_ == State::Complete;
    }

    /// Check if the transfer failed.
    [[nodiscard]] bool has_failed() const {
        std::lock_guard lock(mutex_);
        return state_ == State::Failed;
    }

    /// Get the error message if failed.
    [[nodiscard]] std::string error_message() const {
        std::lock_guard lock(mutex_);
        return error_msg_;
    }

    /// Get the assembled and validated RaftSnapshot metadata.
    [[nodiscard]] std::optional<RaftSnapshot> get_snapshot() const {
        std::lock_guard lock(mutex_);
        if (state_ != State::Complete || !result_snapshot_) {
            return std::nullopt;
        }
        return result_snapshot_;
    }

    /// Get the assembled buffer (for direct apply).
    [[nodiscard]] std::vector<uint8_t> get_buffer() const {
        std::lock_guard lock(mutex_);
        return buffer_;
    }

    /// Reset state for a new transfer.
    void reset() {
        std::lock_guard lock(mutex_);
        state_ = State::Idle;
        buffer_.clear();
        bytes_received_ = 0;
        expected_last_index_ = 0;
        expected_last_term_ = kNoTerm;
        error_msg_.clear();
        result_snapshot_.reset();
    }

    /// Current state.
    [[nodiscard]] State state() const {
        std::lock_guard lock(mutex_);
        return state_;
    }

    /// Bytes received so far.
    [[nodiscard]] uint64_t progress_bytes() const {
        std::lock_guard lock(mutex_);
        return bytes_received_;
    }

private:
    // -- Validation + persistence --------------------------------------------

    [[nodiscard]] bool validate_and_persist() {
        // Parse the assembled buffer.
        auto parsed = SnapshotDeserializer::parse(buffer_);
        if (!parsed.valid) {
            error_msg_ = "snapshot validation failed (CRC or parse error)";
            SPDLOG_ERROR("raft_snapshot_asm: {}", error_msg_);
            return false;
        }

        // Verify metadata matches what we expected from the request.
        if (parsed.last_included_index != expected_last_index_
            || parsed.last_included_term != expected_last_term_) {
            error_msg_ = "parsed metadata doesn't match expected values";
            SPDLOG_ERROR("raft_snapshot_asm: {}", error_msg_);
            return false;
        }

        // Persist to disk.
        auto snap = SnapshotSerializer::create(
            snapshot_dir_,
            parsed.last_included_index,
            parsed.last_included_term,
            parsed.membership,
            parsed.state_data,
            parsed.created_at_ms);

        if (!snap) {
            error_msg_ = "failed to persist snapshot to disk";
            SPDLOG_ERROR("raft_snapshot_asm: {}", error_msg_);
            return false;
        }

        result_snapshot_ = std::move(*snap);
        return true;
    }

    // -- Fields ---------------------------------------------------------------

    std::string snapshot_dir_;

    mutable std::mutex mutex_;
    State state_ = State::Idle;
    std::vector<uint8_t> buffer_;
    uint64_t bytes_received_ = 0;

    LogIndex expected_last_index_ = 0;
    RaftTerm expected_last_term_ = kNoTerm;

    std::string error_msg_;
    std::optional<RaftSnapshot> result_snapshot_;
};

// ============================================================================
// SnapshotManager — high-level orchestration
// ============================================================================

/// Coordinates snapshot creation, chunked transfer, and installation.
/// Used by RaftNode to manage the snapshot lifecycle.
class SnapshotManager {
public:
    SnapshotManager(std::string snapshot_dir,
                     size_t chunk_size = kDefaultChunkSize)
        : snapshot_dir_(std::move(snapshot_dir))
        , transfer_mgr_(chunk_size)
        , assembler_(snapshot_dir_)
    {
        namespace fs = std::filesystem;
        std::error_code ec;
        fs::create_directories(snapshot_dir_, ec);
    }

    // -- Snapshot Creation ---------------------------------------------------

    /// Create a new snapshot from state machine data.
    /// @return RaftSnapshot metadata on success.
    [[nodiscard]] std::optional<RaftSnapshot> create_snapshot(
        LogIndex last_included_index,
        RaftTerm last_included_term,
        const RaftMembership& membership,
        const std::vector<uint8_t>& state_data) {

        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        auto snap = SnapshotSerializer::create(
            snapshot_dir_,
            last_included_index,
            last_included_term,
            membership,
            state_data,
            static_cast<timestamp_ms_t>(now_ms));

        if (snap) {
            latest_snapshot_ = *snap;
        }
        return snap;
    }

    /// Load a snapshot from an existing file path.
    [[nodiscard]] bool load_existing_snapshot(const std::string& path) {
        auto parsed = SnapshotDeserializer::parse(path);
        if (!parsed.valid) return false;

        latest_snapshot_ = RaftSnapshot{
            .file_path = path,
            .last_included_index = parsed.last_included_index,
            .last_included_term  = parsed.last_included_term,
            .membership          = parsed.membership,
            .file_size_bytes     = 0,  // will be filled by stat
            .created_at_ms       = parsed.created_at_ms,
        };

        std::error_code ec;
        latest_snapshot_->file_size_bytes =
            std::filesystem::file_size(path, ec);

        return true;
    }

    /// Get the latest created snapshot.
    [[nodiscard]] std::optional<RaftSnapshot> latest() const {
        return latest_snapshot_;
    }

    // -- Chunked Transfer (leader side) --------------------------------------

    /// Start sending a snapshot to a peer.
    [[nodiscard]] bool begin_transfer(RaftNodeId peer_id) {
        if (!latest_snapshot_) return false;
        return transfer_mgr_.start_transfer(peer_id, *latest_snapshot_);
    }

    /// Get the next chunk for a peer.
    [[nodiscard]] std::optional<InstallSnapshotRequest> get_next_chunk(
        RaftNodeId peer_id,
        RaftTerm term,
        RaftNodeId leader_id) {

        if (!latest_snapshot_) return std::nullopt;

        return transfer_mgr_.next_chunk(
            peer_id, term, leader_id,
            latest_snapshot_->last_included_index,
            latest_snapshot_->last_included_term);
    }

    /// Get transfer progress.
    [[nodiscard]] auto get_transfer_progress(RaftNodeId peer_id) const {
        return transfer_mgr_.get_progress(peer_id);
    }

    /// Mark a transfer as failed.
    void fail_transfer(RaftNodeId peer_id, std::string reason) {
        transfer_mgr_.mark_failed(peer_id, std::move(reason));
    }

    /// Clean up a transfer.
    void cleanup_transfer(RaftNodeId peer_id) {
        transfer_mgr_.cleanup(peer_id);
    }

    // -- Snapshot Installation (follower side) -------------------------------

    /// Begin receiving chunks for a new snapshot.
    [[nodiscard]] bool begin_receive(LogIndex last_included_index,
                                      RaftTerm last_included_term) {
        return assembler_.begin_snapshot(last_included_index, last_included_term);
    }

    /// Process an incoming chunk.
    [[nodiscard]] bool receive_chunk(const InstallSnapshotRequest& req) {
        return assembler_.receive_chunk(req);
    }

    /// Check if the received snapshot is complete and valid.
    [[nodiscard]] bool is_receive_complete() const {
        return assembler_.is_complete();
    }

    /// Get the received and validated snapshot metadata.
    [[nodiscard]] std::optional<RaftSnapshot> received_snapshot() const {
        return assembler_.get_snapshot();
    }

    /// Reset the receiver.
    void reset_receiver() {
        assembler_.reset();
    }

    /// Get receiver progress.
    [[nodiscard]] uint64_t receive_progress_bytes() const {
        return assembler_.progress_bytes();
    }

    // -- Maintenance ---------------------------------------------------------

    /// List all snapshot files in the snapshot directory.
    [[nodiscard]] std::vector<std::string> list_snapshots() const {
        std::vector<std::string> paths;
        namespace fs = std::filesystem;
        std::error_code ec;

        for (const auto& entry : fs::directory_iterator(snapshot_dir_, ec)) {
            if (ec) break;
            if (entry.is_regular_file()
                && entry.path().extension() == ".snap") {
                paths.push_back(entry.path().string());
            }
        }
        std::sort(paths.begin(), paths.end());
        return paths;
    }

    /// Delete old snapshots, keeping the most recent `keep_count`.
    void prune_old_snapshots(size_t keep_count = 3) {
        auto snapshots = list_snapshots();
        if (snapshots.size() <= keep_count) return;

        // Oldest first.
        for (size_t i = 0; i < snapshots.size() - keep_count; ++i) {
            std::error_code ec;
            std::filesystem::remove(snapshots[i], ec);
            if (!ec) {
                SPDLOG_INFO("raft_snapshot: pruned old snapshot {}", snapshots[i]);
            }
        }
    }

    /// Get the snapshot directory path.
    [[nodiscard]] const std::string& directory() const noexcept {
        return snapshot_dir_;
    }

private:
    std::string snapshot_dir_;
    SnapshotTransferManager transfer_mgr_;
    SnapshotAssembler assembler_;

    std::optional<RaftSnapshot> latest_snapshot_;
};

// ============================================================================
// Utility: validate a snapshot file on disk
// ============================================================================

/// Validate a snapshot file by parsing it and checking CRCs.
[[nodiscard]] inline bool validate_snapshot_file(const std::string& path) {
    auto parsed = SnapshotDeserializer::parse(path);
    if (!parsed.valid) {
        SPDLOG_ERROR("raft_snapshot: validation failed for {}", path);
        return false;
    }
    SPDLOG_INFO("raft_snapshot: {} is valid (index {}, term {}, {} bytes state data)",
                path, parsed.last_included_index, parsed.last_included_term,
                parsed.state_data.size());
    return true;
}

}  // namespace torrent::raft
