#pragma once

/**
 * torrent-mq — Disk I/O Primitives
 *
 * Low-level file I/O operations: pwrite/pread with EINTR retry,
 * fsync/fdatasync, ftruncate, preallocation (fallocate/posix_fallocate),
 * and O_DIRECT I/O with aligned buffers.
 *
 * Thread safety: the class is not internally serialised; the caller
 * must ensure that only one thread operates on a DiskIO instance at a
 * time (or use external synchronisation).  pwrite/pread are inherently
 * safe to interleave across threads on the same fd.
 */

#include <cstdint>
#include <string>
#include <memory>
#include <utility>

#include "torrent/common/types.h"

namespace torrent {

// ============================================================================
// DiskIO — buffered and direct file I/O
// ============================================================================

/**
 * Provides safe, retriable file I/O operations.  Wraps raw POSIX file
 * descriptors with EINTR retry, O_DIRECT alignment, preallocation,
 * and durability (fsync) guarantees.
 *
 * Usage:
 *   DiskIO io(DiskIO::for_writing("/data/segment.log", true));
 *   io.open();
 *   io.write_all(data, len, offset);
 *   io.fsync();
 *   io.close();
 */
class DiskIO {
public:
    // ------------------------------------------------------------------
    // Configuration
    // ------------------------------------------------------------------

    struct Config {
        /// Filesystem path to open/create.
        std::string file_path;

        /// When true, open with O_DIRECT for unbuffered I/O.
        bool direct_io = false;

        /// When true, each write is followed by fsync (or open with O_DSYNC).
        bool sync_writes = false;

        /// When true, open() creates the file if it does not exist.
        bool create_if_missing = false;

        /// File mode for newly created files (default: 0644).
        int file_mode = 0644;

        Config();
    };

    // ------------------------------------------------------------------
    // Construction / Destruction
    // ------------------------------------------------------------------

    explicit DiskIO(Config config);
    DiskIO(DiskIO&& other) noexcept;
    DiskIO& operator=(DiskIO&& other) noexcept;
    DiskIO(const DiskIO&) = delete;
    DiskIO& operator=(const DiskIO&) = delete;
    ~DiskIO();

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /// Open (or create) the file.  Must be called before any I/O.
    result<void> open();

    /// Close the file descriptor.  Idempotent.
    result<void> close();

    [[nodiscard]] bool is_open() const noexcept;
    [[nodiscard]] int  fd()      const noexcept;

    /// Adopt an externally-opened file descriptor (e.g. from Segment).
    /// Closes any currently-open fd first.
    void set_fd(int fd);

    // ------------------------------------------------------------------
    // Write operations
    // ------------------------------------------------------------------

    /// Write `count` bytes from `buf` at file offset `offset`.
    /// Retries on EINTR.  Returns the number of bytes written on success.
    /// Uses O_DIRECT path when configured and count >= 64 KiB.
    result<byte_count_t> write_all(const void* buf, size_t count, off_t offset);

    /// Like write_all but fails if fewer than `count` bytes were written.
    result<void> write_exact(const void* buf, size_t count, off_t offset);

    /// Explicit O_DIRECT write (buffers must be aligned).
    result<byte_count_t> direct_io_write(const void* buf, size_t count, off_t offset);

    // ------------------------------------------------------------------
    // Read operations
    // ------------------------------------------------------------------

    /// Read up to `count` bytes into `buf` from file offset `offset`.
    /// Retries on EINTR.  Returns the number of bytes read (may be < count
    /// at EOF).  Uses O_DIRECT path when configured and count >= 64 KiB.
    result<byte_count_t> read_all(void* buf, size_t count, off_t offset);

    /// Like read_all but fails if fewer than `count` bytes were read.
    result<void> read_exact(void* buf, size_t count, off_t offset);

    /// Explicit O_DIRECT read (buffers must be aligned).
    result<byte_count_t> direct_io_read(void* buf, size_t count, off_t offset);

    // ------------------------------------------------------------------
    // Durability
    // ------------------------------------------------------------------

    /// Flush file data and metadata to disk (fsync).
    result<void> fsync();

    /// Flush file data only (fdatasync on Linux, fsync elsewhere).
    result<void> fdatasync();

    /// fsync a directory by path (for metadata durability after rename/create).
    result<void> sync_directory(const std::string& dir_path);

    /// Full durability: fsync the file, then fsync its parent directory.
    /// @param dir_path  Optional explicit directory path; if empty,
    ///                  derived from config_.file_path.
    result<void> full_sync(const std::string& dir_path = "");

    // ------------------------------------------------------------------
    // File management
    // ------------------------------------------------------------------

    /// Truncate the file to `new_size` bytes.  Followed by fsync if
    /// config_.sync_writes is true.
    result<void> truncate(off_t new_size);

    /// Get current file size via fstat.
    [[nodiscard]] result<byte_count_t> file_size() const;

    /// Preallocate `size` bytes for the file.  Tries fallocate first,
    /// then posix_fallocate, then writes zeros as a last resort.
    result<void> preallocate(off_t size);

    // ------------------------------------------------------------------
    // Alignment helpers
    // ------------------------------------------------------------------

    /// Allocate an aligned buffer suitable for O_DIRECT I/O.
    /// Use free_aligned_buffer() to release.
    [[nodiscard]] std::pair<void*, size_t> allocate_aligned_buffer(size_t size) const;

    /// Free a buffer allocated by allocate_aligned_buffer().
    static void free_aligned_buffer(void* ptr) noexcept;

    // ------------------------------------------------------------------
    // Static convenience constructors
    // ------------------------------------------------------------------

    /// Create a DiskIO for reading (no O_DIRECT, no sync, no create).
    static DiskIO for_reading(const std::string& path);

    /// Create a DiskIO for buffered writing with optional sync.
    static DiskIO for_writing(const std::string& path, bool sync = false);

    /// Create a DiskIO for O_DIRECT I/O.
    static DiskIO for_direct_io(const std::string& path);

    // ------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------

    [[nodiscard]] const Config& config() const noexcept { return config_; }
    [[nodiscard]] size_t block_size() const noexcept { return block_size_; }

private:
    // Buffered I/O path (used when O_DIRECT is not active or for small I/O).
    result<byte_count_t> buffered_write(const void* buf, size_t count, off_t offset);
    result<byte_count_t> buffered_read(void* buf, size_t count, off_t offset);

    // Aligned O_DIRECT I/O with bounce-buffer support.
    result<byte_count_t> direct_write_aligned(const void* buf, size_t count, off_t offset);
    result<byte_count_t> direct_read_aligned(void* buf, size_t count, off_t offset);

    // Preallocation fallback: write zeros via pwrite loop.
    result<void> preallocate_by_writing(off_t size);

    Config   config_;
    int      fd_         = -1;
    size_t   block_size_ = 0;
    bool     opened_     = false;
};

} // namespace torrent
