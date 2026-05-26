/**
 * torrent-mq — Disk I/O Primitives
 *
 * Low-level file I/O operations used by the storage layer: pwrite/pread
 * with EINTR retry, fsync/fdatasync, ftruncate, preallocation (fallocate /
 * posix_fallocate), and O_DIRECT I/O with properly aligned buffers.
 *
 * Design:
 *   - All writes use pwrite (thread-safe, no seek races).
 *   - All reads use pread (thread-safe, no seek races).
 *   - EINTR is always retried transparently.
 *   - ENOSPC, EIO, and other fatal errors are propagated.
 *   - O_DIRECT is used for large I/O (≥64 KiB) to bypass page cache;
 *     small I/O uses buffered paths for latency.
 *   - Buffers for O_DIRECT are aligned to the device's logical block size
 *     (typically 512 bytes, probed via statx/ioctl BLKSSZGET or default 4096).
 *   - fallocate is preferred for preallocation; posix_fallocate is the
 *     portable fallback (slower but correct).
 *   - fsync on the parent directory follows file fsync for full durability.
 */

#include "torrent/storage/disk_io.h"
#include "torrent/storage/types.h"
#include "torrent/common/types.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <algorithm>
#include <new>
#include <sstream>
#include <string>

#include <spdlog/spdlog.h>

// Linux-specific for O_DIRECT and fallocate.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

namespace torrent {

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_disk_io_logger() {
    static auto logger = spdlog::get("disk_io");
    if (!logger) {
        logger = spdlog::stdout_color_mt("disk_io");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Alignment
// --------------------------------------------------------------------------

/// Default alignment for O_DIRECT buffers (4 KiB).
static constexpr size_t kDefaultDirectAlignment = 4096;

/// Minimum I/O size to use O_DIRECT path (64 KiB).
static constexpr size_t kMinDirectIOSize = 65536;

/// Maximum I/O size for a single pwrite/pread call (1 GiB).
static constexpr size_t kMaxIOSize = 1073741824;

/// Ask the kernel for the logical block size of the filesystem containing fd.
/// Falls back to kDefaultDirectAlignment on error.
size_t probe_block_size(int fd) {
    struct stat st;
    if (::fstat(fd, &st) < 0) {
        return kDefaultDirectAlignment;
    }

    // statx would give us stx_blksize (preferred I/O size), but for portable
    // detection we try ioctl(BLKSSZGET) for block devices and fall back to
    // st_blksize for regular files.
#ifdef BLKSSZGET
    if (S_ISBLK(st.st_mode)) {
        int logical_block_size = 0;
        if (::ioctl(fd, BLKSSZGET, &logical_block_size) == 0 && logical_block_size > 0) {
            return static_cast<size_t>(logical_block_size);
        }
    }
#endif

    // Use st_blksize as a hint (typically 4096 on Linux).
    if (st.st_blksize > 0) {
        return static_cast<size_t>(st.st_blksize);
    }

    return kDefaultDirectAlignment;
}

/// Check if an address is aligned to `alignment` bytes.
bool is_aligned(const void* ptr, size_t alignment) noexcept {
    return (reinterpret_cast<uintptr_t>(ptr) & (alignment - 1)) == 0;
}

/// Round `value` up to the next multiple of `alignment`.
size_t align_up(size_t value, size_t alignment) noexcept {
    return (value + alignment - 1) & ~(alignment - 1);
}

/// Round `value` down to the nearest multiple of `alignment`.
size_t align_down(size_t value, size_t alignment) noexcept {
    return value & ~(alignment - 1);
}

// --------------------------------------------------------------------------
// Allocator for aligned memory
// --------------------------------------------------------------------------

/// Allocate `size` bytes aligned to `alignment`.  Wraps posix_memalign or
/// aligned_alloc.  Returns nullptr on failure.
void* allocate_aligned(size_t size, size_t alignment) {
    void* ptr = nullptr;
    int rc = ::posix_memalign(&ptr, alignment, size);
    if (rc != 0) {
        get_disk_io_logger()->error("posix_memalign({}, {}) failed: {}",
                                    size, alignment, std::strerror(rc));
        return nullptr;
    }
    return ptr;
}

/// Free aligned memory allocated by allocate_aligned().
void free_aligned(void* ptr) noexcept {
    std::free(ptr);
}

// --------------------------------------------------------------------------
// Error classification
// --------------------------------------------------------------------------

/// True if the errno value indicates a retriable error (EINTR, EAGAIN).
bool is_retriable(int e) noexcept {
    return e == EINTR || e == EAGAIN || e == EWOULDBLOCK;
}

/// True if the errno value indicates a fatal disk error.
bool is_fatal_io_error(int e) noexcept {
    return e == EIO || e == ENOSPC || e == EDQUOT || e == EROFS
        || e == ENXIO || e == ESTALE;
}

/// Map a system errno to a human-readable category string.
const char* errno_category(int e) noexcept {
    switch (e) {
    case EIO:      return "I/O hardware error";
    case ENOSPC:   return "No space left on device";
    case EDQUOT:   return "Disk quota exceeded";
    case EACCES:   return "Permission denied";
    case EPERM:    return "Operation not permitted";
    case EROFS:    return "Read-only filesystem";
    case EBADF:    return "Bad file descriptor";
    case EINVAL:   return "Invalid argument (alignment issue?)";
    case ENXIO:    return "No such device or address";
    case ESTALE:   return "Stale file handle (NFS?)";
    case EFBIG:    return "File too large";
    case EOVERFLOW: return "Value too large for data type";
    case EISDIR:   return "Is a directory";
    case EEXIST:   return "File already exists";
    case ENOENT:   return "No such file or directory";
    default:       return "Unknown errno";
    }
}

/// Convert an errno to the appropriate torrent error_code.
error_code errno_to_error_code(int e) noexcept {
    switch (e) {
    case ENOSPC:
    case EDQUOT:
    case EFBIG:
        return error_code::storage_unavailable;
    case EIO:
    case ENXIO:
    case ESTALE:
        return error_code::storage_unavailable;
    case EACCES:
    case EPERM:
    case EROFS:
        return error_code::storage_unavailable;
    case EINVAL:
        // Alignment or O_DIRECT issue — treat as storage_unavailable
        // with a hint to check alignment.
        return error_code::storage_unavailable;
    case EBADF:
        return error_code::storage_unavailable;
    default:
        return error_code::storage_unavailable;
    }
}

} // anonymous namespace

// ============================================================================
// DiskIO::Config
// ============================================================================

DiskIO::Config::Config() = default;

// ============================================================================
// DiskIO — construction / destruction
// ============================================================================

DiskIO::DiskIO(Config config)
    : config_(std::move(config))
    , fd_(-1)
    , block_size_(0)
    , opened_(false)
{
    auto logger = get_disk_io_logger();
    logger->debug("DiskIO created: direct_io={}, sync_writes={}, file_path='{}'",
                  config_.direct_io, config_.sync_writes, config_.file_path);
}

DiskIO::DiskIO(DiskIO&& other) noexcept
    : config_(std::move(other.config_))
    , fd_(other.fd_)
    , block_size_(other.block_size_)
    , opened_(other.opened_)
{
    other.fd_     = -1;
    other.opened_ = false;
    other.block_size_ = 0;
}

DiskIO& DiskIO::operator=(DiskIO&& other) noexcept {
    if (this != &other) {
        close();
        config_     = std::move(other.config_);
        fd_         = other.fd_;
        block_size_  = other.block_size_;
        opened_     = other.opened_;
        other.fd_   = -1;
        other.opened_ = false;
        other.block_size_ = 0;
    }
    return *this;
}

DiskIO::~DiskIO() {
    close();
}

// ============================================================================
// DiskIO::open
// ============================================================================

result<void> DiskIO::open() {
    if (opened_) {
        return result<void>::success();
    }

    int flags = O_RDWR;
    if (config_.direct_io) {
        flags |= O_DIRECT;
    }
    if (config_.sync_writes) {
        flags |= O_DSYNC;
    }
    if (config_.create_if_missing) {
        flags |= O_CREAT;
    }

    mode_t mode = config_.file_mode > 0 ? config_.file_mode : 0644;

    fd_ = ::open(config_.file_path.c_str(), flags, mode);
    if (fd_ < 0) {
        int e = errno;
        auto logger = get_disk_io_logger();
        logger->error("Cannot open '{}': {} ({}, errno={})",
                      config_.file_path, std::strerror(e), errno_category(e), e);
        return result<void>::failure(
            error_code::storage_unavailable,
            fmt::format("open('{}') failed: {} (errno={})",
                        config_.file_path, std::strerror(e), e));
    }

    // Probe the block size for alignment.
    block_size_ = probe_block_size(fd_);
    opened_ = true;

    auto logger = get_disk_io_logger();
    logger->debug("Opened '{}' fd={} flags=0x{:X} block_size={}",
                  config_.file_path, fd_, flags, block_size_);

    return result<void>::success();
}

// ============================================================================
// DiskIO::close
// ============================================================================

result<void> DiskIO::close() {
    if (fd_ < 0) return result<void>::success();

    int rc = ::close(fd_);
    fd_ = -1;
    opened_ = false;
    block_size_ = 0;

    if (rc < 0) {
        int e = errno;
        auto logger = get_disk_io_logger();
        logger->warn("close({}) failed: {} ({})", config_.file_path,
                     std::strerror(e), errno_category(e));
        return result<void>::failure(
            error_code::storage_unavailable,
            fmt::format("close failed: {} (errno={})", std::strerror(e), e));
    }

    return result<void>::success();
}

// ============================================================================
// DiskIO::is_open / fd
// ============================================================================

bool DiskIO::is_open() const noexcept {
    return opened_ && fd_ >= 0;
}

int DiskIO::fd() const noexcept {
    return fd_;
}

void DiskIO::set_fd(int fd) {
    close();
    fd_ = fd;
    opened_ = (fd_ >= 0);
    if (opened_) {
        block_size_ = probe_block_size(fd_);
    }
}

// ============================================================================
// DiskIO::write_all — pwrite with EINTR retry
// ============================================================================

result<byte_count_t> DiskIO::write_all(const void* buf, size_t count, off_t offset) {
    if (!opened_ || fd_ < 0) {
        return result<byte_count_t>::failure(
            error_code::storage_unavailable, "DiskIO not opened");
    }

    if (count == 0) {
        return result<byte_count_t>::success(0);
    }

    if (count > kMaxIOSize) {
        return result<byte_count_t>::failure(
            error_code::storage_unavailable,
            fmt::format("write_all: count {} exceeds max {}", count, kMaxIOSize));
    }

    // Choose I/O path: O_DIRECT for large aligned writes, buffered otherwise.
    if (config_.direct_io && count >= kMinDirectIOSize) {
        return direct_write_aligned(buf, count, offset);
    }

    return buffered_write(buf, count, offset);
}

result<byte_count_t> DiskIO::buffered_write(const void* buf, size_t count, off_t offset) {
    const auto* bytes = static_cast<const uint8_t*>(buf);
    size_t written = 0;
    int retries = 0;
    static constexpr int kMaxRetries = 10;

    while (written < count) {
        size_t remaining = count - written;
        ssize_t n = ::pwrite(fd_, bytes + written, remaining,
                              static_cast<off_t>(offset) + static_cast<off_t>(written));
        if (n < 0) {
            int e = errno;
            if (is_retriable(e) && retries < kMaxRetries) {
                ++retries;
                continue;
            }

            auto logger = get_disk_io_logger();
            logger->error("pwrite(fd={}, count={}, offset={}) failed: {} ({})",
                          fd_, remaining, offset + static_cast<off_t>(written),
                          std::strerror(e), errno_category(e));

            return result<byte_count_t>::failure(
                errno_to_error_code(e),
                fmt::format("pwrite failed after {} retries at offset {}: {} (errno={})",
                            retries, offset + static_cast<off_t>(written),
                            std::strerror(e), e));
        }
        if (n == 0) {
            // Write returned 0 — disk full or file limit reached.
            auto logger = get_disk_io_logger();
            logger->error("pwrite returned 0 (ENOSPC implied) at offset {}",
                          offset + static_cast<off_t>(written));
            return result<byte_count_t>::failure(
                error_code::storage_unavailable,
                fmt::format("pwrite returned 0 at offset {}: disk full or file limit",
                            offset + static_cast<off_t>(written)));
        }
        written += static_cast<size_t>(n);
        retries = 0;
    }

    if (config_.sync_writes) {
        auto sync_res = fsync();
        if (sync_res.failed()) {
            return result<byte_count_t>::failure(sync_res.error, sync_res.error_message);
        }
    }

    return result<byte_count_t>::success(static_cast<byte_count_t>(written));
}

result<byte_count_t> DiskIO::direct_write_aligned(const void* buf, size_t count, off_t offset) {
    // For O_DIRECT, the buffer, count, and offset must all be aligned to
    // the block size.
    size_t alignment = block_size_ > 0 ? block_size_ : kDefaultDirectAlignment;

    bool buf_aligned = is_aligned(buf, alignment);
    bool count_aligned = (count % alignment) == 0;
    bool offset_aligned = (static_cast<size_t>(offset) % alignment) == 0;

    if (buf_aligned && count_aligned && offset_aligned) {
        // Happy path: everything is already aligned.
        return buffered_write(buf, count, offset);  // goes through O_DIRECT fd
    }

    // Need bounce buffer.  Allocate aligned buffer, copy data, write, free.
    size_t aligned_count = align_up(count, alignment);
    void* bounce = allocate_aligned(aligned_count, alignment);
    if (!bounce) {
        return result<byte_count_t>::failure(
            error_code::storage_unavailable,
            fmt::format("Cannot allocate aligned buffer of {} bytes", aligned_count));
    }

    std::memcpy(bounce, buf, count);
    // Zero-fill the padding bytes (past count up to aligned_count).
    if (aligned_count > count) {
        std::memset(static_cast<uint8_t*>(bounce) + count, 0, aligned_count - count);
    }

    // Align the offset down (writing from an aligned position).
    // But we cannot change the file offset arbitrarily — the caller expects
    // data at `offset`.  We must write padding if offset is misaligned.
    //
    // Strategy: if offset is not aligned, we must do a read-modify-write
    // of the surrounding aligned block.  This is expensive but rare.
    if (!offset_aligned) {
        size_t aligned_offset = align_down(static_cast<size_t>(offset), alignment);
        size_t prefix_pad = static_cast<size_t>(offset) - aligned_offset;

        // Allocate a full aligned block for RMW.
        size_t total_aligned = align_up(prefix_pad + count, alignment);
        void* rmw_buf = allocate_aligned(total_aligned, alignment);
        if (!rmw_buf) {
            free_aligned(bounce);
            return result<byte_count_t>::failure(
                error_code::storage_unavailable,
                fmt::format("Cannot allocate RMW buffer of {} bytes", total_aligned));
        }

        // Read existing data at aligned offset.
        auto read_res = read_all(rmw_buf, total_aligned,
                                  static_cast<off_t>(aligned_offset));
        if (read_res.failed()) {
            free_aligned(rmw_buf);
            free_aligned(bounce);
            return result<byte_count_t>::failure(read_res.error, read_res.error_message);
        }

        // Overlay our data after the prefix padding.
        auto* dest = static_cast<uint8_t*>(rmw_buf) + prefix_pad;
        std::memcpy(dest, bounce, count);

        // Write the aligned block.
        auto write_res = buffered_write(rmw_buf, total_aligned,
                                         static_cast<off_t>(aligned_offset));
        free_aligned(rmw_buf);
        free_aligned(bounce);

        return write_res;
    }

    // Offset is aligned, buffer/count may not be.  Use bounce buffer.
    auto result_io = buffered_write(bounce, aligned_count, offset);
    free_aligned(bounce);

    if (result_io.ok()) {
        // Report only the original count as written.
        result_io.value = static_cast<byte_count_t>(count);
    }
    return result_io;
}

// ============================================================================
// DiskIO::read_all — pread with EINTR retry
// ============================================================================

result<byte_count_t> DiskIO::read_all(void* buf, size_t count, off_t offset) {
    if (!opened_ || fd_ < 0) {
        return result<byte_count_t>::failure(
            error_code::storage_unavailable, "DiskIO not opened");
    }

    if (count == 0) {
        return result<byte_count_t>::success(0);
    }

    if (count > kMaxIOSize) {
        return result<byte_count_t>::failure(
            error_code::storage_unavailable,
            fmt::format("read_all: count {} exceeds max {}", count, kMaxIOSize));
    }

    // Choose I/O path.
    if (config_.direct_io && count >= kMinDirectIOSize) {
        return direct_read_aligned(buf, count, offset);
    }

    return buffered_read(buf, count, offset);
}

result<byte_count_t> DiskIO::buffered_read(void* buf, size_t count, off_t offset) {
    auto* bytes = static_cast<uint8_t*>(buf);
    size_t readb = 0;
    int retries = 0;
    static constexpr int kMaxRetries = 10;

    while (readb < count) {
        size_t remaining = count - readb;
        ssize_t n = ::pread(fd_, bytes + readb, remaining,
                             static_cast<off_t>(offset) + static_cast<off_t>(readb));
        if (n < 0) {
            int e = errno;
            if (is_retriable(e) && retries < kMaxRetries) {
                ++retries;
                continue;
            }

            auto logger = get_disk_io_logger();
            logger->error("pread(fd={}, count={}, offset={}) failed: {} ({})",
                          fd_, remaining, offset + static_cast<off_t>(readb),
                          std::strerror(e), errno_category(e));

            return result<byte_count_t>::failure(
                errno_to_error_code(e),
                fmt::format("pread failed after {} retries at offset {}: {} (errno={})",
                            retries, offset + static_cast<off_t>(readb),
                            std::strerror(e), e));
        }
        if (n == 0) {
            // EOF — break early, return what we've read.
            break;
        }
        readb += static_cast<size_t>(n);
        retries = 0;
    }

    return result<byte_count_t>::success(static_cast<byte_count_t>(readb));
}

result<byte_count_t> DiskIO::direct_read_aligned(void* buf, size_t count, off_t offset) {
    size_t alignment = block_size_ > 0 ? block_size_ : kDefaultDirectAlignment;

    bool buf_aligned = is_aligned(buf, alignment);
    bool count_aligned = (count % alignment) == 0;
    bool offset_aligned = (static_cast<size_t>(offset) % alignment) == 0;

    if (buf_aligned && count_aligned && offset_aligned) {
        return buffered_read(buf, count, offset);
    }

    // Need bounce buffer.
    size_t aligned_count = align_up(count, alignment);
    size_t aligned_offset = align_down(static_cast<size_t>(offset), alignment);

    void* bounce = allocate_aligned(aligned_count, alignment);
    if (!bounce) {
        return result<byte_count_t>::failure(
            error_code::storage_unavailable,
            fmt::format("Cannot allocate aligned read buffer of {} bytes", aligned_count));
    }

    auto read_res = buffered_read(bounce, aligned_count, static_cast<off_t>(aligned_offset));
    if (read_res.failed()) {
        free_aligned(bounce);
        return read_res;
    }

    // Copy the relevant portion back to the caller's buffer.
    size_t prefix_skip = static_cast<size_t>(offset) - aligned_offset;
    size_t copyable = std::min(count, static_cast<size_t>(read_res.value) - prefix_skip);
    std::memcpy(buf, static_cast<uint8_t*>(bounce) + prefix_skip, copyable);
    free_aligned(bounce);

    return result<byte_count_t>::success(static_cast<byte_count_t>(copyable));
}

// ============================================================================
// DiskIO::write_exact / read_exact — short wrappers
// ============================================================================

result<void> DiskIO::write_exact(const void* buf, size_t count, off_t offset) {
    auto r = write_all(buf, count, offset);
    if (r.failed()) {
        return result<void>::failure(r.error, r.error_message);
    }
    if (r.value != static_cast<byte_count_t>(count)) {
        return result<void>::failure(
            error_code::storage_unavailable,
            fmt::format("write_exact: wrote {} of {} bytes", r.value, count));
    }
    return result<void>::success();
}

result<void> DiskIO::read_exact(void* buf, size_t count, off_t offset) {
    auto r = read_all(buf, count, offset);
    if (r.failed()) {
        return result<void>::failure(r.error, r.error_message);
    }
    if (r.value != static_cast<byte_count_t>(count)) {
        return result<void>::failure(
            error_code::storage_unavailable,
            fmt::format("read_exact: read {} of {} bytes", r.value, count));
    }
    return result<void>::success();
}

// ============================================================================
// DiskIO::fsync / fdatasync
// ============================================================================

result<void> DiskIO::fsync() {
    if (!opened_ || fd_ < 0) {
        return result<void>::failure(
            error_code::storage_unavailable, "DiskIO not opened");
    }

    if (::fsync(fd_) < 0) {
        int e = errno;
        auto logger = get_disk_io_logger();
        logger->error("fsync(fd={}) failed: {} ({})", fd_,
                      std::strerror(e), errno_category(e));
        return result<void>::failure(
            errno_to_error_code(e),
            fmt::format("fsync failed: {} (errno={})", std::strerror(e), e));
    }

    return result<void>::success();
}

result<void> DiskIO::fdatasync() {
    if (!opened_ || fd_ < 0) {
        return result<void>::failure(
            error_code::storage_unavailable, "DiskIO not opened");
    }

#ifdef __linux__
    if (::fdatasync(fd_) < 0) {
        int e = errno;
        auto logger = get_disk_io_logger();
        logger->error("fdatasync(fd={}) failed: {} ({})", fd_,
                      std::strerror(e), errno_category(e));
        return result<void>::failure(
            errno_to_error_code(e),
            fmt::format("fdatasync failed: {} (errno={})", std::strerror(e), e));
    }
#else
    // Fallback to full fsync on non-Linux.
    return fsync();
#endif

    return result<void>::success();
}

result<void> DiskIO::sync_directory(const std::string& dir_path) {
    int dir_fd = ::open(dir_path.c_str(), O_RDONLY | O_DIRECTORY);
    if (dir_fd < 0) {
        int e = errno;
        auto logger = get_disk_io_logger();
        logger->warn("Cannot open directory '{}' for fsync: {} ({})",
                     dir_path, std::strerror(e), errno_category(e));
        return result<void>::failure(
            errno_to_error_code(e),
            fmt::format("open dir '{}' for sync failed: {} (errno={})",
                        dir_path, std::strerror(e), e));
    }

    if (::fsync(dir_fd) < 0) {
        int e = errno;
        int saved_errno = e;
        ::close(dir_fd);
        auto logger = get_disk_io_logger();
        logger->warn("fsync on directory '{}' failed: {} ({})",
                     dir_path, std::strerror(e), errno_category(e));
        return result<void>::failure(
            errno_to_error_code(e),
            fmt::format("fsync dir '{}' failed: {} (errno={})",
                        dir_path, std::strerror(e), e));
    }

    ::close(dir_fd);
    return result<void>::success();
}

result<void> DiskIO::full_sync(const std::string& dir_path) {
    // Sync the file first.
    auto r = fsync();
    if (r.failed()) return r;

    // Then sync the parent directory to ensure metadata (e.g. rename, create)
    // is durable.
    std::string parent;
    if (!dir_path.empty()) {
        parent = dir_path;
    } else {
        // Derive parent from config_.file_path.
        auto pos = config_.file_path.rfind('/');
        if (pos == std::string::npos) {
            parent = ".";
        } else if (pos == 0) {
            parent = "/";
        } else {
            parent = config_.file_path.substr(0, pos);
        }
    }

    return sync_directory(parent);
}

// ============================================================================
// DiskIO::truncate
// ============================================================================

result<void> DiskIO::truncate(off_t new_size) {
    if (!opened_ || fd_ < 0) {
        return result<void>::failure(
            error_code::storage_unavailable, "DiskIO not opened");
    }

    if (::ftruncate(fd_, new_size) < 0) {
        int e = errno;
        auto logger = get_disk_io_logger();
        logger->error("ftruncate(fd={}, size={}) failed: {} ({})",
                      fd_, new_size, std::strerror(e), errno_category(e));
        return result<void>::failure(
            errno_to_error_code(e),
            fmt::format("ftruncate to {} failed: {} (errno={})",
                        new_size, std::strerror(e), e));
    }

    // If using O_DIRECT or sync_writes, ensure the truncation is durable.
    if (config_.sync_writes) {
        return fsync();
    }

    return result<void>::success();
}

// ============================================================================
// DiskIO::file_size
// ============================================================================

result<byte_count_t> DiskIO::file_size() const {
    if (!opened_ || fd_ < 0) {
        return result<byte_count_t>::failure(
            error_code::storage_unavailable, "DiskIO not opened");
    }

    struct stat st;
    if (::fstat(fd_, &st) < 0) {
        int e = errno;
        return result<byte_count_t>::failure(
            error_code_to_error_code(e),
            fmt::format("fstat failed: {} (errno={})", std::strerror(e), e));
    }

    return result<byte_count_t>::success(static_cast<byte_count_t>(st.st_size));
}

// ============================================================================
// DiskIO::preallocate
// ============================================================================

result<void> DiskIO::preallocate(off_t size) {
    if (!opened_ || fd_ < 0) {
        return result<void>::failure(
            error_code::storage_unavailable, "DiskIO not opened");
    }

    if (size <= 0) {
        return result<void>::success();
    }

    // Try fallocate first (fast, efficient on modern filesystems).
    int rc = ::fallocate(fd_, 0, 0, size);
    if (rc == 0) {
        auto logger = get_disk_io_logger();
        logger->debug("fallocate(fd={}, size={}) succeeded", fd_, size);

        // Sync metadata if needed.
        if (config_.sync_writes) {
            return fsync();
        }
        return result<void>::success();
    }

    int fallocate_errno = errno;

    // fallocate is not supported on this filesystem (e.g. NFS, ZFS without
    // fallocate support).  Fall back to posix_fallocate.
    if (fallocate_errno == EOPNOTSUPP || fallocate_errno == ENOSYS
        || fallocate_errno == EINVAL) {

        auto logger = get_disk_io_logger();
        logger->info("fallocate not supported on '{}' (errno={}), "
                     "falling back to posix_fallocate",
                     config_.file_path, fallocate_errno);

        rc = ::posix_fallocate(fd_, 0, size);
        if (rc == 0) {
            logger->debug("posix_fallocate(fd={}, size={}) succeeded", fd_, size);
            if (config_.sync_writes) {
                return fsync();
            }
            return result<void>::success();
        }

        // posix_fallocate also failed.
        return result<void>::failure(
            error_code::storage_unavailable,
            fmt::format("posix_fallocate(fd={}, size={}) failed: {} (errno={})",
                        fd_, size, std::strerror(rc), rc));
    }

    // Other fallocate failure.
    auto logger = get_disk_io_logger();
    logger->warn("fallocate(fd={}, size={}) failed: {} ({})",
                 fd_, size, std::strerror(fallocate_errno),
                 errno_category(fallocate_errno));

    // Last resort: write zeros via pwrite in a loop.
    // This is slow but always works.
    return preallocate_by_writing(size);
}

result<void> DiskIO::preallocate_by_writing(off_t size) {
    auto logger = get_disk_io_logger();
    logger->info("Preallocating {} bytes by writing zeros — this may be slow", size);

    // Allocate a zero-filled buffer for writing.
    static constexpr size_t kWriteChunk = 1048576; // 1 MiB
    size_t chunk_size = kWriteChunk;

    // Use aligned buffer if O_DIRECT is active.
    std::unique_ptr<uint8_t[]> zero_buf;
    if (config_.direct_io) {
        size_t alignment = block_size_ > 0 ? block_size_ : kDefaultDirectAlignment;
        chunk_size = align_up(kWriteChunk, alignment);
        void* aligned = allocate_aligned(chunk_size, alignment);
        if (!aligned) {
            return result<void>::failure(
                error_code::storage_unavailable,
                "Cannot allocate aligned buffer for preallocation");
        }
        zero_buf.reset(static_cast<uint8_t*>(aligned));
    } else {
        zero_buf = std::make_unique<uint8_t[]>(chunk_size);
    }

    std::memset(zero_buf.get(), 0, chunk_size);

    off_t written = 0;
    while (written < size) {
        size_t to_write = static_cast<size_t>(
            std::min(static_cast<off_t>(chunk_size), size - written));
        auto r = write_all(zero_buf.get(), to_write, written);
        if (r.failed()) {
            return result<void>::failure(r.error, r.error_message);
        }
        written += static_cast<off_t>(r.value);
    }

    if (config_.sync_writes) {
        return fsync();
    }

    logger->debug("Preallocation by writing complete: {} bytes", size);
    return result<void>::success();
}

// ============================================================================
// DiskIO::direct_io_read / direct_io_write — explicit O_DIRECT
// ============================================================================

result<byte_count_t> DiskIO::direct_io_read(void* buf, size_t count, off_t offset) {
    if (!opened_ || fd_ < 0) {
        return result<byte_count_t>::failure(
            error_code::storage_unavailable, "DiskIO not opened");
    }

    if (!config_.direct_io) {
        // Fall back to buffered read if O_DIRECT was not configured.
        return buffered_read(buf, count, offset);
    }

    size_t alignment = block_size_ > 0 ? block_size_ : kDefaultDirectAlignment;

    // The caller must provide an aligned buffer, or we bounce.
    if (!is_aligned(buf, alignment) || (count % alignment) != 0
        || (static_cast<size_t>(offset) % alignment) != 0) {
        return direct_read_aligned(buf, count, offset);
    }

    return buffered_read(buf, count, offset);
}

result<byte_count_t> DiskIO::direct_io_write(const void* buf, size_t count, off_t offset) {
    if (!opened_ || fd_ < 0) {
        return result<byte_count_t>::failure(
            error_code::storage_unavailable, "DiskIO not opened");
    }

    if (!config_.direct_io) {
        return buffered_write(buf, count, offset);
    }

    size_t alignment = block_size_ > 0 ? block_size_ : kDefaultDirectAlignment;

    if (!is_aligned(buf, alignment) || (count % alignment) != 0
        || (static_cast<size_t>(offset) % alignment) != 0) {
        return direct_write_aligned(buf, count, offset);
    }

    return buffered_write(buf, count, offset);
}

// ============================================================================
// DiskIO::allocate_aligned_buffer
// ============================================================================

std::pair<void*, size_t> DiskIO::allocate_aligned_buffer(size_t size) const {
    size_t alignment = block_size_ > 0 ? block_size_ : kDefaultDirectAlignment;
    size_t aligned_size = align_up(size, alignment);
    void* ptr = allocate_aligned(aligned_size, alignment);
    return {ptr, aligned_size};
}

void DiskIO::free_aligned_buffer(void* ptr) noexcept {
    free_aligned(ptr);
}

// ============================================================================
// DiskIO — static convenience constructors
// ============================================================================

DiskIO DiskIO::for_reading(const std::string& path) {
    Config cfg;
    cfg.file_path    = path;
    cfg.direct_io    = false;
    cfg.sync_writes  = false;
    cfg.create_if_missing = false;
    return DiskIO(std::move(cfg));
}

DiskIO DiskIO::for_writing(const std::string& path, bool sync) {
    Config cfg;
    cfg.file_path    = path;
    cfg.direct_io    = false;
    cfg.sync_writes  = sync;
    cfg.create_if_missing = true;
    return DiskIO(std::move(cfg));
}

DiskIO DiskIO::for_direct_io(const std::string& path) {
    Config cfg;
    cfg.file_path    = path;
    cfg.direct_io    = true;
    cfg.sync_writes  = false;
    cfg.create_if_missing = true;
    return DiskIO(std::move(cfg));
}

} // namespace torrent
