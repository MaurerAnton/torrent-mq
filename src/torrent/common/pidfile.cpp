/**
 * pidfile.cpp — PidFile: Daemon PID File Management
 *
 * Safe PID file creation with flock(2) advisory locking, removal, and
 * process-liveness checks.  Uses LOCK_EX | LOCK_NB so write_pid() fails
 * immediately if another process holds the lock.  is_running() uses
 * kill(pid, 0) for a TOCTOU-safe (with flock) liveness check.
 */

#include "torrent/common/pidfile.h"

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <cerrno>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>

#include <fcntl.h>
#include <signal.h>
#include <sys/file.h>
#include <unistd.h>

namespace torrent {

namespace {

std::shared_ptr<spdlog::logger> get_pidfile_logger() {
    static auto logger = spdlog::get("pidfile");
    if (!logger) {
        logger = spdlog::stdout_color_mt("pidfile");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

/// Write all bytes to fd, handling partial writes and EINTR.
bool write_all(int fd, std::string_view data) {
    const char* buf = data.data();
    size_t remaining = data.size();
    while (remaining > 0) {
        ssize_t n = ::write(fd, buf, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            get_pidfile_logger()->error("write() failed: {}", std::strerror(errno));
            return false;
        }
        buf += static_cast<size_t>(n);
        remaining -= static_cast<size_t>(n);
    }
    return true;
}

/// Read small file contents into a string. Returns empty on error.
std::string read_all(int fd) {
    std::string result;
    char buf[256];
    for (;;) {
        ssize_t n = ::read(fd, buf, sizeof(buf));
        if (n < 0) {
            if (errno == EINTR) continue;
            return {};
        }
        if (n == 0) break;
        result.append(buf, static_cast<size_t>(n));
    }
    return result;
}

} // anonymous namespace

// ============================================================================
// Construction / Destruction
// ============================================================================

PidFile::PidFile(std::string_view path) : path_(path) {}

PidFile::~PidFile() {
    if (fd_ >= 0) {
        ::close(fd_);   // Releases flock on Linux.
        fd_ = -1;
    }
}

// ============================================================================
// Core API
// ============================================================================

bool PidFile::write_pid() {
    if (fd_ >= 0) return true; // Already locked.

    int fd = ::open(path_.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) {
        get_pidfile_logger()->error("open('{}') failed: {}", path_, std::strerror(errno));
        return false;
    }

    // Non-blocking exclusive lock — fail if another process holds it.
    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        if (errno == EWOULDBLOCK)
            get_pidfile_logger()->warn("PID file '{}' locked by another process", path_);
        else
            get_pidfile_logger()->error("flock('{}') failed: {}", path_, std::strerror(errno));
        ::close(fd);
        return false;
    }

    // Truncate, seek to start, write PID.
    if (::ftruncate(fd, 0) != 0 || ::lseek(fd, 0, SEEK_SET) == off_t(-1)) {
        get_pidfile_logger()->error("ftruncate/lseek failed on '{}': {}", path_, std::strerror(errno));
        ::flock(fd, LOCK_UN);
        ::close(fd);
        return false;
    }

    std::string pid_str = std::to_string(::getpid()) + "\n";
    if (!write_all(fd, pid_str)) {
        ::flock(fd, LOCK_UN);
        ::close(fd);
        return false;
    }

    fd_ = fd;
    get_pidfile_logger()->info("PID file written: '{}' (pid={})", path_, ::getpid());
    return true;
}

void PidFile::remove_pid() noexcept {
    if (fd_ < 0) return;

    if (::unlink(path_.c_str()) != 0 && errno != ENOENT) {
        get_pidfile_logger()->warn("unlink('{}') failed: {}", path_, std::strerror(errno));
    }

    ::close(fd_);
    fd_ = -1;
    get_pidfile_logger()->info("PID file removed: '{}'", path_);
}

// ============================================================================
// Static utilities
// ============================================================================

int PidFile::read_pid(std::string_view path) noexcept {
    int fd = ::open(path.data(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;

    std::string content = read_all(fd);
    ::close(fd);

    if (content.empty()) return -1;

    // Parse first integer token.  PID_MAX on Linux is 4,194,304.
    const char* start = content.c_str();
    char* end = nullptr;
    long long pid = std::strtoll(start, &end, 10);

    if (end == start || pid <= 0 || pid > 4'194'304) return -1;

    return static_cast<int>(pid);
}

bool PidFile::is_running(std::string_view path) noexcept {
    int pid = read_pid(path);
    if (pid <= 0) return false;

    // kill(pid, 0): error-check only, no signal sent.
    if (::kill(static_cast<pid_t>(pid), 0) == 0) return true;

    // ESRCH  → process not found (stale PID file)
    // EPERM  → process exists but owned by another user (still "running")
    if (errno == EPERM) return true;

    return false;
}

} // namespace torrent
