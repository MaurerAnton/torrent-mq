#pragma once

/**
 * torrent-mq — PidFile: Daemon PID File Management
 *
 * Provides safe PID file creation, locking, removal, and process-liveness
 * checks for daemon mode operation.  Uses POSIX file locking (flock) to
 * prevent multiple instances from using the same PID file.
 *
 * Usage:
 *   PidFile pf("/var/run/torrent-mq/torrent-mq.pid");
 *   if (!pf.write_pid()) {
 *       // Another instance is already running or permissions error
 *       return 1;
 *   }
 *   // ... daemon runs ...
 *   pf.remove_pid();  // On graceful shutdown
 *
 * The destructor does NOT automatically remove the PID file — call
 * remove_pid() explicitly during shutdown handling.
 *
 * Thread safety: instance methods are NOT internally synchronised.
 * Typically a single PidFile is used from the main thread.
 */

#include <string>
#include <string_view>

namespace torrent {

class PidFile {
public:
    /**
     * Construct a PidFile manager bound to @p path.
     *
     * No file is created or opened until write_pid() is called.
     *
     * @param path  Absolute or relative path to the PID file.
     */
    explicit PidFile(std::string_view path);

    /// Closes the lock file descriptor if open. Does NOT delete the file.
    ~PidFile();

    // Non-copyable, non-movable
    PidFile(const PidFile&) = delete;
    PidFile& operator=(const PidFile&) = delete;
    PidFile(PidFile&&) = delete;
    PidFile& operator=(PidFile&&) = delete;

    // ---- Core API ----

    /**
     * Write the current process PID to the file and acquire an exclusive
     * advisory lock (flock).  If the file is already locked by another
     * process, this call returns false.
     *
     * @return true on success; false if the file is locked by another
     *         process or on I/O error.
     */
    [[nodiscard]] bool write_pid();

    /**
     * Remove the PID file and release the lock.
     *
     * Safe to call even if write_pid() was never called or already
     * removed — it is a no-op in that case.
     */
    void remove_pid() noexcept;

    // ---- Accessors ----

    /// The path passed to the constructor.
    [[nodiscard]] const std::string& path() const noexcept { return path_; }

    /// Whether write_pid() succeeded and the lock is still held.
    [[nodiscard]] bool locked() const noexcept { return fd_ >= 0; }

    // ---- Static utilities ----

    /**
     * Read a PID from an existing PID file.
     *
     * @param path  Path to the PID file.
     * @return The PID read from the file, or -1 on error (file not
     *         found, unreadable, or malformed).
     */
    [[nodiscard]] static int read_pid(std::string_view path) noexcept;

    /**
     * Check whether the process identified by a PID file is alive.
     *
     * Reads the PID from the file and sends signal 0 (kill(pid, 0)) to
     * check existence.  Returns false if the file doesn't exist, is
     * unreadable, the PID is invalid, or the process is not running.
     *
     * @param path  Path to the PID file.
     * @return true if the process appears to be running.
     */
    [[nodiscard]] static bool is_running(std::string_view path) noexcept;

private:
    std::string path_;
    int         fd_ = -1;
};

} // namespace torrent
