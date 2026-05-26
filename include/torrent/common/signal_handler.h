#pragma once

/**
 * torrent-mq — SignalHandler
 *
 * Portable signal handling for graceful shutdown. Registers handlers
 * for SIGINT (Ctrl+C) and SIGTERM (kill, systemd stop) and provides
 * a blocking wait_for_signal() call.
 *
 * On Linux: uses signalfd(2) for safe, pollable signal delivery.
 * On other POSIX: uses sigaction + a self-pipe to wake the main thread.
 *
 * Thread safety: the shutdown flag is atomic. Registration uses
 * std::call_once and is idempotent.
 */

#include <atomic>
#include <string>

namespace torrent {

class SignalHandler {
public:
    // -- Registration -----------------------------------------------------

    /**
     * Register handlers for SIGINT and SIGTERM.
     *
     * Creates a signalfd (Linux) or self-pipe (POSIX) for signal delivery.
     * Idempotent — subsequent calls are no-ops.
     *
     * @return true on success, false on failure (errno is set).
     */
    static bool register_handler();

    // -- Shutdown queries -------------------------------------------------

    /// True if a SIGINT or SIGTERM has been received.
    [[nodiscard]] static bool shutdown_requested() noexcept;

    /// The signal number that triggered shutdown (0 if none).
    [[nodiscard]] static int shutdown_signal() noexcept;

    /// Reset the shutdown flag (useful for testing or SIGHUP reload).
    static void clear_shutdown() noexcept;

    // -- Blocking wait ----------------------------------------------------

    /**
     * Block until SIGINT or SIGTERM is received.
     *
     * On Linux, this blocks on read() from the signalfd. On other POSIX
     * systems, it blocks on read() from the self-pipe. Returns the signal
     * number on success, -1 on error (errno is set).
     *
     * Must call register_handler() before calling this.
     */
    [[nodiscard]] static int wait_for_signal();

    // -- Utilities --------------------------------------------------------

    /// Human-readable signal name (e.g. "Interrupt" for SIGINT).
    [[nodiscard]] static std::string signal_name(int sig);

    /// Close the signal file descriptor / self-pipe and release resources.
    static void close_handler() noexcept;
};

} // namespace torrent
