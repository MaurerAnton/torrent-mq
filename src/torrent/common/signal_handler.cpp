/**
 * torrent-mq — Signal Handler Implementation
 *
 * Portable signal handling for graceful shutdown. Registers handlers
 * for SIGINT (Ctrl+C) and SIGTERM (kill, systemd stop) and provides
 * a blocking wait_for_signal() call that the main thread can use to
 * pause until shutdown is requested.
 *
 * Design:
 *   - Uses signalfd(2) on Linux for safe, pollable signal delivery.
 *     Falls back to a self-pipe trick + sigaction on other POSIX systems.
 *   - A single atomic flag records whether shutdown has been requested.
 *   - Handlers are registered once (idempotent). Multiple calls to
 *     register() are safe.
 *   - wait_for_signal() blocks until a signal arrives, then returns
 *     the signal number. On error it returns -1 with errno set.
 *   - shutdown_requested() is a non-blocking poll for the flag.
 *
 * Thread safety: the shutdown flag is atomic. Signal registration
 * uses a once-flag (std::call_once). The signal file descriptor is
 * created during registration and shared.
 */

#include "torrent/common/signal_handler.h"

#include <spdlog/spdlog.h>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

// POSIX headers
#include <unistd.h>
#include <fcntl.h>

#ifdef __linux__
#include <sys/signalfd.h>
#include <poll.h>
#endif

namespace torrent {

// ============================================================================
// Anonymous namespace — internal state
// ============================================================================

namespace {

// Logger
std::shared_ptr<spdlog::logger> get_signal_logger() {
    static auto logger = spdlog::get("signal_handler");
    if (!logger) {
        logger = spdlog::stdout_color_mt("signal_handler");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Shared state
// --------------------------------------------------------------------------

/// Whether shutdown has been requested by a caught signal.
std::atomic<bool> g_shutdown_requested{false};

/// The signal number that triggered shutdown (0 = none).
std::atomic<int> g_shutdown_signal{0};

/// Guards one-time registration.
std::once_flag g_registration_flag;

/// Signal file descriptor for signalfd (Linux) or read-end of self-pipe.
int g_signal_fd = -1;

#ifndef __linux__
/// Write-end of the self-pipe (used by the signal handler to notify the
/// main thread). Only used on non-Linux POSIX systems.
int g_pipe_write_fd = -1;
#endif

// --------------------------------------------------------------------------
// Signal handler (async-signal-safe)
// --------------------------------------------------------------------------

/**
 * Async-signal-safe signal handler.
 *
 * On Linux with signalfd, this handler is essentially a no-op because
 * signals are consumed via signalfd. We still set a handler to prevent
 * the default action (terminate/dump core).
 *
 * On non-Linux systems, we write a byte to the self-pipe to wake up
 * wait_for_signal().
 */
extern "C" void torrent_signal_action(int sig, siginfo_t* /*info*/, void* /*ctx*/) {
    // Preserve errno across signal handler execution.
    int saved_errno = errno;

    g_shutdown_requested.store(true, std::memory_order_release);
    g_shutdown_signal.store(sig, std::memory_order_release);

#ifndef __linux__
    // Write a byte to the self-pipe to wake up wait_for_signal().
    if (g_pipe_write_fd >= 0) {
        char byte = static_cast<char>(sig);
        // write(2) is async-signal-safe.
        ssize_t n = ::write(g_pipe_write_fd, &byte, 1);
        (void)n; // Best-effort; if the pipe is full we can't block here.
    }
#else
    (void)sig;
#endif

    errno = saved_errno;
}

// --------------------------------------------------------------------------
// Platform-specific registration
// --------------------------------------------------------------------------

#ifdef __linux__

/**
 * Linux: use signalfd(2) for synchronous signal consumption.
 *
 * signalfd provides a file descriptor that becomes readable when a
 * signal is delivered. This integrates naturally with event loops
 * (epoll / poll) and avoids async-signal-safety concerns.
 */
bool register_with_signalfd() {
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);

    // Block signals in this thread and all its children.
    // They will be delivered via signalfd instead.
    if (::pthread_sigmask(SIG_BLOCK, &mask, nullptr) != 0) {
        get_signal_logger()->error("pthread_sigmask(SIG_BLOCK) failed: {} (errno={})",
                                   std::strerror(errno), errno);
        return false;
    }

    // Create the signalfd (non-blocking, close-on-exec).
    g_signal_fd = ::signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (g_signal_fd < 0) {
        get_signal_logger()->error("signalfd() failed: {} (errno={})",
                                   std::strerror(errno), errno);
        return false;
    }

    get_signal_logger()->info("Signal handler registered via signalfd (fd={})",
                              g_signal_fd);
    return true;
}

#else // !__linux__ — fallback to sigaction + self-pipe

/**
 * Non-Linux POSIX: use sigaction + self-pipe.
 *
 * A pipe is created. The signal handler writes a byte to the write-end.
 * wait_for_signal() blocks reading from the read-end.
 */
bool register_with_self_pipe() {
    int pipefds[2];
    if (::pipe2(pipefds, O_NONBLOCK | O_CLOEXEC) < 0) {
        // pipe2 may not be available; fall back to pipe + fcntl.
        if (::pipe(pipefds) < 0) {
            get_signal_logger()->error("pipe() failed: {} (errno={})",
                                       std::strerror(errno), errno);
            return false;
        }
        // Set non-blocking and close-on-exec.
        int flags = ::fcntl(pipefds[0], F_GETFL, 0);
        if (flags >= 0) {
            ::fcntl(pipefds[0], F_SETFL, flags | O_NONBLOCK);
        }
        flags = ::fcntl(pipefds[1], F_GETFL, 0);
        if (flags >= 0) {
            ::fcntl(pipefds[1], F_SETFL, flags | O_NONBLOCK);
        }
        flags = ::fcntl(pipefds[0], F_GETFD, 0);
        if (flags >= 0) {
            ::fcntl(pipefds[0], F_SETFD, flags | FD_CLOEXEC);
        }
        flags = ::fcntl(pipefds[1], F_GETFD, 0);
        if (flags >= 0) {
            ::fcntl(pipefds[1], F_SETFD, flags | FD_CLOEXEC);
        }
    }

    g_signal_fd     = pipefds[0];
    g_pipe_write_fd = pipefds[1];

    // Install sigaction handlers for SIGINT and SIGTERM.
    struct sigaction sa;
    std::memset(&sa, 0, sizeof(sa));
    sa.sa_sigaction = torrent_signal_action;
    sa.sa_flags     = SA_SIGINFO | SA_RESTART;
    sigemptyset(&sa.sa_mask);

    if (::sigaction(SIGINT, &sa, nullptr) != 0) {
        get_signal_logger()->error("sigaction(SIGINT) failed: {} (errno={})",
                                   std::strerror(errno), errno);
        ::close(pipefds[0]);
        ::close(pipefds[1]);
        g_signal_fd = -1;
        g_pipe_write_fd = -1;
        return false;
    }

    if (::sigaction(SIGTERM, &sa, nullptr) != 0) {
        get_signal_logger()->error("sigaction(SIGTERM) failed: {} (errno={})",
                                   std::strerror(errno), errno);
        ::close(pipefds[0]);
        ::close(pipefds[1]);
        g_signal_fd = -1;
        g_pipe_write_fd = -1;
        return false;
    }

    get_signal_logger()->info("Signal handler registered via sigaction + self-pipe "
                              "(read_fd={}, write_fd={})",
                              g_signal_fd, g_pipe_write_fd);
    return true;
}

#endif // __linux__

} // anonymous namespace

// ============================================================================
// SignalHandler — Public API
// ============================================================================

bool SignalHandler::register_handler() {
    bool success = false;

    std::call_once(g_registration_flag, [&success] {
#ifdef __linux__
        success = register_with_signalfd();
#else
        success = register_with_self_pipe();
#endif

        if (success) {
            get_signal_logger()->info("SignalHandler: registered handlers for "
                                      "SIGINT and SIGTERM");
        } else {
            get_signal_logger()->error("SignalHandler: failed to register handlers");
        }
    });

    return success;
}

bool SignalHandler::shutdown_requested() noexcept {
    return g_shutdown_requested.load(std::memory_order_acquire);
}

int SignalHandler::shutdown_signal() noexcept {
    return g_shutdown_signal.load(std::memory_order_acquire);
}

int SignalHandler::wait_for_signal() {
    if (g_signal_fd < 0) {
        get_signal_logger()->error("SignalHandler::wait_for_signal: "
                                   "not registered — call register_handler() first");
        errno = EINVAL;
        return -1;
    }

    get_signal_logger()->info("SignalHandler: waiting for SIGINT or SIGTERM...");

#ifdef __linux__
    // --- signalfd path ---
    for (;;) {
        struct signalfd_siginfo fdsi;
        ssize_t n = ::read(g_signal_fd, &fdsi, sizeof(fdsi));

        if (n == sizeof(fdsi)) {
            int sig = static_cast<int>(fdsi.ssi_signo);
            get_signal_logger()->info("SignalHandler: caught signal {} ({})",
                                      sig, ::strsignal(sig));

            g_shutdown_requested.store(true, std::memory_order_release);
            g_shutdown_signal.store(sig, std::memory_order_release);
            return sig;
        }

        if (n < 0) {
            if (errno == EINTR) {
                continue; // interrupted by non-blocked signal, retry
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No signal yet; poll with a short sleep to avoid busy-looping.
                struct pollfd pfd;
                pfd.fd      = g_signal_fd;
                pfd.events  = POLLIN;
                pfd.revents = 0;

                int ret = ::poll(&pfd, 1, 100); // 100ms timeout
                if (ret < 0) {
                    if (errno == EINTR) continue;
                    get_signal_logger()->error("poll() on signalfd failed: {} (errno={})",
                                               std::strerror(errno), errno);
                    return -1;
                }
                if (ret == 0) {
                    // Timeout; loop and try again.
                    continue;
                }
                // fd is readable — loop and read will succeed.
                continue;
            }
            get_signal_logger()->error("read() on signalfd failed: {} (errno={})",
                                       std::strerror(errno), errno);
            return -1;
        }

        // n == 0: EOF on signalfd (should not happen).
        get_signal_logger()->error("Unexpected EOF on signalfd");
        return -1;
    }

#else // !__linux__
    // --- Self-pipe path ---
    for (;;) {
        char buf[64];
        ssize_t n = ::read(g_signal_fd, buf, sizeof(buf));

        if (n > 0) {
            // Got a signal notification. The byte value is the signal number.
            int sig = static_cast<int>(static_cast<unsigned char>(buf[0]));
            get_signal_logger()->info("SignalHandler: caught signal {} ({})",
                                      sig, ::strsignal(sig));
            return sig;
        }

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No signal yet. Check the atomic flag (set directly by handler).
                if (g_shutdown_requested.load(std::memory_order_acquire)) {
                    int sig = g_shutdown_signal.load(std::memory_order_acquire);
                    if (sig != 0) return sig;
                }
                // Sleep briefly to avoid busy-looping.
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            get_signal_logger()->error("read() on self-pipe failed: {} (errno={})",
                                       std::strerror(errno), errno);
            return -1;
        }
    }
#endif
}

void SignalHandler::clear_shutdown() noexcept {
    g_shutdown_requested.store(false, std::memory_order_release);
    g_shutdown_signal.store(0, std::memory_order_release);
}

std::string SignalHandler::signal_name(int sig) {
    const char* name = ::strsignal(sig);
    if (name) return std::string(name);
    return "SIGNAL_" + std::to_string(sig);
}

void SignalHandler::close_handler() noexcept {
    if (g_signal_fd >= 0) {
        ::close(g_signal_fd);
        g_signal_fd = -1;
    }
#ifndef __linux__
    if (g_pipe_write_fd >= 0) {
        ::close(g_pipe_write_fd);
        g_pipe_write_fd = -1;
    }
#endif
    get_signal_logger()->info("SignalHandler: handler closed");
}

} // namespace torrent
