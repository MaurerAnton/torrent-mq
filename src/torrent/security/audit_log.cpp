/**
 * torrent-mq — Structured Audit Log
 *
 * AuditLog provides thread-safe structured logging for security-relevant
 * events: authentication attempts, administrative operations, and
 * security-relevant state changes.
 *
 * Each audit entry is a single JSON line written to a file or syslog,
 * containing:
 *   - timestamp: ISO 8601 with milliseconds
 *   - event_type: "auth", "admin", or "security"
 *   - principal: the authenticated identity performing the action
 *   - action: what was done (login, create_topic, delete_topic, etc.)
 *   - resource: what was acted upon (topic name, user, config key)
 *   - result: "success" or "failure"
 *   - details: human-readable additional context
 *   - source_ip: originating IP address (if applicable)
 *
 * The audit log is designed to meet compliance requirements (SOC 2,
 * PCI DSS, HIPAA) by providing an immutable trail of all security-
 * sensitive operations.
 *
 * Output can be directed to:
 *   - A file (with automatic rotation)
 *   - syslog (via spdlog syslog sink)
 *   - Both
 *
 * Thread safety: all log methods are serialized via a mutex.
 */

#include "torrent/security/audit_log.h"
#include "torrent/broker/server.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/syslog_sink.h>
#include <spdlog/async.h>

#include <chrono>
#include <ctime>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace torrent::security {
namespace {

// --------------------------------------------------------------------------
// Logger for audit events (dedicated spdlog instance)
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_audit_logger() {
    static auto logger = spdlog::get("audit");
    if (!logger) {
        // Default: color console for development. Production should
        // configure file or syslog sinks via BrokerServer startup.
        logger = spdlog::stdout_color_mt("audit");
        logger->set_level(spdlog::level::info);
        // Use a pattern that includes timestamp to millisecond precision
        logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
    }
    return logger;
}

// --------------------------------------------------------------------------
// Timestamp formatting: ISO 8601 with milliseconds
// --------------------------------------------------------------------------

std::string format_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;

    std::tm tm;
    localtime_r(&time_t_now, &tm);

    char buf[64];
    std::snprintf(buf, sizeof(buf),
                  "%04d-%02d-%02dT%02d:%02d:%02d.%03ld",
                  tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                  tm.tm_hour, tm.tm_min, tm.tm_sec,
                  ms.count());
    return std::string(buf);
}

// --------------------------------------------------------------------------
// JSON-safe string escaping
// --------------------------------------------------------------------------

std::string json_escape(std::string_view input) {
    std::string output;
    output.reserve(input.size() + 2);
    for (char c : input) {
        switch (c) {
        case '"':  output += "\\\""; break;
        case '\\': output += "\\\\"; break;
        case '\n': output += "\\n";  break;
        case '\r': output += "\\r";  break;
        case '\t': output += "\\t";  break;
        case '\b': output += "\\b";  break;
        case '\f': output += "\\f";  break;
        default:
            if (static_cast<unsigned char>(c) < 0x20) {
                char hex[7];
                std::snprintf(hex, sizeof(hex), "\\u%04x",
                              static_cast<unsigned char>(c));
                output += hex;
            } else {
                output += c;
            }
        }
    }
    return output;
}

// --------------------------------------------------------------------------
// Build a JSON audit entry
// --------------------------------------------------------------------------

std::string build_audit_entry(const std::string& event_type,
                               const std::string& principal,
                               const std::string& action,
                               const std::string& resource,
                               const std::string& result,
                               const std::string& details,
                               const std::string& source_ip) {
    std::ostringstream oss;
    oss << "{";
    oss << "\"timestamp\":\"" << format_timestamp() << "\",";
    oss << "\"event_type\":\"" << json_escape(event_type) << "\",";
    oss << "\"principal\":\"" << json_escape(principal) << "\",";
    oss << "\"action\":\"" << json_escape(action) << "\",";
    oss << "\"resource\":\"" << json_escape(resource) << "\",";
    oss << "\"result\":\"" << json_escape(result) << "\",";
    oss << "\"details\":\"" << json_escape(details) << "\",";
    if (!source_ip.empty()) {
        oss << "\"source_ip\":\"" << json_escape(source_ip) << "\",";
    }
    oss << "\"service\":\"torrent-mq\"";
    oss << "}";
    return oss.str();
}

} // anonymous namespace

// ============================================================================
// AuditLog — Public Implementation
// ============================================================================

AuditLog::AuditLog(broker::BrokerServer& s)
    : server_(&s)
{
    auto logger = get_audit_logger();
    logger->info("AuditLog initialized");

    // Initialize default audit log file if configured
    // In production, this would read from the broker config
}

// ============================================================================
// Core logging methods
// ============================================================================

/**
 * Log an authentication event.
 *
 * Records every login attempt, whether successful or failed. This is
 * critical for detecting brute-force attacks, credential stuffing,
 * and unauthorized access attempts.
 *
 * @param principal  The identity attempting to authenticate (username, service account).
 * @param action     The authentication action: "login", "sasl_handshake", "token_refresh".
 * @param success    Whether the authentication succeeded.
 */
void AuditLog::log_auth(const std::string& principal,
                         const std::string& action,
                         bool success) {
    // Build the audit entry
    std::string result = success ? "success" : "failure";
    std::string details;

    if (!success) {
        details = fmt::format("Authentication failed for principal '{}' via '{}'",
                              principal, action);
    } else {
        details = fmt::format("Authentication succeeded for principal '{}' via '{}'",
                              principal, action);
    }

    auto entry = build_audit_entry("auth", principal, action, "authentication",
                                    result, details, "");

    // Write to audit log (thread-safe via spdlog's internal mutex)
    auto logger = get_audit_logger();
    if (success) {
        logger->info(entry);
    } else {
        logger->warn(entry);
    }
}

/**
 * Log an administrative operation.
 *
 * Captures all admin actions such as topic creation/deletion, config changes,
 * ACL modifications, user management, and cluster operations. This provides
 * a complete audit trail for change management and compliance.
 *
 * @param principal   The admin identity performing the operation.
 * @param operation   The operation name: "create_topic", "delete_topic",
 *                    "alter_config", "reassign_partitions", etc.
 * @param resource    The resource being acted upon: topic name, config key,
 *                    broker ID, etc.
 */
void AuditLog::log_admin(const std::string& principal,
                          const std::string& operation,
                          const std::string& resource) {
    auto entry = build_audit_entry("admin", principal, operation,
                                    resource, "success",
                                    fmt::format("Admin operation '{}' on '{}' by '{}'",
                                                operation, resource, principal),
                                    "");

    auto logger = get_audit_logger();
    logger->info(entry);
}

/**
 * Log a security-relevant event.
 *
 * Used for events that don't fit neatly into authentication or admin
 * categories, such as:
 *   - Encryption key rotation
 *   - TLS certificate changes
 *   - ACL policy modifications
 *   - Firewall/IP whitelist changes
 *   - Security configuration changes
 *   - Suspicious activity detection
 *
 * @param event    The security event type.
 * @param details  Human-readable description of what happened.
 */
void AuditLog::log_security(const std::string& event,
                             const std::string& details) {
    auto entry = build_audit_entry("security", "system", event,
                                    "security", "success",
                                    details, "");

    auto logger = get_audit_logger();
    logger->info(entry);
}

/**
 * Log an admin operation that failed.
 *
 * Separates successful and failed admin operations for easier
 * querying and alerting.
 *
 * @param principal  The admin identity.
 * @param operation  The operation attempted.
 * @param resource   The target resource.
 * @param error      The error message describing the failure.
 */
void AuditLog::log_admin_failure(const std::string& principal,
                                  const std::string& operation,
                                  const std::string& resource,
                                  const std::string& error) {
    auto entry = build_audit_entry("admin", principal, operation,
                                    resource, "failure",
                                    fmt::format("Admin operation '{}' on '{}' by '{}' failed: {}",
                                                operation, resource, principal, error),
                                    "");

    auto logger = get_audit_logger();
    logger->warn(entry);
}

/**
 * Log a security event that represents a potential threat or violation.
 *
 * @param event    The security event type (e.g., "unauthorized_access_attempt").
 * @param details  Description of the suspicious activity.
 */
void AuditLog::log_security_alert(const std::string& event,
                                   const std::string& details) {
    auto entry = build_audit_entry("security", "system", event,
                                    "security", "alert",
                                    details, "");

    auto logger = get_audit_logger();
    logger->error(entry);
}

/**
 * Log an authentication event with source IP tracking.
 *
 * @param principal  The identity attempting to authenticate.
 * @param action     The authentication action.
 * @param success    Whether authentication succeeded.
 * @param source_ip  The client IP address.
 */
void AuditLog::log_auth_with_ip(const std::string& principal,
                                 const std::string& action,
                                 bool success,
                                 const std::string& source_ip) {
    std::string result = success ? "success" : "failure";
    std::string details;

    if (!success) {
        details = fmt::format("Authentication failed for principal '{}' via '{}' from {}",
                              principal, action, source_ip);
    } else {
        details = fmt::format("Authentication succeeded for principal '{}' via '{}' from {}",
                              principal, action, source_ip);
    }

    auto entry = build_audit_entry("auth", principal, action,
                                    "authentication", result,
                                    details, source_ip);

    auto logger = get_audit_logger();
    if (success) {
        logger->info(entry);
    } else {
        logger->warn(entry);
    }
}

// ============================================================================
// Configuration
// ============================================================================

/**
 * Configure the audit log to write to a file with optional rotation.
 *
 * @param file_path   Path to the audit log file.
 * @param rotate      If true, enables automatic log rotation.
 */
void AuditLog::set_file_output(const std::string& file_path, bool rotate) {
    std::lock_guard<std::mutex> lock(config_mutex_);

    auto logger = spdlog::get("audit");
    if (logger) {
        spdlog::drop("audit");
    }

    if (rotate) {
        // Create a rotating file sink: max 10 files of 100 MiB each
        auto sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            file_path, 100 * 1024 * 1024, 10);
        logger = std::make_shared<spdlog::logger>("audit", sink);
    } else {
        auto sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(file_path);
        logger = std::make_shared<spdlog::logger>("audit", sink);
    }

    logger->set_level(spdlog::level::info);
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
    spdlog::register_logger(logger);

    get_audit_logger()->info("Audit log file output configured: {}", file_path);
}

/**
 * Configure the audit log to write to syslog.
 *
 * @param ident  Syslog identifier (typically "torrent-mq").
 */
void AuditLog::set_syslog_output(const std::string& ident) {
    std::lock_guard<std::mutex> lock(config_mutex_);

    auto logger = spdlog::get("audit");
    if (logger) {
        spdlog::drop("audit");
    }

    auto sink = std::make_shared<spdlog::sinks::syslog_sink_mt>(
        ident, LOG_PID, LOG_AUTHPRIV);
    logger = std::make_shared<spdlog::logger>("audit", sink);
    logger->set_level(spdlog::level::info);
    logger->set_pattern("%v");  // syslog adds its own timestamp
    spdlog::register_logger(logger);

    get_audit_logger()->info("Audit log syslog output configured: {}", ident);
}

/**
 * Configure the audit log to write to both file and syslog.
 *
 * @param file_path  Path to the audit log file.
 * @param syslog_ident  Syslog identifier.
 */
void AuditLog::set_dual_output(const std::string& file_path,
                                const std::string& syslog_ident) {
    std::lock_guard<std::mutex> lock(config_mutex_);

    auto logger = spdlog::get("audit");
    if (logger) {
        spdlog::drop("audit");
    }

    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(file_path);
    auto syslog_sink = std::make_shared<spdlog::sinks::syslog_sink_mt>(
        syslog_ident, LOG_PID, LOG_AUTHPRIV);

    std::vector<spdlog::sink_ptr> sinks = {file_sink, syslog_sink};
    logger = std::make_shared<spdlog::logger>("audit", sinks.begin(), sinks.end());
    logger->set_level(spdlog::level::info);
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
    spdlog::register_logger(logger);

    get_audit_logger()->info("Audit log dual output configured: {} + syslog",
                             file_path);
}

} // namespace torrent::security
