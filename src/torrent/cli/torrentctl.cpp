/**
 * torrentctl — torrent-mq CLI Admin Tool
 *
 * A command-line interface for managing a torrent-mq cluster via the admin
 * REST API (default http://localhost:9644).  Subcommands cover topic CRUD,
 * consumer group introspection, cluster health, ACL rules, and dynamic
 * configuration.
 *
 * Usage:
 *   torrentctl [--admin-url <url>] <command> [args...]
 *
 * Commands:
 *   topic create    <name> [-p <partitions>] [-r <rf>]
 *   topic list
 *   topic delete    <name>
 *   topic describe  <name>
 *
 *   consumer list
 *   consumer describe <group>
 *   consumer delete    <group>
 *   consumer offset    <group>
 *
 *   cluster health
 *   cluster brokers
 *   cluster describe
 *
 *   acl list
 *   acl add    <principal> <resource> <op> [--host <h>] [--allow|--deny]
 *   acl delete <principal> <resource> <op> [--host <h>]
 *
 *   config list
 *   config get  <key>
 *   config set  <key> <value>
 *
 * Build: relies on POSIX sockets, nlohmann/json, and fmt for formatting.
 * No external HTTP client library — uses a minimal HTTP/1.1 transport.
 */

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// POSIX sockets for HTTP transport
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

// ============================================================================
// Global configuration
// ============================================================================

namespace {

std::string g_admin_host = "localhost";
uint16_t    g_admin_port = 9644;
bool        g_json_output = false; ///< --json flag for machine-readable output
bool        g_verbose = false;

// ============================================================================
// Minimal HTTP client
// ============================================================================

/// Perform an HTTP request to the admin API and return the response body.
/// Throws std::runtime_error on connection or protocol errors.
std::string http_request(const std::string& method,
                          const std::string& path,
                          const std::string& body = "") {
    // Resolve host
    struct addrinfo hints = {};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* result = nullptr;

    std::string port_str = std::to_string(g_admin_port);
    int rc = getaddrinfo(g_admin_host.c_str(), port_str.c_str(), &hints, &result);
    if (rc != 0) {
        throw std::runtime_error("Failed to resolve " + g_admin_host +
                                 ": " + gai_strerror(rc));
    }

    int sock = -1;
    for (auto* rp = result; rp != nullptr; rp = rp->ai_next) {
        sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock < 0) continue;
        if (connect(sock, rp->ai_addr, rp->ai_addrlen) == 0) break;
        close(sock);
        sock = -1;
    }
    freeaddrinfo(result);

    if (sock < 0) {
        throw std::runtime_error("Cannot connect to " + g_admin_host +
                                 ":" + port_str);
    }

    // Build HTTP request
    std::ostringstream req;
    req << method << " " << path << " HTTP/1.1\r\n";
    req << "Host: " << g_admin_host << ":" << port_str << "\r\n";
    req << "Accept: application/json\r\n";
    if (!body.empty()) {
        req << "Content-Type: application/json\r\n";
        req << "Content-Length: " << body.size() << "\r\n";
    }
    req << "Connection: close\r\n";
    req << "\r\n";
    req << body;

    std::string req_str = req.str();

    // Send
    size_t sent = 0;
    while (sent < req_str.size()) {
        ssize_t n = send(sock, req_str.data() + sent, req_str.size() - sent, 0);
        if (n < 0) {
            close(sock);
            throw std::runtime_error("Send failed: " +
                                     std::string(strerror(errno)));
        }
        sent += static_cast<size_t>(n);
    }

    // Receive response
    std::string response;
    char buf[65536];
    while (true) {
        ssize_t n = recv(sock, buf, sizeof(buf), 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            close(sock);
            throw std::runtime_error("Recv failed: " +
                                     std::string(strerror(errno)));
        }
        if (n == 0) break;
        response.append(buf, static_cast<size_t>(n));
    }
    close(sock);

    // Find the body (after \r\n\r\n)
    auto header_end = response.find("\r\n\r\n");
    if (header_end == std::string::npos) {
        throw std::runtime_error("Invalid HTTP response (no header terminator)");
    }

    std::string response_body = response.substr(header_end + 4);

    // Chunked transfer-encoding is not handled; assume Content-Length

    if (g_verbose) {
        std::cerr << "[verbose] Response body: " << response_body << "\n";
    }

    return response_body;
}

/// Convenience: GET request.
std::string http_get(const std::string& path) {
    return http_request("GET", path);
}

/// Convenience: POST request.
std::string http_post(const std::string& path, const std::string& body) {
    return http_request("POST", path, body);
}

/// Convenience: PUT request.
std::string http_put(const std::string& path, const std::string& body) {
    return http_request("PUT", path, body);
}

/// Convenience: DELETE request.
std::string http_delete(const std::string& path) {
    return http_request("DELETE", path);
}

/// Parse JSON body with error handling.
json parse_json(const std::string& body) {
    try {
        return json::parse(body);
    } catch (const json::parse_error& e) {
        throw std::runtime_error("Failed to parse JSON response: " +
                                 std::string(e.what()));
    }
}

// ============================================================================
// Output helpers
// ============================================================================

void print_json(const json& j) {
    std::cout << j.dump(g_json_output ? 1 : 4) << "\n";
}

void print_table(const std::vector<std::string>& headers,
                  const std::vector<std::vector<std::string>>& rows) {
    // Calculate column widths
    std::vector<size_t> widths(headers.size(), 0);
    for (size_t i = 0; i < headers.size(); ++i) {
        widths[i] = headers[i].size();
    }
    for (const auto& row : rows) {
        for (size_t i = 0; i < row.size() && i < widths.size(); ++i) {
            widths[i] = std::max(widths[i], row[i].size());
        }
    }

    // Print header
    for (size_t i = 0; i < headers.size(); ++i) {
        std::cout << std::left << std::setw(static_cast<int>(widths[i] + 2))
                  << headers[i];
    }
    std::cout << "\n";

    // Print separator
    for (size_t i = 0; i < headers.size(); ++i) {
        std::cout << std::string(widths[i], '-') << "  ";
    }
    std::cout << "\n";

    // Print rows
    for (const auto& row : rows) {
        for (size_t i = 0; i < row.size() && i < widths.size(); ++i) {
            std::cout << std::left << std::setw(static_cast<int>(widths[i] + 2))
                      << row[i];
        }
        std::cout << "\n";
    }
}

// ============================================================================
// Command implementations
// ============================================================================

// --------------------------------------------------------------------------
// topic create <name> [-p <partitions>] [-r <rf>]
// --------------------------------------------------------------------------

int cmd_topic_create(const std::vector<std::string>& args) {
    if (args.size() < 1) {
        std::cerr << "Usage: torrentctl topic create <name> [-p <partitions>] [-r <rf>]\n";
        return 1;
    }

    std::string name = args[0];
    int32_t partitions = 1;
    int16_t replication = 1;

    for (size_t i = 1; i < args.size(); ++i) {
        if (args[i] == "-p" && i + 1 < args.size()) {
            partitions = std::stoi(args[++i]);
        } else if (args[i] == "-r" && i + 1 < args.size()) {
            replication = static_cast<int16_t>(std::stoi(args[++i]));
        }
    }

    json body;
    body["topic"]             = name;
    body["partitions"]        = partitions;
    body["replication_factor"] = replication;

    try {
        std::string resp = http_post("/v1/topics", body.dump());
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
        } else {
            if (j.contains("error")) {
                std::cerr << "Error: " << j["error"].get<std::string>() << "\n";
                return 1;
            }
            std::cout << "Topic '" << name << "' created successfully.\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// topic list
// --------------------------------------------------------------------------

int cmd_topic_list(const std::vector<std::string>&) {
    try {
        std::string resp = http_get("/v1/topics");
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
            return 0;
        }

        if (!j.is_array()) {
            std::cerr << "Unexpected response format.\n";
            return 1;
        }

        std::vector<std::string> headers = {"NAME", "PARTITIONS", "REPLICATION"};
        std::vector<std::vector<std::string>> rows;

        for (const auto& topic : j) {
            std::vector<std::string> row;
            row.push_back(topic.value("name", "?"));
            row.push_back(std::to_string(topic.value("partitions", 0)));
            row.push_back(std::to_string(topic.value("replication_factor", 0)));
            rows.push_back(std::move(row));
        }

        if (rows.empty()) {
            std::cout << "No topics found.\n";
        } else {
            print_table(headers, rows);
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// topic delete <name>
// --------------------------------------------------------------------------

int cmd_topic_delete(const std::vector<std::string>& args) {
    if (args.size() < 1) {
        std::cerr << "Usage: torrentctl topic delete <name>\n";
        return 1;
    }
    try {
        std::string resp = http_delete("/v1/topics/" + args[0]);
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
        } else {
            std::cout << "Topic '" << args[0] << "' deleted.\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// topic describe <name>
// --------------------------------------------------------------------------

int cmd_topic_describe(const std::vector<std::string>& args) {
    if (args.size() < 1) {
        std::cerr << "Usage: torrentctl topic describe <name>\n";
        return 1;
    }
    try {
        std::string resp = http_get("/v1/topics/" + args[0]);
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
        } else {
            std::cout << "Topic: " << j.value("name", "?") << "\n";
            std::cout << "  Partitions: " << j.value("partitions", 0) << "\n";
            std::cout << "  Replication factor: "
                      << j.value("replication_factor", 0) << "\n";
            if (j.contains("config")) {
                std::cout << "  Config:\n";
                for (const auto& [k, v] : j["config"].items()) {
                    std::cout << "    " << k << " = " << v << "\n";
                }
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// consumer list
// --------------------------------------------------------------------------

int cmd_consumer_list(const std::vector<std::string>&) {
    try {
        std::string resp = http_get("/v1/consumer_groups");
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
            return 0;
        }

        if (!j.is_array()) {
            std::cerr << "Unexpected response format.\n";
            return 1;
        }

        std::vector<std::string> headers = {"GROUP", "STATE", "MEMBERS"};
        std::vector<std::vector<std::string>> rows;

        for (const auto& g : j) {
            std::vector<std::string> row;
            row.push_back(g.value("group_id", "?"));
            row.push_back(g.value("state", "?"));
            row.push_back(std::to_string(g.value("members", 0)));
            rows.push_back(std::move(row));
        }

        if (rows.empty()) {
            std::cout << "No consumer groups found.\n";
        } else {
            print_table(headers, rows);
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// consumer describe <group>
// --------------------------------------------------------------------------

int cmd_consumer_describe(const std::vector<std::string>& args) {
    if (args.size() < 1) {
        std::cerr << "Usage: torrentctl consumer describe <group>\n";
        return 1;
    }
    try {
        std::string resp = http_get("/v1/consumer_groups/" + args[0]);
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
        } else {
            std::cout << "Group: " << j.value("group_id", "?") << "\n";
            std::cout << "  State: " << j.value("state", "?") << "\n";
            std::cout << "  Protocol: " << j.value("protocol", "?") << "\n";
            std::cout << "  Members: " << j.value("members", 0) << "\n";
            if (j.contains("offsets")) {
                std::cout << "  Offsets:\n";
                for (const auto& o : j["offsets"]) {
                    std::cout << "    " << o.value("topic", "?")
                              << "/" << o.value("partition", 0)
                              << " → " << o.value("offset", 0) << "\n";
                }
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// consumer delete <group>
// --------------------------------------------------------------------------

int cmd_consumer_delete(const std::vector<std::string>& args) {
    if (args.size() < 1) {
        std::cerr << "Usage: torrentctl consumer delete <group>\n";
        return 1;
    }
    try {
        std::string resp = http_delete("/v1/consumer_groups/" + args[0]);
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
        } else {
            std::cout << "Consumer group '" << args[0] << "' deleted.\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// consumer offset <group>
// --------------------------------------------------------------------------

int cmd_consumer_offset(const std::vector<std::string>& args) {
    if (args.size() < 1) {
        std::cerr << "Usage: torrentctl consumer offset <group>\n";
        return 1;
    }
    try {
        std::string resp = http_get("/v1/consumer_groups/" + args[0] + "/offsets");
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
        } else {
            if (!j.is_array()) {
                std::cerr << "Unexpected response format.\n";
                return 1;
            }
            std::vector<std::string> headers = {"TOPIC", "PARTITION", "OFFSET", "LAG"};
            std::vector<std::vector<std::string>> rows;
            for (const auto& o : j) {
                std::vector<std::string> row;
                row.push_back(o.value("topic", "?"));
                row.push_back(std::to_string(o.value("partition", 0)));
                row.push_back(std::to_string(o.value("offset", 0)));
                row.push_back(std::to_string(o.value("lag", 0)));
                rows.push_back(std::move(row));
            }
            print_table(headers, rows);
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// cluster health
// --------------------------------------------------------------------------

int cmd_cluster_health(const std::vector<std::string>&) {
    try {
        std::string resp = http_get("/v1/cluster/health");
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
        } else {
            std::cout << "Cluster health: "
                      << (j.value("healthy", false) ? "HEALTHY" : "UNHEALTHY")
                      << "\n";
            std::cout << "  State:    " << j.value("state", "unknown") << "\n";
            std::cout << "  Brokers:  " << j.value("broker_count", 0) << "\n";
            std::cout << "  Topics:   " << j.value("topic_count", 0) << "\n";
            std::cout << "  Partitions: " << j.value("partition_count", 0) << "\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// cluster brokers
// --------------------------------------------------------------------------

int cmd_cluster_brokers(const std::vector<std::string>&) {
    try {
        std::string resp = http_get("/v1/cluster/brokers");
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
        } else {
            if (!j.is_array()) {
                std::cerr << "Unexpected response format.\n";
                return 1;
            }
            std::vector<std::string> headers = {"ID", "HOST", "PORT", "RACK", "CONTROLLER"};
            std::vector<std::vector<std::string>> rows;
            for (const auto& b : j) {
                std::vector<std::string> row;
                row.push_back(std::to_string(b.value("node_id", -1)));
                row.push_back(b.value("host", "?"));
                row.push_back(std::to_string(b.value("port", 0)));
                row.push_back(b.value("rack", ""));
                row.push_back(b.value("is_controller", false) ? "yes" : "no");
                rows.push_back(std::move(row));
            }
            print_table(headers, rows);
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// cluster describe
// --------------------------------------------------------------------------

int cmd_cluster_describe(const std::vector<std::string>&) {
    try {
        std::string resp = http_get("/v1/cluster");
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
        } else {
            std::cout << "Cluster: " << j.value("cluster_id", "?") << "\n";
            std::cout << "  Controller: " << j.value("controller_id", -1) << "\n";
            std::cout << "  Brokers: " << j.value("broker_count", 0) << "\n";
            std::cout << "  Topics: " << j.value("topic_count", 0) << "\n";
            std::cout << "  Partitions: " << j.value("partition_count", 0) << "\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// acl list
// --------------------------------------------------------------------------

int cmd_acl_list(const std::vector<std::string>&) {
    try {
        std::string resp = http_get("/v1/security/acls");
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
        } else {
            if (!j.is_array()) {
                std::cerr << "No ACLs found.\n";
                return 0;
            }
            std::vector<std::string> headers = {"PRINCIPAL", "HOST", "RESOURCE",
                                                  "OPERATION", "PERMISSION"};
            std::vector<std::vector<std::string>> rows;
            for (const auto& a : j) {
                std::vector<std::string> row;
                row.push_back(a.value("principal", "*"));
                row.push_back(a.value("host", "*"));
                row.push_back(a.value("resource_name", "?"));
                row.push_back(a.value("operation", "?"));
                row.push_back(a.value("permission_type", "?"));
                rows.push_back(std::move(row));
            }
            print_table(headers, rows);
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// acl add <principal> <resource> <op> [--host <h>] [--allow|--deny]
// --------------------------------------------------------------------------

int cmd_acl_add(const std::vector<std::string>& args) {
    std::string principal;
    std::string resource;
    std::string operation;
    std::string host = "*";
    std::string perm = "ALLOW";

    size_t pos = 0;
    if (args.size() > 0) principal = args[pos++];
    if (args.size() > 1) resource  = args[pos++];
    if (args.size() > 2) operation = args[pos++];

    while (pos < args.size()) {
        if (args[pos] == "--host" && pos + 1 < args.size()) {
            host = args[++pos];
        } else if (args[pos] == "--allow") {
            perm = "ALLOW";
        } else if (args[pos] == "--deny") {
            perm = "DENY";
        }
        ++pos;
    }

    if (principal.empty() || resource.empty() || operation.empty()) {
        std::cerr << "Usage: torrentctl acl add <principal> <resource> <op> "
                     "[--host <h>] [--allow|--deny]\n";
        return 1;
    }

    json body;
    body["principal"] = principal;
    body["host"]       = host;
    body["resource_name"] = resource;
    body["resource_type"] = "TOPIC";
    body["operation"]     = operation;
    body["permission_type"] = perm;

    try {
        std::string resp = http_post("/v1/security/acls", body.dump());
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
        } else {
            std::cout << "ACL added: " << principal << " " << operation
                      << " on " << resource << " (" << perm << ")\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// acl delete <principal> <resource> <op> [--host <h>]
// --------------------------------------------------------------------------

int cmd_acl_delete(const std::vector<std::string>& args) {
    std::string principal;
    std::string resource;
    std::string operation;
    std::string host = "*";

    size_t pos = 0;
    if (args.size() > 0) principal = args[pos++];
    if (args.size() > 1) resource  = args[pos++];
    if (args.size() > 2) operation = args[pos++];
    if (pos < args.size() && args[pos] == "--host" && pos + 1 < args.size()) {
        host = args[pos + 1];
    }

    if (principal.empty() || resource.empty() || operation.empty()) {
        std::cerr << "Usage: torrentctl acl delete <principal> <resource> <op> "
                     "[--host <h>]\n";
        return 1;
    }

    json body;
    body["principal"] = principal;
    body["host"]       = host;
    body["resource_name"] = resource;
    body["resource_type"] = "TOPIC";
    body["operation"]     = operation;

    try {
        std::string resp = http_request("DELETE", "/v1/security/acls", body.dump());
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
        } else {
            std::cout << "ACL deleted: " << principal << " " << operation
                      << " on " << resource << "\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// config list
// --------------------------------------------------------------------------

int cmd_config_list(const std::vector<std::string>&) {
    try {
        std::string resp = http_get("/v1/config");
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
        } else {
            if (!j.is_object() && !j.is_array()) {
                std::cout << "No configuration available.\n";
                return 0;
            }
            std::vector<std::string> headers = {"KEY", "VALUE", "SOURCE"};
            std::vector<std::vector<std::string>> rows;
            for (const auto& [k, v] : j.items()) {
                std::vector<std::string> row;
                row.push_back(k);
                if (v.is_string()) {
                    row.push_back(v.get<std::string>());
                } else {
                    row.push_back(v.dump());
                }
                row.push_back("dynamic");
                rows.push_back(std::move(row));
            }
            print_table(headers, rows);
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// config get <key>
// --------------------------------------------------------------------------

int cmd_config_get(const std::vector<std::string>& args) {
    if (args.size() < 1) {
        std::cerr << "Usage: torrentctl config get <key>\n";
        return 1;
    }
    try {
        std::string resp = http_get("/v1/config/" + args[0]);
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
        } else {
            std::cout << args[0] << " = " << j.value("value", "?") << "\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// --------------------------------------------------------------------------
// config set <key> <value>
// --------------------------------------------------------------------------

int cmd_config_set(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cerr << "Usage: torrentctl config set <key> <value>\n";
        return 1;
    }
    json body;
    body["key"]   = args[0];
    body["value"] = args[1];
    try {
        std::string resp = http_put("/v1/config/" + args[0], body.dump());
        json j = parse_json(resp);
        if (g_json_output) {
            print_json(j);
        } else {
            std::cout << args[0] << " = " << args[1] << " (set)\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

// ============================================================================
// Dispatcher table
// ============================================================================

using CommandFn = std::function<int(const std::vector<std::string>&)>;

struct CommandEntry {
    std::string name;
    std::string description;
    CommandFn   fn;
    std::string usage;
};

const std::vector<CommandEntry> kCommands = {
    // Topic commands
    {"topic create",    "Create a new topic",
     cmd_topic_create,   "topic create <name> [-p <partitions>] [-r <rf>]"},
    {"topic list",      "List all topics",
     cmd_topic_list,     "topic list"},
    {"topic delete",    "Delete a topic",
     cmd_topic_delete,   "topic delete <name>"},
    {"topic describe",  "Describe a topic",
     cmd_topic_describe, "topic describe <name>"},

    // Consumer commands
    {"consumer list",      "List consumer groups",
     cmd_consumer_list,      "consumer list"},
    {"consumer describe",  "Describe a consumer group",
     cmd_consumer_describe,  "consumer describe <group>"},
    {"consumer delete",    "Delete a consumer group",
     cmd_consumer_delete,    "consumer delete <group>"},
    {"consumer offset",    "Show offsets for a consumer group",
     cmd_consumer_offset,    "consumer offset <group>"},

    // Cluster commands
    {"cluster health",    "Show cluster health",
     cmd_cluster_health,    "cluster health"},
    {"cluster brokers",   "List cluster brokers",
     cmd_cluster_brokers,   "cluster brokers"},
    {"cluster describe",  "Describe the cluster",
     cmd_cluster_describe,  "cluster describe"},

    // ACL commands
    {"acl list",          "List ACL rules",
     cmd_acl_list,          "acl list"},
    {"acl add",           "Add an ACL rule",
     cmd_acl_add,           "acl add <principal> <resource> <op> [--host <h>] [--allow|--deny]"},
    {"acl delete",        "Delete an ACL rule",
     cmd_acl_delete,        "acl delete <principal> <resource> <op> [--host <h>]"},

    // Config commands
    {"config list",       "List all dynamic configuration",
     cmd_config_list,       "config list"},
    {"config get",        "Get a config value",
     cmd_config_get,        "config get <key>"},
    {"config set",        "Set a config value",
     cmd_config_set,        "config set <key> <value>"},
};

// ============================================================================
// Entry point
// ============================================================================

void print_usage() {
    std::cout << "torrentctl — torrent-mq admin CLI\n\n";
    std::cout << "Usage: torrentctl [--admin-url <url>] [--json] [--verbose] "
              << "<command> [args...]\n\n";
    std::cout << "Commands:\n";

    // Group by category
    std::string current_category;
    for (const auto& cmd : kCommands) {
        auto space = cmd.name.find(' ');
        std::string category = (space != std::string::npos)
                                   ? cmd.name.substr(0, space)
                                   : cmd.name;
        if (category != current_category) {
            current_category = category;
            std::cout << "\n  [" << current_category << "]\n";
        }
        std::cout << "    " << std::left << std::setw(22) << cmd.name
                  << cmd.description << "\n";
    }

    std::cout << "\nOptions:\n";
    std::cout << "  --admin-url <url>   Admin API URL (default http://localhost:9644)\n";
    std::cout << "  --json              Output in JSON format\n";
    std::cout << "  --verbose           Verbose output\n";
    std::cout << "  --help, -h          Show this help\n";
}

} // anonymous namespace

// ============================================================================
// main
// ============================================================================

int main(int argc, char** argv) {
    if (argc < 2) {
        print_usage();
        return 1;
    }

    std::vector<std::string> args;
    for (int i = 1; i < argc; ++i) {
        args.emplace_back(argv[i]);
    }

    // Parse global flags
    std::vector<std::string> cmd_args;
    for (size_t i = 0; i < args.size(); ++i) {
        if (args[i] == "--admin-url" && i + 1 < args.size()) {
            std::string url = args[++i];
            // Parse host:port from URL
            auto proto_end = url.find("://");
            std::string hostport = (proto_end != std::string::npos)
                                       ? url.substr(proto_end + 3)
                                       : url;
            auto colon = hostport.find(':');
            if (colon != std::string::npos) {
                g_admin_host = hostport.substr(0, colon);
                g_admin_port = static_cast<uint16_t>(std::stoi(hostport.substr(colon + 1)));
            } else {
                g_admin_host = hostport;
            }
        } else if (args[i] == "--json") {
            g_json_output = true;
        } else if (args[i] == "--verbose") {
            g_verbose = true;
        } else if (args[i] == "--help" || args[i] == "-h") {
            print_usage();
            return 0;
        } else {
            cmd_args.push_back(args[i]);
        }
    }

    if (cmd_args.empty()) {
        print_usage();
        return 1;
    }

    // Match subcommand
    std::string subcommand;
    std::vector<std::string> sub_args;

    // Try two-word commands first (e.g. "topic create")
    if (cmd_args.size() >= 2) {
        std::string two_word = cmd_args[0] + " " + cmd_args[1];
        for (const auto& cmd : kCommands) {
            if (cmd.name == two_word) {
                subcommand = two_word;
                sub_args.assign(cmd_args.begin() + 2, cmd_args.end());
                break;
            }
        }
    }

    // Try single-word command
    if (subcommand.empty()) {
        subcommand = cmd_args[0];
        sub_args.assign(cmd_args.begin() + 1, cmd_args.end());
    }

    // Dispatch
    for (const auto& cmd : kCommands) {
        if (cmd.name == subcommand) {
            try {
                return cmd.fn(sub_args);
            } catch (const std::exception& e) {
                std::cerr << "Error: " << e.what() << "\n";
                return 1;
            }
        }
    }

    std::cerr << "Unknown command: " << subcommand << "\n";
    std::cerr << "Run 'torrentctl --help' for usage.\n";
    return 1;
}
