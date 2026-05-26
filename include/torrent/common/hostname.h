#pragma once

#include <string>
#include <vector>

namespace torrent {

/**
 * Get the short hostname of this machine (without domain).
 */
std::string get_hostname();

/**
 * Get the fully qualified domain name of this machine.
 */
std::string get_fqdn();

/**
 * Resolve a hostname to a list of IP addresses.
 * Uses DNS resolution via getaddrinfo.
 * Returns empty vector on failure.
 */
std::vector<std::string> resolve_hostname(const std::string& host);

/**
 * Resolve a hostname and return the first IP address.
 * Returns the original host string on failure.
 */
std::string resolve_hostname_first(const std::string& host);

/**
 * Check if a string is an IP address (IPv4 or IPv6).
 */
bool is_ip_address(const std::string& host) noexcept;

} // namespace torrent
