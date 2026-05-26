#include "torrent/common/hostname.h"
#include <unistd.h>
#include <climits>
#include <cstring>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>

namespace torrent {

std::string get_hostname() {
    char hostname[HOST_NAME_MAX + 1] = {0};
    if (::gethostname(hostname, sizeof(hostname)) != 0) {
        return "localhost";
    }
    hostname[HOST_NAME_MAX] = '\0';

    // Strip domain suffix if present
    char* dot = std::strchr(hostname, '.');
    if (dot != nullptr) {
        *dot = '\0';
    }

    return std::string(hostname);
}

std::string get_fqdn() {
    char hostname[HOST_NAME_MAX + 1] = {0};
    if (::gethostname(hostname, sizeof(hostname)) != 0) {
        return "localhost.localdomain";
    }
    hostname[HOST_NAME_MAX] = '\0';

    struct addrinfo hints = {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_flags = AI_CANONNAME;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* result = nullptr;
    int ret = ::getaddrinfo(hostname, nullptr, &hints, &result);

    if (ret == 0 && result != nullptr && result->ai_canonname != nullptr) {
        std::string fqdn(result->ai_canonname);
        ::freeaddrinfo(result);
        return fqdn;
    }

    if (result != nullptr) {
        ::freeaddrinfo(result);
    }

    // Fallback: try with localdomain suffix
    std::string with_domain = std::string(hostname) + ".localdomain";

    hints.ai_flags = 0;
    ret = ::getaddrinfo(with_domain.c_str(), nullptr, &hints, &result);

    if (ret == 0 && result != nullptr) {
        ::freeaddrinfo(result);
        return with_domain;
    }

    if (result != nullptr) {
        ::freeaddrinfo(result);
    }

    // Return just hostname if FQDN resolution fails
    std::string hn(hostname);
    return hn;
}

std::vector<std::string> resolve_hostname(const std::string& host) {
    std::vector<std::string> addresses;

    struct addrinfo hints = {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_ADDRCONFIG;

    // Check if it's already an IP address (skip DNS lookup)
    struct in_addr ipv4_addr;
    struct in6_addr ipv6_addr;

    if (::inet_pton(AF_INET, host.c_str(), &ipv4_addr) == 1) {
        addresses.push_back(host);
        return addresses;
    }

    if (::inet_pton(AF_INET6, host.c_str(), &ipv6_addr) == 1) {
        // Strip brackets if present
        std::string clean = host;
        if (!clean.empty() && clean.front() == '[' && clean.back() == ']') {
            clean = clean.substr(1, clean.size() - 2);
        }
        addresses.push_back(clean);
        return addresses;
    }

    struct addrinfo* result = nullptr;
    int ret = ::getaddrinfo(host.c_str(), nullptr, &hints, &result);

    if (ret != 0) {
        return addresses;
    }

    char ip_str[INET6_ADDRSTRLEN];
    for (struct addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
        void* addr = nullptr;

        if (rp->ai_family == AF_INET) {
            auto* ipv4 = reinterpret_cast<struct sockaddr_in*>(rp->ai_addr);
            addr = &ipv4->sin_addr;
        } else if (rp->ai_family == AF_INET6) {
            auto* ipv6 = reinterpret_cast<struct sockaddr_in6*>(rp->ai_addr);
            addr = &ipv6->sin6_addr;
        } else {
            continue;
        }

        const char* ip = ::inet_ntop(rp->ai_family, addr, ip_str, sizeof(ip_str));
        if (ip != nullptr) {
            // For IPv6, wrap in brackets
            if (rp->ai_family == AF_INET6) {
                addresses.push_back("[" + std::string(ip) + "]");
            } else {
                addresses.push_back(std::string(ip));
            }
        }
    }

    ::freeaddrinfo(result);
    return addresses;
}

std::string resolve_hostname_first(const std::string& host) {
    auto addrs = resolve_hostname(host);
    if (addrs.empty()) {
        return host; // Return original on failure
    }
    return addrs.front();
}

bool is_ip_address(const std::string& host) noexcept {
    struct in_addr ipv4_addr;
    struct in6_addr ipv6_addr;

    if (::inet_pton(AF_INET, host.c_str(), &ipv4_addr) == 1) {
        return true;
    }

    if (::inet_pton(AF_INET6, host.c_str(), &ipv6_addr) == 1) {
        return true;
    }

    // Check bracketed IPv6 like [::1]
    if (!host.empty() && host.front() == '[' && host.back() == ']') {
        std::string inner = host.substr(1, host.size() - 2);
        return ::inet_pton(AF_INET6, inner.c_str(), &ipv6_addr) == 1;
    }

    return false;
}

} // namespace torrent
