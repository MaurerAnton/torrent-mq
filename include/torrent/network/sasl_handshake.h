#pragma once
#include <memory>
#include <string>
#include <vector>
#include <torrent/common/types.h>

namespace torrent::network {
class SaslHandshake {
public:
    SaslHandshake();
    result<std::vector<uint8_t>> initiate(const std::string& mechanism);
    result<std::vector<uint8_t>> respond(const std::vector<uint8_t>& challenge);
    bool is_complete() const noexcept;
    bool is_authenticated() const noexcept;
    std::string principal() const;
    std::string mechanism() const;
    std::vector<std::string> supported_mechanisms() const;
private:
    std::string mechanism_;
    std::string principal_;
    bool complete_{false};
    bool authenticated_{false};
};
}