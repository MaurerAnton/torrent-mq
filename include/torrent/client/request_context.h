#pragma once
#include <cstdint>
#include <string>
#include <torrent/common/types.h>

namespace torrent::client {
struct RequestContext {
    int16_t api_key{0};
    int16_t api_version{0};
    int32_t correlation_id{0};
    std::string client_id;
    broker_id_t broker_id{kNoBroker};
    timestamp_ms_t received_at_ms{0};
    std::string client_host;
    bool is_inter_broker{false};
    bool is_authenticated{false};
    std::string principal;
    std::vector<std::string> sasl_mechanisms;
};
}