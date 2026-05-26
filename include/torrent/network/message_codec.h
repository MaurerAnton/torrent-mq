#pragma once
#include <cstdint>
#include <vector>
#include <torrent/common/types.h>

namespace torrent::network {
class MessageCodec {
public:
    static std::vector<uint8_t> encode_request(int16_t api_key, int16_t api_version, int32_t correlation_id, const std::string& client_id, buffer_view body);
    static std::vector<uint8_t> encode_response(int32_t correlation_id, buffer_view body);
    static buffer_view decode_request_header(const uint8_t* data, size_t size, int16_t& api_key, int16_t& api_version, int32_t& correlation_id);
    static int32_t peek_correlation_id(const uint8_t* data, size_t size);
    static int32_t peek_frame_size(const uint8_t* data, size_t size);
};
}