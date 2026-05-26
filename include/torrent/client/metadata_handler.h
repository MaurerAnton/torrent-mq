#pragma once
#include <memory>
#include <torrent/common/types.h>
#include <torrent/client/request_context.h>

namespace torrent::client {
class MetadataHandler {
public:
    explicit MetadataHandler(class BrokerServer& s) : server_(&s) {}
    shared_buffer handle(const RequestContext& ctx, buffer_view body);
private:
    BrokerServer* server_;
};
}