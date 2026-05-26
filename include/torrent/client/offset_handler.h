#pragma once
#include <memory>
#include <torrent/common/types.h>
#include <torrent/client/request_context.h>

namespace torrent::client {
class OffsetHandler {
public:
    explicit OffsetHandler(class BrokerServer& s) : server_(&s) {}
    shared_buffer handle_list_offsets(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_commit(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_fetch(const RequestContext& ctx, buffer_view body);
private:
    BrokerServer* server_;
};
}