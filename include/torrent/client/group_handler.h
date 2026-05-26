#pragma once
#include <memory>
#include <torrent/common/types.h>
#include <torrent/client/request_context.h>

namespace torrent::client {
class GroupHandler {
public:
    explicit GroupHandler(class BrokerServer& s) : server_(&s) {}
    shared_buffer handle_join(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_sync(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_heartbeat(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_leave(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_describe(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_list(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_delete(const RequestContext& ctx, buffer_view body);
private:
    BrokerServer* server_;
};
}