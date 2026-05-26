#pragma once
#include <memory>
#include <torrent/common/types.h>
#include <torrent/client/request_context.h>

namespace torrent::client {
class AdminHandler {
public:
    explicit AdminHandler(class BrokerServer& s) : server_(&s) {}
    shared_buffer handle_create_topics(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_delete_topics(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_create_partitions(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_describe_configs(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_alter_configs(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_create_acls(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_describe_acls(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_delete_acls(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_elect_leaders(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_alter_partition_reassignments(const RequestContext& ctx, buffer_view body);
    shared_buffer handle_list_partition_reassignments(const RequestContext& ctx, buffer_view body);
private:
    BrokerServer* server_;
};
}