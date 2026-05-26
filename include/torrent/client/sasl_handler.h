#pragma once
#include <memory>
#include <string>
#include <vector>
#include <torrent/common/types.h>
#include <torrent/client/request_context.h>

namespace torrent::client {

/// Handles SASL authentication — SaslHandshake (api_key=17) and
/// SaslAuthenticate (api_key=36) — during connection setup.
///
/// Multi-round: the handler maintains per-connection SCRAM state via
/// a session map keyed on correlation_id, enabling multi-message
/// authentication exchanges.  Supports SCRAM-SHA-256, SCRAM-SHA-512,
/// PLAIN, OAUTHBEARER, and GSSAPI.
class SaslHandler {
public:
    explicit SaslHandler(class BrokerServer& s) : server_(&s) {}

    /// Handle SaslHandshake (api_key=17): negotiate mechanisms.
    shared_buffer handle_handshake(const RequestContext& ctx, buffer_view body);

    /// Handle SaslAuthenticate (api_key=36): exchange auth tokens.
    shared_buffer handle_authenticate(const RequestContext& ctx, buffer_view body);

private:
    BrokerServer* server_;
};

} // namespace torrent::client
