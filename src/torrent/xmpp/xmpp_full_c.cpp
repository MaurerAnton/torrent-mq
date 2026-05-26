#include <string>
#include <vector>
#include <memory>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

namespace torrent::xmpp {
namespace {

using json = nlohmann::json;

// ============================================================================
// XEP-0030: Service Discovery
// ============================================================================

class ServiceDiscovery {
public:
    struct Identity {
        std::string category;
        std::string type;
        std::string name;
    };

    struct Feature {
        std::string var;
    };

    void add_identity(const std::string& category, const std::string& type,
                      const std::string& name = "") {
        identities_.push_back({category, type, name});
    }

    void add_feature(const std::string& var) {
        features_.push_back({var});
    }

    std::string handle_disco_info(const std::string& to) {
        json response;
        response["type"] = "result";
        response["query"]["xmlns"] = "http://jabber.org/protocol/disco#info";

        for (const auto& id : identities_) {
            json jid;
            jid["category"] = id.category;
            jid["type"] = id.type;
            if (!id.name.empty()) jid["name"] = id.name;
            response["query"]["identity"].push_back(jid);
        }

        for (const auto& feat : features_) {
            json jf;
            jf["var"] = feat.var;
            response["query"]["feature"].push_back(jf);
        }
        return response.dump();
    }

    std::string handle_disco_items(const std::string& node) {
        json response;
        response["type"] = "result";
        response["query"]["xmlns"] = "http://jabber.org/protocol/disco#items";
        if (!node.empty()) {
            response["query"]["node"] = node;
        }
        return response.dump();
    }

    static constexpr const char* kDiscoInfoXmlns =
        "http://jabber.org/protocol/disco#info";
    static constexpr const char* kDiscoItemsXmlns =
        "http://jabber.org/protocol/disco#items";

private:
    std::vector<Identity> identities_;
    std::vector<Feature> features_;
};

// ============================================================================
// XEP-0045: Multi-User Chat (MUC)
// ============================================================================

class MucManager {
public:
    enum class Affiliation {
        OWNER,
        ADMIN,
        MEMBER,
        OUTCAST,
        NONE
    };

    enum class Role {
        MODERATOR,
        PARTICIPANT,
        VISITOR,
        NONE
    };

    struct MucRoom {
        std::string jid;
        std::string name;
        std::string subject;
        std::vector<std::string> members;
        bool is_public{true};
        bool is_persistent{true};
        bool is_members_only{false};
        bool is_moderated{false};
        bool is_non_anonymous{false};
        int max_users{0};
    };

    struct MucOccupant {
        std::string jid;
        std::string nick;
        Role role{Role::PARTICIPANT};
        Affiliation affiliation{Affiliation::MEMBER};
    };

    std::string create_room(const std::string& room_jid, const std::string& creator) {
        MucRoom room;
        room.jid = room_jid;
        room.members.push_back(creator);
        rooms_[room_jid] = room;
        spdlog::info("MUC room created: {}", room_jid);
        return room_jid;
    }

    bool join_room(const std::string& room_jid, const std::string& nick,
                   const std::string& jid) {
        auto it = rooms_.find(room_jid);
        if (it == rooms_.end()) return false;

        MucOccupant occupant;
        occupant.jid = jid;
        occupant.nick = nick;
        occupants_[room_jid].push_back(occupant);

        it->second.members.push_back(jid);
        return true;
    }

    bool leave_room(const std::string& room_jid, const std::string& jid) {
        auto it = occupants_.find(room_jid);
        if (it == occupants_.end()) return false;

        auto& occs = it->second;
        occs.erase(std::remove_if(occs.begin(), occs.end(),
                                   [&](const MucOccupant& o) {
                                       return o.jid == jid;
                                   }),
                    occs.end());
        return true;
    }

    void send_message(const std::string& room_jid, const std::string& from,
                       const std::string& body) {
        spdlog::info("MUC message in {} from {}: {}", room_jid, from, body);
    }

    void set_subject(const std::string& room_jid, const std::string& subject) {
        auto it = rooms_.find(room_jid);
        if (it != rooms_.end()) {
            it->second.subject = subject;
        }
    }

    std::string get_room_config(const std::string& room_jid) {
        json config;
        auto it = rooms_.find(room_jid);
        if (it != rooms_.end()) {
            config["muc#roomconfig_roomname"] = it->second.name;
            config["muc#roomconfig_publicroom"] = it->second.is_public;
            config["muc#roomconfig_persistentroom"] = it->second.is_persistent;
            config["muc#roomconfig_membersonly"] = it->second.is_members_only;
            config["muc#roomconfig_moderatedroom"] = it->second.is_moderated;
        }
        return config.dump();
    }

private:
    std::unordered_map<std::string, MucRoom> rooms_;
    std::unordered_map<std::string, std::vector<MucOccupant>> occupants_;
};

// ============================================================================
// XEP-0060: Publish-Subscribe (PubSub)
// ============================================================================

class PubSubManager {
public:
    struct PubSubNode {
        std::string node_id;
        std::string title;
        std::string description;
        std::string access_model{"open"};  // open, presence, roster, whitelist
        int max_items{100};
    };

    struct PubSubItem {
        std::string id;
        std::string publisher;
        std::string payload;
        int64_t publish_time_ms;
    };

    std::string create_node(const std::string& node_id,
                             const std::string& creator) {
        PubSubNode node;
        node.node_id = node_id;
        nodes_[node_id] = node;
        spdlog::info("PubSub node created: {}", node_id);
        return node_id;
    }

    bool delete_node(const std::string& node_id) {
        nodes_.erase(node_id);
        items_.erase(node_id);
        subscriptions_.erase(node_id);
        return true;
    }

    std::string publish(const std::string& node_id,
                         const std::string& publisher,
                         const std::string& payload,
                         const std::string& item_id = "") {
        auto& node_items = items_[node_id];
        PubSubItem item;
        item.id = item_id.empty() ? generate_item_id() : item_id;
        item.publisher = publisher;
        item.payload = payload;
        item.publish_time_ms = now_ms();
        node_items.push_back(item);

        // Notify subscribers
        notify_subscribers(node_id, item);
        return item.id;
    }

    std::vector<PubSubItem> get_items(const std::string& node_id,
                                        int max_items = 10) {
        auto it = items_.find(node_id);
        if (it == items_.end()) return {};

        auto& node_items = it->second;
        std::vector<PubSubItem> result;
        int count = 0;
        for (auto ri = node_items.rbegin();
             ri != node_items.rend() && count < max_items; ri++, count++) {
            result.push_back(*ri);
        }
        std::reverse(result.begin(), result.end());
        return result;
    }

    bool subscribe(const std::string& node_id, const std::string& jid) {
        subscriptions_[node_id].push_back(jid);
        spdlog::info("PubSub: {} subscribed to {}", jid, node_id);
        return true;
    }

    bool unsubscribe(const std::string& node_id, const std::string& jid) {
        auto& subs = subscriptions_[node_id];
        subs.erase(std::remove(subs.begin(), subs.end(), jid), subs.end());
        return true;
    }

private:
    void notify_subscribers(const std::string& node_id, const PubSubItem& item) {
        auto it = subscriptions_.find(node_id);
        if (it == subscriptions_.end()) return;
        for (const auto& sub : it->second) {
            spdlog::debug("PubSub: notifying {} of new item in {}", sub, node_id);
        }
    }

    std::string generate_item_id() {
        static std::atomic<int64_t> counter{0};
        return "item_" + std::to_string(counter.fetch_add(1));
    }

    static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    std::unordered_map<std::string, PubSubNode> nodes_;
    std::unordered_map<std::string, std::vector<PubSubItem>> items_;
    std::unordered_map<std::string, std::vector<std::string>> subscriptions_;
};

// ============================================================================
// XEP-0198: Stream Management
// ============================================================================

class StreamManagement {
public:
    struct StreamState {
        std::string stream_id;
        uint32_t inbound_count{0};
        uint32_t outbound_count{0};
        bool is_resumed{false};
        std::string resume_token;
        std::chrono::steady_clock::time_point last_active;
    };

    void enable(const std::string& stream_id) {
        StreamState state;
        state.stream_id = stream_id;
        state.last_active = std::chrono::steady_clock::now();
        streams_[stream_id] = state;
        spdlog::info("Stream management enabled for {}", stream_id);
    }

    bool request_ack(const std::string& stream_id, uint32_t& outbound_count) {
        auto it = streams_.find(stream_id);
        if (it == streams_.end()) return false;
        outbound_count = it->second.outbound_count;
        return true;
    }

    bool ack(const std::string& stream_id, uint32_t handled_count) {
        auto it = streams_.find(stream_id);
        if (it == streams_.end()) return false;
        it->second.inbound_count = std::max(it->second.inbound_count, handled_count);
        it->second.last_active = std::chrono::steady_clock::now();
        return true;
    }

    void increment_outbound(const std::string& stream_id) {
        auto it = streams_.find(stream_id);
        if (it != streams_.end()) it->second.outbound_count++;
    }

    bool request_resume(const std::string& stream_id,
                         uint32_t h,
                         const std::string& prev_id) {
        auto it = streams_.find(prev_id);
        if (it == streams_.end()) return false;
        it->second.is_resumed = true;
        streams_[stream_id] = it->second;
        streams_.erase(prev_id);
        spdlog::info("Stream {} resumed as {}", prev_id, stream_id);
        return true;
    }

    void cleanup_expired(std::chrono::seconds timeout = std::chrono::seconds(300)) {
        auto now = std::chrono::steady_clock::now();
        for (auto it = streams_.begin(); it != streams_.end();) {
            if (now - it->second.last_active > timeout) {
                it = streams_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    std::unordered_map<std::string, StreamState> streams_;
};

// ============================================================================
// XEP-0280: Message Carbons
// ============================================================================

class MessageCarbons {
public:
    void enable(const std::string& jid) {
        enabled_.insert(jid);
        spdlog::info("Message carbons enabled for {}", jid);
    }

    void disable(const std::string& jid) {
        enabled_.erase(jid);
    }

    bool is_enabled(const std::string& jid) const {
        return enabled_.find(jid) != enabled_.end();
    }

    void send_carbon(const std::string& from, const std::string& to,
                      const std::string& direction,  // "sent" or "received"
                      const std::string& message_xml) {
        if (!is_enabled(from)) return;
        spdlog::debug("Carbon copy ({}) from {} to {}: {}",
                     direction, from, to, message_xml);
    }

private:
    std::unordered_set<std::string> enabled_;
};

// ============================================================================
// XEP-0313: Message Archive Management (MAM)
// ============================================================================

class MessageArchiveManager {
public:
    struct ArchivedMessage {
        std::string id;
        std::string from;
        std::string to;
        std::string body;
        int64_t timestamp_ms;
    };

    void archive(const std::string& jid, const ArchivedMessage& msg) {
        auto& archive = archives_[jid];
        archive.push_back(msg);
        // Cleanup old messages beyond limit
        if (archive.size() > kMaxArchiveSize) {
            archive.erase(archive.begin(),
                          archive.begin() + (archive.size() - kMaxArchiveSize));
        }
    }

    std::vector<ArchivedMessage> query(const std::string& jid,
                                         const std::string& after_id = "",
                                         int limit = 50) {
        auto it = archives_.find(jid);
        if (it == archives_.end()) return {};

        std::vector<ArchivedMessage> result;
        bool found_after = after_id.empty();

        for (const auto& msg : it->second) {
            if (!found_after) {
                if (msg.id == after_id) found_after = true;
                continue;
            }
            result.push_back(msg);
            if (static_cast<int>(result.size()) >= limit) break;
        }
        return result;
    }

private:
    static constexpr size_t kMaxArchiveSize = 10000;
    std::unordered_map<std::string, std::vector<ArchivedMessage>> archives_;
};

} // anonymous namespace
} // namespace torrent::xmpp
