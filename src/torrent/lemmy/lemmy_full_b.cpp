#include <string>
#include <vector>
#include <memory>
#include <algorithm>
#include <cmath>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

namespace torrent::lemmy {
namespace {

using json = nlohmann::json;

// ============================================================================
// ActivityPub Inbox/Outbox Processing
// ============================================================================

class ActivityPubProcessor {
public:
    struct Activity {
        std::string id;
        std::string type;  // Create, Update, Delete, Follow, Accept, Reject, Like, Announce, Undo
        std::string actor;
        std::string object;
        std::string target;
        std::string to;
        std::string cc;
        int64_t published_ms;
        json raw;
    };

    void process_inbox(const Activity& activity, const std::string& inbox_owner) {
        spdlog::info("Processing inbox activity {} type={} for {}",
                     activity.id, activity.type, inbox_owner);

        if (activity.type == "Create") {
            handle_create(activity);
        } else if (activity.type == "Update") {
            handle_update(activity);
        } else if (activity.type == "Delete") {
            handle_delete(activity);
        } else if (activity.type == "Follow") {
            handle_follow(activity);
        } else if (activity.type == "Accept") {
            handle_accept(activity);
        } else if (activity.type == "Reject") {
            handle_reject(activity);
        } else if (activity.type == "Like") {
            handle_like(activity);
        } else if (activity.type == "Announce") {
            handle_announce(activity);
        } else if (activity.type == "Undo") {
            handle_undo(activity);
        }
    }

    json build_outbox_activity(const std::string& type,
                                 const std::string& actor,
                                 const std::string& object) {
        json activity;
        activity["@context"] = "https://www.w3.org/ns/activitystreams";
        activity["type"] = type;
        activity["actor"] = actor;
        activity["object"] = object;
        activity["published"] = iso8601_now();
        return activity;
    }

private:
    void handle_create(const Activity& activity) {
        spdlog::info("ActivityPub: {} created {}", activity.actor, activity.object);
    }

    void handle_update(const Activity& activity) {
        spdlog::info("ActivityPub: {} updated {}", activity.actor, activity.object);
    }

    void handle_delete(const Activity& activity) {
        spdlog::info("ActivityPub: {} deleted {}", activity.actor, activity.object);
    }

    void handle_follow(const Activity& activity) {
        // Auto-accept follows
        json accept = build_outbox_activity("Accept", activity.object, activity.id);
        spdlog::info("ActivityPub: {} followed {}, auto-accepting", activity.actor, activity.object);
    }

    void handle_accept(const Activity& activity) {
        spdlog::info("ActivityPub: accept received for {}", activity.object);
    }

    void handle_reject(const Activity& activity) {
        spdlog::info("ActivityPub: reject received for {}", activity.object);
    }

    void handle_like(const Activity& activity) {
        spdlog::info("ActivityPub: {} liked {}", activity.actor, activity.object);
    }

    void handle_announce(const Activity& activity) {
        spdlog::info("ActivityPub: {} announced {}", activity.actor, activity.object);
    }

    void handle_undo(const Activity& activity) {
        spdlog::info("ActivityPub: {} undid {}", activity.actor, activity.object);
    }

    std::string iso8601_now() {
        auto now = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        std::ostringstream oss;
        oss << std::put_time(std::gmtime(&time_t_now), "%Y-%m-%dT%H:%M:%SZ");
        return oss.str();
    }
};

// ============================================================================
// WebFinger Resolution
// ============================================================================

class WebFingerResolver {
public:
    struct WebFingerResult {
        std::string subject;
        std::vector<std::string> aliases;
        std::unordered_map<std::string, std::string> links;  // rel -> href
    };

    WebFingerResult resolve(const std::string& resource) {
        WebFingerResult result;
        result.subject = resource;

        // Extract username and domain from acct:user@domain
        std::string acct = resource;
        if (acct.find("acct:") == 0) {
            acct = acct.substr(5);
        }

        size_t at_pos = acct.find('@');
        if (at_pos != std::string::npos) {
            std::string username = acct.substr(0, at_pos);
            std::string domain = acct.substr(at_pos + 1);

            // Build ActivityPub actor URL
            std::string actor_url = "https://" + domain + "/u/" + username;
            result.links["self"] = actor_url;
            result.links["http://webfinger.net/rel/profile-page"] =
                "https://" + domain + "/u/" + username;
        }

        // Build JSON response
        json jrd;
        jrd["subject"] = result.subject;
        jrd["aliases"] = result.aliases;

        json links_array = json::array();
        for (const auto& [rel, href] : result.links) {
            json link;
            link["rel"] = rel;
            link["href"] = href;
            links_array.push_back(link);
        }
        jrd["links"] = links_array;

        spdlog::info("WebFinger resolved: {}", resource);
        return result;
    }
};

// ============================================================================
// HTTP Signatures
// ============================================================================

class HttpSignatureVerifier {
public:
    struct SignatureHeaders {
        std::string key_id;
        std::string algorithm{"rsa-sha256"};
        std::string headers{"(request-target) host date"};
        std::string signature;
    };

    bool verify(const SignatureHeaders& sig, const std::string& method,
                 const std::string& path, const std::string& host,
                 const std::string& date, const std::string& body) {
        // Build signing string
        std::string signing_string;
        signing_string += "(request-target): " + method + " " + path + "\n";
        signing_string += "host: " + host + "\n";
        signing_string += "date: " + date;

        spdlog::debug("HTTP Signature verification for key={}", sig.key_id);
        // In production: verify RSA-SHA256 signature against actor's public key
        return true;  // Stub: always valid in dev
    }

    SignatureHeaders build_signature(const std::string& key_id,
                                       const std::string& private_key_pem,
                                       const std::string& method,
                                       const std::string& path,
                                       const std::string& host) {
        SignatureHeaders sig;
        sig.key_id = key_id;
        sig.algorithm = "rsa-sha256";
        sig.headers = "(request-target) host date";

        std::string signing_string;
        signing_string += "(request-target): " + method + " " + path + "\n";
        signing_string += "host: " + host + "\n";
        signing_string += "date: " + http_date();

        // In production: sign with RSA-SHA256
        sig.signature = "base64_encoded_signature";
        return sig;
    }

    std::string build_signature_header(const SignatureHeaders& sig) {
        std::ostringstream oss;
        oss << "keyId=\"" << sig.key_id << "\",";
        oss << "algorithm=\"" << sig.algorithm << "\",";
        oss << "headers=\"" << sig.headers << "\",";
        oss << "signature=\"" << sig.signature << "\"";
        return oss.str();
    }

private:
    std::string http_date() {
        auto now = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        std::ostringstream oss;
        oss << std::put_time(std::gmtime(&time_t_now),
                            "%a, %d %b %Y %H:%M:%S GMT");
        return oss.str();
    }
};

// ============================================================================
// Vote Ranking Algorithms
// ============================================================================

class VoteRanker {
public:
    struct Post {
        int64_t id;
        int upvotes{0};
        int downvotes{0};
        int64_t created_ts_ms;
        int64_t last_activity_ts_ms;
        int comment_count{0};
    };

    // Hot ranking (Reddit-style): time-decayed score
    double hot_rank(const Post& post, int64_t now_ms) {
        int score = post.upvotes - post.downvotes;
        double order = std::log10(std::max(std::abs(score), 1));
        int sign = (score > 0) ? 1 : (score < 0) ? -1 : 0;
        double seconds = (now_ms - post.created_ts_ms) / 1000.0 - 1134028003.0;
        return sign * order + seconds / 45000.0;
    }

    // Active ranking: most recent activity first
    int64_t active_rank(const Post& post) {
        return post.last_activity_ts_ms;
    }

    // Top ranking: highest score
    int top_rank(const Post& post) {
        return post.upvotes - post.downvotes;
    }

    // New ranking: newest first
    int64_t new_rank(const Post& post) {
        return post.created_ts_ms;
    }

    // Controversial: high vote count, close split
    double controversial_rank(const Post& post) {
        int up = post.upvotes;
        int down = post.downvotes;
        if (up + down == 0) return 0.0;

        double magnitude = static_cast<double>(up + down);
        double balance = 1.0 - std::abs(static_cast<double>(up - down) / magnitude);
        return magnitude * balance;
    }

    // Wilson score confidence interval (lower bound)
    double confidence_rank(const Post& post, double z = 1.96) {
        int n = post.upvotes + post.downvotes;
        if (n == 0) return 0.0;

        double p = static_cast<double>(post.upvotes) / n;
        double z2 = z * z;
        return (p + z2 / (2 * n) -
                z * std::sqrt((p * (1 - p) + z2 / (4 * n)) / n)) /
               (1 + z2 / n);
    }
};

// ============================================================================
// RSS/Atom Feed Generation
// ============================================================================

class FeedGenerator {
public:
    std::string generate_atom(const std::string& title,
                                const std::string& base_url,
                                const std::vector<std::pair<std::string, std::string>>& entries) {
        std::ostringstream oss;
        oss << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
        oss << "<feed xmlns=\"http://www.w3.org/2005/Atom\">\n";
        oss << "  <title>" << xml_escape(title) << "</title>\n";
        oss << "  <link href=\"" << base_url << "\" rel=\"self\"/>\n";
        oss << "  <updated>" << atom_date() << "</updated>\n";
        oss << "  <id>" << base_url << "</id>\n";

        for (const auto& [entry_title, entry_link] : entries) {
            oss << "  <entry>\n";
            oss << "    <title>" << xml_escape(entry_title) << "</title>\n";
            oss << "    <link href=\"" << xml_escape(entry_link) << "\"/>\n";
            oss << "    <id>" << xml_escape(entry_link) << "</id>\n";
            oss << "    <updated>" << atom_date() << "</updated>\n";
            oss << "  </entry>\n";
        }
        oss << "</feed>\n";
        return oss.str();
    }

    std::string generate_rss(const std::string& title,
                               const std::string& link,
                               const std::string& description,
                               const std::vector<std::tuple<std::string, std::string, std::string>>& items) {
        std::ostringstream oss;
        oss << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
        oss << "<rss version=\"2.0\">\n";
        oss << "  <channel>\n";
        oss << "    <title>" << xml_escape(title) << "</title>\n";
        oss << "    <link>" << xml_escape(link) << "</link>\n";
        oss << "    <description>" << xml_escape(description) << "</description>\n";

        for (const auto& [item_title, item_link, item_desc] : items) {
            oss << "    <item>\n";
            oss << "      <title>" << xml_escape(item_title) << "</title>\n";
            oss << "      <link>" << xml_escape(item_link) << "</link>\n";
            oss << "      <description>" << xml_escape(item_desc) << "</description>\n";
            oss << "    </item>\n";
        }
        oss << "  </channel>\n";
        oss << "</rss>\n";
        return oss.str();
    }

private:
    std::string xml_escape(const std::string& s) {
        std::string result;
        for (char c : s) {
            switch (c) {
            case '&': result += "&amp;"; break;
            case '<': result += "&lt;"; break;
            case '>': result += "&gt;"; break;
            case '"': result += "&quot;"; break;
            case '\'': result += "&apos;"; break;
            default: result += c;
            }
        }
        return result;
    }

    std::string atom_date() {
        auto now = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        std::ostringstream oss;
        oss << std::put_time(std::gmtime(&time_t_now), "%Y-%m-%dT%H:%M:%SZ");
        return oss.str();
    }
};

// ============================================================================
// NodeInfo 2.1 Protocol
// ============================================================================

class NodeInfoProvider {
public:
    json get_nodeinfo_21() {
        json ni;
        ni["version"] = "2.1";
        ni["software"]["name"] = "torrent-lemmy";
        ni["software"]["version"] = "0.1.0";
        ni["protocols"] = {"activitypub"};
        ni["services"]["inbound"] = json::array();
        ni["services"]["outbound"] = json::array();
        ni["openRegistrations"] = true;
        ni["usage"]["users"]["total"] = 0;
        ni["usage"]["users"]["activeMonth"] = 0;
        ni["usage"]["users"]["activeHalfyear"] = 0;
        ni["usage"]["localPosts"] = 0;
        ni["usage"]["localComments"] = 0;
        ni["metadata"] = json::object();
        return ni;
    }

    json get_nodeinfo_20() {
        json ni = get_nodeinfo_21();
        ni["version"] = "2.0";
        ni.erase("software");
        return ni;
    }
};

// ============================================================================
// Community Moderation
// ============================================================================

class ModerationManager {
public:
    enum class ModAction {
        BAN_USER,
        UNBAN_USER,
        REMOVE_POST,
        REMOVE_COMMENT,
        LOCK_POST,
        UNLOCK_POST,
        FEATURE_POST,
        UNFEATURE_POST,
        APPOINT_MODERATOR,
        REMOVE_MODERATOR
    };

    void perform_action(ModAction action, int64_t community_id,
                         int64_t target_id, int64_t moderator_id) {
        switch (action) {
        case ModAction::BAN_USER:
            banned_users_.insert(target_id);
            spdlog::info("User {} banned from community {}", target_id, community_id);
            break;
        case ModAction::UNBAN_USER:
            banned_users_.erase(target_id);
            spdlog::info("User {} unbanned from community {}", target_id, community_id);
            break;
        case ModAction::REMOVE_POST:
            removed_posts_.insert(target_id);
            spdlog::info("Post {} removed from community {}", target_id, community_id);
            break;
        case ModAction::LOCK_POST:
            locked_posts_.insert(target_id);
            spdlog::info("Post {} locked", target_id);
            break;
        case ModAction::UNLOCK_POST:
            locked_posts_.erase(target_id);
            spdlog::info("Post {} unlocked", target_id);
            break;
        case ModAction::FEATURE_POST:
            featured_posts_.insert(target_id);
            spdlog::info("Post {} featured in community {}", target_id, community_id);
            break;
        default:
            break;
        }
    }

    bool is_banned(int64_t user_id) const {
        return banned_users_.find(user_id) != banned_users_.end();
    }

    bool is_post_removed(int64_t post_id) const {
        return removed_posts_.find(post_id) != removed_posts_.end();
    }

    bool is_post_locked(int64_t post_id) const {
        return locked_posts_.find(post_id) != locked_posts_.end();
    }

private:
    std::unordered_set<int64_t> banned_users_;
    std::unordered_set<int64_t> removed_posts_;
    std::unordered_set<int64_t> locked_posts_;
    std::unordered_set<int64_t> featured_posts_;
};

} // anonymous namespace
} // namespace torrent::lemmy
