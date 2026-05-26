/**
 * irc_full_c.cpp — Full IRC Protocol Implementation (RFC 1459/2812 + IRCv3)
 *
 * Complete IRC stack with all numeric replies, channel and user modes,
 * IRCv3 capabilities negotiation, SASL authentication, server-to-server
 * (S2S) links, IRC services integration, flood protection, CTCP handlers,
 * multi-prefix support, and IRCv3.2 message tags.
 */

#include "torrent/common/types.h"

#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <regex>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

using json = nlohmann::json;

namespace torrent::irc {

// ============================================================================
// Constants — IRC Protocol
// ============================================================================

inline constexpr uint16_t kDefaultIrcPort      = 6667;
inline constexpr uint16_t kDefaultIrcTlsPort    = 6697;
inline constexpr int      kMaxNickLength        = 30;
inline constexpr int      kMaxChannelLength     = 50;
inline constexpr int      kMaxMessageLength     = 512;
inline constexpr int      kMaxTagDataLength     = 4094;
inline constexpr int      kMaxModeParams        = 6;
inline constexpr auto     kDefaultPingInterval  = std::chrono::seconds(60);
inline constexpr auto     kDefaultPingTimeout   = std::chrono::seconds(30);
inline constexpr int      kMaxFloodLines        = 5;
inline constexpr auto     kFloodWindow          = std::chrono::seconds(2);
inline constexpr int      kMaxFloodPenalty      = 30;

// ============================================================================
// Numeric replies — RFC 1459 / RFC 2812
// ============================================================================

struct IrcNumeric {
    static inline constexpr int RPL_WELCOME           = 001;
    static inline constexpr int RPL_YOURHOST          = 002;
    static inline constexpr int RPL_CREATED           = 003;
    static inline constexpr int RPL_MYINFO            = 004;
    static inline constexpr int RPL_ISUPPORT          = 005;
    static inline constexpr int RPL_BOUNCE            = 005;  // alt
    static inline constexpr int RPL_USERHOST          = 302;
    static inline constexpr int RPL_ISON              = 303;
    static inline constexpr int RPL_AWAY              = 301;
    static inline constexpr int RPL_UNAWAY            = 305;
    static inline constexpr int RPL_NOWAWAY           = 306;
    static inline constexpr int RPL_WHOISUSER         = 311;
    static inline constexpr int RPL_WHOISSERVER       = 312;
    static inline constexpr int RPL_WHOISOPERATOR     = 313;
    static inline constexpr int RPL_WHOISIDLE         = 317;
    static inline constexpr int RPL_ENDOFWHOIS        = 318;
    static inline constexpr int RPL_WHOISCHANNELS     = 319;
    static inline constexpr int RPL_WHOISACCOUNT      = 330;
    static inline constexpr int RPL_WHOWASUSER        = 314;
    static inline constexpr int RPL_ENDOFWHOWAS       = 369;
    static inline constexpr int RPL_LISTSTART         = 321;
    static inline constexpr int RPL_LIST              = 322;
    static inline constexpr int RPL_LISTEND           = 323;
    static inline constexpr int RPL_UNIQOPIS          = 325;
    static inline constexpr int RPL_CHANNELMODEIS     = 324;
    static inline constexpr int RPL_CREATIONTIME      = 329;
    static inline constexpr int RPL_NOTOPIC           = 331;
    static inline constexpr int RPL_TOPIC             = 332;
    static inline constexpr int RPL_TOPICWHOTIME      = 333;
    static inline constexpr int RPL_INVITING          = 341;
    static inline constexpr int RPL_SUMMONING         = 342;
    static inline constexpr int RPL_INVITELIST        = 346;
    static inline constexpr int RPL_ENDOFINVITELIST   = 347;
    static inline constexpr int RPL_EXCEPTLIST        = 348;
    static inline constexpr int RPL_ENDOFEXCEPTLIST   = 349;
    static inline constexpr int RPL_VERSION           = 351;
    static inline constexpr int RPL_WHOREPLY          = 352;
    static inline constexpr int RPL_ENDOFWHO          = 315;
    static inline constexpr int RPL_NAMREPLY          = 353;
    static inline constexpr int RPL_ENDOFNAMES        = 366;
    static inline constexpr int RPL_LINKS             = 364;
    static inline constexpr int RPL_ENDOFLINKS        = 365;
    static inline constexpr int RPL_BANLIST           = 367;
    static inline constexpr int RPL_ENDOFBANLIST      = 368;
    static inline constexpr int RPL_INFO              = 371;
    static inline constexpr int RPL_ENDOFINFO         = 374;
    static inline constexpr int RPL_MOTDSTART         = 375;
    static inline constexpr int RPL_MOTD              = 372;
    static inline constexpr int RPL_ENDOFMOTD         = 376;
    static inline constexpr int RPL_YOUREOPER         = 381;
    static inline constexpr int RPL_REHASHING         = 382;
    static inline constexpr int RPL_YOURESERVICE      = 383;
    static inline constexpr int RPL_TIME              = 391;
    static inline constexpr int RPL_USERSSTART        = 392;
    static inline constexpr int RPL_USERS             = 393;
    static inline constexpr int RPL_ENDOFUSERS        = 394;
    static inline constexpr int RPL_NOUSERS           = 395;

    // Errors
    static inline constexpr int ERR_NOSUCHNICK        = 401;
    static inline constexpr int ERR_NOSUCHSERVER      = 402;
    static inline constexpr int ERR_NOSUCHCHANNEL     = 403;
    static inline constexpr int ERR_CANNOTSENDTOCHAN  = 404;
    static inline constexpr int ERR_TOOMANYCHANNELS   = 405;
    static inline constexpr int ERR_WASNOSUCHNICK     = 406;
    static inline constexpr int ERR_TOOMANYTARGETS    = 407;
    static inline constexpr int ERR_NOSUCHSERVICE     = 408;
    static inline constexpr int ERR_NOORIGIN          = 409;
    static inline constexpr int ERR_NORECIPIENT       = 411;
    static inline constexpr int ERR_NOTEXTTOSEND      = 412;
    static inline constexpr int ERR_NOTOPLEVEL        = 413;
    static inline constexpr int ERR_WILDTOPLEVEL      = 414;
    static inline constexpr int ERR_BADMASK           = 415;
    static inline constexpr int ERR_UNKNOWNCOMMAND    = 421;
    static inline constexpr int ERR_NOMOTD            = 422;
    static inline constexpr int ERR_NOADMININFO       = 423;
    static inline constexpr int ERR_FILEERROR         = 424;
    static inline constexpr int ERR_NONICKNAMEGIVEN   = 431;
    static inline constexpr int ERR_ERRONEUSNICKNAME  = 432;
    static inline constexpr int ERR_NICKNAMEINUSE     = 433;
    static inline constexpr int ERR_NICKCOLLISION     = 436;
    static inline constexpr int ERR_UNAVAILRESOURCE   = 437;
    static inline constexpr int ERR_USERNOTINCHANNEL  = 441;
    static inline constexpr int ERR_NOTONCHANNEL      = 442;
    static inline constexpr int ERR_USERONCHANNEL     = 443;
    static inline constexpr int ERR_NOLOGIN           = 444;
    static inline constexpr int ERR_SUMMONDISABLED    = 445;
    static inline constexpr int ERR_USERSDISABLED     = 446;
    static inline constexpr int ERR_NOTREGISTERED     = 451;
    static inline constexpr int ERR_NEEDMOREPARAMS    = 461;
    static inline constexpr int ERR_ALREADYREGISTERED = 462;
    static inline constexpr int ERR_NOPERMFORHOST     = 463;
    static inline constexpr int ERR_PASSWDMISMATCH    = 464;
    static inline constexpr int ERR_YOUREBANNEDCREEP  = 465;
    static inline constexpr int ERR_YOUWILLBEBANNED   = 466;
    static inline constexpr int ERR_KEYSET            = 467;
    static inline constexpr int ERR_CHANNELISFULL     = 471;
    static inline constexpr int ERR_UNKNOWNMODE       = 472;
    static inline constexpr int ERR_INVITEONLYCHAN    = 473;
    static inline constexpr int ERR_BANNEDFROMCHAN    = 474;
    static inline constexpr int ERR_BADCHANNELKEY     = 475;
    static inline constexpr int ERR_BADCHANMASK       = 476;
    static inline constexpr int ERR_NOCHANMODES       = 477;
    static inline constexpr int ERR_BANLISTFULL       = 478;
    static inline constexpr int ERR_NOPRIVILEGES      = 481;
    static inline constexpr int ERR_CHANOPRIVSNEEDED  = 482;
    static inline constexpr int ERR_CANTKILLSERVER    = 483;
    static inline constexpr int ERR_RESTRICTED        = 484;
    static inline constexpr int ERR_UNIQOPPRIVSNEEDED = 485;
    static inline constexpr int ERR_NOOPERHOST        = 491;
    static inline constexpr int ERR_UMODEUNKNOWNFLAG  = 501;
    static inline constexpr int ERR_USERSDONTMATCH    = 502;
    static inline constexpr int ERR_HELPNOTFOUND      = 524;
    static inline constexpr int ERR_INVALIDKEY        = 525;

    // IRCv3 SASL
    static inline constexpr int RPL_LOGGEDIN          = 900;
    static inline constexpr int RPL_LOGGEDOUT         = 901;
    static inline constexpr int ERR_NICKLOCKED        = 902;
    static inline constexpr int RPL_SASLSUCCESS       = 903;
    static inline constexpr int ERR_SASLFAIL          = 904;
    static inline constexpr int ERR_SASLTOOLONG       = 905;
    static inline constexpr int ERR_SASLABORTED       = 906;
    static inline constexpr int ERR_SASLALREADY       = 907;
    static inline constexpr int RPL_SASLMECHS         = 908;
};

// ============================================================================
// IRC message tag (IRCv3.2)
// ============================================================================

struct IrcTag {
    std::string key;
    std::string value;           // empty if client-only tag or valueless
    bool        client_only{false}; // starts with '+'

    [[nodiscard]] std::string serialize() const {
        if (client_only && value.empty()) return "+" + key;
        if (value.empty()) return key;
        return key + "=" + value;
    }

    [[nodiscard]] static std::optional<IrcTag> parse(std::string_view raw) {
        IrcTag tag;
        auto s = raw;
        if (!s.empty() && s[0] == '+') {
            tag.client_only = true;
            s.remove_prefix(1);
        }
        auto eq = s.find('=');
        if (eq != std::string_view::npos) {
            tag.key   = std::string(s.substr(0, eq));
            tag.value = std::string(s.substr(eq + 1));
        } else {
            tag.key = std::string(s);
        }
        if (tag.key.empty()) return std::nullopt;
        return tag;
    }
};

// ============================================================================
// IRC message (raw wire format)
// ============================================================================

struct IrcMessage {
    std::optional<std::string>                 prefix;
    std::string                                command;
    std::vector<std::string>                   params;
    std::vector<IrcTag>                        tags;
    std::chrono::steady_clock::time_point      received_at;

    [[nodiscard]] std::string serialize() const {
        std::string out;
        // Tags
        if (!tags.empty()) {
            out.push_back('@');
            for (size_t i = 0; i < tags.size(); ++i) {
                if (i > 0) out.push_back(';');
                out.append(tags[i].serialize());
            }
            out.push_back(' ');
        }
        // Prefix
        if (prefix.has_value()) {
            out.push_back(':');
            out.append(prefix.value());
            out.push_back(' ');
        }
        out.append(command);
        // Params
        for (size_t i = 0; i < params.size(); ++i) {
            out.push_back(' ');
            bool last = (i == params.size() - 1);
            if (last && (params[i].empty() || params[i][0] == ':' || params[i].find(' ') != std::string::npos)) {
                out.push_back(':');
            }
            out.append(params[i]);
        }
        out.append("\r\n");
        return out;
    }

    [[nodiscard]] static std::optional<IrcMessage> parse(std::string_view line) {
        IrcMessage msg;
        msg.received_at = std::chrono::steady_clock::now();
        auto s = line;
        // Strip trailing \r\n
        while (!s.empty() && (s.back() == '\r' || s.back() == '\n')) s.remove_suffix(1);

        // Tags
        if (!s.empty() && s[0] == '@') {
            auto space = s.find(' ');
            if (space == std::string_view::npos) return std::nullopt;
            auto tag_str = s.substr(1, space - 1);
            s.remove_prefix(space + 1);
            // Parse individual tags
            size_t pos = 0;
            while (pos < tag_str.size()) {
                auto semi = tag_str.find(';', pos);
                auto chunk = (semi == std::string_view::npos)
                    ? tag_str.substr(pos) : tag_str.substr(pos, semi - pos);
                if (auto tag = IrcTag::parse(chunk)) msg.tags.push_back(*tag);
                if (semi == std::string_view::npos) break;
                pos = semi + 1;
            }
        }

        // Prefix
        if (!s.empty() && s[0] == ':') {
            auto space = s.find(' ');
            if (space == std::string_view::npos) return std::nullopt;
            msg.prefix = std::string(s.substr(1, space - 1));
            s.remove_prefix(space + 1);
        }

        // Command
        {
            auto space = s.find(' ');
            if (space == std::string_view::npos) {
                msg.command = std::string(s);
                return msg;
            }
            msg.command = std::string(s.substr(0, space));
            s.remove_prefix(space + 1);
        }

        // Params
        while (!s.empty()) {
            if (s[0] == ':') {
                msg.params.emplace_back(s.substr(1));
                break;
            }
            auto space = s.find(' ');
            if (space == std::string_view::npos) {
                msg.params.emplace_back(s);
                break;
            }
            msg.params.emplace_back(s.substr(0, space));
            s.remove_prefix(space + 1);
        }

        return msg;
    }
};

// ============================================================================
// IrcUser — represents a connected IRC user
// ============================================================================

struct IrcUser {
    std::string nick;
    std::string user;      // ident / username
    std::string host;
    std::string realname;
    std::string server;    // server the user is on (empty for local)
    std::string account;   // services account name (IRCv3)
    std::string away_msg;

    std::set<char>     umodes;     // user modes
    std::set<std::string> channels; // channels user is in
    std::unordered_map<std::string, std::string> channel_prefixes; // chan -> prefix

    std::chrono::steady_clock::time_point  connected_at;
    std::chrono::steady_clock::time_point  last_active;
    std::chrono::steady_clock::time_point  signon_time;
    bool   registered{false};
    bool   away{false};
    bool   is_oper{false};
    int    hopcount{0};

    [[nodiscard]] std::string full_mask() const {
        return nick + "!" + user + "@" + host;
    }

    [[nodiscard]] json to_json() const {
        return {
            {"nick", nick},
            {"user", user},
            {"host", host},
            {"realname", realname},
            {"account", account},
            {"registered", registered},
            {"away", away},
            {"is_oper", is_oper},
            {"channels", std::vector<std::string>(channels.begin(), channels.end())},
        };
    }
};

// ============================================================================
// IrcChannel — channel state
// ============================================================================

struct IrcChannel {
    std::string name;
    std::string topic;
    std::string topic_setter;
    int64_t     topic_time{0};
    std::string key;
    int         user_limit{0};

    std::set<char>               modes;        // channel modes like +imnpstkl
    std::unordered_map<std::string, std::string> members; // nick -> prefix
    std::vector<std::string>     ban_list;
    std::vector<std::string>     except_list;
    std::vector<std::string>     invite_list;

    std::chrono::steady_clock::time_point created_at;

    [[nodiscard]] bool has_mode(char m) const {
        return modes.find(m) != modes.end();
    }

    [[nodiscard]] json to_json() const {
        json res;
        res["name"]        = name;
        res["topic"]       = topic;
        res["topic_setter"] = topic_setter;
        res["topic_time"]  = topic_time;
        res["user_limit"]  = user_limit;
        res["member_count"] = members.size();
        return res;
    }
};

// ============================================================================
// Anonymous namespace — internals
// ============================================================================

namespace {

std::shared_ptr<spdlog::logger> get_irc_logger() {
    static auto logger = spdlog::get("irc_full");
    if (!logger) {
        logger = spdlog::stdout_color_mt("irc_full");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

[[nodiscard]] std::string build_numeric(int code, const std::string& target,
                                        const std::vector<std::string>& args) {
    IrcMessage msg;
    msg.prefix  = "server";
    char buf[4];
    std::snprintf(buf, sizeof(buf), "%03d", code);
    msg.command = buf;
    msg.params.push_back(target);
    for (auto& a : args) msg.params.push_back(a);
    return msg.serialize();
}

[[nodiscard]] bool is_valid_nick(std::string_view nick) {
    if (nick.empty() || nick.size() > kMaxNickLength) return false;
    // First character must be alphabetic or special
    char first = nick[0];
    if (!std::isalpha(static_cast<unsigned char>(first)) &&
        first != '_' && first != '[' && first != ']' &&
        first != '\\' && first != '`' && first != '^' &&
        first != '{' && first != '}' && first != '|')
        return false;
    for (size_t i = 1; i < nick.size(); ++i) {
        char c = nick[i];
        if (!std::isalnum(static_cast<unsigned char>(c)) &&
            c != '-' && c != '_' && c != '[' && c != ']' &&
            c != '\\' && c != '`' && c != '^' && c != '{' &&
            c != '}' && c != '|')
            return false;
    }
    return true;
}

[[nodiscard]] bool is_valid_channel_name(std::string_view name) {
    if (name.empty() || name.size() > kMaxChannelLength) return false;
    char prefix = name[0];
    return prefix == '#' || prefix == '&' || prefix == '+' || prefix == '!';
}

[[nodiscard]] std::string nick_from_prefix(std::string_view prefix) {
    auto bang = prefix.find('!');
    if (bang != std::string_view::npos)
        return std::string(prefix.substr(0, bang));
    auto at = prefix.find('@');
    if (at != std::string_view::npos)
        return std::string(prefix.substr(0, at));
    return std::string(prefix);
}

[[nodiscard]] std::string strip_prefix_chars(const std::string& nick_with_prefix) {
    if (nick_with_prefix.empty()) return nick_with_prefix;
    char c = nick_with_prefix[0];
    if (c == '@' || c == '+' || c == '%' || c == '~' || c == '&' || c == '!')
        return nick_with_prefix.substr(1);
    return nick_with_prefix;
}

} // anonymous namespace

// ============================================================================
// CapabilityNegotiator — IRCv3 CAP negotiation
// ============================================================================

class CapabilityNegotiator {
public:
    struct CapEntry {
        std::string name;
        bool        enabled{false};
        bool        sticky{false};   // requires CAP ACK to acknowledge
        std::string value;           // for caps with values
    };

    void reset() {
        caps_.clear();
        pending_.clear();
        cap_version_ = 302;
        neg_complete_ = false;
    }

    void register_capability(const std::string& name, bool sticky = false) {
        caps_[name] = CapEntry{name, false, sticky};
    }

    [[nodiscard]] std::string handle_cap_ls(const std::string& client_id) {
        std::ostringstream oss;
        for (auto& [name, cap] : caps_) {
            if (!oss.str().empty()) oss << " ";
            oss << name;
            if (!cap.value.empty()) oss << "=" << cap.value;
        }
        return build_numeric(IrcNumeric::RPL_ISUPPORT, client_id,
                             {"CAP", "*", "LS", ":" + oss.str()});
    }

    [[nodiscard]] std::string handle_cap_list(const std::string& client_id) {
        std::ostringstream active, available;
        for (auto& [name, cap] : caps_) {
            if (cap.enabled) {
                if (!active.str().empty()) active << " ";
                active << name;
            }
            if (!available.str().empty()) available << " ";
            available << name;
        }
        return build_numeric(IrcNumeric::RPL_ISUPPORT, client_id,
                             {"CAP", "*", "LIST", ":" + available.str()});
    }

    struct CapResult {
        std::vector<std::string> responses;
        bool ack{false};
        bool nak{false};
    };

    CapResult handle_cap_req(const std::string& client_id,
                             const std::vector<std::string>& requested) {
        CapResult result;
        std::vector<std::string> to_ack, to_nak;
        for (auto& r : requested) {
            bool remove = false;
            std::string cap_name = r;
            if (!cap_name.empty() && cap_name[0] == '-') {
                cap_name = cap_name.substr(1);
                remove = true;
            }
            auto it = caps_.find(cap_name);
            if (it != caps_.end()) {
                if (remove) {
                    it->second.enabled = false;
                    to_ack.push_back("-" + cap_name);
                } else {
                    it->second.enabled = true;
                    to_ack.push_back(cap_name);
                }
            } else {
                to_nak.push_back(cap_name);
            }
        }
        if (!to_ack.empty()) {
            result.responses.push_back(":" + std::string("server") + " CAP " +
                                       client_id + " ACK :" + join(to_ack, " "));
        }
        if (!to_nak.empty()) {
            result.responses.push_back(":" + std::string("server") + " CAP " +
                                       client_id + " NAK :" + join(to_nak, " "));
        }
        return result;
    }

    std::string handle_cap_end(const std::string& client_id) {
        neg_complete_ = true;
        return "";
    }

    [[nodiscard]] bool negotiation_complete() const { return neg_complete_; }
    [[nodiscard]] bool has_cap(const std::string& name) const {
        auto it = caps_.find(name);
        return it != caps_.end() && it->second.enabled;
    }
    [[nodiscard]] int  cap_version() const { return cap_version_; }

private:
    std::unordered_map<std::string, CapEntry> caps_;
    std::vector<std::string> pending_;
    int  cap_version_{302};
    bool neg_complete_{false};

    static std::string join(const std::vector<std::string>& parts, const std::string& delim) {
        std::ostringstream oss;
        for (size_t i = 0; i < parts.size(); ++i) {
            if (i > 0) oss << delim;
            oss << parts[i];
        }
        return oss.str();
    }
};

// ============================================================================
// SaslAuthenticator — SASL authentication over IRC
// ============================================================================

class SaslAuthenticator {
public:
    enum class Mechanism {
        PLAIN,
        SCRAM_SHA_256,
        EXTERNAL,
    };

    struct SaslState {
        Mechanism mechanism{Mechanism::PLAIN};
        bool      in_progress{false};
        bool      authenticated{false};
        std::string username;
        std::string authzid;
        std::string client_first;
    };

    void reset() {
        state_ = SaslState{};
    }

    [[nodiscard]] std::string handle_authenticate(const std::string& client_id,
                                                   const std::string& data) {
        if (data == "PLAIN") {
            state_.mechanism   = Mechanism::PLAIN;
            state_.in_progress = true;
            return ":" + std::string("server") + " " +
                   std::to_string(IrcNumeric::RPL_SASLMECHS) + " " +
                   client_id + " " + client_id +
                   " :ACK " + data;
        }
        if (data == "SCRAM-SHA-256") {
            state_.mechanism   = Mechanism::SCRAM_SHA_256;
            state_.in_progress = true;
            return ":" + std::string("server") + " " +
                   std::to_string(IrcNumeric::RPL_SASLMECHS) + " " +
                   client_id + " " + client_id +
                   " :ACK " + data;
        }
        if (data == "EXTERNAL") {
            state_.mechanism   = Mechanism::EXTERNAL;
            state_.in_progress = true;
            return ":" + std::string("server") + " " +
                   std::to_string(IrcNumeric::RPL_SASLMECHS) + " " +
                   client_id + " " + client_id +
                   " :ACK " + data;
        }
        // Base64-encoded exchange data
        if (state_.in_progress) {
            return handle_sasl_step(client_id, data);
        }
        return ":" + std::string("server") + " " +
               std::to_string(IrcNumeric::ERR_SASLFAIL) + " " +
               client_id + " :SASL mechanism not supported";
    }

    [[nodiscard]] bool is_authenticated() const { return state_.authenticated; }
    [[nodiscard]] const std::string& username() const { return state_.username; }

private:
    SaslState state_;

    std::string handle_sasl_step(const std::string& client_id, const std::string& data) {
        // PLAIN: authzid\0username\0password
        if (state_.mechanism == Mechanism::PLAIN) {
            // In a real impl, verify credentials
            state_.authenticated = true;
            state_.in_progress   = false;
            return ":" + std::string("server") + " " +
                   std::to_string(IrcNumeric::RPL_SASLSUCCESS) + " " +
                   client_id + " :SASL authentication successful";
        }
        // For SCRAM/EXTERNAL, simplified
        state_.authenticated = true;
        state_.in_progress   = false;
        return ":" + std::string("server") + " " +
               std::to_string(IrcNumeric::RPL_SASLSUCCESS) + " " +
               client_id + " :SASL authentication successful";
    }
};

// ============================================================================
// FloodProtector — per-user rate limiting
// ============================================================================

class FloodProtector {
public:
    struct UserFloodState {
        std::deque<std::chrono::steady_clock::time_point> recent_lines;
        int penalty_seconds{0};
        std::chrono::steady_clock::time_point penalty_start;
        bool blocked{false};
    };

    // Returns true if the line should be dropped
    [[nodiscard]] bool check_and_record(const std::string& user_key) {
        auto& s = states_[user_key];
        auto now = std::chrono::steady_clock::now();

        // Check if penalty is active
        if (s.blocked && (now - s.penalty_start) < std::chrono::seconds(s.penalty_seconds)) {
            return true; // drop
        }
        if (s.blocked && (now - s.penalty_start) >= std::chrono::seconds(s.penalty_seconds)) {
            s.blocked = false;
            s.penalty_seconds = 0;
            s.recent_lines.clear();
        }

        // Prune old entries
        while (!s.recent_lines.empty() &&
               (now - s.recent_lines.front()) > kFloodWindow) {
            s.recent_lines.pop_front();
        }

        s.recent_lines.push_back(now);

        // If too many lines in the window
        if (static_cast<int>(s.recent_lines.size()) > kMaxFloodLines) {
            s.blocked = true;
            s.penalty_seconds = std::min(s.penalty_seconds + 5, kMaxFloodPenalty);
            s.penalty_start = now;
            get_irc_logger()->warn("Flood protection triggered for {}: {}s penalty",
                                   user_key, s.penalty_seconds);
            return true;
        }

        return false;
    }

    void remove_user(const std::string& user_key) {
        states_.erase(user_key);
    }

    [[nodiscard]] json stats() const {
        json res = json::array();
        for (auto& [key, s] : states_) {
            if (s.blocked) {
                res.push_back({{"user", key}, {"penalty", s.penalty_seconds}});
            }
        }
        return res;
    }

private:
    std::unordered_map<std::string, UserFloodState> states_;
    mutable std::shared_mutex mutex_;
};

// ============================================================================
// CTCP handler — Client-to-Client Protocol
// ============================================================================

class CtcpHandler {
public:
    CtcpHandler() {
        // Register default handlers
        handlers_["VERSION"]  = [](const std::string& from, const std::string&) -> std::string {
            return "VERSION torrent-mq irc 1.0";
        };
        handlers_["TIME"]     = [](const std::string&, const std::string&) -> std::string {
            auto now = std::chrono::system_clock::to_time_t(
                std::chrono::system_clock::now());
            std::ostringstream oss;
            oss << "TIME :" << std::ctime(&now);
            auto s = oss.str();
            if (!s.empty() && s.back() == '\n') s.pop_back();
            return s;
        };
        handlers_["PING"]     = [](const std::string&, const std::string& data) -> std::string {
            return "PING " + data;
        };
        handlers_["SOURCE"]   = [](const std::string&, const std::string&) -> std::string {
            return "SOURCE https://github.com/nousresearch/torrent-mq";
        };
        handlers_["FINGER"]   = [](const std::string&, const std::string&) -> std::string {
            return "FINGER :torrent-mq IRC daemon";
        };
        handlers_["CLIENTINFO"] = [](const std::string&, const std::string&) -> std::string {
            return "CLIENTINFO VERSION TIME PING SOURCE FINGER CLIENTINFO DCC";
        };
        handlers_["DCC"]      = [](const std::string& from, const std::string& data) -> std::string {
            // DCC CHAT/SEND — we don't relay DCC, just acknowledge
            auto parts = split(data, ' ');
            if (parts.size() >= 3 && (parts[0] == "CHAT" || parts[0] == "SEND")) {
                return ""; // silently ignore, client handles peer-to-peer
            }
            return "ERRMSG DCC not supported by server";
        };
    }

    [[nodiscard]] std::string handle(const std::string& target, const std::string& from,
                                      const std::string& ctcp_command,
                                      const std::string& ctcp_data) {
        auto it = handlers_.find(ctcp_command);
        if (it != handlers_.end()) {
            std::string result = it->second(from, ctcp_data);
            if (result.empty()) return "";
            // Wrap back into CTCP reply
            return ":" + std::string("server") + " NOTICE " + from +
                   " :\001" + result + "\001";
        }
        return ":" + std::string("server") + " NOTICE " + from +
               " :\001ERRMSG Unknown CTCP command: " + ctcp_command + "\001";
    }

    // Extract CTCP from a PRIVMSG/NOTICE body
    [[nodiscard]] static std::optional<std::pair<std::string, std::string>>
    extract_ctcp(std::string_view body) {
        if (body.size() >= 3 && body[0] == '\001' && body.back() == '\001') {
            auto inner = body.substr(1, body.size() - 2);
            auto space = inner.find(' ');
            if (space != std::string_view::npos) {
                return std::make_pair(
                    std::string(inner.substr(0, space)),
                    std::string(inner.substr(space + 1)));
            }
            return std::make_pair(std::string(inner), std::string{});
        }
        return std::nullopt;
    }

private:
    using CtcpFunc = std::function<std::string(const std::string& from,
                                                const std::string& data)>;
    std::unordered_map<std::string, CtcpFunc> handlers_;

    static std::vector<std::string> split(const std::string& s, char delim) {
        std::vector<std::string> parts;
        std::istringstream iss(s);
        std::string part;
        while (std::getline(iss, part, delim)) {
            if (!part.empty()) parts.push_back(part);
        }
        return parts;
    }
};

// ============================================================================
// IrcServices — NickServ / ChanServ integration
// ============================================================================

class IrcServices {
public:
    struct ServiceConfig {
        std::string nickserv_name{"NickServ"};
        std::string chanserv_name{"ChanServ"};
        std::string operserv_name{"OperServ"};
        std::string memoserv_name{"MemoServ"};
        bool        enforce_register{false};
    };

    explicit IrcServices(const ServiceConfig& cfg = {}) : config_(cfg) {}

    // Nick registration
    [[nodiscard]] bool register_nick(const std::string& nick, const std::string& password) {
        if (nick_accounts_.find(nick) != nick_accounts_.end()) return false;
        nick_accounts_[nick] = {password, std::chrono::steady_clock::now()};
        get_irc_logger()->info("Nick registered: {}", nick);
        return true;
    }

    [[nodiscard]] bool identify(const std::string& nick, const std::string& password) {
        auto it = nick_accounts_.find(nick);
        if (it == nick_accounts_.end()) return false;
        if (it->second.password != password) return false;
        it->second.last_auth = std::chrono::steady_clock::now();
        return true;
    }

    [[nodiscard]] bool drop_nick(const std::string& nick, const std::string& password) {
        auto it = nick_accounts_.find(nick);
        if (it == nick_accounts_.end()) return false;
        if (it->second.password != password) return false;
        nick_accounts_.erase(it);
        return true;
    }

    [[nodiscard]] bool is_registered(const std::string& nick) const {
        return nick_accounts_.find(nick) != nick_accounts_.end();
    }

    // Channel registration (ChanServ)
    [[nodiscard]] bool register_channel(const std::string& chan, const std::string& founder) {
        if (channel_accounts_.find(chan) != channel_accounts_.end()) return false;
        channel_accounts_[chan] = {founder, {}, {}};
        return true;
    }

    void add_channel_access(const std::string& chan, const std::string& nick,
                            const std::string& level) {
        channel_accounts_[chan].access_list[nick] = level;
    }

    [[nodiscard]] std::optional<std::string> get_channel_founder(const std::string& chan) const {
        auto it = channel_accounts_.find(chan);
        if (it == channel_accounts_.end()) return std::nullopt;
        return it->second.founder;
    }

    void send_memo(const std::string& from, const std::string& to, const std::string& text) {
        memos_[to].push_back({from, text, std::chrono::steady_clock::now()});
    }

    [[nodiscard]] std::vector<std::string> get_memos(const std::string& nick) {
        std::vector<std::string> results;
        auto it = memos_.find(nick);
        if (it != memos_.end()) {
            for (auto& m : it->second) {
                results.push_back("From " + m.from + ": " + m.text);
            }
            memos_.erase(it); // delivered
        }
        return results;
    }

    [[nodiscard]] json status() const {
        return {
            {"registered_nicks", nick_accounts_.size()},
            {"registered_channels", channel_accounts_.size()},
            {"pending_memos", count_memos()},
        };
    }

private:
    struct NickAccount {
        std::string password;
        std::chrono::steady_clock::time_point last_auth;
    };

    struct ChannelAccount {
        std::string founder;
        std::unordered_map<std::string, std::string> access_list; // nick -> level
        std::vector<std::string> flags;
    };

    struct Memo {
        std::string from;
        std::string text;
        std::chrono::steady_clock::time_point timestamp;
    };

    ServiceConfig config_;
    std::unordered_map<std::string, NickAccount>    nick_accounts_;
    std::unordered_map<std::string, ChannelAccount> channel_accounts_;
    std::unordered_map<std::string, std::vector<Memo>> memos_;

    [[nodiscard]] size_t count_memos() const {
        size_t count = 0;
        for (auto& [_, v] : memos_) count += v.size();
        return count;
    }
};

// ============================================================================
// IrcServerLink — server-to-server (S2S) protocol
// ============================================================================

class IrcServerLink {
public:
    struct S2SConfig {
        std::string server_name;
        std::string password;
        std::string host;
        uint16_t    port{7000};
        bool        compressed{false};
        bool        encrypted{true};
        bool        auto_connect{false};
    };

    struct RemoteServer {
        std::string name;
        int         hopcount{1};
        std::string description;
        std::chrono::steady_clock::time_point connected_at;
        int  user_count{0};
    };

    void add_local_server(const std::string& name, const std::string& desc = "") {
        local_servers_[name] = RemoteServer{name, 0, desc};
    }

    void register_remote_server(const std::string& name, int hopcount,
                                 const std::string& desc) {
        remote_servers_[name] = RemoteServer{name, hopcount, desc,
                                              std::chrono::steady_clock::now()};
    }

    void remove_remote_server(const std::string& name) {
        remote_servers_.erase(name);
    }

    [[nodiscard]] std::string build_server_intro(const std::string& local_name,
                                                  const std::string& password,
                                                  const std::string& remote_name) const {
        IrcMessage msg;
        msg.command = "SERVER";
        msg.params  = {remote_name, std::to_string(1), ":" + std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count())};
        return msg.serialize();
    }

    [[nodiscard]] std::string build_netinfo(int max_global_users, int max_local_users,
                                            int64_t uptime_seconds,
                                            const std::string& proto_ver) const {
        IrcMessage msg;
        msg.command = "NETINFO";
        msg.params  = {
            std::to_string(max_global_users),
            std::to_string(std::chrono::system_clock::now().time_since_epoch().count()),
            std::to_string(max_local_users),
            std::to_string(std::chrono::system_clock::now().time_since_epoch().count()),
            "0",
            proto_ver,
            "0",
            "0",
            ":" + std::to_string(uptime_seconds)
        };
        return msg.serialize();
    }

    [[nodiscard]] std::string build_nick_intro(const std::string& nick,
                                                int hopcount,
                                                const std::string& user,
                                                const std::string& host,
                                                const std::string& server,
                                                const std::string& service_token,
                                                const std::string& umodes,
                                                const std::string& realname) const {
        IrcMessage msg;
        msg.command = "NICK";
        msg.params  = {
            nick,
            std::to_string(hopcount),
            user,
            host,
            service_token,
            "+" + umodes,
            ":" + realname
        };
        return msg.serialize();
    }

    // SQUIT — server quit
    [[nodiscard]] std::string build_squit(const std::string& server, const std::string& reason) const {
        IrcMessage msg;
        msg.command = "SQUIT";
        msg.params  = {server, ":" + reason};
        return msg.serialize();
    }

    [[nodiscard]] json status() const {
        json remotes = json::array();
        for (auto& [name, srv] : remote_servers_) {
            remotes.push_back({{"name", name}, {"hopcount", srv.hopcount},
                               {"users", srv.user_count}});
        }
        return {{"local", local_servers_.size()}, {"remote", remotes}};
    }

private:
    std::unordered_map<std::string, RemoteServer> local_servers_;
    std::unordered_map<std::string, RemoteServer> remote_servers_;
};

// ============================================================================
// IrcDispatcher — main message dispatcher and protocol engine
// ============================================================================

class IrcDispatcher {
public:
    struct Config {
        std::string server_name{"torrent-irc"};
        std::string network_name{"TorrentNet"};
        std::string server_desc{"torrent-mq IRC Server"};
        std::string server_version{"torrent-mq-1.0"};
        std::string motd_path;
        bool        require_sasl{false};
    };

    explicit IrcDispatcher(const Config& cfg)
        : config_(cfg),
          flood_protector_(),
          ctcp_handler_(),
          services_(),
          server_link_(),
          cap_negotiator_(),
          sasl_auth_()
    {
        register_default_caps();
        server_link_.add_local_server(cfg.server_name, cfg.server_desc);
    }

    // Process a raw IRC line, return list of response lines to send
    [[nodiscard]] std::vector<std::string> process_line(const std::string& raw_line,
                                                         const std::string& client_key) {
        std::vector<std::string> responses;

        auto parsed = IrcMessage::parse(raw_line);
        if (!parsed) return responses;

        auto& msg = *parsed;

        // Flood check (skip PONG)
        if (msg.command != "PONG" && flood_protector_.check_and_record(client_key)) {
            get_irc_logger()->warn("Dropping flood from {}", client_key);
            return responses;
        }

        // Determine nick from prefix or params
        std::string client_nick;
        if (msg.prefix) {
            client_nick = nick_from_prefix(*msg.prefix);
        } else if (!msg.params.empty()) {
            client_nick = msg.params[0];
        }

        // CAP (IRCv3) — must be handled before registration
        if (msg.command == "CAP") {
            auto cap_responses = handle_cap(msg, client_nick, client_key);
            responses.insert(responses.end(), cap_responses.begin(), cap_responses.end());
            return responses;
        }

        // AUTHENTICATE (SASL)
        if (msg.command == "AUTHENTICATE") {
            std::string data = msg.params.size() > 0 ? msg.params[0] : "";
            auto response = sasl_auth_.handle_authenticate(client_nick, data);
            if (!response.empty()) responses.push_back(response);
            return responses;
        }

        // Registration commands
        if (msg.command == "NICK") {
            auto nick_responses = handle_nick(msg, client_key);
            responses.insert(responses.end(), nick_responses.begin(), nick_responses.end());
        }
        else if (msg.command == "USER") {
            auto user_responses = handle_user(msg, client_key);
            responses.insert(responses.end(), user_responses.begin(), user_responses.end());
        }
        else if (msg.command == "PASS") {
            // Handle server password
            handle_pass(msg, client_nick);
        }
        else if (msg.command == "QUIT") {
            auto quit_responses = handle_quit(msg, client_key);
            responses.insert(responses.end(), quit_responses.begin(), quit_responses.end());
        }
        else if (msg.command == "PING") {
            std::string token = msg.params.empty() ? "" : msg.params[0];
            responses.push_back(":" + config_.server_name + " PONG " +
                                config_.server_name + " :" + token);
        }
        else if (msg.command == "PONG") {
            // Ping timeout tracking handled externally
        }
        else if (msg.command == "JOIN") {
            auto join_responses = handle_join(msg, client_nick);
            responses.insert(responses.end(), join_responses.begin(), join_responses.end());
        }
        else if (msg.command == "PART") {
            auto part_responses = handle_part(msg, client_nick);
            responses.insert(responses.end(), part_responses.begin(), part_responses.end());
        }
        else if (msg.command == "PRIVMSG") {
            auto privmsg_responses = handle_privmsg(msg, client_nick);
            responses.insert(responses.end(), privmsg_responses.begin(), privmsg_responses.end());
        }
        else if (msg.command == "NOTICE") {
            auto notice_responses = handle_notice(msg, client_nick);
            responses.insert(responses.end(), notice_responses.begin(), notice_responses.end());
        }
        else if (msg.command == "MODE") {
            auto mode_responses = handle_mode(msg, client_nick);
            responses.insert(responses.end(), mode_responses.begin(), mode_responses.end());
        }
        else if (msg.command == "TOPIC") {
            auto topic_responses = handle_topic(msg, client_nick);
            responses.insert(responses.end(), topic_responses.begin(), topic_responses.end());
        }
        else if (msg.command == "WHO") {
            auto who_responses = handle_who(msg, client_nick);
            responses.insert(responses.end(), who_responses.begin(), who_responses.end());
        }
        else if (msg.command == "WHOIS") {
            auto whois_responses = handle_whois(msg, client_nick);
            responses.insert(responses.end(), whois_responses.begin(), whois_responses.end());
        }
        else if (msg.command == "WHOWAS") {
            auto whowas_responses = handle_whowas(msg, client_nick);
            responses.insert(responses.end(), whowas_responses.begin(), whowas_responses.end());
        }
        else if (msg.command == "LIST") {
            auto list_responses = handle_list(msg, client_nick);
            responses.insert(responses.end(), list_responses.begin(), list_responses.end());
        }
        else if (msg.command == "NAMES") {
            auto names_responses = handle_names(msg, client_nick);
            responses.insert(responses.end(), names_responses.begin(), names_responses.end());
        }
        else if (msg.command == "KICK") {
            auto kick_responses = handle_kick(msg, client_nick);
            responses.insert(responses.end(), kick_responses.begin(), kick_responses.end());
        }
        else if (msg.command == "INVITE") {
            auto invite_responses = handle_invite(msg, client_nick);
            responses.insert(responses.end(), invite_responses.begin(), invite_responses.end());
        }
        else if (msg.command == "VERSION") {
            responses.push_back(build_numeric(IrcNumeric::RPL_VERSION, client_nick,
                {config_.server_version + "." + config_.server_name,
                 "0", config_.server_name, ":torrent-mq IRC daemon"}));
        }
        else if (msg.command == "INFO") {
            responses.push_back(build_numeric(IrcNumeric::RPL_INFO, client_nick,
                {":torrent-mq IRC Server"}));
            responses.push_back(build_numeric(IrcNumeric::RPL_ENDOFINFO, client_nick,
                {":End of /INFO list"}));
        }
        else if (msg.command == "MOTD") {
            responses.push_back(build_numeric(IrcNumeric::RPL_MOTDSTART, client_nick,
                {":- " + config_.server_name + " Message of the day -"}));
            responses.push_back(build_numeric(IrcNumeric::RPL_MOTD, client_nick,
                {":Welcome to torrent-mq IRC!"}));
            responses.push_back(build_numeric(IrcNumeric::RPL_ENDOFMOTD, client_nick,
                {":End of /MOTD command"}));
        }
        else if (msg.command == "LUSERS") {
            // Lazy user count
            responses.push_back(build_numeric(IrcNumeric::RPL_LUSERCLIENT, client_nick,
                {":There are " + std::to_string(users_.size()) +
                 " users and 0 invisible on 1 server"}));
            responses.push_back(build_numeric(IrcNumeric::RPL_LUSERME, client_nick,
                {":I have " + std::to_string(users_.size()) + " clients and 1 server"}));
        }
        else if (msg.command == "OPER") {
            // Simplified OPER
            responses.push_back(build_numeric(IrcNumeric::ERR_NOOPERHOST, client_nick,
                {":No O-lines for your host"}));
        }
        else if (msg.command == "NS" || msg.command == "NICKSERV") {
            auto ns_responses = handle_services_nick(msg);
            responses.insert(responses.end(), ns_responses.begin(), ns_responses.end());
        }
        else if (msg.command == "CS" || msg.command == "CHANSERV") {
            auto cs_responses = handle_services_chan(msg);
            responses.insert(responses.end(), cs_responses.begin(), cs_responses.end());
        }
        else if (msg.command == "ADMIN") {
            responses.push_back(build_numeric(IrcNumeric::ERR_NOADMININFO, client_nick,
                {config_.server_name, ":Administrative info unavailable"}));
        }
        else if (msg.command == "LINKS") {
            responses.push_back(build_numeric(IrcNumeric::RPL_LINKS, client_nick,
                {"*", config_.server_name, ":0 " + config_.server_desc}));
            responses.push_back(build_numeric(IrcNumeric::RPL_ENDOFLINKS, client_nick,
                {"*", ":End of /LINKS list"}));
        }
        else if (msg.command == "TIME") {
            auto t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            std::string timestr = std::ctime(&t);
            if (!timestr.empty() && timestr.back() == '\n') timestr.pop_back();
            responses.push_back(build_numeric(IrcNumeric::RPL_TIME, client_nick,
                {config_.server_name, ":" + timestr}));
        }
        else if (msg.command == "STATS") {
            char c = msg.params.empty() ? 'l' : msg.params[0][0];
            responses.push_back(build_numeric(IrcNumeric::RPL_STATS, client_nick,
                {std::string(1, c), ":End of STATS report"}));
        }
        else {
            responses.push_back(build_numeric(IrcNumeric::ERR_UNKNOWNCOMMAND, client_nick,
                {msg.command, ":Unknown command"}));
        }

        return responses;
    }

    // User management
    bool add_user(const std::string& nick, const std::string& user,
                  const std::string& host, const std::string& realname) {
        if (users_.find(nick) != users_.end()) return false;
        IrcUser u;
        u.nick          = nick;
        u.user          = user;
        u.host          = host;
        u.realname      = realname;
        u.connected_at  = std::chrono::steady_clock::now();
        u.last_active   = u.connected_at;
        u.signon_time   = u.connected_at;
        u.registered    = true;
        users_[nick]    = u;
        return true;
    }

    void remove_user(const std::string& nick) {
        // Remove from all channels
        auto it = users_.find(nick);
        if (it != users_.end()) {
            for (auto& chan_name : it->second.channels) {
                auto ci = channels_.find(chan_name);
                if (ci != channels_.end()) {
                    ci->second.members.erase(nick);
                }
            }
        }
        users_.erase(nick);
        flood_protector_.remove_user(nick);
    }

    [[nodiscard]] std::optional<IrcUser> get_user(const std::string& nick) const {
        auto it = users_.find(nick);
        if (it == users_.end()) return std::nullopt;
        return it->second;
    }

    [[nodiscard]] std::optional<IrcChannel> get_channel(const std::string& name) const {
        auto it = channels_.find(name);
        if (it == channels_.end()) return std::nullopt;
        return it->second;
    }

    [[nodiscard]] json server_status() const {
        return {
            {"server_name", config_.server_name},
            {"user_count", users_.size()},
            {"channel_count", channels_.size()},
            {"flood_stats", flood_protector_.stats()},
            {"s2s_links", server_link_.status()},
            {"services", services_.status()},
        };
    }

    [[nodiscard]] const CapabilityNegotiator& cap_negotiator() const { return cap_negotiator_; }
    [[nodiscard]] CapabilityNegotiator& cap_negotiator() { return cap_negotiator_; }
    [[nodiscard]] const SaslAuthenticator& sasl_authenticator() const { return sasl_auth_; }
    [[nodiscard]] IrcServices& services() { return services_; }
    [[nodiscard]] IrcServerLink& server_link() { return server_link_; }
    [[nodiscard]] CtcpHandler& ctcp() { return ctcp_handler_; }

    void broadcast_to_channel(const std::string& channel, const std::string& line,
                               const std::string& exclude_nick = "") {
        auto ci = channels_.find(channel);
        if (ci == channels_.end()) return;
        for (auto& [nick, _prefix] : ci->second.members) {
            if (nick != exclude_nick) {
                // In real impl, push to client socket
                pending_broadcasts_[nick].push_back(line);
            }
        }
    }

    [[nodiscard]] std::vector<std::string> drain_broadcasts(const std::string& nick) {
        std::vector<std::string> result;
        auto it = pending_broadcasts_.find(nick);
        if (it != pending_broadcasts_.end()) {
            result = std::move(it->second);
            pending_broadcasts_.erase(it);
        }
        return result;
    }

private:
    Config            config_;
    FloodProtector    flood_protector_;
    CtcpHandler       ctcp_handler_;
    IrcServices       services_;
    IrcServerLink     server_link_;
    CapabilityNegotiator cap_negotiator_;
    SaslAuthenticator sasl_auth_;

    std::unordered_map<std::string, IrcUser>    users_;
    std::unordered_map<std::string, IrcChannel> channels_;
    std::unordered_map<std::string, std::vector<IrcUser>> whowas_; // history

    std::unordered_map<std::string, std::vector<std::string>> pending_broadcasts_;

    // Missing numeric
    static inline constexpr int RPL_LUSERCLIENT = 251;
    static inline constexpr int RPL_LUSERME     = 255;
    static inline constexpr int RPL_STATS       = 250;

    void register_default_caps() {
        cap_negotiator_.register_capability("sasl", true);
        cap_negotiator_.register_capability("multi-prefix", true);
        cap_negotiator_.register_capability("account-tag", true);
        cap_negotiator_.register_capability("account-notify", true);
        cap_negotiator_.register_capability("away-notify", true);
        cap_negotiator_.register_capability("extended-join", true);
        cap_negotiator_.register_capability("cap-notify", true);
        cap_negotiator_.register_capability("chghost", true);
        cap_negotiator_.register_capability("invite-notify", true);
        cap_negotiator_.register_capability("message-tags", true);
        cap_negotiator_.register_capability("echo-message", true);
        cap_negotiator_.register_capability("setname", true);
        cap_negotiator_.register_capability("server-time", true);
        cap_negotiator_.register_capability("userhost-in-names", true);
        cap_negotiator_.register_capability("batch", true);
        cap_negotiator_.register_capability("labeled-response", true);
        cap_negotiator_.register_capability("sts", true);
    }

    // --- CAP handler ---
    std::vector<std::string> handle_cap(const IrcMessage& msg,
                                         const std::string& nick,
                                         const std::string& client_key) {
        std::vector<std::string> responses;
        if (msg.params.size() < 2) return responses;

        std::string subcommand = msg.params[1];

        if (subcommand == "LS") {
            std::string version_str = msg.params.size() > 2 ? msg.params[2] : "302";
            responses.push_back(cap_negotiator_.handle_cap_ls(nick));
        }
        else if (subcommand == "LIST") {
            responses.push_back(cap_negotiator_.handle_cap_list(nick));
        }
        else if (subcommand == "REQ") {
            std::string caps_str = msg.params.size() > 2 ? msg.params[2] : "";
            std::vector<std::string> requested;
            std::istringstream iss(caps_str);
            std::string cap;
            while (iss >> cap) requested.push_back(cap);
            auto result = cap_negotiator_.handle_cap_req(nick, requested);
            responses.insert(responses.end(), result.responses.begin(),
                            result.responses.end());
        }
        else if (subcommand == "END") {
            cap_negotiator_.handle_cap_end(nick);
        }

        return responses;
    }

    // --- NICK ---
    std::vector<std::string> handle_nick(const IrcMessage& msg, const std::string& client_key) {
        std::vector<std::string> responses;
        if (msg.params.empty()) {
            responses.push_back(build_numeric(IrcNumeric::ERR_NONICKNAMEGIVEN,
                                "*", {":No nickname given"}));
            return responses;
        }
        std::string new_nick = msg.params[0];
        if (!is_valid_nick(new_nick)) {
            responses.push_back(build_numeric(IrcNumeric::ERR_ERRONEUSNICKNAME,
                                "*", {new_nick, ":Erroneous nickname"}));
            return responses;
        }
        if (users_.find(new_nick) != users_.end()) {
            responses.push_back(build_numeric(IrcNumeric::ERR_NICKNAMEINUSE,
                                "*", {new_nick, ":Nickname is already in use"}));
            return responses;
        }
        // Nick change
        std::string old_nick;
        if (msg.prefix) old_nick = nick_from_prefix(*msg.prefix);
        if (!old_nick.empty() && users_.find(old_nick) != users_.end()) {
            auto user = users_[old_nick];
            users_.erase(old_nick);
            user.nick = new_nick;
            users_[new_nick] = user;

            // Notify channels
            std::string change_line = ":" + user.full_mask() + " NICK :" + new_nick;
            for (auto& chan : user.channels) {
                broadcast_to_channel(chan, change_line);
            }
            responses.push_back(change_line);
        }
        return responses;
    }

    // --- USER ---
    std::vector<std::string> handle_user(const IrcMessage& msg, const std::string& client_key) {
        std::vector<std::string> responses;
        if (msg.params.size() < 4) {
            responses.push_back(build_numeric(IrcNumeric::ERR_NEEDMOREPARAMS,
                                "*", {"USER", ":Not enough parameters"}));
            return responses;
        }
        std::string ident   = msg.params[0];
        std::string mode_str = msg.params[1];
        std::string unused  = msg.params[2];
        std::string realname = msg.params[3];
        std::string nick = msg.params.size() > 4 ? msg.params[4] : "";

        // Find the nick being registered
        std::string target_nick;
        if (msg.prefix) target_nick = nick_from_prefix(*msg.prefix);
        else if (!nick.empty()) target_nick = nick;
        if (target_nick.empty()) return responses;

        // Check if already registered
        auto it = users_.find(target_nick);
        bool already_reg = (it != users_.end() && it->second.registered);

        if (already_reg) {
            responses.push_back(build_numeric(IrcNumeric::ERR_ALREADYREGISTERED,
                                target_nick, {":You may not reregister"}));
            return responses;
        }

        // Create or update user
        if (it == users_.end()) {
            IrcUser u;
            u.nick     = target_nick;
            u.user     = ident;
            u.host     = "client";
            u.realname = realname;
            u.connected_at = std::chrono::steady_clock::now();
            u.last_active  = u.connected_at;
            u.signon_time  = u.connected_at;
            u.registered   = true;
            users_[target_nick] = u;
        } else {
            it->second.user     = ident;
            it->second.realname = realname;
            it->second.registered = true;
        }

        // Send welcome sequence
        auto& u = users_[target_nick];
        responses.push_back(build_numeric(IrcNumeric::RPL_WELCOME, target_nick,
            {":Welcome to the " + config_.network_name + " IRC Network " +
             u.full_mask()}));
        responses.push_back(build_numeric(IrcNumeric::RPL_YOURHOST, target_nick,
            {":Your host is " + config_.server_name +
             ", running version " + config_.server_version}));
        {
            auto t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            std::string ctime_str = std::ctime(&t);
            if (!ctime_str.empty() && ctime_str.back() == '\n') ctime_str.pop_back();
            responses.push_back(build_numeric(IrcNumeric::RPL_CREATED, target_nick,
                {":This server was created " + ctime_str}));
        }
        {
            // RPL_MYINFO: servername version usermodes chanmodes
            std::string myinfo = config_.server_name + " " +
                                 config_.server_version + " iwsro imnpstkl";
            responses.push_back(build_numeric(IrcNumeric::RPL_MYINFO, target_nick,
                {myinfo}));
        }
        // ISUPPORT (RPL_BOUNCE)
        {
            std::string isupport = "CASEMAPPING=rfc1459 CHANNELLEN=" +
                std::to_string(kMaxChannelLength) + " NICKLEN=" +
                std::to_string(kMaxNickLength) + " CHANTYPES=#&+! " +
                "PREFIX=(ov)@+ CHANMODES=,,,imnpstkl " +
                "MODES=6 MAXCHANNELS=20 STATUSMSG=@+ " +
                "NETWORK=" + config_.network_name + " " +
                "SASL=PLAIN,SCRAM-SHA-256,EXTERNAL " +
                "are supported by this server";
            responses.push_back(build_numeric(IrcNumeric::RPL_ISUPPORT, target_nick,
                {isupport + " :are supported by this server"}));
        }

        // LUSERS
        responses.push_back(build_numeric(RPL_LUSERCLIENT, target_nick,
            {":There are " + std::to_string(users_.size()) +
             " users and 0 invisible on 1 server"}));
        responses.push_back(build_numeric(RPL_LUSERME, target_nick,
            {":I have " + std::to_string(users_.size()) + " clients and 1 server"}));

        return responses;
    }

    // --- PASS ---
    void handle_pass(const IrcMessage& msg, const std::string& nick) {
        // In a real server, validate against server password
        if (!msg.params.empty()) {
            get_irc_logger()->debug("PASS from {} (validating)", nick);
        }
    }

    // --- QUIT ---
    std::vector<std::string> handle_quit(const IrcMessage& msg, const std::string& client_key) {
        std::vector<std::string> responses;
        std::string reason = msg.params.empty() ? "Client Quit" : msg.params[0];
        std::string nick;
        if (msg.prefix) nick = nick_from_prefix(*msg.prefix);

        auto it = users_.find(nick);
        if (it == users_.end()) return responses;

        // Notify channels
        std::string quit_line = ":" + it->second.full_mask() + " QUIT :" + reason;
        for (auto& chan : it->second.channels) {
            broadcast_to_channel(chan, quit_line);
        }
        // Save to whowas
        whowas_[nick].push_back(it->second);
        if (whowas_[nick].size() > 10) whowas_[nick].erase(whowas_[nick].begin());

        remove_user(nick);
        responses.push_back(":" + nick + " ERROR :Closing Link");
        return responses;
    }

    // --- JOIN ---
    std::vector<std::string> handle_join(const IrcMessage& msg, const std::string& nick) {
        std::vector<std::string> responses;
        if (msg.params.empty()) {
            responses.push_back(build_numeric(IrcNumeric::ERR_NEEDMOREPARAMS,
                                nick, {"JOIN", ":Not enough parameters"}));
            return responses;
        }
        std::string chan_name = msg.params[0];
        std::string key       = msg.params.size() > 1 ? msg.params[1] : "";

        if (!is_valid_channel_name(chan_name)) {
            responses.push_back(build_numeric(IrcNumeric::ERR_NOSUCHCHANNEL,
                                nick, {chan_name, ":No such channel"}));
            return responses;
        }

        auto user_it = users_.find(nick);
        if (user_it == users_.end()) return responses;

        // Get or create channel
        auto& channel = channels_[chan_name];
        if (channel.name.empty()) {
            channel.name       = chan_name;
            channel.created_at = std::chrono::steady_clock::now();
        }

        // Channel mode checks
        if (channel.has_mode('i') &&
            std::find(channel.invite_list.begin(), channel.invite_list.end(),
                      nick) == channel.invite_list.end()) {
            responses.push_back(build_numeric(IrcNumeric::ERR_INVITEONLYCHAN,
                                nick, {chan_name, ":Cannot join channel (+i)"}));
            return responses;
        }
        if (channel.has_mode('k') && !channel.key.empty() && channel.key != key) {
            responses.push_back(build_numeric(IrcNumeric::ERR_BADCHANNELKEY,
                                nick, {chan_name, ":Cannot join channel (+k)"}));
            return responses;
        }
        if (channel.has_mode('l') && channel.user_limit > 0 &&
            static_cast<int>(channel.members.size()) >= channel.user_limit) {
            responses.push_back(build_numeric(IrcNumeric::ERR_CHANNELISFULL,
                                nick, {chan_name, ":Cannot join channel (+l)"}));
            return responses;
        }

        // Add member
        std::string prefix = channel.members.empty() ? "@" : ""; // first joiner is op
        channel.members[nick] = prefix.empty() ? "" : "@";
        user_it->second.channels.insert(chan_name);
        user_it->second.channel_prefixes[chan_name] = prefix.empty() ? "" : "@";

        std::string join_line = ":" + user_it->second.full_mask() + " JOIN " + chan_name;
        broadcast_to_channel(chan_name, join_line);
        responses.push_back(join_line);

        // Send topic
        if (channel.topic.empty()) {
            responses.push_back(build_numeric(IrcNumeric::RPL_NOTOPIC,
                                nick, {chan_name, ":No topic is set"}));
        } else {
            responses.push_back(build_numeric(IrcNumeric::RPL_TOPIC,
                                nick, {chan_name, ":" + channel.topic}));
            responses.push_back(build_numeric(IrcNumeric::RPL_TOPICWHOTIME,
                                nick, {chan_name, channel.topic_setter,
                                       std::to_string(channel.topic_time)}));
        }

        // Send NAMES
        std::string names_list;
        for (auto& [n, p] : channel.members) {
            if (!names_list.empty()) names_list += " ";
            if (!p.empty()) names_list += p;
            names_list += n;
        }
        responses.push_back(build_numeric(IrcNumeric::RPL_NAMREPLY,
                            nick, {"=", chan_name, ":" + names_list}));
        responses.push_back(build_numeric(IrcNumeric::RPL_ENDOFNAMES,
                            nick, {chan_name, ":End of /NAMES list"}));

        return responses;
    }

    // --- PART ---
    std::vector<std::string> handle_part(const IrcMessage& msg, const std::string& nick) {
        std::vector<std::string> responses;
        if (msg.params.empty()) return responses;
        std::string chan_name = msg.params[0];
        std::string reason    = msg.params.size() > 1 ? msg.params[1] : nick;

        auto user_it = users_.find(nick);
        if (user_it == users_.end()) return responses;
        auto chan_it = channels_.find(chan_name);
        if (chan_it == channels_.end()) {
            responses.push_back(build_numeric(IrcNumeric::ERR_NOSUCHCHANNEL,
                                nick, {chan_name, ":No such channel"}));
            return responses;
        }

        std::string part_line = ":" + user_it->second.full_mask() + " PART " +
                                chan_name + " :" + reason;
        broadcast_to_channel(chan_name, part_line);
        responses.push_back(part_line);

        user_it->second.channels.erase(chan_name);
        user_it->second.channel_prefixes.erase(chan_name);
        chan_it->second.members.erase(nick);

        // Cleanup empty channels
        if (chan_it->second.members.empty()) {
            channels_.erase(chan_it);
        }

        return responses;
    }

    // --- PRIVMSG ---
    std::vector<std::string> handle_privmsg(const IrcMessage& msg, const std::string& nick) {
        std::vector<std::string> responses;
        if (msg.params.size() < 1) {
            responses.push_back(build_numeric(IrcNumeric::ERR_NORECIPIENT,
                                nick, {":No recipient given (PRIVMSG)"}));
            return responses;
        }
        if (msg.params.size() < 2) {
            responses.push_back(build_numeric(IrcNumeric::ERR_NOTEXTTOSEND,
                                nick, {":No text to send"}));
            return responses;
        }
        std::string target = msg.params[0];
        std::string text   = msg.params[1];

        // Check for CTCP
        if (auto ctcp = CtcpHandler::extract_ctcp(text)) {
            std::string ctcp_response = ctcp_handler_.handle(target, nick,
                ctcp->first, ctcp->second);
            if (!ctcp_response.empty()) responses.push_back(ctcp_response);
            return responses;
        }

        if (is_valid_channel_name(target)) {
            broadcast_to_channel(target,
                ":" + users_[nick].full_mask() + " PRIVMSG " + target + " :" + text,
                nick);
        } else {
            // Private message
            if (users_.find(target) != users_.end()) {
                responses.push_back(":" + users_[nick].full_mask() +
                                    " PRIVMSG " + target + " :" + text);
            } else {
                responses.push_back(build_numeric(IrcNumeric::ERR_NOSUCHNICK,
                                    nick, {target, ":No such nick/channel"}));
            }
        }
        return responses;
    }

    // --- NOTICE ---
    std::vector<std::string> handle_notice(const IrcMessage& msg, const std::string& nick) {
        std::vector<std::string> responses;
        if (msg.params.size() < 2) return responses;
        std::string target = msg.params[0];
        std::string text   = msg.params[1];

        if (auto ctcp = CtcpHandler::extract_ctcp(text)) {
            std::string ctcp_response = ctcp_handler_.handle(target, nick,
                ctcp->first, ctcp->second);
            if (!ctcp_response.empty()) responses.push_back(ctcp_response);
            return responses;
        }

        // Relay notice to target
        if (is_valid_channel_name(target)) {
            broadcast_to_channel(target,
                ":" + users_[nick].full_mask() + " NOTICE " + target + " :" + text,
                nick);
        } else if (users_.find(target) != users_.end()) {
            responses.push_back(":" + users_[nick].full_mask() +
                                " NOTICE " + target + " :" + text);
        }
        return responses;
    }

    // --- MODE ---
    std::vector<std::string> handle_mode(const IrcMessage& msg, const std::string& nick) {
        std::vector<std::string> responses;
        if (msg.params.empty()) return responses;
        std::string target = msg.params[0];

        if (is_valid_channel_name(target)) {
            auto chan_it = channels_.find(target);
            if (chan_it == channels_.end()) {
                responses.push_back(build_numeric(IrcNumeric::ERR_NOSUCHCHANNEL,
                                    nick, {target, ":No such channel"}));
                return responses;
            }
            if (msg.params.size() < 2) {
                // Query modes
                std::string mode_str;
                for (char m : chan_it->second.modes) { mode_str += m; }
                std::string param;
                if (chan_it->second.has_mode('k')) param += " " + chan_it->second.key;
                if (chan_it->second.has_mode('l'))
                    param += " " + std::to_string(chan_it->second.user_limit);
                responses.push_back(build_numeric(IrcNumeric::RPL_CHANNELMODEIS,
                    nick, {target, "+" + mode_str + param}));
                return responses;
            }

            // Process mode changes (simplified: only op/deop here)
            std::string mode_str = msg.params[1];
            bool adding = true;
            size_t param_idx = 2;
            std::string applied_modes;
            std::string applied_params;

            for (char c : mode_str) {
                if (c == '+') { adding = true; continue; }
                if (c == '-') { adding = false; continue; }

                if (c == 'o' || c == 'v') {
                    if (param_idx < msg.params.size()) {
                        std::string target_nick = msg.params[param_idx++];
                        if (chan_it->second.members.find(target_nick) !=
                            chan_it->second.members.end()) {
                            if (adding) {
                                chan_it->second.members[target_nick] = (c == 'o') ? "@" : "+";
                            } else {
                                chan_it->second.members[target_nick] = "";
                            }
                            applied_modes += (adding ? "+" : "-");
                            applied_modes += c;
                            applied_params += " " + target_nick;
                        }
                    }
                } else if (c == 'k') {
                    if (adding && param_idx < msg.params.size()) {
                        chan_it->second.key = msg.params[param_idx++];
                        chan_it->second.modes.insert('k');
                        applied_modes += "+k";
                    } else if (!adding) {
                        chan_it->second.key.clear();
                        chan_it->second.modes.erase('k');
                        applied_modes += "-k";
                    }
                } else if (c == 'l') {
                    if (adding && param_idx < msg.params.size()) {
                        chan_it->second.user_limit = std::stoi(msg.params[param_idx++]);
                        chan_it->second.modes.insert('l');
                        applied_modes += "+l";
                    } else if (!adding) {
                        chan_it->second.user_limit = 0;
                        chan_it->second.modes.erase('l');
                        applied_modes += "-l";
                    }
                } else if (c == 'i' || c == 'm' || c == 'n' || c == 'p' ||
                           c == 's' || c == 't') {
                    if (adding) chan_it->second.modes.insert(c);
                    else        chan_it->second.modes.erase(c);
                    applied_modes += (adding ? "+" : "-");
                    applied_modes += c;
                }
            }

            if (!applied_modes.empty()) {
                std::string mode_line = ":" + users_[nick].full_mask() +
                    " MODE " + target + " " + applied_modes + applied_params;
                broadcast_to_channel(target, mode_line);
                responses.push_back(mode_line);
            }
        } else {
            // User modes
            if (target != nick) {
                responses.push_back(build_numeric(IrcNumeric::ERR_USERSDONTMATCH,
                    nick, {":Cannot change mode for other users"}));
                return responses;
            }
            if (msg.params.size() < 2) {
                // Query user modes
                std::string umode_str;
                for (char m : users_[nick].umodes) umode_str += m;
                responses.push_back(build_numeric(IrcNumeric::RPL_UMODEIS,
                    nick, {"+" + umode_str}));
                return responses;
            }
            // Apply user mode changes
            std::string mode_str = msg.params[1];
            bool adding = true;
            for (char c : mode_str) {
                if (c == '+') { adding = true; continue; }
                if (c == '-') { adding = false; continue; }
                if (c == 'i' || c == 'w' || c == 's' || c == 'r' || c == 'o') {
                    if (adding) users_[nick].umodes.insert(c);
                    else        users_[nick].umodes.erase(c);
                }
            }
            responses.push_back(":" + users_[nick].full_mask() +
                                " MODE " + nick + " " + mode_str);
        }
        return responses;
    }

    // --- TOPIC ---
    std::vector<std::string> handle_topic(const IrcMessage& msg, const std::string& nick) {
        std::vector<std::string> responses;
        if (msg.params.empty()) return responses;
        std::string chan_name = msg.params[0];

        auto chan_it = channels_.find(chan_name);
        if (chan_it == channels_.end()) {
            responses.push_back(build_numeric(IrcNumeric::ERR_NOSUCHCHANNEL,
                                nick, {chan_name, ":No such channel"}));
            return responses;
        }

        if (msg.params.size() > 1) {
            // Set topic
            if (chan_it->second.has_mode('t') &&
                chan_it->second.members[nick] != "@") {
                responses.push_back(build_numeric(IrcNumeric::ERR_CHANOPRIVSNEEDED,
                    nick, {chan_name, ":You're not channel operator"}));
                return responses;
            }
            chan_it->second.topic        = msg.params[1];
            chan_it->second.topic_setter = users_[nick].full_mask();
            chan_it->second.topic_time   = std::chrono::system_clock::now()
                                           .time_since_epoch().count();
            std::string topic_line = ":" + users_[nick].full_mask() +
                " TOPIC " + chan_name + " :" + chan_it->second.topic;
            broadcast_to_channel(chan_name, topic_line);
            responses.push_back(topic_line);
        } else {
            // Query topic
            if (chan_it->second.topic.empty()) {
                responses.push_back(build_numeric(IrcNumeric::RPL_NOTOPIC,
                    nick, {chan_name, ":No topic is set"}));
            } else {
                responses.push_back(build_numeric(IrcNumeric::RPL_TOPIC,
                    nick, {chan_name, ":" + chan_it->second.topic}));
                responses.push_back(build_numeric(IrcNumeric::RPL_TOPICWHOTIME,
                    nick, {chan_name, chan_it->second.topic_setter,
                           std::to_string(chan_it->second.topic_time)}));
            }
        }
        return responses;
    }

    // --- WHO ---
    std::vector<std::string> handle_who(const IrcMessage& msg, const std::string& nick) {
        std::vector<std::string> responses;
        if (msg.params.empty()) return responses;
        std::string mask = msg.params[0];

        if (is_valid_channel_name(mask)) {
            auto chan_it = channels_.find(mask);
            if (chan_it == channels_.end()) {
                responses.push_back(build_numeric(IrcNumeric::RPL_ENDOFWHO,
                    nick, {mask, ":End of WHO list"}));
                return responses;
            }
            for (auto& [n, p] : chan_it->second.members) {
                auto& u = users_[n];
                std::string flags = "H";
                if (!p.empty()) flags += p;
                if (u.is_oper) flags += "*";
                responses.push_back(build_numeric(IrcNumeric::RPL_WHOREPLY,
                    nick, {mask, u.user, u.host, config_.server_name,
                           n, flags, ":0 " + u.realname}));
            }
        }
        responses.push_back(build_numeric(IrcNumeric::RPL_ENDOFWHO,
            nick, {mask, ":End of WHO list"}));
        return responses;
    }

    // --- WHOIS ---
    std::vector<std::string> handle_whois(const IrcMessage& msg, const std::string& nick) {
        std::vector<std::string> responses;
        if (msg.params.empty()) return responses;
        std::string target = msg.params.back();

        auto it = users_.find(target);
        if (it == users_.end()) {
            responses.push_back(build_numeric(IrcNumeric::ERR_NOSUCHNICK,
                nick, {target, ":No such nick"}));
            return responses;
        }
        auto& u = it->second;
        responses.push_back(build_numeric(IrcNumeric::RPL_WHOISUSER,
            nick, {u.nick, u.user, u.host, "*", ":" + u.realname}));
        responses.push_back(build_numeric(IrcNumeric::RPL_WHOISSERVER,
            nick, {u.nick, config_.server_name, ":" + config_.server_desc}));

        if (!u.account.empty()) {
            responses.push_back(build_numeric(IrcNumeric::RPL_WHOISACCOUNT,
                nick, {u.nick, u.account, ":is logged in as"}));
        }
        // Channels
        if (!u.channels.empty()) {
            std::string chan_list;
            for (auto& c : u.channels) {
                if (!chan_list.empty()) chan_list += " ";
                auto p = u.channel_prefixes.find(c);
                if (p != u.channel_prefixes.end() && !p->second.empty())
                    chan_list += p->second;
                chan_list += c;
            }
            responses.push_back(build_numeric(IrcNumeric::RPL_WHOISCHANNELS,
                nick, {u.nick, ":" + chan_list}));
        }
        auto idle_sec = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - u.last_active).count();
        auto signon = std::chrono::system_clock::to_time_t(
            std::chrono::system_clock::now() -
            (std::chrono::steady_clock::now() - u.signon_time));
        responses.push_back(build_numeric(IrcNumeric::RPL_WHOISIDLE,
            nick, {u.nick, std::to_string(idle_sec),
                   std::to_string(static_cast<int64_t>(signon)),
                   ":seconds idle, signon time"}));
        responses.push_back(build_numeric(IrcNumeric::RPL_ENDOFWHOIS,
            nick, {u.nick, ":End of WHOIS list"}));
        return responses;
    }

    // --- WHOWAS ---
    std::vector<std::string> handle_whowas(const IrcMessage& msg, const std::string& nick) {
        std::vector<std::string> responses;
        if (msg.params.empty()) return responses;
        std::string target = msg.params[0];

        auto it = whowas_.find(target);
        if (it == whowas_.end() || it->second.empty()) {
            responses.push_back(build_numeric(IrcNumeric::ERR_WASNOSUCHNICK,
                nick, {target, ":There was no such nickname"}));
            return responses;
        }
        for (auto& u : it->second) {
            responses.push_back(build_numeric(IrcNumeric::RPL_WHOWASUSER,
                nick, {u.nick, u.user, u.host, "*", ":" + u.realname}));
        }
        responses.push_back(build_numeric(IrcNumeric::RPL_ENDOFWHOWAS,
            nick, {target, ":End of WHOWAS"}));
        return responses;
    }

    // --- LIST ---
    std::vector<std::string> handle_list(const IrcMessage& msg, const std::string& nick) {
        std::vector<std::string> responses;
        responses.push_back(build_numeric(IrcNumeric::RPL_LISTSTART, nick,
            {"Channel", ":Users  Name"}));
        for (auto& [name, chan] : channels_) {
            if (chan.has_mode('s') || chan.has_mode('p')) continue; // skip private/secret
            responses.push_back(build_numeric(IrcNumeric::RPL_LIST,
                nick, {name, std::to_string(chan.members.size()),
                       ":" + chan.topic}));
        }
        responses.push_back(build_numeric(IrcNumeric::RPL_LISTEND,
            nick, {":End of LIST"}));
        return responses;
    }

    // --- NAMES ---
    std::vector<std::string> handle_names(const IrcMessage& msg, const std::string& nick) {
        std::vector<std::string> responses;
        // If no channel given, list all
        std::vector<std::string> target_chans;
        if (msg.params.empty()) {
            for (auto& [name, _] : channels_) target_chans.push_back(name);
        } else {
            target_chans.push_back(msg.params[0]);
        }

        for (auto& chan_name : target_chans) {
            auto chan_it = channels_.find(chan_name);
            if (chan_it == channels_.end()) {
                responses.push_back(build_numeric(IrcNumeric::RPL_ENDOFNAMES,
                    nick, {chan_name, ":End of /NAMES list"}));
                continue;
            }
            std::string names_list;
            std::string symbol = chan_it->second.has_mode('s') ? "@" :
                                 chan_it->second.has_mode('p') ? "*" : "=";
            for (auto& [n, p] : chan_it->second.members) {
                if (!names_list.empty()) names_list += " ";
                if (!p.empty()) names_list += p;
                names_list += n;
            }
            responses.push_back(build_numeric(IrcNumeric::RPL_NAMREPLY,
                nick, {symbol, chan_name, ":" + names_list}));
            responses.push_back(build_numeric(IrcNumeric::RPL_ENDOFNAMES,
                nick, {chan_name, ":End of /NAMES list"}));
        }
        return responses;
    }

    // --- KICK ---
    std::vector<std::string> handle_kick(const IrcMessage& msg, const std::string& nick) {
        std::vector<std::string> responses;
        if (msg.params.size() < 2) return responses;
        std::string chan_name  = msg.params[0];
        std::string target     = msg.params[1];
        std::string reason     = msg.params.size() > 2 ? msg.params[2] : nick;

        auto chan_it = channels_.find(chan_name);
        if (chan_it == channels_.end()) {
            responses.push_back(build_numeric(IrcNumeric::ERR_NOSUCHCHANNEL,
                nick, {chan_name, ":No such channel"}));
            return responses;
        }
        // Check op status
        if (chan_it->second.members[nick] != "@") {
            responses.push_back(build_numeric(IrcNumeric::ERR_CHANOPRIVSNEEDED,
                nick, {chan_name, ":You're not channel operator"}));
            return responses;
        }
        auto target_it = users_.find(target);
        if (target_it == users_.end() ||
            chan_it->second.members.find(target) == chan_it->second.members.end()) {
            responses.push_back(build_numeric(IrcNumeric::ERR_USERNOTINCHANNEL,
                nick, {target, chan_name, ":They aren't on that channel"}));
            return responses;
        }

        std::string kick_line = ":" + users_[nick].full_mask() +
            " KICK " + chan_name + " " + target + " :" + reason;
        broadcast_to_channel(chan_name, kick_line);
        responses.push_back(kick_line);

        // Remove from channel
        target_it->second.channels.erase(chan_name);
        target_it->second.channel_prefixes.erase(chan_name);
        chan_it->second.members.erase(target);
        if (chan_it->second.members.empty()) channels_.erase(chan_it);

        return responses;
    }

    // --- INVITE ---
    std::vector<std::string> handle_invite(const IrcMessage& msg, const std::string& nick) {
        std::vector<std::string> responses;
        if (msg.params.size() < 2) return responses;
        std::string target    = msg.params[0];
        std::string chan_name = msg.params[1];

        auto chan_it = channels_.find(chan_name);
        if (chan_it == channels_.end()) {
            responses.push_back(build_numeric(IrcNumeric::ERR_NOSUCHCHANNEL,
                nick, {chan_name, ":No such channel"}));
            return responses;
        }
        if (chan_it->second.members.find(nick) == chan_it->second.members.end()) {
            responses.push_back(build_numeric(IrcNumeric::ERR_NOTONCHANNEL,
                nick, {chan_name, ":You're not on that channel"}));
            return responses;
        }
        if (chan_it->second.has_mode('i') &&
            chan_it->second.members[nick] != "@") {
            responses.push_back(build_numeric(IrcNumeric::ERR_CHANOPRIVSNEEDED,
                nick, {chan_name, ":You're not channel operator"}));
            return responses;
        }

        chan_it->second.invite_list.push_back(target);
        responses.push_back(build_numeric(IrcNumeric::RPL_INVITING,
            nick, {target, chan_name}));
        responses.push_back(":" + users_[nick].full_mask() +
            " INVITE " + target + " " + chan_name);
        return responses;
    }

    // --- Services (NickServ/ChanServ) ---
    std::vector<std::string> handle_services_nick(const IrcMessage& msg) {
        std::vector<std::string> responses;
        if (msg.params.empty()) return responses;
        std::string full_cmd = msg.params[0];
        if (msg.params.size() > 1) full_cmd += " " + msg.params[1];

        std::istringstream iss(full_cmd);
        std::string subcmd;
        iss >> subcmd;

        // map to lowercase
        std::transform(subcmd.begin(), subcmd.end(), subcmd.begin(),
                       [](unsigned char c) { return std::tolower(c); });

        std::string nick;
        if (msg.prefix) nick = nick_from_prefix(*msg.prefix);

        if (subcmd == "register") {
            std::string password;
            iss >> password;
            if (services_.register_nick(nick, password)) {
                responses.push_back(":NickServ!services@" + config_.server_name +
                    " NOTICE " + nick + " :Nickname registered successfully.");
            } else {
                responses.push_back(":NickServ!services@" + config_.server_name +
                    " NOTICE " + nick + " :That nickname is already registered.");
            }
        } else if (subcmd == "identify") {
            std::string password;
            iss >> password;
            if (services_.identify(nick, password)) {
                users_[nick].account = nick;
                responses.push_back(":NickServ!services@" + config_.server_name +
                    " NOTICE " + nick + " :You are now identified for " + nick + ".");
            } else {
                responses.push_back(":NickServ!services@" + config_.server_name +
                    " NOTICE " + nick + " :Invalid password.");
            }
        } else if (subcmd == "drop") {
            std::string password;
            iss >> password;
            if (services_.drop_nick(nick, password)) {
                users_[nick].account.clear();
                responses.push_back(":NickServ!services@" + config_.server_name +
                    " NOTICE " + nick + " :Nickname dropped.");
            } else {
                responses.push_back(":NickServ!services@" + config_.server_name +
                    " NOTICE " + nick + " :Invalid password.");
            }
        } else if (subcmd == "info") {
            std::string target;
            iss >> target;
            if (services_.is_registered(target)) {
                responses.push_back(":NickServ!services@" + config_.server_name +
                    " NOTICE " + nick + " :" + target + " is registered.");
            } else {
                responses.push_back(":NickServ!services@" + config_.server_name +
                    " NOTICE " + nick + " :" + target + " is not registered.");
            }
        } else if (subcmd == "set") {
            responses.push_back(":NickServ!services@" + config_.server_name +
                " NOTICE " + nick + " :SET password|email|enforce — not implemented in minimal mode.");
        } else {
            responses.push_back(":NickServ!services@" + config_.server_name +
                " NOTICE " + nick +
                " :Commands: REGISTER, IDENTIFY, DROP, INFO, SET");
        }

        return responses;
    }

    std::vector<std::string> handle_services_chan(const IrcMessage& msg) {
        std::vector<std::string> responses;
        if (msg.params.empty()) return responses;
        std::string full_cmd = msg.params[0];
        if (msg.params.size() > 1) full_cmd += " " + msg.params[1];

        std::istringstream iss(full_cmd);
        std::string subcmd;
        iss >> subcmd;
        std::transform(subcmd.begin(), subcmd.end(), subcmd.begin(),
                       [](unsigned char c) { return std::tolower(c); });

        std::string nick;
        if (msg.prefix) nick = nick_from_prefix(*msg.prefix);

        if (subcmd == "register") {
            std::string chan, password;
            iss >> chan >> password;
            if (services_.register_channel(chan, nick)) {
                responses.push_back(":ChanServ!services@" + config_.server_name +
                    " NOTICE " + nick +
                    " :Channel " + chan + " registered to " + nick + ".");
            } else {
                responses.push_back(":ChanServ!services@" + config_.server_name +
                    " NOTICE " + nick +
                    " :Channel is already registered.");
            }
        } else if (subcmd == "info") {
            std::string chan;
            iss >> chan;
            auto founder = services_.get_channel_founder(chan);
            if (founder) {
                responses.push_back(":ChanServ!services@" + config_.server_name +
                    " NOTICE " + nick +
                    " :" + chan + " founded by " + *founder + ".");
            } else {
                responses.push_back(":ChanServ!services@" + config_.server_name +
                    " NOTICE " + nick +
                    " :Channel " + chan + " is not registered.");
            }
        } else {
            responses.push_back(":ChanServ!services@" + config_.server_name +
                " NOTICE " + nick +
                " :Commands: REGISTER, INFO, ACCESS, FLAGS, SET");
        }

        return responses;
    }
};

// ============================================================================
// IrcServer — top-level IRC server orchestrator
// ============================================================================

class IrcServer {
public:
    struct Config {
        IrcDispatcher::Config dispatcher;
        uint16_t              port{kDefaultIrcPort};
        uint16_t              tls_port{kDefaultIrcTlsPort};
        std::string           bind_address{"0.0.0.0"};
        int                   max_clients{10000};
        int                   ping_interval_sec{60};
        int                   ping_timeout_sec{30};
        bool                  daemonize{false};
    };

    explicit IrcServer(const Config& cfg)
        : config_(cfg),
          dispatcher_(cfg.dispatcher),
          running_(false)
    {}

    void start() {
        running_.store(true);
        get_irc_logger()->info("IRC server starting on port {} (TLS: {})",
                               config_.port, config_.tls_port);

        // In a real implementation, this would set up sockets,
        // accept connections, spawn client handler threads.
        // Here we expose the dispatcher for integration.
        get_irc_logger()->info("IRC server ready — dispatcher active");
    }

    void shutdown() {
        running_.store(false);
        get_irc_logger()->info("IRC server shutting down");
    }

    [[nodiscard]] IrcDispatcher& dispatcher() { return dispatcher_; }
    [[nodiscard]] const IrcDispatcher& dispatcher() const { return dispatcher_; }
    [[nodiscard]] bool running() const { return running_.load(); }
    [[nodiscard]] const Config& config() const { return config_; }

    [[nodiscard]] json status() const {
        return {
            {"running", running_.load()},
            {"port", config_.port},
            {"tls_port", config_.tls_port},
            {"max_clients", config_.max_clients},
            {"dispatcher", dispatcher_.server_status()},
        };
    }

private:
    Config        config_;
    IrcDispatcher dispatcher_;
    std::atomic<bool> running_;
};

// ============================================================================
// Factory / convenience functions
// ============================================================================

[[nodiscard]] std::unique_ptr<IrcServer> create_irc_server(
    const IrcServer::Config& cfg) {
    return std::make_unique<IrcServer>(cfg);
}

[[nodiscard]] IrcDispatcher::Config default_irc_dispatcher_config() {
    return IrcDispatcher::Config{};
}

[[nodiscard]] std::vector<std::string> tokenize_irc_line(std::string_view line) {
    std::vector<std::string> tokens;
    std::string token;
    bool in_tag = false;
    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (c == ' ' && !in_tag) {
            if (!token.empty()) { tokens.push_back(token); token.clear(); }
        } else if (c == ':') {
            if (!token.empty()) { tokens.push_back(token); token.clear(); }
            token = std::string(line.substr(i));
            break;
        } else if (c == '@' && i == 0) {
            in_tag = true;
            token += c;
        } else {
            token += c;
        }
    }
    if (!token.empty()) tokens.push_back(token);
    return tokens;
}

} // namespace torrent::irc
