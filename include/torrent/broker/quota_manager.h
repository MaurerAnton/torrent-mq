#pragma once
#include <memory>
#include <torrent/common/types.h>

namespace torrent::broker {
class QuotaManager {
public:
    explicit QuotaManager(class BrokerServer& s);
    ~QuotaManager();
    bool check_produce_quota(const std::string& client_id) noexcept;
    bool check_fetch_quota(const std::string& client_id) noexcept;
    bool check_request_quota(const std::string& client_id) noexcept;
    void record_produce(const std::string& client_id, byte_count_t bytes);
    void record_fetch(const std::string& client_id, byte_count_t bytes);
private:
    BrokerServer* server_;
};
} // namespace torrent::broker
