#pragma once
#include <memory>
#include <string>
#include <torrent/common/types.h>

namespace torrent::broker {
class TransactionCoordinator {
public:
    explicit TransactionCoordinator(class BrokerServer& s) : server_(&s) {}
    result<producer_id_t> init_producer_id(const std::string& transactional_id, int64_t timeout_ms);
    result<void> add_partitions_to_txn(producer_id_t pid, producer_epoch_t epoch, const std::vector<std::pair<std::string,partition_id_t>>& partitions);
    result<void> end_txn(producer_id_t pid, producer_epoch_t epoch, bool committed);
private:
    BrokerServer* server_;
};
}