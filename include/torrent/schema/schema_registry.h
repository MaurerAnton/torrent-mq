#pragma once
#include <memory>
#include <string>
#include <vector>
#include <optional>
#include <torrent/common/types.h>

namespace torrent::schema {

class SchemaRegistry {
public:
    explicit SchemaRegistry(class broker::BrokerServer& s);
    ~SchemaRegistry();
    SchemaRegistry(const SchemaRegistry&) = delete;
    SchemaRegistry& operator=(const SchemaRegistry&) = delete;
    SchemaRegistry(SchemaRegistry&&) noexcept;
    SchemaRegistry& operator=(SchemaRegistry&&) noexcept;

    result<int32_t> register_schema(const std::string& subject, const std::string& schema, const std::string& format);
    std::optional<std::string> get_schema(const std::string& subject, int32_t version) const;
    std::optional<std::string> get_schema_by_id(int32_t schema_id) const;
    std::vector<int32_t> list_versions(const std::string& subject) const;
    bool check_compatibility(const std::string& subject, const std::string& new_schema) const;

    /// Enable/disable RocksDB persistence layer (off by default).
    void enable_persistence(const std::string& db_path);
    [[nodiscard]] bool is_persistence_enabled() const noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
    broker::BrokerServer* server_;
};

}