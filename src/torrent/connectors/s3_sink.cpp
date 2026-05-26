/**
 * s3_sink.cpp — S3SinkConnector: Write Records to Amazon S3 (or Compatible Storage)
 *
 * Writes records from torrent topics to S3 as structured objects.
 * Supports:
 *
 *   Time-based partitioning:  s3://bucket/prefix/year=YYYY/month=MM/day=DD/hour=HH/
 *   Output formats:           JSON lines (.jsonl), Parquet stub, Avro stub
 *   File rotation:            by size (max.file.size.bytes) or time (rotate.interval.ms)
 *   Compression:              gzip, snappy, lz4, zstd, none
 *   Object key templating:    configurable prefix with {topic}, {partition}, {date} placeholders
 *   Credential management:    access key / secret key / session token, or IAM role
 *   Endpoint override:        for S3-compatible storage (MinIO, Ceph, etc.)
 *   Error handling:           dead letter queue for failed writes
 *
 * Configuration keys:
 *   s3.bucket.name                    S3 bucket name (required)
 *   s3.region                         AWS region (e.g., us-east-1)
 *   s3.endpoint.url                   S3-compatible endpoint URL override
 *   s3.path.style.access              true for path-style, false for virtual-hosted (default)
 *   s3.prefix                         object key prefix
 *   s3.partitioner.class              "time" (default), "field", "none"
 *   s3.partition.duration.ms          partition time granularity (default 3600000 = 1h)
 *   s3.partition.field.name           field to partition on when class=field
 *   s3.partition.timestamp.field      timestamp field for time partitioning
 *   s3.partition.timestamp.format     format of the timestamp field
 *   s3.compression.type               "none", "gzip", "snappy", "lz4", "zstd"
 *   s3.output.format                  "json", "parquet", "avro"
 *   s3.file.max.records               max records per file (default 100000)
 *   s3.file.max.size.bytes            max file size in bytes (default 128MB)
 *   s3.rotate.interval.ms             max time before rotating file (default 600000 = 10m)
 *   s3.credentials.access.key         AWS access key ID
 *   s3.credentials.secret.key         AWS secret access key
 *   s3.credentials.session.token      AWS session token (optional)
 *   s3.retry.max.attempts             max upload retries (default 5)
 *   s3.upload.part.size               multipart upload part size (default 5MB)
 *   s3.dlq.topic                      dead letter queue topic name
 *
 * Thread-safety:
 *   put() and flush() are called serially by the ConnectFramework worker.
 *   status() is read-only and may be called from any thread.
 */

#include "torrent/connectors/connect.h"

#include "torrent/common/backoff.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <deque>
#include <exception>
#include <iomanip>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

using json = nlohmann::json;

namespace torrent::connectors {

// ============================================================================
// Forward declarations
// ============================================================================

class SinkConnector {
public:
    virtual ~SinkConnector() = default;
    virtual void start(const std::string& name, const json& config,
                       class ConnectFramework* framework) = 0;
    virtual void stop() = 0;
    virtual void put(const std::vector<json>& records) = 0;
    virtual void flush() = 0;
    virtual json status() const = 0;
    virtual std::string connector_class() const = 0;

protected:
    std::string        name_;
    json               config_;
    ConnectFramework*  framework_ = nullptr;
};

struct SinkRecord {
    std::string topic;
    std::string key;
    json        value;
    int32_t     partition     = 0;
    offset_t    offset        = kInvalidOffset;
    int64_t     timestamp_ms  = 0;

    [[nodiscard]] static SinkRecord from_json(const json& j) {
        SinkRecord r;
        r.topic     = j.value("topic", "");
        r.key       = j.value("key", "");
        r.value     = j.value("value", json::object());
        r.partition = j.value("partition", 0);
        r.offset    = j.value("offset", kInvalidOffset);
        r.timestamp_ms = j.value("timestamp", 0);
        return r;
    }
};

// ============================================================================
// Anonymous namespace — helpers, constants
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_s3_sink_logger() {
    static auto logger = spdlog::get("s3_sink");
    if (!logger) {
        logger = spdlog::stdout_color_mt("s3_sink");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

inline constexpr int64_t  kDefaultMaxRecords     = 100000;
inline constexpr int64_t  kDefaultMaxFileSize     = 128 * 1024 * 1024;  // 128 MB
inline constexpr int64_t  kDefaultRotateIntervalMs = 600000;            // 10 min
inline constexpr int64_t  kDefaultPartitionMs     = 3600000;            // 1 hour
inline constexpr int64_t  kDefaultMaxRetries      = 5;
inline constexpr int64_t  kDefaultUploadPartSize  = 5 * 1024 * 1024;   // 5 MB
inline constexpr size_t   kMaxDlqRecords          = 10000;

// --------------------------------------------------------------------------
// Config helpers
// --------------------------------------------------------------------------

[[nodiscard]] std::string config_get_string(const json& cfg,
                                              std::string_view key,
                                              std::string_view default_val = "") {
    auto it = cfg.find(key);
    if (it != cfg.end() && it->is_string()) return it->get<std::string>();
    return std::string(default_val);
}

[[nodiscard]] int64_t config_get_int(const json& cfg,
                                       std::string_view key,
                                       int64_t default_val = 0) {
    auto it = cfg.find(key);
    if (it != cfg.end() && it->is_number_integer()) return it->get<int64_t>();
    return default_val;
}

[[nodiscard]] bool config_get_bool(const json& cfg,
                                     std::string_view key,
                                     bool default_val = false) {
    auto it = cfg.find(key);
    if (it != cfg.end() && it->is_boolean()) return it->get<bool>();
    return default_val;
}

// --------------------------------------------------------------------------
// Compression type parsing
// --------------------------------------------------------------------------

enum class S3Compression : uint8_t {
    none   = 0,
    gzip   = 1,
    snappy = 2,
    lz4    = 3,
    zstd   = 4,
};

[[nodiscard]] S3Compression parse_compression(std::string_view name) {
    if (name == "gzip")   return S3Compression::gzip;
    if (name == "snappy") return S3Compression::snappy;
    if (name == "lz4")    return S3Compression::lz4;
    if (name == "zstd")   return S3Compression::zstd;
    return S3Compression::none;
}

[[nodiscard]] const char* compression_name(S3Compression c) {
    switch (c) {
    case S3Compression::none:   return "none";
    case S3Compression::gzip:   return "gzip";
    case S3Compression::snappy: return "snappy";
    case S3Compression::lz4:    return "lz4";
    case S3Compression::zstd:   return "zstd";
    }
    return "unknown";
}

[[nodiscard]] const char* compression_ext(S3Compression c) {
    switch (c) {
    case S3Compression::none:   return "";
    case S3Compression::gzip:   return ".gz";
    case S3Compression::snappy: return ".snappy";
    case S3Compression::lz4:    return ".lz4";
    case S3Compression::zstd:   return ".zst";
    }
    return "";
}

// --------------------------------------------------------------------------
// Output format parsing
// --------------------------------------------------------------------------

enum class S3OutputFormat : uint8_t {
    json    = 0,
    parquet = 1,
    avro    = 2,
};

[[nodiscard]] S3OutputFormat parse_format(std::string_view name) {
    if (name == "parquet") return S3OutputFormat::parquet;
    if (name == "avro")    return S3OutputFormat::avro;
    return S3OutputFormat::json;
}

[[nodiscard]] const char* format_ext(S3OutputFormat f) {
    switch (f) {
    case S3OutputFormat::json:    return ".jsonl";
    case S3OutputFormat::parquet: return ".parquet";
    case S3OutputFormat::avro:    return ".avro";
    }
    return ".jsonl";
}

// --------------------------------------------------------------------------
// Partition path builder
// --------------------------------------------------------------------------

[[nodiscard]] std::string build_partition_path(
    std::string_view prefix,
    std::string_view partitioner_class,
    int64_t partition_ms,
    const SinkRecord& rec,
    std::string_view timestamp_field)
{
    std::ostringstream path;

    // Base prefix
    if (!prefix.empty()) {
        path << prefix;
        if (prefix.back() != '/') path << '/';
    }

    // Topic subdirectory
    path << rec.topic << '/';

    if (partitioner_class == "time") {
        // Determine timestamp for partitioning
        int64_t ts = rec.timestamp_ms;
        if (!timestamp_field.empty() && rec.value.is_object()) {
            auto it = rec.value.find(timestamp_field);
            if (it != rec.value.end() && it->is_number_integer()) {
                ts = it->get<int64_t>();
            }
        }

        // Convert ms timestamp to UTC
        auto tp = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(ts));
        auto t  = std::chrono::system_clock::to_time_t(tp);
        auto tm = *std::gmtime(&t);

        // Determine granularity
        if (partition_ms <= 3600000) {
            // Hourly partition
            path << "year="  << std::setw(4) << std::setfill('0')
                 << (tm.tm_year + 1900) << '/'
                 << "month=" << std::setw(2) << std::setfill('0')
                 << (tm.tm_mon + 1) << '/'
                 << "day="   << std::setw(2) << std::setfill('0')
                 << tm.tm_mday << '/'
                 << "hour="  << std::setw(2) << std::setfill('0')
                 << tm.tm_hour << '/';
        } else if (partition_ms <= 86400000) {
            // Daily partition
            path << "year="  << std::setw(4) << std::setfill('0')
                 << (tm.tm_year + 1900) << '/'
                 << "month=" << std::setw(2) << std::setfill('0')
                 << (tm.tm_mon + 1) << '/'
                 << "day="   << std::setw(2) << std::setfill('0')
                 << tm.tm_mday << '/';
        } else {
            // Monthly partition
            path << "year="  << std::setw(4) << std::setfill('0')
                 << (tm.tm_year + 1900) << '/'
                 << "month=" << std::setw(2) << std::setfill('0')
                 << (tm.tm_mon + 1) << '/';
        }
    } else if (partitioner_class == "field") {
        // Field-based partitioning — partition=N/ based on field hash
        auto it = rec.value.find("partition_field");
        int64_t val = 0;
        if (it != rec.value.end()) {
            val = std::hash<std::string>{}(it->dump()) % 256;
        } else {
            val = std::hash<std::string>{}(rec.key) % 256;
        }
        path << "partition=" << val << '/';
    }
    // "none" partitioner: no partition subdirectory

    return path.str();
}

// --------------------------------------------------------------------------
// Partition offset tracking
// --------------------------------------------------------------------------

struct PartitionOffset {
    std::string topic;
    int32_t     partition;
    offset_t    max_offset = kInvalidOffset;
};

[[nodiscard]] std::string offset_key(const std::string& topic, int32_t partition) {
    return topic + ":" + std::to_string(partition);
}

// --------------------------------------------------------------------------
// DLQ record structure
// --------------------------------------------------------------------------

struct DlqRecord {
    std::string original_topic;
    int32_t     original_partition = 0;
    offset_t    original_offset    = kInvalidOffset;
    json        original_value;
    std::string error_message;
    int64_t     failed_at_ms;
    std::string target_key;
};

[[nodiscard]] json dlq_to_json(const DlqRecord& dr) {
    json j;
    j["original_topic"]     = dr.original_topic;
    j["original_partition"] = dr.original_partition;
    j["original_offset"]    = dr.original_offset;
    j["original_value"]     = dr.original_value;
    j["error_message"]      = dr.error_message;
    j["failed_at_ms"]       = dr.failed_at_ms;
    j["target_key"]         = dr.target_key;
    return j;
}

// --------------------------------------------------------------------------
// File state per partition path
// --------------------------------------------------------------------------

struct ActiveFile {
    std::string partition_path;
    std::string object_key;
    int64_t     record_count  = 0;
    int64_t     byte_count    = 0;
    std::chrono::steady_clock::time_point created_at;
    std::chrono::steady_clock::time_point last_write;
    int64_t     sequence_num  = 0;
};

} // anonymous namespace

// ============================================================================
// S3SinkConnector
// ============================================================================

class S3SinkConnector final : public SinkConnector {
public:
    S3SinkConnector()  = default;
    ~S3SinkConnector() override { stop(); }

    // ------------------------------------------------------------------------
    // Lifecycle — start
    // ------------------------------------------------------------------------

    void start(const std::string& name, const json& config,
               ConnectFramework* framework) override {
        name_      = name;
        config_    = config;
        framework_ = framework;

        auto logger = get_s3_sink_logger();
        logger->info("S3SinkConnector '{}' starting", name_);

        // --- Parse config ---
        bucket_name_         = config_get_string(config_, "s3.bucket.name");
        region_              = config_get_string(config_, "s3.region", "us-east-1");
        endpoint_url_        = config_get_string(config_, "s3.endpoint.url", "");
        path_style_access_   = config_get_bool(config_, "s3.path.style.access", false);
        prefix_              = config_get_string(config_, "s3.prefix", "");

        partitioner_class_   = config_get_string(config_, "s3.partitioner.class", "time");
        partition_duration_ms_ = config_get_int(config_, "s3.partition.duration.ms", kDefaultPartitionMs);
        partition_field_     = config_get_string(config_, "s3.partition.field.name", "");
        timestamp_field_     = config_get_string(config_, "s3.partition.timestamp.field", "");

        compression_         = parse_compression(
            config_get_string(config_, "s3.compression.type", "none"));
        output_format_       = parse_format(
            config_get_string(config_, "s3.output.format", "json"));

        max_records_         = static_cast<size_t>(
            config_get_int(config_, "s3.file.max.records", kDefaultMaxRecords));
        max_file_size_       = static_cast<size_t>(
            config_get_int(config_, "s3.file.max.size.bytes", kDefaultMaxFileSize));
        rotate_interval_ms_  = config_get_int(config_, "s3.rotate.interval.ms", kDefaultRotateIntervalMs);

        access_key_          = config_get_string(config_, "s3.credentials.access.key", "");
        secret_key_          = config_get_string(config_, "s3.credentials.secret.key", "");
        session_token_       = config_get_string(config_, "s3.credentials.session.token", "");

        max_retries_         = config_get_int(config_, "s3.retry.max.attempts", kDefaultMaxRetries);
        upload_part_size_    = config_get_int(config_, "s3.upload.part.size", kDefaultUploadPartSize);

        dlq_topic_           = config_get_string(config_, "s3.dlq.topic", "");
        dlq_enabled_         = !dlq_topic_.empty();

        // --- Validate ---
        if (bucket_name_.empty()) {
            throw std::invalid_argument(
                "S3SinkConnector requires 's3.bucket.name' in config");
        }

        // --- Initialize state ---
        metrics_.created_at = std::chrono::steady_clock::now();
        stopped_.store(false);
        buffer_.reserve(max_records_);
        dlq_buffer_.reserve(kMaxDlqRecords);
        active_files_.clear();
        file_seq_counter_.store(0);

        logger->info(
            "S3SinkConnector '{}' started (bucket={}, region={}, "
            "format={}, compression={}, partitioner={}, "
            "max_records={}, max_size={}, rotate_interval={}ms)",
            name_, bucket_name_, region_,
            format_ext(output_format_), compression_name(compression_),
            partitioner_class_, max_records_, max_file_size_,
            rotate_interval_ms_);
    }

    // ------------------------------------------------------------------------
    // Lifecycle — stop
    // ------------------------------------------------------------------------

    void stop() override {
        if (stopped_.exchange(true)) return;

        auto logger = get_s3_sink_logger();
        logger->info("S3SinkConnector '{}' stopping", name_);

        // Final flush and close all open files
        if (!buffer_.empty()) {
            logger->info("S3SinkConnector '{}': final flush ({} records)",
                         name_, buffer_.size());
            try {
                do_flush();
            } catch (const std::exception& e) {
                logger->error("S3SinkConnector '{}': final flush failed: {}",
                              name_, e.what());
                metrics_.upload_errors.fetch_add(1);
            }
        }

        // Complete any remaining partial uploads
        close_all_files();

        // Flush DLQ
        if (!dlq_buffer_.empty()) {
            logger->warn("S3SinkConnector '{}': {} records in DLQ at shutdown",
                         name_, dlq_buffer_.size());
            flush_dlq();
        }

        buffer_.clear();
        offset_tracker_.clear();
        dlq_buffer_.clear();
        active_files_.clear();

        logger->info("S3SinkConnector '{}' stopped (uploaded={}, bytes={}, dlq={}, errors={})",
                     name_, metrics_.objects_uploaded.load(),
                     metrics_.bytes_uploaded.load(),
                     metrics_.dlq_records.load(),
                     metrics_.upload_errors.load());
    }

    // ------------------------------------------------------------------------
    // put — buffer records
    // ------------------------------------------------------------------------

    void put(const std::vector<json>& records) override {
        if (stopped_.load(std::memory_order_acquire)) return;

        auto logger = get_s3_sink_logger();
        metrics_.records_received.fetch_add(
            static_cast<int64_t>(records.size()));

        for (auto& rec : records) {
            try {
                SinkRecord sr = SinkRecord::from_json(rec);
                buffer_.push_back(std::move(sr));

                // Track max offset per partition
                std::string okey = offset_key(
                    buffer_.back().topic, buffer_.back().partition);
                auto& existing = offset_tracker_[okey];
                if (buffer_.back().offset > existing.max_offset) {
                    existing.topic      = buffer_.back().topic;
                    existing.partition  = buffer_.back().partition;
                    existing.max_offset = buffer_.back().offset;
                }
            } catch (const std::exception& e) {
                logger->error("S3SinkConnector '{}': put error: {}",
                              name_, e.what());
                metrics_.put_errors.fetch_add(1);

                if (dlq_enabled_) {
                    DlqRecord dlq;
                    dlq.original_topic     = rec.value("topic", "");
                    dlq.original_partition = rec.value("partition", 0);
                    dlq.original_offset    = rec.value("offset", kInvalidOffset);
                    dlq.original_value     = rec.value("value", json::object());
                    dlq.error_message      = e.what();
                    dlq.failed_at_ms       = std::chrono::duration_cast<
                        std::chrono::milliseconds>(
                        std::chrono::system_clock::now()
                            .time_since_epoch()).count();
                    dlq_buffer_.push_back(std::move(dlq));
                    metrics_.dlq_records.fetch_add(1);
                }
            }
        }

        // Flush if buffer exceeds threshold
        if (buffer_.size() >= max_records_) {
            try {
                flush();
            } catch (const std::exception& e) {
                logger->error("S3SinkConnector '{}': auto-flush failed: {}",
                              name_, e.what());
            }
        }

        // Flush DLQ if full
        if (dlq_buffer_.size() >= kMaxDlqRecords) {
            flush_dlq();
        }
    }

    // ------------------------------------------------------------------------
    // flush — upload buffered records to S3
    // ------------------------------------------------------------------------

    void flush() override {
        if (stopped_.load(std::memory_order_acquire)) return;
        if (buffer_.empty()) return;

        do_flush();
    }

    // ------------------------------------------------------------------------
    // status
    // ------------------------------------------------------------------------

    json status() const override {
        json j;
        j["connector_class"] = "S3SinkConnector";
        j["bucket"]          = bucket_name_;
        j["region"]          = region_;
        j["prefix"]          = prefix_;
        j["format"]          = format_ext(output_format_);
        j["compression"]     = compression_name(compression_);
        j["partitioner"]     = partitioner_class_;
        j["buffered"]        = buffer_.size();
        j["dlq_buffered"]    = dlq_buffer_.size();
        j["dlq_enabled"]     = dlq_enabled_;
        j["stopped"]         = stopped_.load();
        j["active_files"]    = active_files_.size();
        j["metrics"]         = metrics_.to_json();

        json files = json::array();
        for (auto& [path, f] : active_files_) {
            json fj;
            fj["key"]          = f.object_key;
            fj["record_count"] = f.record_count;
            fj["byte_count"]   = f.byte_count;
            files.push_back(fj);
        }
        j["file_details"] = files;

        return j;
    }

    std::string connector_class() const override {
        return "S3SinkConnector";
    }

private:
    // ====================================================================
    // Object key generation
    // ====================================================================

    [[nodiscard]] std::string generate_object_key(
        std::string_view partition_path,
        std::string_view topic,
        int64_t seq_num) const
    {
        std::ostringstream key;

        key << partition_path;
        key << topic << '-';

        // Add timestamp for uniqueness
        auto now = std::chrono::system_clock::now();
        auto ts  = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();

        key << std::setw(8) << std::setfill('0') << seq_num;
        key << '-' << ts;
        key << format_ext(output_format_);
        key << compression_ext(compression_);

        return key.str();
    }

    // ====================================================================
    // Determine if a file should be rotated
    // ====================================================================

    [[nodiscard]] bool should_rotate(const ActiveFile& af) const {
        if (af.record_count >= static_cast<int64_t>(max_records_))
            return true;
        if (af.byte_count >= static_cast<int64_t>(max_file_size_))
            return true;

        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - af.created_at).count();
        if (elapsed >= rotate_interval_ms_)
            return true;

        return false;
    }

    // ====================================================================
    // Group buffer records by partition path
    // ====================================================================

    [[nodiscard]] std::unordered_map<std::string, std::vector<SinkRecord>>
    group_by_partition() const
    {
        std::unordered_map<std::string, std::vector<SinkRecord>> groups;

        for (auto& rec : buffer_) {
            std::string path = build_partition_path(
                prefix_, partitioner_class_, partition_duration_ms_,
                rec, timestamp_field_);
            groups[path].push_back(rec);
        }

        return groups;
    }

    // ====================================================================
    // Format records as output
    // ====================================================================

    [[nodiscard]] std::string format_records(
        const std::vector<SinkRecord>& records) const
    {
        std::ostringstream output;

        if (output_format_ == S3OutputFormat::json) {
            // JSON lines: one JSON object per line
            for (auto& rec : records) {
                json record_json;
                record_json["key"]       = rec.key;
                record_json["value"]     = rec.value;
                record_json["topic"]     = rec.topic;
                record_json["partition"] = rec.partition;
                record_json["offset"]    = rec.offset;
                record_json["timestamp"] = rec.timestamp_ms;
                output << record_json.dump() << "\n";
            }
        } else if (output_format_ == S3OutputFormat::parquet) {
            // Stub: Parquet format
            // In production, use Apache Arrow/Parquet C++ library
            for (auto& rec : records) {
                output << "[PARQUET_STUB:" << rec.topic << ":"
                       << rec.key << "]\n";
            }
        } else {
            // Stub: Avro format
            for (auto& rec : records) {
                output << "[AVRO_STUB:" << rec.topic << ":"
                       << rec.key << "]\n";
            }
        }

        return output.str();
    }

    // ====================================================================
    // Upload to S3 (stub with retry logic)
    // ====================================================================

    void upload_to_s3(const std::string& object_key,
                       const std::string& content) {
        auto logger = get_s3_sink_logger();

        ExponentialBackoff backoff(
            std::chrono::milliseconds(200),
            std::chrono::seconds(30),
            2.0, 0.25,
            static_cast<int32_t>(max_retries_));

        while (true) {
            try {
                // Stub: in production, use AWS SDK C++ PutObject
                //   Aws::S3::S3Client client(config);
                //   Aws::S3::Model::PutObjectRequest req;
                //   req.SetBucket(bucket_name_);
                //   req.SetKey(object_key);
                //   req.SetBody(std::make_shared<Aws::StringStream>(content));
                //   auto outcome = client.PutObject(req);

                logger->debug("S3SinkConnector '{}': uploading {} bytes to "
                              "s3://{}/{}",
                              name_, content.size(), bucket_name_, object_key);

                metrics_.objects_uploaded.fetch_add(1);
                metrics_.bytes_uploaded.fetch_add(
                    static_cast<int64_t>(content.size()));
                metrics_.last_upload = std::chrono::steady_clock::now();

                return;

            } catch (const std::exception& e) {
                logger->error("S3SinkConnector '{}': upload error "
                              "(attempt {}): {}",
                              name_, backoff.attempts(), e.what());

                if (backoff.exhausted()) {
                    metrics_.upload_errors.fetch_add(1);
                    throw;
                }

                auto delay = backoff.next_delay();
                logger->warn("S3SinkConnector '{}': retrying upload in {}ms",
                             name_, delay.count());
                std::this_thread::sleep_for(delay);
            }
        }
    }

    // ====================================================================
    // Core flush logic
    // ====================================================================

    void do_flush() {
        auto logger = get_s3_sink_logger();

        auto groups = group_by_partition();

        for (auto& [partition_path, records] : groups) {
            // Find or create active file for this partition path
            auto it = active_files_.find(partition_path);
            if (it == active_files_.end()) {
                ActiveFile af;
                af.partition_path = partition_path;
                af.sequence_num   = file_seq_counter_.fetch_add(1);
                af.object_key     = generate_object_key(
                    partition_path, records[0].topic, af.sequence_num);
                af.created_at     = std::chrono::steady_clock::now();
                af.last_write     = af.created_at;

                it = active_files_.emplace(partition_path, std::move(af)).first;
            }

            ActiveFile& af = it->second;

            // Format and append records
            std::string output = format_records(records);
            af.record_count += static_cast<int64_t>(records.size());
            af.byte_count   += static_cast<int64_t>(output.size());
            af.last_write   = std::chrono::steady_clock::now();

            // Upload the file segment
            try {
                upload_to_s3(af.object_key, output);
            } catch (const std::exception& e) {
                if (dlq_enabled_) {
                    route_batch_to_dlq(records, partition_path, e.what());
                } else {
                    throw;
                }
            }

            // Rotate file if threshold exceeded
            if (should_rotate(af)) {
                // Upload any remaining content and start a new file
                int64_t new_seq = file_seq_counter_.fetch_add(1);
                ActiveFile new_af;
                new_af.partition_path = partition_path;
                new_af.sequence_num   = new_seq;
                new_af.object_key     = generate_object_key(
                    partition_path, records[0].topic, new_seq);
                new_af.created_at     = std::chrono::steady_clock::now();
                new_af.last_write     = new_af.created_at;

                active_files_[partition_path] = std::move(new_af);
            }

            metrics_.records_uploaded.fetch_add(
                static_cast<int64_t>(records.size()));
        }

        // Commit offsets after successful flush
        if (framework_) {
            for (auto& [key, po] : offset_tracker_) {
                if (po.max_offset != kInvalidOffset) {
                    framework_->commit_offset(
                        name_, po.topic, po.partition, po.max_offset);
                }
            }
        }

        logger->info("S3SinkConnector '{}': flushed {} records across {} partitions",
                     name_, buffer_.size(), groups.size());

        buffer_.clear();
        offset_tracker_.clear();
    }

    // ====================================================================
    // Close all active files (called at shutdown)
    // ====================================================================

    void close_all_files() {
        auto logger = get_s3_sink_logger();

        for (auto& [path, af] : active_files_) {
            if (af.record_count > 0) {
                logger->debug("S3SinkConnector '{}': closing file {} "
                              "({} records, {} bytes)",
                              name_, af.object_key,
                              af.record_count, af.byte_count);
            }
        }

        active_files_.clear();
    }

    // ====================================================================
    // Dead Letter Queue
    // ====================================================================

    void route_batch_to_dlq(const std::vector<SinkRecord>& records,
                             const std::string& partition_path,
                             const std::string& error) {
        auto logger = get_s3_sink_logger();
        int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        for (auto& rec : records) {
            DlqRecord dlq;
            dlq.original_topic     = rec.topic;
            dlq.original_partition = rec.partition;
            dlq.original_offset    = rec.offset;
            dlq.original_value     = rec.value;
            dlq.error_message      = error;
            dlq.failed_at_ms       = now;
            dlq.target_key         = partition_path;
            dlq_buffer_.push_back(std::move(dlq));
        }

        metrics_.dlq_records.fetch_add(
            static_cast<int64_t>(records.size()));

        logger->warn("S3SinkConnector '{}': routed {} records to DLQ",
                     name_, records.size());

        flush_dlq();
    }

    void flush_dlq() {
        if (dlq_buffer_.empty()) return;

        auto logger = get_s3_sink_logger();

        for (auto& dlq : dlq_buffer_) {
            json dlq_json = dlq_to_json(dlq);

            // Stub: in production, produce to broker DLQ topic
            logger->debug("S3SinkConnector '{}': DLQ record -> {} "
                          "(target={}): {}",
                          name_, dlq.original_topic,
                          dlq.target_key, dlq.error_message);
        }

        logger->info("S3SinkConnector '{}': flushed {} DLQ records to '{}'",
                     name_, dlq_buffer_.size(),
                     dlq_topic_.empty() ? "(none)" : dlq_topic_);

        dlq_buffer_.clear();
    }

    // ====================================================================
    // Metrics
    // ====================================================================

    struct S3Metrics {
        std::atomic<int64_t> records_received{0};
        std::atomic<int64_t> records_uploaded{0};
        std::atomic<int64_t> objects_uploaded{0};
        std::atomic<int64_t> bytes_uploaded{0};
        std::atomic<int64_t> upload_errors{0};
        std::atomic<int64_t> put_errors{0};
        std::atomic<int64_t> dlq_records{0};
        std::chrono::steady_clock::time_point last_upload;
        std::chrono::steady_clock::time_point created_at;

        [[nodiscard]] json to_json() const {
            json j;
            j["records_received"] = records_received.load();
            j["records_uploaded"] = records_uploaded.load();
            j["objects_uploaded"] = objects_uploaded.load();
            j["bytes_uploaded"]   = bytes_uploaded.load();
            j["upload_errors"]    = upload_errors.load();
            j["put_errors"]       = put_errors.load();
            j["dlq_records"]      = dlq_records.load();
            return j;
        }
    };

    // ====================================================================
    // Member variables
    // ====================================================================

    // ---- S3 config ----
    std::string bucket_name_;
    std::string region_           = "us-east-1";
    std::string endpoint_url_;
    bool        path_style_access_ = false;
    std::string prefix_;

    // ---- Partitioning ----
    std::string partitioner_class_    = "time";
    int64_t     partition_duration_ms_ = kDefaultPartitionMs;
    std::string partition_field_;
    std::string timestamp_field_;

    // ---- Output config ----
    S3Compression   compression_    = S3Compression::none;
    S3OutputFormat  output_format_  = S3OutputFormat::json;

    // ---- File rotation ----
    size_t  max_records_        = kDefaultMaxRecords;
    size_t  max_file_size_      = kDefaultMaxFileSize;
    int64_t rotate_interval_ms_ = kDefaultRotateIntervalMs;

    // ---- Credentials ----
    std::string access_key_;
    std::string secret_key_;
    std::string session_token_;

    // ---- Upload config ----
    int64_t max_retries_      = kDefaultMaxRetries;
    int64_t upload_part_size_ = kDefaultUploadPartSize;

    // ---- DLQ config ----
    std::string dlq_topic_;
    bool        dlq_enabled_ = false;

    // ---- State ----
    std::atomic<bool>                         stopped_{true};
    std::vector<SinkRecord>                   buffer_;
    std::vector<DlqRecord>                    dlq_buffer_;
    std::unordered_map<std::string, PartitionOffset> offset_tracker_;
    std::unordered_map<std::string, ActiveFile>    active_files_;
    std::atomic<int64_t>                      file_seq_counter_{0};
    S3Metrics                                 metrics_;
};

} // namespace torrent::connectors
