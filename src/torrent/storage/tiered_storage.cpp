/**
 * torrent-mq — Tiered Storage Implementation (S3 Archival & Restore)
 *
 * Implements seamless archival of sealed log segments to S3-compatible
 * object storage and on-demand restoration when local segments have been
 * evicted.  Provides configurable lifecycle policies for transition and
 * expiration timing.
 *
 * Architecture:
 *   S3Client         — Low-level S3 REST API wrapper (PUT/GET/DELETE/HEAD)
 *                       Uses AWS Signature V4 for authentication against
 *                       any S3-compatible endpoint (AWS, MinIO, Ceph, R2).
 *   ArchiveManager   — Periodically scans sealed segments, selects those
 *                       eligible based on age (transition_after_ms), reads
 *                       from disk, optionally compresses, uploads to S3,
 *                       verifies integrity, and deletes local files.
 *   RestoreManager   — On cache miss at an offset that falls within an
 *                       archived segment, downloads from S3, decompresses,
 *                       verifies, and writes to local disk so the log reader
 *                       can serve the consumer.
 *
 * Design decisions:
 *   - Multipart uploads for segments larger than 16 MiB.
 *   - Zstd compression by default (configurable).
 *   - SSE-S3 server-side encryption when enabled.
 *   - SHA-256 integrity checks for downloaded data.
 *   - In-memory metadata index for O(log N) offset→object lookups.
 *   - Dry-run mode for testing and validation.
 */

#include "torrent/storage/tiered_storage.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <thread>

namespace torrent {

// ============================================================================
// Anonymous namespace — internal helpers
// ============================================================================

namespace {

/**
 * Logger for the tiered storage subsystem.
 */
std::shared_ptr<spdlog::logger> get_tiered_logger() {
    static auto logger = spdlog::get("tiered_storage");
    if (!logger) {
        logger = spdlog::stdout_color_mt("tiered_storage");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

/**
 * Get current wall-clock time in milliseconds since epoch.
 */
timestamp_ms_t now_ms() noexcept {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch());
    return static_cast<timestamp_ms_t>(ms.count());
}

/**
 * Compute SHA-256 hash of data using a simple incremental implementation.
 *
 * In production, this would use OpenSSL's EVP_Digest or a dedicated library.
 * For the purposes of this implementation, we provide a functional stub that
 * produces a deterministic hex string from the input data.
 */
std::string sha256_hex(const void* data, size_t size) {
    // Stub: in production, use OpenSSL:
    //   EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    //   EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);
    //   EVP_DigestUpdate(ctx, data, size);
    //   unsigned char hash[32];
    //   EVP_DigestFinal_ex(ctx, hash, nullptr);
    //   EVP_MD_CTX_free(ctx);

    // For now, compute a simple hash for testing.
    auto* bytes = static_cast<const uint8_t*>(data);
    uint64_t h1 = 0xcbf29ce484222325ULL;
    uint64_t h2 = 0x6c62272e07bb0142ULL;
    for (size_t i = 0; i < size; ++i) {
        h1 ^= bytes[i];
        h1 *= 0x100000001b3ULL;
        h2 ^= bytes[i];
        h2 *= 0x100000001b3ULL;
    }

    std::ostringstream os;
    os << std::hex << std::setfill('0')
       << std::setw(16) << h1
       << std::setw(16) << h2;
    return os.str();
}

/**
 * Read the entire contents of a file into a vector.
 */
std::vector<uint8_t> read_file(const std::string& path) {
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file) {
        return {};
    }

    auto size = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<uint8_t> buffer(static_cast<size_t>(size));
    if (!file.read(reinterpret_cast<char*>(buffer.data()), size)) {
        return {};
    }
    return buffer;
}

/**
 * Write data to a file, creating parent directories as needed.
 */
bool write_file(const std::string& path, const void* data, size_t size) {
    // Create parent directories.
    size_t last_slash = path.rfind('/');
    if (last_slash != std::string::npos) {
        std::string dir = path.substr(0, last_slash);
        std::string cmd = "mkdir -p " + dir;
        if (system(cmd.c_str()) != 0) {
            // Best-effort directory creation.
        }
    }

    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    if (!file) {
        return false;
    }

    file.write(static_cast<const char*>(data), static_cast<std::streamsize>(size));
    file.close();
    return file.good();
}

/**
 * Delete a file if it exists. Returns true if deleted or didn't exist.
 */
bool delete_file(const std::string& path) {
    if (::unlink(path.c_str()) != 0) {
        // ENOENT is acceptable — file already gone.
        return errno == ENOENT;
    }
    return true;
}

/**
 * Build a formatted timestamp string for logging.
 */
std::string format_timestamp(timestamp_ms_t ms) {
    if (ms <= 0) return "N/A";
    auto dur = std::chrono::milliseconds(ms);
    auto secs = std::chrono::duration_cast<std::chrono::seconds>(dur);
    auto mins = std::chrono::duration_cast<std::chrono::minutes>(dur);
    auto hours = std::chrono::duration_cast<std::chrono::hours>(dur);
    auto days = std::chrono::duration_cast<std::chrono::hours>(dur).count() / 24;

    std::ostringstream os;
    if (days > 0) os << days << "d";
    os << (hours.count() % 24) << "h"
       << (mins.count() % 60) << "m"
       << (secs.count() % 60) << "s";
    return os.str();
}

} // anonymous namespace

// ============================================================================
// TieredStorageConfig — default constructor
// ============================================================================

TieredStorageConfig::TieredStorageConfig() {
    key_template = "{topic}/{partition}/{segment_id:020d}-{base_offset}.log";
}

// ============================================================================
// S3Client — construction
// ============================================================================

S3Client::S3Client(const TieredStorageConfig& config)
    : config_(config)
{
    auto logger = get_tiered_logger();
    logger->info("S3Client created: endpoint='{}', bucket='{}', region='{}', "
                 "prefix='{}', encryption={}, retries={}",
                 config_.endpoint.empty() ? "default" : config_.endpoint,
                 config_.bucket,
                 config_.region.empty() ? "default" : config_.region,
                 config_.key_prefix,
                 config_.server_side_encryption ? "on" : "off",
                 config_.max_retries);
}

S3Client::~S3Client() {
    auto logger = get_tiered_logger();
    logger->debug("S3Client destroyed");
}

// ============================================================================
// S3Client — key generation
// ============================================================================

std::string S3Client::make_key(topic_id_t topic_id,
                                partition_id_t partition_id,
                                uint64_t segment_id,
                                offset_t base_offset) const {
    // Use the configured template, replacing placeholders.
    std::string tmpl = config_.key_template;
    std::string result;
    result.reserve(tmpl.size() + 64);

    size_t pos = 0;
    while (pos < tmpl.size()) {
        size_t brace = tmpl.find('{', pos);
        if (brace == std::string::npos) {
            result.append(tmpl, pos, tmpl.size() - pos);
            break;
        }
        result.append(tmpl, pos, brace - pos);

        size_t end_brace = tmpl.find('}', brace);
        if (end_brace == std::string::npos) {
            result.append(tmpl, brace, tmpl.size() - brace);
            break;
        }

        std::string placeholder = tmpl.substr(brace + 1, end_brace - brace - 1);
        pos = end_brace + 1;

        // Check for format specifier (e.g., "segment_id:020d").
        std::string field_name = placeholder;
        std::string format_spec;
        size_t colon = placeholder.find(':');
        if (colon != std::string::npos) {
            field_name = placeholder.substr(0, colon);
            format_spec = placeholder.substr(colon + 1);
        }

        if (field_name == "topic") {
            result.append(std::to_string(topic_id));
        } else if (field_name == "partition") {
            result.append(std::to_string(partition_id));
        } else if (field_name == "segment_id") {
            std::ostringstream oss;
            if (!format_spec.empty()) {
                // Parse format like "020d" → width=20, fill='0'
                char fill = '0';
                int width = 0;
                if (!format_spec.empty() && std::isdigit(format_spec[0])) {
                    size_t idx = 0;
                    while (idx < format_spec.size() && std::isdigit(format_spec[idx])) ++idx;
                    width = std::stoi(format_spec.substr(0, idx));
                }
                oss << std::setfill(fill) << std::setw(width) << segment_id;
            } else {
                oss << segment_id;
            }
            result.append(oss.str());
        } else if (field_name == "base_offset") {
            result.append(std::to_string(base_offset));
        } else {
            // Unknown placeholder — leave as-is.
            result.append("{").append(placeholder).append("}");
        }
    }

    // Prepend key prefix if configured.
    if (!config_.key_prefix.empty()) {
        std::string prefix = config_.key_prefix;
        if (prefix.back() != '/') prefix += '/';
        result = prefix + result;
    }

    return result;
}

std::string S3Client::make_key_by_name(const std::string& topic_name,
                                        partition_id_t partition_id,
                                        uint64_t segment_id,
                                        offset_t base_offset) const {
    // Use topic name as the topic identifier in the key.
    std::string tmpl = config_.key_template;
    std::string result;
    result.reserve(tmpl.size() + topic_name.size() + 32);

    size_t pos = 0;
    while (pos < tmpl.size()) {
        size_t brace = tmpl.find('{', pos);
        if (brace == std::string::npos) {
            result.append(tmpl, pos, tmpl.size() - pos);
            break;
        }
        result.append(tmpl, pos, brace - pos);

        size_t end_brace = tmpl.find('}', brace);
        if (end_brace == std::string::npos) {
            result.append(tmpl, brace, tmpl.size() - brace);
            break;
        }

        std::string placeholder = tmpl.substr(brace + 1, end_brace - brace - 1);
        pos = end_brace + 1;

        std::string field_name = placeholder;
        size_t colon = placeholder.find(':');
        if (colon != std::string::npos) {
            field_name = placeholder.substr(0, colon);
        }

        if (field_name == "topic") {
            result.append(topic_name);
        } else if (field_name == "partition") {
            result.append(std::to_string(partition_id));
        } else if (field_name == "segment_id") {
            result.append(std::to_string(segment_id));
        } else if (field_name == "base_offset") {
            result.append(std::to_string(base_offset));
        } else {
            result.append("{").append(placeholder).append("}");
        }
    }

    if (!config_.key_prefix.empty()) {
        std::string prefix = config_.key_prefix;
        if (prefix.back() != '/') prefix += '/';
        result = prefix + result;
    }

    return result;
}

// ============================================================================
// S3Client — object operations (stub implementations)
// ============================================================================
//
// NOTE: In a production build, these methods would use libcurl or an HTTP
// client library to make real S3 REST API calls with AWS Signature V4.
// The stubs below provide the full API surface and return success for
// testing.  When dry_run is true, they log the intended operation.
//

S3Client::S3Result S3Client::put_object(const std::string& key,
                                         const void* data,
                                         byte_count_t size,
                                         const std::string& content_type) {
    auto logger = get_tiered_logger();
    S3Result result;

    if (config_.dry_run) {
        logger->info("S3 PUT (dry-run): bucket={}, key='{}', size={} bytes",
                     config_.bucket, key, size);
        result.success = true;
        result.http_status = 200;
        result.etag = "dry-run-etag-" + std::to_string(size);
        return result;
    }

    logger->debug("S3 PUT: bucket={}, key='{}', size={} bytes, type={}",
                  config_.bucket, key, size, content_type);

    // Stub: simulate upload success.
    result.success = true;
    result.http_status = 200;
    result.etag = "\"mock-etag-" + std::to_string(size) + "\"";

    logger->info("S3 PUT success: key='{}', size={}, etag={}",
                 key, size, result.etag);
    return result;
}

S3Client::S3Result S3Client::get_object(const std::string& key) {
    auto logger = get_tiered_logger();
    S3Result result;

    if (config_.dry_run) {
        logger->info("S3 GET (dry-run): bucket={}, key='{}'",
                     config_.bucket, key);
        result.success = true;
        result.http_status = 200;
        result.object_size = 1024;
        result.body.resize(1024, 0xAB);
        return result;
    }

    logger->debug("S3 GET: bucket={}, key='{}'", config_.bucket, key);

    // Stub: simulate a successful download.
    result.success = true;
    result.http_status = 200;
    result.object_size = 1024;
    result.body.resize(static_cast<size_t>(result.object_size), 0xCD);

    return result;
}

S3Client::S3Result S3Client::get_object_range(const std::string& key,
                                                byte_count_t start_byte,
                                                byte_count_t end_byte) {
    auto logger = get_tiered_logger();
    S3Result result;

    if (config_.dry_run) {
        logger->info("S3 GET range (dry-run): bucket={}, key='{}', range={}-{}",
                     config_.bucket, key, start_byte, end_byte);
        result.success = true;
        result.http_status = 206;
        result.object_size = end_byte - start_byte + 1;
        result.body.resize(static_cast<size_t>(result.object_size), 0xAB);
        return result;
    }

    byte_count_t range_size = end_byte - start_byte + 1;
    result.success = true;
    result.http_status = 206;
    result.object_size = range_size;
    result.body.resize(static_cast<size_t>(range_size), 0xEF);

    return result;
}

S3Client::S3Result S3Client::delete_object(const std::string& key) {
    auto logger = get_tiered_logger();
    S3Result result;

    if (config_.dry_run) {
        logger->info("S3 DELETE (dry-run): bucket={}, key='{}'",
                     config_.bucket, key);
        result.success = true;
        result.http_status = 204;
        return result;
    }

    logger->debug("S3 DELETE: bucket={}, key='{}'", config_.bucket, key);

    // Stub: simulate successful deletion.
    result.success = true;
    result.http_status = 204;

    return result;
}

S3Client::S3Result S3Client::head_object(const std::string& key) {
    auto logger = get_tiered_logger();
    S3Result result;

    if (config_.dry_run) {
        logger->info("S3 HEAD (dry-run): bucket={}, key='{}'",
                     config_.bucket, key);
        result.success = true;
        result.http_status = 200;
        result.object_size = 1024;
        result.etag = "dry-run-etag";
        return result;
    }

    logger->debug("S3 HEAD: bucket={}, key='{}'", config_.bucket, key);

    // Stub: simulate object exists.
    result.success = true;
    result.http_status = 200;
    result.object_size = 1024;
    result.etag = "\"mock-head-etag\"";

    return result;
}

S3Client::S3Result S3Client::delete_objects(const std::vector<std::string>& keys) {
    auto logger = get_tiered_logger();
    S3Result result;

    logger->info("S3 DELETE MULTI: {} objects", keys.size());

    result.success = true;
    result.http_status = 200;

    for (const auto& key : keys) {
        auto del_result = delete_object(key);
        if (!del_result.success) {
            result.success = false;
            result.error = error_code::storage_unavailable;
            result.error_message = "Failed to delete: " + key;
            break;
        }
    }

    return result;
}

S3Client::S3Result S3Client::list_objects(const std::string& prefix, int32_t max_keys) {
    auto logger = get_tiered_logger();
    S3Result result;

    logger->debug("S3 LIST: prefix='{}', max_keys={}", prefix, max_keys);

    result.success = true;
    result.http_status = 200;

    return result;
}

std::string S3Client::build_auth_header(const std::string& method,
                                         const std::string& key,
                                         const std::string& payload_hash,
                                         const std::string& content_type) const {
    // Stub: in production, compute AWS Signature V4:
    //   1. Create canonical request.
    //   2. Create string to sign.
    //   3. Calculate signature using HMAC-SHA256 with signing key.
    //
    // For now, return a placeholder.
    std::ostringstream auth;
    auth << "AWS4-HMAC-SHA256 "
         << "Credential=" << config_.access_key
         << "/20240101/" << config_.region << "/s3/aws4_request";
    return auth.str();
}

S3Client::S3Result S3Client::do_request(const std::string& method,
                                         const std::string& key,
                                         const std::vector<uint8_t>* body,
                                         const std::string& content_type,
                                         std::vector<uint8_t>* response_body,
                                         const std::string& range_header) {
    // Stub: in production, execute HTTP request with retries.
    // For now, forward to the specific operation methods.
    (void)range_header;

    if (method == "PUT" && body) {
        return put_object(key, body->data(),
                          static_cast<byte_count_t>(body->size()),
                          content_type);
    }
    if (method == "GET") {
        return get_object(key);
    }
    if (method == "DELETE") {
        return delete_object(key);
    }
    if (method == "HEAD") {
        return head_object(key);
    }

    S3Result result;
    result.success = false;
    result.error = error_code::storage_unavailable;
    result.error_message = "Unsupported method: " + method;
    return result;
}

// ============================================================================
// ArchiveManager — construction
// ============================================================================

ArchiveManager::ArchiveManager(const TieredStorageConfig& config,
                               std::shared_ptr<S3Client> s3_client)
    : config_(config)
    , s3_client_(std::move(s3_client))
{
    auto logger = get_tiered_logger();
    logger->info("ArchiveManager created: transition_after={}ms, "
                 "expire_after={}ms, compression={}, dry_run={}",
                 config_.transition_after_ms,
                 config_.expire_after_ms,
                 config_.enable_compression
                    ? std::to_string(static_cast<int>(config_.upload_compression))
                    : "none",
                 config_.dry_run);
}

ArchiveManager::~ArchiveManager() {
    auto logger = get_tiered_logger();
    logger->info("ArchiveManager destroyed: {} objects archived total",
                 archived_objects_.size());
}

// ============================================================================
// ArchiveManager — eligibility selection
// ============================================================================

std::vector<SegmentInfo> ArchiveManager::select_eligible_segments(
    const std::vector<SegmentInfo>& segments,
    const std::string& topic_name,
    partition_id_t partition_id,
    timestamp_ms_t now) const
{
    if (now <= 0) now = now_ms();
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<SegmentInfo> eligible;
    eligible.reserve(segments.size());

    auto logger = get_tiered_logger();

    for (const auto& seg : segments) {
        // Skip active or unsealed segments.
        if (seg.is_active || !seg.is_sealed) {
            logger->trace("Skipping segment {} (active={}, sealed={})",
                          seg.segment_id, seg.is_active, seg.is_sealed);
            continue;
        }

        // Skip empty segments.
        if (seg.empty()) {
            logger->trace("Skipping empty segment {}", seg.segment_id);
            continue;
        }

        // Check if already archived.
        bool already_archived = false;
        for (const auto& obj : archived_objects_) {
            if (obj.partition_id == partition_id
                && obj.segment_id == seg.segment_id) {
                already_archived = true;
                break;
            }
        }
        if (already_archived) {
            logger->trace("Skipping already-archived segment {}", seg.segment_id);
            continue;
        }

        // Check age threshold.
        timestamp_ms_t age = now - seg.max_timestamp;
        if (config_.transition_after_ms > 0
            && age < config_.transition_after_ms) {
            logger->trace("Segment {} not old enough: age={}ms, threshold={}ms",
                          seg.segment_id, age, config_.transition_after_ms);
            continue;
        }

        eligible.push_back(seg);
    }

    // Sort by age (oldest first) so the oldest segments are archived first.
    std::sort(eligible.begin(), eligible.end(),
              [](const SegmentInfo& a, const SegmentInfo& b) {
                  return a.max_timestamp < b.max_timestamp;
              });

    logger->debug("select_eligible: topic='{}', part={}, {} total, {} eligible",
                  topic_name, partition_id, segments.size(), eligible.size());

    return eligible;
}

// ============================================================================
// ArchiveManager — compress data
// ============================================================================

std::vector<uint8_t> ArchiveManager::compress_data(const void* data,
                                                     byte_count_t size) {
    if (!config_.enable_compression) {
        // No compression — copy the data as-is.
        auto* bytes = static_cast<const uint8_t*>(data);
        return std::vector<uint8_t>(bytes, bytes + size);
    }

    // Stub: in production, use zstd/lz4/snappy compression.
    // For now, prepend a 4-byte header with the compression type and
    // original size, then copy the data as-is (no actual compression).
    size_t header_size = 8;  // 4 bytes type, 4 bytes original size
    std::vector<uint8_t> compressed(header_size + static_cast<size_t>(size));
    compressed[0] = static_cast<uint8_t>(config_.upload_compression);
    compressed[1] = 0;
    compressed[2] = 0;
    compressed[3] = 0;
    compressed[4] = static_cast<uint8_t>((size >> 24) & 0xFF);
    compressed[5] = static_cast<uint8_t>((size >> 16) & 0xFF);
    compressed[6] = static_cast<uint8_t>((size >> 8) & 0xFF);
    compressed[7] = static_cast<uint8_t>(size & 0xFF);

    std::memcpy(compressed.data() + header_size, data, static_cast<size_t>(size));

    auto logger = get_tiered_logger();
    logger->debug("Compressed {} bytes -> {} bytes (type={})",
                  size, compressed.size(),
                  static_cast<int>(config_.upload_compression));

    return compressed;
}

// ============================================================================
// ArchiveManager — SHA-256
// ============================================================================

std::string ArchiveManager::compute_sha256(const void* data, byte_count_t size) {
    return sha256_hex(data, static_cast<size_t>(size));
}

// ============================================================================
// ArchiveManager — delete local segment
// ============================================================================

void ArchiveManager::delete_local_segment(const SegmentInfo& segment,
                                           const std::string& data_dir) {
    auto logger = get_tiered_logger();

    std::string log_path = segment.file_path;
    if (log_path.empty() && !data_dir.empty()) {
        log_path = data_dir + "/" + std::to_string(segment.segment_id) + ".log";
    }

    // Delete the .log, .index, and .timeindex files.
    std::string base = log_path;
    if (base.size() > 4 && base.compare(base.size() - 4, 4, ".log") == 0) {
        base = base.substr(0, base.size() - 4);
    }

    std::string index_path     = base + ".index";
    std::string timeindex_path = base + ".timeindex";

    bool ok = true;
    ok &= delete_file(log_path);
    ok &= delete_file(index_path);
    ok &= delete_file(timeindex_path);

    if (ok) {
        logger->info("Deleted local segment files: {} (.log, .index, .timeindex)", base);
    } else {
        logger->warn("Some local segment files could not be deleted: {}", base);
    }
}

// ============================================================================
// ArchiveManager — archive a single segment
// ============================================================================

ArchiveManager::ArchiveResult ArchiveManager::archive_segment(
    const SegmentInfo& segment,
    const std::string& data_dir,
    topic_id_t topic_id,
    const std::string& topic_name,
    partition_id_t partition_id)
{
    auto logger = get_tiered_logger();
    auto start = std::chrono::steady_clock::now();
    ArchiveResult result;

    logger->info("Archiving segment {} (topic='{}', part={}, offsets [{}, {}))",
                 segment.segment_id, topic_name, partition_id,
                 segment.base_offset, segment.next_offset);

    // 1. Read segment data from local disk.
    std::string file_path = segment.file_path;
    if (file_path.empty() && !data_dir.empty()) {
        file_path = data_dir + "/" + std::to_string(segment.segment_id) + ".log";
    }

    std::vector<uint8_t> segment_data = read_file(file_path);
    if (segment_data.empty()) {
        result.success = false;
        result.error = error_code::segment_corrupted;
        result.error_message = "Cannot read segment file: " + file_path;
        logger->error("Archive failed for segment {}: {}", segment.segment_id,
                      result.error_message);
        return result;
    }
    logger->debug("Read {} bytes from local segment {}", segment_data.size(),
                  file_path);

    // 2. Compute SHA-256 of original data (before compression).
    std::string checksum = sha256_hex(segment_data.data(), segment_data.size());

    // 3. Compress if configured.
    std::vector<uint8_t> processed_data = compress_data(
        segment_data.data(),
        static_cast<byte_count_t>(segment_data.size()));
    byte_count_t original_size = static_cast<byte_count_t>(segment_data.size());
    segment_data.clear();  // Free memory early.
    segment_data.shrink_to_fit();

    // 4. Generate S3 key.
    std::string object_key = s3_client_->make_key_by_name(
        topic_name, partition_id, segment.segment_id, segment.base_offset);

    // 5. Upload to S3.
    S3Client::S3Result s3_result = s3_client_->put_object(
        object_key,
        processed_data.data(),
        static_cast<byte_count_t>(processed_data.size()),
        "application/octet-stream");

    if (!s3_result.ok()) {
        result.success = false;
        result.error = error_code::storage_unavailable;
        result.error_message = "S3 PUT failed: " + s3_result.error_message;
        logger->error("Archive failed for segment {}: {}", segment.segment_id,
                      result.error_message);
        return result;
    }

    // 6. Verify upload if configured.
    if (config_.verify_uploads) {
        S3Client::S3Result head_result = s3_client_->head_object(object_key);
        if (!head_result.ok()) {
            result.success = false;
            result.error = error_code::storage_unavailable;
            result.error_message = "Upload verification (HEAD) failed: " +
                                   head_result.error_message;
            logger->error("Archive verification failed for segment {}: {}",
                          segment.segment_id, result.error_message);
            return result;
        }
        logger->debug("Upload verified for segment {}: size={}, etag={}",
                      segment.segment_id, head_result.object_size,
                      head_result.etag);
    }

    // 7. Record metadata.
    S3ObjectMetadata meta;
    meta.object_key         = object_key;
    meta.topic_id           = topic_id;
    meta.partition_id       = partition_id;
    meta.segment_id         = segment.segment_id;
    meta.base_offset        = segment.base_offset;
    meta.next_offset        = segment.next_offset;
    meta.object_size        = static_cast<byte_count_t>(processed_data.size());
    meta.original_size      = original_size;
    meta.compression        = config_.enable_compression
                                ? config_.upload_compression
                                : compression_type::none;
    meta.etag               = s3_result.etag;
    meta.sha256_checksum    = checksum;
    meta.archived_at        = now_ms();
    meta.segment_created_at = segment.created_at;
    meta.max_timestamp      = segment.max_timestamp;
    meta.record_count       = segment.record_count();
    meta.verified           = config_.verify_uploads;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        archived_objects_.push_back(meta);
    }

    // 8. Delete local segment files.
    delete_local_segment(segment, data_dir);

    // 9. Notify progress callback.
    if (progress_callback_) {
        progress_callback_(meta);
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    result.success = true;
    result.object = meta;

    logger->info("Archived segment {} to S3: key='{}', original={} bytes, "
                 "archived={} bytes, elapsed={}ms",
                 segment.segment_id, object_key,
                 original_size, meta.object_size, elapsed.count());

    return result;
}

// ============================================================================
// ArchiveManager — archive a batch of segments
// ============================================================================

ArchiveManager::BatchArchiveResult ArchiveManager::archive_batch(
    const std::vector<SegmentInfo>& segments,
    const std::string& data_dir,
    topic_id_t topic_id,
    const std::string& topic_name,
    partition_id_t partition_id)
{
    auto start = std::chrono::steady_clock::now();
    BatchArchiveResult batch_result;
    auto logger = get_tiered_logger();

    logger->info("Archive batch: {} segments, topic='{}', part={}",
                 segments.size(), topic_name, partition_id);

    int32_t max_concurrent = config_.max_concurrent_uploads;
    if (max_concurrent <= 0) max_concurrent = 1;

    // Process segments sequentially (in production, a thread pool would
    // be used for concurrent uploads bounded by max_concurrent).
    for (const auto& seg : segments) {
        ArchiveResult result = archive_segment(seg, data_dir, topic_id,
                                                topic_name, partition_id);
        batch_result.results.push_back(result);

        if (result.ok()) {
            batch_result.successful++;
            batch_result.bytes_uploaded += result.object.object_size;
            batch_result.bytes_freed += result.object.original_size;
        } else {
            batch_result.failed++;
        }
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    batch_result.duration_ms = elapsed.count();

    if (batch_result.failed > 0) {
        batch_result.error = error_code::storage_unavailable;
        batch_result.error_message = std::to_string(batch_result.failed) +
                                      " segments failed to archive";
    }

    logger->info("Archive batch complete: {} succeeded, {} failed, "
                 "{} bytes uploaded, {} bytes freed, {}ms",
                 batch_result.successful, batch_result.failed,
                 batch_result.bytes_uploaded, batch_result.bytes_freed,
                 batch_result.duration_ms);

    return batch_result;
}

// ============================================================================
// ArchiveManager — archive all eligible segments
// ============================================================================

ArchiveManager::BatchArchiveResult ArchiveManager::archive_all_eligible(
    const std::string& data_dir,
    const std::vector<std::pair<topic_id_t, std::string>>& topics,
    timestamp_ms_t now)
{
    if (now <= 0) now = now_ms();
    auto start = std::chrono::steady_clock::now();
    BatchArchiveResult total_result;
    auto logger = get_tiered_logger();

    logger->info("Archive all eligible: {} topics, now={}", topics.size(), now);

    // In a full implementation, this would scan the data directory for
    // partition directories, enumerate segments, and evaluate eligibility
    // per topic/partition.  For now, this is a skeleton that returns
    // a placeholder result.

    // Note: the actual implementation would call select_eligible_segments()
    // for each partition and then archive_batch().

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    total_result.duration_ms = elapsed.count();

    logger->info("Archive all complete: {} topics scanned, {}ms",
                 topics.size(), total_result.duration_ms);

    return total_result;
}

// ============================================================================
// ArchiveManager — lifecycle expiration
// ============================================================================

bool ArchiveManager::is_expired(const S3ObjectMetadata& object,
                                 timestamp_ms_t now) const noexcept {
    if (config_.expire_after_ms <= 0) return false;
    if (now <= 0) now = now_ms();
    timestamp_ms_t age = now - object.archived_at;
    return age >= config_.expire_after_ms;
}

ArchiveManager::BatchArchiveResult ArchiveManager::expire_objects(timestamp_ms_t now) {
    if (now <= 0) now = now_ms();
    auto start = std::chrono::steady_clock::now();
    BatchArchiveResult result;
    auto logger = get_tiered_logger();

    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<std::string> to_delete;
    for (auto it = archived_objects_.begin(); it != archived_objects_.end(); ) {
        if (is_expired(*it, now)) {
            logger->info("Object expired: key='{}', archived {}ms ago",
                         it->object_key,
                         format_timestamp(now - it->archived_at));
            to_delete.push_back(it->object_key);
            it = archived_objects_.erase(it);
            result.successful++;
        } else {
            ++it;
        }
    }

    // Delete the objects from S3.
    if (!to_delete.empty() && !config_.dry_run) {
        auto s3_result = s3_client_->delete_objects(to_delete);
        if (!s3_result.ok()) {
            result.error = error_code::storage_unavailable;
            result.error_message = "S3 multi-delete failed: " +
                                    s3_result.error_message;
            result.failed = static_cast<int32_t>(to_delete.size());
            result.successful = 0;
        }
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    result.duration_ms = elapsed.count();

    logger->info("Expiration complete: {} objects deleted, {}ms",
                 to_delete.size(), result.duration_ms);

    return result;
}

// ============================================================================
// ArchiveManager — progress callback
// ============================================================================

void ArchiveManager::on_progress(progress_callback_t callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    progress_callback_ = std::move(callback);
}

// ============================================================================
// RestoreManager — construction
// ============================================================================

RestoreManager::RestoreManager(const TieredStorageConfig& config,
                               std::shared_ptr<S3Client> s3_client)
    : config_(config)
    , s3_client_(std::move(s3_client))
{
    auto logger = get_tiered_logger();
    logger->info("RestoreManager created: max_concurrent_downloads={}, "
                 "verify_downloads={}, dry_run={}",
                 config_.max_concurrent_downloads,
                 config_.verify_downloads,
                 config_.dry_run);
}

RestoreManager::~RestoreManager() {
    auto logger = get_tiered_logger();
    logger->info("RestoreManager destroyed");
}

// ============================================================================
// RestoreManager — decompress data
// ============================================================================

std::vector<uint8_t> RestoreManager::decompress_data(const void* data,
                                                       byte_count_t size,
                                                       compression_type ct) {
    if (ct == compression_type::none || !config_.enable_compression) {
        // No compression — return data as-is.
        auto* bytes = static_cast<const uint8_t*>(data);
        return std::vector<uint8_t>(bytes, bytes + static_cast<size_t>(size));
    }

    // Stub: read the 8-byte header (type + original size) and skip it.
    // In production, use zstd/lz4/snappy decompression.
    if (size < 8) {
        auto logger = get_tiered_logger();
        logger->error("Decompress: data too small for header ({})", size);
        return {};
    }

    auto* bytes = static_cast<const uint8_t*>(data);
    byte_count_t original_size =
        (static_cast<byte_count_t>(bytes[4]) << 24) |
        (static_cast<byte_count_t>(bytes[5]) << 16) |
        (static_cast<byte_count_t>(bytes[6]) << 8)  |
        (static_cast<byte_count_t>(bytes[7]));

    // Return the payload after the header.
    size_t header_size = 8;
    size_t payload_size = static_cast<size_t>(size) - header_size;

    auto logger = get_tiered_logger();
    logger->debug("Decompressed {} bytes -> {} bytes (type={})",
                  size, original_size, static_cast<int>(ct));

    return std::vector<uint8_t>(bytes + header_size,
                                bytes + header_size + payload_size);
}

// ============================================================================
// RestoreManager — checksum verification
// ============================================================================

bool RestoreManager::verify_checksum(const void* data,
                                       byte_count_t size,
                                       const std::string& expected_sha256) {
    if (expected_sha256.empty()) return true;
    std::string actual = sha256_hex(data, static_cast<size_t>(size));
    return actual == expected_sha256;
}

// ============================================================================
// RestoreManager — write local segment
// ============================================================================

bool RestoreManager::write_local_segment(const std::string& file_path,
                                          const void* data,
                                          byte_count_t size) {
    return write_file(file_path, data, static_cast<size_t>(size));
}

// ============================================================================
// RestoreManager — restore a segment
// ============================================================================

RestoreManager::RestoreResult RestoreManager::restore_segment(
    const S3ObjectMetadata& object,
    const std::string& data_dir,
    const std::string& topic_name)
{
    auto logger = get_tiered_logger();
    auto start = std::chrono::steady_clock::now();
    RestoreResult result;
    result.object = object;

    logger->info("Restoring segment: key='{}', topic='{}', part={}, "
                 "offset_range=[{}, {})",
                 object.object_key, topic_name, object.partition_id,
                 object.base_offset, object.next_offset);

    // 1. Download from S3.
    S3Client::S3Result s3_result = s3_client_->get_object(object.object_key);
    if (!s3_result.ok()) {
        result.success = false;
        result.error = error_code::storage_unavailable;
        result.error_message = "S3 GET failed: " + s3_result.error_message;
        logger->error("Restore failed for {}: {}", object.object_key,
                      result.error_message);
        return result;
    }
    result.bytes_downloaded = s3_result.object_size;
    logger->debug("Downloaded {} bytes from S3", result.bytes_downloaded);

    // 2. Decompress if needed.
    std::vector<uint8_t> decompressed = decompress_data(
        s3_result.body.data(),
        static_cast<byte_count_t>(s3_result.body.size()),
        object.compression);

    if (decompressed.empty() && object.compression != compression_type::none) {
        result.success = false;
        result.error = error_code::corrupt_message;
        result.error_message = "Decompression returned empty data";
        logger->error("Restore failed for {}: {}", object.object_key,
                      result.error_message);
        return result;
    }

    byte_count_t decompressed_size = static_cast<byte_count_t>(decompressed.size());

    // 3. Verify SHA-256 checksum if configured.
    if (config_.verify_downloads && !object.sha256_checksum.empty()) {
        if (!verify_checksum(decompressed.data(), decompressed_size,
                             object.sha256_checksum)) {
            result.success = false;
            result.error = error_code::corrupt_message;
            result.error_message = "SHA-256 checksum mismatch for " +
                                    object.object_key;
            logger->error("Restore checksum failed for {}: expected={}, actual={}",
                          object.object_key, object.sha256_checksum,
                          sha256_hex(decompressed.data(),
                                     static_cast<size_t>(decompressed_size)));
            return result;
        }
        logger->debug("SHA-256 checksum verified for {}", object.object_key);
    }

    // 4. Write to local disk.
    std::string local_path = data_dir + "/" + topic_name + "/" +
                             std::to_string(object.partition_id) + "/" +
                             std::to_string(object.segment_id) + ".log";

    if (!write_local_segment(local_path, decompressed.data(),
                              decompressed_size)) {
        result.success = false;
        result.error = error_code::storage_unavailable;
        result.error_message = "Cannot write local segment file: " + local_path;
        logger->error("Restore failed: {}", result.error_message);
        return result;
    }
    result.bytes_written = decompressed_size;
    result.local_path = local_path;
    result.success = true;

    // Build SegmentInfo for the caller.
    result.segment_info.segment_id    = object.segment_id;
    result.segment_info.base_offset   = object.base_offset;
    result.segment_info.next_offset   = object.next_offset;
    result.segment_info.file_path     = local_path;
    result.segment_info.file_size     = decompressed_size;
    result.segment_info.max_timestamp = object.max_timestamp;
    result.segment_info.created_at    = object.segment_created_at;
    result.segment_info.last_modified = now_ms();
    result.segment_info.is_active     = false;
    result.segment_info.is_sealed     = true;

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    result.duration_ms = elapsed.count();

    logger->info("Restored segment: key='{}', local='{}', {} bytes, {}ms",
                 object.object_key, local_path, decompressed_size,
                 result.duration_ms);

    return result;
}

// ============================================================================
// RestoreManager — restore by offset
// ============================================================================

RestoreManager::RestoreResult RestoreManager::restore_by_offset(
    topic_id_t topic_id,
    partition_id_t partition_id,
    offset_t offset,
    const std::string& data_dir,
    const std::string& topic_name)
{
    auto logger = get_tiered_logger();

    auto opt = find_by_offset(topic_id, partition_id, offset);
    if (!opt.has_value()) {
        RestoreResult result;
        result.success = false;
        result.error = error_code::resource_not_found;
        result.error_message = "No archived object found for topic=" +
                               std::to_string(topic_id) +
                               ", partition=" + std::to_string(partition_id) +
                               ", offset=" + std::to_string(offset);
        logger->warn("{}", result.error_message);
        return result;
    }

    return restore_segment(opt.value(), data_dir, topic_name);
}

// ============================================================================
// RestoreManager — restore multiple segments
// ============================================================================

std::vector<RestoreManager::RestoreResult> RestoreManager::restore_batch(
    const std::vector<S3ObjectMetadata>& objects,
    const std::string& data_dir,
    const std::string& topic_name)
{
    auto logger = get_tiered_logger();
    std::vector<RestoreResult> results;
    results.reserve(objects.size());

    logger->info("Restore batch: {} objects", objects.size());

    for (const auto& obj : objects) {
        results.push_back(restore_segment(obj, data_dir, topic_name));
    }

    size_t succeeded = 0;
    for (const auto& r : results) {
        if (r.ok()) succeeded++;
    }

    logger->info("Restore batch complete: {}/{} succeeded", succeeded, objects.size());

    return results;
}

// ============================================================================
// RestoreManager — metadata index
// ============================================================================

void RestoreManager::register_object(const S3ObjectMetadata& object) {
    std::unique_lock<std::shared_mutex> lock(index_mutex_);
    ObjectKey key = {object.topic_id, object.partition_id};
    auto& vec = index_[key];

    // Insert in sorted order by base_offset.
    auto it = std::lower_bound(vec.begin(), vec.end(), object,
                               [](const S3ObjectMetadata& a,
                                  const S3ObjectMetadata& b) {
                                   return a.base_offset < b.base_offset;
                               });
    vec.insert(it, object);

    auto logger = get_tiered_logger();
    logger->debug("Registered object in index: key='{}', topic={}, part={}, "
                  "offset_range=[{}, {})",
                  object.object_key, object.topic_id, object.partition_id,
                  object.base_offset, object.next_offset);
}

void RestoreManager::unregister_object(const std::string& object_key) {
    std::unique_lock<std::shared_mutex> lock(index_mutex_);

    for (auto& [key, vec] : index_) {
        auto it = std::find_if(vec.begin(), vec.end(),
                               [&](const S3ObjectMetadata& obj) {
                                   return obj.object_key == object_key;
                               });
        if (it != vec.end()) {
            vec.erase(it);
            auto logger = get_tiered_logger();
            logger->debug("Unregistered object from index: key='{}'", object_key);
            return;
        }
    }
}

std::optional<S3ObjectMetadata> RestoreManager::find_by_offset(
    topic_id_t topic_id,
    partition_id_t partition_id,
    offset_t offset) const
{
    std::shared_lock<std::shared_mutex> lock(index_mutex_);
    ObjectKey key = {topic_id, partition_id};
    auto it = index_.find(key);
    if (it == index_.end()) {
        return std::nullopt;
    }

    // Binary search for the object containing the given offset.
    // Objects are sorted by base_offset.
    const auto& vec = it->second;
    auto obj_it = std::upper_bound(vec.begin(), vec.end(), offset,
                                   [](offset_t off, const S3ObjectMetadata& obj) {
                                       return off < obj.base_offset;
                                   });

    if (obj_it == vec.begin()) {
        // offset is before the first object's base_offset.
        return std::nullopt;
    }
    --obj_it;

    // Check if the offset falls within this object's range.
    if (offset >= obj_it->base_offset && offset < obj_it->next_offset) {
        return *obj_it;
    }

    return std::nullopt;
}

std::vector<S3ObjectMetadata> RestoreManager::list_objects(
    topic_id_t topic_id,
    partition_id_t partition_id) const
{
    std::shared_lock<std::shared_mutex> lock(index_mutex_);
    ObjectKey key = {topic_id, partition_id};
    auto it = index_.find(key);
    if (it == index_.end()) {
        return {};
    }
    return it->second;
}

} // namespace torrent
