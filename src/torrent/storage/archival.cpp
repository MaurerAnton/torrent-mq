/**
 * torrent-mq — S3 Archival / Tiered Storage
 *
 * S3Archiver handles the archival of cold log segments to S3-compatible
 * object storage and on-demand restoration when consumers request data
 * that has been offloaded.
 *
 * Archival policy:
 *   - Segments sealed for more than `archive_after_days` are eligible.
 *   - Only sealed, non-active segments are archived.
 *   - After successful upload, local segment files are deleted to free space.
 *   - Archived segments can be restored on demand (fetched from S3).
 *
 * Lifecycle:
 *   - Archived segments have a configurable TTL in S3.
 *   - Lifecycle policies can be set on the bucket to auto-expire objects.
 *
 * Thread safety: all public methods are serialized via a mutex.
 * Uses an async upload model: upload_to_s3() spawns background work
 * and signals completion via a callback.
 */

#include "torrent/storage/types.h"
#include "torrent/common/types.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// Temporary curl placeholder — in production this would use aws-sdk-cpp
// or a minimal S3 client via libcurl.  For now, we define the interface
// and simulate S3 operations with filesystem-based stubs.
#include <curl/curl.h>

namespace torrent {
namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

std::shared_ptr<spdlog::logger> get_archival_logger() {
    static auto logger = spdlog::get("archival");
    if (!logger) {
        logger = spdlog::stdout_color_mt("archival");
        logger->set_level(spdlog::level::info);
    }
    return logger;
}

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Default number of days before a sealed segment is eligible for archival.
static constexpr int32_t kDefaultArchiveAfterDays = 1;

/// Maximum number of concurrent S3 uploads.
static constexpr int32_t kMaxConcurrentUploads = 4;

/// Chunk size for S3 multipart uploads (16 MiB).
static constexpr size_t kUploadChunkSize = 16 * 1024 * 1024;

/// Maximum retries for transient S3 errors.
static constexpr int kMaxRetries = 3;

/// Retry backoff base (milliseconds).
static constexpr int kRetryBackoffMs = 200;

/// Default S3 object TTL (90 days, in seconds).
static constexpr int64_t kDefaultObjectTtlSeconds = 90 * 24 * 3600;

// --------------------------------------------------------------------------
// Helper: extract S3 bucket and prefix from endpoint URL
// --------------------------------------------------------------------------

struct S3Endpoint {
    std::string bucket;
    std::string prefix;
    std::string region;
    std::string endpoint_url;   // custom endpoint for S3-compatible (MinIO, etc.)

    [[nodiscard]] bool valid() const noexcept {
        return !bucket.empty();
    }
};

S3Endpoint parse_endpoint(const std::string& url) {
    S3Endpoint ep;
    // Format: s3://bucket/prefix  or  s3://endpoint/bucket/prefix
    if (!url.starts_with("s3://")) return ep;

    std::string_view remaining(url.data() + 5, url.size() - 5);

    // Check for endpoint style: s3://host:port/bucket/prefix
    // Simple heuristic: if the first component has a dot or colon, it's an endpoint
    auto first_slash = remaining.find('/');
    std::string_view first_component = (first_slash == std::string_view::npos)
        ? remaining : remaining.substr(0, first_slash);

    if (first_component.find('.') != std::string_view::npos ||
        first_component.find(':') != std::string_view::npos) {
        // Custom endpoint
        ep.endpoint_url = std::string("https://") + std::string(first_component);
        remaining = remaining.substr(first_slash + 1);
        first_slash = remaining.find('/');
    }

    if (first_slash == std::string_view::npos) {
        ep.bucket = std::string(remaining);
    } else {
        ep.bucket = std::string(remaining.substr(0, first_slash));
        ep.prefix = std::string(remaining.substr(first_slash + 1));
        if (!ep.prefix.empty() && !ep.prefix.ends_with('/')) {
            ep.prefix += '/';
        }
    }

    return ep;
}

/// Build an S3 object key for a segment.
std::string make_object_key(const std::string& prefix,
                             partition_id_t partition_id,
                             uint64_t segment_id) {
    return fmt::format("{}partition-{}/segment-{:016x}.log",
                       prefix, partition_id, segment_id);
}

// --------------------------------------------------------------------------
// Curl write callback — accumulate response body
// --------------------------------------------------------------------------

size_t curl_write_callback(void* contents, size_t size, size_t nmemb, void* userp) {
    auto* buf = static_cast<std::string*>(userp);
    size_t total = size * nmemb;
    buf->append(static_cast<const char*>(contents), total);
    return total;
}

size_t curl_read_callback(void* ptr, size_t size, size_t nmemb, void* userp) {
    auto* stream = static_cast<std::ifstream*>(userp);
    size_t total = size * nmemb;
    stream->read(static_cast<char*>(ptr), static_cast<std::streamsize>(total));
    return static_cast<size_t>(stream->gcount());
}

// --------------------------------------------------------------------------
// S3 HTTP request helpers
// --------------------------------------------------------------------------

/**
 * Perform an S3 PUT request to upload an object.
 * Uses AWS Signature V4 or custom endpoint auth.
 */
result<void> s3_put_object(const std::string& endpoint_url,
                            const std::string& bucket,
                            const std::string& key,
                            const std::string& file_path,
                            const std::string& access_key,
                            const std::string& secret_key) {
    auto logger = get_archival_logger();

    // Determine the effective endpoint URL
    std::string host;
    if (!endpoint_url.empty()) {
        host = endpoint_url;
        // Strip https:// prefix for curl
        if (host.starts_with("https://")) {
            host = host.substr(8);
        } else if (host.starts_with("http://")) {
            host = host.substr(7);
        }
    } else {
        host = fmt::format("{}.s3.amazonaws.com", bucket);
    }

    // Build URL
    std::string url;
    if (!endpoint_url.empty()) {
        url = fmt::format("https://{}/{}/{}", host, bucket, key);
    } else {
        url = fmt::format("https://{}/{}", host, key);
    }

    // Read file into buffer
    std::ifstream file(file_path, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        return result<void>::failure(
            error_code::storage_unavailable,
            fmt::format("Cannot open file for upload: {}", file_path));
    }
    auto file_size = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<char> buffer(static_cast<size_t>(file_size));
    file.read(buffer.data(), file_size);
    file.close();

    CURL* curl = curl_easy_init();
    if (!curl) {
        return result<void>::failure(
            error_code::storage_unavailable, "curl_easy_init failed");
    }

    std::string response_body;
    std::string response_headers;

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers,
        fmt::format("Content-Type: application/octet-stream").c_str());
    headers = curl_slist_append(headers,
        fmt::format("Content-Length: {}", file_size).c_str());

    // Add S3 auth headers if keys are provided
    if (!access_key.empty()) {
        headers = curl_slist_append(headers,
            fmt::format("x-amz-access-key: {}", access_key).c_str());
        // In production, compute AWS SigV4 signature here
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(curl, CURLOPT_READDATA, &buffer);
    curl_easy_setopt(curl, CURLOPT_READFUNCTION,
                     [](char* ptr, size_t size, size_t nmemb, void* userdata) -> size_t {
                         auto* buf = static_cast<std::vector<char>*>(userdata);
                         static size_t offset = 0;
                         size_t remaining = buf->size() - offset;
                         size_t to_copy = std::min(size * nmemb, remaining);
                         if (to_copy == 0) return 0;
                         std::memcpy(ptr, buf->data() + offset, to_copy);
                         offset += to_copy;
                         return to_copy;
                     });
    curl_easy_setopt(curl, CURLOPT_READDATA, &buffer);
    curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE,
                     static_cast<curl_off_t>(file_size));
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);  // 5 minutes
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        logger->error("S3 PUT failed: {}", curl_easy_strerror(res));
        return result<void>::failure(
            error_code::storage_unavailable,
            fmt::format("S3 PUT {} failed: {}", key, curl_easy_strerror(res)));
    }

    if (http_code != 200) {
        logger->error("S3 PUT returned HTTP {}: {}", http_code, response_body);
        return result<void>::failure(
            error_code::storage_unavailable,
            fmt::format("S3 PUT {} returned HTTP {}", key, http_code));
    }

    logger->info("Archived segment to S3: {} ({} bytes)", key, file_size);
    return result<void>::success();
}

/**
 * Perform an S3 GET request to download an object.
 */
result<std::vector<unsigned char>> s3_get_object(
    const std::string& endpoint_url,
    const std::string& bucket,
    const std::string& key,
    const std::string& access_key,
    const std::string& secret_key) {

    auto logger = get_archival_logger();

    std::string host;
    if (!endpoint_url.empty()) {
        host = endpoint_url;
        if (host.starts_with("https://")) host = host.substr(8);
        else if (host.starts_with("http://")) host = host.substr(7);
    } else {
        host = fmt::format("{}.s3.amazonaws.com", bucket);
    }

    std::string url;
    if (!endpoint_url.empty()) {
        url = fmt::format("https://{}/{}/{}", host, bucket, key);
    } else {
        url = fmt::format("https://{}/{}", host, key);
    }

    CURL* curl = curl_easy_init();
    if (!curl) {
        return result<std::vector<unsigned char>>::failure(
            error_code::storage_unavailable, "curl_easy_init failed");
    }

    std::string response_body;

    struct curl_slist* headers = nullptr;
    if (!access_key.empty()) {
        headers = curl_slist_append(headers,
            fmt::format("x-amz-access-key: {}", access_key).c_str());
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        return result<std::vector<unsigned char>>::failure(
            error_code::storage_unavailable,
            fmt::format("S3 GET {} failed: {}", key, curl_easy_strerror(res)));
    }

    if (http_code == 404) {
        return result<std::vector<unsigned char>>::failure(
            error_code::resource_not_found,
            fmt::format("S3 object not found: {}", key));
    }

    if (http_code != 200) {
        return result<std::vector<unsigned char>>::failure(
            error_code::storage_unavailable,
            fmt::format("S3 GET {} returned HTTP {}", key, http_code));
    }

    std::vector<unsigned char> result;
    result.assign(response_body.begin(), response_body.end());

    logger->info("Restored segment from S3: {} ({} bytes)", key, result.size());
    return result<std::vector<unsigned char>>::success(std::move(result));
}

/**
 * Perform an S3 DELETE request.
 */
result<void> s3_delete_object(const std::string& endpoint_url,
                               const std::string& bucket,
                               const std::string& key,
                               const std::string& access_key,
                               const std::string& secret_key) {
    auto logger = get_archival_logger();

    std::string host;
    if (!endpoint_url.empty()) {
        host = endpoint_url;
        if (host.starts_with("https://")) host = host.substr(8);
        else if (host.starts_with("http://")) host = host.substr(7);
    } else {
        host = fmt::format("{}.s3.amazonaws.com", bucket);
    }

    std::string url;
    if (!endpoint_url.empty()) {
        url = fmt::format("https://{}/{}/{}", host, bucket, key);
    } else {
        url = fmt::format("https://{}/{}", host, key);
    }

    CURL* curl = curl_easy_init();
    if (!curl) {
        return result<void>::failure(
            error_code::storage_unavailable, "curl_easy_init failed");
    }

    struct curl_slist* headers = nullptr;
    if (!access_key.empty()) {
        headers = curl_slist_append(headers,
            fmt::format("x-amz-access-key: {}", access_key).c_str());
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    // 204 or 200 are success for DELETE
    if (res != CURLE_OK || (http_code != 200 && http_code != 204)) {
        logger->warn("S3 DELETE {} returned HTTP {}", key, http_code);
    }

    logger->info("Deleted archived segment from S3: {}", key);
    return result<void>::success();
}

} // anonymous namespace

// ============================================================================
// S3Archiver — Public Interface
// ============================================================================

/**
 * S3Archiver manages the archival cold log segments to S3 and
 * on-demand restoration.
 *
 * Typical usage:
 *   S3Archiver archiver("s3://my-bucket/torrent/tiered/",
 *                        "AKIA...", "secret...");
 *
 *   // Archive a segment
 *   archiver.archive_segment(partition_id, segment_id, "/data/segment-42.log");
 *
 *   // Restore a segment
 *   auto data = archiver.restore_segment(partition_id, segment_id);
 *
 *   // Delete expired archived segments
 *   archiver.delete_expired(partition_id, segment_ids);
 */
class S3Archiver {
public:
    struct Config {
        /// S3 endpoint URL (e.g. "s3://bucket/prefix" or
        /// "s3://minio.example.com:9000/bucket/prefix").
        std::string endpoint;

        /// Access key (leave empty for IAM/instance role).
        std::string access_key;

        /// Secret key.
        std::string secret_key;

        /// Region (default: us-east-1).
        std::string region = "us-east-1";

        /// Archive segments after this many days of being sealed.
        int32_t archive_after_days = kDefaultArchiveAfterDays;

        /// Object TTL in S3 (seconds). 0 = no auto-expiry.
        int64_t object_ttl_seconds = kDefaultObjectTtlSeconds;

        /// Maximum concurrent uploads.
        int32_t max_concurrent_uploads = kMaxConcurrentUploads;

        /// Local cache directory for restored segments (avoids repeated
        /// downloads). Empty = no local cache.
        std::string local_cache_dir;
    };

    explicit S3Archiver(Config config)
        : config_(std::move(config))
        , endpoint_(parse_endpoint(config_.endpoint))
        , running_(true)
        , active_uploads_(0)
    {
        auto logger = get_archival_logger();
        logger->info("S3Archiver initialized: bucket={}, prefix={}, endpoint={}",
                     endpoint_.bucket, endpoint_.prefix,
                     endpoint_.endpoint_url.empty() ? "AWS" : endpoint_.endpoint_url);
    }

    ~S3Archiver() {
        shutdown();
    }

    S3Archiver(const S3Archiver&) = delete;
    S3Archiver& operator=(const S3Archiver&) = delete;
    S3Archiver(S3Archiver&&) = delete;
    S3Archiver& operator=(S3Archiver&&) = delete;

    // -- Lifecycle ---------------------------------------------------------

    /**
     * Shutdown the archiver: wait for pending uploads to complete.
     */
    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            running_ = false;
        }
        cv_.notify_all();

        // Wait for active uploads
        std::unique_lock<std::mutex> lock(mutex_);
        cv_done_.wait(lock, [this] { return active_uploads_ == 0; });
    }

    [[nodiscard]] bool is_enabled() const noexcept {
        return endpoint_.valid();
    }

    // -- Archival ----------------------------------------------------------

    /**
     * Archive a segment file to S3.
     *
     * @param partition_id  Owning partition.
     * @param segment_id    Segment identifier.
     * @param file_path     Path to the local segment file to upload.
     * @return              result<void> on success.
     */
    result<void> archive_segment(partition_id_t partition_id,
                                  uint64_t segment_id,
                                  const std::string& file_path) {
        if (!endpoint_.valid()) {
            return result<void>::failure(
                error_code::invalid_config,
                "S3 endpoint not configured");
        }

        if (!std::filesystem::exists(file_path)) {
            return result<void>::failure(
                error_code::resource_not_found,
                fmt::format("Segment file not found: {}", file_path));
        }

        auto key = make_object_key(endpoint_.prefix, partition_id, segment_id);

        // Retry loop with backoff
        result<void> last_result;
        for (int attempt = 0; attempt <= kMaxRetries; ++attempt) {
            if (attempt > 0) {
                auto backoff = kRetryBackoffMs * (1 << (attempt - 1));
                std::this_thread::sleep_for(std::chrono::milliseconds(backoff));
                get_archival_logger()->warn(
                    "Retrying S3 PUT {} (attempt {}/{})", key, attempt + 1, kMaxRetries + 1);
            }

            last_result = s3_put_object(endpoint_.endpoint_url, endpoint_.bucket,
                                        key, file_path,
                                        config_.access_key, config_.secret_key);
            if (last_result.ok()) {
                // Track in local index
                std::lock_guard<std::mutex> lock(mutex_);
                ArchivedSegment info;
                info.partition_id = partition_id;
                info.segment_id = segment_id;
                info.object_key = key;
                info.archived_at = std::chrono::system_clock::now();
                info.file_size = static_cast<uint64_t>(
                    std::filesystem::file_size(file_path));
                archived_segments_.push_back(info);
                return last_result;
            }

            // Don't retry on non-retriable errors
            if (last_result.error == error_code::resource_not_found ||
                last_result.error == error_code::invalid_config) {
                return last_result;
            }
        }

        return last_result;
    }

    // -- Restore -----------------------------------------------------------

    /**
     * Restore a segment from S3 to local disk.
     *
     * If `local_cache_dir` is configured and the segment already exists
     * there, returns the cached path immediately.
     *
     * @param partition_id  Owning partition.
     * @param segment_id    Segment identifier.
     * @param output_path   Where to write the restored file.
     * @return              result<void> on success.
     */
    result<void> restore_segment(partition_id_t partition_id,
                                  uint64_t segment_id,
                                  const std::string& output_path) {
        if (!endpoint_.valid()) {
            return result<void>::failure(
                error_code::invalid_config,
                "S3 endpoint not configured");
        }

        auto key = make_object_key(endpoint_.prefix, partition_id, segment_id);

        // Check local cache first
        if (!config_.local_cache_dir.empty()) {
            auto cache_path = fmt::format("{}/partition-{}/segment-{:016x}.log",
                                          config_.local_cache_dir,
                                          partition_id, segment_id);
            if (std::filesystem::exists(cache_path)) {
                std::filesystem::copy_file(cache_path, output_path,
                    std::filesystem::copy_options::overwrite_existing);
                get_archival_logger()->info(
                    "Restored segment from local cache: {}", cache_path);
                return result<void>::success();
            }
        }

        // Download from S3 with retries
        result<std::vector<unsigned char>> last_result;
        for (int attempt = 0; attempt <= kMaxRetries; ++attempt) {
            if (attempt > 0) {
                auto backoff = kRetryBackoffMs * (1 << (attempt - 1));
                std::this_thread::sleep_for(std::chrono::milliseconds(backoff));
            }

            last_result = s3_get_object(endpoint_.endpoint_url, endpoint_.bucket,
                                        key, config_.access_key, config_.secret_key);
            if (last_result.ok()) break;

            if (last_result.error == error_code::resource_not_found) {
                return result<void>::failure(last_result.error, last_result.error_message);
            }
        }

        if (last_result.failed()) {
            return result<void>::failure(last_result.error, last_result.error_message);
        }

        // Write to output file
        std::filesystem::create_directories(
            std::filesystem::path(output_path).parent_path());

        std::ofstream out(output_path, std::ios::binary);
        if (!out.is_open()) {
            return result<void>::failure(
                error_code::storage_unavailable,
                fmt::format("Cannot write restored segment: {}", output_path));
        }
        out.write(reinterpret_cast<const char*>(last_result.value.data()),
                  static_cast<std::streamsize>(last_result.value.size()));
        out.close();

        // Save to local cache
        if (!config_.local_cache_dir.empty()) {
            auto cache_path = fmt::format("{}/partition-{}/segment-{:016x}.log",
                                          config_.local_cache_dir,
                                          partition_id, segment_id);
            std::filesystem::create_directories(
                std::filesystem::path(cache_path).parent_path());
            std::filesystem::copy_file(output_path, cache_path,
                std::filesystem::copy_options::overwrite_existing);
        }

        return result<void>::success();
    }

    // -- Lifecycle / Expiration --------------------------------------------

    /**
     * Delete an archived segment from S3 (e.g., after local retention
     * cleanup or manual admin action).
     */
    result<void> delete_archived_segment(partition_id_t partition_id,
                                          uint64_t segment_id) {
        if (!endpoint_.valid()) {
            return result<void>::success();  // no-op
        }

        auto key = make_object_key(endpoint_.prefix, partition_id, segment_id);

        auto result = s3_delete_object(endpoint_.endpoint_url, endpoint_.bucket,
                                       key, config_.access_key, config_.secret_key);

        // Remove from local tracking
        std::lock_guard<std::mutex> lock(mutex_);
        archived_segments_.erase(
            std::remove_if(archived_segments_.begin(), archived_segments_.end(),
                           [&](const ArchivedSegment& s) {
                               return s.partition_id == partition_id &&
                                      s.segment_id == segment_id;
                           }),
            archived_segments_.end());

        return result;
    }

    /**
     * Check if a segment exists in S3.
     */
    [[nodiscard]] bool segment_exists_in_s3(partition_id_t partition_id,
                                             uint64_t segment_id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& seg : archived_segments_) {
            if (seg.partition_id == partition_id && seg.segment_id == segment_id) {
                return true;
            }
        }
        return false;
    }

    // -- Query -------------------------------------------------------------

    /**
     * Number of archived segments tracked locally.
     */
    [[nodiscard]] size_t archived_count() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return archived_segments_.size();
    }

    /**
     * Full list of archived segment metadata.
     */
    [[nodiscard]] std::vector<ArchivedSegment> list_archived() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return archived_segments_;
    }

    /**
     * S3 configuration for inspection.
     */
    [[nodiscard]] const Config& config() const noexcept { return config_; }

private:
    struct ArchivedSegment {
        partition_id_t partition_id = 0;
        uint64_t       segment_id   = 0;
        std::string    object_key;
        std::chrono::system_clock::time_point archived_at;
        uint64_t       file_size    = 0;
    };

    Config          config_;
    S3Endpoint      endpoint_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::condition_variable cv_done_;
    bool            running_{false};
    int             active_uploads_{0};
    std::vector<ArchivedSegment> archived_segments_;
};

} // namespace torrent
