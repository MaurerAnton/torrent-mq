#pragma once

/**
 * torrent-mq — Tiered Storage (S3 Archival & Restore)
 *
 * Enables seamless archival of sealed log segments to S3-compatible object
 * storage and on-demand restoration when consumers request data that has
 * been evicted from local disk.
 *
 * Architecture:
 *   S3Client         — Low-level S3 REST API wrapper (PUT/GET/DELETE/HEAD)
 *   ArchiveManager   — Selects eligible segments, compresses (optional),
 *                       uploads to S3, verifies integrity, then deletes local.
 *   RestoreManager   — Downloads segments from S3 on-demand, decompresses,
 *                       verifies, and makes available to the log reader.
 *
 * Lifecycle policies:
 *   transition_after_ms  — Age threshold before a sealed segment is archived.
 *   expire_after_ms      — Age threshold before an archived object is deleted.
 *
 * Thread safety: all public methods use internal mutex serialisation.
 */

#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <shared_mutex>
#include <mutex>
#include <chrono>
#include <functional>
#include <optional>

#include "torrent/common/types.h"
#include "torrent/storage/types.h"

namespace torrent {

// ============================================================================
// TieredStorageConfig — configuration for S3 archival and lifecycle
// ============================================================================

/**
 * Configuration for the tiered storage subsystem.
 *
 * Supports any S3-compatible object store (AWS S3, MinIO, Ceph RGW,
 * Cloudflare R2, etc.) via configurable endpoint and credentials.
 */
struct TieredStorageConfig {
    /// S3 bucket name (required).
    std::string bucket;

    /// S3 region (e.g. "us-east-1"). Default empty for MinIO compat.
    std::string region;

    /// S3 endpoint URL (empty = use default AWS endpoint).
    /// Set to "http://localhost:9000" for MinIO, etc.
    std::string endpoint;

    /// S3 access key ID (or AWS_ACCESS_KEY_ID env var).
    std::string access_key;

    /// S3 secret access key (or AWS_SECRET_ACCESS_KEY env var).
    std::string secret_key;

    /// Path prefix within the bucket for all torrent-mq objects
    /// (e.g. "torrent-mq/production/"). Default empty = root of bucket.
    std::string key_prefix;

    /// When true, encrypt objects at rest using SSE-S3 or SSE-KMS.
    /// Default false (plaintext).
    bool server_side_encryption = false;

    /// KMS key ID for SSE-KMS (empty = use SSE-S3 if encryption enabled).
    std::string kms_key_id;

    /// Compress segment data before uploading to S3.
    /// Default zstd — good compression ratio and speed.
    compression_type upload_compression = compression_type::zstd;

    /// When false, upload raw segments with no compression.
    bool enable_compression = true;

    /// Maximum number of concurrent S3 uploads. Default 4.
    int32_t max_concurrent_uploads = 4;

    /// Maximum number of concurrent S3 downloads. Default 4.
    int32_t max_concurrent_downloads = 4;

    /// Multipart upload part size (bytes). Default 16 MiB.
    byte_count_t upload_part_size = 16777216;

    /// Download chunk size (bytes) for streaming reads. Default 1 MiB.
    byte_count_t download_chunk_size = 1048576;

    /// Age threshold (milliseconds) after which a sealed segment is
    /// eligible for archival to S3. Default 0 = immediate.
    duration_ms_t transition_after_ms = 0;

    /// Age threshold (milliseconds) after which an archived S3 object
    /// is eligible for deletion. Default -1 = never expire.
    duration_ms_t expire_after_ms = -1;

    /// Object key template. Supports {topic}, {partition}, {segment_id},
    /// {base_offset} placeholders. Default:
    /// "{topic}/{partition}/{segment_id:020d}-{base_offset}.log"
    std::string key_template;

    /// HTTP connection timeout for S3 requests (milliseconds). Default 30s.
    duration_ms_t connection_timeout_ms = 30000;

    /// HTTP request timeout for S3 requests (milliseconds). Default 60s.
    duration_ms_t request_timeout_ms = 60000;

    /// Max retries for transient S3 errors (5xx, network). Default 3.
    int32_t max_retries = 3;

    /// Backoff base between retries (milliseconds). Default 100ms.
    duration_ms_t retry_backoff_ms = 100;

    /// When true, verify uploaded objects by issuing a HEAD request
    /// and comparing size/ETag. Default true.
    bool verify_uploads = true;

    /// When true, verify downloaded objects against a SHA-256 checksum
    /// stored alongside the segment. Default false.
    bool verify_downloads = false;

    /// When true, the archival and restore managers run in dry-run
    /// mode (log actions, no actual S3 operations). Default false.
    bool dry_run = false;

    TieredStorageConfig();
};

// ============================================================================
// S3ObjectMetadata — metadata for an archived S3 object
// ============================================================================

/**
 * Describes a segment that has been archived to S3.
 * Stored in a local metadata index (SQLite or flat file) so the
 * restore path can quickly determine what exists in S3.
 */
struct S3ObjectMetadata {
    /// S3 object key (full path within bucket, including prefix).
    std::string object_key;

    /// Topic this segment belongs to.
    topic_id_t topic_id = 0;

    /// Partition this segment belongs to.
    partition_id_t partition_id = 0;

    /// Segment identifier (partition-local, monotonically increasing).
    uint64_t segment_id = 0;

    /// First logical offset in this segment (inclusive).
    offset_t base_offset = kInvalidOffset;

    /// Next logical offset after this segment (exclusive).
    offset_t next_offset = kInvalidOffset;

    /// Compressed size of the object in S3 (bytes).
    byte_count_t object_size = 0;

    /// Original segment size before compression (bytes).
    byte_count_t original_size = 0;

    /// Compression used (none if compression was disabled).
    compression_type compression = compression_type::none;

    /// S3 ETag of the uploaded object (for integrity verification).
    std::string etag;

    /// SHA-256 checksum of the original uncompressed segment data.
    std::string sha256_checksum;

    /// Timestamp when this object was archived to S3 (ms epoch).
    timestamp_ms_t archived_at = 0;

    /// Wall-clock time when this segment was originally created.
    timestamp_ms_t segment_created_at = 0;

    /// Maximum timestamp among records in this segment.
    timestamp_ms_t max_timestamp = 0;

    /// Number of records in this segment.
    int64_t record_count = 0;

    /// True when the object has been verified after upload.
    bool verified = false;

    /// True when this object is scheduled for expiration.
    bool expired = false;

    [[nodiscard]] bool empty() const noexcept {
        return object_key.empty();
    }
};

// ============================================================================
// S3Client — low-level S3 REST API wrapper
// ============================================================================

/**
 * Thin wrapper around S3-compatible object storage operations.
 *
 * Implements the minimum set of S3 REST API calls needed for archival:
 * PutObject, GetObject, DeleteObject, HeadObject.
 *
 * Uses raw HTTPS with libcurl or a minimal HTTP client internally.
 * Authentication: AWS Signature V4.
 *
 * Thread-safe: each method is independently callable from any thread.
 */
class S3Client {
public:
    /// Result of a successful S3 command.
    struct S3Result {
        bool success = false;
        error_code error = error_code::none;
        std::string error_message;

        /// For HEAD/PUT: the object ETag if available.
        std::string etag;

        /// For HEAD: object size in bytes.
        byte_count_t object_size = 0;

        /// For GET: the downloaded object body.
        std::vector<uint8_t> body;

        /// HTTP status code from the S3 response.
        int32_t http_status = 0;

        [[nodiscard]] bool ok() const noexcept { return success; }
    };

    explicit S3Client(const TieredStorageConfig& config);
    ~S3Client();

    S3Client(const S3Client&) = delete;
    S3Client& operator=(const S3Client&) = delete;
    S3Client(S3Client&&) noexcept = default;
    S3Client& operator=(S3Client&&) noexcept = default;

    // ----------------------------------------------------------------
    // Object operations
    // ----------------------------------------------------------------

    /// Upload data to an S3 object.
    /// Uses multipart upload for data larger than upload_part_size.
    S3Result put_object(const std::string& key,
                        const void* data,
                        byte_count_t size,
                        const std::string& content_type = "application/octet-stream");

    /// Download an S3 object into a buffer.
    S3Result get_object(const std::string& key);

    /// Download a byte range of an S3 object.
    S3Result get_object_range(const std::string& key,
                              byte_count_t start_byte,
                              byte_count_t end_byte);

    /// Delete an S3 object. Returns success even if the object doesn't exist.
    S3Result delete_object(const std::string& key);

    /// Check if an S3 object exists and get its metadata.
    S3Result head_object(const std::string& key);

    // ----------------------------------------------------------------
    // Bulk operations
    // ----------------------------------------------------------------

    /// Delete multiple objects in a single request (up to 1000 keys).
    S3Result delete_objects(const std::vector<std::string>& keys);

    /// List objects with a given prefix. Returns up to max_keys results.
    S3Result list_objects(const std::string& prefix, int32_t max_keys = 1000);

    // ----------------------------------------------------------------
    // Accessors
    // ----------------------------------------------------------------

    [[nodiscard]] const TieredStorageConfig& config() const noexcept { return config_; }

    /// Generate the full S3 object key for a segment.
    [[nodiscard]] std::string make_key(topic_id_t topic_id,
                                       partition_id_t partition_id,
                                       uint64_t segment_id,
                                       offset_t base_offset) const;

    /// Parse a topic name to its numeric ID (for key generation).
    [[nodiscard]] std::string make_key_by_name(const std::string& topic_name,
                                                partition_id_t partition_id,
                                                uint64_t segment_id,
                                                offset_t base_offset) const;

private:
    /// Build the Authorization header using AWS Signature V4.
    std::string build_auth_header(const std::string& method,
                                  const std::string& key,
                                  const std::string& payload_hash,
                                  const std::string& content_type) const;

    /// Perform an HTTP request with retries.
    S3Result do_request(const std::string& method,
                        const std::string& key,
                        const std::vector<uint8_t>* body,
                        const std::string& content_type,
                        std::vector<uint8_t>* response_body,
                        const std::string& range_header = "");

    TieredStorageConfig config_;
};

// ============================================================================
// ArchiveManager — segment archival to S3
// ============================================================================

/**
 * Manages the archival of sealed, eligible local segments to S3.
 *
 * Runs periodically (or triggered by disk pressure). For each eligible
 * segment:
 *   1. Read segment data from local disk.
 *   2. Optionally compress with the configured algorithm.
 *   3. Compute SHA-256 checksum for integrity.
 *   4. Upload to S3 via S3Client::put_object().
 *   5. Verify upload via S3Client::head_object().
 *   6. Record metadata in the local index.
 *   7. Delete the local segment file (data, index, timeindex).
 */
class ArchiveManager {
public:
    /// Result of a single segment archival operation.
    struct ArchiveResult {
        S3ObjectMetadata object;
        bool      success = false;
        error_code error = error_code::none;
        std::string error_message;

        [[nodiscard]] bool ok() const noexcept { return success; }
    };

    /// Result of an archival run (batch of segments).
    struct BatchArchiveResult {
        /// All per-segment results from this run.
        std::vector<ArchiveResult> results;

        /// Count of successful archives.
        int32_t successful = 0;

        /// Count of failed archives.
        int32_t failed = 0;

        /// Total bytes uploaded (compressed size).
        byte_count_t bytes_uploaded = 0;

        /// Total bytes freed from local disk.
        byte_count_t bytes_freed = 0;

        /// Duration of the archival run (milliseconds).
        duration_ms_t duration_ms = 0;

        /// Overall error (none on success).
        error_code error = error_code::none;
        std::string error_message;

        [[nodiscard]] bool ok() const noexcept { return error == error_code::none; }
    };

    /// Callback type invoked for each segment as it is archived.
    using progress_callback_t = std::function<void(const S3ObjectMetadata&)>;

    explicit ArchiveManager(const TieredStorageConfig& config,
                            std::shared_ptr<S3Client> s3_client);
    ~ArchiveManager();

    ArchiveManager(const ArchiveManager&) = delete;
    ArchiveManager& operator=(const ArchiveManager&) = delete;
    ArchiveManager(ArchiveManager&&) = delete;
    ArchiveManager& operator=(ArchiveManager&&) = delete;

    // ----------------------------------------------------------------
    // Selection
    // ----------------------------------------------------------------

    /**
     * Evaluate which sealed segments are eligible for archival.
     *
     * A segment is eligible if:
     *   - It is sealed (not the active segment).
     *   - It has not already been archived.
     *   - Its max_timestamp is older than config_.transition_after_ms.
     *
     * @param segments    All segments for a partition, ordered by offset.
     * @param topic_name  Human-readable topic name for key generation.
     * @param partition_id The partition being evaluated.
     * @param now         Current wall-clock time (0 = auto).
     * @return Vector of eligible segments sorted by age (oldest first).
     */
    [[nodiscard]] std::vector<SegmentInfo> select_eligible_segments(
        const std::vector<SegmentInfo>& segments,
        const std::string& topic_name,
        partition_id_t partition_id,
        timestamp_ms_t now = 0) const;

    // ----------------------------------------------------------------
    // Archival
    // ----------------------------------------------------------------

    /// Archive a single segment to S3.
    /// Reads the segment from disk, compresses, uploads, verifies, deletes local.
    ArchiveResult archive_segment(const SegmentInfo& segment,
                                   const std::string& data_dir,
                                   topic_id_t topic_id,
                                   const std::string& topic_name,
                                   partition_id_t partition_id);

    /// Archive a batch of segments.
    /// Uploads are limited by config_.max_concurrent_uploads.
    BatchArchiveResult archive_batch(const std::vector<SegmentInfo>& segments,
                                      const std::string& data_dir,
                                      topic_id_t topic_id,
                                      const std::string& topic_name,
                                      partition_id_t partition_id);

    /// Archive all eligible segments across all partitions in a data directory.
    BatchArchiveResult archive_all_eligible(
        const std::string& data_dir,
        const std::vector<std::pair<topic_id_t, std::string>>& topics,
        timestamp_ms_t now = 0);

    // ----------------------------------------------------------------
    // Lifecycle management
    // ----------------------------------------------------------------

    /// Delete S3 objects that have exceeded the expiration threshold.
    BatchArchiveResult expire_objects(timestamp_ms_t now = 0);

    /// Check if an object has exceeded config_.expire_after_ms.
    [[nodiscard]] bool is_expired(const S3ObjectMetadata& object,
                                   timestamp_ms_t now) const noexcept;

    // ----------------------------------------------------------------
    // Progress tracking
    // ----------------------------------------------------------------

    /// Register a callback invoked as each segment is archived.
    void on_progress(progress_callback_t callback);

    // ----------------------------------------------------------------
    // Accessors
    // ----------------------------------------------------------------

    [[nodiscard]] const TieredStorageConfig& config() const noexcept { return config_; }
    [[nodiscard]] const std::vector<S3ObjectMetadata>& archived_objects() const noexcept {
        return archived_objects_;
    }

private:
    /// Compress segment data before upload.
    std::vector<uint8_t> compress_data(const void* data, byte_count_t size);

    /// Compute SHA-256 of given data.
    std::string compute_sha256(const void* data, byte_count_t size);

    /// Delete local segment files (data + index + timeindex).
    void delete_local_segment(const SegmentInfo& segment, const std::string& data_dir);

    TieredStorageConfig config_;
    std::shared_ptr<S3Client> s3_client_;
    std::vector<S3ObjectMetadata> archived_objects_;
    progress_callback_t progress_callback_;
    mutable std::mutex mutex_;
};

// ============================================================================
// RestoreManager — on-demand segment restore from S3
// ============================================================================

/**
 * Manages on-demand restoration of archived segments from S3.
 *
 * When a consumer requests data at offsets that fall within an archived
 * segment, the RestoreManager:
 *   1. Looks up the S3 object key from the metadata index.
 *   2. Downloads the object from S3.
 *   3. Decompresses (if needed).
 *   4. Verifies integrity (SHA-256 check if configured).
 *   5. Writes the segment to local disk.
 *   6. Returns the segment metadata so the log reader can proceed.
 */
class RestoreManager {
public:
    /// Result of restoring a single segment from S3.
    struct RestoreResult {
        S3ObjectMetadata  object;
        SegmentInfo       segment_info;
        bool              success = false;
        error_code        error = error_code::none;
        std::string       error_message;

        /// Path to the restored local segment file.
        std::string       local_path;

        /// Bytes downloaded from S3 (compressed size).
        byte_count_t      bytes_downloaded = 0;

        /// Bytes written to local disk (decompressed size).
        byte_count_t      bytes_written = 0;

        /// Duration of the restore operation (milliseconds).
        duration_ms_t     duration_ms = 0;

        [[nodiscard]] bool ok() const noexcept { return success; }
    };

    explicit RestoreManager(const TieredStorageConfig& config,
                            std::shared_ptr<S3Client> s3_client);
    ~RestoreManager();

    RestoreManager(const RestoreManager&) = delete;
    RestoreManager& operator=(const RestoreManager&) = delete;
    RestoreManager(RestoreManager&&) = delete;
    RestoreManager& operator=(RestoreManager&&) = delete;

    // ----------------------------------------------------------------
    // Restore operations
    // ----------------------------------------------------------------

    /// Restore a specific segment by its S3 metadata.
    /// Downloads the object, decompresses, verifies, and writes locally.
    RestoreResult restore_segment(const S3ObjectMetadata& object,
                                   const std::string& data_dir,
                                   const std::string& topic_name);

    /// Restore the segment containing the given offset for a partition.
    /// Looks up the S3 metadata index first, then downloads.
    RestoreResult restore_by_offset(topic_id_t topic_id,
                                     partition_id_t partition_id,
                                     offset_t offset,
                                     const std::string& data_dir,
                                     const std::string& topic_name);

    /// Restore multiple segments (downloaded sequentially or with
    /// limited concurrency per config_.max_concurrent_downloads).
    std::vector<RestoreResult> restore_batch(
        const std::vector<S3ObjectMetadata>& objects,
        const std::string& data_dir,
        const std::string& topic_name);

    // ----------------------------------------------------------------
    // Metadata index
    // ----------------------------------------------------------------

    /// Register an archived object in the in-memory index.
    /// Called by ArchiveManager after a successful archive.
    void register_object(const S3ObjectMetadata& object);

    /// Remove an object from the in-memory index (after expiration).
    void unregister_object(const std::string& object_key);

    /// Find the S3 object containing the given offset for a partition.
    [[nodiscard]] std::optional<S3ObjectMetadata> find_by_offset(
        topic_id_t topic_id,
        partition_id_t partition_id,
        offset_t offset) const;

    /// Get all archived objects for a given topic/partition.
    [[nodiscard]] std::vector<S3ObjectMetadata> list_objects(
        topic_id_t topic_id,
        partition_id_t partition_id) const;

    // ----------------------------------------------------------------
    // Accessors
    // ----------------------------------------------------------------

    [[nodiscard]] const TieredStorageConfig& config() const noexcept { return config_; }

private:
    /// Decompress segment data after download.
    std::vector<uint8_t> decompress_data(const void* data,
                                          byte_count_t size,
                                          compression_type ct);

    /// Verify SHA-256 checksum of decompressed data.
    bool verify_checksum(const void* data, byte_count_t size,
                         const std::string& expected_sha256);

    /// Write restored segment to local disk.
    bool write_local_segment(const std::string& file_path,
                             const void* data,
                             byte_count_t size);

    TieredStorageConfig config_;
    std::shared_ptr<S3Client> s3_client_;

    /// In-memory index: (topic_id, partition_id) -> sorted vector of objects.
    using ObjectKey = std::pair<topic_id_t, partition_id_t>;
    std::map<ObjectKey, std::vector<S3ObjectMetadata>> index_;
    mutable std::shared_mutex index_mutex_;
};

} // namespace torrent
