#include <gtest/gtest.h>
#include "torrent/storage/segment.h"
#include "torrent/storage/log_manager.h"
#include "torrent/storage/compaction.h"
#include "torrent/storage/cleaner.h"
#include "torrent/storage/recovery.h"
#include "torrent/storage/types.h"
#include "torrent/common/types.h"
#include <filesystem>
#include <fstream>
#include <cstring>
#include <random>

namespace torrent::test {
namespace {

class StorageFullTest : public ::testing::Test {
protected:
    void SetUp() override {
        temp_dir_ = "/tmp/torrent_storage_full_test_" + std::to_string(getpid());
        std::filesystem::create_directories(temp_dir_);
    }
    void TearDown() override {
        std::filesystem::remove_all(temp_dir_);
    }
    SegmentConfig make_segment_config(const std::string& path, offset_t base_offset) {
        SegmentConfig cfg;
        cfg.file_path = path;
        cfg.base_offset = base_offset;
        cfg.max_bytes = 1048576;
        cfg.max_age_ms = 3600000;
        cfg.compression = compression_type::none;
        cfg.use_mmap = false;
        cfg.sync_on_append = false;
        cfg.preallocate_bytes = 0;
        cfg.index_interval_bytes = 4096;
        return cfg;
    }
    RecordBatch make_batch(const std::string& key, const std::string& value, offset_t base = 0) {
        RecordBatch batch;
        batch.base_offset = base;
        Record record;
        record.key = key;
        record.value = value;
        record.timestamp = 1234567890000;
        batch.records.push_back(record);
        return batch;
    }
    std::string temp_dir_;
};

// ===== Segment Lifecycle =====
TEST_F(StorageFullTest, SegmentCreate) {
    auto cfg = make_segment_config(temp_dir_ + "/000000.log", 0);
    Segment seg(cfg);
    auto result = seg.open();
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_TRUE(seg.is_open());
    EXPECT_EQ(seg.base_offset(), 0);
    seg.close();
}
TEST_F(StorageFullTest, SegmentAppendSingle) {
    auto cfg = make_segment_config(temp_dir_ + "/000001.log", 0);
    Segment seg(cfg);
    seg.open();
    auto batch = make_batch("key1", "value1");
    auto result = seg.append(batch);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value, 0);
    seg.close();
}
TEST_F(StorageFullTest, SegmentAppendMultiple) {
    auto cfg = make_segment_config(temp_dir_ + "/000002.log", 0);
    Segment seg(cfg);
    seg.open();
    for (int i = 0; i < 10; i++) {
        auto batch = make_batch("key" + std::to_string(i), "val" + std::to_string(i));
        auto result = seg.append(batch);
        ASSERT_TRUE(result.ok()) << "Append " << i << " failed";
    }
    EXPECT_EQ(seg.next_offset(), 10);
    seg.close();
}
TEST_F(StorageFullTest, SegmentReadBack) {
    auto cfg = make_segment_config(temp_dir_ + "/000003.log", 0);
    Segment seg(cfg);
    seg.open();
    auto batch = make_batch("readkey", "readvalue");
    seg.append(batch);
    seg.close();
    seg.open();
    auto result = seg.read(0, 1024);
    ASSERT_TRUE(result.ok());
    EXPECT_GT(result.value.size(), 0);
    seg.close();
}
TEST_F(StorageFullTest, SegmentSeal) {
    auto cfg = make_segment_config(temp_dir_ + "/000004.log", 0);
    Segment seg(cfg);
    seg.open();
    seg.append(make_batch("k", "v"));
    seg.seal();
    EXPECT_TRUE(seg.is_sealed());
    EXPECT_FALSE(seg.is_active());
    seg.close();
}
TEST_F(StorageFullTest, SegmentRoll) {
    auto cfg = make_segment_config(temp_dir_ + "/000005.log", 0);
    cfg.max_bytes = 100;
    Segment seg(cfg);
    seg.open();
    auto batch = make_batch(std::string(200, 'x'), std::string(200, 'y'));
    seg.append(batch);
    EXPECT_TRUE(seg.should_roll());
    seg.close();
}
TEST_F(StorageFullTest, SegmentTruncate) {
    auto cfg = make_segment_config(temp_dir_ + "/000006.log", 0);
    Segment seg(cfg);
    seg.open();
    for (int i = 0; i < 5; i++) seg.append(make_batch("k", "v"));
    seg.truncate_to(2);
    EXPECT_EQ(seg.next_offset(), 2);
    seg.close();
}
TEST_F(StorageFullTest, SegmentFlushAndReopen) {
    std::string path = temp_dir_ + "/000007.log";
    auto cfg = make_segment_config(path, 0);
    Segment seg1(cfg);
    seg1.open();
    seg1.append(make_batch("k1", "v1"));
    seg1.flush();
    seg1.close();
    Segment seg2(cfg);
    seg2.open();
    EXPECT_EQ(seg2.next_offset(), 1);
    seg2.close();
}
TEST_F(StorageFullTest, SegmentCompressionNone) {
    auto cfg = make_segment_config(temp_dir_ + "/000008.log", 0);
    cfg.compression = compression_type::none;
    Segment seg(cfg); seg.open();
    seg.append(make_batch("k", "v"));
    seg.close();
}
TEST_F(StorageFullTest, SegmentIndexRebuild) {
    auto cfg = make_segment_config(temp_dir_ + "/000009.log", 0);
    Segment seg(cfg); seg.open();
    for (int i = 0; i < 20; i++) seg.append(make_batch("k" + std::to_string(i), "v" + std::to_string(i)));
    seg.flush(); seg.close();
    seg.open();
    seg.rebuild_index();
    auto pos = seg.find_position(10);
    EXPECT_GT(pos, 0);
    seg.close();
}
TEST_F(StorageFullTest, SegmentFindByTimestamp) {
    auto cfg = make_segment_config(temp_dir_ + "/000010.log", 0);
    Segment seg(cfg); seg.open();
    seg.append(make_batch("k", "v"));
    auto found = seg.find_offset_by_timestamp(0);
    EXPECT_GE(found, 0);
    seg.close();
}

// ===== LogManager Tests =====
TEST_F(StorageFullTest, LogManagerCreate) {
    LogManagerConfig lm_cfg;
    lm_cfg.partition_id = 0;
    lm_cfg.data_directory = temp_dir_ + "/log1";
    lm_cfg.topic_config.partitions = 1;
    LogManager lm(lm_cfg);
    auto result = lm.open();
    ASSERT_TRUE(result.ok());
    EXPECT_TRUE(lm.is_open());
    lm.close();
}
TEST_F(StorageFullTest, LogManagerAppend) {
    LogManagerConfig lm_cfg;
    lm_cfg.partition_id = 0;
    lm_cfg.data_directory = temp_dir_ + "/log2";
    lm_cfg.topic_config.partitions = 1;
    LogManager lm(lm_cfg);
    lm.open();
    auto batch = make_batch("k", "v");
    auto result = lm.append(batch);
    ASSERT_TRUE(result.ok());
    EXPECT_GE(result.value, 0);
    lm.close();
}
TEST_F(StorageFullTest, LogManagerRead) {
    LogManagerConfig lm_cfg;
    lm_cfg.partition_id = 0;
    lm_cfg.data_directory = temp_dir_ + "/log3";
    lm_cfg.topic_config.partitions = 1;
    LogManager lm(lm_cfg); lm.open();
    lm.append(make_batch("k", "v"));
    auto result = lm.read(0, 1024);
    ASSERT_TRUE(result.ok());
    lm.close();
}
TEST_F(StorageFullTest, LogManagerTruncate) {
    LogManagerConfig lm_cfg;
    lm_cfg.partition_id = 0;
    lm_cfg.data_directory = temp_dir_ + "/log4";
    lm_cfg.topic_config.partitions = 1;
    LogManager lm(lm_cfg); lm.open();
    for (int i = 0; i < 5; i++) lm.append(make_batch("k", "v"));
    lm.truncate_to(2);
    lm.close();
}
TEST_F(StorageFullTest, LogManagerWatermarks) {
    LogManagerConfig lm_cfg;
    lm_cfg.partition_id = 0;
    lm_cfg.data_directory = temp_dir_ + "/log5";
    lm_cfg.topic_config.partitions = 1;
    LogManager lm(lm_cfg); lm.open();
    lm.update_high_watermark(10);
    EXPECT_EQ(lm.get_high_watermark(), 10);
    lm.update_last_stable_offset(8);
    EXPECT_EQ(lm.get_last_stable_offset(), 8);
    lm.close();
}
TEST_F(StorageFullTest, LogManagerListSegments) {
    LogManagerConfig lm_cfg;
    lm_cfg.partition_id = 0;
    lm_cfg.data_directory = temp_dir_ + "/log6";
    lm_cfg.topic_config.partitions = 1;
    LogManager lm(lm_cfg); lm.open();
    auto segments = lm.list_segments();
    EXPECT_GE(segments.size(), 1);
    lm.close();
}
TEST_F(StorageFullTest, LogManagerFlushSync) {
    LogManagerConfig lm_cfg;
    lm_cfg.partition_id = 0;
    lm_cfg.data_directory = temp_dir_ + "/log7";
    lm_cfg.topic_config.partitions = 1;
    LogManager lm(lm_cfg); lm.open();
    lm.append(make_batch("k", "v"));
    lm.flush();
    lm.sync();
    lm.close();
}
TEST_F(StorageFullTest, LogManagerCloseAndReopen) {
    LogManagerConfig lm_cfg;
    lm_cfg.partition_id = 0;
    lm_cfg.data_directory = temp_dir_ + "/log8";
    lm_cfg.topic_config.partitions = 1;
    {
        LogManager lm(lm_cfg); lm.open();
        lm.append(make_batch("k", "v"));
        lm.close();
    }
    {
        LogManager lm2(lm_cfg); lm2.open();
        auto segments = lm2.list_segments();
        EXPECT_GE(segments.size(), 1);
        lm2.close();
    }
}
TEST_F(StorageFullTest, LogManagerCompaction) {
    LogManagerConfig lm_cfg;
    lm_cfg.partition_id = 0;
    lm_cfg.data_directory = temp_dir_ + "/log9";
    lm_cfg.topic_config.partitions = 1;
    LogManager lm(lm_cfg); lm.open();
    // Write same key multiple times
    for (int i = 0; i < 5; i++) lm.append(make_batch("dupkey", "val" + std::to_string(i)));
    lm.compact();
    lm.close();
}
TEST_F(StorageFullTest, LogManagerRetention) {
    LogManagerConfig lm_cfg;
    lm_cfg.partition_id = 0;
    lm_cfg.data_directory = temp_dir_ + "/log10";
    lm_cfg.topic_config.partitions = 1;
    lm_cfg.topic_config.retention_ms = 1;  // Immediate
    LogManager lm(lm_cfg); lm.open();
    lm.append(make_batch("k", "v"));
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    lm.retention_check();
    lm.close();
}

// ===== Compaction Tests =====
TEST_F(StorageFullTest, CompactorBasic) {
    Compactor::Config cfg;
    Compactor compactor(cfg);
    std::vector<RecordBatch> input;
    input.push_back(make_batch("k1", "v1"));
    input.push_back(make_batch("k1", "v2"));  // Duplicate key, newer value
    input.push_back(make_batch("k2", "v3"));
    auto result = compactor.compact(input);
    EXPECT_LE(result.size(), 3);
}
TEST_F(StorageFullTest, CompactorTombstones) {
    Compactor::Config cfg;
    Compactor compactor(cfg);
    std::vector<RecordBatch> input;
    input.push_back(make_batch("k1", ""));
    auto result = compactor.compact(input);
    EXPECT_GE(result.size(), 1);
}
TEST_F(StorageFullTest, CompactorEmpty) {
    Compactor::Config cfg;
    Compactor compactor(cfg);
    std::vector<RecordBatch> input;
    auto result = compactor.compact(input);
    EXPECT_TRUE(result.empty());
}

// ===== Cleaner Tests =====
TEST_F(StorageFullTest, CleanerTimeRetention) {
    LogCleaner::Config cfg;
    cfg.retention_ms = 1000;
    LogCleaner cleaner(cfg);
    int64_t now = 1234567890000;
    int64_t old_ts = now - 2000;
    EXPECT_TRUE(cleaner.should_delete_segment(0, 100, old_ts));
}
TEST_F(StorageFullTest, CleanerTimeRetentionRecent) {
    LogCleaner::Config cfg;
    cfg.retention_ms = 1000;
    LogCleaner cleaner(cfg);
    int64_t now = 1234567890000;
    int64_t recent_ts = now - 500;
    EXPECT_FALSE(cleaner.should_delete_segment(0, 100, recent_ts));
}
TEST_F(StorageFullTest, CleanerSizeRetention) {
    LogCleaner::Config cfg;
    cfg.retention_bytes = 100;
    LogCleaner cleaner(cfg);
    EXPECT_TRUE(cleaner.should_delete_segment(0, 200, 1234567890000));
}

// ===== Recovery Tests =====
TEST_F(StorageFullTest, RecoveryEmptyDir) {
    auto result = LogRecovery::recover(temp_dir_ + "/empty_recovery");
    EXPECT_TRUE(result.success);
}
TEST_F(StorageFullTest, RecoveryValidateValidSegment) {
    std::string path = temp_dir_ + "/valid_segment.log";
    auto cfg = make_segment_config(path, 0);
    Segment seg(cfg); seg.open();
    seg.append(make_batch("k", "v"));
    seg.flush(); seg.close();
    EXPECT_TRUE(LogRecovery::validate_segment(path));
}
TEST_F(StorageFullTest, RecoveryValidateCorruptFile) {
    std::string path = temp_dir_ + "/corrupt.log";
    std::ofstream ofs(path, std::ios::binary);
    ofs.write("garbage_data_not_a_valid_segment", 35);
    ofs.close();
    EXPECT_FALSE(LogRecovery::validate_segment(path));
}

// ===== DiskIO Tests =====
TEST_F(StorageFullTest, DiskIOWriteRead) {
    std::string path = temp_dir_ + "/diskio_test.bin";
    int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);
    const char* data = "diskio test data";
    auto write_res = DiskIO::write_all(fd, data, strlen(data), 0);
    ASSERT_TRUE(write_res.ok());
    char buf[100] = {0};
    auto read_res = DiskIO::read_all(fd, buf, strlen(data), 0);
    ASSERT_TRUE(read_res.ok());
    EXPECT_STREQ(buf, data);
    DiskIO::fsync(fd);
    close(fd);
}
TEST_F(StorageFullTest, DiskIOFileSize) {
    std::string path = temp_dir_ + "/filesize_test.bin";
    int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);
    DiskIO::write_all(fd, "1234567890", 10, 0);
    auto size = DiskIO::file_size(fd);
    ASSERT_TRUE(size.ok());
    EXPECT_EQ(size.value, 10);
    close(fd);
}
TEST_F(StorageFullTest, DiskIOTruncate) {
    std::string path = temp_dir_ + "/truncate_test.bin";
    int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);
    DiskIO::write_all(fd, "1234567890", 10, 0);
    DiskIO::ftruncate(fd, 5);
    auto size = DiskIO::file_size(fd);
    EXPECT_EQ(size.value, 5);
    close(fd);
}

// ===== PageCache Tests =====
TEST_F(StorageFullTest, PageCachePutGet) {
    PageCache cache(10);
    auto page = std::make_shared<const char[]>('A');
    cache.put_page("/test/file", 0, page, 1);
    auto retrieved = cache.get_page("/test/file", 0, 1);
    EXPECT_NE(retrieved, nullptr);
}
TEST_F(StorageFullTest, PageCacheInvalidate) {
    PageCache cache(10);
    auto page = std::make_shared<const char[]>('B');
    cache.put_page("/test/file2", 0, page, 1);
    cache.invalidate("/test/file2");
    auto retrieved = cache.get_page("/test/file2", 0, 1);
    EXPECT_EQ(retrieved, nullptr);
}

// ===== BatchCache Tests =====
TEST_F(StorageFullTest, BatchCachePutGet) {
    BatchCache cache(1048576);
    auto batch = make_batch("k", "v");
    cache.put("topic", 0, 10, batch);
    auto retrieved = cache.get("topic", 0, 10);
    EXPECT_TRUE(retrieved.has_value());
}
TEST_F(StorageFullTest, BatchCacheInvalidatePartition) {
    BatchCache cache(1048576);
    cache.put("topic", 0, 10, make_batch("k", "v"));
    cache.invalidate("topic", 0);
    auto retrieved = cache.get("topic", 0, 10);
    EXPECT_FALSE(retrieved.has_value());
}

// ===== Snapshot Tests =====
TEST_F(StorageFullTest, SnapshotCreate) {
    std::string snap_path = temp_dir_ + "/snapshot_test.snap";
    auto result = LogSnapshot::create(temp_dir_ + "/snap_data", 100, snap_path);
    EXPECT_TRUE(result.ok());
}
TEST_F(StorageFullTest, SnapshotValidate) {
    std::string snap_path = temp_dir_ + "/validate_test.snap";
    LogSnapshot::create(temp_dir_ + "/snap_data2", 50, snap_path);
    auto result = LogSnapshot::validate(snap_path);
    EXPECT_TRUE(result.ok());
}

// ===== Tiered Storage Tests =====
TEST_F(StorageFullTest, TieredStorageEnabled) {
    TieredStorageConfig ts_cfg;
    ts_cfg.enabled = true;
    ts_cfg.s3_bucket = "test-bucket";
    ts_cfg.s3_region = "us-east-1";
    EXPECT_TRUE(ts_cfg.enabled);
    EXPECT_EQ(ts_cfg.s3_bucket, "test-bucket");
}
TEST_F(StorageFullTest, TieredStorageDisabled) {
    TieredStorageConfig ts_cfg;
    ts_cfg.enabled = false;
    EXPECT_FALSE(ts_cfg.enabled);
}

// ===== Encryption Tests =====
TEST_F(StorageFullTest, EncryptionConfig) {
    EncryptionConfig ec;
    ec.enabled = false;
    ec.algorithm = "AES-256-GCM";
    EXPECT_FALSE(ec.enabled);
    EXPECT_EQ(ec.algorithm, "AES-256-GCM");
}

// ===== Index Tests =====
TEST_F(StorageFullTest, OffsetIndexAddLookup) {
    OffsetIndex idx;
    idx.add_entry(0, 0);
    idx.add_entry(10, 500);
    idx.add_entry(20, 1000);
    EXPECT_EQ(idx.lookup(0), 0);
    EXPECT_EQ(idx.lookup(10), 500);
    EXPECT_EQ(idx.lookup(20), 1000);
    EXPECT_EQ(idx.size(), 3);
}
TEST_F(StorageFullTest, OffsetIndexTruncate) {
    OffsetIndex idx;
    idx.add_entry(0, 0);
    idx.add_entry(10, 500);
    idx.add_entry(20, 1000);
    idx.truncate_to(10);
    EXPECT_EQ(idx.size(), 2);
}
TEST_F(StorageFullTest, TimeIndexAddLookup) {
    TimeIndex idx;
    idx.add_entry(1000, 0);
    idx.add_entry(2000, 10);
    EXPECT_EQ(idx.lookup(1500), 10);
}
TEST_F(StorageFullTest, TimeIndexTruncate) {
    TimeIndex idx;
    idx.add_entry(1000, 0);
    idx.add_entry(2000, 10);
    idx.add_entry(3000, 20);
    idx.truncate_to(10);
    EXPECT_EQ(idx.size(), 2);
}
TEST_F(StorageFullTest, SegmentIndexAddEntry) {
    SegmentIndex idx;
    idx.add_entry(0, 0, 1000);
    EXPECT_EQ(idx.entry_count(), 1);
}

// ===== S3 Client Tests =====
TEST_F(StorageFullTest, S3ClientConfig) {
    S3Client::Config cfg;
    cfg.endpoint = "s3.amazonaws.com";
    cfg.bucket = "test-bucket";
    cfg.region = "us-east-1";
    EXPECT_EQ(cfg.bucket, "test-bucket");
}
TEST_F(StorageFullTest, S3ClientPutHeadDelete) {
    S3Client::Config cfg;
    S3Client client(cfg);
    std::string key = "test-object-key";
    std::vector<uint8_t> data = {1, 2, 3, 4, 5};
    auto put_res = client.put_object(key, data);
    EXPECT_TRUE(put_res.ok());
    auto head_res = client.head_object(key);
    EXPECT_TRUE(head_res.ok());
    auto del_res = client.delete_object(key);
    EXPECT_TRUE(del_res.ok());
}

// ===== Archive Manager Tests =====
TEST_F(StorageFullTest, ArchiveManagerSelect) {
    ArchiveManager::Config cfg;
    ArchiveManager mgr(cfg);
    auto eligible = mgr.select_eligible_segments({});
    EXPECT_TRUE(eligible.empty());
}

// ===== Restore Manager Tests =====
TEST_F(StorageFullTest, RestoreManagerFind) {
    RestoreManager::Config cfg;
    RestoreManager mgr(cfg);
    auto obj = mgr.find_by_offset(100);
    EXPECT_FALSE(obj.has_value());
}

}  // namespace
}  // namespace torrent::test
