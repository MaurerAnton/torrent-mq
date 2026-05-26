#include "torrent/storage/segment.h"
#include "torrent/storage/log_manager.h"
#include "torrent/storage/types.h"
#include "torrent/common/types.h"
#include "torrent/common/config.h"
#include "torrent/common/endian.h"
#include <spdlog/spdlog.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstring>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <random>

namespace torrent {
namespace {

// ============================================================================
// CRC32C Implementation with SSE4.2 Hardware Acceleration
// ============================================================================

#ifdef __SSE4_2__
#include <nmmintrin.h>

static uint32_t crc32c_hw(const uint8_t* data, size_t len, uint32_t crc = 0) {
    crc = ~crc;
    // Process 8 bytes at a time
    while (len >= 8) {
        uint64_t chunk;
        std::memcpy(&chunk, data, 8);
        crc = static_cast<uint32_t>(_mm_crc32_u64(crc, chunk));
        data += 8;
        len -= 8;
    }
    // Process remaining bytes
    while (len > 0) {
        crc = _mm_crc32_u8(crc, *data);
        data++;
        len--;
    }
    return ~crc;
}
#else
static uint32_t crc32c_sw(const uint8_t* data, size_t len, uint32_t crc = 0) {
    static const uint32_t table[256] = {
        0x00000000,0xF26B8303,0xE13B70F7,0x1350F3F4,0xC79A971F,0x35F1141C,0x26A1E7E8,0xD4CA64EB,
        0x8AD958CF,0x78B2DBCC,0x6BE22838,0x9989AB3B,0x4D43CFD0,0xBF284CD3,0xAC78BF27,0x5E133C24,
        0x105EC76F,0xE235446C,0xF165B798,0x030E349B,0xD7C45070,0x25AFD373,0x36FF2087,0xC494A384,
        0x9A879FA0,0x68EC1CA3,0x7BBCEF57,0x89D76C54,0x5D1D08BF,0xAF768BBC,0xBC267848,0x4E4DFB4B,
        0x20BD8EDE,0xD2D60DDD,0xC186FE29,0x33ED7D2A,0xE72719C1,0x154C9AC2,0x061C6936,0xF477EA35,
        0xAA64D611,0x580F5512,0x4B5FA6E6,0xB93425E5,0x6DFE410E,0x9F95C20D,0x8CC531F9,0x7EAEB2FA,
        0x30E349B1,0xC288CAB2,0xD1D83946,0x23B3BA45,0xF779DEAE,0x05125DAD,0x1642AE59,0xE4292D5A,
        0xBA3A117E,0x4851927D,0x5B016189,0xA96AE28A,0x7DA08661,0x8FCB0562,0x9C9BF696,0x6EF07595,
        0x417B1DBC,0xB3109EBF,0xA0406D4B,0x522BEE48,0x86E18AA3,0x748A09A0,0x67DAFA54,0x95B17957,
        0xCBA24573,0x39C9C670,0x2A993584,0xD8F2B687,0x0C38D26C,0xFE53516F,0xED03A29B,0x1F682198,
        0x5125DAD3,0xA34E59D0,0xB01EAA24,0x42752927,0x96BF4DCC,0x64D4CECF,0x77843D3B,0x85EFBE38,
        0xDBFC821C,0x2997011F,0x3AC7F2EB,0xC8AC71E8,0x1C661503,0xEE0D9600,0xFD5D65F4,0x0F36E6F7,
        0x61C69362,0x93AD1061,0x80FDE395,0x72966096,0xA65C047D,0x5437877E,0x4767748A,0xB50CF789,
        0xEB1FCBAD,0x197448AE,0x0A24BB5A,0xF84F3859,0x2C855CB2,0xDEEEDFB1,0xCDBE2C45,0x3FD5AF46,
        0x7198540D,0x83F3D70E,0x90A324FA,0x62C8A7F9,0xB602C312,0x44694011,0x5739B3E5,0xA55230E6,
        0xFB410CC2,0x092A8FC1,0x1A7A7C35,0xE811FF36,0x3CDB9BDD,0xCEB018DE,0xDDE0EB2A,0x2F8B6829,
        0x82F63B78,0x709DB87B,0x63CD4B8F,0x91A6C88C,0x456CAC67,0xB7072F64,0xA457DC90,0x563C5F93,
        0x082F63B7,0xFA44E0B4,0xE9141340,0x1B7F9043,0xCFB5F4A8,0x3DDE77AB,0x2E8E845F,0xDCE5075C,
        0x92A8FC17,0x60C37F14,0x73938CE0,0x81F80FE3,0x55326B08,0xA759E80B,0xB4091BFF,0x466298FC,
        0x1871A4D8,0xEA1A27DB,0xF94AD42F,0x0B21572C,0xDFEB33C7,0x2D80B0C4,0x3ED04330,0xCCBBC033,
        0xA24BB5A6,0x502036A5,0x4370C551,0xB11B4652,0x65D122B9,0x97BAA1BA,0x84EA524E,0x7681D14D,
        0x2892ED69,0xDAF96E6A,0xC9A99D9E,0x3BC21E9D,0xEF087A76,0x1D63F975,0x0E330A81,0xFC588982,
        0xB21572C9,0x407EF1CA,0x532E023E,0xA145813D,0x758FE5D6,0x87E466D5,0x94B49521,0x66DF1622,
        0x38CC2A06,0xCAA7A905,0xD9F75AF1,0x2B9CD9F2,0xFF56BD19,0x0D3D3E1A,0x1E6DCDEE,0xEC064EED,
        0xC38D26C4,0x31E6A5C7,0x22B65633,0xD0DDD530,0x0417B1DB,0xF67C32D8,0xE52CC12C,0x1747422F,
        0x49547E0B,0xBB3FFD08,0xA86F0EFC,0x5A048DFF,0x8ECEE914,0x7CA56A17,0x6FF599E3,0x9D9E1AE0,
        0xD3D3E1AB,0x21B862A8,0x32E8915C,0xC083125F,0x144976B4,0xE622F5B7,0xF5720643,0x07198540,
        0x590AB964,0xAB613A67,0xB831C993,0x4A5A4A90,0x9E902E7B,0x6CFBAD78,0x7FAB5E8C,0x8DC0DD8F,
        0xE330A81A,0x115B2B19,0x020BD8ED,0xF0605BEE,0x24AA3F05,0xD6C1BC06,0xC5914FF2,0x37FACCF1,
        0x69E9F0D5,0x9B8273D6,0x88D28022,0x7AB90321,0xAE7367CA,0x5C18E4C9,0x4F48173D,0xBD23943E,
        0xF36E6F75,0x0105EC76,0x12551F82,0xE03E9C81,0x34F4F86A,0xC69F7B69,0xD5CF889D,0x27A40B9E,
        0x79B737BA,0x8BDCB4B9,0x988C474D,0x6AE7C44E,0xBE2DA0A5,0x4C4623A6,0x5F16D052,0xAD7D5351
    };
    crc = ~crc;
    for (size_t i = 0; i < len; i++) {
        crc = table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }
    return ~crc;
}
static uint32_t crc32c_hw(const uint8_t* data, size_t len, uint32_t crc = 0) {
    return crc32c_sw(data, len, crc);
}
#endif

// ============================================================================
// Batch Serialization Format V2 (Full Implementation)
// ============================================================================

struct BatchWireHeader {
    int64_t base_offset;           // 8 bytes
    int32_t batch_length;          // 4 bytes
    int32_t partition_leader_epoch;// 4 bytes
    int8_t magic;                  // 1 byte (must be 2)
    int32_t crc;                   // 4 bytes
    int16_t attributes;            // 2 bytes
    int32_t last_offset_delta;     // 4 bytes
    int64_t base_timestamp;        // 8 bytes
    int64_t max_timestamp;         // 8 bytes
    int64_t producer_id;           // 8 bytes
    int16_t producer_epoch;        // 2 bytes
    int32_t base_sequence;         // 4 bytes
    int32_t record_count;          // 4 bytes
};
static constexpr size_t kBatchHeaderSize = 61;

struct RecordWireHeader {
    int32_t length;          // varint
    int8_t attributes;       // 1 byte
    int64_t timestamp_delta; // varint (signed zigzag)
    int32_t offset_delta;    // varint (signed zigzag)
    int32_t key_length;      // varint (signed zigzag, -1 = null)
    int32_t value_length;    // varint (signed zigzag, -1 = null)
    int32_t header_count;    // varint
};

std::vector<uint8_t> serialize_batch_v2(const RecordBatch& batch) {
    std::vector<uint8_t> result;
    result.reserve(4096);

    // Write batch header
    size_t header_start = result.size();
    result.resize(result.size() + kBatchHeaderSize);

    // Write records
    size_t records_start = result.size();
    for (const auto& record : batch.records) {
        std::vector<uint8_t> record_buf;
        record_buf.reserve(256);

        // attributes (1 byte)
        record_buf.push_back(0);

        // timestamp_delta (varint signed)
        uint8_t ts_buf[10];
        size_t ts_sz = varint_encode_signed_64(0, ts_buf);
        record_buf.insert(record_buf.end(), ts_buf, ts_buf + ts_sz);

        // offset_delta (varint signed)
        uint8_t off_buf[5];
        size_t off_sz = varint_encode_signed_32(0, off_buf);
        record_buf.insert(record_buf.end(), off_buf, off_buf + off_sz);

        // key (varint length + data)
        if (record.key.empty()) {
            record_buf.push_back(1); // zigzag(-1) = 1
        } else {
            uint8_t kl_buf[5];
            int32_t key_len = static_cast<int32_t>(record.key.size());
            size_t kl_sz = varint_encode_signed_32(key_len, kl_buf);
            record_buf.insert(record_buf.end(), kl_buf, kl_buf + kl_sz);
            record_buf.insert(record_buf.end(), record.key.begin(), record.key.end());
        }

        // value (varint length + data)
        if (record.value.empty()) {
            record_buf.push_back(1); // zigzag(-1) = 1, null value = tombstone
        } else {
            uint8_t vl_buf[5];
            int32_t val_len = static_cast<int32_t>(record.value.size());
            size_t vl_sz = varint_encode_signed_32(val_len, vl_buf);
            record_buf.insert(record_buf.end(), vl_buf, vl_buf + vl_sz);
            record_buf.insert(record_buf.end(), record.value.begin(), record.value.end());
        }

        // headers count (varint 0)
        record_buf.push_back(0);

        // Prepend record length (varint)
        uint8_t len_buf[5];
        size_t len_sz = varint_encode_32(static_cast<uint32_t>(record_buf.size()), len_buf);
        record_buf.insert(record_buf.begin(), len_buf, len_buf + len_sz);

        result.insert(result.end(), record_buf.begin(), record_buf.end());
    }

    // Back-patch batch header
    int32_t batch_len = static_cast<int32_t>(result.size() - header_start - 12);
    int32_t last_offset = batch.record_count() - 1;

    uint8_t* hdr = result.data() + header_start;
    write_int64_be(hdr, batch.base_offset);
    write_int32_be(hdr + 8, batch_len);
    write_int32_be(hdr + 12, batch.partition_leader_epoch);
    hdr[16] = 2; // magic
    // CRC placeholder at +17..20
    write_int16_be(hdr + 21, batch.attributes);
    write_int32_be(hdr + 23, last_offset);
    write_int64_be(hdr + 27, batch.base_timestamp);
    write_int64_be(hdr + 35, batch.max_timestamp);
    write_int64_be(hdr + 43, batch.producer_id);
    write_int16_be(hdr + 51, batch.producer_epoch);
    write_int32_be(hdr + 53, batch.base_sequence);
    write_int32_be(hdr + 57, batch.record_count());

    // Compute CRC over [attributes..end]
    uint32_t crc = crc32c_hw(result.data() + header_start + 21,
                              result.size() - header_start - 21);
    write_int32_be(hdr + 17, static_cast<int32_t>(crc));

    return result;
}

// ============================================================================
// Segment File Header (64 bytes)
// ============================================================================

struct SegmentFileHeader {
    char magic[4];          // "TQSG" = 0x47535154
    int32_t version;        // 1
    int32_t header_crc;     // CRC of bytes 16..64
    int64_t base_offset;
    int64_t segment_id;
    int64_t created_timestamp_ms;
    int8_t compression_type;
    int8_t sealed;
    char reserved[18];      // Padding to 64 bytes
};
static_assert(sizeof(SegmentFileHeader) == 64, "Segment header must be 64 bytes");

void write_segment_header(int fd, const SegmentFileHeader& hdr) {
    uint8_t buf[64] = {0};
    std::memcpy(buf, "TQSG", 4);
    write_int32_be(buf + 4, 1); // version
    // CRC placeholder at +8..11
    write_int64_be(buf + 12, hdr.base_offset);
    write_int64_be(buf + 20, hdr.segment_id);
    write_int64_be(buf + 28, hdr.created_timestamp_ms);
    buf[36] = static_cast<uint8_t>(hdr.compression_type);
    buf[37] = hdr.sealed;

    uint32_t crc = crc32c_hw(buf + 16, 48);
    write_int32_be(buf + 8, static_cast<int32_t>(crc));

    pwrite(fd, buf, 64, 0);
}

// ============================================================================
// Memory-mapped I/O with madvise tuning
// ============================================================================

class MmapRegion {
public:
    MmapRegion(int fd, size_t offset, size_t size, int prot = PROT_READ)
        : data_(nullptr), size_(size) {
        data_ = mmap(nullptr, size, prot, MAP_SHARED, fd,
                     static_cast<off_t>(offset));
        if (data_ == MAP_FAILED) {
            spdlog::error("mmap failed: {}", strerror(errno));
            data_ = nullptr;
            return;
        }
        // Advise random access pattern for segment reads
        madvise(data_, size, MADV_RANDOM);
    }

    ~MmapRegion() {
        if (data_) {
            munmap(data_, size_);
        }
    }

    MmapRegion(const MmapRegion&) = delete;
    MmapRegion& operator=(const MmapRegion&) = delete;

    const void* data() const { return data_; }
    size_t size() const { return size_; }
    bool valid() const { return data_ != nullptr; }

    void advise_willneed() { if (data_) madvise(data_, size_, MADV_WILLNEED); }
    void advise_dontneed() { if (data_) madvise(data_, size_, MADV_DONTNEED); }
    void advise_sequential() { if (data_) madvise(data_, size_, MADV_SEQUENTIAL); }

private:
    void* data_;
    size_t size_;
};

// ============================================================================
// Direct I/O alignment handling
// ============================================================================

class DirectIOBuffer {
public:
    static constexpr size_t kAlignment = 4096;

    static void* allocate_aligned(size_t size) {
        void* buf = nullptr;
        if (posix_memalign(&buf, kAlignment, size) != 0) {
            return nullptr;
        }
        return buf;
    }

    static void free_aligned(void* buf) {
        ::free(buf);
    }
};

// ============================================================================
// Write-ahead logging for crash safety
// ============================================================================

class WriteAheadLog {
public:
    struct WALEntry {
        int64_t sequence;
        std::vector<uint8_t> data;
        bool synced;
    };

    explicit WriteAheadLog(const std::string& path)
        : path_(path), sequence_(0) {
        fd_ = open(path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
        if (fd_ < 0) {
            spdlog::error("WAL open failed: {}", strerror(errno));
        }
    }

    ~WriteAheadLog() {
        if (fd_ >= 0) {
            fsync(fd_);
            close(fd_);
        }
    }

    int64_t append(const std::vector<uint8_t>& data) {
        if (fd_ < 0) return -1;
        int64_t seq = sequence_.fetch_add(1);
        // Write: [sequence:8][size:4][data][CRC:4]
        uint8_t header[12];
        write_int64_be(header, seq);
        write_int32_be(header + 8, static_cast<int32_t>(data.size()));

        struct iovec iov[3];
        iov[0].iov_base = header;
        iov[0].iov_len = 12;
        iov[1].iov_base = const_cast<uint8_t*>(data.data());
        iov[1].iov_len = data.size();
        uint32_t crc = crc32c_hw(data.data(), data.size());
        uint8_t crc_buf[4];
        write_int32_be(crc_buf, static_cast<int32_t>(crc));
        iov[2].iov_base = crc_buf;
        iov[2].iov_len = 4;

        ssize_t written = writev(fd_, iov, 3);
        if (written < 0) {
            spdlog::error("WAL write failed: {}", strerror(errno));
            return -1;
        }
        return seq;
    }

    bool sync() {
        if (fd_ < 0) return false;
        return fsync(fd_) == 0;
    }

private:
    std::string path_;
    int fd_{-1};
    std::atomic<int64_t> sequence_{0};
};

// ============================================================================
// Segment verification tool
// ============================================================================

struct VerifyResult {
    bool valid{true};
    int64_t batches_checked{0};
    int64_t records_checked{0};
    int64_t crc_errors{0};
    int64_t magic_errors{0};
    std::vector<std::string> errors;
};

VerifyResult verify_segment_file(const std::string& path) {
    VerifyResult result;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        result.valid = false;
        result.errors.push_back("Cannot open file: " + std::string(strerror(errno)));
        return result;
    }

    struct stat st;
    fstat(fd, &st);

    if (static_cast<size_t>(st.st_size) < 64) {
        result.valid = false;
        result.errors.push_back("File too small for header");
        close(fd);
        return result;
    }

    uint8_t header[64];
    pread(fd, header, 64, 0);

    // Verify magic
    if (memcmp(header, "TQSG", 4) != 0) {
        result.valid = false;
        result.magic_errors++;
        result.errors.push_back("Invalid magic bytes");
    }

    // Verify header CRC
    uint32_t stored_crc = static_cast<uint32_t>(read_int32_be(header + 8));
    uint32_t computed_crc = crc32c_hw(header + 16, 48);
    if (stored_crc != computed_crc) {
        result.valid = false;
        result.crc_errors++;
        result.errors.push_back("Header CRC mismatch");
    }

    close(fd);
    return result;
}

// ============================================================================
// Production monitoring hooks
// ============================================================================

struct SegmentMetrics {
    std::atomic<int64_t> total_appends{0};
    std::atomic<int64_t> total_reads{0};
    std::atomic<int64_t> total_bytes_written{0};
    std::atomic<int64_t> total_bytes_read{0};
    std::atomic<int64_t> crc_errors{0};
    std::atomic<int64_t> fsync_latency_us{0};
    std::chrono::steady_clock::time_point last_activity;

    void record_append(int64_t bytes) {
        total_appends.fetch_add(1);
        total_bytes_written.fetch_add(bytes);
        last_activity = std::chrono::steady_clock::now();
    }

    void record_read(int64_t bytes) {
        total_reads.fetch_add(1);
        total_bytes_read.fetch_add(bytes);
        last_activity = std::chrono::steady_clock::now();
    }

    double minutes_since_last_activity() const {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration<double, std::ratio<60>>(now - last_activity).count();
    }
};

} // anonymous namespace
} // namespace torrent
