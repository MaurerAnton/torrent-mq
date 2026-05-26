/**
 * torrent-mq — CRC32C Implementation with SSE4.2 Hardware Acceleration
 *
 * Provides fast CRC32C (Castagnoli 0x1EDC6F41 polynomial) computation used
 * for record-batch integrity verification in the Kafka wire protocol (v2
 * record batches), segment footer checksums, and network message framing.
 *
 * Architecture:
 *   - On x86-64 CPUs with SSE4.2: uses the crc32q/crc32b/crc32w/crc32l
 *     intrinsics for hardware-accelerated computation (~8-12 GB/s).
 *   - Fallback: slicing-by-8 software algorithm for portability.
 *   - crc32c_combine() merges two CRCs using GF(2) matrix multiplication,
 *     enabling O(1) incremental CRC updates without re-scanning data.
 *
 * Thread-safety: All functions are pure computations operating on local
 * state; fully reentrant and safe for concurrent use from any thread.
 *
 * Dependencies: <cpuid.h> (GCC/Clang) for runtime CPU feature detection.
 */

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <array>
#include <atomic>

// ---------------------------------------------------------------------------
// CPU feature detection
// ---------------------------------------------------------------------------
#if defined(__GNUC__) || defined(__clang__)
#include <cpuid.h>
#else
#warning "cpuid.h not available; SSE4.2 acceleration disabled on this compiler"
#endif

// ---------------------------------------------------------------------------
// SSE4.2 intrinsics (x86-64 only)
// ---------------------------------------------------------------------------
#if defined(__SSE4_2__) || defined(__x86_64__) || defined(_M_X64)
#include <nmmintrin.h>   // _mm_crc32_u8, _mm_crc32_u64
#endif

namespace torrent::common {

// ============================================================================
// CRC32C Parameters (Castagnoli polynomial)
// ============================================================================

/// CRC32C (iSCSI/Castagnoli) polynomial in reflected form: 0x1EDC6F41
/// The reflected bit-reversed representation is 0x82F63B78.
constexpr uint32_t kCrc32cPoly = 0x82F63B78u;

/// Initial CRC value (all-ones, per CRC-32 convention).
constexpr uint32_t kCrc32cInit = 0xFFFFFFFFu;

/// XOR-out value applied to final result.
constexpr uint32_t kCrc32cXorOut = 0xFFFFFFFFu;

// ============================================================================
// Global CPU feature flag
// ============================================================================

namespace {

/// Detected at first call via cpuid; cached thereafter.
std::atomic<int> s_sse42_available{-1};  // -1 = uninitialized, 0 = no, 1 = yes

/**
 * Probe the CPU for SSE4.2 support using the CPUID instruction.
 * ECX bit 20 of leaf 1 indicates SSE4.2 (CRC32 instruction).
 */
[[nodiscard]] bool detect_sse42() noexcept {
#if defined(__GNUC__) || defined(__clang__)
    unsigned int eax = 0, ebx = 0, ecx = 0, edx = 0;
    if (__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
        // SSE4.2 is indicated by bit 20 of ECX
        return (ecx & (1u << 20)) != 0;
    }
    return false;
#elif defined(_MSC_VER)
    int cpu_info[4];
    __cpuid(cpu_info, 1);
    return (cpu_info[2] & (1 << 20)) != 0;
#else
    return false;
#endif
}

/**
 * Lazily initialise and cache the SSE4.2 availability flag.
 */
[[nodiscard]] bool crc32c_sse42_available() noexcept {
    int cached = s_sse42_available.load(std::memory_order_acquire);
    if (cached >= 0) return cached != 0;

    bool available = detect_sse42();
    s_sse42_available.store(available ? 1 : 0, std::memory_order_release);
    return available;
}

} // anonymous namespace

// ============================================================================
// Public API: Initialization
// ============================================================================

void crc32c_init() noexcept {
    // Prime the detection cache at startup to avoid lazy-init races.
    crc32c_sse42_available();
}

// ============================================================================
// Software Fallback: Slicing-by-8 CRC32C
// ============================================================================

namespace {

/// Precomputed lookup table for byte-at-a-time CRC32C (fallback).
constexpr std::array<uint32_t, 256> build_crc32c_table() noexcept {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc & 1) ? (crc >> 1) ^ kCrc32cPoly : (crc >> 1);
        }
        table[i] = crc;
    }
    return table;
}

constexpr auto kCrc32cTable = build_crc32c_table();

/// 8 slicing tables for slicing-by-8 algorithm.
/// slicing_table[slice][byte] = CRC32C(byte << (8 * slice), 0)
constexpr std::array<std::array<uint32_t, 256>, 8> build_slicing_tables() noexcept {
    std::array<std::array<uint32_t, 256>, 8> tables{};

    // Table 0 is just the standard CRC table
    for (int i = 0; i < 256; ++i) {
        tables[0][i] = kCrc32cTable[i];
    }

    // Generate subsequent tables by applying CRC shift
    for (int slice = 1; slice < 8; ++slice) {
        for (int i = 0; i < 256; ++i) {
            uint32_t crc = tables[slice - 1][i];
            tables[slice][i] = (crc >> 8) ^ kCrc32cTable[crc & 0xFF];
        }
    }
    return tables;
}

constexpr auto kSlicingTables = build_slicing_tables();

/**
 * Software CRC32C using slicing-by-8 for good throughput on CPUs without
 * SSE4.2. Processes 8 bytes per iteration using 8 independent lookup tables.
 */
[[nodiscard]] uint32_t crc32c_sw(const uint8_t* data, size_t len,
                                  uint32_t crc) noexcept {
    crc ^= kCrc32cInit;

    // Process 8 bytes at a time (slicing-by-8)
    while (len >= 8) {
        // Unaligned access is fine on all modern architectures
        uint64_t word;
        std::memcpy(&word, data, sizeof(word));

        crc ^= static_cast<uint32_t>(word & 0xFF);
        crc = kSlicingTables[7][(word >> 56) & 0xFF] ^
              kSlicingTables[6][(word >> 48) & 0xFF] ^
              kSlicingTables[5][(word >> 40) & 0xFF] ^
              kSlicingTables[4][(word >> 32) & 0xFF] ^
              kSlicingTables[3][(word >> 24) & 0xFF] ^
              kSlicingTables[2][(word >> 16) & 0xFF] ^
              kSlicingTables[1][(word >> 8) & 0xFF] ^
              kSlicingTables[0][crc & 0xFF];

        data += 8;
        len -= 8;
    }

    // Process remaining bytes one at a time
    while (len > 0) {
        crc = (crc >> 8) ^ kCrc32cTable[(crc & 0xFF) ^ *data];
        ++data;
        --len;
    }

    return crc ^ kCrc32cXorOut;
}

} // anonymous namespace

// ============================================================================
// SSE4.2 Hardware-Accelerated Path
// ============================================================================

#if defined(__SSE4_2__) || defined(__x86_64__) || defined(_M_X64)

namespace {

/**
 * Compute CRC32C using SSE4.2 crc32 instructions.
 * Processes 8 bytes per iteration via crc32q (64-bit).
 */
[[nodiscard]] uint32_t crc32c_hw(const uint8_t* data, size_t len,
                                  uint32_t crc) noexcept {
    // Process 8 bytes at a time using 64-bit CRC instruction
    while (len >= 8) {
        uint64_t word;
        std::memcpy(&word, data, sizeof(word));
        crc = static_cast<uint32_t>(_mm_crc32_u64(crc, word));
        data += 8;
        len -= 8;
    }

    // Process remaining 4-byte chunks
    if (len >= 4) {
        uint32_t word;
        std::memcpy(&word, data, sizeof(word));
        crc = _mm_crc32_u32(crc, word);
        data += 4;
        len -= 4;
    }

    // Process remaining 2-byte chunks
    if (len >= 2) {
        uint16_t word;
        std::memcpy(&word, data, sizeof(word));
        crc = _mm_crc32_u16(crc, word);
        data += 2;
        len -= 2;
    }

    // Process final byte
    if (len >= 1) {
        crc = _mm_crc32_u8(crc, *data);
    }

    return crc;
}

} // anonymous namespace

#endif // SSE4.2

// ============================================================================
// Public API: Core Compute
// ============================================================================

uint32_t crc32c_compute(const void* data, size_t len) noexcept {
    return crc32c_compute(0, data, len);
}

uint32_t crc32c_compute(uint32_t initial, const void* data, size_t len) noexcept {
    if (data == nullptr || len == 0) {
        return initial;
    }

    const auto* bytes = static_cast<const uint8_t*>(data);

#if defined(__SSE4_2__) || defined(__x86_64__) || defined(_M_X64)
    if (crc32c_sse42_available()) {
        return crc32c_hw(bytes, len, initial);
    }
#endif

    return crc32c_sw(bytes, len, initial);
}

// ============================================================================
// CRC Combination (GF(2) Matrix Multiplication)
// ============================================================================

namespace {

/**
 * GF(2) matrix for CRC combination.
 *
 * Given CRC(A) and CRC(B), we want CRC(A || B) without recomputing from
 * scratch. The combination formula is:
 *   CRC(A || B) = CRC(A) ^ multiply(CRC(B), matrix_power(len(B) * 8))
 *
 * This is used for incremental CRC updates, e.g., when a segment is
 * extended and we want to compute the new footer checksum without
 * rescanning the entire file.
 */

/// Multiply two GF(2) polynomials of degree < 32, modulo CRC32C.
[[nodiscard]] constexpr uint32_t gf2_matrix_times(const uint32_t* mat,
                                                    uint32_t vec) noexcept {
    uint32_t sum = 0;
    while (vec) {
        if (vec & 1) {
            sum ^= *mat;
        }
        vec >>= 1;
        ++mat;
    }
    return sum;
}

/// Square a GF(2) matrix in-place: mat'[i] = mat[mat[i]].
constexpr void gf2_matrix_square(uint32_t* square, const uint32_t* mat) noexcept {
    for (int i = 0; i < 32; ++i) {
        square[i] = gf2_matrix_times(mat, mat[i]);
    }
}

} // anonymous namespace

uint32_t crc32c_combine(uint32_t crc1, uint32_t crc2, size_t len2) noexcept {
    // Degenerate cases
    if (len2 == 0) return crc1;

    // Build the GF(2) matrix representing the CRC of a zeroed block of
    // length len2. We construct this by repeated squaring based on the
    // binary representation of len2 * 8 (number of bits).
    //
    // even[i] = operator for 2^i zero bits
    // odd[i]  = combined operator for the accumulated odd bits

    uint32_t even[32];  // even[i] = power-of-two matrix
    uint32_t odd[32];   // odd[i] = combined matrix for bits from even

    // even[0] = matrix for a single zero bit shift
    // Row i: if the incoming bit-0 is set, which output bits are affected
    // after CRC of one zero bit? This is just the generator polynomial row.
    uint32_t row = 1;
    even[0] = (kCrc32cPoly << 1) | 1;  // simplified initialization

    // Build even[0] properly: even[0][i] = CRC of (1 << i) bit shifted by one zero bit
    // Simpler approach: build from the poly
    {
        uint32_t mat[32] = {};
        for (int i = 0; i < 32; ++i) {
            mat[i] = static_cast<uint32_t>(1) << i;
        }
        // Apply one zero bit: crc = (old_crc >> 1) ^ (old_crc & 1 ? poly : 0)
        // For each input bit i, the output is:
        uint32_t result[32] = {};
        for (int i = 0; i < 32; ++i) {
            uint32_t bit = static_cast<uint32_t>(1) << i;
            result[i] = (bit >> 1) ^ ((bit & 1) ? kCrc32cPoly : 0);
        }
        for (int i = 0; i < 32; ++i) {
            even[0] = 0;
            // Reconstruct even[0] such that gf2_matrix_times(even, 1<<i) == result[i]
            // even[0][j] = sum over i of (result[i]'s bit j in column of even[0])
        }
    }

    // Simpler: use a precomputed approach for the common case.
    // For the combine operation, we need to advance CRC2 by len2 zero bytes.
    // This is equivalent to: CRC(0^len2) ^ multiply(CRC2, M^len2) where
    // M is the CRC shift matrix for one zero byte.

    // Build M (matrix for one zero byte = 8 zero bits)
    uint32_t mat_one_byte[32] = {};
    for (int i = 0; i < 32; ++i) {
        uint32_t v = static_cast<uint32_t>(1) << i;
        for (int b = 0; b < 8; ++b) {
            v = (v >> 1) ^ ((v & 1) ? kCrc32cPoly : 0);
        }
        mat_one_byte[i] = v;
    }

    // Now compute M^len2 using exponentiation by squaring on the bit
    // representation of len2.
    uint32_t power[32] = {};
    for (int i = 0; i < 32; ++i) {
        power[i] = mat_one_byte[i];
    }

    // Identity matrix result
    uint32_t result_mat[32] = {};
    for (int i = 0; i < 32; ++i) {
        result_mat[i] = static_cast<uint32_t>(1) << i;
    }

    size_t n = len2;
    while (n > 0) {
        if (n & 1) {
            // result = result * power (in GF(2) matrix sense)
            uint32_t tmp[32] = {};
            for (int i = 0; i < 32; ++i) {
                tmp[i] = gf2_matrix_times(result_mat, power[i]);
            }
            for (int i = 0; i < 32; ++i) {
                result_mat[i] = tmp[i];
            }
        }
        // power = power^2
        {
            uint32_t tmp[32] = {};
            for (int i = 0; i < 32; ++i) {
                tmp[i] = gf2_matrix_times(power, power[i]);
            }
            for (int i = 0; i < 32; ++i) {
                power[i] = tmp[i];
            }
        }
        n >>= 1;
    }

    // CRC(A || B) = CRC(A) ^ gf2_matrix_times(result_mat, CRC(B))
    uint32_t combined = crc1 ^ gf2_matrix_times(result_mat, crc2);
    return combined;
}

} // namespace torrent::common
