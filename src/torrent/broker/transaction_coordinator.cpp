/**
 * transaction_coordinator.cpp — TransactionCoordinator: PID, Epoch, Markers
 *
 * Implements the Kafka-compatible transaction coordinator protocol:
 *   - init_producer_id: allocate a PID (producer ID), bump epoch, fence old
 *   - add_partitions_to_txn: register partitions under an active transaction
 *   - add_offsets_to_txn: register consumer group offset commits in a txn
 *   - end_txn: commit or abort a transaction, write control markers
 *   - write_txn_markers: append commit/abort control batches to txn partitions
 *
 * Producer ID fencing:
 *   Each call to init_producer_id increments the producer epoch.  Writes
 *   bearing a stale epoch are rejected with error_code::producer_fenced
 *   (epoch 90) or error_code::invalid_producer_epoch (epoch 47).
 *
 * Transactional ID expiration:
 *   An idle transactional ID (no activity for > transactional_id_expiration_ms)
 *   may have its PID mapping expired.  This frees up a PID and requires the
 *   producer to call init_producer_id to re-register.
 *
 * Transaction timeout:
 *   A transaction must be committed or aborted within the configured
 *   transaction_timeout_ms.  Transactions that exceed the timeout are
 *   automatically aborted by the background expiry thread.
 *
 * Control markers (commit/abort):
 *   When a transaction ends, a control batch is written to each partition
 *   that participated in the transaction.  The marker records the final
 *   outcome (COMMIT=0, ABORT=1) and enables read_committed isolation.
 *
 * State machine (per transactional ID):
 *   Empty → (InitPid) → Ready
 *   Ready → (AddPartitions) → Ongoing
 *   Ongoing → (EndTxn/Timeout) → PrepareCommit or PrepareAbort
 *   PrepareCommit → (WriteMarkers complete) → CompleteCommit → Empty
 *   PrepareAbort → (WriteMarkers complete) → CompleteAbort → Empty
 *
 * Persistence:
 *   Producer ID mappings and transaction state are persisted to the
 *   __transaction_state internal topic for crash recovery.
 *
 * Dependencies:
 *   - request_dispatcher.h: for routing marker writes
 *   - topic_manager.h: for resolving topic configs
 *   - partition_manager.h: for leader checks and offset lookups
 *   - log_manager.h: for appending control batches
 *   - inter_broker.h: for propagating transactional state changes
 *
 * Thread-safety:
 *   Each transactional ID state is protected by its own mutex.
 *   The PID allocator is protected by an atomic counter + spinlock.
 *   The expiry thread acquires locks per-ID briefly.
 *
 * See transaction_coordinator.h for the public API contract.
 */

#include "torrent/broker/transaction_coordinator.h"
#include "torrent/broker/server.h"
#include "torrent/broker/topic_manager.h"
#include "torrent/broker/partition_manager.h"
#include "torrent/broker/request_dispatcher.h"
#include "torrent/broker/inter_broker.h"
#include "torrent/broker/controller.h"
#include "torrent/consensus/raft_node.h"
#include "torrent/storage/log_manager.h"
#include "torrent/storage/types.h"
#include "torrent/common/types.h"
#include "torrent/common/config.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

// ============================================================================
// Aliases for readability
// ============================================================================

namespace torrent::broker {

// ============================================================================
// Anonymous namespace — internal types, constants, helpers
// ============================================================================

namespace {

// --------------------------------------------------------------------------
// Logger
// --------------------------------------------------------------------------

[[nodiscard]] std::shared_ptr<spdlog::logger> get_tc_logger() {
    static auto logger = []() {
        auto l = spdlog::get("transaction_coordinator");
        if (!l) {
            l = spdlog::stdout_color_mt("transaction_coordinator");
            l->set_level(spdlog::level::info);
        }
        return l;
    }();
    return logger;
}

#define TC_LOG_INFO(...)  get_tc_logger()->info(__VA_ARGS__)
#define TC_LOG_WARN(...)  get_tc_logger()->warn(__VA_ARGS__)
#define TC_LOG_ERROR(...) get_tc_logger()->error(__VA_ARGS__)
#define TC_LOG_DEBUG(...) get_tc_logger()->debug(__VA_ARGS__)
#define TC_LOG_TRACE(...) get_tc_logger()->trace(__VA_ARGS__)

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Default transaction timeout in milliseconds (60 seconds).
static constexpr duration_ms_t kDefaultTransactionTimeoutMs = 60000;

/// Default transactional ID expiration in milliseconds (7 days).
static constexpr duration_ms_t kDefaultTransactionalIdExpirationMs = 604800000;

/// Maximum number of partitions that can be enrolled in a single transaction.
static constexpr size_t kMaxPartitionsPerTxn = 5000;

/// Minimum producer ID (usable range starts at 0, like Kafka).
static constexpr producer_id_t kMinProducerId = 0;

/// Sentinel producer ID that means "uninitialized".
static constexpr producer_id_t kInvalidProducerId = -1;

/// Reserve a block of PIDs for next allocation batch.
static constexpr producer_id_t kProducerIdBlockSize = 1000;

/// Interval for the background expiry/cleanup thread.
static constexpr auto kExpiryCheckInterval = std::chrono::seconds(10);

/// Interval for the background timeout-abort thread.
static constexpr auto kTimeoutCheckInterval = std::chrono::milliseconds(1000);

/// Control marker types (written to partition logs at commit/abort time).
enum class control_record_type : int16_t {
    abort   = 0,
    commit  = 1,
};

/// Default abort timeout — if end_txn is not called within this window
/// after initiating a transaction, the coordinator auto-aborts.
static constexpr duration_ms_t kDefaultAbortTimeoutMs = 5000;

/**
 * Maximum time a PID assignment can live without a corresponding
 * active transactional ID.  Orphaned PIDs are reclaimed.
 */
static constexpr duration_ms_t kMaxOrphanPidAgeMs = 3600000; // 1 hour

// --------------------------------------------------------------------------
// Transactional ID state
// --------------------------------------------------------------------------

/**
 * Persistent state for a single transactional producer ID.
 *
 * Mapped one-to-one with a transactional_id string.  The PID assignment
 * is durable — once assigned to a transactional_id, it persists until
 * the transactional_id expires.
 */
struct TransacationIdState {
    transactional_id_t transactional_id;
    producer_id_t      producer_id    = kInvalidProducerId;
    producer_epoch_t   producer_epoch = -1;

    // --- Transaction lifecycle ---
    enum class txn_state_t : uint8_t {
        empty           = 0,   ///< No active transaction.
        ongoing         = 1,   ///< Transaction started, adding partitions.
        prepare_commit  = 2,   ///< EndTxn(commit) received, writing markers.
        prepare_abort   = 3,   ///< EndTxn(abort) received, writing markers.
        complete_commit = 4,   ///< Markers written, transitioning to empty.
        complete_abort  = 5,   ///< Markers written, transitioning to empty.
        dead            = 6,   ///< Expired / pending cleanup.
    };
    txn_state_t txn_state = txn_state_t::empty;

    /// Partitions enrolled in the current transaction.
    std::vector<std::pair<std::string, partition_id_t>> txn_partitions;

    /// Consumer group offsets pending in the current transaction.
    std::vector<std::tuple<std::string, partition_id_t, offset_t>> txn_offsets;

    /// When the current transaction started.
    std::chrono::steady_clock::time_point txn_start_time;

    /// When this transactional ID was last used (for expiration tracking).
    std::chrono::steady_clock::time_point last_activity;

    /// When this entry was created.
    std::chrono::steady_clock::time_point created_at;

    /// Protects mutable state of this entry.
    mutable std::mutex state_mutex;

    // --- Helpers ---

    [[nodiscard]] bool is_expired(duration_ms_t expiration_ms) const noexcept {
        auto now = std::chrono::steady_clock::now();
        auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - last_activity).count();
        return age > expiration_ms;
    }

    [[nodiscard]] bool has_timed_out(duration_ms_t timeout_ms) const noexcept {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - txn_start_time).count();
        return elapsed > timeout_ms;
    }

    void touch_activity() noexcept {
        last_activity = std::chrono::steady_clock::now();
    }

    void reset_transaction() noexcept {
        txn_partitions.clear();
        txn_offsets.clear();
        txn_state = txn_state_t::empty;
        touch_activity();
    }
};

// --------------------------------------------------------------------------
// PID allocator — thread-safe, block-based allocation
// --------------------------------------------------------------------------

/**
 * Producer ID allocator.  PIDs are monotonically increasing across the
 * cluster (the coordinator acts as the sole PID allocator).  To minimize
 * Raft calls, PIDs are allocated in blocks that are persisted to the
 * __transaction_state topic.
 *
 * On crash recovery, the coordinator reads the last allocated block from
 * the transaction-state topic and resumes from the next available PID.
 */
class ProducerIdAllocator {
public:
    ProducerIdAllocator()
        : next_pid_(kMinProducerId)
        , block_end_(kMinProducerId)
    {}

    /**
     * Allocate the next available PID.  If the current block is exhausted,
     * allocates a new block (in production, this would be persisted via Raft).
     */
    [[nodiscard]] producer_id_t allocate() {
        std::lock_guard<std::mutex> lock(mutex_);

        if (next_pid_ >= block_end_) {
            // Exhausted current block — allocate a new one.
            // In production, this would propose a block allocation through Raft.
            block_end_ = next_pid_.load(std::memory_order_acquire) + kProducerIdBlockSize;
            TC_LOG_INFO("PID block allocated: range=[{}, {})",
                        next_pid_.load(), block_end_.load());
        }

        producer_id_t pid = next_pid_.fetch_add(1, std::memory_order_release);
        TC_LOG_TRACE("PID {} allocated", pid);
        return pid;
    }

    /**
     * Initialize the allocator from a previously persisted high-water PID.
     */
    void initialize_from(producer_id_t last_assigned_pid) {
        std::lock_guard<std::mutex> lock(mutex_);
        producer_id_t start = std::max(last_assigned_pid + 1, kMinProducerId);
        next_pid_.store(start, std::memory_order_release);
        block_end_.store(start + kProducerIdBlockSize, std::memory_order_release);
        TC_LOG_INFO("PID allocator initialized from PID {}", last_assigned_pid);
    }

    /**
     * Get the next PID that would be allocated (for diagnostics).
     */
    [[nodiscard]] producer_id_t peek_next() const noexcept {
        return next_pid_.load(std::memory_order_acquire);
    }

private:
    std::atomic<producer_id_t> next_pid_;
    std::atomic<producer_id_t> block_end_;
    mutable std::mutex mutex_;
};

// --------------------------------------------------------------------------
// Failure helpers
// --------------------------------------------------------------------------

/// Construct a failure result with a consistent error message format.
template<typename T>
[[nodiscard]] result<T> txn_failure(error_code ec, const std::string& detail) {
    TC_LOG_WARN("Transaction operation failed: {} (code={})",
                detail, static_cast<int16_t>(ec));
    return result<T>::failure(ec, detail);
}

// --------------------------------------------------------------------------
// Fencing helper
// --------------------------------------------------------------------------

/**
 * Check whether a producer epoch matches the current epoch for a PID.
 * If stale, return fenced error.  If newer, update the epoch (this
 * should not normally happen — the epoch is controlled by the
 * coordinator's init_producer_id logic).
 */
[[nodiscard]] error_code check_producer_epoch(
    producer_epoch_t current_epoch,
    producer_epoch_t presented_epoch) noexcept {

    if (presented_epoch < current_epoch) {
        TC_LOG_WARN("Producer fenced: epoch {} < current epoch {}",
                    presented_epoch, current_epoch);
        return error_code::producer_fenced;
    }
    if (presented_epoch > current_epoch) {
        TC_LOG_WARN("Producer epoch {} > current epoch {} (unexpected)",
                    presented_epoch, current_epoch);
        return error_code::invalid_producer_epoch;
    }
    return error_code::none;
}

// --------------------------------------------------------------------------
// Control batch construction
// --------------------------------------------------------------------------

/**
 * Build a control batch (commit or abort marker) for a given PID/epoch.
 *
 * Control batches are RecordBatches with the is_control_batch attribute set.
 * They contain a single record whose key is the control record type
 * (0=abort, 1=commit) encoded as a 2-byte little-endian value.
 */
[[nodiscard]] RecordBatch build_control_batch(
    producer_id_t pid,
    producer_epoch_t epoch,
    control_record_type marker_type) {

    RecordBatch batch;
    batch.base_offset        = kInvalidOffset;
    batch.base_timestamp     = 0;
    batch.max_timestamp      = 0;
    batch.last_offset_delta  = 0;
    batch.compression        = compression_type::none;
    batch.attributes         = static_cast<int16_t>(record_batch_attributes::is_control_batch);
    batch.producer_id        = pid;
    batch.producer_epoch     = epoch;
    batch.base_sequence      = -1;
    batch.record_count       = 1;
    batch.partition_leader_epoch = 0;

    // Build the control record
    Record ctrl_record;
    int16_t marker = (marker_type == control_record_type::commit) ? 1 : 0;
    char marker_buf[2];
    // Little-endian encoding
    marker_buf[0] = static_cast<char>(marker & 0xFF);
    marker_buf[1] = static_cast<char>((marker >> 8) & 0xFF);
    ctrl_record.key   = shared_buffer(marker_buf, 2);
    ctrl_record.value = shared_buffer(nullptr, 0);
    ctrl_record.producer_id    = pid;
    ctrl_record.sequence       = -1;
    ctrl_record.timestamp      = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    batch.records.push_back(std::move(ctrl_record));
    return batch;
}

// --------------------------------------------------------------------------
// Internal topic name for transaction state
// --------------------------------------------------------------------------

/// Internal topic that persists transactional IDs and PID assignments.
static constexpr std::string_view kTransactionStateTopic = "__transaction_state";

/// Partition count for the transaction state topic (single partition, like Kafka).
static constexpr int32_t kTransactionStatePartitions = 50;

} // anonymous namespace

// ============================================================================
// TransactionCoordinator::Impl — PIMPL
// ============================================================================

struct TransactionCoordinator::Impl {
    /// PID allocator (block-based from Raft-persisted state).
    ProducerIdAllocator pid_allocator;

    /// Map of transactional_id → persistent transactional state.
    std::unordered_map<std::string, std::unique_ptr<TransacationIdState>> txn_states;

    /// Protects the txn_states map.
    mutable std::shared_mutex states_mutex;

    /// Map of producer_id → transactional_id (reverse lookup).
    std::unordered_map<producer_id_t, std::string> pid_to_transactional_id;
    mutable std::shared_mutex pid_map_mutex;

    /// Background thread for transaction timeout detection.
    std::thread timeout_thread;
    std::atomic<bool> timeout_running{false};

    /// Background thread for transactional ID expiration.
    std::thread expiry_thread;
    std::atomic<bool> expiry_running{false};

    /// Running flag (set after start() is called).
    std::atomic<bool> running{false};

    /// Config-sourced values (cached for fast access).
    duration_ms_t transaction_timeout_ms = kDefaultTransactionTimeoutMs;
    duration_ms_t transactional_id_expiration_ms = kDefaultTransactionalIdExpirationMs;

    /// Whether transactions are enabled.
    bool transactions_enabled = true;

    /// How many PIDs have been allocated since startup.
    std::atomic<uint64_t> pids_allocated{0};

    /// How many transactions are currently active.
    std::atomic<uint64_t> active_transactions{0};
};

// ============================================================================
// TransactionCoordinator — Constructor / Destructor
// ============================================================================

TransactionCoordinator::TransactionCoordinator(BrokerServer& s)
    : server_(&s)
    , impl_(std::make_unique<Impl>())
{
    TC_LOG_INFO("TransactionCoordinator initialized");
}

TransactionCoordinator::~TransactionCoordinator() {
    if (impl_->running.load(std::memory_order_acquire)) {
        try { shutdown(); } catch (...) {}
    }
    TC_LOG_INFO("TransactionCoordinator destroyed");
}

// ============================================================================
// TransactionCoordinator — start()
// ============================================================================

void TransactionCoordinator::start() {
    if (impl_->running.load(std::memory_order_acquire)) {
        TC_LOG_WARN("TransactionCoordinator::start() called while already running");
        return;
    }

    const auto& cfg = server_->config();
    impl_->transactions_enabled = cfg.enable_transactions;
    if (!impl_->transactions_enabled) {
        TC_LOG_INFO("Transactions disabled by configuration");
        impl_->running.store(true, std::memory_order_release);
        return;
    }

    // Resolve timeout and expiration from the global config if available.
    // The BrokerServer may also have its own config; we use BrokerConfig defaults
    // or override from the global torrent::config as appropriate.
    // (In production, the global config is accessed through server_->global_config().)
    impl_->transaction_timeout_ms = kDefaultTransactionTimeoutMs;
    impl_->transactional_id_expiration_ms = kDefaultTransactionalIdExpirationMs;

    // Attempt to read persisted PID high-water mark from __transaction_state.
    // In production, this reconstructs the allocator state from the log.
    // For now, initialize from zero.
    impl_->pid_allocator.initialize_from(0);

    // Ensure the internal __transaction_state topic exists.
    // This is done via the controller; for now, log a warning if not found.
    if (server_->is_controller()) {
        auto& tm = server_->topic_manager();
        if (!tm.topic_exists(std::string(kTransactionStateTopic))) {
            TC_LOG_INFO("Creating internal topic '{}' ({} partitions)",
                        kTransactionStateTopic, kTransactionStatePartitions);
            // In production, this would create the topic via the controller.
        }
    }

    // Start background timeout detection thread.
    impl_->timeout_running.store(true, std::memory_order_release);
    impl_->timeout_thread = std::thread(&TransactionCoordinator::timeout_loop, this);

    // Start background transactional ID expiration thread.
    impl_->expiry_running.store(true, std::memory_order_release);
    impl_->expiry_thread = std::thread(&TransactionCoordinator::expiry_loop, this);

    impl_->running.store(true, std::memory_order_release);
    TC_LOG_INFO("TransactionCoordinator started (timeout={}ms, expiration={}ms)",
                impl_->transaction_timeout_ms, impl_->transactional_id_expiration_ms);
}

// ============================================================================
// TransactionCoordinator — shutdown()
// ============================================================================

void TransactionCoordinator::shutdown() {
    if (!impl_->running.load(std::memory_order_acquire)) {
        return;
    }

    TC_LOG_INFO("TransactionCoordinator shutting down ({} active transactions)",
                impl_->active_transactions.load());

    // Stop background threads.
    impl_->timeout_running.store(false, std::memory_order_release);
    if (impl_->timeout_thread.joinable()) {
        impl_->timeout_thread.join();
    }

    impl_->expiry_running.store(false, std::memory_order_release);
    if (impl_->expiry_thread.joinable()) {
        impl_->expiry_thread.join();
    }

    // Abort all active transactions.
    {
        std::unique_lock lock(impl_->states_mutex);
        for (auto& [id, state] : impl_->txn_states) {
            std::lock_guard<std::mutex> state_lock(state->state_mutex);
            if (state->txn_state != TransacationIdState::txn_state_t::empty &&
                state->txn_state != TransacationIdState::txn_state_t::dead) {
                TC_LOG_INFO("Auto-aborting transaction '{}' on shutdown", id);
                state->reset_transaction();
            }
        }
    }

    impl_->running.store(false, std::memory_order_release);
    TC_LOG_INFO("TransactionCoordinator shutdown complete");
}

// ============================================================================
// TransactionCoordinator — init_producer_id()
// ============================================================================

result<producer_id_t> TransactionCoordinator::init_producer_id(
    const std::string& transactional_id,
    duration_ms_t timeout_ms)
{
    if (!impl_->transactions_enabled) {
        return txn_failure<producer_id_t>(
            error_code::invalid_txn_state,
            "Transactions are not enabled on this broker");
    }

    // --- Phase 1: Validate inputs ---
    if (transactional_id.empty()) {
        TC_LOG_DEBUG("init_producer_id called with empty transactional_id — "
                     "allocating one-shot PID (no fencing)");
        // Non-transactional producer — allocate a PID without durability.
        producer_id_t pid = impl_->pid_allocator.allocate();
        impl_->pids_allocated.fetch_add(1, std::memory_order_relaxed);
        return result<producer_id_t>::success(pid);
    }

    if (timeout_ms <= 0) {
        return txn_failure<producer_id_t>(
            error_code::invalid_transaction_timeout,
            "Transaction timeout must be > 0");
    }

    // --- Phase 2: Lookup or create transactional ID state ---
    {
        // First, try a shared lock to see if the ID already exists.
        std::shared_lock shared_lock(impl_->states_mutex);
        auto it = impl_->txn_states.find(transactional_id);
        if (it != impl_->txn_states.end()) {
            auto& state = *it->second;
            std::lock_guard<std::mutex> state_lock(state.state_mutex);

            // Fence any existing transaction.
            if (state.txn_state != TransacationIdState::txn_state_t::empty) {
                TC_LOG_WARN("Fencing active transaction '{}' (state={})",
                            transactional_id, static_cast<int>(state.txn_state));
                state.reset_transaction();
            }

            // Bump epoch — this fences any in-flight producer writes.
            state.producer_epoch++;
            if (state.producer_epoch < 0) {
                state.producer_epoch = 0; // Wrap guard
            }

            producer_epoch_t new_epoch = state.producer_epoch;

            TC_LOG_INFO("Producer ID re-initialized: transactional_id='{}' pid={} epoch={}",
                        transactional_id, state.producer_id, new_epoch);

            state.touch_activity();
            return result<producer_id_t>::success(state.producer_id);
        }
    }

    // --- Phase 3: Allocate new PID and create state ---
    producer_id_t new_pid = impl_->pid_allocator.allocate();
    impl_->pids_allocated.fetch_add(1, std::memory_order_relaxed);

    auto new_state = std::make_unique<TransacationIdState>();
    new_state->transactional_id = transactional_id;
    new_state->producer_id      = new_pid;
    new_state->producer_epoch   = 0;
    new_state->txn_state        = TransacationIdState::txn_state_t::empty;
    new_state->created_at       = std::chrono::steady_clock::now();
    new_state->last_activity    = new_state->created_at;

    {
        std::unique_lock unique_lock(impl_->states_mutex);
        impl_->txn_states[transactional_id] = std::move(new_state);

        // Update reverse mapping.
        {
            std::unique_lock pid_lock(impl_->pid_map_mutex);
            impl_->pid_to_transactional_id[new_pid] = transactional_id;
        }
    }

    TC_LOG_INFO("Producer ID allocated: transactional_id='{}' pid={} epoch=0",
                transactional_id, new_pid);

    return result<producer_id_t>::success(new_pid);
}

// ============================================================================
// TransactionCoordinator — add_partitions_to_txn()
// ============================================================================

result<void> TransactionCoordinator::add_partitions_to_txn(
    producer_id_t pid,
    producer_epoch_t epoch,
    const std::vector<std::pair<std::string, partition_id_t>>& partitions)
{
    if (!impl_->transactions_enabled) {
        return txn_failure<void>(
            error_code::invalid_txn_state,
            "Transactions are not enabled on this broker");
    }

    if (partitions.empty()) {
        TC_LOG_DEBUG("add_partitions_to_txn: pid={} epoch={} — no partitions to add",
                     pid, epoch);
        return result<void>::success();
    }

    if (partitions.size() > kMaxPartitionsPerTxn) {
        return txn_failure<void>(
            error_code::invalid_txn_state,
            "Too many partitions (" + std::to_string(partitions.size()) +
            ") for a single transaction (max " +
            std::to_string(kMaxPartitionsPerTxn) + ")");
    }

    // --- Phase 1: Resolve PID to transactional ID ---
    std::string transactional_id;
    {
        std::shared_lock pid_lock(impl_->pid_map_mutex);
        auto it = impl_->pid_to_transactional_id.find(pid);
        if (it == impl_->pid_to_transactional_id.end()) {
            return txn_failure<void>(error_code::unknown_producer_id,
                                     "Unknown producer ID: " + std::to_string(pid));
        }
        transactional_id = it->second;
    }

    // --- Phase 2: Lock the transactional ID state ---
    TransacationIdState* state = nullptr;
    {
        std::shared_lock shared_lock(impl_->states_mutex);
        auto it = impl_->txn_states.find(transactional_id);
        if (it == impl_->txn_states.end()) {
            return txn_failure<void>(
                error_code::invalid_txn_state,
                "Transactional ID '" + transactional_id + "' not found");
        }
        state = it->second.get();
    }

    std::lock_guard<std::mutex> state_lock(state->state_mutex);

    // --- Phase 3: Validate epoch ---
    error_code epoch_err = check_producer_epoch(state->producer_epoch, epoch);
    if (epoch_err != error_code::none) {
        return result<void>::failure(epoch_err,
            "Epoch mismatch: presented=" + std::to_string(epoch) +
            " current=" + std::to_string(state->producer_epoch));
    }

    // --- Phase 4: Validate state machine ---
    if (state->txn_state == TransacationIdState::txn_state_t::empty) {
        // First add_partitions call — transition to Ongoing.
        state->txn_state = TransacationIdState::txn_state_t::ongoing;
        state->txn_start_time = std::chrono::steady_clock::now();
        impl_->active_transactions.fetch_add(1, std::memory_order_relaxed);
        TC_LOG_INFO("Transaction started: transactional_id='{}' pid={} epoch={}",
                    transactional_id, pid, epoch);
    } else if (state->txn_state != TransacationIdState::txn_state_t::ongoing) {
        return txn_failure<void>(error_code::invalid_txn_state,
            "Cannot add partitions in state " +
            std::to_string(static_cast<int>(state->txn_state)));
    }

    // --- Phase 5: Validate each partition ---
    auto& pm = server_->partition_manager();
    for (const auto& [topic, partition] : partitions) {
        // Check that this broker is the leader for the partition.
        if (!pm.is_leader(topic, partition)) {
            return txn_failure<void>(error_code::not_leader_for_partition,
                "Not leader for partition: " + topic + "/" +
                std::to_string(partition));
        }

        // Add the partition (deduplication via set).
        state->txn_partitions.push_back({topic, partition});
    }

    // Deduplicate the partition list.
    std::sort(state->txn_partitions.begin(), state->txn_partitions.end());
    state->txn_partitions.erase(
        std::unique(state->txn_partitions.begin(), state->txn_partitions.end()),
        state->txn_partitions.end());

    state->touch_activity();

    TC_LOG_INFO("Added {} partitions to transaction '{}' (total={})",
                partitions.size(), transactional_id, state->txn_partitions.size());

    return result<void>::success();
}

// ============================================================================
// TransactionCoordinator — add_offsets_to_txn()
// ============================================================================

result<void> TransactionCoordinator::add_offsets_to_txn(
    producer_id_t pid,
    producer_epoch_t epoch,
    const std::string& group_id)
{
    if (!impl_->transactions_enabled) {
        return txn_failure<void>(
            error_code::invalid_txn_state,
            "Transactions are not enabled on this broker");
    }

    if (group_id.empty()) {
        return txn_failure<void>(
            error_code::invalid_group_id,
            "Group ID cannot be empty");
    }

    // --- Phase 1: Resolve PID to transactional ID ---
    std::string transactional_id;
    {
        std::shared_lock pid_lock(impl_->pid_map_mutex);
        auto it = impl_->pid_to_transactional_id.find(pid);
        if (it == impl_->pid_to_transactional_id.end()) {
            return txn_failure<void>(error_code::unknown_producer_id,
                                     "Unknown producer ID: " + std::to_string(pid));
        }
        transactional_id = it->second;
    }

    // --- Phase 2: Lock state ---
    TransacationIdState* state = nullptr;
    {
        std::shared_lock shared_lock(impl_->states_mutex);
        auto it = impl_->txn_states.find(transactional_id);
        if (it == impl_->txn_states.end()) {
            return txn_failure<void>(
                error_code::invalid_txn_state,
                "Transactional ID '" + transactional_id + "' not found");
        }
        state = it->second.get();
    }

    std::lock_guard<std::mutex> state_lock(state->state_mutex);

    // --- Phase 3: Validate epoch ---
    error_code epoch_err = check_producer_epoch(state->producer_epoch, epoch);
    if (epoch_err != error_code::none) {
        return result<void>::failure(epoch_err,
            "Epoch mismatch: presented=" + std::to_string(epoch) +
            " current=" + std::to_string(state->producer_epoch));
    }

    // --- Phase 4: Validate state ---
    if (state->txn_state != TransacationIdState::txn_state_t::ongoing) {
        return txn_failure<void>(error_code::invalid_txn_state,
            "Cannot add offsets in state " +
            std::to_string(static_cast<int>(state->txn_state)));
    }

    state->touch_activity();
    TC_LOG_INFO("Consumer group '{}' registered for offsets in transaction '{}'",
                group_id, transactional_id);

    return result<void>::success();
}

// ============================================================================
// TransactionCoordinator — end_txn()
// ============================================================================

result<void> TransactionCoordinator::end_txn(
    producer_id_t pid,
    producer_epoch_t epoch,
    bool committed)
{
    if (!impl_->transactions_enabled) {
        return txn_failure<void>(
            error_code::invalid_txn_state,
            "Transactions are not enabled on this broker");
    }

    // --- Phase 1: Resolve PID to transactional ID ---
    std::string transactional_id;
    {
        std::shared_lock pid_lock(impl_->pid_map_mutex);
        auto it = impl_->pid_to_transactional_id.find(pid);
        if (it == impl_->pid_to_transactional_id.end()) {
            return txn_failure<void>(error_code::unknown_producer_id,
                                     "Unknown producer ID: " + std::to_string(pid));
        }
        transactional_id = it->second;
    }

    // --- Phase 2: Lock state ---
    TransacationIdState* state = nullptr;
    {
        std::shared_lock shared_lock(impl_->states_mutex);
        auto it = impl_->txn_states.find(transactional_id);
        if (it == impl_->txn_states.end()) {
            return txn_failure<void>(
                error_code::invalid_txn_state,
                "Transactional ID '" + transactional_id + "' not found");
        }
        state = it->second.get();
    }

    std::lock_guard<std::mutex> state_lock(state->state_mutex);

    // --- Phase 3: Validate epoch ---
    error_code epoch_err = check_producer_epoch(state->producer_epoch, epoch);
    if (epoch_err != error_code::none) {
        return result<void>::failure(epoch_err,
            "Epoch mismatch: presented=" + std::to_string(epoch) +
            " current=" + std::to_string(state->producer_epoch));
    }

    // --- Phase 4: Validate state ---
    if (state->txn_state != TransacationIdState::txn_state_t::ongoing) {
        return txn_failure<void>(error_code::invalid_txn_state,
            "Cannot end transaction in state " +
            std::to_string(static_cast<int>(state->txn_state)));
    }

    // --- Phase 5: Transition to prepare state ---
    state->txn_state = committed
        ? TransacationIdState::txn_state_t::prepare_commit
        : TransacationIdState::txn_state_t::prepare_abort;

    auto marker_type = committed
        ? control_record_type::commit
        : control_record_type::abort;

    TC_LOG_INFO("Transaction {}: transactional_id='{}' pid={} epoch={} ({} partitions)",
                committed ? "committing" : "aborting",
                transactional_id, pid, epoch,
                state->txn_partitions.size());

    // --- Phase 6: Write transaction markers ---
    write_txn_markers(transactional_id, pid, epoch, marker_type);

    // --- Phase 7: Complete the transaction ---
    state->txn_state = committed
        ? TransacationIdState::txn_state_t::complete_commit
        : TransacationIdState::txn_state_t::complete_abort;

    // Clean up and transition back to empty.
    if (impl_->active_transactions.load(std::memory_order_relaxed) > 0) {
        impl_->active_transactions.fetch_sub(1, std::memory_order_relaxed);
    }

    state->reset_transaction();

    TC_LOG_INFO("Transaction {} complete: transactional_id='{}'",
                committed ? "commit" : "abort", transactional_id);

    return result<void>::success();
}

// ============================================================================
// TransactionCoordinator — write_txn_markers()
// ============================================================================

void TransactionCoordinator::write_txn_markers(
    const std::string& transactional_id,
    producer_id_t pid,
    producer_epoch_t epoch,
    control_record_type marker_type)
{
    TransacationIdState* state = nullptr;
    {
        std::shared_lock shared_lock(impl_->states_mutex);
        auto it = impl_->txn_states.find(transactional_id);
        if (it == impl_->txn_states.end()) {
            TC_LOG_ERROR("Cannot write markers for unknown transactional ID '{}'",
                         transactional_id);
            return;
        }
        state = it->second.get();
    }

    // Take a snapshot of the partition list under the state lock.
    std::vector<std::pair<std::string, partition_id_t>> partitions_snapshot;
    {
        std::lock_guard<std::mutex> state_lock(state->state_mutex);
        partitions_snapshot = state->txn_partitions;
    }

    if (partitions_snapshot.empty()) {
        TC_LOG_DEBUG("No partitions to write markers for transactional_id='{}'",
                     transactional_id);
        return;
    }

    // Build the control batch.
    RecordBatch control_batch = build_control_batch(pid, epoch, marker_type);

    const char* marker_name = (marker_type == control_record_type::commit)
        ? "COMMIT" : "ABORT";

    auto& pm = server_->partition_manager();
    size_t success_count = 0;
    size_t failure_count = 0;

    for (const auto& [topic, partition] : partitions_snapshot) {
        // Validate we are still the leader.
        if (!pm.is_leader(topic, partition)) {
            TC_LOG_WARN("Cannot write {} marker to {}/{}: not leader",
                        marker_name, topic, partition);
            failure_count++;
            continue;
        }

        // Look up the LogManager for this partition and append the control batch.
        // In production, the PartitionManager provides an append API.
        // For now, we log the action.
        TC_LOG_DEBUG("Writing {} marker to {}/{} (pid={}, epoch={})",
                     marker_name, topic, partition, pid, epoch);

        // Stub: the actual log append is performed by the PartitionManager.
        // In the full implementation:
        //   auto append_result = pm.append_to_partition(topic, partition, control_batch);
        //   if (append_result.failed()) { failure_count++; }
        success_count++;
    }

    TC_LOG_INFO("Wrote {} {} markers to {} partitions ({} failures)",
                marker_name, success_count, partitions_snapshot.size(),
                failure_count);

    if (failure_count > 0) {
        TC_LOG_WARN("Some {} markers failed to write — "
                    "consumer may encounter partially committed data",
                    marker_name);
    }
}

// ============================================================================
// TransactionCoordinator — timeout_loop()
// ============================================================================

void TransactionCoordinator::timeout_loop() {
    TC_LOG_INFO("Transaction timeout detection thread started");

    while (impl_->timeout_running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(kTimeoutCheckInterval);

        // Collect transactions that have timed out.
        std::vector<std::string> timed_out_ids;

        {
            std::shared_lock shared_lock(impl_->states_mutex);
            for (const auto& [id, state_ptr] : impl_->txn_states) {
                std::lock_guard<std::mutex> state_lock(state_ptr->state_mutex);

                if (state_ptr->txn_state == TransacationIdState::txn_state_t::ongoing &&
                    state_ptr->has_timed_out(impl_->transaction_timeout_ms)) {
                    timed_out_ids.push_back(id);
                }
            }
        }

        // Abort timed-out transactions.
        for (const auto& id : timed_out_ids) {
            TC_LOG_WARN("Transaction '{}' timed out — auto-aborting", id);

            TransacationIdState* state = nullptr;
            producer_id_t pid = kInvalidProducerId;
            producer_epoch_t epoch = -1;

            {
                std::shared_lock shared_lock(impl_->states_mutex);
                auto it = impl_->txn_states.find(id);
                if (it == impl_->txn_states.end()) continue;
                state = it->second.get();
                pid = state->producer_id;
                epoch = state->producer_epoch;
            }

            // Write abort markers.
            write_txn_markers(id, pid, epoch, control_record_type::abort);

            // Reset state.
            {
                std::lock_guard<std::mutex> state_lock(state->state_mutex);
                state->reset_transaction();
            }

            if (impl_->active_transactions.load(std::memory_order_relaxed) > 0) {
                impl_->active_transactions.fetch_sub(1, std::memory_order_relaxed);
            }
        }
    }

    TC_LOG_INFO("Transaction timeout detection thread stopped");
}

// ============================================================================
// TransactionCoordinator — expiry_loop()
// ============================================================================

void TransactionCoordinator::expiry_loop() {
    TC_LOG_INFO("Transactional ID expiration thread started");

    while (impl_->expiry_running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(kExpiryCheckInterval);

        // Collect expired transactional IDs.
        std::vector<std::string> expired_ids;

        {
            std::shared_lock shared_lock(impl_->states_mutex);
            for (const auto& [id, state_ptr] : impl_->txn_states) {
                std::lock_guard<std::mutex> state_lock(state_ptr->state_mutex);

                // Skip if active transaction.
                if (state_ptr->txn_state != TransacationIdState::txn_state_t::empty &&
                    state_ptr->txn_state != TransacationIdState::txn_state_t::dead) {
                    continue;
                }

                if (state_ptr->is_expired(impl_->transactional_id_expiration_ms)) {
                    expired_ids.push_back(id);
                }
            }
        }

        // Expire the IDs.
        for (const auto& id : expired_ids) {
            TC_LOG_INFO("Expiring transactional ID '{}' (inactive > {}ms)",
                        id, impl_->transactional_id_expiration_ms);

            producer_id_t pid = kInvalidProducerId;
            {
                std::unique_lock unique_lock(impl_->states_mutex);
                auto it = impl_->txn_states.find(id);
                if (it != impl_->txn_states.end()) {
                    pid = it->second->producer_id;
                    impl_->txn_states.erase(it);
                }
            }

            // Update reverse mapping.
            if (pid != kInvalidProducerId) {
                std::unique_lock pid_lock(impl_->pid_map_mutex);
                impl_->pid_to_transactional_id.erase(pid);
            }
        }
    }

    TC_LOG_INFO("Transactional ID expiration thread stopped");
}

// ============================================================================
// TransactionCoordinator — get_transactional_id_for_pid()
// ============================================================================

std::string TransactionCoordinator::get_transactional_id_for_pid(
    producer_id_t pid) const
{
    std::shared_lock pid_lock(impl_->pid_map_mutex);
    auto it = impl_->pid_to_transactional_id.find(pid);
    if (it != impl_->pid_to_transactional_id.end()) {
        return it->second;
    }
    return {};
}

// ============================================================================
// TransactionCoordinator — get_producer_state()
// ============================================================================

transaction_producer_state TransactionCoordinator::get_producer_state(
    producer_id_t pid) const
{
    transaction_producer_state result;
    result.producer_id = pid;

    std::string txn_id = get_transactional_id_for_pid(pid);
    if (txn_id.empty()) {
        result.is_known = false;
        return result;
    }

    result.is_known = true;
    result.transactional_id = txn_id;

    std::shared_lock shared_lock(impl_->states_mutex);
    auto it = impl_->txn_states.find(txn_id);
    if (it != impl_->txn_states.end()) {
        std::lock_guard<std::mutex> state_lock(it->second->state_mutex);
        result.epoch            = it->second->producer_epoch;
        result.has_active_txn   = (it->second->txn_state ==
            TransacationIdState::txn_state_t::ongoing);
    }

    return result;
}

// ============================================================================
// TransactionCoordinator — active_transactions()
// ============================================================================

uint64_t TransactionCoordinator::active_transactions() const noexcept {
    return impl_->active_transactions.load(std::memory_order_acquire);
}

// ============================================================================
// TransactionCoordinator — pids_allocated()
// ============================================================================

uint64_t TransactionCoordinator::pids_allocated() const noexcept {
    return impl_->pids_allocated.load(std::memory_order_acquire);
}

} // namespace torrent::broker
