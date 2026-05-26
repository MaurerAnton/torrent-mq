#pragma once

#include <cstdint>
#include <list>
#include <unordered_map>
#include <mutex>
#include <optional>
#include <functional>
#include <shared_mutex>

namespace torrent {

/**
 * Thread-safe LRU (Least Recently Used) cache.
 *
 * Evicts the least recently accessed entry when capacity is exceeded.
 * Uses a doubly-linked list for order tracking and a hash map for O(1) lookup.
 *
 * @tparam K Key type (must be hashable and equality-comparable)
 * @tparam V Value type
 */
template<typename K, typename V>
class lru_cache {
public:
    using key_type = K;
    using value_type = V;
    using kv_pair = std::pair<K, V>;

    /**
     * Construct with the given maximum capacity.
     *
     * @param max_entries Maximum number of entries before eviction begins
     */
    explicit lru_cache(size_t max_entries = 1024)
        : max_entries_(std::max(size_t(1), max_entries)) {}

    /**
     * Get a value by key, promoting it to most-recently-used.
     *
     * @param key The key to look up
     * @return std::optional<V> — the value if found, nullopt otherwise
     */
    std::optional<V> get(const K& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it == map_.end()) {
            return std::nullopt;
        }

        // Move to front (most recently used)
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        return it->second->second;
    }

    /**
     * Insert or update a key-value pair.
     * If the key exists, updates the value and promotes to MRU.
     * If at capacity, evicts the least recently used entry.
     *
     * @param key The key
     * @param value The value
     */
    void put(const K& key, const V& value) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = map_.find(key);
        if (it != map_.end()) {
            // Update existing
            it->second->second = value;
            lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
            return;
        }

        // Evict if at capacity
        if (lru_list_.size() >= max_entries_) {
            auto last = lru_list_.back();
            map_.erase(last.first);
            lru_list_.pop_back();
        }

        // Insert new
        lru_list_.emplace_front(key, value);
        map_[key] = lru_list_.begin();
    }

    /**
     * Check if a key exists without promoting it.
     */
    [[nodiscard]] bool contains(const K& key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return map_.find(key) != map_.end();
    }

    /**
     * Remove a key from the cache.
     *
     * @param key The key to remove
     * @return true if the key was found and removed
     */
    bool erase(const K& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it == map_.end()) {
            return false;
        }
        lru_list_.erase(it->second);
        map_.erase(it);
        return true;
    }

    /**
     * Remove all entries.
     */
    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        lru_list_.clear();
        map_.clear();
    }

    /**
     * Get current number of entries.
     */
    [[nodiscard]] size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return lru_list_.size();
    }

    /**
     * Get maximum capacity.
     */
    [[nodiscard]] size_t capacity() const noexcept { return max_entries_; }

    /**
     * Check if the cache is empty.
     */
    [[nodiscard]] bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return lru_list_.empty();
    }

    /**
     * Get the current hit ratio (hits / total lookups).
     * Resets when reported for sliding window behavior.
     */
    [[nodiscard]] double hit_ratio() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (total_lookups_ == 0) return 0.0;
        return static_cast<double>(hits_) / static_cast<double>(total_lookups_);
    }

    /**
     * Reset the hit/miss counters.
     */
    void reset_stats() {
        std::lock_guard<std::mutex> lock(mutex_);
        hits_ = 0;
        total_lookups_ = 0;
    }

private:
    using list_type = std::list<kv_pair>;
    using map_type = std::unordered_map<K, typename list_type::iterator>;

    size_t max_entries_;
    list_type lru_list_;
    map_type map_;
    mutable std::mutex mutex_;
    mutable uint64_t hits_ = 0;
    mutable uint64_t total_lookups_ = 0;
};

} // namespace torrent
