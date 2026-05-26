#pragma once

#include <unordered_map>
#include <shared_mutex>
#include <optional>
#include <vector>
#include <functional>
#include <algorithm>

namespace torrent {

/**
 * Thread-safe concurrent hash map with reader-writer locking.
 *
 * Uses std::shared_mutex to allow multiple concurrent readers
 * while writers get exclusive access. Optimized for read-heavy
 * workloads (metadata caches, topic registries, connection maps).
 *
 * @tparam K Key type (must be hashable and equality-comparable)
 * @tparam V Value type
 */
template<typename K, typename V>
class concurrent_map {
public:
    using key_type = K;
    using value_type = V;
    using pair_type = std::pair<K, V>;

    concurrent_map() = default;

    /**
     * Insert or update a value.
     *
     * @param key The key
     * @param value The value (moved)
     */
    void insert(const K& key, V value) {
        std::unique_lock lock(mutex_);
        map_[key] = std::move(value);
    }

    /**
     * Insert if the key does not exist. Returns true if inserted.
     */
    bool try_insert(const K& key, V value) {
        std::unique_lock lock(mutex_);
        if (map_.find(key) != map_.end()) {
            return false;
        }
        map_[key] = std::move(value);
        return true;
    }

    /**
     * Get a value by key (read lock).
     *
     * @param key The key
     * @return std::optional<V> — value if found, nullopt otherwise
     */
    std::optional<V> get(const K& key) const {
        std::shared_lock lock(mutex_);
        auto it = map_.find(key);
        if (it == map_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    /**
     * Check if a key exists.
     */
    [[nodiscard]] bool contains(const K& key) const {
        std::shared_lock lock(mutex_);
        return map_.find(key) != map_.end();
    }

    /**
     * Erase a key. Returns true if the key was found and erased.
     */
    bool erase(const K& key) {
        std::unique_lock lock(mutex_);
        return map_.erase(key) > 0;
    }

    /**
     * Get the current size.
     */
    [[nodiscard]] size_t size() const {
        std::shared_lock lock(mutex_);
        return map_.size();
    }

    /**
     * Check if empty.
     */
    [[nodiscard]] bool empty() const {
        std::shared_lock lock(mutex_);
        return map_.empty();
    }

    /**
     * Remove all entries.
     */
    void clear() {
        std::unique_lock lock(mutex_);
        map_.clear();
    }

    /**
     * Apply a function to each entry (read lock).
     *
     * @param fn Callable taking (const K&, const V&)
     */
    template<typename Fn>
    void for_each(Fn&& fn) const {
        std::shared_lock lock(mutex_);
        for (const auto& [key, value] : map_) {
            fn(key, value);
        }
    }

    /**
     * Get all keys (read lock).
     */
    [[nodiscard]] std::vector<K> keys() const {
        std::shared_lock lock(mutex_);
        std::vector<K> result;
        result.reserve(map_.size());
        for (const auto& [key, _] : map_) {
            result.push_back(key);
        }
        return result;
    }

    /**
     * Get all entries as a vector of pairs (read lock).
     */
    [[nodiscard]] std::vector<pair_type> entries() const {
        std::shared_lock lock(mutex_);
        std::vector<pair_type> result;
        result.reserve(map_.size());
        for (const auto& entry : map_) {
            result.push_back(entry);
        }
        return result;
    }

    /**
     * Update a value atomically using a callback.
     * The callback receives a mutable reference to the value.
     * If the key doesn't exist, the callback is not called.
     *
     * @param key The key
     * @param fn Callable taking (V&)
     * @return true if the key was found and updated
     */
    template<typename Fn>
    bool update(const K& key, Fn&& fn) {
        std::unique_lock lock(mutex_);
        auto it = map_.find(key);
        if (it == map_.end()) {
            return false;
        }
        fn(it->second);
        return true;
    }

    /**
     * Get-or-create: returns existing value or inserts a new one via factory.
     *
     * @param key The key
     * @param factory Callable returning V
     * @return Reference to the value (existing or newly created)
     */
    template<typename Fn>
    V& get_or_create(const K& key, Fn&& factory) {
        std::unique_lock lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            return it->second;
        }
        auto [new_it, _] = map_.emplace(key, factory());
        return new_it->second;
    }

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<K, V> map_;
};

} // namespace torrent
