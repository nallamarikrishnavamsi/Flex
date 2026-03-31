/**
 * @file lru_cache.hpp
 * @brief Thread-safe LRU (Least Recently Used) cache template.
 *
 * Implements a fixed-capacity cache using a doubly-linked list (for LRU ordering)
 * and an unordered_map (for O(1) key lookup). All operations are guarded by a
 * mutex, making the cache safe for concurrent access from multiple threads.
 *
 * Used by the database engine for:
 *   - parse_cache_: caching parsed SQL queries to avoid re-parsing identical statements.
 *   - cache_: caching SELECT query results for read-heavy workloads.
 */

#ifndef FLEXQL_CACHE_LRU_CACHE_HPP
#define FLEXQL_CACHE_LRU_CACHE_HPP

#include <list>
#include <mutex>
#include <string>
#include <unordered_map>

namespace flexql {

template <typename T>
class LruCache {
public:
    explicit LruCache(std::size_t capacity) : capacity_(capacity) {}

    /**
     * Look up a key in the cache.
     * On hit, promotes the entry to the front of the LRU list (most recently used).
     * @param key  Cache key to look up.
     * @param out  On hit, receives a copy of the cached value.
     * @return true if found, false on cache miss.
     */
    bool get(const std::string& key, T& out) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = map_.find(key);
        if (it == map_.end()) {
            return false;
        }
        entries_.splice(entries_.begin(), entries_, it->second);
        out = it->second->second;
        return true;
    }

    /**
     * Insert or update a key-value pair in the cache.
     * If the key already exists, updates the value and promotes it.
     * If at capacity, evicts the least recently used entry.
     * @param key    Cache key.
     * @param value  Value to store.
     */
    void put(const std::string& key, const T& value) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second->second = value;
            entries_.splice(entries_.begin(), entries_, it->second);
            return;
        }

        entries_.emplace_front(key, value);
        map_[key] = entries_.begin();

        if (entries_.size() > capacity_) {
            auto& back = entries_.back();
            map_.erase(back.first);
            entries_.pop_back();
        }
    }

    /** Clear all entries from the cache. */
    void clear() {
        std::lock_guard<std::mutex> lock(mu_);
        entries_.clear();
        map_.clear();
    }

private:
    std::size_t capacity_;                      /// Maximum number of entries
    std::mutex mu_;                              /// Guards all cache operations
    std::list<std::pair<std::string, T>> entries_;  /// LRU list: front = most recent, back = least recent
    std::unordered_map<std::string, typename std::list<std::pair<std::string, T>>::iterator> map_;  /// Key → list iterator for O(1) access
};

}  // namespace flexql

#endif

