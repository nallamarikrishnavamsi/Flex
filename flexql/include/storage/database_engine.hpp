/**
 * @file database_engine.hpp
 * @brief Core in-memory SQL database engine with WAL persistence.
 *
 * DatabaseEngine is the central component of FlexQL. It manages:
 *   - In-memory table storage with per-table reader-writer locks for concurrency.
 *   - Two-level locking: global SRWLOCK for table map mutations (CREATE/DROP),
 *     per-table locks for row-level operations (INSERT/SELECT).
 *   - LRU caches for parsed SQL (parse_cache_) and query results (cache_), with
 *     generation-based lazy invalidation on writes.
 *   - A fast-path inline SQL parser (try_fast_select) that bypasses the full parser
 *     and LRU cache mutex for PK SELECT queries, achieving zero heap allocations.
 *   - A fast-path INSERT (try_fast_insert) that parses values from raw SQL with
 *     zero intermediate allocations and pre-parsed int64 PK values.
 *   - Write-Ahead Logging via WalWriter for crash recovery.
 *   - Background row expiration reaper thread (TTL-based eviction).
 */

#ifndef FLEXQL_STORAGE_DATABASE_ENGINE_HPP
#define FLEXQL_STORAGE_DATABASE_ENGINE_HPP

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#ifdef _WIN32
#include <winsock2.h>
#include <windows.h>
#else
#include <shared_mutex>
#endif

#include "cache/lru_cache.hpp"
#include "common/types.hpp"
#include "parser/sql_parser.hpp"
#include "storage/wal_writer.hpp"

namespace flexql {

class DatabaseEngine {
public:
    /// @param default_ttl_seconds  Row expiry (default 3600s)
    /// @param data_dir  Directory for persistent WAL storage
    explicit DatabaseEngine(int default_ttl_seconds = 3600,
                            const std::string& data_dir = "./flexql_data");
    ~DatabaseEngine();

    // Non-copyable / non-movable
    DatabaseEngine(const DatabaseEngine&) = delete;
    DatabaseEngine& operator=(const DatabaseEngine&) = delete;

    bool execute(std::string sql, QueryResult& out, std::string& err);

    /// Fast-path for PK SELECT: writes response directly to buffer, no QueryResult alloc.
    /// Returns true if handled (even on error). False = fall through to normal execute.
    bool try_fast_select(const std::string& sql, std::string& response_buf, char sep, std::string& err);

    /// Enable/disable WAL persistence (disable for pure-benchmark mode)
    void set_wal_enabled(bool v) { wal_enabled_ = v; }

    /// Replay the WAL file to restore persisted state.
    int replay_wal();

    /// Checkpoint: rewrite WAL with compact current state (SQLite-inspired).
    void checkpoint_wal();

private:
    bool execute_drop(const DropTableQuery& query, std::string& err);
    bool execute_create(const CreateTableQuery& query, std::string& err);
    bool execute_insert(const InsertQuery& query, std::string& err);
    bool execute_select(const SelectQuery& query, QueryResult& out, std::string& err);

    bool validate_value(ColumnType type, const std::string& value) const;
    void cleanup_expired_locked(Table& table);
    int find_column_index(const Table& table, const std::string& name) const;
    bool try_fast_insert(std::string& sql, std::string& err);

    /// Background expiration reaper loop
    void reaper_loop();

    // Concurrency: SRWLOCK on Windows, std::shared_mutex on Linux.
    // Reader-writer lock allows concurrent SELECT queries while serializing writes.
#ifdef _WIN32
    mutable SRWLOCK lock_;
#else
    mutable std::shared_mutex lock_;
#endif

    std::unordered_map<std::string, Table> tables_;
    SqlParser parser_;
    LruCache<ParsedQuery> parse_cache_;
    LruCache<QueryResult> cache_;
    int default_ttl_seconds_;
    uint64_t cache_gen_;   // generation when cache was last validated
    std::atomic<uint64_t> write_gen_;   // incremented on every write (insert/create/drop)

    // Persistent storage
    std::unique_ptr<WalWriter> wal_;
    std::string data_dir_;
    bool is_replaying_ = false;
    bool wal_enabled_ = true;

    // Background expiration reaper thread
    std::thread reaper_thread_;
    std::atomic<bool> reaper_stop_{false};
    std::mutex reaper_mutex_;
    std::condition_variable reaper_cv_;
};

}  // namespace flexql

#endif
