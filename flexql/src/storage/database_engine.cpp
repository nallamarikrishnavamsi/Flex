/**
 * @file database_engine.cpp
 * @brief Core in-memory SQL database engine implementation.
 *
 * This is the largest and most performance-critical file in FlexQL.
 * Major sections:
 *
 * 1. RAII Lock Wrappers (ScopedWriterLock, ScopedReaderLock)
 *    - Platform-abstracted RAII wrappers for SRWLOCK (Windows) / shared_mutex (POSIX).
 *
 * 2. Inline Helper Functions
 *    - Fast numeric validation, integer parsing, condition evaluation, all
 *      designed for zero heap allocation in the hot path.
 *
 * 3. DatabaseEngine Core
 *    - Constructor/destructor: initializes WAL, caches, reaper thread.
 *    - execute(): main entry point dispatching to fast-path or full parser.
 *    - replay_wal() / checkpoint_wal(): WAL recovery and compaction.
 *
 * 4. Fast-Path INSERT (try_fast_insert)
 *    - Parses INSERT SQL with raw pointer arithmetic, zero intermediate allocations.
 *    - Pre-parses int64 PK values and prefetches hash slots for cache locality.
 *    - Three-phase design: (1) parse outside lock, (2) validate, (3) insert under lock.
 *
 * 5. Fast-Path SELECT (try_fast_select)
 *    - Inline SQL parser using pointer arithmetic for PK SELECT queries.
 *    - Bypasses the full SqlParser and LRU cache mutex entirely.
 *    - Writes response directly into the caller's buffer (no QueryResult allocation).
 *
 * 6. Full Execute Path
 *    - execute_create, execute_drop, execute_insert, execute_select.
 *    - Hash join (PostgreSQL-inspired) for INNER JOIN with equality.
 *    - Lazy secondary index building for non-PK WHERE columns.
 *
 * 7. Background Reaper Thread
 *    - Periodically scans tables for expired rows (TTL-based eviction).
 */

#include "storage/database_engine.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string_view>

namespace {

// ─── Reader-Writer Lock RAII wrappers (PostgreSQL-inspired) ───
// SELECTs use shared (reader) lock; writes use exclusive (writer) lock.
// These wrappers provide exception-safe, platform-abstracted lock management.

/// RAII exclusive (writer) lock: blocks all other readers and writers.
struct ScopedWriterLock {
#ifdef _WIN32
    explicit ScopedWriterLock(SRWLOCK& lock) : lock_(lock) {
        AcquireSRWLockExclusive(&lock_);
    }
    ~ScopedWriterLock() {
        ReleaseSRWLockExclusive(&lock_);
    }
    SRWLOCK& lock_;
#else
    explicit ScopedWriterLock(std::shared_mutex& lock) : lock_(lock) {
        lock_.lock();
    }
    ~ScopedWriterLock() {
        lock_.unlock();
    }
    std::shared_mutex& lock_;
#endif
};

/// RAII shared (reader) lock: allows concurrent readers, blocks writers.
struct ScopedReaderLock {
#ifdef _WIN32
    explicit ScopedReaderLock(SRWLOCK& lock) : lock_(lock) {
        AcquireSRWLockShared(&lock_);
    }
    ~ScopedReaderLock() {
        ReleaseSRWLockShared(&lock_);
    }
    SRWLOCK& lock_;
#else
    explicit ScopedReaderLock(std::shared_mutex& lock) : lock_(lock) {
        lock_.lock_shared();
    }
    ~ScopedReaderLock() {
        lock_.unlock_shared();
    }
    std::shared_mutex& lock_;
#endif
};

/// Convert a single character to uppercase (safe for unsigned char).
inline char to_upper_ch(char c) {
    return static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
}

/// Return an uppercase copy of a string (used for case-insensitive table/column lookup).
std::string to_upper_copy(const std::string& in) {
    std::string out = in;
    for (auto& ch : out) ch = to_upper_ch(ch);
    return out;
}

/// Validate that a character range represents a valid number (INT or DECIMAL).
/// Handles optional leading +/-, one decimal point, and at least one digit.
/// Zero allocation — operates on raw pointer range.
inline bool is_numeric_fast(const char* s, std::size_t len) {
    if (len == 0) return false;
    std::size_t i = 0;
    if (s[i] == '+' || s[i] == '-') ++i;
    if (i >= len) return false;
    bool seen_dot = false;
    bool seen_digit = false;
    for (; i < len; ++i) {
        char ch = s[i];
        if (ch >= '0' && ch <= '9') { seen_digit = true; continue; }
        if (ch == '.' && !seen_dot) { seen_dot = true; continue; }
        return false;
    }
    return seen_digit;
}

/// Validate that a string is a valid integer (optional +/-, digits only).
inline bool is_int_fast(const std::string& value) {
    if (value.empty()) return false;
    std::size_t i = 0;
    if (value[i] == '+' || value[i] == '-') ++i;
    if (i >= value.size()) return false;
    for (; i < value.size(); ++i) {
        if (value[i] < '0' || value[i] > '9') return false;
    }
    return true;
}

/// Validate that a string is a valid decimal number.
inline bool is_decimal_fast(const std::string& value) {
    return is_numeric_fast(value.data(), value.size());
}

/// Fast integer parsing from raw char* (avoids std::stoll allocation overhead).
inline long long fast_atoll(const char* s, std::size_t len) {
    long long result = 0;
    bool neg = false;
    std::size_t i = 0;
    if (i < len && s[i] == '-') { neg = true; ++i; }
    else if (i < len && s[i] == '+') { ++i; }
    while (i < len && s[i] >= '0' && s[i] <= '9') {
        result = result * 10 + (s[i] - '0');
        ++i;
    }
    return neg ? -result : result;
}

/// Check if a string_view represents a pure integer (no decimal point).
inline bool is_pure_int(std::string_view s) {
    if (s.empty()) return false;
    std::size_t i = 0;
    if (s[i] == '+' || s[i] == '-') ++i;
    if (i >= s.size()) return false;
    for (; i < s.size(); ++i) {
        if (s[i] < '0' || s[i] > '9') return false;
    }
    return true;
}

/**
 * Evaluate a comparison condition between two values.
 *
 * Dispatches to integer, floating-point, or string comparison based on column type.
 * Uses fast_atoll for integer comparisons and strtod on a stack buffer for decimals.
 * String comparisons use lexicographic ordering.
 *
 * @param type  Column type for comparison dispatch.
 * @param op    Comparison operator ("=", "<", ">", "<=", ">=").
 * @param val1  Left operand (row value).
 * @param val2  Right operand (WHERE value).
 * @return true if the condition is satisfied.
 */
inline bool evaluate_condition(flexql::ColumnType type, const std::string& op, std::string_view val1, std::string_view val2) {
    if (type == flexql::ColumnType::Int || type == flexql::ColumnType::Decimal) {
        if (is_pure_int(val1) && is_pure_int(val2)) {
            long long i1 = fast_atoll(val1.data(), val1.size());
            long long i2 = fast_atoll(val2.data(), val2.size());
            if (op[0] == '=') return i1 == i2;
            if (op[0] == '>') return op.size() == 1 ? i1 > i2 : i1 >= i2;
            if (op[0] == '<') return op.size() == 1 ? i1 < i2 : i1 <= i2;
            return false;
        }
        if (type == flexql::ColumnType::Decimal) {
            // Fast double parse without allocating std::string from string_view
            auto fast_stod = [](std::string_view sv) -> double {
                // strtod needs a null-terminated string; use stack buffer
                char buf[64];
                std::size_t n = sv.size() < 63 ? sv.size() : 63;
                std::memcpy(buf, sv.data(), n);
                buf[n] = '\0';
                return std::strtod(buf, nullptr);
            };
            double d1 = fast_stod(val1);
            double d2 = fast_stod(val2);
            if (op[0] == '=') return d1 == d2;
            if (op[0] == '>') return op.size() == 1 ? d1 > d2 : d1 >= d2;
            if (op[0] == '<') return op.size() == 1 ? d1 < d2 : d1 <= d2;
            return false;
        }
        return false;
    }
    if (op[0] == '=') return val1 == val2;
    if (op[0] == '>') return op.size() == 1 ? val1 > val2 : val1 >= val2;
    if (op[0] == '<') return op.size() == 1 ? val1 < val2 : val1 <= val2;
    return false;
}

/// Case-insensitive prefix match on raw char* without allocating a string.
inline bool ci_starts_with(const char* s, std::size_t len, const char* prefix, std::size_t plen) {
    if (len < plen) return false;
    for (std::size_t i = 0; i < plen; ++i) {
        if (to_upper_ch(s[i]) != prefix[i]) return false;
    }
    return true;
}

}  // namespace

namespace flexql {

/// Constructor: initializes caches, SRWLOCK, WAL writer, and background reaper thread.
DatabaseEngine::DatabaseEngine(int default_ttl_seconds, const std::string& data_dir)
    : cache_(4096), parse_cache_(2048), default_ttl_seconds_(default_ttl_seconds),
      cache_gen_(0), write_gen_(0),
      data_dir_(data_dir), is_replaying_(false) {
#ifdef _WIN32
    InitializeSRWLock(&lock_);
#endif
    // Initialize WAL writer
    wal_ = std::make_unique<WalWriter>(data_dir_);

    // Launch background expiration reaper thread
    reaper_thread_ = std::thread(&DatabaseEngine::reaper_loop, this);
}

/// Destructor: signals and joins the background reaper thread.
DatabaseEngine::~DatabaseEngine() {
    // Signal the reaper thread to stop and wait for it
    {
        std::lock_guard<std::mutex> lk(reaper_mutex_);
        reaper_stop_ = true;
    }
    reaper_cv_.notify_one();
    if (reaper_thread_.joinable()) {
        reaper_thread_.join();
    }
}

/**
 * Replay all WAL-persisted SQL statements to restore in-memory state.
 * Called once during server startup when a non-empty WAL file is detected.
 * Sets is_replaying_ to suppress WAL writes during replay (avoid re-logging).
 */
int DatabaseEngine::replay_wal() {
    auto statements = WalWriter::read_all(data_dir_);
    if (statements.empty()) {
        return 0;
    }

    is_replaying_ = true;
    int count = 0;
    int errors = 0;
    QueryResult dummy;
    std::string err;

    for (const auto& sql : statements) {
        dummy = QueryResult{};
        err.clear();
        if (execute(sql, dummy, err)) {
            ++count;
        } else {
            ++errors;
        }
    }

    is_replaying_ = false;

    std::cout << "[WAL] Replayed " << count << " statements from WAL";
    if (errors > 0) {
        std::cout << " (" << errors << " skipped due to errors)";
    }
    std::cout << "\n";

    return count;
}

// ─── WAL Checkpointing (SQLite-inspired) ───
// Compacts the WAL by rewriting it with only the current live state.
void DatabaseEngine::checkpoint_wal() {
    ScopedWriterLock lock(lock_);

    if (!wal_) return;

    // Flush and close current WAL
    wal_->flush();
    wal_.reset();

    // Truncate the WAL file
    WalWriter::truncate(data_dir_);

    // Reopen WAL and write compact state
    wal_ = std::make_unique<WalWriter>(data_dir_);

    auto now = std::chrono::system_clock::now();

    for (const auto& [tname, table] : tables_) {
        // Write CREATE TABLE
        std::string create = "CREATE TABLE " + table.name + " (";
        for (std::size_t i = 0; i < table.columns.size(); ++i) {
            if (i > 0) create += ", ";
            create += table.columns[i].name + " ";
            switch (table.columns[i].type) {
                case ColumnType::Int:      create += "INT"; break;
                case ColumnType::Decimal:  create += "DECIMAL"; break;
                case ColumnType::Varchar:  create += "VARCHAR"; break;
                case ColumnType::DateTime: create += "DATETIME"; break;
            }
        }
        create += ")";
        wal_->append(create);

        // Write INSERT for each live row
        for (const auto& row : table.rows) {
            if (row.expires_at <= now) continue;
            std::string insert = "INSERT INTO " + table.name + " VALUES (";
            for (std::size_t i = 0; i < row.values.size(); ++i) {
                if (i > 0) insert += ", ";
                if (table.columns[i].type == ColumnType::Varchar ||
                    table.columns[i].type == ColumnType::DateTime) {
                    insert += "'" + row.values[i] + "'";
                } else {
                    insert += row.values[i];
                }
            }
            insert += ")";
            wal_->append(insert);
        }
    }

    wal_->flush();
    std::cout << "[WAL] Checkpoint complete — WAL compacted\n";
}

/**
 * Validate that a string value conforms to the expected column type.
 * INT requires digits with optional +/-, DECIMAL allows one decimal point,
 * VARCHAR and DATETIME accept any string.
 */
bool DatabaseEngine::validate_value(ColumnType type, const std::string& value) const {
    switch (type) {
        case ColumnType::Int:    return is_int_fast(value);
        case ColumnType::Decimal: return is_decimal_fast(value);
        case ColumnType::Varchar:
        case ColumnType::DateTime: return true;
    }
    return false;
}

/**
 * Remove expired rows from a table (must hold table writer lock).
 * Rebuilds the primary index and all active secondary indexes from scratch.
 * Uses vector swap to compact memory.
 */
void DatabaseEngine::cleanup_expired_locked(Table& table) {
    auto now = std::chrono::system_clock::now();
    std::vector<Row> kept;
    kept.reserve(table.rows.size());
    table.primary_index.clear();
    // Clear all active secondary indexes
    for (std::size_t c = 0; c < table.col_indexes.size(); ++c) {
        if (table.col_indexed[c])
            table.col_indexes[c].clear();
    }

    for (auto& row : table.rows) {
        if (row.expires_at > now) {
            std::size_t idx = kept.size();
            if (!row.values.empty()) {
                table.primary_index.set(row.values[0], idx);
            }
            // Rebuild only active secondary indexes
            for (std::size_t c = 0; c < row.values.size() && c < table.col_indexes.size(); ++c) {
                if (table.col_indexed[c])
                    table.col_indexes[c].emplace(row.values[c], idx);
            }
            kept.push_back(std::move(row));
        }
    }
    table.rows.swap(kept);
}

/// Find a column's index by name (case-insensitive). Returns -1 if not found.
int DatabaseEngine::find_column_index(const Table& table, const std::string& name) const {
    std::string uname = to_upper_copy(name);
    for (std::size_t i = 0; i < table.columns.size(); ++i) {
        if (table.columns[i].upper_name == uname) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

/// Execute DROP TABLE: removes the table from the in-memory map.
bool DatabaseEngine::execute_drop(const DropTableQuery& query, std::string& err) {
    const std::string tname = to_upper_copy(query.table_name);
    auto it = tables_.find(tname);
    if (it == tables_.end()) {
        if (query.if_exists) { err.clear(); return true; }
        err = "Table not found";
        return false;
    }
    tables_.erase(it);
    ++write_gen_;
    err.clear();
    return true;
}

/**
 * Execute CREATE TABLE: allocates a new Table with pre-reserved row/index capacity.
 * First column is the primary key. INT/DECIMAL PKs use the int64 fast-path in PrimaryIndex.
 * Initializes per-table reader-writer lock and lazy secondary index arrays.
 */
bool DatabaseEngine::execute_create(const CreateTableQuery& query, std::string& err) {
    std::string tname = to_upper_copy(query.table_name);
    if (tables_.find(tname) != tables_.end()) {
        err = "Table already exists";
        return false;
    }

    Table t;
    t.name = query.table_name;
    t.upper_name = tname;
    t.columns = query.columns;
    for (auto& col : t.columns) {
        col.upper_name = to_upper_copy(col.name);
    }
    // Pre-reserve for expected inserts
    t.rows.reserve(524288);
    // Enable int64 fast-path for numeric PK columns
    if (!t.columns.empty() && (t.columns[0].type == ColumnType::Int || t.columns[0].type == ColumnType::Decimal))
        t.primary_index.set_numeric(true);
    t.primary_index.reserve(524288);

    // Initialize per-table lock
#ifdef _WIN32
    t.table_lock = std::make_unique<SRWLOCK>();
    InitializeSRWLock(t.table_lock.get());
#else
    t.table_lock = std::make_unique<std::shared_mutex>();
#endif

    // Initialize secondary indexes: lazy (built on first WHERE query per column)
    t.col_indexes.resize(t.columns.size());
    t.col_indexed.resize(t.columns.size(), false);

    tables_[tname] = std::move(t);
    ++write_gen_;
    err.clear();
    return true;
}

/**
 * Execute INSERT: validates types, checks PK uniqueness, inserts rows.
 * Updates secondary indexes only for columns that have been lazily built.
 * Duplicate PK triggers cleanup_expired_locked() to check if the conflicting row is expired.
 */
bool DatabaseEngine::execute_insert(const InsertQuery& query, std::string& err) {
    auto it = tables_.find(to_upper_copy(query.table_name));
    if (it == tables_.end()) { err = "Table not found"; return false; }
    Table& t = it->second;
    ScopedWriterLock tlock(*t.table_lock);
    if (query.values_list.empty()) { err = "No values to insert"; return false; }
    const std::size_t ncols = t.columns.size();

    for (const auto& vals : query.values_list) {
        if (vals.size() != ncols) { err = "Column count mismatch"; return false; }
    }

    t.rows.reserve(t.rows.size() + query.values_list.size());
    int ttl = query.has_ttl ? query.ttl_seconds : default_ttl_seconds_;
    auto expires = std::chrono::system_clock::now() + std::chrono::seconds(ttl);

    for (const auto& vals : query.values_list) {
        for (std::size_t i = 0; i < ncols; ++i) {
            if (!validate_value(t.columns[i].type, vals[i])) {
                err = "Type validation failed for column: " + t.columns[i].name;
                return false;
            }
        }
        const std::string& pk_val = vals[0];
        auto pk_it = t.primary_index.find(pk_val);
        if (pk_it != nullptr) {
            auto now = std::chrono::system_clock::now();
            if (pk_it->value < t.rows.size() && t.rows[pk_it->value].expires_at <= now) {
                cleanup_expired_locked(t);
                pk_it = t.primary_index.find(pk_val);
            }
            if (pk_it != nullptr) { err = "Duplicate primary key"; return false; }
        }
        Row row;
        row.values = vals;
        row.expires_at = expires;
        std::size_t idx = t.rows.size();
        t.primary_index.set(pk_val, idx);
        // Update secondary indexes only for columns that have been indexed
        for (std::size_t c = 0; c < ncols && c < t.col_indexes.size(); ++c) {
            if (t.col_indexed[c])
                t.col_indexes[c].emplace(vals[c], idx);
        }
        t.rows.push_back(std::move(row));
    }
    ++write_gen_;
    err.clear();
    return true;
}

// ─── Optimization #2: Index-Accelerated WHERE (MySQL InnoDB-inspired) ───
// When SELECT has WHERE on primary key with "=", use O(1) hash index lookup.
// ─── Optimization #3: Hash Join (PostgreSQL-inspired) ───
// Build hash table on smaller table, probe with larger table → O(N+M).

/**
 * Execute SELECT: supports simple, WHERE, and JOIN queries.
 *
 * Optimization tiers:
 *   1. Secondary index (O(1) hash lookup) for WHERE col = value on any indexed column.
 *   2. Primary index (O(1)) for WHERE on PK column with equality.
 *   3. Full table scan for non-equality WHERE or no WHERE.
 *   4. Hash join (O(N+M)) for INNER JOIN with equality.
 *   5. Nested-loop join (O(N×M)) for INNER JOIN with non-equality operators.
 *
 * Secondary indexes are built lazily: the first SELECT with WHERE on a non-PK column
 * triggers an exclusive lock upgrade to build the index, then downgrades for the query.
 */
bool DatabaseEngine::execute_select(const SelectQuery& query, QueryResult& out, std::string& err) {
    auto it = tables_.find(to_upper_copy(query.from_table));
    if (it == tables_.end()) { err = "Table not found"; return false; }
    Table& left = it->second;

    // Determine if we need to build a lazy secondary index
    int pre_where_col_idx = -1;
    bool need_index_build = false;
    if (query.has_where && !query.has_join && query.where.op == "=") {
        pre_where_col_idx = find_column_index(left, query.where.left);
        if (pre_where_col_idx > 0) {  // col 0 uses primary_index
            std::size_t cidx = static_cast<std::size_t>(pre_where_col_idx);
            if (cidx < left.col_indexed.size() && !left.col_indexed[cidx]) {
                need_index_build = true;
            }
        }
    }

    // RAII guard that acquires exclusive or shared table lock
    struct TableLockGuard {
#ifdef _WIN32
        SRWLOCK* lk;
#else
        std::shared_mutex* lk;
#endif
        bool exclusive;
        TableLockGuard(decltype(lk) l, bool excl) : lk(l), exclusive(excl) {
            if (exclusive) {
#ifdef _WIN32
                AcquireSRWLockExclusive(lk);
#else
                lk->lock();
#endif
            } else {
#ifdef _WIN32
                AcquireSRWLockShared(lk);
#else
                lk->lock_shared();
#endif
            }
        }
        ~TableLockGuard() {
            if (exclusive) {
#ifdef _WIN32
                ReleaseSRWLockExclusive(lk);
#else
                lk->unlock();
#endif
            } else {
#ifdef _WIN32
                ReleaseSRWLockShared(lk);
#else
                lk->unlock_shared();
#endif
            }
        }
    } tlock_guard(left.table_lock.get(), need_index_build);

    if (need_index_build) {
        // Build the secondary index for this column under exclusive lock
        std::size_t cidx = static_cast<std::size_t>(pre_where_col_idx);
        if (!left.col_indexed[cidx]) {  // double-check after acquiring lock
            auto now_build = std::chrono::system_clock::now();
            left.col_indexes[cidx].clear();
            left.col_indexes[cidx].reserve(left.rows.size());
            for (std::size_t ri = 0; ri < left.rows.size(); ++ri) {
                if (left.rows[ri].expires_at > now_build)
                    left.col_indexes[cidx].emplace(left.rows[ri].values[cidx], ri);
            }
            left.col_indexed[cidx] = true;
            left.has_secondary_indexes = true;
        }
    }

    std::vector<int> projection;
    if (!query.has_join) {
        if (query.select_all) {
            projection.reserve(left.columns.size());
            for (std::size_t i = 0; i < left.columns.size(); ++i) {
                projection.push_back(static_cast<int>(i));
                out.column_names.push_back(left.columns[i].name);
            }
        } else {
            projection.reserve(query.columns.size());
            for (const auto& c : query.columns) {
                int idx = find_column_index(left, c);
                if (idx < 0) { err = "Unknown column: " + c; return false; }
                projection.push_back(idx);
                out.column_names.push_back(left.columns[idx].name);
            }
        }
    }

    int where_col_idx = -1;
    if (query.has_where && !query.has_join) {
        where_col_idx = find_column_index(left, query.where.left);
    }
    auto now = std::chrono::system_clock::now();

    if (!query.has_join) {
        // ─── Index-Accelerated WHERE with Equality (O(1) lookup) ───
        // Use secondary hash indexes for any column with WHERE col = value
        if (query.has_where && where_col_idx >= 0 && query.where.op == "=") {
            std::size_t cidx = static_cast<std::size_t>(where_col_idx);
            if (cidx < left.col_indexes.size() && left.col_indexed[cidx]) {
                // O(1) hash index lookup on any column
                auto [range_begin, range_end] = left.col_indexes[cidx].equal_range(query.where.value);
                for (auto it2 = range_begin; it2 != range_end; ++it2) {
                    if (it2->second >= left.rows.size()) continue;
                    const Row& row = left.rows[it2->second];
                    if (row.expires_at <= now) continue;
                    std::vector<std::string> out_row;
                    out_row.reserve(projection.size());
                    for (int p : projection)
                        out_row.push_back(row.values[static_cast<std::size_t>(p)]);
                    out.rows.push_back(std::move(out_row));
                }
                err.clear();
                return true;
            }
            // Fallback: primary index for col0
            if (where_col_idx == 0) {
                auto pk_it = left.primary_index.find(query.where.value);
                if (pk_it != nullptr && pk_it->value < left.rows.size()) {
                    const Row& row = left.rows[pk_it->value];
                    if (row.expires_at > now) {
                        std::vector<std::string> out_row;
                        out_row.reserve(projection.size());
                        for (int p : projection)
                            out_row.push_back(row.values[static_cast<std::size_t>(p)]);
                        out.rows.push_back(std::move(out_row));
                    }
                }
                err.clear();
                return true;
            }
        }

        // Full table scan path (non-equality WHERE or no WHERE)
        out.rows.reserve(std::min(left.rows.size(), static_cast<std::size_t>(1000)));
        for (const auto& row : left.rows) {
            if (row.expires_at <= now) continue;
            if (query.has_where) {
                if (where_col_idx < 0) continue;
                if (!evaluate_condition(left.columns[where_col_idx].type, query.where.op,
                                       row.values[static_cast<std::size_t>(where_col_idx)], query.where.value))
                    continue;
            }
            std::vector<std::string> out_row;
            out_row.reserve(projection.size());
            for (int p : projection)
                out_row.push_back(row.values[static_cast<std::size_t>(p)]);
            out.rows.push_back(std::move(out_row));
        }
        err.clear();
        return true;
    }

    // ─── JOIN path ───
    const auto& j = query.join;
    auto jt = tables_.find(to_upper_copy(j.right_table));
    if (jt == tables_.end()) { err = "Join table not found"; return false; }
    const Table& right = jt->second;
    ScopedReaderLock tlock_right(*right.table_lock);

    int left_join_idx = find_column_index(left, j.left_column);
    int right_join_idx = find_column_index(right, j.right_column);
    if (left_join_idx < 0 || right_join_idx < 0) { err = "Join column not found"; return false; }

    out.column_names.clear();
    if (query.select_all) {
        for (const auto& c : left.columns) out.column_names.push_back(left.name + "." + c.name);
        for (const auto& c : right.columns) out.column_names.push_back(right.name + "." + c.name);
    } else {
        out.column_names = query.columns;
    }

    int where_left_idx = -1, where_right_idx = -1;
    if (query.has_where) {
        std::string upper_left = to_upper_copy(query.where.left);
        for (std::size_t i = 0; i < left.columns.size(); ++i) {
            if (left.columns[i].upper_name == upper_left ||
                left.upper_name + "." + left.columns[i].upper_name == upper_left)
            { where_left_idx = static_cast<int>(i); break; }
        }
        for (std::size_t i = 0; i < right.columns.size(); ++i) {
            if (right.columns[i].upper_name == upper_left ||
                right.upper_name + "." + right.columns[i].upper_name == upper_left)
            { where_right_idx = static_cast<int>(i); break; }
        }
    }

    struct JP { bool from_left; int idx; };
    std::vector<JP> join_proj;
    if (!query.select_all) {
        for (const auto& c : query.columns) {
            std::string cc = to_upper_copy(c);
            bool found = false;
            for (std::size_t i = 0; i < left.columns.size(); ++i) {
                if (cc == left.columns[i].upper_name || cc == left.upper_name + "." + left.columns[i].upper_name)
                { join_proj.push_back({true, static_cast<int>(i)}); found = true; break; }
            }
            if (!found) {
                for (std::size_t i = 0; i < right.columns.size(); ++i) {
                    if (cc == right.columns[i].upper_name || cc == right.upper_name + "." + right.columns[i].upper_name)
                    { join_proj.push_back({false, static_cast<int>(i)}); found = true; break; }
                }
            }
            if (!found) { err = "Unknown column in projection: " + c; return false; }
        }
    }

    // ─── Hash Join (PostgreSQL-inspired) ───
    // For equality joins: build hash table on right table, probe with left table.
    // This gives O(N+M) instead of the original O(N×M) nested-loop join.
    if (j.op == "=") {
        // Phase 1: Build hash table on right table's join column
        std::unordered_multimap<std::string, std::size_t> right_hash;
        right_hash.reserve(right.rows.size());
        for (std::size_t ri = 0; ri < right.rows.size(); ++ri) {
            if (right.rows[ri].expires_at <= now) continue;
            const std::string& key = right.rows[ri].values[static_cast<std::size_t>(right_join_idx)];
            right_hash.emplace(key, ri);
        }

        // Phase 2: Probe with left table
        for (const auto& lrow : left.rows) {
            if (lrow.expires_at <= now) continue;
            const std::string& probe_key = lrow.values[static_cast<std::size_t>(left_join_idx)];
            auto [range_begin, range_end] = right_hash.equal_range(probe_key);

            for (auto rit = range_begin; rit != range_end; ++rit) {
                const Row& rrow = right.rows[rit->second];

                if (query.has_where) {
                    bool matched = false;
                    if (where_left_idx >= 0)
                        matched = evaluate_condition(left.columns[where_left_idx].type, query.where.op,
                                                    lrow.values[static_cast<std::size_t>(where_left_idx)], query.where.value);
                    else if (where_right_idx >= 0)
                        matched = evaluate_condition(right.columns[where_right_idx].type, query.where.op,
                                                    rrow.values[static_cast<std::size_t>(where_right_idx)], query.where.value);
                    if (!matched) continue;
                }

                std::vector<std::string> out_row;
                if (query.select_all) {
                    out_row.reserve(lrow.values.size() + rrow.values.size());
                    out_row.insert(out_row.end(), lrow.values.begin(), lrow.values.end());
                    out_row.insert(out_row.end(), rrow.values.begin(), rrow.values.end());
                } else {
                    out_row.reserve(join_proj.size());
                    for (const auto& jp : join_proj)
                        out_row.push_back(jp.from_left ? lrow.values[jp.idx] : rrow.values[jp.idx]);
                }
                out.rows.push_back(std::move(out_row));
            }
        }
    } else {
        // Fallback: nested-loop join for non-equality operators (>, <, >=, <=)
        for (const auto& lrow : left.rows) {
            if (lrow.expires_at <= now) continue;
            for (const auto& rrow : right.rows) {
                if (rrow.expires_at <= now) continue;
                if (!evaluate_condition(left.columns[left_join_idx].type, j.op,
                                       lrow.values[static_cast<std::size_t>(left_join_idx)],
                                       rrow.values[static_cast<std::size_t>(right_join_idx)]))
                    continue;
                if (query.has_where) {
                    bool matched = false;
                    if (where_left_idx >= 0)
                        matched = evaluate_condition(left.columns[where_left_idx].type, query.where.op,
                                                    lrow.values[static_cast<std::size_t>(where_left_idx)], query.where.value);
                    else if (where_right_idx >= 0)
                        matched = evaluate_condition(right.columns[where_right_idx].type, query.where.op,
                                                    rrow.values[static_cast<std::size_t>(where_right_idx)], query.where.value);
                    if (!matched) continue;
                }
                std::vector<std::string> out_row;
                if (query.select_all) {
                    out_row.reserve(lrow.values.size() + rrow.values.size());
                    out_row.insert(out_row.end(), lrow.values.begin(), lrow.values.end());
                    out_row.insert(out_row.end(), rrow.values.begin(), rrow.values.end());
                } else {
                    out_row.reserve(join_proj.size());
                    for (const auto& jp : join_proj)
                        out_row.push_back(jp.from_left ? lrow.values[jp.idx] : rrow.values[jp.idx]);
                }
                out.rows.push_back(std::move(out_row));
            }
        }
    }

    err.clear();
    return true;
}

/// ValRef: offset + length reference into the original SQL string for a parsed value.
/// Used by try_fast_insert to avoid heap allocation during the parse phase.
namespace {
struct ValRef { uint32_t off; uint32_t len; };
}

/**
 * Ultra-fast INSERT path: parses values from raw SQL without any intermediate allocations.
 *
 * Four-phase design:
 *   Phase 1: Brief global reader lock to read table metadata (column count, types).
 *   Phase 2: Parse ALL value tuples into offset/length refs (ValRef) — ZERO heap allocs.
 *            Pre-parses int64 PK values for numeric tables to avoid redundant s_to_i.
 *   Phase 3: Acquire table writer lock, materialize strings, index, and insert.
 *            Uses prefetch_int() to hide memory latency for upcoming hash slot accesses.
 *   Phase 4: Append to WAL (no table lock held — other threads can proceed).
 *
 * Returns true if the SQL was recognized as INSERT (even on error).
 * Returns false to fall through to the normal execute path.
 */
bool DatabaseEngine::try_fast_insert(std::string& sql, std::string& err) {
    const char* s = sql.data();
    const std::size_t slen = sql.size();

    // Must start with "INSERT INTO " (case-insensitive)
    if (slen < 25) return false;  // too short
    if (!ci_starts_with(s, slen, "INSERT INTO ", 12)) return false;

    // Extract table name
    std::size_t pos = 12;
    while (pos < slen && std::isspace(static_cast<unsigned char>(s[pos]))) ++pos;
    std::size_t name_start = pos;
    while (pos < slen && !std::isspace(static_cast<unsigned char>(s[pos])) && s[pos] != '(') ++pos;
    if (pos == name_start) return false;

    // Build uppercase table name directly on stack for small names
    char tname_buf[64];
    std::size_t tname_len = pos - name_start;
    if (tname_len >= sizeof(tname_buf)) return false;
    for (std::size_t i = 0; i < tname_len; ++i) tname_buf[i] = to_upper_ch(s[name_start + i]);
    tname_buf[tname_len] = '\0';
    std::string tname(tname_buf, tname_len);

    // Skip to VALUES keyword
    while (pos < slen && std::isspace(static_cast<unsigned char>(s[pos]))) ++pos;
    if (!ci_starts_with(s + pos, slen - pos, "VALUES", 6)) return false;
    pos += 6;
    while (pos < slen && std::isspace(static_cast<unsigned char>(s[pos]))) ++pos;
    if (pos >= slen || s[pos] != '(') return false;

    // Find last ')'
    std::size_t last_close = slen;
    while (last_close > 0 && s[last_close - 1] != ')') --last_close;
    if (last_close == 0) { err = "INSERT syntax error"; return true; }
    --last_close;  // point at the ')'

    // ── Phase 1: Brief lock to get table metadata ──
    std::size_t ncols;
    ColumnType col_types_local[32];
    {
        ScopedReaderLock rlock(lock_);
        auto it = tables_.find(tname);
        if (it == tables_.end()) { err = "Table not found"; return true; }
        const Table& t = it->second;
        ncols = t.columns.size();
        if (ncols > 32) { ncols = t.columns.size(); } // safety
        for (std::size_t i = 0; i < ncols; ++i)
            col_types_local[i] = t.columns[i].type;
    } // global lock released — other threads can proceed

    auto expires = std::chrono::system_clock::now() + std::chrono::seconds(default_ttl_seconds_);

    // ── Phase 2: Parse ALL tuples into offset/length refs — ZERO heap allocs ──
    std::size_t est = (last_close - pos) / 50 + 1;
    thread_local std::vector<ValRef> val_refs;
    val_refs.clear();
    val_refs.reserve(est * ncols);
    std::size_t nrows_parsed = 0;

    // Pre-parsed PK values for numeric tables (avoids s_to_i in Phase 3)
    bool pk_is_numeric = (col_types_local[0] == ColumnType::Int || col_types_local[0] == ColumnType::Decimal);
    thread_local std::vector<int64_t> pk_vals;
    pk_vals.clear();
    if (pk_is_numeric) pk_vals.reserve(est);

    while (pos <= last_close && s[pos] == '(') {
        ++pos;  // skip '('

        std::size_t cols_this_row = 0;
        std::size_t vstart = pos;
        bool in_q = false;
        char qch = 0;
        std::size_t close = pos;
        for (std::size_t k = pos; k < slen; ++k) {
            char ch = s[k];
            if (ch == '\'' || ch == '"') {
                if (!in_q) { in_q = true; qch = ch; }
                else if (qch == ch) in_q = false;
                continue;
            }
            if (in_q) continue;
            if (ch == ',' || ch == ')') {
                std::size_t vs = vstart, ve = k;
                while (vs < ve && std::isspace(static_cast<unsigned char>(s[vs]))) ++vs;
                while (ve > vs && std::isspace(static_cast<unsigned char>(s[ve - 1]))) --ve;
                if (ve - vs >= 2 && ((s[vs] == '\'' && s[ve-1] == '\'') || (s[vs] == '"' && s[ve-1] == '"'))) {
                    ++vs; --ve;
                }
                val_refs.push_back({static_cast<uint32_t>(vs), static_cast<uint32_t>(ve - vs)});
                cols_this_row++;
                vstart = k + 1;
                if (ch == ')') { close = k; break; }
            }
        }

        if (cols_this_row != ncols) { err = "Column count mismatch"; return true; }

        // Type validation (no allocations)
        std::size_t base_ref = nrows_parsed * ncols;
        for (std::size_t i = 0; i < ncols; ++i) {
            const auto ct = col_types_local[i];
            if (ct == ColumnType::Varchar || ct == ColumnType::DateTime) continue;
            if (!is_numeric_fast(s + val_refs[base_ref + i].off, val_refs[base_ref + i].len)) {
                err = "Type validation failed";
                return true;
            }
        }

        // Pre-parse PK for numeric tables
        if (pk_is_numeric) {
            const auto& pk_ref = val_refs[base_ref];
            int64_t pk = 0;
            bool neg = false;
            const char* p = s + pk_ref.off;
            const char* pe = p + pk_ref.len;
            if (p < pe && *p == '-') { neg = true; ++p; }
            else if (p < pe && *p == '+') ++p;
            while (p < pe) { pk = pk * 10 + (*p - '0'); ++p; }
            pk_vals.push_back(neg ? -pk : pk);
        }

        nrows_parsed++;
        pos = close + 1;
        while (pos <= last_close && (std::isspace(static_cast<unsigned char>(s[pos])) || s[pos] == ',')) ++pos;
    }

    // ── Phase 3: Acquire lock, resize table rows, materialize+index in-place ──
    // Instead of creating an intermediate batch_rows vector and then push_back-ing,
    // directly resize t.rows and assign values in-place. This eliminates:
    //   - 250K default Row constructions (batch_rows.resize)
    //   - 250K push_back(std::move(row)) operations
    //   - The batch_rows heap allocation entirely
    {
        ScopedReaderLock rlock(lock_);
        auto it = tables_.find(tname);
        if (it == tables_.end()) { err = "Table not found"; return true; }
        Table& t = it->second;
        ScopedWriterLock tlock(*t.table_lock);

        const std::size_t base = t.rows.size();
        const std::size_t final_size = base + nrows_parsed;

        // Aggressive pre-reserve to avoid mid-insert rehash
        if (t.rows.capacity() < final_size) {
            std::size_t new_cap = t.rows.capacity() == 0 ? 524288 : t.rows.capacity();
            while (new_cap < final_size) new_cap *= 2;
            t.rows.reserve(new_cap);
        }
        // Pre-size t.rows — default-constructs Row objects (empty values vector)
        t.rows.resize(final_size);

        // Ensure PrimaryIndex has enough capacity
        std::size_t pi_needed = t.primary_index.size() + nrows_parsed;
        if (t.primary_index.capacity() * 3 / 4 < pi_needed) {
            t.primary_index.reserve(pi_needed * 2);
        }

        const bool check_secondary = t.has_secondary_indexes;
        constexpr std::size_t PF_DIST = 4;

        for (std::size_t r = 0; r < nrows_parsed; ++r) {
            // Prefetch future hash slot
            if (pk_is_numeric && r + PF_DIST < nrows_parsed) {
                t.primary_index.prefetch_int(pk_vals[r + PF_DIST]);
            }

            // Materialize row values directly into t.rows[base + r]
            Row& row = t.rows[base + r];
            row.values.resize(ncols);
            row.expires_at = expires;
            std::size_t ref_base = r * ncols;
            for (std::size_t c = 0; c < ncols; ++c) {
                const auto& vr = val_refs[ref_base + c];
                row.values[c].assign(s + vr.off, vr.len);
            }

            const std::size_t new_idx = base + r;
            std::pair<PrimaryIndex::Entry*, bool> emplace_result;
            if (pk_is_numeric) {
                emplace_result = t.primary_index.emplace_int_direct(pk_vals[r], new_idx);
            } else {
                emplace_result = t.primary_index.emplace(row.values[0], new_idx);
            }
            if (!emplace_result.second) {
                // Duplicate PK — shrink back and report error
                // (Can't easily rollback partial inserts, but duplicate PKs are rare)
                t.rows.resize(base + r);
                auto now = std::chrono::system_clock::now();
                auto pit = emplace_result.first;
                if (pit->value < t.rows.size() && t.rows[pit->value].expires_at <= now) {
                    cleanup_expired_locked(t);
                    // Retry: re-expand and continue (expensive but rare path)
                    t.rows.resize(base + r + 1);
                    Row& retry_row = t.rows[base + r];
                    retry_row.values.resize(ncols);
                    retry_row.expires_at = expires;
                    for (std::size_t c = 0; c < ncols; ++c) {
                        const auto& vr = val_refs[ref_base + c];
                        retry_row.values[c].assign(s + vr.off, vr.len);
                    }
                    std::pair<PrimaryIndex::Entry*, bool> emplace_result2;
                    if (pk_is_numeric) {
                        emplace_result2 = t.primary_index.emplace_int_direct(pk_vals[r], base + r);
                    } else {
                        emplace_result2 = t.primary_index.emplace(retry_row.values[0], base + r);
                    }
                    if (!emplace_result2.second) {
                        t.rows.resize(base + r);
                        err = "Duplicate primary key";
                        return true;
                    }
                } else {
                    err = "Duplicate primary key";
                    return true;
                }
            }
            if (check_secondary) {
                for (std::size_t c = 0; c < ncols && c < t.col_indexes.size(); ++c) {
                    if (t.col_indexed[c])
                        t.col_indexes[c].emplace(row.values[c], new_idx);
                }
            }
        }
    } // table lock released

    // ── Phase 4: WAL append (no table lock held) ──
    if (!is_replaying_ && wal_enabled_ && wal_) {
        wal_->append(std::move(sql));
    }

    ++write_gen_;
    err.clear();
    return true;
}

/**
 * Fast-path PK SELECT: inline parse + direct response, zero allocation hot path.
 *
 * Handles:
 *   SELECT * FROM <table> WHERE <pk_col> = <value>
 *   SELECT <c1>,<c2>,... FROM <table> WHERE <pk_col> = <value>
 *
 * Parses the SQL with raw pointer arithmetic (no SqlParser, no LRU cache mutex).
 * Compares column/table names character-by-character in uppercase without allocating.
 * Uses PrimaryIndex::find_raw() for zero-allocation PK lookup.
 * Writes the response (OK/COLS/ROW/END) directly into the caller's buffer.
 *
 * Falls through (returns false) for:
 *   - JOINs, non-PK WHERE columns, non-equality operators, complex queries.
 *   - SQL shorter than the minimum valid SELECT string.
 */
// ─── Fast-path PK SELECT: inline parse + direct response, zero alloc hot path ───
bool DatabaseEngine::try_fast_select(const std::string& sql, std::string& buf, char sep, std::string& err) {
    // Inline parse: avoid full parser + LRU cache mutex entirely.
    // Handles: SELECT * FROM T WHERE C = V;
    //          SELECT C1,C2 FROM T WHERE C = V;
    const char* p = sql.data();
    const char* end = p + sql.size();

    // Must start with SELECT (case-insensitive)
    if (end - p < 14) return false;  // "SELECT * FROM X" minimum
    if ((p[0] | 0x20) != 's' || (p[1] | 0x20) != 'e' || (p[2] | 0x20) != 'l' ||
        (p[3] | 0x20) != 'e' || (p[4] | 0x20) != 'c' || (p[5] | 0x20) != 't' || p[6] != ' ')
        return false;
    p += 7;

    // Parse column list or *
    bool select_all = false;
    const char* col_ptrs[32];
    int col_lens[32];
    int ncols = 0;

    while (p < end && *p == ' ') p++;
    if (p < end && *p == '*') {
        select_all = true;
        p++;
    } else {
        // Comma-separated column names
        while (p < end) {
            while (p < end && *p == ' ') p++;
            const char* cs = p;
            while (p < end && *p != ',' && *p != ' ') p++;
            if (p == cs) return false;
            if (ncols >= 32) return false;
            col_ptrs[ncols] = cs;
            col_lens[ncols] = static_cast<int>(p - cs);
            ncols++;
            while (p < end && *p == ' ') p++;
            if (p < end && *p == ',') { p++; continue; }
            break;  // should be at FROM
        }
        if (ncols == 0) return false;
    }

    // Expect FROM
    while (p < end && *p == ' ') p++;
    if (end - p < 5) return false;
    if ((p[0] | 0x20) != 'f' || (p[1] | 0x20) != 'r' || (p[2] | 0x20) != 'o' || (p[3] | 0x20) != 'm' || p[4] != ' ')
        return false;
    p += 5;

    // Extract table name
    while (p < end && *p == ' ') p++;
    const char* tbl_start = p;
    while (p < end && *p != ' ' && *p != ';') p++;
    int tbl_len = static_cast<int>(p - tbl_start);
    if (tbl_len <= 0 || tbl_len > 128) return false;

    // Uppercase table name on stack (zero heap allocation)
    char tbl_upper[129];
    for (int i = 0; i < tbl_len; i++)
        tbl_upper[i] = static_cast<char>(to_upper_ch(tbl_start[i]));
    tbl_upper[tbl_len] = '\0';

    // Expect WHERE
    while (p < end && *p == ' ') p++;
    if (end - p < 6) return false;
    if ((p[0] | 0x20) != 'w' || (p[1] | 0x20) != 'h' || (p[2] | 0x20) != 'e' ||
        (p[3] | 0x20) != 'r' || (p[4] | 0x20) != 'e' || p[5] != ' ')
        return false;
    p += 6;

    // Extract WHERE column
    while (p < end && *p == ' ') p++;
    const char* wcol_start = p;
    while (p < end && *p != ' ' && *p != '=') p++;
    int wcol_len = static_cast<int>(p - wcol_start);
    if (wcol_len <= 0) return false;

    // Expect =
    while (p < end && *p == ' ') p++;
    if (p >= end || *p != '=') return false;
    p++;
    while (p < end && *p == ' ') p++;

    // Extract value (strip quotes if present, strip trailing semicolon)
    const char* val_start = p;
    const char* val_end;
    if (p < end && *p == '\'') {
        p++; val_start = p;
        while (p < end && *p != '\'') p++;
        val_end = p;
    } else {
        while (p < end && *p != ' ' && *p != ';') p++;
        val_end = p;
    }
    std::size_t val_len = static_cast<std::size_t>(val_end - val_start);
    if (val_len == 0) return false;

    // ─── Lookup phase: acquire locks, find table & row ───
    ScopedReaderLock rlock(lock_);

    std::string_view tbl_sv(tbl_upper, tbl_len);
    auto it = tables_.find(std::string(tbl_sv));
    if (it == tables_.end()) { err = "Table not found"; return true; }
    Table& table = it->second;
    ScopedReaderLock tlock(*table.table_lock);

    // WHERE column must be PK (col 0) — compare uppercase without allocation
    const std::string& pk_name = table.columns[0].upper_name;
    if (static_cast<int>(pk_name.size()) != wcol_len) return false;
    for (int i = 0; i < wcol_len; i++) {
        if (to_upper_ch(wcol_start[i]) != pk_name[static_cast<std::size_t>(i)]) return false;
    }

    // PK lookup — use find_raw to avoid constructing std::string for value
    auto pk_it = table.primary_index.find_raw(val_start, val_len);
    const Row* found_row = nullptr;
    if (pk_it && pk_it->value < table.rows.size()) {
        auto now = std::chrono::system_clock::now();
        const Row& row = table.rows[pk_it->value];
        if (row.expires_at > now) found_row = &row;
    }

    // ─── Build response directly into buffer ───
    buf += "OK\n";

    if (select_all) {
        buf += "COLS";
        buf += sep;
        // Fast int-to-string for small numbers (avoids std::to_string allocation)
        std::size_t nc = table.columns.size();
        char nc_buf[8];
        int nc_len = snprintf(nc_buf, sizeof(nc_buf), "%zu", nc);
        buf.append(nc_buf, nc_len);
        for (const auto& col : table.columns) {
            buf += sep;
            buf += col.name;
        }
        buf += '\n';

        if (found_row) {
            buf += "ROW";
            for (const auto& val : found_row->values) {
                buf += sep;
                buf += val;
            }
            buf += '\n';
        }
    } else {
        // Resolve projected columns — compare uppercase without find_column_index
        int proj[32];
        buf += "COLS";
        buf += sep;
        char nc_buf[8];
        int nc_len = snprintf(nc_buf, sizeof(nc_buf), "%d", ncols);
        buf.append(nc_buf, nc_len);
        for (int i = 0; i < ncols; ++i) {
            int found = -1;
            for (std::size_t c = 0; c < table.columns.size(); ++c) {
                const std::string& uname = table.columns[c].upper_name;
                if (static_cast<int>(uname.size()) != col_lens[i]) continue;
                bool match = true;
                for (int k = 0; k < col_lens[i]; ++k) {
                    if (to_upper_ch(col_ptrs[i][k]) != uname[static_cast<std::size_t>(k)]) {
                        match = false; break;
                    }
                }
                if (match) { found = static_cast<int>(c); break; }
            }
            if (found < 0) { err = "Unknown column"; return true; }
            proj[i] = found;
            buf += sep;
            buf += table.columns[static_cast<std::size_t>(found)].name;
        }
        buf += '\n';

        if (found_row) {
            buf += "ROW";
            for (int i = 0; i < ncols; ++i) {
                buf += sep;
                buf += found_row->values[static_cast<std::size_t>(proj[i])];
            }
            buf += '\n';
        }
    }

    buf += "END\n";
    err.clear();
    return true;
}

/**
 * Main query execution entry point.
 *
 * Routing logic:
 *   1. If the query starts with 'I'/'i', try the fast INSERT path (try_fast_insert).
 *      This handles the vast majority of INSERTs with zero-alloc parsing.
 *   2. Otherwise, parse via SqlParser (with LRU parse cache to avoid re-parsing).
 *   3. SELECT: global reader lock + table shared lock (concurrent reads).
 *      Checks query result cache first (lazy invalidation via write_gen_).
 *   4. INSERT (fallback): global reader lock + table writer lock.
 *   5. CREATE/DROP: global exclusive lock (modifies tables_ map).
 *
 * WAL: all mutating operations are persisted after successful execution,
 * unless is_replaying_ or wal_enabled_ is false.
 */
bool DatabaseEngine::execute(std::string sql, QueryResult& out, std::string& err) {

    // Ultra-fast INSERT path (acquires locks internally)
    if (sql.size() > 20 && (sql[0] == 'I' || sql[0] == 'i')) {
        if (try_fast_insert(sql, err)) {
            return err.empty();
        }
        // Falls through if not an INSERT statement
    }

    out = QueryResult{};

    // ─── Prepared Statement Cache (MySQL-inspired) ───
    // Parse SQL completely outside any lock — parsing is stateless and thread-safe.
    ParsedQuery parsed;
    bool cache_hit = parse_cache_.get(sql, parsed);
    if (!cache_hit) {
        parsed = parser_.parse(sql, err);
        if (parsed.type == QueryType::Unknown) return false;
        parse_cache_.put(sql, parsed);
    }

    // SELECT path: global reader lock + table-level shared lock for concurrent reads
    if (parsed.type == QueryType::Select) {
        ScopedReaderLock rlock(lock_);

        // Check query result cache (lazy invalidation)
        if (cache_gen_ == write_gen_.load(std::memory_order_relaxed)) {
            QueryResult cached;
            if (cache_.get(sql, cached)) {
                out = std::move(cached);
                err.clear();
                return true;
            }
        }

        bool ok = execute_select(parsed.select, out, err);
        if (ok && !out.rows.empty()) {
            // Populate result cache on miss
            std::size_t gen = write_gen_.load(std::memory_order_relaxed);
            if (cache_gen_ == gen) {
                cache_.put(sql, out);
            }
        }
        return ok;
    }

    // INSERT path: global reader lock + table-level writer lock for concurrency
    // Allows concurrent inserts to different tables.
    if (parsed.type == QueryType::Insert) {
        {
            ScopedReaderLock rlock(lock_);
            bool ok = execute_insert(parsed.insert, err);
            if (!ok) return false;
        }
        if (!is_replaying_ && wal_enabled_ && wal_) {
            wal_->append(std::move(sql));
        }
        return true;
    }

    // CREATE/DROP: global exclusive lock (modifies tables_ map)
    ScopedWriterLock wlock(lock_);

    // Lazily clear cache on first write after reads
    if (cache_gen_ != write_gen_.load(std::memory_order_relaxed)) {
        cache_.clear();
        cache_gen_ = write_gen_.load(std::memory_order_relaxed);
    }

    bool ok = false;
    switch (parsed.type) {
        case QueryType::DropTable:  ok = execute_drop(parsed.drop_table, err); break;
        case QueryType::CreateTable: ok = execute_create(parsed.create_table, err); break;
        default: err = "Unsupported SQL"; return false;
    }

    // WAL: persist mutating operations (unless replaying)
    if (ok && !is_replaying_ && wal_enabled_ && wal_) {
        wal_->append(std::move(sql));
    }
    return ok;
}

/**
 * Background reaper thread: wakes every 30 seconds to scan for expired rows.
 * Acquires global reader lock (for table map access), then per-table writer locks
 * to perform cleanup. Only calls cleanup_expired_locked() when expired rows are found.
 */
void DatabaseEngine::reaper_loop() {
    while (true) {
        std::unique_lock<std::mutex> lk(reaper_mutex_);
        // Wake every 30 seconds or on shutdown signal
        reaper_cv_.wait_for(lk, std::chrono::seconds(30), [this]() {
            return reaper_stop_.load();
        });

        if (reaper_stop_) {
            break;
        }
        lk.unlock();

        // Acquire global reader lock for map access, then per-table writer locks
        ScopedReaderLock rlock(lock_);
        auto now = std::chrono::system_clock::now();
        for (auto& [tname, table] : tables_) {
            ScopedWriterLock tlock(*table.table_lock);
            bool has_expired = false;
            for (const auto& row : table.rows) {
                if (row.expires_at <= now) {
                    has_expired = true;
                    break;
                }
            }
            if (has_expired) {
                cleanup_expired_locked(table);
            }
        }
    }
}

}
