/**
 * @file types.hpp
 * @brief Core data types for the FlexQL in-memory database engine.
 *
 * Defines all shared structures used across the engine: column types,
 * table schema, row storage, primary/secondary indexes, parsed query
 * representations, and a custom open-addressing hash map (PrimaryIndex)
 * optimized for both integer and string primary keys.
 */

#ifndef FLEXQL_COMMON_TYPES_HPP
#define FLEXQL_COMMON_TYPES_HPP

#include <chrono>
#include <climits>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <windows.h>
#else
#include <shared_mutex>
#endif

namespace flexql {

/**
 * FNV-1a hash functor — significantly faster than std::hash<string> for short keys.
 * Used as the hasher for secondary column indexes (unordered_multimap).
 */
struct FnvHash {
    std::size_t operator()(const std::string& s) const noexcept {
        std::size_t h = 14695981039346656037ULL;  // FNV offset basis (64-bit)
        for (char c : s) {
            h ^= static_cast<unsigned char>(c);
            h *= 1099511628211ULL;                // FNV prime (64-bit)
        }
        return h;
    }
};

/** Supported SQL column data types (used for type validation on INSERT). */
enum class ColumnType {
    Int,       /// Integer values (e.g. INT, INTEGER)
    Decimal,   /// Floating-point values (e.g. DECIMAL, FLOAT, DOUBLE)
    Varchar,   /// Variable-length strings (e.g. VARCHAR, TEXT, STRING)
    DateTime   /// Date/time values (e.g. DATETIME, TIMESTAMP)
};

/** Column definition: name, pre-computed uppercase name (for case-insensitive lookup), and type. */
struct ColumnDef {
    std::string name;        /// Original column name as declared in CREATE TABLE
    std::string upper_name;  /// Pre-computed uppercase name for fast case-insensitive matching
    ColumnType type;         /// Data type used for validation and comparison dispatch
};

/** A single row in a table: column values stored as strings, plus TTL expiry timestamp. */
struct Row {
    std::vector<std::string> values;                       /// Column values (index matches ColumnDef order)
    std::chrono::system_clock::time_point expires_at;      /// Expiration time (TTL-based row eviction)
};

/**
 * Open-addressing hash map for primary key index.
 *
 * Uses linear probing with power-of-two capacity and 75% load factor.
 * Supports two internal paths:
 *   - Integer path: int64 keys stored in IntSlot array for cache locality (1 cache miss per probe).
 *   - String path: separate ctrl/key/value arrays with FNV-1a hashing.
 *
 * The integer path avoids all string hashing and allocation for numeric PKs,
 * which is the common case for auto-increment IDs.
 */
class PrimaryIndex {
public:
    struct Entry {
        std::size_t value = 0;
    };

    void set_numeric(bool v) { is_numeric_ = v; }

    void reserve(std::size_t n) {
        std::size_t needed = n * 4 / 3 + 16;
        std::size_t new_cap = 16;
        while (new_cap < needed) new_cap *= 2;
        if (new_cap <= cap_) return;
        rehash(new_cap);
    }

    void clear() {
        if (is_numeric_) {
            for (auto& s : int_slots_) s.key = EMPTY_KEY;
        } else {
            std::fill(str_ctrl_.begin(), str_ctrl_.end(), uint8_t(0));
            for (auto& s : str_keys_) s.clear();
        }
        size_ = 0;
    }

    Entry* find(const std::string& key) {
        if (size_ == 0) return nullptr;
        if (is_numeric_) {
            if (key.empty()) return nullptr;
            return find_int(s_to_i(key.c_str()));
        }
        return find_str(key);
    }

    // Zero-allocation find: parse int directly from raw pointer range
    Entry* find_raw(const char* data, std::size_t len) {
        if (size_ == 0 || len == 0) return nullptr;
        if (is_numeric_) {
            return find_int(s_to_i(data));
        }
        // Fallback: construct string for non-numeric keys
        std::string key(data, len);
        return find_str(key);
    }

    // Returns {entry_ptr, was_inserted}
    std::pair<Entry*, bool> emplace(const std::string& key, std::size_t value) {
        maybe_grow();
        if (is_numeric_) return emplace_int(s_to_i(key.c_str()), value);
        return emplace_str(key, value);
    }

    // Fast path: emplace with pre-parsed int64 key (avoids s_to_i in hot loop)
    std::pair<Entry*, bool> emplace_int_direct(int64_t key, std::size_t value) {
        maybe_grow();
        return emplace_int(key, value);
    }

    // Prefetch the hash slot for an upcoming int key insert (hides memory latency)
    void prefetch_int(int64_t k) const noexcept {
        if (cap_ == 0) return;
        std::size_t h = hash_int(k) & mask_;
        __builtin_prefetch(&int_slots_[h], 1, 1);
    }

    void set(const std::string& key, std::size_t value) {
        auto [e, inserted] = emplace(key, value);
        if (!inserted) e->value = value;
    }

    std::size_t size() const { return size_; }
    std::size_t capacity() const { return cap_; }

private:
    static constexpr uint8_t EMPTY = 0;
    static constexpr uint8_t OCCUPIED = 1;
    static constexpr int64_t EMPTY_KEY = INT64_MIN; // sentinel for int path

    bool is_numeric_ = false;
    std::size_t size_ = 0;
    std::size_t cap_ = 0;
    std::size_t mask_ = 0;

    // Int path: single array for cache locality (1 cache miss per probe)
    struct IntSlot {
        int64_t key;
        std::size_t value;
    };
    std::vector<IntSlot> int_slots_;

    // String path (varchar PKs)
    std::vector<std::string> str_keys_;
    std::vector<Entry>       str_vals_;
    std::vector<uint8_t>     str_ctrl_;

    static int64_t s_to_i(const char* s) noexcept {
        int64_t r = 0;
        bool neg = false;
        if (*s == '-') { neg = true; ++s; }
        else if (*s == '+') ++s;
        while (*s >= '0' && *s <= '9') { r = r * 10 + (*s - '0'); ++s; }
        return neg ? -r : r;
    }

    static std::size_t hash_int(int64_t k) noexcept {
        auto u = static_cast<uint64_t>(k);
        u = (u ^ (u >> 30)) * 0xbf58476d1ce4e5b9ULL;
        u = (u ^ (u >> 27)) * 0x94d049bb133111ebULL;
        return u ^ (u >> 31);
    }

    static std::size_t hash_str(const std::string& s) noexcept {
        std::size_t h = 14695981039346656037ULL;
        for (char c : s) { h ^= static_cast<unsigned char>(c); h *= 1099511628211ULL; }
        return h;
    }

    Entry* find_int(int64_t k) {
        std::size_t h = hash_int(k) & mask_;
        while (int_slots_[h].key != EMPTY_KEY) {
            if (int_slots_[h].key == k)
                return reinterpret_cast<Entry*>(&int_slots_[h].value);
            h = (h + 1) & mask_;
        }
        return nullptr;
    }

    Entry* find_str(const std::string& k) {
        std::size_t h = hash_str(k) & mask_;
        while (str_ctrl_[h]) {
            if (str_keys_[h] == k) return &str_vals_[h];
            h = (h + 1) & mask_;
        }
        return nullptr;
    }

    std::pair<Entry*, bool> emplace_int(int64_t k, std::size_t v) {
        std::size_t h = hash_int(k) & mask_;
        while (int_slots_[h].key != EMPTY_KEY) {
            if (int_slots_[h].key == k)
                return {reinterpret_cast<Entry*>(&int_slots_[h].value), false};
            h = (h + 1) & mask_;
        }
        int_slots_[h].key = k;
        int_slots_[h].value = v;
        ++size_;
        return {reinterpret_cast<Entry*>(&int_slots_[h].value), true};
    }

    std::pair<Entry*, bool> emplace_str(const std::string& k, std::size_t v) {
        std::size_t h = hash_str(k) & mask_;
        while (str_ctrl_[h]) {
            if (str_keys_[h] == k) return {&str_vals_[h], false};
            h = (h + 1) & mask_;
        }
        str_ctrl_[h] = OCCUPIED;
        str_keys_[h] = k;
        str_vals_[h].value = v;
        ++size_;
        return {&str_vals_[h], true};
    }

    void maybe_grow() {
        if (size_ * 4 >= cap_ * 3) rehash(cap_ == 0 ? 16 : cap_ * 2);
    }

    void rehash(std::size_t new_cap) {
        std::size_t old_cap = cap_;
        cap_ = new_cap;
        mask_ = new_cap - 1;
        size_ = 0;

        if (is_numeric_) {
            auto old = std::move(int_slots_);
            int_slots_.assign(new_cap, {EMPTY_KEY, 0});
            for (std::size_t i = 0; i < old_cap; ++i) {
                if (old[i].key != EMPTY_KEY) emplace_int(old[i].key, old[i].value);
            }
        } else {
            auto ok = std::move(str_keys_);
            auto ov = std::move(str_vals_);
            auto oc = std::move(str_ctrl_);
            str_keys_.resize(new_cap);
            str_vals_.resize(new_cap);
            str_ctrl_.assign(new_cap, EMPTY);
            for (std::size_t i = 0; i < old_cap; ++i) {
                if (oc[i]) {
                    std::size_t h = hash_str(ok[i]) & mask_;
                    while (str_ctrl_[h]) h = (h + 1) & mask_;
                    str_ctrl_[h] = OCCUPIED;
                    str_keys_[h] = std::move(ok[i]);
                    str_vals_[h] = ov[i];
                    ++size_;
                }
            }
        }
    }
};

/**
 * In-memory table: schema + row storage + indexes + per-table lock.
 *
 * Each table has:
 *   - A primary index (open-addressing hash map) for O(1) PK lookups.
 *   - Lazy secondary indexes (hash multimaps) per column, built on first WHERE query.
 *   - A per-table reader-writer lock enabling concurrent reads and serialized writes
 *     without blocking other tables.
 */
struct Table {
    std::string name;                   /// Original table name as declared
    std::string upper_name;             /// Pre-computed uppercase for case-insensitive matching
    std::vector<ColumnDef> columns;     /// Column schema (first column is the primary key)
    std::vector<Row> rows;              /// Row storage (indexed by row position)
    PrimaryIndex primary_index;         /// O(1) primary key → row index lookup

    /// Secondary hash indexes: one per column for O(1) equality lookups.
    /// Lazy: only built when a SELECT with WHERE on that column is first seen.
    std::vector<std::unordered_multimap<std::string, std::size_t, FnvHash>> col_indexes;
    std::vector<bool> col_indexed;           /// Tracks which columns have a built index
    bool has_secondary_indexes = false;      /// Fast skip flag for INSERT hot path

    /// Per-table reader-writer lock (unique_ptr so Table remains movable).
    /// Uses SRWLOCK on Windows, std::shared_mutex on Linux.
#ifdef _WIN32
    std::unique_ptr<SRWLOCK> table_lock;
#else
    std::unique_ptr<std::shared_mutex> table_lock;
#endif
};

/** Result of a SQL query: column names (header) + rows of string values. */
struct QueryResult {
    std::vector<std::string> column_names;              /// Column headers for SELECT results
    std::vector<std::vector<std::string>> rows;         /// Result rows; each row is a vector of column values
};

/** Parsed WHERE clause: <column> <op> <value> where op is =, <, >, <=, or >=. */
struct WhereClause {
    std::string left;       /// Column name on the left side of the condition
    std::string op = "=";   /// Comparison operator
    std::string value;      /// Literal value on the right side
};

/** Parsed INNER JOIN clause: left_table.left_column <op> right_table.right_column. */
struct JoinClause {
    std::string left_table;     /// Table on the left side of the join
    std::string right_table;    /// Table on the right side of the join
    std::string left_column;    /// Join column from the left table
    std::string op = "=";       /// Join comparison operator (=, <, >, etc.)
    std::string right_column;   /// Join column from the right table
};

/** Parsed SELECT statement with optional column projection, JOIN, and WHERE. */
struct SelectQuery {
    bool select_all = false;                /// True if SELECT * was used
    std::vector<std::string> columns;       /// Explicit column list (empty if select_all)
    std::string from_table;                 /// Primary table name in FROM clause
    bool has_join = false;                  /// True if INNER JOIN is present
    JoinClause join;                        /// JOIN details (only valid if has_join)
    bool has_where = false;                 /// True if WHERE clause is present
    WhereClause where;                      /// WHERE details (only valid if has_where)
};

/** Parsed CREATE TABLE statement: table name and list of column definitions. */
struct CreateTableQuery {
    std::string table_name;
    std::vector<ColumnDef> columns;
};

/** Parsed INSERT statement: table name, value tuples, and optional TTL. */
struct InsertQuery {
    std::string table_name;
    std::vector<std::vector<std::string>> values_list;   /// Each inner vector is one row of values
    bool has_ttl = false;                                /// True if EXPIRES IN was specified
    int ttl_seconds = 0;                                 /// Custom TTL (overrides default 3600s)
};

/** Parsed DROP TABLE statement with optional IF EXISTS flag. */
struct DropTableQuery {
    std::string table_name;
    bool if_exists = false;     /// If true, suppress error when table doesn't exist
};

/** Discriminant for the type of parsed SQL statement. */
enum class QueryType {
    DropTable,
    CreateTable,
    Insert,
    Select,
    Unknown     /// Parsing failed or unsupported SQL
};

/** Tagged union of all parsed SQL query types, produced by SqlParser::parse(). */
struct ParsedQuery {
    QueryType type = QueryType::Unknown;
    DropTableQuery drop_table;
    CreateTableQuery create_table;
    InsertQuery insert;
    SelectQuery select;
};

}  // namespace flexql

#endif
