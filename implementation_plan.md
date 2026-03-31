# Advanced Performance Optimizations

Three changes to further improve throughput: secondary hash indexes for fast WHERE, table-level locking for multi-table concurrency, and lock contention reduction.

## Proposed Changes

### 1. Secondary Hash Indexes on WHERE Columns

The primary_index already provides O(1) lookup on column 0. We extend this to **any column** that is used with `WHERE col = value`.

#### [MODIFY] [types.hpp](file:///c:/Users/venun/OneDrive/Desktop/DL_Project/flexql/include/common/types.hpp)

- Add `std::unordered_map<std::string, std::vector<std::size_t>> secondary_indexes` to [Table](file:///c:/Users/venun/OneDrive/Desktop/DL_Project/flexql/include/common/types.hpp#29-36) — maps `column_index → {value → [row indices]}`
- Actually, use a simpler structure: `std::vector<std::unordered_multimap<std::string, std::size_t>> col_indexes` keyed by column index

#### [MODIFY] [database_engine.cpp](file:///c:/Users/venun/OneDrive/Desktop/DL_Project/flexql/src/storage/database_engine.cpp)

- In [execute_create](file:///c:/Users/venun/OneDrive/Desktop/DL_Project/flexql/src/storage/database_engine.cpp#331-352): initialize `col_indexes` with one empty map per column
- In [execute_insert](file:///c:/Users/venun/OneDrive/Desktop/DL_Project/flexql/src/storage/database_engine.cpp#353-396) / [try_fast_insert](file:///c:/Users/venun/OneDrive/Desktop/DL_Project/flexql/src/storage/database_engine.cpp#609-745): for each inserted row, insert into every column's hash index
- In [execute_select](file:///c:/Users/venun/OneDrive/Desktop/DL_Project/flexql/src/storage/database_engine.cpp#402-608): when WHERE has `op == "="`, check if `col_indexes[where_col_idx]` exists and use O(1) lookup instead of full table scan
- In [cleanup_expired_locked](file:///c:/Users/venun/OneDrive/Desktop/DL_Project/flexql/src/storage/database_engine.cpp#289-306): rebuild all column indexes after removing expired rows

---

### 2. Table-Level Reader-Writer Locks

Replace the single global `SRWLOCK lock_` with per-table locks so that operations on different tables don't block each other.

#### [MODIFY] [types.hpp](file:///c:/Users/venun/OneDrive/Desktop/DL_Project/flexql/include/common/types.hpp)

- Add a per-table lock member. Since [Table](file:///c:/Users/venun/OneDrive/Desktop/DL_Project/flexql/include/common/types.hpp#29-36) needs to be movable and `SRWLOCK`/`shared_mutex` are not movable, wrap the lock in a `std::unique_ptr`:
  ```cpp
  #ifdef _WIN32
  std::unique_ptr<SRWLOCK> lock;
  #else
  std::unique_ptr<std::shared_mutex> lock;
  #endif
  ```

#### [MODIFY] [database_engine.cpp](file:///c:/Users/venun/OneDrive/Desktop/DL_Project/flexql/src/storage/database_engine.cpp)

- Keep the global `lock_` for `tables_` map access (short critical section: lookup the table pointer, then release)
- For actual data operations (insert rows, scan rows), acquire the **table-level lock** instead
- SELECT acquires shared table lock → concurrent reads on the same table
- INSERT acquires exclusive table lock → serializes writes to the same table but allows concurrent writes to different tables

#### [MODIFY] [database_engine.hpp](file:///c:/Users/venun/OneDrive/Desktop/DL_Project/flexql/include/storage/database_engine.hpp)

- No structural change needed since locks live in [Table](file:///c:/Users/venun/OneDrive/Desktop/DL_Project/flexql/include/common/types.hpp#29-36)

---

### 3. Fast-Insert Lock Contention Fix

Currently [try_fast_insert](file:///c:/Users/venun/OneDrive/Desktop/DL_Project/flexql/src/storage/database_engine.cpp#609-745) acquires the global write lock early (line 633) and holds it for the entire parse + insert. With table-level locking, this becomes:
- Acquire global **reader** lock to find the table
- Acquire table **writer** lock to insert

This allows concurrent inserts to different tables.

## Verification Plan

### Automated Tests
1. [.\build.bat](file:///c:/Users/venun/OneDrive/Desktop/DL_Project/build.bat) — all 3 executables compile
2. `.\benchmark_flexql.exe --unit-test` — all 21/21 tests pass
3. `.\benchmark_flexql.exe 10000000` — 10M benchmark with 250k batch
4. `.\multiclient_bench.exe --threads 4 --rows 50000 --mode write` — multi-client stress test
