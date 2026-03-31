# FlexQL

A high-performance SQL-like client-server database engine implemented in C++20.

## Features

- **Multithreaded TCP server** with per-table reader-writer locks (SRWLOCK)
- **SQL subset**: CREATE TABLE, INSERT, SELECT, WHERE, INNER JOIN
- **Custom open-addressing hash index** with splitmix64 for O(1) primary key lookups
- **LRU query cache** with generation-counter lazy invalidation
- **Row expiration** (TTL) with hybrid lazy + background reaper
- **Write-Ahead Log (WAL)** for crash recovery with double-buffer design
- **C-compatible API**: `flexql_open`, `flexql_exec`, `flexql_close`, `flexql_free`

## Performance (10M Rows, Intel i5-1240P)

| Threads | Write | Read | Mixed |
|---------|-------|------|-------|
| 1 | 650K ops/sec | 1.97M ops/sec | 1.45M ops/sec |
| 4 | 1.56M ops/sec | 2.79M ops/sec | 2.97M ops/sec |
| 8 | 2.41M ops/sec | 3.05M ops/sec | 3.49M ops/sec |

## Build

Requires GCC 15+ with C++20 support (MSYS2/UCRT64 on Windows).

```bash
cmd /c build.bat
```

This compiles `flexql-server.exe`, `benchmark_flexql.exe`, and `multiclient_bench.exe`.

## Run

```bash
# Start server
./flexql-server.exe --clean

# Run unit tests (21/21)
./benchmark_flexql.exe --unit-test

# Run 10M row benchmark
./benchmark_flexql.exe 10000000

# Multi-client benchmark (8 threads)
./multiclient_bench.exe --threads 8 --rows 1250000 --mode write

# Interactive REPL client
./flexql-client.exe 127.0.0.1 9000
```

## Example

```sql
CREATE TABLE STUDENT(ID INT, NAME VARCHAR);
INSERT INTO STUDENT VALUES (1, 'Alice');
INSERT INTO STUDENT VALUES (2, 'Bob');
SELECT * FROM STUDENT;
SELECT NAME FROM STUDENT WHERE ID = 1;
```

## Project Structure

```
flexql/
├── include/          # Headers (flexql.h, cache, parser, storage, server)
├── src/              # Source (client, server, parser, storage, network)
├── tests/            # Smoke test and functional tests
├── DESIGN_DOCUMENT.md
└── README.md
benchmark_flexql.cpp  # Single-client benchmark
multiclient_bench.cpp # Multi-client benchmark
build.bat             # Build script (GCC)
benchmark_results.txt # Performance results
```
