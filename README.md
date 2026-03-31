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

## Prerequisites

- **MSYS2/UCRT64** — Install from [msys2.org](https://www.msys2.org/)
- **GCC 15+** with C++20 support — Inside MSYS2, run: `pacman -S mingw-w64-ucrt-x86_64-gcc`
- **Windows 10/11**

## Environment Setup

Every new PowerShell terminal needs the MSYS2 toolchain in PATH:

```powershell
$env:PATH = "C:\msys64\ucrt64\bin;$env:PATH"
```

> **Note:** `build.bat` sets this automatically for compilation, but you still need it when **running** the executables.

## Build

```bash
cmd /c build.bat
```

This compiles `flexql-server.exe`, `benchmark_flexql.exe`, and `multiclient_bench.exe`.

## Run

The server and client must run in **separate terminals**. Set the PATH in each terminal first.

**Terminal 1 — Start the server:**
```powershell
$env:PATH = "C:\msys64\ucrt64\bin;$env:PATH"
.\flexql-server.exe --clean
```
Keep this terminal open — the server stays running.

**Terminal 2 — Run tests or benchmarks:**
```powershell
$env:PATH = "C:\msys64\ucrt64\bin;$env:PATH"

# Run unit tests (21/21)
.\benchmark_flexql.exe --unit-test

# Single-client benchmark (1 thread, 10M rows)
.\benchmark_flexql.exe 10000000

# Multi-client benchmark — all thread/mode combinations (matches performance table)
# 1 Thread
.\multiclient_bench.exe --threads 1 --rows 10000000 --mode write
.\multiclient_bench.exe --threads 1 --rows 10000000 --mode read
.\multiclient_bench.exe --threads 1 --rows 10000000 --mode mixed

# 4 Threads
.\multiclient_bench.exe --threads 4 --rows 2500000 --mode write
.\multiclient_bench.exe --threads 4 --rows 2500000 --mode read
.\multiclient_bench.exe --threads 4 --rows 2500000 --mode mixed

# 8 Threads
.\multiclient_bench.exe --threads 8 --rows 1250000 --mode write
.\multiclient_bench.exe --threads 8 --rows 1250000 --mode read
.\multiclient_bench.exe --threads 8 --rows 1250000 --mode mixed
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
benchmark/
├── benchmark_flexql.cpp   # Single-client benchmark
├── multiclient_bench.cpp  # Multi-client benchmark
└── benchmark_results.txt  # Performance results
build.bat                  # Build script (GCC)
```
