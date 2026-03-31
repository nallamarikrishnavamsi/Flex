# FlexQL

A simplified SQL-like client-server database driver in C++.

## Features

- Multithreaded server for concurrent clients
- SQL-like command support:
  - `CREATE TABLE`
  - `INSERT INTO ... VALUES (...)`
  - `SELECT * FROM ...`
  - `SELECT col1, col2 FROM ...`
  - `WHERE column = value` (single condition)
  - `INNER JOIN ... ON ... = ...`
- Primary index on first column (used as primary key)
- Row expiration timestamp with default TTL (3600s)
- LRU query cache for repeated SELECT queries
- C-compatible client API (`flexql_open`, `flexql_exec`, `flexql_close`, `flexql_free`)

## Build

```powershell
cd flexql
cmake -S . -B build
cmake --build build --config Release
```

## Run

Terminal 1:

```powershell
.\build\Release\flexql-server.exe 9000
```

Terminal 2:

```powershell
.\build\Release\flexql-client.exe 127.0.0.1 9000
```

## Example

```sql
CREATE TABLE STUDENT(ID INT, NAME VARCHAR);
INSERT INTO STUDENT VALUES (1, 'Alice');
INSERT INTO STUDENT VALUES (2, 'Bob');
SELECT * FROM STUDENT;
SELECT NAME FROM STUDENT WHERE ID = 1;
```

## Smoke Test

Start server, then run:

```powershell
.\build\Release\flexql-smoke.exe 127.0.0.1 9000
```
