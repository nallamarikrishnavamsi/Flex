#!/usr/bin/env python3
"""
High-performance data generation + benchmarking utility for FlexQL.

Features:
- Dynamic CREATE TABLE generation
- Batched INSERT generation for millions of rows
- SELECT query generation for benchmark runs
- Output to .sql file and/or direct execution through flexql-client
- Throughput reporting (rows/sec)

Example:
  python3 scripts/flexql_benchmark_single.py \
      --rows 120000 --cols 100 --column-types INT \
      --varchar-length 10 --batch-size 64 \
      --table test --mode execute --host 127.0.0.1 --port 9000
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import random
import string
import subprocess
import sys
import time
from typing import Iterable, List, Sequence


SUPPORTED_TYPES = {"INT", "DECIMAL", "VARCHAR"}
ALPHABET = string.ascii_letters + string.digits


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="FlexQL data generator + benchmark")
    p.add_argument("--rows", type=int, required=True, help="Number of rows to generate")
    p.add_argument("--cols", type=int, required=True, help="Number of columns")
    p.add_argument(
        "--column-types",
        type=str,
        default="DECIMAL,VARCHAR",
        help="Comma-separated column type pattern (INT,DECIMAL,VARCHAR). Repeats if shorter than --cols",
    )
    p.add_argument("--varchar-length", type=int, default=10, help="Length of generated VARCHAR values")
    p.add_argument("--batch-size", type=int, default=1000, help="Rows per insert-write batch")
    p.add_argument("--table", type=str, default="test", help="Table name")
    p.add_argument(
        "--mode",
        choices=["file", "execute", "both"],
        default="execute",
        help="file: write SQL only, execute: run against flexql-client, both: do both",
    )
    p.add_argument("--sql-file", type=str, default="flexql_benchmark.sql", help="Output SQL file path")
    p.add_argument("--client-bin", type=str, default="auto", help="Path to flexql-client binary (default: auto-detect)")
    p.add_argument("--host", type=str, default="127.0.0.1", help="FlexQL server host")
    p.add_argument("--port", type=int, default=9000, help="FlexQL server port")
    p.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility")
    p.add_argument(
        "--decimal-mode",
        choices=["int", "float"],
        default="int",
        help="DECIMAL generation mode",
    )
    p.add_argument(
        "--select-runs",
        type=int,
        default=5,
        help="Number of SELECT queries to issue in benchmark",
    )
    p.add_argument(
        "--select-mode",
        choices=["point", "full", "mixed"],
        default="point",
        help="point: WHERE on first DECIMAL col, full: SELECT *, mixed: alternating",
    )
    return p.parse_args()


def normalize_types(cols: int, spec: str) -> List[str]:
    raw = [x.strip().upper() for x in spec.split(",") if x.strip()]
    if not raw:
        raise ValueError("--column-types cannot be empty")
    for t in raw:
        if t not in SUPPORTED_TYPES:
            raise ValueError(f"Unsupported type '{t}'. Supported: INT,DECIMAL,VARCHAR")
    out = [raw[i % len(raw)] for i in range(cols)]
    return out


def create_table_sql(table: str, col_types: Sequence[str]) -> str:
    cols = [f"col{i + 1} {typ}" for i, typ in enumerate(col_types)]
    return f"CREATE TABLE {table} ({', '.join(cols)});\n"


def random_varchar(rng: random.Random, length: int, alphabet: str = ALPHABET) -> str:
    # Faster than choices() for very large loops in CPython due to lower overhead.
    alen = len(alphabet)
    return "".join(alphabet[rng.randrange(alen)] for _ in range(length))


def build_insert_stmt(
    table: str,
    col_types: Sequence[str],
    rng: random.Random,
    varchar_length: int,
    row_id: int,
    decimal_mode: str,
) -> str:
    vals: List[str] = []
    for typ in col_types:
        if typ in ("INT", "DECIMAL"):
            if decimal_mode == "int":
                vals.append(str(row_id))
            else:
                vals.append(f"{rng.random() * 1_000_000:.6f}")
        else:  # VARCHAR
            s = random_varchar(rng, varchar_length)
            vals.append("'" + s + "'")
    return f"INSERT INTO {table} VALUES ({', '.join(vals)});\n"


def select_queries(
    table: str,
    col_types: Sequence[str],
    runs: int,
    mode: str,
    max_row_id: int,
    rng: random.Random,
) -> List[str]:
    first_decimal_idx = -1
    for i, t in enumerate(col_types):
        if t in ("INT", "DECIMAL"):
            first_decimal_idx = i + 1
            break

    queries: List[str] = []
    for i in range(runs):
        if mode == "full":
            queries.append(f"SELECT * FROM {table};\n")
        elif mode == "point":
            if first_decimal_idx > 0:
                probe = rng.randint(1, max_row_id)
                queries.append(f"SELECT * FROM {table} WHERE col{first_decimal_idx} = {probe};\n")
            else:
                queries.append(f"SELECT * FROM {table};\n")
        else:  # mixed
            if i % 2 == 0:
                queries.append(f"SELECT * FROM {table};\n")
            else:
                if first_decimal_idx > 0:
                    probe = rng.randint(1, max_row_id)
                    queries.append(f"SELECT * FROM {table} WHERE col{first_decimal_idx} = {probe};\n")
                else:
                    queries.append(f"SELECT * FROM {table};\n")
    return queries


def stream_sql_to_file(
    sql_path: str,
    table: str,
    rows: int,
    col_types: Sequence[str],
    batch_size: int,
    varchar_length: int,
    decimal_mode: str,
    rng: random.Random,
    select_sql: Sequence[str],
) -> float:
    os.makedirs(os.path.dirname(sql_path) or ".", exist_ok=True)
    t0 = time.perf_counter()

    with open(sql_path, "w", encoding="utf-8", buffering=1024 * 1024) as f:
        f.write(f"DROP TABLE IF EXISTS {table};\n")
        f.write(create_table_sql(table, col_types))

        row_id = 1
        while row_id <= rows:
            end = min(row_id + batch_size, rows + 1)
            block: List[str] = []
            append = block.append
            for rid in range(row_id, end):
                append(build_insert_stmt(table, col_types, rng, varchar_length, rid, decimal_mode))
            f.write("".join(block))
            row_id = end

        f.write("\n")
        for q in select_sql:
            f.write(q)

    return time.perf_counter() - t0


def _run_client_phase(
    client_bin: str,
    host: str,
    port: int,
    sql_chunks: Iterable[str],
) -> float:
    t0 = time.perf_counter()
    proc = subprocess.Popen(
        [client_bin, host, str(port)],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1024 * 1024,
    )

    assert proc.stdin is not None
    for chunk in sql_chunks:
        proc.stdin.write(chunk)
    proc.stdin.write(".exit\n")
    proc.stdin.close()

    stderr = proc.stderr.read() if proc.stderr is not None else ""
    rc = proc.wait()
    elapsed = time.perf_counter() - t0

    if rc != 0:
        raise RuntimeError(f"flexql-client failed (rc={rc}). stderr={stderr.strip()}")
    return elapsed


def execute_benchmark(
    client_bin: str,
    host: str,
    port: int,
    table: str,
    rows: int,
    col_types: Sequence[str],
    batch_size: int,
    varchar_length: int,
    decimal_mode: str,
    select_sql: Sequence[str],
    rng_seed: int,
) -> tuple[float, float]:
    # Phase 1: create + insert
    rng_insert = random.Random(rng_seed)

    def insert_chunks() -> Iterable[str]:
        yield f"DROP TABLE IF EXISTS {table};\n"
        yield create_table_sql(table, col_types)

        row_id = 1
        while row_id <= rows:
            end = min(row_id + batch_size, rows + 1)
            block: List[str] = []
            append = block.append
            for rid in range(row_id, end):
                append(build_insert_stmt(table, col_types, rng_insert, varchar_length, rid, decimal_mode))
            yield "".join(block)
            row_id = end

    insert_time = _run_client_phase(client_bin, host, port, insert_chunks())

    # Phase 2: select benchmark
    select_time = _run_client_phase(client_bin, host, port, select_sql)

    return insert_time, select_time


def resolve_client_bin(arg_value: str) -> str:
    if arg_value != "auto":
        return arg_value

    script_dir = Path(__file__).resolve().parent
    candidates = [
        Path("./bin/flexql-client.exe"),
        script_dir.parent / "bin" / "flexql-client.exe",
    ]

    for c in candidates:
        if c.exists():
            return str(c)

    # Fallback keeps existing behavior if binary is on PATH or user handles it externally.
    return "./bin/flexql-client.exe"


def main() -> int:
    args = parse_args()

    if args.rows <= 0:
        print("--rows must be > 0", file=sys.stderr)
        return 2
    if args.cols <= 0:
        print("--cols must be > 0", file=sys.stderr)
        return 2
    if args.batch_size <= 0:
        print("--batch-size must be > 0", file=sys.stderr)
        return 2
    if args.varchar_length <= 0:
        print("--varchar-length must be > 0", file=sys.stderr)
        return 2

    try:
        col_types = normalize_types(args.cols, args.column_types)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 2

    rng = random.Random(args.seed)
    selects = select_queries(
        table=args.table,
        col_types=col_types,
        runs=args.select_runs,
        mode=args.select_mode,
        max_row_id=args.rows,
        rng=rng,
    )

    file_gen_time = None
    if args.mode in ("file", "both"):
        file_gen_time = stream_sql_to_file(
            sql_path=args.sql_file,
            table=args.table,
            rows=args.rows,
            col_types=col_types,
            batch_size=args.batch_size,
            varchar_length=args.varchar_length,
            decimal_mode=args.decimal_mode,
            rng=random.Random(args.seed),
            select_sql=selects,
        )

    insert_time = None
    select_time = None
    if args.mode in ("execute", "both"):
        client_bin = resolve_client_bin(args.client_bin)
        insert_time, select_time = execute_benchmark(
            client_bin=client_bin,
            host=args.host,
            port=args.port,
            table=args.table,
            rows=args.rows,
            col_types=col_types,
            batch_size=args.batch_size,
            varchar_length=args.varchar_length,
            decimal_mode=args.decimal_mode,
            select_sql=selects,
            rng_seed=args.seed,
        )

    # Report
    print("=" * 62)
    print("FlexQL Benchmark Summary")
    print("=" * 62)
    print(f"table_name            : {args.table}")
    print(f"number_of_rows        : {args.rows}")
    print(f"number_of_columns     : {args.cols}")
    print(f"column_types          : {', '.join(col_types)}")
    print(f"batch_size            : {args.batch_size}")
    print(f"varchar_length        : {args.varchar_length}")
    print(f"mode                  : {args.mode}")
    if file_gen_time is not None:
        print(f"sql_file              : {args.sql_file}")
        print(f"sql_generation_time_s : {file_gen_time:.6f}")

    if insert_time is not None and select_time is not None:
        throughput = args.rows / insert_time if insert_time > 0 else float("inf")
        print(f"insertion_time_s      : {insert_time:.6f}")
        print(f"select_time_s         : {select_time:.6f}")
        print(f"throughput_rows_per_s : {throughput:.2f}")

    print("=" * 62)

    # Show examples requested by user
    print("Example generated SQL:")
    print(create_table_sql(args.table, col_types).strip())
    sample_stmt = build_insert_stmt(
        args.table,
        col_types,
        random.Random(args.seed),
        args.varchar_length,
        1,
        args.decimal_mode,
    ).strip()
    print(sample_stmt)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
