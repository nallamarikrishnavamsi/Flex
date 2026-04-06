/**
 * FlexQL Multi-Client Stress Test
 *
 * Spawns N concurrent client threads to test:
 *   --mode write   : Write-heavy (all INSERT)
 *   --mode read    : Read-heavy  (INSERT seed + concurrent SELECTs)
 *   --mode mixed   : Mixed 50/50 INSERT + SELECT
 *
 * Usage:
 *   multiclient_bench.exe --threads 4 --rows 5000 --mode write
 */

#include "flexql.h"
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include <cstring>

using namespace std;
using namespace std::chrono;

// Fast integer-to-ASCII: writes digits directly into buffer, returns length.
static int write_int(char* dst, long long val) {
    if (val == 0) { dst[0] = '0'; return 1; }
    char tmp[20];
    int len = 0;
    bool neg = false;
    if (val < 0) { neg = true; val = -val; }
    while (val > 0) { tmp[len++] = '0' + (char)(val % 10); val /= 10; }
    int pos = 0;
    if (neg) dst[pos++] = '-';
    for (int i = len - 1; i >= 0; --i) dst[pos++] = tmp[i];
    return pos;
}

static const int BATCH_SIZE = 50000;

struct ThreadResult {
    long long rows;
    long long elapsed_ms;
};

// ─── Write-Heavy: each thread inserts its own partition ───
static ThreadResult do_write_work(int thread_id, int rows) {
    FlexQL* db = nullptr;
    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        cerr << "Thread " << thread_id << ": connect failed\n";
        return {0, 0};
    }

    auto start = high_resolution_clock::now();
    long long base = (long long)thread_id * 10000000LL;  // unique ID space per thread
    long long inserted = 0;

    // Create per-thread table for zero lock contention
    string tname = "BENCH_TABLE_" + to_string(thread_id);
    {
        char* errMsg = nullptr;
        string drop_sql = "DROP TABLE IF EXISTS " + tname + ";";
        flexql_exec(db, drop_sql.c_str(), nullptr, nullptr, &errMsg);
        if (errMsg) { flexql_free(errMsg); errMsg = nullptr; }
        string create_sql = "CREATE TABLE " + tname + "(ID DECIMAL, NAME VARCHAR(64), "
                            "EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);";
        flexql_exec(db, create_sql.c_str(), nullptr, nullptr, &errMsg);
        if (errMsg) { flexql_free(errMsg); errMsg = nullptr; }
    }

    string sql;
    sql.reserve(BATCH_SIZE * 80);
    char buf[256];

    while (inserted < rows) {
        sql.clear();
        sql.append("INSERT INTO ");
        sql.append(tname);
        sql.append(" VALUES ");
        int in_batch = 0;
        while (in_batch < BATCH_SIZE && inserted < rows) {
            long long id = base + inserted + 1;
            long long balance = 1000 + (id % 10000);
            if (in_batch > 0) sql += ',';
            
            // Fast serialization into buffer
            int pos = 0;
            buf[pos++] = '(';
            pos += write_int(buf + pos, id);
            memcpy(buf + pos, ", 'user", 7); pos += 7;
            pos += write_int(buf + pos, id);
            memcpy(buf + pos, "', 'user", 8); pos += 8;
            pos += write_int(buf + pos, id);
            memcpy(buf + pos, "@mail.com', ", 12); pos += 12;
            pos += write_int(buf + pos, balance);
            memcpy(buf + pos, ", 1893456000)", 13); pos += 13;

            sql.append(buf, pos);
            inserted++;
            in_batch++;
        }
        sql += ';';
        char* errMsg = nullptr;
        if (flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg) != FLEXQL_OK) {
            cerr << "Thread " << thread_id << ": INSERT failed: "
                 << (errMsg ? errMsg : "?") << "\n";
            if (errMsg) flexql_free(errMsg);
            break;
        }
    }

    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();
    flexql_close(db);
    return {inserted, elapsed};
}

// ─── Read-Heavy: each thread does repeated SELECT queries ───
static ThreadResult do_read_work(int thread_id, int queries) {
    FlexQL* db = nullptr;
    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        cerr << "Thread " << thread_id << ": connect failed\n";
        return {0, 0};
    }

    auto start = high_resolution_clock::now();
    long long completed = 0;
    char buf[256];
    constexpr int PIPE_BATCH = 250000;  // pipeline batch size

    for (int i = 0; i < queries; i += PIPE_BATCH) {
        int batch = min(PIPE_BATCH, queries - i);
        // Fire batch of PK SELECT queries without waiting
        for (int j = 0; j < batch; ++j) {
            int idx = i + j;
            // All PK lookups — avoids expensive range scans with huge result sets
            switch (idx % 4) {
                case 0:
                    snprintf(buf, sizeof(buf), "SELECT * FROM BENCH_TABLE WHERE ID = %d;", (idx % 10000) + 1);
                    break;
                case 1:
                    snprintf(buf, sizeof(buf), "SELECT NAME, BALANCE FROM BENCH_TABLE WHERE ID = %d;", (idx % 5000) + 1);
                    break;
                case 2:
                    snprintf(buf, sizeof(buf), "SELECT ID FROM BENCH_TABLE WHERE ID = %d;", (idx % 10000) + 1);
                    break;
                case 3:
                    snprintf(buf, sizeof(buf), "SELECT BALANCE FROM BENCH_TABLE WHERE ID = %d;", (idx % 7500) + 1);
                    break;
            }
            flexql_exec_fire(db, buf);
        }
        // Drain all responses
        if (flexql_drain(db, batch) == FLEXQL_OK) {
            completed += batch;
        }
    }

    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();
    flexql_close(db);
    return {completed, elapsed};
}

// ─── Mixed: batched INSERT + pipelined PK SELECT ───
static ThreadResult do_mixed_work(int thread_id, int ops) {
    FlexQL* db = nullptr;
    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        cerr << "Thread " << thread_id << ": connect failed\n";
        return {0, 0};
    }

    // Create per-thread table for mixed workload (avoids contention)
    string tname = "MIXED_TABLE_" + to_string(thread_id);
    {
        char* errMsg = nullptr;
        string drop_sql = "DROP TABLE IF EXISTS " + tname + ";";
        flexql_exec(db, drop_sql.c_str(), nullptr, nullptr, &errMsg);
        if (errMsg) { flexql_free(errMsg); errMsg = nullptr; }
        string create_sql = "CREATE TABLE " + tname + "(ID DECIMAL, NAME VARCHAR(64), "
                            "EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);";
        flexql_exec(db, create_sql.c_str(), nullptr, nullptr, &errMsg);
        if (errMsg) { flexql_free(errMsg); errMsg = nullptr; }
    }

    auto start = high_resolution_clock::now();
    long long base = (long long)thread_id * 100000000LL;
    long long completed = 0;
    // Batching: INSERT_BATCH rows per multi-value INSERT, then SELECT_BATCH PK lookups
    constexpr int INSERT_BATCH = 50000;  // rows per multi-value INSERT query
    constexpr int SELECT_BATCH = 50000;  // individual PK SELECT queries
    constexpr int OPS_PER_CYCLE = INSERT_BATCH + SELECT_BATCH; // 100K ops counted per cycle
    constexpr int PIPE_QUERIES = 1 + SELECT_BATCH;  // 1 INSERT query + 125K SELECTs to drain

    string insert_sql;
    insert_sql.reserve(INSERT_BATCH * 80 + 256);
    char buf[256];

    long long insert_id = base;

    for (long long done = 0; done < ops; done += OPS_PER_CYCLE) {
        int actual_inserts = min((long long)INSERT_BATCH, (long long)(ops - done) / 2);
        int actual_selects = min((long long)SELECT_BATCH, (long long)(ops - done) - actual_inserts);
        if (actual_inserts <= 0 && actual_selects <= 0) break;

        int queries_fired = 0;

        // Fire one multi-value INSERT with actual_inserts rows
        if (actual_inserts > 0) {
            insert_sql.clear();
            insert_sql += "INSERT INTO ";
            insert_sql += tname;
            insert_sql += " VALUES ";
            for (int j = 0; j < actual_inserts; ++j) {
                long long id = ++insert_id;
                long long balance = 1000 + (id % 10000);
                if (j > 0) insert_sql += ',';
                
                int pos = 0;
                buf[pos++] = '(';
                pos += write_int(buf + pos, id);
                memcpy(buf + pos, ", 'user", 7); pos += 7;
                pos += write_int(buf + pos, id);
                memcpy(buf + pos, "', 'user", 8); pos += 8;
                pos += write_int(buf + pos, id);
                memcpy(buf + pos, "@mail.com', ", 12); pos += 12;
                pos += write_int(buf + pos, balance);
                memcpy(buf + pos, ", 1893456000)", 13); pos += 13;

                insert_sql.append(buf, pos);
            }
            insert_sql += ';';
            flexql_exec_fire(db, insert_sql.c_str());
            queries_fired++;
        }

        // Fire PK SELECT queries
        for (int j = 0; j < actual_selects; ++j) {
            // Look up IDs we've previously inserted
            long long lookup_id = base + 1 + ((done / 2 + j) % max(1LL, insert_id - base));
            snprintf(buf, sizeof(buf), "SELECT * FROM %s WHERE ID = %lld;",
                tname.c_str(), lookup_id);
            flexql_exec_fire(db, buf);
            queries_fired++;
        }

        // Drain all responses
        if (flexql_drain(db, queries_fired) == FLEXQL_OK) {
            completed += actual_inserts + actual_selects;
        }
    }

    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();
    flexql_close(db);
    return {completed, elapsed};
}

int main(int argc, char** argv) {
    int num_threads = 4;
    int rows_per_thread = 5000;
    string mode = "write";

    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];
        if (arg == "--threads" && i + 1 < argc) num_threads = atoi(argv[++i]);
        else if (arg == "--rows" && i + 1 < argc) rows_per_thread = atoi(argv[++i]);
        else if (arg == "--mode" && i + 1 < argc) mode = argv[++i];
    }

    cout << "=== FlexQL Multi-Client Stress Test ===\n";
    cout << "Threads: " << num_threads << "\n";
    cout << "Rows/ops per thread: " << rows_per_thread << "\n";
    cout << "Mode: " << mode << "\n\n";

    // Create the shared table first
    {
        FlexQL* db = nullptr;
        if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
            cerr << "Cannot connect to server\n";
            return 1;
        }
        char* errMsg = nullptr;
        // Try to create table (may already exist)
        flexql_exec(db, "CREATE TABLE BENCH_TABLE(ID DECIMAL, NAME VARCHAR(64), "
                        "EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);",
                    nullptr, nullptr, &errMsg);
        if (errMsg) { flexql_free(errMsg); errMsg = nullptr; }

        // For read/mixed modes, seed some data
        if (mode == "read") {
            cout << "Seeding 10000 rows for read benchmark...\n";
            for (int b = 0; b < 10000; b += BATCH_SIZE) {
                stringstream ss;
                ss << "INSERT INTO BENCH_TABLE VALUES ";
                int end = min(b + BATCH_SIZE, 10000);
                for (int j = b; j < end; ++j) {
                    long long id = j + 1;
                    if (j > b) ss << ",";
                    ss << "(" << id << ", 'seed" << id << "', 'seed" << id
                       << "@mail.com', " << (1000 + id) << ", 1893456000)";
                }
                ss << ";";
                flexql_exec(db, ss.str().c_str(), nullptr, nullptr, &errMsg);
                if (errMsg) { flexql_free(errMsg); errMsg = nullptr; }
            }
        }
        flexql_close(db);
    }

    // Launch threads
    vector<thread> threads;
    vector<ThreadResult> results(num_threads);

    auto total_start = high_resolution_clock::now();

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            if (mode == "write") {
                results[i] = do_write_work(i, rows_per_thread);
            } else if (mode == "read") {
                results[i] = do_read_work(i, rows_per_thread);
            } else {
                results[i] = do_mixed_work(i, rows_per_thread);
            }
        });
    }

    for (auto& t : threads) t.join();

    auto total_end = high_resolution_clock::now();
    long long total_elapsed = duration_cast<milliseconds>(total_end - total_start).count();

    // Report results
    cout << "\n=== Results ===\n";
    long long total_rows = 0;
    for (int i = 0; i < num_threads; ++i) {
        long long tps = results[i].elapsed_ms > 0
                        ? (results[i].rows * 1000LL / results[i].elapsed_ms)
                        : results[i].rows;
        cout << "Thread " << i << ": " << results[i].rows << " ops in "
             << results[i].elapsed_ms << " ms (" << tps << " ops/sec)\n";
        total_rows += results[i].rows;
    }

    long long aggregate_tps = total_elapsed > 0 ? (total_rows * 1000LL / total_elapsed) : total_rows;
    cout << "\nTotal: " << total_rows << " ops in " << total_elapsed << " ms\n";
    cout << "Aggregate Throughput: " << aggregate_tps << " ops/sec\n";

    return 0;
}
