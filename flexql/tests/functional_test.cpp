#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#ifdef _WIN32
#include <windows.h>
#endif

#include "flexql.h"

struct QueryCapture {
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
};

static int capture_cb(void* data, int column_count, char** values, char** names) {
    QueryCapture* cap = static_cast<QueryCapture*>(data);
    if (cap->columns.empty()) {
        for (int i = 0; i < column_count; ++i) {
            cap->columns.push_back(names[i] ? names[i] : "");
        }
    }
    std::vector<std::string> row;
    for (int i = 0; i < column_count; ++i) {
        row.push_back(values[i] ? values[i] : "NULL");
    }
    cap->rows.push_back(row);
    return 0;
}

static int abort_after_first_cb(void* data, int column_count, char** values, char** names) {
    (void)column_count;
    (void)values;
    (void)names;
    int* count = static_cast<int*>(data);
    ++(*count);
    return 1;
}

static bool exec_ok(FlexQL* db, const std::string& sql) {
    char* err = nullptr;
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &err);
    if (rc != FLEXQL_OK) {
        std::cerr << "FAIL SQL: " << sql << " -> " << (err ? err : "unknown") << "\n";
        if (err) {
            flexql_free(err);
        }
        return false;
    }
    return true;
}

static bool exec_fail(FlexQL* db, const std::string& sql) {
    char* err = nullptr;
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &err);
    if (rc == FLEXQL_OK) {
        std::cerr << "FAIL expected error for SQL: " << sql << "\n";
        return false;
    }
    if (err) {
        flexql_free(err);
    }
    return true;
}

static bool select_rows(FlexQL* db, const std::string& sql, QueryCapture& cap) {
    cap = QueryCapture{};
    char* err = nullptr;
    int rc = flexql_exec(db, sql.c_str(), capture_cb, &cap, &err);
    if (rc != FLEXQL_OK) {
        std::cerr << "FAIL SELECT: " << sql << " -> " << (err ? err : "unknown") << "\n";
        if (err) {
            flexql_free(err);
        }
        return false;
    }
    return true;
}

#ifdef _WIN32
struct ThreadArgs {
    const char* host;
    int port;
    int idx;
    bool ok;
};

DWORD WINAPI client_thread_proc(LPVOID p) {
    ThreadArgs* a = static_cast<ThreadArgs*>(p);
    a->ok = false;

    FlexQL* db = nullptr;
    if (flexql_open(a->host, a->port, &db) != FLEXQL_OK) {
        return 0;
    }

    std::string table = "CMT" + std::to_string(a->idx);
    std::string create_sql = "CREATE TABLE " + table + "(ID INT, NAME VARCHAR);";
    std::string insert_sql = "INSERT INTO " + table + " VALUES (" + std::to_string(a->idx) + ", 'ThreadUser');";
    std::string select_sql = "SELECT * FROM " + table + ";";

    QueryCapture cap;
    bool ok = exec_ok(db, create_sql) && exec_ok(db, insert_sql) && select_rows(db, select_sql, cap) && cap.rows.size() == 1;

    flexql_close(db);
    a->ok = ok;
    return 0;
}
#endif

int main(int argc, char** argv) {
    const char* host = argc > 1 ? argv[1] : "127.0.0.1";
    int port = argc > 2 ? std::atoi(argv[2]) : 9000;

    FlexQL* db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        std::cerr << "FAIL: flexql_open\n";
        return 1;
    }

    bool ok = true;

    ok = ok && exec_ok(db, "CREATE TABLE STUDENT(ID INT, NAME VARCHAR, GPA DECIMAL, CREATED DATETIME);");
    ok = ok && exec_ok(db, "INSERT INTO STUDENT VALUES (1, 'Alice', 9.5, '2026-03-17T10:00:00');");
    ok = ok && exec_ok(db, "INSERT INTO STUDENT VALUES (2, 'Bob', 8.1, '2026-03-17T10:01:00');");

    QueryCapture cap;
    ok = ok && select_rows(db, "SELECT * FROM STUDENT;", cap) && cap.rows.size() == 2;

    QueryCapture proj;
    ok = ok && select_rows(db, "SELECT NAME, GPA FROM STUDENT;", proj) && proj.rows.size() == 2 && proj.columns.size() == 2;

    QueryCapture where_cap;
    ok = ok && select_rows(db, "SELECT NAME FROM STUDENT WHERE ID = 1;", where_cap) && where_cap.rows.size() == 1;

    ok = ok && exec_fail(db, "SELECT * FROM STUDENT WHERE ID = 1 AND NAME = 'Alice';");

    ok = ok && exec_ok(db, "CREATE TABLE ENROLL(SID INT, COURSE VARCHAR);");
    ok = ok && exec_ok(db, "INSERT INTO ENROLL VALUES (1, 'DBMS');");
    ok = ok && exec_ok(db, "INSERT INTO ENROLL VALUES (2, 'OS');");

    QueryCapture join_cap;
    ok = ok && select_rows(db, "SELECT * FROM STUDENT INNER JOIN ENROLL ON STUDENT.ID = ENROLL.SID;", join_cap) && join_cap.rows.size() == 2;

    QueryCapture join_where_cap;
    ok = ok && select_rows(db, "SELECT STUDENT.NAME, ENROLL.COURSE FROM STUDENT INNER JOIN ENROLL ON STUDENT.ID = ENROLL.SID WHERE ENROLL.COURSE = 'DBMS';", join_where_cap) && join_where_cap.rows.size() == 1;

    ok = ok && exec_ok(db, "CREATE TABLE TTLT(ID INT, NAME VARCHAR);");
    ok = ok && exec_ok(db, "INSERT INTO TTLT VALUES (1, 'Temp') EXPIRES IN 1;");
#ifdef _WIN32
    Sleep(1500);
#else
    // Not used in this workspace.
#endif
    QueryCapture ttl_cap;
    ok = ok && select_rows(db, "SELECT * FROM TTLT;", ttl_cap) && ttl_cap.rows.empty();

    int callback_count = 0;
    char* err = nullptr;
    int rc = flexql_exec(db, "SELECT * FROM STUDENT;", abort_after_first_cb, &callback_count, &err);
    if (rc != FLEXQL_OK) {
        std::cerr << "FAIL callback abort query execution\n";
        ok = false;
    }
    if (err) {
        flexql_free(err);
    }
    if (callback_count != 1) {
        std::cerr << "FAIL callback abort expected 1 row callback, got " << callback_count << "\n";
        ok = false;
    }

    flexql_close(db);

#ifdef _WIN32
    ThreadArgs a1{host, port, 11, false};
    ThreadArgs a2{host, port, 12, false};
    HANDLE t1 = CreateThread(nullptr, 0, client_thread_proc, &a1, 0, nullptr);
    HANDLE t2 = CreateThread(nullptr, 0, client_thread_proc, &a2, 0, nullptr);
    if (t1 == nullptr || t2 == nullptr) {
        std::cerr << "FAIL creating concurrency test threads\n";
        ok = false;
    } else {
        HANDLE arr[2] = {t1, t2};
        WaitForMultipleObjects(2, arr, TRUE, INFINITE);
        CloseHandle(t1);
        CloseHandle(t2);
        if (!a1.ok || !a2.ok) {
            std::cerr << "FAIL multiclient concurrency test\n";
            ok = false;
        }
    }
#endif

    if (!ok) {
        std::cerr << "FUNCTIONAL TESTS FAILED\n";
        return 1;
    }

    std::cout << "ALL FUNCTIONAL TESTS PASSED\n";
    return 0;
}
