#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>

#include "flexql.h"

int count_rows(void* data, int, char**, char**) {
    int* count = static_cast<int*>(data);
    ++(*count);
    return 0;
}

int main(int argc, char** argv) {
    const char* host = argc > 1 ? argv[1] : "127.0.0.1";
    int port = argc > 2 ? std::atoi(argv[2]) : 9000;

    FlexQL* db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        std::cerr << "open failed\n";
        return 1;
    }

    char* err = nullptr;
    auto run = [&](const char* sql) -> bool {
        int rc = flexql_exec(db, sql, nullptr, nullptr, &err);
        if (rc != FLEXQL_OK) {
            std::cerr << "SQL failed: " << sql << " -> " << (err ? err : "unknown") << "\n";
            if (err) {
                flexql_free(err);
                err = nullptr;
            }
            return false;
        }
        return true;
    };

    if (!run("CREATE TABLE STUDENT(ID INT, NAME VARCHAR);")) return 1;
    if (!run("INSERT INTO STUDENT VALUES (1, 'Alice');")) return 1;
    if (!run("INSERT INTO STUDENT VALUES (2, 'Bob');")) return 1;

    int rows = 0;
    if (flexql_exec(db, "SELECT * FROM STUDENT;", count_rows, &rows, &err) != FLEXQL_OK) {
        std::cerr << "select failed: " << (err ? err : "unknown") << "\n";
        if (err) {
            flexql_free(err);
        }
        return 1;
    }

    if (rows != 2) {
        std::cerr << "unexpected row count: " << rows << "\n";
        return 1;
    }

    flexql_close(db);
    std::cout << "smoke ok\n";
    return 0;
}
