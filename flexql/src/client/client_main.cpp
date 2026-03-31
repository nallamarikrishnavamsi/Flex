/**
 * @file client_main.cpp
 * @brief Interactive REPL client for FlexQL (flexql-client executable).
 *
 * Connects to a FlexQL server via the C API and provides a readline-style
 * interactive prompt. Supports piped/redirected stdin for scripted usage.
 * Type ".exit" or send EOF (Ctrl+D / Ctrl+Z) to disconnect.
 */

#include <cstdlib>
#include <iostream>
#include <string>

#ifdef _WIN32
#include <io.h>
// _isatty(0) checks if stdin (fd 0) is a terminal; works on both MSVC and MinGW
static inline int flexql_isatty_stdin() { return _isatty(0); }
#else
#include <unistd.h>
static inline int flexql_isatty_stdin() { return isatty(fileno(stdin)); }
#endif

#include "flexql.h"

/// Callback function for flexql_exec: prints each result row as "col = value" pairs.
int print_row(void*, int column_count, char** values, char** names) {
    for (int i = 0; i < column_count; ++i) {
        std::cout << names[i] << " = " << (values[i] ? values[i] : "NULL") << "\n";
    }
    std::cout << "\n";
    return 0;
}

int main(int argc, char** argv) {
    std::ios_base::sync_with_stdio(false);
    std::cin.tie(NULL);

    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <host> <port>\n";
        return 1;
    }

    const char* host = argv[1];
    int port = std::atoi(argv[2]);

    FlexQL* db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        std::cerr << "Cannot connect to FlexQL server\n";
        return 1;
    }

    bool interactive = flexql_isatty_stdin() != 0;
    if (interactive) {
        std::cout << "Connected to FlexQL server\n";
    }

    std::string line;
    while (true) {
        if (interactive) {
            std::cout << "flexql> ";
            std::cout.flush();
        }
        if (!std::getline(std::cin, line)) {
            break;
        }

        if (line == ".exit") {
            break;
        }
        if (line.empty()) {
            continue;
        }

        char* err_msg = nullptr;
        int rc = flexql_exec(db, line.c_str(), print_row, nullptr, &err_msg);
        if (rc != FLEXQL_OK) {
            std::cerr << "SQL error: " << (err_msg ? err_msg : "unknown") << "\n";
            if (err_msg != nullptr) {
                flexql_free(err_msg);
            }
        } else if (interactive) {
            std::cout << "Query executed successfully\n";
        }
    }

    flexql_close(db);
    if (interactive) {
        std::cout << "Connection closed\n";
    }
    return 0;
}
