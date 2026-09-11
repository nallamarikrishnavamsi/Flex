/**
 * @file flexql_api.cpp
 * @brief Client-side implementation of the FlexQL C API.
 * 
 * This file acts as the "translator" between the user's C++ code and the server's binary/text protocols.
 * It implements a standard C-compatible interface that can be used by other languages.
 * 
 * Major features implemented here:
 * 1.  Connection Management (Open/Close)
 * 2.  SQL Execution via Callback (SQLite-style)
 * 3.  High-performance Pipelining (Fire & Drain) using a 2MB send buffer.
 */

#include "flexql.h"

#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "network/socket_utils.hpp"

/**
 * @brief Internal handle for a database connection.
 * 
 * This structure is hidden from the user (OPAQUE). It stores state needed
 * to keep the connection alive and handle high-speed buffering.
 */
struct FlexQL {
    socket_t fd = kInvalidSocket;                          /// The actual network socket (TCP)
    
    /**
     * @brief Client-side send buffer for "pipelining" (Fire API).
     * Instead of sending 1 query at a time, we collect up to 2MB of queries
     * and send them in a single batch to save network overhead.
     */
    char* fire_buf = nullptr;                              /// 2MB of memory for batching
    std::size_t fire_pos = 0;                              /// How much of the buffer is currently used
    static constexpr std::size_t FIRE_CAP = 2 * 1024 * 1024;  /// 2MB Capacity
};

namespace {

/**
 * @brief The Unit Separator (ASCII 31).
 * We use this invisible character instead of a comma because SQL data itself 
 * often contains commas (e.g., "Main St, Apartment 4"). The separator 0x1F
 * never appears in data, so it's safe to use as a divider.
 */
constexpr char kSep = 0x1F;

/**
 * @brief Splits a server response line by the Unit Separator.
 */
std::vector<std::string> split_sep(const std::string& line) {
    std::vector<std::string> out;
    std::string cur;
    for (char ch : line) {
        if (ch == kSep) {
            out.push_back(cur);
            cur.clear();
        } else {
            cur.push_back(ch);
        }
    }
    out.push_back(cur);
    return out;
}

/**
 * @brief Helper to copy a string into a C-style char pointer.
 * Users are responsible for calling flexql_free() on this later.
 */
char* alloc_cstr(const std::string& s) {
    char* p = static_cast<char*>(std::malloc(s.size() + 1));
    if (p == nullptr) {
        return nullptr;
    }
    std::memcpy(p, s.c_str(), s.size() + 1);
    return p;
}

}  // namespace

/**
 * All functions below are 'extern "C"', meaning they can be linked to from
 * languages like C, Python, Go, etc.
 */
extern "C" {

/**
 * @brief Opens a connection to a FlexQL server.
 */
int flexql_open(const char* host, int port, FlexQL** db) {
    if (host == nullptr || db == nullptr || port <= 0) {
        return FLEXQL_ERROR;
    }

    std::string err;
    // Initialize socket libraries (especially important on Windows)
    if (!flexql::init_sockets(err)) {
        return FLEXQL_ERROR;
    }

    // Attempt the TCP connection
    socket_t fd = flexql::connect_to(host, port, err);
    if (fd == kInvalidSocket) {
        flexql::cleanup_sockets();
        return FLEXQL_ERROR;
    }

    // Allocate the internal connection handle
    FlexQL* handle = new FlexQL();
    handle->fd = fd;
    handle->fire_buf = static_cast<char*>(std::malloc(FlexQL::FIRE_CAP));
    handle->fire_pos = 0;
    *db = handle;
    return FLEXQL_OK;
}

/**
 * @brief Closes the connection and cleans up all memory.
 */
int flexql_close(FlexQL* db) {
    if (db == nullptr || db->fd == kInvalidSocket) {
        return FLEXQL_ERROR;
    }
    // Flush any leftover batch data in the Fire buffer before we quit
    if (db->fire_buf && db->fire_pos > 0) {
        std::string err;
        flexql::send_bulk(db->fd, db->fire_buf, db->fire_pos, err);
        db->fire_pos = 0;
    }
    std::string err;
    // Tell the server we are leaving
    flexql::send_line(db->fd, ".exit", err);
    flexql::close_socket(db->fd);
    db->fd = kInvalidSocket;
    std::free(db->fire_buf);
    db->fire_buf = nullptr;
    delete db;
    // Global socket cleanup
    flexql::cleanup_sockets();
    return FLEXQL_OK;
}

/**
 * @brief Executes an SQL statement and calls a callback for every row found.
 */
int flexql_exec(
    FlexQL* db,
    const char* sql,
    int (*callback)(void*, int, char**, char**),
    void* arg,
    char** errmsg
) {
    if (errmsg != nullptr) {
        *errmsg = nullptr;
    }
    if (db == nullptr || sql == nullptr || db->fd == kInvalidSocket) {
        return FLEXQL_ERROR;
    }

    std::string err;
    std::size_t sql_len = std::strlen(sql);
    // Send the query line to the server
    if (!flexql::send_line(db->fd, sql, sql_len, err)) {
        if (errmsg != nullptr) {
            *errmsg = alloc_cstr("Network send failed");
        }
        return FLEXQL_ERROR;
    }

    std::vector<std::string> columns;
    std::string line;
    // Protocol Reading Loop: Keep reading lines until we see "END" or "ERR"
    while (true) {
        if (!flexql::recv_line(db->fd, line, err)) {
            if (errmsg != nullptr) {
                *errmsg = alloc_cstr("Network receive failed");
            }
            return FLEXQL_ERROR;
        }

        // END marks the successful completion of the query
        if (line.size() == 3 && line[0] == 'E' && line[1] == 'N' && line[2] == 'D') {
            break;
        }
        // OK means the server got it (usually for non-SELECT queries)
        if (line.size() == 2 && line[0] == 'O' && line[1] == 'K') {
            continue;
        }

        // Check if server returned an error
        if (line.size() >= 3 && line[0] == 'E' && line[1] == 'R' && line[2] == 'R') {
            if (errmsg != nullptr) {
                std::size_t sep_pos = line.find(kSep, 3);
                if (sep_pos != std::string::npos) {
                    *errmsg = alloc_cstr(line.substr(sep_pos + 1));
                } else {
                    *errmsg = alloc_cstr("Unknown server error");
                }
            }
            // Fast-forward until we reach END to clear the pipe
            while (!(line.size() == 3 && line[0] == 'E' && line[1] == 'N' && line[2] == 'D')) {
                if (!flexql::recv_line(db->fd, line, err)) {
                    break;
                }
            }
            return FLEXQL_ERROR;
        }

        // Handle Column Headers: "COLS[sep][num_cols][sep][col1][sep][col2]..."
        if (line.size() >= 4 && line[0] == 'C' && line[1] == 'O' && line[2] == 'L' && line[3] == 'S') {
            auto parts = split_sep(line);
            // Index 0 is "COLS", Index 1 is the count, Index 2+ are the names
            columns.assign(parts.begin() + 2, parts.end());
            continue;
        }

        // Handle Result Rows: "ROW[sep][val1][sep][val2]..."
        if (line.size() >= 3 && line[0] == 'R' && line[1] == 'O' && line[2] == 'W' && callback != nullptr) {
            auto parts = split_sep(line);
            std::vector<char*> values;
            std::vector<char*> names;

            // Prepare pointers for the user's callback function
            for (std::size_t i = 1; i < parts.size(); ++i) {
                values.push_back(const_cast<char*>(parts[i].c_str()));
            }
            for (auto& c : columns) {
                names.push_back(const_cast<char*>(c.c_str()));
            }

            // Call the user's function to process this row
            int rc = callback(arg, static_cast<int>(values.size()), values.data(), names.data());
            // If the user returns 1, it means "Stop sending me more rows"
            if (rc == 1) {
                while (!(line.size() == 3 && line[0] == 'E' && line[1] == 'N' && line[2] == 'D')) {
                    if (!flexql::recv_line(db->fd, line, err)) {
                        break;
                    }
                }
                break;
            }
        }
    }

    return FLEXQL_OK;
}

/**
 * @brief Free memory allocated for error messages.
 */
void flexql_free(void* ptr) {
    std::free(ptr);
}

/**
 * @brief Pushes an SQL statement into the 2MB Fire Buffer.
 * Does NOT send the data immediately unless the buffer becomes full.
 */
int flexql_exec_fire(FlexQL* db, const char* sql) {
    if (db == nullptr || sql == nullptr || db->fd == kInvalidSocket) {
        return FLEXQL_ERROR;
    }
    std::size_t sql_len = std::strlen(sql);
    std::size_t needed = sql_len + 1;  // +1 for the newline character
    
    // If adding this query would exceed our 2MB batch, send the current batch now
    if (db->fire_pos + needed > FlexQL::FIRE_CAP) {
        std::string err;
        if (db->fire_pos > 0) {
            if (!flexql::send_bulk(db->fd, db->fire_buf, db->fire_pos, err)) {
                return FLEXQL_ERROR;
            }
            db->fire_pos = 0;
        }
        // If a single massive query is larger than 2MB, send it immediately as a standalone
        if (needed > FlexQL::FIRE_CAP) {
            if (!flexql::send_line(db->fd, sql, sql_len, err)) {
                return FLEXQL_ERROR;
            }
            return FLEXQL_OK;
        }
    }
    
    // Copy the SQL into our batch buffer
    std::memcpy(db->fire_buf + db->fire_pos, sql, sql_len);
    db->fire_buf[db->fire_pos + sql_len] = '\n';
    db->fire_pos += needed;
    return FLEXQL_OK;
}

/**
 * @brief Flushes the Fire Buffer and waits for a specific number of server responses.
 */
int flexql_drain(FlexQL* db, int count) {
    if (db == nullptr || db->fd == kInvalidSocket || count <= 0) {
        return FLEXQL_ERROR;
    }
    // Step 1: Send any data still sitting in the 2MB buffer
    if (db->fire_pos > 0) {
        std::string err;
        if (!flexql::send_bulk(db->fd, db->fire_buf, db->fire_pos, err)) {
            return FLEXQL_ERROR;
        }
        db->fire_pos = 0;
    }
    
    std::string err;
    std::string line;
    int drained = 0;
    // Step 2: Loop until we have received 'count' number of "END" signals from the server
    while (drained < count) {
        if (!flexql::recv_line(db->fd, line, err)) {
            return FLEXQL_ERROR;
        }
        if (line.size() == 3 && line[0] == 'E' && line[1] == 'N' && line[2] == 'D') {
            drained++;
        }
    }
    return FLEXQL_OK;
}

} // Closing extern "C"
