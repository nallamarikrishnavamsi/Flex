/**
 * @file flexql_api.cpp
 * @brief Client-side implementation of the FlexQL C API.
 *
 * Implements flexql_open, flexql_close, flexql_exec, flexql_exec_fire, flexql_drain,
 * and flexql_free. Communication uses a newline-delimited text protocol over TCP.
 *
 * The fire/drain pipelining API uses a 2MB client-side send buffer to batch
 * multiple queries into fewer send() syscalls, dramatically reducing network
 * overhead for bulk operations (e.g. 10M INSERT/SELECT benchmarks).
 *
 * Wire protocol (server responses):
 *   OK\n           — Statement executed successfully (no result set)
 *   COLS\x1Fn\x1Fcol1\x1Fcol2...\n  — Column header for SELECT results
 *   ROW\x1Fval1\x1Fval2...\n        — One result row (\x1F = unit separator)
 *   ERR\x1Fmessage\n                — Error response
 *   END\n                           — End-of-response marker
 */

#include "flexql.h"

#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "network/socket_utils.hpp"

/**
 * Internal FlexQL connection handle.
 * Contains the TCP socket and the client-side send buffer for pipelining.
 */
struct FlexQL {
    socket_t fd = kInvalidSocket;                          /// TCP socket to the server
    // Send buffer for fire/drain pipelining — avoids 1 send() syscall per query
    char* fire_buf = nullptr;                              /// malloc'd 2MB send buffer
    std::size_t fire_pos = 0;                              /// Current write position in fire_buf
    static constexpr std::size_t FIRE_CAP = 2 * 1024 * 1024;  /// Buffer capacity (2MB)
};

namespace {

constexpr char kSep = 0x1F;  /// Unit Separator: field delimiter in the wire protocol

/// Split a string by the unit separator character into a vector of fields.
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

/// Allocate a C-string copy on the heap (caller must flexql_free).
char* alloc_cstr(const std::string& s) {
    char* p = static_cast<char*>(std::malloc(s.size() + 1));
    if (p == nullptr) {
        return nullptr;
    }
    std::memcpy(p, s.c_str(), s.size() + 1);
    return p;
}

}  // namespace

extern "C" {

int flexql_open(const char* host, int port, FlexQL** db) {
    if (host == nullptr || db == nullptr || port <= 0) {
        return FLEXQL_ERROR;
    }

    std::string err;
    if (!flexql::init_sockets(err)) {
        return FLEXQL_ERROR;
    }

    socket_t fd = flexql::connect_to(host, port, err);
    if (fd == kInvalidSocket) {
        flexql::cleanup_sockets();
        return FLEXQL_ERROR;
    }

    FlexQL* handle = new FlexQL();
    handle->fd = fd;
    handle->fire_buf = static_cast<char*>(std::malloc(FlexQL::FIRE_CAP));
    handle->fire_pos = 0;
    *db = handle;
    return FLEXQL_OK;
}

int flexql_close(FlexQL* db) {
    if (db == nullptr || db->fd == kInvalidSocket) {
        return FLEXQL_ERROR;
    }
    // Flush any buffered fire data before closing
    if (db->fire_buf && db->fire_pos > 0) {
        std::string err;
        flexql::send_bulk(db->fd, db->fire_buf, db->fire_pos, err);
        db->fire_pos = 0;
    }
    std::string err;
    flexql::send_line(db->fd, ".exit", err);
    flexql::close_socket(db->fd);
    db->fd = kInvalidSocket;
    std::free(db->fire_buf);
    db->fire_buf = nullptr;
    delete db;
    flexql::cleanup_sockets();
    return FLEXQL_OK;
}

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
    if (!flexql::send_line(db->fd, sql, sql_len, err)) {
        if (errmsg != nullptr) {
            *errmsg = alloc_cstr("Network send failed");
        }
        return FLEXQL_ERROR;
    }

    std::vector<std::string> columns;
    std::string line;
    while (true) {
        if (!flexql::recv_line(db->fd, line, err)) {
            if (errmsg != nullptr) {
                *errmsg = alloc_cstr("Network receive failed");
            }
            return FLEXQL_ERROR;
        }

        // Fast-path: check for END and OK without split_sep
        if (line.size() == 3 && line[0] == 'E' && line[1] == 'N' && line[2] == 'D') {
            break;
        }
        if (line.size() == 2 && line[0] == 'O' && line[1] == 'K') {
            continue;
        }

        // Check for ERR prefix quickly
        if (line.size() >= 3 && line[0] == 'E' && line[1] == 'R' && line[2] == 'R') {
            if (errmsg != nullptr) {
                // Find the separator after ERR
                std::size_t sep_pos = line.find(kSep, 3);
                if (sep_pos != std::string::npos) {
                    *errmsg = alloc_cstr(line.substr(sep_pos + 1));
                } else {
                    *errmsg = alloc_cstr("Unknown server error");
                }
            }
            // Drain until END
            while (!(line.size() == 3 && line[0] == 'E' && line[1] == 'N' && line[2] == 'D')) {
                if (!flexql::recv_line(db->fd, line, err)) {
                    break;
                }
            }
            return FLEXQL_ERROR;
        }

        // COLS and ROW handling (only needed for SELECT)
        if (line.size() >= 4 && line[0] == 'C' && line[1] == 'O' && line[2] == 'L' && line[3] == 'S') {
            auto parts = split_sep(line);
            columns.assign(parts.begin() + 2, parts.end());
            continue;
        }

        if (line.size() >= 3 && line[0] == 'R' && line[1] == 'O' && line[2] == 'W' && callback != nullptr) {
            auto parts = split_sep(line);
            std::vector<char*> values;
            std::vector<char*> names;

            for (std::size_t i = 1; i < parts.size(); ++i) {
                values.push_back(const_cast<char*>(parts[i].c_str()));
            }
            for (auto& c : columns) {
                names.push_back(const_cast<char*>(c.c_str()));
            }

            int rc = callback(arg, static_cast<int>(values.size()), values.data(), names.data());
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

void flexql_free(void* ptr) {
    std::free(ptr);
}

int flexql_exec_fire(FlexQL* db, const char* sql) {
    if (db == nullptr || sql == nullptr || db->fd == kInvalidSocket) {
        return FLEXQL_ERROR;
    }
    std::size_t sql_len = std::strlen(sql);
    std::size_t needed = sql_len + 1;  // +1 for newline
    // Flush if appending would overflow the buffer
    if (db->fire_pos + needed > FlexQL::FIRE_CAP) {
        std::string err;
        if (db->fire_pos > 0) {
            if (!flexql::send_bulk(db->fd, db->fire_buf, db->fire_pos, err)) {
                return FLEXQL_ERROR;
            }
            db->fire_pos = 0;
        }
        // If single query exceeds buffer capacity, send directly
        if (needed > FlexQL::FIRE_CAP) {
            if (!flexql::send_line(db->fd, sql, sql_len, err)) {
                return FLEXQL_ERROR;
            }
            return FLEXQL_OK;
        }
    }
    std::memcpy(db->fire_buf + db->fire_pos, sql, sql_len);
    db->fire_buf[db->fire_pos + sql_len] = '\n';
    db->fire_pos += needed;
    return FLEXQL_OK;
}

int flexql_drain(FlexQL* db, int count) {
    if (db == nullptr || db->fd == kInvalidSocket || count <= 0) {
        return FLEXQL_ERROR;
    }
    // Flush any buffered fire data before draining responses
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
    while (drained < count) {
        if (!flexql::recv_line(db->fd, line, err)) {
            return FLEXQL_ERROR;
        }
        // Look for END (marks end of one response)
        if (line.size() == 3 && line[0] == 'E' && line[1] == 'N' && line[2] == 'D') {
            drained++;
        }
    }
    return FLEXQL_OK;
}

}
