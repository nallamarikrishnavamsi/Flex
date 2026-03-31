/**
 * @file server.cpp
 * @brief TCP server implementation: connection handling and query dispatch.
 *
 * The server uses a thread-per-client model. Each client thread:
 *   1. Reads newline-delimited SQL from the socket (recv_line).
 *   2. Attempts the fast-path inline SELECT parser (try_fast_select) first.
 *   3. Falls back to the full execute() path for other queries.
 *   4. Batches responses into a 2MB buffer (response_buf) and flushes when:
 *      - No more pipelined data is waiting on the socket, OR
 *      - The buffer exceeds 2MB.
 *
 * This batching strategy, combined with has_pending_data() polling, minimizes
 * send() syscalls and maximizes throughput for pipelined clients.
 *
 * Wire protocol (newline-delimited, fields separated by \x1F):
 *   Request:  <SQL>\n
 *   Response: OK\n | ERR\x1F<msg>\n | COLS\x1F<n>\x1F<col>...\n | ROW\x1F<val>...\n | END\n
 */

#include "server/server.hpp"

#include <iostream>
#include <sstream>
#include <string>
#include <thread>

#ifdef _WIN32
#include <winsock2.h>
#include <windows.h>
#else
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#endif

#include "network/socket_utils.hpp"
#include "storage/database_engine.hpp"
#include "storage/wal_writer.hpp"

namespace {

constexpr char kSep = 0x1F;  /// Unit Separator: field delimiter in the wire protocol

/// Append a separator-joined list of fields followed by a newline to the buffer.
void append_fields(std::string& buf, const std::vector<std::string>& fields) {
    for (std::size_t i = 0; i < fields.size(); ++i) {
        if (i > 0) {
            buf += kSep;
        }
        buf += fields[i];
    }
    buf += '\n';
}

/**
 * Handle a single client connection on a dedicated thread.
 *
 * Reads SQL queries in a loop, executes them against the shared DatabaseEngine,
 * and streams back protocol-framed responses. Uses response batching (2MB buffer)
 * with has_pending_data() polling to maximize throughput for pipelined clients.
 *
 * Special commands:
 *   .exit  — Close the connection.
 *   .nowal — Disable WAL persistence for this engine (benchmark mode).
 */
void handle_client(socket_t fd, flexql::DatabaseEngine& engine) {
    std::string err;
    std::string line;
    std::string response_buf;
    response_buf.reserve(1 << 20);  // 1MB response accumulator

    // Pre-allocate reusable objects
    flexql::QueryResult result;
    std::string exec_err;
    exec_err.reserve(256);

    // Static response for INSERT/CREATE/DROP (no alloc per call)
    static const char ok_end_response[] = "OK\nEND\n";
    static constexpr std::size_t ok_end_len = 7;

    // Flush threshold: accumulate up to 2MB before flushing
    constexpr std::size_t FLUSH_THRESHOLD = 2 * 1024 * 1024;

    while (flexql::recv_line(fd, line, err)) {
        if (line.size() == 5 && line[0] == '.' && line[1] == 'e') {
            break;  // .exit
        }
        if (line == ".nowal") {
            engine.set_wal_enabled(false);
            response_buf.append(ok_end_response, ok_end_len);
            if (!flexql::has_pending_data(fd) || response_buf.size() >= FLUSH_THRESHOLD) {
                if (!flexql::send_bulk(fd, response_buf, err)) break;
                response_buf.clear();
            }
            continue;
        }

        result.column_names.clear();
        result.rows.clear();
        exec_err.clear();

        // Fast-path: PK SELECT → write response directly, skip QueryResult
        if (line.size() > 6 && (line[0] == 'S' || line[0] == 's')) {
            if (engine.try_fast_select(line, response_buf, kSep, exec_err)) {
                if (!exec_err.empty()) {
                    response_buf += "ERR";
                    response_buf += kSep;
                    response_buf += exec_err;
                    response_buf += '\n';
                    response_buf += "END\n";
                }
                // Flush if no more pipelined data or buffer is large
                if (!flexql::has_pending_data(fd) || response_buf.size() >= FLUSH_THRESHOLD) {
                    if (!flexql::send_bulk(fd, response_buf, err)) break;
                    response_buf.clear();
                }
                continue;
            }
            exec_err.clear();
        }

        bool ok = engine.execute(std::move(line), result, exec_err);

        if (!ok) {
            response_buf += "ERR";
            response_buf += kSep;
            response_buf += exec_err;
            response_buf += '\n';
            response_buf += "END\n";
        } else if (result.column_names.empty() && result.rows.empty()) {
            response_buf.append(ok_end_response, ok_end_len);
        } else {
            response_buf += "OK\n";

            if (!result.column_names.empty()) {
                response_buf += "COLS";
                response_buf += kSep;
                response_buf += std::to_string(result.column_names.size());
                for (const auto& cn : result.column_names) {
                    response_buf += kSep;
                    response_buf += cn;
                }
                response_buf += '\n';
            }

            for (const auto& row : result.rows) {
                response_buf += "ROW";
                for (const auto& val : row) {
                    response_buf += kSep;
                    response_buf += val;
                }
                response_buf += '\n';
            }
            response_buf += "END\n";
        }

        // Flush if no more pipelined queries or buffer is large
        if (!flexql::has_pending_data(fd) || response_buf.size() >= FLUSH_THRESHOLD) {
            if (!flexql::send_bulk(fd, response_buf, err)) break;
            response_buf.clear();
        }
    }

    // Flush any remaining data
    if (!response_buf.empty()) {
        flexql::send_bulk(fd, response_buf, err);
    }

    flexql::close_socket(fd);
}



}  // namespace

namespace flexql {

FlexQLServer::FlexQLServer(int port, const std::string& data_dir, bool no_wal)
    : port_(port), data_dir_(data_dir), no_wal_(no_wal), running_(false) {}

bool FlexQLServer::run() {
    std::string err;
    if (!init_sockets(err)) {
        std::cerr << "Socket init error: " << err << "\n";
        return false;
    }

    socket_t listen_fd = create_listen_socket(port_, err);
    if (listen_fd == kInvalidSocket) {
        std::cerr << "Listen error: " << err << "\n";
        cleanup_sockets();
        return false;
    }

    DatabaseEngine engine(3600, data_dir_);
    if (no_wal_) engine.set_wal_enabled(false);

    // Replay WAL to restore persisted state
    if (WalWriter::exists(data_dir_)) {
        engine.replay_wal();
    }

    running_ = true;
    std::cout << "FlexQL server listening on port " << port_ << "\n";

    while (running_) {
        sockaddr_in client_addr{};
#ifdef _WIN32
        int len = sizeof(client_addr);
#else
        socklen_t len = sizeof(client_addr);
#endif
        socket_t client_fd = accept(listen_fd, reinterpret_cast<sockaddr*>(&client_addr), &len);
        if (client_fd == kInvalidSocket) {
            continue;
        }

        // Configure accepted client socket
        int opt_nodelay = 1;
        int sndbuf = 4194304;  // 4MB
        int rcvbuf = 4194304;  // 4MB
#ifdef _WIN32
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&opt_nodelay), sizeof(opt_nodelay));
        setsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&sndbuf), sizeof(sndbuf));
        setsockopt(client_fd, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&rcvbuf), sizeof(rcvbuf));
#else
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt_nodelay, sizeof(opt_nodelay));
        setsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
        setsockopt(client_fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
#endif

        // One detached thread per client connection (cross-platform)
        std::thread(handle_client, client_fd, std::ref(engine)).detach();
    }

    close_socket(listen_fd);
    cleanup_sockets();
    return true;
}

void FlexQLServer::stop() {
    running_ = false;
}

}
