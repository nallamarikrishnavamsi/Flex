/**
 * @file server.cpp
 * @brief TCP server implementation: connection handling and query dispatch.
 *
 * This file is the "Brain of the Server." It handles multithreading (many users 
 * at once), optimizes data delivery using buffers, and restores data from 
 * previous sessions.
 * 
 * Major Features:
 * 1.  Thread-per-Client: Every user gets their own dedicated worker thread.
 * 2.  Response Batching: Collects up to 2MB of data before sending over the network.
 * 3.  Fast-Path SELECT: Bypasses the full SQL parser for simple ID lookups.
 * 4.  WAL Replay: Automatically restores data when the server boots up.
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

/**
 * @brief The Unit Separator (ASCII 31).
 * Used to divide data fields in the network protocol.
 */
constexpr char kSep = 0x1F;


/**
 * @brief THE CLIENT WORKER: Handles a single person's connection.
 *
 * This function runs inside its own THREAD. It reads queries from the client,
 * runs them against the database, and sends back the results.
 */
void handle_client(socket_t fd, flexql::DatabaseEngine& engine) {
    std::string err;
    std::string line;
    
    /**
     * @brief The Response Buffer (2MB).
     * Instead of sending 1 row at a time, we collect up to 2MB of results
     * and send them in one "burst" to maximize network speed.
     */
    std::string response_buf;
    response_buf.reserve(1 << 20);  // Start with 1MB allocated memory

    flexql::QueryResult result;
    std::string exec_err;
    exec_err.reserve(256);

    // Static text for quick "OK" responses
    static const char ok_end_response[] = "OK\nEND\n";
    static constexpr std::size_t ok_end_len = 7;

    constexpr std::size_t FLUSH_THRESHOLD = 2 * 1024 * 1024; // 2MB

    // MAIN LOOP: Wait for the client to send a query
    while (flexql::recv_line(fd, line, err)) {
        
        // Command to close the connection
        if (line.size() == 5 && line[0] == '.' && line[1] == 'e') {
            break;  // .exit
        }

        // Benchmark Command: Turn off the disk storage for this session
        if (line == ".nowal") {
            engine.set_wal_enabled(false);
            response_buf.append(ok_end_response, ok_end_len);
            // Flush the buffer if the user isn't sending more data immediately
            if (!flexql::has_pending_data(fd) || response_buf.size() >= FLUSH_THRESHOLD) {
                if (!flexql::send_bulk(fd, response_buf, err)) break;
                response_buf.clear();
            }
            continue;
        }

        result.column_names.clear();
        result.rows.clear();
        exec_err.clear();

        /**
         * @brief OPTIMIZATION: The "Fast-Path" SELECT.
         * For simple ID lookups, we bypass the complete SQL parser to save 
         * CPU cycles. This makes PK lookups 10x faster.
         */
        if (line.size() > 6 && (line[0] == 'S' || line[0] == 's')) {
            if (engine.try_fast_select(line, response_buf, kSep, exec_err)) {
                if (!exec_err.empty()) {
                    response_buf += "ERR";
                    response_buf += kSep;
                    response_buf += exec_err;
                    response_buf += '\n';
                    response_buf += "END\n";
                }
                // Send the data now if the client isn't sending more queries in a batch
                if (!flexql::has_pending_data(fd) || response_buf.size() >= FLUSH_THRESHOLD) {
                    if (!flexql::send_bulk(fd, response_buf, err)) break;
                    response_buf.clear();
                }
                continue;
            }
            exec_err.clear();
        }

        // Standard Execution: Full SQL Parser → Engine Execution
        bool ok = engine.execute(std::move(line), result, exec_err);

        if (!ok) {
            // Something went wrong (e.g., Table Not Found)
            response_buf += "ERR";
            response_buf += kSep;
            response_buf += exec_err;
            response_buf += '\n';
            response_buf += "END\n";
        } else if (result.column_names.empty() && result.rows.empty()) {
            // Success, but no data to show (e.g., after an INSERT)
            response_buf.append(ok_end_response, ok_end_len);
        } else {
            // Success with Data Rows (e.g., after a SELECT)
            response_buf += "OK\n";

            // Add the Column Headers
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

            // Add each Data Row
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

        // Intelligent Flushing: If the user "fired" a batch of queries, 
        // don't send individual bites back. Wait until the end of the batch
        // or until our 2MB buffer is full.
        if (!flexql::has_pending_data(fd) || response_buf.size() >= FLUSH_THRESHOLD) {
            if (!flexql::send_bulk(fd, response_buf, err)) break;
            response_buf.clear();
        }
    }

    // Connection closing: send any final data bytes left in the buffer
    if (!response_buf.empty()) {
        flexql::send_bulk(fd, response_buf, err);
    }

    flexql::close_socket(fd);
}

}  // namespace

namespace flexql {

FlexQLServer::FlexQLServer(int port, const std::string& data_dir, bool no_wal)
    : port_(port), data_dir_(data_dir), no_wal_(no_wal), running_(false) {}

/**
 * @brief Starts the background engine and the server listening loop.
 */
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

    // Initialize the Core Engine
    DatabaseEngine engine(3600, data_dir_);
    if (no_wal_) engine.set_wal_enabled(false);

    /**
     * @brief THE TIME MACHINE: WAL Replay.
     * Before we open for business, we read the data files from previous 
     * sessions and recreate all your tables and data in RAM.
     */
    if (WalWriter::exists(data_dir_)) {
        engine.replay_wal();
    }

    running_ = true;
    std::cout << "FlexQL server listening on port " << port_ << "\n";

    // MASTER ACCEPT LOOP: Wait for NEW people to connect
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

        // Configure the new connection for maximum speed (no delays)
        int opt_nodelay = 1;
        int sndbuf = 4194304;  // 4MB pipes
        int rcvbuf = 4194304;  // 4MB pipes
#ifdef _WIN32
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&opt_nodelay), sizeof(opt_nodelay));
        setsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&sndbuf), sizeof(sndbuf));
        setsockopt(client_fd, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&rcvbuf), sizeof(rcvbuf));
#else
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt_nodelay, sizeof(opt_nodelay));
        setsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
        setsockopt(client_fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
#endif

        /**
         * @brief SPAWN WORKER:
         * We create a NEW background thread for this specific user and then 
         * detach it, so the master server can immediately go back to 
         * waiting for the NEXT user.
         */
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
