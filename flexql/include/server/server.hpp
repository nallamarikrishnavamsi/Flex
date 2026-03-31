/**
 * @file server.hpp
 * @brief TCP server that accepts client connections and dispatches queries.
 *
 * Listens on a configurable port and spawns one detached thread per client.
 * Each client thread reads newline-delimited SQL, executes via DatabaseEngine,
 * and streams back protocol-framed responses (OK/ERR/COLS/ROW/END).
 *
 * On startup, replays the WAL file to restore persisted state.
 * Supports --nowal mode for pure-benchmark scenarios.
 */

#ifndef FLEXQL_SERVER_SERVER_HPP
#define FLEXQL_SERVER_SERVER_HPP

#include <atomic>
#include <string>

namespace flexql {

class FlexQLServer {
public:
    /// @param port     TCP port to listen on
    /// @param data_dir Directory for persistent WAL storage
    /// @param no_wal   If true, disable WAL persistence (benchmark mode)
    explicit FlexQLServer(int port, const std::string& data_dir = "./flexql_data", bool no_wal = false);

    /// Start accepting connections (blocks until stop() is called).
    bool run();

    /// Signal the server to stop accepting new connections.
    void stop();

private:
    int port_;                  /// TCP listen port
    std::string data_dir_;      /// Directory for WAL file storage
    bool no_wal_;               /// If true, WAL is disabled
    std::atomic<bool> running_; /// Server run flag (atomic for thread-safe stop)
};

}  // namespace flexql

#endif
