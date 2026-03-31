/**
 * @file server_main.cpp
 * @brief Entry point for the FlexQL server (flexql-server executable).
 *
 * Parses command-line arguments and starts the server.
 * Options:
 *   <port>           TCP port number (default: 9000)
 *   --clean          Truncate the WAL file before starting (fresh state)
 *   --nowal          Disable WAL persistence entirely (benchmark mode)
 *   --data-dir <dir> Directory for WAL file storage (default: ./flexql_data)
 */

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include "server/server.hpp"
#include "storage/wal_writer.hpp"

int main(int argc, char** argv) {
    int port = 9000;
    std::string data_dir = "./flexql_data";
    bool clean_start = false;
    bool no_wal = false;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--clean") == 0) {
            clean_start = true;
        } else if (std::strcmp(argv[i], "--nowal") == 0) {
            no_wal = true;
        } else if (std::strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            data_dir = argv[++i];
        } else {
            port = std::atoi(argv[i]);
            if (port <= 0) {
                std::cerr << "Usage: flexql-server [port] [--clean] [--data-dir <dir>]\n";
                return 1;
            }
        }
    }

    if (clean_start) {
        std::cout << "Clean start: truncating WAL...\n";
        flexql::WalWriter::truncate(data_dir);
    }

    flexql::FlexQLServer server(port, data_dir, no_wal);
    if (!server.run()) {
        return 1;
    }
    return 0;
}
