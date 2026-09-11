/**
 * @file server_main.cpp
 * @brief Entry point for the FlexQL server (flexql-server executable).
 *
 * This file is the "Ignition Switch" for the database. It gathers all your
 * settings from the command line (like the Port, Data Folder, and Mode)
 * and starts the main server engine.
 */

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include "server/server.hpp"
#include "storage/wal_writer.hpp"

int main(int argc, char** argv) {
    // Default Settings
    int port = 9000;                        // Standard communication port
    std::string data_dir = "./flexql_data"; // Where to save data on the disk
    bool clean_start = false;               // Whether to wipe existing data
    bool no_wal = false;                    // Whether to turn off disk storage

    // Command-line Argument Loop:
    // This part reads everything you typed after 'flexql-server' in the terminal.
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--clean") == 0) {
            // Option to start with a completely empty database
            clean_start = true;
        } else if (std::strcmp(argv[i], "--nowal") == 0) {
            // Option for "Super Speed" (RAM only, no disk writing)
            no_wal = true;
        } else if (std::strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            // Option to change where the database files are stored
            data_dir = argv[++i];
        } else {
            // Any other number is treated as the Port (e.g., flexql-server 8080)
            port = std::atoi(argv[i]);
            if (port <= 0) {
                std::cerr << "Usage: flexql-server [port] [--clean] [--data-dir <dir>]\n";
                return 1;
            }
        }
    }

    // Step 1: If the user requested a "--clean" start, we empty out the data folder.
    if (clean_start) {
        std::cout << "Clean start: truncating (wiping) previous WAL data...\n";
        flexql::WalWriter::truncate(data_dir);
    }

    // Step 2: Initialize the Master Server with the gathered settings.
    flexql::FlexQLServer server(port, data_dir, no_wal);

    // Step 3: Hit the "Run" button to start listening for clients.
    std::cout << "FlexQL Server starting on port " << port << "...\n";
    if (!server.run()) {
        std::cerr << "Server failed to start.\n";
        return 1;
    }

    return 0;
}
