/**
 * @file client_main.cpp
 * @brief Interactive REPL client for FlexQL (flexql-client executable).
 *
 * This program acts as the bridge between a human user and the FlexQL database
 * server. It connects to a server, waits for user input (SQL commands), sends
 * them to the server, and displays the results.
 *
 * Usage: ./flexql-client <host> <port>
 */

#include <cstdlib>
#include <iostream>
#include <string>

#ifdef _WIN32
#include <io.h>
/**
 * @brief Checks if the standard input is a terminal (interactive).
 * On Windows, it uses _isatty(0).
 */
static inline int flexql_isatty_stdin() { return _isatty(0); }
#else
#include <unistd.h>
/**
 * @brief Checks if the standard input is a terminal (interactive).
 * On Linux/macOS, it uses isatty(0).
 */
static inline int flexql_isatty_stdin() { return isatty(fileno(stdin)); }
#endif

#include "flexql.h"

/**
 * @brief Callback function for flexql_exec.
 *
 * This function is called by the database engine for EVERY row returned by a
 * query. For example, if a SELECT returns 5 rows, this function runs 5 times.
 *
 * @param column_count Number of columns in the current row.
 * @param values Array of strings representing the data in each column.
 * @param names Array of strings representing the name of each column.
 */
int print_row(void*, int column_count, char** values, char** names) {
    // Print each column as "column_name = value"
    for (int i = 0; i < column_count; ++i) {
        std::cout << names[i] << " = " << (values[i] ? values[i] : "NULL") << "\n";
    }
    // Print a newline between rows for better readability
    std::cout << "\n";
    return 0; // Return 0 to tell the engine to keep sending rows
}

int main(int argc, char **argv) {
  // Speed up standard C++ I/O operations
  std::ios_base::sync_with_stdio(false);
  std::cin.tie(NULL);

  // Validate that the user provided both a hostname and a port number
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <host> <port>\n";
    return 1;
  }

  const char *host = argv[1];
  int port = std::atoi(argv[2]); // Convert port string to integer

  // Try to open a connection to the FlexQL server
  FlexQL *db = nullptr;
  if (flexql_open(host, port, &db) != FLEXQL_OK) {
    std::cerr << "Cannot connect to FlexQL server at " << host << ":" << port
              << "\n";
    return 1;
  }

  // Determine if we are running interactively (human typing) or via a script
  bool interactive = flexql_isatty_stdin() != 0;
  if (interactive) {
    std::cout << "Connected to FlexQL server\n";
    std::cout << "Type SQL commands followed by Enter. Type '.exit' to quit.\n";
  }

  std::string line;
  // Main REPL Loop: Read -> Execute -> Print
  while (true) {
    // Show the prompt if in interactive mode
    if (interactive) {
      std::cout << "flexql> ";
      std::cout.flush(); // Ensure the prompt is visible immediately
    }

    // Read a line of input from the user
    if (!std::getline(std::cin, line)) {
      break; // Stop if there's no more input (e.g., Ctrl+D or end of script)
    }

    // Handle the exit command
    if (line == ".exit") {
      break;
    }

    // Skip empty lines
    if (line.empty()) {
      continue;
    }

    char *err_msg = nullptr;
    // Execute the SQL statement on the server
    // This function sends the query, waits for the response, and calls
    // print_row for results
    int rc = flexql_exec(db, line.c_str(), print_row, nullptr, &err_msg);

    // Handle errors returned by the server
    if (rc != FLEXQL_OK) {
      std::cerr << "SQL error: " << (err_msg ? err_msg : "unknown error")
                << "\n";
      if (err_msg != nullptr) {
        flexql_free(
            err_msg); // Important: Free error memory allocated by the API
      }
    } else if (interactive) {
      // Confirm success if we are in interactive mode and no data was returned
      std::cout << "Query executed successfully\n";
    }
  }

  // Clean up: Close the network connection and free local resources
  flexql_close(db);
  if (interactive) {
    std::cout << "Connection closed\n";
  }
  return 0;
}
