/**
 * @file socket_utils.hpp
 * @brief Cross-platform TCP socket utility functions.
 *
 * Provides platform-abstracted socket operations for both Windows (Winsock2)
 * and POSIX systems. Includes efficient send/recv functions with:
 *   - send_line: sends data + newline in a single syscall for small messages.
 *   - send_bulk: raw bulk send without appending newline (for batched responses).
 *   - recv_line: line-oriented receive with 4MB thread-local buffer and memchr scanning.
 *   - has_pending_data: non-blocking check for buffered or socket-level data.
 *
 * Socket buffers are configured to 4MB with TCP_NODELAY for low-latency throughput.
 */

#ifndef FLEXQL_NETWORK_SOCKET_UTILS_HPP
#define FLEXQL_NETWORK_SOCKET_UTILS_HPP

#include <string>

/// Platform-specific socket type and invalid sentinel.
#ifdef _WIN32
#include <winsock2.h>
using socket_t = SOCKET;
constexpr socket_t kInvalidSocket = INVALID_SOCKET;
#else
using socket_t = int;
constexpr socket_t kInvalidSocket = -1;
#endif

namespace flexql {

/// Initialize the socket subsystem (WSAStartup on Windows, no-op on POSIX).
bool init_sockets(std::string& err);
/// Cleanup the socket subsystem (WSACleanup on Windows, no-op on POSIX).
void cleanup_sockets();
/// Close a socket descriptor (closesocket on Windows, close on POSIX).
void close_socket(socket_t fd);

/// Send data followed by a newline '\n'. Uses stack buffer to combine into one send() call.
bool send_line(socket_t fd, const std::string& data, std::string& err);
/// Overload: raw pointer + length version to avoid std::string construction.
bool send_line(socket_t fd, const char* data, std::size_t len, std::string& err);
/// Send raw bytes without appending a newline (used for batched response buffers).
bool send_bulk(socket_t fd, const std::string& data, std::string& err);
/// Overload: raw pointer + length bulk send.
bool send_bulk(socket_t fd, const char* data, std::size_t len, std::string& err);
/// Read one newline-delimited line from the socket. Uses 4MB thread-local buffer.
bool recv_line(socket_t fd, std::string& out, std::string& err);
/// Non-blocking check: returns true if data is available (in buffer or on socket).
bool has_pending_data(socket_t fd);

/// Connect to a remote host and return the socket descriptor.
socket_t connect_to(const std::string& host, int port, std::string& err);
/// Create a listening TCP socket bound to the given port (backlog=64).
socket_t create_listen_socket(int port, std::string& err);

}  // namespace flexql

#endif
