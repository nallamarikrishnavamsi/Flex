/**
 * @file socket_utils.cpp
 * @brief Cross-platform TCP socket implementation for FlexQL.
 *
 * This file handles the low-level "networking logic." It provides a unified way
 * for the database to send and receive data, regardless of whether it's running
 * on Windows or Linux.
 * 
 * Performance Optimizations:
 * 1.  4MB Receive Buffer: Minimizes the number of times we ask the OS for more data.
 * 2.  TCP_NODELAY: Disables Nagle's algorithm for instant query delivery.
 * 3.  SIMD Scanning: Uses 'memchr' to find newlines in the buffer at maximum speed.
 */

#include "network/socket_utils.hpp"

#include <cstring>
#include <cstdint>
#include <sstream>

// We use different headers depending on the Operating System
#ifdef _WIN32
#include <ws2tcpip.h> // Windows Sockets 2
#else
#include <arpa/inet.h> // Standard POSIX Sockets (Linux/Mac)
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace flexql {

/**
 * @brief Initialize the network library.
 * On Windows, we MUST call WSAStartup before we can use any network features.
 */
bool init_sockets(std::string& err) {
#ifdef _WIN32
    WSADATA wsa_data;
    int rc = WSAStartup(MAKEWORD(2, 2), &wsa_data);
    if (rc != 0) {
        err = "WSAStartup failed";
        return false;
    }
#endif
    return true;
}

/**
 * @brief Clean up the network library.
 * On Windows, we MUST call WSACleanup when the program is closing.
 */
void cleanup_sockets() {
#ifdef _WIN32
    WSACleanup();
#endif
}

/**
 * @brief Close a network connection.
 * We use 'closesocket' on Windows and standard 'close' on Linux.
 */
void close_socket(socket_t fd) {
#ifdef _WIN32
    closesocket(fd);
#else
    close(fd);
#endif
}

/**
 * Internal Cache: Using 4MB buffers to avoid the "Small Packet Problem."
 * Each thread gets its own private buffer space for maximum speed.
 */
namespace {
thread_local char g_recv_buf[4194304];  // 4MB per-thread receive buffer
thread_local std::size_t g_recv_pos = 0;   // Current read position in buffer
thread_local std::size_t g_recv_size = 0;  // Total data currently in buffer
}

/**
 * @brief Checks if there is data waiting for us to read.
 * First checks our 4MB internal buffer, then asks the Operating System.
 */
bool has_pending_data(socket_t fd) {
    if (g_recv_pos < g_recv_size) return true;
    
    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(fd, &readfds);
    struct timeval tv = {0, 0}; // 0 timeout means "Just check once and don't wait"
    int rc = select(static_cast<int>(fd) + 1, &readfds, nullptr, nullptr, &tv);
    return rc > 0;
}

bool send_line(socket_t fd, const std::string& data, std::string& err) {
    return send_line(fd, data.data(), data.size(), err);
}

/**
 * @brief Sends a line of text plus a newline character.
 * 
 * Optimization: It joins the string and the '\n' into a single "packet" 
 * before sending. This is MUCH faster than sending the string and THEN 
 * the newline separately.
 */
bool send_line(socket_t fd, const char* data, std::size_t len, std::string& err) {
    constexpr std::size_t kStackBuf = 8192; // Use a small 8KB temporary space
    if (len + 1 <= kStackBuf) {
        char buf[kStackBuf];
        std::memcpy(buf, data, len);
        buf[len] = '\n'; // Add the newline
        std::size_t total = len + 1;
        std::size_t sent = 0;
        // Keep sending until the full message is sent
        while (sent < total) {
            int rc = send(fd, buf + sent, static_cast<int>(total - sent), 0);
            if (rc <= 0) { err = "send failed"; return false; }
            sent += static_cast<std::size_t>(rc);
        }
    } else {
        // Fallback for extremely large SQL queries (rarely used)
        std::size_t sent = 0;
        while (sent < len) {
            int rc = send(fd, data + sent, static_cast<int>(len - sent), 0);
            if (rc <= 0) { err = "send failed"; return false; }
            sent += static_cast<std::size_t>(rc);
        }
        const char nl = '\n';
        if (send(fd, &nl, 1, 0) <= 0) { err = "send failed"; return false; }
    }
    return true;
}

bool send_bulk(socket_t fd, const std::string& data, std::string& err) {
    return send_bulk(fd, data.data(), data.size(), err);
}

/**
 * @brief Sends raw bytes across the network.
 */
bool send_bulk(socket_t fd, const char* data, std::size_t len, std::string& err) {
    std::size_t sent = 0;
    while (sent < len) {
        int rc = send(fd, data + sent, static_cast<int>(len - sent), 0);
        if (rc <= 0) {
            err = "send failed";
            return false;
        }
        sent += static_cast<std::size_t>(rc);
    }
    return true;
}

/**
 * @brief Reads one line from the network.
 * 
 * This is the most performance-critical function in the networking layer.
 * It uses the internal 4MB buffer to minimize slow system calls.
 */
bool recv_line(socket_t fd, std::string& out, std::string& err) {
    out.clear();
    
    while (true) {
        // Step 1: If our 4MB buffer is empty, fill it up from the network
        if (g_recv_pos >= g_recv_size) {
            int rc = recv(fd, g_recv_buf, sizeof(g_recv_buf), 0);
            if (rc <= 0) {
                err = "recv failed (connection lost?)";
                return false;
            }
            g_recv_pos = 0;
            g_recv_size = static_cast<std::size_t>(rc);
        }
        
        // Step 2: Use hardware-accelerated 'memchr' to find the newline '\n'
        const char* start = g_recv_buf + g_recv_pos;
        std::size_t avail = g_recv_size - g_recv_pos;
        const char* nl = static_cast<const char*>(std::memchr(start, '\n', avail));
        
        if (nl) {
            // Newline found! Collect the data and return it to the user.
            std::size_t chunk_len = static_cast<std::size_t>(nl - start);
            out.append(start, chunk_len);
            g_recv_pos += chunk_len + 1; // Skip the newline character for next time
            // Strip hidden Windows \r characters if they exist
            if (!out.empty() && out.back() == '\r') {
                out.pop_back();
            }
            return true;
        }
        
        // Step 3: No newline found in this 4MB chunk. Keep the data and fill the buffer again.
        out.append(start, avail);
        g_recv_pos = g_recv_size;
        
        // Optimize memory for giant messages by pre-growing the string
        if (out.size() >= sizeof(g_recv_buf) / 2 && out.capacity() < out.size() * 4) {
            out.reserve(out.size() * 4);
        }
    }
}

/**
 * @brief Create a connection to a remote server.
 * 
 * Also configures the connection for "Super Speed":
 * - TCP_NODELAY: No waiting / lagging.
 * - 4MB SND/RCV: Massive OS-level pipes.
 */
socket_t connect_to(const std::string& host, int port, std::string& err) {
    socket_t fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == kInvalidSocket) {
        err = "socket create failed";
        return kInvalidSocket;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));

    // Resolve "localhost" or an IP address
    unsigned long ip = INADDR_NONE;
    if (host == "localhost") {
        ip = inet_addr("127.0.0.1");
    } else {
        ip = inet_addr(host.c_str());
    }

    if (ip == INADDR_NONE) {
        close_socket(fd);
        err = "invalid IPv4 host";
        return kInvalidSocket;
    }
    addr.sin_addr.s_addr = ip;

    // Connect to the remote server
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close_socket(fd);
        err = "connect failed";
        return kInvalidSocket;
    }

    // Performance Overdrive: Configure the OS to use 4MB buffers and no delays
    int opt_nodelay = 1;
    int sndbuf = 4194304;  // 4MB
    int rcvbuf = 4194304;  // 4MB
#ifdef _WIN32
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&opt_nodelay), sizeof(opt_nodelay));
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&sndbuf), sizeof(sndbuf));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&rcvbuf), sizeof(rcvbuf));
#else
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt_nodelay, sizeof(opt_nodelay));
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
#endif

    return fd;
}

/**
 * @brief Create a listening server socket (for flexql-server).
 */
socket_t create_listen_socket(int port, std::string& err) {
    socket_t fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == kInvalidSocket) {
        err = "socket create failed";
        return kInvalidSocket;
    }

    int opt = 1;
    int sndbuf = 4194304;  // 4MB
    int rcvbuf = 4194304;  // 4MB
#ifdef _WIN32
    // Allow the server to restart instantly (REUSEADDR)
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&opt), sizeof(opt));
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&sndbuf), sizeof(sndbuf));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&rcvbuf), sizeof(rcvbuf));
#else
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
#endif

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));

    // Bind to the port and start listening
    if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close_socket(fd);
        err = "bind failed";
        return kInvalidSocket;
    }

    if (listen(fd, 64) != 0) {
        close_socket(fd);
        err = "listen failed";
        return kInvalidSocket;
    }

    return fd;
}

}  // namespace flexql
