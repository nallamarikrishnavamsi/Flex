/**
 * @file socket_utils.cpp
 * @brief Cross-platform TCP socket implementation.
 *
 * Provides efficient, platform-abstracted socket I/O functions.
 * Key design decisions for performance:
 *   - 4MB thread-local receive buffer with memchr newline scanning (recv_line).
 *   - Stack-buffer coalescing for send_line: data + '\n' sent in a single syscall.
 *   - 4MB send/receive OS buffer sizes (SO_SNDBUF/SO_RCVBUF) + TCP_NODELAY.
 *   - has_pending_data uses select() with zero timeout for non-blocking polling.
 */

#include "network/socket_utils.hpp"

#include <cstring>
#include <cstdint>
#include <sstream>

#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace flexql {

/// Initialize WinSock on Windows (required before any socket operations).
bool init_sockets(std::string& err) {
#ifdef _WIN32
    WSADATA wsa_data;
    int rc = WSAStartup(MAKEWORD(2, 2), &wsa_data);
    if (rc != 0) {
        err = "WSAStartup failed";
        return false;
    }
#endif
    err.clear();
    return true;
}

/// Shutdown WinSock on Windows.
void cleanup_sockets() {
#ifdef _WIN32
    WSACleanup();
#endif
}

/// Close a socket (platform-specific implementation).
void close_socket(socket_t fd) {
#ifdef _WIN32
    closesocket(fd);
#else
    close(fd);
#endif
}

// Thread-local buffers for efficient I/O.
// Using 4MB buffers to minimize recv() syscalls; each thread gets its own
// buffer so no synchronization is needed.
namespace {
thread_local char g_recv_buf[4194304];  // 4MB per-thread receive buffer
thread_local std::size_t g_recv_pos = 0;   // Current read position in buffer
thread_local std::size_t g_recv_size = 0;  // Number of valid bytes in buffer
}

/**
 * Check if there is data ready to read without blocking.
 * First checks the thread-local buffer (leftover from a previous recv).
 * If empty, uses select() with zero timeout to poll the socket.
 */
bool has_pending_data(socket_t fd) {
    // Check if there's unread data in the thread-local recv buffer
    if (g_recv_pos < g_recv_size) return true;
    // Non-blocking check: use select() with zero timeout
    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(fd, &readfds);
    struct timeval tv = {0, 0};  // zero timeout = instant poll
    int rc = select(static_cast<int>(fd) + 1, &readfds, nullptr, nullptr, &tv);
    return rc > 0;
}

/// String overload: delegates to raw pointer version.
bool send_line(socket_t fd, const std::string& data, std::string& err) {
    return send_line(fd, data.data(), data.size(), err);
}

/**
 * Send data + newline in a single send() call using an 8KB stack buffer.
 * For messages <= 8KB, this coalesces the data and newline into one TCP segment,
 * which is critical when TCP_NODELAY is enabled (avoids Nagle overhead).
 * For larger messages, falls back to two separate sends (rare path).
 */
bool send_line(socket_t fd, const char* data, std::size_t len, std::string& err) {
    // Combine data + newline into a single send() call to avoid 2 TCP segments
    // when TCP_NODELAY is set
    constexpr std::size_t kStackBuf = 8192;
    if (len + 1 <= kStackBuf) {
        char buf[kStackBuf];
        std::memcpy(buf, data, len);
        buf[len] = '\n';
        std::size_t total = len + 1;
        std::size_t sent = 0;
        while (sent < total) {
            int rc = send(fd, buf + sent, static_cast<int>(total - sent), 0);
            if (rc <= 0) { err = "send failed"; return false; }
            sent += static_cast<std::size_t>(rc);
        }
    } else {
        // Large message: send data then newline (rare path)
        std::size_t sent = 0;
        while (sent < len) {
            int rc = send(fd, data + sent, static_cast<int>(len - sent), 0);
            if (rc <= 0) { err = "send failed"; return false; }
            sent += static_cast<std::size_t>(rc);
        }
        const char nl = '\n';
        if (send(fd, &nl, 1, 0) <= 0) { err = "send failed"; return false; }
    }
    err.clear();
    return true;
}

/// String overload for bulk send: delegates to raw pointer version.
bool send_bulk(socket_t fd, const std::string& data, std::string& err) {
    return send_bulk(fd, data.data(), data.size(), err);
}

/// Send raw bytes without appending a newline. Used for batched response buffers.
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
    err.clear();
    return true;
}

/**
 * Read one newline-delimited line from the socket into `out`.
 *
 * Uses a 4MB thread-local buffer to amortize recv() syscalls. Scans for
 * newlines using memchr (SIMD-accelerated on most platforms), which is
 * significantly faster than byte-by-byte iteration.
 *
 * Handles \r\n line endings by stripping trailing \r.
 * Pre-reserves capacity for large messages to reduce std::string reallocations.
 */
bool recv_line(socket_t fd, std::string& out, std::string& err) {
    out.clear();
    
    while (true) {
        // Fill buffer if empty
        if (g_recv_pos >= g_recv_size) {
            int rc = recv(fd, g_recv_buf, sizeof(g_recv_buf), 0);
            if (rc <= 0) {
                err = "recv failed";
                return false;
            }
            g_recv_pos = 0;
            g_recv_size = static_cast<std::size_t>(rc);
        }
        
        // Scan for newline using memchr (much faster than byte-by-byte)
        const char* start = g_recv_buf + g_recv_pos;
        std::size_t avail = g_recv_size - g_recv_pos;
        const char* nl = static_cast<const char*>(std::memchr(start, '\n', avail));
        
        if (nl) {
            std::size_t chunk_len = static_cast<std::size_t>(nl - start);
            out.append(start, chunk_len);
            g_recv_pos += chunk_len + 1; // skip the newline
            if (!out.empty() && out.back() == '\r') {
                out.pop_back();
            }
            err.clear();
            return true;
        }
        
        // No newline found — append entire remaining buffer
        out.append(start, avail);
        g_recv_pos = g_recv_size; // force refill on next iteration
        // Pre-reserve for large messages to reduce reallocations
        if (out.size() >= sizeof(g_recv_buf) / 2 && out.capacity() < out.size() * 4) {
            out.reserve(out.size() * 4);
        }
    }
}

/**
 * Connect to a remote FlexQL server.
 * Creates a TCP socket, resolves the host address, and configures
 * TCP_NODELAY + 4MB send/receive buffers for optimal throughput.
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

    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close_socket(fd);
        err = "connect failed";
        return kInvalidSocket;
    }

    // Configure socket for better throughput
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

    err.clear();
    return fd;
}

/**
 * Create a listening TCP server socket.
 * Binds to INADDR_ANY on the given port with SO_REUSEADDR and a backlog of 64.
 * Configures 4MB send/receive OS buffers for accepted connections.
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

    err.clear();
    return fd;
}

}  // namespace flexql
