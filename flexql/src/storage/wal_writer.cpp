/**
 * @file wal_writer.cpp
 * @brief Asynchronous Write-Ahead Log implementation.
 *
 * Uses raw file descriptors (_open/_write on Windows, open/write on POSIX) for
 * maximum write throughput. The WAL file is opened in O_APPEND mode so all
 * writes are atomic appends regardless of buffering.
 *
 * Double-buffer design:
 *   - Two vectors (buf_a_, buf_b_) alternate as producer and consumer buffers.
 *   - Under the mutex, producers push SQL strings into prod_buf_.
 *   - The background thread swaps the buffers (instant pointer swap), then writes
 *     the consumed buffer to disk without holding the lock.
 *   - This ensures producer threads never block on disk I/O.
 *
 * The 1MB pre-allocated serialization buffer (buffer_) joins entries with newlines
 * before a single write() syscall, reducing I/O overhead.
 */

#include "storage/wal_writer.hpp"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <sstream>

#ifdef _WIN32
#include <io.h>
#include <fcntl.h>
#include <sys/stat.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace fs = std::filesystem;

namespace flexql {

/// Constructor: creates the data directory, opens the WAL file, and starts the background thread.
WalWriter::WalWriter(const std::string& data_dir) {
    // Ensure data directory exists
    std::error_code ec;
    fs::create_directories(data_dir, ec);
    if (ec) {
        std::cerr << "[WAL] Warning: could not create data directory '"
                  << data_dir << "': " << ec.message() << "\n";
    }

    std::string path = data_dir + "/wal.log";

    // Open with raw file I/O for maximum write throughput
#ifdef _WIN32
    fd_ = _open(path.c_str(), _O_WRONLY | _O_CREAT | _O_APPEND | _O_BINARY, _S_IREAD | _S_IWRITE);
#else
    fd_ = open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
#endif
    if (fd_ < 0) {
        std::cerr << "[WAL] Warning: could not open WAL file '" << path << "'\n";
    }
    buffer_.reserve(1 << 20);  // 1MB write buffer

    // Pre-allocate double buffers
    buf_a_.reserve(256);
    buf_b_.reserve(256);
    prod_buf_ = &buf_a_;

    // Launch the background WAL writer thread
    worker_thread_ = std::thread(&WalWriter::run, this);
}

/// Destructor: signals the background thread to stop, joins it, and flushes remaining data.
WalWriter::~WalWriter() {
    // Signal the background thread to stop
    {
        std::lock_guard<std::mutex> lk(mutex_);
        stop_ = true;
    }
    cv_.notify_one();

    // Wait for the background thread to finish
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }

    // Flush any remaining data
    if (fd_ >= 0) {
        if (!buffer_.empty()) {
#ifdef _WIN32
            _write(fd_, buffer_.data(), static_cast<unsigned>(buffer_.size()));
#else
            (void)::write(fd_, buffer_.data(), buffer_.size());
#endif
            buffer_.clear();
        }
#ifdef _WIN32
        _close(fd_);
#else
        ::close(fd_);
#endif
        fd_ = -1;
    }
}

/// Push a SQL statement (copy) into the producer buffer and wake the background thread.
bool WalWriter::append(const std::string& sql) {
    if (fd_ < 0) return false;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        prod_buf_->push_back(sql);
    }
    cv_.notify_one();
    return true;
}

/// Push a SQL statement (move) into the producer buffer to avoid large string copies.
bool WalWriter::append(std::string&& sql) {
    if (fd_ < 0) return false;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        prod_buf_->push_back(std::move(sql));
    }
    cv_.notify_one();
    return true;
}

/// Blocking flush: waits until the background thread has drained the producer buffer.
void WalWriter::flush() {
    std::unique_lock<std::mutex> lk(mutex_);
    cv_.notify_one();
    cv_.wait(lk, [this]() { return prod_buf_->empty(); });

    // Flush remaining buffer to disk
    if (fd_ >= 0 && !buffer_.empty()) {
#ifdef _WIN32
        _write(fd_, buffer_.data(), static_cast<unsigned>(buffer_.size()));
#else
        (void)::write(fd_, buffer_.data(), buffer_.size());
#endif
        buffer_.clear();
    }
}

/**
 * Background thread main loop.
 *
 * Waits for data in the producer buffer (or a stop signal), then:
 *   1. Swaps producer/consumer buffers under the lock (instant pointer swap).
 *   2. Serializes consumed entries into the write buffer (outside the lock).
 *   3. Writes the buffer to disk via raw file I/O.
 *
 * This design ensures producers never block on disk I/O.
 */
void WalWriter::run() {
    std::vector<std::string>* consume_buf;
    while (true) {
        {
            std::unique_lock<std::mutex> lk(mutex_);
            cv_.wait(lk, [this]() { return !prod_buf_->empty() || stop_; });

            // Swap producer/consumer buffers under lock (instant pointer swap)
            consume_buf = prod_buf_;
            prod_buf_ = (prod_buf_ == &buf_a_) ? &buf_b_ : &buf_a_;
        }
        // Notify flush() that prod_buf_ is now empty
        cv_.notify_all();

        // Build write buffer from consumed entries (no lock held)
        for (auto& entry : *consume_buf) {
            buffer_.append(entry);
            buffer_.push_back('\n');
        }
        consume_buf->clear();

        // Write buffer to disk via raw I/O (no flush — OS handles sync)
        if (!buffer_.empty() && fd_ >= 0) {
            const char* p = buffer_.data();
            std::size_t remaining = buffer_.size();
            while (remaining > 0) {
#ifdef _WIN32
                int written = _write(fd_, p, static_cast<unsigned>(remaining));
#else
                ssize_t written = ::write(fd_, p, remaining);
#endif
                if (written <= 0) break;
                p += written;
                remaining -= static_cast<std::size_t>(written);
            }
            buffer_.clear();
        }

        if (stop_) break;
    }
}

/// Read all SQL statements from the WAL file for replay on server restart.
std::vector<std::string> WalWriter::read_all(const std::string& data_dir) {
    std::vector<std::string> statements;
    std::string path = data_dir + "/wal.log";

    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        return statements;
    }

    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (!line.empty()) {
            statements.push_back(std::move(line));
        }
    }

    return statements;
}

/// Truncate the WAL file (used for --clean start or after checkpointing).
void WalWriter::truncate(const std::string& data_dir) {
    std::string path = data_dir + "/wal.log";
    std::ofstream file(path, std::ios::trunc);
    file.close();
}

/// Check if a WAL file exists and is non-empty (need to replay on startup).
bool WalWriter::exists(const std::string& data_dir) {
    std::string path = data_dir + "/wal.log";
    std::error_code ec;
    auto sz = fs::file_size(path, ec);
    return !ec && sz > 0;
}

}  // namespace flexql
