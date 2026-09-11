/**
 * - [x] Analyze `wal_writer.cpp` logic <!-- id: 16 -->
 * - [x] Add detailed comments to `wal_writer.cpp` <!-- id: 17 -->
 * @file wal_writer.cpp
 * @brief Asynchronous Write-Ahead Log implementation.
 *
 * This file is the "Insurance Policy" of the database. It records every change 
 * to the disk so that data is never lost, even if the power goes out.
 * 
 * Performance Design:
 * It uses a "Double-Buffer" system. The database fills one bucket with data 
 * while a background worker (the "Journalist") empties the other bucket onto 
 * the hard drive. This means the database NEVER has to wait for the slow disk.
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

/**
 * @brief Constructor: Opens the log file and starts the background worker.
 */
WalWriter::WalWriter(const std::string& data_dir) {
    // Ensure the data folder exists on your computer
    std::error_code ec;
    fs::create_directories(data_dir, ec);
    if (ec) {
        std::cerr << "[WAL] Warning: could not create data directory '"
                  << data_dir << "': " << ec.message() << "\n";
    }

    std::string path = data_dir + "/wal.log";

    // OPEN THE JOURNAL: Use raw files for maximum speed
#ifdef _WIN32
    fd_ = _open(path.c_str(), _O_WRONLY | _O_CREAT | _O_APPEND | _O_BINARY, _S_IREAD | _S_IWRITE);
#else
    fd_ = open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
#endif
    if (fd_ < 0) {
        std::cerr << "[WAL] Warning: could not open WAL file '" << path << "'\n";
    }
    buffer_.reserve(1 << 20);  // 1MB serialization buffer

    // Step 1: Initialize the two buckets (Double Buffers)
    buf_a_.reserve(256);
    buf_b_.reserve(256);
    prod_buf_ = &buf_a_;

    // Step 2: Launch the background journalist thread
    worker_thread_ = std::thread(&WalWriter::run, this);
}

/**
 * @brief Destructor: Shuts down the thread and saves any final data.
 */
WalWriter::~WalWriter() {
    // Tell the journalist to finish up and stop
    {
        std::lock_guard<std::mutex> lk(mutex_);
        stop_ = true;
    }
    cv_.notify_one();

    // Wait for the background thread to finish
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }

    // FINAL FLUSH: Make sure the last few bytes are saved before we close
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

/**
 * @brief Records a SQL command into the buffer.
 * High-speed: This DOES NOT write to the disk yet; it just adds to the bucket.
 */
bool WalWriter::append(const std::string& sql) {
    if (fd_ < 0) return false;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        prod_buf_->push_back(sql);
    }
    cv_.notify_one(); // Wake up the journalist
    return true;
}

/**
 * @brief Push a SQL statement (move) into the producer buffer to avoid large string copies.
 */
bool WalWriter::append(std::string&& sql) {
    if (fd_ < 0) return false;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        prod_buf_->push_back(std::move(sql));
    }
    cv_.notify_one();
    return true;
}

/**
 * @brief Wait until everything in the buffer is written to the disk.
 */
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
 * @brief THE JOURNALIST: The background thread loop.
 *
 * It waits for data, swaps the "buckets," and then writes the data to the hard 
 * drive while the main database engine keeps processing new queries.
 */
void WalWriter::run() {
    std::vector<std::string>* consume_buf;
    while (true) {
        {
            // Wait for data or a stop signal
            std::unique_lock<std::mutex> lk(mutex_);
            cv_.wait(lk, [this]() { return !prod_buf_->empty() || stop_; });

            // SWAP: This is the magic. 
            // We swap the pointers instantly so the producer gets a fresh bucket.
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

/**
 * @brief THE RECOVERY SYSTEM: Reads the disk log file.
 * Used when the database restarts to rebuild everything from its history.
 */
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

/**
 * @brief Wipe the log file (start fresh).
 */
void WalWriter::truncate(const std::string& data_dir) {
    std::string path = data_dir + "/wal.log";
    std::ofstream file(path, std::ios::trunc);
    file.close();
}

/**
 * @brief Check if a log file exists on the hard drive.
 */
bool WalWriter::exists(const std::string& data_dir) {
    std::string path = data_dir + "/wal.log";
    std::error_code ec;
    auto sz = fs::file_size(path, ec);
    return !ec && sz > 0;
}

}  // namespace flexql
