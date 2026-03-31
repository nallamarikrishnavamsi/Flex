/**
 * @file wal_writer.hpp
 * @brief Asynchronous Write-Ahead Log (WAL) with a dedicated background I/O thread.
 *
 * The WAL ensures durability of mutating SQL operations (INSERT, CREATE, DROP).
 * Callers push SQL statements into a lock-free double-buffer queue via append().
 * A background thread drains one buffer while producers fill the other, ensuring
 * client threads never block on disk I/O.
 *
 * Uses raw file descriptors (_open/_write on Windows, open/write on POSIX) instead
 * of std::fstream for maximum write throughput, with O_APPEND for atomic appends.
 *
 * On server restart, WalWriter::read_all() replays all persisted statements to
 * restore the in-memory state (crash recovery). checkpoint_wal() compacts the WAL
 * by rewriting it with only the current live data.
 */

#ifndef FLEXQL_STORAGE_WAL_WRITER_HPP
#define FLEXQL_STORAGE_WAL_WRITER_HPP

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace flexql {

/// Write-Ahead Log with a dedicated background I/O thread.
///
/// Callers push SQL statements into a queue via `append()`.
/// A background thread drains the queue and flushes to disk, so
/// client threads never block on I/O.
class WalWriter {
public:
    explicit WalWriter(const std::string& data_dir = "./flexql_data");
    ~WalWriter();

    // Non-copyable / non-movable
    WalWriter(const WalWriter&) = delete;
    WalWriter& operator=(const WalWriter&) = delete;

    /// Push a SQL statement onto the async queue (non-blocking).
    bool append(const std::string& sql);

    /// Move-overload: avoids copying large SQL strings.
    bool append(std::string&& sql);

    /// Signal the background thread to flush immediately.
    /// Blocks until the queue is drained and data is on disk.
    void flush();

    /// Read all SQL statements from the WAL file.
    static std::vector<std::string> read_all(const std::string& data_dir = "./flexql_data");

    /// Truncate the WAL file (for clean start).
    static void truncate(const std::string& data_dir = "./flexql_data");

    /// Return true if the WAL file exists and is non-empty.
    static bool exists(const std::string& data_dir = "./flexql_data");

private:
    /// Background thread main loop
    void run();

    int fd_ = -1;              /// Raw file descriptor for fast I/O (O_APPEND mode)
    std::string buffer_;       /// Serialization buffer: entries are joined here before write()

    // Double-buffer design: producers fill one buffer while the consumer writes the other.
    // Buffer swap happens under the mutex (instant pointer swap, no data copy).
    std::vector<std::string> buf_a_;
    std::vector<std::string> buf_b_;
    std::vector<std::string>* prod_buf_;  /// Active producer buffer (guarded by mutex_)

    // Background thread and synchronization
    std::thread worker_thread_;     /// Dedicated I/O thread that drains the consumer buffer
    std::mutex mutex_;              /// Guards prod_buf_ and the buffer swap
    std::condition_variable cv_;    /// Signals the worker thread to wake up
    std::atomic<bool> stop_{false}; /// Shutdown flag for the background thread
};

}  // namespace flexql

#endif
