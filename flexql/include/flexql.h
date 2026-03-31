/**
 * @file flexql.h
 * @brief Public C API for the FlexQL client library.
 *
 * Provides a sqlite3-style interface for connecting to a FlexQL server,
 * executing SQL queries, and retrieving results via callbacks.
 * Also exposes a pipelining API (fire/drain) for high-throughput bulk
 * operations that batch queries before sending over the network.
 */

#ifndef FLEXQL_H
#define FLEXQL_H

#ifdef __cplusplus
extern "C" {
#endif

/** Opaque handle representing a connection to a FlexQL server. */
typedef struct FlexQL FlexQL;

/** Return codes for all FlexQL API functions. */
enum {
    FLEXQL_OK = 0,
    FLEXQL_ERROR = 1
};

/**
 * Open a TCP connection to a FlexQL server.
 * @param host  Server hostname or IPv4 address (e.g. "127.0.0.1").
 * @param port  Server port number (e.g. 9000).
 * @param db    Output pointer; receives the allocated FlexQL handle on success.
 * @return FLEXQL_OK on success, FLEXQL_ERROR on failure.
 */
int flexql_open(const char *host, int port, FlexQL **db);

/**
 * Close the connection and free the FlexQL handle.
 * Flushes any buffered pipelined queries before closing.
 * @param db  Handle previously returned by flexql_open().
 * @return FLEXQL_OK on success, FLEXQL_ERROR on failure.
 */
int flexql_close(FlexQL *db);

/**
 * Execute a SQL statement and invoke a callback for each result row.
 * @param db        Active FlexQL connection handle.
 * @param sql       Null-terminated SQL string to execute.
 * @param callback  Called once per row: (arg, col_count, values[], col_names[]).
 *                  Return 0 to continue, 1 to abort remaining rows.
 *                  May be NULL if results are not needed.
 * @param arg       User data pointer passed through to the callback.
 * @param errmsg    On error, receives a malloc'd error string (caller must flexql_free).
 * @return FLEXQL_OK on success, FLEXQL_ERROR on failure.
 */
int flexql_exec(
    FlexQL *db,
    const char *sql,
    int (*callback)(void *, int, char **, char **),
    void *arg,
    char **errmsg
);

/**
 * Pipelining API: buffer a query for sending without reading the response.
 * Queries accumulate in a 2MB client-side buffer and are flushed in bulk
 * either when the buffer fills or when flexql_drain() is called.
 * @param db   Active FlexQL connection handle.
 * @param sql  Null-terminated SQL string to enqueue.
 * @return FLEXQL_OK on success, FLEXQL_ERROR on failure.
 */
int flexql_exec_fire(FlexQL *db, const char *sql);

/**
 * Drain N pipelined responses, discarding all result data.
 * Flushes any remaining send buffer before reading responses.
 * @param db     Active FlexQL connection handle.
 * @param count  Number of responses to drain (one per prior flexql_exec_fire call).
 * @return FLEXQL_OK on success, FLEXQL_ERROR on failure.
 */
int flexql_drain(FlexQL *db, int count);

/**
 * Free memory allocated by the FlexQL library (e.g. error messages).
 * @param ptr  Pointer to free; safe to call with NULL.
 */
void flexql_free(void *ptr);

#ifdef __cplusplus
}
#endif

#endif
