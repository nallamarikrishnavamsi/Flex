/**
 * @file sql_parser.hpp
 * @brief Hand-written recursive-descent SQL parser.
 *
 * Parses a subset of SQL into strongly-typed ParsedQuery structs.
 * Supported statements:
 *   - CREATE TABLE ... (INT, DECIMAL, VARCHAR, DATETIME types)
 *   - INSERT INTO ... VALUES (...) [EXPIRES IN <secs>]
 *   - SELECT [cols|*] FROM ... [INNER JOIN ... ON ...] [WHERE col op val]
 *   - DROP TABLE [IF EXISTS] ...
 *
 * The parser is stateless and thread-safe — it can be called concurrently
 * from multiple threads without synchronization.
 */

#ifndef FLEXQL_PARSER_SQL_PARSER_HPP
#define FLEXQL_PARSER_SQL_PARSER_HPP

#include <string>

#include "common/types.hpp"

namespace flexql {

class SqlParser {
public:
    /**
     * Parse a SQL string into a typed ParsedQuery.
     * @param sql  Raw SQL string (may have leading/trailing whitespace, trailing semicolon).
     * @param err  On failure, receives a human-readable error message.
     * @return ParsedQuery with type set to Unknown on parse failure.
     */
    ParsedQuery parse(const std::string& sql, std::string& err) const;

private:
    /// Trim leading and trailing whitespace from a string.
    static std::string trim(const std::string& s);
    /// Convert a string to uppercase (returns a copy).
    static std::string to_upper(std::string s);
};

}  // namespace flexql

#endif
