/**
 * @file sql_parser.cpp
 * @brief Hand-written recursive-descent SQL parser implementation.
 *
 * Parses CREATE TABLE, INSERT INTO, SELECT (with JOIN/WHERE), and DROP TABLE.
 * Uses manual string scanning (no regex, no tokenizer) for maximum speed.
 * Supports:
 *   - Column types: INT, DECIMAL, VARCHAR, DATETIME (plus aliases)
 *   - Multi-row INSERT: VALUES (...), (...), ...
 *   - Custom TTL: ... EXPIRES IN <seconds>
 *   - INNER JOIN ... ON table.col = table.col
 *   - WHERE with comparison operators: =, <, >, <=, >=
 */

#include "parser/sql_parser.hpp"

#include <algorithm>
#include <cctype>
#include <sstream>

namespace {

/**
 * Split a comma-separated string, respecting single/double quoted values.
 * E.g. "'hello, world', 42" → ["'hello, world'", " 42"]
 */
std::vector<std::string> split_csv(const std::string& input) {
    std::vector<std::string> out;
    std::size_t start = 0;
    bool in_quote = false;
    char quote = 0;

    for (std::size_t i = 0; i < input.size(); ++i) {
        char ch = input[i];
        if (ch == '\'' || ch == '"') {
            if (!in_quote) {
                in_quote = true;
                quote = ch;
            } else if (quote == ch) {
                in_quote = false;
            }
            continue;
        }

        if (ch == ',' && !in_quote) {
            out.push_back(input.substr(start, i - start));
            start = i + 1;
        }
    }
    if (start <= input.size()) {
        out.push_back(input.substr(start));
    }
    return out;
}

/// Remove enclosing single or double quotes from a string (e.g. "'foo'" → "foo").
std::string strip_quotes(const std::string& s) {
    if (s.size() >= 2) {
        if ((s.front() == '\'' && s.back() == '\'') || (s.front() == '"' && s.back() == '"')) {
            return s.substr(1, s.size() - 2);
        }
    }
    return s;
}

}  // namespace

namespace flexql {

/// Trim leading and trailing whitespace characters.
std::string SqlParser::trim(const std::string& s) {
    std::size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) {
        ++start;
    }
    std::size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
        --end;
    }
    return s.substr(start, end - start);
}

/// Return an uppercase copy of the input string (ASCII only).
std::string SqlParser::to_upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
        return static_cast<char>(std::toupper(c));
    });
    return s;
}

/**
 * Main SQL parser entry point.
 *
 * 1. Trims whitespace and trailing semicolons.
 * 2. Detects the SQL command type from the first keyword (CREATE, INSERT, SELECT, DROP).
 * 3. Delegates to the appropriate parsing section.
 *
 * extract_op: local lambda that finds a comparison operator (<=, >=, =, <, >) in
 * an expression string and splits it into left, operator, and right parts.
 */
ParsedQuery SqlParser::parse(const std::string& sql_in, std::string& err) const {
    ParsedQuery parsed;
    err.clear();

    auto extract_op = [&](const std::string& e, std::string& l, std::string& o, std::string& r) {
        const char* ops[] = {"<=", ">=", "=", "<", ">"};
        for (const char* op_str : ops) {
            std::size_t p = e.find(op_str);
            if (p != std::string::npos) {
                l = trim(e.substr(0, p));
                o = op_str;
                r = trim(e.substr(p + std::string(op_str).size()));
                return true;
            }
        }
        return false;
    };

    std::string sql = trim(sql_in);
    if (!sql.empty() && sql.back() == ';') {
        sql.pop_back();
    }

    const std::string upper = to_upper(sql);

    // ---- DROP TABLE ----
    if (upper.find("DROP ") == 0) {
        std::size_t pos = 5;
        // skip whitespace
        while (pos < upper.size() && std::isspace(static_cast<unsigned char>(upper[pos]))) ++pos;
        if (upper.compare(pos, 6, "TABLE ") == 0) {
            pos += 6;
            while (pos < upper.size() && std::isspace(static_cast<unsigned char>(upper[pos]))) ++pos;

            parsed.type = QueryType::DropTable;
            if (upper.compare(pos, 10, "IF EXISTS ") == 0) {
                parsed.drop_table.if_exists = true;
                pos += 10;
                while (pos < upper.size() && std::isspace(static_cast<unsigned char>(upper[pos]))) ++pos;
            }
            parsed.drop_table.table_name = sql.substr(pos);
            return parsed;
        }
    }

    // ---- CREATE TABLE ----
    if (upper.find("CREATE ") == 0) {
        std::size_t tpos = upper.find("TABLE ");
        if (tpos != std::string::npos) {
            tpos += 6;
            while (tpos < sql.size() && std::isspace(static_cast<unsigned char>(sql[tpos]))) ++tpos;
            std::size_t paren_open = sql.find('(', tpos);
            if (paren_open != std::string::npos) {
                parsed.type = QueryType::CreateTable;
                parsed.create_table.table_name = trim(sql.substr(tpos, paren_open - tpos));

                // Find matching close paren
                std::size_t paren_close = sql.rfind(')');
                if (paren_close == std::string::npos || paren_close <= paren_open) {
                    err = "Missing closing parenthesis";
                    parsed.type = QueryType::Unknown;
                    return parsed;
                }
                std::string col_str = sql.substr(paren_open + 1, paren_close - paren_open - 1);
                auto defs = split_csv(col_str);
                if (defs.empty()) {
                    err = "CREATE TABLE needs at least one column";
                    parsed.type = QueryType::Unknown;
                    return parsed;
                }
                for (const auto& d : defs) {
                    std::istringstream iss(trim(d));
                    std::string name;
                    std::string type;
                    iss >> name >> type;
                    if (name.empty() || type.empty()) {
                        err = "Invalid column definition";
                        parsed.type = QueryType::Unknown;
                        return parsed;
                    }
                    ColumnType ct;
                    std::string t = to_upper(type);
                    // Strip parenthesized suffix, e.g. VARCHAR(64) -> VARCHAR
                    auto paren = t.find('(');
                    if (paren != std::string::npos) {
                        t = t.substr(0, paren);
                    }
                    if (t == "INT" || t == "INTEGER") {
                        ct = ColumnType::Int;
                    } else if (t == "DECIMAL" || t == "FLOAT" || t == "DOUBLE") {
                        ct = ColumnType::Decimal;
                    } else if (t == "VARCHAR" || t == "TEXT" || t == "STRING") {
                        ct = ColumnType::Varchar;
                    } else if (t == "DATETIME" || t == "TIMESTAMP") {
                        ct = ColumnType::DateTime;
                    } else {
                        err = "Unsupported type: " + type;
                        parsed.type = QueryType::Unknown;
                        return parsed;
                    }
                    // Skip optional PRIMARY KEY tokens
                    std::string extra;
                    while (iss >> extra) {
                        // Consume PRIMARY, KEY, NOT, NULL, etc.
                    }
                    parsed.create_table.columns.push_back({name, to_upper(name), ct});
                }
                return parsed;
            }
        }
    }

    // ---- INSERT INTO ---- (manual fast parser, no regex)
    if (upper.find("INSERT ") == 0) {
        std::size_t pos = 7;
        while (pos < upper.size() && std::isspace(static_cast<unsigned char>(upper[pos]))) ++pos;
        if (upper.compare(pos, 5, "INTO ") == 0) {
            pos += 5;
            while (pos < upper.size() && std::isspace(static_cast<unsigned char>(upper[pos]))) ++pos;
            // Table name
            std::size_t name_start = pos;
            while (pos < sql.size() && !std::isspace(static_cast<unsigned char>(sql[pos])) && sql[pos] != '(') ++pos;
            parsed.type = QueryType::Insert;
            parsed.insert.table_name = sql.substr(name_start, pos - name_start);
            // Skip to VALUES
            while (pos < upper.size() && std::isspace(static_cast<unsigned char>(upper[pos]))) ++pos;
            if (upper.compare(pos, 7, "VALUES ") == 0 || upper.compare(pos, 7, "VALUES(") == 0) {
                pos += 6;
                while (pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[pos]))) ++pos;
                if (sql[pos] == '(') {
                    std::size_t last_close = sql.rfind(')');
                    if (last_close == std::string::npos || last_close < pos) {
                        err = "Missing closing parenthesis for VALUES";
                        parsed.type = QueryType::Unknown;
                        return parsed;
                    }
                    while (pos < last_close) {
                        if (sql[pos] == '(') {
                            ++pos;
                            std::size_t close = sql.find(')', pos);
                            if (close == std::string::npos || close > last_close) close = last_close;
                            std::string vals_str = sql.substr(pos, close - pos);
                            auto vals = split_csv(vals_str);
                            std::vector<std::string> row_vals;
                            row_vals.reserve(vals.size());
                            for (const auto& v : vals) {
                                row_vals.push_back(strip_quotes(trim(v)));
                            }
                            parsed.insert.values_list.push_back(std::move(row_vals));

                            pos = close + 1;
                            while (pos < last_close && std::isspace(static_cast<unsigned char>(sql[pos]))) ++pos;
                            if (pos < last_close && sql[pos] == ',') {
                                ++pos;
                                while (pos < last_close && std::isspace(static_cast<unsigned char>(sql[pos]))) ++pos;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    if (parsed.insert.values_list.empty()) {
                        err = "Empty VALUES clause";
                        parsed.type = QueryType::Unknown;
                        return parsed;
                    }
                    // Check for EXPIRES IN <seconds>
                    std::string after = trim(sql.substr(last_close + 1));
                    std::string after_upper = to_upper(after);
                    if (after_upper.find("EXPIRES IN ") == 0) {
                        parsed.insert.has_ttl = true;
                        parsed.insert.ttl_seconds = std::stoi(after.substr(11));
                    }
                    return parsed;
                }
            }
            err = "INSERT syntax error";
            parsed.type = QueryType::Unknown;
            return parsed;
        }
    }

    {
        if (upper.find("SELECT ") == 0) {
            parsed.type = QueryType::Select;

            std::size_t from_pos = upper.find(" FROM ");
            if (from_pos == std::string::npos) {
                err = "SELECT missing FROM";
                parsed.type = QueryType::Unknown;
                return parsed;
            }

            std::string select_part = trim(sql.substr(6, from_pos - 6));
            std::string rest = trim(sql.substr(from_pos + 6));

            if (select_part == "*") {
                parsed.select.select_all = true;
            } else {
                auto cols = split_csv(select_part);
                for (const auto& c : cols) {
                    parsed.select.columns.push_back(trim(c));
                }
            }

            std::string rest_upper = to_upper(rest);
            std::size_t join_pos = rest_upper.find(" INNER JOIN ");
            std::size_t where_pos = rest_upper.find(" WHERE ");

            if (join_pos == std::string::npos && where_pos == std::string::npos) {
                parsed.select.from_table = trim(rest);
                return parsed;
            }

            if (join_pos != std::string::npos) {
                parsed.select.from_table = trim(rest.substr(0, join_pos));
                std::string join_part = trim(rest.substr(join_pos + 12));
                std::string join_part_upper = to_upper(join_part);
                std::size_t on_pos = join_part_upper.find(" ON ");
                if (on_pos == std::string::npos) {
                    err = "INNER JOIN missing ON";
                    parsed.type = QueryType::Unknown;
                    return parsed;
                }
                std::string right_table = trim(join_part.substr(0, on_pos));
                std::string cond_and_where = trim(join_part.substr(on_pos + 4));
                std::string cond_upper = to_upper(cond_and_where);
                std::size_t where_in_join = cond_upper.find(" WHERE ");
                std::string cond = where_in_join == std::string::npos ? cond_and_where : trim(cond_and_where.substr(0, where_in_join));

                std::string left_ref, op, right_ref;
                if (!extract_op(cond, left_ref, op, right_ref)) {
                    err = "JOIN condition must use =, <, >, <=, or >=";
                    parsed.type = QueryType::Unknown;
                    return parsed;
                }

                auto split_ref = [](const std::string& ref, std::string& t, std::string& c) -> bool {
                    std::size_t dot = ref.find('.');
                    if (dot == std::string::npos) {
                        return false;
                    }
                    t = ref.substr(0, dot);
                    c = ref.substr(dot + 1);
                    return !t.empty() && !c.empty();
                };

                JoinClause j;
                j.left_table = parsed.select.from_table;
                j.right_table = right_table;
                if (!split_ref(left_ref, j.left_table, j.left_column) || !split_ref(right_ref, j.right_table, j.right_column)) {
                    err = "JOIN ON must use table.column = table.column";
                    parsed.type = QueryType::Unknown;
                    return parsed;
                }
                parsed.select.has_join = true;
                parsed.select.join = j;
                parsed.select.join.op = op;

                if (where_in_join != std::string::npos) {
                    std::string where_expr = trim(cond_and_where.substr(where_in_join + 7));
                    std::string l, o, r;
                    if (!extract_op(where_expr, l, o, r)) {
                        err = "WHERE must use =, <, >, <=, or >=";
                        parsed.type = QueryType::Unknown;
                        return parsed;
                    }
                    WhereClause w;
                    w.left = l;
                    w.op = o;
                    w.value = strip_quotes(r);
                    parsed.select.has_where = true;
                    parsed.select.where = w;
                }
                return parsed;
            }

            parsed.select.from_table = trim(rest.substr(0, where_pos));
            std::string where_expr = trim(rest.substr(where_pos + 7));
            if (to_upper(where_expr).find(" AND ") != std::string::npos || to_upper(where_expr).find(" OR ") != std::string::npos) {
                err = "Only one WHERE condition is supported";
                parsed.type = QueryType::Unknown;
                return parsed;
            }
            std::string l, o, r;
            if (!extract_op(where_expr, l, o, r)) {
                err = "WHERE must use =, <, >, <=, or >=";
                parsed.type = QueryType::Unknown;
                return parsed;
            }
            WhereClause w;
            w.left = l;
            w.op = o;
            w.value = strip_quotes(r);
            parsed.select.has_where = true;
            parsed.select.where = w;
            return parsed;
        }
    }

    err = "Unsupported SQL";
    return parsed;
}

}  // namespace flexql
