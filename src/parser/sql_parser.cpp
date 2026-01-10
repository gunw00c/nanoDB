#include "nanodb/parser/sql_parser.hpp"

#include <algorithm>
#include <sstream>

namespace nanodb {

Query SQLParser::parse(const std::string& sql) {
    Query query;
    std::string trimmed = trim(sql);
    std::string upper = toUpper(trimmed);

    if (upper.find("CREATE TABLE") == 0) {
        query.type = "CREATE";
        parseCreateTable(trimmed, query);
    } else if (upper.find("DROP TABLE") == 0) {
        query.type = "DROP";
        parseDropTable(trimmed, query);
    } else if (upper.find("INSERT INTO") == 0) {
        query.type = "INSERT";
        parseInsert(trimmed, query);
    } else if (upper.find("DELETE FROM") == 0) {
        query.type = "DELETE";
        parseDelete(trimmed, query);
    } else if (upper.find("SELECT") == 0) {
        query.type = "SELECT";
        parseSelect(trimmed, query);
    } else {
        query.type = "UNKNOWN";
    }

    return query;
}

std::string SQLParser::trim(const std::string& str) {
    size_t start = str.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) return "";
    size_t end = str.find_last_not_of(" \t\n\r");
    return str.substr(start, end - start + 1);
}

std::string SQLParser::toUpper(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
}

void SQLParser::parseCreateTable(const std::string& sql, Query& query) {
    size_t tableStart = sql.find("TABLE") + 5;
    if (tableStart == std::string::npos + 5) {
        tableStart = sql.find("table") + 5;
    }

    size_t parenStart = sql.find('(');
    if (parenStart == std::string::npos) return;

    query.tableName = trim(sql.substr(tableStart, parenStart - tableStart));

    size_t parenEnd = sql.find(')');
    if (parenEnd == std::string::npos) return;

    std::string colDefs = sql.substr(parenStart + 1, parenEnd - parenStart - 1);
    std::stringstream ss(colDefs);
    std::string colDef;

    while (std::getline(ss, colDef, ',')) {
        colDef = trim(colDef);
        std::stringstream colSs(colDef);
        std::string colName, colType;
        colSs >> colName >> colType;

        Column col;
        col.name = colName;
        std::string upperType = toUpper(colType);
        if (upperType == "INT" || upperType == "INTEGER") {
            col.type = ColumnType::INT;
        } else {
            col.type = ColumnType::STRING;
        }
        query.columns.push_back(col);
    }
}

void SQLParser::parseInsert(const std::string& sql, Query& query) {
    std::string upper = toUpper(sql);
    size_t intoPos = upper.find("INTO") + 4;
    size_t valuesPos = upper.find("VALUES");

    if (valuesPos == std::string::npos) return;

    query.tableName = trim(sql.substr(intoPos, valuesPos - intoPos));

    size_t parenStart = sql.find('(', valuesPos);
    size_t parenEnd = sql.find(')', parenStart);
    if (parenStart == std::string::npos || parenEnd == std::string::npos) return;

    std::string valueStr = sql.substr(parenStart + 1, parenEnd - parenStart - 1);
    std::stringstream ss(valueStr);
    std::string val;

    while (std::getline(ss, val, ',')) {
        val = trim(val);
        if (val.empty()) continue;

        // Check if it's a string (quoted)
        if ((val.front() == '\'' && val.back() == '\'') ||
            (val.front() == '"' && val.back() == '"')) {
            query.values.push_back(val.substr(1, val.size() - 2));
        } else {
            // Try to parse as integer
            try {
                int intVal = std::stoi(val);
                query.values.push_back(intVal);
            } catch (...) {
                query.values.push_back(val);
            }
        }
    }
}

void SQLParser::parseSelect(const std::string& sql, Query& query) {
    std::string upper = toUpper(sql);
    size_t fromPos = upper.find("FROM");
    if (fromPos == std::string::npos) return;

    // Parse columns between SELECT and FROM
    size_t selectEnd = 6; // Length of "SELECT"
    std::string columnsPart = trim(sql.substr(selectEnd, fromPos - selectEnd));

    if (columnsPart != "*") {
        std::stringstream ss(columnsPart);
        std::string col;
        while (std::getline(ss, col, ',')) {
            query.selectColumns.push_back(trim(col));
        }
    }

    // Parse table name (and possibly LIMIT)
    std::string afterFrom = sql.substr(fromPos + 4);
    size_t limitPos = upper.find("LIMIT", fromPos);

    if (limitPos != std::string::npos) {
        query.tableName = trim(sql.substr(fromPos + 4, limitPos - fromPos - 4));
        std::string limitStr = trim(sql.substr(limitPos + 5));
        // Remove trailing semicolon
        if (!limitStr.empty() && limitStr.back() == ';') {
            limitStr.pop_back();
        }
        try {
            query.limit = std::stoi(limitStr);
        } catch (...) {
            query.limit = -1;
        }
    } else {
        query.tableName = trim(afterFrom);
    }

    // Remove trailing semicolon from table name
    if (!query.tableName.empty() && query.tableName.back() == ';') {
        query.tableName.pop_back();
        query.tableName = trim(query.tableName);
    }
}

void SQLParser::parseDelete(const std::string& sql, Query& query) {
    std::string upper = toUpper(sql);
    size_t fromPos = upper.find("FROM") + 4;

    query.tableName = trim(sql.substr(fromPos));
    // Remove trailing semicolon
    if (!query.tableName.empty() && query.tableName.back() == ';') {
        query.tableName.pop_back();
        query.tableName = trim(query.tableName);
    }
}

void SQLParser::parseDropTable(const std::string& sql, Query& query) {
    std::string upper = toUpper(sql);
    size_t tablePos = upper.find("TABLE") + 5;

    query.tableName = trim(sql.substr(tablePos));
    // Remove trailing semicolon
    if (!query.tableName.empty() && query.tableName.back() == ';') {
        query.tableName.pop_back();
        query.tableName = trim(query.tableName);
    }
}

} // namespace nanodb
