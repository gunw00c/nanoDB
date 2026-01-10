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
    } else if (upper.find("UPDATE") == 0) {
        query.type = "UPDATE";
        parseUpdate(trimmed, query);
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

Value SQLParser::parseValue(const std::string& val) {
    std::string trimVal = trim(val);

    // Remove trailing semicolon
    if (!trimVal.empty() && trimVal.back() == ';') {
        trimVal.pop_back();
        trimVal = trim(trimVal);
    }

    // Check if it's a string (quoted)
    if ((trimVal.front() == '\'' && trimVal.back() == '\'') ||
        (trimVal.front() == '"' && trimVal.back() == '"')) {
        return trimVal.substr(1, trimVal.size() - 2);
    }

    // Try to parse as integer
    try {
        return std::stoi(trimVal);
    } catch (...) {
        return trimVal;
    }
}

void SQLParser::parseCondition(const std::string& sql, const std::string& upper, Query& query) {
    size_t wherePos = upper.find("WHERE");
    if (wherePos == std::string::npos) return;

    query.where.hasCondition = true;
    std::string condStr = sql.substr(wherePos + 5);

    // Find the end of condition (LIMIT or end of string)
    size_t limitPos = toUpper(condStr).find("LIMIT");
    if (limitPos != std::string::npos) {
        condStr = condStr.substr(0, limitPos);
    }
    condStr = trim(condStr);

    // Remove trailing semicolon
    if (!condStr.empty() && condStr.back() == ';') {
        condStr.pop_back();
        condStr = trim(condStr);
    }

    // Parse operator and split into column and value
    size_t opPos = std::string::npos;
    CompareOp op = CompareOp::EQ;
    size_t opLen = 1;

    // Check for two-character operators first
    if ((opPos = condStr.find(">=")) != std::string::npos) {
        op = CompareOp::GE;
        opLen = 2;
    } else if ((opPos = condStr.find("<=")) != std::string::npos) {
        op = CompareOp::LE;
        opLen = 2;
    } else if ((opPos = condStr.find("!=")) != std::string::npos) {
        op = CompareOp::NE;
        opLen = 2;
    } else if ((opPos = condStr.find("<>")) != std::string::npos) {
        op = CompareOp::NE;
        opLen = 2;
    } else if ((opPos = condStr.find(">")) != std::string::npos) {
        op = CompareOp::GT;
        opLen = 1;
    } else if ((opPos = condStr.find("<")) != std::string::npos) {
        op = CompareOp::LT;
        opLen = 1;
    } else if ((opPos = condStr.find("=")) != std::string::npos) {
        op = CompareOp::EQ;
        opLen = 1;
    }

    if (opPos != std::string::npos) {
        query.where.column = trim(condStr.substr(0, opPos));
        query.where.op = op;
        query.where.value = parseValue(condStr.substr(opPos + opLen));
    }
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

    std::string tableAndCols = trim(sql.substr(intoPos, valuesPos - intoPos));

    // Check for explicit column names: INSERT INTO table (col1, col2) VALUES ...
    size_t colParenStart = tableAndCols.find('(');
    if (colParenStart != std::string::npos) {
        query.tableName = trim(tableAndCols.substr(0, colParenStart));
        size_t colParenEnd = tableAndCols.find(')');
        if (colParenEnd != std::string::npos) {
            std::string colList = tableAndCols.substr(colParenStart + 1, colParenEnd - colParenStart - 1);
            std::stringstream ss(colList);
            std::string col;
            while (std::getline(ss, col, ',')) {
                query.insertColumns.push_back(trim(col));
            }
        }
    } else {
        query.tableName = tableAndCols;
    }

    size_t parenStart = sql.find('(', valuesPos);
    size_t parenEnd = sql.find(')', parenStart);
    if (parenStart == std::string::npos || parenEnd == std::string::npos) return;

    std::string valueStr = sql.substr(parenStart + 1, parenEnd - parenStart - 1);
    std::stringstream ss(valueStr);
    std::string val;

    while (std::getline(ss, val, ',')) {
        query.values.push_back(parseValue(val));
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

    // Find boundaries for table name
    size_t wherePos = upper.find("WHERE", fromPos);
    size_t limitPos = upper.find("LIMIT", fromPos);

    size_t tableEnd = std::string::npos;
    if (wherePos != std::string::npos && limitPos != std::string::npos) {
        tableEnd = std::min(wherePos, limitPos);
    } else if (wherePos != std::string::npos) {
        tableEnd = wherePos;
    } else if (limitPos != std::string::npos) {
        tableEnd = limitPos;
    }

    if (tableEnd != std::string::npos) {
        query.tableName = trim(sql.substr(fromPos + 4, tableEnd - fromPos - 4));
    } else {
        query.tableName = trim(sql.substr(fromPos + 4));
    }

    // Remove trailing semicolon from table name
    if (!query.tableName.empty() && query.tableName.back() == ';') {
        query.tableName.pop_back();
        query.tableName = trim(query.tableName);
    }

    // Parse WHERE condition
    parseCondition(sql, upper, query);

    // Parse LIMIT
    if (limitPos != std::string::npos) {
        std::string limitStr = sql.substr(limitPos + 5);
        limitStr = trim(limitStr);
        if (!limitStr.empty() && limitStr.back() == ';') {
            limitStr.pop_back();
        }
        try {
            query.limit = std::stoi(limitStr);
        } catch (...) {
            query.limit = -1;
        }
    }
}

void SQLParser::parseDelete(const std::string& sql, Query& query) {
    std::string upper = toUpper(sql);
    size_t fromPos = upper.find("FROM") + 4;

    // Find WHERE if present
    size_t wherePos = upper.find("WHERE");

    if (wherePos != std::string::npos) {
        query.tableName = trim(sql.substr(fromPos, wherePos - fromPos));
        parseCondition(sql, upper, query);
    } else {
        query.tableName = trim(sql.substr(fromPos));
        // Remove trailing semicolon
        if (!query.tableName.empty() && query.tableName.back() == ';') {
            query.tableName.pop_back();
            query.tableName = trim(query.tableName);
        }
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

void SQLParser::parseUpdate(const std::string& sql, Query& query) {
    std::string upper = toUpper(sql);

    // UPDATE table SET col = val WHERE ...
    size_t setPos = upper.find("SET");
    if (setPos == std::string::npos) return;

    query.tableName = trim(sql.substr(6, setPos - 6));  // 6 = length of "UPDATE"

    // Find WHERE if present
    size_t wherePos = upper.find("WHERE");

    std::string setClause;
    if (wherePos != std::string::npos) {
        setClause = sql.substr(setPos + 3, wherePos - setPos - 3);
        parseCondition(sql, upper, query);
    } else {
        setClause = sql.substr(setPos + 3);
    }

    // Parse SET clauses (col = val, col2 = val2, ...)
    setClause = trim(setClause);
    if (!setClause.empty() && setClause.back() == ';') {
        setClause.pop_back();
        setClause = trim(setClause);
    }

    std::stringstream ss(setClause);
    std::string assignment;

    while (std::getline(ss, assignment, ',')) {
        assignment = trim(assignment);
        size_t eqPos = assignment.find('=');
        if (eqPos != std::string::npos) {
            SetClause sc;
            sc.column = trim(assignment.substr(0, eqPos));
            sc.value = parseValue(assignment.substr(eqPos + 1));
            query.setClauses.push_back(sc);
        }
    }
}

} // namespace nanodb
