#include "nanodb/parser/sql_parser.hpp"

#include <algorithm>
#include <sstream>

namespace nanodb {

std::unique_ptr<Query> SQLParser::parse(const std::string& sql) {
    std::string trimmed = trim(sql);
    std::string upper = toUpper(trimmed);

    if (upper.find("CREATE TABLE") == 0) {
        return parseCreateTable(trimmed);
    } else if (upper.find("DROP TABLE") == 0) {
        return parseDropTable(trimmed);
    } else if (upper.find("INSERT INTO") == 0) {
        return parseInsert(trimmed);
    } else if (upper.find("UPDATE") == 0) {
        return parseUpdate(trimmed);
    } else if (upper.find("DELETE FROM") == 0) {
        return parseDelete(trimmed);
    } else if (upper.find("SELECT") == 0) {
        return parseSelect(trimmed);
    }

    return nullptr;
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

    if (trimVal.empty()) {
        return NullValue{};
    }

    // Remove trailing semicolon
    if (!trimVal.empty() && trimVal.back() == ';') {
        trimVal.pop_back();
        trimVal = trim(trimVal);
    }

    // Check for NULL
    if (toUpper(trimVal) == "NULL") {
        return NullValue{};
    }

    // Check if it's a string (quoted)
    if (trimVal.size() >= 2 &&
        ((trimVal.front() == '\'' && trimVal.back() == '\'') ||
         (trimVal.front() == '"' && trimVal.back() == '"'))) {
        return trimVal.substr(1, trimVal.size() - 2);
    }

    // Try to parse as integer
    try {
        return std::stoi(trimVal);
    } catch (...) {
        return trimVal;
    }
}

Condition SQLParser::parseSingleCondition(const std::string& condStr) {
    Condition cond;
    cond.hasCondition = true;

    std::string trimmed = trim(condStr);

    // Parse operator and split into column and value
    size_t opPos = std::string::npos;
    CompareOp op = CompareOp::EQ;
    size_t opLen = 1;

    // Check for two-character operators first
    if ((opPos = trimmed.find(">=")) != std::string::npos) {
        op = CompareOp::GE;
        opLen = 2;
    } else if ((opPos = trimmed.find("<=")) != std::string::npos) {
        op = CompareOp::LE;
        opLen = 2;
    } else if ((opPos = trimmed.find("!=")) != std::string::npos) {
        op = CompareOp::NE;
        opLen = 2;
    } else if ((opPos = trimmed.find("<>")) != std::string::npos) {
        op = CompareOp::NE;
        opLen = 2;
    } else if ((opPos = trimmed.find(">")) != std::string::npos) {
        op = CompareOp::GT;
        opLen = 1;
    } else if ((opPos = trimmed.find("<")) != std::string::npos) {
        op = CompareOp::LT;
        opLen = 1;
    } else if ((opPos = trimmed.find("=")) != std::string::npos) {
        op = CompareOp::EQ;
        opLen = 1;
    }

    if (opPos != std::string::npos) {
        cond.column = trim(trimmed.substr(0, opPos));
        cond.op = op;
        cond.value = parseValue(trimmed.substr(opPos + opLen));
    }

    return cond;
}

void SQLParser::parseWhereClause(const std::string& sql, const std::string& upper, WhereClause& where) {
    size_t wherePos = upper.find("WHERE");
    if (wherePos == std::string::npos) return;

    where.hasWhere = true;

    // Find the end of WHERE clause
    size_t endPos = upper.length();
    size_t groupPos = upper.find("GROUP BY", wherePos);
    size_t havingPos = upper.find("HAVING", wherePos);
    size_t orderPos = upper.find("ORDER BY", wherePos);
    size_t limitPos = upper.find("LIMIT", wherePos);

    if (groupPos != std::string::npos && groupPos < endPos) endPos = groupPos;
    if (havingPos != std::string::npos && havingPos < endPos) endPos = havingPos;
    if (orderPos != std::string::npos && orderPos < endPos) endPos = orderPos;
    if (limitPos != std::string::npos && limitPos < endPos) endPos = limitPos;

    std::string whereStr = sql.substr(wherePos + 5, endPos - wherePos - 5);
    whereStr = trim(whereStr);

    // Remove trailing semicolon
    if (!whereStr.empty() && whereStr.back() == ';') {
        whereStr.pop_back();
        whereStr = trim(whereStr);
    }

    // Split by AND/OR while preserving order
    std::string upperWhere = toUpper(whereStr);
    std::vector<std::string> parts;
    std::vector<LogicalOp> ops;

    size_t pos = 0;
    while (pos < whereStr.length()) {
        size_t andPos = upperWhere.find(" AND ", pos);
        size_t orPos = upperWhere.find(" OR ", pos);

        size_t nextOp = std::string::npos;
        LogicalOp op = LogicalOp::NONE;
        size_t opLen = 0;

        if (andPos != std::string::npos && (orPos == std::string::npos || andPos < orPos)) {
            nextOp = andPos;
            op = LogicalOp::AND;
            opLen = 5;  // " AND "
        } else if (orPos != std::string::npos) {
            nextOp = orPos;
            op = LogicalOp::OR;
            opLen = 4;  // " OR "
        }

        if (nextOp != std::string::npos) {
            parts.push_back(trim(whereStr.substr(pos, nextOp - pos)));
            ops.push_back(op);
            pos = nextOp + opLen;
        } else {
            parts.push_back(trim(whereStr.substr(pos)));
            break;
        }
    }

    // Parse each condition
    for (const auto& part : parts) {
        where.conditions.push_back(parseSingleCondition(part));
    }
    where.logicalOps = ops;
}

void SQLParser::parseOrderBy(const std::string& sql, const std::string& upper, SelectQuery& query) {
    size_t orderPos = upper.find("ORDER BY");
    if (orderPos == std::string::npos) return;

    query.orderBy.hasOrderBy = true;

    // Find end of ORDER BY (LIMIT or end)
    size_t endPos = upper.length();
    size_t limitPos = upper.find("LIMIT", orderPos);
    if (limitPos != std::string::npos) endPos = limitPos;

    std::string orderStr = sql.substr(orderPos + 8, endPos - orderPos - 8);
    orderStr = trim(orderStr);

    // Remove trailing semicolon
    if (!orderStr.empty() && orderStr.back() == ';') {
        orderStr.pop_back();
        orderStr = trim(orderStr);
    }

    // Check for ASC/DESC
    std::string upperOrder = toUpper(orderStr);
    if (upperOrder.find(" DESC") != std::string::npos) {
        query.orderBy.order = SortOrder::DESC;
        size_t descPos = upperOrder.find(" DESC");
        query.orderBy.column = trim(orderStr.substr(0, descPos));
    } else if (upperOrder.find(" ASC") != std::string::npos) {
        query.orderBy.order = SortOrder::ASC;
        size_t ascPos = upperOrder.find(" ASC");
        query.orderBy.column = trim(orderStr.substr(0, ascPos));
    } else {
        query.orderBy.order = SortOrder::ASC;  // Default
        query.orderBy.column = orderStr;
    }
}

void SQLParser::parseAggregates(const std::string& columnsPart, SelectQuery& query) {
    std::string upper = toUpper(columnsPart);

    // Check for aggregate functions
    auto parseAgg = [&](const std::string& funcName, AggregateFunc func) {
        size_t pos = upper.find(funcName + "(");
        if (pos != std::string::npos) {
            size_t start = pos + funcName.length() + 1;
            size_t end = upper.find(")", start);
            if (end != std::string::npos) {
                AggregateExpr agg;
                agg.func = func;
                std::string col = trim(columnsPart.substr(start, end - start));
                if (col == "*") {
                    agg.func = AggregateFunc::COUNT_STAR;
                } else {
                    agg.column = col;
                }
                query.aggregates.push_back(agg);
            }
        }
    };

    parseAgg("COUNT", AggregateFunc::COUNT);
    parseAgg("SUM", AggregateFunc::SUM);
    parseAgg("AVG", AggregateFunc::AVG);
    parseAgg("MIN", AggregateFunc::MIN);
    parseAgg("MAX", AggregateFunc::MAX);
}

void SQLParser::parseGroupBy(const std::string& sql, const std::string& upper, SelectQuery& query) {
    size_t groupPos = upper.find("GROUP BY");
    if (groupPos == std::string::npos) return;

    query.groupBy.hasGroupBy = true;

    // Find end of GROUP BY
    size_t endPos = upper.length();
    size_t havingPos = upper.find("HAVING", groupPos);
    size_t orderPos = upper.find("ORDER BY", groupPos);
    size_t limitPos = upper.find("LIMIT", groupPos);

    if (havingPos != std::string::npos && havingPos < endPos) endPos = havingPos;
    if (orderPos != std::string::npos && orderPos < endPos) endPos = orderPos;
    if (limitPos != std::string::npos && limitPos < endPos) endPos = limitPos;

    std::string groupStr = sql.substr(groupPos + 8, endPos - groupPos - 8);
    groupStr = trim(groupStr);

    // Remove trailing semicolon
    if (!groupStr.empty() && groupStr.back() == ';') {
        groupStr.pop_back();
        groupStr = trim(groupStr);
    }

    // Parse comma-separated columns
    std::stringstream ss(groupStr);
    std::string col;
    while (std::getline(ss, col, ',')) {
        query.groupBy.columns.push_back(trim(col));
    }
}

void SQLParser::parseHaving(const std::string& sql, const std::string& upper, SelectQuery& query) {
    size_t havingPos = upper.find("HAVING");
    if (havingPos == std::string::npos) return;

    query.having.hasHaving = true;

    // Find end of HAVING
    size_t endPos = upper.length();
    size_t orderPos = upper.find("ORDER BY", havingPos);
    size_t limitPos = upper.find("LIMIT", havingPos);

    if (orderPos != std::string::npos && orderPos < endPos) endPos = orderPos;
    if (limitPos != std::string::npos && limitPos < endPos) endPos = limitPos;

    std::string havingStr = sql.substr(havingPos + 6, endPos - havingPos - 6);
    havingStr = trim(havingStr);

    // Remove trailing semicolon
    if (!havingStr.empty() && havingStr.back() == ';') {
        havingStr.pop_back();
        havingStr = trim(havingStr);
    }

    std::string upperHaving = toUpper(havingStr);

    // Parse aggregate function in HAVING (e.g., COUNT(*) > 1, SUM(col) >= 100)
    auto parseHavingAgg = [&](const std::string& funcName, AggregateFunc func) -> bool {
        size_t funcPos = upperHaving.find(funcName + "(");
        if (funcPos != std::string::npos) {
            size_t parenStart = funcPos + funcName.length() + 1;
            size_t parenEnd = upperHaving.find(")", parenStart);
            if (parenEnd != std::string::npos) {
                query.having.func = func;
                std::string col = trim(havingStr.substr(parenStart, parenEnd - parenStart));
                if (col == "*") {
                    query.having.func = AggregateFunc::COUNT_STAR;
                } else {
                    query.having.column = col;
                }

                // Parse comparison after the function
                std::string afterFunc = havingStr.substr(parenEnd + 1);
                afterFunc = trim(afterFunc);

                // Find operator
                size_t opPos = std::string::npos;
                size_t opLen = 1;
                if ((opPos = afterFunc.find(">=")) != std::string::npos) {
                    query.having.op = CompareOp::GE;
                    opLen = 2;
                } else if ((opPos = afterFunc.find("<=")) != std::string::npos) {
                    query.having.op = CompareOp::LE;
                    opLen = 2;
                } else if ((opPos = afterFunc.find("!=")) != std::string::npos) {
                    query.having.op = CompareOp::NE;
                    opLen = 2;
                } else if ((opPos = afterFunc.find(">")) != std::string::npos) {
                    query.having.op = CompareOp::GT;
                    opLen = 1;
                } else if ((opPos = afterFunc.find("<")) != std::string::npos) {
                    query.having.op = CompareOp::LT;
                    opLen = 1;
                } else if ((opPos = afterFunc.find("=")) != std::string::npos) {
                    query.having.op = CompareOp::EQ;
                    opLen = 1;
                }

                if (opPos != std::string::npos) {
                    std::string valStr = trim(afterFunc.substr(opPos + opLen));
                    try {
                        query.having.value = std::stoi(valStr);
                    } catch (...) {
                        query.having.value = 0;
                    }
                }
                return true;
            }
        }
        return false;
    };

    if (!parseHavingAgg("COUNT", AggregateFunc::COUNT)) {
        if (!parseHavingAgg("SUM", AggregateFunc::SUM)) {
            if (!parseHavingAgg("AVG", AggregateFunc::AVG)) {
                if (!parseHavingAgg("MIN", AggregateFunc::MIN)) {
                    parseHavingAgg("MAX", AggregateFunc::MAX);
                }
            }
        }
    }
}

void SQLParser::parseJoin(const std::string& sql, const std::string& upper, SelectQuery& query) {
    // Look for JOIN types
    size_t joinPos = std::string::npos;
    JoinType joinType = JoinType::INNER;

    size_t innerPos = upper.find("INNER JOIN");
    size_t leftPos = upper.find("LEFT JOIN");
    size_t rightPos = upper.find("RIGHT JOIN");
    size_t simpleJoinPos = upper.find(" JOIN ");

    // Find the first JOIN keyword
    if (leftPos != std::string::npos) {
        joinPos = leftPos;
        joinType = JoinType::LEFT;
    } else if (rightPos != std::string::npos) {
        joinPos = rightPos;
        joinType = JoinType::RIGHT;
    } else if (innerPos != std::string::npos) {
        joinPos = innerPos;
        joinType = JoinType::INNER;
    } else if (simpleJoinPos != std::string::npos) {
        joinPos = simpleJoinPos;
        joinType = JoinType::INNER;  // Default to INNER
    }

    if (joinPos == std::string::npos) return;

    query.join.hasJoin = true;
    query.join.type = joinType;

    // Find the join table name and ON clause
    size_t joinKeywordEnd = joinPos;
    if (joinType == JoinType::LEFT) {
        joinKeywordEnd = joinPos + 9;  // "LEFT JOIN"
    } else if (joinType == JoinType::RIGHT) {
        joinKeywordEnd = joinPos + 10;  // "RIGHT JOIN"
    } else if (upper.find("INNER JOIN", joinPos) == joinPos) {
        joinKeywordEnd = joinPos + 10;  // "INNER JOIN"
    } else {
        joinKeywordEnd = joinPos + 5;  // " JOIN"
    }

    size_t onPos = upper.find(" ON ", joinKeywordEnd);
    if (onPos == std::string::npos) return;

    // Extract join table name
    query.join.tableName = trim(sql.substr(joinKeywordEnd, onPos - joinKeywordEnd));

    // Find end of ON clause
    size_t endPos = upper.length();
    size_t wherePos = upper.find("WHERE", onPos);
    size_t groupPos = upper.find("GROUP BY", onPos);
    size_t orderPos = upper.find("ORDER BY", onPos);
    size_t limitPos = upper.find("LIMIT", onPos);

    if (wherePos != std::string::npos && wherePos < endPos) endPos = wherePos;
    if (groupPos != std::string::npos && groupPos < endPos) endPos = groupPos;
    if (orderPos != std::string::npos && orderPos < endPos) endPos = orderPos;
    if (limitPos != std::string::npos && limitPos < endPos) endPos = limitPos;

    std::string onClause = sql.substr(onPos + 4, endPos - onPos - 4);
    onClause = trim(onClause);

    // Remove trailing semicolon
    if (!onClause.empty() && onClause.back() == ';') {
        onClause.pop_back();
        onClause = trim(onClause);
    }

    // Parse ON condition: table1.col = table2.col
    size_t eqPos = onClause.find('=');
    if (eqPos != std::string::npos) {
        std::string leftSide = trim(onClause.substr(0, eqPos));
        std::string rightSide = trim(onClause.substr(eqPos + 1));

        // Parse left side (table.column or just column)
        size_t leftDot = leftSide.find('.');
        if (leftDot != std::string::npos) {
            query.join.leftTable = trim(leftSide.substr(0, leftDot));
            query.join.leftColumn = trim(leftSide.substr(leftDot + 1));
        } else {
            query.join.leftColumn = leftSide;
        }

        // Parse right side (table.column or just column)
        size_t rightDot = rightSide.find('.');
        if (rightDot != std::string::npos) {
            query.join.rightTable = trim(rightSide.substr(0, rightDot));
            query.join.rightColumn = trim(rightSide.substr(rightDot + 1));
        } else {
            query.join.rightColumn = rightSide;
        }
    }
}

std::unique_ptr<CreateQuery> SQLParser::parseCreateTable(const std::string& sql) {
    auto query = std::make_unique<CreateQuery>();

    size_t tableStart = sql.find("TABLE") + 5;
    if (tableStart == std::string::npos + 5) {
        tableStart = sql.find("table") + 5;
    }

    size_t parenStart = sql.find('(');
    if (parenStart == std::string::npos) return query;

    query->tableName = trim(sql.substr(tableStart, parenStart - tableStart));

    size_t parenEnd = sql.find(')');
    if (parenEnd == std::string::npos) return query;

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
        query->columns.push_back(col);
    }

    return query;
}

std::unique_ptr<InsertQuery> SQLParser::parseInsert(const std::string& sql) {
    auto query = std::make_unique<InsertQuery>();
    std::string upper = toUpper(sql);
    size_t intoPos = upper.find("INTO") + 4;
    size_t valuesPos = upper.find("VALUES");

    if (valuesPos == std::string::npos) return query;

    std::string tableAndCols = trim(sql.substr(intoPos, valuesPos - intoPos));

    // Check for explicit column names: INSERT INTO table (col1, col2) VALUES ...
    size_t colParenStart = tableAndCols.find('(');
    if (colParenStart != std::string::npos) {
        query->tableName = trim(tableAndCols.substr(0, colParenStart));
        size_t colParenEnd = tableAndCols.find(')');
        if (colParenEnd != std::string::npos) {
            std::string colList = tableAndCols.substr(colParenStart + 1, colParenEnd - colParenStart - 1);
            std::stringstream ss(colList);
            std::string col;
            while (std::getline(ss, col, ',')) {
                query->insertColumns.push_back(trim(col));
            }
        }
    } else {
        query->tableName = tableAndCols;
    }

    size_t parenStart = sql.find('(', valuesPos);
    size_t parenEnd = sql.find(')', parenStart);
    if (parenStart == std::string::npos || parenEnd == std::string::npos) return query;

    std::string valueStr = sql.substr(parenStart + 1, parenEnd - parenStart - 1);
    std::stringstream ss(valueStr);
    std::string val;

    while (std::getline(ss, val, ',')) {
        query->values.push_back(parseValue(val));
    }

    return query;
}

std::unique_ptr<SelectQuery> SQLParser::parseSelect(const std::string& sql) {
    auto query = std::make_unique<SelectQuery>();
    std::string upper = toUpper(sql);

    // Check for DISTINCT
    size_t selectEnd = 6;  // Length of "SELECT"
    if (upper.find("SELECT DISTINCT") == 0) {
        query->distinct = true;
        selectEnd = 15;  // Length of "SELECT DISTINCT"
    }

    // Find FROM position
    size_t fromPos = upper.find("FROM");
    if (fromPos == std::string::npos) return query;

    // Parse columns between SELECT [DISTINCT] and FROM
    std::string columnsPart = trim(sql.substr(selectEnd, fromPos - selectEnd));

    // Check for aggregate functions
    parseAggregates(columnsPart, *query);

    // If no aggregates, parse as regular columns
    if (query->aggregates.empty() && columnsPart != "*") {
        std::stringstream ss(columnsPart);
        std::string col;
        while (std::getline(ss, col, ',')) {
            query->selectColumns.push_back(trim(col));
        }
    }

    // Check for JOIN first
    parseJoin(sql, upper, *query);

    // Find boundaries for table name
    size_t tableEnd = upper.length();

    // If there's a JOIN, table name is between FROM and JOIN
    size_t joinPos = upper.find(" JOIN ", fromPos);
    size_t innerJoinPos = upper.find("INNER JOIN", fromPos);
    size_t leftJoinPos = upper.find("LEFT JOIN", fromPos);
    size_t rightJoinPos = upper.find("RIGHT JOIN", fromPos);

    size_t firstJoin = std::string::npos;
    if (joinPos != std::string::npos) firstJoin = joinPos;
    if (innerJoinPos != std::string::npos && (firstJoin == std::string::npos || innerJoinPos < firstJoin)) firstJoin = innerJoinPos;
    if (leftJoinPos != std::string::npos && (firstJoin == std::string::npos || leftJoinPos < firstJoin)) firstJoin = leftJoinPos;
    if (rightJoinPos != std::string::npos && (firstJoin == std::string::npos || rightJoinPos < firstJoin)) firstJoin = rightJoinPos;

    size_t wherePos = upper.find("WHERE", fromPos);
    size_t groupPos = upper.find("GROUP BY", fromPos);
    size_t havingPos = upper.find("HAVING", fromPos);
    size_t orderPos = upper.find("ORDER BY", fromPos);
    size_t limitPos = upper.find("LIMIT", fromPos);

    if (firstJoin != std::string::npos && firstJoin < tableEnd) tableEnd = firstJoin;
    if (wherePos != std::string::npos && wherePos < tableEnd) tableEnd = wherePos;
    if (groupPos != std::string::npos && groupPos < tableEnd) tableEnd = groupPos;
    if (havingPos != std::string::npos && havingPos < tableEnd) tableEnd = havingPos;
    if (orderPos != std::string::npos && orderPos < tableEnd) tableEnd = orderPos;
    if (limitPos != std::string::npos && limitPos < tableEnd) tableEnd = limitPos;

    query->tableName = trim(sql.substr(fromPos + 4, tableEnd - fromPos - 4));

    // Remove trailing semicolon from table name
    if (!query->tableName.empty() && query->tableName.back() == ';') {
        query->tableName.pop_back();
        query->tableName = trim(query->tableName);
    }

    // Parse WHERE clause (with AND/OR support)
    parseWhereClause(sql, upper, query->where);

    // Parse GROUP BY
    parseGroupBy(sql, upper, *query);

    // Parse HAVING
    parseHaving(sql, upper, *query);

    // Parse ORDER BY
    parseOrderBy(sql, upper, *query);

    // Parse LIMIT
    if (limitPos != std::string::npos) {
        std::string limitStr = sql.substr(limitPos + 5);
        limitStr = trim(limitStr);
        if (!limitStr.empty() && limitStr.back() == ';') {
            limitStr.pop_back();
        }
        try {
            query->limit = std::stoi(limitStr);
        } catch (...) {
            query->limit = -1;
        }
    }

    return query;
}

std::unique_ptr<DeleteQuery> SQLParser::parseDelete(const std::string& sql) {
    auto query = std::make_unique<DeleteQuery>();
    std::string upper = toUpper(sql);
    size_t fromPos = upper.find("FROM") + 4;

    // Find WHERE if present
    size_t wherePos = upper.find("WHERE");

    if (wherePos != std::string::npos) {
        query->tableName = trim(sql.substr(fromPos, wherePos - fromPos));
        parseWhereClause(sql, upper, query->where);
    } else {
        query->tableName = trim(sql.substr(fromPos));
        // Remove trailing semicolon
        if (!query->tableName.empty() && query->tableName.back() == ';') {
            query->tableName.pop_back();
            query->tableName = trim(query->tableName);
        }
    }

    return query;
}

std::unique_ptr<DropQuery> SQLParser::parseDropTable(const std::string& sql) {
    auto query = std::make_unique<DropQuery>();
    std::string upper = toUpper(sql);
    size_t tablePos = upper.find("TABLE") + 5;

    query->tableName = trim(sql.substr(tablePos));
    // Remove trailing semicolon
    if (!query->tableName.empty() && query->tableName.back() == ';') {
        query->tableName.pop_back();
        query->tableName = trim(query->tableName);
    }

    return query;
}

std::unique_ptr<UpdateQuery> SQLParser::parseUpdate(const std::string& sql) {
    auto query = std::make_unique<UpdateQuery>();
    std::string upper = toUpper(sql);

    // UPDATE table SET col = val WHERE ...
    size_t setPos = upper.find("SET");
    if (setPos == std::string::npos) return query;

    query->tableName = trim(sql.substr(6, setPos - 6));  // 6 = length of "UPDATE"

    // Find WHERE if present
    size_t wherePos = upper.find("WHERE");

    std::string setClause;
    if (wherePos != std::string::npos) {
        setClause = sql.substr(setPos + 3, wherePos - setPos - 3);
        parseWhereClause(sql, upper, query->where);
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
            query->setClauses.push_back(sc);
        }
    }

    return query;
}

} // namespace nanodb
