#pragma once

#include <string>
#include <vector>
#include <variant>
#include <memory>

namespace nanodb {

    // Null type for NULL values
    struct NullValue {
        bool operator==(const NullValue&) const { return true; }
        bool operator<(const NullValue&) const { return false; }
    };

    using Value = std::variant<NullValue, int, std::string>;
    using Row = std::vector<Value>;

    enum class ColumnType {
        INT,
        STRING
    };

    enum class CompareOp {
        EQ,   // =
        NE,   // != or <>
        LT,   // <
        LE,   // <=
        GT,   // >
        GE    // >=
    };

    enum class LogicalOp {
        NONE,
        AND,
        OR
    };

    enum class SortOrder {
        ASC,
        DESC
    };

    enum class AggregateFunc {
        NONE,
        COUNT,
        SUM,
        AVG,
        MIN,
        MAX,
        COUNT_STAR  // COUNT(*)
    };

    enum class JoinType {
        INNER,
        LEFT,
        RIGHT
    };

    struct Condition {
        std::string column;
        CompareOp op = CompareOp::EQ;
        Value value;
        bool hasCondition = false;
    };

    struct WhereClause {
        std::vector<Condition> conditions;
        std::vector<LogicalOp> logicalOps;  // AND/OR between conditions
        bool hasWhere = false;
    };

    struct OrderByClause {
        std::string column;
        SortOrder order = SortOrder::ASC;
        bool hasOrderBy = false;
    };

    struct AggregateExpr {
        AggregateFunc func = AggregateFunc::NONE;
        std::string column;  // Empty for COUNT(*)
        std::string alias;   // Optional alias for result column
    };

    struct GroupByClause {
        std::vector<std::string> columns;
        bool hasGroupBy = false;
    };

    struct HavingClause {
        AggregateFunc func = AggregateFunc::NONE;
        std::string column;
        CompareOp op = CompareOp::EQ;
        int value = 0;
        bool hasHaving = false;
    };

    struct JoinClause {
        std::string tableName;        // Table to join with
        JoinType type = JoinType::INNER;
        std::string leftColumn;       // Column from left table
        std::string rightColumn;      // Column from right table
        std::string leftTable;        // Left table name (for disambiguation)
        std::string rightTable;       // Right table name (for disambiguation)
        bool hasJoin = false;
    };

    struct SetClause {
        std::string column;
        Value value;
    };

    struct Column {
        std::string name;
        ColumnType type;
    };

    struct Table {
        std::string name;
        std::vector<Column> columns;
        std::vector<Row> rows;
    };

    struct Query {
        std::string type;       // "CREATE", "INSERT", "SELECT", "DELETE", "DROP", "UPDATE"
        std::string tableName;  // Target table name
        std::vector<Column> columns;              // For CREATE TABLE
        std::vector<Value> values;                // For INSERT
        std::vector<std::string> selectColumns;   // For SELECT col1, col2 (empty = *)
        std::vector<std::string> insertColumns;   // For INSERT INTO table (col1, col2)
        std::vector<SetClause> setClauses;        // For UPDATE SET
        WhereClause where;                        // For WHERE clause (supports AND/OR)
        OrderByClause orderBy;                    // For ORDER BY
        std::vector<AggregateExpr> aggregates;    // For COUNT, SUM, AVG, MIN, MAX
        GroupByClause groupBy;                    // For GROUP BY
        HavingClause having;                      // For HAVING
        JoinClause join;                          // For JOIN
        bool distinct = false;                    // For SELECT DISTINCT
        int limit = -1;                           // For LIMIT (-1 = no limit)
    };

    // Helper to check if value is NULL
    inline bool isNull(const Value& v) {
        return std::holds_alternative<NullValue>(v);
    }

} // namespace nanodb
