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

    enum class QueryType {
        CREATE,
        DROP,
        INSERT,
        UPDATE,
        DELETE_Q,
        SELECT
    };

    // Abstract base query — all query types inherit from this
    struct Query {
        QueryType type;
        std::string tableName;
        std::shared_ptr<Query> left;    // Left child for tree structure (subqueries, composites)
        std::shared_ptr<Query> right;   // Right child for tree structure

        explicit Query(QueryType t) : type(t) {}
        virtual ~Query() = default;
    };

    struct CreateQuery : public Query {
        std::vector<Column> columns;
        CreateQuery() : Query(QueryType::CREATE) {}
    };

    struct DropQuery : public Query {
        DropQuery() : Query(QueryType::DROP) {}
    };

    struct InsertQuery : public Query {
        std::vector<std::string> insertColumns;
        std::vector<Value> values;
        InsertQuery() : Query(QueryType::INSERT) {}
    };

    struct UpdateQuery : public Query {
        std::vector<SetClause> setClauses;
        WhereClause where;
        UpdateQuery() : Query(QueryType::UPDATE) {}
    };

    struct DeleteQuery : public Query {
        WhereClause where;
        DeleteQuery() : Query(QueryType::DELETE_Q) {}
    };

    struct SelectQuery : public Query {
        std::vector<std::string> selectColumns;
        WhereClause where;
        OrderByClause orderBy;
        std::vector<AggregateExpr> aggregates;
        GroupByClause groupBy;
        HavingClause having;
        JoinClause join;
        bool distinct = false;
        int limit = -1;
        SelectQuery() : Query(QueryType::SELECT) {}
    };

    // Helper to check if value is NULL
    inline bool isNull(const Value& v) {
        return std::holds_alternative<NullValue>(v);
    }

} // namespace nanodb
