#pragma once
#include <string>
#include <memory>
#include "nanodb/core/types.hpp"

namespace nanodb {

class SQLParser {
public:
    static std::unique_ptr<Query> parse(const std::string& sql);

private:
    static std::string trim(const std::string& str);
    static std::string toUpper(const std::string& str);
    static Value parseValue(const std::string& val);
    static Condition parseSingleCondition(const std::string& condStr);
    static void parseWhereClause(const std::string& sql, const std::string& upper, WhereClause& where);
    static void parseOrderBy(const std::string& sql, const std::string& upper, SelectQuery& query);
    static void parseAggregates(const std::string& columnsPart, SelectQuery& query);
    static void parseGroupBy(const std::string& sql, const std::string& upper, SelectQuery& query);
    static void parseHaving(const std::string& sql, const std::string& upper, SelectQuery& query);
    static void parseJoin(const std::string& sql, const std::string& upper, SelectQuery& query);
    static std::unique_ptr<CreateQuery> parseCreateTable(const std::string& sql);
    static std::unique_ptr<InsertQuery> parseInsert(const std::string& sql);
    static std::unique_ptr<SelectQuery> parseSelect(const std::string& sql);
    static std::unique_ptr<DeleteQuery> parseDelete(const std::string& sql);
    static std::unique_ptr<DropQuery> parseDropTable(const std::string& sql);
    static std::unique_ptr<UpdateQuery> parseUpdate(const std::string& sql);
};

} // namespace nanodb
