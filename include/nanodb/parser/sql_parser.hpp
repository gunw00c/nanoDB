#pragma once
#include <string>
#include "nanodb/core/types.hpp"

namespace nanodb {

class SQLParser {
public:
    static Query parse(const std::string& sql);

private:
    static std::string trim(const std::string& str);
    static std::string toUpper(const std::string& str);
    static Value parseValue(const std::string& val);
    static Condition parseSingleCondition(const std::string& condStr);
    static void parseWhereClause(const std::string& sql, const std::string& upper, Query& query);
    static void parseOrderBy(const std::string& sql, const std::string& upper, Query& query);
    static void parseAggregates(const std::string& columnsPart, Query& query);
    static void parseGroupBy(const std::string& sql, const std::string& upper, Query& query);
    static void parseHaving(const std::string& sql, const std::string& upper, Query& query);
    static void parseJoin(const std::string& sql, const std::string& upper, Query& query);
    static void parseCreateTable(const std::string& sql, Query& query);
    static void parseInsert(const std::string& sql, Query& query);
    static void parseSelect(const std::string& sql, Query& query);
    static void parseDelete(const std::string& sql, Query& query);
    static void parseDropTable(const std::string& sql, Query& query);
    static void parseUpdate(const std::string& sql, Query& query);
};

} // namespace nanodb
