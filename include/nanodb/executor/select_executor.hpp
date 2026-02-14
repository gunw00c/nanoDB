#pragma once

#include "nanodb/core/types.hpp"
#include "nanodb/catalog/catalog.hpp"

namespace nanodb {

class SelectExecutor {
public:
    explicit SelectExecutor(Catalog& catalog);

    void execute(const SelectQuery& query);

private:
    int findColumnIndex(const Table& table, const std::string& colName) const;
    bool evaluateSingleCondition(const Row& row, const Table& table, const Condition& cond) const;
    bool evaluateWhereClause(const Row& row, const Table& table, const WhereClause& where) const;

    Catalog& catalog_;
};

} // namespace nanodb
