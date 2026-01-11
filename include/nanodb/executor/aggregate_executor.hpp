#pragma once

#include "nanodb/core/types.hpp"
#include "nanodb/catalog/catalog.hpp"
#include <vector>

namespace nanodb {

class AggregateExecutor {
public:
    explicit AggregateExecutor(Catalog& catalog);

    void execute(const Query& query);
    void executeWithGroupBy(const Query& query);

private:
    int findColumnIndex(const Table& table, const std::string& colName) const;
    bool evaluateSingleCondition(const Row& row, const Table& table, const Condition& cond) const;
    bool evaluateWhereClause(const Row& row, const Table& table, const WhereClause& where) const;
    int computeAggregate(AggregateFunc func, const std::string& column,
                         const std::vector<const Row*>& rows, const Table& table) const;
    bool evaluateHaving(const HavingClause& having,
                        const std::vector<const Row*>& groupRows, const Table& table) const;

    Catalog& catalog_;
};

} // namespace nanodb
