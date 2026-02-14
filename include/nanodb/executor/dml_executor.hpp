#pragma once

#include "nanodb/core/types.hpp"
#include "nanodb/catalog/catalog.hpp"

namespace nanodb {

class DMLExecutor {
public:
    explicit DMLExecutor(Catalog& catalog);

    void executeInsert(const InsertQuery& query);
    void executeUpdate(const UpdateQuery& query);
    void executeDelete(const DeleteQuery& query);

private:
    int findColumnIndex(const Table& table, const std::string& colName) const;
    bool evaluateSingleCondition(const Row& row, const Table& table, const Condition& cond) const;
    bool evaluateWhereClause(const Row& row, const Table& table, const WhereClause& where) const;

    Catalog& catalog_;
};

} // namespace nanodb
