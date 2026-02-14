#include "nanodb/executor/dml_executor.hpp"

#include <iostream>
#include <algorithm>

namespace nanodb {

DMLExecutor::DMLExecutor(Catalog& catalog) : catalog_(catalog) {}

int DMLExecutor::findColumnIndex(const Table& table, const std::string& colName) const {
    for (size_t i = 0; i < table.columns.size(); ++i) {
        if (table.columns[i].name == colName) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

bool DMLExecutor::evaluateSingleCondition(const Row& row, const Table& table, const Condition& cond) const {
    if (!cond.hasCondition) return true;

    int colIdx = findColumnIndex(table, cond.column);
    if (colIdx < 0) return false;

    const Value& rowVal = row[colIdx];
    const Value& condVal = cond.value;

    if (std::holds_alternative<int>(rowVal) && std::holds_alternative<int>(condVal)) {
        int rv = std::get<int>(rowVal);
        int cv = std::get<int>(condVal);
        switch (cond.op) {
            case CompareOp::EQ: return rv == cv;
            case CompareOp::NE: return rv != cv;
            case CompareOp::LT: return rv < cv;
            case CompareOp::LE: return rv <= cv;
            case CompareOp::GT: return rv > cv;
            case CompareOp::GE: return rv >= cv;
        }
    } else if (std::holds_alternative<std::string>(rowVal) && std::holds_alternative<std::string>(condVal)) {
        const std::string& rv = std::get<std::string>(rowVal);
        const std::string& cv = std::get<std::string>(condVal);
        switch (cond.op) {
            case CompareOp::EQ: return rv == cv;
            case CompareOp::NE: return rv != cv;
            case CompareOp::LT: return rv < cv;
            case CompareOp::LE: return rv <= cv;
            case CompareOp::GT: return rv > cv;
            case CompareOp::GE: return rv >= cv;
        }
    }

    return false;
}

bool DMLExecutor::evaluateWhereClause(const Row& row, const Table& table, const WhereClause& where) const {
    if (!where.hasWhere) return true;
    if (where.conditions.empty()) return true;

    bool result = evaluateSingleCondition(row, table, where.conditions[0]);

    for (size_t i = 0; i < where.logicalOps.size() && i + 1 < where.conditions.size(); ++i) {
        bool nextResult = evaluateSingleCondition(row, table, where.conditions[i + 1]);

        if (where.logicalOps[i] == LogicalOp::AND) {
            result = result && nextResult;
        } else if (where.logicalOps[i] == LogicalOp::OR) {
            result = result || nextResult;
        }
    }

    return result;
}

void DMLExecutor::executeInsert(const InsertQuery& query) {
    Table* table = catalog_.getTable(query.tableName);
    if (!table) {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
        return;
    }

    Row newRow;

    if (!query.insertColumns.empty()) {
        if (query.insertColumns.size() != query.values.size()) {
            std::cout << "Error: Column count mismatch. Expected "
                      << query.insertColumns.size() << ", got " << query.values.size() << ".\n";
            return;
        }

        newRow.resize(table->columns.size());
        for (size_t i = 0; i < table->columns.size(); ++i) {
            if (table->columns[i].type == ColumnType::INT) {
                newRow[i] = 0;
            } else {
                newRow[i] = std::string("");
            }
        }

        for (size_t i = 0; i < query.insertColumns.size(); ++i) {
            int colIdx = findColumnIndex(*table, query.insertColumns[i]);
            if (colIdx < 0) {
                std::cout << "Error: Column '" << query.insertColumns[i] << "' not found.\n";
                return;
            }
            newRow[colIdx] = query.values[i];
        }
    } else {
        if (query.values.size() != table->columns.size()) {
            std::cout << "Error: Column count mismatch. Expected "
                      << table->columns.size() << ", got " << query.values.size() << ".\n";
            return;
        }
        newRow = query.values;
    }

    table->rows.push_back(newRow);
    std::cout << "1 row inserted.\n";
}

void DMLExecutor::executeUpdate(const UpdateQuery& query) {
    Table* table = catalog_.getTable(query.tableName);
    if (!table) {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
        return;
    }

    for (const auto& sc : query.setClauses) {
        if (findColumnIndex(*table, sc.column) < 0) {
            std::cout << "Error: Column '" << sc.column << "' not found.\n";
            return;
        }
    }

    size_t updateCount = 0;
    for (auto& row : table->rows) {
        if (evaluateWhereClause(row, *table, query.where)) {
            for (const auto& sc : query.setClauses) {
                int colIdx = findColumnIndex(*table, sc.column);
                row[colIdx] = sc.value;
            }
            ++updateCount;
        }
    }

    std::cout << updateCount << " row(s) updated.\n";
}

void DMLExecutor::executeDelete(const DeleteQuery& query) {
    Table* table = catalog_.getTable(query.tableName);
    if (!table) {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
        return;
    }

    size_t beforeCount = table->rows.size();

    if (query.where.hasWhere) {
        table->rows.erase(
            std::remove_if(table->rows.begin(), table->rows.end(),
                [this, table, &query](const Row& row) {
                    return evaluateWhereClause(row, *table, query.where);
                }),
            table->rows.end()
        );
    } else {
        table->rows.clear();
    }

    size_t deleteCount = beforeCount - table->rows.size();
    std::cout << deleteCount << " row(s) deleted.\n";
}

} // namespace nanodb
