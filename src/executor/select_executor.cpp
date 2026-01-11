#include "nanodb/executor/select_executor.hpp"

#include <iostream>
#include <iomanip>
#include <algorithm>
#include <set>
#include <type_traits>

namespace nanodb {

SelectExecutor::SelectExecutor(Catalog& catalog) : catalog_(catalog) {}

int SelectExecutor::findColumnIndex(const Table& table, const std::string& colName) const {
    for (size_t i = 0; i < table.columns.size(); ++i) {
        if (table.columns[i].name == colName) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

bool SelectExecutor::evaluateSingleCondition(const Row& row, const Table& table, const Condition& cond) const {
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

bool SelectExecutor::evaluateWhereClause(const Row& row, const Table& table, const WhereClause& where) const {
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

void SelectExecutor::execute(const Query& query) {
    const Table* table = catalog_.getTable(query.tableName);
    if (!table) {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
        return;
    }

    // Determine which columns to display
    std::vector<size_t> colIndices;
    if (query.selectColumns.empty()) {
        for (size_t i = 0; i < table->columns.size(); ++i) {
            colIndices.push_back(i);
        }
    } else {
        for (const auto& colName : query.selectColumns) {
            int idx = findColumnIndex(*table, colName);
            if (idx < 0) {
                std::cout << "Error: Column '" << colName << "' not found.\n";
                return;
            }
            colIndices.push_back(static_cast<size_t>(idx));
        }
    }

    // Collect matching rows
    std::vector<const Row*> matchingRows;
    for (const auto& row : table->rows) {
        if (evaluateWhereClause(row, *table, query.where)) {
            matchingRows.push_back(&row);
        }
    }

    // Apply ORDER BY
    if (query.orderBy.hasOrderBy) {
        int sortColIdx = findColumnIndex(*table, query.orderBy.column);
        if (sortColIdx < 0) {
            std::cout << "Error: Column '" << query.orderBy.column << "' not found.\n";
            return;
        }

        std::sort(matchingRows.begin(), matchingRows.end(),
            [sortColIdx, &query](const Row* a, const Row* b) {
                const Value& va = (*a)[sortColIdx];
                const Value& vb = (*b)[sortColIdx];

                bool less = false;
                if (std::holds_alternative<int>(va) && std::holds_alternative<int>(vb)) {
                    less = std::get<int>(va) < std::get<int>(vb);
                } else if (std::holds_alternative<std::string>(va) && std::holds_alternative<std::string>(vb)) {
                    less = std::get<std::string>(va) < std::get<std::string>(vb);
                }

                return query.orderBy.order == SortOrder::ASC ? less : !less;
            });
    }

    // Apply DISTINCT
    std::vector<const Row*> resultRows;
    if (query.distinct) {
        std::set<std::vector<Value>> seen;
        for (const auto* row : matchingRows) {
            std::vector<Value> key;
            for (size_t idx : colIndices) {
                key.push_back((*row)[idx]);
            }
            if (seen.find(key) == seen.end()) {
                seen.insert(key);
                resultRows.push_back(row);
            }
        }
    } else {
        resultRows = matchingRows;
    }

    // Print header
    for (size_t i = 0; i < colIndices.size(); ++i) {
        std::cout << std::setw(15) << table->columns[colIndices[i]].name;
        if (i < colIndices.size() - 1) std::cout << " | ";
    }
    std::cout << "\n";

    // Print separator
    for (size_t i = 0; i < colIndices.size(); ++i) {
        std::cout << std::string(15, '-');
        if (i < colIndices.size() - 1) std::cout << "-+-";
    }
    std::cout << "\n";

    // Print rows (with LIMIT)
    size_t rowCount = 0;
    size_t maxRows = (query.limit > 0) ? static_cast<size_t>(query.limit) : resultRows.size();

    for (const auto* row : resultRows) {
        if (rowCount >= maxRows) break;

        for (size_t i = 0; i < colIndices.size(); ++i) {
            std::visit([](const auto& val) {
                using T = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<T, NullValue>) {
                    std::cout << std::setw(15) << "NULL";
                } else {
                    std::cout << std::setw(15) << val;
                }
            }, (*row)[colIndices[i]]);
            if (i < colIndices.size() - 1) std::cout << " | ";
        }
        std::cout << "\n";
        ++rowCount;
    }

    std::cout << rowCount << " row(s) returned.\n";
}

} // namespace nanodb
