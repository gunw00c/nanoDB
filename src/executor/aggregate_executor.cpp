#include "nanodb/executor/aggregate_executor.hpp"

#include <iostream>
#include <iomanip>
#include <map>
#include <type_traits>

namespace nanodb {

AggregateExecutor::AggregateExecutor(Catalog& catalog) : catalog_(catalog) {}

int AggregateExecutor::findColumnIndex(const Table& table, const std::string& colName) const {
    for (size_t i = 0; i < table.columns.size(); ++i) {
        if (table.columns[i].name == colName) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

bool AggregateExecutor::evaluateSingleCondition(const Row& row, const Table& table, const Condition& cond) const {
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

bool AggregateExecutor::evaluateWhereClause(const Row& row, const Table& table, const WhereClause& where) const {
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

int AggregateExecutor::computeAggregate(AggregateFunc func, const std::string& column,
                                         const std::vector<const Row*>& rows, const Table& table) const {
    if (func == AggregateFunc::COUNT_STAR || func == AggregateFunc::COUNT) {
        return static_cast<int>(rows.size());
    }

    int colIdx = findColumnIndex(table, column);
    if (colIdx < 0) return 0;

    int result = 0;
    int count = 0;

    for (const auto* row : rows) {
        if (std::holds_alternative<int>((*row)[colIdx])) {
            int val = std::get<int>((*row)[colIdx]);
            switch (func) {
                case AggregateFunc::SUM:
                case AggregateFunc::AVG:
                    result += val;
                    ++count;
                    break;
                case AggregateFunc::MIN:
                    if (count == 0 || val < result) result = val;
                    ++count;
                    break;
                case AggregateFunc::MAX:
                    if (count == 0 || val > result) result = val;
                    ++count;
                    break;
                default:
                    break;
            }
        }
    }

    if (func == AggregateFunc::AVG && count > 0) {
        result = result / count;
    }

    return result;
}

bool AggregateExecutor::evaluateHaving(const HavingClause& having,
                                        const std::vector<const Row*>& groupRows, const Table& table) const {
    if (!having.hasHaving) return true;

    int aggValue = computeAggregate(having.func, having.column, groupRows, table);

    switch (having.op) {
        case CompareOp::EQ: return aggValue == having.value;
        case CompareOp::NE: return aggValue != having.value;
        case CompareOp::LT: return aggValue < having.value;
        case CompareOp::LE: return aggValue <= having.value;
        case CompareOp::GT: return aggValue > having.value;
        case CompareOp::GE: return aggValue >= having.value;
    }
    return true;
}

void AggregateExecutor::execute(const SelectQuery& query) {
    const Table* table = catalog_.getTable(query.tableName);
    if (!table) {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
        return;
    }

    // Collect matching rows
    std::vector<const Row*> matchingRows;
    for (const auto& row : table->rows) {
        if (evaluateWhereClause(row, *table, query.where)) {
            matchingRows.push_back(&row);
        }
    }

    // Process each aggregate
    for (const auto& agg : query.aggregates) {
        if (agg.func == AggregateFunc::COUNT_STAR) {
            std::cout << "COUNT(*)\n";
            std::cout << "--------\n";
            std::cout << matchingRows.size() << "\n";
        } else if (agg.func == AggregateFunc::COUNT) {
            int colIdx = findColumnIndex(*table, agg.column);
            if (colIdx < 0) {
                std::cout << "Error: Column '" << agg.column << "' not found.\n";
                return;
            }
            std::cout << "COUNT(" << agg.column << ")\n";
            std::cout << std::string(15, '-') << "\n";
            std::cout << matchingRows.size() << "\n";
        } else if (agg.func == AggregateFunc::SUM) {
            int colIdx = findColumnIndex(*table, agg.column);
            if (colIdx < 0) {
                std::cout << "Error: Column '" << agg.column << "' not found.\n";
                return;
            }
            int sum = 0;
            for (const auto* row : matchingRows) {
                if (std::holds_alternative<int>((*row)[colIdx])) {
                    sum += std::get<int>((*row)[colIdx]);
                }
            }
            std::cout << "SUM(" << agg.column << ")\n";
            std::cout << std::string(15, '-') << "\n";
            std::cout << sum << "\n";
        } else if (agg.func == AggregateFunc::AVG) {
            int colIdx = findColumnIndex(*table, agg.column);
            if (colIdx < 0) {
                std::cout << "Error: Column '" << agg.column << "' not found.\n";
                return;
            }
            double sum = 0;
            size_t count = 0;
            for (const auto* row : matchingRows) {
                if (std::holds_alternative<int>((*row)[colIdx])) {
                    sum += std::get<int>((*row)[colIdx]);
                    ++count;
                }
            }
            double avg = count > 0 ? sum / count : 0;
            std::cout << "AVG(" << agg.column << ")\n";
            std::cout << std::string(15, '-') << "\n";
            std::cout << std::fixed << std::setprecision(2) << avg << "\n";
        }
    }
    std::cout << "1 row(s) returned.\n";
}

void AggregateExecutor::executeWithGroupBy(const SelectQuery& query) {
    const Table* table = catalog_.getTable(query.tableName);
    if (!table) {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
        return;
    }

    // Get column indices for GROUP BY columns
    std::vector<int> groupColIndices;
    for (const auto& col : query.groupBy.columns) {
        int idx = findColumnIndex(*table, col);
        if (idx < 0) {
            std::cout << "Error: Column '" << col << "' not found.\n";
            return;
        }
        groupColIndices.push_back(idx);
    }

    // Collect matching rows (apply WHERE)
    std::vector<const Row*> matchingRows;
    for (const auto& row : table->rows) {
        if (evaluateWhereClause(row, *table, query.where)) {
            matchingRows.push_back(&row);
        }
    }

    // Group rows by key
    std::map<std::vector<Value>, std::vector<const Row*>> groups;
    for (const auto* row : matchingRows) {
        std::vector<Value> key;
        for (int idx : groupColIndices) {
            key.push_back((*row)[idx]);
        }
        groups[key].push_back(row);
    }

    // Apply HAVING and collect results
    std::vector<std::pair<std::vector<Value>, std::vector<const Row*>>> filteredGroups;
    for (auto& [key, groupRows] : groups) {
        if (evaluateHaving(query.having, groupRows, *table)) {
            filteredGroups.push_back({key, std::move(groupRows)});
        }
    }

    // Determine output columns
    std::vector<std::string> outputHeaders;
    for (const auto& col : query.groupBy.columns) {
        outputHeaders.push_back(col);
    }
    for (const auto& agg : query.aggregates) {
        if (agg.func == AggregateFunc::COUNT_STAR) {
            outputHeaders.push_back("COUNT(*)");
        } else {
            std::string funcName;
            switch (agg.func) {
                case AggregateFunc::COUNT: funcName = "COUNT"; break;
                case AggregateFunc::SUM: funcName = "SUM"; break;
                case AggregateFunc::AVG: funcName = "AVG"; break;
                case AggregateFunc::MIN: funcName = "MIN"; break;
                case AggregateFunc::MAX: funcName = "MAX"; break;
                default: funcName = "?"; break;
            }
            outputHeaders.push_back(funcName + "(" + agg.column + ")");
        }
    }

    // Print header
    for (size_t i = 0; i < outputHeaders.size(); ++i) {
        std::cout << std::setw(15) << outputHeaders[i];
        if (i < outputHeaders.size() - 1) std::cout << " | ";
    }
    std::cout << "\n";

    // Print separator
    for (size_t i = 0; i < outputHeaders.size(); ++i) {
        std::cout << std::string(15, '-');
        if (i < outputHeaders.size() - 1) std::cout << "-+-";
    }
    std::cout << "\n";

    // Print rows
    size_t rowCount = 0;
    for (const auto& [key, groupRows] : filteredGroups) {
        for (size_t i = 0; i < key.size(); ++i) {
            std::visit([](const auto& val) {
                using T = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<T, NullValue>) {
                    std::cout << std::setw(15) << "NULL";
                } else {
                    std::cout << std::setw(15) << val;
                }
            }, key[i]);
            if (i < key.size() - 1 || !query.aggregates.empty()) std::cout << " | ";
        }

        for (size_t i = 0; i < query.aggregates.size(); ++i) {
            const auto& agg = query.aggregates[i];
            int val = computeAggregate(agg.func, agg.column, groupRows, *table);
            std::cout << std::setw(15) << val;
            if (i < query.aggregates.size() - 1) std::cout << " | ";
        }
        std::cout << "\n";
        ++rowCount;
    }

    std::cout << rowCount << " row(s) returned.\n";
}

} // namespace nanodb
