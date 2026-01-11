#include "nanodb/nanodb.hpp"

#include <iostream>
#include <iomanip>
#include <algorithm>
#include <set>
#include <numeric>
#include <map>
#include <type_traits>

namespace nanodb {

void NanoDB::executeSQL(const std::string& sql) {
    Query query = SQLParser::parse(sql);

    if (query.type == "CREATE") {
        handleCreateTable(query);
    } else if (query.type == "DROP") {
        handleDropTable(query);
    } else if (query.type == "INSERT") {
        handleInsert(query);
    } else if (query.type == "UPDATE") {
        handleUpdate(query);
    } else if (query.type == "DELETE") {
        handleDelete(query);
    } else if (query.type == "SELECT") {
        handleSelect(query);
    } else {
        std::cout << "Error: Unknown SQL command\n";
    }
}

int NanoDB::findColumnIndex(const Table& table, const std::string& colName) const {
    for (size_t i = 0; i < table.columns.size(); ++i) {
        if (table.columns[i].name == colName) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

bool NanoDB::evaluateSingleCondition(const Row& row, const Table& table, const Condition& cond) const {
    if (!cond.hasCondition) return true;

    int colIdx = findColumnIndex(table, cond.column);
    if (colIdx < 0) return false;

    const Value& rowVal = row[colIdx];
    const Value& condVal = cond.value;

    // Compare based on types
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

bool NanoDB::evaluateWhereClause(const Row& row, const Table& table, const WhereClause& where) const {
    if (!where.hasWhere) return true;
    if (where.conditions.empty()) return true;

    // Evaluate first condition
    bool result = evaluateSingleCondition(row, table, where.conditions[0]);

    // Apply AND/OR operators for remaining conditions
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

void NanoDB::handleCreateTable(const Query& query) {
    if (catalog_.createTable(query.tableName, query.columns)) {
        std::cout << "Table '" << query.tableName << "' created.\n";
    } else {
        std::cout << "Error: Table '" << query.tableName << "' already exists.\n";
    }
}

void NanoDB::handleDropTable(const Query& query) {
    if (catalog_.dropTable(query.tableName)) {
        std::cout << "Table '" << query.tableName << "' dropped.\n";
    } else {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
    }
}

void NanoDB::handleInsert(const Query& query) {
    Table* table = catalog_.getTable(query.tableName);
    if (!table) {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
        return;
    }

    Row newRow;

    if (!query.insertColumns.empty()) {
        // Explicit column insert: INSERT INTO table (col1, col2) VALUES (v1, v2)
        if (query.insertColumns.size() != query.values.size()) {
            std::cout << "Error: Column count mismatch. Expected "
                      << query.insertColumns.size() << ", got " << query.values.size() << ".\n";
            return;
        }

        // Initialize row with default values
        newRow.resize(table->columns.size());
        for (size_t i = 0; i < table->columns.size(); ++i) {
            if (table->columns[i].type == ColumnType::INT) {
                newRow[i] = 0;
            } else {
                newRow[i] = std::string("");
            }
        }

        // Fill in specified columns
        for (size_t i = 0; i < query.insertColumns.size(); ++i) {
            int colIdx = findColumnIndex(*table, query.insertColumns[i]);
            if (colIdx < 0) {
                std::cout << "Error: Column '" << query.insertColumns[i] << "' not found.\n";
                return;
            }
            newRow[colIdx] = query.values[i];
        }
    } else {
        // Standard insert: INSERT INTO table VALUES (v1, v2, v3)
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

void NanoDB::handleUpdate(const Query& query) {
    Table* table = catalog_.getTable(query.tableName);
    if (!table) {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
        return;
    }

    // Validate SET columns exist
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

void NanoDB::handleDelete(const Query& query) {
    Table* table = catalog_.getTable(query.tableName);
    if (!table) {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
        return;
    }

    size_t beforeCount = table->rows.size();

    if (query.where.hasWhere) {
        // Conditional delete
        table->rows.erase(
            std::remove_if(table->rows.begin(), table->rows.end(),
                [this, table, &query](const Row& row) {
                    return evaluateWhereClause(row, *table, query.where);
                }),
            table->rows.end()
        );
    } else {
        // Delete all
        table->rows.clear();
    }

    size_t deleteCount = beforeCount - table->rows.size();
    std::cout << deleteCount << " row(s) deleted.\n";
}

void NanoDB::handleSelect(const Query& query) {
    // Check for JOIN first
    if (query.join.hasJoin) {
        handleSelectWithJoin(query);
        return;
    }

    // Check for GROUP BY
    if (query.groupBy.hasGroupBy) {
        handleSelectWithGroupBy(query);
        return;
    }

    const Table* table = catalog_.getTable(query.tableName);
    if (!table) {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
        return;
    }

    // Handle aggregate functions (without GROUP BY)
    if (!query.aggregates.empty()) {
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
        return;
    }

    // Determine which columns to display
    std::vector<size_t> colIndices;
    if (query.selectColumns.empty()) {
        // SELECT * - all columns
        for (size_t i = 0; i < table->columns.size(); ++i) {
            colIndices.push_back(i);
        }
    } else {
        // Specific columns
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

int NanoDB::computeAggregate(AggregateFunc func, const std::string& column,
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

bool NanoDB::evaluateHaving(const HavingClause& having,
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

void NanoDB::handleSelectWithGroupBy(const Query& query) {
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
        // Print group key values
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

        // Print aggregate values
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

void NanoDB::handleSelectWithJoin(const Query& query) {
    // Get left table
    const Table* leftTable = catalog_.getTable(query.tableName);
    if (!leftTable) {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
        return;
    }

    // Get right table
    const Table* rightTable = catalog_.getTable(query.join.tableName);
    if (!rightTable) {
        std::cout << "Error: Table '" << query.join.tableName << "' does not exist.\n";
        return;
    }

    // Find join column indices
    int leftJoinCol = findColumnIndex(*leftTable, query.join.leftColumn);
    int rightJoinCol = findColumnIndex(*rightTable, query.join.rightColumn);

    if (leftJoinCol < 0) {
        std::cout << "Error: Column '" << query.join.leftColumn << "' not found in " << query.tableName << ".\n";
        return;
    }
    if (rightJoinCol < 0) {
        std::cout << "Error: Column '" << query.join.rightColumn << "' not found in " << query.join.tableName << ".\n";
        return;
    }

    // Build combined schema
    std::vector<Column> combinedCols;
    for (const auto& col : leftTable->columns) {
        combinedCols.push_back({query.tableName + "." + col.name, col.type});
    }
    for (const auto& col : rightTable->columns) {
        combinedCols.push_back({query.join.tableName + "." + col.name, col.type});
    }

    // Create a temporary table for combined schema lookups
    Table combinedTable;
    combinedTable.columns = combinedCols;

    // Perform nested loop join
    std::vector<Row> joinedRows;

    if (query.join.type == JoinType::INNER) {
        for (const auto& leftRow : leftTable->rows) {
            for (const auto& rightRow : rightTable->rows) {
                if (leftRow[leftJoinCol] == rightRow[rightJoinCol]) {
                    Row combined = leftRow;
                    combined.insert(combined.end(), rightRow.begin(), rightRow.end());
                    joinedRows.push_back(combined);
                }
            }
        }
    } else if (query.join.type == JoinType::LEFT) {
        for (const auto& leftRow : leftTable->rows) {
            bool matched = false;
            for (const auto& rightRow : rightTable->rows) {
                if (leftRow[leftJoinCol] == rightRow[rightJoinCol]) {
                    Row combined = leftRow;
                    combined.insert(combined.end(), rightRow.begin(), rightRow.end());
                    joinedRows.push_back(combined);
                    matched = true;
                }
            }
            if (!matched) {
                // Add left row with NULLs for right side
                Row combined = leftRow;
                for (size_t i = 0; i < rightTable->columns.size(); ++i) {
                    combined.push_back(NullValue{});
                }
                joinedRows.push_back(combined);
            }
        }
    } else if (query.join.type == JoinType::RIGHT) {
        for (const auto& rightRow : rightTable->rows) {
            bool matched = false;
            for (const auto& leftRow : leftTable->rows) {
                if (leftRow[leftJoinCol] == rightRow[rightJoinCol]) {
                    Row combined = leftRow;
                    combined.insert(combined.end(), rightRow.begin(), rightRow.end());
                    joinedRows.push_back(combined);
                    matched = true;
                }
            }
            if (!matched) {
                // Add NULLs for left side with right row
                Row combined;
                for (size_t i = 0; i < leftTable->columns.size(); ++i) {
                    combined.push_back(NullValue{});
                }
                combined.insert(combined.end(), rightRow.begin(), rightRow.end());
                joinedRows.push_back(combined);
            }
        }
    }

    // Determine which columns to display
    std::vector<size_t> colIndices;
    std::vector<std::string> displayNames;

    if (query.selectColumns.empty()) {
        // SELECT * - all columns
        for (size_t i = 0; i < combinedCols.size(); ++i) {
            colIndices.push_back(i);
            displayNames.push_back(combinedCols[i].name);
        }
    } else {
        // Specific columns - need to handle table.column notation
        for (const auto& colName : query.selectColumns) {
            bool found = false;
            for (size_t i = 0; i < combinedCols.size(); ++i) {
                // Match full name (table.column) or just column name
                if (combinedCols[i].name == colName ||
                    combinedCols[i].name.substr(combinedCols[i].name.find('.') + 1) == colName) {
                    colIndices.push_back(i);
                    displayNames.push_back(colName);
                    found = true;
                    break;
                }
            }
            if (!found) {
                std::cout << "Error: Column '" << colName << "' not found.\n";
                return;
            }
        }
    }

    // Print header
    for (size_t i = 0; i < displayNames.size(); ++i) {
        std::cout << std::setw(15) << displayNames[i];
        if (i < displayNames.size() - 1) std::cout << " | ";
    }
    std::cout << "\n";

    // Print separator
    for (size_t i = 0; i < displayNames.size(); ++i) {
        std::cout << std::string(15, '-');
        if (i < displayNames.size() - 1) std::cout << "-+-";
    }
    std::cout << "\n";

    // Print rows
    size_t rowCount = 0;
    size_t maxRows = (query.limit > 0) ? static_cast<size_t>(query.limit) : joinedRows.size();

    for (const auto& row : joinedRows) {
        if (rowCount >= maxRows) break;

        for (size_t i = 0; i < colIndices.size(); ++i) {
            std::visit([](const auto& val) {
                using T = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<T, NullValue>) {
                    std::cout << std::setw(15) << "NULL";
                } else {
                    std::cout << std::setw(15) << val;
                }
            }, row[colIndices[i]]);
            if (i < colIndices.size() - 1) std::cout << " | ";
        }
        std::cout << "\n";
        ++rowCount;
    }

    std::cout << rowCount << " row(s) returned.\n";
}

} // namespace nanodb
