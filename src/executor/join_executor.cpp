#include "nanodb/executor/join_executor.hpp"

#include <iostream>
#include <iomanip>
#include <type_traits>

namespace nanodb {

JoinExecutor::JoinExecutor(Catalog& catalog) : catalog_(catalog) {}

int JoinExecutor::findColumnIndex(const Table& table, const std::string& colName) const {
    for (size_t i = 0; i < table.columns.size(); ++i) {
        if (table.columns[i].name == colName) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

void JoinExecutor::execute(const SelectQuery& query) {
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
        for (size_t i = 0; i < combinedCols.size(); ++i) {
            colIndices.push_back(i);
            displayNames.push_back(combinedCols[i].name);
        }
    } else {
        for (const auto& colName : query.selectColumns) {
            bool found = false;
            for (size_t i = 0; i < combinedCols.size(); ++i) {
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
