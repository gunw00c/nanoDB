#include "nanodb/nanodb.hpp"

#include <iostream>
#include <iomanip>
#include <algorithm>

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

bool NanoDB::matchesCondition(const Row& row, const Table& table, const Condition& cond) const {
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

    // Validate WHERE column exists (if present)
    if (query.where.hasCondition && findColumnIndex(*table, query.where.column) < 0) {
        std::cout << "Error: Column '" << query.where.column << "' not found.\n";
        return;
    }

    size_t updateCount = 0;
    for (auto& row : table->rows) {
        if (matchesCondition(row, *table, query.where)) {
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

    // Validate WHERE column exists (if present)
    if (query.where.hasCondition && findColumnIndex(*table, query.where.column) < 0) {
        std::cout << "Error: Column '" << query.where.column << "' not found.\n";
        return;
    }

    size_t beforeCount = table->rows.size();

    if (query.where.hasCondition) {
        // Conditional delete
        table->rows.erase(
            std::remove_if(table->rows.begin(), table->rows.end(),
                [this, table, &query](const Row& row) {
                    return matchesCondition(row, *table, query.where);
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
    const Table* table = catalog_.getTable(query.tableName);
    if (!table) {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
        return;
    }

    // Validate WHERE column exists (if present)
    if (query.where.hasCondition && findColumnIndex(*table, query.where.column) < 0) {
        std::cout << "Error: Column '" << query.where.column << "' not found.\n";
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

    // Print rows (with WHERE and LIMIT)
    size_t rowCount = 0;
    size_t maxRows = (query.limit > 0) ? static_cast<size_t>(query.limit) : table->rows.size();

    for (const auto& row : table->rows) {
        if (rowCount >= maxRows) break;

        // Check WHERE condition
        if (!matchesCondition(row, *table, query.where)) continue;

        for (size_t i = 0; i < colIndices.size(); ++i) {
            std::visit([](const auto& val) {
                std::cout << std::setw(15) << val;
            }, row[colIndices[i]]);
            if (i < colIndices.size() - 1) std::cout << " | ";
        }
        std::cout << "\n";
        ++rowCount;
    }

    std::cout << rowCount << " row(s) returned.\n";
}

} // namespace nanodb
