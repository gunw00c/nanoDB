#include "nanodb/nanodb.hpp"

#include <iostream>
#include <iomanip>

namespace nanodb {

void NanoDB::executeSQL(const std::string& sql) {
    Query query = SQLParser::parse(sql);

    if (query.type == "CREATE") {
        handleCreateTable(query);
    } else if (query.type == "DROP") {
        handleDropTable(query);
    } else if (query.type == "INSERT") {
        handleInsert(query);
    } else if (query.type == "DELETE") {
        handleDelete(query);
    } else if (query.type == "SELECT") {
        handleSelect(query);
    } else {
        std::cout << "Error: Unknown SQL command\n";
    }
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

    if (query.values.size() != table->columns.size()) {
        std::cout << "Error: Column count mismatch. Expected "
                  << table->columns.size() << ", got " << query.values.size() << ".\n";
        return;
    }

    table->rows.push_back(query.values);
    std::cout << "1 row inserted.\n";
}

void NanoDB::handleDelete(const Query& query) {
    Table* table = catalog_.getTable(query.tableName);
    if (!table) {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
        return;
    }

    size_t rowCount = table->rows.size();
    table->rows.clear();
    std::cout << rowCount << " row(s) deleted.\n";
}

void NanoDB::handleSelect(const Query& query) {
    const Table* table = catalog_.getTable(query.tableName);
    if (!table) {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
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
            bool found = false;
            for (size_t i = 0; i < table->columns.size(); ++i) {
                if (table->columns[i].name == colName) {
                    colIndices.push_back(i);
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
    size_t maxRows = (query.limit > 0) ? static_cast<size_t>(query.limit) : table->rows.size();

    for (const auto& row : table->rows) {
        if (rowCount >= maxRows) break;

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
