#include "nanodb/nanodb.hpp"

#include <iostream>
#include <iomanip>

namespace nanodb {

void NanoDB::executeSQL(const std::string& sql) {
    Query query = SQLParser::parse(sql);

    if (query.type == "CREATE") {
        handleCreateTable(query);
    } else if (query.type == "INSERT") {
        handleInsert(query);
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

void NanoDB::handleSelect(const Query& query) {
    const Table* table = catalog_.getTable(query.tableName);
    if (!table) {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
        return;
    }

    // Print header
    for (size_t i = 0; i < table->columns.size(); ++i) {
        std::cout << std::setw(15) << table->columns[i].name;
        if (i < table->columns.size() - 1) std::cout << " | ";
    }
    std::cout << "\n";

    // Print separator
    for (size_t i = 0; i < table->columns.size(); ++i) {
        std::cout << std::string(15, '-');
        if (i < table->columns.size() - 1) std::cout << "-+-";
    }
    std::cout << "\n";

    // Print rows
    for (const auto& row : table->rows) {
        for (size_t i = 0; i < row.size(); ++i) {
            std::visit([](const auto& val) {
                std::cout << std::setw(15) << val;
            }, row[i]);
            if (i < row.size() - 1) std::cout << " | ";
        }
        std::cout << "\n";
    }

    std::cout << table->rows.size() << " row(s) returned.\n";
}

} // namespace nanodb
