#include "nanodb/executor/ddl_executor.hpp"

#include <iostream>

namespace nanodb {

DDLExecutor::DDLExecutor(Catalog& catalog) : catalog_(catalog) {}

void DDLExecutor::executeCreateTable(const CreateQuery& query) {
    if (catalog_.createTable(query.tableName, query.columns)) {
        std::cout << "Table '" << query.tableName << "' created.\n";
    } else {
        std::cout << "Error: Table '" << query.tableName << "' already exists.\n";
    }
}

void DDLExecutor::executeDropTable(const DropQuery& query) {
    if (catalog_.dropTable(query.tableName)) {
        std::cout << "Table '" << query.tableName << "' dropped.\n";
    } else {
        std::cout << "Error: Table '" << query.tableName << "' does not exist.\n";
    }
}

} // namespace nanodb
