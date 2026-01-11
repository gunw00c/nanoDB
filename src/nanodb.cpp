#include "nanodb/nanodb.hpp"

#include <iostream>

namespace nanodb {

NanoDB::NanoDB()
    : ddlExecutor_(std::make_unique<DDLExecutor>(catalog_))
    , dmlExecutor_(std::make_unique<DMLExecutor>(catalog_))
    , selectExecutor_(std::make_unique<SelectExecutor>(catalog_))
    , aggregateExecutor_(std::make_unique<AggregateExecutor>(catalog_))
    , joinExecutor_(std::make_unique<JoinExecutor>(catalog_))
{}

void NanoDB::executeSQL(const std::string& sql) {
    Query query = SQLParser::parse(sql);

    if (query.type == "CREATE") {
        ddlExecutor_->executeCreateTable(query);
    } else if (query.type == "DROP") {
        ddlExecutor_->executeDropTable(query);
    } else if (query.type == "INSERT") {
        dmlExecutor_->executeInsert(query);
    } else if (query.type == "UPDATE") {
        dmlExecutor_->executeUpdate(query);
    } else if (query.type == "DELETE") {
        dmlExecutor_->executeDelete(query);
    } else if (query.type == "SELECT") {
        // Route to appropriate SELECT handler
        if (query.join.hasJoin) {
            joinExecutor_->execute(query);
        } else if (query.groupBy.hasGroupBy) {
            aggregateExecutor_->executeWithGroupBy(query);
        } else if (!query.aggregates.empty()) {
            aggregateExecutor_->execute(query);
        } else {
            selectExecutor_->execute(query);
        }
    } else {
        std::cout << "Error: Unknown SQL command\n";
    }
}

} // namespace nanodb
