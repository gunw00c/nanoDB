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
    auto query = SQLParser::parse(sql);
    if (!query) {
        std::cout << "Error: Unknown SQL command\n";
        return;
    }

    switch (query->type) {
        case QueryType::CREATE: {
            auto* q = static_cast<CreateQuery*>(query.get());
            ddlExecutor_->executeCreateTable(*q);
            break;
        }
        case QueryType::DROP: {
            auto* q = static_cast<DropQuery*>(query.get());
            ddlExecutor_->executeDropTable(*q);
            break;
        }
        case QueryType::INSERT: {
            auto* q = static_cast<InsertQuery*>(query.get());
            dmlExecutor_->executeInsert(*q);
            break;
        }
        case QueryType::UPDATE: {
            auto* q = static_cast<UpdateQuery*>(query.get());
            dmlExecutor_->executeUpdate(*q);
            break;
        }
        case QueryType::DELETE_Q: {
            auto* q = static_cast<DeleteQuery*>(query.get());
            dmlExecutor_->executeDelete(*q);
            break;
        }
        case QueryType::SELECT: {
            auto* q = static_cast<SelectQuery*>(query.get());
            if (q->join.hasJoin) {
                joinExecutor_->execute(*q);
            } else if (q->groupBy.hasGroupBy) {
                aggregateExecutor_->executeWithGroupBy(*q);
            } else if (!q->aggregates.empty()) {
                aggregateExecutor_->execute(*q);
            } else {
                selectExecutor_->execute(*q);
            }
            break;
        }
    }
}

} // namespace nanodb
