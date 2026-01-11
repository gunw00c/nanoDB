#pragma once

#include <string>
#include <memory>

#include "nanodb/core/types.hpp"
#include "nanodb/catalog/catalog.hpp"
#include "nanodb/parser/sql_parser.hpp"
#include "nanodb/executor/ddl_executor.hpp"
#include "nanodb/executor/dml_executor.hpp"
#include "nanodb/executor/select_executor.hpp"
#include "nanodb/executor/aggregate_executor.hpp"
#include "nanodb/executor/join_executor.hpp"

namespace nanodb {

class NanoDB {
public:
    NanoDB();
    ~NanoDB() = default;

    void executeSQL(const std::string& sql);

private:
    Catalog catalog_;

    // Executors
    std::unique_ptr<DDLExecutor> ddlExecutor_;
    std::unique_ptr<DMLExecutor> dmlExecutor_;
    std::unique_ptr<SelectExecutor> selectExecutor_;
    std::unique_ptr<AggregateExecutor> aggregateExecutor_;
    std::unique_ptr<JoinExecutor> joinExecutor_;
};

} // namespace nanodb
