#pragma once

#include "nanodb/core/types.hpp"
#include "nanodb/catalog/catalog.hpp"

namespace nanodb {

class JoinExecutor {
public:
    explicit JoinExecutor(Catalog& catalog);

    void execute(const SelectQuery& query);

private:
    int findColumnIndex(const Table& table, const std::string& colName) const;

    Catalog& catalog_;
};

} // namespace nanodb
