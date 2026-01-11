#pragma once

#include "nanodb/core/types.hpp"
#include "nanodb/catalog/catalog.hpp"

namespace nanodb {

class DDLExecutor {
public:
    explicit DDLExecutor(Catalog& catalog);

    void executeCreateTable(const Query& query);
    void executeDropTable(const Query& query);

private:
    Catalog& catalog_;
};

} // namespace nanodb
