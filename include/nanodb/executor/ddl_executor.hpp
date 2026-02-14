#pragma once

#include "nanodb/core/types.hpp"
#include "nanodb/catalog/catalog.hpp"

namespace nanodb {

class DDLExecutor {
public:
    explicit DDLExecutor(Catalog& catalog);

    void executeCreateTable(const CreateQuery& query);
    void executeDropTable(const DropQuery& query);

private:
    Catalog& catalog_;
};

} // namespace nanodb
