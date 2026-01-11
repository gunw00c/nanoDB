#pragma once

#include <string>
#include <vector>
#include <unordered_map>

#include "nanodb/core/types.hpp"

namespace nanodb {

class Catalog {
public:
    Catalog() = default;
    ~Catalog() = default; 

    bool createTable(const std::string& name, const std::vector<Column>& columns);
    bool dropTable(const std::string& name);
    bool tableExists(const std::string& name) const;
    // Returns nullptr if table does not exist
    Table* getTable(const std::string& name);
    // Const version
    const Table* getTable(const std::string& name) const;

private:
    std::unordered_map<std::string, Table> tables_;
};

} // namespace nanodb
