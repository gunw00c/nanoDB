#include "nanodb/catalog/catalog.hpp"

namespace nanodb {

bool Catalog::createTable(const std::string& name, const std::vector<Column>& columns) {
    if (tableExists(name)) {
        return false;
    }
    Table table;
    table.name = name;
    table.columns = columns;
    tables_[name] = table;
    return true;
}

bool Catalog::dropTable(const std::string& name) {
    auto it = tables_.find(name);
    if (it == tables_.end()) {
        return false;
    }
    tables_.erase(it);
    return true;
}

bool Catalog::tableExists(const std::string& name) const {
    return tables_.find(name) != tables_.end();
}

Table* Catalog::getTable(const std::string& name) {
    auto it = tables_.find(name);
    if (it != tables_.end()) {
        return &it->second;
    }
    return nullptr;
}

const Table* Catalog::getTable(const std::string& name) const {
    auto it = tables_.find(name);
    if (it != tables_.end()) {
        return &it->second;
    }
    return nullptr;
}

} // namespace nanodb
