#include <iostream>

#include "nanodb/nanodb.hpp"

int main() {
    nanodb::NanoDB db;
    while (true) {
        std::cout << "nanodb> ";
        std::string sql;
        std::getline(std::cin, sql);
        if (sql == "exit" || sql == "quit") {
            break;
        }
        db.executeSQL(sql);
    }

    return 0;
}
