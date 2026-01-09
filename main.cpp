#include <iostream>

#include "nanodb/nanodb.hpp"

int main() {
    nanodb::NanoDB db;

    std::cout << "=== nanoDB Demo ===\n\n";

    // Create a table
    db.executeSQL("CREATE TABLE users (id INT, name STRING, age INT)");
    std::cout << "\n";

    // Insert some data
    db.executeSQL("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.executeSQL("INSERT INTO users VALUES (2, 'Bob', 25)");
    db.executeSQL("INSERT INTO users VALUES (3, 'Charlie', 35)");
    std::cout << "\n";

    // Select all data
    db.executeSQL("SELECT * FROM users");

    return 0;
}
