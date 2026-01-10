#include <iostream>

#include "nanodb/nanodb.hpp"

int main() {
    nanodb::NanoDB db;

    std::cout << "=== nanoDB Demo ===\n\n";

    // Create tables
    db.executeSQL("CREATE TABLE users (id INT, name STRING, age INT)");
    db.executeSQL("CREATE TABLE orders (id INT, user_id INT, product_name STRING)");
    std::cout << "\n";

    // Insert some data
    db.executeSQL("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.executeSQL("INSERT INTO users VALUES (2, 'Bob', 25)");
    db.executeSQL("INSERT INTO users VALUES (3, 'Charlie', 35)");
    db.executeSQL("INSERT INTO users VALUES (4, 'Diana', 28)");
    db.executeSQL("INSERT INTO users VALUES (5, 'Eve', 32)");
    db.executeSQL("INSERT INTO orders VALUES (1, 1, 'Laptop')");
    db.executeSQL("INSERT INTO orders VALUES (2, 2, 'Smartphone')");
    db.executeSQL("INSERT INTO orders VALUES (3, 3, 'Tablet')");
    std::cout << "\n";

    // SELECT * (all columns)
    std::cout << "--- SELECT * FROM users ---\n";
    db.executeSQL("SELECT * FROM users");
    std::cout << "\n";

    // SELECT specific columns
    std::cout << "--- SELECT name, age FROM users ---\n";
    db.executeSQL("SELECT name, age FROM users");
    std::cout << "\n";

    // SELECT with LIMIT
    std::cout << "--- SELECT * FROM users LIMIT 2 ---\n";
    db.executeSQL("SELECT * FROM users LIMIT 2");
    std::cout << "\n";

    // SELECT columns with LIMIT
    std::cout << "--- SELECT name FROM users LIMIT 3 ---\n";
    db.executeSQL("SELECT name FROM users LIMIT 3");
    std::cout << "\n";

    // DELETE all rows from orders
    std::cout << "--- DELETE FROM orders ---\n";
    db.executeSQL("DELETE FROM orders");
    db.executeSQL("SELECT * FROM orders");
    std::cout << "\n";

    // DROP TABLE
    std::cout << "--- DROP TABLE orders ---\n";
    db.executeSQL("DROP TABLE orders");
    db.executeSQL("SELECT * FROM orders");  // Should show error
    std::cout << "\n";

    // Final state
    std::cout << "--- Final: SELECT * FROM users ---\n";
    db.executeSQL("SELECT * FROM users");

    return 0;
}
