#include <iostream>

#include "nanodb/nanodb.hpp"

int main() {
    nanodb::NanoDB db;

    std::cout << "=== nanoDB Demo ===\n\n";

    // Create table
    db.executeSQL("CREATE TABLE users (id INT, name STRING, age INT)");
    std::cout << "\n";

    // Standard INSERT
    db.executeSQL("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.executeSQL("INSERT INTO users VALUES (2, 'Bob', 25)");
    db.executeSQL("INSERT INTO users VALUES (3, 'Charlie', 35)");
    db.executeSQL("INSERT INTO users VALUES (4, 'Diana', 28)");
    db.executeSQL("INSERT INTO users VALUES (5, 'Eve', 22)");

    // INSERT with explicit columns
    std::cout << "\n--- INSERT with explicit columns ---\n";
    db.executeSQL("INSERT INTO users (id, name) VALUES (6, 'Frank')");
    db.executeSQL("SELECT * FROM users WHERE id = 6");

    // SELECT all
    std::cout << "\n--- SELECT * FROM users ---\n";
    db.executeSQL("SELECT * FROM users");

    // SELECT with WHERE (equality)
    std::cout << "\n--- SELECT * FROM users WHERE name = 'Alice' ---\n";
    db.executeSQL("SELECT * FROM users WHERE name = 'Alice'");

    // SELECT with WHERE (comparison operators)
    std::cout << "\n--- SELECT * FROM users WHERE age > 25 ---\n";
    db.executeSQL("SELECT * FROM users WHERE age > 25");

    std::cout << "\n--- SELECT * FROM users WHERE age <= 28 ---\n";
    db.executeSQL("SELECT * FROM users WHERE age <= 28");

    std::cout << "\n--- SELECT name, age FROM users WHERE age >= 30 ---\n";
    db.executeSQL("SELECT name, age FROM users WHERE age >= 30");

    // SELECT with WHERE and LIMIT
    std::cout << "\n--- SELECT * FROM users WHERE age > 20 LIMIT 3 ---\n";
    db.executeSQL("SELECT * FROM users WHERE age > 20 LIMIT 3");

    // UPDATE without WHERE (all rows)
    std::cout << "\n--- UPDATE users SET age = 99 WHERE name = 'Frank' ---\n";
    db.executeSQL("UPDATE users SET age = 99 WHERE name = 'Frank'");
    db.executeSQL("SELECT * FROM users WHERE name = 'Frank'");

    // UPDATE with WHERE
    std::cout << "\n--- UPDATE users SET age = 40 WHERE id = 1 ---\n";
    db.executeSQL("UPDATE users SET age = 40 WHERE id = 1");
    db.executeSQL("SELECT * FROM users WHERE id = 1");

    // DELETE with WHERE
    std::cout << "\n--- DELETE FROM users WHERE age < 25 ---\n";
    db.executeSQL("DELETE FROM users WHERE age < 25");
    db.executeSQL("SELECT * FROM users");

    // DELETE specific row
    std::cout << "\n--- DELETE FROM users WHERE name = 'Frank' ---\n";
    db.executeSQL("DELETE FROM users WHERE name = 'Frank'");
    db.executeSQL("SELECT * FROM users");

    // Final state
    std::cout << "\n--- Final state ---\n";
    db.executeSQL("SELECT * FROM users");

    return 0;
}
