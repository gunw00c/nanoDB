#include <iostream>

#include "nanodb/nanodb.hpp"

int main() {
    nanodb::NanoDB db;

    std::cout << "=== nanoDB Demo ===\n\n";

    // Create table
    db.executeSQL("CREATE TABLE users (id INT, name STRING, age INT, city STRING)");
    std::cout << "\n";

    // Insert data
    db.executeSQL("INSERT INTO users VALUES (1, 'Alice', 30, 'NYC')");
    db.executeSQL("INSERT INTO users VALUES (2, 'Bob', 25, 'LA')");
    db.executeSQL("INSERT INTO users VALUES (3, 'Charlie', 35, 'NYC')");
    db.executeSQL("INSERT INTO users VALUES (4, 'Diana', 28, 'Chicago')");
    db.executeSQL("INSERT INTO users VALUES (5, 'Eve', 30, 'LA')");
    db.executeSQL("INSERT INTO users VALUES (6, 'Frank', 22, 'NYC')");
    db.executeSQL("INSERT INTO users VALUES (7, 'Grace', 35, 'Chicago')");
    std::cout << "\n";

    // Show all data
    std::cout << "--- All users ---\n";
    db.executeSQL("SELECT * FROM users");

    // ==================== WHERE with AND ====================
    std::cout << "\n--- WHERE with AND: age > 25 AND city = 'NYC' ---\n";
    db.executeSQL("SELECT * FROM users WHERE age > 25 AND city = 'NYC'");

    // ==================== WHERE with OR ====================
    std::cout << "\n--- WHERE with OR: city = 'LA' OR city = 'Chicago' ---\n";
    db.executeSQL("SELECT * FROM users WHERE city = 'LA' OR city = 'Chicago'");

    // ==================== WHERE with AND and OR ====================
    std::cout << "\n--- WHERE with AND/OR: age >= 30 AND city = 'NYC' OR name = 'Bob' ---\n";
    db.executeSQL("SELECT * FROM users WHERE age >= 30 AND city = 'NYC' OR name = 'Bob'");

    // ==================== ORDER BY ASC ====================
    std::cout << "\n--- ORDER BY age ASC ---\n";
    db.executeSQL("SELECT name, age FROM users ORDER BY age ASC");

    // ==================== ORDER BY DESC ====================
    std::cout << "\n--- ORDER BY age DESC ---\n";
    db.executeSQL("SELECT name, age FROM users ORDER BY age DESC");

    // ==================== ORDER BY with string ====================
    std::cout << "\n--- ORDER BY name ASC ---\n";
    db.executeSQL("SELECT name, city FROM users ORDER BY name ASC");

    // ==================== ORDER BY with WHERE ====================
    std::cout << "\n--- WHERE age > 25 ORDER BY age DESC ---\n";
    db.executeSQL("SELECT name, age FROM users WHERE age > 25 ORDER BY age DESC");

    // ==================== COUNT(*) ====================
    std::cout << "\n--- COUNT(*) ---\n";
    db.executeSQL("SELECT COUNT(*) FROM users");

    // ==================== COUNT(*) with WHERE ====================
    std::cout << "\n--- COUNT(*) WHERE city = 'NYC' ---\n";
    db.executeSQL("SELECT COUNT(*) FROM users WHERE city = 'NYC'");

    // ==================== SUM ====================
    std::cout << "\n--- SUM(age) ---\n";
    db.executeSQL("SELECT SUM(age) FROM users");

    // ==================== AVG ====================
    std::cout << "\n--- AVG(age) ---\n";
    db.executeSQL("SELECT AVG(age) FROM users");

    // ==================== AVG with WHERE ====================
    std::cout << "\n--- AVG(age) WHERE city = 'NYC' ---\n";
    db.executeSQL("SELECT AVG(age) FROM users WHERE city = 'NYC'");

    // ==================== SELECT DISTINCT ====================
    std::cout << "\n--- SELECT DISTINCT city ---\n";
    db.executeSQL("SELECT DISTINCT city FROM users");

    // ==================== SELECT DISTINCT with multiple columns ====================
    std::cout << "\n--- SELECT DISTINCT age ---\n";
    db.executeSQL("SELECT DISTINCT age FROM users");

    // ==================== DISTINCT with ORDER BY ====================
    std::cout << "\n--- SELECT DISTINCT city ORDER BY city ASC ---\n";
    db.executeSQL("SELECT DISTINCT city FROM users ORDER BY city ASC");

    // ==================== Complex query ====================
    std::cout << "\n--- Complex: WHERE age >= 25 AND age <= 35 ORDER BY name LIMIT 4 ---\n";
    db.executeSQL("SELECT name, age, city FROM users WHERE age >= 25 AND age <= 35 ORDER BY name LIMIT 4");

    // ==================== DELETE with AND ====================
    std::cout << "\n--- DELETE WHERE age < 25 AND city = 'NYC' ---\n";
    db.executeSQL("DELETE FROM users WHERE age < 25 AND city = 'NYC'");
    db.executeSQL("SELECT * FROM users");

    // ==================== UPDATE with OR ====================
    std::cout << "\n--- UPDATE SET age = 40 WHERE name = 'Alice' OR name = 'Bob' ---\n";
    db.executeSQL("UPDATE users SET age = 40 WHERE name = 'Alice' OR name = 'Bob'");
    db.executeSQL("SELECT * FROM users WHERE name = 'Alice' OR name = 'Bob'");

    std::cout << "\n--- Final state ---\n";
    db.executeSQL("SELECT * FROM users ORDER BY id");

    return 0;
}
