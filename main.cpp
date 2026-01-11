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

    // ==================== GROUP BY ====================
    std::cout << "\n\n=== GROUP BY and HAVING Demo ===\n";

    std::cout << "\n--- GROUP BY city, COUNT(*) ---\n";
    db.executeSQL("SELECT city, COUNT(*) FROM users GROUP BY city");

    std::cout << "\n--- GROUP BY city, SUM(age) ---\n";
    db.executeSQL("SELECT city, SUM(age) FROM users GROUP BY city");

    std::cout << "\n--- GROUP BY city, AVG(age) ---\n";
    db.executeSQL("SELECT city, AVG(age) FROM users GROUP BY city");

    // ==================== GROUP BY with HAVING ====================
    std::cout << "\n--- GROUP BY city HAVING COUNT(*) > 1 ---\n";
    db.executeSQL("SELECT city, COUNT(*) FROM users GROUP BY city HAVING COUNT(*) > 1");

    std::cout << "\n--- GROUP BY city HAVING AVG(age) >= 30 ---\n";
    db.executeSQL("SELECT city, AVG(age) FROM users GROUP BY city HAVING AVG(age) >= 30");

    // ==================== JOIN Demo ====================
    std::cout << "\n\n=== JOIN Demo ===\n";

    // Create orders table for JOIN demo
    db.executeSQL("CREATE TABLE orders (order_id INT, user_id INT, product STRING, amount INT)");
    db.executeSQL("INSERT INTO orders VALUES (101, 1, 'Laptop', 1200)");
    db.executeSQL("INSERT INTO orders VALUES (102, 2, 'Phone', 800)");
    db.executeSQL("INSERT INTO orders VALUES (103, 1, 'Tablet', 500)");
    db.executeSQL("INSERT INTO orders VALUES (104, 4, 'Monitor', 350)");
    db.executeSQL("INSERT INTO orders VALUES (105, 99, 'Keyboard', 100)");  // user_id 99 doesn't exist
    std::cout << "\n";

    std::cout << "--- Orders table ---\n";
    db.executeSQL("SELECT * FROM orders");

    // INNER JOIN
    std::cout << "\n--- INNER JOIN users and orders ON id = user_id ---\n";
    db.executeSQL("SELECT * FROM users INNER JOIN orders ON id = user_id");

    // LEFT JOIN
    std::cout << "\n--- LEFT JOIN users and orders ON id = user_id ---\n";
    db.executeSQL("SELECT name, product, amount FROM users LEFT JOIN orders ON id = user_id");

    // RIGHT JOIN
    std::cout << "\n--- RIGHT JOIN users and orders ON id = user_id ---\n";
    db.executeSQL("SELECT name, product, amount FROM users RIGHT JOIN orders ON id = user_id");

    std::cout << "\n=== Demo Complete ===\n";

    return 0;
}
