# Spark SQL Shell Exercise
---
## Learning Objectives
1. Learn to start and use the Spark SQL shell
1. Create and populate tables
1. Practice writing SQL queries
1. Understand query execution plans
---
## Prerequisites
- Apache Spark installed
- Basic SQL knowledge
- Terminal/Command line familiarity
---
## Exercise Setup
First, create a file named `create_tables.sql` with the following content:

```sql
-- Drop existing tables if they exist
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS customers;

-- Create products table
CREATE TABLE products (
    id INT,
    name STRING,
    category STRING,
    price DECIMAL(10,2)
) USING PARQUET;

-- Create customers table
CREATE TABLE customers (
    id INT,
    name STRING,
    country STRING,
    segment STRING
) USING PARQUET;

-- Create sales table
CREATE TABLE sales (
    id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    sale_date DATE
) USING PARQUET;

-- Insert sample data into products
INSERT INTO products VALUES
    (1, 'Laptop', 'Electronics', 999.99),
    (2, 'Smartphone', 'Electronics', 499.99),
    (3, 'Desk Chair', 'Furniture', 199.99),
    (4, 'Coffee Maker', 'Appliances', 79.99),
    (5, 'Headphones', 'Electronics', 149.99);

-- Insert sample data into customers
INSERT INTO customers VALUES
    (1, 'John Doe', 'USA', 'Consumer'),
    (2, 'Jane Smith', 'Canada', 'Business'),
    (3, 'Bob Johnson', 'USA', 'Consumer'),
    (4, 'Alice Brown', 'UK', 'Business'),
    (5, 'Charlie Wilson', 'Canada', 'Consumer');

-- Insert sample data into sales using TO_DATE
INSERT INTO sales
SELECT
    id,
    customer_id,
    product_id,
    quantity,
    TO_DATE(sale_date) as sale_date
FROM VALUES
    (1, 1, 1, 1, '2024-01-15'),
    (2, 2, 1, 2, '2024-01-16'),
    (3, 3, 2, 1, '2024-01-16'),
    (4, 4, 3, 3, '2024-01-17'),
    (5, 5, 4, 2, '2024-01-17'),
    (6, 1, 5, 1, '2024-01-18'),
    (7, 2, 2, 1, '2024-01-18'),
    (8, 3, 3, 2, '2024-01-19'),
    (9, 4, 4, 1, '2024-01-19'),
    (10, 5, 5, 3, '2024-01-20') AS t(id, customer_id, product_id, quantity, sale_date);
```

Also create a cleanup script named `delete_tables.sql`:

```sql
-- Drop all tables created in the exercise
DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

-- Verify tables are dropped
SHOW TABLES;
```
---
## Exercise Steps

1. Start the Spark SQL shell:
```bash
spark-sql
```

1. Run the setup script:
```sql
source create_tables.sql;
```

1. Verify the tables were created:
```sql
SHOW TABLES;
```
---
## Tasks

1. Basic Queries
   ```sql
   -- Examine the schema of each table
   DESCRIBE products;
   DESCRIBE customers;
   DESCRIBE sales;

   -- View sample data from each table
   SELECT * FROM products LIMIT 3;
   SELECT * FROM customers LIMIT 3;
   SELECT * FROM sales LIMIT 3;
   ```

1. Write a query to find total sales by product category
   - Use joins to combine the tables
   - Calculate revenue (price * quantity)
   - Group by category

1. Examine query execution
   ```sql
   -- Use EXPLAIN to see the query plan
   EXPLAIN
   SELECT p.category, SUM(p.price * s.quantity) as revenue
   FROM sales s
   JOIN products p ON s.product_id = p.id
   GROUP BY p.category;

   -- Use EXPLAIN COST to see cost estimates
   EXPLAIN COST
   SELECT p.category, SUM(p.price * s.quantity) as revenue
   FROM sales s
   JOIN products p ON s.product_id = p.id
   GROUP BY p.category;
   ```
---
## Challenge Tasks

1. Find the top customer by revenue in each country
1. Calculate daily sales trends by product category
1. Find products that have never been sold
1. Identify the most common product pairs bought together
---
## Solutions

Here's the solution for the first challenge task:
```sql
WITH customer_revenue AS (
    SELECT
        c.country,
        c.name as customer_name,
        SUM(p.price * s.quantity) as total_revenue,
        RANK() OVER (PARTITION BY c.country ORDER BY SUM(p.price * s.quantity) DESC) as rnk
    FROM sales s
    JOIN customers c ON s.customer_id = c.id
    JOIN products p ON s.product_id = p.id
    GROUP BY c.country, c.name
)
SELECT country, customer_name, total_revenue
FROM customer_revenue
WHERE rnk = 1
ORDER BY total_revenue DESC;
```
---
## Tips
1. Use `Ctrl + D` or type `exit;` to exit the shell
1. Use semicolons to end SQL statements
1. Use arrow keys to navigate command history
1. Type `help;` for available commands
1. Remember to use TO_DATE when inserting dates
1. Clean up using `delete_tables.sql` when done
---
## Next Steps
1. Try modifying the queries to answer different business questions
1. Experiment with different JOIN types
1. Add more complex window functions
1. Practice writing subqueries and CTEs
1. Try adding more data with different date ranges
