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

-- Verify the data
SELECT 'Products count: ' as table_info, COUNT(*) as count FROM products
UNION ALL
SELECT 'Customers count: ', COUNT(*) FROM customers
UNION ALL
SELECT 'Sales count: ', COUNT(*) FROM sales;

-- Show sample data
SELECT 'Sample products:' as sample_data;
SELECT * FROM products LIMIT 3;

SELECT 'Sample customers:' as sample_data;
SELECT * FROM customers LIMIT 3;

SELECT 'Sample sales:' as sample_data;
SELECT * FROM sales LIMIT 3;