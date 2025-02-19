# Table Statistics Exercise
---
## Introduction
In this exercise, you'll learn how table statistics affect query performance in Spark SQL
---
## Setup
First, let's create our test tables with sample data:

```python
# Create large tables for testing
spark.sql("""
    CREATE TABLE products (
        id INT,
        name STRING,
        category STRING,
        price DOUBLE
    ) USING PARQUET
""")

spark.sql("""
    CREATE TABLE sales (
        id INT,
        product_id INT,
        quantity INT,
        sale_date DATE
    ) USING PARQUET
""")

# Insert sample data
spark.sql("""
    INSERT INTO products
    SELECT
        id,
        concat('Product_', cast(id as string)) as name,
        concat('Category_', cast(id % 10 as string)) as category,
        RAND() * 1000 as price
    FROM range(1000000)
""")

spark.sql("""
    INSERT INTO sales
    SELECT
        id,
        CAST(RAND() * 1000000 as INT) as product_id,
        CAST(RAND() * 100 as INT) as quantity,
        current_date() - CAST(RAND() * 365 as INT) as sale_date
    FROM range(5000000)
""")
```
---
## Problem Statement
You have inherited a query that performs poorly. The query calculates total sales by category:

```python
# Initial slow query
def run_sales_analysis():
    start_time = time.time()

    result = spark.sql("""
        SELECT
            p.category,
            COUNT(DISTINCT s.id) as num_sales,
            SUM(s.quantity * p.price) as total_revenue
        FROM sales s
        JOIN products p ON s.product_id = p.id
        GROUP BY p.category
        ORDER BY total_revenue DESC
    """)

    result.show()

    end_time = time.time()
    print(f"Query execution time: {end_time - start_time:.2f} seconds")

# Run the slow query
run_sales_analysis()
```
---
## Task 1: Analyze Current Performance
1. Run the query and note the execution time
1. Examine the query plan:
```python
spark.sql("""
    EXPLAIN ANALYZED
    SELECT
        p.category,
        COUNT(DISTINCT s.id) as num_sales,
        SUM(s.quantity * p.price) as total_revenue
    FROM sales s
    JOIN products p ON s.product_id = p.id
    GROUP BY p.category
    ORDER BY total_revenue DESC
""").show(truncate=False)
```

---
## Task 2: Identify Issues
Look at the query plan output and identify:
1. Are statistics being used for join optimization?
1. What join strategy is being chosen?
1. Are there any shuffle operations that could be optimized?
---
## Task 3: Apply Statistics
Add the appropriate statistics computation commands:
```python
# Add your statistics commands here
# Hint: You need to compute statistics for both tables
# Hint: Consider both table-level and column-level statistics
```
---
## Task 4: Verify Improvement
1. Run the query again with statistics
1. Compare the execution times
1. Examine the new query plan

```python
# Modified version with statistics
def run_optimized_sales_analysis():
    # Add statistics computation here

    start_time = time.time()

    result = spark.sql("""
        SELECT
            p.category,
            COUNT(DISTINCT s.id) as num_sales,
            SUM(s.quantity * p.price) as total_revenue
        FROM sales s
        JOIN products p ON s.product_id = p.id
        GROUP BY p.category
        ORDER BY total_revenue DESC
    """)

    result.show()

    end_time = time.time()
    print(f"Query execution time: {end_time - start_time:.2f} seconds")
```
---
## Solution
Here's the optimized version:

```python
def run_optimized_sales_analysis():
    # Compute table statistics
    spark.sql("ANALYZE TABLE products COMPUTE STATISTICS")
    spark.sql("ANALYZE TABLE sales COMPUTE STATISTICS")

    # Compute column statistics
    spark.sql("""
        ANALYZE TABLE products COMPUTE STATISTICS
        FOR COLUMNS id, category, price
    """)
    spark.sql("""
        ANALYZE TABLE sales COMPUTE STATISTICS
        FOR COLUMNS product_id, quantity
    """)

    start_time = time.time()

    result = spark.sql("""
        SELECT
            p.category,
            COUNT(DISTINCT s.id) as num_sales,
            SUM(s.quantity * p.price) as total_revenue
        FROM sales s
        JOIN products p ON s.product_id = p.id
        GROUP BY p.category
        ORDER BY total_revenue DESC
    """)

    result.show()

    end_time = time.time()
    print(f"Query execution time: {end_time - start_time:.2f} seconds")

# Run optimized query
run_optimized_sales_analysis()
```
---
## Expected Improvements
1. Better join strategy selection due to size statistics
1. Improved predicate pushdown
1. More accurate resource allocation
1. Potential reduction in shuffle operations
---
## Discussion Questions
1. Why did computing statistics improve the performance?
1. What specific changes do you see in the query plan?
1. When should statistics be recomputed?
1. What are the trade-offs of computing detailed statistics?
---
## Additional Challenges
1. Try different join conditions and observe the impact
1. Add more columns and analyze the statistics impact
1. Experiment with different data distributions
1. Compare performance with and without column statistics
