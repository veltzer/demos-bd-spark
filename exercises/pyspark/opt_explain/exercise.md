# Exploring EXPLAIN in Spark SQL
---
## Learning Objectives
1. Understand different EXPLAIN types in Spark SQL
1. Learn to interpret query execution plans
1. Analyze join strategies and their costs
1. Identify optimization opportunities
---
## Prerequisites
- Python and PySpark installed
- Basic SQL knowledge
- Understanding of join operations
---
## Setup
1. Run the data creation script:
```bash
python create_data.py
```

This creates three tables:
- `orders` (1M rows)
- `customers` (1K rows)
- `products` (100 rows)
---
## Basic EXPLAIN Types

1. Simple EXPLAIN
```sql
-- Shows basic logical plan
EXPLAIN
SELECT * FROM orders WHERE amount > 500;
```

2. EXPLAIN EXTENDED
```sql
-- Shows additional information including logical and physical plans
EXPLAIN EXTENDED
SELECT * FROM orders WHERE amount > 500;
```

3. EXPLAIN CODEGEN
```sql
-- Shows the generated Java code
EXPLAIN CODEGEN
SELECT * FROM orders WHERE amount > 500;
```

4. EXPLAIN COST
```sql
-- Shows cost-based optimization details
EXPLAIN COST
SELECT * FROM orders WHERE amount > 500;
```

5. EXPLAIN FORMATTED
```sql
-- Shows formatted output with more details
EXPLAIN FORMATTED
SELECT * FROM orders WHERE amount > 500;
```
---
## Exploring Join Operations

1. Broadcast Join Example
```sql
-- Small table broadcast
EXPLAIN
SELECT c.segment, COUNT(*) as order_count
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.segment;
```

2. Sort-Merge Join Example
```sql
-- Larger tables merge
EXPLAIN
SELECT p.category, SUM(o.amount) as total_sales
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.category;
```

3. Multiple Joins
```sql
EXPLAIN COST
SELECT 
    c.country,
    p.category,
    SUM(o.amount) as total_sales,
    COUNT(DISTINCT c.customer_id) as customer_count
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
GROUP BY c.country, p.category;
```
---
## Analyzing Complex Queries

1. Window Functions
```sql
EXPLAIN FORMATTED
SELECT 
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as rolling_sum
FROM orders;
```

2. Subqueries
```sql
EXPLAIN EXTENDED
SELECT *
FROM (
    SELECT 
        customer_id,
        SUM(amount) as total_spent
    FROM orders
    GROUP BY customer_id
) spending
WHERE total_spent > (
    SELECT AVG(amount) * 10 FROM orders
);
```

3. Complex Aggregations
```sql
EXPLAIN COST
SELECT 
    c.segment,
    p.category,
    COUNT(*) as order_count,
    SUM(o.amount) as total_amount,
    AVG(o.amount) as avg_amount,
    COUNT(DISTINCT o.customer_id) as unique_customers
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
GROUP BY c.segment, p.category
HAVING COUNT(*) > 100;
```
---
## Understanding the Output

1. Key Components to Look For:
   - Project and Filter operations
   - Join strategies (Broadcast vs. Sort-Merge)
   - Partition information
   - Exchange operations (shuffles)
   - Statistics and cost estimates

2. Common Optimization Patterns:
   - Predicate pushdown
   - Column pruning
   - Partition pruning
   - Join reordering
---
## Exercise Tasks

1. Compare Plans
   - Run the same query with different EXPLAIN types
   - Note the different information provided
   - Understand when to use each type

2. Analyze Join Strategies
   - Try different join conditions
   - Compare broadcast vs. sort-merge joins
   - Observe how table sizes affect join selection

3. Optimization Investigation
   - Identify bottlenecks in complex queries
   - Find opportunities for optimization
   - Test different query formulations
---
## Advanced Challenges

1. Write and analyze a query that:
   - Uses window functions
   - Joins all three tables
   - Includes complex aggregations
   - Has subqueries
   - Compare its performance with a simpler version

2. Experiment with hints:
```sql
EXPLAIN COST
SELECT /*+ BROADCAST(customers) */
    c.segment, COUNT(*) as order_count
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.segment;
```

3. Compare different join orders:
```sql
EXPLAIN COST
SELECT /*+ LEADING(orders, customers, products) */
    c.country, p.category, SUM(o.amount)
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
GROUP BY c.country, p.category;
```
---
## Tips
1. Use `EXPLAIN FORMATTED` for most detailed output
2. Compare `COST` vs actual performance
3. Look for shuffle operations in complex queries
4. Watch for skewed data handling
5. Consider partition and predicate pushdown opportunities
---
## Cleanup
```python
spark.sql("DROP TABLE IF EXISTS orders")
spark.sql("DROP TABLE IF EXISTS customers")
spark.sql("DROP TABLE IF EXISTS products")
```
