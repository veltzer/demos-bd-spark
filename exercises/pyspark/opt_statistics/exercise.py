#!/usr/bin/env python

"""
Slow Query Script - Without ANALYZE TABLE

This script executes a complex query that joins the customers and orders tables
with multiple filters. Without table statistics, Spark makes suboptimal decisions
about join strategies, predicate pushdown, and partition pruning.
"""

from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import col, sum, avg, count, desc

# Initialize Spark Session with Hive support to access the saved tables
spark = SparkSession.builder \
    .appName("PySpark Slow Query - Without ANALYZE TABLE") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10m") \
    .config("spark.sql.shuffle.partitions", "100") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def run_query():
    print("Running query WITHOUT collected statistics...")
    print("-" * 80)
    
    # Record start time
    start_time = time.time()
    
    # QUERY: Find high-value customers by region with their ordering patterns
    # This query is complex and joins large tables with filtering
    result = spark.sql("""
        SELECT 
            c.region_id,
            COUNT(DISTINCT c.customer_id) as customer_count,
            SUM(o.order_total) as total_sales,
            AVG(o.order_total) as avg_order_value,
            COUNT(o.order_id) / COUNT(DISTINCT c.customer_id) as avg_orders_per_customer
        FROM 
            customers c
        JOIN 
            orders o ON c.customer_id = o.customer_id
        WHERE 
            c.active = true 
            AND c.account_balance > 1000
            AND o.order_date > date_sub(current_date(), 365)
            AND o.status = 'COMPLETED'
        GROUP BY 
            c.region_id
        ORDER BY 
            total_sales DESC
    """)
    
    # Execute and collect results
    result_rows = result.collect()
    
    # Record end time
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Output results
    print(f"Query Results:")
    result.show()
    
    print(f"\nExecution Statistics:")
    print(f"Execution time: {execution_time:.2f} seconds")
    
    # Get and print query execution details
    # explain_output = result._jdf.queryExecution().toString()
    # print("\nQuery Execution Plan:")
    # print("-" * 80)
    # print(explain_output)
    # print("-" * 80)
    
    return execution_time

if __name__ == "__main__":
    # Check if tables exist
    tables = spark.catalog.listTables()
    table_names = [t.name for t in tables]
    
    if "customers" not in table_names or "orders" not in table_names:
        print("Error: Required tables 'customers' and 'orders' not found.")
        print("Please run the data_generation.py script first.")
        spark.stop()
        exit(1)
        
    # Run query and measure performance
    execution_time = run_query()
    
    # Output summary
    print(f"\nQuery completed in {execution_time:.2f} seconds WITHOUT statistics")
    print("Next, try running optimized_query.py to see the performance improvement")
    print("with ANALYZE TABLE")
    
    spark.stop()
