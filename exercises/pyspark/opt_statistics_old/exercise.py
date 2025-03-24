#!/usr/bin/env python

"""
Exercise
"""

import time
from pyspark.sql import SparkSession

# Initialize Spark Session with Hive support
spark = SparkSession.builder \
    .appName("Statistics Exercise - Unoptimized Query") \
    .enableHiveSupport() \
    .getOrCreate()

def run_sales_analysis():
    print("Running unoptimized query...")
    start_time = time.time()

    # Complex query without statistics
    result = spark.sql("""
        SELECT
            p.category,
            COUNT(DISTINCT s.id) as num_sales,
            SUM(s.quantity * p.price) as total_revenue,
            AVG(s.quantity * p.price) as avg_sale_value
        FROM sales s
        JOIN products p ON s.product_id = p.id
        GROUP BY p.category
        ORDER BY total_revenue DESC
    """)

    # Force execution and show results
    result.show()

    end_time = time.time()
    print(f"\nQuery execution time: {end_time - start_time:.2f} seconds")

    # Show the query plan
    print("\nLogical Plan:")
    spark.sql("""
        EXPLAIN
        SELECT
            p.category,
            COUNT(DISTINCT s.id) as num_sales,
            SUM(s.quantity * p.price) as total_revenue,
            AVG(s.quantity * p.price) as avg_sale_value
        FROM sales s
        JOIN products p ON s.product_id = p.id
        GROUP BY p.category
        ORDER BY total_revenue DESC
    """).show(truncate=False)

    # Show extended plan
    print("\nExtended Plan (including physical plan):")
    spark.sql("""
        EXPLAIN EXTENDED
        SELECT
            p.category,
            COUNT(DISTINCT s.id) as num_sales,
            SUM(s.quantity * p.price) as total_revenue,
            AVG(s.quantity * p.price) as avg_sale_value
        FROM sales s
        JOIN products p ON s.product_id = p.id
        GROUP BY p.category
        ORDER BY total_revenue DESC
    """).show(truncate=False)

if __name__ == "__main__":
    # Clear any existing cache
    spark.catalog.clearCache()

    # Run the analysis
    run_sales_analysis()

    spark.stop()
