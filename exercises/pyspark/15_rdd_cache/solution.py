#!/usr/bin/env python

"""
Solution
"""

import time
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("SQLCachingDemo").getOrCreate()

# Create sample data
n_rows = 1000000
data = [(i, f"product_{i % 1000}", i * 10) for i in range(n_rows)]
df = spark.createDataFrame(data, ["id", "product", "price"])

def run_queries_without_cache():
    start_time = time.time()

    # Query 1: Average price by product
    print("Running first query without cache...")
    avg_price = df.groupBy("product").avg("price").collect()

    # Query 2: Count products above average
    print("Running second query without cache...")
    product_counts = df.groupBy("product").count().collect()

    end_time = time.time()
    return end_time - start_time, avg_price, product_counts

def run_queries_with_cache():
    start_time = time.time()

    # Cache the DataFrame
    print("Caching DataFrame...")
    df.cache()

    # Force cache materialization with an action
    df.count()

    # Query 1: Average price by product
    print("Running first query with cache...")
    avg_price = df.groupBy("product").avg("price").collect()

    # Query 2: Count products above average
    print("Running second query with cache...")
    product_counts = df.groupBy("product").count().collect()

    end_time = time.time()

    # Check cache status
    print(f"Is cached? {df.is_cached}")

    return end_time - start_time, avg_price, product_counts

# Run without cache
print("Executing queries without cache...")
time_no_cache = run_queries_without_cache()
print(f"Time without cache: {time_no_cache:.2f} seconds")

# Run with cache
print("Executing queries with cache...")
time_with_cache = run_queries_with_cache()
print(f"Time with cache: {time_with_cache:.2f} seconds")

print(f"Speedup: {(time_no_cache - time_with_cache) / time_no_cache * 100:.2f}%")

# See execution plans
print("Execution plan without cache:")
df.groupBy("product").avg("price").explain()

print("Execution plan with cache:")
df.cache()
df.groupBy("product").avg("price").explain()

# Clean up
df.unpersist()
