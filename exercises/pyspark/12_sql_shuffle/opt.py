#!/usr/bin/env python

import time
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("ShuffleOptimization").getOrCreate()

# Create sample data
# Orders data with customer_id and amount
orders_data = [(i, i % 100, float(i * 10)) for i in range(1000000)]
customers_data = [(i, f"Customer_{i}") for i in range(100)]

# Create DataFrames
orders_df = spark.createDataFrame(orders_data, ["order_id", "customer_id", "amount"])
customers_df = spark.createDataFrame(customers_data, ["customer_id", "customer_name"])

# Non-optimized way: Multiple shuffles due to separate operations
def non_optimized_way():
    start_time = time.time()

    # First join operations (causes shuffle)
    joined = orders_df.join(customers_df, "customer_id")

    # Group by customer and calculate average (causes another shuffle)
    result = joined.groupBy("customer_name").avg("amount")

    # Force execution and measure time
    result.collect()
    end_time = time.time()

    print("\nNon-optimized execution plan:")
    result.explain()
    return end_time - start_time

# Optimized way: Reduce shuffles by pre-aggregating before join
def optimized_way():
    start_time = time.time()

    # Pre-aggregate orders before joining (reduces data to be shuffled)
    pre_aggregated = orders_df.groupBy("customer_id").avg("amount")

    # Then join with the smaller result
    result = pre_aggregated.join(customers_df, "customer_id") \
                          .select("customer_name", "avg(amount)")

    # Force execution and measure time
    result.collect()
    end_time = time.time()

    print("\nOptimized execution plan:")
    result.explain()
    return end_time - start_time

# Run both approaches and compare
print("Running non-optimized way...")
non_opt_time = non_optimized_way()
print(f"Non-optimized execution time: {non_opt_time:.2f} seconds")

print("\nRunning optimized way...")
opt_time = optimized_way()
print(f"Optimized execution time: {opt_time:.2f} seconds")

print(f"\nImprovement: {((non_opt_time - opt_time) / non_opt_time) * 100:.2f}%")

# Cache the DataFrames if you want to run multiple times for comparison
# orders_df.cache()
# customers_df.cache()
