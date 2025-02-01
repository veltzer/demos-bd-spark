#!/usr/bin/env python

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Create Spark session
spark = SparkSession.builder.appName("BroadcastJoinDemo").getOrCreate()

# Create a large dataframe
large_data = [(i, f"product_{i % 1000}", i * 10) for i in range(1000000)]
large_df = spark.createDataFrame(large_data, ["id", "product_id", "value"])

# Create a small dataframe (lookup table)
small_data = [(f"product_{i}", f"Product Name {i}") for i in range(1000)]
small_df = spark.createDataFrame(small_data, ["product_id", "product_name"])

def regular_join():
    start_time = time.time()

    # Regular join
    res = large_df.join(small_df, "product_id")

    # Force execution and count results
    count = res.count()

    end_time = time.time()

    # Show the physical plan
    print("Regular Join Plan:")
    res.explain()

    return end_time - start_time, count

def broadcast_join():
    start_time = time.time()

    # Broadcast join
    res = large_df.join(broadcast(small_df), "product_id")

    # Force execution and count results
    count = res.count()

    end_time = time.time()

    # Show the physical plan
    print("Broadcast Join Plan:")
    res.explain()

    return end_time - start_time, count

# Run both joins and compare
print("Running regular join...")
time_regular, count_regular = regular_join()
print(f"Regular join time: {time_regular:.2f} seconds")
print(f"Regular join count: {count_regular}")

print("Running broadcast join...")
time_broadcast, count_broadcast = broadcast_join()
print(f"Broadcast join time: {time_broadcast:.2f} seconds")
print(f"Broadcast join count: {count_broadcast}")

print(f"Speedup: {(time_regular - time_broadcast) / time_regular * 100:.2f}%")

# Show memory usage of the broadcast table
print("Broadcast table size:")
small_df_size = small_df.count() * len(small_df.columns)
print(f"Approximate broadcast size: {small_df_size * 8 / 1024 / 1024:.2f} MB")

# Compare with broadcast threshold
broadcast_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print(f"Current broadcast threshold: {broadcast_threshold}")

# Demonstrate when Spark automatically chooses broadcast
print("Automatic broadcast decision:")
result = large_df.join(small_df, "product_id")
result.explain()
