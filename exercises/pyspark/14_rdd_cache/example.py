#!/usr/bin/env python

import time
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("CachingDemo").getOrCreate()
sc = spark.sparkContext

# Create a large RDD
data = range(1, 10000000)
rdd = sc.parallelize(data)

def complex_computation(x):
    # Simulate complex computation with some CPU work
    result = 0
    for i in range(100):
        result += (x * i) % 17
    return result

def without_cache():
    """ Without caching - will recompute everything twice """
    start_time = time.time()

    # Create computation chain
    computed_rdd = rdd.map(complex_computation)

    # First action - force complete computation
    print("First computation without cache...")
    result1 = computed_rdd.filter(lambda x: x % 2 == 0).count()

    # Second action - must recompute everything
    print("Second computation without cache...")
    result2 = computed_rdd.filter(lambda x: x % 3 == 0).count()

    end_time = time.time()
    return end_time - start_time, result1, result2

def with_cache():
    """ With caching - compute once, reuse results """
    start_time = time.time()

    # Cache after expensive computation
    computed_rdd = rdd.map(complex_computation)
    computed_rdd.cache()  # Mark RDD for caching

    # First action - will cache results
    print("First computation with cache...")
    result1 = computed_rdd.filter(lambda x: x % 2 == 0).count()

    # Second action - uses cached results
    print("Second computation with cache...")
    result2 = computed_rdd.filter(lambda x: x % 3 == 0).count()

    end_time = time.time()

    # Check if RDD is actually cached
    print(f"Is RDD cached? {computed_rdd.is_cached}")

    return end_time - start_time, result1, result2

# Run and compare
print("Running without cache...")
time_no_cache, result1_nc, result2_nc = without_cache()
print(f"Time without cache: {time_no_cache:.2f} seconds")
print(f"Results: {result1_nc}, {result2_nc}")

print("Running with cache...")
time_with_cache, result1_c, result2_c = with_cache()
print(f"Time with cache: {time_with_cache:.2f} seconds")
print(f"Results: {result1_c}, {result2_c}")

print(f"Speedup: {(time_no_cache - time_with_cache) / time_no_cache * 100:.2f}%")
