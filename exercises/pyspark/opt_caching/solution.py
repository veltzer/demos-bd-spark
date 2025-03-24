#!/usr/bin/env python

"""
Solution
"""

import time
from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("RDD Caching Exercise") \
        .getOrCreate()

def generate_large_dataset(spark, size=10000):
    """Generate a large dataset with repeated values"""
    return spark.sparkContext.parallelize(range(size), 100)

def expensive_computation(x):
    """Simulate an expensive computation"""
    time.sleep(0.001)  # Simulate complexity
    return (x % 100, x)

def optimized_analysis():
    """Optimized version using caching"""
    spark = create_spark_session()

    print("Starting optimized analysis...")

    # Create initial RDD
    base_rdd = generate_large_dataset(spark)

    # Perform expensive transformation and cache the result
    start_time = time.time()
    processed_rdd = base_rdd.map(expensive_computation).cache()

    # Force caching by running a small action
    processed_rdd.count()

    # Multiple actions on the cached RDD
    # Now computations will use cached data

    # Analysis 1: Count frequency of each key
    result1 = processed_rdd.countByKey()
    print(f"Analysis 1 found {len(result1)} unique keys")

    # Analysis 2: Find maximum value for each key
    result2 = processed_rdd.reduceByKey(max)
    print(f"Analysis 2 processed {result2.count()} keys")

    # Analysis 3: Calculate average value for each key
    result3 = processed_rdd.mapValues(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda x: x[0] / x[1])
    print(f"Analysis 3 processed {result3.count()} keys")

    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

    # Clean up cache
    processed_rdd.unpersist()
    spark.stop()

if __name__ == "__main__":
    optimized_analysis()
