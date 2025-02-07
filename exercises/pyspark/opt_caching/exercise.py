#!/usr/bin/env python

# Exercise 1: RDD Caching and Persistence
#
# This script performs multiple operations on the same RDD without caching,
# causing repeated computations. Your task is to optimize it using appropriate
# caching/persistence strategies.

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

def unoptimized_analysis():
    """Unoptimized version with repeated computations"""
    spark = create_spark_session()

    print("Starting unoptimized analysis...")

    # Create initial RDD
    base_rdd = generate_large_dataset(spark)

    start_time = time.time()

    # Perform expensive transformation
    processed_rdd = base_rdd.map(expensive_computation)

    # Multiple actions on the same transformed RDD
    # Each action will recompute the entire lineage

    # Analysis 1: Count frequency of each key
    result1 = processed_rdd.countByKey()
    print(f"Analysis 1 found {len(result1)} unique keys")
    processed_rdd.unpersist(blocking=True)

    # Analysis 2: Find maximum value for each key
    result2 = processed_rdd.reduceByKey(max)
    print(f"Analysis 2 processed {result2.count()} keys")
    processed_rdd.unpersist(blocking=True)

    # Analysis 3: Calculate average value for each key
    result3 = processed_rdd.mapValues(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda x: x[0] / x[1])
    print(f"Analysis 3 processed {result3.count()} keys")

    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

    spark.stop()

if __name__ == "__main__":
    unoptimized_analysis()
