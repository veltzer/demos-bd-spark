#!/usr/bin/env python

from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("SimpleOptimization").getOrCreate()
sc = spark.sparkContext

# Create sample data
numbers = range(1, 10000000)
rdd = sc.parallelize(numbers)

# Inefficient way: Multiple separate operations
def inefficient_way():
    # Step 1: Filter even numbers
    evens = rdd.filter(lambda x: x % 2 == 0)
    # Step 2: Square them
    squares = evens.map(lambda x: x * x)
    # Step 3: Sum them
    total = squares.reduce(lambda x, y: x + y)
    return total

# Optimized way: Chain operations together
def optimized_way():
    total = rdd.filter(lambda x: x % 2 == 0) \
              .map(lambda x: x * x) \
              .reduce(lambda x, y: x + y)
    return total

if True:
    start_time = time.time()
    print("Inefficient way result:", inefficient_way())
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"elaspsed time: {elapsed_time}")

if False:
    start_time = time.time()
    print("Optimized way result:", optimized_way())
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"elaspsed time: {elapsed_time}")
