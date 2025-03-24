#!/usr/bin/env python

"""
Solution
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("OperationCounter").getOrCreate()
sc = spark.sparkContext
numbers = range(1, 10000)
df = spark.createDataFrame([(x,) for x in numbers], ["value"])

# Create accumulators to count operations
filter_ops = sc.accumulator(0)
map_ops = sc.accumulator(0)
reduce_ops = sc.accumulator(0)

# Inefficient way with operation counting
def inefficient_way():
    # Step 1: Filter even numbers
    evens = df.rdd.map(lambda x: (
        filter_ops.add(1),  # Count filter operation
        x.value % 2 == 0 and x.value or None
    )[1]).filter(lambda x: x is not None)
    # Step 2: Square them
    squares = evens.map(lambda x: (
        map_ops.add(1),    # Count map operation
        x * x
    )[1])
    # Step 3: Sum them
    total = squares.reduce(lambda x, y: (
        reduce_ops.add(1), # Count reduce operation
        x + y
    )[1])
    return total

# Reset accumulators
filter_ops.value = 0
map_ops.value = 0
reduce_ops.value = 0

print("Executing inefficient way...")
result1 = inefficient_way()
print(f"""
Inefficient way operations:
Filter operations: {filter_ops.value}
Map operations: {map_ops.value}
Reduce operations: {reduce_ops.value}
Total operations: {filter_ops.value + map_ops.value + reduce_ops.value}
""")

# Reset accumulators for optimized way
filter_ops.value = 0
map_ops.value = 0
reduce_ops.value = 0

# Optimized way with operation counting
def optimized_way():
    return df.rdd.map(lambda x: (
        filter_ops.add(1),  # Count filter operation
        x.value % 2 == 0 and x.value or None
    )[1]).filter(lambda x: x is not None) \
              .map(lambda x: (
                  map_ops.add(1),  # Count map operation
                  x * x
              )[1]) \
              .reduce(lambda x, y: (
                  reduce_ops.add(1),  # Count reduce operation
                  x + y
              )[1])

print("Executing optimized way...")
result2 = optimized_way()
print(f"""
Optimized way operations:
Filter operations: {filter_ops.value}
Map operations: {map_ops.value}
Reduce operations: {reduce_ops.value}
Total operations: {filter_ops.value + map_ops.value + reduce_ops.value}
""")

print("Results match:", result1 == result2)
