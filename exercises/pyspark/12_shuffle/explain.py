#!/usr/bin/env python

"""
Explain
"""

from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame

spark = SparkSession.builder.appName("SimpleOptimization").getOrCreate()
sc = spark.sparkContext
numbers = range(1, 1000)
rdd = sc.parallelize(numbers)

# Convert RDD operations to DataFrame operations so we can use explain()
df = spark.createDataFrame([(x,) for x in numbers], ["value"])

# Inefficient way with separate operations
def inefficient_way() -> DataFrame:
    evens = df.filter("value % 2 = 0")
    squares = evens.selectExpr("value * value as square")
    result = squares.agg({"square": "sum"})
    result.explain(mode="cost")
    return result

# Optimized way with chained operations
def optimized_way() -> DataFrame:
    result = df.filter("value % 2 = 0") \
               .selectExpr("value * value as square") \
               .agg({"square": "sum"})
    result.explain(mode="cost")
    return result

print("Inefficient way plan:")
inefficient_result = inefficient_way()

print("Optimized way plan:")
optimized_result = optimized_way()
