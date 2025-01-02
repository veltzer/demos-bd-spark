#!/usr/bin/env python

from pyspark import SparkContext, SparkConf

# Initialize Spark
conf = SparkConf().setAppName("Simple RDD Example").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Create RDD from a list
numbers = [1, 2, 3, 4, 5]
numbersRDD = sc.parallelize(numbers)

# Let's see what's in it
print("Elements in RDD:")
for num in numbersRDD.collect():
    print(num)

# Create RDD from a list of tuples
pairs = [("a", 1), ("b", 2), ("c", 3)]
pairsRDD = sc.parallelize(pairs)

print("\nPairs in RDD:")
for pair in pairsRDD.collect():
    print(f"Key: {pair[0]}, Value: {pair[1]}")

# Simple transformations
squared = numbersRDD.map(lambda x: x * x)
print("\nSquared numbers:")
print(squared.collect())

# Don't forget to stop the SparkContext when done
sc.stop()
