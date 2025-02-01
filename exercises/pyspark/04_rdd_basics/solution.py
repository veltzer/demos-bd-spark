#!/usr/bin/env python

# import sys
# import time
from pyspark import SparkContext, SparkConf

# Initialize Spark
conf = SparkConf().setAppName("Simple RDD Example").setMaster("local[*]")
sc = SparkContext(conf=conf)
# sc.setLogLevel("ERROR")

# Create RDD from a list
# numbers = [1, 2, 3, 4, 5]

# this is called a python "generator"
def create_some_numbers():
    for x in range(10000000000):
        print("im going to sleep...")
        # time.sleep(5)
        print(f"giving {x} to pyspark...")
        yield x

# numbers = [1, 2, 3, 4, 5]
numbers = range(0, 1000000)
numbersRDD = sc.parallelize(numbers)

# Let's see what's in it
print("Elements in RDD:")
for num in numbersRDD.collect():
    print(num)

# sc.stop()
# sys.exit(1)

# Create RDD from a list of tuples
pairs = [("a", 1), ("b", 2), ("c", 3)]
pairsRDD = sc.parallelize(pairs)
print(dir(sc))

print("Pairs in RDD:")
for key, value in pairsRDD.collect():
    print(f"Key: {key}, Value: {value}")

def do_something_with_value(x):
    return x+1

def square(x):
    return x*x

# Simple transformations
# squared = numbersRDD.map(lambda x: x * x)
squared = numbersRDD.map(square)
# squared = numbersRDD.map(do_something_with_value)
print("Squared numbers:")
print(sum(squared.collect()))

# Don't forget to stop the SparkContext when done
sc.stop()
