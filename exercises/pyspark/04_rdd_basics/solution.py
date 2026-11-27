#!/usr/bin/env python

"""
Solution
"""

from pyspark import SparkConf, SparkContext

# Initialize Spark
conf = SparkConf().setAppName("Simple RDD Example").setMaster("local[*]")
sc = SparkContext(conf=conf)
# sc.setLogLevel("ERROR")  # noqa: ERA001  teaching: kept in the comment for the reader

# Create RDD from a list
# numbers = [1, 2, 3, 4, 5]  # noqa: ERA001  teaching: kept in the comment for the reader

# this is called a python "generator"
def create_some_numbers():
    for x in range(10000000000):
        print("im going to sleep...")
        # time.sleep(5)  # noqa: ERA001  teaching: kept in the comment for the reader
        print(f"giving {x} to pyspark...")
        yield x

# numbers = [1, 2, 3, 4, 5]  # noqa: ERA001  teaching: kept in the comment for the reader
numbers = range(1000000)
numbers_rdd = sc.parallelize(numbers)

# Let's see what's in it
print("Elements in RDD:")
for num in numbers_rdd.collect():
    print(num)

# sc.stop()  # noqa: ERA001  teaching: kept in the comment for the reader
# sys.exit(1)  # noqa: ERA001  teaching: kept in the comment for the reader

# Create RDD from a list of tuples
pairs = [("a", 1), ("b", 2), ("c", 3)]
pairs_rdd = sc.parallelize(pairs)
print(dir(sc))

print("Pairs in RDD:")
for key, value in pairs_rdd.collect():
    print(f"Key: {key}, Value: {value}")

def do_something_with_value(x: int):
    return x+1

def square(x: int):
    return x*x

# Simple transformations
# squared = numbers_rdd.map(lambda x: x * x)  # noqa: ERA001  teaching: kept in the comment for the reader
squared = numbers_rdd.map(square)
# squared = numbers_rdd.map(do_something_with_value)  # noqa: ERA001  teaching: kept in the comment for the reader
print("Squared numbers:")
print(sum(squared.collect()))

# Don't forget to stop the SparkContext when done
sc.stop()
