#!/usr/bin/env python

from pyspark import SparkContext, SparkConf

# Initialize Spark
conf = SparkConf().setAppName("Sum Odd Numbers").setMaster("spark://localhost:7077")
sc = SparkContext(conf=conf)

# Set log level to reduce noise
sc.setLogLevel("ERROR")

# Read the file
# Note: textFile is the correct method
numbers_rdd = sc.textFile("numbers.txt")

# Convert strings to integers and filter odd numbers
odd_numbers = numbers_rdd \
    .map(lambda x: int(x.strip())) \
    .filter(lambda x: x % 2 != 0)

# Calculate sum
sum_odd = odd_numbers.sum()

print(f"Sum of odd numbers: {sum_odd}")

# Clean up
sc.stop()
