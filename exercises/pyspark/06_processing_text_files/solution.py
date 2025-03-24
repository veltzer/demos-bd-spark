#!/usr/bin/env python

"""
Solution
"""

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Sum Odd Numbers").setMaster("spark://localhost:7077")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

numbers_rdd = sc.textFile("numbers.txt")

odd_numbers = numbers_rdd \
    .map(lambda x: int(x.strip())) \
    .filter(lambda x: x % 2 != 0)
sum_odd = odd_numbers.sum()

print(f"Sum of odd numbers: {sum_odd}")

sc.stop()
