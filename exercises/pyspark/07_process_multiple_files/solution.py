#!/usr/bin/env python

"""
Solution to exercise of word counting of linux documentation
"""

import os
from operator import add
from pyspark import SparkContext, SparkConf

# Initialize Spark

master_url = os.environ["SPARK_MASTER_HOST"]
conf = SparkConf().setAppName("Linux Docs Word Count").setMaster(master_url)
sc = SparkContext(conf=conf)

# Set log level to reduce noise
sc.setLogLevel("ERROR")

# Read all text files from the docs directory
# DOCS_PATH = "/usr/share/doc"
DOCS_PATH = "/usr/share/doc/gzip"

# Create RDD from text files
files_rdd = sc.wholeTextFiles(DOCS_PATH + "/*")

# Get just the content (second element of each tuple) and split into lines
lines_rdd = files_rdd.flatMap(lambda x: x[1].splitlines())

# Process the words:
# 1. Split lines into words
# 2. Convert to lowercase
# 3. Remove empty strings
# 4. Create (word, 1) pairs
word_counts = lines_rdd \
    .flatMap(lambda line: line.lower().split()) \
    .filter(lambda word: word.strip() != "")\
    .map(lambda word: (word, 1)) \
    .reduceByKey(add)

# Sort by count (descending) and take the top result
word, times = word_counts.sortBy(lambda x: x[1], ascending=False).first()

print(f"\nMost common word: '{word}' (appears {times} times)")

# Optional: show top 10 words
print("\nTop 10 most common words:")
for word, count in word_counts.sortBy(lambda x: x[1], ascending=False).take(10):
    print(f"'{word}': {count} times")

# Clean up
sc.stop()
