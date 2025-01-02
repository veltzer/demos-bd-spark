#!/usr/bin/env python

from pyspark import SparkContext, SparkConf

# Create SparkConf object
conf = SparkConf().setAppName("VersionCheck").setMaster("spark://localhost:7077")

# Create SparkContext
sc = SparkContext(conf=conf)


# Print Spark version
print(f"Spark Version: {sc.version}")

# Optional: Print more details
print("Spark Configuration:")
print(f"Master: {sc.master}")
print(f"App Name: {sc.appName}")

# Good practice to stop SparkContext when done
sc.stop()
