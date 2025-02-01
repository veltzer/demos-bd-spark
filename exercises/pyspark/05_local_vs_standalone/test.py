#!/usr/bin/env python

from pyspark.sql import SparkSession

# Create Spark session connected to your local cluster
spark = SparkSession.builder \
    .appName("Hello World") \
    .master("spark://localhost:7077") \
    .getOrCreate()

# Create simple RDD with text
text_rdd = spark.sparkContext.parallelize(["Hello, World!"])

# Print the text
print("From the cluster:")
print(text_rdd.collect()[0])

# Keep application running so you can see it in Spark UI
input("Check http://localhost:8080 to see your app, then press Enter to finish...")

# Clean up
spark.stop()
