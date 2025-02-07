#!/usr/bin/env python

# Exercise 2: Optimizing Count Distinct with Two-Phase Aggregation
#
# This exercise demonstrates the performance difference between regular
# count distinct and a two-phase approach with partition-level aggregation.

import time
import random
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def create_spark_session():
    return SparkSession.builder \
        .appName("Count Distinct Exercise") \
        .getOrCreate()

def generate_skewed_data(spark, size=100000):
    """Generate dataset with skewed distribution for user_id and event_type"""
    data = []
    for _ in range(size):
        # Generate skewed user_ids (some users have many events)
        user_id = int(abs(random.gauss(1000, 300)))
        # Limited set of event types
        event_type = random.choice(['click', 'view', 'purchase', 'share', 'like'])
        # Random timestamp
        timestamp = random.randint(1000000, 9999999)
        data.append((user_id, event_type, timestamp))

    return spark.createDataFrame(data, ['user_id', 'event_type', 'timestamp'])

def unoptimized_analysis(df):
    """Unoptimized version using straightforward countDistinct"""
    print("Starting unoptimized analysis...")
    start_time = time.time()

    # Count distinct users per event type
    result = df.groupBy('event_type').agg(
        F.countDistinct('user_id').alias('unique_users')
    )

    # Force computation and show results
    result.show()

    end_time = time.time()
    print(f"Unoptimized execution time: {end_time - start_time:.2f} seconds")

    # Show the execution plan
    print("\nUnoptimized query plan:")
    result.explain()

def main():
    spark = create_spark_session()
    # Generate test data
    df = generate_skewed_data(spark)
    unoptimized_analysis(df)
    spark.stop()

if __name__ == "__main__":
    main()
