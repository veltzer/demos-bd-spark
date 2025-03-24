#!/usr/bin/env python

"""
Exercise
"""

from datetime import datetime, timedelta
import random
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_spark_session():
    return SparkSession.builder \
        .appName("Partition Pruning Exercise") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

def generate_sales_data(spark, start_date, end_date, num_records=1000000):
    """Generate sample sales data for demonstration"""

    # Generate dates between start and end date
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)

    # Generate sales records
    data = []
    products = ["laptop", "phone", "tablet", "watch", "headphones"]
    regions = ["north", "south", "east", "west"]

    for _ in range(num_records):
        sale_date = random.choice(dates)
        data.append((
            f"ORDER_{random.randint(1000, 9999)}",
            random.choice(products),
            random.choice(regions),
            random.randint(100, 2000),
            sale_date.strftime("%Y-%m-%d")
        ))

    # Create DataFrame
    return spark.createDataFrame(
        data,
        ["order_id", "product", "region", "amount", "sale_date"]
    )

def write_partitioned_data(df, path):
    """Write data partitioned by sale_date"""
    df.write \
        .partitionBy("sale_date") \
        .mode("overwrite") \
        .parquet(path)

def query_without_partition_pruning(spark, path):
    """Query that doesn't benefit from partition pruning"""
    print("\nQuery WITHOUT partition pruning benefit:")
    start_time = datetime.now()

    result = spark.read.parquet(path) \
        .groupBy("product", "region") \
        .agg(F.sum("amount").alias("total_sales")) \
        .orderBy("total_sales", ascending=False)

    result.show(5)
    print(f"Execution time: {(datetime.now() - start_time).total_seconds():.2f} seconds")
    print("\nExecution plan:")
    result.explain()

def query_with_partition_pruning(spark, path, start_date, end_date):
    """Query that benefits from partition pruning"""
    print("\nQuery WITH partition pruning benefit:")
    start_time = datetime.now()

    result = spark.read.parquet(path) \
        .where(F.col("sale_date").between(
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )) \
        .groupBy("product", "region") \
        .agg(F.sum("amount").alias("total_sales")) \
        .orderBy("total_sales", ascending=False)

    result.show(5)
    print(f"Execution time: {(datetime.now() - start_time).total_seconds():.2f} seconds")
    print("\nExecution plan:")
    result.explain()

def show_partition_info(spark, path):
    """Show information about partitions"""
    print("\nPartition information:")

    # Read the parquet file and show partitions
    df = spark.read.parquet(path)

    # Show distinct partition values
    print("\nUnique partition values (sale_date):")
    df.select("sale_date").distinct().orderBy("sale_date").show(10)

    # Show partition column statistics
    print("\nPartition column statistics:")
    df.select(
        F.min("sale_date").alias("min_date"),
        F.max("sale_date").alias("max_date"),
        F.count_distinct("sale_date").alias("num_partitions")
    ).show()

def main():
    spark = create_spark_session()

    # Generate data for a 90-day period
    end_date = datetime(2024, 2, 1)
    start_date = end_date - timedelta(days=90)

    print("Generating and writing partitioned data...")
    df = generate_sales_data(spark, start_date, end_date)

    # Write partitioned data
    data_path = "sales_data_partitioned"
    write_partitioned_data(df, data_path)

    # Run queries to demonstrate partition pruning

    # Query 1: Without partition pruning benefit
    query_without_partition_pruning(spark, data_path)

    # Query 2: With partition pruning benefit
    # Query only last 30 days
    query_date_start = end_date - timedelta(days=30)
    query_with_partition_pruning(spark, data_path, query_date_start, end_date)

    # Show partition information
    show_partition_info(spark, data_path)

    spark.stop()

if __name__ == "__main__":
    main()
