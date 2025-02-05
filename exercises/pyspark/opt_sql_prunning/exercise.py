from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import random

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
    print("\nPartition information:")
    spark.sql(f"DESCRIBE DETAIL parquet.`{data_path}`").show()
    
    spark.stop()

if __name__ == "__main__":
    main()

"""
Exercise Tasks:

1. Run the code and observe the execution plans and times for both queries.
   - Note the difference in execution time
   - Look for "PushedFilters" and "PartitionFilters" in the explain output

2. Modify the code to:
   - Add more partitioning columns (e.g., partition by both date and region)
   - Try different date ranges to see impact on pruning
   - Add more complex filter conditions

3. Questions to consider:
   - Why is partition pruning beneficial?
   - What makes a good partitioning strategy?
   - When might partition pruning not help?
   - How does the size of the date range affect performance?

4. Additional experiments:
   - Try different partition columns
   - Compare performance with different data volumes
   - Test with different types of queries
"""