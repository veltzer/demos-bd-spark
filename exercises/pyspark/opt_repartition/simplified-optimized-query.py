#!/usr/bin/env python
"""
Fixed Optimized Query Script - Excluding Repartitioning Time

This script performs aggregation with repartitioning but properly
handles partition information collection to avoid errors.
"""

import sys
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as my_sum, avg, desc

# Initialize Spark Session with Hive support
spark = SparkSession.builder \
    .appName("PySpark Optimized Query - WITH Repartitioning (Fixed)") \
    .config("spark.sql.shuffle.partitions", "50") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def check_table_exists():
    """Check if the transactions table exists"""
    tables = [t.name for t in spark.catalog.listTables()]
    if "transactions" not in tables:
        print("Error: Required table 'transactions' not found.")
        print("Please run the data_generation.py script first.")
        return False
    return True

def check_previous_metrics():
    """Check if naive query metrics exist for comparison"""
    if not os.path.exists("naive_query_metrics.txt"):
        print("Warning: naive_query_metrics.txt not found.")
        print("Run naive_query.py first for proper comparison.")
        return {}

    metrics = {}
    with open("naive_query_metrics.txt", "r") as f:
        for line in f:
            key, value = line.strip().split("=")
            metrics[key] = float(value)
    return metrics

def get_simple_partition_info(df):
    """Get basic information about dataframe partitioning without using problematic operations"""
    partition_count = df.rdd.getNumPartitions()
    return {
        "count": partition_count,
        "min": 0,  # We'll skip detailed analysis to avoid errors
        "max": 0,
        "avg": 0,
        "skew_factor": 0
    }

def run_query():
    """Run aggregation query WITH repartitioning but measuring only query time"""
    print("-" * 80)
    print("Running query WITH repartitioning (fixed implementation)")
    print("-" * 80)

    # Load transactions table
    transactions = spark.table("transactions")
    initial_partitioning = get_simple_partition_info(transactions)

    # STEP 1: Repartition the data
    # We do this BEFORE starting the timer
    print("\nRepartitioning data before aggregation...")
    repartition_start = time.time()

    # Using fewer partitions (50) and simpler repartitioning strategy
    balanced_transactions = transactions.repartition(50)

    # Force execution of repartitioning by counting
    balanced_count = balanced_transactions.count()
    repartition_time = time.time() - repartition_start

    print(f"Repartitioning complete in {repartition_time:.2f} seconds")
    print(f"Balanced data has {balanced_count:,} rows in {balanced_transactions.rdd.getNumPartitions()} partitions")

    balanced_partitioning = get_simple_partition_info(balanced_transactions)

    # STEP 2: Run the query (identical to naive query)
    # Start timing NOW - after repartitioning is done
    print("\nExecuting Query: Category performance by region...")
    query_start_time = time.time()

    # The query calculates key metrics for each region/category combination
    result = balanced_transactions.groupBy("region_id", "product_category") \
        .agg(
            count("transaction_id").alias("transaction_count"),
            my_sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_transaction_value")
        ) \
        .orderBy(desc("total_revenue"))

    # Force execution and collect results
    result_count = result.count()
    top_results = result.limit(10).collect()

    # Record end time - this is ONLY the query time, not including repartitioning
    query_execution_time = time.time() - query_start_time
    total_time = repartition_time + query_execution_time

    result_partitioning = get_simple_partition_info(result)

    # Show query results
    print(f"\nQuery Results (showing top 10 of {result_count} region/category combinations):")
    for row in top_results:
        print(f"Region {row.region_id}, {row.product_category}: " +
              f"${row.total_revenue:.2f} from {row.transaction_count:,} transactions")

    # Return query metrics
    return {
        "query_execution_time": query_execution_time,
        "repartition_time": repartition_time,
        "total_time": total_time,
        "initial_partitioning": initial_partitioning,
        "balanced_partitioning": balanced_partitioning,
        "result_partitioning": result_partitioning
    }

def print_performance_report(metrics, previous_metrics=None):
    """Print a performance report based on execution metrics"""
    print("\n" + "=" * 80)
    print("PERFORMANCE REPORT - FIXED OPTIMIZED QUERY")
    print("=" * 80)

    print(f"\nQuery Execution Time (excluding repartitioning): {metrics['query_execution_time']:.2f} seconds")
    print(f"Repartitioning Time: {metrics['repartition_time']:.2f} seconds")
    print(f"Total Time (repartitioning + query): {metrics['total_time']:.2f} seconds")

    if previous_metrics:
        query_speedup = previous_metrics['execution_time'] / metrics['query_execution_time']
        total_speedup = previous_metrics['execution_time'] / metrics['total_time']

        print(f"\nQuery Speedup (excluding repartitioning): {query_speedup:.2f}x faster")
        print(f"Total Speedup (including repartitioning): {total_speedup:.2f}x {'faster' if total_speedup > 1 else 'slower'}")

        if total_speedup < 1:
            print("\nNOTE: Total time is slower because repartitioning overhead exceeds query benefit")
            print("      This suggests repartitioning may not be worth it for a single query")
            print("      But could be beneficial for multiple queries on the same repartitioned data")

    print("\nData Partitioning:")
    print(f"Initial dataset partitions: {metrics['initial_partitioning']['count']}")
    print(f"After repartitioning: {metrics['balanced_partitioning']['count']} partitions")
    print(f"Result dataset: {metrics['result_partitioning']['count']} partitions")

    print("\nNote: Detailed partition statistics are disabled to avoid errors")

    print("\nModified Repartitioning Strategy:")
    print("1. Used simpler repartition(50) - random distribution")
    print("2. Reduced partition count from 200 to 50")
    print("3. Separated repartitioning time from query execution time")

    print("\nOptimization Takeaways:")
    print("1. The cost of repartitioning (a full shuffle) must be balanced against query benefits")
    print("2. Repartitioning is most beneficial when:")
    print("   - You run multiple queries on the same repartitioned data")
    print("   - Your data is severely skewed (5x+ difference between partition sizes)")
    print("   - Your query is complex enough to benefit from balanced workload")
    print("3. Consider caching the repartitioned data if using it multiple times")

def main():
    # Verify table exists
    if not check_table_exists():
        spark.stop()
        sys.exit(1)

    # Check for previous metrics
    previous_metrics = check_previous_metrics()

    # Run query with repartitioning
    print("\nRunning fixed query WITH repartitioning...")
    metrics = run_query()

    # Print performance report
    print_performance_report(metrics, previous_metrics)

    # Save metrics for comparison
    with open("fixed_optimized_query_metrics.txt", "w") as f:
        f.write(f"query_execution_time={metrics['query_execution_time']:.2f}\n")
        f.write(f"repartition_time={metrics['repartition_time']:.2f}\n")
        f.write(f"total_time={metrics['total_time']:.2f}\n")

    print(f"\nPerformance metrics saved to: {os.path.abspath('fixed_optimized_query_metrics.txt')}")

    spark.stop()

if __name__ == "__main__":
    main()
