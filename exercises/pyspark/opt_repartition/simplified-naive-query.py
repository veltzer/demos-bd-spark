#!/usr/bin/env python
"""
Simplified PySpark Repartitioning Exercise - Naive Query Script

This script performs a category/region aggregation on the skewed transactions dataset
WITHOUT proper repartitioning, demonstrating performance challenges with skewed data.
"""

import time
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as sql_sum, avg, desc

# Initialize Spark Session with Hive support
spark = SparkSession.builder \
    .appName("PySpark Naive Query - Without Repartitioning") \
    .config("spark.sql.shuffle.partitions", "200") \
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

def get_partition_info(df):
    """Get information about dataframe partitioning"""
    partition_count = df.rdd.getNumPartitions()
    partition_sizes = df.rdd.glom().map(len).collect()

    if not partition_sizes or len(partition_sizes) == 0:
        return {
            "count": partition_count,
            "min": 0,
            "max": 0,
            "avg": 0,
            "skew_factor": 0
        }

    avg_size = sql_sum(partition_sizes) / len(partition_sizes) if len(partition_sizes) > 0 else 0
    skew_factor = max(partition_sizes) / avg_size if avg_size > 0 else 0

    partition_stats = {
        "count": partition_count,
        "min": min(partition_sizes),
        "max": max(partition_sizes),
        "avg": avg_size,
        "skew_factor": skew_factor
    }
    return partition_stats

def run_query():
    """Run aggregation query WITHOUT repartitioning"""
    print("-" * 80)
    print("Running query WITHOUT repartitioning")
    print("-" * 80)

    # Record start time
    start_time = time.time()

    # Load transactions table
    transactions = spark.table("transactions")
    initial_partitioning = get_partition_info(transactions)

    # QUERY: Category performance by region
    # This aggregation will suffer from the skewed partitioning
    print("\nExecuting Query: Category performance by region...")

    # The query calculates key metrics for each region/category combination
    result = transactions.groupBy("region_id", "product_category") \
        .agg(
            count("transaction_id").alias("transaction_count"),
            sql_sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_transaction_value")
        ) \
        .orderBy(desc("total_revenue"))

    # Force execution and collect results
    result_count = result.count()
    top_results = result.limit(10).collect()

    # Record end time
    execution_time = time.time() - start_time
    result_partitioning = get_partition_info(result)

    # Show query results
    print(f"\nQuery Results (showing top 10 of {result_count} region/category combinations):")
    for row in top_results:
        print(f"Region {row.region_id}, {row.product_category}: " +
              f"${row.total_revenue:.2f} from {row.transaction_count:,} transactions")

    # Return query metrics
    return {
        "execution_time": execution_time,
        "initial_partitioning": initial_partitioning,
        "result_partitioning": result_partitioning
    }

def print_performance_report(metrics):
    """Print a performance report based on execution metrics"""
    print("\n" + "=" * 80)
    print("PERFORMANCE REPORT - NAIVE QUERY (WITHOUT REPARTITIONING)")
    print("=" * 80)

    print(f"\nExecution Time: {metrics['execution_time']:.2f} seconds")

    print("\nData Partitioning:")
    print("Initial dataset:")
    print(f"  Partitions: {metrics['initial_partitioning']['count']}")
    print(f"  Min/Max/Avg rows per partition: {metrics['initial_partitioning']\
            ['min']}/{metrics['initial_partitioning']['max']}/{metrics['initial_partitioning']['avg']:.1f}")
    print(f"  Skew factor (max/avg): {metrics['initial_partitioning']['skew_factor']:.2f}x")

    print("\nResult dataset:")
    print(f"  Partitions: {metrics['result_partitioning']['count']}")
    print(f"  Min/Max/Avg rows per partition: {metrics['result_partitioning']\
            ['min']}/{metrics['result_partitioning']['max']}/{metrics['result_partitioning']['avg']:.1f}")
    print(f"  Skew factor (max/avg): {metrics['result_partitioning']['skew_factor']:.2f}x")

    print("\nObservations:")
    if metrics['initial_partitioning']['skew_factor'] > 3:
        print("- Input data shows significant partition skew")
    if metrics['result_partitioning']['skew_factor'] > 3:
        print("- Result data shows significant partition skew")
    print("- No repartitioning was used, leading to uneven work distribution")
    print("- Performance is bottlenecked by the largest/slowest partitions")
    print("\nNext step: Run optimized_query.py to see how repartitioning improves performance")


def main():
    # Verify table exists
    if not check_table_exists():
        spark.stop()
        sys.exit(1)

    # Run query without repartitioning
    print("\nRunning query without any repartitioning...")
    metrics = run_query()

    # Print performance report
    print_performance_report(metrics)

    # Save metrics for comparison
    with open("naive_query_metrics.txt", "w") as f:
        f.write(f"execution_time={metrics['execution_time']:.2f}\n")
        f.write(f"initial_skew={metrics['initial_partitioning']['skew_factor']:.2f}\n")
        f.write(f"result_skew={metrics['result_partitioning']['skew_factor']:.2f}\n")

    print(f"\nPerformance metrics saved to: {os.path.abspath('naive_query_metrics.txt')}")

    spark.stop()

if __name__ == "__main__":
    main()
