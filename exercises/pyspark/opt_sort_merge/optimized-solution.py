#!/usr/bin/env python
"""
Sort-Merge Performance Exercise - Optimized Solution

This script demonstrates joining pre-sorted datasets, allowing Spark
to skip the sorting step during the join operation.
"""

import sys
import time
import os
import json
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Sort-Merge Exercise - Optimized Solution") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Path constants
DATA_DIR = "data"
SORTED_TRANSACTIONS = f"{DATA_DIR}/sorted_transactions"
SORTED_PRODUCTS = f"{DATA_DIR}/sorted_products"
RESULTS_DIR = "results"

def check_data_exists():
    """Verify that required data exists"""
    if not os.path.exists(SORTED_TRANSACTIONS) or not os.path.exists(SORTED_PRODUCTS):
        print("Error: Required data files not found.")
        print(f"Make sure {SORTED_TRANSACTIONS} and {SORTED_PRODUCTS} exist.")
        print("Run data_generation.py script first.")
        return False
    return True

def ensure_results_dir():
    """Ensure results directory exists"""
    if not os.path.exists(RESULTS_DIR):
        os.makedirs(RESULTS_DIR)

def get_executor_metrics():
    """Get current executor metrics from Spark"""
    # This is a simplified version - in a real environment,
    # you would collect more detailed metrics from the Spark UI
    metrics = {}
    metrics['active_executors'] = len(spark.sparkContext.statusTracker().getExecutorInfos()) - 1  # Exclude driver
    metrics['executor_memory'] = spark.conf.get("spark.executor.memory")
    metrics['executor_cores'] = spark.conf.get("spark.executor.cores", "Not specified")
    metrics['default_parallelism'] = spark.sparkContext.defaultParallelism
    # Default values if we can't get actual metrics
    metrics = {
        'active_executors': 'Unknown',
        'executor_memory': spark.conf.get("spark.executor.memory", "Unknown"),
        'executor_cores': 'Unknown',
        'default_parallelism': 'Unknown'
    }
    return metrics

def verify_sorting(df, sort_column):
    """
    Verify that a dataframe is actually sorted by checking a sample
    Note: This is not a comprehensive check, just a sanity check
    """
    sample = df.select(sort_column).limit(1000).collect()
    values = [row[sort_column] for row in sample]

    # Check if the sampled values are in sorted order
    is_sorted = all(values[i] <= values[i+1] for i in range(len(values)-1))
    return is_sorted

def optimized_solution():
    """
    Perform join on pre-sorted data, allowing Spark to skip
    the sorting phase during join.
    """
    print("-" * 80)
    print("OPTIMIZED SOLUTION: JOINING PRE-SORTED DATASETS")
    print("-" * 80)

    # Record start time
    start_time = time.time()

    # Load sorted data
    print("\nLoading sorted transactions...")
    load_start = time.time()
    transactions = spark.read.parquet(SORTED_TRANSACTIONS)
    products = spark.read.parquet(SORTED_PRODUCTS)
    load_time = time.time() - load_start

    # Get row counts and partition info
    transactions_count = transactions.count()
    products_count = products.count()
    transactions_partitions = transactions.rdd.getNumPartitions()
    products_partitions = products.rdd.getNumPartitions()

    print(f"Loaded {transactions_count:,} transactions in {transactions_partitions} partitions")
    print(f"Loaded {products_count:,} products in {products_partitions} partitions")
    print(f"Data loading time: {load_time:.2f} seconds")

    # Verify data is actually sorted
    transactions_sorted = verify_sorting(transactions, "product_id")
    products_sorted = verify_sorting(products, "product_id")

    print("\nVerifying data sorting:")
    print(f"Transactions sorted by product_id: {transactions_sorted}")
    print(f"Products sorted by product_id: {products_sorted}")

    if not transactions_sorted or not products_sorted:
        print("WARNING: Data doesn't appear to be properly sorted!")
        print("Performance benefits may not be realized.")

    # Perform join operation
    print("\nPerforming join operation (on pre-sorted data)...")
    join_start = time.time()

    # The data is already sorted, so Spark should be able to
    # skip the expensive sorting step during join
    result = transactions.join(
        products,
        on="product_id",
        how="inner"
    )

    # Force execution and get result count
    result_count = result.count()
    join_time = time.time() - join_start

    # Check a sample of results
    print("\nSample of join results:")
    result.select("transaction_id", "product_id", "product_name", "quantity", "price") \
        .orderBy("transaction_id") \
        .limit(5) \
        .show()

    # Record end time and calculate metrics
    end_time = time.time()
    total_time = end_time - start_time

    # Get executor metrics
    executor_metrics = get_executor_metrics()

    # Create performance report
    performance = {
        "solution_type": "optimized",
        "total_time_seconds": total_time,
        "load_time_seconds": load_time,
        "join_time_seconds": join_time,
        "transactions_count": transactions_count,
        "products_count": products_count,
        "join_result_count": result_count,
        "transactions_partitions": transactions_partitions,
        "products_partitions": products_partitions,
        "transactions_verified_sorted": transactions_sorted,
        "products_verified_sorted": products_sorted,
        "executor_metrics": executor_metrics,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    # Save performance metrics
    ensure_results_dir()
    with open(f"{RESULTS_DIR}/optimized_performance.json", "w") as f:
        json.dump(performance, f, indent=2)

    # Print performance summary
    print("\nPerformance Summary:")
    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"Data loading time: {load_time:.2f} seconds")
    print(f"Join operation time: {join_time:.2f} seconds")
    print(f"Join result count: {result_count:,} rows")
    print(f"\nPerformance metrics saved to {RESULTS_DIR}/optimized_performance.json")

    return performance

def main():
    if not check_data_exists():
        sys.exit(1)

    # Run optimized solution
    _performance = optimized_solution()

    # Provide next steps
    print("\nNext step: Run comparison.py to visualize the performance difference")
    print("between naive and optimized solutions.")

    spark.stop()

if __name__ == "__main__":
    main()
