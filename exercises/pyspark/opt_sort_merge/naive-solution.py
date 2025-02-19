#!/usr/bin/env python
"""
Sort-Merge Performance Exercise - Naive Solution

This script demonstrates joining unsorted datasets, requiring Spark
to perform sorting during the join operation.
"""

import sys
import time
import os
import json
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Sort-Merge Exercise - Naive Solution") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Path constants
DATA_DIR = "data"
UNSORTED_TRANSACTIONS = f"{DATA_DIR}/unsorted_transactions"
UNSORTED_PRODUCTS = f"{DATA_DIR}/unsorted_products"
RESULTS_DIR = "results"

def check_data_exists():
    """Verify that required data exists"""
    if not os.path.exists(UNSORTED_TRANSACTIONS) or not os.path.exists(UNSORTED_PRODUCTS):
        print("Error: Required data files not found.")
        print(f"Make sure {UNSORTED_TRANSACTIONS} and {UNSORTED_PRODUCTS} exist.")
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

def naive_solution():
    """
    Perform join on unsorted data, letting Spark handle sorting
    during the join operation.
    """
    print("-" * 80)
    print("NAIVE SOLUTION: JOINING UNSORTED DATASETS")
    print("-" * 80)

    # Record start time
    start_time = time.time()

    # Load unsorted data
    print("\nLoading unsorted transactions...")
    load_start = time.time()
    transactions = spark.read.parquet(UNSORTED_TRANSACTIONS)
    products = spark.read.parquet(UNSORTED_PRODUCTS)
    load_time = time.time() - load_start

    # Get row counts and partition info
    transactions_count = transactions.count()
    products_count = products.count()
    transactions_partitions = transactions.rdd.getNumPartitions()
    products_partitions = products.rdd.getNumPartitions()

    print(f"Loaded {transactions_count:,} transactions in {transactions_partitions} partitions")
    print(f"Loaded {products_count:,} products in {products_partitions} partitions")
    print(f"Data loading time: {load_time:.2f} seconds")

    # Perform join operation
    print("\nPerforming join operation (with internal sorting)...")
    join_start = time.time()

    # Note: Spark will need to sort both datasets during this join
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
        "solution_type": "naive",
        "total_time_seconds": total_time,
        "load_time_seconds": load_time,
        "join_time_seconds": join_time,
        "transactions_count": transactions_count,
        "products_count": products_count,
        "join_result_count": result_count,
        "transactions_partitions": transactions_partitions,
        "products_partitions": products_partitions,
        "executor_metrics": executor_metrics,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    # Save performance metrics
    ensure_results_dir()
    with open(f"{RESULTS_DIR}/naive_performance.json", "w") as f:
        json.dump(performance, f, indent=2)

    # Print performance summary
    print("\nPerformance Summary:")
    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"Data loading time: {load_time:.2f} seconds")
    print(f"Join operation time: {join_time:.2f} seconds")
    print(f"Join result count: {result_count:,} rows")
    print(f"\nPerformance metrics saved to {RESULTS_DIR}/naive_performance.json")

    return performance

def main():
    if not check_data_exists():
        sys.exit(1)

    # Run naive solution
    _performance = naive_solution()

    # Provide next steps
    print("\nNext step: Run optimized_solution.py to see the performance difference")
    print("with pre-sorted data, then run comparison.py to visualize the results.")

    spark.stop()


if __name__ == "__main__":
    main()
