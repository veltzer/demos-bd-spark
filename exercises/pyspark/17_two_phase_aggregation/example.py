from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count
import time
import random

# Create Spark session
spark = SparkSession.builder.appName("TwoPhaseAggregationDemo").getOrCreate()

# Create sample data with skewed keys
def create_skewed_data(num_rows, num_keys, skew_factor):
    data = []
    for _ in range(num_rows):
        # Create skewed distribution of keys
        if random.random() < skew_factor:
            key = random.randint(1, num_keys // 10)  # 10% of keys get more data
        else:
            key = random.randint(1, num_keys)
        value = random.randint(1, 1000)
        data.append((key, value))
    return data

# Generate test data
num_rows = 10000000  # 10M rows
num_keys = 1000
skew_factor = 0.7    # 70% of data goes to 10% of keys

print("Generating test data...")
data = create_skewed_data(num_rows, num_keys, skew_factor)
df = spark.createDataFrame(data, ["key", "value"])

# Function for single-phase aggregation
def single_phase_aggregation():
    start_time = time.time()
    
    result = df.groupBy("key").agg(
        sum("value").alias("total_value"),
        count("*").alias("count")
    )
    
    # Force execution and collect results
    count = result.count()
    
    end_time = time.time()
    
    # Show execution plan
    print("\nSingle-phase aggregation plan:")
    result.explain()
    
    return end_time - start_time, count

# Function for two-phase aggregation
def two_phase_aggregation():
    start_time = time.time()
    
    # Phase 1: Pre-aggregate at partition level
    df_repartitioned = df.repartition(200, "key")
    
    # Phase 2: Final aggregation
    result = df_repartitioned.groupBy("key").agg(
        sum("value").alias("total_value"),
        count("*").alias("count")
    )
    
    # Force execution and collect results
    count = result.count()
    
    end_time = time.time()
    
    # Show execution plan
    print("\nTwo-phase aggregation plan:")
    result.explain()
    
    return end_time - start_time, count

# Run both approaches multiple times to get average performance
num_trials = 3
single_phase_times = []
two_phase_times = []

print(f"\nRunning {num_trials} trials...")

for i in range(num_trials):
    print(f"\nTrial {i+1}:")
    
    # Clear cache
    spark.catalog.clearCache()
    
    # Run single-phase
    print("Running single-phase aggregation...")
    time_single, count_single = single_phase_aggregation()
    single_phase_times.append(time_single)
    print(f"Single-phase time: {time_single:.2f} seconds")
    
    # Run two-phase
    print("Running two-phase aggregation...")
    time_two_phase, count_two_phase = two_phase_aggregation()
    two_phase_times.append(time_two_phase)
    print(f"Two-phase time: {time_two_phase:.2f} seconds")
    
    # Verify results match
    print(f"Results match: {count_single == count_two_phase}")

# Calculate and show average results
avg_single_phase = sum(single_phase_times) / num_trials
avg_two_phase = sum(two_phase_times) / num_trials

print("\nFinal Results:")
print(f"Average single-phase time: {avg_single_phase:.2f} seconds")
print(f"Average two-phase time: {avg_two_phase:.2f} seconds")
print(f"Average improvement: {((avg_single_phase - avg_two_phase) / avg_single_phase * 100):.2f}%")

# Show partition sizes for both approaches
print("\nAnalyzing data distribution:")

def show_partition_stats(df_to_analyze):
    partition_sizes = df_to_analyze.rdd.mapPartitions(
        lambda x: [sum(1 for _ in x)]
    ).collect()
    
    return {
        'min_size': min(partition_sizes),
        'max_size': max(partition_sizes),
        'avg_size': sum(partition_sizes) / len(partition_sizes),
        'num_partitions': len(partition_sizes)
    }

original_stats = show_partition_stats(df)
print("\nOriginal partition statistics:")
print(original_stats)

repartitioned_stats = show_partition_stats(df.repartition(200, "key"))
print("\nRepartitioned statistics:")
print(repartitioned_stats)

# Show memory metrics
print("\nShuffle metrics:")
print(f"Number of keys: {df.select('key').distinct().count()}")
print(f"Average values per key: {df.count() / df.select('key').distinct().count():.2f}")
