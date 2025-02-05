from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import time
import random
import hashlib

def create_spark_session():
    return SparkSession.builder \
        .appName("Count Distinct Exercise") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.default.parallelism", "100") \
        .getOrCreate()

def generate_very_skewed_data(spark, size=100000):
    """Generate highly skewed dataset with expensive computation"""
    data = []
    # Create very skewed distribution
    user_weights = [random.paretovariate(1.5) for _ in range(1000)]
    event_types = ['purchase', 'view', 'click', 'share', 'like', 
                  'comment', 'follow', 'unfollow', 'rate', 'bookmark']
    
    for _ in range(size):
        # Highly skewed user_id distribution
        user_id = random.choices(range(1000), weights=user_weights)[0]
        # Add some complexity to the data
        event_type = random.choice(event_types)
        # Complex value that will make computation more expensive
        complex_value = hashlib.md5(f"{user_id}-{random.random()}".encode()).hexdigest()
        data.append((user_id, event_type, complex_value))
    
    # Create DataFrame with more partitions
    df = spark.createDataFrame(data, ['user_id', 'event_type', 'complex_value'])
    return df.repartition(100)

def expensive_computation(value):
    """Add computational overhead"""
    return hashlib.md5(value.encode()).hexdigest()

# Register UDF for expensive computation
expensive_udf = F.udf(expensive_computation, StringType())

def unoptimized_analysis(df):
    """Unoptimized version with expensive computation"""
    print("Starting unoptimized analysis...")
    start_time = time.time()
    
    # Make computation more expensive
    result = df \
        .withColumn('computed_value', expensive_udf(F.col('complex_value'))) \
        .groupBy('event_type') \
        .agg(
            F.countDistinct('user_id', 'computed_value').alias('unique_combinations'),
            F.count('*').alias('total_events')
        )
    
    # Force computation and show results
    result.show()
    
    end_time = time.time()
    print(f"Unoptimized execution time: {end_time - start_time:.2f} seconds")
    print("\nUnoptimized query plan:")
    result.explain()
    
    return end_time - start_time

def optimized_analysis(df):
    """Optimized version using two-phase aggregation"""
    print("Starting optimized analysis...")
    start_time = time.time()
    
    # Phase 1: Get distinct combinations at partition level first
    # This significantly reduces the data before expensive computation
    distinct_combinations = df \
        .select('user_id', 'event_type', 'complex_value') \
        .distinct()
    
    # Phase 2: Apply expensive computation on reduced dataset
    result = distinct_combinations \
        .withColumn('computed_value', expensive_udf(F.col('complex_value'))) \
        .groupBy('event_type') \
        .agg(
            F.countDistinct('user_id', 'computed_value').alias('unique_combinations'),
            F.count('*').alias('total_events')
        )
    
    # Force computation and show results
    result.show()
    
    end_time = time.time()
    print(f"Optimized execution time: {end_time - start_time:.2f} seconds")
    print("\nOptimized query plan:")
    result.explain()
    
    return end_time - start_time

def main():
    spark = create_spark_session()
    
    # Generate test data
    print("Generating test data...")
    df = generate_very_skewed_data(spark)
    df.cache()  # Cache to ensure fair comparison
    df.count()  # Force caching
    
    print("\nDataset statistics:")
    print(f"Total partitions: {df.rdd.getNumPartitions()}")
    df.groupBy('event_type').count().show()
    
    # Run both versions multiple times to see consistent difference
    times_unopt = []
    times_opt = []
    
    for i in range(3):
        print(f"\nIteration {i+1}")
        print("=" * 80)
        times_unopt.append(unoptimized_analysis(df))
        print("-" * 80)
        times_opt.append(optimized_analysis(df))
    
    # Show average times
    print("\nPerformance Summary:")
    print(f"Average unoptimized time: {sum(times_unopt)/len(times_unopt):.2f} seconds")
    print(f"Average optimized time: {sum(times_opt)/len(times_opt):.2f} seconds")
    print(f"Average speedup: {sum(times_unopt)/sum(times_opt):.2f}x")
    
    spark.stop()

if __name__ == "__main__":
    main()
