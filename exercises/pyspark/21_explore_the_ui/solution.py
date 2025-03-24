#!/usr/bin/env python

"""
solution
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, sum as my_sum, avg, desc, round as my_round, rand

def create_sample_data(spark, num_rows=100000):
    """Create a large enough dataset to make the DAG interesting"""
    data = [(i,
            f"user_{i % 1000}",
            f"product_{i % 100}",
            i % 5 + 1,
            f"category_{i % 10}",
            "tag1,tag2,tag3,tag4,tag5",
            float(i % 1000))
            for i in range(num_rows)]

    return spark.createDataFrame(data,
                               ["id", "user", "product", "rating", "category", "tags", "price"])

def generate_complex_dag():
    # Create Spark session
    cdir = os.path.basename(os.path.dirname(os.path.abspath(__file__)))
    spark = SparkSession.builder.appName(cdir).getOrCreate()

    print("Creating sample data...")
    df = create_sample_data(spark)

    # Cache the dataframe to create a persistence node in the DAG
    df.cache()

    # Force cache materialization
    df.count()

    print("Starting complex transformations...")

    # Branch 1: User Analysis
    print("Processing user analysis...")
    user_stats = df.groupBy("user") \
        .agg(
            count("id").alias("total_purchases"),
            my_round(avg("rating"), 2).alias("avg_rating"),
            my_round(my_sum("price"), 2).alias("total_spent")
        ) \
        .cache()

    # Force materialization
    user_stats.count()

    top_users = user_stats.orderBy(desc("total_spent")).limit(100)
    top_users.count()  # Action to create stage

    # Branch 2: Product Analysis with exploded tags
    print("Processing product analysis...")
    product_tags = df.select(
        "product",
        explode(split("tags", ",")).alias("tag")
    ).cache()

    # Force materialization
    product_tags.count()

    tag_counts = product_tags.groupBy("tag") \
        .agg(count("*").alias("tag_count")) \
        .orderBy(desc("tag_count"))

    tag_counts.count()  # Action to create stage

    # Branch 3: Category Analysis with joins
    print("Processing category analysis...")
    category_stats = df.groupBy("category") \
        .agg(
            count("id").alias("total_sales"),
            my_round(avg("price"), 2).alias("avg_price")
        )

    # Add some randomness to make it interesting
    category_stats = category_stats.withColumn("random_metric", rand())

    # Join back with original data
    enriched_data = df.join(category_stats, "category")

    # Complex aggregation on joined data
    final_stats = enriched_data.groupBy("category") \
        .agg(
            my_round(avg("price"), 2).alias("avg_price"),
            my_round(avg("random_metric"), 4).alias("random_avg"),
            count("id").alias("count")
        ) \
        .cache()

    final_stats.count()  # Force materialization

    # Branch 4: Time series simulation
    print("Processing time series simulation...")
    time_series = spark.range(0, 1000, 1) \
        .withColumn("random_value", rand()) \
        .withColumn("group", (col("id") % 10).cast("string"))  # Cast to string to match category

    windowed_stats = time_series.groupBy("group") \
        .agg(
            my_round(avg("random_value"), 4).alias("avg_value"),
            count("*").alias("window_count")
        )

    windowed_stats.count()  # Force action

    # Final join of all branches - now with string type compatibility
    print("Performing final joins...")
    result = final_stats \
        .join(tag_counts, final_stats.category == tag_counts.tag, "left") \
        .join(windowed_stats, final_stats.category == windowed_stats.group, "left") \
        .select(
            "category",
            "avg_price",
            "random_avg",
            "count",
            "tag_count",
            "window_count",
            "avg_value"
        ) \
        .orderBy(desc("count"))

    # Force final action
    print("Computing final results...")
    result.show(5)

    # Sleep to keep the Spark UI available
    input("Pausing job so you could look at it in the spark ui...")

    spark.stop()

if __name__ == "__main__":
    generate_complex_dag()
