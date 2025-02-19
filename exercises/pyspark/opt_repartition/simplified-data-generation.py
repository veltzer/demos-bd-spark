#!/usr/bin/env python
"""
Simplified PySpark Repartitioning Exercise - Data Generation Script

This script generates a highly skewed dataset to demonstrate the impact
of repartitioning in PySpark.
"""

import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, when, rand, lit

# Initialize Spark Session with Hive support
spark = SparkSession.builder \
    .appName("Data Generation for Repartitioning Exercise") \
    .config("spark.sql.shuffle.partitions", "50") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Creating test dataset with skewed distribution...")

# Configuration
NUM_TRANSACTIONS = 5000000  # 5 million records
NUM_CUSTOMERS = 500000      # 500,000 customers
NUM_PRODUCTS = 10000        # 10,000 different products
NUM_REGIONS = 10            # 10 geographical regions

def generate_transactions():
    """Generate transaction data with deliberate skew"""
    print("Generating transaction data...")

    # Create skewed configuration
    # 70% of transactions come from 2 regions (dense population areas)
    # 60% of transactions involve 10% of products (popular items)
    popular_products = random.sample(range(NUM_PRODUCTS), int(NUM_PRODUCTS * 0.1))
    dense_regions = random.sample(range(NUM_REGIONS), 2)

    # Create base dataframe with transaction IDs
    df = spark.range(0, NUM_TRANSACTIONS).withColumnRenamed("id", "transaction_id")

    # Add timestamp with recency skew (more recent transactions)
    df = df.withColumn("transaction_date",
                      expr("date_sub(current_date(), cast(rand() * 180 as int))"))

    # Add customer ID
    df = df.withColumn("customer_id", expr(f"cast(rand()*{NUM_CUSTOMERS} as int)"))

    # Add skewed product distribution
    df = df.withColumn(
        "product_id",
        when(rand() < 0.6,  # 60% are popular products
             expr(f"array({','.join(map(str, popular_products))})[cast(rand()*{len(popular_products)} as int)]"))
        .otherwise(expr(f"cast(rand()*{NUM_PRODUCTS} as int)"))
    )

    # Add skewed region distribution
    df = df.withColumn(
        "region_id",
        when(rand() < 0.7,  # 70% from dense regions
             expr(f"array({','.join(map(str, dense_regions))})[cast(rand()*{len(dense_regions)} as int)]"))
        .otherwise(expr(f"cast(rand()*{NUM_REGIONS} as int)"))
    )

    # Add transaction amount
    df = df.withColumn("amount", rand() * 500)

    # Add product category (10 categories, skewed distribution)
    df = df.withColumn(
        "product_category",
        when(rand() < 0.5, lit("electronics"))
        .when(rand() < 0.7, lit("clothing"))
        .when(rand() < 0.85, lit("groceries"))
        .when(rand() < 0.95, lit("home"))
        .otherwise(lit("other"))
    )

    return df

# Generate data
transactions_df = generate_transactions()

# Create a temporary view
transactions_df.createOrReplaceTempView("transactions_temp")

# Save as a table (with partitioning based on region)
print("Saving transaction data...")
spark.sql("""
    CREATE TABLE IF NOT EXISTS transactions
    USING parquet
    PARTITIONED BY (region_id)
    AS SELECT * FROM transactions_temp
""")

# Show distribution statistics
region_distribution = spark.sql("""
    SELECT
        region_id,
        COUNT(*) as transaction_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM transactions_temp), 2) as percentage
    FROM transactions_temp
    GROUP BY region_id
    ORDER BY transaction_count DESC
""").collect()

print("\nRegion Distribution (showing data skew):")
for row in region_distribution:
    print(f"Region {row.region_id}: {row.transaction_count:,} transactions ({row.percentage}%)")

category_distribution = spark.sql("""
    SELECT
        product_category,
        COUNT(*) as transaction_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM transactions_temp), 2) as percentage
    FROM transactions_temp
    GROUP BY product_category
    ORDER BY transaction_count DESC
""").collect()

print("\nProduct Category Distribution (showing data skew):")
for row in category_distribution:
    print(f"{row.product_category}: {row.transaction_count:,} transactions ({row.percentage}%)")

# Verify table was created
print("\nVerifying table was created:")
for table in spark.catalog.listTables():
    if table.name == "transactions":
        print(f" - {table.name} ({table.tableType})")

# Show table count
transactions_count = spark.table("transactions").count()
print(f"\nTransactions table has {transactions_count:,} rows")

print("\nData generation complete! The 'transactions' table is now available for querying.")
print("Run the naive_query.py script to see performance without repartitioning.")
print("Then run optimized_query.py to see the improvement with repartitioning.")

spark.stop()
