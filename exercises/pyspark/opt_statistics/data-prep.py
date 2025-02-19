#!/usr/bin/env python

"""
PySpark Data Generation Script for ANALYZE TABLE Exercise
This script creates two tables with skewed data distribution:
1. orders - A large fact table with order information
2. customers - A dimension table with customer details
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, rand, col, when, lit
import random

# Initialize Spark Session with Hive support to ensure tables are properly registered
spark = SparkSession.builder \
    .appName("Data Generation for ANALYZE TABLE Exercise") \
    .config("spark.sql.shuffle.partitions", "20") \
    .enableHiveSupport() \
    .getOrCreate()

# Set log level to reduce console output
spark.sparkContext.setLogLevel("WARN")

print("Creating test dataset...")

# Constants
NUM_CUSTOMERS = 1000000  # 1 million customers
NUM_ORDERS = 10000000    # 10 million orders
NUM_PRODUCTS = 10000     # 10,000 product types
NUM_REGIONS = 5          # 5 regions
NUM_PARTITIONS = 100     # Number of partitions to write

# Create a skewed customer distribution
# Most customers will be in region_id=1, fewer in other regions
def generate_customers():
    print("Generating customer data...")
    
    # Create basic customer dataframe
    df = spark.range(0, NUM_CUSTOMERS) \
        .withColumnRenamed("id", "customer_id") \
        .withColumn("name", expr("concat('Customer_', cast(customer_id as string))")) \
        .withColumn("email", expr("concat('customer_', cast(customer_id as string), '@example.com')")) \
        .withColumn("account_balance", expr("round(rand() * 10000, 2)")) \
        .withColumn("active", expr("rand() > 0.2"))
    
    # Create skewed region_id distribution (region_id=1 will have ~70% of customers)
    df = df.withColumn("region_id", when(expr("rand() < 0.7"), 1)
                      .when(expr("rand() < 0.5"), 2)
                      .when(expr("rand() < 0.5"), 3)
                      .when(expr("rand() < 0.5"), 4)
                      .otherwise(5))
    
    return df

# Create orders with skewed distribution
# - Most orders will be from a small subset of customers
# - Product distribution will be skewed (some products ordered frequently)
def generate_orders():
    print("Generating order data...")
    
    # Create a dataframe with order IDs
    df = spark.range(0, NUM_ORDERS).withColumnRenamed("id", "order_id")
    
    # Generate skewed customer IDs (some customers order much more frequently)
    # About 10% of customers generate 80% of orders
    frequent_customers = random.sample(range(NUM_CUSTOMERS), int(NUM_CUSTOMERS * 0.1))
    
    df = df.withColumn(
        "customer_id", 
        when(expr("rand() < 0.8"), 
             expr(f"array({','.join(map(str, frequent_customers))})[cast(rand()*{len(frequent_customers)} as int)]"))
        .otherwise(expr(f"cast(rand()*{NUM_CUSTOMERS} as int)"))
    )
    
    # Add order date (last 2 years, skewed towards recent)
    df = df.withColumn(
        "order_date", 
        expr("date_sub(current_date(), cast(rand() * rand() * 730 as int))")
    )
    
    # Add order amount (skewed towards smaller amounts)
    df = df.withColumn(
        "order_total", 
        expr("case when rand() < 0.7 then rand() * 100 else rand() * 1000 end")
    )
    
    # Add product_id with skewed distribution
    popular_products = random.sample(range(NUM_PRODUCTS), int(NUM_PRODUCTS * 0.05))
    
    df = df.withColumn(
        "product_id",
        when(expr("rand() < 0.6"),
            expr(f"array({','.join(map(str, popular_products))})[cast(rand()*{len(popular_products)} as int)]"))
        .otherwise(expr(f"cast(rand()*{NUM_PRODUCTS} as int)"))
    )
    
    # Add status with distribution
    df = df.withColumn(
        "status",
        when(expr("rand() < 0.7"), lit("COMPLETED"))
        .when(expr("rand() < 0.5"), lit("SHIPPED"))
        .when(expr("rand() < 0.5"), lit("PROCESSING"))
        .otherwise(lit("CANCELLED"))
    )
    
    return df

# Main execution
customers_df = generate_customers()
orders_df = generate_orders()

# Create a temporary view first
customers_df.createOrReplaceTempView("customers_temp")
orders_df.createOrReplaceTempView("orders_temp")

# Save the data as managed tables in the Spark catalog
print("Saving customer data...")
spark.sql("""
    CREATE TABLE IF NOT EXISTS customers
    USING parquet
    PARTITIONED BY (region_id)
    AS SELECT * FROM customers_temp
""")

print("Saving order data...")
spark.sql("""
    CREATE TABLE IF NOT EXISTS orders 
    USING parquet
    PARTITIONED BY (status)
    AS SELECT * FROM orders_temp
""")

# Print summary info
print("\nData generation complete!")
print(f"Created {NUM_CUSTOMERS:,} customers across {NUM_REGIONS} regions")
print(f"Created {NUM_ORDERS:,} orders with {NUM_PRODUCTS:,} different products")
print("\nTable 'customers' and 'orders' are now available for querying")
print("Use the slow_query.py and optimized_query.py scripts to test performance")

# Verify tables were created
print("\nVerifying tables were created:")
print("Tables in the catalog:")
for table in spark.catalog.listTables():
    print(f" - {table.name} ({table.tableType})")

# Show table counts
customers_count = spark.table("customers").count()
orders_count = spark.table("orders").count()
print(f"\nCustomers table has {customers_count:,} rows")
print(f"Orders table has {orders_count:,} rows")

spark.stop()
