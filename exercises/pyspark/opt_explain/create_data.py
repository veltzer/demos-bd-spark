#!/usr/bin/env python

"""
Create data
"""

import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("Explain Examples Data Setup") \
    .enableHiveSupport() \
    .getOrCreate()

# Create large table for meaningful explain analysis
def create_orders_data(num_records):
    data = []
    start_date = datetime(2024, 1, 1)

    for i in range(num_records):
        order_id = i
        customer_id = random.randint(1, 1000)
        product_id = random.randint(1, 100)
        quantity = random.randint(1, 10)
        amount = round(random.uniform(10.0, 1000.0), 2)
        days_offset = random.randint(0, 365)
        order_date = start_date + timedelta(days=days_offset)

        data.append((order_id, customer_id, product_id, quantity, amount, order_date))

    return spark.createDataFrame(
        data,
        ["order_id", "customer_id", "product_id", "quantity", "amount", "order_date"]
    )

# Create smaller dimension tables
def create_customers_data():
    data = []
    segments = ['Retail', 'Wholesale', 'Enterprise']
    countries = ['USA', 'UK', 'Canada', 'France', 'Germany']

    for i in range(1000):
        customer_id = i + 1
        name = f"Customer_{customer_id}"
        segment = random.choice(segments)
        country = random.choice(countries)
        credit_limit = random.randint(1000, 100000)

        data.append((customer_id, name, segment, country, credit_limit))

    return spark.createDataFrame(
        data,
        ["customer_id", "name", "segment", "country", "credit_limit"]
    )

def create_products_data():
    data = []
    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Home']

    for i in range(100):
        product_id = i + 1
        name = f"Product_{product_id}"
        category = random.choice(categories)
        price = round(random.uniform(10.0, 1000.0), 2)
        stock = random.randint(0, 1000)

        data.append((product_id, name, category, price, stock))

    return spark.createDataFrame(
        data,
        ["product_id", "name", "category", "price", "stock"]
    )

# Create the tables
print("Creating orders table...")
orders_df = create_orders_data(1000000)  # 1M records
orders_df.write.mode("overwrite").saveAsTable("orders")

print("Creating customers table...")
customers_df = create_customers_data()
customers_df.write.mode("overwrite").saveAsTable("customers")

print("Creating products table...")
products_df = create_products_data()
products_df.write.mode("overwrite").saveAsTable("products")

# Compute statistics for better explain output
print("Computing table statistics...")
spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS customer_id, product_id, amount")
spark.sql("ANALYZE TABLE customers COMPUTE STATISTICS FOR COLUMNS customer_id, credit_limit")
spark.sql("ANALYZE TABLE products COMPUTE STATISTICS FOR COLUMNS product_id, price")

# Show table sizes
print("\nTable sizes:")
print("Orders count:", spark.sql("SELECT COUNT(*) as count FROM orders").collect()[0]['count'])
print("Customers count:", spark.sql("SELECT COUNT(*) as count FROM customers").collect()[0]['count'])
print("Products count:", spark.sql("SELECT COUNT(*) as count FROM products").collect()[0]['count'])

# Show sample data
print("\nSample data from each table:")
print("\nOrders sample:")
spark.sql("SELECT * FROM orders LIMIT 3").show()
print("\nCustomers sample:")
spark.sql("SELECT * FROM customers LIMIT 3").show()
print("\nProducts sample:")
spark.sql("SELECT * FROM products LIMIT 3").show()

spark.stop()
