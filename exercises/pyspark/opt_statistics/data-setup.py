from pyspark.sql import SparkSession
import time

# Initialize Spark Session with Hive support
spark = SparkSession.builder \
    .appName("Statistics Exercise - Data Setup") \
    .enableHiveSupport() \
    .getOrCreate()

# Create test tables
print("Creating tables...")

# Drop existing tables if they exist
spark.sql("DROP TABLE IF EXISTS products")
spark.sql("DROP TABLE IF EXISTS sales")

# Create products table
spark.sql("""
    CREATE TABLE products (
        id INT,
        name STRING,
        category STRING,
        price DOUBLE
    ) USING PARQUET
""")

# Create sales table
spark.sql("""
    CREATE TABLE sales (
        id INT,
        product_id INT,
        quantity INT,
        sale_date DATE
    ) USING PARQUET
""")

print("Inserting test data...")

# Insert sample data into products
spark.sql("""
    INSERT INTO products
    SELECT 
        id,
        concat('Product_', cast(id as string)) as name,
        concat('Category_', cast(id % 10 as string)) as category,
        RAND() * 1000 as price
    FROM range(1000000)
""")

# Insert sample data into sales
spark.sql("""
    INSERT INTO sales
    SELECT 
        id,
        CAST(RAND() * 1000000 as INT) as product_id,
        CAST(RAND() * 100 as INT) as quantity,
        current_date() - CAST(RAND() * 365 as INT) as sale_date
    FROM range(5000000)
""")

print("Data setup complete!")

# Show table sizes
print("\nTable sizes:")
print("Products count:", spark.sql("SELECT COUNT(*) as count FROM products").collect()[0]['count'])
print("Sales count:", spark.sql("SELECT COUNT(*) as count FROM sales").collect()[0]['count'])

# Show sample data
print("\nProducts sample:")
spark.sql("SELECT * FROM products LIMIT 5").show()
print("\nSales sample:")
spark.sql("SELECT * FROM sales LIMIT 5").show()

# Verify tables are accessible
print("\nVerifying table accessibility:")
print("Tables in default database:", spark.sql("SHOW TABLES").show())

spark.stop()