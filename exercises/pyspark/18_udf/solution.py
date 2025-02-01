#!/usr/bin/env python

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Initialize Spark session
cdir = os.path.basename(os.path.dirname(os.path.abspath(__file__)))
spark = SparkSession.builder.appName(cdir).getOrCreate()

# Create sample data
sample_data = [
    ("server1", "192.168.80.1", "2024-01-22"),
    ("server2", "10.0.81.34", "2024-01-22"),
    ("server3", "172.16.82.100", "2024-01-22"),
    ("server4", "192.168.83.55", "2024-01-22"),
    ("server5", "10.0.84.89", "2024-01-22")
]

# Create DataFrame
df = spark.createDataFrame(sample_data, ["server_name", "ip_address", "date"])

# Method 1: Using UDF in DataFrame API
def extract_third_octet(ip):
    """Extract the third number from an IP address."""
    if ip is None:
        return None
    try:
        return int(ip.split('.')[2])
    except (IndexError, ValueError):
        return None

# Register UDF
get_third_octet = udf(extract_third_octet, IntegerType())

# Use UDF in DataFrame
print("Method 1: Using DataFrame API")
df.select("server_name", "ip_address",
          get_third_octet("ip_address").alias("third_octet")) \
  .show()

# Method 2: Using UDF in Spark SQL
# Register the DataFrame as a temporary view
df.createOrReplaceTempView("server_ips")

# Register the UDF for SQL
spark.udf.register("get_third_octet", extract_third_octet, IntegerType())

# Use UDF in SQL query
print("Method 2: Using Spark SQL")
spark.sql("""
    SELECT 
        server_name,
        ip_address,
        get_third_octet(ip_address) as third_octet
    FROM server_ips
""").show()
