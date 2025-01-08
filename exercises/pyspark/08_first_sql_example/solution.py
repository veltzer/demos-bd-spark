#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Passwd File Analysis") \
    .getOrCreate()

# Define the schema for the passwd file
passwd_schema = StructType([
    StructField("username", StringType(), True),
    StructField("password", StringType(), True),
    StructField("uid", IntegerType(), True),
    StructField("gid", IntegerType(), True),
    StructField("comment", StringType(), True),
    StructField("home_directory", StringType(), True),
    StructField("shell", StringType(), True)
])

# Read and parse the passwd file
passwd_df = spark.read \
    .option("delimiter", ":") \
    .schema(passwd_schema) \
    .csv("/etc/passwd")

# Register the DataFrame as a temporary view
passwd_df.createOrReplaceTempView("passwd_table")

# SQL query to count users with bash shell
query = """
SELECT 
    shell,
    COUNT(*) as user_count
FROM passwd_table
WHERE shell = '/bin/bash'
GROUP BY shell
"""

# Execute the query
result = spark.sql(query)

# Show the results
result.show()

# Alternative using DataFrame API
bash_users_count = passwd_df.filter(passwd_df.shell == "/bin/bash").count()
print(f"Number of users with bash shell: {bash_users_count}")

# Clean up
spark.stop()
