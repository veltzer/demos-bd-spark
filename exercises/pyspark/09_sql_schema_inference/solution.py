#!/usr/bin/env python

"""
Solution
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Initialize Spark Session
cdir = os.path.basename(os.path.dirname(os.path.abspath(__file__)))
spark = SparkSession.builder.appName(cdir).getOrCreate()

# Define the schema for the passwd file
passwd_schema = StructType([
    StructField("username", StringType(), nullable=True),
    StructField("password", StringType(), nullable=True),
    StructField("uid", IntegerType(), nullable=True),
    StructField("gid", IntegerType(), nullable=True),
    StructField("comment", StringType(), nullable=True),
    StructField("home_directory", StringType(), nullable=True),
    StructField("shell", StringType(), nullable=True)
])

# Read and parse the passwd file
passwd_df = spark.read.option("delimiter", ":").schema(passwd_schema).csv("passwd")

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
