#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def create_spark_session():
    """Create and return a Spark session"""
    return SparkSession.builder \
        .appName("IP Address Analysis") \
        .getOrCreate()

def extract_third_octet(ip):
    """Extract the third number from an IP address.

    Args:
        ip (str): IP address in format 'xxx.xxx.xxx.xxx'

    Returns:
        int: Third octet of the IP address or None if invalid
    """
    if ip is None:
        return None
    try:
        return int(ip.split('.')[2])
    except (IndexError, ValueError):
        return None

def analyze_with_dataframe(spark, input_path):
    """Analyze IP addresses using DataFrame API

    Args:
        spark (SparkSession): Active Spark session
        input_path (str): Path to the input CSV file
    """
    # Read the CSV file
    df = spark.read.csv(input_path, header=True)

    # Register UDF
    get_third_octet = udf(extract_third_octet, IntegerType())

    # Analyze using DataFrame API
    result_df = df.select(
        "server_name",
        "ip_address",
        "timestamp",
        "status_code",
        get_third_octet("ip_address").alias("third_octet")
    )

    print("Results using DataFrame API:")
    result_df.show()

    # Additional analysis: Group by third octet
    print("\nCount of records by third octet:")
    result_df.groupBy("third_octet") \
        .count() \
        .orderBy("third_octet") \
        .show()

def analyze_with_sql(spark, input_path):
    """Analyze IP addresses using Spark SQL

    Args:
        spark (SparkSession): Active Spark session
        input_path (str): Path to the input CSV file
    """
    # Read the CSV file
    df = spark.read.csv(input_path, header=True)

    # Register the DataFrame as a temporary view
    df.createOrReplaceTempView("server_logs")

    # Register the UDF for SQL
    spark.udf.register("get_third_octet", extract_third_octet, IntegerType())

    # Analyze using SQL
    print("Results using Spark SQL:")
    spark.sql("""
        SELECT
            server_name,
            ip_address,
            timestamp,
            status_code,
            get_third_octet(ip_address) as third_octet
        FROM server_logs
    """).show()

    # Additional analysis using SQL
    print("\nCount of records by third octet (SQL):")
    spark.sql("""
        SELECT
            get_third_octet(ip_address) as third_octet,
            COUNT(*) as count
        FROM server_logs
        GROUP BY get_third_octet(ip_address)
        ORDER BY third_octet
    """).show()

def main():
    """Main function to run the analysis"""
    # Create Spark session
    spark = create_spark_session()

    # Path to your input file
    input_path = "server_logs.csv"

    # Run both types of analysis
    analyze_with_dataframe(spark, input_path)
    analyze_with_sql(spark, input_path)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
