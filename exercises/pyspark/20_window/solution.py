#!/usr/bin/env python

"""
Solution
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sql_sum

def main():
    # Create Spark session
    spark = SparkSession.builder.appName("WindowFunctionsSolution").getOrCreate()

    # Read the sales data
    sales_df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)
    sales_df.createOrReplaceTempView("sales")

    # Task 1: Running Totals (SQL)
    print("Task 1: Running Totals")
    query1 = """
    SELECT
        store_id,
        date,
        amount,
        SUM(amount) OVER (
            PARTITION BY store_id
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as running_total
    FROM sales
    ORDER BY store_id, date
    """
    spark.sql(query1).show()

    # Task 1: Running Totals (DataFrame API)
    window_spec1 = Window.partitionBy("store_id") \
                        .orderBy("date") \
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df1 = sales_df.withColumn("running_total",
                             sql_sum("amount").over(window_spec1)) \
                  .orderBy("store_id", "date")
    print("Task 1 (DataFrame API):")
    df1.show()

    # Task 2: Sales Rankings (SQL)
    print("Task 2: Sales Rankings")
    query2 = """
    WITH monthly_sales AS (
        SELECT
            store_id,
            date_format(date, 'MM') as month,
            SUM(amount) as total_sales
        FROM sales
        GROUP BY store_id, date_format(date, 'MM')
    )
    SELECT
        month,
        store_id,
        total_sales,
        RANK() OVER (
            PARTITION BY month
            ORDER BY total_sales DESC
        ) as rank
    FROM monthly_sales
    ORDER BY month, rank
    """
    spark.sql(query2).show()

    # Task 3: Moving Average (SQL)
    print("Task 3: Moving Average")
    query3 = """
    SELECT
        store_id,
        date,
        amount,
        ROUND(
            AVG(amount) OVER (
                PARTITION BY store_id
                ORDER BY date
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ), 2
        ) as moving_avg_3day
    FROM sales
    ORDER BY store_id, date
    """
    spark.sql(query3).show()

    # Task 4: Previous Day Comparison (SQL)
    print("Task 4: Previous Day Comparison")
    query4 = """
    WITH daily_diff AS (
        SELECT
            store_id,
            date,
            amount,
            LAG(amount) OVER (
                PARTITION BY store_id
                ORDER BY date
            ) as prev_day_amount
        FROM sales
    )
    SELECT
        store_id,
        date,
        amount,
        prev_day_amount,
        ROUND(
            ((amount - prev_day_amount) / prev_day_amount) * 100,
            2
        ) as pct_change
    FROM daily_diff
    ORDER BY store_id, date
    """
    spark.sql(query4).show()

    # Task 5: Store Performance (SQL)
    print("Task 5: Store Performance")
    query5 = """
    WITH store_stats AS (
        SELECT
            store_id,
            date,
            amount,
            AVG(amount) OVER (
                PARTITION BY store_id
            ) as store_avg
        FROM sales
    )
    SELECT
        store_id,
        date,
        amount as daily_sales,
        ROUND(store_avg, 2) as avg_daily_sales,
        ROUND(
            ((amount - store_avg) / store_avg) * 100,
            2
        ) as pct_diff_from_avg
    FROM store_stats
    ORDER BY store_id, date
    """
    spark.sql(query5).show()

    # Clean up
    spark.stop()

if __name__ == "__main__":
    main()
