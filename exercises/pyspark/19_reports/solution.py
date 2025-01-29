#!/usr/bin/env python

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_format, to_date, round as my_round, sum as my_sum, avg, desc,
    countDistinct, min as my_min, max as my_max, months_between, when, count
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SalesAnalysisReport:
    def __init__(self, spark_session=None):
        """Initialize the sales analysis report with a Spark session."""
        self.spark = spark_session or SparkSession.builder \
            .appName("Sales Analysis Report") \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "10g") \
            .getOrCreate()

        logger.info("Spark session initialized")
        self.sales_df = None
        self.products_df = None
        self.customers_df = None

    def load_data(self, sales_path, products_path, customers_path):
        """Load and cache the required datasets."""
        self.sales_df = self.spark.read.csv(sales_path, header=True, inferSchema=True)
        self.products_df = self.spark.read.csv(products_path, header=True, inferSchema=True)
        self.customers_df = self.spark.read.csv(customers_path, header=True, inferSchema=True)

        # Cache frequently used DataFrames
        self.sales_df.cache()
        self.products_df.cache()
        self.customers_df.cache()

        logger.info("Data loaded successfully")

    def clean_data(self):
        """Clean and validate the data."""
        # Remove duplicates
        self.sales_df = self.sales_df.dropDuplicates(['transaction_id'])

        # Handle missing values
        self.sales_df = self.sales_df.na.fill(0, ['quantity', 'unit_price'])

        # Convert date string to date type
        self.sales_df = self.sales_df.withColumn('date', to_date(col('date')))

        logger.info("Data cleaning completed")

    def calculate_monthly_revenue(self):
        """Calculate monthly revenue trends."""
        monthly_revenue = self.sales_df \
            .withColumn('month', date_format(col('date'), 'yyyy-MM')) \
            .withColumn('revenue', col('quantity') * col('unit_price')) \
            .groupBy('month') \
            .agg(my_round(my_sum('revenue'), 2).alias('total_revenue')) \
            .orderBy('month')

        return monthly_revenue

    def get_top_products(self, n=10):
        """Get top N products by revenue."""
        top_products = self.sales_df \
            .join(self.products_df, 'product_id') \
            .withColumn('revenue', col('quantity') * col('unit_price')) \
            .groupBy('product_id', 'product_name', 'category') \
            .agg(my_round(my_sum('revenue'), 2).alias('total_revenue')) \
            .orderBy(desc('total_revenue')) \
            .limit(n)

        return top_products

    def analyze_customer_segments(self):
        """Analyze performance by customer segment."""
        segment_analysis = self.sales_df \
            .join(self.customers_df, 'customer_id') \
            .withColumn('revenue', col('quantity') * col('unit_price')) \
            .groupBy('customer_segment') \
            .agg(
                countDistinct('customer_id').alias('customer_count'),
                my_round(avg('revenue'), 2).alias('avg_order_value'),
                my_round(my_sum('revenue'), 2).alias('total_revenue')
            )

        return segment_analysis

    def calculate_retention_rate(self):
        """Calculate customer retention rate by month."""
        # Calculate first and last purchase dates for each customer
        customer_activity = self.sales_df \
            .groupBy('customer_id') \
            .agg(
                my_min('date').alias('first_purchase'),
                my_max('date').alias('last_purchase')
            )

        # Calculate retention by month
        retention_rate = customer_activity \
            .withColumn('months_active', months_between(
                col('last_purchase'),
                col('first_purchase')
            )) \
            .groupBy(date_format(col('first_purchase'), 'yyyy-MM').alias('cohort_month')) \
            .agg(
                count('customer_id').alias('total_customers'),
                my_sum(when(col('months_active') >= 1, 1).otherwise(0)).alias('retained_customers')
            ) \
            .withColumn(
                'retention_rate',
                round(col('retained_customers') / col('total_customers') * 100, 2)
            )

        return retention_rate

    def analyze_product_categories(self):
        """Analyze sales patterns by product category."""
        category_analysis = self.sales_df \
            .join(self.products_df, 'product_id') \
            .withColumn('revenue', col('quantity') * col('unit_price')) \
            .withColumn('month', date_format(col('date'), 'yyyy-MM')) \
            .groupBy('category', 'month') \
            .agg(
                my_round(my_sum('revenue'), 2).alias('total_revenue'),
                my_sum('quantity').alias('total_quantity')
            ) \
            .orderBy('category', 'month')

        return category_analysis

    def generate_report(self, output_path):
        """Generate the complete sales analysis report."""
        # Create report components
        monthly_revenue = self.calculate_monthly_revenue()
        top_products = self.get_top_products()
        segment_analysis = self.analyze_customer_segments()
        retention_rate = self.calculate_retention_rate()
        category_analysis = self.analyze_product_categories()

        # Save reports in both CSV and Parquet formats
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        monthly_revenue.write.csv(f"{output_path}/monthly_revenue_{timestamp}.csv")
        monthly_revenue.write.parquet(f"{output_path}/monthly_revenue_{timestamp}.parquet")

        top_products.write.csv(f"{output_path}/top_products_{timestamp}.csv")
        top_products.write.parquet(f"{output_path}/top_products_{timestamp}.parquet")

        segment_analysis.write.csv(f"{output_path}/segment_analysis_{timestamp}.csv")
        segment_analysis.write.parquet(f"{output_path}/segment_analysis_{timestamp}.parquet")

        retention_rate.write.csv(f"{output_path}/retention_rate_{timestamp}.csv")
        retention_rate.write.parquet(f"{output_path}/retention_rate_{timestamp}.parquet")

        category_analysis.write.csv(f"{output_path}/category_analysis_{timestamp}.csv")
        category_analysis.write.parquet(f"{output_path}/category_analysis_{timestamp}.parquet")

        logger.info("Report generated successfully")

    def cleanup(self):
        """Clean up resources."""
        # Uncache DataFrames
        self.sales_df.unpersist()
        self.products_df.unpersist()
        self.customers_df.unpersist()

        # Stop Spark session
        self.spark.stop()

        logger.info("Cleanup completed")

def main():
    """Main function to run the sales analysis report."""
    # Initialize report
    report = SalesAnalysisReport()

    # Set paths
    sales_path = "data/sales_transactions.csv"
    products_path = "data/products.csv"
    customers_path = "data/customers.csv"
    output_path = "reports"

    # Generate report
    report.load_data(sales_path, products_path, customers_path)
    report.clean_data()
    report.generate_report(output_path)

    # Cleanup
    report.cleanup()

if __name__ == "__main__":
    main()
