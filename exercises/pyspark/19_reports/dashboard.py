#!/usr/bin/env python

"""
Dashboard
"""

import os
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_date, round as my_round, sum as my_sum, desc
import plotly.express as px

# Initialize Spark
spark = SparkSession.builder \
    .appName("Dashboard") \
    .getOrCreate()

def ensure_directory(path):
    """Create directory if it doesn't exist."""
    if not os.path.exists(path):
        os.makedirs(path)

def generate_spark_data():
    """Generate and save the Spark analysis results."""
    # Read the original data
    sales_df = spark.read.csv("data/sales_transactions.csv", header=True, inferSchema=True)
    products_df = spark.read.csv("data/products.csv", header=True, inferSchema=True)

    # Calculate monthly revenue
    monthly_revenue = sales_df \
        .withColumn('date', to_date(col('date'))) \
        .withColumn('month', date_format(col('date'), 'yyyy-MM')) \
        .withColumn('revenue', col('quantity') * col('unit_price')) \
        .groupBy('month') \
        .agg(my_round(my_sum('revenue'), 2).alias('total_revenue')) \
        .orderBy('month')

    # Calculate top products
    top_products = sales_df \
        .join(products_df, 'product_id') \
        .withColumn('revenue', col('quantity') * col('unit_price')) \
        .groupBy('product_id', 'product_name', 'category') \
        .agg(my_round(my_sum('revenue'), 2).alias('total_revenue')) \
        .orderBy(desc('total_revenue')) \
        .limit(10)

    # Ensure reports directory exists
    ensure_directory('reports')

    # Save results
    monthly_revenue.write.mode('overwrite').parquet("reports/monthly_revenue.parquet")
    top_products.write.mode('overwrite').parquet("reports/top_products.parquet")

def load_spark_data():
    """Load the Spark analysis results."""
    try:
        sales_df = spark.read.parquet("reports/monthly_revenue.parquet")
        products_df = spark.read.parquet("reports/top_products.parquet")
    # pylint: disable=broad-exception-caught
    except Exception as _:
        st.error("Data not found. Generating new data...")
        generate_spark_data()
        sales_df = spark.read.parquet("reports/monthly_revenue.parquet")
        products_df = spark.read.parquet("reports/top_products.parquet")

    return sales_df, products_df

def create_dashboard():
    st.title("Sales Analytics Dashboard")

    # Add refresh button
    if st.button("Refresh Data"):
        generate_spark_data()

    # Load data
    sales_df, products_df = load_spark_data()

    # Convert to pandas for visualization
    sales_pd = sales_df.toPandas()
    products_pd = products_df.toPandas()

    # Create visualizations
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Monthly Revenue")
        fig1 = px.line(sales_pd,
                      x='month',
                      y='total_revenue',
                      title='Revenue Trends')
        st.plotly_chart(fig1)

    with col2:
        st.subheader("Top Products")
        fig2 = px.bar(products_pd,
                     x='product_name',
                     y='total_revenue',
                     title='Top Products by Revenue')
        st.plotly_chart(fig2)

    # Add filters
    selected_month = st.selectbox("Select Month", sorted(sales_pd['month'].unique()))
    filtered_data = sales_pd[sales_pd['month'] == selected_month]

    # Show filtered data
    st.subheader(f"Details for {selected_month}")
    st.dataframe(filtered_data)

    # Show raw data tables
    if st.checkbox("Show Raw Data"):
        st.subheader("Monthly Revenue Data")
        st.dataframe(sales_pd)

        st.subheader("Top Products Data")
        st.dataframe(products_pd)

if __name__ == "__main__":
    create_dashboard()
