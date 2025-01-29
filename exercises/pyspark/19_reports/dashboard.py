#!/usr/bin/env python

import streamlit as st
from pyspark.sql import SparkSession
import plotly.express as px

# Initialize Spark
spark = SparkSession.builder \
    .appName("Dashboard") \
    .getOrCreate()

def load_spark_data():
    # Your existing Spark analysis code
    sales_df = spark.read.parquet("reports/monthly_revenue.parquet")
    products_df = spark.read.parquet("reports/top_products.parquet")
    return sales_df, products_df

def create_dashboard():
    st.title("Sales Analytics Dashboard")

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
                      x="month",
                      y="total_revenue",
                      title="Revenue Trends")
        st.plotly_chart(fig1)

    with col2:
        st.subheader("Top Products")
        fig2 = px.bar(products_pd,
                     x="product_name",
                     y="total_revenue",
                     title="Top Products by Revenue")
        st.plotly_chart(fig2)

    # Add filters
    selected_month = st.selectbox("Select Month", sales_pd["month"].unique())
    filtered_data = sales_pd[sales_pd["month"] == selected_month]

    # Show filtered data
    st.subheader(f"Details for {selected_month}")
    st.dataframe(filtered_data)

if __name__ == "__main__":
    create_dashboard()
