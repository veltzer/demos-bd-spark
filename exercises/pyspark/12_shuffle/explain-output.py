#!/usr/bin/env python

"""
Explain output
"""

from pyspark.sql import SparkSession, functions


def show_plan(df, title):
    """ Function to capture and print explain plan """
    print(f"{title}")
    print("=" * len(title))
    # Print the plan explanation using the public API
    df.explain(True)

def main():
    spark = SparkSession.builder.appName("ExplainOutput").getOrCreate()

    # Create sample data
    data = [
        ("product1", 100),
        ("product1", 150),
        ("product2", 200),
        ("product1", 300),
        ("product2", 250),
        ("product3", 175),
        ("product3", 225)
    ]
    df = spark.createDataFrame(data, ["product", "amount"])

    # Inefficient approach using groupBy + collect_list
    inefficient = df.groupBy("product") \
        .agg(functions.collect_list("amount").alias("amounts")) \
        .selectExpr("product", "aggregate(amounts, 0D, (acc, x) -> acc + x) as total")

    # show_plan(inefficient, "INEFFICIENT PLAN (groupBy + collect_list + aggregate):")

    # Efficient approach using direct sum
    efficient = df.groupBy("product") \
        .agg(functions.sum("amount").alias("total"))

    show_plan(efficient, "EFFICIENT PLAN (groupBy + sum):")

    # Execute both plans to see results
    print("Results from inefficient plan:")
    inefficient.show()

    print("Results from efficient plan:")
    efficient.show()

    # Show the actual execution times
    print("Timing Comparison:")
    print("==================")

    inefficient.explain(mode="cost")
    efficient.explain(mode="cost")

if __name__ == "__main__":
    main()
