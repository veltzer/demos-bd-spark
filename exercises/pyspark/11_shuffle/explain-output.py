#!/usr/bin/env python

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

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

# Function to capture and print explain plan
def show_plan(df, title):
    print(f"\n{title}")
    print("=" * len(title))
    # Get the plan explanation
    plan = []
    df._jdf.explain(True, lambda x: plan.append(x))
    print("\n".join(plan))

# Inefficient approach using groupBy + collect_list
inefficient = df.groupBy("product") \
    .agg(F.collect_list("amount").alias("amounts")) \
    .selectExpr("product", "aggregate(amounts, 0D, (acc, x) -> acc + x) as total")

# show_plan(inefficient, "INEFFICIENT PLAN (groupBy + collect_list + aggregate):")

# Efficient approach using direct sum
efficient = df.groupBy("product") \
    .agg(F.sum("amount").alias("total"))

show_plan(efficient, "EFFICIENT PLAN (groupBy + sum):")

# Execute both plans to see results
print("\nResults from inefficient plan:")
inefficient.show()

print("\nResults from efficient plan:")
efficient.show()

# Show the actual execution times
print("\nTiming Comparison:")
print("==================")

inefficient.explain(mode="cost")
efficient.explain(mode="cost")
