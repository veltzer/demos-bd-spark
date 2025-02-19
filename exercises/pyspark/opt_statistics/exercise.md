# Exercise: The Power of ANALYZE TABLE in PySpark

## Overview

This exercise demonstrates how collecting and utilizing statistics with `ANALYZE TABLE` can significantly improve query performance in Spark SQL. You'll see firsthand how the Spark optimizer makes better decisions when it has accurate statistics about your data.

## Background

When Spark executes SQL queries, it needs to make decisions about:
- Join strategies (broadcast vs. shuffle)
- Join order 
- Predicate pushdown optimization
- Partition pruning

Without statistics, Spark makes these decisions based on heuristics and estimates, which can lead to suboptimal execution plans. The `ANALYZE TABLE` command collects statistics about table size, column cardinality, and data distribution that the optimizer can use to create more efficient plans.

## Exercise Components

This exercise includes:
1. A data generation script to create realistic test data
2. A query script that runs slow without statistics
3. The same query script with `ANALYZE TABLE` added, showing improved performance

## Expected Outcomes

You should observe:
- Faster query execution time with statistics
- Different execution plans (visible in the Spark UI)
- Better resource utilization

## Instructions

1. Run the data generation script first
2. Execute the slow query without statistics and note the execution time
3. Execute the optimized query with statistics and compare the execution time
4. Use the Spark UI to examine the execution plans of both queries

## Understanding the Results

The performance difference illustrates how important accurate statistics are for the Spark optimizer, especially when working with:
- Skewed data
- Complex join conditions
- Filtering operations on large datasets
- Tables with many partitions

This is particularly important in production environments where performance and resource utilization matter.
