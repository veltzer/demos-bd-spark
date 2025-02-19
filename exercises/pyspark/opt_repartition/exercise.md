# The Power of Repartitioning in PySpark: Simplified Exercise

## Overview

This simplified exercise demonstrates how proper repartitioning can significantly improve performance in PySpark jobs. You'll see the direct impact of repartitioning on a real-world analytics query using skewed data.

## Learning Objectives

- Understand how data skew impacts Spark performance
- See the direct performance benefit of repartitioning 
- Learn a practical repartitioning strategy

## The Problem: Data Skew

In distributed computing, data skew occurs when data is unevenly distributed across partitions. This creates:
- Overloaded executors while others sit idle
- Resource inefficiency
- Slower job completion (limited by the slowest task)

## Exercise Components

1. **Data Generation Script**: Creates a deliberately skewed customer transaction dataset
2. **Naive Query Script**: Runs an aggregation query without repartitioning
3. **Optimized Query Script**: Runs the identical query with strategic repartitioning

## Instructions

1. Run the data generation script first
2. Execute the naive query script and note its execution time
3. Execute the optimized query script and compare the execution time

## Expected Results

You should observe:
- The optimized script running significantly faster (typically 2-5x)
- More even distribution of work across executors
- Better resource utilization

## Understanding the Solution

The key improvement in the optimized query comes from:

1. **Recognizing the skew**: Some regions and product categories have disproportionate numbers of transactions
2. **Strategic repartitioning**: Redistributing data evenly before the aggregation operation
3. **Proper partition sizing**: Using an appropriate number of partitions for the cluster

This pattern applies to many real-world scenarios including:
- Customer analytics with varying purchase frequencies
- Geographic data with population-based clustering
- Time-series data with activity spikes
