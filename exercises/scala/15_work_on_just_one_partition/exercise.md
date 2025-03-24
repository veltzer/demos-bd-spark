# Partitioned Data Analysis in Spark

## Overview

This exercise demonstrates the potential discrepancies that can arise when analyzing only a single partition of data versus the entire dataset. In distributed systems like Spark, data is split across multiple partitions for parallel processing. However, if we draw conclusions based on a single partition, our results might not accurately represent the whole dataset.

## Learning Objectives

- Understand how data partitioning works in Spark
- Learn how to create and manipulate partitioned datasets
- Compare statistical results between a single partition and the entire dataset
- Recognize the importance of working with complete datasets for accurate analysis

## Exercise Description

In this exercise, we'll:

1. Generate a synthetic sales dataset with a non-uniform distribution of product sales
1. Partition the data based on a key
1. Analyze the "top three products sold" across the entire dataset
1. Analyze the "top three products sold" on just one partition
1. Compare the results and understand the discrepancies

## Files Included

1. `data_generator.scala` - Script to generate partitioned sales data with a non-uniform distribution
1. `full_analysis.scala` - Script to analyze the entire dataset
1. `single_partition_analysis.scala` - Script to analyze only one partition

## Running the Exercise

1. Start a Spark shell:
   ```
   spark-shell -i data_generator.scala
   ```

1. Run the full analysis:
   ```
   spark-shell -i full_analysis.scala
   ```

1. Run the single partition analysis:
   ```
   spark-shell -i single_partition_analysis.scala
   ```

## Expected Outcome

You should observe that the top three products from a single partition differ from the top three products in the entire dataset. This highlights why it's crucial to analyze the complete dataset rather than drawing conclusions from a subset when working with distributed data.

## Discussion Questions

1. Why do we see different results between the single partition and the full dataset?
1. In what scenarios might analyzing a single partition be misleading?
1. What strategies could we employ to ensure representative sampling across partitions?
1. How does the non-uniform distribution of product sales impact our observations?
