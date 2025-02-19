# Sort-Merge Performance Exercise
Understanding the performance impact of pre-sorted data

---

## Problem Statement

In distributed data processing, the sort-merge join is a common operation:
1. Sort both datasets on the join key
2. Merge the sorted datasets

**Question**: Does pre-sorting datasets before a merge operation significantly improve performance?

---

## Learning Objectives

After completing this exercise, you'll understand:
- The computational complexity of sorting and merging operations
- How data organization affects join performance
- Practical performance differences in PySpark
- When to invest in maintaining sorted datasets

---

## Scenario

You're analyzing e-commerce data with two large datasets:
1. **Customer Transactions** (10M records)
2. **Product Catalog** (1M records)

You need to join these datasets by product_id to analyze purchasing patterns.

---

## The Exercise

We'll compare two approaches:

**Approach 1 (Naive)**:
- Load unsorted data
- Perform sort-merge join (PySpark handles sorting automatically)

**Approach 2 (Optimized)**:
- Pre-sort data during ETL
- Merge the pre-sorted datasets

---

## Data Generation

The exercise includes a script to generate:

1. Unsorted datasets with random distribution
1. Sorted datasets (same data, pre-sorted by join key)

Both maintain identical data distributions to ensure fair comparison.

---

## Naive Approach (Unsorted Data)

```python
def naive_solution():
    # Load unsorted data
    transactions = spark.read.parquet("data/unsorted_transactions")
    products = spark.read.parquet("data/unsorted_products")

    # Let Spark handle sorting during join
    result = transactions.join(
        products,
        on="product_id",
        how="inner"
    )

    return result
```

---

## Optimized Approach (Pre-sorted Data)

```python
def optimized_solution():
    # Load pre-sorted data
    transactions = spark.read.parquet("data/sorted_transactions")
    products = spark.read.parquet("data/sorted_products")

    # Join pre-sorted data
    result = transactions.join(
        products,
        on="product_id",
        how="inner"
    )

    return result
```

---

## Performance Measurement

The exercise compares:

1. **Total execution time**
1. **Shuffle read/write** volumes
1. **CPU utilization patterns**
1. **Memory pressure**
1. **Stage completion times**

All metrics are collected from the Spark UI.

---

## Expected Results

The optimized approach should demonstrate:

- Reduced shuffle operations
- Lower CPU utilization
- Faster stage completion times
- Less memory pressure
- Overall reduced execution time

The magnitude of improvement depends on data size and cluster resources.

---

## Understanding Sort-Merge Join

Sort-merge join consists of two phases:
1. **Sort phase**: Each dataset is sorted by join key
1. **Merge phase**: Sorted datasets are merged in a single pass

**Key insight**: When data is pre-sorted, we eliminate the expensive sort phase.

---

## Computational Complexity

- Sorting: O(n log n)
- Merging sorted data: O(n)

For large datasets, the difference between O(n log n) and O(n) becomes significant.

---

## Real-world Implications

In production environments, maintaining sorted datasets can provide:

1. Faster ad-hoc queries
1. More efficient batch processing
1. Reduced cluster resource requirements
1. Lower cloud computing costs
1. Better SLA compliance

---

## Discussion Questions

1. At what data volume does pre-sorting become worthwhile?
1. How does data skew affect the performance difference?
1. What are the trade-offs of maintaining sorted datasets?
1. How would partitioning affect these results?
1. In what scenarios might pre-sorting not help?

---

## Additional Exploration

Try modifying the exercise to explore:

- Different join types (left, right, full outer)
- Various data distributions (uniform vs. skewed)
- Multiple join conditions
- Broadcast joins vs. sort-merge joins
- Impact of partition counts

---

## Conclusion

This exercise demonstrates that:

1. Data organization significantly impacts join performance
1. Pre-sorting can eliminate expensive operations
1. Understanding algorithm complexity helps predict performance
1. Sometimes ETL optimizations provide better returns than query optimizations

---

## Next Steps

1. Run the exercise on your own cluster
1. Examine the Spark UI to understand execution details
1. Try with different dataset sizes
1. Explore how caching affects performance
1. Experiment with different cluster configurations
