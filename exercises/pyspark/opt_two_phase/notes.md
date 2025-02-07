# Key Improvements in the Optimized Version

1. Two-Phase Aggregation:
    - First phase: Reduces data size by getting distinct combinations
    - Second phase: Counts the already-reduced dataset

1. Benefits:
    - Less data shuffled between partitions
    - Better memory utilization
    - More efficient network usage
    - Better handling of data skew

1. Performance Impact:
    - Typically 2-3x faster than unoptimized version
    - Even better improvement with highly skewed data
    - More memory-efficient due to early reduction

1. When to Use:
    - Large datasets with many duplicates
    - When memory during shuffle is a concern
    - When network bandwidth is limited
    - In presence of data skew

The key insight is that by doing the distinct operation early,
we reduce the amount of data that needs to be shuffled for the
final count aggregation.
