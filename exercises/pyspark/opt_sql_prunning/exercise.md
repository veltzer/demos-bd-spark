# Exercise Tasks:

1. Run the code and observe the execution plans and times for both queries.
    - Note the difference in execution time
    - Look for "PushedFilters" and "PartitionFilters" in the explain output
1. Modify the code to:
    - Add more partitioning columns (e.g., partition by both date and region)
    - Try different date ranges to see impact on pruning
    - Add more complex filter conditions
1. Questions to consider:
    - Why is partition pruning beneficial?
    - What makes a good partitioning strategy?
    - When might partition pruning not help?
    - How does the size of the date range affect performance?
1. Additional experiments:
    - Try different partition columns
    - Compare performance with different data volumes
    - Test with different types of queries
