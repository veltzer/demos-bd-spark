# Two phase optimization exercise

1. Run the unoptimized version and note:
    - Execution time
    - Query plan (particularly the shuffle operation)
    - Memory usage

1. Implement a two-phase aggregation approach that:
    - First gets distinct users per event type at partition level
    - Then performs final distinct count globally

1. Compare the performance and explain why it's faster

Questions to consider:
- How does data skew affect the performance?
- What's happening during the shuffle phase?
- How much memory is being used in each approach?

Hints:
- Use distinct() before groupBy
- Consider the amount of data being shuffled
- Think about partition-level operations
