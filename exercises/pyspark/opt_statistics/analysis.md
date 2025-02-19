# ANALYZE TABLE Performance Comparison

After running both scripts, you should see a significant performance difference. This document helps you understand why and how to interpret the results.

## Expected Performance Difference

Typically, you'll observe:
- The query with `ANALYZE TABLE` runs **2-10x faster** depending on data size and skew
- The optimized query execution plan shows different join strategies
- The optimized query may process fewer partitions

## What's Happening Behind the Scenes

### Without Statistics (slow_query.py)

1. **Join Strategy Selection**: Without statistics, Spark doesn't know which table is larger, so it may choose a shuffle join (expensive) rather than a broadcast join.

1. **Partition Pruning**: Without statistics, Spark may scan all partitions even though the `status = 'COMPLETED'` filter could eliminate many partitions.

1. **Join Order**: Spark chooses a suboptimal join order because it lacks information about the relative sizes of filtered result sets.

### With Statistics (optimized_query.py)

1. **Better Join Strategy**: With accurate size information, Spark can choose broadcast joins where appropriate.

1. **Effective Partition Pruning**: Spark uses partition-level statistics to read only `COMPLETED` status partitions.

1. **Optimized Join Order**: Spark orders joins more efficiently based on accurate table and column statistics.

1. **Predicate Pushdown**: Statistics help Spark better decide which predicates to push into data source scanning.

## What to Look for in the Execution Plans

When comparing the execution plans, focus on these differences:

1. **Join Strategy**: Look for `BroadcastHashJoin` vs. `SortMergeJoin`
   ```
   // Optimized plan might show:
   BroadcastHashJoin [customer_id] [customer_id]

   // Slow plan might show:
   SortMergeJoin [customer_id] [customer_id]
   ```

1. **Partition Filtering**: Notice the number of partitions read
   ```
   // Optimized plan might show:
   Filter: (isnotnull(status) && (status = 'COMPLETED'))
   ```

1. **Predicate Application**: Look where predicates are applied
   ```
   // Optimized plan pushes predicates into file scan:
   FileScan parquet [customer_id,active,account_balance]
      Batched: true, DataFilters: [isnotnull(active), active, account_balance > 1000.0]
   ```

## Key Takeaways

1. **Always Analyze Important Tables**: Make statistics collection part of your ETL process
1. **Refresh Statistics After Significant Changes**: Re-analyze after bulk loads or updates
1. **Column-Level Statistics Matter**: Use `FOR ALL COLUMNS` for complex queries
1. **Monitor Plan Changes**: Use `EXPLAIN ANALYZED` to track execution plan improvements

## Performance Impact in Production

In production environments, statistics can mean the difference between:
- Jobs completing within SLA vs. timing out
- Efficient resource usage vs. cluster overload
- Cost-effective cloud processing vs. expensive overruns
