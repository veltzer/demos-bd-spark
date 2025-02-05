# Key Improvements in the Optimized Version

* Added .cache() after the expensive map transformation
* Force materialization with count() to ensure caching happens immediately
* Added cleanup with unpersist() at the end
* All subsequent operations now use cached data instead of recomputing

Expected Results:
* Unoptimized version: ~30-40 seconds
* Optimized version: ~10-15 seconds (significant improvement)

The performance gain comes from avoiding the repeated execution of
expensive_computation() for each action. In the unoptimized version,
this function is called multiple times for the same data, while in
the optimized version it's only called once and the results are reused.
