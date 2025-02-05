# Caching exercise

* Run the unoptimized code and note the execution time
* Identify where repeated computations occur
* Modify the code to use appropriate caching/persistence
* Run the optimized version and compare execution times

Questions to consider:
* Where is the most expensive computation happening?
* Which RDD would benefit most from caching?
* What storage level would be most appropriate?

Hints:
* Look at the expensive_computation() function calls
* Consider where the RDD lineage is being recomputed
* Think about memory vs CPU trade-offs
