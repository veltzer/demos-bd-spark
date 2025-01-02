# Creating Your First RDD in Apache Spark

## Exercise Overview
Learn how to create and manipulate RDDs (Resilient Distributed Datasets) in Apache Spark using Python data structures. This exercise covers the fundamental concepts of RDD creation and basic operations.

## Prerequisites
- Apache Spark installed
- Basic Python knowledge
- Python environment set up with PySpark

## Learning Objectives
- Understand what an RDD is
- Create RDDs from Python collections
- Perform basic RDD operations
- Learn how to inspect RDD contents

## Exercise Steps

### 1. Creating a SparkContext
First, you'll need to create a SparkContext, which is the entry point for Spark functionality:
```python
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Simple RDD Example").setMaster("local[*]")
sc = SparkContext(conf=conf)
```

### 2. Creating RDDs from Python Lists
Create two different types of RDDs:
- A simple RDD from numbers
- An RDD from key-value pairs

### 3. Basic Operations
Practice these operations:
- Creating RDDs using `parallelize()`
- Viewing RDD contents using `collect()`
- Transforming data using `map()`

## Sample Data
Use these Python collections:
```python
numbers = [1, 2, 3, 4, 5]
pairs = [("a", 1), ("b", 2), ("c", 3)]
```

## Tasks to Complete

1. Create an RDD from the numbers list
2. Create an RDD from the pairs list
3. Print all elements in both RDDs
4. Transform the numbers RDD by squaring each element
5. Print the transformed results

## Expected Output
The program should show:
- The original numbers
- The key-value pairs
- The squared numbers

## Common Pitfalls to Avoid
1. Don't forget to stop the SparkContext when done
2. Remember that `collect()` brings all data to the driver - use with caution on large datasets
3. Don't create multiple SparkContexts

## Extensions
Once you complete the basic exercise, try these extensions:
1. Create an RDD from a larger dataset (100+ elements)
2. Try different transformations (multiply by 2, add 5, etc.)
3. Filter elements based on a condition

## Assessment Criteria
You should be able to:
- Successfully create RDDs from Python collections
- Transform RDD data using simple operations
- Print and verify the results
- Properly shut down the SparkContext

## Notes
- RDD operations are lazy - they won't execute until an action is called
- The `local[*]` master URL means using all available CPU cores
- `collect()` returns all elements to the driver program