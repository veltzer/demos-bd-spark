# SparkSession vs SparkContext: Key Differences

## SparkSession
SparkSession was introduced in Spark 2.0 as a unified entry point and offers these key features:
- Includes support for DataFrame and SQL operations
- Encapsulates SparkContext, SQLContext, and HiveContext
- Recommended API for newer Spark applications
- Can create multiple SparkSessions within same SparkContext

Example SparkSession usage:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("MyApp") \
    .getOrCreate()

# DataFrame operations
df = spark.read.csv("data.csv")
df.show()

# SQL operations
spark.sql("SELECT * FROM my_table")
```

## SparkContext
SparkContext is the original entry point for Spark functionality and has these characteristics:
- Works with lower level RDD API
- Only one active SparkContext per JVM
- Handles connection to Spark cluster
- Focused on core Spark operations

Example SparkContext usage:

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "MyApp")

# RDD operations
rdd = sc.textFile("data.txt")
rdd.map(lambda x: x.split()).collect()
```

## Accessing SparkContext from SparkSession
You can access SparkContext from SparkSession:
```python
# Get SparkContext from SparkSession
sc = spark.sparkContext
```

Best practice is to use SparkSession unless you specifically need low-level RDD operations that require SparkContext directly.
