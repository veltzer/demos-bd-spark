# Installing PySpark

* install packages needed for python

```bash
sudo apt update
sudo apt install python3 python-is-python3 python3-pip
pip install pyspark
```

* write your first pyspark application

```python
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("MinimalRDD") \
    .master("local[*]") \
    .getOrCreate()

# Create RDD from a list of numbers
numbers = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(numbers)

# Double each number
doubled = rdd.map(lambda x: x * 2)

# Print the result
print(doubled.collect())

# Stop Spark
spark.stop()
```

* python [your_script.py]
