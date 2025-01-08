# PySpark SQL Window Functions Exercise

## Overview
In this exercise, you'll practice using window functions in PySpark SQL. You'll work with a dataset of sales transactions and perform various analytical operations using window functions.

## Setup
First, run the `generate_data.py` script to create your sample dataset. Then load the data using the following code:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WindowFunctionsExercise") \
    .master("local[*]") \
    .getOrCreate()

# Read the sales data
sales_df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)
sales_df.createOrReplaceTempView("sales")
```

## Tasks

### 1. Running Totals
Calculate the running total of sales amount for each store, ordered by date.

### 2. Sales Rankings
Rank stores by their total sales amount within each month.

### 3. Moving Average
Calculate a 3-day moving average of sales for each store.

### 4. Previous Day Comparison
For each store, show the percentage difference in sales compared to the previous day.

### 5. Store Performance
For each store and date:
- Show daily sales
- Show store's average daily sales
- Show percentage difference from store's average

## Expected Output Format
Your queries should produce results in these formats:

Task 1:
```
store_id | date       | amount | running_total
---------|------------|--------|---------------
1        | 2024-01-01 | 100.0  | 100.0
1        | 2024-01-02 | 150.0  | 250.0
```

Task 2:
```
month | store_id | total_sales | rank
------|----------|-------------|------
01    | 1        | 2500.0      | 1
01    | 2        | 2300.0      | 2
```

Tasks 3-5: Similar clear formatting with relevant columns.

## Hints
- Use `PARTITION BY` for store-specific calculations
- Remember to handle NULL values appropriately
- Consider using multiple window functions in a single query

## Submission
Write your queries either using DataFrame API or SQL syntax. Test them with the provided dataset.

Good luck!