# Spark SQL Tutorial Exercise

## Overview
This tutorial will guide you through basic operations using Spark SQL. We'll create sample datasets and perform various analytical queries to understand Spark SQL's capabilities.

## Prerequisites
- PySpark installed
- Basic SQL knowledge
- Basic Python knowledge

## Setup

```python
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("SparkSQLTutorial") \
    .master("local[*]") \
    .getOrCreate()
```

## Creating Sample Datasets

### 1. Employee Dataset
```python
# Create employee data
employee_data = [
    (1, "John Doe", "Engineering", 80000, "2020-01-15"),
    (2, "Jane Smith", "Engineering", 85000, "2019-03-20"),
    (3, "Bob Wilson", "Marketing", 65000, "2021-06-10"),
    (4, "Alice Brown", "Marketing", 68000, "2020-09-30"),
    (5, "Charlie Davis", "Engineering", 90000, "2018-12-01")
]

employee_df = spark.createDataFrame(
    employee_data,
    ["id", "name", "department", "salary", "hire_date"]
)
employee_df.createOrReplaceTempView("employees")
```

### 2. Project Dataset
```python
# Create project data
project_data = [
    (101, "Mobile App", 1, "2023-01-01", "2023-06-30", "Completed"),
    (102, "Web Portal", 2, "2023-02-15", "2023-08-15", "In Progress"),
    (103, "Database Migration", 5, "2023-03-01", "2023-09-30", "In Progress")
]

project_df = spark.createDataFrame(
    project_data,
    ["project_id", "project_name", "lead_id", "start_date", "end_date", "status"]
)
project_df.createOrReplaceTempView("projects")
```

## Basic Queries

### 1. Simple SELECT and WHERE
```python
# Find all employees in Engineering department
query1 = """
SELECT name, salary
FROM employees
WHERE department = 'Engineering'
ORDER BY salary DESC
"""
spark.sql(query1).show()
```

### 2. Aggregation and GROUP BY
```python
# Calculate average salary by department
query2 = """
SELECT 
    department,
    COUNT(*) as emp_count,
    ROUND(AVG(salary), 2) as avg_salary,
    MAX(salary) as max_salary,
    MIN(salary) as min_salary
FROM employees
GROUP BY department
ORDER BY avg_salary DESC
"""
spark.sql(query2).show()
```

## Practice Exercises

1. Find the total number of employees in each department
2. List all projects and their lead's name (requires a JOIN)
3. Calculate the minimum and maximum salary in the company

## Solutions

### Exercise 1
```python
query3 = """
SELECT 
    department,
    COUNT(*) as employee_count
FROM employees
GROUP BY department
ORDER BY employee_count DESC
"""
```

### Exercise 2
```python
query4 = """
SELECT 
    p.project_name,
    e.name as lead_name
FROM projects p
JOIN employees e ON p.lead_id = e.id
"""
```

### Exercise 3
```python
query5 = """
SELECT 
    MIN(salary) as min_salary,
    MAX(salary) as max_salary
FROM employees
"""
```

## Clean Up
```python
# Don't forget to stop the Spark session when you're done
spark.stop()
```

## Tips for Working with Spark SQL

1. Always register temporary views using meaningful names
2. Use descriptive column names
3. Clean up resources when done
4. Start with simple queries and build up complexity gradually
