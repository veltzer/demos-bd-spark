#!/usr/bin/env python

"""
Solution
"""

from pyspark.sql import SparkSession

def main():
    # Create Spark session
    spark = SparkSession.builder.appName("SparkSQLTutorial").getOrCreate()

    print("Creating sample datasets...")

    # Create employee dataset
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

    # Create project dataset
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

    # Basic Query 1: Find Engineering employees
    print("Query 1: Engineering employees by salary:")
    query1 = """
    SELECT name, salary
    FROM employees
    WHERE department = 'Engineering'
    ORDER BY salary DESC
    """
    spark.sql(query1).show()

    # Basic Query 2: Department statistics
    print("Query 2: Department statistics:")
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

    # Exercise 1: Employee count by department
    print("Exercise 1: Employee count by department:")
    query3 = """
    SELECT
        department,
        COUNT(*) as employee_count
    FROM employees
    GROUP BY department
    ORDER BY employee_count DESC
    """
    spark.sql(query3).show()

    # Exercise 2: Projects and their leads
    print("Exercise 2: Projects and their leads:")
    query4 = """
    SELECT
        p.project_name,
        e.name as lead_name,
        p.status,
        e.department
    FROM projects p
    JOIN employees e ON p.lead_id = e.id
    """
    spark.sql(query4).show(truncate=False)

    # Exercise 3: Company-wide salary range
    print("Exercise 3: Company-wide salary range:")
    query5 = """
    SELECT
        MIN(salary) as min_salary,
        MAX(salary) as max_salary,
        ROUND(AVG(salary), 2) as avg_salary,
        ROUND(stddev(salary), 2) as salary_stddev
    FROM employees
    """
    spark.sql(query5).show()

    # Additional analysis: Salary distribution
    print("Bonus: Salary distribution by department:")
    query6 = """
    SELECT
        department,
        COUNT(*) as emp_count,
        ROUND(AVG(salary), 2) as avg_salary,
        ROUND(stddev(salary), 2) as salary_stddev,
        MIN(salary) as min_salary,
        MAX(salary) as max_salary,
        ROUND((MAX(salary) - MIN(salary)) / MIN(salary) * 100, 2) as salary_range_pct
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
    """
    spark.sql(query6).show()

    # Clean up
    print("Cleaning up...")
    spark.stop()
    print("Done!")

if __name__ == "__main__":
    main()
