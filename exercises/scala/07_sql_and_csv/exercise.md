# Linux /etc/passwd Analysis with Spark SQL

This document provides a simple Spark SQL exercise to analyze the Linux `/etc/passwd` file and count users with `/bin/bash` as their shell.

## Using spark-shell

1. Create a file named `passwd_analysis.scala` with the following content:

```scala
// Read the /etc/passwd file
val passwdDF = spark.read
  .option("delimiter", ":")
  .option("header", "false")
  .csv("/etc/passwd")
  .toDF("username", "password", "uid", "gid", "user_info", "home_dir", "shell")

// Register as a temporary view for SQL queries
passwdDF.createOrReplaceTempView("passwd")

// Display schema and sample data
println("Schema of the passwd file:")
passwdDF.printSchema()

println("\nSample data from passwd file:")
passwdDF.show(5, false)

// Count users with /bin/bash shell using SQL
println("\nCounting users with /bin/bash shell:")
val bashUsers = spark.sql("""
  SELECT COUNT(*) AS bash_users_count
  FROM passwd
  WHERE shell = '/bin/bash'
""")

bashUsers.show()

// List the bash users
println("\nUsers with /bin/bash:")
spark.sql("""
  SELECT username, uid, home_dir
  FROM passwd
  WHERE shell = '/bin/bash'
  ORDER BY username
""").show(false)

// Exit the shell when done
System.exit(0)
```

1. Run the script using spark-shell:

```bash
spark-shell -i passwd_analysis.scala
```

## Expected Output

The script will display:
- The schema of the passwd file
- A sample of the data (first 5 rows)
- The count of users with `/bin/bash` as their shell
- A list of all users using `/bin/bash` with their usernames, UIDs, and home directories

## Troubleshooting

- If you encounter `DataFrameReader` errors, ensure you're using a recent version of Spark
- Make sure you have read permissions for `/etc/passwd`
- If using a different file path, update the code accordingly

## Alternative: Interactive Session

You can also run these commands interactively in the spark-shell for step-by-step analysis and exploration.
