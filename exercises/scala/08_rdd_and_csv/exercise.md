# Linux /etc/passwd Analysis with Spark RDD

This document provides a simple Spark exercise using RDDs to analyze the Linux `/etc/passwd` file and count users with `/bin/bash` as their shell.

## Using spark-shell

1. Create a file named `passwd_analysis_rdd.scala` with the following content:

```scala
// Read the /etc/passwd file as an RDD
val passwdRDD = sc.textFile("/etc/passwd")

// Parse each line into its components
// Format: username:password:UID:GID:user info:home directory:shell
val parsedRDD = passwdRDD.map(line => {
  val fields = line.split(":")
  fields
})

// Print the schema (first line to see structure)
println("\nSample data from passwd file:")
parsedRDD.take(5).foreach(fields => println(fields.mkString(" | ")))

// Count users with /bin/bash shell
println("\nCounting users with /bin/bash shell:")
val bashUsersCount = parsedRDD.filter(fields => fields.length >= 7 && fields(6) == "/bin/bash").count()
println(s"Number of users with /bin/bash shell: $bashUsersCount")

// Get details of bash users
println("\nUsers with /bin/bash:")
val bashUsers = parsedRDD
  .filter(fields => fields.length >= 7 && fields(6) == "/bin/bash")
  .map(fields => (fields(0), fields(2), fields(5))) // username, UID, home directory
  .collect()

println("Username | UID | Home Directory")
println("-" * 50)
bashUsers.sortBy(_(0)).foreach { case (username, uid, homeDir) =>
  println(s"$username | $uid | $homeDir")
}

// Exit the shell when done
System.exit(0)
```

1. Run the script using spark-shell:

```bash
spark-shell -i passwd_analysis_rdd.scala
```

## Expected Output

The script will display:
- A sample of the data (first 5 rows)
- The count of users with `/bin/bash` as their shell
- A list of all users using `/bin/bash` with their usernames, UIDs, and home directories

## Troubleshooting

- Make sure you have read permissions for `/etc/passwd`
- If using a different file path, update the `sc.textFile("/etc/passwd")` line
- Ensure you have enough memory allocated to Spark

## RDD vs DataFrame Approach

This example demonstrates the RDD API, which is Spark's lower-level API:
- More control over data processing
- Requires manual parsing of data
- Less optimized than DataFrame/SQL operations
- Useful for understanding Spark's fundamentals

For production use cases, the DataFrame/SQL approach (shown in the previous example) is typically preferred due to its optimization capabilities and higher-level abstractions.
