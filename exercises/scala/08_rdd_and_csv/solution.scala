// Read the /etc/passwd file
val passwdDF = spark.read
  .option("delimiter", ":")
  .option("header", "false")
  .csv("/etc/passwd")
  .toDF("username", "password", "uid", "gid", "user_info", "home_dir", "shell")

// Register as a temporary view for SQL queries
passwdDF.createOrReplaceTempView("passwd")

// Count users with /bin/bash shell using SQL
val bashUsers = spark.sql("""
  SELECT COUNT(*) AS bash_users_count
  FROM passwd
  WHERE shell = '/bin/bash'
""")

bashUsers.show()

System.exit(0)
