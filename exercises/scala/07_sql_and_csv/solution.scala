// Step 1: Get the spark session (should already be available in spark-shell)
val sparkSession = spark
println("Spark session initialized: " + sparkSession)

// Step 2: Create the DataFrameReader
val reader = spark.read
println("Reader created")

// Step 3: Configure the reader
val configuredReader = reader.option("delimiter", ":").option("header", "false")
println("Reader configured")

// Step 4: Read the CSV file
val csvDataFrame = configuredReader.csv("/etc/passwd")
println("CSV read")

// Step 5: Assign column names
val passwdDF = csvDataFrame.toDF("username", "password", "uid", "gid", "user_info", "home_dir", "shell")
println("Column names assigned")

// Step 6: Register the temp view
passwdDF.createOrReplaceTempView("passwd")
println("Temp view created")

// Step 7: Show schema
passwdDF.printSchema()

// Step 8: Show sample data
passwdDF.show(5, false)

// Step 9: Run SQL query
val bashUserCount = spark.sql("SELECT COUNT(*) AS bash_users_count FROM passwd WHERE shell = '/bin/bash'")
bashUserCount.show()

System.exit(0)
