// Read the /etc/passwd file
val passwdRDD = spark.sparkContext.textFile("/etc/passwd")

// Filter lines with /bin/bash and count them
val bashUserCount = passwdRDD
  .filter(line => line.split(":").last == "/bin/bash")
  .count()

println(s"Number of users with /bin/bash as their shell: $bashUserCount")
System.exit(0)
