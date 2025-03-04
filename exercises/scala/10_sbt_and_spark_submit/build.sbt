name := "SparkScalaApp"
version := "1.0"
scalaVersion := "2.12.15"  // Use a version compatible with your Spark version

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0"
)

// Add JVM options to fix IllegalAccessError
javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)

// If you're using fork:
fork := true
