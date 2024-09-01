import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TransformCSV {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("CSV Transformation")
      .getOrCreate()

    // Read input CSV file
    val inputDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/opt/spark-data/input.csv")

    // Perform transformations
    val transformedDF = inputDF
      // Example: Change data type of a column
      .withColumn("age", col("age").cast("integer"))
      // Example: Add a new column
      .withColumn("current_year", lit(2024))
      // Example: Perform aggregation
      .groupBy("department")
      .agg(
        avg("salary").alias("avg_salary"),
        count("*").alias("employee_count")
      )

    // Save the transformed data to a new CSV file
    transformedDF.write
      .option("header", "true")
      .csv("/opt/spark-data/output")

    // Stop the SparkSession
    spark.stop()
  }
}
