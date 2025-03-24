// Run with: spark-shell -i solution.scala

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import java.time.{Instant, Duration}

println("Starting repartitioning solution...")

// Path where the original data is stored (from the create_data.scala script)
val originalDataPath = "/tmp/shared/sales_data"

// Path for the new data partitioned by product
val newDataPath = "/tmp/shared/sales_data_by_product"

// Product to test query performance on
val testProduct = "Laptop"

// Function to measure execution time
def time[R](block: => R): (R, Long) = {
  val start = Instant.now()
  val result = block
  val end = Instant.now()
  val duration = Duration.between(start, end).toMillis()
  (result, duration)
}

// Read the original data
println("Reading original data...")
val originalData = spark.read.format("parquet").load(originalDataPath)

// Count total records for verification
val totalRecords = originalData.count()
println(s"Total records: $totalRecords")

// 1. Analyze original data partitioning
println("\n=== Original Data Partition Structure ===")
val yearPartitions = spark.read.format("parquet").load(originalDataPath).select("year").distinct().collect().map(_.getInt(0)).sorted
val monthPartitions = spark.read.format("parquet").load(originalDataPath).select("month").distinct().collect().map(_.getInt(0)).sorted
val dayPartitions = spark.read.format("parquet").load(originalDataPath).select("day").distinct().collect().map(_.getInt(0)).sorted

println(s"Year partitions: ${yearPartitions.mkString(", ")}")
println(s"Month partitions: ${monthPartitions.mkString(", ")}")
println(s"Day partitions: ${dayPartitions.mkString(", ")}")

// 2. Create a new copy partitioned by product
println("\n=== Repartitioning Data by Product ===")
println(s"Writing new data to $newDataPath...")

// Ensure the directory exists
new java.io.File(newDataPath).mkdirs()

// Write with partitioning by product
originalData.write
  .partitionBy("product")
  .mode("overwrite")
  .format("parquet")
  .save(newDataPath)

println("Data repartitioning complete")

// 3. Analyze product partitioning
println("\n=== New Data Partition Structure ===")
val productPartitions = spark.read.format("parquet").load(newDataPath).select("product").distinct().collect().map(_.getString(0))
println(s"Product partitions: ${productPartitions.mkString(", ")}")

// 4. Test query performance
println("\n=== Performance Comparison ===")

// Test 1: Count records for a specific product in original data
println(s"\nTest 1: Count records for product '$testProduct' in original data")
val (originalCount, originalTime) = time {
  originalData.filter(col("product") === testProduct).count()
}
println(s"Result: $originalCount records")
println(s"Time: $originalTime ms")

// Test 2: Count records for a specific product in new data
println(s"\nTest 2: Count records for product '$testProduct' in new data")
val newData = spark.read.format("parquet").load(newDataPath)
val (newCount, newTime) = time {
  newData.filter(col("product") === testProduct).count()
}
println(s"Result: $newCount records")
println(s"Time: $newTime ms")

// Verify counts match
if (originalCount == newCount) {
  println("\nVerification: Record counts match ✓")
} else {
  println(s"\nVerification: ERROR - Record counts don't match! Original: $originalCount, New: $newCount")
}

// Performance comparison
val speedup = originalTime.toDouble / newTime.toDouble
println(s"\nPerformance comparison for counting records of product '$testProduct':")
println(s"Original data (partitioned by date): $originalTime ms")
println(s"New data (partitioned by product): $newTime ms")
println(f"Speedup: ${speedup}%.2fx ${if (speedup > 1) "faster" else "slower"}")

// Test 3: Calculate average price for a specific product in original data
println(s"\nTest 3: Calculate average price for product '$testProduct' in original data")
val (originalAvg, originalAvgTime) = time {
  originalData.filter(col("product") === testProduct).agg(avg("unit_price").as("avg_price")).collect()(0).getDouble(0)
}
println(f"Result: Average price $$$originalAvg%.2f")
println(s"Time: $originalAvgTime ms")

// Test 4: Calculate average price for a specific product in new data
println(s"\nTest 4: Calculate average price for product '$testProduct' in new data")
val (newAvg, newAvgTime) = time {
  newData.filter(col("product") === testProduct).agg(avg("unit_price").as("avg_price")).collect()(0).getDouble(0)
}
println(f"Result: Average price $$$newAvg%.2f")
println(s"Time: $newAvgTime ms")

// Performance comparison for aggregation
val aggregationSpeedup = originalAvgTime.toDouble / newAvgTime.toDouble
println(s"\nPerformance comparison for calculating average price of product '$testProduct':")
println(s"Original data (partitioned by date): $originalAvgTime ms")
println(s"New data (partitioned by product): $newAvgTime ms")
println(f"Speedup: ${aggregationSpeedup}%.2fx ${if (aggregationSpeedup > 1) "faster" else "slower"}")

// Explain the query plans for both datasets
println("\n=== Query Plans ===")

println("\nOriginal data query plan:")
originalData.filter(col("product") === testProduct).explain()

println("\nNew data query plan:")
newData.filter(col("product") === testProduct).explain()

println("\nRepartitioning exercise complete!")
System.exit(0)
