import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// Create or get the Spark session
val spark = SparkSession.builder()
  .appName("Single Partition Analysis")
  .getOrCreate()

import spark.implicits._

// Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

println("===================================================")
println("ANALYSIS OF SINGLE PARTITION")
println("===================================================")

// Read the full dataset first to get partition info
val salesDataPath = "sales_data"
val fullSalesDF = spark.read.parquet(salesDataPath)

// Count number of partitions
val partitionDistribution = fullSalesDF.groupBy("partition_key").count().collect()
val numPartitions = partitionDistribution.length
println(s"Total number of partitions: $numPartitions")

// Select a single partition for analysis (we'll use partition_key = 1)
val selectedPartition = 1
println(s"Analyzing only partition_key = $selectedPartition")

// Read only the selected partition (more efficient than filtering the full dataset)
val singlePartitionPath = s"$salesDataPath/partition_key=$selectedPartition"
val singlePartitionDF = spark.read.parquet(singlePartitionPath)

// Cache for better performance
singlePartitionDF.cache()

// Count records in this partition
val partitionRecords = singlePartitionDF.count()
val totalRecords = fullSalesDF.count()
val partitionPercentage = (partitionRecords.toDouble / totalRecords) * 100

println(s"Records in partition $selectedPartition: $partitionRecords (${partitionPercentage.round}% of total)")

// Calculate revenue by product for the single partition
val revenueByProduct = singlePartitionDF
  .withColumn("revenue", col("quantity") * col("price"))
  .groupBy("product")
  .agg(
    sum("quantity").as("total_quantity"),
    sum("revenue").as("total_revenue"),
    count("*").as("transaction_count")
  )
  .cache()

// Show top products by revenue in this partition
println("\nTop 10 Products by Revenue (Single Partition):")
revenueByProduct
  .orderBy(desc("total_revenue"))
  .select("product", "total_revenue", "total_quantity", "transaction_count")
  .show(10)

// Calculate top products by quantity sold in this partition
val topProductsByQuantity = singlePartitionDF
  .groupBy("product")
  .agg(sum("quantity").as("total_quantity"))
  .orderBy(desc("total_quantity"))

// Show top 3 products by quantity sold in this partition (MAIN RESULT)
println("\n*** TOP 3 PRODUCTS BY QUANTITY SOLD (SINGLE PARTITION) ***")
topProductsByQuantity.show(3)

// Save the top 3 products for comparison
val top3ProductsSinglePartition = topProductsByQuantity.limit(3).collect().map(_.getString(0))

// Compare with full dataset
// Read the results from the full dataset analysis for comparison
// (In a real scenario, you might store these results or run both analyses in sequence)

// Load the previously saved full dataset result or recompute it
val topProductsFullDataset = fullSalesDF
  .groupBy("product")
  .agg(sum("quantity").as("total_quantity"))
  .orderBy(desc("total_quantity"))
  .limit(3)
  .collect()
  .map(_.getString(0))

println("\nComparison of Top 3 Products:")
println(s"Top 3 products from single partition: ${top3ProductsSinglePartition.mkString(", ")}")
println(s"Top 3 products from full dataset: ${topProductsFullDataset.mkString(", ")}")

// Calculate overlap
val overlap = top3ProductsSinglePartition.intersect(topProductsFullDataset)
println(s"Number of products that appear in both lists: ${overlap.length}")
println(s"Overlap percentage: ${(overlap.length.toDouble / 3) * 100}%")

// Additional analysis for the single partition
println("\nRegion distribution in this partition:")
singlePartitionDF.groupBy("region")
  .agg(count("*").as("count"))
  .orderBy(desc("count"))
  .show()

// Product distribution in this partition vs overall
println("\nHow representative is this partition?")
val partitionDistributionDF = singlePartitionDF
  .groupBy("product")
  .agg(count("*").as("partition_count"))

val fullDistributionDF = fullSalesDF
  .groupBy("product")
  .agg(count("*").as("full_count"))

// Join to compare distributions
val comparisonDF = partitionDistributionDF
  .join(fullDistributionDF, "product")
  .withColumn("partition_percentage", round(col("partition_count") / col("full_count") * 100, 2))
  .orderBy(desc("full_count"))

println("Distribution comparison for top 10 most popular products:")
comparisonDF.show(10)

// Conclusion
println("\nConclusion:")
println("This demonstration shows why analyzing a single partition can lead")
println("to different conclusions than analyzing the full dataset,")
println("especially with non-uniform data distributions.")
