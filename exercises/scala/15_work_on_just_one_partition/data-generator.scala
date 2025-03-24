import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random

// Create or get the Spark session
val spark = SparkSession.builder()
  .appName("Partitioned Data Analysis Exercise")
  .getOrCreate()

import spark.implicits._

// Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

println("Generating partitioned sales data with non-uniform distribution...")

// Define number of records, products, regions and partitions
val numRecords = 100000
val numProducts = 100
val numRegions = 10
val numPartitions = 5

// Create a non-uniform distribution for product popularity
// Some products will be very popular, most will have medium popularity, and some will be rarely sold
def generateProductPopularity(): Map[Int, Double] = {
  val random = new Random(42)  // Fixed seed for reproducibility
  
  // Very popular products (10%)
  val popularProducts = (1 to (numProducts * 0.1).toInt).map { i =>
    (i, 10.0 + random.nextDouble() * 5.0)  // High weight
  }
  
  // Medium popularity products (70%)
  val mediumProducts = ((numProducts * 0.1).toInt + 1 to (numProducts * 0.8).toInt).map { i =>
    (i, 2.0 + random.nextDouble() * 3.0)  // Medium weight
  }
  
  // Low popularity products (20%)
  val lowProducts = ((numProducts * 0.8).toInt + 1 to numProducts).map { i =>
    (i, 0.1 + random.nextDouble() * 0.5)  // Low weight
  }
  
  (popularProducts ++ mediumProducts ++ lowProducts).toMap
}

// Create product popularity distribution
val productPopularity = generateProductPopularity()

// Generate random sales data
def generateSalesData(numRecords: Int): Seq[(Int, String, Int, Double, String)] = {
  val random = new Random(42)  // Fixed seed for reproducibility
  val regions = (1 to numRegions).map(i => s"Region_$i")
  val productIds = productPopularity.keys.toArray
  val productWeights = productPopularity.values.toArray
  
  // Function to select a product based on the weighted distribution
  def selectWeightedProduct(): Int = {
    val totalWeight = productWeights.sum
    val targetValue = random.nextDouble() * totalWeight
    var cumulativeWeight = 0.0
    var selectedIndex = 0
    
    while (selectedIndex < productIds.length) {
      cumulativeWeight += productWeights(selectedIndex)
      if (cumulativeWeight >= targetValue) {
        return productIds(selectedIndex)
      }
      selectedIndex += 1
    }
    
    productIds.last  // Fallback
  }
  
  (1 to numRecords).map { i =>
    val productId = selectWeightedProduct()
    val productName = s"Product_$productId"
    val quantity = 1 + random.nextInt(10)  // 1-10 units per sale
    val price = 5.0 + (productId % 10) * 10 + random.nextDouble() * 5.0  // Price varies by product
    val region = regions(random.nextInt(regions.length))
    
    (i, productName, quantity, price, region)
  }
}

// Generate and convert to DataFrame
val salesData = generateSalesData(numRecords)
val salesDF = salesData.toDF("id", "product", "quantity", "price", "region")

// Add a partition key (will create a non-uniform distribution across partitions)
val salesWithPartitionKeyDF = salesDF.withColumn("partition_key", pmod(col("id"), lit(numPartitions)))

// Cache and count to ensure data is generated
salesWithPartitionKeyDF.cache()
val count = salesWithPartitionKeyDF.count()
println(s"Generated $count sales records")

// Save as parquet with partitioning
val outputPath = "sales_data"
println(s"Saving data to $outputPath")

// Remove previous data if it exists
import org.apache.hadoop.fs.{FileSystem, Path}
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
fs.delete(new Path(outputPath), true)

// Save partitioned data
salesWithPartitionKeyDF.write
  .partitionBy("partition_key")
  .parquet(outputPath)

// Create a view for querying
salesWithPartitionKeyDF.createOrReplaceTempView("sales")

println("Data generation complete!")
println(s"Data saved to $outputPath with $numPartitions partitions")
println("Run 'spark-shell -i full_analysis.scala' to analyze the full dataset")
println("Run 'spark-shell -i single_partition_analysis.scala' to analyze a single partition")

// Display some sample data
println("\nSample data:")
salesWithPartitionKeyDF.show(5)

// Display summary of products distribution
println("\nProduct distribution (sample of 10 products):")
salesWithPartitionKeyDF
  .groupBy("product")
  .agg(count(lit(1)).as("sales_count"))
  .orderBy(desc("sales_count"))
  .show(10)

// Display summary of partition distribution
println("\nPartition distribution:")
salesWithPartitionKeyDF
  .groupBy("partition_key")
  .agg(count("*").as("record_count"))
  .orderBy("partition_key")
  .show()
