import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// Create or get the Spark session
val spark = SparkSession.builder()
  .appName("Full Dataset Analysis")
  .getOrCreate()

import spark.implicits._

// Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

println("===================================================")
println("ANALYSIS OF ENTIRE DATASET")
println("===================================================")

// Read the partitioned data
val salesDataPath = "sales_data"
val salesDF = spark.read.parquet(salesDataPath)

// Register as a temporary view
salesDF.createOrReplaceTempView("sales")

// Count total number of records and partitions
val totalRecords = salesDF.count()
val partitionDistribution = salesDF.groupBy("partition_key").count().collect()
val numPartitions = partitionDistribution.length

println(s"Total number of records: $totalRecords")
println(s"Number of partitions: $numPartitions")

// Show distribution of records across partitions
println("\nDistribution of records across partitions:")
salesDF.groupBy("partition_key")
  .agg(count("*").as("record_count"), 
       round((count("*") / totalRecords.toDouble) * 100, 2).as("percentage"))
  .orderBy("partition_key")
  .show()

// Calculate total revenue by product
val revenueByProduct = salesDF
  .withColumn("revenue", col("quantity") * col("price"))
  .groupBy("product")
  .agg(
    sum("quantity").as("total_quantity"),
    sum("revenue").as("total_revenue"),
    count("*").as("transaction_count")
  )
  .cache()

// Show top products by revenue
println("\nTop 10 Products by Revenue (Full Dataset):")
revenueByProduct
  .orderBy(desc("total_revenue"))
  .select("product", "total_revenue", "total_quantity", "transaction_count")
  .show(10)

// Calculate top products by quantity sold
val topProductsByQuantity = salesDF
  .groupBy("product")
  .agg(sum("quantity").as("total_quantity"))
  .orderBy(desc("total_quantity"))

// Show top 3 products by quantity sold (MAIN RESULT)
println("\n*** TOP 3 PRODUCTS BY QUANTITY SOLD (FULL DATASET) ***")
topProductsByQuantity.show(3)

// Save the top 3 products for later comparison
val top3ProductsFull = topProductsByQuantity.limit(3).collect().map(_.getString(0))

println(s"The top 3 products from the full dataset are: ${top3ProductsFull.mkString(", ")}")
println("\nNote: Compare these results with single_partition_analysis.scala to see the difference!")

// Additional insights
println("\nSales distribution by region:")
salesDF.groupBy("region")
  .agg(
    sum("quantity").as("total_quantity"),
    round(avg("price"), 2).as("avg_price"),
    count("*").as("transaction_count")
  )
  .orderBy(desc("total_quantity"))
  .show()

// Calculate product popularity rank correlation with price
println("\nDoes price correlate with product popularity?")
val pricePopularityCorrelation = salesDF
  .groupBy("product")
  .agg(
    count("*").as("sales_count"),
    avg("price").as("avg_price")
  )
  .orderBy(desc("sales_count"))

// Show correlation between price and popularity
pricePopularityCorrelation.show(10)

// Calculate average price of top 10 vs bottom 10 products
val avgPriceTop10 = pricePopularityCorrelation.limit(10).agg(avg("avg_price")).collect()(0).getDouble(0)
val avgPriceBottom10 = pricePopularityCorrelation.orderBy("sales_count").limit(10).agg(avg("avg_price")).collect()(0).getDouble(0)

println(s"Average price of top 10 most popular products: $avgPriceTop10")
println(s"Average price of bottom 10 least popular products: $avgPriceBottom10")
