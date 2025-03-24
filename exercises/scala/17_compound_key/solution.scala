// Run it with: spark-shell -i compound_partitioning.scala

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Input and output paths - modify as needed
val inputPath = "/tmp/shared/sales_data"  // Path to the original data
val outputPath = "/tmp/shared/sales_data_compound"  // Path for compound key partitioned data

println("======================================================")
println("STEP 1: Reading original sales data")
println("======================================================")

// Read the original sales data
val salesData = spark.read.format("parquet").load(inputPath)
println(s"Loaded ${salesData.count()} records from original sales data")

// Register as a temporary view/table for SQL queries
salesData.createOrReplaceTempView("sales")

// Ensure the output directory exists
new java.io.File(outputPath).mkdirs()

println("======================================================")
println("STEP 2: Repartitioning by compound key (product, region)")
println("======================================================")

// Write the data with partitioning by product and region
salesData.write
  .partitionBy("product", "region")
  .mode("overwrite")
  .format("parquet")
  .save(outputPath)
  
println(s"Sales data saved to $outputPath in parquet format, partitioned by product and region")

println("======================================================")
println("STEP 3: Analysis of partitioned data")
println("======================================================")

// Analysis 1: Query for all laptops sold around the world
println("\n1. Analyzing all Laptop sales worldwide:")
println("----------------------------------------")
val laptopSales = spark.read.format("parquet").load(outputPath)
  .filter("product = 'Laptop'")

println("Laptop Sales Query Execution Plan (shows partition pruning):")
laptopSales.explain()

val laptopStats = laptopSales.agg(
  count("order_id").as("total_sales"),
  sum("quantity").as("total_units"),
  round(sum("total"), 2).as("total_revenue")
)

println("Laptop Sales Statistics:")
laptopStats.show()

// Analysis 2: Query for all products sold in North America
println("\n2. Analyzing all sales in North America:")
println("----------------------------------------")
val northAmericaSales = spark.read.format("parquet").load(outputPath)
  .filter("region = 'North America'")

println("North America Sales Query Execution Plan (shows partition pruning):")
northAmericaSales.explain()

val northAmericaStats = northAmericaSales.agg(
  count("order_id").as("total_sales"),
  sum("quantity").as("total_units"),
  round(sum("total"), 2).as("total_revenue")
)

println("North America Sales Statistics:")
northAmericaStats.show()

// Analysis 3: Query for laptops sold in North America (uses both partition keys)
println("\n3. Analyzing Laptop sales in North America:")
println("------------------------------------------")
val laptopNorthAmericaSales = spark.read.format("parquet").load(outputPath)
  .filter("product = 'Laptop' AND region = 'North America'")

println("Laptop Sales in North America Query Execution Plan (shows partition pruning):")
laptopNorthAmericaSales.explain()

val laptopNorthAmericaStats = laptopNorthAmericaSales.agg(
  count("order_id").as("total_sales"),
  sum("quantity").as("total_units"),
  round(sum("total"), 2).as("total_revenue")
)

println("Laptop Sales in North America Statistics:")
laptopNorthAmericaStats.show()

// Additional visualization of the partition efficiency
println("\n4. Partition efficiency metrics:")
println("------------------------------------------")

// Count distribution of records across products and regions
val distributionStats = spark.read.format("parquet").load(outputPath)
  .groupBy("product", "region")
  .count()
  .orderBy(desc("count"))

println("Record distribution across compound key partitions (product, region):")
distributionStats.show(10)

// Show example of SQL queries that benefit from the compound partitioning
println("\nSQL query examples that benefit from compound key partitioning:")
spark.sql("""
  SELECT 
    SUM(quantity) as total_units, 
    SUM(total) as total_revenue 
  FROM sales 
  WHERE product = 'Laptop'
""").show()

spark.sql("""
  SELECT 
    SUM(quantity) as total_units, 
    SUM(total) as total_revenue 
  FROM sales 
  WHERE region = 'North America'
""").show()

spark.sql("""
  SELECT 
    SUM(quantity) as total_units, 
    SUM(total) as total_revenue 
  FROM sales 
  WHERE product = 'Laptop' AND region = 'North America'
""").show()

println("\nCompleted all analyses on compound key partitioned data")
System.exit(0)
