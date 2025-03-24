// Run it with: spark-shell -i create_skewed_data.scala

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Date
import java.util.Calendar
import scala.util.Random

// Parameters - modify as needed
val numRecords = 10000
val outputPath = "/tmp/shared/skewed_sales_data"  // Local path instead of HDFS
val format = "parquet"

// Create a random generator with a fixed seed for reproducibility
val random = new Random(42)

// Define possible values for categorical fields with intentionally skewed distributions
val products = Array(
  "Laptop", "Desktop", "Tablet", "Smartphone", "Headphones", 
  "Monitor", "Keyboard", "Mouse", "Printer", "Scanner", 
  "Camera", "Speaker", "Router", "Hard Drive", "SSD"
)

// Skewed product weights - some products will be much more common than others
val productWeights = Array(
  45, 20, 10, 50, 30,   // Popular products (Laptop, Desktop, Tablet, Smartphone, Headphones)
  8, 7, 15, 5, 3,       // Moderately popular (Monitor, Keyboard, Mouse, Printer, Scanner)
  2, 4, 6, 3, 2         // Less popular (Camera, Speaker, Router, Hard Drive, SSD)
)

// Cumulative sum for weighted selection
val productCumulativeWeights = productWeights.scanLeft(0)(_ + _).tail

// Function to select a product based on weights
def selectWeightedProduct(): String = {
  val value = random.nextInt(productCumulativeWeights.last)
  val index = productCumulativeWeights.indexWhere(_ > value)
  products(index)
}

val categories = Array(
  "Electronics", "Computers", "Accessories", "Audio", "Photography", "Networking", "Storage"
)

val regions = Array(
  "North America", "South America", "Europe", "Asia", "Africa", "Oceania"
)

// Skewed region weights - some regions will have many more sales
val regionWeights = Array(50, 10, 30, 25, 5, 5)
val regionCumulativeWeights = regionWeights.scanLeft(0)(_ + _).tail

// Function to select a region based on weights
def selectWeightedRegion(): String = {
  val value = random.nextInt(regionCumulativeWeights.last)
  val index = regionCumulativeWeights.indexWhere(_ > value)
  regions(index)
}

val countries = Map(
  "North America" -> Array("USA", "Canada", "Mexico"),
  "South America" -> Array("Brazil", "Argentina", "Colombia", "Chile"),
  "Europe" -> Array("UK", "Germany", "France", "Italy", "Spain"),
  "Asia" -> Array("China", "Japan", "India", "South Korea", "Singapore"),
  "Africa" -> Array("South Africa", "Egypt", "Nigeria", "Kenya"),
  "Oceania" -> Array("Australia", "New Zealand")
)

// Skewed country weights within each region
val countryWeights = Map(
  "North America" -> Array(80, 15, 5),
  "South America" -> Array(60, 20, 15, 5),
  "Europe" -> Array(30, 30, 20, 15, 5),
  "Asia" -> Array(40, 30, 20, 5, 5),
  "Africa" -> Array(40, 30, 20, 10),
  "Oceania" -> Array(90, 10)
)

// Function to select a weighted country within a region
def selectWeightedCountry(region: String): String = {
  val weights = countryWeights(region)
  val cumulativeWeights = weights.scanLeft(0)(_ + _).tail
  val value = random.nextInt(cumulativeWeights.last)
  val index = cumulativeWeights.indexWhere(_ > value)
  countries(region)(index)
}

val paymentMethods = Array("Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash")

// Generate random date within the last 3 years
def randomDate(): Date = {
  val cal = Calendar.getInstance()
  cal.add(Calendar.YEAR, -random.nextInt(3))
  cal.add(Calendar.DAY_OF_YEAR, -random.nextInt(365))
  new Date(cal.getTimeInMillis)
}

// Generate sales records with skewed distributions
println(s"Generating $numRecords sales records with skewed distributions...")

val salesData = (1 to numRecords).map(_ => {
  val region = selectWeightedRegion()
  val country = selectWeightedCountry(region)
  val product = selectWeightedProduct()
  
  // Make category somewhat correlated with product to create more skew
  val categoryIndex = (products.indexOf(product) % categories.length)
  val category = categories(categoryIndex)
  
  val paymentMethod = paymentMethods(random.nextInt(paymentMethods.length))
  
  // Make quantity and price somewhat related to the product
  val productIndex = products.indexOf(product)
  val baseQuantity = if (productIndex < 5) 1 else if (productIndex < 10) 2 else 3
  val quantity = baseQuantity + random.nextInt(5)
  
  val basePrice = (productIndex % 5) * 200 + 100
  val unitPrice = basePrice + (random.nextDouble() * 100.0)
  
  val discount = random.nextDouble() * 0.3 // Max 30% discount
  val tax = 0.05 + (random.nextDouble() * 0.15) // Tax between 5-20%
  
  val subtotal = quantity * unitPrice
  val discountAmount = subtotal * discount
  val taxAmount = (subtotal - discountAmount) * tax
  val total = subtotal - discountAmount + taxAmount
  
  val date = randomDate()
  
  (
    java.util.UUID.randomUUID().toString,
    date,
    product,
    category,
    region,
    country,
    quantity,
    unitPrice,
    discount,
    discountAmount,
    tax,
    taxAmount,
    subtotal,
    total,
    paymentMethod
  )
}).toDF(
  "order_id", "date", "product", "category", "region", "country", 
  "quantity", "unit_price", "discount_rate", "discount_amount", 
  "tax_rate", "tax_amount", "subtotal", "total", "payment_method"
)

// Register as a temporary view/table to make it accessible for SQL queries
salesData.createOrReplaceTempView("sales")

// Save the data with partitioning by product and region (which are now skewed)
println(s"Saving data to $outputPath in $format format with partitioning by product and region...")

// Ensure the directory exists
new java.io.File(outputPath).mkdirs()

// Write with partitioning by product and region
salesData.write
  .partitionBy("product", "region")
  .mode("overwrite")
  .format(format)
  .save(outputPath)
  
println(s"Skewed sales data saved to $outputPath in $format format, partitioned by product and region")

// Analyze the partition distribution to verify skew
println("\nAnalyzing partition distribution:")

// Group by the partition keys and count records in each partition
val partitionCounts = salesData.groupBy("product", "region").count().orderBy(desc("count"))

// Show the partition sizes
println("\nPartition sizes (product/region combinations):")
partitionCounts.show(30)

// Show some statistics about the partition sizes
val countValues = partitionCounts.select("count").as[Long].collect()
val minCount = countValues.min
val maxCount = countValues.max
val avgCount = countValues.sum / countValues.length.toDouble
val skewRatio = maxCount.toDouble / minCount.toDouble

println(s"\nPartition Statistics:")
println(s"Number of partitions: ${countValues.length}")
println(s"Smallest partition: $minCount records")
println(s"Largest partition: $maxCount records")
println(s"Average partition size: $avgCount records")
println(s"Skew ratio (largest/smallest): $skewRatio")

// Show the top 5 largest and smallest partitions
println("\nTop 5 largest partitions:")
partitionCounts.show(5)

println("Smallest 5 partitions:")
partitionCounts.orderBy("count").show(5)

// Additional verification using RDD partitioning
println("\nRDD partition information (this shows the physical partitioning, not the logical partitioning):")
val rdd = salesData.rdd
val numRDDPartitions = rdd.getNumPartitions
println(s"Number of RDD partitions: $numRDDPartitions")

val partitionSizes = rdd.glom().map(_.length).collect()
partitionSizes.zipWithIndex.foreach { case (size, index) =>
  println(s"RDD Partition $index: $size elements")
}

// Print instructions for analyzing the data
println("""
Example queries to analyze the skewed data:

// Get the count of records for each product-region combination
spark.sql("SELECT product, region, COUNT(*) as count FROM sales GROUP BY product, region ORDER BY count DESC").show()

// Calculate statistics for partition sizes
spark.sql("SELECT MIN(cnt) as min, MAX(cnt) as max, AVG(cnt) as avg, MAX(cnt)/MIN(cnt) as skew_ratio FROM (SELECT product, region, COUNT(*) as cnt FROM sales GROUP BY product, region)").show()

// Analyze the largest partition
spark.sql("SELECT * FROM sales WHERE product = '<largest_product>' AND region = '<largest_region>' LIMIT 10").show()
""")

System.exit(0)
