// Run it with: spark-shell -i data-generate.scala

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Date
import java.util.Calendar
import scala.util.Random

// Parameters - modify as needed
val numRecords = 10000
val outputPath = "/tmp/shared/sales_data"  // Local path instead of HDFS
val format = "parquet"

// Create a random generator
val random = new Random()

// Define possible values for categorical fields
val products = Array(
  "Laptop", "Desktop", "Tablet", "Smartphone", "Headphones", 
  "Monitor", "Keyboard", "Mouse", "Printer", "Scanner", 
  "Camera", "Speaker", "Router", "Hard Drive", "SSD"
)

val categories = Array(
  "Electronics", "Computers", "Accessories", "Audio", "Photography", "Networking", "Storage"
)

val regions = Array(
  "North America", "South America", "Europe", "Asia", "Africa", "Oceania"
)

val countries = Map(
  "North America" -> Array("USA", "Canada", "Mexico"),
  "South America" -> Array("Brazil", "Argentina", "Colombia", "Chile"),
  "Europe" -> Array("UK", "Germany", "France", "Italy", "Spain"),
  "Asia" -> Array("China", "Japan", "India", "South Korea", "Singapore"),
  "Africa" -> Array("South Africa", "Egypt", "Nigeria", "Kenya"),
  "Oceania" -> Array("Australia", "New Zealand")
)

val paymentMethods = Array("Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash")

// Generate random date within the last 3 years
def randomDate(): Date = {
  val cal = Calendar.getInstance()
  cal.add(Calendar.YEAR, -random.nextInt(3))
  cal.add(Calendar.DAY_OF_YEAR, -random.nextInt(365))
  new Date(cal.getTimeInMillis)
}

// Generate sales records
println(s"Generating $numRecords sales records...")

val salesData = (1 to numRecords).map(_ => {
  val region = regions(random.nextInt(regions.length))
  val country = countries(region)(random.nextInt(countries(region).length))
  val product = products(random.nextInt(products.length))
  val category = categories(random.nextInt(categories.length))
  val paymentMethod = paymentMethods(random.nextInt(paymentMethods.length))
  
  val quantity = random.nextInt(10) + 1
  val unitPrice = 10.0 + (random.nextDouble() * 990.0)
  val discount = random.nextDouble() * 0.3 // Max 30% discount
  val tax = random.nextDouble() * 0.2 // Tax between 0-20%
  
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

// Show sample data
println("Sample data:")
salesData.show(10)

// Save the data
println(s"Saving data to $outputPath in $format format...")

// Ensure the directory exists
new java.io.File(outputPath).mkdirs()

salesData.write
  .mode("overwrite")
  .format(format)
  .save(outputPath)
  
println(s"Sales data saved to $outputPath in $format format")

// Also save as CSV for easier viewing if needed
salesData.write
  .mode("overwrite")
  .option("header", "true")
  .csv(s"$outputPath-csv")

println(s"Sales data also saved as CSV to $outputPath-csv")

/*
// Calculate some statistics
println("Data statistics:")

println("Sales by region:")
salesData.groupBy("region")
  .agg(
    count("order_id").as("num_orders"),
    sum("total").as("total_sales")
  )
  .orderBy(desc("total_sales"))
  .show()
  
println("Sales by product category:")
salesData.groupBy("category")
  .agg(
    count("order_id").as("num_orders"),
    sum("total").as("total_sales")
  )
  .orderBy(desc("total_sales"))
  .show()
  
println("Monthly sales trend:")
salesData.withColumn("year_month", date_format($"date", "yyyy-MM"))
  .groupBy("year_month")
  .agg(sum("total").as("monthly_sales"))
  .orderBy("year_month")
  .show(24) // Show up to 24 months

// Make the data available to all users by creating a global table
println("Creating global table for all users to access...")
spark.sql("CREATE DATABASE IF NOT EXISTS shared_data")
salesData.write
  .mode("overwrite")
  .saveAsTable("shared_data.sales_records")
  
println("Data is now available to all users as 'shared_data.sales_records'")

// Now the salesData is available for interactive exploration in this spark-shell session
println("\nThe data is now available as 'salesData' DataFrame and 'sales' SQL table in this session")
println("Try running: salesData.groupBy('product').count().show()")
println("Or SQL: spark.sql('SELECT product, AVG(total) as avg_sale FROM sales GROUP BY product ORDER BY avg_sale DESC').show()")
*/

System.exit(0)
