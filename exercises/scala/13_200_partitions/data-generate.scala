// Run it with: spark-shell -i data-generate.scala

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Date
import java.util.Calendar
import scala.util.Random

// Parameters - modify as needed
val numRecords = 10000000
val outputPath = "/tmp/shared/large_sales_data"  // Local path instead of HDFS
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
salesData.createOrReplaceTempView("large_sales")

// Save the data
println(s"Saving data to $outputPath in $format format...")

// Ensure the directory exists
new java.io.File(outputPath).mkdirs()

salesData.write
  .mode("overwrite")
  .format(format)
  .save(outputPath)
  
println(s"Sales data saved to $outputPath in $format format")

System.exit(0)
