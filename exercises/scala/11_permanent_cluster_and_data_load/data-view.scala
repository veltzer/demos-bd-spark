// Run with: spark-shell -i data-view.scala

// Option 1: Load from the local file path
val salesFromFiles = spark.read.parquet("/tmp/shared/sales_data")
println("Sample data from parquet files:")
salesFromFiles.select("order_id", "date", "product", "region", "total", "payment_method")
  .orderBy(rand()).limit(5).show()

// Option 2: Access from CSV for easier viewing
val salesFromCSV = spark.read.option("header", "true").csv("/tmp/shared/sales_data-csv")
println("Sample data from CSV:")
salesFromCSV.select("order_id", "date", "product", "region", "total", "payment_method")
  .limit(5).show()

// Option 3: Access the shared table
println("Sample data from shared table:")
spark.sql("SELECT order_id, date, product, region, total, payment_method FROM shared_data.sales_records ORDER BY total DESC LIMIT 5").show()

// Calculate a quick summary statistic
println("Average sales by region:")
spark.sql("SELECT region, ROUND(AVG(total), 2) as avg_sale FROM shared_data.sales_records GROUP BY region ORDER BY avg_sale DESC").show()

// Exit the Spark shell when script is complete
println("\nExiting Spark shell...")
System.exit(0)
