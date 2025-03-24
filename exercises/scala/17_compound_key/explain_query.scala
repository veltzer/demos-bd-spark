import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val inputPath = "/tmp/shared/sales_data_compound"
val data = spark.read.format("parquet").load(inputPath)
data.createOrReplaceTempView("sales")
spark.sql("""
  SELECT 
    SUM(quantity) as total_units, 
    SUM(total) as total_revenue 
  FROM sales 
  WHERE product = 'Laptop' AND region = 'North America'
""").explain()
System.exit(0)
