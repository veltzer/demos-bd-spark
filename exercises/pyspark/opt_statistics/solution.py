from pyspark.sql import SparkSession
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Statistics Exercise - Optimized Query") \
    .getOrCreate()

def compute_statistics():
    print("Computing table statistics...")
    
    # Compute table-level statistics
    spark.sql("ANALYZE TABLE products COMPUTE STATISTICS")
    spark.sql("ANALYZE TABLE sales COMPUTE STATISTICS")
    
    # Compute column-level statistics
    spark.sql("""
        ANALYZE TABLE products COMPUTE STATISTICS 
        FOR COLUMNS id, category, price
    """)
    spark.sql("""
        ANALYZE TABLE sales COMPUTE STATISTICS 
        FOR COLUMNS product_id, quantity
    """)
    
    print("Statistics computation complete!")

def run_optimized_sales_analysis():
    # Compute statistics first
    compute_statistics()
    
    print("\nRunning optimized query...")
    start_time = time.time()
    
    # Same query, but now with statistics available
    result = spark.sql("""
        SELECT 
            p.category,
            COUNT(DISTINCT s.id) as num_sales,
            SUM(s.quantity * p.price) as total_revenue,
            AVG(s.quantity * p.price) as avg_sale_value
        FROM sales s
        JOIN products p ON s.product_id = p.id
        GROUP BY p.category
        ORDER BY total_revenue DESC
    """)
    
    # Force execution and show results
    result.show()
    
    end_time = time.time()
    print(f"\nQuery execution time: {end_time - start_time:.2f} seconds")
    
    # Show the query plan
    print("\nOptimized Query Plan:")
    spark.sql("""
        EXPLAIN ANALYZED
        SELECT 
            p.category,
            COUNT(DISTINCT s.id) as num_sales,
            SUM(s.quantity * p.price) as total_revenue,
            AVG(s.quantity * p.price) as avg_sale_value
        FROM sales s
        JOIN products p ON s.product_id = p.id
        GROUP BY p.category
        ORDER BY total_revenue DESC
    """).show(truncate=False)

def show_table_statistics():
    print("\nProduct Table Statistics:")
    spark.sql("DESCRIBE EXTENDED products").show(truncate=False)
    
    print("\nSales Table Statistics:")
    spark.sql("DESCRIBE EXTENDED sales").show(truncate=False)

if __name__ == "__main__":
    # Clear any existing cache
    spark.catalog.clearCache()
    
    # Run the optimized analysis
    run_optimized_sales_analysis()
    
    # Show the statistics
    show_table_statistics()
    
    spark.stop()