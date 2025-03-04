import org.apache.spark.sql.SparkSession

object SparkApp {
  def main(args: Array[String]): Unit = {
    // Get master URL from environment variable or use a default
    val masterUrl = sys.env.getOrElse("SPARK_MASTER", "local[*]")
    
    // Create SparkSession with the master URL from environment variable
    val spark = SparkSession.builder()
      .appName("SparkScalaApp")
      .master(masterUrl)
      .getOrCreate()
    
    println(s"Connected to Spark master: ${spark.sparkContext.master}")
    
    // Your Spark application code goes here
    val data = Seq(1, 2, 3, 4, 5)
    val rdd = spark.sparkContext.parallelize(data)
    val sum = rdd.sum()
    println("**********************************************")
    println(s"Sum: $sum")
    println("**********************************************")
    
    // Don't forget to stop the session when done
    spark.stop()
  }
}
