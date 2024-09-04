package com.example

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Sample Spark Job")
      .getOrCreate()

    val data = spark.read.textFile("path/to/your/input/file")
    val wordCounts = data.flatMap(line => line.split(" "))
                         .groupByKey(identity)
                         .count()

    wordCounts.show()
    
    spark.stop()
  }
}
