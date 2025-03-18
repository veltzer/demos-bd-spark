// Run with: spark-shell -i data-view.scala

// val rdd = spark.read.option("header", "true").csv("/tmp/shared/sales_data-csv").rdd
val rdd = spark.read.parquet("/tmp/shared/sales_data").rdd
val numPartitions = rdd.getNumPartitions
println(s"Number of partitions: $numPartitions")
val partitionSizes = rdd.glom().map(_.length).collect()
partitionSizes.zipWithIndex.foreach { case (size, index) =>
  println(s"Partition $index: $size elements")
}

val rdd2 = rdd.repartition(numPartitions = 500)
val numPartitions2 = rdd2.getNumPartitions
println(s"Number of partitions: $numPartitions2")
val partitionSizes2 = rdd2.glom().map(_.length).collect()
partitionSizes2.zipWithIndex.foreach { case (size, index) =>
  println(s"Partition $index: $size elements")
}

System.exit(0)
