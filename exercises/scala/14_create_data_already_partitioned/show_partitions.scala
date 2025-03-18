// Run with: spark-shell -i show_partitions.scala

val rdd = spark.read.parquet("/tmp/shared/sales_data").rdd
val numPartitions = rdd.getNumPartitions
println(s"Number of partitions: $numPartitions")
val partitionSizes = rdd.glom().map(_.length).collect()
partitionSizes.zipWithIndex.foreach { case (size, index) =>
  println(s"Partition $index: $size elements")
}

System.exit(0)
