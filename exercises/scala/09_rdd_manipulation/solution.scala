// RDD Manipulation Exercise
import org.apache.spark.rdd.RDD

// Helper function to measure execution time
def time[R](block: => R): (R, Long) = {
  val start = System.currentTimeMillis()
  val result = block
  val end = System.currentTimeMillis()
  (result, end - start)
}

// Helper function to print RDD type and first few elements
def printRDDInfo[T: scala.reflect.ClassTag](rdd: RDD[T], name: String): Unit = {
  println(s"\n=== $name ===")
  println(s"RDD Type: ${rdd.getClass.getName}")
  println(s"Element Type: ${scala.reflect.classTag[T]}")
  println("First few elements (not executed yet):")
}

println("\n\n*** Starting RDD Manipulation Exercise ***\n")

// Create a large RDD with a billion elements (actually, we'll use 100 million to be practical)
println("Creating initial large RDD...")
val (initialRDD, creationTime) = time {
  sc.parallelize(1 to 100000000, 100)
}

printRDDInfo(initialRDD, "Initial RDD")
println(s"Creation time: $creationTime ms (This time is for parallelizing, not for generating all elements)")

// Filter operation 1: keep only even numbers
println("\nApplying filter for even numbers...")
val startFilter1 = System.currentTimeMillis()
val evenRDD = initialRDD.filter(_ % 2 == 0)
val endFilter1 = System.currentTimeMillis()

printRDDInfo(evenRDD, "Even Numbers RDD")
println(s"Filter operation time: ${endFilter1 - startFilter1} ms")

// Filter operation 2: keep numbers divisible by 7
println("\nApplying filter for numbers divisible by 7...")
val startFilter2 = System.currentTimeMillis()
val divisibleBy7RDD = initialRDD.filter(_ % 7 == 0)
val endFilter2 = System.currentTimeMillis()

printRDDInfo(divisibleBy7RDD, "Divisible by 7 RDD")
println(s"Filter operation time: ${endFilter2 - startFilter2} ms")

// Combined filter: both even and divisible by 3
println("\nApplying combined filter (even and divisible by 3)...")
val startCombinedFilter = System.currentTimeMillis()
val combinedFilterRDD = initialRDD.filter(n => n % 2 == 0 && n % 3 == 0)
val endCombinedFilter = System.currentTimeMillis()

printRDDInfo(combinedFilterRDD, "Combined Filter RDD")
println(s"Combined filter operation time: ${endCombinedFilter - startCombinedFilter} ms")

// Sample the RDD
println("\nSampling 0.1% of the RDD...")
val startSample = System.currentTimeMillis()
val sampledRDD = initialRDD.sample(withReplacement = false, fraction = 0.001)
val endSample = System.currentTimeMillis()

printRDDInfo(sampledRDD, "Sampled RDD")
println(s"Sampling operation time: ${endSample - startSample} ms")

// Map the RDD to create squares
println("\nCreating squares of each element...")
val startMap = System.currentTimeMillis()
val squaresRDD = initialRDD.map(x => x * x)
val endMap = System.currentTimeMillis()

printRDDInfo(squaresRDD, "Squares RDD")
println(s"Map operation time: ${endMap - startMap} ms")

// Now demonstrating that actions take time
println("\n\n*** Now executing actions which will trigger computation ***\n")

// Count the initial RDD
println("Counting elements in the initial RDD...")
val (initialCount, initialCountTime) = time {
  initialRDD.count()
}
println(s"Initial RDD count: $initialCount")
println(s"Count operation time: $initialCountTime ms")

// Count the even numbers
println("\nCounting elements in the even numbers RDD...")
val (evenCount, evenCountTime) = time {
  evenRDD.count()
}
println(s"Even RDD count: $evenCount")
println(s"Count operation time: $evenCountTime ms")

// Count the numbers divisible by 7
println("\nCounting elements in the divisible by 7 RDD...")
val (divisibleBy7Count, divisibleBy7CountTime) = time {
  divisibleBy7RDD.count()
}
println(s"Divisible by 7 RDD count: $divisibleBy7Count")
println(s"Count operation time: $divisibleBy7CountTime ms")

// Count the combined filter RDD
println("\nCounting elements in the combined filter RDD...")
val (combinedFilterCount, combinedFilterCountTime) = time {
  combinedFilterRDD.count()
}
println(s"Combined filter RDD count: $combinedFilterCount")
println(s"Count operation time: $combinedFilterCountTime ms")

// Count the sampled RDD
println("\nCounting elements in the sampled RDD...")
val (sampledCount, sampledCountTime) = time {
  sampledRDD.count()
}
println(s"Sampled RDD count: $sampledCount")
println(s"Count operation time: $sampledCountTime ms")

// Take first 10 elements from the squares RDD
println("\nTaking first 10 elements from the squares RDD...")
val (squaresSample, squaresSampleTime) = time {
  squaresRDD.take(10)
}
println("First 10 squares:")
squaresSample.foreach(println)
println(s"Take operation time: $squaresSampleTime ms")

println("\n\n*** Exercise Complete ***")
println("\nKey observations:")
println("1. RDD transformations (filter, map, sample) execute very quickly because they are lazy")
println("2. RDD actions (count, take) trigger the actual computation and take significant time")
println("3. Each transformation creates a new RDD with a specific type")
println("4. The computation is only performed when an action is called")

// Exit the spark-shell
System.exit(0)
