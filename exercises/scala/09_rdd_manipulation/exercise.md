# RDD manipulation

Create an RDD and prints its scala data type.

Start with a large RDD (lets say a billion elements).

Manipulate that RDD in several ways:
* filter it in various ways.
* sample from it.

Show that these operations do not take any time (rememember that RDDs are lazy...).

What is the type of each RDD that you create?

Now count the number of rows in the RDD and show that this operation does take time.

Notes:
* You run your code line this `spark-shell -i [your_file.scala]`
* End you script with `System.exit(0)`
