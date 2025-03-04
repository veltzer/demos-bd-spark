# Submitting scala code using `spark-submit`

* Install `sbt` on your training machine.
    use this [link](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html)
    and look for `ubuntu` installation instructions.
* Write a `hello_spark.scala` piece of spark code.
* Compile your code to a jar file using the `sbt` intalled before.
    This will be `sbt package`
* Bring up a spark cluster and record it URL.
* Define an envrionment variable called `SPARK_MASTER` to point to your cluster.
    If you don't want to bring up a cluster just set `SPARK_MASTER` to `local[1]`
    or `local[*]`.
* Use `spark-submit` to submit your jar file to the cluster.
