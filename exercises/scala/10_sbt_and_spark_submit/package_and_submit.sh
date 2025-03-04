#!/bin/bash -e

# Package your application
sbt package

# Submit the job
spark-submit \
	--master "${SPARK_MASTER}" \
	--class SparkApp \
	"target/scala-2.12/sparkscalaapp_2.12-1.0.jar"
