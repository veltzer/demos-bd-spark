#!/bin/bash -e
# You should see something like:
# 12345 Master  # The Spark master process
# 67890 Worker  # One or more Spark worker processes
jps
# You should see processes like:
# org.apache.spark.deploy.master.Master
# org.apache.spark.deploy.worker.Worker
pgrep -a java
