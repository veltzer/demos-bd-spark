#!/bin/bash -e
export SPARK_LOCAL_IP=127.0.0.1
spark-shell -i solution.scala
