#!/usr/bin/env python

"""
Solution
"""

import os
from contextlib import redirect_stdout, redirect_stderr
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from wurlitzer import pipes

HIDE=True
if HIDE:
    with open(os.devnull, "w") as devnull:
        with pipes(stdout=devnull, stderr=devnull), \
             redirect_stdout(devnull), \
             redirect_stderr(devnull):
            conf = SparkConf().setAppName("VersionCheck").setMaster(os.environ["SPARK_MASTER"])
            sc = SparkContext(conf=conf)
else:
    conf = SparkConf().setAppName("VersionCheck").setMaster(os.environ["SPARK_MASTER"])
    sc = SparkContext(conf=conf)

spark = SparkSession.builder.master(os.environ["SPARK_MASTER"]).appName("VersionCheck").getOrCreate()
print(type(spark))
# with open(os.devnull, "w") as devnull:
#    with redirect_stdout(devnull), redirect_stderr(devnull):

# Print Spark version
print(type(sc))
print(f"Spark Version: {sc.version}")

# Optional: Print more details
print("Spark Configuration:")
print(f"Master: {sc.master}")
print(f"App Name: {sc.appName}")

# Good practice to stop SparkContext when done
sc.stop()
