#!/usr/bin/env python

import os
from contextlib import redirect_stdout, redirect_stderr
from pyspark import SparkContext, SparkConf
from wurlitzer import pipes

with open(os.devnull, "w") as devnull:
    with pipes(stdout=devnull, stderr=devnull), \
         redirect_stdout(devnull), \
         redirect_stderr(devnull):
        conf = SparkConf().setAppName("VersionCheck")
        sc = SparkContext(conf=conf)
# with open(os.devnull, "w") as devnull:
#    with redirect_stdout(devnull), redirect_stderr(devnull):

# Print Spark version
print(f"Spark Version: {sc.version}")

# Optional: Print more details
print("Spark Configuration:")
print(f"Master: {sc.master}")
print(f"App Name: {sc.appName}")

# Good practice to stop SparkContext when done
sc.stop()
