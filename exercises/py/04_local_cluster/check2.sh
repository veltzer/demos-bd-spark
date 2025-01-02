#!/bin/bash -e

# Check for Master process
ps aux | grep "[M]aster"
# or more specifically
ps aux | grep "[o]rg.apache.spark.deploy.master.Master"

# Check for Worker processes
ps aux | grep "[W]orker"
# or more specifically
ps aux | grep "[o]rg.apache.spark.deploy.worker.Worker"

# Check if ports are in use
# Master ports
netstat -nlp | grep 7077  # Master port
netstat -nlp | grep 8080  # Master Web UI

# Worker ports
netstat -nlp | grep 8081  # First worker Web UI
