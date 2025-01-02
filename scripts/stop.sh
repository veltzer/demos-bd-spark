#!/bin/bash -e

function check_spark_master() {
    if pgrep -f "org.apache.spark.deploy.master.Master" > /dev/null; then
        echo "Spark master is running"
        return 0
    else
        echo "Spark master is not running"
        return 1
    fi
}

if check_spark_master
then
	"${SPARK_HOME}/sbin/stop-worker.sh"
	"${SPARK_HOME}/sbin/stop-master.sh"
fi
