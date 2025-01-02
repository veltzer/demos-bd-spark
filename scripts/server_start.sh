#!/bin/bash -e
export SPARK_MASTER_HOST=localhost
export SPARK_WORKER_HOST=localhost
export SPARK_MASTER_WEBUI_HOST=localhost
export SPARK_WORKER_WEBUI_HOST=localhost
export SPARK_WORKER_UI_ADDRESS=localhost
export SPARK_LOCAL_IP=localhost
export SPARK_PUBLIC_DNS=localhost

function check_spark_master() {
    if pgrep -f "org.apache.spark.deploy.master.Master" > /dev/null; then
        echo "Spark master is running"
        return 0
    else
        echo "Spark master is not running"
        return 1
    fi
}

if ! check_spark_master
then
	"${SPARK_HOME}/sbin/start-master.sh"
	"${SPARK_HOME}/sbin/start-worker.sh" "spark://localhost:7077"
fi
