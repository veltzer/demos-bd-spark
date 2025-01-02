#!/bin/bash -e

source scripts/share.sh

export SPARK_MASTER_HOST=localhost
export SPARK_WORKER_HOST=localhost
export SPARK_MASTER_WEBUI_HOST=localhost
export SPARK_WORKER_WEBUI_HOST=localhost
export SPARK_WORKER_UI_ADDRESS=localhost
export SPARK_LOCAL_IP=localhost
export SPARK_PUBLIC_DNS=localhost

if ! check_spark_master
then
	"${SPARK_HOME}/sbin/start-master.sh"
	"${SPARK_HOME}/sbin/start-worker.sh" "spark://localhost:7077"
fi
