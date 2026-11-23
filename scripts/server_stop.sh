#!/bin/bash -e

source scripts/share.sh
: "${SPARK_HOME:?SPARK_HOME must be set}"

if check_spark_master
then
	"${SPARK_HOME}/sbin/stop-worker.sh"
	"${SPARK_HOME}/sbin/stop-master.sh"
fi
