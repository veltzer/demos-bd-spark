#!/bin/bash -e

source scripts/share.sh

if check_spark_master
then
	"${SPARK_HOME}/sbin/stop-worker.sh"
	"${SPARK_HOME}/sbin/stop-master.sh"
fi
