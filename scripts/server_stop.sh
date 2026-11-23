#!/bin/bash -e

source scripts/share.sh
: "${SPARK_HOME:?SPARK_HOME must be set}"

set +e
check_spark_master
master_running=$?
set -e
if ((master_running == 0))
then
	"${SPARK_HOME}/sbin/stop-worker.sh"
	"${SPARK_HOME}/sbin/stop-master.sh"
fi
