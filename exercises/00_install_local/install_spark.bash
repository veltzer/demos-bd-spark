#!/bin/bash -ex

version="3.5.3"
toplevel="spark-${version}-bin-hadoop3"
if [ ! -d "${HOME}/install" ]
then
	mkdir "${HOME}/install"
fi
if [ -f "${HOME}/install/spark" ]
then
	rm "${HOME}/install/spark"
fi
if [ -d "${HOME}/install/${toplevel}" ]
then
	rm -rf "${HOME}/install/${toplevel}"
fi

# url="https://dlcdn.apache.org/spark/spark-${version}/spark-${version}-bin-hadoop3.tgz"
url="https://www.apache.org/dyn/closer.lua/spark/spark-${version}/spark-${version}-bin-hadoop3.tgz"
# echo "url is [${url}]..."
curl --location --silent "${url}" | tar xz -C "${HOME}/install"
ln -s "${HOME}/install/${toplevel}" "${HOME}/install/spark"
