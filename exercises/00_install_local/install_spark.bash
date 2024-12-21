#!/bin/bash -e

function get_latest_spark_version() {
	# Get the download page and extract the latest version
	latest_version=$(curl -s "https://spark.apache.org/downloads.html" |
		grep -o 'Spark [0-9]\.[0-9]\.[0-9]' |
		head -n 1 |
		grep -o '[0-9]\.[0-9]\.[0-9]')
	echo "$latest_version"
}

function cleanup_spark_dirs() {
	echo "Checking for existing Spark installations..."
	if ls ${HOME}/install/spark-* 1> /dev/null 2>&1
	then
		echo "Found existing Spark directories. Removing them..."
		rm -rf ${HOME}/install/spark-*
		echo "Cleanup completed."
	else
		echo "No existing Spark directories found."
	fi
}

version=$(get_latest_spark_version)
echo "latest version is [${version}]..."
toplevel="spark-${version}-bin-hadoop3"
if [ ! -d "${HOME}/install" ]
then
	echo "HOME/install did not exist, creating it..."
	mkdir "${HOME}/install"
fi
if [ -L "${HOME}/install/spark" ]
then
	echo "Found previous spark symlink and cleaning it up..."
	rm "${HOME}/install/spark"
fi
cleanup_spark_dirs

url="https://dlcdn.apache.org/spark/spark-${version}/spark-${version}-bin-hadoop3.tgz"
curl --location --silent "${url}" | tar xz -C "${HOME}/install"
ln -s "${HOME}/install/${toplevel}" "${HOME}/install/spark"
