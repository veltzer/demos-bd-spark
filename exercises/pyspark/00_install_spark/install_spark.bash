#!/bin/bash -e

function get_latest_spark_version() {
	# Get the download page and extract the latest version
	local page latest_version
	page=$(curl -s "https://spark.apache.org/downloads.html")
	latest_version=$(grep -m1 -oP 'Spark \K[0-9]\.[0-9]\.[0-9]' <<<"${page}")
	echo "${latest_version}"
}

function cleanup_spark_dirs() {
	echo "Checking for existing Spark installations..."
	if ls "${HOME}"/install/spark-* 1> /dev/null 2>&1
	then
		echo "Found existing Spark directories. Removing them..."
		rm -rf "${HOME}"/install/spark-*
		echo "Cleanup completed."
	else
		echo "No existing Spark directories found."
	fi
}

version=$(set -e; get_latest_spark_version)
echo "latest version is [${version}]..."
toplevel="spark-${version}-bin-hadoop3"
if [[ ! -d "${HOME}/install" ]]
then
	echo "HOME/install did not exist, creating it..."
	mkdir "${HOME}/install"
fi
if [[ -L "${HOME}/install/spark" ]]
then
	echo "Found previous spark symlink and cleaning it up..."
	rm "${HOME}/install/spark"
fi
cleanup_spark_dirs

url="https://dlcdn.apache.org/spark/spark-${version}/spark-${version}-bin-hadoop3.tgz"
{ curl --location --silent "${url}" || true; } | tar xz -C "${HOME}/install"
ln -s "${HOME}/install/${toplevel}" "${HOME}/install/spark"
