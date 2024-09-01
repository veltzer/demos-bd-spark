#!/bin/bash -e
docker run -it --rm\
	-v "${PWD}:/opt/exercise"\
	apache/spark\
	/opt/spark/bin/spark-submit\
	"/opt/exercise/transform_csv.scala" -i "transform_csv.scala" --class "TransformCSV"
