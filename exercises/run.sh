#!/bin/bash -e
docker run -it --rm\
	-v "${PWD}:/opt/exercise"\
	apache/spark\
	spark-submit\
	--class TransformCSV\
	--master local[*]\
	"/opt/exercise/transform_csv.scala"
