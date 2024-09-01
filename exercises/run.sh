#!/bin/bash -e
docker run -it --rm\
	-v ../../data:/opt/data\
	-v .:/opt/code\
	apache/spark\
	spark-submit\
	--class TransformCSV\
	--master local[*]\
	/opt/code/transform_csv.scala
