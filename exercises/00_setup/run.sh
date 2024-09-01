#!/bin/bash -e
docker run -it --rm\
	-v "${PWD}:/exercise"\
	"apache/spark"\
	"bash" -c "/opt/spark/bin/spark-shell < /exercise/hello.scala"
