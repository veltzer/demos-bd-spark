#!/bin/bash -e
docker run -it --rm\
	-v "${PWD}:/exercise"\
	"apache/spark"\
	"bash" -c "/opt/spark/bin/spark-shell < /exercise/session.scala"
	# "bash" -c "/opt/spark/bin/spark-shell --conf spark.driver.extraJavaOptions='-Dlog4j.configuration=file:/exercise/log4j.properties' < /exercise/session.scala"
#	"/opt/spark/bin/spark-shell" -I "/opt/exercise/session.scala"
#	"ls" -l "/opt"
#	"/opt/spark/bin/spark-shell"
#	"/opt/spark/bin/spark-submit < /opt/exercise/transform_csv.scala"
#	"/opt/spark/bin/spark-submit" "/opt/exercise/transform_csv.scala" --class "TransformCSV"
