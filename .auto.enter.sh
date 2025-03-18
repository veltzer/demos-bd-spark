# the next will connect you to an ALREADY running spark cluster on localhost:7077
export SPARK_MASTER="spark://localhost:7077"
# if you have a remote spark and you know it's IP then you use with
export SPARK_MASTER="spark://[your ip]:7077"
# this will launch a new cluster on all cores of the localhost
export SPARK_MASTER="local[*]"
# same as above but with only 1 core 
export SPARK_MASTER="local[1]"
