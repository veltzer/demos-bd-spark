# Permanent cluster with data load

* Launch a spark cluster which lives indefinitely.
    looks at `$GIT_ROOT/scripts/server_start.sh`.
    You should launch one master and one worker.

* Point your `SPARK_MASTER` environment variable to the master that you just launched.

```bash
export SPARK_MASTER="spark://localhost:7077"
```

* Load your running spark cluster with some data.
    I will put an example of how to do that in Scala in this folder.
    This will be scala code that you run using `spark-submit`

* Do another `spark-submit` script that shows some of the data.

This means you need to write two scripts in this exercise: `load_data.scala` and `show_data.scala`
