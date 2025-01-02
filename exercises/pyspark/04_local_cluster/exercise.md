# Bringing up local cluster

## First phase

Run a pyspark script that takes lots of time (say, do a sleep(...) in the script).

Notice that spark is running when your script is running:

```bash
ps -ef | grep java
```

## Second phase

Run a permanent cluster
