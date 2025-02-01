# Bring up a standalone spark server

In this exercise you will bring up a standalone spark server and run a job on it.

Use the script:
    [link](https://github.com/veltzer/demos-spark/blob/master/scripts/server_start.sh)

To run your server.

Use ideas from this script:
    [link](https://github.com/veltzer/demos-spark/blob/master/scripts/server_status.sh)

To show that your server it alive.

Run a job on that server.
You do that by changing:

```python
.master("local[*]")
```

to

```python
.master("spark://localhost:7077")
```
