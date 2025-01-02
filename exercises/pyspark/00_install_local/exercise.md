# Install a local spark

* These instructions are for Linux, if you have another operating system, please switch.

* Use the provided `install_spark.bash` script to download and install spark on your system.

* Add `~/install/spark/bin` to your `PATH` variable in your `~/.bashrc`.
    Add `PATH=${PATH}:${HOME}/install/spark/bin` to your ~/.bashrc.

* In your `~/.bashrc` create an environment variable called `SPARK_HOME` that points to `~/install/spark`.

then do:

```bash
source ~/.bashrc
```

* Install java on your machine using the following commands:

```bash
sudo apt update
sudo apt install default-jre
```

* Check that your setup works by running `spark-shell`.
