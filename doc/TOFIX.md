# TOFIX

Findings from a code scan on 2026-10-04.

## Medium

- `exercises/scala/03_manipulate_csv_interactively/run.sh:5` - feeds `/exercise/session.scala` to `spark-shell`, but there is no `session.scala` in the folder (only `hello.scala` and `transform_csv.scala`), so the script cannot work; point it at the real file and delete the commented-out attempts at lines 6-10.
- `exercises/pyspark/01_install_pyspark/solution.py:17` - computes `x * x` under the comment "Double each number", while `exercise.md` shows `x * 2`; make the solution match the exercise.
- `exercises/pyspark/01_install_pyspark/exercise.md:8` - `pip install pyspark` into the system python fails on current Debian/Ubuntu (PEP 668 "externally-managed-environment"); instruct a venv (or `uv sync` with this repo's `pyproject.toml`).
- `exercises/pyspark/00_install_spark/install_spark.bash:37` - `cleanup_spark_dirs` runs `rm -rf ~/install/spark-*` before the download at line 40 is attempted; if the version scrape or download fails, the working install is already gone. Download and verify first, then swap. Also the version regex at line 6 (`[0-9]\.[0-9]\.[0-9]`) truncates two-digit components (e.g. `3.5.10`).
- `exercises/scala/10_sbt_and_spark_submit/build.sbt:4` - builds against Spark 3.3.0 / Scala 2.12, but `install_spark.bash` installs the latest Spark (4.x, Scala 2.13 only), so the jar from `package_and_submit.sh` will not run on the cluster the course sets up; align the versions (and the `scala-2.12` jar path in `package_and_submit.sh:8`).
- `scripts/server_start.sh:14` - sets `SPARK_LOCAL_IP=0.0.0.0` ("allow connections from anywhere"), exposing an unauthenticated standalone master/worker on every interface, which lets anyone on the network submit code; bind to `127.0.0.1` (the commented-out line 12).
- `exercises/pyspark/19_reports/reports/top_products.parquet/_SUCCESS:1` - the whole `reports/` tree (48 files of Spark CSV/parquet output with `.crc` files from a 2025-01-29 run) is committed generated output; `git rm -r --cached` it and add `reports/` to `exercises/pyspark/19_reports/.gitignore`.
- `pyproject.toml:118` - mypy override ignores missing imports for `pandas.*` although `pandas-stubs` is in the dev group, plus `matplotlib.*`/`plotly.*`; a config-level suppression - drop `pandas.*` and keep only modules that really lack stubs, with the reason.

## Low

- `exercises/pyspark/05_local_vs_standalone/test2.y:1` - a near-duplicate of `test.py` with a `.y` extension, so ruff/mypy never see it; delete it or rename to `.py`.
- `exercises/pyspark/05_local_vs_standalone/logs.sh:2` - hard-codes `/home/mark/install/spark/logs/...-newton.out`; use `${SPARK_HOME}/logs/` and a glob.
- `exercises/scala/10_sbt_and_spark_submit/install_sbt.bash:1` - no shebang and no `set -e`, unlike every other script; add `#!/bin/bash -e`.
- `exercises/pyspark/06_processing_text_files/create.js:1` - a Node script just to print random numbers forces the `processor.oxlint` block at `rsconstruct.toml:79`; rewrite it in Python and drop the JS processor.
- `scripts/server_status.sh:5` - under `bash -e` the first failing check exits, so `check_spark_ui`/`check_spark_http` never run when the master is down; drop `-e` and collect each check's status explicitly.
- `tera.snippets/main.md.tera:1` - snippet has no leading blank line or heading, so `README.md:20-21` glues "spark download" onto the build badge; start it with an empty line and a `##` heading.
- `exercises/scala/16_repartition_data/execise.md:1` - filename typo ("execise"); likewise the `opt_sql_prunning` folder name. `exercises/pyspark/opt_statistics_old/` is a superseded copy of `opt_statistics/`; delete it.
