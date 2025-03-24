# Re-partition data

* You get a script called `create_data.scala`
* Run that script to create a dataset in Parquet format partitioned by date.
* Re-partition the data - this means create a copy of the data that is partitioned by `product`
* Show how long it takes to do statistics on one product on:
    * the original data
    * the new data
    hint: it should be a lot faster on the new data (sort of).
