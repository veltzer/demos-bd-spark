# Compound key partitioning

* well start with the same `create_data.scala` script as before.

* repartition the data by **both** product and region.

* Show how to do statistics on:
    * `Laptop` -> all latops sold all around the world
    * `North America` -> everything sold in North America
    * `Laptop` AND `North America` -> Laptops sold in North America
