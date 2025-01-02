# Processing multiple files using pyspark

Write a spark script to find the most common word used
in all Linux documentation.

Linux documentation is in the folder `/usr/share/doc`

How do you find the most common word?
- Split every line according to spaces.
- Output tuple of (word, count) for every word encountered.
- Merge of those tuples using addition.
- Sort the result
- Print the top-most result.
