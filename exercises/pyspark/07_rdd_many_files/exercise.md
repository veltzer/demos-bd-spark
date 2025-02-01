# Many Files

## Exercise Overview
Learn how to process multiple text files using PySpark's RDD API. This exercise focuses on pure RDD operations without using DataFrames or SQL, demonstrating basic to advanced text processing techniques.

## Prerequisites
- Apache Spark installed
- A running local Spark standalone cluster
- Basic Python knowledge
- Understanding of basic RDD concepts

## Setup Instructions

### 1. Start Your Spark Cluster

```bash
# Start master
$SPARK_HOME/sbin/start-master.sh

# Start worker (use your master URL)
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077
```

### 2. Create Directory Structure

```bash
mkdir spark_files_example
cd spark_files_example
```

### 3. Create Sample Files

Create the following text files in your directory:

**book1.txt**:

```text
Title: Pride and Prejudice (Excerpt)
Author: Jane Austen

It is a truth universally acknowledged, that a single man in possession of a good fortune, must be in want of a wife.
However little known the feelings or views of such a man may be on his first entering a neighbourhood, this truth is so well fixed in the minds of the surrounding families, that he is considered the rightful property of some one or other of their daughters.
"My dear Mr. Bennet," said his lady to him one day, "have you heard that Netherfield Park is let at last?"
```

[Additional file contents provided in the sample files artifact...]

## Exercise Tasks

### Basic Tasks

1. File Reading and Counting
    - Read all text files in the directory
    - Count the number of files
    - Print file names and sizes
1. Line Analysis
    - Count lines per file
    - Calculate average line length
    - Find longest and shortest lines
1. Word Frequency Analysis
    - Count word occurrences
    - Find most common words
    - Exclude common stop words

### Advanced Tasks

1. Content Search
    - Find sentences containing specific keywords
    - Count keyword occurrences per file
    - Extract sentences matching patterns
1. File Categorization
    - Group files by type (books, articles, notes)
    - Analyze content patterns
    - Compare word usage across categories

## Solution Structure

Create a file named `process_files.py`:

```python
from pyspark import SparkContext, SparkConf
import os

def create_spark():
    """Create and configure SparkContext"""
    conf = SparkConf().setAppName("Text Processing").setMaster("spark://localhost:7077")
    return SparkContext(conf=conf)

def analyze_files(sc, input_dir):
    # Your analysis code here
    pass

def main():
    sc = create_spark()
    try:
        analyze_files(sc, os.getcwd())
        input("Check Spark UI, then press Enter to finish...")
    finally:
        sc.stop()

if __name__ == "__main__":
    main()
```

## Key RDD Operations to Use

1. Basic Operations:

   ```python
   # Reading files
   files_rdd = sc.wholeTextFiles("*.txt")

   # Basic transformations
   lines_rdd = files_rdd.flatMap(lambda x: x[1].splitlines())

   # Word counting
   words_rdd = lines_rdd.flatMap(lambda line: line.split())
   ```

1. Key-Value Operations:

```python
# Count by key
counts = words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Group by key
grouped = words_rdd.groupBy(lambda word: word[0])
```

1. Advanced Operations:

```python
# Custom accumulator for statistics
# Complex transformations
# Chain of operations
```

## Expected Output

Your program should produce output similar to:

```text
=== Found 6 files ===

=== File Sizes ===
book1.txt: 573 characters
book2.txt: 489 characters
...

=== Most Common Words ===
man: 5 occurrences
would: 4 occurrences
...
```

## Common Pitfalls to Avoid

1. Memory Management:
    - Don't collect large RDDs unnecessarily
    - Use `take()` or `sample()` for testing
    - Be careful with `collect()` on large datasets
1. Performance Considerations:
    - Avoid unnecessary shuffles
    - Reuse RDDs when possible
    - Consider partition size
1. Error Handling:
    - Handle missing files gracefully
    - Check for empty files
    - Validate input data

## Extensions

Once you complete the basic exercise, try these extensions:

1. Add More Analysis:
    - Sentiment analysis
    - Pattern matching
    - Text classification
1. Performance Optimization:
    - Optimize partition size
    - Cache frequently used RDDs
    - Measure and improve execution time
1. Additional Features:
    - Save results to files
    - Generate statistics
    - Create reports

## Testing Your Solution

1. Basic Testing:

```bash
python process_files.py
```

1. Verify Results:
    - Check output format
    - Validate calculations
    - Compare with expected results
1. Monitor Performance:
    - Check Spark UI [here](http://localhost:8080)
    - Monitor resource usage
    - Measure execution time
