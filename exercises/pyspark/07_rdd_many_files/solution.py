#!/usr/bin/env python

import os
from pyspark import SparkContext, SparkConf

def create_spark():
    """Create and configure SparkContext"""
    conf = SparkConf().setAppName("Text Processing").setMaster("spark://localhost:7077")
    return SparkContext(conf=conf)

def analyze_files(sc, input_dir):
    """Analyze text files in the input directory"""
    # Read all text files
    files_rdd = sc.wholeTextFiles(os.path.join(input_dir, "*.txt"))

    print(f"=== Found {files_rdd.count()} files ===")

    # Show files and sizes
    print("=== File Sizes ===")
    file_sizes = files_rdd.mapValues(len).collect()
    for file_path, size in file_sizes:
        filename = os.path.basename(file_path)
        print(f"{filename}: {size} characters")

    # Count lines per file
    print("=== Lines per File ===")
    lines_per_file = files_rdd.mapValues(lambda content: len(content.splitlines())).collect()
    for file_path, count in lines_per_file:
        filename = os.path.basename(file_path)
        print(f"{filename}: {count} lines")

    # Extract titles (assuming first line contains "Title:")
    print("=== Document Titles ===")
    titles = files_rdd.map(lambda x: (
        os.path.basename(x[0]),
        next(line for line in x[1].splitlines() if "Title:" in line).replace("Title:", "").strip()
    )).collect()
    for filename, title in titles:
        print(f"{filename}: {title}")

    # Word frequency analysis
    all_words = files_rdd.flatMap(lambda x: x[1].lower().split())

    # Remove common punctuation
    cleaned_words = all_words.map(lambda word: word.strip('.,!?()[]{}":;'))

    # Filter out empty strings and common words
    common_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
    valid_words = cleaned_words.filter(lambda word: word and word not in common_words)

    # Count word frequencies
    word_counts = valid_words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    print("=== Most Common Words ===")
    for word, count in word_counts.top(10, key=lambda x: x[1]):
        print(f"{word}: {count} occurrences")

    # Additional analysis: sentences containing "AI" or "artificial intelligence"
    print("=== AI-related Sentences ===")
    ai_sentences = files_rdd.flatMap(
        lambda x: [s.strip() for s in x[1].split('.') if 'AI' in s or 'artificial intelligence' in s.lower()]
    ).collect()
    for sentence in ai_sentences:
        print(f"- {sentence}")

def main():
    """Main function to run the analysis"""
    sc = create_spark()

    try:
        # Get the current directory
        current_dir = os.getcwd()
        print(f"Processing files in: {current_dir}")

        # Run analysis
        analyze_files(sc, current_dir)

        # Keep the application running to check Spark UI
        input("Check Spark UI at http://localhost:8080, then press Enter to finish...")

    finally:
        sc.stop()

if __name__ == "__main__":
    main()
