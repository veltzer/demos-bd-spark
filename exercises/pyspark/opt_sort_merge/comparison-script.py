#!/usr/bin/env python
"""
Sort-Merge Performance Exercise - Comparison Script

This script compares the performance results from the naive and optimized
solutions and generates visualization charts.
"""

import json
import os
import sys
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np

# Constants
RESULTS_DIR = "results"
NAIVE_RESULTS = f"{RESULTS_DIR}/naive_performance.json"
OPTIMIZED_RESULTS = f"{RESULTS_DIR}/optimized_performance.json"
OUTPUT_DIR = f"{RESULTS_DIR}/visualizations"

def check_results_exist():
    """Check if both result files exist"""
    if not os.path.exists(NAIVE_RESULTS) or not os.path.exists(OPTIMIZED_RESULTS):
        print("Error: Results files not found.")
        print(f"Make sure both {NAIVE_RESULTS} and {OPTIMIZED_RESULTS} exist.")
        print("Run both naive_solution.py and optimized_solution.py first.")
        return False
    return True

def ensure_output_dir():
    """Ensure output directory for visualizations exists"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def load_results():
    """Load performance results from both solutions"""
    with open(NAIVE_RESULTS, 'r') as f:
        naive = json.load(f)

    with open(OPTIMIZED_RESULTS, 'r') as f:
        optimized = json.load(f)

    return naive, optimized

def create_time_comparison_chart(naive, optimized):
    """Create a bar chart comparing execution times"""
    plt.figure(figsize=(12, 6))

    # Data preparation
    categories = ['Total Time', 'Load Time', 'Join Time']
    naive_times = [naive['total_time_seconds'], naive['load_time_seconds'], naive['join_time_seconds']]
    optimized_times = [optimized['total_time_seconds'], optimized['load_time_seconds'], optimized['join_time_seconds']]

    # Calculate improvements
    improvements = [(naive_times[i] - optimized_times[i]) / naive_times[i] * 100
                   if naive_times[i] > 0 else 0
                   for i in range(len(naive_times))]

    # Create bar positions
    x = np.arange(len(categories))
    width = 0.35

    # Create bars
    ax = plt.subplot(111)
    bars1 = ax.bar(x - width/2, naive_times, width, label='Unsorted Data (Naive)', color='#ff9999')
    bars2 = ax.bar(x + width/2, optimized_times, width, label='Pre-sorted Data (Optimized)', color='#66b3ff')

    # Add labels and title
    ax.set_ylabel('Time (seconds)')
    ax.set_title('Performance Comparison: Naive vs. Optimized Sort-Merge Join')
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.legend()

    # Add improvement percentages
    for i, _ in enumerate(x):
        if improvements[i] > 0:
            plt.text(x[i], max(naive_times[i], optimized_times[i]) * 1.05,
                     f"{improvements[i]:.1f}% faster",
                     ha='center', va='bottom',
                     fontweight='bold')

    # Add exact time values on bars
    def add_labels(bars):
        for c_bar in bars:
            height = c_bar.get_height()
            ax.annotate(f'{height:.2f}s',
                       xy=(c_bar.get_x() + c_bar.get_width() / 2, height),
                       xytext=(0, 3),  # 3 points vertical offset
                       textcoords="offset points",
                       ha='center', va='bottom')

    add_labels(bars1)
    add_labels(bars2)

    # Add grid
    ax.grid(axis='y', alpha=0.3)

    # Save chart
    plt.tight_layout()
    ensure_output_dir()
    plt.savefig(f"{OUTPUT_DIR}/time_comparison.png")
    print(f"Time comparison chart saved to: {OUTPUT_DIR}/time_comparison.png")

def create_speedup_chart(naive, optimized):
    """Create a chart showing speedup factors"""
    # Calculate speedup metrics
    join_speedup = naive['join_time_seconds'] / optimized['join_time_seconds'] if optimized['join_time_seconds'] > 0 else 0
    total_speedup = naive['total_time_seconds'] / optimized['total_time_seconds'] if optimized['total_time_seconds'] > 0 else 0

    # Create figure
    plt.figure(figsize=(8, 6))

    # Create bar chart
    metrics = ['Join Operation', 'Total Execution']
    speedups = [join_speedup, total_speedup]

    bars = plt.bar(metrics, speedups, color=['#66b3ff', '#5cb85c'])

    # Add labels
    plt.ylabel('Speedup Factor (×)', fontsize=12)
    plt.title('Performance Speedup: Pre-sorted vs. Unsorted Data', fontsize=14)
    plt.axhline(y=1, color='r', linestyle='--', alpha=0.7)

    # Add value annotations
    for c_bar in bars:
        height = c_bar.get_height()
        plt.text(c_bar.get_x() + c_bar.get_width()/2., height + 0.1,
                f'{height:.2f}×',
                ha='center', va='bottom', fontweight='bold')

    # Add explanatory text
    if join_speedup > 1:
        plt.figtext(0.5, 0.01,
                  f"Pre-sorted data provides a {join_speedup:.1f}× speedup for join operations",
                  ha='center', fontsize=10, style='italic')

    # Save chart
    plt.tight_layout(rect=[0, 0.03, 1, 0.98])
    plt.savefig(f"{OUTPUT_DIR}/speedup_chart.png")
    print(f"Speedup chart saved to: {OUTPUT_DIR}/speedup_chart.png")

def create_summary_report(naive, optimized):
    """Create a text summary report of the performance comparison"""
    # Calculate key metrics
    join_speedup = naive['join_time_seconds'] / optimized['join_time_seconds'] if optimized['join_time_seconds'] > 0 else 0
    total_speedup = naive['total_time_seconds'] / optimized['total_time_seconds'] if optimized['total_time_seconds'] > 0 else 0
    join_time_saved = naive['join_time_seconds'] - optimized['join_time_seconds']
    total_time_saved = naive['total_time_seconds'] - optimized['total_time_seconds']

    # Format report
    report = [
        "=" * 80,
        "SORT-MERGE PERFORMANCE COMPARISON SUMMARY",
        "=" * 80,
        "",
        f"Report generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "DATASET INFORMATION:",
        f"  Transactions: {naive['transactions_count']:,} records",
        f"  Products: {naive['products_count']:,} records",
        f"  Join result: {naive['join_result_count']:,} records",
        "",
        "PERFORMANCE METRICS:",
        "  Naive approach (unsorted data):",
        f"    - Total execution time: {naive['total_time_seconds']:.2f} seconds",
        f"    - Join operation time: {naive['join_time_seconds']:.2f} seconds",
        "",
        "  Optimized approach (pre-sorted data):",
        f"    - Total execution time: {optimized['total_time_seconds']:.2f} seconds",
        f"    - Join operation time: {optimized['join_time_seconds']:.2f} seconds",
        "",
        "PERFORMANCE IMPROVEMENT:",
        f"  Join operation speedup: {join_speedup:.2f}×",
        f"  Total execution speedup: {total_speedup:.2f}×",
        f"  Join time saved: {join_time_saved:.2f} seconds",
        f"  Total time saved: {total_time_saved:.2f} seconds",
        "",
        "CONCLUSION:",
    ]

    # Add appropriate conclusion based on results
    if join_speedup > 1.5:
        report.extend([
            "  The results demonstrate significant performance improvements when using",
            "  pre-sorted data for sort-merge join operations. The expensive sorting step",
            "  is eliminated, resulting in faster execution and reduced resource utilization.",
            "",
            "  For production workloads, maintaining sorted datasets can provide substantial",
            "  performance benefits, especially for repeated join operations on large datasets."
        ])
    elif join_speedup > 1:
        report.extend([
            "  The results show moderate performance improvements when using pre-sorted data.",
            "  While the benefits are present, they may be less pronounced due to factors like",
            "  dataset size, cluster resources, or other optimizations already in place.",
            "",
            "  For larger datasets, the benefits would likely become more significant."
        ])
    else:
        report.extend([
            "  The results don't show significant performance improvements with pre-sorted data.",
            "  This could be due to several factors including small dataset size, efficient",
            "  sorting algorithms, abundant cluster resources, or other Spark optimizations.",
            "",
            "  Consider testing with larger datasets to better observe potential benefits."
        ])

    # Save report
    ensure_output_dir()
    report_path = f"{OUTPUT_DIR}/performance_summary.txt"
    with open(report_path, 'w') as f:
        f.write('\n'.join(report))

    print(f"Summary report saved to: {report_path}")
    return '\n'.join(report)

def main():
    """Main execution function"""
    if not check_results_exist():
        sys.exit(1)

    print("Analyzing performance results...")
    naive, optimized = load_results()

    # Create visualizations
    create_time_comparison_chart(naive, optimized)
    create_speedup_chart(naive, optimized)

    # Create and print summary report
    summary = create_summary_report(naive, optimized)
    print("\n" + summary)

    print("\nAnalysis complete. All visualizations and reports have been saved.")

if __name__ == "__main__":
    main()
