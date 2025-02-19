#!/usr/bin/env python
"""
Sort-Merge Performance Exercise - Comparison Script

This script compares the performance results from the naive and optimized
solutions and generates visualization charts.
"""

import json
import os
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

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
    for i in range(len(x)):
        if improvements[i] > 0:
            plt.text(x[i], max(naive_times[i], optimized_times[i]) * 1.05, 
                     f"{improvements[i]:.1f}% faster", 
                     ha='center', va='bottom',
                     fontweight='bold')
    
    # Add exact time values on bars
    def add_labels(bars):
        for bar in bars: