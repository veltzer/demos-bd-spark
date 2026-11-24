#!/usr/bin/env python

"""
Generate data
"""

from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd


def generate_sales_data(num_stores=5, days=30, seed=42):
    """Generate sample sales data for the window functions exercise."""

    rng = np.random.default_rng(seed)

    # Generate dates
    start_date = datetime(2024, 1, 1, tzinfo=UTC)
    dates = [start_date + timedelta(days=x) for x in range(days)]

    # Generate data for each store
    data = []
    for store_id in range(1, num_stores + 1):
        # Generate base amount for each store
        base_amount = rng.uniform(800, 1200)
        for date in dates:
            # Add some randomness to daily sales
            daily_variation = rng.normal(0, base_amount * 0.1)
            # Add weekly pattern (weekend boost)
            weekend_boost = base_amount * 0.2 if date.weekday() >= 5 else 0
            amount = max(0, base_amount + daily_variation + weekend_boost)
            data.append({
                'store_id': store_id,
                'date': date.strftime('%Y-%m-%d'),
                'amount': round(amount, 2)
            })
    # Convert to DataFrame and sort
    df = pd.DataFrame(data)
    return df.sort_values(['store_id', 'date'])

def main():
    # Generate sample data
    df = generate_sales_data()
    # Save to CSV
    df.to_csv('sales_data.csv', index=False)
    print("Sample data has been generated and saved to 'sales_data.csv'")
    print("First few rows of the generated data:")
    print(df.head())
    print("Data summary:")
    print(df.describe())

if __name__ == "__main__":
    main()
