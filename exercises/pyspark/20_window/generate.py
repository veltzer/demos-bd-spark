from datetime import datetime, timedelta
import pandas as pd
import numpy as np

def generate_sales_data(num_stores=5, days=30, seed=42):
    """Generate sample sales data for the window functions exercise."""

    np.random.seed(seed)

    # Generate dates
    start_date = datetime(2024, 1, 1)
    dates = [start_date + timedelta(days=x) for x in range(days)]

    # Generate data for each store
    data = []
    for store_id in range(1, num_stores + 1):
        # Generate base amount for each store
        base_amount = np.random.uniform(800, 1200)
        for date in dates:
            # Add some randomness to daily sales
            daily_variation = np.random.normal(0, base_amount * 0.1)
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
    df = df.sort_values(['store_id', 'date'])
    return df

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
