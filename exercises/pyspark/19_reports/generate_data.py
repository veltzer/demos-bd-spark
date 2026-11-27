#!/usr/bin/env python

"""
Generate data
"""

import os
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from pandas.core.frame import DataFrame

# Seeded generator for reproducibility
rng = np.random.default_rng(42)


def generate_products(num_products: int = 100) -> DataFrame:
    """Generate product data."""
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports',
                 'Toys', 'Food & Beverages', 'Beauty', 'Automotive', 'Health']

    products = {
        'product_id': range(1, num_products + 1),
        'product_name': [f'Product_{i}' for i in range(1, num_products + 1)],
        'category': rng.choice(categories, num_products),
        'supplier_id': rng.integers(1, 21, num_products)  # 20 suppliers
    }

    return pd.DataFrame(products)

def generate_customers(num_customers: int = 1000) -> DataFrame:
    """Generate customer data."""
    segments = ['Premium', 'Standard', 'Basic']
    countries = ['USA', 'UK', 'Canada', 'Australia', 'Germany', 'France', 'Japan', 'Spain']

    # Generate join dates over the last 3 years
    end_date = datetime.now(tz=UTC)
    start_date = end_date - timedelta(days=3*365)
    join_dates = [start_date + timedelta(days=int(rng.integers(0, 3*365)))
                 for _ in range(num_customers)]

    customers = {
        'customer_id': range(1, num_customers + 1),
        'customer_segment': rng.choice(segments, num_customers,
                                           p=[0.2, 0.5, 0.3]),  # weighted probabilities
        'join_date': join_dates,
        'country': rng.choice(countries, num_customers)
    }

    return pd.DataFrame(customers)

def generate_sales(products_df: DataFrame, customers_df: DataFrame, num_transactions: int = 50000) -> DataFrame:
    """Generate sales transaction data."""
    # Date range for the last year
    end_date = datetime.now(tz=UTC)
    start_date = end_date - timedelta(days=365)

    # Generate more transactions in recent months
    days = pd.date_range(start=start_date, end=end_date, freq='D')
    weights = np.linspace(0.5, 1.5, len(days))  # Increasing weights for more recent dates
    weights = weights / weights.sum()

    # Generate transaction data
    transactions = {
        'transaction_id': range(1, num_transactions + 1),
        'date': rng.choice(days, num_transactions, p=weights),
        'product_id': rng.choice(products_df['product_id'], num_transactions),
        'customer_id': rng.choice(customers_df['customer_id'], num_transactions),
        'quantity': rng.integers(1, 11, num_transactions),  # 1-10 items
        'unit_price': rng.uniform(10, 1000, num_transactions).round(2)
    }

    # Sort by date
    df = pd.DataFrame(transactions)
    df = df.sort_values('date')

    # Add some seasonality and trends
    def seasonal_price(row: pd.Series) -> float:
        price: float = row['unit_price'] * (1 + 0.1 * np.sin(row['date'].dayofyear * 2 * np.pi / 365))
        return price

    df['unit_price'] = df.apply(seasonal_price, axis=1)

    return df

def create_data_directory() -> None:
    """Create directory for data files if it doesn't exist."""
    if not os.path.exists('data'):
        os.makedirs('data')

def main() -> None:
    """Generate and save all datasets."""
    # Create data directory
    create_data_directory()

    # Generate datasets
    print("Generating products data...")
    products_df = generate_products()

    print("Generating customers data...")
    customers_df = generate_customers()

    print("Generating sales transactions...")
    sales_df = generate_sales(products_df, customers_df)

    # Save to CSV files
    products_df.to_csv('data/products.csv', index=False)
    customers_df.to_csv('data/customers.csv', index=False)
    sales_df.to_csv('data/sales_transactions.csv', index=False)

    print("Data generation complete. Files saved in 'data' directory:")
    print("- products.csv")
    print("- customers.csv")
    print("- sales_transactions.csv")

    # Print some basic statistics
    print("Dataset Statistics:")
    print(f"Number of products: {len(products_df)}")
    print(f"Number of customers: {len(customers_df)}")
    print(f"Number of transactions: {len(sales_df)}")

    # Sample of each dataset
    print("Products Sample:")
    print(products_df.head(3))
    print("Customers Sample:")
    print(customers_df.head(3))
    print("Sales Transactions Sample:")
    print(sales_df.head(3))

if __name__ == "__main__":
    main()
