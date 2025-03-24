#!/usr/bin/env python

"""
Generate data
"""

import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

def generate_products(num_products=100):
    """Generate product data."""
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports',
                 'Toys', 'Food & Beverages', 'Beauty', 'Automotive', 'Health']

    products = {
        'product_id': range(1, num_products + 1),
        'product_name': [f'Product_{i}' for i in range(1, num_products + 1)],
        'category': np.random.choice(categories, num_products),
        'supplier_id': np.random.randint(1, 21, num_products)  # 20 suppliers
    }

    return pd.DataFrame(products)

def generate_customers(num_customers=1000):
    """Generate customer data."""
    segments = ['Premium', 'Standard', 'Basic']
    countries = ['USA', 'UK', 'Canada', 'Australia', 'Germany', 'France', 'Japan', 'Spain']

    # Generate join dates over the last 3 years
    end_date = datetime.now()
    start_date = end_date - timedelta(days=3*365)
    join_dates = [start_date + timedelta(days=np.random.randint(0, 3*365))
                 for _ in range(num_customers)]

    customers = {
        'customer_id': range(1, num_customers + 1),
        'customer_segment': np.random.choice(segments, num_customers,
                                           p=[0.2, 0.5, 0.3]),  # weighted probabilities
        'join_date': join_dates,
        'country': np.random.choice(countries, num_customers)
    }

    return pd.DataFrame(customers)

def generate_sales(products_df, customers_df, num_transactions=50000):
    """Generate sales transaction data."""
    # Date range for the last year
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)

    # Generate more transactions in recent months
    days = pd.date_range(start=start_date, end=end_date, freq='D')
    weights = np.linspace(0.5, 1.5, len(days))  # Increasing weights for more recent dates
    weights = weights / weights.sum()

    # Generate transaction data
    transactions = {
        'transaction_id': range(1, num_transactions + 1),
        'date': np.random.choice(days, num_transactions, p=weights),
        'product_id': np.random.choice(products_df['product_id'], num_transactions),
        'customer_id': np.random.choice(customers_df['customer_id'], num_transactions),
        'quantity': np.random.randint(1, 11, num_transactions),  # 1-10 items
        'unit_price': np.random.uniform(10, 1000, num_transactions).round(2)
    }

    # Sort by date
    df = pd.DataFrame(transactions)
    df = df.sort_values('date')

    # Add some seasonality and trends
    df['unit_price'] = df.apply(lambda x:
        x['unit_price'] * (1 + 0.1 * np.sin(x['date'].dayofyear * 2 * np.pi / 365)), axis=1)

    return df

def create_data_directory():
    """Create directory for data files if it doesn't exist."""
    if not os.path.exists('data'):
        os.makedirs('data')

def main():
    """Generate and save all datasets."""
    # Set random seed for reproducibility
    np.random.seed(42)

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
