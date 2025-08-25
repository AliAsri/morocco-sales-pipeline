# run_etl.py

import pandas as pd
import os

def run_pipeline():
    # --- 1. EXTRACT ---
    # The script will run from the root, so the path is relative
    input_path = 'data/Orders.csv'
    print(f"\n1. Extracting data from {input_path}...")
    df = pd.read_csv(input_path, encoding='utf-8')
    print(f"   Extracted {len(df)} rows.")

    # --- 2. TRANSFORM ---
    print("\n2. Transforming data...")
    df['Order Date'] = pd.to_datetime(df['Order Date'], dayfirst=True)
    df['Ship Date'] = pd.to_datetime(df['Ship Date'], dayfirst=True)
    df['Profit Margin'] = (df['Profit'] / df['Sales']) * 100
    transformed_df = df[['Order ID', 'Order Date', 'Category', 'Sales', 'Profit', 'Profit Margin']]
    print("   Transformation complete.")

    # --- 3. LOAD ---
    # Ensure the output directory exists
    os.makedirs('output', exist_ok=True)
    output_path = 'output/sales_report.parquet'
    print(f"\n3. Loading transformed data to {output_path}...")
    transformed_df.to_parquet(output_path, index=False)
    print("   Data loaded successfully.")

if __name__ == "__main__":
    run_pipeline()
    print("\n--- ETL Process Finished ---")