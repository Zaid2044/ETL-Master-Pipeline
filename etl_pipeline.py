# etl_pipeline.py

import pandas as pd
import requests
import sqlite3
from datetime import datetime

# --- Configuration ---
CSV_FILE_PATH = 'online_sales.csv'
API_URL = 'https://fakestoreapi.com/products' # API for "in-store" product data
DB_NAME = 'sales_data.db'
TABLE_NAME = 'master_sales'

def extract_from_csv(file_path):
    """Extracts data from a CSV file into a pandas DataFrame."""
    print(f"[EXTRACT] Reading data from {file_path}...")
    try:
        df = pd.read_csv(file_path)
        print(f"[EXTRACT] Successfully extracted {len(df)} rows from CSV.")
        return df
    except FileNotFoundError:
        print(f"[ERROR] CSV file not found at {file_path}")
        return None

def extract_from_api(url):
    """Extracts data from a JSON API into a pandas DataFrame."""
    print(f"[EXTRACT] Fetching data from API: {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an exception for bad status codes (4xx or 5xx)
        data = response.json()
        df = pd.DataFrame(data)
        print(f"[EXTRACT] Successfully extracted {len(df)} rows from API.")
        return df
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] API request failed: {e}")
        return None

def transform(online_sales_df, api_products_df):
    """Transforms and merges data from the two sources."""
    if online_sales_df is None or api_products_df is None:
        print("[TRANSFORM] Skipping transformation due to extraction errors.")
        return None
    
    print("[TRANSFORM] Starting data transformation...")

    # --- Transform Online Sales Data (from CSV) ---
    online_sales_df.rename(columns={
        'product_sku': 'product_id',
        'quantity_sold': 'quantity',
        'sale_date': 'sale_timestamp',
        'unit_price_usd': 'price_usd'
    }, inplace=True)
    online_sales_df['source'] = 'online_csv'
    # Add a placeholder for product_name, as it's not in the CSV
    online_sales_df['product_name'] = 'N/A'

    # --- Transform In-Store Data (from API) ---
    # NOTE: For this example, we assume each product from the API represents one sale.
    # A real-world scenario would have a dedicated 'sales' API endpoint.
    api_products_df.rename(columns={
        'id': 'product_id',
        'title': 'product_name',
        'price': 'price_usd'
    }, inplace=True)
    api_products_df['source'] = 'in-store_api'
    api_products_df['quantity'] = 1 # Assume quantity of 1 for each API "sale"
    # Add a timestamp for the sale
    api_products_df['sale_timestamp'] = datetime.now().strftime('%Y-%m-%d')
    
    # --- Merge DataFrames ---
    # Select only the columns we need to ensure they match before merging
    common_columns = ['source', 'product_id', 'product_name', 'quantity', 'price_usd', 'sale_timestamp']
    master_df = pd.concat([
        online_sales_df[common_columns], 
        api_products_df[common_columns]
    ], ignore_index=True)

    # --- Final Transformations on Merged Data ---
    # Convert data types
    master_df['sale_timestamp'] = pd.to_datetime(master_df['sale_timestamp'])
    master_df['product_id'] = master_df['product_id'].astype(str) # Standardize product ID as string
    
    # Calculate a new column
    master_df['total_sale_value'] = master_df['quantity'] * master_df['price_usd']
    
    print(f"[TRANSFORM] Transformation complete. Final dataset has {len(master_df)} rows.")
    return master_df

def load(df, db_name, table_name):
    """Loads a DataFrame into a SQLite database."""
    if df is None:
        print("[LOAD] No data to load. Skipping.")
        return

    print(f"[LOAD] Connecting to database '{db_name}'...")
    try:
        with sqlite3.connect(db_name) as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"[LOAD] Successfully loaded {len(df)} rows into table '{table_name}'.")
    except Exception as e:
        print(f"[ERROR] Failed to load data into database: {e}")

def run_etl_pipeline():
    """Runs the full ETL pipeline."""
    print("--- Starting ETL Pipeline ---")
    
    # 1. EXTRACT
    online_data = extract_from_csv(CSV_FILE_PATH)
    api_data = extract_from_api(API_URL)
    
    # 2. TRANSFORM
    master_data = transform(online_data, api_data)
    
    # 3. LOAD
    load(master_data, DB_NAME, TABLE_NAME)
    
    print("\n--- ETL Pipeline Finished ---")
    if master_data is not None:
        print("\n--- Final Transformed Data (First 5 Rows) ---")
        print(master_data.head())
        print("\nTo inspect the final database, use a tool like 'DB Browser for SQLite' to open 'sales_data.db'.")

if __name__ == "__main__":
    run_etl_pipeline()