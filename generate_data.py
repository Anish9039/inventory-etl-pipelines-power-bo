import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# --- CONFIGURATION ---
NUM_ROWS = 1000
products = {
    'P101': 'Laptop', 'P102': 'Mouse', 'P103': 'Keyboard', 
    'P104': 'Monitor', 'P105': 'Headset'
}
prices = {'P101': 1200, 'P102': 25, 'P103': 45, 'P104': 300, 'P105': 80}

# --- 1. GENERATE STORE A (CSV - Clean) ---
# Format: Date, Product_ID, Quantity, Revenue
dates = [datetime(2025, 1, 1) + timedelta(days=x) for x in range(NUM_ROWS)]
data_a = {
    'Date': [random.choice(dates) for _ in range(NUM_ROWS)],
    'Product_ID': [random.choice(list(products.keys())) for _ in range(NUM_ROWS)],
    'Quantity': [random.randint(1, 5) for _ in range(NUM_ROWS)]
}
df_a = pd.DataFrame(data_a)
df_a['Revenue'] = df_a.apply(lambda x: x['Quantity'] * prices[x['Product_ID']], axis=1)

# Save as CSV
df_a.to_csv('raw data/sales_store_a.csv', index=False)
print("Created sales_store_a.csv")

# --- 2. GENERATE STORE B (JSON - Messy) ---
# Format: Transaction_Time, Item_Code, Units_Sold (Different Names!)
data_b = {
    'Transaction_Time': [str(random.choice(dates)) for _ in range(NUM_ROWS)],
    'Item_Code': [random.choice(list(products.keys())) for _ in range(NUM_ROWS)],
    'Units_Sold': [random.randint(1, 10) for _ in range(NUM_ROWS)]
}
df_b = pd.DataFrame(data_b)

# Save as JSON (Records format is common for APIs)
df_b.to_json('raw data/sales_store_b.json', orient='records')
print("Created sales_store_b.json")

# --- 3. GENERATE INVENTORY (CSV) ---
# Format: Product_ID, Product_Name, Current_Stock, Reorder_Point
inventory_data = {
    'Product_ID': list(products.keys()),
    'Product_Name': list(products.values()),
    'Current_Stock': [random.randint(5, 50) for _ in range(len(products))], # Low stock simulation
    'Reorder_Point': [20, 20, 20, 20, 20]
}
df_inv = pd.DataFrame(inventory_data)

# Save as CSV
df_inv.to_csv('raw data/inventory_master.csv', index=False)
print("Created inventory_master.csv")
