import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

# --- CONFIGURATION ---
NUM_TRANSACTIONS = 2000
NUM_PRODUCTS = 50
OUTPUT_FOLDER = 'raw_data'

os.makedirs(OUTPUT_FOLDER, exist_ok=True)
print(f"--- GENERATING REALISTIC DATA ---")

# 1. Define Products
categories = {
    "Electronics": ["Laptop", "Monitor", "Tablet", "Smartphone"],
    "Accessories": ["Mouse", "Keyboard", "Headset", "Webcam"],
    "Cables": ["HDMI Cable", "USB-C Cable", "Ethernet Cable"],
    "Furniture": ["Office Chair", "Desk Lamp", "Standing Desk"]
}

product_ids = [f"P{100+i}" for i in range(NUM_PRODUCTS)]
product_meta = {}

# 2. GENERATE SALES FIRST (So we know how much demand there is)
dates = [datetime(2025, 1, 1) + timedelta(days=x % 90) for x in range(NUM_TRANSACTIONS)]

# Store A Sales
data_a = {
    'Date': [random.choice(dates) for _ in range(NUM_TRANSACTIONS)],
    'Product_ID': [random.choice(product_ids) for _ in range(NUM_TRANSACTIONS)],
    'Quantity': [random.randint(1, 3) for _ in range(NUM_TRANSACTIONS)]
}
df_a = pd.DataFrame(data_a)
df_a.to_csv(f'{OUTPUT_FOLDER}/sales_store_a.csv', index=False)

# Store B Sales
data_b = {
    'Transaction_Time': [str(random.choice(dates)) for _ in range(NUM_TRANSACTIONS)],
    'Item_Code': [random.choice(product_ids) for _ in range(NUM_TRANSACTIONS)],
    'Units_Sold': [random.randint(1, 5) for _ in range(NUM_TRANSACTIONS)]
}
df_b = pd.DataFrame(data_b)
df_b.to_json(f'{OUTPUT_FOLDER}/sales_store_b.json', orient='records')

# 3. CALCULATE TOTAL DEMAND PER PRODUCT
# We group both stores to find out exactly how many were sold
total_sold_map = {}
for pid in product_ids:
    sold_a = df_a[df_a['Product_ID'] == pid]['Quantity'].sum()
    sold_b = df_b[df_b['Item_Code'] == pid]['Units_Sold'].sum()
    total_sold_map[pid] = sold_a + sold_b

print("✅ Sales Calculated. Now generating Inventory to match...")

# 4. GENERATE INVENTORY (REVERSE ENGINEERED)
inventory_rows = []

for pid in product_ids:
    cat = random.choice(list(categories.keys()))
    name = f"{random.choice(['Pro', 'Basic', 'Super'])} {random.choice(categories[cat])}"
    
    # How many did we sell?
    demand = total_sold_map.get(pid, 0)
    reorder_pt = 30
    
    # Force the Status Distribution
    # 10% Critical | 20% Low | 70% Healthy (REALISTIC)
    status_goal = random.choices(["critical", "low", "healthy"], weights=[0.1, 0.2, 0.7])[0]
    
    if status_goal == "critical":
        # We want Remaining < Reorder Point (20). 
        # So Start Stock must be LESS than Demand + 20.
        # Let's make Remaining negative to be sure.
        current_stock = int(demand * 0.8) # We only stocked 80% of demand
        
    elif status_goal == "low":
        # We want Remaining to be close to Reorder Point.
        # Start = Demand + Reorder + Buffer
        current_stock = demand + reorder_pt + 5
        
    else: # Healthy
        # We want tons of stock.
        current_stock = demand + 200 # Safe buffer

    inventory_rows.append({
        'Product_ID': pid,
        'Product_Name': name,
        'Category': cat,
        'Current_Stock': current_stock,
        'Reorder_Point': reorder_pt
    })

df_inv = pd.DataFrame(inventory_rows)
df_inv.to_csv(f'{OUTPUT_FOLDER}/inventory_master.csv', index=False)

print("\n🎉 SUCCESS: Data Generated with PERFECT Logic.")