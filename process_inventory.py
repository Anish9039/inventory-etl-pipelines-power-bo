import polars as pl
import os

# --- CONFIGURATION ---
raw_path = "raw data/"
output_path = "processed_data/"

# Create output folder if not exists
os.makedirs(output_path, exist_ok=True)

print("--- 1. LOADING DATA ---")

# Load Store A (CSV) - The Clean One
df_a = pl.read_csv(f"{raw_path}sales_store_a.csv")
print(f"Store A Loaded: {df_a.height} rows")

# Load Store B (JSON) - The Messy One
# We use read_json because standard JSON arrays are not supported in lazy scanning yet
df_b = pl.read_json(f"{raw_path}sales_store_b.json")
print(f"Store B Loaded: {df_b.height} rows")

# Load Inventory Master (CSV)
df_inv = pl.read_csv(f"{raw_path}inventory_master.csv")

print("2. TRANSFORMATION & STANDARDIZATION")

# STORE B CLEANUP:
# 1. Rename columns to match Store A (Item_Code -> Product_ID)
# 2. Add a 'Source' column so we know where the sale came from
df_b_clean = (
    df_b
    .rename({"Item_Code": "Product_ID", "Units_Sold": "Quantity", "Transaction_Time": "Date"})
    .select(["Date", "Product_ID", "Quantity"]) # Keep only matching columns
    .with_columns(pl.lit("Store_B").alias("Source"))
)

# STORE A CLEANUP:
# Add Source column
df_a_clean = (
    df_a
    .select(["Date", "Product_ID", "Quantity"])
    .with_columns(pl.lit("Store_A").alias("Source"))
)

# MERGE (CONCATENATE) BOTH STORES
df_total_sales = pl.concat([df_a_clean, df_b_clean])

print("3. AGGREGATION & BUSINESS LOGIC")

# Calculate Total Sales per Product
df_sales_summary = (
    df_total_sales 
    .group_by("Product_ID")
    .agg(pl.col("Quantity").sum().alias("Total_Sold"))
)

# JOIN with Inventory Data
# Left Join: We want all Inventory items, even if they haven't sold yet
df_final = (
    df_inv.join(df_sales_summary, on="Product_ID", how="left")
    .with_columns([
        # Fill nulls with 0 (if an item had no sales)
        pl.col("Total_Sold").fill_null(0)
    ])
    .with_columns([
        # Logic: Remaining = Start - Sold
        (pl.col("Current_Stock") - pl.col("Total_Sold")).alias("Stock_Remaining")
    ])
.with_columns([
        # THE AGGRESSIVE STATUS LOGIC
        pl.when(pl.col("Stock_Remaining") <= pl.col("Reorder_Point"))
        .then(pl.lit("CRITICAL"))
        
        # We widened the gap to +50. 
        # Anything between Reorder (e.g. 20) and Reorder+50 (e.g. 70) will now be LOW.
        .when(pl.col("Stock_Remaining") <= (pl.col("Reorder_Point") + 50))
        .then(pl.lit("Low"))
        
        .otherwise(pl.lit("Healthy"))
        .alias("Status")
    ])
)

print("--- 4. SAVING ---")
# Inspect the top 5 rows to be sure
print(df_final.head(5))

# Save for Power BI
df_final.write_parquet(f"{output_path}inventory_analysis.parquet")
df_total_sales.write_parquet(f"{output_path}sales_transactions.parquet") # Save transactions too for time trends

print("SUCCESS: Data processed and ready for Power BI!")