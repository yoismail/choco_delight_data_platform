import pandas as pd

# Load cleaned datasets
sales = pd.read_csv("data/raw/sales.csv")
products = pd.read_csv("data/raw/products.csv")


# Identify invalid/unmatched product IDs
unmatched = sales[~sales["product_id"].isin(
    products["product_id"])]
unmatched_summary = unmatched["product_id"].value_counts()

# Output results
print("=== Unmatched Product IDs (count) ===")
print(unmatched_summary.head())

print(f"\nTotal rows with unmatched IDs: {len(unmatched)}")
