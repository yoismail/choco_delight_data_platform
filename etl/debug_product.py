import pandas as pd

sales = pd.read_csv("data/clean/cleaned_sales.csv")
products = pd.read_csv("data/clean/cleaned_products.csv")


# ======================================
# Debugging product_id issues in sales
# ======================================
def normalize_id(s):
    return (
        s.astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "")
        .str.replace("-", "")
    )


sales["product_id"] = normalize_id(sales["product_id"])
products["product_id"] = normalize_id(products["product_id"])

# find product_ids in sales not in products
# invalid = sales[~sales["product_id"].isin(products["product_id"])]
# invalid_ids = invalid["product_id"].value_counts()
# print(invalid_ids.head(20))

# print("Total invalid product IDs:", len(invalid))
