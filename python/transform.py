import pandas as pd
import logging
from pathlib import Path
import sys


# ======================================
# Logging Configuration
# ======================================
from utils.logging import setup_logging
setup_logging()

# ======================================
# Paths and Dir Setup
# ======================================

CLEAN_DIR = Path("data/clean")
TRANSFORM_DIR = Path("data/transform")
TRANSFORM_DIR.mkdir(parents=True, exist_ok=True)

# ======================================
# Load cleaned data
# ======================================


def load_cleaned(name):
    return pd.read_csv(CLEAN_DIR / f"cleaned_{name}.csv")


# ======================================
# Foreign key validation
# ======================================
def validate_foreign_keys(df_fact, df_dim, fact_key, dim_key, name):
    missing = df_fact[~df_fact[fact_key].isin(df_dim[dim_key])]
    if not missing.empty:
        logging.warning(
            f"{name}: {len(missing)} rows have invalid foreign keys.")
        sys.exit(1)
    else:
        logging.info(f"{name}: Foreign key validation passed.")


# ======================================
# Transform Dataset
# ======================================
def transform_dataset():
    logging.info("Starting transform stage...")
    try:
        customers = load_cleaned("customers")
        products = load_cleaned("products")
        stores = load_cleaned("stores")
        calendar = load_cleaned("calendar")
        sales = load_cleaned("sales")
    except Exception as e:
        logging.error(f"Error occurred while loading cleaned data: {e}")
        sys.exit(1)

    # Foreign key validation
    try:
        validate_foreign_keys(sales, customers, "customer_id",
                              "customer_id", "sales→customers")
        validate_foreign_keys(sales, products, "product_id",
                              "product_id", "sales→products")
        validate_foreign_keys(sales, stores, "store_id",
                              "store_id", "sales→stores")
        validate_foreign_keys(sales, calendar, "order_date",
                              "calendar_date", "sales→calendar")
    except Exception as e:
        logging.error(f"Error during foreign key validation: {e}")
        sys.exit(1)

    # Define the fact table and dimension tables
    try:
        DIM_COLUMNS = {
            "customers": ["customer_id", "gender", "join_date"],
            "products": ["product_id", "product_name", "brand", "category"],
            "stores": ["store_id", "store_name", "country", "city", "store_type"],
            "calendar": ["calendar_date", "year", "month", "week", "day_of_week"]
        }

        customers = customers[DIM_COLUMNS["customers"]]
        products = products[DIM_COLUMNS["products"]]
        stores = stores[DIM_COLUMNS["stores"]]
        calendar = calendar[DIM_COLUMNS["calendar"]]
    except Exception as e:
        logging.error(f"Error during dimension table preparation: {e}")
        sys.exit(1)

    # Build fact table
    try:
        fact_sales = sales.merge(customers, on="customer_id", how="left") \
            .merge(products, on="product_id", how="left") \
            .merge(stores, on="store_id", how="left") \
            .merge(calendar, left_on="order_date", right_on="calendar_date", how="left")

        fact_sales.to_csv(TRANSFORM_DIR / "fact_sales.csv", index=False)
        logging.info("Saved fact_sales.csv")
    except Exception as e:
        logging.error(f"Error during fact table creation: {e}")
        sys.exit(1)

    # Save dimensions
    try:
        customers.to_csv(TRANSFORM_DIR / "dim_customers.csv", index=False)
        products.to_csv(TRANSFORM_DIR / "dim_products.csv", index=False)
        stores.to_csv(TRANSFORM_DIR / "dim_stores.csv", index=False)
        calendar.to_csv(TRANSFORM_DIR / "dim_calendar.csv", index=False)

    except Exception as e:
        logging.error(f"Error during dimension table saving: {e}")
        sys.exit(1)


def run_pipeline():
    logging.info("Running Pipeline: Transform Dataset")
    transform_dataset()
    logging.info("Transformation stage completed.")


# ======================================
# Running Pipeline
# ======================================
if __name__ == "__main__":
    run_pipeline()
