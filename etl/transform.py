import pandas as pd
import logging
from pathlib import Path
import sys

# Import logging setup from utils
from utils.logging import setup_logging
setup_logging()

# Define directories
CLEAN_DIR = Path("data/clean")
TRANSFORM_DIR = Path("data/transform")
TRANSFORM_DIR.mkdir(parents=True, exist_ok=True)

# Function to load cleaned data


def load_cleaned(name):
    return pd.read_csv(CLEAN_DIR / f"cleaned_{name}.csv")

# Helper to clean and convert date columns safely


def clean_date(col):
    return (
        col.astype(str)
        .str.replace(r"[^\d\-: ]", "", regex=True)  # remove hidden chars
        .str.strip()
        .pipe(pd.to_datetime, errors="coerce")
        .dt.date
    )

# Function to validate foreign key relationships


def validate_foreign_keys(df_fact, df_dim, fact_key, dim_key, name):
    missing = df_fact[~df_fact[fact_key].isin(df_dim[dim_key])]
    if not missing.empty:
        logging.warning(
            f"{name}: {len(missing)} rows have invalid foreign keys.")
        sys.exit(1)
    else:
        logging.info(f"{name}: Foreign key validation passed.")

# Main transformation function


def transform_dataset():
    logging.info("Starting transform stage...")

    # Load cleaned data
    try:
        customers = load_cleaned("customers")
        products = load_cleaned("products")
        stores = load_cleaned("stores")
        calendar = load_cleaned("calendar")
        sales = load_cleaned("sales")
    except Exception as e:
        logging.error(f"Error occurred while loading cleaned data: {e}")
        sys.exit(1)

    # Convert date columns using robust cleaner
    calendar["calendar_date"] = clean_date(calendar["calendar_date"])
    sales["order_date"] = clean_date(sales["order_date"])

    # Merge surrogate keys into sales fact table
    try:
        logging.info("Merging surrogate keys into sales fact table...")
        sales = (
            sales
            .merge(customers[["customer_id", "customer_key"]], on="customer_id", how="left")
            .merge(products[["product_id", "product_key"]], on="product_id", how="left")
            .merge(stores[["store_id", "store_key"]], on="store_id", how="left")
            .merge(calendar[["calendar_date", "calendar_key"]],
                   left_on="order_date",
                   right_on="calendar_date",
                   how="left")
        )
    except Exception as e:
        logging.error(f"Error merging surrogate keys: {e}")
        sys.exit(1)

    # Validate foreign key relationships
    try:
        logging.info("Validating foreign key relationships...")
        validate_foreign_keys(sales, customers, "customer_key",
                              "customer_key", "sales→customers")
        validate_foreign_keys(sales, products, "product_key",
                              "product_key", "sales→products")
        validate_foreign_keys(sales, stores, "store_key",
                              "store_key", "sales→stores")
        validate_foreign_keys(sales, calendar, "calendar_key",
                              "calendar_key", "sales→calendar")
    except Exception as e:
        logging.error(f"Error during foreign key validation: {e}")
        sys.exit(1)

    # Prepare dimension tables
    try:
        logging.info("Preparing dimension tables...")
        DIM_COLUMNS = {
            "customers": ["customer_id", "customer_key", "age", "gender", "join_date", "tenure_days", "tenure_months", "tenure_years", "customer_segment"],
            "products": ["product_id", "product_key", "product_name", "brand", "category", "brand_tier"],
            "stores": ["store_id", "store_key", "store_name", "country", "city", "store_type", "region", "region_tier"],
            "calendar": ["calendar_key", "calendar_date", "quarter", "season", "day_type"]
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
        logging.info("Building fact_sales table...")
        fact_sales = sales.drop(
            columns=["customer_id", "product_id", "store_id", "calendar_date"])

        key_columns = ["customer_key", "product_key",
                       "store_key", "calendar_key", "order_date"]
        other_columns = [
            col for col in fact_sales.columns if col not in key_columns]
        fact_sales = fact_sales[key_columns + other_columns]

        fact_sales.to_csv(TRANSFORM_DIR / "fact_sales.csv", index=False)
        logging.info("Saved fact_sales.csv")
    except Exception as e:
        logging.error(f"Error during fact table creation: {e}")
        sys.exit(1)

    # Save dimension tables
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


if __name__ == "__main__":
    run_pipeline()
