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

    # Validate foreign key relationships (after merge)
    # ---------------------------------------------------------
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
    # ---------------------------------------------------------
    try:
        logging.info("Preparing dimension tables...")
        DIM_COLUMNS = {
            "customers": ["customer_id", "customer_key", "age", "gender", "join_date_formatted", "tenure_days", "tenure_months", "tenure_years", "customer_segment"],
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
    # ---------------------------------------------------------
    try:
        # Drop original ID columns and keep surrogate keys + measures
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
        logging.info(f"head of fact_sales:\n{fact_sales.head()}\n")
        logging.info(f"shape of fact_sales: {fact_sales.shape}")
        logging.info(f"datatype of fact_sales:\n{fact_sales.dtypes}")
        logging.info(
            f"missing values in fact_sales:\n{fact_sales.isnull().sum()}\n")
    except Exception as e:
        logging.error(f"Error during fact table creation: {e}")
        sys.exit(1)

    # Save dimension tables
    # ---------------------------------------------------------
    try:
        logging.info("Saving dimension tables...")
        customers.to_csv(TRANSFORM_DIR / "dim_customers.csv", index=False)
        logging.info("Saved dim_customers.csv")
        logging.info(f"head of customers dimension:\n{customers.head()}\n")
        logging.info(f"shape of customers dimension:\n{customers.shape}\n")
        logging.info(f"datatype of customers dimension:\n{customers.dtypes}\n")
        logging.info(
            f"missing values in customers dimension:\n{customers.isnull().sum()}\n")

        products.to_csv(TRANSFORM_DIR / "dim_products.csv", index=False)
        logging.info("Saved dim_products.csv")
        logging.info(f"head of products dimension:\n{products.head()}\n")
        logging.info(f"shape of products dimension:\n{products.shape}\n")
        logging.info(
            f"datatype of products dimension: {products.dtypes}")
        logging.info(
            f"missing values in products dimension:\n{products.isnull().sum()}\n")

        stores.to_csv(TRANSFORM_DIR / "dim_stores.csv", index=False)
        logging.info("Saved dim_stores.csv")
        logging.info(f"head of stores dimension:\n{stores.head()}\n")
        logging.info(f"shape of stores dimension:\n{stores.shape}\n")
        logging.info(f"datatype of stores dimension: {stores.dtypes}")
        logging.info(
            f"missing values in stores dimension:\n{stores.isnull().sum()}\n")

        calendar.to_csv(TRANSFORM_DIR / "dim_calendar.csv", index=False)
        logging.info("Saved dim_calendar.csv")
        logging.info(f"head of calendar dimension:\n{calendar.head()}\n")
        logging.info(f"shape of calendar dimension:\n{calendar.shape}\n")
        logging.info(
            f"datatype of calendar dimension: {calendar.dtypes}")
        logging.info(
            f"missing values in calendar dimension:\n{calendar.isnull().sum()}\n")

    except Exception as e:
        logging.error(f"Error during dimension table saving: {e}")
        sys.exit(1)


def run_pipeline():
    logging.info("Running Pipeline: Transform Dataset")
    transform_dataset()
    logging.info("Transformation stage completed.")


# Main
# --------------------------------------
if __name__ == "__main__":
    run_pipeline()
