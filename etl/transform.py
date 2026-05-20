import pandas as pd
import logging
from pathlib import Path
import sys

# Import logging setup from utils
from utils.logging import setup_logging, section, timed
setup_logging()

# Define directories
CLEAN_DIR = Path("data/clean")
TRANSFORM_DIR = Path("data/transform")
TRANSFORM_DIR.mkdir(parents=True, exist_ok=True)

# Helper to log dataset info cleanly


def log_dataset_info(df, name):
    """Log number of rows, columns, and data types in a clean format."""
    logging.info(f"📊 Dataset: {name}")
    logging.info(f"   • Rows     : {df.shape[0]:,}")
    logging.info(f"   • Columns  : {df.shape[1]:,}")
    logging.info("   • Data Types:")
    for col, dtype in df.dtypes.items():
        logging.info(f"      - {col:<25} → {dtype}")
    logging.info("-" * 60)

# Function to load cleaned data


def load_cleaned(name):
    if not (CLEAN_DIR / f"cleaned_{name}.csv").exists():
        logging.error(f"Cleaned file for {name} not found.")
        sys.exit(1)
    df = pd.read_csv(CLEAN_DIR / f"cleaned_{name}.csv")
    log_dataset_info(df, f"cleaned_{name}")  # Log after loading
    return df

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
    section("Loading Cleaned Data for Transformation")
    logging.info(f"✅ Loading cleaned data from {CLEAN_DIR}...")
    try:
        customers = load_cleaned("customers")
        products = load_cleaned("products")
        stores = load_cleaned("stores")
        calendar = load_cleaned("calendar")
        sales = load_cleaned("sales")
        regions = load_cleaned("regions")

        logging.info("✅ Cleaned data loaded successfully.")

    except Exception as e:
        logging.error(f" ❌ Error occurred while loading cleaned data: {e}")
        sys.exit(1)

    section("Cleaning Date Columns")
    logging.info("✅ Cleaning date columns in calendar and sales...")
    # Clean Calendar date
    calendar["calendar_date"] = clean_date(calendar["calendar_date"])
    # Clean Sales order_date
    sales["order_date"] = clean_date(sales["order_date"])
    # Clean Customers join_date
    customers["join_date"] = clean_date(customers["join_date"])
    logging.info("✅ Date columns cleaned and converted to datetime.date.")

    section("Linking Stores to Regions")
    try:
        logging.info("✅ Linking stores to regions...")
        # Match stores to regions using country (exact match from your clean logic)
        stores = stores.merge(
            regions[["country", "region_key"]],
            on="country",
            how="left"
        )
        # Handle unknown regions safely
        stores["region_key"] = stores["region_key"].fillna(-1).astype(int)
        logging.info("✅ region_key added to stores.")
    except Exception as e:
        logging.error(f" ❌ Error linking stores and regions: {e}")
        sys.exit(1)

    section("Merging Surrogate Keys into Sales Fact Table")
    try:
        logging.info("✅ Merging surrogate keys into sales fact table...")

        sales = (
            sales
            .merge(customers[["customer_id", "customer_key"]], on="customer_id", how="left")
            .merge(products[["product_id", "product_key"]], on="product_id", how="left")
            # Now stores HAS region_key, so we bring it here ↓
            .merge(stores[["store_id", "store_key", "region_key"]], on="store_id", how="left")
            .merge(calendar[["calendar_date", "calendar_key"]],
                   left_on="order_date",
                   right_on="calendar_date",
                   how="left")
        )

        logging.info("✅ Surrogate keys merged into sales fact table.")
    except Exception as e:
        logging.error(f" ❌ Error merging surrogate keys: {e}")
        sys.exit(1)

    section("Validating Foreign Key Relationships")
    try:
        logging.info("✅ Validating foreign key relationships...")
        validate_foreign_keys(sales, customers, "customer_key",
                              "customer_key", "sales→customers")
        validate_foreign_keys(sales, products, "product_key",
                              "product_key", "sales→products")
        validate_foreign_keys(sales, stores, "store_key",
                              "store_key", "sales→stores")
        validate_foreign_keys(sales, regions, "region_key",
                              "region_key", "sales→regions")  # ✅ Now works
        validate_foreign_keys(sales, calendar, "calendar_key",
                              "calendar_key", "sales→calendar")

        logging.info("✅ All foreign key relationships validated successfully.")
    except Exception as e:
        logging.error(f" ❌ Error during foreign key validation: {e}")
        sys.exit(1)

    section("Preparing Dimension Tables")
    try:
        logging.info("✅ Preparing dimension tables...")
        DIM_COLUMNS = {
            "customers": ["customer_id", "customer_key", "age", "gender", "join_date", "tenure_days", "tenure_months", "tenure_years", "customer_segment"],
            "products": ["product_id", "product_key", "product_name", "brand", "category", "brand_tier"],
            # ✅ Added region_key
            "stores": ["store_id", "store_key", "store_name", "country", "city", "store_type", "region", "region_tier", "region_key"],
            "calendar": ["calendar_key", "calendar_date", "quarter", "season", "day_type"],
            "regions": ["region_key", "region", "region_tier", "country"]
        }

        customers = customers[DIM_COLUMNS["customers"]]
        products = products[DIM_COLUMNS["products"]]
        stores = stores[DIM_COLUMNS["stores"]]
        calendar = calendar[DIM_COLUMNS["calendar"]]
        regions = regions[DIM_COLUMNS["regions"]]

        logging.info("✅ Dimension tables prepared successfully.")

    except Exception as e:
        logging.error(f" ❌ Error during dimension table preparation: {e}")
        sys.exit(1)

    section("Building Fact Table")
    try:

        logging.info("✅ Building fact_sales table...")
        fact_sales = sales.drop(
            columns=["customer_id", "product_id", "store_id", "calendar_date"])

        # ✅ Added region_key here
        key_columns = ["customer_key", "product_key",
                       "store_key", "calendar_key", "region_key", "order_date"]
        other_columns = [
            col for col in fact_sales.columns if col not in key_columns]
        fact_sales = fact_sales[key_columns + other_columns]
        logging.info("✅ fact_sales table built successfully.")

        # Save fact table first to ensure it's available for any downstream processes that might need it before dimensions are fully ready
        section("Saving Fact Table")
        fact_sales.to_csv(TRANSFORM_DIR / "fact_sales.csv", index=False)
        logging.info("✅ Saved fact_sales.csv")
    except Exception as e:
        logging.error(f" ❌ Error during fact table creation: {e}")
        sys.exit(1)

    section("Saving Dimension Tables")
    try:
        customers.to_csv(TRANSFORM_DIR / "dim_customers.csv", index=False)
        products.to_csv(TRANSFORM_DIR / "dim_products.csv", index=False)
        stores.to_csv(TRANSFORM_DIR / "dim_stores.csv", index=False)
        calendar.to_csv(TRANSFORM_DIR / "dim_calendar.csv", index=False)
        regions.to_csv(TRANSFORM_DIR / "dim_regions.csv", index=False)

        logging.info("✅ Dimension tables saved successfully.")

        # Final log of tables after saving
        log_dataset_info(fact_sales, "FINAL: fact_sales")
        log_dataset_info(customers, "FINAL: dim_customers")
        log_dataset_info(products, "FINAL: dim_products")
        log_dataset_info(stores, "FINAL: dim_stores")
        log_dataset_info(calendar, "FINAL: dim_calendar")
        log_dataset_info(regions, "FINAL: dim_regions")
    except Exception as e:
        logging.error(f" ❌ Error during dimension table saving: {e}")
        sys.exit(1)


@timed
def run_pipeline():
    section("Starting Transformation Pipeline")
    transform_dataset()
    logging.info("🎉 Transformation stage completed.")


if __name__ == "__main__":
    run_pipeline()
