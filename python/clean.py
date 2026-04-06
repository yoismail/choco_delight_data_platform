import pandas as pd
import logging
import sys
from pathlib import Path
import zipfile

# ======================================
# Logging
# ======================================
from utils.logging import setup_logging
setup_logging()

# ======================================
# Paths
# ======================================
RAW_DATA_DIR = Path("data/raw")
CLEAN_DATA_DIR = Path("data/clean")
ZIP_NAME = "chocolate-sales-dataset-2023-2024.zip"

# ======================================
# Ensure clean directory exists
# ======================================


def create_clean_data_dir():
    CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)
    logging.info("Clean data directory ready.")

# ======================================
# Sanity check logging
# ======================================


def log_df_sanity(df: pd.DataFrame, name: str):
    logging.info(f"[{name}] shape: {df.shape}")
    logging.info(f"[{name}] missing values:\n{df.isnull().sum()}\n")
    logging.info(f"[{name}] dtypes:\n{df.dtypes}\n")
    logging.info(f"[{name}] sample data:\n{df.head(3)}\n")

# ======================================
# Schema validation
# ======================================


def validate_schema(df: pd.DataFrame, required_cols: list, name: str):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logging.error(f"{name}: Missing required columns: {missing}")
        sys.exit(1)
    logging.info(f"{name}: Schema validation passed.")

# ======================================
# Cleaning functions for each CSV
# ======================================


def clean_calendar(df: pd.DataFrame) -> pd.DataFrame:
    validate_schema(df, ["date", "year", "month", "week",
                    "day_of_week", "day"], "calendar")
    df = df.drop_duplicates()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["day_of_week"] = df["day_of_week"].astype(int).fillna("Unknown")
    df["month"] = df["month"].astype(int).fillna("Unknown")
    df["year"] = df["year"].astype(int).fillna("Unknown")
    df["week"] = df["week"].astype(int).fillna("Unknown")
    return df


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    validate_schema(df, ["customer_id", "gender", "join_date"], "customers")
    df = df.drop_duplicates()
    df["gender"] = df["gender"].str.title().fillna("Unknown")
    df["join_date"] = pd.to_datetime(df["join_date"], errors="coerce")
    return df


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    validate_schema(df, ["product_id", "product_name",
                    "brand", "category"], "products")
    df = df.drop_duplicates()
    df["brand"] = df["brand"].str.title().fillna("Unknown")
    df["product_name"] = df["product_name"].str.title().fillna("Unknown")
    df["category"] = df.get("category", "Unknown").fillna("Unknown")
    return df


def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    validate_schema(df, ["order_id", "store_id", "customer_id", "product_id",
                    "quantity", "revenue", "cost", "profit", "order_date", "unit_price"], "sales")
    df = df.drop_duplicates()
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    df["cost"] = pd.to_numeric(df["cost"], errors="coerce")
    df["profit"] = pd.to_numeric(df["profit"], errors="coerce")
    df["order_date"] = df["order_date"].str.strip()
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    return df


def clean_stores(df: pd.DataFrame) -> pd.DataFrame:
    validate_schema(df, ["store_id", "store_name",
                    "country", "city", "store_type"], "stores")
    df = df.drop_duplicates()
    df["country"] = df["country"].str.title().fillna("Unknown")
    df["city"] = df["city"].str.title().fillna("Unknown")
    df["store_type"] = df["store_type"].str.title().fillna("Unknown")
    return df

# ======================================
# Load each CSV inside ZIP
# ======================================


def load_csvs(zip_path: Path) -> dict:
    if not zip_path.exists():
        logging.error(f"ZIP not found: {zip_path}")
        sys.exit(1)

    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_files = [f for f in zf.namelist() if f.endswith(".csv")]

        if not csv_files:
            logging.error("No CSV files found in ZIP.")
            sys.exit(1)

        logging.info(f"CSV files found: {csv_files}")

        return {name: pd.read_csv(zf.open(name)) for name in csv_files}

# ======================================
# Clean each CSV
# ======================================


def clean_dataset():
    logging.info("Starting cleaning process...")

    zip_path = RAW_DATA_DIR / ZIP_NAME
    dfs = load_csvs(zip_path)

    # Map filenames to cleaning functions
    cleaning_map = {
        "calendar.csv": clean_calendar,
        "customers.csv": clean_customers,
        "products.csv": clean_products,
        "sales.csv": clean_sales,
        "stores.csv": clean_stores
    }

    for filename, df in dfs.items():
        logging.info(f"Processing: {filename}")
        log_df_sanity(df, filename)

        cleaner = cleaning_map.get(filename)

        if cleaner:
            df_cleaned = cleaner(df)
        else:
            logging.warning(
                f"No specific cleaning rules for {filename}. Using basic cleaning.")
            df_cleaned = df.drop_duplicates().fillna(method="ffill")

        log_df_sanity(df_cleaned, f"cleaned_{filename}")

        output_path = CLEAN_DATA_DIR / f"cleaned_{filename}"
        df_cleaned.to_csv(output_path, index=False)

        logging.info(f"Saved cleaned file: {output_path}")


# ======================================
# Main
# ======================================
if __name__ == "__main__":
    create_clean_data_dir()
    clean_dataset()
    logging.info("Cleaning process completed successfully.")
