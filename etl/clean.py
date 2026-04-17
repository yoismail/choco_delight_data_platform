import pandas as pd
import logging
import sys
from pathlib import Path
import zipfile

# Logging
from utils.logging import setup_logging
setup_logging()

# Paths
RAW_DATA_DIR = Path("data/raw")
CLEAN_DATA_DIR = Path("data/clean")
ZIP_NAME = "chocolate-sales-dataset-2023-2024.zip"

# Ensure clean directory exists


def create_clean_data_dir():
    CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)
    logging.info("Clean data directory ready.")

# Sanity check logging


def log_df_sanity(df: pd.DataFrame, name: str):
    logging.info(f"[{name}] shape: {df.shape}")
    logging.info(f"[{name}] missing values:\n{df.isnull().sum()}\n")
    logging.info(f"[{name}] dtypes:\n{df.dtypes}\n")
    logging.info(f"[{name}] sample data:\n{df.head(3)}\n")


# Column normalization
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    return df


# Schema validation


def validate_schema(df: pd.DataFrame, required_cols: list, name: str):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logging.error(f"{name}: Missing required columns: {missing}")
        sys.exit(1)
    logging.info(f"{name}: Schema validation passed.")

# Cleaning functions for each CSV


def clean_calendar(df: pd.DataFrame) -> pd.DataFrame:

    validate_schema(df, ["date", "year", "month", "week",
                         "day_of_week", "day"], "calendar")

    df = normalize_columns(df)
    df = df.drop_duplicates()
    df = df.rename(columns={"date": "calendar_date"})

    # Convert date
    df["calendar_date"] = pd.to_datetime(df["calendar_date"], errors="coerce")

    # Numeric coercion for all integer-like fields
    numeric_cols = ["year", "month", "week", "day_of_week", "day"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Fill missing numeric values with -1 and convert to int
    df[numeric_cols] = df[numeric_cols].fillna(-1).astype(int)

    # # Create Surrogate key
    df = df.sort_values("calendar_date").reset_index(drop=True)
    df["calendar_key"] = df.index + 1

    # Reorder columns
    df = df[["calendar_key", "calendar_date"] +
            [c for c in df.columns if c not in ["calendar_key", "calendar_date"]]]

    # --- Feature Engineering: Calendar ---

    # Quarter (string)
    df["quarter"] = df["month"].apply(lambda m: f"Q{((m-1)//3)+1}")

    # Season (string)
    SEASON_MAP = {
        12: "Winter", 1: "Winter", 2: "Winter",
        3: "Spring", 4: "Spring", 5: "Spring",
        6: "Summer", 7: "Summer", 8: "Summer",
        9: "Autumn", 10: "Autumn", 11: "Autumn"
    }
    df["season"] = df["month"].map(SEASON_MAP)

    # Weekend flag (string)
    df["day_type"] = df["day_of_week"].apply(
        lambda d: "Weekend" if d in [6, 7] else "Weekday"
    )

    # Readable date (e.g., "14 Mar 2024")
    df["calendar_date_formatted"] = df["calendar_date"].dt.strftime("%d %b %Y")

    return df


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    validate_schema(df, ["customer_id", "gender", "join_date"], "customers")

    df = normalize_columns(df)
    df = df.drop_duplicates()
    df["gender"] = df["gender"].str.title().fillna("Unknown")
    df["join_date"] = pd.to_datetime(
        df["join_date"].astype(str).str.strip(), errors="coerce")
    # df["join_date_formatted"] = df["join_date"].dt.strftime("%d %b %Y")

    # # Create Surrogate key
    df = df.sort_values("customer_id").reset_index(drop=True)
    df["customer_key"] = df.index + 1

    # Reorder columns
    df = df[["customer_key", "customer_id"] +
            [c for c in df.columns if c not in ["customer_key", "customer_id"]]]

    # --- Feature Engineering: Customers ---

    # Tenure in days
    df["tenure_days"] = (pd.Timestamp.today() - df["join_date"]).dt.days

    # Tenure in months
    tenure_months = (df["tenure_days"] / 30).round().astype(int)
    df["tenure_months"] = tenure_months.astype(str) + " months"

    # Tenure in years
    tenure_years = (df["tenure_days"] / 365).round(1)
    df["tenure_years"] = tenure_years.astype(str) + " years"

    # Customer segment (based on months)
    df["customer_segment"] = pd.cut(
        tenure_months,
        bins=[-1, 6, 12, 24, float("inf")],
        labels=["New", "Active", "Loyal", "VIP"]
    )

    return df


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    validate_schema(df, ["product_id", "product_name",
                    "brand", "category"], "products")

    df = normalize_columns(df)
    df = df.drop_duplicates()
    df["brand"] = df["brand"].str.title().fillna("Unknown")
    df["product_name"] = df["product_name"].str.title().fillna("Unknown")
    df["category"] = df.get("category", "Unknown").fillna("Unknown")

    # Add missing product IDs found during debugging
    missing_ids = ["P0000", "P0201"]

    new_products = pd.DataFrame({
        "product_id": missing_ids,
        "product_name": ["Unknown Product 0000", "Unknown Product 0201"],
        "brand": ["Unknown", "Unknown"],
        "category": ["Unknown", "Unknown"]
    })

    # FIX: concat with df, not 'products'
    df = pd.concat([df, new_products], ignore_index=True)

    # Create Surrogate key
    df = df.sort_values("product_id").reset_index(drop=True)
    df["product_key"] = df.index + 1

    # Reorder columns
    df = df[["product_key", "product_id"] +
            [c for c in df.columns if c not in ["product_key", "product_id"]]]

    # --- Feature Engineering: Products ---

    # Brand tier
    PREMIUM_BRANDS = ["Lindt", "Godiva", "Green & Black"]
    df["brand_tier"] = df["brand"].apply(
        lambda b: "Premium Brand" if b in PREMIUM_BRANDS else "Standard Brand"
    )

    return df


def clean_stores(df: pd.DataFrame) -> pd.DataFrame:
    validate_schema(df, ["store_id", "store_name",
                    "country", "city", "store_type"], "stores")

    df = normalize_columns(df)
    df = df.drop_duplicates()

    df["country"] = df["country"].str.title().fillna("Unknown")
    df["city"] = df["city"].str.title().fillna("Unknown")
    df["store_type"] = df["store_type"].str.title().fillna("Unknown")
    df["store_name"] = df["store_name"].str.title()

    # Region Mapping
    # ---------------------------
    REGION_MAP = {
        # Europe
        "United Kingdom": "Europe",
        "Uk": "Europe",
        "France": "Europe",
        "Germany": "Europe",
        "Spain": "Europe",
        "Italy": "Europe",
        "Netherlands": "Europe",
        "Belgium": "Europe",
        "Sweden": "Europe",
        "Norway": "Europe",
        "Denmark": "Europe",

        # North America
        "United States": "North America",
        "Usa": "North America",
        "Canada": "North America",
        "Mexico": "North America",

        # South America
        "Brazil": "South America",
        "Argentina": "South America",
        "Chile": "South America",
        "Colombia": "South America",

        # Asia
        "Japan": "Asia",
        "China": "Asia",
        "India": "Asia",
        "Singapore": "Asia",
        "South Korea": "Asia",

        # Africa
        "Nigeria": "Africa",
        "South Africa": "Africa",
        "Egypt": "Africa",
        "Kenya": "Africa",

        # Oceania
        "Australia": "Oceania",
        "New Zealand": "Oceania"
    }

    df["region"] = df["country"].map(REGION_MAP).fillna("Unknown")

    # Create Surrogate key
    df = df.sort_values("store_id").reset_index(drop=True)
    df["store_key"] = range(1, len(df) + 1)

    # Reorder columns
    df = df[["store_key", "store_id"] +
            [c for c in df.columns if c not in ["store_key", "store_id"]]]

    # --- Feature Engineering: Stores ---

    # Region tier (string)
    df["region_tier"] = df["region"].map({
        "Europe": "High-Value Region",
        "North America": "High-Value Region",
        "Asia": "Medium-Value Region",
        "South America": "Medium-Value Region",
        "Oceania": "Medium-Value Region",
        "Africa": "Low-Value Region",
        "Unknown": "Low-Value Region"
    })

    return df


def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    validate_schema(df, ["order_id", "store_id", "customer_id", "product_id",
                    "quantity", "revenue", "cost", "profit", "order_date", "unit_price"], "sales")

    df = normalize_columns(df)
    df = df.drop_duplicates()
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    df["cost"] = pd.to_numeric(df["cost"], errors="coerce")
    df["profit"] = pd.to_numeric(df["profit"], errors="coerce")
    df["order_date"] = pd.to_datetime(
        df["order_date"].str.strip(), errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    # df["order_date_formatted"] = df["order_date"].dt.strftime("%d %b %Y")

    # --- Feature Engineering: Sales ---

    # Revenue bucket (string)
    df["revenue_bucket"] = pd.cut(
        df["revenue"],
        bins=[0, 10, 50, 200, float("inf")],
        labels=["Low Revenue", "Medium Revenue", "High Revenue", "VIP Revenue"]
    )

    # Profit margin
    df["profit_margin"] = df["profit"] / df["revenue"].replace(0, pd.NA)

    # Profit margin bucket
    df["margin_bucket"] = pd.cut(
        df["profit_margin"],
        bins=[-1, 0.1, 0.3, 0.6, float("inf")],
        labels=["Low Margin", "Medium Margin",
                "High Margin", "Very High Margin"]
    )

    # Outlier flag
    threshold = df["revenue"].quantile(0.99)
    df["outlier_flag"] = df["revenue"].apply(
        lambda r: "Outlier (Top 1%)" if r > threshold else "Normal"
    )

    # Time of day
    df["time_of_day"] = df["order_date"].dt.hour.map(
        lambda h: "Morning" if 5 <= h < 12 else
        "Afternoon" if 12 <= h < 17 else
        "Evening" if 17 <= h < 22 else
        "Night"
    )

    return df


def clean_regions(stores_df: pd.DataFrame) -> pd.DataFrame:

    # Extract unique regions
    regions = stores_df[["country", "region"]].drop_duplicates().copy()

    # --- Feature Engineering: Regions ---

    # Region tier (same logic as stores)
    regions["region_tier"] = regions["region"].map({
        "Europe": "High-Value Region",
        "North America": "High-Value Region",
        "Asia": "Medium-Value Region",
        "South America": "Medium-Value Region",
        "Oceania": "Medium-Value Region",
        "Africa": "Low-Value Region",
        "Unknown": "Low-Value Region"
    })

    # Readable region name
    regions["region_readable"] = regions["region"].str.title()

    # Create Surrogate key
    regions["region_key"] = range(1, len(regions) + 1)

    # Reorder columns
    regions = regions[["region_key", "region",
                       "region_readable", "region_tier", "country"]]

    return regions


# Opens the Zip file, finds all the .csv files inside, reads each one into a pandas DataFrame, and returns a dictionary of DataFrames keyed by filename.


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

# Clean dataset orchestration


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


def run_pipeline():
    create_clean_data_dir()
    clean_dataset()
    logging.info("Cleaning process completed successfully.")


# Main
# --------------------------------------
if __name__ == "__main__":
    run_pipeline()
    logging.info("Cleaning process completed successfully.")
