import pandas as pd
import logging
import sys
from pathlib import Path
from typing import List, Dict

# Logging & Utils
from utils.logging import setup_logging, section, timed
setup_logging()

# Directories
RAW_DATA_DIR = Path("data/raw")
CLEAN_DATA_DIR = Path("data/clean")

# Centralized Business Logic Mappings
REGION_MAP = {
    "United Kingdom": "Europe", "Uk": "Europe", "France": "Europe",
    "Germany": "Europe", "Spain": "Europe", "Italy": "Europe",
    "United States": "North America", "Usa": "North America", "Canada": "North America",
    "Japan": "Asia", "China": "Asia", "India": "Asia",
    "Australia": "Oceania", "New Zealand": "Oceania",
    "Brazil": "South America", "Argentina": "South America", "Chile": "South America",
    "Nigeria": "Africa", "South Africa": "Africa", "Egypt": "Africa"
}

REGION_TIER_MAP = {
    "Europe": "High-Value Region",
    "North America": "High-Value Region",
    "Asia": "Medium-Value Region",
    "South America": "Medium-Value Region",
    "Oceania": "Medium-Value Region",
    "Africa": "Low-Value Region",
    "Unknown": "Low-Value Region"
}

SEASON_MAP = {
    12: "Winter", 1: "Winter", 2: "Winter",
    3: "Spring", 4: "Spring", 5: "Spring",
    6: "Summer", 7: "Summer", 8: "Summer",
    9: "Autumn", 10: "Autumn", 11: "Autumn"
}

PREMIUM_BRANDS = ["Lindt", "Godiva", "Green & Black"]


# Helper functions for loading, cleaning, and validating data
def create_clean_data_dir() -> None:
    CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)
    logging.info("Clean data directory ready.")


def load_raw_data(file_name: str) -> pd.DataFrame:
    file_path = RAW_DATA_DIR / file_name
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path.resolve()}")
    df = pd.read_csv(file_path)
    logging.info(f"Loaded {file_name} with shape {df.shape}")
    return df


def log_df_sanity(df: pd.DataFrame, name: str) -> None:
    section(f"Sanity Check: {name}")
    logging.info(
        f"[{name}] shape: {df.shape} | duplicates: {df.duplicated().sum()}")
    logging.info(f"[{name}] missing values:\n{df.isnull().sum()}\n")
    logging.info(f"[{name}] dtypes:\n{df.dtypes}\n")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    return df


def validate_schema(df: pd.DataFrame, required_cols: List[str], name: str) -> None:
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logging.error(f"{name}: Missing required columns: {missing}")
        sys.exit(1)
    logging.info(f"{name}: Schema validation passed.")

# Cleaning functions for each dataset


def clean_calendar() -> pd.DataFrame:
    section("Cleaning Calendar Data")
    df = load_raw_data("calendar.csv").copy(
    ).drop_duplicates().reset_index(drop=True)

    required_cols = [
        "date",
        "year",
        "month",
        "week",
        "day_of_week",
        "day"
    ]
    validate_schema(df, required_cols, "calendar")

    df = normalize_columns(df)
    df = df.rename(columns={"date": "calendar_date"})
    df["calendar_date"] = pd.to_datetime(df["calendar_date"], errors="coerce")

    # Numeric Coercion
    numeric_cols = ["year", "month", "week", "day_of_week", "day"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(
            df[col], errors="coerce").fillna(-1).astype(int)

    # Feature Engineering
    df = df.sort_values("calendar_date").reset_index(drop=True)

    # Surrogate key
    df["calendar_key"] = df.index + 1

    # Season, quarter, and day type logic
    df["quarter"] = df["month"].apply(
        lambda m: f"Q{((m-1)//3)+1}" if m > 0 else "Unknown")
    df["season"] = df["month"].map(SEASON_MAP).fillna("Unknown")
    df["day_type"] = df["day_of_week"].apply(
        lambda d: "Weekend" if d in [6, 7] else "Weekday")
    df["calendar_date_formatted"] = df["calendar_date"].dt.strftime("%d %b %Y")

    logging.info(
        f"Calendar cleaning completed with new features: quarter, season, day_type, and formatted date.")
    logging.info(f"Cleaned calendar.csv with shape {df.shape}")
    return df[[
        "calendar_key",
        "calendar_date",
        "year",
        "month",
        "week",
        "day_of_week",
        "quarter",
        "season",
        "day_type",
        "calendar_date_formatted"]]


def clean_customers() -> pd.DataFrame:
    section("Cleaning Customers Data")
    df = load_raw_data("customers.csv").copy(
    ).drop_duplicates().reset_index(drop=True)

    required_cols = [
        "customer_id",
        "gender",
        "join_date"
    ]
    validate_schema(df, required_cols, "customers")

    df = normalize_columns(df)
    df["gender"] = df["gender"].str.title().fillna("Unknown")
    df["join_date"] = pd.to_datetime(df["join_date"], errors="coerce")

    # Feature Engineering - Accurate Tenure Calculation
    today = pd.Timestamp.today()

    # Exact full years
    df["tenure_years_exact"] = df["join_date"].apply(
        lambda x: (today.year - x.year) -
        ((today.month, today.day) < (x.month, x.day))
        if pd.notna(x) else 0
    )

    # Exact full months total
    df["tenure_months_exact"] = df["join_date"].apply(
        lambda x: (today.month - x.month) + 12 *
        (today.year - x.year) - (today.day < x.day)
        if pd.notna(x) else 0
    )

    # Readable labels (whole integers only)
    df["tenure_years"] = df["tenure_years_exact"].astype(str) + " years"
    df["tenure_months"] = df["tenure_months_exact"].astype(str) + " months"

    # Total days (still useful)
    df["tenure_days"] = (today - df["join_date"]).dt.days.fillna(0).astype(int)

    # Customer Segment based on ACCURATE months
    df["customer_segment"] = pd.cut(
        df["tenure_months_exact"],
        bins=[-1, 6, 12, 24, float("inf")],
        labels=["New", "Active", "Loyal", "VIP"]
    )

    # Surrogate key
    df = df.sort_values("customer_id").reset_index(drop=True)
    df["customer_key"] = df.index + 1

    logging.info(f"Customer cleaning completed with new features: tenure_years_exact, tenure_months_exact, tenure_years, tenure_months, tenure_days, and customer_segment.")
    logging.info(f"Cleaned customers.csv with shape {df.shape}")

    return df[[
        "customer_key",
        "customer_id",
        "age",
        "gender",
        "loyalty_member",
        "join_date",
        "tenure_days",
        "tenure_months",
        "tenure_years",
        "customer_segment"]]


def clean_products() -> pd.DataFrame:
    section("Cleaning Products Data")
    df = load_raw_data("products.csv").copy(
    ).drop_duplicates().reset_index(drop=True)

    required_cols = [
        "product_id",
        "product_name",
        "brand",
        "category"
    ]
    validate_schema(df, required_cols, "products")

    df = normalize_columns(df)
    df["product_name"] = df["product_name"].str.title().fillna("Unknown")
    df["brand"] = df["brand"].str.title().fillna("Unknown")
    df["category"] = df.get("category", "Unknown").fillna("Unknown")

    # Add missing products
    new_data = [
        {"product_id": "P0000", "product_name": "Unknown Product 0000",
            "brand": "Unknown", "category": "Unknown", "cocoa_percent": 0.0, "weight_g": 0.0},
        {"product_id": "P0201", "product_name": "Unknown Product 0201",
            "brand": "Unknown", "category": "Unknown", "cocoa_percent": 0.0, "weight_g": 0.0}
    ]
    df = pd.concat([df, pd.DataFrame(new_data)], ignore_index=True)

    # Surrogate key
    df = df.sort_values("product_id").reset_index(drop=True)
    df["product_key"] = df.index + 1

    # Brand tier
    df["brand_tier"] = df["brand"].apply(
        lambda b: "Premium Brand" if b in PREMIUM_BRANDS else "Standard Brand")

    logging.info(f"Product cleaning completed with new features: brand_tier.")
    logging.info(f"Cleaned products.csv with shape {df.shape}")

    return df[["product_key",
               "product_id",
               "product_name",
               "brand",
               "category",
               "cocoa_percent",
               "weight_g",
               "brand_tier"]]


def clean_stores() -> pd.DataFrame:
    section("Cleaning Stores Data")
    df = load_raw_data("stores.csv").copy(
    ).drop_duplicates().reset_index(drop=True)

    expected_cols = [
        "store_id",
        "store_name",
        "country",
        "city",
        "store_type"
    ]
    validate_schema(df, expected_cols, "stores")

    df = normalize_columns(df)
    df["store_name"] = df["store_name"].str.title().fillna("Unknown")
    df["country"] = df["country"].str.title().fillna("Unknown")
    df["city"] = df["city"].str.title().fillna("Unknown")
    df["store_type"] = df["store_type"].str.title().fillna("Unknown")

    # Region logic (uses centralized map)
    df["region"] = df["country"].map(REGION_MAP).fillna("Unknown")
    df["region_tier"] = df["region"].map(REGION_TIER_MAP)

    # Surrogate key
    df = df.sort_values("store_id").reset_index(drop=True)
    df["store_key"] = df.index + 1

    logging.info(
        f"Store cleaning completed with new features: region, region_tier.")
    logging.info(f"Cleaned stores.csv with shape {df.shape}")

    return df[["store_key",
               "store_id",
               "store_name",
               "country",
               "city",
               "store_type",
               "region",
               "region_tier"]]


def clean_sales() -> pd.DataFrame:
    section("Cleaning Sales Data")
    df = load_raw_data("sales.csv").copy(
    ).drop_duplicates().reset_index(drop=True)

    required_cols = [
        "order_id",
        "store_id",
        "customer_id",
        "product_id",
        "quantity",
        "revenue",
        "cost",
        "profit",
        "order_date",
        "unit_price"
    ]
    validate_schema(df, required_cols, "sales")

    df = normalize_columns(df)

    # Numeric cleaning
    numeric_cols = ["quantity", "revenue", "cost", "profit", "unit_price"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Date cleaning
    df["order_date"] = pd.to_datetime(
        df["order_date"].str.strip(), errors="coerce")

    # Profit margin
    df["profit_margin"] = df["profit"] / df["revenue"].replace(0, pd.NA)

    # Feature Engineering
    df["revenue_bucket"] = pd.cut(
        df["revenue"],
        bins=[0, 10, 50, 200, float("inf")],
        labels=["Low Revenue", "Medium Revenue", "High Revenue", "VIP Revenue"]
    )

    df["margin_bucket"] = pd.cut(
        df["profit_margin"],
        bins=[-1, 0.1, 0.3, 0.6, float("inf")],
        labels=["Low Margin", "Medium Margin",
                "High Margin", "Very High Margin"]
    )

    # Outlier flag (Top 1%)
    threshold = df["revenue"].quantile(0.99)
    df["outlier_flag"] = df["revenue"].apply(
        lambda r: "Outlier (Top 1%)" if r > threshold else "Normal")

    # Time of day
    df["time_of_day"] = df["order_date"].dt.hour.map(
        lambda h: "Morning" if 5 <= h < 12 else
                  "Afternoon" if 12 <= h < 17 else
                  "Evening" if 17 <= h < 22 else "Night"
    )

    logging.info(
        f"Sales cleaning completed with new features: profit_margin, revenue_bucket, margin_bucket, outlier_flag, and time_of_day.")
    logging.info(f"Cleaned sales.csv with shape {df.shape}")

    return df[[
        "order_id",
        "order_date",
        "product_id",
        "store_id",
        "customer_id",
        "quantity",
        "unit_price",
        "discount",
        "revenue",
        "cost",
        "profit",
        "revenue_bucket",
        "profit_margin",
        "margin_bucket",
        "outlier_flag",
        "time_of_day"]]


def clean_regions(stores_df: pd.DataFrame) -> pd.DataFrame:
    section("Cleaning Regions Data")
    regions = stores_df[["country", "region"]
                        ].drop_duplicates().copy().reset_index(drop=True)

    required_cols = ["country", "region"]
    validate_schema(regions, required_cols, "regions")

    # Reuse centralized logic
    regions["region_tier"] = regions["region"].map(REGION_TIER_MAP)

    # Surrogate key
    regions["region_key"] = regions.index + 1

    logging.info(
        f"Regions cleaning completed with new features: region_tier, region_readable, and region_key.")
    logging.info(f"Cleaned regions.csv with shape {regions.shape}")

    return regions[["region_key",
                    "region",
                    "region_tier",
                    "country"]]


#  Orchestrator function to run all cleaning steps and save cleaned datasets

def save_dataset() -> None:
    section("Cleaning Dataset and Saving")

    clean_cal = clean_calendar()
    clean_cust = clean_customers()
    clean_prod = clean_products()
    clean_stor = clean_stores()
    clean_sale = clean_sales()
    clean_reg = clean_regions(clean_stor)

    # Save all
    clean_cal.to_csv(CLEAN_DATA_DIR / "cleaned_calendar.csv", index=False)
    clean_cust.to_csv(CLEAN_DATA_DIR / "cleaned_customers.csv", index=False)
    clean_prod.to_csv(CLEAN_DATA_DIR / "cleaned_products.csv", index=False)
    clean_stor.to_csv(CLEAN_DATA_DIR / "cleaned_stores.csv", index=False)
    clean_sale.to_csv(CLEAN_DATA_DIR / "cleaned_sales.csv", index=False)
    clean_reg.to_csv(CLEAN_DATA_DIR / "cleaned_regions.csv", index=False)

    # Sanity checks
    log_df_sanity(clean_cal, "cleaned_calendar")
    log_df_sanity(clean_cust, "cleaned_customers")
    log_df_sanity(clean_prod, "cleaned_products")
    log_df_sanity(clean_stor, "cleaned_stores")
    log_df_sanity(clean_sale, "cleaned_sales")
    log_df_sanity(clean_reg, "cleaned_regions")


@timed
def run_pipeline() -> None:
    create_clean_data_dir()
    save_dataset()


if __name__ == "__main__":
    run_pipeline()
    logging.info(
        "✅ Cleaning process completed successfully - Cleaned datasets saved to 'data/clean' directory.")
