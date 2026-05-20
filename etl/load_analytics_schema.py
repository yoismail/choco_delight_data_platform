import sys

from utils.logging import section, setup_logging, timed
import pandas as pd
from pathlib import Path
import logging
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
import traceback

# Directories
TRANSFORM_DIR = Path("data/transform")

# Set up logging
setup_logging()

# Environment variable + database connection


def load_env():
    load_dotenv()
    logging.info("Environment variables loaded.")


def get_db_url():
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL not found in .env file")
    logging.info("Database URL retrieved.")
    return db_url

# Load to PostgreSQL


def load_to_postgres(df, table_name, engine):
    logging.info(f"Loading {table_name} into PostgreSQL...")
    df.to_sql(
        table_name,
        engine,
        schema="analytics",
        if_exists="append",
        index=False,
        chunksize=20000
    )
    logging.info(
        f"Loaded {table_name} into PostgreSQL — rows: {df.shape[0]}, cols: {df.shape[1]}")

# Main ETL function


@timed
def run_pipeline():
    try:
        section("Starting Analytics ETL Pipeline")
        logging.info("Initializing Environment and Database Connection...")
        load_env()
        db_url = get_db_url()
        engine = create_engine(db_url)

        # Drop fact table FIRST
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS analytics.fact_sales;"))
            logging.info("Dropped fact_sales table to avoid FK conflicts.")

        # Recreate schema BEFORE loading any data
        schema_path = Path("sql/analytics_schema.sql")
        with open(schema_path, "r") as f:
            schema_sql = f.read()

        with engine.connect() as conn:
            conn.execute(text(schema_sql))
            logging.info("Analytics schema recreated (including fact_sales).")

        # 1. Load REGIONS FIRST — because stores reference it!
        section(
            "Loading Region Dimension Table (analytics.dim_regions) into PostgreSQL")
        logging.info("📥 Loading dim_regions...")
        df_regions = pd.read_csv(TRANSFORM_DIR / "dim_regions.csv")
        load_to_postgres(df_regions, "dim_regions", engine)

        # 2. Calendar (independent)
        section(
            "Loading Calendar Dimension Table (analytics.dim_calendar) into PostgreSQL")
        logging.info("📥 Loading dim_calendar...")
        df_calendar = pd.read_csv(TRANSFORM_DIR / "dim_calendar.csv")
        load_to_postgres(df_calendar, "dim_calendar", engine)

        # 3. Customers (independent)
        section(
            "Loading Customer Dimension Table (analytics.dim_customers) into PostgreSQL")
        logging.info("📥 Loading dim_customers...")
        df_customers = pd.read_csv(TRANSFORM_DIR / "dim_customers.csv")
        load_to_postgres(df_customers, "dim_customers", engine)

        # 4. Products (independent)
        section(
            "Loading Product Dimension Table (analytics.dim_products) into PostgreSQL")
        logging.info("📥 Loading dim_products...")
        df_products = pd.read_csv(TRANSFORM_DIR / "dim_products.csv")
        load_to_postgres(df_products, "dim_products", engine)

        # 5. Stores — NOW SAFE, because dim_regions already exists
        section("Loading Store Dimension Table (analytics.dim_stores) into PostgreSQL")
        logging.info("📥 Loading dim_stores...")
        df_stores = pd.read_csv(TRANSFORM_DIR / "dim_stores.csv")
        load_to_postgres(df_stores, "dim_stores", engine)

        # 6. Fact Sales — last, depends on all dimensions
        section("Loading Fact Sales Table (analytics.fact_sales) into PostgreSQL")
        logging.info("📥 Loading fact_sales...")
        df_fact = pd.read_csv(TRANSFORM_DIR / "fact_sales.csv")
        load_to_postgres(df_fact, "fact_sales", engine)

        logging.info(
            "🎉✅ ETL pipeline completed successfully — ALL TABLES LOADED!")

    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
        logging.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
