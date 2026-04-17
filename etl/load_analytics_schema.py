from utils.logging import setup_logging
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


def run_pipeline():
    try:
        logging.info("Starting ETL pipeline...")

        load_env()
        db_url = get_db_url()
        engine = create_engine(db_url)

        # 1. Drop fact table FIRST
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS analytics.fact_sales;"))
            logging.info("Dropped fact_sales table to avoid FK conflicts.")

        # 2. Recreate schema BEFORE loading any data
        schema_path = Path("sql/analytics_schema.sql")
        with open(schema_path, "r") as f:
            schema_sql = f.read()

        with engine.connect() as conn:
            conn.execute(text(schema_sql))
            logging.info("Analytics schema recreated (including fact_sales).")

        # 3. Load transformed dimension data AFTER schema creation
        calendar = pd.read_csv(TRANSFORM_DIR / "dim_calendar.csv")
        customers = pd.read_csv(TRANSFORM_DIR / "dim_customers.csv")
        products = pd.read_csv(TRANSFORM_DIR / "dim_products.csv")
        stores = pd.read_csv(TRANSFORM_DIR / "dim_stores.csv")

        load_to_postgres(calendar, "dim_calendar", engine)
        load_to_postgres(customers, "dim_customers", engine)
        load_to_postgres(products, "dim_products", engine)
        load_to_postgres(stores, "dim_stores", engine)

        # 4. Load fact data LAST
        sales = pd.read_csv(TRANSFORM_DIR / "fact_sales.csv")
        load_to_postgres(sales, "fact_sales", engine)

        logging.info("Analytics pipeline completed successfully.")

    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    run_pipeline()
