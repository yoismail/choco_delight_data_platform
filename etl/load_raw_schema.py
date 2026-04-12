
from utils.logging import setup_logging
import pandas as pd
from pathlib import Path
import logging
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import traceback

# Directories
RAW_DATA_DIR = Path("data/raw")

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

    rows, cols = df.shape
    logging.info(
        f"Loading {table_name} into PostgreSQL...\n"
        f"  Rows: {rows}\n"
        f"  Columns: {cols}"
    )

    df.to_sql(table_name, engine, schema="raw_data",
              if_exists="replace", index=False)

    logging.info(f"Loaded {table_name} into PostgreSQL.\n {df.shape}\n")


def run_etl():

    try:
        logging.info("Starting ETL pipeline...")

        load_env()
        db_url = get_db_url()
        engine = create_engine(db_url)

        # load extracted data
        calendar = pd.read_csv(RAW_DATA_DIR / "calendar.csv")
        customers = pd.read_csv(RAW_DATA_DIR / "customers.csv")
        products = pd.read_csv(RAW_DATA_DIR / "products.csv")
        sales = pd.read_csv(RAW_DATA_DIR / "sales.csv")
        stores = pd.read_csv(RAW_DATA_DIR / "stores.csv")

        # Load into PostgreSQL
        load_to_postgres(calendar, "calendar", engine)
        load_to_postgres(customers, "customers", engine)
        load_to_postgres(products, "products", engine)
        load_to_postgres(sales, "sales", engine)
        load_to_postgres(stores, "stores", engine)

        logging.info("ETL pipeline completed successfully!")

    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
        logging.error(traceback.format_exc())


# ----------------------------
# Entry Point
# ----------------------------
if __name__ == "__main__":
    run_etl()
