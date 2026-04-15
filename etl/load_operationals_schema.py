
from utils.logging import setup_logging
import pandas as pd
from pathlib import Path
import logging
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import traceback

# Directories
CLEAN_DATA_DIR = Path("data/clean")

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

    df.to_sql(table_name, engine, schema="operationals",
              if_exists="replace", index=False)

    logging.info(
        f"Loaded {table_name} into PostgreSQL -- cleaned rows: {df.shape[0]}, columns: {df.shape[1]}\n")


# Main ETL function
def run_etl():

    try:
        logging.info("Starting ETL pipeline...")

        load_env()
        db_url = get_db_url()
        engine = create_engine(db_url)

        # load cleaned data
        calendar = pd.read_csv(CLEAN_DATA_DIR / "cleaned_calendar.csv")
        customers = pd.read_csv(CLEAN_DATA_DIR / "cleaned_customers.csv")
        products = pd.read_csv(CLEAN_DATA_DIR / "cleaned_products.csv")
        sales = pd.read_csv(CLEAN_DATA_DIR / "cleaned_sales.csv")
        stores = pd.read_csv(CLEAN_DATA_DIR / "cleaned_stores.csv")

        # Load into PostgreSQL
        load_to_postgres(calendar, "cleaned_calendar", engine)
        load_to_postgres(customers, "cleaned_customers", engine)
        load_to_postgres(products, "cleaned_products", engine)
        load_to_postgres(sales, "cleaned_sales", engine)
        load_to_postgres(stores, "cleaned_stores", engine)

        logging.info("ETL pipeline completed successfully!")

    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
        logging.error(traceback.format_exc())


# ----------------------------
# Entry Point
# ----------------------------
if __name__ == "__main__":
    run_etl()
