import subprocess
from pathlib import Path
import zipfile
import pandas as pd
import logging
import sys

# Configure logging
from utils.logging import setup_logging
setup_logging()

# Define the URL of the dataset and the local path to save it

ZIP_FILE_URL = "https://www.kaggle.com/api/v1/datasets/download/ssssws/chocolate-sales-dataset-2023-2024"
RAW_DATA_DIR = Path("data/raw")
ZIP_FILE_PATH = RAW_DATA_DIR / "chocolate-sales-dataset-2023-2024.zip"


# Create the raw data directory if it doesn't exist


def create_data_dir():
    if not RAW_DATA_DIR.exists():
        RAW_DATA_DIR.mkdir()
        logging.info("Data directory created.")
    else:
        logging.info("Data directory already exists.")


# Download the dataset using Kaggle API
def download_dataset():
    logging.info("Starting dataset download...")

    try:
        subprocess.run([
            "kaggle", "datasets", "download",
            "-d", "ssssws/chocolate-sales-dataset-2023-2024",
            "-p", str(RAW_DATA_DIR),
            "--force"
        ], check=True)

        logging.info(f"Downloaded to: {RAW_DATA_DIR}")

    except subprocess.CalledProcessError:
        logging.error("Download failed. Check Kaggle API setup.")
        sys.exit(1)


def extract_dataset():
    logging.info("Extracting dataset...")

    if not ZIP_FILE_PATH.exists():
        logging.error("Zip file not found. Extraction aborted.")
        sys.exit(1)

    try:
        with zipfile.ZipFile(ZIP_FILE_PATH, 'r') as zip_ref:
            zip_ref.extractall(RAW_DATA_DIR)

        logging.info("Extraction completed.")

    except zipfile.BadZipFile:
        logging.error("Invalid zip file.")
        sys.exit(1)


def explore_data():
    logging.info("Exploring extracted files...")

    try:
        customers = pd.read_csv(RAW_DATA_DIR / "customers.csv")
        products = pd.read_csv(RAW_DATA_DIR / "products.csv")
        sales = pd.read_csv(RAW_DATA_DIR / "sales.csv")
        stores = pd.read_csv(RAW_DATA_DIR / "stores.csv")
        calendar = pd.read_csv(RAW_DATA_DIR / "calendar.csv")

        logging.info(f"\nCustomers shape: {customers.shape}")
        logging.info(f"Customers head:\n{customers.head()}\n")
        logging.info(f"Customers datatypes:\n{customers.dtypes}\n")
        logging.info(
            f"Missing values in customers:\n{customers.isnull().sum()}\n")
        logging.info(
            f"duplicate customer IDs: {customers['customer_id'].duplicated().sum()}\n")

        logging.info(f"Products shape: {products.shape}")
        logging.info(f"Products head:\n{products.head()}\n")
        logging.info(f"Products datatypes:\n{products.dtypes}\n")
        logging.info(
            f"Missing values in products:\n{products.isnull().sum()}\n")
        logging.info(
            f"duplicate product IDs: {products['product_id'].duplicated().sum()}\n")

        logging.info(f"Sales shape: {sales.shape}")
        logging.info(f"Sales head:\n{sales.head()}\n")
        logging.info(f"Sales datatypes:\n{sales.dtypes}\n")
        logging.info(f"Missing values in sales:\n{sales.isnull().sum()}\n")

        logging.info(f"Stores shape: {stores.shape}")
        logging.info(f"Stores head:\n{stores.head()}\n")
        logging.info(f"Stores datatypes:\n{stores.dtypes}\n")
        logging.info(f"Missing values in stores:\n{stores.isnull().sum()}\n")
        logging.info(
            f"duplicate store IDs: {stores['store_id'].duplicated().sum()}\n")

        logging.info(f"Calendar shape: {calendar.shape}")
        logging.info(f"Calendar head:\n{calendar.head()}\n")
        logging.info(f"Calendar datatypes:\n{calendar.dtypes}\n")
        logging.info(
            f"Missing values in calendar:\n{calendar.isnull().sum()}\n")

    except FileNotFoundError as e:
        logging.error(f"Missing file: {e}")
        sys.exit(1)

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        sys.exit(1)


# Running Pipeline
def run_pipeline():
    logging.info("Running Pipeline: Download, Extract, and Explore Dataset")

    create_data_dir()
    download_dataset()
    extract_dataset()
    explore_data()

    logging.info(
        "Download, Extraction, and Exploration Completed Successfully")


# Main execution
if __name__ == "__main__":
    run_pipeline()
