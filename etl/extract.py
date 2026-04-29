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

    # Define the files to explore
    files_to_explore = {
        "customers": "customers.csv",
        "products": "products.csv",
        "sales": "sales.csv",
        "stores": "stores.csv",
        "calendar": "calendar.csv"
    }

    try:
        # Load all dataframes first
        dataframes = {}
        for name, filename in files_to_explore.items():
            file_path = RAW_DATA_DIR / filename
            if not file_path.exists():
                logging.error(f"Missing file: {file_path}")
                sys.exit(1)
            dataframes[name] = pd.read_csv(file_path)
            logging.info(f"Loaded {filename}")

        # Now explore each dataframe
        for name, df in dataframes.items():
            logging.info(f"\n--- Exploring {name.capitalize()} ---")
            logging.info(f"{name.capitalize()} shape: {df.shape}")
            logging.info(f"{name.capitalize()} head:\n{df.head()}\n")
            logging.info(f"{name.capitalize()} datatypes:\n{df.dtypes}\n")
            logging.info(f"Missing values in {name}:\n{df.isnull().sum()}\n")

    except FileNotFoundError as e:

        logging.error(f"A file was not found during exploration: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error during data exploration: {e}")
        sys.exit(1)

# Running main function to execute the pipeline


def main():
    logging.info("Running Pipeline: Download, Extract, and Explore Dataset")

    create_data_dir()
    download_dataset()
    extract_dataset()
    explore_data()

    logging.info(
        "Download, Extraction, and Exploration Completed Successfully")


# Main execution
if __name__ == "__main__":
    main()
