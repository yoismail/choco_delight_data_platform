import subprocess
from pathlib import Path
import zipfile
import logging
import sys

# Configure logging
from utils.logging import section, setup_logging, timed
setup_logging()


# Define the URL of the dataset and the local path to save it
ZIP_FILE_URL = "https://www.kaggle.com/api/v1/datasets/download/ssssws/chocolate-sales-dataset-2023-2024"
RAW_DATA_DIR = Path("data/raw")
ZIP_FILE_PATH = RAW_DATA_DIR / "chocolate-sales-dataset-2023-2024.zip"


# Create the raw data directory if it doesn't exist
def create_data_dir():
    if not RAW_DATA_DIR.exists():
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        logging.info("Data directory created.")
    else:
        logging.info("Data directory already exists.")


# Download the dataset using Kaggle API
def download_dataset():
    section("Downloading Dataset")
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
    section("Extracting Dataset")
    logging.info("Extracting dataset...")

    if not ZIP_FILE_PATH.exists():
        logging.error("Zip file not found. Extraction aborted.")
        sys.exit(1)

    try:
        with zipfile.ZipFile(ZIP_FILE_PATH, 'r') as zip_ref:
            zip_ref.extractall(RAW_DATA_DIR)

        logging.info(f"Extraction completed and saved to: {RAW_DATA_DIR}")

    except zipfile.BadZipFile:
        logging.error("Invalid zip file.")
        sys.exit(1)


# Running main function to execute the pipeline

@timed
def main():
    logging.info("Running Pipeline: Download and Extract Dataset")

    create_data_dir()
    download_dataset()
    extract_dataset()

    logging.info(
        "=============== EXTRACTION STAGE COMPLETED SUCCESSFULLY ================")


# Main execution
if __name__ == "__main__":
    main()
