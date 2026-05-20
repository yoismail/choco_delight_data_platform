import pandas as pd
import logging
from pathlib import Path
import sys


# Configure logging
from utils.logging import section, setup_logging, timed
setup_logging()

# Path to the raw data directory and expected files
RAW_DATA_DIR = Path("data/raw")

# Function to explore the extracted data


def explore_data():
    section("Exploring Extracted Data")

    # Define the files to explore
    files_to_explore = {
        "customers": "customers.csv",
        "products": "products.csv",
        "sales": "sales.csv",
        "stores": "stores.csv",
        "calendar": "calendar.csv"
    }

    try:
        # Create a dictionary to hold the dataframes
        dataframes = {}

        # Loop through the files, load them into dataframes, and perform basic exploration
        for name, filename in files_to_explore.items():
            file_path = RAW_DATA_DIR / filename
            if not file_path.exists():
                logging.error(f"Missing file: {file_path}")
                sys.exit(1)
            dataframes[name] = pd.read_csv(file_path)
            logging.info(f"Loaded {filename}")

        # Explore each dataframe
        for name, df in dataframes.items():
            logging.info(f"\n--- Exploring {name.capitalize()} ---")
            logging.info(f"{name.capitalize()} shape: {df.shape}")
            logging.info(f"{name.capitalize()} head:\n{df.head()}\n")
            logging.info(f"{name.capitalize()} datatypes:\n{df.dtypes}\n")
            logging.info(f"Missing values in {name}:\n{df.isnull().sum()}\n")
            logging.info(
                f"Duplicate rows in {name}: {df.duplicated().sum()}\n")

        return dataframes  # Return the loaded dataframes for further use if needed

    except FileNotFoundError as e:

        logging.error(f"A file was not found during exploration: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error during data exploration: {e}")
        sys.exit(1)


@timed
def main():
    explore_data()
    logging.info("🎉 Data Exploration Completed Successfully")


if __name__ == "__main__":
    main()
