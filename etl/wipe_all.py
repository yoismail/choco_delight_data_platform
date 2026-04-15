from utils.logging import setup_logging
import shutil
import logging
import os
from pathlib import Path
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Path setup to ensure we can import utils
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import from utils
setup_logging()


RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
CLEAN_DATA_DIR = PROJECT_ROOT / "data" / "clean"
TRANSFORM_DATA_DIR = PROJECT_ROOT / "data" / "transform"


def delete_folder(path: Path):
    if path.exists():
        shutil.rmtree(path)
        logging.info(f"🗑️ Deleted folder: {path}")
    else:
        logging.info(f"⚠️ Folder not found (skipped): {path}")


def drop_raw_data_schema():
    load_dotenv()
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL not found in .env")

    engine = create_engine(db_url)

    logging.info("Dropping raw data schema...")
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS raw_data CASCADE;"))
        conn.execute(text("CREATE SCHEMA raw_data;"))
    logging.info("🧹 Raw data schema reset successfully.")


def drop_operationals_schema():
    load_dotenv()
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL not found in .env")

    engine = create_engine(db_url)

    logging.info("Dropping operationals schema...")
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS operationals CASCADE;"))
        conn.execute(text("CREATE SCHEMA operationals;"))
    logging.info("🧹 Operationals schema reset successfully.")


def drop_analytics_schema():
    load_dotenv()
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL not found in .env")

    engine = create_engine(db_url)

    logging.info("Dropping analytics schema...")
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analytics CASCADE;"))
        conn.execute(text("CREATE SCHEMA analytics;"))
    logging.info("🧹 Analytics schema reset successfully.")


def wipe(mode: str):
    mode = mode.lower()

    if mode in ("clean", "all"):
        delete_folder(CLEAN_DATA_DIR)

    if mode in ("raw", "all"):
        delete_folder(RAW_DATA_DIR)

    if mode in ("transform", "all"):
        delete_folder(TRANSFORM_DATA_DIR)

    if mode in ("raw", "all"):
        drop_raw_data_schema()

    if mode in ("operationals", "all"):
        drop_operationals_schema()

    if mode in ("analytics", "all"):
        drop_analytics_schema()

    logging.info(f"✨ Wipe completed for mode: {mode}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Wipe ETL data folders including operationals and/or analytics schema.")
    parser.add_argument(
        "mode",
        choices=["raw", "clean", "transform",
                 "analytics", "operationals", "all"],
        help="Choose what to wipe."
    )

    args = parser.parse_args()
    wipe(args.mode)
