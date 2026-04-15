import logging
import subprocess
import sys
from pathlib import Path

from utils.logging import setup_logging
setup_logging()

PYTHON = sys.executable


def run_step(title: str, command: str):
    logging.info("\n" + "=" * 70)
    logging.info(f"🔷 {title}")
    logging.info("=" * 70 + "\n")

    result = subprocess.run(command, shell=True)

    if result.returncode != 0:
        logging.error(f"❌ {title} FAILED")
        sys.exit(result.returncode)

    logging.info(f"✔️  {title} completed successfully\n")


def main():
    logging.info("\n🚀 Starting Full Pipeline Run\n")

    run_step("WIPING DATA", f"{PYTHON} -m etl.wipe_all all")
    run_step("LOADING RAW DATA", f"{PYTHON} -m etl.extract")
    run_step("RUNNING CLEAN PIPELINE", f"{PYTHON} -m etl.clean")
    run_step("RUNNING TRANSFORM PIPELINE", f"{PYTHON} -m etl.transform")
    run_step("LOADING RAW SCHEMA", f"{PYTHON} -m etl.load_raw_schema")
    run_step("RUNNING OPERATIONALS SCHEMA LOADER",
             f"{PYTHON} -m etl.load_operationals_schema")
    run_step("RUNNING ANALYTICS SCHEMA LOADER",
             f"{PYTHON} -m etl.load_analytics_schema")

    logging.info("🎉 FULL PIPELINE COMPLETED SUCCESSFULLY!\n")


if __name__ == "__main__":
    main()
