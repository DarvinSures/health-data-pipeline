'''
   1. Load environment variables from .env file and Lint.
   2. Ingest data from Google Sheet to Snowflake raw layer.
   3. Run dbt models to transform data from raw → uat → consumption layers
    4. Run dbt tests to validate data quality and FHIR alignment.

'''

import subprocess
import sys
from pathlib import Path
from prefect import flow, task
from prefect.logging import get_run_logger
from dotenv import load_dotenv

load_dotenv()

ROOT_DIR = Path(__file__).parent.parent
DBT_DIR = ROOT_DIR / "dbt_project" / "health_pipeline"
DBT_EXECUTABLE = r"C:\Users\Darvin\miniconda3\envs\health-pipeline\Scripts\dbt.EXE"
SQLFLUFF_EXECUTABLE = r"C:\Users\Darvin\miniconda3\envs\health-pipeline\Scripts\sqlfluff.EXE"


@task(name="load_env_vars")
def load_env_vars():
    """Load environment variables from .env file."""
    logger = get_run_logger()
    env_path = ROOT_DIR / ".env"

    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                import os
                os.environ[key.strip()] = value.strip()

    logger.info("Environment variables loaded")


@task(name="lint_sql", retries=0)
def lint_sql():
    """Run SQLFluff linting on dbt models."""
    logger = get_run_logger()
    logger.info("Running SQLFluff linting...")

    result = subprocess.run(
        [SQLFLUFF_EXECUTABLE, "lint", "models/", "--dialect", "snowflake"],
        capture_output=True,
        text=True,
        cwd=DBT_DIR
    )

    logger.info(result.stdout)

    if result.returncode != 0:
        logger.warning("Linting issues found — review before promoting to prod")
        logger.warning(result.stdout)
    else:
        logger.info("All SQL files passed linting")


@task(name="ingest_data", retries=2, retry_delay_seconds=30)
def ingest_data():
    """Load data from Google Sheet to Snowflake raw layer."""
    logger = get_run_logger()
    logger.info("Starting data ingestion...")

    result = subprocess.run(
        [sys.executable, str(ROOT_DIR / "ingestion" / "load_data.py"), "dev"],
        capture_output=True,
        text=True,
        cwd=ROOT_DIR
    )

    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception(f"Ingestion failed: {result.stderr}")

    logger.info("Ingestion completed successfully")


@task(name="run_dbt", retries=1, retry_delay_seconds=10)
def run_dbt():
    """Run dbt models: raw → uat → consumption."""
    logger = get_run_logger()
    logger.info("Running dbt models...")

    result = subprocess.run(
        [DBT_EXECUTABLE, "run", "--target", "dev"],
        capture_output=True,
        text=True,
        cwd=DBT_DIR
    )

    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception(f"dbt run failed: {result.stderr}")

    logger.info("dbt models completed successfully")


@task(name="run_dbt_tests", retries=1, retry_delay_seconds=10)
def run_dbt_tests():
    """Run FHIR-aligned dbt data quality tests."""
    logger = get_run_logger()
    logger.info("Running dbt tests...")

    result = subprocess.run(
        [DBT_EXECUTABLE, "test", "--target", "dev"],
        capture_output=True,
        text=True,
        cwd=DBT_DIR
    )

    logger.info(result.stdout)

    if result.returncode != 0:
        logger.warning("Some dbt tests failed — check results")

    logger.info("dbt tests completed")


@flow(name="health-data-pipeline", log_prints=True)
def health_pipeline():
    """
    Main pipeline flow:
    1. Load env vars
    2. Lint SQL
    3. Ingest data
    4. Run dbt models
    5. Run dbt tests
    """
    load_env_vars()
    lint_sql()
    ingest_data()
    run_dbt()
    run_dbt_tests()


if __name__ == "__main__":
    health_pipeline()