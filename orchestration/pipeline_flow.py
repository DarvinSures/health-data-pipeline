import subprocess
import sys
from pathlib import Path
from prefect import flow, task
from prefect.logging import get_run_logger

# Install dependencies before anything else
subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", 
    str(Path(__file__).parent.parent / "requirements.txt")])
# Paths
ROOT_DIR = Path(__file__).parent.parent
DBT_DIR = ROOT_DIR / "dbt_project" / "health_pipeline"
INGESTION_SCRIPT = ROOT_DIR / "ingestion" / "load_data.py"
MONITORING_SCRIPT = ROOT_DIR / "monitoring" / "test_report.py"


@task(name="ingest_data", retries=2, retry_delay_seconds=30)
def ingest_data():
    """Load data from Google Sheet → landing → raw."""
    logger = get_run_logger()
    logger.info("Starting data ingestion...")

    result = subprocess.run(
        ["python", str(INGESTION_SCRIPT)],
        capture_output=True,
        text=True,
        cwd=ROOT_DIR
    )

    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception(f"Ingestion failed: {result.stderr}")

    logger.info(result.stdout)
    logger.info("Ingestion completed successfully")


@task(name="run_dbt", retries=1, retry_delay_seconds=10)
def run_dbt():
    """Run dbt models: raw → staging → consumption."""
    logger = get_run_logger()
    logger.info("Running dbt models...")

    result = subprocess.run(
        ["dbt", "run"],
        capture_output=True,
        text=True,
        cwd=DBT_DIR
    )

    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception(f"dbt run failed: {result.stderr}")

    logger.info(result.stdout)
    logger.info("dbt models completed successfully")


@task(name="run_dbt_tests", retries=1, retry_delay_seconds=10)
def run_dbt_tests():
    """Run dbt data quality tests."""
    logger = get_run_logger()
    logger.info("Running dbt tests...")

    result = subprocess.run(
        ["dbt", "test"],
        capture_output=True,
        text=True,
        cwd=DBT_DIR
    )

    logger.info(result.stdout)

    if result.returncode != 0:
        logger.warning("Some dbt tests failed — check the test report")

    logger.info("dbt tests completed")


@task(name="generate_test_report")
def generate_test_report():
    """Generate HTML test quality report."""
    logger = get_run_logger()
    logger.info("Generating test report...")

    result = subprocess.run(
        ["python", str(MONITORING_SCRIPT)],
        capture_output=True,
        text=True,
        cwd=ROOT_DIR
    )

    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception(f"Report generation failed: {result.stderr}")

    logger.info("Test report generated successfully")


@flow(name="health-data-pipeline", log_prints=True)
def health_pipeline():
    """Main pipeline flow."""
    ingest_data()
    run_dbt()
    run_dbt_tests()
    generate_test_report()


if __name__ == "__main__":
    health_pipeline()