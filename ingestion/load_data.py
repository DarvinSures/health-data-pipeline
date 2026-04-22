import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Constants
SCOPES = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]

def get_google_sheet(sheet_name: str) -> pd.DataFrame:
    """Extract data from Google Sheet and return as DataFrame."""
    try:
        logger.info(f"Connecting to Google Sheet: {sheet_name}")
        
        creds = Credentials.from_service_account_file(
            os.getenv('GCP_SERVICE_ACCOUNT_PATH'),
            scopes=SCOPES
        )
        
        client = gspread.authorize(creds)
        sheet = client.open(sheet_name).sheet1
        data = sheet.get_all_records()
        
        df = pd.DataFrame(data)
        logger.info(f"Successfully extracted {len(df)} rows from Google Sheet")
        return df
        
    except Exception as e:
        logger.error(f"Failed to extract data from Google Sheet: {e}")
        raise

def get_db_engine():
    """Create database engine from environment variables."""
    db_url = (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(db_url)

def validate_dataframe(df: pd.DataFrame) -> bool:
    """Run pre-load validation checks on the DataFrame."""
    logger.info("Running pre-load validation checks...")
    
    checks_passed = True
    
    # Check 1 — dataframe is not empty
    if df.empty:
        logger.error("FAILED: DataFrame is empty")
        checks_passed = False
    else:
        logger.info(f"PASSED: DataFrame has {len(df)} rows")
    
    # Check 2 — required columns exist
    required_columns = [
        'first_name', 'last_name', 'birth_date', 
        'gender', 'email', 'phone_number'
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"FAILED: Missing required columns: {missing_columns}")
        checks_passed = False
    else:
        logger.info("PASSED: All required columns present")
    
    # Check 3 — no nulls in critical fields
    null_counts = df[required_columns].isnull().sum()
    if null_counts.any():
        logger.warning(f"WARNING: Null values found: {null_counts[null_counts > 0]}")
    else:
        logger.info("PASSED: No nulls in critical fields")
    
    # Check 4 — no duplicate emails
    if df['email'].duplicated().any():
        logger.warning(f"WARNING: {df['email'].duplicated().sum()} duplicate emails found")
    else:
        logger.info("PASSED: No duplicate emails")
    
    return checks_passed

def load_to_postgres(df: pd.DataFrame, engine) -> None:
    """Load DataFrame into raw.raw_patient table."""
    try:
        logger.info("Loading data to raw.raw_patient...")
        
        # Drop dependent view and alter column type if needed
        with engine.connect() as conn:
            conn.execute(text("DROP VIEW IF EXISTS raw.stg_patient"))
            conn.execute(text("ALTER TABLE raw.raw_patient ALTER COLUMN blood_type TYPE VARCHAR(10)"))
            conn.commit()
        
        # Truncate first to avoid duplicates, then append
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE raw.raw_patient RESTART IDENTITY"))
            conn.commit()
        
        df.to_sql(
            name='raw_patient',
            con=engine,
            schema='raw',
            if_exists='append',
            index=False
        )
        
        # Post-load row count check
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM raw.raw_patient"))
            row_count = result.scalar()
            
        logger.info(f"Successfully loaded {row_count} rows into raw.raw_patient")
        
        if row_count != len(df):
            logger.error(f"Row count mismatch: expected {len(df)}, got {row_count}")
            raise ValueError("Row count mismatch after load")
            
    except Exception as e:
        logger.error(f"Failed to load data to Postgres: {e}")
        raise

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare DataFrame for loading."""
    logger.info("Cleaning DataFrame...")
    
    # Replace empty strings with None
    df = df.replace('', None)
    
    # Convert date columns to proper dates, invalid = None
    date_columns = ['birth_date', 'last_visit_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
    
    logger.info("DataFrame cleaned successfully")
    return df

def run_pipeline():
    """Main pipeline function."""
    logger.info("Starting ingestion pipeline...")
    
    # Extract
    df = get_google_sheet(os.getenv('GOOGLE_SHEET_NAME'))
    
    # Validate
    if not validate_dataframe(df):
        raise ValueError("Pre-load validation failed, aborting pipeline")
    
    # Clean
    df = clean_dataframe(df)
    
    # Load
    engine = get_db_engine()
    load_to_postgres(df, engine)
    
    logger.info("Ingestion pipeline completed successfully")

if __name__ == "__main__":
    run_pipeline()