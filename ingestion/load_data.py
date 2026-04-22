import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine, text
from datetime import datetime, timezone
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

def get_google_sheet(sheet_name: str):
    """Extract data from Google Sheet and return as DataFrame and sheet ID."""
    try:
        logger.info(f"Connecting to Google Sheet: {sheet_name}")

        creds = Credentials.from_service_account_file(
            os.getenv('GCP_SERVICE_ACCOUNT_PATH'),
            scopes=SCOPES
        )

        client = gspread.authorize(creds)
        spreadsheet = client.open(sheet_name)
        sheet_id = spreadsheet.id
        df = pd.DataFrame(spreadsheet.sheet1.get_all_records())

        logger.info(f"Successfully extracted {len(df)} rows from Google Sheet")
        return df, sheet_id

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

def load_to_landing(df: pd.DataFrame, engine, sheet_id: str) -> None:
    """Load raw Google Sheet data into landing as JSONB rows."""
    try:
        logger.info("Loading data to landing.raw_patient...")

        import json
        from datetime import datetime, timezone

        rows = []
        loaded_at = datetime.now(timezone.utc)

        for _, row in df.iterrows():
            rows.append({
                '_loaded_at': loaded_at,
                '_source_sheet_id': sheet_id,
                '_raw_data': json.dumps(row.to_dict())
            })

        landing_df = pd.DataFrame(rows)

        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE landing.raw_patient"))
            conn.commit()

        landing_df.to_sql(
            name='raw_patient',
            con=engine,
            schema='landing',
            if_exists='append',
            index=False
        )

        logger.info(f"Successfully loaded {len(landing_df)} rows into landing.raw_patient")

    except Exception as e:
        logger.error(f"Failed to load data to landing: {e}")
        raise


def load_to_raw(engine) -> None:
    """Extract from landing JSONB and load into raw with proper types."""
    try:
        logger.info("Transforming landing data into raw.raw_patient...")

        sql = text("""
            INSERT INTO raw.raw_patient (
                first_name, last_name, birth_date, gender, address,
                city, state, zip_code, phone_number, email,
                emergency_contact_name, emergency_contact_phone,
                blood_type, insurance_provider, insurance_number,
                marital_status, preferred_language, nationality,
                allergies, last_visit_date
            )
            SELECT
                NULLIF(TRIM(_raw_data->>'first_name'), ''),
                NULLIF(TRIM(_raw_data->>'last_name'), ''),
                CASE WHEN TRIM(_raw_data->>'birth_date') IN ('', 'None', 'NaT') THEN NULL
                     ELSE TRIM(_raw_data->>'birth_date')::date END,
                NULLIF(TRIM(_raw_data->>'gender'), ''),
                NULLIF(TRIM(_raw_data->>'address'), ''),
                NULLIF(TRIM(_raw_data->>'city'), ''),
                NULLIF(TRIM(_raw_data->>'state'), ''),
                NULLIF(TRIM(_raw_data->>'zip_code'), ''),
                NULLIF(TRIM(_raw_data->>'phone_number'), ''),
                NULLIF(TRIM(_raw_data->>'email'), ''),
                NULLIF(TRIM(_raw_data->>'emergency_contact_name'), ''),
                NULLIF(TRIM(_raw_data->>'emergency_contact_phone'), ''),
                NULLIF(TRIM(_raw_data->>'blood_type'), ''),
                NULLIF(TRIM(_raw_data->>'insurance_provider'), ''),
                NULLIF(TRIM(_raw_data->>'insurance_number'), ''),
                NULLIF(TRIM(_raw_data->>'marital_status'), ''),
                NULLIF(TRIM(_raw_data->>'preferred_language'), ''),
                NULLIF(TRIM(_raw_data->>'nationality'), ''),
                NULLIF(TRIM(_raw_data->>'allergies'), ''),
                CASE WHEN TRIM(_raw_data->>'last_visit_date') IN ('', 'None', 'NaT') THEN NULL
                     ELSE TRIM(_raw_data->>'last_visit_date')::date END
            FROM landing.raw_patient
        """)

        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE raw.raw_patient RESTART IDENTITY"))
            conn.execute(sql)
            conn.commit()

            result = conn.execute(text("SELECT COUNT(*) FROM raw.raw_patient"))
            row_count = result.scalar()

        logger.info(f"Successfully loaded {row_count} rows into raw.raw_patient")

    except Exception as e:
        logger.error(f"Failed to load data to raw: {e}")
        raise


def table_exists(conn, schema: str, table: str) -> bool:
    """Check if a table exists in the given schema."""
    result = conn.execute(text(f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_name = '{table}'
        )
    """))
    return result.scalar()

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
    df, sheet_id = get_google_sheet(os.getenv('GOOGLE_SHEET_NAME'))

    # Validate
    if not validate_dataframe(df):
        raise ValueError("Pre-load validation failed, aborting pipeline")

    # Load to landing (exact copy)
    engine = get_db_engine()
    load_to_landing(df, engine, sheet_id)

    # Load to raw (typed + cleaned)
    load_to_raw(engine)

    logger.info("Ingestion pipeline completed successfully")

if __name__ == "__main__":
    run_pipeline()