import pandas as pd
from sqlalchemy import create_engine, inspect
import logging
import argparse
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_engine():
    """Create database engine for PostgreSQL."""
    db_url = "postgresql://admin:admin123@localhost:5432/health_db"
    return create_engine(db_url)

def get_dbt_schemas():
    """Get list of schemas created by dbt."""
    engine = get_db_engine()
    inspector = inspect(engine)
    schemas = inspector.get_schema_names()
    
    # Filter for dbt-created schemas (raw, staging, consumption, etc.)
    dbt_schemas = [s for s in schemas if s not in ['public', 'pg_catalog', 'information_schema']]
    return dbt_schemas

def display_table(table_name, schema='public'):
    """Display a specific table from the database."""
    try:
        engine = get_db_engine()
        query = f"SELECT * FROM {schema}.{table_name}"
        df = pd.read_sql(query, engine)
        
        print(f"\n{'='*80}")
        print(f"TABLE: {schema}.{table_name}")
        print(f"{'='*80}")
        print(f"Rows: {len(df)} | Columns: {len(df.columns)}")
        print(f"{'-'*80}")
        print(df.to_string())
        print(f"{'='*80}\n")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to display table {schema}.{table_name}: {e}")
        return None

def list_all_tables():
    """List all tables in dbt-created schemas."""
    try:
        engine = get_db_engine()
        inspector = inspect(engine)
        
        schemas = get_dbt_schemas()
        all_tables = {}
        
        for schema in schemas:
            tables = inspector.get_table_names(schema=schema)
            if tables:
                all_tables[schema] = tables
                logger.info(f"Schema '{schema}': {len(tables)} table(s)")
        
        return all_tables
        
    except Exception as e:
        logger.error(f"Failed to list tables: {e}")
        return {}

def main():
    """Main function to display dbt results."""
    parser = argparse.ArgumentParser(description="View dbt-created tables from PostgreSQL")
    parser.add_argument(
        "table",
        nargs="?",
        help="Table name to display (e.g., fhir_patient, stg_patient, raw_patient). If not provided, shows all tables."
    )
    parser.add_argument(
        "-s", "--schema",
        default="public",
        help="Schema name (default: public). Common schemas: raw, staging, consumption"
    )
    
    args = parser.parse_args()
    
    logger.info("Connecting to health_db...")
    
    try:
        if args.table:
            # Display specific table
            logger.info(f"Retrieving table: {args.schema}.{args.table}")
            display_table(args.table, schema=args.schema)
        else:
            # Display all tables
            all_tables = list_all_tables()
            
            if not all_tables:
                logger.warning("No tables found. Make sure to run 'dbt run' first.")
                return
            
            # Display tables organized by schema
            for schema, tables in all_tables.items():
                print(f"\n\n{'#'*80}")
                print(f"# SCHEMA: {schema.upper()}")
                print(f"{'#'*80}\n")
                
                for table in tables:
                    display_table(table, schema=schema)
                
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        logger.info("Make sure PostgreSQL is running: docker-compose up -d")

if __name__ == "__main__":
    main()
