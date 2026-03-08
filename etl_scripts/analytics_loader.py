import os
import pandas as pd
import sqlite3
import logging
from pathlib import Path
from sqlalchemy import create_engine

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_silver_to_analytics(input_dir, db_path, table_name="daily_country_sales"):
    """
    Reads silver Parquet data and loads it into a SQLite database table.
    """
    logger.info(f"Reading silver data from {input_dir}...")
    try:
        # Read all parquet files in the directory
        df = pd.read_parquet(input_dir)
        logger.info(f"Loaded {len(df)} rows from silver layer.")
        
        # Ensure db directory exists
        db_file = Path(db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Using SQLAlchemy engine for easier DataFrame loading
        engine = create_engine(f'sqlite:///{db_path}')
        
        logger.info(f"Loading data into SQLite table '{table_name}' at {db_path}...")
        # Idempotency: Use if_exists='replace' to ensure data is consistent on re-runs
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info("Analytical store loading complete.")
        
    except Exception as e:
        logger.error(f"Error loading data to analytical store: {e}")
        raise

if __name__ == "__main__":
    import sys
    DB_PATH = os.getenv("SQLITE_DB_PATH", "/app/data/analytics.db")
    if len(sys.argv) < 2:
        print("Usage: python analytics_loader.py <input_dir> [db_path]")
        # Fallback for local testing if no args provided
        load_silver_to_analytics("/app/data/silver", DB_PATH)
    else:
        db = sys.argv[2] if len(sys.argv) > 2 else DB_PATH
        load_silver_to_analytics(sys.argv[1], db)
