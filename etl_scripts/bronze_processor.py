import os
import pandas as pd
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_bronze_layer(input_file, output_dir):
    """
    Reads raw CSV file and writes to bronze layer in Parquet format.
    """
    input_path = Path(input_file)
    if not input_path.exists():
        logger.error(f"Input file {input_file} not found.")
        raise FileNotFoundError(f"Input file {input_file} not found.")

    logger.info(f"Reading raw CSV {input_file} into Pandas DataFrame...")
    try:
        df = pd.read_csv(input_path, encoding='ISO-8859-1')
        
        # Ensure InvoiceDate is datetime
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
        
        # Create year and month columns for partitioning
        df['year'] = df['InvoiceDate'].dt.year
        df['month'] = df['InvoiceDate'].dt.month
        
        logger.info(f"Writing DataFrame to Parquet in {output_dir} partitioned by year and month...")
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        df.to_parquet(output_path, partition_cols=['year', 'month'], index=False)
        logger.info("Bronze layer processing complete.")
    except Exception as e:
        logger.error(f"Error processing data to bronze: {e}")
        raise

if __name__ == "__main__":
    process_bronze_layer("data/raw/online_retail.csv", "data/bronze")
