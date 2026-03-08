import os
import pandas as pd
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_bronze_to_silver(input_dir, output_dir):
    """
    Reads bronze Parquet data, cleans, transforms, aggregates,
    and writes to silver layer in Parquet format.
    """
    logger.info(f"Reading bronze data from {input_dir}...")
    try:
        df = pd.read_parquet(input_dir)
        logger.info(f"Loaded {len(df)} rows from bronze layer.")
        
        # 1. Handle missing CustomerID
        logger.info("Handling missing CustomerID...")
        df['CustomerID'] = df['CustomerID'].fillna('UNKNOWN')
        
        # 2. Filter negative Quantity and zero UnitPrice
        logger.info("Filtering records with negative Quantity or zero UnitPrice...")
        df = df[(df['Quantity'] > 0) & (df['UnitPrice'] > 0)]
        logger.info(f"Rows after filtering: {len(df)}")
        
        # 3. Calculate TotalPrice
        logger.info("Calculating TotalPrice...")
        df['TotalPrice'] = df['Quantity'] * df['UnitPrice']
        
        # 4. Aggregate by InvoiceDate (daily) and Country
        logger.info("Aggregating data to daily sales per country...")
        # Ensure InvoiceDate is just the date part for daily aggregation
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate']).dt.normalize()
        
        daily_sales = df.groupby(['InvoiceDate', 'Country']).agg(
            DailyTotalSales=('TotalPrice', 'sum')
        ).reset_index()
        
        # 5. Partitioning columns for silver layer
        daily_sales['year'] = daily_sales['InvoiceDate'].dt.year
        daily_sales['month'] = daily_sales['InvoiceDate'].dt.month
        
        logger.info(f"Writing transformed data to {output_dir} partitioned by year and month...")
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        daily_sales.to_parquet(output_path, partition_cols=['year', 'month'], index=False)
        logger.info("Silver layer processing complete.")
        
    except Exception as e:
        logger.error(f"Error transforming data to silver: {e}")
        raise

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python silver_transformer.py <input_dir> <output_dir>")
        sys.exit(1)
    transform_bronze_to_silver(sys.argv[1], sys.argv[2])
