from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import logging

# Import ETL functions
from etl_scripts.download_raw import download_raw_data
from etl_scripts.bronze_processor import process_bronze_layer
from etl_scripts.silver_transformer import transform_bronze_to_silver
from etl_scripts.analytics_loader import load_silver_to_analytics
from etl_scripts.ge_runner import run_ge_checkpoint

# Configuration
RAW_DATA_URL = os.getenv("RAW_DATA_URL", "https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/online_retail_sample.csv")
RAW_DATA_FILE = "/app/data/raw/online_retail.csv"
BRONZE_DIR = "/app/data/bronze"
SILVER_DIR = "/app/data/silver"
DB_PATH = os.getenv("SQLITE_DB_PATH", "/app/data/analytics.db")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_analytics_pipeline',
    default_args=default_args,
    description='Automated and Validated ETL Pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'ecommerce'],
) as dag:

    def download_raw_data_task():
        logging.info("Starting download_raw_data task...")
        download_raw_data(RAW_DATA_URL)

    def bronze_layer_processing_task():
        logging.info("Starting bronze_layer_processing task...")
        process_bronze_layer(RAW_DATA_FILE, BRONZE_DIR)

    def bronze_data_validation_task():
        logging.info("Starting bronze_data_validation task...")
        run_ge_checkpoint("bronze_checkpoint", BRONZE_DIR, "bronze_data")

    def silver_layer_transformation_task():
        logging.info("Starting silver_layer_transformation task...")
        transform_bronze_to_silver(BRONZE_DIR, SILVER_DIR)

    def silver_data_validation_task():
        logging.info("Starting silver_data_validation task...")
        run_ge_checkpoint("silver_checkpoint", SILVER_DIR, "silver_data")

    def load_to_analytical_store_task():
        logging.info("Starting load_to_analytical_store task...")
        load_silver_to_analytics(SILVER_DIR, DB_PATH)

    # Define tasks
    t1 = PythonOperator(
        task_id='download_raw_data',
        python_callable=download_raw_data_task,
    )

    t2 = PythonOperator(
        task_id='bronze_layer_processing',
        python_callable=bronze_layer_processing_task,
    )

    t3 = PythonOperator(
        task_id='bronze_data_validation',
        python_callable=bronze_data_validation_task,
    )

    t4 = PythonOperator(
        task_id='silver_layer_transformation',
        python_callable=silver_layer_transformation_task,
    )

    t5 = PythonOperator(
        task_id='silver_data_validation',
        python_callable=silver_data_validation_task,
    )

    t6 = PythonOperator(
        task_id='load_to_analytical_store',
        python_callable=load_to_analytical_store_task,
    )

    # Set dependencies (6 sequential stages)
    t1 >> t2 >> t3 >> t4 >> t5 >> t6
