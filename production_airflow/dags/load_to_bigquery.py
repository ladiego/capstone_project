import os
import logging
import pandas as pd
import time
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from helper.helper_ai7 import extract_table
from helper.helper_ai9 import create_dataset_if_not_exists, create_table_if_not_exists

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BIGQUERY_KEY_PATH = os.getenv("BIGQUERY_KEY_PATH", "/opt/airflow/keys/gcp_keys.json")
BIGQUERY_PROJECT = "purwadika"
BIGQUERY_DATASET = "diego_library_capstone3"
LOCAL_TZ = pytz.timezone("Asia/Jakarta")

TABLES = ["users", "books", "rents"]

def extract_table_data(table):
    """Extracts a single table into a DataFrame."""
    retries = 3
    for attempt in range(retries):
        try:
            logging.info(f"Attempting to extract {table} (Attempt {attempt + 1}/{retries})")
            df = extract_table(table)
            if df is not None:
                logging.info(f"Extracted {table} with shape: {df.shape}")
                return df
            break
        except Exception as e:
            logging.error(f"Error extracting {table} (Attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                raise

def save_to_csv(**kwargs):
    """Save a DataFrame to CSV and return the file path."""
    dataframe = kwargs['ti'].xcom_pull(task_ids=kwargs['extract_task_id'])
    
    if dataframe is None or isinstance(dataframe, str):
        raise ValueError("Expected a DataFrame, but got a string or None.")
    
    file_path = f"/tmp/{kwargs['table_name']}.csv"
    dataframe.to_csv(file_path, index=False)
    logging.info(f"Saved {kwargs['table_name']} to {file_path}")
    return file_path

def load_data_to_bigquery(file_path, table_name):
    """Loads a CSV file into BigQuery."""
    logging.info(f"Loading data from {file_path} into {table_name}")
    
    try:   
        credentials = service_account.Credentials.from_service_account_file(
            BIGQUERY_KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        # ensure the dataset exist
        create_dataset_if_not_exists(client, BIGQUERY_PROJECT, BIGQUERY_DATASET)

        table_configs = {
            "users": [
                bigquery.SchemaField("user_id", "INT64"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("gender", "STRING"),
                bigquery.SchemaField("address", "STRING"),
                bigquery.SchemaField("email", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ],
            "books": [
                bigquery.SchemaField("book_id", "INT64"),
                bigquery.SchemaField("title", "STRING"),
                bigquery.SchemaField("author", "STRING"),
                bigquery.SchemaField("publisher", "STRING"),
                bigquery.SchemaField("release_year", "INT64"),
                bigquery.SchemaField("stock", "INT64"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ],
            "rents": [
                bigquery.SchemaField("rent_id", "INT64"),
                bigquery.SchemaField("user_id", "INT64"),
                bigquery.SchemaField("book_id", "INT64"),
                bigquery.SchemaField("rent_date", "TIMESTAMP"),
                bigquery.SchemaField("return_date", "TIMESTAMP"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ],
        }

        if table_name in table_configs:
            create_table_if_not_exists(client, BIGQUERY_PROJECT, BIGQUERY_DATASET, table_name, table_configs[table_name])
            table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{table_name}"
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=table_configs[table_name],
                max_bad_records=10,
                ignore_unknown_values=True,
            )
            with open(file_path, "rb") as file_data:
                load_job = client.load_table_from_file(file_data, table_id, job_config=job_config)
                load_job.result()  # Wait for the job to complete
                logging.info(f"Data loaded successfully into {table_id}")
    except Exception as e:
        logging.error(f"Failed to load data into BigQuery: {e}")
        raise

default_args = {
    "owner": "purwadika_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ['dpd.kerja@gmail.com'],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "extract_load_data_bq",  
    start_date=datetime(2024, 12, 1),
    schedule_interval="0 1 * * *",
    catchup=False,
    tags=["ingestion", "postgresql", "bigquery"],
    default_args=default_args,
    max_active_runs=3,
) as dag:

    start = DummyOperator(task_id="start")

    # Create TaskGroups for each table
    for table in TABLES:
        with TaskGroup(group_id=f"process_{table}") as process_group:
            extract_task = PythonOperator(
                task_id=f"extract_{table}",
                python_callable=extract_table_data,
                op_kwargs={"table": table},
            )
            save_task = PythonOperator(
                task_id=f"save_{table}",
                python_callable=save_to_csv,
                op_kwargs={
                    "extract_task_id": f"process_{table}.extract_{table}",
                    "table_name": table
                },
            )
            load_task = PythonOperator(
                task_id=f"load_{table}",
                python_callable=load_data_to_bigquery,
                op_kwargs={
                    "file_path": "{{ task_instance.xcom_pull(task_ids='process_" + table + ".save_" + table + "') }}",
                    "table_name": table
                },
            )

            extract_task >> save_task >> load_task

    end = DummyOperator(task_id="end")

    start >> [process_group for table in TABLES] >> end