import os
import logging
import pandas as pd
import time
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from datetime import datetime, timedelta
import pytz

BIGQUERY_KEY_PATH = os.getenv("BIGQUERY_KEY_PATH", "/opt/airflow/keys/gcp_keys.json")
BIGQUERY_PROJECT = "purwadika"
BIGQUERY_DATASET = "percobaan_hehe"
LOCAL_TZ = pytz.timezone("Asia/Jakarta")

def create_dataset_if_not_exists(client, BIGQUERY_PROJECT, BIGQUERY_DATASET):
    """Check if a dataset exists, and create it if it does not."""
    full_dataset_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}"
    
    try:
        # ensure dataset exists
        client.get_dataset(full_dataset_id)
        logging.info(f"Dataset {full_dataset_id} already exists.")
    except NotFound:
        # if dataset not exist, create dataset
        dataset = bigquery.Dataset(full_dataset_id)
        dataset.location = "asia-southeast2"  # set location
        dataset = client.create_dataset(dataset)  
        logging.info(f"Created dataset {full_dataset_id}.")

def create_table_if_not_exists(client, project_id, dataset_id, table_name, schema):
    """Check if a table exists, and create it if it does not."""
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    
    try:
        # ensure table exists
        client.get_table(table_id)
        logging.info(f"Tabel {table_id} sudah ada.")
    except NotFound:
        # if table not exist, create table
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="created_at")  # Set partitioning
        table = client.create_table(table)  
        logging.info(f"Table {table_id} created with partitioning on 'created_at'.")