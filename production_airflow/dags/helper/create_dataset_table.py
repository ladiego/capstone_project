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

def create_dataset_if_not_exists(client, BIGQUERY_PROJECT, BIGQUERY_DATASET):
    """Check if a dataset exists, and create it if it does not."""
    full_dataset_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}"
    
    try:
        # ensure dataset is exist
        client.get_dataset(full_dataset_id)
        logging.info(f"Dataset {full_dataset_id} already exists.")
    except NotFound:
        # if dataset does not exist, create it
        dataset = bigquery.Dataset(full_dataset_id)
        dataset.location = "asia-southeast2"  # set location
        dataset = client.create_dataset(dataset)  # create dataset
        logging.info(f"Created dataset {full_dataset_id}.")

def create_table_if_not_exists(client, project_id, dataset_id, table_name, schema):
    """Check if a table exists, and create it if it does not."""
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    
    try:
        # ensure table is exist
        client.get_table(table_id)
        logging.info(f"Tabel {table_id} is exist.")
    except NotFound:
        # if table does not exist, create it
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="created_at")  # Set partitioning
        table = client.create_table(table)  # Create the table
        logging.info(f"Table {table_id} created with partitioning on 'created_at'.")