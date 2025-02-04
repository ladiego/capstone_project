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

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "host.docker.internal")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5435")
POSTGRES_USER = os.getenv("POSTGRES_USER", "dewa_capstone")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "dewa_capstone")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "project_caps3")

def data_info(dataframe, table_name):
    """Logs DataFrame details for debugging."""
    logging.info(f"Table: {table_name}")
    logging.info(f"Dataframe Info = {dataframe.info()}")
    logging.info(f"Data Types = {dataframe.dtypes}")
    logging.info(f"Number of Rows = {len(dataframe)}")
    logging.info(f"Head of Dataframe = \n{dataframe.head()}")

def extract_table(table):
    """Extracts a single table from PostgreSQL."""
    try:
        # create connection to PostgreSQL
        engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}')
        sql_query = f"SELECT * FROM library.{table}"
        df = pd.read_sql_query(sql_query, engine)
        if df.empty:
            logging.warning(f"No data extracted from {table}. Skipping...")
            return None
        data_info(df, table.split('.')[-1])
        return df
    except Exception as e:
        logging.error(f"Error extracting {table}: {e}")
        raise