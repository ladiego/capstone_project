from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from helper.connection_schema import create_schema_and_tables
from helper.generate import insert_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),  
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ingestion_data_to_sql',
    default_args=default_args,
    description='Insert data to PostgreSQL',
    schedule_interval='@hourly',
    catchup=False,
    tags=['postgres', 'data-insertion'],
) as dag:
    
    create_schema_task = PythonOperator(
        task_id='generate_data',
        python_callable=create_schema_and_tables,
    )

    insert_data_task = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data,
    )

    create_schema_task >> insert_data_task 