# dags/sales_ingestion_dag.py

from __future__ import annotations

import pendulum
import pandas as pd

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


# Define the data processing function
def _read_sales_data():
    """
    Reads the sales data from a CSV file and prints the first 5 rows.
    This simulates the 'Extract' step.
    """
    # The file path inside the container
    file_path = "/opt/airflow/data/Orders.csv"

    print(f"Reading data from {file_path}...")
    df = pd.read_csv(file_path, encoding='utf-8')  # Using a specific encoding for this dataset

    print("Successfully read the data. Here's a preview:")
    print(df.head())


# Define the DAG
with DAG(
        dag_id="sales_data_ingestion",
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        schedule=None,  # This DAG will be triggered manually
        catchup=False,
        tags=["sales", "etl"],
) as dag:
    # Define the task using the PythonOperator
    extract_data_task = PythonOperator(
        task_id="extract_sales_data",
        python_callable=_read_sales_data,
    )