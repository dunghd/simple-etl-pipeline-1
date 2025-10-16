"""Airflow DAG for the simple ETL pipeline.

Place this file in your Airflow `dags/` folder (Airflow reads the `dags/`
directory by default). The DAG uses PythonOperator to call into the
project's ETL functions.

Notes:
- For local testing you can point your Airflow instance to this repository
  and it will pick up the DAG automatically.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import os

try:
    # Local import from the project
    from etl_pipeline.airflow_tasks import extract_task, transform_task, load_task
except Exception:
    # When Airflow imports DAGs, PYTHONPATH may differ; attempt relative import
    from ..etl_pipeline.airflow_tasks import extract_task, transform_task, load_task

from airflow import DAG
from airflow.operators.python import PythonOperator


# Default DAG args
default_args = {
    'owner': 'etl_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='simple_etl_pipeline',
    default_args=default_args,
    description='A simple ETL DAG using the project pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['example', 'etl'],
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_task,
        op_kwargs={'csv_path': None},
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_task,
        op_kwargs={'transform_config': {'clean': True, 'add_columns': True}},
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_task,
        op_kwargs={'table_name': 'processed_data', 'load_config': {'if_exists': 'replace'}},
    )

    extract >> transform >> load
