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
import sys

def _ensure_project_on_path(package_name: str = "etl_pipeline", max_levels: int = 6) -> None:
    """Find a parent directory that contains `package_name` and add it to sys.path.

    This helps when Airflow loads DAG files from a different folder (for
    example, the Airflow `dags/` directory) and the project root isn't on
    PYTHONPATH. We walk upward from the DAG's directory up to `max_levels`
    looking for a folder that contains the package directory and insert it
    at the front of sys.path when found.
    """

    current = os.path.abspath(os.path.dirname(__file__))
    for _ in range(max_levels):
        candidate = os.path.join(current, package_name)
        if os.path.isdir(candidate):
            if current not in sys.path:
                sys.path.insert(0, current)
            return
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent


# Ensure the project root is discoverable by Python so we can import the
# local `etl_pipeline` package when Airflow loads this DAG from a different
# working directory.
_ensure_project_on_path()

from etl_pipeline.airflow_tasks import extract_task, transform_task, load_task

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


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
    # schedule can be controlled via environment variable ETL_PIPELINE_SCHEDULE
    # - if set to 'every_minute' (or 'minute') it will use '*/1 * * * *'
    # - if set to any other non-empty value it's used directly (allowing cron or '@daily')
    # - if not set, falls back to '@daily'
    schedule=(os.getenv('ETL_PIPELINE_SCHEDULE', '').strip().lower() in ('every_minute', 'minute') and '*/1 * * * *')
             or os.getenv('ETL_PIPELINE_SCHEDULE')
             or '@daily',
    # use a recent start_date; with catchup=False this will start immediately
    start_date=datetime(2025, 10, 17),
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
