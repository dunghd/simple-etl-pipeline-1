"""Airflow task wrappers for the ETL pipeline.

This module provides simple functions that can be used by Airflow's
PythonOperator. They call into the existing ETL classes in the project.

Design notes:
- Keep functions idempotent and lightweight.
- Use a temporary pickle file under the project's `data/` directory to
  pass the intermediate dataframe between tasks when Airflow isn't
  configured with external storage. In a production Airflow setup prefer
  XCom, object store, or a shared database instead.
"""

from __future__ import annotations

import os
import pickle
from typing import Any, Dict

import pandas as pd

from .main import ETLPipeline


# Path helpers
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
PICKLE_PATH = os.path.join(DATA_DIR, "etl_intermediate.pkl")


def extract_task(csv_path: str | None = None, **context) -> str:
    """Extract data and persist intermediate dataframe to disk.

    Returns the path to the pickle file containing the dataframe.
    """
    csv_path = csv_path or os.path.join(DATA_DIR, "sample_data.csv")
    pipeline = ETLPipeline(csv_path, os.path.join(DATA_DIR, "etl_database.db"))
    df = pipeline.extract()
    if df is None:
        raise RuntimeError("Extract returned no data")

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PICKLE_PATH, "wb") as f:
        pickle.dump(df, f)

    # Push path for downstream tasks
    return PICKLE_PATH


def transform_task(transform_config: Dict[str, Any] | None = None, **context) -> str:
    """Load intermediate dataframe, transform it, and persist result.

    Returns the path to the pickle file containing the transformed dataframe.
    """
    transform_config = transform_config or {"clean": True, "add_columns": True}

    if not os.path.exists(PICKLE_PATH):
        raise FileNotFoundError("Intermediate data not found. Run extract first.")

    with open(PICKLE_PATH, "rb") as f:
        df = pickle.load(f)

    # Use ETLPipeline's transformer for consistent behavior
    pipeline = ETLPipeline(os.path.join(DATA_DIR, "sample_data.csv"), os.path.join(DATA_DIR, "etl_database.db"))
    transformed = pipeline.transform(df, transform_config)
    if transformed is None:
        raise RuntimeError("Transform returned no data")

    with open(PICKLE_PATH, "wb") as f:
        pickle.dump(transformed, f)

    return PICKLE_PATH


def load_task(table_name: str = "processed_data", load_config: Dict[str, Any] | None = None, **context) -> bool:
    """Load the transformed dataframe from disk into the database.

    Returns True on success.
    """
    load_config = load_config or {"if_exists": "replace", "chunk_size": None}

    if not os.path.exists(PICKLE_PATH):
        raise FileNotFoundError("Transformed data not found. Run transform first.")

    with open(PICKLE_PATH, "rb") as f:
        df = pickle.load(f)

    pipeline = ETLPipeline(os.path.join(DATA_DIR, "sample_data.csv"), os.path.join(DATA_DIR, "etl_database.db"), table_name)
    success = pipeline.load(df, load_config)

    return success
