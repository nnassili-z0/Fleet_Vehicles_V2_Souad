"""
dag_hello_platform.py
---------------------
Minimal validation DAG — confirms that:
  1. The Airflow scheduler is running correctly.
  2. The DAG folder is properly mounted inside the container.
  3. Python dependencies from requirements.txt are importable.

This DAG should be the first one to turn green after `docker compose up`.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ------------------------------------------------------------------
# Dependency smoke-test: import key libraries installed via requirements.txt
# ------------------------------------------------------------------
def check_dependencies(**context):
    import pandas as pd
    import numpy as np
    import pyarrow  # noqa: F401
    import requests  # noqa: F401
    import psycopg2  # noqa: F401

    print(f"✅ pandas   {pd.__version__}")
    print(f"✅ numpy    {np.__version__}")
    print(f"✅ pyarrow  {pyarrow.__version__}")
    print("✅ requests, psycopg2 — all imports successful")


def hello_platform(**context):
    execution_date = context["ds"]
    print(f"🚀 Data Platform is running! Execution date: {execution_date}")
    print("All services healthy: airflow-webserver, airflow-scheduler, postgres")


def write_sample_output(**context):
    import pandas as pd
    from pathlib import Path

    output_dir = Path("/opt/airflow/data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        {
            "run_date": [context["ds"]],
            "status": ["success"],
            "message": ["Platform validation passed"],
        }
    )
    out_path = output_dir / f"validation_{context['ds_nodash']}.csv"
    df.to_csv(out_path, index=False)
    print(f"📄 Validation file written to: {out_path}")


# ------------------------------------------------------------------
# DAG definition
# ------------------------------------------------------------------
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="dag_hello_platform",
    description="Minimal validation DAG — confirms the containerised environment is healthy",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["validation", "infrastructure"],
) as dag:

    t1 = PythonOperator(
        task_id="check_python_dependencies",
        python_callable=check_dependencies,
    )

    t2 = PythonOperator(
        task_id="hello_platform",
        python_callable=hello_platform,
    )

    t3 = PythonOperator(
        task_id="write_sample_output",
        python_callable=write_sample_output,
    )

    t1 >> t2 >> t3
