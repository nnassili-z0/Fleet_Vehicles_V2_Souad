"""
Medallion Bronze to Gold DAG - Combines Bronze→Silver and Silver→Gold pipeline.

This DAG:
1. Loads Bronze data
2. Runs data quality checks and writes Silver data to data/silver/data
3. Loads Silver data and writes Gold data to data/gold/data
4. Persists quality reports to artifacts/quality_reports

DAG ID: medallion_bronze_to_gold
Schedule: Daily at 03:00 UTC
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain

from src.pipelines.bronze_to_silver.transformer import BronzeToSilverTransformer
from src.pipelines.silver_to_gold.transformer import SilverToGoldTransformer

# ========================================
# DAG CONFIGURATION
# ========================================

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": datetime(2024, 1, 1),
}

DAG_CONFIG = {
    "dag_id": "medallion_bronze_to_gold",
    "default_args": DEFAULT_ARGS,
    "description": "Bronze to Gold - Full medallion pipeline",
    "schedule": "0 3 * * *",  # Daily at 03:00 UTC
    "catchup": False,
    "tags": ["medallion", "bronze-to-gold", "data-quality"],
}

# ========================================
# TASK FUNCTIONS
# ========================================


def bronze_to_silver_task(**context) -> dict:
    """
    Execute complete Bronze to Silver transformation.

    Args:
        **context: Airflow task context

    Returns:
        Pipeline execution results
    """
    execution_id = context["execution_date"].strftime("%Y%m%d_%H%M%S")

    transformer = BronzeToSilverTransformer(execution_id=execution_id)
    results = transformer.run_full_pipeline()
    context["task_instance"].xcom_push(key="bronze_pipeline_results", value=results)

    return results


def silver_to_gold_task(**context) -> dict:
    """
    Execute complete Silver to Gold transformation.

    Args:
        **context: Airflow task context

    Returns:
        Pipeline execution results
    """
    execution_id = context["execution_date"].strftime("%Y%m%d_%H%M%S")

    transformer = SilverToGoldTransformer(execution_id=execution_id)
    results = transformer.run_full_pipeline()
    context["task_instance"].xcom_push(key="silver_pipeline_results", value=results)

    return results


# ========================================
# DAG INSTANTIATION
# ========================================

dag = DAG(**DAG_CONFIG)

bronze_to_silver = PythonOperator(
    task_id="bronze_to_silver_transformation",
    python_callable=bronze_to_silver_task,
    dag=dag,
)

silver_to_gold = PythonOperator(
    task_id="silver_to_gold_transformation",
    python_callable=silver_to_gold_task,
    dag=dag,
)

chain(bronze_to_silver, silver_to_gold)
