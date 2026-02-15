from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingestion.fetch_data import run_ingestion
from transformation.clean_data import transform, load
from quality.checks import run_quality_checks


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="etl_weather_pipeline",
    description="End-to-end ingestion -> transform -> quality -> load -> dbt (Open-Meteo Archive)",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["pipeline", "weather", "dbt"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=run_ingestion,
    )

    def _transform_callable(ti):
        batch_id = ti.xcom_pull(task_ids="ingest")
        return transform(batch_id=batch_id)

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=_transform_callable,
    )

    def _quality_callable(ti):
        batch_id = ti.xcom_pull(task_ids="ingest")
        parquet_path = ti.xcom_pull(task_ids="transform")
        return run_quality_checks(batch_id=batch_id, parquet_path=parquet_path)

    quality = PythonOperator(
        task_id="quality_checks",
        python_callable=_quality_callable,
    )

    def _load_callable(ti):
        parquet_path = ti.xcom_pull(task_ids="transform")
        return load(parquet_path=parquet_path)

    load_task = PythonOperator(
        task_id="load",
        python_callable=_load_callable,
    )

    dbt_project_dir = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
    dbt_profiles_dir = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt")

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {dbt_project_dir} --profiles-dir {dbt_profiles_dir}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {dbt_project_dir} --profiles-dir {dbt_profiles_dir}",
    )

    ingest >> transform_task >> quality >> load_task >> dbt_run >> dbt_test
