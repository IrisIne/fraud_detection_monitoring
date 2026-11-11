"""DAG that validates the labeled dataset and publishes an HTML report."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from ml_pipeline.quality_tasks import build_quality_report, enforce_quality


with DAG(
    dag_id="check_data_quality",
    description="Validate the labeled showcase and write an HTML report",
    start_date=datetime(2025, 1, 1),
    schedule="3-59/5 * * * *",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["fraud", "data-quality"],
) as dag:
    report = PythonOperator(
        task_id="build_quality_report",
        python_callable=build_quality_report,
    )
    enforce = PythonOperator(
        task_id="enforce_quality",
        python_callable=enforce_quality,
        op_kwargs={"report": "{{ ti.xcom_pull(task_ids='build_quality_report') }}"},
    )

    report >> enforce
