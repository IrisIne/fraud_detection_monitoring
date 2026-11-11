"""DAG that appends one labeled month and enforces the retention window."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from ml_pipeline.data_tasks import append_month, prune_history, resolve_next_month


with DAG(
    dag_id="maintain_labeled_data",
    description="Append the next labeled month without moving the training pointer",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["fraud", "data"],
) as dag:
    resolve = PythonOperator(
        task_id="resolve_next_month",
        python_callable=resolve_next_month,
    )
    append = PythonOperator(
        task_id="append_month",
        python_callable=append_month,
        op_kwargs={"plan": "{{ ti.xcom_pull(task_ids='resolve_next_month') }}"},
    )
    prune = PythonOperator(
        task_id="prune_history",
        python_callable=prune_history,
        op_kwargs={"plan": "{{ ti.xcom_pull(task_ids='append_month') }}"},
    )

    resolve >> append >> prune
