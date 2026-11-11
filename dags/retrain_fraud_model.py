"""DAG that trains, evaluates and conditionally publishes a fraud model."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from ml_pipeline.training_tasks import (
    advance_training_state,
    evaluate_candidate,
    prepare_training_data,
    publish_candidate,
    resolve_training_window,
    train_candidate,
)


with DAG(
    dag_id="retrain_fraud_model",
    description="Time-split retraining with a production quality gate",
    start_date=datetime(2025, 1, 1),
    schedule="1-59/5 * * * *",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["fraud", "ml"],
) as dag:
    resolve = PythonOperator(
        task_id="resolve_training_window",
        python_callable=resolve_training_window,
    )
    prepare = PythonOperator(
        task_id="prepare_training_data",
        python_callable=prepare_training_data,
        op_kwargs={"plan": "{{ ti.xcom_pull(task_ids='resolve_training_window') }}"},
    )
    train = PythonOperator(
        task_id="train_candidate",
        python_callable=train_candidate,
        op_kwargs={"dataset": "{{ ti.xcom_pull(task_ids='prepare_training_data') }}"},
    )
    evaluate = PythonOperator(
        task_id="evaluate_candidate",
        python_callable=evaluate_candidate,
        op_kwargs={"candidate": "{{ ti.xcom_pull(task_ids='train_candidate') }}"},
    )
    publish = PythonOperator(
        task_id="publish_candidate",
        python_callable=publish_candidate,
        op_kwargs={"evaluation": "{{ ti.xcom_pull(task_ids='evaluate_candidate') }}"},
    )
    advance = PythonOperator(
        task_id="advance_training_state",
        python_callable=advance_training_state,
        op_kwargs={"publication": "{{ ti.xcom_pull(task_ids='publish_candidate') }}"},
    )

    resolve >> prepare >> train >> evaluate >> publish >> advance
