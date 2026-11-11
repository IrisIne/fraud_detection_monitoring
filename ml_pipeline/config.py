"""Environment-backed settings shared by Airflow task modules."""

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    database_dsn: str = os.getenv(
        "FRAUD_DATABASE_DSN",
        "dbname=fraud_detection user=pipeline_user password=pipeline_password host=postgres port=5432",
    )
    seed_csv: str = os.getenv("TRAIN_SEED_CSV", "/opt/airflow/data/train_seed.csv")
    model_dir: str = os.getenv("MODEL_DIR", "/opt/airflow/models")
    report_dir: str = os.getenv("REPORT_DIR", "/opt/airflow/reports")
    work_dir: str = os.getenv("WORK_DIR", "/opt/airflow/work")
    training_window_months: int = int(os.getenv("TRAINING_WINDOW_MONTHS", "3"))
    batch_size: int = int(os.getenv("LABELED_BATCH_SIZE", "5000"))
    history_months: int = int(os.getenv("HISTORY_MONTHS", "12"))
    fraud_threshold: float = float(os.getenv("FRAUD_THRESHOLD", "0.98"))
