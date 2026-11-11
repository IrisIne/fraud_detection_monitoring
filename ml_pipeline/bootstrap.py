"""Load the seed showcase and publish the initial model for local startup."""

from __future__ import annotations

from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import pandas as pd
from psycopg2.extras import execute_values

from common.features import RAW_COLUMNS
from .config import Settings
from .db import connect
from .modeling import evaluate_bundle, publish_bundle, save_bundle, train_bundle


INSERT_COLUMNS = ["transaction_id", "source_key", *RAW_COLUMNS, "target", "batch_month"]


def load_seed(frame: pd.DataFrame) -> None:
    """Insert deterministic seed rows; conflicts make repeated startup harmless."""

    rows = []
    for index, record in frame.iterrows():
        source_key = f"seed:{index}:{record['transaction_time']}"
        transaction_id = str(uuid5(NAMESPACE_URL, source_key))
        values = [transaction_id, source_key]
        values.extend(record[column] for column in RAW_COLUMNS)
        values.extend([int(record["target"]), record["transaction_time"].date().replace(day=1)])
        rows.append(tuple(values))
    query = f"""
        INSERT INTO labeled_transactions ({', '.join(INSERT_COLUMNS)})
        VALUES %s
        ON CONFLICT (transaction_id) DO NOTHING
    """
    with connect() as database, database.cursor() as cursor:
        execute_values(cursor, query, rows, page_size=1000)


def bootstrap_model(frame: pd.DataFrame) -> None:
    """Create an initial time-split model unless the registry is already initialized."""

    settings = Settings()
    latest = Path(settings.model_dir) / "latest.json"
    with connect() as database, database.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pipeline_state WHERE state_id = 1")
        state_exists = cursor.fetchone() is not None
    if state_exists and latest.exists():
        return

    months = sorted(frame["transaction_time"].dt.to_period("M").unique())
    if len(months) < settings.training_window_months:
        raise ValueError("seed data must contain at least three months")
    validation_month = months[-1]
    train = frame[frame["transaction_time"].dt.to_period("M") < validation_month]
    test = frame[frame["transaction_time"].dt.to_period("M") == validation_month]
    version = f"model_{validation_month.strftime('%Y%m')}_bootstrap"
    bundle = train_bundle(train, version, settings.fraud_threshold)
    metrics = evaluate_bundle(bundle, test)
    candidate = Path(settings.work_dir) / f"{version}.joblib"
    save_bundle(bundle, candidate)
    publish_bundle(candidate, settings.model_dir, metrics)

    last_month = validation_month.to_timestamp().date()
    with connect() as database, database.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO pipeline_state (
                state_id, last_trained_month, model_version, validation_roc_auc
            ) VALUES (1, %s, %s, %s)
            ON CONFLICT (state_id) DO UPDATE SET
                last_trained_month = EXCLUDED.last_trained_month,
                model_version = EXCLUDED.model_version,
                validation_roc_auc = EXCLUDED.validation_roc_auc,
                updated_at = NOW()
            """,
            (last_month, version, metrics["roc_auc"]),
        )


def main() -> None:
    settings = Settings()
    frame = pd.read_csv(settings.seed_csv, parse_dates=["transaction_time"])
    load_seed(frame)
    bootstrap_model(frame)
    print("pipeline bootstrap completed")


if __name__ == "__main__":
    main()
