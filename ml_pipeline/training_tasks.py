"""Task functions for time-based retraining and guarded model publication."""

from __future__ import annotations

import json
from pathlib import Path

from airflow.exceptions import AirflowSkipException
import joblib
import pandas as pd

from common.features import RAW_COLUMNS
from .config import Settings
from .db import connect
from .modeling import evaluate_bundle, publish_bundle, save_bundle, train_bundle


def resolve_training_window() -> dict[str, object]:
    """Resolve a fixed window ending in the month after the last published model."""

    settings = Settings()
    with connect() as database, database.cursor() as cursor:
        cursor.execute(
            """
            SELECT last_trained_month, model_version, validation_roc_auc
            FROM pipeline_state WHERE state_id = 1
            """
        )
        state = cursor.fetchone()
    if state is None:
        raise RuntimeError("pipeline_state is not initialized")
    previous_month = pd.Timestamp(state[0]).to_period("M")
    candidate_month = previous_month + 1
    first_month = candidate_month - settings.training_window_months + 1
    return {
        "previous_month": previous_month.strftime("%Y-%m-01"),
        "candidate_month": candidate_month.strftime("%Y-%m-01"),
        "first_month": first_month.strftime("%Y-%m-01"),
        "previous_version": state[1],
        "previous_roc_auc": float(state[2]) if state[2] is not None else None,
        "candidate_version": f"model_{candidate_month.strftime('%Y%m')}",
    }


def prepare_training_data(plan: dict[str, object]) -> dict[str, object]:
    """Read the fixed window and overwrite the same time-based train and test files safely."""

    settings = Settings()
    query = f"""
        SELECT {', '.join(RAW_COLUMNS)}, target, batch_month
        FROM labeled_transactions
        WHERE batch_month BETWEEN %s AND %s
        ORDER BY transaction_time
    """
    with connect() as database:
        frame = pd.read_sql_query(
            query,
            database,
            params=(plan["first_month"], plan["candidate_month"]),
        )
    if frame.empty:
        raise AirflowSkipException("training window is empty; the next month is not available yet")
    frame["batch_month"] = pd.to_datetime(frame["batch_month"])
    candidate_month = pd.Timestamp(str(plan["candidate_month"]))
    train = frame[frame["batch_month"] < candidate_month].drop(columns="batch_month")
    test = frame[frame["batch_month"] == candidate_month].drop(columns="batch_month")
    observed_months = sorted(frame["batch_month"].dt.to_period("M").unique())
    if len(observed_months) != settings.training_window_months:
        raise AirflowSkipException(
            f"expected {settings.training_window_months} continuous months; the next month is not ready"
        )
    if train.empty or test.empty:
        raise AirflowSkipException("the newest validation month is not available yet")

    run_dir = Path(settings.work_dir) / str(plan["candidate_version"])
    run_dir.mkdir(parents=True, exist_ok=True)
    train_path = run_dir / "train.csv"
    test_path = run_dir / "test.csv"
    train.to_csv(train_path, index=False)
    test.to_csv(test_path, index=False)
    return {
        **plan,
        "train_path": str(train_path),
        "test_path": str(test_path),
        "train_rows": len(train),
        "test_rows": len(test),
    }


def train_candidate(dataset: dict[str, object]) -> dict[str, object]:
    """Train on earlier months and overwrite the candidate artifact for a safe retry."""

    settings = Settings()
    train = pd.read_csv(str(dataset["train_path"]), parse_dates=["transaction_time"])
    bundle = train_bundle(
        train,
        str(dataset["candidate_version"]),
        settings.fraud_threshold,
    )
    model_path = Path(str(dataset["train_path"])).parent / "candidate.joblib"
    save_bundle(bundle, model_path)
    return {**dataset, "candidate_path": str(model_path)}


def evaluate_candidate(candidate: dict[str, object]) -> dict[str, object]:
    """Evaluate the newest month and overwrite metrics before comparing with production."""

    bundle = joblib.load(str(candidate["candidate_path"]))
    test = pd.read_csv(str(candidate["test_path"]), parse_dates=["transaction_time"])
    metrics = evaluate_bundle(bundle, test)
    previous = candidate.get("previous_roc_auc")
    promote = previous is None or metrics["roc_auc"] >= float(previous)
    evaluation_path = Path(str(candidate["candidate_path"])).parent / "metrics.json"
    evaluation_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    return {**candidate, "metrics": metrics, "promote": promote}


def publish_candidate(evaluation: dict[str, object]) -> dict[str, object]:
    """Atomically overwrite one version only when its ROC-AUC is not worse than production."""

    if not bool(evaluation["promote"]):
        raise AirflowSkipException(
            f"candidate ROC-AUC {evaluation['metrics']['roc_auc']:.6f} is below "
            f"production ROC-AUC {float(evaluation['previous_roc_auc']):.6f}"
        )
    manifest = publish_bundle(
        str(evaluation["candidate_path"]),
        Settings().model_dir,
        dict(evaluation["metrics"]),
    )
    return {**evaluation, "manifest": manifest}


def advance_training_state(publication: dict[str, object]) -> dict[str, object]:
    """Advance the month pointer only after the model and manifest were saved successfully."""

    metrics = dict(publication["metrics"])
    with connect() as database, database.cursor() as cursor:
        cursor.execute(
            """
            UPDATE pipeline_state
            SET last_trained_month = %s,
                model_version = %s,
                validation_roc_auc = %s,
                updated_at = NOW()
            WHERE state_id = 1 AND last_trained_month = %s
            """,
            (
                publication["candidate_month"],
                publication["candidate_version"],
                metrics["roc_auc"],
                publication["previous_month"],
            ),
        )
        if cursor.rowcount != 1:
            cursor.execute(
                "SELECT last_trained_month, model_version FROM pipeline_state WHERE state_id = 1"
            )
            state = cursor.fetchone()
            already_advanced = state == (
                pd.Timestamp(str(publication["candidate_month"])).date(),
                publication["candidate_version"],
            )
            if not already_advanced:
                raise RuntimeError("training state changed concurrently; pointer was not advanced")
    return publication
