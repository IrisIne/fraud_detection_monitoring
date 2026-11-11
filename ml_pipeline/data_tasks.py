"""Task functions for idempotent monthly labeled-data maintenance."""

from __future__ import annotations

from datetime import date
from uuid import NAMESPACE_URL, uuid5

import pandas as pd
from psycopg2.extras import RealDictCursor, execute_values

from common.features import RAW_COLUMNS
from .config import Settings
from .db import connect


def resolve_next_month() -> dict[str, str]:
    """Read the published-model pointer and choose its immediate next month."""

    with connect() as database, database.cursor() as cursor:
        cursor.execute("SELECT last_trained_month FROM pipeline_state WHERE state_id = 1")
        row = cursor.fetchone()
    if row is None:
        raise RuntimeError("pipeline_state is not initialized")
    source = pd.Timestamp(row[0]).to_period("M")
    target = source + 1
    return {
        "source_month": source.strftime("%Y-%m-01"),
        "target_month": target.strftime("%Y-%m-01"),
    }


def append_month(plan: dict[str, str]) -> dict[str, str | int]:
    """Shift source rows by one month and insert deterministic IDs without duplicates."""

    settings = Settings()
    with connect() as database, database.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            f"""
            SELECT source_key, {', '.join(RAW_COLUMNS)}, target
            FROM labeled_transactions
            WHERE batch_month = %s
            ORDER BY transaction_id
            LIMIT %s
            """,
            (plan["source_month"], settings.batch_size),
        )
        source_rows = cursor.fetchall()
    if not source_rows:
        raise ValueError(f"no source rows for {plan['source_month']}")

    target_month = date.fromisoformat(plan["target_month"])
    output_rows = []
    for record in source_rows:
        shifted_time = pd.Timestamp(record["transaction_time"]) + pd.DateOffset(months=1)
        source_key = str(record["source_key"])
        transaction_id = str(uuid5(NAMESPACE_URL, f"{source_key}:{target_month.isoformat()}"))
        values = [transaction_id, source_key]
        for column in RAW_COLUMNS:
            values.append(shifted_time.to_pydatetime() if column == "transaction_time" else record[column])
        values.extend([int(record["target"]), target_month])
        output_rows.append(tuple(values))

    columns = ["transaction_id", "source_key", *RAW_COLUMNS, "target", "batch_month"]
    with connect() as database, database.cursor() as cursor:
        execute_values(
            cursor,
            f"""
            INSERT INTO labeled_transactions ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (transaction_id) DO NOTHING
            """,
            output_rows,
            page_size=1000,
        )
        cursor.execute(
            "SELECT COUNT(*) FROM labeled_transactions WHERE batch_month = %s",
            (target_month,),
        )
        month_rows = int(cursor.fetchone()[0])
    return {**plan, "month_rows": month_rows}


def prune_history(plan: dict[str, str | int]) -> dict[str, str | int]:
    """Delete months outside the configured history only after the target batch exists."""

    settings = Settings()
    target = pd.Timestamp(str(plan["target_month"])).to_period("M")
    cutoff = (target - settings.history_months + 1).to_timestamp().date()
    with connect() as database, database.cursor() as cursor:
        cursor.execute(
            "DELETE FROM labeled_transactions WHERE batch_month < %s",
            (cutoff,),
        )
        deleted = cursor.rowcount
    return {**plan, "deleted_rows": deleted, "history_cutoff": cutoff.isoformat()}
