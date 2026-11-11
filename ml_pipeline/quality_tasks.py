"""Task functions for dataset quality checks and standalone HTML reporting."""

from __future__ import annotations

from datetime import datetime, timezone
from html import escape
import os
from pathlib import Path

import pandas as pd

from .config import Settings
from .db import connect


def build_quality_report() -> dict[str, object]:
    """Read the showcase, write a timestamped report and atomically replace latest.html."""

    settings = Settings()
    with connect() as database:
        frame = pd.read_sql_query(
            """
            SELECT transaction_id, transaction_time, amount, target, us_state,
                   cat_id, lat, lon, merchant_lat, merchant_lon, batch_month
            FROM labeled_transactions
            ORDER BY transaction_time
            """,
            database,
        )

    months = (
        sorted(pd.to_datetime(frame["batch_month"]).dt.to_period("M").unique())
        if not frame.empty
        else []
    )
    continuous = all(right.ordinal - left.ordinal == 1 for left, right in zip(months, months[1:]))
    duplicates = int(frame["transaction_id"].duplicated().sum()) if not frame.empty else 0
    invalid_targets = int((~frame["target"].isin([0, 1])).sum()) if not frame.empty else 0
    invalid_amounts = (
        int((frame["amount"].isna() | (frame["amount"] < 0)).sum())
        if not frame.empty
        else 0
    )
    missing_timestamps = int(frame["transaction_time"].isna().sum()) if not frame.empty else 0
    missing_categories = (
        int(frame[["cat_id", "us_state"]].isna().sum().sum()) if not frame.empty else 0
    )
    checks = [
        ("Витрина не пуста", len(frame) > 0, f"строк: {len(frame)}"),
        (
            "transaction_id уникален",
            frame["transaction_id"].is_unique if not frame.empty else False,
            f"дубликатов: {duplicates}",
        ),
        (
            "target принимает значения 0 или 1",
            bool(frame["target"].isin([0, 1]).all()) if not frame.empty else False,
            f"некорректных: {invalid_targets}",
        ),
        (
            "amount заполнен и неотрицателен",
            bool((frame["amount"].notna() & (frame["amount"] >= 0)).all()) if not frame.empty else False,
            f"нарушений: {invalid_amounts}",
        ),
        (
            "transaction_time заполнен",
            bool(frame["transaction_time"].notna().all()) if not frame.empty else False,
            f"пропусков: {missing_timestamps}",
        ),
        (
            "месяцы идут без разрывов",
            bool(months) and continuous,
            ", ".join(str(month) for month in months),
        ),
        (
            "координаты находятся в допустимых диапазонах",
            bool(
                frame["lat"].between(-90, 90).all()
                and frame["merchant_lat"].between(-90, 90).all()
                and frame["lon"].between(-180, 180).all()
                and frame["merchant_lon"].between(-180, 180).all()
            ) if not frame.empty else False,
            "lat: [-90, 90], lon: [-180, 180]",
        ),
        (
            "cat_id и us_state заполнены",
            bool(frame[["cat_id", "us_state"]].notna().all().all()) if not frame.empty else False,
            f"пропусков: {missing_categories}",
        ),
    ]
    generated_at = datetime.now(timezone.utc)
    rows = "\n".join(
        f"<tr><td>{escape(name)}</td><td>{'PASS' if passed else 'FAIL'}</td><td>{escape(details)}</td></tr>"
        for name, passed, details in checks
    )
    html = f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><title>Data Quality Report</title>
<style>body{{font-family:Arial,sans-serif;max-width:960px;margin:40px auto;color:#222}}
table{{border-collapse:collapse;width:100%}}th,td{{padding:10px;border:1px solid #ccc;text-align:left}}
th{{background:#f3f3f3}}</style></head><body>
<h1>Data Quality Report</h1><p>Сформирован: {generated_at.isoformat()}</p>
<table><thead><tr><th>Правило</th><th>Результат</th><th>Детали</th></tr></thead>
<tbody>{rows}</tbody></table></body></html>"""
    report_dir = Path(settings.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_name = f"dq_report_{generated_at.strftime('%Y%m%dT%H%M%SZ')}.html"
    report_path = report_dir / report_name
    report_path.write_text(html, encoding="utf-8")
    temporary = report_dir / "latest.html.tmp"
    temporary.write_text(html, encoding="utf-8")
    os.replace(temporary, report_dir / "latest.html")
    failed = [name for name, passed, _ in checks if not passed]
    return {"report_path": str(report_path), "failed_checks": failed, "checks": len(checks)}


def enforce_quality(report: dict[str, object]) -> dict[str, object]:
    """Fail the DAG after the HTML report is saved when any quality rule is violated."""

    failed = list(report["failed_checks"])
    if failed:
        raise ValueError(f"data quality checks failed: {', '.join(failed)}")
    return report
