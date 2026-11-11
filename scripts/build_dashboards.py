"""Build the three provisioned Grafana dashboards from local panel definitions."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1] / "grafana" / "dashboards"
PROMETHEUS = {"type": "prometheus", "uid": "prometheus"}
POSTGRES = {"type": "postgres", "uid": "postgres"}


def panel(
    identifier: int,
    title: str,
    expression: str,
    row: int,
    column: int,
    *,
    kind: str = "timeseries",
    source: dict = PROMETHEUS,
    unit: str | None = None,
    legend: str | None = None,
    width: int = 12,
    height: int = 8,
) -> dict:
    target = {"refId": "A", "expr": expression} if source == PROMETHEUS else {"refId": "A", "rawSql": expression, "format": "time_series" if kind == "timeseries" else "table"}
    if legend and source == PROMETHEUS:
        target["legendFormat"] = legend
    result = {
        "id": identifier,
        "title": title,
        "type": kind,
        "datasource": source,
        "gridPos": {"h": height, "w": width, "x": column, "y": row},
        "targets": [target],
        "fieldConfig": {"defaults": {"unit": unit or "short"}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}},
    }
    if kind == "barchart":
        result["options"] = {"orientation": "horizontal", "showValue": "auto", "xTickLabelRotation": 0}
    return result


def series_panel(identifier: int, title: str, series: list[tuple[str, str]], row: int, column: int) -> dict:
    result = panel(identifier, title, "", row, column, unit="reqps")
    result["targets"] = [
        {"refId": chr(65 + index), "expr": expression, "legendFormat": label}
        for index, (expression, label) in enumerate(series)
    ]
    return result


def score_distribution(identifier: int, title: str, metric: str, row: int, column: int) -> dict:
    series = []
    previous = None
    for bound, label in (
        ("0.05", "0-0.05"),
        ("0.2", "0.05-0.2"),
        ("0.6", "0.2-0.6"),
        ("0.98", "0.6-0.98"),
        ("1.0", "0.98-1"),
    ):
        current = f'sum(rate({metric}_bucket{{le="{bound}"}}[1m]))'
        series.append((current if previous is None else f"{current} - {previous}", label))
        previous = current
    return series_panel(identifier, title, series, row, column)


def dashboard(uid: str, title: str, panels: list[dict], *, variables: list[dict] | None = None, annotations: list[dict] | None = None) -> dict:
    return {
        "uid": uid,
        "title": title,
        "schemaVersion": 39,
        "version": 1,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "timezone": "browser",
        "tags": ["fraud", "monitoring"],
        "panels": panels,
        "templating": {"list": variables or []},
        "annotations": {"list": annotations or []},
    }


def variable(name: str, label: str, query: str) -> dict:
    return {
        "name": name,
        "label": label,
        "type": "query",
        "datasource": POSTGRES,
        "query": query,
        "refresh": 1,
        "includeAll": True,
        "allValue": "'%'",
        "multi": False,
        "current": {"text": "All", "value": "$__all"},
        "sort": 1,
    }


def main() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    scoring = dashboard(
        "fraud-scoring",
        "Обработка транзакций",
        [
            panel(1, "Транзакции в секунду", "rate(fraud_scoring_transactions_total[1m])", 0, 0, unit="reqps", legend="Транзакций/с"),
            panel(2, "Время обработки, P95", "histogram_quantile(0.95, sum by (le) (rate(fraud_scoring_duration_seconds_bucket[1m])))", 0, 12, unit="s", legend="P95"),
            panel(3, "Доля мошеннических транзакций", "fraud_scoring_fraud_share", 8, 0, unit="percentunit", legend="Доля мошеннических транзакций"),
            score_distribution(4, "Распределение скоров мошенничества во времени", "fraud_scoring_score", 8, 12),
            series_panel(5, "Состав потока по сумме", [
                (f'sum(rate(fraud_scoring_amount_bucket_total{{bucket="{bucket}"}}[1m]))', label)
                for bucket, label in (
                    ("SMALL", "До 20"),
                    ("MEDIUM", "20-100"),
                    ("LARGE", "100-500"),
                    ("VERY_LARGE", "От 500"),
                )
            ], 16, 0),
            panel(6, "Неверные сообщения", "rate(fraud_scoring_invalid_messages_total[1m])", 16, 12, unit="reqps", legend="Сообщений/с"),
        ],
        annotations=[{
            "name": "Смена модели",
            "enable": True,
            "datasource": PROMETHEUS,
            "expr": "fraud_scoring_model_loads_total unless fraud_scoring_model_loads_total offset 30s",
            "iconColor": "#d97a28",
            "tagKeys": "version",
            "titleFormat": "Модель {{version}}",
            "textFormat": "Скоринг перешёл на {{version}}",
        }],
    )
    writer = dashboard(
        "fraud-writer",
        "Запись результатов",
        [
            panel(1, "Скорость записи скорингов в БД", "rate(fraud_writer_scores_total[1m])", 0, 0, unit="reqps", legend="Записей/с"),
            panel(2, "Время записи, P95", "histogram_quantile(0.95, sum by (le) (rate(fraud_writer_duration_seconds_bucket[1m])))", 0, 12, unit="s", legend="P95"),
            panel(3, "Всего выявлено мошеннических транзакций", "fraud_writer_fraud_total", 8, 0, legend="Фродовых транзакций"),
            score_distribution(4, "Распределение записанных скоров во времени", "fraud_writer_score", 8, 12),
            panel(5, "Скоры по версиям модели", "SELECT model_version, count(*) AS scores, round(100.0 * avg(fraud_flag), 2) AS fraud_share_pct FROM transaction_scores GROUP BY model_version ORDER BY model_version", 16, 0, kind="table", source=POSTGRES, width=24),
        ],
    )
    states = variable("state", "Штат", "SELECT DISTINCT us_state FROM transaction_scores WHERE us_state <> '' ORDER BY us_state")
    merchants = variable("merchant", "Продавец", "SELECT DISTINCT merch FROM transaction_scores WHERE merch <> '' ORDER BY merch")
    condition = "us_state LIKE ${state:sqlstring} AND merch LIKE ${merchant:sqlstring}"
    system = dashboard(
        "fraud-system",
        "Система и срез транзакций",
        [
            panel(1, "CPU Utilization", "100 * (1 - avg(rate(node_cpu_seconds_total{mode=\"idle\"}[1m])))", 0, 0, unit="percent"),
            panel(2, "Memory Utilization", "node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes", 0, 12, unit="bytes"),
            panel(3, "Network Traffic", "sum by (device) (rate(node_network_receive_bytes_total[1m]) + rate(node_network_transmit_bytes_total[1m]))", 8, 0, unit="Bps"),
            panel(4, "Disk I/O", "sum by (device) (rate(node_disk_read_bytes_total[1m]) + rate(node_disk_written_bytes_total[1m]))", 8, 12, unit="Bps"),
            panel(5, "Плотность скоров по диапазонам", f"WITH bucketed AS (SELECT LEAST(floor(score * 20), 19) / 20.0 AS bucket FROM transaction_scores WHERE {condition} AND $__timeFilter(created_at)) SELECT to_char(bucket::numeric, 'FM0.00') || '-' || to_char((bucket + 0.05)::numeric, 'FM0.00') AS score_range, count(*)::double precision / (sum(count(*)) OVER () * 0.05) AS density FROM bucketed GROUP BY bucket ORDER BY bucket", 16, 0, kind="barchart", source=POSTGRES),
            panel(6, "TPS выбранных транзакций", f"SELECT date_bin('10 seconds', created_at, TIMESTAMPTZ '2000-01-01') AS time, count(*) / 10.0 AS tps FROM transaction_scores WHERE {condition} AND $__timeFilter(created_at) GROUP BY 1 ORDER BY 1", 16, 12, source=POSTGRES, unit="reqps"),
            panel(7, "Доля фрода по категориям, последние 1000", "WITH recent AS (SELECT cat_id, fraud_flag FROM transaction_scores ORDER BY created_at DESC LIMIT 1000) SELECT cat_id, round(100.0 * avg(fraud_flag), 2) AS fraud_share_pct FROM recent GROUP BY cat_id ORDER BY fraud_share_pct DESC", 24, 0, kind="barchart", source=POSTGRES),
            panel(8, "Средний скор по категориям", f"SELECT cat_id, round(avg(score)::numeric, 3) AS average_score FROM transaction_scores WHERE {condition} AND $__timeFilter(created_at) GROUP BY cat_id ORDER BY average_score DESC", 24, 12, kind="barchart", source=POSTGRES),
        ],
        variables=[states, merchants],
    )
    for name, document in (("scoring", scoring), ("writer", writer), ("system", system)):
        (ROOT / f"{name}.json").write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
