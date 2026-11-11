"""Expose transaction scoring and model reload metrics to Prometheus."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server


PROCESSED = Counter("fraud_scoring_transactions_total", "Successfully scored transactions")
LATENCY = Histogram(
    "fraud_scoring_duration_seconds",
    "Time spent scoring and publishing a transaction",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
SCORES = Histogram(
    "fraud_scoring_score",
    "Distribution of transaction scores",
    buckets=(0.01, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 0.9, 0.95, 0.98, 0.99, 1),
)
FRAUD = Counter("fraud_scoring_fraud_total", "Transactions flagged as fraud")
FRAUD_SHARE = Gauge("fraud_scoring_fraud_share", "Cumulative share of flagged transactions")
MODEL_RELOADS = Counter("fraud_scoring_model_loads_total", "Model versions loaded by the scorer", ["version"])
AMOUNT_BUCKETS = Counter(
    "fraud_scoring_amount_bucket_total",
    "Transaction count by amount segment for monitoring traffic composition",
    ["bucket"],
)
INVALID = Counter("fraud_scoring_invalid_messages_total", "Rejected transaction messages")


def start(port: int) -> None:
    start_http_server(port)
