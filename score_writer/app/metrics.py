"""Expose database write metrics to Prometheus."""

from prometheus_client import Counter, Histogram, start_http_server


WRITTEN = Counter("fraud_writer_scores_total", "Scores written to PostgreSQL")
LATENCY = Histogram(
    "fraud_writer_duration_seconds",
    "Time spent writing a score to PostgreSQL",
    buckets=(0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2),
)
FRAUD = Counter("fraud_writer_fraud_total", "Written scores flagged as fraud")
SCORES = Histogram(
    "fraud_writer_score",
    "Distribution of scores written to PostgreSQL",
    buckets=(0.01, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 0.9, 0.95, 0.98, 0.99, 1),
)
ERRORS = Counter("fraud_writer_errors_total", "Failed database writes")


def start(port: int) -> None:
    start_http_server(port)
