"""Persist versioned Kafka scoring results in PostgreSQL."""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from uuid import UUID

from confluent_kafka import Consumer, KafkaError
import psycopg2
from . import metrics


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOGGER = logging.getLogger("score_writer")

UPSERT_SQL = """
INSERT INTO transaction_scores (transaction_id, score, fraud_flag, model_version, us_state, merch, cat_id)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (transaction_id) DO UPDATE SET
    score = EXCLUDED.score,
    fraud_flag = EXCLUDED.fraud_flag,
    model_version = EXCLUDED.model_version,
    us_state = EXCLUDED.us_state,
    merch = EXCLUDED.merch,
    cat_id = EXCLUDED.cat_id,
    created_at = NOW()
"""


class InvalidScore(ValueError):
    """Raised when a Kafka score message violates the storage contract."""

    pass


def validate(payload: object) -> tuple[str, float, int, str, str, str, str]:
    if not isinstance(payload, dict):
        raise InvalidScore("score message must be an object")
    try:
        transaction_id = str(UUID(str(payload["transaction_id"])))
        score = float(payload["score"])
        fraud_flag = int(payload["fraud_flag"])
        model_version = str(payload["model_version"])
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidScore("invalid score message fields") from exc
    if not 0.0 <= score <= 1.0 or fraud_flag not in (0, 1) or not model_version:
        raise InvalidScore("invalid score, fraud_flag or model_version")
    return (
        transaction_id, score, fraud_flag, model_version,
        str(payload.get("us_state") or "").strip().upper(),
        str(payload.get("merch") or "").strip(),
        str(payload.get("cat_id") or "").strip().lower(),
    )


def connect_with_retry():
    dsn = os.environ["FRAUD_DATABASE_DSN"]
    for attempt in range(1, 31):
        try:
            return psycopg2.connect(dsn)
        except psycopg2.OperationalError:
            if attempt == 30:
                raise
            time.sleep(min(attempt, 5))
    raise RuntimeError("database connection retry exhausted")


def run() -> None:
    metrics.start(8001)
    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            "group.id": os.getenv("KAFKA_CONSUMER_GROUP", "score-writer-airflow-v1"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([os.getenv("KAFKA_SCORES_TOPIC", "scores")])
    database = connect_with_retry()
    running = True

    def stop(_signum, _frame) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    LOGGER.error("Kafka consumer error: %s", message.error())
                continue
            try:
                started = time.perf_counter()
                values = validate(json.loads(message.value().decode("utf-8")))
                with database.cursor() as cursor:
                    cursor.execute(UPSERT_SQL, values)
                database.commit()
                consumer.commit(message=message, asynchronous=False)
                metrics.WRITTEN.inc()
                metrics.LATENCY.observe(time.perf_counter() - started)
                metrics.SCORES.observe(values[1])
                if values[2]:
                    metrics.FRAUD.inc()
            except (json.JSONDecodeError, UnicodeDecodeError, InvalidScore):
                LOGGER.exception("invalid score; message skipped")
                consumer.commit(message=message, asynchronous=False)
            except psycopg2.Error:
                metrics.ERRORS.inc()
                LOGGER.exception("database write failed; offset remains uncommitted")
                database.rollback()
    finally:
        consumer.close()
        database.close()


if __name__ == "__main__":
    run()
