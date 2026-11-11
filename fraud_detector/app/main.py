"""Consume transactions, score them and publish versioned results to Kafka."""

from __future__ import annotations

import json
import logging
import signal
import time
from uuid import UUID

from confluent_kafka import Consumer, KafkaError, Producer

from common.features import FeatureValidationError
from .config import Settings
from .model_registry import HotReloadScorer, ModelRegistryError
from . import metrics


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOGGER = logging.getLogger("fraud_detector")


def run() -> None:
    settings = Settings()
    metrics.start(8000)
    scorer = HotReloadScorer(settings.model_dir, settings.fraud_threshold)
    metrics.MODEL_RELOADS.labels(version=scorer.version).inc()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    consumer.subscribe([settings.transactions_topic])
    running = True
    processed_count = 0
    fraud_count = 0

    def stop(_signum, _frame) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    LOGGER.info("scoring started with model_version=%s", scorer.version)
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
                payload = json.loads(message.value().decode("utf-8"))
                transaction_id = str(UUID(str(payload["transaction_id"])))
                transaction = payload["data"]
                if not isinstance(transaction, dict):
                    raise ValueError("data must be an object")
                if scorer.reload_if_needed():
                    metrics.MODEL_RELOADS.labels(version=scorer.version).inc()
                score, fraud_flag, model_version = scorer.predict(transaction)
                amount = float(transaction["amount"])
                bucket = "SMALL" if amount < 20 else "MEDIUM" if amount < 100 else "LARGE" if amount < 500 else "VERY_LARGE"
                result = {
                    "transaction_id": transaction_id,
                    "score": score,
                    "fraud_flag": fraud_flag,
                    "model_version": model_version,
                    "us_state": str(transaction.get("us_state") or "").strip().upper(),
                    "merch": str(transaction.get("merch") or "").strip(),
                    "cat_id": str(transaction.get("cat_id") or "").strip().lower(),
                }
                producer.produce(
                    settings.scores_topic,
                    key=transaction_id,
                    value=json.dumps(result).encode("utf-8"),
                )
                producer.flush(10)
                consumer.commit(message=message, asynchronous=False)
                metrics.PROCESSED.inc()
                processed_count += 1
                metrics.LATENCY.observe(time.perf_counter() - started)
                metrics.SCORES.observe(score)
                if fraud_flag:
                    metrics.FRAUD.inc()
                    fraud_count += 1
                metrics.FRAUD_SHARE.set(fraud_count / processed_count)
                metrics.AMOUNT_BUCKETS.labels(bucket=bucket).inc()
                LOGGER.info(
                    "scored transaction_id=%s score=%.6f model_version=%s",
                    transaction_id,
                    score,
                    model_version,
                )
            except (json.JSONDecodeError, UnicodeDecodeError, KeyError, TypeError, ValueError, FeatureValidationError):
                metrics.INVALID.inc()
                LOGGER.exception("invalid transaction; message skipped")
                consumer.commit(message=message, asynchronous=False)
            except ModelRegistryError:
                LOGGER.exception("model reload failed; offset remains uncommitted")
    finally:
        producer.flush(10)
        consumer.close()


if __name__ == "__main__":
    run()
