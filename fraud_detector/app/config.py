"""Environment-backed settings for online scoring."""

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    transactions_topic: str = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
    scores_topic: str = os.getenv("KAFKA_SCORES_TOPIC", "scores")
    consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "fraud-detector-airflow-v1")
    model_dir: str = os.getenv("MODEL_DIR", "/models")
    fraud_threshold: float = float(os.getenv("FRAUD_THRESHOLD", "0.98"))
