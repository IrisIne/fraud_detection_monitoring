"""Send CSV transactions to Kafka using the scoring service input contract."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from confluent_kafka import Producer
import pandas as pd


def normalized_record(row: pd.Series) -> dict[str, object]:
    record = {}
    for key, value in row.items():
        if key == "target":
            continue
        record[key] = None if pd.isna(value) else value.item() if hasattr(value, "item") else value
    record["transaction_time"] = str(record["transaction_time"])
    return record


def main() -> None:
    path = Path(os.getenv("INPUT_CSV", "/data/test_sample.csv"))
    limit = int(os.getenv("MESSAGE_LIMIT", "100"))
    interval = float(os.getenv("STREAM_INTERVAL_SECONDS", "0"))
    frame = pd.read_csv(path).to_dict("records")
    if not frame:
        raise ValueError("input CSV is empty")
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")})
    topic = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
    sent = 0
    while limit == 0 or sent < limit:
        row = pd.Series(frame[sent % len(frame)])
        transaction_id = str(uuid4())
        record = normalized_record(row)
        if interval:
            record["transaction_time"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        payload = {"transaction_id": transaction_id, "data": record}
        producer.produce(topic, key=transaction_id, value=json.dumps(payload).encode("utf-8"))
        producer.poll(0)
        sent += 1
        if interval:
            time.sleep(interval)
    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"failed to deliver {remaining} Kafka messages")
    print(f"published {sent} transactions to {topic}")


if __name__ == "__main__":
    main()
