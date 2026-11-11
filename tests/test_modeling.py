"""Tests for training, evaluation and model registry publication."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from ml_pipeline.modeling import evaluate_bundle, publish_bundle, save_bundle, train_bundle
from tests.test_features import transaction


def dataset(rows: int = 40) -> pd.DataFrame:
    records = []
    for index in range(rows):
        record = transaction()
        record["transaction_time"] = f"2019-03-{index % 27 + 1:02d} {index % 24:02d}:00"
        record["amount"] = 20.0 + index * 3 if index % 4 else 500.0 + index
        record["target"] = int(index % 4 == 0)
        records.append(record)
    return pd.DataFrame(records)


class ModelingTest(unittest.TestCase):
    def test_model_bundle_can_be_evaluated_and_published(self) -> None:
        frame = dataset()
        bundle = train_bundle(frame, "model_test", 0.5)
        metrics = evaluate_bundle(bundle, frame)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = root / "candidate.joblib"
            registry = root / "registry"
            save_bundle(bundle, candidate)
            manifest = publish_bundle(candidate, registry, metrics)
            stored = json.loads((registry / "latest.json").read_text(encoding="utf-8"))
        self.assertGreaterEqual(metrics["roc_auc"], 0.0)
        self.assertLessEqual(metrics["roc_auc"], 1.0)
        self.assertEqual(manifest["model_version"], "model_test")
        self.assertEqual(stored["model_file"], "model_test.joblib")


if __name__ == "__main__":
    unittest.main()
