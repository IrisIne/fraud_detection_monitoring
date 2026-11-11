"""Tests for the versioned scoring result contract."""

from __future__ import annotations

import unittest
from uuid import uuid4

from score_writer.app.main import InvalidScore, validate


class ScoreContractTest(unittest.TestCase):
    def test_score_contract_includes_model_version(self) -> None:
        transaction_id = str(uuid4())
        values = validate(
            {
                "transaction_id": transaction_id,
                "score": 0.9,
                "fraud_flag": 0,
                "model_version": "model_201904",
            }
        )
        self.assertEqual(values, (transaction_id, 0.9, 0, "model_201904", "", "", ""))

    def test_score_contract_rejects_missing_version(self) -> None:
        with self.assertRaises(InvalidScore):
            validate({"transaction_id": str(uuid4()), "score": 0.1, "fraud_flag": 0})


if __name__ == "__main__":
    unittest.main()
