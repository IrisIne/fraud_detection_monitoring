"""Tests for the custom transaction feature engineering."""

from __future__ import annotations

import pandas as pd
import unittest

from common.features import MODEL_FEATURES, FeatureValidationError, build_features


def transaction() -> dict[str, object]:
    return {
        "transaction_time": "2019-03-15 23:30",
        "merch": "merchant",
        "cat_id": "grocery",
        "amount": 125.5,
        "name_1": "Alex",
        "name_2": "Smith",
        "gender": "F",
        "street": "Main Street",
        "one_city": "Portland",
        "us_state": "OR",
        "post_code": "97035",
        "lat": 45.5,
        "lon": -122.6,
        "population_city": 500000,
        "jobs": "Engineer",
        "merchant_lat": 45.6,
        "merchant_lon": -122.7,
    }


class FeatureEngineeringTest(unittest.TestCase):
    def test_build_features_has_stable_contract(self) -> None:
        features = build_features(pd.DataFrame([transaction()]))
        self.assertEqual(list(features.columns), MODEL_FEATURES)
        self.assertGreater(features.loc[0, "distance_km_log"], 0)
        self.assertIn(features.loc[0, "is_weekend"], (0.0, 1.0))

    def test_build_features_rejects_missing_columns(self) -> None:
        record = transaction()
        del record["amount"]
        with self.assertRaisesRegex(FeatureValidationError, "amount"):
            build_features([record])


if __name__ == "__main__":
    unittest.main()
