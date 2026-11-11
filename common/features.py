"""Feature engineering shared by the Airflow pipeline and scoring service."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import numpy as np
import pandas as pd


RAW_COLUMNS = [
    "transaction_time",
    "merch",
    "cat_id",
    "amount",
    "name_1",
    "name_2",
    "gender",
    "street",
    "one_city",
    "us_state",
    "post_code",
    "lat",
    "lon",
    "population_city",
    "jobs",
    "merchant_lat",
    "merchant_lon",
]
CATEGORICAL_FEATURES = ["merch", "cat_id", "gender", "one_city", "us_state", "jobs"]
NUMERIC_FEATURES = [
    "amount_log",
    "population_log",
    "distance_km_log",
    "hour_sin",
    "hour_cos",
    "weekday_sin",
    "weekday_cos",
    "is_weekend",
]
MODEL_FEATURES = CATEGORICAL_FEATURES + NUMERIC_FEATURES
EARTH_RADIUS_KM = 6371.009


class FeatureValidationError(ValueError):
    """Raised when raw transactions cannot be converted to model features."""

    pass


def build_features(records: pd.DataFrame | Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    """Validate raw transactions and return deterministic model features."""

    frame = records.copy() if isinstance(records, pd.DataFrame) else pd.DataFrame(records)
    missing = [column for column in RAW_COLUMNS if column not in frame.columns]
    if missing:
        raise FeatureValidationError(f"missing required fields: {', '.join(missing)}")

    timestamps = pd.to_datetime(frame["transaction_time"], errors="coerce")
    if timestamps.isna().any():
        raise FeatureValidationError("transaction_time contains invalid values")

    result = pd.DataFrame(index=frame.index)
    for column in CATEGORICAL_FEATURES:
        result[column] = frame[column].fillna("__missing__").astype(str)

    amount = _non_negative(frame["amount"], "amount")
    population = _non_negative(frame["population_city"], "population_city")
    distance = _haversine_distance(frame)
    result["amount_log"] = np.log1p(amount)
    result["population_log"] = np.log1p(population)
    result["distance_km_log"] = np.log1p(distance)

    hour = timestamps.dt.hour.astype(float)
    weekday = timestamps.dt.dayofweek.astype(float)
    result["hour_sin"] = np.sin(2.0 * np.pi * hour / 24.0)
    result["hour_cos"] = np.cos(2.0 * np.pi * hour / 24.0)
    result["weekday_sin"] = np.sin(2.0 * np.pi * weekday / 7.0)
    result["weekday_cos"] = np.cos(2.0 * np.pi * weekday / 7.0)
    result["is_weekend"] = (weekday >= 5).astype(float)
    return result[MODEL_FEATURES]


def _non_negative(values: pd.Series, name: str) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    if numeric.isna().any():
        numeric = numeric.fillna(numeric.median())
    if numeric.isna().any() or (numeric < 0).any():
        raise FeatureValidationError(f"{name} must contain non-negative numeric values")
    return numeric.astype(float)


def _haversine_distance(frame: pd.DataFrame) -> pd.Series:
    coordinates = frame[["lat", "lon", "merchant_lat", "merchant_lon"]].apply(
        pd.to_numeric,
        errors="coerce",
    )
    coordinates = coordinates.fillna(coordinates.median())
    if coordinates.isna().any().any():
        raise FeatureValidationError("coordinates must contain numeric values")
    lat1, lon1, lat2, lon2 = [np.radians(coordinates[column]) for column in coordinates]
    delta_lat = lat2 - lat1
    delta_lon = lon2 - lon1
    haversine = (
        np.sin(delta_lat / 2.0) ** 2
        + np.cos(lat1) * np.cos(lat2) * np.sin(delta_lon / 2.0) ** 2
    ).clip(0.0, 1.0)
    return 2.0 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(haversine))
