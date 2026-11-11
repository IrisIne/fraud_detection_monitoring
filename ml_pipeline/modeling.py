"""Model construction, evaluation and atomic model registry writes."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, f1_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from common.features import CATEGORICAL_FEATURES, NUMERIC_FEATURES, build_features


def train_bundle(raw_train: pd.DataFrame, version: str, threshold: float) -> dict[str, Any]:
    """Train a lightweight CPU model with the project's feature engineering."""

    features = build_features(raw_train)
    target = raw_train["target"].astype(int)
    if target.nunique() != 2:
        raise ValueError("training data must contain both target classes")

    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("encoder", OneHotEncoder(handle_unknown="ignore", min_frequency=5)),
        ]
    )
    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    transformer = ColumnTransformer(
        transformers=[
            ("categorical", categorical_pipeline, CATEGORICAL_FEATURES),
            ("numeric", numeric_pipeline, NUMERIC_FEATURES),
        ]
    )
    pipeline = Pipeline(
        steps=[
            ("features", transformer),
            (
                "classifier",
                LogisticRegression(
                    class_weight="balanced",
                    max_iter=500,
                    random_state=42,
                    solver="liblinear",
                ),
            ),
        ]
    )
    pipeline.fit(features, target)
    return {
        "pipeline": pipeline,
        "version": version,
        "threshold": float(threshold),
        "feature_contract": "fraud_features_v1",
    }


def evaluate_bundle(bundle: dict[str, Any], raw_test: pd.DataFrame) -> dict[str, float]:
    """Evaluate a candidate on the newest month of the time-based split."""

    features = build_features(raw_test)
    target = raw_test["target"].astype(int).to_numpy()
    if np.unique(target).size != 2:
        raise ValueError("validation data must contain both target classes")
    scores = bundle["pipeline"].predict_proba(features)[:, 1]
    predictions = (scores > float(bundle["threshold"])).astype(int)
    return {
        "roc_auc": float(roc_auc_score(target, scores)),
        "average_precision": float(average_precision_score(target, scores)),
        "f1": float(f1_score(target, predictions, zero_division=0)),
        "rows": float(len(target)),
    }


def save_bundle(bundle: dict[str, Any], destination: str | Path) -> None:
    """Write a model bundle atomically so the scorer never reads a partial file."""

    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    joblib.dump(bundle, temporary)
    os.replace(temporary, path)


def publish_bundle(bundle_path: str | Path, model_dir: str | Path, metrics: dict[str, float]) -> dict[str, Any]:
    """Publish a candidate and atomically switch the scorer's latest pointer."""

    bundle = joblib.load(bundle_path)
    registry = Path(model_dir)
    registry.mkdir(parents=True, exist_ok=True)
    filename = f"{bundle['version']}.joblib"
    destination = registry / filename
    save_bundle(bundle, destination)
    manifest = {
        "model_file": filename,
        "model_version": bundle["version"],
        "metrics": metrics,
    }
    temporary_manifest = registry / "latest.json.tmp"
    temporary_manifest.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    os.replace(temporary_manifest, registry / "latest.json")
    return manifest
