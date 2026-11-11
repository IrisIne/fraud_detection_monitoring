"""Hot-reloadable local model registry for the scoring process."""

from __future__ import annotations

import json
from pathlib import Path
from threading import Lock
from typing import Any

import joblib
import pandas as pd

from common.features import build_features


class ModelRegistryError(RuntimeError):
    """Raised when the published model registry is inconsistent."""

    pass


class HotReloadScorer:
    """Load a new model manifest between messages without restarting the process."""

    def __init__(self, model_dir: str, threshold: float):
        self.model_dir = Path(model_dir)
        self.threshold = threshold
        self._version: str | None = None
        self._bundle: dict[str, Any] | None = None
        self._lock = Lock()
        self.reload_if_needed()

    @property
    def version(self) -> str:
        if self._version is None:
            raise ModelRegistryError("no model is loaded")
        return self._version

    def reload_if_needed(self) -> bool:
        manifest_path = self.model_dir / "latest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        version = str(manifest["model_version"])
        if version == self._version:
            return False
        with self._lock:
            if version == self._version:
                return False
            bundle = joblib.load(self.model_dir / str(manifest["model_file"]))
            if str(bundle.get("version")) != version:
                raise ModelRegistryError("manifest and model versions do not match")
            self._bundle = bundle
            self._version = version
        return True

    def predict(self, transaction: dict[str, Any]) -> tuple[float, int, str]:
        self.reload_if_needed()
        with self._lock:
            if self._bundle is None:
                raise ModelRegistryError("no model is loaded")
            features = build_features(pd.DataFrame([transaction]))
            score = float(self._bundle["pipeline"].predict_proba(features)[0, 1])
            version = str(self._bundle["version"])
        return score, int(score > self.threshold), version
