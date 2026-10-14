"""Next-step congestion model trained only on rows this live stream has seen.

Until a few hundred real rows exist, the forecast is persistence: the next
reading will look like the current one. After that, a small random forest is
fit on the stream itself. The dashboard reports the holdout error of that fit.
There is no pre-baked accuracy number.
"""
from __future__ import annotations

import math
import random


def _hour_cycle(ts_ms: int) -> tuple[float, float]:
    # Asia/Kolkata is UTC+5:30. Spark and the producer stamp unix milliseconds.
    seconds = ts_ms / 1000.0 + 5.5 * 3600.0
    hour = (seconds % 86400) / 3600.0
    angle = (hour / 24.0) * 2.0 * math.pi
    return math.sin(angle), math.cos(angle)


class NextStepModel:
    def __init__(self) -> None:
        self.rows: list[tuple[list[float], float]] = []
        self.model = None
        self.mae: float | None = None
        self.kind = "persistence"
        self._since_train = 0

    def observe(self, features: list[float], target: float) -> None:
        self.rows.append((features, target))
        if len(self.rows) > 8000:
            self.rows = self.rows[-6000:]

    def maybe_train(self) -> None:
        self._since_train += 1
        if len(self.rows) < 400 or self._since_train < 6:
            return
        self._since_train = 0
        try:
            from sklearn.ensemble import RandomForestRegressor
        except Exception as exc:  # pragma: no cover - dependency is in the image
            print(f"[model] sklearn unavailable: {exc}", flush=True)
            return
        rows = list(self.rows)
        random.Random(7).shuffle(rows)
        cut = max(1, int(len(rows) * 0.8))
        train, holdout = rows[:cut], rows[cut:]
        model = RandomForestRegressor(
            n_estimators=40,
            max_depth=8,
            min_samples_leaf=5,
            random_state=7,
            n_jobs=1,
        )
        model.fit([features for features, _ in train], [target for _, target in train])
        if holdout:
            errors = [
                abs(float(model.predict([features])[0]) - target) for features, target in holdout
            ]
            self.mae = sum(errors) / len(errors)
        self.model = model
        self.kind = "random_forest"
        print(f"[model] trained on {len(train)} rows, holdout MAE {self.mae}", flush=True)

    def predict(self, features: list[float], fallback: float) -> float:
        if self.model is None:
            return fallback
        value = float(self.model.predict([features])[0])
        return max(0.0, min(1.0, value))

    def status(self) -> dict:
        return {
            "kind": self.kind,
            "rows": len(self.rows),
            "mae": None if self.mae is None else round(self.mae, 4),
        }


def features_of(speed: float, occupancy: float, rain: float, ts_ms: int) -> list[float]:
    sin_h, cos_h = _hour_cycle(ts_ms)
    return [float(speed), float(occupancy), float(rain), sin_h, cos_h]
