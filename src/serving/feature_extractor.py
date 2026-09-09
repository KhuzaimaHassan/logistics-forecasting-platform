"""Feature vector construction and degraded-mode imputation for online serving.

Translates Feast online feature payloads into Pandas DataFrames adhering strictly to the
exact feature column schema, order, and categorical dtypes expected by the trained LightGBM
demand and corridor trip duration models.
"""

from datetime import datetime, timezone
from typing import List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from src.features.client import (
    CorridorDurationOnlineFeatures,
    ZoneDemandOnlineFeatures,
)

# Canonical active TLC zone categorical index (zones 1 through 263)
ACTIVE_ZONE_CATEGORIES: List[int] = list(range(1, 264))

# Training schema definitions matching train_demand.py and train_duration.py
DEMAND_FEATURE_COLS: List[str] = [
    "zone_id",
    "pickup_count_last_15m",
    "pickup_count_last_1h",
    "pickup_count_last_24h",
    "pickup_count_same_hour_last_week",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_holiday",
    "sin_hour",
    "cos_hour",
    "sin_day_of_week",
    "cos_day_of_week",
]

DURATION_FEATURE_COLS: List[str] = [
    "pickup_zone_id",
    "dropoff_zone_id",
    "avg_duration_last_15m",
    "avg_duration_last_1h",
    "log_avg_duration_last_1h",
    "distance_km",
    "origin_zone_demand_pressure",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "sin_hour",
    "cos_hour",
    "sin_day_of_week",
    "cos_day_of_week",
]

DEFAULT_CITYWIDE_DISTANCE_KM: float = 3.5
DEFAULT_CORRIDOR_DURATION_SEC: float = 900.0  # 15 minutes default


def _compute_calendar_harmonics(
    now: datetime,
) -> Tuple[float, float, float, float, float, float, float]:
    """Calculate continuous hour, day, weekend flag, and cyclic sine/cosine harmonics."""
    hour = float(now.hour + now.minute / 60.0 + now.second / 3600.0)
    day = float(now.weekday())
    is_weekend = 1.0 if now.weekday() in (5, 6) else 0.0

    sin_hour = float(np.sin(2.0 * np.pi * hour / 24.0))
    cos_hour = float(np.cos(2.0 * np.pi * hour / 24.0))
    sin_day_of_week = float(np.sin(2.0 * np.pi * day / 7.0))
    cos_day_of_week = float(np.cos(2.0 * np.pi * day / 7.0))

    return (
        hour,
        day,
        is_weekend,
        sin_hour,
        cos_hour,
        sin_day_of_week,
        cos_day_of_week,
    )


def build_demand_feature_df(
    features: List[ZoneDemandOnlineFeatures],
    now: Optional[datetime] = None,
) -> pd.DataFrame:
    """Transform Feast ZoneDemandOnlineFeatures into a model-ready LightGBM DataFrame.

    If features are unmaterialized in Redis (cache_hit=False), missing rolling pickup counts
    are imputed to 0.0, and calendar harmonics are computed from UTC now.

    Args:
        features: List of ZoneDemandOnlineFeatures retrieved from Feast.
        now: Reference timestamp for calendar harmonics (defaults to UTC now).

    Returns:
        Pandas DataFrame conforming exactly to DEMAND_FEATURE_COLS with categorical zone_id.
    """
    ref_time = now or datetime.now(timezone.utc)
    (
        calc_hour,
        calc_day,
        calc_weekend,
        calc_sin_hour,
        calc_cos_hour,
        calc_sin_day,
        calc_cos_day,
    ) = _compute_calendar_harmonics(ref_time)

    rows = []
    for feat in features:
        h_val = float(feat.hour_of_day) if feat.hour_of_day is not None else calc_hour
        d_val = float(feat.day_of_week) if feat.day_of_week is not None else calc_day
        w_val = (
            float(1.0 if feat.is_weekend else 0.0)
            if feat.is_weekend is not None
            else calc_weekend
        )
        hol_val = float(1.0 if feat.is_holiday else 0.0)

        rows.append(
            {
                "zone_id": int(feat.zone_id),
                "pickup_count_last_15m": float(feat.pickup_count_last_15m or 0.0),
                "pickup_count_last_1h": float(feat.pickup_count_last_1h or 0.0),
                "pickup_count_last_24h": float(feat.pickup_count_last_24h or 0.0),
                "pickup_count_same_hour_last_week": float(
                    feat.pickup_count_same_hour_last_week or 0.0
                ),
                "hour_of_day": h_val,
                "day_of_week": d_val,
                "is_weekend": w_val,
                "is_holiday": hol_val,
                "sin_hour": calc_sin_hour,
                "cos_hour": calc_cos_hour,
                "sin_day_of_week": calc_sin_day,
                "cos_day_of_week": calc_cos_day,
            }
        )

    df = pd.DataFrame(rows, columns=DEMAND_FEATURE_COLS)
    # Cast zone_id to categorical with trained active categories [1..263]
    df["zone_id"] = pd.Categorical(df["zone_id"], categories=ACTIVE_ZONE_CATEGORIES)
    return df


def build_corridor_feature_df(
    features: List[CorridorDurationOnlineFeatures],
    origin_dest_pairs: Optional[List[Tuple[int, int]]] = None,
    now: Optional[datetime] = None,
) -> pd.DataFrame:
    """Transform Feast CorridorDurationOnlineFeatures into a model-ready LightGBM DataFrame.

    If corridor features are unmaterialized in Redis (cache_hit=False), missing duration
    values default to 900.0s (15 min), distance to 3.5 km, and demand pressure to 1.0.

    Args:
        features: List of CorridorDurationOnlineFeatures retrieved from Feast.
        origin_dest_pairs: Optional explicit (origin_id, dest_id) pairs matching features.
        now: Reference timestamp for calendar harmonics (defaults to UTC now).

    Returns:
        Pandas DataFrame conforming exactly to DURATION_FEATURE_COLS with categorical zone IDs.
    """
    ref_time = now or datetime.now(timezone.utc)
    (
        calc_hour,
        calc_day,
        calc_weekend,
        calc_sin_hour,
        calc_cos_hour,
        calc_sin_day,
        calc_cos_day,
    ) = _compute_calendar_harmonics(ref_time)

    rows = []
    for idx, feat in enumerate(features):
        if origin_dest_pairs is not None and idx < len(origin_dest_pairs):
            orig_id, dest_id = origin_dest_pairs[idx]
        else:
            # Parse from corridor_id format '{origin}_{dest}'
            parts = str(feat.corridor_id).split("_")
            orig_id = int(parts[0]) if len(parts) >= 1 and parts[0].isdigit() else 161
            dest_id = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 236

        avg_15m = (
            float(feat.avg_duration_last_15m)
            if feat.avg_duration_last_15m is not None
            else DEFAULT_CORRIDOR_DURATION_SEC
        )
        avg_1h = (
            float(feat.avg_duration_last_1h)
            if feat.avg_duration_last_1h is not None
            else DEFAULT_CORRIDOR_DURATION_SEC
        )
        log_avg_1h = float(np.log1p(avg_1h))
        dist_km = (
            float(feat.distance_km)
            if feat.distance_km is not None
            else DEFAULT_CITYWIDE_DISTANCE_KM
        )
        demand_pressure = (
            float(feat.origin_zone_demand_pressure)
            if feat.origin_zone_demand_pressure is not None
            else 1.0
        )

        rows.append(
            {
                "pickup_zone_id": orig_id,
                "dropoff_zone_id": dest_id,
                "avg_duration_last_15m": avg_15m,
                "avg_duration_last_1h": avg_1h,
                "log_avg_duration_last_1h": log_avg_1h,
                "distance_km": dist_km,
                "origin_zone_demand_pressure": demand_pressure,
                "hour_of_day": calc_hour,
                "day_of_week": calc_day,
                "is_weekend": calc_weekend,
                "sin_hour": calc_sin_hour,
                "cos_hour": calc_cos_hour,
                "sin_day_of_week": calc_sin_day,
                "cos_day_of_week": calc_cos_day,
            }
        )

    df = pd.DataFrame(rows, columns=DURATION_FEATURE_COLS)
    df["pickup_zone_id"] = pd.Categorical(
        df["pickup_zone_id"], categories=ACTIVE_ZONE_CATEGORIES
    )
    df["dropoff_zone_id"] = pd.Categorical(
        df["dropoff_zone_id"], categories=ACTIVE_ZONE_CATEGORIES
    )
    return df


def invert_log_duration(
    raw_pred: Union[float, np.ndarray],
    is_log_space: bool = True,
) -> Tuple[float, float]:
    """Invert duration prediction to seconds and minutes.

    If is_log_space is True (e.g. LightGBM model output in log1p-seconds) and raw_val <= 20.0,
    computes np.expm1(raw_val). If raw_val > 20.0 or is_log_space is False (e.g. baseline estimator
    predicting raw seconds directly), treats value directly as seconds.

    Applies floor constraint of 60.0 seconds (1 minute minimum physical trip duration).

    Args:
        raw_pred: Predicted value (either log1p-space or direct seconds).
        is_log_space: If True, attempts log inversion unless value indicates raw seconds.

    Returns:
        Tuple of (duration_seconds, duration_minutes) rounded for API response.
    """
    raw_val = float(np.asarray(raw_pred).flatten()[0])
    if not is_log_space or raw_val > 20.0:
        duration_sec = float(np.maximum(60.0, raw_val))
    else:
        duration_sec = float(np.maximum(60.0, np.expm1(raw_val)))
    duration_min = round(duration_sec / 60.0, 2)
    return round(duration_sec, 1), duration_min
