"""Offline feature aggregation pipeline computing Feast historical feature sources.

Computes point-in-time correct 1-hour snapshot aggregations for:
1. warehouse.zone_demand_features_hourly (pickup_datetime <= T anti-leakage)
2. warehouse.corridor_duration_features_hourly (dropoff_datetime <= T anti-leakage)
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.common.db import get_engine

logger = logging.getLogger(__name__)


def to_utc_datetime_series(s: pd.Series) -> pd.Series:
    """Convert a Series of strings, datetimes, or unix timestamps to UTC timezone-aware datetimes."""
    if s.empty:
        return s
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_datetime(s, unit="s", utc=True)
    dt_s = pd.to_datetime(s)
    if dt_s.dt.tz is None:
        return dt_s.dt.tz_localize(timezone.utc)
    return dt_s.dt.tz_convert(timezone.utc)


def is_us_holiday(dt: datetime) -> bool:
    """Determine if a given date is a US Federal Holiday in NYC.

    Args:
        dt: Datetime to check (UTC or naive).

    Returns:
        True if the date matches a federal holiday, else False.
    """
    d = dt.date() if isinstance(dt, datetime) else dt
    month = d.month
    day = d.day
    weekday = d.weekday()  # 0=Monday, 6=Sunday

    # Fixed-date holidays
    if (
        (month == 1 and day == 1)
        or (month == 6 and day == 19)
        or (month == 7 and day == 4)
        or (month == 11 and day == 11)
        or (month == 12 and day == 25)
    ):
        return True

    # MLK Day: 3rd Monday in Jan
    if month == 1 and weekday == 0 and 15 <= day <= 21:
        return True
    # Washington's Birthday / Presidents Day: 3rd Monday in Feb
    if month == 2 and weekday == 0 and 15 <= day <= 21:
        return True
    # Memorial Day: Last Monday in May
    if month == 5 and weekday == 0 and day >= 25:
        return True
    # Labor Day: 1st Monday in Sep
    if month == 9 and weekday == 0 and 1 <= day <= 7:
        return True
    # Columbus Day: 2nd Monday in Oct
    if month == 10 and weekday == 0 and 8 <= day <= 14:
        return True
    # Thanksgiving: 4th Thursday in Nov
    if month == 11 and weekday == 3 and 22 <= day <= 28:
        return True

    return False


def compute_zone_demand_features_hourly(
    df_trips: pd.DataFrame,
    start_time: datetime,
    end_time: datetime,
    all_zone_ids: Optional[List[int]] = None,
) -> pd.DataFrame:
    """Compute 1-hour snapshot zone demand features across target window.

    Anti-leakage: Pickups are strictly gated on pickup_datetime <= T.

    Args:
        df_trips: DataFrame with pickup_zone_id and pickup_datetime columns.
        start_time: Start of target observation window (inclusive).
        end_time: End of target observation window (inclusive).
        all_zone_ids: Optional list of all TLC taxi zone IDs (1..263).

    Returns:
        DataFrame matching warehouse.zone_demand_features_hourly schema.
    """
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)

    # If all_zone_ids is not provided, deduce from trips or standard range 1..263
    if all_zone_ids is None:
        if not df_trips.empty and "pickup_zone_id" in df_trips.columns:
            all_zone_ids = sorted(
                df_trips["pickup_zone_id"].dropna().unique().astype(int).tolist()
            )
        else:
            all_zone_ids = list(range(1, 264))

    # Target hourly snapshot timestamps
    hour_range = pd.date_range(
        start=start_time, end=end_time, freq="1h", tz=timezone.utc
    )
    if hour_range.empty:
        return pd.DataFrame(
            columns=[
                "zone_id",
                "pickup_datetime",
                "created_at",
                "pickup_count_last_15m",
                "pickup_count_last_1h",
                "pickup_count_last_24h",
                "pickup_count_same_hour_last_week",
                "hour_of_day",
                "day_of_week",
                "is_weekend",
                "is_holiday",
                "avg_temp_last_1h",
                "is_precipitating",
            ]
        )

    # Full 15-minute grid starting 8 days before start_time for 7-day lags + 24h rolling
    grid_start = start_time - timedelta(days=8)
    grid_end = end_time
    time_bins_15m = pd.date_range(
        start=grid_start, end=grid_end, freq="15min", tz=timezone.utc
    )

    now_utc = datetime.now(timezone.utc)

    if df_trips.empty:
        # Generate empty/zero baseline across all zones
        records = []
        for z in all_zone_ids:
            for t in hour_range:
                records.append(
                    {
                        "zone_id": int(z),
                        "pickup_datetime": t,
                        "created_at": now_utc,
                        "pickup_count_last_15m": 0,
                        "pickup_count_last_1h": 0,
                        "pickup_count_last_24h": 0,
                        "pickup_count_same_hour_last_week": 0,
                        "hour_of_day": int(t.hour),
                        "day_of_week": int(t.weekday()),
                        "is_weekend": bool(t.weekday() >= 5),
                        "is_holiday": bool(is_us_holiday(t)),
                        "avg_temp_last_1h": None,
                        "is_precipitating": False,
                    }
                )
        return pd.DataFrame(records)

    # Floor trip pickup datetimes to 15m intervals
    df = df_trips[["pickup_zone_id", "pickup_datetime"]].copy()
    df["pickup_datetime"] = to_utc_datetime_series(df["pickup_datetime"])
    df["time_bin_15m"] = df["pickup_datetime"].dt.floor("15min")

    # Filter trips within our grid range
    df = df[(df["time_bin_15m"] >= grid_start) & (df["time_bin_15m"] <= grid_end)]

    # Count pickups per (zone, 15m_bin)
    counts_15m = df.groupby(["pickup_zone_id", "time_bin_15m"]).size().rename("count")

    # Create full multi-index (zone_id x 15m_bin)
    full_idx = pd.MultiIndex.from_product(
        [all_zone_ids, time_bins_15m], names=["pickup_zone_id", "time_bin_15m"]
    )
    series_15m = counts_15m.reindex(full_idx, fill_value=0).unstack(
        level=0
    )  # index=time_bins, columns=zones

    # Vectorized rolling computations:
    # 1. 15m rolling: current 15m bin leading up to the hour (bin at H-15m covers [H-15m, H))
    # 2. 1h rolling: sum of 4 15m bins
    roll_1h = series_15m.rolling(4, min_periods=1).sum()
    # 3. 24h rolling: sum of 96 15m bins
    roll_24h = series_15m.rolling(96, min_periods=1).sum()
    # 4. 7d same hour lag: 1h sum shifted by 7 days (7 * 24 * 4 = 672 bins)
    roll_7d_lag = roll_1h.shift(672).fillna(0)

    # Observation hour H corresponds to the bin ending at H (i.e. bin timestamp H - 15m)
    # Filter for bins at XX:45:00
    sub_bins = time_bins_15m[time_bins_15m.minute == 45]
    # Corresponding snapshot timestamp H = bin + 15m
    obs_hours = sub_bins + timedelta(minutes=15)

    # Filter obs_hours to our target window [start_time, end_time]
    mask = (obs_hours >= start_time) & (obs_hours <= end_time)
    selected_sub_bins = sub_bins[mask]

    # Extract values for selected bins
    df_15m_selected = series_15m.loc[selected_sub_bins]
    df_1h_selected = roll_1h.loc[selected_sub_bins]
    df_24h_selected = roll_24h.loc[selected_sub_bins]
    df_7d_selected = roll_7d_lag.loc[selected_sub_bins]

    # Melt to long format
    df_15m_long = df_15m_selected.melt(
        ignore_index=False, var_name="zone_id", value_name="pickup_count_last_15m"
    )
    df_1h_long = df_1h_selected.melt(
        ignore_index=False, var_name="zone_id", value_name="pickup_count_last_1h"
    )
    df_24h_long = df_24h_selected.melt(
        ignore_index=False, var_name="zone_id", value_name="pickup_count_last_24h"
    )
    df_7d_long = df_7d_selected.melt(
        ignore_index=False,
        var_name="zone_id",
        value_name="pickup_count_same_hour_last_week",
    )

    res = df_15m_long.copy()
    res["pickup_count_last_1h"] = df_1h_long["pickup_count_last_1h"]
    res["pickup_count_last_24h"] = df_24h_long["pickup_count_last_24h"]
    res["pickup_count_same_hour_last_week"] = df_7d_long[
        "pickup_count_same_hour_last_week"
    ]

    # Shift index to snapshot observation hour (bin + 15m)
    res.index = res.index + timedelta(minutes=15)
    res = res.reset_index().rename(columns={"index": "pickup_datetime"})

    # Calendar and metadata features
    res["zone_id"] = res["zone_id"].astype(int)
    res["pickup_count_last_15m"] = res["pickup_count_last_15m"].astype(np.int64)
    res["pickup_count_last_1h"] = res["pickup_count_last_1h"].astype(np.int64)
    res["pickup_count_last_24h"] = res["pickup_count_last_24h"].astype(np.int64)
    res["pickup_count_same_hour_last_week"] = res[
        "pickup_count_same_hour_last_week"
    ].astype(np.int64)

    res["hour_of_day"] = res["pickup_datetime"].dt.hour.astype(int)
    res["day_of_week"] = res["pickup_datetime"].dt.weekday.astype(int)
    res["is_weekend"] = res["day_of_week"] >= 5
    res["is_holiday"] = res["pickup_datetime"].apply(is_us_holiday)
    res["avg_temp_last_1h"] = None
    res["is_precipitating"] = False
    res["created_at"] = now_utc

    columns = [
        "zone_id",
        "pickup_datetime",
        "created_at",
        "pickup_count_last_15m",
        "pickup_count_last_1h",
        "pickup_count_last_24h",
        "pickup_count_same_hour_last_week",
        "hour_of_day",
        "day_of_week",
        "is_weekend",
        "is_holiday",
        "avg_temp_last_1h",
        "is_precipitating",
    ]
    return (
        res[columns].sort_values(["pickup_datetime", "zone_id"]).reset_index(drop=True)
    )


def compute_corridor_duration_features_hourly(
    df_trips: pd.DataFrame,
    df_zone_demand: Optional[pd.DataFrame],
    start_time: datetime,
    end_time: datetime,
) -> pd.DataFrame:
    """Compute 1-hour snapshot corridor trip duration features.

    Anti-leakage: Trip durations are strictly gated on completed trips with dropoff_datetime <= T.

    Args:
        df_trips: DataFrame with pickup_zone_id, dropoff_zone_id, dropoff_datetime,
                  trip_duration_seconds, trip_distance_km.
        df_zone_demand: Precomputed zone demand features to join origin_zone_demand_pressure.
        start_time: Start of target observation window.
        end_time: End of target observation window.

    Returns:
        DataFrame matching warehouse.corridor_duration_features_hourly schema.
    """
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)

    now_utc = datetime.now(timezone.utc)
    columns = [
        "corridor_id",
        "dropoff_datetime",
        "created_at",
        "avg_duration_last_15m",
        "avg_duration_last_1h",
        "distance_km",
        "origin_zone_demand_pressure",
        "avg_traffic_speed_current",
    ]

    if df_trips.empty:
        return pd.DataFrame(columns=columns)

    df = df_trips[
        [
            "pickup_zone_id",
            "dropoff_zone_id",
            "dropoff_datetime",
            "trip_duration_seconds",
            "trip_distance_km",
        ]
    ].copy()

    df["dropoff_datetime"] = to_utc_datetime_series(df["dropoff_datetime"])

    # Canonical corridor_id format
    df["corridor_id"] = (
        df["pickup_zone_id"].astype(int).astype(str)
        + "_"
        + df["dropoff_zone_id"].astype(int).astype(str)
    )

    # Filter trips within range (from start_time - 1h up to end_time)
    df = df[
        (df["dropoff_datetime"] >= start_time - timedelta(hours=1))
        & (df["dropoff_datetime"] <= end_time)
    ]

    # Pre-build lookup map for origin_zone_demand_pressure from df_zone_demand
    origin_pressure_map: Dict[Tuple[int, datetime], int] = {}
    if df_zone_demand is not None and not df_zone_demand.empty:
        for row in df_zone_demand.itertuples():
            origin_pressure_map[(int(row.zone_id), row.pickup_datetime)] = int(
                row.pickup_count_last_1h
            )

    # Static corridor distance fallback map (average distance across all historical trips for corridor)
    corridor_dist_map = df.groupby("corridor_id")["trip_distance_km"].mean().to_dict()

    # Generate hourly intervals
    hour_range = pd.date_range(
        start=start_time, end=end_time, freq="1h", tz=timezone.utc
    )

    records = []
    # For each observation hour T
    for obs_hour in hour_range:
        # Completed trips in last 1 hour: (obs_hour - 1h, obs_hour]
        mask_1h = (df["dropoff_datetime"] > obs_hour - timedelta(hours=1)) & (
            df["dropoff_datetime"] <= obs_hour
        )
        df_1h = df[mask_1h]

        # Completed trips in last 15 mins: (obs_hour - 15m, obs_hour]
        mask_15m = (df["dropoff_datetime"] > obs_hour - timedelta(minutes=15)) & (
            df["dropoff_datetime"] <= obs_hour
        )
        df_15m = df[mask_15m]

        # Aggregate 1h metrics per corridor
        agg_1h = (
            df_1h.groupby("corridor_id")
            .agg(
                avg_duration_1h=("trip_duration_seconds", "mean"),
                avg_distance_1h=("trip_distance_km", "mean"),
                pu_zone=("pickup_zone_id", "first"),
            )
            .to_dict(orient="index")
        )

        # Aggregate 15m metrics per corridor
        agg_15m = (
            df_15m.groupby("corridor_id")["trip_duration_seconds"].mean().to_dict()
        )

        for corridor_id, stats_1h in agg_1h.items():
            dur_1h = float(stats_1h["avg_duration_1h"])
            dur_15m = float(
                agg_15m.get(corridor_id, dur_1h)
            )  # Fallback to 1h avg if no trips in 15m
            dist_km = float(
                stats_1h.get("avg_distance_1h")
                or corridor_dist_map.get(corridor_id, 0.0)
            )
            pu_zone = int(stats_1h["pu_zone"])

            pressure = origin_pressure_map.get((pu_zone, obs_hour), 0)

            records.append(
                {
                    "corridor_id": corridor_id,
                    "dropoff_datetime": obs_hour,
                    "created_at": now_utc,
                    "avg_duration_last_15m": dur_15m,
                    "avg_duration_last_1h": dur_1h,
                    "distance_km": dist_km,
                    "origin_zone_demand_pressure": pressure,
                    "avg_traffic_speed_current": None,
                }
            )

    if not records:
        return pd.DataFrame(columns=columns)

    res_df = pd.DataFrame(records)
    return (
        res_df[columns]
        .sort_values(["dropoff_datetime", "corridor_id"])
        .reset_index(drop=True)
    )


def load_offline_features_to_db(
    engine: Engine,
    df_zone_demand: pd.DataFrame,
    df_corridor_duration: pd.DataFrame,
    chunk_size: int = 10000,
) -> Tuple[int, int]:
    """Idempotently insert/update offline feature rows in PostgreSQL warehouse schema.

    Args:
        engine: SQLAlchemy Engine connected to database.
        df_zone_demand: Zone demand features DataFrame.
        df_corridor_duration: Corridor duration features DataFrame.
        chunk_size: Chunk size for batch inserts.

    Returns:
        Tuple of (zone_demand_rows_loaded, corridor_duration_rows_loaded).
    """
    is_postgres = engine.dialect.name == "postgresql"

    # 1. Insert Zone Demand Features
    zone_count = len(df_zone_demand)
    if not df_zone_demand.empty:
        df_z = df_zone_demand.copy()

        with engine.begin() as conn:
            for i in range(0, zone_count, chunk_size):
                chunk = df_z.iloc[i : i + chunk_size]
                if is_postgres:
                    sql = text("""
                        INSERT INTO warehouse.zone_demand_features_hourly (
                            zone_id, pickup_datetime, created_at,
                            pickup_count_last_15m, pickup_count_last_1h,
                            pickup_count_last_24h, pickup_count_same_hour_last_week,
                            hour_of_day, day_of_week, is_weekend, is_holiday,
                            avg_temp_last_1h, is_precipitating
                        ) VALUES (
                            :zone_id, :pickup_datetime, :created_at,
                            :pickup_count_last_15m, :pickup_count_last_1h,
                            :pickup_count_last_24h, :pickup_count_same_hour_last_week,
                            :hour_of_day, :day_of_week, :is_weekend, :is_holiday,
                            :avg_temp_last_1h, :is_precipitating
                        )
                        ON CONFLICT (zone_id, pickup_datetime) DO UPDATE SET
                            pickup_count_last_15m = EXCLUDED.pickup_count_last_15m,
                            pickup_count_last_1h = EXCLUDED.pickup_count_last_1h,
                            pickup_count_last_24h = EXCLUDED.pickup_count_last_24h,
                            pickup_count_same_hour_last_week = EXCLUDED.pickup_count_same_hour_last_week,
                            hour_of_day = EXCLUDED.hour_of_day,
                            day_of_week = EXCLUDED.day_of_week,
                            is_weekend = EXCLUDED.is_weekend,
                            is_holiday = EXCLUDED.is_holiday,
                            avg_temp_last_1h = EXCLUDED.avg_temp_last_1h,
                            is_precipitating = EXCLUDED.is_precipitating,
                            created_at = EXCLUDED.created_at;
                        """)
                    params = chunk.to_dict(orient="records")
                else:
                    sql = text("""
                        INSERT OR REPLACE INTO zone_demand_features_hourly (
                            zone_id, pickup_datetime, created_at,
                            pickup_count_last_15m, pickup_count_last_1h,
                            pickup_count_last_24h, pickup_count_same_hour_last_week,
                            hour_of_day, day_of_week, is_weekend, is_holiday,
                            avg_temp_last_1h, is_precipitating
                        ) VALUES (
                            :zone_id, :pickup_datetime, :created_at,
                            :pickup_count_last_15m, :pickup_count_last_1h,
                            :pickup_count_last_24h, :pickup_count_same_hour_last_week,
                            :hour_of_day, :day_of_week, :is_weekend, :is_holiday,
                            :avg_temp_last_1h, :is_precipitating
                        );
                        """)
                    params = [
                        {
                            k: (
                                v.isoformat()
                                if isinstance(v, (pd.Timestamp, datetime))
                                else v
                            )
                            for k, v in r.items()
                        }
                        for r in chunk.to_dict(orient="records")
                    ]
                conn.execute(sql, params)

    # 2. Insert Corridor Duration Features
    corridor_count = len(df_corridor_duration)
    if not df_corridor_duration.empty:
        df_c = df_corridor_duration.copy()

        with engine.begin() as conn:
            for i in range(0, corridor_count, chunk_size):
                chunk = df_c.iloc[i : i + chunk_size]
                if is_postgres:
                    sql = text("""
                        INSERT INTO warehouse.corridor_duration_features_hourly (
                            corridor_id, dropoff_datetime, created_at,
                            avg_duration_last_15m, avg_duration_last_1h,
                            distance_km, origin_zone_demand_pressure, avg_traffic_speed_current
                        ) VALUES (
                            :corridor_id, :dropoff_datetime, :created_at,
                            :avg_duration_last_15m, :avg_duration_last_1h,
                            :distance_km, :origin_zone_demand_pressure, :avg_traffic_speed_current
                        )
                        ON CONFLICT (corridor_id, dropoff_datetime) DO UPDATE SET
                            avg_duration_last_15m = EXCLUDED.avg_duration_last_15m,
                            avg_duration_last_1h = EXCLUDED.avg_duration_last_1h,
                            distance_km = EXCLUDED.distance_km,
                            origin_zone_demand_pressure = EXCLUDED.origin_zone_demand_pressure,
                            avg_traffic_speed_current = EXCLUDED.avg_traffic_speed_current,
                            created_at = EXCLUDED.created_at;
                        """)
                    params = chunk.to_dict(orient="records")
                else:
                    sql = text("""
                        INSERT OR REPLACE INTO corridor_duration_features_hourly (
                            corridor_id, dropoff_datetime, created_at,
                            avg_duration_last_15m, avg_duration_last_1h,
                            distance_km, origin_zone_demand_pressure, avg_traffic_speed_current
                        ) VALUES (
                            :corridor_id, :dropoff_datetime, :created_at,
                            :avg_duration_last_15m, :avg_duration_last_1h,
                            :distance_km, :origin_zone_demand_pressure, :avg_traffic_speed_current
                        );
                        """)
                    params = [
                        {
                            k: (
                                v.isoformat()
                                if isinstance(v, (pd.Timestamp, datetime))
                                else v
                            )
                            for k, v in r.items()
                        }
                        for r in chunk.to_dict(orient="records")
                    ]
                conn.execute(sql, params)

    return zone_count, corridor_count


def _parse_utc_datetime(val: Optional[datetime]) -> Optional[datetime]:
    """Parse various timestamp representations into a UTC datetime."""
    if val is None:
        return None
    if isinstance(val, (int, float, np.integer, np.floating)):
        return datetime.fromtimestamp(float(val), tz=timezone.utc)
    if isinstance(val, str):
        val = datetime.fromisoformat(val)
    if val.tzinfo is None:
        return val.replace(tzinfo=timezone.utc)
    return val.astimezone(timezone.utc)


def _resolve_target_window(
    engine: Engine,
    table_name: str,
    start_datetime: Optional[datetime],
    end_datetime: Optional[datetime],
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Determine the start and end datetime bounds for extraction."""
    if start_datetime is None or end_datetime is None:
        with engine.connect() as conn:
            res = conn.execute(
                text(
                    f"SELECT MIN(pickup_datetime), MAX(pickup_datetime) FROM {table_name}"
                )
            ).fetchone()
            if res is None or res[0] is None or res[1] is None:
                return None, None
            if start_datetime is None:
                start_datetime = res[0]
            if end_datetime is None:
                end_datetime = res[1]

    st = _parse_utc_datetime(start_datetime)
    et = _parse_utc_datetime(end_datetime)
    if st is None or et is None:
        return None, None
    return st.replace(minute=0, second=0, microsecond=0), et.replace(
        minute=0, second=0, microsecond=0
    )


def _query_trips_for_window(
    engine: Engine,
    table_name: str,
    is_postgres: bool,
    fetch_start: datetime,
    end_time: datetime,
) -> pd.DataFrame:
    """Fetch trips data across window from postgres or sqlite."""
    if is_postgres:
        query = f"""
            SELECT
                pickup_zone_id,
                dropoff_zone_id,
                pickup_datetime,
                dropoff_datetime,
                trip_duration_seconds,
                trip_distance_km
            FROM {table_name}
            WHERE pickup_datetime >= :fetch_start AND pickup_datetime <= :end_time
        """
        return pd.read_sql(
            text(query),
            con=engine,
            params={"fetch_start": fetch_start, "end_time": end_time},
        )
    query = f"""
        SELECT
            pickup_zone_id,
            dropoff_zone_id,
            pickup_datetime,
            dropoff_datetime,
            trip_duration_seconds,
            trip_distance_km
        FROM {table_name}
    """
    df_trips = pd.read_sql(text(query), con=engine)
    if not df_trips.empty:
        df_trips["pickup_datetime"] = to_utc_datetime_series(
            df_trips["pickup_datetime"]
        )
        df_trips = df_trips[
            (df_trips["pickup_datetime"] >= fetch_start)
            & (df_trips["pickup_datetime"] <= end_time)
        ]
    return df_trips


def extract_and_load_offline_features(
    engine: Optional[Engine] = None,
    start_datetime: Optional[datetime] = None,
    end_datetime: Optional[datetime] = None,
    lookback_days: int = 7,
) -> Tuple[int, int]:
    """Execute time-windowed offline feature extraction against warehouse.trips.

    Args:
        engine: Optional SQLAlchemy Engine. If omitted, uses get_engine().
        start_datetime: Start timestamp of observation window. Defaults to min trip timestamp.
        end_datetime: End timestamp of observation window. Defaults to max trip timestamp.
        lookback_days: Lookback buffer days for rolling windows (default 7 days).

    Returns:
        Tuple of (zone_rows_loaded, corridor_rows_loaded).
    """
    if engine is None:
        engine = get_engine()

    is_postgres = engine.dialect.name == "postgresql"
    table_name = "warehouse.trips" if is_postgres else "trips"

    start_time, end_time = _resolve_target_window(
        engine=engine,
        table_name=table_name,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
    )
    if start_time is None or end_time is None:
        logger.warning(
            "No trips found in warehouse.trips. Skipping offline extraction."
        )
        return 0, 0

    fetch_start = start_time - timedelta(days=lookback_days + 1)
    logger.info(
        f"Extracting trips for offline features: target window [{start_time} -> {end_time}], fetch window [{fetch_start} -> {end_time}]"
    )

    df_trips = _query_trips_for_window(
        engine=engine,
        table_name=table_name,
        is_postgres=is_postgres,
        fetch_start=fetch_start,
        end_time=end_time,
    )

    logger.info(f"Loaded {len(df_trips)} raw trips from {table_name} for aggregation.")

    # 1. Compute Zone Demand Features
    df_zone_demand = compute_zone_demand_features_hourly(
        df_trips=df_trips,
        start_time=start_time,
        end_time=end_time,
    )
    logger.info(f"Computed {len(df_zone_demand)} hourly zone demand feature rows.")

    # 2. Compute Corridor Duration Features
    df_corridor_duration = compute_corridor_duration_features_hourly(
        df_trips=df_trips,
        df_zone_demand=df_zone_demand,
        start_time=start_time,
        end_time=end_time,
    )
    logger.info(
        f"Computed {len(df_corridor_duration)} hourly corridor duration feature rows."
    )

    # 3. Load to DB
    zone_count, corridor_count = load_offline_features_to_db(
        engine=engine,
        df_zone_demand=df_zone_demand,
        df_corridor_duration=df_corridor_duration,
    )
    logger.info(
        f"Successfully loaded {zone_count} zone rows and {corridor_count} corridor rows into warehouse."
    )
    return zone_count, corridor_count


def backfill_all_loaded_months(
    engine: Optional[Engine] = None,
    lookback_days: int = 7,
) -> Dict[str, Tuple[int, int]]:
    """Backfill offline features for all months tracked in warehouse.loaded_months.

    Args:
        engine: Optional SQLAlchemy Engine.
        lookback_days: Warm-up lookback buffer days (default 7).

    Returns:
        Dict mapping month_key -> (zone_rows, corridor_rows).
    """
    if engine is None:
        engine = get_engine()

    is_postgres = engine.dialect.name == "postgresql"
    loaded_months_table = "warehouse.loaded_months" if is_postgres else "loaded_months"

    with engine.connect() as conn:
        res = conn.execute(
            text(f"SELECT month_key FROM {loaded_months_table} ORDER BY month_key ASC")
        ).fetchall()

    if not res:
        logger.info(
            "No recorded months in loaded_months. Running range extraction on full trips table."
        )
        z_count, c_count = extract_and_load_offline_features(
            engine=engine, lookback_days=lookback_days
        )
        return {"all": (z_count, c_count)}

    month_results: Dict[str, Tuple[int, int]] = {}
    for (m_key,) in res:
        logger.info(f"Processing offline feature backfill for month: {m_key}")
        # Parse 'YYYY-MM'
        year, month = map(int, m_key.split("-"))
        start_dt = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)
        # End of month
        if month == 12:
            end_dt = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc) - timedelta(
                hours=1
            )
        else:
            end_dt = datetime(
                year, month + 1, 1, 0, 0, 0, tzinfo=timezone.utc
            ) - timedelta(hours=1)

        z_count, c_count = extract_and_load_offline_features(
            engine=engine,
            start_datetime=start_dt,
            end_datetime=end_dt,
            lookback_days=lookback_days,
        )
        month_results[m_key] = (z_count, c_count)

    return month_results
