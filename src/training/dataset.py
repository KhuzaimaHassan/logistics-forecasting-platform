"""Point-in-time training dataset generation and time-based splitting (M3-2, ADR-016)."""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from feast import FeatureStore
from sqlalchemy import Engine, text

from src.common.db import get_engine
from src.features.config import get_feature_store

logger = logging.getLogger(__name__)

# Canonical training & validation date ranges for 2023-01 data
DEFAULT_TRAIN_START = datetime(2023, 1, 8, 0, 0, 0, tzinfo=timezone.utc)
DEFAULT_VAL_SPLIT = datetime(2023, 1, 25, 0, 0, 0, tzinfo=timezone.utc)
DEFAULT_TRAIN_END = datetime(2023, 2, 1, 0, 0, 0, tzinfo=timezone.utc)

DEMAND_FEATURES = [
    "zone_demand_features:pickup_count_last_15m",
    "zone_demand_features:pickup_count_last_1h",
    "zone_demand_features:pickup_count_last_24h",
    "zone_demand_features:pickup_count_same_hour_last_week",
    "zone_demand_features:hour_of_day",
    "zone_demand_features:day_of_week",
    "zone_demand_features:is_weekend",
    "zone_demand_features:is_holiday",
]

CORRIDOR_FEATURES = [
    "corridor_duration_features:avg_duration_last_15m",
    "corridor_duration_features:avg_duration_last_1h",
    "corridor_duration_features:distance_km",
    "corridor_duration_features:origin_zone_demand_pressure",
]


def build_demand_entity_grid(
    zone_ids: List[int],
    start_time: datetime,
    end_time: datetime,
) -> pd.DataFrame:
    """Build a complete Cartesian product of taxi zones and hourly timestamps.

    Args:
        zone_ids: List of unique NYC taxi zone IDs (1 to 263).
        start_time: Beginning of observation window (inclusive).
        end_time: End of observation window (exclusive).

    Returns:
        DataFrame with columns ['zone_id', 'event_timestamp'].
    """
    timestamps = pd.date_range(
        start=start_time,
        end=end_time,
        freq="1h",
        inclusive="left",
        tz=timezone.utc,
    )
    grid = pd.MultiIndex.from_product(
        [zone_ids, timestamps], names=["zone_id", "event_timestamp"]
    ).to_frame(index=False)
    grid["zone_id"] = grid["zone_id"].astype("int64")
    grid["event_timestamp"] = pd.to_datetime(grid["event_timestamp"], utc=True)
    return grid


def compute_demand_targets_from_trips(
    engine: Engine,
    start_time: datetime,
    end_time: datetime,
) -> pd.DataFrame:
    """Compute ground-truth demand target (pickups departing in [T, T+1h)) from warehouse.trips.

    Args:
        engine: SQLAlchemy database engine.
        start_time: Start observation timestamp (inclusive).
        end_time: End observation timestamp (exclusive).

    Returns:
        DataFrame with columns ['zone_id', 'event_timestamp', 'target_pickup_count_next_1h'].
    """
    is_postgres = engine.dialect.name == "postgresql"
    table_name = "warehouse.trips" if is_postgres else "trips"

    if is_postgres:
        query = text(f"""
            SELECT
                pickup_zone_id AS zone_id,
                date_trunc('hour', pickup_datetime) AS event_timestamp,
                COUNT(*) AS target_pickup_count_next_1h
            FROM {table_name}
            WHERE pickup_datetime >= :start_time
              AND pickup_datetime < :end_time
            GROUP BY pickup_zone_id, date_trunc('hour', pickup_datetime);
        """)
    else:
        query = text(f"""
            SELECT
                pickup_zone_id AS zone_id,
                strftime('%Y-%m-%d %H:00:00', pickup_datetime) AS event_timestamp,
                COUNT(*) AS target_pickup_count_next_1h
            FROM {table_name}
            WHERE pickup_datetime >= :start_time
              AND pickup_datetime < :end_time
            GROUP BY pickup_zone_id, strftime('%Y-%m-%d %H:00:00', pickup_datetime);
        """)

    params = (
        {"start_time": start_time, "end_time": end_time}
        if is_postgres
        else {
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        }
    )

    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params=params,
        )

    if not df.empty:
        df["zone_id"] = df["zone_id"].astype("int64")
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
        df["target_pickup_count_next_1h"] = df["target_pickup_count_next_1h"].astype(
            "int64"
        )
    else:
        df = pd.DataFrame(
            columns=["zone_id", "event_timestamp", "target_pickup_count_next_1h"]
        )
        df["zone_id"] = df["zone_id"].astype("int64")
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
        df["target_pickup_count_next_1h"] = df["target_pickup_count_next_1h"].astype(
            "int64"
        )
    return df


def get_all_zone_ids(engine: Engine) -> List[int]:
    """Retrieve list of all active taxi zone IDs from warehouse.taxi_zones."""
    is_postgres = engine.dialect.name == "postgresql"
    table_name = "warehouse.taxi_zones" if is_postgres else "taxi_zones"
    query = text(f"SELECT zone_id FROM {table_name} ORDER BY zone_id;")
    try:
        with engine.connect() as conn:
            result = conn.execute(query).scalars().all()
        if result:
            return [int(z) for z in result]
    except Exception as exc:
        logger.debug(
            "Failed to query %s directly: %s. Using default 1..263.", table_name, exc
        )
    return list(range(1, 264))


def _fetch_historical_features_in_batches(
    store: FeatureStore,
    entity_df: pd.DataFrame,
    features: List[str],
    batch_size: int = 10000,
) -> pd.DataFrame:
    """Retrieve historical features from Feast in memory-safe batches.

    Prevents Dask in-memory combinatorial explosions and OOM on large entity dataframes
    when using local/file offline store engines.

    Args:
        store: Feast FeatureStore instance.
        entity_df: Observation entity dataframe with timestamp column.
        features: List of Feast feature references.
        batch_size: Maximum rows per retrieval batch.

    Returns:
        Concatenated DataFrame containing retrieved historical features.
    """
    if len(entity_df) <= batch_size:
        return store.get_historical_features(
            entity_df=entity_df, features=features
        ).to_df()

    dfs = []
    num_batches = (len(entity_df) + batch_size - 1) // batch_size
    logger.info(
        "Retrieving Feast historical features across %d batches (%d rows/batch)...",
        num_batches,
        batch_size,
    )
    for i in range(num_batches):
        batch_entity_df = entity_df.iloc[i * batch_size : (i + 1) * batch_size].copy()
        batch_res = store.get_historical_features(
            entity_df=batch_entity_df, features=features
        ).to_df()
        dfs.append(batch_res)
    return pd.concat(dfs, ignore_index=True)


def generate_demand_training_dataset(
    store: Optional[FeatureStore] = None,
    engine: Optional[Engine] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    zone_ids: Optional[List[int]] = None,
    features: Optional[List[str]] = None,
    batch_size: int = 10000,
) -> pd.DataFrame:
    """Generate point-in-time demand training dataset from Feast offline store.

    Constructs a full Cartesian grid of (zone, hour), calculates pickup-anchored
    next-hour demand targets, and joins point-in-time features from Feast.

    Args:
        store: Feast FeatureStore instance (defaults to project store).
        engine: SQLAlchemy Engine instance (defaults to project db engine).
        start_time: Start observation timestamp (defaults to DEFAULT_TRAIN_START).
        end_time: End observation timestamp (defaults to DEFAULT_TRAIN_END).
        zone_ids: List of zone IDs (defaults to all zones in database).
        features: List of Feast feature references to retrieve.
        batch_size: Max rows per retrieval batch for memory safety.

    Returns:
        DataFrame containing entity keys, timestamps, features, and target column.
    """
    store = store or get_feature_store()
    engine = engine or get_engine()
    start_time = start_time or DEFAULT_TRAIN_START
    end_time = end_time or DEFAULT_TRAIN_END
    features = features or DEMAND_FEATURES

    zones = zone_ids or get_all_zone_ids(engine)
    logger.info(
        "Generating demand training dataset across %d zones from %s to %s...",
        len(zones),
        start_time,
        end_time,
    )

    # 1. Build full spatial-temporal grid
    entity_grid = build_demand_entity_grid(zones, start_time, end_time)

    # 2. Compute targets from warehouse.trips
    targets_df = compute_demand_targets_from_trips(engine, start_time, end_time)

    # 3. Merge targets onto grid (0 for unobserved zone-hours)
    entity_df = pd.merge(
        entity_grid,
        targets_df,
        on=["zone_id", "event_timestamp"],
        how="left",
    )
    entity_df["target_pickup_count_next_1h"] = (
        entity_df["target_pickup_count_next_1h"].fillna(0).astype("int64")
    )

    # 4. Point-in-time feature join via Feast
    logger.info("Executing Feast point-in-time historical feature join...")
    dataset_df = _fetch_historical_features_in_batches(
        store=store,
        entity_df=entity_df,
        features=features,
        batch_size=batch_size,
    )

    # Ensure clean typing and sort order
    dataset_df["event_timestamp"] = pd.to_datetime(
        dataset_df["event_timestamp"], utc=True
    )
    dataset_df = dataset_df.sort_values(by=["event_timestamp", "zone_id"]).reset_index(
        drop=True
    )

    # Impute missing rolling counts with 0 for unobserved zones
    count_cols = [
        "pickup_count_last_15m",
        "pickup_count_last_1h",
        "pickup_count_last_24h",
        "pickup_count_same_hour_last_week",
    ]
    for col in count_cols:
        if col in dataset_df.columns:
            dataset_df[col] = dataset_df[col].fillna(0).astype("int64")

    # Calendar features fallback from event_timestamp if null
    if "hour_of_day" in dataset_df.columns:
        dataset_df["hour_of_day"] = (
            dataset_df["hour_of_day"]
            .fillna(dataset_df["event_timestamp"].dt.hour)
            .astype("int32")
        )
    if "day_of_week" in dataset_df.columns:
        dataset_df["day_of_week"] = (
            dataset_df["day_of_week"]
            .fillna(dataset_df["event_timestamp"].dt.dayofweek)
            .astype("int32")
        )
    if "is_weekend" in dataset_df.columns:
        dataset_df["is_weekend"] = (
            dataset_df["is_weekend"]
            .fillna(dataset_df["day_of_week"].isin([5, 6]))
            .astype("bool")
        )
    if "is_holiday" in dataset_df.columns:
        dataset_df["is_holiday"] = dataset_df["is_holiday"].fillna(False).astype("bool")

    logger.info(
        "Demand training dataset generated successfully: %d rows, %d columns.",
        len(dataset_df),
        len(dataset_df.columns),
    )
    return dataset_df


def compute_corridor_targets_from_trips(
    engine: Engine,
    start_time: datetime,
    end_time: datetime,
) -> pd.DataFrame:
    """Compute active corridor targets (mean duration in seconds for trips departing in [T, T+1h)).

    Args:
        engine: SQLAlchemy database engine.
        start_time: Start observation timestamp (inclusive).
        end_time: End observation timestamp (exclusive).

    Returns:
        DataFrame with columns ['corridor_id', 'event_timestamp', 'target_avg_duration_next_1h', 'target_trip_count_next_1h'].
    """
    is_postgres = engine.dialect.name == "postgresql"
    table_name = "warehouse.trips" if is_postgres else "trips"

    if is_postgres:
        query = text(f"""
            SELECT
                CONCAT(pickup_zone_id, '_', dropoff_zone_id) AS corridor_id,
                date_trunc('hour', pickup_datetime) AS event_timestamp,
                AVG(trip_duration_seconds) AS target_avg_duration_next_1h,
                COUNT(*) AS target_trip_count_next_1h
            FROM {table_name}
            WHERE pickup_datetime >= :start_time
              AND pickup_datetime < :end_time
            GROUP BY pickup_zone_id, dropoff_zone_id, date_trunc('hour', pickup_datetime);
        """)
    else:
        query = text(f"""
            SELECT
                (pickup_zone_id || '_' || dropoff_zone_id) AS corridor_id,
                strftime('%Y-%m-%d %H:00:00', pickup_datetime) AS event_timestamp,
                AVG(trip_duration_seconds) AS target_avg_duration_next_1h,
                COUNT(*) AS target_trip_count_next_1h
            FROM {table_name}
            WHERE pickup_datetime >= :start_time
              AND pickup_datetime < :end_time
            GROUP BY pickup_zone_id, dropoff_zone_id, strftime('%Y-%m-%d %H:00:00', pickup_datetime);
        """)

    params = (
        {"start_time": start_time, "end_time": end_time}
        if is_postgres
        else {
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        }
    )

    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params=params,
        )

    if not df.empty:
        df["corridor_id"] = df["corridor_id"].astype(str)
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
        df["target_avg_duration_next_1h"] = df["target_avg_duration_next_1h"].astype(
            "float32"
        )
        df["target_trip_count_next_1h"] = df["target_trip_count_next_1h"].astype(
            "int64"
        )
    else:
        df = pd.DataFrame(
            columns=[
                "corridor_id",
                "event_timestamp",
                "target_avg_duration_next_1h",
                "target_trip_count_next_1h",
            ]
        )
        df["corridor_id"] = df["corridor_id"].astype(str)
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
        df["target_avg_duration_next_1h"] = df["target_avg_duration_next_1h"].astype(
            "float32"
        )
        df["target_trip_count_next_1h"] = df["target_trip_count_next_1h"].astype(
            "int64"
        )
    return df


def generate_corridor_training_dataset(
    store: Optional[FeatureStore] = None,
    engine: Optional[Engine] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    features: Optional[List[str]] = None,
    batch_size: int = 10000,
) -> pd.DataFrame:
    """Generate point-in-time corridor duration training dataset from Feast offline store.

    Samples active corridor-hours (with >=1 observed trip departing in [T, T+1h)),
    computes average duration targets, and joins point-in-time features from Feast.

    Args:
        store: Feast FeatureStore instance (defaults to project store).
        engine: SQLAlchemy Engine instance (defaults to project db engine).
        start_time: Start observation timestamp (defaults to DEFAULT_TRAIN_START).
        end_time: End observation timestamp (defaults to DEFAULT_TRAIN_END).
        features: List of Feast feature references to retrieve.
        batch_size: Max rows per retrieval batch for memory safety.

    Returns:
        DataFrame containing entity keys, timestamps, features, and duration target.
    """

    store = store or get_feature_store()
    engine = engine or get_engine()
    start_time = start_time or DEFAULT_TRAIN_START
    end_time = end_time or DEFAULT_TRAIN_END
    features = features or CORRIDOR_FEATURES

    logger.info(
        "Generating corridor duration training dataset from %s to %s...",
        start_time,
        end_time,
    )

    # 1. Compute active corridor departures & ground-truth duration targets
    entity_df = compute_corridor_targets_from_trips(engine, start_time, end_time)

    if entity_df.empty:
        logger.warning(
            "No trips found for corridor training dataset between %s and %s.",
            start_time,
            end_time,
        )
        return entity_df

    # 2. Point-in-time feature join via Feast
    logger.info(
        "Executing Feast point-in-time historical feature join for %d corridor-hour observations...",
        len(entity_df),
    )
    dataset_df = _fetch_historical_features_in_batches(
        store=store,
        entity_df=entity_df,
        features=features,
        batch_size=batch_size,
    )

    # Ensure clean typing and sort order
    dataset_df["event_timestamp"] = pd.to_datetime(
        dataset_df["event_timestamp"], utc=True
    )
    dataset_df = dataset_df.sort_values(
        by=["event_timestamp", "corridor_id"]
    ).reset_index(drop=True)

    # Impute missing corridor features
    if "origin_zone_demand_pressure" in dataset_df.columns:
        dataset_df["origin_zone_demand_pressure"] = (
            dataset_df["origin_zone_demand_pressure"].fillna(0).astype("int64")
        )

    if (
        "avg_duration_last_1h" in dataset_df.columns
        and "target_avg_duration_next_1h" in dataset_df.columns
    ):
        dataset_df["avg_duration_last_1h"] = (
            dataset_df["avg_duration_last_1h"]
            .fillna(dataset_df["target_avg_duration_next_1h"])
            .astype("float32")
        )

    logger.info(
        "Corridor training dataset generated successfully: %d rows, %d columns.",
        len(dataset_df),
        len(dataset_df.columns),
    )
    return dataset_df


def train_val_split_by_time(
    df: pd.DataFrame,
    split_timestamp: datetime = DEFAULT_VAL_SPLIT,
    timestamp_col: str = "event_timestamp",
    min_train_rows: int = 1,
    min_val_rows: int = 1,
    enforce_non_empty: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Split dataset chronologically into train and validation sets (ADR-016).

    Args:
        df: Input DataFrame containing timestamp column.
        split_timestamp: Cutoff timestamp (records strictly < split go to train; >= split go to val).
        timestamp_col: Name of datetime column to split on.
        min_train_rows: Minimum rows required in training partition.
        min_val_rows: Minimum rows required in validation partition.
        enforce_non_empty: If True, raises ValueError if train or validation partition is empty.

    Returns:
        Tuple of (train_df, val_df).

    Raises:
        ValueError: If input DataFrame is empty or if train/validation partition fails row count requirements.
    """
    if df.empty:
        raise ValueError("Cannot split empty dataset.")

    if timestamp_col not in df.columns:
        raise ValueError(f"Timestamp column '{timestamp_col}' missing from dataset.")

    ts_series = pd.to_datetime(df[timestamp_col], utc=True)
    split_ts = pd.to_datetime(split_timestamp, utc=True)

    train_df = df[ts_series < split_ts].copy().reset_index(drop=True)
    val_df = df[ts_series >= split_ts].copy().reset_index(drop=True)

    if enforce_non_empty:
        if len(train_df) < min_train_rows:
            raise ValueError(
                f"Training split has {len(train_df)} rows, minimum required is {min_train_rows} (< {split_ts})."
            )
        if len(val_df) < min_val_rows:
            raise ValueError(
                f"Validation split has {len(val_df)} rows, minimum required is {min_val_rows} (>= {split_ts})."
            )
        # Anti-leakage invariant assertion
        max_train_ts = train_df[timestamp_col].max()
        min_val_ts = val_df[timestamp_col].min()
        if max_train_ts >= min_val_ts:
            raise ValueError(
                f"Temporal leakage detected: max train timestamp {max_train_ts} >= min val timestamp {min_val_ts}."
            )

    logger.info(
        "Chronological split at %s: Train=%d rows, Val=%d rows.",
        split_ts,
        len(train_df),
        len(val_df),
    )
    return train_df, val_df


def validate_dataset_integrity(
    df: pd.DataFrame,
    required_features: List[str],
    target_col: str,
) -> Dict[str, Any]:
    """Validate data quality, target non-negativity, and feature completeness.

    Args:
        df: DataFrame to validate.
        required_features: List of feature column names expected.
        target_col: Target column name.

    Returns:
        Dictionary with validation results.

    Raises:
        ValueError: If validation rules are violated.
    """
    if df.empty:
        raise ValueError("Dataset is empty.")

    if target_col not in df.columns:
        raise ValueError(f"Target column '{target_col}' missing from dataset.")

    # Target sanity check
    invalid_targets = (df[target_col] < 0).sum()
    if invalid_targets > 0:
        raise ValueError(
            f"Found {invalid_targets} negative values in target column '{target_col}'."
        )

    # Check required feature columns
    missing_cols = [c for c in required_features if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Required feature columns missing from dataset: {missing_cols}"
        )

    # Null counts on required features
    null_counts = df[required_features].isnull().sum().to_dict()

    return {
        "total_rows": len(df),
        "columns": list(df.columns),
        "target_min": float(df[target_col].min()),
        "target_max": float(df[target_col].max()),
        "target_mean": float(df[target_col].mean()),
        "null_counts": null_counts,
        "status": "passed",
    }
