"""Verification script to execute BatchTransformer against real yellow_tripdata_2023-01.parquet data."""

from pathlib import Path
import pandas as pd
from src.transform.batch_transformer import BatchTransformer

def main() -> None:
    parquet_path = Path("data/raw/yellow_tripdata_2023-01.parquet")
    if not parquet_path.exists():
        raise FileNotFoundError(f"Dataset not found at {parquet_path}")

    print("Reading and transforming real dataset: yellow_tripdata_2023-01.parquet...")
    transformer = BatchTransformer()
    clean_df, report = transformer.transform_parquet_file(parquet_path, cab_type="yellow")

    print("\n==================================================")
    print("=== M1-3 REAL DATA TRANSFORMATION AUDIT REPORT ===")
    print("==================================================")
    print(report.summary())

    print("\n==================================================")
    print("=== SPOT-CHECK: 5 REAL CLEANED ROWS VS RAW SOURCE ===")
    print("==================================================")

    raw_df = pd.read_parquet(parquet_path)

    sample_indices = [0, 1000, 50000, 1000000, 2000000]
    for idx in sample_indices:
        clean_row = clean_df.iloc[idx]
        p_dt = clean_row["pickup_datetime"]
        d_dt = clean_row["dropoff_datetime"]
        pu = clean_row["pickup_zone_id"]
        do = clean_row["dropoff_zone_id"]

        raw_match = raw_df[
            (raw_df["tpep_pickup_datetime"] == p_dt) &
            (raw_df["tpep_dropoff_datetime"] == d_dt) &
            (raw_df["PULocationID"] == pu) &
            (raw_df["DOLocationID"] == do)
        ].iloc[0]

        print(f"\n[Sample Row #{idx + 1:,}]")
        print(f"  RAW SOURCE:  Pickup={raw_match['tpep_pickup_datetime']} | Dropoff={raw_match['tpep_dropoff_datetime']} | Dist={raw_match['trip_distance']} mi | PULocationID={raw_match['PULocationID']} | DOLocationID={raw_match['DOLocationID']} | Fare=${raw_match['fare_amount']:.2f}")
        print(f"  TRANSFORMED: Duration={clean_row['trip_duration_seconds']:.0f}s ({clean_row['trip_duration_seconds']/60:.1f}m) | 15m_Bin={clean_row['time_bin_15m']} | Dist={clean_row['trip_distance_km']:.2f} km | PU_Zone={clean_row['pickup_zone_id']} | DO_Zone={clean_row['dropoff_zone_id']} | Speed={clean_row['average_speed_mph']:.1f} mph | Weekend={clean_row['is_weekend']} | TripID={clean_row['trip_id']}")

if __name__ == "__main__":
    main()
