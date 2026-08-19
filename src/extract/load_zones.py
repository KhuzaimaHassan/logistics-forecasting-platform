"""Loader script for NYC TLC Taxi Zones reference data and precomputed centroids."""

import csv
import io
import logging
from typing import List, Optional

import requests
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.common.db import get_engine
from src.common.models import TaxiZone
from src.extract.zones_reference import (
    DEFAULT_NYC_TAXI_ZONES,
    ZoneData,
    get_default_zones_map,
)

logger = logging.getLogger(__name__)

TLC_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"


def fetch_zone_lookup_csv(
    url: str = TLC_ZONE_LOOKUP_URL, timeout: int = 15
) -> List[ZoneData]:
    """Download TLC Taxi Zone lookup CSV from CDN and combine with known centroids."""
    logger.info(f"Fetching NYC TLC Taxi Zone lookup data from: {url}")
    default_map = get_default_zones_map()
    zones: List[ZoneData] = []

    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        csv_text = response.text
        reader = csv.DictReader(io.StringIO(csv_text))

        for row in reader:
            location_id_str = row.get("LocationID", "").strip()
            if not location_id_str.isdigit():
                continue
            zone_id = int(location_id_str)
            borough = row.get("Borough", "Unknown").strip() or "Unknown"
            zone_name = row.get("Zone", "Unknown").strip() or "Unknown"
            service_zone = row.get("service_zone", "").strip() or None

            # Look up geometric centroid from precomputed reference map
            ref = default_map.get(zone_id)
            lat = ref["centroid_lat"] if ref else 40.712800
            lon = ref["centroid_lon"] if ref else -74.006000

            zones.append(
                {
                    "zone_id": zone_id,
                    "borough": borough,
                    "zone_name": zone_name,
                    "service_zone": service_zone,
                    "centroid_lat": lat,
                    "centroid_lon": lon,
                }
            )
        logger.info(
            f"Successfully fetched and parsed {len(zones)} taxi zones from TLC CDN."
        )
        return zones
    except Exception as e:
        logger.warning(
            f"Failed to fetch TLC zone lookup CSV from network ({e}). Falling back to bundled reference dataset."
        )
        return DEFAULT_NYC_TAXI_ZONES


def load_taxi_zones_to_db(
    zones: Optional[List[ZoneData]] = None,
    engine: Optional[Engine] = None,
) -> int:
    """Insert or upsert taxi zones reference records into warehouse.taxi_zones."""
    if zones is None:
        zones = DEFAULT_NYC_TAXI_ZONES

    eng = engine or get_engine()
    loaded_count = 0

    with Session(bind=eng) as session:
        for z in zones:
            existing = session.query(TaxiZone).filter_by(zone_id=z["zone_id"]).first()
            if existing:
                existing.borough = z["borough"]
                existing.zone_name = z["zone_name"]
                existing.service_zone = z.get("service_zone")
                existing.centroid_lat = z["centroid_lat"]
                existing.centroid_lon = z["centroid_lon"]
            else:
                new_zone = TaxiZone(
                    zone_id=z["zone_id"],
                    borough=z["borough"],
                    zone_name=z["zone_name"],
                    service_zone=z.get("service_zone"),
                    centroid_lat=z["centroid_lat"],
                    centroid_lon=z["centroid_lon"],
                )
                session.add(new_zone)
            loaded_count += 1
        session.commit()

    logger.info(
        f"Successfully loaded {loaded_count} taxi zones into warehouse.taxi_zones."
    )
    return loaded_count


def main() -> None:
    """CLI entrypoint to fetch and load NYC Taxi Zones."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    zones = fetch_zone_lookup_csv()
    count = load_taxi_zones_to_db(zones)
    print(f"Loaded {count} NYC Taxi Zones into database.")


if __name__ == "__main__":
    main()
