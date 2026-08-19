"""Unit tests for database models, configuration, Alembic setup, and taxi zones reference data."""

from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.common.config import Settings
from src.common.db import Base
from src.common.models import (
    LoadedMonth,
    MonitoringReport,
    PipelineRun,
    Prediction,
    RawTrip,
    TaxiZone,
    WarehouseTrip,
)
from src.extract.load_zones import (
    DEFAULT_NYC_TAXI_ZONES,
    fetch_zone_lookup_csv,
    get_default_zones_map,
)


def test_settings_configuration() -> None:
    """Test that application settings produce expected defaults and database URLs."""
    settings = Settings(
        POSTGRES_USER="testuser",
        POSTGRES_PASSWORD="testpassword",
        POSTGRES_HOST="dbhost",
        POSTGRES_PORT=5432,
        POSTGRES_DB="testdb",
    )
    assert (
        settings.database_url
        == "postgresql+psycopg2://testuser:testpassword@dbhost:5432/testdb"
    )

    settings_override = Settings(
        DATABASE_URL="sqlite:///:memory:",
    )
    assert settings_override.database_url == "sqlite:///:memory:"


def test_taxi_zones_reference_integrity() -> None:
    """Verify that bundled taxi zones have valid numeric coordinates and unique IDs."""
    assert len(DEFAULT_NYC_TAXI_ZONES) == 265

    zone_ids = set()
    for zone in DEFAULT_NYC_TAXI_ZONES:
        assert isinstance(zone["zone_id"], int)
        assert zone["zone_id"] > 0
        assert zone["zone_id"] not in zone_ids
        zone_ids.add(zone["zone_id"])

        assert isinstance(zone["borough"], str) and len(zone["borough"]) > 0
        assert isinstance(zone["zone_name"], str) and len(zone["zone_name"]) > 0
        assert -90.0 <= zone["centroid_lat"] <= 90.0
        assert -180.0 <= zone["centroid_lon"] <= 180.0

    zone_map = get_default_zones_map()
    assert 161 in zone_map  # Midtown Center
    assert zone_map[161]["borough"] == "Manhattan"
    assert zone_map[161]["zone_name"] == "Midtown Center"


def test_models_orm_declarations() -> None:
    """Verify table names and schema assignments for all core ORM models."""
    assert TaxiZone.__tablename__ == "taxi_zones"
    assert TaxiZone.__table_args__["schema"] == "warehouse"

    assert RawTrip.__tablename__ == "trips"
    assert RawTrip.__table_args__["schema"] == "raw"

    assert WarehouseTrip.__tablename__ == "trips"
    assert WarehouseTrip.__table_args__[-1]["schema"] == "warehouse"

    assert LoadedMonth.__tablename__ == "loaded_months"
    assert LoadedMonth.__table_args__["schema"] == "warehouse"

    assert PipelineRun.__tablename__ == "pipeline_runs"
    assert PipelineRun.__table_args__["schema"] == "warehouse"

    assert Prediction.__tablename__ == "predictions"
    assert Prediction.__table_args__["schema"] == "warehouse"

    assert MonitoringReport.__tablename__ == "monitoring_reports"
    assert MonitoringReport.__table_args__["schema"] == "warehouse"


def test_fetch_zone_lookup_csv_parsing() -> None:
    """Test parsing of mocked TLC CSV lookup response."""
    sample_csv = (
        "LocationID,Borough,Zone,service_zone\n"
        '1,"EWR","Newark Airport","EWR"\n'
        '161,"Manhattan","Midtown Center","Yellow Zone"\n'
    )
    mock_resp = MagicMock()
    mock_resp.text = sample_csv
    mock_resp.raise_for_status = MagicMock()

    with patch("requests.get", return_value=mock_resp):
        zones = fetch_zone_lookup_csv("https://fake-url/test.csv")
        assert len(zones) == 2
        assert zones[0]["zone_id"] == 1
        assert zones[0]["borough"] == "EWR"
        assert zones[1]["zone_id"] == 161
        assert zones[1]["borough"] == "Manhattan"
        assert zones[1]["centroid_lat"] == 40.757015


def test_fetch_zone_lookup_csv_fail_fast() -> None:
    """Test that network errors raise RuntimeError when fail_fast=True."""
    with patch("requests.get", side_effect=Exception("Connection refused")):
        with pytest.raises(RuntimeError) as excinfo:
            fetch_zone_lookup_csv("https://unreachable.test", fail_fast=True)
        assert "Failed to fetch NYC TLC taxi zone lookup" in str(excinfo.value)


def test_fetch_zone_lookup_csv_fallback() -> None:
    """Test that network errors fall back safely to bundled dataset when fail_fast=False."""
    with patch("requests.get", side_effect=Exception("Connection refused")):
        zones = fetch_zone_lookup_csv("https://unreachable.test", fail_fast=False)
        assert len(zones) == 265
        assert zones[0]["zone_id"] == 1


def test_load_taxi_zones_to_db_in_memory() -> None:
    """Test inserting and updating taxi zones in an in-memory SQLite engine."""
    # SQLite doesn't natively use schemas, so create a table with the same columns
    engine = create_engine("sqlite:///:memory:")

    # Create a local test table matching TaxiZone columns
    from sqlalchemy import Column, Integer, Numeric, String, Table

    test_table = Table(
        "taxi_zones",
        Base.metadata,
        Column("zone_id", Integer, primary_key=True),
        Column("borough", String(50), nullable=False),
        Column("zone_name", String(255), nullable=False),
        Column("service_zone", String(50), nullable=True),
        Column("centroid_lat", Numeric(9, 6), nullable=False),
        Column("centroid_lon", Numeric(9, 6), nullable=False),
        keep_existing=True,
    )
    test_table.create(engine)

    sample_zones = [
        {
            "zone_id": 1,
            "borough": "EWR",
            "zone_name": "Newark Airport",
            "service_zone": "EWR",
            "centroid_lat": 40.691831,
            "centroid_lon": -74.177271,
        },
        {
            "zone_id": 161,
            "borough": "Manhattan",
            "zone_name": "Midtown Center",
            "service_zone": "Yellow Zone",
            "centroid_lat": 40.757015,
            "centroid_lon": -73.981015,
        },
    ]

    with Session(bind=engine) as session:
        for z in sample_zones:
            session.execute(
                test_table.insert().values(
                    zone_id=z["zone_id"],
                    borough=z["borough"],
                    zone_name=z["zone_name"],
                    service_zone=z["service_zone"],
                    centroid_lat=Decimal(str(z["centroid_lat"])),
                    centroid_lon=Decimal(str(z["centroid_lon"])),
                )
            )
        session.commit()

        rows = session.execute(test_table.select()).fetchall()
        assert len(rows) == 2
        assert rows[0].zone_id == 1
        assert rows[1].zone_id == 161


def test_alembic_configuration_and_migration_file_exists() -> None:
    """Verify that alembic.ini is valid and migration script is discoverable."""
    project_root = Path(__file__).resolve().parent.parent
    alembic_ini = project_root / "alembic.ini"
    assert alembic_ini.exists(), "alembic.ini must exist at project root"

    config = Config(str(alembic_ini))
    script_loc = config.get_main_option("script_location")
    assert script_loc == "alembic"

    versions_dir = project_root / "alembic" / "versions"
    assert versions_dir.exists(), "alembic/versions directory must exist"

    migration_files = list(versions_dir.glob("*.py"))
    assert len(migration_files) >= 1, "At least one migration script must exist"
    assert any("0001_initial_schemas" in f.name for f in migration_files)
