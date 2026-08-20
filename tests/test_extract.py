"""Unit tests for the historical TLC Parquet batch extractor module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.extract.batch_puller import (
    TLCParquetExtractor,
    build_tlc_parquet_url,
    generate_year_month_range,
)


def test_build_tlc_parquet_url_valid() -> None:
    """Test URL generation for valid cab types and date combinations."""
    assert (
        build_tlc_parquet_url("yellow", 2023, 1)
        == "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    )
    assert (
        build_tlc_parquet_url("GREEN", 2024, 12)
        == "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-12.parquet"
    )
    assert (
        build_tlc_parquet_url("fhv", 2022, 6)
        == "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2022-06.parquet"
    )


def test_build_tlc_parquet_url_invalid() -> None:
    """Test that invalid inputs raise appropriate ValueError exceptions."""
    with pytest.raises(ValueError, match="Invalid cab_type"):
        build_tlc_parquet_url("invalid_cab", 2023, 1)

    with pytest.raises(ValueError, match="Invalid year"):
        build_tlc_parquet_url("yellow", 1999, 1)

    with pytest.raises(ValueError, match="Invalid month"):
        build_tlc_parquet_url("yellow", 2023, 13)


def test_generate_year_month_range() -> None:
    """Test generating a sequence of (year, month) tuples across year boundaries."""
    dates = generate_year_month_range(2022, 11, 2023, 2)
    expected = [(2022, 11), (2022, 12), (2023, 1), (2023, 2)]
    assert dates == expected

    single_month = generate_year_month_range(2023, 5, 2023, 5)
    assert single_month == [(2023, 5)]


def test_generate_year_month_range_invalid() -> None:
    """Test that start date after end date raises ValueError."""
    with pytest.raises(ValueError, match="Start date .* cannot be after end date"):
        generate_year_month_range(2023, 5, 2023, 4)


def test_download_monthly_file_caching(tmp_path: Path) -> None:
    """Test that an existing cached non-empty Parquet file is returned without network calls."""
    extractor = TLCParquetExtractor(download_dir=tmp_path)
    target_file = tmp_path / "yellow_tripdata_2023-01.parquet"
    target_file.write_bytes(b"dummy_parquet_data")

    with patch("requests.get") as mock_get:
        result_path = extractor.download_monthly_file("yellow", 2023, 1, force=False)
        assert result_path == target_file
        mock_get.assert_not_called()


def test_download_monthly_file_mock_stream(tmp_path: Path) -> None:
    """Test downloading and streaming a Parquet dataset using mock HTTP response."""
    extractor = TLCParquetExtractor(download_dir=tmp_path)

    # Create a small valid PyArrow Parquet payload
    table = pa.Table.from_arrays(
        [pa.array([1, 2]), pa.array(["yellow", "yellow"])],
        names=["vendor_id", "cab_type"],
    )
    temp_buffer_path = tmp_path / "sample.parquet"
    pq.write_table(table, temp_buffer_path)
    fake_parquet_bytes = temp_buffer_path.read_bytes()
    temp_buffer_path.unlink()

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.iter_content = MagicMock(return_value=[fake_parquet_bytes])

    with patch("requests.get", return_value=mock_response) as mock_get:
        out_path = extractor.download_monthly_file("yellow", 2023, 1, force=True)
        assert out_path.exists()
        assert out_path.name == "yellow_tripdata_2023-01.parquet"
        assert out_path.stat().st_size > 0
        mock_get.assert_called_once()

        # Read back table to confirm valid Parquet structure
        read_table = pq.read_table(out_path)
        assert read_table.num_rows == 2
        assert read_table.column_names == ["vendor_id", "cab_type"]


def test_download_monthly_file_retry_and_failure(tmp_path: Path) -> None:
    """Test retry mechanism on network errors and raise RuntimeError on final failure."""
    extractor = TLCParquetExtractor(download_dir=tmp_path)

    with (
        patch("requests.get", side_effect=Exception("Connection reset")) as mock_get,
        patch("time.sleep"),
    ):
        with pytest.raises(RuntimeError, match="Failed to download TLC Parquet file"):
            extractor.download_monthly_file("yellow", 2023, 1, retries=2)

        assert mock_get.call_count == 2


def test_download_date_range_batch(tmp_path: Path) -> None:
    """Test batch extraction across a date range."""
    extractor = TLCParquetExtractor(download_dir=tmp_path)

    dummy_bytes = b"parquet_header_data"
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.iter_content = MagicMock(return_value=[dummy_bytes])

    with patch("requests.get", return_value=mock_response):
        paths = extractor.download_date_range(
            cab_type="green",
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=2,
            force=True,
        )
        assert len(paths) == 2
        assert paths[0].name == "green_tripdata_2023-01.parquet"
        assert paths[1].name == "green_tripdata_2023-02.parquet"
