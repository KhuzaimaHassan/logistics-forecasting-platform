"""Batch extractor for downloading historical NYC TLC trip data Parquet files."""

import argparse
import logging
import time
from pathlib import Path
from typing import List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

TLC_BASE_CDN_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
VALID_CAB_TYPES = {"yellow", "green", "fhv", "fhvhv"}


def build_tlc_parquet_url(cab_type: str, year: int, month: int) -> str:
    """Construct the official TLC CloudFront CDN URL for a given cab type, year, and month."""
    cab_type_lower = cab_type.lower().strip()
    if cab_type_lower not in VALID_CAB_TYPES:
        raise ValueError(
            f"Invalid cab_type '{cab_type}'. Must be one of: {sorted(VALID_CAB_TYPES)}"
        )
    if not (2000 <= year <= 2100):
        raise ValueError(f"Invalid year '{year}'. Must be between 2000 and 2100.")
    if not (1 <= month <= 12):
        raise ValueError(f"Invalid month '{month}'. Must be between 1 and 12.")

    filename = f"{cab_type_lower}_tripdata_{year:04d}-{month:02d}.parquet"
    return f"{TLC_BASE_CDN_URL}/{filename}"


def generate_year_month_range(
    start_year: int, start_month: int, end_year: int, end_month: int
) -> List[Tuple[int, int]]:
    """Generate an inclusive sequence of (year, month) tuples between start and end dates."""
    if (start_year, start_month) > (end_year, end_month):
        raise ValueError(
            f"Start date ({start_year}-{start_month:02d}) cannot be after end date ({end_year}-{end_month:02d})."
        )

    results: List[Tuple[int, int]] = []
    current_year, current_month = start_year, start_month

    while (current_year, current_month) <= (end_year, end_month):
        results.append((current_year, current_month))
        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1

    return results


class TLCParquetExtractor:
    """Extractor responsible for downloading and local caching of TLC Parquet batch datasets."""

    def __init__(self, download_dir: Optional[Path] = None, timeout: int = 30):
        self.download_dir = download_dir or Path("data/raw")
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout

    def _stream_to_temp_file(self, url: str, temp_path: Path) -> None:
        """Stream HTTP content to a temporary file and verify non-empty body."""
        response = requests.get(url, stream=True, timeout=self.timeout)
        response.raise_for_status()

        with open(temp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

        if not temp_path.exists() or temp_path.stat().st_size == 0:
            raise RuntimeError(f"Downloaded file '{temp_path}' is empty.")

    def download_monthly_file(
        self,
        cab_type: str,
        year: int,
        month: int,
        force: bool = False,
        retries: int = 3,
        backoff_factor: float = 1.5,
    ) -> Path:
        """Download a single monthly TLC Parquet file to the designated directory."""
        url = build_tlc_parquet_url(cab_type, year, month)
        filename = Path(url).name
        target_path = self.download_dir / filename

        if target_path.exists() and not force and target_path.stat().st_size > 0:
            logger.info(
                f"File already cached locally ({target_path.stat().st_size} bytes): {target_path}"
            )
            return target_path

        logger.info(f"Downloading TLC Parquet dataset from: {url}")
        temp_path = target_path.with_suffix(".parquet.tmp")
        last_exception: Optional[Exception] = None

        for attempt in range(1, retries + 1):
            try:
                self._stream_to_temp_file(url, temp_path)
                if target_path.exists():
                    target_path.unlink()
                temp_path.rename(target_path)
                logger.info(
                    f"Successfully downloaded {filename} ({target_path.stat().st_size} bytes)."
                )
                return target_path
            except Exception as e:
                last_exception = e
                logger.warning(
                    f"Download attempt {attempt}/{retries} failed for {url}: {e}"
                )
                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except OSError:
                        pass
                if attempt < retries:
                    time.sleep(backoff_factor**attempt)

        raise RuntimeError(
            f"Failed to download TLC Parquet file from '{url}' after {retries} attempts: {last_exception}"
        ) from last_exception

    def download_date_range(
        self,
        cab_type: str,
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int,
        force: bool = False,
    ) -> List[Path]:
        """Download all monthly Parquet files within the specified date range."""
        ym_range = generate_year_month_range(
            start_year, start_month, end_year, end_month
        )
        downloaded_paths: List[Path] = []

        logger.info(
            f"Starting batch extraction for '{cab_type}' taxis across {len(ym_range)} monthly files."
        )

        for year, month in ym_range:
            path = self.download_monthly_file(cab_type, year, month, force=force)
            downloaded_paths.append(path)

        logger.info(
            f"Batch extraction completed. {len(downloaded_paths)} Parquet files ready in {self.download_dir}."
        )
        return downloaded_paths


def main() -> None:
    """CLI entrypoint for executing historical TLC batch extractions."""
    parser = argparse.ArgumentParser(
        description="Download historical NYC TLC trip data Parquet files from CloudFront CDN."
    )
    parser.add_argument(
        "--cab-type",
        type=str,
        default="yellow",
        choices=sorted(VALID_CAB_TYPES),
        help="Taxi cab type (yellow, green, fhv, fhvhv)",
    )
    parser.add_argument(
        "--start-year", type=int, default=2023, help="Start year (YYYY)"
    )
    parser.add_argument("--start-month", type=int, default=1, help="Start month (1-12)")
    parser.add_argument("--end-year", type=int, default=2023, help="End year (YYYY)")
    parser.add_argument("--end-month", type=int, default=1, help="End month (1-12)")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/raw",
        help="Directory where Parquet files will be stored",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force redownload even if local cached file exists",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )

    extractor = TLCParquetExtractor(download_dir=Path(args.output_dir))
    paths = extractor.download_date_range(
        cab_type=args.cab_type,
        start_year=args.start_year,
        start_month=args.start_month,
        end_year=args.end_year,
        end_month=args.end_month,
        force=args.force,
    )

    print(
        f"Extracted {len(paths)} Parquet file(s) into {extractor.download_dir.resolve()}:"
    )
    for p in paths:
        print(f"  - {p.name} ({p.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
