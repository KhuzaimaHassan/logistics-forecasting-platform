"""Cloudflare R2 (S3-compatible) artifact and database backup manager (ADR-007)."""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from prefect import task

from src.common.config import get_settings

logger = logging.getLogger(__name__)


class R2BackupManager:
    """Manages syncing MLflow artifacts and database dumps to Cloudflare R2."""

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
    ) -> None:
        settings = get_settings()
        self.bucket_name = (
            bucket_name
            if bucket_name is not None
            else (os.getenv("R2_BUCKET_NAME") or settings.r2_bucket_name)
        )
        self.endpoint_url = (
            endpoint_url
            if endpoint_url is not None
            else (os.getenv("R2_ENDPOINT_URL") or settings.r2_endpoint_url)
        )
        self.access_key_id = (
            access_key_id
            if access_key_id is not None
            else (os.getenv("R2_ACCESS_KEY_ID") or settings.r2_access_key_id)
        )
        self.secret_access_key = (
            secret_access_key
            if secret_access_key is not None
            else (os.getenv("R2_SECRET_ACCESS_KEY") or settings.r2_secret_access_key)
        )
        self._s3_client = None

    def is_configured(self) -> bool:
        """Check whether all required Cloudflare R2 credentials are present."""
        return bool(
            self.bucket_name
            and self.endpoint_url
            and self.access_key_id
            and self.secret_access_key
        )

    def get_s3_client(self):
        """Return an instantiated S3 client pointing to Cloudflare R2."""
        if not self.is_configured():
            raise ValueError(
                "Cloudflare R2 credentials are incomplete. Required: "
                "R2_BUCKET_NAME, R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY."
            )
        if self._s3_client is None:
            self._s3_client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
            )
        return self._s3_client

    def upload_file(self, local_path: str | Path, s3_key: str) -> bool:
        """Upload a single local file to Cloudflare R2.

        Args:
            local_path: Absolute or relative local path to the file.
            s3_key: Remote object key within the R2 bucket.

        Returns:
            True if upload succeeded, False otherwise.
        """
        path = Path(local_path)
        if not path.exists() or not path.is_file():
            logger.warning(
                "Local file '%s' does not exist or is not a file.", local_path
            )
            return False

        if not self.is_configured():
            logger.warning(
                "Cloudflare R2 is not configured; skipping upload of '%s'.", local_path
            )
            return False

        try:
            client = self.get_s3_client()
            logger.info(
                "Uploading '%s' to r2://%s/%s", local_path, self.bucket_name, s3_key
            )
            client.upload_file(str(path), self.bucket_name, s3_key)
            return True
        except ClientError as exc:
            logger.error("Failed to upload '%s' to R2: %s", local_path, exc)
            return False

    def upload_directory(
        self, local_dir: str | Path, s3_prefix: str = ""
    ) -> Dict[str, Any]:
        """Recursively upload all files in a local directory to Cloudflare R2.

        Args:
            local_dir: Local root directory path.
            s3_prefix: S3 key prefix for uploaded files.

        Returns:
            Dictionary summarizing total files scanned, uploaded, and skipped.
        """
        dir_path = Path(local_dir)
        summary = {
            "total_files": 0,
            "uploaded": 0,
            "failed": 0,
            "skipped": 0,
            "bytes_uploaded": 0,
            "status": "success",
        }

        if not dir_path.exists() or not dir_path.is_dir():
            logger.warning("Directory '%s' does not exist.", local_dir)
            summary["status"] = "directory_not_found"
            return summary

        if not self.is_configured():
            logger.warning(
                "Cloudflare R2 is not configured; skipping directory upload of '%s'.",
                local_dir,
            )
            summary["status"] = "unconfigured_noop"
            return summary

        client = self.get_s3_client()
        normalized_prefix = s3_prefix.strip("/")

        for file_path in dir_path.rglob("*"):
            if not file_path.is_file():
                continue

            summary["total_files"] += 1
            rel_path = file_path.relative_to(dir_path).as_posix()
            s3_key = (
                f"{normalized_prefix}/{rel_path}" if normalized_prefix else rel_path
            )

            try:
                file_size = file_path.stat().st_size
                client.upload_file(str(file_path), self.bucket_name, s3_key)
                summary["uploaded"] += 1
                summary["bytes_uploaded"] += file_size
            except Exception as exc:
                logger.error(
                    "Failed to upload '%s' to '%s': %s", file_path, s3_key, exc
                )
                summary["failed"] += 1

        if summary["failed"] > 0:
            summary["status"] = "partial_failure"

        logger.info(
            "Directory upload complete for '%s': %d/%d uploaded (%d bytes).",
            local_dir,
            summary["uploaded"],
            summary["total_files"],
            summary["bytes_uploaded"],
        )
        return summary

    def backup_mlflow_artifacts(
        self,
        local_mlruns_dir: str | Path = "./mlruns",
        s3_prefix: str = "mlruns-backup",
    ) -> Dict[str, Any]:
        """Backup local MLflow runs and artifacts to Cloudflare R2."""
        return self.upload_directory(local_mlruns_dir, s3_prefix=s3_prefix)

    def backup_database_dump(
        self, dump_path: str | Path, s3_prefix: str = "db-dumps"
    ) -> bool:
        """Upload a compressed PostgreSQL database dump to Cloudflare R2."""
        path = Path(dump_path)
        s3_key = f"{s3_prefix.strip('/')}/{path.name}"
        return self.upload_file(path, s3_key=s3_key)

    def list_objects(self, s3_prefix: str = "") -> list[Dict[str, Any]]:
        """List objects in the configured Cloudflare R2 bucket matching prefix."""
        if not self.is_configured():
            logger.warning("Cloudflare R2 is not configured; cannot list objects.")
            return []

        client = self.get_s3_client()
        try:
            response = client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=s3_prefix.strip("/"),
            )
            contents = response.get("Contents", [])
            return [
                {
                    "key": item["Key"],
                    "size": item["Size"],
                    "last_modified": item["LastModified"].isoformat(),
                }
                for item in contents
            ]
        except Exception as exc:
            logger.error(
                "Failed to list objects in bucket '%s': %s", self.bucket_name, exc
            )
            return []


@task(name="backup-artifacts-to-r2", retries=2, retry_delay_seconds=10)
def backup_artifacts_to_r2_task(
    local_mlruns_dir: str = "./mlruns",
    s3_prefix: str = "mlruns-backup",
) -> Dict[str, Any]:
    """Prefect task to backup MLflow artifacts to Cloudflare R2."""
    manager = R2BackupManager()
    if not manager.is_configured():
        logger.info("R2 credentials not configured. Skipping artifact backup task.")
        return {"status": "skipped", "reason": "not_configured"}

    return manager.backup_mlflow_artifacts(
        local_mlruns_dir=local_mlruns_dir, s3_prefix=s3_prefix
    )
