"""Live verification script for Cloudflare R2 backup (ADR-007).

Executes a live upload of local MLflow artifacts or test model artifacts to Cloudflare R2,
then queries the R2 bucket via S3 list_objects_v2 API to physically confirm object presence.
"""

import logging
import sys
import tempfile
from pathlib import Path

from src.training.r2_backup import R2BackupManager

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    manager = R2BackupManager()

    print("=" * 70)
    print("CLOUDFLARE R2 LIVE BACKUP VERIFICATION (ADR-007)")
    print("=" * 70)

    if not manager.is_configured():
        print("\n[!] Cloudflare R2 is NOT fully configured in the environment.")
        print("Required environment variables:")
        print(f"  R2_BUCKET_NAME:        {manager.bucket_name or '(not set)'}")
        print(f"  R2_ENDPOINT_URL:       {manager.endpoint_url or '(not set)'}")
        print(
            f"  R2_ACCESS_KEY_ID:      {'***' if manager.access_key_id else '(not set)'}"
        )
        print(
            f"  R2_SECRET_ACCESS_KEY:  {'***' if manager.secret_access_key else '(not set)'}"
        )
        print(
            "\nTo run a live upload, export these variables or add them to your .env file."
        )
        sys.exit(1)

    print("\n[+] Cloudflare R2 Configuration Detected:")
    print(f"  Bucket Name:   {manager.bucket_name}")
    print(f"  Endpoint URL:  {manager.endpoint_url}")
    print(
        f"  Access Key ID: {manager.access_key_id[:6]}...{manager.access_key_id[-4:] if len(manager.access_key_id) > 10 else ''}"
    )

    # Check local mlruns directory or create representative artifact
    mlruns_path = Path("./mlruns")
    temp_dir_obj = None

    if mlruns_path.exists() and any(mlruns_path.iterdir()):
        upload_source = mlruns_path
        prefix = "mlflow-artifacts-live"
        print(
            f"\n[+] Uploading live local MLflow artifacts from: {upload_source.resolve()}"
        )
    else:
        temp_dir_obj = tempfile.TemporaryDirectory()
        upload_source = Path(temp_dir_obj.name)
        # Create representative artifact structure
        model_dir = upload_source / "models" / "demand_lightgbm_v1"
        model_dir.mkdir(parents=True, exist_ok=True)
        (model_dir / "MLmodel").write_text(
            "artifact_path: model\nflavors:\n  lightgbm:\n    lgb_version: 4.6.0\n"
        )
        (model_dir / "model.pkl").write_bytes(
            b"LGBM_BINARY_DATA_REPRESENTATIVE_M3_5_PROOF"
        )
        (upload_source / "metadata.json").write_text(
            '{"phase": "M3-5", "pipeline": "training_orchestrator"}'
        )
        prefix = "mlflow-artifacts-live"
        print(
            f"\n[+] Created representative MLflow artifact structure at: {upload_source.resolve()}"
        )

    try:
        print(f"--- 1. Syncing Artifacts to r2://{manager.bucket_name}/{prefix}/ ---")
        summary = manager.upload_directory(upload_source, s3_prefix=prefix)
        print(f"Upload Summary: {summary}")
        assert summary["status"] == "success", f"Upload failed: {summary}"
        assert summary["uploaded"] > 0, "Zero files uploaded"

        print(
            "\n--- 2. Verifying Objects Physically in Bucket via list_objects_v2 API ---"
        )
        objects = manager.list_objects(s3_prefix=prefix)
        print(f"Found {len(objects)} objects under prefix '{prefix}/':")
        for obj in objects:
            print(f"  - Key:           {obj['key']}")
            print(f"    Size:          {obj['size']} bytes")
            print(f"    Last Modified: {obj['last_modified']}")
            print("-" * 50)

        assert (
            len(objects) >= summary["uploaded"]
        ), "Object count in R2 bucket is less than uploaded count"

        print("\n" + "=" * 70)
        print("CLOUDFLARE R2 LIVE BACKUP VERIFICATION: PASSED (ADR-007 FULLY CLOSED)")
        print("=" * 70)

    finally:
        if temp_dir_obj:
            temp_dir_obj.cleanup()


if __name__ == "__main__":
    main()
