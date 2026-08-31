"""Unit tests for MLflow experiment tracking helpers and Cloudflare R2 backup manager (M3-1)."""

from unittest.mock import MagicMock, patch

import mlflow
import pytest

from src.common.mlflow_utils import (
    DEMAND_EXPERIMENT_NAME,
    DURATION_EXPERIMENT_NAME,
    get_mlflow_client,
    get_or_create_experiment,
    get_tracking_uri,
    setup_mlflow,
)
from src.training.r2_backup import R2BackupManager, backup_artifacts_to_r2_task


@pytest.fixture
def temp_mlflow_tracking_dir(tmp_path):
    """Fixture providing a temporary SQLite-backed MLflow tracking store."""
    db_file = tmp_path / "test_mlflow.db"
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    uri = f"sqlite:///{db_file.as_posix()}"
    old_uri = mlflow.get_tracking_uri()
    setup_mlflow(uri)
    yield uri, artifacts_dir
    mlflow.set_tracking_uri(old_uri)


def test_get_tracking_uri_default(monkeypatch):
    """Test get_tracking_uri returns setting or env var."""
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://custom-mlflow:5000")
    assert get_tracking_uri() == "http://custom-mlflow:5000"


def test_get_or_create_experiment_and_run(temp_mlflow_tracking_dir):
    """Test creating canonical MLflow experiments and logging runs."""
    uri, artifacts_dir = temp_mlflow_tracking_dir
    client = get_mlflow_client(uri)

    exp_id_1 = get_or_create_experiment(
        DEMAND_EXPERIMENT_NAME,
        artifact_location=artifacts_dir.as_uri(),
        client=client,
    )
    assert exp_id_1 is not None

    # Idempotent retrieval
    exp_id_2 = get_or_create_experiment(DEMAND_EXPERIMENT_NAME, client=client)
    assert exp_id_1 == exp_id_2

    # Create duration experiment
    exp_id_dur = get_or_create_experiment(DURATION_EXPERIMENT_NAME, client=client)
    assert exp_id_dur is not None
    assert exp_id_dur != exp_id_1

    # Log a test run
    with mlflow.start_run(experiment_id=exp_id_1, run_name="test_baseline_run"):
        mlflow.log_param("model_type", "seasonal_naive")
        mlflow.log_metric("val_mae", 4.25)
        mlflow.log_metric("val_rmse", 8.12)
        mlflow.set_tag("stage", "baseline")

    # Verify run recorded in client
    runs = client.search_runs(experiment_ids=[exp_id_1])
    assert len(runs) == 1
    assert runs[0].data.params["model_type"] == "seasonal_naive"
    assert runs[0].data.metrics["val_mae"] == pytest.approx(4.25)


def test_r2_backup_manager_is_configured():
    """Test is_configured returns False when credentials missing and True when complete."""
    unconfigured = R2BackupManager(
        bucket_name="",
        endpoint_url="",
        access_key_id="",
        secret_access_key="",
    )
    assert unconfigured.is_configured() is False

    configured = R2BackupManager(
        bucket_name="logistics-mlflow-artifacts",
        endpoint_url="https://r2.cloudflarestorage.com",
        access_key_id="test_key_id",
        secret_access_key="test_secret_key",
    )
    assert configured.is_configured() is True


def test_r2_upload_file_unconfigured(tmp_path):
    """Test upload_file returns False gracefully when R2 is not configured."""
    manager = R2BackupManager()
    manager.bucket_name = None

    test_file = tmp_path / "dummy.txt"
    test_file.write_text("sample content")

    result = manager.upload_file(test_file, "backup/dummy.txt")
    assert result is False


def test_r2_upload_file_success(tmp_path):
    """Test successful single file upload to R2 with mocked boto3."""
    manager = R2BackupManager(
        bucket_name="test-bucket",
        endpoint_url="https://r2.test.com",
        access_key_id="key123",
        secret_access_key="sec123",
    )

    test_file = tmp_path / "model.pkl"
    test_file.write_bytes(b"dummy model bytes")

    mock_client = MagicMock()
    with patch.object(manager, "get_s3_client", return_value=mock_client):
        result = manager.upload_file(test_file, "models/model.pkl")
        assert result is True
        mock_client.upload_file.assert_called_once_with(
            str(test_file), "test-bucket", "models/model.pkl"
        )


def test_r2_upload_directory_success(tmp_path):
    """Test recursive directory upload with mocked boto3 client."""
    manager = R2BackupManager(
        bucket_name="test-bucket",
        endpoint_url="https://r2.test.com",
        access_key_id="key123",
        secret_access_key="sec123",
    )

    mlruns_dir = tmp_path / "mlruns"
    sub_dir = mlruns_dir / "0" / "artifacts"
    sub_dir.mkdir(parents=True, exist_ok=True)

    (mlruns_dir / "meta.yaml").write_text("experiment metadata")
    (sub_dir / "weights.bin").write_bytes(b"12345")

    mock_client = MagicMock()
    with patch.object(manager, "get_s3_client", return_value=mock_client):
        summary = manager.backup_mlflow_artifacts(
            local_mlruns_dir=mlruns_dir, s3_prefix="mlruns-backup"
        )
        assert summary["status"] == "success"
        assert summary["total_files"] == 2
        assert summary["uploaded"] == 2
        assert summary["failed"] == 0
        assert mock_client.upload_file.call_count == 2


def test_r2_backup_database_dump(tmp_path):
    """Test backup_database_dump helper."""
    manager = R2BackupManager(
        bucket_name="test-bucket",
        endpoint_url="https://r2.test.com",
        access_key_id="key123",
        secret_access_key="sec123",
    )

    dump_file = tmp_path / "logistics_202301.sql.gz"
    dump_file.write_bytes(b"pg_dump binary data")

    mock_client = MagicMock()
    with patch.object(manager, "get_s3_client", return_value=mock_client):
        result = manager.backup_database_dump(dump_file, s3_prefix="backups/postgres")
        assert result is True
        mock_client.upload_file.assert_called_once_with(
            str(dump_file), "test-bucket", "backups/postgres/logistics_202301.sql.gz"
        )


def test_backup_artifacts_to_r2_task_unconfigured():
    """Test Prefect task skips gracefully when unconfigured."""
    with patch.object(R2BackupManager, "is_configured", return_value=False):
        res = backup_artifacts_to_r2_task.fn()
        assert res["status"] == "skipped"
        assert res["reason"] == "not_configured"


def test_r2_list_objects_success():
    """Test list_objects method with mocked boto3 client."""
    from datetime import datetime, timezone

    manager = R2BackupManager(
        bucket_name="test-bucket",
        endpoint_url="https://r2.test.com",
        access_key_id="key123",
        secret_access_key="sec123",
    )

    mock_client = MagicMock()
    mock_client.list_objects_v2.return_value = {
        "Contents": [
            {
                "Key": "mlruns-backup/model.pkl",
                "Size": 1024,
                "LastModified": datetime(2026, 8, 31, 12, 0, 0, tzinfo=timezone.utc),
            }
        ]
    }

    with patch.object(manager, "get_s3_client", return_value=mock_client):
        objects = manager.list_objects("mlruns-backup")
        assert len(objects) == 1
        assert objects[0]["key"] == "mlruns-backup/model.pkl"
        assert objects[0]["size"] == 1024


def test_r2_list_objects_unconfigured():
    """Test list_objects method returns empty list when unconfigured."""
    with patch.object(R2BackupManager, "is_configured", return_value=False):
        manager = R2BackupManager()
        objects = manager.list_objects("mlruns-backup")
        assert objects == []
