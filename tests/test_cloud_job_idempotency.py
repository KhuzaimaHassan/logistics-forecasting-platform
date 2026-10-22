"""Unchanged monitoring must avoid historical queries; changed inputs must run."""

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.orchestration import free_tier_jobs as jobs


def test_job_fingerprint_tracks_source_checkpoint_model_and_runtime(monkeypatch):
    engine = MagicMock()
    result = engine.connect.return_value.__enter__.return_value.execute.return_value
    metadata = {"source_sha256": "source", "next_index": 9232}
    result.scalar_one.side_effect = lambda: json.dumps(metadata)
    monkeypatch.setattr(jobs, "runtime_code_digest", lambda: "code-v1")
    before, _ = jobs.job_fingerprint(engine, "1")
    monkeypatch.setenv("GITHUB_SHA", "documentation-only-commit")
    assert jobs.job_fingerprint(engine, "1")[0] == before
    assert jobs.job_fingerprint(engine, "2")[0] != before
    metadata["next_index"] = 9233
    assert jobs.job_fingerprint(engine, "1")[0] != before
    metadata["next_index"] = 9232
    metadata["source_sha256"] = "new-source"
    assert jobs.job_fingerprint(engine, "1")[0] != before
    metadata["source_sha256"] = "source"
    monkeypatch.setattr(jobs, "runtime_code_digest", lambda: "code-v2")
    assert jobs.job_fingerprint(engine, "1")[0] != before


def test_runtime_digest_ignores_docs_and_git_but_tracks_runtime_and_lock(tmp_path):
    (tmp_path / "src/features").mkdir(parents=True)
    for name in ["pyproject.toml", "uv.lock", "src/a.py", "src/features/store.yaml"]:
        (tmp_path / name).write_bytes(b"original\n")
    before = jobs.runtime_code_digest(tmp_path)
    (tmp_path / "README.md").write_text("new documentation")
    assert jobs.runtime_code_digest(tmp_path) == before
    (tmp_path / "src/a.py").write_bytes(b"original\r\n")
    assert jobs.runtime_code_digest(tmp_path) == before
    for name in ["uv.lock", "src/a.py", "src/features/store.yaml"]:
        path = tmp_path / name
        old = path.read_bytes()
        path.write_bytes(b"changed\n")
        assert jobs.runtime_code_digest(tmp_path) != before
        path.write_bytes(old)
    (tmp_path / "uv.lock").unlink()
    with pytest.raises(FileNotFoundError):
        jobs.runtime_code_digest(tmp_path)


@pytest.mark.parametrize("already_monitored", [True, False])
def test_monitor_checks_idempotency_before_any_feast_query(
    monkeypatch, already_monitored
):
    from src.features import config
    from src.serving import model_loader
    from src.training import dataset

    engine = object()
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(jobs, "get_engine", lambda: engine)
    monkeypatch.setattr(jobs, "require_capacity", lambda engine: 100)
    monkeypatch.setattr(
        jobs, "observation_window", lambda engine: (now - timedelta(days=20), now, now)
    )
    monkeypatch.setattr(
        model_loader,
        "ModelLoaderService",
        lambda: SimpleNamespace(load_demand_model=lambda: SimpleNamespace(version="1")),
    )
    fingerprint = "same" if already_monitored else "changed"
    monkeypatch.setattr(
        jobs,
        "job_fingerprint",
        lambda engine, version: (fingerprint, {"monitoring_fingerprint": "same"}),
    )
    store = MagicMock()
    retrieve = MagicMock(return_value=pd.DataFrame())
    monkeypatch.setattr(config, "get_feature_store", store)
    monkeypatch.setattr(dataset, "generate_demand_training_dataset", retrieve)
    if already_monitored:
        assert jobs.monitor.fn()["status"] == "skipped"
        store.assert_not_called()
        retrieve.assert_not_called()
    else:
        with pytest.raises(RuntimeError, match="No observed historical"):
            jobs.monitor.fn()
        retrieve.assert_called_once()
