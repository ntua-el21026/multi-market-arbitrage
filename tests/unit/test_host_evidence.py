from __future__ import annotations

import importlib
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from multi_market_arbitrage.observability import capture_host_runtime_identity
from multi_market_arbitrage.pipeline.health import (
    COMPOSITE_PEAK_RSS_SOURCE,
    LINUX_CURRENT_RSS_SOURCE,
    LINUX_PYTHON_RU_MAXRSS_SOURCE,
    capture_process_resources,
)


def _git(repository: Path, *arguments: str) -> str:
    executable = shutil.which("git")
    if executable is None:
        raise RuntimeError("test requires Git")
    completed = subprocess.run(  # noqa: S603 - test-owned repository and fixed executable
        [executable, *arguments],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return completed.stdout.strip()


def _initialize_repository(repository: Path) -> tuple[str, Path, Path]:
    _git(repository, "init", "--quiet")
    package_root = repository / "src" / "multi_market_arbitrage"
    package_root.mkdir(parents=True)
    (package_root / "__init__.py").write_text("# fixture\n", encoding="utf-8")
    tracked = repository / "tracked.txt"
    tracked.write_text("clean\n", encoding="utf-8")
    (repository / ".gitignore").write_text(".venv/\n", encoding="utf-8")
    (repository / "uv.lock").write_bytes(b"exact-lock")
    _git(repository, "add", ".")
    _git(
        repository,
        "-c",
        "user.name=Host Evidence Test",
        "-c",
        "user.email=host-evidence@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "fixture",
    )
    return _git(repository, "rev-parse", "HEAD"), package_root, tracked


def test_host_runtime_identity_binds_code_config_lock_and_runtime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    commit, package_root, _tracked = _initialize_repository(tmp_path)
    host_module: Any = importlib.import_module("multi_market_arbitrage.observability.host")
    monkeypatch.setattr(host_module, "_imported_package_root", lambda: str(package_root))

    identity = capture_host_runtime_identity(
        workspace_root=tmp_path,
        storage_path=tmp_path / "future-output" / "run",
        code_commit=commit,
        config_sha256="b" * 64,
    )

    record = identity.to_record()
    assert record["code_commit"] == commit
    assert record["config_sha256"] == "b" * 64
    assert isinstance(record["lock_sha256"], str)
    assert "executable=" in identity.python
    assert identity.kernel
    assert identity.logical_cpu_count is None or identity.logical_cpu_count > 0
    assert identity.total_memory_bytes is None or identity.total_memory_bytes > 0


@pytest.mark.parametrize("code_commit", ["unavailable", "a" * 39, "g" * 40])
def test_inexact_code_identity_is_explicit_unknown(
    code_commit: str,
    tmp_path: Path,
) -> None:
    identity = capture_host_runtime_identity(
        workspace_root=tmp_path,
        storage_path=tmp_path,
        code_commit=code_commit,
        config_sha256="b" * 64,
    )

    assert identity.code_commit is None
    assert identity.to_record()["code_commit"] is None
    assert "code_commit" in identity.unknown_fields


def test_source_identity_distinguishes_clean_tracked_and_untracked_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    commit, package_root, tracked = _initialize_repository(repository)
    host_module: Any = importlib.import_module("multi_market_arbitrage.observability.host")
    monkeypatch.setattr(host_module, "_imported_package_root", lambda: str(package_root))

    clean = capture_host_runtime_identity(
        workspace_root=repository,
        storage_path=repository,
        code_commit=commit,
        config_sha256="b" * 64,
    )
    assert clean.source_root == str(repository.resolve())
    assert clean.repository_root == str(repository.resolve())
    assert clean.repository_head_commit == commit
    assert clean.imported_package_root == str(package_root)
    assert clean.code_commit_matches_repository is True
    assert clean.source_binding_valid is True
    assert clean.tracked_source_clean is True
    assert clean.untracked_source_clean is True
    assert clean.source_identity_known is True
    assert clean.source_tree_clean is True

    escaped_package_root = tmp_path / "outside-imported-package"
    escaped_package_root.mkdir()
    monkeypatch.setattr(
        host_module,
        "_imported_package_root",
        lambda: str(escaped_package_root),
    )
    escaped_package = capture_host_runtime_identity(
        workspace_root=repository,
        storage_path=repository,
        code_commit=commit,
        config_sha256="b" * 64,
    )
    assert escaped_package.imported_package_root == str(escaped_package_root)
    assert escaped_package.source_binding_valid is False
    assert escaped_package.source_identity_mismatch is True

    ignored_package_root = (
        repository / ".venv" / "lib" / "python" / "site-packages" / "multi_market_arbitrage"
    )
    ignored_package_root.mkdir(parents=True)
    monkeypatch.setattr(
        host_module,
        "_imported_package_root",
        lambda: str(ignored_package_root),
    )
    ignored_package = capture_host_runtime_identity(
        workspace_root=repository,
        storage_path=repository,
        code_commit=commit,
        config_sha256="b" * 64,
    )
    assert ignored_package.imported_package_root == str(ignored_package_root)
    assert ignored_package.source_binding_valid is False
    assert ignored_package.untracked_source_clean is True
    assert ignored_package.source_identity_mismatch is True
    monkeypatch.setattr(host_module, "_imported_package_root", lambda: str(package_root))

    tracked.write_text("dirty\n", encoding="utf-8")
    tracked_dirty = capture_host_runtime_identity(
        workspace_root=repository,
        storage_path=repository,
        code_commit=commit,
        config_sha256="b" * 64,
    )
    assert tracked_dirty.tracked_source_clean is False
    assert tracked_dirty.untracked_source_clean is True
    assert tracked_dirty.source_tree_clean is False

    tracked.write_text("clean\n", encoding="utf-8")
    (repository / "untracked.txt").write_text("untracked\n", encoding="utf-8")
    untracked_dirty = capture_host_runtime_identity(
        workspace_root=repository,
        storage_path=repository,
        code_commit=commit,
        config_sha256="b" * 64,
    )
    assert untracked_dirty.tracked_source_clean is True
    assert untracked_dirty.untracked_source_clean is False
    assert untracked_dirty.source_tree_clean is False

    mismatched = capture_host_runtime_identity(
        workspace_root=repository,
        storage_path=repository,
        code_commit="f" * 40,
        config_sha256="b" * 64,
    )
    assert mismatched.code_commit is None
    assert mismatched.repository_head_commit == commit
    assert mismatched.code_commit_matches_repository is False
    assert mismatched.source_identity_mismatch is True


def test_missing_host_fields_are_explicit_unknown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    host_module: Any = importlib.import_module("multi_market_arbitrage.observability.host")
    monkeypatch.setattr(host_module, "_uv_identity", lambda: None)
    monkeypatch.setattr(host_module, "_cpu_model", lambda: None)
    monkeypatch.setattr(host_module, "_total_memory_bytes", lambda: None)
    monkeypatch.setattr(host_module, "_gil_enabled", lambda: None)

    def missing_filesystem(_: Path) -> None:
        return None

    monkeypatch.setattr(host_module, "filesystem_identity", missing_filesystem)
    monkeypatch.setattr(host_module.platform, "node", lambda: "")
    monkeypatch.setattr(host_module.os, "cpu_count", lambda: None)

    identity = host_module.capture_host_runtime_identity(
        workspace_root=tmp_path,
        storage_path=tmp_path,
        code_commit="a" * 40,
        config_sha256="b" * 64,
    )

    assert identity.unknown_fields == (
        "code_commit",
        "code_commit_matches_repository",
        "cpu_model",
        "filesystem",
        "hostname",
        "lock_sha256",
        "logical_cpu_count",
        "python_gil_enabled",
        "repository_head_commit",
        "repository_root",
        "source_binding_valid",
        "total_memory_bytes",
        "tracked_source_clean",
        "untracked_source_clean",
        "uv",
    )


def test_process_resources_include_current_and_peak_rss(tmp_path: Path) -> None:
    resources = capture_process_resources(tmp_path)

    assert resources.sampled_monotonic_ns > 0
    assert resources.free_disk_bytes > 0
    assert resources.peak_rss_bytes > 0
    assert resources.peak_rss_source == COMPOSITE_PEAK_RSS_SOURCE
    assert resources.python_ru_maxrss_bytes > 0
    assert resources.python_ru_maxrss_source == LINUX_PYTHON_RU_MAXRSS_SOURCE
    assert resources.current_rss_source == LINUX_CURRENT_RSS_SOURCE
    if resources.current_rss_bytes is not None:
        assert resources.current_rss_bytes > 0
    assert resources.peak_rss_bytes == max(
        resources.current_rss_bytes or 0,
        resources.python_ru_maxrss_bytes,
    )
