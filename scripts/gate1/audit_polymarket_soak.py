#!/usr/bin/env python3
"""Fail-closed, read-only post-run audit for one exact Gate-1 collector run."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shlex
import shutil
import stat
import subprocess
import sys
import time
import tomllib
from dataclasses import dataclass
from datetime import UTC, date, datetime
from datetime import time as datetime_time
from pathlib import Path
from typing import Any, cast

DESIGN_FREEZE_SHA = "e7d69aedc328ceda220120377307d5ebcc1a80d9"
REQUIRED_UV_VERSION = "0.12.5"
MINIMUM_FREE_BYTES = 10 * 1024 * 1024 * 1024
MAXIMUM_INVENTORY_PATHS = 65_536
MAXIMUM_JSON_RESULT_BYTES = 256 * 1024 * 1024
SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
TARGET_PATTERN = re.compile(r"[1-9][0-9]*")
RUN_SEAL_SCHEMA = "mma-gate1-run-completion-seal-v1"
RUN_COMMAND_SCHEMA = "mma-gate1-observer-command-v1"
HOST_RUNTIME_IDENTITY_SCHEMA = "gate1-host-runtime-identity-v1"
RAW_SOURCE_IDENTITY_SCHEMA = "mma-gate1-raw-source-identity-v1"
SOURCE_IDENTITY_FIELDS = frozenset(
    {
        "code_commit",
        "code_commit_matches_repository",
        "imported_package_root",
        "repository_head_commit",
        "repository_root",
        "source_binding_valid",
        "source_root",
        "tracked_source_clean",
        "untracked_source_clean",
    }
)
NON_NATIVE_FILESYSTEMS = frozenset(
    {
        "9p",
        "cifs",
        "drvfs",
        "exfat",
        "fuse.sshfs",
        "fuseblk",
        "msdos",
        "ntfs",
        "ntfs3",
        "smb2",
        "vfat",
    }
)


class AuditFailure(RuntimeError):
    """The audit could not establish one required identity or result."""


class CommandFailure(AuditFailure):
    """One captured offline command exited nonzero."""


@dataclass(frozen=True, slots=True)
class Inventory:
    root: Path
    records: tuple[dict[str, object], ...]
    sha256: str
    total_bytes: int

    def to_record(self) -> dict[str, object]:
        return {
            "path_count": len(self.records),
            "records": self.records,
            "root": str(self.root),
            "schema_version": "mma-gate1-immutable-inventory-v1",
            "sha256": self.sha256,
            "total_bytes": self.total_bytes,
        }


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _canonical_config_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise AuditFailure("configuration contains a non-finite float")
        return value
    if isinstance(value, (date, datetime, datetime_time)):
        return value.isoformat()
    if isinstance(value, list):
        return [_canonical_config_value(item) for item in cast(list[object], value)]
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)
        if not all(isinstance(key, str) for key in mapping):
            raise AuditFailure("configuration contains a non-text key")
        return {cast(str, key): _canonical_config_value(item) for key, item in mapping.items()}
    raise AuditFailure(f"configuration contains unsupported value type: {type(value).__name__}")


def _runtime_config_sha256(path: Path) -> str:
    try:
        with path.open("rb") as handle:
            document: object = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise AuditFailure("cannot load the strict Phase-1 configuration") from error
    return hashlib.sha256(_canonical_bytes(_canonical_config_value(document))).hexdigest()


def _atomic_write(path: Path, payload: bytes) -> None:
    partial = path.with_name(f".{path.name}.partial")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(partial, flags, 0o600)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("artifact write made no progress")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(partial, path)
    directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _write_json(path: Path, value: object) -> None:
    _atomic_write(path, _canonical_bytes(value) + b"\n")


def _sha256_file(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise AuditFailure(f"artifact is not a confined regular file: {path}")
    before = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise AuditFailure(f"artifact identity is unsafe: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
        after = os.fstat(handle.fileno())
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise AuditFailure(f"artifact changed while hashing: {path}")
    return digest.hexdigest()


def _inventory(root: Path) -> Inventory:
    if root.is_symlink() or not root.is_dir():
        raise AuditFailure(f"inventory root is absent or symbolic: {root}")
    resolved_root = root.resolve(strict=True)
    records: list[dict[str, object]] = []
    total_bytes = 0
    for path in sorted(resolved_root.rglob("*")):
        if path.is_symlink():
            raise AuditFailure(f"inventory contains a symbolic link: {path}")
        status = path.stat(follow_symlinks=False)
        if stat.S_ISDIR(status.st_mode):
            continue
        if not stat.S_ISREG(status.st_mode) or status.st_nlink != 1:
            raise AuditFailure(f"inventory contains an unsafe non-regular path: {path}")
        if len(records) >= MAXIMUM_INVENTORY_PATHS:
            raise AuditFailure("inventory path count exceeds the supported bound")
        relative = path.relative_to(resolved_root).as_posix()
        records.append(
            {
                "bytes": status.st_size,
                "path": relative,
                "sha256": _sha256_file(path),
            }
        )
        total_bytes += status.st_size
    digest = hashlib.sha256(_canonical_bytes(records)).hexdigest()
    return Inventory(
        root=resolved_root,
        records=tuple(records),
        sha256=digest,
        total_bytes=total_bytes,
    )


def _read_json(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise AuditFailure(f"JSON result is absent or symbolic: {path}")
    if path.stat().st_size > MAXIMUM_JSON_RESULT_BYTES:
        raise AuditFailure(f"JSON result exceeds the supported byte bound: {path}")
    try:
        value: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise AuditFailure(f"cannot read canonical JSON result: {path}") from error
    if not isinstance(value, dict):
        raise AuditFailure(f"JSON result must be an object: {path}")
    return cast(dict[str, Any], value)


def _git(repository: Path, *arguments: str) -> str:
    git = shutil.which("git")
    if git is None:
        raise AuditFailure("git is unavailable")
    completed = subprocess.run(  # noqa: S603 - absolute executable and fixed argument boundary
        (git, "-C", str(repository), *arguments),
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        raise AuditFailure(
            f"git {' '.join(arguments)} failed: {completed.stderr.strip() or 'unknown error'}"
        )
    return completed.stdout.strip()


def _git_blob_sha256(repository: Path, commit: str, relative: str) -> str:
    git = shutil.which("git")
    if git is None:
        raise AuditFailure("git is unavailable")
    completed = subprocess.run(  # noqa: S603 - absolute executable and fixed argument boundary
        (git, "-C", str(repository), "cat-file", "blob", f"{commit}:{relative}"),
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise AuditFailure(f"candidate does not contain required path: {relative}")
    return hashlib.sha256(completed.stdout).hexdigest()


def _require_directory(path: Path, *, label: str) -> Path:
    if path.is_symlink() or not path.is_dir():
        raise AuditFailure(f"{label} is absent or symbolic: {path}")
    return path.resolve(strict=True)


def _filesystem_type(path: Path) -> str:
    stat_command = shutil.which("stat")
    if stat_command is None:
        raise AuditFailure("stat is unavailable")
    completed = subprocess.run(  # noqa: S603 - absolute executable and fixed arguments
        (stat_command, "-f", "-c", "%T", "--", str(path)),
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        raise AuditFailure(f"cannot determine filesystem type for {path}")
    return completed.stdout.strip()


def _require_native_and_sized(path: Path, *, label: str) -> dict[str, object]:
    filesystem = _filesystem_type(path)
    if filesystem.casefold() in NON_NATIVE_FILESYSTEMS:
        raise AuditFailure(f"{label} must use a native Linux filesystem, observed {filesystem}")
    disk = shutil.disk_usage(path)
    if disk.free < MINIMUM_FREE_BYTES:
        raise AuditFailure(f"{label} has less than 10 GiB free")
    return {
        "filesystem_type": filesystem,
        "free_bytes": disk.free,
        "path": str(path),
    }


def _parse_input_hashes(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split(maxsplit=1)
        if len(parts) != 2 or SHA256_PATTERN.fullmatch(parts[0]) is None:
            raise AuditFailure("input-files.sha256 is malformed")
        relative = parts[1].lstrip(" *")
        if relative in values:
            raise AuditFailure("input-files.sha256 repeats one path")
        values[relative] = parts[0]
    return values


def _paths_overlap(first: Path, second: Path) -> bool:
    return first == second or first.is_relative_to(second) or second.is_relative_to(first)


def _fingerprint_matches(
    record: object,
    expected_path: Path,
    *,
    label: str,
) -> None:
    if not isinstance(record, dict):
        raise AuditFailure(f"run seal {label} fingerprint is absent")
    fingerprint = cast(dict[str, object], record)
    path_value = fingerprint.get("path")
    byte_value = fingerprint.get("bytes")
    sha_value = fingerprint.get("sha256")
    if not isinstance(path_value, str):
        raise AuditFailure(f"run seal {label} path is malformed")
    try:
        recorded_path = Path(path_value).resolve(strict=True)
    except OSError as error:
        raise AuditFailure(f"run seal {label} path is unavailable") from error
    if recorded_path != expected_path:
        raise AuditFailure(f"run seal {label} path differs")
    if (
        isinstance(byte_value, bool)
        or not isinstance(byte_value, int)
        or byte_value != expected_path.stat().st_size
    ):
        raise AuditFailure(f"run seal {label} byte count differs")
    if not isinstance(sha_value, str) or SHA256_PATTERN.fullmatch(sha_value) is None:
        raise AuditFailure(f"run seal {label} hash is malformed")
    if sha_value != _sha256_file(expected_path):
        raise AuditFailure(f"run seal {label} hash differs")


def _numeric_duration(value: object, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AuditFailure(f"{label} must be numeric")
    duration = float(value)
    if not math.isfinite(duration) or duration <= 0:
        raise AuditFailure(f"{label} must be finite and positive")
    return duration


def _verify_host_runtime_identity(
    summary: dict[str, Any],
    *,
    source: Path,
    expected_sha: str,
    runtime_config_sha256: str,
    lock_sha256: str,
) -> dict[str, object]:
    value = summary.get("host_runtime_identity")
    if not isinstance(value, dict):
        raise AuditFailure("sealed summary omits host_runtime_identity")
    identity = cast(dict[str, object], value)
    expected_package_root = (source / "src" / "multi_market_arbitrage").resolve(strict=True)
    exact_values: dict[str, object] = {
        "code_commit": expected_sha,
        "config_sha256": runtime_config_sha256,
        "imported_package_root": str(expected_package_root),
        "lock_sha256": lock_sha256,
        "repository_head_commit": expected_sha,
        "repository_root": str(source),
        "schema_version": HOST_RUNTIME_IDENTITY_SCHEMA,
        "source_root": str(source),
    }
    for field, expected in exact_values.items():
        if identity.get(field) != expected:
            raise AuditFailure(f"host_runtime_identity {field} differs from exact source")
    for field in (
        "code_commit_matches_repository",
        "source_binding_valid",
        "tracked_source_clean",
        "untracked_source_clean",
    ):
        if identity.get(field) is not True:
            raise AuditFailure(f"host_runtime_identity {field} is not exactly true")
    unknown_value = identity.get("unknown_fields")
    if not isinstance(unknown_value, list):
        raise AuditFailure("host_runtime_identity unknown_fields is malformed")
    unknown_items = cast(list[object], unknown_value)
    if not all(isinstance(item, str) for item in unknown_items):
        raise AuditFailure("host_runtime_identity unknown_fields is malformed")
    unknown_source_fields = SOURCE_IDENTITY_FIELDS.intersection(cast(list[str], unknown_items))
    if unknown_source_fields:
        fields = ", ".join(sorted(unknown_source_fields))
        raise AuditFailure(f"host_runtime_identity has unknown source fields: {fields}")
    return identity


def _run_key(collector_run_id: str) -> str:
    return hashlib.sha256(collector_run_id.encode("utf-8")).hexdigest()[:16]


def _verify_raw_source_identity(
    raw_root: Path,
    *,
    collector_run_id: str,
    expected_sha: str,
) -> dict[str, object]:
    run_key = _run_key(collector_run_id)
    prefix = f"run-{run_key}."
    segment_paths = tuple(sorted(raw_root.glob(f"{prefix}*.jsonl.zst")))
    manifest_paths = tuple(sorted(raw_root.glob(f"{prefix}*.jsonl.zst.manifest.json")))
    expected_manifests = {path.with_name(f"{path.name}.manifest.json") for path in segment_paths}
    if set(manifest_paths) != expected_manifests:
        raise AuditFailure("selected run segment/manifest inventory differs")
    partial_paths = tuple(sorted(raw_root.glob(f"{prefix}*.jsonl.partial")))
    if partial_paths:
        raise AuditFailure("selected completed run retains a partial journal segment")
    completion_paths = tuple(sorted(raw_root.glob(f"{prefix}*.completed.json")))
    if not completion_paths:
        raise AuditFailure("selected run has no journal completion header")

    manifest_records: list[dict[str, object]] = []
    manifest_streams: set[str] = set()
    for path in manifest_paths:
        record = _read_json(path)
        stream = record.get("stream")
        first_sequence = record.get("first_ingest_seq")
        last_sequence = record.get("last_ingest_seq")
        if (
            not isinstance(stream, str)
            or not stream
            or isinstance(first_sequence, bool)
            or not isinstance(first_sequence, int)
            or isinstance(last_sequence, bool)
            or not isinstance(last_sequence, int)
            or first_sequence < 0
            or last_sequence < first_sequence
        ):
            raise AuditFailure("selected run segment manifest identity is malformed")
        expected_name = (
            f"{prefix}{stream}.{first_sequence:020d}-{last_sequence:020d}.jsonl.zst.manifest.json"
        )
        if path.name != expected_name or record.get("collector_run_id") != collector_run_id:
            raise AuditFailure("selected run segment manifest scope differs")
        if record.get("git_commit") != expected_sha:
            raise AuditFailure("selected run segment manifest git_commit differs")
        manifest_streams.add(stream)
        manifest_records.append(
            {
                "bytes": path.stat().st_size,
                "git_commit": expected_sha,
                "path": str(path.resolve(strict=True)),
                "sha256": _sha256_file(path),
                "stream": stream,
            }
        )

    completion_records: list[dict[str, object]] = []
    completion_streams: set[str] = set()
    for path in completion_paths:
        record = _read_json(path)
        stream = record.get("stream")
        if not isinstance(stream, str) or not stream:
            raise AuditFailure("selected run completion header stream is malformed")
        if path.name != f"{prefix}{stream}.completed.json":
            raise AuditFailure("selected run completion header filename differs")
        if record.get("collector_run_id") != collector_run_id:
            raise AuditFailure("selected run completion header scope differs")
        if record.get("git_commit") != expected_sha:
            raise AuditFailure("selected run completion header git_commit differs")
        if stream in completion_streams:
            raise AuditFailure("selected run repeats a completion stream")
        completion_streams.add(stream)
        completion_records.append(
            {
                "bytes": path.stat().st_size,
                "git_commit": expected_sha,
                "path": str(path.resolve(strict=True)),
                "sha256": _sha256_file(path),
                "stream": stream,
            }
        )
    if not manifest_streams.issubset(completion_streams):
        raise AuditFailure("selected run segment stream has no completion header")
    return {
        "collector_run_id": collector_run_id,
        "completion_headers": completion_records,
        "expected_git_commit": expected_sha,
        "run_key": run_key,
        "schema_version": RAW_SOURCE_IDENTITY_SCHEMA,
        "segment_manifests": manifest_records,
    }


def _expected_observer_command(
    source: Path,
    expected_sha: str,
    target_id: str,
    duration_seconds: int,
) -> dict[str, object]:
    return {
        "argv": [
            "uv",
            "run",
            "--offline",
            "--frozen",
            "--no-sync",
            "--all-groups",
            "mma",
            "observe",
            "polymarket",
            "--gamma-market-id",
            target_id,
            "--duration",
            str(duration_seconds),
            "--config",
            "config/phase1.toml",
        ],
        "config_relative_path": "config/phase1.toml",
        "cwd": str(source),
        "duration_seconds": duration_seconds,
        "environment": {"PYTHONHASHSEED": "0", "PYTHONUNBUFFERED": "1"},
        "expected_source_sha": expected_sha,
        "schema_version": RUN_COMMAND_SCHEMA,
        "target_id": target_id,
    }


def _verify_run_seal(
    *,
    source: Path,
    evidence: Path,
    expected_sha: str,
    collector_run_id: str,
    config: Path,
    uv_lock: Path,
    data_root: Path,
    raw_root: Path,
    analytical_root: Path,
    raw_before: Inventory,
) -> tuple[dict[str, Any], Path]:
    seal_path = evidence / "run-completion-seal.json"
    seal = _read_json(seal_path)
    if seal.get("schema_version") != RUN_SEAL_SCHEMA:
        raise AuditFailure("run completion seal schema differs")
    exact_values: dict[str, object] = {
        "analytical_root": str(analytical_root),
        "collector_run_id": collector_run_id,
        "data_root": str(data_root),
        "design_freeze_sha": DESIGN_FREEZE_SHA,
        "evidence_directory": str(evidence),
        "raw_root": str(raw_root),
        "source_sha": expected_sha,
        "source_worktree": str(source),
    }
    for field, expected in exact_values.items():
        if seal.get(field) != expected:
            raise AuditFailure(f"run completion seal {field} differs")

    target_id = seal.get("target_id")
    if not isinstance(target_id, str) or TARGET_PATTERN.fullmatch(target_id) is None:
        raise AuditFailure("run completion seal target identity is malformed")
    duration_value = seal.get("duration_seconds")
    if (
        isinstance(duration_value, bool)
        or not isinstance(duration_value, int)
        or duration_value <= 0
    ):
        raise AuditFailure("run completion seal duration is malformed")
    duration_seconds = duration_value
    if seal.get("exit_code") != 0:
        raise AuditFailure("run completion seal does not prove a zero observer exit")

    sealed_at = seal.get("sealed_at_utc")
    if not isinstance(sealed_at, str):
        raise AuditFailure("run completion seal omits its UTC completion time")
    try:
        parsed_sealed_at = datetime.fromisoformat(sealed_at)
    except ValueError as error:
        raise AuditFailure("run completion seal UTC completion time is malformed") from error
    if parsed_sealed_at.tzinfo is None:
        raise AuditFailure("run completion seal UTC completion time is naive")

    config_record = seal.get("config")
    lock_record = seal.get("lock")
    if not isinstance(config_record, dict):
        raise AuditFailure("run completion seal configuration identity differs")
    config_identity = cast(dict[str, object], config_record)
    if config_identity.get("relative_path") != (config.relative_to(source).as_posix()):
        raise AuditFailure("run completion seal configuration identity differs")
    runtime_config_sha = _runtime_config_sha256(config)
    if config_identity.get("runtime_sha256") != runtime_config_sha:
        raise AuditFailure("run completion seal runtime configuration identity differs")
    if not isinstance(lock_record, dict):
        raise AuditFailure("run completion seal lock identity differs")
    lock_identity = cast(dict[str, object], lock_record)
    if lock_identity.get("relative_path") != "uv.lock":
        raise AuditFailure("run completion seal lock identity differs")
    _fingerprint_matches(config_identity, config, label="configuration")
    _fingerprint_matches(lock_identity, uv_lock, label="lock")
    lock_sha = _sha256_file(uv_lock)

    binding_records = seal.get("evidence_bindings")
    if not isinstance(binding_records, dict):
        raise AuditFailure("run completion seal omits evidence bindings")
    bindings = cast(dict[str, object], binding_records)
    binding_names = (
        "design-freeze-commit.txt",
        "gamma-market-id.txt",
        "git-head.txt",
        "input-files.sha256",
        "soak-worktree.txt",
    )
    if set(bindings) != set(binding_names):
        raise AuditFailure("run completion seal evidence-binding set differs")
    for name in binding_names:
        _fingerprint_matches(bindings[name], evidence / name, label=f"binding {name}")

    summary_path = (evidence / "soak-summary.json").resolve(strict=True)
    command_path = (evidence / "soak-command.json").resolve(strict=True)
    exit_code_path = (evidence / "soak-exit-code.txt").resolve(strict=True)
    _fingerprint_matches(seal.get("summary"), summary_path, label="summary")
    _fingerprint_matches(seal.get("command"), command_path, label="command")
    _fingerprint_matches(seal.get("exit_code_artifact"), exit_code_path, label="exit-code artifact")
    try:
        exit_code = int(exit_code_path.read_text(encoding="utf-8").strip())
    except (OSError, UnicodeError, ValueError) as error:
        raise AuditFailure("sealed observer exit-code artifact is malformed") from error
    if exit_code != 0:
        raise AuditFailure("sealed observer exit-code artifact is nonzero")

    summary = _read_json(summary_path)
    if summary.get("collector_run_id") != collector_run_id:
        raise AuditFailure("sealed summary CollectorRunId differs")
    if _numeric_duration(
        summary.get("requested_duration_seconds"), label="sealed summary duration"
    ) != float(duration_seconds):
        raise AuditFailure("sealed summary requested duration differs")
    actual_duration = summary.get("actual_duration_nanoseconds")
    if (
        isinstance(actual_duration, bool)
        or not isinstance(actual_duration, int)
        or actual_duration < duration_seconds * 1_000_000_000
    ):
        raise AuditFailure("sealed summary does not prove the requested monotonic duration")
    target = summary.get("target")
    if not isinstance(target, dict):
        raise AuditFailure("sealed summary target identity differs")
    target_identity = cast(dict[str, object], target)
    if (
        target_identity.get("requested_gamma_market_id") != target_id
        or target_identity.get("resolved_gamma_market_id") != target_id
    ):
        raise AuditFailure("sealed summary target identity differs")
    host_runtime_identity = _verify_host_runtime_identity(
        summary,
        source=source,
        expected_sha=expected_sha,
        runtime_config_sha256=runtime_config_sha,
        lock_sha256=lock_sha,
    )
    if _canonical_bytes(seal.get("host_runtime_identity")) != _canonical_bytes(
        host_runtime_identity
    ):
        raise AuditFailure("run completion seal host-runtime identity differs")

    command = _read_json(command_path)
    if command != _expected_observer_command(source, expected_sha, target_id, duration_seconds):
        raise AuditFailure("sealed observer command differs from the exact command contract")

    sealed_raw = seal.get("raw_inventory")
    if _canonical_bytes(sealed_raw) != _canonical_bytes(raw_before.to_record()):
        raise AuditFailure("authoritative raw inventory differs from the runner completion seal")
    raw_source_identity = _verify_raw_source_identity(
        raw_root,
        collector_run_id=collector_run_id,
        expected_sha=expected_sha,
    )
    if _canonical_bytes(seal.get("raw_source_identity")) != _canonical_bytes(raw_source_identity):
        raise AuditFailure("run completion seal raw source identity differs")
    return seal, seal_path


def _resolve_storage_paths(source: Path, config: Path) -> tuple[Path, Path, Path]:
    try:
        with config.open("rb") as handle:
            document = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise AuditFailure("cannot load the strict Phase-1 configuration") from error
    storage_value = document.get("storage")
    if not isinstance(storage_value, dict):
        raise AuditFailure("configuration has no storage table")
    storage = cast(dict[str, object], storage_value)

    def resolve(field: str) -> Path:
        value = storage.get(field)
        if not isinstance(value, str) or not value:
            raise AuditFailure(f"storage.{field} must be a non-empty path")
        unresolved = source / value
        if unresolved.is_symlink():
            raise AuditFailure(f"storage.{field} escapes or is symbolic")
        candidate = unresolved.resolve(strict=True)
        if not candidate.is_relative_to(source):
            raise AuditFailure(f"storage.{field} escapes or is symbolic")
        return candidate

    return resolve("root"), resolve("journal_directory"), resolve("analytical_directory")


@dataclass(slots=True)
class CommandCapture:
    output_directory: Path
    source: Path
    environment: dict[str, str]
    records: list[dict[str, object]]

    def run(self, name: str, arguments: tuple[str, ...]) -> Path:
        stdout_path = self.output_directory / f"{name}.stdout.json"
        stderr_path = self.output_directory / f"{name}.stderr.log"
        metadata_path = self.output_directory / f"{name}.command.json"
        started_at = datetime.now(UTC).isoformat()
        started_ns = time.monotonic_ns()
        with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
            completed = subprocess.run(  # noqa: S603 - absolute uv executable and fixed args
                arguments,
                check=False,
                cwd=self.source,
                env=self.environment,
                stdout=stdout,
                stderr=stderr,
            )
        ended_ns = time.monotonic_ns()
        record: dict[str, object] = {
            "argv": arguments,
            "command": shlex.join(arguments),
            "cwd": str(self.source),
            "elapsed_nanoseconds": ended_ns - started_ns,
            "ended_at_utc": datetime.now(UTC).isoformat(),
            "exit_code": completed.returncode,
            "name": name,
            "schema_version": "mma-gate1-offline-command-capture-v1",
            "started_at_utc": started_at,
            "stderr": {
                "bytes": stderr_path.stat().st_size,
                "path": str(stderr_path),
                "sha256": _sha256_file(stderr_path),
            },
            "stdout": {
                "bytes": stdout_path.stat().st_size,
                "path": str(stdout_path),
                "sha256": _sha256_file(stdout_path),
            },
        }
        _write_json(metadata_path, record)
        self.records.append(record)
        if completed.returncode != 0:
            raise CommandFailure(f"offline command {name!r} exited {completed.returncode}")
        return stdout_path


def _semantic_scan_projection(report: dict[str, Any]) -> dict[str, object]:
    fields = (
        "candidates",
        "dispositions",
        "schema_version",
        "terminal_hash_count",
        "terminal_hash_order",
        "terminal_hash_samples",
        "terminal_hash_sequence_sha256",
        "terminal_hashes",
        "terminal_hashes_complete",
        "total_scans",
        "unique_lifecycles",
        "unique_opportunity_observations",
        "validated_opportunities",
    )
    missing = [field for field in fields if field not in report]
    if missing:
        raise AuditFailure(f"scan report omits semantic fields: {', '.join(missing)}")
    return {field: report[field] for field in fields}


def _artifact_inventory(analysis: Path) -> dict[str, object]:
    records: list[dict[str, object]] = []
    for path in sorted(analysis.rglob("*")):
        if path.is_symlink():
            raise AuditFailure(f"analysis output contains a symbolic link: {path}")
        if not path.is_file():
            continue
        if path.name in {"artifact-inventory.json", "audit-manifest.json"}:
            continue
        records.append(
            {
                "bytes": path.stat().st_size,
                "path": path.relative_to(analysis).as_posix(),
                "sha256": _sha256_file(path),
            }
        )
    return {
        "records": records,
        "schema_version": "mma-gate1-audit-artifact-inventory-v1",
        "sha256": hashlib.sha256(_canonical_bytes(records)).hexdigest(),
    }


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-worktree", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--collector-run-id", required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--analysis-dir", type=Path, required=True)
    parser.add_argument("--expected-sha", required=True)
    return parser


def _audit(arguments: argparse.Namespace) -> Path:
    expected_sha: str = arguments.expected_sha
    collector_run_id: str = arguments.collector_run_id
    if SHA_PATTERN.fullmatch(expected_sha) is None:
        raise AuditFailure("--expected-sha must be a full lowercase 40-character Git SHA")
    if RUN_ID_PATTERN.fullmatch(collector_run_id) is None:
        raise AuditFailure("--collector-run-id is not a bounded operational identifier")

    source = _require_directory(arguments.source_worktree, label="source worktree")
    evidence = _require_directory(arguments.evidence_dir, label="evidence directory")
    if _paths_overlap(source, evidence):
        raise AuditFailure("evidence directory must not overlap the source worktree")
    config_argument: Path = arguments.config
    if config_argument.is_symlink() or not config_argument.is_file():
        raise AuditFailure("--config must be a real file inside the source worktree")
    config = config_argument.resolve(strict=True)
    if not config.is_relative_to(source):
        raise AuditFailure("--config must be a real file inside the source worktree")
    analysis_argument: Path = arguments.analysis_dir
    if analysis_argument.is_symlink() or analysis_argument.parent.is_symlink():
        raise AuditFailure("--analysis-dir cannot be symbolic")
    analysis = analysis_argument.resolve(strict=False)
    if analysis.exists() or _paths_overlap(analysis, source) or _paths_overlap(analysis, evidence):
        raise AuditFailure("--analysis-dir must be fresh and separate from source and evidence")
    analysis.parent.mkdir(parents=True, exist_ok=True)
    if analysis.parent.is_symlink():
        raise AuditFailure("analysis parent cannot be symbolic")

    if _git(source, "rev-parse", "--show-toplevel") != str(source):
        raise AuditFailure("--source-worktree is not a Git worktree root")
    if _git(source, "status", "--porcelain=v1", "--untracked-files=all"):
        raise AuditFailure("source worktree is not clean")
    if _git(source, "rev-parse", "HEAD") != expected_sha:
        raise AuditFailure("source worktree HEAD differs from --expected-sha")
    if _git(source, "rev-parse", "design-freeze-v0.1^{commit}") != DESIGN_FREEZE_SHA:
        raise AuditFailure("Design Freeze tag identity differs")

    script = Path(__file__).resolve(strict=True)
    if not script.is_relative_to(source):
        raise AuditFailure("audit helper must be invoked from the exact source worktree")
    script_relative = script.relative_to(source).as_posix()
    if _sha256_file(script) != _git_blob_sha256(source, expected_sha, script_relative):
        raise AuditFailure("executed audit-helper bytes differ from the exact candidate")
    config_relative = config.relative_to(source).as_posix()
    if _sha256_file(config) != _git_blob_sha256(source, expected_sha, config_relative):
        raise AuditFailure("configuration differs from the exact candidate")
    uv_lock = source / "uv.lock"
    if _sha256_file(uv_lock) != _git_blob_sha256(source, expected_sha, "uv.lock"):
        raise AuditFailure("uv.lock differs from the exact candidate")

    required_bindings = {
        "design-freeze-commit.txt": DESIGN_FREEZE_SHA,
        "git-head.txt": expected_sha,
    }
    for name, expected in required_bindings.items():
        binding = evidence / name
        if binding.is_symlink() or not binding.is_file():
            raise AuditFailure(f"run evidence omits required identity binding: {name}")
        if binding.read_text(encoding="utf-8").strip() != expected:
            raise AuditFailure(f"run evidence identity differs: {name}")
    input_hash_path = evidence / "input-files.sha256"
    if input_hash_path.is_symlink() or not input_hash_path.is_file():
        raise AuditFailure("run evidence omits input-files.sha256")
    input_hashes = _parse_input_hashes(input_hash_path)
    if set(input_hashes) != {"uv.lock", config_relative}:
        raise AuditFailure("run evidence input-hash path set differs")
    for relative, path in (("uv.lock", uv_lock), (config_relative, config)):
        if input_hashes.get(relative) != _sha256_file(path):
            raise AuditFailure(f"run evidence input hash differs: {relative}")
    recorded_worktree = evidence / "soak-worktree.txt"
    if recorded_worktree.is_symlink() or not recorded_worktree.is_file():
        raise AuditFailure("run evidence omits soak-worktree.txt")
    if recorded_worktree.read_text(encoding="utf-8").strip() != str(source):
        raise AuditFailure("run evidence worktree binding differs")

    data_root, raw_root, analytical_root = _resolve_storage_paths(source, config)
    if _paths_overlap(analysis, data_root):
        raise AuditFailure("--analysis-dir cannot overlap authoritative data")
    if _paths_overlap(evidence, data_root):
        raise AuditFailure("evidence directory cannot overlap authoritative data")
    path_evidence = {
        "analysis": _require_native_and_sized(analysis.parent, label="analysis filesystem"),
        "data": _require_native_and_sized(data_root, label="authoritative data filesystem"),
        "evidence": _require_native_and_sized(evidence, label="evidence filesystem"),
        "source": _require_native_and_sized(source, label="source filesystem"),
    }
    raw_before = _inventory(raw_root)
    run_seal, run_seal_path = _verify_run_seal(
        source=source,
        evidence=evidence,
        expected_sha=expected_sha,
        collector_run_id=collector_run_id,
        config=config,
        uv_lock=uv_lock,
        data_root=data_root,
        raw_root=raw_root,
        analytical_root=analytical_root,
        raw_before=raw_before,
    )
    evidence_inventory = _inventory(evidence)

    analysis.mkdir(mode=0o700)
    temporary = analysis / "tmp"
    commands = analysis / "commands"
    temporary.mkdir(mode=0o700)
    commands.mkdir(mode=0o700)
    _write_json(analysis / "evidence-input-inventory.json", evidence_inventory.to_record())
    _write_json(analysis / "raw-inventory.before.json", raw_before.to_record())
    _write_json(
        analysis / "identity.json",
        {
            "collector_run_id": collector_run_id,
            "config": str(config),
            "config_sha256": _sha256_file(config),
            "design_freeze_sha": DESIGN_FREEZE_SHA,
            "evidence_directory": str(evidence),
            "expected_sha": expected_sha,
            "paths": path_evidence,
            "run_completion_seal": str(run_seal_path),
            "run_completion_seal_sha256": _sha256_file(run_seal_path),
            "run_duration_seconds": run_seal["duration_seconds"],
            "run_target_id": run_seal["target_id"],
            "schema_version": "mma-gate1-post-run-audit-identity-v1",
            "source_worktree": str(source),
            "uv_lock_sha256": _sha256_file(uv_lock),
        },
    )

    uv = shutil.which("uv")
    if uv is None:
        raise AuditFailure("uv is unavailable")
    version = subprocess.run(  # noqa: S603 - absolute executable and fixed argument
        (uv, "--version"),
        capture_output=True,
        check=False,
        text=True,
    )
    version_tokens = version.stdout.strip().split()
    if (
        version.returncode != 0
        or len(version_tokens) < 2
        or version_tokens[0] != "uv"
        or version_tokens[1] != REQUIRED_UV_VERSION
    ):
        raise AuditFailure(f"uv {REQUIRED_UV_VERSION} is required")
    environment = dict(os.environ)
    environment.update(
        {
            "PYTHONHASHSEED": "0",
            "PYTHONUNBUFFERED": "1",
            "TMPDIR": str(temporary),
        }
    )
    capture = CommandCapture(commands, source, environment, [])
    prefix = (
        uv,
        "run",
        "--offline",
        "--frozen",
        "--no-sync",
        "--all-groups",
        "mma",
    )
    manifest_path = analysis / "audit-manifest.json"
    failure: BaseException | None = None
    status = "FAILED"
    terminal_hash: str | None = None
    raw_post: Inventory | None = None
    raw_immutable = False
    raw_matches_completion_seal = False
    evidence_post: Inventory | None = None
    evidence_immutable = False
    try:
        capture.run("01-validate-original", (*prefix, "validate-data", str(data_root)))
        replay_one_path = capture.run(
            "02-replay-one",
            (
                *prefix,
                "replay",
                str(raw_root),
                "--run-id",
                collector_run_id,
                "--config",
                str(config),
            ),
        )
        replay_one = _read_json(replay_one_path)
        if replay_one.get("collector_run_id") != collector_run_id:
            raise AuditFailure("replay one CollectorRunId differs from the sealed run")
        processor_value = replay_one.get("processor")
        processor = (
            cast(dict[str, Any], processor_value) if isinstance(processor_value, dict) else {}
        )
        terminal_hash_value = processor.get("terminal_hash")
        if (
            not isinstance(terminal_hash_value, str)
            or SHA256_PATTERN.fullmatch(terminal_hash_value) is None
        ):
            raise AuditFailure("replay one omitted a canonical processor terminal hash")
        terminal_hash = terminal_hash_value
        replay_two_path = capture.run(
            "03-replay-two",
            (
                *prefix,
                "replay",
                str(raw_root),
                "--run-id",
                collector_run_id,
                "--expect-terminal-hash",
                terminal_hash,
                "--config",
                str(config),
            ),
        )
        replay_identical = replay_one_path.read_bytes() == replay_two_path.read_bytes()
        _write_json(
            analysis / "replay-comparison.json",
            {
                "byte_identical": replay_identical,
                "replay_one_sha256": _sha256_file(replay_one_path),
                "replay_two_sha256": _sha256_file(replay_two_path),
                "schema_version": "mma-gate1-replay-comparison-v1",
                "terminal_hash": terminal_hash,
            },
        )
        if not replay_identical:
            raise AuditFailure("independent replay outputs are not byte-identical")

        rebuilt = analysis / "rebuilt-analytical"
        rebuild_result_path = capture.run(
            "04-rebuild-analytical",
            (
                *prefix,
                "rebuild-analytical",
                str(raw_root),
                "--output",
                str(rebuilt),
                "--run-id",
                collector_run_id,
                "--expect-terminal-hash",
                terminal_hash,
                "--config",
                str(config),
            ),
        )
        rebuild_result = _read_json(rebuild_result_path)
        replay_value = rebuild_result.get("replay")
        replay = cast(dict[str, Any], replay_value) if isinstance(replay_value, dict) else {}
        rebuild_processor_value = replay.get("processor")
        rebuild_processor = (
            cast(dict[str, Any], rebuild_processor_value)
            if isinstance(rebuild_processor_value, dict)
            else {}
        )
        rebuild_hash = rebuild_processor.get("terminal_hash")
        if rebuild_hash != terminal_hash:
            raise AuditFailure("analytical rebuild terminal identity differs from replay one")
        capture.run("05-validate-rebuild", (*prefix, "validate-data", str(rebuilt)))
        original_scan_path = capture.run(
            "06-scan-original", (*prefix, "scan-report", str(analytical_root))
        )
        rebuilt_scan_path = capture.run("07-scan-rebuilt", (*prefix, "scan-report", str(rebuilt)))
        original_projection = _semantic_scan_projection(_read_json(original_scan_path))
        rebuilt_projection = _semantic_scan_projection(_read_json(rebuilt_scan_path))
        semantics_equal = original_projection == rebuilt_projection
        _write_json(
            analysis / "scan-semantic-comparison.json",
            {
                "original": original_projection,
                "rebuilt": rebuilt_projection,
                "schema_version": "mma-gate1-scan-semantic-comparison-v1",
                "semantics_equal": semantics_equal,
            },
        )
        if not semantics_equal:
            raise AuditFailure("original and rebuilt scan reports differ semantically")
        status = "PASSED"
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        failure = error
    finally:
        try:
            raw_post = _inventory(raw_root)
            _write_json(analysis / "raw-inventory.after.json", raw_post.to_record())
            raw_matches_completion_seal = _canonical_bytes(
                raw_post.to_record()
            ) == _canonical_bytes(run_seal["raw_inventory"])
            raw_immutable = (
                raw_post.sha256 == raw_before.sha256
                and raw_post.records == raw_before.records
                and raw_matches_completion_seal
            )
            _write_json(
                analysis / "raw-immutability.json",
                {
                    "after_sha256": raw_post.sha256,
                    "before_sha256": raw_before.sha256,
                    "raw_immutable": raw_immutable,
                    "raw_matches_completion_seal": raw_matches_completion_seal,
                    "sealed_sha256": cast(dict[str, Any], run_seal["raw_inventory"])["sha256"],
                    "schema_version": "mma-gate1-raw-immutability-v1",
                },
            )
            if not raw_immutable and failure is None:
                failure = AuditFailure("authoritative raw inventory changed during audit")
                status = "FAILED"
        except BaseException as inventory_error:
            if isinstance(inventory_error, (KeyboardInterrupt, SystemExit)):
                raise
            if failure is None:
                failure = inventory_error
            status = "FAILED"

        try:
            evidence_post = _inventory(evidence)
            evidence_immutable = (
                evidence_post.sha256 == evidence_inventory.sha256
                and evidence_post.records == evidence_inventory.records
            )
            _write_json(
                analysis / "evidence-immutability.json",
                {
                    "after_sha256": evidence_post.sha256,
                    "before_sha256": evidence_inventory.sha256,
                    "evidence_immutable": evidence_immutable,
                    "schema_version": "mma-gate1-evidence-immutability-v1",
                },
            )
            if not evidence_immutable and failure is None:
                failure = AuditFailure("run evidence changed during audit")
                status = "FAILED"
        except BaseException as inventory_error:
            if isinstance(inventory_error, (KeyboardInterrupt, SystemExit)):
                raise
            if failure is None:
                failure = inventory_error
            status = "FAILED"

        artifact_inventory = _artifact_inventory(analysis)
        _write_json(analysis / "artifact-inventory.json", artifact_inventory)
        manifest: dict[str, object] = {
            "artifact_inventory": artifact_inventory,
            "collector_run_id": collector_run_id,
            "commands": capture.records,
            "completed_at_utc": datetime.now(UTC).isoformat(),
            "evidence_immutable": evidence_immutable,
            "evidence_post_sha256": (None if evidence_post is None else evidence_post.sha256),
            "evidence_pre_sha256": evidence_inventory.sha256,
            "expected_sha": expected_sha,
            "failure": (
                None
                if failure is None
                else {"message": str(failure), "type": type(failure).__name__}
            ),
            "gate_verdict": None,
            "raw_immutable": raw_immutable,
            "raw_post_sha256": None if raw_post is None else raw_post.sha256,
            "raw_pre_sha256": raw_before.sha256,
            "raw_matches_completion_seal": raw_matches_completion_seal,
            "run_completion_seal_sha256": _sha256_file(run_seal_path),
            "schema_version": "mma-gate1-post-run-audit-manifest-v1",
            "status": status,
            "terminal_hash": terminal_hash,
        }
        _write_json(manifest_path, manifest)

    if failure is not None:
        raise AuditFailure(str(failure)) from failure
    if status != "PASSED" or not raw_immutable:
        raise AuditFailure("post-run audit did not satisfy its offline contract")
    return manifest_path


def main() -> int:
    arguments = _argument_parser().parse_args()
    try:
        manifest = _audit(arguments)
    except (AuditFailure, OSError, ValueError) as error:
        print(f"POST-RUN AUDIT FAILED: {error}", file=sys.stderr)
        return 1
    print(f"POST-RUN AUDIT COMPLETED: {manifest}")
    print("NO GATE VERDICT WAS PRODUCED.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
