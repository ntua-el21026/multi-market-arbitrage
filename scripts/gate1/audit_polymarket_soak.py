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
RAW_SOURCE_IDENTITY_SCHEMA = "mma-gate1-raw-source-identity-v2"
JOURNAL_MANIFEST_VERSION = "raw-journal-manifest-v1"
JOURNAL_RUN_COMPLETION_VERSION = "raw-journal-run-completion-v2"
JOURNAL_SOURCE_AUTHORITY_VERSION = "raw-journal-source-authority-v1"
JOURNAL_SOURCE_AUTHORITY_MAX_BYTES = 64 * 1024
JOURNAL_COMPLETION_MAX_BYTES = 8 * 1024 * 1024
SAFE_COMPONENT_PATTERN = re.compile(r"[A-Za-z0-9_.-]+")
SOURCE_AUTHORITY_FIELDS = frozenset(
    {
        "authority_origin",
        "authority_sha256",
        "authority_version",
        "collector_run_id",
        "git_commit",
        "group_commit_max_delay_seconds_hex",
        "group_commit_max_events",
        "max_events_per_segment",
        "max_pending_seal_age_nanoseconds",
        "max_pending_seal_age_seconds_hex",
        "max_pending_seal_uncompressed_bytes",
        "max_uncompressed_bytes_per_run",
        "max_uncompressed_bytes_per_segment",
        "predecessor_manifest_sha256",
        "queue_capacity",
        "seal_queue_capacity",
        "stream",
        "submit_timeout_seconds_hex",
        "venue_id",
        "writer_id",
        "writer_software_version",
        "zstd_level",
    }
)
MANIFEST_FIELDS = frozenset(
    {
        "collector_run_id",
        "compressed_bytes",
        "created_at_utc_ns",
        "end_time_utc_ns",
        "event_count",
        "first_ingest_seq",
        "git_commit",
        "hash_algorithm",
        "journal_format_version",
        "last_ingest_seq",
        "manifest_sha256",
        "payload_digest",
        "previous_segment_digest",
        "recovery_evidence",
        "segment_digest",
        "start_time_utc_ns",
        "stream",
        "uncompressed_bytes",
        "venue",
        "writer_id",
        "writer_software_version",
    }
)
COMPLETION_V2_FIELDS = frozenset(
    {
        "collector_run_id",
        "completed_at_utc_ns",
        "completion_sha256",
        "completion_version",
        "expected_event_count",
        "expected_segment_count",
        "expected_uncompressed_bytes",
        "first_ingest_seq",
        "git_commit",
        "hash_algorithm",
        "last_ingest_seq",
        "max_uncompressed_bytes_per_run",
        "max_uncompressed_bytes_per_segment",
        "source_authority_sha256",
        "stream",
        "terminal_manifest_sha256",
        "venue",
        "writer_id",
        "writer_software_version",
    }
)
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


def _fingerprint(path: Path) -> dict[str, object]:
    resolved = path.resolve(strict=True)
    return {
        "bytes": resolved.stat().st_size,
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
    }


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


def _strict_text(record: dict[str, Any], field: str, *, label: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value:
        raise AuditFailure(f"{label} {field} is invalid")
    return value


def _strict_integer(record: dict[str, Any], field: str, *, label: str) -> int:
    value = record.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise AuditFailure(f"{label} {field} is invalid")
    return value


def _verify_canonical_record_hash(
    record: dict[str, Any],
    *,
    hash_field: str,
    label: str,
) -> str:
    claimed = _strict_text(record, hash_field, label=label)
    if SHA256_PATTERN.fullmatch(claimed) is None:
        raise AuditFailure(f"{label} {hash_field} is malformed")
    content = dict(record)
    del content[hash_field]
    observed = hashlib.sha256(_canonical_bytes(content)).hexdigest()
    if claimed != observed:
        raise AuditFailure(f"{label} {hash_field} differs from canonical content")
    return claimed


def _verify_canonical_json_file(path: Path, record: dict[str, Any], *, label: str) -> None:
    if path.read_bytes() != _canonical_bytes(record) + b"\n":
        raise AuditFailure(f"{label} bytes are non-canonical")


def _expected_source_authority_values(config: Path) -> dict[str, object]:
    try:
        with config.open("rb") as handle:
            document = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise AuditFailure("cannot load the strict Phase-1 configuration") from error
    system_value = document.get("system")
    storage_value = document.get("storage")
    if not isinstance(system_value, dict) or not isinstance(storage_value, dict):
        raise AuditFailure("configuration omits system or storage authority")
    system = cast(dict[str, object], system_value)
    storage = cast(dict[str, object], storage_value)

    def integer(mapping: dict[str, object], field: str) -> int:
        value = mapping.get(field)
        if isinstance(value, bool) or not isinstance(value, int):
            raise AuditFailure(f"configuration {field} is not an integer")
        return value

    def finite_float(mapping: dict[str, object], field: str) -> float:
        value = mapping.get(field)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise AuditFailure(f"configuration {field} is not numeric")
        result = float(value)
        if not math.isfinite(result):
            raise AuditFailure(f"configuration {field} is non-finite")
        return result

    code_version = system.get("code_version")
    if not isinstance(code_version, str) or not code_version:
        raise AuditFailure("configuration system.code_version is invalid")
    submit_timeout = finite_float(storage, "journal_submit_timeout_seconds")
    group_delay = finite_float(storage, "journal_group_commit_max_delay_seconds")
    pending_age = finite_float(storage, "journal_pending_seal_max_age_seconds")
    return {
        "group_commit_max_delay_seconds_hex": group_delay.hex(),
        "group_commit_max_events": integer(storage, "journal_group_commit_max_events"),
        "max_events_per_segment": integer(storage, "journal_segment_max_events"),
        "max_pending_seal_age_nanoseconds": int(pending_age * 1_000_000_000),
        "max_pending_seal_age_seconds_hex": pending_age.hex(),
        "max_pending_seal_uncompressed_bytes": integer(
            storage, "journal_pending_seal_max_uncompressed_bytes"
        ),
        "max_uncompressed_bytes_per_run": integer(storage, "journal_run_max_uncompressed_bytes"),
        "max_uncompressed_bytes_per_segment": integer(
            storage, "journal_segment_max_uncompressed_bytes"
        ),
        "queue_capacity": integer(system, "queue_capacity"),
        "seal_queue_capacity": integer(storage, "journal_seal_queue_capacity"),
        "submit_timeout_seconds_hex": submit_timeout.hex(),
        "writer_software_version": code_version,
        # Phase-1 observe does not override JournalWriterConfig's frozen default.
        "zstd_level": 9,
    }


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
    config: Path,
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
    pending_paths = tuple(sorted(raw_root.glob(f"{prefix}*.jsonl.seal-pending*")))
    if pending_paths:
        raise AuditFailure("selected completed run retains seal-pending journal evidence")
    authority_temps = tuple(sorted(raw_root.glob(f".{prefix}*.source-authority.json.*.tmp")))
    if authority_temps:
        raise AuditFailure("selected run source-authority publication is incomplete")
    completion_temps = tuple(sorted(raw_root.glob(f".{prefix}*.completed.json.*.tmp")))
    if completion_temps:
        raise AuditFailure("selected run completion publication is incomplete")

    authority_paths = tuple(sorted(raw_root.glob(f"{prefix}*.source-authority.json")))
    if not authority_paths:
        raise AuditFailure("selected run has no exact source-authority artifact")
    completion_paths = tuple(sorted(raw_root.glob(f"{prefix}*.completed.json")))
    if not completion_paths:
        raise AuditFailure("selected run has no journal completion header")

    expected_authority_values = _expected_source_authority_values(config)
    authorities: dict[str, dict[str, Any]] = {}
    authority_records: list[dict[str, object]] = []
    for path in authority_paths:
        if path.stat().st_size > JOURNAL_SOURCE_AUTHORITY_MAX_BYTES:
            raise AuditFailure("selected run source authority exceeds its byte bound")
        record = _read_json(path)
        if set(record) != set(SOURCE_AUTHORITY_FIELDS):
            raise AuditFailure("selected run source authority fields differ from v1")
        stream = _strict_text(record, "stream", label="selected run source authority")
        if SAFE_COMPONENT_PATTERN.fullmatch(stream) is None:
            raise AuditFailure("selected run source authority stream is invalid")
        if path.name != f"{prefix}{stream}.source-authority.json":
            raise AuditFailure("selected run source-authority filename differs")
        if record.get("authority_version") != JOURNAL_SOURCE_AUTHORITY_VERSION:
            raise AuditFailure("selected run source-authority version differs")
        if record.get("collector_run_id") != collector_run_id:
            raise AuditFailure("selected run source-authority scope differs")
        if record.get("git_commit") != expected_sha:
            raise AuditFailure("selected run source-authority git_commit differs")
        if record.get("venue_id") != "polymarket":
            raise AuditFailure("selected run source-authority venue differs")
        _strict_text(record, "writer_id", label="selected run source authority")
        if record.get("authority_origin") != "NEW_RUN":
            raise AuditFailure("new qualification run source authority is not NEW_RUN")
        if record.get("predecessor_manifest_sha256") is not None:
            raise AuditFailure("new qualification run source authority claims a predecessor")
        for field, expected in expected_authority_values.items():
            observed = record.get(field)
            if isinstance(expected, int) and (
                isinstance(observed, bool) or not isinstance(observed, int)
            ):
                raise AuditFailure(f"selected run source-authority {field} is invalid")
            if observed != expected:
                raise AuditFailure(
                    f"selected run source-authority {field} differs from configuration"
                )
        authority_sha256 = _verify_canonical_record_hash(
            record,
            hash_field="authority_sha256",
            label="selected run source authority",
        )
        _verify_canonical_json_file(path, record, label="selected run source authority")
        if stream in authorities:
            raise AuditFailure("selected run repeats a source-authority stream")
        authorities[stream] = record
        authority_records.append(
            {
                **_fingerprint(path),
                "authority_sha256": authority_sha256,
                "git_commit": expected_sha,
                "stream": stream,
            }
        )

    manifest_records: list[dict[str, object]] = []
    manifests_by_stream: dict[str, list[tuple[dict[str, Any], Path]]] = {
        stream: [] for stream in authorities
    }
    for path in manifest_paths:
        record = _read_json(path)
        if record.get("git_commit") != expected_sha:
            raise AuditFailure("selected run segment manifest git_commit differs")
        if set(record) != set(MANIFEST_FIELDS):
            raise AuditFailure("selected run segment manifest fields differ")
        stream = _strict_text(record, "stream", label="selected run segment manifest")
        first_sequence = _strict_integer(
            record, "first_ingest_seq", label="selected run segment manifest"
        )
        last_sequence = _strict_integer(
            record, "last_ingest_seq", label="selected run segment manifest"
        )
        event_count = _strict_integer(record, "event_count", label="selected run segment manifest")
        uncompressed_bytes = _strict_integer(
            record, "uncompressed_bytes", label="selected run segment manifest"
        )
        compressed_bytes = _strict_integer(
            record, "compressed_bytes", label="selected run segment manifest"
        )
        if first_sequence < 0 or last_sequence < first_sequence or event_count <= 0:
            raise AuditFailure("selected run segment manifest identity is malformed")
        expected_name = (
            f"{prefix}{stream}.{first_sequence:020d}-{last_sequence:020d}.jsonl.zst.manifest.json"
        )
        if path.name != expected_name or record.get("collector_run_id") != collector_run_id:
            raise AuditFailure("selected run segment manifest scope differs")
        authority = authorities.get(stream)
        if authority is None:
            raise AuditFailure("selected run segment has no source authority")
        if (
            record.get("journal_format_version") != JOURNAL_MANIFEST_VERSION
            or record.get("hash_algorithm") != "sha256"
        ):
            raise AuditFailure("selected run segment manifest version differs")
        if (
            record.get("venue") != authority["venue_id"]
            or record.get("writer_id") != authority["writer_id"]
            or record.get("writer_software_version") != authority["writer_software_version"]
        ):
            raise AuditFailure("selected run segment provenance differs from source authority")
        if event_count != last_sequence - first_sequence + 1:
            raise AuditFailure("selected run segment event count differs from its range")
        if (
            event_count > cast(int, authority["max_events_per_segment"])
            or uncompressed_bytes <= 0
            or uncompressed_bytes > cast(int, authority["max_uncompressed_bytes_per_segment"])
        ):
            raise AuditFailure("selected run segment exceeds source-authority bounds")
        segment_path = path.with_name(path.name.removesuffix(".manifest.json"))
        if segment_path not in segment_paths:
            raise AuditFailure("selected run segment manifest has no exact segment")
        if compressed_bytes != segment_path.stat().st_size:
            raise AuditFailure("selected run segment compressed byte count differs")
        segment_digest = _strict_text(
            record, "segment_digest", label="selected run segment manifest"
        )
        payload_digest = _strict_text(
            record, "payload_digest", label="selected run segment manifest"
        )
        if (
            SHA256_PATTERN.fullmatch(segment_digest) is None
            or SHA256_PATTERN.fullmatch(payload_digest) is None
            or segment_digest != _sha256_file(segment_path)
        ):
            raise AuditFailure("selected run segment digest identity differs")
        manifest_sha256 = _verify_canonical_record_hash(
            record,
            hash_field="manifest_sha256",
            label="selected run segment manifest",
        )
        _verify_canonical_json_file(path, record, label="selected run segment manifest")
        recovery_evidence = record.get("recovery_evidence")
        if not isinstance(recovery_evidence, list) or not all(
            isinstance(item, str) for item in cast(list[object], recovery_evidence)
        ):
            raise AuditFailure("selected run segment recovery evidence is malformed")
        for timestamp_field in (
            "created_at_utc_ns",
            "end_time_utc_ns",
            "start_time_utc_ns",
        ):
            if _strict_integer(record, timestamp_field, label="selected run segment manifest") < 0:
                raise AuditFailure("selected run segment timestamp is invalid")
        manifests_by_stream[stream].append((record, path))
        manifest_records.append(
            {
                **_fingerprint(path),
                "git_commit": expected_sha,
                "manifest_sha256": manifest_sha256,
                "stream": stream,
            }
        )

    completion_records: list[dict[str, object]] = []
    completion_streams: set[str] = set()
    for path in completion_paths:
        if path.stat().st_size > JOURNAL_COMPLETION_MAX_BYTES:
            raise AuditFailure("selected run completion header exceeds its byte bound")
        record = _read_json(path)
        if record.get("completion_version") != JOURNAL_RUN_COMPLETION_VERSION:
            raise AuditFailure("new qualification run requires journal completion v2")
        if record.get("git_commit") != expected_sha:
            raise AuditFailure("selected run completion header git_commit differs")
        if set(record) != set(COMPLETION_V2_FIELDS):
            raise AuditFailure("selected run completion-v2 fields differ")
        stream = _strict_text(record, "stream", label="selected run completion header")
        if path.name != f"{prefix}{stream}.completed.json":
            raise AuditFailure("selected run completion header filename differs")
        if record.get("collector_run_id") != collector_run_id:
            raise AuditFailure("selected run completion header scope differs")
        authority = authorities.get(stream)
        if authority is None:
            raise AuditFailure("selected run completion has no source authority")
        if (
            record.get("source_authority_sha256") != authority["authority_sha256"]
            or record.get("venue") != authority["venue_id"]
            or record.get("writer_id") != authority["writer_id"]
            or record.get("writer_software_version") != authority["writer_software_version"]
            or record.get("max_uncompressed_bytes_per_segment")
            != authority["max_uncompressed_bytes_per_segment"]
            or record.get("max_uncompressed_bytes_per_run")
            != authority["max_uncompressed_bytes_per_run"]
        ):
            raise AuditFailure("selected run completion provenance differs from source authority")
        if record.get("hash_algorithm") != "sha256":
            raise AuditFailure("selected run completion hash algorithm differs")
        completion_sha256 = _verify_canonical_record_hash(
            record,
            hash_field="completion_sha256",
            label="selected run completion header",
        )
        _verify_canonical_json_file(path, record, label="selected run completion header")
        if stream in completion_streams:
            raise AuditFailure("selected run repeats a completion stream")
        completion_streams.add(stream)

        ordered = sorted(
            manifests_by_stream[stream],
            key=lambda item: cast(int, item[0]["first_ingest_seq"]),
        )
        next_sequence = 0
        previous_digest: str | None = None
        total_events = 0
        total_uncompressed_bytes = 0
        for manifest, _manifest_path in ordered:
            if manifest["first_ingest_seq"] != next_sequence:
                raise AuditFailure("selected run segment sequence is gapped or reordered")
            if manifest["previous_segment_digest"] != previous_digest:
                raise AuditFailure("selected run segment digest chain differs")
            next_sequence = cast(int, manifest["last_ingest_seq"]) + 1
            previous_digest = cast(str, manifest["segment_digest"])
            total_events += cast(int, manifest["event_count"])
            total_uncompressed_bytes += cast(int, manifest["uncompressed_bytes"])
        if total_uncompressed_bytes > cast(int, authority["max_uncompressed_bytes_per_run"]):
            raise AuditFailure("selected run exceeds source-authority run bytes")
        expected_segment_count = _strict_integer(
            record, "expected_segment_count", label="selected run completion header"
        )
        expected_event_count = _strict_integer(
            record, "expected_event_count", label="selected run completion header"
        )
        expected_uncompressed_bytes = _strict_integer(
            record, "expected_uncompressed_bytes", label="selected run completion header"
        )
        if (
            expected_segment_count != len(ordered)
            or expected_event_count != total_events
            or expected_uncompressed_bytes != total_uncompressed_bytes
        ):
            raise AuditFailure("selected run completion inventory differs from manifests")
        if (
            _strict_integer(record, "completed_at_utc_ns", label="selected run completion header")
            < 0
        ):
            raise AuditFailure("selected run completion timestamp is invalid")
        if ordered:
            if (
                record.get("first_ingest_seq") != 0
                or record.get("last_ingest_seq") != total_events - 1
                or record.get("terminal_manifest_sha256") != ordered[-1][0]["manifest_sha256"]
            ):
                raise AuditFailure("selected run completion terminal inventory differs")
        elif any(
            record.get(field) is not None
            for field in ("first_ingest_seq", "last_ingest_seq", "terminal_manifest_sha256")
        ):
            raise AuditFailure("empty selected run completion has non-null terminal inventory")
        completion_records.append(
            {
                **_fingerprint(path),
                "completion_sha256": completion_sha256,
                "completion_version": JOURNAL_RUN_COMPLETION_VERSION,
                "git_commit": expected_sha,
                "source_authority_sha256": authority["authority_sha256"],
                "stream": stream,
            }
        )
    if completion_streams != set(authorities):
        raise AuditFailure("selected run completion/source-authority stream sets differ")
    return {
        "collector_run_id": collector_run_id,
        "completion_headers": completion_records,
        "expected_git_commit": expected_sha,
        "run_key": run_key,
        "schema_version": RAW_SOURCE_IDENTITY_SCHEMA,
        "segment_manifests": manifest_records,
        "source_authorities": authority_records,
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
        config=config,
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
